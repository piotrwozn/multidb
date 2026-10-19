use std::{
    env,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::Arc,
};

use serde_json::json;

use multidb::{
    backup::{
        BackupConfig, BackupError, RestoreConfig, RestoreTarget, full_backup, incremental_backup,
        list_backups, restore_backup_with_config, verify_backup,
    },
    cdc::{
        ChangefeedFilter, ChangefeedOptions, ChangefeedTarget, MaterializedViewSpec, ResumeToken,
        SubscriptionConfig,
    },
    config_spec::{
        ApplyCheckReport, ApplyStatus, CertificationImpact, ChangePolicy, CommitMode,
        CompressionKnob, ConfigExplainer, ConsistencyDomainSpec, ConsistencyMode,
        CustomProfileSpec, DATABASE_SPEC_VERSION_V2, DatabaseDefaults, DatabaseSpec,
        DeploymentMode, DeploymentSpec, ExplainConfigReport, FloorOverrides, GuaranteeSpec,
        GuaranteeValidator, KNOB_REGISTRY, KnobDefinition, KnobEnforcement, KnobSource, KnobTiming,
        LayoutKnob, MigrationPlan, MigrationPlanner, PolicyCompiler, ProfileKnobOverrides,
        ProfileKnobs, ProfileResolution, ProfileSpec, ProfileVariant, RESERVED_KNOBS,
        ReplicationMode, ResolvedProfile, ResolvedProfileKind, RoleSetDelta, StorageMedium,
        TopologySpec, UnmanagedPolicy, ValidationIssue, ValidationReport, ValidationSeverity,
        built_in_profile_normalized, built_in_profiles, collection_role_definition,
        collection_role_definitions, consistency_domain_definition, consistency_domain_definitions,
        parse_knob_assignment, profile_variants, resolve_named_profile, resolve_profile,
        spec_digest,
        spec_io::{self, SpecFormat, SpecIoError},
        suggest_profile_name,
    },
    db::{
        Database, DbConfig, DbError, MetadataProbe, OperationalConfig, PersistedProfileManifest,
        ProbedMetadata, Profile, ReplicationKind, create_database, inspect_database_metadata,
        open_database, open_database_with_ops, recover_database_file,
    },
    migration::{
        ExportFormat, ImportOptions, MigrationError, export_table_csv, export_table_jsonl,
        import_pg_dump_plain, import_table_csv, import_table_jsonl, migrate_value_codec,
    },
    network::{AuthConfig, NetworkConfig, PgServer, ScramCredential, TlsConfig, TlsMode},
    provision::{
        self, Confirmation, ExportedSpec, PlanOutcome, ProvisionActor, ProvisionApplyReport,
        ProvisionEnvironment, ProvisionError, ProvisionOptions, ProvisionOutcome, ProvisionPlan,
        ProvisionSurface, ProvisionTargetRef, StartupMode, StartupRefusal, StartupReport,
    },
    runtime_advisor::{RuntimeAdviceDecisionRequest, RuntimeAdviceReport, RuntimeAdviceStatus},
    templates::{
        TEMPLATE_SPEC_VERSION, TemplateMaterialization, TemplateOutputFormat, built_in_template,
        built_in_templates, materialize_template,
    },
};

#[derive(thiserror::Error, Debug)]
enum CliError {
    #[error("usage: {0}")]
    Usage(String),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("backup: {0}")]
    Backup(#[from] BackupError),

    #[error("database config: {0}")]
    Config(#[from] multidb::db::ConfigError),

    #[error("database: {0}")]
    Db(#[from] DbError),

    #[error("migration: {0}")]
    Migration(#[from] MigrationError),

    #[error("network: {0}")]
    Network(#[from] multidb::network::NetworkError),

    #[error("runtime: {0}")]
    Runtime(String),

    #[error("template: {0}")]
    Template(#[from] multidb::templates::TemplateError),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("yaml: {0}")]
    Yaml(#[from] serde_norway::Error),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CliRun {
    message: String,
    exit_code: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ServeRuntimeMode {
    Production,
    LocalDev,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ServePgTls {
    Require { cert: PathBuf, key: PathBuf },
    DisabledForLocalDev,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ServeConfig {
    admin_bind: SocketAddr,
    pg_bind: SocketAddr,
    db_path: PathBuf,
    profile: ProfileSelection,
    admin_token: Option<String>,
    admin_password: Option<String>,
    admin_password_reset: bool,
    admin_session_ttl_seconds: u64,
    admin_login_rate_limit: multidb::admin::AdminLoginRateLimitConfig,
    pg_user: String,
    pg_password: String,
    pg_tls: ServePgTls,
    studio_dir: PathBuf,
    /// Spec mode, when `MULTIDB_SPEC_PATH` is set.
    spec: Option<ServeSpecConfig>,
}

/// `serve` in spec mode: the database is built from, and checked against, the spec at
/// `MULTIDB_SPEC_PATH` at every start (see `serve_spec_database`).
#[derive(Clone, Debug, PartialEq, Eq)]
struct ServeSpecConfig {
    path: PathBuf,
    /// `MULTIDB_SPEC_APPLY`: `verify` (the default), `create` or `additive`.
    mode: StartupMode,
    /// `MULTIDB_DB_PATH` is set (present and not blank), so it wins over the spec
    /// `deployment.storage_path`.
    explicit_db_path: bool,
    /// `MULTIDB_PROFILE` is set, so it must not select another storage profile than the spec.
    explicit_profile: bool,
    /// `MULTIDB_REPLICATION`, read in spec mode only; it must equal the spec's replication.
    explicit_replication: Option<ReplicationMode>,
}

/// What a `--profile` flag or `MULTIDB_PROFILE` selects: a storage profile, or `auto`, which
/// reads the storage profile of an existing single-node CP database (see `cli_db_config`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProfileSelection {
    Fixed(Profile),
    Auto,
}

impl CliRun {
    fn success(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            exit_code: 0,
        }
    }

    fn failure(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            exit_code: 1,
        }
    }

    fn validation(message: impl Into<String>, valid: bool) -> Self {
        Self {
            message: message.into(),
            exit_code: if valid { 0 } else { 2 },
        }
    }

    /// A run with an explicit exit code, such as the provisioning codes of
    /// `provision_exit_code`.
    fn exit(message: impl Into<String>, exit_code: u8) -> Self {
        Self {
            message: message.into(),
            exit_code,
        }
    }
}

fn main() -> ExitCode {
    match run(env::args().skip(1).collect()) {
        Ok(outcome) => {
            println!("{}", outcome.message);
            ExitCode::from(outcome.exit_code)
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: Vec<String>) -> Result<CliRun, CliError> {
    let Some(command) = args.first() else {
        return Err(usage());
    };

    match command.as_str() {
        "config" => run_config(&args[1..]),
        "explain" => run_explain(&args[1..]),
        "init" => run_init(&args[1..]),
        "profile" => run_profile(&args[1..]),
        "role" => run_role(&args[1..]),
        "domain" => run_domain(&args[1..]),
        "template" => run_template(&args[1..]),
        "advice" => run_advice(&args[1..]),
        "serve" => run_serve(&args[1..]).map(CliRun::success),
        _ => run_command(&args).map(CliRun::success),
    }
}

fn run_command(args: &[String]) -> Result<String, CliError> {
    let Some(command) = args.first() else {
        return Err(usage());
    };

    match command.as_str() {
        "backup" => run_backup(&args[1..]),
        "restore" => run_restore(&args[1..]),
        "verify" => run_verify(&args[1..]),
        "list" => run_list(&args[1..]),
        "cdc" => run_cdc(&args[1..]),
        "view" => run_view(&args[1..]),
        "import" => run_import(&args[1..]),
        "export" => run_export(&args[1..]),
        "migrate" => run_migrate(&args[1..]),
        "admin" => run_admin(&args[1..]),
        "config" => run_config(&args[1..]).map(|outcome| outcome.message),
        "template" => run_template(&args[1..]).map(|outcome| outcome.message),
        "advice" => run_advice(&args[1..]).map(|outcome| outcome.message),
        _ => Err(usage()),
    }
}

fn run_config(args: &[String]) -> Result<CliRun, CliError> {
    let Some(command) = args.first() else {
        return Err(usage());
    };

    match command.as_str() {
        "validate" => run_config_validate(&args[1..]),
        "explain" => run_config_explain(&args[1..]),
        "plan" => run_config_plan(&args[1..]),
        "apply" => run_config_apply(&args[1..]),
        "upgrade" => run_config_upgrade(&args[1..]),
        "drift" => run_config_drift(&args[1..]),
        "status" => run_config_status(&args[1..]),
        "export" => run_config_export(&args[1..]),
        _ => Err(usage()),
    }
}

fn run_config_validate(args: &[String]) -> Result<CliRun, CliError> {
    let spec_path = PathBuf::from(required_value(args, "--spec")?);
    let output = output_format(args)?;
    let spec = match read_spec_file(&spec_path)? {
        Ok(spec) => spec,
        Err(report) => return spec_report_run(&report, &output, None),
    };
    let report = GuaranteeValidator::validate(&spec);
    let message = if output == "json" {
        serde_json::to_string_pretty(&report)?
    } else {
        format_validation_report(&report)
    };

    Ok(CliRun::validation(message, report.valid))
}

fn run_config_explain(args: &[String]) -> Result<CliRun, CliError> {
    let spec_path = PathBuf::from(required_value(args, "--spec")?);
    let output = output_format(args)?;
    let spec = match read_spec_file(&spec_path)? {
        Ok(spec) => spec,
        Err(report) => return spec_report_run(&report, &output, None),
    };
    let report = ConfigExplainer::explain(&spec);
    let message = if output == "json" {
        serde_json::to_string_pretty(&report)?
    } else {
        format_explain_report(&report)
    };

    Ok(CliRun::validation(message, report.validation.valid))
}

fn run_config_plan(args: &[String]) -> Result<CliRun, CliError> {
    if has_flag(args, "--spec") {
        reject_spec_mixing(args)?;
        return run_spec_plan(args);
    }
    let current_path = PathBuf::from(required_value(args, "--current")?);
    let desired_path = PathBuf::from(required_value(args, "--desired")?);
    let output = output_format(args)?;
    let current = match read_spec_file(&current_path)? {
        Ok(spec) => spec,
        Err(report) => {
            return spec_report_run(
                &report,
                &output,
                Some(("--current", current_path.as_path())),
            );
        }
    };
    let desired = match read_spec_file(&desired_path)? {
        Ok(spec) => spec,
        Err(report) => {
            return spec_report_run(
                &report,
                &output,
                Some(("--desired", desired_path.as_path())),
            );
        }
    };
    let plan = MigrationPlanner::plan(&current, &desired);
    let message = if let Some(out_path) = optional_value(args, "--out") {
        let out_path = PathBuf::from(out_path);
        std::fs::write(&out_path, serde_json::to_string_pretty(&plan)?)?;
        if output == "json" {
            serde_json::to_string_pretty(&json!({
                "plan_id": plan.plan_id,
                "valid": plan.valid,
                "apply_supported": plan.apply_supported,
                "path": out_path.display().to_string(),
            }))?
        } else {
            format!(
                "wrote plan {} valid={} apply_supported={} path={}",
                plan.plan_id,
                plan.valid,
                plan.apply_supported,
                out_path.display()
            )
        }
    } else if output == "json" {
        serde_json::to_string_pretty(&plan)?
    } else {
        format_migration_plan(&plan)
    };

    Ok(CliRun::validation(message, plan.valid))
}

fn run_config_apply(args: &[String]) -> Result<CliRun, CliError> {
    if has_flag(args, "--spec") {
        reject_spec_mixing(args)?;
        return run_spec_apply(args);
    }
    let plan_path = PathBuf::from(required_value(args, "--plan")?);
    reject_yaml_plan_path(&plan_path)?;
    let output = output_format(args)?;
    let confirm = required_value(args, "--confirm")?;
    let plan = read_json_file::<MigrationPlan>(&plan_path)?;
    let report = MigrationPlanner::check_apply(&plan, &confirm);
    let message = if output == "json" {
        serde_json::to_string_pretty(&report)?
    } else {
        format_apply_report(&report)
    };

    Ok(CliRun::validation(
        message,
        report.status == ApplyStatus::Confirmed,
    ))
}

/// `config upgrade`: converts a spec to version 2 (see `upgrade_spec`) and validates it. A valid
/// result goes to `--out`, in the format its extension names, or to stdout in the input's
/// format. An invalid result exits 2 and is never written to `--out`. A result over
/// `spec_io::MAX_SPEC_BYTES`, which multidb could not read back, exits 2 with `SPEC_TOO_LARGE`
/// and is written nowhere, not even to stdout.
fn run_config_upgrade(args: &[String]) -> Result<CliRun, CliError> {
    let spec_path = PathBuf::from(required_value(args, "--spec")?);
    let output = output_format(args)?;
    let out_path = optional_value(args, "--out").map(PathBuf::from);
    let spec = match read_spec_file(&spec_path)? {
        Ok(spec) => spec,
        Err(report) => return spec_report_run(&report, &output, None),
    };
    if let Some(out_path) = &out_path {
        ensure_upgrade_target(&spec_path, out_path, has_flag(args, "--force"))?;
    }

    let (upgraded, mut notes) = upgrade_spec(spec);
    notes.extend(provisioning_blocker_notes(&upgraded));
    let report = GuaranteeValidator::validate(&upgraded);
    let format =
        match spec_io::spec_format_for_path(out_path.as_deref().unwrap_or(spec_path.as_path())) {
            SpecFormat::Yaml => "yaml",
            SpecFormat::Json => "json",
        };
    let content = serialize_spec(&upgraded, format)?;
    // Re-serializing can grow a compact spec past the size limit, and with `--force` the
    // result would replace an input that multidb can read with one it refuses.
    if let Some(refused) = refuse_oversized_spec(&content, &output, out_path.as_deref())? {
        return Ok(refused);
    }
    // An invalid result is not written: with `--force` it could replace a valid input.
    let written = match &out_path {
        Some(out_path) if report.valid => {
            write_file_replacing(out_path, &content)?;
            true
        }
        _ => false,
    };

    let message = if output == "json" {
        let mut summary = json!({
            "spec": upgraded,
            "notes": notes,
            "validation": report,
        });
        if let Some(out_path) = &out_path {
            summary["path"] = json!(out_path.display().to_string());
            summary["written"] = json!(written);
        }
        serde_json::to_string_pretty(&summary)?
    } else if let Some(out_path) = &out_path {
        let mut lines = vec![format!(
            "{} path={} version={} profile={} status={:?} valid={}",
            if written {
                "upgraded config"
            } else {
                "upgraded config not written (invalid)"
            },
            out_path.display(),
            upgraded.version,
            upgraded.profile,
            report.status,
            report.valid
        )];
        lines.extend(notes.iter().map(|note| format!("note: {note}")));
        lines.extend(report.issues.iter().map(format_issue));
        lines.join("\n")
    } else {
        // stdout carries only the spec, so it can be redirected into a file. The warnings and
        // advice of a valid result (PROFILE_NOT_PROVISIONABLE among them) go to stderr, as
        // they go to the `--out` summary.
        for note in &notes {
            eprintln!("note: {note}");
        }
        if report.valid {
            for issue in &report.issues {
                eprintln!("{}", format_issue(issue));
            }
        } else {
            eprintln!("{}", format_validation_report(&report));
        }
        content.trim_end_matches('\n').to_owned()
    };
    Ok(CliRun::validation(message, report.valid))
}

/// Studio's version 1 custom-profile convention, converted by `config upgrade`.
const STUDIO_PROFILE_BASE: &str = "profile.custom.base";
const STUDIO_PROFILE_DESCRIPTION: &str = "profile.custom.description";
const STUDIO_PROFILE_SLUG: &str = "profile.custom.slug";

/// Converts a spec to version 2. Studio's custom-profile convention
/// (`overrides["profile.custom.base"]`, `overrides["profile.custom.description"]` and
/// `operation_hints["profile.custom.slug"]`) becomes `custom_profile { extends, description }`
/// with the slug as `profile`, and those three keys are removed. Every other key is left as it
/// is. Deterministic and idempotent: upgrading an upgraded spec changes nothing.
fn upgrade_spec(mut spec: DatabaseSpec) -> (DatabaseSpec, Vec<String>) {
    let mut notes = Vec::new();
    if spec.version == DATABASE_SPEC_VERSION_V2 {
        notes.push("the spec is already version 2".to_owned());
    } else {
        notes.push(format!(
            "version {} -> {DATABASE_SPEC_VERSION_V2}",
            spec.version
        ));
        spec.version = DATABASE_SPEC_VERSION_V2;
    }

    if spec.custom_profile.is_none()
        && let Some(extends) = spec.overrides.remove(STUDIO_PROFILE_BASE)
    {
        let description = spec
            .overrides
            .remove(STUDIO_PROFILE_DESCRIPTION)
            .filter(|description| !description.trim().is_empty());
        if let Some(slug) = spec.operation_hints.remove(STUDIO_PROFILE_SLUG) {
            notes.push(format!(
                "operation_hints[{STUDIO_PROFILE_SLUG}] = {slug} became profile"
            ));
            spec.profile = slug;
        }
        notes.push(format!(
            "overrides[{STUDIO_PROFILE_BASE}] = {extends} became custom_profile.extends{}",
            if description.is_some() {
                format!(
                    ", and overrides[{STUDIO_PROFILE_DESCRIPTION}] became custom_profile.description"
                )
            } else {
                String::new()
            }
        ));
        spec.custom_profile = Some(CustomProfileSpec {
            extends,
            description,
            knobs: ProfileKnobOverrides::default(),
            floors: FloorOverrides::default(),
            compatible_roles: RoleSetDelta::default(),
        });
    } else if spec.overrides.contains_key(STUDIO_PROFILE_BASE)
        || spec.overrides.contains_key(STUDIO_PROFILE_DESCRIPTION)
        || spec.operation_hints.contains_key(STUDIO_PROFILE_SLUG)
    {
        notes.push(format!(
            "{} so the profile.custom.* keys were left in place; move them into custom_profile \
             by hand",
            if spec.custom_profile.is_some() {
                "custom_profile is already set,"
            } else {
                "there is no overrides[profile.custom.base],"
            }
        ));
    }

    notes.push(
        "the spec is re-serialized: comments and formatting are not preserved, and defaulted \
         fields such as topology are written out"
            .to_owned(),
    );
    (spec, notes)
}

/// One note per physical parameter that provisioning still needs before it can build the
/// upgraded spec (`PHYSICAL_PARAMETER_REQUIRED` of `provision::physical::project`, which is only
/// Advice in version 2 validation), so `config upgrade` lists what to add before `config apply
/// --spec`. Nothing for a profile that does not resolve: validation reports that.
fn provisioning_blocker_notes(spec: &DatabaseSpec) -> Vec<String> {
    let ProfileResolution::Resolved { profile, .. } = resolve_profile(spec) else {
        return Vec::new();
    };
    match provision::physical::project(spec, &profile) {
        Ok(_) => Vec::new(),
        Err(issues) => issues
            .iter()
            .filter(|issue| issue.code == "PHYSICAL_PARAMETER_REQUIRED")
            .map(|issue| {
                format!(
                    "provisioning blocker: add {} before config apply --spec ({})",
                    issue.path, issue.message
                )
            })
            .collect(),
    }
}

/// Refuses to replace an existing file, and in particular the input spec, without `--force`.
fn ensure_upgrade_target(input: &Path, out: &Path, force: bool) -> Result<(), CliError> {
    if force {
        return Ok(());
    }
    let same_file = match (std::fs::canonicalize(input), std::fs::canonicalize(out)) {
        (Ok(input), Ok(out)) => input == out,
        _ => input == out,
    };
    if same_file {
        return Err(CliError::Usage(format!(
            "refusing to overwrite the input spec {}; pass --force to replace it",
            out.display()
        )));
    }
    if out.exists() {
        return Err(CliError::Usage(format!(
            "refusing to overwrite {}; pass --force to replace it",
            out.display()
        )));
    }
    Ok(())
}

/// Writes `content` to a new, randomly named file next to `path` and renames it into place, so
/// a failed write never leaves a half-written file where the old one was (`config upgrade
/// --force` may replace its input; `config plan --spec --out` and `config export --out` write
/// through it too). See `write_through_temp_file`.
fn write_file_replacing(path: &Path, content: &str) -> Result<(), CliError> {
    let Some(file_name) = path.file_name() else {
        return Err(CliError::Usage(format!(
            "--out {} must name a file",
            path.display()
        )));
    };
    let mut temp_name = std::ffi::OsString::from(".");
    temp_name.push(file_name);
    temp_name.push(format!(".{}.upgrade-tmp", uuid::Uuid::now_v7()));
    Ok(write_through_temp_file(
        path,
        &path.with_file_name(temp_name),
        content,
    )?)
}

/// Writes `content` to `temp_path`, syncs it and renames it to `path`, then syncs the parent
/// directory (best effort) so the rename survives a crash. `temp_path` is created with
/// `create_new`, which never follows or reuses an existing path, so a symlink planted there
/// fails the write with `AlreadyExists` instead of redirecting it; the rename replaces `path`
/// itself, never the target of a symlink at `path`. The temporary file is removed on failure
/// only when this call created it.
fn write_through_temp_file(path: &Path, temp_path: &Path, content: &str) -> std::io::Result<()> {
    use std::io::Write as _;

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temp_path)?;
    let written = file
        .write_all(content.as_bytes())
        .and_then(|()| file.sync_all());
    drop(file);
    if let Err(error) = written.and_then(|()| std::fs::rename(temp_path, path)) {
        let _ = std::fs::remove_file(temp_path);
        return Err(error);
    }
    if let Some(parent) = path.parent() {
        let parent = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        let _ = std::fs::File::open(parent).and_then(|directory| directory.sync_all());
    }
    Ok(())
}

/// The local operator label of every CLI apply; its audit events carry it as the principal.
const CLI_OPERATOR: &str = "local:cli";
/// How many provision history entries `config status` shows, newest last.
const STATUS_HISTORY_ENTRIES: usize = 20;
/// The refusal for a database another handle holds open: redb takes an exclusive file lock, so
/// a served database is planned and applied through the Control Plane instead.
const DATABASE_IN_USE: &str =
    "database is in use (is multidb serve running?); use POST /provision/plan";

/// `--spec` belongs to provisioning; `--plan`, `--current` and `--desired` to the version 1
/// migration plan flow, which stays audit-only.
fn reject_spec_mixing(args: &[String]) -> Result<(), CliError> {
    let mixed = ["--plan", "--current", "--desired"]
        .into_iter()
        .filter(|flag| has_flag(args, flag))
        .collect::<Vec<_>>();
    if mixed.is_empty() {
        return Ok(());
    }
    Err(CliError::Usage(format!(
        "--spec provisions a database from a spec and cannot be combined with {}, which belong \
         to the version 1 plan flow (config plan --current --desired, config apply --plan \
         --confirm)",
        mixed.join(", ")
    )))
}

/// The value of `flag`, or a usage error when the flag is given without one.
fn flag_value(args: &[String], flag: &str) -> Result<Option<String>, CliError> {
    match optional_value(args, flag) {
        Some(value) if !value.starts_with("--") => Ok(Some(value)),
        _ if has_flag(args, flag) => Err(CliError::Usage(format!("missing value for {flag}"))),
        _ => Ok(None),
    }
}

/// `storage_path` of a spec resolved against `dir`, the directory of the spec file (or the
/// output directory of `init`); an absolute path stays as it is.
fn resolve_against(dir: &Path, storage_path: &str) -> PathBuf {
    if dir.as_os_str().is_empty() {
        PathBuf::from(storage_path)
    } else {
        dir.join(storage_path)
    }
}

/// The directory a file lives in, `.` for a bare file name.
fn parent_dir(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

/// The provisioning options of a spec command: `--allow-weakening <path>[,<path>]` and
/// `--allow-destructive <path>[,<path>]` (both repeatable), `--allow-format-upgrade` and
/// `--unmanaged ignore|report|fail`. They are part of the `prov_` id, so an apply must repeat
/// the flags of the plan it confirms.
fn provision_options(args: &[String]) -> Result<ProvisionOptions, CliError> {
    let unmanaged = flag_value(args, "--unmanaged")?
        .map(|value| match value.as_str() {
            "ignore" => Ok(UnmanagedPolicy::Ignore),
            "report" => Ok(UnmanagedPolicy::Report),
            "fail" => Ok(UnmanagedPolicy::Fail),
            other => Err(CliError::Usage(format!(
                "unsupported --unmanaged {other}; expected ignore, report or fail"
            ))),
        })
        .transpose()?;
    Ok(ProvisionOptions {
        allow_weakening: flag_paths(args, "--allow-weakening")?,
        allow_destructive: flag_paths(args, "--allow-destructive")?,
        allow_format_upgrade: has_flag(args, "--allow-format-upgrade"),
        unmanaged,
    })
}

/// Every spec path of a repeatable `--allow-*` flag.
fn flag_paths(args: &[String], flag: &str) -> Result<std::collections::BTreeSet<String>, CliError> {
    let paths = values(args, flag);
    if paths.is_empty() && has_flag(args, flag) {
        return Err(CliError::Usage(format!(
            "missing value for {flag}; give the spec path a step names, for example \
             collections.orders"
        )));
    }
    if let Some(flag_like) = paths.iter().find(|path| path.starts_with("--")) {
        return Err(CliError::Usage(format!(
            "{flag} takes a spec path, not {flag_like}"
        )));
    }
    Ok(paths.into_iter().collect())
}

/// The spec, target path, options and output format of `config plan|apply|drift --spec`, and
/// of `init --apply`.
struct SpecCommand {
    spec_path: PathBuf,
    spec: DatabaseSpec,
    /// `--db`, else `deployment.storage_path` resolved against the directory of the spec file;
    /// `None` when neither names a path.
    db_path: Option<PathBuf>,
    options: ProvisionOptions,
    output: String,
}

impl SpecCommand {
    /// Reads `--spec` through `spec_io`. A spec refused for its size or version comes back as
    /// its report (exit 2); I/O, syntax and decode errors stay CLI errors (exit 1).
    fn load(args: &[String]) -> Result<Result<Self, CliRun>, CliError> {
        let spec_path = PathBuf::from(required_value(args, "--spec")?);
        let output = output_format(args)?;
        let options = provision_options(args)?;
        let db = flag_value(args, "--db")?;
        let spec = match read_spec_file(&spec_path)? {
            Ok(spec) => spec,
            Err(report) => {
                return Ok(Err(spec_report_run(
                    &report,
                    &output,
                    Some(("--spec", spec_path.as_path())),
                )?));
            }
        };
        let db_path = db.map(PathBuf::from).or_else(|| {
            spec.deployment
                .storage_path
                .as_deref()
                .map(|storage_path| resolve_against(parent_dir(&spec_path), storage_path))
        });
        Ok(Ok(Self {
            spec_path,
            spec,
            db_path,
            options,
            output,
        }))
    }

    fn json(&self) -> bool {
        self.output == "json"
    }
}

/// The database a spec command acts on.
enum SpecTarget {
    /// Neither `--db` nor the spec names a path: the plan is computed against a new in-memory
    /// target, which the CLI never builds (`PROVISION_STORAGE_PATH_REQUIRED` for a durable
    /// profile, `PROVISION_VOLATILE_ON_SURFACE` for a volatile one).
    NoPath,
    /// Nothing exists at the path yet, or the file is empty: planned as absent, created by
    /// `config apply`.
    Absent(PathBuf),
    /// An initialized single-node CP database, open for this command.
    Existing {
        db: Box<Database>,
        metadata: Box<ProbedMetadata>,
    },
}

/// Why a spec command cannot act on its target.
enum TargetRefusal {
    /// Another handle holds the database open (exit 1).
    InUse(PathBuf),
    /// The file cannot be probed or opened (exit 1).
    Unreadable(String),
    /// The database exists, but provisioning cannot plan it (exit 2).
    Blocked(Box<ValidationIssue>),
}

/// Probes `path` and opens an initialized single-node CP database with
/// `open_database_with_ops`, which loads its persisted RBAC state. Nothing is created: a missing
/// path stays missing. A file that was not closed cleanly first gets redb's crash recovery,
/// which every opener runs anyway; it is the only change a dry run can make to a file.
fn open_spec_target(path: Option<&Path>) -> Result<SpecTarget, TargetRefusal> {
    let Some(path) = path else {
        return Ok(SpecTarget::NoPath);
    };
    let probe = match inspect_database_metadata(path) {
        MetadataProbe::NeedsRecovery => {
            recover_database_file(path).map_err(|error| {
                TargetRefusal::Unreadable(format!(
                    "{} was not closed cleanly and its crash recovery failed: {error}",
                    path.display()
                ))
            })?;
            inspect_database_metadata(path)
        }
        probe => probe,
    };
    match probe {
        MetadataProbe::NotFound | MetadataProbe::Uninitialized => {
            Ok(SpecTarget::Absent(path.to_path_buf()))
        }
        MetadataProbe::Locked => Err(TargetRefusal::InUse(path.to_path_buf())),
        MetadataProbe::NeedsRecovery => Err(TargetRefusal::Unreadable(format!(
            "{} still needs crash recovery after recovering it",
            path.display()
        ))),
        MetadataProbe::Unreadable { reason } => Err(TargetRefusal::Unreadable(format!(
            "{} cannot be read: {reason}",
            path.display()
        ))),
        MetadataProbe::Initialized(metadata) => {
            if metadata.replication != ReplicationKind::Cp {
                return Err(TargetRefusal::Blocked(Box::new(ValidationIssue {
                    code: "PROVISION_REPLICATION_UNSUPPORTED".to_owned(),
                    severity: ValidationSeverity::Error,
                    path: "defaults.replication".to_owned(),
                    message: format!(
                        "the database at {} uses {:?} replication; provisioning plans and \
                         applies single-node CP databases only",
                        path.display(),
                        metadata.replication
                    ),
                    suggestion: "Provision a single-node CP database instead.".to_owned(),
                    certification_impact: CertificationImpact::BlocksCertified,
                })));
            }
            let config = DbConfig::on_disk(metadata.storage_profile, path);
            let db =
                open_database_with_ops(config, &OperationalConfig::new()).map_err(|error| {
                    TargetRefusal::Unreadable(format!(
                        "{} cannot be opened: {error}",
                        path.display()
                    ))
                })?;
            Ok(SpecTarget::Existing {
                db: Box::new(db),
                metadata,
            })
        }
    }
}

const fn cli_environment() -> ProvisionEnvironment {
    ProvisionEnvironment::new(ProvisionSurface::Cli)
}

/// The plan of the spec against its target, on the CLI surface.
fn plan_spec_target(
    command: &SpecCommand,
    target: &SpecTarget,
) -> Result<ProvisionPlan, ProvisionError> {
    let target = match target {
        SpecTarget::NoPath => ProvisionTargetRef::Volatile,
        SpecTarget::Absent(path) => ProvisionTargetRef::Path(path),
        SpecTarget::Existing { db, .. } => ProvisionTargetRef::Open(db),
    };
    provision::plan_provision(&command.spec, target, &command.options, &cli_environment())
}

/// What a provisioning command ended with, for its exit code.
enum ProvisionEnd<'a> {
    /// `config plan --spec`: the dry-run plan.
    Planned(&'a ProvisionPlan),
    /// `config drift`: the plan whose drift is reported.
    Drifted(&'a ProvisionPlan),
    /// `config apply --spec` and `init --apply`.
    Applied(&'a ProvisionApplyReport),
    Failed(&'a ProvisionError),
}

/// The exit code of a provisioning command:
/// - 0: a ready or noop plan, an applied or noop apply, a database in sync;
/// - 1: a partial apply, a runtime or I/O error, a locked database;
/// - 2: an invalid spec, a blocked plan, missing flags, a missing or rejected confirmation, a
///   target that already holds a database; for `config drift`, an invalid spec or a spec
///   without a database path;
/// - 3: drift (`config drift` only);
/// - 4: the confirmed plan changed.
///
/// Usage errors stay `CliError::Usage`, which `main` reports with exit 1.
fn provision_exit_code(end: ProvisionEnd<'_>) -> u8 {
    match end {
        ProvisionEnd::Planned(plan) => match plan.outcome {
            PlanOutcome::Noop | PlanOutcome::Ready => 0,
            PlanOutcome::NeedsFlags | PlanOutcome::Blocked => 2,
        },
        ProvisionEnd::Drifted(plan) => {
            if !plan.validation.valid || plan.target.path.is_none() {
                2
            } else if plan.drift.in_sync {
                0
            } else {
                3
            }
        }
        ProvisionEnd::Applied(report) => match report.status {
            ProvisionOutcome::Applied | ProvisionOutcome::Noop => 0,
            ProvisionOutcome::PartiallyApplied => 1,
            ProvisionOutcome::Rejected | ProvisionOutcome::Blocked => 2,
            ProvisionOutcome::PlanChanged => 4,
        },
        ProvisionEnd::Failed(error) => match error {
            ProvisionError::PlanChanged { .. } => 4,
            ProvisionError::InvalidSpec(_)
            | ProvisionError::Blocked(_)
            | ProvisionError::NeedsFlags(_)
            | ProvisionError::ConfirmationRequired(_)
            | ProvisionError::AlreadyInitialized
            | ProvisionError::AuditRequired
            | ProvisionError::PermissionMissing(_) => 2,
            ProvisionError::StartupRefused(refusal) => {
                if refusal.applied.is_some() {
                    1
                } else {
                    2
                }
            }
            ProvisionError::Locked(_)
            | ProvisionError::RecordUnreadable(_)
            | ProvisionError::Db(_)
            | ProvisionError::Config(_)
            | ProvisionError::Io(_) => 1,
        },
    }
}

/// The `status` of a provisioning error in JSON output.
const fn provision_error_status(error: &ProvisionError) -> &'static str {
    match error {
        ProvisionError::InvalidSpec(_) => "invalid_spec",
        ProvisionError::Blocked(_) => "blocked",
        ProvisionError::NeedsFlags(_) => "needs_flags",
        ProvisionError::ConfirmationRequired(_) => "confirmation_required",
        ProvisionError::PlanChanged { .. } => "plan_changed",
        ProvisionError::AlreadyInitialized => "already_initialized",
        ProvisionError::AuditRequired => "audit_required",
        ProvisionError::PermissionMissing(_) => "permission_missing",
        ProvisionError::Locked(_) => "in_use",
        ProvisionError::RecordUnreadable(_) => "record_unreadable",
        ProvisionError::StartupRefused(_) => "startup_refused",
        ProvisionError::Db(_) => "database_error",
        ProvisionError::Config(_) => "config_error",
        ProvisionError::Io(_) => "io_error",
    }
}

fn in_use_message(path: Option<&Path>) -> String {
    path.map_or_else(
        || DATABASE_IN_USE.to_owned(),
        |path| {
            format!(
                "{DATABASE_IN_USE} ({} is held open by another handle)",
                path.display()
            )
        },
    )
}

/// A run that only carries a message, as `{"status", "message"}` in JSON output.
fn message_run(
    status: &str,
    message: String,
    exit_code: u8,
    json: bool,
) -> Result<CliRun, CliError> {
    let message = if json {
        serde_json::to_string_pretty(&json!({ "status": status, "message": message }))?
    } else {
        message
    };
    Ok(CliRun::exit(message, exit_code))
}

fn refusal_run(refusal: &TargetRefusal, json: bool) -> Result<CliRun, CliError> {
    match refusal {
        TargetRefusal::InUse(path) => message_run("in_use", in_use_message(Some(path)), 1, json),
        TargetRefusal::Unreadable(reason) => message_run("unreadable", reason.clone(), 1, json),
        TargetRefusal::Blocked(issue) => {
            let message = if json {
                serde_json::to_string_pretty(&json!({
                    "status": "blocked",
                    "outcome": "blocked",
                    "message": issue.message,
                    "issues": [issue],
                }))?
            } else {
                format!("provisioning blocked\n{}", format_issue(issue))
            };
            Ok(CliRun::exit(message, 2))
        }
    }
}

/// The serde name of a provisioning enum value, for text output.
fn slug<T: serde::Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(name)) => name,
        _ => "unknown".to_owned(),
    }
}

/// A `required_flags` entry as a CLI flag: `allow_weakening:<path>` is
/// `--allow-weakening <path>`, `allow_destructive:<path>` is `--allow-destructive <path>`.
fn cli_flag(flag: &str) -> String {
    if let Some(path) = flag.strip_prefix("allow_weakening:") {
        format!("--allow-weakening {path}")
    } else if let Some(path) = flag.strip_prefix("allow_destructive:") {
        format!("--allow-destructive {path}")
    } else if flag == "allow_format_upgrade" {
        "--allow-format-upgrade".to_owned()
    } else {
        flag.to_owned()
    }
}

/// The options as CLI flags, each with a leading space.
fn option_flags(options: &ProvisionOptions) -> String {
    let mut flags = String::new();
    for path in &options.allow_weakening {
        flags.push_str(&format!(" --allow-weakening {path}"));
    }
    for path in &options.allow_destructive {
        flags.push_str(&format!(" --allow-destructive {path}"));
    }
    if options.allow_format_upgrade {
        flags.push_str(" --allow-format-upgrade");
    }
    if let Some(policy) = options.unmanaged {
        flags.push_str(&format!(" --unmanaged {}", slug(&policy)));
    }
    flags
}

/// `options` plus every flag of `required_flags`.
fn options_with_flags(options: &ProvisionOptions, required_flags: &[String]) -> ProvisionOptions {
    let mut options = options.clone();
    for flag in required_flags {
        if let Some(path) = flag.strip_prefix("allow_weakening:") {
            options.allow_weakening.insert(path.to_owned());
        } else if let Some(path) = flag.strip_prefix("allow_destructive:") {
            options.allow_destructive.insert(path.to_owned());
        } else if flag == "allow_format_upgrade" {
            options.allow_format_upgrade = true;
        }
    }
    options
}

/// `flag` is given by `options`.
fn flag_given(options: &ProvisionOptions, flag: &str) -> bool {
    if let Some(path) = flag.strip_prefix("allow_weakening:") {
        options.allow_weakening.contains(path)
    } else if let Some(path) = flag.strip_prefix("allow_destructive:") {
        options.allow_destructive.contains(path)
    } else {
        flag == "allow_format_upgrade" && options.allow_format_upgrade
    }
}

/// ` --db <path>`: the canonical target path of a plan when there is one, else the path of the
/// command. Every spelling of one file gives the same `prov_` id.
fn db_flag(command: &SpecCommand, canonical: Option<&str>) -> String {
    canonical
        .map(str::to_owned)
        .or_else(|| {
            command
                .db_path
                .as_ref()
                .map(|path| path.display().to_string())
        })
        .map_or_else(String::new, |path| format!(" --db {path}"))
}

fn plan_command(
    command: &SpecCommand,
    options: &ProvisionOptions,
    canonical: Option<&str>,
) -> String {
    format!(
        "multidb config plan --spec {}{}{}",
        command.spec_path.display(),
        db_flag(command, canonical),
        option_flags(options)
    )
}

fn apply_command(command: &SpecCommand, plan: &ProvisionPlan) -> String {
    format!(
        "multidb config apply --spec {}{}{} --confirm {}",
        command.spec_path.display(),
        db_flag(command, plan.target.path.as_deref()),
        option_flags(&command.options),
        plan.plan_id
    )
}

/// What to do with a plan: the exact apply command of a ready plan, the re-plan command with
/// the missing flags of a plan that needs them.
fn plan_next_step(plan: &ProvisionPlan, command: &SpecCommand) -> String {
    match plan.outcome {
        PlanOutcome::Noop => "nothing to apply: the database already matches the spec".to_owned(),
        PlanOutcome::Ready => format!(
            "apply with: {}{}",
            apply_command(command, plan),
            if plan.auto_approvable {
                " (or --auto-approve in place of --confirm: the plan only creates, adopts, \
                 records or strengthens)"
            } else {
                ""
            }
        ),
        PlanOutcome::NeedsFlags => format!(
            "the plan needs flags; review the steps that name them, then re-plan with: {}",
            plan_command(
                command,
                &options_with_flags(&command.options, &plan.required_flags),
                plan.target.path.as_deref()
            )
        ),
        PlanOutcome::Blocked => {
            "blocked: resolve the errors above; nothing can be applied".to_owned()
        }
    }
}

/// The text form of a provisioning plan: header, target, steps with their safety class and
/// flag, flags, objects, every guarantee status, issues, drift, impact and rollback.
fn format_provision_plan(plan: &ProvisionPlan, options: &ProvisionOptions) -> String {
    let mut lines = vec![
        format!(
            "provisioning plan {} outcome={} auto_approvable={} surface={}",
            plan.plan_id,
            slug(&plan.outcome),
            plan.auto_approvable,
            plan.surface.slug()
        ),
        format!(
            "target {} path={} storage_profile={} schema_version={}",
            slug(&plan.target.kind),
            plan.target.path.as_deref().unwrap_or("(in memory)"),
            plan.target
                .storage_profile
                .map_or_else(|| "unknown".to_owned(), |profile| format!("{profile:?}")),
            plan.target
                .schema_version
                .map_or_else(|| "unknown".to_owned(), |version| version.to_string())
        ),
        format!(
            "spec {} profile={}",
            plan.spec_digest,
            plan.resolved_profile
                .as_ref()
                .map_or("unresolved", |profile| profile.slug.as_str())
        ),
    ];
    if !plan.validation.valid {
        lines.push(format!(
            "validation valid=false status={:?}",
            plan.validation.status
        ));
        lines.extend(
            plan.validation
                .issues
                .iter()
                .map(|issue| format!("  {}", format_issue(issue))),
        );
    }
    lines.extend(format_plan_steps(plan, options));
    if !plan.objects.is_empty() {
        lines.push("objects:".to_owned());
        lines.extend(plan.objects.iter().map(format_planned_object));
    }
    lines.push("guarantees:".to_owned());
    lines.extend(plan.guarantees.iter().map(format_guarantee));
    if !plan.issues.is_empty() {
        lines.push("issues:".to_owned());
        lines.extend(
            plan.issues
                .iter()
                .map(|issue| format!("  {}", format_issue(issue))),
        );
    }
    lines.push(format!("drift in_sync={}", plan.drift.in_sync));
    lines.extend(plan.drift.items.iter().map(format_drift_item));
    lines.push(format!(
        "impact downtime={:?} disk={:?} cpu={:?} risk={:?}",
        plan.impact.downtime, plan.impact.disk, plan.impact.cpu, plan.impact.risk
    ));
    lines.extend(
        plan.impact
            .notes
            .iter()
            .map(|note| format!("  note: {note}")),
    );
    lines.push(format!(
        "rollback possible={}: {}",
        plan.rollback.possible, plan.rollback.description
    ));
    lines.join("\n")
}

/// The steps of a plan, each with its safety class and the flag it needs, and the flags.
fn format_plan_steps(plan: &ProvisionPlan, options: &ProvisionOptions) -> Vec<String> {
    let mut lines = vec![format!("steps ({}):", plan.steps.len())];
    for step in &plan.steps {
        let mut line = format!(
            "  {} {} {} {}",
            step.step_id,
            slug(&step.kind),
            step.path,
            slug(&step.safety)
        );
        if let Some(flag) = &step.requires_flag {
            line.push_str(&format!(" needs {}", cli_flag(flag)));
        }
        if !step.supported {
            line.push_str(" unsupported");
        }
        line.push_str(": ");
        line.push_str(&step.action);
        lines.push(line);
    }
    if plan.required_flags.is_empty() {
        lines.push("flags: none required".to_owned());
    } else {
        let flags = plan
            .required_flags
            .iter()
            .map(|flag| {
                let given = if flag_given(options, flag) {
                    "given"
                } else {
                    "missing"
                };
                format!("{} ({given})", cli_flag(flag))
            })
            .collect::<Vec<_>>();
        lines.push(format!("flags: {}", flags.join(", ")));
    }
    lines
}

fn format_planned_object(object: &provision::PlannedObject) -> String {
    let mut line = format!(
        "  {} {} {}",
        object.name,
        object.kind.slug(),
        slug(&object.state)
    );
    for diff in &object.diff {
        line.push_str(&format!(
            " {}: expected {} actual {};",
            diff.field, diff.expected, diff.actual
        ));
    }
    line
}

fn format_drift_item(item: &provision::DriftItem) -> String {
    format!(
        "  {} {} expected={:?} actual={:?} blocking={} resolution={}",
        item.path,
        slug(&item.kind),
        item.expected,
        item.actual,
        item.blocking,
        slug(&item.resolution)
    )
}

/// The text form of an apply report.
fn format_provision_report(report: &ProvisionApplyReport, command: &SpecCommand) -> String {
    let mut lines = vec![format!(
        "provisioning {} plan={} data_mutated={}: {}",
        slug(&report.status),
        report.plan_id,
        report.check.data_mutated,
        report.check.message
    )];
    for step in &report.applied_steps {
        lines.push(format!(
            "  {} {}{} {}: {}",
            step.step_id,
            slug(&step.kind),
            step.object
                .as_deref()
                .map_or_else(String::new, |object| format!(" {object}")),
            slug(&step.outcome),
            step.detail
        ));
    }
    if let Some(record) = &report.record {
        lines.push(format!(
            "record status={} plan={} objects={} roles={} principals={} released={} \
             completed_steps={} applied_by={}",
            slug(&record.status),
            record.plan_id,
            record.objects,
            record.managed_roles,
            record.managed_principals,
            record.released_objects,
            record.completed_steps,
            record.applied_by
        ));
    }
    lines.extend(report.warnings.iter().map(format_issue));
    if report.session_pending_restart {
        lines.push(
            "restart pending: retuned knobs and the storage format upgrade take effect at the \
             next open"
                .to_owned(),
        );
    }
    if report.status == ProvisionOutcome::PartiallyApplied {
        lines.push(format!(
            "step {} failed; the steps before it stay applied and the record says failed: \
             re-run plan ({}) to see the remaining steps, and apply that plan",
            report.failed_step.as_deref().unwrap_or("(unknown)"),
            plan_command(command, &command.options, None)
        ));
    }
    lines.join("\n")
}

fn apply_report_run(
    report: &ProvisionApplyReport,
    command: &SpecCommand,
) -> Result<CliRun, CliError> {
    let message = if command.json() {
        serde_json::to_string_pretty(report)?
    } else {
        format_provision_report(report, command)
    };
    Ok(CliRun::exit(
        message,
        provision_exit_code(ProvisionEnd::Applied(report)),
    ))
}

/// The text form of a provisioning error, with the plan it carries and what to do next.
fn provision_error_text(error: &ProvisionError, command: &SpecCommand) -> String {
    match error {
        ProvisionError::Blocked(plan)
        | ProvisionError::NeedsFlags(plan)
        | ProvisionError::ConfirmationRequired(plan) => format!(
            "{error}\n{}\n{}",
            format_provision_plan(plan, &command.options),
            plan_next_step(plan, command)
        ),
        ProvisionError::InvalidSpec(report) => {
            format!("{error}\n{}", format_validation_report(report))
        }
        ProvisionError::PlanChanged { .. } => format!(
            "{error}\nnothing was applied; re-run {} and confirm the new plan id (the \
             --allow-* and --unmanaged flags are part of the id, so apply with the flags of the \
             plan you reviewed)",
            plan_command(command, &command.options, None)
        ),
        ProvisionError::AlreadyInitialized => format!(
            "{error}\nconverge it with {} and config apply --confirm",
            plan_command(command, &command.options, None)
        ),
        ProvisionError::AuditRequired => format!(
            "{error}: audit is off and the spec keeps it off; set guarantees.audit.enabled to \
             true, and the plan turns audit on first"
        ),
        ProvisionError::Locked(_) => in_use_message(command.db_path.as_deref()),
        ProvisionError::PermissionMissing(_)
        | ProvisionError::RecordUnreadable(_)
        | ProvisionError::StartupRefused(_)
        | ProvisionError::Db(_)
        | ProvisionError::Config(_)
        | ProvisionError::Io(_) => error.to_string(),
    }
}

/// The JSON form of a provisioning error: `status`, `message` and the plan, report or ids it
/// carries.
fn provision_error_json(
    error: &ProvisionError,
    command: &SpecCommand,
) -> Result<serde_json::Value, CliError> {
    let message = match error {
        ProvisionError::Locked(_) => in_use_message(command.db_path.as_deref()),
        other => other.to_string(),
    };
    let mut value = json!({ "status": provision_error_status(error), "message": message });
    match error {
        ProvisionError::Blocked(plan)
        | ProvisionError::NeedsFlags(plan)
        | ProvisionError::ConfirmationRequired(plan) => {
            value["plan"] = serde_json::to_value(plan.as_ref())?;
        }
        ProvisionError::InvalidSpec(report) => {
            value["validation"] = serde_json::to_value(report.as_ref())?;
        }
        ProvisionError::PlanChanged { confirmed, current } => {
            value["confirmed"] = json!(confirmed);
            value["current"] = json!(current);
        }
        _ => {}
    }
    Ok(value)
}

fn provision_error_run(error: &ProvisionError, command: &SpecCommand) -> Result<CliRun, CliError> {
    let message = if command.json() {
        serde_json::to_string_pretty(&provision_error_json(error, command)?)?
    } else {
        provision_error_text(error, command)
    };
    Ok(CliRun::exit(
        message,
        provision_exit_code(ProvisionEnd::Failed(error)),
    ))
}

/// `config plan --spec <file> [--db <path>] [--out <plan.json> [--force]] [flags] [--json]`: a
/// dry run against the database at the target, or against an absent target. It never creates
/// the database file and never writes a database; exit 0 when the plan is ready or a noop, 2
/// when it is blocked, the spec is invalid or flags are missing.
fn run_spec_plan(args: &[String]) -> Result<CliRun, CliError> {
    let command = match SpecCommand::load(args)? {
        Ok(command) => command,
        Err(run) => return Ok(run),
    };
    let out_path = flag_value(args, "--out")?.map(PathBuf::from);
    if let Some(out_path) = &out_path {
        ensure_plan_out_target(out_path, &command, has_flag(args, "--force"))?;
    }
    let target = match open_spec_target(command.db_path.as_deref()) {
        Ok(target) => target,
        Err(refusal) => return refusal_run(&refusal, command.json()),
    };
    let plan = plan_spec_target(&command, &target);
    drop(target);
    let plan = match plan {
        Ok(plan) => plan,
        Err(error) => return provision_error_run(&error, &command),
    };
    if let Some(out_path) = &out_path {
        write_file_replacing(out_path, &serde_json::to_string_pretty(&plan)?)?;
    }
    let message = match (&out_path, command.json()) {
        (Some(out_path), true) => serde_json::to_string_pretty(&json!({
            "plan_id": plan.plan_id,
            "outcome": plan.outcome,
            "auto_approvable": plan.auto_approvable,
            "required_flags": plan.required_flags,
            "path": out_path.display().to_string(),
        }))?,
        (None, true) => serde_json::to_string_pretty(&plan)?,
        (out_path, false) => {
            let mut text = format!(
                "{}\n{}",
                format_provision_plan(&plan, &command.options),
                plan_next_step(&plan, &command)
            );
            if let Some(out_path) = out_path {
                text.push_str(&format!(
                    "\nwrote plan {} to {}",
                    plan.plan_id,
                    out_path.display()
                ));
            }
            text
        }
    };
    Ok(CliRun::exit(
        message,
        provision_exit_code(ProvisionEnd::Planned(&plan)),
    ))
}

/// `config plan --spec --out` is a dry run, so it never writes over the spec or the database,
/// even with `--force`, and it replaces another existing file only with `--force`, like
/// `config export --out`.
fn ensure_plan_out_target(
    out_path: &Path,
    command: &SpecCommand,
    force: bool,
) -> Result<(), CliError> {
    let inputs = [
        ("spec", Some(command.spec_path.as_path())),
        ("database", command.db_path.as_deref()),
    ];
    for (what, input) in inputs {
        if input.is_some_and(|input| same_file_path(input, out_path)) {
            return Err(CliError::Usage(format!(
                "refusing to write the plan over the {what} {}; choose another --out",
                out_path.display()
            )));
        }
    }
    if !force && std::fs::symlink_metadata(out_path).is_ok() {
        return Err(CliError::Usage(format!(
            "refusing to overwrite {}; pass --force to replace it",
            out_path.display()
        )));
    }
    Ok(())
}

/// Whether `left` and `right` name the same file: by their canonical paths when both exist,
/// by their absolute spellings when neither does.
fn same_file_path(left: &Path, right: &Path) -> bool {
    match (std::fs::canonicalize(left), std::fs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        (Err(_), Err(_)) => match (std::path::absolute(left), std::path::absolute(right)) {
            (Ok(left), Ok(right)) => left == right,
            _ => left == right,
        },
        _ => false,
    }
}

/// `--confirm <prov_id>` or `--auto-approve`, never both; `None` without either.
fn apply_confirmation(args: &[String]) -> Result<Option<Confirmation>, CliError> {
    match (
        flag_value(args, "--confirm")?,
        has_flag(args, "--auto-approve"),
    ) {
        (Some(_), true) => Err(CliError::Usage(
            "use either --confirm <prov_id> or --auto-approve, not both".to_owned(),
        )),
        (Some(id), false) => Ok(Some(Confirmation::PlanId(id))),
        (None, true) => Ok(Some(Confirmation::AutoApproveAdditive)),
        (None, false) => Ok(None),
    }
}

/// `config apply --spec <file> [--db <path>] (--confirm <prov_id> | --auto-approve) [flags]
/// [--json]`: recomputes the plan and applies it as the local operator `local:cli`. A missing
/// database file is created with `provision_new_database`; an existing one is opened with
/// `open_database_with_ops` and converged with `apply_provision`. Without a confirmation a noop
/// exits 0 and any other plan is printed with exit 2.
fn run_spec_apply(args: &[String]) -> Result<CliRun, CliError> {
    let confirmation = apply_confirmation(args)?;
    let command = match SpecCommand::load(args)? {
        Ok(command) => command,
        Err(run) => return Ok(run),
    };
    let target = match open_spec_target(command.db_path.as_deref()) {
        Ok(target) => target,
        Err(refusal) => return refusal_run(&refusal, command.json()),
    };
    let Some(confirmation) = confirmation else {
        let plan = plan_spec_target(&command, &target);
        drop(target);
        return match plan {
            Ok(plan) => unconfirmed_apply_run(&plan, &command),
            Err(error) => provision_error_run(&error, &command),
        };
    };
    let env = cli_environment();
    let result = match target {
        // Without a path the CLI plan is always blocked: a durable profile needs a path, and a
        // volatile one is refused on the CLI.
        SpecTarget::NoPath => match plan_spec_target(&command, &SpecTarget::NoPath) {
            Ok(plan) if plan.outcome == PlanOutcome::Blocked => {
                Err(ProvisionError::Blocked(Box::new(plan)))
            }
            Ok(_) => {
                return Err(CliError::Usage(
                    "missing --db; name the database file, or set deployment.storage_path"
                        .to_owned(),
                ));
            }
            Err(error) => Err(error),
        },
        SpecTarget::Absent(path) => provision::provision_new_database(
            &command.spec,
            Some(&path),
            &confirmation,
            &command.options,
            &env,
        )
        .map(|(_db, report)| report),
        SpecTarget::Existing { mut db, .. } => provision::apply_provision(
            &mut db,
            &ProvisionActor::local_operator(CLI_OPERATOR),
            &command.spec,
            &confirmation,
            &command.options,
            &env,
        ),
    };
    match result {
        Ok(report) => apply_report_run(&report, &command),
        Err(error) => provision_error_run(&error, &command),
    }
}

/// `config apply --spec` without `--confirm` or `--auto-approve`: nothing is applied.
fn unconfirmed_apply_run(plan: &ProvisionPlan, command: &SpecCommand) -> Result<CliRun, CliError> {
    if plan.outcome == PlanOutcome::Noop {
        return message_run(
            "noop",
            format!(
                "nothing to apply: the database already matches the spec (plan {})",
                plan.plan_id
            ),
            0,
            command.json(),
        );
    }
    let (status, reason) = if plan.outcome == PlanOutcome::Blocked {
        ("blocked", "nothing was applied: the plan is blocked")
    } else {
        (
            "confirmation_required",
            "nothing was applied: config apply --spec needs --confirm <prov_id> or \
             --auto-approve",
        )
    };
    let message = if command.json() {
        serde_json::to_string_pretty(&json!({
            "status": status,
            "message": reason,
            "plan": plan,
        }))?
    } else {
        format!(
            "{}\n{reason}\n{}",
            format_provision_plan(plan, &command.options),
            plan_next_step(plan, command)
        )
    };
    Ok(CliRun::exit(message, 2))
}

/// `config drift --spec <file> [--db <path>] [--unmanaged ignore|report|fail] [--json]`: how the
/// database differs from the spec. Exit 0 in sync, 3 drift (including a blocked plan or a
/// missing database), 2 for an invalid spec or a spec without a database path. Unmanaged
/// objects under `report` (the default) or `ignore` are information only; `--unmanaged fail`
/// turns them into drift.
fn run_config_drift(args: &[String]) -> Result<CliRun, CliError> {
    reject_spec_mixing(args)?;
    let command = match SpecCommand::load(args)? {
        Ok(command) => command,
        Err(run) => return Ok(run),
    };
    let target = match open_spec_target(command.db_path.as_deref()) {
        Ok(target) => target,
        Err(refusal) => return refusal_run(&refusal, command.json()),
    };
    let plan = plan_spec_target(&command, &target);
    drop(target);
    let plan = match plan {
        Ok(plan) => plan,
        Err(error) => return provision_error_run(&error, &command),
    };
    let exit_code = provision_exit_code(ProvisionEnd::Drifted(&plan));
    let issues = plan
        .issues
        .iter()
        .filter(|issue| issue.severity != ValidationSeverity::Advice)
        .collect::<Vec<_>>();
    let message = if !plan.validation.valid {
        if command.json() {
            serde_json::to_string_pretty(&plan.validation)?
        } else {
            format_validation_report(&plan.validation)
        }
    } else if command.json() {
        serde_json::to_string_pretty(&json!({
            "in_sync": plan.drift.in_sync,
            "items": plan.drift.items,
            "plan_id": plan.plan_id,
            "outcome": plan.outcome,
            "issues": issues,
        }))?
    } else {
        let mut lines = vec![format!(
            "drift in_sync={} items={} plan={} outcome={}",
            plan.drift.in_sync,
            plan.drift.items.len(),
            plan.plan_id,
            slug(&plan.outcome)
        )];
        lines.extend(plan.drift.items.iter().map(format_drift_item));
        lines.extend(issues.iter().map(|issue| format_issue(issue)));
        lines.join("\n")
    };
    Ok(CliRun::exit(message, exit_code))
}

/// An initialized database opened for `config status` and `config export`.
type OpenedTarget = (Box<Database>, Box<ProbedMetadata>);

/// Opens the database `--db` names for `config status` and `config export`: a missing, locked
/// or unreadable database is exit 1.
fn open_existing_target(path: &Path, json: bool) -> Result<Result<OpenedTarget, CliRun>, CliError> {
    match open_spec_target(Some(path)) {
        Ok(SpecTarget::Existing { db, metadata }) => Ok(Ok((db, metadata))),
        Ok(SpecTarget::Absent(_) | SpecTarget::NoPath) => Ok(Err(message_run(
            "not_found",
            format!("no database at {}", path.display()),
            1,
            json,
        )?)),
        Err(refusal) => Ok(Err(refusal_run(&refusal, json)?)),
    }
}

/// `config status --db <path> [--json]`: the provision record, the last 20 history entries, the
/// profile manifest and the drift of the live database against the spec the record applied.
fn run_config_status(args: &[String]) -> Result<CliRun, CliError> {
    let path = PathBuf::from(required_value(args, "--db")?);
    let output = output_format(args)?;
    let json_output = output == "json";
    let (db, metadata) = match open_existing_target(&path, json_output)? {
        Ok(opened) => opened,
        Err(run) => return Ok(run),
    };
    let record = db.provision_state()?;
    let history = db.provision_history()?;
    let history_total = history.len();
    let recent = &history[history_total.saturating_sub(STATUS_HISTORY_ENTRIES)..];
    let drift = record
        .as_ref()
        .map(|record| {
            provision::plan_provision(
                &record.applied_spec,
                ProvisionTargetRef::Open(&db),
                &ProvisionOptions::default(),
                &cli_environment(),
            )
            .map(|plan| plan.drift)
        })
        .transpose()
        .map_err(|error| CliError::Runtime(format!("drift against the applied spec: {error}")))?;
    drop(db);

    let message = if json_output {
        serde_json::to_string_pretty(&json!({
            "path": path.display().to_string(),
            "provisioned": record.is_some(),
            "record": record,
            "history": recent,
            "history_total": history_total,
            "metadata": metadata,
            "drift": drift,
        }))?
    } else {
        let mut lines = vec![format!("database {}", path.display())];
        match &record {
            Some(record) => lines.extend(format_provision_record(record)),
            None => lines.push(
                "not provisioned: no provision record (config apply --spec writes one)".to_owned(),
            ),
        }
        lines.push(format!(
            "history (last {} of {history_total}):",
            recent.len()
        ));
        lines.extend(recent.iter().map(|entry| {
            format!(
                "  {} {} {} by {} flags={}",
                entry.at_millis,
                entry.plan_id,
                slug(&entry.status),
                entry.actor,
                list_or_none(&entry.flags)
            )
        }));
        lines.push(format_metadata_probe(
            &path,
            &MetadataProbe::Initialized(metadata),
        )?);
        if let Some(drift) = &drift {
            lines.push(format!(
                "drift against the applied spec: in_sync={} items={}",
                drift.in_sync,
                drift.items.len()
            ));
            lines.extend(drift.items.iter().map(format_drift_item));
        }
        lines.join("\n")
    };
    Ok(CliRun::success(message))
}

/// One guarantee line of a plan or a provision record: the path, what the spec asks for, how
/// it is enforced and what to do about it.
fn format_guarantee(guarantee: &provision::GuaranteeEnforcement) -> String {
    let mut line = format!(
        "  {}={} {}: {}",
        guarantee.path,
        guarantee.requested,
        slug(&guarantee.status),
        guarantee.detail
    );
    if !guarantee.remediation.is_empty() {
        line.push_str(" remediation: ");
        line.push_str(&guarantee.remediation);
    }
    line
}

/// The text form of a provision record for `config status`.
fn format_provision_record(record: &provision::ProvisionRecord) -> Vec<String> {
    let objects = record
        .objects
        .iter()
        .map(|(name, object)| {
            format!(
                "{name} ({}{})",
                object.kind.slug(),
                if object.adopted { ", adopted" } else { "" }
            )
        })
        .collect::<Vec<_>>();
    let mut lines = vec![
        format!(
            "provisioned: status={} plan={} spec={} applied_by={} applied_at_millis={} binary={}",
            slug(&record.status),
            record.plan_id,
            record.spec_digest,
            record.applied_by,
            record.applied_at_millis,
            record.binary_version
        ),
        format!(
            "managed objects ({}): {}",
            objects.len(),
            list_or_none(&objects)
        ),
        format!(
            "managed roles: {}; managed principals: {}; released objects: {}",
            list_or_none(&record.managed_roles),
            list_or_none(&record.managed_principals),
            list_or_none(&record.released_objects)
        ),
        format!(
            "completed steps: {}; failed step: {}; flags: {}",
            record.completed_steps.len(),
            record.failed_step.as_deref().unwrap_or("none"),
            list_or_none(&record.flags)
        ),
    ];
    lines.push("guarantees:".to_owned());
    lines.extend(record.guarantees.iter().map(format_guarantee));
    lines
}

/// `a, b, c`, or `none`.
fn list_or_none<'a, I>(items: I) -> String
where
    I: IntoIterator<Item = &'a String>,
{
    let items = items.into_iter().map(String::as_str).collect::<Vec<_>>();
    if items.is_empty() {
        "none".to_owned()
    } else {
        items.join(", ")
    }
}

/// `config export --db <path> [--format yaml|json] [--out <file>] [--force]`: the database as a
/// spec (`provision::export_spec`): the spec the provision record applied, else a version 2 spec
/// inferred from the catalog, the profile manifest and the RBAC state. Without `--out` the spec
/// goes to stdout and the notes to stderr; `--out` refuses to replace a file without `--force`.
/// A spec over `spec_io::MAX_SPEC_BYTES`, which multidb could not read back, exits 2 with
/// `SPEC_TOO_LARGE` and is written nowhere, as in `config upgrade`.
fn run_config_export(args: &[String]) -> Result<CliRun, CliError> {
    let path = PathBuf::from(required_value(args, "--db")?);
    let out_path = flag_value(args, "--out")?.map(PathBuf::from);
    let format = match flag_value(args, "--format")? {
        Some(format) if format == "yaml" || format == "json" => format,
        Some(other) => {
            return Err(CliError::Usage(format!(
                "unsupported --format {other}; expected yaml or json"
            )));
        }
        None => match out_path.as_deref().map(spec_io::spec_format_for_path) {
            Some(SpecFormat::Json) => "json".to_owned(),
            Some(SpecFormat::Yaml) | None => "yaml".to_owned(),
        },
    };
    if let Some(out_path) = &out_path {
        ensure_export_out_target(out_path, &path, has_flag(args, "--force"))?;
    }
    let (db, _) = match open_existing_target(&path, false)? {
        Ok(opened) => opened,
        Err(run) => return Ok(run),
    };
    let exported = provision::export_spec(&db);
    drop(db);
    let mut exported = match exported {
        Ok(exported) => exported,
        Err(error) => {
            return Ok(CliRun::exit(
                error.to_string(),
                provision_exit_code(ProvisionEnd::Failed(&error)),
            ));
        }
    };
    point_storage_path_at(&mut exported, &path, out_path.as_deref());
    let content = serialize_spec(&exported.spec, &format)?;
    // A spec the Rust API applied, or one inferred from a large catalog, can serialize past
    // the size limit; `config plan|apply --spec` would refuse to read it back.
    if let Some(refused) = refuse_oversized_spec(&content, "text", out_path.as_deref())? {
        return Ok(refused);
    }
    let Some(out_path) = out_path else {
        // stdout carries only the spec, so it can be redirected into a file.
        for note in &exported.notes {
            eprintln!("note: {note}");
        }
        return Ok(CliRun::success(content.trim_end_matches('\n')));
    };
    write_file_replacing(&out_path, &content)?;
    let mut lines = vec![format!(
        "exported spec path={} source={} format={format} version={} profile={}",
        out_path.display(),
        slug(&exported.source),
        exported.spec.version,
        exported.spec.profile
    )];
    lines.extend(exported.notes.iter().map(|note| format!("note: {note}")));
    Ok(CliRun::success(lines.join("\n")))
}

/// Refuses an export `--out` that names the database being exported or any other multidb
/// database, even with `--force`, and any other existing file without `--force`.
fn ensure_export_out_target(out_path: &Path, db_path: &Path, force: bool) -> Result<(), CliError> {
    if same_file_path(db_path, out_path) {
        return Err(CliError::Usage(format!(
            "refusing to write the spec over the database {}; choose another --out",
            out_path.display()
        )));
    }
    if matches!(
        inspect_database_metadata(out_path),
        MetadataProbe::Initialized(_) | MetadataProbe::Locked | MetadataProbe::NeedsRecovery
    ) {
        return Err(CliError::Usage(format!(
            "refusing to write the spec over {}, which holds a multidb database; choose another \
             --out",
            out_path.display()
        )));
    }
    if !force && std::fs::symlink_metadata(out_path).is_ok() {
        return Err(CliError::Usage(format!(
            "refusing to overwrite {}; pass --force to replace it",
            out_path.display()
        )));
    }
    Ok(())
}

/// Points `deployment.storage_path` of an exported spec at the exported database the way `config
/// plan|apply --spec` and `serve` resolve it: relative to the directory of `--out`, or the
/// canonical absolute path of the database for stdout (or when no relative path exists, as
/// across Windows drives). `export_spec` returns the path the database was opened with, or the
/// one the applied spec named relative to its own file, so neither would find the database
/// from the exported file.
fn point_storage_path_at(exported: &mut ExportedSpec, db_path: &Path, out_path: Option<&Path>) {
    let database = absolute_path(db_path);
    let relative = out_path.and_then(|out| {
        let dir = std::fs::canonicalize(parent_dir(out)).ok()?;
        relative_path(&dir, &database)
    });
    let storage_path = relative
        .as_deref()
        .unwrap_or(&database)
        .to_string_lossy()
        .into_owned();
    if exported.spec.deployment.storage_path.as_deref() == Some(storage_path.as_str()) {
        return;
    }
    exported.notes.push(if relative.is_some() {
        format!(
            "deployment.storage_path is {storage_path}: the database relative to the directory \
             of the exported spec, which config plan|apply --spec and multidb serve resolve it \
             against"
        )
    } else {
        format!(
            "deployment.storage_path is the absolute path of the database ({storage_path}), so \
             the spec finds it from any directory"
        )
    });
    exported.spec.deployment.storage_path = Some(storage_path);
}

/// `target` relative to the directory `base`; both are canonical. `None` when they share no
/// root, as paths on two Windows drives do.
fn relative_path(base: &Path, target: &Path) -> Option<PathBuf> {
    let base = base.components().collect::<Vec<_>>();
    let target = target.components().collect::<Vec<_>>();
    let common = base
        .iter()
        .zip(&target)
        .take_while(|(left, right)| left == right)
        .count();
    let rooted = |components: &[std::path::Component<'_>]| {
        components
            .iter()
            .take_while(|component| {
                matches!(
                    component,
                    std::path::Component::Prefix(_) | std::path::Component::RootDir
                )
            })
            .count()
    };
    if common == 0 || common < rooted(&base) || common < rooted(&target) {
        return None;
    }
    let mut relative = PathBuf::new();
    for _ in common..base.len() {
        relative.push("..");
    }
    for component in &target[common..] {
        relative.push(component);
    }
    Some(relative)
}

/// `init --apply [--db <path>] [--confirm <prov_id>]`.
struct InitApply {
    db: Option<PathBuf>,
    confirm: Option<String>,
}

/// The `--apply` options of `init`; `--db` and `--confirm` need `--apply`.
fn init_apply_options(args: &[String]) -> Result<Option<InitApply>, CliError> {
    let db = flag_value(args, "--db")?.map(PathBuf::from);
    let confirm = flag_value(args, "--confirm")?;
    if !has_flag(args, "--apply") {
        if db.is_some() || confirm.is_some() {
            return Err(CliError::Usage(
                "--db and --confirm belong to init --apply, which provisions the database"
                    .to_owned(),
            ));
        }
        return Ok(None);
    }
    Ok(Some(InitApply { db, confirm }))
}

/// What `init --apply` did: an exit code and the text and JSON to add to the init summary.
struct InitProvision {
    db_path: PathBuf,
    exit_code: u8,
    text: String,
    json: serde_json::Value,
}

impl InitProvision {
    /// Adds the database path and the provisioning result to the init summary.
    fn extend_json(&self, summary: &mut serde_json::Value) {
        summary["database"] = json!(self.db_path.display().to_string());
        summary["provision"] = self.json.clone();
    }

    fn extend_text(&self, message: &mut String) {
        message.push_str(&format!("\ndatabase path={}\n", self.db_path.display()));
        message.push_str(&self.text);
    }

    /// The init run with the provisioning exit code when provisioning did not succeed.
    fn finish(provision: Option<&Self>, run: CliRun) -> CliRun {
        match provision {
            Some(provision) if provision.exit_code != 0 => {
                CliRun::exit(run.message, provision.exit_code)
            }
            _ => run,
        }
    }
}

/// Provisions a new database from the spec `init` just wrote, at `--db`, else the spec
/// `storage_path` relative to `out_dir`, else `<out_dir>/<sanitized name>.redb`. An existing
/// database is refused (exit 2; `config apply` converges it). The plan is auto-approved unless
/// `--confirm` names its id; a blocked or not auto-approvable plan leaves the written files and
/// exits 2.
fn init_provision(
    apply: &InitApply,
    spec: &DatabaseSpec,
    spec_path: &Path,
    out_dir: &Path,
    output: &str,
) -> Result<InitProvision, CliError> {
    let db_path = apply
        .db
        .clone()
        .or_else(|| {
            spec.deployment
                .storage_path
                .as_deref()
                .map(|storage_path| resolve_against(out_dir, storage_path))
        })
        .unwrap_or_else(|| out_dir.join(storage_file_name(&spec.name)));
    let command = SpecCommand {
        spec_path: spec_path.to_path_buf(),
        spec: spec.clone(),
        db_path: Some(db_path.clone()),
        options: ProvisionOptions::default(),
        output: output.to_owned(),
    };
    let existing = |detail: String| {
        Some((
            "already_initialized",
            format!(
                "{detail}; init --apply only creates new databases, so converge an existing one \
                 with {} and config apply --confirm",
                plan_command(&command, &command.options, None)
            ),
            2,
        ))
    };
    let refusal = match inspect_database_metadata(&db_path) {
        MetadataProbe::NotFound | MetadataProbe::Uninitialized => None,
        MetadataProbe::Locked => Some(("in_use", in_use_message(Some(&db_path)), 1)),
        MetadataProbe::Initialized(_) | MetadataProbe::NeedsRecovery => existing(format!(
            "a database already exists at {}",
            db_path.display()
        )),
        MetadataProbe::Unreadable { reason } => existing(format!(
            "{} exists and is not an empty database ({reason})",
            db_path.display()
        )),
    };
    if let Some((status, message, exit_code)) = refusal {
        return Ok(InitProvision {
            db_path,
            exit_code,
            json: json!({ "status": status, "message": message }),
            text: message,
        });
    }
    let confirmation = apply
        .confirm
        .clone()
        .map_or(Confirmation::AutoApproveAdditive, Confirmation::PlanId);
    let result = provision::provision_new_database(
        spec,
        Some(&db_path),
        &confirmation,
        &command.options,
        &cli_environment(),
    )
    .map(|(_db, report)| report);
    Ok(match &result {
        Ok(report) => InitProvision {
            db_path,
            exit_code: provision_exit_code(ProvisionEnd::Applied(report)),
            text: format_provision_report(report, &command),
            json: serde_json::to_value(report)?,
        },
        Err(error) => InitProvision {
            db_path,
            exit_code: provision_exit_code(ProvisionEnd::Failed(error)),
            text: provision_error_text(error, &command),
            json: provision_error_json(error, &command)?,
        },
    })
}

fn run_explain(args: &[String]) -> Result<CliRun, CliError> {
    let Some(command) = args.first() else {
        return Err(usage());
    };

    match command.as_str() {
        "config" => run_config_explain(&args[1..]),
        _ => Err(usage()),
    }
}

fn run_init(args: &[String]) -> Result<CliRun, CliError> {
    if !has_flag(args, "--guided") {
        return Err(usage());
    }
    let name = required_value(args, "--name")?;
    if name.trim().is_empty() {
        return Err(CliError::Usage(
            "missing --name; provide a non-empty database name".to_owned(),
        ));
    }
    let profile_arg = optional_value(args, "--profile");
    let template_arg = optional_value(args, "--template");
    if profile_arg.is_some() && template_arg.is_some() {
        return Err(CliError::Usage(
            "use either --profile or --template, not both".to_owned(),
        ));
    }
    let customize = has_flag(args, "--override");
    let apply = init_apply_options(args)?;
    if let Some(template_name) = template_arg {
        if customize || has_flag(args, "--profile-name") {
            return Err(CliError::Usage(
                "--override and --profile-name apply to init --guided --profile, not --template"
                    .to_owned(),
            ));
        }
        return run_init_template(args, &template_name, &name, apply.as_ref());
    }
    if !customize && has_flag(args, "--profile-name") {
        return Err(CliError::Usage(
            "--profile-name names the custom profile that --override defines; pass at least one \
             --override <knob>=<value>"
                .to_owned(),
        ));
    }

    let profile_name = profile_arg.ok_or_else(|| {
        CliError::Usage("missing --profile or --template for guided init".to_owned())
    })?;
    let (profile, base) = resolve_built_in_profile(&profile_name)
        .zip(named_profile(&profile_name))
        .ok_or_else(|| {
            CliError::Usage(format!(
                "unknown profile {profile_name}{}; run multidb profile list",
                did_you_mean(&profile_name)
            ))
        })?;
    let (knobs, floors) = parse_knob_overrides(args)?;
    let file_format = init_file_format(args)?;
    let out_path = optional_value(args, "--out")
        .map(PathBuf::from)
        .unwrap_or_else(|| default_init_path(&file_format));
    if out_path.exists() && !has_flag(args, "--force") {
        return Err(CliError::Usage(format!(
            "refusing to overwrite {}; pass --force to replace it",
            out_path.display()
        )));
    }

    let (spec, warning) = if customize {
        let slug = optional_value(args, "--profile-name")
            .unwrap_or_else(|| format!("{}_custom", base.slug));
        let spec = custom_guided_spec(
            profile,
            &base,
            CustomProfileSpec {
                extends: base.slug.clone(),
                description: None,
                knobs,
                floors,
                compatible_roles: RoleSetDelta::default(),
            },
            &slug,
            &name,
        );
        (spec, None)
    } else {
        (
            guided_spec(profile, &name),
            variant_warning(&profile_name, &base, profile),
        )
    };
    let report = GuaranteeValidator::validate(&spec);
    let content = serialize_spec(&spec, &file_format)?;
    std::fs::write(&out_path, content.as_bytes())?;

    let output = output_format(args)?;
    let provision = apply
        .as_ref()
        .map(|apply| init_provision(apply, &spec, &out_path, parent_dir(&out_path), &output))
        .transpose()?;
    let extends = spec
        .custom_profile
        .as_ref()
        .map(|custom| custom.extends.as_str());
    let message = if output == "json" {
        let mut summary = json!({
            "path": out_path.display().to_string(),
            "name": spec.name,
            "profile": spec.profile,
            "status": report.status,
            "valid": report.valid,
        });
        if let Some(extends) = extends {
            summary["version"] = json!(spec.version);
            summary["extends"] = json!(extends);
        }
        if let Some(warning) = &warning {
            summary["warnings"] = json!([warning.message]);
            summary["variant"] = json!(warning.variant);
            summary["storage_profile"] = json!(warning.storage_profile);
        }
        if let Some(provision) = &provision {
            provision.extend_json(&mut summary);
        }
        serde_json::to_string_pretty(&summary)?
    } else {
        let mut message = format!(
            "created config path={} name={} profile={} status={:?} valid={}",
            out_path.display(),
            spec.name,
            spec.profile,
            report.status,
            report.valid
        );
        if let Some(extends) = extends {
            message.push_str(&format!(" version={} extends={extends}", spec.version));
        }
        if let Some(warning) = &warning {
            message.push_str("\nwarning: ");
            message.push_str(&warning.message);
        }
        if let Some(provision) = &provision {
            provision.extend_text(&mut message);
        }
        message
    };

    Ok(InitProvision::finish(
        provision.as_ref(),
        CliRun::validation(message, report.valid),
    ))
}

/// Warns that a version 1 guided spec records the product slug, so an alias that names a
/// non-canonical variant (for example `high_durability`) does not select that variant.
struct VariantWarning {
    message: String,
    /// The variant the written spec resolves to: the product's canonical one.
    variant: &'static str,
    storage_profile: Profile,
}

fn variant_warning(
    typed: &str,
    base: &ResolvedProfile,
    product: &ProfileSpec,
) -> Option<VariantWarning> {
    let canonical = product_variants(product.slug).find(|variant| variant.canonical)?;
    if canonical.name == base.variant {
        return None;
    }
    Some(VariantWarning {
        message: format!(
            "profile {typed} is the {} variant (storage profile {:?}) of {}, but a version 1 \
             spec records the product slug {}, whose canonical variant is {} (storage profile \
             {:?}); pass --override <knob>=<value> to write a version 2 custom_profile that \
             extends {}",
            base.variant,
            base.storage_profile,
            product.slug,
            product.slug,
            canonical.name,
            canonical.storage_profile,
            base.variant
        ),
        variant: canonical.name,
        storage_profile: canonical.storage_profile,
    })
}

fn run_init_template(
    args: &[String],
    template_name: &str,
    name: &str,
    apply: Option<&InitApply>,
) -> Result<CliRun, CliError> {
    let format = template_output_format(args)?;
    let materialized = materialize_template(template_name, name, format)?;
    let out_dir = optional_value(args, "--out")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(materialized.template_slug));
    ensure_template_output_dir(&out_dir, has_flag(args, "--force"))?;
    write_template_materialization(&out_dir, &materialized)?;

    let output = output_format(args)?;
    let provision = apply
        .map(|apply| {
            init_provision(
                apply,
                &materialized.spec,
                &out_dir.join(format.spec_file_name()),
                &out_dir,
                &output,
            )
        })
        .transpose()?;
    let files = materialized
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect::<Vec<_>>();
    let message = if output == "json" {
        let mut summary = json!({
            "path": out_dir.display().to_string(),
            "name": materialized.spec.name,
            "template": materialized.template_slug,
            "profile": materialized.spec.profile,
            "status": materialized.validation.status,
            "valid": materialized.validation.valid,
            "files": files,
        });
        if let Some(provision) = &provision {
            provision.extend_json(&mut summary);
        }
        serde_json::to_string_pretty(&summary)?
    } else {
        let mut message = format!(
            "created template path={} name={} template={} profile={} status={:?} valid={} files={}",
            out_dir.display(),
            materialized.spec.name,
            materialized.template_slug,
            materialized.spec.profile,
            materialized.validation.status,
            materialized.validation.valid,
            files.join(",")
        );
        if let Some(provision) = &provision {
            provision.extend_text(&mut message);
        }
        message
    };

    Ok(InitProvision::finish(
        provision.as_ref(),
        CliRun::validation(message, materialized.validation.valid),
    ))
}

fn run_profile(args: &[String]) -> Result<CliRun, CliError> {
    let Some(command) = args.first() else {
        return Err(usage());
    };
    match command.as_str() {
        "list" => run_profile_list(args),
        "show" => run_profile_show(&args[1..]),
        "explain" => run_profile_explain(&args[1..]),
        "knobs" => run_profile_knobs(&args[1..]),
        _ => Err(usage()),
    }
}

fn run_profile_list(args: &[String]) -> Result<CliRun, CliError> {
    let message = if output_format(args)? == "json" {
        let profiles = built_in_profiles()
            .iter()
            .map(|profile| {
                json!({
                    "slug": profile.slug,
                    "aliases": profile.aliases,
                    "status": profile.status,
                    "description": profile.description,
                    "default_domain": domain_slug(profile.default_domain),
                    "compatible_roles": profile
                        .compatible_roles
                        .iter()
                        .map(|role| role_slug(*role))
                        .collect::<Vec<_>>(),
                    "variants": product_variants(profile.slug)
                        .map(|variant| {
                            json!({
                                "name": variant.name,
                                "storage_profile": variant.storage_profile,
                                "canonical": variant.canonical,
                            })
                        })
                        .collect::<Vec<_>>(),
                    "knobs": named_profile(profile.slug).map(|resolved| resolved.knobs),
                })
            })
            .collect::<Vec<_>>();
        serde_json::to_string_pretty(&profiles)?
    } else {
        format_profiles()
    };
    Ok(CliRun::success(message))
}

/// `profile show <name> [--override path=value]...` resolves a built-in slug or alias as a
/// what-if; `profile show --db <path>` reports the profile persisted in a database. The name
/// may stand anywhere among the flags.
fn run_profile_show(args: &[String]) -> Result<CliRun, CliError> {
    let output = output_format(args)?;
    let names = positional_args(args, &["--db", "--output", "--override"]);
    if let [first, second, ..] = names.as_slice() {
        return Err(CliError::Usage(format!(
            "profile show takes one profile name, found {first} and {second}"
        )));
    }
    let name = names.first().copied();
    match (name, optional_value(args, "--db")) {
        (Some(_), Some(_)) => Err(CliError::Usage(
            "use either a profile name or --db with profile show, not both".to_owned(),
        )),
        (None, Some(db)) => {
            if has_flag(args, "--override") {
                return Err(CliError::Usage(
                    "--override applies to profile show <profile>, not to --db".to_owned(),
                ));
            }
            run_profile_show_db(Path::new(&db), &output)
        }
        (Some(name), None) => {
            let (knobs, floors) = parse_knob_overrides(args)?;
            let resolution = resolve_what_if(name, &knobs, floors);
            resolution_run(&resolution, &output)
        }
        (None, None) => Err(CliError::Usage(
            "missing profile name or --db; run multidb profile list".to_owned(),
        )),
    }
}

fn run_profile_show_db(path: &Path, output: &str) -> Result<CliRun, CliError> {
    let probe = inspect_database_metadata(path);
    let initialized = matches!(probe, MetadataProbe::Initialized(_));
    let message = if output == "json" {
        serde_json::to_string_pretty(&probe)?
    } else {
        format_metadata_probe(path, &probe)?
    };
    Ok(if initialized {
        CliRun::success(message)
    } else {
        CliRun::failure(message)
    })
}

/// `profile explain --spec <file>`: the resolver result for a spec. Exit 2 when the profile is
/// invalid, including a version 1 spec that sets `custom_profile`; a version 1 spec whose
/// profile name is not built in is reported as unresolved, with exit 0.
fn run_profile_explain(args: &[String]) -> Result<CliRun, CliError> {
    let spec_path = PathBuf::from(required_value(args, "--spec")?);
    let output = output_format(args)?;
    let spec = match read_spec_file(&spec_path)? {
        Ok(spec) => spec,
        Err(report) => return spec_report_run(&report, &output, None),
    };
    resolution_run(&resolve_profile(&spec), &output)
}

/// `profile knobs`: every `KNOB_REGISTRY` entry, then the reserved knobs.
fn run_profile_knobs(args: &[String]) -> Result<CliRun, CliError> {
    let message = if output_format(args)? == "json" {
        let mut knobs = Vec::with_capacity(KNOB_REGISTRY.len() + RESERVED_KNOBS.len());
        for definition in KNOB_REGISTRY {
            let mut entry = serde_json::to_value(definition)?;
            entry["reserved"] = json!(false);
            knobs.push(entry);
        }
        for reserved in RESERVED_KNOBS {
            knobs.push(json!({
                "path": reserved.path,
                "reserved": true,
                "reason": reserved.reason,
            }));
        }
        serde_json::to_string_pretty(&knobs)?
    } else {
        let mut rows = vec![
            [
                "path",
                "values",
                "timing",
                "enforcement",
                "after creation",
                "sink",
            ]
            .map(str::to_owned)
            .to_vec(),
        ];
        rows.extend(KNOB_REGISTRY.iter().map(|definition| {
            vec![
                definition.path.to_owned(),
                definition.value_type.describe(),
                timing_label(definition.timing).to_owned(),
                enforcement_label(definition.enforcement),
                change_policy_label(definition.after_creation).to_owned(),
                definition.sink.to_owned(),
            ]
        }));
        rows.extend(RESERVED_KNOBS.iter().map(|reserved| {
            vec![
                reserved.path.to_owned(),
                "reserved".to_owned(),
                "-".to_owned(),
                "not supported".to_owned(),
                "-".to_owned(),
                reserved.reason.to_owned(),
            ]
        }));
        align_rows(&rows)
    };
    Ok(CliRun::success(message))
}

/// Resolves a what-if profile. Knob overrides go through `resolve_named_profile`; floors need a
/// version 2 `custom_profile`, so with floors the same custom profile is resolved from a guided
/// spec.
fn resolve_what_if(
    name: &str,
    knobs: &ProfileKnobOverrides,
    floors: FloorOverrides,
) -> ProfileResolution {
    if floors.is_empty() {
        return resolve_named_profile(name, knobs);
    }
    let Some(product) = built_in_profile_normalized(name) else {
        return resolve_named_profile(name, knobs);
    };
    let base = match resolve_named_profile(name, &ProfileKnobOverrides::default()) {
        ProfileResolution::Resolved { profile, .. } => profile,
        other => return other,
    };
    let custom = CustomProfileSpec {
        extends: name.trim().to_owned(),
        description: None,
        knobs: knobs.clone(),
        floors,
        compatible_roles: RoleSetDelta::default(),
    };
    let slug = format!("{}_custom", base.slug);
    resolve_profile(&custom_guided_spec(
        product, &base, custom, &slug, "what_if",
    ))
}

/// Prints a resolver result; exit 2 when it is invalid.
fn resolution_run(resolution: &ProfileResolution, output: &str) -> Result<CliRun, CliError> {
    let message = if output == "json" {
        let value = match resolution {
            ProfileResolution::Resolved { profile, issues } => json!({
                "resolution": "resolved",
                "profile": profile,
                "rows": knob_rows_json(&profile.knobs, Some(&profile.provenance))?,
                "issues": issues,
            }),
            ProfileResolution::Unresolved { reason, issues } => json!({
                "resolution": "unresolved",
                "reason": reason,
                "issues": issues,
            }),
            ProfileResolution::Invalid(issues) => json!({
                "resolution": "invalid",
                "issues": issues,
            }),
        };
        serde_json::to_string_pretty(&value)?
    } else {
        format_resolution(resolution)?
    };
    Ok(CliRun::validation(
        message,
        !matches!(resolution, ProfileResolution::Invalid(_)),
    ))
}

fn run_role(args: &[String]) -> Result<CliRun, CliError> {
    require_list_command(args)?;
    let message = if output_format(args)? == "json" {
        let roles = collection_role_definitions()
            .iter()
            .map(|role| {
                json!({
                    "slug": role.slug,
                    "status": role.status,
                    "description": role.description,
                    "required_capabilities": role.required_capabilities,
                    "constraints": role.constraints,
                })
            })
            .collect::<Vec<_>>();
        serde_json::to_string_pretty(&roles)?
    } else {
        format_roles()
    };
    Ok(CliRun::success(message))
}

fn run_domain(args: &[String]) -> Result<CliRun, CliError> {
    require_list_command(args)?;
    let message = if output_format(args)? == "json" {
        let domains = consistency_domain_definitions()
            .iter()
            .map(|domain| {
                json!({
                    "slug": domain.slug,
                    "status": domain.status,
                    "guarantees": domain.guarantees,
                    "limits": domain.limits,
                })
            })
            .collect::<Vec<_>>();
        serde_json::to_string_pretty(&domains)?
    } else {
        format_domains()
    };
    Ok(CliRun::success(message))
}

fn run_template(args: &[String]) -> Result<CliRun, CliError> {
    let Some(command) = args.first() else {
        return Err(usage());
    };

    match command.as_str() {
        "list" => run_template_list(&args[1..]),
        "explain" => run_template_explain(&args[1..]),
        _ => Err(usage()),
    }
}

fn run_template_list(args: &[String]) -> Result<CliRun, CliError> {
    let message = if output_format(args)? == "json" {
        let templates = built_in_templates()
            .iter()
            .map(|template| {
                json!({
                    "slug": template.slug,
                    "aliases": template.aliases,
                    "title": template.title,
                    "profile": template.profile,
                    "spec_version": TEMPLATE_SPEC_VERSION,
                    "description": template.description,
                    "collections": template
                        .collections
                        .iter()
                        .map(|collection| {
                            json!({
                                "name": collection.name,
                                "role": role_slug(collection.role),
                                "indexes": collection
                                    .indexes
                                    .iter()
                                    .map(|index| template_index_slug(*index))
                                    .collect::<Vec<_>>(),
                                "physical": template.physical(collection.name),
                                "description": collection.description,
                            })
                        })
                        .collect::<Vec<_>>(),
                    "known_limits": template.known_limits,
                })
            })
            .collect::<Vec<_>>();
        serde_json::to_string_pretty(&templates)?
    } else {
        format_templates()
    };
    Ok(CliRun::success(message))
}

fn run_template_explain(args: &[String]) -> Result<CliRun, CliError> {
    let Some(template_name) = args.first() else {
        return Err(CliError::Usage(
            "missing template name; run multidb template list".to_owned(),
        ));
    };
    if template_name.starts_with("--") {
        return Err(CliError::Usage(
            "missing template name; run multidb template list".to_owned(),
        ));
    }
    let name = optional_value(args, "--name").unwrap_or_else(|| template_name.to_owned());
    let materialized = materialize_template(template_name, &name, TemplateOutputFormat::Yaml)?;
    let report = ConfigExplainer::explain(&materialized.spec);
    let message = if output_format(args)? == "json" {
        serde_json::to_string_pretty(&json!({
            "template": materialized.template_slug,
            "spec": materialized.spec,
            "explain": report,
        }))?
    } else {
        let Some(template) = built_in_template(template_name) else {
            return Err(CliError::Usage(format!(
                "unknown template {template_name}; run multidb template list"
            )));
        };
        format!(
            "template={} title={} profile={} spec_version={} valid={} status={:?}\nwhy={}\n{}",
            template.slug,
            template.title,
            materialized.spec.profile,
            materialized.spec.version,
            report.validation.valid,
            report.validation.status,
            template.why_profile,
            format_explain_report(&report)
        )
    };
    Ok(CliRun::validation(message, report.validation.valid))
}

fn run_advice(args: &[String]) -> Result<CliRun, CliError> {
    let Some(command) = args.first() else {
        return Err(usage());
    };
    match command.as_str() {
        "list" => run_advice_list(args),
        "plan" => run_advice_plan(args),
        "reject" => run_advice_reject(args),
        _ => Err(usage()),
    }
}

fn run_advice_list(args: &[String]) -> Result<CliRun, CliError> {
    let database = open_cli_database(args)?;
    let report = database.runtime_advice()?;
    let message = if output_format(args)? == "json" {
        serde_json::to_string_pretty(&report)?
    } else {
        format_runtime_advice_report(&report)
    };
    Ok(CliRun::success(message))
}

fn run_advice_plan(args: &[String]) -> Result<CliRun, CliError> {
    let database = open_cli_database(args)?;
    let advice_id = required_value(args, "--advice-id")?;
    let output = output_format(args)?;
    let plan = database.runtime_advice_plan(&advice_id)?;
    let message = if let Some(out_path) = optional_value(args, "--out") {
        let out_path = PathBuf::from(out_path);
        std::fs::write(&out_path, serde_json::to_string_pretty(&plan)?)?;
        if output == "json" {
            serde_json::to_string_pretty(&json!({
                "advice_id": advice_id,
                "plan_id": plan.plan_id,
                "valid": plan.valid,
                "apply_supported": plan.apply_supported,
                "path": out_path.display().to_string(),
            }))?
        } else {
            format!(
                "wrote advice plan advice_id={} plan_id={} valid={} apply_supported={} path={}",
                advice_id,
                plan.plan_id,
                plan.valid,
                plan.apply_supported,
                out_path.display()
            )
        }
    } else if output == "json" {
        serde_json::to_string_pretty(&plan)?
    } else {
        format_migration_plan(&plan)
    };
    Ok(CliRun::validation(message, plan.valid))
}

fn run_advice_reject(args: &[String]) -> Result<CliRun, CliError> {
    let database = open_cli_database(args)?;
    let advice_id = required_value(args, "--advice-id")?;
    let reason = required_value(args, "--reason")?;
    let output = output_format(args)?;
    let _plan = database.runtime_advice_plan(&advice_id)?;
    let decision = database.record_runtime_advice_decision(
        RuntimeAdviceDecisionRequest {
            advice_id,
            status: RuntimeAdviceStatus::Rejected,
            reason,
        },
        "cli",
    )?;
    let message = if output == "json" {
        serde_json::to_string_pretty(&decision)?
    } else {
        format!(
            "advice_id={} status={:?} suppress_until_millis={:?}",
            decision.advice_id, decision.status, decision.suppress_until_millis
        )
    };
    Ok(CliRun::success(message))
}

fn run_backup(args: &[String]) -> Result<String, CliError> {
    let Some(kind) = args.first() else {
        return Err(usage());
    };
    let db = PathBuf::from(required_value(args, "--db")?);
    let dest = PathBuf::from(required_value(args, "--dest")?);
    let selection = parse_profile_selection(&required_value(args, "--profile")?)?;
    let database = open_database(cli_db_config(selection, db)?)?;
    let config = BackupConfig::default();

    match kind.as_str() {
        "full" => {
            let report = full_backup(&database, dest, &config)?;
            Ok(format!(
                "full backup {} lsn={} path={}",
                report.manifest.backup_id,
                report.manifest.end_lsn,
                report.path.display()
            ))
        }
        "incr" | "incremental" => {
            let parent = PathBuf::from(required_value(args, "--parent")?);
            let report = incremental_backup(&database, dest, parent, &config)?;
            Ok(format!(
                "incremental backup {} lsn={}..{} path={}",
                report.manifest.backup_id,
                report.manifest.start_lsn,
                report.manifest.end_lsn,
                report.path.display()
            ))
        }
        _ => Err(usage()),
    }
}

fn run_restore(args: &[String]) -> Result<String, CliError> {
    let backup = PathBuf::from(required_value(args, "--backup")?);
    let dest_db = PathBuf::from(required_value(args, "--dest-db")?);
    let ProfileSelection::Fixed(profile) =
        parse_profile_selection(&required_value(args, "--profile")?)?
    else {
        return Err(CliError::Usage(
            "restore does not accept --profile auto: the restore target must be absent or \
             empty, so there is no database to inspect; pass the storage profile the backed-up \
             database was created with"
                .to_owned(),
        ));
    };
    let target = match optional_value(args, "--to-lsn") {
        Some(value) => RestoreTarget::Lsn(
            value
                .parse()
                .map_err(|error| CliError::Usage(format!("invalid --to-lsn: {error}")))?,
        ),
        None => RestoreTarget::Latest,
    };
    let allow_unmet_floors = has_flag(args, "--allow-unmet-floors");

    let report = restore_backup_with_config(
        &backup,
        DbConfig::on_disk(profile, &dest_db),
        target,
        &RestoreConfig::default().with_unmet_profile_floors_allowed(allow_unmet_floors),
    )?;
    let mut lines = vec![format!(
        "restored timeline={} lsn={} applied_commits={}",
        report.timeline_id, report.restored_lsn, report.applied_commits
    )];
    // Without the flag, a restore that would miss a floor is refused.
    if allow_unmet_floors
        && let MetadataProbe::Initialized(metadata) = inspect_database_metadata(&dest_db)
    {
        lines.extend(
            metadata
                .unmet_floors
                .iter()
                .map(|floor| unmet_floor_warning(floor)),
        );
    }
    Ok(lines.join("\n"))
}

fn run_verify(args: &[String]) -> Result<String, CliError> {
    let backup = PathBuf::from(required_value(args, "--backup")?);
    let report = verify_backup(backup)?;
    Ok(format!(
        "verified backup {} lsn={} files={}",
        report.manifest.backup_id, report.restored_lsn, report.checked_files
    ))
}

fn run_list(args: &[String]) -> Result<String, CliError> {
    let root = PathBuf::from(required_value(args, "--backup-root")?);
    let manifests = list_backups(root)?;
    let mut lines = Vec::new();
    for manifest in manifests {
        lines.push(format!(
            "{} {:?} lsn={}..{} parent={}",
            manifest.backup_id,
            manifest.kind,
            manifest.start_lsn,
            manifest.end_lsn,
            manifest.parent_backup_id.unwrap_or_else(|| "-".to_owned())
        ));
    }
    if lines.is_empty() {
        Ok("no backups".to_owned())
    } else {
        Ok(lines.join("\n"))
    }
}

fn run_cdc(args: &[String]) -> Result<String, CliError> {
    let Some(command) = args.first() else {
        return Err(usage());
    };
    let database = open_cli_database(args)?;
    match command.as_str() {
        "poll" => {
            let token = resume_token(args)?;
            let filter = changefeed_filter(args);
            let include_system = has_flag(args, "--include-system");
            let max = optional_value(args, "--max")
                .map(|value| value.parse())
                .transpose()
                .map_err(|error| CliError::Usage(format!("invalid --max: {error}")))?
                .unwrap_or(1_024);
            let (events, next) = database.poll_changefeed(
                &token,
                &filter,
                &ChangefeedOptions { include_system },
                max,
            )?;
            serde_json::to_string_pretty(&serde_json::json!({
                "events": events,
                "next": next,
            }))
            .map_err(Into::into)
        }
        "subscribe" => {
            let name = required_value(args, "--name")?;
            let mut config = SubscriptionConfig::new(name, resume_token(args)?);
            config.filter = changefeed_filter(args);
            if let Some(limit) = optional_value(args, "--buffer-limit") {
                config.buffer_limit = limit
                    .parse()
                    .map_err(|error| CliError::Usage(format!("invalid --buffer-limit: {error}")))?;
            }
            let state = database.create_subscription(config)?;
            serde_json::to_string_pretty(&state).map_err(Into::into)
        }
        "ack" => {
            let name = required_value(args, "--name")?;
            let state = database.ack_subscription(&name, resume_token(args)?)?;
            serde_json::to_string_pretty(&state).map_err(Into::into)
        }
        "state" => {
            let name = required_value(args, "--name")?;
            let state = database.subscription_state(&name)?;
            serde_json::to_string_pretty(&state).map_err(Into::into)
        }
        _ => Err(usage()),
    }
}

fn run_view(args: &[String]) -> Result<String, CliError> {
    let Some(command) = args.first() else {
        return Err(usage());
    };
    let database = open_cli_database(args)?;
    match command.as_str() {
        "create" => {
            let spec_json = required_value(args, "--spec-json")?;
            let spec = serde_json::from_str::<MaterializedViewSpec>(&spec_json)?;
            let rows = database.create_materialized_view(&spec)?;
            serde_json::to_string_pretty(&rows).map_err(Into::into)
        }
        "refresh" => {
            let name = required_value(args, "--name")?;
            let rows = database.refresh_materialized_view_to_current(&name)?;
            serde_json::to_string_pretty(&rows).map_err(Into::into)
        }
        "read" => {
            let name = required_value(args, "--name")?;
            let rows = database.read_materialized_view(&name)?;
            serde_json::to_string_pretty(&rows).map_err(Into::into)
        }
        _ => Err(usage()),
    }
}

fn run_import(args: &[String]) -> Result<String, CliError> {
    let Some(format) = args.first() else {
        return Err(usage());
    };
    let mut database = open_cli_database(args)?;
    let defaults = ImportOptions::default();
    let batch_size = optional_value(args, "--batch-size")
        .map(|value| value.parse())
        .transpose()
        .map_err(|error| CliError::Usage(format!("invalid --batch-size: {error}")))?
        .unwrap_or(defaults.batch_size);
    let options = ImportOptions {
        batch_size,
        strict: has_flag(args, "--strict"),
        resume_token_path: optional_value(args, "--resume-token").map(PathBuf::from),
        reject_path: optional_value(args, "--reject-file").map(PathBuf::from),
    };
    let file = PathBuf::from(required_value(args, "--file")?);
    let input = std::fs::read_to_string(file)?;
    let report = match format.as_str() {
        "csv" => {
            let table = required_value(args, "--table")?;
            import_table_csv(&database, &table, &input, &options)?
        }
        "jsonl" => {
            let table = required_value(args, "--table")?;
            import_table_jsonl(&database, &table, &input, &options)?
        }
        "pg" => import_pg_dump_plain(&mut database, &input, &options)?,
        other => {
            return Err(CliError::Usage(format!(
                "unsupported import format {other}"
            )));
        }
    };
    serde_json::to_string_pretty(&report).map_err(Into::into)
}

fn run_export(args: &[String]) -> Result<String, CliError> {
    let Some(format) = args.first() else {
        return Err(usage());
    };
    let database = open_cli_database(args)?;
    let table = required_value(args, "--table")?;
    let output = match parse_export_format(format)? {
        ExportFormat::Csv => export_table_csv(&database, &table)?,
        ExportFormat::Jsonl => export_table_jsonl(&database, &table)?,
        ExportFormat::Parquet => {
            return Err(CliError::Usage(
                "Parquet export is exposed through the library API in this preview".to_owned(),
            ));
        }
    };
    if let Some(file) = optional_value(args, "--file") {
        std::fs::write(file, output.as_bytes())?;
        Ok(format!("exported {} bytes", output.len()))
    } else {
        Ok(output)
    }
}

fn run_migrate(args: &[String]) -> Result<String, CliError> {
    let Some(command) = args.first() else {
        return Err(usage());
    };
    let database = open_cli_database(args)?;
    match command.as_str() {
        "value-codec" => {
            let report = migrate_value_codec(&database)?;
            serde_json::to_string_pretty(&report).map_err(Into::into)
        }
        other => Err(CliError::Usage(format!(
            "unsupported migrate command {other}"
        ))),
    }
}

fn run_serve(args: &[String]) -> Result<String, CliError> {
    if !args.is_empty() {
        return Err(CliError::Usage(
            "multidb serve is configured through MULTIDB_* environment variables".to_owned(),
        ));
    }

    let config = serve_config_from_env()?;
    let (database, spec_mode) = serve_database_with_spec_mode(&config)?;
    let database = Arc::new(tokio::sync::Mutex::new(database));

    let mut admin_state = multidb::admin::AdminState::from_database_handle(Arc::clone(&database))
        .with_admin_session_ttl_seconds(config.admin_session_ttl_seconds)
        .with_admin_login_rate_limit(config.admin_login_rate_limit)
        .with_studio_assets_dir(config.studio_dir.clone());
    if let Some(spec_mode) = spec_mode {
        admin_state = admin_state.with_spec_mode(spec_mode.spec_digest, spec_mode.apply_mode);
    }
    if let Some(token) = &config.admin_token {
        admin_state = admin_state.with_admin_token(token.clone());
    }
    let admin_password = config.admin_password.clone();
    let admin_password_reset = config.admin_password_reset;

    let tls = match &config.pg_tls {
        ServePgTls::Require { cert, key } => {
            TlsMode::Require(TlsConfig::from_pem_files(cert, key)?)
        }
        ServePgTls::DisabledForLocalDev => TlsMode::DisabledForTests,
    };
    let pg_auth = AuthConfig::single_user(
        config.pg_user.clone(),
        4096,
        ScramCredential::from_password(&config.pg_password, 4096)?,
    );
    let pg_config = NetworkConfig::new(tls, pg_auth);

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        if let Some(password) = admin_password {
            admin_state
                .bootstrap_admin_password(password, admin_password_reset)
                .await
                .map_err(CliError::Runtime)?;
        }
        if !admin_state.is_admin_auth_configured() {
            return Err(CliError::Usage(
                "multidb serve requires MULTIDB_ADMIN_PASSWORD(_FILE), \
                 MULTIDB_ADMIN_TOKEN(_FILE), or an existing stored admin credential"
                    .to_owned(),
            ));
        }
        let admin_router = multidb::admin::router_with_studio(admin_state);
        let admin_listener = tokio::net::TcpListener::bind(config.admin_bind).await?;
        let pg_listener = tokio::net::TcpListener::bind(config.pg_bind).await?;
        let pg_database = Arc::clone(&database);

        let admin_task = tokio::spawn(async move {
            axum::serve(
                admin_listener,
                admin_router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .map_err(CliError::Io)
        });
        let pg_task = tokio::spawn(async move {
            PgServer::serve_shared(pg_listener, pg_database, pg_config)
                .await
                .map_err(CliError::Network)
        });

        tokio::select! {
            result = admin_task => flatten_serve_task(result)?,
            result = pg_task => flatten_serve_task(result)?,
        }

        Ok::<(), CliError>(())
    })?;

    Ok("serve stopped".to_owned())
}

/// [`serve_database_with_spec_mode`] without the spec mode.
#[cfg(test)]
fn serve_database(config: &ServeConfig) -> Result<multidb::db::Database, CliError> {
    serve_database_with_spec_mode(config).map(|(database, _)| database)
}

/// Creates or opens the single-node CP database `serve` runs on. With `MULTIDB_PROFILE=auto`
/// the database must already exist (see `cli_db_config`). In spec mode the spec decides, see
/// `serve_spec_database`, and the spec digest and `MULTIDB_SPEC_APPLY` mode are returned for
/// `GET /provision/state`.
fn serve_database_with_spec_mode(
    config: &ServeConfig,
) -> Result<(multidb::db::Database, Option<multidb::admin::ServeSpecMode>), CliError> {
    if let Some(serve_spec) = &config.spec {
        let (database, spec_digest) = serve_spec_database(config, serve_spec)?;
        let spec_mode = multidb::admin::ServeSpecMode {
            spec_digest,
            apply_mode: serve_spec.mode,
        };
        return Ok((database, Some(spec_mode)));
    }
    // Resolved first, so `auto` on a missing path fails before any directory is created.
    let db_config = cli_db_config(config.profile, config.db_path.clone())?;
    if let Some(parent) = config.db_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    Ok((create_database(db_config)?, None))
}

/// `serve` in spec mode (`MULTIDB_SPEC_PATH`). The spec is read through `spec_io`, validated
/// and compiled; then, before anything is opened, serve refuses an explicit `MULTIDB_PROFILE`
/// that selects another storage profile, an explicit `MULTIDB_REPLICATION` that contradicts the
/// spec, `encryption.at_rest` (serve cannot open encrypted databases) and a volatile profile.
/// The database path is an explicit `MULTIDB_DB_PATH`, else the spec `storage_path` resolved
/// against the directory of the spec file, else serve's default path. `open_from_spec` on the
/// serve surface then starts, applies an auto-approvable plan as `local:serve` when
/// `MULTIDB_SPEC_APPLY` allows it, or refuses: a refusal prints the plan and the exact `multidb
/// config apply ... --confirm prov_...` command that confirms it, or the `config plan` command
/// that reviews a blocked plan. Every refusal comes before any port is bound. Returns the
/// database and the `spec_digest` of the spec it started from.
fn serve_spec_database(
    config: &ServeConfig,
    serve_spec: &ServeSpecConfig,
) -> Result<(multidb::db::Database, String), CliError> {
    let spec = match read_spec_file(&serve_spec.path) {
        Ok(Ok(spec)) => spec,
        Ok(Err(report)) => {
            return Err(serve_refusal(
                serve_spec,
                &format!(
                    "the spec cannot be decoded\n{}",
                    format_validation_report(&report)
                ),
            ));
        }
        Err(error) => {
            return Err(serve_refusal(
                serve_spec,
                &format!("the spec cannot be read: {error}"),
            ));
        }
    };
    let compiled = PolicyCompiler::compile(&spec).map_err(|report| {
        serve_refusal(
            serve_spec,
            &format!("the spec is invalid\n{}", format_validation_report(&report)),
        )
    })?;
    if let Some(reason) = serve_spec_conflict(
        config,
        serve_spec,
        &spec,
        compiled.resolved_profile.as_ref(),
    ) {
        return Err(serve_refusal(serve_spec, &reason));
    }
    let (db_path, notes) = serve_spec_db_path(config, serve_spec, &spec);
    if let Some(parent) = db_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let opened = provision::open_from_spec(
        &spec,
        Some(&db_path),
        serve_spec.mode,
        &ProvisionOptions::default(),
        &ProvisionEnvironment::new(ProvisionSurface::Serve),
    );
    match opened {
        Ok((database, report)) => {
            eprintln!(
                "{}",
                format_startup_report(serve_spec, &db_path, &report, &notes)
            );
            Ok((database, spec_digest(&spec)))
        }
        Err(ProvisionError::StartupRefused(refusal)) => Err(serve_refusal(
            serve_spec,
            &serve_refusal_details(serve_spec, &spec, &db_path, &refusal, &notes),
        )),
        Err(ProvisionError::Config(error)) => Err(CliError::Config(error)),
        Err(ProvisionError::Db(error)) => Err(CliError::Db(error)),
        Err(error) => Err(serve_refusal(serve_spec, &error.to_string())),
    }
}

/// A refused start in spec mode, as a usage error.
fn serve_refusal(serve_spec: &ServeSpecConfig, reason: &str) -> CliError {
    CliError::Usage(format!(
        "multidb serve refused to start before binding any port (MULTIDB_SPEC_PATH={}, \
         MULTIDB_SPEC_APPLY={}): {reason}",
        serve_spec.path.display(),
        serve_spec.mode.slug()
    ))
}

/// What in serve's own configuration contradicts the spec: an explicit `MULTIDB_PROFILE` that
/// selects another storage profile (`auto` never does), an explicit `MULTIDB_REPLICATION` other
/// than the spec's, encryption at rest and a volatile profile.
fn serve_spec_conflict(
    config: &ServeConfig,
    serve_spec: &ServeSpecConfig,
    spec: &DatabaseSpec,
    resolved: Option<&ResolvedProfile>,
) -> Option<String> {
    if let (true, ProfileSelection::Fixed(selected), Some(resolved)) =
        (serve_spec.explicit_profile, config.profile, resolved)
        && selected != resolved.storage_profile
    {
        return Some(format!(
            "MULTIDB_PROFILE selects the storage profile {selected:?}, but the spec profile {} \
             uses {:?}; in spec mode the spec decides the profile, so set MULTIDB_PROFILE to an \
             empty value (unsetting it falls back to the image default) or make it agree",
            resolved.slug, resolved.storage_profile
        ));
    }
    if let Some(replication) = serve_spec.explicit_replication
        && replication != spec.defaults.replication
    {
        return Some(format!(
            "MULTIDB_REPLICATION is {}, but the spec defaults.replication is {}; unset \
             MULTIDB_REPLICATION or make it agree",
            slug(&replication),
            slug(&spec.defaults.replication)
        ));
    }
    if spec.guarantees.encryption.at_rest {
        return Some(
            "the spec sets guarantees.encryption.at_rest, but multidb serve cannot open \
             encrypted databases in this build: encryption at rest is provisioned through the \
             Rust API only (GUARANTEE_NOT_ENFORCEABLE)"
                .to_owned(),
        );
    }
    if let Some(resolved) = resolved
        && resolved.knobs.storage.medium == StorageMedium::Volatile
    {
        return Some(format!(
            "the spec profile {} is volatile (in memory), and multidb serve serves durable \
             databases only (PROVISION_VOLATILE_ON_SURFACE); use a durable profile",
            resolved.slug
        ));
    }
    None
}

/// The database path of spec mode, and a note when an explicit `MULTIDB_DB_PATH` wins over the
/// spec `deployment.storage_path`. Without either, serve's default path.
fn serve_spec_db_path(
    config: &ServeConfig,
    serve_spec: &ServeSpecConfig,
    spec: &DatabaseSpec,
) -> (PathBuf, Vec<String>) {
    match spec.deployment.storage_path.as_deref() {
        Some(storage_path) if serve_spec.explicit_db_path => (
            config.db_path.clone(),
            vec![format!(
                "MULTIDB_DB_PATH {} wins over the spec deployment.storage_path {storage_path}",
                config.db_path.display()
            )],
        ),
        Some(storage_path) => (
            resolve_against(parent_dir(&serve_spec.path), storage_path),
            Vec::new(),
        ),
        None => (config.db_path.clone(), Vec::new()),
    }
}

/// `path` canonicalized when it exists, else made absolute: commands printed to run later work
/// from any directory.
fn absolute_path(path: &Path) -> PathBuf {
    std::fs::canonicalize(path)
        .or_else(|_| std::path::absolute(path))
        .unwrap_or_else(|_| path.to_path_buf())
}

/// The reason of a refused start, the plan it was refused on and what to run next.
fn serve_refusal_details(
    serve_spec: &ServeSpecConfig,
    spec: &DatabaseSpec,
    db_path: &Path,
    refusal: &StartupRefusal,
    notes: &[String],
) -> String {
    let mut command = SpecCommand {
        spec_path: absolute_path(&serve_spec.path),
        spec: spec.clone(),
        db_path: Some(
            refusal
                .plan
                .target
                .path
                .as_deref()
                .map_or_else(|| absolute_path(db_path), PathBuf::from),
        ),
        options: ProvisionOptions::default(),
        output: "text".to_owned(),
    };
    let mut lines = vec![refusal.reason.clone()];
    lines.extend(notes.iter().map(|note| format!("note: {note}")));
    if let Some(applied) = &refusal.applied {
        lines.push(format_provision_report(applied, &command));
    }
    lines.push(format_provision_plan(&refusal.plan, &command.options));
    lines.push(serve_next_step(&mut command, refusal));
    lines.join("\n")
}

/// The command that resolves a refused start. For a ready plan, or one that needs flags, it is
/// `multidb config apply --spec <abs spec> --db <canonical db> [exactly the flags the plan
/// needs] --confirm <prov_id>`, with the id of a plan on the CLI surface against the same
/// canonical target with those flags, so running it confirms that plan. For a blocked plan, or
/// what remains of a failed startup apply, it is the `config plan` command that reviews it.
fn serve_next_step(command: &mut SpecCommand, refusal: &StartupRefusal) -> String {
    let review = |command: &SpecCommand| {
        format!(
            "review the plan with: {}",
            plan_command(command, &command.options, None)
        )
    };
    if refusal.applied.is_some()
        || matches!(
            refusal.plan.outcome,
            PlanOutcome::Blocked | PlanOutcome::Noop
        )
    {
        return review(command);
    }
    command.options = options_with_flags(&command.options, &refusal.plan.required_flags);
    let cli_plan = open_spec_target(command.db_path.as_deref())
        .ok()
        .and_then(|target| plan_spec_target(command, &target).ok());
    match cli_plan {
        Some(plan) if plan.outcome == PlanOutcome::Ready => format!(
            "apply it with: {}\nthen start multidb serve again (prov_ ids are per surface: this \
             is the id of the plan on the CLI surface, not of the serve plan above)",
            apply_command(command, &plan)
        ),
        _ => review(command),
    }
}

/// What a start in spec mode did, for stderr: whether the database matched or which plan was
/// applied, the notes and the Warnings of the plan.
fn format_startup_report(
    serve_spec: &ServeSpecConfig,
    db_path: &Path,
    report: &StartupReport,
    notes: &[String],
) -> String {
    let headline = match &report.applied {
        Some(applied) => format!(
            "applied plan {} to {} ({} steps) as local:serve",
            report.plan_id,
            db_path.display(),
            applied.applied_steps.len()
        ),
        None => format!(
            "the database at {} matches the spec (plan {})",
            db_path.display(),
            report.plan_id
        ),
    };
    let mut lines = vec![format!(
        "multidb serve: spec {} (MULTIDB_SPEC_APPLY={}): {headline}",
        serve_spec.path.display(),
        serve_spec.mode.slug()
    )];
    lines.extend(notes.iter().map(|note| format!("note: {note}")));
    lines.extend(report.warnings.iter().map(format_issue));
    lines.join("\n")
}

fn run_admin(args: &[String]) -> Result<String, CliError> {
    let Some(command) = args.first() else {
        return Err(usage());
    };
    match command.as_str() {
        "status" => {
            let database = open_cli_database(args)?;
            serde_json::to_string_pretty(
                &multidb::admin::AdminState::from_database(database).status(),
            )
            .map_err(Into::into)
        }
        "serve" => {
            let database = create_cli_database(args)?;
            let bind = optional_value(args, "--bind")
                .unwrap_or_else(|| "127.0.0.1:9090".to_owned())
                .parse::<SocketAddr>()
                .map_err(|error| CliError::Usage(format!("invalid --bind: {error}")))?;
            let admin_token = optional_value(args, "--admin-token").map_or_else(
                || {
                    secret_from_sources(
                        &|name| env::var(name).ok(),
                        "MULTIDB_ADMIN_TOKEN",
                        "MULTIDB_ADMIN_TOKEN_FILE",
                    )
                },
                |token| Ok(Some(token)),
            )?;
            let admin_password = admin_password_from_args(args)?;
            let admin_password_reset = has_flag(args, "--admin-password-reset")
                || env_bool(&|name| env::var(name).ok(), "MULTIDB_ADMIN_PASSWORD_RESET");
            let admin_session_ttl_seconds = optional_value(args, "--admin-session-ttl-seconds")
                .map_or_else(
                    || {
                        env::var("MULTIDB_ADMIN_SESSION_TTL_SECONDS").ok().map_or(
                            Ok(multidb::admin::DEFAULT_ADMIN_SESSION_TTL_SECONDS),
                            parse_admin_session_ttl,
                        )
                    },
                    parse_admin_session_ttl,
                )?;
            let admin_login_rate_limit =
                admin_login_rate_limit_from_sources(&|name| env::var(name).ok())?;
            let insecure_local_admin = has_flag(args, "--insecure-local-admin");
            if insecure_local_admin && !multidb::admin::local_insecure_admin_allowed(bind) {
                return Err(CliError::Usage(
                    "pass --insecure-local-admin only for loopback development binds".to_owned(),
                ));
            }
            let database = Arc::new(tokio::sync::Mutex::new(database));
            let mut state = multidb::admin::AdminState::from_database_handle(Arc::clone(&database))
                .with_admin_session_ttl_seconds(admin_session_ttl_seconds)
                .with_admin_login_rate_limit(admin_login_rate_limit);
            state = if let Some(token) = admin_token {
                state.with_admin_token(token)
            } else if insecure_local_admin {
                state.with_insecure_local_admin()
            } else {
                state
            };
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(async move {
                if let Some(password) = admin_password {
                    state
                        .bootstrap_admin_password(password, admin_password_reset)
                        .await
                        .map_err(|error| {
                            std::io::Error::other(format!("admin password: {error}"))
                        })?;
                }
                if !state.is_admin_auth_configured() {
                    return Err(std::io::Error::other(
                        "admin serve requires --admin-password, \
                         MULTIDB_ADMIN_PASSWORD(_FILE), --admin-token, \
                         MULTIDB_ADMIN_TOKEN, an existing stored admin credential, \
                         or --insecure-local-admin for loopback development",
                    ));
                }
                let listener = tokio::net::TcpListener::bind(bind).await?;
                axum::serve(
                    listener,
                    multidb::admin::router(state)
                        .into_make_service_with_connect_info::<SocketAddr>(),
                )
                .await?;
                Ok::<(), std::io::Error>(())
            })?;
            Ok(format!("admin server stopped on {bind}"))
        }
        _ => Err(usage()),
    }
}

fn format_validation_report(report: &ValidationReport) -> String {
    let mut lines = vec![format!("valid={} status={:?}", report.valid, report.status)];
    lines.extend(report.issues.iter().map(format_issue));
    lines.join("\n")
}

fn format_issue(issue: &ValidationIssue) -> String {
    format!(
        "{} {} {}: {} suggestion: {}",
        severity_label(issue.severity),
        issue.code,
        issue.path,
        issue.message,
        issue.suggestion
    )
}

/// The text form of a resolver result: a header, one aligned row per knob and the issues.
fn format_resolution(resolution: &ProfileResolution) -> Result<String, CliError> {
    let mut lines = match resolution {
        ProfileResolution::Resolved { profile, .. } => {
            let identity = match (&profile.kind, &profile.extends) {
                (ResolvedProfileKind::Custom, Some(extends)) => {
                    format!("{} extends {extends}", profile.slug)
                }
                _ => profile.slug.clone(),
            };
            vec![
                format!(
                    "{identity} -> {} variant {} ({:?}), storage profile {:?} ({})",
                    profile.product_profile,
                    profile.variant,
                    profile.status,
                    profile.storage_profile,
                    storage_summary(&profile.knobs)
                ),
                format!(
                    "kind={} digest={}",
                    kind_label(profile.kind),
                    profile.digest
                ),
                knob_table(&profile.knobs, Some(&profile.provenance))?,
            ]
        }
        ProfileResolution::Unresolved { reason, .. } => vec![format!("unresolved: {reason}")],
        ProfileResolution::Invalid(_) => vec!["invalid profile".to_owned()],
    };
    lines.extend(resolution.issues().iter().map(format_issue));
    Ok(lines.join("\n"))
}

/// The warning `profile show --db` and `restore --allow-unmet-floors` print for a manifest floor
/// the database does not meet (`ProbedMetadata.unmet_floors`).
fn unmet_floor_warning(floor: &str) -> String {
    let reason = match floor {
        "floors.encryption_at_rest" => {
            "the metadata is readable without a key, so this file is not encrypted at rest (a \
             restore target never is)"
        }
        "floors.audit" => {
            "audit is switched off in this database (a restore keeps the backed-up audit flag)"
        }
        _ => "the database does not meet it",
    };
    format!("warning: {floor} is not met: {reason}; see docs/profiles.md, \"Backup and restore\"")
}

/// The text form of `profile show --db`.
fn format_metadata_probe(path: &Path, probe: &MetadataProbe) -> Result<String, CliError> {
    Ok(match probe {
        MetadataProbe::Initialized(metadata) => {
            let details = format!(
                "schema_version={} replication={:?} layout={:?} values_framed={}",
                metadata.schema_version,
                metadata.replication,
                metadata.layout,
                metadata.values_framed
            );
            let mut lines = vec![match &metadata.manifest {
                Some(manifest) => format_manifest(manifest, &details)?,
                None => format!(
                    "legacy database: storage profile {:?}, no manifest\n{details}",
                    metadata.storage_profile
                ),
            }];
            lines.extend(
                metadata
                    .unmet_floors
                    .iter()
                    .map(|floor| unmet_floor_warning(floor)),
            );
            lines.join("\n")
        }
        MetadataProbe::NotFound => format!("no database at {}", path.display()),
        MetadataProbe::Uninitialized => {
            format!("{} holds no database metadata", path.display())
        }
        MetadataProbe::Locked => format!(
            "{} is locked: another handle holds the database open",
            path.display()
        ),
        MetadataProbe::NeedsRecovery => format!(
            "{} was not closed cleanly and needs crash recovery; profile show never writes the \
             file, so open it once (any command with --profile auto or its profile) and retry",
            path.display()
        ),
        MetadataProbe::Unreadable { reason } => {
            format!("{} cannot be read: {reason}", path.display())
        }
    })
}

fn format_manifest(manifest: &PersistedProfileManifest, details: &str) -> Result<String, CliError> {
    let identity = match (&manifest.kind, &manifest.extends) {
        (ResolvedProfileKind::Custom, Some(extends)) => {
            format!("{} extends {extends}", manifest.profile)
        }
        _ => manifest.profile.clone(),
    };
    Ok([
        format!(
            "profile manifest: {identity} -> {} variant {}, storage profile {:?} ({})",
            manifest.product_profile,
            manifest.variant,
            manifest.storage_profile,
            storage_summary(&manifest.knobs)
        ),
        format!(
            "{details} kind={} digest={} written_by={}",
            kind_label(manifest.kind),
            manifest.digest,
            manifest.written_by
        ),
        knob_table(&manifest.knobs, None)?,
    ]
    .join("\n"))
}

/// One row per `KNOB_REGISTRY` path: the value, where it came from (when known) and the
/// registry's timing, sink and enforcement.
fn knob_rows_json(
    knobs: &ProfileKnobs,
    provenance: Option<&std::collections::BTreeMap<String, KnobSource>>,
) -> Result<Vec<serde_json::Value>, CliError> {
    let values = serde_json::to_value(knobs)?;
    Ok(KNOB_REGISTRY
        .iter()
        .map(|definition| {
            json!({
                "path": definition.path,
                "value": knob_value(&values, definition),
                "source": provenance.and_then(|provenance| provenance.get(definition.path)),
                "timing": definition.timing,
                "sink": definition.sink,
                "enforcement": definition.enforcement,
            })
        })
        .collect())
}

fn knob_table(
    knobs: &ProfileKnobs,
    provenance: Option<&std::collections::BTreeMap<String, KnobSource>>,
) -> Result<String, CliError> {
    let values = serde_json::to_value(knobs)?;
    let rows = KNOB_REGISTRY
        .iter()
        .map(|definition| {
            let source = provenance
                .and_then(|provenance| provenance.get(definition.path))
                .map_or("-", |source| source_label(*source));
            vec![
                definition.path.to_owned(),
                display_knob_value(&knob_value(&values, definition)),
                source.to_owned(),
                timing_label(definition.timing).to_owned(),
                enforcement_label(definition.enforcement),
                definition.sink.to_owned(),
            ]
        })
        .collect::<Vec<_>>();
    Ok(align_rows(&rows))
}

fn knob_value(values: &serde_json::Value, definition: &KnobDefinition) -> serde_json::Value {
    values
        .pointer(&format!("/{}", definition.path.replace('.', "/")))
        .cloned()
        .unwrap_or(serde_json::Value::Null)
}

fn display_knob_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Array(items) => items
            .iter()
            .map(display_knob_value)
            .collect::<Vec<_>>()
            .join(","),
        other => other.to_string(),
    }
}

/// Pads every column but the last to its widest cell.
fn align_rows(rows: &[Vec<String>]) -> String {
    let columns = rows.iter().map(Vec::len).max().unwrap_or(0);
    let widths = (0..columns)
        .map(|column| {
            rows.iter()
                .filter_map(|row| row.get(column))
                .map(|cell| cell.chars().count())
                .max()
                .unwrap_or(0)
        })
        .collect::<Vec<_>>();
    rows.iter()
        .map(|row| {
            let mut line = String::new();
            for (column, cell) in row.iter().enumerate() {
                if column + 1 == row.len() {
                    line.push_str(cell);
                } else {
                    let width = widths.get(column).copied().unwrap_or(0);
                    line.push_str(&format!("{cell:<width$}  "));
                }
            }
            line.trim_end().to_owned()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The engine, layout, commit protocol and compression the knobs select, for example
/// `redb, row, two-phase commit`.
fn storage_summary(knobs: &ProfileKnobs) -> String {
    let mut parts = vec![
        match knobs.storage.medium {
            StorageMedium::Durable => "redb",
            StorageMedium::Volatile => "memory",
        },
        match knobs.storage.layout {
            LayoutKnob::Row => "row",
            LayoutKnob::Columnar => "columnar",
        },
        match knobs.durability.commit {
            CommitMode::Immediate => "immediate commit",
            CommitMode::TwoPhase => "two-phase commit",
        },
    ];
    match knobs.compression.algorithm {
        CompressionKnob::None => {}
        CompressionKnob::Lz4 => parts.push("lz4 value framing"),
        CompressionKnob::Zstd => parts.push("zstd value framing"),
    }
    parts.join(", ")
}

const fn kind_label(kind: ResolvedProfileKind) -> &'static str {
    match kind {
        ResolvedProfileKind::BuiltIn => "built_in",
        ResolvedProfileKind::Variant => "variant",
        ResolvedProfileKind::Custom => "custom",
    }
}

const fn source_label(source: KnobSource) -> &'static str {
    match source {
        KnobSource::Preset => "preset",
        KnobSource::Variant => "variant",
        KnobSource::Override => "override",
        KnobSource::Derived => "derived",
    }
}

const fn timing_label(timing: KnobTiming) -> &'static str {
    match timing {
        KnobTiming::Creation => "creation",
        KnobTiming::EveryOpen => "every open",
        KnobTiming::Provisioning => "provisioning",
        KnobTiming::GenerationDefault => "generation default",
        KnobTiming::Validation => "validation",
    }
}

fn enforcement_label(enforcement: KnobEnforcement) -> String {
    match enforcement {
        KnobEnforcement::Enforced => "enforced".to_owned(),
        KnobEnforcement::Planned { tracking } => format!("planned ({tracking})"),
        KnobEnforcement::Declarative => "declarative".to_owned(),
    }
}

const fn change_policy_label(policy: ChangePolicy) -> &'static str {
    match policy {
        ChangePolicy::Unsupported => "unsupported",
        ChangePolicy::Retune => "retune",
        ChangePolicy::FormatUpgrade => "format upgrade",
        ChangePolicy::Provisioning => "provisioning",
        ChangePolicy::NotApplicable => "not applicable",
    }
}

fn format_explain_report(report: &ExplainConfigReport) -> String {
    let mut lines = vec![
        format!(
            "valid={} status={:?} compiled_policy={}",
            report.validation.valid,
            report.validation.status,
            report.compiled_policy.is_some()
        ),
        format!("decisions={}", report.decisions.len()),
    ];
    for decision in &report.decisions {
        lines.push(format!(
            "{} value={} source={} outcome={} risk={:?}",
            decision.path, decision.value, decision.source, decision.outcome, decision.impact.risk
        ));
    }
    lines.join("\n")
}

fn format_runtime_advice_report(report: &RuntimeAdviceReport) -> String {
    let mut lines = vec![format!(
        "schema_version={} auto_apply_enabled={} recommendations={} suppressed={}",
        report.schema_version,
        report.auto_apply_enabled,
        report.recommendations.len(),
        report.suppressed_recommendations
    )];
    for advice in &report.recommendations {
        lines.push(format!(
            "{} {} status={:?} risk={:?} plan_id={} hint={} gain={}",
            advice.id,
            advice.code,
            advice.status,
            advice.risk,
            advice.dry_run.plan_id,
            advice.dry_run.operation_hint,
            advice.expected_gain
        ));
    }
    lines.join("\n")
}

fn format_migration_plan(plan: &MigrationPlan) -> String {
    let mut lines = vec![
        format!(
            "plan_id={} valid={} apply_supported={} confirm={}",
            plan.plan_id, plan.valid, plan.apply_supported, plan.required_confirmation
        ),
        format!(
            "impact downtime={:?} disk={:?} cpu={:?} risk={:?} backup={} downtime_required={}",
            plan.impact.downtime,
            plan.impact.disk,
            plan.impact.cpu,
            plan.impact.risk,
            plan.impact.requires_backup,
            plan.impact.requires_downtime
        ),
    ];
    for step in &plan.steps {
        lines.push(format!(
            "{} {:?} {} supported={} confirm={} action={}",
            step.step_id,
            step.kind,
            step.path,
            step.supported,
            step.requires_confirmation,
            step.action
        ));
    }
    lines.join("\n")
}

fn format_apply_report(report: &ApplyCheckReport) -> String {
    format!(
        "plan_id={} status={:?} valid={} confirmation_matched={} audit_recorded={} data_mutated={} message={}",
        report.plan_id,
        report.status,
        report.valid,
        report.confirmation_matched,
        report.audit_recorded,
        report.data_mutated,
        report.message
    )
}

fn format_profiles() -> String {
    built_in_profiles()
        .iter()
        .map(|profile| {
            let aliases = if profile.aliases.is_empty() {
                "-".to_owned()
            } else {
                profile.aliases.join(",")
            };
            let roles = profile
                .compatible_roles
                .iter()
                .map(|role| role_slug(*role))
                .collect::<Vec<_>>()
                .join(",");
            let variants = product_variants(profile.slug)
                .map(|variant| {
                    format!(
                        "{}({:?}{})",
                        variant.name,
                        variant.storage_profile,
                        if variant.canonical { ",canonical" } else { "" }
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "slug={} status={:?} aliases={} default_domain={} roles={} variants={} description={}",
                profile.slug,
                profile.status,
                aliases,
                domain_slug(profile.default_domain),
                roles,
                variants,
                profile.description
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_templates() -> String {
    built_in_templates()
        .iter()
        .map(|template| {
            let aliases = if template.aliases.is_empty() {
                "-".to_owned()
            } else {
                template.aliases.join(",")
            };
            let collections = template
                .collections
                .iter()
                .map(|collection| format!("{}:{}", collection.name, role_slug(collection.role)))
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "slug={} title={} profile={} aliases={} collections={} description={}",
                template.slug,
                template.title,
                template.profile,
                aliases,
                collections,
                template.description
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_roles() -> String {
    collection_role_definitions()
        .iter()
        .map(|role| {
            format!(
                "slug={} status={:?} capabilities={} constraints={} description={}",
                role.slug,
                role.status,
                role.required_capabilities.join(","),
                role.constraints.join(","),
                role.description
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_domains() -> String {
    consistency_domain_definitions()
        .iter()
        .map(|domain| {
            format!(
                "slug={} status={:?} guarantees={} limits={}",
                domain.slug,
                domain.status,
                domain.guarantees.join(","),
                domain.limits.join(",")
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn guided_spec(profile: &ProfileSpec, name: &str) -> DatabaseSpec {
    guided_spec_for_domain(profile, name, profile.default_domain)
}

/// The guided spec for `init --override`: version 2 with `custom`, the profile it extends. The
/// resolved storage medium and default domain shape the deployment, the domain and the
/// replication, and every required floor raises the matching guarantee.
fn custom_guided_spec(
    product: &ProfileSpec,
    base: &ResolvedProfile,
    custom: CustomProfileSpec,
    slug: &str,
    name: &str,
) -> DatabaseSpec {
    let medium = custom
        .knobs
        .storage
        .and_then(|storage| storage.medium)
        .unwrap_or(base.knobs.storage.medium);
    let domain = custom
        .knobs
        .consistency
        .and_then(|consistency| consistency.default_domain)
        .unwrap_or(base.knobs.consistency.default_domain);
    let mut spec = guided_spec_for_domain(product, name, domain);
    spec.version = DATABASE_SPEC_VERSION_V2;
    spec.profile = slug.to_owned();
    if medium == StorageMedium::Volatile {
        spec.deployment = DeploymentSpec {
            mode: DeploymentMode::Embedded,
            storage_path: None,
        };
    }
    let preset = base.knobs.floors;
    let raised = custom.floors;
    if preset.audit || raised.audit == Some(true) {
        spec.guarantees.audit.enabled = true;
    }
    if preset.backup || raised.backup == Some(true) {
        spec.guarantees.backup.enabled = true;
    }
    if preset.pitr || raised.pitr == Some(true) {
        // Point-in-time recovery is restored from backups.
        spec.guarantees.backup.enabled = true;
        spec.guarantees.backup.pitr = true;
    }
    if preset.encryption_at_rest || raised.encryption_at_rest == Some(true) {
        spec.guarantees.encryption.at_rest = true;
    }
    spec.custom_profile = Some(custom);
    spec
}

fn guided_spec_for_domain(
    profile: &ProfileSpec,
    name: &str,
    domain: ConsistencyMode,
) -> DatabaseSpec {
    let replication = replication_for_domain(domain);
    let mut guarantees = GuaranteeSpec::for_replication(replication);
    if profile.slug == "production_cp" {
        guarantees.backup.enabled = true;
        guarantees.backup.pitr = true;
    }
    let durable_storage = guided_profile_uses_durable_storage(profile.slug);
    DatabaseSpec {
        version: multidb::config_spec::DATABASE_SPEC_VERSION,
        name: name.to_owned(),
        profile: profile.slug.to_owned(),
        deployment: DeploymentSpec {
            mode: if durable_storage {
                DeploymentMode::SingleNode
            } else {
                DeploymentMode::Embedded
            },
            storage_path: durable_storage.then(|| storage_file_name(name)),
        },
        topology: TopologySpec::default(),
        defaults: DatabaseDefaults {
            consistency_domain: "primary".to_owned(),
            replication,
        },
        guarantees,
        domains: vec![ConsistencyDomainSpec {
            name: "primary".to_owned(),
            mode: domain,
        }],
        collections: Vec::new(),
        extensions: Vec::new(),
        overrides: std::collections::BTreeMap::new(),
        operation_hints: std::collections::BTreeMap::new(),
        custom_profile: None,
        security: None,
        provisioning: None,
    }
}

const fn replication_for_domain(domain: ConsistencyMode) -> ReplicationMode {
    match domain {
        ConsistencyMode::EventualAp => ReplicationMode::Ap,
        ConsistencyMode::LocalSnapshot | ConsistencyMode::StrongCp => ReplicationMode::Cp,
    }
}

fn guided_profile_uses_durable_storage(profile: &str) -> bool {
    !matches!(profile, "game_local_balanced")
}

fn storage_file_name(name: &str) -> String {
    let mut file_name = String::with_capacity(name.len() + ".redb".len());
    for character in name.chars() {
        if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
            file_name.push(character.to_ascii_lowercase());
        } else {
            file_name.push('_');
        }
    }
    if file_name.is_empty() {
        file_name.push_str("multidb");
    }
    file_name.push_str(".redb");
    file_name
}

fn role_slug(role: multidb::config_spec::CollectionRole) -> &'static str {
    collection_role_definition(role).map_or("unknown", |definition| definition.slug)
}

fn domain_slug(domain: ConsistencyMode) -> &'static str {
    consistency_domain_definition(domain).map_or("unknown", |definition| definition.slug)
}

const fn template_index_slug(index: multidb::config_spec::CollectionIndexKind) -> &'static str {
    match index {
        multidb::config_spec::CollectionIndexKind::Primary => "primary",
        multidb::config_spec::CollectionIndexKind::Document => "document",
        multidb::config_spec::CollectionIndexKind::Vector => "vector",
        multidb::config_spec::CollectionIndexKind::Graph => "graph",
        multidb::config_spec::CollectionIndexKind::FullText => "full_text",
        multidb::config_spec::CollectionIndexKind::Columnar => "columnar",
        multidb::config_spec::CollectionIndexKind::TimeSeries => "time_series",
        multidb::config_spec::CollectionIndexKind::Geo => "geo",
    }
}

fn resolve_built_in_profile(value: &str) -> Option<&'static ProfileSpec> {
    built_in_profile_normalized(value)
}

fn require_list_command(args: &[String]) -> Result<(), CliError> {
    if args.first().is_some_and(|command| command == "list") {
        Ok(())
    } else {
        Err(usage())
    }
}

fn init_file_format(args: &[String]) -> Result<String, CliError> {
    let format = optional_value(args, "--format").unwrap_or_else(|| "yaml".to_owned());
    if format == "yaml" || format == "json" {
        Ok(format)
    } else {
        Err(CliError::Usage(format!(
            "unsupported --format {format}; expected yaml or json"
        )))
    }
}

fn template_output_format(args: &[String]) -> Result<TemplateOutputFormat, CliError> {
    match init_file_format(args)?.as_str() {
        "json" => Ok(TemplateOutputFormat::Json),
        "yaml" => Ok(TemplateOutputFormat::Yaml),
        other => Err(CliError::Usage(format!(
            "unsupported --format {other}; expected yaml or json"
        ))),
    }
}

fn ensure_template_output_dir(path: &Path, force: bool) -> Result<(), CliError> {
    if path.exists() {
        if !path.is_dir() {
            return Err(CliError::Usage(format!(
                "template output path {} must be a directory",
                path.display()
            )));
        }
        if !force && directory_is_non_empty(path)? {
            return Err(CliError::Usage(format!(
                "refusing to write template into non-empty directory {}; pass --force to replace generated files",
                path.display()
            )));
        }
    }
    std::fs::create_dir_all(path)?;
    Ok(())
}

fn directory_is_non_empty(path: &Path) -> Result<bool, CliError> {
    let mut entries = std::fs::read_dir(path)?;
    Ok(entries.next().transpose()?.is_some())
}

fn write_template_materialization(
    out_dir: &Path,
    materialized: &TemplateMaterialization,
) -> Result<(), CliError> {
    for file in &materialized.files {
        let path = out_dir.join(&file.path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, file.contents.as_bytes())?;
    }
    Ok(())
}

fn default_init_path(format: &str) -> PathBuf {
    if format == "json" {
        PathBuf::from("multidb.json")
    } else {
        PathBuf::from("multidb.yaml")
    }
}

fn serialize_spec(spec: &DatabaseSpec, format: &str) -> Result<String, CliError> {
    if format == "json" {
        serde_json::to_string_pretty(spec).map_err(Into::into)
    } else {
        serde_norway::to_string(spec).map_err(Into::into)
    }
}

const fn severity_label(severity: ValidationSeverity) -> &'static str {
    match severity {
        ValidationSeverity::Error => "error",
        ValidationSeverity::Warning => "warning",
        ValidationSeverity::Advice => "advice",
    }
}

fn output_format(args: &[String]) -> Result<String, CliError> {
    if has_flag(args, "--json") {
        if optional_value(args, "--output").is_some_and(|output| output != "json") {
            return Err(CliError::Usage(
                "conflicting --json and --output; use --output json or remove --json".to_owned(),
            ));
        }
        return Ok("json".to_owned());
    }
    let output = optional_value(args, "--output").unwrap_or_else(|| "text".to_owned());
    if output != "text" && output != "json" {
        return Err(CliError::Usage(format!(
            "unsupported --output {output}; expected text or json"
        )));
    }
    Ok(output)
}

/// Reads a spec through `config_spec::spec_io` (1 MiB cap, version peek, bounded YAML aliases).
/// Size and version problems come back as a validation report (exit 2); I/O, syntax and decode
/// errors stay CLI errors (exit 1), as before.
fn read_spec_file(path: &Path) -> Result<Result<DatabaseSpec, ValidationReport>, CliError> {
    match spec_io::read_spec_file(path) {
        Ok(spec) => Ok(Ok(spec)),
        Err(error) => {
            if let Some(report) = error.validation_report() {
                return Ok(Err(report));
            }
            Err(match error {
                SpecIoError::Io(error) => CliError::Io(error),
                SpecIoError::Yaml(error) => CliError::Yaml(error),
                SpecIoError::Json(error) => CliError::Json(error),
                other => CliError::Runtime(other.to_string()),
            })
        }
    }
}

/// The `SPEC_TOO_LARGE` report (exit 2) for a serialized spec over `spec_io::MAX_SPEC_BYTES`,
/// which multidb could not read back; `None` for one within the limit. `config upgrade` and
/// `config export` write such a spec nowhere, neither to `--out` (named in the text report)
/// nor to stdout.
fn refuse_oversized_spec(
    content: &str,
    output: &str,
    out_path: Option<&Path>,
) -> Result<Option<CliRun>, CliError> {
    if content.len() <= spec_io::MAX_SPEC_BYTES {
        return Ok(None);
    }
    let too_large = SpecIoError::TooLarge {
        limit: spec_io::MAX_SPEC_BYTES,
        found: u64::try_from(content.len()).unwrap_or(u64::MAX),
    };
    let report = too_large
        .validation_report()
        .ok_or_else(|| CliError::Runtime(too_large.to_string()))?;
    let source = out_path.map(|out_path| ("--out", out_path));
    spec_report_run(&report, output, source).map(Some)
}

/// Prints the report of a spec that could not be decoded, with exit code 2.
fn spec_report_run(
    report: &ValidationReport,
    output: &str,
    source: Option<(&str, &Path)>,
) -> Result<CliRun, CliError> {
    let message = if output == "json" {
        serde_json::to_string_pretty(report)?
    } else {
        let text = format_validation_report(report);
        match source {
            Some((flag, path)) => format!("{flag} {}\n{text}", path.display()),
            None => text,
        }
    };
    Ok(CliRun::validation(message, false))
}

fn read_json_file<T>(path: &Path) -> Result<T, CliError>
where
    T: serde::de::DeserializeOwned,
{
    let input = std::fs::read_to_string(path)?;
    serde_json::from_str(&input).map_err(Into::into)
}

fn reject_yaml_plan_path(path: &Path) -> Result<(), CliError> {
    if is_yaml_path(path) {
        return Err(CliError::Usage(
            "config apply accepts JSON MigrationPlan files; write plans with multidb config plan --out <plan.json>"
                .to_owned(),
        ));
    }
    Ok(())
}

fn is_yaml_path(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            extension.eq_ignore_ascii_case("yaml") || extension.eq_ignore_ascii_case("yml")
        })
}

fn flatten_serve_task(
    result: Result<Result<(), CliError>, tokio::task::JoinError>,
) -> Result<(), CliError> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error),
        Err(error) => Err(CliError::Runtime(error.to_string())),
    }
}

fn serve_config_from_env() -> Result<ServeConfig, CliError> {
    serve_config_from_sources(|name| env::var(name).ok())
}

fn serve_config_from_sources<F>(get_env: F) -> Result<ServeConfig, CliError>
where
    F: Fn(&str) -> Option<String>,
{
    let runtime_mode = parse_runtime_mode(env_or_default(
        &get_env,
        "MULTIDB_RUNTIME_MODE",
        "production",
    ))?;
    let admin_bind = parse_socket_addr(env_or_default(&get_env, "MULTIDB_BIND", "0.0.0.0:8080"))?;
    let pg_bind = parse_socket_addr(env_or_default(&get_env, "MULTIDB_PG_BIND", "0.0.0.0:5432"))?;
    let db_path = PathBuf::from(env_or_default(
        &get_env,
        "MULTIDB_DB_PATH",
        "/var/lib/multidb/multidb.redb",
    ));
    let profile = parse_profile_selection(&env_or_default(
        &get_env,
        "MULTIDB_PROFILE",
        "transactional",
    ))?;
    let admin_token =
        secret_from_sources(&get_env, "MULTIDB_ADMIN_TOKEN", "MULTIDB_ADMIN_TOKEN_FILE")?;
    let admin_password = secret_from_sources_prefer_file(
        &get_env,
        "MULTIDB_ADMIN_PASSWORD",
        "MULTIDB_ADMIN_PASSWORD_FILE",
    )?;
    let admin_password_reset = env_bool(&get_env, "MULTIDB_ADMIN_PASSWORD_RESET");
    let admin_session_ttl_seconds = parse_admin_session_ttl(env_or_default(
        &get_env,
        "MULTIDB_ADMIN_SESSION_TTL_SECONDS",
        &multidb::admin::DEFAULT_ADMIN_SESSION_TTL_SECONDS.to_string(),
    ))?;
    let admin_login_rate_limit = admin_login_rate_limit_from_sources(&get_env)?;
    let pg_password = required_secret(&get_env, "MULTIDB_PG_PASSWORD", "MULTIDB_PG_PASSWORD_FILE")?;
    let pg_user = env_or_default(&get_env, "MULTIDB_PG_USER", "multidb");
    let studio_dir = PathBuf::from(env_or_default(
        &get_env,
        "MULTIDB_STUDIO_DIR",
        "/usr/share/multidb/studio",
    ));
    let pg_tls = pg_tls_from_sources(&get_env, runtime_mode)?;
    let spec = serve_spec_from_sources(&get_env)?;

    Ok(ServeConfig {
        admin_bind,
        pg_bind,
        db_path,
        profile,
        admin_token,
        admin_password,
        admin_password_reset,
        admin_session_ttl_seconds,
        admin_login_rate_limit,
        pg_user,
        pg_password,
        pg_tls,
        studio_dir,
        spec,
    })
}

/// Spec mode: `MULTIDB_SPEC_PATH`, `MULTIDB_SPEC_APPLY` (`verify` by default, `create` or
/// `additive`) and which of `MULTIDB_DB_PATH`, `MULTIDB_PROFILE` and `MULTIDB_REPLICATION` are
/// set explicitly, which means present and not blank, as `env_or_default` reads them.
/// `MULTIDB_REPLICATION` (`cp` or `ap`) is read in spec mode only; without a spec, `serve`
/// ignores it as before. `MULTIDB_SPEC_APPLY` without `MULTIDB_SPEC_PATH` is a usage error.
fn serve_spec_from_sources<F>(get_env: &F) -> Result<Option<ServeSpecConfig>, CliError>
where
    F: Fn(&str) -> Option<String>,
{
    let explicit = |name: &str| get_env(name).filter(|value| !value.trim().is_empty());
    let apply = explicit("MULTIDB_SPEC_APPLY");
    let Some(path) = explicit("MULTIDB_SPEC_PATH") else {
        return match apply {
            Some(_) => Err(CliError::Usage(
                "MULTIDB_SPEC_APPLY needs MULTIDB_SPEC_PATH, the spec multidb serve starts from"
                    .to_owned(),
            )),
            None => Ok(None),
        };
    };
    let mode = match apply {
        None => StartupMode::Verify,
        Some(value) => StartupMode::from_slug(&value).ok_or_else(|| {
            CliError::Usage(format!(
                "unsupported MULTIDB_SPEC_APPLY {value}; expected verify, create or additive"
            ))
        })?,
    };
    let explicit_replication = explicit("MULTIDB_REPLICATION")
        .map(|value| match value.trim().to_ascii_lowercase().as_str() {
            "cp" => Ok(ReplicationMode::Cp),
            "ap" => Ok(ReplicationMode::Ap),
            _ => Err(CliError::Usage(format!(
                "unsupported MULTIDB_REPLICATION {value}; expected cp or ap"
            ))),
        })
        .transpose()?;
    Ok(Some(ServeSpecConfig {
        path: PathBuf::from(path),
        mode,
        explicit_db_path: explicit("MULTIDB_DB_PATH").is_some(),
        explicit_profile: explicit("MULTIDB_PROFILE").is_some(),
        explicit_replication,
    }))
}

fn env_or_default<F>(get_env: &F, name: &str, default: &str) -> String
where
    F: Fn(&str) -> Option<String>,
{
    get_env(name)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.to_owned())
}

fn required_secret<F>(get_env: &F, name: &str, file_name: &str) -> Result<String, CliError>
where
    F: Fn(&str) -> Option<String>,
{
    secret_from_sources(get_env, name, file_name)?.ok_or_else(|| {
        CliError::Usage(format!(
            "{name} or {file_name} is required for multidb serve"
        ))
    })
}

fn secret_from_sources<F>(
    get_env: &F,
    name: &str,
    file_name: &str,
) -> Result<Option<String>, CliError>
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(value) = get_env(name).filter(|value| !value.trim().is_empty()) {
        return Ok(Some(value));
    }
    let Some(path) = get_env(file_name).filter(|value| !value.trim().is_empty()) else {
        return Ok(None);
    };
    let secret = std::fs::read_to_string(PathBuf::from(path))?;
    let secret = secret.trim().to_owned();
    if secret.is_empty() {
        return Ok(None);
    }
    Ok(Some(secret))
}

fn secret_from_sources_prefer_file<F>(
    get_env: &F,
    name: &str,
    file_name: &str,
) -> Result<Option<String>, CliError>
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(path) = get_env(file_name).filter(|value| !value.trim().is_empty()) {
        let secret = std::fs::read_to_string(PathBuf::from(path))?;
        let secret = secret.trim().to_owned();
        if !secret.is_empty() {
            return Ok(Some(secret));
        }
    }
    Ok(get_env(name).filter(|value| !value.trim().is_empty()))
}

fn admin_password_from_args(args: &[String]) -> Result<Option<String>, CliError> {
    if let Some(path) = optional_value(args, "--admin-password-file") {
        let password = std::fs::read_to_string(PathBuf::from(path))?
            .trim()
            .to_owned();
        if !password.is_empty() {
            return Ok(Some(password));
        }
    }
    if let Some(password) =
        optional_value(args, "--admin-password").filter(|password| !password.trim().is_empty())
    {
        return Ok(Some(password));
    }
    secret_from_sources_prefer_file(
        &|name| env::var(name).ok(),
        "MULTIDB_ADMIN_PASSWORD",
        "MULTIDB_ADMIN_PASSWORD_FILE",
    )
}

fn parse_admin_session_ttl(value: String) -> Result<u64, CliError> {
    let ttl = value.parse::<u64>().map_err(|error| {
        CliError::Usage(format!(
            "invalid admin session ttl {value}; expected seconds: {error}"
        ))
    })?;
    Ok(ttl.clamp(
        multidb::admin::MIN_ADMIN_SESSION_TTL_SECONDS,
        multidb::admin::MAX_ADMIN_SESSION_TTL_SECONDS,
    ))
}

fn admin_login_rate_limit_from_sources<F>(
    get_env: &F,
) -> Result<multidb::admin::AdminLoginRateLimitConfig, CliError>
where
    F: Fn(&str) -> Option<String>,
{
    let max_failures = parse_admin_login_max_failures(env_or_default(
        get_env,
        "MULTIDB_ADMIN_LOGIN_MAX_FAILURES",
        &multidb::admin::DEFAULT_ADMIN_LOGIN_MAX_FAILURES.to_string(),
    ))?;
    let window_seconds = parse_positive_seconds(
        env_or_default(
            get_env,
            "MULTIDB_ADMIN_LOGIN_WINDOW_SECONDS",
            &multidb::admin::DEFAULT_ADMIN_LOGIN_WINDOW_SECONDS.to_string(),
        ),
        "MULTIDB_ADMIN_LOGIN_WINDOW_SECONDS",
    )?;
    let lockout_seconds = parse_positive_seconds(
        env_or_default(
            get_env,
            "MULTIDB_ADMIN_LOGIN_LOCKOUT_SECONDS",
            &multidb::admin::DEFAULT_ADMIN_LOGIN_LOCKOUT_SECONDS.to_string(),
        ),
        "MULTIDB_ADMIN_LOGIN_LOCKOUT_SECONDS",
    )?;
    Ok(multidb::admin::AdminLoginRateLimitConfig::new(
        max_failures,
        window_seconds,
        lockout_seconds,
    ))
}

fn parse_admin_login_max_failures(value: String) -> Result<u32, CliError> {
    value.parse::<u32>().map(|value| value.max(1)).map_err(|error| {
        CliError::Usage(format!(
            "invalid MULTIDB_ADMIN_LOGIN_MAX_FAILURES {value}; expected positive integer: {error}"
        ))
    })
}

fn parse_positive_seconds(value: String, name: &str) -> Result<u64, CliError> {
    value
        .parse::<u64>()
        .map(|value| value.max(1))
        .map_err(|error| {
            CliError::Usage(format!(
                "invalid {name} {value}; expected positive seconds: {error}"
            ))
        })
}

fn parse_runtime_mode(value: String) -> Result<ServeRuntimeMode, CliError> {
    match value.as_str() {
        "production" => Ok(ServeRuntimeMode::Production),
        "local-dev" => Ok(ServeRuntimeMode::LocalDev),
        other => Err(CliError::Usage(format!(
            "unsupported MULTIDB_RUNTIME_MODE {other}; expected production or local-dev"
        ))),
    }
}

fn parse_socket_addr(value: String) -> Result<SocketAddr, CliError> {
    value
        .parse()
        .map_err(|error| CliError::Usage(format!("invalid socket address {value}: {error}")))
}

fn pg_tls_from_sources<F>(
    get_env: &F,
    runtime_mode: ServeRuntimeMode,
) -> Result<ServePgTls, CliError>
where
    F: Fn(&str) -> Option<String>,
{
    let tls_disabled = env_bool(get_env, "MULTIDB_PG_TLS_DISABLED")
        || get_env("MULTIDB_PG_TLS_MODE")
            .is_some_and(|value| value.eq_ignore_ascii_case("disabled"));
    if tls_disabled {
        return if runtime_mode == ServeRuntimeMode::LocalDev {
            Ok(ServePgTls::DisabledForLocalDev)
        } else {
            Err(CliError::Usage(
                "PG TLS can only be disabled when MULTIDB_RUNTIME_MODE=local-dev".to_owned(),
            ))
        };
    }

    let cert = get_env("MULTIDB_PG_TLS_CERT").filter(|value| !value.trim().is_empty());
    let key = get_env("MULTIDB_PG_TLS_KEY").filter(|value| !value.trim().is_empty());
    match (cert, key) {
        (Some(cert), Some(key)) => Ok(ServePgTls::Require {
            cert: PathBuf::from(cert),
            key: PathBuf::from(key),
        }),
        _ => Err(CliError::Usage(
            "MULTIDB_PG_TLS_CERT and MULTIDB_PG_TLS_KEY are required unless MULTIDB_RUNTIME_MODE=local-dev disables PG TLS".to_owned(),
        )),
    }
}

fn env_bool<F>(get_env: &F, name: &str) -> bool
where
    F: Fn(&str) -> Option<String>,
{
    get_env(name).is_some_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn required_value(args: &[String], flag: &str) -> Result<String, CliError> {
    optional_value(args, flag).ok_or_else(|| CliError::Usage(format!("missing {flag}")))
}

fn optional_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find(|window| window.first().is_some_and(|value| value == flag))
        .and_then(|window| window.get(1))
        .cloned()
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|value| value == flag)
}

/// The arguments that are neither a `--flag` nor the value after one of `value_flags`, in order.
fn positional_args<'a>(args: &'a [String], value_flags: &[&str]) -> Vec<&'a str> {
    let mut positional = Vec::new();
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if value_flags.contains(&arg.as_str()) {
            iter.next();
        } else if !arg.starts_with("--") {
            positional.push(arg.as_str());
        }
    }
    positional
}

/// Every value of a repeatable flag, in order. Each occurrence may also hold a comma-separated
/// list, so `--override a=1 --override b=2` and `--override a=1,b=2` are the same.
fn values(args: &[String], flag: &str) -> Vec<String> {
    args.windows(2)
        .filter(|window| window.first().is_some_and(|value| value == flag))
        .filter_map(|window| window.get(1))
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
}

fn parse_export_format(value: &str) -> Result<ExportFormat, CliError> {
    match value {
        "csv" => Ok(ExportFormat::Csv),
        "jsonl" => Ok(ExportFormat::Jsonl),
        "parquet" => Ok(ExportFormat::Parquet),
        other => Err(CliError::Usage(format!(
            "unsupported export format {other}"
        ))),
    }
}

fn open_cli_database(args: &[String]) -> Result<multidb::db::Database, CliError> {
    let db = PathBuf::from(required_value(args, "--db")?);
    let selection = parse_profile_selection(&required_value(args, "--profile")?)?;
    Ok(open_database(cli_db_config(selection, db)?)?)
}

fn create_cli_database(args: &[String]) -> Result<multidb::db::Database, CliError> {
    let db = PathBuf::from(required_value(args, "--db")?);
    let selection = parse_profile_selection(&required_value(args, "--profile")?)?;
    Ok(create_database(cli_db_config(selection, db)?)?)
}

fn resume_token(args: &[String]) -> Result<ResumeToken, CliError> {
    let lsn = optional_value(args, "--lsn")
        .map(|value| value.parse())
        .transpose()
        .map_err(|error| CliError::Usage(format!("invalid --lsn: {error}")))?
        .unwrap_or(0);
    let timeline = optional_value(args, "--timeline").unwrap_or_else(|| "default".to_owned());
    Ok(ResumeToken::new(timeline, lsn))
}

fn changefeed_filter(args: &[String]) -> ChangefeedFilter {
    let target = optional_value(args, "--table")
        .map(ChangefeedTarget::Table)
        .or_else(|| optional_value(args, "--collection").map(ChangefeedTarget::Collection))
        .or_else(|| optional_value(args, "--vector").map(ChangefeedTarget::VectorCollection))
        .or_else(|| optional_value(args, "--system").map(ChangefeedTarget::System))
        .unwrap_or(ChangefeedTarget::All);
    ChangefeedFilter { target }
}

/// Parses a `--profile` value or `MULTIDB_PROFILE`: today's storage profile spellings (unchanged),
/// `auto`, then a built-in product slug (its canonical variant) or alias (its variant), in snake
/// or kebab case.
fn parse_profile_selection(value: &str) -> Result<ProfileSelection, CliError> {
    if let Some(profile) = storage_profile_name(value) {
        return Ok(ProfileSelection::Fixed(profile));
    }
    if value.trim().eq_ignore_ascii_case("auto") {
        return Ok(ProfileSelection::Auto);
    }
    if let Some(resolved) = named_profile(value) {
        return Ok(ProfileSelection::Fixed(resolved.storage_profile));
    }
    Err(CliError::Usage(format!(
        "unknown profile {value}{}; expected a storage profile (InMemory, Transactional, \
         Analytical, Document, Vector, TimeSeries, HighDurability, Balanced), a product profile \
         slug or alias (run multidb profile list), or auto",
        did_you_mean(value)
    )))
}

/// The storage profile spellings `--profile` accepted before product slugs and aliases.
fn storage_profile_name(value: &str) -> Option<Profile> {
    match value {
        "InMemory" | "in-memory" | "memory" => Some(Profile::InMemory),
        "Transactional" | "transactional" => Some(Profile::Transactional),
        "Analytical" | "analytical" => Some(Profile::Analytical),
        "Document" | "document" => Some(Profile::Document),
        "Vector" | "vector" => Some(Profile::Vector),
        "TimeSeries" | "time-series" | "time_series" => Some(Profile::TimeSeries),
        "HighDurability" | "high-durability" => Some(Profile::HighDurability),
        "Balanced" | "balanced" => Some(Profile::Balanced),
        _ => None,
    }
}

/// The `DbConfig` of the database at `path`. The CLI and `serve` open single-node CP databases
/// only, so the replication is always CP. `auto` reads only the storage profile of the existing
/// database, without creating anything: a database whose metadata records another replication
/// fails with `ReplicationMismatch`, exactly as it does with a named profile. `auto` is an error
/// for anything but an initialized database. A file that was not closed cleanly (`serve` has no
/// graceful shutdown, so every stop by a signal leaves one) first gets redb's crash recovery,
/// which every opener runs anyway, also for a file that then fails the replication check; `auto`
/// writes nothing else, and nothing at all to a cleanly closed file.
fn cli_db_config(selection: ProfileSelection, path: PathBuf) -> Result<DbConfig, CliError> {
    let ProfileSelection::Fixed(profile) = selection else {
        let probe = match inspect_database_metadata(&path) {
            MetadataProbe::NeedsRecovery => {
                recover_database_file(&path)?;
                inspect_database_metadata(&path)
            }
            probe => probe,
        };
        return match probe {
            MetadataProbe::Initialized(metadata) => {
                let config = DbConfig::on_disk(metadata.storage_profile, path);
                if metadata.replication != config.replication {
                    return Err(multidb::db::ConfigError::ReplicationMismatch {
                        expected: config.replication,
                        found: metadata.replication,
                    }
                    .into());
                }
                Ok(config)
            }
            MetadataProbe::NotFound => Err(CliError::Usage(format!(
                "profile auto reads the profile of an existing database, but nothing exists at \
                 {}; name a profile to create a database",
                path.display()
            ))),
            MetadataProbe::Uninitialized => Err(CliError::Usage(format!(
                "profile auto found no database metadata in {}; name a profile to initialize it",
                path.display()
            ))),
            MetadataProbe::Locked => Err(CliError::Usage(format!(
                "profile auto cannot inspect {}: another process holds the database open; stop \
                 it or name the profile",
                path.display()
            ))),
            MetadataProbe::NeedsRecovery => Err(CliError::Usage(format!(
                "profile auto cannot read the metadata of {}: it still needs crash recovery \
                 after recovering it; name the profile",
                path.display()
            ))),
            MetadataProbe::Unreadable { reason } => Err(CliError::Usage(format!(
                "profile auto cannot read the metadata of {}: {reason}; name the profile",
                path.display()
            ))),
        };
    };
    Ok(DbConfig::on_disk(profile, path))
}

/// Resolves a built-in product slug or alias (snake or kebab case) without overrides.
fn named_profile(name: &str) -> Option<ResolvedProfile> {
    match resolve_named_profile(name, &ProfileKnobOverrides::default()) {
        ProfileResolution::Resolved { profile, .. } => Some(*profile),
        ProfileResolution::Unresolved { .. } | ProfileResolution::Invalid(_) => None,
    }
}

fn product_variants(product: &'static str) -> impl Iterator<Item = &'static ProfileVariant> {
    profile_variants()
        .iter()
        .filter(move |variant| variant.product == product)
}

/// ` (did you mean <slug>?)` for the closest built-in profile name, or nothing.
fn did_you_mean(value: &str) -> String {
    suggest_profile_name(value).map_or_else(String::new, |candidate| {
        format!(" (did you mean {candidate}?)")
    })
}

/// Parses every `--override path=value` into knob and floor overrides.
fn parse_knob_overrides(
    args: &[String],
) -> Result<(ProfileKnobOverrides, FloorOverrides), CliError> {
    let assignments = values(args, "--override");
    if assignments.is_empty() && has_flag(args, "--override") {
        return Err(CliError::Usage(
            "missing value for --override; use --override <knob>=<value> (multidb profile knobs \
             lists the knobs)"
                .to_owned(),
        ));
    }
    let mut knobs = ProfileKnobOverrides::default();
    let mut floors = FloorOverrides::default();
    for input in &assignments {
        parse_knob_assignment(input)
            .and_then(|assignment| assignment.apply(&mut knobs, &mut floors))
            .map_err(|error| CliError::Usage(format!("--override {input}: {error}")))?;
    }
    Ok((knobs, floors))
}

fn usage() -> CliError {
    CliError::Usage(
        "multidb serve (configured through MULTIDB_* environment variables; MULTIDB_SPEC_PATH=<spec> [MULTIDB_SPEC_APPLY=verify|create|additive] builds and checks the database from a spec)\n\
         multidb backup full --db <path> --profile <profile> --dest <dir>\n\
         multidb init --guided --profile <profile> --name <name> [--override <knob>=<value>]... [--profile-name <slug>] [--out <path>] [--format yaml|json] [--force] [--apply [--db <path>] [--confirm <prov_id>]] [--json]\n\
         multidb init --guided --template <template> --name <name> [--out <dir>] [--format yaml|json] [--force] [--apply [--db <path>] [--confirm <prov_id>]] [--json]\n\
         multidb template list [--json]\n\
         multidb template explain <template> [--name <name>] [--output text|json] [--json]\n\
         multidb profile list [--json]\n\
         multidb profile show <profile> [--override <knob>=<value>]... [--output text|json] [--json]\n\
         multidb profile show --db <path> [--output text|json] [--json]\n\
         multidb profile explain --spec <json|yaml> [--output text|json] [--json]\n\
         multidb profile knobs [--output text|json] [--json]\n\
         multidb role list [--json]\n\
         multidb domain list [--json]\n\
         multidb config validate --spec <json|yaml> [--output text|json] [--json]\n\
         multidb config explain --spec <json|yaml> [--output text|json] [--json]\n\
         multidb explain config --spec <json|yaml> [--output text|json] [--json]\n\
         multidb config plan --current <json|yaml> --desired <json|yaml> [--out <plan.json>] [--output text|json] [--json]\n\
         multidb config apply --plan <json> --confirm <plan_id> [--output text|json]\n\
         multidb config plan --spec <json|yaml> [--db <path>] [--out <plan.json> [--force]] [--allow-weakening <path>[,<path>]]... [--allow-destructive <path>[,<path>]]... [--allow-format-upgrade] [--unmanaged ignore|report|fail] [--output text|json] [--json]\n\
         multidb config apply --spec <json|yaml> [--db <path>] (--confirm <prov_id>|--auto-approve) [--allow-weakening <path>[,<path>]]... [--allow-destructive <path>[,<path>]]... [--allow-format-upgrade] [--unmanaged ignore|report|fail] [--output text|json] [--json]\n\
         multidb config drift --spec <json|yaml> [--db <path>] [--unmanaged ignore|report|fail] [--output text|json] [--json]\n\
         multidb config status --db <path> [--output text|json] [--json]\n\
         multidb config export --db <path> [--format yaml|json] [--out <path>] [--force]\n\
         multidb config upgrade --spec <json|yaml> [--out <path>] [--force] [--output text|json] [--json]\n\
         multidb advice list --db <path> --profile <profile> [--output text|json] [--json]\n\
         multidb advice plan --db <path> --profile <profile> --advice-id <id> [--out <plan.json>] [--output text|json] [--json]\n\
         multidb advice reject --db <path> --profile <profile> --advice-id <id> --reason <reason> [--output text|json] [--json]\n\
         multidb backup incr --db <path> --profile <profile> --dest <dir> --parent <backup-dir>\n\
         multidb restore --backup <backup-dir> --dest-db <path> --profile <profile> [--to-lsn <lsn>] [--allow-unmet-floors]\n\
         multidb verify --backup <backup-dir>\n\
         multidb list --backup-root <dir>\n\
         multidb cdc poll --db <path> --profile <profile> [--lsn <lsn>] [--table <name>] [--include-system]\n\
         multidb cdc subscribe --db <path> --profile <profile> --name <name> [--table <name>]\n\
         multidb cdc ack --db <path> --profile <profile> --name <name> --lsn <lsn>\n\
         multidb cdc state --db <path> --profile <profile> --name <name>\n\
         multidb view create --db <path> --profile <profile> --spec-json <json>\n\
         multidb view refresh --db <path> --profile <profile> --name <name>\n\
         multidb view read --db <path> --profile <profile> --name <name>\n\
         multidb import <csv|jsonl|pg> --db <path> --profile <profile> --file <path> [--table <name>] [--batch-size <n>] [--resume-token <path>] [--reject-file <path>]\n\
         multidb admin status --db <path> --profile <profile>\n\
         multidb admin serve --db <path> --profile <profile> [--bind <addr>] [--admin-password <password>|--admin-password-file <path>|--admin-token <token>|--insecure-local-admin]\n\
         multidb migrate value-codec --db <path> --profile <profile>\n\
         <profile> on --profile and MULTIDB_PROFILE: a storage profile (InMemory, Transactional, Analytical, Document, Vector, TimeSeries, HighDurability, Balanced), a product slug or alias from multidb profile list (snake or kebab case), or auto to read an existing database (not accepted by restore)\n\
         --override takes a knob path from multidb profile knobs; repeat it or separate assignments with commas\n\
         config plan|apply|drift --spec provision the database at --db, else at the spec storage_path relative to the spec file; exit 0 ready, applied or in sync, 1 runtime error, locked database or partial apply, 2 invalid, blocked, flags or confirmation missing, 3 drift, 4 plan changed"
            .to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    #[cfg(unix)]
    use super::write_through_temp_file;
    use super::{
        CliError, ProfileSelection, ServePgTls, ServeSpecConfig, cli_db_config,
        format_metadata_probe, parse_profile_selection, run, serve_config_from_sources,
        serve_database, values,
    };
    use multidb::{
        backup::{
            BackupConfig, BackupError, RestoreConfig, RestoreTarget, full_backup,
            restore_backup_with_config,
        },
        config_spec::{
            CollectionRole, CollectionRoleSpec, ConsistencyMode, DatabaseSpec, DeploymentMode,
            GuaranteeValidator, KNOB_REGISTRY, MigrationPlanner, PolicyCompiler, ProfileResolution,
            RESERVED_KNOBS, WriteAck, resolve_named_profile,
            spec_io::{MAX_SPEC_BYTES, SpecFormat, decode_spec_bytes},
        },
        db::{
            ConfigError, DbConfig, EncryptionConfig, MetadataProbe, OperationalConfig, Profile,
            ReplicationKind, create_database, create_database_with_profile,
            inspect_database_metadata, open_database,
        },
        tuning::WorkloadSample,
    };
    use serde_json::{Value, json};
    use std::collections::BTreeMap;

    fn write_spec(
        path: &std::path::Path,
        spec: &DatabaseSpec,
    ) -> Result<(), Box<dyn std::error::Error>> {
        std::fs::write(path, serde_json::to_string_pretty(spec)?)?;
        Ok(())
    }

    fn write_yaml_spec(
        path: &std::path::Path,
        spec: &DatabaseSpec,
    ) -> Result<(), Box<dyn std::error::Error>> {
        std::fs::write(path, serde_norway::to_string(spec)?)?;
        Ok(())
    }

    fn env_map(values: &[(&str, &str)]) -> BTreeMap<String, String> {
        values
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn serve_config_allows_missing_admin_env_for_stored_credentials()
    -> Result<(), Box<dyn std::error::Error>> {
        let env = env_map(&[
            ("MULTIDB_RUNTIME_MODE", "local-dev"),
            ("MULTIDB_PG_TLS_MODE", "disabled"),
            ("MULTIDB_PG_PASSWORD", "pg-secret"),
        ]);

        let config = serve_config_from_sources(|name| env.get(name).cloned())?;

        assert_eq!(config.admin_token, None);
        assert_eq!(config.admin_password, None);
        assert_eq!(config.pg_password, "pg-secret");
        Ok(())
    }

    #[test]
    fn serve_config_accepts_local_dev_file_secrets() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let admin_file = temp.path().join("admin-token");
        let pg_file = temp.path().join("pg-password");
        std::fs::write(&admin_file, "admin-secret\n")?;
        std::fs::write(&pg_file, "pg-secret\n")?;
        let env = env_map(&[
            ("MULTIDB_RUNTIME_MODE", "local-dev"),
            ("MULTIDB_PG_TLS_MODE", "disabled"),
            (
                "MULTIDB_ADMIN_TOKEN_FILE",
                admin_file.to_str().ok_or("non-utf8 path")?,
            ),
            (
                "MULTIDB_PG_PASSWORD_FILE",
                pg_file.to_str().ok_or("non-utf8 path")?,
            ),
            ("MULTIDB_DB_PATH", "target/docker-smoke/multidb.redb"),
            ("MULTIDB_BIND", "0.0.0.0:18080"),
            ("MULTIDB_PG_BIND", "0.0.0.0:15432"),
            ("MULTIDB_PROFILE", "balanced"),
        ]);

        let config = serve_config_from_sources(|name| env.get(name).cloned())?;

        assert_eq!(config.admin_token.as_deref(), Some("admin-secret"));
        assert_eq!(config.pg_password, "pg-secret");
        assert_eq!(config.profile, ProfileSelection::Fixed(Profile::Balanced));
        assert_eq!(config.pg_tls, ServePgTls::DisabledForLocalDev);
        assert_eq!(config.admin_bind.port(), 18_080);
        assert_eq!(config.pg_bind.port(), 15_432);
        Ok(())
    }

    #[test]
    fn serve_config_prefers_admin_password_file_over_env() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp = tempdir()?;
        let admin_file = temp.path().join("admin-password");
        std::fs::write(&admin_file, "file-secret\n")?;
        let env = env_map(&[
            ("MULTIDB_RUNTIME_MODE", "local-dev"),
            ("MULTIDB_PG_TLS_MODE", "disabled"),
            ("MULTIDB_ADMIN_PASSWORD", "env-secret"),
            (
                "MULTIDB_ADMIN_PASSWORD_FILE",
                admin_file.to_str().ok_or("non-utf8 path")?,
            ),
            ("MULTIDB_ADMIN_SESSION_TTL_SECONDS", "1"),
            ("MULTIDB_ADMIN_PASSWORD_RESET", "true"),
            ("MULTIDB_PG_PASSWORD", "pg-secret"),
        ]);

        let config = serve_config_from_sources(|name| env.get(name).cloned())?;

        assert_eq!(config.admin_password.as_deref(), Some("file-secret"));
        assert_eq!(
            config.admin_session_ttl_seconds,
            multidb::admin::MIN_ADMIN_SESSION_TTL_SECONDS
        );
        assert!(config.admin_password_reset);
        Ok(())
    }

    #[test]
    fn serve_config_parses_admin_login_rate_limit() -> Result<(), Box<dyn std::error::Error>> {
        let env = env_map(&[
            ("MULTIDB_RUNTIME_MODE", "local-dev"),
            ("MULTIDB_PG_TLS_MODE", "disabled"),
            ("MULTIDB_PG_PASSWORD", "pg-secret"),
            ("MULTIDB_ADMIN_LOGIN_MAX_FAILURES", "0"),
            ("MULTIDB_ADMIN_LOGIN_WINDOW_SECONDS", "2"),
            ("MULTIDB_ADMIN_LOGIN_LOCKOUT_SECONDS", "3"),
        ]);

        let config = serve_config_from_sources(|name| env.get(name).cloned())?;

        assert_eq!(config.admin_login_rate_limit.max_failures, 1);
        assert_eq!(config.admin_login_rate_limit.window_seconds, 2);
        assert_eq!(config.admin_login_rate_limit.lockout_seconds, 3);
        Ok(())
    }

    #[test]
    fn serve_config_requires_tls_in_production() {
        let env = env_map(&[
            ("MULTIDB_ADMIN_TOKEN", "admin-secret"),
            ("MULTIDB_PG_PASSWORD", "pg-secret"),
        ]);

        let error = serve_config_from_sources(|name| env.get(name).cloned());

        assert!(
            matches!(error, Err(CliError::Usage(ref message)) if message.contains("MULTIDB_PG_TLS_CERT")),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn config_validate_returns_zero_for_valid_json_spec() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp = tempdir()?;
        let spec_path = temp.path().join("valid.json");
        let spec = DatabaseSpec::from_db_config(
            "valid",
            &DbConfig::on_disk(Profile::Balanced, "valid.redb"),
        );
        write_spec(&spec_path, &spec)?;

        let outcome = run(vec![
            "config".to_owned(),
            "validate".to_owned(),
            "--spec".to_owned(),
            spec_path.display().to_string(),
        ])?;

        assert_eq!(outcome.exit_code, 0);
        assert!(outcome.message.contains("valid=true"));
        Ok(())
    }

    #[test]
    fn config_validate_returns_two_for_invalid_json_spec() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp = tempdir()?;
        let spec_path = temp.path().join("invalid.json");
        let mut spec = DatabaseSpec::from_db_config(
            "invalid",
            &DbConfig::on_disk(Profile::Vector, "invalid.redb"),
        );
        spec.domains[0].mode = ConsistencyMode::LocalSnapshot;
        spec.collections.push(CollectionRoleSpec {
            name: "memory".to_owned(),
            role: CollectionRole::VectorMemory,
            domain: "primary".to_owned(),
            indexes: Vec::new(),
            physical: None,
        });
        write_spec(&spec_path, &spec)?;

        let outcome = run(vec![
            "config".to_owned(),
            "validate".to_owned(),
            "--spec".to_owned(),
            spec_path.display().to_string(),
            "--output".to_owned(),
            "json".to_owned(),
        ])?;

        assert_eq!(outcome.exit_code, 2);
        assert!(outcome.message.contains("\"valid\": false"));
        assert!(outcome.message.contains("VECTOR_INDEX_REQUIRED"));
        Ok(())
    }

    #[test]
    fn config_validate_accepts_yaml_spec() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_path = temp.path().join("spec.yaml");
        let spec = DatabaseSpec::from_db_config(
            "valid_yaml",
            &DbConfig::on_disk(Profile::Balanced, "valid_yaml.redb"),
        );
        write_yaml_spec(&spec_path, &spec)?;

        let outcome = run(vec![
            "config".to_owned(),
            "validate".to_owned(),
            "--spec".to_owned(),
            spec_path.display().to_string(),
        ])?;

        assert_eq!(outcome.exit_code, 0);
        assert!(outcome.message.contains("valid=true"));
        Ok(())
    }

    #[test]
    fn config_validate_text_output_includes_actionable_suggestion()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_path = temp.path().join("local-ack.json");
        let mut spec = DatabaseSpec::from_db_config(
            "local_ack",
            &DbConfig::on_disk(Profile::Transactional, "local.redb"),
        );
        spec.guarantees.write_ack = WriteAck::Local;
        write_spec(&spec_path, &spec)?;

        let outcome = run(vec![
            "config".to_owned(),
            "validate".to_owned(),
            "--spec".to_owned(),
            spec_path.display().to_string(),
        ])?;

        assert_eq!(outcome.exit_code, 2);
        assert!(outcome.message.contains("suggestion:"));
        assert!(outcome.message.contains("CP_LOCAL_ACK"));
        Ok(())
    }

    #[test]
    fn config_explain_json_includes_decisions() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_path = temp.path().join("explain.json");
        let mut spec = DatabaseSpec::from_db_config(
            "explain",
            &DbConfig::on_disk(Profile::Vector, "explain.redb"),
        );
        spec.collections.push(CollectionRoleSpec {
            name: "memory".to_owned(),
            role: CollectionRole::VectorMemory,
            domain: "primary".to_owned(),
            indexes: vec![multidb::config_spec::CollectionIndexKind::Vector],
            physical: None,
        });
        write_spec(&spec_path, &spec)?;

        let outcome = run(vec![
            "config".to_owned(),
            "explain".to_owned(),
            "--spec".to_owned(),
            spec_path.display().to_string(),
            "--output".to_owned(),
            "json".to_owned(),
        ])?;

        assert_eq!(outcome.exit_code, 0);
        assert!(outcome.message.contains("\"decisions\""));
        assert!(outcome.message.contains("\"compiled_policy\""));
        assert!(outcome.message.contains("vector_hnsw"));
        Ok(())
    }

    #[test]
    fn json_flag_matches_output_json() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_path = temp.path().join("valid.json");
        let spec = DatabaseSpec::from_db_config(
            "valid",
            &DbConfig::on_disk(Profile::Balanced, "valid.redb"),
        );
        write_spec(&spec_path, &spec)?;

        let explicit = run(vec![
            "config".to_owned(),
            "validate".to_owned(),
            "--spec".to_owned(),
            spec_path.display().to_string(),
            "--output".to_owned(),
            "json".to_owned(),
        ])?;
        let shorthand = run(vec![
            "config".to_owned(),
            "validate".to_owned(),
            "--spec".to_owned(),
            spec_path.display().to_string(),
            "--json".to_owned(),
        ])?;

        assert_eq!(explicit.message, shorthand.message);
        let parsed = serde_json::from_str::<serde_json::Value>(&shorthand.message)?;
        assert_eq!(parsed["valid"], true);
        Ok(())
    }

    #[test]
    fn config_plan_json_reports_plan_id_and_steps() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let current_path = temp.path().join("current.json");
        let desired_path = temp.path().join("desired.json");
        let current = DatabaseSpec::from_db_config(
            "plan",
            &DbConfig::on_disk(Profile::Balanced, "plan.redb"),
        );
        let mut desired = current.clone();
        desired
            .operation_hints
            .insert("planner.sample_rows".to_owned(), "1000".to_owned());
        write_spec(&current_path, &current)?;
        write_spec(&desired_path, &desired)?;

        let outcome = run(vec![
            "config".to_owned(),
            "plan".to_owned(),
            "--current".to_owned(),
            current_path.display().to_string(),
            "--desired".to_owned(),
            desired_path.display().to_string(),
            "--output".to_owned(),
            "json".to_owned(),
        ])?;

        assert_eq!(outcome.exit_code, 0);
        assert!(outcome.message.contains("\"plan_id\""));
        assert!(outcome.message.contains("\"steps\""));
        assert!(outcome.message.contains("operation_hints"));
        Ok(())
    }

    #[test]
    fn config_plan_accepts_yaml_and_writes_json_out_file() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp = tempdir()?;
        let current_path = temp.path().join("current.yaml");
        let desired_path = temp.path().join("desired.yaml");
        let out_path = temp.path().join("plan.json");
        let current = DatabaseSpec::from_db_config(
            "plan",
            &DbConfig::on_disk(Profile::Balanced, "plan.redb"),
        );
        let mut desired = current.clone();
        desired
            .operation_hints
            .insert("planner.sample_rows".to_owned(), "1000".to_owned());
        write_yaml_spec(&current_path, &current)?;
        write_yaml_spec(&desired_path, &desired)?;

        let outcome = run(vec![
            "config".to_owned(),
            "plan".to_owned(),
            "--current".to_owned(),
            current_path.display().to_string(),
            "--desired".to_owned(),
            desired_path.display().to_string(),
            "--out".to_owned(),
            out_path.display().to_string(),
            "--json".to_owned(),
        ])?;

        assert_eq!(outcome.exit_code, 0);
        let message = serde_json::from_str::<serde_json::Value>(&outcome.message)?;
        assert_eq!(message["valid"], true);
        assert_eq!(message["path"], out_path.display().to_string());
        let plan = serde_json::from_str::<serde_json::Value>(&std::fs::read_to_string(out_path)?)?;
        assert!(plan["plan_id"].as_str().is_some());
        assert!(plan["steps"].as_array().is_some());
        Ok(())
    }

    fn run_validate(path: &std::path::Path, json: bool) -> Result<super::CliRun, CliError> {
        let mut args = vec![
            "config".to_owned(),
            "validate".to_owned(),
            "--spec".to_owned(),
            path.display().to_string(),
        ];
        if json {
            args.push("--json".to_owned());
        }
        run(args)
    }

    #[test]
    fn config_validate_reports_oversized_specs_as_invalid() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp = tempdir()?;
        let spec_path = temp.path().join("huge.yaml");
        let mut yaml = serde_norway::to_string(&DatabaseSpec::from_db_config(
            "huge",
            &DbConfig::on_disk(Profile::Balanced, "huge.redb"),
        ))?;
        yaml.push_str(&"#".repeat(multidb::config_spec::spec_io::MAX_SPEC_BYTES));
        std::fs::write(&spec_path, yaml)?;

        let text = run_validate(&spec_path, false)?;
        let json = run_validate(&spec_path, true)?;

        assert_eq!(text.exit_code, 2);
        assert!(text.message.contains("valid=false"), "{}", text.message);
        assert!(text.message.contains("SPEC_TOO_LARGE"), "{}", text.message);
        assert_eq!(json.exit_code, 2);
        let report = serde_json::from_str::<serde_json::Value>(&json.message)?;
        assert_eq!(report["valid"], false);
        assert_eq!(report["issues"][0]["code"], "SPEC_TOO_LARGE");
        Ok(())
    }

    #[test]
    fn config_commands_report_unsupported_versions() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let current_path = temp.path().join("current.json");
        let desired_path = temp.path().join("desired.json");
        let current = DatabaseSpec::from_db_config(
            "versions",
            &DbConfig::on_disk(Profile::Balanced, "versions.redb"),
        );
        let mut desired = serde_json::to_value(&current)?;
        desired["version"] = serde_json::json!(3);
        write_spec(&current_path, &current)?;
        std::fs::write(&desired_path, serde_json::to_string_pretty(&desired)?)?;

        let validate = run_validate(&desired_path, true)?;
        assert_eq!(validate.exit_code, 2);
        let report = serde_json::from_str::<serde_json::Value>(&validate.message)?;
        assert_eq!(report["issues"][0]["code"], "UNSUPPORTED_VERSION");

        let explain = run(vec![
            "explain".to_owned(),
            "config".to_owned(),
            "--spec".to_owned(),
            desired_path.display().to_string(),
        ])?;
        assert_eq!(explain.exit_code, 2);
        assert!(explain.message.contains("UNSUPPORTED_VERSION"));

        let plan = run(vec![
            "config".to_owned(),
            "plan".to_owned(),
            "--current".to_owned(),
            current_path.display().to_string(),
            "--desired".to_owned(),
            desired_path.display().to_string(),
        ])?;
        assert_eq!(plan.exit_code, 2);
        assert!(plan.message.starts_with("--desired "), "{}", plan.message);
        assert!(
            plan.message.contains("UNSUPPORTED_VERSION"),
            "{}",
            plan.message
        );
        Ok(())
    }

    #[test]
    fn config_validate_accepts_v2_custom_profiles() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_path = temp.path().join("custom.yaml");
        let mut spec = DatabaseSpec::from_db_config(
            "orders",
            &DbConfig::on_disk(Profile::Transactional, "orders.redb"),
        );
        spec.version = 2;
        spec.profile = "orders_durable".to_owned();
        spec.custom_profile = Some(serde_json::from_value(serde_json::json!({
            "extends": "high_durability",
            "knobs": { "cache": { "max_entries": 8192 } }
        }))?);
        write_yaml_spec(&spec_path, &spec)?;

        let outcome = run_validate(&spec_path, false)?;

        assert_eq!(outcome.exit_code, 0, "{}", outcome.message);
        assert!(outcome.message.contains("valid=true status=Custom"));
        assert!(outcome.message.contains("CUSTOM_PROFILE_DEFINED"));
        Ok(())
    }

    #[test]
    fn config_validate_keeps_yaml_syntax_errors_as_cli_errors()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_path = temp.path().join("broken.yaml");
        std::fs::write(&spec_path, "version: 1\nname: [unclosed\n")?;

        let error = run_validate(&spec_path, false);

        assert!(matches!(error, Err(CliError::Yaml(_))), "{error:?}");
        Ok(())
    }

    #[test]
    fn advice_list_plan_and_reject_support_json_and_out_file()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let db_path = temp.path().join("advice.redb");
        let database = create_database(DbConfig::on_disk(Profile::Balanced, &db_path))?;
        database.record_workload_sample(
            &WorkloadSample::new("SELECT * FROM users WHERE age = 37")
                .with_observed_rows(1, 2_000)
                .with_access("users", "age"),
        )?;
        drop(database);

        let list = run(vec![
            "advice".to_owned(),
            "list".to_owned(),
            "--db".to_owned(),
            db_path.display().to_string(),
            "--profile".to_owned(),
            "balanced".to_owned(),
            "--json".to_owned(),
        ])?;
        assert_eq!(list.exit_code, 0);
        let report = serde_json::from_str::<serde_json::Value>(&list.message)?;
        let advice = report["recommendations"]
            .as_array()
            .and_then(|items| items.iter().find(|item| item["code"] == "CREATE_INDEX"))
            .ok_or("missing create-index advice")?;
        let advice_id = advice["id"].as_str().ok_or("missing advice id")?.to_owned();

        let plan_path = temp.path().join("advice-plan.json");
        let plan = run(vec![
            "advice".to_owned(),
            "plan".to_owned(),
            "--db".to_owned(),
            db_path.display().to_string(),
            "--profile".to_owned(),
            "balanced".to_owned(),
            "--advice-id".to_owned(),
            advice_id.clone(),
            "--out".to_owned(),
            plan_path.display().to_string(),
            "--json".to_owned(),
        ])?;
        assert_eq!(plan.exit_code, 0);
        let summary = serde_json::from_str::<serde_json::Value>(&plan.message)?;
        assert_eq!(summary["valid"], true);
        assert_eq!(summary["path"], plan_path.display().to_string());
        let written =
            serde_json::from_str::<serde_json::Value>(&std::fs::read_to_string(plan_path)?)?;
        assert_eq!(written["valid"], true);

        let reject = run(vec![
            "advice".to_owned(),
            "reject".to_owned(),
            "--db".to_owned(),
            db_path.display().to_string(),
            "--profile".to_owned(),
            "balanced".to_owned(),
            "--advice-id".to_owned(),
            advice_id,
            "--reason".to_owned(),
            "not now".to_owned(),
            "--json".to_owned(),
        ])?;
        assert_eq!(reject.exit_code, 0);
        assert!(reject.message.contains("\"status\": \"rejected\""));
        Ok(())
    }

    #[test]
    fn config_apply_rejects_wrong_confirmation() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let plan_path = temp.path().join("plan.json");
        let current = DatabaseSpec::from_db_config(
            "apply",
            &DbConfig::on_disk(Profile::Balanced, "apply.redb"),
        );
        let mut desired = current.clone();
        desired
            .operation_hints
            .insert("planner.sample_rows".to_owned(), "1000".to_owned());
        let plan = MigrationPlanner::plan(&current, &desired);
        std::fs::write(&plan_path, serde_json::to_string_pretty(&plan)?)?;

        let outcome = run(vec![
            "config".to_owned(),
            "apply".to_owned(),
            "--plan".to_owned(),
            plan_path.display().to_string(),
            "--confirm".to_owned(),
            "wrong".to_owned(),
            "--output".to_owned(),
            "json".to_owned(),
        ])?;

        assert_eq!(outcome.exit_code, 2);
        assert!(outcome.message.contains("\"status\": \"rejected\""));
        assert!(outcome.message.contains("confirmation id mismatch"));
        Ok(())
    }

    #[test]
    fn explain_config_alias_accepts_yaml_spec() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_path = temp.path().join("spec.yaml");
        let mut spec = DatabaseSpec::from_db_config(
            "explain",
            &DbConfig::on_disk(Profile::Vector, "explain.redb"),
        );
        spec.collections.push(CollectionRoleSpec {
            name: "memory".to_owned(),
            role: CollectionRole::VectorMemory,
            domain: "primary".to_owned(),
            indexes: vec![multidb::config_spec::CollectionIndexKind::Vector],
            physical: None,
        });
        write_yaml_spec(&spec_path, &spec)?;

        let outcome = run(vec![
            "explain".to_owned(),
            "config".to_owned(),
            "--spec".to_owned(),
            spec_path.display().to_string(),
            "--json".to_owned(),
        ])?;

        assert_eq!(outcome.exit_code, 0);
        let report = serde_json::from_str::<serde_json::Value>(&outcome.message)?;
        assert_eq!(report["validation"]["valid"], true);
        assert!(outcome.message.contains("vector_hnsw"));
        Ok(())
    }

    #[test]
    fn init_guided_writes_valid_yaml_spec_and_refuses_overwrite()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_path = temp.path().join("multidb.yaml");

        let outcome = run(vec![
            "init".to_owned(),
            "--guided".to_owned(),
            "--profile".to_owned(),
            "desktop_app_embedded".to_owned(),
            "--name".to_owned(),
            "Patchspire".to_owned(),
            "--out".to_owned(),
            spec_path.display().to_string(),
        ])?;

        assert_eq!(outcome.exit_code, 0);
        assert!(outcome.message.contains("created config"));
        let spec = serde_norway::from_str::<DatabaseSpec>(&std::fs::read_to_string(&spec_path)?)?;
        assert_eq!(spec.name, "Patchspire");
        assert_eq!(spec.profile, "desktop_app_embedded");
        assert_eq!(
            spec.deployment.storage_path,
            Some("patchspire.redb".to_owned())
        );

        let error = run(vec![
            "init".to_owned(),
            "--guided".to_owned(),
            "--profile".to_owned(),
            "desktop_app_embedded".to_owned(),
            "--name".to_owned(),
            "Patchspire".to_owned(),
            "--out".to_owned(),
            spec_path.display().to_string(),
        ]);
        assert!(
            matches!(error, Err(CliError::Usage(ref message)) if message.contains("--force")),
            "unexpected error: {error:?}"
        );
        Ok(())
    }

    #[test]
    fn init_guided_json_stdout_is_parseable() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_path = temp.path().join("multidb.json");

        let outcome = run(vec![
            "init".to_owned(),
            "--guided".to_owned(),
            "--profile".to_owned(),
            "time-series".to_owned(),
            "--name".to_owned(),
            "Metrics".to_owned(),
            "--format".to_owned(),
            "json".to_owned(),
            "--out".to_owned(),
            spec_path.display().to_string(),
            "--json".to_owned(),
        ])?;

        assert_eq!(outcome.exit_code, 0);
        let report = serde_json::from_str::<serde_json::Value>(&outcome.message)?;
        assert_eq!(report["profile"], "analytics_columnar");
        assert_eq!(report["valid"], true);
        // `time-series` names the non-canonical time_series variant: the warning is a JSON field.
        assert_eq!(report["variant"], "analytical");
        assert_eq!(report["storage_profile"], "Analytical");
        assert!(
            report["warnings"][0]
                .as_str()
                .is_some_and(|warning| warning.contains("time_series variant")),
            "{report}"
        );
        let spec = serde_json::from_str::<DatabaseSpec>(&std::fs::read_to_string(spec_path)?)?;
        assert_eq!(spec.profile, "analytics_columnar");
        Ok(())
    }

    #[test]
    fn catalog_list_commands_have_text_and_json_output() -> Result<(), Box<dyn std::error::Error>> {
        let profiles_text = run(vec!["profile".to_owned(), "list".to_owned()])?;
        assert!(profiles_text.message.contains("game_local_balanced"));
        assert!(profiles_text.message.contains("time_series"));

        let profiles_json = run(vec![
            "profile".to_owned(),
            "list".to_owned(),
            "--json".to_owned(),
        ])?;
        let profiles = serde_json::from_str::<serde_json::Value>(&profiles_json.message)?;
        assert!(profiles.as_array().is_some_and(|items| items.len() == 6));

        let roles_json = run(vec![
            "role".to_owned(),
            "list".to_owned(),
            "--json".to_owned(),
        ])?;
        assert!(roles_json.message.contains("vector_memory"));
        serde_json::from_str::<serde_json::Value>(&roles_json.message)?;

        let domains_text = run(vec!["domain".to_owned(), "list".to_owned()])?;
        assert!(domains_text.message.contains("strong_cp"));
        assert!(domains_text.message.contains("eventual_ap"));
        Ok(())
    }

    #[test]
    fn template_list_has_text_and_json_output() -> Result<(), Box<dyn std::error::Error>> {
        let text = run(vec!["template".to_owned(), "list".to_owned()])?;
        assert_eq!(text.exit_code, 0);
        assert!(text.message.contains("game-save"));
        assert!(text.message.contains("analytics_columnar"));

        let json = run(vec![
            "template".to_owned(),
            "list".to_owned(),
            "--json".to_owned(),
        ])?;
        let templates = serde_json::from_str::<serde_json::Value>(&json.message)?;
        assert!(templates.as_array().is_some_and(|items| items.len() == 5));
        assert!(json.message.contains("ai-memory"));
        let templates = templates.as_array().ok_or("template list is an array")?;
        assert!(
            templates
                .iter()
                .all(|template| template["spec_version"] == 2)
        );
        let ai = templates
            .iter()
            .find(|template| template["slug"] == "ai-memory")
            .ok_or("ai-memory is listed")?;
        assert_eq!(ai["collections"][0]["name"], "memories");
        assert_eq!(
            ai["collections"][0]["physical"],
            json!({ "dim": 1536, "metric": "cosine" })
        );
        assert_eq!(
            ai["collections"][1]["physical"],
            json!({ "document_indexes": [{ "path": "subject" }] })
        );
        assert_eq!(ai["collections"][2]["physical"], Value::Null);
        Ok(())
    }

    #[test]
    fn template_explain_json_includes_spec_and_explain() -> Result<(), Box<dyn std::error::Error>> {
        let outcome = run(vec![
            "template".to_owned(),
            "explain".to_owned(),
            "ai-memory".to_owned(),
            "--name".to_owned(),
            "Agent Memory".to_owned(),
            "--json".to_owned(),
        ])?;

        assert_eq!(outcome.exit_code, 0);
        let report = serde_json::from_str::<serde_json::Value>(&outcome.message)?;
        assert_eq!(report["template"], "ai-memory");
        assert_eq!(report["spec"]["profile"], "ai_agent_memory");
        assert_eq!(report["spec"]["version"], 2);
        assert_eq!(report["spec"]["collections"][0]["physical"]["dim"], 1536);
        assert_eq!(report["explain"]["validation"]["valid"], true);
        assert!(outcome.message.contains("vector_hnsw"));

        let text = run(strings(&["template", "explain", "analytics"]))?;
        assert_eq!(text.exit_code, 0, "{}", text.message);
        assert!(
            text.message
                .starts_with("template=analytics title=Analytics profile=analytics_columnar spec_version=2 valid=true"),
            "{}",
            text.message
        );
        Ok(())
    }

    #[test]
    fn init_guided_template_writes_artifacts_and_refuses_non_empty()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let out_dir = temp.path().join("secure");

        let outcome = run(vec![
            "init".to_owned(),
            "--guided".to_owned(),
            "--template".to_owned(),
            "secure-saas".to_owned(),
            "--name".to_owned(),
            "Secure App".to_owned(),
            "--out".to_owned(),
            out_dir.display().to_string(),
            "--json".to_owned(),
        ])?;

        assert_eq!(outcome.exit_code, 0);
        let summary = serde_json::from_str::<serde_json::Value>(&outcome.message)?;
        assert_eq!(summary["template"], "secure-saas");
        assert_eq!(summary["profile"], "secure_app");
        assert_eq!(summary["valid"], true);

        let spec_path = out_dir.join("multidb.yaml");
        let spec = serde_norway::from_str::<DatabaseSpec>(&std::fs::read_to_string(&spec_path)?)?;
        assert_eq!(spec.name, "Secure App");
        assert_eq!(spec.profile, "secure_app");
        assert!(spec.guarantees.encryption.at_rest);
        assert!(out_dir.join("README.md").exists());
        assert!(out_dir.join("seed.json").exists());
        assert!(out_dir.join("smoke.ps1").exists());

        let error = run(vec![
            "init".to_owned(),
            "--guided".to_owned(),
            "--template".to_owned(),
            "secure-saas".to_owned(),
            "--name".to_owned(),
            "Secure App".to_owned(),
            "--out".to_owned(),
            out_dir.display().to_string(),
        ]);
        assert!(
            matches!(error, Err(CliError::Usage(ref message)) if message.contains("--force")),
            "unexpected error: {error:?}"
        );
        Ok(())
    }

    #[test]
    fn init_guided_template_rejects_profile_conflict() -> Result<(), Box<dyn std::error::Error>> {
        let error = run(vec![
            "init".to_owned(),
            "--guided".to_owned(),
            "--template".to_owned(),
            "game-save".to_owned(),
            "--profile".to_owned(),
            "balanced".to_owned(),
            "--name".to_owned(),
            "conflict".to_owned(),
        ]);

        assert!(
            matches!(error, Err(CliError::Usage(ref message)) if message.contains("either --profile or --template")),
            "unexpected error: {error:?}"
        );
        Ok(())
    }

    #[test]
    fn init_guided_template_unknown_suggests_template_list()
    -> Result<(), Box<dyn std::error::Error>> {
        let error = run(vec![
            "init".to_owned(),
            "--guided".to_owned(),
            "--template".to_owned(),
            "unknown".to_owned(),
            "--name".to_owned(),
            "demo".to_owned(),
        ]);

        assert!(
            matches!(error, Err(CliError::Template(ref template_error)) if template_error.to_string().contains("template list")),
            "unexpected error: {error:?}"
        );
        Ok(())
    }

    #[test]
    fn init_guided_unknown_profile_suggests_profile_list() -> Result<(), Box<dyn std::error::Error>>
    {
        let error = run(vec![
            "init".to_owned(),
            "--guided".to_owned(),
            "--profile".to_owned(),
            "unknown".to_owned(),
            "--name".to_owned(),
            "demo".to_owned(),
        ]);

        assert!(
            matches!(error, Err(CliError::Usage(ref message)) if message.contains("profile list")),
            "unexpected error: {error:?}"
        );
        Ok(())
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn path_arg(path: &std::path::Path) -> String {
        path.display().to_string()
    }

    fn json_message(outcome: &super::CliRun) -> Result<Value, Box<dyn std::error::Error>> {
        Ok(serde_json::from_str::<Value>(&outcome.message)?)
    }

    fn knob_row<'a>(
        report: &'a Value,
        path: &str,
    ) -> Result<&'a Value, Box<dyn std::error::Error>> {
        report["rows"]
            .as_array()
            .and_then(|rows| rows.iter().find(|row| row["path"] == path))
            .ok_or_else(|| format!("missing knob row {path}: {report}").into())
    }

    fn has_issue(report: &Value, code: &str) -> bool {
        report["issues"]
            .as_array()
            .is_some_and(|issues| issues.iter().any(|issue| issue["code"] == code))
    }

    #[test]
    fn values_collects_repeated_and_comma_separated_flags() {
        let args = strings(&[
            "show",
            "secure_app",
            "--override",
            "cache.max_entries=1,query.timeout_ms=2",
            "--json",
            "--override",
            " floors.audit=true ",
        ]);

        assert_eq!(
            values(&args, "--override"),
            strings(&[
                "cache.max_entries=1",
                "query.timeout_ms=2",
                "floors.audit=true"
            ])
        );
        assert!(values(&args, "--missing").is_empty());
    }

    #[test]
    fn parse_profile_selection_keeps_storage_names_and_adds_slugs_aliases_and_auto()
    -> Result<(), Box<dyn std::error::Error>> {
        let storage_names = [
            ("InMemory", Profile::InMemory),
            ("in-memory", Profile::InMemory),
            ("memory", Profile::InMemory),
            ("Transactional", Profile::Transactional),
            ("transactional", Profile::Transactional),
            ("Analytical", Profile::Analytical),
            ("analytical", Profile::Analytical),
            ("Document", Profile::Document),
            ("document", Profile::Document),
            ("Vector", Profile::Vector),
            ("vector", Profile::Vector),
            ("TimeSeries", Profile::TimeSeries),
            ("time-series", Profile::TimeSeries),
            ("time_series", Profile::TimeSeries),
            ("HighDurability", Profile::HighDurability),
            ("high-durability", Profile::HighDurability),
            ("Balanced", Profile::Balanced),
            ("balanced", Profile::Balanced),
        ];
        let aliases = [
            ("in_memory", Profile::InMemory),
            ("high_durability", Profile::HighDurability),
            ("High-Durability", Profile::HighDurability),
        ];
        for (value, profile) in storage_names.into_iter().chain(aliases) {
            assert_eq!(
                parse_profile_selection(value)?,
                ProfileSelection::Fixed(profile),
                "{value}"
            );
        }

        let products = [
            ("game_local_balanced", Profile::Balanced),
            ("desktop_app_embedded", Profile::Document),
            ("ai_agent_memory", Profile::Vector),
            ("secure_app", Profile::Transactional),
            ("production_cp", Profile::Transactional),
            ("analytics_columnar", Profile::Analytical),
        ];
        for (slug, profile) in products {
            for value in [slug.to_owned(), slug.replace('_', "-")] {
                assert_eq!(
                    parse_profile_selection(&value)?,
                    ProfileSelection::Fixed(profile),
                    "{value}"
                );
            }
        }

        assert_eq!(parse_profile_selection("auto")?, ProfileSelection::Auto);
        let unknown = parse_profile_selection("secure_ap");
        assert!(
            matches!(unknown, Err(CliError::Usage(ref message))
                if message.starts_with("unknown profile secure_ap")
                    && message.contains("did you mean secure_app?")),
            "unexpected result: {unknown:?}"
        );
        Ok(())
    }

    #[test]
    fn profile_auto_reads_an_existing_database_and_refuses_missing_ones()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let db_path = temp.path().join("vectors.redb");
        let config = DbConfig::on_disk(Profile::Vector, &db_path);
        drop(create_database(config.clone())?);

        assert_eq!(
            cli_db_config(ProfileSelection::Auto, db_path.clone())?,
            config
        );
        let advice = run(strings(&[
            "advice",
            "list",
            "--db",
            &path_arg(&db_path),
            "--profile",
            "auto",
            "--json",
        ]))?;
        assert_eq!(advice.exit_code, 0, "{}", advice.message);

        let missing = temp.path().join("missing.redb");
        let error = cli_db_config(ProfileSelection::Auto, missing.clone());
        assert!(
            matches!(error, Err(CliError::Usage(ref message)) if message.contains("nothing exists")),
            "unexpected result: {error:?}"
        );
        let admin = run(strings(&[
            "admin",
            "status",
            "--db",
            &path_arg(&missing),
            "--profile",
            "auto",
        ]));
        assert!(
            matches!(admin, Err(CliError::Usage(_))),
            "unexpected result: {admin:?}"
        );
        assert!(!missing.exists());

        // `serve` resolves `auto` before it creates the parent directories of the path.
        let nested = temp.path().join("volume");
        let nested_db = nested.join("data").join("missing.redb");
        let env = env_map(&[
            ("MULTIDB_RUNTIME_MODE", "local-dev"),
            ("MULTIDB_PG_TLS_MODE", "disabled"),
            ("MULTIDB_PG_PASSWORD", "pg-secret"),
            ("MULTIDB_PROFILE", "auto"),
            ("MULTIDB_DB_PATH", &path_arg(&nested_db)),
        ]);
        let serve = serve_config_from_sources(|name| env.get(name).cloned())?;
        let database = serve_database(&serve);
        assert!(
            matches!(database, Err(CliError::Usage(ref message)) if message.contains("nothing exists")),
            "unexpected result: {:?}",
            database.err()
        );
        assert!(!nested.exists());
        Ok(())
    }

    /// The CLI and `serve` open single-node CP files only. `auto` reads the storage profile but
    /// never adopts AP replication: an AP file fails exactly as it does with a named profile.
    #[test]
    fn profile_auto_refuses_ap_databases_like_a_named_profile()
    -> Result<(), Box<dyn std::error::Error>> {
        fn is_ap_mismatch<T>(result: &Result<T, CliError>) -> bool {
            matches!(
                result,
                Err(CliError::Config(ConfigError::ReplicationMismatch {
                    expected: ReplicationKind::Cp,
                    found: ReplicationKind::Ap,
                }))
            )
        }

        let temp = tempdir()?;
        let db_path = temp.path().join("ap.redb");
        let ap_config =
            DbConfig::on_disk(Profile::Document, &db_path).with_replication(ReplicationKind::Ap);
        drop(create_database(ap_config.clone())?);
        let before = std::fs::read(&db_path)?;

        let config = cli_db_config(ProfileSelection::Auto, db_path.clone());
        assert!(is_ap_mismatch(&config), "unexpected result: {config:?}");
        let csv = temp.path().join("rows.csv");
        std::fs::write(&csv, "id,name\n1,a\n")?;
        let csv_arg = path_arg(&csv);
        let db_arg = path_arg(&db_path);
        // `auto` fails before anything opens the file; a named profile fails in the opener.
        for profile in ["auto", "document"] {
            for mut args in [
                vec!["admin", "status"],
                vec!["advice", "list"],
                vec!["migrate", "value-codec"],
                vec!["import", "csv", "--file", &csv_arg, "--table", "t"],
            ] {
                args.extend(["--db", &db_arg, "--profile", profile]);
                let outcome = run(strings(&args));
                assert!(
                    is_ap_mismatch(&outcome),
                    "{args:?}: unexpected result: {outcome:?}"
                );
            }

            let env = env_map(&[
                ("MULTIDB_RUNTIME_MODE", "local-dev"),
                ("MULTIDB_PG_TLS_MODE", "disabled"),
                ("MULTIDB_PG_PASSWORD", "pg-secret"),
                ("MULTIDB_PROFILE", profile),
                ("MULTIDB_DB_PATH", &db_arg),
            ]);
            let serve = serve_config_from_sources(|name| env.get(name).cloned())?;
            let database = serve_database(&serve);
            assert!(
                is_ap_mismatch(&database),
                "serve {profile}: unexpected result: {:?}",
                database.err()
            );
            if profile == "auto" {
                assert_eq!(
                    std::fs::read(&db_path)?,
                    before,
                    "auto wrote to the AP file"
                );
            }
        }

        let reopened = open_database(ap_config)?;
        assert_eq!(reopened.replication_kind(), ReplicationKind::Ap);
        Ok(())
    }

    /// `serve` stops only when it is killed, so the file it leaves behind needs crash recovery.
    /// `profile show --db` still never writes it, while `--profile auto` and
    /// `MULTIDB_PROFILE=auto` recover it, as every opener would, before reading its profile.
    /// Unix only: the crash is simulated by copying the file while the database holds it open,
    /// and Windows refuses to read a file that redb has locked.
    #[cfg(unix)]
    #[test]
    fn profile_auto_recovers_a_database_that_was_not_closed_cleanly()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let live = temp.path().join("live.redb");
        let overrides = serde_json::from_value(json!({ "cache": { "max_entries": 8192 } }))?;
        let ProfileResolution::Resolved { profile, .. } =
            resolve_named_profile("high_durability", &overrides)
        else {
            return Err("high_durability with a cache override must resolve".into());
        };
        let binding = profile.bind(ReplicationKind::Cp, Some(live.clone()))?;
        let database = create_database_with_profile(&binding, OperationalConfig::default())?;
        // A copy taken while the database is open is what a killed process leaves behind.
        let shown_path = temp.path().join("shown.redb");
        let cli_path = temp.path().join("cli.redb");
        let serve_path = temp.path().join("serve.redb");
        for path in [&shown_path, &cli_path, &serve_path] {
            std::fs::copy(&live, path)?;
        }
        drop(database);

        let before = std::fs::read(&shown_path)?;
        let shown = run(strings(&[
            "profile",
            "show",
            "--db",
            &path_arg(&shown_path),
        ]))?;
        assert_eq!(shown.exit_code, 1);
        assert!(
            shown.message.contains("needs crash recovery"),
            "{}",
            shown.message
        );
        let shown = run(strings(&[
            "profile",
            "show",
            "--db",
            &path_arg(&shown_path),
            "--json",
        ]))?;
        assert_eq!(shown.exit_code, 1);
        assert_eq!(json_message(&shown)?["state"], "needs_recovery");
        assert_eq!(
            std::fs::read(&shown_path)?,
            before,
            "profile show wrote the file"
        );

        assert_eq!(
            cli_db_config(ProfileSelection::Auto, shown_path.clone())?,
            DbConfig::on_disk(Profile::HighDurability, &shown_path)
        );
        let shown = run(strings(&[
            "profile",
            "show",
            "--db",
            &path_arg(&shown_path),
        ]))?;
        assert_eq!(shown.exit_code, 0, "{}", shown.message);

        let status = run(strings(&[
            "admin",
            "status",
            "--db",
            &path_arg(&cli_path),
            "--profile",
            "auto",
        ]))?;
        assert_eq!(status.exit_code, 0, "{}", status.message);

        let serve_arg = path_arg(&serve_path);
        let env = env_map(&[
            ("MULTIDB_RUNTIME_MODE", "local-dev"),
            ("MULTIDB_PG_TLS_MODE", "disabled"),
            ("MULTIDB_PG_PASSWORD", "pg-secret"),
            ("MULTIDB_PROFILE", "auto"),
            ("MULTIDB_DB_PATH", &serve_arg),
        ]);
        let serve = serve_config_from_sources(|name| env.get(name).cloned())?;
        let database = serve_database(&serve)?;
        assert_eq!(database.profile(), Profile::HighDurability);
        drop(database);

        // Recovery runs before the replication check, so a crashed AP file is recovered and
        // then refused; it keeps its replication.
        let ap_live = temp.path().join("ap-live.redb");
        let ap_crashed = temp.path().join("ap-crashed.redb");
        let database = create_database(
            DbConfig::on_disk(Profile::Document, &ap_live).with_replication(ReplicationKind::Ap),
        )?;
        std::fs::copy(&ap_live, &ap_crashed)?;
        drop(database);
        let config = cli_db_config(ProfileSelection::Auto, ap_crashed.clone());
        assert!(
            matches!(
                config,
                Err(CliError::Config(ConfigError::ReplicationMismatch {
                    expected: ReplicationKind::Cp,
                    found: ReplicationKind::Ap,
                }))
            ),
            "unexpected result: {config:?}"
        );
        let reopened = open_database(
            DbConfig::on_disk(Profile::Document, &ap_crashed).with_replication(ReplicationKind::Ap),
        )?;
        assert_eq!(reopened.replication_kind(), ReplicationKind::Ap);
        Ok(())
    }

    #[test]
    fn backup_accepts_profile_auto_and_restore_refuses_it() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp = tempdir()?;
        let db_path = temp.path().join("source.redb");
        drop(create_database(DbConfig::on_disk(
            Profile::Document,
            &db_path,
        ))?);

        let backup = run(strings(&[
            "backup",
            "full",
            "--db",
            &path_arg(&db_path),
            "--profile",
            "auto",
            "--dest",
            &path_arg(&temp.path().join("backups")),
        ]))?;
        assert_eq!(backup.exit_code, 0);
        let backup_dir = backup
            .message
            .split("path=")
            .nth(1)
            .ok_or("missing backup path")?
            .trim()
            .to_owned();

        let restored = temp.path().join("restored.redb");
        let refused = run(strings(&[
            "restore",
            "--backup",
            &backup_dir,
            "--dest-db",
            &path_arg(&restored),
            "--profile",
            "auto",
        ]));
        assert!(
            matches!(refused, Err(CliError::Usage(ref message))
                if message.contains("restore does not accept --profile auto")),
            "unexpected result: {refused:?}"
        );
        assert!(!restored.exists());

        let restore = run(strings(&[
            "restore",
            "--backup",
            &backup_dir,
            "--dest-db",
            &path_arg(&restored),
            "--profile",
            "desktop-app-embedded",
        ]))?;
        assert_eq!(restore.exit_code, 0, "{}", restore.message);
        assert_eq!(
            cli_db_config(ProfileSelection::Auto, restored.clone())?,
            DbConfig::on_disk(Profile::Document, &restored)
        );
        Ok(())
    }

    /// A restore target is never encrypted, so `restore` refuses a backup whose manifest sets
    /// `floors.encryption_at_rest` unless `--allow-unmet-floors` is given, and then warns.
    #[test]
    fn restore_refuses_unmet_profile_floors_without_allow_unmet_floors()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let mut spec = decode_spec_bytes(
            include_str!("../../examples/custom-profile/multidb.yaml").as_bytes(),
            SpecFormat::Yaml,
        )?;
        spec.custom_profile
            .as_mut()
            .ok_or("the example sets custom_profile")?
            .floors
            .encryption_at_rest = Some(true);
        spec.guarantees.encryption.at_rest = true;
        let compiled = PolicyCompiler::compile(&spec).map_err(|report| format!("{report:?}"))?;
        let source = temp.path().join("source.redb");
        let binding = compiled.binding(&spec.deployment, Some(source.clone()))?;
        let key_path = temp.path().join("key.bin");
        std::fs::write(&key_path, [3_u8; 32])?;
        let encryption = EncryptionConfig::file_key(&key_path);
        let database = create_database_with_profile(
            &binding,
            OperationalConfig::new().with_encryption(encryption.clone()),
        )?;
        let encrypted_backup = full_backup(
            &database,
            temp.path().join("encrypted-backups"),
            &BackupConfig::default(),
        )?;
        drop(database);
        // The library restore with the opt-in gives an unencrypted database whose manifest
        // still sets the floor; the CLI backs it up without encryption.
        let restored = temp.path().join("restored.redb");
        restore_backup_with_config(
            &encrypted_backup.path,
            DbConfig::on_disk(Profile::HighDurability, &restored),
            RestoreTarget::Latest,
            &RestoreConfig::default()
                .with_encryption(encryption)
                .with_unmet_profile_floors_allowed(true),
        )?;
        let backup = run(strings(&[
            "backup",
            "full",
            "--db",
            &path_arg(&restored),
            "--profile",
            "auto",
            "--dest",
            &path_arg(&temp.path().join("backups")),
        ]))?;
        assert_eq!(backup.exit_code, 0, "{}", backup.message);
        let backup_dir = backup
            .message
            .split("path=")
            .nth(1)
            .ok_or("missing backup path")?
            .trim()
            .to_owned();

        let target = temp.path().join("target.redb");
        let restore_args = [
            "restore",
            "--backup",
            &backup_dir,
            "--dest-db",
            &path_arg(&target),
            "--profile",
            "high_durability",
        ];
        let refused = run(strings(&restore_args));
        assert!(
            matches!(
                &refused,
                Err(CliError::Backup(BackupError::ProfileFloorsNotMet { floors }))
                    if floors == &["floors.encryption_at_rest"]
            ),
            "unexpected result: {refused:?}"
        );
        assert!(!target.exists(), "a refused restore creates nothing");

        let mut allowed_args = restore_args.to_vec();
        allowed_args.push("--allow-unmet-floors");
        let allowed = run(strings(&allowed_args))?;
        assert_eq!(allowed.exit_code, 0, "{}", allowed.message);
        assert!(
            allowed.message.starts_with("restored timeline=")
                && allowed
                    .message
                    .contains("\nwarning: floors.encryption_at_rest is not met"),
            "{}",
            allowed.message
        );
        let shown = run(strings(&["profile", "show", "--db", &path_arg(&target)]))?;
        assert!(
            shown
                .message
                .contains("\nwarning: floors.encryption_at_rest is not met"),
            "{}",
            shown.message
        );
        Ok(())
    }

    #[test]
    fn serve_config_accepts_product_slugs_aliases_and_auto()
    -> Result<(), Box<dyn std::error::Error>> {
        for (value, expected) in [
            (
                "secure_app",
                ProfileSelection::Fixed(Profile::Transactional),
            ),
            (
                "high_durability",
                ProfileSelection::Fixed(Profile::HighDurability),
            ),
            ("auto", ProfileSelection::Auto),
        ] {
            let env = env_map(&[
                ("MULTIDB_RUNTIME_MODE", "local-dev"),
                ("MULTIDB_PG_TLS_MODE", "disabled"),
                ("MULTIDB_PG_PASSWORD", "pg-secret"),
                ("MULTIDB_PROFILE", value),
            ]);

            let config = serve_config_from_sources(|name| env.get(name).cloned())?;

            assert_eq!(config.profile, expected, "{value}");
        }
        Ok(())
    }

    #[test]
    fn profile_list_json_adds_variants_and_knobs() -> Result<(), Box<dyn std::error::Error>> {
        let outcome = run(strings(&["profile", "list", "--json"]))?;
        let report = json_message(&outcome)?;
        let profiles = report.as_array().ok_or("profile list is not an array")?;

        assert_eq!(profiles.len(), 6);
        for profile in profiles {
            let variants = profile["variants"].as_array().ok_or("missing variants")?;
            assert_eq!(
                variants
                    .iter()
                    .filter(|variant| variant["canonical"] == true)
                    .count(),
                1,
                "{profile}"
            );
            assert!(
                profile["knobs"]["durability"]["commit"].is_string(),
                "{profile}"
            );
        }
        let game = profiles
            .iter()
            .find(|profile| profile["slug"] == "game_local_balanced")
            .ok_or("missing game_local_balanced")?;
        assert_eq!(
            game["variants"],
            json!([
                { "name": "balanced", "storage_profile": "Balanced", "canonical": true },
                { "name": "in_memory", "storage_profile": "InMemory", "canonical": false },
            ])
        );
        assert_eq!(game["knobs"]["storage"]["medium"], "durable");

        let text = run(strings(&["profile", "list"]))?;
        assert!(
            text.message
                .contains("variants=balanced(Balanced,canonical),in_memory(InMemory)"),
            "{}",
            text.message
        );
        Ok(())
    }

    #[test]
    fn profile_show_reports_resolved_knobs_with_provenance()
    -> Result<(), Box<dyn std::error::Error>> {
        let json = run(strings(&["profile", "show", "high_durability", "--json"]))?;
        assert_eq!(json.exit_code, 0);
        let report = json_message(&json)?;
        assert_eq!(report["resolution"], "resolved");
        assert_eq!(report["profile"]["storage_profile"], "HighDurability");
        assert_eq!(report["profile"]["product_profile"], "secure_app");
        assert_eq!(report["profile"]["kind"], "variant");
        assert_eq!(
            report["rows"].as_array().map(Vec::len),
            Some(KNOB_REGISTRY.len())
        );
        let commit = knob_row(&report, "durability.commit")?;
        assert_eq!(commit["value"], "two_phase");
        assert_eq!(commit["source"], "variant");
        assert_eq!(commit["timing"], "creation");
        assert_eq!(
            knob_row(&report, "limits.max_value_bytes")?["enforcement"]["planned"]["tracking"],
            "single write pipeline"
        );

        let text = run(strings(&["profile", "show", "high_durability"]))?;
        assert_eq!(text.exit_code, 0);
        assert!(
            text.message.starts_with(
                "high_durability -> secure_app variant high_durability (Stable), storage profile \
                 HighDurability (redb, row, two-phase commit)"
            ),
            "{}",
            text.message
        );
        for needle in ["HighDurability", "two_phase", "planned"] {
            assert!(text.message.contains(needle), "{needle}: {}", text.message);
        }
        Ok(())
    }

    #[test]
    fn profile_show_overrides_are_a_what_if() -> Result<(), Box<dyn std::error::Error>> {
        let custom = run(strings(&[
            "profile",
            "show",
            "high_durability",
            "--override",
            "cache.max_entries=8192",
            "--json",
        ]))?;
        assert_eq!(custom.exit_code, 0);
        let report = json_message(&custom)?;
        assert_eq!(report["profile"]["kind"], "custom");
        assert_eq!(report["profile"]["slug"], "high_durability_custom");
        let cache = knob_row(&report, "cache.max_entries")?;
        assert_eq!(cache["value"], 8192);
        assert_eq!(cache["source"], "override");

        let typo = run(strings(&[
            "profile",
            "show",
            "high_durability",
            "--override",
            "cache.max_entires=1",
        ]));
        assert!(
            matches!(typo, Err(CliError::Usage(ref message)) if message.contains("cache.max_entries")),
            "unexpected result: {typo:?}"
        );

        let volatile = run(strings(&[
            "profile",
            "show",
            "high_durability",
            "--override",
            "storage.medium=volatile",
            "--json",
        ]))?;
        assert_eq!(volatile.exit_code, 2);
        let report = json_message(&volatile)?;
        assert_eq!(report["resolution"], "invalid");
        assert!(
            has_issue(&report, "PROFILE_KNOB_COMBINATION_UNSUPPORTED"),
            "{report}"
        );

        let floors = run(strings(&[
            "profile",
            "show",
            "secure-app",
            "--override",
            "floors.audit=true,cache.max_entries=4096",
            "--json",
        ]))?;
        assert_eq!(floors.exit_code, 0, "{}", floors.message);
        let report = json_message(&floors)?;
        assert_eq!(report["profile"]["slug"], "secure_app_custom");
        assert_eq!(report["profile"]["knobs"]["floors"]["audit"], true);
        assert_eq!(knob_row(&report, "floors.audit")?["source"], "override");
        assert_eq!(knob_row(&report, "cache.max_entries")?["value"], 4096);
        Ok(())
    }

    #[test]
    fn profile_show_finds_the_name_anywhere_among_the_flags()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = json_message(&run(strings(&[
            "profile",
            "show",
            "high_durability",
            "--override",
            "cache.max_entries=8192",
            "--json",
        ]))?)?;
        for args in [
            vec![
                "--json",
                "--override",
                "cache.max_entries=8192",
                "high_durability",
            ],
            vec![
                "--override",
                "cache.max_entries=8192",
                "high_durability",
                "--output",
                "json",
            ],
        ] {
            let mut full = vec!["profile", "show"];
            full.extend(args);
            let outcome = run(strings(&full))?;
            assert_eq!(outcome.exit_code, 0, "{full:?}: {}", outcome.message);
            assert_eq!(json_message(&outcome)?, first, "{full:?}");
        }

        let two = run(strings(&[
            "profile",
            "show",
            "secure_app",
            "--json",
            "balanced",
        ]));
        assert!(
            matches!(two, Err(CliError::Usage(ref message))
                if message.contains("one profile name") && message.contains("balanced")),
            "unexpected result: {two:?}"
        );
        let both = run(strings(&[
            "profile",
            "show",
            "--db",
            "x.redb",
            "secure_app",
        ]));
        assert!(
            matches!(both, Err(CliError::Usage(ref message)) if message.contains("not both")),
            "unexpected result: {both:?}"
        );
        Ok(())
    }

    #[test]
    fn profile_show_db_prints_the_manifest_or_legacy() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let manifest_path = temp.path().join("orders.redb");
        let overrides = serde_json::from_value(json!({ "cache": { "max_entries": 8192 } }))?;
        let ProfileResolution::Resolved { profile, .. } =
            resolve_named_profile("high_durability", &overrides)
        else {
            return Err("high_durability with a cache override must resolve".into());
        };
        let binding = profile.bind(ReplicationKind::Cp, Some(manifest_path.clone()))?;
        drop(create_database_with_profile(
            &binding,
            OperationalConfig::default(),
        )?);

        let json = run(strings(&[
            "profile",
            "show",
            "--db",
            &path_arg(&manifest_path),
            "--json",
        ]))?;
        assert_eq!(json.exit_code, 0);
        let report = json_message(&json)?;
        assert_eq!(report["state"], "initialized");
        assert_eq!(report["manifest"]["profile"], "high_durability_custom");
        assert_eq!(report["manifest"]["knobs"]["cache"]["max_entries"], 8192);
        let text = run(strings(&[
            "profile",
            "show",
            "--db",
            &path_arg(&manifest_path),
        ]))?;
        assert!(
            text.message.starts_with(
                "profile manifest: high_durability_custom extends high_durability -> secure_app"
            ),
            "{}",
            text.message
        );
        assert!(text.message.contains("HighDurability"), "{}", text.message);
        assert_eq!(report["unmet_floors"], json!([]));
        assert!(!text.message.contains("warning:"), "{}", text.message);
        // Every floor the probe sees unmet (a restore that was allowed to miss a floor, covered
        // in the backup tests) is printed as a warning.
        let MetadataProbe::Initialized(mut probed) = inspect_database_metadata(&manifest_path)
        else {
            return Err("the manifest database is not initialized".into());
        };
        probed.unmet_floors = vec!["floors.audit", "floors.encryption_at_rest"];
        let warned = format_metadata_probe(&manifest_path, &MetadataProbe::Initialized(probed))?;
        assert!(
            warned.contains("\nwarning: floors.audit is not met: audit is switched off")
                && warned.contains("\nwarning: floors.encryption_at_rest is not met"),
            "{warned}"
        );
        assert_eq!(
            cli_db_config(ProfileSelection::Auto, manifest_path.clone())?,
            DbConfig::on_disk(Profile::HighDurability, &manifest_path)
        );

        let legacy_path = temp.path().join("legacy.redb");
        drop(create_database(DbConfig::on_disk(
            Profile::Transactional,
            &legacy_path,
        ))?);
        let legacy = run(strings(&[
            "profile",
            "show",
            "--db",
            &path_arg(&legacy_path),
        ]))?;
        assert_eq!(legacy.exit_code, 0);
        assert!(
            legacy
                .message
                .starts_with("legacy database: storage profile Transactional, no manifest"),
            "{}",
            legacy.message
        );

        let missing = temp.path().join("missing.redb");
        let not_found = run(strings(&["profile", "show", "--db", &path_arg(&missing)]))?;
        assert_eq!(not_found.exit_code, 1);
        assert!(not_found.message.starts_with("no database at"));
        assert!(!missing.exists());
        Ok(())
    }

    #[test]
    fn profile_explain_reports_the_resolver_result_of_a_spec()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_path = temp.path().join("custom.yaml");
        let mut spec = DatabaseSpec::from_db_config(
            "orders",
            &DbConfig::on_disk(Profile::Transactional, "orders.redb"),
        );
        spec.version = 2;
        spec.profile = "orders_durable".to_owned();
        spec.custom_profile = Some(serde_json::from_value(json!({
            "extends": "high_durability",
            "knobs": { "cache": { "max_entries": 8192 } }
        }))?);
        write_yaml_spec(&spec_path, &spec)?;
        let explain = |json: bool| {
            let mut args = strings(&["profile", "explain", "--spec", &path_arg(&spec_path)]);
            if json {
                args.push("--json".to_owned());
            }
            run(args)
        };

        let resolved = explain(true)?;
        assert_eq!(resolved.exit_code, 0);
        let report = json_message(&resolved)?;
        assert_eq!(report["resolution"], "resolved");
        assert_eq!(report["profile"]["kind"], "custom");
        assert_eq!(report["profile"]["storage_profile"], "HighDurability");

        spec.custom_profile = Some(serde_json::from_value(
            json!({ "extends": "high_durabilty" }),
        )?);
        write_yaml_spec(&spec_path, &spec)?;
        let invalid = explain(true)?;
        assert_eq!(invalid.exit_code, 2);
        let report = json_message(&invalid)?;
        assert_eq!(report["resolution"], "invalid");
        assert!(
            has_issue(&report, "CUSTOM_PROFILE_EXTENDS_UNKNOWN"),
            "{report}"
        );
        let text = explain(false)?;
        assert_eq!(text.exit_code, 2);
        assert!(
            text.message.starts_with("invalid profile"),
            "{}",
            text.message
        );

        spec.version = 1;
        write_yaml_spec(&spec_path, &spec)?;
        let v1_custom = explain(true)?;
        assert_eq!(v1_custom.exit_code, 2);
        let report = json_message(&v1_custom)?;
        assert_eq!(report["resolution"], "invalid");
        assert!(has_issue(&report, "FIELD_REQUIRES_SPEC_V2"), "{report}");
        let v1_custom_text = explain(false)?;
        assert_eq!(v1_custom_text.exit_code, 2);
        assert!(
            v1_custom_text.message.contains("FIELD_REQUIRES_SPEC_V2"),
            "{}",
            v1_custom_text.message
        );

        let mut legacy = DatabaseSpec::from_db_config(
            "legacy",
            &DbConfig::on_disk(Profile::Transactional, "legacy.redb"),
        );
        legacy.profile = "ops_custom_profile".to_owned();
        write_yaml_spec(&spec_path, &legacy)?;
        let unresolved = explain(true)?;
        assert_eq!(unresolved.exit_code, 0);
        let report = json_message(&unresolved)?;
        assert_eq!(report["resolution"], "unresolved");
        assert!(has_issue(&report, "PROFILE_NOT_PROVISIONABLE"), "{report}");

        let mut future = serde_json::to_value(&legacy)?;
        future["version"] = json!(3);
        std::fs::write(&spec_path, serde_norway::to_string(&future)?)?;
        let unsupported = explain(false)?;
        assert_eq!(unsupported.exit_code, 2);
        assert!(unsupported.message.contains("UNSUPPORTED_VERSION"));
        Ok(())
    }

    #[test]
    fn profile_knobs_lists_the_registry_and_reserved_knobs()
    -> Result<(), Box<dyn std::error::Error>> {
        let json = run(strings(&["profile", "knobs", "--json"]))?;
        let report = json_message(&json)?;
        let knobs = report.as_array().ok_or("profile knobs is not an array")?;

        assert_eq!(knobs.len(), KNOB_REGISTRY.len() + RESERVED_KNOBS.len());
        assert_eq!(
            knobs.iter().filter(|knob| knob["reserved"] == true).count(),
            RESERVED_KNOBS.len()
        );
        assert!(knobs.iter().any(|knob| knob["path"] == "cache.max_entries"));
        let medium = knobs
            .iter()
            .find(|knob| knob["path"] == "storage.medium")
            .ok_or("missing storage.medium")?;
        assert_eq!(medium["after_creation"], "unsupported");

        let text = run(strings(&["profile", "knobs"]))?;
        assert!(text.message.contains("history.mvcc_retention_millis"));
        assert!(text.message.contains("reserved"));
        // The text output names ChangePolicy::Unsupported as the docs and --json do, in the
        // after-creation column (aligned to its header; cells are padded by characters).
        let header = text.message.lines().next().ok_or("missing header row")?;
        let column = header
            .find("after creation")
            .ok_or("missing after creation column")?;
        let after_creation =
            |line: &str| -> String { line.chars().skip(column).collect::<String>() };
        let medium_row = text
            .message
            .lines()
            .find(|line| line.starts_with("storage.medium "))
            .ok_or("missing storage.medium row")?;
        assert!(
            after_creation(medium_row).starts_with("unsupported "),
            "{medium_row}"
        );
        assert!(
            text.message
                .lines()
                .all(|line| !after_creation(line).starts_with("fixed ")),
            "{}",
            text.message
        );
        Ok(())
    }

    fn studio_style_spec() -> DatabaseSpec {
        let mut spec = DatabaseSpec::from_db_config(
            "ops_custom_profile_example",
            &DbConfig::on_disk(Profile::Transactional, "ops.redb"),
        );
        spec.profile = "ops_custom_profile".to_owned();
        spec.overrides
            .insert("profile.custom.base".to_owned(), "secure_app".to_owned());
        spec.overrides.insert(
            "profile.custom.description".to_owned(),
            "Ops profile".to_owned(),
        );
        spec.operation_hints
            .insert("studio.builder".to_owned(), "custom_profile".to_owned());
        spec.operation_hints.insert(
            "profile.custom.slug".to_owned(),
            "ops_custom_profile".to_owned(),
        );
        spec
    }

    #[test]
    fn config_upgrade_converts_studio_custom_profiles_idempotently()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let input = temp.path().join("studio.yaml");
        write_yaml_spec(&input, &studio_style_spec())?;
        let first_out = temp.path().join("v2.yaml");

        let first = run(strings(&[
            "config",
            "upgrade",
            "--spec",
            &path_arg(&input),
            "--out",
            &path_arg(&first_out),
        ]))?;
        assert_eq!(first.exit_code, 0, "{}", first.message);
        assert!(first.message.contains("valid=true"), "{}", first.message);
        let written = std::fs::read_to_string(&first_out)?;
        let upgraded = serde_norway::from_str::<DatabaseSpec>(&written)?;
        assert_eq!(upgraded.version, 2);
        assert_eq!(upgraded.profile, "ops_custom_profile");
        let custom = upgraded
            .custom_profile
            .as_ref()
            .ok_or("missing custom_profile")?;
        assert_eq!(custom.extends, "secure_app");
        assert_eq!(custom.description.as_deref(), Some("Ops profile"));
        assert!(upgraded.overrides.is_empty(), "{:?}", upgraded.overrides);
        assert_eq!(
            upgraded.operation_hints,
            BTreeMap::from([("studio.builder".to_owned(), "custom_profile".to_owned())])
        );
        assert!(GuaranteeValidator::validate(&upgraded).valid);

        let second_out = temp.path().join("v2-again.yaml");
        let second = run(strings(&[
            "config",
            "upgrade",
            "--spec",
            &path_arg(&first_out),
            "--out",
            &path_arg(&second_out),
        ]))?;
        assert_eq!(second.exit_code, 0);
        assert_eq!(std::fs::read_to_string(&second_out)?, written);

        let stdout = run(strings(&["config", "upgrade", "--spec", &path_arg(&input)]))?;
        assert_eq!(stdout.exit_code, 0);
        assert_eq!(stdout.message, written.trim_end());

        let json = run(strings(&[
            "config",
            "upgrade",
            "--spec",
            &path_arg(&input),
            "--json",
        ]))?;
        let report = json_message(&json)?;
        assert_eq!(report["validation"]["valid"], true);
        assert_eq!(report["spec"]["custom_profile"]["extends"], "secure_app");
        assert!(
            report["notes"]
                .as_array()
                .is_some_and(|notes| !notes.is_empty())
        );
        Ok(())
    }

    #[test]
    fn config_upgrade_refuses_to_overwrite_without_force() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp = tempdir()?;
        let input = temp.path().join("spec.json");
        write_spec(&input, &studio_style_spec())?;
        let original = std::fs::read_to_string(&input)?;

        let same = run(strings(&[
            "config",
            "upgrade",
            "--spec",
            &path_arg(&input),
            "--out",
            &path_arg(&input),
        ]));
        assert!(
            matches!(same, Err(CliError::Usage(ref message))
                if message.contains("input spec") && message.contains("--force")),
            "unexpected result: {same:?}"
        );
        let existing = temp.path().join("existing.json");
        std::fs::write(&existing, "{}")?;
        let other = run(strings(&[
            "config",
            "upgrade",
            "--spec",
            &path_arg(&input),
            "--out",
            &path_arg(&existing),
        ]));
        assert!(
            matches!(other, Err(CliError::Usage(ref message)) if message.contains("--force")),
            "unexpected result: {other:?}"
        );
        assert_eq!(std::fs::read_to_string(&input)?, original);
        assert_eq!(std::fs::read_to_string(&existing)?, "{}");

        let forced = run(strings(&[
            "config",
            "upgrade",
            "--spec",
            &path_arg(&input),
            "--out",
            &path_arg(&input),
            "--force",
        ]))?;
        assert_eq!(forced.exit_code, 0, "{}", forced.message);
        let upgraded = serde_json::from_str::<DatabaseSpec>(&std::fs::read_to_string(&input)?)?;
        assert_eq!(upgraded.version, 2);
        assert!(upgraded.custom_profile.is_some());
        assert_eq!(upgrade_temp_files(temp.path())?, Vec::<String>::new());
        Ok(())
    }

    /// Pretty-printing can grow a compact spec past `MAX_SPEC_BYTES`. Such a result is written
    /// nowhere, so `--force` never replaces an input multidb reads with one it refuses.
    #[test]
    fn config_upgrade_refuses_results_over_the_spec_size_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let input = temp.path().join("compact.json");
        let mut spec = studio_style_spec();
        spec.operation_hints
            .extend((0..70_000).map(|index| (format!("h{index}"), String::new())));
        let compact = serde_json::to_string(&spec)?;
        assert!(compact.len() < MAX_SPEC_BYTES, "{}", compact.len());
        assert!(serde_json::to_string_pretty(&spec)?.len() > MAX_SPEC_BYTES);
        std::fs::write(&input, &compact)?;
        let out = temp.path().join("upgraded.json");
        let (input_arg, out_arg) = (path_arg(&input), path_arg(&out));

        for extra in [
            vec![],
            vec!["--json"],
            vec!["--out", out_arg.as_str()],
            vec!["--out", out_arg.as_str(), "--json"],
            vec!["--out", input_arg.as_str(), "--force"],
        ] {
            let mut args = vec!["config", "upgrade", "--spec", input_arg.as_str()];
            args.extend(extra);
            let outcome = run(strings(&args))?;
            assert_eq!(outcome.exit_code, 2, "{args:?}: {}", outcome.message);
            assert!(
                outcome.message.contains("SPEC_TOO_LARGE")
                    && !outcome.message.contains("operation_hints"),
                "{args:?}: {}",
                outcome.message
            );
        }
        assert!(!out.exists());
        assert_eq!(std::fs::read_to_string(&input)?, compact);
        assert_eq!(upgrade_temp_files(temp.path())?, Vec::<String>::new());
        Ok(())
    }

    /// The names of `config upgrade` temporary files left in `dir`.
    fn upgrade_temp_files(dir: &std::path::Path) -> Result<Vec<String>, std::io::Error> {
        let mut names = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let name = entry?.file_name().to_string_lossy().into_owned();
            if name.ends_with(".upgrade-tmp") {
                names.push(name);
            }
        }
        Ok(names)
    }

    /// A symlink planted at the temporary name, or at `--out` itself, never redirects the
    /// write: the temporary file is created exclusively (a planted link fails it with
    /// `AlreadyExists`), and the rename replaces the link at `--out`, not its target.
    #[cfg(unix)]
    #[test]
    fn config_upgrade_out_never_writes_through_a_planted_symlink()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let input = temp.path().join("spec.json");
        write_spec(&input, &studio_style_spec())?;
        let victim = temp.path().join("victim.txt");
        std::fs::write(&victim, "keep")?;
        let out = temp.path().join("upgraded.json");
        // A symlink planted at the exact temporary name fails the write instead of redirecting
        // it, and is left in place.
        let planted = temp.path().join(".upgraded.json.planted.upgrade-tmp");
        std::os::unix::fs::symlink(&victim, &planted)?;
        let refused = write_through_temp_file(&out, &planted, "{}");
        assert!(
            matches!(&refused, Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists),
            "unexpected result: {refused:?}"
        );
        assert_eq!(std::fs::read_to_string(&victim)?, "keep");
        assert!(!out.exists());

        let outcome = run(strings(&[
            "config",
            "upgrade",
            "--spec",
            &path_arg(&input),
            "--out",
            &path_arg(&out),
        ]))?;
        assert_eq!(outcome.exit_code, 0, "{}", outcome.message);
        assert_eq!(std::fs::read_to_string(&victim)?, "keep");
        assert!(
            std::fs::symlink_metadata(&planted)?
                .file_type()
                .is_symlink()
        );
        let upgraded = serde_json::from_str::<DatabaseSpec>(&std::fs::read_to_string(&out)?)?;
        assert_eq!(upgraded.version, 2);

        let linked_out = temp.path().join("linked.json");
        std::os::unix::fs::symlink(&victim, &linked_out)?;
        let forced = run(strings(&[
            "config",
            "upgrade",
            "--spec",
            &path_arg(&input),
            "--out",
            &path_arg(&linked_out),
            "--force",
        ]))?;
        assert_eq!(forced.exit_code, 0, "{}", forced.message);
        assert_eq!(std::fs::read_to_string(&victim)?, "keep");
        assert!(
            !std::fs::symlink_metadata(&linked_out)?
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            upgrade_temp_files(temp.path())?,
            vec![".upgraded.json.planted.upgrade-tmp".to_owned()]
        );
        Ok(())
    }

    /// A spec that is not UTF-8 is an I/O error (exit 1) for every spec reader, as before
    /// `spec_io`, not a validation report (exit 2).
    #[test]
    fn spec_files_that_are_not_utf8_keep_exit_code_1() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec = temp.path().join("latin1.yaml");
        std::fs::write(&spec, b"version: 1\nname: caf\xe9\n")?;
        let spec_arg = path_arg(&spec);
        for args in [
            vec!["config", "validate", "--spec", &spec_arg],
            vec!["config", "explain", "--spec", &spec_arg, "--json"],
            vec!["config", "upgrade", "--spec", &spec_arg],
            vec!["profile", "explain", "--spec", &spec_arg],
            vec![
                "config",
                "plan",
                "--current",
                &spec_arg,
                "--desired",
                &spec_arg,
            ],
        ] {
            let outcome = run(strings(&args));
            assert!(
                matches!(
                    &outcome,
                    Err(error @ CliError::Io(io))
                        if io.kind() == std::io::ErrorKind::InvalidData
                            && error.to_string() == "io: stream did not contain valid UTF-8"
                ),
                "{args:?}: unexpected result: {outcome:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn config_upgrade_reports_unknown_profiles_without_a_base()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let input = temp.path().join("unknown.yaml");
        let mut spec = DatabaseSpec::from_db_config(
            "unknown",
            &DbConfig::on_disk(Profile::Balanced, "unknown.redb"),
        );
        spec.profile = "ops_custom_profile".to_owned();
        write_yaml_spec(&input, &spec)?;

        let outcome = run(strings(&[
            "config",
            "upgrade",
            "--spec",
            &path_arg(&input),
            "--json",
        ]))?;

        assert_eq!(outcome.exit_code, 2);
        let report = json_message(&outcome)?;
        assert_eq!(report["validation"]["valid"], false);
        assert!(
            has_issue(&report["validation"], "PROFILE_UNKNOWN"),
            "{report}"
        );

        // An invalid result never replaces a file, not even the input with --force.
        let original = std::fs::read_to_string(&input)?;
        let forced = run(strings(&[
            "config",
            "upgrade",
            "--spec",
            &path_arg(&input),
            "--out",
            &path_arg(&input),
            "--force",
        ]))?;
        assert_eq!(forced.exit_code, 2);
        assert!(
            forced.message.contains("not written") && forced.message.contains("PROFILE_UNKNOWN"),
            "{}",
            forced.message
        );
        assert_eq!(std::fs::read_to_string(&input)?, original);
        let out = temp.path().join("upgraded.yaml");
        let json = run(strings(&[
            "config",
            "upgrade",
            "--spec",
            &path_arg(&input),
            "--out",
            &path_arg(&out),
            "--json",
        ]))?;
        assert_eq!(json.exit_code, 2);
        assert_eq!(json_message(&json)?["written"], false);
        assert!(!out.exists());
        Ok(())
    }

    #[test]
    fn config_upgrade_writes_the_format_of_the_out_path() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp = tempdir()?;
        let input = temp.path().join("studio.yaml");
        write_yaml_spec(&input, &studio_style_spec())?;
        let json_out = temp.path().join("v2.json");

        let upgraded = run(strings(&[
            "config",
            "upgrade",
            "--spec",
            &path_arg(&input),
            "--out",
            &path_arg(&json_out),
        ]))?;
        assert_eq!(upgraded.exit_code, 0, "{}", upgraded.message);
        let written = serde_json::from_str::<DatabaseSpec>(&std::fs::read_to_string(&json_out)?)?;
        assert_eq!(written.version, 2);
        let validated = run(strings(&[
            "config",
            "validate",
            "--spec",
            &path_arg(&json_out),
        ]))?;
        assert_eq!(validated.exit_code, 0, "{}", validated.message);

        let yaml_out = temp.path().join("v2-again.yml");
        let again = run(strings(&[
            "config",
            "upgrade",
            "--spec",
            &path_arg(&json_out),
            "--out",
            &path_arg(&yaml_out),
        ]))?;
        assert_eq!(again.exit_code, 0, "{}", again.message);
        assert_eq!(
            serde_norway::from_str::<DatabaseSpec>(&std::fs::read_to_string(&yaml_out)?)?,
            written
        );
        Ok(())
    }

    #[test]
    fn init_guided_override_writes_a_v2_custom_profile() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_path = temp.path().join("multidb.yaml");

        let outcome = run(strings(&[
            "init",
            "--guided",
            "--profile",
            "secure_app",
            "--name",
            "Orders",
            "--override",
            "cache.max_entries=8192",
            "--out",
            &path_arg(&spec_path),
            "--json",
        ]))?;

        assert_eq!(outcome.exit_code, 0, "{}", outcome.message);
        let summary = json_message(&outcome)?;
        assert_eq!(summary["valid"], true);
        assert_eq!(summary["profile"], "secure_app_custom");
        assert_eq!(summary["extends"], "secure_app");
        assert_eq!(summary["version"], 2);
        let spec = serde_norway::from_str::<DatabaseSpec>(&std::fs::read_to_string(&spec_path)?)?;
        assert_eq!(spec.version, 2);
        assert_eq!(spec.profile, "secure_app_custom");
        let custom = spec
            .custom_profile
            .as_ref()
            .ok_or("missing custom_profile")?;
        assert_eq!(custom.extends, "secure_app");
        assert_eq!(
            serde_json::to_value(&custom.knobs)?,
            json!({ "cache": { "max_entries": 8192 } })
        );
        assert!(GuaranteeValidator::validate(&spec).valid);

        let game_path = temp.path().join("game.yaml");
        let game = run(strings(&[
            "init",
            "--guided",
            "--profile",
            "In-Memory",
            "--name",
            "Game",
            "--override",
            "floors.backup=true,query.timeout_ms=1000",
            "--profile-name",
            "game_fast",
            "--out",
            &path_arg(&game_path),
        ]))?;
        assert_eq!(game.exit_code, 0, "{}", game.message);
        let spec = serde_norway::from_str::<DatabaseSpec>(&std::fs::read_to_string(&game_path)?)?;
        assert_eq!(spec.profile, "game_fast");
        assert_eq!(
            spec.custom_profile
                .as_ref()
                .map(|custom| custom.extends.as_str()),
            Some("in_memory")
        );
        assert_eq!(spec.deployment.mode, DeploymentMode::Embedded);
        assert_eq!(spec.deployment.storage_path, None);
        assert!(spec.guarantees.backup.enabled);
        Ok(())
    }

    #[test]
    fn init_guided_profile_name_and_override_need_a_profile()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_path = temp.path().join("multidb.yaml");

        let named = run(strings(&[
            "init",
            "--guided",
            "--profile",
            "secure_app",
            "--name",
            "Orders",
            "--profile-name",
            "orders_custom",
            "--out",
            &path_arg(&spec_path),
        ]));
        assert!(
            matches!(named, Err(CliError::Usage(ref message)) if message.contains("--override")),
            "unexpected result: {named:?}"
        );
        assert!(!spec_path.exists());

        let template = run(strings(&[
            "init",
            "--guided",
            "--template",
            "game-save",
            "--name",
            "Game",
            "--override",
            "cache.max_entries=1",
        ]));
        assert!(
            matches!(template, Err(CliError::Usage(ref message)) if message.contains("not --template")),
            "unexpected result: {template:?}"
        );
        Ok(())
    }

    #[test]
    fn init_guided_warns_when_an_alias_names_a_non_canonical_variant()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_path = temp.path().join("multidb.yaml");
        let init = |profile: &str| {
            run(strings(&[
                "init",
                "--guided",
                "--profile",
                profile,
                "--name",
                "Orders",
                "--out",
                &path_arg(&spec_path),
                "--force",
            ]))
        };

        let alias = init("high_durability")?;
        assert_eq!(alias.exit_code, 0);
        let mut lines = alias.message.lines();
        assert!(
            lines
                .next()
                .is_some_and(|line| line.starts_with("created config"))
        );
        assert!(
            lines.next().is_some_and(|line| line
                .starts_with("warning: profile high_durability is the high_durability variant")),
            "{}",
            alias.message
        );
        let spec = serde_norway::from_str::<DatabaseSpec>(&std::fs::read_to_string(&spec_path)?)?;
        assert_eq!(spec.version, 1);
        assert_eq!(spec.profile, "secure_app");

        for canonical in ["secure_app", "transactional"] {
            let outcome = init(canonical)?;
            assert!(!outcome.message.contains("warning"), "{}", outcome.message);
        }
        Ok(())
    }

    /// A version 2 spec for the durable `balanced` profile whose `storage_path` is the relative
    /// `shop.redb`, so the CLI resolves it against the directory of the spec file.
    fn provision_spec(collections: &Value) -> Result<DatabaseSpec, Box<dyn std::error::Error>> {
        let mut spec = DatabaseSpec::from_db_config(
            "shop",
            &DbConfig::on_disk(Profile::Balanced, "shop.redb"),
        );
        spec.version = multidb::config_spec::DATABASE_SPEC_VERSION_V2;
        spec.profile = "balanced".to_owned();
        spec.collections = serde_json::from_value(collections.clone())?;
        Ok(spec)
    }

    fn document_collection(name: &str) -> Value {
        json!({ "name": name, "role": "document_entity", "domain": "primary", "indexes": ["primary"] })
    }

    /// A database created with the `balanced` profile manifest, outside provisioning.
    fn create_balanced_database(
        path: &std::path::Path,
    ) -> Result<multidb::db::Database, Box<dyn std::error::Error>> {
        let ProfileResolution::Resolved { profile, .. } = resolve_named_profile(
            "balanced",
            &multidb::config_spec::ProfileKnobOverrides::default(),
        ) else {
            return Err("balanced does not resolve".into());
        };
        let binding = profile.bind(ReplicationKind::Cp, Some(path.to_path_buf()))?;
        Ok(create_database_with_profile(
            &binding,
            OperationalConfig::new(),
        )?)
    }

    fn plan_id_of(outcome: &super::CliRun) -> Result<String, Box<dyn std::error::Error>> {
        Ok(json_message(outcome)?["plan_id"]
            .as_str()
            .ok_or("missing plan_id")?
            .to_owned())
    }

    #[test]
    fn config_plan_spec_is_a_dry_run_against_the_spec_storage_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_dir = temp.path().join("app");
        std::fs::create_dir(&spec_dir)?;
        let spec_path = spec_dir.join("multidb.yaml");
        write_yaml_spec(
            &spec_path,
            &provision_spec(&json!([document_collection("orders")]))?,
        )?;
        let spec_arg = path_arg(&spec_path);
        let db_path = spec_dir.join("shop.redb");

        let text = run(strings(&["config", "plan", "--spec", &spec_arg]))?;
        assert_eq!(text.exit_code, 0, "{}", text.message);
        for expected in [
            "outcome=ready auto_approvable=true surface=cli",
            "pstep-001 create_database",
            "create_collection collections.orders additive",
            "flags: none required",
            "guarantees.audit.enabled=true enforced_with_development_key",
            "apply with: multidb config apply --spec",
        ] {
            assert!(
                text.message.contains(expected),
                "{expected}: {}",
                text.message
            );
        }
        assert!(
            !db_path.exists(),
            "a dry run never creates the database file"
        );

        let json = run(strings(&["config", "plan", "--spec", &spec_arg, "--json"]))?;
        assert_eq!(json.exit_code, 0, "{}", json.message);
        let plan = json_message(&json)?;
        assert_eq!(plan["outcome"], "ready");
        assert_eq!(plan["surface"], "cli");
        assert_eq!(plan["target"]["kind"], "new_database");
        // The relative storage_path resolves against the directory of the spec file, not the
        // working directory.
        assert_eq!(
            plan["target"]["path"],
            json!(
                std::fs::canonicalize(&spec_dir)?
                    .join("shop.redb")
                    .display()
                    .to_string()
            )
        );
        let plan_id = plan_id_of(&json)?;
        assert!(plan_id.starts_with("prov_"), "{plan_id}");
        let explicit = run(strings(&[
            "config",
            "plan",
            "--spec",
            &spec_arg,
            "--db",
            &path_arg(&db_path),
            "--json",
        ]))?;
        assert_eq!(plan_id_of(&explicit)?, plan_id);

        let out = temp.path().join("plan.json");
        let written = run(strings(&[
            "config",
            "plan",
            "--spec",
            &spec_arg,
            "--out",
            &path_arg(&out),
            "--json",
        ]))?;
        assert_eq!(written.exit_code, 0, "{}", written.message);
        assert_eq!(plan_id_of(&written)?, plan_id);
        let decoded = serde_json::from_str::<multidb::provision::ProvisionPlan>(
            &std::fs::read_to_string(&out)?,
        )?;
        assert_eq!(decoded.plan_id, plan_id);
        assert!(
            !db_path.exists(),
            "a dry run never creates the database file"
        );

        // --out never replaces an existing file without --force, and never the spec or the
        // database, even with --force.
        let plan_out = |out: &std::path::Path, force: bool| {
            let mut args = strings(&[
                "config",
                "plan",
                "--spec",
                &spec_arg,
                "--db",
                &path_arg(&db_path),
                "--out",
                &path_arg(out),
            ]);
            if force {
                args.push("--force".to_owned());
            }
            run(args)
        };
        match plan_out(&out, false) {
            Err(CliError::Usage(message)) => {
                assert!(message.contains("pass --force"), "{message}");
            }
            other => return Err(format!("expected a usage error, got {other:?}").into()),
        }
        assert_eq!(plan_out(&out, true)?.exit_code, 0);
        let spec_before = std::fs::read(&spec_path)?;
        for (target, what) in [(&spec_path, "spec"), (&db_path, "database")] {
            match plan_out(target, true) {
                Err(CliError::Usage(message)) => {
                    assert!(message.contains(&format!("over the {what}")), "{message}");
                }
                other => return Err(format!("expected a usage error, got {other:?}").into()),
            }
        }
        assert_eq!(std::fs::read(&spec_path)?, spec_before);
        assert!(!db_path.exists());
        Ok(())
    }

    #[test]
    fn config_apply_spec_creates_converges_and_checks_confirmations()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_path = temp.path().join("multidb.yaml");
        write_yaml_spec(
            &spec_path,
            &provision_spec(&json!([document_collection("orders")]))?,
        )?;
        let spec_arg = path_arg(&spec_path);
        let db_path = temp.path().join("shop.redb");
        let apply = |extra: &[&str]| {
            let mut args = strings(&["config", "apply", "--spec", &spec_arg]);
            args.extend(strings(extra));
            run(args)
        };

        let unconfirmed = apply(&[])?;
        assert_eq!(unconfirmed.exit_code, 2, "{}", unconfirmed.message);
        assert!(
            unconfirmed.message.contains("nothing was applied"),
            "{}",
            unconfirmed.message
        );
        let legacy_id = apply(&["--confirm", "plan_0123456789abcdef"])?;
        assert_eq!(legacy_id.exit_code, 2, "{}", legacy_id.message);
        assert!(!db_path.exists());
        let both = apply(&["--confirm", "prov_0", "--auto-approve"]);
        assert!(
            matches!(both, Err(CliError::Usage(ref message)) if message.contains("not both")),
            "unexpected result: {both:?}"
        );

        let created = apply(&["--auto-approve", "--json"])?;
        assert_eq!(created.exit_code, 0, "{}", created.message);
        let report = json_message(&created)?;
        assert_eq!(report["status"], "applied");
        assert_eq!(report["check"]["data_mutated"], true);
        assert_eq!(report["record"]["applied_by"], "local:cli");
        assert!(db_path.exists());

        let again = apply(&["--auto-approve"])?;
        assert_eq!(again.exit_code, 0, "{}", again.message);
        assert!(
            again.message.starts_with("provisioning noop"),
            "{}",
            again.message
        );
        let nothing = apply(&[])?;
        assert_eq!(nothing.exit_code, 0, "{}", nothing.message);
        assert!(nothing.message.contains("nothing to apply"));
        let rejected = apply(&["--confirm", "plan_0123456789abcdef", "--json"])?;
        assert_eq!(rejected.exit_code, 2, "{}", rejected.message);
        assert_eq!(json_message(&rejected)?["status"], "rejected");

        // A plan reviewed before an out-of-band change no longer confirms.
        let grown_path = temp.path().join("grown.yaml");
        write_yaml_spec(
            &grown_path,
            &provision_spec(&json!([
                document_collection("orders"),
                document_collection("events")
            ]))?,
        )?;
        let grown_arg = path_arg(&grown_path);
        let reviewed = run(strings(&["config", "plan", "--spec", &grown_arg, "--json"]))?;
        assert_eq!(reviewed.exit_code, 0, "{}", reviewed.message);
        let reviewed_id = plan_id_of(&reviewed)?;
        let mut db = open_database(DbConfig::on_disk(Profile::Balanced, &db_path))?;
        db.create_graph_auto_id("social")?;
        drop(db);
        let stale = run(strings(&[
            "config",
            "apply",
            "--spec",
            &grown_arg,
            "--confirm",
            &reviewed_id,
        ]))?;
        assert_eq!(stale.exit_code, 4, "{}", stale.message);
        assert!(
            stale.message.contains("the plan changed"),
            "{}",
            stale.message
        );
        let current = run(strings(&["config", "plan", "--spec", &grown_arg, "--json"]))?;
        let current_id = plan_id_of(&current)?;
        assert_ne!(current_id, reviewed_id);
        let applied = run(strings(&[
            "config",
            "apply",
            "--spec",
            &grown_arg,
            "--confirm",
            &current_id,
        ]))?;
        assert_eq!(applied.exit_code, 0, "{}", applied.message);
        assert!(
            applied.message.starts_with("provisioning applied"),
            "{}",
            applied.message
        );
        Ok(())
    }

    #[test]
    fn config_drift_reports_sync_drift_and_invalid_specs() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp = tempdir()?;
        let spec_path = temp.path().join("multidb.yaml");
        write_yaml_spec(
            &spec_path,
            &provision_spec(&json!([document_collection("orders")]))?,
        )?;
        let spec_arg = path_arg(&spec_path);
        let db_path = temp.path().join("shop.redb");
        let drift = |spec: &str, extra: &[&str]| {
            let mut args = strings(&["config", "drift", "--spec", spec]);
            args.extend(strings(extra));
            run(args)
        };

        let missing = drift(&spec_arg, &[])?;
        assert_eq!(missing.exit_code, 3, "{}", missing.message);
        let applied = run(strings(&[
            "config",
            "apply",
            "--spec",
            &spec_arg,
            "--auto-approve",
        ]))?;
        assert_eq!(applied.exit_code, 0, "{}", applied.message);
        let in_sync = drift(&spec_arg, &[])?;
        assert_eq!(in_sync.exit_code, 0, "{}", in_sync.message);
        assert!(in_sync.message.starts_with("drift in_sync=true"));

        // An object created outside provisioning is information under the default `report`
        // policy, and drift under `--unmanaged fail`.
        let mut db = open_database(DbConfig::on_disk(Profile::Balanced, &db_path))?;
        db.create_graph_auto_id("social")?;
        drop(db);
        let reported = drift(&spec_arg, &["--json"])?;
        assert_eq!(reported.exit_code, 0, "{}", reported.message);
        let report = json_message(&reported)?;
        assert_eq!(report["in_sync"], true);
        assert!(
            report["items"]
                .as_array()
                .is_some_and(|items| items.iter().any(|item| item["kind"] == "unmanaged_object")),
            "{report}"
        );
        let failed = drift(&spec_arg, &["--unmanaged", "fail"])?;
        assert_eq!(failed.exit_code, 3, "{}", failed.message);

        let grown_path = temp.path().join("grown.yaml");
        write_yaml_spec(
            &grown_path,
            &provision_spec(&json!([
                document_collection("orders"),
                document_collection("events")
            ]))?,
        )?;
        let grown = drift(&path_arg(&grown_path), &[])?;
        assert_eq!(grown.exit_code, 3, "{}", grown.message);
        assert!(
            grown.message.contains("missing_object"),
            "{}",
            grown.message
        );

        let invalid_path = temp.path().join("invalid.yaml");
        write_yaml_spec(
            &invalid_path,
            &provision_spec(&json!([document_collection("__secret")]))?,
        )?;
        let invalid = drift(&path_arg(&invalid_path), &[])?;
        assert_eq!(invalid.exit_code, 2, "{}", invalid.message);
        assert!(
            invalid.message.contains("COLLECTION_NAME_RESERVED"),
            "{}",
            invalid.message
        );
        Ok(())
    }

    #[test]
    fn config_status_and_export_describe_the_database() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_path = temp.path().join("multidb.yaml");
        let spec = provision_spec(&json!([document_collection("orders")]))?;
        write_yaml_spec(&spec_path, &spec)?;
        let db_path = temp.path().join("shop.redb");
        let db_arg = path_arg(&db_path);
        let applied = run(strings(&[
            "config",
            "apply",
            "--spec",
            &path_arg(&spec_path),
            "--auto-approve",
        ]))?;
        assert_eq!(applied.exit_code, 0, "{}", applied.message);

        let status = run(strings(&["config", "status", "--db", &db_arg]))?;
        assert_eq!(status.exit_code, 0, "{}", status.message);
        for expected in [
            "provisioned: status=applied",
            "applied_by=local:cli",
            "managed objects (1): orders (document)",
            "history (last 1 of 1):",
            "guarantees:\n",
            "guarantees.audit.enabled=true enforced_with_development_key",
            "profile manifest: ",
            "drift against the applied spec: in_sync=true",
        ] {
            assert!(
                status.message.contains(expected),
                "{expected}: {}",
                status.message
            );
        }
        let status_json = run(strings(&["config", "status", "--db", &db_arg, "--json"]))?;
        let report = json_message(&status_json)?;
        assert_eq!(report["provisioned"], true);
        assert_eq!(report["record"]["status"], "applied");
        assert_eq!(report["history"].as_array().map(Vec::len), Some(1));
        assert_eq!(report["drift"]["in_sync"], true);

        let out = temp.path().join("exported.yaml");
        let exported = run(strings(&[
            "config",
            "export",
            "--db",
            &db_arg,
            "--out",
            &path_arg(&out),
        ]))?;
        assert_eq!(exported.exit_code, 0, "{}", exported.message);
        assert!(
            exported.message.contains("source=applied_record"),
            "{}",
            exported.message
        );
        assert_eq!(
            serde_norway::from_str::<DatabaseSpec>(&std::fs::read_to_string(&out)?)?,
            spec
        );
        let overwrite = run(strings(&[
            "config",
            "export",
            "--db",
            &db_arg,
            "--out",
            &path_arg(&out),
        ]));
        assert!(
            matches!(overwrite, Err(CliError::Usage(ref message)) if message.contains("--force")),
            "unexpected result: {overwrite:?}"
        );
        let forced = run(strings(&[
            "config",
            "export",
            "--db",
            &db_arg,
            "--out",
            &path_arg(&out),
            "--format",
            "json",
            "--force",
        ]))?;
        assert_eq!(forced.exit_code, 0, "{}", forced.message);
        assert_eq!(
            serde_json::from_str::<DatabaseSpec>(&std::fs::read_to_string(&out)?)?,
            spec
        );
        // stdout has no file to be relative to: the storage path is the database's absolute
        // path.
        let stdout = run(strings(&["config", "export", "--db", &db_arg]))?;
        assert_eq!(stdout.exit_code, 0);
        let mut absolute = spec.clone();
        absolute.deployment.storage_path =
            Some(std::fs::canonicalize(&db_path)?.display().to_string());
        assert_eq!(
            serde_norway::from_str::<DatabaseSpec>(&stdout.message)?,
            absolute
        );

        // A database built outside provisioning exports an inferred spec that plans as
        // adoption and records only.
        let builder_path = temp.path().join("builder.redb");
        let mut builder = create_balanced_database(&builder_path)?;
        builder.create_graph_auto_id("social")?;
        drop(builder);
        let builder_arg = path_arg(&builder_path);
        let unprovisioned = run(strings(&["config", "status", "--db", &builder_arg]))?;
        assert_eq!(unprovisioned.exit_code, 0, "{}", unprovisioned.message);
        assert!(
            unprovisioned.message.contains("not provisioned"),
            "{}",
            unprovisioned.message
        );
        let inferred_path = temp.path().join("inferred.yaml");
        let inferred = run(strings(&[
            "config",
            "export",
            "--db",
            &builder_arg,
            "--out",
            &path_arg(&inferred_path),
        ]))?;
        assert_eq!(inferred.exit_code, 0, "{}", inferred.message);
        assert!(
            inferred.message.contains("source=inferred"),
            "{}",
            inferred.message
        );
        let plan = run(strings(&[
            "config",
            "plan",
            "--spec",
            &path_arg(&inferred_path),
            "--db",
            &builder_arg,
            "--json",
        ]))?;
        assert_eq!(plan.exit_code, 0, "{}", plan.message);
        let plan = json_message(&plan)?;
        let steps = plan["steps"].as_array().ok_or("missing steps")?;
        assert!(!steps.is_empty());
        for step in steps {
            assert!(
                [
                    "adopt_object",
                    "adopt_profile",
                    "apply_security",
                    "record_guarantees",
                    "record_metadata",
                    "record_applied_spec"
                ]
                .contains(&step["kind"].as_str().unwrap_or_default()),
                "{step}"
            );
            assert!(
                ["additive", "metadata"].contains(&step["safety"].as_str().unwrap_or_default()),
                "{step}"
            );
        }
        Ok(())
    }

    /// `path` relative to the working directory of the test process, so a command gets the
    /// relative `--db` spelling a user would type.
    fn relative_to_cwd(path: &std::path::Path) -> Result<String, Box<dyn std::error::Error>> {
        let cwd = std::fs::canonicalize(std::env::current_dir()?)?;
        let relative = super::relative_path(&cwd, &std::fs::canonicalize(path)?)
            .ok_or("no relative path from the working directory")?;
        Ok(relative.display().to_string())
    }

    fn exported_storage_path(path: &std::path::Path) -> Result<String, Box<dyn std::error::Error>> {
        let spec = serde_norway::from_str::<DatabaseSpec>(&std::fs::read_to_string(path)?)?;
        Ok(spec
            .deployment
            .storage_path
            .ok_or("the export has no storage_path")?)
    }

    #[test]
    fn config_export_points_storage_path_at_the_database_from_the_exported_file()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let data = temp.path().join("data");
        std::fs::create_dir_all(data.join("sub"))?;
        let db_path = data.join("l3.redb");
        let mut builder = create_balanced_database(&db_path)?;
        builder.create_graph_auto_id("social")?;
        drop(builder);
        // The relative spelling a user types, relative to the working directory.
        let db_arg = relative_to_cwd(&db_path)?;

        let inferred = data.join("sub").join("l3.yaml");
        let exported = run(strings(&[
            "config",
            "export",
            "--db",
            &db_arg,
            "--out",
            &path_arg(&inferred),
        ]))?;
        assert_eq!(exported.exit_code, 0, "{}", exported.message);
        assert!(
            exported.message.contains("deployment.storage_path is"),
            "{}",
            exported.message
        );
        assert_eq!(
            exported_storage_path(&inferred)?,
            std::path::Path::new("..")
                .join("l3.redb")
                .display()
                .to_string()
        );

        // Without --db the spec finds the exported database and only adopts and records.
        let plan = run(strings(&[
            "config",
            "plan",
            "--spec",
            &path_arg(&inferred),
            "--json",
        ]))?;
        assert_eq!(plan.exit_code, 0, "{}", plan.message);
        let plan = json_message(&plan)?;
        assert_eq!(plan["target"]["kind"], "existing_database", "{plan}");
        let steps = plan["steps"].as_array().ok_or("missing steps")?;
        assert!(!steps.is_empty());
        for step in steps {
            assert!(
                [
                    "adopt_object",
                    "adopt_profile",
                    "record_guarantees",
                    "record_metadata",
                    "record_applied_spec"
                ]
                .contains(&step["kind"].as_str().unwrap_or_default()),
                "{step}"
            );
        }
        let applied = run(strings(&[
            "config",
            "apply",
            "--spec",
            &path_arg(&inferred),
            "--auto-approve",
        ]))?;
        assert_eq!(applied.exit_code, 0, "{}", applied.message);

        // The applied record exports with a storage path relative to the new file, and that
        // spec is the applied one: a different storage path is not a change.
        let moved = temp.path().join("l3.yaml");
        let exported = run(strings(&[
            "config",
            "export",
            "--db",
            &db_arg,
            "--out",
            &path_arg(&moved),
        ]))?;
        assert_eq!(exported.exit_code, 0, "{}", exported.message);
        assert!(
            exported.message.contains("source=applied_record"),
            "{}",
            exported.message
        );
        assert_eq!(
            exported_storage_path(&moved)?,
            std::path::Path::new("data")
                .join("l3.redb")
                .display()
                .to_string()
        );
        let noop = run(strings(&[
            "config",
            "plan",
            "--spec",
            &path_arg(&moved),
            "--json",
        ]))?;
        assert_eq!(noop.exit_code, 0, "{}", noop.message);
        let noop = json_message(&noop)?;
        assert_eq!(noop["outcome"], "noop", "{noop}");
        let drift = run(strings(&["config", "drift", "--spec", &path_arg(&moved)]))?;
        assert_eq!(drift.exit_code, 0, "{}", drift.message);

        // --out never names the database itself or another database, even with --force.
        for out in [db_arg.clone(), path_arg(&db_path)] {
            let refused = run(strings(&[
                "config", "export", "--db", &db_arg, "--out", &out, "--force",
            ]));
            assert!(
                matches!(refused, Err(CliError::Usage(ref message)) if message.contains("over the database")),
                "unexpected result: {refused:?}"
            );
        }
        let other_path = temp.path().join("other.redb");
        drop(create_balanced_database(&other_path)?);
        let refused = run(strings(&[
            "config",
            "export",
            "--db",
            &db_arg,
            "--out",
            &path_arg(&other_path),
            "--force",
        ]));
        assert!(
            matches!(refused, Err(CliError::Usage(ref message)) if message.contains("holds a multidb database")),
            "unexpected result: {refused:?}"
        );
        for path in [&db_path, &other_path] {
            assert!(matches!(
                inspect_database_metadata(path),
                MetadataProbe::Initialized(_)
            ));
        }
        Ok(())
    }

    /// `config export` refuses a spec over `MAX_SPEC_BYTES` as `config upgrade` does: exit 2
    /// with `SPEC_TOO_LARGE`, and the spec goes nowhere, neither to `--out` (even with `--force`)
    /// nor to stdout. The Rust API applies specs of any size, so 300 operation hints of 4 KiB
    /// each record an applied spec of more than 1 MiB.
    #[test]
    fn config_export_refuses_specs_over_the_spec_size_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        use multidb::provision::{
            Confirmation, ProvisionEnvironment, ProvisionOptions, ProvisionOutcome,
            ProvisionSurface, provision_new_database,
        };

        let temp = tempdir()?;
        let db_path = temp.path().join("shop.redb");
        let mut spec = provision_spec(&json!([document_collection("orders")]))?;
        let hint = "x".repeat(multidb::config_spec::MAX_MAP_VALUE_BYTES);
        spec.operation_hints
            .extend((0..300).map(|index| (format!("bulk.h{index}"), hint.clone())));
        let (db, report) = provision_new_database(
            &spec,
            Some(&db_path),
            &Confirmation::AutoApproveAdditive,
            &ProvisionOptions::default(),
            &ProvisionEnvironment::new(ProvisionSurface::RustApi),
        )?;
        assert_eq!(report.status, ProvisionOutcome::Applied, "{report:?}");
        drop(db);

        let db_arg = path_arg(&db_path);
        let out = temp.path().join("exported.yaml");
        let out_arg = path_arg(&out);
        let kept = temp.path().join("kept.json");
        std::fs::write(&kept, "keep")?;
        let kept_arg = path_arg(&kept);
        for extra in [
            vec![],
            vec!["--format", "json"],
            vec!["--out", out_arg.as_str()],
            vec!["--out", kept_arg.as_str(), "--force"],
        ] {
            let mut args = vec!["config", "export", "--db", db_arg.as_str()];
            args.extend(extra);
            let outcome = run(strings(&args))?;
            assert_eq!(outcome.exit_code, 2, "{args:?}: {}", outcome.message);
            assert!(
                outcome.message.contains("SPEC_TOO_LARGE")
                    && !outcome.message.contains("operation_hints")
                    && !outcome.message.contains(&hint),
                "{args:?}: {}",
                outcome.message
            );
        }
        assert!(!out.exists());
        assert_eq!(std::fs::read_to_string(&kept)?, "keep");
        Ok(())
    }

    #[test]
    fn config_spec_commands_refuse_locked_pathless_and_ap_targets()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_path = temp.path().join("multidb.yaml");
        write_yaml_spec(
            &spec_path,
            &provision_spec(&json!([document_collection("orders")]))?,
        )?;
        let spec_arg = path_arg(&spec_path);

        let locked_path = temp.path().join("locked.redb");
        let held = create_balanced_database(&locked_path)?;
        let locked_arg = path_arg(&locked_path);
        for args in [
            vec!["config", "plan", "--spec", &spec_arg, "--db", &locked_arg],
            vec![
                "config",
                "apply",
                "--spec",
                &spec_arg,
                "--db",
                &locked_arg,
                "--auto-approve",
            ],
            vec!["config", "drift", "--spec", &spec_arg, "--db", &locked_arg],
            vec!["config", "status", "--db", &locked_arg],
        ] {
            let outcome = run(strings(&args))?;
            assert_eq!(outcome.exit_code, 1, "{args:?}: {}", outcome.message);
            assert!(
                outcome
                    .message
                    .starts_with("database is in use (is multidb serve running?)"),
                "{args:?}: {}",
                outcome.message
            );
        }
        drop(held);

        let mut pathless = provision_spec(&json!([document_collection("orders")]))?;
        pathless.deployment.storage_path = None;
        let pathless_path = temp.path().join("pathless.yaml");
        write_yaml_spec(&pathless_path, &pathless)?;
        let pathless_arg = path_arg(&pathless_path);
        for args in [
            vec!["config", "plan", "--spec", &pathless_arg],
            vec!["config", "apply", "--spec", &pathless_arg, "--auto-approve"],
            vec!["config", "drift", "--spec", &pathless_arg],
        ] {
            let outcome = run(strings(&args))?;
            assert_eq!(outcome.exit_code, 2, "{args:?}: {}", outcome.message);
            if args[1] != "drift" {
                assert!(
                    outcome.message.contains("PROVISION_STORAGE_PATH_REQUIRED"),
                    "{args:?}: {}",
                    outcome.message
                );
            }
        }

        let ap_path = temp.path().join("ap.redb");
        drop(create_database(
            DbConfig::on_disk(Profile::Document, &ap_path).with_replication(ReplicationKind::Ap),
        )?);
        let before = std::fs::read(&ap_path)?;
        let ap = run(strings(&[
            "config",
            "plan",
            "--spec",
            &spec_arg,
            "--db",
            &path_arg(&ap_path),
        ]))?;
        assert_eq!(ap.exit_code, 2, "{}", ap.message);
        assert!(
            ap.message.contains("PROVISION_REPLICATION_UNSUPPORTED"),
            "{}",
            ap.message
        );
        assert_eq!(std::fs::read(&ap_path)?, before);
        Ok(())
    }

    #[test]
    fn config_spec_flags_parse_and_never_mix_with_the_v1_plan_flow()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_path = temp.path().join("multidb.yaml");
        write_yaml_spec(
            &spec_path,
            &provision_spec(&json!([document_collection("orders")]))?,
        )?;
        let spec_arg = path_arg(&spec_path);

        for args in [
            vec![
                "config",
                "plan",
                "--spec",
                &spec_arg,
                "--current",
                &spec_arg,
            ],
            vec![
                "config",
                "plan",
                "--spec",
                &spec_arg,
                "--desired",
                &spec_arg,
            ],
            vec![
                "config",
                "apply",
                "--spec",
                &spec_arg,
                "--plan",
                "plan.json",
            ],
            vec![
                "config",
                "drift",
                "--spec",
                &spec_arg,
                "--plan",
                "plan.json",
            ],
        ] {
            let outcome = run(strings(&args));
            assert!(
                matches!(outcome, Err(CliError::Usage(ref message)) if message.contains("--spec")),
                "{args:?}: unexpected result: {outcome:?}"
            );
        }
        for (flags, expected) in [
            (vec!["--unmanaged", "sometimes"], "unsupported --unmanaged"),
            (vec!["--allow-weakening", "--json"], "takes a spec path"),
            (
                vec!["--allow-destructive"],
                "missing value for --allow-destructive",
            ),
        ] {
            let mut args = strings(&["config", "plan", "--spec", &spec_arg]);
            args.extend(strings(&flags));
            let outcome = run(args);
            assert!(
                matches!(outcome, Err(CliError::Usage(ref message)) if message.contains(expected)),
                "{flags:?}: unexpected result: {outcome:?}"
            );
        }

        // Options are part of the plan id; repeated and comma-separated flags are the same.
        let plan = |extra: &[&str]| -> Result<String, Box<dyn std::error::Error>> {
            let mut args = strings(&["config", "plan", "--spec", &spec_arg, "--json"]);
            args.extend(strings(extra));
            plan_id_of(&run(args)?)
        };
        let plain = plan(&[])?;
        let comma = plan(&["--allow-destructive", "collections.a,collections.b"])?;
        let repeated = plan(&[
            "--allow-destructive",
            "collections.b",
            "--allow-destructive",
            "collections.a",
        ])?;
        assert_eq!(comma, repeated);
        assert_ne!(comma, plain);
        assert_ne!(plan(&["--unmanaged", "fail"])?, plain);
        Ok(())
    }

    #[test]
    fn config_apply_plan_stays_audit_only() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let plan_path = temp.path().join("plan.json");
        let current = DatabaseSpec::from_db_config(
            "apply",
            &DbConfig::on_disk(Profile::Balanced, "apply.redb"),
        );
        let mut desired = current.clone();
        desired
            .operation_hints
            .insert("planner.sample_rows".to_owned(), "1000".to_owned());
        let plan = MigrationPlanner::plan(&current, &desired);
        std::fs::write(&plan_path, serde_json::to_string_pretty(&plan)?)?;

        let outcome = run(strings(&[
            "config",
            "apply",
            "--plan",
            &path_arg(&plan_path),
            "--confirm",
            &plan.plan_id,
            "--json",
        ]))?;

        assert_eq!(outcome.exit_code, 0, "{}", outcome.message);
        let report = json_message(&outcome)?;
        assert_eq!(report["status"], "confirmed");
        assert_eq!(report["data_mutated"], false);
        assert!(!temp.path().join("apply.redb").exists());
        Ok(())
    }

    #[test]
    fn init_guided_apply_provisions_a_new_database_and_refuses_an_existing_one()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_path = temp.path().join("multidb.yaml");
        let init = |extra: &[&str]| {
            let mut args = strings(&[
                "init",
                "--guided",
                "--profile",
                "secure_app",
                "--name",
                "Vault",
                "--out",
                &path_arg(&spec_path),
                "--force",
            ]);
            args.extend(strings(extra));
            run(args)
        };
        let db_path = temp.path().join("vault.redb");

        let created = init(&["--apply"])?;
        assert_eq!(created.exit_code, 0, "{}", created.message);
        assert!(
            created
                .message
                .contains(&format!("database path={}", db_path.display())),
            "{}",
            created.message
        );
        assert!(
            created.message.contains("provisioning applied"),
            "{}",
            created.message
        );
        assert!(db_path.exists());
        let spec = serde_norway::from_str::<DatabaseSpec>(&std::fs::read_to_string(&spec_path)?)?;
        assert_eq!(spec.version, 1);
        assert!(!spec.guarantees.encryption.at_rest);

        let again = init(&["--apply", "--json"])?;
        assert_eq!(again.exit_code, 2, "{}", again.message);
        let summary = json_message(&again)?;
        assert_eq!(summary["valid"], true);
        assert_eq!(summary["provision"]["status"], "already_initialized");
        assert!(
            summary["provision"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("config apply")),
            "{summary}"
        );
        assert!(spec_path.exists());

        let custom_db = temp.path().join("custom.redb");
        let explicit = init(&["--apply", "--db", &path_arg(&custom_db), "--json"])?;
        assert_eq!(explicit.exit_code, 0, "{}", explicit.message);
        let summary = json_message(&explicit)?;
        assert_eq!(summary["database"], json!(path_arg(&custom_db)));
        assert_eq!(summary["provision"]["status"], "applied");
        assert!(custom_db.exists());

        let without_apply = init(&["--db", &path_arg(&custom_db)]);
        assert!(
            matches!(without_apply, Err(CliError::Usage(ref message)) if message.contains("--apply")),
            "unexpected result: {without_apply:?}"
        );
        Ok(())
    }

    #[test]
    fn init_guided_template_apply_keeps_the_files_of_a_blocked_plan()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let out_dir = temp.path().join("secure");

        let outcome = run(strings(&[
            "init",
            "--guided",
            "--template",
            "secure-saas",
            "--name",
            "Secure App",
            "--out",
            &path_arg(&out_dir),
            "--apply",
        ]))?;

        assert_eq!(outcome.exit_code, 2, "{}", outcome.message);
        for file in ["multidb.yaml", "README.md", "seed.json", "smoke.ps1"] {
            assert!(out_dir.join(file).exists(), "{file}");
        }
        assert!(!out_dir.join("secure_app.redb").exists());
        // Encryption at rest is the only reason: the CLI cannot enforce it.
        assert!(
            outcome.message.contains(
                "is blocked: GUARANTEE_NOT_ENFORCEABLE at guarantees.encryption.at_rest\n"
            ),
            "{}",
            outcome.message
        );

        let json = run(strings(&[
            "init",
            "--guided",
            "--template",
            "secure-saas",
            "--name",
            "Secure App",
            "--out",
            &path_arg(&out_dir),
            "--force",
            "--apply",
            "--json",
        ]))?;
        assert_eq!(json.exit_code, 2, "{}", json.message);
        let summary = json_message(&json)?;
        assert_eq!(summary["valid"], true);
        assert_eq!(summary["provision"]["status"], "blocked");
        let errors = summary["provision"]["plan"]["issues"]
            .as_array()
            .ok_or("plan issues")?
            .iter()
            .filter(|issue| issue["severity"] == "error")
            .map(|issue| issue["code"].as_str())
            .collect::<Vec<_>>();
        assert_eq!(errors, [Some("GUARANTEE_NOT_ENFORCEABLE")], "{summary}");
        assert!(!out_dir.join("secure_app.redb").exists());
        Ok(())
    }

    #[test]
    fn init_guided_template_apply_creates_the_default_database_and_stays_in_sync()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let out_dir = temp.path().join("game");

        let outcome = run(strings(&[
            "init",
            "--guided",
            "--template",
            "game-save",
            "--name",
            "Arcade Save",
            "--out",
            &path_arg(&out_dir),
            "--apply",
            "--json",
        ]))?;

        assert_eq!(outcome.exit_code, 0, "{}", outcome.message);
        let summary = json_message(&outcome)?;
        // game-save leaves the path to the app, so init --apply picks <out>/<sanitized name>.redb.
        let db_path = out_dir.join("arcade_save.redb");
        assert_eq!(summary["database"], json!(path_arg(&db_path)));
        assert_eq!(summary["provision"]["status"], "applied");
        assert!(db_path.exists());
        let spec_path = out_dir.join("multidb.yaml");
        let spec = serde_norway::from_str::<DatabaseSpec>(&std::fs::read_to_string(&spec_path)?)?;
        assert_eq!(spec.version, 2);
        assert_eq!(spec.deployment.storage_path, None);

        let spec_db = |command: &str, extra: &[&str]| {
            let mut args = strings(&[
                "config",
                command,
                "--spec",
                &path_arg(&spec_path),
                "--db",
                &path_arg(&db_path),
            ]);
            args.extend(strings(extra));
            run(args)
        };
        let drift = spec_db("drift", &["--json"])?;
        assert_eq!(drift.exit_code, 0, "{}", drift.message);
        assert_eq!(json_message(&drift)?["in_sync"], true);
        let again = spec_db("apply", &["--auto-approve", "--json"])?;
        assert_eq!(again.exit_code, 0, "{}", again.message);
        assert_eq!(json_message(&again)?["status"], "noop");
        Ok(())
    }

    #[test]
    fn config_upgrade_lists_the_physical_parameters_provisioning_needs()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/spec-v1/ai-memory.yaml");

        let outcome = run(strings(&[
            "config",
            "upgrade",
            "--spec",
            &path_arg(&fixture),
            "--json",
        ]))?;

        assert_eq!(outcome.exit_code, 0, "{}", outcome.message);
        let report = json_message(&outcome)?;
        let notes = report["notes"].as_array().ok_or("missing notes")?;
        for path in [
            "collections[0].physical.dim",
            "collections[1].physical.document_indexes",
        ] {
            let prefix = format!("provisioning blocker: add {path} before config apply --spec");
            assert!(
                notes
                    .iter()
                    .any(|note| note.as_str().is_some_and(|note| note.starts_with(&prefix))),
                "{path}: {notes:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn provision_exit_code_covers_every_outcome_and_error() -> Result<(), Box<dyn std::error::Error>>
    {
        use super::{ProvisionEnd, provision_exit_code};
        use multidb::config_spec::{ApplyCheckReport, ApplyStatus};
        use multidb::provision::{
            PlanOutcome, ProvisionApplyReport, ProvisionEnvironment, ProvisionError,
            ProvisionOptions, ProvisionOutcome, ProvisionSurface, ProvisionTargetRef, StartupMode,
            StartupRefusal, plan_provision,
        };

        let temp = tempdir()?;
        let spec = provision_spec(&json!([document_collection("orders")]))?;
        let mut plan = plan_provision(
            &spec,
            ProvisionTargetRef::Path(&temp.path().join("shop.redb")),
            &ProvisionOptions::default(),
            &ProvisionEnvironment::new(ProvisionSurface::Cli),
        )?;
        for (outcome, code) in [
            (PlanOutcome::Noop, 0),
            (PlanOutcome::Ready, 0),
            (PlanOutcome::NeedsFlags, 2),
            (PlanOutcome::Blocked, 2),
        ] {
            plan.outcome = outcome;
            assert_eq!(
                provision_exit_code(ProvisionEnd::Planned(&plan)),
                code,
                "{outcome:?}"
            );
        }

        let mut drifted = plan.clone();
        drifted.drift.in_sync = true;
        assert_eq!(provision_exit_code(ProvisionEnd::Drifted(&drifted)), 0);
        drifted.drift.in_sync = false;
        assert_eq!(provision_exit_code(ProvisionEnd::Drifted(&drifted)), 3);
        drifted.target.path = None;
        assert_eq!(provision_exit_code(ProvisionEnd::Drifted(&drifted)), 2);
        let mut invalid = plan.clone();
        invalid.validation.valid = false;
        assert_eq!(provision_exit_code(ProvisionEnd::Drifted(&invalid)), 2);

        let mut partial = None;
        for (status, code) in [
            (ProvisionOutcome::Applied, 0),
            (ProvisionOutcome::Noop, 0),
            (ProvisionOutcome::PartiallyApplied, 1),
            (ProvisionOutcome::Rejected, 2),
            (ProvisionOutcome::Blocked, 2),
            (ProvisionOutcome::PlanChanged, 4),
        ] {
            let report = ProvisionApplyReport {
                status,
                check: ApplyCheckReport {
                    plan_id: plan.plan_id.clone(),
                    status: ApplyStatus::Confirmed,
                    valid: true,
                    confirmation_matched: true,
                    audit_recorded: true,
                    data_mutated: false,
                    message: String::new(),
                },
                plan_id: plan.plan_id.clone(),
                applied_steps: Vec::new(),
                failed_step: None,
                record: None,
                warnings: Vec::new(),
                session_pending_restart: false,
            };
            assert_eq!(
                provision_exit_code(ProvisionEnd::Applied(&report)),
                code,
                "{status:?}"
            );
            if status == ProvisionOutcome::PartiallyApplied {
                partial = Some(report);
            }
        }

        let unsupported = || ConfigError::Unsupported("unsupported".to_owned());
        let startup_refused = |applied: Option<ProvisionApplyReport>| {
            ProvisionError::StartupRefused(Box::new(StartupRefusal {
                mode: StartupMode::Verify,
                reason: "database missing".to_owned(),
                plan: plan.clone(),
                applied,
            }))
        };
        for (error, code) in [
            (
                ProvisionError::InvalidSpec(Box::new(plan.validation.clone())),
                2,
            ),
            (ProvisionError::Blocked(Box::new(plan.clone())), 2),
            (ProvisionError::NeedsFlags(Box::new(plan.clone())), 2),
            (
                ProvisionError::ConfirmationRequired(Box::new(plan.clone())),
                2,
            ),
            (
                ProvisionError::PlanChanged {
                    confirmed: "prov_a".to_owned(),
                    current: "prov_b".to_owned(),
                },
                4,
            ),
            (ProvisionError::AlreadyInitialized, 2),
            (ProvisionError::AuditRequired, 2),
            (ProvisionError::PermissionMissing("admin".to_owned()), 2),
            (ProvisionError::Locked("held".to_owned()), 1),
            (ProvisionError::RecordUnreadable("newer".to_owned()), 1),
            (
                ProvisionError::Db(multidb::db::DbError::Config(unsupported())),
                1,
            ),
            (ProvisionError::Config(unsupported()), 1),
            (ProvisionError::Io("disk".to_owned()), 1),
            (startup_refused(None), 2),
            (startup_refused(partial.clone()), 1),
        ] {
            assert_eq!(
                provision_exit_code(ProvisionEnd::Failed(&error)),
                code,
                "{error:?}"
            );
        }
        Ok(())
    }

    /// The environment of a local-dev `serve` in spec mode with `spec_path`, plus `extra`.
    fn spec_serve_config(
        spec_path: &std::path::Path,
        extra: &[(&str, &str)],
    ) -> Result<super::ServeConfig, CliError> {
        let spec_arg = path_arg(spec_path);
        let mut values = vec![
            ("MULTIDB_RUNTIME_MODE", "local-dev"),
            ("MULTIDB_PG_TLS_MODE", "disabled"),
            ("MULTIDB_PG_PASSWORD", "pg-secret"),
            ("MULTIDB_SPEC_PATH", spec_arg.as_str()),
        ];
        values.extend_from_slice(extra);
        let env = env_map(&values);
        serve_config_from_sources(|name| env.get(name).cloned())
    }

    /// The message of a start that `serve` refused.
    fn serve_refusal(
        result: Result<multidb::db::Database, CliError>,
    ) -> Result<String, Box<dyn std::error::Error>> {
        match result {
            Err(CliError::Usage(message)) => Ok(message),
            Err(other) => Err(format!("expected a refused start, got {other}").into()),
            Ok(_) => Err("expected a refused start, but serve opened the database".into()),
        }
    }

    /// The `multidb config apply ...` command a refused start prints, as CLI arguments.
    fn printed_apply_command(message: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let command = message
            .lines()
            .find_map(|line| line.strip_prefix("apply it with: multidb "))
            .ok_or_else(|| format!("no apply command in: {message}"))?;
        Ok(command.split_whitespace().map(str::to_owned).collect())
    }

    #[test]
    fn serve_config_parses_spec_mode() -> Result<(), Box<dyn std::error::Error>> {
        use multidb::{config_spec::ReplicationMode, provision::StartupMode};

        let base = [
            ("MULTIDB_RUNTIME_MODE", "local-dev"),
            ("MULTIDB_PG_TLS_MODE", "disabled"),
            ("MULTIDB_PG_PASSWORD", "pg-secret"),
        ];
        let parse = |extra: &[(&str, &str)]| {
            let mut values = base.to_vec();
            values.extend_from_slice(extra);
            let env = env_map(&values);
            serve_config_from_sources(|name| env.get(name).cloned())
        };

        // Without a spec path, serve is unchanged and MULTIDB_REPLICATION is not read.
        assert_eq!(parse(&[("MULTIDB_REPLICATION", "raft")])?.spec, None);
        assert_eq!(parse(&[("MULTIDB_SPEC_PATH", "  ")])?.spec, None);
        let apply_alone = parse(&[("MULTIDB_SPEC_APPLY", "create")]);
        assert!(
            matches!(apply_alone, Err(CliError::Usage(ref message)) if message.contains("needs MULTIDB_SPEC_PATH")),
            "unexpected result: {apply_alone:?}"
        );

        let spec_path = "/etc/multidb/multidb.yaml";
        let default = parse(&[("MULTIDB_SPEC_PATH", spec_path)])?;
        assert_eq!(
            default.spec,
            Some(ServeSpecConfig {
                path: std::path::PathBuf::from(spec_path),
                mode: StartupMode::Verify,
                explicit_db_path: false,
                explicit_profile: false,
                explicit_replication: None,
            })
        );
        for (value, mode) in [
            ("verify", StartupMode::Verify),
            ("create", StartupMode::Create),
            ("additive", StartupMode::Additive),
            (" Additive ", StartupMode::Additive),
            ("", StartupMode::Verify),
        ] {
            let config = parse(&[
                ("MULTIDB_SPEC_PATH", spec_path),
                ("MULTIDB_SPEC_APPLY", value),
            ])?;
            assert_eq!(config.spec.map(|spec| spec.mode), Some(mode), "{value:?}");
        }
        let invalid = parse(&[
            ("MULTIDB_SPEC_PATH", spec_path),
            ("MULTIDB_SPEC_APPLY", "apply"),
        ]);
        assert!(
            matches!(invalid, Err(CliError::Usage(ref message)) if message.contains("unsupported MULTIDB_SPEC_APPLY apply")),
            "unexpected result: {invalid:?}"
        );

        // Explicit means present and not blank, as env_or_default reads the values.
        let explicit = parse(&[
            ("MULTIDB_SPEC_PATH", spec_path),
            ("MULTIDB_DB_PATH", "/data/shop.redb"),
            ("MULTIDB_PROFILE", "balanced"),
            ("MULTIDB_REPLICATION", "CP"),
        ])?;
        let spec = explicit.spec.ok_or("spec mode")?;
        assert!(spec.explicit_db_path && spec.explicit_profile);
        assert_eq!(spec.explicit_replication, Some(ReplicationMode::Cp));
        assert_eq!(
            explicit.db_path,
            std::path::PathBuf::from("/data/shop.redb")
        );
        let blank = parse(&[
            ("MULTIDB_SPEC_PATH", spec_path),
            ("MULTIDB_DB_PATH", "   "),
            ("MULTIDB_PROFILE", ""),
            ("MULTIDB_REPLICATION", " "),
        ])?;
        let spec = blank.spec.ok_or("spec mode")?;
        assert!(!spec.explicit_db_path && !spec.explicit_profile);
        assert_eq!(spec.explicit_replication, None);
        assert_eq!(
            blank.db_path,
            std::path::PathBuf::from("/var/lib/multidb/multidb.redb")
        );
        let replication = parse(&[
            ("MULTIDB_SPEC_PATH", spec_path),
            ("MULTIDB_REPLICATION", "raft"),
        ]);
        assert!(
            matches!(replication, Err(CliError::Usage(ref message)) if message.contains("unsupported MULTIDB_REPLICATION raft")),
            "unexpected result: {replication:?}"
        );
        Ok(())
    }

    /// Explicit variables that contradict the spec, encryption at rest and volatile profiles
    /// are refused before anything is opened or created.
    #[test]
    fn serve_spec_mode_refuses_conflicts_before_opening() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp = tempdir()?;
        let spec_path = temp.path().join("multidb.yaml");
        let mut spec = provision_spec(&json!([document_collection("orders")]))?;
        spec.deployment.storage_path = Some("data/shop.redb".to_owned());
        write_yaml_spec(&spec_path, &spec)?;
        let data_dir = temp.path().join("data");

        for (extra, reason) in [
            (
                vec![("MULTIDB_PROFILE", "analytical")],
                "MULTIDB_PROFILE selects the storage profile Analytical, but the spec profile \
                 balanced uses Balanced",
            ),
            (
                vec![("MULTIDB_REPLICATION", "ap")],
                "MULTIDB_REPLICATION is ap, but the spec defaults.replication is cp",
            ),
        ] {
            let message = serve_refusal(serve_database(&spec_serve_config(&spec_path, &extra)?))?;
            assert!(message.contains(reason), "{message}");
            assert!(message.contains("before binding any port"), "{message}");
            assert!(!data_dir.exists(), "nothing is created: {message}");
        }

        // Agreeing values, auto and blank values (the Helm chart blanks the image's
        // MULTIDB_PROFILE=transactional in spec mode) never conflict: the start goes on to the
        // plan, which verify refuses for the missing database.
        for extra in [
            vec![("MULTIDB_PROFILE", "balanced")],
            vec![("MULTIDB_PROFILE", "auto")],
            vec![("MULTIDB_PROFILE", ""), ("MULTIDB_REPLICATION", "")],
            vec![("MULTIDB_REPLICATION", "cp")],
        ] {
            let message = serve_refusal(serve_database(&spec_serve_config(&spec_path, &extra)?))?;
            assert!(message.contains("database missing at"), "{message}");
        }

        // Refused specs name another directory, which must not be created either.
        let mut refused = spec.clone();
        refused.deployment.storage_path = Some("refused/shop.redb".to_owned());
        let mut at_rest = refused.clone();
        at_rest.guarantees.encryption.at_rest = true;
        let mut volatile = refused.clone();
        volatile.profile = "in_memory".to_owned();
        volatile.deployment.mode = DeploymentMode::Embedded;
        volatile.deployment.storage_path = None;
        let mut invalid = refused;
        invalid.collections[0].name = "__orders".to_owned();
        for (spec, reason) in [
            (&at_rest, "guarantees.encryption.at_rest"),
            (&volatile, "PROVISION_VOLATILE_ON_SURFACE"),
            (&invalid, "the spec is invalid"),
        ] {
            let path = temp.path().join("refused.yaml");
            write_yaml_spec(&path, spec)?;
            let message = serve_refusal(serve_database(&spec_serve_config(&path, &[])?))?;
            assert!(message.contains(reason), "{reason}: {message}");
        }
        let missing = serve_refusal(serve_database(&spec_serve_config(
            &temp.path().join("missing.yaml"),
            &[],
        )?))?;
        assert!(missing.contains("the spec cannot be read"), "{missing}");
        let version = temp.path().join("version.yaml");
        std::fs::write(&version, "version: 3\nname: shop\n")?;
        let decode = serve_refusal(serve_database(&spec_serve_config(&version, &[])?))?;
        assert!(decode.contains("the spec cannot be decoded"), "{decode}");
        assert!(!temp.path().join("refused").exists());
        Ok(())
    }

    /// Verify mode refuses a missing database and prints a `config apply` command whose
    /// `prov_` id confirms the plan; after running it, verify mode starts.
    #[test]
    fn serve_spec_mode_verify_prints_a_command_that_confirms()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_dir = temp.path().join("app");
        std::fs::create_dir(&spec_dir)?;
        let spec_path = spec_dir.join("multidb.yaml");
        let mut spec = provision_spec(&json!([document_collection("orders")]))?;
        spec.deployment.storage_path = Some("data/shop.redb".to_owned());
        write_yaml_spec(&spec_path, &spec)?;
        let db_path = spec_dir.join("data").join("shop.redb");
        let verify = spec_serve_config(&spec_path, &[])?;

        let message = serve_refusal(serve_database(&verify))?;
        for expected in [
            "MULTIDB_SPEC_APPLY=verify",
            "database missing at",
            "set MULTIDB_SPEC_APPLY=create",
            "provisioning plan prov_",
            "apply it with: multidb config apply --spec",
            "--confirm prov_",
        ] {
            assert!(message.contains(expected), "{expected}: {message}");
        }
        assert!(!db_path.exists(), "verify never creates the database");

        let args = printed_apply_command(&message)?;
        assert!(args.contains(&std::fs::canonicalize(&spec_path)?.display().to_string()));
        assert!(
            args.contains(
                &std::fs::canonicalize(spec_dir.join("data"))?
                    .join("shop.redb")
                    .display()
                    .to_string()
            )
        );
        let applied = run(args)?;
        assert_eq!(applied.exit_code, 0, "{}", applied.message);
        assert!(db_path.exists());

        let database = serve_database(&verify)?;
        assert!(database.catalog().contains_key("orders"));
        Ok(())
    }

    /// Create mode boots a new database; verify then starts it unchanged, refuses pending
    /// additive changes, which additive mode applies, and prints the exact flags a plan needs.
    #[test]
    fn serve_spec_mode_creates_verifies_and_applies_additive_changes()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let spec_path = temp.path().join("multidb.yaml");
        let orders = provision_spec(&json!([document_collection("orders")]))?;
        write_yaml_spec(&spec_path, &orders)?;
        let db_path = temp.path().join("shop.redb");
        let config = |mode: &str| spec_serve_config(&spec_path, &[("MULTIDB_SPEC_APPLY", mode)]);

        let mut database = serve_database(&config("create")?)?;
        assert!(database.catalog().contains_key("orders"));
        let record = database.provision_state()?.ok_or("a provision record")?;
        assert_eq!(record.applied_by, "local:serve");
        // serve bootstraps the admin principal after provisioning; its names are reserved.
        database.ensure_bootstrap_admin_principal()?;
        drop(database);
        assert!(db_path.exists());

        drop(serve_database(&config("verify")?)?);
        drop(serve_database(&config("create")?)?);

        let both = provision_spec(&json!([
            document_collection("orders"),
            document_collection("notes")
        ]))?;
        write_yaml_spec(&spec_path, &both)?;
        for mode in ["verify", "create"] {
            let message = serve_refusal(serve_database(&config(mode)?))?;
            for expected in [
                "differs from the spec by",
                "set MULTIDB_SPEC_APPLY=additive",
                "--confirm prov_",
            ] {
                assert!(message.contains(expected), "{mode}: {expected}: {message}");
            }
        }
        let database = serve_database(&config("additive")?)?;
        assert!(database.catalog().contains_key("notes"));
        drop(database);
        drop(serve_database(&config("verify")?)?);

        // Releasing notes is destructive: no mode applies it, and the printed command carries
        // exactly the flag the plan needs.
        write_yaml_spec(&spec_path, &orders)?;
        let message = serve_refusal(serve_database(&config("additive")?))?;
        assert!(
            message.contains("needs flags a start never gives"),
            "{message}"
        );
        let args = printed_apply_command(&message)?;
        let flag = args
            .iter()
            .position(|arg| arg == "--allow-destructive")
            .ok_or_else(|| format!("no --allow-destructive in {args:?}"))?;
        assert_eq!(
            args.get(flag + 1).map(String::as_str),
            Some("collections.notes")
        );
        let applied = run(args)?;
        assert_eq!(applied.exit_code, 0, "{}", applied.message);
        let database = serve_database(&config("verify")?)?;
        assert!(
            database.catalog().contains_key("notes"),
            "release keeps the data"
        );
        drop(database);

        // An explicit MULTIDB_DB_PATH wins over the spec storage path.
        let other = temp.path().join("other").join("explicit.redb");
        let explicit = spec_serve_config(
            &spec_path,
            &[
                ("MULTIDB_SPEC_APPLY", "create"),
                ("MULTIDB_DB_PATH", &path_arg(&other)),
            ],
        )?;
        drop(serve_database(&explicit)?);
        assert!(other.exists());
        Ok(())
    }

    /// In spec mode serve hands the spec digest and the apply mode to the Control Plane, which
    /// reports them in `GET /provision/state`; without a spec there is no spec mode.
    #[test]
    fn serve_spec_mode_reports_the_spec_digest_and_mode() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp = tempdir()?;
        let spec_path = temp.path().join("multidb.yaml");
        let orders = provision_spec(&json!([document_collection("orders")]))?;
        write_yaml_spec(&spec_path, &orders)?;
        let config = spec_serve_config(&spec_path, &[("MULTIDB_SPEC_APPLY", "create")])?;
        let (database, spec_mode) = super::serve_database_with_spec_mode(&config)?;
        drop(database);
        let spec_mode = spec_mode.ok_or("spec mode reports the spec")?;
        assert_eq!(
            spec_mode.spec_digest,
            multidb::config_spec::spec_digest(&orders)
        );
        assert_eq!(
            spec_mode.apply_mode,
            multidb::provision::StartupMode::Create
        );

        let plain_path = temp.path().join("plain.redb");
        let env = env_map(&[
            ("MULTIDB_RUNTIME_MODE", "local-dev"),
            ("MULTIDB_PG_TLS_MODE", "disabled"),
            ("MULTIDB_PG_PASSWORD", "pg-secret"),
            ("MULTIDB_DB_PATH", &path_arg(&plain_path)),
        ]);
        let plain = serve_config_from_sources(|name| env.get(name).cloned())?;
        let (database, spec_mode) = super::serve_database_with_spec_mode(&plain)?;
        drop(database);
        assert_eq!(spec_mode, None);
        Ok(())
    }
}
