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
        BackupConfig, BackupError, RestoreTarget, full_backup, incremental_backup, list_backups,
        restore_backup, verify_backup,
    },
    cdc::{
        ChangefeedFilter, ChangefeedOptions, ChangefeedTarget, MaterializedViewSpec, ResumeToken,
        SubscriptionConfig,
    },
    config_spec::{
        ApplyCheckReport, ApplyStatus, ConfigExplainer, ConsistencyDomainSpec, ConsistencyMode,
        DatabaseDefaults, DatabaseSpec, DeploymentMode, DeploymentSpec, ExplainConfigReport,
        GuaranteeSpec, GuaranteeValidator, MigrationPlan, MigrationPlanner, ProfileSpec,
        ReplicationMode, TopologySpec, ValidationReport, ValidationSeverity, built_in_profile,
        built_in_profiles, collection_role_definition, collection_role_definitions,
        consistency_domain_definition, consistency_domain_definitions,
    },
    db::{DbConfig, DbError, Profile, create_database, open_database},
    migration::{
        ExportFormat, ImportOptions, MigrationError, export_table_csv, export_table_jsonl,
        import_pg_dump_plain, import_table_csv, import_table_jsonl, migrate_value_codec,
    },
    network::{AuthConfig, NetworkConfig, PgServer, ScramCredential, TlsConfig, TlsMode},
    runtime_advisor::{RuntimeAdviceDecisionRequest, RuntimeAdviceReport, RuntimeAdviceStatus},
    templates::{
        TemplateMaterialization, TemplateOutputFormat, built_in_template, built_in_templates,
        materialize_template,
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
    Yaml(#[from] serde_yaml::Error),
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
    profile: Profile,
    admin_token: Option<String>,
    admin_password: Option<String>,
    admin_password_reset: bool,
    admin_session_ttl_seconds: u64,
    admin_login_rate_limit: multidb::admin::AdminLoginRateLimitConfig,
    pg_user: String,
    pg_password: String,
    pg_tls: ServePgTls,
    studio_dir: PathBuf,
}

impl CliRun {
    fn success(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            exit_code: 0,
        }
    }

    fn validation(message: impl Into<String>, valid: bool) -> Self {
        Self {
            message: message.into(),
            exit_code: if valid { 0 } else { 2 },
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
        _ => Err(usage()),
    }
}

fn run_config_validate(args: &[String]) -> Result<CliRun, CliError> {
    let spec_path = PathBuf::from(required_value(args, "--spec")?);
    let output = output_format(args)?;
    let spec = read_spec_file(&spec_path)?;
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
    let spec = read_spec_file(&spec_path)?;
    let report = ConfigExplainer::explain(&spec);
    let message = if output == "json" {
        serde_json::to_string_pretty(&report)?
    } else {
        format_explain_report(&report)
    };

    Ok(CliRun::validation(message, report.validation.valid))
}

fn run_config_plan(args: &[String]) -> Result<CliRun, CliError> {
    let current_path = PathBuf::from(required_value(args, "--current")?);
    let desired_path = PathBuf::from(required_value(args, "--desired")?);
    let output = output_format(args)?;
    let current = read_spec_file(&current_path)?;
    let desired = read_spec_file(&desired_path)?;
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
    if let Some(template_name) = template_arg {
        return run_init_template(args, &template_name, &name);
    }

    let profile_name = profile_arg.ok_or_else(|| {
        CliError::Usage("missing --profile or --template for guided init".to_owned())
    })?;
    let profile = resolve_built_in_profile(&profile_name).ok_or_else(|| {
        CliError::Usage(format!(
            "unknown profile {profile_name}; run multidb profile list"
        ))
    })?;
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

    let spec = guided_spec(profile, &name);
    let report = GuaranteeValidator::validate(&spec);
    let content = serialize_spec(&spec, &file_format)?;
    std::fs::write(&out_path, content.as_bytes())?;

    let output = output_format(args)?;
    let message = if output == "json" {
        serde_json::to_string_pretty(&json!({
            "path": out_path.display().to_string(),
            "name": spec.name,
            "profile": spec.profile,
            "status": report.status,
            "valid": report.valid,
        }))?
    } else {
        format!(
            "created config path={} name={} profile={} status={:?} valid={}",
            out_path.display(),
            spec.name,
            spec.profile,
            report.status,
            report.valid
        )
    };

    Ok(CliRun::validation(message, report.valid))
}

fn run_init_template(args: &[String], template_name: &str, name: &str) -> Result<CliRun, CliError> {
    let format = template_output_format(args)?;
    let materialized = materialize_template(template_name, name, format)?;
    let out_dir = optional_value(args, "--out")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(materialized.template_slug));
    ensure_template_output_dir(&out_dir, has_flag(args, "--force"))?;
    write_template_materialization(&out_dir, &materialized)?;

    let files = materialized
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect::<Vec<_>>();
    let message = if output_format(args)? == "json" {
        serde_json::to_string_pretty(&json!({
            "path": out_dir.display().to_string(),
            "name": materialized.spec.name,
            "template": materialized.template_slug,
            "profile": materialized.spec.profile,
            "status": materialized.validation.status,
            "valid": materialized.validation.valid,
            "files": files,
        }))?
    } else {
        format!(
            "created template path={} name={} template={} profile={} status={:?} valid={} files={}",
            out_dir.display(),
            materialized.spec.name,
            materialized.template_slug,
            materialized.spec.profile,
            materialized.validation.status,
            materialized.validation.valid,
            files.join(",")
        )
    };

    Ok(CliRun::validation(message, materialized.validation.valid))
}

fn run_profile(args: &[String]) -> Result<CliRun, CliError> {
    require_list_command(args)?;
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
                })
            })
            .collect::<Vec<_>>();
        serde_json::to_string_pretty(&profiles)?
    } else {
        format_profiles()
    };
    Ok(CliRun::success(message))
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
            "template={} title={} profile={} valid={} status={:?}\nwhy={}\n{}",
            template.slug,
            template.title,
            materialized.spec.profile,
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
    let profile = parse_profile(&required_value(args, "--profile")?)?;
    let database = open_database(DbConfig::on_disk(profile, db))?;
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
    let profile = parse_profile(&required_value(args, "--profile")?)?;
    let target = match optional_value(args, "--to-lsn") {
        Some(value) => RestoreTarget::Lsn(
            value
                .parse()
                .map_err(|error| CliError::Usage(format!("invalid --to-lsn: {error}")))?,
        ),
        None => RestoreTarget::Latest,
    };

    let report = restore_backup(&backup, DbConfig::on_disk(profile, dest_db), target)?;
    Ok(format!(
        "restored timeline={} lsn={} applied_commits={}",
        report.timeline_id, report.restored_lsn, report.applied_commits
    ))
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
    if let Some(parent) = config.db_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let database = create_database(DbConfig::on_disk(config.profile, &config.db_path))?;
    let database = Arc::new(tokio::sync::Mutex::new(database));

    let mut admin_state = multidb::admin::AdminState::from_database_handle(Arc::clone(&database))
        .with_admin_session_ttl_seconds(config.admin_session_ttl_seconds)
        .with_admin_login_rate_limit(config.admin_login_rate_limit)
        .with_studio_assets_dir(config.studio_dir.clone());
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
            axum::serve(admin_listener, admin_router)
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
                axum::serve(listener, multidb::admin::router(state)).await?;
                Ok::<(), std::io::Error>(())
            })?;
            Ok(format!("admin server stopped on {bind}"))
        }
        _ => Err(usage()),
    }
}

fn format_validation_report(report: &ValidationReport) -> String {
    let mut lines = vec![format!("valid={} status={:?}", report.valid, report.status)];
    for issue in &report.issues {
        lines.push(format!(
            "{} {} {}: {} suggestion: {}",
            severity_label(issue.severity),
            issue.code,
            issue.path,
            issue.message,
            issue.suggestion
        ));
    }
    lines.join("\n")
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
            format!(
                "slug={} status={:?} aliases={} default_domain={} roles={} description={}",
                profile.slug,
                profile.status,
                aliases,
                domain_slug(profile.default_domain),
                roles,
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
    let replication = replication_for_domain(profile.default_domain);
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
            mode: profile.default_domain,
        }],
        collections: Vec::new(),
        extensions: Vec::new(),
        overrides: std::collections::BTreeMap::new(),
        operation_hints: std::collections::BTreeMap::new(),
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
    }
}

fn resolve_built_in_profile(value: &str) -> Option<&'static ProfileSpec> {
    built_in_profile(value).or_else(|| {
        let normalized = value.replace('-', "_");
        built_in_profile(&normalized)
    })
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
        serde_yaml::to_string(spec).map_err(Into::into)
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

fn read_spec_file(path: &Path) -> Result<DatabaseSpec, CliError> {
    let input = std::fs::read_to_string(path)?;
    if is_yaml_path(path) {
        serde_yaml::from_str(&input).map_err(Into::into)
    } else {
        serde_json::from_str(&input).map_err(Into::into)
    }
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
    let profile = parse_profile(&env_or_default(
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
    })
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
    let profile = parse_profile(&required_value(args, "--profile")?)?;
    Ok(open_database(DbConfig::on_disk(profile, db))?)
}

fn create_cli_database(args: &[String]) -> Result<multidb::db::Database, CliError> {
    let db = PathBuf::from(required_value(args, "--db")?);
    let profile = parse_profile(&required_value(args, "--profile")?)?;
    Ok(create_database(DbConfig::on_disk(profile, db))?)
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

fn parse_profile(value: &str) -> Result<Profile, CliError> {
    match value {
        "InMemory" | "in-memory" | "memory" => Ok(Profile::InMemory),
        "Transactional" | "transactional" => Ok(Profile::Transactional),
        "Analytical" | "analytical" => Ok(Profile::Analytical),
        "Document" | "document" => Ok(Profile::Document),
        "Vector" | "vector" => Ok(Profile::Vector),
        "TimeSeries" | "time-series" | "time_series" => Ok(Profile::TimeSeries),
        "HighDurability" | "high-durability" => Ok(Profile::HighDurability),
        "Balanced" | "balanced" => Ok(Profile::Balanced),
        _ => Err(CliError::Usage(format!("unknown profile {value}"))),
    }
}

fn usage() -> CliError {
    CliError::Usage(
        "multidb serve\n\
         multidb backup full --db <path> --profile <profile> --dest <dir>\n\
         multidb init --guided --profile <profile> --name <name> [--out <path>] [--format yaml|json] [--force] [--json]\n\
         multidb init --guided --template <template> --name <name> [--out <dir>] [--format yaml|json] [--force] [--json]\n\
         multidb template list [--json]\n\
         multidb template explain <template> [--name <name>] [--output text|json] [--json]\n\
         multidb profile list [--json]\n\
         multidb role list [--json]\n\
         multidb domain list [--json]\n\
         multidb config validate --spec <json|yaml> [--output text|json] [--json]\n\
         multidb config explain --spec <json|yaml> [--output text|json] [--json]\n\
         multidb explain config --spec <json|yaml> [--output text|json] [--json]\n\
         multidb config plan --current <json|yaml> --desired <json|yaml> [--out <plan.json>] [--output text|json] [--json]\n\
         multidb config apply --plan <json> --confirm <plan_id> [--output text|json]\n\
         multidb advice list --db <path> --profile <profile> [--output text|json] [--json]\n\
         multidb advice plan --db <path> --profile <profile> --advice-id <id> [--out <plan.json>] [--output text|json] [--json]\n\
         multidb advice reject --db <path> --profile <profile> --advice-id <id> --reason <reason> [--output text|json] [--json]\n\
         multidb backup incr --db <path> --profile <profile> --dest <dir> --parent <backup-dir>\n\
         multidb restore --backup <backup-dir> --dest-db <path> --profile <profile> [--to-lsn <lsn>]\n\
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
         multidb migrate value-codec --db <path> --profile <profile>"
            .to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{CliError, ServePgTls, run, serve_config_from_sources};
    use multidb::{
        config_spec::{
            CollectionRole, CollectionRoleSpec, ConsistencyMode, DatabaseSpec, MigrationPlanner,
            WriteAck,
        },
        db::{DbConfig, Profile, create_database},
        tuning::WorkloadSample,
    };
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
        std::fs::write(path, serde_yaml::to_string(spec)?)?;
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
        assert_eq!(config.profile, Profile::Balanced);
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
        let spec = serde_yaml::from_str::<DatabaseSpec>(&std::fs::read_to_string(&spec_path)?)?;
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
        assert_eq!(report["explain"]["validation"]["valid"], true);
        assert!(outcome.message.contains("vector_hnsw"));
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
        let spec = serde_yaml::from_str::<DatabaseSpec>(&std::fs::read_to_string(&spec_path)?)?;
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
}
