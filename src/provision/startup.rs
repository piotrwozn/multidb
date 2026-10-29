//! Starting from a spec: `multidb serve` in spec mode and embedded apps.
//!
//! [`open_from_spec`] probes the target, opens an existing database with
//! `open_database_with_ops` (which loads its persisted RBAC state) or plans against the absent
//! target, and asks [`serve_startup_decision`] what the [`StartupMode`] allows. The decision is
//! a pure function of the plan and the mode:
//!
//! | target | `verify` | `create` | `additive` |
//! | --- | --- | --- | --- |
//! | absent, plan auto-approvable | refuse | apply | apply |
//! | existing, plan a noop (drift, if any, is information only) | start | start | start |
//! | existing, plan auto-approvable | refuse | refuse | apply |
//! | interrupted apply, remaining plan auto-approvable | refuse | refuse | apply |
//! | anything else: retune, weakening, destructive, format upgrade, RBAC changes on an existing database, missing flags, blocked | refuse | refuse | refuse |
//!
//! A start applies only auto-approvable plans, with `Confirmation::AutoApproveAdditive`, as the
//! local operator of the surface (`local:serve` or `local:embedded`). Everything else needs a
//! reviewed plan and a confirmed apply, which a refusal points to.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::execute::{local_operator_label, plan_warnings};
use super::plan::safety_slug;
use super::{
    Confirmation, DriftKind, PlanOutcome, ProvisionActor, ProvisionApplyReport,
    ProvisionEnvironment, ProvisionError, ProvisionOptions, ProvisionOutcome, ProvisionPlan,
    ProvisionStepKind, ProvisionSurface, ProvisionTargetRef, StepSafety, TargetKind,
    apply_provision, issue_summary, plan_provision, provision_new_database,
};
use crate::config_spec::{DatabaseSpec, ValidationIssue, resolve_profile};
use crate::db::{
    ConfigError, Database, DbConfig, MetadataProbe, OperationalConfig, ReplicationKind,
    inspect_database_metadata, open_database_with_ops, recover_database_file,
};

/// What a start may change. The default, `verify`, never changes anything.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartupMode {
    /// Starts only when the database exists and already matches the spec.
    #[default]
    Verify,
    /// Also creates a missing database from an auto-approvable plan; an existing database must
    /// match the spec.
    Create,
    /// Also applies an auto-approvable plan to an existing database, including the remaining
    /// steps of an interrupted apply.
    Additive,
}

impl StartupMode {
    /// `verify`, `create` or `additive`: the values of `MULTIDB_SPEC_APPLY`.
    #[must_use]
    pub const fn slug(self) -> &'static str {
        match self {
            Self::Verify => "verify",
            Self::Create => "create",
            Self::Additive => "additive",
        }
    }

    /// The mode a slug names, ignoring ASCII case and surrounding whitespace.
    #[must_use]
    pub fn from_slug(value: &str) -> Option<Self> {
        [Self::Verify, Self::Create, Self::Additive]
            .into_iter()
            .find(|mode| value.trim().eq_ignore_ascii_case(mode.slug()))
    }
}

/// What a start does.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartupDecision {
    /// The database matches the spec: start without changing anything.
    Start,
    /// Apply the auto-approvable plan as the local operator of the surface, then start.
    ApplyThenStart,
    /// Do not start, for this reason.
    Refuse(String),
}

/// How a start from a spec went. Its decision is `Start` or `ApplyThenStart`: a refusal is
/// `ProvisionError::StartupRefused`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartupReport {
    pub mode: StartupMode,
    pub decision: StartupDecision,
    /// The plan the decision was made on; for an apply, the plan id the apply confirmed (for a
    /// new database, the id of the plan against the absent target).
    pub plan_id: String,
    /// The report of the apply of an `ApplyThenStart`.
    pub applied: Option<ProvisionApplyReport>,
    /// The Warnings of the plan, for example `DRIFT_UNMANAGED` or `AUDIT_DEVELOPMENT_KEY`.
    pub warnings: Vec<ValidationIssue>,
}

/// Why a start from a spec was refused (`ProvisionError::StartupRefused`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartupRefusal {
    pub mode: StartupMode,
    pub reason: String,
    /// The plan the decision was made on.
    pub plan: ProvisionPlan,
    /// The report of a startup apply that failed part-way: what it applied stays applied, and
    /// the provision record says `failed` when it could be written. `None` when nothing was
    /// applied.
    pub applied: Option<ProvisionApplyReport>,
}

/// Decides what a start in `mode` does with `plan`, the plan of the spec against the database
/// to serve (on the serve or Rust API surface): start, apply the plan and start, or refuse.
///
/// The target state comes from the plan itself: `target.kind` tells an absent database from an
/// existing one, and an `interrupted_apply` drift item an apply that did not finish. Only an
/// `auto_approvable` plan is ever applied, and only an existing database whose plan is a noop
/// starts unchanged; drift that is information only (unmanaged objects under `report` or
/// `ignore`, `operator_managed` or `enforced_with_development_key` guarantees) leaves the plan a
/// noop. The refusal reasons name the mode or the command that resolves them.
#[must_use]
pub fn serve_startup_decision(plan: &ProvisionPlan, mode: StartupMode) -> StartupDecision {
    match plan.target.kind {
        TargetKind::NewDatabase if !plan.auto_approvable => {
            StartupDecision::Refuse(not_auto_approvable(plan))
        }
        TargetKind::NewDatabase => match mode {
            StartupMode::Verify => StartupDecision::Refuse(missing_database(plan)),
            StartupMode::Create | StartupMode::Additive => StartupDecision::ApplyThenStart,
        },
        TargetKind::ExistingDatabase if plan.outcome == PlanOutcome::Noop => StartupDecision::Start,
        TargetKind::ExistingDatabase if !plan.auto_approvable => {
            StartupDecision::Refuse(not_auto_approvable(plan))
        }
        TargetKind::ExistingDatabase => match mode {
            StartupMode::Additive => StartupDecision::ApplyThenStart,
            StartupMode::Verify | StartupMode::Create => {
                StartupDecision::Refuse(pending_changes(plan, mode))
            }
        },
    }
}

/// Opens the database of `spec` for a start, as `multidb serve` in spec mode and embedded apps
/// do, on the serve or Rust API surface.
///
/// The target is `path`, else `deployment.storage_path` (relative to the working directory),
/// else a new in-memory database (Rust API only). A file that was not closed cleanly first gets
/// redb's crash recovery, as with every opener. An existing single-node CP database is opened
/// with `open_database_with_ops`, which loads its persisted RBAC state, with
/// `ProvisionEnvironment.encryption`; an encrypted database, whose metadata the
/// probe cannot read without the key, is opened with that config and the storage profile of the
/// spec, which the open checks against the decrypted metadata. `ProvisionEnvironment.audit`
/// supplies the opener's runtime key and anchor, and the stored signed prefix is verified
/// before planning or new audit events. The plan of `spec` against the database, or against
/// the absent target, goes to [`serve_startup_decision`]: `Start` returns the database as it
/// is, and `ApplyThenStart` applies the plan with `Confirmation::AutoApproveAdditive` as the
/// local operator of the surface (`local:serve` or `local:embedded`), creating a missing
/// database with [`provision_new_database`].
/// # Errors
/// Fails with `StartupRefused` when the decision refuses, or when the startup apply fails
/// part-way (the database is closed again, and the next start sees the interrupted apply);
/// `Config(Unsupported)` on the CLI and Control Plane surfaces;
/// `Config(ReplicationMismatch)` for a database that is not single-node CP; `Locked`
/// when another handle holds the database; `Io` when it cannot be probed; `Db` when the audit
/// key cannot be read; and as [`plan_provision`], [`apply_provision`] and
/// [`provision_new_database`].
pub fn open_from_spec(
    spec: &DatabaseSpec,
    path: Option<&Path>,
    mode: StartupMode,
    opts: &ProvisionOptions,
    env: &ProvisionEnvironment,
) -> Result<(Database, StartupReport), ProvisionError> {
    let label = match env.surface {
        ProvisionSurface::RustApi | ProvisionSurface::Serve => local_operator_label(env.surface),
        ProvisionSurface::Cli | ProvisionSurface::ControlPlane => None,
    }
    .ok_or_else(|| {
        unsupported(
            "open_from_spec runs on the rust_api and serve surfaces; the CLI provisions with \
             config plan|apply --spec, and the Control Plane provisions the database it serves",
        )
    })?;
    if let Some(audit) = &env.audit {
        // Preserve the audit error contract and reject invalid credentials even before
        // metadata probing/crash recovery. The operational opener also rechecks the key.
        audit.validate_key()?;
    }
    let path = path
        .map(Path::to_path_buf)
        .or_else(|| spec.deployment.storage_path.as_deref().map(PathBuf::from));
    let existing = match &path {
        Some(path) => open_existing(spec, path, env)?,
        None => None,
    };
    let plan = match (&existing, &path) {
        (Some(db), _) => plan_provision(spec, ProvisionTargetRef::Open(db), opts, env)?,
        (None, Some(path)) => plan_provision(spec, ProvisionTargetRef::Path(path), opts, env)?,
        (None, None) => plan_provision(spec, ProvisionTargetRef::Volatile, opts, env)?,
    };
    let actor = ProvisionActor::local_operator(label);
    match (serve_startup_decision(&plan, mode), existing) {
        (StartupDecision::Refuse(reason), _) => Err(refused(mode, reason, plan, None)),
        (StartupDecision::Start, Some(db)) => {
            let report = StartupReport {
                mode,
                decision: StartupDecision::Start,
                plan_id: plan.plan_id.clone(),
                applied: None,
                warnings: plan_warnings(&plan),
            };
            Ok((db, report))
        }
        (StartupDecision::ApplyThenStart, Some(mut db)) => {
            let applied = apply_provision(
                &mut db,
                &actor,
                spec,
                &Confirmation::AutoApproveAdditive,
                opts,
                env,
            )?;
            started(db, mode, plan, applied)
        }
        (StartupDecision::ApplyThenStart, None) => {
            let (db, applied) = provision_new_database(
                spec,
                path.as_deref(),
                &Confirmation::AutoApproveAdditive,
                opts,
                env,
            )?;
            started(db, mode, plan, applied)
        }
        // The decision never starts an absent database; refuse rather than build one.
        (StartupDecision::Start, None) => Err(refused(
            mode,
            "nothing to start: the database does not exist".to_owned(),
            plan,
            None,
        )),
    }
}

/// Probes `path` and opens an initialized single-node CP database; `None` when nothing exists
/// at the path yet or the file is empty. The probe reads `__meta` without a key, so the
/// metadata of an encrypted database is unreadable to it: with an encryption config, such a
/// file is opened with that config and the storage profile of the spec, and the open checks
/// both against the metadata it then decrypts (a wrong key or another storage profile or
/// replication fails it).
fn open_existing(
    spec: &DatabaseSpec,
    path: &Path,
    env: &ProvisionEnvironment,
) -> Result<Option<Database>, ProvisionError> {
    let probe = match inspect_database_metadata(path) {
        MetadataProbe::NeedsRecovery => {
            recover_database_file(path)?;
            inspect_database_metadata(path)
        }
        probe => probe,
    };
    let metadata = match probe {
        MetadataProbe::NotFound | MetadataProbe::Uninitialized => return Ok(None),
        MetadataProbe::Initialized(metadata) => metadata,
        MetadataProbe::Locked => {
            return Err(ProvisionError::Locked(format!(
                "{} is held open by another handle",
                path.display()
            )));
        }
        MetadataProbe::NeedsRecovery => {
            return Err(ProvisionError::Io(format!(
                "{} still needs crash recovery after recovering it",
                path.display()
            )));
        }
        MetadataProbe::Unreadable { reason } => {
            let storage_profile = resolve_profile(spec)
                .resolved()
                .map(|resolved| resolved.storage_profile);
            let regular_file = std::fs::metadata(path).is_ok_and(|metadata| metadata.is_file());
            return match (&env.encryption, storage_profile) {
                (Some(_), Some(storage_profile)) if regular_file => {
                    open_with_environment(DbConfig::on_disk(storage_profile, path), env).map(Some)
                }
                _ => Err(ProvisionError::Io(format!(
                    "{} cannot be probed: {reason}",
                    path.display()
                ))),
            };
        }
    };
    if metadata.replication != ReplicationKind::Cp {
        return Err(ConfigError::ReplicationMismatch {
            expected: ReplicationKind::Cp,
            found: metadata.replication,
        }
        .into());
    }
    open_with_environment(DbConfig::on_disk(metadata.storage_profile, path), env).map(Some)
}

/// Opens an existing database with `open_database_with_ops` and the environment's
/// encryption/audit configuration, verifying the signed prefix before returning it.
fn open_with_environment(
    config: DbConfig,
    env: &ProvisionEnvironment,
) -> Result<Database, ProvisionError> {
    let mut operational = OperationalConfig::new();
    if let Some(encryption) = &env.encryption {
        operational = operational.with_encryption(encryption.clone());
    }
    if let Some(audit) = &env.audit {
        operational.security.audit.clone_from(audit);
    }
    Ok(open_database_with_ops(config, &operational)?)
}

/// The database after a startup apply, or the refusal of an apply that failed part-way.
fn started(
    db: Database,
    mode: StartupMode,
    plan: ProvisionPlan,
    applied: ProvisionApplyReport,
) -> Result<(Database, StartupReport), ProvisionError> {
    if matches!(
        applied.status,
        ProvisionOutcome::Applied | ProvisionOutcome::Noop
    ) {
        let report = StartupReport {
            mode,
            decision: StartupDecision::ApplyThenStart,
            plan_id: applied.plan_id.clone(),
            warnings: applied.warnings.clone(),
            applied: Some(applied),
        };
        return Ok((db, report));
    }
    drop(db);
    let stopped = applied.failed_step.as_deref().map_or_else(
        || "it stopped between steps".to_owned(),
        |step| format!("step {step} failed"),
    );
    let reason = format!(
        "the startup apply of plan {} did not finish: {stopped} ({}); what it applied stays \
         applied: review what remains with multidb config plan --spec, and apply it (a start in \
         additive mode applies it when it is all additive)",
        applied.plan_id, applied.check.message
    );
    Err(refused(mode, reason, plan, Some(applied)))
}

fn refused(
    mode: StartupMode,
    reason: String,
    plan: ProvisionPlan,
    applied: Option<ProvisionApplyReport>,
) -> ProvisionError {
    ProvisionError::StartupRefused(Box::new(StartupRefusal {
        mode,
        reason,
        plan,
        applied,
    }))
}

fn unsupported(message: &str) -> ProvisionError {
    ProvisionError::Config(ConfigError::Unsupported(message.to_owned()))
}

/// The last apply did not finish, and this plan holds what remains of it.
fn interrupted(plan: &ProvisionPlan) -> bool {
    plan.drift
        .items
        .iter()
        .any(|item| item.kind == DriftKind::InterruptedApply)
}

fn missing_database(plan: &ProvisionPlan) -> String {
    plan.target.path.as_deref().map_or_else(
        || {
            "an in-memory database is new at every open, and the verify startup mode never \
             creates a database; open it with StartupMode::Create"
                .to_owned()
        },
        |path| {
            format!(
                "database missing at {path}: the verify startup mode never creates a database; \
                 set MULTIDB_SPEC_APPLY=create (StartupMode::Create), or create it with multidb \
                 config apply --spec or multidb init --apply"
            )
        },
    )
}

/// An existing database with an auto-approvable plan, in a mode that does not apply it.
fn pending_changes(plan: &ProvisionPlan, mode: StartupMode) -> String {
    let steps = plan.steps.len();
    if interrupted(plan) {
        format!(
            "the last provisioning apply did not finish (DRIFT_INTERRUPTED_APPLY), and the {} \
             startup mode never resumes it: set MULTIDB_SPEC_APPLY=additive \
             (StartupMode::Additive) to apply its {steps} remaining steps, which are all \
             additive, or apply them with multidb config apply --spec",
            mode.slug()
        )
    } else {
        format!(
            "the database differs from the spec by {steps} additive steps, and the {} startup \
             mode never changes an existing database: set MULTIDB_SPEC_APPLY=additive \
             (StartupMode::Additive), or apply them with multidb config apply --spec",
            mode.slug()
        )
    }
}

/// A plan a start never applies: blocked, missing flags or not auto-approvable.
fn not_auto_approvable(plan: &ProvisionPlan) -> String {
    let subject = match plan.target.kind {
        TargetKind::NewDatabase => "the plan that creates the missing database",
        TargetKind::ExistingDatabase => "the plan for the database",
    };
    match plan.outcome {
        PlanOutcome::Blocked if !plan.validation.valid => format!(
            "the spec is invalid: {}",
            issue_summary(&plan.validation.issues)
        ),
        PlanOutcome::Blocked => format!(
            "{subject} is blocked: {}; resolve the errors, then review it with multidb config \
             plan --spec",
            issue_summary(&plan.issues)
        ),
        PlanOutcome::NeedsFlags => format!(
            "{subject} needs flags a start never gives ({}); review it, then apply it with \
             multidb config apply --spec and those flags",
            plan.required_flags.join(", ")
        ),
        PlanOutcome::Ready | PlanOutcome::Noop => format!(
            "{subject} is not auto-approvable: it {}; a start applies only plans that create, \
             adopt, record or strengthen, without RBAC changes on an existing database, so \
             review it and confirm it with multidb config apply --spec --confirm <prov_id>",
            unapprovable_steps(plan)
        ),
    }
}

/// What keeps a ready plan from being auto-approvable, for example `has retune steps`.
fn unapprovable_steps(plan: &ProvisionPlan) -> String {
    let existing = plan.target.kind == TargetKind::ExistingDatabase;
    let mut reasons = Vec::<String>::new();
    for step in &plan.steps {
        let reason = match step.safety {
            StepSafety::Additive | StepSafety::Metadata | StepSafety::Strengthening => {
                if existing && step.kind == ProvisionStepKind::ApplySecurity {
                    "changes RBAC on an existing database".to_owned()
                } else {
                    continue;
                }
            }
            other => format!("has {} steps", safety_slug(other)),
        };
        if !reasons.contains(&reason) {
            reasons.push(reason);
        }
    }
    if reasons.is_empty() {
        "needs a confirmation".to_owned()
    } else {
        reasons.join(" and ")
    }
}
