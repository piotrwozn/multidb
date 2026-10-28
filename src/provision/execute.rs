//! Applying provisioning plans.
//!
//! [`apply_provision`] recomputes the plan of a spec against an open database under exclusive
//! `&mut Database`, checks the confirmation against the recomputed `prov_` id and runs the steps
//! in plan order. Audit comes first: an apply needs audit on (or turns it on as its first step),
//! audits its intent before anything else changes, journals its progress in the provision record
//! after every step and audits its completion or failure. A failed apply leaves a `failed` record
//! whose completed steps the next plan skips, so applying again converges. Nothing unmanaged is
//! ever dropped, altered or overwritten: objects are only created or adopted, RBAC changes touch
//! managed names only, and releasing an object only removes it from the record.
//!
//! [`provision_new_database`] creates a database from a spec at an absent target: it plans
//! against the absent target, checks the confirmation, creates the database with its profile
//! manifest, recomputes the plan against the new database and applies it under the confirmed
//! plan id.

#[cfg(test)]
use std::cell::Cell;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::physical::{self, DesiredKind, DesiredObject, entry_kind, time_series_config};
use super::plan::{
    DriftKind, PlanOutcome, ProvisionEnvironment, ProvisionOptions, ProvisionPlan, ProvisionStep,
    ProvisionStepKind, ProvisionSurface, ProvisionTargetRef, StepSafety, desired_manifest,
    expected_session_performance, object_resource, plan_provision, security_post_state,
};
use super::record::{
    ManagedObject, ObjectKind, ProvisionHistoryEntry, ProvisionRecord, ProvisionRecordStatus,
    RECORD_VERSION,
};
use super::{ProvisionError, issue};
use crate::config_spec::{
    ApplyCheckReport, ApplyStatus, DatabaseSpec, ResolvedProfile, ValidationIssue,
    ValidationSeverity, spec_digest,
};
use crate::db::{
    CatalogEntry, ConfigError, Database, DbError, OperationalConfig, PersistedProfileManifest,
    ReplicationKind, SCHEMA_VERSION, SCHEMA_VERSION_FRAMED, SecurityConfig,
    create_database_with_profile,
};
use crate::geo::GeoIndexConfig;
use crate::model::CollectionId;
use crate::security::{AuditOutcome, Principal, Resource};
use crate::temporal::TemporalRetention;
use crate::text::FullTextIndexConfig;
use crate::txn::TxnId;
use crate::vector::VectorCollectionConfig;

/// The failures and perturbations tests inject into the applies of one thread.
#[cfg(test)]
#[derive(Clone, Copy)]
struct TestInjections {
    /// The 0-based index of the plan step that fails before it changes anything.
    fail_step: Option<usize>,
    /// The 0-based index of the plan step that fails after its write committed.
    fail_after_write: Option<usize>,
    /// The provisioning audit event (`provision_apply_started`, ...) whose write fails.
    fail_event: Option<&'static str>,
    /// Runs on the database `provision_new_database` created, before it recomputes the plan.
    new_database_hook: Option<fn(&mut Database)>,
}

#[cfg(test)]
thread_local! {
    static INJECTIONS: Cell<TestInjections> = const {
        Cell::new(TestInjections {
            fail_step: None,
            fail_after_write: None,
            fail_event: None,
            new_database_hook: None,
        })
    };
}

#[cfg(test)]
fn injections() -> TestInjections {
    INJECTIONS.with(Cell::get)
}

#[cfg(test)]
fn inject(change: impl FnOnce(&mut TestInjections)) {
    INJECTIONS.with(|cell| {
        let mut injections = cell.get();
        change(&mut injections);
        cell.set(injections);
    });
}

/// Makes the apply on this thread fail at the plan step with this 0-based index
/// (`pstep-001` is 0) before the step changes anything; `None` turns the injection off.
#[cfg(test)]
pub(crate) fn fail_provision_step_for_tests(step: Option<usize>) {
    inject(|injections| injections.fail_step = step);
}

/// Makes the apply on this thread fail at the plan step with this 0-based index after the step
/// wrote everything it writes, as when the audit event of a committed create fails; `None`
/// turns the injection off.
#[cfg(test)]
pub(crate) fn fail_provision_step_after_write_for_tests(step: Option<usize>) {
    inject(|injections| injections.fail_after_write = step);
}

/// Makes the provisioning audit event `action` (for example `provision_apply_completed`) fail
/// to be written by the apply on this thread; `None` turns the injection off.
#[cfg(test)]
pub(crate) fn fail_provision_event_for_tests(action: Option<&'static str>) {
    inject(|injections| injections.fail_event = action);
}

/// Runs `hook` on the database `provision_new_database` just created on this thread, before it
/// recomputes the plan; `None` turns it off.
#[cfg(test)]
pub(crate) fn perturb_new_database_for_tests(hook: Option<fn(&mut Database)>) {
    inject(|injections| injections.new_database_hook = hook);
}

/// Carries the test injections set on this thread into `work`, for an apply that runs on
/// another thread, such as the blocking pool of the Control Plane. `work` restores the
/// injections of the thread it runs on when it returns or unwinds.
#[cfg(test)]
pub(crate) fn carry_test_injections<T>(
    work: impl FnOnce() -> T + Send,
) -> impl FnOnce() -> T + Send {
    struct Restore(TestInjections);
    impl Drop for Restore {
        fn drop(&mut self) {
            INJECTIONS.with(|cell| cell.set(self.0));
        }
    }
    let carried = injections();
    move || {
        let _restore = Restore(INJECTIONS.with(|cell| cell.replace(carried)));
        work()
    }
}

/// Outside tests there are no injections to carry, so `work` is returned unchanged.
#[cfg(not(test))]
pub(crate) fn carry_test_injections<T>(
    work: impl FnOnce() -> T + Send,
) -> impl FnOnce() -> T + Send {
    work
}

/// How an apply is confirmed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Confirmation {
    /// The `prov_` id of the reviewed plan. It must equal the id of the plan recomputed at
    /// apply time; another `prov_` id is `PlanChanged`, and anything else (a `plan_` id of
    /// `config plan`, for example) is rejected.
    PlanId(String),
    /// Applies a plan without an id when it is `auto_approvable`: it only creates, adopts,
    /// records or strengthens, without RBAC changes on an existing database.
    AutoApproveAdditive,
}

/// Who applies a plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProvisionActor {
    /// An RBAC principal, such as a Control Plane caller. It needs `System:Admin` and
    /// `Database:Admin`, and the `*_as` APIs audit every object it creates.
    Principal(Principal),
    /// A local operator that relies on filesystem trust, like `backup` and `restore`:
    /// `local:cli`, `local:serve` or `local:embedded`. Its audit events carry the label as the
    /// principal name.
    LocalOperator { label: String },
}

impl ProvisionActor {
    /// A local operator with `label`, for example `local:cli`.
    #[must_use]
    pub fn local_operator(label: impl Into<String>) -> Self {
        Self::LocalOperator {
            label: label.into(),
        }
    }

    /// The principal name or the local operator label.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Principal(principal) => principal.name(),
            Self::LocalOperator { label } => label,
        }
    }

    /// The principal audit events of this actor carry.
    pub(crate) fn audit_principal(&self) -> Principal {
        match self {
            Self::Principal(principal) => principal.clone(),
            Self::LocalOperator { label } => Principal::new(label.clone()),
        }
    }

    const fn principal(&self) -> Option<&Principal> {
        match self {
            Self::Principal(principal) => Some(principal),
            Self::LocalOperator { .. } => None,
        }
    }
}

/// How an apply ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProvisionOutcome {
    /// Every step ran.
    Applied,
    /// The database already matched the spec; nothing changed and nothing was audited.
    Noop,
    /// The confirmation was not a provisioning plan id; nothing changed.
    Rejected,
    /// The plan is blocked; nothing changed. `apply_provision` reports it as
    /// `ProvisionError::Blocked`; the status is for surfaces that answer with a report.
    Blocked,
    /// The confirmed plan is not the current one. `apply_provision` reports it as
    /// `ProvisionError::PlanChanged`, since nothing changed; `provision_new_database` reports it
    /// when the plan it recomputed against the database it created differs, and then the new
    /// database is the only change (`data_mutated` is true).
    PlanChanged,
    /// A step failed, or the apply stopped between steps (`failed_step` is then `None`): what
    /// was applied stays applied, including what a failed step wrote before it failed, the
    /// record says `failed`, and the next plan holds what remains.
    PartiallyApplied,
}

/// How one step ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepOutcome {
    Applied,
    /// Nothing was left to do, for example a second `apply_security` step after the first one
    /// committed every RBAC change.
    AlreadySatisfied,
    Failed,
}

/// One step of an apply.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppliedStep {
    pub step_id: String,
    pub kind: ProvisionStepKind,
    pub object: Option<String>,
    pub outcome: StepOutcome,
    pub detail: String,
    /// For a failed step, the kind of error, for example `catalog_object_exists`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<String>,
}

/// The provision record an apply left, in short.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProvisionRecordSummary {
    pub status: ProvisionRecordStatus,
    pub plan_id: String,
    pub spec_digest: String,
    /// Managed objects.
    pub objects: usize,
    pub managed_roles: usize,
    pub managed_principals: usize,
    pub released_objects: usize,
    pub completed_steps: usize,
    pub failed_step: Option<String>,
    pub applied_at_millis: u64,
    pub applied_by: String,
}

impl ProvisionRecordSummary {
    fn of(record: &ProvisionRecord) -> Self {
        Self {
            status: record.status,
            plan_id: record.plan_id.clone(),
            spec_digest: record.spec_digest.clone(),
            objects: record.objects.len(),
            managed_roles: record.managed_roles.len(),
            managed_principals: record.managed_principals.len(),
            released_objects: record.released_objects.len(),
            completed_steps: record.completed_steps.len(),
            failed_step: record.failed_step.clone(),
            applied_at_millis: record.applied_at_millis,
            applied_by: record.applied_by.clone(),
        }
    }
}

/// What an apply did.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProvisionApplyReport {
    pub status: ProvisionOutcome,
    /// The confirmation check; `data_mutated` is true only for `applied`, `partially_applied`
    /// and the `plan_changed` report of a database `provision_new_database` created.
    pub check: ApplyCheckReport,
    /// The confirmed plan id; for a new database, the id of the plan against the absent target.
    pub plan_id: String,
    pub applied_steps: Vec<AppliedStep>,
    pub failed_step: Option<String>,
    pub record: Option<ProvisionRecordSummary>,
    /// The Warnings of the plan, and problems recording a failure.
    pub warnings: Vec<ValidationIssue>,
    /// A retune or storage format upgrade was written, or the session already ran knobs other
    /// than the profile manifest's: they take effect at the next open.
    pub session_pending_restart: bool,
}

/// Applies `spec` to the open single-node database `db` as `actor`.
///
/// The plan is recomputed from the live database, so a client plan is never trusted:
/// `confirmation` must name the recomputed `prov_` id, or be `AutoApproveAdditive` for an
/// `auto_approvable` plan. A noop returns without writing anything, an audit event included. A
/// [`ProvisionActor::Principal`] needs `System:Admin` and `Database:Admin`, and the plan is
/// recomputed with it as `ProvisionEnvironment.principal`, so `RBAC_SELF_LOCKOUT` covers the
/// actor that applies.
///
/// Once anything changed the database, the apply answers with a report, never an error: a step
/// failure is `partially_applied` with a `failed` record, and applying again runs only what
/// remains; a step that failed after it wrote says so in its `detail`, and an object its create
/// committed is managed by the record. A completion event that cannot be written leaves the
/// apply `applied` with the Warning `PROVISION_COMPLETION_NOT_AUDITED` and `audit_recorded`
/// false.
/// # Errors
/// Fails with `PlanChanged` when the confirmed `prov_` id is not the current one, `Blocked`,
/// `AuditRequired` (audit is off and the spec keeps it off), `NeedsFlags`,
/// `ConfirmationRequired` (auto-approval of a plan that is not `auto_approvable`),
/// `PermissionMissing`, and as [`plan_provision`]; and with `Db` when turning audit on before
/// the first step fails without changing anything, or the intent event or the first journal
/// write fails before any step changed the database.
pub fn apply_provision(
    db: &mut Database,
    actor: &ProvisionActor,
    spec: &DatabaseSpec,
    confirmation: &Confirmation,
    opts: &ProvisionOptions,
    env: &ProvisionEnvironment,
) -> Result<ProvisionApplyReport, ProvisionError> {
    let env = actor_environment(actor, env);
    let plan = plan_provision(spec, ProvisionTargetRef::Open(&*db), opts, &env)?;
    if let Some(rejected) = check_confirmation(&plan, confirmation)? {
        return Ok(rejected);
    }
    if plan.outcome == PlanOutcome::Noop {
        return noop_report(db, &plan, confirmation);
    }
    gate(&plan, confirmation)?;
    if let ProvisionActor::Principal(principal) = actor {
        db.authorize_provision_actor(principal)
            .map_err(|error| match error {
                DbError::AuthzDenied(denied) => ProvisionError::PermissionMissing(format!(
                    "{denied}; applying a provisioning plan needs System:Admin and \
                     Database:Admin"
                )),
                other => ProvisionError::Db(other),
            })?;
    }
    execute(db, actor, spec, &plan, opts, Vec::new())
}

/// Creates a database from `spec` at an absent target and applies the spec to it.
///
/// The target is `path`, else `deployment.storage_path` (relative to the working directory),
/// else a new in-memory database (Rust API only). The plan is computed against the absent
/// target and confirmed as in [`apply_provision`]; the database is then created with
/// `create_database_with_profile` (audit on, `ProvisionEnvironment.audit` as its audit config,
/// `ProvisionEnvironment.encryption`, the profile's performance), the plan is
/// recomputed against it and its steps must equal the confirmed ones minus `create_database`,
/// and the steps run as the local operator of the surface (`local:cli`, `local:serve` or
/// `local:embedded`). The record and the audit events carry the confirmed plan id.
///
/// Once the database is created, every outcome is a report with `data_mutated` true and the
/// applied `create_database` step: when the recomputed plan differs the report is
/// `plan_changed`, and when it cannot be recomputed or the apply stops before its first step it
/// is `partially_applied`; the new database is kept but nothing else is applied: plan it through
/// its open handle.
/// # Errors
/// Fails with `AlreadyInitialized` when the target holds a database, `Config(Unsupported)` on
/// the Control Plane surface (which never creates databases), `ConfirmationRequired` when the
/// confirmation is not a provisioning plan id or the plan is not `auto_approvable`, `PlanChanged`
/// when the confirmed id is not the current one, `Config` when the database cannot be created,
/// and as [`plan_provision`] before the database is created.
pub fn provision_new_database(
    spec: &DatabaseSpec,
    path: Option<&Path>,
    confirmation: &Confirmation,
    opts: &ProvisionOptions,
    env: &ProvisionEnvironment,
) -> Result<(Database, ProvisionApplyReport), ProvisionError> {
    let Some(label) = local_operator_label(env.surface) else {
        return Err(ProvisionError::Config(ConfigError::Unsupported(
            "the Control Plane never creates databases; it provisions the database it serves"
                .to_owned(),
        )));
    };
    let path = path
        .map(Path::to_path_buf)
        .or_else(|| spec.deployment.storage_path.as_deref().map(PathBuf::from));
    let target = path
        .as_deref()
        .map_or(ProvisionTargetRef::Volatile, ProvisionTargetRef::Path);
    let confirmed = plan_provision(spec, target, opts, env)?;
    if check_confirmation(&confirmed, confirmation)?.is_some() {
        return Err(ProvisionError::ConfirmationRequired(Box::new(confirmed)));
    }
    gate(&confirmed, confirmation)?;
    let resolved = resolved_profile(&confirmed)?;
    let binding = resolved.bind(ReplicationKind::Cp, path)?;
    let mut operational = OperationalConfig::new().with_security(SecurityConfig {
        audit: env.audit.clone().unwrap_or_default(),
        ..SecurityConfig::default()
    });
    if let Some(encryption) = &env.encryption {
        operational = operational.with_encryption(encryption.clone());
    }
    let mut db =
        create_database_with_profile(&binding, operational).map_err(|error| match error {
            ConfigError::AlreadyInitialized => ProvisionError::AlreadyInitialized,
            other => ProvisionError::Config(other),
        })?;
    #[cfg(test)]
    if let Some(hook) = injections().new_database_hook {
        hook(&mut db);
    }

    // The database exists from here on, so every outcome is a report that says so.
    let created: Vec<AppliedStep> = confirmed
        .steps
        .iter()
        .filter(|step| step.kind == ProvisionStepKind::CreateDatabase)
        .map(|step| AppliedStep {
            step_id: step.step_id.clone(),
            kind: step.kind,
            object: step.object.clone(),
            outcome: StepOutcome::Applied,
            detail: step.action.clone(),
            error_kind: None,
        })
        .collect();
    let recomputed = match plan_provision(spec, ProvisionTargetRef::Open(&db), opts, env) {
        Ok(recomputed) => recomputed,
        Err(error) => {
            let message = format!(
                "the database was created, but its plan could not be recomputed: {error}; \
                 nothing else was applied: plan the new database to see what remains"
            );
            let report = created_report(
                &confirmed,
                created,
                ProvisionOutcome::PartiallyApplied,
                message,
            );
            return Ok((db, report));
        }
    };
    if !continuation_matches(&confirmed, &recomputed) {
        let message = format!(
            "the database was created, but the plan recomputed against it ({}) is not the \
             confirmed plan ({}); nothing else was applied: plan the new database and confirm \
             the new plan id",
            recomputed.plan_id, confirmed.plan_id
        );
        let report = created_report(&confirmed, created, ProvisionOutcome::PlanChanged, message);
        return Ok((db, report));
    }
    let actor = ProvisionActor::local_operator(label);
    let report = match execute(&mut db, &actor, spec, &confirmed, opts, created.clone()) {
        Ok(report) => report,
        Err(error) => created_report(
            &confirmed,
            created,
            ProvisionOutcome::PartiallyApplied,
            format!(
                "the database was created, but the apply stopped before its next step: {error}; \
                 plan the new database again to see what remains"
            ),
        ),
    };
    Ok((db, report))
}

/// The report of a new database that was created, with nothing else applied.
fn created_report(
    plan: &ProvisionPlan,
    created: Vec<AppliedStep>,
    status: ProvisionOutcome,
    message: String,
) -> ProvisionApplyReport {
    let mut report = unapplied_report(plan, status, ApplyStatus::Confirmed, message, None);
    report.check.confirmation_matched = true;
    report.check.data_mutated = true;
    report.applied_steps = created;
    report
}

/// The local operator label of a surface; `None` for the Control Plane, which applies as a
/// principal.
pub(super) const fn local_operator_label(surface: ProvisionSurface) -> Option<&'static str> {
    match surface {
        ProvisionSurface::RustApi => Some("local:embedded"),
        ProvisionSurface::Cli => Some("local:cli"),
        ProvisionSurface::Serve => Some("local:serve"),
        ProvisionSurface::ControlPlane => None,
    }
}

/// The environment of the recomputed plan: a principal actor is the principal checked for
/// `RBAC_SELF_LOCKOUT`. The principal is not part of the plan id.
fn actor_environment(actor: &ProvisionActor, env: &ProvisionEnvironment) -> ProvisionEnvironment {
    let mut env = env.clone();
    if let ProvisionActor::Principal(principal) = actor {
        env.principal = Some(principal.clone());
    }
    env
}

/// `prov_` and 32 lowercase hex characters.
fn is_provision_plan_id(id: &str) -> bool {
    id.strip_prefix("prov_").is_some_and(|hex| {
        hex.len() == 32
            && hex
                .chars()
                .all(|character| character.is_ascii_digit() || ('a'..='f').contains(&character))
    })
}

/// `Ok(None)` when the confirmation may proceed, `Ok(Some(report))` for a rejected one.
fn check_confirmation(
    plan: &ProvisionPlan,
    confirmation: &Confirmation,
) -> Result<Option<ProvisionApplyReport>, ProvisionError> {
    match confirmation {
        Confirmation::AutoApproveAdditive => Ok(None),
        Confirmation::PlanId(id) if *id == plan.plan_id => Ok(None),
        Confirmation::PlanId(id) if is_provision_plan_id(id) => Err(ProvisionError::PlanChanged {
            confirmed: id.clone(),
            current: plan.plan_id.clone(),
        }),
        Confirmation::PlanId(id) => Ok(Some(unapplied_report(
            plan,
            ProvisionOutcome::Rejected,
            ApplyStatus::Rejected,
            format!(
                "{} is not a provisioning plan id; confirm with the prov_ id of the plan ({})",
                printable(id),
                plan.plan_id
            ),
            None,
        ))),
    }
}

/// Refuses blocked plans, plans that need flags and auto-approval of plans that are not
/// `auto_approvable`.
fn gate(plan: &ProvisionPlan, confirmation: &Confirmation) -> Result<(), ProvisionError> {
    match plan.outcome {
        PlanOutcome::Blocked => {
            if plan
                .issues
                .iter()
                .any(|found| found.code == "AUDIT_REQUIRED_FOR_APPLY")
            {
                Err(ProvisionError::AuditRequired)
            } else {
                Err(ProvisionError::Blocked(Box::new(plan.clone())))
            }
        }
        PlanOutcome::NeedsFlags => Err(ProvisionError::NeedsFlags(Box::new(plan.clone()))),
        PlanOutcome::Ready | PlanOutcome::Noop => {
            if *confirmation == Confirmation::AutoApproveAdditive && !plan.auto_approvable {
                Err(ProvisionError::ConfirmationRequired(Box::new(plan.clone())))
            } else {
                Ok(())
            }
        }
    }
}

/// The plan recomputed against a database `provision_new_database` just created holds the
/// confirmed steps without `create_database`: same kinds, paths, objects, safety classes and
/// flags, in the same order. `auto_approvable` is not compared, since RBAC steps on an existing
/// database are never auto-approvable.
fn continuation_matches(confirmed: &ProvisionPlan, recomputed: &ProvisionPlan) -> bool {
    fn signature(
        step: &ProvisionStep,
    ) -> (
        ProvisionStepKind,
        &str,
        Option<&str>,
        StepSafety,
        Option<&str>,
    ) {
        (
            step.kind,
            &step.path,
            step.object.as_deref(),
            step.safety,
            step.requires_flag.as_deref(),
        )
    }
    let expected = confirmed
        .steps
        .iter()
        .filter(|step| step.kind != ProvisionStepKind::CreateDatabase)
        .map(signature)
        .collect::<Vec<_>>();
    let actual = recomputed.steps.iter().map(signature).collect::<Vec<_>>();
    recomputed.outcome == PlanOutcome::Ready
        && expected == actual
        && confirmed.required_flags == recomputed.required_flags
}

fn resolved_profile(plan: &ProvisionPlan) -> Result<&ResolvedProfile, ProvisionError> {
    plan.resolved_profile
        .as_ref()
        .ok_or_else(|| ProvisionError::Blocked(Box::new(plan.clone())))
}

/// A confirmation, at most 64 printable characters of it, for messages.
fn printable(confirmation: &str) -> String {
    let shown = confirmation
        .chars()
        .filter(|character| !character.is_control())
        .take(64)
        .collect::<String>();
    format!("{shown:?}")
}

/// A report for an apply that changed nothing.
fn unapplied_report(
    plan: &ProvisionPlan,
    status: ProvisionOutcome,
    check_status: ApplyStatus,
    message: String,
    record: Option<ProvisionRecordSummary>,
) -> ProvisionApplyReport {
    ProvisionApplyReport {
        status,
        check: ApplyCheckReport {
            plan_id: plan.plan_id.clone(),
            status: check_status,
            valid: plan.validation.valid,
            confirmation_matched: false,
            audit_recorded: false,
            data_mutated: false,
            message,
        },
        plan_id: plan.plan_id.clone(),
        applied_steps: Vec::new(),
        failed_step: None,
        record,
        warnings: plan_warnings(plan),
        session_pending_restart: pending_restart(plan),
    }
}

fn noop_report(
    db: &Database,
    plan: &ProvisionPlan,
    confirmation: &Confirmation,
) -> Result<ProvisionApplyReport, ProvisionError> {
    let record = db
        .read_provision_record()?
        .readable()
        .map(ProvisionRecordSummary::of);
    let mut report = unapplied_report(
        plan,
        ProvisionOutcome::Noop,
        ApplyStatus::Confirmed,
        "the database already matches the spec; nothing was applied".to_owned(),
        record,
    );
    report.check.confirmation_matched =
        matches!(confirmation, Confirmation::PlanId(id) if *id == plan.plan_id);
    Ok(report)
}

/// The Warnings of a plan, which every report of it carries.
pub(super) fn plan_warnings(plan: &ProvisionPlan) -> Vec<ValidationIssue> {
    plan.issues
        .iter()
        .filter(|found| found.severity == ValidationSeverity::Warning)
        .cloned()
        .collect()
}

fn pending_restart(plan: &ProvisionPlan) -> bool {
    plan.drift
        .items
        .iter()
        .any(|item| item.kind == DriftKind::SessionPendingRestart)
}

/// The `--allow-*` flags of the options, as the record and history store them.
fn option_flags(opts: &ProvisionOptions) -> Vec<String> {
    let mut flags = opts
        .allow_weakening
        .iter()
        .map(|path| format!("allow_weakening:{path}"))
        .collect::<Vec<_>>();
    flags.extend(
        opts.allow_destructive
            .iter()
            .map(|path| format!("allow_destructive:{path}")),
    );
    if opts.allow_format_upgrade {
        flags.push("allow_format_upgrade".to_owned());
    }
    flags
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// The kind of a step error, for reports and the Control Plane's status mapping.
const fn error_kind(error: &DbError) -> &'static str {
    match error {
        DbError::CatalogObjectExists(_) => "catalog_object_exists",
        DbError::MissingCatalogObject(_) => "missing_catalog_object",
        DbError::CatalogKindMismatch { .. } => "catalog_kind_mismatch",
        DbError::InvalidCatalogName(_) => "invalid_catalog_name",
        DbError::AuthzDenied(_) => "authz_denied",
        DbError::AuditIntegrity(_) => "audit_integrity",
        DbError::Config(_) => "config",
        DbError::Storage(_) | DbError::Repl(_) => "storage",
        DbError::Serde(_) => "serde",
        DbError::TransactionAborted(_) => "transaction_aborted",
        _ => "runtime",
    }
}

/// The RBAC resource of a managed object, for its release event.
fn managed_resource(name: &str, kind: ObjectKind) -> Resource {
    let name = name.to_owned();
    match kind {
        ObjectKind::Document => Resource::Collection(name),
        ObjectKind::Vector => Resource::VectorCollection(name),
        ObjectKind::TimeSeries => Resource::TimeSeries(name),
        ObjectKind::Graph => Resource::Graph(name),
        ObjectKind::FullText => Resource::FullTextIndex(name),
        ObjectKind::Geo => Resource::GeoIndex(name),
        ObjectKind::Table
        | ObjectKind::History
        | ObjectKind::ForeignTable
        | ObjectKind::MaterializedView => Resource::Table(name),
    }
}

/// The record an apply journals before its first step: the target spec, with what the previous
/// record manages until the steps change it, and the spec the last finished apply converged to.
fn journal_record(
    previous: Option<&ProvisionRecord>,
    plan: &ProvisionPlan,
    spec: &DatabaseSpec,
    resolved: &ResolvedProfile,
    actor: &ProvisionActor,
    opts: &ProvisionOptions,
) -> ProvisionRecord {
    ProvisionRecord {
        record_version: RECORD_VERSION,
        status: ProvisionRecordStatus::InProgress,
        plan_id: plan.plan_id.clone(),
        spec_digest: spec_digest(spec),
        applied_spec: spec.clone(),
        last_applied_spec: previous.and_then(ProvisionRecord::last_applied).cloned(),
        profile_digest: resolved.digest.clone(),
        objects: previous
            .map(|record| record.objects.clone())
            .unwrap_or_default(),
        managed_roles: previous
            .map(|record| record.managed_roles.clone())
            .unwrap_or_default(),
        managed_principals: previous
            .map(|record| record.managed_principals.clone())
            .unwrap_or_default(),
        released_objects: previous
            .map(|record| record.released_objects.clone())
            .unwrap_or_default(),
        guarantees: plan.guarantees.clone(),
        completed_steps: Vec::new(),
        failed_step: None,
        flags: option_flags(opts),
        seed_digest: None,
        applied_at_millis: now_millis(),
        applied_by: actor.name().to_owned(),
        binary_version: env!("CARGO_PKG_VERSION").to_owned(),
    }
}

/// Runs the steps of a confirmed plan. `done` holds steps that already ran (the
/// `create_database` step of a new database).
fn execute(
    db: &mut Database,
    actor: &ProvisionActor,
    spec: &DatabaseSpec,
    plan: &ProvisionPlan,
    opts: &ProvisionOptions,
    done: Vec<AppliedStep>,
) -> Result<ProvisionApplyReport, ProvisionError> {
    let resolved = resolved_profile(plan)?;
    let desired = physical::project(spec, resolved)
        .map_err(|_| ProvisionError::Blocked(Box::new(plan.clone())))?;
    let manifest = desired_manifest(resolved)?;
    let previous = db.read_provision_record()?.readable().cloned();
    let mut record = journal_record(previous.as_ref(), plan, spec, resolved, actor, opts);
    record
        .completed_steps
        .extend(done.iter().map(|step| step.step_id.clone()));
    let run = Run {
        db,
        actor,
        spec,
        plan,
        desired,
        manifest,
        record,
        applied: done,
        warnings: plan_warnings(plan),
        security_committed: false,
        session_pending_restart: pending_restart(plan),
        audit_recorded: true,
    };
    run.execute()
}

/// What a step did, before it is reported.
struct Done {
    outcome: StepOutcome,
    detail: String,
}

impl Done {
    fn applied(detail: impl Into<String>) -> Self {
        Self {
            outcome: StepOutcome::Applied,
            detail: detail.into(),
        }
    }

    fn satisfied(detail: impl Into<String>) -> Self {
        Self {
            outcome: StepOutcome::AlreadySatisfied,
            detail: detail.into(),
        }
    }
}

/// One apply in progress.
struct Run<'a> {
    db: &'a mut Database,
    actor: &'a ProvisionActor,
    spec: &'a DatabaseSpec,
    plan: &'a ProvisionPlan,
    desired: Vec<DesiredObject>,
    manifest: PersistedProfileManifest,
    /// The journal: written after every step.
    record: ProvisionRecord,
    applied: Vec<AppliedStep>,
    warnings: Vec<ValidationIssue>,
    /// Every `apply_security` step commits with the first one.
    security_committed: bool,
    session_pending_restart: bool,
    /// The completion or failure event of the apply was written.
    audit_recorded: bool,
}

impl Run<'_> {
    fn execute(mut self) -> Result<ProvisionApplyReport, ProvisionError> {
        let plan = self.plan;
        let done = self
            .applied
            .iter()
            .map(|step| step.step_id.clone())
            .collect::<BTreeSet<_>>();
        let steps = plan
            .steps
            .iter()
            .filter(|step| !done.contains(&step.step_id))
            .collect::<Vec<_>>();
        let mut rest = steps.as_slice();

        // The audit gate: an apply audits its intent first, so audit must be on, or be turned
        // on by the first step.
        if !self.db.audit_enabled() {
            let Some((first, tail)) = rest
                .split_first()
                .filter(|(first, _)| first.kind == ProvisionStepKind::EnableAudit)
            else {
                return Err(ProvisionError::AuditRequired);
            };
            let lsn = self.db.commit_lsn().ok();
            match self.run_step(first) {
                Ok(enabled) => self.complete(first, enabled),
                // Audit may be on without its event: the database changed, so report it.
                Err(error) if self.wrote_since(lsn) => {
                    self.push_failure(first, &error, true);
                    return Ok(self.fail(Some(&first.step_id), &error, true));
                }
                Err(error) => return Err(error.into()),
            }
            rest = tail;
        }

        let intent = self
            .event(
                "provision_apply_started",
                Resource::System,
                AuditOutcome::Succeeded,
                &self.intent_detail(),
            )
            .and_then(|()| self.db.write_provision_record(&self.record, None));
        if let Err(error) = intent {
            // Nothing is returned as an error once a step changed the database: audit turned on
            // above, or the database `provision_new_database` created.
            if self.applied_any() {
                return Ok(self.fail(None, &error, false));
            }
            return Err(error.into());
        }

        let disable_audit = match self.run_steps(rest) {
            Ok(disable_audit) => disable_audit,
            Err((step, error, wrote)) => {
                return Ok(self.fail(Some(&step.step_id), &error, wrote));
            }
        };

        if let Err(error) = self.write_applied_record() {
            let last = steps.last().map(|step| step.step_id.clone());
            return Ok(self.fail(last.as_deref(), &error, true));
        }
        // Every step is applied and the record says so: a completion event that cannot be
        // written is a Warning, never an error.
        if let Err(error) = self.event(
            "provision_apply_completed",
            Resource::System,
            AuditOutcome::Succeeded,
            &format!(
                "plan_id: {} steps: {} data_mutated: true",
                plan.plan_id,
                self.applied.len()
            ),
        ) {
            self.audit_recorded = false;
            self.warnings.push(completion_not_audited(&error));
        }

        // Audit is disabled last, after the completion event.
        if let Some(step) = disable_audit {
            let lsn = self.db.commit_lsn().ok();
            match self.run_step(step) {
                Ok(done) => self.complete(step, done),
                Err(error) => {
                    let wrote = self.wrote_since(lsn);
                    self.push_failure(step, &error, wrote);
                    return Ok(self.fail(Some(&step.step_id), &error, wrote));
                }
            }
        }
        Ok(self.report(
            ProvisionOutcome::Applied,
            "every step of the plan was applied",
        ))
    }

    /// Runs `steps` in plan order and journals each one, all but the `disable_audit` step,
    /// which is returned to run last. A failure returns the step, its error and whether the step
    /// wrote to the database before it failed (a step whose journal entry fails did).
    fn run_steps<'s>(
        &mut self,
        steps: &[&'s ProvisionStep],
    ) -> Result<Option<&'s ProvisionStep>, (&'s ProvisionStep, DbError, bool)> {
        let mut disable_audit = None;
        for step in steps {
            if step.kind == ProvisionStepKind::DisableAudit {
                disable_audit = Some(*step);
                continue;
            }
            let lsn = self.db.commit_lsn().ok();
            match self.run_step(step) {
                Ok(done) => {
                    self.complete(step, done);
                    // The step is applied even when its journal entry is not.
                    self.db
                        .write_provision_record(&self.record, None)
                        .map_err(|error| (*step, error, true))?;
                }
                Err(error) => {
                    let wrote = self.wrote_since(lsn);
                    if wrote {
                        self.manage_committed_object(step);
                    }
                    self.push_failure(step, &error, wrote);
                    return Err((*step, error, wrote));
                }
            }
        }
        Ok(disable_audit)
    }

    /// Records a provisioning audit event as the actor.
    fn event(
        &self,
        action: &str,
        resource: Resource,
        outcome: AuditOutcome,
        detail: &str,
    ) -> Result<(), DbError> {
        #[cfg(test)]
        if injections().fail_event == Some(action) {
            return Err(DbError::AuditIntegrity(format!(
                "injected failure of {action}"
            )));
        }
        self.db
            .record_provision_event(self.actor, action, resource, outcome, detail)
    }

    /// Whether a step of this apply changed the database.
    fn applied_any(&self) -> bool {
        self.applied
            .iter()
            .any(|step| step.outcome == StepOutcome::Applied)
    }

    /// Whether anything committed since `lsn`, the commit LSN before a step. When either LSN
    /// cannot be read, the step counts as having written.
    fn wrote_since(&self, lsn: Option<TxnId>) -> bool {
        match (lsn, self.db.commit_lsn().ok()) {
            (Some(before), Some(now)) => now > before,
            _ => true,
        }
    }

    /// A create that failed after its catalog entry committed, for example on its audit event,
    /// still created the object: the record takes it over, so the next plan finds it managed.
    fn manage_committed_object(&mut self, step: &ProvisionStep) {
        let Ok(object) = self.desired_object(step) else {
            return;
        };
        let created = self
            .db
            .catalog()
            .get(&object.name)
            .map(entry_kind)
            .is_some_and(|kind| kind == object.kind.object_kind());
        if created && !self.record.objects.contains_key(&object.name) {
            self.manage(&object, false);
        }
    }

    fn intent_detail(&self) -> String {
        format!(
            "plan_id: {} surface: {} steps: {} flags: {}",
            self.plan.plan_id,
            self.plan.surface.slug(),
            self.plan.steps.len(),
            if self.record.flags.is_empty() {
                "none".to_owned()
            } else {
                self.record.flags.join(" ")
            }
        )
    }

    /// Runs one step. Every step first checks whether it is already satisfied.
    fn run_step(&mut self, step: &ProvisionStep) -> Result<Done, DbError> {
        #[cfg(test)]
        if let Some(index) = injections().fail_step
            && step.step_id == format!("pstep-{:03}", index + 1)
        {
            return Err(DbError::TransactionAborted(format!(
                "injected failure of {}",
                step.step_id
            )));
        }
        let done = self.run_step_kind(step)?;
        #[cfg(test)]
        if let Some(index) = injections().fail_after_write
            && step.step_id == format!("pstep-{:03}", index + 1)
        {
            return Err(DbError::TransactionAborted(format!(
                "injected failure of {} after its write",
                step.step_id
            )));
        }
        Ok(done)
    }

    fn run_step_kind(&mut self, step: &ProvisionStep) -> Result<Done, DbError> {
        match step.kind {
            ProvisionStepKind::CreateDatabase => Ok(Done::satisfied("the database exists")),
            ProvisionStepKind::EnableAudit => self.set_audit(true),
            ProvisionStepKind::DisableAudit => self.set_audit(false),
            ProvisionStepKind::CreateCollection
            | ProvisionStepKind::CreateVectorCollection
            | ProvisionStepKind::CreateTable
            | ProvisionStepKind::CreateTimeSeries
            | ProvisionStepKind::CreateGraph
            | ProvisionStepKind::CreateFullTextIndex
            | ProvisionStepKind::CreateGeoIndex
            | ProvisionStepKind::EnableSystemVersioning => self.create(step),
            ProvisionStepKind::AdoptObject => self.adopt(step),
            ProvisionStepKind::AdoptProfile
            | ProvisionStepKind::RetuneProfile
            | ProvisionStepKind::UpgradeStorageFormat => self.write_profile(step),
            ProvisionStepKind::ApplySecurity => self.apply_security(),
            ProvisionStepKind::ReleaseObject => self.release(step),
            ProvisionStepKind::RecordGuarantees
            | ProvisionStepKind::RecordMetadata
            | ProvisionStepKind::RecordAppliedSpec
            | ProvisionStepKind::Noop => Ok(Done::applied(format!(
                "{}; written with the applied record",
                step.action
            ))),
        }
    }

    fn complete(&mut self, step: &ProvisionStep, done: Done) {
        self.applied.push(AppliedStep {
            step_id: step.step_id.clone(),
            kind: step.kind,
            object: step.object.clone(),
            outcome: done.outcome,
            detail: done.detail,
            error_kind: None,
        });
        self.record.completed_steps.push(step.step_id.clone());
    }

    /// Reports a failed step; `wrote` says that it committed a write before it failed.
    fn push_failure(&mut self, step: &ProvisionStep, error: &DbError, wrote: bool) {
        let detail = if wrote {
            format!("{error}; the step wrote to the database before it failed")
        } else {
            error.to_string()
        };
        self.applied.push(AppliedStep {
            step_id: step.step_id.clone(),
            kind: step.kind,
            object: step.object.clone(),
            outcome: StepOutcome::Failed,
            detail,
            error_kind: Some(error_kind(error).to_owned()),
        });
    }

    /// Records a failure: a `failed` record with a history entry, and `provision_apply_failed`.
    /// When either cannot be written, the report says so in a warning; the record then still
    /// says `in_progress`, which the next plan treats the same way. `failed_step` is `None` when
    /// the apply stopped between steps, and `wrote` says that the failed step committed a write
    /// before it failed.
    fn fail(
        mut self,
        failed_step: Option<&str>,
        error: &DbError,
        wrote: bool,
    ) -> ProvisionApplyReport {
        self.record.status = ProvisionRecordStatus::Failed;
        self.record.failed_step = failed_step.map(str::to_owned);
        self.record.applied_at_millis = now_millis();
        let history = self.history_entry();
        if let Err(write_error) = self.db.write_provision_record(&self.record, Some(&history)) {
            self.warnings.push(failure_not_recorded(&format!(
                "the failed record could not be written: {write_error}"
            )));
        }
        let detail = format!(
            "plan_id: {} failed_step: {} error: {} data_mutated: true",
            self.plan.plan_id,
            failed_step.unwrap_or("none"),
            error_kind(error)
        );
        if let Err(audit_error) = self.event(
            "provision_apply_failed",
            Resource::System,
            AuditOutcome::Failed,
            &detail,
        ) {
            self.audit_recorded = false;
            self.warnings.push(failure_not_recorded(&format!(
                "provision_apply_failed could not be audited: {audit_error}"
            )));
        }
        let message = match failed_step {
            Some(step) if wrote => format!(
                "{step} failed after it wrote to the database: {error}; what it wrote and the \
                 steps before it stay applied; plan again to see what remains"
            ),
            Some(step) => format!(
                "{step} failed: {error}; the steps before it stay applied; plan again to see what \
                 remains"
            ),
            None => format!(
                "the apply stopped before its next step: {error}; the steps before it stay \
                 applied; plan again to see what remains"
            ),
        };
        self.report(ProvisionOutcome::PartiallyApplied, &message)
    }

    /// The `applied` record and its history entry. Managed roles and principals are the
    /// declared ones: every RBAC step ran, and a managed name that is gone is no longer managed.
    fn write_applied_record(&mut self) -> Result<(), DbError> {
        let security = self.spec.security.clone().unwrap_or_default();
        self.record.managed_roles = security.roles.into_iter().map(|role| role.name).collect();
        self.record.managed_principals = security
            .principals
            .into_iter()
            .map(|principal| principal.user)
            .collect();
        let record = &mut self.record;
        let catalog = self.db.catalog();
        let objects = &record.objects;
        record
            .released_objects
            .retain(|name| catalog.contains_key(name) && !objects.contains_key(name));
        record.status = ProvisionRecordStatus::Applied;
        record.last_applied_spec = None;
        record.failed_step = None;
        record.applied_at_millis = now_millis();
        let history = self.history_entry();
        self.db.write_provision_record(&self.record, Some(&history))
    }

    fn history_entry(&self) -> ProvisionHistoryEntry {
        ProvisionHistoryEntry {
            plan_id: self.plan.plan_id.clone(),
            status: self.record.status,
            flags: self.record.flags.clone(),
            actor: self.actor.name().to_owned(),
            at_millis: self.record.applied_at_millis,
        }
    }

    fn report(self, status: ProvisionOutcome, message: &str) -> ProvisionApplyReport {
        ProvisionApplyReport {
            status,
            check: ApplyCheckReport {
                plan_id: self.plan.plan_id.clone(),
                status: ApplyStatus::Confirmed,
                valid: true,
                confirmation_matched: true,
                audit_recorded: self.audit_recorded,
                data_mutated: true,
                message: message.to_owned(),
            },
            plan_id: self.plan.plan_id.clone(),
            failed_step: self.record.failed_step.clone(),
            record: Some(ProvisionRecordSummary::of(&self.record)),
            applied_steps: self.applied,
            warnings: self.warnings,
            session_pending_restart: self.session_pending_restart,
        }
    }

    fn desired_object(&self, step: &ProvisionStep) -> Result<DesiredObject, DbError> {
        let name = step.object.as_deref().unwrap_or_default();
        self.desired
            .iter()
            .find(|object| object.name == name)
            .cloned()
            .ok_or_else(|| DbError::MissingCatalogObject(format!("{name} is not in the spec")))
    }

    /// Takes ownership of a live object in the record, with the id it has.
    fn manage(&mut self, object: &DesiredObject, adopted: bool) {
        let allocated_id = match self.db.catalog().get(&object.name) {
            Some(
                CatalogEntry::Collection { collection_id, .. }
                | CatalogEntry::Vector { collection_id, .. },
            ) => Some(collection_id.as_u32()),
            Some(CatalogEntry::Graph { graph_id }) => Some(graph_id.as_u32()),
            _ => None,
        };
        self.record.released_objects.remove(&object.name);
        self.record.objects.insert(
            object.name.clone(),
            ManagedObject {
                spec_collection: object.spec_collection.clone(),
                kind: object.kind.object_kind(),
                adopted,
                allocated_id,
            },
        );
    }

    fn set_audit(&mut self, enabled: bool) -> Result<Done, DbError> {
        if self.db.audit_enabled() == enabled {
            return Ok(Done::satisfied(format!(
                "audit is already {}",
                on_off(enabled)
            )));
        }
        match self.actor {
            ProvisionActor::Principal(principal) => {
                self.db.set_audit_enabled_as(principal, enabled)?;
            }
            ProvisionActor::LocalOperator { label } => {
                self.db.set_audit_enabled_by_provisioning(label, enabled)?;
            }
        }
        Ok(Done::applied(format!("turned audit {}", on_off(enabled))))
    }

    /// Creates a container or a dependent object. A principal uses the audited `*_as` creates;
    /// a local operator uses the base creates and audits `provision_create_object`.
    fn create(&mut self, step: &ProvisionStep) -> Result<Done, DbError> {
        let object = self.desired_object(step)?;
        // The apply holds the database exclusively, so only an earlier step of this apply can
        // have created the object.
        match self.db.catalog().get(&object.name).map(entry_kind) {
            Some(kind) if kind == object.kind.object_kind() => {
                self.manage(&object, false);
                return Ok(Done::satisfied(format!("{} already exists", object.name)));
            }
            Some(_) => return Err(DbError::CatalogObjectExists(object.name)),
            None => {}
        }
        let principal = self.actor.principal().cloned();
        self.create_object(&object, principal.as_ref())?;
        self.manage(&object, false);
        if principal.is_none() {
            self.event(
                "provision_create_object",
                object_resource(&object.name, &object.kind),
                AuditOutcome::Succeeded,
                &format!("step: {} kind: {}", step.step_id, object.kind.slug()),
            )?;
        }
        Ok(Done::applied(step.action.clone()))
    }

    fn create_object(
        &mut self,
        object: &DesiredObject,
        principal: Option<&Principal>,
    ) -> Result<(), DbError> {
        let name = object.name.as_str();
        match &object.kind {
            DesiredKind::Document { .. }
            | DesiredKind::Vector { .. }
            | DesiredKind::Graph { .. } => self.create_with_id(object, principal)?,
            DesiredKind::Table {
                layout,
                schema,
                indexes,
            } => {
                let (schema, indexes) = (schema.clone(), indexes.clone());
                match principal {
                    Some(principal) => {
                        self.db.create_table_with_layout_as(
                            principal, name, schema, indexes, *layout,
                        )?;
                    }
                    None => {
                        self.db
                            .create_table_with_layout(name, schema, indexes, *layout)?;
                    }
                }
            }
            DesiredKind::TimeSeries { .. } => {
                let config = time_series_config(name, &object.kind).ok_or_else(|| {
                    DbError::MissingCatalogObject(format!("{name} is not a time series"))
                })?;
                match principal {
                    Some(principal) => {
                        let _ = self.db.create_time_series_as(principal, &config)?;
                    }
                    None => {
                        let _ = self.db.create_time_series(config)?;
                    }
                }
            }
            DesiredKind::FullText {
                source,
                path,
                language,
                refresh_lag_target,
            } => {
                let mut config = FullTextIndexConfig::collection(
                    name,
                    self.source_collection_id(source)?,
                    path.clone(),
                );
                config.language.clone_from(language);
                config.refresh_lag_target = *refresh_lag_target;
                match principal {
                    Some(principal) => {
                        let _ = self.db.create_full_text_index_as(principal, &config)?;
                    }
                    None => {
                        let _ = self.db.create_full_text_index(config)?;
                    }
                }
            }
            DesiredKind::Geo {
                source,
                path,
                precision,
            } => {
                let mut config =
                    GeoIndexConfig::new(name, self.source_collection_id(source)?, path.clone());
                config.precision = *precision;
                match principal {
                    Some(principal) => {
                        let _ = self.db.create_geo_index_as(principal, &config)?;
                    }
                    None => {
                        let _ = self.db.create_geo_index(config)?;
                    }
                }
            }
            DesiredKind::History {
                base_table,
                keep_history,
            } => {
                let retention = TemporalRetention {
                    min_lsn: self.history_start(base_table)?,
                    keep_history: *keep_history,
                };
                match principal {
                    Some(principal) => {
                        self.db
                            .enable_system_versioning_as(principal, base_table, retention)?;
                    }
                    None => {
                        self.db.enable_system_versioning(base_table, retention)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// The LSN the history of `base_table` starts from (`TemporalRetention.min_lsn`). A table
    /// this apply created had no history before it existed, so its history starts at the newest
    /// commit, after the table's own; an existing table keeps the history the database still
    /// holds for it, from the history GC horizon (0 when GC never pruned), so its `AS OF` reads
    /// answer as before. Both are at or above the horizon, which `enable_system_versioning`
    /// requires, and a kept history holds the GC watermark at its start.
    fn history_start(&self, base_table: &str) -> Result<TxnId, DbError> {
        let horizon = self.db.history_horizon()?;
        let created = self.applied.iter().any(|step| {
            step.kind == ProvisionStepKind::CreateTable
                && step.object.as_deref() == Some(base_table)
        });
        if created {
            Ok(self.db.commit_lsn()?.max(horizon))
        } else {
            Ok(horizon)
        }
    }

    /// Creates a document collection, vector collection or graph: with its pinned id, or with
    /// the next free id (the `_auto_id` creates).
    fn create_with_id(
        &mut self,
        object: &DesiredObject,
        principal: Option<&Principal>,
    ) -> Result<(), DbError> {
        let name = object.name.as_str();
        match &object.kind {
            DesiredKind::Document {
                collection_id,
                fields,
                indexes,
            } => {
                let (fields, indexes) = (fields.clone(), indexes.clone());
                match (principal, collection_id) {
                    (Some(principal), Some(id)) => {
                        let _ = self
                            .db
                            .create_collection_as(principal, name, *id, fields, indexes)?;
                    }
                    (Some(principal), None) => {
                        self.db
                            .create_collection_auto_id_as(principal, name, fields, indexes)?;
                    }
                    (None, Some(id)) => {
                        let _ = self.db.create_collection(name, *id, fields, indexes)?;
                    }
                    (None, None) => {
                        let _ = self.db.create_collection_auto_id(name, fields, indexes)?;
                    }
                }
            }
            DesiredKind::Vector {
                collection_id,
                dim,
                metric,
                hnsw,
            } => match (principal, collection_id) {
                (Some(principal), Some(id)) => {
                    let config = VectorCollectionConfig::new(*id, *dim)
                        .with_metric(*metric)
                        .with_hnsw(*hnsw);
                    let _ = self
                        .db
                        .create_vector_collection_as(principal, name, config)?;
                }
                (Some(principal), None) => {
                    self.db.create_vector_collection_auto_id_as(
                        principal, name, *dim, *metric, *hnsw,
                    )?;
                }
                (None, Some(id)) => {
                    let config = VectorCollectionConfig::new(*id, *dim)
                        .with_metric(*metric)
                        .with_hnsw(*hnsw);
                    let _ = self.db.create_vector_collection(name, config)?;
                }
                (None, None) => {
                    let _ = self
                        .db
                        .create_vector_collection_auto_id(name, *dim, *metric, *hnsw)?;
                }
            },
            DesiredKind::Graph { graph_id } => match (principal, graph_id) {
                (Some(principal), Some(id)) => {
                    let _ = self.db.create_graph_as(principal, name, *id)?;
                }
                (Some(principal), None) => {
                    self.db.create_graph_auto_id_as(principal, name)?;
                }
                (None, Some(id)) => {
                    let _ = self.db.create_graph(name, *id)?;
                }
                (None, None) => {
                    let _ = self.db.create_graph_auto_id(name)?;
                }
            },
            _ => {}
        }
        Ok(())
    }

    /// The live id of the document collection a full-text or geo index reads: it may have been
    /// allocated by an earlier step of this apply.
    fn source_collection_id(&self, source: &str) -> Result<CollectionId, DbError> {
        match self.db.catalog().get(source) {
            Some(CatalogEntry::Collection { collection_id, .. }) => Ok(*collection_id),
            _ => Err(DbError::MissingCatalogObject(format!(
                "document collection {source}"
            ))),
        }
    }

    /// Takes ownership of an existing object that matches the spec. Nothing in the database
    /// changes but the record.
    fn adopt(&mut self, step: &ProvisionStep) -> Result<Done, DbError> {
        let object = self.desired_object(step)?;
        if !self.db.catalog().contains_key(&object.name) {
            return Err(DbError::MissingCatalogObject(object.name));
        }
        self.manage(&object, true);
        Ok(Done::applied(step.action.clone()))
    }

    /// Writes the desired profile manifest, once: every profile step of a plan converges to the
    /// same manifest. Retunes and format upgrades take effect at the next open.
    fn write_profile(&mut self, step: &ProvisionStep) -> Result<Done, DbError> {
        match step.safety {
            StepSafety::FormatUpgrade => self.session_pending_restart = true,
            // Only knobs the session does not already run wait for the next open: a query
            // governor knob set for the process keeps its own limit.
            StepSafety::Retune => {
                let session = self.db.performance_config();
                let expected = expected_session_performance(
                    session,
                    self.db.query_governor_overrides(),
                    &self.manifest,
                );
                if *session != expected {
                    self.session_pending_restart = true;
                }
            }
            _ => {}
        }
        let schema_version = if self.manifest.storage_format.value_framing {
            SCHEMA_VERSION_FRAMED
        } else {
            SCHEMA_VERSION
        };
        let current = self.db.probed_metadata()?;
        let written = current.schema_version == schema_version
            && current
                .manifest
                .as_ref()
                .is_some_and(|live| same_manifest(live, &self.manifest));
        if written {
            return Ok(Done::satisfied("the profile manifest is already written"));
        }
        self.db
            .write_profile_manifest(&self.manifest, schema_version)?;
        self.event(
            "provision_write_profile_manifest",
            Resource::System,
            AuditOutcome::Succeeded,
            &format!(
                "step: {} profile: {} schema_version: {schema_version}",
                step.step_id, self.manifest.profile
            ),
        )?;
        Ok(Done::applied(step.action.clone()))
    }

    /// Commits every RBAC change of the plan in one batch, with the first `apply_security` step.
    fn apply_security(&mut self) -> Result<Done, DbError> {
        if self.security_committed {
            return Ok(Done::satisfied(
                "committed with the first apply_security step",
            ));
        }
        let (policy, principals) = security_post_state(
            self.spec,
            &self.desired,
            self.db.authz_policy(),
            self.db.principal_registry(),
            Some(&self.record),
        );
        let unchanged =
            &policy == self.db.authz_policy() && &principals == self.db.principal_registry();
        let detail = format!(
            "roles: {} principals: {}",
            policy.roles().len(),
            principals.principals().len()
        );
        if !unchanged {
            match self.actor {
                ProvisionActor::Principal(principal) => {
                    self.db
                        .set_security_state_as(principal, policy, principals)?;
                }
                ProvisionActor::LocalOperator { .. } => {
                    self.db.set_security_state(policy, principals)?;
                    self.event(
                        "set_security_state",
                        Resource::System,
                        AuditOutcome::Succeeded,
                        &detail,
                    )?;
                }
            }
        }
        self.security_committed = true;
        let security = self.spec.security.clone().unwrap_or_default();
        self.record.managed_roles = security.roles.into_iter().map(|role| role.name).collect();
        self.record.managed_principals = security
            .principals
            .into_iter()
            .map(|principal| principal.user)
            .collect();
        Ok(if unchanged {
            Done::satisfied("the RBAC state already matches; the record takes ownership")
        } else {
            Done::applied(format!(
                "committed every RBAC change in one batch ({detail})"
            ))
        })
    }

    /// Releases an object from management: the record forgets it, and an object that still
    /// exists keeps its data and is listed as released. Nothing in the database changes but the
    /// record.
    fn release(&mut self, step: &ProvisionStep) -> Result<Done, DbError> {
        let name = step.object.clone().unwrap_or_default();
        let kind = self
            .record
            .objects
            .remove(&name)
            .map_or(ObjectKind::Document, |managed| managed.kind);
        let live = self.db.catalog().contains_key(&name);
        if live {
            self.record.released_objects.insert(name.clone());
        }
        self.event(
            "provision_release_object",
            managed_resource(&name, kind),
            AuditOutcome::Succeeded,
            &format!(
                "step: {} data: {}",
                step.step_id,
                if live { "retained" } else { "missing" }
            ),
        )?;
        Ok(Done::applied(step.action.clone()))
    }
}

/// Equal manifests, apart from the build that wrote them.
fn same_manifest(live: &PersistedProfileManifest, desired: &PersistedProfileManifest) -> bool {
    PersistedProfileManifest {
        written_by: desired.written_by.clone(),
        ..live.clone()
    } == *desired
}

const fn on_off(enabled: bool) -> &'static str {
    if enabled { "on" } else { "off" }
}

fn completion_not_audited(error: &DbError) -> ValidationIssue {
    issue(
        "PROVISION_COMPLETION_NOT_AUDITED",
        ValidationSeverity::Warning,
        "guarantees.audit.enabled",
        format!(
            "every step was applied and the record says applied, but provision_apply_completed \
             could not be audited: {error}"
        ),
        "Check the audit log and its storage; the apply itself is complete, and a further plan \
         is a noop.",
    )
}

fn failure_not_recorded(message: &str) -> ValidationIssue {
    issue(
        "PROVISION_FAILURE_NOT_RECORDED",
        ValidationSeverity::Warning,
        "database.record",
        message,
        "Plan again: a record that still says in_progress is treated as an interrupted apply.",
    )
}
