//! Spec-driven provisioning (preview): building and converging a database from a
//! [`DatabaseSpec`](crate::config_spec::DatabaseSpec).
//!
//! - [`physical::project`] maps every collection of a version 2 spec, plus the full-text, geo
//!   and history objects it derives, onto the runtime objects the `Database::create_*` APIs
//!   build. Projection is pure: it validates and describes, and never touches a database.
//! - [`plan_provision`] computes a [`ProvisionPlan`] against an absent target or an open
//!   single-node database: every step with its safety class, the flags weakening, destructive
//!   and format-upgrade steps need, how each guarantee is enforced on the calling surface, the
//!   drift of the live database and a `prov_` id that changes whenever the spec, the live state,
//!   the options, the surface, the target or the binary changes.
//! - [`drift`] reports how a live database differs from a spec, [`export_spec`] describes a
//!   live database as a spec, and [`live_fingerprint`] is the fingerprint of the live state a
//!   plan was computed from.
//! - [`apply_provision`] applies a confirmed plan to an open database, and
//!   [`provision_new_database`] creates a database from a spec at an absent target. Audit comes
//!   first, progress is journaled in the provision record after every step, and applying again
//!   after a failure converges.
//! - [`open_from_spec`] opens (or creates) the database of a spec at startup, for `multidb
//!   serve` in spec mode and embedded apps: [`serve_startup_decision`] decides from the plan and
//!   the [`StartupMode`] whether to start, to apply an auto-approvable plan first, or to refuse.
//!
//! Planning, drift and export are read-only: they never create a file or write a database.
//! See `docs/provisioning.md`.

mod execute;
mod export;
mod guarantees;
mod live;
pub mod physical;
mod plan;
pub(crate) mod record;
mod startup;

#[cfg(test)]
mod tests;

pub(crate) use execute::carry_test_injections;
pub use execute::{
    AppliedStep, Confirmation, ProvisionActor, ProvisionApplyReport, ProvisionOutcome,
    ProvisionRecordSummary, StepOutcome, apply_provision, provision_new_database,
};
#[cfg(test)]
pub(crate) use execute::{
    fail_provision_event_for_tests, fail_provision_step_after_write_for_tests,
    fail_provision_step_for_tests, perturb_new_database_for_tests,
};
pub use export::{ExportSource, ExportedSpec, export_spec};
pub use guarantees::{EnforcementStatus, GuaranteeEnforcement};
pub use live::live_fingerprint;
pub use plan::{
    DriftItem, DriftKind, DriftReport, DriftResolution, FieldDiff, ObjectState, PLAN_FORMAT,
    PlanOutcome, PlannedObject, PlannedTarget, ProvisionEnvironment, ProvisionOptions,
    ProvisionPlan, ProvisionStep, ProvisionStepKind, ProvisionSurface, ProvisionTargetRef,
    StepSafety, TargetKind, drift, plan_provision,
};
pub use record::{
    ManagedObject, ObjectKind, ProvisionHistoryEntry, ProvisionRecord, ProvisionRecordStatus,
    RECORD_VERSION,
};
pub use startup::{
    StartupDecision, StartupMode, StartupRefusal, StartupReport, open_from_spec,
    serve_startup_decision,
};

use crate::config_spec::{
    CertificationImpact, ValidationIssue, ValidationReport, ValidationSeverity,
};
use crate::db::{ConfigError, DbError};

/// Why provisioning refused or failed.
#[derive(thiserror::Error, Debug)]
pub enum ProvisionError {
    #[error("the spec is invalid: {}", issue_summary(&.0.issues))]
    InvalidSpec(Box<ValidationReport>),

    #[error("provisioning plan {} is blocked: {}", .0.plan_id, issue_summary(&.0.issues))]
    Blocked(Box<ProvisionPlan>),

    #[error(
        "provisioning plan {} needs flags: {}",
        .0.plan_id,
        .0.required_flags.join(", ")
    )]
    NeedsFlags(Box<ProvisionPlan>),

    #[error("provisioning plan {} needs a confirmation: --confirm {}", .0.plan_id, .0.plan_id)]
    ConfirmationRequired(Box<ProvisionPlan>),

    #[error(
        "the plan changed since it was confirmed: confirmed {confirmed}, current {current}; \
         review the new plan and confirm it"
    )]
    PlanChanged { confirmed: String, current: String },

    #[error(
        "the target already holds a database: provisioning creates databases only at absent \
         targets; plan an existing database through its open handle"
    )]
    AlreadyInitialized,

    #[error("audit must be enabled to apply a provisioning plan")]
    AuditRequired,

    #[error("permission missing: {0}")]
    PermissionMissing(String),

    #[error("database is locked: {0}")]
    Locked(String),

    #[error("the provision record is unreadable: {0}")]
    RecordUnreadable(String),

    #[error("startup refused in {} mode: {}", .0.mode.slug(), .0.reason)]
    StartupRefused(Box<StartupRefusal>),

    #[error("database: {0}")]
    Db(#[from] DbError),

    #[error("config: {0}")]
    Config(#[from] ConfigError),

    #[error("io: {0}")]
    Io(String),
}

/// The codes of the Errors among `issues`, for error messages.
fn issue_summary(issues: &[ValidationIssue]) -> String {
    let codes = issues
        .iter()
        .filter(|issue| issue.severity == ValidationSeverity::Error)
        .map(|issue| format!("{} at {}", issue.code, issue.path))
        .collect::<Vec<_>>();
    if codes.is_empty() {
        "see the plan".to_owned()
    } else {
        codes.join("; ")
    }
}

/// A provisioning issue. Errors block certification; Warnings and Advice do not.
fn issue(
    code: &str,
    severity: ValidationSeverity,
    path: impl Into<String>,
    message: impl Into<String>,
    suggestion: impl Into<String>,
) -> ValidationIssue {
    ValidationIssue {
        code: code.to_owned(),
        severity,
        path: path.into(),
        message: message.into(),
        suggestion: suggestion.into(),
        certification_impact: if severity == ValidationSeverity::Error {
            CertificationImpact::BlocksCertified
        } else {
            CertificationImpact::None
        },
    }
}
