//! How each declared guarantee is enforced on the surface that provisions it.
//!
//! Every guarantee of a spec is reported with one status, so a plan never claims more than the
//! runtime does: enforced, enforced with the development audit key, trivially satisfied (one
//! replica), operator-managed (backups have no scheduler), not enforced (role constraints) or
//! not enforceable, which blocks the plan. A guarantee the spec does not ask for is reported as
//! not requested.

use serde::{Deserialize, Serialize};

use super::issue;
use super::plan::ProvisionSurface;
use crate::config_spec::{
    CollectionRole, ConflictResolution, ConsistencyMode, DatabaseSpec, ValidationIssue,
    ValidationSeverity, WriteAck,
};

/// The runbook printed for operator-managed backups.
const BACKUP_RUNBOOK: &str = "Back up with `multidb backup full --db <path> --profile auto --dest \
    <dir>` and `multidb backup incr --db <path> --profile auto --dest <dir> --parent \
    <backup-dir>`, check backups with `multidb verify --backup <backup-dir>` and restore with \
    `multidb restore --backup <backup-dir> --dest-db <path> --profile <profile>`; this build has \
    no backup scheduler, so schedule these yourself.";
/// The runbook printed for operator-managed point-in-time recovery.
const PITR_RUNBOOK: &str = "Keep a chain of full and incremental backups (`multidb backup full` \
    and `multidb backup incr --parent <backup-dir>`) and restore to a log position with \
    `multidb restore --backup <backup-dir> --dest-db <path> --profile <profile> --to-lsn <lsn>`; \
    this build has no backup scheduler.";

/// How one guarantee is enforced.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnforcementStatus {
    /// The runtime enforces it.
    Enforced,
    /// Audit is on, but its hash chain uses the built-in development key.
    EnforcedWithDevelopmentKey,
    /// Holds by construction on one replica.
    TriviallySatisfied,
    /// The operator provides it; the plan prints the runbook.
    OperatorManaged,
    /// Declared, but this build does not enforce it.
    NotEnforced,
    /// This surface or database cannot enforce it; the plan is blocked.
    NotEnforceable,
    /// The spec does not ask for it.
    NotRequested,
}

/// One guarantee and how it is enforced.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuaranteeEnforcement {
    /// The spec path, for example `guarantees.backup.enabled`.
    pub path: String,
    /// The requested value.
    pub requested: String,
    pub status: EnforcementStatus,
    pub detail: String,
    /// What to do about it; empty when nothing is needed.
    pub remediation: String,
}

/// What the guarantee matrix depends on besides the spec.
pub(crate) struct GuaranteeInputs<'a> {
    pub spec: &'a DatabaseSpec,
    pub surface: ProvisionSurface,
    /// `Some(encrypted)` for an existing database, `None` for a new one.
    pub existing_encrypted: Option<bool>,
    /// A new database gets an encryption config from the operator's environment.
    pub new_target_encrypted: bool,
    /// Audit events are chained with a configured key: on the open handle, or in
    /// `ProvisionEnvironment.audit` for a new database.
    pub audit_key_configured: bool,
}

/// Evaluates every guarantee of the spec, returning the statuses and the issues they raise:
/// `GUARANTEE_NOT_ENFORCEABLE` and `PROVISION_ENCRYPTION_MISMATCH` (Errors), and the Warnings
/// `GUARANTEE_OPERATOR_MANAGED`, `AUDIT_DEVELOPMENT_KEY` and `ROLE_CONSTRAINT_NOT_ENFORCED`.
pub(crate) fn evaluate(
    inputs: &GuaranteeInputs<'_>,
) -> (Vec<GuaranteeEnforcement>, Vec<ValidationIssue>) {
    let mut matrix = Matrix::default();
    replication_guarantees(inputs.spec, &mut matrix);
    backup_guarantees(inputs.spec, &mut matrix);
    let at_rest = at_rest_status(inputs, &mut matrix);
    if inputs.spec.guarantees.sensitive_data {
        let detail = if inputs.audit_key_configured {
            "follows guarantees.encryption.at_rest".to_owned()
        } else {
            "follows guarantees.encryption.at_rest; the audit hash chain uses the development \
             key (AUDIT_DEVELOPMENT_KEY)"
                .to_owned()
        };
        matrix.entries.push(GuaranteeEnforcement {
            path: "guarantees.sensitive_data".to_owned(),
            requested: "true".to_owned(),
            status: at_rest,
            detail,
            remediation: String::new(),
        });
    } else {
        matrix.not_requested("guarantees.sensitive_data");
    }
    audit_guarantee(inputs, &mut matrix);
    domain_and_role_guarantees(inputs.spec, &mut matrix);
    (matrix.entries, matrix.issues)
}

/// Write acknowledgement, conflict resolution and strict cross-domain transactions hold
/// trivially on one CP replica.
fn replication_guarantees(spec: &DatabaseSpec, matrix: &mut Matrix) {
    let guarantees = &spec.guarantees;
    matrix.push(
        "guarantees.write_ack",
        match guarantees.write_ack {
            WriteAck::Local => "local",
            WriteAck::Quorum => "quorum",
            WriteAck::All => "all",
        },
        EnforcementStatus::TriviallySatisfied,
        "one replica acknowledges every write, and redb commits with Durability::Immediate",
        "",
    );
    if guarantees.conflict_resolution == ConflictResolution::None {
        matrix.push(
            "guarantees.conflict_resolution",
            "none",
            EnforcementStatus::TriviallySatisfied,
            "a single CP replica has no concurrent writers to reconcile",
            "",
        );
    } else {
        matrix.push(
            "guarantees.conflict_resolution",
            &json_slug(&guarantees.conflict_resolution),
            EnforcementStatus::NotEnforceable,
            "conflict policies belong to AP replication, which provisioning does not build",
            "Set guarantees.conflict_resolution to none.",
        );
    }
    if guarantees.strict_cross_domain_transactions {
        matrix.push(
            "guarantees.strict_cross_domain_transactions",
            "true",
            EnforcementStatus::TriviallySatisfied,
            "every domain of a single-node CP database shares one replication mode",
            "",
        );
    } else {
        matrix.not_requested("guarantees.strict_cross_domain_transactions");
    }
}

/// Backups and point-in-time recovery are operator-managed: there is no scheduler.
fn backup_guarantees(spec: &DatabaseSpec, matrix: &mut Matrix) {
    if spec.guarantees.backup.enabled {
        matrix.operator_managed("guarantees.backup.enabled", BACKUP_RUNBOOK);
    } else {
        matrix.not_requested("guarantees.backup.enabled");
    }
    if spec.guarantees.backup.pitr {
        matrix.operator_managed("guarantees.backup.pitr", PITR_RUNBOOK);
    } else {
        matrix.not_requested("guarantees.backup.pitr");
    }
}

/// Audit is enforced; without a configured key its hash chain uses the development key.
fn audit_guarantee(inputs: &GuaranteeInputs<'_>, matrix: &mut Matrix) {
    const PATH: &str = "guarantees.audit.enabled";
    if !inputs.spec.guarantees.audit.enabled {
        matrix.not_requested(PATH);
        return;
    }
    if inputs.audit_key_configured {
        matrix.push(
            PATH,
            "true",
            EnforcementStatus::Enforced,
            "audit events are hash-chained with the configured audit key",
            "",
        );
        return;
    }
    let remediation = "On the Rust API, set ProvisionEnvironment.audit to an AuditConfig with a \
                       key file; the CLI, serve and the Control Plane use the development key in \
                       this build.";
    matrix.push(
        PATH,
        "true",
        EnforcementStatus::EnforcedWithDevelopmentKey,
        "audit events are recorded and hash-chained with the built-in development key",
        remediation,
    );
    matrix.issues.push(issue(
        "AUDIT_DEVELOPMENT_KEY",
        ValidationSeverity::Warning,
        PATH,
        "audit is enforced, but its hash chain uses the built-in development key, so anyone \
         with this build can recompute it",
        remediation,
    ));
}

/// Domains hold trivially on one CP replica; cache and event-log role constraints are not
/// enforced.
fn domain_and_role_guarantees(spec: &DatabaseSpec, matrix: &mut Matrix) {
    for (index, domain) in spec.domains.iter().enumerate() {
        let path = format!("domains[{index}].mode");
        match domain.mode {
            ConsistencyMode::LocalSnapshot | ConsistencyMode::StrongCp => matrix.push(
                &path,
                &json_slug(&domain.mode),
                EnforcementStatus::TriviallySatisfied,
                "one CP replica serves every read and write; the domain is recorded as metadata",
                "",
            ),
            ConsistencyMode::EventualAp => matrix.push(
                &path,
                "eventual_ap",
                EnforcementStatus::NotEnforceable,
                "eventual AP domains need AP replication, which provisioning does not build",
                "Use local_snapshot or strong_cp.",
            ),
        }
    }
    for (index, collection) in spec.collections.iter().enumerate() {
        let constraint = match collection.role {
            CollectionRole::Cache => "TTL and eviction",
            CollectionRole::EventLog => "append-only writes",
            _ => continue,
        };
        let path = format!("collections[{index}].role");
        let detail = format!(
            "collection {} has role {}, but this build does not enforce {constraint}",
            collection.name,
            json_slug(&collection.role)
        );
        matrix.issues.push(issue(
            "ROLE_CONSTRAINT_NOT_ENFORCED",
            ValidationSeverity::Warning,
            &path,
            &detail,
            "Enforce the constraint in the application.",
        ));
        matrix.push(
            &path,
            constraint,
            EnforcementStatus::NotEnforced,
            &detail,
            "Enforce the constraint in the application.",
        );
    }
}

fn at_rest_status(inputs: &GuaranteeInputs<'_>, matrix: &mut Matrix) -> EnforcementStatus {
    const PATH: &str = "guarantees.encryption.at_rest";
    let requested = inputs.spec.guarantees.encryption.at_rest;
    match (requested, inputs.existing_encrypted) {
        (true, Some(true)) => {
            matrix.push(
                PATH,
                "true",
                EnforcementStatus::Enforced,
                "the database is encrypted at rest with the keys this handle was opened with",
                "",
            );
            EnforcementStatus::Enforced
        }
        (true, Some(false)) | (false, Some(true)) => {
            let (status, detail) = if requested {
                (
                    EnforcementStatus::NotEnforceable,
                    "the database is not encrypted at rest, and provisioning cannot encrypt an \
                     existing database",
                )
            } else {
                (
                    EnforcementStatus::NotRequested,
                    "the database is encrypted at rest, but the spec says at_rest: false, and \
                     provisioning cannot decrypt an existing database",
                )
            };
            let remediation = "Set guarantees.encryption.at_rest to match the database, or build \
                               a new database and move the data.";
            matrix.push(PATH, &requested.to_string(), status, detail, remediation);
            matrix.issues.push(issue(
                "PROVISION_ENCRYPTION_MISMATCH",
                ValidationSeverity::Error,
                PATH,
                detail,
                remediation,
            ));
            status
        }
        (true, None) if inputs.new_target_encrypted => {
            matrix.push(
                PATH,
                "true",
                EnforcementStatus::Enforced,
                "the new database is created with the encryption config of the environment",
                "",
            );
            EnforcementStatus::Enforced
        }
        (true, None) => {
            let detail = "no encryption config was supplied for the new database";
            let remediation = match inputs.surface {
                ProvisionSurface::RustApi => {
                    "Set ProvisionEnvironment.encryption to an EncryptionConfig backed by an operator-provided key."
                }
                ProvisionSurface::Cli | ProvisionSurface::Serve => {
                    "Set MULTIDB_ENCRYPTION_KEY_PATH to an existing encryption key file, or supply ProvisionEnvironment.encryption when embedding this surface."
                }
                ProvisionSurface::ControlPlane => {
                    "Create the encrypted database through the Rust API or CLI with an operator-provided key, then serve that database."
                }
            };
            matrix.push(
                PATH,
                "true",
                EnforcementStatus::NotEnforceable,
                detail,
                remediation,
            );
            matrix.issues.push(issue(
                "GUARANTEE_NOT_ENFORCEABLE",
                ValidationSeverity::Error,
                PATH,
                format!("guarantees.encryption.at_rest cannot be enforced: {detail}"),
                remediation,
            ));
            EnforcementStatus::NotEnforceable
        }
        (false, None) if inputs.new_target_encrypted => encrypted_without_at_rest(matrix),
        (false, _) => {
            matrix.not_requested(PATH);
            EnforcementStatus::NotRequested
        }
    }
}

/// Encrypting a new database whose spec says `at_rest: false` would leave one that every later
/// plan reports as a mismatch, so it is refused before anything is created.
fn encrypted_without_at_rest(matrix: &mut Matrix) -> EnforcementStatus {
    const PATH: &str = "guarantees.encryption.at_rest";
    let detail = "ProvisionEnvironment.encryption would encrypt the new database, but the spec \
                  says at_rest: false";
    let remediation = "Set guarantees.encryption.at_rest to true, or leave \
                       ProvisionEnvironment.encryption unset.";
    matrix.push(
        PATH,
        "false",
        EnforcementStatus::NotRequested,
        detail,
        remediation,
    );
    matrix.issues.push(issue(
        "PROVISION_ENCRYPTION_MISMATCH",
        ValidationSeverity::Error,
        PATH,
        detail,
        remediation,
    ));
    EnforcementStatus::NotRequested
}

#[derive(Default)]
struct Matrix {
    entries: Vec<GuaranteeEnforcement>,
    issues: Vec<ValidationIssue>,
}

impl Matrix {
    fn push(
        &mut self,
        path: &str,
        requested: &str,
        status: EnforcementStatus,
        detail: &str,
        remediation: &str,
    ) {
        self.entries.push(GuaranteeEnforcement {
            path: path.to_owned(),
            requested: requested.to_owned(),
            status,
            detail: detail.to_owned(),
            remediation: remediation.to_owned(),
        });
    }

    fn not_requested(&mut self, path: &str) {
        self.push(
            path,
            "false",
            EnforcementStatus::NotRequested,
            "the spec does not ask for it",
            "",
        );
    }

    fn operator_managed(&mut self, path: &str, runbook: &str) {
        let detail = format!("{path} is provided by the operator, not by the runtime");
        self.push(
            path,
            "true",
            EnforcementStatus::OperatorManaged,
            &detail,
            runbook,
        );
        self.issues.push(issue(
            "GUARANTEE_OPERATOR_MANAGED",
            ValidationSeverity::Warning,
            path,
            &detail,
            runbook,
        ));
    }
}

/// The `snake_case` spelling of a spec enum value.
pub(super) fn json_slug<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(slug)) => slug,
        Ok(other) => other.to_string(),
        Err(error) => format!("<{error}>"),
    }
}
