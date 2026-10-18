//! Provisioning plans: what it takes to converge a target to a spec.
//!
//! [`plan_provision`] compiles the spec, checks that the calling surface and the target can
//! provision it, compares the resolved profile, the projected catalog objects, audit, RBAC,
//! guarantees and metadata with the live database (or with nothing, for an absent target) and
//! classifies every resulting step. Steps run in a fixed phase order; weakening and destructive
//! steps need a per-path flag and the storage format upgrade needs `allow_format_upgrade`. The
//! `prov_` id hashes everything the plan depends on, so an apply that recomputes the plan can
//! tell whether the confirmed plan is still the current one. Planning never writes.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::guarantees::{self, EnforcementStatus, GuaranteeEnforcement, GuaranteeInputs};
use super::live::LiveState;
use super::physical::{self, DesiredKind, DesiredObject, entry_kind, live_params_json};
use super::record::{ObjectKind, ProvisionRecord, ProvisionRecordStatus, RecordState};
use super::{ProvisionError, issue};
use crate::config_spec::{
    ChangePolicy, CollectionRoleSpec, ConflictResolution, ConsistencyMode, DatabaseSpec,
    DeploymentMode, ExtensionStability, GrantScope, ImpactLevel, MigrationPlan, MigrationPlanner,
    PermissionSpec, PhysicalObjectSpec, PlanImpact, PolicyCompiler, RESERVED_PRINCIPAL_NAMES,
    RESERVED_ROLE_NAMES, ReplicationMode, ResolvedProfile, RiskLevel, RollbackPlan, SecuritySpec,
    StorageMedium, SupportStatus, TopologySpec, UnmanagedPolicy, ValidationIssue, ValidationReport,
    ValidationSeverity, WriteAck, built_in_extension_manifest, canonical_spec_json, hex_prefix,
    impact_none, knob_definition, low_risk_impact, medium_impact, merge_impact, spec_digest,
};
use crate::db::{
    AuditConfig, CatalogEntry, Database, EncryptionConfig, EncryptionMode, MetadataProbe,
    PersistedProfileManifest, Profile, ReplicationKind, SCHEMA_VERSION, SCHEMA_VERSION_FRAMED,
    inspect_database_metadata, performance_for_manifest,
};
use crate::graph::GraphId;
use crate::model::{CollectionId, IndexSpec};
use crate::performance::{CompressionAlgorithm, PerformanceConfig};
use crate::query::{RelIndexExpression, RelIndexSpec, TableLayout};
use crate::security::{AuthzPolicy, Permission, Principal, PrincipalRegistry, Resource, Role};

/// The `ProvisionPlan.plan_format` this build writes.
pub const PLAN_FORMAT: u32 = 1;
/// The target path a volatile (in-memory) target is hashed as.
const VOLATILE_TARGET: &str = "volatile";

/// The surface a plan is computed for. It is part of the `prov_` id, so an id computed on one
/// surface never confirms an apply on another.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProvisionSurface {
    RustApi,
    Cli,
    Serve,
    ControlPlane,
}

impl ProvisionSurface {
    /// The surface as a lowercase slug: `rust_api`, `cli`, `serve` or `control_plane`.
    #[must_use]
    pub const fn slug(self) -> &'static str {
        match self {
            Self::RustApi => "rust_api",
            Self::Cli => "cli",
            Self::Serve => "serve",
            Self::ControlPlane => "control_plane",
        }
    }
}

/// What a plan is computed against.
#[derive(Clone, Copy)]
pub enum ProvisionTargetRef<'a> {
    /// A database file that does not exist yet, or an empty file. A path that already holds a
    /// database is refused with `ProvisionError::AlreadyInitialized`: open it and plan with
    /// `Open`, so a dry run never opens a database implicitly.
    Path(&'a Path),
    /// An open single-node database.
    Open(&'a Database),
    /// A new in-memory database (Rust API only).
    Volatile,
}

/// Per-run options of a plan. They are never stored in a spec, and they are part of the
/// `prov_` id.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProvisionOptions {
    /// Spec paths whose weakening steps may run, for example `guarantees.audit.enabled` or
    /// `security.roles.order_writer`.
    pub allow_weakening: BTreeSet<String>,
    /// Spec paths whose destructive steps may run, for example `collections.orders`.
    pub allow_destructive: BTreeSet<String>,
    /// Allows the forward-only storage format upgrade (value framing and `schema_version` 2).
    pub allow_format_upgrade: bool,
    /// Overrides `provisioning.unmanaged` of the spec.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unmanaged: Option<UnmanagedPolicy>,
}

/// Where a plan runs and with what. Only `surface` is part of the `prov_` id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProvisionEnvironment {
    pub surface: ProvisionSurface,
    /// Encryption for a new database: Rust API only, `EncryptionMode::LocalEnvelope` only. With
    /// an open database it must equal the config the database was opened with.
    pub encryption: Option<EncryptionConfig>,
    /// The audit key and anchor of a new database. With an open database it must equal the
    /// audit config of the handle.
    pub audit: Option<AuditConfig>,
    /// The principal that will apply the plan, checked for `RBAC_SELF_LOCKOUT`.
    pub principal: Option<Principal>,
}

impl ProvisionEnvironment {
    /// An environment for `surface` without encryption, audit config or principal.
    #[must_use]
    pub const fn new(surface: ProvisionSurface) -> Self {
        Self {
            surface,
            encryption: None,
            audit: None,
            principal: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetKind {
    NewDatabase,
    ExistingDatabase,
}

/// The target of a plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedTarget {
    pub kind: TargetKind,
    /// The canonical absolute path; `None` for an in-memory database.
    pub path: Option<String>,
    /// The storage profile the database has (or will have).
    pub storage_profile: Option<Profile>,
    /// The `__meta.schema_version` the database has (or will have).
    pub schema_version: Option<u32>,
}

/// How a catalog object relates to the spec.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectState {
    /// Declared and missing: the plan creates it.
    Create,
    /// Declared, managed and matching: nothing to do.
    Satisfied,
    /// Declared and matching, but not managed yet: the plan takes ownership.
    Adopt,
    /// Declared, but the live object differs: blocks, since there is no alter API.
    Conflict,
    /// Managed but no longer declared: the plan releases it from management, data retained.
    Release,
    /// Live but neither declared nor managed.
    Unmanaged,
}

/// One parameter that differs between the spec and the live object.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldDiff {
    /// The `physical` key, for example `dim` or `document_indexes`.
    pub field: String,
    pub expected: JsonValue,
    pub actual: JsonValue,
}

/// A catalog object of the plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedObject {
    pub name: String,
    /// The spec collection the object comes from; `None` for an undeclared live object.
    pub spec_collection: Option<String>,
    pub kind: ObjectKind,
    pub state: ObjectState,
    /// The parameters in spec vocabulary: the desired ones for a declared object, the live ones
    /// otherwise.
    pub params: JsonValue,
    /// What differs, for a conflict.
    pub diff: Vec<FieldDiff>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProvisionStepKind {
    CreateDatabase,
    AdoptProfile,
    RetuneProfile,
    UpgradeStorageFormat,
    EnableAudit,
    DisableAudit,
    CreateCollection,
    CreateVectorCollection,
    CreateTable,
    CreateTimeSeries,
    CreateGraph,
    AdoptObject,
    CreateFullTextIndex,
    CreateGeoIndex,
    EnableSystemVersioning,
    ApplySecurity,
    ReleaseObject,
    RecordGuarantees,
    RecordMetadata,
    RecordAppliedSpec,
    Noop,
}

/// How risky a step is, and what it needs to run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepSafety {
    /// Creates or adopts something; nothing existing changes.
    Additive,
    /// Changes only records and labels.
    Metadata,
    /// Rewrites session knobs in the profile manifest; takes effect at the next open.
    Retune,
    /// Turns a guarantee on (audit).
    Strengthening,
    /// Weakens a guarantee, a floor or a grant; needs `allow_weakening` for its path.
    Weakening,
    /// Removes a managed role or principal, or releases a managed object; needs
    /// `allow_destructive` for its path.
    Destructive,
    /// The forward-only value-framing upgrade; needs `allow_format_upgrade`.
    FormatUpgrade,
    /// No runtime API performs it; the plan is blocked.
    Unsupported,
}

/// One step of a plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProvisionStep {
    /// `pstep-001`, `pstep-002`, ... in execution order.
    pub step_id: String,
    pub kind: ProvisionStepKind,
    /// The spec path the step comes from, for example `collections.orders` or
    /// `security.roles.order_writer`.
    pub path: String,
    /// The catalog object, role or principal the step acts on.
    pub object: Option<String>,
    pub action: String,
    pub safety: StepSafety,
    pub impact: PlanImpact,
    pub supported: bool,
    /// `allow_weakening:<path>`, `allow_destructive:<path>` or `allow_format_upgrade`.
    pub requires_flag: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanOutcome {
    /// The target already matches the spec.
    Noop,
    /// Every step can run with the given options.
    Ready,
    /// Some steps need flags the options do not give (`STEP_REQUIRES_FLAG`).
    NeedsFlags,
    /// A blocking issue, an unsupported step or a guarantee that cannot be enforced.
    Blocked,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DriftKind {
    MissingObject,
    ObjectConflict,
    UnmanagedObject,
    OrphanedObject,
    ReleasedObject,
    SettingDiffers,
    StorageClassMismatch,
    FormatMismatch,
    EncryptionMismatch,
    GuaranteeNotEnforceable,
    SessionPendingRestart,
    InterruptedApply,
    SpecChanged,
}

/// What resolves a drift item.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DriftResolution {
    Create,
    Adopt,
    /// Applying the spec resolves it.
    Apply,
    Retune,
    Release,
    /// Restarting the process resolves it.
    Restart,
    /// No runtime API resolves it.
    Unsupported,
    /// Information only.
    None,
}

/// One difference between a spec and the live database.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriftItem {
    pub path: String,
    pub kind: DriftKind,
    pub expected: String,
    pub actual: String,
    /// Blocks every apply until it is resolved.
    pub blocking: bool,
    pub resolution: DriftResolution,
}

/// How a live database differs from a spec.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriftReport {
    /// The plan is not blocked and every item is information only (resolution `none`, not
    /// blocking): released objects, unmanaged objects under `report` or `ignore`.
    pub in_sync: bool,
    pub items: Vec<DriftItem>,
}

/// A provisioning plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProvisionPlan {
    pub plan_format: u32,
    /// `prov_` and 32 hex characters.
    pub plan_id: String,
    pub surface: ProvisionSurface,
    pub target: PlannedTarget,
    /// `config_spec::spec_digest` of the spec.
    pub spec_digest: String,
    /// The fingerprint of the live database; `None` for an absent target.
    pub live_fingerprint: Option<String>,
    pub resolved_profile: Option<ResolvedProfile>,
    /// Spec validation; an invalid spec blocks the plan.
    pub validation: ValidationReport,
    /// Provisioning issues: what this surface and target cannot build or enforce, and drift.
    pub issues: Vec<ValidationIssue>,
    pub objects: Vec<PlannedObject>,
    pub guarantees: Vec<GuaranteeEnforcement>,
    pub drift: DriftReport,
    pub steps: Vec<ProvisionStep>,
    /// `MigrationPlanner::plan(last applied spec, spec)`, for the metadata-level diff against
    /// the spec the last finished apply converged to (`ProvisionRecord::last_applied`).
    pub spec_migration: Option<MigrationPlan>,
    pub outcome: PlanOutcome,
    /// Every flag a step of the plan needs, given or not.
    pub required_flags: Vec<String>,
    /// The plan is ready and only creates, adopts, records or strengthens, without RBAC changes
    /// on an existing database and without enabling system versioning on a table it adopts:
    /// `--auto-approve` and `init --apply` accept it.
    pub auto_approvable: bool,
    pub impact: PlanImpact,
    pub rollback: RollbackPlan,
    /// The `plan_id` an apply must confirm; `None` for a noop.
    pub required_confirmation: Option<String>,
}

/// Computes the provisioning plan of `spec` against `target`. Never writes: an absent target is
/// only probed, and an open database is only read.
///
/// An invalid spec gives a blocked plan with the report in `validation`.
/// # Errors
/// Fails with `AlreadyInitialized` when a `Path` target already holds a database, `Locked` when
/// another handle holds it, `Io` when it cannot be probed, `Config(Unsupported)` when the
/// environment is not accepted (encryption outside the Rust API or not `LocalEnvelope`, or an
/// encryption or audit config that differs from the open database's) or the open database is
/// not single-node, and `Db` when the live state cannot be read.
pub fn plan_provision(
    spec: &DatabaseSpec,
    target: ProvisionTargetRef<'_>,
    opts: &ProvisionOptions,
    env: &ProvisionEnvironment,
) -> Result<ProvisionPlan, ProvisionError> {
    check_environment(target, env)?;
    let (target, target_path) = resolve_target(target)?;
    let (validation, compiled) = PolicyCompiler::validate_and_compile(spec);
    let mut builder = PlanBuilder {
        spec,
        opts,
        env,
        target,
        validation,
        resolved: None,
        desired_manifest: None,
        desired: Vec::new(),
        issues: Vec::new(),
        objects: Vec::new(),
        steps: Vec::new(),
        drift: Vec::new(),
        guarantees: Vec::new(),
        audit_required: false,
        confirm_only: BTreeSet::new(),
    };
    if let Some(compiled) = compiled {
        builder.build(compiled.resolved_profile)?;
    }
    Ok(builder.finish(target_path.as_deref(), env!("CARGO_PKG_VERSION")))
}

/// Reports how the open database `db` differs from `spec`: the drift of
/// [`plan_provision`] on the Rust API surface.
/// # Errors
/// Fails with `InvalidSpec` when the spec does not validate, and as [`plan_provision`].
pub fn drift(
    db: &Database,
    spec: &DatabaseSpec,
    opts: &ProvisionOptions,
) -> Result<DriftReport, ProvisionError> {
    let plan = plan_provision(
        spec,
        ProvisionTargetRef::Open(db),
        opts,
        &ProvisionEnvironment::new(ProvisionSurface::RustApi),
    )?;
    if !plan.validation.valid {
        return Err(ProvisionError::InvalidSpec(Box::new(plan.validation)));
    }
    Ok(plan.drift)
}

/// Everything a `prov_` id hashes besides the binary version.
pub(crate) struct PlanIdInputs<'a> {
    pub spec: &'a DatabaseSpec,
    /// `None` for an absent target.
    pub live_fingerprint: Option<&'a str>,
    pub options: &'a ProvisionOptions,
    pub surface: ProvisionSurface,
    /// The canonical target path; `None` for an in-memory target.
    pub target_path: Option<&'a str>,
}

/// `prov_` and 32 hex characters of blake3("multidb-provision-plan-v1" ‖ canonical spec ‖ live
/// fingerprint or "absent" ‖ canonical options ‖ surface ‖ canonical target path or "volatile" ‖
/// version). Every part is length-prefixed, so no two different inputs hash alike.
pub(crate) fn plan_id_for(inputs: &PlanIdInputs<'_>, version: &str) -> String {
    let spec = canonical_spec_json(inputs.spec);
    let options = serde_json::to_string(inputs.options)
        .unwrap_or_else(|error| format!("<unserializable options: {error}>"));
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"multidb-provision-plan-v1");
    for part in [
        spec.as_str(),
        inputs.live_fingerprint.unwrap_or("absent"),
        options.as_str(),
        inputs.surface.slug(),
        inputs.target_path.unwrap_or(VOLATILE_TARGET),
        version,
    ] {
        hasher.update(&u64::try_from(part.len()).unwrap_or(u64::MAX).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    format!("prov_{}", hex_prefix(hasher.finalize().as_bytes(), 16))
}

/// The canonical spelling of a target path: the canonicalized path when the file exists, else
/// the canonicalized parent joined with the file name, else the absolute path. Symlinked and
/// relative spellings of one file therefore give one `prov_` id.
pub(crate) fn canonical_target_path(path: &Path) -> String {
    if let Ok(canonical) = std::fs::canonicalize(path) {
        return canonical.to_string_lossy().into_owned();
    }
    if let Some(name) = path.file_name() {
        let parent = match path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent,
            _ => Path::new("."),
        };
        if let Ok(parent) = std::fs::canonicalize(parent) {
            return parent.join(name).to_string_lossy().into_owned();
        }
    }
    std::path::absolute(path)
        .unwrap_or_else(|_| path.to_path_buf())
        .to_string_lossy()
        .into_owned()
}

fn check_environment(
    target: ProvisionTargetRef<'_>,
    env: &ProvisionEnvironment,
) -> Result<(), ProvisionError> {
    let unsupported = |message: &str| -> Result<(), ProvisionError> {
        Err(ProvisionError::Config(crate::db::ConfigError::Unsupported(
            message.to_owned(),
        )))
    };
    if let Some(encryption) = &env.encryption {
        if env.surface != ProvisionSurface::RustApi {
            return unsupported(
                "ProvisionEnvironment.encryption is accepted on the rust_api surface only; the \
                 CLI, serve and the Control Plane cannot encrypt a database in this build",
            );
        }
        if encryption.mode == EncryptionMode::LegacyFile {
            return unsupported(
                "provisioned databases refuse LegacyFile encryption; use \
                 EncryptionConfig::local_envelope",
            );
        }
        if let ProvisionTargetRef::Open(db) = target
            && db.encryption_config() != Some(encryption)
        {
            return unsupported(
                "ProvisionEnvironment.encryption differs from the encryption config the database \
                 was opened with",
            );
        }
    }
    if let (Some(audit), ProvisionTargetRef::Open(db)) = (&env.audit, target)
        && db.audit_config() != audit
    {
        return unsupported(
            "ProvisionEnvironment.audit differs from the audit config of the open database; \
             open_database_with_ops does not apply OperationalConfig.security.audit, so open the \
             database with open_from_spec or leave ProvisionEnvironment.audit unset",
        );
    }
    Ok(())
}

fn resolve_target(
    target: ProvisionTargetRef<'_>,
) -> Result<(Target<'_>, Option<String>), ProvisionError> {
    match target {
        ProvisionTargetRef::Path(path) => match inspect_database_metadata(path) {
            MetadataProbe::NotFound | MetadataProbe::Uninitialized => Ok((
                Target::New {
                    path: Some(path.to_path_buf()),
                },
                Some(canonical_target_path(path)),
            )),
            MetadataProbe::Initialized(_) => Err(ProvisionError::AlreadyInitialized),
            MetadataProbe::Locked => Err(ProvisionError::Locked(format!(
                "{} is held open by another handle",
                path.display()
            ))),
            MetadataProbe::NeedsRecovery => Err(ProvisionError::Io(format!(
                "{} was not closed cleanly and needs crash recovery; open it, which recovers it, \
                 and plan against the open database",
                path.display()
            ))),
            MetadataProbe::Unreadable { reason } => Err(ProvisionError::Io(format!(
                "{} cannot be probed: {reason}",
                path.display()
            ))),
        },
        ProvisionTargetRef::Open(db) => {
            let live = LiveState::capture(db)?;
            let path = db.config().path.as_deref().map(canonical_target_path);
            Ok((
                Target::Existing {
                    db,
                    live: Box::new(live),
                },
                path,
            ))
        }
        ProvisionTargetRef::Volatile => Ok((Target::New { path: None }, None)),
    }
}

enum Target<'a> {
    /// An absent target; `path` is `None` for an in-memory database.
    New { path: Option<PathBuf> },
    Existing {
        db: &'a Database,
        live: Box<LiveState>,
    },
}

/// The execution order of steps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Phase {
    Database,
    AuditEnable,
    Containers,
    Dependents,
    Profile,
    Security,
    Release,
    Metadata,
    AppliedSpec,
    AuditDisable,
}

struct Queued {
    phase: Phase,
    /// The order within the phase.
    key: (u8, u8, usize),
    step: ProvisionStep,
}

struct PlanBuilder<'a> {
    spec: &'a DatabaseSpec,
    opts: &'a ProvisionOptions,
    env: &'a ProvisionEnvironment,
    target: Target<'a>,
    validation: ValidationReport,
    resolved: Option<ResolvedProfile>,
    desired_manifest: Option<PersistedProfileManifest>,
    desired: Vec<DesiredObject>,
    issues: Vec<ValidationIssue>,
    objects: Vec<PlannedObject>,
    steps: Vec<Queued>,
    drift: Vec<DriftItem>,
    guarantees: Vec<GuaranteeEnforcement>,
    /// Audit is off and stays off: any step blocks the plan with `AUDIT_REQUIRED_FOR_APPLY`.
    audit_required: bool,
    /// Paths of steps that change an existing object provisioning does not manage yet (system
    /// versioning of an adopted table): they need a typed confirmation, never auto-approval.
    confirm_only: BTreeSet<String>,
}

impl PlanBuilder<'_> {
    fn build(&mut self, resolved: Option<ResolvedProfile>) -> Result<(), ProvisionError> {
        let Some(resolved) = resolved else {
            self.issues.push(issue(
                "PROVISION_PROFILE_UNRESOLVED",
                ValidationSeverity::Error,
                "profile",
                format!(
                    "profile {} does not resolve to built-in knobs, so provisioning cannot \
                     build it",
                    self.spec.profile
                ),
                "Use a built-in profile slug or alias, or a version 2 custom_profile that \
                 extends one; multidb config upgrade converts Studio custom profiles.",
            ));
            return Ok(());
        };
        match desired_manifest(&resolved) {
            Ok(manifest) => self.desired_manifest = Some(manifest),
            Err(error) => self.issues.push(issue(
                "PROVISION_PROFILE_UNRESOLVED",
                ValidationSeverity::Error,
                "profile",
                format!(
                    "profile {} cannot be bound to a database: {error}",
                    resolved.slug
                ),
                "Use a profile with a single-node storage class.",
            )),
        }
        self.check_scope();
        self.check_extensions();
        self.check_medium(&resolved);
        self.check_record();
        self.plan_database(&resolved);
        // Without a projection nothing can be matched, so the plan (blocked by the projection
        // issues) leaves undeclared objects and RBAC alone instead of planning bogus changes.
        let projected = self.plan_objects(&resolved)?;
        if projected {
            self.plan_unmanaged();
            self.plan_forgotten_orphans();
        }
        self.plan_audit();
        if projected {
            self.plan_security();
        }
        self.plan_guarantees();
        self.plan_metadata();
        if !self.steps.is_empty() {
            self.check_audit_chain()?;
        }
        self.resolved = Some(resolved);
        Ok(())
    }

    /// An apply appends audit events chained with the key of this handle. When the newest event
    /// of an existing database does not verify under that key, the appended events would break
    /// `verify_audit_chain`: `PROVISION_AUDIT_KEY_MISMATCH` blocks the plan. Only plans with
    /// steps are checked, since a noop writes nothing; the check reads the whole audit log.
    fn check_audit_chain(&mut self) -> Result<(), ProvisionError> {
        let Target::Existing { db, live } = &self.target else {
            return Ok(());
        };
        if db.audit_tail_matches_key()? != Some(false) {
            return Ok(());
        }
        let message = if live.audit_key_configured {
            "the newest audit event does not verify under the audit key this handle was given: \
             the chain was written with another key, or its head event is missing, so the \
             events an apply appends would break verify_audit_chain"
        } else {
            "the newest audit event does not verify under the built-in development key this \
             handle chains audit events with: the chain was written with a configured audit \
             key, or its head event is missing, so the events an apply appends would break \
             verify_audit_chain"
        };
        let suggestion = if self.env.surface == ProvisionSurface::RustApi {
            "Open the database with provision::open_from_spec and set \
             ProvisionEnvironment.audit to the AuditConfig whose key the chain was written with."
        } else {
            "Apply the spec on the Rust API with provision::open_from_spec and the chain's audit \
             key in ProvisionEnvironment.audit; the CLI, serve and the Control Plane have no \
             audit key setting in this build."
        };
        self.error(
            "PROVISION_AUDIT_KEY_MISMATCH",
            "guarantees.audit.enabled",
            message.to_owned(),
            suggestion,
        );
        Ok(())
    }

    fn live(&self) -> Option<&LiveState> {
        match &self.target {
            Target::Existing { live, .. } => Some(live),
            Target::New { .. } => None,
        }
    }

    fn record(&self) -> Option<&ProvisionRecord> {
        self.live().and_then(|live| live.record.readable())
    }

    fn queue(&mut self, phase: Phase, key: (u8, u8, usize), step: ProvisionStep) {
        self.steps.push(Queued { phase, key, step });
    }

    fn error(&mut self, code: &str, path: impl Into<String>, message: String, suggestion: &str) {
        self.issues.push(issue(
            code,
            ValidationSeverity::Error,
            path,
            message,
            suggestion,
        ));
    }

    /// Deployment, replication and domain rules: provisioning builds embedded and single-node CP
    /// databases with one replica and one shard.
    fn check_scope(&mut self) {
        let spec = self.spec;
        if spec.deployment.mode == DeploymentMode::Cluster {
            self.error(
                "PROVISION_DEPLOYMENT_UNSUPPORTED",
                "deployment.mode",
                "provisioning builds embedded and single-node databases only; cluster \
                 deployments are not supported, and the embedded Database cluster \
                 constructors fail closed in this build"
                    .to_owned(),
                "Use deployment.mode embedded or single_node; run a replicated CP cluster with \
                 repl::start_cp_cluster, outside provisioning.",
            );
        }
        if spec.topology != TopologySpec::default() {
            self.error(
                "PROVISION_DEPLOYMENT_UNSUPPORTED",
                "topology",
                format!(
                    "provisioning builds one replica and one shard; the spec asks for {} \
                     replicas and {} shards",
                    spec.topology.replica_count, spec.topology.shard_count
                ),
                "Set topology to 1 replica and 1 shard, or remove it; multi-node CP \
                 replication runs through repl::start_cp_cluster, outside provisioning.",
            );
        }
        if spec.defaults.replication == ReplicationMode::Ap {
            self.error(
                "PROVISION_REPLICATION_UNSUPPORTED",
                "defaults.replication",
                "provisioning builds CP databases only; AP replication is not supported".to_owned(),
                "Set defaults.replication to cp.",
            );
        }
        if self
            .live()
            .is_some_and(|live| live.metadata.replication == ReplicationKind::Ap)
        {
            self.error(
                "PROVISION_REPLICATION_UNSUPPORTED",
                "defaults.replication",
                "the database uses AP replication, which provisioning does not support".to_owned(),
                "Provision a CP database instead.",
            );
            self.drift.push(drift_item(
                "defaults.replication",
                DriftKind::StorageClassMismatch,
                "cp",
                "ap",
                true,
                DriftResolution::Unsupported,
            ));
        }
        for (index, domain) in spec.domains.iter().enumerate() {
            if domain.mode == ConsistencyMode::EventualAp {
                self.error(
                    "DOMAIN_MODE_INCOMPATIBLE_WITH_REPLICATION",
                    format!("domains[{index}].mode"),
                    format!(
                        "domain {} is eventual_ap, which needs AP replication; provisioning \
                         builds CP databases only",
                        domain.name
                    ),
                    "Use local_snapshot or strong_cp.",
                );
            }
        }
        if spec.guarantees.conflict_resolution != ConflictResolution::None {
            self.error(
                "PROVISION_REPLICATION_UNSUPPORTED",
                "guarantees.conflict_resolution",
                "conflict policies belong to AP replication, which provisioning does not \
                 support"
                    .to_owned(),
                "Set guarantees.conflict_resolution to none.",
            );
        }
    }

    /// Volatile (in-memory) profiles are built on the Rust API only, at a `Volatile` target;
    /// durable ones need a path. The Control Plane provisions the database it serves, so a spec
    /// `storage_path` is only advice there.
    fn check_medium(&mut self, resolved: &ResolvedProfile) {
        let volatile = resolved.knobs.storage.medium == StorageMedium::Volatile;
        let surface = self.env.surface;
        if surface == ProvisionSurface::ControlPlane && self.spec.deployment.storage_path.is_some()
        {
            self.issues.push(issue(
                "PROVISION_STORAGE_PATH_IGNORED",
                ValidationSeverity::Advice,
                "deployment.storage_path",
                "the Control Plane provisions the database it serves and never opens a path \
                 from a request, so deployment.storage_path is not used",
                "Keep deployment.storage_path for the CLI and multidb serve, or remove it.",
            ));
        }
        let new_path = match &self.target {
            Target::New { path } => Some(path.is_some()),
            Target::Existing { .. } => None,
        };
        if volatile && surface != ProvisionSurface::RustApi {
            self.error(
                "PROVISION_VOLATILE_ON_SURFACE",
                "profile",
                format!(
                    "profile {} is volatile (in memory); the {} surface provisions durable \
                     databases only",
                    resolved.slug,
                    surface.slug()
                ),
                "Use a durable profile, or build volatile databases through the Rust API.",
            );
        } else if volatile && new_path == Some(true) {
            self.error(
                "PROFILE_DEPLOYMENT_MISMATCH",
                "deployment.storage_path",
                format!(
                    "profile {} is volatile (in memory) and cannot be built at a path",
                    resolved.slug
                ),
                "Plan volatile specs with ProvisionTargetRef::Volatile.",
            );
        } else if !volatile && new_path == Some(false) {
            self.error(
                "PROVISION_STORAGE_PATH_REQUIRED",
                "deployment.storage_path",
                format!(
                    "profile {} is durable, so the new database needs a path",
                    resolved.slug
                ),
                "Give a database path, or set deployment.storage_path.",
            );
        }
    }

    /// Built-in extensions with a matching version are recorded as metadata; experimental ones
    /// warn; custom or unknown ones block, since provisioning never runs extension code.
    fn check_extensions(&mut self) {
        let spec = self.spec;
        for (index, extension) in spec.extensions.iter().enumerate() {
            let path = format!("extensions[{index}]");
            match built_in_extension_manifest(&extension.name) {
                None => self.error(
                    "PROVISION_EXTENSION_UNAVAILABLE",
                    format!("{path}.name"),
                    format!(
                        "extension {} is not a built-in extension; provisioning only uses \
                         built-in extension manifests",
                        extension.name
                    ),
                    "Remove the extension, or use a built-in one (multidb extension list).",
                ),
                Some(manifest) if manifest.version != extension.version => self.error(
                    "EXTENSION_VERSION_INCOMPATIBLE",
                    format!("{path}.version"),
                    format!(
                        "extension {} is version {} in this build, but the spec asks for {}",
                        extension.name, manifest.version, extension.version
                    ),
                    "Set the version of the built-in manifest.",
                ),
                Some(manifest) => {
                    if extension.stability == ExtensionStability::Experimental
                        || manifest.status == SupportStatus::Experimental
                    {
                        self.issues.push(issue(
                            "PROVISION_EXTENSION_EXPERIMENTAL",
                            ValidationSeverity::Warning,
                            path,
                            format!(
                                "extension {} is experimental; provisioning records it, but \
                                 its behaviour may change between builds",
                                extension.name
                            ),
                            "Accept experimental behaviour, or remove the extension.",
                        ));
                    }
                }
            }
        }
    }

    /// An unreadable record blocks; an interrupted apply is reported.
    fn check_record(&mut self) {
        let Target::Existing { live, .. } = &self.target else {
            return;
        };
        match &live.record {
            RecordState::Unreadable(reason) => {
                self.issues.push(issue(
                    "PROVISION_RECORD_UNREADABLE",
                    ValidationSeverity::Error,
                    "database.record",
                    format!("the provision record of the database is unreadable: {reason}"),
                    "Use the build that wrote the record, or restore a backup taken before it.",
                ));
                self.drift.push(drift_item(
                    "database.record",
                    DriftKind::SettingDiffers,
                    "a readable provision record",
                    reason,
                    true,
                    DriftResolution::Unsupported,
                ));
            }
            RecordState::Readable(record) if record.status != ProvisionRecordStatus::Applied => {
                let status = if record.status == ProvisionRecordStatus::Failed {
                    format!(
                        "failed at {}",
                        record.failed_step.as_deref().unwrap_or("an unknown step")
                    )
                } else {
                    "in progress".to_owned()
                };
                self.issues.push(issue(
                    "DRIFT_INTERRUPTED_APPLY",
                    ValidationSeverity::Warning,
                    "database.record",
                    format!(
                        "the last apply ({}) did not finish: {status}; {} of its steps \
                         completed, and this plan holds only what remains",
                        record.plan_id,
                        record.completed_steps.len()
                    ),
                    "Review the remaining steps and apply them.",
                ));
                self.drift.push(drift_item(
                    "database.record",
                    DriftKind::InterruptedApply,
                    "applied",
                    &status,
                    false,
                    DriftResolution::Apply,
                ));
            }
            RecordState::None | RecordState::Readable(_) => {}
        }
    }

    /// A new database is one `CreateDatabase` step. An existing one must have the storage class
    /// of the spec; then its profile manifest is compared with the desired one.
    fn plan_database(&mut self, resolved: &ResolvedProfile) {
        if matches!(self.target, Target::New { .. }) {
            self.plan_new_database(resolved);
        } else {
            self.plan_existing_database(resolved);
        }
    }

    fn plan_new_database(&mut self, resolved: &ResolvedProfile) {
        let Target::New { path } = &self.target else {
            return;
        };
        let framed = self
            .desired_manifest
            .as_ref()
            .is_some_and(|manifest| manifest.storage_format.value_framing);
        let location = path.as_ref().map_or_else(
            || "in memory".to_owned(),
            |path| format!("at {}", path.display()),
        );
        let action = format!(
            "create a new {:?} database {location} with profile {} and record the resolved \
             profile in __meta.profile_manifest (schema_version {})",
            resolved.storage_profile,
            resolved.slug,
            if framed {
                SCHEMA_VERSION_FRAMED
            } else {
                SCHEMA_VERSION
            }
        );
        self.drift.push(drift_item(
            "database",
            DriftKind::MissingObject,
            "a database",
            "no database",
            false,
            DriftResolution::Create,
        ));
        self.queue(
            Phase::Database,
            (0, 0, 0),
            step(
                ProvisionStepKind::CreateDatabase,
                "database",
                None,
                action,
                StepSafety::Additive,
            ),
        );
    }

    fn plan_existing_database(&mut self, resolved: &ResolvedProfile) {
        let Target::Existing { live, .. } = &self.target else {
            return;
        };
        let desired_layout = TableLayout::from(resolved.knobs.storage.layout);
        let mut mismatches = Vec::new();
        if live.metadata.storage_profile != resolved.storage_profile {
            mismatches.push((
                "storage profile",
                format!("{:?}", resolved.storage_profile),
                format!("{:?}", live.metadata.storage_profile),
            ));
        }
        if live.metadata.layout != desired_layout {
            mismatches.push((
                "layout",
                format!("{desired_layout:?}"),
                format!("{:?}", live.metadata.layout),
            ));
        }
        let manifest_plan = if mismatches.is_empty() {
            self.desired_manifest
                .as_ref()
                .map(|desired| plan_manifest(live, desired))
                .unwrap_or_default()
        } else {
            ManifestPlan::default()
        };
        for (what, expected, actual) in mismatches {
            self.issues.push(issue(
                "PROVISION_ENGINE_MISMATCH",
                ValidationSeverity::Error,
                "profile",
                format!(
                    "the spec needs {what} {expected}, but the database has {actual}; there is \
                     no reprofile or storage-class switch"
                ),
                "Use a profile with the storage class of the database, or build a new database \
                 and move the data.",
            ));
            self.drift.push(drift_item(
                "profile",
                DriftKind::StorageClassMismatch,
                &expected,
                &actual,
                true,
                DriftResolution::Unsupported,
            ));
            self.steps.push(Queued {
                phase: Phase::Profile,
                key: (0, 0, 0),
                step: step(
                    ProvisionStepKind::RetuneProfile,
                    "profile",
                    None,
                    format!("change the {what} from {actual} to {expected}"),
                    StepSafety::Unsupported,
                ),
            });
        }
        self.issues.extend(manifest_plan.issues);
        self.drift.extend(manifest_plan.drift);
        for (index, planned) in manifest_plan.steps.into_iter().enumerate() {
            self.queue(Phase::Profile, (0, 0, index), planned);
        }
    }

    /// Projects the spec and matches every desired object against the live catalog and the
    /// record. `false` when the spec does not project.
    fn plan_objects(&mut self, resolved: &ResolvedProfile) -> Result<bool, ProvisionError> {
        let desired = match physical::project(self.spec, resolved) {
            Ok(desired) => desired,
            Err(issues) => {
                self.issues.extend(issues);
                return Ok(false);
            }
        };
        let counts = self.source_counts(&desired)?;
        if let Target::Existing { live, .. } = &mut self.target {
            live.source_row_counts.clone_from(&counts);
        }
        for (index, object) in desired.iter().enumerate() {
            self.plan_object(index, object, &counts);
        }
        self.desired = desired;
        Ok(true)
    }

    /// Document counts of existing collections that get a new full-text or geo index.
    fn source_counts(
        &self,
        desired: &[DesiredObject],
    ) -> Result<BTreeMap<String, u64>, ProvisionError> {
        let Target::Existing { db, live } = &self.target else {
            return Ok(BTreeMap::new());
        };
        let mut counts = BTreeMap::new();
        for object in desired {
            let (DesiredKind::FullText { source, .. } | DesiredKind::Geo { source, .. }) =
                &object.kind
            else {
                continue;
            };
            if live.catalog.contains_key(&object.name) || counts.contains_key(source) {
                continue;
            }
            if let Some(CatalogEntry::Collection { collection_id, .. }) = live.catalog.get(source) {
                counts.insert(source.clone(), db.document_count(*collection_id)?);
            }
        }
        Ok(counts)
    }

    fn plan_object(
        &mut self,
        index: usize,
        object: &DesiredObject,
        counts: &BTreeMap<String, u64>,
    ) {
        let (phase, key) = object_order(index, object);
        let path = object_path(&object.spec_collection, &object.name);
        let physical = collection_spec(self.spec, &object.spec_collection)
            .and_then(|collection| collection.physical.as_ref());
        let (live, record) = match &self.target {
            Target::Existing { live, .. } => (Some(&**live), live.record.readable()),
            Target::New { .. } => (None, None),
        };
        let managed = record.and_then(|record| record.objects.get(&object.name));
        let mut outcome =
            match live.and_then(|live| live.catalog.get(&object.name).map(|entry| (live, entry))) {
                Some((live, entry)) => {
                    let diff = object_diff(
                        object,
                        physical,
                        entry,
                        live,
                        managed.and_then(|managed| managed.allocated_id),
                    );
                    existing_object_outcome(object, &path, diff, managed.is_some())
                }
                None => missing_object_outcome(object, &path, live, managed.is_some(), counts),
            };
        if let DesiredKind::History { base_table, .. } = &object.kind
            && outcome.state == ObjectState::Create
            && self
                .objects
                .iter()
                .any(|planned| planned.name == *base_table && planned.state == ObjectState::Adopt)
            && let Some(planned_step) = &mut outcome.step
        {
            planned_step.action = format!(
                "{}; {base_table} is an existing table this plan adopts, and versioning \
                 changes how it records writes, so the step needs a typed confirmation and is \
                 never auto-approved",
                planned_step.action
            );
            self.confirm_only.insert(planned_step.path.clone());
        }
        self.issues.extend(outcome.issues);
        self.drift.extend(outcome.drift);
        self.objects.push(PlannedObject {
            name: object.name.clone(),
            spec_collection: Some(object.spec_collection.clone()),
            kind: object.kind.object_kind(),
            state: outcome.state,
            params: object.params_json(),
            diff: outcome.diff,
        });
        if let Some(planned_step) = outcome.step {
            self.queue(phase, key, planned_step);
        }
    }

    /// Live objects the spec does not declare: managed ones are released (data retained),
    /// released ones are information, the rest follow the unmanaged policy. Managed objects
    /// missing from both the spec and the database are forgotten.
    fn plan_unmanaged(&mut self) {
        let Target::Existing { live, .. } = &self.target else {
            return;
        };
        let desired = self
            .desired
            .iter()
            .map(|object| object.name.as_str())
            .collect::<BTreeSet<_>>();
        let record = live.record.readable();
        let policy = self
            .opts
            .unmanaged
            .or_else(|| self.spec.provisioning.map(|policy| policy.unmanaged))
            .unwrap_or_default();
        for (position, (name, entry)) in live.catalog.iter().enumerate() {
            if desired.contains(name.as_str()) {
                continue;
            }
            let kind = entry_kind(entry);
            let params = live_params_json(name, entry, &live.catalog, &live.table_schemas);
            if let Some(managed) = record.and_then(|record| record.objects.get(name)) {
                let (planned, item, object) = release(name, &managed.spec_collection, kind, params);
                self.steps.push(Queued {
                    phase: Phase::Release,
                    key: (0, 0, position),
                    step: planned,
                });
                self.drift.push(item);
                self.objects.push(object);
                continue;
            }
            let released = record.is_some_and(|record| record.released_objects.contains(name));
            if policy == UnmanagedPolicy::Ignore && !released {
                continue;
            }
            let path = format!("database.catalog.{name}");
            if released {
                self.drift.push(drift_item(
                    &path,
                    DriftKind::ReleasedObject,
                    "unmanaged",
                    "released by an earlier apply",
                    false,
                    DriftResolution::None,
                ));
            } else {
                let fail = policy == UnmanagedPolicy::Fail;
                self.issues.push(issue(
                    "DRIFT_UNMANAGED",
                    if fail {
                        ValidationSeverity::Error
                    } else {
                        ValidationSeverity::Warning
                    },
                    path.clone(),
                    format!(
                        "{} {name} exists in the database, but the spec does not declare it and \
                         provisioning does not manage it",
                        kind.slug()
                    ),
                    if fail {
                        "Declare the object in the spec, or set provisioning.unmanaged to report."
                    } else {
                        "Declare the object to adopt it, or leave it unmanaged."
                    },
                ));
                self.drift.push(drift_item(
                    &path,
                    DriftKind::UnmanagedObject,
                    "not present",
                    kind.slug(),
                    fail,
                    DriftResolution::None,
                ));
            }
            self.objects.push(PlannedObject {
                name: name.clone(),
                spec_collection: None,
                kind,
                state: ObjectState::Unmanaged,
                params,
                diff: Vec::new(),
            });
        }
    }

    /// Managed objects missing from both the database and the spec are removed from the
    /// record.
    fn plan_forgotten_orphans(&mut self) {
        let Target::Existing { live, .. } = &self.target else {
            return;
        };
        let Some(record) = live.record.readable() else {
            return;
        };
        let desired = self
            .desired
            .iter()
            .map(|object| object.name.as_str())
            .collect::<BTreeSet<_>>();
        for (position, (name, managed)) in record.objects.iter().enumerate() {
            if live.catalog.contains_key(name) || desired.contains(name.as_str()) {
                continue;
            }
            let path = object_path(&managed.spec_collection, name);
            self.issues.push(issue(
                "DRIFT_ORPHAN",
                ValidationSeverity::Warning,
                path.clone(),
                format!(
                    "{} {name} is managed by provisioning but missing from the database, and \
                     the spec no longer declares it; the plan removes it from the record",
                    managed.kind.slug()
                ),
                "Find out why the object disappeared (a restore of an older backup?).",
            ));
            self.drift.push(drift_item(
                &path,
                DriftKind::OrphanedObject,
                "not managed",
                "managed and missing",
                false,
                DriftResolution::Release,
            ));
            self.steps.push(Queued {
                phase: Phase::Release,
                key: (1, 0, position),
                step: step(
                    ProvisionStepKind::ReleaseObject,
                    path,
                    Some(name),
                    format!(
                        "remove the missing {} {name} from the record",
                        managed.kind.slug()
                    ),
                    StepSafety::Metadata,
                ),
            });
        }
    }

    /// Audit must be on for an apply: off to on is the first step, on to off the last one.
    fn plan_audit(&mut self) {
        const PATH: &str = "guarantees.audit.enabled";
        let wanted = self.spec.guarantees.audit.enabled;
        let floor = self
            .desired_manifest
            .as_ref()
            .is_some_and(|manifest| manifest.knobs.floors.audit);
        let live_enabled = self.live().is_none_or(|live| live.audit_enabled);
        match (live_enabled, wanted) {
            (false, true) => {
                self.drift.push(drift_item(
                    PATH,
                    DriftKind::SettingDiffers,
                    "true",
                    "false",
                    false,
                    DriftResolution::Apply,
                ));
                self.queue(
                    Phase::AuditEnable,
                    (0, 0, 0),
                    step(
                        ProvisionStepKind::EnableAudit,
                        PATH,
                        None,
                        "enable audit before any other change".to_owned(),
                        StepSafety::Strengthening,
                    ),
                );
            }
            (true, false) => {
                if self.live().is_some() {
                    self.drift.push(drift_item(
                        PATH,
                        DriftKind::SettingDiffers,
                        "false",
                        "true",
                        false,
                        DriftResolution::Apply,
                    ));
                }
                let disable = if floor {
                    step(
                        ProvisionStepKind::DisableAudit,
                        PATH,
                        None,
                        "disable audit, which the profile floor floors.audit forbids".to_owned(),
                        StepSafety::Unsupported,
                    )
                } else {
                    step(
                        ProvisionStepKind::DisableAudit,
                        PATH,
                        None,
                        "disable audit after every other step and the completion event".to_owned(),
                        StepSafety::Weakening,
                    )
                    .with_flag(format!("allow_weakening:{PATH}"))
                };
                self.queue(Phase::AuditDisable, (0, 0, 0), disable);
            }
            (false, false) => self.audit_required = true,
            (true, true) => {}
        }
    }

    /// Managed-only RBAC: spec roles and principals replace their managed namesakes, managed
    /// ones the spec dropped are removed, everything else is left alone.
    fn plan_security(&mut self) {
        let (live_policy, live_registry, stale, record) = match &self.target {
            Target::Existing { live, .. } => (
                live.authz.clone(),
                live.principals.clone(),
                live.security_state_stale(),
                live.record.readable().cloned(),
            ),
            Target::New { .. } => (
                AuthzPolicy::default(),
                PrincipalRegistry::default(),
                false,
                None,
            ),
        };
        if stale {
            self.error(
                "PROVISION_SECURITY_STATE_STALE",
                "security",
                "the RBAC policy, principals or audit flag this handle enforces differ from \
                 the ones persisted in the database: they were changed in memory only, with \
                 set_authz_policy or set_principal_registry (as multidb admin serve \
                 --insecure-local-admin does for its local administrator), which do not \
                 persist, so an apply would persist state the database never had"
                    .to_owned(),
                "Persist the change (set_authz_policy_as, set_principal_registry_as or POST \
                 /security) or reopen the database, whose openers load the persisted RBAC \
                 state, and plan again.",
            );
            self.drift.push(drift_item(
                "security",
                DriftKind::SettingDiffers,
                "the persisted RBAC state",
                "a different in-memory RBAC state",
                true,
                DriftResolution::Unsupported,
            ));
        }
        let (managed_roles, managed_users) = managed_security_names(record.as_ref());
        let security = self.spec.security.clone().unwrap_or_default();
        let (mut planned, post_roles) =
            plan_roles(&security, &self.desired, &live_policy, &managed_roles);
        let (principal_changes, post_principals) =
            plan_principals(&security, &live_registry, &managed_users);
        planned.extend(principal_changes);
        self.flag_unmanaged_holders(&security, &post_principals, &mut planned);

        let env = self.env;
        if let Some(actor) = &env.principal
            && !planned.is_empty()
        {
            let post_policy = AuthzPolicy::new(post_roles.into_values());
            let allowed_now = live_policy
                .authorize(actor, &Resource::System, Permission::Admin)
                .is_ok();
            let allowed_after = post_apply_actors(actor, &live_registry, &post_principals)
                .iter()
                .all(|post_actor| {
                    post_policy
                        .authorize(post_actor, &Resource::System, Permission::Admin)
                        .is_ok()
                });
            if allowed_now && !allowed_after {
                self.error(
                    "RBAC_SELF_LOCKOUT",
                    "security",
                    format!(
                        "after this plan, principal {} would no longer have system \
                         administration, so it could not undo the change",
                        actor.name()
                    ),
                    "Keep a grant that gives the applying principal system administration, or \
                     apply as another administrator.",
                );
            }
        }

        self.queue_security_changes(planned);
    }

    /// A role that is created or gains permissions also authorizes every principal that already
    /// lists its name, since authorization resolves roles by name: principals the spec does not
    /// manage (left over from a removed role, or assigned through the admin API) are named in the
    /// step and get the Warning `SECURITY_ROLE_ALREADY_ASSIGNED`.
    fn flag_unmanaged_holders(
        &mut self,
        security: &SecuritySpec,
        post_principals: &BTreeMap<String, Principal>,
        planned: &mut [SecurityChange],
    ) {
        let declared = security
            .principals
            .iter()
            .map(|principal| principal.user.as_str())
            .collect::<BTreeSet<_>>();
        for change in planned.iter_mut().filter(|change| change.widens) {
            let holders = post_principals
                .iter()
                .filter(|(user, principal)| {
                    !declared.contains(user.as_str()) && principal.roles().contains(&change.object)
                })
                .map(|(user, _)| user.as_str())
                .collect::<Vec<_>>();
            if holders.is_empty() {
                continue;
            }
            let holders = holders.join(", ");
            change.action = format!(
                "{}; users the spec does not manage already hold the role and gain its \
                 permissions: {holders}",
                change.action
            );
            self.issues.push(issue(
                "SECURITY_ROLE_ALREADY_ASSIGNED",
                ValidationSeverity::Warning,
                change.path.clone(),
                format!(
                    "role {} gains permissions, and users the spec does not manage already hold \
                     it, so they gain them too: {holders}",
                    change.object
                ),
                "Check that these users should get the role's permissions, or give the role \
                 another name in the spec.",
            ));
        }
    }

    fn queue_security_changes(&mut self, planned: Vec<SecurityChange>) {
        let existing = self.live().is_some();
        for (position, change) in planned.into_iter().enumerate() {
            if change.conflict {
                self.error(
                    "PROVISION_OBJECT_CONFLICT",
                    change.path.clone(),
                    change.action.clone(),
                    "Rename the role or principal in the spec, or align it with the live one \
                     (multidb config export shows it).",
                );
            }
            // A new database has no RBAC state to drift from; its drift is the missing database.
            if existing {
                self.drift.push(drift_item(
                    &change.path,
                    if change.conflict {
                        DriftKind::ObjectConflict
                    } else {
                        DriftKind::SettingDiffers
                    },
                    "as declared",
                    if change.conflict {
                        "unmanaged and different"
                    } else {
                        "different"
                    },
                    change.conflict,
                    if change.conflict {
                        DriftResolution::Unsupported
                    } else if change.safety == StepSafety::Metadata {
                        DriftResolution::Adopt
                    } else {
                        DriftResolution::Apply
                    },
                ));
            }
            let mut planned_step = step(
                ProvisionStepKind::ApplySecurity,
                change.path,
                Some(&change.object),
                change.action,
                change.safety,
            );
            if let Some(flag) = change.flag {
                planned_step = planned_step.with_flag(flag);
            }
            self.queue(Phase::Security, (0, 0, position), planned_step);
        }
    }

    /// The guarantee matrix of the surface and target.
    fn plan_guarantees(&mut self) {
        let (existing_encrypted, audit_key_configured) = match &self.target {
            Target::Existing { live, .. } => {
                (Some(live.encryption_configured), live.audit_key_configured)
            }
            Target::New { .. } => (
                None,
                self.env
                    .audit
                    .as_ref()
                    .is_some_and(|audit| audit.key_path.is_some()),
            ),
        };
        let new_target_encrypted = self.env.encryption.as_ref().is_some_and(|encryption| {
            matches!(encryption.mode, EncryptionMode::LocalEnvelope { .. })
        });
        let (entries, issues) = guarantees::evaluate(&GuaranteeInputs {
            spec: self.spec,
            surface: self.env.surface,
            existing_encrypted,
            new_target_encrypted,
            audit_key_configured,
        });
        for entry in &entries {
            if entry.status == EnforcementStatus::NotEnforceable {
                self.drift.push(drift_item(
                    &entry.path,
                    DriftKind::GuaranteeNotEnforceable,
                    &entry.requested,
                    "not enforceable",
                    true,
                    DriftResolution::Unsupported,
                ));
            }
        }
        for found in &issues {
            if found.code == "PROVISION_ENCRYPTION_MISMATCH" {
                self.drift.push(drift_item(
                    &found.path,
                    DriftKind::EncryptionMismatch,
                    &self.spec.guarantees.encryption.at_rest.to_string(),
                    &(!self.spec.guarantees.encryption.at_rest).to_string(),
                    true,
                    DriftResolution::Unsupported,
                ));
            }
        }
        self.guarantees = entries;
        self.issues.extend(issues);
        self.plan_recorded_guarantees();
    }

    /// `RecordGuarantees` steps for declared guarantees that changed since the last applied
    /// spec: a weaker one needs `allow_weakening` for its path.
    fn plan_recorded_guarantees(&mut self) {
        let Some(applied) = self
            .record()
            .and_then(ProvisionRecord::last_applied)
            .map(|applied| applied.guarantees.clone())
        else {
            return;
        };
        let wanted = &self.spec.guarantees;
        let changes = [
            (
                "guarantees.write_ack",
                ack_rank(applied.write_ack),
                ack_rank(wanted.write_ack),
            ),
            (
                "guarantees.backup.enabled",
                u8::from(applied.backup.enabled),
                u8::from(wanted.backup.enabled),
            ),
            (
                "guarantees.backup.pitr",
                u8::from(applied.backup.pitr),
                u8::from(wanted.backup.pitr),
            ),
            (
                "guarantees.sensitive_data",
                u8::from(applied.sensitive_data),
                u8::from(wanted.sensitive_data),
            ),
            (
                "guarantees.strict_cross_domain_transactions",
                u8::from(applied.strict_cross_domain_transactions),
                u8::from(wanted.strict_cross_domain_transactions),
            ),
        ];
        for (position, (path, before, after)) in changes.into_iter().enumerate() {
            if before == after {
                continue;
            }
            let weaker = after < before;
            self.drift.push(drift_item(
                path,
                DriftKind::SettingDiffers,
                "as declared",
                "as last applied",
                false,
                DriftResolution::Apply,
            ));
            let planned = if weaker {
                step(
                    ProvisionStepKind::RecordGuarantees,
                    path,
                    None,
                    format!("record the weaker declared guarantee {path}"),
                    StepSafety::Weakening,
                )
                .with_flag(format!("allow_weakening:{path}"))
            } else {
                step(
                    ProvisionStepKind::RecordGuarantees,
                    path,
                    None,
                    format!("record the stronger declared guarantee {path}"),
                    StepSafety::Metadata,
                )
            };
            self.queue(Phase::Metadata, (0, 0, position), planned);
        }
    }

    /// Domains, overrides, operation hints and extension references are recorded as metadata;
    /// the applied spec is recorded last, before audit is disabled.
    fn plan_metadata(&mut self) {
        let record = self.record();
        let applied = record.and_then(ProvisionRecord::last_applied);
        let spec = self.spec;
        let changed = [
            (
                "domains",
                applied.map_or(!spec.domains.is_empty(), |applied| {
                    applied.domains != spec.domains
                }),
            ),
            (
                "overrides",
                applied.map_or(!spec.overrides.is_empty(), |applied| {
                    applied.overrides != spec.overrides
                }),
            ),
            (
                "operation_hints",
                applied.map_or(!spec.operation_hints.is_empty(), |applied| {
                    applied.operation_hints != spec.operation_hints
                }),
            ),
            (
                "extensions",
                applied.map_or(!spec.extensions.is_empty(), |applied| {
                    applied.extensions != spec.extensions
                }),
            ),
        ];
        let digest = spec_digest(spec);
        let applied_current = record.is_some_and(|record| {
            record.status == ProvisionRecordStatus::Applied
                && identity_digest(&record.applied_spec) == identity_digest(spec)
        });
        let existing = self.live().is_some();
        let had_record = record.is_some();
        for (position, (path, differs)) in changed.into_iter().enumerate() {
            if !differs {
                continue;
            }
            self.queue(
                Phase::Metadata,
                (1, 0, position),
                step(
                    ProvisionStepKind::RecordMetadata,
                    path,
                    None,
                    format!("record {path} as metadata; it has no runtime effect"),
                    StepSafety::Metadata,
                ),
            );
        }
        if !applied_current {
            if existing {
                self.drift.push(drift_item(
                    "$",
                    DriftKind::SpecChanged,
                    &digest,
                    if had_record {
                        "a different or unfinished applied spec"
                    } else {
                        "no provision record"
                    },
                    false,
                    DriftResolution::Apply,
                ));
            }
            self.queue(
                Phase::AppliedSpec,
                (0, 0, 0),
                step(
                    ProvisionStepKind::RecordAppliedSpec,
                    "$",
                    None,
                    format!("record the applied spec ({digest}) in __provision"),
                    StepSafety::Metadata,
                ),
            );
        }
    }

    fn finish(mut self, target_path: Option<&str>, version: &str) -> ProvisionPlan {
        let live_fingerprint = self.live().map(LiveState::fingerprint);
        let plan_id = plan_id_for(
            &PlanIdInputs {
                spec: self.spec,
                live_fingerprint: live_fingerprint.as_deref(),
                options: self.opts,
                surface: self.env.surface,
                target_path,
            },
            version,
        );
        let existing = self.live().is_some();
        let spec_migration = self
            .record()
            .and_then(ProvisionRecord::last_applied)
            .map(|applied| MigrationPlanner::plan(applied, self.spec));
        let target = self.planned_target(target_path);
        self.check_audit_required();

        let mut queued = std::mem::take(&mut self.steps);
        queued.sort_by_key(|queued| (queued.phase, queued.key));
        let steps = queued
            .into_iter()
            .enumerate()
            .map(|(index, queued)| ProvisionStep {
                step_id: format!("pstep-{:03}", index + 1),
                ..queued.step
            })
            .collect::<Vec<_>>();

        let required_flags = required_flags(&steps);
        let blocked = !self.validation.valid
            || self
                .issues
                .iter()
                .any(|found| found.severity == ValidationSeverity::Error)
            || steps.iter().any(|planned| !planned.supported)
            || self
                .guarantees
                .iter()
                .any(|entry| entry.status == EnforcementStatus::NotEnforceable);
        let flag_issues = missing_flag_issues(&steps, self.opts);
        let missing_flags = !flag_issues.is_empty();
        self.issues.extend(flag_issues);
        let outcome = if blocked {
            PlanOutcome::Blocked
        } else if missing_flags {
            PlanOutcome::NeedsFlags
        } else if steps.is_empty() {
            PlanOutcome::Noop
        } else {
            PlanOutcome::Ready
        };
        let auto_approvable = outcome == PlanOutcome::Ready
            && steps.iter().all(|planned| {
                matches!(
                    planned.safety,
                    StepSafety::Additive | StepSafety::Metadata | StepSafety::Strengthening
                ) && !(existing && planned.kind == ProvisionStepKind::ApplySecurity)
                    && !self.confirm_only.contains(&planned.path)
            });

        let impact = steps.iter().fold(impact_none(), |mut impact, planned| {
            merge_impact(&mut impact, &planned.impact);
            impact
        });
        let rollback = rollback_plan(&steps);
        let in_sync = outcome != PlanOutcome::Blocked
            && self
                .drift
                .iter()
                .all(|item| !item.blocking && item.resolution == DriftResolution::None);
        let required_confirmation = (outcome != PlanOutcome::Noop).then(|| plan_id.clone());

        ProvisionPlan {
            plan_format: PLAN_FORMAT,
            plan_id,
            surface: self.env.surface,
            target,
            spec_digest: spec_digest(self.spec),
            live_fingerprint,
            resolved_profile: self.resolved,
            validation: self.validation,
            issues: self.issues,
            objects: self.objects,
            guarantees: self.guarantees,
            drift: DriftReport {
                in_sync,
                items: self.drift,
            },
            steps,
            spec_migration,
            outcome,
            required_flags,
            auto_approvable,
            impact,
            rollback,
            required_confirmation,
        }
    }

    /// Audit stays off: any step makes the plan blocked, since an apply audits its intent first.
    fn check_audit_required(&mut self) {
        if self.audit_required && !self.steps.is_empty() {
            self.error(
                "AUDIT_REQUIRED_FOR_APPLY",
                "guarantees.audit.enabled",
                "audit is disabled on the database and the spec keeps it disabled, but every \
                 apply must audit its intent first"
                    .to_owned(),
                "Set guarantees.audit.enabled to true; the plan then enables audit first.",
            );
        }
    }

    fn planned_target(&self, target_path: Option<&str>) -> PlannedTarget {
        match &self.target {
            Target::New { .. } => PlannedTarget {
                kind: TargetKind::NewDatabase,
                path: target_path.map(str::to_owned),
                storage_profile: self
                    .resolved
                    .as_ref()
                    .map(|resolved| resolved.storage_profile),
                schema_version: self.desired_manifest.as_ref().map(|manifest| {
                    if manifest.storage_format.value_framing {
                        SCHEMA_VERSION_FRAMED
                    } else {
                        SCHEMA_VERSION
                    }
                }),
            },
            Target::Existing { live, .. } => PlannedTarget {
                kind: TargetKind::ExistingDatabase,
                path: target_path.map(str::to_owned),
                storage_profile: Some(live.metadata.storage_profile),
                schema_version: Some(live.metadata.schema_version),
            },
        }
    }
}

impl ProvisionStep {
    fn with_flag(mut self, flag: String) -> Self {
        self.requires_flag = Some(flag);
        self
    }

    fn with_impact(mut self, impact: PlanImpact) -> Self {
        self.impact = impact;
        self
    }
}

/// A step without an id; ids are assigned once the plan is in execution order.
fn step(
    kind: ProvisionStepKind,
    path: impl Into<String>,
    object: Option<&str>,
    action: String,
    safety: StepSafety,
) -> ProvisionStep {
    ProvisionStep {
        step_id: String::new(),
        kind,
        path: path.into(),
        object: object.map(str::to_owned),
        action,
        safety,
        impact: default_impact(safety),
        supported: safety != StepSafety::Unsupported,
        requires_flag: None,
    }
}

fn default_impact(safety: StepSafety) -> PlanImpact {
    match safety {
        StepSafety::Metadata | StepSafety::Strengthening => impact_none(),
        StepSafety::Additive => PlanImpact {
            disk: ImpactLevel::Low,
            cpu: ImpactLevel::Low,
            risk: RiskLevel::Low,
            ..impact_none()
        },
        StepSafety::Retune => low_risk_impact("takes effect at the next open of the database"),
        StepSafety::Weakening | StepSafety::Destructive => PlanImpact {
            risk: RiskLevel::Medium,
            notes: vec!["runs only with an explicit flag for its path".to_owned()],
            ..impact_none()
        },
        StepSafety::FormatUpgrade => medium_impact(
            "forward-only: the database gets schema_version 2, which older binaries refuse; \
             takes effect at the next open",
        ),
        StepSafety::Unsupported => PlanImpact {
            downtime: ImpactLevel::High,
            disk: ImpactLevel::High,
            cpu: ImpactLevel::High,
            risk: RiskLevel::High,
            requires_backup: true,
            requires_downtime: true,
            notes: vec!["no runtime API performs this change, so the plan is blocked".to_owned()],
        },
    }
}

fn drift_item(
    path: &str,
    kind: DriftKind,
    expected: &str,
    actual: &str,
    blocking: bool,
    resolution: DriftResolution,
) -> DriftItem {
    DriftItem {
        path: path.to_owned(),
        kind,
        expected: expected.to_owned(),
        actual: actual.to_owned(),
        blocking,
        resolution,
    }
}

pub(super) const fn safety_slug(safety: StepSafety) -> &'static str {
    match safety {
        StepSafety::Additive => "additive",
        StepSafety::Metadata => "metadata",
        StepSafety::Retune => "retune",
        StepSafety::Strengthening => "strengthening",
        StepSafety::Weakening => "weakening",
        StepSafety::Destructive => "destructive",
        StepSafety::FormatUpgrade => "format_upgrade",
        StepSafety::Unsupported => "unsupported",
    }
}

/// Every flag a step needs, in step order, once each.
fn required_flags(steps: &[ProvisionStep]) -> Vec<String> {
    let mut flags = Vec::new();
    for planned in steps {
        if let Some(flag) = &planned.requires_flag
            && !flags.contains(flag)
        {
            flags.push(flag.clone());
        }
    }
    flags
}

/// One `STEP_REQUIRES_FLAG` issue per step whose flag the options do not give.
fn missing_flag_issues(steps: &[ProvisionStep], opts: &ProvisionOptions) -> Vec<ValidationIssue> {
    steps
        .iter()
        .filter_map(|planned| {
            let flag = planned.requires_flag.as_ref()?;
            if flag_given(opts, flag) {
                return None;
            }
            Some(issue(
                "STEP_REQUIRES_FLAG",
                ValidationSeverity::Error,
                planned.path.clone(),
                format!(
                    "{} ({}) is a {} step and needs {flag}",
                    planned.step_id,
                    planned.action,
                    safety_slug(planned.safety)
                ),
                flag_suggestion(flag),
            ))
        })
        .collect()
}

fn flag_given(opts: &ProvisionOptions, flag: &str) -> bool {
    if flag == "allow_format_upgrade" {
        return opts.allow_format_upgrade;
    }
    if let Some(path) = flag.strip_prefix("allow_weakening:") {
        return opts.allow_weakening.contains(path);
    }
    if let Some(path) = flag.strip_prefix("allow_destructive:") {
        return opts.allow_destructive.contains(path);
    }
    false
}

fn flag_suggestion(flag: &str) -> String {
    if let Some(path) = flag.strip_prefix("allow_weakening:") {
        format!("Re-run with --allow-weakening {path} (ProvisionOptions.allow_weakening).")
    } else if let Some(path) = flag.strip_prefix("allow_destructive:") {
        format!("Re-run with --allow-destructive {path} (ProvisionOptions.allow_destructive).")
    } else {
        "Re-run with --allow-format-upgrade (ProvisionOptions.allow_format_upgrade).".to_owned()
    }
}

const fn ack_rank(ack: WriteAck) -> u8 {
    match ack {
        WriteAck::Local => 0,
        WriteAck::Quorum => 1,
        WriteAck::All => 2,
    }
}

fn rollback_plan(steps: &[ProvisionStep]) -> RollbackPlan {
    let possible = steps.iter().all(|planned| {
        matches!(
            planned.safety,
            StepSafety::Metadata
                | StepSafety::Retune
                | StepSafety::Strengthening
                | StepSafety::Weakening
        )
    });
    RollbackPlan {
        possible,
        description: "Provisioning never drops or alters objects: objects a plan creates stay \
                      when the previous spec is applied again, which only releases them from \
                      management and keeps their data. Profile, audit, RBAC and record changes \
                      are undone by applying the previous spec again; the storage format \
                      upgrade is forward-only. To undo an apply completely, restore a backup \
                      taken before it."
            .to_owned(),
        steps: steps
            .iter()
            .map(|planned| format!("{}: {}", planned.step_id, rollback_text(planned)))
            .collect(),
    }
}

fn rollback_text(planned: &ProvisionStep) -> String {
    let object = planned.object.as_deref().unwrap_or(&planned.path);
    match planned.kind {
        ProvisionStepKind::CreateDatabase => {
            "delete the new database file; nothing else refers to it".to_owned()
        }
        ProvisionStepKind::CreateCollection
        | ProvisionStepKind::CreateVectorCollection
        | ProvisionStepKind::CreateTable
        | ProvisionStepKind::CreateTimeSeries
        | ProvisionStepKind::CreateGraph
        | ProvisionStepKind::CreateFullTextIndex
        | ProvisionStepKind::CreateGeoIndex
        | ProvisionStepKind::EnableSystemVersioning => format!(
            "there is no drop API: applying the previous spec releases {object} from \
             management and keeps it"
        ),
        ProvisionStepKind::AdoptObject => {
            format!("applying the previous spec releases {object} from management")
        }
        ProvisionStepKind::ReleaseObject => {
            format!("declare {object} again to adopt it back")
        }
        ProvisionStepKind::UpgradeStorageFormat => {
            "forward-only; restore a backup taken before the upgrade".to_owned()
        }
        ProvisionStepKind::EnableAudit => {
            "apply a spec with audit disabled and --allow-weakening guarantees.audit.enabled"
                .to_owned()
        }
        ProvisionStepKind::AdoptProfile
        | ProvisionStepKind::RetuneProfile
        | ProvisionStepKind::DisableAudit
        | ProvisionStepKind::ApplySecurity
        | ProvisionStepKind::RecordGuarantees
        | ProvisionStepKind::RecordMetadata
        | ProvisionStepKind::RecordAppliedSpec
        | ProvisionStepKind::Noop => "apply the previous spec again".to_owned(),
    }
}

/// `spec_digest` of `spec` without `deployment.storage_path`: the storage path is a default
/// location, not identity, so a spec that only moves it is still the applied spec. The record
/// and the plan keep the full `spec_digest`.
fn identity_digest(spec: &DatabaseSpec) -> String {
    if spec.deployment.storage_path.is_none() {
        return spec_digest(spec);
    }
    let mut spec = spec.clone();
    spec.deployment.storage_path = None;
    spec_digest(&spec)
}

/// The step kind that creates `kind`.
const fn create_kind(kind: &DesiredKind) -> ProvisionStepKind {
    match kind {
        DesiredKind::Document { .. } => ProvisionStepKind::CreateCollection,
        DesiredKind::Table { .. } => ProvisionStepKind::CreateTable,
        DesiredKind::Vector { .. } => ProvisionStepKind::CreateVectorCollection,
        DesiredKind::TimeSeries { .. } => ProvisionStepKind::CreateTimeSeries,
        DesiredKind::Graph { .. } => ProvisionStepKind::CreateGraph,
        DesiredKind::FullText { .. } => ProvisionStepKind::CreateFullTextIndex,
        DesiredKind::Geo { .. } => ProvisionStepKind::CreateGeoIndex,
        DesiredKind::History { .. } => ProvisionStepKind::EnableSystemVersioning,
    }
}

/// Containers run before dependents. Containers with a pinned collection or graph id come
/// first, so ids allocated at apply time never take a pinned one; then documents, vectors,
/// tables, time series and graphs, each in spec order. Full-text and geo indexes come before
/// system versioning.
fn object_order(index: usize, object: &DesiredObject) -> (Phase, (u8, u8, usize)) {
    match &object.kind {
        DesiredKind::Document { collection_id, .. } => (
            Phase::Containers,
            (u8::from(collection_id.is_none()), 0, index),
        ),
        DesiredKind::Vector { collection_id, .. } => (
            Phase::Containers,
            (u8::from(collection_id.is_none()), 1, index),
        ),
        DesiredKind::Table { .. } => (Phase::Containers, (1, 2, index)),
        DesiredKind::TimeSeries { .. } => (Phase::Containers, (1, 3, index)),
        DesiredKind::Graph { graph_id } => {
            (Phase::Containers, (u8::from(graph_id.is_none()), 4, index))
        }
        DesiredKind::FullText { .. } | DesiredKind::Geo { .. } => {
            (Phase::Dependents, (0, 0, index))
        }
        DesiredKind::History { .. } => (Phase::Dependents, (1, 0, index)),
    }
}

/// `collections.<collection>` for a container, `collections.<collection>.<name>` for an object
/// it derives. Release flags use the same paths.
fn object_path(spec_collection: &str, name: &str) -> String {
    if spec_collection == name {
        format!("collections.{name}")
    } else {
        format!("collections.{spec_collection}.{name}")
    }
}

fn collection_spec<'s>(spec: &'s DatabaseSpec, name: &str) -> Option<&'s CollectionRoleSpec> {
    spec.collections
        .iter()
        .find(|collection| collection.name == name)
}

/// The object in a few words, for step actions and issues.
fn describe(object: &DesiredObject) -> String {
    let name = &object.name;
    match &object.kind {
        DesiredKind::Document { .. } => format!("document collection {name}"),
        DesiredKind::Table { layout, .. } => format!(
            "{} table {name}",
            match layout {
                TableLayout::Row => "row",
                TableLayout::Columnar => "columnar",
            }
        ),
        DesiredKind::Vector { dim, .. } => format!("vector collection {name} (dim {dim})"),
        DesiredKind::TimeSeries { .. } => format!("time series {name}"),
        DesiredKind::Graph { .. } => format!("graph {name}"),
        DesiredKind::FullText { source, path, .. } => format!(
            "full-text index {name} on {source}.{}",
            path.segments().join(".")
        ),
        DesiredKind::Geo { source, path, .. } => {
            format!("geo index {name} on {source}.{}", path.segments().join("."))
        }
        DesiredKind::History { base_table, .. } => {
            format!("system versioning of {base_table} ({name})")
        }
    }
}

fn create_step(
    object: &DesiredObject,
    path: &str,
    counts: &BTreeMap<String, u64>,
) -> ProvisionStep {
    let verb = if matches!(object.kind, DesiredKind::History { .. }) {
        "enable the"
    } else {
        "create the"
    };
    let created = step(
        create_kind(&object.kind),
        path,
        Some(&object.name),
        format!("{verb} {}", describe(object)),
        StepSafety::Additive,
    );
    let (DesiredKind::FullText { source, .. } | DesiredKind::Geo { source, .. }) = &object.kind
    else {
        return created;
    };
    match counts.get(source) {
        Some(&rows) if rows > 0 => {
            let note = format!(
                "the source collection {source} holds {rows} documents, which this build does \
                 not backfill into the new index"
            );
            ProvisionStep {
                action: format!("{}; {note}", created.action),
                ..created
            }
            .with_impact(medium_impact(&note))
        }
        _ => created,
    }
}

/// Releasing a managed object the spec stopped declaring: the step, its drift item and the
/// planned object. The object and its data stay; there is no drop API.
fn release(
    name: &str,
    spec_collection: &str,
    kind: ObjectKind,
    params: JsonValue,
) -> (ProvisionStep, DriftItem, PlannedObject) {
    let path = object_path(spec_collection, name);
    let planned = step(
        ProvisionStepKind::ReleaseObject,
        path.clone(),
        Some(name),
        format!(
            "release {} {name} from management: the spec no longer declares it, and its data \
             is kept (there is no drop API)",
            kind.slug()
        ),
        StepSafety::Destructive,
    )
    .with_flag(format!("allow_destructive:{path}"));
    let item = drift_item(
        &path,
        DriftKind::SpecChanged,
        "not declared",
        "managed",
        false,
        DriftResolution::Release,
    );
    let object = PlannedObject {
        name: name.to_owned(),
        spec_collection: Some(spec_collection.to_owned()),
        kind,
        state: ObjectState::Release,
        params,
        diff: Vec::new(),
    };
    (planned, item, object)
}

/// What planning one declared object produced.
struct ObjectOutcome {
    state: ObjectState,
    diff: Vec<FieldDiff>,
    step: Option<ProvisionStep>,
    issues: Vec<ValidationIssue>,
    drift: Vec<DriftItem>,
}

/// A declared object that exists: satisfied when managed and matching, adopted when only
/// matching, a conflict otherwise.
fn existing_object_outcome(
    object: &DesiredObject,
    path: &str,
    diff: Vec<FieldDiff>,
    managed: bool,
) -> ObjectOutcome {
    if diff.is_empty() {
        if managed {
            return ObjectOutcome {
                state: ObjectState::Satisfied,
                diff,
                step: None,
                issues: Vec::new(),
                drift: Vec::new(),
            };
        }
        return ObjectOutcome {
            state: ObjectState::Adopt,
            diff,
            step: Some(step(
                ProvisionStepKind::AdoptObject,
                path,
                Some(&object.name),
                format!(
                    "adopt the existing {}, which matches the spec",
                    describe(object)
                ),
                StepSafety::Additive,
            )),
            issues: Vec::new(),
            drift: vec![drift_item(
                path,
                DriftKind::UnmanagedObject,
                "managed",
                "matching, not managed",
                false,
                DriftResolution::Adopt,
            )],
        };
    }
    let fields = diff
        .iter()
        .map(|field| field.field.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    ObjectOutcome {
        state: ObjectState::Conflict,
        step: Some(step(
            create_kind(&object.kind),
            path,
            Some(&object.name),
            format!(
                "{} exists with different parameters ({fields}); there is no alter API",
                describe(object)
            ),
            StepSafety::Unsupported,
        )),
        issues: vec![issue(
            "PROVISION_OBJECT_CONFLICT",
            ValidationSeverity::Error,
            path,
            format!(
                "{} exists with different parameters ({fields}); provisioning cannot alter or \
                 replace objects",
                describe(object)
            ),
            "Match the spec to the live object (multidb config export shows it), or rename the \
             collection.",
        )],
        drift: vec![drift_item(
            path,
            DriftKind::ObjectConflict,
            &fields_json(&diff, true),
            &fields_json(&diff, false),
            true,
            DriftResolution::Unsupported,
        )],
        diff,
    }
}

/// A declared object that does not exist: created, also when the record still manages it (an
/// orphan). A pinned id that a live object already uses is `ID_CONFLICT`.
fn missing_object_outcome(
    object: &DesiredObject,
    path: &str,
    live: Option<&LiveState>,
    managed: bool,
    counts: &BTreeMap<String, u64>,
) -> ObjectOutcome {
    let mut issues = Vec::new();
    let mut drift = Vec::new();
    if let Some((field, holder)) = live.and_then(|live| pinned_id_holder(object, live)) {
        issues.push(issue(
            "ID_CONFLICT",
            ValidationSeverity::Error,
            format!("{path}.{field}"),
            format!(
                "{} pins a {field} that the live object {holder} already uses",
                describe(object)
            ),
            "Pin another id, or remove the pin so an id is allocated.",
        ));
    }
    if managed {
        issues.push(issue(
            "DRIFT_ORPHAN",
            ValidationSeverity::Warning,
            path,
            format!(
                "{} is managed by provisioning but missing from the database; the plan creates \
                 it again",
                describe(object)
            ),
            "Find out why the object disappeared (a restore of an older backup?).",
        ));
        drift.push(drift_item(
            path,
            DriftKind::OrphanedObject,
            "present",
            "missing",
            false,
            DriftResolution::Create,
        ));
    } else if live.is_some() {
        drift.push(drift_item(
            path,
            DriftKind::MissingObject,
            "present",
            "missing",
            false,
            DriftResolution::Create,
        ));
    }
    ObjectOutcome {
        state: ObjectState::Create,
        diff: Vec::new(),
        step: Some(create_step(object, path, counts)),
        issues,
        drift,
    }
}

/// The live object that already holds the id `object` pins, when `object` is missing.
fn pinned_id_holder(object: &DesiredObject, live: &LiveState) -> Option<(&'static str, String)> {
    match &object.kind {
        DesiredKind::Document {
            collection_id: Some(id),
            ..
        }
        | DesiredKind::Vector {
            collection_id: Some(id),
            ..
        } => live.catalog.iter().find_map(|(name, entry)| match entry {
            CatalogEntry::Collection { collection_id, .. }
            | CatalogEntry::Vector { collection_id, .. }
                if collection_id == id =>
            {
                Some(("collection_id", name.clone()))
            }
            _ => None,
        }),
        DesiredKind::Graph { graph_id: Some(id) } => {
            live.catalog.iter().find_map(|(name, entry)| match entry {
                CatalogEntry::Graph { graph_id } if graph_id == id => {
                    Some(("graph_id", name.clone()))
                }
                _ => None,
            })
        }
        _ => None,
    }
}

/// The spec-controlled parameters of `object` that differ from the live `entry`, with the
/// desired and the live value of each.
fn object_diff(
    object: &DesiredObject,
    physical: Option<&PhysicalObjectSpec>,
    entry: &CatalogEntry,
    live: &LiveState,
    allocated_id: Option<u32>,
) -> Vec<FieldDiff> {
    let fields = differing_fields(object, physical, entry, live, allocated_id);
    if fields.is_empty() {
        return Vec::new();
    }
    let expected = object.params_json();
    let actual = live_params_json(&object.name, entry, &live.catalog, &live.table_schemas);
    fields
        .into_iter()
        .map(|field| FieldDiff {
            field: field.to_owned(),
            expected: expected.get(field).cloned().unwrap_or(JsonValue::Null),
            actual: actual.get(field).cloned().unwrap_or(JsonValue::Null),
        })
        .collect()
}

/// The matching rule: only what a spec controls is compared. Ids are compared when the spec pins
/// them or the record allocated them; document and table index sets are compared without their
/// ids unless an index id is pinned.
fn differing_fields(
    object: &DesiredObject,
    physical: Option<&PhysicalObjectSpec>,
    entry: &CatalogEntry,
    live: &LiveState,
    allocated_id: Option<u32>,
) -> Vec<&'static str> {
    let mut fields = Vec::new();
    let same_kind = match &object.kind {
        DesiredKind::Document { .. } | DesiredKind::Table { .. } | DesiredKind::Vector { .. } => {
            record_differences(object, physical, entry, live, allocated_id, &mut fields)
        }
        _ => other_differences(object, entry, live, allocated_id, &mut fields),
    };
    if !same_kind {
        fields.push("kind");
    }
    fields
}

/// Document collections, tables and vector collections; `false` when the live entry has another
/// kind.
fn record_differences(
    object: &DesiredObject,
    physical: Option<&PhysicalObjectSpec>,
    entry: &CatalogEntry,
    live: &LiveState,
    allocated_id: Option<u32>,
    fields: &mut Vec<&'static str>,
) -> bool {
    let mut differs = |field: &'static str, condition: bool| {
        if condition {
            fields.push(field);
        }
    };
    match (&object.kind, entry) {
        (
            DesiredKind::Document {
                collection_id,
                fields: wanted,
                indexes,
            },
            CatalogEntry::Collection {
                collection_id: live_id,
                fields: have,
                indexes: live_indexes,
            },
        ) => {
            differs(
                "collection_id",
                id_differs(
                    collection_id.map(CollectionId::as_u32),
                    allocated_id,
                    live_id.as_u32(),
                ),
            );
            let pinned = physical.map_or_else(Vec::new, |physical| {
                pinned_ids(physical.document_indexes.iter().map(|index| index.id))
            });
            differs("fields", wanted != have);
            differs(
                "document_indexes",
                !document_indexes_match(indexes, &pinned, live_indexes),
            );
            true
        }
        (
            DesiredKind::Table {
                layout,
                schema,
                indexes,
            },
            CatalogEntry::Table {
                indexes: live_indexes,
                layout: live_layout,
            },
        ) => {
            let pinned = physical.map_or_else(Vec::new, |physical| {
                pinned_ids(physical.table_indexes.iter().map(|index| index.id))
            });
            differs("layout", layout != live_layout);
            differs(
                "columns",
                schema
                    .as_ref()
                    .is_some_and(|schema| live.table_schemas.get(&object.name) != Some(schema)),
            );
            differs(
                "table_indexes",
                !table_indexes_match(indexes, &pinned, live_indexes),
            );
            true
        }
        (
            DesiredKind::Vector {
                collection_id,
                dim,
                metric,
                hnsw,
            },
            CatalogEntry::Vector {
                collection_id: live_id,
                dim: live_dim,
                metric: live_metric,
                hnsw: live_hnsw,
                ..
            },
        ) => {
            differs(
                "collection_id",
                id_differs(
                    collection_id.map(CollectionId::as_u32),
                    allocated_id,
                    live_id.as_u32(),
                ),
            );
            differs("dim", dim != live_dim);
            differs("metric", metric != live_metric);
            differs("hnsw", hnsw != live_hnsw);
            true
        }
        _ => false,
    }
}

/// Time series, graphs, full-text and geo indexes and history tables; `false` when the live
/// entry has another kind.
fn other_differences(
    object: &DesiredObject,
    entry: &CatalogEntry,
    live: &LiveState,
    allocated_id: Option<u32>,
    fields: &mut Vec<&'static str>,
) -> bool {
    let mut differs = |field: &'static str, condition: bool| {
        if condition {
            fields.push(field);
        }
    };
    match (&object.kind, entry) {
        (
            DesiredKind::TimeSeries {
                chunk_millis,
                retention_millis,
            },
            CatalogEntry::TimeSeries { config },
        ) => {
            differs("chunk_millis", *chunk_millis != config.chunk_millis);
            differs(
                "retention_millis",
                *retention_millis != config.retention_millis,
            );
        }
        (DesiredKind::Graph { graph_id }, CatalogEntry::Graph { graph_id: live_id }) => differs(
            "graph_id",
            id_differs(
                graph_id.map(GraphId::as_u32),
                allocated_id,
                live_id.as_u32(),
            ),
        ),
        (
            DesiredKind::FullText {
                source,
                path,
                language,
                refresh_lag_target,
            },
            CatalogEntry::FullTextIndex { config },
        ) => {
            let crate::text::TextSource::Collection {
                collection_id,
                path: live_path,
            } = &config.source;
            differs("source", source_id(live, source) != Some(*collection_id));
            differs("path", path != live_path);
            differs("language", *language != config.language);
            differs(
                "refresh_lag_target",
                *refresh_lag_target != config.refresh_lag_target,
            );
        }
        (
            DesiredKind::Geo {
                source,
                path,
                precision,
            },
            CatalogEntry::GeoIndex { config },
        ) => {
            differs(
                "source",
                source_id(live, source) != Some(config.collection_id),
            );
            differs("path", *path != config.path);
            differs("precision", *precision != config.precision);
        }
        (
            DesiredKind::History {
                base_table,
                keep_history,
            },
            CatalogEntry::TemporalTable {
                base_table: live_base,
                retention,
                ..
            },
        ) => {
            differs("base_table", base_table != live_base);
            differs("keep_history", *keep_history != retention.keep_history);
        }
        _ => return false,
    }
    true
}

/// Which index ids of a spec are pinned, by position.
fn pinned_ids(ids: impl Iterator<Item = Option<u32>>) -> Vec<bool> {
    ids.map(|id| id.is_some()).collect()
}

fn id_differs(pinned: Option<u32>, allocated: Option<u32>, live: u32) -> bool {
    pinned
        .or(allocated)
        .is_some_and(|expected| expected != live)
}

/// The live collection id of the document collection `source`.
fn source_id(live: &LiveState, source: &str) -> Option<CollectionId> {
    match live.catalog.get(source) {
        Some(CatalogEntry::Collection { collection_id, .. }) => Some(*collection_id),
        _ => None,
    }
}

fn document_indexes_match(wanted: &[IndexSpec], pinned: &[bool], live: &[IndexSpec]) -> bool {
    let mut wanted_paths = wanted
        .iter()
        .map(|index| index.path.segments())
        .collect::<Vec<_>>();
    let mut live_paths = live
        .iter()
        .map(|index| index.path.segments())
        .collect::<Vec<_>>();
    wanted_paths.sort();
    live_paths.sort();
    wanted_paths == live_paths
        && wanted
            .iter()
            .zip(pinned)
            .filter(|(_, pinned)| **pinned)
            .all(|(index, _)| {
                live.iter()
                    .any(|candidate| candidate.path == index.path && candidate.id == index.id)
            })
}

/// A table index without its id: (lowercased, column, include, partial).
fn table_index_key(index: &RelIndexSpec) -> (bool, usize, Vec<usize>, bool) {
    let (lower, column) = match index.expression {
        RelIndexExpression::Column(column) => (false, column),
        RelIndexExpression::LowerAscii(column) => (true, column),
    };
    (
        lower,
        column,
        index.include.clone(),
        index.predicate.is_some(),
    )
}

fn table_indexes_match(wanted: &[RelIndexSpec], pinned: &[bool], live: &[RelIndexSpec]) -> bool {
    let mut wanted_keys = wanted.iter().map(table_index_key).collect::<Vec<_>>();
    let mut live_keys = live.iter().map(table_index_key).collect::<Vec<_>>();
    wanted_keys.sort();
    live_keys.sort();
    wanted_keys == live_keys
        && wanted
            .iter()
            .zip(pinned)
            .filter(|(_, pinned)| **pinned)
            .all(|(index, _)| {
                live.iter().any(|candidate| {
                    candidate.id == index.id && table_index_key(candidate) == table_index_key(index)
                })
            })
}

/// The differing fields as a JSON object of their desired (or live) values.
fn fields_json(diff: &[FieldDiff], expected: bool) -> String {
    let object = diff
        .iter()
        .map(|field| {
            (
                field.field.clone(),
                if expected {
                    field.expected.clone()
                } else {
                    field.actual.clone()
                },
            )
        })
        .collect::<serde_json::Map<_, _>>();
    JsonValue::Object(object).to_string()
}

/// The role a spec declares, with its grants resolved to the physical resources of the
/// projected objects (`<table>_history` is a table). A grant on an object the projection does
/// not know is left out; validation reports it.
pub(super) fn desired_role(role: &crate::config_spec::RoleSpec, desired: &[DesiredObject]) -> Role {
    let mut resolved = Role::new(role.name.clone());
    for grant in &role.grants {
        let resource = match (&grant.object, grant.resource) {
            (None, Some(GrantScope::Database)) => Some(Resource::Database),
            (Some(name), None) => desired
                .iter()
                .find(|object| &object.name == name)
                .map(|object| object_resource(&object.name, &object.kind)),
            _ => None,
        };
        let Some(resource) = resource else {
            continue;
        };
        for permission in &grant.permissions {
            resolved = resolved.grant(resource.clone(), permission_of(*permission));
        }
    }
    resolved
}

/// The RBAC resource of a projected object.
pub(crate) fn object_resource(name: &str, kind: &DesiredKind) -> Resource {
    let name = name.to_owned();
    match kind {
        DesiredKind::Document { .. } => Resource::Collection(name),
        DesiredKind::Table { .. } | DesiredKind::History { .. } => Resource::Table(name),
        DesiredKind::Vector { .. } => Resource::VectorCollection(name),
        DesiredKind::TimeSeries { .. } => Resource::TimeSeries(name),
        DesiredKind::Graph { .. } => Resource::Graph(name),
        DesiredKind::FullText { .. } => Resource::FullTextIndex(name),
        DesiredKind::Geo { .. } => Resource::GeoIndex(name),
    }
}

const fn permission_of(permission: PermissionSpec) -> Permission {
    match permission {
        PermissionSpec::Read => Permission::Read,
        PermissionSpec::Write => Permission::Write,
        PermissionSpec::Admin => Permission::Admin,
    }
}

/// The changes to managed roles, and the roles the policy holds after the apply.
fn plan_roles(
    security: &SecuritySpec,
    desired: &[DesiredObject],
    live_policy: &AuthzPolicy,
    managed_roles: &BTreeSet<String>,
) -> (Vec<SecurityChange>, BTreeMap<String, Role>) {
    let mut planned = Vec::new();
    let mut post_roles = live_policy.roles().clone();
    for name in managed_roles {
        post_roles.remove(name);
    }
    let mut declared = BTreeSet::new();
    for role in &security.roles {
        declared.insert(role.name.clone());
        let wanted = desired_role(role, desired);
        planned.extend(role_change(
            &role.name,
            live_policy.roles().get(&role.name),
            &wanted,
            managed_roles.contains(&role.name),
        ));
        post_roles.insert(role.name.clone(), wanted);
    }
    for name in managed_roles.difference(&declared) {
        if live_policy.roles().contains_key(name) {
            let path = format!("security.roles.{name}");
            planned.push(SecurityChange {
                path: path.clone(),
                object: name.clone(),
                action: format!("remove the managed role {name}, which the spec dropped"),
                safety: StepSafety::Destructive,
                flag: Some(format!("allow_destructive:{path}")),
                conflict: false,
                widens: false,
            });
        }
    }
    (planned, post_roles)
}

/// The changes to managed principals, and the registry after the apply.
fn plan_principals(
    security: &SecuritySpec,
    live_registry: &PrincipalRegistry,
    managed_users: &BTreeSet<String>,
) -> (Vec<SecurityChange>, BTreeMap<String, Principal>) {
    let mut planned = Vec::new();
    let mut post_principals = live_registry.principals().clone();
    for user in managed_users {
        post_principals.remove(user);
    }
    let mut declared = BTreeSet::new();
    for principal in &security.principals {
        declared.insert(principal.user.clone());
        let wanted = principal.roles.iter().fold(
            Principal::new(principal.principal_name()),
            |wanted, role| wanted.with_role(role.clone()),
        );
        planned.extend(principal_change(
            &principal.user,
            live_registry.principals().get(&principal.user),
            &wanted,
            managed_users.contains(&principal.user),
        ));
        post_principals.insert(principal.user.clone(), wanted);
    }
    for user in managed_users.difference(&declared) {
        if live_registry.principals().contains_key(user) {
            let path = format!("security.principals.{user}");
            planned.push(SecurityChange {
                path: path.clone(),
                object: user.clone(),
                action: format!("remove the managed principal {user}, which the spec dropped"),
                safety: StepSafety::Destructive,
                flag: Some(format!("allow_destructive:{path}")),
                conflict: false,
                widens: false,
            });
        }
    }
    (planned, post_principals)
}

/// What the applying `actor` resolves to after the apply, the way authorization resolves a
/// login user (`PrincipalRegistry::principal_for_user`). The registry is keyed by login user,
/// and a user may map to a principal of another name, so the actor is matched against the live
/// principals: exactly first, then by principal name. Each matching user maps to its post-apply
/// principal, or to a principal without roles when the plan removes the user. Every match
/// counts, so an ambiguous actor fails closed. An actor that no login user maps to (a session,
/// or a principal the caller built) keeps its roles.
fn post_apply_actors(
    actor: &Principal,
    live_registry: &PrincipalRegistry,
    post_principals: &BTreeMap<String, Principal>,
) -> Vec<Principal> {
    let live = live_registry.principals();
    let mut users: Vec<&String> = live
        .iter()
        .filter(|(_, principal)| *principal == actor)
        .map(|(user, _)| user)
        .collect();
    if users.is_empty() {
        users = live
            .iter()
            .filter(|(_, principal)| principal.name() == actor.name())
            .map(|(user, _)| user)
            .collect();
    }
    if users.is_empty() {
        return vec![actor.clone()];
    }
    users
        .into_iter()
        .map(|user| {
            post_principals
                .get(user)
                .cloned()
                .unwrap_or_else(|| Principal::new(user.clone()))
        })
        .collect()
}

/// The roles and principals a record manages, without the reserved bootstrap names, which
/// provisioning never touches.
fn managed_security_names(
    record: Option<&ProvisionRecord>,
) -> (BTreeSet<String>, BTreeSet<String>) {
    let roles = record
        .map(|record| record.managed_roles.clone())
        .unwrap_or_default()
        .into_iter()
        .filter(|name| !RESERVED_ROLE_NAMES.contains(&name.as_str()))
        .collect();
    let users = record
        .map(|record| record.managed_principals.clone())
        .unwrap_or_default()
        .into_iter()
        .filter(|name| !RESERVED_PRINCIPAL_NAMES.contains(&name.as_str()))
        .collect();
    (roles, users)
}

/// The RBAC state an apply of `spec` leaves: the live policy and registry with the roles and
/// principals `record` manages replaced by the spec's, and everything else untouched. The same
/// merge the plan's `ApplySecurity` steps describe, so the executor commits exactly what was
/// planned.
pub(super) fn security_post_state(
    spec: &DatabaseSpec,
    desired: &[DesiredObject],
    live_policy: &AuthzPolicy,
    live_registry: &PrincipalRegistry,
    record: Option<&ProvisionRecord>,
) -> (AuthzPolicy, PrincipalRegistry) {
    let (managed_roles, managed_users) = managed_security_names(record);
    let security = spec.security.clone().unwrap_or_default();
    let (_, roles) = plan_roles(&security, desired, live_policy, &managed_roles);
    let (_, principals) = plan_principals(&security, live_registry, &managed_users);
    let registry = principals
        .into_iter()
        .fold(PrincipalRegistry::new(), |registry, (user, principal)| {
            registry.with_principal(user, principal)
        });
    (AuthzPolicy::new(roles.into_values()), registry)
}

/// The profile manifest `resolved` binds to: what a new database records and what an apply
/// writes to an existing one. The manifest does not depend on the database path.
/// # Errors
/// Fails when the profile cannot be bound to a single-node database.
pub(super) fn desired_manifest(
    resolved: &ResolvedProfile,
) -> Result<PersistedProfileManifest, crate::db::ConfigError> {
    let binding_path = resolved
        .storage_profile
        .is_on_disk()
        .then(|| PathBuf::from("provisioning-plan.redb"));
    resolved
        .bind(ReplicationKind::Cp, binding_path)
        .map(|binding| binding.manifest)
}

/// One planned RBAC change.
struct SecurityChange {
    path: String,
    object: String,
    action: String,
    safety: StepSafety,
    flag: Option<String>,
    /// The spec names an unmanaged role or principal that differs from the live one.
    conflict: bool,
    /// A role is created or gains permissions.
    widens: bool,
}

fn grant_pairs(role: &Role) -> BTreeSet<(Resource, Permission)> {
    role.grants()
        .iter()
        .flat_map(|(resource, permissions)| {
            permissions
                .iter()
                .map(move |permission| (resource.clone(), *permission))
        })
        .collect()
}

fn role_change(
    name: &str,
    live: Option<&Role>,
    desired: &Role,
    managed: bool,
) -> Option<SecurityChange> {
    let path = format!("security.roles.{name}");
    let change = |action: String, safety: StepSafety, flag: Option<String>, conflict: bool| {
        Some(SecurityChange {
            path: path.clone(),
            object: name.to_owned(),
            action,
            safety,
            flag,
            conflict,
            widens: false,
        })
    };
    let widening = |change: Option<SecurityChange>| {
        change.map(|change| SecurityChange {
            widens: true,
            ..change
        })
    };
    let Some(live) = live else {
        return widening(change(
            format!(
                "create the role {name} with {} grants",
                grant_pairs(desired).len()
            ),
            StepSafety::Additive,
            None,
            false,
        ));
    };
    let have = grant_pairs(live);
    let want = grant_pairs(desired);
    if have == want {
        return if managed {
            None
        } else {
            change(
                format!("adopt the existing role {name}, whose grants match the spec"),
                StepSafety::Metadata,
                None,
                false,
            )
        };
    }
    if !managed {
        return change(
            format!(
                "role {name} exists, is not managed by provisioning and has different grants; \
                 provisioning never changes unmanaged roles"
            ),
            StepSafety::Unsupported,
            None,
            true,
        );
    }
    let revoked = have.difference(&want).count();
    let added = want.difference(&have).count();
    let planned = if revoked > 0 {
        change(
            format!("change the role {name}: revoke {revoked} and add {added} permissions"),
            StepSafety::Weakening,
            Some(format!("allow_weakening:{path}")),
            false,
        )
    } else {
        change(
            format!("add {added} permissions to the role {name}"),
            StepSafety::Additive,
            None,
            false,
        )
    };
    if added > 0 {
        widening(planned)
    } else {
        planned
    }
}

fn principal_change(
    user: &str,
    live: Option<&Principal>,
    desired: &Principal,
    managed: bool,
) -> Option<SecurityChange> {
    let path = format!("security.principals.{user}");
    let change = |action: String, safety: StepSafety, flag: Option<String>, conflict: bool| {
        Some(SecurityChange {
            path: path.clone(),
            object: user.to_owned(),
            action,
            safety,
            flag,
            conflict,
            widens: false,
        })
    };
    let Some(live) = live else {
        return change(
            format!(
                "map the user {user} to the principal {} with roles {}",
                desired.name(),
                join(desired.roles())
            ),
            StepSafety::Additive,
            None,
            false,
        );
    };
    if live == desired {
        return if managed {
            None
        } else {
            change(
                format!("adopt the existing principal of the user {user}, which matches the spec"),
                StepSafety::Metadata,
                None,
                false,
            )
        };
    }
    if !managed {
        return change(
            format!(
                "the user {user} is mapped to a principal that provisioning does not manage and \
                 that differs from the spec; provisioning never changes unmanaged principals"
            ),
            StepSafety::Unsupported,
            None,
            true,
        );
    }
    let narrowed = live.name() != desired.name() || !live.roles().is_subset(desired.roles());
    if narrowed {
        change(
            format!(
                "change the principal of the user {user} to {} with roles {}",
                desired.name(),
                join(desired.roles())
            ),
            StepSafety::Weakening,
            Some(format!("allow_weakening:{path}")),
            false,
        )
    } else {
        change(
            format!("add roles to the principal of the user {user}"),
            StepSafety::Additive,
            None,
            false,
        )
    }
}

fn join(names: &BTreeSet<String>) -> String {
    if names.is_empty() {
        "none".to_owned()
    } else {
        names.iter().cloned().collect::<Vec<_>>().join(", ")
    }
}

/// The steps, issues and drift of comparing a live database's profile manifest (or its absence)
/// with the desired one. The storage class already matches.
#[derive(Default)]
struct ManifestPlan {
    steps: Vec<ProvisionStep>,
    issues: Vec<ValidationIssue>,
    drift: Vec<DriftItem>,
}

/// How each changed manifest field is classified.
#[derive(Default)]
struct ManifestDiff {
    /// Identity fields and knobs whose change is a metadata manifest rewrite: `Provisioning`
    /// and `NotApplicable` knobs, raised floors, profile identity.
    metadata: Vec<String>,
    /// Session knobs; take effect at the next open.
    retune: Vec<String>,
    /// Floors set in the live manifest and cleared in the desired one.
    lowered_floors: Vec<String>,
    /// Knobs that cannot change after creation.
    unsupported: Vec<String>,
    /// Compression turns value framing on.
    format_upgrade: bool,
    /// Compression would turn value framing off.
    format_mismatch: bool,
}

fn plan_manifest(live: &LiveState, desired: &PersistedProfileManifest) -> ManifestPlan {
    let mut plan = ManifestPlan::default();
    let Some(current) = &live.metadata.manifest else {
        legacy_manifest_plan(live, desired, &mut plan);
        return plan;
    };

    let diff = diff_manifests(current, desired);
    push_manifest_steps(&diff, desired, &mut plan);
    for path in diff
        .metadata
        .iter()
        .chain(&diff.retune)
        .chain(&diff.lowered_floors)
    {
        plan.drift.push(drift_item(
            &knob_spec_path(path),
            DriftKind::SettingDiffers,
            "as declared",
            "as in the profile manifest",
            false,
            DriftResolution::Retune,
        ));
    }
    if live.session_performance != performance_for_manifest(current) {
        plan.drift.push(drift_item(
            "profile",
            DriftKind::SessionPendingRestart,
            "the performance of the profile manifest",
            "a different session performance",
            false,
            DriftResolution::Restart,
        ));
    }
    plan
}

/// The steps a manifest diff needs, most significant first.
fn push_manifest_steps(
    diff: &ManifestDiff,
    desired: &PersistedProfileManifest,
    plan: &mut ManifestPlan,
) {
    if !diff.unsupported.is_empty() {
        let paths = diff.unsupported.join(", ");
        plan.issues.push(issue(
            "PROVISION_ENGINE_MISMATCH",
            ValidationSeverity::Error,
            knob_spec_path(&diff.unsupported[0]),
            format!(
                "the spec changes knobs fixed at creation ({paths}); there is no reprofile or \
                 storage-class switch"
            ),
            "Keep the knobs the database was created with.",
        ));
        plan.steps.push(step(
            ProvisionStepKind::RetuneProfile,
            "custom_profile.knobs",
            None,
            format!("change knobs fixed at creation: {paths}"),
            StepSafety::Unsupported,
        ));
    }
    if diff.format_mismatch {
        format_mismatch(plan);
    }
    if diff.format_upgrade {
        plan.steps.push(format_upgrade_step(desired));
        plan.drift.push(drift_item(
            "custom_profile.knobs.compression.algorithm",
            DriftKind::FormatMismatch,
            "value framing",
            "no value framing",
            false,
            DriftResolution::Retune,
        ));
    }
    if !diff.retune.is_empty() {
        plan.steps.push(step(
            ProvisionStepKind::RetuneProfile,
            "custom_profile.knobs",
            None,
            format!(
                "rewrite the profile manifest with new session knobs ({}); they take effect at \
                 the next open",
                diff.retune.join(", ")
            ),
            StepSafety::Retune,
        ));
    }
    for floor in &diff.lowered_floors {
        let path = knob_spec_path(floor);
        plan.steps.push(
            step(
                ProvisionStepKind::AdoptProfile,
                path.clone(),
                None,
                format!("lower the profile floor {floor} in the profile manifest"),
                StepSafety::Weakening,
            )
            .with_flag(format!("allow_weakening:{path}")),
        );
    }
    if !diff.metadata.is_empty() {
        plan.steps.push(step(
            ProvisionStepKind::AdoptProfile,
            "profile",
            None,
            format!(
                "rewrite the profile manifest ({}); existing objects keep their parameters",
                diff.metadata.join(", ")
            ),
            StepSafety::Metadata,
        ));
    }
}

/// A legacy database (no manifest) is adopted by writing the manifest: metadata when the session
/// already runs the desired knobs, retune otherwise, plus the format upgrade compression needs.
fn legacy_manifest_plan(
    live: &LiveState,
    desired: &PersistedProfileManifest,
    plan: &mut ManifestPlan,
) {
    let desired_framing = desired.storage_format.value_framing;
    if live.metadata.values_framed && !desired_framing {
        format_mismatch(plan);
    } else if desired_framing {
        plan.steps.push(format_upgrade_step(desired));
        plan.drift.push(drift_item(
            "custom_profile.knobs.compression.algorithm",
            DriftKind::FormatMismatch,
            "value framing",
            if live.metadata.values_framed {
                "legacy value framing without a profile manifest"
            } else {
                "no value framing"
            },
            false,
            DriftResolution::Retune,
        ));
    }
    let session = without_compression(&live.session_performance);
    let wanted = without_compression(&performance_for_manifest(desired));
    let safety = if session == wanted {
        StepSafety::Metadata
    } else {
        StepSafety::Retune
    };
    plan.steps.push(step(
        ProvisionStepKind::AdoptProfile,
        "profile",
        None,
        format!(
            "adopt the legacy database with profile {}: write __meta.profile_manifest{}",
            desired.profile,
            if safety == StepSafety::Retune {
                "; its knobs differ from the storage profile defaults and take effect at the \
                 next open"
            } else {
                ""
            }
        ),
        safety,
    ));
    plan.drift.push(drift_item(
        "profile",
        DriftKind::SettingDiffers,
        &desired.profile,
        "a legacy database without a profile manifest",
        false,
        DriftResolution::Adopt,
    ));
}

fn format_mismatch(plan: &mut ManifestPlan) {
    let path = "custom_profile.knobs.compression.algorithm";
    plan.issues.push(issue(
        "PROVISION_FORMAT_MISMATCH",
        ValidationSeverity::Error,
        path,
        "the database stores compression-framed values, and value framing can never be turned \
         off again",
        "Keep a compression algorithm (lz4 or zstd).",
    ));
    plan.steps.push(step(
        ProvisionStepKind::UpgradeStorageFormat,
        path,
        None,
        "turn value framing off".to_owned(),
        StepSafety::Unsupported,
    ));
    plan.drift.push(drift_item(
        path,
        DriftKind::FormatMismatch,
        "no value framing",
        "value framing",
        true,
        DriftResolution::Unsupported,
    ));
}

fn format_upgrade_step(desired: &PersistedProfileManifest) -> ProvisionStep {
    step(
        ProvisionStepKind::UpgradeStorageFormat,
        "custom_profile.knobs.compression.algorithm",
        None,
        format!(
            "upgrade the storage format: write a value-framing manifest ({:?} compression) with \
             schema_version 2; forward-only, older binaries refuse the database, and new values \
             are framed after the next open",
            desired.storage_format.compression.algorithm
        ),
        StepSafety::FormatUpgrade,
    )
    .with_flag("allow_format_upgrade".to_owned())
}

/// The session performance without compression, which only a value-framing engine applies.
fn without_compression(performance: &PerformanceConfig) -> PerformanceConfig {
    let mut performance = performance.clone();
    performance.compression.algorithm = CompressionAlgorithm::None;
    performance.compression.min_bytes = 0;
    performance.compression.zstd_level = 0;
    performance
}

fn diff_manifests(
    current: &PersistedProfileManifest,
    desired: &PersistedProfileManifest,
) -> ManifestDiff {
    let mut diff = ManifestDiff::default();
    for (field, differs) in [
        ("profile", current.profile != desired.profile),
        ("kind", current.kind != desired.kind),
        (
            "product_profile",
            current.product_profile != desired.product_profile,
        ),
        ("variant", current.variant != desired.variant),
        ("extends", current.extends != desired.extends),
    ] {
        if differs {
            diff.metadata.push(field.to_owned());
        }
    }
    let have = knob_leaves(&current.knobs);
    let want = knob_leaves(&desired.knobs);
    let paths = have.keys().chain(want.keys()).collect::<BTreeSet<_>>();
    for path in paths {
        let before = have.get(path);
        let after = want.get(path);
        if before == after {
            continue;
        }
        if path == "compression.algorithm" {
            let to_none = after.and_then(JsonValue::as_str) == Some("none");
            match (current.storage_format.value_framing, to_none) {
                (true, true) => diff.format_mismatch = true,
                (true, false) => diff.retune.push(path.clone()),
                (false, false) => diff.format_upgrade = true,
                (false, true) => {}
            }
            continue;
        }
        match knob_definition(path).map(|definition| definition.after_creation) {
            Some(ChangePolicy::Retune | ChangePolicy::FormatUpgrade) => {
                diff.retune.push(path.clone());
            }
            Some(ChangePolicy::Provisioning) => diff.metadata.push(path.clone()),
            Some(ChangePolicy::NotApplicable) => {
                let lowered = path.starts_with("floors.")
                    && before == Some(&JsonValue::Bool(true))
                    && after == Some(&JsonValue::Bool(false));
                if lowered {
                    diff.lowered_floors.push(path.clone());
                } else {
                    diff.metadata.push(path.clone());
                }
            }
            Some(ChangePolicy::Unsupported) | None => diff.unsupported.push(path.clone()),
        }
    }
    diff
}

/// Every knob leaf by registry path, for example `cache.max_entries`. Arrays (the role set) are
/// leaves.
pub(super) fn knob_leaves(knobs: &crate::config_spec::ProfileKnobs) -> BTreeMap<String, JsonValue> {
    fn walk(prefix: &str, value: &JsonValue, leaves: &mut BTreeMap<String, JsonValue>) {
        match value {
            JsonValue::Object(map) => {
                for (key, value) in map {
                    let path = if prefix.is_empty() {
                        key.clone()
                    } else {
                        format!("{prefix}.{key}")
                    };
                    walk(&path, value, leaves);
                }
            }
            other => {
                leaves.insert(prefix.to_owned(), other.clone());
            }
        }
    }
    let mut leaves = BTreeMap::new();
    if let Ok(value) = serde_json::to_value(knobs) {
        walk("", &value, &mut leaves);
    }
    leaves
}

/// The spec path of a registry knob: `custom_profile.floors.*` and
/// `custom_profile.compatible_roles` for floors and roles, `custom_profile.knobs.*` otherwise.
fn knob_spec_path(path: &str) -> String {
    if path.starts_with("floors.") || path == "compatible_roles" {
        format!("custom_profile.{path}")
    } else if path.contains('.') {
        format!("custom_profile.knobs.{path}")
    } else {
        path.to_owned()
    }
}
