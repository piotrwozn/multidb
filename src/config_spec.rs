#![allow(
    clippy::missing_errors_doc,
    clippy::result_large_err,
    clippy::too_many_lines
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::LazyLock,
};

use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

use crate::db::{DbConfig, Profile, ReplicationKind};

mod schema;

pub use schema::database_spec_v1_schema;

pub const DATABASE_SPEC_VERSION: u32 = 1;

const DEFAULT_DOMAIN: &str = "primary";

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseSpec {
    pub version: u32,
    pub name: String,
    pub profile: String,
    pub deployment: DeploymentSpec,
    #[serde(default)]
    pub topology: TopologySpec,
    pub defaults: DatabaseDefaults,
    pub guarantees: GuaranteeSpec,
    pub domains: Vec<ConsistencyDomainSpec>,
    pub collections: Vec<CollectionRoleSpec>,
    pub extensions: Vec<ExtensionManifestRef>,
    pub overrides: BTreeMap<String, String>,
    pub operation_hints: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentSpec {
    pub mode: DeploymentMode,
    pub storage_path: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TopologySpec {
    pub replica_count: u16,
    pub shard_count: u16,
}

impl Default for TopologySpec {
    fn default() -> Self {
        Self {
            replica_count: 1,
            shard_count: 1,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentMode {
    Embedded,
    SingleNode,
    Cluster,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseDefaults {
    pub consistency_domain: String,
    pub replication: ReplicationMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationMode {
    Cp,
    Ap,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GuaranteeSpec {
    pub write_ack: WriteAck,
    pub conflict_resolution: ConflictResolution,
    pub backup: BackupGuarantee,
    pub encryption: EncryptionGuarantee,
    pub audit: AuditGuarantee,
    pub sensitive_data: bool,
    pub strict_cross_domain_transactions: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteAck {
    Local,
    Quorum,
    All,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictResolution {
    None,
    LastWriteWins,
    VectorClock,
    Crdt,
    Custom,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackupGuarantee {
    pub enabled: bool,
    pub pitr: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EncryptionGuarantee {
    pub at_rest: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuditGuarantee {
    pub enabled: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportStatus {
    Certified,
    Stable,
    Experimental,
    Custom,
    Invalid,
}

impl SupportStatus {
    const fn rank(self) -> u8 {
        match self {
            Self::Invalid => 0,
            Self::Custom => 1,
            Self::Experimental => 2,
            Self::Stable => 3,
            Self::Certified => 4,
        }
    }

    const fn weakest(self, other: Self) -> Self {
        if self.rank() <= other.rank() {
            self
        } else {
            other
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConsistencyDomainSpec {
    pub name: String,
    pub mode: ConsistencyMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsistencyMode {
    LocalSnapshot,
    StrongCp,
    EventualAp,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionRoleSpec {
    pub name: String,
    pub role: CollectionRole,
    pub domain: String,
    pub indexes: Vec<CollectionIndexKind>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CollectionRole {
    DocumentEntity,
    KeyValue,
    EventLog,
    VectorMemory,
    Cache,
    Audit,
    Graph,
    Analytics,
    TimeSeries,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CollectionIndexKind {
    Primary,
    Document,
    Vector,
    Graph,
    FullText,
    Columnar,
    TimeSeries,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProfileSpec {
    pub slug: &'static str,
    pub aliases: &'static [&'static str],
    pub status: SupportStatus,
    pub description: &'static str,
    pub default_domain: ConsistencyMode,
    pub compatible_roles: &'static [CollectionRole],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CollectionRoleDefinition {
    pub role: CollectionRole,
    pub slug: &'static str,
    pub status: SupportStatus,
    pub description: &'static str,
    pub required_capabilities: &'static [&'static str],
    pub constraints: &'static [&'static str],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsistencyDomainDefinition {
    pub mode: ConsistencyMode,
    pub slug: &'static str,
    pub status: SupportStatus,
    pub guarantees: &'static [&'static str],
    pub limits: &'static [&'static str],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct ExtensionCapabilityDefinition {
    pub slug: &'static str,
    pub status: SupportStatus,
    pub source: &'static str,
    pub description: &'static str,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionManifest {
    pub name: String,
    pub version: String,
    pub compatible_multidb: String,
    pub status: SupportStatus,
    pub provides: ExtensionProvides,
    pub registries: ExtensionRegistries,
    pub capabilities: Vec<String>,
    pub config_schema: JsonValue,
    pub limitations: Vec<String>,
    pub migrations: Vec<ExtensionMigration>,
    pub ui_panels: Vec<ExtensionUiPanel>,
    pub core_boundary: ExtensionCoreBoundary,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionProvides {
    pub types: Vec<String>,
    pub indexes: Vec<String>,
    pub operators: Vec<String>,
    pub storage_strategies: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionRegistries {
    pub types: Vec<ExtensionRegistryEntry>,
    pub indexes: Vec<ExtensionRegistryEntry>,
    pub operators: Vec<ExtensionRegistryEntry>,
    pub storage_strategies: Vec<ExtensionRegistryEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionRegistryEntry {
    pub id: String,
    pub status: SupportStatus,
    pub required_capabilities: Vec<String>,
    pub description: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionMigration {
    pub id: String,
    pub from: String,
    pub to: String,
    pub kind: String,
    pub requires_downtime: bool,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionUiPanel {
    pub id: String,
    pub title: String,
    pub route: String,
    pub required_capabilities: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionCoreBoundary {
    pub wal: ExtensionCoreOwner,
    pub transactions: ExtensionCoreOwner,
    pub recovery: ExtensionCoreOwner,
    pub security: ExtensionCoreOwner,
    pub rbac: ExtensionCoreOwner,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionCoreOwner {
    CoreOwned,
    ExtensionOwned,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ExtensionCatalogEntry {
    pub slug: String,
    pub status: SupportStatus,
    pub source: String,
    pub description: String,
    pub manifest: ExtensionManifest,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompiledExtensionCatalog {
    pub manifests: Vec<String>,
    pub capabilities: Vec<String>,
    pub types: Vec<String>,
    pub indexes: Vec<String>,
    pub operators: Vec<String>,
    pub storage_strategies: Vec<String>,
    pub ui_panels: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionManifestRef {
    pub name: String,
    pub version: String,
    pub stability: ExtensionStability,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionStability {
    Stable,
    Experimental,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationReport {
    pub valid: bool,
    pub status: SupportStatus,
    pub issues: Vec<ValidationIssue>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationIssue {
    pub code: String,
    pub severity: ValidationSeverity,
    pub path: String,
    pub message: String,
    pub suggestion: String,
    pub certification_impact: CertificationImpact,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationSeverity {
    Error,
    Warning,
    Advice,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CertificationImpact {
    None,
    BlocksCertified,
    DowngradesToCustom,
    DowngradesToExperimental,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompiledPolicy {
    pub storage_profile: String,
    pub replication_kind: ReplicationMode,
    pub topology: TopologySpec,
    pub required_extensions: Vec<String>,
    pub runtime_limits: RuntimeLimits,
    pub collections: Vec<CompiledCollectionPolicy>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeLimits {
    pub max_value_bytes: usize,
    pub max_batch_ops: usize,
    pub max_concurrent_queries: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompiledCollectionPolicy {
    pub name: String,
    pub role: CollectionRole,
    pub domain: String,
    pub indexes: Vec<CollectionIndexKind>,
    pub required_capabilities: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExplainConfigReport {
    pub validation: ValidationReport,
    pub compiled_policy: Option<CompiledPolicy>,
    pub decisions: Vec<ExplainDecision>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExplainDecision {
    pub path: String,
    pub value: String,
    pub source: String,
    pub reason: String,
    pub outcome: String,
    pub impact: PlanImpact,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ImpactLevel {
    #[default]
    None,
    Low,
    Medium,
    High,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    #[default]
    None,
    Low,
    Medium,
    High,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PlanImpact {
    pub downtime: ImpactLevel,
    pub disk: ImpactLevel,
    pub cpu: ImpactLevel,
    pub risk: RiskLevel,
    pub requires_backup: bool,
    pub requires_downtime: bool,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationPlan {
    pub plan_id: String,
    pub valid: bool,
    pub apply_supported: bool,
    pub current_validation: ValidationReport,
    pub desired_validation: ValidationReport,
    pub steps: Vec<MigrationStep>,
    pub impact: PlanImpact,
    pub rollback: RollbackPlan,
    pub required_confirmation: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationStep {
    pub step_id: String,
    pub kind: MigrationStepKind,
    pub path: String,
    pub action: String,
    pub impact: PlanImpact,
    pub rollback: String,
    pub requires_confirmation: bool,
    pub supported: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationStepKind {
    ValidateCurrent,
    ValidateDesired,
    ChangeProfile,
    ChangeDeployment,
    ChangeTopology,
    ChangeGuarantee,
    ChangeDomain,
    ChangeCollection,
    ChangeIndex,
    ChangeExtension,
    ChangeOverride,
    ChangeOperationHint,
    Noop,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RollbackPlan {
    pub possible: bool,
    pub description: String,
    pub steps: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_excessive_bools)]
pub struct ApplyCheckReport {
    pub plan_id: String,
    pub status: ApplyStatus,
    pub valid: bool,
    pub confirmation_matched: bool,
    pub audit_recorded: bool,
    pub data_mutated: bool,
    pub message: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplyStatus {
    Confirmed,
    Rejected,
    Unsupported,
}

pub struct GuaranteeValidator;

pub struct PolicyCompiler;

pub struct ConfigExplainer;

pub struct MigrationPlanner;

pub struct ExtensionManifestValidator;

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum ConfigSpecError {
    #[error("unsupported DatabaseSpec version: {found}")]
    UnsupportedVersion { found: u32 },

    #[error("field {field} must not be empty")]
    EmptyField { field: &'static str },

    #[error("duplicate {kind}: {name}")]
    Duplicate { kind: &'static str, name: String },

    #[error("collection {collection} references unknown domain {domain}")]
    UnknownDomain { collection: String, domain: String },
}

impl GuaranteeSpec {
    #[must_use]
    pub const fn for_replication(replication: ReplicationMode) -> Self {
        Self {
            write_ack: WriteAck::Quorum,
            conflict_resolution: match replication {
                ReplicationMode::Cp => ConflictResolution::None,
                ReplicationMode::Ap => ConflictResolution::VectorClock,
            },
            backup: BackupGuarantee {
                enabled: false,
                pitr: false,
            },
            encryption: EncryptionGuarantee { at_rest: false },
            audit: AuditGuarantee { enabled: true },
            sensitive_data: false,
            strict_cross_domain_transactions: false,
        }
    }
}

impl Default for AuditGuarantee {
    fn default() -> Self {
        Self { enabled: true }
    }
}

impl ValidationReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.issues
            .iter()
            .any(|issue| issue.severity == ValidationSeverity::Error)
    }
}

impl RuntimeLimits {
    #[must_use]
    pub fn for_profile(profile: &str) -> Self {
        match profile {
            "production_cp" => Self {
                max_value_bytes: 4 * 1_024 * 1_024,
                max_batch_ops: 2_048,
                max_concurrent_queries: 16,
            },
            "analytics_columnar" => Self {
                max_value_bytes: 2 * 1_024 * 1_024,
                max_batch_ops: 4_096,
                max_concurrent_queries: 16,
            },
            _ => Self {
                max_value_bytes: 1_048_576,
                max_batch_ops: 1_024,
                max_concurrent_queries: 8,
            },
        }
    }
}

fn validation_issue(
    code: &str,
    severity: ValidationSeverity,
    path: impl Into<String>,
    message: impl Into<String>,
    suggestion: impl Into<String>,
    certification_impact: CertificationImpact,
) -> ValidationIssue {
    ValidationIssue {
        code: code.to_owned(),
        severity,
        path: path.into(),
        message: message.into(),
        suggestion: suggestion.into(),
        certification_impact,
    }
}

impl ExtensionManifestValidator {
    #[must_use]
    pub fn validate(manifest: &ExtensionManifest) -> ValidationReport {
        let mut issues = Vec::new();

        require_manifest_text("name", &manifest.name, &mut issues);
        require_manifest_text("version", &manifest.version, &mut issues);
        require_manifest_text(
            "compatible_multidb",
            &manifest.compatible_multidb,
            &mut issues,
        );
        validate_unique_manifest_values(
            "capabilities",
            &manifest.capabilities,
            "$.capabilities",
            &mut issues,
        );

        validate_provides_bucket(
            "types",
            &manifest.provides.types,
            "$.provides.types",
            &mut issues,
        );
        validate_provides_bucket(
            "indexes",
            &manifest.provides.indexes,
            "$.provides.indexes",
            &mut issues,
        );
        validate_provides_bucket(
            "operators",
            &manifest.provides.operators,
            "$.provides.operators",
            &mut issues,
        );
        validate_provides_bucket(
            "storage_strategies",
            &manifest.provides.storage_strategies,
            "$.provides.storage_strategies",
            &mut issues,
        );

        validate_registry_bucket(
            "types",
            &manifest.registries.types,
            &manifest.provides.types,
            &manifest.capabilities,
            "$.registries.types",
            &mut issues,
        );
        validate_registry_bucket(
            "indexes",
            &manifest.registries.indexes,
            &manifest.provides.indexes,
            &manifest.capabilities,
            "$.registries.indexes",
            &mut issues,
        );
        validate_registry_bucket(
            "operators",
            &manifest.registries.operators,
            &manifest.provides.operators,
            &manifest.capabilities,
            "$.registries.operators",
            &mut issues,
        );
        validate_registry_bucket(
            "storage_strategies",
            &manifest.registries.storage_strategies,
            &manifest.provides.storage_strategies,
            &manifest.capabilities,
            "$.registries.storage_strategies",
            &mut issues,
        );

        validate_core_boundary(&manifest.core_boundary, &mut issues);
        validate_manifest_migrations(&manifest.migrations, &mut issues);
        validate_manifest_ui_panels(&manifest.ui_panels, &manifest.capabilities, &mut issues);

        let valid = !issues
            .iter()
            .any(|issue| issue.severity == ValidationSeverity::Error);
        ValidationReport {
            valid,
            status: if valid {
                manifest.status
            } else {
                SupportStatus::Invalid
            },
            issues,
        }
    }
}

fn role_required_extensions(role: CollectionRole) -> &'static [&'static str] {
    match role {
        CollectionRole::EventLog => &["cdc"],
        CollectionRole::VectorMemory => &["vector_hnsw"],
        CollectionRole::Audit => &["audit"],
        CollectionRole::Graph => &["graph_index"],
        CollectionRole::Analytics => &["columnar_layout"],
        CollectionRole::TimeSeries => &["time_series"],
        CollectionRole::DocumentEntity | CollectionRole::KeyValue | CollectionRole::Cache => &[],
    }
}

fn index_required_extensions(indexes: &[CollectionIndexKind]) -> Vec<String> {
    let mut required = BTreeSet::new();
    for index in indexes {
        match index {
            CollectionIndexKind::Document => {
                required.insert("document_index");
            }
            CollectionIndexKind::Vector => {
                required.insert("vector_hnsw");
            }
            CollectionIndexKind::Graph => {
                required.insert("graph_index");
            }
            CollectionIndexKind::FullText => {
                required.insert("full_text");
            }
            CollectionIndexKind::Columnar => {
                required.insert("columnar_layout");
            }
            CollectionIndexKind::TimeSeries => {
                required.insert("time_series");
            }
            CollectionIndexKind::Primary => {}
        }
    }
    required.into_iter().map(str::to_owned).collect()
}

fn require_manifest_text(field: &'static str, value: &str, issues: &mut Vec<ValidationIssue>) {
    if value.trim().is_empty() {
        issues.push(validation_issue(
            "EXTENSION_MANIFEST_EMPTY_FIELD",
            ValidationSeverity::Error,
            format!("$.{field}"),
            format!("extension manifest field {field} must not be empty"),
            "Provide a stable non-empty manifest value.",
            CertificationImpact::BlocksCertified,
        ));
    }
}

fn validate_provides_bucket(
    bucket: &'static str,
    values: &[String],
    path: &'static str,
    issues: &mut Vec<ValidationIssue>,
) {
    validate_unique_manifest_values(bucket, values, path, issues);
}

fn validate_unique_manifest_values(
    bucket: &'static str,
    values: &[String],
    path: &'static str,
    issues: &mut Vec<ValidationIssue>,
) {
    let mut seen = BTreeSet::new();
    for (index, value) in values.iter().enumerate() {
        if value.trim().is_empty() {
            issues.push(validation_issue(
                "EXTENSION_MANIFEST_EMPTY_FIELD",
                ValidationSeverity::Error,
                format!("{path}[{index}]"),
                format!("extension manifest {bucket} entry must not be empty"),
                "Remove the empty entry or replace it with a stable identifier.",
                CertificationImpact::BlocksCertified,
            ));
        } else if !seen.insert(value.as_str()) {
            issues.push(validation_issue(
                "EXTENSION_MANIFEST_DUPLICATE",
                ValidationSeverity::Error,
                format!("{path}[{index}]"),
                format!("duplicate extension manifest {bucket} entry {value}"),
                "Keep each extension manifest entry unique.",
                CertificationImpact::BlocksCertified,
            ));
        }
    }
}

fn validate_registry_bucket(
    bucket: &'static str,
    entries: &[ExtensionRegistryEntry],
    provides: &[String],
    capabilities: &[String],
    path: &'static str,
    issues: &mut Vec<ValidationIssue>,
) {
    let declared_provides = provides.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let manifest_capabilities = capabilities
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();

    for (index, entry) in entries.iter().enumerate() {
        if entry.id.trim().is_empty() {
            issues.push(validation_issue(
                "EXTENSION_REGISTRY_EMPTY_ID",
                ValidationSeverity::Error,
                format!("{path}[{index}].id"),
                format!("extension registry {bucket} entry id must not be empty"),
                "Give every registry entry a stable identifier.",
                CertificationImpact::BlocksCertified,
            ));
            continue;
        }

        if !seen.insert(entry.id.as_str()) {
            issues.push(validation_issue(
                "EXTENSION_REGISTRY_DUPLICATE_ID",
                ValidationSeverity::Error,
                format!("{path}[{index}].id"),
                format!("duplicate extension registry {bucket} entry {}", entry.id),
                "Keep registry entry identifiers unique within each registry.",
                CertificationImpact::BlocksCertified,
            ));
        }

        if !declared_provides.contains(entry.id.as_str()) {
            issues.push(validation_issue(
                "EXTENSION_REGISTRY_WITHOUT_PROVIDE",
                ValidationSeverity::Error,
                format!("{path}[{index}].id"),
                format!(
                    "registry {bucket} entry {} is not declared in provides.{bucket}",
                    entry.id
                ),
                "Declare the entry in the matching provides bucket or remove it from the registry.",
                CertificationImpact::BlocksCertified,
            ));
        }

        if entry.required_capabilities.is_empty() {
            issues.push(validation_issue(
                "EXTENSION_REGISTRY_CAPABILITY_REQUIRED",
                ValidationSeverity::Error,
                format!("{path}[{index}].required_capabilities"),
                format!(
                    "registry {bucket} entry {} must declare at least one core capability",
                    entry.id
                ),
                "List the core capability required to activate this registry entry.",
                CertificationImpact::BlocksCertified,
            ));
        }

        for (capability_index, capability) in entry.required_capabilities.iter().enumerate() {
            if capability.trim().is_empty() {
                issues.push(validation_issue(
                    "EXTENSION_REGISTRY_EMPTY_CAPABILITY",
                    ValidationSeverity::Error,
                    format!("{path}[{index}].required_capabilities[{capability_index}]"),
                    format!(
                        "registry {bucket} entry {} has an empty capability",
                        entry.id
                    ),
                    "Remove the empty capability or replace it with a stable capability id.",
                    CertificationImpact::BlocksCertified,
                ));
            } else if !manifest_capabilities.contains(capability.as_str()) {
                issues.push(validation_issue(
                    "EXTENSION_REGISTRY_UNKNOWN_CAPABILITY",
                    ValidationSeverity::Error,
                    format!("{path}[{index}].required_capabilities[{capability_index}]"),
                    format!(
                        "registry {bucket} entry {} requires undeclared capability {capability}",
                        entry.id
                    ),
                    "Declare the required capability in manifest.capabilities.",
                    CertificationImpact::BlocksCertified,
                ));
            }
        }
    }
}

fn validate_core_boundary(boundary: &ExtensionCoreBoundary, issues: &mut Vec<ValidationIssue>) {
    for (field, owner) in [
        ("wal", boundary.wal),
        ("transactions", boundary.transactions),
        ("recovery", boundary.recovery),
        ("security", boundary.security),
        ("rbac", boundary.rbac),
    ] {
        if owner != ExtensionCoreOwner::CoreOwned {
            issues.push(validation_issue(
                "EXTENSION_CORE_BOUNDARY_VIOLATION",
                ValidationSeverity::Error,
                format!("$.core_boundary.{field}"),
                format!("extension manifest cannot own core {field} guarantees"),
                "Set the boundary owner to core_owned; extensions must go through core APIs.",
                CertificationImpact::BlocksCertified,
            ));
        }
    }
}

fn validate_manifest_migrations(
    migrations: &[ExtensionMigration],
    issues: &mut Vec<ValidationIssue>,
) {
    let mut seen = BTreeSet::new();
    for (index, migration) in migrations.iter().enumerate() {
        if migration.id.trim().is_empty() {
            issues.push(validation_issue(
                "EXTENSION_MIGRATION_EMPTY_ID",
                ValidationSeverity::Error,
                format!("$.migrations[{index}].id"),
                "extension migration id must not be empty",
                "Give every extension migration a stable identifier.",
                CertificationImpact::BlocksCertified,
            ));
        } else if !seen.insert(migration.id.as_str()) {
            issues.push(validation_issue(
                "EXTENSION_MIGRATION_DUPLICATE_ID",
                ValidationSeverity::Error,
                format!("$.migrations[{index}].id"),
                format!("duplicate extension migration id {}", migration.id),
                "Keep migration identifiers unique within the manifest.",
                CertificationImpact::BlocksCertified,
            ));
        }

        for (field, value) in [
            ("from", migration.from.as_str()),
            ("to", migration.to.as_str()),
            ("kind", migration.kind.as_str()),
        ] {
            if value.trim().is_empty() {
                issues.push(validation_issue(
                    "EXTENSION_MIGRATION_EMPTY_FIELD",
                    ValidationSeverity::Error,
                    format!("$.migrations[{index}].{field}"),
                    format!("extension migration field {field} must not be empty"),
                    "Provide explicit migration version metadata.",
                    CertificationImpact::BlocksCertified,
                ));
            }
        }
    }
}

fn validate_manifest_ui_panels(
    panels: &[ExtensionUiPanel],
    capabilities: &[String],
    issues: &mut Vec<ValidationIssue>,
) {
    let manifest_capabilities = capabilities
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    for (index, panel) in panels.iter().enumerate() {
        if panel.id.trim().is_empty() {
            issues.push(validation_issue(
                "EXTENSION_UI_PANEL_EMPTY_ID",
                ValidationSeverity::Error,
                format!("$.ui_panels[{index}].id"),
                "extension UI panel id must not be empty",
                "Give every UI panel a stable identifier.",
                CertificationImpact::BlocksCertified,
            ));
        } else if !seen.insert(panel.id.as_str()) {
            issues.push(validation_issue(
                "EXTENSION_UI_PANEL_DUPLICATE_ID",
                ValidationSeverity::Error,
                format!("$.ui_panels[{index}].id"),
                format!("duplicate extension UI panel id {}", panel.id),
                "Keep UI panel identifiers unique within the manifest.",
                CertificationImpact::BlocksCertified,
            ));
        }

        for (field, value) in [
            ("title", panel.title.as_str()),
            ("route", panel.route.as_str()),
        ] {
            if value.trim().is_empty() {
                issues.push(validation_issue(
                    "EXTENSION_UI_PANEL_EMPTY_FIELD",
                    ValidationSeverity::Error,
                    format!("$.ui_panels[{index}].{field}"),
                    format!("extension UI panel field {field} must not be empty"),
                    "Provide a title and route for every extension UI panel.",
                    CertificationImpact::BlocksCertified,
                ));
            }
        }

        for (capability_index, capability) in panel.required_capabilities.iter().enumerate() {
            if !manifest_capabilities.contains(capability.as_str()) {
                issues.push(validation_issue(
                    "EXTENSION_UI_PANEL_UNKNOWN_CAPABILITY",
                    ValidationSeverity::Error,
                    format!("$.ui_panels[{index}].required_capabilities[{capability_index}]"),
                    format!(
                        "extension UI panel {} requires undeclared capability {capability}",
                        panel.id
                    ),
                    "Only reference capabilities declared by the extension manifest.",
                    CertificationImpact::BlocksCertified,
                ));
            }
        }
    }
}

impl DatabaseSpec {
    #[must_use]
    pub fn from_db_config(name: impl Into<String>, config: &DbConfig) -> Self {
        let profile = profile_slug(config.profile).to_owned();
        let replication = ReplicationMode::from(config.replication);
        let domain_mode = match replication {
            ReplicationMode::Cp => ConsistencyMode::StrongCp,
            ReplicationMode::Ap => ConsistencyMode::EventualAp,
        };

        Self {
            version: DATABASE_SPEC_VERSION,
            name: name.into(),
            profile,
            deployment: DeploymentSpec {
                mode: if config.profile == Profile::InMemory {
                    DeploymentMode::Embedded
                } else {
                    DeploymentMode::SingleNode
                },
                storage_path: config
                    .path
                    .as_ref()
                    .map(|path| path.to_string_lossy().into_owned()),
            },
            topology: TopologySpec::default(),
            defaults: DatabaseDefaults {
                consistency_domain: DEFAULT_DOMAIN.to_owned(),
                replication,
            },
            guarantees: GuaranteeSpec::for_replication(replication),
            domains: vec![ConsistencyDomainSpec {
                name: DEFAULT_DOMAIN.to_owned(),
                mode: domain_mode,
            }],
            collections: Vec::new(),
            extensions: Vec::new(),
            overrides: BTreeMap::new(),
            operation_hints: BTreeMap::new(),
        }
    }

    pub fn validate_structure(&self) -> Result<(), ConfigSpecError> {
        if self.version != DATABASE_SPEC_VERSION {
            return Err(ConfigSpecError::UnsupportedVersion {
                found: self.version,
            });
        }

        require_non_empty("name", &self.name)?;
        require_non_empty("profile", &self.profile)?;
        require_non_empty(
            "defaults.consistency_domain",
            &self.defaults.consistency_domain,
        )?;

        let domains = unique_names(
            self.domains.iter().map(|domain| domain.name.as_str()),
            "domain",
        )?;
        if !domains.contains(self.defaults.consistency_domain.as_str()) {
            return Err(ConfigSpecError::UnknownDomain {
                collection: "defaults".to_owned(),
                domain: self.defaults.consistency_domain.clone(),
            });
        }

        unique_names(
            self.collections
                .iter()
                .map(|collection| collection.name.as_str()),
            "collection",
        )?;
        unique_names(
            self.extensions
                .iter()
                .map(|extension| extension.name.as_str()),
            "extension",
        )?;

        for collection in &self.collections {
            require_non_empty("collections.name", &collection.name)?;
            require_non_empty("collections.domain", &collection.domain)?;
            if !domains.contains(collection.domain.as_str()) {
                return Err(ConfigSpecError::UnknownDomain {
                    collection: collection.name.clone(),
                    domain: collection.domain.clone(),
                });
            }
        }

        for extension in &self.extensions {
            require_non_empty("extensions.name", &extension.name)?;
            require_non_empty("extensions.version", &extension.version)?;
        }

        validate_map_keys("overrides", &self.overrides)?;
        validate_map_keys("operation_hints", &self.operation_hints)?;

        Ok(())
    }

    #[must_use]
    pub fn catalog_support_status(&self) -> SupportStatus {
        GuaranteeValidator::validate(self).status
    }

    fn catalog_support_status_without_guarantees(&self) -> SupportStatus {
        if self.validate_structure().is_err() {
            return SupportStatus::Invalid;
        }

        let Some(profile) = built_in_profile(&self.profile) else {
            return SupportStatus::Custom;
        };

        let mut status = profile.status;
        for domain in &self.domains {
            let Some(definition) = consistency_domain_definition(domain.mode) else {
                return SupportStatus::Invalid;
            };
            status = status.weakest(definition.status);
        }

        for collection in &self.collections {
            if !profile.compatible_roles.contains(&collection.role) {
                return SupportStatus::Custom;
            }
            let Some(definition) = collection_role_definition(collection.role) else {
                return SupportStatus::Invalid;
            };
            status = status.weakest(definition.status);
        }

        status
    }
}

impl GuaranteeValidator {
    #[must_use]
    pub fn validate(spec: &DatabaseSpec) -> ValidationReport {
        let mut issues = Vec::new();

        if let Err(error) = spec.validate_structure() {
            issues.push(validation_issue(
                "STRUCTURE_INVALID",
                ValidationSeverity::Error,
                "$",
                format!("DatabaseSpec structure is invalid: {error}"),
                "Fix the structural DatabaseSpec error before checking product guarantees.",
                CertificationImpact::BlocksCertified,
            ));
            return ValidationReport {
                valid: false,
                status: SupportStatus::Invalid,
                issues,
            };
        }

        let mut status = spec.catalog_support_status_without_guarantees();
        let profile = built_in_profile(&spec.profile);
        if profile.is_none() {
            issues.push(validation_issue(
                "CUSTOM_PROFILE",
                ValidationSeverity::Warning,
                "profile",
                format!(
                    "profile {} is not part of the built-in support catalog",
                    spec.profile
                ),
                "Use a built-in profile or treat this configuration as Custom.",
                CertificationImpact::DowngradesToCustom,
            ));
        }

        validate_topology(spec, &mut issues);

        if let Some(profile) = profile {
            for (index, collection) in spec.collections.iter().enumerate() {
                if !profile.compatible_roles.contains(&collection.role) {
                    issues.push(validation_issue(
                        "ROLE_OUTSIDE_PROFILE_CERTIFICATION",
                        ValidationSeverity::Warning,
                        format!("collections[{index}].role"),
                        format!(
                            "collection {} uses role {:?}, which is outside profile {} certification",
                            collection.name, collection.role, profile.slug
                        ),
                        "Move the collection to a compatible profile or accept Custom support status.",
                        CertificationImpact::DowngradesToCustom,
                    ));
                }
            }
        }

        let explicit_extensions = spec
            .extensions
            .iter()
            .map(|extension| extension.name.as_str())
            .collect::<BTreeSet<_>>();
        for (index, extension) in spec.extensions.iter().enumerate() {
            if extension.stability == ExtensionStability::Experimental {
                status = status.weakest(SupportStatus::Experimental);
                issues.push(validation_issue(
                    "EXPERIMENTAL_EXTENSION",
                    ValidationSeverity::Warning,
                    format!("extensions[{index}].stability"),
                    format!("extension {} is marked experimental", extension.name),
                    "Use a stable extension for certified configurations, or accept Experimental support status.",
                    CertificationImpact::DowngradesToExperimental,
                ));
            }

            if let Some(manifest) = built_in_extension_manifest(&extension.name) {
                status = status.weakest(manifest.status);
                let manifest_report = ExtensionManifestValidator::validate(manifest);
                if manifest_report.has_errors() {
                    issues.push(validation_issue(
                        "BUILTIN_EXTENSION_MANIFEST_INVALID",
                        ValidationSeverity::Error,
                        format!("extensions[{index}]"),
                        format!(
                            "built-in extension manifest {} failed validation",
                            extension.name
                        ),
                        "Fix the built-in extension manifest before using this catalog.",
                        CertificationImpact::BlocksCertified,
                    ));
                }
            } else {
                status = status.weakest(SupportStatus::Custom);
                issues.push(validation_issue(
                    "CUSTOM_EXTENSION",
                    ValidationSeverity::Warning,
                    format!("extensions[{index}].name"),
                    format!(
                        "extension {} is not part of the built-in extension manifest catalog",
                        extension.name
                    ),
                    "Provide an extension manifest and treat the configuration as Custom.",
                    CertificationImpact::DowngradesToCustom,
                ));
            }
        }

        let mut required_extensions = BTreeSet::new();
        for collection in &spec.collections {
            for extension in role_required_extensions(collection.role) {
                required_extensions.insert((*extension).to_owned());
            }
            for extension in index_required_extensions(&collection.indexes) {
                required_extensions.insert(extension);
            }
        }
        for extension in required_extensions {
            if built_in_extension_manifest(&extension).is_some()
                && !explicit_extensions.contains(extension.as_str())
            {
                issues.push(validation_issue(
                    "IMPLICIT_EXTENSION_REQUIRED",
                    ValidationSeverity::Advice,
                    "$.collections",
                    format!("collection roles or indexes require extension {extension}"),
                    "The policy compiler will include this built-in extension in the runtime requirements.",
                    CertificationImpact::None,
                ));
            }
        }

        let has_strong_cp = spec.defaults.replication == ReplicationMode::Cp
            || spec
                .domains
                .iter()
                .any(|domain| domain.mode == ConsistencyMode::StrongCp);
        if has_strong_cp && spec.guarantees.write_ack == WriteAck::Local {
            issues.push(validation_issue(
                "CP_LOCAL_ACK",
                ValidationSeverity::Error,
                "guarantees.write_ack",
                "strong CP cannot be certified with local write acknowledgement",
                "Use write_ack=quorum or write_ack=all for strong CP guarantees.",
                CertificationImpact::BlocksCertified,
            ));
        }

        let has_eventual_ap = spec.defaults.replication == ReplicationMode::Ap
            || spec
                .domains
                .iter()
                .any(|domain| domain.mode == ConsistencyMode::EventualAp);
        if has_eventual_ap && spec.guarantees.conflict_resolution == ConflictResolution::None {
            issues.push(validation_issue(
                "AP_MISSING_CONFLICT_POLICY",
                ValidationSeverity::Error,
                "guarantees.conflict_resolution",
                "AP/eventual consistency requires an explicit conflict resolution policy",
                "Set conflict_resolution to vector_clock, last_write_wins, crdt, or custom.",
                CertificationImpact::BlocksCertified,
            ));
        }

        if profile.is_some_and(|profile| profile.slug == "production_cp")
            && !spec.guarantees.backup.enabled
        {
            issues.push(validation_issue(
                "PRODUCTION_BACKUP_DISABLED",
                ValidationSeverity::Error,
                "guarantees.backup.enabled",
                "production_cp requires backup to be enabled",
                "Enable backup before using the production_cp profile.",
                CertificationImpact::BlocksCertified,
            ));
        }

        if spec.guarantees.sensitive_data && !spec.guarantees.encryption.at_rest {
            issues.push(validation_issue(
                "SENSITIVE_WITHOUT_ENCRYPTION",
                ValidationSeverity::Error,
                "guarantees.encryption.at_rest",
                "sensitive data requires encryption at rest",
                "Enable encryption.at_rest or mark the data as non-sensitive.",
                CertificationImpact::BlocksCertified,
            ));
        }

        for (index, collection) in spec.collections.iter().enumerate() {
            match collection.role {
                CollectionRole::VectorMemory
                    if !collection.indexes.contains(&CollectionIndexKind::Vector) =>
                {
                    issues.push(validation_issue(
                        "VECTOR_INDEX_REQUIRED",
                        ValidationSeverity::Error,
                        format!("collections[{index}].indexes"),
                        format!(
                            "vector_memory collection {} requires a vector index",
                            collection.name
                        ),
                        "Add vector to the collection indexes.",
                        CertificationImpact::BlocksCertified,
                    ));
                }
                CollectionRole::Graph
                    if !collection.indexes.contains(&CollectionIndexKind::Graph) =>
                {
                    issues.push(validation_issue(
                        "GRAPH_INDEX_REQUIRED",
                        ValidationSeverity::Error,
                        format!("collections[{index}].indexes"),
                        format!(
                            "graph collection {} requires a graph index",
                            collection.name
                        ),
                        "Add graph to the collection indexes.",
                        CertificationImpact::BlocksCertified,
                    ));
                }
                CollectionRole::Audit if !spec.guarantees.audit.enabled => {
                    issues.push(validation_issue(
                        "AUDIT_DISABLED",
                        ValidationSeverity::Error,
                        "guarantees.audit.enabled",
                        format!(
                            "audit collection {} requires audit to be enabled",
                            collection.name
                        ),
                        "Enable audit guarantees or change the collection role.",
                        CertificationImpact::BlocksCertified,
                    ));
                }
                _ => {}
            }
        }

        if spec.guarantees.strict_cross_domain_transactions
            && spec
                .domains
                .iter()
                .any(|domain| domain.mode == ConsistencyMode::StrongCp)
            && spec
                .domains
                .iter()
                .any(|domain| domain.mode == ConsistencyMode::EventualAp)
        {
            issues.push(validation_issue(
                "STRICT_CP_AP_CROSS_DOMAIN_TXN",
                ValidationSeverity::Error,
                "guarantees.strict_cross_domain_transactions",
                "strict cross-domain transactions cannot span CP and AP consistency domains",
                "Use one consistency mode for strict transactions or disable strict cross-domain transactions.",
                CertificationImpact::BlocksCertified,
            ));
        }

        let valid = !issues
            .iter()
            .any(|issue| issue.severity == ValidationSeverity::Error);
        if !valid {
            status = SupportStatus::Invalid;
        }

        ValidationReport {
            valid,
            status,
            issues,
        }
    }
}

impl PolicyCompiler {
    pub fn compile(spec: &DatabaseSpec) -> Result<CompiledPolicy, ValidationReport> {
        let report = GuaranteeValidator::validate(spec);
        if report.has_errors() {
            return Err(report);
        }

        let storage_profile = built_in_profile(&spec.profile)
            .map_or_else(|| spec.profile.clone(), |profile| profile.slug.to_owned());
        let mut required_extensions = BTreeSet::new();
        for extension in &spec.extensions {
            required_extensions.insert(extension.name.clone());
        }

        let mut collections = Vec::with_capacity(spec.collections.len());
        for collection in &spec.collections {
            for extension in role_required_extensions(collection.role) {
                required_extensions.insert((*extension).to_owned());
            }
            for extension in index_required_extensions(&collection.indexes) {
                required_extensions.insert(extension);
            }

            let required_capabilities = collection_role_definition(collection.role)
                .map(|definition| {
                    definition
                        .required_capabilities
                        .iter()
                        .map(|capability| (*capability).to_owned())
                        .collect()
                })
                .unwrap_or_default();

            collections.push(CompiledCollectionPolicy {
                name: collection.name.clone(),
                role: collection.role,
                domain: collection.domain.clone(),
                indexes: collection.indexes.clone(),
                required_capabilities,
            });
        }

        Ok(CompiledPolicy {
            storage_profile: storage_profile.clone(),
            replication_kind: spec.defaults.replication,
            topology: spec.topology,
            required_extensions: required_extensions.into_iter().collect(),
            runtime_limits: RuntimeLimits::for_profile(&storage_profile),
            collections,
        })
    }
}

#[allow(clippy::collapsible_match, clippy::manual_is_multiple_of)]
fn validate_topology(spec: &DatabaseSpec, issues: &mut Vec<ValidationIssue>) {
    if spec.topology.replica_count == 0 {
        issues.push(validation_issue(
            "TOPOLOGY_REPLICA_COUNT_ZERO",
            ValidationSeverity::Error,
            "topology.replica_count",
            "replica_count must be at least 1",
            "Set replica_count to 1 for local/single-node databases, 3+ odd for CP clusters, or 2+ for AP clusters.",
            CertificationImpact::BlocksCertified,
        ));
    }

    if spec.topology.shard_count == 0 {
        issues.push(validation_issue(
            "TOPOLOGY_SHARD_COUNT_ZERO",
            ValidationSeverity::Error,
            "topology.shard_count",
            "shard_count must be at least 1",
            "Set shard_count to 1 unless this database is intentionally sharded.",
            CertificationImpact::BlocksCertified,
        ));
    }

    match spec.deployment.mode {
        DeploymentMode::Embedded | DeploymentMode::SingleNode
            if spec.topology.replica_count != 1 =>
        {
            issues.push(validation_issue(
                "TOPOLOGY_LOCAL_REPLICA_COUNT",
                ValidationSeverity::Error,
                "topology.replica_count",
                "embedded and single_node deployments must use exactly 1 replica",
                "Switch deployment.mode to cluster before increasing replica_count.",
                CertificationImpact::BlocksCertified,
            ));
        }
        DeploymentMode::Cluster if spec.defaults.replication == ReplicationMode::Cp => {
            if spec.topology.replica_count < 3 || spec.topology.replica_count % 2 == 0 {
                issues.push(validation_issue(
                    "TOPOLOGY_CP_CLUSTER_QUORUM",
                    ValidationSeverity::Error,
                    "topology.replica_count",
                    "cluster CP deployments require an odd replica_count of at least 3",
                    "Use 3, 5, or another odd replica count so quorum can be formed safely.",
                    CertificationImpact::BlocksCertified,
                ));
            }
        }
        DeploymentMode::Cluster if spec.defaults.replication == ReplicationMode::Ap => {
            if spec.topology.replica_count < 2 {
                issues.push(validation_issue(
                    "TOPOLOGY_AP_CLUSTER_REPLICAS",
                    ValidationSeverity::Error,
                    "topology.replica_count",
                    "cluster AP deployments require at least 2 replicas",
                    "Use replica_count=2 or higher for AP cluster replication.",
                    CertificationImpact::BlocksCertified,
                ));
            }
        }
        _ => {}
    }
}

impl ConfigExplainer {
    #[must_use]
    pub fn explain(spec: &DatabaseSpec) -> ExplainConfigReport {
        let validation = GuaranteeValidator::validate(spec);
        let compiled_policy = PolicyCompiler::compile(spec).ok();
        let mut decisions = Vec::new();

        push_decision(
            &mut decisions,
            "$.validation",
            describe_value(&validation.status),
            "GuaranteeValidator",
            "The validator combines structure, support catalog and guarantee checks.",
            if validation.valid {
                "configuration is valid"
            } else {
                "configuration is blocked"
            },
            impact_none(),
        );
        explain_catalog_decisions(spec, &mut decisions);
        explain_guarantee_decisions(spec, &mut decisions);
        explain_domain_decisions(spec, &mut decisions);
        explain_collection_decisions(spec, &mut decisions);
        explain_extension_decisions(spec, &mut decisions);

        if let Some(policy) = &compiled_policy {
            explain_compiled_policy(policy, &mut decisions);
        } else {
            push_decision(
                &mut decisions,
                "$.compiled_policy",
                "null".to_owned(),
                "PolicyCompiler",
                "Invalid guarantee reports block policy compilation.",
                "no runtime policy was produced",
                unsupported_impact(
                    "compiler output is unavailable until validation errors are fixed",
                ),
            );
        }

        ExplainConfigReport {
            validation,
            compiled_policy,
            decisions,
        }
    }
}

impl MigrationPlanner {
    #[must_use]
    pub fn plan(current: &DatabaseSpec, desired: &DatabaseSpec) -> MigrationPlan {
        let current_validation = GuaranteeValidator::validate(current);
        let desired_validation = GuaranteeValidator::validate(desired);
        let plan_id = stable_plan_id(current, desired);
        let mut steps = Vec::new();

        if !current_validation.valid {
            push_step(
                &mut steps,
                MigrationStepKind::ValidateCurrent,
                "$.current",
                "current spec must validate before a migration can be planned".to_owned(),
                unsupported_impact("current configuration has validation errors"),
                "Fix the current specification or re-export it from a valid running configuration.",
                false,
            );
        }
        if !desired_validation.valid {
            push_step(
                &mut steps,
                MigrationStepKind::ValidateDesired,
                "$.desired",
                "desired spec must validate before a migration can be planned".to_owned(),
                unsupported_impact("desired configuration has validation errors"),
                "Fix the desired specification and rerun the dry-run.",
                false,
            );
        }

        if current_validation.valid && desired_validation.valid {
            diff_top_level(current, desired, &mut steps);
            diff_domains(current, desired, &mut steps);
            diff_collections(current, desired, &mut steps);
            diff_extensions(current, desired, &mut steps);
            diff_string_map(
                "overrides",
                &current.overrides,
                &desired.overrides,
                MigrationStepKind::ChangeOverride,
                &mut steps,
            );
            diff_string_map(
                "operation_hints",
                &current.operation_hints,
                &desired.operation_hints,
                MigrationStepKind::ChangeOperationHint,
                &mut steps,
            );
        }

        if steps.is_empty() {
            push_step(
                &mut steps,
                MigrationStepKind::Noop,
                "$",
                "current and desired specs are identical".to_owned(),
                impact_none(),
                "No rollback action is required.",
                true,
            );
        }

        let impact = aggregate_impact(&steps);
        let valid = current_validation.valid && desired_validation.valid;
        let apply_supported = valid && steps.iter().all(|step| step.supported);
        let rollback = rollback_plan(&steps);

        MigrationPlan {
            plan_id: plan_id.clone(),
            valid,
            apply_supported,
            current_validation,
            desired_validation,
            steps,
            impact,
            rollback,
            required_confirmation: plan_id,
        }
    }

    #[must_use]
    pub fn check_apply(plan: &MigrationPlan, confirm_id: &str) -> ApplyCheckReport {
        if confirm_id != plan.required_confirmation {
            return ApplyCheckReport {
                plan_id: plan.plan_id.clone(),
                status: ApplyStatus::Rejected,
                valid: false,
                confirmation_matched: false,
                audit_recorded: false,
                data_mutated: false,
                message: format!(
                    "confirmation id mismatch: expected {}",
                    plan.required_confirmation
                ),
            };
        }

        if !plan.valid {
            return ApplyCheckReport {
                plan_id: plan.plan_id.clone(),
                status: ApplyStatus::Rejected,
                valid: false,
                confirmation_matched: true,
                audit_recorded: false,
                data_mutated: false,
                message: "plan is invalid because current or desired validation failed".to_owned(),
            };
        }

        if !plan.apply_supported {
            return ApplyCheckReport {
                plan_id: plan.plan_id.clone(),
                status: ApplyStatus::Unsupported,
                valid: false,
                confirmation_matched: true,
                audit_recorded: false,
                data_mutated: false,
                message: "plan is a dry-run/audit artifact; automatic apply is unsupported in config apply v1"
                    .to_owned(),
            };
        }

        ApplyCheckReport {
            plan_id: plan.plan_id.clone(),
            status: ApplyStatus::Confirmed,
            valid: true,
            confirmation_matched: true,
            audit_recorded: false,
            data_mutated: false,
            message: "plan id confirmed and audited as a no-op; no physical migration is executed"
                .to_owned(),
        }
    }
}

fn explain_catalog_decisions(spec: &DatabaseSpec, decisions: &mut Vec<ExplainDecision>) {
    let profile = built_in_profile(&spec.profile);
    push_decision(
        decisions,
        "$.profile",
        describe_value(&spec.profile),
        "ProfileCatalog",
        profile.map_or(
            "The profile is custom and outside the built-in support matrix.",
            |profile| profile.description,
        ),
        profile.map_or("custom support status", |profile| profile.slug),
        if profile.is_some() {
            impact_none()
        } else {
            low_risk_impact("custom profiles require operator review")
        },
    );
    push_decision(
        decisions,
        "$.deployment.mode",
        describe_value(&spec.deployment.mode),
        "DatabaseSpec",
        "Deployment mode determines whether the runtime is embedded, single-node or cluster-shaped.",
        "recorded for planner and control-plane consumers",
        impact_none(),
    );
    push_decision(
        decisions,
        "$.deployment.storage_path",
        describe_value(&spec.deployment.storage_path),
        "DatabaseSpec",
        "Storage path selects the durable backing location when the profile is on disk.",
        "recorded for migration impact checks",
        impact_none(),
    );
    push_decision(
        decisions,
        "$.topology",
        describe_value(&spec.topology),
        "DatabaseSpec",
        "Topology records the desired replica and shard shape for migration planning.",
        "used by validator and PolicyCompiler.topology",
        impact_none(),
    );
    push_decision(
        decisions,
        "$.defaults.replication",
        describe_value(&spec.defaults.replication),
        "DatabaseSpec",
        "Default replication chooses the CP or AP runtime policy for unspecified domains.",
        "used by PolicyCompiler.replication_kind",
        impact_none(),
    );
}

fn explain_guarantee_decisions(spec: &DatabaseSpec, decisions: &mut Vec<ExplainDecision>) {
    push_decision(
        decisions,
        "$.guarantees.write_ack",
        describe_value(&spec.guarantees.write_ack),
        "GuaranteeValidator",
        "Write acknowledgement is the durability boundary for CP-style writes.",
        "checked against consistency domain guarantees",
        impact_none(),
    );
    push_decision(
        decisions,
        "$.guarantees.conflict_resolution",
        describe_value(&spec.guarantees.conflict_resolution),
        "GuaranteeValidator",
        "AP configurations must declare how conflicting writes converge.",
        "checked against AP/eventual domains",
        impact_none(),
    );
    push_decision(
        decisions,
        "$.guarantees.backup",
        describe_value(&spec.guarantees.backup),
        "GuaranteeValidator",
        "Production CP profiles require backup coverage before certification.",
        if spec.guarantees.backup.enabled {
            "backup enabled"
        } else {
            "backup disabled"
        },
        if spec.guarantees.backup.enabled {
            impact_none()
        } else {
            low_risk_impact("disabled backups lower recovery confidence")
        },
    );
    push_decision(
        decisions,
        "$.guarantees.encryption.at_rest",
        describe_value(&spec.guarantees.encryption.at_rest),
        "GuaranteeValidator",
        "Sensitive data requires encryption at rest.",
        if spec.guarantees.encryption.at_rest {
            "encrypted storage required"
        } else {
            "unencrypted storage allowed only for non-sensitive data"
        },
        impact_none(),
    );
    push_decision(
        decisions,
        "$.guarantees.audit.enabled",
        describe_value(&spec.guarantees.audit.enabled),
        "GuaranteeValidator",
        "Audit collections require audit guarantees to remain enabled.",
        if spec.guarantees.audit.enabled {
            "audit enabled"
        } else {
            "audit disabled"
        },
        impact_none(),
    );
}

fn explain_domain_decisions(spec: &DatabaseSpec, decisions: &mut Vec<ExplainDecision>) {
    for domain in &spec.domains {
        let definition = consistency_domain_definition(domain.mode);
        push_decision(
            decisions,
            format!("$.domains.{}.mode", domain.name),
            describe_value(&domain.mode),
            "ConsistencyDomainCatalog",
            definition.map_or(
                "Unknown consistency domain mode cannot be cataloged.",
                |definition| {
                    definition
                        .guarantees
                        .first()
                        .copied()
                        .unwrap_or("cataloged domain")
                },
            ),
            definition.map_or("unknown domain", |definition| definition.slug),
            if definition.is_some() {
                impact_none()
            } else {
                unsupported_impact("unknown consistency mode")
            },
        );
    }
}

fn explain_collection_decisions(spec: &DatabaseSpec, decisions: &mut Vec<ExplainDecision>) {
    for collection in &spec.collections {
        let definition = collection_role_definition(collection.role);
        push_decision(
            decisions,
            format!("$.collections.{}.role", collection.name),
            describe_value(&collection.role),
            "CollectionRoleCatalog",
            definition.map_or(
                "Unknown collection role cannot be mapped to runtime capabilities.",
                |definition| definition.description,
            ),
            definition.map_or("unknown role", |definition| definition.slug),
            if definition.is_some() {
                impact_none()
            } else {
                unsupported_impact("unknown collection role")
            },
        );
        push_decision(
            decisions,
            format!("$.collections.{}.indexes", collection.name),
            describe_value(&collection.indexes),
            "PolicyCompiler",
            "Indexes add required runtime extensions and shape migration cost.",
            "included in compiled collection policy",
            if collection.indexes.is_empty() {
                low_risk_impact("collections without indexes may need scans")
            } else {
                impact_none()
            },
        );
    }
}

fn explain_extension_decisions(spec: &DatabaseSpec, decisions: &mut Vec<ExplainDecision>) {
    for extension in &spec.extensions {
        push_decision(
            decisions,
            format!("$.extensions.{}", extension.name),
            describe_value(extension),
            "ExtensionCatalog",
            "Extensions widen runtime capability and can downgrade support when experimental.",
            match extension.stability {
                ExtensionStability::Stable => "stable extension",
                ExtensionStability::Experimental => "experimental extension",
            },
            if extension.stability == ExtensionStability::Experimental {
                low_risk_impact("experimental extensions require support review")
            } else {
                impact_none()
            },
        );
    }
}

fn explain_compiled_policy(policy: &CompiledPolicy, decisions: &mut Vec<ExplainDecision>) {
    push_decision(
        decisions,
        "$.compiled_policy.storage_profile",
        describe_value(&policy.storage_profile),
        "PolicyCompiler",
        "The profile alias is resolved to the runtime storage profile.",
        "storage profile selected",
        impact_none(),
    );
    push_decision(
        decisions,
        "$.compiled_policy.replication_kind",
        describe_value(&policy.replication_kind),
        "PolicyCompiler",
        "Default replication becomes the runtime replication policy.",
        "replication policy selected",
        impact_none(),
    );
    push_decision(
        decisions,
        "$.compiled_policy.topology",
        describe_value(&policy.topology),
        "PolicyCompiler",
        "Desired topology is carried into compiled policy for operators and control-plane consumers.",
        "topology policy selected",
        impact_none(),
    );
    push_decision(
        decisions,
        "$.compiled_policy.runtime_limits",
        describe_value(&policy.runtime_limits),
        "PolicyCompiler",
        "Runtime limits are derived from the resolved storage profile.",
        "limits selected",
        impact_none(),
    );
    push_decision(
        decisions,
        "$.compiled_policy.required_extensions",
        describe_value(&policy.required_extensions),
        "PolicyCompiler",
        "Explicit extensions and role/index requirements are unioned deterministically.",
        "extension set selected",
        impact_none(),
    );
    for collection in &policy.collections {
        push_decision(
            decisions,
            format!("$.compiled_policy.collections.{}.role", collection.name),
            describe_value(&collection.role),
            "PolicyCompiler",
            "Collection role is preserved in the runtime policy.",
            "collection role selected",
            impact_none(),
        );
        push_decision(
            decisions,
            format!("$.compiled_policy.collections.{}.indexes", collection.name),
            describe_value(&collection.indexes),
            "PolicyCompiler",
            "Collection indexes are preserved in the runtime policy.",
            "collection indexes selected",
            impact_none(),
        );
        push_decision(
            decisions,
            format!(
                "$.compiled_policy.collections.{}.required_capabilities",
                collection.name
            ),
            describe_value(&collection.required_capabilities),
            "PolicyCompiler",
            "Collection role catalog capabilities become runtime requirements.",
            "collection capabilities selected",
            impact_none(),
        );
    }
}

fn diff_top_level(current: &DatabaseSpec, desired: &DatabaseSpec, steps: &mut Vec<MigrationStep>) {
    record_diff(
        steps,
        "$.profile",
        &current.profile,
        &desired.profile,
        diff_metadata(
            MigrationStepKind::ChangeProfile,
            unsupported_impact("profile changes can alter storage, replication and runtime limits"),
            "Restore the previous profile and re-run validation.",
            false,
        ),
    );
    record_diff(
        steps,
        "$.deployment.mode",
        &current.deployment.mode,
        &desired.deployment.mode,
        diff_metadata(
            MigrationStepKind::ChangeDeployment,
            unsupported_impact(
                "deployment mode changes are not physically switched by config apply v1",
            ),
            "Restore the previous deployment mode.",
            false,
        ),
    );
    record_diff(
        steps,
        "$.deployment.storage_path",
        &current.deployment.storage_path,
        &desired.deployment.storage_path,
        diff_metadata(
            MigrationStepKind::ChangeDeployment,
            unsupported_impact(
                "storage path changes require explicit operator-managed data movement",
            ),
            "Keep the existing storage path until an operator migration is complete.",
            false,
        ),
    );
    record_diff(
        steps,
        "$.topology.replica_count",
        &current.topology.replica_count,
        &desired.topology.replica_count,
        diff_metadata(
            MigrationStepKind::ChangeTopology,
            unsupported_impact("replica count changes require operator-managed topology migration"),
            "Restore the previous replica count until runtime topology migration is ready.",
            false,
        ),
    );
    record_diff(
        steps,
        "$.topology.shard_count",
        &current.topology.shard_count,
        &desired.topology.shard_count,
        diff_metadata(
            MigrationStepKind::ChangeTopology,
            unsupported_impact("shard count changes require operator-managed data placement"),
            "Restore the previous shard count until sharding migration is ready.",
            false,
        ),
    );
    record_diff(
        steps,
        "$.defaults.consistency_domain",
        &current.defaults.consistency_domain,
        &desired.defaults.consistency_domain,
        diff_metadata(
            MigrationStepKind::ChangeDomain,
            unsupported_impact(
                "default domain changes can move collections across consistency guarantees",
            ),
            "Restore the previous default domain.",
            false,
        ),
    );
    record_diff(
        steps,
        "$.defaults.replication",
        &current.defaults.replication,
        &desired.defaults.replication,
        diff_metadata(
            MigrationStepKind::ChangeGuarantee,
            unsupported_impact("replication kind changes require runtime topology migration"),
            "Restore the previous replication kind.",
            false,
        ),
    );
    diff_guarantees(&current.guarantees, &desired.guarantees, steps);
}

fn diff_guarantees(
    current: &GuaranteeSpec,
    desired: &GuaranteeSpec,
    steps: &mut Vec<MigrationStep>,
) {
    record_diff(
        steps,
        "$.guarantees.write_ack",
        &current.write_ack,
        &desired.write_ack,
        diff_metadata(
            MigrationStepKind::ChangeGuarantee,
            unsupported_impact("write acknowledgement changes alter durability semantics"),
            "Restore the previous write acknowledgement policy.",
            false,
        ),
    );
    record_diff(
        steps,
        "$.guarantees.conflict_resolution",
        &current.conflict_resolution,
        &desired.conflict_resolution,
        diff_metadata(
            MigrationStepKind::ChangeGuarantee,
            unsupported_impact("conflict resolution changes require data model review"),
            "Restore the previous conflict resolution policy.",
            false,
        ),
    );
    record_diff(
        steps,
        "$.guarantees.backup.enabled",
        &current.backup.enabled,
        &desired.backup.enabled,
        diff_metadata(
            MigrationStepKind::ChangeGuarantee,
            medium_impact("backup policy changes require backup state audit"),
            "Restore the previous backup enabled flag.",
            true,
        ),
    );
    record_diff(
        steps,
        "$.guarantees.backup.pitr",
        &current.backup.pitr,
        &desired.backup.pitr,
        diff_metadata(
            MigrationStepKind::ChangeGuarantee,
            medium_impact("PITR policy changes require retention and manifest review"),
            "Restore the previous PITR flag.",
            true,
        ),
    );
    record_diff(
        steps,
        "$.guarantees.encryption.at_rest",
        &current.encryption.at_rest,
        &desired.encryption.at_rest,
        diff_metadata(
            MigrationStepKind::ChangeGuarantee,
            unsupported_impact(
                "encryption-at-rest changes require physical data rewrite or key rollout",
            ),
            "Restore the previous encryption-at-rest flag.",
            false,
        ),
    );
    record_diff(
        steps,
        "$.guarantees.audit.enabled",
        &current.audit.enabled,
        &desired.audit.enabled,
        diff_metadata(
            MigrationStepKind::ChangeGuarantee,
            medium_impact("audit guarantee changes require audit trail review"),
            "Restore the previous audit flag.",
            true,
        ),
    );
    record_diff(
        steps,
        "$.guarantees.sensitive_data",
        &current.sensitive_data,
        &desired.sensitive_data,
        diff_metadata(
            MigrationStepKind::ChangeGuarantee,
            unsupported_impact("sensitive data classification changes require security review"),
            "Restore the previous sensitive data flag.",
            false,
        ),
    );
    record_diff(
        steps,
        "$.guarantees.strict_cross_domain_transactions",
        &current.strict_cross_domain_transactions,
        &desired.strict_cross_domain_transactions,
        diff_metadata(
            MigrationStepKind::ChangeGuarantee,
            unsupported_impact("cross-domain transaction guarantees alter transactional semantics"),
            "Restore the previous cross-domain transaction flag.",
            false,
        ),
    );
}

fn diff_domains(current: &DatabaseSpec, desired: &DatabaseSpec, steps: &mut Vec<MigrationStep>) {
    let current_domains = domains_by_name(current);
    let desired_domains = domains_by_name(desired);
    let names = union_keys(&current_domains, &desired_domains);
    for name in names {
        match (
            current_domains.get(name.as_str()),
            desired_domains.get(name.as_str()),
        ) {
            (None, Some(domain)) => push_step(
                steps,
                MigrationStepKind::ChangeDomain,
                format!("$.domains.{}", domain.name),
                format!(
                    "add domain {} with mode {}",
                    domain.name,
                    describe_value(&domain.mode)
                ),
                unsupported_impact("new domains require topology and collection placement review"),
                "Remove the new domain from the desired spec.",
                false,
            ),
            (Some(domain), None) => push_step(
                steps,
                MigrationStepKind::ChangeDomain,
                format!("$.domains.{}", domain.name),
                format!("remove domain {}", domain.name),
                unsupported_impact("removing domains can orphan collection placement"),
                "Re-add the removed domain to the desired spec.",
                false,
            ),
            (Some(current), Some(desired)) => record_diff(
                steps,
                format!("$.domains.{}.mode", desired.name),
                &current.mode,
                &desired.mode,
                diff_metadata(
                    MigrationStepKind::ChangeDomain,
                    unsupported_impact("domain mode changes alter consistency guarantees"),
                    "Restore the previous domain mode.",
                    false,
                ),
            ),
            (None, None) => {}
        }
    }
}

fn diff_collections(
    current: &DatabaseSpec,
    desired: &DatabaseSpec,
    steps: &mut Vec<MigrationStep>,
) {
    let current_collections = collections_by_name(current);
    let desired_collections = collections_by_name(desired);
    let names = union_keys(&current_collections, &desired_collections);
    for name in names {
        match (
            current_collections.get(name.as_str()),
            desired_collections.get(name.as_str()),
        ) {
            (None, Some(collection)) => push_step(
                steps,
                MigrationStepKind::ChangeCollection,
                format!("$.collections.{}", collection.name),
                format!(
                    "add collection {} as {:?}",
                    collection.name, collection.role
                ),
                unsupported_impact(
                    "collection creation is outside config apply v1 automatic changes",
                ),
                "Remove the new collection from the desired spec.",
                false,
            ),
            (Some(collection), None) => push_step(
                steps,
                MigrationStepKind::ChangeCollection,
                format!("$.collections.{}", collection.name),
                format!("remove collection {}", collection.name),
                unsupported_impact("collection removal can delete data"),
                "Re-add the removed collection to the desired spec.",
                false,
            ),
            (Some(current), Some(desired)) => {
                record_diff(
                    steps,
                    format!("$.collections.{}.role", desired.name),
                    &current.role,
                    &desired.role,
                    diff_metadata(
                        MigrationStepKind::ChangeCollection,
                        unsupported_impact(
                            "role changes can alter data guarantees and required capabilities",
                        ),
                        "Restore the previous collection role.",
                        false,
                    ),
                );
                record_diff(
                    steps,
                    format!("$.collections.{}.domain", desired.name),
                    &current.domain,
                    &desired.domain,
                    diff_metadata(
                        MigrationStepKind::ChangeDomain,
                        unsupported_impact(
                            "collection domain changes can move data across consistency boundaries",
                        ),
                        "Restore the previous collection domain.",
                        false,
                    ),
                );
                if index_set(&current.indexes) != index_set(&desired.indexes) {
                    push_step(
                        steps,
                        MigrationStepKind::ChangeIndex,
                        format!("$.collections.{}.indexes", desired.name),
                        format!(
                            "change indexes from {} to {}",
                            describe_value(&current.indexes),
                            describe_value(&desired.indexes)
                        ),
                        unsupported_impact(
                            "index changes require physical index build/drop planning",
                        ),
                        "Restore the previous index set.",
                        false,
                    );
                }
            }
            (None, None) => {}
        }
    }
}

fn diff_extensions(current: &DatabaseSpec, desired: &DatabaseSpec, steps: &mut Vec<MigrationStep>) {
    let current_extensions = extensions_by_name(current);
    let desired_extensions = extensions_by_name(desired);
    let names = union_keys(&current_extensions, &desired_extensions);
    for name in names {
        match (
            current_extensions.get(name.as_str()),
            desired_extensions.get(name.as_str()),
        ) {
            (None, Some(extension)) => push_step(
                steps,
                MigrationStepKind::ChangeExtension,
                format!("$.extensions.{}", extension.name),
                format!("add extension {} {}", extension.name, extension.version),
                unsupported_impact("extension installation is an operator-approved runtime change"),
                "Remove the new extension from the desired spec.",
                false,
            ),
            (Some(extension), None) => push_step(
                steps,
                MigrationStepKind::ChangeExtension,
                format!("$.extensions.{}", extension.name),
                format!("remove extension {}", extension.name),
                unsupported_impact("extension removal can break data or query capabilities"),
                "Re-add the removed extension to the desired spec.",
                false,
            ),
            (Some(current), Some(desired)) => {
                record_diff(
                    steps,
                    format!("$.extensions.{}.version", desired.name),
                    &current.version,
                    &desired.version,
                    diff_metadata(
                        MigrationStepKind::ChangeExtension,
                        unsupported_impact(
                            "extension version changes require compatibility review",
                        ),
                        "Restore the previous extension version.",
                        false,
                    ),
                );
                record_diff(
                    steps,
                    format!("$.extensions.{}.stability", desired.name),
                    &current.stability,
                    &desired.stability,
                    diff_metadata(
                        MigrationStepKind::ChangeExtension,
                        unsupported_impact("extension stability changes affect support status"),
                        "Restore the previous extension stability.",
                        false,
                    ),
                );
            }
            (None, None) => {}
        }
    }
}

fn diff_string_map(
    name: &str,
    current: &BTreeMap<String, String>,
    desired: &BTreeMap<String, String>,
    kind: MigrationStepKind,
    steps: &mut Vec<MigrationStep>,
) {
    let keys = union_keys(current, desired);
    for key in keys {
        match (current.get(&key), desired.get(&key)) {
            (None, Some(value)) => push_step(
                steps,
                kind,
                format!("$.{name}.{key}"),
                format!("add {name}.{key}={}", describe_value(value)),
                low_risk_impact("metadata-only configuration change"),
                "Remove the added metadata key.",
                true,
            ),
            (Some(value), None) => push_step(
                steps,
                kind,
                format!("$.{name}.{key}"),
                format!("remove {name}.{key}={}", describe_value(value)),
                low_risk_impact("metadata-only configuration change"),
                "Re-add the removed metadata key.",
                true,
            ),
            (Some(current), Some(desired)) if current != desired => push_step(
                steps,
                kind,
                format!("$.{name}.{key}"),
                format!(
                    "change {name}.{key} from {} to {}",
                    describe_value(current),
                    describe_value(desired)
                ),
                low_risk_impact("metadata-only configuration change"),
                "Restore the previous metadata value.",
                true,
            ),
            _ => {}
        }
    }
}

struct DiffMetadata<'a> {
    kind: MigrationStepKind,
    impact: PlanImpact,
    rollback: &'a str,
    supported: bool,
}

fn diff_metadata(
    kind: MigrationStepKind,
    impact: PlanImpact,
    rollback: &str,
    supported: bool,
) -> DiffMetadata<'_> {
    DiffMetadata {
        kind,
        impact,
        rollback,
        supported,
    }
}

fn record_diff<T>(
    steps: &mut Vec<MigrationStep>,
    path: impl Into<String>,
    current: &T,
    desired: &T,
    metadata: DiffMetadata<'_>,
) where
    T: Serialize + PartialEq,
{
    if current == desired {
        return;
    }
    let path = path.into();
    push_step(
        steps,
        metadata.kind,
        path.clone(),
        format!(
            "change {path} from {} to {}",
            describe_value(current),
            describe_value(desired)
        ),
        metadata.impact,
        metadata.rollback,
        metadata.supported,
    );
}

fn push_decision(
    decisions: &mut Vec<ExplainDecision>,
    path: impl Into<String>,
    value: String,
    source: &str,
    reason: &str,
    outcome: &str,
    impact: PlanImpact,
) {
    decisions.push(ExplainDecision {
        path: path.into(),
        value,
        source: source.to_owned(),
        reason: reason.to_owned(),
        outcome: outcome.to_owned(),
        impact,
    });
}

fn push_step(
    steps: &mut Vec<MigrationStep>,
    kind: MigrationStepKind,
    path: impl Into<String>,
    action: String,
    impact: PlanImpact,
    rollback: &str,
    supported: bool,
) {
    steps.push(MigrationStep {
        step_id: format!("step-{:03}", steps.len() + 1),
        kind,
        path: path.into(),
        action,
        requires_confirmation: !supported
            || impact.risk >= RiskLevel::Medium
            || impact.requires_backup
            || impact.requires_downtime,
        impact,
        rollback: rollback.to_owned(),
        supported,
    });
}

fn rollback_plan(steps: &[MigrationStep]) -> RollbackPlan {
    let possible = steps.iter().all(|step| step.supported);
    let steps = steps
        .iter()
        .map(|step| format!("{}: {}", step.step_id, step.rollback))
        .collect::<Vec<_>>();
    RollbackPlan {
        possible,
        description: if possible {
            "All planned changes are metadata-level and can be reverted by restoring the previous spec."
                .to_owned()
        } else {
            "At least one step is operator-managed in config apply v1; rollback must be planned outside automatic apply."
                .to_owned()
        },
        steps,
    }
}

fn aggregate_impact(steps: &[MigrationStep]) -> PlanImpact {
    let mut impact = impact_none();
    for step in steps {
        merge_impact(&mut impact, &step.impact);
    }
    impact
}

fn merge_impact(target: &mut PlanImpact, source: &PlanImpact) {
    target.downtime = target.downtime.max(source.downtime);
    target.disk = target.disk.max(source.disk);
    target.cpu = target.cpu.max(source.cpu);
    target.risk = target.risk.max(source.risk);
    target.requires_backup |= source.requires_backup;
    target.requires_downtime |= source.requires_downtime;
    for note in &source.notes {
        if !target.notes.contains(note) {
            target.notes.push(note.clone());
        }
    }
}

fn impact_none() -> PlanImpact {
    PlanImpact::default()
}

fn low_risk_impact(note: &str) -> PlanImpact {
    PlanImpact {
        downtime: ImpactLevel::None,
        disk: ImpactLevel::None,
        cpu: ImpactLevel::Low,
        risk: RiskLevel::Low,
        requires_backup: false,
        requires_downtime: false,
        notes: vec![note.to_owned()],
    }
}

fn medium_impact(note: &str) -> PlanImpact {
    PlanImpact {
        downtime: ImpactLevel::Low,
        disk: ImpactLevel::Low,
        cpu: ImpactLevel::Medium,
        risk: RiskLevel::Medium,
        requires_backup: true,
        requires_downtime: false,
        notes: vec![note.to_owned()],
    }
}

fn unsupported_impact(note: &str) -> PlanImpact {
    PlanImpact {
        downtime: ImpactLevel::High,
        disk: ImpactLevel::High,
        cpu: ImpactLevel::High,
        risk: RiskLevel::High,
        requires_backup: true,
        requires_downtime: true,
        notes: vec![
            note.to_owned(),
            "automatic apply unsupported in config apply v1".to_owned(),
        ],
    }
}

fn stable_plan_id(current: &DatabaseSpec, desired: &DatabaseSpec) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"multidb-config-migration-plan-v1");
    update_hash_with_json(&mut hasher, current);
    update_hash_with_json(&mut hasher, desired);
    let digest = hasher.finalize();
    format!("plan_{}", hex_prefix(digest.as_bytes(), 16))
}

fn update_hash_with_json<T: Serialize>(hasher: &mut blake3::Hasher, value: &T) {
    match serde_json::to_vec(value) {
        Ok(bytes) => hasher.update(&bytes),
        Err(error) => hasher.update(error.to_string().as_bytes()),
    };
}

fn hex_prefix(bytes: &[u8], len: usize) -> String {
    let mut output = String::with_capacity(len.saturating_mul(2));
    for byte in bytes.iter().take(len) {
        output.push(hex_digit(byte >> 4));
        output.push(hex_digit(byte & 0x0F));
    }
    output
}

fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => char::from(b'0' + value),
        _ => char::from(b'a' + (value - 10)),
    }
}

fn describe_value<T: Serialize>(value: &T) -> String {
    match serde_json::to_string(value) {
        Ok(value) => value,
        Err(error) => format!("<unserializable: {error}>"),
    }
}

fn domains_by_name(spec: &DatabaseSpec) -> BTreeMap<String, &ConsistencyDomainSpec> {
    spec.domains
        .iter()
        .map(|domain| (domain.name.clone(), domain))
        .collect()
}

fn collections_by_name(spec: &DatabaseSpec) -> BTreeMap<String, &CollectionRoleSpec> {
    spec.collections
        .iter()
        .map(|collection| (collection.name.clone(), collection))
        .collect()
}

fn extensions_by_name(spec: &DatabaseSpec) -> BTreeMap<String, &ExtensionManifestRef> {
    spec.extensions
        .iter()
        .map(|extension| (extension.name.clone(), extension))
        .collect()
}

fn union_keys<T, U>(left: &BTreeMap<String, T>, right: &BTreeMap<String, U>) -> BTreeSet<String> {
    left.keys().chain(right.keys()).cloned().collect()
}

fn index_set(indexes: &[CollectionIndexKind]) -> BTreeSet<CollectionIndexKind> {
    indexes.iter().copied().collect()
}

impl From<ReplicationKind> for ReplicationMode {
    fn from(value: ReplicationKind) -> Self {
        match value {
            ReplicationKind::Cp => Self::Cp,
            ReplicationKind::Ap => Self::Ap,
        }
    }
}

const GAME_LOCAL_BALANCED_ROLES: &[CollectionRole] = &[
    CollectionRole::DocumentEntity,
    CollectionRole::KeyValue,
    CollectionRole::EventLog,
    CollectionRole::Cache,
];
const DESKTOP_APP_EMBEDDED_ROLES: &[CollectionRole] = &[
    CollectionRole::DocumentEntity,
    CollectionRole::KeyValue,
    CollectionRole::Cache,
    CollectionRole::Audit,
    CollectionRole::TimeSeries,
];
const AI_AGENT_MEMORY_ROLES: &[CollectionRole] = &[
    CollectionRole::VectorMemory,
    CollectionRole::DocumentEntity,
    CollectionRole::KeyValue,
    CollectionRole::EventLog,
    CollectionRole::Cache,
];
const SECURE_APP_ROLES: &[CollectionRole] = &[
    CollectionRole::DocumentEntity,
    CollectionRole::KeyValue,
    CollectionRole::EventLog,
    CollectionRole::Audit,
];
const ANALYTICS_COLUMNAR_ROLES: &[CollectionRole] = &[
    CollectionRole::Analytics,
    CollectionRole::TimeSeries,
    CollectionRole::EventLog,
];

const BUILT_IN_PROFILES: &[ProfileSpec] = &[
    ProfileSpec {
        slug: "game_local_balanced",
        aliases: &["balanced", "in_memory"],
        status: SupportStatus::Certified,
        description: "Local-first balanced profile for games and lightweight embedded state.",
        default_domain: ConsistencyMode::LocalSnapshot,
        compatible_roles: GAME_LOCAL_BALANCED_ROLES,
    },
    ProfileSpec {
        slug: "desktop_app_embedded",
        aliases: &["document"],
        status: SupportStatus::Certified,
        description: "Embedded desktop application profile for durable local app data.",
        default_domain: ConsistencyMode::LocalSnapshot,
        compatible_roles: DESKTOP_APP_EMBEDDED_ROLES,
    },
    ProfileSpec {
        slug: "ai_agent_memory",
        aliases: &["vector"],
        status: SupportStatus::Stable,
        description: "Agent memory profile for vectors, documents, event context and cache data.",
        default_domain: ConsistencyMode::LocalSnapshot,
        compatible_roles: AI_AGENT_MEMORY_ROLES,
    },
    ProfileSpec {
        slug: "secure_app",
        aliases: &["transactional", "high_durability"],
        status: SupportStatus::Stable,
        description: "Security-focused transactional profile for audited application state.",
        default_domain: ConsistencyMode::StrongCp,
        compatible_roles: SECURE_APP_ROLES,
    },
    ProfileSpec {
        slug: "production_cp",
        aliases: &[],
        status: SupportStatus::Stable,
        description: "CP OpenRaft cluster profile covered by the local/process smoke gate.",
        default_domain: ConsistencyMode::StrongCp,
        compatible_roles: SECURE_APP_ROLES,
    },
    ProfileSpec {
        slug: "analytics_columnar",
        aliases: &["analytical", "time_series"],
        status: SupportStatus::Stable,
        description: "Columnar analytics profile for event, time-series and aggregate workloads.",
        default_domain: ConsistencyMode::LocalSnapshot,
        compatible_roles: ANALYTICS_COLUMNAR_ROLES,
    },
];

const COLLECTION_ROLE_DEFINITIONS: &[CollectionRoleDefinition] = &[
    CollectionRoleDefinition {
        role: CollectionRole::DocumentEntity,
        slug: "document_entity",
        status: SupportStatus::Certified,
        description: "Mutable JSON-like entities with document indexes and schema-aware access.",
        required_capabilities: &["document-indexes", "json-path"],
        constraints: &["requires stable object identity"],
    },
    CollectionRoleDefinition {
        role: CollectionRole::KeyValue,
        slug: "key_value",
        status: SupportStatus::Certified,
        description: "Opaque key-value data for direct lookup and small metadata records.",
        required_capabilities: &["ordered-key-storage"],
        constraints: &["keys must be application-stable"],
    },
    CollectionRoleDefinition {
        role: CollectionRole::EventLog,
        slug: "event_log",
        status: SupportStatus::Stable,
        description: "Append-oriented event data for CDC, audit trails and replayable facts.",
        required_capabilities: &["commit-log", "cdc"],
        constraints: &["updates should be exceptional"],
    },
    CollectionRoleDefinition {
        role: CollectionRole::VectorMemory,
        slug: "vector_memory",
        status: SupportStatus::Stable,
        description: "Embedding memory with vector indexes and document payloads.",
        required_capabilities: &["hnsw", "vector-search"],
        constraints: &["requires a vector index for similarity queries"],
    },
    CollectionRoleDefinition {
        role: CollectionRole::Cache,
        slug: "cache",
        status: SupportStatus::Certified,
        description: "Regenerable data with bounded durability expectations.",
        required_capabilities: &["ttl-or-eviction-policy"],
        constraints: &["must tolerate loss or rebuild"],
    },
    CollectionRoleDefinition {
        role: CollectionRole::Audit,
        slug: "audit",
        status: SupportStatus::Stable,
        description: "Tamper-evident audit records and security-sensitive trails.",
        required_capabilities: &["audit-enabled", "hash-chain"],
        constraints: &["audit disabling is not compatible with this role"],
    },
    CollectionRoleDefinition {
        role: CollectionRole::Graph,
        slug: "graph",
        status: SupportStatus::Experimental,
        description: "Graph nodes and edges with traversal-oriented access patterns.",
        required_capabilities: &["graph-index"],
        constraints: &["full GQL/Cypher compatibility is outside v1"],
    },
    CollectionRoleDefinition {
        role: CollectionRole::Analytics,
        slug: "analytics",
        status: SupportStatus::Stable,
        description: "Columnar data for scans, aggregates and reporting workloads.",
        required_capabilities: &["columnar-layout", "parquet"],
        constraints: &["optimized for read-heavy workloads"],
    },
    CollectionRoleDefinition {
        role: CollectionRole::TimeSeries,
        slug: "time_series",
        status: SupportStatus::Stable,
        description: "Timestamped measurements with chunks, retention and downsampling.",
        required_capabilities: &["time-series", "chunked-storage"],
        constraints: &["requires a stable timestamp column or field"],
    },
];

const CONSISTENCY_DOMAIN_DEFINITIONS: &[ConsistencyDomainDefinition] = &[
    ConsistencyDomainDefinition {
        mode: ConsistencyMode::LocalSnapshot,
        slug: "local_snapshot",
        status: SupportStatus::Certified,
        guarantees: &[
            "single-process snapshot reads",
            "local write visibility after commit",
        ],
        limits: &["does not claim cross-node quorum semantics"],
    },
    ConsistencyDomainDefinition {
        mode: ConsistencyMode::StrongCp,
        slug: "strong_cp",
        status: SupportStatus::Stable,
        guarantees: &[
            "CP-side intent",
            "quorum-oriented consistency boundary",
            "local/process cluster smoke coverage",
        ],
        limits: &["Kubernetes automation, multi-region placement and SLA are separate concerns"],
    },
    ConsistencyDomainDefinition {
        mode: ConsistencyMode::EventualAp,
        slug: "eventual_ap",
        status: SupportStatus::Experimental,
        guarantees: &["available-write intent", "eventual convergence boundary"],
        limits: &["conflict policy must be validated by workload-specific review"],
    },
];

const EXTENSION_CAPABILITY_DEFINITIONS: &[ExtensionCapabilityDefinition] = &[
    ExtensionCapabilityDefinition {
        slug: "audit",
        status: SupportStatus::Stable,
        source: "collection_role.audit",
        description: "Tamper-evident audit collection capability.",
    },
    ExtensionCapabilityDefinition {
        slug: "cdc",
        status: SupportStatus::Stable,
        source: "collection_role.event_log",
        description: "Change data capture and replayable event stream capability.",
    },
    ExtensionCapabilityDefinition {
        slug: "columnar_layout",
        status: SupportStatus::Experimental,
        source: "collection_role.analytics",
        description: "Columnar analytics layout and scan capability.",
    },
    ExtensionCapabilityDefinition {
        slug: "document_index",
        status: SupportStatus::Stable,
        source: "collection_index.document",
        description: "Document path indexing capability.",
    },
    ExtensionCapabilityDefinition {
        slug: "full_text",
        status: SupportStatus::Stable,
        source: "collection_index.full_text",
        description: "Full-text indexing and search capability.",
    },
    ExtensionCapabilityDefinition {
        slug: "graph_index",
        status: SupportStatus::Experimental,
        source: "collection_role.graph",
        description: "Graph traversal and graph index capability.",
    },
    ExtensionCapabilityDefinition {
        slug: "time_series",
        status: SupportStatus::Stable,
        source: "collection_role.time_series",
        description: "Time-series storage and rollup capability.",
    },
    ExtensionCapabilityDefinition {
        slug: "vector_hnsw",
        status: SupportStatus::Stable,
        source: "collection_role.vector_memory",
        description: "Vector memory and HNSW similarity search capability.",
    },
];

static BUILT_IN_EXTENSION_MANIFESTS: LazyLock<Vec<ExtensionManifest>> = LazyLock::new(|| {
    vec![
        extension_manifest(
            "audit",
            SupportStatus::Stable,
            ExtensionProvides {
                types: vec!["audit_record".to_owned()],
                indexes: vec!["audit_hash_chain".to_owned()],
                operators: vec!["verify_audit_hash_chain".to_owned()],
                storage_strategies: Vec::new(),
            },
            ExtensionRegistries {
                types: vec![registry_entry(
                    "audit_record",
                    SupportStatus::Stable,
                    &["audit-enabled"],
                    "Tamper-evident audit record type.",
                )],
                indexes: vec![registry_entry(
                    "audit_hash_chain",
                    SupportStatus::Stable,
                    &["hash-chain"],
                    "Hash-chain index owned by the core audit pipeline.",
                )],
                operators: vec![registry_entry(
                    "verify_audit_hash_chain",
                    SupportStatus::Stable,
                    &["hash-chain"],
                    "Audit integrity verification operator.",
                )],
                storage_strategies: Vec::new(),
            },
            extension_manifest_details(
                &["audit-enabled", "hash-chain"],
                &["Audit records must be emitted through the core audit subsystem."],
                vec![extension_migration("audit-1", "1.0.0", "1.0.0", "metadata")],
                vec![extension_panel(
                    "audit-overview",
                    "Audit",
                    "/extensions/audit",
                    &["audit-enabled"],
                )],
            ),
        ),
        extension_manifest(
            "cdc",
            SupportStatus::Stable,
            ExtensionProvides {
                types: vec!["change_event".to_owned()],
                indexes: Vec::new(),
                operators: vec!["stream_changes".to_owned()],
                storage_strategies: Vec::new(),
            },
            ExtensionRegistries {
                types: vec![registry_entry(
                    "change_event",
                    SupportStatus::Stable,
                    &["commit-log"],
                    "Replayable change event type.",
                )],
                indexes: Vec::new(),
                operators: vec![registry_entry(
                    "stream_changes",
                    SupportStatus::Stable,
                    &["cdc"],
                    "CDC stream read operator.",
                )],
                storage_strategies: Vec::new(),
            },
            extension_manifest_details(
                &["commit-log", "cdc"],
                &[
                    "CDC reads from the core commit log and cannot write replication metadata directly.",
                ],
                vec![extension_migration("cdc-1", "1.0.0", "1.0.0", "metadata")],
                vec![extension_panel(
                    "cdc-streams",
                    "CDC",
                    "/extensions/cdc",
                    &["cdc"],
                )],
            ),
        ),
        extension_manifest(
            "columnar_layout",
            SupportStatus::Experimental,
            ExtensionProvides {
                types: vec!["columnar_segment".to_owned()],
                indexes: Vec::new(),
                operators: vec!["scan_columnar".to_owned()],
                storage_strategies: vec!["columnar_parquet".to_owned()],
            },
            ExtensionRegistries {
                types: vec![registry_entry(
                    "columnar_segment",
                    SupportStatus::Experimental,
                    &["columnar-layout"],
                    "Columnar segment metadata type.",
                )],
                indexes: Vec::new(),
                operators: vec![registry_entry(
                    "scan_columnar",
                    SupportStatus::Experimental,
                    &["columnar-layout"],
                    "Columnar scan operator.",
                )],
                storage_strategies: vec![registry_entry(
                    "columnar_parquet",
                    SupportStatus::Experimental,
                    &["parquet"],
                    "Parquet-backed columnar storage strategy.",
                )],
            },
            extension_manifest_details(
                &["columnar-layout", "parquet"],
                &[
                    "Columnar layout is read-optimized and remains experimental before Performance Truth.",
                ],
                vec![extension_migration(
                    "columnar-1",
                    "1.0.0",
                    "1.0.0",
                    "metadata",
                )],
                vec![extension_panel(
                    "columnar-segments",
                    "Columnar",
                    "/extensions/columnar_layout",
                    &["columnar-layout"],
                )],
            ),
        ),
        extension_manifest(
            "document_index",
            SupportStatus::Stable,
            ExtensionProvides {
                types: Vec::new(),
                indexes: vec!["document_btree".to_owned()],
                operators: vec!["json_path_lookup".to_owned()],
                storage_strategies: Vec::new(),
            },
            ExtensionRegistries {
                types: Vec::new(),
                indexes: vec![registry_entry(
                    "document_btree",
                    SupportStatus::Stable,
                    &["document-indexes"],
                    "Document path index registered through core index APIs.",
                )],
                operators: vec![registry_entry(
                    "json_path_lookup",
                    SupportStatus::Stable,
                    &["json-path"],
                    "JSON path lookup operator.",
                )],
                storage_strategies: Vec::new(),
            },
            extension_manifest_details(
                &["document-indexes", "json-path"],
                &["Document indexes must be declared through collection index metadata."],
                vec![extension_migration(
                    "document-index-1",
                    "1.0.0",
                    "1.0.0",
                    "metadata",
                )],
                vec![extension_panel(
                    "document-indexes",
                    "Document indexes",
                    "/extensions/document_index",
                    &["document-indexes"],
                )],
            ),
        ),
        extension_manifest(
            "full_text",
            SupportStatus::Stable,
            ExtensionProvides {
                types: vec!["text_document".to_owned()],
                indexes: vec!["full_text".to_owned()],
                operators: vec!["match_text".to_owned()],
                storage_strategies: Vec::new(),
            },
            ExtensionRegistries {
                types: vec![registry_entry(
                    "text_document",
                    SupportStatus::Stable,
                    &["text-search"],
                    "Full-text document metadata type.",
                )],
                indexes: vec![registry_entry(
                    "full_text",
                    SupportStatus::Stable,
                    &["full-text"],
                    "Full-text inverted index.",
                )],
                operators: vec![registry_entry(
                    "match_text",
                    SupportStatus::Stable,
                    &["text-search"],
                    "Full-text search operator.",
                )],
                storage_strategies: Vec::new(),
            },
            extension_manifest_details(
                &["full-text", "text-search"],
                &[
                    "Derived text indexes can lag source writes and are rebuilt through core recovery.",
                ],
                vec![extension_migration(
                    "full-text-1",
                    "1.0.0",
                    "1.0.0",
                    "metadata",
                )],
                vec![extension_panel(
                    "full-text-indexes",
                    "Full text",
                    "/extensions/full_text",
                    &["full-text"],
                )],
            ),
        ),
        extension_manifest(
            "graph_index",
            SupportStatus::Experimental,
            ExtensionProvides {
                types: vec!["graph_edge".to_owned()],
                indexes: vec!["graph_index".to_owned()],
                operators: vec!["traverse_graph".to_owned()],
                storage_strategies: Vec::new(),
            },
            ExtensionRegistries {
                types: vec![registry_entry(
                    "graph_edge",
                    SupportStatus::Experimental,
                    &["graph-index"],
                    "Graph edge metadata type.",
                )],
                indexes: vec![registry_entry(
                    "graph_index",
                    SupportStatus::Experimental,
                    &["graph-index"],
                    "Graph traversal index.",
                )],
                operators: vec![registry_entry(
                    "traverse_graph",
                    SupportStatus::Experimental,
                    &["graph-index"],
                    "Graph traversal operator.",
                )],
                storage_strategies: Vec::new(),
            },
            extension_manifest_details(
                &["graph-index"],
                &["Full GQL/Cypher compatibility is outside the current extension contract."],
                vec![extension_migration(
                    "graph-index-1",
                    "1.0.0",
                    "1.0.0",
                    "metadata",
                )],
                vec![extension_panel(
                    "graph-indexes",
                    "Graph",
                    "/extensions/graph_index",
                    &["graph-index"],
                )],
            ),
        ),
        extension_manifest(
            "time_series",
            SupportStatus::Stable,
            ExtensionProvides {
                types: vec!["time_series_chunk".to_owned()],
                indexes: vec!["time_series".to_owned()],
                operators: vec!["rollup_time_series".to_owned()],
                storage_strategies: vec!["chunked_time_series".to_owned()],
            },
            ExtensionRegistries {
                types: vec![registry_entry(
                    "time_series_chunk",
                    SupportStatus::Stable,
                    &["time-series"],
                    "Time-series chunk metadata type.",
                )],
                indexes: vec![registry_entry(
                    "time_series",
                    SupportStatus::Stable,
                    &["time-series"],
                    "Time-series index.",
                )],
                operators: vec![registry_entry(
                    "rollup_time_series",
                    SupportStatus::Stable,
                    &["chunked-storage"],
                    "Time-series rollup operator.",
                )],
                storage_strategies: vec![registry_entry(
                    "chunked_time_series",
                    SupportStatus::Stable,
                    &["chunked-storage"],
                    "Chunked time-series storage strategy.",
                )],
            },
            extension_manifest_details(
                &["time-series", "chunked-storage"],
                &["A stable timestamp field is required before enabling time-series rollups."],
                vec![extension_migration(
                    "time-series-1",
                    "1.0.0",
                    "1.0.0",
                    "metadata",
                )],
                vec![extension_panel(
                    "time-series-chunks",
                    "Time series",
                    "/extensions/time_series",
                    &["time-series"],
                )],
            ),
        ),
        extension_manifest(
            "vector_hnsw",
            SupportStatus::Stable,
            ExtensionProvides {
                types: vec!["vector".to_owned()],
                indexes: vec!["hnsw".to_owned()],
                operators: vec!["knn".to_owned()],
                storage_strategies: vec!["vector_memory".to_owned()],
            },
            ExtensionRegistries {
                types: vec![registry_entry(
                    "vector",
                    SupportStatus::Stable,
                    &["vector-search"],
                    "Dense vector type.",
                )],
                indexes: vec![registry_entry(
                    "hnsw",
                    SupportStatus::Stable,
                    &["hnsw"],
                    "HNSW approximate nearest-neighbor index.",
                )],
                operators: vec![registry_entry(
                    "knn",
                    SupportStatus::Stable,
                    &["vector-search"],
                    "Vector nearest-neighbor operator.",
                )],
                storage_strategies: vec![registry_entry(
                    "vector_memory",
                    SupportStatus::Stable,
                    &["vector-search"],
                    "Vector memory storage strategy.",
                )],
            },
            extension_manifest_details(
                &["hnsw", "vector-search"],
                &["Asynchronous vector indexing can lag source writes."],
                vec![extension_migration(
                    "vector-hnsw-1",
                    "1.0.0",
                    "1.0.0",
                    "metadata",
                )],
                vec![extension_panel(
                    "vector-indexes",
                    "Vectors",
                    "/extensions/vector_hnsw",
                    &["vector-search"],
                )],
            ),
        ),
    ]
});

#[must_use]
pub const fn built_in_profiles() -> &'static [ProfileSpec] {
    BUILT_IN_PROFILES
}

#[must_use]
pub fn built_in_profile(slug_or_alias: &str) -> Option<&'static ProfileSpec> {
    let key = slug_or_alias.trim();
    built_in_profiles()
        .iter()
        .find(|profile| profile.slug == key || profile.aliases.contains(&key))
}

#[must_use]
pub const fn collection_role_definitions() -> &'static [CollectionRoleDefinition] {
    COLLECTION_ROLE_DEFINITIONS
}

#[must_use]
pub fn collection_role_definition(
    role: CollectionRole,
) -> Option<&'static CollectionRoleDefinition> {
    collection_role_definitions()
        .iter()
        .find(|definition| definition.role == role)
}

#[must_use]
pub const fn consistency_domain_definitions() -> &'static [ConsistencyDomainDefinition] {
    CONSISTENCY_DOMAIN_DEFINITIONS
}

#[must_use]
pub const fn extension_capability_definitions() -> &'static [ExtensionCapabilityDefinition] {
    EXTENSION_CAPABILITY_DEFINITIONS
}

#[must_use]
pub fn built_in_extension_manifests() -> &'static [ExtensionManifest] {
    BUILT_IN_EXTENSION_MANIFESTS.as_slice()
}

#[must_use]
pub fn built_in_extension_manifest(name: &str) -> Option<&'static ExtensionManifest> {
    built_in_extension_manifests()
        .iter()
        .find(|manifest| manifest.name == name)
}

#[must_use]
pub fn extension_catalog_entries() -> Vec<ExtensionCatalogEntry> {
    extension_capability_definitions()
        .iter()
        .map(|definition| {
            let manifest = built_in_extension_manifest(definition.slug)
                .cloned()
                .unwrap_or_else(|| extension_manifest_placeholder(definition));
            ExtensionCatalogEntry {
                slug: definition.slug.to_owned(),
                status: definition.status,
                source: definition.source.to_owned(),
                description: definition.description.to_owned(),
                manifest,
            }
        })
        .collect()
}

#[must_use]
pub fn compile_extension_catalog(manifests: &[ExtensionManifest]) -> CompiledExtensionCatalog {
    let mut manifest_names = BTreeSet::new();
    let mut capabilities = BTreeSet::new();
    let mut types = BTreeSet::new();
    let mut indexes = BTreeSet::new();
    let mut operators = BTreeSet::new();
    let mut storage_strategies = BTreeSet::new();
    let mut ui_panels = BTreeSet::new();

    for manifest in manifests {
        manifest_names.insert(manifest.name.clone());
        capabilities.extend(manifest.capabilities.iter().cloned());
        types.extend(
            manifest
                .registries
                .types
                .iter()
                .map(|entry| entry.id.clone()),
        );
        indexes.extend(
            manifest
                .registries
                .indexes
                .iter()
                .map(|entry| entry.id.clone()),
        );
        operators.extend(
            manifest
                .registries
                .operators
                .iter()
                .map(|entry| entry.id.clone()),
        );
        storage_strategies.extend(
            manifest
                .registries
                .storage_strategies
                .iter()
                .map(|entry| entry.id.clone()),
        );
        ui_panels.extend(manifest.ui_panels.iter().map(|panel| panel.id.clone()));
    }

    CompiledExtensionCatalog {
        manifests: manifest_names.into_iter().collect(),
        capabilities: capabilities.into_iter().collect(),
        types: types.into_iter().collect(),
        indexes: indexes.into_iter().collect(),
        operators: operators.into_iter().collect(),
        storage_strategies: storage_strategies.into_iter().collect(),
        ui_panels: ui_panels.into_iter().collect(),
    }
}

#[must_use]
pub fn consistency_domain_definition(
    mode: ConsistencyMode,
) -> Option<&'static ConsistencyDomainDefinition> {
    consistency_domain_definitions()
        .iter()
        .find(|definition| definition.mode == mode)
}

struct ExtensionManifestDetails<'a> {
    capabilities: &'a [&'a str],
    limitations: &'a [&'a str],
    migrations: Vec<ExtensionMigration>,
    ui_panels: Vec<ExtensionUiPanel>,
}

fn extension_manifest(
    name: &str,
    status: SupportStatus,
    provides: ExtensionProvides,
    registries: ExtensionRegistries,
    details: ExtensionManifestDetails<'_>,
) -> ExtensionManifest {
    ExtensionManifest {
        name: name.to_owned(),
        version: "1.0.0".to_owned(),
        compatible_multidb: ">=0.1.0".to_owned(),
        status,
        provides,
        registries,
        capabilities: details
            .capabilities
            .iter()
            .map(|capability| (*capability).to_owned())
            .collect(),
        config_schema: json!({
            "type": "object",
            "additionalProperties": false
        }),
        limitations: details
            .limitations
            .iter()
            .map(|limit| (*limit).to_owned())
            .collect(),
        migrations: details.migrations,
        ui_panels: details.ui_panels,
        core_boundary: core_owned_extension_boundary(),
    }
}

fn extension_manifest_details<'a>(
    capabilities: &'a [&'a str],
    limitations: &'a [&'a str],
    migrations: Vec<ExtensionMigration>,
    ui_panels: Vec<ExtensionUiPanel>,
) -> ExtensionManifestDetails<'a> {
    ExtensionManifestDetails {
        capabilities,
        limitations,
        migrations,
        ui_panels,
    }
}

fn extension_manifest_placeholder(definition: &ExtensionCapabilityDefinition) -> ExtensionManifest {
    extension_manifest(
        definition.slug,
        definition.status,
        ExtensionProvides::default(),
        ExtensionRegistries::default(),
        extension_manifest_details(
            &[],
            &["Manifest details are unavailable for this capability."],
            Vec::new(),
            Vec::new(),
        ),
    )
}

fn registry_entry(
    id: &str,
    status: SupportStatus,
    required_capabilities: &[&str],
    description: &str,
) -> ExtensionRegistryEntry {
    ExtensionRegistryEntry {
        id: id.to_owned(),
        status,
        required_capabilities: required_capabilities
            .iter()
            .map(|capability| (*capability).to_owned())
            .collect(),
        description: description.to_owned(),
    }
}

fn extension_migration(id: &str, from: &str, to: &str, kind: &str) -> ExtensionMigration {
    ExtensionMigration {
        id: id.to_owned(),
        from: from.to_owned(),
        to: to.to_owned(),
        kind: kind.to_owned(),
        requires_downtime: false,
        notes: Vec::new(),
    }
}

fn extension_panel(
    id: &str,
    title: &str,
    route: &str,
    required_capabilities: &[&str],
) -> ExtensionUiPanel {
    ExtensionUiPanel {
        id: id.to_owned(),
        title: title.to_owned(),
        route: route.to_owned(),
        required_capabilities: required_capabilities
            .iter()
            .map(|capability| (*capability).to_owned())
            .collect(),
    }
}

fn core_owned_extension_boundary() -> ExtensionCoreBoundary {
    ExtensionCoreBoundary {
        wal: ExtensionCoreOwner::CoreOwned,
        transactions: ExtensionCoreOwner::CoreOwned,
        recovery: ExtensionCoreOwner::CoreOwned,
        security: ExtensionCoreOwner::CoreOwned,
        rbac: ExtensionCoreOwner::CoreOwned,
    }
}

fn profile_slug(profile: Profile) -> &'static str {
    match profile {
        Profile::InMemory => "in_memory",
        Profile::Transactional => "transactional",
        Profile::Analytical => "analytical",
        Profile::Document => "document",
        Profile::Vector => "vector",
        Profile::TimeSeries => "time_series",
        Profile::HighDurability => "high_durability",
        Profile::Balanced => "balanced",
    }
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), ConfigSpecError> {
    if value.trim().is_empty() {
        return Err(ConfigSpecError::EmptyField { field });
    }
    Ok(())
}

fn unique_names<'a>(
    names: impl Iterator<Item = &'a str>,
    kind: &'static str,
) -> Result<BTreeSet<&'a str>, ConfigSpecError> {
    let mut seen = BTreeSet::new();
    for name in names {
        require_non_empty(kind, name)?;
        if !seen.insert(name) {
            return Err(ConfigSpecError::Duplicate {
                kind,
                name: name.to_owned(),
            });
        }
    }
    Ok(seen)
}

fn validate_map_keys(
    field: &'static str,
    values: &BTreeMap<String, String>,
) -> Result<(), ConfigSpecError> {
    if values.keys().any(|key| key.trim().is_empty()) {
        return Err(ConfigSpecError::EmptyField { field });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::json;

    use super::{
        ApplyStatus, CollectionIndexKind, CollectionRole, CollectionRoleSpec, ConfigExplainer,
        ConfigSpecError, ConsistencyDomainSpec, ConsistencyMode, DATABASE_SPEC_VERSION,
        DatabaseSpec, DeploymentMode, ExtensionCoreOwner, ExtensionManifestRef,
        ExtensionManifestValidator, ExtensionStability, GuaranteeValidator, MigrationPlanner,
        MigrationStepKind, PolicyCompiler, ReplicationMode, SupportStatus, TopologySpec,
        ValidationSeverity, WriteAck, built_in_extension_manifest, built_in_extension_manifests,
        built_in_profile, built_in_profiles, collection_role_definition,
        collection_role_definitions, compile_extension_catalog, consistency_domain_definition,
        consistency_domain_definitions, database_spec_v1_schema, extension_capability_definitions,
    };
    use crate::db::{DbConfig, Profile, ReplicationKind};

    fn catalog_spec(profile: &str, mode: ConsistencyMode) -> DatabaseSpec {
        let mut spec = DatabaseSpec::from_db_config(
            "catalog",
            &DbConfig::on_disk(Profile::Balanced, "catalog.redb"),
        );
        spec.profile = profile.to_owned();
        spec.domains[0].mode = mode;
        spec
    }

    fn collection(
        name: &str,
        role: CollectionRole,
        indexes: Vec<CollectionIndexKind>,
    ) -> CollectionRoleSpec {
        CollectionRoleSpec {
            name: name.to_owned(),
            role,
            domain: "primary".to_owned(),
            indexes,
        }
    }

    fn issue_codes(report: &super::ValidationReport) -> BTreeSet<String> {
        report
            .issues
            .iter()
            .map(|issue| issue.code.clone())
            .collect()
    }

    #[test]
    fn database_spec_round_trips_json() -> Result<(), Box<dyn std::error::Error>> {
        let mut spec = DatabaseSpec::from_db_config(
            "accounts",
            &DbConfig::on_disk(Profile::Transactional, "accounts.redb"),
        );
        spec.collections.push(CollectionRoleSpec {
            name: "users".to_owned(),
            role: CollectionRole::DocumentEntity,
            domain: "primary".to_owned(),
            indexes: vec![CollectionIndexKind::Document],
        });
        spec.overrides
            .insert("storage.cache.max_capacity".to_owned(), "4096".to_owned());
        spec.operation_hints
            .insert("backup.rpo".to_owned(), "15m".to_owned());

        let json = serde_json::to_string_pretty(&spec)?;
        let decoded = serde_json::from_str::<DatabaseSpec>(&json)?;

        assert_eq!(decoded, spec);
        decoded.validate_structure()?;
        Ok(())
    }

    #[test]
    fn database_spec_without_topology_deserializes_with_single_node_default()
    -> Result<(), Box<dyn std::error::Error>> {
        let value = json!({
            "version": DATABASE_SPEC_VERSION,
            "name": "legacy",
            "profile": "balanced",
            "deployment": { "mode": "single_node", "storage_path": "legacy.redb" },
            "defaults": { "consistency_domain": "primary", "replication": "cp" },
            "guarantees": {
                "write_ack": "quorum",
                "conflict_resolution": "none",
                "backup": { "enabled": false, "pitr": false },
                "encryption": { "at_rest": false },
                "audit": { "enabled": true },
                "sensitive_data": false,
                "strict_cross_domain_transactions": false
            },
            "domains": [{ "name": "primary", "mode": "local_snapshot" }],
            "collections": [],
            "extensions": [],
            "overrides": {},
            "operation_hints": {}
        });

        let decoded = serde_json::from_value::<DatabaseSpec>(value)?;

        assert_eq!(decoded.topology, TopologySpec::default());
        decoded.validate_structure()?;
        Ok(())
    }

    #[test]
    fn topology_rejects_bad_types_and_unknown_fields() -> Result<(), Box<dyn std::error::Error>> {
        let spec = DatabaseSpec::from_db_config(
            "accounts",
            &DbConfig::on_disk(Profile::Balanced, "accounts.redb"),
        );

        let mut bad_type = serde_json::to_value(&spec)?;
        bad_type["topology"]["replica_count"] = json!("three");
        assert!(serde_json::from_value::<DatabaseSpec>(bad_type).is_err());

        let mut unknown_field = serde_json::to_value(&spec)?;
        unknown_field["topology"]["placement"] = json!("zone-a");
        assert!(serde_json::from_value::<DatabaseSpec>(unknown_field).is_err());

        Ok(())
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let value = json!({
            "version": DATABASE_SPEC_VERSION,
            "name": "bad",
            "profile": "balanced",
            "deployment": { "mode": "single_node", "storage_path": "db.redb" },
            "defaults": { "consistency_domain": "primary", "replication": "cp" },
            "guarantees": {
                "write_ack": "quorum",
                "conflict_resolution": "none",
                "backup": { "enabled": false, "pitr": false },
                "encryption": { "at_rest": false },
                "audit": { "enabled": true },
                "sensitive_data": false,
                "strict_cross_domain_transactions": false
            },
            "domains": [{ "name": "primary", "mode": "strong_cp" }],
            "collections": [],
            "extensions": [],
            "overrides": {},
            "operation_hints": {},
            "unexpected": true
        });

        assert!(serde_json::from_value::<DatabaseSpec>(value).is_err());
    }

    #[test]
    fn unknown_version_is_rejected() {
        let mut spec = DatabaseSpec::from_db_config(
            "accounts",
            &DbConfig::on_disk(Profile::Balanced, "accounts.redb"),
        );
        spec.version = 2;

        assert_eq!(
            spec.validate_structure(),
            Err(ConfigSpecError::UnsupportedVersion { found: 2 })
        );
    }

    #[test]
    fn duplicate_domain_is_rejected() {
        let mut spec = DatabaseSpec::from_db_config(
            "accounts",
            &DbConfig::on_disk(Profile::Balanced, "accounts.redb"),
        );
        spec.domains.push(ConsistencyDomainSpec {
            name: "primary".to_owned(),
            mode: ConsistencyMode::LocalSnapshot,
        });

        assert_eq!(
            spec.validate_structure(),
            Err(ConfigSpecError::Duplicate {
                kind: "domain",
                name: "primary".to_owned(),
            })
        );
    }

    #[test]
    fn collection_domain_must_exist() {
        let mut spec = DatabaseSpec::from_db_config(
            "accounts",
            &DbConfig::on_disk(Profile::Balanced, "accounts.redb"),
        );
        spec.collections.push(CollectionRoleSpec {
            name: "events".to_owned(),
            role: CollectionRole::EventLog,
            domain: "missing".to_owned(),
            indexes: Vec::new(),
        });

        assert_eq!(
            spec.validate_structure(),
            Err(ConfigSpecError::UnknownDomain {
                collection: "events".to_owned(),
                domain: "missing".to_owned(),
            })
        );
    }

    #[test]
    fn schema_snapshot_matches_checked_in_file() -> Result<(), Box<dyn std::error::Error>> {
        let expected = serde_json::from_str::<serde_json::Value>(include_str!(
            "../docs/schemas/database-spec-v1.schema.json"
        ))?;

        assert_eq!(database_spec_v1_schema(), expected);
        Ok(())
    }

    #[test]
    fn schema_exposes_optional_topology_with_integer_bounds()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = database_spec_v1_schema();
        let required = schema["required"]
            .as_array()
            .ok_or("required array missing")?;
        assert!(
            !required
                .iter()
                .any(|entry| entry.as_str() == Some("topology"))
        );
        assert_eq!(
            schema["properties"]["topology"]["$ref"].as_str(),
            Some("#/$defs/topology")
        );
        assert_eq!(
            schema["$defs"]["topology"]["properties"]["replica_count"]["minimum"],
            json!(1)
        );
        assert_eq!(
            schema["$defs"]["topology"]["additionalProperties"],
            json!(false)
        );
        Ok(())
    }

    #[test]
    fn db_config_import_covers_every_profile() {
        for profile in Profile::all() {
            let config = if profile == Profile::InMemory {
                DbConfig::new(profile)
            } else {
                DbConfig::on_disk(profile, format!("{profile:?}.redb"))
            };
            let spec = DatabaseSpec::from_db_config(format!("{profile:?}"), &config);

            assert_eq!(spec.version, DATABASE_SPEC_VERSION);
            assert_eq!(spec.domains.len(), 1);
            assert_eq!(spec.defaults.consistency_domain, "primary");
            assert_eq!(spec.deployment.storage_path.is_some(), profile.is_on_disk());
            assert!(spec.validate_structure().is_ok());
        }
    }

    #[test]
    fn db_config_import_preserves_deployment_and_replication() {
        let config =
            DbConfig::on_disk(Profile::Vector, "vector.redb").with_replication(ReplicationKind::Ap);
        let spec = DatabaseSpec::from_db_config("vectors", &config);

        assert_eq!(spec.profile, "vector");
        assert_eq!(spec.deployment.mode, DeploymentMode::SingleNode);
        assert_eq!(spec.deployment.storage_path, Some("vector.redb".to_owned()));
        assert_eq!(spec.defaults.replication, ReplicationMode::Ap);
        assert_eq!(spec.domains[0].mode, ConsistencyMode::EventualAp);
        assert_eq!(
            spec.guarantees.conflict_resolution,
            super::ConflictResolution::VectorClock
        );
    }

    #[test]
    fn phase37_catalog_contains_profiles_roles_and_domains() {
        assert_eq!(built_in_profiles().len(), 6);
        assert_eq!(collection_role_definitions().len(), 9);
        assert_eq!(consistency_domain_definitions().len(), 3);

        let mut profile_names = BTreeSet::new();
        for profile in built_in_profiles() {
            assert!(profile_names.insert(profile.slug));
            assert!(!profile.description.trim().is_empty());
            assert!(!profile.compatible_roles.is_empty());
            assert!(consistency_domain_definition(profile.default_domain).is_some());

            for alias in profile.aliases {
                assert!(profile_names.insert(alias));
                let Some(resolved) = built_in_profile(alias) else {
                    panic!("missing profile alias {alias}");
                };
                assert_eq!(resolved.slug, profile.slug);
            }
        }

        for role in collection_role_definitions() {
            assert!(!role.description.trim().is_empty());
            assert!(!role.required_capabilities.is_empty());
            assert!(!role.constraints.is_empty());
            assert_eq!(collection_role_definition(role.role), Some(role));
        }

        for domain in consistency_domain_definitions() {
            assert!(!domain.guarantees.is_empty());
            assert!(!domain.limits.is_empty());
            assert_eq!(consistency_domain_definition(domain.mode), Some(domain));
        }

        let extension_names = extension_capability_definitions()
            .iter()
            .map(|definition| definition.slug)
            .collect::<BTreeSet<_>>();
        assert_eq!(extension_names.len(), 8);
        assert!(extension_names.contains("audit"));
        assert!(extension_names.contains("full_text"));
        assert!(extension_names.contains("vector_hnsw"));
    }

    #[test]
    fn phase43_built_in_extension_manifests_validate_and_cover_capabilities() {
        let manifest_names = built_in_extension_manifests()
            .iter()
            .map(|manifest| manifest.name.as_str())
            .collect::<BTreeSet<_>>();
        let capability_names = extension_capability_definitions()
            .iter()
            .map(|definition| definition.slug)
            .collect::<BTreeSet<_>>();

        assert_eq!(manifest_names, capability_names);
        for manifest in built_in_extension_manifests() {
            let report = ExtensionManifestValidator::validate(manifest);
            assert!(
                report.valid,
                "{} manifest should validate: {:?}",
                manifest.name, report.issues
            );
        }
    }

    #[test]
    fn phase43_manifest_validator_rejects_core_boundary_ownership() {
        let Some(base) = built_in_extension_manifest("vector_hnsw") else {
            panic!("missing vector_hnsw manifest");
        };
        let mut manifest = base.clone();
        manifest.core_boundary.wal = ExtensionCoreOwner::ExtensionOwned;

        let report = ExtensionManifestValidator::validate(&manifest);

        assert!(!report.valid);
        assert!(issue_codes(&report).contains("EXTENSION_CORE_BOUNDARY_VIOLATION"));
    }

    #[test]
    fn phase43_manifest_validator_rejects_registry_without_provide() {
        let Some(base) = built_in_extension_manifest("full_text") else {
            panic!("missing full_text manifest");
        };
        let mut manifest = base.clone();
        manifest.provides.indexes.clear();

        let report = ExtensionManifestValidator::validate(&manifest);

        assert!(!report.valid);
        assert!(issue_codes(&report).contains("EXTENSION_REGISTRY_WITHOUT_PROVIDE"));
    }

    #[test]
    fn phase43_extension_catalog_compilation_is_deterministic() {
        let mut reversed = built_in_extension_manifests().to_vec();
        reversed.reverse();

        let first = compile_extension_catalog(built_in_extension_manifests());
        let second = compile_extension_catalog(&reversed);

        assert_eq!(first, second);
        assert!(first.manifests.contains(&"vector_hnsw".to_owned()));
        assert!(first.indexes.contains(&"hnsw".to_owned()));
        assert!(first.ui_panels.contains(&"vector-indexes".to_owned()));
    }

    #[test]
    fn technical_profile_aliases_resolve_to_product_profiles() {
        let aliases = [
            ("in_memory", "game_local_balanced"),
            ("balanced", "game_local_balanced"),
            ("document", "desktop_app_embedded"),
            ("vector", "ai_agent_memory"),
            ("transactional", "secure_app"),
            ("high_durability", "secure_app"),
            ("analytical", "analytics_columnar"),
            ("time_series", "analytics_columnar"),
        ];

        for (alias, expected_slug) in aliases {
            let Some(profile) = built_in_profile(alias) else {
                panic!("missing alias {alias}");
            };
            assert_eq!(profile.slug, expected_slug);
        }

        for profile in Profile::all() {
            let config = if profile == Profile::InMemory {
                DbConfig::new(profile)
            } else {
                DbConfig::on_disk(profile, format!("{profile:?}.redb"))
            };
            let spec = DatabaseSpec::from_db_config(format!("{profile:?}"), &config);
            assert!(
                built_in_profile(&spec.profile).is_some(),
                "missing catalog alias for {}",
                spec.profile
            );
        }
    }

    #[test]
    fn catalog_support_status_reports_certified_builtin() {
        let mut spec = catalog_spec("game_local_balanced", ConsistencyMode::LocalSnapshot);
        spec.collections.push(CollectionRoleSpec {
            name: "settings".to_owned(),
            role: CollectionRole::KeyValue,
            domain: "primary".to_owned(),
            indexes: vec![CollectionIndexKind::Primary],
        });

        assert_eq!(spec.catalog_support_status(), SupportStatus::Certified);
    }

    #[test]
    fn catalog_support_status_reports_custom_and_invalid_specs() {
        let mut unknown_profile = catalog_spec("unknown_profile", ConsistencyMode::LocalSnapshot);
        unknown_profile.collections.push(CollectionRoleSpec {
            name: "settings".to_owned(),
            role: CollectionRole::KeyValue,
            domain: "primary".to_owned(),
            indexes: vec![CollectionIndexKind::Primary],
        });
        assert_eq!(
            unknown_profile.catalog_support_status(),
            SupportStatus::Custom
        );

        let mut invalid = catalog_spec("game_local_balanced", ConsistencyMode::LocalSnapshot);
        invalid.name.clear();
        assert_eq!(invalid.catalog_support_status(), SupportStatus::Invalid);
    }

    #[test]
    fn catalog_support_status_reports_custom_for_incompatible_role() {
        let mut spec = catalog_spec("game_local_balanced", ConsistencyMode::LocalSnapshot);
        spec.collections.push(CollectionRoleSpec {
            name: "graph".to_owned(),
            role: CollectionRole::Graph,
            domain: "primary".to_owned(),
            indexes: vec![CollectionIndexKind::Graph],
        });

        assert_eq!(spec.catalog_support_status(), SupportStatus::Custom);
    }

    #[test]
    fn catalog_support_status_uses_weakest_catalog_status() {
        let mut production = catalog_spec("production_cp", ConsistencyMode::StrongCp);
        production.guarantees.backup.enabled = true;
        assert_eq!(production.catalog_support_status(), SupportStatus::Stable);

        let mut eventual = catalog_spec("secure_app", ConsistencyMode::EventualAp);
        eventual.guarantees.conflict_resolution = super::ConflictResolution::VectorClock;
        assert_eq!(
            eventual.catalog_support_status(),
            SupportStatus::Experimental
        );
    }

    #[test]
    fn phase38_validator_blocks_hard_conflicts() {
        let mut cases = Vec::new();

        let mut cp_local_ack = catalog_spec("secure_app", ConsistencyMode::StrongCp);
        cp_local_ack.guarantees.write_ack = WriteAck::Local;
        cases.push(("CP_LOCAL_ACK", cp_local_ack));

        let mut ap_without_conflict = catalog_spec("ai_agent_memory", ConsistencyMode::EventualAp);
        ap_without_conflict.defaults.replication = ReplicationMode::Ap;
        ap_without_conflict.guarantees.conflict_resolution = super::ConflictResolution::None;
        cases.push(("AP_MISSING_CONFLICT_POLICY", ap_without_conflict));

        let production_without_backup = catalog_spec("production_cp", ConsistencyMode::StrongCp);
        cases.push(("PRODUCTION_BACKUP_DISABLED", production_without_backup));

        let mut sensitive_without_encryption =
            catalog_spec("secure_app", ConsistencyMode::StrongCp);
        sensitive_without_encryption.guarantees.sensitive_data = true;
        cases.push(("SENSITIVE_WITHOUT_ENCRYPTION", sensitive_without_encryption));

        let mut vector_without_index =
            catalog_spec("ai_agent_memory", ConsistencyMode::LocalSnapshot);
        vector_without_index.collections.push(collection(
            "memory",
            CollectionRole::VectorMemory,
            Vec::new(),
        ));
        cases.push(("VECTOR_INDEX_REQUIRED", vector_without_index));

        let mut graph_without_index =
            catalog_spec("game_local_balanced", ConsistencyMode::LocalSnapshot);
        graph_without_index.collections.push(collection(
            "graph",
            CollectionRole::Graph,
            Vec::new(),
        ));
        cases.push(("GRAPH_INDEX_REQUIRED", graph_without_index));

        let mut audit_disabled = catalog_spec("secure_app", ConsistencyMode::StrongCp);
        audit_disabled.guarantees.audit.enabled = false;
        audit_disabled.collections.push(collection(
            "audit_log",
            CollectionRole::Audit,
            vec![CollectionIndexKind::Primary],
        ));
        cases.push(("AUDIT_DISABLED", audit_disabled));

        let mut strict_mixed = catalog_spec("secure_app", ConsistencyMode::StrongCp);
        strict_mixed.guarantees.conflict_resolution = super::ConflictResolution::VectorClock;
        strict_mixed.guarantees.strict_cross_domain_transactions = true;
        strict_mixed.domains.push(ConsistencyDomainSpec {
            name: "async".to_owned(),
            mode: ConsistencyMode::EventualAp,
        });
        cases.push(("STRICT_CP_AP_CROSS_DOMAIN_TXN", strict_mixed));

        for (expected, spec) in cases {
            let report = GuaranteeValidator::validate(&spec);
            assert!(!report.valid, "{expected} should make the report invalid");
            assert_eq!(report.status, SupportStatus::Invalid);
            assert!(
                issue_codes(&report).contains(expected),
                "missing issue code {expected}: {:?}",
                report.issues
            );
        }
    }

    #[test]
    fn validator_enforces_topology_replica_rules() {
        let mut single_node = catalog_spec("game_local_balanced", ConsistencyMode::LocalSnapshot);
        single_node.topology.replica_count = 2;

        let mut cluster_cp = catalog_spec("production_cp", ConsistencyMode::StrongCp);
        cluster_cp.deployment.mode = DeploymentMode::Cluster;
        cluster_cp.defaults.replication = ReplicationMode::Cp;
        cluster_cp.topology.replica_count = 2;
        cluster_cp.guarantees.backup.enabled = true;

        let mut eventual_ap_cluster = catalog_spec("ai_agent_memory", ConsistencyMode::EventualAp);
        eventual_ap_cluster.deployment.mode = DeploymentMode::Cluster;
        eventual_ap_cluster.defaults.replication = ReplicationMode::Ap;
        eventual_ap_cluster.guarantees.conflict_resolution = super::ConflictResolution::VectorClock;
        eventual_ap_cluster.topology.replica_count = 1;

        let mut zero_shards = catalog_spec("game_local_balanced", ConsistencyMode::LocalSnapshot);
        zero_shards.topology.shard_count = 0;

        let cases = [
            ("TOPOLOGY_LOCAL_REPLICA_COUNT", single_node),
            ("TOPOLOGY_CP_CLUSTER_QUORUM", cluster_cp),
            ("TOPOLOGY_AP_CLUSTER_REPLICAS", eventual_ap_cluster),
            ("TOPOLOGY_SHARD_COUNT_ZERO", zero_shards),
        ];

        for (expected, spec) in cases {
            let report = GuaranteeValidator::validate(&spec);
            assert!(!report.valid, "{expected} should make the report invalid");
            assert!(
                issue_codes(&report).contains(expected),
                "missing issue code {expected}: {:?}",
                report.issues
            );
        }
    }

    #[test]
    fn validator_accepts_cluster_cp_with_odd_replica_quorum() {
        let mut spec = catalog_spec("production_cp", ConsistencyMode::StrongCp);
        spec.deployment.mode = DeploymentMode::Cluster;
        spec.defaults.replication = ReplicationMode::Cp;
        spec.topology.replica_count = 3;
        spec.guarantees.backup.enabled = true;

        let report = GuaranteeValidator::validate(&spec);

        assert!(report.valid, "{:?}", report.issues);
    }

    #[test]
    fn phase38_validator_reports_actionable_issue_metadata() {
        let mut spec = catalog_spec("secure_app", ConsistencyMode::StrongCp);
        spec.guarantees.write_ack = WriteAck::Local;

        let report = GuaranteeValidator::validate(&spec);

        assert!(!report.valid);
        for issue in report
            .issues
            .iter()
            .filter(|issue| issue.severity == ValidationSeverity::Error)
        {
            assert!(!issue.path.trim().is_empty());
            assert!(!issue.suggestion.trim().is_empty());
        }
    }

    #[test]
    fn phase38_validator_warns_for_custom_profile_role_and_experimental_extension() {
        let mut spec = catalog_spec("unknown_profile", ConsistencyMode::LocalSnapshot);
        spec.collections.push(collection(
            "graph",
            CollectionRole::Graph,
            vec![CollectionIndexKind::Graph],
        ));
        spec.extensions.push(ExtensionManifestRef {
            name: "lab-index".to_owned(),
            version: "0.1.0".to_owned(),
            stability: ExtensionStability::Experimental,
        });

        let report = GuaranteeValidator::validate(&spec);

        assert!(report.valid);
        assert_eq!(report.status, SupportStatus::Custom);
        assert!(issue_codes(&report).contains("CUSTOM_PROFILE"));
        assert!(issue_codes(&report).contains("EXPERIMENTAL_EXTENSION"));

        let mut role_warning = catalog_spec("game_local_balanced", ConsistencyMode::LocalSnapshot);
        role_warning.collections.push(collection(
            "graph",
            CollectionRole::Graph,
            vec![CollectionIndexKind::Graph],
        ));
        let role_report = GuaranteeValidator::validate(&role_warning);
        assert!(role_report.valid);
        assert_eq!(role_report.status, SupportStatus::Custom);
        assert!(issue_codes(&role_report).contains("ROLE_OUTSIDE_PROFILE_CERTIFICATION"));
    }

    #[test]
    fn phase43_validator_detects_implicit_extension_requirements() {
        let mut spec = catalog_spec("ai_agent_memory", ConsistencyMode::LocalSnapshot);
        spec.collections.push(collection(
            "memory",
            CollectionRole::VectorMemory,
            vec![CollectionIndexKind::Vector],
        ));

        let report = GuaranteeValidator::validate(&spec);

        assert!(report.valid);
        assert!(
            report.issues.iter().any(|issue| {
                issue.code == "IMPLICIT_EXTENSION_REQUIRED" && issue.message.contains("vector_hnsw")
            }),
            "missing implicit vector_hnsw extension issue: {:?}",
            report.issues
        );
    }

    #[test]
    fn phase43_unknown_extensions_are_custom_but_allowed() {
        let mut spec = catalog_spec("game_local_balanced", ConsistencyMode::LocalSnapshot);
        spec.extensions.push(ExtensionManifestRef {
            name: "third_party_codec".to_owned(),
            version: "1.0.0".to_owned(),
            stability: ExtensionStability::Stable,
        });

        let report = GuaranteeValidator::validate(&spec);

        assert!(report.valid);
        assert_eq!(report.status, SupportStatus::Custom);
        assert!(issue_codes(&report).contains("CUSTOM_EXTENSION"));
    }

    #[test]
    fn phase38_conflicting_config_is_never_certified() {
        let mut spec = catalog_spec("game_local_balanced", ConsistencyMode::LocalSnapshot);
        spec.guarantees.write_ack = WriteAck::Local;
        spec.domains[0].mode = ConsistencyMode::StrongCp;

        assert_eq!(spec.catalog_support_status(), SupportStatus::Invalid);
    }

    #[test]
    fn policy_compiler_is_deterministic_and_rejects_invalid_specs() {
        let mut spec = catalog_spec("ai_agent_memory", ConsistencyMode::LocalSnapshot);
        spec.collections.push(collection(
            "memory",
            CollectionRole::VectorMemory,
            vec![CollectionIndexKind::Vector],
        ));
        spec.extensions.push(ExtensionManifestRef {
            name: "stable-codec".to_owned(),
            version: "1.0.0".to_owned(),
            stability: ExtensionStability::Stable,
        });

        let Ok(first) = PolicyCompiler::compile(&spec) else {
            panic!("valid spec should compile");
        };
        let Ok(second) = PolicyCompiler::compile(&spec) else {
            panic!("valid spec should compile");
        };

        assert_eq!(first, second);
        assert_eq!(first.storage_profile, "ai_agent_memory");
        assert_eq!(first.replication_kind, ReplicationMode::Cp);
        assert_eq!(
            first.required_extensions,
            vec!["stable-codec".to_owned(), "vector_hnsw".to_owned()]
        );

        spec.guarantees.write_ack = WriteAck::Local;
        spec.domains[0].mode = ConsistencyMode::StrongCp;
        assert!(PolicyCompiler::compile(&spec).is_err());
    }

    #[test]
    fn phase39_explain_covers_compiled_policy_decisions() {
        let mut spec = catalog_spec("ai_agent_memory", ConsistencyMode::LocalSnapshot);
        spec.collections.push(collection(
            "memory",
            CollectionRole::VectorMemory,
            vec![CollectionIndexKind::Vector],
        ));
        spec.extensions.push(ExtensionManifestRef {
            name: "stable-codec".to_owned(),
            version: "1.0.0".to_owned(),
            stability: ExtensionStability::Stable,
        });

        let report = ConfigExplainer::explain(&spec);
        let paths = report
            .decisions
            .iter()
            .map(|decision| decision.path.as_str())
            .collect::<BTreeSet<_>>();

        assert!(report.validation.valid);
        assert!(report.compiled_policy.is_some());
        assert!(paths.contains("$.compiled_policy.storage_profile"));
        assert!(paths.contains("$.compiled_policy.replication_kind"));
        assert!(paths.contains("$.compiled_policy.runtime_limits"));
        assert!(paths.contains("$.compiled_policy.required_extensions"));
        assert!(paths.contains("$.compiled_policy.collections.memory.role"));
        assert!(paths.contains("$.compiled_policy.collections.memory.indexes"));
        assert!(
            report
                .decisions
                .iter()
                .any(|decision| decision.value.contains("vector_hnsw"))
        );
    }

    #[test]
    fn phase39_explain_invalid_spec_has_no_compiled_policy() {
        let mut spec = catalog_spec("secure_app", ConsistencyMode::StrongCp);
        spec.guarantees.write_ack = WriteAck::Local;

        let report = ConfigExplainer::explain(&spec);

        assert!(!report.validation.valid);
        assert!(report.compiled_policy.is_none());
        assert!(
            report
                .decisions
                .iter()
                .any(|decision| decision.path == "$.compiled_policy")
        );
    }

    #[test]
    fn phase39_plan_id_is_deterministic_and_content_addressed() {
        let current = catalog_spec("game_local_balanced", ConsistencyMode::LocalSnapshot);
        let mut desired = current.clone();
        desired
            .operation_hints
            .insert("planner.sample_rows".to_owned(), "1000".to_owned());

        let first = MigrationPlanner::plan(&current, &desired);
        let second = MigrationPlanner::plan(&current, &desired);
        desired
            .operation_hints
            .insert("planner.sample_rows".to_owned(), "2000".to_owned());
        let changed = MigrationPlanner::plan(&current, &desired);

        assert_eq!(first.plan_id, second.plan_id);
        assert_eq!(first.required_confirmation, first.plan_id);
        assert_ne!(first.plan_id, changed.plan_id);
        assert!(first.valid);
        assert!(first.apply_supported);
    }

    #[test]
    fn policy_compiler_carries_topology_into_compiled_policy()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut spec = catalog_spec("production_cp", ConsistencyMode::StrongCp);
        spec.deployment.mode = DeploymentMode::Cluster;
        spec.defaults.replication = ReplicationMode::Cp;
        spec.topology = TopologySpec {
            replica_count: 3,
            shard_count: 2,
        };
        spec.guarantees.backup.enabled = true;

        let policy =
            PolicyCompiler::compile(&spec).map_err(|report| format!("{:?}", report.issues))?;

        assert_eq!(policy.topology, spec.topology);
        Ok(())
    }

    #[test]
    fn planner_detects_replica_count_change_as_unsupported_topology_step()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut current = catalog_spec("production_cp", ConsistencyMode::StrongCp);
        current.deployment.mode = DeploymentMode::Cluster;
        current.defaults.replication = ReplicationMode::Cp;
        current.topology.replica_count = 3;
        current.guarantees.backup.enabled = true;

        let mut desired = current.clone();
        desired.topology.replica_count = 5;

        let plan = MigrationPlanner::plan(&current, &desired);
        let topology_step = plan
            .steps
            .iter()
            .find(|step| step.kind == MigrationStepKind::ChangeTopology)
            .ok_or("missing topology step")?;

        assert!(plan.valid, "{:?}", plan.desired_validation.issues);
        assert!(!plan.apply_supported);
        assert_eq!(topology_step.path, "$.topology.replica_count");
        assert!(!topology_step.supported);
        Ok(())
    }

    #[test]
    fn phase39_dry_run_marks_high_risk_changes_and_rollback() {
        let mut current = catalog_spec("game_local_balanced", ConsistencyMode::LocalSnapshot);
        current.collections.push(collection(
            "items",
            CollectionRole::KeyValue,
            vec![CollectionIndexKind::Primary],
        ));
        let mut desired = current.clone();
        desired.profile = "ai_agent_memory".to_owned();
        desired.domains[0].mode = ConsistencyMode::EventualAp;
        desired.defaults.replication = ReplicationMode::Ap;
        desired.guarantees.conflict_resolution = super::ConflictResolution::VectorClock;
        desired.collections[0].role = CollectionRole::VectorMemory;
        desired.collections[0].indexes = vec![CollectionIndexKind::Vector];
        desired.extensions.push(ExtensionManifestRef {
            name: "lab-vector".to_owned(),
            version: "0.1.0".to_owned(),
            stability: ExtensionStability::Experimental,
        });

        let plan = MigrationPlanner::plan(&current, &desired);
        let kinds = plan
            .steps
            .iter()
            .map(|step| step.kind)
            .collect::<BTreeSet<_>>();

        assert!(plan.valid);
        assert!(!plan.apply_supported);
        assert!(plan.impact.requires_backup);
        assert!(plan.impact.requires_downtime);
        assert!(!plan.rollback.possible);
        assert!(kinds.contains(&MigrationStepKind::ChangeProfile));
        assert!(kinds.contains(&MigrationStepKind::ChangeDomain));
        assert!(kinds.contains(&MigrationStepKind::ChangeCollection));
        assert!(kinds.contains(&MigrationStepKind::ChangeIndex));
        assert!(kinds.contains(&MigrationStepKind::ChangeExtension));
    }

    #[test]
    fn phase39_apply_check_confirms_only_and_never_mutates_data() {
        let current = catalog_spec("game_local_balanced", ConsistencyMode::LocalSnapshot);
        let mut metadata_only = current.clone();
        metadata_only
            .operation_hints
            .insert("planner.sample_rows".to_owned(), "1000".to_owned());

        let confirmed_plan = MigrationPlanner::plan(&current, &metadata_only);
        let confirmed =
            MigrationPlanner::check_apply(&confirmed_plan, &confirmed_plan.required_confirmation);

        assert_eq!(confirmed.status, ApplyStatus::Confirmed);
        assert!(confirmed.valid);
        assert!(confirmed.confirmation_matched);
        assert!(!confirmed.audit_recorded);
        assert!(!confirmed.data_mutated);
    }

    #[test]
    fn phase39_apply_check_rejects_wrong_plan_id_and_unsupported_plans() {
        let current = catalog_spec("game_local_balanced", ConsistencyMode::LocalSnapshot);
        let mut desired = current.clone();
        desired.profile = "secure_app".to_owned();
        desired.domains[0].mode = ConsistencyMode::StrongCp;

        let plan = MigrationPlanner::plan(&current, &desired);
        let wrong = MigrationPlanner::check_apply(&plan, "wrong");
        let unsupported = MigrationPlanner::check_apply(&plan, &plan.required_confirmation);

        assert_eq!(wrong.status, ApplyStatus::Rejected);
        assert!(!wrong.valid);
        assert!(!wrong.confirmation_matched);
        assert!(!wrong.audit_recorded);
        assert!(!wrong.data_mutated);
        assert_eq!(unsupported.status, ApplyStatus::Unsupported);
        assert!(!unsupported.valid);
        assert!(unsupported.confirmation_matched);
        assert!(!unsupported.audit_recorded);
        assert!(!unsupported.data_mutated);
    }
}
