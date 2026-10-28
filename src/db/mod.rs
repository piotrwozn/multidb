use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    ops::ControlFlow,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
    thread,
    time::{Duration, Instant, UNIX_EPOCH},
};

use serde::{Serialize, de::DeserializeOwned};
use sqlparser::{
    ast::{
        Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName,
        ObjectNamePart, Query as SqlQuery, SelectItem, SetExpr, Statement,
        TableFactor as SqlTableFactor, TableObject as SqlTableObject, UnaryOperator,
        Value as SqlValue, Visit, Visitor,
    },
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use tokio::sync::Semaphore;

use crate::autoscale::ResourcePolicy;
use crate::cdc::{
    self, ChangeEvent, ChangeOp, ChangefeedFilter, ChangefeedOptions, ChangefeedPage, FeedError,
    HookError, HookSpec, LogicalTarget, MaterializedViewError, MaterializedViewRows,
    MaterializedViewSpec, PostCommitFailure, PostCommitStage, ResumeToken, SubscriptionConfig,
    SubscriptionError, SubscriptionState,
};
use crate::cloud::{CloudConfig, TenantConfig, TenantId, TenantRuntime, repl_quota_error};
use crate::compat::{self, PgCatalogSnapshot};
use crate::config_spec::{
    ApplyCheckReport, ApplyStatus, DatabaseSpec, MigrationPlan, MigrationPlanner,
};
use crate::continuous::{
    self, ContinuousError, ContinuousQuerySpec, ContinuousQueryState, OutboxConnectorSpec,
    ProcedureCommand, ProcedureResult, ProcedureSpec, TriggerEvent, TriggerOutcome, TriggerSpec,
    TriggerTiming,
};
use crate::extension::{
    self, CodecSpec, CollationSpec, ExtensionError, PolicyConfig, UdfSpec, WasmRuntime,
};
use crate::federation::{
    FederationError, ForeignSource, ForeignTableOptions, ForeignTableSpec, ForeignTableStats,
    validate_foreign_source,
};
use crate::geo::{GeoError, GeoIndex, GeoIndexConfig, GeoPoint};
use crate::graph::{Graph, GraphError, GraphId, GraphNodeId, TraversalOptions};
use crate::keyenc;
use crate::model::{
    CollectionId, DOCUMENT_TABLE, DerivedIndexes, DocumentCollection, DocumentId, IndexSpec,
    ModelError, Predicate, QueryResult, Value, collection_range_bounds, decode_value, encode_value,
};
use crate::observability;
use crate::observability::MetricsConfig;
use crate::performance::{
    BenchmarkReport, CompressionAlgorithm, PerformanceCache, PerformanceConfig,
    QueryExecutionConfig, RegressionGate,
};
use crate::query::{
    AnalyzeMode, AnalyzeReport, AnalyzeTarget, CostProfile, DocField, ExplainOptions,
    ExplainReport, PlanCache, PlanCacheMetrics, PlannerFeedback, QueryError,
    REL_COLUMNAR_SEGMENTS_TABLE, REL_INDEX_TABLE, REL_ROWS_TABLE, RelIndexSpec, RelTable, Row,
    SqlEngine, SqlOutput, SqlReadGuard, SqlReadTarget, SqlRelationPolicy, SqlRows, StatsCatalog,
    TableLayout, TableSchema, decode_row_bytes, parse_with_limits,
};
use crate::repl::{
    ApClusterConfig, ApDynamo, ConditionalBatch, CpClusterConfig, CpRaft, HealingAction,
    HealingBackend, HealingMode, HealingPolicy, HealthConfig, HealthProbe, HealthView,
    ManualHealthProbe, NodeId, Op, PartitionStrategy, RaftNode, ReadConsistency, ReplError,
    Replication, SelfHealingController, ShardId, ShardMap, ShardedReplication, SingleNode,
    VectorClock, VersionedBytes, WriteCondition, propose_system_batch, validate_ap_cluster_config,
    validate_cp_cluster_config,
};
use crate::runtime_advisor::{
    RuntimeAdviceDecision, RuntimeAdviceDecisionRequest, RuntimeAdviceReport, RuntimeAdvisor,
    RuntimeAdvisorError,
};
use crate::security::{
    AUDIT_HEAD_TABLE, AUDIT_TABLE, AuditEvent, AuditHead, AuditOutcome, AuthzError, AuthzPolicy,
    Permission, Principal, PrincipalRegistry, Resource, Role,
};
use crate::storage::{
    AnyEngine, AnyReadTxn, Bytes, EngineKind, ReadTransaction, ReencryptionReport, StorageEngine,
    StorageError, WriteTransaction, decode_stored_value, is_framed,
};
use crate::temporal::{TemporalError, TemporalPoint, TemporalRetention};
use crate::text::{FullTextIndex, FullTextIndexConfig, TextError, TextSource};
use crate::timeseries::{TimeSeriesCollection, TimeSeriesConfig, TimeSeriesError, time_bucket};
use crate::tuning::{
    self, IndexAdviceReport, IndexAdvisor, ReprofileJob, ReprofilePlan, ReprofileStatus,
    TuningDecision, TuningError, TuningLogEntry, TuningPolicy, TuningSystemView, WorkloadBuffer,
    WorkloadReport, WorkloadSample, WorkloadWindow,
};
use crate::txn::{self, IsolationLevel, TxnId, TxnOptions, WriteKey, WriteSet};
use crate::vector::{
    DiskAnnConfig, HnswParams, QuantizationConfig, SharedVectorIndex, VectorCollection,
    VectorCollectionConfig, VectorError, VectorIndexState, VectorMetric,
};

mod audit_batch;
mod changefeed_policy;
mod graphrag;
mod history;
mod lifecycle;
mod profile_binding;
mod provisioning;
mod review_fixes;
mod runtime;
mod sql_cache;
mod tensor;
mod vector_search;

pub(crate) use profile_binding::{
    MANIFEST_VERSION, create_restore_target, decode_profile_manifest, performance_for_manifest,
    unmet_profile_floors, validate_manifest,
};
pub use profile_binding::{
    MetadataProbe, PersistedProfileManifest, ProbedMetadata, RuntimeProfileBinding, StorageFormat,
    create_database_with_profile, inspect_database_metadata,
    inspect_database_metadata_with_encryption, recover_database_file,
};
use runtime::wasm_runtime_for_database;

const META_TABLE: &str = "__meta";
const CATALOG_TABLE: &str = "__catalog__";
const KEY_SCHEMA_VERSION: &[u8] = b"schema_version";
const KEY_SCHEMA_VERSION_NAME: &str = "schema_version";
const KEY_PROFILE: &[u8] = b"profile";
const KEY_PROFILE_NAME: &str = "profile";
const KEY_REPLICATION: &[u8] = b"replication";
const KEY_REPLICATION_NAME: &str = "replication";
const KEY_LAYOUT: &[u8] = b"layout";
const KEY_LAYOUT_NAME: &str = "layout";
const KEY_PROFILE_MANIFEST: &[u8] = b"profile_manifest";
const KEY_NEXT_COLLECTION_ID: &[u8] = b"next_collection_id";
const KEY_NEXT_GRAPH_ID: &[u8] = b"next_graph_id";
/// `schema_version` of every database whose values are not compression-framed.
pub(crate) const SCHEMA_VERSION: u32 = 1;
/// `schema_version` of a profile-manifest database with value framing. Older binaries refuse it.
pub(crate) const SCHEMA_VERSION_FRAMED: u32 = 2;
/// A legacy database (no profile manifest) whose values are compression-framed, read through an
/// engine that does not frame values.
const LEGACY_FRAMED_DATABASE: &str =
    "legacy compression-framed database: open with OperationalConfig.performance.compression";
/// Every encryption frame starts with this prefix (`MDBE1`, `MDBE2`, `MDBE3`).
const ENCRYPTED_FRAME_PREFIX: &[u8] = b"MDBE";
const AUTHZ_TABLE: &str = "__authz_policy";
const KEY_AUTHZ_POLICY: &[u8] = b"policy";
const KEY_PRINCIPAL_REGISTRY: &[u8] = b"principals";
const KEY_AUDIT_ENABLED: &[u8] = b"audit_enabled";
const ADMIN_AUTH_TABLE: &str = "__admin_auth";
/// The system table of spec provisioning: the provision record (key `record`) and its history
/// (keys `history/{seq:020}`). Backup and restore carry it like every registered keyspace.
pub(crate) const PROVISION_TABLE: &str = "__provision";
/// The characters of a config apply plan id its audit detail keeps; real `plan_` ids have 37.
const CONFIG_APPLY_DETAIL_PLAN_ID_CHARS: usize = 48;

/// Where the plan a config apply confirms comes from, recorded as `plan_source` in its audit
/// event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConfigPlanSource {
    /// The caller sent the plan: `POST /config/apply` with `plan`, or the Rust API.
    Client,
    /// The Control Plane recomputed the plan from the `current` and `desired` specs it was sent.
    Recomputed,
}

impl ConfigPlanSource {
    const fn slug(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Recomputed => "recomputed",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Profile {
    InMemory,
    Transactional,
    Analytical,
    Document,
    Vector,
    TimeSeries,
    HighDurability,
    #[default]
    Balanced,
    Tensor,
}

impl Profile {
    #[must_use]
    pub const fn all() -> [Self; 9] {
        [
            Self::InMemory,
            Self::Transactional,
            Self::Analytical,
            Self::Document,
            Self::Vector,
            Self::TimeSeries,
            Self::HighDurability,
            Self::Balanced,
            Self::Tensor,
        ]
    }

    #[must_use]
    pub const fn is_in_memory(self) -> bool {
        matches!(self, Self::InMemory)
    }

    #[must_use]
    pub const fn is_on_disk(self) -> bool {
        !self.is_in_memory()
    }

    /// Whether opening this profile requires a physical GPU compute adapter.
    #[must_use]
    pub const fn requires_gpu(self) -> bool {
        matches!(self, Self::Tensor)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ReplicationKind {
    #[default]
    Cp,
    Ap,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DbConfig {
    pub profile: Profile,
    pub replication: ReplicationKind,
    pub path: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecurityConfig {
    pub authz_policy: AuthzPolicy,
    pub principals: PrincipalRegistry,
    pub audit_enabled: bool,
    pub audit: AuditConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AdminCredentialRecord {
    pub username: String,
    pub password_hash: String,
    pub updated_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncryptionConfig {
    pub key_path: PathBuf,
    pub mode: EncryptionMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EncryptionMode {
    LegacyFile,
    LocalEnvelope {
        keyring_path: PathBuf,
        kek_path: PathBuf,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AuditConfig {
    pub key_path: Option<PathBuf>,
    pub anchor_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OperationalConfig {
    pub security: SecurityConfig,
    pub encryption: Option<EncryptionConfig>,
    pub metrics: Option<MetricsConfig>,
    pub resources: Option<ResourcePolicy>,
    pub performance: Option<PerformanceConfig>,
    pub cloud: Option<CloudConfig>,
    pub tenant: Option<TenantConfig>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProfileValidationReport {
    pub profile: Profile,
    pub replication: ReplicationKind,
    pub layout: TableLayout,
    pub engine_kind: EngineKind,
    pub durable_storage: bool,
    pub capabilities: BTreeSet<&'static str>,
}

impl DbConfig {
    #[must_use]
    pub fn new(profile: Profile) -> Self {
        Self {
            profile,
            replication: ReplicationKind::default(),
            path: None,
        }
    }

    #[must_use]
    pub fn on_disk(profile: Profile, path: impl Into<PathBuf>) -> Self {
        Self {
            profile,
            replication: ReplicationKind::default(),
            path: Some(path.into()),
        }
    }

    #[must_use]
    pub fn with_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = Some(path.into());
        self
    }

    #[must_use]
    pub fn with_replication(mut self, replication: ReplicationKind) -> Self {
        self.replication = replication;
        self
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            authz_policy: AuthzPolicy::default(),
            principals: PrincipalRegistry::default(),
            audit_enabled: true,
            audit: AuditConfig::default(),
        }
    }
}

impl EncryptionConfig {
    #[must_use]
    pub fn file_key(path: impl Into<PathBuf>) -> Self {
        let key_path = path.into();
        Self {
            key_path,
            mode: EncryptionMode::LegacyFile,
        }
    }

    #[must_use]
    pub fn local_envelope(keyring_path: impl Into<PathBuf>, kek_path: impl Into<PathBuf>) -> Self {
        let keyring_path = keyring_path.into();
        let kek_path = kek_path.into();
        Self {
            key_path: keyring_path.clone(),
            mode: EncryptionMode::LocalEnvelope {
                keyring_path,
                kek_path,
            },
        }
    }
}

impl OperationalConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_security(mut self, security: SecurityConfig) -> Self {
        self.security = security;
        self
    }

    #[must_use]
    pub fn with_encryption(mut self, encryption: EncryptionConfig) -> Self {
        self.encryption = Some(encryption);
        self
    }

    #[must_use]
    pub fn with_performance(mut self, performance: PerformanceConfig) -> Self {
        self.performance = Some(performance);
        self
    }

    #[must_use]
    pub fn with_cloud(mut self, cloud: CloudConfig) -> Self {
        self.cloud = Some(cloud);
        self
    }

    #[must_use]
    pub fn with_tenant(mut self, tenant: TenantConfig) -> Self {
        self.tenant = Some(tenant);
        self
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ConfigError {
    #[error("profile {profile:?} is volatile but a path was provided")]
    VolatileWithPath { profile: Profile },

    #[error("profile {profile:?} requires a database path")]
    MissingPath { profile: Profile },

    #[error("profile mismatch: expected {expected:?}, found {found:?}")]
    ProfileMismatch { expected: Profile, found: Profile },

    #[error("replication mismatch: expected {expected:?}, found {found:?}")]
    ReplicationMismatch {
        expected: ReplicationKind,
        found: ReplicationKind,
    },

    #[error("layout mismatch: expected {expected:?}, found {found:?}")]
    LayoutMismatch {
        expected: TableLayout,
        found: TableLayout,
    },

    #[error("missing database metadata key: {key}")]
    MissingMetadata { key: &'static str },

    #[error("invalid cluster config: {0}")]
    InvalidClusterConfig(String),

    #[error("unsupported config: {0}")]
    Unsupported(String),

    #[error("metadata schema version mismatch: expected {expected}, found {found}")]
    SchemaVersionMismatch { expected: u32, found: u32 },

    #[error(
        "database metadata already exists; create_database_with_profile only initializes new databases"
    )]
    AlreadyInitialized,

    #[error("storage format mismatch: {0}")]
    StorageFormatMismatch(String),

    #[error("unsupported profile manifest version {found}; this build reads version 1")]
    UnsupportedProfileManifest { found: u32 },

    #[error("invalid profile manifest: {0}")]
    ProfileManifestInvalid(String),

    #[error("profile floor {floor} is not met: {reason}")]
    ProfileFloorNotMet { floor: &'static str, reason: String },

    #[error(
        "database metadata key {key} is unreadable ({reason}); metadata is encrypted or corrupt; supply OperationalConfig.encryption"
    )]
    MetadataUnreadable { key: &'static str, reason: String },

    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("runtime initialization: {0}")]
    RuntimeInitialization(String),

    #[error("metadata serialization: {0}")]
    Serde(String),
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum CatalogEntry {
    Table {
        indexes: Vec<RelIndexSpec>,
        #[serde(default)]
        layout: TableLayout,
    },
    Collection {
        collection_id: CollectionId,
        fields: Vec<DocField>,
        indexes: Vec<IndexSpec>,
    },
    Vector {
        collection_id: CollectionId,
        dim: usize,
        metric: VectorMetric,
        hnsw: HnswParams,
        #[serde(default)]
        quantization: QuantizationConfig,
        #[serde(default)]
        disk_ann: DiskAnnConfig,
    },
    FullTextIndex {
        config: FullTextIndexConfig,
    },
    TimeSeries {
        config: TimeSeriesConfig,
    },
    Graph {
        graph_id: GraphId,
    },
    GeoIndex {
        config: GeoIndexConfig,
    },
    ForeignTable {
        spec: ForeignTableSpec,
    },
    MaterializedView {
        spec: MaterializedViewSpec,
        source_schema: TableSchema,
    },
    TemporalTable {
        base_table: String,
        schema: TableSchema,
        retention: TemporalRetention,
    },
    TensorDataset {
        config: crate::tensor::TensorDatasetConfig,
    },
}

#[derive(thiserror::Error, Debug)]
pub enum DbError {
    #[error("config: {0}")]
    Config(#[from] ConfigError),

    #[error("query: {0}")]
    Query(#[source] QueryError),

    #[error("model: {0}")]
    Model(#[from] ModelError),

    #[error("vector: {0}")]
    Vector(#[from] VectorError),

    #[error("text: {0}")]
    Text(#[from] TextError),

    #[error("time-series: {0}")]
    TimeSeries(#[from] TimeSeriesError),

    #[error("graph: {0}")]
    Graph(#[from] GraphError),

    #[error("GraphRAG: {0}")]
    GraphRag(#[from] crate::graphrag::GraphRagError),

    #[error("tensor: {0}")]
    Tensor(#[from] crate::tensor::TensorError),

    #[error("geo: {0}")]
    Geo(#[from] GeoError),

    #[error("extension: {0}")]
    Extension(#[from] ExtensionError),

    #[error("tuning: {0}")]
    Tuning(#[from] TuningError),

    #[error("runtime advisor: {0}")]
    RuntimeAdvisor(#[from] RuntimeAdvisorError),

    #[error("cdc: {0}")]
    Cdc(FeedError),

    #[error("subscription: {0}")]
    Subscription(SubscriptionError),

    #[error("materialized view: {0}")]
    MaterializedView(MaterializedViewError),

    #[error("hook: {0}")]
    Hook(HookError),

    #[error("federation: {0}")]
    Federation(#[from] FederationError),

    #[error("temporal: {0}")]
    Temporal(#[from] TemporalError),

    #[error("continuous: {0}")]
    Continuous(#[from] ContinuousError),

    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("authorization denied: {0}")]
    AuthzDenied(#[from] AuthzError),

    #[error("audit integrity: {0}")]
    AuditIntegrity(String),

    #[error("catalog object already exists: {0}")]
    CatalogObjectExists(String),

    #[error("missing catalog object: {0}")]
    MissingCatalogObject(String),

    #[error("catalog object {name} has wrong kind: expected {expected}, found {found}")]
    CatalogKindMismatch {
        name: String,
        expected: &'static str,
        found: &'static str,
    },

    #[error("invalid catalog object name: {0}")]
    InvalidCatalogName(String),

    #[error("metadata serialization: {0}")]
    Serde(String),

    #[error("transaction aborted: {0}")]
    TransactionAborted(String),
}

impl From<QueryError> for DbError {
    fn from(error: QueryError) -> Self {
        match error {
            QueryError::AuthzDenied(error) => Self::AuthzDenied(error),
            error => Self::Query(error),
        }
    }
}

impl DbError {
    #[must_use]
    pub fn is_conflict(&self) -> bool {
        matches!(
            self,
            Self::Storage(StorageError::Conflict)
                | Self::Tensor(
                    crate::tensor::TensorError::Storage(StorageError::Conflict)
                        | crate::tensor::TensorError::Repl(
                            ReplError::Conflict | ReplError::Storage(StorageError::Conflict)
                        )
                )
                | Self::Repl(ReplError::Conflict | ReplError::Storage(StorageError::Conflict))
                | Self::Query(
                    QueryError::Storage(StorageError::Conflict)
                        | QueryError::Repl(
                            ReplError::Conflict | ReplError::Storage(StorageError::Conflict),
                        ),
                )
                | Self::Model(
                    ModelError::Storage(StorageError::Conflict)
                        | ModelError::Repl(
                            ReplError::Conflict | ReplError::Storage(StorageError::Conflict),
                        ),
                )
                | Self::Vector(
                    VectorError::Storage(StorageError::Conflict)
                        | VectorError::Repl(
                            ReplError::Conflict | ReplError::Storage(StorageError::Conflict),
                        ),
                )
                | Self::Text(
                    TextError::Storage(StorageError::Conflict)
                        | TextError::Repl(
                            ReplError::Conflict | ReplError::Storage(StorageError::Conflict),
                        ),
                )
                | Self::TimeSeries(
                    TimeSeriesError::Storage(StorageError::Conflict)
                        | TimeSeriesError::Repl(
                            ReplError::Conflict | ReplError::Storage(StorageError::Conflict),
                        ),
                )
                | Self::Graph(
                    GraphError::Storage(StorageError::Conflict)
                        | GraphError::Repl(
                            ReplError::Conflict | ReplError::Storage(StorageError::Conflict),
                        ),
                )
                | Self::Geo(
                    GeoError::Storage(StorageError::Conflict)
                        | GeoError::Repl(
                            ReplError::Conflict | ReplError::Storage(StorageError::Conflict),
                        ),
                )
        )
    }
}

pub struct Database {
    config: DbConfig,
    repl: DatabaseRepl,
    catalog: BTreeMap<String, CatalogEntry>,
    authz: AuthzPolicy,
    principals: PrincipalRegistry,
    audit_enabled: bool,
    audit_config: AuditConfig,
    audit_lock: Arc<Mutex<()>>,
    audit_batch: Arc<audit_batch::AuditBatchCoordinator>,
    history_pins: Arc<Mutex<txn::ActiveSnapshotRegistry>>,
    encryption: Option<EncryptionConfig>,
    plan_cache: Arc<Mutex<PlanCache>>,
    sql_catalog_cache: sql_cache::SqlCatalogCache,
    performance: PerformanceConfig,
    performance_cache: PerformanceCache,
    query_permits: Arc<Semaphore>,
    /// The query governor knobs `override_query_governor` set for this handle: their limits are
    /// the caller's, not the profile's.
    query_governor_overrides: QueryGovernorOverrides,
    tenant: Option<TenantRuntime>,
    wasm_runtime: Arc<WasmRuntime>,
    vector_indexes: Arc<Mutex<BTreeMap<CollectionId, SharedVectorIndex>>>,
    workload: WorkloadBuffer,
}

/// The query governor knobs `Database::override_query_governor` set for a handle, one flag per
/// knob: a set knob is the caller's for the process, and a restart sets it again; an unset one
/// is the profile's.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct QueryGovernorOverrides {
    pub(crate) max_concurrent_queries: bool,
    pub(crate) queue_timeout_ms: bool,
}

pub type DatabaseSelfHealingController =
    SelfHealingController<DatabaseHealingBackend, ManualHealthProbe>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShardBackendConfig {
    Local(DbConfig),
    /// Fails closed like [`create_cluster_database`] until the embedded API is
    /// wired to the live `OpenRaft` runtime.
    Cp {
        config: DbConfig,
        cluster: CpClusterConfig,
    },
    Ap {
        config: DbConfig,
        cluster: ApClusterConfig,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardSpec {
    pub id: ShardId,
    pub backend: ShardBackendConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardedDatabaseConfig {
    pub strategy: PartitionStrategy,
    pub global_shard: ShardId,
    pub shards: Vec<ShardSpec>,
}

impl ShardSpec {
    #[must_use]
    pub const fn new(id: ShardId, backend: ShardBackendConfig) -> Self {
        Self { id, backend }
    }
}

impl ShardedDatabaseConfig {
    #[must_use]
    pub fn new(strategy: PartitionStrategy, global_shard: ShardId, shards: Vec<ShardSpec>) -> Self {
        Self {
            strategy,
            global_shard,
            shards,
        }
    }
}

#[derive(Clone)]
pub enum DatabaseHealingBackend {
    Cp(Arc<CpRaft<AnyEngine>>),
    Ap(Arc<ApDynamo<AnyEngine>>),
}

enum DatabaseRepl {
    SingleNode(Arc<SingleNode<AnyEngine>>),
    CpRaft(Arc<CpRaft<AnyEngine>>),
    ApDynamo(Arc<ApDynamo<AnyEngine>>),
    Sharded(Arc<ShardedReplication>),
}

impl DatabaseRepl {
    fn storage(&self) -> Option<&AnyEngine> {
        match self {
            Self::SingleNode(repl) => Some(repl.storage()),
            Self::CpRaft(repl) => Some(repl.storage()),
            Self::ApDynamo(repl) => Some(repl.storage()),
            Self::Sharded(_) => None,
        }
    }

    fn engine_kind(&self) -> EngineKind {
        self.storage().map_or(EngineKind::Sharded, AnyEngine::kind)
    }

    fn replication_handle(&self) -> Arc<dyn Replication> {
        match self {
            Self::SingleNode(repl) => {
                let repl: Arc<dyn Replication> = repl.clone();
                repl
            }
            Self::CpRaft(repl) => {
                let repl: Arc<dyn Replication> = repl.clone();
                repl
            }
            Self::ApDynamo(repl) => {
                let repl: Arc<dyn Replication> = repl.clone();
                repl
            }
            Self::Sharded(repl) => {
                let repl: Arc<dyn Replication> = repl.clone();
                repl
            }
        }
    }

    fn replication_ref(&self) -> &dyn Replication {
        match self {
            Self::SingleNode(repl) => repl.as_ref(),
            Self::CpRaft(repl) => repl.as_ref(),
            Self::ApDynamo(repl) => repl.as_ref(),
            Self::Sharded(repl) => repl.as_ref(),
        }
    }

    fn commit_write_set(&self, snapshot_id: TxnId, write_set: WriteSet) -> Result<TxnId, DbError> {
        match self {
            Self::SingleNode(repl) => Ok(repl.commit_write_set(snapshot_id, write_set)?),
            Self::CpRaft(repl) => Ok(repl.commit_write_set(snapshot_id, write_set)?),
            Self::ApDynamo(_) => Err(ConfigError::Unsupported(
                "AP replication does not support snapshot transactions".to_owned(),
            )
            .into()),
            Self::Sharded(_) => Err(ConfigError::Unsupported(
                "sharded replication does not support snapshot transactions".to_owned(),
            )
            .into()),
        }
    }

    fn commit_write_set_with_preflight<'a, F>(
        &'a self,
        snapshot_id: TxnId,
        write_set: WriteSet,
        preflight: F,
    ) -> Result<TxnId, DbError>
    where
        F: FnOnce(&mut <AnyEngine as StorageEngine>::WriteTxn<'a>) -> Result<(), StorageError>,
    {
        match self {
            Self::SingleNode(repl) => {
                Ok(repl.commit_write_set_with_preflight(snapshot_id, write_set, preflight)?)
            }
            Self::CpRaft(repl) => {
                Ok(repl.commit_write_set_with_preflight(snapshot_id, write_set, preflight)?)
            }
            Self::ApDynamo(_) => Err(ConfigError::Unsupported(
                "AP replication does not support snapshot transactions".to_owned(),
            )
            .into()),
            Self::Sharded(_) => Err(ConfigError::Unsupported(
                "sharded replication does not support snapshot transactions".to_owned(),
            )
            .into()),
        }
    }

    fn is_ap(&self) -> bool {
        !self.replication_ref().supports_commit_log()
    }

    const fn is_sharded(&self) -> bool {
        matches!(self, Self::Sharded(_))
    }

    fn shard_map(&self) -> Option<ShardMap> {
        match self {
            Self::Sharded(repl) => Some(repl.shard_map()),
            Self::SingleNode(_) | Self::CpRaft(_) | Self::ApDynamo(_) => None,
        }
    }

    fn read_conflict_versions(
        &self,
        table: &str,
        key: &[u8],
    ) -> Result<Vec<VersionedBytes>, DbError> {
        match self {
            Self::ApDynamo(repl) => Ok(repl.read_conflict_versions(table, key)?),
            Self::SingleNode(_) | Self::CpRaft(_) | Self::Sharded(_) => {
                Err(ConfigError::Unsupported(
                    "conflict siblings are only available on AP replication".to_owned(),
                )
                .into())
            }
        }
    }

    fn resolve_conflict(
        &self,
        table: &str,
        key: &[u8],
        value: Bytes,
        parents: Vec<VectorClock>,
    ) -> Result<(), DbError> {
        match self {
            Self::ApDynamo(repl) => Ok(repl.resolve_conflict(table, key, value, parents)?),
            Self::SingleNode(_) | Self::CpRaft(_) | Self::Sharded(_) => {
                Err(ConfigError::Unsupported(
                    "conflict resolution is only available on AP replication".to_owned(),
                )
                .into())
            }
        }
    }

    fn healing_backend(&self) -> Result<DatabaseHealingBackend, DbError> {
        match self {
            Self::CpRaft(repl) => Ok(DatabaseHealingBackend::Cp(repl.clone())),
            Self::ApDynamo(repl) => Ok(DatabaseHealingBackend::Ap(repl.clone())),
            Self::SingleNode(_) | Self::Sharded(_) => Err(ConfigError::Unsupported(
                "self-healing requires CP or AP replication".to_owned(),
            )
            .into()),
        }
    }
}

impl HealingBackend for DatabaseHealingBackend {
    fn mode(&self) -> HealingMode {
        match self {
            Self::Cp(repl) => repl.mode(),
            Self::Ap(repl) => repl.mode(),
        }
    }

    fn local_node_id(&self) -> NodeId {
        match self {
            Self::Cp(repl) => repl.local_node_id(),
            Self::Ap(repl) => repl.local_node_id(),
        }
    }

    fn known_nodes(&self) -> Vec<NodeId> {
        match self {
            Self::Cp(repl) => repl.known_nodes(),
            Self::Ap(repl) => repl.known_nodes(),
        }
    }

    fn has_write_quorum(&self, health: &HealthView) -> bool {
        match self {
            Self::Cp(repl) => repl.has_write_quorum(health),
            Self::Ap(repl) => repl.has_write_quorum(health),
        }
    }

    fn evict_dead_node(&self, node: NodeId) -> Result<(), ReplError> {
        match self {
            Self::Cp(repl) => repl.evict_dead_node(node),
            Self::Ap(repl) => repl.evict_dead_node(node),
        }
    }

    fn add_replacement_learner(&self, node: RaftNode) -> Result<(), ReplError> {
        match self {
            Self::Cp(repl) => repl.add_replacement_learner(node),
            Self::Ap(repl) => repl.add_replacement_learner(node),
        }
    }

    fn resync_node(&self, node: NodeId) -> Result<(), ReplError> {
        match self {
            Self::Cp(repl) => repl.resync_node(node),
            Self::Ap(repl) => repl.resync_node(node),
        }
    }

    fn promote_learner(&self, node: NodeId) -> Result<(), ReplError> {
        match self {
            Self::Cp(repl) => repl.promote_learner(node),
            Self::Ap(repl) => repl.promote_learner(node),
        }
    }

    fn deliver_ap_hints(&self, node: NodeId) -> Result<(), ReplError> {
        match self {
            Self::Cp(repl) => repl.deliver_ap_hints(node),
            Self::Ap(repl) => repl.deliver_ap_hints(node),
        }
    }

    fn run_ap_anti_entropy(&self, node: NodeId) -> Result<(), ReplError> {
        match self {
            Self::Cp(repl) => repl.run_ap_anti_entropy(node),
            Self::Ap(repl) => repl.run_ap_anti_entropy(node),
        }
    }

    fn record_healing_action(
        &self,
        action: &HealingAction,
        at: std::time::SystemTime,
    ) -> Result<(), ReplError> {
        match self {
            Self::Cp(repl) => repl.record_healing_action(action, at),
            Self::Ap(repl) => repl.record_healing_action(action, at),
        }
    }
}

impl Replication for DatabaseRepl {
    fn propose_plain_document_lww(&self, collection_id: u32, op: Op) -> Result<(), ReplError> {
        self.replication_ref()
            .propose_plain_document_lww(collection_id, op)
    }
    fn registered_keyspaces(&self) -> Result<Vec<String>, ReplError> {
        self.replication_ref().registered_keyspaces()
    }
    fn read_many(
        &self,
        table: &str,
        keys: &[Bytes],
        consistency: ReadConsistency,
    ) -> Result<Vec<Option<Bytes>>, ReplError> {
        tracing::trace!(
            table,
            key_count = keys.len(),
            ?consistency,
            "replication batch read"
        );
        let result = self.replication_ref().read_many(table, keys, consistency);
        observability::record_replication_operation(
            "read_many",
            if result.is_ok() { "ok" } else { "error" },
        );
        if let Err(error) = &result {
            observability::record_replication_error(error.metric_kind());
        }
        result
    }

    fn table_data_version(&self, table: &str) -> Result<Option<u64>, ReplError> {
        self.replication_ref().table_data_version(table)
    }

    fn supports_bounded_scans(&self) -> bool {
        self.replication_ref().supports_bounded_scans()
    }

    fn supports_commit_log(&self) -> bool {
        self.replication_ref().supports_commit_log()
    }

    fn propose(&self, op: Op) -> Result<(), ReplError> {
        let result = self.replication_ref().propose(op);
        observability::record_replication_operation(
            "propose",
            if result.is_ok() { "ok" } else { "error" },
        );
        if let Err(error) = &result {
            observability::record_replication_error(error.metric_kind());
        }
        result
    }

    fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
        let result = self.replication_ref().propose_batch(ops);
        observability::record_replication_operation(
            "propose_batch",
            if result.is_ok() { "ok" } else { "error" },
        );
        if let Err(error) = &result {
            observability::record_replication_error(error.metric_kind());
        }
        result
    }

    fn propose_authorized_batch(
        &self,
        ops: Vec<Op>,
        authorization: txn::WriteAuthorization,
    ) -> Result<(), ReplError> {
        let result = self
            .replication_ref()
            .propose_authorized_batch(ops, authorization);
        observability::record_replication_operation(
            "propose_authorized_batch",
            if result.is_ok() { "ok" } else { "error" },
        );
        if let Err(error) = &result {
            observability::record_replication_error(error.metric_kind());
        }
        result
    }

    fn propose_conditional_batch(&self, batch: ConditionalBatch) -> Result<(), ReplError> {
        let result = self.replication_ref().propose_conditional_batch(batch);
        observability::record_replication_operation(
            "propose_conditional_batch",
            if result.is_ok() { "ok" } else { "error" },
        );
        if let Err(error) = &result {
            observability::record_replication_error(error.metric_kind());
        }
        result
    }

    fn propose_authorized_conditional_batch(
        &self,
        batch: ConditionalBatch,
        authorization: txn::WriteAuthorization,
    ) -> Result<(), ReplError> {
        self.replication_ref()
            .propose_authorized_conditional_batch(batch, authorization)
    }

    fn read(
        &self,
        table: &str,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Option<Bytes>, ReplError> {
        tracing::trace!(table, key_len = key.len(), ?consistency, "replication read");
        let result = self.replication_ref().read(table, key, consistency);
        observability::record_replication_operation(
            "read",
            if result.is_ok() { "ok" } else { "error" },
        );
        if let Err(error) = &result {
            observability::record_replication_error(error.metric_kind());
        }
        result
    }

    fn range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        tracing::trace!(
            table,
            start_len = start.len(),
            end_len = end.len(),
            ?consistency,
            "replication range"
        );
        let result = self.replication_ref().range(table, start, end, consistency);
        observability::record_replication_operation(
            "range",
            if result.is_ok() { "ok" } else { "error" },
        );
        if let Err(error) = &result {
            observability::record_replication_error(error.metric_kind());
        }
        result
    }

    fn scan_range_batches(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
        batch_rows: usize,
        cancelled: &dyn Fn() -> bool,
        on_batch: &mut dyn FnMut(&[(Bytes, Bytes)]) -> Result<bool, ReplError>,
    ) -> Result<(), ReplError> {
        tracing::trace!(
            table,
            start_len = start.len(),
            end_len = end.len(),
            ?consistency,
            batch_rows,
            "replication batch range"
        );
        let result = self.replication_ref().scan_range_batches(
            table,
            start,
            end,
            consistency,
            batch_rows,
            cancelled,
            on_batch,
        );
        observability::record_replication_operation(
            "scan_range_batches",
            if result.is_ok() { "ok" } else { "error" },
        );
        if let Err(error) = &result {
            observability::record_replication_error(error.metric_kind());
        }
        result
    }
}

impl Database {
    #[must_use]
    pub const fn config(&self) -> &DbConfig {
        &self.config
    }

    #[must_use]
    pub const fn profile(&self) -> Profile {
        self.config.profile
    }

    #[must_use]
    pub const fn replication_kind(&self) -> ReplicationKind {
        self.config.replication
    }

    #[must_use]
    pub const fn layout(&self) -> TableLayout {
        layout_for(self.config.profile)
    }

    #[must_use]
    pub fn engine_kind(&self) -> EngineKind {
        self.repl.engine_kind()
    }

    #[must_use]
    pub fn shard_map(&self) -> Option<ShardMap> {
        self.repl.shard_map()
    }

    pub(crate) fn local_storage(&self) -> Option<&AnyEngine> {
        self.repl.storage()
    }

    /// Proposes a crate-internal conditional maintenance batch.
    ///
    /// This is the conditional counterpart of the system batch path: it runs no
    /// user WASM triggers, CDC hooks, or tenant quota accounting. Conditions and
    /// ops must still target public keyspaces.
    pub(crate) fn propose_maintenance_conditional_batch(
        &self,
        batch: ConditionalBatch,
    ) -> Result<(), ReplError> {
        self.repl.propose_conditional_batch(batch)
    }

    pub(crate) fn uses_ap_replication(&self) -> bool {
        self.repl.is_ap()
    }

    fn ensure_commit_log_available(&self, feature: &str) -> Result<(), ConfigError> {
        if self.repl.is_ap() {
            return Err(ConfigError::Unsupported(format!(
                "AP replication does not support {feature}: AP writes are not recorded in the commit log"
            )));
        }
        Ok(())
    }

    #[must_use]
    pub const fn performance_config(&self) -> &PerformanceConfig {
        &self.performance
    }

    /// Sets the query governor: at most `max_concurrent_queries` statements run
    /// at once, and a statement that finds no free slot waits up to
    /// `queue_timeout_ms` before it fails with [`QueryError::Busy`]
    /// (SQLSTATE 53200).
    ///
    /// It takes `&mut self`, so no statement holds a slot while the limits change.
    /// The limits belong to this handle: they are never written to the database,
    /// and provisioning plans do not report them as a session pending a restart.
    /// [`Database::override_query_governor`] sets one limit and keeps the other.
    /// # Errors
    /// Fails when either limit is zero or `max_concurrent_queries` is more than
    /// the governor can count.
    pub fn set_query_governor(
        &mut self,
        max_concurrent_queries: usize,
        queue_timeout_ms: u64,
    ) -> Result<(), ConfigError> {
        self.override_query_governor(Some(max_concurrent_queries), Some(queue_timeout_ms))
    }

    /// Sets the query governor limits that are `Some`, as [`Database::set_query_governor`]
    /// does, and keeps the others; `None` for both changes nothing. `multidb serve` passes
    /// `MULTIDB_MAX_CONCURRENT_QUERIES` and `MULTIDB_QUERY_QUEUE_TIMEOUT_MS`, each when it is
    /// set.
    ///
    /// A limit set here belongs to this handle: it is never written to the database, and
    /// provisioning plans do not report it as a session pending a restart. A limit never set
    /// here stays the profile's: plans compare it with the profile manifest, so a retune of it
    /// waits for the next open.
    /// # Errors
    /// Fails, and changes nothing, when a given limit is zero or `max_concurrent_queries` is
    /// more than the governor can count.
    pub fn override_query_governor(
        &mut self,
        max_concurrent_queries: Option<usize>,
        queue_timeout_ms: Option<u64>,
    ) -> Result<(), ConfigError> {
        if let Some(max) = max_concurrent_queries
            && (max == 0 || max > Semaphore::MAX_PERMITS)
        {
            return Err(ConfigError::Unsupported(format!(
                "query governor max concurrent queries must be 1 to {}, got {max}",
                Semaphore::MAX_PERMITS
            )));
        }
        if queue_timeout_ms == Some(0) {
            return Err(ConfigError::Unsupported(
                "query governor queue timeout must be at least 1 ms".to_owned(),
            ));
        }
        if max_concurrent_queries.is_none() && queue_timeout_ms.is_none() {
            return Ok(());
        }
        if let Some(max) = max_concurrent_queries {
            self.performance.query.max_concurrent_queries = max;
            self.query_governor_overrides.max_concurrent_queries = true;
        }
        if let Some(timeout) = queue_timeout_ms {
            self.performance.query.queue_timeout_ms = timeout;
            self.query_governor_overrides.queue_timeout_ms = true;
        }
        self.query_permits = query_permits_for(&self.performance);
        self.invalidate_plan_cache();
        Ok(())
    }

    #[must_use]
    pub const fn encryption_config(&self) -> Option<&EncryptionConfig> {
        self.encryption.as_ref()
    }

    /// Rotates the current data encryption key for an envelope-encrypted database.
    ///
    /// New writes use the new key; existing values keep their old key until
    /// [`Self::reencrypt_existing_values`] rewrites them.
    /// # Errors
    /// Fails when the database is not encrypted or the key provider cannot rotate.
    pub fn rotate_dek(&self) -> Result<u64, DbError> {
        Ok(self
            .local_storage()
            .ok_or_else(|| {
                ConfigError::Unsupported(
                    "replication backend has no local encrypted storage".to_owned(),
                )
            })?
            .rotate_dek()?)
    }

    /// Destroys one data encryption key version for crypto-shredding.
    ///
    /// Refuses the current key, the last live key, and any key that still
    /// protects stored values: call [`Self::rotate_dek`], then
    /// [`Self::reencrypt_existing_values`] until it reports nothing left to
    /// rewrite, then destroy the old key. Backups and other residual copies
    /// encrypted under a destroyed key become permanently unreadable.
    /// # Errors
    /// Fails when the database is not encrypted, the key id is unknown, or the
    /// key is current, last, or still in use.
    pub fn destroy_dek(&self, key_id: u64) -> Result<(), DbError> {
        Ok(self
            .local_storage()
            .ok_or_else(|| {
                ConfigError::Unsupported(
                    "replication backend has no local encrypted storage".to_owned(),
                )
            })?
            .destroy_dek(key_id)?)
    }

    /// Re-encrypts every stored value under the current data encryption key.
    ///
    /// Runs in committed batches of at most `batch_size` values and is safe to
    /// rerun; rerun it until `reencrypted_values` is zero before destroying old
    /// keys. Each batch holds the storage writer and the call is synchronous, so
    /// async callers should use `spawn_blocking`.
    /// # Errors
    /// Fails when the database is not encrypted, `batch_size` is zero, or a
    /// batch cannot be read, decrypted, or committed.
    pub fn reencrypt_existing_values(
        &self,
        batch_size: usize,
    ) -> Result<ReencryptionReport, DbError> {
        Ok(self
            .local_storage()
            .ok_or_else(|| {
                ConfigError::Unsupported(
                    "replication backend has no local encrypted storage".to_owned(),
                )
            })?
            .reencrypt_existing_values(batch_size)?)
    }

    /// Lists live data encryption key versions.
    /// # Errors
    /// Fails when the database is not encrypted or the keyring cannot be read.
    pub fn encryption_key_versions(&self) -> Result<Vec<u64>, DbError> {
        Ok(self
            .local_storage()
            .ok_or_else(|| {
                ConfigError::Unsupported(
                    "replication backend has no local encrypted storage".to_owned(),
                )
            })?
            .list_deks()?)
    }

    #[must_use]
    pub fn performance_cache_metrics(&self) -> crate::performance::CacheMetrics {
        self.performance_cache.metrics()
    }

    #[must_use]
    pub fn tenant_id(&self) -> Option<TenantId> {
        self.tenant.as_ref().map(TenantRuntime::tenant_id)
    }

    /// Builds a self-healing controller with a mutable manual probe.
    /// # Errors
    /// Fails when the database is not cluster-backed or the health config is invalid.
    pub fn self_healing_controller(
        &self,
        config: HealthConfig,
        policy: HealingPolicy,
    ) -> Result<DatabaseSelfHealingController, DbError> {
        self.self_healing_controller_with_probe(config, policy, ManualHealthProbe::default())
    }

    /// Builds a self-healing controller with a custom health probe.
    /// # Errors
    /// Fails when the database is not cluster-backed or the health config is invalid.
    pub fn self_healing_controller_with_probe<P>(
        &self,
        config: HealthConfig,
        policy: HealingPolicy,
        probe: P,
    ) -> Result<SelfHealingController<DatabaseHealingBackend, P>, DbError>
    where
        P: HealthProbe,
    {
        Ok(SelfHealingController::new(
            self.repl.healing_backend()?,
            probe,
            config,
            policy,
        )?)
    }

    #[must_use]
    pub fn catalog(&self) -> &BTreeMap<String, CatalogEntry> {
        &self.catalog
    }

    /// Probes whether the database can serve catalog-backed requests.
    /// # Errors
    /// Fails when the active replication/storage layer cannot satisfy a strong catalog read.
    pub fn readiness_probe(&self) -> Result<(), DbError> {
        self.repl
            .replication_ref()
            .range(CATALOG_TABLE, &[], &[0xFF], ReadConsistency::Strong)?;
        Ok(())
    }

    #[must_use]
    pub const fn authz_policy(&self) -> &AuthzPolicy {
        &self.authz
    }

    #[must_use]
    pub const fn principal_registry(&self) -> &PrincipalRegistry {
        &self.principals
    }

    #[must_use]
    pub const fn audit_enabled(&self) -> bool {
        self.audit_enabled
    }

    #[must_use]
    pub fn principal_for_user(&self, pg_user: &str) -> Principal {
        self.principals.principal_for_user(pg_user)
    }

    /// Reads the persisted admin password credential for a username.
    /// # Errors
    /// Fails when the internal auth keyspace cannot be read or decoded.
    pub fn admin_credential(
        &self,
        username: &str,
    ) -> Result<Option<AdminCredentialRecord>, DbError> {
        self.repl
            .read(
                ADMIN_AUTH_TABLE,
                username.as_bytes(),
                ReadConsistency::Strong,
            )?
            .map(|bytes| {
                serde_json::from_slice(&bytes).map_err(|error| DbError::Serde(error.to_string()))
            })
            .transpose()
    }

    /// Persists one admin password credential in an internal system keyspace.
    /// # Errors
    /// Fails when the credential cannot be serialized or persisted.
    pub fn set_admin_credential(&self, credential: &AdminCredentialRecord) -> Result<(), DbError> {
        let value =
            serde_json::to_vec(credential).map_err(|error| DbError::Serde(error.to_string()))?;
        propose_system_batch(
            self.replication_ref(),
            vec![Op::Put {
                table: ADMIN_AUTH_TABLE.to_owned(),
                key: credential.username.as_bytes().to_vec(),
                value,
            }],
        )?;
        Ok(())
    }

    /// Replaces a credential only while the verified credential is still current.
    /// `None` creates an account only if no other initializer has created it.
    /// # Errors
    /// Fails with a conflict on concurrent rotation, or on metadata/storage errors.
    pub fn compare_and_set_admin_credential(
        &self,
        expected: Option<&AdminCredentialRecord>,
        credential: &AdminCredentialRecord,
    ) -> Result<(), DbError> {
        let key = credential.username.as_bytes().to_vec();
        let current_bytes =
            self.replication_ref()
                .read(ADMIN_AUTH_TABLE, &key, ReadConsistency::Strong)?;
        let current: Option<AdminCredentialRecord> = current_bytes
            .as_ref()
            .map(|bytes| {
                serde_json::from_slice(bytes).map_err(|error| DbError::Serde(error.to_string()))
            })
            .transpose()?;
        if current.as_ref() != expected {
            return Err(ReplError::Conflict.into());
        }
        let value =
            serde_json::to_vec(credential).map_err(|error| DbError::Serde(error.to_string()))?;
        crate::repl::propose_system_conditional_batch(
            self.replication_ref(),
            ConditionalBatch::new(
                vec![WriteCondition::ValueEquals {
                    table: ADMIN_AUTH_TABLE.to_owned(),
                    key: key.clone(),
                    expected: current_bytes,
                }],
                vec![Op::Put {
                    table: ADMIN_AUTH_TABLE.to_owned(),
                    key,
                    value,
                }],
            ),
        )?;
        Ok(())
    }

    pub fn set_authz_policy(&mut self, policy: AuthzPolicy) {
        self.authz = policy;
    }

    pub fn set_principal_registry(&mut self, registry: PrincipalRegistry) {
        self.principals = registry;
    }

    /// Reports whether `ensure_bootstrap_admin_principal` would leave the
    /// security state unchanged, so that callers can skip it and the exclusive
    /// access it needs.
    #[must_use]
    pub(crate) fn has_bootstrap_admin_principal(&self) -> bool {
        let (role, principal) = self.bootstrap_admin_updates();
        role.is_none() && principal.is_none()
    }

    /// The bootstrap admin role and principal merged into the loaded security
    /// state, keeping grants and roles they already hold. Each is `None` when
    /// the merge would not change it.
    fn bootstrap_admin_updates(&self) -> (Option<Role>, Option<Principal>) {
        let (bootstrap_role, bootstrap_principal) = bootstrap_admin_identity();
        let current_role = self.authz.roles().get(bootstrap_role.name());
        let mut role = current_role
            .cloned()
            .unwrap_or_else(|| Role::new(bootstrap_role.name()));
        for (resource, permissions) in bootstrap_role.grants() {
            for permission in permissions {
                role = role.grant(resource.clone(), *permission);
            }
        }
        let current_principal = self.principals.principals().get(bootstrap_principal.name());
        let principal = current_principal
            .cloned()
            .unwrap_or_else(|| Principal::new(bootstrap_principal.name()))
            .with_role(bootstrap_role.name());
        (
            (current_role != Some(&role)).then_some(role),
            (current_principal != Some(&principal)).then_some(principal),
        )
    }

    /// Ensures the bootstrap admin identity can use password-backed sessions.
    ///
    /// Merges the bootstrap admin role and principal into the loaded security
    /// state without replacing other roles, mappings or the audit flag; persists
    /// only when something was missing.
    /// # Errors
    /// Fails when the updated security metadata cannot be persisted.
    pub fn ensure_bootstrap_admin_principal(&mut self) -> Result<(), DbError> {
        let (role, principal) = self.bootstrap_admin_updates();
        if role.is_none() && principal.is_none() {
            return Ok(());
        }
        if let Some(role) = role {
            self.authz = self.authz.clone().allow(role);
        }
        if let Some(principal) = principal {
            self.principals
                .insert(principal.name().to_owned(), principal);
        }
        self.persist_security_state()
    }

    /// Records an admin authentication audit event without exposing secrets.
    /// # Errors
    /// Fails when the audit log cannot be written.
    pub fn record_admin_auth_event(
        &self,
        principal: Option<&Principal>,
        action: &str,
        outcome: AuditOutcome,
        detail: Option<&str>,
    ) -> Result<(), DbError> {
        self.record_audit(&AuditEvent::new(
            principal,
            action,
            Resource::System,
            outcome,
            detail,
        ))
    }

    /// Confirms a configuration apply plan after checking system admin permission.
    ///
    /// This is an audited control-plane confirmation only. It never mutates user
    /// data, catalog state, indexes, extensions, or runtime configuration. The audit
    /// event records `plan_source: client`: the caller supplied the plan.
    /// # Errors
    /// Fails when authorization fails, audit is disabled, or the audit write fails.
    pub fn confirm_config_apply_as(
        &self,
        principal: &Principal,
        plan: &MigrationPlan,
        confirm: &str,
    ) -> Result<ApplyCheckReport, DbError> {
        self.confirm_config_apply_with_source_as(principal, plan, confirm, ConfigPlanSource::Client)
    }

    /// [`Database::confirm_config_apply_as`], recording where the plan comes from: a plan the
    /// client sent, or one the Control Plane recomputed from `current` and `desired` specs.
    /// # Errors
    /// Fails when authorization fails, audit is disabled, or the audit write fails.
    pub(crate) fn confirm_config_apply_with_source_as(
        &self,
        principal: &Principal,
        plan: &MigrationPlan,
        confirm: &str,
        source: ConfigPlanSource,
    ) -> Result<ApplyCheckReport, DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::System,
            Permission::Admin,
            "config_apply",
        )?;
        if !self.audit_enabled {
            return Err(DbError::AuditIntegrity(
                "config_apply requires audit to be enabled".to_owned(),
            ));
        }

        let mut report = MigrationPlanner::check_apply(plan, confirm);
        let outcome = if report.status == ApplyStatus::Confirmed {
            AuditOutcome::Succeeded
        } else {
            AuditOutcome::Failed
        };
        // A client plan id is not checked, so only its start is recorded: the detail is cut at
        // 160 characters, and `plan_source` must survive a forged, overlong id.
        let plan_id = report
            .plan_id
            .chars()
            .take(CONFIG_APPLY_DETAIL_PLAN_ID_CHARS)
            .collect::<String>();
        let detail = format!(
            "plan_id: {plan_id}; status: {:?}; confirmation_matched: {}; data_mutated: false; \
             plan_source: {}",
            report.status,
            report.confirmation_matched,
            source.slug()
        );
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "config_apply",
            Resource::System,
            outcome,
            Some(&detail),
        ))?;
        report.audit_recorded = true;
        Ok(report)
    }

    /// Replaces the persisted RBAC policy after checking system admin permission.
    /// # Errors
    /// Fails when authorization or metadata persistence fails.
    pub fn set_authz_policy_as(
        &mut self,
        principal: &Principal,
        policy: AuthzPolicy,
    ) -> Result<(), DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::System,
            Permission::Admin,
            "set_authz_policy",
        )?;
        self.authz = policy;
        self.persist_security_state()?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "set_authz_policy",
            Resource::System,
            AuditOutcome::Succeeded,
            None,
        ))?;
        Ok(())
    }

    /// Replaces the persisted PG-user to principal registry after checking system admin permission.
    /// # Errors
    /// Fails when authorization or metadata persistence fails.
    pub fn set_principal_registry_as(
        &mut self,
        principal: &Principal,
        registry: PrincipalRegistry,
    ) -> Result<(), DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::System,
            Permission::Admin,
            "set_principal_registry",
        )?;
        self.principals = registry;
        self.persist_security_state()?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "set_principal_registry",
            Resource::System,
            AuditOutcome::Succeeded,
            None,
        ))?;
        Ok(())
    }

    /// Enables or disables audit recording after checking system admin permission.
    ///
    /// Disabling audit writes an audit event before the flag is persisted. A database whose
    /// profile manifest sets `floors.audit` keeps audit on: disabling it is refused and audited
    /// as failed.
    /// # Errors
    /// Fails when authorization, audit, or metadata persistence fails, and with
    /// `Config(ProfileFloorNotMet)` when disabling audit would break the `floors.audit` floor.
    pub fn set_audit_enabled_as(
        &mut self,
        principal: &Principal,
        enabled: bool,
    ) -> Result<(), DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::System,
            Permission::Admin,
            "set_audit_enabled",
        )?;
        self.change_audit_enabled(principal, enabled)
    }

    /// The body of [`Database::set_audit_enabled_as`] after authorization, shared with
    /// provisioning's local operators: the `floors.audit` refusal, the disable event before the
    /// flag and the enable event after it.
    fn change_audit_enabled(
        &mut self,
        principal: &Principal,
        enabled: bool,
    ) -> Result<(), DbError> {
        if !enabled {
            // Only single-node openers accept a profile manifest: CP, AP and sharded openers
            // refuse one (`refuse_profile_manifest`), and an AP database keeps `__meta` outside
            // the tables its replication reads, so `__meta` is read for a single node only.
            let audit_floor = match &self.repl {
                DatabaseRepl::SingleNode(repl) => read_metadata_from_repl(repl.as_ref())?
                    .manifest
                    .is_some_and(|manifest| manifest.knobs.floors.audit),
                DatabaseRepl::CpRaft(_) | DatabaseRepl::ApDynamo(_) | DatabaseRepl::Sharded(_) => {
                    false
                }
            };
            if audit_floor {
                self.record_audit(&AuditEvent::new(
                    Some(principal),
                    "set_audit_enabled",
                    Resource::System,
                    AuditOutcome::Failed,
                    Some("enabled: false; refused by profile floor floors.audit"),
                ))?;
                return Err(DbError::Config(ConfigError::ProfileFloorNotMet {
                    floor: "floors.audit",
                    reason: "the profile manifest of this database requires audit, so it cannot \
                             be disabled"
                        .to_owned(),
                }));
            }
            self.record_audit(&AuditEvent::new(
                Some(principal),
                "set_audit_enabled",
                Resource::System,
                AuditOutcome::Succeeded,
                Some("enabled: false"),
            ))?;
            self.audit_enabled = false;
            self.persist_security_state()?;
            return Ok(());
        }

        self.audit_enabled = true;
        self.persist_security_state()?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "set_audit_enabled",
            Resource::System,
            AuditOutcome::Succeeded,
            Some("enabled: true"),
        ))?;
        Ok(())
    }

    /// Creates a table after checking `Admin` on the database.
    /// # Errors
    /// Fails when authorization or table creation fails.
    pub fn create_table_as(
        &mut self,
        principal: &Principal,
        name: impl Into<String>,
        schema: Option<TableSchema>,
        indexes: Vec<RelIndexSpec>,
    ) -> Result<RelTable, DbError> {
        let name = name.into();
        self.authorize_or_audit(
            principal,
            &Resource::Database,
            Permission::Admin,
            "create_table",
        )?;
        let table = self.create_table(name.clone(), schema, indexes)?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "create_table",
            Resource::Table(name),
            AuditOutcome::Succeeded,
            None,
        ))?;
        Ok(table)
    }

    /// Creates a foreign table after checking database admin permission.
    /// # Errors
    /// Fails when authorization, source validation, or catalog persistence fails.
    pub fn create_foreign_table_as(
        &mut self,
        principal: &Principal,
        name: impl Into<String>,
        schema: TableSchema,
        source: ForeignSource,
        options: ForeignTableOptions,
    ) -> Result<(), DbError> {
        let name = name.into();
        self.authorize_or_audit(
            principal,
            &Resource::Database,
            Permission::Admin,
            "create_foreign_table",
        )?;
        self.create_foreign_table(name.clone(), schema, source, options)?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "create_foreign_table",
            Resource::Table(name),
            AuditOutcome::Succeeded,
            None,
        ))?;
        Ok(())
    }

    /// Enables system versioning after checking table admin permission.
    /// # Errors
    /// Fails when authorization, base table lookup, or catalog persistence fails.
    pub fn enable_system_versioning_as(
        &mut self,
        principal: &Principal,
        table: &str,
        retention: TemporalRetention,
    ) -> Result<String, DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::Table(table.to_owned()),
            Permission::Admin,
            "enable_system_versioning",
        )?;
        let history_name = self.enable_system_versioning(table, retention)?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "enable_system_versioning",
            Resource::Table(table.to_owned()),
            AuditOutcome::Succeeded,
            Some(&format!("history: {history_name}")),
        ))?;
        Ok(history_name)
    }

    /// Creates a collection after checking `Admin` on the database.
    /// # Errors
    /// Fails when authorization or collection creation fails.
    pub fn create_collection_as(
        &mut self,
        principal: &Principal,
        name: impl Into<String>,
        collection_id: CollectionId,
        fields: Vec<DocField>,
        indexes: Vec<IndexSpec>,
    ) -> Result<DocumentCollection<'_, dyn Replication + '_>, DbError> {
        let name = name.into();
        let returned_indexes = indexes.clone();
        self.authorize_or_audit(
            principal,
            &Resource::Database,
            Permission::Admin,
            "create_collection",
        )?;
        self.create_collection(name.clone(), collection_id, fields, indexes)?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "create_collection",
            Resource::Collection(name),
            AuditOutcome::Succeeded,
            None,
        ))?;

        let derived = self.derived_indexes_for(collection_id);
        let repl: &dyn Replication = self;
        Ok(
            DocumentCollection::with_indexes(repl, collection_id, returned_indexes)
                .with_derived_indexes(derived),
        )
    }

    /// Creates a vector collection after checking `Admin` on the database.
    /// # Errors
    /// Fails when authorization or vector collection creation fails.
    pub fn create_vector_collection_as(
        &mut self,
        principal: &Principal,
        name: impl Into<String>,
        config: VectorCollectionConfig,
    ) -> Result<VectorCollection<'_, dyn Replication + '_>, DbError> {
        let name = name.into();
        let returned_config = config.clone();
        self.authorize_or_audit(
            principal,
            &Resource::Database,
            Permission::Admin,
            "create_vector_collection",
        )?;
        self.create_vector_collection(name.clone(), config)?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "create_vector_collection",
            Resource::VectorCollection(name),
            AuditOutcome::Succeeded,
            None,
        ))?;
        self.open_vector_collection_with_config(returned_config)
    }

    /// Creates a full-text index after checking `Admin` on the database.
    ///
    /// Existing documents are backfilled and later document writes keep the index current
    /// (see `create_full_text_index`).
    /// # Errors
    /// Fails when authorization, catalog validation, the backfill, or index metadata
    /// persistence fails, including when the source is not a document collection.
    pub fn create_full_text_index_as(
        &mut self,
        principal: &Principal,
        config: &FullTextIndexConfig,
    ) -> Result<FullTextIndex<'_, dyn Replication + '_>, DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::Database,
            Permission::Admin,
            "create_full_text_index",
        )?;
        let name = config.name.clone();
        self.create_full_text_index(config.clone())?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "create_full_text_index",
            Resource::FullTextIndex(name.clone()),
            AuditOutcome::Succeeded,
            None,
        ))?;
        self.full_text_index(&config.name)
    }

    /// Creates a time-series collection after checking `Admin` on the database.
    /// # Errors
    /// Fails when authorization or catalog persistence fails.
    pub fn create_time_series_as(
        &mut self,
        principal: &Principal,
        config: &TimeSeriesConfig,
    ) -> Result<TimeSeriesCollection<'_, dyn Replication + '_>, DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::Database,
            Permission::Admin,
            "create_time_series",
        )?;
        let name = config.name.clone();
        self.create_time_series(config.clone())?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "create_time_series",
            Resource::TimeSeries(name.clone()),
            AuditOutcome::Succeeded,
            None,
        ))?;
        self.time_series(&config.name)
    }

    /// Creates a graph after checking `Admin` on the database.
    /// # Errors
    /// Fails when authorization or catalog persistence fails.
    pub fn create_graph_as(
        &mut self,
        principal: &Principal,
        name: impl Into<String>,
        graph_id: GraphId,
    ) -> Result<Graph<'_, dyn Replication + '_>, DbError> {
        let name = name.into();
        self.authorize_or_audit(
            principal,
            &Resource::Database,
            Permission::Admin,
            "create_graph",
        )?;
        self.create_graph(name.clone(), graph_id)?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "create_graph",
            Resource::Graph(name.clone()),
            AuditOutcome::Succeeded,
            None,
        ))?;
        self.graph(&name)
    }

    /// Creates a geo index after checking `Admin` on the database.
    ///
    /// Existing documents are backfilled and later document writes keep the index current
    /// (see `create_geo_index`).
    /// # Errors
    /// Fails when authorization, catalog validation, the backfill, or index validation
    /// fails, including when the source is not a document collection.
    pub fn create_geo_index_as(
        &mut self,
        principal: &Principal,
        config: &GeoIndexConfig,
    ) -> Result<GeoIndex<'_, dyn Replication + '_>, DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::Database,
            Permission::Admin,
            "create_geo_index",
        )?;
        let name = config.name.clone();
        self.create_geo_index(config.clone())?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "create_geo_index",
            Resource::GeoIndex(name.clone()),
            AuditOutcome::Succeeded,
            None,
        ))?;
        self.geo_index(&config.name)
    }

    /// Creates a named relational table and persists it in the catalog.
    /// # Errors
    /// Fails when the name collides, schema is invalid, or storage rejects metadata.
    pub fn create_table(
        &mut self,
        name: impl Into<String>,
        schema: Option<TableSchema>,
        indexes: Vec<RelIndexSpec>,
    ) -> Result<RelTable, DbError> {
        let layout = self.layout();
        self.create_table_with_layout(name, schema, indexes, layout)
    }

    /// Creates a foreign table backed by a local object-store file or remote PG-compatible source.
    /// # Errors
    /// Fails when the catalog name, schema, source, or metadata write is invalid.
    pub fn create_foreign_table(
        &mut self,
        name: impl Into<String>,
        schema: TableSchema,
        source: ForeignSource,
        options: ForeignTableOptions,
    ) -> Result<(), DbError> {
        self.ensure_non_tensor_model_allowed("create_foreign_table")?;
        let name = name.into();
        self.ensure_name_available(&name)?;
        validate_foreign_source(&source)?;
        let spec = ForeignTableSpec {
            schema,
            source,
            options,
            stats: ForeignTableStats::default(),
        };
        let entry = CatalogEntry::ForeignTable { spec };
        propose_system_batch(&self.repl, vec![catalog_put_op(&name, &entry)?])?;
        self.catalog.insert(name, entry);
        self.invalidate_plan_cache();
        Ok(())
    }

    /// Creates a foreign table with default scan options.
    /// # Errors
    /// Fails when the catalog name, schema, source, or metadata write is invalid.
    pub fn create_foreign_table_default(
        &mut self,
        name: impl Into<String>,
        schema: TableSchema,
        source: ForeignSource,
    ) -> Result<(), DbError> {
        self.create_foreign_table(name, schema, source, ForeignTableOptions::default())
    }

    /// Enables MVCC-backed system versioning for a table and registers its history object.
    /// # Errors
    /// Fails when the database uses AP replication, the base table is missing, untyped, or the
    /// history name collides, and with `RetentionExpired` when `retention.min_lsn` is below the
    /// history GC horizon.
    pub fn enable_system_versioning(
        &mut self,
        table: &str,
        retention: TemporalRetention,
    ) -> Result<String, DbError> {
        if self.repl.is_ap() {
            return Err(TemporalError::Unsupported(
                "AP replication does not support system versioning: AP writes do not record MVCC \
                 history"
                    .to_owned(),
            )
            .into());
        }
        let base = self.table(table)?;
        let schema = base.schema().cloned().ok_or_else(|| {
            QueryError::InvalidSchema("temporal tables require schema".to_owned())
        })?;
        let history_name = format!("{table}_history");
        self.ensure_name_available(&history_name)?;
        self.ensure_retention_above_history_horizon(retention.min_lsn)?;
        let entry = CatalogEntry::TemporalTable {
            base_table: table.to_owned(),
            schema,
            retention,
        };
        propose_system_batch(&self.repl, vec![catalog_put_op(&history_name, &entry)?])?;
        self.catalog.insert(history_name.clone(), entry);
        self.invalidate_plan_cache();
        Ok(history_name)
    }

    /// Creates a named document collection and persists it in the catalog.
    /// # Errors
    /// Fails when the name collides, fields are invalid, or storage rejects metadata.
    pub fn create_collection(
        &mut self,
        name: impl Into<String>,
        collection_id: CollectionId,
        fields: Vec<DocField>,
        indexes: Vec<IndexSpec>,
    ) -> Result<DocumentCollection<'_, dyn Replication + '_>, DbError> {
        self.ensure_non_tensor_model_allowed("create_collection")?;
        let name = name.into();
        self.ensure_name_available(&name)?;
        self.ensure_collection_id_available(collection_id)?;
        let entry = CatalogEntry::Collection {
            collection_id,
            fields: fields.clone(),
            indexes: indexes.clone(),
        };

        let mut validator = SqlEngine::new(self.replication_handle());
        validator.register_collection(&name, collection_id, fields, indexes.clone())?;
        propose_system_batch(
            &self.repl,
            vec![
                catalog_put_op(&name, &entry)?,
                next_collection_id_op(self.next_collection_id_after(collection_id)?)?,
            ],
        )?;
        self.catalog.insert(name, entry);
        self.invalidate_plan_cache();

        let derived = self.derived_indexes_for(collection_id);
        let repl: &dyn Replication = self;
        Ok(
            DocumentCollection::with_indexes(repl, collection_id, indexes)
                .with_derived_indexes(derived),
        )
    }

    /// Creates a document collection with the next available collection id.
    /// # Errors
    /// Fails when catalog validation or metadata persistence fails.
    pub fn create_collection_auto_id(
        &mut self,
        name: impl Into<String>,
        fields: Vec<DocField>,
        indexes: Vec<IndexSpec>,
    ) -> Result<DocumentCollection<'_, dyn Replication + '_>, DbError> {
        let id = CollectionId::new(self.catalog_next_collection_id());
        self.create_collection(name, id, fields, indexes)
    }

    /// Creates a named vector collection and persists it in the catalog.
    /// # Errors
    /// Fails when the name collides, config is invalid, or storage rejects metadata.
    pub fn create_vector_collection(
        &mut self,
        name: impl Into<String>,
        config: VectorCollectionConfig,
    ) -> Result<VectorCollection<'_, dyn Replication + '_>, DbError> {
        self.ensure_non_tensor_model_allowed("create_vector_collection")?;
        let name = name.into();
        self.ensure_name_available(&name)?;
        self.ensure_collection_id_available(config.collection_id)?;
        {
            let repl: &dyn Replication = self;
            VectorCollection::new(repl, config.clone())?;
        }

        let entry = CatalogEntry::Vector {
            collection_id: config.collection_id,
            dim: config.dim,
            metric: config.metric,
            hnsw: config.hnsw,
            quantization: config.quantization.clone(),
            disk_ann: config.disk_ann.clone(),
        };
        propose_system_batch(
            &self.repl,
            vec![
                catalog_put_op(&name, &entry)?,
                next_collection_id_op(self.next_collection_id_after(config.collection_id)?)?,
            ],
        )?;
        self.catalog.insert(name, entry);
        self.invalidate_plan_cache();

        self.open_vector_collection_with_config(config)
    }

    /// Creates a vector collection with the next available collection id.
    /// # Errors
    /// Fails when catalog validation or metadata persistence fails.
    pub fn create_vector_collection_auto_id(
        &mut self,
        name: impl Into<String>,
        dim: usize,
        metric: VectorMetric,
        hnsw: HnswParams,
    ) -> Result<VectorCollection<'_, dyn Replication + '_>, DbError> {
        let config =
            VectorCollectionConfig::new(CollectionId::new(self.catalog_next_collection_id()), dim)
                .with_metric(metric)
                .with_hnsw(hnsw);
        self.create_vector_collection(name, config)
    }

    /// Creates a named full-text index and persists it in the catalog.
    ///
    /// Documents already in the source collection are backfilled before the index is
    /// published. After that, document writes made through `collection` handles,
    /// transactions and Control Plane document CRUD maintain the index in the same batch.
    /// # Errors
    /// Fails when the name collides, the source is not an existing document collection,
    /// the backfill fails, or storage rejects metadata.
    pub fn create_full_text_index(
        &mut self,
        config: FullTextIndexConfig,
    ) -> Result<FullTextIndex<'_, dyn Replication + '_>, DbError> {
        self.ensure_non_tensor_model_allowed("create_full_text_index")?;
        let name = config.name.clone();
        self.ensure_name_available(&name)?;
        let TextSource::Collection { collection_id, .. } = &config.source;
        self.ensure_document_collection(*collection_id)?;
        // Backfill before the catalog entry becomes visible: `&mut self` keeps in-process
        // writers out until it is published, and every later write through `collection`
        // handles maintains the index itself.
        let state = {
            let repl: &dyn Replication = self;
            FullTextIndex::new(repl, config.clone())?.rebuild_entries()?
        };
        let entry = CatalogEntry::FullTextIndex {
            config: config.clone(),
        };
        let mut ops = FullTextIndex::<dyn Replication>::metadata_ops_with_state(&config, &state)?;
        ops.push(catalog_put_op(&name, &entry)?);
        propose_system_batch(&self.repl, ops)?;
        self.catalog.insert(name, entry);
        self.invalidate_plan_cache();
        let repl: &dyn Replication = self;
        Ok(FullTextIndex::new(repl, config)?.with_commit_log(!self.repl.is_ap()))
    }

    /// Creates a named time-series collection and persists it in the catalog.
    /// # Errors
    /// Fails when the name collides or metadata cannot be written.
    pub fn create_time_series(
        &mut self,
        config: TimeSeriesConfig,
    ) -> Result<TimeSeriesCollection<'_, dyn Replication + '_>, DbError> {
        self.ensure_non_tensor_model_allowed("create_time_series")?;
        let name = config.name.clone();
        self.ensure_name_available(&name)?;
        {
            let repl: &dyn Replication = self;
            TimeSeriesCollection::new(repl, config.clone())?;
        }
        let entry = CatalogEntry::TimeSeries {
            config: config.clone(),
        };
        propose_system_batch(
            &self.repl,
            vec![
                TimeSeriesCollection::<dyn Replication>::metadata_op(&config)?,
                catalog_put_op(&name, &entry)?,
            ],
        )?;
        self.catalog.insert(name, entry);
        self.invalidate_plan_cache();
        let repl: &dyn Replication = self;
        Ok(TimeSeriesCollection::new(repl, config)?)
    }

    /// Creates a named graph and persists it in the catalog.
    /// # Errors
    /// Fails when the name collides or metadata cannot be written.
    pub fn create_graph(
        &mut self,
        name: impl Into<String>,
        graph_id: GraphId,
    ) -> Result<Graph<'_, dyn Replication + '_>, DbError> {
        self.ensure_non_tensor_model_allowed("create_graph")?;
        let name = name.into();
        self.ensure_name_available(&name)?;
        self.ensure_graph_id_available(graph_id)?;
        let entry = CatalogEntry::Graph { graph_id };
        propose_system_batch(
            &self.repl,
            vec![
                catalog_put_op(&name, &entry)?,
                next_graph_id_op(self.next_graph_id_after(graph_id)?)?,
            ],
        )?;
        self.catalog.insert(name, entry);
        self.invalidate_plan_cache();
        let repl: &dyn Replication = self;
        Ok(Graph::new(repl, graph_id))
    }

    /// Creates a graph with the next available graph id.
    /// # Errors
    /// Fails when catalog validation or metadata persistence fails.
    pub fn create_graph_auto_id(
        &mut self,
        name: impl Into<String>,
    ) -> Result<Graph<'_, dyn Replication + '_>, DbError> {
        self.create_graph(name, GraphId::new(self.catalog_next_graph_id()))
    }

    /// Creates a named geo index and persists it in the catalog.
    ///
    /// Documents already in the source collection are backfilled before the index is
    /// published; documents with an invalid point are skipped and counted. After that,
    /// document writes made through `collection` handles, transactions and Control Plane
    /// document CRUD maintain the index in the same batch.
    /// # Errors
    /// Fails when the name collides, the source is not an existing document collection,
    /// index validation or the backfill fails, or storage rejects metadata.
    pub fn create_geo_index(
        &mut self,
        config: GeoIndexConfig,
    ) -> Result<GeoIndex<'_, dyn Replication + '_>, DbError> {
        self.ensure_non_tensor_model_allowed("create_geo_index")?;
        let name = config.name.clone();
        self.ensure_name_available(&name)?;
        self.ensure_document_collection(config.collection_id)?;
        // Backfill before the catalog entry becomes visible: `&mut self` keeps in-process
        // writers out until it is published, and every later write through `collection`
        // handles maintains the index itself.
        let state = {
            let repl: &dyn Replication = self;
            GeoIndex::new(repl, config.clone())?.rebuild_entries()?
        };
        let entry = CatalogEntry::GeoIndex {
            config: config.clone(),
        };
        let mut ops = GeoIndex::<dyn Replication>::metadata_ops_with_state(&config, &state)?;
        ops.push(catalog_put_op(&name, &entry)?);
        propose_system_batch(&self.repl, ops)?;
        self.catalog.insert(name, entry);
        self.invalidate_plan_cache();
        let repl: &dyn Replication = self;
        Ok(GeoIndex::new(repl, config)?.with_commit_log(!self.repl.is_ap()))
    }

    /// Opens a named relational table from the catalog.
    /// # Errors
    /// Fails when the name is missing, has the wrong kind, or schema metadata is invalid.
    pub fn table(&self, name: &str) -> Result<RelTable, DbError> {
        match self.catalog.get(name) {
            Some(CatalogEntry::Table { indexes, layout }) => Ok(RelTable::open_with_layout(
                self.replication_handle(),
                name,
                indexes.clone(),
                *layout,
            )?),
            Some(other) => Err(kind_mismatch(name, "table", other)),
            None => Err(DbError::MissingCatalogObject(name.to_owned())),
        }
    }

    /// Opens a named document collection from the catalog.
    ///
    /// Writes built by the handle also maintain the collection's full-text and geo
    /// indexes in the same batch.
    /// # Errors
    /// Fails when the name is missing or has the wrong kind.
    pub fn collection(
        &self,
        name: &str,
    ) -> Result<DocumentCollection<'_, dyn Replication + '_>, DbError> {
        match self.catalog.get(name) {
            Some(CatalogEntry::Collection {
                collection_id,
                indexes,
                ..
            }) => {
                let derived = self.derived_indexes_for(*collection_id);
                let repl: &dyn Replication = self;
                Ok(
                    DocumentCollection::with_indexes(repl, *collection_id, indexes.clone())
                        .with_derived_indexes(derived),
                )
            }
            Some(other) => Err(kind_mismatch(name, "collection", other)),
            None => Err(DbError::MissingCatalogObject(name.to_owned())),
        }
    }

    /// Opens a named vector collection from the catalog.
    /// # Errors
    /// Fails when the name is missing, has the wrong kind, or the index cannot rebuild.
    pub fn vector_collection(
        &self,
        name: &str,
    ) -> Result<VectorCollection<'_, dyn Replication + '_>, DbError> {
        match self.catalog.get(name) {
            Some(CatalogEntry::Vector {
                collection_id,
                dim,
                metric,
                hnsw,
                quantization,
                disk_ann,
            }) => self.open_vector_collection_with_config(VectorCollectionConfig {
                collection_id: *collection_id,
                dim: *dim,
                metric: *metric,
                hnsw: *hnsw,
                quantization: quantization.clone(),
                disk_ann: disk_ann.clone(),
            }),
            Some(other) => Err(kind_mismatch(name, "vector collection", other)),
            None => Err(DbError::MissingCatalogObject(name.to_owned())),
        }
    }

    /// Opens a named full-text index.
    /// # Errors
    /// Fails when the object is missing or has the wrong kind.
    pub fn full_text_index(
        &self,
        name: &str,
    ) -> Result<FullTextIndex<'_, dyn Replication + '_>, DbError> {
        match self.catalog.get(name) {
            Some(CatalogEntry::FullTextIndex { config }) => {
                let repl: &dyn Replication = self;
                Ok(FullTextIndex::new(repl, config.clone())?.with_commit_log(!self.repl.is_ap()))
            }
            Some(other) => Err(kind_mismatch(name, "full-text index", other)),
            None => Err(DbError::MissingCatalogObject(name.to_owned())),
        }
    }

    /// Opens a named time-series collection.
    /// # Errors
    /// Fails when the object is missing or has the wrong kind.
    pub fn time_series(
        &self,
        name: &str,
    ) -> Result<TimeSeriesCollection<'_, dyn Replication + '_>, DbError> {
        match self.catalog.get(name) {
            Some(CatalogEntry::TimeSeries { config }) => {
                let repl: &dyn Replication = self;
                Ok(TimeSeriesCollection::new(repl, config.clone())?)
            }
            Some(other) => Err(kind_mismatch(name, "time-series collection", other)),
            None => Err(DbError::MissingCatalogObject(name.to_owned())),
        }
    }

    /// Opens a named graph.
    /// # Errors
    /// Fails when the object is missing or has the wrong kind.
    pub fn graph(&self, name: &str) -> Result<Graph<'_, dyn Replication + '_>, DbError> {
        match self.catalog.get(name) {
            Some(CatalogEntry::Graph { graph_id }) => {
                let repl: &dyn Replication = self;
                Ok(Graph::new(repl, *graph_id))
            }
            Some(other) => Err(kind_mismatch(name, "graph", other)),
            None => Err(DbError::MissingCatalogObject(name.to_owned())),
        }
    }

    /// Opens a named geo index.
    /// # Errors
    /// Fails when the object is missing or has the wrong kind.
    pub fn geo_index(&self, name: &str) -> Result<GeoIndex<'_, dyn Replication + '_>, DbError> {
        match self.catalog.get(name) {
            Some(CatalogEntry::GeoIndex { config }) => {
                let repl: &dyn Replication = self;
                Ok(GeoIndex::new(repl, config.clone())?.with_commit_log(!self.repl.is_ap()))
            }
            Some(other) => Err(kind_mismatch(name, "geo index", other)),
            None => Err(DbError::MissingCatalogObject(name.to_owned())),
        }
    }

    /// Collects and persists optimizer statistics.
    /// # Errors
    /// Fails when the target is missing or stats cannot be written.
    pub fn analyze(
        &self,
        target: AnalyzeTarget,
        mode: AnalyzeMode,
    ) -> Result<AnalyzeReport, DbError> {
        Ok(self.sql_engine()?.analyze(target, mode)?)
    }

    /// Builds an optimizer explanation for a SELECT statement.
    /// # Errors
    /// Fails when SQL is unsupported or `EXPLAIN ANALYZE` execution fails.
    pub fn explain(&self, sql: &str, options: ExplainOptions) -> Result<ExplainReport, DbError> {
        Ok(self.sql_engine()?.explain(sql, options)?)
    }

    /// Reads recorded estimated-vs-actual planner feedback.
    /// # Errors
    /// Fails when feedback storage cannot be read.
    pub fn planner_feedback(&self) -> Result<Vec<PlannerFeedback>, DbError> {
        Ok(StatsCatalog::read_feedback(self.replication_ref())?)
    }

    /// Records one workload observation from trusted code or tests.
    ///
    /// The sample is persisted, together with any buffered SQL observations, in one system
    /// write before this returns.
    /// # Errors
    /// Fails when workload metadata cannot be persisted.
    pub fn record_workload_sample(&self, sample: &WorkloadSample) -> Result<(), DbError> {
        self.workload.record(self.replication_ref(), sample)?;
        self.flush_workload_samples()
    }

    /// Persists buffered workload observations in one batched system write.
    ///
    /// SQL statements only aggregate workload samples in memory. The buffer is also flushed
    /// once enough statements, distinct fingerprints or SQL text have accumulated, on the
    /// first statement after the oldest buffered sample is 10 seconds old, best-effort before
    /// index or runtime advice, and when a single-node database is dropped. Samples buffered
    /// since the last flush are lost on crash.
    /// # Errors
    /// Fails when workload metadata cannot be read or persisted; the drained samples are
    /// discarded.
    pub fn flush_workload_samples(&self) -> Result<(), DbError> {
        Ok(self.workload.flush(self.replication_ref())?)
    }

    /// Reads the workload profile, including SQL observations still buffered in memory.
    ///
    /// This performs no write.
    /// # Errors
    /// Fails when workload metadata cannot be read.
    pub fn workload_report(&self, window: WorkloadWindow) -> Result<WorkloadReport, DbError> {
        Ok(self.workload.report(self.replication_ref(), window)?)
    }

    /// Reads workload profile after checking system admin permission.
    /// # Errors
    /// Fails when authorization or workload metadata reads fail.
    pub fn workload_report_as(
        &self,
        principal: &Principal,
        window: WorkloadWindow,
    ) -> Result<WorkloadReport, DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::System,
            Permission::Admin,
            "workload_report",
        )?;
        self.workload_report(window)
    }

    /// Builds and persists index recommendations from workload observations.
    ///
    /// Buffered SQL observations are flushed first on a best-effort basis.
    /// # Errors
    /// Fails when workload metadata or advice storage fails.
    pub fn index_advice(&self) -> Result<IndexAdviceReport, DbError> {
        self.flush_workload_samples_before_advice();
        Ok(IndexAdvisor::advise(
            self.replication_ref(),
            &self.catalog_index_map(),
        )?)
    }

    /// Builds index recommendations after checking system admin permission.
    /// # Errors
    /// Fails when authorization or advice generation fails.
    pub fn index_advice_as(&self, principal: &Principal) -> Result<IndexAdviceReport, DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::System,
            Permission::Admin,
            "index_advice",
        )?;
        let advice = self.index_advice()?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "index_advice",
            Resource::System,
            AuditOutcome::Succeeded,
            Some(&format!(
                "recommendations: {}",
                advice.recommendations.len()
            )),
        ))?;
        Ok(advice)
    }

    /// Builds Runtime Advisor V2 recommendations from workload, planner feedback and config state.
    ///
    /// Buffered SQL observations are flushed first on a best-effort basis.
    /// # Errors
    /// Fails when advisor metadata or dry-run planning cannot be read.
    pub fn runtime_advice(&self) -> Result<RuntimeAdviceReport, DbError> {
        self.flush_workload_samples_before_advice();
        let current = DatabaseSpec::from_db_config("current", self.config());
        Ok(RuntimeAdvisor::advise(
            self.replication_ref(),
            &current,
            &self.catalog_index_map(),
        )?)
    }

    /// Builds Runtime Advisor V2 recommendations after checking system admin permission.
    /// # Errors
    /// Fails when authorization, audit, or advisor generation fails.
    pub fn runtime_advice_as(&self, principal: &Principal) -> Result<RuntimeAdviceReport, DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::System,
            Permission::Admin,
            "runtime_advice",
        )?;
        let advice = self.runtime_advice()?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "runtime_advice",
            Resource::System,
            AuditOutcome::Succeeded,
            Some(&format!(
                "recommendations: {}; suppressed: {}; auto_apply_enabled: false",
                advice.recommendations.len(),
                advice.suppressed_recommendations
            )),
        ))?;
        Ok(advice)
    }

    /// Returns the dry-run migration plan for a runtime advice id.
    ///
    /// Buffered SQL observations are flushed first on a best-effort basis.
    /// # Errors
    /// Fails when the advice cannot be generated or the id is unknown.
    pub fn runtime_advice_plan(&self, advice_id: &str) -> Result<MigrationPlan, DbError> {
        self.flush_workload_samples_before_advice();
        let current = DatabaseSpec::from_db_config("current", self.config());
        Ok(RuntimeAdvisor::plan_by_id(
            self.replication_ref(),
            &current,
            &self.catalog_index_map(),
            advice_id,
        )?)
    }

    /// Returns a runtime advice dry-run plan after checking system admin permission.
    /// # Errors
    /// Fails when authorization, audit, or plan generation fails.
    pub fn runtime_advice_plan_as(
        &self,
        principal: &Principal,
        advice_id: &str,
    ) -> Result<MigrationPlan, DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::System,
            Permission::Admin,
            "runtime_advice_plan",
        )?;
        let plan = self.runtime_advice_plan(advice_id)?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "runtime_advice_plan",
            Resource::System,
            AuditOutcome::Succeeded,
            Some(&format!(
                "advice_id: {advice_id}; plan_id: {}; data_mutated: false",
                plan.plan_id
            )),
        ))?;
        Ok(plan)
    }

    /// Records a Runtime Advisor decision from trusted embedded/admin code.
    /// # Errors
    /// Fails when the decision cannot be persisted.
    pub fn record_runtime_advice_decision(
        &self,
        request: RuntimeAdviceDecisionRequest,
        decided_by: &str,
    ) -> Result<RuntimeAdviceDecision, DbError> {
        Ok(RuntimeAdvisor::record_decision(
            self.replication_ref(),
            request,
            decided_by,
        )?)
    }

    /// Records a Runtime Advisor decision after checking system admin permission and audit.
    /// # Errors
    /// Fails when authorization, audit, or decision persistence fails.
    pub fn record_runtime_advice_decision_as(
        &self,
        principal: &Principal,
        request: RuntimeAdviceDecisionRequest,
    ) -> Result<RuntimeAdviceDecision, DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::System,
            Permission::Admin,
            "runtime_advice_decision",
        )?;
        if !self.audit_enabled {
            return Err(DbError::AuditIntegrity(
                "runtime_advice_decision requires audit to be enabled".to_owned(),
            ));
        }
        let decision = self.record_runtime_advice_decision(request, principal.name())?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "runtime_advice_decision",
            Resource::System,
            AuditOutcome::Succeeded,
            Some(&format!(
                "advice_id: {}; status: {:?}; suppress_until_millis: {:?}; data_mutated: false",
                decision.advice_id, decision.status, decision.suppress_until_millis
            )),
        ))?;
        Ok(decision)
    }

    /// Persists the self-tuning policy from trusted embedded/admin code.
    /// # Errors
    /// Fails when policy validation or metadata persistence fails.
    pub fn set_tuning_policy(&self, policy: &TuningPolicy) -> Result<(), DbError> {
        Ok(tuning::write_policy(self.replication_ref(), policy)?)
    }

    /// Persists the self-tuning policy after checking system admin permission.
    /// # Errors
    /// Fails when authorization, audit, or metadata persistence fails.
    pub fn set_tuning_policy_as(
        &self,
        principal: &Principal,
        policy: &TuningPolicy,
    ) -> Result<(), DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::System,
            Permission::Admin,
            "set_tuning_policy",
        )?;
        self.set_tuning_policy(policy)?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "set_tuning_policy",
            Resource::System,
            AuditOutcome::Succeeded,
            Some(&format!(
                "enabled: {}, recommend_only: {}",
                policy.enabled, policy.recommend_only
            )),
        ))?;
        Ok(())
    }

    /// Reads the persisted self-tuning policy.
    /// # Errors
    /// Fails when policy metadata cannot be read.
    pub fn tuning_policy(&self) -> Result<TuningPolicy, DbError> {
        Ok(tuning::read_policy(self.replication_ref())?)
    }

    /// Applies a tuning decision inside the persisted policy envelope.
    /// # Errors
    /// Fails when the decision is outside policy or the tuning log cannot be written.
    pub fn apply_tuning(&mut self, decision: TuningDecision) -> Result<TuningLogEntry, DbError> {
        let policy = self.tuning_policy()?;
        let history = self.tuning_log()?;
        let entry = tuning::apply_decision_to_config_with_history(
            &mut self.performance,
            &policy,
            decision,
            &history,
        )?;
        self.performance_cache = PerformanceCache::new(&self.performance.cache);
        self.query_permits = query_permits_for(&self.performance);
        self.invalidate_plan_cache();
        tuning::write_tuning_log(self.replication_ref(), &entry)?;
        Ok(entry)
    }

    /// Applies a tuning decision after checking system admin permission.
    /// # Errors
    /// Fails when authorization, policy validation, audit, or logging fails.
    pub fn apply_tuning_as(
        &mut self,
        principal: &Principal,
        decision: TuningDecision,
    ) -> Result<TuningLogEntry, DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::System,
            Permission::Admin,
            "apply_tuning",
        )?;
        let entry = self.apply_tuning(decision)?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "apply_tuning",
            Resource::System,
            AuditOutcome::Succeeded,
            Some(&format!("decision: {}", entry.id)),
        ))?;
        Ok(entry)
    }

    /// Rolls back a previously applied tuning decision.
    /// # Errors
    /// Fails when the decision is missing or the rollback log cannot be written.
    pub fn rollback_tuning(
        &mut self,
        decision_id: &str,
        reason: &str,
    ) -> Result<TuningLogEntry, DbError> {
        let applied = tuning::find_tuning_log_entry(self.replication_ref(), decision_id)?;
        let rollback =
            tuning::rollback_decision_in_config(&mut self.performance, &applied, reason)?;
        self.performance_cache = PerformanceCache::new(&self.performance.cache);
        self.query_permits = query_permits_for(&self.performance);
        self.invalidate_plan_cache();
        tuning::write_tuning_log(self.replication_ref(), &rollback)?;
        observability::record_tuning_rollback(&rollback.decision.id);
        Ok(rollback)
    }

    /// Evaluates candidate performance and automatically rolls back a tuning decision on regression.
    /// # Errors
    /// Fails when the rollback cannot be written or the decision is missing.
    pub fn evaluate_tuning_regression(
        &mut self,
        decision_id: &str,
        baseline: &[BenchmarkReport],
        candidate: &[BenchmarkReport],
        gate: RegressionGate,
    ) -> Result<Option<TuningLogEntry>, DbError> {
        let result = gate.compare(baseline, candidate);
        if result.is_ok() {
            return Ok(None);
        }
        let reason = format!("performance regression: {}", result.failed.join("; "));
        let rollback = self.rollback_tuning(decision_id, &reason)?;
        self.record_audit(&AuditEvent::new(
            None,
            "evaluate_tuning_regression",
            Resource::System,
            AuditOutcome::Succeeded,
            Some(&format!("decision: {decision_id}; {reason}")),
        ))?;
        Ok(Some(rollback))
    }

    /// Rolls back a tuning decision after checking system admin permission.
    /// # Errors
    /// Fails when authorization, audit, or rollback fails.
    pub fn rollback_tuning_as(
        &mut self,
        principal: &Principal,
        decision_id: &str,
        reason: &str,
    ) -> Result<TuningLogEntry, DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::System,
            Permission::Admin,
            "rollback_tuning",
        )?;
        let entry = self.rollback_tuning(decision_id, reason)?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "rollback_tuning",
            Resource::System,
            AuditOutcome::Succeeded,
            Some(&format!("decision: {decision_id}")),
        ))?;
        Ok(entry)
    }

    /// Reads self-tuning decisions and rollbacks.
    /// # Errors
    /// Fails when metadata cannot be read.
    pub fn tuning_log(&self) -> Result<Vec<TuningLogEntry>, DbError> {
        Ok(tuning::read_tuning_log(self.replication_ref())?)
    }

    /// Reads self-tuning log after checking system admin permission.
    /// # Errors
    /// Fails when authorization or metadata reads fail.
    pub fn tuning_log_as(&self, principal: &Principal) -> Result<Vec<TuningLogEntry>, DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::System,
            Permission::Admin,
            "tuning_log",
        )?;
        self.tuning_log()
    }

    /// Starts a reversible online reprofiling job.
    /// # Errors
    /// Fails when the job is unsupported or cannot be persisted.
    pub fn start_reprofile(&self, plan: ReprofilePlan) -> Result<ReprofileJob, DbError> {
        Ok(tuning::start_reprofile_job(self.replication_ref(), plan)?)
    }

    /// Starts a reversible online reprofiling job after checking system admin permission.
    /// # Errors
    /// Fails when authorization, audit, or job persistence fails.
    pub fn start_reprofile_as(
        &self,
        principal: &Principal,
        plan: ReprofilePlan,
    ) -> Result<ReprofileJob, DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::System,
            Permission::Admin,
            "start_reprofile",
        )?;
        let job = self.start_reprofile(plan)?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "start_reprofile",
            Resource::System,
            AuditOutcome::Succeeded,
            Some(&format!("job: {}", job.id)),
        ))?;
        Ok(job)
    }

    /// Advances a reprofiling job state.
    /// # Errors
    /// Fails when the job is missing or cannot be persisted.
    pub fn advance_reprofile(
        &self,
        id: &str,
        status: ReprofileStatus,
        detail: &str,
    ) -> Result<ReprofileJob, DbError> {
        Ok(tuning::advance_reprofile_job(
            self.replication_ref(),
            id,
            status,
            detail,
        )?)
    }

    /// Reads one reprofiling job.
    /// # Errors
    /// Fails when job metadata cannot be read.
    pub fn reprofile_status(&self, id: &str) -> Result<Option<ReprofileJob>, DbError> {
        Ok(tuning::read_reprofile_job(self.replication_ref(), id)?)
    }

    /// Reads all reprofiling jobs.
    /// # Errors
    /// Fails when job metadata cannot be read.
    pub fn reprofile_jobs(&self) -> Result<Vec<ReprofileJob>, DbError> {
        Ok(tuning::read_reprofile_jobs(self.replication_ref())?)
    }

    /// Reads committed changes after a resume token.
    ///
    /// This trusted API checks no permission. With `options.include_system`, the events carry
    /// system rows too: admin password hashes, the RBAC policy and principal registry, the audit
    /// log, policy definitions and raw index keys. [`Database::poll_changefeed_as`] reads for a
    /// principal.
    /// # Errors
    /// Fails when the database uses AP replication, the token is invalid, or CDC metadata cannot
    /// be read.
    pub fn poll_changefeed(
        &self,
        token: &ResumeToken,
        filter: &ChangefeedFilter,
        options: &ChangefeedOptions,
        max: usize,
    ) -> Result<(Vec<ChangeEvent>, ResumeToken), DbError> {
        self.ensure_commit_log_available("changefeeds")?;
        Ok(cdc::poll_changefeed(
            self.replication_ref(),
            &self.catalog,
            token,
            filter,
            options,
            max,
        )?)
    }

    /// Reads one page of committed changes after a resume token.
    ///
    /// Like [`Database::poll_changefeed`], this trusted API checks no permission and returns
    /// system rows with `options.include_system`; [`Database::poll_changefeed_page_as`] reads
    /// for a principal.
    /// # Errors
    /// Fails when the database uses AP replication, the token is invalid, or CDC metadata cannot
    /// be read.
    pub fn poll_changefeed_page(
        &self,
        token: &ResumeToken,
        filter: &ChangefeedFilter,
        options: &ChangefeedOptions,
        max: usize,
    ) -> Result<ChangefeedPage, DbError> {
        self.ensure_commit_log_available("changefeeds")?;
        Ok(cdc::poll_changefeed_page(
            self.replication_ref(),
            &self.catalog,
            token,
            filter,
            options,
            max,
        )?)
    }

    /// Polls committed change events after checking RBAC and applying policies.
    ///
    /// The principal needs `Read` on the filter target. A poll that can return system rows
    /// (`options.include_system` set, or a `ChangefeedTarget::System` target) also needs
    /// `System:Admin`, which for a `System` target covers that `Read`: system rows carry
    /// credential hashes, the RBAC state, the audit log and raw index keys. A denied
    /// `System:Admin` check is audited as `poll_changefeed`. System rows are never returned to a
    /// principal without `System:Admin`.
    /// # Errors
    /// Fails when authorization, changefeed reads, or policy masking fail.
    pub fn poll_changefeed_as(
        &self,
        principal: &Principal,
        token: &ResumeToken,
        filter: &ChangefeedFilter,
        options: &ChangefeedOptions,
        max: usize,
    ) -> Result<(Vec<ChangeEvent>, ResumeToken), DbError> {
        self.authorize_changefeed_poll(principal, filter, options)?;
        let (events, next) = self.poll_changefeed(token, filter, options, max)?;
        Ok((self.changefeed_events_visible_to(principal, events)?, next))
    }

    /// Polls one page of committed change events after checking RBAC and applying policies.
    ///
    /// Checks the same permissions as [`Database::poll_changefeed_as`].
    /// # Errors
    /// Fails when authorization, changefeed reads, or policy masking fail.
    pub fn poll_changefeed_page_as(
        &self,
        principal: &Principal,
        token: &ResumeToken,
        filter: &ChangefeedFilter,
        options: &ChangefeedOptions,
        max: usize,
    ) -> Result<ChangefeedPage, DbError> {
        self.authorize_changefeed_poll(principal, filter, options)?;
        let mut page = self.poll_changefeed_page(token, filter, options, max)?;
        page.events = self.changefeed_events_visible_to(principal, page.events)?;
        Ok(page)
    }

    /// Creates or replaces a named CDC subscription after checking RBAC.
    /// # Errors
    /// Fails when the database uses AP replication, or authorization or subscription metadata
    /// persistence fails.
    pub fn create_subscription_as(
        &self,
        principal: &Principal,
        config: SubscriptionConfig,
    ) -> Result<SubscriptionState, DbError> {
        self.ensure_commit_log_available("CDC subscriptions")?;
        Ok(cdc::create_subscription(
            self.replication_ref(),
            &self.authz,
            principal,
            config,
        )?)
    }

    /// Creates or replaces a named CDC subscription from trusted embedded/admin code.
    /// # Errors
    /// Fails when the database uses AP replication or subscription metadata persistence fails.
    pub fn create_subscription(
        &self,
        config: SubscriptionConfig,
    ) -> Result<SubscriptionState, DbError> {
        self.ensure_commit_log_available("CDC subscriptions")?;
        Ok(cdc::create_subscription_trusted(
            self.replication_ref(),
            config,
        )?)
    }

    /// Acknowledges a named CDC subscription from trusted embedded/admin code.
    /// # Errors
    /// Fails when the subscription is missing or metadata persistence fails.
    pub fn ack_subscription(
        &self,
        name: &str,
        token: ResumeToken,
    ) -> Result<SubscriptionState, DbError> {
        Ok(cdc::ack_subscription(self.replication_ref(), name, token)?)
    }

    /// Reads a named CDC subscription state from trusted embedded/admin code.
    /// # Errors
    /// Fails when the subscription is missing or metadata reads fail.
    pub fn subscription_state(&self, name: &str) -> Result<SubscriptionState, DbError> {
        Ok(cdc::subscription_state(self.replication_ref(), name)?)
    }

    /// Acknowledges a named CDC subscription.
    /// # Errors
    /// Fails when the subscription is missing or metadata persistence fails.
    pub fn ack_subscription_as(
        &self,
        principal: &Principal,
        name: &str,
        token: ResumeToken,
    ) -> Result<SubscriptionState, DbError> {
        Ok(cdc::ack_subscription_as(
            self.replication_ref(),
            &self.authz,
            principal,
            name,
            token,
        )?)
    }

    /// Reads a named CDC subscription state.
    /// # Errors
    /// Fails when authorization or metadata reads fail.
    pub fn subscription_state_as(
        &self,
        principal: &Principal,
        name: &str,
    ) -> Result<SubscriptionState, DbError> {
        let state = cdc::subscription_state(self.replication_ref(), name)?;
        cdc::require_subscription_owner(&state, principal)?;
        cdc::authorize_filter_access(&self.authz, principal, &state.config.filter)?;
        Ok(state)
    }

    /// Creates a continuous query on top of the durable CDC subscription machinery.
    /// # Errors
    /// Fails when the database uses AP replication, or metadata or subscription persistence fails.
    pub fn create_continuous_query(
        &self,
        spec: &ContinuousQuerySpec,
    ) -> Result<ContinuousQueryState, DbError> {
        self.ensure_commit_log_available("continuous queries")?;
        let state = continuous::create_continuous_query(self.replication_ref(), spec)?;
        cdc::create_subscription_trusted(self.replication_ref(), spec.subscription_config())?;
        Ok(state)
    }

    /// Creates a continuous query after checking RBAC on its changefeed target.
    /// # Errors
    /// Fails when authorization, metadata, or subscription persistence fails.
    pub fn create_continuous_query_as(
        &self,
        principal: &Principal,
        spec: &ContinuousQuerySpec,
    ) -> Result<ContinuousQueryState, DbError> {
        cdc::authorize_filter_access(&self.authz, principal, &spec.filter)?;
        self.ensure_commit_log_available("continuous queries")?;
        let state = continuous::create_continuous_query_as(
            self.replication_ref(),
            &self.authz,
            principal,
            spec,
        )?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "create_continuous_query",
            Resource::Database,
            AuditOutcome::Succeeded,
            Some(&format!("name: {}", spec.name)),
        ))?;
        Ok(state)
    }

    /// Acknowledges a continuous query and its underlying CDC subscription.
    /// # Errors
    /// Fails when the query/subscription is missing or metadata persistence fails.
    pub fn ack_continuous_query(
        &self,
        name: &str,
        token: ResumeToken,
    ) -> Result<ContinuousQueryState, DbError> {
        Ok(continuous::ack_continuous_query_and_subscription(
            self.replication_ref(),
            name,
            token,
        )?)
    }

    /// Acknowledges a continuous query after checking RBAC on its subscription filter.
    /// # Errors
    /// Fails when authorization or metadata persistence fails.
    pub fn ack_continuous_query_as(
        &self,
        principal: &Principal,
        name: &str,
        token: ResumeToken,
    ) -> Result<ContinuousQueryState, DbError> {
        Ok(continuous::ack_continuous_query_and_subscription_as(
            self.replication_ref(),
            &self.authz,
            principal,
            name,
            token,
        )?)
    }

    /// Registers an outbox connector in durable metadata.
    /// # Errors
    /// Fails when connector metadata is invalid or cannot be written.
    pub fn create_outbox(&self, spec: &OutboxConnectorSpec) -> Result<(), DbError> {
        Ok(continuous::register_outbox_connector(
            self.replication_ref(),
            spec,
        )?)
    }

    /// Registers an outbox connector after checking RBAC on its filter.
    /// # Errors
    /// Fails when authorization or metadata persistence fails.
    pub fn create_outbox_as(
        &self,
        principal: &Principal,
        spec: &OutboxConnectorSpec,
    ) -> Result<(), DbError> {
        cdc::authorize_filter_access(&self.authz, principal, &spec.filter)?;
        self.create_outbox(spec)?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "create_outbox",
            Resource::Database,
            AuditOutcome::Succeeded,
            Some(&format!("name: {}", spec.name)),
        ))?;
        Ok(())
    }

    /// Creates and initializes an incremental materialized view.
    /// # Errors
    /// Fails when the source table is missing or the spec is unsupported.
    pub fn create_materialized_view(
        &self,
        spec: &MaterializedViewSpec,
    ) -> Result<MaterializedViewRows, DbError> {
        self.ensure_non_tensor_model_allowed("create_materialized_view")?;
        Ok(cdc::create_materialized_view(self, spec)?)
    }

    /// Creates an incremental materialized view and registers it in the query catalog.
    /// # Errors
    /// Fails when the view is unsupported, source schema is missing, or catalog persistence fails.
    pub fn create_materialized_view_object(
        &mut self,
        spec: &MaterializedViewSpec,
    ) -> Result<MaterializedViewRows, DbError> {
        self.ensure_non_tensor_model_allowed("create_materialized_view_object")?;
        self.ensure_name_available(&spec.name)?;
        let source_schema = self
            .table(&spec.source_table)?
            .schema()
            .cloned()
            .ok_or_else(|| {
                QueryError::InvalidSchema("materialized view source requires schema".to_owned())
            })?;
        let rows = cdc::create_materialized_view(self, spec)?;
        let entry = CatalogEntry::MaterializedView {
            spec: spec.clone(),
            source_schema,
        };
        propose_system_batch(&self.repl, vec![catalog_put_op(&spec.name, &entry)?])?;
        self.catalog.insert(spec.name.clone(), entry);
        self.invalidate_plan_cache();
        Ok(rows)
    }

    /// Creates a cataloged materialized view after checking source-table admin permission.
    /// # Errors
    /// Fails when authorization, view creation, or catalog persistence fails.
    pub fn create_materialized_view_object_as(
        &mut self,
        principal: &Principal,
        spec: &MaterializedViewSpec,
    ) -> Result<MaterializedViewRows, DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::Table(spec.source_table.clone()),
            Permission::Admin,
            "create_materialized_view",
        )?;
        let source = Resource::Table(spec.source_table.clone());
        self.authorize_or_audit(
            principal,
            &source,
            Permission::Read,
            "create_materialized_view",
        )?;
        if policy_has_row_or_masking(&self.policy_config()?, &source) {
            return Err(ExtensionError::PolicyDenied(
                "materialized views over policy-protected sources cannot be created as a principal"
                    .to_owned(),
            )
            .into());
        }
        let rows = self.create_materialized_view_object(spec)?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "create_materialized_view",
            Resource::Table(spec.name.clone()),
            AuditOutcome::Succeeded,
            Some(&format!("source: {}", spec.source_table)),
        ))?;
        Ok(rows)
    }

    /// Refreshes an incremental materialized view to the current LSN.
    /// # Errors
    /// Fails when the view is missing or delta application fails.
    pub fn refresh_materialized_view_to_current(
        &self,
        name: &str,
    ) -> Result<MaterializedViewRows, DbError> {
        Ok(cdc::refresh_materialized_view(self, name)?)
    }

    /// Reads materialized view rows and freshness.
    /// # Errors
    /// Fails when the view is missing or corrupt.
    pub fn read_materialized_view(&self, name: &str) -> Result<MaterializedViewRows, DbError> {
        Ok(cdc::read_materialized_view(self.replication_ref(), name)?)
    }

    /// Registers a CDC hook.
    /// # Errors
    /// Fails when an after-commit hook is registered on an AP database or hook metadata cannot
    /// be written.
    pub fn register_hook(&self, hook: &HookSpec) -> Result<(), DbError> {
        if hook.kind == cdc::HookKind::AfterCommit {
            self.ensure_commit_log_available("after-commit hooks")?;
        }
        Ok(cdc::register_hook(self.replication_ref(), hook)?)
    }

    /// Deletes a CDC hook.
    /// # Errors
    /// Fails when hook metadata cannot be deleted.
    pub fn unregister_hook(&self, name: &str) -> Result<(), DbError> {
        Ok(cdc::unregister_hook(self.replication_ref(), name)?)
    }

    /// Lists after-commit hook and after-trigger failures, oldest first.
    ///
    /// A write whose backend commit succeeded is never reported as failed. When an
    /// after-commit hook or an after trigger fails afterwards, the failure is logged,
    /// counted in `multidb_post_commit_failures_total`, and recorded here instead.
    /// # Errors
    /// Fails when the failure records cannot be read or decoded.
    pub fn post_commit_failures(&self) -> Result<Vec<PostCommitFailure>, DbError> {
        Ok(cdc::read_post_commit_failures(self.replication_ref())?)
    }

    #[must_use]
    pub fn plan_cache_metrics(&self) -> PlanCacheMetrics {
        self.plan_cache
            .lock()
            .map_or_else(|_| PlanCacheMetrics::default(), |cache| cache.metrics())
    }

    /// Registers a scalar WASM UDF from trusted embedded/admin code.
    /// # Errors
    /// Fails when the module is invalid or metadata cannot be persisted.
    pub fn register_wasm_udf(&self, name: &str, wasm: &[u8]) -> Result<UdfSpec, DbError> {
        Ok(extension::register_wasm_udf(
            self.replication_ref(),
            &self.wasm_runtime,
            name,
            wasm,
        )?)
    }

    /// Registers a scalar WASM UDF after checking system admin permission.
    /// # Errors
    /// Fails when authorization, module validation, audit, or metadata persistence fails.
    pub fn register_wasm_udf_as(
        &self,
        principal: &Principal,
        name: &str,
        wasm: &[u8],
    ) -> Result<UdfSpec, DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::System,
            Permission::Admin,
            "register_wasm_udf",
        )?;
        let spec = self.register_wasm_udf(name, wasm)?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "register_wasm_udf",
            Resource::System,
            AuditOutcome::Succeeded,
            Some(&format!("name: {name}, wasm_len: {}", wasm.len())),
        ))?;
        Ok(spec)
    }

    /// Calls a registered WASM UDF from trusted embedded/admin code.
    /// # Errors
    /// Fails when the UDF is missing, traps, or returns invalid bytes.
    pub fn call_udf(&self, name: &str, args: &[Value]) -> Result<Value, DbError> {
        Ok(extension::call_registered_udf(
            self.replication_ref(),
            &self.wasm_runtime,
            name,
            args,
        )?)
    }

    /// Calls a registered WASM UDF after checking database read permission.
    /// # Errors
    /// Fails when authorization or WASM execution fails.
    pub fn call_udf_as(
        &self,
        principal: &Principal,
        name: &str,
        args: &[Value],
    ) -> Result<Value, DbError> {
        self.authorize_or_audit(principal, &Resource::Database, Permission::Read, "call_udf")?;
        self.call_udf(name, args)
    }

    /// Registers a WASM trigger from trusted embedded/admin code.
    ///
    /// Triggers run for every user write to the target: `Database` writes, table
    /// handles, SQL `INSERT`, transactions and stored procedures. A before trigger
    /// returns `accept`, `reject` or `replace`. A `replace` value must be a row that
    /// matches the table schema and keeps the primary key, and the row's
    /// secondary-index entries are rebuilt. After triggers must return `accept`;
    /// anything else is recorded as a post-commit failure and does not fail the
    /// committed write. Triggers are supported on row-layout tables only.
    /// # Errors
    /// Fails when the table is missing or columnar, WASM validation fails, or
    /// metadata cannot be written.
    pub fn register_wasm_trigger(
        &self,
        mut spec: TriggerSpec,
        wasm: &[u8],
    ) -> Result<TriggerSpec, DbError> {
        // A columnar write replaces a whole segment, so row events cannot be told
        // apart; refuse triggers there instead of firing them with the wrong event.
        if self.table(&spec.table)?.layout() == TableLayout::Columnar {
            return Err(DbError::Query(QueryError::Unsupported(format!(
                "WASM triggers on columnar table {}",
                spec.table
            ))));
        }
        let module =
            extension::register_wasm_module(self.replication_ref(), &self.wasm_runtime, wasm)?;
        spec.module_hash = module.hash;
        continuous::register_trigger(self.replication_ref(), &spec)?;
        Ok(spec)
    }

    /// Registers a WASM trigger after checking table admin permission.
    /// # Errors
    /// Fails when authorization, WASM validation, or metadata persistence fails.
    pub fn register_wasm_trigger_as(
        &self,
        principal: &Principal,
        spec: TriggerSpec,
        wasm: &[u8],
    ) -> Result<TriggerSpec, DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::Table(spec.table.clone()),
            Permission::Admin,
            "register_wasm_trigger",
        )?;
        let spec = self.register_wasm_trigger(spec, wasm)?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "register_wasm_trigger",
            Resource::Table(spec.table.clone()),
            AuditOutcome::Succeeded,
            Some(&format!("name: {}, wasm_len: {}", spec.name, wasm.len())),
        ))?;
        Ok(spec)
    }

    /// Registers a WASM stored procedure from trusted embedded/admin code.
    /// # Errors
    /// Fails when WASM validation or metadata persistence fails.
    pub fn register_wasm_procedure(
        &self,
        mut spec: ProcedureSpec,
        wasm: &[u8],
    ) -> Result<ProcedureSpec, DbError> {
        let module =
            extension::register_wasm_module(self.replication_ref(), &self.wasm_runtime, wasm)?;
        spec.module_hash = module.hash;
        continuous::register_procedure(self.replication_ref(), &spec)?;
        Ok(spec)
    }

    /// Registers a WASM stored procedure after checking database admin permission.
    /// # Errors
    /// Fails when authorization, WASM validation, or metadata persistence fails.
    pub fn register_wasm_procedure_as(
        &self,
        principal: &Principal,
        spec: ProcedureSpec,
        wasm: &[u8],
    ) -> Result<ProcedureSpec, DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::Database,
            Permission::Admin,
            "register_wasm_procedure",
        )?;
        let spec = self.register_wasm_procedure(spec, wasm)?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "register_wasm_procedure",
            Resource::Database,
            AuditOutcome::Succeeded,
            Some(&format!("name: {}, wasm_len: {}", spec.name, wasm.len())),
        ))?;
        Ok(spec)
    }

    /// Calls a registered WASM procedure and atomically applies host-validated commands.
    /// # Errors
    /// Fails when the procedure is missing, traps, returns invalid commands, or writes fail.
    pub fn call_procedure(&self, name: &str, args: &[Value]) -> Result<ProcedureResult, DbError> {
        let spec = continuous::read_procedure(self.replication_ref(), name)?;
        let (_, wasm) = extension::read_wasm_module(self.replication_ref(), &spec.module_hash)?;
        let mut udf = UdfSpec::scalar(&spec.name, &spec.module_hash);
        udf.entry.clone_from(&spec.entry);
        udf.abi = spec.abi;
        udf.budget = spec.budget.clone();
        let value = self.wasm_runtime.call_udf(&udf, &wasm, args)?;
        let commands = continuous::procedure_commands_from_value(value)?;
        // Procedure writes are user writes: route them through the write pipeline.
        let repl: &dyn Replication = self;
        apply_procedure_commands(repl, commands)
    }

    /// Calls a registered WASM procedure after checking database write permission.
    /// # Errors
    /// Fails when authorization or procedure execution fails.
    pub fn call_procedure_as(
        &self,
        principal: &Principal,
        name: &str,
        args: &[Value],
    ) -> Result<ProcedureResult, DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::Database,
            Permission::Write,
            "call_procedure",
        )?;
        self.call_procedure(name, args)
    }

    /// Lists registered UDF metadata from trusted embedded/admin code.
    /// # Errors
    /// Fails when extension metadata cannot be read.
    pub fn udf_specs(&self) -> Result<Vec<UdfSpec>, DbError> {
        Ok(extension::read_udfs(self.replication_ref())?)
    }

    /// Persists extension policies from trusted embedded/admin code.
    /// # Errors
    /// Fails when policy metadata cannot be written.
    pub fn set_policy_config(&self, policy: &PolicyConfig) -> Result<(), DbError> {
        Ok(extension::write_policy_config(
            self.replication_ref(),
            policy,
        )?)
    }

    /// Persists extension policies after checking system admin permission.
    /// # Errors
    /// Fails when authorization, audit, or metadata persistence fails.
    pub fn set_policy_config_as(
        &self,
        principal: &Principal,
        policy: &PolicyConfig,
    ) -> Result<(), DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::System,
            Permission::Admin,
            "set_policy_config",
        )?;
        self.set_policy_config(policy)?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "set_policy_config",
            Resource::System,
            AuditOutcome::Succeeded,
            None,
        ))?;
        Ok(())
    }

    /// Reads extension policies.
    /// # Errors
    /// Fails when policy metadata cannot be read.
    pub fn policy_config(&self) -> Result<PolicyConfig, DbError> {
        Ok(extension::read_policy_config(self.replication_ref())?)
    }

    /// Registers codec metadata from trusted embedded/admin code.
    /// # Errors
    /// Fails when codec metadata cannot be written.
    pub fn register_codec_spec(&self, spec: &CodecSpec) -> Result<(), DbError> {
        Ok(extension::register_codec_spec(
            self.replication_ref(),
            spec,
        )?)
    }

    /// Registers collation metadata from trusted embedded/admin code.
    /// # Errors
    /// Fails when collation metadata cannot be written.
    pub fn register_collation_spec(&self, spec: &CollationSpec) -> Result<(), DbError> {
        Ok(extension::register_collation_spec(
            self.replication_ref(),
            spec,
        )?)
    }

    /// Runs a SQL query over all cataloged tables and document projections.
    /// # Errors
    /// Fails when catalog metadata is invalid or query execution fails.
    pub async fn query(&self, sql: &str) -> Result<SqlOutput, DbError> {
        self.query_with_read_guard(sql, None).await
    }

    async fn query_with_read_guard(
        &self,
        sql: &str,
        guard: Option<&mut dyn SqlReadGuard>,
    ) -> Result<SqlOutput, DbError> {
        validate_sql_text_size(sql, &self.performance.query)?;
        let started = Instant::now();
        let _query_permit = self.acquire_query_permit().await.map_err(DbError::from)?;
        if let Some((rewritten, point)) = parse_as_of_query(sql)? {
            let result = self
                .query_as_of_with_read_guard(&rewritten, point, guard)
                .await;
            return self.finish_query(sql, started, result);
        }
        if let Some((name, args)) = parse_call_procedure_sql(sql)? {
            let result = self
                .call_procedure(&name, &args)
                .map(procedure_result_to_sql);
            return self.finish_query(sql, started, result);
        }
        if let Some(kind) = phase32_declaration_kind(sql) {
            return self.finish_query(
                sql,
                started,
                Err(QueryError::Unsupported(format!(
                    "{kind} SQL DDL is outside the current SQL support matrix; use the stable Database API"
                ))
                .into()),
            );
        }
        if let Some(command) = extension::parse_create_wasm_function(sql)? {
            let result = self
                .register_wasm_udf(&command.name, &command.wasm)
                .map(|_| SqlOutput::AffectedRows(1));
            return self.finish_query(sql, started, result);
        }
        if let Some(output) = compat::execute_compat_sql(
            sql,
            &self.config,
            &self.compat_catalog_snapshot(),
            &self.performance.query,
        )? {
            return self.finish_query(sql, started, Ok(output));
        }
        if let Some(output) = self.execute_tuning_system_sql(sql)? {
            return self.finish_query(sql, started, Ok(output));
        }
        if let Some(output) = self.execute_phase19_sql(sql)? {
            return self.finish_query(sql, started, Ok(output));
        }
        if let Some((name, args)) = extension::parse_select_udf_call(sql)? {
            let result = self.call_udf(&name, &args).map(|value| {
                SqlOutput::Rows(crate::query::SqlRows {
                    columns: vec![name],
                    rows: vec![vec![value]],
                })
            });
            return self.finish_query(sql, started, result);
        }
        let engine = self.sql_engine()?;
        let result = engine
            .execute_with_read_guard(sql, guard)
            .await
            .map_err(DbError::from);
        self.finish_query(sql, started, result)
    }

    /// Executes a SQL query against an MVCC snapshot.
    /// # Errors
    /// Fails when the temporal point is outside retention, unsupported, or execution fails.
    pub async fn query_as_of(&self, sql: &str, point: TemporalPoint) -> Result<SqlOutput, DbError> {
        self.query_as_of_with_read_guard(sql, point, None).await
    }

    async fn query_as_of_with_read_guard(
        &self,
        sql: &str,
        point: TemporalPoint,
        guard: Option<&mut dyn SqlReadGuard>,
    ) -> Result<SqlOutput, DbError> {
        validate_sql_text_size(sql, &self.performance.query)?;
        if self.repl.is_ap() {
            return Err(TemporalError::Unsupported(
                "AP replication does not support temporal AS OF".to_owned(),
            )
            .into());
        }
        if self.repl.is_sharded() {
            return Err(TemporalError::Unsupported(
                "sharded replication does not support temporal AS OF".to_owned(),
            )
            .into());
        }
        let lsn = crate::temporal::resolve_temporal_point(self.replication_ref(), point)?;
        self.validate_temporal_retention_for_sql(sql, lsn)?;
        let _history_pin = self.pin_history_lsn(lsn)?;
        let engine = self.sql_engine()?.as_ref().clone().with_snapshot_lsn(lsn);
        Ok(engine.execute_with_read_guard(sql, guard).await?)
    }

    fn finish_query(
        &self,
        sql: &str,
        started: Instant,
        result: Result<SqlOutput, DbError>,
    ) -> Result<SqlOutput, DbError> {
        let outcome = if result.is_ok() { "ok" } else { "error" };
        observability::record_query(started, outcome);
        if let Ok(output) = &result
            && self
                .record_workload_observation(sql, started.elapsed(), output)
                .is_err()
        {
            tracing::debug!("failed to record workload observation");
        }
        result
    }

    fn record_workload_observation(
        &self,
        sql: &str,
        elapsed: std::time::Duration,
        output: &SqlOutput,
    ) -> Result<(), DbError> {
        if tuning::parse_system_view(sql)?.is_some() {
            return Ok(());
        }
        let sample = tuning::output_to_workload_sample(sql, elapsed, output);
        Ok(self.workload.record(self.replication_ref(), &sample)?)
    }

    fn flush_workload_samples_before_advice(&self) {
        // Advice falls back to persisted statistics when the flush fails, as it did when
        // per-statement workload writes failed.
        if self.flush_workload_samples().is_err() {
            tracing::debug!("failed to flush workload observations before advice");
        }
    }

    fn execute_tuning_system_sql(&self, sql: &str) -> Result<Option<SqlOutput>, DbError> {
        let Some(view) = tuning::parse_system_view(sql)? else {
            return Ok(None);
        };
        Ok(Some(match view {
            TuningSystemView::Workload => {
                self.workload_report(WorkloadWindow::All)?.to_sql_output()
            }
            TuningSystemView::TuningLog => TuningLogEntry::to_sql_output(&self.tuning_log()?),
            TuningSystemView::IndexAdvice => self.index_advice()?.to_sql_output(),
            TuningSystemView::ReprofileJobs => ReprofileJob::to_sql_output(&self.reprofile_jobs()?),
        }))
    }

    fn execute_phase19_sql(&self, sql: &str) -> Result<Option<SqlOutput>, DbError> {
        let Some(call) = parse_phase19_call(sql)? else {
            return Ok(None);
        };

        match call.name.as_str() {
            "match" => {
                if call.args.len() < 2 || call.args.len() > 3 {
                    return Err(QueryError::Unsupported(sql.to_owned()).into());
                }
                let index = self.full_text_index(call.args[0].as_str()?)?;
                let limit = call
                    .args
                    .get(2)
                    .map_or(Ok(10_usize), Phase19Arg::as_usize)?;
                let rows = index
                    .search(call.args[1].as_str()?, limit)?
                    .into_iter()
                    .map(|hit| {
                        vec![
                            Value::Bytes(hit.id.as_bytes().to_vec()),
                            Value::Float(hit.score),
                            Value::Str(hit.text),
                        ]
                    })
                    .collect();
                Ok(Some(SqlOutput::Rows(crate::query::SqlRows {
                    columns: vec!["id".to_owned(), "score".to_owned(), "text".to_owned()],
                    rows,
                })))
            }
            "time_bucket" => {
                if call.args.len() != 2 {
                    return Err(QueryError::Unsupported(sql.to_owned()).into());
                }
                let bucket = time_bucket(call.args[0].as_i64()?, call.args[1].as_i64()?)?;
                Ok(Some(SqlOutput::Rows(crate::query::SqlRows {
                    columns: vec!["bucket".to_owned()],
                    rows: vec![vec![Value::Int(bucket)]],
                })))
            }
            "within_radius" => {
                if call.args.len() != 4 {
                    return Err(QueryError::Unsupported(sql.to_owned()).into());
                }
                let index = self.geo_index(call.args[0].as_str()?)?;
                let center = GeoPoint::new(call.args[1].as_f64()?, call.args[2].as_f64()?)?;
                let rows = index
                    .within_radius(center, call.args[3].as_f64()?)?
                    .into_iter()
                    .map(|hit| {
                        vec![
                            Value::Bytes(hit.id.as_bytes().to_vec()),
                            Value::GeoPoint {
                                lon: hit.point.lon,
                                lat: hit.point.lat,
                            },
                            Value::Float(hit.distance_meters),
                        ]
                    })
                    .collect();
                Ok(Some(SqlOutput::Rows(crate::query::SqlRows {
                    columns: vec![
                        "id".to_owned(),
                        "point".to_owned(),
                        "distance_meters".to_owned(),
                    ],
                    rows,
                })))
            }
            "graph_neighbors" => {
                if call.args.len() != 4 {
                    return Err(QueryError::Unsupported(sql.to_owned()).into());
                }
                let graph = self.graph(call.args[0].as_str()?)?;
                let start = GraphNodeId::from_str_id(call.args[1].as_str()?);
                let depth = call.args[3].as_usize()?;
                let rows = graph
                    .k_hop(
                        &start,
                        call.args[2].as_str()?,
                        TraversalOptions {
                            max_depth: depth,
                            max_expansion: 10_000,
                        },
                    )?
                    .into_iter()
                    .map(|node| {
                        vec![Value::Str(
                            String::from_utf8_lossy(node.as_bytes()).into_owned(),
                        )]
                    })
                    .collect();
                Ok(Some(SqlOutput::Rows(crate::query::SqlRows {
                    columns: vec!["node".to_owned()],
                    rows,
                })))
            }
            _ => Ok(None),
        }
    }

    /// Runs SQL after checking RBAC for the referenced resources.
    /// # Errors
    /// Fails when authorization, catalog metadata, or query execution fails.
    pub async fn query_as(&self, principal: &Principal, sql: &str) -> Result<SqlOutput, DbError> {
        let requirements = sql_requirements(sql, &self.catalog, &self.performance.query)?;
        for (resource, permission) in &requirements {
            self.authorize_or_audit(principal, resource, *permission, "query")?;
        }

        let result = match self.sql_read_authorizer(principal, "query", &requirements) {
            Ok(mut authorizer) => self.query_with_read_guard(sql, Some(&mut authorizer)).await,
            Err(error) => Err(error),
        };
        let detail = format!("statements: {}", requirements.len());
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "query",
            Resource::Database,
            if result.is_ok() {
                AuditOutcome::Succeeded
            } else {
                AuditOutcome::Failed
            },
            Some(&detail),
        ))?;
        result
    }

    /// Executes a temporal query after checking RBAC and applying row/masking policies.
    /// # Errors
    /// Fails when authorization, temporal resolution, query execution, or policy masking fails.
    pub async fn query_as_of_as(
        &self,
        principal: &Principal,
        sql: &str,
        point: TemporalPoint,
    ) -> Result<SqlOutput, DbError> {
        let requirements = sql_requirements(sql, &self.catalog, &self.performance.query)?;
        for (resource, permission) in &requirements {
            self.authorize_or_audit(principal, resource, *permission, "query_as_of")?;
        }

        let result = match self.sql_read_authorizer(principal, "query_as_of", &requirements) {
            Ok(mut authorizer) => {
                self.query_as_of_with_read_guard(sql, point, Some(&mut authorizer))
                    .await
            }
            Err(error) => Err(error),
        };
        let detail = format!("statements: {}", requirements.len());
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "query_as_of",
            Resource::Database,
            if result.is_ok() {
                AuditOutcome::Succeeded
            } else {
                AuditOutcome::Failed
            },
            Some(&detail),
        ))?;
        result
    }

    /// Builds the read guard of SQL run as `principal`, carrying the row and masking policies
    /// the engine applies where it scans each catalog object.
    /// Index searches return source documents without going through the engine, so they are
    /// refused while the source collection has row or masking policies.
    fn sql_read_authorizer<'a>(
        &'a self,
        principal: &'a Principal,
        action: &'static str,
        requirements: &[(Resource, Permission)],
    ) -> Result<SqlReadAuthorizer<'a>, DbError> {
        let policy = self.policy_config()?;
        if requirements.iter().any(|(resource, permission)| {
            *permission == Permission::Read
                && matches!(resource, Resource::Graph(_))
                && policy_has_row_or_masking(&policy, resource)
        }) {
            return Err(ExtensionError::PolicyDenied(
                "graph traversals with row or masking policies are not supported".to_owned(),
            )
            .into());
        }
        let searches_index = requirements.iter().any(|(resource, _)| {
            matches!(resource, Resource::FullTextIndex(_) | Resource::GeoIndex(_))
        });
        if searches_index
            && policy_resources(requirements)
                .iter()
                .any(|resource| policy_has_row_or_masking(&policy, resource))
        {
            return Err(ExtensionError::PolicyDenied(
                "index searches over collections with row or masking policies are not supported"
                    .to_owned(),
            )
            .into());
        }
        Ok(SqlReadAuthorizer {
            database: self,
            principal,
            action,
            policies: sql_relation_policies(&self.catalog, &policy, principal),
            protected_views: review_fixes::protected_materialized_views(&self.catalog, &policy),
        })
    }

    /// Checks RBAC for a changefeed poll: `System:Admin` first, audited, when the poll can return
    /// system rows, then `Read` on the filter target.
    fn authorize_changefeed_poll(
        &self,
        principal: &Principal,
        filter: &ChangefeedFilter,
        options: &ChangefeedOptions,
    ) -> Result<(), DbError> {
        if cdc::reads_system_rows(filter, options) {
            self.authorize_or_audit(
                principal,
                &Resource::System,
                Permission::Admin,
                "poll_changefeed",
            )?;
        }
        cdc::authorize_filter_access(&self.authz, principal, filter).map_err(DbError::from)
    }

    /// The polled events `principal` may see: system rows only with `System:Admin`, whatever
    /// the poll's options, and user rows after row and masking policies.
    fn changefeed_events_visible_to(
        &self,
        principal: &Principal,
        events: Vec<ChangeEvent>,
    ) -> Result<Vec<ChangeEvent>, DbError> {
        let policy = self.policy_config()?;
        let events = cdc::retain_events_visible_to(&self.authz, principal, events)
            .into_iter()
            .map(|event| self.apply_changefeed_policy(principal, &policy, event))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(events.into_iter().flatten().collect())
    }

    fn apply_changefeed_policy(
        &self,
        principal: &Principal,
        policy: &PolicyConfig,
        mut event: ChangeEvent,
    ) -> Result<Option<ChangeEvent>, DbError> {
        if !self.index_changefeed_event_visible_to(principal, policy, &event) {
            return Ok(None);
        }
        if !self.primary_key_changefeed_event_visible(policy, &event)? {
            return Ok(None);
        }
        let Some(resource) = resource_for_changefeed_target(&event.target) else {
            return Ok(Some(event));
        };
        if !policy_has_row_or_masking(policy, &resource) {
            return Ok(Some(event));
        }

        // Delete records carry no old row. Its visibility and masked primary
        // key cannot be reconstructed safely, so protected feeds omit it.
        let ChangeOp::Upsert { value_after, key } = event.op else {
            return Ok(None);
        };

        let masked = match &event.target {
            LogicalTarget::Table(name) => {
                let table = self.table(name)?;
                let schema = table.schema().ok_or_else(|| {
                    ExtensionError::PolicyDenied(format!(
                        "changefeed masking for table {name} requires schema"
                    ))
                })?;
                let Some(row) = decode_row_bytes(&value_after)? else {
                    return Err(ExtensionError::PolicyDenied(format!(
                        "changefeed value for table {name} cannot be decoded"
                    ))
                    .into());
                };
                let columns = schema
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect::<Vec<_>>();
                let object = row_to_policy_object(&columns, &row);
                if !policy.row_visible(principal, &resource, &object) {
                    return Ok(None);
                }
                let masked = policy.mask_value(&resource, &object);
                encode_value(&Value::Array(policy_object_to_row(&columns, &masked, row)))?
            }
            LogicalTarget::Collection(_) => {
                let value = decode_value(&value_after)?;
                if !policy.row_visible(principal, &resource, &value) {
                    return Ok(None);
                }
                encode_value(&policy.mask_value(&resource, &value))?
            }
            _ => {
                return Err(ExtensionError::PolicyDenied(format!(
                    "changefeed policy on {resource:?} is not supported"
                ))
                .into());
            }
        };

        event.op = ChangeOp::Upsert {
            key,
            value_after: masked,
        };
        Ok(Some(event))
    }

    /// Reads sanitized audit events.
    /// # Errors
    /// Fails when audit storage or decoding fails.
    pub fn audit_events(&self) -> Result<Vec<AuditEvent>, DbError> {
        self.repl
            .range(AUDIT_TABLE, &[], &[0xFF], ReadConsistency::Strong)?
            .into_iter()
            .map(|(_, value)| {
                serde_json::from_slice(&value).map_err(|error| DbError::Serde(error.to_string()))
            })
            .collect()
    }

    /// Reads sanitized audit events after checking system admin permission.
    /// # Errors
    /// Fails when authorization or audit storage fails.
    pub fn audit_events_as(&self, principal: &Principal) -> Result<Vec<AuditEvent>, DbError> {
        self.authorize_or_audit(
            principal,
            &Resource::System,
            Permission::Admin,
            "audit_events",
        )?;
        self.audit_events()
    }

    /// Verifies the tamper-evident audit hash chain.
    /// # Errors
    /// Fails when an audit row is missing integrity metadata, has been changed, or the chain is broken.
    pub fn verify_audit_chain(&self) -> Result<(), DbError> {
        let _audit_guard = self
            .audit_lock
            .lock()
            .map_err(|_| DbError::AuditIntegrity("audit lock poisoned".to_owned()))?;
        let key = self.audit_key()?;
        let mut events = self.audit_events()?;
        events.sort_by_key(|event| {
            event
                .integrity
                .as_ref()
                .map_or(0, |integrity| integrity.sequence)
        });
        let mut previous_hash = None;
        let mut expected_sequence = 0_u64;
        for event in events {
            expected_sequence = expected_sequence
                .checked_add(1)
                .ok_or_else(|| DbError::AuditIntegrity("audit sequence overflow".to_owned()))?;
            event
                .verify_integrity_link(previous_hash.as_deref(), expected_sequence, &key)
                .map_err(DbError::AuditIntegrity)?;
            previous_hash = event
                .integrity
                .as_ref()
                .map(|integrity| integrity.hash.clone());
        }
        let head = self.audit_head()?;
        if head.sequence != expected_sequence || head.hash != previous_hash {
            return Err(DbError::AuditIntegrity(
                "audit head does not match event chain".to_owned(),
            ));
        }
        Ok(())
    }

    /// Runs a document predicate against a cataloged collection.
    /// # Errors
    /// Fails when the collection is missing or the document query fails.
    pub fn find(&self, collection: &str, predicate: &Predicate) -> Result<QueryResult, DbError> {
        Ok(self.collection(collection)?.query(predicate)?)
    }

    /// Reads AP siblings for a conflicted key.
    /// # Errors
    /// Fails when the database is not AP-backed or the AP backend rejects the read.
    pub fn read_conflict_versions(
        &self,
        table: &str,
        key: &[u8],
    ) -> Result<Vec<VersionedBytes>, DbError> {
        self.repl.read_conflict_versions(table, key)
    }

    /// Resolves AP siblings by writing a version that dominates the supplied parent clocks.
    /// # Errors
    /// Fails when the database is not AP-backed or the AP backend rejects the write.
    pub fn resolve_conflict(
        &self,
        table: &str,
        key: &[u8],
        value: Bytes,
        parents: Vec<VectorClock>,
    ) -> Result<(), DbError> {
        self.repl.resolve_conflict(table, key, value, parents)
    }

    /// Starts a snapshot transaction.
    /// # Errors
    /// Fails when the storage snapshot or transaction metadata cannot be read.
    pub fn begin_transaction(&self) -> Result<MultiModelTxn<'_>, DbError> {
        self.begin_transaction_with_options(TxnOptions::default())
    }

    /// Starts a transaction with explicit isolation options.
    /// # Errors
    /// Fails when the storage snapshot or transaction metadata cannot be read.
    pub fn begin_transaction_with_options(
        &self,
        options: TxnOptions,
    ) -> Result<MultiModelTxn<'_>, DbError> {
        if self.repl.is_ap() {
            return Err(ConfigError::Unsupported(
                "AP replication does not support snapshot transactions".to_owned(),
            )
            .into());
        }

        if self.repl.is_sharded() {
            return Err(ConfigError::Unsupported(
                "sharded replication does not support snapshot transactions".to_owned(),
            )
            .into());
        }

        let storage = self.repl.storage().ok_or_else(|| {
            ConfigError::Unsupported("replication backend has no local snapshot storage".to_owned())
        })?;
        let read = storage.begin_read()?;
        let snapshot_id = txn::current_txn_id_from(&read)?;

        Ok(MultiModelTxn {
            database: self,
            snapshot_id,
            read,
            options: TxnOptions {
                isolation: normalize_isolation(options.isolation),
                ..options
            },
            write_set: WriteSet::new(),
            read_keys: RefCell::new(BTreeSet::new()),
            read_ranges: RefCell::new(Vec::new()),
            savepoints: BTreeMap::new(),
        })
    }

    /// Runs one transactional attempt over relational tables and document collections.
    /// # Errors
    /// Returns the closure error without writing anything, or a commit error: a
    /// conflict or storage failure, a before trigger or before-commit hook
    /// rejection, or an exceeded tenant quota. Nothing is written in any of these
    /// cases; after-commit failures are recorded instead of returned.
    pub fn transaction<T>(
        &self,
        f: impl FnOnce(&mut MultiModelTxn<'_>) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        self.transaction_with_options(TxnOptions::default(), f)
    }

    /// Runs one transactional attempt with explicit isolation options.
    /// # Errors
    /// Returns the closure error without writing anything, or a commit error: a
    /// conflict or storage failure, a before trigger or before-commit hook
    /// rejection, or an exceeded tenant quota. Nothing is written in any of these
    /// cases; after-commit failures are recorded instead of returned.
    pub fn transaction_with_options<T>(
        &self,
        options: TxnOptions,
        f: impl FnOnce(&mut MultiModelTxn<'_>) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        let mut txn = self.begin_transaction_with_options(options)?;
        let value = f(&mut txn)?;
        txn.commit()?;
        Ok(value)
    }

    /// Retries a transaction when optimistic validation detects a write conflict.
    /// # Errors
    /// Fails when the closure or storage returns a non-retryable error, or retries are exhausted.
    pub fn transaction_with_retry<T>(
        &self,
        max_retries: u32,
        f: impl FnMut(&mut MultiModelTxn<'_>) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        self.transaction_with_retry_with_options(
            TxnOptions {
                max_retries,
                ..TxnOptions::default()
            },
            f,
        )
    }

    /// Retries a transaction with explicit isolation options when conflict validation aborts.
    /// # Errors
    /// Fails when the closure or storage returns a non-retryable error, or retries are exhausted.
    pub fn transaction_with_retry_with_options<T>(
        &self,
        options: TxnOptions,
        mut f: impl FnMut(&mut MultiModelTxn<'_>) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        let mut attempts = 0;

        loop {
            match self.transaction_with_options(options, |txn| f(txn)) {
                Err(error) if error.is_conflict() && attempts < options.max_retries => {
                    attempts += 1;
                    thread::yield_now();
                }
                result => return result,
            }
        }
    }

    fn new_with_performance(
        config: DbConfig,
        engine: AnyEngine,
        catalog: BTreeMap<String, CatalogEntry>,
        performance: PerformanceConfig,
    ) -> Result<Self, ConfigError> {
        let performance_cache = PerformanceCache::new(&performance.cache);
        let query_permits = query_permits_for(&performance);
        let wasm_runtime = wasm_runtime_for_database()?;
        Ok(Self {
            config,
            repl: DatabaseRepl::SingleNode(Arc::new(SingleNode::with_group_commit_window(
                engine,
                performance.group_commit_window(),
            ))),
            catalog,
            authz: AuthzPolicy::default(),
            principals: PrincipalRegistry::default(),
            audit_enabled: true,
            audit_config: AuditConfig::default(),
            audit_lock: Arc::new(Mutex::new(())),
            audit_batch: Arc::new(audit_batch::AuditBatchCoordinator::default()),
            history_pins: Arc::new(Mutex::new(txn::ActiveSnapshotRegistry::default())),
            encryption: None,
            plan_cache: Arc::new(Mutex::new(PlanCache::default())),
            sql_catalog_cache: sql_cache::SqlCatalogCache::default(),
            performance,
            performance_cache,
            query_permits,
            query_governor_overrides: QueryGovernorOverrides::default(),
            tenant: None,
            wasm_runtime,
            vector_indexes: Arc::new(Mutex::new(BTreeMap::new())),
            workload: WorkloadBuffer::default(),
        })
    }

    fn new_cp(
        config: DbConfig,
        engine: AnyEngine,
        catalog: BTreeMap<String, CatalogEntry>,
        cluster: CpClusterConfig,
    ) -> Result<Self, ConfigError> {
        let performance = PerformanceConfig::for_profile(config.profile);
        let performance_cache = PerformanceCache::new(&performance.cache);
        let query_permits = query_permits_for(&performance);
        let wasm_runtime = wasm_runtime_for_database()?;
        Ok(Self {
            config,
            repl: DatabaseRepl::CpRaft(Arc::new(CpRaft::new_unchecked(engine, cluster))),
            catalog,
            authz: AuthzPolicy::default(),
            principals: PrincipalRegistry::default(),
            audit_enabled: true,
            audit_config: AuditConfig::default(),
            audit_lock: Arc::new(Mutex::new(())),
            audit_batch: Arc::new(audit_batch::AuditBatchCoordinator::default()),
            history_pins: Arc::new(Mutex::new(txn::ActiveSnapshotRegistry::default())),
            encryption: None,
            plan_cache: Arc::new(Mutex::new(PlanCache::default())),
            sql_catalog_cache: sql_cache::SqlCatalogCache::default(),
            performance,
            performance_cache,
            query_permits,
            query_governor_overrides: QueryGovernorOverrides::default(),
            tenant: None,
            wasm_runtime,
            vector_indexes: Arc::new(Mutex::new(BTreeMap::new())),
            workload: WorkloadBuffer::default(),
        })
    }

    fn new_ap(
        config: DbConfig,
        engine: AnyEngine,
        catalog: BTreeMap<String, CatalogEntry>,
        cluster: ApClusterConfig,
    ) -> Result<Self, ConfigError> {
        let performance = PerformanceConfig::for_profile(config.profile);
        let performance_cache = PerformanceCache::new(&performance.cache);
        let query_permits = query_permits_for(&performance);
        let wasm_runtime = wasm_runtime_for_database()?;
        Ok(Self {
            config,
            repl: DatabaseRepl::ApDynamo(Arc::new(ApDynamo::new_unchecked(engine, cluster))),
            catalog,
            authz: AuthzPolicy::default(),
            principals: PrincipalRegistry::default(),
            audit_enabled: true,
            audit_config: AuditConfig::default(),
            audit_lock: Arc::new(Mutex::new(())),
            audit_batch: Arc::new(audit_batch::AuditBatchCoordinator::default()),
            history_pins: Arc::new(Mutex::new(txn::ActiveSnapshotRegistry::default())),
            encryption: None,
            plan_cache: Arc::new(Mutex::new(PlanCache::default())),
            sql_catalog_cache: sql_cache::SqlCatalogCache::default(),
            performance,
            performance_cache,
            query_permits,
            query_governor_overrides: QueryGovernorOverrides::default(),
            tenant: None,
            wasm_runtime,
            vector_indexes: Arc::new(Mutex::new(BTreeMap::new())),
            workload: WorkloadBuffer::default(),
        })
    }

    fn new_sharded(
        config: DbConfig,
        repl: Arc<ShardedReplication>,
        catalog: BTreeMap<String, CatalogEntry>,
    ) -> Result<Self, ConfigError> {
        let performance = PerformanceConfig::for_profile(config.profile);
        let performance_cache = PerformanceCache::new(&performance.cache);
        let query_permits = query_permits_for(&performance);
        let wasm_runtime = wasm_runtime_for_database()?;
        Ok(Self {
            config,
            repl: DatabaseRepl::Sharded(repl),
            catalog,
            authz: AuthzPolicy::default(),
            principals: PrincipalRegistry::default(),
            audit_enabled: true,
            audit_config: AuditConfig::default(),
            audit_lock: Arc::new(Mutex::new(())),
            audit_batch: Arc::new(audit_batch::AuditBatchCoordinator::default()),
            history_pins: Arc::new(Mutex::new(txn::ActiveSnapshotRegistry::default())),
            encryption: None,
            plan_cache: Arc::new(Mutex::new(PlanCache::default())),
            sql_catalog_cache: sql_cache::SqlCatalogCache::default(),
            performance,
            performance_cache,
            query_permits,
            query_governor_overrides: QueryGovernorOverrides::default(),
            tenant: None,
            wasm_runtime,
            vector_indexes: Arc::new(Mutex::new(BTreeMap::new())),
            workload: WorkloadBuffer::default(),
        })
    }

    async fn acquire_query_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, QueryError> {
        tokio::time::timeout(
            std::time::Duration::from_millis(self.performance.query.queue_timeout_ms),
            self.query_permits.clone().acquire_owned(),
        )
        .await
        .map_err(|_| {
            QueryError::Busy(format!(
                "query queue exceeded {} ms",
                self.performance.query.queue_timeout_ms
            ))
        })?
        .map_err(|_| QueryError::ResourceLimit("query governor closed".to_owned()))
    }

    /// Returns the handle given to `RelTable` and `SqlEngine`. Its writes run the
    /// same write pipeline as `Database` writes (triggers, hooks, tenant quotas).
    fn replication_handle(&self) -> Arc<dyn Replication> {
        Arc::new(self.write_pipeline())
    }

    fn write_pipeline(&self) -> WritePipeline {
        WritePipeline {
            tensor_only: self.profile().requires_gpu(),
            typed_tensor_write: false,
            backend: self.repl.replication_handle(),
            tenant: self.tenant.clone(),
            wasm_runtime: Arc::clone(&self.wasm_runtime),
            sql_schema_epoch: Arc::clone(&self.sql_catalog_cache.schema_epoch),
        }
    }

    fn replication_ref(&self) -> &dyn Replication {
        self.repl.replication_ref()
    }

    fn open_vector_collection_with_config(
        &self,
        config: VectorCollectionConfig,
    ) -> Result<VectorCollection<'_, dyn Replication + '_>, DbError> {
        let state = self.shared_vector_state(&config)?;
        let repl: &dyn Replication = self;
        Ok(VectorCollection::with_shared_state(repl, config, state)?)
    }

    fn shared_vector_state(
        &self,
        config: &VectorCollectionConfig,
    ) -> Result<SharedVectorIndex, DbError> {
        let mut indexes = self
            .vector_indexes
            .lock()
            .map_err(|_| DbError::Vector(VectorError::LockPoisoned))?;
        Ok(indexes
            .entry(config.collection_id)
            .or_insert_with(|| {
                Arc::new(RwLock::new(VectorIndexState::new(
                    config.metric,
                    config.hnsw,
                    0,
                )))
            })
            .clone())
    }

    fn build_sql_engine(&self) -> Result<SqlEngine, DbError> {
        let mut engine = SqlEngine::with_layout_profile_cache_and_performance(
            self.replication_handle(),
            self.layout(),
            cost_profile_for(self.config.profile),
            self.plan_cache.clone(),
            self.performance.clone(),
        );

        for (name, entry) in &self.catalog {
            match entry {
                CatalogEntry::Table { indexes, layout } => {
                    engine.open_table_with_layout(name, indexes.clone(), *layout)?;
                }
                CatalogEntry::Collection {
                    collection_id,
                    fields,
                    indexes,
                } => {
                    engine.register_collection(
                        name,
                        *collection_id,
                        fields.clone(),
                        indexes.clone(),
                    )?;
                }
                CatalogEntry::ForeignTable { spec } => {
                    engine.register_foreign_table(name, spec.clone())?;
                }
                CatalogEntry::MaterializedView {
                    spec,
                    source_schema,
                } => {
                    engine.register_materialized_view(name, spec.clone(), source_schema)?;
                }
                CatalogEntry::TemporalTable {
                    base_table,
                    schema,
                    retention,
                } => {
                    engine.register_temporal_table(
                        name,
                        base_table.clone(),
                        schema.clone(),
                        *retention,
                    )?;
                }
                CatalogEntry::Vector { .. }
                | CatalogEntry::FullTextIndex { .. }
                | CatalogEntry::TimeSeries { .. }
                | CatalogEntry::Graph { .. }
                | CatalogEntry::GeoIndex { .. }
                | CatalogEntry::TensorDataset { .. } => {}
            }
        }

        Ok(engine)
    }

    fn validate_temporal_retention_for_sql(&self, sql: &str, lsn: TxnId) -> Result<(), DbError> {
        let statements = parse_with_limits(sql, &self.performance.query)?;
        let mut names = BTreeSet::new();
        for statement in statements {
            if let Statement::Query(query) = statement {
                collect_query_tables(&query, &mut names)?;
            }
        }
        for name in names {
            let name = sql_relation_catalog_name(&self.catalog, &name);
            for entry in self.catalog.values() {
                if let CatalogEntry::TemporalTable {
                    base_table,
                    retention,
                    ..
                } = entry
                    && base_table == &name
                {
                    crate::temporal::validate_retention(*retention, lsn)?;
                }
            }
        }
        Ok(())
    }

    fn invalidate_plan_cache(&self) {
        self.sql_catalog_cache.invalidate();
        if let Ok(mut cache) = self.plan_cache.lock() {
            cache.invalidate_all();
        }
        self.performance_cache.invalidate_all();
    }

    fn catalog_index_map(&self) -> BTreeMap<String, Vec<RelIndexSpec>> {
        self.catalog
            .iter()
            .filter_map(|(name, entry)| match entry {
                CatalogEntry::Table { indexes, .. } => Some((name.clone(), indexes.clone())),
                CatalogEntry::Collection { .. }
                | CatalogEntry::ForeignTable { .. }
                | CatalogEntry::MaterializedView { .. }
                | CatalogEntry::TemporalTable { .. }
                | CatalogEntry::Vector { .. }
                | CatalogEntry::FullTextIndex { .. }
                | CatalogEntry::TimeSeries { .. }
                | CatalogEntry::Graph { .. }
                | CatalogEntry::GeoIndex { .. }
                | CatalogEntry::TensorDataset { .. } => None,
            })
            .collect()
    }

    fn compat_catalog_snapshot(&self) -> PgCatalogSnapshot {
        let schemas = self
            .catalog
            .iter()
            .filter_map(|(name, entry)| match entry {
                CatalogEntry::Table { .. } => self
                    .table(name)
                    .ok()
                    .and_then(|table| table.schema().cloned())
                    .map(|schema| (name.clone(), schema)),
                CatalogEntry::ForeignTable { spec } => Some((name.clone(), spec.schema.clone())),
                CatalogEntry::TemporalTable { schema, .. } => Some((
                    name.clone(),
                    crate::temporal::system_versioned_schema(schema),
                )),
                CatalogEntry::Collection { .. }
                | CatalogEntry::MaterializedView { .. }
                | CatalogEntry::Vector { .. }
                | CatalogEntry::FullTextIndex { .. }
                | CatalogEntry::TimeSeries { .. }
                | CatalogEntry::Graph { .. }
                | CatalogEntry::GeoIndex { .. }
                | CatalogEntry::TensorDataset { .. } => None,
            })
            .collect::<BTreeMap<_, _>>();
        PgCatalogSnapshot::from_catalog(&self.catalog, &schemas)
    }

    fn authorize_or_audit(
        &self,
        principal: &Principal,
        resource: &Resource,
        permission: Permission,
        action: &str,
    ) -> Result<(), DbError> {
        self.authorize_with_audit(principal, resource, permission, action)
            .map_err(DbError::AuthzDenied)
    }

    fn authorize_with_audit(
        &self,
        principal: &Principal,
        resource: &Resource,
        permission: Permission,
        action: &str,
    ) -> Result<(), AuthzError> {
        match self.authz.authorize(principal, resource, permission) {
            Ok(()) => Ok(()),
            Err(error) => {
                let detail = format!("required: {permission:?}");
                let event = AuditEvent::new(
                    Some(principal),
                    action,
                    resource.clone(),
                    AuditOutcome::Denied,
                    Some(&detail),
                );
                if self.record_audit(&event).is_err() {
                    tracing::warn!("failed to write denied authorization audit event");
                }
                observability::record_authz_denied(action, resource, permission);
                Err(error)
            }
        }
    }

    fn record_audit(&self, event: &AuditEvent) -> Result<(), DbError> {
        if !self.audit_enabled {
            return Ok(());
        }
        self.audit_batch
            .record(event, |events| self.persist_audit_batch(events))
    }

    fn audit_head(&self) -> Result<AuditHead, DbError> {
        let Some(bytes) = self
            .repl
            .read(AUDIT_HEAD_TABLE, b"head", ReadConsistency::Strong)?
        else {
            return Ok(AuditHead::default());
        };
        serde_json::from_slice(&bytes).map_err(|error| DbError::Serde(error.to_string()))
    }

    fn audit_key(&self) -> Result<[u8; 32], DbError> {
        let Some(path) = &self.audit_config.key_path else {
            return Ok(blake3::derive_key(
                "multidb default audit integrity key",
                b"development default audit key",
            ));
        };
        let bytes = std::fs::read(path).map_err(StorageError::Io)?;
        parse_audit_key_file(&bytes).map_err(DbError::AuditIntegrity)
    }

    fn persist_security_state(&self) -> Result<(), DbError> {
        write_security_to_repl(
            self.replication_ref(),
            &SecurityConfig {
                authz_policy: self.authz.clone(),
                principals: self.principals.clone(),
                audit_enabled: self.audit_enabled,
                audit: self.audit_config.clone(),
            },
        )
        .map_err(DbError::Config)
    }

    fn apply_security_config(&mut self, security: SecurityConfig) {
        self.authz = security.authz_policy;
        self.principals = security.principals;
        self.audit_enabled = security.audit_enabled;
        self.audit_config = security.audit;
    }

    /// Restores the persisted RBAC policy, principal registry, and audit flag when present.
    fn load_persisted_security(&mut self) -> Result<(), ConfigError> {
        if let Some(security) = read_security_from_repl(self.replication_ref())? {
            self.apply_security_config(security);
        }
        Ok(())
    }

    fn apply_tenant_config(&mut self, tenant: Option<TenantConfig>) {
        self.tenant = tenant.map(|tenant| {
            let runtime = TenantRuntime::new(tenant);
            runtime.reconcile_storage_bytes(self.estimate_tenant_storage_bytes());
            runtime
        });
        self.invalidate_plan_cache();
    }

    fn estimate_tenant_storage_bytes(&self) -> u64 {
        [
            CATALOG_TABLE,
            REL_ROWS_TABLE,
            REL_COLUMNAR_SEGMENTS_TABLE,
            DOCUMENT_TABLE,
            crate::tensor::TENSOR_CHUNKS_TABLE,
            crate::tensor::TENSOR_META_TABLE,
            crate::tensor::TENSOR_SAMPLE_INDEX_TABLE,
            crate::tensor::TENSOR_REVISIONS_TABLE,
            AUDIT_TABLE,
            AUDIT_HEAD_TABLE,
            ADMIN_AUTH_TABLE,
        ]
        .into_iter()
        .filter_map(|table| {
            self.repl
                .replication_ref()
                .range(table, &[], &[0xFF], ReadConsistency::Strong)
                .ok()
                .map(|rows| (table, rows))
        })
        .flat_map(|(table, rows)| {
            rows.into_iter().map(move |(key, value)| {
                u64::try_from(
                    table
                        .len()
                        .saturating_add(key.len())
                        .saturating_add(value.len()),
                )
                .unwrap_or(u64::MAX)
            })
        })
        .sum()
    }

    fn ensure_non_tensor_model_allowed(&self, operation: &str) -> Result<(), DbError> {
        if self.profile().requires_gpu() {
            return Err(ConfigError::Unsupported(format!(
                "{operation} is unavailable in the Tensor profile; create a typed tensor dataset"
            ))
            .into());
        }
        Ok(())
    }

    fn ensure_name_available(&self, name: &str) -> Result<(), DbError> {
        validate_catalog_name(name)?;
        if self.catalog.contains_key(name) {
            return Err(DbError::CatalogObjectExists(name.to_owned()));
        }

        Ok(())
    }

    fn ensure_collection_id_available(&self, collection_id: CollectionId) -> Result<(), DbError> {
        if self.catalog.values().any(|entry| {
            matches!(
                entry,
                CatalogEntry::Collection {
                    collection_id: existing,
                    ..
                } | CatalogEntry::Vector {
                    collection_id: existing,
                    ..
                } if *existing == collection_id
            )
        }) {
            return Err(DbError::CatalogObjectExists(format!(
                "collection id {}",
                collection_id.as_u32()
            )));
        }
        Ok(())
    }

    /// Requires `collection_id` to name an existing document collection.
    fn ensure_document_collection(&self, collection_id: CollectionId) -> Result<(), DbError> {
        for (name, entry) in &self.catalog {
            match entry {
                CatalogEntry::Collection {
                    collection_id: existing,
                    ..
                } if *existing == collection_id => return Ok(()),
                CatalogEntry::Vector {
                    collection_id: existing,
                    ..
                } if *existing == collection_id => {
                    return Err(kind_mismatch(name, "collection", entry));
                }
                _ => {}
            }
        }
        Err(DbError::MissingCatalogObject(format!(
            "collection id {}",
            collection_id.as_u32()
        )))
    }

    /// Collects the full-text and geo indexes whose source is `collection_id`.
    fn derived_indexes_for(&self, collection_id: CollectionId) -> DerivedIndexes {
        let mut derived = DerivedIndexes::default();
        for entry in self.catalog.values() {
            match entry {
                CatalogEntry::FullTextIndex { config } => {
                    let TextSource::Collection {
                        collection_id: source,
                        ..
                    } = &config.source;
                    if *source == collection_id {
                        derived.full_text.push(config.clone());
                    }
                }
                CatalogEntry::GeoIndex { config } if config.collection_id == collection_id => {
                    derived.geo.push(config.clone());
                }
                _ => {}
            }
        }
        derived
    }

    fn ensure_graph_id_available(&self, graph_id: GraphId) -> Result<(), DbError> {
        if self.catalog.values().any(|entry| {
            matches!(entry, CatalogEntry::Graph { graph_id: existing } if *existing == graph_id)
        }) {
            return Err(DbError::CatalogObjectExists(format!(
                "graph id {}",
                graph_id.as_u32()
            )));
        }
        Ok(())
    }

    fn next_collection_id_after(&self, collection_id: CollectionId) -> Result<u32, DbError> {
        let next = collection_id
            .as_u32()
            .checked_add(1)
            .ok_or_else(|| DbError::InvalidCatalogName("collection id overflow".to_owned()))?;
        Ok(next.max(self.catalog_next_collection_id()))
    }

    fn next_graph_id_after(&self, graph_id: GraphId) -> Result<u32, DbError> {
        let next = graph_id
            .as_u32()
            .checked_add(1)
            .ok_or_else(|| DbError::InvalidCatalogName("graph id overflow".to_owned()))?;
        Ok(next.max(self.catalog_next_graph_id()))
    }

    fn catalog_next_collection_id(&self) -> u32 {
        self.catalog
            .values()
            .filter_map(|entry| match entry {
                CatalogEntry::Collection { collection_id, .. }
                | CatalogEntry::Vector { collection_id, .. } => Some(collection_id.as_u32()),
                _ => None,
            })
            .max()
            .and_then(|id| id.checked_add(1))
            .unwrap_or(1)
    }

    fn catalog_next_graph_id(&self) -> u32 {
        self.catalog
            .values()
            .filter_map(|entry| match entry {
                CatalogEntry::Graph { graph_id } => Some(graph_id.as_u32()),
                _ => None,
            })
            .max()
            .and_then(|id| id.checked_add(1))
            .unwrap_or(1)
    }
}

pub struct MultiModelTxn<'db> {
    database: &'db Database,
    snapshot_id: TxnId,
    read: AnyReadTxn,
    options: TxnOptions,
    write_set: WriteSet,
    read_keys: RefCell<BTreeSet<WriteKey>>,
    read_ranges: RefCell<Vec<TxnReadRange>>,
    savepoints: BTreeMap<String, TxnSavepoint>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TxnReadRange {
    table: String,
    start: Bytes,
    end: Bytes,
}

#[derive(Clone, Debug)]
struct TxnSavepoint {
    write_set: WriteSet,
    read_keys: BTreeSet<WriteKey>,
    read_ranges: Vec<TxnReadRange>,
}

impl MultiModelTxn<'_> {
    #[must_use]
    pub const fn snapshot_id(&self) -> TxnId {
        self.snapshot_id
    }

    /// Reads one raw storage key from the transaction snapshot plus local writes.
    /// # Errors
    /// Fails when storage rejects the read.
    pub fn read_raw(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, DbError> {
        let write_key = (table.to_owned(), key.to_vec());
        if let Some(value) = self.write_set.get(&write_key) {
            return Ok(value.clone());
        }

        let value = if self.options.isolation == IsolationLevel::ReadCommitted {
            self.read_current_raw(table, key)?
        } else {
            self.read_snapshot_raw(table, key)?
        };
        self.record_read_key(table, key);
        Ok(value)
    }

    /// Reads one raw storage range from the transaction snapshot plus local writes.
    /// # Errors
    /// Fails when storage rejects the range read.
    pub fn range_raw(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Bytes, Bytes)>, DbError> {
        if !end.is_empty() && start >= end {
            return Ok(Vec::new());
        }

        let mut rows = BTreeMap::new();
        let base_rows = if self.options.isolation == IsolationLevel::ReadCommitted {
            self.range_current_raw(table, start, end)?
        } else {
            self.read
                .range(table, start, end)?
                .collect::<Result<Vec<_>, _>>()?
        };
        for (key, value) in base_rows {
            rows.insert(key, value);
        }

        for ((pending_table, key), value) in &self.write_set {
            if pending_table == table && key_in_range(key, start, end) {
                match value {
                    Some(bytes) => {
                        rows.insert(key.clone(), bytes.clone());
                    }
                    None => {
                        rows.remove(key);
                    }
                }
            }
        }

        let rows = rows.into_iter().collect::<Vec<_>>();
        self.record_read_range(table, start, end);
        for (key, _) in &rows {
            self.record_read_key(table, key);
        }
        Ok(rows)
    }

    /// Creates or replaces a savepoint for the current staged transaction state.
    pub fn savepoint(&mut self, name: impl Into<String>) {
        let name = name.into();
        self.savepoints.insert(
            name,
            TxnSavepoint {
                write_set: self.write_set.clone(),
                read_keys: self.read_keys.borrow().clone(),
                read_ranges: self.read_ranges.borrow().clone(),
            },
        );
    }

    /// Restores staged writes and serializable read tracking to a savepoint.
    /// # Errors
    /// Fails when the savepoint does not exist.
    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<(), DbError> {
        let savepoint = self
            .savepoints
            .get(name)
            .ok_or_else(|| DbError::TransactionAborted(format!("missing savepoint {name}")))?
            .clone();
        self.write_set = savepoint.write_set;
        *self.read_keys.borrow_mut() = savepoint.read_keys;
        *self.read_ranges.borrow_mut() = savepoint.read_ranges;
        Ok(())
    }

    /// Releases a savepoint.
    /// # Errors
    /// Fails when the savepoint does not exist.
    pub fn release_savepoint(&mut self, name: &str) -> Result<(), DbError> {
        if self.savepoints.remove(name).is_some() {
            Ok(())
        } else {
            Err(DbError::TransactionAborted(format!(
                "missing savepoint {name}"
            )))
        }
    }

    fn read_current_raw(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, DbError> {
        let storage = self.database.repl.storage().ok_or_else(|| {
            ConfigError::Unsupported("replication backend has no local snapshot storage".to_owned())
        })?;
        let read = storage.begin_read()?;
        Ok(read.get(table, key)?)
    }

    fn range_current_raw(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Bytes, Bytes)>, DbError> {
        let storage = self.database.repl.storage().ok_or_else(|| {
            ConfigError::Unsupported("replication backend has no local snapshot storage".to_owned())
        })?;
        let read = storage.begin_read()?;
        Ok(read
            .range(table, start, end)?
            .collect::<Result<Vec<_>, _>>()?)
    }

    fn read_snapshot_raw(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, DbError> {
        // `self.read` is the point-in-time storage snapshot that defined `snapshot_id`.
        Ok(self.read.get(table, key)?)
    }

    fn record_read_key(&self, table: &str, key: &[u8]) {
        if self.options.isolation == IsolationLevel::Serializable {
            self.read_keys
                .borrow_mut()
                .insert((table.to_owned(), key.to_vec()));
        }
    }

    fn record_read_range(&self, table: &str, start: &[u8], end: &[u8]) {
        if self.options.isolation == IsolationLevel::Serializable {
            self.read_ranges.borrow_mut().push(TxnReadRange {
                table: table.to_owned(),
                start: start.to_vec(),
                end: end.to_vec(),
            });
        }
    }

    /// Reads a relational row by primary key.
    /// # Errors
    /// Fails when the table is missing, key encoding fails, or row bytes are corrupt.
    pub fn get_row(&self, table: &str, primary_key: &Value) -> Result<Option<Row>, DbError> {
        let table = self.database.table(table)?;
        if table.layout() == TableLayout::Columnar {
            let segments = self.columnar_segments(&table)?;
            return Ok(Self::columnar_rows(&table, &segments)?
                .into_iter()
                .find(|row| {
                    table
                        .primary_key_value_for_row(row)
                        .is_ok_and(|key| key == *primary_key)
                }));
        }

        let row_key = table.row_key_for_primary_key(primary_key)?;
        self.get_row_by_key(&row_key)
    }

    /// Queues a relational insert.
    /// # Errors
    /// Fails when the table is missing or the row is invalid.
    pub fn insert_row(&mut self, table: &str, row: Row) -> Result<(), DbError> {
        let table = self.database.table(table)?;
        if table.layout() == TableLayout::Columnar {
            let segments = self.columnar_segments(&table)?;
            self.queue_ops(table.columnar_append_ops(&segments, &[row])?);
            return Ok(());
        }

        let row_key = table.row_key_for_row(&row)?;
        if self.get_row_by_key(&row_key)?.is_some() {
            return Err(QueryError::DuplicatePrimaryKey.into());
        }

        self.queue_ops(table.put_ops_for_key(row, &row_key)?);
        Ok(())
    }

    /// Queues a relational update.
    /// # Errors
    /// Fails when the table is missing or the row is invalid.
    pub fn update_row(&mut self, table: &str, row: Row) -> Result<(), DbError> {
        let table = self.database.table(table)?;
        if table.layout() == TableLayout::Columnar {
            let primary_key = table.primary_key_value_for_row(&row)?;
            let segments = self.columnar_segments(&table)?;
            let mut rows = Self::columnar_rows(&table, &segments)?;
            if let Some(index) = rows.iter().position(|existing| {
                table
                    .primary_key_value_for_row(existing)
                    .is_ok_and(|key| key == primary_key)
            }) {
                rows[index] = row;
            } else {
                rows.push(row);
            }

            self.queue_ops(table.columnar_rewrite_ops(&segments, rows)?);
            return Ok(());
        }

        let row_key = table.row_key_for_row(&row)?;

        let ops = if let Some(old) = self.get_row_by_key(&row_key)? {
            table.update_ops_for_key(&old, row, &row_key)?
        } else {
            table.put_ops_for_key(row, &row_key)?
        };
        self.queue_ops(ops);
        Ok(())
    }

    /// Queues a relational delete.
    /// # Errors
    /// Fails when the table is missing or the key is invalid.
    pub fn delete_row(&mut self, table: &str, primary_key: &Value) -> Result<(), DbError> {
        let table = self.database.table(table)?;
        if table.layout() == TableLayout::Columnar {
            let segments = self.columnar_segments(&table)?;
            let mut rows = Self::columnar_rows(&table, &segments)?;
            let before = rows.len();
            rows.retain(|row| {
                !table
                    .primary_key_value_for_row(row)
                    .is_ok_and(|key| key == *primary_key)
            });

            if rows.len() != before {
                self.queue_ops(table.columnar_rewrite_ops(&segments, rows)?);
            }

            return Ok(());
        }

        let row_key = table.row_key_for_primary_key(primary_key)?;

        if let Some(old) = self.get_row_by_key(&row_key)? {
            self.queue_ops(table.index_delete_ops_for_key(&old, &row_key)?);
            self.queue_ops(vec![Op::Delete {
                table: REL_ROWS_TABLE.to_owned(),
                key: row_key,
            }]);
        }

        Ok(())
    }

    /// Reads a document from the transaction snapshot.
    /// # Errors
    /// Fails when the collection is missing, storage rejects the read, or bytes are corrupt.
    pub fn get_document(&self, collection: &str, id: DocumentId) -> Result<Option<Value>, DbError> {
        let collection = self.database.collection(collection)?;
        let key = collection.document_key(id);
        let Some(bytes) = self.read_raw(DOCUMENT_TABLE, &key)? else {
            return Ok(None);
        };

        Ok(Some(decode_value(&bytes)?))
    }

    /// Queues a document insert and returns the generated id.
    /// # Errors
    /// Fails when the collection is missing or the document cannot be encoded.
    pub fn insert_document(
        &mut self,
        collection: &str,
        doc: &Value,
    ) -> Result<DocumentId, DbError> {
        let collection = self.database.collection(collection)?;
        let (id, ops) = collection.insert_ops(doc)?;
        self.queue_ops(ops);
        Ok(id)
    }

    /// Queues a document update.
    /// # Errors
    /// Fails when the collection is missing or the document cannot be encoded.
    pub fn update_document(
        &mut self,
        collection: &str,
        id: DocumentId,
        doc: &Value,
    ) -> Result<(), DbError> {
        let collection_handle = self.database.collection(collection)?;
        let old = self.get_document(collection, id)?;
        self.queue_ops(collection_handle.replace_ops_with_old(id, old.as_ref(), doc)?);
        Ok(())
    }

    /// Queues a document delete.
    /// # Errors
    /// Fails when the collection is missing or indexed values cannot be encoded.
    pub fn delete_document(&mut self, collection: &str, id: DocumentId) -> Result<(), DbError> {
        let collection_handle = self.database.collection(collection)?;
        let old = self.get_document(collection, id)?;
        self.queue_ops(collection_handle.delete_ops_with_old(id, old.as_ref())?);
        Ok(())
    }

    /// Commits the transaction through the database write pipeline (triggers, hooks,
    /// tenant quotas). After-commit hook and after-trigger failures are recorded in
    /// [`Database::post_commit_failures`]; they never fail a committed transaction.
    /// # Errors
    /// Fails with conflict when a written key changed after this transaction snapshot,
    /// when a before trigger or before hook rejects the write, or when the tenant
    /// storage quota or write concurrency limit is exceeded.
    pub fn commit(self) -> Result<(), DbError> {
        let database = self.database;
        let snapshot_id = self.snapshot_id;
        let isolation = self.options.isolation;
        let write_set = self.write_set;
        let read_keys = self.read_keys.into_inner();
        let read_ranges = self.read_ranges.into_inner();
        let snapshot_read = self.read;

        let storage = database.repl.storage().ok_or_else(|| {
            ConfigError::Unsupported("replication backend has no local snapshot storage".to_owned())
        })?;

        if write_set.is_empty() {
            if isolation == IsolationLevel::Serializable {
                let read = storage.begin_read()?;
                validate_serializable_reads(
                    &read,
                    snapshot_id,
                    &read_keys,
                    &read_ranges,
                    &BTreeSet::new(),
                )?;
            }
            return Ok(());
        }

        // The commit fails if a written key changed after the snapshot. When
        // triggers fire, the pipeline checks that before any of them runs, so they
        // take each key's old value from the snapshot, which then holds what the
        // transaction read at every isolation level.
        let batch = ConditionalBatch::new(Vec::new(), ops_from_write_set(&write_set));
        let snapshot = TxnSnapshot {
            id: snapshot_id,
            read: &snapshot_read,
            storage,
        };
        database
            .write_pipeline()
            .execute(batch, Some(snapshot), |batch| {
                let write_set = txn::ops_to_write_set(batch.ops);
                let write_keys = write_set.keys().cloned().collect::<BTreeSet<_>>();
                if isolation == IsolationLevel::Serializable {
                    database.repl.commit_write_set_with_preflight(
                        snapshot_id,
                        write_set,
                        |write| {
                            validate_serializable_reads(
                                write,
                                snapshot_id,
                                &read_keys,
                                &read_ranges,
                                &write_keys,
                            )
                        },
                    )?;
                } else {
                    database.repl.commit_write_set(snapshot_id, write_set)?;
                }
                Ok(())
            })
    }

    pub fn rollback(self) {}

    fn get_row_by_key(&self, row_key: &[u8]) -> Result<Option<Row>, DbError> {
        let Some(bytes) = self.read_raw(REL_ROWS_TABLE, row_key)? else {
            return Ok(None);
        };

        Ok(decode_row_bytes(&bytes)?)
    }

    fn columnar_segments(&self, table: &RelTable) -> Result<Vec<(Bytes, Bytes)>, DbError> {
        let (start, end) = table.columnar_segment_bounds();
        self.range_raw(REL_COLUMNAR_SEGMENTS_TABLE, &start, &end)
    }

    fn columnar_rows(table: &RelTable, segments: &[(Bytes, Bytes)]) -> Result<Vec<Row>, DbError> {
        let mut rows = Vec::new();
        for (_, bytes) in segments {
            rows.extend(table.decode_columnar_rows(Some(bytes))?);
        }
        Ok(rows)
    }

    fn queue_ops(&mut self, ops: Vec<Op>) {
        self.write_set.extend(txn::ops_to_write_set(ops));
    }
}

fn normalize_isolation(isolation: IsolationLevel) -> IsolationLevel {
    match isolation {
        IsolationLevel::Snapshot => IsolationLevel::SnapshotIsolation,
        other => other,
    }
}

fn key_in_range(key: &[u8], start: &[u8], end: &[u8]) -> bool {
    key >= start && (end.is_empty() || key < end)
}

fn validate_serializable_reads(
    read: &impl ReadTransaction,
    snapshot_id: TxnId,
    read_keys: &BTreeSet<WriteKey>,
    read_ranges: &[TxnReadRange],
    write_keys: &BTreeSet<WriteKey>,
) -> Result<(), StorageError> {
    for (table, key) in read_keys {
        if write_keys.contains(&(table.clone(), key.clone())) {
            continue;
        }
        if txn::last_key_version_from(read, table, key)? > snapshot_id {
            return Err(StorageError::Conflict);
        }
    }

    for range in read_ranges {
        for row in read.range(&range.table, &range.start, &range.end)? {
            let (key, _) = row?;
            if write_keys.contains(&(range.table.clone(), key.clone())) {
                continue;
            }
            if txn::last_key_version_from(read, &range.table, &key)? > snapshot_id {
                return Err(StorageError::Conflict);
            }
        }
    }

    Ok(())
}

/// The one write pipeline every user write goes through: `Database` writes,
/// `MultiModelTxn` commits, stored procedures, and the `RelTable`/`SqlEngine`
/// handles built from [`Database::replication_handle`].
///
/// Stages run in this order:
/// 1. before triggers, which may reject the write or replace a row;
/// 2. the after-trigger snapshot (old values are taken before the commit);
/// 3. before-commit hooks;
/// 4. the tenant write permit and storage reservation;
/// 5. the backend commit;
/// 6. tenant settlement;
/// 7. after-commit hooks and after triggers.
///
/// Once the backend commit returns `Ok`, the pipeline never returns `Err`: an
/// after-commit hook or after trigger failure is logged, counted in
/// `multidb_post_commit_failures_total` and recorded in
/// [`cdc::POST_COMMIT_FAILURES_TABLE`]. System writes (`propose_authorized_batch`)
/// bypass every stage.
#[derive(Clone)]
struct WritePipeline {
    tensor_only: bool,
    typed_tensor_write: bool,
    backend: Arc<dyn Replication>,
    tenant: Option<TenantRuntime>,
    wasm_runtime: Arc<WasmRuntime>,
    sql_schema_epoch: Arc<std::sync::atomic::AtomicU64>,
}

fn plain_document_catalog_allowed(
    catalog: &BTreeMap<String, CatalogEntry>,
    collection_id: u32,
) -> bool {
    let mut source = false;
    for entry in catalog.values() {
        match entry {
            CatalogEntry::Collection {
                collection_id: id,
                indexes,
                ..
            } if id.as_u32() == collection_id => {
                if source || !indexes.is_empty() {
                    return false;
                }
                source = true;
            }
            CatalogEntry::Vector {
                collection_id: id, ..
            } if id.as_u32() == collection_id => return false,
            CatalogEntry::FullTextIndex { config } => {
                let TextSource::Collection {
                    collection_id: id, ..
                } = &config.source;
                if id.as_u32() == collection_id {
                    return false;
                }
            }
            CatalogEntry::GeoIndex { config } if config.collection_id.as_u32() == collection_id => {
                return false;
            }
            _ => {}
        }
    }
    source
}

/// Maps rejections raised before the commit into the caller's error type.
trait PipelineError: From<ReplError> {
    fn trigger(error: DbError) -> Self;
    fn hook(error: HookError) -> Self;
}

impl PipelineError for ReplError {
    fn trigger(error: DbError) -> Self {
        db_trigger_to_repl(&error)
    }

    fn hook(error: HookError) -> Self {
        db_hook_to_repl(error)
    }
}

impl PipelineError for DbError {
    fn trigger(error: DbError) -> Self {
        error
    }

    fn hook(error: HookError) -> Self {
        Self::Hook(error)
    }
}

#[derive(Clone)]
struct WasmTriggerInvocation {
    spec: TriggerSpec,
    event: TriggerEvent,
    op: Op,
    old: Option<Bytes>,
}

/// The old values a write's commit asserts. Triggers take a written key's event
/// and `old` value from here, so they see the row the commit checks.
///
/// A conditional batch asserts the value of each key it has a condition on, and a
/// `MultiModelTxn` commit asserts that no key it writes changed after the
/// transaction's snapshot. When another writer changes such a row after the
/// caller read it, or before a read-committed transaction read it, the write
/// fails with a conflict, which callers retry, instead of a before trigger running
/// for an event the write does not have. A key without an assertion is read when
/// the pipeline runs, so the events of unconditional writes are best effort under
/// concurrent writes.
struct AssertedOldValues<'a> {
    conditions: BTreeMap<(&'a str, &'a [u8]), Option<&'a [u8]>>,
    snapshot: Option<TxnSnapshot<'a>>,
}

impl<'a> AssertedOldValues<'a> {
    /// Fails with the conflict the commit would report when `snapshot` is set and a
    /// key `ops` writes changed after it, so no trigger runs for such a commit.
    fn new(
        conditions: &'a [WriteCondition],
        snapshot: Option<TxnSnapshot<'a>>,
        ops: &[Op],
    ) -> Result<Self, DbError> {
        if let Some(snapshot) = &snapshot {
            snapshot.check_unchanged(ops)?;
        }
        let conditions = conditions
            .iter()
            .map(|condition| match condition {
                WriteCondition::KeyMissing { table, key } => {
                    ((table.as_str(), key.as_slice()), None)
                }
                WriteCondition::ValueEquals {
                    table,
                    key,
                    expected,
                } => ((table.as_str(), key.as_slice()), expected.as_deref()),
            })
            .collect();
        Ok(Self {
            conditions,
            snapshot,
        })
    }
}

/// The storage snapshot a `MultiModelTxn` began with. Its commit fails with a
/// conflict when a key it writes changed after `id`, at every isolation level.
struct TxnSnapshot<'a> {
    id: TxnId,
    read: &'a AnyReadTxn,
    storage: &'a AnyEngine,
}

impl TxnSnapshot<'_> {
    /// Fails with the conflict the commit would report when a key `ops` writes
    /// changed after the snapshot. A read-committed transaction reads current
    /// values, so it may have read a later value of such a key than the snapshot
    /// holds.
    fn check_unchanged(&self, ops: &[Op]) -> Result<(), DbError> {
        let current = self.storage.begin_read()?;
        for op in ops {
            let (table, key) = op_table_key(op);
            if txn::last_key_version_from(&current, table, key)? > self.id {
                return Err(StorageError::Conflict.into());
            }
        }
        Ok(())
    }

    /// The value `key` held at the snapshot. Once `check_unchanged` passed for the
    /// written keys, each of them held that value at every read the transaction
    /// made, at every isolation level.
    fn old_value(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, DbError> {
        Ok(self.read.get(table, key)?)
    }
}

/// An enabled trigger with the catalog entry of its target, read once per write.
struct ResolvedTrigger {
    spec: TriggerSpec,
    target: Option<CatalogEntry>,
}

#[derive(Default)]
struct ResolvedTriggers {
    before: Vec<ResolvedTrigger>,
    after: Vec<ResolvedTrigger>,
}

impl ResolvedTriggers {
    fn is_empty(&self) -> bool {
        self.before.is_empty() && self.after.is_empty()
    }
}

/// A relational row replaced by a before trigger. Its index entries are rebuilt.
struct ReplacedRow {
    trigger: String,
    table: RelTable,
    row_key: Bytes,
    original: Row,
    replacement: Row,
}

const AFTER_COMMIT_HOOKS_FAILURE_NAME: &str = "after_commit_hooks";

impl WritePipeline {
    /// Runs `batch` through every stage; `commit` writes it to the backend. The
    /// before triggers may change `batch.ops`, never its conditions. `snapshot` is
    /// the snapshot of a `MultiModelTxn`, whose commit asserts that no written key
    /// changed after it; when triggers fire, that is checked before any of them
    /// runs.
    fn execute<E: PipelineError>(
        &self,
        batch: ConditionalBatch,
        snapshot: Option<TxnSnapshot<'_>>,
        commit: impl FnOnce(ConditionalBatch) -> Result<(), E>,
    ) -> Result<(), E> {
        let ConditionalBatch { conditions, ops } = batch;
        if self.tensor_only && !self.typed_tensor_write && !ops.is_empty() {
            return Err(ReplError::Unsupported(
                "the Tensor profile accepts user writes only from validated typed tensor appends"
                    .to_owned(),
            )
            .into());
        }
        let triggers = self.resolved_triggers(&ops).map_err(E::trigger)?;
        let (ops, invocations) = if triggers.is_empty() {
            (ops, Vec::new())
        } else {
            let asserted =
                AssertedOldValues::new(&conditions, snapshot, &ops).map_err(E::trigger)?;
            let ops = self
                .apply_before_triggers(&triggers.before, ops, &asserted)
                .map_err(E::trigger)?;
            let invocations = self
                .after_trigger_invocations(&triggers.after, &ops, &asserted)
                .map_err(E::trigger)?;
            (ops, invocations)
        };
        cdc::validate_before_hooks(self.backend.as_ref(), &ops).map_err(E::hook)?;
        let start_lsn = current_lsn_from_repl(self.backend.as_ref())?;
        let write_permit = self
            .tenant
            .as_ref()
            .map(|tenant| tenant.try_begin_write().map_err(repl_quota_error))
            .transpose()?;
        let reservation = match &self.tenant {
            Some(tenant) => Some((
                tenant,
                tenant.reserve_ops(&ops).map_err(repl_quota_error)?,
                ops.clone(),
            )),
            None => None,
        };
        let schema_changed = sql_cache::schema_changed(&ops);
        if let Err(error) = commit(ConditionalBatch::new(conditions, ops)) {
            if let Some((tenant, reservation, _)) = &reservation {
                tenant.release(reservation);
            }
            return Err(error);
        }

        // The write is durable from here on: nothing below may return an error.
        if schema_changed {
            self.sql_schema_epoch
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
        if let Some((tenant, reservation, committed)) = &reservation {
            tenant.commit_successful_ops(committed, reservation);
        }
        drop(write_permit);
        self.after_commit(start_lsn, invocations);
        Ok(())
    }

    /// Runs after-commit hooks and after triggers. The write is already durable, so
    /// every failure is recorded and none is returned.
    fn after_commit(&self, start_lsn: TxnId, invocations: Vec<WasmTriggerInvocation>) {
        let mut failures = Vec::new();
        let end_lsn = match current_lsn_from_repl(self.backend.as_ref()) {
            Ok(end_lsn) => {
                if let Err(error) =
                    cdc::deliver_after_commit_hooks(self.backend.as_ref(), start_lsn, end_lsn)
                {
                    observability::record_hook_failure(hook_failure_kind(&error));
                    failures.push(PostCommitFailure::new(
                        PostCommitStage::AfterHook,
                        AFTER_COMMIT_HOOKS_FAILURE_NAME,
                        start_lsn,
                        end_lsn,
                        error.to_string(),
                    ));
                }
                end_lsn
            }
            Err(error) => {
                // Only a missed delivery is a failure; record it unless no after-commit
                // hook is registered. An unreadable hook list counts as a missed delivery.
                if cdc::has_enabled_after_commit_hooks(self.backend.as_ref()).unwrap_or(true) {
                    failures.push(PostCommitFailure::new(
                        PostCommitStage::AfterHook,
                        AFTER_COMMIT_HOOKS_FAILURE_NAME,
                        start_lsn,
                        start_lsn,
                        format!("cannot read the commit LSN for after-commit hooks: {error}"),
                    ));
                }
                start_lsn
            }
        };

        for invocation in invocations {
            let error = match self.run_wasm_trigger(
                &invocation.spec,
                TriggerTiming::After,
                invocation.event,
                &invocation.op,
                invocation.old.as_deref(),
            ) {
                Ok(TriggerOutcome::Accept) => continue,
                Ok(_) => format!("after trigger {} must return accept", invocation.spec.name),
                Err(error) => error.to_string(),
            };
            failures.push(PostCommitFailure::new(
                PostCommitStage::AfterTrigger,
                invocation.spec.name,
                start_lsn,
                end_lsn,
                error,
            ));
        }

        cdc::record_post_commit_failures(self.backend.as_ref(), &failures);
    }

    /// Reads the enabled triggers once and resolves each target catalog entry.
    fn resolved_triggers(&self, ops: &[Op]) -> Result<ResolvedTriggers, DbError> {
        let mut triggers = ResolvedTriggers::default();
        if ops.is_empty() {
            return Ok(triggers);
        }

        for spec in continuous::read_triggers(self.backend.as_ref())? {
            if !spec.enabled {
                continue;
            }
            let target = read_catalog_entry_from_repl(self.backend.as_ref(), &spec.table)?;
            let timing = spec.timing;
            let resolved = ResolvedTrigger { spec, target };
            match timing {
                TriggerTiming::Before => triggers.before.push(resolved),
                TriggerTiming::After => triggers.after.push(resolved),
            }
        }
        Ok(triggers)
    }

    fn apply_before_triggers(
        &self,
        triggers: &[ResolvedTrigger],
        ops: Vec<Op>,
        asserted: &AssertedOldValues<'_>,
    ) -> Result<Vec<Op>, DbError> {
        if triggers.is_empty() || ops.is_empty() {
            return Ok(ops);
        }

        let mut transformed = Vec::with_capacity(ops.len());
        let mut replaced = Vec::new();
        for op in ops {
            if !triggers
                .iter()
                .any(|trigger| trigger_targets_op(trigger, &op))
            {
                transformed.push(op);
                continue;
            }
            let mut current = op;
            let old = self.old_value_for_op(&current, asserted)?;
            let Some(event) = event_for_op(&current, old.as_deref()) else {
                transformed.push(current);
                continue;
            };

            let mut replacement = None;
            for trigger in triggers {
                if !trigger_matches_op(trigger, &current, event) {
                    continue;
                }
                match self.run_wasm_trigger(
                    &trigger.spec,
                    TriggerTiming::Before,
                    event,
                    &current,
                    old.as_deref(),
                )? {
                    TriggerOutcome::Accept => {}
                    TriggerOutcome::Reject(message) => {
                        return Err(ContinuousError::Invalid(format!(
                            "before trigger {} rejected write: {message}",
                            trigger.spec.name
                        ))
                        .into());
                    }
                    TriggerOutcome::Replace(value) => {
                        let (op, row) =
                            self.replace_trigger_row(trigger, current, value, replacement.take())?;
                        current = op;
                        replacement = Some(row);
                    }
                }
            }

            replaced.extend(replacement);
            transformed.push(current);
        }

        if replaced.is_empty() {
            return Ok(transformed);
        }
        reject_repeated_replaced_rows(&transformed, &replaced)?;
        rebuild_replaced_row_indexes(transformed, replaced)
    }

    /// Validates a before-trigger `replace` value and builds the replacement op.
    ///
    /// Only rows of row-layout tables can be replaced. The replacement must match
    /// the table schema and keep the primary key; anything else fails closed.
    fn replace_trigger_row(
        &self,
        trigger: &ResolvedTrigger,
        op: Op,
        value: Value,
        previous: Option<ReplacedRow>,
    ) -> Result<(Op, ReplacedRow), DbError> {
        let name = &trigger.spec.name;
        let (storage_table, key, current_value) = match op {
            Op::Put { table, key, value } => (table, key, value),
            Op::Delete { .. } => {
                return Err(ContinuousError::Invalid(
                    "delete triggers cannot replace a deleted value".to_owned(),
                )
                .into());
            }
        };
        if storage_table != REL_ROWS_TABLE {
            let target = match storage_table.as_str() {
                REL_COLUMNAR_SEGMENTS_TABLE => "columnar table segments".to_owned(),
                DOCUMENT_TABLE => "documents".to_owned(),
                other => format!("writes to {other}"),
            };
            return Err(ContinuousError::Invalid(format!(
                "before trigger {name} cannot replace {target}; only rows of row-layout tables can be replaced"
            ))
            .into());
        }
        let Some(CatalogEntry::Table {
            indexes,
            layout: TableLayout::Row,
        }) = &trigger.target
        else {
            return Err(ContinuousError::Invalid(format!(
                "before trigger {name} can only replace rows of row-layout tables"
            ))
            .into());
        };
        let Value::Array(row) = value else {
            return Err(ContinuousError::Invalid(
                "relational trigger replacement must be a row array".to_owned(),
            )
            .into());
        };

        let table = RelTable::open_with_layout(
            Arc::clone(&self.backend),
            &trigger.spec.table,
            indexes.clone(),
            TableLayout::Row,
        )?;
        let row_key = table.row_key_for_row(&row).map_err(|error| {
            ContinuousError::Invalid(format!(
                "before trigger {name} replacement is invalid: {error}"
            ))
        })?;
        if row_key != key {
            return Err(ContinuousError::Invalid(format!(
                "before trigger {name} replacement must keep the primary key"
            ))
            .into());
        }
        let original = match previous {
            Some(previous) => previous.original,
            None => decode_row_bytes(&current_value)?.ok_or_else(|| {
                QueryError::Storage(StorageError::Corruption(
                    "trigger row payload is missing".to_owned(),
                ))
            })?,
        };

        let op = Op::Put {
            table: storage_table,
            key,
            value: encode_value(&Value::Array(row.clone()))?,
        };
        Ok((
            op,
            ReplacedRow {
                trigger: name.clone(),
                table,
                row_key,
                original,
                replacement: row,
            },
        ))
    }

    fn after_trigger_invocations(
        &self,
        triggers: &[ResolvedTrigger],
        ops: &[Op],
        asserted: &AssertedOldValues<'_>,
    ) -> Result<Vec<WasmTriggerInvocation>, DbError> {
        if triggers.is_empty() || ops.is_empty() {
            return Ok(Vec::new());
        }

        let mut invocations = Vec::new();
        for op in ops {
            if !triggers
                .iter()
                .any(|trigger| trigger_targets_op(trigger, op))
            {
                continue;
            }
            let old = self.old_value_for_op(op, asserted)?;
            let Some(event) = event_for_op(op, old.as_deref()) else {
                continue;
            };
            invocations.extend(
                triggers
                    .iter()
                    .filter(|trigger| trigger_matches_op(trigger, op, event))
                    .map(|trigger| WasmTriggerInvocation {
                        spec: trigger.spec.clone(),
                        event,
                        op: op.clone(),
                        old: old.clone(),
                    }),
            );
        }

        Ok(invocations)
    }

    /// The value `op`'s key holds before the write: the value the commit asserts,
    /// or the current value when the commit asserts none.
    fn old_value_for_op(
        &self,
        op: &Op,
        asserted: &AssertedOldValues<'_>,
    ) -> Result<Option<Bytes>, DbError> {
        let (table, key) = op_table_key(op);
        if let Some(value) = asserted.conditions.get(&(table, key)) {
            return Ok(value.map(<[u8]>::to_vec));
        }
        if let Some(snapshot) = &asserted.snapshot {
            return snapshot.old_value(table, key);
        }
        Ok(self.backend.read(table, key, ReadConsistency::Strong)?)
    }

    fn run_wasm_trigger(
        &self,
        spec: &TriggerSpec,
        timing: TriggerTiming,
        event: TriggerEvent,
        op: &Op,
        old: Option<&[u8]>,
    ) -> Result<TriggerOutcome, DbError> {
        let (_, wasm) = extension::read_wasm_module(self.backend.as_ref(), &spec.module_hash)?;
        let mut udf = UdfSpec::scalar(&spec.name, &spec.module_hash);
        udf.entry.clone_from(&spec.entry);
        udf.abi = spec.abi;
        udf.budget = spec.budget.clone();
        let value = self.wasm_runtime.call_udf(
            &udf,
            &wasm,
            &[trigger_context_value(spec, timing, event, op, old)?],
        )?;
        Ok(continuous::trigger_outcome_from_value(value)?)
    }
}

impl Replication for WritePipeline {
    fn propose_plain_document_lww(&self, collection_id: u32, op: Op) -> Result<(), ReplError> {
        let catalog = read_catalog_from_repl(self.backend.as_ref())
            .map_err(|error| db_trigger_to_repl(&DbError::Config(error)))?;
        if !plain_document_catalog_allowed(&catalog, collection_id) {
            return Err(ReplError::Unsupported(
                "plain AP document LWW requires a known collection without indexes".to_owned(),
            ));
        }
        let source_key = match &op {
            Op::Put { table, key, .. } | Op::Delete { table, key }
                if table == DOCUMENT_TABLE
                    && key.len() == 20
                    && key.starts_with(&collection_id.to_be_bytes()) =>
            {
                key.clone()
            }
            _ => {
                return Err(ReplError::Unsupported(
                    "plain AP document LWW requires one source document".to_owned(),
                ));
            }
        };
        self.execute(
            ConditionalBatch::new(Vec::new(), vec![op]),
            None,
            |mut batch| {
                if !batch.conditions.is_empty() || batch.ops.len() != 1 {
                    return Err(ReplError::Unsupported(
                        "plain AP document LWW cannot include index or trigger deltas".to_owned(),
                    ));
                }
                let op = batch.ops.pop().ok_or_else(|| {
                    ReplError::Unsupported("missing plain document write".to_owned())
                })?;
                if !matches!(&op, Op::Put { table, key, .. } | Op::Delete { table, key }
                if table == DOCUMENT_TABLE && *key == source_key)
                {
                    return Err(ReplError::Unsupported(
                        "plain AP document LWW changed its source identity".to_owned(),
                    ));
                }
                self.backend.propose_plain_document_lww(collection_id, op)
            },
        )
    }
    fn registered_keyspaces(&self) -> Result<Vec<String>, ReplError> {
        self.backend.registered_keyspaces()
    }
    fn read_many(
        &self,
        table: &str,
        keys: &[Bytes],
        consistency: ReadConsistency,
    ) -> Result<Vec<Option<Bytes>>, ReplError> {
        let _query_permit = self
            .tenant
            .as_ref()
            .map(|tenant| tenant.try_begin_query().map_err(repl_quota_error))
            .transpose()?;
        self.backend.read_many(table, keys, consistency)
    }

    fn table_data_version(&self, table: &str) -> Result<Option<u64>, ReplError> {
        self.backend.table_data_version(table)
    }

    fn supports_bounded_scans(&self) -> bool {
        self.backend.supports_bounded_scans()
    }

    fn supports_commit_log(&self) -> bool {
        self.backend.supports_commit_log()
    }

    fn propose(&self, op: Op) -> Result<(), ReplError> {
        self.propose_batch(vec![op])
    }

    fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
        self.execute(ConditionalBatch::new(Vec::new(), ops), None, |batch| {
            self.backend.propose_batch(batch.ops)
        })
    }

    fn propose_authorized_batch(
        &self,
        ops: Vec<Op>,
        authorization: txn::WriteAuthorization,
    ) -> Result<(), ReplError> {
        let schema_changed = sql_cache::schema_changed(&ops);
        self.backend.propose_authorized_batch(ops, authorization)?;
        if schema_changed {
            self.sql_schema_epoch
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
        Ok(())
    }

    fn propose_authorized_conditional_batch(
        &self,
        batch: ConditionalBatch,
        authorization: txn::WriteAuthorization,
    ) -> Result<(), ReplError> {
        let schema_changed = sql_cache::schema_changed(&batch.ops);
        self.backend
            .propose_authorized_conditional_batch(batch, authorization)?;
        if schema_changed {
            self.sql_schema_epoch
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
        Ok(())
    }

    fn propose_conditional_batch(&self, batch: ConditionalBatch) -> Result<(), ReplError> {
        self.execute(batch, None, |batch| {
            self.backend.propose_conditional_batch(batch)
        })
    }

    fn propose_tensor_batch(
        &self,
        batch: ConditionalBatch,
        authorization: crate::tensor::TensorWriteAuthorization,
    ) -> Result<(), ReplError> {
        authorization.validate(&batch)?;
        let mut pipeline = self.clone();
        pipeline.typed_tensor_write = true;
        pipeline.execute(batch, None, |batch| {
            self.backend.propose_conditional_batch(batch)
        })
    }

    fn read(
        &self,
        table: &str,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Option<Bytes>, ReplError> {
        let _query_permit = self
            .tenant
            .as_ref()
            .map(|tenant| tenant.try_begin_query().map_err(repl_quota_error))
            .transpose()?;
        self.backend.read(table, key, consistency)
    }

    fn range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        let _query_permit = self
            .tenant
            .as_ref()
            .map(|tenant| tenant.try_begin_query().map_err(repl_quota_error))
            .transpose()?;
        self.backend.range(table, start, end, consistency)
    }

    fn scan_range_batches(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
        batch_rows: usize,
        cancelled: &dyn Fn() -> bool,
        on_batch: &mut dyn FnMut(&[(Bytes, Bytes)]) -> Result<bool, ReplError>,
    ) -> Result<(), ReplError> {
        let _query_permit = self
            .tenant
            .as_ref()
            .map(|tenant| tenant.try_begin_query().map_err(repl_quota_error))
            .transpose()?;
        self.backend.scan_range_batches(
            table,
            start,
            end,
            consistency,
            batch_rows,
            cancelled,
            on_batch,
        )
    }
}

fn event_for_op(op: &Op, old: Option<&[u8]>) -> Option<TriggerEvent> {
    match op {
        Op::Put { .. } if old.is_some() => Some(TriggerEvent::Update),
        Op::Put { .. } => Some(TriggerEvent::Insert),
        Op::Delete { .. } if old.is_some() => Some(TriggerEvent::Delete),
        Op::Delete { .. } => None,
    }
}

/// Whether `op` writes the object a trigger is defined on, for any event.
fn trigger_targets_op(trigger: &ResolvedTrigger, op: &Op) -> bool {
    let (storage_table, key) = op_table_key(op);
    match &trigger.target {
        Some(CatalogEntry::Table { .. }) => {
            (storage_table == REL_ROWS_TABLE || storage_table == REL_COLUMNAR_SEGMENTS_TABLE)
                && key_has_rel_table_prefix(key, &trigger.spec.table)
        }
        Some(CatalogEntry::Collection { collection_id, .. }) => {
            storage_table == DOCUMENT_TABLE
                && key_in_bounds(key, &collection_range_bounds(*collection_id))
        }
        _ => false,
    }
}

fn trigger_matches_op(trigger: &ResolvedTrigger, op: &Op, event: TriggerEvent) -> bool {
    trigger.spec.event == event && trigger_targets_op(trigger, op)
}

/// Fails closed when a replaced row is written more than once in one batch (for
/// example `INSERT .. VALUES (1, ..), (1, ..) ON CONFLICT DO UPDATE`): the index
/// entries of the earlier write could not be rebuilt reliably.
fn reject_repeated_replaced_rows(ops: &[Op], replaced: &[ReplacedRow]) -> Result<(), DbError> {
    let mut row_writes = BTreeMap::<&[u8], usize>::new();
    for op in ops {
        let (table, key) = op_table_key(op);
        if table == REL_ROWS_TABLE {
            *row_writes.entry(key).or_default() += 1;
        }
    }
    for row in replaced {
        if row_writes
            .get(row.row_key.as_slice())
            .copied()
            .unwrap_or_default()
            > 1
        {
            return Err(ContinuousError::Invalid(format!(
                "before trigger {} cannot replace a row that the same batch writes more than once",
                row.trigger
            ))
            .into());
        }
    }
    Ok(())
}

/// Rebuilds the secondary-index entries of rows replaced by before triggers.
///
/// The writer built index puts for the original row. Those puts become deletes in
/// place: in a transaction write set such a put may also stand for the delete of
/// the previous row's entry with the same key. The replacement row's index puts are
/// appended at the end, so last-write-wins ordering keeps exactly its entries.
fn rebuild_replaced_row_indexes(
    ops: Vec<Op>,
    replaced: Vec<ReplacedRow>,
) -> Result<Vec<Op>, DbError> {
    let mut stale = BTreeSet::new();
    let mut fresh = Vec::new();
    for row in replaced {
        stale.extend(
            row.table
                .index_delete_ops_for_key(&row.original, &row.row_key)?
                .into_iter()
                .map(|op| match op {
                    Op::Put { key, .. } | Op::Delete { key, .. } => key,
                }),
        );
        fresh.extend(
            row.table
                .put_ops_for_key(row.replacement, &row.row_key)?
                .into_iter()
                .filter(|op| op_table_key(op).0 == REL_INDEX_TABLE),
        );
    }

    let mut ops = ops
        .into_iter()
        .map(|op| match op {
            Op::Put { table, key, .. } if table == REL_INDEX_TABLE && stale.contains(&key) => {
                Op::Delete { table, key }
            }
            other => other,
        })
        .collect::<Vec<_>>();
    ops.extend(fresh);
    Ok(ops)
}

fn op_table_key(op: &Op) -> (&str, &[u8]) {
    match op {
        Op::Put { table, key, .. } | Op::Delete { table, key } => (table, key),
    }
}

fn ops_from_write_set(write_set: &WriteSet) -> Vec<Op> {
    write_set
        .iter()
        .map(|((table, key), value)| match value {
            Some(value) => Op::Put {
                table: table.clone(),
                key: key.clone(),
                value: value.clone(),
            },
            None => Op::Delete {
                table: table.clone(),
                key: key.clone(),
            },
        })
        .collect()
}

fn db_trigger_to_repl(error: &DbError) -> ReplError {
    ReplError::Rejected(format!("WASM trigger failed: {error}"))
}

fn key_has_rel_table_prefix(key: &[u8], table: &str) -> bool {
    let mut prefix = Vec::new();
    keyenc::push_len_bytes(&mut prefix, table.as_bytes());
    key.starts_with(&prefix)
}

fn key_in_bounds(key: &[u8], (start, end): &(Bytes, Bytes)) -> bool {
    key >= start.as_slice() && (end.is_empty() || key < end.as_slice())
}

fn trigger_context_value(
    spec: &TriggerSpec,
    timing: TriggerTiming,
    event: TriggerEvent,
    op: &Op,
    old: Option<&[u8]>,
) -> Result<Value, DbError> {
    let (storage_table, key) = op_table_key(op);
    let new = match op {
        Op::Put { value, .. } => trigger_payload_value(storage_table, value)?,
        Op::Delete { .. } => Value::Null,
    };
    let old = old
        .map(|bytes| trigger_payload_value(storage_table, bytes))
        .transpose()?
        .unwrap_or(Value::Null);

    Ok(Value::Object(BTreeMap::from([
        ("trigger".to_owned(), Value::Str(spec.name.clone())),
        ("table".to_owned(), Value::Str(spec.table.clone())),
        (
            "timing".to_owned(),
            Value::Str(trigger_timing_name(timing).to_owned()),
        ),
        (
            "event".to_owned(),
            Value::Str(trigger_event_name(event).to_owned()),
        ),
        (
            "storage_table".to_owned(),
            Value::Str(storage_table.to_owned()),
        ),
        ("key".to_owned(), Value::Bytes(key.to_vec())),
        ("old".to_owned(), old),
        ("new".to_owned(), new),
    ])))
}

fn trigger_payload_value(storage_table: &str, bytes: &[u8]) -> Result<Value, DbError> {
    if storage_table == REL_ROWS_TABLE {
        return decode_row_bytes(bytes)?.map(Value::Array).ok_or_else(|| {
            QueryError::Storage(StorageError::Corruption(
                "trigger row payload is missing".to_owned(),
            ))
            .into()
        });
    }
    if storage_table == DOCUMENT_TABLE {
        return Ok(decode_value(bytes)?);
    }
    Ok(Value::Bytes(bytes.to_vec()))
}

fn trigger_timing_name(timing: TriggerTiming) -> &'static str {
    match timing {
        TriggerTiming::Before => "before",
        TriggerTiming::After => "after",
    }
}

fn trigger_event_name(event: TriggerEvent) -> &'static str {
    match event {
        TriggerEvent::Insert => "insert",
        TriggerEvent::Update => "update",
        TriggerEvent::Delete => "delete",
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // Workload statistics are advisory. Cluster backends are skipped so drop never blocks
        // on Raft or quorum writes, and nothing is written when no sample is buffered.
        if std::thread::panicking() || !matches!(self.repl, DatabaseRepl::SingleNode(_)) {
            return;
        }
        if self.flush_workload_samples().is_err() {
            tracing::debug!("failed to flush workload observations on drop");
        }
    }
}

impl Replication for Database {
    fn propose_plain_document_lww(&self, collection_id: u32, op: Op) -> Result<(), ReplError> {
        self.write_pipeline()
            .propose_plain_document_lww(collection_id, op)
    }
    fn registered_keyspaces(&self) -> Result<Vec<String>, ReplError> {
        self.repl.registered_keyspaces()
    }
    fn read_many(
        &self,
        table: &str,
        keys: &[Bytes],
        consistency: ReadConsistency,
    ) -> Result<Vec<Option<Bytes>>, ReplError> {
        let _query_permit = self
            .tenant
            .as_ref()
            .map(|tenant| tenant.try_begin_query().map_err(repl_quota_error))
            .transpose()?;
        self.repl.read_many(table, keys, consistency)
    }

    fn table_data_version(&self, table: &str) -> Result<Option<u64>, ReplError> {
        self.repl.table_data_version(table)
    }

    fn supports_bounded_scans(&self) -> bool {
        self.repl.supports_bounded_scans()
    }

    fn supports_commit_log(&self) -> bool {
        self.repl.supports_commit_log()
    }

    fn propose(&self, op: Op) -> Result<(), ReplError> {
        self.propose_batch(vec![op])
    }

    fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
        self.write_pipeline()
            .execute(ConditionalBatch::new(Vec::new(), ops), None, |batch| {
                self.repl.propose_batch(batch.ops)
            })
    }

    fn propose_authorized_batch(
        &self,
        ops: Vec<Op>,
        authorization: txn::WriteAuthorization,
    ) -> Result<(), ReplError> {
        let schema_changed = sql_cache::schema_changed(&ops);
        self.repl.propose_authorized_batch(ops, authorization)?;
        if schema_changed {
            self.sql_catalog_cache.invalidate();
        }
        Ok(())
    }

    fn propose_authorized_conditional_batch(
        &self,
        batch: ConditionalBatch,
        authorization: txn::WriteAuthorization,
    ) -> Result<(), ReplError> {
        let schema_changed = sql_cache::schema_changed(&batch.ops);
        self.repl
            .propose_authorized_conditional_batch(batch, authorization)?;
        if schema_changed {
            self.sql_catalog_cache.invalidate();
        }
        Ok(())
    }

    fn propose_conditional_batch(&self, batch: ConditionalBatch) -> Result<(), ReplError> {
        self.write_pipeline().execute(batch, None, |batch| {
            self.repl.propose_conditional_batch(batch)
        })
    }

    fn propose_tensor_batch(
        &self,
        batch: ConditionalBatch,
        authorization: crate::tensor::TensorWriteAuthorization,
    ) -> Result<(), ReplError> {
        self.write_pipeline()
            .propose_tensor_batch(batch, authorization)
    }

    fn read(
        &self,
        table: &str,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Option<crate::storage::Bytes>, ReplError> {
        let _query_permit = self
            .tenant
            .as_ref()
            .map(|tenant| tenant.try_begin_query().map_err(repl_quota_error))
            .transpose()?;
        self.repl.read(table, key, consistency)
    }

    fn range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Vec<(crate::storage::Bytes, crate::storage::Bytes)>, ReplError> {
        let _query_permit = self
            .tenant
            .as_ref()
            .map(|tenant| tenant.try_begin_query().map_err(repl_quota_error))
            .transpose()?;
        self.repl.range(table, start, end, consistency)
    }

    fn scan_range_batches(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
        batch_rows: usize,
        cancelled: &dyn Fn() -> bool,
        on_batch: &mut dyn FnMut(&[(Bytes, Bytes)]) -> Result<bool, ReplError>,
    ) -> Result<(), ReplError> {
        let _query_permit = self
            .tenant
            .as_ref()
            .map(|tenant| tenant.try_begin_query().map_err(repl_quota_error))
            .transpose()?;
        self.repl.scan_range_batches(
            table,
            start,
            end,
            consistency,
            batch_rows,
            cancelled,
            on_batch,
        )
    }
}

fn current_lsn_from_repl(repl: &dyn Replication) -> Result<TxnId, ReplError> {
    let Some(bytes) = repl.read(
        txn::TXN_META_TABLE,
        txn::CURRENT_TXN_ID_KEY,
        ReadConsistency::Strong,
    )?
    else {
        return Ok(0);
    };
    Ok(txn::decode_txn_id(&bytes)?)
}

fn hook_failure_kind(error: &HookError) -> &'static str {
    match error {
        HookError::Rejected(_) => "rejected",
        HookError::Timeout(_) => "timeout",
        HookError::InvalidTarget(_) => "invalid_target",
        HookError::Repl(_) => "replication",
        HookError::Serde(_) => "serde",
    }
}

fn db_hook_to_repl(error: HookError) -> ReplError {
    if !matches!(error, HookError::Repl(_)) {
        observability::record_hook_failure(hook_failure_kind(&error));
    }
    match error {
        HookError::Rejected(message) => {
            ReplError::Rejected(format!("hook rejected write: {message}"))
        }
        HookError::Timeout(message) => ReplError::Rejected(format!("hook timed out: {message}")),
        HookError::InvalidTarget(message) => {
            ReplError::Rejected(format!("invalid hook target: {message}"))
        }
        HookError::Serde(message) => ReplError::Transport(message),
        HookError::Repl(error) => error,
    }
}

/// Creates a database and initializes metadata when it is missing.
/// # Errors
/// Fails when the config is invalid, storage cannot open, existing metadata conflicts, or
/// persisted security metadata cannot be read or decoded.
pub fn create_database(config: DbConfig) -> Result<Database, ConfigError> {
    let engine = engine_for(&config)?;

    let read = match read_metadata(&engine) {
        Ok(read) => {
            ensure_metadata_matches(&config, read.metadata)?;
            read
        }
        Err(ConfigError::MissingMetadata { key }) if key == KEY_SCHEMA_VERSION_NAME => {
            write_metadata(&engine, &config)?;
            MetadataRead::written(&config)
        }
        Err(error) => return Err(error),
    };

    let (engine, performance) = finish_single_node_open(engine, None, &config, &read)?;
    let catalog = read_catalog(&engine)?;
    let mut database = Database::new_with_performance(config, engine, catalog, performance)?;
    database.load_persisted_security()?;
    profile_binding::warn_unmet_profile_floors(read.manifest.as_ref(), &database);
    Ok(database)
}

/// Opens an existing database and validates its metadata.
/// # Errors
/// Fails when the config is invalid, storage cannot open, metadata is missing, metadata
/// conflicts, or persisted security metadata cannot be read or decoded.
pub fn open_database(config: DbConfig) -> Result<Database, ConfigError> {
    let engine = engine_for(&config)?;
    let read = read_metadata(&engine)?;
    ensure_metadata_matches(&config, read.metadata)?;
    let (engine, performance) = finish_single_node_open(engine, None, &config, &read)?;
    let catalog = read_catalog(&engine)?;
    let mut database = Database::new_with_performance(config, engine, catalog, performance)?;
    database.load_persisted_security()?;
    profile_binding::warn_unmet_profile_floors(read.manifest.as_ref(), &database);
    Ok(database)
}

/// Creates a database with operational security, encryption, metrics, and resource settings.
/// # Errors
/// Fails when config, encryption, storage, or metadata validation fails.
pub fn create_database_with_ops(
    config: DbConfig,
    ops: OperationalConfig,
) -> Result<Database, ConfigError> {
    validate_operational(&config, &ops)?;
    let engine = engine_for_operational(&config, &ops)?;

    let read = match read_metadata(&engine) {
        Ok(read) => {
            ensure_metadata_matches(&config, read.metadata)?;
            read
        }
        Err(ConfigError::MissingMetadata { key }) if key == KEY_SCHEMA_VERSION_NAME => {
            write_metadata(&engine, &config)?;
            MetadataRead::written(&config)
        }
        Err(error) => return Err(error),
    };

    let (engine, performance) =
        finish_single_node_open(engine, ops.performance.as_ref(), &config, &read)?;
    let catalog = read_catalog(&engine)?;
    let mut database = Database::new_with_performance(config, engine, catalog, performance)?;
    database.encryption.clone_from(&ops.encryption);
    database.apply_tenant_config(ops.tenant.clone());
    database.initialize_security_if_missing(ops.security)?;
    profile_binding::warn_unmet_profile_floors(read.manifest.as_ref(), &database);
    Ok(database)
}

/// Opens a database with operational security, encryption, metrics, and resource settings.
/// # Errors
/// Fails when config, encryption, storage, or metadata validation fails.
pub fn open_database_with_ops(
    config: DbConfig,
    ops: &OperationalConfig,
) -> Result<Database, ConfigError> {
    validate_operational(&config, ops)?;
    let engine = engine_for_operational(&config, ops)?;
    let read = read_metadata(&engine)?;
    ensure_metadata_matches(&config, read.metadata)?;
    let (engine, performance) =
        finish_single_node_open(engine, ops.performance.as_ref(), &config, &read)?;
    let catalog = read_catalog(&engine)?;
    let mut database = Database::new_with_performance(config, engine, catalog, performance)?;
    database.encryption.clone_from(&ops.encryption);
    let mut security = read_security_from_repl(database.replication_ref())?.unwrap_or_default();
    security.audit.clone_from(&ops.security.audit);
    database.apply_tenant_config(ops.tenant.clone());
    database.apply_security_config(security);
    profile_binding::warn_unmet_profile_floors(read.manifest.as_ref(), &database);
    Ok(database)
}

/// Creates a database backed by a CP/Raft replication backend.
///
/// The embedded `Database` API is not wired to the live `OpenRaft` runtime yet,
/// so this constructor fails closed instead of acknowledging writes without
/// replication. Use `repl::start_cp_cluster` for replicated CP storage, or
/// `create_database` for a single node.
/// # Errors
/// Fails when config or cluster topology validation fails, and otherwise always
/// returns `ConfigError::Unsupported`.
#[allow(clippy::needless_pass_by_value)]
pub fn create_cluster_database(
    config: DbConfig,
    cluster: CpClusterConfig,
) -> Result<Database, ConfigError> {
    ensure_cluster_supported(&config, &cluster)?;
    Err(cluster_database_requires_live_raft())
}

/// Opens a database backed by a CP/Raft replication backend.
///
/// The embedded `Database` API is not wired to the live `OpenRaft` runtime yet,
/// so this constructor fails closed instead of acknowledging writes without
/// replication. Use `repl::start_cp_cluster` for replicated CP storage, or
/// `open_database` for a single node (it also opens data written by earlier
/// versions of this constructor).
/// # Errors
/// Fails when config or cluster topology validation fails, and otherwise always
/// returns `ConfigError::Unsupported`.
#[allow(clippy::needless_pass_by_value)]
pub fn open_cluster_database(
    config: DbConfig,
    cluster: CpClusterConfig,
) -> Result<Database, ConfigError> {
    ensure_cluster_supported(&config, &cluster)?;
    Err(cluster_database_requires_live_raft())
}

/// Creates a database on the in-process simulated CP adapter for in-crate tests.
///
/// The adapter has no `OpenRaft` runtime or transport: every node acknowledges
/// writes locally and reports itself as leader. Never use it for a deployment.
/// # Errors
/// Fails when config, cluster topology, storage, or metadata validation fails, or persisted
/// security metadata cannot be read or decoded.
#[doc(hidden)]
pub fn create_simulated_cluster_database(
    config: DbConfig,
    cluster: CpClusterConfig,
) -> Result<Database, ConfigError> {
    ensure_cluster_supported(&config, &cluster)?;
    let engine = engine_for(&config)?;

    match read_metadata(&engine) {
        Ok(read) => {
            ensure_metadata_matches(&config, read.metadata)?;
            refuse_profile_manifest(&read)?;
        }
        Err(ConfigError::MissingMetadata { key }) if key == KEY_SCHEMA_VERSION_NAME => {
            write_metadata(&engine, &config)?;
        }
        Err(error) => return Err(error),
    }

    let catalog = read_catalog(&engine)?;
    let mut database = Database::new_cp(config, engine, catalog, cluster)?;
    database.load_persisted_security()?;
    Ok(database)
}

/// Opens a database on the in-process simulated CP adapter for in-crate tests.
///
/// The adapter has no `OpenRaft` runtime or transport: every node acknowledges
/// writes locally and reports itself as leader. Never use it for a deployment.
/// # Errors
/// Fails when config, cluster topology, storage, or metadata validation fails, or persisted
/// security metadata cannot be read or decoded.
#[doc(hidden)]
pub fn open_simulated_cluster_database(
    config: DbConfig,
    cluster: CpClusterConfig,
) -> Result<Database, ConfigError> {
    ensure_cluster_supported(&config, &cluster)?;
    let engine = engine_for(&config)?;
    let read = read_metadata(&engine)?;
    ensure_metadata_matches(&config, read.metadata)?;
    refuse_profile_manifest(&read)?;
    let catalog = read_catalog(&engine)?;
    let mut database = Database::new_cp(config, engine, catalog, cluster)?;
    database.load_persisted_security()?;
    Ok(database)
}

/// Creates a CP/Raft database with operational settings.
///
/// Fails closed like [`create_cluster_database`]: the embedded `Database` API is
/// not wired to the live `OpenRaft` runtime yet.
/// # Errors
/// Fails when config, cluster topology or operational validation fails, and
/// otherwise always returns `ConfigError::Unsupported`.
#[allow(clippy::needless_pass_by_value)]
pub fn create_cluster_database_with_ops(
    config: DbConfig,
    cluster: CpClusterConfig,
    ops: OperationalConfig,
) -> Result<Database, ConfigError> {
    ensure_cluster_supported(&config, &cluster)?;
    validate_operational(&config, &ops)?;
    Err(cluster_database_requires_live_raft())
}

/// Opens a CP/Raft database with operational settings.
///
/// Fails closed like [`open_cluster_database`]: the embedded `Database` API is
/// not wired to the live `OpenRaft` runtime yet.
/// # Errors
/// Fails when config, cluster topology or operational validation fails, and
/// otherwise always returns `ConfigError::Unsupported`.
#[allow(clippy::needless_pass_by_value)]
pub fn open_cluster_database_with_ops(
    config: DbConfig,
    cluster: CpClusterConfig,
    ops: &OperationalConfig,
) -> Result<Database, ConfigError> {
    ensure_cluster_supported(&config, &cluster)?;
    validate_operational(&config, ops)?;
    Err(cluster_database_requires_live_raft())
}

/// Creates a database backed by an AP/Dynamo-style replication backend.
/// # Errors
/// Fails when config, cluster topology, storage, or metadata validation fails, or persisted
/// security metadata cannot be read or decoded.
pub fn create_ap_database(
    config: DbConfig,
    cluster: ApClusterConfig,
) -> Result<Database, ConfigError> {
    ensure_ap_supported(&config, &cluster)?;
    let engine = engine_for(&config)?;

    match read_metadata(&engine) {
        Ok(read) => {
            ensure_metadata_matches(&config, read.metadata)?;
            refuse_profile_manifest(&read)?;
        }
        Err(ConfigError::MissingMetadata { key }) if key == KEY_SCHEMA_VERSION_NAME => {
            write_metadata(&engine, &config)?;
        }
        Err(error) => return Err(error),
    }

    let catalog = read_catalog(&engine)?;
    let mut database = Database::new_ap(config, engine, catalog, cluster)?;
    database.load_persisted_security()?;
    Ok(database)
}

/// Opens a database backed by an AP/Dynamo-style replication backend.
/// # Errors
/// Fails when config, cluster topology, storage, or metadata validation fails, or persisted
/// security metadata cannot be read or decoded.
pub fn open_ap_database(
    config: DbConfig,
    cluster: ApClusterConfig,
) -> Result<Database, ConfigError> {
    ensure_ap_supported(&config, &cluster)?;
    let engine = engine_for(&config)?;
    let read = read_metadata(&engine)?;
    ensure_metadata_matches(&config, read.metadata)?;
    refuse_profile_manifest(&read)?;
    let catalog = read_catalog(&engine)?;
    let mut database = Database::new_ap(config, engine, catalog, cluster)?;
    database.load_persisted_security()?;
    Ok(database)
}

/// Creates an AP/Dynamo database with operational settings.
/// # Errors
/// Fails when config, cluster topology, encryption, storage, or metadata validation fails.
pub fn create_ap_database_with_ops(
    config: DbConfig,
    cluster: ApClusterConfig,
    ops: OperationalConfig,
) -> Result<Database, ConfigError> {
    ensure_ap_supported(&config, &cluster)?;
    validate_operational(&config, &ops)?;
    let engine = engine_for_operational(&config, &ops)?;

    match read_metadata(&engine) {
        Ok(read) => {
            ensure_metadata_matches(&config, read.metadata)?;
            refuse_profile_manifest(&read)?;
        }
        Err(ConfigError::MissingMetadata { key }) if key == KEY_SCHEMA_VERSION_NAME => {
            write_metadata(&engine, &config)?;
        }
        Err(error) => return Err(error),
    }

    let catalog = read_catalog(&engine)?;
    let mut database = Database::new_ap(config, engine, catalog, cluster)?;
    database.encryption.clone_from(&ops.encryption);
    database.apply_tenant_config(ops.tenant.clone());
    database.initialize_security_if_missing(ops.security)?;
    Ok(database)
}

/// Opens an AP/Dynamo database with operational settings.
/// # Errors
/// Fails when config, cluster topology, encryption, storage, or metadata validation fails.
pub fn open_ap_database_with_ops(
    config: DbConfig,
    cluster: ApClusterConfig,
    ops: &OperationalConfig,
) -> Result<Database, ConfigError> {
    ensure_ap_supported(&config, &cluster)?;
    validate_operational(&config, ops)?;
    let engine = engine_for_operational(&config, ops)?;
    let read = read_metadata(&engine)?;
    ensure_metadata_matches(&config, read.metadata)?;
    refuse_profile_manifest(&read)?;
    let catalog = read_catalog(&engine)?;
    let mut database = Database::new_ap(config, engine, catalog, cluster)?;
    database.encryption.clone_from(&ops.encryption);
    let mut security = read_security_from_repl(database.replication_ref())?.unwrap_or_default();
    security.audit.clone_from(&ops.security.audit);
    database.apply_tenant_config(ops.tenant.clone());
    database.apply_security_config(security);
    Ok(database)
}

/// Creates a sharded database backed by multiple replication groups.
/// # Errors
/// Fails when config, shard topology, metadata, or catalog initialization fails, or persisted
/// security metadata cannot be read or decoded.
pub fn create_sharded_database(
    config: DbConfig,
    sharding: ShardedDatabaseConfig,
) -> Result<Database, ConfigError> {
    if config.profile.requires_gpu() {
        return Err(ConfigError::Unsupported(
            "Tensor GPU databases do not support sharding".to_owned(),
        ));
    }
    validate(&config)?;
    let repl = Arc::new(build_sharded_replication(sharding, true)?);

    match read_metadata_from_repl(repl.as_ref()) {
        Ok(read) => {
            ensure_metadata_matches(&config, read.metadata)?;
            refuse_profile_manifest(&read)?;
        }
        Err(ConfigError::MissingMetadata { key }) if key == KEY_SCHEMA_VERSION_NAME => {
            write_metadata_to_repl(repl.as_ref(), &config)?;
        }
        Err(error) => return Err(error),
    }

    let catalog = read_catalog_from_repl(repl.as_ref())?;
    let mut database = Database::new_sharded(config, repl, catalog)?;
    database.load_persisted_security()?;
    Ok(database)
}

/// Opens a sharded database backed by multiple replication groups.
/// # Errors
/// Fails when config, shard topology, metadata, or catalog validation fails, or persisted
/// security metadata cannot be read or decoded.
pub fn open_sharded_database(
    config: DbConfig,
    sharding: ShardedDatabaseConfig,
) -> Result<Database, ConfigError> {
    if config.profile.requires_gpu() {
        return Err(ConfigError::Unsupported(
            "Tensor GPU databases do not support sharding".to_owned(),
        ));
    }
    validate(&config)?;
    let repl = Arc::new(build_sharded_replication(sharding, false)?);
    let read = read_metadata_from_repl(repl.as_ref())?;
    ensure_metadata_matches(&config, read.metadata)?;
    refuse_profile_manifest(&read)?;
    let catalog = read_catalog_from_repl(repl.as_ref())?;
    let mut database = Database::new_sharded(config, repl, catalog)?;
    database.load_persisted_security()?;
    Ok(database)
}

/// Creates a sharded database with operational settings.
/// # Errors
/// Fails when config, shard topology, metadata, or operational settings conflict.
pub fn create_sharded_database_with_ops(
    config: DbConfig,
    sharding: ShardedDatabaseConfig,
    ops: OperationalConfig,
) -> Result<Database, ConfigError> {
    if ops.encryption.is_some() {
        return Err(ConfigError::Unsupported(
            "sharded encryption requires per-shard key configuration".to_owned(),
        ));
    }

    let mut database = create_sharded_database(config, sharding)?;
    database.apply_tenant_config(ops.tenant.clone());
    database.initialize_security_if_missing(ops.security)?;
    Ok(database)
}

/// Opens a sharded database with operational settings.
/// # Errors
/// Fails when config, shard topology, metadata, or operational settings conflict.
pub fn open_sharded_database_with_ops(
    config: DbConfig,
    sharding: ShardedDatabaseConfig,
    ops: &OperationalConfig,
) -> Result<Database, ConfigError> {
    if ops.encryption.is_some() {
        return Err(ConfigError::Unsupported(
            "sharded encryption requires per-shard key configuration".to_owned(),
        ));
    }

    let mut database = open_sharded_database(config, sharding)?;
    let mut security = read_security_from_repl(database.replication_ref())?.unwrap_or_default();
    security.audit.clone_from(&ops.security.audit);
    database.apply_tenant_config(ops.tenant.clone());
    database.apply_security_config(security);
    Ok(database)
}

fn ensure_tensor_gpu_ready(config: &DbConfig) -> Result<(), ConfigError> {
    if config.profile.requires_gpu() {
        crate::tensor_gpu::TensorGpuEngine::shared()
            .map_err(|error| ConfigError::Unsupported(error.to_string()))?;
    }
    Ok(())
}

/// Maps a profile to the concrete storage engine used in this phase.
/// # Errors
/// Fails when the config is invalid or the selected storage cannot open.
pub fn engine_for(config: &DbConfig) -> Result<AnyEngine, ConfigError> {
    validate(config)?;
    ensure_tensor_gpu_ready(config)?;

    if config.profile.is_in_memory() {
        return Ok(AnyEngine::memory());
    }

    let path = config.path.as_ref().ok_or(ConfigError::MissingPath {
        profile: config.profile,
    })?;

    if config.profile == Profile::HighDurability {
        return Ok(AnyEngine::redb_high_durability(path)?);
    }

    Ok(AnyEngine::redb(path)?)
}

/// Maps a profile plus operational encryption settings to a concrete storage engine.
/// # Errors
/// Fails when the config is invalid, the key is invalid, or storage cannot open.
pub fn engine_for_operational(
    config: &DbConfig,
    ops: &OperationalConfig,
) -> Result<AnyEngine, ConfigError> {
    validate_operational(config, ops)?;
    ensure_tensor_gpu_ready(config)?;
    let compression = ops
        .performance
        .as_ref()
        .map(|performance| performance.compression.clone())
        .filter(|compression| compression.algorithm != CompressionAlgorithm::None);

    match (&ops.encryption, compression) {
        (None, None) => engine_for(config),
        (None, Some(compression)) if config.profile.is_in_memory() => {
            Ok(AnyEngine::compressed_memory(compression))
        }
        (None, Some(compression)) => {
            let path = config.path.as_ref().ok_or(ConfigError::MissingPath {
                profile: config.profile,
            })?;
            if config.profile == Profile::HighDurability {
                Ok(AnyEngine::compressed_redb_high_durability(
                    path,
                    compression,
                )?)
            } else {
                Ok(AnyEngine::compressed_redb(path, compression)?)
            }
        }
        (Some(encryption), None) if config.profile.is_in_memory() => {
            encrypted_memory_for(encryption)
        }
        (Some(encryption), None) => {
            let path = config.path.as_ref().ok_or(ConfigError::MissingPath {
                profile: config.profile,
            })?;
            if config.profile == Profile::HighDurability {
                encrypted_redb_high_durability_for(path, encryption)
            } else {
                encrypted_redb_for(path, encryption)
            }
        }
        (Some(encryption), Some(compression)) if config.profile.is_in_memory() => {
            Ok(compressed_encrypted_memory_for(encryption, compression)?)
        }
        (Some(encryption), Some(compression)) => {
            let path = config.path.as_ref().ok_or(ConfigError::MissingPath {
                profile: config.profile,
            })?;
            if config.profile == Profile::HighDurability {
                compressed_encrypted_redb_high_durability_for(path, encryption, compression)
            } else {
                compressed_encrypted_redb_for(path, encryption, compression)
            }
        }
    }
}

fn encrypted_memory_for(encryption: &EncryptionConfig) -> Result<AnyEngine, ConfigError> {
    match &encryption.mode {
        EncryptionMode::LegacyFile => Ok(AnyEngine::encrypted_memory(encryption.key_path.clone())?),
        EncryptionMode::LocalEnvelope {
            keyring_path,
            kek_path,
        } => Ok(AnyEngine::encrypted_memory_envelope(
            keyring_path.clone(),
            kek_path.clone(),
        )?),
    }
}

fn encrypted_redb_for(
    path: impl AsRef<std::path::Path>,
    encryption: &EncryptionConfig,
) -> Result<AnyEngine, ConfigError> {
    match &encryption.mode {
        EncryptionMode::LegacyFile => Ok(AnyEngine::encrypted_redb(
            path,
            encryption.key_path.clone(),
        )?),
        EncryptionMode::LocalEnvelope {
            keyring_path,
            kek_path,
        } => Ok(AnyEngine::encrypted_redb_envelope(
            path,
            keyring_path.clone(),
            kek_path.clone(),
        )?),
    }
}

fn encrypted_redb_high_durability_for(
    path: impl AsRef<std::path::Path>,
    encryption: &EncryptionConfig,
) -> Result<AnyEngine, ConfigError> {
    match &encryption.mode {
        EncryptionMode::LegacyFile => Ok(AnyEngine::encrypted_redb_high_durability(
            path,
            encryption.key_path.clone(),
        )?),
        EncryptionMode::LocalEnvelope {
            keyring_path,
            kek_path,
        } => Ok(AnyEngine::encrypted_redb_high_durability_envelope(
            path,
            keyring_path.clone(),
            kek_path.clone(),
        )?),
    }
}

fn compressed_encrypted_memory_for(
    encryption: &EncryptionConfig,
    compression: crate::performance::CompressionConfig,
) -> Result<AnyEngine, ConfigError> {
    match &encryption.mode {
        EncryptionMode::LegacyFile => Ok(AnyEngine::compressed_encrypted_memory(
            encryption.key_path.clone(),
            compression,
        )?),
        EncryptionMode::LocalEnvelope {
            keyring_path,
            kek_path,
        } => Ok(AnyEngine::compressed_encrypted_memory_envelope(
            keyring_path.clone(),
            kek_path.clone(),
            compression,
        )?),
    }
}

fn compressed_encrypted_redb_for(
    path: impl AsRef<std::path::Path>,
    encryption: &EncryptionConfig,
    compression: crate::performance::CompressionConfig,
) -> Result<AnyEngine, ConfigError> {
    match &encryption.mode {
        EncryptionMode::LegacyFile => Ok(AnyEngine::compressed_encrypted_redb(
            path,
            encryption.key_path.clone(),
            compression,
        )?),
        EncryptionMode::LocalEnvelope {
            keyring_path,
            kek_path,
        } => Ok(AnyEngine::compressed_encrypted_redb_envelope(
            path,
            keyring_path.clone(),
            kek_path.clone(),
            compression,
        )?),
    }
}

fn compressed_encrypted_redb_high_durability_for(
    path: impl AsRef<std::path::Path>,
    encryption: &EncryptionConfig,
    compression: crate::performance::CompressionConfig,
) -> Result<AnyEngine, ConfigError> {
    match &encryption.mode {
        EncryptionMode::LegacyFile => Ok(AnyEngine::compressed_encrypted_redb_high_durability(
            path,
            encryption.key_path.clone(),
            compression,
        )?),
        EncryptionMode::LocalEnvelope {
            keyring_path,
            kek_path,
        } => Ok(
            AnyEngine::compressed_encrypted_redb_high_durability_envelope(
                path,
                keyring_path.clone(),
                kek_path.clone(),
                compression,
            )?,
        ),
    }
}

/// Applies what a single-node database records in `__meta` to the engine and the runtime
/// performance: the storage format of its profile manifest (`docs/profiles.md`, "Persistence and
/// format") and the manifest knobs, re-applied at every open.
///
/// Without a manifest the legacy behaviour is kept: `ops_performance`, else the storage
/// profile's defaults. A legacy database whose values are compression-framed must be opened
/// with an engine that frames values.
fn finish_single_node_open(
    engine: AnyEngine,
    ops_performance: Option<&PerformanceConfig>,
    config: &DbConfig,
    read: &MetadataRead,
) -> Result<(AnyEngine, PerformanceConfig), ConfigError> {
    let Some(manifest) = &read.manifest else {
        if read.values_framed {
            return Err(ConfigError::StorageFormatMismatch(
                LEGACY_FRAMED_DATABASE.to_owned(),
            ));
        }
        let performance = ops_performance
            .cloned()
            .unwrap_or_else(|| PerformanceConfig::for_profile(config.profile));
        return Ok((engine, performance));
    };

    let engine = if manifest.storage_format.value_framing {
        if engine.is_value_framed() {
            // Frames are self-describing: an ops algorithm or level that differs from the
            // manifest still reads every value.
            engine
        } else {
            let compression = ops_performance.map_or_else(
                || manifest.storage_format.compression.clone(),
                |performance| performance.compression.clone(),
            );
            engine.with_value_framing(compression)
        }
    } else if read.values_framed {
        return Err(ConfigError::ProfileManifestInvalid(
            "the manifest records no value framing, but __meta values are compression-framed"
                .to_owned(),
        ));
    } else if engine.is_value_framed() {
        return Err(ConfigError::StorageFormatMismatch(
            "the database was created without value framing: enable compression through a \
             provisioning format upgrade, not OperationalConfig.performance.compression"
                .to_owned(),
        ));
    } else {
        engine
    };

    let performance = match ops_performance {
        Some(performance) => {
            tracing::warn!(
                profile = %manifest.profile,
                digest = %manifest.digest,
                "OperationalConfig.performance overrides the persisted profile manifest for this session"
            );
            performance.clone()
        }
        None => profile_binding::performance_for_manifest(manifest),
    };
    Ok((engine, performance))
}

/// CP, AP and sharded openers: profile manifests are single-node only in this build, and a
/// database whose values are compression-framed needs an engine that frames values.
fn refuse_profile_manifest(read: &MetadataRead) -> Result<(), ConfigError> {
    if read.manifest.is_some() {
        return Err(ConfigError::Unsupported(
            "profile manifests are single-node only in this build".to_owned(),
        ));
    }
    if read.values_framed {
        return Err(ConfigError::StorageFormatMismatch(
            LEGACY_FRAMED_DATABASE.to_owned(),
        ));
    }
    Ok(())
}

fn query_permits_for(performance: &PerformanceConfig) -> Arc<Semaphore> {
    Arc::new(Semaphore::new(
        performance.query.max_concurrent_queries.max(1),
    ))
}

/// The bootstrap `admin` role and the `admin` principal that holds it.
///
/// Password-backed Control Plane sessions and the insecure local admin act as
/// this principal. `Database::ensure_bootstrap_admin_principal` merges this
/// definition into the stored `admin` role and principal, and
/// `Database::has_bootstrap_admin_principal` reports whether that merge would
/// change anything, so a grant change here applies to both.
pub(crate) fn bootstrap_admin_identity() -> (Role, Principal) {
    let role = Role::new("admin")
        .grant(Resource::System, Permission::Admin)
        .grant(Resource::Database, Permission::Read)
        .grant(Resource::Database, Permission::Write)
        .grant(Resource::Database, Permission::Admin);
    let principal = Principal::new("admin").with_role(role.name());
    (role, principal)
}

#[cfg(test)]
fn force_wasm_runtime_init_failure(enabled: bool) {
    runtime::force_wasm_runtime_init_failure(enabled);
}

/// Validates profile and storage-path compatibility.
/// # Errors
/// Fails when the selected profile contradicts the provided path.
pub fn validate(config: &DbConfig) -> Result<(), ConfigError> {
    if config.profile.requires_gpu() && config.replication != ReplicationKind::Cp {
        return Err(ConfigError::Unsupported(
            "the Tensor GPU profile requires native single-node CP storage".to_owned(),
        ));
    }
    if config.profile.is_in_memory() && config.path.is_some() {
        return Err(ConfigError::VolatileWithPath {
            profile: config.profile,
        });
    }

    if config.profile.is_on_disk() && config.path.is_none() {
        return Err(ConfigError::MissingPath {
            profile: config.profile,
        });
    }

    Ok(())
}

/// Validates profile, storage, and operational settings.
/// # Errors
/// Fails when operational settings contradict the selected profile.
pub fn validate_operational(config: &DbConfig, ops: &OperationalConfig) -> Result<(), ConfigError> {
    validate(config)?;

    if !ops.security.audit_enabled {
        return Err(ConfigError::Unsupported(
            "audit can only be disabled through set_audit_enabled_as".to_owned(),
        ));
    }

    if ops.encryption.is_some() {
        match config.profile {
            Profile::InMemory => {
                tracing::info!(
                    "InMemory with encryption is volatile encrypted storage for test/dev"
                );
            }
            Profile::HighDurability if config.path.is_none() => {
                return Err(ConfigError::Unsupported(
                    "HighDurability with encryption requires on-disk storage".to_owned(),
                ));
            }
            _ => {}
        }
    }

    if let Some(performance) = &ops.performance {
        if performance.parallelism.max_threads == 0
            || performance.parallelism.target_partitions == 0
        {
            return Err(ConfigError::Unsupported(
                "performance parallelism limits must be greater than zero".to_owned(),
            ));
        }
        if performance.compression.algorithm != CompressionAlgorithm::None
            && config.profile == Profile::InMemory
        {
            tracing::info!("InMemory compression is volatile and intended for test/dev");
        }
    }

    if ops.cloud.is_some() && config.profile.is_in_memory() {
        return Err(ConfigError::Unsupported(
            "cloud tiering requires durable local storage; InMemory can only use cloud helpers directly in tests"
                .to_owned(),
        ));
    }

    if let Some(tenant) = &ops.tenant {
        if tenant.quota.max_storage_bytes == 0 {
            return Err(ConfigError::Unsupported(
                "tenant storage quota must be greater than zero".to_owned(),
            ));
        }
        if tenant.quota.max_concurrent_queries == 0 {
            return Err(ConfigError::Unsupported(
                "tenant query concurrency quota must be greater than zero".to_owned(),
            ));
        }
        if tenant.quota.max_concurrent_writes == 0 {
            return Err(ConfigError::Unsupported(
                "tenant write concurrency quota must be greater than zero".to_owned(),
            ));
        }
    }

    Ok(())
}

/// Validates a profile and returns the concrete stack chosen by the preset.
/// # Errors
/// Fails when the profile contradicts the supplied storage settings.
pub fn profile_validation_report(
    config: &DbConfig,
) -> Result<ProfileValidationReport, ConfigError> {
    validate(config)?;

    let layout = layout_for(config.profile);
    let engine_kind = if config.profile.is_in_memory() {
        EngineKind::Memory
    } else {
        EngineKind::Redb
    };
    let mut capabilities = BTreeSet::new();

    match config.profile {
        Profile::InMemory => {
            capabilities.insert("volatile");
            capabilities.insert("memory-storage");
        }
        Profile::Transactional => {
            capabilities.insert("row-layout");
            capabilities.insert("mvcc");
            capabilities.insert("btree-indexes");
        }
        Profile::Analytical => {
            capabilities.insert("columnar-layout");
            capabilities.insert("parquet");
            capabilities.insert("parallel-scan");
            capabilities.insert("read-ahead");
        }
        Profile::Document => {
            capabilities.insert("document-indexes");
            capabilities.insert("json-path");
        }
        Profile::Vector => {
            capabilities.insert("hnsw");
            capabilities.insert("vector-search");
        }
        Profile::TimeSeries => {
            capabilities.insert("time-series");
            capabilities.insert("chunked-storage");
            capabilities.insert("retention");
            capabilities.insert("downsampling");
        }
        Profile::HighDurability => {
            capabilities.insert("durable-storage");
            capabilities.insert("redb-commit");
            capabilities.insert("compression-lz4");
        }
        Profile::Tensor => {
            capabilities.insert("tensor-datasets");
            capabilities.insert("typed-tensor-chunks");
            capabilities.insert("gpu-compute-required");
            capabilities.insert("dataset-revisions");
        }
        Profile::Balanced => {
            capabilities.insert("row-layout");
            capabilities.insert("multi-model");
            capabilities.insert("compression-lz4");
            capabilities.insert("group-commit");
        }
    }

    match config.replication {
        ReplicationKind::Cp => {
            capabilities.insert("cp-replication");
        }
        ReplicationKind::Ap => {
            capabilities.insert("ap-replication");
        }
    }

    Ok(ProfileValidationReport {
        profile: config.profile,
        replication: config.replication,
        layout,
        engine_kind,
        durable_storage: config.profile.is_on_disk(),
        capabilities,
    })
}

/// Authorizes the relations of a planned SQL statement for one principal and
/// hands the engine that principal's row and masking policies.
struct SqlReadAuthorizer<'a> {
    database: &'a Database,
    principal: &'a Principal,
    action: &'static str,
    policies: BTreeMap<String, SqlRelationPolicy>,
    protected_views: BTreeSet<String>,
}

impl SqlReadGuard for SqlReadAuthorizer<'_> {
    fn authorize_reads(&mut self, reads: &BTreeSet<SqlReadTarget>) -> Result<(), AuthzError> {
        let resources = if reads.is_empty() {
            BTreeSet::from([Resource::Database])
        } else {
            reads
                .iter()
                .map(|read| match read {
                    SqlReadTarget::Catalog(name) => {
                        resource_for_catalog_name(&self.database.catalog, name)
                    }
                    SqlReadTarget::Builtin(_) => Resource::Database,
                })
                .collect()
        };
        for resource in resources {
            if let Resource::Table(name) = &resource
                && self.protected_views.contains(name)
            {
                return Err(AuthzError::Denied {
                    principal: self.principal.name().to_owned(),
                    resource,
                    permission: Permission::Read,
                });
            }
            self.database.authorize_with_audit(
                self.principal,
                &resource,
                Permission::Read,
                self.action,
            )?;
        }
        Ok(())
    }

    fn relation_policies(&self) -> BTreeMap<String, SqlRelationPolicy> {
        self.policies.clone()
    }
}

/// Translates the row and masking policies of every catalog object SQL can scan into what
/// the engine applies for `principal`. A row policy shows a row whose value at its path
/// equals the policy's value or the principal's name. A table's history holds the table's
/// own rows, so it is read under the table's policies as well as under its own.
fn sql_relation_policies(
    catalog: &BTreeMap<String, CatalogEntry>,
    policy: &PolicyConfig,
    principal: &Principal,
) -> BTreeMap<String, SqlRelationPolicy> {
    let mut policies = BTreeMap::new();
    if policy.row_policies.is_empty() && policy.masking.is_empty() {
        return policies;
    }
    for (name, entry) in catalog {
        if !is_sql_relation(entry) {
            continue;
        }
        let mut resources = Vec::with_capacity(2);
        if let CatalogEntry::TemporalTable { base_table, .. } = entry {
            resources.push(resource_for_catalog_name(catalog, base_table));
        }
        // The relation's own masks come last, so they win over the base table's.
        resources.push(resource_for_catalog_name(catalog, name));
        let mut relation = SqlRelationPolicy::default();
        for resource in &resources {
            let any_of = policy
                .row_policies
                .iter()
                .filter(|row_policy| &row_policy.resource == resource)
                .flat_map(|row_policy| {
                    [
                        (row_policy.path.clone(), row_policy.equals.clone()),
                        (
                            row_policy.path.clone(),
                            Value::Str(principal.name().to_owned()),
                        ),
                    ]
                })
                .collect::<Vec<_>>();
            if !any_of.is_empty() {
                relation.visible_when.push(any_of);
            }
            relation.masks.extend(
                policy
                    .masking
                    .iter()
                    .filter(|masking| &masking.resource == resource)
                    .flat_map(|masking| {
                        masking
                            .paths
                            .iter()
                            .map(|path| (path.clone(), masking.replacement.clone()))
                    }),
            );
        }
        if relation != SqlRelationPolicy::default() {
            policies.insert(name.clone(), relation);
        }
    }
    policies
}

/// Whether the SQL engine registers `entry` as a relation queries can scan.
const fn is_sql_relation(entry: &CatalogEntry) -> bool {
    matches!(
        entry,
        CatalogEntry::Table { .. }
            | CatalogEntry::Collection { .. }
            | CatalogEntry::ForeignTable { .. }
            | CatalogEntry::MaterializedView { .. }
            | CatalogEntry::TemporalTable { .. }
    )
}

fn sql_requirements(
    sql: &str,
    catalog: &BTreeMap<String, CatalogEntry>,
    limits: &QueryExecutionConfig,
) -> Result<Vec<(Resource, Permission)>, DbError> {
    validate_sql_text_size(sql, limits)?;
    if compat::compat_sql_requirements(sql, limits)? {
        return Ok(vec![(Resource::Database, Permission::Read)]);
    }

    if tuning::parse_system_view(sql)?.is_some() {
        return Ok(vec![(Resource::System, Permission::Admin)]);
    }

    if let Some((target, _)) = crate::query::parse_analyze_for_authz(sql)? {
        return Ok(match target {
            AnalyzeTarget::All => vec![(Resource::System, Permission::Admin)],
            AnalyzeTarget::Named(name) => {
                vec![(resource_for_catalog_name(catalog, &name), Permission::Admin)]
            }
        });
    }

    if let Some(requirements) = phase19_requirements(sql, catalog)? {
        return Ok(requirements);
    }

    if let Some(requirements) = extension::extension_sql_requirements(sql)? {
        return Ok(requirements);
    }

    if parse_call_procedure_sql(sql)?.is_some() {
        return Ok(vec![(Resource::Database, Permission::Write)]);
    }

    let statements = parse_with_limits(sql, limits)?;
    if statements.is_empty() {
        return Err(QueryError::Unsupported("empty SQL".to_owned()).into());
    }

    let mut requirements = Vec::new();
    for statement in statements {
        match statement {
            Statement::Query(query) => {
                let mut names = BTreeSet::new();
                collect_query_tables(&query, &mut names)?;
                if names.is_empty() {
                    requirements.push((Resource::Database, Permission::Read));
                } else {
                    requirements.extend(names.into_iter().map(|name| {
                        let name = sql_relation_catalog_name(catalog, &name);
                        (resource_for_catalog_name(catalog, &name), Permission::Read)
                    }));
                }
            }
            Statement::Insert(insert) => {
                let table_name = match &insert.table {
                    SqlTableObject::TableName(name) => object_name_to_string(name)?,
                    other => return Err(QueryError::Unsupported(other.to_string()).into()),
                };
                let resource = resource_for_catalog_name(catalog, &table_name);
                // RETURNING hands back stored rows, as PostgreSQL's SELECT privilege rule says.
                if insert.returning.is_some() {
                    requirements.push((resource.clone(), Permission::Read));
                }
                requirements.push((resource, Permission::Write));
            }
            Statement::Analyze(analyze) => {
                if let Some(name) = analyze.table_name {
                    requirements.push((
                        resource_for_catalog_name(catalog, &object_name_to_string(&name)?),
                        Permission::Admin,
                    ));
                } else {
                    requirements.push((Resource::System, Permission::Admin));
                }
            }
            Statement::Explain { statement, .. } => {
                requirements.extend(sql_requirements(&statement.to_string(), catalog, limits)?);
            }
            other => return Err(QueryError::Unsupported(other.to_string()).into()),
        }
    }

    requirements.sort();
    requirements.dedup();
    Ok(requirements)
}

fn validate_sql_text_size(sql: &str, limits: &QueryExecutionConfig) -> Result<(), QueryError> {
    if sql.len() > limits.max_sql_bytes {
        return Err(QueryError::InputLimit(format!(
            "SQL text is {} bytes, limit is {} bytes",
            sql.len(),
            limits.max_sql_bytes
        )));
    }
    Ok(())
}

fn phase19_requirements(
    sql: &str,
    catalog: &BTreeMap<String, CatalogEntry>,
) -> Result<Option<Vec<(Resource, Permission)>>, QueryError> {
    let Some(call) = parse_phase19_call(sql)? else {
        return Ok(None);
    };
    let requirements = match call.name.as_str() {
        "match" => {
            let name = phase19_resource_name(&call, "match")?;
            index_with_source_requirements(Resource::FullTextIndex(name.clone()), &name, catalog)
        }
        "within_radius" => {
            let name = phase19_resource_name(&call, "within_radius")?;
            index_with_source_requirements(Resource::GeoIndex(name.clone()), &name, catalog)
        }
        "graph_neighbors" => vec![(
            Resource::Graph(phase19_resource_name(&call, "graph_neighbors")?),
            Permission::Read,
        )],
        "time_bucket" => vec![(Resource::Database, Permission::Read)],
        _ => Vec::new(),
    };
    Ok(Some(requirements))
}

/// Index searches return source document text, ids and points, so they also need Read on the
/// source collection, and are refused while it has row or masking policies.
fn index_with_source_requirements(
    index: Resource,
    name: &str,
    catalog: &BTreeMap<String, CatalogEntry>,
) -> Vec<(Resource, Permission)> {
    let source = match catalog.get(name) {
        Some(CatalogEntry::FullTextIndex { config }) => {
            let TextSource::Collection { collection_id, .. } = &config.source;
            Some(*collection_id)
        }
        Some(CatalogEntry::GeoIndex { config }) => Some(config.collection_id),
        _ => None,
    };
    let mut requirements = vec![(index, Permission::Read)];
    if let Some(collection) = source.and_then(|id| collection_name_for_id(catalog, id)) {
        requirements.push((Resource::Collection(collection), Permission::Read));
    }
    requirements
}

fn collection_name_for_id(
    catalog: &BTreeMap<String, CatalogEntry>,
    collection_id: CollectionId,
) -> Option<String> {
    catalog.iter().find_map(|(name, entry)| match entry {
        CatalogEntry::Collection {
            collection_id: existing,
            ..
        } if *existing == collection_id => Some(name.clone()),
        _ => None,
    })
}

fn phase19_resource_name(call: &Phase19Call, function: &str) -> Result<String, QueryError> {
    Ok(call
        .args
        .first()
        .ok_or_else(|| QueryError::Unsupported(format!("{function} requires object name")))?
        .as_str()?
        .to_owned())
}

#[derive(Clone, Debug, PartialEq)]
struct Phase19Call {
    name: String,
    args: Vec<Phase19Arg>,
}

#[derive(Clone, Debug, PartialEq)]
enum Phase19Arg {
    String(String),
    Number(String),
}

impl Phase19Arg {
    fn as_str(&self) -> Result<&str, QueryError> {
        match self {
            Self::String(value) => Ok(value),
            Self::Number(value) => Err(QueryError::InvalidValue(format!(
                "expected string argument, found {value}"
            ))),
        }
    }

    fn as_i64(&self) -> Result<i64, QueryError> {
        match self {
            Self::Number(value) => value
                .parse::<i64>()
                .map_err(|error| QueryError::InvalidValue(error.to_string())),
            Self::String(value) => Err(QueryError::InvalidValue(format!(
                "expected integer argument, found {value}"
            ))),
        }
    }

    fn as_usize(&self) -> Result<usize, QueryError> {
        match self {
            Self::Number(value) => value
                .parse::<usize>()
                .map_err(|error| QueryError::InvalidValue(error.to_string())),
            Self::String(value) => Err(QueryError::InvalidValue(format!(
                "expected unsigned integer argument, found {value}"
            ))),
        }
    }

    fn as_f64(&self) -> Result<f64, QueryError> {
        match self {
            Self::Number(value) => value
                .parse::<f64>()
                .map_err(|error| QueryError::InvalidValue(error.to_string())),
            Self::String(value) => Err(QueryError::InvalidValue(format!(
                "expected float argument, found {value}"
            ))),
        }
    }
}

fn parse_phase19_call(sql: &str) -> Result<Option<Phase19Call>, QueryError> {
    let dialect = PostgreSqlDialect {};
    let Ok(statements) = Parser::parse_sql(&dialect, sql) else {
        return Ok(None);
    };
    if statements.len() != 1 {
        return Ok(None);
    }
    let Statement::Query(query) = &statements[0] else {
        return Ok(None);
    };
    phase19_call_from_query(query)
}

fn phase19_call_from_query(query: &SqlQuery) -> Result<Option<Phase19Call>, QueryError> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    for item in &select.projection {
        if let Some(call) = phase19_call_from_select_item(item)? {
            return Ok(Some(call));
        }
    }
    for table in &select.from {
        if let Some(call) = phase19_call_from_table_factor(&table.relation)? {
            return Ok(Some(call));
        }
    }
    Ok(None)
}

fn phase19_call_from_select_item(item: &SelectItem) -> Result<Option<Phase19Call>, QueryError> {
    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            phase19_call_from_expr(expr)
        }
        SelectItem::ExprWithAliases { expr, .. } => phase19_call_from_expr(expr),
        SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => Ok(None),
    }
}

fn phase19_call_from_table_factor(
    factor: &SqlTableFactor,
) -> Result<Option<Phase19Call>, QueryError> {
    match factor {
        SqlTableFactor::Table {
            name,
            args: Some(args),
            ..
        } => phase19_call_from_name_args(name, &args.args),
        SqlTableFactor::Function { name, args, .. } => phase19_call_from_name_args(name, args),
        SqlTableFactor::TableFunction { expr, .. } => phase19_call_from_expr(expr),
        _ => Ok(None),
    }
}

fn phase19_call_from_expr(expr: &SqlExpr) -> Result<Option<Phase19Call>, QueryError> {
    match expr {
        SqlExpr::Function(function) => match &function.args {
            FunctionArguments::List(args) => {
                phase19_call_from_name_args(&function.name, &args.args)
            }
            FunctionArguments::None => phase19_call_from_name_args(&function.name, &[]),
            FunctionArguments::Subquery(_) => Ok(None),
        },
        SqlExpr::Nested(expr) => phase19_call_from_expr(expr),
        _ => Ok(None),
    }
}

fn phase19_call_from_name_args(
    name: &ObjectName,
    args: &[FunctionArg],
) -> Result<Option<Phase19Call>, QueryError> {
    let Some(name) = object_name_last(name) else {
        return Ok(None);
    };
    let name = name.to_ascii_lowercase();
    if !matches!(
        name.as_str(),
        "graph_neighbors" | "within_radius" | "time_bucket" | "match"
    ) {
        return Ok(None);
    }
    Ok(Some(Phase19Call {
        name,
        args: args
            .iter()
            .map(phase19_arg_from_function_arg)
            .collect::<Result<Vec<_>, _>>()?,
    }))
}

fn phase19_arg_from_function_arg(arg: &FunctionArg) -> Result<Phase19Arg, QueryError> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => phase19_arg_from_expr(expr),
        _ => Err(QueryError::Unsupported(
            "these SQL helper functions require literal arguments".to_owned(),
        )),
    }
}

fn phase19_arg_from_expr(expr: &SqlExpr) -> Result<Phase19Arg, QueryError> {
    match expr {
        SqlExpr::Value(value) => phase19_arg_from_sql_value(&value.value),
        SqlExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match phase19_arg_from_expr(expr)? {
            Phase19Arg::Number(value) => Ok(Phase19Arg::Number(format!("-{value}"))),
            Phase19Arg::String(value) => Err(QueryError::InvalidValue(format!(
                "expected numeric argument, found {value}"
            ))),
        },
        SqlExpr::Nested(expr) => phase19_arg_from_expr(expr),
        _ => Err(QueryError::Unsupported(
            "these SQL helper functions require literal arguments".to_owned(),
        )),
    }
}

fn phase19_arg_from_sql_value(value: &SqlValue) -> Result<Phase19Arg, QueryError> {
    match value {
        SqlValue::Number(value, _) => Ok(Phase19Arg::Number(value.clone())),
        SqlValue::SingleQuotedString(value)
        | SqlValue::TripleSingleQuotedString(value)
        | SqlValue::EscapedStringLiteral(value)
        | SqlValue::UnicodeStringLiteral(value) => Ok(Phase19Arg::String(value.clone())),
        other => Err(QueryError::Unsupported(format!(
            "unsupported SQL helper literal {other}"
        ))),
    }
}

fn object_name_last(name: &ObjectName) -> Option<&str> {
    name.0.last().and_then(|part| match part {
        ObjectNamePart::Identifier(ident) => Some(ident.value.as_str()),
        ObjectNamePart::Function(_) => None,
    })
}

fn collect_query_tables(query: &SqlQuery, names: &mut BTreeSet<String>) -> Result<(), QueryError> {
    collect_query_tables_scoped(query, names, &BTreeSet::new())
}

/// Collects the catalog names a query reads, skipping names that resolve to a
/// CTE in scope. Every clause is walked; relation shapes that cannot be mapped
/// to a catalog name fail closed with `Unsupported`.
fn collect_query_tables_scoped(
    query: &SqlQuery,
    names: &mut BTreeSet<String>,
    cte_names: &BTreeSet<String>,
) -> Result<(), QueryError> {
    let SqlQuery {
        with,
        body,
        order_by,
        limit_clause,
        fetch,
        locks,
        for_clause,
        settings,
        format_clause,
        pipe_operators,
    } = query;

    // CTE names are normalized like DataFusion does: quoted names keep their
    // case, unquoted names are lowercased. A CTE is only in scope after its
    // own body, so a self-reference (even under WITH RECURSIVE) still counts
    // as a table read.
    let mut scoped_ctes = cte_names.clone();
    if let Some(with) = with {
        for cte in &with.cte_tables {
            collect_query_tables_scoped(&cte.query, names, &scoped_ctes)?;
            scoped_ctes.insert(normalized_ident(&cte.alias.name));
        }
    }

    collect_set_expr_tables(body, names, &scoped_ctes)?;
    collect_nested_tables(order_by, names, &scoped_ctes)?;
    collect_nested_tables(limit_clause, names, &scoped_ctes)?;
    collect_nested_tables(fetch, names, &scoped_ctes)?;
    collect_nested_tables(locks, names, &scoped_ctes)?;
    collect_nested_tables(for_clause, names, &scoped_ctes)?;
    collect_nested_tables(settings, names, &scoped_ctes)?;
    collect_nested_tables(format_clause, names, &scoped_ctes)?;
    collect_nested_tables(pipe_operators, names, &scoped_ctes)
}

fn collect_set_expr_tables(
    expr: &SetExpr,
    names: &mut BTreeSet<String>,
    cte_names: &BTreeSet<String>,
) -> Result<(), QueryError> {
    match expr {
        SetExpr::Select(select) => collect_nested_tables(select.as_ref(), names, cte_names),
        SetExpr::Query(query) => collect_query_tables_scoped(query, names, cte_names),
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr_tables(left, names, cte_names)?;
            collect_set_expr_tables(right, names, cte_names)
        }
        SetExpr::Values(values) => collect_nested_tables(values, names, cte_names),
        other => Err(QueryError::Unsupported(other.to_string())),
    }
}

/// Walks every node of `node` and collects the relations it reads. Nested
/// queries are handed to `collect_query_tables_scoped` so their WITH scopes are
/// honoured.
fn collect_nested_tables<T: Visit + ?Sized>(
    node: &T,
    names: &mut BTreeSet<String>,
    cte_names: &BTreeSet<String>,
) -> Result<(), QueryError> {
    let mut collector = NestedTableCollector {
        names,
        cte_names,
        depth: 0,
    };
    match node.visit(&mut collector) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(*error),
    }
}

struct NestedTableCollector<'a> {
    names: &'a mut BTreeSet<String>,
    cte_names: &'a BTreeSet<String>,
    /// Number of enclosing nested queries. Relations inside a nested query
    /// were already collected when that query was entered.
    depth: usize,
}

impl Visitor for NestedTableCollector<'_> {
    type Break = Box<QueryError>;

    fn pre_visit_query(&mut self, query: &SqlQuery) -> ControlFlow<Self::Break> {
        if self.depth == 0
            && let Err(error) = collect_query_tables_scoped(query, self.names, self.cte_names)
        {
            return ControlFlow::Break(Box::new(error));
        }
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &SqlQuery) -> ControlFlow<Self::Break> {
        self.depth = self.depth.saturating_sub(1);
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &SqlTableFactor) -> ControlFlow<Self::Break> {
        if self.depth > 0 {
            return ControlFlow::Continue(());
        }
        match factor {
            SqlTableFactor::Table { name, .. } => {
                let [ObjectNamePart::Identifier(ident)] = name.0.as_slice() else {
                    return ControlFlow::Break(Box::new(QueryError::Unsupported(name.to_string())));
                };
                let name = normalized_ident(ident);
                if !self.cte_names.contains(&name) {
                    self.names.insert(name);
                }
                ControlFlow::Continue(())
            }
            SqlTableFactor::Derived { .. } => ControlFlow::Continue(()),
            other => ControlFlow::Break(Box::new(QueryError::Unsupported(other.to_string()))),
        }
    }

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
        ControlFlow::Break(Box::new(QueryError::Unsupported(statement.to_string())))
    }
}

/// Normalizes an identifier the way `DataFusion` resolves it.
fn normalized_ident(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

/// Maps a relation name as `DataFusion` resolves it (see `normalized_ident`) to the catalog
/// object the planner reads under that name. The engine registers every SQL relation under
/// its lowercased catalog name, so `orders`, `ORDERS` and `"orders"` all read the object
/// `ORDERS`, while `"ORDERS"` reads nothing. A name that reads no object is returned as is;
/// planning then fails.
fn sql_relation_catalog_name(catalog: &BTreeMap<String, CatalogEntry>, resolved: &str) -> String {
    if resolved.bytes().any(|byte| byte.is_ascii_uppercase())
        || catalog.get(resolved).is_some_and(is_sql_relation)
    {
        return resolved.to_owned();
    }
    catalog
        .iter()
        .find(|(name, entry)| is_sql_relation(entry) && name.eq_ignore_ascii_case(resolved))
        .map_or_else(|| resolved.to_owned(), |(name, _)| name.clone())
}

fn apply_procedure_commands(
    repl: &dyn Replication,
    commands: Vec<ProcedureCommand>,
) -> Result<ProcedureResult, DbError> {
    let mut ops = Vec::new();
    let mut rows = None;
    for command in commands {
        match command {
            ProcedureCommand::Put { table, key, value } => {
                ops.push(Op::Put { table, key, value });
            }
            ProcedureCommand::Delete { table, key } => {
                ops.push(Op::Delete { table, key });
            }
            ProcedureCommand::Rows { schema, rows: data } => {
                if rows.is_some() || !ops.is_empty() {
                    return Err(ContinuousError::Invalid(
                        "procedure rows result cannot be mixed with writes".to_owned(),
                    )
                    .into());
                }
                rows = Some((schema, data));
            }
        }
    }
    if let Some((schema, rows)) = rows {
        return Ok(ProcedureResult::Rows { schema, rows });
    }
    let affected = ops.len();
    if affected > 0 {
        repl.propose_batch(ops)?;
    }
    Ok(ProcedureResult::AffectedRows(affected))
}

fn parse_as_of_query(sql: &str) -> Result<Option<(String, TemporalPoint)>, DbError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    if let Some(index) = lower.rfind(" as of lsn ") {
        let point = trimmed[index + " as of lsn ".len()..].trim();
        let lsn = point
            .parse::<TxnId>()
            .map_err(|error| QueryError::Parse(format!("invalid AS OF LSN: {error}")))?;
        return Ok(Some((
            trimmed[..index].trim().to_owned(),
            TemporalPoint::Lsn(lsn),
        )));
    }
    if let Some(index) = lower.rfind(" as of timestamp ") {
        let point = trimmed[index + " as of timestamp ".len()..].trim();
        let millis = point
            .parse::<u64>()
            .map_err(|error| QueryError::Parse(format!("invalid AS OF TIMESTAMP: {error}")))?;
        return Ok(Some((
            trimmed[..index].trim().to_owned(),
            TemporalPoint::Timestamp(UNIX_EPOCH + Duration::from_millis(millis)),
        )));
    }
    Ok(None)
}

fn parse_call_procedure_sql(sql: &str) -> Result<Option<(String, Vec<Value>)>, DbError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("call ") {
        return Ok(None);
    }
    let expr = trimmed["call ".len()..].trim();
    let Some(open) = expr.find('(') else {
        return Err(QueryError::Parse("CALL requires argument list".to_owned()).into());
    };
    if !expr.ends_with(')') {
        return Err(QueryError::Parse("CALL argument list is not closed".to_owned()).into());
    }
    let name = expr[..open].trim().trim_matches('"').to_owned();
    let args = expr[open + 1..expr.len() - 1].trim();
    let values = if args.is_empty() {
        Vec::new()
    } else {
        extension::split_args(args)?
            .into_iter()
            .map(|arg| extension::parse_literal_value(&arg))
            .collect::<Result<Vec<_>, _>>()?
    };
    Ok(Some((name, values)))
}

fn phase32_declaration_kind(sql: &str) -> Option<&'static str> {
    let words = sql
        .split_whitespace()
        .take(4)
        .map(|word| {
            word.trim_matches(|character: char| character == ';' || character == '(')
                .to_ascii_lowercase()
        })
        .collect::<Vec<_>>();
    match words.as_slice() {
        [create, foreign, table, ..]
            if create == "create" && foreign == "foreign" && table == "table" =>
        {
            Some("CREATE FOREIGN TABLE")
        }
        [create, trigger, ..] if create == "create" && trigger == "trigger" => {
            Some("CREATE TRIGGER")
        }
        [create, procedure, ..] if create == "create" && procedure == "procedure" => {
            Some("CREATE PROCEDURE")
        }
        [create, continuous, query, ..]
            if create == "create" && continuous == "continuous" && query == "query" =>
        {
            Some("CREATE CONTINUOUS QUERY")
        }
        [create, materialized, view, ..]
            if create == "create" && materialized == "materialized" && view == "view" =>
        {
            Some("CREATE MATERIALIZED VIEW")
        }
        [create, temporal, table, ..]
            if create == "create" && temporal == "temporal" && table == "table" =>
        {
            Some("CREATE TEMPORAL TABLE")
        }
        _ => None,
    }
}

fn procedure_result_to_sql(result: ProcedureResult) -> SqlOutput {
    match result {
        ProcedureResult::AffectedRows(rows) => SqlOutput::AffectedRows(rows),
        ProcedureResult::Rows { schema, rows } => SqlOutput::Rows(SqlRows {
            columns: schema
                .columns
                .into_iter()
                .map(|column| column.name)
                .collect(),
            rows,
        }),
    }
}

fn resource_for_catalog_name(catalog: &BTreeMap<String, CatalogEntry>, name: &str) -> Resource {
    let lower = name.to_ascii_lowercase();
    if lower.starts_with("information_schema.") || lower.starts_with("pg_catalog.") {
        return Resource::Database;
    }

    match catalog.get(name) {
        Some(CatalogEntry::Collection { .. } | CatalogEntry::TensorDataset { .. }) => {
            Resource::Collection(name.to_owned())
        }
        Some(CatalogEntry::Vector { .. }) => Resource::VectorCollection(name.to_owned()),
        Some(CatalogEntry::FullTextIndex { .. }) => Resource::FullTextIndex(name.to_owned()),
        Some(CatalogEntry::TimeSeries { .. }) => Resource::TimeSeries(name.to_owned()),
        Some(CatalogEntry::Graph { .. }) => Resource::Graph(name.to_owned()),
        Some(CatalogEntry::GeoIndex { .. }) => Resource::GeoIndex(name.to_owned()),
        Some(
            CatalogEntry::Table { .. }
            | CatalogEntry::ForeignTable { .. }
            | CatalogEntry::MaterializedView { .. }
            | CatalogEntry::TemporalTable { .. },
        )
        | None => Resource::Table(name.to_owned()),
    }
}

fn policy_resources(requirements: &[(Resource, Permission)]) -> Vec<&Resource> {
    requirements
        .iter()
        .filter_map(|(resource, permission)| {
            if *permission == Permission::Read {
                Some(resource)
            } else {
                None
            }
        })
        .filter(|resource| {
            matches!(
                resource,
                Resource::Table(_) | Resource::Collection(_) | Resource::VectorCollection(_)
            )
        })
        .collect()
}

fn policy_has_row_or_masking(policy: &PolicyConfig, resource: &Resource) -> bool {
    policy
        .masking
        .iter()
        .any(|masking| &masking.resource == resource)
        || policy
            .row_policies
            .iter()
            .any(|row_policy| &row_policy.resource == resource)
}

fn resource_for_changefeed_target(target: &LogicalTarget) -> Option<Resource> {
    match target {
        LogicalTarget::Database | LogicalTarget::System(_) => None,
        LogicalTarget::Table(name) => Some(Resource::Table(name.clone())),
        LogicalTarget::Collection(name) => Some(Resource::Collection(name.clone())),
        LogicalTarget::VectorCollection(name) => Some(Resource::VectorCollection(name.clone())),
        LogicalTarget::FullTextIndex(name) => Some(Resource::FullTextIndex(name.clone())),
        LogicalTarget::TimeSeries(name) => Some(Resource::TimeSeries(name.clone())),
        LogicalTarget::Graph(name) => Some(Resource::Graph(name.clone())),
        LogicalTarget::GeoIndex(name) => Some(Resource::GeoIndex(name.clone())),
    }
}

fn row_to_policy_object(columns: &[String], row: &[Value]) -> Value {
    let fields = columns
        .iter()
        .zip(row.iter())
        .map(|(column, value)| (column.clone(), value.clone()))
        .collect();
    Value::Object(fields)
}

fn policy_object_to_row(columns: &[String], value: &Value, fallback: Vec<Value>) -> Vec<Value> {
    let Value::Object(fields) = value else {
        return fallback;
    };
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            fields
                .get(column)
                .cloned()
                .unwrap_or_else(|| fallback.get(index).cloned().unwrap_or(Value::Null))
        })
        .collect()
}

fn object_name_to_string(name: &ObjectName) -> Result<String, QueryError> {
    let [part] = name.0.as_slice() else {
        return Err(QueryError::Unsupported(name.to_string()));
    };

    match part {
        ObjectNamePart::Identifier(identifier) => Ok(identifier.value.clone()),
        ObjectNamePart::Function(_) => Err(QueryError::Unsupported(name.to_string())),
    }
}

fn build_sharded_replication(
    sharding: ShardedDatabaseConfig,
    create: bool,
) -> Result<ShardedReplication, ConfigError> {
    let ShardedDatabaseConfig {
        strategy,
        global_shard,
        shards: specs,
    } = sharding;

    if specs.is_empty() {
        return Err(ConfigError::InvalidClusterConfig(
            "sharded database needs at least one shard".to_owned(),
        ));
    }

    let mut seen = BTreeSet::new();
    let mut shards = BTreeMap::new();
    for spec in specs {
        if !seen.insert(spec.id) {
            return Err(ConfigError::InvalidClusterConfig(format!(
                "duplicate shard id {}",
                spec.id
            )));
        }
        shards.insert(spec.id, build_shard_backend(&spec.backend, create)?);
    }

    if !shards.contains_key(&global_shard) {
        return Err(ConfigError::InvalidClusterConfig(format!(
            "global shard {global_shard} is not configured"
        )));
    }

    let shard_map = ShardMap::balanced(strategy, shards.keys().copied())?;
    Ok(ShardedReplication::new(shard_map, shards, global_shard)?)
}

fn build_shard_backend(
    backend: &ShardBackendConfig,
    create: bool,
) -> Result<Arc<dyn Replication>, ConfigError> {
    match backend {
        ShardBackendConfig::Local(config) => {
            Ok(Arc::new(open_local_shard(config.clone(), create)?))
        }
        ShardBackendConfig::Cp { config, cluster } => {
            let database = if create {
                create_cluster_database(config.clone(), cluster.clone())?
            } else {
                open_cluster_database(config.clone(), cluster.clone())?
            };
            Ok(Arc::new(database))
        }
        ShardBackendConfig::Ap { config, cluster } => {
            let database = if create {
                create_ap_database(config.clone(), cluster.clone())?
            } else {
                open_ap_database(config.clone(), cluster.clone())?
            };
            Ok(Arc::new(database))
        }
    }
}

/// Opens one local shard, initializing its metadata when `create` is set. A local shard is a
/// plain single-node database, except that a profile manifest on any shard is refused before
/// the shard database is built. Like `open_database`, it restores the shard's persisted security
/// state.
fn open_local_shard(config: DbConfig, create: bool) -> Result<Database, ConfigError> {
    let engine = engine_for(&config)?;

    match read_metadata(&engine) {
        Ok(read) => {
            ensure_metadata_matches(&config, read.metadata)?;
            refuse_profile_manifest(&read)?;
        }
        Err(ConfigError::MissingMetadata { key }) if create && key == KEY_SCHEMA_VERSION_NAME => {
            write_metadata(&engine, &config)?;
        }
        Err(error) => return Err(error),
    }

    let catalog = read_catalog(&engine)?;
    let performance = PerformanceConfig::for_profile(config.profile);
    let mut database = Database::new_with_performance(config, engine, catalog, performance)?;
    database.load_persisted_security()?;
    Ok(database)
}

fn ensure_cluster_supported(
    config: &DbConfig,
    cluster: &CpClusterConfig,
) -> Result<(), ConfigError> {
    if config.replication == ReplicationKind::Ap {
        return Err(ConfigError::Unsupported(
            "AP cluster replication is not supported by this preview".to_owned(),
        ));
    }

    validate_cp_cluster_config(cluster, false).map_err(ConfigError::InvalidClusterConfig)
}

fn cluster_database_requires_live_raft() -> ConfigError {
    ConfigError::Unsupported(
        "multi-node CP cluster databases are not available through the embedded Database API: \
         it is not wired to the live OpenRaft runtime, so writes would be acknowledged without \
         replication; use repl::start_cp_cluster for replicated CP storage, or \
         create_database/open_database for a single node (also opens data written by earlier \
         versions)"
            .to_owned(),
    )
}

fn ensure_ap_supported(config: &DbConfig, cluster: &ApClusterConfig) -> Result<(), ConfigError> {
    if config.replication != ReplicationKind::Ap {
        return Err(ConfigError::Unsupported(
            "AP database creation requires ReplicationKind::Ap".to_owned(),
        ));
    }

    validate_ap_cluster_config(cluster).map_err(ConfigError::InvalidClusterConfig)
}

#[must_use]
pub const fn layout_for(profile: Profile) -> TableLayout {
    match profile {
        Profile::Analytical | Profile::Tensor => TableLayout::Columnar,
        Profile::InMemory
        | Profile::Transactional
        | Profile::Document
        | Profile::Vector
        | Profile::TimeSeries
        | Profile::HighDurability
        | Profile::Balanced => TableLayout::Row,
    }
}

const fn cost_profile_for(profile: Profile) -> CostProfile {
    match profile {
        Profile::InMemory => CostProfile::InMemory,
        Profile::Analytical | Profile::Tensor => CostProfile::Analytical,
        Profile::Transactional | Profile::HighDurability => CostProfile::Transactional,
        Profile::Document | Profile::Vector | Profile::TimeSeries | Profile::Balanced => {
            CostProfile::Balanced
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Metadata {
    schema_version: u32,
    profile: Profile,
    replication: ReplicationKind,
    layout: TableLayout,
}

/// `__meta` as the frame-tolerant reader decoded it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct MetadataRead {
    metadata: Metadata,
    manifest: Option<PersistedProfileManifest>,
    /// A `__meta` value still carried a compression frame after the engine read it: the database
    /// frames values but the engine that read them does not. A framing engine strips every frame
    /// itself, so this is only ever set when the engine does not frame values.
    values_framed: bool,
}

impl MetadataRead {
    /// The legacy metadata `write_metadata` just wrote for `config`.
    fn written(config: &DbConfig) -> Self {
        Self {
            metadata: Metadata {
                schema_version: SCHEMA_VERSION,
                profile: config.profile,
                replication: config.replication,
                layout: layout_for(config.profile),
            },
            manifest: None,
            values_framed: false,
        }
    }
}

#[derive(Serialize, serde::Deserialize)]
struct StoredAuthzPolicy {
    roles: Vec<StoredRole>,
}

#[derive(Serialize, serde::Deserialize)]
struct StoredRole {
    name: String,
    grants: Vec<StoredGrant>,
}

#[derive(Serialize, serde::Deserialize)]
struct StoredGrant {
    resource: Resource,
    permissions: Vec<Permission>,
}

fn write_metadata<S: StorageEngine>(engine: &S, config: &DbConfig) -> Result<(), ConfigError> {
    let mut txn = engine.begin_write()?;
    write_json(&mut txn, KEY_SCHEMA_VERSION, &SCHEMA_VERSION)?;
    write_json(&mut txn, KEY_PROFILE, &config.profile)?;
    write_json(&mut txn, KEY_REPLICATION, &config.replication)?;
    write_json(&mut txn, KEY_LAYOUT, &layout_for(config.profile))?;
    txn.commit()?;
    Ok(())
}

fn read_metadata<S: StorageEngine>(engine: &S) -> Result<MetadataRead, ConfigError> {
    let txn = engine.begin_read()?;
    decode_metadata(|key| Ok(txn.get(META_TABLE, key)?))
}

/// The frame-tolerant `__meta` reader behind `read_metadata` and `read_metadata_from_repl`.
///
/// Every value passes through `decode_stored_value` before serde, so compression-framed
/// metadata decodes and is reported as `values_framed`. `schema_version` 1 and 2 are accepted;
/// the profile manifest is decoded and re-validated against the rest of `__meta`.
fn decode_metadata(
    get: impl Fn(&[u8]) -> Result<Option<Bytes>, ConfigError>,
) -> Result<MetadataRead, ConfigError> {
    let mut framed = false;
    let schema_version: u32 = required_meta_value(
        &get,
        KEY_SCHEMA_VERSION,
        KEY_SCHEMA_VERSION_NAME,
        &mut framed,
    )?;

    if !(SCHEMA_VERSION..=SCHEMA_VERSION_FRAMED).contains(&schema_version) {
        return Err(ConfigError::SchemaVersionMismatch {
            expected: SCHEMA_VERSION_FRAMED,
            found: schema_version,
        });
    }

    let profile = required_meta_value(&get, KEY_PROFILE, KEY_PROFILE_NAME, &mut framed)?;
    let replication =
        required_meta_value(&get, KEY_REPLICATION, KEY_REPLICATION_NAME, &mut framed)?;
    let layout = match get(KEY_LAYOUT)? {
        Some(raw) => decode_meta_value(&raw, KEY_LAYOUT_NAME, &mut framed)?,
        None => layout_for(profile),
    };
    let manifest = match get(KEY_PROFILE_MANIFEST)? {
        Some(raw) => Some(profile_binding::decode_profile_manifest(
            &decode_meta_bytes(&raw, &mut framed)?,
        )?),
        None => None,
    };

    let metadata = Metadata {
        schema_version,
        profile,
        replication,
        layout,
    };
    check_metadata_format(metadata, manifest.as_ref())?;
    Ok(MetadataRead {
        metadata,
        manifest,
        values_framed: framed,
    })
}

/// `schema_version` 2 exists only together with a value-framing profile manifest, and a
/// manifest is re-validated against `__meta` on every read.
fn check_metadata_format(
    metadata: Metadata,
    manifest: Option<&PersistedProfileManifest>,
) -> Result<(), ConfigError> {
    let value_framing = manifest.is_some_and(|manifest| manifest.storage_format.value_framing);
    if metadata.schema_version == SCHEMA_VERSION_FRAMED && !value_framing {
        return Err(ConfigError::ProfileManifestInvalid(
            "schema_version 2 requires a profile manifest with storage_format.value_framing"
                .to_owned(),
        ));
    }
    if value_framing && metadata.schema_version != SCHEMA_VERSION_FRAMED {
        return Err(ConfigError::ProfileManifestInvalid(format!(
            "storage_format.value_framing requires schema_version 2, found {}",
            metadata.schema_version
        )));
    }
    if let Some(manifest) = manifest {
        profile_binding::validate_manifest(manifest, metadata.profile)?;
    }
    Ok(())
}

fn required_meta_value<T: DeserializeOwned>(
    get: &impl Fn(&[u8]) -> Result<Option<Bytes>, ConfigError>,
    key: &[u8],
    key_name: &'static str,
    framed: &mut bool,
) -> Result<T, ConfigError> {
    let raw = get(key)?.ok_or(ConfigError::MissingMetadata { key: key_name })?;
    decode_meta_value(&raw, key_name, framed)
}

/// Decodes one raw `__meta` value: strips a compression frame (recording it in `framed`), then
/// parses the JSON. A frame that does not decode is storage corruption; JSON that does not parse
/// is `MetadataUnreadable`.
fn decode_meta_value<T: DeserializeOwned>(
    raw: &[u8],
    key: &'static str,
    framed: &mut bool,
) -> Result<T, ConfigError> {
    let bytes = decode_meta_bytes(raw, framed)?;
    serde_json::from_slice(&bytes).map_err(|error| ConfigError::MetadataUnreadable {
        key,
        reason: if bytes.starts_with(ENCRYPTED_FRAME_PREFIX) {
            format!("the value carries an encryption frame (MDBE): {error}")
        } else {
            error.to_string()
        },
    })
}

fn decode_meta_bytes(raw: &[u8], framed: &mut bool) -> Result<Bytes, ConfigError> {
    if is_framed(raw) {
        *framed = true;
    }
    Ok(decode_stored_value(raw)?)
}

fn ensure_metadata_matches(config: &DbConfig, metadata: Metadata) -> Result<(), ConfigError> {
    if metadata.profile != config.profile {
        return Err(ConfigError::ProfileMismatch {
            expected: config.profile,
            found: metadata.profile,
        });
    }

    if metadata.replication != config.replication {
        return Err(ConfigError::ReplicationMismatch {
            expected: config.replication,
            found: metadata.replication,
        });
    }

    let expected_layout = layout_for(config.profile);
    if metadata.layout != expected_layout {
        return Err(ConfigError::LayoutMismatch {
            expected: expected_layout,
            found: metadata.layout,
        });
    }

    Ok(())
}

fn read_catalog<S: StorageEngine>(
    engine: &S,
) -> Result<BTreeMap<String, CatalogEntry>, ConfigError> {
    let txn = engine.begin_read()?;
    txn.range(CATALOG_TABLE, &[], &[0xFF])?
        .map(|entry| {
            let (key, value) = entry?;
            let name =
                String::from_utf8(key).map_err(|error| ConfigError::Serde(error.to_string()))?;
            let entry = serde_json::from_slice(&value)
                .map_err(|error| ConfigError::Serde(error.to_string()))?;
            Ok((name, entry))
        })
        .collect()
}

fn write_metadata_to_repl(repl: &dyn Replication, config: &DbConfig) -> Result<(), ConfigError> {
    propose_system_batch(
        repl,
        vec![
            meta_put_op(KEY_SCHEMA_VERSION, &SCHEMA_VERSION)?,
            meta_put_op(KEY_PROFILE, &config.profile)?,
            meta_put_op(KEY_REPLICATION, &config.replication)?,
            meta_put_op(KEY_LAYOUT, &layout_for(config.profile))?,
        ],
    )?;
    Ok(())
}

fn read_metadata_from_repl(repl: &dyn Replication) -> Result<MetadataRead, ConfigError> {
    decode_metadata(|key| Ok(repl.read(META_TABLE, key, ReadConsistency::Strong)?))
}

/// Reads one catalog entry straight from `__catalog__`.
/// # Errors
/// Fails when the read fails or the entry cannot be decoded.
pub(crate) fn read_catalog_entry_from_repl(
    repl: &dyn Replication,
    name: &str,
) -> Result<Option<CatalogEntry>, ConfigError> {
    repl.read(CATALOG_TABLE, name.as_bytes(), ReadConsistency::Strong)?
        .map(|value| {
            serde_json::from_slice(&value).map_err(|error| ConfigError::Serde(error.to_string()))
        })
        .transpose()
}

fn read_catalog_from_repl(
    repl: &dyn Replication,
) -> Result<BTreeMap<String, CatalogEntry>, ConfigError> {
    repl.range(CATALOG_TABLE, &[], &[0xFF], ReadConsistency::Strong)?
        .into_iter()
        .map(|(key, value)| {
            let name =
                String::from_utf8(key).map_err(|error| ConfigError::Serde(error.to_string()))?;
            let entry = serde_json::from_slice(&value)
                .map_err(|error| ConfigError::Serde(error.to_string()))?;
            Ok((name, entry))
        })
        .collect()
}

fn write_security_to_repl(
    repl: &dyn Replication,
    security: &SecurityConfig,
) -> Result<(), ConfigError> {
    propose_system_batch(
        repl,
        vec![
            authz_put_op(
                KEY_AUTHZ_POLICY,
                &StoredAuthzPolicy::from(&security.authz_policy),
            )?,
            authz_put_op(KEY_PRINCIPAL_REGISTRY, &security.principals)?,
            authz_put_op(KEY_AUDIT_ENABLED, &security.audit_enabled)?,
        ],
    )?;
    Ok(())
}

fn read_security_from_repl(repl: &dyn Replication) -> Result<Option<SecurityConfig>, ConfigError> {
    let Some(policy_bytes) = repl.read(AUTHZ_TABLE, KEY_AUTHZ_POLICY, ReadConsistency::Strong)?
    else {
        return Ok(None);
    };

    let stored_policy: StoredAuthzPolicy = serde_json::from_slice(&policy_bytes)
        .map_err(|error| ConfigError::Serde(error.to_string()))?;
    let authz_policy = AuthzPolicy::from(stored_policy);

    let principals =
        match repl.read(AUTHZ_TABLE, KEY_PRINCIPAL_REGISTRY, ReadConsistency::Strong)? {
            Some(bytes) => serde_json::from_slice(&bytes)
                .map_err(|error| ConfigError::Serde(error.to_string()))?,
            None => PrincipalRegistry::default(),
        };

    let audit_enabled = match repl.read(AUTHZ_TABLE, KEY_AUDIT_ENABLED, ReadConsistency::Strong)? {
        Some(bytes) => {
            serde_json::from_slice(&bytes).map_err(|error| ConfigError::Serde(error.to_string()))?
        }
        None => true,
    };

    Ok(Some(SecurityConfig {
        authz_policy,
        principals,
        audit_enabled,
        audit: AuditConfig::default(),
    }))
}

impl From<&AuthzPolicy> for StoredAuthzPolicy {
    fn from(policy: &AuthzPolicy) -> Self {
        Self {
            roles: policy
                .roles()
                .values()
                .map(|role| StoredRole {
                    name: role.name().to_owned(),
                    grants: role
                        .grants()
                        .iter()
                        .map(|(resource, permissions)| StoredGrant {
                            resource: resource.clone(),
                            permissions: permissions.iter().copied().collect(),
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

impl From<StoredAuthzPolicy> for AuthzPolicy {
    fn from(stored: StoredAuthzPolicy) -> Self {
        AuthzPolicy::new(stored.roles.into_iter().map(|stored_role| {
            let mut role = Role::new(stored_role.name);
            for grant in stored_role.grants {
                for permission in grant.permissions {
                    role = role.grant(grant.resource.clone(), permission);
                }
            }
            role
        }))
    }
}

fn meta_put_op<T: Serialize>(key: &[u8], value: &T) -> Result<Op, ConfigError> {
    let value = serde_json::to_vec(value).map_err(|error| ConfigError::Serde(error.to_string()))?;
    Ok(Op::Put {
        table: META_TABLE.to_owned(),
        key: key.to_vec(),
        value,
    })
}

fn authz_put_op<T: Serialize>(key: &[u8], value: &T) -> Result<Op, ConfigError> {
    let value = serde_json::to_vec(value).map_err(|error| ConfigError::Serde(error.to_string()))?;
    Ok(Op::Put {
        table: AUTHZ_TABLE.to_owned(),
        key: key.to_vec(),
        value,
    })
}

fn parse_audit_key_file(bytes: &[u8]) -> Result<[u8; 32], String> {
    if bytes.len() == 32 {
        let mut key = [0; 32];
        key.copy_from_slice(bytes);
        return Ok(key);
    }

    let text = std::str::from_utf8(bytes)
        .map(str::trim)
        .map_err(|_| "audit key file is not valid utf-8".to_owned())?;
    if text.len() != 64 {
        return Err("audit key file must contain 32 raw bytes or 64 hex characters".to_owned());
    }

    let mut key = [0; 32];
    for (index, chunk) in text.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let high = audit_hex_value(chunk[0])?;
        let low = audit_hex_value(chunk[1])?;
        key[index] = (high << 4) | low;
    }
    Ok(key)
}

fn audit_hex_value(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err("audit key file contains invalid hex".to_owned()),
    }
}

fn catalog_put_op(name: &str, entry: &CatalogEntry) -> Result<Op, DbError> {
    let value = serde_json::to_vec(entry).map_err(|error| DbError::Serde(error.to_string()))?;
    Ok(Op::Put {
        table: CATALOG_TABLE.to_owned(),
        key: name.as_bytes().to_vec(),
        value,
    })
}

fn next_collection_id_op(next: u32) -> Result<Op, DbError> {
    Ok(meta_put_op(KEY_NEXT_COLLECTION_ID, &next)?)
}

fn next_graph_id_op(next: u32) -> Result<Op, DbError> {
    Ok(meta_put_op(KEY_NEXT_GRAPH_ID, &next)?)
}

fn validate_catalog_name(name: &str) -> Result<(), DbError> {
    if name.is_empty() {
        return Err(DbError::InvalidCatalogName("empty name".to_owned()));
    }
    if !is_valid_catalog_name(name) {
        return Err(DbError::InvalidCatalogName(name.to_owned()));
    }
    Ok(())
}

/// The catalog naming rule every `create_*` call enforces: an ASCII letter or `_`, then ASCII
/// letters, digits or `_`, at most 128 bytes. Spec validation applies the same rule.
pub(crate) fn is_valid_catalog_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && name.len() <= 128
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn kind_mismatch(name: &str, expected: &'static str, entry: &CatalogEntry) -> DbError {
    DbError::CatalogKindMismatch {
        name: name.to_owned(),
        expected,
        found: match entry {
            CatalogEntry::Table { .. } => "table",
            CatalogEntry::Collection { .. } => "collection",
            CatalogEntry::Vector { .. } => "vector collection",
            CatalogEntry::FullTextIndex { .. } => "full-text index",
            CatalogEntry::TimeSeries { .. } => "time-series collection",
            CatalogEntry::Graph { .. } => "graph",
            CatalogEntry::GeoIndex { .. } => "geo index",
            CatalogEntry::ForeignTable { .. } => "foreign table",
            CatalogEntry::MaterializedView { .. } => "materialized view",
            CatalogEntry::TemporalTable { .. } => "temporal table",
            CatalogEntry::TensorDataset { .. } => "tensor dataset",
        },
    }
}

fn write_json<T: serde::Serialize>(
    txn: &mut impl WriteTransaction,
    key: &[u8],
    value: &T,
) -> Result<(), ConfigError> {
    let bytes = serde_json::to_vec(value).map_err(|error| ConfigError::Serde(error.to_string()))?;
    txn.put(META_TABLE, key, &bytes)?;
    Ok(())
}

#[cfg(test)]
mod read_many_tests;

#[cfg(test)]
mod tests;
