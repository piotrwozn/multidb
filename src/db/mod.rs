use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
    thread,
    time::{Duration, Instant, UNIX_EPOCH},
};

use serde::{Serialize, de::DeserializeOwned};
use sqlparser::{
    ast::{
        Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
        JoinConstraint, JoinOperator, ObjectName, ObjectNamePart, Query as SqlQuery, SelectItem,
        SetExpr, Statement, TableFactor as SqlTableFactor, TableObject as SqlTableObject,
        UnaryOperator, Value as SqlValue,
    },
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use tokio::sync::Semaphore;

use crate::autoscale::ResourcePolicy;
use crate::cdc::{
    self, ChangeEvent, ChangeOp, ChangefeedFilter, ChangefeedOptions, ChangefeedPage, FeedError,
    HookError, HookSpec, HookedReplication, LogicalTarget, MaterializedViewError,
    MaterializedViewRows, MaterializedViewSpec, ResumeToken, SubscriptionConfig, SubscriptionError,
    SubscriptionState,
};
use crate::cloud::{
    CloudConfig, QuotaReplication, TenantConfig, TenantId, TenantRuntime, repl_quota_error,
};
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
    CollectionId, DOCUMENT_TABLE, DocumentCollection, DocumentId, IndexSpec, ModelError, Predicate,
    QueryResult, Value, collection_range_bounds, decode_value, encode_value,
};
use crate::observability;
use crate::observability::MetricsConfig;
use crate::performance::{
    BenchmarkReport, CompressionAlgorithm, PerformanceCache, PerformanceConfig, RegressionGate,
};
use crate::query::{
    AnalyzeMode, AnalyzeReport, AnalyzeTarget, CostProfile, DocField, ExplainOptions,
    ExplainReport, PlanCache, PlanCacheMetrics, PlannerFeedback, QueryError,
    REL_COLUMNAR_SEGMENTS_TABLE, REL_ROWS_TABLE, RelIndexSpec, RelTable, Row, SqlEngine, SqlOutput,
    SqlRows, StatsCatalog, TableLayout, TableSchema, decode_row_bytes, schema_put_op,
};
use crate::repl::{
    ApClusterConfig, ApDynamo, ConditionalBatch, CpClusterConfig, CpRaft, HealingAction,
    HealingBackend, HealingMode, HealingPolicy, HealthConfig, HealthProbe, HealthView,
    ManualHealthProbe, NodeId, Op, PartitionStrategy, RaftNode, ReadConsistency, ReplError,
    Replication, SelfHealingController, ShardId, ShardMap, ShardedReplication, SingleNode,
    VectorClock, VersionedBytes, propose_system_batch, validate_ap_cluster_config,
    validate_cp_cluster_config,
};
use crate::runtime_advisor::{
    RuntimeAdviceDecision, RuntimeAdviceDecisionRequest, RuntimeAdviceReport, RuntimeAdvisor,
    RuntimeAdvisorError,
};
use crate::security::{
    AUDIT_HEAD_TABLE, AUDIT_TABLE, AuditEvent, AuditHead, AuditOutcome, AuditSink, AuthzError,
    AuthzPolicy, FileAuditSink, Permission, Principal, PrincipalRegistry, Resource, Role,
};
use crate::storage::{
    AnyEngine, AnyReadTxn, Bytes, EngineKind, ReadTransaction, StorageEngine, StorageError,
    WriteTransaction,
};
use crate::temporal::{TemporalError, TemporalPoint, TemporalRetention};
use crate::text::{FullTextIndex, FullTextIndexConfig, TextError};
use crate::timeseries::{TimeSeriesCollection, TimeSeriesConfig, TimeSeriesError, time_bucket};
use crate::tuning::{
    self, IndexAdviceReport, IndexAdvisor, ReprofileJob, ReprofilePlan, ReprofileStatus,
    TuningDecision, TuningError, TuningLogEntry, TuningPolicy, TuningSystemView, WorkloadProfiler,
    WorkloadReport, WorkloadSample, WorkloadWindow,
};
use crate::txn::{self, IsolationLevel, TxnId, TxnOptions, WriteKey, WriteSet};
use crate::vector::{
    DiskAnnConfig, HnswParams, QuantizationConfig, SharedVectorIndex, VectorCollection,
    VectorCollectionConfig, VectorError, VectorIndexState, VectorMetric,
};

mod runtime;

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
const KEY_NEXT_COLLECTION_ID: &[u8] = b"next_collection_id";
const KEY_NEXT_GRAPH_ID: &[u8] = b"next_graph_id";
const SCHEMA_VERSION: u32 = 1;
const AUTHZ_TABLE: &str = "__authz_policy";
const KEY_AUTHZ_POLICY: &[u8] = b"policy";
const KEY_PRINCIPAL_REGISTRY: &[u8] = b"principals";
const KEY_AUDIT_ENABLED: &[u8] = b"audit_enabled";
const ADMIN_AUTH_TABLE: &str = "__admin_auth";

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
}

impl Profile {
    #[must_use]
    pub const fn all() -> [Self; 8] {
        [
            Self::InMemory,
            Self::Transactional,
            Self::Analytical,
            Self::Document,
            Self::Vector,
            Self::TimeSeries,
            Self::HighDurability,
            Self::Balanced,
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
}

#[derive(thiserror::Error, Debug)]
pub enum DbError {
    #[error("config: {0}")]
    Config(#[from] ConfigError),

    #[error("query: {0}")]
    Query(#[from] QueryError),

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

impl DbError {
    #[must_use]
    pub fn is_conflict(&self) -> bool {
        matches!(
            self,
            Self::Storage(StorageError::Conflict)
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
    encryption: Option<EncryptionConfig>,
    plan_cache: Arc<Mutex<PlanCache>>,
    performance: PerformanceConfig,
    performance_cache: PerformanceCache,
    query_permits: Arc<Semaphore>,
    tenant: Option<TenantRuntime>,
    wasm_runtime: WasmRuntime,
    vector_indexes: Arc<Mutex<BTreeMap<CollectionId, SharedVectorIndex>>>,
}

pub type DatabaseSelfHealingController =
    SelfHealingController<DatabaseHealingBackend, ManualHealthProbe>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShardBackendConfig {
    Local(DbConfig),
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

    const fn is_ap(&self) -> bool {
        matches!(self, Self::ApDynamo(_))
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

    #[must_use]
    pub const fn performance_config(&self) -> &PerformanceConfig {
        &self.performance
    }

    #[must_use]
    pub const fn encryption_config(&self) -> Option<&EncryptionConfig> {
        self.encryption.as_ref()
    }

    /// Rotates the current data encryption key for an envelope-encrypted database.
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
    /// # Errors
    /// Fails when the database is not encrypted or the key id is unknown.
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

    pub fn set_authz_policy(&mut self, policy: AuthzPolicy) {
        self.authz = policy;
    }

    pub fn set_principal_registry(&mut self, registry: PrincipalRegistry) {
        self.principals = registry;
    }

    /// Ensures the bootstrap admin identity can use password-backed sessions.
    /// # Errors
    /// Fails when the updated security metadata cannot be persisted.
    pub fn ensure_bootstrap_admin_principal(&mut self) -> Result<(), DbError> {
        let admin_role = Role::new("admin")
            .grant(Resource::System, Permission::Admin)
            .grant(Resource::Database, Permission::Read)
            .grant(Resource::Database, Permission::Write)
            .grant(Resource::Database, Permission::Admin);
        self.authz = self.authz.clone().allow(admin_role);
        self.principals
            .insert("admin", Principal::new("admin").with_role("admin"));
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
    /// data, catalog state, indexes, extensions, or runtime configuration.
    /// # Errors
    /// Fails when authorization fails, audit is disabled, or the audit write fails.
    pub fn confirm_config_apply_as(
        &self,
        principal: &Principal,
        plan: &MigrationPlan,
        confirm: &str,
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
        let detail = format!(
            "plan_id: {}; status: {:?}; confirmation_matched: {}; data_mutated: false",
            report.plan_id, report.status, report.confirmation_matched
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
    /// Disabling audit writes an audit event before the flag is persisted.
    /// # Errors
    /// Fails when authorization, audit, or metadata persistence fails.
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
        if !enabled {
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

        let repl: &dyn Replication = self;
        Ok(DocumentCollection::with_indexes(
            repl,
            collection_id,
            returned_indexes,
        ))
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
    /// # Errors
    /// Fails when authorization, catalog, or index metadata persistence fails.
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
    /// # Errors
    /// Fails when authorization, catalog, or index validation fails.
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
        let name = name.into();
        self.ensure_name_available(&name)?;
        let layout = self.layout();
        let table = RelTable::handle_with_layout(
            self.replication_handle(),
            name.clone(),
            schema,
            indexes,
            layout,
        )?;

        let mut ops = Vec::new();
        if let Some(schema) = table.schema() {
            ops.push(schema_put_op(table.name(), schema)?);
        }

        let entry = CatalogEntry::Table {
            indexes: table.indexes().to_vec(),
            layout,
        };
        ops.push(catalog_put_op(&name, &entry)?);
        propose_system_batch(&self.repl, ops)?;
        self.catalog.insert(name, entry);
        self.invalidate_plan_cache();

        Ok(table)
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
    /// Fails when the base table is missing, untyped, or the history name collides.
    pub fn enable_system_versioning(
        &mut self,
        table: &str,
        retention: TemporalRetention,
    ) -> Result<String, DbError> {
        let base = self.table(table)?;
        let schema = base.schema().cloned().ok_or_else(|| {
            QueryError::InvalidSchema("temporal tables require schema".to_owned())
        })?;
        let history_name = format!("{table}_history");
        self.ensure_name_available(&history_name)?;
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

        let repl: &dyn Replication = self;
        Ok(DocumentCollection::with_indexes(
            repl,
            collection_id,
            indexes,
        ))
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
    /// # Errors
    /// Fails when the name collides or storage rejects metadata.
    pub fn create_full_text_index(
        &mut self,
        config: FullTextIndexConfig,
    ) -> Result<FullTextIndex<'_, dyn Replication + '_>, DbError> {
        let name = config.name.clone();
        self.ensure_name_available(&name)?;
        {
            let repl: &dyn Replication = self;
            FullTextIndex::new(repl, config.clone())?;
        }
        let entry = CatalogEntry::FullTextIndex {
            config: config.clone(),
        };
        let mut ops = FullTextIndex::<dyn Replication>::metadata_ops(&config)?;
        ops.push(catalog_put_op(&name, &entry)?);
        propose_system_batch(&self.repl, ops)?;
        self.catalog.insert(name, entry);
        self.invalidate_plan_cache();
        let repl: &dyn Replication = self;
        Ok(FullTextIndex::new(repl, config)?)
    }

    /// Creates a named time-series collection and persists it in the catalog.
    /// # Errors
    /// Fails when the name collides or metadata cannot be written.
    pub fn create_time_series(
        &mut self,
        config: TimeSeriesConfig,
    ) -> Result<TimeSeriesCollection<'_, dyn Replication + '_>, DbError> {
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
    /// # Errors
    /// Fails when the name collides or index validation fails.
    pub fn create_geo_index(
        &mut self,
        config: GeoIndexConfig,
    ) -> Result<GeoIndex<'_, dyn Replication + '_>, DbError> {
        let name = config.name.clone();
        self.ensure_name_available(&name)?;
        {
            let repl: &dyn Replication = self;
            let _ = GeoIndex::new(repl, config.clone())?;
        }
        let entry = CatalogEntry::GeoIndex {
            config: config.clone(),
        };
        let mut ops = GeoIndex::<dyn Replication>::metadata_ops(&config)?;
        ops.push(catalog_put_op(&name, &entry)?);
        propose_system_batch(&self.repl, ops)?;
        self.catalog.insert(name, entry);
        self.invalidate_plan_cache();
        let repl: &dyn Replication = self;
        Ok(GeoIndex::new(repl, config)?)
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
                let repl: &dyn Replication = self;
                Ok(DocumentCollection::with_indexes(
                    repl,
                    *collection_id,
                    indexes.clone(),
                ))
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
                Ok(FullTextIndex::new(repl, config.clone())?)
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
                Ok(GeoIndex::new(repl, config.clone())?)
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
    /// # Errors
    /// Fails when workload metadata cannot be persisted.
    pub fn record_workload_sample(&self, sample: &WorkloadSample) -> Result<(), DbError> {
        Ok(WorkloadProfiler::record(self.replication_ref(), sample)?)
    }

    /// Reads the persisted workload profile.
    /// # Errors
    /// Fails when workload metadata cannot be read.
    pub fn workload_report(&self, window: WorkloadWindow) -> Result<WorkloadReport, DbError> {
        Ok(WorkloadProfiler::report(self.replication_ref(), window)?)
    }

    /// Reads workload profile after checking system read permission.
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
            Permission::Read,
            "workload_report",
        )?;
        self.workload_report(window)
    }

    /// Builds and persists index recommendations from workload observations.
    /// # Errors
    /// Fails when workload metadata or advice storage fails.
    pub fn index_advice(&self) -> Result<IndexAdviceReport, DbError> {
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
    /// # Errors
    /// Fails when advisor metadata or dry-run planning cannot be read.
    pub fn runtime_advice(&self) -> Result<RuntimeAdviceReport, DbError> {
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
    /// # Errors
    /// Fails when the advice cannot be generated or the id is unknown.
    pub fn runtime_advice_plan(&self, advice_id: &str) -> Result<MigrationPlan, DbError> {
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
    /// # Errors
    /// Fails when the token is invalid or CDC metadata cannot be read.
    pub fn poll_changefeed(
        &self,
        token: &ResumeToken,
        filter: &ChangefeedFilter,
        options: &ChangefeedOptions,
        max: usize,
    ) -> Result<(Vec<ChangeEvent>, ResumeToken), DbError> {
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
    /// # Errors
    /// Fails when the token is invalid or CDC metadata cannot be read.
    pub fn poll_changefeed_page(
        &self,
        token: &ResumeToken,
        filter: &ChangefeedFilter,
        options: &ChangefeedOptions,
        max: usize,
    ) -> Result<ChangefeedPage, DbError> {
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
        cdc::authorize_filter_access(&self.authz, principal, filter)?;
        let (events, next) = self.poll_changefeed(token, filter, options, max)?;
        let policy = self.policy_config()?;
        let events = events
            .into_iter()
            .map(|event| self.apply_changefeed_policy(principal, &policy, event))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();
        Ok((events, next))
    }

    /// Polls one page of committed change events after checking RBAC and applying policies.
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
        cdc::authorize_filter_access(&self.authz, principal, filter)?;
        let mut page = self.poll_changefeed_page(token, filter, options, max)?;
        let policy = self.policy_config()?;
        page.events = page
            .events
            .into_iter()
            .map(|event| self.apply_changefeed_policy(principal, &policy, event))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();
        Ok(page)
    }

    /// Creates or replaces a named CDC subscription after checking RBAC.
    /// # Errors
    /// Fails when authorization or subscription metadata persistence fails.
    pub fn create_subscription_as(
        &self,
        principal: &Principal,
        config: SubscriptionConfig,
    ) -> Result<SubscriptionState, DbError> {
        Ok(cdc::create_subscription(
            self.replication_ref(),
            &self.authz,
            principal,
            config,
        )?)
    }

    /// Creates or replaces a named CDC subscription from trusted embedded/admin code.
    /// # Errors
    /// Fails when subscription metadata persistence fails.
    pub fn create_subscription(
        &self,
        config: SubscriptionConfig,
    ) -> Result<SubscriptionState, DbError> {
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
        let state = cdc::subscription_state(self.replication_ref(), name)?;
        cdc::authorize_filter_access(&self.authz, principal, &state.config.filter)?;
        Ok(cdc::ack_subscription(self.replication_ref(), name, token)?)
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
        cdc::authorize_filter_access(&self.authz, principal, &state.config.filter)?;
        Ok(state)
    }

    /// Creates a continuous query on top of the durable CDC subscription machinery.
    /// # Errors
    /// Fails when metadata or subscription persistence fails.
    pub fn create_continuous_query(
        &self,
        spec: &ContinuousQuerySpec,
    ) -> Result<ContinuousQueryState, DbError> {
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
        let state = self.create_continuous_query(spec)?;
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
        let state = continuous::ack_continuous_query(self.replication_ref(), name, token.clone())?;
        cdc::ack_subscription(self.replication_ref(), name, token)?;
        Ok(state)
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
        let state = cdc::subscription_state(self.replication_ref(), name)?;
        cdc::authorize_filter_access(&self.authz, principal, &state.config.filter)?;
        self.ack_continuous_query(name, token)
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
        Ok(cdc::create_materialized_view(self, spec)?)
    }

    /// Creates an incremental materialized view and registers it in the query catalog.
    /// # Errors
    /// Fails when the view is unsupported, source schema is missing, or catalog persistence fails.
    pub fn create_materialized_view_object(
        &mut self,
        spec: &MaterializedViewSpec,
    ) -> Result<MaterializedViewRows, DbError> {
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
    /// Fails when hook metadata cannot be written.
    pub fn register_hook(&self, hook: &HookSpec) -> Result<(), DbError> {
        Ok(cdc::register_hook(self.replication_ref(), hook)?)
    }

    /// Deletes a CDC hook.
    /// # Errors
    /// Fails when hook metadata cannot be deleted.
    pub fn unregister_hook(&self, name: &str) -> Result<(), DbError> {
        Ok(cdc::unregister_hook(self.replication_ref(), name)?)
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
    /// # Errors
    /// Fails when the table is missing, WASM validation fails, or metadata cannot be written.
    pub fn register_wasm_trigger(
        &self,
        mut spec: TriggerSpec,
        wasm: &[u8],
    ) -> Result<TriggerSpec, DbError> {
        let _ = self.table(&spec.table)?;
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
        apply_procedure_commands(self.replication_ref(), commands)
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
        let started = Instant::now();
        let _query_permit = self.acquire_query_permit().await.map_err(DbError::from)?;
        if let Some((rewritten, point)) = parse_as_of_query(sql)? {
            let result = self.query_as_of(&rewritten, point).await;
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
        if let Some(output) =
            compat::execute_compat_sql(sql, &self.config, &self.compat_catalog_snapshot())?
        {
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
        let result = engine.execute(sql).await.map_err(DbError::from);
        self.finish_query(sql, started, result)
    }

    /// Executes a SQL query against an MVCC snapshot.
    /// # Errors
    /// Fails when the temporal point is outside retention, unsupported, or execution fails.
    pub async fn query_as_of(&self, sql: &str, point: TemporalPoint) -> Result<SqlOutput, DbError> {
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
        let engine = self.sql_engine()?.with_snapshot_lsn(lsn);
        Ok(engine.execute(sql).await?)
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
        self.record_workload_sample(&sample)
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
        let requirements = sql_requirements(sql, &self.catalog)?;
        for (resource, permission) in &requirements {
            self.authorize_or_audit(principal, resource, *permission, "query")?;
        }

        let result = self
            .query(sql)
            .await
            .and_then(|output| self.apply_query_policies(principal, output, &requirements));
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
        let requirements = sql_requirements(sql, &self.catalog)?;
        for (resource, permission) in &requirements {
            self.authorize_or_audit(principal, resource, *permission, "query_as_of")?;
        }

        let result = self
            .query_as_of(sql, point)
            .await
            .and_then(|output| self.apply_query_policies(principal, output, &requirements));
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

    fn apply_query_policies(
        &self,
        principal: &Principal,
        output: SqlOutput,
        requirements: &[(Resource, Permission)],
    ) -> Result<SqlOutput, DbError> {
        let SqlOutput::Rows(rows) = output else {
            return Ok(output);
        };
        let policy = self.policy_config()?;
        let resources = policy_resources(requirements);
        if resources.len() != 1 {
            if resources
                .iter()
                .any(|resource| policy_has_row_or_masking(&policy, resource))
            {
                return Err(ExtensionError::PolicyDenied(
                    "multi-resource queries with row or masking policies require column lineage"
                        .to_owned(),
                )
                .into());
            }
            return Ok(SqlOutput::Rows(rows));
        }
        let resource = resources[0];
        let mut filtered = Vec::new();
        for row in rows.rows {
            let object = row_to_policy_object(&rows.columns, &row);
            if !policy.row_visible(principal, resource, &object) {
                continue;
            }
            let masked = policy.mask_value(resource, &object);
            filtered.push(policy_object_to_row(&rows.columns, &masked, row));
        }
        Ok(SqlOutput::Rows(crate::query::SqlRows {
            columns: rows.columns,
            rows: filtered,
        }))
    }

    fn apply_changefeed_policy(
        &self,
        principal: &Principal,
        policy: &PolicyConfig,
        mut event: ChangeEvent,
    ) -> Result<Option<ChangeEvent>, DbError> {
        let Some(resource) = resource_for_changefeed_target(&event.target) else {
            return Ok(Some(event));
        };
        if !policy_has_row_or_masking(policy, &resource) {
            return Ok(Some(event));
        }

        let ChangeOp::Upsert { value_after, key } = event.op else {
            return Ok(Some(event));
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
        let key = self.audit_key()?;
        let mut previous_hash = None;
        let mut expected_sequence = 0_u64;
        for event in self.audit_events()? {
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
    /// Returns the closure error without writing anything, or a storage error on commit.
    pub fn transaction<T>(
        &self,
        f: impl FnOnce(&mut MultiModelTxn<'_>) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        self.transaction_with_options(TxnOptions::default(), f)
    }

    /// Runs one transactional attempt with explicit isolation options.
    /// # Errors
    /// Returns the closure error without writing anything, or a storage error on commit.
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

    fn new(
        config: DbConfig,
        engine: AnyEngine,
        catalog: BTreeMap<String, CatalogEntry>,
    ) -> Result<Self, ConfigError> {
        let performance = PerformanceConfig::for_profile(config.profile);
        Self::new_with_performance(config, engine, catalog, performance)
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
            encryption: None,
            plan_cache: Arc::new(Mutex::new(PlanCache::default())),
            performance,
            performance_cache,
            query_permits,
            tenant: None,
            wasm_runtime,
            vector_indexes: Arc::new(Mutex::new(BTreeMap::new())),
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
            encryption: None,
            plan_cache: Arc::new(Mutex::new(PlanCache::default())),
            performance,
            performance_cache,
            query_permits,
            tenant: None,
            wasm_runtime,
            vector_indexes: Arc::new(Mutex::new(BTreeMap::new())),
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
            encryption: None,
            plan_cache: Arc::new(Mutex::new(PlanCache::default())),
            performance,
            performance_cache,
            query_permits,
            tenant: None,
            wasm_runtime,
            vector_indexes: Arc::new(Mutex::new(BTreeMap::new())),
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
            encryption: None,
            plan_cache: Arc::new(Mutex::new(PlanCache::default())),
            performance,
            performance_cache,
            query_permits,
            tenant: None,
            wasm_runtime,
            vector_indexes: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    async fn acquire_query_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, QueryError> {
        tokio::time::timeout(
            std::time::Duration::from_millis(self.performance.query.queue_timeout_ms),
            self.query_permits.clone().acquire_owned(),
        )
        .await
        .map_err(|_| {
            QueryError::ResourceLimit(format!(
                "query queue exceeded {} ms",
                self.performance.query.queue_timeout_ms
            ))
        })?
        .map_err(|_| QueryError::ResourceLimit("query governor closed".to_owned()))
    }

    fn replication_handle(&self) -> Arc<dyn Replication> {
        let handle: Arc<dyn Replication> =
            Arc::new(HookedReplication::new(self.repl.replication_handle()));
        self.tenant.as_ref().map_or(handle.clone(), |tenant| {
            Arc::new(QuotaReplication::new(handle, tenant.clone())) as Arc<dyn Replication>
        })
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

    fn sql_engine(&self) -> Result<SqlEngine, DbError> {
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
                | CatalogEntry::GeoIndex { .. } => {}
            }
        }

        Ok(engine)
    }

    fn validate_temporal_retention_for_sql(&self, sql: &str, lsn: TxnId) -> Result<(), DbError> {
        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|error| QueryError::Parse(error.to_string()))?;
        let mut names = BTreeSet::new();
        for statement in statements {
            if let Statement::Query(query) = statement {
                collect_query_tables(&query, &mut names)?;
            }
        }
        for name in names {
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
                | CatalogEntry::GeoIndex { .. } => None,
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
                | CatalogEntry::GeoIndex { .. } => None,
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
                Err(DbError::AuthzDenied(error))
            }
        }
    }

    fn record_audit(&self, event: &AuditEvent) -> Result<(), DbError> {
        if !self.audit_enabled {
            return Ok(());
        }

        let _audit_guard = self
            .audit_lock
            .lock()
            .map_err(|_| DbError::AuditIntegrity("audit lock poisoned".to_owned()))?;
        let key = self.audit_key()?;
        let head = self.audit_head()?;
        let sequence = head
            .sequence
            .checked_add(1)
            .ok_or_else(|| DbError::AuditIntegrity("audit sequence overflow".to_owned()))?;
        let event = event
            .clone()
            .with_integrity(head.hash.clone(), sequence, &key)
            .map_err(DbError::AuditIntegrity)?;
        let new_head = AuditHead {
            sequence,
            hash: event
                .integrity
                .as_ref()
                .map(|integrity| integrity.hash.clone()),
        };
        let value =
            serde_json::to_vec(&event).map_err(|error| DbError::Serde(error.to_string()))?;
        let head_value =
            serde_json::to_vec(&new_head).map_err(|error| DbError::Serde(error.to_string()))?;
        let result = propose_system_batch(
            &self.repl,
            vec![
                Op::Put {
                    table: AUDIT_TABLE.to_owned(),
                    key: event.key(),
                    value,
                },
                Op::Put {
                    table: AUDIT_HEAD_TABLE.to_owned(),
                    key: b"head".to_vec(),
                    value: head_value,
                },
            ],
        );
        observability::record_audit_write(if result.is_ok() { "ok" } else { "error" });
        result?;
        if let Some(anchor_path) = &self.audit_config.anchor_path {
            FileAuditSink::new(anchor_path)
                .append(&event)
                .map_err(DbError::AuditIntegrity)?;
        }
        Ok(())
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

    fn apply_tenant_config(&mut self, tenant: Option<TenantConfig>) {
        self.tenant = tenant.map(|tenant| {
            let runtime = TenantRuntime::new(tenant);
            runtime.reconcile_storage_bytes(self.estimate_tenant_storage_bytes());
            runtime
        });
    }

    fn estimate_tenant_storage_bytes(&self) -> u64 {
        [
            CATALOG_TABLE,
            REL_ROWS_TABLE,
            REL_COLUMNAR_SEGMENTS_TABLE,
            DOCUMENT_TABLE,
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

enum SnapshotLookup {
    Missing,
    Found(Option<Bytes>),
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
        if let SnapshotLookup::Found(value) = self.read_mvcc_snapshot_raw(table, key)? {
            return Ok(value);
        }
        Ok(self.read.get(table, key)?)
    }

    fn read_mvcc_snapshot_raw(&self, table: &str, key: &[u8]) -> Result<SnapshotLookup, DbError> {
        let prefix = txn::version_key(table, key)?;
        let end = keyenc::range_end(&prefix);
        let mut selected = None;
        let rows = self
            .read
            .range(txn::TXN_MVCC_TABLE, &prefix, &end)?
            .collect::<Result<Vec<_>, _>>()?;
        for (version_key, value) in rows {
            if version_key.len() != prefix.len() + 8 || !version_key.starts_with(&prefix) {
                continue;
            }
            let version = txn::decode_txn_id(&version_key[prefix.len()..])?;
            if version <= self.snapshot_id {
                selected = Some((version, txn::decode_mvcc_record(&value)?.value));
            }
        }
        Ok(selected.map_or(SnapshotLookup::Missing, |(_, value)| {
            SnapshotLookup::Found(value)
        }))
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
            return Ok(self.columnar_rows(&table)?.into_iter().find(|row| {
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
            let primary_key = table.primary_key_value_for_row(&row)?;
            let mut rows = self.columnar_rows(&table)?;
            if rows.iter().any(|existing| {
                table
                    .primary_key_value_for_row(existing)
                    .is_ok_and(|key| key == primary_key)
            }) {
                return Err(QueryError::DuplicatePrimaryKey.into());
            }

            rows.push(row);
            self.queue_ops(table.columnar_replace_ops(rows)?);
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
            let mut rows = self.columnar_rows(&table)?;
            if let Some(index) = rows.iter().position(|existing| {
                table
                    .primary_key_value_for_row(existing)
                    .is_ok_and(|key| key == primary_key)
            }) {
                rows[index] = row;
            } else {
                rows.push(row);
            }

            self.queue_ops(table.columnar_replace_ops(rows)?);
            return Ok(());
        }

        let row_key = table.row_key_for_row(&row)?;

        if let Some(old) = self.get_row_by_key(&row_key)? {
            self.queue_ops(table.index_delete_ops_for_key(&old, &row_key)?);
        }

        self.queue_ops(table.put_ops_for_key(row, &row_key)?);
        Ok(())
    }

    /// Queues a relational delete.
    /// # Errors
    /// Fails when the table is missing or the key is invalid.
    pub fn delete_row(&mut self, table: &str, primary_key: &Value) -> Result<(), DbError> {
        let table = self.database.table(table)?;
        if table.layout() == TableLayout::Columnar {
            let mut rows = self.columnar_rows(&table)?;
            let before = rows.len();
            rows.retain(|row| {
                !table
                    .primary_key_value_for_row(row)
                    .is_ok_and(|key| key == *primary_key)
            });

            if rows.len() != before {
                self.queue_ops(table.columnar_replace_ops(rows)?);
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

    /// Commits the transaction.
    /// # Errors
    /// Fails with conflict when a written key changed after this transaction snapshot.
    pub fn commit(self) -> Result<(), DbError> {
        let database = self.database;
        let snapshot_id = self.snapshot_id;
        let isolation = self.options.isolation;
        let write_set = self.write_set;
        let initial_ops = ops_from_write_set(&write_set);
        let final_ops = database.apply_before_wasm_triggers(initial_ops)?;
        let after_triggers = database.after_wasm_trigger_invocations(&final_ops)?;
        let write_set = txn::ops_to_write_set(final_ops.clone());
        let write_keys = write_set.keys().cloned().collect::<BTreeSet<_>>();
        let read_keys = self.read_keys.into_inner();
        let read_ranges = self.read_ranges.into_inner();

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
                    &write_keys,
                )?;
            }
            return Ok(());
        }

        cdc::validate_write_set_hooks(database.replication_ref(), &write_set)?;
        let before = txn::current_txn_id(storage)?;
        if isolation == IsolationLevel::Serializable {
            database
                .repl
                .commit_write_set_with_preflight(snapshot_id, write_set, |write| {
                    validate_serializable_reads(
                        write,
                        snapshot_id,
                        &read_keys,
                        &read_ranges,
                        &write_keys,
                    )
                })?;
        } else {
            database.repl.commit_write_set(snapshot_id, write_set)?;
        }
        let after = txn::current_txn_id(storage)?;
        cdc::deliver_after_commit_hooks(database.replication_ref(), before, after)?;
        database.deliver_after_wasm_triggers(after_triggers)?;
        Ok(())
    }

    pub fn rollback(self) {}

    fn get_row_by_key(&self, row_key: &[u8]) -> Result<Option<Row>, DbError> {
        let Some(bytes) = self.read_raw(REL_ROWS_TABLE, row_key)? else {
            return Ok(None);
        };

        Ok(decode_row_bytes(&bytes)?)
    }

    fn columnar_rows(&self, table: &RelTable) -> Result<Vec<Row>, DbError> {
        let key = table.columnar_segment_key();
        let bytes = self.read_raw(REL_COLUMNAR_SEGMENTS_TABLE, &key)?;
        Ok(table.decode_columnar_rows(bytes.as_deref())?)
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

#[derive(Clone)]
struct WasmTriggerInvocation {
    spec: TriggerSpec,
    event: TriggerEvent,
    op: Op,
    old: Option<Bytes>,
}

impl Database {
    fn apply_before_wasm_triggers(&self, ops: Vec<Op>) -> Result<Vec<Op>, DbError> {
        let triggers = self.enabled_wasm_triggers(TriggerTiming::Before)?;
        if triggers.is_empty() || ops.is_empty() {
            return Ok(ops);
        }

        let mut transformed = Vec::with_capacity(ops.len());
        for op in ops {
            let mut current = op;
            let old = self.old_value_for_op(&current)?;
            let Some(event) = Self::event_for_op(&current, old.as_deref()) else {
                transformed.push(current);
                continue;
            };

            for spec in &triggers {
                if !self.trigger_matches_op(spec, &current, event) {
                    continue;
                }
                match self.run_wasm_trigger(
                    spec,
                    TriggerTiming::Before,
                    event,
                    &current,
                    old.as_deref(),
                )? {
                    TriggerOutcome::Accept => {}
                    TriggerOutcome::Reject(message) => {
                        return Err(ContinuousError::Invalid(format!(
                            "before trigger {} rejected write: {message}",
                            spec.name
                        ))
                        .into());
                    }
                    TriggerOutcome::Replace(value) => {
                        current = replace_trigger_op_value(current, &value)?;
                    }
                }
            }

            transformed.push(current);
        }

        Ok(transformed)
    }

    fn after_wasm_trigger_invocations(
        &self,
        ops: &[Op],
    ) -> Result<Vec<WasmTriggerInvocation>, DbError> {
        let triggers = self.enabled_wasm_triggers(TriggerTiming::After)?;
        if triggers.is_empty() || ops.is_empty() {
            return Ok(Vec::new());
        }

        let mut invocations = Vec::new();
        for op in ops {
            let old = self.old_value_for_op(op)?;
            let Some(event) = Self::event_for_op(op, old.as_deref()) else {
                continue;
            };
            invocations.extend(
                triggers
                    .iter()
                    .filter(|spec| self.trigger_matches_op(spec, op, event))
                    .cloned()
                    .map(|spec| WasmTriggerInvocation {
                        spec,
                        event,
                        op: op.clone(),
                        old: old.clone(),
                    }),
            );
        }

        Ok(invocations)
    }

    fn deliver_after_wasm_triggers(
        &self,
        invocations: Vec<WasmTriggerInvocation>,
    ) -> Result<(), DbError> {
        for invocation in invocations {
            let outcome = self.run_wasm_trigger(
                &invocation.spec,
                TriggerTiming::After,
                invocation.event,
                &invocation.op,
                invocation.old.as_deref(),
            )?;
            if !matches!(outcome, TriggerOutcome::Accept) {
                return Err(ContinuousError::Invalid(format!(
                    "after trigger {} must return accept",
                    invocation.spec.name
                ))
                .into());
            }
        }
        Ok(())
    }

    fn enabled_wasm_triggers(&self, timing: TriggerTiming) -> Result<Vec<TriggerSpec>, DbError> {
        Ok(continuous::read_triggers(self.replication_ref())?
            .into_iter()
            .filter(|spec| spec.enabled && spec.timing == timing)
            .collect())
    }

    fn old_value_for_op(&self, op: &Op) -> Result<Option<Bytes>, DbError> {
        let (table, key) = op_table_key(op);
        Ok(self.repl.read(table, key, ReadConsistency::Strong)?)
    }

    fn event_for_op(op: &Op, old: Option<&[u8]>) -> Option<TriggerEvent> {
        match op {
            Op::Put { .. } if old.is_some() => Some(TriggerEvent::Update),
            Op::Put { .. } => Some(TriggerEvent::Insert),
            Op::Delete { .. } if old.is_some() => Some(TriggerEvent::Delete),
            Op::Delete { .. } => None,
        }
    }

    fn trigger_matches_op(&self, spec: &TriggerSpec, op: &Op, event: TriggerEvent) -> bool {
        if spec.event != event {
            return false;
        }

        let (storage_table, key) = op_table_key(op);
        match self.catalog.get(&spec.table) {
            Some(CatalogEntry::Table { .. }) => {
                (storage_table == REL_ROWS_TABLE || storage_table == REL_COLUMNAR_SEGMENTS_TABLE)
                    && key_has_rel_table_prefix(key, &spec.table)
            }
            Some(CatalogEntry::Collection { collection_id, .. }) => {
                storage_table == DOCUMENT_TABLE
                    && key_in_bounds(key, &collection_range_bounds(*collection_id))
            }
            _ => false,
        }
    }

    fn run_wasm_trigger(
        &self,
        spec: &TriggerSpec,
        timing: TriggerTiming,
        event: TriggerEvent,
        op: &Op,
        old: Option<&[u8]>,
    ) -> Result<TriggerOutcome, DbError> {
        let (_, wasm) = extension::read_wasm_module(self.replication_ref(), &spec.module_hash)?;
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
    ReplError::Transport(format!("WASM trigger failed: {error}"))
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

fn replace_trigger_op_value(op: Op, value: &Value) -> Result<Op, DbError> {
    match op {
        Op::Put { table, key, .. } => {
            if table == REL_ROWS_TABLE && !matches!(value, Value::Array(_)) {
                return Err(ContinuousError::Invalid(
                    "relational trigger replacement must be a row array".to_owned(),
                )
                .into());
            }
            Ok(Op::Put {
                table,
                key,
                value: encode_value(value)?,
            })
        }
        Op::Delete { .. } => Err(ContinuousError::Invalid(
            "delete triggers cannot replace a deleted value".to_owned(),
        )
        .into()),
    }
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

impl Replication for Database {
    fn propose(&self, op: Op) -> Result<(), ReplError> {
        self.propose_batch(vec![op])
    }

    fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
        let ops = self
            .apply_before_wasm_triggers(ops)
            .map_err(|error| db_trigger_to_repl(&error))?;
        let after_triggers = self
            .after_wasm_trigger_invocations(&ops)
            .map_err(|error| db_trigger_to_repl(&error))?;
        cdc::validate_before_hooks(self.repl.replication_ref(), &ops).map_err(db_hook_to_repl)?;
        let before = current_lsn_from_repl(self.repl.replication_ref())?;
        let quota_ops = ops.clone();
        let _write_permit = self
            .tenant
            .as_ref()
            .map(|tenant| tenant.try_begin_write().map_err(repl_quota_error))
            .transpose()?;
        let reservation = self
            .tenant
            .as_ref()
            .map(|tenant| tenant.reserve_ops(&ops).map_err(repl_quota_error))
            .transpose()?;
        if let Err(error) = self.repl.propose_batch(ops) {
            if let (Some(tenant), Some(reservation)) = (&self.tenant, &reservation) {
                tenant.release(reservation);
            }
            return Err(error);
        }
        if let (Some(tenant), Some(reservation)) = (&self.tenant, &reservation) {
            tenant.commit_successful_ops(&quota_ops, reservation);
        }
        let after = current_lsn_from_repl(self.repl.replication_ref())?;
        cdc::deliver_after_commit_hooks(self.repl.replication_ref(), before, after)
            .map_err(db_hook_to_repl)?;
        self.deliver_after_wasm_triggers(after_triggers)
            .map_err(|error| db_trigger_to_repl(&error))?;
        Ok(())
    }

    fn propose_authorized_batch(
        &self,
        ops: Vec<Op>,
        authorization: txn::WriteAuthorization,
    ) -> Result<(), ReplError> {
        self.repl.propose_authorized_batch(ops, authorization)
    }

    fn propose_conditional_batch(&self, batch: ConditionalBatch) -> Result<(), ReplError> {
        let mut batch = batch;
        batch.ops = self
            .apply_before_wasm_triggers(batch.ops)
            .map_err(|error| db_trigger_to_repl(&error))?;
        let after_triggers = self
            .after_wasm_trigger_invocations(&batch.ops)
            .map_err(|error| db_trigger_to_repl(&error))?;
        cdc::validate_before_hooks(self.repl.replication_ref(), &batch.ops)
            .map_err(db_hook_to_repl)?;
        let before = current_lsn_from_repl(self.repl.replication_ref())?;
        let quota_ops = batch.ops.clone();
        let _write_permit = self
            .tenant
            .as_ref()
            .map(|tenant| tenant.try_begin_write().map_err(repl_quota_error))
            .transpose()?;
        let reservation = self
            .tenant
            .as_ref()
            .map(|tenant| tenant.reserve_ops(&batch.ops).map_err(repl_quota_error))
            .transpose()?;
        if let Err(error) = self.repl.propose_conditional_batch(batch) {
            if let (Some(tenant), Some(reservation)) = (&self.tenant, &reservation) {
                tenant.release(reservation);
            }
            return Err(error);
        }
        if let (Some(tenant), Some(reservation)) = (&self.tenant, &reservation) {
            tenant.commit_successful_ops(&quota_ops, reservation);
        }
        let after = current_lsn_from_repl(self.repl.replication_ref())?;
        cdc::deliver_after_commit_hooks(self.repl.replication_ref(), before, after)
            .map_err(db_hook_to_repl)?;
        self.deliver_after_wasm_triggers(after_triggers)
            .map_err(|error| db_trigger_to_repl(&error))?;
        Ok(())
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

fn db_hook_to_repl(error: HookError) -> ReplError {
    let (kind, message) = match error {
        HookError::Rejected(message) => ("rejected", format!("hook rejected write: {message}")),
        HookError::Timeout(message) => ("timeout", format!("hook timed out: {message}")),
        HookError::Serde(message) => ("serde", message),
        HookError::Repl(error) => return error,
    };
    observability::record_hook_failure(kind);
    ReplError::Transport(message)
}

/// Creates a database and initializes metadata when it is missing.
/// # Errors
/// Fails when the config is invalid, storage cannot open, or existing metadata conflicts.
pub fn create_database(config: DbConfig) -> Result<Database, ConfigError> {
    let engine = engine_for(&config)?;

    match read_metadata(&engine) {
        Ok(metadata) => ensure_metadata_matches(&config, metadata)?,
        Err(ConfigError::MissingMetadata { key }) if key == KEY_SCHEMA_VERSION_NAME => {
            write_metadata(&engine, &config)?;
        }
        Err(error) => return Err(error),
    }

    let catalog = read_catalog(&engine)?;
    Database::new(config, engine, catalog)
}

/// Opens an existing database and validates its metadata.
/// # Errors
/// Fails when the config is invalid, storage cannot open, metadata is missing, or metadata conflicts.
pub fn open_database(config: DbConfig) -> Result<Database, ConfigError> {
    let engine = engine_for(&config)?;
    let metadata = read_metadata(&engine)?;
    ensure_metadata_matches(&config, metadata)?;
    let catalog = read_catalog(&engine)?;
    Database::new(config, engine, catalog)
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

    match read_metadata(&engine) {
        Ok(metadata) => ensure_metadata_matches(&config, metadata)?,
        Err(ConfigError::MissingMetadata { key }) if key == KEY_SCHEMA_VERSION_NAME => {
            write_metadata(&engine, &config)?;
        }
        Err(error) => return Err(error),
    }

    let catalog = read_catalog(&engine)?;
    let performance = effective_performance(&config, &ops);
    let mut database = Database::new_with_performance(config, engine, catalog, performance)?;
    database.encryption.clone_from(&ops.encryption);
    write_security_to_repl(database.replication_ref(), &ops.security)?;
    database.apply_tenant_config(ops.tenant.clone());
    database.apply_security_config(ops.security);
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
    let metadata = read_metadata(&engine)?;
    ensure_metadata_matches(&config, metadata)?;
    let catalog = read_catalog(&engine)?;
    let performance = effective_performance(&config, ops);
    let mut database = Database::new_with_performance(config, engine, catalog, performance)?;
    database.encryption.clone_from(&ops.encryption);
    let security = read_security_from_repl(database.replication_ref())?.unwrap_or_default();
    database.apply_tenant_config(ops.tenant.clone());
    database.apply_security_config(security);
    Ok(database)
}

/// Creates a database backed by a CP/Raft replication backend.
/// # Errors
/// Fails when config, cluster topology, storage, or metadata validation fails.
pub fn create_cluster_database(
    config: DbConfig,
    cluster: CpClusterConfig,
) -> Result<Database, ConfigError> {
    ensure_cluster_supported(&config, &cluster)?;
    let engine = engine_for(&config)?;

    match read_metadata(&engine) {
        Ok(metadata) => ensure_metadata_matches(&config, metadata)?,
        Err(ConfigError::MissingMetadata { key }) if key == KEY_SCHEMA_VERSION_NAME => {
            write_metadata(&engine, &config)?;
        }
        Err(error) => return Err(error),
    }

    let catalog = read_catalog(&engine)?;
    Database::new_cp(config, engine, catalog, cluster)
}

/// Opens a database backed by a CP/Raft replication backend.
/// # Errors
/// Fails when config, cluster topology, storage, or metadata validation fails.
pub fn open_cluster_database(
    config: DbConfig,
    cluster: CpClusterConfig,
) -> Result<Database, ConfigError> {
    ensure_cluster_supported(&config, &cluster)?;
    let engine = engine_for(&config)?;
    let metadata = read_metadata(&engine)?;
    ensure_metadata_matches(&config, metadata)?;
    let catalog = read_catalog(&engine)?;
    Database::new_cp(config, engine, catalog, cluster)
}

/// Creates a CP/Raft database with operational settings.
/// # Errors
/// Fails when config, cluster topology, encryption, storage, or metadata validation fails.
pub fn create_cluster_database_with_ops(
    config: DbConfig,
    cluster: CpClusterConfig,
    ops: OperationalConfig,
) -> Result<Database, ConfigError> {
    ensure_cluster_supported(&config, &cluster)?;
    validate_operational(&config, &ops)?;
    let engine = engine_for_operational(&config, &ops)?;

    match read_metadata(&engine) {
        Ok(metadata) => ensure_metadata_matches(&config, metadata)?,
        Err(ConfigError::MissingMetadata { key }) if key == KEY_SCHEMA_VERSION_NAME => {
            write_metadata(&engine, &config)?;
        }
        Err(error) => return Err(error),
    }

    let catalog = read_catalog(&engine)?;
    let mut database = Database::new_cp(config, engine, catalog, cluster)?;
    database.encryption.clone_from(&ops.encryption);
    write_security_to_repl(database.replication_ref(), &ops.security)?;
    database.apply_tenant_config(ops.tenant.clone());
    database.apply_security_config(ops.security);
    Ok(database)
}

/// Opens a CP/Raft database with operational settings.
/// # Errors
/// Fails when config, cluster topology, encryption, storage, or metadata validation fails.
pub fn open_cluster_database_with_ops(
    config: DbConfig,
    cluster: CpClusterConfig,
    ops: &OperationalConfig,
) -> Result<Database, ConfigError> {
    ensure_cluster_supported(&config, &cluster)?;
    validate_operational(&config, ops)?;
    let engine = engine_for_operational(&config, ops)?;
    let metadata = read_metadata(&engine)?;
    ensure_metadata_matches(&config, metadata)?;
    let catalog = read_catalog(&engine)?;
    let mut database = Database::new_cp(config, engine, catalog, cluster)?;
    database.encryption.clone_from(&ops.encryption);
    let security = read_security_from_repl(database.replication_ref())?.unwrap_or_default();
    database.apply_tenant_config(ops.tenant.clone());
    database.apply_security_config(security);
    Ok(database)
}

/// Creates a database backed by an AP/Dynamo-style replication backend.
/// # Errors
/// Fails when config, cluster topology, storage, or metadata validation fails.
pub fn create_ap_database(
    config: DbConfig,
    cluster: ApClusterConfig,
) -> Result<Database, ConfigError> {
    ensure_ap_supported(&config, &cluster)?;
    let engine = engine_for(&config)?;

    match read_metadata(&engine) {
        Ok(metadata) => ensure_metadata_matches(&config, metadata)?,
        Err(ConfigError::MissingMetadata { key }) if key == KEY_SCHEMA_VERSION_NAME => {
            write_metadata(&engine, &config)?;
        }
        Err(error) => return Err(error),
    }

    let catalog = read_catalog(&engine)?;
    Database::new_ap(config, engine, catalog, cluster)
}

/// Opens a database backed by an AP/Dynamo-style replication backend.
/// # Errors
/// Fails when config, cluster topology, storage, or metadata validation fails.
pub fn open_ap_database(
    config: DbConfig,
    cluster: ApClusterConfig,
) -> Result<Database, ConfigError> {
    ensure_ap_supported(&config, &cluster)?;
    let engine = engine_for(&config)?;
    let metadata = read_metadata(&engine)?;
    ensure_metadata_matches(&config, metadata)?;
    let catalog = read_catalog(&engine)?;
    Database::new_ap(config, engine, catalog, cluster)
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
        Ok(metadata) => ensure_metadata_matches(&config, metadata)?,
        Err(ConfigError::MissingMetadata { key }) if key == KEY_SCHEMA_VERSION_NAME => {
            write_metadata(&engine, &config)?;
        }
        Err(error) => return Err(error),
    }

    let catalog = read_catalog(&engine)?;
    let mut database = Database::new_ap(config, engine, catalog, cluster)?;
    database.encryption.clone_from(&ops.encryption);
    write_security_to_repl(database.replication_ref(), &ops.security)?;
    database.apply_tenant_config(ops.tenant.clone());
    database.apply_security_config(ops.security);
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
    let metadata = read_metadata(&engine)?;
    ensure_metadata_matches(&config, metadata)?;
    let catalog = read_catalog(&engine)?;
    let mut database = Database::new_ap(config, engine, catalog, cluster)?;
    database.encryption.clone_from(&ops.encryption);
    let security = read_security_from_repl(database.replication_ref())?.unwrap_or_default();
    database.apply_tenant_config(ops.tenant.clone());
    database.apply_security_config(security);
    Ok(database)
}

/// Creates a sharded database backed by multiple replication groups.
/// # Errors
/// Fails when config, shard topology, metadata, or catalog initialization fails.
pub fn create_sharded_database(
    config: DbConfig,
    sharding: ShardedDatabaseConfig,
) -> Result<Database, ConfigError> {
    validate(&config)?;
    let repl = Arc::new(build_sharded_replication(sharding, true)?);

    match read_metadata_from_repl(repl.as_ref()) {
        Ok(metadata) => ensure_metadata_matches(&config, metadata)?,
        Err(ConfigError::MissingMetadata { key }) if key == KEY_SCHEMA_VERSION_NAME => {
            write_metadata_to_repl(repl.as_ref(), &config)?;
        }
        Err(error) => return Err(error),
    }

    let catalog = read_catalog_from_repl(repl.as_ref())?;
    Database::new_sharded(config, repl, catalog)
}

/// Opens a sharded database backed by multiple replication groups.
/// # Errors
/// Fails when config, shard topology, metadata, or catalog validation fails.
pub fn open_sharded_database(
    config: DbConfig,
    sharding: ShardedDatabaseConfig,
) -> Result<Database, ConfigError> {
    validate(&config)?;
    let repl = Arc::new(build_sharded_replication(sharding, false)?);
    let metadata = read_metadata_from_repl(repl.as_ref())?;
    ensure_metadata_matches(&config, metadata)?;
    let catalog = read_catalog_from_repl(repl.as_ref())?;
    Database::new_sharded(config, repl, catalog)
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
    write_security_to_repl(database.replication_ref(), &ops.security)?;
    database.apply_tenant_config(ops.tenant.clone());
    database.apply_security_config(ops.security);
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
    let security = read_security_from_repl(database.replication_ref())?.unwrap_or_default();
    database.apply_tenant_config(ops.tenant.clone());
    database.apply_security_config(security);
    Ok(database)
}

/// Maps a profile to the concrete storage engine used in this phase.
/// # Errors
/// Fails when the config is invalid or the selected storage cannot open.
pub fn engine_for(config: &DbConfig) -> Result<AnyEngine, ConfigError> {
    validate(config)?;

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

fn effective_performance(config: &DbConfig, ops: &OperationalConfig) -> PerformanceConfig {
    ops.performance
        .clone()
        .unwrap_or_else(|| PerformanceConfig::for_profile(config.profile))
}

fn query_permits_for(performance: &PerformanceConfig) -> Arc<Semaphore> {
    Arc::new(Semaphore::new(
        performance.query.max_concurrent_queries.max(1),
    ))
}

#[cfg(test)]
fn force_wasm_runtime_init_failure(enabled: bool) {
    runtime::force_wasm_runtime_init_failure(enabled);
}

/// Validates profile and storage-path compatibility.
/// # Errors
/// Fails when the selected profile contradicts the provided path.
pub fn validate(config: &DbConfig) -> Result<(), ConfigError> {
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

fn sql_requirements(
    sql: &str,
    catalog: &BTreeMap<String, CatalogEntry>,
) -> Result<Vec<(Resource, Permission)>, DbError> {
    if compat::compat_sql_requirements(sql)? {
        return Ok(vec![(Resource::Database, Permission::Read)]);
    }

    if tuning::parse_system_view(sql)?.is_some() {
        return Ok(vec![(Resource::System, Permission::Read)]);
    }

    if let Some((target, _)) = crate::query::parse_analyze_for_authz(sql)? {
        return Ok(match target {
            AnalyzeTarget::All => vec![(Resource::System, Permission::Admin)],
            AnalyzeTarget::Named(name) => {
                vec![(resource_for_catalog_name(catalog, &name), Permission::Admin)]
            }
        });
    }

    if let Some(requirements) = phase19_requirements(sql)? {
        return Ok(requirements);
    }

    if let Some(requirements) = extension::extension_sql_requirements(sql)? {
        return Ok(requirements);
    }

    if parse_call_procedure_sql(sql)?.is_some() {
        return Ok(vec![(Resource::Database, Permission::Write)]);
    }

    let dialect = PostgreSqlDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|error| QueryError::Parse(error.to_string()))?;
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
                    requirements.extend(
                        names.into_iter().map(|name| {
                            (resource_for_catalog_name(catalog, &name), Permission::Read)
                        }),
                    );
                }
            }
            Statement::Insert(insert) => {
                let table_name = match &insert.table {
                    SqlTableObject::TableName(name) => object_name_to_string(name)?,
                    other => return Err(QueryError::Unsupported(other.to_string()).into()),
                };
                requirements.push((
                    resource_for_catalog_name(catalog, &table_name),
                    Permission::Write,
                ));
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
                requirements.extend(sql_requirements(&statement.to_string(), catalog)?);
            }
            other => return Err(QueryError::Unsupported(other.to_string()).into()),
        }
    }

    requirements.sort();
    requirements.dedup();
    Ok(requirements)
}

fn phase19_requirements(sql: &str) -> Result<Option<Vec<(Resource, Permission)>>, QueryError> {
    let Some(call) = parse_phase19_call(sql)? else {
        return Ok(None);
    };
    let requirements = match call.name.as_str() {
        "match" => vec![(
            Resource::FullTextIndex(phase19_resource_name(&call, "match")?),
            Permission::Read,
        )],
        "within_radius" => vec![(
            Resource::GeoIndex(phase19_resource_name(&call, "within_radius")?),
            Permission::Read,
        )],
        "graph_neighbors" => vec![(
            Resource::Graph(phase19_resource_name(&call, "graph_neighbors")?),
            Permission::Read,
        )],
        "time_bucket" => vec![(Resource::Database, Permission::Read)],
        _ => Vec::new(),
    };
    Ok(Some(requirements))
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

fn collect_query_tables_scoped(
    query: &SqlQuery,
    names: &mut BTreeSet<String>,
    cte_names: &BTreeSet<String>,
) -> Result<(), QueryError> {
    let mut scoped_ctes = cte_names.clone();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_query_tables_scoped(&cte.query, names, &scoped_ctes)?;
            scoped_ctes.insert(cte.alias.name.value.to_ascii_lowercase());
        }
    }

    collect_set_expr_tables(query.body.as_ref(), names, &scoped_ctes)
}

fn collect_set_expr_tables(
    expr: &SetExpr,
    names: &mut BTreeSet<String>,
    cte_names: &BTreeSet<String>,
) -> Result<(), QueryError> {
    match expr {
        SetExpr::Select(select) => {
            for table in &select.from {
                collect_table_factor(&table.relation, names, cte_names)?;
                for join in &table.joins {
                    collect_table_factor(&join.relation, names, cte_names)?;
                    collect_join_operator_tables(&join.join_operator, names, cte_names)?;
                }
            }
            for item in &select.projection {
                collect_select_item_tables(item, names, cte_names)?;
            }
            if let Some(selection) = &select.selection {
                collect_expr_tables(selection, names, cte_names)?;
            }
            if let GroupByExpr::Expressions(expressions, _) = &select.group_by {
                for expression in expressions {
                    collect_expr_tables(expression, names, cte_names)?;
                }
            }
            if let Some(having) = &select.having {
                collect_expr_tables(having, names, cte_names)?;
            }
        }
        SetExpr::Query(query) => collect_query_tables_scoped(query, names, cte_names)?,
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr_tables(left, names, cte_names)?;
            collect_set_expr_tables(right, names, cte_names)?;
        }
        SetExpr::Values(_) => {}
        other => return Err(QueryError::Unsupported(other.to_string())),
    }
    Ok(())
}

fn collect_table_factor(
    factor: &SqlTableFactor,
    names: &mut BTreeSet<String>,
    cte_names: &BTreeSet<String>,
) -> Result<(), QueryError> {
    match factor {
        SqlTableFactor::Table { name, .. } => {
            let name = object_name_to_string(name)?;
            if !cte_names.contains(&name.to_ascii_lowercase()) {
                names.insert(name);
            }
            Ok(())
        }
        SqlTableFactor::Derived { subquery, .. } => {
            collect_query_tables_scoped(subquery, names, cte_names)
        }
        other => Err(QueryError::Unsupported(other.to_string())),
    }
}

fn collect_select_item_tables(
    item: &SelectItem,
    names: &mut BTreeSet<String>,
    cte_names: &BTreeSet<String>,
) -> Result<(), QueryError> {
    match item {
        SelectItem::UnnamedExpr(expr)
        | SelectItem::ExprWithAlias { expr, .. }
        | SelectItem::ExprWithAliases { expr, .. } => collect_expr_tables(expr, names, cte_names),
        SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => Ok(()),
    }
}

fn collect_join_operator_tables(
    operator: &JoinOperator,
    names: &mut BTreeSet<String>,
    cte_names: &BTreeSet<String>,
) -> Result<(), QueryError> {
    match operator {
        JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::Right(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::CrossJoin(constraint)
        | JoinOperator::Semi(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::Anti(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint)
        | JoinOperator::StraightJoin(constraint) => {
            collect_join_constraint_tables(constraint, names, cte_names)
        }
        JoinOperator::AsOf {
            match_condition,
            constraint,
        } => {
            collect_expr_tables(match_condition, names, cte_names)?;
            collect_join_constraint_tables(constraint, names, cte_names)
        }
        JoinOperator::CrossApply
        | JoinOperator::OuterApply
        | JoinOperator::ArrayJoin
        | JoinOperator::LeftArrayJoin
        | JoinOperator::InnerArrayJoin => Ok(()),
    }
}

fn collect_join_constraint_tables(
    constraint: &JoinConstraint,
    names: &mut BTreeSet<String>,
    cte_names: &BTreeSet<String>,
) -> Result<(), QueryError> {
    match constraint {
        JoinConstraint::On(expr) => collect_expr_tables(expr, names, cte_names),
        JoinConstraint::Using(_) | JoinConstraint::Natural | JoinConstraint::None => Ok(()),
    }
}

fn collect_expr_tables(
    expr: &SqlExpr,
    names: &mut BTreeSet<String>,
    cte_names: &BTreeSet<String>,
) -> Result<(), QueryError> {
    match expr {
        SqlExpr::IsFalse(expr)
        | SqlExpr::IsNotFalse(expr)
        | SqlExpr::IsTrue(expr)
        | SqlExpr::IsNotTrue(expr)
        | SqlExpr::IsNull(expr)
        | SqlExpr::IsNotNull(expr)
        | SqlExpr::IsUnknown(expr)
        | SqlExpr::IsNotUnknown(expr)
        | SqlExpr::Nested(expr)
        | SqlExpr::UnaryOp { expr, .. }
        | SqlExpr::Cast { expr, .. }
        | SqlExpr::AtTimeZone {
            timestamp: expr, ..
        }
        | SqlExpr::Extract { expr, .. }
        | SqlExpr::Ceil { expr, .. }
        | SqlExpr::Floor { expr, .. } => collect_expr_tables(expr, names, cte_names),
        SqlExpr::IsDistinctFrom(left, right)
        | SqlExpr::IsNotDistinctFrom(left, right)
        | SqlExpr::BinaryOp { left, right, .. }
        | SqlExpr::AnyOp { left, right, .. }
        | SqlExpr::AllOp { left, right, .. } => {
            collect_expr_tables(left, names, cte_names)?;
            collect_expr_tables(right, names, cte_names)
        }
        SqlExpr::InList { expr, list, .. } => {
            collect_expr_tables(expr, names, cte_names)?;
            for item in list {
                collect_expr_tables(item, names, cte_names)?;
            }
            Ok(())
        }
        SqlExpr::InSubquery { expr, subquery, .. } => {
            collect_expr_tables(expr, names, cte_names)?;
            collect_query_tables_scoped(subquery, names, cte_names)
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            collect_expr_tables(expr, names, cte_names)?;
            collect_expr_tables(low, names, cte_names)?;
            collect_expr_tables(high, names, cte_names)
        }
        SqlExpr::Exists { subquery, .. } | SqlExpr::Subquery(subquery) => {
            collect_query_tables_scoped(subquery, names, cte_names)
        }
        SqlExpr::Function(function) => {
            collect_function_arguments_tables(&function.args, names, cte_names)
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                collect_expr_tables(operand, names, cte_names)?;
            }
            for condition in conditions {
                collect_expr_tables(&condition.condition, names, cte_names)?;
                collect_expr_tables(&condition.result, names, cte_names)?;
            }
            if let Some(else_result) = else_result {
                collect_expr_tables(else_result, names, cte_names)?;
            }
            Ok(())
        }
        SqlExpr::Tuple(expressions) => {
            for expr in expressions {
                collect_expr_tables(expr, names, cte_names)?;
            }
            Ok(())
        }
        SqlExpr::GroupingSets(groups) | SqlExpr::Cube(groups) | SqlExpr::Rollup(groups) => {
            for expr in groups.iter().flatten() {
                collect_expr_tables(expr, names, cte_names)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn collect_function_arguments_tables(
    args: &FunctionArguments,
    names: &mut BTreeSet<String>,
    cte_names: &BTreeSet<String>,
) -> Result<(), QueryError> {
    match args {
        FunctionArguments::None => Ok(()),
        FunctionArguments::Subquery(query) => collect_query_tables_scoped(query, names, cte_names),
        FunctionArguments::List(list) => {
            for arg in &list.args {
                match arg {
                    FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg) => {
                        if let FunctionArgExpr::Expr(expr) = arg {
                            collect_expr_tables(expr, names, cte_names)?;
                        }
                    }
                    FunctionArg::ExprNamed { name, arg, .. } => {
                        collect_expr_tables(name, names, cte_names)?;
                        if let FunctionArgExpr::Expr(expr) = arg {
                            collect_expr_tables(expr, names, cte_names)?;
                        }
                    }
                }
            }
            Ok(())
        }
    }
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
        Some(CatalogEntry::Collection { .. }) => Resource::Collection(name.to_owned()),
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
            let database = if create {
                create_database(config.clone())?
            } else {
                open_database(config.clone())?
            };
            Ok(Arc::new(database))
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
        Profile::Analytical => TableLayout::Columnar,
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
        Profile::Analytical => CostProfile::Analytical,
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

fn read_metadata<S: StorageEngine>(engine: &S) -> Result<Metadata, ConfigError> {
    let txn = engine.begin_read()?;
    let schema_version = read_json(&txn, KEY_SCHEMA_VERSION, KEY_SCHEMA_VERSION_NAME)?;

    if schema_version != SCHEMA_VERSION {
        return Err(ConfigError::SchemaVersionMismatch {
            expected: SCHEMA_VERSION,
            found: schema_version,
        });
    }

    let profile = read_json(&txn, KEY_PROFILE, KEY_PROFILE_NAME)?;
    let replication = read_json(&txn, KEY_REPLICATION, KEY_REPLICATION_NAME)?;
    let layout = match txn.get(META_TABLE, KEY_LAYOUT)? {
        Some(bytes) => {
            serde_json::from_slice(&bytes).map_err(|error| ConfigError::Serde(error.to_string()))?
        }
        None => layout_for(profile),
    };

    Ok(Metadata {
        schema_version,
        profile,
        replication,
        layout,
    })
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

fn read_metadata_from_repl(repl: &dyn Replication) -> Result<Metadata, ConfigError> {
    let schema_version = read_json_from_repl(repl, KEY_SCHEMA_VERSION, KEY_SCHEMA_VERSION_NAME)?;

    if schema_version != SCHEMA_VERSION {
        return Err(ConfigError::SchemaVersionMismatch {
            expected: SCHEMA_VERSION,
            found: schema_version,
        });
    }

    let profile = read_json_from_repl(repl, KEY_PROFILE, KEY_PROFILE_NAME)?;
    let replication = read_json_from_repl(repl, KEY_REPLICATION, KEY_REPLICATION_NAME)?;
    let layout = match repl.read(META_TABLE, KEY_LAYOUT, ReadConsistency::Strong)? {
        Some(bytes) => {
            serde_json::from_slice(&bytes).map_err(|error| ConfigError::Serde(error.to_string()))?
        }
        None => layout_for(profile),
    };

    Ok(Metadata {
        schema_version,
        profile,
        replication,
        layout,
    })
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
    for (index, chunk) in text.as_bytes().chunks_exact(2).enumerate() {
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

fn read_json_from_repl<T: DeserializeOwned>(
    repl: &dyn Replication,
    key: &[u8],
    key_name: &'static str,
) -> Result<T, ConfigError> {
    let bytes = repl
        .read(META_TABLE, key, ReadConsistency::Strong)?
        .ok_or(ConfigError::MissingMetadata { key: key_name })?;

    serde_json::from_slice(&bytes).map_err(|error| ConfigError::Serde(error.to_string()))
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
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(DbError::InvalidCatalogName("empty name".to_owned()));
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(DbError::InvalidCatalogName(name.to_owned()));
    }
    if name.len() > 128 || !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()) {
        return Err(DbError::InvalidCatalogName(name.to_owned()));
    }
    Ok(())
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

fn read_json<T: DeserializeOwned>(
    txn: &impl ReadTransaction,
    key: &[u8],
    key_name: &'static str,
) -> Result<T, ConfigError> {
    let bytes = txn
        .get(META_TABLE, key)?
        .ok_or(ConfigError::MissingMetadata { key: key_name })?;

    serde_json::from_slice(&bytes).map_err(|error| ConfigError::Serde(error.to_string()))
}

#[cfg(test)]
mod tests;
