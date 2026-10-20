use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::Path,
    sync::Arc,
    thread,
};

use serde_json::json;

use super::{
    AUTHZ_TABLE, ConfigError, Database, DbConfig, DbError, EncryptionConfig, KEY_AUDIT_ENABLED,
    KEY_LAYOUT, KEY_PROFILE_MANIFEST, KEY_SCHEMA_VERSION, META_TABLE, Metadata, MetadataProbe,
    MetadataRead, OperationalConfig, PersistedProfileManifest, ProbedMetadata, Profile,
    ReplicationKind, RuntimeProfileBinding, SecurityConfig, ShardBackendConfig, ShardSpec,
    ShardedDatabaseConfig, create_ap_database, create_cluster_database,
    create_cluster_database_with_ops, create_database, create_database_with_ops,
    create_database_with_profile, create_sharded_database, create_simulated_cluster_database,
    engine_for, engine_for_operational, force_wasm_runtime_init_failure, inspect_database_metadata,
    layout_for, open_ap_database, open_cluster_database, open_cluster_database_with_ops,
    open_database, open_database_with_ops, open_sharded_database, open_simulated_cluster_database,
    profile_validation_report, recover_database_file, refuse_profile_manifest, validate,
};
use crate::cdc::{
    AggregateKind, AggregateSpec, ChangeOp, ChangefeedFilter, ChangefeedOptions, ChangefeedTarget,
    FAIL_AFTER_HOOK_FOR_TESTS, HOOK_DELIVERIES_TABLE, HookAction, HookError, HookSpec, HookTarget,
    MaterializedViewError, MaterializedViewSpec, PostCommitStage, ResumeToken, SubscriptionConfig,
};
use crate::cloud::{ObjectStoreUri, TenantConfig, TenantQuota};
use crate::config_spec::{
    ApplyStatus, ConsistencyMode, DatabaseSpec, DeploymentMode, MigrationPlanner, PolicyCompiler,
    ProfileKnobOverrides, ResolvedProfileKind, resolve_named_profile,
    spec_io::{SpecFormat, decode_spec_bytes},
};
use crate::continuous::{
    ContinuousQuerySpec, OutboxConnectorSpec, OutboxSink, ProcedureSpec, TriggerEvent, TriggerSpec,
    TriggerTiming,
};
use crate::extension::{
    AbiVersion, ExtensionError, LimitPolicy, MaskingPolicy, PolicyConfig, UdfBudget,
};
use crate::federation::{ForeignSource, ForeignTableOptions};
use crate::geo::{GeoError, GeoIndexConfig, GeoPoint};
use crate::graph::{GraphId, GraphNodeId, TraversalOptions};
use crate::model::{
    CollectionId, DocumentId, FieldPath, IndexId, IndexSpec, PlanKind, Predicate, Value,
    decode_value,
};
use crate::performance::{
    BenchmarkReport, CompressionAlgorithm, PerformanceConfig, RegressionGate,
};
use crate::phase30::{InternalTransportConfig, InternalTransportSecurity};
use crate::provision::{
    ProvisionActor, ProvisionHistoryEntry, ProvisionRecord, ProvisionRecordStatus, RECORD_VERSION,
};
use crate::query::{
    ColumnDef, ColumnType, DocField, ExplainOptions, QueryError, QueryFingerprint,
    REL_COLUMNAR_SEGMENT_META_TABLE, REL_COLUMNAR_SEGMENTS_TABLE, REL_INDEX_TABLE, REL_ROWS_TABLE,
    RelIndexSpec, RelPlanKind, Row, SqlOutput, SqlRows, TableLayout, TableSchema,
};
use crate::repl::{
    ApClusterConfig, ConditionalBatch, CpClusterConfig, HealingBackend, HealingPolicy,
    HealthConfig, ManualHealthProbe, Op, PartitionStrategy, RaftNode, ReadConsistency, ReplError,
    Replication, WriteCondition, propose_system,
};
use crate::security::{
    AUDIT_HEAD_TABLE, AUDIT_TABLE, AuditOutcome, AuthzPolicy, Permission, Principal,
    PrincipalRegistry, Resource, Role,
};
use crate::storage::{
    Bytes, EngineKind, ReadTransaction, RedbEngine, StorageEngine, StorageError, WriteTransaction,
    decode_stored_value,
};
use crate::temporal::{TemporalError, TemporalPoint, TemporalRetention};
use crate::text::{FullTextIndexConfig, TextError};
use crate::timeseries::{TimePoint, TimeSeriesConfig, TimeSeriesError};
use crate::tuning::{
    ParameterEnvelope, ReprofilePlan, ReprofileStatus, TunableParameter, TuningDecision,
    TuningPolicy, WORKLOAD_TABLE, WorkloadEntry, WorkloadSample, WorkloadWindow,
};
use crate::txn;
use crate::vector::{HnswParams, VectorCollectionConfig, VectorMetric};

#[test]
fn defaults_match_roadmap() {
    assert_eq!(Profile::default(), Profile::Balanced);
    assert_eq!(ReplicationKind::default(), ReplicationKind::Cp);
}

#[test]
fn database_creation_reports_wasm_runtime_initialization_failure() {
    struct RuntimeFailureGuard;

    impl Drop for RuntimeFailureGuard {
        fn drop(&mut self) {
            force_wasm_runtime_init_failure(false);
        }
    }

    force_wasm_runtime_init_failure(true);
    let _guard = RuntimeFailureGuard;

    match create_database(DbConfig::new(Profile::InMemory)) {
        Err(ConfigError::RuntimeInitialization(message)) => {
            assert!(message.contains("forced wasm runtime init failure"));
        }
        Err(error) => panic!("expected runtime initialization error, got {error}"),
        Ok(_) => panic!("expected runtime initialization error"),
    }
}

fn trigger_response(action: &str, value: Option<Value>) -> Value {
    let mut fields = BTreeMap::from([("action".to_owned(), Value::Str(action.to_owned()))]);
    if let Some(value) = value {
        fields.insert("value".to_owned(), value);
    }
    Value::Object(fields)
}

fn wasm_trigger(
    name: &str,
    table: &str,
    timing: TriggerTiming,
    event: TriggerEvent,
) -> TriggerSpec {
    TriggerSpec {
        name: name.to_owned(),
        timing,
        event,
        table: table.to_owned(),
        module_hash: String::new(),
        entry: "udf_call".to_owned(),
        budget: UdfBudget::default(),
        abi: AbiVersion::V1,
        enabled: true,
    }
}

#[test]
fn validation_rejects_conflicting_path_settings() {
    assert!(validate(&DbConfig::new(Profile::InMemory)).is_ok());
    assert!(validate(&DbConfig::on_disk(Profile::Transactional, "db.redb")).is_ok());

    assert!(matches!(
        validate(&DbConfig::new(Profile::InMemory).with_path("db.redb")),
        Err(ConfigError::VolatileWithPath {
            profile: Profile::InMemory
        })
    ));

    assert!(matches!(
        validate(&DbConfig::new(Profile::Balanced)),
        Err(ConfigError::MissingPath {
            profile: Profile::Balanced
        })
    ));
}

#[test]
fn profile_validation_report_describes_real_profile_stack() -> Result<(), Box<dyn std::error::Error>>
{
    let analytical = profile_validation_report(&DbConfig::on_disk(Profile::Analytical, "a.redb"))?;
    assert_eq!(analytical.layout, TableLayout::Columnar);
    assert_eq!(analytical.engine_kind, EngineKind::Redb);
    assert!(analytical.capabilities.contains("columnar-layout"));

    let vector = profile_validation_report(&DbConfig::on_disk(Profile::Vector, "v.redb"))?;
    assert!(vector.capabilities.contains("hnsw"));

    assert!(matches!(
        profile_validation_report(&DbConfig::new(Profile::HighDurability)),
        Err(ConfigError::MissingPath {
            profile: Profile::HighDurability
        })
    ));
    Ok(())
}

#[tokio::test]
async fn rbac_default_denies_and_audits_query_attempts() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_orders(&mut database)?;
    let principal = Principal::new("alice");

    assert!(matches!(
        database.query_as(&principal, "SELECT * FROM users").await,
        Err(DbError::AuthzDenied(_))
    ));

    let audit = database.audit_events()?;
    assert!(audit.iter().any(|event| {
        event.principal.as_deref() == Some("alice")
            && event.outcome == AuditOutcome::Denied
            && event.action == "query"
    }));
    assert!(
        audit
            .iter()
            .all(|event| event.detail.as_deref() != Some("Ada"))
    );
    Ok(())
}

#[tokio::test]
async fn rbac_read_role_cannot_insert() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_orders(&mut database)?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("reader").grant(Resource::Table("users".to_owned()), Permission::Read)
    ]));
    let principal = Principal::new("alice").with_role("reader");

    assert!(matches!(
        database
            .query_as(&principal, "SELECT name FROM users")
            .await?,
        SqlOutput::Rows(_)
    ));
    assert!(matches!(
        database
            .query_as(
                &principal,
                "INSERT INTO users (id, name, age) VALUES (3, 'Lin', 41)"
            )
            .await,
        Err(DbError::AuthzDenied(_))
    ));
    Ok(())
}

#[tokio::test]
async fn rbac_sees_cte_and_union_table_references() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_orders(&mut database)?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("reader").grant(Resource::Table("users".to_owned()), Permission::Read)
    ]));
    let principal = Principal::new("alice").with_role("reader");

    assert!(matches!(
        database
            .query_as(
                &principal,
                "WITH hidden AS (SELECT id FROM orders) \
                 SELECT id FROM users UNION SELECT id FROM hidden"
            )
            .await,
        Err(DbError::AuthzDenied(_))
    ));
    Ok(())
}

#[tokio::test]
async fn rbac_enforces_analyze_and_explain_permissions() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_orders(&mut database)?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("reader").grant(Resource::Table("users".to_owned()), Permission::Read),
        Role::new("table_admin").grant(Resource::Table("users".to_owned()), Permission::Admin),
    ]));
    let reader = Principal::new("alice").with_role("reader");
    let admin = Principal::new("root").with_role("table_admin");

    assert!(matches!(
        database
            .query_as(&reader, "EXPLAIN SELECT name FROM users WHERE age = 37")
            .await?,
        SqlOutput::Rows(_)
    ));
    assert!(matches!(
        database.query_as(&reader, "ANALYZE users").await,
        Err(DbError::AuthzDenied(_))
    ));
    assert!(matches!(
        database.query_as(&admin, "ANALYZE users").await?,
        SqlOutput::Rows(_)
    ));

    let report = database.explain(
        "SELECT name FROM users WHERE age = 37",
        ExplainOptions { analyze: true },
    )?;
    assert_eq!(report.nodes[0].actual_rows, Some(1));
    Ok(())
}

#[tokio::test]
async fn phase21_workload_system_view_requires_rbac_and_records_queries()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_orders(&mut database)?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("reader").grant(Resource::Table("users".to_owned()), Permission::Read)
    ]));
    let reader = Principal::new("alice").with_role("reader");

    assert!(matches!(
        database
            .query_as(&reader, "SELECT * FROM system.workload")
            .await,
        Err(DbError::AuthzDenied(_))
    ));

    database.set_authz_policy(AuthzPolicy::new([Role::new("reader")
        .grant(Resource::Table("users".to_owned()), Permission::Read)
        .grant(Resource::System, Permission::Read)]));
    database
        .query_as(&reader, "SELECT name FROM users WHERE age = 37")
        .await?;
    let output = database
        .query_as(&reader, "SELECT * FROM system.workload")
        .await?;

    let SqlOutput::Rows(rows) = output else {
        panic!("system workload should return rows");
    };
    assert!(rows.columns.contains(&"fingerprint".to_owned()));
    assert!(!rows.rows.is_empty(), "expected a non-empty value");
    Ok(())
}

#[tokio::test]
async fn phase21_read_only_selects_do_not_commit_workload_samples()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let mut database = create_database(DbConfig::on_disk(
        Profile::Balanced,
        temp_dir.path().join("workload.redb"),
    ))?;
    seed_orders(&mut database)?;
    let sql = "SELECT name FROM users WHERE id = 1";
    let fingerprint = QueryFingerprint::new(sql).0;
    let before = latest_lsn(&database)?;

    for _ in 0..16 {
        database.query(sql).await?;
    }
    let SqlOutput::Rows(view) = database.query("SELECT * FROM system.workload").await? else {
        panic!("system workload should return rows");
    };
    assert!(view.rows.iter().any(|row| {
        row.first() == Some(&Value::Str(fingerprint.clone())) && row.get(1) == Some(&Value::Int(16))
    }));

    assert_eq!(latest_lsn(&database)?, before);
    assert_eq!(
        persisted_workload_executions(&database, &fingerprint)?,
        None
    );
    assert_eq!(workload_executions(&database, &fingerprint)?, 16);

    database.flush_workload_samples()?;
    assert_eq!(latest_lsn(&database)?, before + 1);
    assert_eq!(
        persisted_workload_executions(&database, &fingerprint)?,
        Some(16)
    );
    assert_eq!(workload_executions(&database, &fingerprint)?, 16);

    database.flush_workload_samples()?;
    assert_eq!(latest_lsn(&database)?, before + 1);
    Ok(())
}

#[tokio::test]
async fn phase21_query_as_selects_commit_only_audit_events()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_orders(&mut database)?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("reader").grant(Resource::Table("users".to_owned()), Permission::Read)
    ]));
    let reader = Principal::new("alice").with_role("reader");
    let sql = "SELECT name FROM users WHERE id = 1";
    let before = latest_lsn(&database)?;
    let events_before = database.audit_events()?;

    for _ in 0..16 {
        database.query_as(&reader, sql).await?;
    }

    assert_eq!(latest_lsn(&database)? - before, 16);
    let events = database.audit_events()?;
    assert_eq!(events.len(), events_before.len() + 16);
    let query_events = |events: &[crate::security::AuditEvent]| {
        events
            .iter()
            .filter(|event| event.action == "query")
            .count()
    };
    assert_eq!(query_events(&events), query_events(&events_before) + 16);
    assert_eq!(
        workload_executions(&database, &QueryFingerprint::new(sql).0)?,
        16
    );
    Ok(())
}

#[tokio::test]
async fn phase21_workload_samples_survive_clean_drop_and_merge_with_persisted_counts()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("workload.redb");
    let sql = "SELECT name FROM users WHERE id = 1";
    let fingerprint = QueryFingerprint::new(sql).0;

    let mut database = create_database(DbConfig::on_disk(Profile::Balanced, &path))?;
    seed_orders(&mut database)?;
    database.record_workload_sample(&WorkloadSample::new(sql))?;
    assert_eq!(
        persisted_workload_executions(&database, &fingerprint)?,
        Some(1)
    );
    for _ in 0..5 {
        database.query(sql).await?;
    }
    assert_eq!(
        persisted_workload_executions(&database, &fingerprint)?,
        Some(1)
    );
    assert_eq!(workload_executions(&database, &fingerprint)?, 6);
    drop(database);

    let reopened = open_database(DbConfig::on_disk(Profile::Balanced, &path))?;
    assert_eq!(
        persisted_workload_executions(&reopened, &fingerprint)?,
        Some(6)
    );
    assert_eq!(workload_executions(&reopened, &fingerprint)?, 6);
    let lsn = latest_lsn(&reopened)?;
    drop(reopened);

    let reopened = open_database(DbConfig::on_disk(Profile::Balanced, &path))?;
    assert_eq!(latest_lsn(&reopened)?, lsn);
    Ok(())
}

#[tokio::test]
async fn phase21_runtime_advice_flushes_buffered_workload_samples()
-> Result<(), Box<dyn std::error::Error>> {
    let sql = "SELECT name FROM users WHERE id = 1";
    let fingerprint = QueryFingerprint::new(sql).0;

    let mut runtime = create_database(DbConfig::new(Profile::InMemory))?;
    seed_orders(&mut runtime)?;
    for _ in 0..8 {
        runtime.query(sql).await?;
    }
    assert_eq!(persisted_workload_executions(&runtime, &fingerprint)?, None);
    runtime.runtime_advice()?;
    assert_eq!(
        persisted_workload_executions(&runtime, &fingerprint)?,
        Some(8)
    );

    let mut index = create_database(DbConfig::new(Profile::InMemory))?;
    seed_orders(&mut index)?;
    for _ in 0..8 {
        index.query(sql).await?;
    }
    assert_eq!(persisted_workload_executions(&index, &fingerprint)?, None);
    index.index_advice()?;
    assert_eq!(
        persisted_workload_executions(&index, &fingerprint)?,
        Some(8)
    );
    Ok(())
}

#[test]
fn phase21_index_advisor_uses_workload_and_catalog_indexes()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_orders(&mut database)?;
    database.record_workload_sample(
        &WorkloadSample::new("SELECT * FROM users WHERE age = 37")
            .with_access("users", "age")
            .with_observed_rows(1, 3_000),
    )?;

    let advice = database.index_advice()?;
    assert!(advice.recommendations.iter().any(|recommendation| {
        matches!(
            recommendation,
            crate::tuning::IndexRecommendation::Create { candidate, .. }
                if candidate.table == "users" && candidate.column == "age"
        )
    }));
    Ok(())
}

#[test]
fn phase21_tuning_policy_applies_rolls_back_and_logs() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let mut policy = TuningPolicy {
        enabled: true,
        recommend_only: false,
        ..TuningPolicy::default()
    };
    policy.envelopes.insert(
        TunableParameter::CacheMaxCapacity,
        ParameterEnvelope::new(64, 4_096, 64),
    );
    database.set_tuning_policy(&policy)?;

    let old = database.performance_config().cache.max_capacity;
    let entry = database.apply_tuning(TuningDecision::new(
        TunableParameter::CacheMaxCapacity,
        old,
        1_024,
        "cache miss pressure",
    ))?;
    assert_eq!(database.performance_config().cache.max_capacity, 1_024);

    let rollback = database.rollback_tuning(&entry.id, "p95 regression")?;
    assert_eq!(database.performance_config().cache.max_capacity, old);
    assert_eq!(rollback.rollback_of.as_deref(), Some(entry.id.as_str()));
    assert_eq!(database.tuning_log()?.len(), 2);
    Ok(())
}

#[test]
fn phase21_tuning_regression_gate_rolls_back_and_audits() -> Result<(), Box<dyn std::error::Error>>
{
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let mut policy = TuningPolicy {
        enabled: true,
        recommend_only: false,
        ..TuningPolicy::default()
    };
    policy.envelopes.insert(
        TunableParameter::CacheMaxCapacity,
        ParameterEnvelope::new(64, 4_096, 64),
    );
    database.set_tuning_policy(&policy)?;

    let old = database.performance_config().cache.max_capacity;
    let entry = database.apply_tuning(TuningDecision::new(
        TunableParameter::CacheMaxCapacity,
        old,
        1_024,
        "cache miss pressure",
    ))?;
    let baseline = [BenchmarkReport {
        name: "point_read".to_owned(),
        throughput_ops_per_sec: 100.0,
        p50_ms: 4.0,
        p95_ms: 10.0,
        p99_ms: 20.0,
        metadata: BTreeMap::new(),
    }];
    let candidate = [BenchmarkReport {
        name: "point_read".to_owned(),
        throughput_ops_per_sec: 70.0,
        p50_ms: 6.0,
        p95_ms: 15.0,
        p99_ms: 30.0,
        metadata: BTreeMap::new(),
    }];

    let rollback = database.evaluate_tuning_regression(
        &entry.id,
        &baseline,
        &candidate,
        RegressionGate::new(10),
    )?;

    let Some(rollback) = rollback else {
        panic!("regression must trigger rollback");
    };
    assert_eq!(database.performance_config().cache.max_capacity, old);
    assert_eq!(rollback.rollback_of.as_deref(), Some(entry.id.as_str()));
    assert_eq!(database.tuning_log()?.len(), 2);
    assert!(database.audit_events()?.iter().any(|event| {
        event.action == "evaluate_tuning_regression"
            && event
                .detail
                .as_deref()
                .is_some_and(|detail| detail.contains("performance regression"))
    }));
    Ok(())
}

#[test]
fn phase21_reprofile_job_lifecycle_is_visible() -> Result<(), Box<dyn std::error::Error>> {
    let database = create_database(DbConfig::new(Profile::InMemory))?;
    let job = database.start_reprofile(ReprofilePlan {
        object: "sales".to_owned(),
        from_layout: TableLayout::Row,
        to_layout: TableLayout::Columnar,
        reversible: true,
    })?;

    assert_eq!(
        database.reprofile_status(&job.id)?.map(|job| job.status),
        Some(ReprofileStatus::PlanningOnly)
    );
    assert!(
        database
            .advance_reprofile(
                &job.id,
                ReprofileStatus::ReadyToSwitch,
                "shadow copy verified"
            )
            .is_err()
    );
    assert_eq!(database.reprofile_jobs()?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn phase22_pg_catalog_and_version_are_queryable() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_orders(&mut database)?;

    let SqlOutput::Rows(version) = database.query("SELECT version()").await? else {
        panic!("version should return rows");
    };
    assert!(matches!(&version.rows[0][0], Value::Str(value) if value.contains("multidb")));

    let SqlOutput::Rows(columns) = database
        .query("SELECT * FROM information_schema.columns")
        .await?
    else {
        panic!("information_schema.columns should return rows");
    };
    assert!(columns.rows.iter().any(|row| {
        row.get(1) == Some(&Value::Str("users".to_owned()))
            && row.get(2) == Some(&Value::Str("name".to_owned()))
    }));

    let SqlOutput::Rows(filtered) = database
        .query("SELECT table_name FROM information_schema.tables WHERE table_name = 'users'")
        .await?
    else {
        panic!("filtered information_schema query should return rows");
    };
    assert_eq!(filtered.columns, vec!["table_name".to_owned()]);
    assert_eq!(filtered.rows, vec![vec![Value::Str("users".to_owned())]]);
    Ok(())
}

#[tokio::test]
async fn phase22_insert_returning_and_upsert_work() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_orders(&mut database)?;

    assert_eq!(
        database
            .query("INSERT INTO users (id, name, age) VALUES (3, 'Lin', 41) RETURNING id, name")
            .await?,
        SqlOutput::Rows(SqlRows {
            columns: vec!["id".to_owned(), "name".to_owned()],
            rows: vec![vec![Value::Int(3), Value::Str("Lin".to_owned())]],
        })
    );

    assert_eq!(
            database
                .query("INSERT INTO users (id, name, age) VALUES (3, 'Ignored', 1) ON CONFLICT (id) DO NOTHING")
                .await?,
            SqlOutput::AffectedRows(0)
        );

    assert_eq!(
            database
                .query("INSERT INTO users (id, name, age) VALUES (3, 'Lin2', 42) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, age = EXCLUDED.age RETURNING *")
                .await?,
            SqlOutput::Rows(SqlRows {
                columns: vec!["id".to_owned(), "name".to_owned(), "age".to_owned()],
                rows: vec![vec![
                    Value::Int(3),
                    Value::Str("Lin2".to_owned()),
                    Value::Int(42)
                ]],
            })
        );
    Ok(())
}

#[test]
fn phase22_jsonl_export_import_round_trips_table() -> Result<(), Box<dyn std::error::Error>> {
    let mut source = create_database(DbConfig::new(Profile::InMemory))?;
    seed_orders(&mut source)?;
    let jsonl = crate::migration::export_table_jsonl(&source, "users")?;

    let mut target = create_database(DbConfig::new(Profile::InMemory))?;
    target.create_table("users", Some(account_schema()), Vec::new())?;
    let report = crate::migration::import_table_jsonl(
        &target,
        "users",
        &jsonl,
        &crate::migration::ImportOptions::default(),
    )?;
    assert_eq!(report.written_rows, 2);
    assert_eq!(target.table("users")?.scan()?.len(), 2);
    Ok(())
}

#[test]
fn rbac_admin_can_create_catalog_objects() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("admin").grant(Resource::Database, Permission::Admin)
    ]));
    let principal = Principal::new("root").with_role("admin");

    database.create_table_as(&principal, "users", Some(account_schema()), Vec::new())?;
    database.create_collection_as(
        &principal,
        "profiles",
        CollectionId::new(77),
        vec![DocField::document_id("id")],
        Vec::new(),
    )?;

    let audit = database.audit_events()?;
    assert!(audit.iter().any(|event| event.action == "create_table"));
    assert!(
        audit
            .iter()
            .any(|event| event.action == "create_collection")
    );
    Ok(())
}

#[tokio::test]
async fn phase20_sql_registers_and_calls_wasm_udf_with_rbac()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.set_authz_policy(AuthzPolicy::new([Role::new("extension_admin")
        .grant(Resource::System, Permission::Admin)
        .grant(Resource::Database, Permission::Read)]));
    let admin = Principal::new("alice").with_role("extension_admin");
    let wasm = constant_value_wasm(&Value::Int(20))?;
    let hex = hex_encode(&wasm);
    let create = format!("CREATE FUNCTION phase20 LANGUAGE wasm AS HEX '{hex}'");

    assert_eq!(
        database.query_as(&admin, &create).await?,
        SqlOutput::AffectedRows(1)
    );
    assert_eq!(database.udf_specs()?.len(), 1);
    assert_eq!(
        database.query_as(&admin, "SELECT phase20()").await?,
        SqlOutput::Rows(SqlRows {
            columns: vec!["phase20".to_owned()],
            rows: vec![vec![Value::Int(20)]],
        })
    );

    Ok(())
}

#[tokio::test]
async fn phase20_query_as_applies_masking_policy() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_orders(&mut database)?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("reader").grant(Resource::Table("users".to_owned()), Permission::Read)
    ]));
    database.set_policy_config(&PolicyConfig {
        validations: Vec::new(),
        masking: vec![MaskingPolicy {
            resource: Resource::Table("users".to_owned()),
            paths: vec![FieldPath::new(["name"])],
            replacement: Value::Str("***".to_owned()),
        }],
        row_policies: Vec::new(),
        limits: LimitPolicy::default(),
    })?;
    let reader = Principal::new("alice").with_role("reader");

    assert_eq!(
        database
            .query_as(&reader, "SELECT name FROM users WHERE age = 37")
            .await?,
        SqlOutput::Rows(SqlRows {
            columns: vec!["name".to_owned()],
            rows: vec![vec![Value::Str("***".to_owned())]],
        })
    );

    Ok(())
}

#[tokio::test]
async fn multi_resource_query_with_masking_policy_fails_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_orders(&mut database)?;
    database.set_authz_policy(AuthzPolicy::new([Role::new("reader")
        .grant(Resource::Table("users".to_owned()), Permission::Read)
        .grant(Resource::Table("orders".to_owned()), Permission::Read)]));
    database.set_policy_config(&PolicyConfig {
        validations: Vec::new(),
        masking: vec![MaskingPolicy {
            resource: Resource::Table("users".to_owned()),
            paths: vec![FieldPath::new(["name"])],
            replacement: Value::Str("***".to_owned()),
        }],
        row_policies: Vec::new(),
        limits: LimitPolicy::default(),
    })?;
    let reader = Principal::new("alice").with_role("reader");

    let error = match database
        .query_as(
            &reader,
            "SELECT users.name, orders.amount \
             FROM users JOIN orders ON users.id = orders.user_id",
        )
        .await
    {
        Ok(output) => panic!("join with protected resources should fail closed: {output:?}"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        DbError::Extension(ExtensionError::PolicyDenied(message))
            if message.contains("multi-resource queries")
    ));
    Ok(())
}

#[test]
fn phase29_catalog_rejects_duplicate_ids_and_bad_names() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_collection(
        "profiles",
        CollectionId::new(7),
        user_doc_fields(),
        Vec::new(),
    )?;
    assert!(matches!(
        database.create_vector_collection(
            "embeddings",
            VectorCollectionConfig::new(CollectionId::new(7), 3),
        ),
        Err(DbError::CatalogObjectExists(_))
    ));
    database.create_graph("social", GraphId::new(1))?;
    assert!(matches!(
        database.create_graph("other_social", GraphId::new(1)),
        Err(DbError::CatalogObjectExists(_))
    ));
    assert!(matches!(
        database.create_collection(
            "bad-name",
            CollectionId::new(8),
            user_doc_fields(),
            Vec::new()
        ),
        Err(DbError::InvalidCatalogName(_))
    ));
    Ok(())
}

#[test]
fn phase29_phase19_parser_uses_ast() -> Result<(), Box<dyn std::error::Error>> {
    assert!(super::parse_phase19_call("SELECT rematch('idx', 'x')")?.is_none());
    assert!(super::parse_phase19_call("SELECT 'match(idx)'")?.is_none());
    assert_eq!(
        super::parse_phase19_call("SELECT * FROM within_radius('places', 1.0, -2.0, 3)")?
            .map(|call| call.name),
        Some("within_radius".to_owned())
    );
    Ok(())
}

#[test]
fn phase29_vector_handles_share_fresh_index_state() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_vector_collection(
        "embeddings",
        VectorCollectionConfig::new(CollectionId::new(70), 3),
    )?;
    let first = database.vector_collection("embeddings")?;
    let second = database.vector_collection("embeddings")?;
    let id = first.insert_vector(&Value::Str("Ada".to_owned()), vec![1.0, 0.0, 0.0])?;

    assert_eq!(second.knn(&[1.0, 0.0, 0.0], 1)?[0].id, id);
    second.delete_vector(id)?;
    assert!(
        first.knn(&[1.0, 0.0, 0.0], 1)?.is_empty(),
        "expected an empty value, got {:?}",
        first.knn(&[1.0, 0.0, 0.0], 1)?
    );
    Ok(())
}

#[test]
fn phase29_hooks_fire_for_time_series_and_graph() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_time_series(TimeSeriesConfig::new("metrics"))?;
    database.create_graph("social", GraphId::new(9))?;
    database.register_hook(&HookSpec::after(
        "ts_after",
        HookTarget::TimeSeries("metrics".to_owned()),
        HookAction::RecordAfterCommit,
        1_000,
    ))?;
    database.register_hook(&HookSpec::after(
        "graph_after",
        HookTarget::Graph("social".to_owned()),
        HookAction::RecordAfterCommit,
        1_000,
    ))?;

    database.time_series("metrics")?.insert_point(
        "cpu",
        TimePoint {
            timestamp_millis: 1,
            value: 0.5,
        },
    )?;
    database.graph("social")?.add_edge(
        GraphNodeId::from_str_id("alice"),
        "knows",
        GraphNodeId::from_str_id("bob"),
        &Value::Null,
    )?;

    let deliveries =
        database.range(HOOK_DELIVERIES_TABLE, &[], &[0xFF], ReadConsistency::Strong)?;
    assert!(
        deliveries
            .iter()
            .any(|(_, value)| { String::from_utf8_lossy(value).contains("ts_after") })
    );
    assert!(
        deliveries
            .iter()
            .any(|(_, value)| { String::from_utf8_lossy(value).contains("graph_after") })
    );
    Ok(())
}

#[tokio::test]
async fn time_bucket_sql_rejects_overflowing_timestamp() -> Result<(), Box<dyn std::error::Error>> {
    let database = create_database(DbConfig::new(Profile::InMemory))?;

    assert!(matches!(
        database
            .query("SELECT time_bucket(60000, -9223372036854775807)")
            .await,
        Err(DbError::TimeSeries(TimeSeriesError::InvalidPoint(_)))
    ));

    let SqlOutput::Rows(rows) = database.query("SELECT time_bucket(1000, -1)").await? else {
        return Err("time_bucket should return rows".into());
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(-1_000)]]);
    Ok(())
}

#[test]
fn phase29_changefeed_as_masks_table_values() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_table("users", Some(account_schema()), Vec::new())?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("reader").grant(Resource::Table("users".to_owned()), Permission::Read)
    ]));
    database.set_policy_config(&PolicyConfig {
        validations: Vec::new(),
        masking: vec![MaskingPolicy {
            resource: Resource::Table("users".to_owned()),
            paths: vec![FieldPath::new(["name"])],
            replacement: Value::Str("***".to_owned()),
        }],
        row_policies: Vec::new(),
        limits: LimitPolicy::default(),
    })?;
    let reader = Principal::new("alice").with_role("reader");
    database.table("users")?.insert(vec![
        Value::Int(1),
        Value::Str("Ada".to_owned()),
        Value::Int(37),
    ])?;

    let (events, _) = database.poll_changefeed_as(
        &reader,
        &ResumeToken::default(),
        &ChangefeedFilter {
            target: ChangefeedTarget::Table("users".to_owned()),
        },
        &ChangefeedOptions::default(),
        100,
    )?;
    let masked = events
        .into_iter()
        .find_map(|event| match event.op {
            ChangeOp::Upsert { value_after, .. } => Some(value_after),
            _ => None,
        })
        .ok_or("missing upsert")?;
    assert_eq!(
        decode_value(&masked)?,
        Value::Array(vec![
            Value::Int(1),
            Value::Str("***".to_owned()),
            Value::Int(37)
        ])
    );
    Ok(())
}

#[tokio::test]
async fn persisted_authz_policy_survives_reopen() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("authz.redb");
    let security = security_for_alice_users_reader();

    {
        let mut database = create_database_with_ops(
            DbConfig::on_disk(Profile::Transactional, &path),
            OperationalConfig::new().with_security(security),
        )?;
        seed_orders(&mut database)?;
    }

    let database = open_database_with_ops(
        DbConfig::on_disk(Profile::Transactional, &path),
        &OperationalConfig::default(),
    )?;
    let principal = database.principal_for_user("alice");

    assert!(matches!(
        database
            .query_as(&principal, "SELECT name FROM users")
            .await?,
        SqlOutput::Rows(_)
    ));
    assert!(matches!(
        database
            .query_as(
                &principal,
                "INSERT INTO users (id, name, age) VALUES (3, 'Lin', 41)"
            )
            .await,
        Err(DbError::AuthzDenied(_))
    ));
    Ok(())
}

#[tokio::test]
async fn old_database_without_authz_opens_default_deny() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("old.redb");

    {
        let mut database = create_database(DbConfig::on_disk(Profile::Transactional, &path))?;
        seed_orders(&mut database)?;
    }

    let database = open_database_with_ops(
        DbConfig::on_disk(Profile::Transactional, &path),
        &OperationalConfig::default(),
    )?;
    assert!(matches!(
        database
            .query_as(&Principal::new("alice"), "SELECT name FROM users")
            .await,
        Err(DbError::AuthzDenied(_))
    ));
    Ok(())
}

#[tokio::test]
async fn serve_reopen_keeps_persisted_security_state() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("serve-security.redb");
    let admin = Principal::new("admin").with_role("admin");
    let alice = Principal::new("alice").with_role("reader");

    {
        let mut database = create_database(DbConfig::on_disk(Profile::Transactional, &path))?;
        seed_orders(&mut database)?;
        database.ensure_bootstrap_admin_principal()?;
        let stored_admin = database
            .authz_policy()
            .roles()
            .get("admin")
            .cloned()
            .ok_or("missing bootstrap admin role")?;
        database.set_authz_policy_as(
            &admin,
            AuthzPolicy::new([
                stored_admin,
                Role::new("reader").grant(Resource::Table("users".to_owned()), Permission::Read),
            ]),
        )?;
        database.set_principal_registry_as(
            &admin,
            PrincipalRegistry::new().with_principal("alice", alice.clone()),
        )?;
        database.set_audit_enabled_as(&admin, false)?;
    }

    {
        let mut database = create_database(DbConfig::on_disk(Profile::Transactional, &path))?;
        assert!(database.authz_policy().roles().contains_key("reader"));
        assert_eq!(database.principal_for_user("alice"), alice);
        assert!(
            !database
                .principal_registry()
                .principals()
                .contains_key("admin")
        );
        assert!(!database.audit_enabled());
        let principal = database.principal_for_user("alice");
        assert!(matches!(
            database
                .query_as(&principal, "SELECT name FROM users")
                .await?,
            SqlOutput::Rows(_)
        ));
        database.ensure_bootstrap_admin_principal()?;
    }

    let database = open_database(DbConfig::on_disk(Profile::Transactional, &path))?;
    assert_eq!(
        database
            .authz_policy()
            .roles()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["admin", "reader"]
    );
    assert_eq!(database.principal_for_user("alice"), alice);
    assert_eq!(database.principal_for_user("admin"), admin);
    assert!(!database.audit_enabled());
    let principal = database.principal_for_user("alice");
    assert!(matches!(
        database
            .query_as(&principal, "SELECT name FROM users")
            .await?,
        SqlOutput::Rows(_)
    ));
    assert!(matches!(
        database
            .query_as(
                &principal,
                "INSERT INTO users (id, name, age) VALUES (3, 'Lin', 41)"
            )
            .await,
        Err(DbError::AuthzDenied(_))
    ));
    Ok(())
}

#[test]
fn bootstrap_admin_merges_into_existing_security() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let users = Resource::Table("users".to_owned());
    let reader = Role::new("reader").grant(users.clone(), Permission::Read);
    let alice = Principal::new("alice").with_role("reader");
    database.set_authz_policy(AuthzPolicy::new([
        reader.clone(),
        Role::new("admin").grant(users.clone(), Permission::Read),
    ]));
    database.set_principal_registry(
        PrincipalRegistry::new()
            .with_principal("alice", alice.clone())
            .with_principal("admin", Principal::new("admin").with_role("ops")),
    );
    let initial_txn = database.read(
        txn::TXN_META_TABLE,
        txn::CURRENT_TXN_ID_KEY,
        ReadConsistency::Strong,
    )?;

    database.ensure_bootstrap_admin_principal()?;

    let policy = database.authz_policy();
    assert_eq!(policy.roles().get("reader"), Some(&reader));
    let admin_role = policy.roles().get("admin").ok_or("missing admin role")?;
    assert!(
        admin_role
            .grants()
            .get(&users)
            .is_some_and(|permissions| permissions.contains(&Permission::Read))
    );
    let admin_member = Principal::new("member").with_role("admin");
    policy.authorize(&admin_member, &Resource::System, Permission::Admin)?;
    policy.authorize(&admin_member, &Resource::Database, Permission::Write)?;
    assert_eq!(database.principal_for_user("alice"), alice);
    assert_eq!(
        database.principal_for_user("admin"),
        Principal::new("admin").with_role("admin").with_role("ops")
    );

    let txn_after_merge = database.read(
        txn::TXN_META_TABLE,
        txn::CURRENT_TXN_ID_KEY,
        ReadConsistency::Strong,
    )?;
    assert_ne!(txn_after_merge, initial_txn);
    let policy_after_merge = database.authz_policy().clone();
    let registry_after_merge = database.principal_registry().clone();

    database.ensure_bootstrap_admin_principal()?;

    assert_eq!(
        database.read(
            txn::TXN_META_TABLE,
            txn::CURRENT_TXN_ID_KEY,
            ReadConsistency::Strong,
        )?,
        txn_after_merge
    );
    assert_eq!(database.authz_policy(), &policy_after_merge);
    assert_eq!(database.principal_registry(), &registry_after_merge);
    Ok(())
}

#[test]
fn plain_open_rejects_corrupt_persisted_security() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("corrupt-security.redb");
    let config = DbConfig::on_disk(Profile::Transactional, &path);

    {
        let database = create_database(config.clone())?;
        assert!(database.authz_policy().roles().is_empty());
        assert!(database.principal_registry().principals().is_empty());
        assert!(database.audit_enabled());
    }

    {
        let engine = RedbEngine::open(&path)?;
        let mut txn = engine.begin_write()?;
        txn.put(super::AUTHZ_TABLE, super::KEY_AUTHZ_POLICY, b"not-json")?;
        txn.commit()?;
    }

    assert!(matches!(
        open_database(config.clone()),
        Err(ConfigError::Serde(_))
    ));
    assert!(matches!(
        create_database(config),
        Err(ConfigError::Serde(_))
    ));
    Ok(())
}

#[test]
fn audit_events_as_requires_system_admin() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("admin").grant(Resource::System, Permission::Admin)
    ]));
    let admin = Principal::new("root").with_role("admin");

    assert!(matches!(
        database.audit_events_as(&Principal::new("alice")),
        Err(DbError::AuthzDenied(_))
    ));
    let events = database.audit_events_as(&admin)?;
    assert!(
        events.iter().any(|event| {
            event.action == "audit_events" && event.outcome == AuditOutcome::Denied
        })
    );
    Ok(())
}

#[test]
fn audit_chain_verifies_and_detects_tampering() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("admin").grant(Resource::System, Permission::Admin)
    ]));
    let admin = Principal::new("root").with_role("admin");

    assert!(matches!(
        database.audit_events_as(&Principal::new("alice")),
        Err(DbError::AuthzDenied(_))
    ));
    database.audit_events_as(&admin)?;
    database.verify_audit_chain()?;

    let mut events = database.audit_events()?;
    let Some(mut first_event) = events.pop() else {
        return Err("expected at least one audit event".into());
    };
    first_event.detail = Some("tampered".to_owned());
    let value = serde_json::to_vec(&first_event)?;
    propose_system(
        &database.repl,
        Op::Put {
            table: AUDIT_TABLE.to_owned(),
            key: first_event.key(),
            value,
        },
    )?;

    assert!(matches!(
        database.verify_audit_chain(),
        Err(DbError::AuditIntegrity(_))
    ));
    Ok(())
}

#[test]
fn audit_chain_detects_truncation() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("admin").grant(Resource::System, Permission::Admin)
    ]));
    let admin = Principal::new("root").with_role("admin");

    database.audit_events_as(&Principal::new("alice")).ok();
    database.audit_events_as(&admin)?;
    database.verify_audit_chain()?;

    let events = database.audit_events()?;
    let last = events.last().ok_or("expected audit event")?;
    propose_system(
        &database.repl,
        Op::Delete {
            table: AUDIT_TABLE.to_owned(),
            key: last.key(),
        },
    )?;

    assert!(matches!(
        database.verify_audit_chain(),
        Err(DbError::AuditIntegrity(_))
    ));
    Ok(())
}

#[test]
fn concurrent_audit_writes_keep_one_chain() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("admin").grant(Resource::System, Permission::Admin)
    ]));
    let database = Arc::new(database);
    let admin = Principal::new("root").with_role("admin");

    let mut handles = Vec::new();
    for _ in 0..8 {
        let database = Arc::clone(&database);
        let admin = admin.clone();
        handles.push(thread::spawn(move || -> Result<(), String> {
            for _ in 0..25 {
                database
                    .audit_events_as(&admin)
                    .map_err(|error| error.to_string())?;
            }
            Ok(())
        }));
    }
    for handle in handles {
        handle.join().map_err(|_| "audit thread panicked")??;
    }

    database.verify_audit_chain()?;
    Ok(())
}

#[test]
fn audit_can_only_be_disabled_by_admin_api() -> Result<(), Box<dyn std::error::Error>> {
    let security = SecurityConfig {
        audit_enabled: false,
        ..SecurityConfig::default()
    };
    assert!(matches!(
        create_database_with_ops(
            DbConfig::new(Profile::InMemory),
            OperationalConfig::new().with_security(security)
        ),
        Err(ConfigError::Unsupported(_))
    ));

    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("admin").grant(Resource::System, Permission::Admin)
    ]));
    let admin = Principal::new("root").with_role("admin");

    assert!(matches!(
        database.set_audit_enabled_as(&Principal::new("alice"), false),
        Err(DbError::AuthzDenied(_))
    ));
    database.set_audit_enabled_as(&admin, false)?;
    let events = database.audit_events()?;
    assert!(events.iter().any(|event| {
        event.action == "set_audit_enabled"
            && event.outcome == AuditOutcome::Succeeded
            && event.detail.as_deref() == Some("enabled: false")
    }));
    Ok(())
}

#[test]
fn config_apply_confirm_is_audited_noop() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("admin").grant(Resource::System, Permission::Admin)
    ]));
    let admin = Principal::new("root").with_role("admin");
    let current = DatabaseSpec::from_db_config("current", database.config());
    let plan = MigrationPlanner::plan(&current, &current);
    let before_catalog = database.catalog().len();

    let report = database.confirm_config_apply_as(&admin, &plan, &plan.required_confirmation)?;

    assert_eq!(report.status, ApplyStatus::Confirmed);
    assert!(report.audit_recorded);
    assert!(!report.data_mutated);
    assert_eq!(database.catalog().len(), before_catalog);
    assert!(database.audit_events()?.iter().any(|event| {
        event.action == "config_apply" && event.outcome == AuditOutcome::Succeeded
    }));
    Ok(())
}

#[test]
fn config_apply_unsupported_plan_is_audited_failed_noop() -> Result<(), Box<dyn std::error::Error>>
{
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("admin").grant(Resource::System, Permission::Admin)
    ]));
    let admin = Principal::new("root").with_role("admin");
    let current = DatabaseSpec::from_db_config("current", database.config());
    let mut desired = current.clone();
    desired.profile = "secure_app".to_owned();
    desired.domains[0].mode = ConsistencyMode::StrongCp;
    let plan = MigrationPlanner::plan(&current, &desired);
    let before_catalog = database.catalog().len();

    let report = database.confirm_config_apply_as(&admin, &plan, &plan.required_confirmation)?;

    assert_eq!(report.status, ApplyStatus::Unsupported);
    assert!(report.audit_recorded);
    assert!(!report.data_mutated);
    assert_eq!(database.catalog().len(), before_catalog);
    assert!(
        database.audit_events()?.iter().any(|event| {
            event.action == "config_apply" && event.outcome == AuditOutcome::Failed
        })
    );
    Ok(())
}

#[test]
fn config_apply_requires_system_admin() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("admin").grant(Resource::System, Permission::Admin)
    ]));
    let current = DatabaseSpec::from_db_config("current", database.config());
    let plan = MigrationPlanner::plan(&current, &current);

    assert!(matches!(
        database.confirm_config_apply_as(
            &Principal::new("alice"),
            &plan,
            &plan.required_confirmation
        ),
        Err(DbError::AuthzDenied(_))
    ));
    assert!(
        database.audit_events()?.iter().any(|event| {
            event.action == "config_apply" && event.outcome == AuditOutcome::Denied
        })
    );
    Ok(())
}

#[test]
fn public_writes_reject_reserved_keyspaces() -> Result<(), Box<dyn std::error::Error>> {
    let database = create_database(DbConfig::new(Profile::InMemory))?;
    let before = database.read(
        txn::TXN_META_TABLE,
        txn::CURRENT_TXN_ID_KEY,
        ReadConsistency::Strong,
    )?;

    assert_reserved_put_rejected(&database, txn::TXN_META_TABLE);
    assert_reserved_put_rejected(&database, AUDIT_TABLE);
    assert_reserved_put_rejected(&database, AUDIT_HEAD_TABLE);
    assert_reserved_put_rejected(&database, super::AUTHZ_TABLE);
    assert_reserved_condition_rejected(&database, AUDIT_TABLE);

    let after = database.read(
        txn::TXN_META_TABLE,
        txn::CURRENT_TXN_ID_KEY,
        ReadConsistency::Strong,
    )?;
    assert_eq!(before, after);

    Ok(())
}

#[test]
fn cluster_backends_reject_public_reserved_keyspaces() -> Result<(), Box<dyn std::error::Error>> {
    let cp =
        create_simulated_cluster_database(DbConfig::new(Profile::InMemory), cp_cluster_config())?;
    assert_reserved_put_rejected(&cp, AUDIT_TABLE);
    assert_reserved_condition_rejected(&cp, AUDIT_HEAD_TABLE);

    let ap = create_ap_database(
        DbConfig::new(Profile::InMemory).with_replication(ReplicationKind::Ap),
        ap_cluster_config(),
    )?;
    assert_reserved_put_rejected(&ap, AUDIT_TABLE);

    let sharded = create_sharded_database(DbConfig::new(Profile::InMemory), sharded_config())?;
    assert_reserved_put_rejected(&sharded, super::AUTHZ_TABLE);
    assert_reserved_condition_rejected(&sharded, AUDIT_HEAD_TABLE);
    Ok(())
}

#[tokio::test]
async fn create_database_with_ops_encrypts_and_reopens() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("encrypted.redb");
    let key_path = temp_dir.path().join("key.bin");
    let wrong_key_path = temp_dir.path().join("wrong-key.bin");
    std::fs::write(&key_path, [7_u8; 32])?;
    std::fs::write(&wrong_key_path, [8_u8; 32])?;
    let ops = OperationalConfig::new().with_encryption(EncryptionConfig::file_key(&key_path));

    {
        let mut database =
            create_database_with_ops(DbConfig::on_disk(Profile::Transactional, &path), ops)?;
        seed_orders(&mut database)?;
    }

    let raw = std::fs::read(&path)?;
    assert!(!raw.windows(b"Ada".len()).any(|window| window == b"Ada"));

    let database = open_database_with_ops(
        DbConfig::on_disk(Profile::Transactional, &path),
        &OperationalConfig::new().with_encryption(EncryptionConfig::file_key(&key_path)),
    )?;
    assert!(matches!(
        database.query("SELECT name FROM users").await?,
        SqlOutput::Rows(_)
    ));
    drop(database);

    match open_database_with_ops(
        DbConfig::on_disk(Profile::Transactional, &path),
        &OperationalConfig::new().with_encryption(EncryptionConfig::file_key(&wrong_key_path)),
    ) {
        Err(ConfigError::Storage(StorageError::Corruption(_))) => {}
        Err(error) => panic!("expected corruption, got {error:?}"),
        Ok(_) => panic!("wrong encryption key unexpectedly opened the database"),
    }
    Ok(())
}

#[tokio::test]
async fn envelope_rotate_reencrypt_shred_keeps_database_readable()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("envelope.redb");
    let keyring_path = temp_dir.path().join("keyring.json");
    let kek_path = temp_dir.path().join("kek.bin");
    std::fs::write(&kek_path, [5_u8; 32])?;
    let ops = || {
        OperationalConfig::new()
            .with_encryption(EncryptionConfig::local_envelope(&keyring_path, &kek_path))
    };

    {
        let mut database =
            create_database_with_ops(DbConfig::on_disk(Profile::Transactional, &path), ops())?;
        seed_orders(&mut database)?;
        assert_eq!(database.rotate_dek()?, 2);
        database.table("users")?.insert(vec![
            Value::Int(3),
            Value::Str("Linus".to_owned()),
            Value::Int(54),
        ])?;

        match database.destroy_dek(1) {
            Err(DbError::Storage(StorageError::Backend(message))) => {
                assert!(message.contains("still encrypted"), "{message}");
            }
            other => panic!("expected in-use key refusal, got {other:?}"),
        }
        match database.destroy_dek(2) {
            Err(DbError::Storage(StorageError::Backend(message))) => {
                assert!(message.contains("current"), "{message}");
            }
            other => panic!("expected current key refusal, got {other:?}"),
        }

        assert!(database.reencrypt_existing_values(4)?.reencrypted_values > 0);
        assert_eq!(database.reencrypt_existing_values(4)?.reencrypted_values, 0);
        database.destroy_dek(1)?;
        assert_eq!(database.encryption_key_versions()?, vec![2]);
    }

    let reopened =
        open_database_with_ops(DbConfig::on_disk(Profile::Transactional, &path), &ops())?;
    let SqlOutput::Rows(rows) = reopened.query("SELECT name FROM users").await? else {
        panic!("user query should return rows");
    };
    assert_eq!(rows.rows.len(), 3);

    let (events, _) = reopened.poll_changefeed(
        &ResumeToken::default(),
        &ChangefeedFilter {
            target: ChangefeedTarget::Table("users".to_owned()),
        },
        &ChangefeedOptions::default(),
        100,
    )?;
    assert!(
        events
            .iter()
            .filter(|event| matches!(event.op, ChangeOp::Upsert { .. }))
            .count()
            >= 3
    );

    crate::backup::full_backup(
        &reopened,
        temp_dir.path().join("backups"),
        &crate::backup::BackupConfig::default(),
    )?;
    Ok(())
}

#[tokio::test]
async fn envelope_database_with_missing_keyring_fails_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("envelope.redb");
    let keyring_path = temp_dir.path().join("keyring.json");
    let kek_path = temp_dir.path().join("kek.bin");
    std::fs::write(&kek_path, [5_u8; 32])?;
    let ops = || {
        OperationalConfig::new()
            .with_encryption(EncryptionConfig::local_envelope(&keyring_path, &kek_path))
    };

    {
        let mut database =
            create_database_with_ops(DbConfig::on_disk(Profile::Transactional, &path), ops())?;
        seed_orders(&mut database)?;
    }
    let keyring = std::fs::read(&keyring_path)?;
    std::fs::remove_file(&keyring_path)?;

    match open_database_with_ops(DbConfig::on_disk(Profile::Transactional, &path), &ops()) {
        Err(ConfigError::Storage(StorageError::Corruption(message))) => {
            assert!(message.contains("keyring"), "{message}");
        }
        Err(error) => panic!("expected missing keyring corruption, got {error:?}"),
        Ok(_) => panic!("database opened without its keyring"),
    }
    assert!(!keyring_path.exists());
    match create_database_with_ops(DbConfig::on_disk(Profile::Transactional, &path), ops()) {
        Err(ConfigError::Storage(StorageError::Corruption(message))) => {
            assert!(message.contains("keyring"), "{message}");
        }
        Err(error) => panic!("expected missing keyring corruption, got {error:?}"),
        Ok(_) => panic!("database was recreated without its keyring"),
    }
    assert!(!keyring_path.exists());

    std::fs::write(&keyring_path, keyring)?;
    let reopened =
        open_database_with_ops(DbConfig::on_disk(Profile::Transactional, &path), &ops())?;
    let SqlOutput::Rows(rows) = reopened.query("SELECT name FROM users").await? else {
        panic!("user query should return rows");
    };
    assert_eq!(rows.rows.len(), 2);
    Ok(())
}

#[test]
fn engine_for_operational_uses_encrypted_dispatch() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let key_path = temp_dir.path().join("key.bin");
    std::fs::write(&key_path, [9_u8; 32])?;
    let ops = OperationalConfig::new().with_encryption(EncryptionConfig::file_key(&key_path));

    assert_eq!(
        engine_for_operational(&DbConfig::new(Profile::InMemory), &ops)?.kind(),
        EngineKind::EncryptedMemory
    );

    let path = temp_dir.path().join("encrypted.redb");
    assert_eq!(
        engine_for_operational(&DbConfig::on_disk(Profile::Transactional, path), &ops)?.kind(),
        EngineKind::EncryptedRedb
    );
    Ok(())
}

#[test]
fn engine_for_operational_uses_compressed_dispatch() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let key_path = temp_dir.path().join("key.bin");
    std::fs::write(&key_path, [3_u8; 32])?;

    let mut memory_performance = PerformanceConfig::for_profile(Profile::InMemory);
    memory_performance.compression.algorithm = CompressionAlgorithm::Lz4;
    memory_performance.compression.min_bytes = 1;
    let memory_ops = OperationalConfig::new().with_performance(memory_performance.clone());
    assert_eq!(
        engine_for_operational(&DbConfig::new(Profile::InMemory), &memory_ops)?.kind(),
        EngineKind::CompressedMemory
    );

    let database = create_database_with_ops(DbConfig::new(Profile::InMemory), memory_ops)?;
    assert_eq!(database.engine_kind(), EngineKind::CompressedMemory);
    assert_eq!(
        database.performance_config().compression.algorithm,
        CompressionAlgorithm::Lz4
    );

    let mut disk_performance = PerformanceConfig::for_profile(Profile::Transactional);
    disk_performance.compression.algorithm = CompressionAlgorithm::Lz4;
    disk_performance.compression.min_bytes = 1;
    let disk_ops = OperationalConfig::new()
        .with_encryption(EncryptionConfig::file_key(&key_path))
        .with_performance(disk_performance);
    let path = temp_dir.path().join("compressed-encrypted.redb");
    assert_eq!(
        engine_for_operational(&DbConfig::on_disk(Profile::Transactional, path), &disk_ops)?.kind(),
        EngineKind::CompressedEncryptedRedb
    );
    Ok(())
}

#[test]
fn engine_for_maps_profiles() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        engine_for(&DbConfig::new(Profile::InMemory))?.kind(),
        EngineKind::Memory
    );

    for profile in Profile::all()
        .into_iter()
        .filter(|profile| profile.is_on_disk())
    {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join(format!("{profile:?}.redb"));
        let engine = engine_for(&DbConfig::on_disk(profile, path))?;
        assert_eq!(engine.kind(), EngineKind::Redb);
    }

    Ok(())
}

#[test]
fn create_database_exposes_selected_engine() -> Result<(), Box<dyn std::error::Error>> {
    let memory = create_database(DbConfig::new(Profile::InMemory))?;
    assert_eq!(memory.engine_kind(), EngineKind::Memory);

    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("balanced.redb");
    let redb = create_database(DbConfig::on_disk(Profile::Balanced, path))?;
    assert_eq!(redb.engine_kind(), EngineKind::Redb);

    Ok(())
}

#[test]
fn self_healing_controller_is_only_available_for_cluster_backends()
-> Result<(), Box<dyn std::error::Error>> {
    let local = create_database(DbConfig::new(Profile::InMemory))?;
    assert!(matches!(
        local.self_healing_controller(HealthConfig::default(), HealingPolicy::default()),
        Err(DbError::Config(ConfigError::Unsupported(_)))
    ));

    let cp =
        create_simulated_cluster_database(DbConfig::new(Profile::InMemory), cp_cluster_config())?;
    let cp_controller =
        cp.self_healing_controller(HealthConfig::default(), HealingPolicy::default())?;
    assert_eq!(cp_controller.backend.local_node_id(), 1);

    let ap = create_ap_database(
        DbConfig::new(Profile::InMemory).with_replication(ReplicationKind::Ap),
        ap_cluster_config(),
    )?;
    let ap_controller = ap.self_healing_controller_with_probe(
        HealthConfig::default(),
        HealingPolicy::default(),
        ManualHealthProbe::new([(1, true)]),
    )?;
    assert_eq!(ap_controller.backend.local_node_id(), 1);

    Ok(())
}

#[test]
fn sharded_database_exposes_shard_map_and_rejects_snapshot_transactions()
-> Result<(), Box<dyn std::error::Error>> {
    let database = create_sharded_database(DbConfig::new(Profile::InMemory), sharded_config())?;

    assert_eq!(database.engine_kind(), EngineKind::Sharded);
    assert!(database.shard_map().is_some());
    assert!(matches!(
        database.begin_transaction(),
        Err(DbError::Config(ConfigError::Unsupported(_)))
    ));

    Ok(())
}

#[tokio::test]
async fn sharded_sql_matches_single_node_for_scatter_gather()
-> Result<(), Box<dyn std::error::Error>> {
    let mut sharded = create_sharded_database(DbConfig::new(Profile::InMemory), sharded_config())?;
    seed_orders(&mut sharded)?;

    let mut single = create_database(DbConfig::new(Profile::InMemory))?;
    seed_orders(&mut single)?;

    let aggregate =
        "select user_id, avg(amount), count(*) from orders group by user_id order by user_id";
    assert_eq!(
        sharded.query(aggregate).await?,
        single.query(aggregate).await?
    );

    let join = "select users.name, orders.amount \
                    from users join orders on users.id = orders.user_id \
                    order by users.name, orders.amount";
    assert_eq!(sharded.query(join).await?, single.query(join).await?);

    Ok(())
}

#[test]
fn sharded_database_supports_document_and_vector_models() -> Result<(), Box<dyn std::error::Error>>
{
    let mut database = create_sharded_database(DbConfig::new(Profile::InMemory), sharded_config())?;

    let profiles = database.create_collection(
        "profiles",
        CollectionId::new(501),
        user_doc_fields(),
        Vec::new(),
    )?;
    let doc_id = profiles.insert(&user_document(1, Some(Value::Str("Warsaw".to_owned()))))?;
    assert!(profiles.get(doc_id)?.is_some());
    drop(profiles);

    let vectors = database.create_vector_collection(
        "embeddings",
        VectorCollectionConfig::new(CollectionId::new(502), 3),
    )?;
    let vector_id = vectors.insert_vector(&Value::Str("Ada".to_owned()), vec![1.0, 0.0, 0.0])?;
    assert_eq!(vectors.knn(&[1.0, 0.0, 0.0], 1)?[0].id, vector_id);

    Ok(())
}

#[test]
fn profiles_select_expected_table_layouts() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(layout_for(Profile::Analytical), TableLayout::Columnar);
    for profile in Profile::all()
        .into_iter()
        .filter(|profile| *profile != Profile::Analytical)
    {
        assert_eq!(layout_for(profile), TableLayout::Row);
    }

    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("analytical.redb");
    let mut analytical = create_database(DbConfig::on_disk(Profile::Analytical, path))?;
    assert_eq!(analytical.layout(), TableLayout::Columnar);

    let sales = analytical.create_table("sales", Some(sales_schema()), Vec::new())?;
    assert_eq!(sales.layout(), TableLayout::Columnar);

    Ok(())
}

#[test]
fn open_database_requires_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("raw.redb");
    drop(RedbEngine::open(&path)?);

    assert!(matches!(
        open_database(DbConfig::on_disk(Profile::Transactional, path)),
        Err(ConfigError::MissingMetadata {
            key: "schema_version"
        })
    ));

    Ok(())
}

#[test]
fn metadata_persists_and_opens_with_same_config() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("transactional.redb");
    let config = DbConfig::on_disk(Profile::Transactional, path);

    {
        let database = create_database(config.clone())?;
        assert_eq!(database.profile(), Profile::Transactional);
        assert_eq!(database.replication_kind(), ReplicationKind::Cp);
    }

    let database = open_database(config)?;
    assert_eq!(database.profile(), Profile::Transactional);
    assert_eq!(database.replication_kind(), ReplicationKind::Cp);

    Ok(())
}

#[test]
fn analytical_layout_persists_and_reopens() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("analytical-layout.redb");
    let config = DbConfig::on_disk(Profile::Analytical, path);

    {
        let mut database = create_database(config.clone())?;
        let table = database.create_table("sales", Some(sales_schema()), Vec::new())?;
        table.insert(vec![
            Value::Int(1),
            Value::Str("a".to_owned()),
            Value::Float(10.0),
        ])?;
    }

    let database = open_database(config)?;
    assert_eq!(database.layout(), TableLayout::Columnar);
    assert_eq!(database.table("sales")?.layout(), TableLayout::Columnar);
    assert_eq!(
        database.table("sales")?.get(&Value::Int(1))?,
        Some(vec![
            Value::Int(1),
            Value::Str("a".to_owned()),
            Value::Float(10.0),
        ])
    );

    Ok(())
}

/// Gates per-table layout for provisioning: a columnar table inside a row-class database takes
/// SQL writes and reads and keeps its layout across a reopen. A handful of rows keeps the table
/// within one columnar segment.
#[tokio::test]
async fn create_table_with_layout_columnar_in_balanced_database_supports_sql_and_survives_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("balanced-columnar.redb");
    let config = DbConfig::on_disk(Profile::Balanced, &path);
    let expected = SqlOutput::Rows(SqlRows {
        columns: vec!["id".to_owned(), "category".to_owned(), "price".to_owned()],
        rows: vec![
            vec![
                Value::Int(1),
                Value::Str("a".to_owned()),
                Value::Float(10.0),
            ],
            vec![
                Value::Int(2),
                Value::Str("b".to_owned()),
                Value::Float(20.5),
            ],
        ],
    });

    {
        let mut database = create_database(config.clone())?;
        assert_eq!(database.layout(), TableLayout::Row);
        let rollups = database.create_table_with_layout(
            "rollups",
            Some(sales_schema()),
            Vec::new(),
            TableLayout::Columnar,
        )?;
        assert_eq!(rollups.layout(), TableLayout::Columnar);
        let orders = database.create_table("orders", Some(sales_schema()), Vec::new())?;
        assert_eq!(orders.layout(), TableLayout::Row);

        assert_eq!(
            database
                .query(
                    "INSERT INTO rollups (id, category, price) VALUES (1, 'a', 10.0), (2, 'b', 20.5)"
                )
                .await?,
            SqlOutput::AffectedRows(2)
        );
        database
            .query("INSERT INTO orders (id, category, price) VALUES (7, 'row', 1.0)")
            .await?;
        assert_eq!(
            database
                .query("SELECT id, category, price FROM rollups ORDER BY id")
                .await?,
            expected
        );
    }

    let database = open_database(config)?;
    assert_eq!(database.layout(), TableLayout::Row);
    assert!(matches!(
        database.catalog.get("rollups"),
        Some(super::CatalogEntry::Table {
            layout: TableLayout::Columnar,
            ..
        })
    ));
    assert!(matches!(
        database.catalog.get("orders"),
        Some(super::CatalogEntry::Table {
            layout: TableLayout::Row,
            ..
        })
    ));
    assert_eq!(database.table("rollups")?.layout(), TableLayout::Columnar);
    assert_eq!(
        database
            .query("SELECT id, category, price FROM rollups ORDER BY id")
            .await?,
        expected
    );
    assert_eq!(
        database.query("SELECT count(*) AS n FROM orders").await?,
        SqlOutput::Rows(SqlRows {
            columns: vec!["n".to_owned()],
            rows: vec![vec![Value::Int(1)]],
        })
    );
    Ok(())
}

#[test]
fn create_table_with_layout_refuses_columnar_without_schema_or_with_indexes()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;

    assert!(
        database
            .create_table_with_layout("loose", None, Vec::new(), TableLayout::Columnar)
            .is_err()
    );
    assert!(
        database
            .create_table_with_layout(
                "indexed",
                Some(sales_schema()),
                vec![RelIndexSpec::new(1, 1)],
                TableLayout::Columnar,
            )
            .is_err()
    );
    assert!(database.table("loose").is_err());
    assert!(database.table("indexed").is_err());
    Ok(())
}

#[test]
fn create_table_with_layout_as_requires_database_admin_and_audits()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("admin").grant(Resource::Database, Permission::Admin),
        Role::new("reader").grant(Resource::Database, Permission::Read),
    ]));
    let admin = Principal::new("root").with_role("admin");
    let reader = Principal::new("ada").with_role("reader");

    assert!(matches!(
        database.create_table_with_layout_as(
            &reader,
            "rollups",
            Some(sales_schema()),
            Vec::new(),
            TableLayout::Columnar,
        ),
        Err(DbError::AuthzDenied(_))
    ));
    assert!(database.table("rollups").is_err());

    let table = database.create_table_with_layout_as(
        &admin,
        "rollups",
        Some(sales_schema()),
        Vec::new(),
        TableLayout::Columnar,
    )?;
    assert_eq!(table.layout(), TableLayout::Columnar);

    let audit = database.audit_events()?;
    assert!(audit.iter().any(|event| {
        event.action == "create_table"
            && event.outcome == AuditOutcome::Denied
            && event.principal.as_deref() == Some("ada")
    }));
    assert!(audit.iter().any(|event| {
        event.action == "create_table"
            && event.outcome == AuditOutcome::Succeeded
            && event.resource == Resource::Table("rollups".to_owned())
            && event.detail.as_deref() == Some("layout: columnar")
    }));
    Ok(())
}

#[test]
fn is_valid_catalog_name_matches_validate_catalog_name() {
    let long = "a".repeat(128);
    let too_long = "a".repeat(129);
    for name in [
        "",
        "_",
        "__provision",
        "orders",
        "Orders_2",
        "orders_notes_fts",
        long.as_str(),
        too_long.as_str(),
        "1orders",
        "order-items",
        "order items",
        "zamówienia",
        "orders.notes",
    ] {
        assert_eq!(
            super::is_valid_catalog_name(name),
            super::validate_catalog_name(name).is_ok(),
            "{name}"
        );
    }
    assert!(super::is_valid_catalog_name(&long));
    assert!(!super::is_valid_catalog_name(&too_long));
    assert!(!super::is_valid_catalog_name(""));
}

#[test]
fn provision_state_reads_the_record_and_refuses_unreadable_ones()
-> Result<(), Box<dyn std::error::Error>> {
    let database = create_database(DbConfig::new(Profile::InMemory))?;
    assert_eq!(database.provision_state()?, None);

    let spec = DatabaseSpec::from_db_config("app", database.config());
    let record = json!({
        "record_version": 1,
        "status": "applied",
        "plan_id": "prov_0123",
        "spec_digest": "spec_0123",
        "applied_spec": spec,
        "profile_digest": "prof_0123",
        "applied_at_millis": 1,
        "applied_by": "local:test",
        "binary_version": "0.1.0"
    });
    let put = |value: &serde_json::Value| -> Result<(), Box<dyn std::error::Error>> {
        propose_system(
            &database,
            Op::Put {
                table: super::PROVISION_TABLE.to_owned(),
                key: b"record".to_vec(),
                value: serde_json::to_vec(value)?,
            },
        )?;
        Ok(())
    };
    put(&record)?;
    let state = database.provision_state()?.ok_or("a record")?;
    assert_eq!(state.plan_id, "prov_0123");
    assert_eq!(state.applied_spec, spec);
    assert!(state.objects.is_empty() && state.released_objects.is_empty());

    let mut newer = record;
    newer["record_version"] = json!(2);
    put(&newer)?;
    assert!(matches!(
        database.provision_state(),
        Err(DbError::Serde(message)) if message.contains("record_version 2")
    ));
    Ok(())
}

#[test]
fn document_count_counts_the_documents_of_one_collection() -> Result<(), Box<dyn std::error::Error>>
{
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_collection("users", CollectionId::new(1), user_doc_fields(), Vec::new())?;
    database.create_collection(
        "others",
        CollectionId::new(2),
        user_doc_fields(),
        vec![IndexSpec::new(IndexId::new(1), FieldPath::new(["name"]))],
    )?;
    assert_eq!(database.document_count(CollectionId::new(1))?, 0);
    for name in ["Ada", "Grace", "Linus"] {
        database.collection("users")?.insert(&Value::Object(
            [("name".to_owned(), Value::Str(name.to_owned()))].into(),
        ))?;
    }
    database.collection("others")?.insert(&Value::Object(
        [("name".to_owned(), Value::Str("Alan".to_owned()))].into(),
    ))?;
    assert_eq!(database.document_count(CollectionId::new(1))?, 3);
    assert_eq!(database.document_count(CollectionId::new(2))?, 1);
    assert_eq!(database.document_count(CollectionId::new(9))?, 0);
    Ok(())
}

#[test]
fn provisioning_views_match_the_file_and_refuse_replicated_backends()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("views.redb");
    let config = DbConfig::on_disk(Profile::Balanced, &path);
    let security = SecurityConfig {
        authz_policy: AuthzPolicy::new([
            Role::new("reader").grant(Resource::Collection("users".to_owned()), Permission::Read)
        ]),
        ..SecurityConfig::default()
    };
    drop(create_database_with_ops(
        config.clone(),
        OperationalConfig::new().with_security(security.clone()),
    )?);
    let MetadataProbe::Initialized(probe) = inspect_database_metadata(&path) else {
        return Err("the closed database is initialized".into());
    };

    // Plain openers reload the persisted RBAC state, so the handle enforces what is persisted.
    let plain = open_database(config.clone())?;
    assert_eq!(plain.probed_metadata()?, *probe);
    assert_eq!(plain.persisted_security_state()?, security);
    assert_eq!(*plain.authz_policy(), security.authz_policy);
    assert!(!plain.encryption_configured());
    assert!(!plain.audit_key_configured());
    assert_eq!(*plain.audit_config(), super::AuditConfig::default());
    drop(plain);

    let fresh = create_database(DbConfig::new(Profile::InMemory))?;
    assert_eq!(fresh.persisted_security_state()?, SecurityConfig::default());
    assert!(!fresh.probed_metadata()?.values_framed);

    // The public CP constructors fail closed, so only the in-crate simulated cluster has a CP
    // backend to refuse.
    assert!(matches!(
        create_cluster_database(DbConfig::new(Profile::InMemory), cp_cluster_config()),
        Err(ConfigError::Unsupported(_))
    ));
    let cp =
        create_simulated_cluster_database(DbConfig::new(Profile::InMemory), cp_cluster_config())?;
    assert!(matches!(
        cp.probed_metadata(),
        Err(DbError::Config(ConfigError::Unsupported(_)))
    ));
    let sharded = create_sharded_database(DbConfig::new(Profile::InMemory), sharded_config())?;
    assert!(matches!(
        sharded.probed_metadata(),
        Err(DbError::Config(ConfigError::Unsupported(_)))
    ));
    Ok(())
}

/// A provision record with the given plan id and nothing managed.
fn provision_record(plan_id: &str) -> ProvisionRecord {
    ProvisionRecord {
        record_version: RECORD_VERSION,
        status: ProvisionRecordStatus::Applied,
        plan_id: plan_id.to_owned(),
        spec_digest: "spec_test".to_owned(),
        applied_spec: DatabaseSpec::from_db_config("app", &DbConfig::new(Profile::InMemory)),
        last_applied_spec: None,
        profile_digest: "prof_test".to_owned(),
        objects: BTreeMap::new(),
        managed_roles: BTreeSet::new(),
        managed_principals: BTreeSet::new(),
        released_objects: BTreeSet::new(),
        guarantees: Vec::new(),
        completed_steps: Vec::new(),
        failed_step: None,
        flags: Vec::new(),
        seed_digest: None,
        applied_at_millis: 1,
        applied_by: "local:test".to_owned(),
        binary_version: "test".to_owned(),
    }
}

/// The commit-log records committed after `lsn`.
fn commit_records_after(
    database: &Database,
    lsn: u64,
) -> Result<Vec<txn::CommitLogRecord>, Box<dyn std::error::Error>> {
    Ok(database
        .range(txn::COMMIT_LOG_TABLE, &[], &[], ReadConsistency::Strong)?
        .into_iter()
        .map(|(_, value)| txn::decode_commit_log_record(&value))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|record| record.txn_id > lsn)
        .collect())
}

#[test]
fn provision_records_round_trip_and_history_keeps_the_newest_entries()
-> Result<(), Box<dyn std::error::Error>> {
    let database = create_database(DbConfig::new(Profile::InMemory))?;
    let history = database.provision_history()?;
    assert!(
        history.is_empty(),
        "expected an empty value, got {history:?}"
    );

    for at_millis in 1..=105_u64 {
        let record = provision_record(&format!("prov_{at_millis}"));
        let entry = ProvisionHistoryEntry {
            plan_id: record.plan_id.clone(),
            status: ProvisionRecordStatus::Applied,
            flags: vec!["allow_format_upgrade".to_owned()],
            actor: "local:test".to_owned(),
            at_millis,
        };
        database.write_provision_record(&record, Some(&entry))?;
    }
    // A record without a history entry leaves the history alone.
    let last = provision_record("prov_last");
    database.write_provision_record(&last, None)?;
    assert_eq!(database.provision_state()?, Some(last));

    let history = database.provision_history()?;
    assert_eq!(history.len(), super::provisioning::HISTORY_LIMIT);
    assert_eq!(history.first().map(|entry| entry.at_millis), Some(6));
    assert_eq!(history.last().map(|entry| entry.at_millis), Some(105));
    assert!(
        history
            .iter()
            .all(|entry| entry.flags == ["allow_format_upgrade"])
    );
    let keys = database
        .range(
            super::PROVISION_TABLE,
            b"history/",
            b"history0",
            ReadConsistency::Strong,
        )?
        .into_iter()
        .map(|(key, _)| String::from_utf8(key))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(keys.len(), 100);
    assert_eq!(
        keys.first().map(String::as_str),
        Some("history/00000000000000000006")
    );
    assert_eq!(
        keys.last().map(String::as_str),
        Some("history/00000000000000000105")
    );
    Ok(())
}

#[test]
fn provision_events_need_audit_and_local_operators_toggle_it()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let actor = ProvisionActor::local_operator("local:test");
    database.record_provision_event(
        &actor,
        "provision_apply_started",
        Resource::System,
        AuditOutcome::Succeeded,
        "plan_id: prov_test",
    )?;
    let events = database.audit_events()?;
    let started = events.last().ok_or("an event")?;
    assert_eq!(started.action, "provision_apply_started");
    assert_eq!(started.principal.as_deref(), Some("local:test"));

    database.set_audit_enabled_by_provisioning("local:test", false)?;
    assert!(!database.audit_enabled());
    let disabled = database.audit_events()?;
    let toggle = disabled.last().ok_or("the disable event")?;
    assert_eq!(toggle.action, "set_audit_enabled");
    assert_eq!(toggle.principal.as_deref(), Some("local:test"));
    assert_eq!(toggle.detail.as_deref(), Some("enabled: false"));
    assert!(!database.persisted_security_state()?.audit_enabled);
    assert!(matches!(
        database.record_provision_event(
            &actor,
            "provision_apply_started",
            Resource::System,
            AuditOutcome::Succeeded,
            "plan_id: prov_test",
        ),
        Err(DbError::AuditIntegrity(_))
    ));
    assert_eq!(database.audit_events()?.len(), disabled.len());

    database.set_audit_enabled_by_provisioning("local:test", true)?;
    assert!(database.audit_enabled());
    let enabled = database.audit_events()?;
    assert_eq!(
        enabled.last().and_then(|event| event.detail.as_deref()),
        Some("enabled: true")
    );
    database.verify_audit_chain()?;
    Ok(())
}

#[test]
fn local_operators_cannot_disable_audit_below_the_audit_floor()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("audit-floor.redb");
    let overrides: ProfileKnobOverrides = serde_json::from_value(json!({}))?;
    let resolution = resolve_named_profile("balanced", &overrides);
    let mut resolved = resolution.resolved().cloned().ok_or("balanced resolves")?;
    resolved.knobs.floors.audit = true;
    resolved.digest = crate::config_spec::profile_digest(&resolved.knobs, resolved.storage_profile);
    let binding = resolved.bind(ReplicationKind::Cp, Some(path))?;
    let mut database = create_database_with_profile(&binding, OperationalConfig::new())?;

    assert!(matches!(
        database.set_audit_enabled_by_provisioning("local:test", false),
        Err(DbError::Config(ConfigError::ProfileFloorNotMet {
            floor: "floors.audit",
            ..
        }))
    ));
    assert!(database.audit_enabled());
    let events = database.audit_events()?;
    let refused = events.last().ok_or("the refusal event")?;
    assert_eq!(refused.outcome, AuditOutcome::Failed);
    assert_eq!(refused.principal.as_deref(), Some("local:test"));
    Ok(())
}

#[test]
fn set_security_state_as_requires_system_admin_and_writes_one_batch()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database_with_ops(
        DbConfig::new(Profile::InMemory),
        OperationalConfig::new().with_security(SecurityConfig {
            authz_policy: AuthzPolicy::new([
                Role::new("admin").grant(Resource::System, Permission::Admin),
                Role::new("reader").grant(Resource::Database, Permission::Read),
            ]),
            ..SecurityConfig::default()
        }),
    )?;
    let admin = Principal::new("root").with_role("admin");
    let reader = Principal::new("ada").with_role("reader");
    let policy = AuthzPolicy::new([
        Role::new("admin").grant(Resource::System, Permission::Admin),
        Role::new("writer").grant(Resource::Collection("orders".to_owned()), Permission::Write),
    ]);
    let principals = PrincipalRegistry::new()
        .with_principal("svc", Principal::new("svc").with_role("writer"))
        .with_principal("root", admin.clone());

    assert!(matches!(
        database.set_security_state_as(&reader, policy.clone(), principals.clone()),
        Err(DbError::AuthzDenied(_))
    ));
    assert!(database.principal_registry().principals().is_empty());

    let before = latest_lsn(&database)?;
    database.set_security_state_as(&admin, policy.clone(), principals.clone())?;
    assert_eq!(database.authz_policy(), &policy);
    assert_eq!(database.principal_registry(), &principals);
    let persisted = database.persisted_security_state()?;
    assert_eq!(persisted.authz_policy, policy);
    assert_eq!(persisted.principals, principals);

    let security_records = commit_records_after(&database, before)?
        .into_iter()
        .filter(|record| record.writes.iter().any(|write| write.table == AUTHZ_TABLE))
        .collect::<Vec<_>>();
    assert_eq!(
        security_records.len(),
        1,
        "one batch for the policy and principals"
    );
    let keys = security_records[0]
        .writes
        .iter()
        .filter(|write| write.table == AUTHZ_TABLE)
        .map(|write| write.key.clone())
        .collect::<BTreeSet<_>>();
    assert!(keys.contains(b"policy".as_slice()) && keys.contains(b"principals".as_slice()));

    let events = database.audit_events()?;
    let changed = events.last().ok_or("an event")?;
    assert_eq!(changed.action, "set_security_state");
    assert_eq!(changed.principal.as_deref(), Some("root"));
    assert_eq!(changed.outcome, AuditOutcome::Succeeded);
    Ok(())
}

#[test]
fn auto_id_creates_as_need_database_admin_and_return_the_allocated_id()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("admin").grant(Resource::Database, Permission::Admin),
        Role::new("reader").grant(Resource::Database, Permission::Read),
    ]));
    let admin = Principal::new("root").with_role("admin");
    let reader = Principal::new("ada").with_role("reader");
    let fields = || vec![DocField::document_id("id")];

    assert!(matches!(
        database.create_collection_auto_id_as(&reader, "orders", fields(), Vec::new()),
        Err(DbError::AuthzDenied(_))
    ));
    assert!(matches!(
        database.create_vector_collection_auto_id_as(
            &reader,
            "memories",
            3,
            VectorMetric::Cosine,
            HnswParams::default(),
        ),
        Err(DbError::AuthzDenied(_))
    ));
    assert!(matches!(
        database.create_graph_auto_id_as(&reader, "social"),
        Err(DbError::AuthzDenied(_))
    ));
    assert!(database.catalog().is_empty());

    database.create_collection("pinned", CollectionId::new(7), fields(), Vec::new())?;
    assert_eq!(
        database.create_collection_auto_id_as(&admin, "orders", fields(), Vec::new())?,
        CollectionId::new(8)
    );
    assert_eq!(
        database.create_vector_collection_auto_id_as(
            &admin,
            "memories",
            3,
            VectorMetric::L2,
            HnswParams::default(),
        )?,
        CollectionId::new(9)
    );
    assert_eq!(
        database.create_graph_auto_id_as(&admin, "social")?,
        GraphId::new(1)
    );
    assert!(matches!(
        database.catalog().get("memories"),
        Some(super::CatalogEntry::Vector {
            dim: 3,
            metric: VectorMetric::L2,
            ..
        })
    ));

    let created = database
        .audit_events()?
        .into_iter()
        .filter(|event| event.outcome == AuditOutcome::Succeeded)
        .map(|event| (event.action, event.resource))
        .collect::<Vec<_>>();
    assert_eq!(
        created,
        vec![
            (
                "create_collection".to_owned(),
                Resource::Collection("orders".to_owned())
            ),
            (
                "create_vector_collection".to_owned(),
                Resource::VectorCollection("memories".to_owned())
            ),
            (
                "create_graph".to_owned(),
                Resource::Graph("social".to_owned())
            ),
        ]
    );
    Ok(())
}

#[test]
fn write_profile_manifest_validates_and_writes_manifest_and_version_together()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("upgrade.redb");
    let plain = named_binding("balanced", json!({}), ReplicationKind::Cp, Some(&path))?;
    let zstd = named_binding(
        "balanced",
        json!({ "compression": { "algorithm": "zstd" } }),
        ReplicationKind::Cp,
        Some(&path),
    )?;
    {
        let database = create_database_with_profile(&plain, OperationalConfig::new())?;
        assert!(matches!(
            database.write_profile_manifest(&plain.manifest, 2),
            Err(DbError::Config(ConfigError::ProfileManifestInvalid(_)))
        ));
        assert!(matches!(
            database.write_profile_manifest(&zstd.manifest, 1),
            Err(DbError::Config(ConfigError::ProfileManifestInvalid(_)))
        ));
        let mut tampered = zstd.manifest.clone();
        tampered.digest = "prof_tampered".to_owned();
        assert!(matches!(
            database.write_profile_manifest(&tampered, 2),
            Err(DbError::Config(ConfigError::ProfileManifestInvalid(_)))
        ));
        let mut floored = plain.manifest.clone();
        floored.knobs.floors.encryption_at_rest = true;
        floored.digest =
            crate::config_spec::profile_digest(&floored.knobs, floored.storage_profile);
        assert!(matches!(
            database.write_profile_manifest(&floored, 1),
            Err(DbError::Config(ConfigError::ProfileFloorNotMet {
                floor: "floors.encryption_at_rest",
                ..
            }))
        ));
        assert_eq!(database.probed_metadata()?.schema_version, 1);

        let before = latest_lsn(&database)?;
        database.write_profile_manifest(&zstd.manifest, 2)?;
        let records = commit_records_after(&database, before)?;
        assert_eq!(records.len(), 1);
        let keys = records[0]
            .writes
            .iter()
            .filter(|write| write.table == META_TABLE)
            .map(|write| write.key.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            keys,
            BTreeSet::from([KEY_PROFILE_MANIFEST.to_vec(), KEY_SCHEMA_VERSION.to_vec()])
        );
        let probed = database.probed_metadata()?;
        assert_eq!(probed.schema_version, 2);
        assert_eq!(probed.manifest.as_ref(), Some(&zstd.manifest));
        // The session keeps its engine: framing applies from the next open.
        assert_eq!(database.engine_kind(), EngineKind::Redb);
        assert!(matches!(
            database.write_profile_manifest(&plain.manifest, 1),
            Err(DbError::Config(ConfigError::StorageFormatMismatch(_)))
        ));
    }
    let database = open_database(DbConfig::on_disk(Profile::Balanced, &path))?;
    assert_eq!(database.engine_kind(), EngineKind::CompressedRedb);
    assert_eq!(database.performance_config(), &zstd.performance);
    Ok(())
}

#[test]
fn open_database_rejects_layout_mismatch() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("layout-mismatch.redb");
    let config = DbConfig::on_disk(Profile::Analytical, path.clone());
    create_database(config.clone())?;

    {
        let engine = RedbEngine::open(path)?;
        let mut txn = engine.begin_write()?;
        txn.put(
            META_TABLE,
            KEY_LAYOUT,
            &serde_json::to_vec(&TableLayout::Row)?,
        )?;
        txn.commit()?;
    }

    assert!(matches!(
        open_database(config),
        Err(ConfigError::LayoutMismatch {
            expected: TableLayout::Columnar,
            found: TableLayout::Row,
        })
    ));

    Ok(())
}

#[test]
fn open_database_rejects_profile_mismatch() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("profile-mismatch.redb");

    create_database(DbConfig::on_disk(Profile::Transactional, path.clone()))?;

    assert!(matches!(
        open_database(DbConfig::on_disk(Profile::Balanced, path)),
        Err(ConfigError::ProfileMismatch {
            expected: Profile::Balanced,
            found: Profile::Transactional
        })
    ));

    Ok(())
}

#[test]
fn open_database_rejects_replication_mismatch() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("replication-mismatch.redb");
    let config =
        DbConfig::on_disk(Profile::Document, path.clone()).with_replication(ReplicationKind::Ap);

    create_database(config)?;

    assert!(matches!(
        open_database(DbConfig::on_disk(Profile::Document, path)),
        Err(ConfigError::ReplicationMismatch {
            expected: ReplicationKind::Cp,
            found: ReplicationKind::Ap
        })
    ));

    Ok(())
}

#[test]
fn ap_replication_kind_round_trips_through_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("ap.redb");
    let config = DbConfig::on_disk(Profile::Vector, path).with_replication(ReplicationKind::Ap);

    create_database(config.clone())?;
    let database = open_database(config)?;

    assert_eq!(database.replication_kind(), ReplicationKind::Ap);

    Ok(())
}

#[test]
fn cluster_database_rejects_ap_until_phase_11b() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("ap-cluster.redb");
    let config = DbConfig::on_disk(Profile::Balanced, path).with_replication(ReplicationKind::Ap);

    assert!(matches!(
        create_cluster_database(config, cp_cluster_config()),
        Err(ConfigError::Unsupported(_))
    ));

    Ok(())
}

#[test]
fn cluster_database_rejects_even_voter_count() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("even-cluster.redb");
    let config = DbConfig::on_disk(Profile::Balanced, path);
    let cluster = CpClusterConfig::new(
        1,
        "127.0.0.1:7101",
        vec![
            RaftNode::new(1, "127.0.0.1:7101"),
            RaftNode::new(2, "127.0.0.1:7102"),
            RaftNode::new(3, "127.0.0.1:7103"),
            RaftNode::new(4, "127.0.0.1:7104"),
        ],
    )
    .with_transport(InternalTransportConfig::new(
        "127.0.0.1:7101",
        InternalTransportSecurity::PlaintextForTests,
    ));

    assert!(matches!(
        create_cluster_database(config, cluster),
        Err(ConfigError::InvalidClusterConfig(_))
    ));

    Ok(())
}

#[test]
fn simulated_cluster_database_persists_through_open() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("cp-cluster.redb");
    let config = DbConfig::on_disk(Profile::Balanced, path);

    {
        let database = create_simulated_cluster_database(config.clone(), cp_cluster_config())?;
        database.propose(Op::Put {
            table: "t".to_owned(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        })?;
    }

    {
        let database = open_simulated_cluster_database(config.clone(), cp_cluster_config())?;
        assert_eq!(
            database.read("t", b"k", ReadConsistency::Strong)?,
            Some(b"v".to_vec())
        );
    }

    let database = open_database(config)?;
    assert_eq!(
        database.read("t", b"k", ReadConsistency::Strong)?,
        Some(b"v".to_vec())
    );

    Ok(())
}

#[test]
fn cluster_database_constructors_fail_closed_without_live_raft()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("cp-cluster.redb");
    let config = DbConfig::on_disk(Profile::Balanced, &path);

    match create_cluster_database(config.clone(), cp_cluster_config()) {
        Err(ConfigError::Unsupported(message)) => {
            assert!(message.contains("start_cp_cluster"));
        }
        Err(error) => panic!("expected unsupported cluster database, got {error}"),
        Ok(_) => panic!("cluster database must fail closed without the live Raft runtime"),
    }
    assert!(!path.exists());
    assert!(matches!(
        open_cluster_database(config.clone(), cp_cluster_config()),
        Err(ConfigError::Unsupported(_))
    ));
    assert!(matches!(
        create_cluster_database_with_ops(
            config.clone(),
            cp_cluster_config(),
            OperationalConfig::new(),
        ),
        Err(ConfigError::Unsupported(_))
    ));
    assert!(matches!(
        open_cluster_database_with_ops(config, cp_cluster_config(), &OperationalConfig::new()),
        Err(ConfigError::Unsupported(_))
    ));
    assert!(!path.exists());

    Ok(())
}

#[test]
fn sharded_database_rejects_cp_shard_backends() {
    let sharding = || {
        ShardedDatabaseConfig::new(
            PartitionStrategy::HashSlots { partitions: 16 },
            1,
            vec![ShardSpec::new(
                1,
                ShardBackendConfig::Cp {
                    config: DbConfig::new(Profile::InMemory),
                    cluster: cp_cluster_config(),
                },
            )],
        )
    };

    assert!(matches!(
        create_sharded_database(DbConfig::new(Profile::InMemory), sharding()),
        Err(ConfigError::Unsupported(_))
    ));
    assert!(matches!(
        open_sharded_database(DbConfig::new(Profile::InMemory), sharding()),
        Err(ConfigError::Unsupported(_))
    ));
}

#[test]
fn ap_database_requires_ap_replication_kind() {
    assert!(matches!(
        create_ap_database(DbConfig::new(Profile::InMemory), ap_cluster_config()),
        Err(ConfigError::Unsupported(_))
    ));
}

#[test]
fn ap_database_persists_through_open() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("ap-cluster.redb");
    let config = DbConfig::on_disk(Profile::Balanced, path).with_replication(ReplicationKind::Ap);

    {
        let database = create_ap_database(config.clone(), ap_cluster_config())?;
        database.propose(Op::Put {
            table: "t".to_owned(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        })?;
    }

    let database = open_ap_database(config, ap_cluster_config())?;
    assert_eq!(
        database.read("t", b"k", ReadConsistency::Strong)?,
        Some(b"v".to_vec())
    );
    assert_eq!(database.read_conflict_versions("t", b"k")?.len(), 1);

    Ok(())
}

#[test]
fn cluster_openers_restore_persisted_security() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;

    let cp_config = DbConfig::on_disk(Profile::Balanced, temp_dir.path().join("cp-security.redb"));
    {
        let mut database =
            create_simulated_cluster_database(cp_config.clone(), cp_cluster_config())?;
        persist_reader_security(&mut database)?;
    }
    let database = open_simulated_cluster_database(cp_config, cp_cluster_config())?;
    assert_reader_security_restored(&database);
    drop(database);

    let ap_config = DbConfig::on_disk(Profile::Balanced, temp_dir.path().join("ap-security.redb"))
        .with_replication(ReplicationKind::Ap);
    {
        let mut database = create_ap_database(ap_config.clone(), ap_cluster_config())?;
        persist_reader_security(&mut database)?;
    }
    let database = open_ap_database(ap_config, ap_cluster_config())?;
    assert_reader_security_restored(&database);

    Ok(())
}

#[test]
fn sharded_openers_restore_persisted_security() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let config = DbConfig::on_disk(Profile::Balanced, temp_dir.path().join("router.redb"));

    {
        let mut database =
            create_sharded_database(config.clone(), on_disk_sharded_config(temp_dir.path()))?;
        persist_reader_security(&mut database)?;
    }
    let database = open_sharded_database(config, on_disk_sharded_config(temp_dir.path()))?;
    assert_reader_security_restored(&database);

    Ok(())
}

#[test]
fn ap_database_rejects_snapshot_transactions() -> Result<(), Box<dyn std::error::Error>> {
    let database = create_ap_database(
        DbConfig::new(Profile::InMemory).with_replication(ReplicationKind::Ap),
        ap_cluster_config(),
    )?;

    assert!(matches!(
        database.begin_transaction(),
        Err(DbError::Config(ConfigError::Unsupported(_)))
    ));

    Ok(())
}

#[test]
fn ap_database_rejects_changefeeds_subscriptions_and_continuous_queries()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = ap_database()?;
    let accounts = database.create_table("accounts", Some(account_schema()), Vec::new())?;
    accounts.insert(vec![
        Value::Int(1),
        Value::Str("Ada".to_owned()),
        Value::Int(37),
    ])?;

    assert!(matches!(
        database.poll_changefeed(
            &ResumeToken::default(),
            &ChangefeedFilter::default(),
            &ChangefeedOptions::default(),
            10,
        ),
        Err(DbError::Config(ConfigError::Unsupported(_)))
    ));
    assert!(matches!(
        database.poll_changefeed_page(
            &ResumeToken::default(),
            &ChangefeedFilter::default(),
            &ChangefeedOptions::default(),
            10,
        ),
        Err(DbError::Config(ConfigError::Unsupported(_)))
    ));

    assert!(matches!(
        database.create_subscription(SubscriptionConfig::new("feed", ResumeToken::default())),
        Err(DbError::Config(ConfigError::Unsupported(_)))
    ));
    assert!(matches!(
        database.create_subscription_as(
            &Principal::new("alice"),
            SubscriptionConfig::new("feed", ResumeToken::default()),
        ),
        Err(DbError::Config(ConfigError::Unsupported(_)))
    ));
    assert!(database.subscription_state("feed").is_err());

    let spec = ContinuousQuerySpec {
        name: "cq_accounts".to_owned(),
        sql: "SELECT * FROM accounts".to_owned(),
        filter: ChangefeedFilter {
            target: ChangefeedTarget::Table("accounts".to_owned()),
        },
        start: ResumeToken::default(),
        buffer_limit: 8,
    };
    assert!(matches!(
        database.create_continuous_query(&spec),
        Err(DbError::Config(ConfigError::Unsupported(_)))
    ));
    assert!(
        database
            .ack_continuous_query(&spec.name, ResumeToken::default())
            .is_err()
    );

    Ok(())
}

#[test]
fn ap_database_rejects_materialized_views_hooks_and_system_versioning()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = ap_database()?;
    database.create_table("accounts", Some(account_schema()), Vec::new())?;
    let spec = MaterializedViewSpec {
        name: "accounts_by_name".to_owned(),
        source_table: "accounts".to_owned(),
        filter: None,
        group_by: 1,
        aggregates: vec![AggregateSpec {
            output_name: "count".to_owned(),
            kind: AggregateKind::Count,
            column: None,
        }],
    };

    assert!(matches!(
        database.create_materialized_view(&spec),
        Err(DbError::MaterializedView(
            MaterializedViewError::Unsupported(_)
        ))
    ));
    assert!(matches!(
        database.create_materialized_view_object(&spec),
        Err(DbError::MaterializedView(
            MaterializedViewError::Unsupported(_)
        ))
    ));
    assert!(matches!(
        database.refresh_materialized_view_to_current(&spec.name),
        Err(DbError::MaterializedView(
            MaterializedViewError::Unsupported(_)
        ))
    ));
    assert!(!database.catalog().contains_key(&spec.name));

    assert!(matches!(
        database.register_hook(&HookSpec::after(
            "record_after",
            HookTarget::AnyUser,
            HookAction::RecordAfterCommit,
            1_000,
        )),
        Err(DbError::Config(ConfigError::Unsupported(_)))
    ));
    database.register_hook(&HookSpec::before(
        "reject_empty",
        HookTarget::AnyUser,
        HookAction::RejectEmptyValue,
        1_000,
    ))?;

    assert!(matches!(
        database.enable_system_versioning(
            "accounts",
            TemporalRetention {
                min_lsn: 0,
                keep_history: true,
            },
        ),
        Err(DbError::Temporal(TemporalError::Unsupported(_)))
    ));
    assert!(!database.catalog().contains_key("accounts_history"));

    Ok(())
}

#[test]
fn ap_database_rejects_incremental_full_text_and_geo_refresh()
-> Result<(), Box<dyn std::error::Error>> {
    let mut ap = ap_database()?;
    insert_located_post(&mut ap)?;
    let catalog = ap.catalog().clone();
    let text = ap.create_full_text_index(posts_text_index())?;
    assert!(matches!(
        text.refresh_incremental(&catalog),
        Err(TextError::Repl(ReplError::Unsupported(_)))
    ));
    let geo = ap.create_geo_index(posts_geo_index())?;
    assert!(matches!(
        geo.refresh_incremental(&catalog),
        Err(GeoError::Repl(ReplError::Unsupported(_)))
    ));
    assert!(matches!(
        ap.full_text_index("posts_text")?
            .refresh_incremental(ap.catalog()),
        Err(TextError::Repl(ReplError::Unsupported(_)))
    ));
    assert!(matches!(
        ap.geo_index("posts_geo")?.refresh_incremental(ap.catalog()),
        Err(GeoError::Repl(ReplError::Unsupported(_)))
    ));
    assert_eq!(
        ap.full_text_index("posts_text")?
            .refresh_full()?
            .indexed_documents,
        1
    );
    assert_eq!(ap.geo_index("posts_geo")?.refresh_full()?, 1);

    let mut local = create_database(DbConfig::new(Profile::InMemory))?;
    insert_located_post(&mut local)?;
    local.create_full_text_index(posts_text_index())?;
    local.create_geo_index(posts_geo_index())?;
    assert_eq!(
        local
            .full_text_index("posts_text")?
            .refresh_incremental(local.catalog())?
            .indexed_documents,
        1
    );
    local
        .geo_index("posts_geo")?
        .refresh_incremental(local.catalog())?;
    assert_eq!(
        local
            .geo_index("posts_geo")?
            .within_radius(GeoPoint::new(21.0122, 52.2297)?, 10.0)?
            .len(),
        1
    );

    Ok(())
}

#[test]
fn models_work_on_ap_database_without_conflicts() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_ap_database(
        DbConfig::new(Profile::InMemory).with_replication(ReplicationKind::Ap),
        ap_cluster_config(),
    )?;

    let accounts = database.create_table("accounts", Some(account_schema()), Vec::new())?;
    accounts.insert(vec![
        Value::Int(1),
        Value::Str("Ada".to_owned()),
        Value::Int(37),
    ])?;
    assert_eq!(
        accounts.get(&Value::Int(1))?,
        Some(vec![
            Value::Int(1),
            Value::Str("Ada".to_owned()),
            Value::Int(37),
        ])
    );

    let profiles = database.create_collection(
        "profiles",
        CollectionId::new(91),
        user_doc_fields(),
        Vec::new(),
    )?;
    let doc_id = profiles.insert(&user_document(1, Some(Value::Str("Warsaw".to_owned()))))?;
    assert!(profiles.get(doc_id)?.is_some());

    let vectors = database.create_vector_collection(
        "embeddings",
        VectorCollectionConfig::new(CollectionId::new(92), 3),
    )?;
    let vector_id = vectors.insert_vector(&Value::Str("Ada".to_owned()), vec![1.0, 0.0, 0.0])?;
    assert_eq!(vectors.knn(&[1.0, 0.0, 0.0], 1)?[0].id, vector_id);

    Ok(())
}

#[test]
fn database_delegates_reads_and_writes_to_replication() -> Result<(), Box<dyn std::error::Error>> {
    let database = create_database(DbConfig::new(Profile::InMemory))?;

    database.propose(Op::Put {
        table: "t".to_owned(),
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    })?;

    assert_eq!(
        database.read("t", b"k", ReadConsistency::Strong)?,
        Some(b"v".to_vec())
    );

    database.propose(Op::Delete {
        table: "t".to_owned(),
        key: b"k".to_vec(),
    })?;

    assert_eq!(database.read("t", b"k", ReadConsistency::Strong)?, None);

    Ok(())
}

#[test]
fn catalog_tracks_tables_and_collections() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_table("orders", Some(order_schema()), Vec::new())?;
    database.create_collection("users", CollectionId::new(1), user_doc_fields(), Vec::new())?;

    assert!(database.table("orders").is_ok());
    assert!(database.collection("users").is_ok());

    assert!(matches!(
        database.create_collection(
            "orders",
            CollectionId::new(2),
            user_doc_fields(),
            Vec::new()
        ),
        Err(DbError::CatalogObjectExists(name)) if name == "orders"
    ));

    Ok(())
}

#[test]
fn catalog_tracks_vector_collections() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let config = VectorCollectionConfig::new(CollectionId::new(42), 3)
        .with_metric(VectorMetric::L2)
        .with_hnsw(HnswParams::new(8, 16, 16));

    let vectors = database.create_vector_collection("embeddings", config.clone())?;
    assert_eq!(vectors.config(), &config);
    drop(vectors);

    assert!(database.vector_collection("embeddings").is_ok());
    assert!(matches!(
        database.create_table("embeddings", Some(order_schema()), Vec::new()),
        Err(DbError::CatalogObjectExists(name)) if name == "embeddings"
    ));

    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn phase19_models_are_cataloged_queryable_and_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("phase19.redb");
    {
        let mut database = create_database(DbConfig::on_disk(Profile::Balanced, &path))?;
        let posts = database.create_collection(
            "posts",
            CollectionId::new(190),
            vec![DocField::path(
                "body",
                FieldPath::new(["body"]),
                ColumnType::Str,
            )],
            Vec::new(),
        )?;
        let post_id = posts.insert(&Value::Object(BTreeMap::from([
            (
                "body".to_owned(),
                Value::Str("rust database search storage".to_owned()),
            ),
            (
                "point".to_owned(),
                Value::GeoPoint {
                    lon: 21.0122,
                    lat: 52.2297,
                },
            ),
        ])))?;

        let text = database.create_full_text_index(FullTextIndexConfig::collection(
            "posts_text",
            CollectionId::new(190),
            FieldPath::new(["body"]),
        ))?;
        assert_eq!(text.search("rust", 5)?.len(), 1);
        assert_eq!(text.refresh_full()?.indexed_documents, 1);

        let geo = database.create_geo_index(GeoIndexConfig::new(
            "posts_geo",
            CollectionId::new(190),
            FieldPath::new(["point"]),
        ))?;
        assert_eq!(geo.refresh_full()?, 1);

        let series = database
            .create_time_series(TimeSeriesConfig::new("metrics").with_chunk_millis(1_000))?;
        series.insert_point(
            "cpu",
            TimePoint {
                timestamp_millis: 1_500,
                value: 42.0,
            },
        )?;

        let graph = database.create_graph("social", GraphId::new(77))?;
        graph.add_edge(
            GraphNodeId::from_str_id("alice"),
            "knows",
            GraphNodeId::from_str_id("bob"),
            &Value::Null,
        )?;

        let SqlOutput::Rows(rows) = database
            .query("SELECT * FROM match('posts_text', 'rust database', 5)")
            .await?
        else {
            panic!("match should return rows");
        };
        assert_eq!(rows.rows[0][0], Value::Bytes(post_id.as_bytes().to_vec()));

        let SqlOutput::Rows(rows) = database.query("SELECT time_bucket(1000, 1500)").await? else {
            panic!("time_bucket should return rows");
        };
        assert_eq!(rows.rows, vec![vec![Value::Int(1_000)]]);

        let SqlOutput::Rows(rows) = database
            .query("SELECT * FROM graph_neighbors('social', 'alice', 'knows', 1)")
            .await?
        else {
            panic!("graph_neighbors should return rows");
        };
        assert_eq!(rows.rows, vec![vec![Value::Str("bob".to_owned())]]);

        let SqlOutput::Rows(rows) = database
            .query("SELECT * FROM within_radius('posts_geo', 21.0122, 52.2297, 10)")
            .await?
        else {
            panic!("within_radius should return rows");
        };
        assert_eq!(rows.rows.len(), 1);
    }

    let database = open_database(DbConfig::on_disk(Profile::Balanced, &path))?;
    assert_eq!(
        database
            .full_text_index("posts_text")?
            .search("search", 5)?
            .len(),
        1
    );
    assert_eq!(
        database.time_series("metrics")?.latest("cpu")?,
        Some(TimePoint {
            timestamp_millis: 1_500,
            value: 42.0,
        })
    );
    assert_eq!(
        database.graph("social")?.k_hop(
            &GraphNodeId::from_str_id("alice"),
            "knows",
            TraversalOptions {
                max_depth: 1,
                max_expansion: 10,
            },
        )?,
        vec![GraphNodeId::from_str_id("bob")]
    );
    assert_eq!(
        database
            .geo_index("posts_geo")?
            .within_radius(GeoPoint::new(21.0122, 52.2297)?, 10.0)?
            .len(),
        1
    );
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn full_text_and_geo_indexes_follow_document_writes_without_refresh()
-> Result<(), Box<dyn std::error::Error>> {
    const RUST: &str = "SELECT * FROM match('places_text', 'rust', 10)";
    const ZIG: &str = "SELECT * FROM match('places_text', 'zig', 10)";
    const NEAR_WARSAW: &str = "SELECT * FROM within_radius('places_geo', 21.0122, 52.2297, 1000)";
    const NEAR_PARIS: &str = "SELECT * FROM within_radius('places_geo', 2.3522, 48.8566, 1000)";

    let temp = tempfile::tempdir()?;
    let path = temp.path().join("derived_indexes.redb");
    let warsaw = Value::GeoPoint {
        lon: 21.0122,
        lat: 52.2297,
    };
    let (p, r) = {
        let mut database = create_database(DbConfig::on_disk(Profile::Balanced, &path))?;
        database.create_collection(
            "places",
            CollectionId::new(191),
            vec![DocField::path(
                "body",
                FieldPath::new(["body"]),
                ColumnType::Str,
            )],
            Vec::new(),
        )?;
        let p = database
            .collection("places")?
            .insert(&place("rust database", warsaw.clone()))?;
        database.create_full_text_index(FullTextIndexConfig::collection(
            "places_text",
            CollectionId::new(191),
            FieldPath::new(["body"]),
        ))?;
        database.create_geo_index(GeoIndexConfig::new(
            "places_geo",
            CollectionId::new(191),
            FieldPath::new(["location"]),
        ))?;

        let q = database.collection("places")?.insert(&place(
            "rust tokio",
            lon_lat(Value::Float(-0.1276), Value::Float(51.5072)),
        ))?;
        let r = database.transaction(|txn| {
            txn.insert_document(
                "places",
                &place("go tooling", lon_lat(Value::Int(21), Value::Int(52))),
            )
        })?;
        database.collection("places")?.update(
            p,
            &place(
                "zig allocator",
                Value::GeoPoint {
                    lon: 2.3522,
                    lat: 48.8566,
                },
            ),
        )?;
        database.transaction(|txn| {
            txn.update_document("places", r, &place("rust compiler", warsaw.clone()))
        })?;
        database.transaction(|txn| txn.delete_document("places", q))?;

        assert_eq!(result_ids(&database, RUST).await?, ids([r]));
        assert_eq!(result_ids(&database, ZIG).await?, ids([p]));
        assert_eq!(result_ids(&database, NEAR_WARSAW).await?, ids([r]));
        assert_eq!(result_ids(&database, NEAR_PARIS).await?, ids([p]));

        let text = database.full_text_index("places_text")?;
        let rust_hits = text.search("rust", 10)?;
        assert_eq!(
            rust_hits.iter().map(|hit| hit.id).collect::<Vec<_>>(),
            vec![r]
        );
        assert!(
            text.search("tokio", 10)?.is_empty(),
            "expected an empty value, got {:?}",
            text.search("tokio", 10)?
        );
        assert!(
            text.search("go", 10)?.is_empty(),
            "expected an empty value, got {:?}",
            text.search("go", 10)?
        );
        let geo = database.geo_index("places_geo")?;
        let near_warsaw = geo.within_radius(GeoPoint::new(21.0122, 52.2297)?, 1_000.0)?;
        assert_eq!(
            near_warsaw.iter().map(|hit| hit.id).collect::<Vec<_>>(),
            vec![r]
        );
        assert!(
            geo.within_radius(GeoPoint::new(-0.1276, 51.5072)?, 1_000.0)?
                .is_empty(),
            "expected an empty value, got {:?}",
            geo.within_radius(GeoPoint::new(-0.1276, 51.5072)?, 1_000.0)?
        );
        (p, r)
    };

    let database = open_database(DbConfig::on_disk(Profile::Balanced, &path))?;
    assert_eq!(result_ids(&database, ZIG).await?, ids([p]));
    assert_eq!(result_ids(&database, NEAR_WARSAW).await?, ids([r]));

    // Writes after reopen keep the indexes current, so maintenance reattaches from the catalog.
    database
        .collection("places")?
        .update(p, &place("rust kernel", warsaw))?;
    assert_eq!(result_ids(&database, RUST).await?, ids([p, r]));
    assert!(result_ids(&database, ZIG).await?.is_empty());
    assert!(result_ids(&database, NEAR_PARIS).await?.is_empty());
    assert_eq!(result_ids(&database, NEAR_WARSAW).await?, ids([p, r]));
    Ok(())
}

#[test]
fn full_text_and_geo_index_creation_requires_document_collection()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_vector_collection(
        "embeddings",
        VectorCollectionConfig::new(CollectionId::new(50), 3),
    )?;

    for (collection_id, expect_missing) in [
        (CollectionId::new(999), true),
        (CollectionId::new(50), false),
    ] {
        let text = database
            .create_full_text_index(FullTextIndexConfig::collection(
                "orphan_text",
                collection_id,
                FieldPath::new(["body"]),
            ))
            .err();
        assert!(
            is_index_source_error(text.as_ref(), expect_missing),
            "{text:?}"
        );
        let geo = database
            .create_geo_index(GeoIndexConfig::new(
                "orphan_geo",
                collection_id,
                FieldPath::new(["point"]),
            ))
            .err();
        assert!(
            is_index_source_error(geo.as_ref(), expect_missing),
            "{geo:?}"
        );
        assert!(!database.catalog().contains_key("orphan_text"));
        assert!(!database.catalog().contains_key("orphan_geo"));
    }
    Ok(())
}

fn is_index_source_error(error: Option<&DbError>, expect_missing: bool) -> bool {
    match error {
        Some(DbError::MissingCatalogObject(_)) => expect_missing,
        Some(DbError::CatalogKindMismatch { .. }) => !expect_missing,
        _ => false,
    }
}

async fn result_ids(
    database: &Database,
    sql: &str,
) -> Result<BTreeSet<DocumentId>, Box<dyn std::error::Error>> {
    let SqlOutput::Rows(rows) = database.query(sql).await? else {
        return Err(format!("{sql} should return rows").into());
    };
    let mut ids = BTreeSet::new();
    for row in rows.rows {
        let Some(Value::Bytes(bytes)) = row.first() else {
            return Err(format!("{sql} returned a row without an id").into());
        };
        let bytes: [u8; 16] = bytes.as_slice().try_into()?;
        ids.insert(DocumentId::from_bytes(bytes));
    }
    Ok(ids)
}

fn ids<const N: usize>(ids: [DocumentId; N]) -> BTreeSet<DocumentId> {
    BTreeSet::from(ids)
}

#[tokio::test]
async fn index_search_requires_source_collection_read_and_respects_its_policies()
-> Result<(), Box<dyn std::error::Error>> {
    const SEARCH: &str = "SELECT * FROM match('notes_text', 'rust', 10)";
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_collection(
        "notes",
        CollectionId::new(301),
        vec![DocField::path(
            "body",
            FieldPath::new(["body"]),
            ColumnType::Str,
        )],
        Vec::new(),
    )?;
    database.collection("notes")?.insert(&place(
        "rust secret",
        lon_lat(Value::Float(21.0), Value::Float(52.0)),
    ))?;
    database.create_full_text_index(FullTextIndexConfig::collection(
        "notes_text",
        CollectionId::new(301),
        FieldPath::new(["body"]),
    ))?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("indexer").grant(
            Resource::FullTextIndex("notes_text".to_owned()),
            Permission::Read,
        ),
        Role::new("reader")
            .grant(
                Resource::FullTextIndex("notes_text".to_owned()),
                Permission::Read,
            )
            .grant(Resource::Collection("notes".to_owned()), Permission::Read),
    ]));
    let indexer = Principal::new("ivan").with_role("indexer");
    let reader = Principal::new("alice").with_role("reader");

    assert!(matches!(
        database.query_as(&indexer, SEARCH).await,
        Err(DbError::AuthzDenied(_))
    ));
    let SqlOutput::Rows(rows) = database.query_as(&reader, SEARCH).await? else {
        return Err("expected rows".into());
    };
    assert_eq!(rows.rows.len(), 1);

    database.set_policy_config(&PolicyConfig {
        validations: Vec::new(),
        masking: vec![MaskingPolicy {
            resource: Resource::Collection("notes".to_owned()),
            paths: vec![FieldPath::new(["body"])],
            replacement: Value::Str("***".to_owned()),
        }],
        row_policies: Vec::new(),
        limits: LimitPolicy::default(),
    })?;
    assert!(matches!(
        database.query_as(&reader, SEARCH).await,
        Err(DbError::Extension(ExtensionError::PolicyDenied(_)))
    ));
    Ok(())
}

fn place(body: &str, location: Value) -> Value {
    Value::Object(BTreeMap::from([
        ("body".to_owned(), Value::Str(body.to_owned())),
        ("location".to_owned(), location),
    ]))
}

fn lon_lat(lon: Value, lat: Value) -> Value {
    Value::Object(BTreeMap::from([
        ("lon".to_owned(), lon),
        ("lat".to_owned(), lat),
    ]))
}

#[test]
fn catalog_survives_redb_reopen() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("catalog.redb");
    let config = DbConfig::on_disk(Profile::Balanced, path);

    {
        let mut database = create_database(config.clone())?;
        database.create_table(
            "orders",
            Some(order_schema()),
            vec![RelIndexSpec::new(1, 1)],
        )?;
        database.create_collection(
            "users",
            CollectionId::new(7),
            user_doc_fields(),
            vec![IndexSpec::new(IndexId::new(1), city_path())],
        )?;
        database.create_vector_collection(
            "embeddings",
            VectorCollectionConfig::new(CollectionId::new(70), 3),
        )?;
    }

    let database = open_database(config)?;
    assert_eq!(database.table("orders")?.schema(), Some(&order_schema()));
    assert_eq!(
        database.collection("users")?.collection_id(),
        CollectionId::new(7)
    );
    assert_eq!(
        database.vector_collection("embeddings")?.collection_id(),
        CollectionId::new(70)
    );

    Ok(())
}

#[tokio::test]
async fn cross_model_join_works_on_in_memory() -> Result<(), Box<dyn std::error::Error>> {
    run_cross_model_join(create_database(DbConfig::new(Profile::InMemory))?).await
}

#[tokio::test]
async fn cross_model_join_works_on_redb() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("cross-model.redb");
    run_cross_model_join(create_database(DbConfig::on_disk(
        Profile::Transactional,
        path,
    ))?)
    .await
}

#[tokio::test]
async fn document_provider_turns_missing_or_bad_fields_into_null()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let collection =
        database.create_collection("users", CollectionId::new(9), user_doc_fields(), Vec::new())?;

    collection.insert(&user_document(1, Some(Value::Str("Warsaw".to_owned()))))?;
    collection.insert(&user_document(2, None))?;
    collection.insert(&user_document(3, Some(Value::Int(123))))?;

    let output = database
        .query("select user_id, city from users order by user_id")
        .await?;

    assert_eq!(
        output,
        SqlOutput::Rows(SqlRows {
            columns: vec!["user_id".to_owned(), "city".to_owned()],
            rows: vec![
                vec![Value::Int(1), Value::Str("Warsaw".to_owned())],
                vec![Value::Int(2), Value::Null],
                vec![Value::Int(3), Value::Null],
            ],
        })
    );

    Ok(())
}

#[test]
fn transaction_commits_and_rolls_back_across_models() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_table(
        "accounts",
        Some(account_schema()),
        vec![RelIndexSpec::new(1, 2)],
    )?;
    database.create_collection(
        "profiles",
        CollectionId::new(10),
        user_doc_fields(),
        vec![IndexSpec::new(IndexId::new(1), city_path())],
    )?;

    let doc_id = database.transaction(|txn| {
        txn.insert_row(
            "accounts",
            vec![Value::Int(1), Value::Str("Ada".to_owned()), Value::Int(37)],
        )?;
        txn.insert_document(
            "profiles",
            &user_document(1, Some(Value::Str("Warsaw".to_owned()))),
        )
    })?;

    assert!(database.table("accounts")?.get(&Value::Int(1))?.is_some());
    assert!(database.collection("profiles")?.get(doc_id)?.is_some());
    assert_eq!(
        database
            .table("accounts")?
            .query_eq(2, &Value::Int(37))?
            .plan,
        RelPlanKind::IndexScan(1)
    );
    assert_eq!(
        database
            .find(
                "profiles",
                &Predicate::Eq {
                    path: city_path(),
                    value: Value::Str("Warsaw".to_owned()),
                },
            )?
            .plan,
        PlanKind::IndexScan(IndexId::new(1))
    );

    let result: Result<(), DbError> = database.transaction(|txn| {
        txn.insert_row(
            "accounts",
            vec![
                Value::Int(2),
                Value::Str("Grace".to_owned()),
                Value::Int(85),
            ],
        )?;
        Err(DbError::TransactionAborted("stop".to_owned()))
    });

    assert!(matches!(result, Err(DbError::TransactionAborted(_))));
    assert_eq!(database.table("accounts")?.get(&Value::Int(2))?, None);

    Ok(())
}

#[test]
fn transaction_keeps_snapshot_after_concurrent_commit() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let table = database.create_table("accounts", Some(account_schema()), Vec::new())?;
    table.insert(vec![
        Value::Int(1),
        Value::Str("Ada".to_owned()),
        Value::Int(37),
    ])?;

    let txn = database.begin_transaction()?;
    table.update(vec![
        Value::Int(1),
        Value::Str("Ada".to_owned()),
        Value::Int(38),
    ])?;

    assert_eq!(
        txn.get_row("accounts", &Value::Int(1))?,
        Some(vec![
            Value::Int(1),
            Value::Str("Ada".to_owned()),
            Value::Int(37),
        ])
    );

    Ok(())
}

#[test]
fn transaction_snapshot_reads_base_rows_on_disk() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("snapshot.redb");
    let mut database = create_database(DbConfig::on_disk(Profile::Balanced, path))?;
    let table = database.create_table("accounts", Some(account_schema()), Vec::new())?;
    table.insert(vec![
        Value::Int(1),
        Value::Str("Ada".to_owned()),
        Value::Int(37),
    ])?;
    table.insert(vec![
        Value::Int(2),
        Value::Str("Grace".to_owned()),
        Value::Int(85),
    ])?;

    let mut txn = database.begin_transaction()?;
    table.update(vec![
        Value::Int(1),
        Value::Str("Ada".to_owned()),
        Value::Int(38),
    ])?;
    table.delete(&Value::Int(2))?;
    table.insert(vec![
        Value::Int(3),
        Value::Str("Linus".to_owned()),
        Value::Int(55),
    ])?;

    assert_eq!(
        txn.get_row("accounts", &Value::Int(1))?
            .and_then(|row| row.get(2).cloned()),
        Some(Value::Int(37))
    );
    assert!(txn.get_row("accounts", &Value::Int(2))?.is_some());
    assert_eq!(txn.get_row("accounts", &Value::Int(3))?, None);

    txn.insert_row(
        "accounts",
        vec![
            Value::Int(3),
            Value::Str("Linus".to_owned()),
            Value::Int(56),
        ],
    )?;
    let error = txn.commit().err().ok_or("stale insert committed")?;
    assert!(error.is_conflict(), "unexpected error: {error}");

    Ok(())
}

#[test]
fn read_committed_transaction_observes_new_commits() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let table = database.create_table("accounts", Some(account_schema()), Vec::new())?;
    table.insert(vec![
        Value::Int(1),
        Value::Str("Ada".to_owned()),
        Value::Int(37),
    ])?;

    let txn = database.begin_transaction_with_options(txn::TxnOptions {
        isolation: txn::IsolationLevel::ReadCommitted,
        ..txn::TxnOptions::default()
    })?;
    assert_eq!(
        txn.get_row("accounts", &Value::Int(1))?
            .and_then(|row| row.get(2).cloned()),
        Some(Value::Int(37))
    );

    table.update(vec![
        Value::Int(1),
        Value::Str("Ada".to_owned()),
        Value::Int(38),
    ])?;

    assert_eq!(
        txn.get_row("accounts", &Value::Int(1))?
            .and_then(|row| row.get(2).cloned()),
        Some(Value::Int(38))
    );

    Ok(())
}

#[test]
fn transaction_reads_own_writes_before_commit() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_table("accounts", Some(account_schema()), Vec::new())?;

    let mut txn = database.begin_transaction()?;
    txn.insert_row(
        "accounts",
        vec![Value::Int(1), Value::Str("Ada".to_owned()), Value::Int(37)],
    )?;

    assert_eq!(
        txn.get_row("accounts", &Value::Int(1))?,
        Some(vec![
            Value::Int(1),
            Value::Str("Ada".to_owned()),
            Value::Int(37),
        ])
    );
    txn.commit()?;

    assert!(database.table("accounts")?.get(&Value::Int(1))?.is_some());

    Ok(())
}

#[test]
fn transaction_savepoint_rolls_back_staged_writes() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_table("accounts", Some(account_schema()), Vec::new())?;

    let mut txn = database.begin_transaction()?;
    txn.insert_row(
        "accounts",
        vec![Value::Int(1), Value::Str("Ada".to_owned()), Value::Int(37)],
    )?;
    txn.savepoint("after-ada");
    txn.insert_row(
        "accounts",
        vec![
            Value::Int(2),
            Value::Str("Grace".to_owned()),
            Value::Int(85),
        ],
    )?;
    assert!(txn.get_row("accounts", &Value::Int(2))?.is_some());

    txn.rollback_to_savepoint("after-ada")?;
    assert_eq!(txn.get_row("accounts", &Value::Int(2))?, None);
    txn.release_savepoint("after-ada")?;
    txn.commit()?;

    let accounts = database.table("accounts")?;
    assert!(accounts.get(&Value::Int(1))?.is_some());
    assert_eq!(accounts.get(&Value::Int(2))?, None);

    Ok(())
}

#[test]
fn uncommitted_writes_are_not_dirty_read() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_table("accounts", Some(account_schema()), Vec::new())?;

    let mut writer = database.begin_transaction()?;
    writer.insert_row(
        "accounts",
        vec![Value::Int(1), Value::Str("Ada".to_owned()), Value::Int(37)],
    )?;

    let reader = database.begin_transaction()?;
    assert_eq!(reader.get_row("accounts", &Value::Int(1))?, None);
    writer.rollback();

    Ok(())
}

#[test]
fn snapshot_isolation_allows_write_skew_but_serializable_rejects_it()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let table = database.create_table("accounts", Some(counter_schema()), Vec::new())?;
    table.insert(vec![Value::Int(1), Value::Int(1)])?;
    table.insert(vec![Value::Int(2), Value::Int(1)])?;

    let mut first = database.begin_transaction_with_options(txn::TxnOptions {
        isolation: txn::IsolationLevel::SnapshotIsolation,
        ..txn::TxnOptions::default()
    })?;
    let mut second = database.begin_transaction_with_options(txn::TxnOptions {
        isolation: txn::IsolationLevel::Snapshot,
        ..txn::TxnOptions::default()
    })?;

    assert_eq!(
        first
            .get_row("accounts", &Value::Int(2))?
            .and_then(|row| row.get(1).cloned()),
        Some(Value::Int(1))
    );
    assert_eq!(
        second
            .get_row("accounts", &Value::Int(1))?
            .and_then(|row| row.get(1).cloned()),
        Some(Value::Int(1))
    );
    first.update_row("accounts", vec![Value::Int(1), Value::Int(0)])?;
    second.update_row("accounts", vec![Value::Int(2), Value::Int(0)])?;
    first.commit()?;
    second.commit()?;
    assert_eq!(
        database.table("accounts")?.scan()?,
        vec![
            vec![Value::Int(1), Value::Int(0)],
            vec![Value::Int(2), Value::Int(0)],
        ]
    );

    let mut serializable = create_database(DbConfig::new(Profile::InMemory))?;
    let table = serializable.create_table("accounts", Some(counter_schema()), Vec::new())?;
    table.insert(vec![Value::Int(1), Value::Int(1)])?;
    table.insert(vec![Value::Int(2), Value::Int(1)])?;
    let mut first = serializable.begin_transaction_with_options(txn::TxnOptions {
        isolation: txn::IsolationLevel::Serializable,
        ..txn::TxnOptions::default()
    })?;
    let mut second = serializable.begin_transaction_with_options(txn::TxnOptions {
        isolation: txn::IsolationLevel::Serializable,
        ..txn::TxnOptions::default()
    })?;
    assert!(first.get_row("accounts", &Value::Int(2))?.is_some());
    assert!(second.get_row("accounts", &Value::Int(1))?.is_some());
    first.update_row("accounts", vec![Value::Int(1), Value::Int(0)])?;
    second.update_row("accounts", vec![Value::Int(2), Value::Int(0)])?;
    first.commit()?;

    assert!(matches!(
        second.commit(),
        Err(DbError::Storage(StorageError::Conflict))
    ));

    Ok(())
}

#[test]
fn serializable_rejects_phantom_range_change() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let table = database.create_table("accounts", Some(account_schema()), Vec::new())?;
    table.insert(vec![
        Value::Int(1),
        Value::Str("Ada".to_owned()),
        Value::Int(37),
    ])?;

    let mut txn = database.begin_transaction_with_options(txn::TxnOptions {
        isolation: txn::IsolationLevel::Serializable,
        ..txn::TxnOptions::default()
    })?;
    let rows = txn.range_raw(REL_ROWS_TABLE, &[], &[])?;
    assert_eq!(rows.len(), 1);

    table.insert(vec![
        Value::Int(2),
        Value::Str("Grace".to_owned()),
        Value::Int(85),
    ])?;
    txn.update_row(
        "accounts",
        vec![Value::Int(1), Value::Str("Ada".to_owned()), Value::Int(38)],
    )?;

    assert!(matches!(
        txn.commit(),
        Err(DbError::Storage(StorageError::Conflict))
    ));

    Ok(())
}

#[test]
fn write_write_conflict_returns_storage_conflict() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let table = database.create_table("accounts", Some(account_schema()), Vec::new())?;
    table.insert(vec![
        Value::Int(1),
        Value::Str("Ada".to_owned()),
        Value::Int(37),
    ])?;

    let mut first = database.begin_transaction()?;
    let mut second = database.begin_transaction()?;

    first.update_row(
        "accounts",
        vec![Value::Int(1), Value::Str("Ada".to_owned()), Value::Int(38)],
    )?;
    second.update_row(
        "accounts",
        vec![Value::Int(1), Value::Str("Ada".to_owned()), Value::Int(39)],
    )?;

    first.commit()?;
    assert!(matches!(
        second.commit(),
        Err(DbError::Storage(StorageError::Conflict))
    ));

    Ok(())
}

#[test]
fn transaction_with_retry_does_not_lose_concurrent_increments()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let counter = database.create_table("counters", Some(counter_schema()), Vec::new())?;
    counter.insert(vec![Value::Int(1), Value::Int(0)])?;

    let database = Arc::new(database);
    let mut handles = Vec::new();
    for _ in 0..8 {
        let database = Arc::clone(&database);
        handles.push(thread::spawn(move || {
            database
                .transaction_with_retry(100, |txn| {
                    let row = txn
                        .get_row("counters", &Value::Int(1))?
                        .ok_or_else(|| DbError::TransactionAborted("missing counter".to_owned()))?;
                    let count = match row.get(1) {
                        Some(Value::Int(value)) => *value,
                        _ => {
                            return Err(DbError::TransactionAborted("invalid counter".to_owned()));
                        }
                    };
                    txn.update_row("counters", vec![Value::Int(1), Value::Int(count + 1)])
                })
                .map_err(|error| error.to_string())
        }));
    }

    for handle in handles {
        match handle.join() {
            Ok(result) => result.map_err(io::Error::other)?,
            Err(_) => return Err(io::Error::other("counter thread panicked").into()),
        }
    }

    assert_eq!(
        database.table("counters")?.get(&Value::Int(1))?,
        Some(vec![Value::Int(1), Value::Int(8)])
    );

    Ok(())
}

#[test]
fn concurrent_vector_inserts_stay_in_the_in_memory_index() -> Result<(), Box<dyn std::error::Error>>
{
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_vector_collection(
        "embeddings",
        VectorCollectionConfig::new(CollectionId::new(93), 2).with_metric(VectorMetric::L2),
    )?;

    let database = Arc::new(database);
    let mut handles = Vec::new();
    for worker in 0..8_u16 {
        let database = Arc::clone(&database);
        handles.push(thread::spawn(move || {
            let mut ids = Vec::new();
            for index in 0..32_u16 {
                // Reopening per insert exercises first-open and resize rebuilds.
                let vectors = database
                    .vector_collection("embeddings")
                    .map_err(|error| error.to_string())?;
                let id = vectors
                    .insert_vector(
                        &Value::Int(i64::from(index)),
                        vec![f32::from(worker) + 1.0, f32::from(index) + 1.0],
                    )
                    .map_err(|error| error.to_string())?;
                ids.push(id);
            }
            Ok::<_, String>(ids)
        }));
    }

    let mut inserted = BTreeSet::new();
    for handle in handles {
        match handle.join() {
            Ok(result) => inserted.extend(result.map_err(io::Error::other)?),
            Err(_) => return Err(io::Error::other("vector insert thread panicked").into()),
        }
    }
    assert_eq!(inserted.len(), 256);

    let hits = database
        .vector_collection("embeddings")?
        .knn(&[0.0, 0.0], 256)?;
    let found = hits.iter().map(|hit| hit.id).collect::<BTreeSet<_>>();
    assert_eq!(hits.len(), 256);
    assert_eq!(found, inserted);

    Ok(())
}

/// Runs `sql` from several threads at once, each on its own runtime, as `PostgreSQL`
/// wire does. Returns, per thread, the indexes of the statements that completed;
/// statements that still conflict after every retry are skipped.
fn run_concurrent_statements(
    database: &Arc<Database>,
    workers: i64,
    statements: i64,
    sql: impl Fn(i64, i64) -> String + Send + Sync + 'static,
) -> Result<Vec<Vec<i64>>, Box<dyn std::error::Error>> {
    let sql = Arc::new(sql);
    let mut handles = Vec::new();
    for worker in 0..workers {
        let database = Arc::clone(database);
        let sql = Arc::clone(&sql);
        handles.push(thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            let mut completed = Vec::new();
            for index in 0..statements {
                let statement = sql(worker, index);
                match runtime.block_on(database.query(&statement)) {
                    Ok(_) => completed.push(index),
                    Err(error) if error.is_conflict() => {}
                    Err(error) => return Err(format!("{statement}: {error}")),
                }
            }
            Ok::<_, String>(completed)
        }));
    }

    let mut results = Vec::new();
    for handle in handles {
        match handle.join() {
            Ok(result) => results.push(result.map_err(io::Error::other)?),
            Err(_) => return Err(io::Error::other("statement thread panicked").into()),
        }
    }
    Ok(results)
}

#[test]
fn concurrent_columnar_sql_inserts_keep_every_acknowledged_row()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("analytical-concurrent.redb");
    let mut database = create_database(DbConfig::on_disk(Profile::Analytical, path))?;
    let sales = database.create_table("sales", Some(sales_schema()), Vec::new())?;
    assert_eq!(sales.layout(), TableLayout::Columnar);

    let database = Arc::new(database);
    let results = run_concurrent_statements(&database, 4, 16, |worker, index| {
        let id = worker * 100 + index;
        format!("INSERT INTO sales VALUES ({id}, 'c{worker}', 1.5)")
    })?;

    let mut acknowledged = BTreeSet::new();
    for (worker, completed) in (0_i64..).zip(&results) {
        acknowledged.extend(completed.iter().map(|index| worker * 100 + index));
    }
    let stored = database
        .table("sales")?
        .scan()?
        .into_iter()
        .filter_map(|row| match row.first() {
            Some(Value::Int(id)) => Some(*id),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    // Every insert commits within its retries, and none is lost.
    assert_eq!(acknowledged.len(), 64);
    assert_eq!(stored, acknowledged);

    Ok(())
}

#[test]
fn concurrent_on_conflict_inserts_do_not_report_duplicate_keys()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let counters = database.create_table("counters", Some(counter_schema()), Vec::new())?;
    counters.insert(vec![Value::Int(1), Value::Int(-1)])?;

    let database = Arc::new(database);
    let results = run_concurrent_statements(&database, 4, 16, |worker, index| {
        if index % 2 == 0 {
            format!(
                "INSERT INTO counters (id, value) VALUES (1, {worker}) \
                 ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value"
            )
        } else {
            format!(
                "INSERT INTO counters (id, value) VALUES ({}, {worker}) \
                 ON CONFLICT (id) DO NOTHING",
                100 + index
            )
        }
    })?;

    // DO NOTHING succeeds once it sees the row the winning statement inserted.
    for completed in &results {
        assert!((1..16).step_by(2).all(|index| completed.contains(&index)));
    }
    let rows = database.table("counters")?.scan()?;
    assert_eq!(rows.len(), 9);
    assert!(matches!(
        rows.first().and_then(|row| row.get(1)),
        Some(Value::Int(0..=3))
    ));

    // Plain INSERTs of one new key from several threads: all but one report the
    // duplicate primary key. query::tests covers the lost-race path
    // deterministically.
    let results = run_concurrent_statements(&database, 4, 1, |worker, _| {
        format!("INSERT INTO counters (id, value) VALUES (500, {worker})")
    });
    let message = match results {
        Ok(_) => String::new(),
        Err(error) => error.to_string(),
    };
    assert!(message.contains("duplicate primary key"), "{message}");

    Ok(())
}

#[tokio::test]
async fn query_governor_limits_are_settable_and_a_full_queue_is_busy()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_table("counters", Some(counter_schema()), Vec::new())?;
    let defaults = database.performance_config().query.clone();
    assert!(matches!(
        database.set_query_governor(0, 10),
        Err(ConfigError::Unsupported(_))
    ));
    assert!(matches!(
        database.set_query_governor(1, 0),
        Err(ConfigError::Unsupported(_))
    ));
    assert_eq!(database.performance_config().query, defaults);

    database.set_query_governor(1, 10)?;
    assert_eq!(
        database.performance_config().query.max_concurrent_queries,
        1
    );
    assert_eq!(database.performance_config().query.queue_timeout_ms, 10);

    // Hold the only slot, as a running statement would.
    let slot = Arc::clone(&database.query_permits).acquire_owned().await?;
    let error = match database.query("SELECT id FROM counters").await {
        Ok(output) => return Err(format!("expected a busy governor, got {output:?}").into()),
        Err(error) => error,
    };
    assert!(
        matches!(error, DbError::Query(QueryError::Busy(_))),
        "{error}"
    );
    assert!(!error.is_conflict());
    assert_eq!(crate::compat::sqlstate_for_db_error(&error), "53200");

    drop(slot);
    database.query("SELECT id FROM counters").await?;
    Ok(())
}

#[test]
fn bootstrap_admin_check_agrees_with_ensure() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let reader = Role::new("reader").grant(Resource::Database, Permission::Read);
    database.set_authz_policy(AuthzPolicy::new([reader.clone()]));
    database.set_principal_registry(
        PrincipalRegistry::new().with_principal("ada", Principal::new("ada").with_role("reader")),
    );
    assert!(!database.has_bootstrap_admin_principal());

    database.ensure_bootstrap_admin_principal()?;
    assert!(database.has_bootstrap_admin_principal());
    // Other roles and principals are kept.
    assert_eq!(database.authz_policy().roles().get("reader"), Some(&reader));
    assert!(
        database
            .principal_registry()
            .principals()
            .contains_key("ada")
    );

    // A narrowed admin role or a changed admin principal is not the bootstrap
    // definition, and ensure() restores it.
    let narrowed = Role::new("admin").grant(Resource::System, Permission::Admin);
    database.set_authz_policy(database.authz_policy().clone().allow(narrowed));
    assert!(!database.has_bootstrap_admin_principal());
    database.ensure_bootstrap_admin_principal()?;
    assert!(database.has_bootstrap_admin_principal());

    let mut principals = database.principal_registry().clone();
    principals.insert("admin", Principal::new("admin").with_role("reader"));
    database.set_principal_registry(principals);
    assert!(!database.has_bootstrap_admin_principal());
    database.ensure_bootstrap_admin_principal()?;
    assert!(database.has_bootstrap_admin_principal());

    let (role, principal) = super::bootstrap_admin_identity();
    assert!(
        database
            .authz_policy()
            .authorize(&principal, &Resource::System, Permission::Admin)
            .is_ok()
    );
    assert_eq!(
        principal.roles().iter().collect::<Vec<_>>(),
        vec![role.name()]
    );
    Ok(())
}

#[test]
fn bootstrap_admin_principal_is_recognized_after_reopen() -> Result<(), Box<dyn std::error::Error>>
{
    let temp_dir = tempfile::tempdir()?;
    let config = DbConfig::on_disk(
        Profile::Transactional,
        temp_dir.path().join("bootstrap-admin.redb"),
    );
    {
        let mut database = create_database(config.clone())?;
        database.ensure_bootstrap_admin_principal()?;
    }
    // The stored definition reads back equal, so once stored security state is
    // loaded, login and change-password keep to the read guard instead of
    // rewriting it.
    let database = open_database_with_ops(config, &OperationalConfig::default())?;
    assert!(database.has_bootstrap_admin_principal());
    Ok(())
}

#[test]
fn redb_committed_transaction_survives_reopen_and_rollback_does_not()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("txn-durable.redb");
    let config = DbConfig::on_disk(Profile::HighDurability, path);

    {
        let mut database = create_database(config.clone())?;
        database.create_table("accounts", Some(account_schema()), Vec::new())?;
        database.transaction(|txn| {
            txn.insert_row(
                "accounts",
                vec![Value::Int(1), Value::Str("Ada".to_owned()), Value::Int(37)],
            )
        })?;

        let mut rolled_back = database.begin_transaction()?;
        rolled_back.insert_row(
            "accounts",
            vec![
                Value::Int(2),
                Value::Str("Grace".to_owned()),
                Value::Int(85),
            ],
        )?;
        rolled_back.rollback();
    }

    let database = open_database(config)?;
    let accounts = database.table("accounts")?;
    assert!(accounts.get(&Value::Int(1))?.is_some());
    assert_eq!(accounts.get(&Value::Int(2))?, None);

    Ok(())
}

#[test]
fn analytical_transaction_rollback_does_not_write_columnar_segment()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("analytical-rollback.redb");
    let mut database = create_database(DbConfig::on_disk(Profile::Analytical, path))?;
    database.create_table("sales", Some(sales_schema()), Vec::new())?;

    let mut txn = database.begin_transaction()?;
    txn.insert_row(
        "sales",
        vec![
            Value::Int(1),
            Value::Str("a".to_owned()),
            Value::Float(10.0),
        ],
    )?;
    assert_eq!(
        txn.get_row("sales", &Value::Int(1))?,
        Some(vec![
            Value::Int(1),
            Value::Str("a".to_owned()),
            Value::Float(10.0),
        ])
    );
    txn.rollback();

    assert_eq!(database.table("sales")?.scan()?, Vec::<Row>::new());

    Ok(())
}

#[test]
fn analytical_transaction_row_crud_keeps_rows_beyond_first_segment()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let database =
        analytical_sales_database(temp_dir.path().join("analytical-segments.redb"), 2_500)?;

    let txn = database.begin_transaction()?;
    assert_eq!(
        txn.get_row("sales", &Value::Int(1_500))?,
        Some(sales_row(1_500))
    );
    assert_eq!(
        txn.get_row("sales", &Value::Int(2_400))?,
        Some(sales_row(2_400))
    );
    txn.rollback();

    database.transaction(|txn| txn.insert_row("sales", sales_row(2_500)))?;
    assert_eq!(database.table("sales")?.scan()?.len(), 2_501);

    let updated = vec![
        Value::Int(1_500),
        Value::Str("updated".to_owned()),
        Value::Float(99.0),
    ];
    database.transaction(|txn| txn.update_row("sales", updated.clone()))?;
    database.transaction(|txn| txn.delete_row("sales", &Value::Int(2_400)))?;
    database.transaction_with_options(
        txn::TxnOptions {
            isolation: txn::IsolationLevel::Serializable,
            ..txn::TxnOptions::default()
        },
        |txn| {
            txn.get_row("sales", &Value::Int(2_499))?;
            txn.update_row("sales", sales_row(2_499))
        },
    )?;

    let sales = database.table("sales")?;
    assert_eq!(sales.scan()?.len(), 2_500);
    assert_eq!(sales.get(&Value::Int(1_500))?, Some(updated));
    assert_eq!(sales.get(&Value::Int(2_400))?, None);
    for id in [1_100, 2_499, 2_500] {
        assert_eq!(sales.get(&Value::Int(id))?, Some(sales_row(id)));
    }

    Ok(())
}

#[test]
fn analytical_transaction_rewrites_pending_columnar_segments()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let database =
        analytical_sales_database(temp_dir.path().join("analytical-pending.redb"), 2_048)?;

    let mut txn = database.begin_transaction()?;
    txn.insert_row("sales", sales_row(2_048))?;
    assert_eq!(
        txn.get_row("sales", &Value::Int(2_048))?,
        Some(sales_row(2_048))
    );
    assert!(txn.get_row("sales", &Value::Int(1_500))?.is_some());
    txn.delete_row("sales", &Value::Int(0))?;
    txn.delete_row("sales", &Value::Int(1))?;
    txn.commit()?;

    let sales = database.table("sales")?;
    assert_eq!(sales.scan()?.len(), 2_047);
    assert_eq!(sales.get(&Value::Int(0))?, None);
    assert_eq!(sales.get(&Value::Int(1))?, None);
    for id in [2_047, 2_048] {
        assert_eq!(sales.get(&Value::Int(id))?, Some(sales_row(id)));
    }

    Ok(())
}

#[test]
fn analytical_transaction_conflicts_with_concurrent_columnar_write()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let database =
        analytical_sales_database(temp_dir.path().join("analytical-conflict.redb"), 2_500)?;

    let mut txn = database.begin_transaction()?;
    assert!(txn.get_row("sales", &Value::Int(2_100))?.is_some());
    database.table("sales")?.insert(sales_row(5_000))?;
    txn.update_row(
        "sales",
        vec![
            Value::Int(2_100),
            Value::Str("changed".to_owned()),
            Value::Float(1.0),
        ],
    )?;
    assert!(matches!(
        txn.commit(),
        Err(DbError::Storage(StorageError::Conflict))
    ));

    let sales = database.table("sales")?;
    assert_eq!(sales.scan()?.len(), 2_501);
    assert!(sales.get(&Value::Int(5_000))?.is_some());
    assert_eq!(sales.get(&Value::Int(2_100))?, Some(sales_row(2_100)));

    Ok(())
}

#[test]
fn analytical_read_committed_transaction_keeps_rows_beyond_first_segment()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let database = analytical_sales_database(
        temp_dir.path().join("analytical-read-committed.redb"),
        2_500,
    )?;
    let read_committed = || {
        database.begin_transaction_with_options(txn::TxnOptions {
            isolation: txn::IsolationLevel::ReadCommitted,
            ..txn::TxnOptions::default()
        })
    };

    let mut txn = read_committed()?;
    assert!(txn.get_row("sales", &Value::Int(2_200))?.is_some());
    txn.insert_row("sales", sales_row(5_001))?;
    txn.commit()?;
    assert_eq!(database.table("sales")?.scan()?.len(), 2_501);

    // Read committed sees a concurrent commit in every segment, but rewriting
    // segments that changed after the transaction began still conflicts.
    let mut txn = read_committed()?;
    database.table("sales")?.insert(sales_row(5_000))?;
    assert!(txn.get_row("sales", &Value::Int(5_000))?.is_some());
    txn.insert_row("sales", sales_row(5_002))?;
    assert!(matches!(
        txn.commit(),
        Err(DbError::Storage(StorageError::Conflict))
    ));

    let sales = database.table("sales")?;
    assert_eq!(sales.scan()?.len(), 2_502);
    for id in [1_100, 2_200, 2_499, 5_000, 5_001] {
        assert!(sales.get(&Value::Int(id))?.is_some(), "row {id} is missing");
    }
    assert_eq!(sales.get(&Value::Int(5_002))?, None);

    Ok(())
}

#[test]
fn analytical_transaction_deletes_committed_segments_it_no_longer_needs()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let database =
        analytical_sales_database(temp_dir.path().join("analytical-shrink.redb"), 1_025)?;
    let (start, end) = database.table("sales")?.columnar_segment_bounds();
    let stored_keys = |table: &str| -> Result<Vec<Vec<u8>>, ReplError> {
        Ok(database
            .range(table, &start, &end, ReadConsistency::Strong)?
            .into_iter()
            .map(|(key, _)| key)
            .collect())
    };
    assert_eq!(stored_keys(REL_COLUMNAR_SEGMENTS_TABLE)?.len(), 2);
    assert_eq!(stored_keys(REL_COLUMNAR_SEGMENT_META_TABLE)?.len(), 2);

    // 1,024 rows fit one segment, so the committed second segment must go.
    database.transaction(|txn| txn.delete_row("sales", &Value::Int(7)))?;

    let expected = (0..1_025)
        .filter(|id| *id != 7)
        .map(sales_row)
        .collect::<Vec<_>>();
    assert_eq!(database.table("sales")?.scan()?, expected);
    let segments = stored_keys(REL_COLUMNAR_SEGMENTS_TABLE)?;
    assert_eq!(segments.len(), 1);
    assert_eq!(stored_keys(REL_COLUMNAR_SEGMENT_META_TABLE)?, segments);

    Ok(())
}

async fn run_cross_model_join(mut database: Database) -> Result<(), Box<dyn std::error::Error>> {
    let orders = database.create_table("orders", Some(order_schema()), Vec::new())?;
    let users =
        database.create_collection("users", CollectionId::new(8), user_doc_fields(), Vec::new())?;

    orders.insert(vec![Value::Int(10), Value::Int(1), Value::Float(5.5)])?;
    orders.insert(vec![Value::Int(11), Value::Int(2), Value::Float(7.0)])?;
    users.insert(&user_document(1, Some(Value::Str("Warsaw".to_owned()))))?;
    users.insert(&user_document(2, Some(Value::Str("London".to_owned()))))?;

    let output = database
        .query(
            "select orders.id, users.city \
                 from orders join users on orders.user_id = users.user_id \
                 order by orders.id",
        )
        .await?;

    assert_eq!(
        output,
        SqlOutput::Rows(SqlRows {
            columns: vec!["id".to_owned(), "city".to_owned()],
            rows: vec![
                vec![Value::Int(10), Value::Str("Warsaw".to_owned())],
                vec![Value::Int(11), Value::Str("London".to_owned())],
            ],
        })
    );

    Ok(())
}

#[tokio::test]
async fn phase32_foreign_csv_table_is_queryable() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let temp_dir = tempfile::tempdir()?;
    std::fs::write(temp_dir.path().join("users.csv"), "1,Ada,37\n2,Grace,85\n")?;

    database.create_foreign_table(
        "foreign_users",
        account_schema(),
        ForeignSource::Csv {
            uri: ObjectStoreUri::from_local_dir(temp_dir.path()),
            path: "users.csv".to_owned(),
            has_header: false,
        },
        ForeignTableOptions::default(),
    )?;

    let output = database
        .query("SELECT name FROM foreign_users WHERE id = 2")
        .await?;
    assert_eq!(
        output,
        SqlOutput::Rows(SqlRows {
            columns: vec!["name".to_owned()],
            rows: vec![vec![Value::Str("Grace".to_owned())]],
        })
    );
    Ok(())
}

#[tokio::test]
async fn phase32_temporal_as_of_lsn_and_retention() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let users = database.create_table("users", Some(account_schema()), Vec::new())?;
    database.enable_system_versioning("users", TemporalRetention::default())?;

    users.insert(vec![
        Value::Int(1),
        Value::Str("Ada".to_owned()),
        Value::Int(37),
    ])?;
    let inserted_lsn = latest_lsn(&database)?;
    users.update(vec![
        Value::Int(1),
        Value::Str("Ada".to_owned()),
        Value::Int(38),
    ])?;

    let output = database
        .query_as_of(
            "SELECT age FROM users WHERE id = 1",
            TemporalPoint::Lsn(inserted_lsn),
        )
        .await?;
    assert_eq!(
        output,
        SqlOutput::Rows(SqlRows {
            columns: vec!["age".to_owned()],
            rows: vec![vec![Value::Int(37)]],
        })
    );

    let mut expired = create_database(DbConfig::new(Profile::InMemory))?;
    let expired_users = expired.create_table("users", Some(account_schema()), Vec::new())?;
    expired_users.insert(vec![
        Value::Int(1),
        Value::Str("Ada".to_owned()),
        Value::Int(37),
    ])?;
    let lsn = latest_lsn(&expired)?;
    expired.enable_system_versioning(
        "users",
        TemporalRetention {
            min_lsn: lsn + 1,
            keep_history: true,
        },
    )?;
    assert!(matches!(
        expired
            .query_as_of("SELECT * FROM users", TemporalPoint::Lsn(lsn))
            .await,
        Err(DbError::Temporal(
            crate::temporal::TemporalError::RetentionExpired { .. }
        ))
    ));
    Ok(())
}

/// `SELECT name FROM users WHERE id = ++...+2` with `depth` unary plus signs, each one parser
/// recursion level deeper. The engine answers it from the `id` index, without `DataFusion`, whose
/// own parser stops at a fixed depth of 50.
fn nested_select(depth: usize) -> String {
    format!("SELECT name FROM users WHERE id = {}2", "+".repeat(depth))
}

/// Seeds `users` and `orders`, keeps `users` history, lets `alice` read `users` and lets the
/// role `writer` write it.
fn seed_parser_limit_database(
    database: &mut Database,
) -> Result<(Principal, u64), Box<dyn std::error::Error>> {
    seed_orders(database)?;
    database.enable_system_versioning("users", TemporalRetention::default())?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("reader").grant(Resource::Table("users".to_owned()), Permission::Read),
        Role::new("writer").grant(Resource::Table("users".to_owned()), Permission::Write),
    ]));
    Ok((
        Principal::new("alice").with_role("reader"),
        latest_lsn(database)?,
    ))
}

/// The SQL parser limits (`query.parser_recursion_limit`, `query.max_sql_bytes`, ...) bind the
/// engine's parse and the compatibility, RBAC and temporal pre-parses, which parse with the
/// configured limits instead of the sqlparser defaults.
#[tokio::test]
async fn sql_parser_limits_bind_the_engine_and_pre_parses() -> Result<(), Box<dyn std::error::Error>>
{
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let (reader, lsn) = seed_parser_limit_database(&mut database)?;
    assert_eq!(
        database.performance_config().query.parser_recursion_limit,
        128
    );

    // Deeper than the sqlparser default of 50, within the preset.
    let deep = nested_select(80);
    let grace = SqlOutput::Rows(SqlRows {
        columns: vec!["name".to_owned()],
        rows: vec![vec![Value::Str("Grace".to_owned())]],
    });
    assert_eq!(database.query(&deep).await?, grace);
    assert_eq!(database.query_as(&reader, &deep).await?, grace);
    let insert = format!(
        "INSERT INTO users (id, name, age) VALUES (3, 'Lin', {}41)",
        "+".repeat(80)
    );
    let writer = Principal::new("bob").with_role("writer");
    assert_eq!(
        database.query_as(&writer, &insert).await?,
        SqlOutput::AffectedRows(1)
    );
    // An AS OF query and an ordered SELECT go to DataFusion, whose parser keeps its own limit of
    // 50. Past it they are parse errors (SQLSTATE 42601), not internal errors.
    for capped in [
        database.query_as_of(&deep, TemporalPoint::Lsn(lsn)).await,
        database.query(&format!("{deep} ORDER BY name")).await,
    ] {
        match &capped {
            Err(error @ DbError::Query(QueryError::Parse(message)))
                if message.contains("RecursionLimitExceeded") =>
            {
                assert_eq!(crate::compat::sqlstate_for_db_error(error), "42601");
            }
            other => return Err(format!("expected a DataFusion parse error: {other:?}").into()),
        }
    }

    let mut performance = PerformanceConfig::for_profile(Profile::InMemory);
    performance.query.parser_recursion_limit = 20;
    performance.query.max_sql_bytes = 256;
    let mut limited = create_database_with_ops(
        DbConfig::new(Profile::InMemory),
        OperationalConfig::new().with_performance(performance),
    )?;
    let (reader, lsn) = seed_parser_limit_database(&mut limited)?;
    let too_deep = nested_select(30);
    for result in [
        limited.query(&too_deep).await,
        limited.query_as(&reader, &too_deep).await,
        limited
            .query_as_of(&too_deep, TemporalPoint::Lsn(lsn))
            .await,
    ] {
        assert!(
            matches!(&result, Err(DbError::Query(QueryError::Parse(message)))
                if message.contains("recursion limit exceeded")),
            "{result:?}"
        );
    }
    // The compatibility pre-parse answers `SELECT version()` and the catalog views, but not past
    // max_sql_bytes, which counts the whole text, trailing whitespace and semicolons included.
    for padded in [
        format!("SELECT version() -- {}", "x".repeat(300)),
        format!("SELECT version(){}", " ".repeat(300)),
        format!("SELECT version(){}", ";".repeat(300)),
        format!("SELECT * FROM pg_catalog.pg_type{}", "; \n".repeat(100)),
    ] {
        for result in [
            limited.query(&padded).await,
            limited.query_as(&reader, &padded).await,
        ] {
            assert!(
                matches!(&result, Err(DbError::Query(QueryError::InputLimit(_)))),
                "{result:?}"
            );
        }
    }
    for sql in [
        "SELECT version()",
        "  SELECT version() ;; ",
        "SELECT * FROM pg_catalog.pg_type;",
    ] {
        assert!(matches!(limited.query(sql).await?, SqlOutput::Rows(_)));
    }
    Ok(())
}

#[tokio::test]
async fn phase32_cataloged_materialized_view_is_queryable() -> Result<(), Box<dyn std::error::Error>>
{
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_orders(&mut database)?;
    let spec = MaterializedViewSpec {
        name: "users_by_name".to_owned(),
        source_table: "users".to_owned(),
        filter: None,
        group_by: 1,
        aggregates: vec![AggregateSpec {
            output_name: "count".to_owned(),
            kind: AggregateKind::Count,
            column: None,
        }],
    };
    database.create_materialized_view_object(&spec)?;

    let output = database
        .query("SELECT count FROM users_by_name WHERE name = 'Ada'")
        .await?;
    assert_eq!(
        output,
        SqlOutput::Rows(SqlRows {
            columns: vec!["count".to_owned()],
            rows: vec![vec![Value::Int(1)]],
        })
    );
    Ok(())
}

#[test]
fn phase32_continuous_query_and_outbox_are_durable_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let database = create_database(DbConfig::new(Profile::InMemory))?;
    let spec = ContinuousQuerySpec {
        name: "cq_users".to_owned(),
        sql: "SELECT * FROM users".to_owned(),
        filter: ChangefeedFilter {
            target: ChangefeedTarget::Table("users".to_owned()),
        },
        start: ResumeToken::default(),
        buffer_limit: 8,
    };
    let state = database.create_continuous_query(&spec)?;
    assert_eq!(state.name, "cq_users");
    let ack = database.ack_continuous_query("cq_users", ResumeToken::new("default", 7))?;
    assert_eq!(ack.last_ack.lsn, 7);

    database.create_outbox(&OutboxConnectorSpec {
        name: "internal_users".to_owned(),
        filter: ChangefeedFilter {
            target: ChangefeedTarget::Table("users".to_owned()),
        },
        sink: OutboxSink::InternalTable,
        enabled: true,
    })?;
    Ok(())
}

#[tokio::test]
async fn phase32_sql_declarations_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let database = create_database(DbConfig::new(Profile::InMemory))?;
    let cases = [
        "CREATE FOREIGN TABLE ft (id int) SERVER csv",
        "CREATE TRIGGER trg BEFORE INSERT ON users EXECUTE FUNCTION wasm_trigger()",
        "CREATE PROCEDURE proc() LANGUAGE wasm AS '00'",
        "CREATE CONTINUOUS QUERY cq AS SELECT * FROM users",
        "CREATE MATERIALIZED VIEW mv AS SELECT 1",
        "CREATE TEMPORAL TABLE users_history (id int)",
    ];

    for sql in cases {
        match database.query(sql).await {
            Err(DbError::Query(QueryError::Unsupported(message))) => {
                assert!(message.contains("current SQL support matrix"));
            }
            other => panic!("expected deterministic unsupported error for {sql}: {other:?}"),
        }
    }

    Ok(())
}

#[test]
fn phase32_before_wasm_trigger_rejects_transaction_write() -> Result<(), Box<dyn std::error::Error>>
{
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_table("accounts", Some(account_schema()), Vec::new())?;
    let wasm = constant_value_wasm(&trigger_response("reject", None))?;
    database.register_wasm_trigger(
        wasm_trigger(
            "reject_accounts_insert",
            "accounts",
            TriggerTiming::Before,
            TriggerEvent::Insert,
        ),
        &wasm,
    )?;

    let error = match database.transaction(|txn| {
        txn.insert_row(
            "accounts",
            vec![Value::Int(1), Value::Str("Ada".to_owned()), Value::Int(37)],
        )
    }) {
        Ok(()) => panic!("before trigger must reject the write"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("rejected write"));
    assert_eq!(database.table("accounts")?.get(&Value::Int(1))?, None);
    Ok(())
}

#[test]
fn phase32_before_wasm_trigger_rejects_direct_batch() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_table("accounts", Some(account_schema()), Vec::new())?;
    let wasm = constant_value_wasm(&trigger_response("reject", None))?;
    database.register_wasm_trigger(
        wasm_trigger(
            "reject_accounts_direct_insert",
            "accounts",
            TriggerTiming::Before,
            TriggerEvent::Insert,
        ),
        &wasm,
    )?;

    let ops = database.table("accounts")?.insert_ops(vec![
        Value::Int(1),
        Value::Str("Ada".to_owned()),
        Value::Int(37),
    ])?;
    let error = match database.propose_batch(ops) {
        Ok(()) => panic!("before trigger must reject the direct batch"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("rejected write"));
    assert_eq!(database.table("accounts")?.get(&Value::Int(1))?, None);
    Ok(())
}

#[test]
fn phase32_before_wasm_trigger_can_replace_transaction_row()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_table("accounts", Some(account_schema()), Vec::new())?;
    let replacement = Value::Array(vec![
        Value::Int(1),
        Value::Str("Grace".to_owned()),
        Value::Int(99),
    ]);
    let wasm = constant_value_wasm(&trigger_response("replace", Some(replacement)))?;
    database.register_wasm_trigger(
        wasm_trigger(
            "replace_accounts_insert",
            "accounts",
            TriggerTiming::Before,
            TriggerEvent::Insert,
        ),
        &wasm,
    )?;

    database.transaction(|txn| {
        txn.insert_row(
            "accounts",
            vec![Value::Int(1), Value::Str("Ada".to_owned()), Value::Int(37)],
        )
    })?;

    assert_eq!(
        database.table("accounts")?.get(&Value::Int(1))?,
        Some(vec![
            Value::Int(1),
            Value::Str("Grace".to_owned()),
            Value::Int(99),
        ])
    );
    Ok(())
}

#[tokio::test]
async fn phase32_before_wasm_trigger_rejects_sql_and_table_writes()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let created = database.create_table("accounts", Some(account_schema()), Vec::new())?;
    register_accounts_trigger(
        &database,
        "reject_accounts_insert",
        TriggerTiming::Before,
        TriggerEvent::Insert,
        &trigger_response("reject", None),
    )?;
    database.set_authz_policy(AuthzPolicy::new([Role::new("writer")
        .grant(Resource::Table("accounts".to_owned()), Permission::Read)
        .grant(Resource::Table("accounts".to_owned()), Permission::Write)]));
    let writer = Principal::new("alice").with_role("writer");
    let sql = "INSERT INTO accounts (id, name, age) VALUES (1, 'Ada', 37)";

    assert_rejected("SQL INSERT", database.query(sql).await, "rejected write");
    match database.query_as(&writer, sql).await {
        Err(DbError::AuthzDenied(error)) => panic!("writer must be authorized: {error}"),
        result => assert_rejected("SQL INSERT as writer", result, "rejected write"),
    }
    assert_rejected(
        "catalog table handle",
        database
            .table("accounts")?
            .insert(account_row(1, "Ada", 37)),
        "rejected write",
    );
    assert_rejected(
        "create_table handle",
        created.insert(account_row(1, "Ada", 37)),
        "rejected write",
    );

    assert!(
        database.table("accounts")?.scan()?.is_empty(),
        "expected an empty value, got {:?}",
        database.table("accounts")?.scan()?
    );
    let SqlOutput::Rows(rows) = database.query("SELECT id FROM accounts").await? else {
        panic!("SELECT must return rows");
    };
    assert!(
        rows.rows.is_empty(),
        "expected an empty value, got {:?}",
        rows.rows
    );
    Ok(())
}

#[tokio::test]
async fn phase32_before_wasm_trigger_replace_is_validated_against_schema()
-> Result<(), Box<dyn std::error::Error>> {
    let cases = [
        (
            "wrong column type",
            vec![Value::Int(1), Value::Int(5), Value::Int(99)],
            "replacement is invalid",
        ),
        ("wrong arity", vec![Value::Int(1)], "replacement is invalid"),
        (
            "changed primary key",
            account_row(2, "Grace", 99),
            "must keep the primary key",
        ),
    ];

    for (case, replacement, expected) in cases {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.create_table("accounts", Some(account_schema()), Vec::new())?;
        register_accounts_trigger(
            &database,
            "replace_accounts_insert",
            TriggerTiming::Before,
            TriggerEvent::Insert,
            &trigger_response("replace", Some(Value::Array(replacement))),
        )?;

        assert_rejected(
            &format!("{case}: transaction"),
            database.transaction(|txn| txn.insert_row("accounts", account_row(1, "Ada", 37))),
            expected,
        );
        assert_rejected(
            &format!("{case}: table handle"),
            database
                .table("accounts")?
                .insert(account_row(1, "Ada", 37)),
            expected,
        );
        assert_rejected(
            &format!("{case}: SQL INSERT"),
            database
                .query("INSERT INTO accounts (id, name, age) VALUES (1, 'Ada', 37)")
                .await,
            expected,
        );

        let accounts = database.table("accounts")?;
        assert_eq!(accounts.get(&Value::Int(1))?, None, "{case}");
        assert_eq!(accounts.get(&Value::Int(2))?, None, "{case}");
    }
    Ok(())
}

#[tokio::test]
async fn phase32_before_wasm_trigger_replace_maintains_secondary_index()
-> Result<(), Box<dyn std::error::Error>> {
    // One database per entry path: the constant replacement always has primary key 1.
    for path in ["table handle", "SQL INSERT", "transaction"] {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.create_table(
            "accounts",
            Some(account_schema()),
            vec![RelIndexSpec::new(1, 1)],
        )?;
        register_accounts_trigger(
            &database,
            "replace_accounts_insert",
            TriggerTiming::Before,
            TriggerEvent::Insert,
            &trigger_response("replace", Some(Value::Array(account_row(1, "Grace", 99)))),
        )?;

        match path {
            "table handle" => {
                database
                    .table("accounts")?
                    .insert(account_row(1, "Ada", 37))?;
            }
            "SQL INSERT" => {
                database
                    .query("INSERT INTO accounts (id, name, age) VALUES (1, 'Ada', 37)")
                    .await?;
            }
            _ => {
                database
                    .transaction(|txn| txn.insert_row("accounts", account_row(1, "Ada", 37)))?;
            }
        }

        let entries = rel_index_entries(&database)?;
        assert_eq!(entries.len(), 1, "{path}: {entries:?}");
        assert!(contains_bytes(&entries[0].0, b"Grace"), "{path}");
        let accounts = database.table("accounts")?;
        assert_eq!(
            accounts.get(&Value::Int(1))?,
            Some(account_row(1, "Grace", 99)),
            "{path}"
        );
        assert_eq!(
            accounts.query_eq(1, &Value::Str("Grace".to_owned()))?.rows,
            vec![account_row(1, "Grace", 99)],
            "{path}"
        );
        assert!(
            accounts
                .query_eq(1, &Value::Str("Ada".to_owned()))?
                .rows
                .is_empty(),
            "{path}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn phase32_before_wasm_trigger_replace_on_update_rewrites_stale_index()
-> Result<(), Box<dyn std::error::Error>> {
    let cases = [
        ("table handle", account_row(1, "Grace", 99)),
        ("transaction", account_row(1, "Grace", 99)),
        ("SQL ON CONFLICT", account_row(1, "Grace", 99)),
        (
            "table handle keeps indexed value",
            account_row(1, "Bob", 99),
        ),
        ("transaction keeps indexed value", account_row(1, "Bob", 99)),
    ];

    for (case, replacement) in cases {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.create_table(
            "accounts",
            Some(account_schema()),
            vec![RelIndexSpec::new(1, 1).with_include(vec![2])],
        )?;
        database
            .table("accounts")?
            .insert(account_row(1, "Ada", 37))?;
        register_accounts_trigger(
            &database,
            "replace_accounts_update",
            TriggerTiming::Before,
            TriggerEvent::Update,
            &trigger_response("replace", Some(Value::Array(replacement.clone()))),
        )?;

        if case.starts_with("table handle") {
            database
                .table("accounts")?
                .update(account_row(1, "Bob", 40))?;
        } else if case.starts_with("transaction") {
            database.transaction(|txn| txn.update_row("accounts", account_row(1, "Bob", 40)))?;
        } else {
            database
                .query(
                    "INSERT INTO accounts (id, name, age) VALUES (1, 'Bob', 40) \
                     ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
                )
                .await?;
        }

        let Value::Str(indexed) = &replacement[1] else {
            panic!("{case}: replacement name must be a string");
        };
        let entries = rel_index_entries(&database)?;
        assert_eq!(entries.len(), 1, "{case}: {entries:?}");
        let (key, payload) = &entries[0];
        assert!(contains_bytes(key, indexed.as_bytes()), "{case}: {key:?}");
        assert_eq!(
            decode_value(payload)?,
            Value::Array(vec![Value::Int(99)]),
            "{case}"
        );
        assert_eq!(
            database.table("accounts")?.get(&Value::Int(1))?,
            Some(replacement),
            "{case}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn phase32_before_wasm_trigger_replace_rejects_repeated_row_key()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_table("accounts", Some(account_schema()), Vec::new())?;
    register_accounts_trigger(
        &database,
        "replace_accounts_insert",
        TriggerTiming::Before,
        TriggerEvent::Insert,
        &trigger_response("replace", Some(Value::Array(account_row(1, "Grace", 99)))),
    )?;

    assert_rejected(
        "repeated row key",
        database
            .query(
                "INSERT INTO accounts (id, name, age) VALUES (1, 'A', 1), (1, 'B', 2) \
                 ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
            )
            .await,
        "more than once",
    );
    assert_eq!(database.table("accounts")?.get(&Value::Int(1))?, None);
    Ok(())
}

#[test]
fn phase32_wasm_triggers_are_refused_on_columnar_tables() -> Result<(), Box<dyn std::error::Error>>
{
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("columnar.redb");
    let mut database = create_database(DbConfig::on_disk(Profile::Analytical, &path))?;
    let table = database.create_table("accounts", Some(account_schema()), Vec::new())?;
    assert_eq!(table.layout(), TableLayout::Columnar);
    let wasm = constant_value_wasm(&trigger_response(
        "replace",
        Some(Value::Array(account_row(1, "Grace", 99))),
    ))?;
    let spec = wasm_trigger(
        "replace_accounts_insert",
        "accounts",
        TriggerTiming::Before,
        TriggerEvent::Insert,
    );

    match database.register_wasm_trigger(spec.clone(), &wasm) {
        Err(DbError::Query(QueryError::Unsupported(message))) => {
            assert!(message.contains("columnar table accounts"), "{message}");
        }
        other => panic!("columnar trigger registration must be refused, got {other:?}"),
    }

    // A trigger stored before registration refused columnar tables still cannot
    // replace a whole segment.
    let mut legacy = spec;
    legacy.module_hash = crate::extension::register_wasm_module(
        database.replication_ref(),
        &database.wasm_runtime,
        &wasm,
    )?
    .hash;
    crate::continuous::register_trigger(database.replication_ref(), &legacy)?;
    assert_rejected(
        "columnar insert",
        table.insert(account_row(1, "Ada", 37)),
        "cannot replace columnar table segments",
    );
    assert!(
        database.table("accounts")?.scan()?.is_empty(),
        "expected an empty value, got {:?}",
        database.table("accounts")?.scan()?
    );
    Ok(())
}

#[tokio::test]
async fn phase32_after_wasm_trigger_failure_keeps_committed_write()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_table("accounts", Some(account_schema()), Vec::new())?;
    register_accounts_trigger(
        &database,
        "reject_after_accounts_insert",
        TriggerTiming::After,
        TriggerEvent::Insert,
        &trigger_response("reject", None),
    )?;

    database
        .table("accounts")?
        .insert(account_row(1, "Ada", 37))?;
    database.transaction(|txn| txn.insert_row("accounts", account_row(2, "Grace", 41)))?;
    database
        .query("INSERT INTO accounts (id, name, age) VALUES (3, 'Lin', 29)")
        .await?;
    let ops = database
        .table("accounts")?
        .insert_ops(account_row(4, "Bo", 51))?;
    database.propose_batch(ops)?;

    assert_eq!(database.table("accounts")?.scan()?.len(), 4);
    assert!(matches!(
        database
            .table("accounts")?
            .insert(account_row(1, "Ada", 37)),
        Err(QueryError::DuplicatePrimaryKey)
    ));
    let failures = database.post_commit_failures()?;
    assert_eq!(failures.len(), 4, "{failures:?}");
    assert!(failures.iter().all(|failure| {
        failure.stage == PostCommitStage::AfterTrigger
            && failure.name == "reject_after_accounts_insert"
            && failure.error.contains("must return accept")
    }));
    let metrics = crate::observability::global_registry().render()?;
    assert!(metrics.contains("multidb_post_commit_failures_total"));
    Ok(())
}

#[test]
fn after_trigger_failure_keeps_tenant_reservation() -> Result<(), Box<dyn std::error::Error>> {
    let ops = OperationalConfig::new().with_tenant(TenantConfig::new(
        "tenant-a",
        TenantQuota::storage_bytes(1_500),
    ));
    let mut database = create_database_with_ops(DbConfig::new(Profile::InMemory), ops)?;
    database.create_table("accounts", Some(account_schema()), Vec::new())?;
    register_accounts_trigger(
        &database,
        "reject_after_accounts_insert",
        TriggerTiming::After,
        TriggerEvent::Insert,
        &trigger_response("reject", None),
    )?;
    let accounts = database.table("accounts")?;

    accounts.insert(account_row(1, &"x".repeat(900), 1))?;
    match accounts.insert(account_row(2, &"y".repeat(900), 2)) {
        Err(QueryError::Repl(ReplError::QuotaExceeded { .. })) => {}
        other => panic!("committed write must keep its quota reservation, got {other:?}"),
    }
    assert_eq!(accounts.scan()?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn after_commit_hook_failure_is_recorded_not_returned()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_table("accounts", Some(account_schema()), Vec::new())?;
    database.create_collection(
        "events",
        CollectionId::new(7),
        user_doc_fields(),
        Vec::new(),
    )?;
    database.register_hook(&HookSpec::after(
        FAIL_AFTER_HOOK_FOR_TESTS,
        HookTarget::AnyUser,
        HookAction::RecordAfterCommit,
        1_000,
    ))?;

    database
        .table("accounts")?
        .insert(account_row(1, "Ada", 37))?;
    database.transaction(|txn| txn.insert_row("accounts", account_row(2, "Grace", 41)))?;
    database
        .query("INSERT INTO accounts (id, name, age) VALUES (3, 'Lin', 29)")
        .await?;
    database
        .collection("events")?
        .insert(&user_document(1, Some(Value::Str("Paris".to_owned()))))?;

    assert_eq!(database.collection("events")?.scan()?.len(), 1);
    let (events, _) = database.poll_changefeed(
        &ResumeToken::default(),
        &ChangefeedFilter {
            target: ChangefeedTarget::Table("accounts".to_owned()),
        },
        &ChangefeedOptions::default(),
        100,
    )?;
    let upserts = events
        .iter()
        .filter(|event| matches!(event.op, ChangeOp::Upsert { .. }))
        .count();
    assert_eq!(
        upserts, 3,
        "every committed row must be written exactly once"
    );
    let failures = database.post_commit_failures()?;
    assert_eq!(failures.len(), 4, "{failures:?}");
    assert!(failures.iter().all(|failure| {
        failure.stage == PostCommitStage::AfterHook
            && failure.error.contains(FAIL_AFTER_HOOK_FOR_TESTS)
    }));
    assert!(
        database
            .range(HOOK_DELIVERIES_TABLE, &[], &[0xFF], ReadConsistency::Strong)?
            .is_empty(),
        "expected an empty value, got {:?}",
        database.range(HOOK_DELIVERIES_TABLE, &[], &[0xFF], ReadConsistency::Strong)?
    );
    Ok(())
}

#[test]
fn hook_targets_are_scoped_to_named_objects() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_collection("a", CollectionId::new(1), user_doc_fields(), Vec::new())?;
    database.create_collection("b", CollectionId::new(2), user_doc_fields(), Vec::new())?;
    database
        .create_vector_collection("v1", VectorCollectionConfig::new(CollectionId::new(70), 3))?;
    database
        .create_vector_collection("v2", VectorCollectionConfig::new(CollectionId::new(71), 3))?;
    database.create_graph("g1", GraphId::new(5))?;
    database.create_graph("g2", GraphId::new(6))?;
    for (name, target) in [
        (
            "small-missing",
            HookTarget::Collection("missing".to_owned()),
        ),
        ("small-wrong-kind", HookTarget::Collection("v1".to_owned())),
        ("graph-wrong-kind", HookTarget::Graph("a".to_owned())),
    ] {
        match database.register_hook(&HookSpec::before(
            name,
            target,
            HookAction::MaxValueBytes(1),
            1_000,
        )) {
            Err(DbError::Hook(HookError::InvalidTarget(_))) => {}
            other => panic!("hook {name} with an unknown target must be refused, got {other:?}"),
        }
    }
    for (name, target) in [
        ("small-a", HookTarget::Collection("a".to_owned())),
        ("small-v1", HookTarget::VectorCollection("v1".to_owned())),
    ] {
        database.register_hook(&HookSpec::before(
            name,
            target,
            HookAction::MaxValueBytes(1),
            1_000,
        ))?;
    }
    database.register_hook(&HookSpec::after(
        "g1-after",
        HookTarget::Graph("g1".to_owned()),
        HookAction::RecordAfterCommit,
        1_000,
    ))?;

    let document = user_document(1, Some(Value::Str("Paris".to_owned())));
    database.collection("b")?.insert(&document)?;
    assert!(database.collection("a")?.insert(&document).is_err());
    database
        .vector_collection("v2")?
        .insert_vector(&Value::Null, vec![1.0, 0.0, 0.0])?;
    assert!(
        database
            .vector_collection("v1")?
            .insert_vector(&Value::Null, vec![1.0, 0.0, 0.0])
            .is_err()
    );
    assert_eq!(database.collection("b")?.scan()?.len(), 1);
    assert!(
        database.collection("a")?.scan()?.is_empty(),
        "expected an empty value, got {:?}",
        database.collection("a")?.scan()?
    );

    let edge = |graph: &str| -> Result<(), Box<dyn std::error::Error>> {
        database.graph(graph)?.add_edge(
            GraphNodeId::from_str_id("alice"),
            "knows",
            GraphNodeId::from_str_id("bob"),
            &Value::Null,
        )?;
        Ok(())
    };
    edge("g2")?;
    assert!(
        database
            .range(HOOK_DELIVERIES_TABLE, &[], &[0xFF], ReadConsistency::Strong)?
            .is_empty(),
        "expected an empty value, got {:?}",
        database.range(HOOK_DELIVERIES_TABLE, &[], &[0xFF], ReadConsistency::Strong)?
    );
    edge("g1")?;
    let deliveries =
        database.range(HOOK_DELIVERIES_TABLE, &[], &[0xFF], ReadConsistency::Strong)?;
    assert_eq!(deliveries.len(), 1);
    assert!(String::from_utf8_lossy(&deliveries[0].1).contains("g1-after"));
    Ok(())
}

#[tokio::test]
async fn procedure_writes_run_through_write_pipeline() -> Result<(), Box<dyn std::error::Error>> {
    let database = create_database(DbConfig::new(Profile::InMemory))?;
    database.register_hook(&HookSpec::before(
        "non-empty-kv",
        HookTarget::Keyspace("kv".to_owned()),
        HookAction::RejectEmptyValue,
        1_000,
    ))?;
    let command = Value::Object(BTreeMap::from([
        ("action".to_owned(), Value::Str("put".to_owned())),
        ("table".to_owned(), Value::Str("kv".to_owned())),
        ("key".to_owned(), Value::Str("k".to_owned())),
        ("value".to_owned(), Value::Str(String::new())),
    ]));
    let wasm = constant_value_wasm(&Value::Array(vec![command]))?;
    database.register_wasm_procedure(
        ProcedureSpec {
            name: "put_empty".to_owned(),
            module_hash: String::new(),
            entry: "udf_call".to_owned(),
            budget: UdfBudget::default(),
            abi: AbiVersion::V1,
        },
        &wasm,
    )?;

    assert_rejected(
        "call_procedure",
        database.call_procedure("put_empty", &[]),
        "hook rejected write",
    );
    assert_rejected(
        "SQL CALL",
        database.query("CALL put_empty()").await,
        "hook rejected write",
    );
    assert_eq!(database.read("kv", b"k", ReadConsistency::Strong)?, None);
    Ok(())
}

fn account_row(id: i64, name: &str, age: i64) -> Row {
    vec![Value::Int(id), Value::Str(name.to_owned()), Value::Int(age)]
}

fn register_accounts_trigger(
    database: &Database,
    name: &str,
    timing: TriggerTiming,
    event: TriggerEvent,
    response: &Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let wasm = constant_value_wasm(response)?;
    database.register_wasm_trigger(wasm_trigger(name, "accounts", timing, event), &wasm)?;
    Ok(())
}

fn assert_rejected<T: std::fmt::Debug, E: std::fmt::Display>(
    context: &str,
    result: Result<T, E>,
    expected: &str,
) {
    match result {
        Ok(value) => panic!("{context}: expected an error containing {expected:?}, got {value:?}"),
        Err(error) => assert!(
            error.to_string().contains(expected),
            "{context}: expected an error containing {expected:?}, got {error}"
        ),
    }
}

fn rel_index_entries(database: &Database) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
    database.range(REL_INDEX_TABLE, &[], &[0xFF], ReadConsistency::Strong)
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn order_schema() -> TableSchema {
    TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("user_id", ColumnType::Int, false),
            ColumnDef::new("amount", ColumnType::Float, false),
        ],
        0,
    )
}

fn account_schema() -> TableSchema {
    TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("name", ColumnType::Str, false),
            ColumnDef::new("age", ColumnType::Int, false),
        ],
        0,
    )
}

fn counter_schema() -> TableSchema {
    TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("value", ColumnType::Int, false),
        ],
        0,
    )
}

fn sales_schema() -> TableSchema {
    TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("category", ColumnType::Str, false),
            ColumnDef::new("price", ColumnType::Float, false),
        ],
        0,
    )
}

fn sales_row(id: i64) -> Row {
    vec![
        Value::Int(id),
        Value::Str(format!("cat{}", id % 4)),
        Value::Float(10.0),
    ]
}

fn analytical_sales_database(
    path: std::path::PathBuf,
    rows: i64,
) -> Result<Database, Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::on_disk(Profile::Analytical, path))?;
    let sales = database.create_table("sales", Some(sales_schema()), Vec::new())?;
    database.propose_batch(sales.columnar_replace_ops((0..rows).map(sales_row).collect())?)?;
    Ok(database)
}

fn user_doc_fields() -> Vec<DocField> {
    vec![
        DocField::path("user_id", FieldPath::new(["user_id"]), ColumnType::Int),
        DocField::path("city", city_path(), ColumnType::Str),
    ]
}

fn constant_value_wasm(value: &Value) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let encoded = crate::model::encode_value(value)?;
    let bytes = wat_data_bytes(&encoded);
    let len = encoded.len();
    let wat = format!(
        r#"
            (module
              (memory (export "memory") 1)
              (data (i32.const 4096) "{bytes}")
              (func (export "udf_call") (param i32 i32) (result i64)
                (i64.or
                  (i64.shl (i64.const 4096) (i64.const 32))
                  (i64.const {len}))))
            "#
    );
    Ok(wat::parse_str(wat)?)
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(hex_digit(byte >> 4));
        out.push(hex_digit(byte & 0x0F));
    }
    out
}

fn wat_data_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 3);
    for byte in bytes {
        out.push('\\');
        out.push(hex_digit(byte >> 4));
        out.push(hex_digit(byte & 0x0F));
    }
    out
}

fn hex_digit(value: u8) -> char {
    char::from(b"0123456789abcdef"[usize::from(value)])
}

fn cp_cluster_config() -> CpClusterConfig {
    CpClusterConfig::new(
        1,
        "127.0.0.1:7101",
        vec![
            RaftNode::new(1, "127.0.0.1:7101"),
            RaftNode::new(2, "127.0.0.1:7102"),
            RaftNode::new(3, "127.0.0.1:7103"),
        ],
    )
    .with_transport(InternalTransportConfig::new(
        "127.0.0.1:7101",
        InternalTransportSecurity::PlaintextForTests,
    ))
}

fn ap_cluster_config() -> ApClusterConfig {
    ApClusterConfig::single_node_for_tests(1)
}

fn ap_database() -> Result<Database, ConfigError> {
    create_ap_database(
        DbConfig::new(Profile::InMemory).with_replication(ReplicationKind::Ap),
        ap_cluster_config(),
    )
}

fn insert_located_post(database: &mut Database) -> Result<(), Box<dyn std::error::Error>> {
    let posts = database.create_collection(
        "posts",
        CollectionId::new(190),
        vec![DocField::path(
            "body",
            FieldPath::new(["body"]),
            ColumnType::Str,
        )],
        Vec::new(),
    )?;
    posts.insert(&Value::Object(BTreeMap::from([
        (
            "body".to_owned(),
            Value::Str("rust database search storage".to_owned()),
        ),
        (
            "point".to_owned(),
            Value::GeoPoint {
                lon: 21.0122,
                lat: 52.2297,
            },
        ),
    ])))?;
    Ok(())
}

fn posts_text_index() -> FullTextIndexConfig {
    FullTextIndexConfig::collection(
        "posts_text",
        CollectionId::new(190),
        FieldPath::new(["body"]),
    )
}

fn posts_geo_index() -> GeoIndexConfig {
    GeoIndexConfig::new(
        "posts_geo",
        CollectionId::new(190),
        FieldPath::new(["point"]),
    )
}

fn sharded_config() -> ShardedDatabaseConfig {
    ShardedDatabaseConfig::new(
        PartitionStrategy::HashSlots { partitions: 16 },
        1,
        vec![
            ShardSpec::new(
                1,
                ShardBackendConfig::Local(DbConfig::new(Profile::InMemory)),
            ),
            ShardSpec::new(
                2,
                ShardBackendConfig::Local(DbConfig::new(Profile::InMemory)),
            ),
        ],
    )
}

fn on_disk_sharded_config(dir: &std::path::Path) -> ShardedDatabaseConfig {
    ShardedDatabaseConfig::new(
        PartitionStrategy::HashSlots { partitions: 16 },
        1,
        vec![
            ShardSpec::new(
                1,
                ShardBackendConfig::Local(DbConfig::on_disk(
                    Profile::Balanced,
                    dir.join("s1.redb"),
                )),
            ),
            ShardSpec::new(
                2,
                ShardBackendConfig::Local(DbConfig::on_disk(
                    Profile::Balanced,
                    dir.join("s2.redb"),
                )),
            ),
        ],
    )
}

fn security_for_alice_users_reader() -> SecurityConfig {
    let policy = AuthzPolicy::new([
        Role::new("reader").grant(Resource::Table("users".to_owned()), Permission::Read)
    ]);
    let mut principals = PrincipalRegistry::new();
    principals.insert("alice", Principal::new("alice").with_role("reader"));

    SecurityConfig {
        authz_policy: policy,
        principals,
        audit_enabled: true,
        audit: super::AuditConfig::default(),
    }
}

/// Bootstraps the admin identity, then persists a reader role and an alice mapping as admin.
fn persist_reader_security(database: &mut Database) -> Result<(), Box<dyn std::error::Error>> {
    database.ensure_bootstrap_admin_principal()?;
    let admin = Principal::new("admin").with_role("admin");
    let stored_admin = database
        .authz_policy()
        .roles()
        .get("admin")
        .cloned()
        .ok_or("missing bootstrap admin role")?;
    database.set_authz_policy_as(
        &admin,
        AuthzPolicy::new([
            stored_admin,
            Role::new("reader").grant(Resource::Table("users".to_owned()), Permission::Read),
        ]),
    )?;
    database.set_principal_registry_as(
        &admin,
        PrincipalRegistry::new()
            .with_principal("admin", admin.clone())
            .with_principal("alice", Principal::new("alice").with_role("reader")),
    )?;
    Ok(())
}

fn assert_reader_security_restored(database: &Database) {
    assert!(database.authz_policy().roles().contains_key("reader"));
    assert_eq!(
        database.principal_for_user("alice"),
        Principal::new("alice").with_role("reader")
    );
    assert_eq!(
        database.principal_for_user("admin"),
        Principal::new("admin").with_role("admin")
    );
}

fn assert_reserved_put_rejected(database: &Database, table: &str) {
    let error = match database.propose(Op::Put {
        table: table.to_owned(),
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    }) {
        Ok(()) => panic!("public reserved write should fail for {table}"),
        Err(error) => error,
    };
    assert_reserved_error(error);
}

fn assert_reserved_condition_rejected(database: &Database, table: &str) {
    let error = match database.propose_conditional_batch(ConditionalBatch::new(
        vec![WriteCondition::ValueEquals {
            table: table.to_owned(),
            key: b"k".to_vec(),
            expected: None,
        }],
        vec![Op::Put {
            table: "public".to_owned(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        }],
    )) {
        Ok(()) => panic!("public reserved condition should fail for {table}"),
        Err(error) => error,
    };
    assert_reserved_error(error);
}

fn assert_reserved_error(error: ReplError) {
    assert!(matches!(
        error,
        ReplError::Storage(StorageError::Backend(message))
            if message.contains("reserved keyspace")
    ));
}

fn latest_lsn(database: &Database) -> Result<u64, Box<dyn std::error::Error>> {
    let records = database.range(txn::COMMIT_LOG_TABLE, &[], &[], ReadConsistency::Strong)?;
    let lsn = records
        .into_iter()
        .map(|(_, value)| txn::decode_commit_log_record(&value).map(|record| record.txn_id))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .ok_or("missing commit log")?;
    Ok(lsn)
}

fn workload_executions(
    database: &Database,
    fingerprint: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    let entry = database
        .workload_report(WorkloadWindow::All)?
        .entries
        .into_iter()
        .find(|entry| entry.fingerprint.0 == fingerprint)
        .ok_or("missing workload entry")?;
    Ok(entry.executions)
}

fn persisted_workload_executions(
    database: &Database,
    fingerprint: &str,
) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    let Some(bytes) = database.read(
        WORKLOAD_TABLE,
        fingerprint.as_bytes(),
        ReadConsistency::Strong,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(
        serde_json::from_slice::<WorkloadEntry>(&bytes)?.executions,
    ))
}

fn seed_orders(database: &mut Database) -> Result<(), Box<dyn std::error::Error>> {
    let users = database.create_table("users", Some(account_schema()), Vec::new())?;
    let orders = database.create_table("orders", Some(order_schema()), Vec::new())?;

    users.insert(vec![
        Value::Int(1),
        Value::Str("Ada".to_owned()),
        Value::Int(37),
    ])?;
    users.insert(vec![
        Value::Int(2),
        Value::Str("Grace".to_owned()),
        Value::Int(85),
    ])?;
    orders.insert(vec![Value::Int(10), Value::Int(1), Value::Float(5.5)])?;
    orders.insert(vec![Value::Int(11), Value::Int(1), Value::Float(7.0)])?;
    orders.insert(vec![Value::Int(12), Value::Int(2), Value::Float(3.0)])?;

    Ok(())
}

fn city_path() -> FieldPath {
    FieldPath::new(["profile", "city"])
}

fn user_document(user_id: i64, city: Option<Value>) -> Value {
    let mut profile = BTreeMap::new();
    if let Some(city) = city {
        profile.insert("city".to_owned(), city);
    }

    let mut doc = BTreeMap::new();
    doc.insert("user_id".to_owned(), Value::Int(user_id));
    doc.insert("profile".to_owned(), Value::Object(profile));
    Value::Object(doc)
}

#[test]
fn profile_manifest_create_writes_unframed_metadata_once() -> Result<(), Box<dyn std::error::Error>>
{
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("balanced-profile.redb");
    let binding = named_binding("balanced", json!({}), ReplicationKind::Cp, Some(&path))?;
    assert!(!binding.manifest.storage_format.value_framing);

    let database = create_database_with_profile(&binding, OperationalConfig::new())?;
    assert_eq!(database.profile(), Profile::Balanced);
    assert_eq!(database.engine_kind(), EngineKind::Redb);
    drop(database);

    let schema_version = raw_value(&path, META_TABLE, KEY_SCHEMA_VERSION)?;
    assert_eq!(schema_version.as_deref(), Some(b"1".as_slice()));
    let manifest = raw_value(&path, META_TABLE, KEY_PROFILE_MANIFEST)?.ok_or("manifest missing")?;
    assert!(manifest.starts_with(b"{"));
    let stored: PersistedProfileManifest = serde_json::from_slice(&manifest)?;
    assert_eq!(stored, binding.manifest);
    assert_eq!(stored.manifest_version, 1);
    assert_eq!(stored.written_by, env!("CARGO_PKG_VERSION"));

    assert!(matches!(
        create_database_with_profile(&binding, OperationalConfig::new()),
        Err(ConfigError::AlreadyInitialized)
    ));

    let other = temp_dir.path().join("with-performance.redb");
    let other_binding = named_binding("balanced", json!({}), ReplicationKind::Cp, Some(&other))?;
    let ops = OperationalConfig::new()
        .with_performance(PerformanceConfig::for_profile(Profile::Balanced));
    assert!(matches!(
        create_database_with_profile(&other_binding, ops),
        Err(ConfigError::Unsupported(_))
    ));
    assert!(!other.exists());
    Ok(())
}

#[test]
fn profile_manifest_knobs_are_reapplied_by_every_single_node_opener()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("tuned.redb");
    let binding = named_binding(
        "balanced",
        json!({
            "cache": { "max_entries": 8192 },
            "query": { "max_concurrent_queries": 3, "memory_limit_bytes": 134_217_728 },
            "durability": { "group_commit_window_ms": 5 }
        }),
        ReplicationKind::Cp,
        Some(&path),
    )?;
    assert_eq!(binding.manifest.kind, ResolvedProfileKind::Custom);
    assert_eq!(binding.performance.cache.max_capacity, 8192);
    assert_eq!(binding.performance.query.max_concurrent_queries, 3);
    assert_eq!(binding.performance.io.group_commit_window_ms, 5);
    assert_eq!(binding.performance.query.memory_limit_bytes, 134_217_728);
    assert_ne!(
        binding.performance,
        PerformanceConfig::for_profile(Profile::Balanced)
    );

    let admin = Principal::new("root").with_role("admin");
    let admin_role = Role::new("admin").grant(Resource::System, Permission::Admin);
    let mut bootstrap = PrincipalRegistry::new();
    bootstrap.insert("root", admin.clone());
    let security = SecurityConfig {
        authz_policy: AuthzPolicy::new([admin_role.clone()]),
        principals: bootstrap,
        ..SecurityConfig::default()
    };
    let policy = AuthzPolicy::new([
        admin_role,
        Role::new("reader").grant(Resource::Table("users".to_owned()), Permission::Read),
    ]);
    let mut registry = PrincipalRegistry::new();
    registry.insert("root", admin.clone());
    registry.insert("alice", Principal::new("alice").with_role("reader"));

    {
        let mut database = create_database_with_profile(
            &binding,
            OperationalConfig::new().with_security(security),
        )?;
        assert_eq!(database.performance_config(), &binding.performance);
        assert_eq!(database.query_permits.available_permits(), 3);
        database.set_authz_policy_as(&admin, policy.clone())?;
        database.set_principal_registry_as(&admin, registry.clone())?;
    }
    let authz_before = raw_rows(&path, AUTHZ_TABLE)?;
    assert!(!authz_before.is_empty(), "expected a non-empty value");

    let config = DbConfig::on_disk(Profile::Balanced, &path);
    {
        let database = create_database(config.clone())?;
        assert_eq!(database.performance_config(), &binding.performance);
        assert_eq!(database.query_permits.available_permits(), 3);
    }
    {
        let database = open_database(config.clone())?;
        assert_eq!(database.performance_config(), &binding.performance);
        assert_eq!(database.query_permits.available_permits(), 3);
    }
    assert_eq!(raw_rows(&path, AUTHZ_TABLE)?, authz_before);

    let database = open_database_with_ops(config, &OperationalConfig::new())?;
    assert_eq!(database.performance_config(), &binding.performance);
    assert_eq!(database.query_permits.available_permits(), 3);
    assert_eq!(database.authz_policy(), &policy);
    assert_eq!(database.principal_registry(), &registry);
    drop(database);
    assert_eq!(raw_rows(&path, AUTHZ_TABLE)?, authz_before);
    Ok(())
}

#[test]
fn profile_manifest_compression_frames_every_value() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("zstd.redb");
    let binding = named_binding(
        "balanced",
        json!({ "compression": { "algorithm": "zstd" } }),
        ReplicationKind::Cp,
        Some(&path),
    )?;
    assert!(binding.manifest.storage_format.value_framing);
    let value = b"framed value ".repeat(256);

    {
        let database = create_database_with_profile(&binding, OperationalConfig::new())?;
        assert_eq!(database.engine_kind(), EngineKind::CompressedRedb);
        database.propose(Op::Put {
            table: "t".to_owned(),
            key: b"k".to_vec(),
            value: value.clone(),
        })?;
    }

    let schema_version = raw_value(&path, META_TABLE, KEY_SCHEMA_VERSION)?.ok_or("no version")?;
    assert!(schema_version.starts_with(b"MDBC1"));
    assert_eq!(decode_stored_value(&schema_version)?, b"2");
    let manifest = raw_value(&path, META_TABLE, KEY_PROFILE_MANIFEST)?.ok_or("no manifest")?;
    assert!(manifest.starts_with(b"MDBC1"));
    let data = raw_value(&path, "t", b"k")?.ok_or("data key missing")?;
    assert!(data.starts_with(b"MDBC1"));
    assert_eq!(data[5], 2, "values at or above min_bytes are zstd frames");
    assert!(data.len() < value.len());

    let config = DbConfig::on_disk(Profile::Balanced, &path);
    {
        let database = open_database(config.clone())?;
        assert_eq!(database.engine_kind(), EngineKind::CompressedRedb);
        assert_eq!(
            database.read("t", b"k", ReadConsistency::Strong)?,
            Some(value.clone())
        );
        assert_eq!(
            database.performance_config().compression.algorithm,
            CompressionAlgorithm::Zstd
        );
        assert_eq!(database.performance_config(), &binding.performance);
    }

    let mut lz4 = PerformanceConfig::for_profile(Profile::Balanced);
    lz4.compression.algorithm = CompressionAlgorithm::Lz4;
    let database = open_database_with_ops(config, &OperationalConfig::new().with_performance(lz4))?;
    assert_eq!(database.engine_kind(), EngineKind::CompressedRedb);
    assert_eq!(
        database.read("t", b"k", ReadConsistency::Strong)?,
        Some(value)
    );
    assert_eq!(
        database.performance_config().compression.algorithm,
        CompressionAlgorithm::Lz4
    );
    Ok(())
}

#[test]
fn profile_manifest_framing_without_compression_writes_raw_frames()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("raw-frames.redb");
    let mut binding = named_binding("balanced", json!({}), ReplicationKind::Cp, Some(&path))?;
    binding.manifest.storage_format.value_framing = true;

    {
        let database = create_database_with_profile(&binding, OperationalConfig::new())?;
        assert_eq!(database.engine_kind(), EngineKind::CompressedRedb);
        database.propose(Op::Put {
            table: "t".to_owned(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        })?;
    }

    let data = raw_value(&path, "t", b"k")?.ok_or("data key missing")?;
    assert!(data.starts_with(b"MDBC1"));
    assert_eq!(data[5], 0, "algorithm none writes RAW frames");
    let schema_version = raw_value(&path, META_TABLE, KEY_SCHEMA_VERSION)?.ok_or("no version")?;
    assert_eq!(decode_stored_value(&schema_version)?, b"2");

    let database = open_database(DbConfig::on_disk(Profile::Balanced, &path))?;
    assert_eq!(database.engine_kind(), EngineKind::CompressedRedb);
    assert_eq!(
        database.read("t", b"k", ReadConsistency::Strong)?,
        Some(b"v".to_vec())
    );
    Ok(())
}

/// A profile-manifest database (the example custom profile: `HighDurability`, zstd and
/// `floors.audit`) with a tenant quota runs every user write through the write pipeline. Table
/// handles, transactions, SQL, `query_as` and before-trigger replacements store compressed
/// frames and read back decoded, also through the changefeed; after-trigger failures and audit
/// events are recorded framed; and the tenant quota charges the decoded bytes, also after a
/// reopen in which the persisted manifest supplies the performance.
#[tokio::test]
async fn profile_manifest_database_runs_user_writes_through_the_write_pipeline()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("orders.redb");
    let spec = decode_spec_bytes(
        include_str!("../../examples/custom-profile/multidb.yaml").as_bytes(),
        SpecFormat::Yaml,
    )?;
    let compiled = PolicyCompiler::compile(&spec).map_err(|report| format!("{report:?}"))?;
    let binding = compiled.binding(&spec.deployment, Some(path.clone()))?;
    assert_eq!(binding.db_config.profile, Profile::HighDurability);
    assert!(binding.manifest.storage_format.value_framing);
    assert!(binding.manifest.knobs.floors.audit);
    let tenant = || {
        OperationalConfig::new().with_tenant(TenantConfig::new(
            "tenant-a",
            TenantQuota::storage_bytes(16_384),
        ))
    };
    let name = "n".repeat(600);
    let large = "x".repeat(32_768);

    let mut database = create_database_with_profile(&binding, tenant())?;
    assert_eq!(database.engine_kind(), EngineKind::CompressedRedb);
    write_accounts_through_every_pipeline_path(&mut database, &name, &large).await?;
    drop(database);
    assert_pipeline_values_framed(&path, &name)?;

    let reopened =
        open_database_with_ops(DbConfig::on_disk(Profile::HighDurability, &path), &tenant())?;
    assert_eq!(reopened.performance_config(), &binding.performance);
    assert!(reopened.audit_enabled());
    assert_eq!(
        reopened.table("accounts")?.get(&Value::Int(5))?,
        Some(account_row(5, &name, 55))
    );
    let (events, _) = reopened.poll_changefeed(
        &ResumeToken::default(),
        &ChangefeedFilter {
            target: ChangefeedTarget::Table("accounts".to_owned()),
        },
        &ChangefeedOptions::default(),
        100,
    )?;
    let upserts = events
        .iter()
        .filter_map(|event| match &event.op {
            ChangeOp::Upsert { value_after, .. } => Some(value_after),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(upserts.len(), 5);
    assert!(
        upserts
            .iter()
            .all(|value| contains_bytes(value, name.as_bytes()))
    );
    assert_eq!(reopened.post_commit_failures()?.len(), 5);
    assert!(matches!(
        reopened.propose(Op::Put {
            table: "user_data".to_owned(),
            key: b"large".to_vec(),
            value: large.into_bytes(),
        }),
        Err(ReplError::QuotaExceeded { .. })
    ));
    Ok(())
}

/// Writes accounts 1 to 5 through a table handle, a transaction, SQL, `query_as` and a SQL
/// insert that a before trigger replaces, with an after trigger that fails on every insert;
/// a transaction and a SQL insert of `large` exceed the tenant quota and write nothing.
async fn write_accounts_through_every_pipeline_path(
    database: &mut Database,
    name: &str,
    large: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    database.create_table("accounts", Some(account_schema()), Vec::new())?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("writer").grant(Resource::Table("accounts".to_owned()), Permission::Write)
    ]));
    register_accounts_trigger(
        database,
        "reject_after_accounts_insert",
        TriggerTiming::After,
        TriggerEvent::Insert,
        &trigger_response("reject", None),
    )?;

    database
        .table("accounts")?
        .insert(account_row(1, name, 1))?;
    database.transaction(|txn| txn.insert_row("accounts", account_row(2, name, 2)))?;
    database
        .query(&format!(
            "INSERT INTO accounts (id, name, age) VALUES (3, '{name}', 3)"
        ))
        .await?;
    database
        .query_as(
            &Principal::new("writer").with_role("writer"),
            &format!("INSERT INTO accounts (id, name, age) VALUES (4, '{name}', 4)"),
        )
        .await?;

    match database.transaction(|txn| txn.insert_row("accounts", account_row(6, large, 6))) {
        Err(DbError::Repl(ReplError::QuotaExceeded { .. })) => {}
        other => panic!("transactions must charge the tenant quota, got {other:?}"),
    }
    assert_rejected(
        "SQL over the tenant quota",
        database
            .query(&format!(
                "INSERT INTO accounts (id, name, age) VALUES (7, '{large}', 7)"
            ))
            .await,
        "quota exceeded for tenant",
    );

    register_accounts_trigger(
        database,
        "replace_accounts_insert",
        TriggerTiming::Before,
        TriggerEvent::Insert,
        &trigger_response("replace", Some(Value::Array(account_row(5, name, 55)))),
    )?;
    database
        .query("INSERT INTO accounts (id, name, age) VALUES (5, 'short', 5)")
        .await?;

    assert_eq!(database.table("accounts")?.scan()?.len(), 5);
    let failures = database.post_commit_failures()?;
    assert_eq!(failures.len(), 5, "{failures:?}");
    assert!(
        failures
            .iter()
            .all(|failure| failure.stage == PostCommitStage::AfterTrigger)
    );
    assert!(
        database
            .audit_events()?
            .iter()
            .any(|event| { event.action == "query" && event.outcome == AuditOutcome::Succeeded })
    );
    Ok(())
}

/// Every value the write pipeline stored at `path` is a compression frame, and rows and commit
/// records holding `name` are compressed.
fn assert_pipeline_values_framed(
    path: &Path,
    name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for table in [
        REL_ROWS_TABLE,
        txn::TXN_MVCC_TABLE,
        txn::COMMIT_LOG_TABLE,
        crate::cdc::POST_COMMIT_FAILURES_TABLE,
        AUDIT_TABLE,
    ] {
        let rows = raw_rows(path, table)?;
        assert!(!rows.is_empty(), "{table} is empty");
        assert!(
            rows.iter().all(|(_, value)| value.starts_with(b"MDBC1")),
            "{table} holds an unframed value"
        );
    }
    for table in [REL_ROWS_TABLE, txn::COMMIT_LOG_TABLE] {
        assert!(
            raw_rows(path, table)?
                .iter()
                .all(|(_, value)| !contains_bytes(value, name.as_bytes())),
            "{table} holds an uncompressed row"
        );
    }
    Ok(())
}

#[test]
fn legacy_framed_database_needs_ops_compression() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("legacy-zstd.redb");
    let config = DbConfig::on_disk(Profile::Transactional, &path);
    let mut performance = PerformanceConfig::for_profile(Profile::Transactional);
    performance.compression.algorithm = CompressionAlgorithm::Zstd;
    performance.compression.min_bytes = 1;
    let ops = OperationalConfig::new().with_performance(performance);

    {
        let database = create_database_with_ops(config.clone(), ops.clone())?;
        database.propose(Op::Put {
            table: "t".to_owned(),
            key: b"k".to_vec(),
            value: b"legacy".to_vec(),
        })?;
    }

    match open_database(config.clone()) {
        Err(ConfigError::StorageFormatMismatch(message)) => {
            assert!(message.contains("OperationalConfig.performance.compression"));
        }
        Err(error) => panic!("expected a storage format mismatch, got {error:?}"),
        Ok(_) => panic!("a framed legacy database opened without compression"),
    }
    // refuse_profile_manifest_reports_single_node_only pins the framed refusal of the CP and AP
    // openers directly; a CP constructor may refuse the cluster before it reads metadata.
    assert!(matches!(
        open_cluster_database(config.clone(), cp_cluster_config()),
        Err(ConfigError::StorageFormatMismatch(_) | ConfigError::Unsupported(_))
    ));
    let ap_config = DbConfig::on_disk(Profile::Transactional, temp_dir.path().join("ap.redb"))
        .with_replication(ReplicationKind::Ap);
    drop(create_database_with_ops(ap_config.clone(), ops.clone())?);
    assert!(matches!(
        open_ap_database(ap_config, ap_cluster_config()),
        Err(ConfigError::StorageFormatMismatch(_))
    ));

    let database = open_database_with_ops(config, &ops)?;
    assert_eq!(database.engine_kind(), EngineKind::CompressedRedb);
    assert_eq!(
        database.read("t", b"k", ReadConsistency::Strong)?,
        Some(b"legacy".to_vec())
    );
    assert!(matches!(
        inspect_database_metadata(&path),
        MetadataProbe::Locked
    ));
    drop(database);
    match inspect_database_metadata(&path) {
        MetadataProbe::Initialized(probed) => {
            assert!(probed.values_framed);
            assert_eq!(probed.schema_version, 1);
            assert!(probed.manifest.is_none());
        }
        other => panic!("expected initialized metadata, got {other:?}"),
    }
    Ok(())
}

#[test]
fn unframed_profile_manifest_refuses_ops_compression() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("unframed.redb");
    let binding = named_binding("balanced", json!({}), ReplicationKind::Cp, Some(&path))?;
    drop(create_database_with_profile(
        &binding,
        OperationalConfig::new(),
    )?);

    let mut performance = PerformanceConfig::for_profile(Profile::Balanced);
    performance.compression.algorithm = CompressionAlgorithm::Lz4;
    match open_database_with_ops(
        DbConfig::on_disk(Profile::Balanced, &path),
        &OperationalConfig::new().with_performance(performance),
    ) {
        Err(ConfigError::StorageFormatMismatch(message)) => {
            assert!(message.contains("provisioning format upgrade"));
        }
        Err(error) => panic!("expected a storage format mismatch, got {error:?}"),
        Ok(_) => panic!("ops compression silently framed an unframed manifest database"),
    }
    Ok(())
}

#[test]
fn metadata_reader_rejects_unknown_formats() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;

    let newer = temp_dir.path().join("schema-3.redb");
    drop(create_database(DbConfig::on_disk(
        Profile::Balanced,
        &newer,
    ))?);
    put_raw(&newer, META_TABLE, KEY_SCHEMA_VERSION, b"3")?;
    assert!(matches!(
        open_database(DbConfig::on_disk(Profile::Balanced, &newer)),
        Err(ConfigError::SchemaVersionMismatch {
            expected: 2,
            found: 3
        })
    ));

    let framed_without_manifest = temp_dir.path().join("schema-2.redb");
    drop(create_database(DbConfig::on_disk(
        Profile::Balanced,
        &framed_without_manifest,
    ))?);
    put_raw(
        &framed_without_manifest,
        META_TABLE,
        KEY_SCHEMA_VERSION,
        b"2",
    )?;
    assert!(matches!(
        open_database(DbConfig::on_disk(
            Profile::Balanced,
            &framed_without_manifest
        )),
        Err(ConfigError::ProfileManifestInvalid(_))
    ));

    let newer_manifest = manifest_database(temp_dir.path(), "manifest-2.redb")?;
    edit_raw_manifest(&newer_manifest, |manifest| {
        manifest["manifest_version"] = json!(2);
    })?;
    assert!(matches!(
        open_database(DbConfig::on_disk(Profile::Balanced, &newer_manifest)),
        Err(ConfigError::UnsupportedProfileManifest { found: 2 })
    ));
    Ok(())
}

#[test]
fn metadata_reader_revalidates_profile_manifests() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let edits: [(&str, ManifestEdit); 4] = [
        ("storage-profile.redb", |manifest| {
            manifest["storage_profile"] = json!("Document");
        }),
        ("out-of-range.redb", |manifest| {
            manifest["knobs"]["cache"]["max_entries"] = json!(2_000_000);
        }),
        ("derived-class.redb", |manifest| {
            manifest["knobs"]["storage"]["layout"] = json!("columnar");
        }),
        ("unknown-field.redb", |manifest| {
            manifest["knobs"]["cache"]["ttl_millis"] = json!(1);
        }),
    ];

    for (name, edit) in edits {
        let path = manifest_database(temp_dir.path(), name)?;
        edit_raw_manifest(&path, edit)?;
        match open_database(DbConfig::on_disk(Profile::Balanced, &path)) {
            Err(ConfigError::ProfileManifestInvalid(_)) => {}
            Err(error) => panic!("{name}: expected an invalid manifest, got {error:?}"),
            Ok(_) => panic!("{name}: an edited manifest drove the runtime"),
        }
        assert!(matches!(
            inspect_database_metadata(&path),
            MetadataProbe::Unreadable { .. }
        ));
    }

    // In-range values the resolver still refuses: a planned limit and two knob conflicts.
    let refused: [(&str, ManifestEdit, &str); 3] = [
        (
            "planned-limit.redb",
            |manifest| manifest["knobs"]["limits"]["max_value_bytes"] = json!(4_096),
            "planned but not enforced",
        ),
        (
            "cache-conflict.redb",
            |manifest| manifest["knobs"]["cache"]["max_entries"] = json!(0),
            "cache.max_entries is 0",
        ),
        (
            "retention-conflict.redb",
            |manifest| manifest["knobs"]["time_series"]["default_retention_millis"] = json!(1_000),
            "shorter than",
        ),
    ];
    for (name, edit, expected) in refused {
        let path = manifest_database(temp_dir.path(), name)?;
        edit_raw_manifest(&path, edit)?;
        match open_database(DbConfig::on_disk(Profile::Balanced, &path)) {
            Err(ConfigError::ProfileManifestInvalid(message)) => {
                assert!(message.contains(expected), "{name}: {message}");
            }
            Err(error) => panic!("{name}: expected an invalid manifest, got {error:?}"),
            Ok(_) => panic!("{name}: a refused knob drove the runtime"),
        }
    }
    Ok(())
}

#[test]
fn encrypted_metadata_without_key_is_unreadable() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("encrypted.redb");
    let key_path = temp_dir.path().join("key.bin");
    std::fs::write(&key_path, [5_u8; 32])?;
    drop(create_database_with_ops(
        DbConfig::on_disk(Profile::Transactional, &path),
        OperationalConfig::new().with_encryption(EncryptionConfig::file_key(&key_path)),
    )?);

    match open_database(DbConfig::on_disk(Profile::Transactional, &path)) {
        Err(error @ ConfigError::MetadataUnreadable { key, .. }) => {
            assert_eq!(key, "schema_version");
            let message = error.to_string();
            assert!(message.contains("MDBE"), "{message}");
            assert!(message.contains("supply OperationalConfig.encryption"));
        }
        Err(error) => panic!("expected unreadable metadata, got {error:?}"),
        Ok(_) => panic!("encrypted metadata opened without a key"),
    }

    match inspect_database_metadata(&path) {
        MetadataProbe::Unreadable { reason } => {
            assert!(
                reason.contains("metadata is encrypted or corrupt"),
                "{reason}"
            );
            assert!(reason.contains("supply OperationalConfig.encryption"));
        }
        other => panic!("expected unreadable metadata, got {other:?}"),
    }
    Ok(())
}

#[test]
fn encrypted_compressed_profile_manifest_reopens_only_with_its_key()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("encrypted-zstd.redb");
    let key_path = temp_dir.path().join("key.bin");
    std::fs::write(&key_path, [7_u8; 32])?;
    let encrypted = OperationalConfig::new().with_encryption(EncryptionConfig::file_key(&key_path));
    let binding = named_binding(
        "balanced",
        json!({ "compression": { "algorithm": "zstd" }, "cache": { "max_entries": 8192 } }),
        ReplicationKind::Cp,
        Some(&path),
    )?;
    assert!(binding.manifest.storage_format.value_framing);
    let value = b"encrypted framed value ".repeat(128);

    {
        let database = create_database_with_profile(&binding, encrypted.clone())?;
        assert_eq!(database.engine_kind(), EngineKind::CompressedEncryptedRedb);
        database.propose(Op::Put {
            table: "t".to_owned(),
            key: b"k".to_vec(),
            value: value.clone(),
        })?;
    }

    // Values are compressed, then encrypted: on disk every value is an encryption frame.
    for (table, key) in [
        (META_TABLE, KEY_SCHEMA_VERSION),
        (META_TABLE, KEY_PROFILE_MANIFEST),
        ("t", b"k".as_slice()),
    ] {
        let raw = raw_value(&path, table, key)?.ok_or("value missing")?;
        assert!(raw.starts_with(b"MDBE"), "{table} is not encrypted");
    }

    let config = DbConfig::on_disk(Profile::Balanced, &path);
    {
        // No OperationalConfig.performance: the manifest supplies compression and the cache.
        let database = open_database_with_ops(config.clone(), &encrypted)?;
        assert_eq!(database.engine_kind(), EngineKind::CompressedEncryptedRedb);
        assert_eq!(
            database.read("t", b"k", ReadConsistency::Strong)?,
            Some(value)
        );
        assert_eq!(database.performance_config(), &binding.performance);
        assert_eq!(database.performance_config().cache.max_capacity, 8192);
        assert_eq!(
            database.performance_config().compression.algorithm,
            CompressionAlgorithm::Zstd
        );
    }

    match open_database(config) {
        Err(ConfigError::MetadataUnreadable { key, reason }) => {
            assert_eq!(key, "schema_version");
            assert!(reason.contains("MDBE"), "{reason}");
        }
        Err(error) => panic!("expected unreadable metadata, got {error:?}"),
        Ok(_) => panic!("an encrypted manifest database opened without a key"),
    }
    match inspect_database_metadata(&path) {
        MetadataProbe::Unreadable { reason } => {
            assert!(
                reason.contains("supply OperationalConfig.encryption"),
                "{reason}"
            );
        }
        other => panic!("expected unreadable metadata, got {other:?}"),
    }
    Ok(())
}

#[test]
fn refuse_profile_manifest_reports_single_node_only() -> Result<(), Box<dyn std::error::Error>> {
    let binding = named_binding(
        "balanced",
        json!({}),
        ReplicationKind::Cp,
        Some(Path::new("unused.redb")),
    )?;
    let read = MetadataRead {
        metadata: Metadata {
            schema_version: 1,
            profile: Profile::Balanced,
            replication: ReplicationKind::Cp,
            layout: TableLayout::Row,
        },
        manifest: Some(binding.manifest),
        values_framed: false,
    };

    match refuse_profile_manifest(&read) {
        Err(ConfigError::Unsupported(message)) => {
            assert_eq!(
                message,
                "profile manifests are single-node only in this build"
            );
        }
        other => panic!("expected the manifest to be refused, got {other:?}"),
    }

    let legacy = MetadataRead {
        manifest: None,
        ..read.clone()
    };
    assert!(refuse_profile_manifest(&legacy).is_ok());
    let framed = MetadataRead {
        values_framed: true,
        ..legacy
    };
    assert!(matches!(
        refuse_profile_manifest(&framed),
        Err(ConfigError::StorageFormatMismatch(_))
    ));
    Ok(())
}

#[test]
fn cluster_and_ap_openers_refuse_profile_manifests() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;

    let cp_path = manifest_database(temp_dir.path(), "cp.redb")?;
    assert!(matches!(
        open_cluster_database(
            DbConfig::on_disk(Profile::Balanced, &cp_path),
            cp_cluster_config()
        ),
        Err(ConfigError::Unsupported(_))
    ));
    assert!(matches!(
        create_cluster_database(
            DbConfig::on_disk(Profile::Balanced, &cp_path),
            cp_cluster_config()
        ),
        Err(ConfigError::Unsupported(_))
    ));
    // The public CP constructors fail closed before reading metadata; the
    // simulated adapter reads it and refuses the manifest itself.
    for result in [
        open_simulated_cluster_database(
            DbConfig::on_disk(Profile::Balanced, &cp_path),
            cp_cluster_config(),
        ),
        create_simulated_cluster_database(
            DbConfig::on_disk(Profile::Balanced, &cp_path),
            cp_cluster_config(),
        ),
    ] {
        match result {
            Err(ConfigError::Unsupported(message)) => {
                assert!(message.contains("single-node only"), "{message}");
            }
            Err(error) => panic!("expected the manifest refusal, got {error:?}"),
            Ok(_) => panic!("a simulated CP opener accepted a profile manifest"),
        }
    }

    let ap_path = temp_dir.path().join("ap.redb");
    let binding = named_binding("balanced", json!({}), ReplicationKind::Ap, Some(&ap_path))?;
    drop(create_database_with_profile(
        &binding,
        OperationalConfig::new(),
    )?);
    let ap_config =
        DbConfig::on_disk(Profile::Balanced, &ap_path).with_replication(ReplicationKind::Ap);
    assert!(matches!(
        open_ap_database(ap_config.clone(), ap_cluster_config()),
        Err(ConfigError::Unsupported(_))
    ));
    assert!(matches!(
        create_ap_database(ap_config.clone(), ap_cluster_config()),
        Err(ConfigError::Unsupported(_))
    ));
    let reopened = open_database(ap_config)?;
    assert_eq!(reopened.replication_kind(), ReplicationKind::Ap);
    Ok(())
}

#[test]
fn sharded_openers_refuse_profile_manifests_on_any_local_shard()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let manifest_shard = manifest_database(temp_dir.path(), "shard.redb")?;
    let local_manifest =
        || ShardBackendConfig::Local(DbConfig::on_disk(Profile::Balanced, &manifest_shard));
    let local_memory = || ShardBackendConfig::Local(DbConfig::new(Profile::InMemory));

    let global = ShardedDatabaseConfig::new(
        PartitionStrategy::HashSlots { partitions: 16 },
        1,
        vec![
            ShardSpec::new(1, local_manifest()),
            ShardSpec::new(2, local_memory()),
        ],
    );
    assert!(matches!(
        open_sharded_database(DbConfig::new(Profile::InMemory), global),
        Err(ConfigError::Unsupported(message)) if message.contains("single-node only")
    ));

    let non_global = ShardedDatabaseConfig::new(
        PartitionStrategy::HashSlots { partitions: 16 },
        1,
        vec![
            ShardSpec::new(1, local_memory()),
            ShardSpec::new(2, local_manifest()),
        ],
    );
    assert!(matches!(
        create_sharded_database(DbConfig::new(Profile::InMemory), non_global),
        Err(ConfigError::Unsupported(message)) if message.contains("single-node only")
    ));
    Ok(())
}

#[test]
fn inspect_database_metadata_never_creates_files() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;

    let missing = temp_dir.path().join("missing.redb");
    assert_eq!(inspect_database_metadata(&missing), MetadataProbe::NotFound);
    assert!(!missing.exists());

    let empty = temp_dir.path().join("empty.redb");
    std::fs::write(&empty, [])?;
    assert_eq!(
        inspect_database_metadata(&empty),
        MetadataProbe::Uninitialized
    );
    assert_eq!(std::fs::metadata(&empty)?.len(), 0);

    let raw = temp_dir.path().join("raw.redb");
    drop(RedbEngine::open(&raw)?);
    assert_eq!(
        inspect_database_metadata(&raw),
        MetadataProbe::Uninitialized
    );

    let legacy = temp_dir.path().join("legacy.redb");
    let database = create_database(
        DbConfig::on_disk(Profile::Analytical, &legacy).with_replication(ReplicationKind::Ap),
    )?;
    assert_eq!(inspect_database_metadata(&legacy), MetadataProbe::Locked);
    drop(database);
    assert_eq!(
        inspect_database_metadata(&legacy),
        MetadataProbe::Initialized(Box::new(ProbedMetadata {
            schema_version: 1,
            storage_profile: Profile::Analytical,
            replication: ReplicationKind::Ap,
            layout: TableLayout::Columnar,
            manifest: None,
            values_framed: false,
            unmet_floors: Vec::new(),
        }))
    );

    let framed = temp_dir.path().join("framed.redb");
    let binding = named_binding(
        "high_durability",
        json!({ "compression": { "algorithm": "lz4" } }),
        ReplicationKind::Cp,
        Some(&framed),
    )?;
    drop(create_database_with_profile(
        &binding,
        OperationalConfig::new(),
    )?);
    assert_eq!(
        inspect_database_metadata(&framed),
        MetadataProbe::Initialized(Box::new(ProbedMetadata {
            schema_version: 2,
            storage_profile: Profile::HighDurability,
            replication: ReplicationKind::Cp,
            layout: TableLayout::Row,
            manifest: Some(binding.manifest.clone()),
            values_framed: true,
            unmet_floors: Vec::new(),
        }))
    );
    let probe = serde_json::to_value(inspect_database_metadata(&framed))?;
    assert_eq!(probe["state"], "initialized");
    assert_eq!(probe["manifest"]["variant"], "high_durability");
    assert_eq!(probe["unmet_floors"], json!([]));
    Ok(())
}

/// The probe opens files read-only: it never writes a file, and a file that needs crash
/// recovery is `NeedsRecovery` instead of being repaired. `recover_database_file` recovers it
/// without touching the database metadata, and leaves a cleanly closed file alone. Unix only:
/// the crash is simulated by copying the file while the database holds it open, and Windows
/// refuses to read a file that redb has locked.
#[cfg(unix)]
#[test]
fn inspect_database_metadata_never_writes_or_repairs() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("live.redb");
    let crashed = temp_dir.path().join("crashed.redb");
    let reopened = temp_dir.path().join("reopened.redb");
    let ap_config =
        DbConfig::on_disk(Profile::Document, &path).with_replication(ReplicationKind::Ap);
    let mut database = create_database(ap_config)?;
    database.create_table("users", Some(account_schema()), Vec::new())?;
    database.table("users")?.insert(vec![
        Value::Int(1),
        Value::Str("Ada".to_owned()),
        Value::Int(37),
    ])?;
    // A copy taken while the database is open is what a crash leaves behind.
    std::fs::copy(&path, &crashed)?;
    std::fs::copy(&path, &reopened)?;
    drop(database);

    let clean = std::fs::read(&path)?;
    assert!(matches!(
        inspect_database_metadata(&path),
        MetadataProbe::Initialized(_)
    ));
    recover_database_file(&path)?;
    assert_eq!(std::fs::read(&path)?, clean);

    let before = std::fs::read(&crashed)?;
    assert_eq!(
        inspect_database_metadata(&crashed),
        MetadataProbe::NeedsRecovery
    );
    assert_eq!(
        serde_json::to_value(inspect_database_metadata(&crashed))?["state"],
        "needs_recovery"
    );
    assert_eq!(std::fs::read(&crashed)?, before);

    // Recovery keeps every committed record; then the probe reads it.
    recover_database_file(&crashed)?;
    match inspect_database_metadata(&crashed) {
        MetadataProbe::Initialized(probed) => {
            assert_eq!(probed.storage_profile, Profile::Document);
            assert_eq!(probed.replication, ReplicationKind::Ap);
        }
        other => panic!("expected a recovered database, got {other:?}"),
    }
    let recovered = open_database(
        DbConfig::on_disk(Profile::Document, &crashed).with_replication(ReplicationKind::Ap),
    )?;
    assert!(recovered.table("users")?.get(&Value::Int(1))?.is_some());
    drop(recovered);

    // A normal opener recovers it too.
    drop(open_database(
        DbConfig::on_disk(Profile::Document, &reopened).with_replication(ReplicationKind::Ap),
    )?);
    assert!(matches!(
        inspect_database_metadata(&reopened),
        MetadataProbe::Initialized(_)
    ));

    let missing = temp_dir.path().join("missing.redb");
    assert!(matches!(
        recover_database_file(&missing),
        Err(ConfigError::Storage(_))
    ));
    assert!(!missing.exists());
    Ok(())
}

/// The probe and the recovery refuse a path that is not a regular file before opening it, so a
/// FIFO never blocks them.
#[test]
fn metadata_probe_and_recovery_refuse_a_directory() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let directory = temp_dir.path().join("not-a-database");
    std::fs::create_dir(&directory)?;

    match inspect_database_metadata(&directory) {
        MetadataProbe::Unreadable { reason } => {
            assert!(reason.contains("not a regular file"), "{reason}");
        }
        other => panic!("expected an unreadable directory, got {other:?}"),
    }
    match recover_database_file(&directory) {
        Err(ConfigError::Storage(error)) => {
            assert!(error.to_string().contains("not a regular file"), "{error}");
        }
        other => panic!("expected the recovery to refuse a directory, got {other:?}"),
    }
    assert_eq!(std::fs::read_dir(&directory)?.count(), 0);
    Ok(())
}

/// An in-range, conflict-free hand edit passes every value check, so only the digest notices
/// it. The edited manifest must never drive the runtime or be reported as the original.
#[test]
fn metadata_reader_refuses_manifests_whose_digest_does_not_match()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let edits: [(&str, ManifestEdit); 3] = [
        ("cache.redb", |manifest| {
            let current = manifest["knobs"]["cache"]["max_entries"].as_u64();
            manifest["knobs"]["cache"]["max_entries"] =
                json!(if current == Some(4_096) { 8_192 } else { 4_096 });
        }),
        ("timeout.redb", |manifest| {
            manifest["knobs"]["query"]["timeout_ms"] = json!(1_234);
        }),
        ("digest.redb", |manifest| {
            manifest["digest"] = json!("prof_00000000000000000000000000000000");
        }),
    ];

    for (name, edit) in edits {
        let path = manifest_database(temp_dir.path(), name)?;
        edit_raw_manifest(&path, edit)?;
        match open_database(DbConfig::on_disk(Profile::Balanced, &path)) {
            Err(ConfigError::ProfileManifestInvalid(message)) => {
                assert!(message.contains("digest"), "{name}: {message}");
            }
            Err(error) => panic!("{name}: expected an invalid manifest, got {error:?}"),
            Ok(_) => panic!("{name}: an edited manifest drove the runtime"),
        }
        match inspect_database_metadata(&path) {
            MetadataProbe::Unreadable { reason } => {
                assert!(reason.contains("digest"), "{name}: {reason}");
            }
            other => panic!("{name}: expected an unreadable manifest, got {other:?}"),
        }
    }
    Ok(())
}

/// The example spec with `floors.encryption_at_rest` raised, and the guarantee that meets it.
fn encryption_floor_binding(
    path: &Path,
) -> Result<RuntimeProfileBinding, Box<dyn std::error::Error>> {
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
    Ok(compiled.binding(&spec.deployment, Some(path.to_path_buf()))?)
}

/// A manifest never claims a floor its database does not meet: creation refuses operational
/// settings that miss a floor the runtime can check, before anything is created.
#[test]
fn profile_floors_the_runtime_can_check_must_be_met_at_creation()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;

    let unencrypted = temp_dir.path().join("unencrypted.redb");
    let binding = encryption_floor_binding(&unencrypted)?;
    assert!(binding.manifest.knobs.floors.encryption_at_rest);
    match create_database_with_profile(&binding, OperationalConfig::new()) {
        Err(ConfigError::ProfileFloorNotMet { floor, reason }) => {
            assert_eq!(floor, "floors.encryption_at_rest");
            assert!(reason.contains("OperationalConfig.encryption"), "{reason}");
        }
        Err(error) => panic!("expected an unmet floor, got {error:?}"),
        Ok(_) => panic!("an unencrypted database claimed encryption at rest"),
    }
    assert!(!unencrypted.exists());

    let encrypted = temp_dir.path().join("encrypted.redb");
    let key_path = temp_dir.path().join("key.bin");
    std::fs::write(&key_path, [9_u8; 32])?;
    let ops = OperationalConfig::new().with_encryption(EncryptionConfig::file_key(&key_path));
    let binding = encryption_floor_binding(&encrypted)?;
    let database = create_database_with_profile(&binding, ops.clone())?;
    assert_eq!(database.engine_kind(), EngineKind::CompressedEncryptedRedb);
    drop(database);
    let reopened =
        open_database_with_ops(DbConfig::on_disk(Profile::HighDurability, &encrypted), &ops)?;
    assert_eq!(reopened.performance_config(), &binding.performance);
    drop(reopened);

    // The probe reads `__meta` without a key: the encrypted file stays unreadable, and an
    // unencrypted restore target whose manifest claims the floor reports it as unmet.
    assert!(matches!(
        inspect_database_metadata(&encrypted),
        MetadataProbe::Unreadable { .. }
    ));
    let restored = temp_dir.path().join("restored.redb");
    drop(super::create_restore_target(
        DbConfig::on_disk(Profile::HighDurability, &restored),
        Some(&binding.manifest),
    )?);
    match inspect_database_metadata(&restored) {
        MetadataProbe::Initialized(probed) => {
            assert_eq!(probed.manifest.as_ref(), Some(&binding.manifest));
            assert_eq!(probed.unmet_floors, vec!["floors.encryption_at_rest"]);
        }
        other => panic!("expected the restore target to be readable, got {other:?}"),
    }

    // The example raises floors.audit; audit cannot be switched off for it either.
    let audited = temp_dir.path().join("audited.redb");
    let spec = decode_spec_bytes(
        include_str!("../../examples/custom-profile/multidb.yaml").as_bytes(),
        SpecFormat::Yaml,
    )?;
    let compiled = PolicyCompiler::compile(&spec).map_err(|report| format!("{report:?}"))?;
    let binding = compiled.binding(&spec.deployment, Some(audited.clone()))?;
    assert!(binding.manifest.knobs.floors.audit);
    let security = SecurityConfig {
        audit_enabled: false,
        ..SecurityConfig::default()
    };
    assert!(matches!(
        create_database_with_profile(&binding, OperationalConfig::new().with_security(security)),
        Err(ConfigError::ProfileFloorNotMet {
            floor: "floors.audit",
            ..
        })
    ));
    assert!(!audited.exists());
    Ok(())
}

/// `floors.audit` holds after creation too: `set_audit_enabled_as` refuses to disable audit on
/// such a database and audits the refusal, while a manifest without the floor still allows it.
#[test]
fn audit_floor_keeps_audit_enabled_after_creation() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let admin = Principal::new("root").with_role("admin");
    let admin_policy =
        || AuthzPolicy::new([Role::new("admin").grant(Resource::System, Permission::Admin)]);

    let audited = temp_dir.path().join("audited.redb");
    let spec = decode_spec_bytes(
        include_str!("../../examples/custom-profile/multidb.yaml").as_bytes(),
        SpecFormat::Yaml,
    )?;
    let compiled = PolicyCompiler::compile(&spec).map_err(|report| format!("{report:?}"))?;
    let binding = compiled.binding(&spec.deployment, Some(audited.clone()))?;
    assert!(binding.manifest.knobs.floors.audit);
    let mut database = create_database_with_profile(&binding, OperationalConfig::new())?;
    database.set_authz_policy(admin_policy());
    match database.set_audit_enabled_as(&admin, false) {
        Err(DbError::Config(ConfigError::ProfileFloorNotMet { floor, .. })) => {
            assert_eq!(floor, "floors.audit");
        }
        other => panic!("expected the audit floor to refuse, got {other:?}"),
    }
    assert!(database.audit_enabled());
    assert!(database.audit_events()?.iter().any(|event| {
        event.action == "set_audit_enabled" && event.outcome == AuditOutcome::Failed
    }));
    drop(database);
    let reopened = open_database(DbConfig::on_disk(binding.db_config.profile, &audited))?;
    assert!(reopened.audit_enabled());
    drop(reopened);

    let unaudited = manifest_database(temp_dir.path(), "balanced.redb")?;
    let mut database = open_database(DbConfig::on_disk(Profile::Balanced, &unaudited))?;
    database.set_authz_policy(admin_policy());
    database.set_audit_enabled_as(&admin, false)?;
    assert!(!database.audit_enabled());
    Ok(())
}

/// Runs `run` with a subscriber that records what `tracing` logs on this thread, without ANSI
/// colours.
fn capture_logs<T>(run: impl FnOnce() -> T) -> Result<(T, String), Box<dyn std::error::Error>> {
    #[derive(Clone, Default)]
    struct Captured(Arc<std::sync::Mutex<Vec<u8>>>);
    impl io::Write for Captured {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .map_err(|_| io::Error::other("the log buffer is poisoned"))?
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let captured = Captured::default();
    let writer = captured.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let value = tracing::subscriber::with_default(subscriber, run);
    let logs = captured
        .0
        .lock()
        .map_err(|_| "the log buffer is poisoned")?
        .clone();
    Ok((value, String::from_utf8(logs)?))
}

/// Every single-node opener warns about each manifest floor the opened database does not meet,
/// as the probe lists it: an unencrypted restore target that was allowed to miss
/// `floors.encryption_at_rest`, and audit switched off below `floors.audit`. A database that
/// meets its floors opens without a warning.
#[test]
fn single_node_openers_warn_about_unmet_profile_floors() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;

    let encrypted = temp_dir.path().join("encrypted.redb");
    let key_path = temp_dir.path().join("key.bin");
    std::fs::write(&key_path, [4_u8; 32])?;
    let ops = OperationalConfig::new().with_encryption(EncryptionConfig::file_key(&key_path));
    let binding = encryption_floor_binding(&encrypted)?;
    let config = |path: &Path| DbConfig::on_disk(binding.db_config.profile, path);
    drop(create_database_with_profile(&binding, ops.clone())?);
    let (opened, logs) = capture_logs(|| open_database_with_ops(config(&encrypted), &ops))?;
    drop(opened?);
    assert!(!logs.contains("floors."), "{logs}");

    let restored = temp_dir.path().join("restored.redb");
    drop(super::create_restore_target(
        config(&restored),
        Some(&binding.manifest),
    )?);
    for (name, open) in [
        (
            "open_database",
            open_database as fn(DbConfig) -> Result<Database, ConfigError>,
        ),
        ("create_database", create_database),
    ] {
        let (opened, logs) = capture_logs(|| open(config(&restored)))?;
        drop(opened?);
        assert!(
            logs.contains("WARN") && logs.contains("floors.encryption_at_rest"),
            "{name}: {logs}"
        );
        // The restore target holds no security state, so audit is on.
        assert!(!logs.contains("floors.audit"), "{name}: {logs}");
    }

    // An older binary that ignores the manifest could switch audit off.
    let audited = temp_dir.path().join("audited.redb");
    let spec = decode_spec_bytes(
        include_str!("../../examples/custom-profile/multidb.yaml").as_bytes(),
        SpecFormat::Yaml,
    )?;
    let compiled = PolicyCompiler::compile(&spec).map_err(|report| format!("{report:?}"))?;
    let binding = compiled.binding(&spec.deployment, Some(audited.clone()))?;
    let database = create_database_with_profile(&binding, OperationalConfig::new())?;
    propose_system(
        &database,
        Op::Put {
            table: AUTHZ_TABLE.to_owned(),
            key: KEY_AUDIT_ENABLED.to_vec(),
            value: b"false".to_vec(),
        },
    )?;
    drop(database);
    match inspect_database_metadata(&audited) {
        MetadataProbe::Initialized(probed) => {
            assert_eq!(probed.unmet_floors, vec!["floors.audit"]);
        }
        other => return Err(format!("expected an initialized database, got {other:?}").into()),
    }
    let audited_config = DbConfig::on_disk(binding.db_config.profile, &audited);
    let (opened, logs) =
        capture_logs(|| open_database_with_ops(audited_config, &OperationalConfig::new()))?;
    let opened = opened?;
    assert!(!opened.audit_enabled());
    // The handle view that provisioning plans from lists the same floor as the file probe.
    assert_eq!(opened.probed_metadata()?.unmet_floors, vec!["floors.audit"]);
    drop(opened);
    assert!(
        logs.contains("WARN") && logs.contains("floors.audit"),
        "{logs}"
    );
    assert!(!logs.contains("floors.encryption_at_rest"), "{logs}");
    Ok(())
}

/// Only single-node databases can carry a profile manifest, so the `floors.audit` check never
/// reads `__meta` of a CP, AP or sharded database: an AP opener keeps `__meta` outside the
/// tables `ApDynamo::read` serves, and disabling audit there must keep working.
#[test]
fn audit_can_be_disabled_on_databases_without_a_manifest_opener()
-> Result<(), Box<dyn std::error::Error>> {
    fn disable_audit(database: &mut Database) -> Result<(), Box<dyn std::error::Error>> {
        database.set_authz_policy(AuthzPolicy::new([
            Role::new("admin").grant(Resource::System, Permission::Admin)
        ]));
        database.set_audit_enabled_as(&Principal::new("root").with_role("admin"), false)?;
        assert!(!database.audit_enabled());
        Ok(())
    }

    disable_audit(&mut create_ap_database(
        DbConfig::new(Profile::InMemory).with_replication(ReplicationKind::Ap),
        ap_cluster_config(),
    )?)?;
    let temp_dir = tempfile::tempdir()?;
    let config = DbConfig::on_disk(Profile::Balanced, temp_dir.path().join("ap.redb"))
        .with_replication(ReplicationKind::Ap);
    disable_audit(&mut create_ap_database(
        config.clone(),
        ap_cluster_config(),
    )?)?;
    disable_audit(&mut open_ap_database(config, ap_cluster_config())?)?;
    disable_audit(&mut create_simulated_cluster_database(
        DbConfig::new(Profile::InMemory),
        cp_cluster_config(),
    )?)?;
    disable_audit(&mut create_sharded_database(
        DbConfig::new(Profile::InMemory),
        sharded_config(),
    )?)?;
    Ok(())
}

/// A `__meta.profile_manifest` exactly as the first build with profile manifests wrote it
/// (crate 0.1.0, `manifest_version` 1): `high_durability` with a larger cache and zstd. Never
/// edit it. If a change stops it from decoding or validating, every database created by that
/// build stops opening: give new knob leaves a serde default that leaves this manifest and its
/// digest unchanged, and bump `manifest_version` with a migration for anything else (see
/// `ProfileKnobs` and docs/profiles.md, "Evolving the manifest").
const GOLDEN_MANIFEST_V1: &str = concat!(
    r#"{"manifest_version":1,"profile":"high_durability_custom","kind":"custom","#,
    r#""product_profile":"secure_app","variant":"high_durability","extends":"high_durability","#,
    r#""storage_profile":"HighDurability","knobs":{"storage":{"medium":"durable","#,
    r#""layout":"row"},"durability":{"commit":"two_phase","group_commit_window_ms":0},"#,
    r#""compression":{"algorithm":"zstd","min_bytes":256,"zstd_level":3},"#,
    r#""cache":{"enabled":true,"max_entries":8192},"query":{"max_concurrent_queries":"auto","#,
    r#""queue_timeout_ms":100,"timeout_ms":30000,"memory_limit_bytes":805306368,"#,
    r#""spill_limit_bytes":2147483648,"target_partitions":"auto","batch_rows":1024,"#,
    r#""max_sql_bytes":1048576,"max_statements":32,"max_values_rows":10000,"#,
    r#""parser_recursion_limit":128},"limits":{"max_value_bytes":"unlimited","#,
    r#""max_batch_ops":"unlimited"},"history":{"system_versioning":"off","keep_history":true},"#,
    r#""time_series":{"default_chunk_millis":3600000,"default_retention_millis":null},"#,
    r#""consistency":{"default_domain":"strong_cp"},"floors":{"audit":false,"backup":false,"#,
    r#""pitr":false,"encryption_at_rest":false},"compatible_roles":["document_entity","#,
    r#""key_value","event_log","audit"]},"storage_format":{"value_framing":true,"#,
    r#""compression":{"algorithm":"Zstd","min_bytes":256,"zstd_level":3}},"#,
    r#""digest":"prof_1a834651abbcb9023c91f831a368591e","written_by":"0.1.0"}"#,
);

#[test]
fn profile_manifest_v1_golden_still_decodes_and_validates() -> Result<(), Box<dyn std::error::Error>>
{
    let manifest = super::decode_profile_manifest(GOLDEN_MANIFEST_V1.as_bytes())?;
    super::validate_manifest(&manifest, Profile::HighDurability)?;
    assert_eq!(manifest.manifest_version, 1);
    assert_eq!(manifest.profile, "high_durability_custom");
    assert_eq!(manifest.kind, ResolvedProfileKind::Custom);
    assert_eq!(manifest.variant, "high_durability");
    assert_eq!(manifest.written_by, "0.1.0");
    assert!(manifest.storage_format.value_framing);
    assert_eq!(
        manifest.storage_format.compression.algorithm,
        CompressionAlgorithm::Zstd
    );
    let performance = super::profile_binding::performance_for_manifest(&manifest);
    assert_eq!(performance.cache.max_capacity, 8192);
    Ok(())
}

#[test]
fn compiled_policy_binding_creates_a_manifest_database() -> Result<(), Box<dyn std::error::Error>> {
    let spec = decode_spec_bytes(
        include_str!("../../examples/custom-profile/multidb.yaml").as_bytes(),
        SpecFormat::Yaml,
    )?;
    let compiled = PolicyCompiler::compile(&spec).map_err(|report| format!("{report:?}"))?;
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("orders.redb");

    let binding = compiled.binding(&spec.deployment, Some(path.clone()))?;
    assert_eq!(binding.db_config.path.as_deref(), Some(path.as_path()));
    assert_eq!(binding.manifest.kind, ResolvedProfileKind::Custom);
    assert_eq!(binding.manifest.profile, "orders_durable");
    assert_eq!(binding.manifest.extends.as_deref(), Some("high_durability"));
    let database = create_database_with_profile(&binding, OperationalConfig::new())?;
    assert_eq!(database.profile(), Profile::HighDurability);
    assert_eq!(database.engine_kind(), EngineKind::CompressedRedb);
    assert_eq!(database.performance_config().cache.max_capacity, 8192);
    drop(database);
    let reopened = open_database(DbConfig::on_disk(Profile::HighDurability, &path))?;
    assert_eq!(reopened.performance_config(), &binding.performance);

    let mut cluster = spec.deployment.clone();
    cluster.mode = DeploymentMode::Cluster;
    assert!(matches!(
        compiled.binding(&cluster, Some(path.clone())),
        Err(ConfigError::Unsupported(_))
    ));
    let mut replicated = compiled.clone();
    replicated.topology.replica_count = 3;
    assert!(matches!(
        replicated.binding(&spec.deployment, Some(path.clone())),
        Err(ConfigError::Unsupported(_))
    ));
    let mut unresolved = compiled;
    unresolved.resolved_profile = None;
    assert!(matches!(
        unresolved.binding(&spec.deployment, Some(path)),
        Err(ConfigError::Unsupported(_))
    ));
    Ok(())
}

#[test]
fn compiled_policy_binding_refuses_a_path_for_a_volatile_class()
-> Result<(), Box<dyn std::error::Error>> {
    // Version 1 only warns about a volatile profile with a storage path, so it compiles.
    let mut spec = decode_spec_bytes(
        include_str!("../../tests/fixtures/spec-v1/game-save.yaml").as_bytes(),
        SpecFormat::Yaml,
    )?;
    spec.profile = "in_memory".to_owned();
    spec.deployment.storage_path = Some("game.redb".to_owned());
    let compiled = PolicyCompiler::compile(&spec).map_err(|report| format!("{report:?}"))?;
    assert_eq!(
        compiled
            .resolved_profile
            .as_ref()
            .map(|resolved| resolved.storage_profile),
        Some(Profile::InMemory)
    );
    let volatile_with_path = |result: Result<RuntimeProfileBinding, ConfigError>| {
        matches!(
            result,
            Err(ConfigError::VolatileWithPath {
                profile: Profile::InMemory
            })
        )
    };
    assert!(volatile_with_path(compiled.binding(&spec.deployment, None)));

    let mut without_path = spec.deployment.clone();
    without_path.storage_path = None;
    assert!(volatile_with_path(
        compiled.binding(&without_path, Some("x.redb".into()))
    ));

    let memory = compiled.binding(&without_path, None)?;
    assert_eq!(memory.db_config.path, None);
    let database = create_database_with_profile(&memory, OperationalConfig::new())?;
    assert_eq!(database.engine_kind(), EngineKind::Memory);
    Ok(())
}

#[test]
fn resolved_profile_bind_keeps_path_rules() -> Result<(), Box<dyn std::error::Error>> {
    assert!(matches!(
        named_binding("balanced", json!({}), ReplicationKind::Cp, None),
        Err(error) if matches!(
            error.downcast_ref::<ConfigError>(),
            Some(ConfigError::MissingPath { profile: Profile::Balanced })
        )
    ));
    assert!(matches!(
        named_binding("in_memory", json!({}), ReplicationKind::Cp, Some(Path::new("x.redb"))),
        Err(error) if matches!(
            error.downcast_ref::<ConfigError>(),
            Some(ConfigError::VolatileWithPath { profile: Profile::InMemory })
        )
    ));

    let memory = named_binding("in_memory", json!({}), ReplicationKind::Cp, None)?;
    let database = create_database_with_profile(&memory, OperationalConfig::new())?;
    assert_eq!(database.engine_kind(), EngineKind::Memory);
    assert_eq!(database.performance_config(), &memory.performance);
    Ok(())
}

#[test]
fn tuning_rollback_returns_to_manifest_values() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("tuning.redb");
    let binding = named_binding(
        "balanced",
        json!({ "cache": { "max_entries": 8192 } }),
        ReplicationKind::Cp,
        Some(&path),
    )?;
    drop(create_database_with_profile(
        &binding,
        OperationalConfig::new(),
    )?);

    let mut database = open_database(DbConfig::on_disk(Profile::Balanced, &path))?;
    assert_eq!(database.performance_config().cache.max_capacity, 8192);
    let mut policy = TuningPolicy {
        enabled: true,
        recommend_only: false,
        ..TuningPolicy::default()
    };
    policy.envelopes.insert(
        TunableParameter::CacheMaxCapacity,
        ParameterEnvelope::new(64, 16_384, 64),
    );
    database.set_tuning_policy(&policy)?;

    let entry = database.apply_tuning(TuningDecision::new(
        TunableParameter::CacheMaxCapacity,
        8192,
        1_024,
        "cache pressure",
    ))?;
    assert_eq!(database.performance_config().cache.max_capacity, 1_024);
    database.rollback_tuning(&entry.id, "regression")?;
    assert_eq!(database.performance_config().cache.max_capacity, 8192);
    assert_ne!(
        PerformanceConfig::for_profile(Profile::Balanced)
            .cache
            .max_capacity,
        8192
    );
    Ok(())
}

type ManifestEdit = fn(&mut serde_json::Value);
type RawRows = Vec<(Vec<u8>, Vec<u8>)>;

fn named_binding(
    name: &str,
    overrides: serde_json::Value,
    replication: ReplicationKind,
    path: Option<&Path>,
) -> Result<RuntimeProfileBinding, Box<dyn std::error::Error>> {
    let overrides: ProfileKnobOverrides = serde_json::from_value(overrides)?;
    let resolution = resolve_named_profile(name, &overrides);
    let resolved = resolution
        .resolved()
        .ok_or_else(|| format!("{name} did not resolve: {:?}", resolution.issues()))?;
    Ok(resolved.bind(replication, path.map(Path::to_path_buf))?)
}

/// Creates an unframed `balanced` profile-manifest database and returns its path.
fn manifest_database(
    dir: &Path,
    name: &str,
) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let path = dir.join(name);
    let binding = named_binding("balanced", json!({}), ReplicationKind::Cp, Some(&path))?;
    drop(create_database_with_profile(
        &binding,
        OperationalConfig::new(),
    )?);
    Ok(path)
}

fn raw_value(
    path: &Path,
    table: &str,
    key: &[u8],
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let engine = RedbEngine::open(path)?;
    let txn = engine.begin_read()?;
    Ok(txn.get(table, key)?)
}

fn raw_rows(path: &Path, table: &str) -> Result<RawRows, Box<dyn std::error::Error>> {
    let engine = RedbEngine::open(path)?;
    let txn = engine.begin_read()?;
    let rows = txn.range(table, &[], &[])?.collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn put_raw(
    path: &Path,
    table: &str,
    key: &[u8],
    value: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let engine = RedbEngine::open(path)?;
    let mut txn = engine.begin_write()?;
    txn.put(table, key, value)?;
    txn.commit()?;
    Ok(())
}

fn edit_raw_manifest(
    path: &Path,
    edit: impl FnOnce(&mut serde_json::Value),
) -> Result<(), Box<dyn std::error::Error>> {
    let raw = raw_value(path, META_TABLE, KEY_PROFILE_MANIFEST)?.ok_or("manifest missing")?;
    let mut manifest: serde_json::Value = serde_json::from_slice(&raw)?;
    edit(&mut manifest);
    put_raw(
        path,
        META_TABLE,
        KEY_PROFILE_MANIFEST,
        &serde_json::to_vec(&manifest)?,
    )
}
