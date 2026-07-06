use std::{collections::BTreeMap, io, sync::Arc, thread};

use super::{
    ConfigError, Database, DbConfig, DbError, EncryptionConfig, KEY_LAYOUT, META_TABLE,
    OperationalConfig, Profile, ReplicationKind, SecurityConfig, ShardBackendConfig, ShardSpec,
    ShardedDatabaseConfig, create_ap_database, create_cluster_database, create_database,
    create_database_with_ops, create_sharded_database, engine_for, engine_for_operational,
    force_wasm_runtime_init_failure, layout_for, open_ap_database, open_cluster_database,
    open_database, open_database_with_ops, profile_validation_report, validate,
};
use crate::cdc::{
    AggregateKind, AggregateSpec, ChangeOp, ChangefeedFilter, ChangefeedOptions, ChangefeedTarget,
    HOOK_DELIVERIES_TABLE, HookAction, HookSpec, HookTarget, MaterializedViewSpec, ResumeToken,
};
use crate::cloud::ObjectStoreUri;
use crate::config_spec::{ApplyStatus, ConsistencyMode, DatabaseSpec, MigrationPlanner};
use crate::continuous::{
    ContinuousQuerySpec, OutboxConnectorSpec, OutboxSink, TriggerEvent, TriggerSpec, TriggerTiming,
};
use crate::extension::{
    AbiVersion, ExtensionError, LimitPolicy, MaskingPolicy, PolicyConfig, UdfBudget,
};
use crate::federation::{ForeignSource, ForeignTableOptions};
use crate::geo::{GeoIndexConfig, GeoPoint};
use crate::graph::{GraphId, GraphNodeId, TraversalOptions};
use crate::model::{
    CollectionId, FieldPath, IndexId, IndexSpec, PlanKind, Predicate, Value, decode_value,
};
use crate::performance::{
    BenchmarkReport, CompressionAlgorithm, PerformanceConfig, RegressionGate,
};
use crate::phase30::{InternalTransportConfig, InternalTransportSecurity};
use crate::query::{
    ColumnDef, ColumnType, DocField, ExplainOptions, QueryError, REL_ROWS_TABLE, RelIndexSpec,
    RelPlanKind, Row, SqlOutput, SqlRows, TableLayout, TableSchema,
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
use crate::storage::{EngineKind, RedbEngine, StorageEngine, StorageError, WriteTransaction};
use crate::temporal::{TemporalPoint, TemporalRetention};
use crate::text::FullTextIndexConfig;
use crate::timeseries::{TimePoint, TimeSeriesConfig};
use crate::tuning::{
    ParameterEnvelope, ReprofilePlan, ReprofileStatus, TunableParameter, TuningDecision,
    TuningPolicy, WorkloadSample,
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
    assert!(!rows.rows.is_empty());
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
    assert!(first.knn(&[1.0, 0.0, 0.0], 1)?.is_empty());
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
    let cp = create_cluster_database(DbConfig::new(Profile::InMemory), cp_cluster_config())?;
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

    let cp = create_cluster_database(DbConfig::new(Profile::InMemory), cp_cluster_config())?;
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
fn cluster_database_persists_through_open() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("cp-cluster.redb");
    let config = DbConfig::on_disk(Profile::Balanced, path);

    {
        let database = create_cluster_database(config.clone(), cp_cluster_config())?;
        database.propose(Op::Put {
            table: "t".to_owned(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        })?;
    }

    let database = open_cluster_database(config, cp_cluster_config())?;
    assert_eq!(
        database.read("t", b"k", ReadConsistency::Strong)?,
        Some(b"v".to_vec())
    );

    Ok(())
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
