//! Provisioning plans, drift and export against real databases.

use std::collections::BTreeSet;
use std::path::Path;

use serde_json::{Value as JsonValue, json};

use super::live::LiveState;
use super::physical::{entry_kind, project};
use super::plan::{PlanIdInputs, canonical_target_path, desired_role, plan_id_for};
use super::record::{RECORD_KEY, RecordState, decode_record};
use super::{
    Confirmation, DriftKind, DriftResolution, EnforcementStatus, ExportSource, ManagedObject,
    ObjectKind, ObjectState, PlanOutcome, ProvisionActor, ProvisionApplyReport,
    ProvisionEnvironment, ProvisionError, ProvisionOptions, ProvisionOutcome, ProvisionPlan,
    ProvisionRecord, ProvisionRecordStatus, ProvisionStep, ProvisionStepKind, ProvisionSurface,
    ProvisionTargetRef, RECORD_VERSION, StartupDecision, StartupMode, StartupRefusal,
    StartupReport, StepOutcome, StepSafety, TargetKind, apply_provision, drift, export_spec,
    fail_provision_step_for_tests, open_from_spec, perturb_new_database_for_tests, plan_provision,
    provision_new_database, serve_startup_decision,
};
use crate::backup::{BackupConfig, RestoreTarget, full_backup, restore_backup, verify_backup};
use crate::cdc::{AggregateKind, AggregateSpec, MaterializedViewSpec};
use crate::config_spec::{
    ApplyStatus, ConflictResolution, ConsistencyDomainSpec, ConsistencyMode,
    DATABASE_SPEC_VERSION_V2, DatabaseSpec, DeploymentMode, ExtensionManifestRef,
    ExtensionStability, GuaranteeValidator, ImpactLevel, ReplicationMode, ResolvedProfile,
    RiskLevel, UnmanagedPolicy, ValidationSeverity, resolve_profile, spec_digest,
    spec_io::{SpecFormat, decode_spec_bytes},
};
use crate::db::{
    AuditConfig, CatalogEntry, ConfigError, Database, DbConfig, DbError, EncryptionConfig,
    OperationalConfig, PROVISION_TABLE, Profile, ReplicationKind, SecurityConfig,
    create_cluster_database, create_database, create_database_with_ops,
    create_database_with_profile, create_simulated_cluster_database, open_database,
    open_database_with_ops,
};
use crate::extension::{LimitPolicy, PolicyConfig};
use crate::geo::{GeoIndexConfig, GeoPoint};
use crate::graph::GraphId;
use crate::model::{CollectionId, FieldPath, IndexId, IndexSpec, Value};
use crate::phase30::{InternalTransportConfig, InternalTransportSecurity};
use crate::query::{
    ColumnDef, ColumnType, DocField, RelIndexExpression, RelIndexSpec, RelPredicate, SqlOutput,
    SqlRows, TableLayout, TableSchema,
};
use crate::repl::{CpClusterConfig, Op, RaftNode, ReadConsistency, Replication, propose_system};
use crate::security::{
    AuditEvent, AuditOutcome, AuthzPolicy, Permission, Principal, PrincipalRegistry, Resource, Role,
};
use crate::storage::{EngineKind, ReadTransaction, RedbEngine, StorageEngine};
use crate::temporal::TemporalRetention;
use crate::text::FullTextIndexConfig;
use crate::timeseries::{TimePoint, TimeSeriesConfig};
use crate::txn;
use crate::vector::{VectorCollectionConfig, VectorMetric};

type TestResult = Result<(), Box<dyn std::error::Error>>;
type Error = Box<dyn std::error::Error>;

fn spec(profile: &str, collections: &JsonValue) -> Result<DatabaseSpec, Error> {
    let mut spec =
        DatabaseSpec::from_db_config("app", &DbConfig::on_disk(Profile::Balanced, "app.redb"));
    spec.version = DATABASE_SPEC_VERSION_V2;
    spec.profile = profile.to_owned();
    spec.collections = serde_json::from_value(collections.clone())?;
    Ok(spec)
}

/// `spec` for an embedded in-memory database.
fn in_memory(mut spec: DatabaseSpec) -> DatabaseSpec {
    spec.profile = "in_memory".to_owned();
    spec.deployment.mode = DeploymentMode::Embedded;
    spec.deployment.storage_path = None;
    spec
}

fn with_custom_profile(
    mut spec: DatabaseSpec,
    slug: &str,
    custom: &JsonValue,
) -> Result<DatabaseSpec, Error> {
    spec.profile = slug.to_owned();
    spec.custom_profile = Some(serde_json::from_value(custom.clone())?);
    Ok(spec)
}

fn with_security(mut spec: DatabaseSpec, security: &JsonValue) -> Result<DatabaseSpec, Error> {
    spec.security = Some(serde_json::from_value(security.clone())?);
    Ok(spec)
}

/// Every kind provisioning builds: a document collection with full-text and geo indexes, a
/// vector collection, a typed row table with history, a columnar table, a time series and a
/// graph, plus a role and a principal.
fn full_spec() -> Result<DatabaseSpec, Error> {
    let spec = spec(
        "balanced",
        &json!([
            {
                "name": "orders", "role": "document_entity", "domain": "primary",
                "indexes": ["primary", "document", "full_text", "geo"],
                "physical": {
                    "fields": [{ "name": "customer_id", "path": "customer.id", "type": "str" }],
                    "document_indexes": [{ "path": "customer.id" }],
                    "full_text": [{ "path": "notes" }],
                    "geo": [{ "path": "shipping.location" }]
                }
            },
            {
                "name": "memories", "role": "vector_memory", "domain": "primary",
                "indexes": ["vector"], "physical": { "dim": 3 }
            },
            {
                "name": "ledger", "role": "event_log", "domain": "primary", "indexes": ["primary"],
                "physical": {
                    "kind": "table",
                    "columns": [
                        { "name": "entry_id", "type": "int" },
                        { "name": "amount", "type": "float" }
                    ],
                    "primary_key": "entry_id",
                    "system_versioning": true
                }
            },
            {
                "name": "rollups", "role": "analytics", "domain": "primary", "indexes": ["columnar"],
                "physical": {
                    "columns": [{ "name": "id", "type": "str" }, { "name": "revenue", "type": "float" }],
                    "primary_key": "id"
                }
            },
            { "name": "metrics", "role": "time_series", "domain": "primary", "indexes": ["time_series"] },
            { "name": "social", "role": "graph", "domain": "primary", "indexes": ["graph"] }
        ]),
    )?;
    with_security(
        spec,
        &json!({
            "roles": [{
                "name": "order_writer",
                "grants": [
                    { "object": "orders", "permissions": ["read", "write"] },
                    { "object": "orders_notes_fts", "permissions": ["read"] }
                ]
            }],
            "principals": [{ "user": "orders_svc", "roles": ["order_writer"] }]
        }),
    )
}

fn orders_spec() -> Result<DatabaseSpec, Error> {
    spec(
        "balanced",
        &json!([{
            "name": "orders", "role": "document_entity", "domain": "primary",
            "indexes": ["primary", "document"],
            "physical": {
                "fields": [{ "name": "customer_id", "path": "customer.id", "type": "str" }],
                "document_indexes": [{ "path": "customer.id" }]
            }
        }]),
    )
}

fn orders_fields() -> Vec<DocField> {
    vec![DocField::path(
        "customer_id",
        FieldPath::new(["customer", "id"]),
        ColumnType::Str,
    )]
}

fn orders_indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::new(
        IndexId::new(1),
        FieldPath::new(["customer", "id"]),
    )]
}

fn order_document(customer: &str) -> Value {
    Value::Object(
        [
            (
                "customer".to_owned(),
                Value::Object([("id".to_owned(), Value::Str(customer.to_owned()))].into()),
            ),
            ("notes".to_owned(), Value::Str("fragile".to_owned())),
        ]
        .into(),
    )
}

fn env(surface: ProvisionSurface) -> ProvisionEnvironment {
    ProvisionEnvironment::new(surface)
}

fn plan_path(
    spec: &DatabaseSpec,
    path: &Path,
    surface: ProvisionSurface,
) -> Result<ProvisionPlan, ProvisionError> {
    plan_provision(
        spec,
        ProvisionTargetRef::Path(path),
        &ProvisionOptions::default(),
        &env(surface),
    )
}

fn plan_open(spec: &DatabaseSpec, db: &Database) -> Result<ProvisionPlan, ProvisionError> {
    plan_open_with(spec, db, &ProvisionOptions::default())
}

fn plan_open_with(
    spec: &DatabaseSpec,
    db: &Database,
    opts: &ProvisionOptions,
) -> Result<ProvisionPlan, ProvisionError> {
    plan_provision(
        spec,
        ProvisionTargetRef::Open(db),
        opts,
        &env(ProvisionSurface::Cli),
    )
}

fn plan_rust_api(spec: &DatabaseSpec, db: &Database) -> Result<ProvisionPlan, ProvisionError> {
    plan_provision(
        spec,
        ProvisionTargetRef::Open(db),
        &ProvisionOptions::default(),
        &env(ProvisionSurface::RustApi),
    )
}

fn issue_codes(plan: &ProvisionPlan) -> Vec<&str> {
    plan.issues
        .iter()
        .map(|issue| issue.code.as_str())
        .collect()
}

fn has_issue(plan: &ProvisionPlan, code: &str, severity: ValidationSeverity) -> bool {
    plan.issues
        .iter()
        .chain(&plan.validation.issues)
        .any(|issue| issue.code == code && issue.severity == severity)
}

fn assert_blocked(plan: &ProvisionPlan, code: &str) {
    assert_eq!(
        plan.outcome,
        PlanOutcome::Blocked,
        "{code}: {:#?}",
        plan.issues
    );
    assert!(
        has_issue(plan, code, ValidationSeverity::Error),
        "{code} missing: {:#?} {:#?}",
        plan.issues,
        plan.validation.issues
    );
    assert!(!plan.auto_approvable);
}

fn step_kinds(plan: &ProvisionPlan) -> Vec<ProvisionStepKind> {
    plan.steps.iter().map(|step| step.kind).collect()
}

fn has_step(plan: &ProvisionPlan, kind: ProvisionStepKind, safety: StepSafety) -> bool {
    plan.steps
        .iter()
        .any(|step| step.kind == kind && step.safety == safety)
}

fn resolved(spec: &DatabaseSpec) -> Result<ResolvedProfile, Error> {
    resolve_profile(spec)
        .resolved()
        .cloned()
        .ok_or_else(|| "the test profile resolves".into())
}

/// A new single-node database with the resolved profile of `spec` and its manifest.
fn create_with_profile(path: &Path, spec: &DatabaseSpec) -> Result<Database, Error> {
    let binding = resolved(spec)?.bind(ReplicationKind::Cp, Some(path.to_path_buf()))?;
    Ok(create_database_with_profile(
        &binding,
        OperationalConfig::new(),
    )?)
}

fn write_record(db: &Database, record: &ProvisionRecord) -> TestResult {
    write_record_bytes(db, serde_json::to_vec(record)?)
}

fn write_record_bytes(db: &Database, value: Vec<u8>) -> TestResult {
    propose_system(
        db,
        Op::Put {
            table: PROVISION_TABLE.to_owned(),
            key: RECORD_KEY.to_vec(),
            value,
        },
    )?;
    Ok(())
}

/// The record an apply of `plan` would leave: every declared object and the spec's roles and
/// principals managed.
fn record_for(spec: &DatabaseSpec, plan: &ProvisionPlan) -> ProvisionRecord {
    let security = spec.security.clone().unwrap_or_default();
    ProvisionRecord {
        record_version: RECORD_VERSION,
        status: ProvisionRecordStatus::Applied,
        plan_id: plan.plan_id.clone(),
        spec_digest: spec_digest(spec),
        applied_spec: spec.clone(),
        last_applied_spec: None,
        profile_digest: plan
            .resolved_profile
            .as_ref()
            .map(|resolved| resolved.digest.clone())
            .unwrap_or_default(),
        objects: plan
            .objects
            .iter()
            .filter_map(|object| {
                object.spec_collection.clone().map(|spec_collection| {
                    (
                        object.name.clone(),
                        ManagedObject {
                            spec_collection,
                            kind: object.kind,
                            adopted: object.state == ObjectState::Adopt,
                            allocated_id: None,
                        },
                    )
                })
            })
            .collect(),
        managed_roles: security
            .roles
            .iter()
            .map(|role| role.name.clone())
            .collect(),
        managed_principals: security
            .principals
            .iter()
            .map(|principal| principal.user.clone())
            .collect(),
        released_objects: BTreeSet::new(),
        guarantees: plan.guarantees.clone(),
        completed_steps: plan.steps.iter().map(|step| step.step_id.clone()).collect(),
        failed_step: None,
        flags: Vec::new(),
        seed_digest: None,
        applied_at_millis: 1,
        applied_by: "local:test".to_owned(),
        binary_version: env!("CARGO_PKG_VERSION").to_owned(),
    }
}

type Ranges = Vec<Vec<(crate::storage::Bytes, crate::storage::Bytes)>>;
type Mutation = Box<dyn Fn(&mut LiveState)>;

fn ranges(db: &Database, tables: &[&str]) -> Result<Ranges, Error> {
    tables
        .iter()
        .map(|table| {
            Ok(
                Replication::range(db, table, &[], &[0xFF], ReadConsistency::Strong)?
                    .into_iter()
                    .collect(),
            )
        })
        .collect()
}

#[test]
fn new_database_plan_is_deterministic_ordered_ready_and_auto_approvable() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("orders.redb");
    let spec = full_spec()?;

    let plan = plan_path(&spec, &path, ProvisionSurface::Cli)?;
    let again = plan_path(&spec, &path, ProvisionSurface::Cli)?;

    assert!(!path.exists(), "a dry run never creates the database file");
    assert_eq!(plan, again);
    assert!(plan.plan_id.starts_with("prov_"));
    assert_eq!(plan.plan_id.len(), "prov_".len() + 32);
    assert!(
        plan.plan_id["prov_".len()..]
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    );
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
    assert!(plan.auto_approvable);
    assert_eq!(
        plan.required_confirmation.as_deref(),
        Some(plan.plan_id.as_str())
    );
    assert_eq!(plan.required_flags, Vec::<String>::new());
    assert_eq!(plan.target.kind, TargetKind::NewDatabase);
    assert_eq!(plan.target.storage_profile, Some(Profile::Balanced));
    assert_eq!(plan.target.schema_version, Some(1));
    assert_eq!(plan.target.path, Some(canonical_target_path(&path)));
    assert_eq!(plan.live_fingerprint, None);
    assert_eq!(plan.spec_digest, spec_digest(&spec));
    assert!(plan.spec_digest.starts_with("spec_"));
    assert!(plan.spec_migration.is_none());

    assert_eq!(
        step_kinds(&plan),
        vec![
            ProvisionStepKind::CreateDatabase,
            ProvisionStepKind::CreateCollection,
            ProvisionStepKind::CreateVectorCollection,
            ProvisionStepKind::CreateTable,
            ProvisionStepKind::CreateTable,
            ProvisionStepKind::CreateTimeSeries,
            ProvisionStepKind::CreateGraph,
            ProvisionStepKind::CreateFullTextIndex,
            ProvisionStepKind::CreateGeoIndex,
            ProvisionStepKind::EnableSystemVersioning,
            ProvisionStepKind::ApplySecurity,
            ProvisionStepKind::ApplySecurity,
            ProvisionStepKind::RecordMetadata,
            ProvisionStepKind::RecordAppliedSpec,
        ]
    );
    let objects = plan
        .steps
        .iter()
        .map(|step| step.object.as_deref().unwrap_or("-"))
        .collect::<Vec<_>>();
    assert_eq!(
        objects,
        vec![
            "-",
            "orders",
            "memories",
            "ledger",
            "rollups",
            "metrics",
            "social",
            "orders_notes_fts",
            "orders_shipping_location_geo",
            "ledger_history",
            "order_writer",
            "orders_svc",
            "-",
            "-",
        ]
    );
    for (index, step) in plan.steps.iter().enumerate() {
        assert_eq!(step.step_id, format!("pstep-{:03}", index + 1));
        assert!(step.supported);
        assert!(step.requires_flag.is_none());
        assert!(matches!(
            step.safety,
            StepSafety::Additive | StepSafety::Metadata
        ));
    }
    assert!(
        plan.objects
            .iter()
            .all(|object| object.state == ObjectState::Create)
    );
    assert!(has_issue(
        &plan,
        "AUDIT_DEVELOPMENT_KEY",
        ValidationSeverity::Warning
    ));
    assert!(has_issue(
        &plan,
        "ROLE_CONSTRAINT_NOT_ENFORCED",
        ValidationSeverity::Warning
    ));
    assert!(!plan.drift.in_sync);
    Ok(())
}

#[test]
fn a_columnar_table_plans_inside_a_high_durability_custom_profile() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let mut spec = with_custom_profile(
        full_spec()?,
        "orders_durable",
        &json!({ "extends": "high_durability", "knobs": { "cache": { "max_entries": 8192 } } }),
    )?;
    spec.guarantees.backup.enabled = true;
    spec.guarantees.backup.pitr = true;
    let plan = plan_path(
        &spec,
        &temp_dir.path().join("orders.redb"),
        ProvisionSurface::Cli,
    )?;
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
    assert!(plan.auto_approvable);
    assert_eq!(plan.target.storage_profile, Some(Profile::HighDurability));
    let rollups = plan
        .objects
        .iter()
        .find(|object| object.name == "rollups")
        .ok_or("rollups")?;
    assert_eq!(rollups.params["layout"], json!("columnar"));
    let ledger = plan
        .objects
        .iter()
        .find(|object| object.name == "ledger")
        .ok_or("ledger")?;
    assert_eq!(ledger.params["layout"], json!("row"));
    Ok(())
}

#[test]
fn plan_json_round_trips_and_rejects_unknown_fields() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let plan = plan_path(
        &full_spec()?,
        &temp_dir.path().join("orders.redb"),
        ProvisionSurface::Cli,
    )?;

    let json = serde_json::to_value(&plan)?;
    let decoded: ProvisionPlan = serde_json::from_value(json.clone())?;
    assert_eq!(decoded, plan);

    let mut unknown = json.clone();
    unknown["unexpected"] = json!(true);
    assert!(serde_json::from_value::<ProvisionPlan>(unknown).is_err());
    let mut unknown_step = json;
    unknown_step["steps"][0]["unexpected"] = json!(1);
    assert!(serde_json::from_value::<ProvisionPlan>(unknown_step).is_err());
    assert!(
        serde_json::from_value::<ProvisionOptions>(json!({ "allow_everything": true })).is_err()
    );
    assert_eq!(
        serde_json::from_value::<ProvisionOptions>(json!({}))?,
        ProvisionOptions::default()
    );
    Ok(())
}

#[test]
fn plan_ids_cover_options_surface_target_path_and_binary_version() -> TestResult {
    let spec = full_spec()?;
    let options = ProvisionOptions::default();
    let inputs = PlanIdInputs {
        spec: &spec,
        live_fingerprint: None,
        options: &options,
        surface: ProvisionSurface::Cli,
        target_path: Some("/data/orders.redb"),
    };
    let base = plan_id_for(&inputs, "0.1.0");
    assert_eq!(base, plan_id_for(&inputs, "0.1.0"));
    assert_ne!(base, plan_id_for(&inputs, "0.1.1"));

    let flagged = ProvisionOptions {
        allow_weakening: BTreeSet::from(["guarantees.audit.enabled".to_owned()]),
        ..ProvisionOptions::default()
    };
    let unmanaged = ProvisionOptions {
        unmanaged: Some(UnmanagedPolicy::Fail),
        ..ProvisionOptions::default()
    };
    let mut changed_spec = spec.clone();
    changed_spec.name = "orders_v2".to_owned();
    for other in [
        PlanIdInputs {
            options: &flagged,
            ..inputs
        },
        PlanIdInputs {
            options: &unmanaged,
            ..inputs
        },
        PlanIdInputs {
            surface: ProvisionSurface::ControlPlane,
            ..inputs
        },
        PlanIdInputs {
            target_path: Some("/data/other.redb"),
            ..inputs
        },
        PlanIdInputs {
            target_path: None,
            ..inputs
        },
        PlanIdInputs {
            live_fingerprint: Some("live_0123"),
            ..inputs
        },
        PlanIdInputs {
            spec: &changed_spec,
            ..inputs
        },
    ] {
        assert_ne!(plan_id_for(&other, "0.1.0"), base);
    }

    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("orders.redb");
    let cli = plan_path(&spec, &path, ProvisionSurface::Cli)?;
    let serve = plan_path(&spec, &path, ProvisionSurface::Serve)?;
    let other_path = plan_path(
        &spec,
        &temp_dir.path().join("x.redb"),
        ProvisionSurface::Cli,
    )?;
    assert_ne!(cli.plan_id, serve.plan_id);
    assert_ne!(cli.plan_id, other_path.plan_id);
    Ok(())
}

#[test]
fn spellings_of_one_target_path_give_one_plan_id() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    std::fs::create_dir(temp_dir.path().join("sub"))?;
    let direct = temp_dir.path().join("orders.redb");
    let dotted = temp_dir.path().join("sub").join("..").join("orders.redb");
    let spec = orders_spec()?;
    let id = plan_path(&spec, &direct, ProvisionSurface::Cli)?.plan_id;
    assert_eq!(
        plan_path(&spec, &dotted, ProvisionSurface::Cli)?.plan_id,
        id
    );

    #[cfg(unix)]
    {
        let link = temp_dir.path().join("link");
        std::os::unix::fs::symlink(temp_dir.path(), &link)?;
        assert_eq!(
            plan_path(&spec, &link.join("orders.redb"), ProvisionSurface::Cli)?.plan_id,
            id
        );
    }

    // A relative spelling resolves against the working directory, which `cargo test` sets to
    // the crate root.
    let working = std::env::current_dir()?;
    for relative in ["Cargo.toml", "missing-provision-target.redb"] {
        assert_eq!(
            canonical_target_path(Path::new(relative)),
            canonical_target_path(&working.join(relative))
        );
    }
    assert!(!working.join("missing-provision-target.redb").exists());
    Ok(())
}

#[test]
fn live_fingerprint_covers_every_part_of_the_live_state_but_document_counts() -> TestResult {
    let mut db = create_database(DbConfig::new(Profile::InMemory))?;
    db.create_collection_auto_id("orders", orders_fields(), orders_indexes())?;
    let live = LiveState::capture(&db)?;
    let base = live.fingerprint();
    assert!(base.starts_with("live_"));
    assert_eq!(base, LiveState::capture(&db)?.fingerprint());

    let mut counted = live.clone();
    counted.source_row_counts.insert("orders".to_owned(), 99);
    assert_eq!(counted.fingerprint(), base);

    let mutations: Vec<(&str, Mutation)> = vec![
        (
            "catalog",
            Box::new(|live| {
                live.catalog.insert(
                    "social".to_owned(),
                    CatalogEntry::Graph {
                        graph_id: GraphId::new(7),
                    },
                );
            }),
        ),
        (
            "table schemas",
            Box::new(|live| {
                live.table_schemas.insert(
                    "orders".to_owned(),
                    TableSchema::new(vec![ColumnDef::new("id", ColumnType::Int, false)], 0),
                );
            }),
        ),
        (
            "rbac",
            Box::new(|live| {
                live.authz = AuthzPolicy::new([Role::new("reader")
                    .grant(Resource::Collection("orders".to_owned()), Permission::Read)]);
            }),
        ),
        (
            "principals",
            Box::new(|live| {
                live.principals = PrincipalRegistry::new()
                    .with_principal("ada", Principal::new("ada").with_role("reader"));
            }),
        ),
        ("audit flag", Box::new(|live| live.audit_enabled = false)),
        (
            "audit key",
            Box::new(|live| live.audit_key_configured = true),
        ),
        (
            "encryption",
            Box::new(|live| live.encryption_configured = true),
        ),
        (
            "persisted rbac",
            Box::new(|live| live.persisted_authz = AuthzPolicy::new([Role::new("x")])),
        ),
        (
            "persisted audit flag",
            Box::new(|live| live.persisted_audit_enabled = false),
        ),
        (
            "policy config",
            Box::new(|live| {
                live.policy_config.limits = LimitPolicy {
                    max_value_bytes: 7,
                    max_batch_ops: 7,
                };
            }),
        ),
        (
            "session performance",
            Box::new(|live| live.session_performance.cache.max_capacity += 1),
        ),
        (
            "metadata",
            Box::new(|live| live.metadata.schema_version = 2),
        ),
        (
            "record",
            Box::new(|live| live.record = RecordState::Unreadable("x".to_owned())),
        ),
    ];
    for (what, mutate) in mutations {
        let mut changed = live.clone();
        mutate(&mut changed);
        assert_ne!(changed.fingerprint(), base, "{what}");
    }
    Ok(())
}

#[test]
fn plan_ids_follow_the_live_database_but_not_its_documents() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("live.redb");
    let mut db = create_database(DbConfig::on_disk(Profile::Balanced, &path))?;
    db.create_collection(
        "orders",
        CollectionId::new(1),
        orders_fields(),
        orders_indexes(),
    )?;
    let spec = orders_spec()?;
    let first = plan_open(&spec, &db)?;
    assert!(first.live_fingerprint.is_some());
    assert_eq!(first.target.kind, TargetKind::ExistingDatabase);
    assert_eq!(first.target.path, Some(canonical_target_path(&path)));

    let orders = db.collection("orders")?;
    for customer in ["a", "b", "c"] {
        orders.insert(&order_document(customer))?;
    }
    assert_eq!(plan_open(&spec, &db)?.plan_id, first.plan_id);

    let mut seen = vec![first.plan_id.clone()];
    let mut assert_new_id = |db: &Database, what: &str| -> TestResult {
        let id = plan_open(&spec, db)?.plan_id;
        assert!(!seen.contains(&id), "{what} did not change the plan id");
        seen.push(id);
        Ok(())
    };

    db.create_graph("social", GraphId::new(1))?;
    assert_new_id(&db, "a builder create")?;

    db.set_policy_config(&PolicyConfig {
        limits: LimitPolicy {
            max_value_bytes: 1_024,
            max_batch_ops: 16,
        },
        ..PolicyConfig::default()
    })?;
    assert_new_id(&db, "the policy config")?;

    db.set_authz_policy(AuthzPolicy::new([
        Role::new("admin").grant(Resource::System, Permission::Admin)
    ]));
    assert_new_id(&db, "the RBAC policy")?;

    db.set_audit_enabled_as(&Principal::new("root").with_role("admin"), false)?;
    assert_new_id(&db, "the audit flag")?;

    write_record(&db, &record_for(&spec, &first))?;
    assert_new_id(&db, "the provision record")?;
    Ok(())
}

#[test]
fn matching_builder_objects_are_adopted_and_unmanaged_ones_follow_the_policy() -> TestResult {
    let mut db = create_database(DbConfig::new(Profile::InMemory))?;
    db.create_collection(
        "orders",
        CollectionId::new(4),
        orders_fields(),
        orders_indexes(),
    )?;
    db.create_collection_auto_id("extra", orders_fields(), Vec::new())?;
    let mut spec = in_memory(orders_spec()?);

    let plan = plan_rust_api(&spec, &db)?;
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
    let orders = plan
        .objects
        .iter()
        .find(|object| object.name == "orders")
        .ok_or("orders is planned")?;
    assert_eq!(orders.state, ObjectState::Adopt);
    assert!(
        plan.steps
            .iter()
            .any(|step| step.kind == ProvisionStepKind::AdoptObject
                && step.object.as_deref() == Some("orders")
                && step.safety == StepSafety::Additive)
    );
    // Report is the default: a Warning and information-only drift.
    assert!(has_issue(
        &plan,
        "DRIFT_UNMANAGED",
        ValidationSeverity::Warning
    ));
    assert!(plan.objects.iter().any(|object| object.name == "extra"
        && object.state == ObjectState::Unmanaged
        && object.spec_collection.is_none()));

    let ignore = ProvisionOptions {
        unmanaged: Some(UnmanagedPolicy::Ignore),
        ..ProvisionOptions::default()
    };
    let rust_api = env(ProvisionSurface::RustApi);
    let ignored = plan_provision(&spec, ProvisionTargetRef::Open(&db), &ignore, &rust_api)?;
    assert!(!issue_codes(&ignored).contains(&"DRIFT_UNMANAGED"));
    assert!(!ignored.objects.iter().any(|object| object.name == "extra"));

    spec.provisioning = Some(serde_json::from_value(json!({ "unmanaged": "fail" }))?);
    assert_blocked(&plan_rust_api(&spec, &db)?, "DRIFT_UNMANAGED");
    // The option overrides the spec policy.
    let overridden = plan_provision(&spec, ProvisionTargetRef::Open(&db), &ignore, &rust_api)?;
    assert_eq!(overridden.outcome, PlanOutcome::Ready);
    Ok(())
}

#[test]
fn system_versioning_of_an_adopted_table_needs_a_typed_confirmation() -> TestResult {
    let ledger = |versioned: bool| -> Result<DatabaseSpec, Error> {
        Ok(in_memory(spec(
            "in_memory",
            &json!([{
                "name": "ledger", "role": "event_log", "domain": "primary", "indexes": ["primary"],
                "physical": {
                    "kind": "table",
                    "columns": [
                        { "name": "entry_id", "type": "int" },
                        { "name": "amount", "type": "float" }
                    ],
                    "primary_key": "entry_id",
                    "system_versioning": versioned
                }
            }]),
        )?))
    };
    let mut db = create_database(DbConfig::new(Profile::InMemory))?;
    db.create_table(
        "ledger",
        Some(TableSchema::new(
            vec![
                ColumnDef::new("entry_id", ColumnType::Int, false),
                ColumnDef::new("amount", ColumnType::Float, false),
            ],
            0,
        )),
        Vec::new(),
    )?;

    // Adopting the table as it is stays auto-approvable.
    let adoption = plan_rust_api(&ledger(false)?, &db)?;
    assert_eq!(
        adoption.outcome,
        PlanOutcome::Ready,
        "{:#?}",
        adoption.issues
    );
    assert!(adoption.auto_approvable, "{:#?}", adoption.steps);

    // Turning versioning on alters the unmanaged table: additive, but never auto-approved.
    let spec = ledger(true)?;
    let plan = plan_rust_api(&spec, &db)?;
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
    assert!(!plan.auto_approvable);
    let versioning = plan
        .steps
        .iter()
        .find(|step| step.kind == ProvisionStepKind::EnableSystemVersioning)
        .ok_or("versioning is planned")?;
    assert_eq!(versioning.safety, StepSafety::Additive);
    assert!(
        versioning.action.contains("typed confirmation"),
        "{}",
        versioning.action
    );
    let operator = ProvisionActor::local_operator("local:embedded");
    let rust_api = env(ProvisionSurface::RustApi);
    assert!(matches!(
        apply_provision(
            &mut db,
            &operator,
            &spec,
            &Confirmation::AutoApproveAdditive,
            &ProvisionOptions::default(),
            &rust_api,
        ),
        Err(ProvisionError::ConfirmationRequired(_))
    ));
    assert!(!db.catalog().contains_key("ledger_history"));
    assert!(matches!(
        serve_startup_decision(&plan, StartupMode::Additive),
        StartupDecision::Refuse(_)
    ));
    let report = apply_as(
        &mut db,
        &operator,
        &spec,
        &ProvisionOptions::default(),
        &rust_api,
    )?;
    assert_applied(&report);
    assert!(db.catalog().contains_key("ledger_history"));
    Ok(())
}

#[test]
fn differing_live_objects_are_conflicts_with_a_field_diff() -> TestResult {
    let mut db = create_database(DbConfig::new(Profile::InMemory))?;
    db.create_vector_collection("dims", VectorCollectionConfig::new(CollectionId::new(1), 4))?;
    db.create_vector_collection(
        "metrics_l2",
        VectorCollectionConfig::new(CollectionId::new(2), 3).with_metric(VectorMetric::L2),
    )?;
    db.create_collection(
        "docs",
        CollectionId::new(3),
        orders_fields(),
        vec![IndexSpec::new(IndexId::new(1), FieldPath::new(["status"]))],
    )?;
    db.create_table(
        "typed",
        Some(TableSchema::new(
            vec![
                ColumnDef::new("id", ColumnType::Int, false),
                ColumnDef::new("name", ColumnType::Str, false),
            ],
            0,
        )),
        Vec::new(),
    )?;
    db.create_table("kinded", None, Vec::new())?;
    let spec = in_memory(spec(
        "in_memory",
        &json!([
            { "name": "dims", "role": "vector_memory", "domain": "primary", "indexes": ["vector"], "physical": { "dim": 3 } },
            { "name": "metrics_l2", "role": "vector_memory", "domain": "primary", "indexes": ["vector"], "physical": { "dim": 3 } },
            {
                "name": "docs", "role": "document_entity", "domain": "primary", "indexes": ["document"],
                "physical": {
                    "fields": [{ "name": "customer_id", "path": "customer.id", "type": "str" }],
                    "document_indexes": [{ "path": "customer.id" }]
                }
            },
            {
                "name": "typed", "role": "document_entity", "domain": "primary", "indexes": ["primary"],
                "physical": {
                    "kind": "table",
                    "columns": [{ "name": "id", "type": "int" }, { "name": "title", "type": "str" }],
                    "primary_key": "id"
                }
            },
            { "name": "kinded", "role": "document_entity", "domain": "primary", "indexes": ["primary"] }
        ]),
    )?);

    let plan = plan_rust_api(&spec, &db)?;
    assert_blocked(&plan, "PROVISION_OBJECT_CONFLICT");
    for (name, field) in [
        ("dims", "dim"),
        ("metrics_l2", "metric"),
        ("docs", "document_indexes"),
        ("typed", "columns"),
        ("kinded", "kind"),
    ] {
        let object = plan
            .objects
            .iter()
            .find(|object| object.name == name)
            .ok_or(name)?;
        assert_eq!(object.state, ObjectState::Conflict, "{name}");
        assert!(
            object.diff.iter().any(|diff| diff.field == field),
            "{name}: {:#?}",
            object.diff
        );
        assert!(
            plan.steps
                .iter()
                .any(|step| step.object.as_deref() == Some(name)
                    && step.safety == StepSafety::Unsupported
                    && !step.supported)
        );
        assert!(
            plan.drift
                .items
                .iter()
                .any(|item| item.kind == DriftKind::ObjectConflict
                    && item.path == format!("collections.{name}")
                    && item.blocking)
        );
    }
    let dims = plan
        .objects
        .iter()
        .find(|object| object.name == "dims")
        .ok_or("dims")?;
    let diff = dims
        .diff
        .iter()
        .find(|diff| diff.field == "dim")
        .ok_or("dim diff")?;
    assert_eq!(
        (diff.expected.clone(), diff.actual.clone()),
        (json!(3), json!(4))
    );
    Ok(())
}

#[test]
fn unsupported_scopes_and_surfaces_block_the_plan() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("scope.redb");

    let mut at_rest = orders_spec()?;
    at_rest.guarantees.encryption.at_rest = true;
    for surface in [
        ProvisionSurface::Cli,
        ProvisionSurface::Serve,
        ProvisionSurface::ControlPlane,
    ] {
        let plan = plan_path(&at_rest, &path, surface)?;
        assert_blocked(&plan, "GUARANTEE_NOT_ENFORCEABLE");
        assert!(
            plan.guarantees
                .iter()
                .any(|entry| entry.path == "guarantees.encryption.at_rest"
                    && entry.status == EnforcementStatus::NotEnforceable)
        );
        assert!(!plan.drift.in_sync);
    }

    let mut cluster = orders_spec()?;
    cluster.deployment.mode = DeploymentMode::Cluster;
    cluster.topology.replica_count = 3;
    assert_blocked(
        &plan_path(&cluster, &path, ProvisionSurface::Cli)?,
        "PROVISION_DEPLOYMENT_UNSUPPORTED",
    );

    let mut sharded = orders_spec()?;
    sharded.topology.shard_count = 2;
    let plan = plan_path(&sharded, &path, ProvisionSurface::Cli)?;
    assert_blocked(&plan, "PROVISION_DEPLOYMENT_UNSUPPORTED");
    assert!(plan.issues.iter().any(
        |issue| issue.path == "topology" && issue.suggestion.contains("repl::start_cp_cluster")
    ));

    let mut ap = orders_spec()?;
    ap.defaults.replication = ReplicationMode::Ap;
    ap.guarantees.conflict_resolution = ConflictResolution::VectorClock;
    ap.domains[0].mode = ConsistencyMode::EventualAp;
    let plan = plan_path(&ap, &path, ProvisionSurface::Cli)?;
    assert_blocked(&plan, "PROVISION_REPLICATION_UNSUPPORTED");
    assert!(has_issue(
        &plan,
        "DOMAIN_MODE_INCOMPATIBLE_WITH_REPLICATION",
        ValidationSeverity::Error
    ));

    let mut eventual = orders_spec()?;
    eventual.guarantees.conflict_resolution = ConflictResolution::LastWriteWins;
    eventual.domains.push(ConsistencyDomainSpec {
        name: "events".to_owned(),
        mode: ConsistencyMode::EventualAp,
    });
    assert_blocked(
        &plan_path(&eventual, &path, ProvisionSurface::Cli)?,
        "DOMAIN_MODE_INCOMPATIBLE_WITH_REPLICATION",
    );

    let mut custom_extension = orders_spec()?;
    custom_extension.extensions.push(ExtensionManifestRef {
        name: "acme_codec".to_owned(),
        version: "1.0.0".to_owned(),
        stability: ExtensionStability::Stable,
    });
    assert_blocked(
        &plan_path(&custom_extension, &path, ProvisionSurface::Cli)?,
        "PROVISION_EXTENSION_UNAVAILABLE",
    );
    let mut old_extension = orders_spec()?;
    old_extension.extensions.push(ExtensionManifestRef {
        name: "document_index".to_owned(),
        version: "0.9.0".to_owned(),
        stability: ExtensionStability::Stable,
    });
    assert_blocked(
        &plan_path(&old_extension, &path, ProvisionSurface::Cli)?,
        "EXTENSION_VERSION_INCOMPATIBLE",
    );
    let mut experimental = orders_spec()?;
    experimental.extensions.push(ExtensionManifestRef {
        name: "columnar_layout".to_owned(),
        version: "1.0.0".to_owned(),
        stability: ExtensionStability::Experimental,
    });
    let plan = plan_path(&experimental, &path, ProvisionSurface::Cli)?;
    assert_eq!(plan.outcome, PlanOutcome::Ready);
    assert!(has_issue(
        &plan,
        "PROVISION_EXTENSION_EXPERIMENTAL",
        ValidationSeverity::Warning
    ));
    assert!(plan.steps.iter().any(
        |step| step.kind == ProvisionStepKind::RecordMetadata && step.path == "extensions"
    ));
    assert!(!path.exists());
    Ok(())
}

/// The public CP cluster constructors fail closed, so no CP handle reaches the planner from
/// outside the crate; the in-crate simulated one is refused before anything is planned.
#[test]
fn cp_cluster_handles_never_reach_the_planner() -> TestResult {
    let cluster_config = || {
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
    };
    assert!(matches!(
        create_cluster_database(DbConfig::new(Profile::InMemory), cluster_config()),
        Err(ConfigError::Unsupported(_))
    ));
    let simulated =
        create_simulated_cluster_database(DbConfig::new(Profile::InMemory), cluster_config())?;
    assert!(matches!(
        plan_open(&orders_spec()?, &simulated),
        Err(ProvisionError::Db(DbError::Config(
            ConfigError::Unsupported(_)
        )))
    ));
    drop(simulated);
    Ok(())
}

#[test]
fn specs_provisioning_cannot_build_are_blocked() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("blocked.redb");

    let no_dim = spec(
        "balanced",
        &json!([{ "name": "memories", "role": "vector_memory", "domain": "primary", "indexes": ["vector"] }]),
    )?;
    let plan = plan_path(&no_dim, &path, ProvisionSurface::Cli)?;
    assert!(plan.validation.valid);
    assert_blocked(&plan, "PHYSICAL_PARAMETER_REQUIRED");

    for (name, yaml) in [
        (
            "game-save",
            include_str!("../../tests/fixtures/spec-v1/game-save.yaml"),
        ),
        (
            "desktop-embedded",
            include_str!("../../tests/fixtures/spec-v1/desktop-embedded.yaml"),
        ),
        (
            "ai-memory",
            include_str!("../../tests/fixtures/spec-v1/ai-memory.yaml"),
        ),
        (
            "secure-saas",
            include_str!("../../tests/fixtures/spec-v1/secure-saas.yaml"),
        ),
        (
            "analytics",
            include_str!("../../tests/fixtures/spec-v1/analytics.yaml"),
        ),
    ] {
        let template = decode_spec_bytes(yaml.as_bytes(), SpecFormat::Yaml)?;
        let plan = plan_path(&template, &path, ProvisionSurface::Cli)?;
        assert!(plan.validation.valid, "{name}");
        assert_blocked(&plan, "PHYSICAL_PARAMETER_REQUIRED");
    }

    let mut studio = orders_spec()?;
    studio.version = 1;
    studio.collections.clear();
    studio.profile = "studio_custom".to_owned();
    let plan = plan_path(&studio, &path, ProvisionSurface::Cli)?;
    assert!(plan.validation.valid);
    assert_blocked(&plan, "PROVISION_PROFILE_UNRESOLVED");
    assert!(plan.resolved_profile.is_none());

    for (name, code) in [
        ("bad-name", "COLLECTION_NAME_INVALID"),
        ("__secret", "COLLECTION_NAME_RESERVED"),
    ] {
        let mut named = spec(
            "balanced",
            &json!([{ "name": name, "role": "key_value", "domain": "primary", "indexes": ["primary"] }]),
        )?;
        named.version = 1;
        let plan = plan_path(&named, &path, ProvisionSurface::Cli)?;
        assert!(plan.validation.valid, "{name}");
        assert!(has_issue(&plan, code, ValidationSeverity::Warning));
        assert_blocked(&plan, code);

        named.version = DATABASE_SPEC_VERSION_V2;
        let plan = plan_path(&named, &path, ProvisionSurface::Cli)?;
        assert!(!plan.validation.valid, "{name}");
        assert_blocked(&plan, code);
        assert!(
            plan.steps.is_empty(),
            "expected an empty value, got {:?}",
            plan.steps
        );
    }

    let plan = plan_provision(
        &orders_spec()?,
        ProvisionTargetRef::Volatile,
        &ProvisionOptions::default(),
        &env(ProvisionSurface::RustApi),
    )?;
    assert_blocked(&plan, "PROVISION_STORAGE_PATH_REQUIRED");

    let volatile = in_memory(orders_spec()?);
    let plan = plan_provision(
        &volatile,
        ProvisionTargetRef::Volatile,
        &ProvisionOptions::default(),
        &env(ProvisionSurface::Cli),
    )?;
    assert_blocked(&plan, "PROVISION_VOLATILE_ON_SURFACE");
    let plan = plan_provision(
        &volatile,
        ProvisionTargetRef::Volatile,
        &ProvisionOptions::default(),
        &env(ProvisionSurface::RustApi),
    )?;
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
    assert_eq!(plan.target.path, None);
    assert_eq!(plan.target.storage_profile, Some(Profile::InMemory));
    assert_blocked(
        &plan_path(&volatile, &path, ProvisionSurface::RustApi)?,
        "PROFILE_DEPLOYMENT_MISMATCH",
    );
    assert!(!path.exists());
    Ok(())
}

#[test]
fn live_state_problems_block_the_plan() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let empty = spec("balanced", &json!([]))?;

    let balanced = create_database(DbConfig::on_disk(
        Profile::Balanced,
        temp_dir.path().join("balanced.redb"),
    ))?;
    let plan = plan_open(&spec("secure_app", &json!([]))?, &balanced)?;
    assert_blocked(&plan, "PROVISION_ENGINE_MISMATCH");
    assert!(
        plan.drift
            .items
            .iter()
            .any(|item| item.kind == DriftKind::StorageClassMismatch && item.blocking)
    );

    write_record_bytes(&balanced, br#"{"record_version": 99}"#.to_vec())?;
    assert_blocked(
        &plan_open(&empty, &balanced)?,
        "PROVISION_RECORD_UNREADABLE",
    );
    assert!(balanced.provision_state().is_err());
    assert!(matches!(
        export_spec(&balanced),
        Err(ProvisionError::RecordUnreadable(_))
    ));
    drop(balanced);

    let stale_path = temp_dir.path().join("stale.redb");
    let security = SecurityConfig {
        authz_policy: AuthzPolicy::new([
            Role::new("reader").grant(Resource::Collection("orders".to_owned()), Permission::Read)
        ]),
        ..SecurityConfig::default()
    };
    drop(create_database_with_ops(
        DbConfig::on_disk(Profile::Balanced, &stale_path),
        OperationalConfig::new().with_security(security),
    )?);
    let loaded = open_database_with_ops(
        DbConfig::on_disk(Profile::Balanced, &stale_path),
        &OperationalConfig::new(),
    )?;
    let plan = plan_open(&empty, &loaded)?;
    assert!(!issue_codes(&plan).contains(&"PROVISION_SECURITY_STATE_STALE"));
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);

    let mut audited = env(ProvisionSurface::RustApi);
    audited.audit = Some(AuditConfig {
        key_path: Some(temp_dir.path().join("audit.key")),
        anchor_path: None,
    });
    assert!(matches!(
        plan_provision(
            &empty,
            ProvisionTargetRef::Open(&loaded),
            &ProvisionOptions::default(),
            &audited,
        ),
        Err(ProvisionError::Config(ConfigError::Unsupported(_)))
    ));
    let mut encrypted_cli = env(ProvisionSurface::Cli);
    encrypted_cli.encryption = Some(EncryptionConfig::local_envelope(
        temp_dir.path().join("keyring"),
        temp_dir.path().join("kek"),
    ));
    assert!(matches!(
        plan_provision(
            &empty,
            ProvisionTargetRef::Path(&temp_dir.path().join("new.redb")),
            &ProvisionOptions::default(),
            &encrypted_cli,
        ),
        Err(ProvisionError::Config(ConfigError::Unsupported(_)))
    ));
    let mut legacy_key = env(ProvisionSurface::RustApi);
    legacy_key.encryption = Some(EncryptionConfig::file_key(temp_dir.path().join("key")));
    assert!(matches!(
        plan_provision(
            &empty,
            ProvisionTargetRef::Path(&temp_dir.path().join("new.redb")),
            &ProvisionOptions::default(),
            &legacy_key,
        ),
        Err(ProvisionError::Config(ConfigError::Unsupported(_)))
    ));

    assert!(matches!(
        plan_path(&empty, &stale_path, ProvisionSurface::Cli),
        Err(ProvisionError::Locked(_))
    ));
    drop(loaded);
    assert!(matches!(
        plan_path(&empty, &stale_path, ProvisionSurface::Cli),
        Err(ProvisionError::AlreadyInitialized)
    ));
    Ok(())
}

/// Every opener loads the persisted RBAC state, so only an RBAC change held in memory
/// (`set_authz_policy` does not persist) makes a handle stale.
#[test]
fn only_in_memory_rbac_changes_make_a_handle_stale() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let empty = spec("balanced", &json!([]))?;
    let stale_path = temp_dir.path().join("stale.redb");
    let security = SecurityConfig {
        authz_policy: AuthzPolicy::new([
            Role::new("reader").grant(Resource::Collection("orders".to_owned()), Permission::Read)
        ]),
        ..SecurityConfig::default()
    };
    drop(create_database_with_ops(
        DbConfig::on_disk(Profile::Balanced, &stale_path),
        OperationalConfig::new().with_security(security),
    )?);
    let mut plainly_opened = open_database(DbConfig::on_disk(Profile::Balanced, &stale_path))?;
    let plan = plan_open(&empty, &plainly_opened)?;
    assert!(!issue_codes(&plan).contains(&"PROVISION_SECURITY_STATE_STALE"));
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
    plainly_opened.set_authz_policy(AuthzPolicy::default());
    assert_blocked(
        &plan_open(&empty, &plainly_opened)?,
        "PROVISION_SECURITY_STATE_STALE",
    );
    drop(plainly_opened);
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn profile_manifest_changes_are_classified() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let secure = spec("secure_app", &json!([]))?;
    let custom = |knobs: JsonValue| {
        with_custom_profile(
            secure.clone(),
            "orders_custom",
            &json!({ "extends": "secure_app", "knobs": knobs }),
        )
    };

    // Session knobs are a retune; the identity change is metadata.
    let db = create_with_profile(&temp_dir.path().join("cache.redb"), &secure)?;
    let plan = plan_open(&custom(json!({ "cache": { "max_entries": 4096 } }))?, &db)?;
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
    assert!(has_step(
        &plan,
        ProvisionStepKind::RetuneProfile,
        StepSafety::Retune
    ));
    assert!(has_step(
        &plan,
        ProvisionStepKind::AdoptProfile,
        StepSafety::Metadata
    ));
    assert!(!plan.auto_approvable);
    assert!(
        plan.steps
            .iter()
            .any(|step| step.action.contains("cache.max_entries"))
    );

    // Identity only: the same storage profile and knobs under an alias.
    let plan = plan_open(&spec("transactional", &json!([]))?, &db)?;
    let profile_steps = plan
        .steps
        .iter()
        .filter(|step| {
            matches!(
                step.kind,
                ProvisionStepKind::AdoptProfile
                    | ProvisionStepKind::RetuneProfile
                    | ProvisionStepKind::UpgradeStorageFormat
            )
        })
        .map(|step| step.safety)
        .collect::<Vec<_>>();
    assert_eq!(profile_steps, vec![StepSafety::Metadata]);
    assert!(plan.auto_approvable);

    // none -> zstd turns value framing on: a format upgrade behind its flag.
    let zstd = custom(json!({ "compression": { "algorithm": "zstd" } }))?;
    let plan = plan_open(&zstd, &db)?;
    assert_eq!(plan.outcome, PlanOutcome::NeedsFlags);
    assert!(has_step(
        &plan,
        ProvisionStepKind::UpgradeStorageFormat,
        StepSafety::FormatUpgrade
    ));
    assert_eq!(plan.required_flags, vec!["allow_format_upgrade".to_owned()]);
    assert!(has_issue(
        &plan,
        "STEP_REQUIRES_FLAG",
        ValidationSeverity::Error
    ));
    let allowed = ProvisionOptions {
        allow_format_upgrade: true,
        ..ProvisionOptions::default()
    };
    let plan = plan_open_with(&zstd, &db, &allowed)?;
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
    assert!(!plan.auto_approvable);
    drop(db);

    // lz4 -> zstd on a framed database is a retune; turning framing off is refused.
    let lz4 = custom(json!({ "compression": { "algorithm": "lz4" } }))?;
    let framed = create_with_profile(&temp_dir.path().join("framed.redb"), &lz4)?;
    let plan = plan_open(&zstd, &framed)?;
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
    assert!(has_step(
        &plan,
        ProvisionStepKind::RetuneProfile,
        StepSafety::Retune
    ));
    assert!(!step_kinds(&plan).contains(&ProvisionStepKind::UpgradeStorageFormat));
    assert_blocked(&plan_open(&secure, &framed)?, "PROVISION_FORMAT_MISMATCH");
    drop(framed);

    // A provisioning-timing knob rewrites the manifest; existing objects keep their parameters
    // and conflict with the new default.
    let mut series = create_with_profile(&temp_dir.path().join("series.redb"), &secure)?;
    series.create_time_series(TimeSeriesConfig::new("metrics").with_chunk_millis(3_600_000))?;
    let mut chunked = custom(json!({ "time_series": { "default_chunk_millis": 60_000 } }))?;
    chunked.collections = serde_json::from_value(json!([
        { "name": "metrics", "role": "time_series", "domain": "primary", "indexes": ["time_series"] }
    ]))?;
    let plan = plan_open(&chunked, &series)?;
    assert!(has_step(
        &plan,
        ProvisionStepKind::AdoptProfile,
        StepSafety::Metadata
    ));
    assert!(
        plan.steps
            .iter()
            .any(|step| step.action.contains("time_series.default_chunk_millis"))
    );
    assert_blocked(&plan, "PROVISION_OBJECT_CONFLICT");
    let metrics = plan
        .objects
        .iter()
        .find(|object| object.name == "metrics")
        .ok_or("metrics")?;
    assert_eq!(metrics.diff[0].field, "chunk_millis");
    drop(series);

    // A lowered floor is weakening behind its own flag.
    let mut floored_spec = with_custom_profile(
        secure.clone(),
        "orders_custom",
        &json!({ "extends": "secure_app", "floors": { "backup": true } }),
    )?;
    floored_spec.guarantees.backup.enabled = true;
    let floored = create_with_profile(&temp_dir.path().join("floor.redb"), &floored_spec)?;
    let mut lowered = with_custom_profile(
        secure.clone(),
        "orders_custom",
        &json!({ "extends": "secure_app" }),
    )?;
    lowered.guarantees.backup.enabled = true;
    let plan = plan_open(&lowered, &floored)?;
    assert_eq!(plan.outcome, PlanOutcome::NeedsFlags, "{:#?}", plan.issues);
    assert!(has_step(
        &plan,
        ProvisionStepKind::AdoptProfile,
        StepSafety::Weakening
    ));
    assert_eq!(
        plan.required_flags,
        vec!["allow_weakening:custom_profile.floors.backup".to_owned()]
    );
    let allowed = ProvisionOptions {
        allow_weakening: BTreeSet::from(["custom_profile.floors.backup".to_owned()]),
        ..ProvisionOptions::default()
    };
    assert_eq!(
        plan_open_with(&lowered, &floored, &allowed)?.outcome,
        PlanOutcome::Ready
    );
    Ok(())
}

#[test]
fn legacy_databases_are_adopted_as_metadata_or_retune() -> TestResult {
    let db = create_database(DbConfig::new(Profile::InMemory))?;
    let legacy = in_memory(spec("in_memory", &json!([]))?);

    let plan = plan_rust_api(&legacy, &db)?;
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
    let adopt = plan
        .steps
        .iter()
        .find(|step| step.kind == ProvisionStepKind::AdoptProfile)
        .ok_or("adopt profile")?;
    assert_eq!(adopt.safety, StepSafety::Metadata);
    assert!(plan.auto_approvable);

    let tuned = with_custom_profile(
        legacy,
        "game_custom",
        &json!({ "extends": "in_memory", "knobs": { "query": { "timeout_ms": 1_234 } } }),
    )?;
    let plan = plan_rust_api(&tuned, &db)?;
    let adopt = plan
        .steps
        .iter()
        .find(|step| step.kind == ProvisionStepKind::AdoptProfile)
        .ok_or("adopt profile")?;
    assert_eq!(adopt.safety, StepSafety::Retune);
    assert!(!plan.auto_approvable);
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn guarantees_are_reported_honestly_per_surface() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("guarantees.redb");
    let status = |plan: &ProvisionPlan, path: &str| {
        plan.guarantees
            .iter()
            .find(|entry| entry.path == path)
            .map(|entry| entry.status)
    };

    let unprotected = orders_spec()?;
    let plan = plan_path(&unprotected, &path, ProvisionSurface::Cli)?;
    assert_eq!(
        status(&plan, "guarantees.backup.enabled"),
        Some(EnforcementStatus::NotRequested)
    );
    assert_eq!(
        status(&plan, "guarantees.backup.pitr"),
        Some(EnforcementStatus::NotRequested)
    );
    assert!(!issue_codes(&plan).contains(&"GUARANTEE_OPERATOR_MANAGED"));
    assert_eq!(
        status(&plan, "guarantees.write_ack"),
        Some(EnforcementStatus::TriviallySatisfied)
    );
    assert_eq!(
        status(&plan, "guarantees.audit.enabled"),
        Some(EnforcementStatus::EnforcedWithDevelopmentKey)
    );
    assert!(has_issue(
        &plan,
        "AUDIT_DEVELOPMENT_KEY",
        ValidationSeverity::Warning
    ));

    let mut backed_up = orders_spec()?;
    backed_up.guarantees.backup.enabled = true;
    backed_up.guarantees.backup.pitr = true;
    let plan = plan_path(&backed_up, &path, ProvisionSurface::Cli)?;
    assert_eq!(plan.outcome, PlanOutcome::Ready);
    for guarantee in ["guarantees.backup.enabled", "guarantees.backup.pitr"] {
        assert_eq!(
            status(&plan, guarantee),
            Some(EnforcementStatus::OperatorManaged)
        );
        let warning = plan
            .issues
            .iter()
            .find(|issue| issue.code == "GUARANTEE_OPERATOR_MANAGED" && issue.path == guarantee)
            .ok_or(guarantee)?;
        assert!(warning.suggestion.contains("multidb backup full"));
        assert_eq!(warning.severity, ValidationSeverity::Warning);
    }

    let mut keyed = env(ProvisionSurface::RustApi);
    keyed.audit = Some(AuditConfig {
        key_path: Some(temp_dir.path().join("audit.key")),
        anchor_path: None,
    });
    let plan = plan_provision(
        &unprotected,
        ProvisionTargetRef::Path(&path),
        &ProvisionOptions::default(),
        &keyed,
    )?;
    assert_eq!(
        status(&plan, "guarantees.audit.enabled"),
        Some(EnforcementStatus::Enforced)
    );
    assert!(!issue_codes(&plan).contains(&"AUDIT_DEVELOPMENT_KEY"));

    let mut sensitive = orders_spec()?;
    sensitive.guarantees.encryption.at_rest = true;
    sensitive.guarantees.sensitive_data = true;
    let mut encrypted = env(ProvisionSurface::RustApi);
    encrypted.encryption = Some(EncryptionConfig::local_envelope(
        temp_dir.path().join("keyring"),
        temp_dir.path().join("kek"),
    ));
    let plan = plan_provision(
        &sensitive,
        ProvisionTargetRef::Path(&path),
        &ProvisionOptions::default(),
        &encrypted,
    )?;
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
    assert!(plan.auto_approvable);
    assert_eq!(
        status(&plan, "guarantees.encryption.at_rest"),
        Some(EnforcementStatus::Enforced)
    );
    assert_eq!(
        status(&plan, "guarantees.sensitive_data"),
        Some(EnforcementStatus::Enforced)
    );
    // The development audit key warns, but never blocks sensitive data.
    assert!(has_issue(
        &plan,
        "AUDIT_DEVELOPMENT_KEY",
        ValidationSeverity::Warning
    ));
    assert_blocked(
        &plan_path(&sensitive, &path, ProvisionSurface::RustApi)?,
        "GUARANTEE_NOT_ENFORCEABLE",
    );
    assert!(!path.exists());
    Ok(())
}

#[test]
fn rbac_grants_resolve_to_the_physical_resource_of_each_object() -> TestResult {
    let mut spec = full_spec()?;
    spec.security = None;
    let spec = with_security(
        spec,
        &json!({
            "roles": [{
                "name": "everything",
                "grants": [
                    { "object": "orders", "permissions": ["read"] },
                    { "object": "memories", "permissions": ["read"] },
                    { "object": "ledger", "permissions": ["read"] },
                    { "object": "ledger_history", "permissions": ["read"] },
                    { "object": "rollups", "permissions": ["read"] },
                    { "object": "metrics", "permissions": ["read"] },
                    { "object": "social", "permissions": ["read"] },
                    { "object": "orders_notes_fts", "permissions": ["read"] },
                    { "object": "orders_shipping_location_geo", "permissions": ["read"] },
                    { "resource": "database", "permissions": ["write"] }
                ]
            }]
        }),
    )?;
    let desired = project(&spec, &resolved(&spec)?).map_err(|issues| format!("{issues:#?}"))?;
    let security = spec.security.clone().ok_or("security")?;
    let role = desired_role(&security.roles[0], &desired);
    let resources = role.grants().keys().cloned().collect::<BTreeSet<_>>();
    assert_eq!(
        resources,
        BTreeSet::from([
            Resource::Database,
            Resource::Collection("orders".to_owned()),
            Resource::VectorCollection("memories".to_owned()),
            Resource::Table("ledger".to_owned()),
            Resource::Table("ledger_history".to_owned()),
            Resource::Table("rollups".to_owned()),
            Resource::TimeSeries("metrics".to_owned()),
            Resource::Graph("social".to_owned()),
            Resource::FullTextIndex("orders_notes_fts".to_owned()),
            Resource::GeoIndex("orders_shipping_location_geo".to_owned()),
        ])
    );
    assert!(role.allows(&Resource::Database, Permission::Write));
    assert!(!role.allows(
        &Resource::Collection("orders".to_owned()),
        Permission::Admin
    ));
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn rbac_changes_are_classified_and_reserved_names_refused() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("rbac.redb");

    for security in [
        json!({ "roles": [{ "name": "admin", "grants": [] }] }),
        json!({ "roles": [], "principals": [{ "user": "root", "roles": [] }] }),
        json!({ "roles": [], "principals": [{ "user": "ada", "principal": "admin", "roles": [] }] }),
    ] {
        let reserved = with_security(orders_spec()?, &security)?;
        assert_blocked(
            &plan_path(&reserved, &path, ProvisionSurface::Cli)?,
            "SECURITY_RESERVED_NAME",
        );
    }

    let index_only = with_security(
        full_spec()?,
        &json!({
            "roles": [{ "name": "searcher", "grants": [{ "object": "orders_notes_fts", "permissions": ["read"] }] }]
        }),
    )?;
    let plan = plan_path(&index_only, &path, ProvisionSurface::Cli)?;
    assert_eq!(plan.outcome, PlanOutcome::Ready);
    assert!(has_issue(
        &plan,
        "SECURITY_INDEX_GRANT_WITHOUT_SOURCE_READ",
        ValidationSeverity::Warning
    ));

    let writer = Role::new("writer")
        .grant(Resource::Collection("orders".to_owned()), Permission::Read)
        .grant(Resource::Collection("orders".to_owned()), Permission::Write);
    let operator = Role::new("ops").grant(Resource::Database, Permission::Admin);
    let admin = Role::new("admin").grant(Resource::System, Permission::Admin);
    let security = SecurityConfig {
        authz_policy: AuthzPolicy::new([admin, writer, operator, Role::new("legacy")]),
        principals: PrincipalRegistry::new()
            .with_principal("ops", Principal::new("ops").with_role("ops")),
        ..SecurityConfig::default()
    };
    let mut db = create_database_with_ops(
        DbConfig::on_disk(Profile::Balanced, &path),
        OperationalConfig::new().with_security(security),
    )?;
    db.create_collection(
        "orders",
        CollectionId::new(1),
        orders_fields(),
        orders_indexes(),
    )?;
    let roles = |writer: &[&str], ops: JsonValue| {
        json!({
            "roles": [
                { "name": "writer", "grants": [{ "object": "orders", "permissions": writer }] },
                { "name": "ops", "grants": [ops] }
            ],
            "principals": [{ "user": "ops", "roles": ["ops"] }]
        })
    };
    let database_admin = json!({ "resource": "database", "permissions": ["admin"] });
    let managed = with_security(
        orders_spec()?,
        &roles(&["read", "write"], database_admin.clone()),
    )?;

    // Without a record the identical roles and principal are adopted as metadata; they still
    // need a confirmation, since RBAC changes on an existing database are never auto-approved.
    let plan = plan_open(&managed, &db)?;
    let security_steps = plan
        .steps
        .iter()
        .filter(|step| step.kind == ProvisionStepKind::ApplySecurity)
        .map(|step| (step.path.as_str(), step.safety))
        .collect::<Vec<_>>();
    assert_eq!(
        security_steps,
        vec![
            ("security.roles.writer", StepSafety::Metadata),
            ("security.roles.ops", StepSafety::Metadata),
            ("security.principals.ops", StepSafety::Metadata),
        ]
    );
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
    assert!(!plan.auto_approvable);
    // An unmanaged role with other grants is never changed.
    let renamed = with_security(
        orders_spec()?,
        &json!({ "roles": [{ "name": "legacy", "grants": [{ "object": "orders", "permissions": ["read"] }] }] }),
    )?;
    assert_blocked(&plan_open(&renamed, &db)?, "PROVISION_OBJECT_CONFLICT");

    write_record(&db, &record_for(&managed, &plan))?;
    let plan = plan_open(&managed, &db)?;
    assert!(
        !step_kinds(&plan).contains(&ProvisionStepKind::ApplySecurity),
        "{:#?}",
        plan.steps
    );

    let revoked = with_security(orders_spec()?, &roles(&["read"], database_admin.clone()))?;
    let plan = plan_open(&revoked, &db)?;
    assert_eq!(plan.outcome, PlanOutcome::NeedsFlags, "{:#?}", plan.issues);
    assert_eq!(
        plan.required_flags,
        vec!["allow_weakening:security.roles.writer".to_owned()]
    );
    let allowed = ProvisionOptions {
        allow_weakening: BTreeSet::from(["security.roles.writer".to_owned()]),
        ..ProvisionOptions::default()
    };
    assert_eq!(
        plan_open_with(&revoked, &db, &allowed)?.outcome,
        PlanOutcome::Ready
    );

    let mut dropped = managed.clone();
    if let Some(security) = dropped.security.as_mut() {
        security.roles.retain(|role| role.name != "writer");
    }
    let plan = plan_open(&dropped, &db)?;
    assert!(
        plan.steps
            .iter()
            .any(|step| step.kind == ProvisionStepKind::ApplySecurity
                && step.safety == StepSafety::Destructive
                && step.requires_flag.as_deref()
                    == Some("allow_destructive:security.roles.writer"))
    );

    let locked_out = with_security(
        orders_spec()?,
        &roles(
            &["read", "write"],
            json!({ "object": "orders", "permissions": ["read"] }),
        ),
    )?;
    let opts = ProvisionOptions {
        allow_weakening: BTreeSet::from(["security.roles.ops".to_owned()]),
        ..ProvisionOptions::default()
    };
    let mut actor = env(ProvisionSurface::ControlPlane);
    actor.principal = Some(Principal::new("ops").with_role("ops"));
    let plan = plan_provision(&locked_out, ProvisionTargetRef::Open(&db), &opts, &actor)?;
    assert_blocked(&plan, "RBAC_SELF_LOCKOUT");
    // The bootstrap administrator role is never touched, so its principals keep system
    // administration.
    actor.principal = Some(Principal::new("root").with_role("admin"));
    let plan = plan_provision(&locked_out, ProvisionTargetRef::Open(&db), &opts, &actor)?;
    assert!(!issue_codes(&plan).contains(&"RBAC_SELF_LOCKOUT"));
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
    Ok(())
}

#[test]
fn a_role_that_unmanaged_users_already_hold_is_named_in_its_step() -> TestResult {
    let mut db = create_database(DbConfig::new(Profile::InMemory))?;
    // A user still assigned a role name the policy no longer defines, and one the spec manages.
    db.set_security_state(
        AuthzPolicy::default(),
        PrincipalRegistry::new()
            .with_principal(
                "legacy_svc",
                Principal::new("legacy").with_role("order_writer"),
            )
            .with_principal(
                "orders_svc",
                Principal::new("orders_svc").with_role("order_writer"),
            ),
    )?;
    let spec = security(
        in_memory(orders_spec()?),
        &json!([{ "object": "orders", "permissions": ["read"] }]),
    )?;
    let plan = plan_rust_api(&spec, &db)?;
    assert!(has_issue(
        &plan,
        "SECURITY_ROLE_ALREADY_ASSIGNED",
        ValidationSeverity::Warning
    ));
    let warning = plan
        .issues
        .iter()
        .find(|found| found.code == "SECURITY_ROLE_ALREADY_ASSIGNED")
        .ok_or("the warning")?;
    assert_eq!(warning.path, "security.roles.order_writer");
    assert!(
        warning.message.contains("legacy_svc"),
        "{}",
        warning.message
    );
    assert!(
        !warning.message.contains("orders_svc"),
        "{}",
        warning.message
    );
    let created = plan
        .steps
        .iter()
        .find(|step| step.path == "security.roles.order_writer")
        .ok_or("the role step")?;
    assert!(created.action.contains("legacy_svc"), "{}", created.action);

    // Without a stray holder there is no warning.
    db.set_security_state(AuthzPolicy::default(), PrincipalRegistry::new())?;
    let plan = plan_rust_api(&spec, &db)?;
    assert!(!issue_codes(&plan).contains(&"SECURITY_ROLE_ALREADY_ASSIGNED"));
    Ok(())
}

#[test]
fn audit_must_be_on_for_every_apply() -> TestResult {
    let mut db = create_database(DbConfig::new(Profile::InMemory))?;
    let admin = Principal::new("root").with_role("admin");
    db.set_authz_policy(AuthzPolicy::new([
        Role::new("admin").grant(Resource::System, Permission::Admin)
    ]));
    let on = in_memory(spec("in_memory", &json!([]))?);
    let mut off = on.clone();
    off.guarantees.audit.enabled = false;

    // The in-memory policy was never persisted, so the handle is stale.
    assert_blocked(&plan_rust_api(&off, &db)?, "PROVISION_SECURITY_STATE_STALE");
    db.set_audit_enabled_as(&admin, true)?;

    // Audit on, spec off: disabling is weakening and runs last.
    let plan = plan_rust_api(&off, &db)?;
    assert_eq!(plan.outcome, PlanOutcome::NeedsFlags, "{:#?}", plan.issues);
    assert_eq!(
        plan.steps.last().map(|step| (step.kind, step.safety)),
        Some((ProvisionStepKind::DisableAudit, StepSafety::Weakening))
    );
    assert_eq!(
        plan.required_flags,
        vec!["allow_weakening:guarantees.audit.enabled".to_owned()]
    );

    // Audit off, spec on: enabling comes first.
    db.set_audit_enabled_as(&admin, false)?;
    let plan = plan_rust_api(&on, &db)?;
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
    assert_eq!(
        plan.steps.first().map(|step| (step.kind, step.safety)),
        Some((ProvisionStepKind::EnableAudit, StepSafety::Strengthening))
    );

    // Audit off and kept off: nothing can be applied.
    assert_blocked(&plan_rust_api(&off, &db)?, "AUDIT_REQUIRED_FOR_APPLY");

    // A new database is created with audit on, so a spec without audit disables it last.
    let plan = plan_provision(
        &off,
        ProvisionTargetRef::Volatile,
        &ProvisionOptions::default(),
        &env(ProvisionSurface::RustApi),
    )?;
    assert_eq!(plan.outcome, PlanOutcome::NeedsFlags);
    assert_eq!(
        plan.steps.last().map(|step| step.kind),
        Some(ProvisionStepKind::DisableAudit)
    );
    Ok(())
}

#[test]
fn records_drive_release_orphans_and_interrupted_applies() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let base = orders_spec()?;
    let mut db = create_with_profile(&temp_dir.path().join("record.redb"), &base)?;
    db.create_collection(
        "orders",
        CollectionId::new(1),
        orders_fields(),
        orders_indexes(),
    )?;
    db.create_collection_auto_id("old", orders_fields(), Vec::new())?;
    let mut declared = base.clone();
    declared
        .collections
        .extend(serde_json::from_value::<Vec<_>>(json!([
            { "name": "old", "role": "document_entity", "domain": "primary", "indexes": ["primary"],
              "physical": { "fields": [{ "name": "customer_id", "path": "customer.id", "type": "str" }] } }
        ]))?);
    let plan = plan_open(&declared, &db)?;
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
    let mut record = record_for(&declared, &plan);
    record.objects.insert(
        "ghost".to_owned(),
        ManagedObject {
            spec_collection: "ghost".to_owned(),
            kind: ObjectKind::Graph,
            adopted: false,
            allocated_id: Some(9),
        },
    );
    write_record(&db, &record)?;

    // `old` is managed and no longer declared: released behind a destructive flag, data kept.
    // `ghost` is managed but gone from both the database and the spec: forgotten.
    let plan = plan_open(&base, &db)?;
    assert_eq!(plan.outcome, PlanOutcome::NeedsFlags, "{:#?}", plan.issues);
    assert!(
        plan.steps
            .iter()
            .any(|step| step.kind == ProvisionStepKind::ReleaseObject
                && step.safety == StepSafety::Destructive
                && step.requires_flag.as_deref() == Some("allow_destructive:collections.old"))
    );
    assert!(
        plan.steps
            .iter()
            .any(|step| step.kind == ProvisionStepKind::ReleaseObject
                && step.object.as_deref() == Some("ghost")
                && step.safety == StepSafety::Metadata)
    );
    assert!(has_issue(
        &plan,
        "DRIFT_ORPHAN",
        ValidationSeverity::Warning
    ));
    assert!(
        plan.drift
            .items
            .iter()
            .any(|item| item.kind == DriftKind::OrphanedObject && item.path == "collections.ghost")
    );
    assert!(
        plan.objects
            .iter()
            .any(|object| object.name == "old" && object.state == ObjectState::Release)
    );
    assert!(plan.spec_migration.is_some());
    assert!(plan.rollback.description.contains("never drops"));

    // A declared orphan is created again.
    let mut ghost = declared.clone();
    ghost
        .collections
        .extend(serde_json::from_value::<Vec<_>>(json!([
            { "name": "ghost", "role": "graph", "domain": "primary", "indexes": ["graph"] }
        ]))?);
    let plan = plan_open(&ghost, &db)?;
    assert!(
        plan.steps
            .iter()
            .any(|step| step.kind == ProvisionStepKind::CreateGraph
                && step.object.as_deref() == Some("ghost"))
    );
    assert!(has_issue(
        &plan,
        "DRIFT_ORPHAN",
        ValidationSeverity::Warning
    ));

    record.status = ProvisionRecordStatus::Failed;
    record.failed_step = Some("pstep-003".to_owned());
    write_record(&db, &record)?;
    let plan = plan_open(&declared, &db)?;
    assert!(has_issue(
        &plan,
        "DRIFT_INTERRUPTED_APPLY",
        ValidationSeverity::Warning
    ));
    assert!(
        plan.drift
            .items
            .iter()
            .any(|item| item.kind == DriftKind::InterruptedApply)
    );
    assert!(step_kinds(&plan).contains(&ProvisionStepKind::RecordAppliedSpec));
    Ok(())
}

#[test]
fn dry_runs_never_write() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let missing = temp_dir.path().join("missing").join("db.redb");
    let new_plan = plan_path(&full_spec()?, &missing, ProvisionSurface::Cli)?;
    assert_eq!(new_plan.target.kind, TargetKind::NewDatabase);
    assert!(!missing.exists());
    assert!(!temp_dir.path().join("missing").exists());

    let path = temp_dir.path().join("live.redb");
    let mut db = create_database(DbConfig::on_disk(Profile::Balanced, &path))?;
    let orders = db.create_collection(
        "orders",
        CollectionId::new(1),
        orders_fields(),
        orders_indexes(),
    )?;
    orders.insert(&order_document("a"))?;
    db.create_graph("strays", GraphId::new(1))?;
    write_record(&db, &record_for(&orders_spec()?, &new_plan))?;
    let tables = [
        "__meta",
        "__catalog__",
        "__authz_policy",
        PROVISION_TABLE,
        "__audit_log",
        "__audit_head",
        "__schema__",
    ];
    let before = ranges(&db, &tables)?;
    assert!(!before[0].is_empty() && !before[1].is_empty() && !before[3].is_empty());

    let mut spec = full_spec()?;
    spec.guarantees.audit.enabled = false;
    spec.guarantees.backup.enabled = true;
    let plan = plan_open(&spec, &db)?;
    assert!(!plan.steps.is_empty(), "expected a non-empty value");
    drift(&db, &spec, &ProvisionOptions::default())?;
    export_spec(&db)?;
    assert_eq!(ranges(&db, &tables)?, before);
    Ok(())
}

#[test]
fn full_text_and_geo_on_populated_sources_show_the_document_count() -> TestResult {
    let mut db = create_database(DbConfig::new(Profile::InMemory))?;
    let orders = db.create_collection(
        "orders",
        CollectionId::new(1),
        orders_fields(),
        orders_indexes(),
    )?;
    for customer in ["a", "b", "c"] {
        orders.insert(&order_document(customer))?;
    }
    assert_eq!(db.document_count(CollectionId::new(1))?, 3);
    let spec = in_memory(full_spec()?);
    let plan = plan_rust_api(&spec, &db)?;
    for kind in [
        ProvisionStepKind::CreateFullTextIndex,
        ProvisionStepKind::CreateGeoIndex,
    ] {
        let step = plan
            .steps
            .iter()
            .find(|step| step.kind == kind)
            .ok_or("index step")?;
        assert!(
            step.action
                .contains("3 documents, which creating the index backfills"),
            "{}",
            step.action
        );
        assert_eq!(step.impact.cpu, ImpactLevel::Medium);
        assert_eq!(step.impact.risk, RiskLevel::Low);
        assert!(!step.impact.requires_backup);
    }

    // The apply backfills both indexes, so the documents written before it are found.
    assert_applied(&apply_as(
        &mut db,
        &cli_operator(),
        &spec,
        &ProvisionOptions::default(),
        &env(ProvisionSurface::RustApi),
    )?);
    let hits = db
        .full_text_index("orders_notes_fts")?
        .search("fragile", 10)?;
    assert_eq!(hits.len(), 3);
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn export_then_plan_only_adopts_and_records() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let csv_dir = temp_dir.path().join("csv");
    std::fs::create_dir(&csv_dir)?;
    std::fs::write(csv_dir.join("users.csv"), "1,Ada,37\n")?;
    let mut db = create_with_profile(
        &temp_dir.path().join("export.redb"),
        &spec("balanced", &json!([]))?,
    )?;
    db.create_collection(
        "orders",
        CollectionId::new(2),
        orders_fields(),
        orders_indexes(),
    )?;
    db.create_full_text_index(FullTextIndexConfig::collection(
        "orders_notes_fts",
        CollectionId::new(2),
        FieldPath::new(["notes"]),
    ))?;
    db.create_geo_index(GeoIndexConfig::new(
        "where_shipped",
        CollectionId::new(2),
        FieldPath::new(["shipping", "location"]),
    ))?;
    db.create_vector_collection(
        "memories",
        VectorCollectionConfig::new(CollectionId::new(5), 3).with_metric(VectorMetric::L2),
    )?;
    let schema = TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("name", ColumnType::Str, false),
            ColumnDef::new("age", ColumnType::Int, false),
        ],
        0,
    );
    db.create_table(
        "ledger",
        Some(schema.clone()),
        vec![RelIndexSpec::new(3, 1)],
    )?;
    db.enable_system_versioning("ledger", TemporalRetention::default())?;
    db.create_table("plain", Some(schema.clone()), Vec::new())?;
    db.create_table_with_layout(
        "rollups",
        Some(schema.clone()),
        Vec::new(),
        TableLayout::Columnar,
    )?;
    db.create_table(
        "partial",
        Some(schema.clone()),
        vec![RelIndexSpec::new(1, 1).with_predicate(RelPredicate::Eq {
            expression: RelIndexExpression::Column(1),
            value: Value::Str("Ada".to_owned()),
        })],
    )?;
    db.create_materialized_view_object(&MaterializedViewSpec {
        name: "plain_by_name".to_owned(),
        source_table: "plain".to_owned(),
        filter: None,
        group_by: 1,
        aggregates: vec![AggregateSpec {
            output_name: "count".to_owned(),
            kind: AggregateKind::Count,
            column: None,
        }],
    })?;
    db.create_time_series(TimeSeriesConfig::new("metrics").with_chunk_millis(60_000))?;
    db.create_graph("social", GraphId::new(3))?;
    db.create_foreign_table_default(
        "foreign_users",
        schema,
        crate::federation::ForeignSource::Csv {
            uri: crate::cloud::ObjectStoreUri::from_local_dir(&csv_dir),
            path: "users.csv".to_owned(),
            has_header: false,
        },
    )?;
    let admin = Principal::new("root").with_role("admin");
    let admin_role = Role::new("admin").grant(Resource::System, Permission::Admin);
    db.set_authz_policy(AuthzPolicy::new([admin_role.clone()]));
    db.set_authz_policy_as(
        &admin,
        AuthzPolicy::new([
            admin_role,
            Role::new("reader")
                .grant(Resource::Collection("orders".to_owned()), Permission::Read)
                .grant(
                    Resource::FullTextIndex("orders_notes_fts".to_owned()),
                    Permission::Read,
                ),
            Role::new("operator").grant(Resource::System, Permission::Admin),
        ]),
    )?;
    db.set_principal_registry_as(
        &admin,
        PrincipalRegistry::new()
            .with_principal("ada", Principal::new("ada").with_role("reader"))
            .with_principal("root", Principal::new("root").with_role("admin")),
    )?;

    let exported = export_spec(&db)?;
    assert_eq!(exported.source, ExportSource::Inferred);
    let spec = &exported.spec;
    assert_eq!(spec.version, DATABASE_SPEC_VERSION_V2);
    assert_eq!(spec.profile, "balanced");
    let report = GuaranteeValidator::validate(spec);
    assert!(report.valid, "{:#?}", report.issues);
    let hints = &spec.operation_hints;
    assert!(
        hints
            .get("export.inferred_roles")
            .is_some_and(|roles| roles.contains("orders=document_entity")
                && roles.contains("rollups=analytics")
                && roles.contains("memories=vector_memory")),
        "{hints:#?}"
    );
    assert!(
        hints
            .get("export.unsupported_objects")
            .is_some_and(|objects| objects.contains("foreign_users")
                && objects.contains("plain_by_name")
                && objects.contains("partial.index_1")
                && objects.contains("role operator")),
        "{hints:#?}"
    );
    let security = spec.security.as_ref().ok_or("security")?;
    assert_eq!(
        security
            .roles
            .iter()
            .map(|role| role.name.as_str())
            .collect::<Vec<_>>(),
        vec!["reader"]
    );
    assert_eq!(security.principals.len(), 1);

    let plan = plan_open(spec, &db)?;
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
    for step in &plan.steps {
        assert!(
            matches!(
                step.kind,
                ProvisionStepKind::AdoptObject
                    | ProvisionStepKind::AdoptProfile
                    | ProvisionStepKind::ApplySecurity
                    | ProvisionStepKind::RecordMetadata
                    | ProvisionStepKind::RecordAppliedSpec
            ),
            "{step:#?}"
        );
        if step.kind == ProvisionStepKind::ApplySecurity {
            assert_eq!(step.safety, StepSafety::Metadata, "{step:#?}");
        }
    }
    let adopted = plan
        .steps
        .iter()
        .filter(|step| step.kind == ProvisionStepKind::AdoptObject)
        .filter_map(|step| step.object.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        adopted,
        [
            "ledger",
            "ledger_history",
            "memories",
            "metrics",
            "orders",
            "orders_notes_fts",
            "plain",
            "rollups",
            "social",
            "where_shipped",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    );

    // With a record, export returns the applied spec.
    write_record(&db, &record_for(spec, &plan))?;
    let recorded = export_spec(&db)?;
    assert_eq!(recorded.source, ExportSource::AppliedRecord);
    assert_eq!(&recorded.spec, spec);
    Ok(())
}

#[test]
fn exported_specs_meet_the_floors_and_rebuild_the_custom_profile() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let mut production = spec("production_cp", &json!([]))?;
    production.guarantees.backup.enabled = true;
    let db = create_with_profile(&temp_dir.path().join("production.redb"), &production)?;
    let exported = export_spec(&db)?;
    assert_eq!(exported.spec.profile, "production_cp");
    assert!(exported.spec.guarantees.backup.enabled);
    let report = GuaranteeValidator::validate(&exported.spec);
    assert!(report.valid, "{:#?}", report.issues);
    let plan = plan_open(&exported.spec, &db)?;
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
    assert!(
        plan.guarantees
            .iter()
            .any(|entry| entry.path == "guarantees.backup.enabled"
                && entry.status == EnforcementStatus::OperatorManaged)
    );
    drop(db);

    let custom = with_custom_profile(
        spec("secure_app", &json!([]))?,
        "orders_custom",
        &json!({
            "extends": "secure_app",
            "knobs": { "cache": { "max_entries": 4096 }, "compression": { "algorithm": "lz4" } },
            "floors": { "audit": true },
            "compatible_roles": { "add": ["graph"] }
        }),
    )?;
    let db = create_with_profile(&temp_dir.path().join("custom.redb"), &custom)?;
    let exported = export_spec(&db)?;
    assert_eq!(exported.spec.profile, "orders_custom");
    assert_eq!(
        resolved(&exported.spec)?.digest,
        resolved(&custom)?.digest,
        "{:#?}",
        exported.spec.custom_profile
    );
    let plan = plan_open(&exported.spec, &db)?;
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
    assert!(
        !plan.steps.iter().any(|step| matches!(
            step.kind,
            ProvisionStepKind::AdoptProfile
                | ProvisionStepKind::RetuneProfile
                | ProvisionStepKind::UpgradeStorageFormat
        )),
        "{:#?}",
        plan.steps
    );
    Ok(())
}

#[test]
fn drift_is_in_sync_for_a_matching_database_and_reports_differences() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let mut spec = orders_spec()?;
    spec.guarantees.backup.enabled = true;
    let mut db = create_with_profile(&temp_dir.path().join("drift.redb"), &spec)?;
    db.create_collection(
        "orders",
        CollectionId::new(1),
        orders_fields(),
        orders_indexes(),
    )?;

    let before = drift(&db, &spec, &ProvisionOptions::default())?;
    assert!(!before.in_sync);
    assert!(
        before
            .items
            .iter()
            .any(|item| item.resolution == DriftResolution::Adopt)
    );
    let plan = plan_open(&spec, &db)?;
    write_record(&db, &record_for(&spec, &plan))?;

    let report = drift(&db, &spec, &ProvisionOptions::default())?;
    assert!(report.in_sync, "{:#?}", report.items);
    assert!(report.items.is_empty(), "{:#?}", report.items);
    let plan = plan_open(&spec, &db)?;
    assert_eq!(plan.outcome, PlanOutcome::Noop, "{:#?}", plan.steps);
    assert_eq!(plan.required_confirmation, None);
    assert!(plan.drift.in_sync);
    // Operator-managed backups and the development audit key are statuses, not drift.
    assert!(
        plan.guarantees
            .iter()
            .any(|entry| entry.status == EnforcementStatus::OperatorManaged)
    );
    assert!(
        plan.guarantees
            .iter()
            .any(|entry| entry.status == EnforcementStatus::EnforcedWithDevelopmentKey)
    );

    // An unmanaged object is information under report, drift under fail.
    db.create_graph("strays", GraphId::new(1))?;
    assert!(drift(&db, &spec, &ProvisionOptions::default())?.in_sync);
    let fail = ProvisionOptions {
        unmanaged: Some(UnmanagedPolicy::Fail),
        ..ProvisionOptions::default()
    };
    assert!(!drift(&db, &spec, &fail)?.in_sync);

    // The storage path is a default location, not identity: moving it changes nothing.
    let mut moved = spec.clone();
    moved.deployment.storage_path = Some("/srv/multidb/elsewhere.redb".to_owned());
    let report = drift(&db, &moved, &ProvisionOptions::default())?;
    assert!(report.in_sync, "{:#?}", report.items);
    assert!(
        report
            .items
            .iter()
            .all(|item| item.kind != DriftKind::SpecChanged)
    );
    let plan = plan_open(&moved, &db)?;
    assert_eq!(plan.outcome, PlanOutcome::Noop, "{:#?}", plan.steps);
    assert_ne!(plan.spec_digest, spec_digest(&spec));
    moved.deployment.storage_path = None;
    assert_eq!(plan_open(&moved, &db)?.outcome, PlanOutcome::Noop);

    let mut changed = spec.clone();
    changed.name = "renamed".to_owned();
    let report = drift(&db, &changed, &ProvisionOptions::default())?;
    assert!(!report.in_sync);
    assert!(
        report
            .items
            .iter()
            .any(|item| item.kind == DriftKind::SpecChanged)
    );

    let mut invalid = spec.clone();
    invalid.collections[0].name = "bad-name".to_owned();
    assert!(matches!(
        drift(&db, &invalid, &ProvisionOptions::default()),
        Err(ProvisionError::InvalidSpec(_))
    ));
    Ok(())
}

#[test]
fn records_decode_only_their_own_version() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let plan = plan_path(
        &orders_spec()?,
        &temp_dir.path().join("x.redb"),
        ProvisionSurface::Cli,
    )?;
    let record = record_for(&orders_spec()?, &plan);
    let bytes = serde_json::to_vec(&record)?;
    assert_eq!(
        decode_record(&bytes),
        RecordState::Readable(Box::new(record.clone()))
    );

    let mut newer = serde_json::to_value(&record)?;
    newer["record_version"] = json!(2);
    assert!(matches!(
        decode_record(&serde_json::to_vec(&newer)?),
        RecordState::Unreadable(reason) if reason.contains("newer build")
    ));
    let mut unknown = serde_json::to_value(&record)?;
    unknown["unexpected"] = json!(true);
    assert!(matches!(
        decode_record(&serde_json::to_vec(&unknown)?),
        RecordState::Unreadable(_)
    ));
    assert!(matches!(
        decode_record(b"not json"),
        RecordState::Unreadable(_)
    ));
    assert!(matches!(decode_record(b"{}"), RecordState::Unreadable(_)));

    // Fields added after the first record default.
    let mut minimal = serde_json::to_value(&record)?;
    if let Some(object) = minimal.as_object_mut() {
        for field in [
            "released_objects",
            "flags",
            "seed_digest",
            "completed_steps",
        ] {
            object.remove(field);
        }
    }
    assert!(matches!(
        decode_record(&serde_json::to_vec(&minimal)?),
        RecordState::Readable(_)
    ));
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Applying plans
// ---------------------------------------------------------------------------------------------

fn cli_operator() -> ProvisionActor {
    ProvisionActor::local_operator("local:cli")
}

/// Plans `spec` against `db` and applies it as `actor`, confirming the plan's id.
fn apply_as(
    db: &mut Database,
    actor: &ProvisionActor,
    spec: &DatabaseSpec,
    opts: &ProvisionOptions,
    environment: &ProvisionEnvironment,
) -> Result<ProvisionApplyReport, ProvisionError> {
    let plan = plan_provision(spec, ProvisionTargetRef::Open(db), opts, environment)?;
    apply_provision(
        db,
        actor,
        spec,
        &Confirmation::PlanId(plan.plan_id),
        opts,
        environment,
    )
}

/// Plans and applies `spec` on the CLI surface as `local:cli`.
fn apply_cli(
    db: &mut Database,
    spec: &DatabaseSpec,
    opts: &ProvisionOptions,
) -> Result<ProvisionApplyReport, ProvisionError> {
    apply_as(db, &cli_operator(), spec, opts, &env(ProvisionSurface::Cli))
}

fn assert_applied(report: &ProvisionApplyReport) {
    assert_eq!(report.status, ProvisionOutcome::Applied, "{report:#?}");
    assert!(report.check.data_mutated && report.check.audit_recorded);
    assert!(report.failed_step.is_none());
    assert!(
        report
            .applied_steps
            .iter()
            .all(|step| step.outcome != StepOutcome::Failed)
    );
}

fn actions(events: &[AuditEvent]) -> Vec<&str> {
    events.iter().map(|event| event.action.as_str()).collect()
}

/// A new in-memory database with the resolved profile of `spec`.
fn create_in_memory(spec: &DatabaseSpec) -> Result<Database, Error> {
    let binding = resolved(spec)?.bind(ReplicationKind::Cp, None)?;
    Ok(create_database_with_profile(
        &binding,
        OperationalConfig::new(),
    )?)
}

/// The commit-log records committed after `lsn`.
fn commit_records_after(db: &Database, lsn: u64) -> Result<Vec<txn::CommitLogRecord>, Error> {
    Ok(
        Replication::range(db, txn::COMMIT_LOG_TABLE, &[], &[], ReadConsistency::Strong)?
            .into_iter()
            .map(|(_, value)| txn::decode_commit_log_record(&value))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|record| record.txn_id > lsn)
            .collect(),
    )
}

fn latest_lsn(db: &Database) -> Result<u64, Error> {
    Ok(commit_records_after(db, 0)?
        .iter()
        .map(|record| record.txn_id)
        .max()
        .unwrap_or_default())
}

/// The keys of `table` each commit-log record after `lsn` writes, for the records that write it.
fn table_commits(db: &Database, lsn: u64, table: &str) -> Result<Vec<BTreeSet<Vec<u8>>>, Error> {
    Ok(commit_records_after(db, lsn)?
        .into_iter()
        .map(|record| {
            record
                .writes
                .into_iter()
                .filter(|write| write.table == table)
                .map(|write| write.key)
                .collect::<BTreeSet<_>>()
        })
        .filter(|keys| !keys.is_empty())
        .collect())
}

/// An order with notes and a shipping location.
fn shipped_order(customer: &str, notes: &str, lon: f64, lat: f64) -> Value {
    Value::Object(
        [
            (
                "customer".to_owned(),
                Value::Object([("id".to_owned(), Value::Str(customer.to_owned()))].into()),
            ),
            ("notes".to_owned(), Value::Str(notes.to_owned())),
            (
                "shipping".to_owned(),
                Value::Object(
                    [(
                        "location".to_owned(),
                        Value::Object(
                            [
                                ("lon".to_owned(), Value::Float(lon)),
                                ("lat".to_owned(), Value::Float(lat)),
                            ]
                            .into(),
                        ),
                    )]
                    .into(),
                ),
            ),
        ]
        .into(),
    )
}

/// `orders` plus a document collection `notes`.
fn with_notes(mut spec: DatabaseSpec) -> Result<DatabaseSpec, Error> {
    spec.collections.push(serde_json::from_value(json!({
        "name": "notes", "role": "document_entity", "domain": "primary", "indexes": ["primary"]
    }))?);
    Ok(spec)
}

fn security(spec: DatabaseSpec, grants: &JsonValue) -> Result<DatabaseSpec, Error> {
    with_security(
        spec,
        &json!({
            "roles": [{ "name": "order_writer", "grants": grants }],
            "principals": [{ "user": "orders_svc", "roles": ["order_writer"] }]
        }),
    )
}

fn weakening(path: &str) -> ProvisionOptions {
    ProvisionOptions {
        allow_weakening: BTreeSet::from([path.to_owned()]),
        ..ProvisionOptions::default()
    }
}

fn destructive(paths: &[&str]) -> ProvisionOptions {
    ProvisionOptions {
        allow_destructive: paths.iter().map(|path| (*path).to_owned()).collect(),
        ..ProvisionOptions::default()
    }
}

/// Turns the step failure injection off when dropped.
struct InjectedFailure;

impl InjectedFailure {
    fn at(index: usize) -> Self {
        fail_provision_step_for_tests(Some(index));
        Self
    }
}

impl Drop for InjectedFailure {
    fn drop(&mut self) {
        fail_provision_step_for_tests(None);
    }
}

/// Turns the new-database perturbation off when dropped.
struct Perturbation;

impl Perturbation {
    fn run(hook: fn(&mut Database)) -> Self {
        perturb_new_database_for_tests(Some(hook));
        Self
    }
}

impl Drop for Perturbation {
    fn drop(&mut self) {
        perturb_new_database_for_tests(None);
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn executor_builds_every_object_kind() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("orders.redb");
    let spec = full_spec()?;
    let rows = SqlOutput::Rows(SqlRows {
        columns: vec!["id".to_owned(), "revenue".to_owned()],
        rows: vec![
            vec![Value::Str("a".to_owned()), Value::Float(1.5)],
            vec![Value::Str("b".to_owned()), Value::Float(2.5)],
        ],
    });
    {
        let mut db = create_with_profile(&path, &spec)?;
        assert_eq!(db.layout(), TableLayout::Row);
        let plan = plan_open(&spec, &db)?;
        let report = apply_cli(&mut db, &spec, &ProvisionOptions::default())?;
        assert_applied(&report);
        assert_eq!(report.plan_id, plan.plan_id);
        assert!(!report.session_pending_restart);
        assert_eq!(
            report
                .applied_steps
                .iter()
                .map(|step| step.step_id.as_str())
                .collect::<Vec<_>>(),
            plan.steps
                .iter()
                .map(|step| step.step_id.as_str())
                .collect::<Vec<_>>()
        );

        for (name, kind) in [
            ("orders", ObjectKind::Document),
            ("memories", ObjectKind::Vector),
            ("ledger", ObjectKind::Table),
            ("rollups", ObjectKind::Table),
            ("metrics", ObjectKind::TimeSeries),
            ("social", ObjectKind::Graph),
            ("orders_notes_fts", ObjectKind::FullText),
            ("orders_shipping_location_geo", ObjectKind::Geo),
            ("ledger_history", ObjectKind::History),
        ] {
            assert_eq!(db.catalog().get(name).map(entry_kind), Some(kind), "{name}");
        }
        match db.catalog().get("memories") {
            Some(CatalogEntry::Vector { dim, hnsw, .. }) => {
                assert_eq!(*dim, 3);
                assert_eq!(
                    (hnsw.m, hnsw.ef_construction, hnsw.ef_search),
                    (16, 200, 48)
                );
            }
            other => return Err(format!("memories: {other:?}").into()),
        }
        match db.catalog().get("metrics") {
            Some(CatalogEntry::TimeSeries { config }) => {
                assert_eq!(config.chunk_millis, 3_600_000);
                assert_eq!(config.retention_millis, None);
                assert_eq!(
                    config.max_future_skew_millis,
                    crate::timeseries::DEFAULT_MAX_FUTURE_SKEW_MILLIS
                );
            }
            other => return Err(format!("metrics: {other:?}").into()),
        }
        match db.catalog().get("ledger_history") {
            Some(CatalogEntry::TemporalTable {
                base_table,
                retention,
                ..
            }) => {
                assert_eq!(base_table, "ledger");
                assert!(retention.keep_history);
            }
            other => return Err(format!("ledger_history: {other:?}").into()),
        }
        assert_eq!(db.table("ledger")?.layout(), TableLayout::Row);
        assert_eq!(db.table("rollups")?.layout(), TableLayout::Columnar);

        let record = db.provision_state()?.ok_or("a provision record")?;
        assert_eq!(record.status, ProvisionRecordStatus::Applied);
        assert_eq!(record.plan_id, plan.plan_id);
        assert_eq!(record.spec_digest, spec_digest(&spec));
        assert_eq!(record.applied_spec, spec);
        assert_eq!(record.objects.len(), 9);
        assert!(record.objects.values().all(|object| !object.adopted));
        for name in ["orders", "memories", "social"] {
            assert!(
                record
                    .objects
                    .get(name)
                    .is_some_and(|object| object.allocated_id.is_some()),
                "{name}"
            );
        }
        assert_eq!(
            record.managed_roles,
            BTreeSet::from(["order_writer".to_owned()])
        );
        assert_eq!(
            record.managed_principals,
            BTreeSet::from(["orders_svc".to_owned()])
        );
        assert_eq!(
            record.completed_steps,
            plan.steps
                .iter()
                .map(|step| step.step_id.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(record.applied_by, "local:cli");
        assert_eq!(db.provision_history()?.len(), 1);
        assert!(db.authz_policy().roles().contains_key("order_writer"));
        assert_eq!(
            db.principal_for_user("orders_svc").roles(),
            &BTreeSet::from(["order_writer".to_owned()])
        );

        // Document writes maintain the provisioned indexes, so documents inserted after
        // provisioning are found.
        let id =
            db.collection("orders")?
                .insert(&shipped_order("c-1", "fragile glass", 13.4, 52.5))?;
        db.collection("orders")?
            .insert(&shipped_order("c-2", "books", -70.0, -30.0))?;
        let hits = db
            .full_text_index("orders_notes_fts")?
            .search("fragile", 10)?;
        assert_eq!(hits.iter().map(|hit| hit.id).collect::<Vec<_>>(), vec![id]);
        let near = db
            .geo_index("orders_shipping_location_geo")?
            .within_radius(GeoPoint::new(13.4, 52.5)?, 1_000.0)?;
        assert_eq!(near.iter().map(|hit| hit.id).collect::<Vec<_>>(), vec![id]);

        // SQL on the columnar table inside the row-class database.
        assert_eq!(
            db.query("INSERT INTO rollups (id, revenue) VALUES ('a', 1.5), ('b', 2.5)")
                .await?,
            SqlOutput::AffectedRows(2)
        );
        assert_eq!(
            db.query("SELECT id, revenue FROM rollups ORDER BY id")
                .await?,
            rows
        );
    }

    let db = open_database_with_ops(
        DbConfig::on_disk(Profile::Balanced, &path),
        &OperationalConfig::new(),
    )?;
    assert_eq!(db.table("rollups")?.layout(), TableLayout::Columnar);
    assert_eq!(
        db.query("SELECT id, revenue FROM rollups ORDER BY id")
            .await?,
        rows
    );
    assert_eq!(plan_open(&spec, &db)?.outcome, PlanOutcome::Noop);
    Ok(())
}

#[test]
fn applying_again_is_a_noop_without_audit_events_and_drift_is_in_sync() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let spec = full_spec()?;
    let mut db = create_with_profile(&temp_dir.path().join("again.redb"), &spec)?;
    assert_applied(&apply_cli(&mut db, &spec, &ProvisionOptions::default())?);
    let events = db.audit_events()?.len();
    let record = db.provision_state()?;

    let plan = plan_open(&spec, &db)?;
    assert_eq!(plan.outcome, PlanOutcome::Noop, "{:#?}", plan.steps);
    assert!(
        plan.steps.is_empty(),
        "expected an empty value, got {:?}",
        plan.steps
    );
    assert!(plan.required_confirmation.is_none());
    for confirmation in [
        Confirmation::PlanId(plan.plan_id.clone()),
        Confirmation::AutoApproveAdditive,
    ] {
        let report = apply_provision(
            &mut db,
            &cli_operator(),
            &spec,
            &confirmation,
            &ProvisionOptions::default(),
            &env(ProvisionSurface::Cli),
        )?;
        assert_eq!(report.status, ProvisionOutcome::Noop);
        assert!(!report.check.data_mutated && !report.check.audit_recorded);
        assert!(
            report.applied_steps.is_empty(),
            "expected an empty value, got {:?}",
            report.applied_steps
        );
        assert_eq!(
            report.record.as_ref().map(|summary| summary.objects),
            Some(9)
        );
    }
    assert_eq!(db.audit_events()?.len(), events);
    assert_eq!(db.provision_state()?, record);
    assert!(drift(&db, &spec, &ProvisionOptions::default())?.in_sync);
    Ok(())
}

#[test]
fn a_local_operator_audits_intent_first_each_object_and_completion() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let spec = with_notes(full_spec()?)?;
    let mut db = create_with_profile(&temp_dir.path().join("audit.redb"), &spec)?;
    let before = db.audit_events()?.len();
    let report = apply_cli(&mut db, &spec, &ProvisionOptions::default())?;
    assert_applied(&report);

    let events = db.audit_events()?.split_off(before);
    let actions = actions(&events);
    assert_eq!(actions.first(), Some(&"provision_apply_started"));
    assert_eq!(actions.last(), Some(&"provision_apply_completed"));
    assert!(
        events
            .iter()
            .all(|event| event.principal.as_deref() == Some("local:cli"))
    );
    let started = events.first().ok_or("the intent event")?;
    let detail = started.detail.as_deref().unwrap_or_default();
    assert!(detail.contains(&report.plan_id), "{detail}");
    assert!(detail.contains("surface: cli") && detail.contains("flags: none"));
    let completed = events.last().ok_or("the completion event")?;
    assert!(
        completed
            .detail
            .as_deref()
            .is_some_and(|detail| detail.contains("data_mutated: true"))
    );
    // One provision_create_object per object, between the intent and the completion; no
    // principal-only create_* events.
    let created = events
        .iter()
        .filter(|event| event.action == "provision_create_object")
        .map(|event| event.resource.clone())
        .collect::<Vec<_>>();
    assert_eq!(created.len(), 10);
    assert!(created.contains(&Resource::Collection("notes".to_owned())));
    assert!(created.contains(&Resource::Table("ledger_history".to_owned())));
    assert!(!actions.iter().any(|action| action.starts_with("create_")));
    assert!(actions.contains(&"set_security_state"));
    db.verify_audit_chain()?;
    Ok(())
}

#[test]
fn a_principal_actor_needs_admin_and_gets_per_object_create_events() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("principal.redb");
    let spec = full_spec()?;
    let binding = resolved(&spec)?.bind(ReplicationKind::Cp, Some(path))?;
    let mut db = create_database_with_profile(
        &binding,
        OperationalConfig::new().with_security(SecurityConfig {
            authz_policy: AuthzPolicy::new([
                Role::new("ops")
                    .grant(Resource::System, Permission::Admin)
                    .grant(Resource::Database, Permission::Admin),
                Role::new("reader").grant(Resource::Database, Permission::Read),
            ]),
            principals: PrincipalRegistry::new()
                .with_principal("ops", Principal::new("ops").with_role("ops"))
                .with_principal("ada", Principal::new("ada").with_role("reader")),
            ..SecurityConfig::default()
        }),
    )?;
    let control = env(ProvisionSurface::ControlPlane);
    let reader = ProvisionActor::Principal(db.principal_for_user("ada"));
    match apply_as(
        &mut db,
        &reader,
        &spec,
        &ProvisionOptions::default(),
        &control,
    ) {
        Err(ProvisionError::PermissionMissing(message)) => {
            assert!(message.contains("System:Admin"), "{message}");
        }
        other => return Err(format!("expected PermissionMissing, got {other:?}").into()),
    }
    assert!(db.catalog().is_empty());
    assert_eq!(db.provision_state()?, None);

    let before = db.audit_events()?.len();
    let ops = ProvisionActor::Principal(db.principal_for_user("ops"));
    let report = apply_as(&mut db, &ops, &spec, &ProvisionOptions::default(), &control)?;
    assert_applied(&report);
    let events = db.audit_events()?.split_off(before);
    let actions = actions(&events);
    assert_eq!(actions.first(), Some(&"provision_apply_started"));
    assert_eq!(actions.last(), Some(&"provision_apply_completed"));
    for action in [
        "create_collection",
        "create_vector_collection",
        "create_table",
        "create_time_series",
        "create_graph",
        "create_full_text_index",
        "create_geo_index",
        "enable_system_versioning",
        "set_security_state",
    ] {
        assert!(actions.contains(&action), "{action}: {actions:?}");
    }
    assert!(!actions.contains(&"provision_create_object"));
    assert!(
        events
            .iter()
            .all(|event| event.principal.as_deref() == Some("ops"))
    );
    // Unpinned ids were allocated through the audited _auto_id_as creates.
    let record = db.provision_state()?.ok_or("a provision record")?;
    for name in ["orders", "memories", "social"] {
        assert!(
            record
                .objects
                .get(name)
                .is_some_and(|object| object.allocated_id.is_some())
        );
    }
    assert_eq!(record.applied_by, "ops");
    // The roles provisioning does not manage are left alone.
    assert!(db.authz_policy().roles().contains_key("ops"));
    assert!(db.authz_policy().roles().contains_key("reader"));
    Ok(())
}

#[test]
fn audit_is_enabled_first_disabled_last_and_required_in_between() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let spec = orders_spec()?;
    let mut db = create_with_profile(&temp_dir.path().join("audit-gate.redb"), &spec)?;
    assert_applied(&apply_cli(&mut db, &spec, &ProvisionOptions::default())?);

    // Disabling audit is weakening: it needs its flag and runs after the completion event.
    let mut off = spec.clone();
    off.guarantees.audit.enabled = false;
    assert!(matches!(
        apply_cli(&mut db, &off, &ProvisionOptions::default()),
        Err(ProvisionError::NeedsFlags(plan))
            if plan.required_flags == ["allow_weakening:guarantees.audit.enabled"]
    ));
    let before = db.audit_events()?.len();
    let allow = weakening("guarantees.audit.enabled");
    assert!(matches!(
        apply_provision(
            &mut db,
            &cli_operator(),
            &off,
            &Confirmation::AutoApproveAdditive,
            &allow,
            &env(ProvisionSurface::Cli),
        ),
        Err(ProvisionError::ConfirmationRequired(_))
    ));
    let report = apply_cli(&mut db, &off, &allow)?;
    assert_applied(&report);
    assert_eq!(
        report.applied_steps.last().map(|step| step.kind),
        Some(ProvisionStepKind::DisableAudit)
    );
    assert!(!db.audit_enabled());
    assert!(!db.persisted_security_state()?.audit_enabled);
    let events = db.audit_events()?.split_off(before);
    assert_eq!(
        actions(&events),
        vec![
            "provision_apply_started",
            "provision_apply_completed",
            "set_audit_enabled"
        ]
    );
    assert_eq!(
        events.last().and_then(|event| event.detail.as_deref()),
        Some("enabled: false")
    );
    assert_eq!(
        events.last().and_then(|event| event.principal.as_deref()),
        Some("local:cli")
    );
    assert_eq!(
        plan_open_with(&off, &db, &allow)?.outcome,
        PlanOutcome::Noop
    );

    // Audit off and kept off: no change can be applied, also with every flag.
    let more = with_notes(off)?;
    assert!(matches!(
        apply_cli(&mut db, &more, &allow),
        Err(ProvisionError::AuditRequired)
    ));
    assert!(!db.catalog().contains_key("notes"));

    // Audit off, spec on: the first step turns it on, before the intent event.
    let on = with_notes(spec)?;
    let events_before = db.audit_events()?.len();
    let report = apply_cli(&mut db, &on, &ProvisionOptions::default())?;
    assert_applied(&report);
    assert_eq!(
        report.applied_steps.first().map(|step| step.kind),
        Some(ProvisionStepKind::EnableAudit)
    );
    assert!(db.audit_enabled());
    let events = db.audit_events()?.split_off(events_before);
    assert_eq!(actions(&events).first().copied(), Some("set_audit_enabled"));
    assert_eq!(
        events.first().and_then(|event| event.detail.as_deref()),
        Some("enabled: true")
    );
    assert_eq!(
        actions(&events).get(1).copied(),
        Some("provision_apply_started")
    );
    assert!(db.catalog().contains_key("notes"));
    db.verify_audit_chain()?;
    Ok(())
}

/// A step without its id: what it does and to what.
type StepSignature = (ProvisionStepKind, Option<String>, String);

#[test]
#[allow(clippy::too_many_lines)]
fn a_failed_step_leaves_a_failed_record_and_applying_again_converges() -> TestResult {
    let spec = in_memory(full_spec()?);
    let rust = env(ProvisionSurface::RustApi);
    let embedded = ProvisionActor::local_operator("local:embedded");
    let defaults = ProvisionOptions::default();
    let step_count = {
        let db = create_in_memory(&spec)?;
        plan_rust_api(&spec, &db)?.steps.len()
    };
    assert!(step_count >= 12);
    let signature = |step: &ProvisionStep| -> StepSignature {
        (step.kind, step.object.clone(), step.path.clone())
    };

    for index in 0..step_count {
        let mut db = create_in_memory(&spec)?;
        let plan = plan_rust_api(&spec, &db)?;
        let failed_id = plan.steps[index].step_id.clone();
        let report = {
            let _failure = InjectedFailure::at(index);
            apply_provision(
                &mut db,
                &embedded,
                &spec,
                &Confirmation::PlanId(plan.plan_id.clone()),
                &defaults,
                &rust,
            )?
        };
        assert_eq!(report.status, ProvisionOutcome::PartiallyApplied, "{index}");
        assert!(report.check.data_mutated);
        assert!(
            report
                .check
                .message
                .ends_with("the steps before it stay applied; plan again to see what remains"),
            "{}",
            report.check.message
        );
        assert_eq!(report.failed_step.as_ref(), Some(&failed_id));
        let failed = report.applied_steps.last().ok_or("the failed step")?;
        assert_eq!(failed.outcome, StepOutcome::Failed);
        assert_eq!(failed.error_kind.as_deref(), Some("transaction_aborted"));
        assert!(
            report
                .warnings
                .iter()
                .all(|warning| warning.code != "PROVISION_FAILURE_NOT_RECORDED")
        );

        let record = db.provision_state()?.ok_or("a provision record")?;
        assert_eq!(record.status, ProvisionRecordStatus::Failed, "{index}");
        assert_eq!(record.failed_step.as_ref(), Some(&failed_id));
        assert_eq!(
            record.completed_steps,
            plan.steps[..index]
                .iter()
                .map(|step| step.step_id.clone())
                .collect::<Vec<_>>()
        );
        let events = db.audit_events()?;
        let last = events.last().ok_or("the failure event")?;
        assert_eq!(last.action, "provision_apply_failed");
        assert_eq!(last.outcome, AuditOutcome::Failed);

        // The next plan holds only what remains: nothing that completed comes back, and every
        // object the failed apply did not create is still planned. Recorded metadata counts
        // only once an apply finishes, so it is planned against the last finished one again.
        let remaining = plan_rust_api(&spec, &db)?;
        assert!(has_issue(
            &remaining,
            "DRIFT_INTERRUPTED_APPLY",
            ValidationSeverity::Warning
        ));
        let left = plan.steps[index..]
            .iter()
            .map(signature)
            .collect::<Vec<_>>();
        for step in &remaining.steps {
            assert!(
                step.kind == ProvisionStepKind::RecordMetadata || left.contains(&signature(step)),
                "{index}: {step:?}"
            );
        }
        for step in &plan.steps[index..] {
            if step.object.is_some() && step.kind != ProvisionStepKind::ApplySecurity {
                assert!(
                    remaining
                        .steps
                        .iter()
                        .any(|planned| signature(planned) == signature(step)),
                    "{index}: {step:?} is missing"
                );
            }
        }

        let again = apply_provision(
            &mut db,
            &embedded,
            &spec,
            &Confirmation::PlanId(remaining.plan_id.clone()),
            &defaults,
            &rust,
        )?;
        assert_applied(&again);
        assert_eq!(
            plan_rust_api(&spec, &db)?.outcome,
            PlanOutcome::Noop,
            "{index}"
        );
        let record = db.provision_state()?.ok_or("a provision record")?;
        assert_eq!(record.status, ProvisionRecordStatus::Applied);
        assert_eq!(record.objects.len(), 9);
        assert_eq!(db.provision_history()?.len(), 2);
    }
    Ok(())
}

#[test]
fn an_unfinished_apply_does_not_carry_its_weakened_guarantees() -> TestResult {
    let mut strict = in_memory(orders_spec()?);
    strict.guarantees.strict_cross_domain_transactions = true;
    let embedded = ProvisionActor::local_operator("local:embedded");
    let rust = env(ProvisionSurface::RustApi);
    let mut db = create_in_memory(&strict)?;
    assert_applied(&apply_as(
        &mut db,
        &embedded,
        &strict,
        &ProvisionOptions::default(),
        &rust,
    )?);

    let mut relaxed = with_notes(strict.clone())?;
    relaxed.guarantees.strict_cross_domain_transactions = false;
    let flag = "guarantees.strict_cross_domain_transactions";
    let allow = weakening(flag);
    let plan = plan_provision(&relaxed, ProvisionTargetRef::Open(&db), &allow, &rust)?;
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
    let create = plan
        .steps
        .iter()
        .position(|step| step.object.as_deref() == Some("notes"))
        .ok_or("the notes step")?;
    let report = {
        let _failure = InjectedFailure::at(create);
        apply_provision(
            &mut db,
            &embedded,
            &relaxed,
            &Confirmation::PlanId(plan.plan_id.clone()),
            &allow,
            &rust,
        )?
    };
    assert_eq!(report.status, ProvisionOutcome::PartiallyApplied);
    let record = db.provision_state()?.ok_or("a provision record")?;
    assert_eq!(record.status, ProvisionRecordStatus::Failed);
    assert_eq!(record.applied_spec, relaxed);
    assert_eq!(record.last_applied_spec.as_ref(), Some(&strict));
    assert_eq!(record.last_applied(), Some(&strict));

    // The weakening the failed apply confirmed still needs its flag on the next run.
    let resumed = plan_rust_api(&relaxed, &db)?;
    assert_eq!(
        resumed.outcome,
        PlanOutcome::NeedsFlags,
        "{:#?}",
        resumed.steps
    );
    assert!(
        resumed
            .required_flags
            .contains(&format!("allow_weakening:{flag}"))
    );
    assert!(
        resumed
            .steps
            .iter()
            .any(|step| step.kind == ProvisionStepKind::RecordGuarantees && step.path == flag)
    );
    let spec_migration = resumed.spec_migration.ok_or("a spec migration")?;
    assert!(
        !spec_migration.steps.is_empty(),
        "expected a non-empty value"
    );

    assert_applied(&apply_as(&mut db, &embedded, &relaxed, &allow, &rust)?);
    let record = db.provision_state()?.ok_or("a provision record")?;
    assert_eq!(record.status, ProvisionRecordStatus::Applied);
    assert_eq!(record.last_applied_spec, None);
    assert_eq!(record.last_applied(), Some(&relaxed));
    assert_eq!(plan_rust_api(&relaxed, &db)?.outcome, PlanOutcome::Noop);
    Ok(())
}

#[test]
fn confirmations_must_name_the_current_plan() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let spec = orders_spec()?;
    let mut db = create_with_profile(&temp_dir.path().join("confirm.redb"), &spec)?;
    let cli = env(ProvisionSurface::Cli);
    let defaults = ProvisionOptions::default();
    let plan = plan_open(&spec, &db)?;

    // A config plan id or anything else is rejected without a change.
    let events = db.audit_events()?.len();
    for confirmation in [
        "plan_0123456789abcdef0123456789abcdef",
        "yes",
        "prov_short",
        "",
    ] {
        let report = apply_provision(
            &mut db,
            &cli_operator(),
            &spec,
            &Confirmation::PlanId(confirmation.to_owned()),
            &defaults,
            &cli,
        )?;
        assert_eq!(report.status, ProvisionOutcome::Rejected, "{confirmation}");
        assert_eq!(report.check.status, ApplyStatus::Rejected);
        assert!(!report.check.data_mutated && !report.check.confirmation_matched);
        assert!(report.check.message.contains(&plan.plan_id));
    }
    assert!(db.catalog().is_empty());
    assert_eq!(db.provision_state()?, None);
    assert_eq!(db.audit_events()?.len(), events);

    // Another prov_ id is a plan change.
    let other = format!("prov_{}", "0".repeat(32));
    match apply_provision(
        &mut db,
        &cli_operator(),
        &spec,
        &Confirmation::PlanId(other.clone()),
        &defaults,
        &cli,
    ) {
        Err(ProvisionError::PlanChanged { confirmed, current }) => {
            assert_eq!(confirmed, other);
            assert_eq!(current, plan.plan_id);
        }
        result => return Err(format!("expected PlanChanged, got {result:?}").into()),
    }

    // An id computed on the CLI does not confirm on the Control Plane.
    assert!(matches!(
        apply_provision(
            &mut db,
            &ProvisionActor::Principal(Principal::new("root")),
            &spec,
            &Confirmation::PlanId(plan.plan_id.clone()),
            &defaults,
            &env(ProvisionSurface::ControlPlane),
        ),
        Err(ProvisionError::PlanChanged { .. })
    ));

    // An out-of-band change between plan and apply changes the plan.
    db.create_collection_auto_id("notes", orders_fields(), Vec::new())?;
    assert!(matches!(
        apply_provision(
            &mut db,
            &cli_operator(),
            &spec,
            &Confirmation::PlanId(plan.plan_id.clone()),
            &defaults,
            &cli,
        ),
        Err(ProvisionError::PlanChanged { .. })
    ));
    assert!(!db.catalog().contains_key("orders"));
    assert_eq!(db.audit_events()?.len(), events);

    // Additive changes auto-approve.
    let report = apply_provision(
        &mut db,
        &cli_operator(),
        &spec,
        &Confirmation::AutoApproveAdditive,
        &defaults,
        &cli,
    )?;
    assert_applied(&report);
    Ok(())
}

#[test]
fn auto_approval_refuses_retunes_weakening_and_rbac_on_an_existing_database() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let secure = spec("secure_app", &json!([]))?;
    let mut db = create_with_profile(&temp_dir.path().join("auto.redb"), &secure)?;
    let cli = env(ProvisionSurface::Cli);
    let auto = |db: &mut Database, spec: &DatabaseSpec, opts: &ProvisionOptions| {
        apply_provision(
            db,
            &cli_operator(),
            spec,
            &Confirmation::AutoApproveAdditive,
            opts,
            &cli,
        )
    };
    assert_applied(&auto(&mut db, &secure, &ProvisionOptions::default())?);

    let retune = with_custom_profile(
        secure.clone(),
        "orders_custom",
        &json!({ "extends": "secure_app", "knobs": { "cache": { "max_entries": 4096 } } }),
    )?;
    let mut weaker = secure.clone();
    weaker.guarantees.audit.enabled = false;
    let rbac = with_security(
        secure.clone(),
        &json!({
            "roles": [{ "name": "auditor", "grants": [{ "resource": "database", "permissions": ["read"] }] }],
            "principals": []
        }),
    )?;
    for (name, spec, opts) in [
        ("retune", &retune, ProvisionOptions::default()),
        ("weakening", &weaker, weakening("guarantees.audit.enabled")),
        ("rbac", &rbac, ProvisionOptions::default()),
    ] {
        assert!(
            matches!(
                auto(&mut db, spec, &opts),
                Err(ProvisionError::ConfirmationRequired(plan)) if plan.outcome == PlanOutcome::Ready
            ),
            "{name}"
        );
    }
    assert_eq!(db.authz_policy().roles().len(), 0);
    assert!(db.audit_enabled());
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn new_databases_are_created_only_at_absent_targets() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let spec = full_spec()?;
    let cli = env(ProvisionSurface::Cli);
    let defaults = ProvisionOptions::default();

    // A spec with roles is auto-approved for a new database: the continuation check compares
    // steps, not auto_approvable.
    let path = temp_dir.path().join("new.redb");
    let plan = plan_path(&spec, &path, ProvisionSurface::Cli)?;
    assert!(plan.auto_approvable);
    let (db, report) = provision_new_database(
        &spec,
        Some(&path),
        &Confirmation::AutoApproveAdditive,
        &defaults,
        &cli,
    )?;
    assert_applied(&report);
    assert_eq!(report.plan_id, plan.plan_id);
    assert_eq!(
        report.applied_steps.first().map(|step| step.kind),
        Some(ProvisionStepKind::CreateDatabase)
    );
    assert_eq!(report.applied_steps.len(), plan.steps.len());
    let record = db.provision_state()?.ok_or("a provision record")?;
    assert_eq!(record.plan_id, plan.plan_id);
    assert_eq!(record.applied_by, "local:cli");
    assert_eq!(
        record.completed_steps,
        plan.steps
            .iter()
            .map(|step| step.step_id.clone())
            .collect::<Vec<_>>()
    );
    let events = db.audit_events()?;
    assert_eq!(
        actions(&events).first().copied(),
        Some("provision_apply_started")
    );
    assert!(
        events
            .first()
            .and_then(|event| event.detail.as_deref())
            .is_some_and(|detail| detail.contains(&plan.plan_id))
    );
    assert!(db.authz_policy().roles().contains_key("order_writer"));
    assert!(db.probed_metadata()?.manifest.is_some());
    drop(db);
    let db = open_database_with_ops(
        DbConfig::on_disk(Profile::Balanced, &path),
        &OperationalConfig::new(),
    )?;
    assert_eq!(plan_open(&spec, &db)?.outcome, PlanOutcome::Noop);
    drop(db);

    // An existing database is never created over.
    assert!(matches!(
        provision_new_database(
            &spec,
            Some(&path),
            &Confirmation::AutoApproveAdditive,
            &defaults,
            &cli,
        ),
        Err(ProvisionError::AlreadyInitialized)
    ));
    // The Control Plane never creates databases.
    assert!(matches!(
        provision_new_database(
            &spec,
            Some(&temp_dir.path().join("control.redb")),
            &Confirmation::AutoApproveAdditive,
            &defaults,
            &env(ProvisionSurface::ControlPlane),
        ),
        Err(ProvisionError::Config(ConfigError::Unsupported(_)))
    ));
    // A confirmation that is not a provisioning plan id creates nothing.
    let refused = temp_dir.path().join("refused.redb");
    assert!(matches!(
        provision_new_database(
            &spec,
            Some(&refused),
            &Confirmation::PlanId("plan_0123".to_owned()),
            &defaults,
            &cli,
        ),
        Err(ProvisionError::ConfirmationRequired(_))
    ));
    assert!(!refused.exists());

    // A database that differs from the confirmed plan once created aborts the apply.
    let perturbed = temp_dir.path().join("perturbed.redb");
    let plan = plan_path(&spec, &perturbed, ProvisionSurface::Cli)?;
    let result = {
        let _perturbation = Perturbation::run(|db: &mut Database| {
            let created = db.create_collection_auto_id("orders", orders_fields(), orders_indexes());
            assert!(created.is_ok());
        });
        provision_new_database(
            &spec,
            Some(&perturbed),
            &Confirmation::PlanId(plan.plan_id.clone()),
            &defaults,
            &cli,
        )
    };
    match result {
        Err(ProvisionError::PlanChanged { confirmed, current }) => {
            assert_eq!(confirmed, plan.plan_id);
            assert_ne!(current, plan.plan_id);
        }
        Err(other) => return Err(format!("expected PlanChanged, got {other:?}").into()),
        Ok((_, report)) => return Err(format!("expected PlanChanged, got {report:?}").into()),
    }
    // The created database is kept, empty but for the perturbation, and has no record.
    let db = open_database_with_ops(
        DbConfig::on_disk(Profile::Balanced, &perturbed),
        &OperationalConfig::new(),
    )?;
    assert_eq!(db.provision_state()?, None);
    assert_eq!(db.catalog().len(), 1);
    Ok(())
}

#[test]
fn rust_api_provisions_an_encrypted_database_with_a_local_envelope() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("encrypted.redb");
    let kek_path = temp_dir.path().join("kek.bin");
    std::fs::write(&kek_path, [7_u8; 32])?;
    let encryption =
        EncryptionConfig::local_envelope(temp_dir.path().join("keyring.json"), &kek_path);
    let mut spec = orders_spec()?;
    spec.guarantees.encryption.at_rest = true;
    let environment = ProvisionEnvironment {
        encryption: Some(encryption.clone()),
        ..env(ProvisionSurface::RustApi)
    };
    let plan = plan_provision(
        &spec,
        ProvisionTargetRef::Path(&path),
        &ProvisionOptions::default(),
        &environment,
    )?;
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
    let (db, report) = provision_new_database(
        &spec,
        Some(&path),
        &Confirmation::PlanId(plan.plan_id.clone()),
        &ProvisionOptions::default(),
        &environment,
    )?;
    assert_applied(&report);
    let record = db.provision_state()?.ok_or("a provision record")?;
    assert!(record.guarantees.iter().any(|entry| {
        entry.path == "guarantees.encryption.at_rest" && entry.status == EnforcementStatus::Enforced
    }));
    assert!(db.catalog().contains_key("orders"));
    drop(db);

    let config = DbConfig::on_disk(Profile::Balanced, &path);
    assert!(matches!(
        open_database(config.clone()),
        Err(ConfigError::MetadataUnreadable { .. })
    ));
    let db = open_database_with_ops(
        config,
        &OperationalConfig::new().with_encryption(encryption.clone()),
    )?;
    assert!(db.catalog().contains_key("orders"));

    // Reopened with its key, the database is in sync and at rest stays enforced.
    let reopened = plan_provision(
        &spec,
        ProvisionTargetRef::Open(&db),
        &ProvisionOptions::default(),
        &environment,
    )?;
    assert_eq!(reopened.outcome, PlanOutcome::Noop, "{:#?}", reopened.steps);
    assert!(reopened.guarantees.iter().any(|entry| {
        entry.path == "guarantees.encryption.at_rest" && entry.status == EnforcementStatus::Enforced
    }));

    // A spec that says at_rest: false cannot decrypt it.
    let mut plaintext = spec.clone();
    plaintext.guarantees.encryption.at_rest = false;
    let mismatch = plan_provision(
        &plaintext,
        ProvisionTargetRef::Open(&db),
        &ProvisionOptions::default(),
        &environment,
    )?;
    assert_blocked(&mismatch, "PROVISION_ENCRYPTION_MISMATCH");
    assert!(
        mismatch
            .drift
            .items
            .iter()
            .any(|item| item.kind == DriftKind::EncryptionMismatch && item.blocking)
    );

    // An encryption config other than the one the database was opened with is refused.
    let other = ProvisionEnvironment {
        encryption: Some(EncryptionConfig::local_envelope(
            temp_dir.path().join("other-keyring.json"),
            &kek_path,
        )),
        ..env(ProvisionSurface::RustApi)
    };
    assert!(matches!(
        plan_provision(
            &spec,
            ProvisionTargetRef::Open(&db),
            &ProvisionOptions::default(),
            &other,
        ),
        Err(ProvisionError::Config(ConfigError::Unsupported(_)))
    ));
    drop(db);

    // open_from_spec opens it with the key and starts it unchanged.
    let (db, report) = open_from_spec(
        &spec,
        Some(&path),
        StartupMode::Verify,
        &ProvisionOptions::default(),
        &environment,
    )?;
    assert_eq!(report.decision, StartupDecision::Start);
    assert!(report.applied.is_none());
    assert!(db.catalog().contains_key("orders"));
    Ok(())
}

/// A new encrypted database gets its keyring when it is created, but opening an existing one
/// never mints a keyring: with the keyring gone, every start mode fails closed and leaves no
/// keyring behind, and the database opens again once the keyring is back.
#[test]
fn an_existing_encrypted_database_never_gets_a_new_keyring() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("encrypted.redb");
    let kek_path = temp_dir.path().join("kek.bin");
    std::fs::write(&kek_path, [9_u8; 32])?;
    let keyring = temp_dir.path().join("keyring.json");
    let mut spec = orders_spec()?;
    spec.guarantees.encryption.at_rest = true;
    let environment = ProvisionEnvironment {
        encryption: Some(EncryptionConfig::local_envelope(&keyring, &kek_path)),
        ..env(ProvisionSurface::RustApi)
    };
    let (db, report) = provision_new_database(
        &spec,
        Some(&path),
        &Confirmation::AutoApproveAdditive,
        &ProvisionOptions::default(),
        &environment,
    )?;
    assert_applied(&report);
    drop(db);
    assert!(
        keyring.is_file(),
        "creating the database creates its keyring"
    );

    let moved = temp_dir.path().join("keyring.moved");
    std::fs::rename(&keyring, &moved)?;
    for mode in [
        StartupMode::Verify,
        StartupMode::Create,
        StartupMode::Additive,
    ] {
        let opened = open_from_spec(
            &spec,
            Some(&path),
            mode,
            &ProvisionOptions::default(),
            &environment,
        );
        let Err(error) = opened else {
            return Err(format!("{mode:?} opened a database without its keyring").into());
        };
        assert!(error.to_string().contains("keyring"), "{mode:?}: {error}");
        assert!(!keyring.exists(), "{mode:?} minted a keyring");
    }
    assert!(matches!(
        provision_new_database(
            &spec,
            Some(&path),
            &Confirmation::AutoApproveAdditive,
            &ProvisionOptions::default(),
            &environment,
        ),
        Err(ProvisionError::Io(_))
    ));
    assert!(!keyring.exists(), "provision_new_database minted a keyring");

    std::fs::rename(&moved, &keyring)?;
    let (db, report) = open_from_spec(
        &spec,
        Some(&path),
        StartupMode::Verify,
        &ProvisionOptions::default(),
        &environment,
    )?;
    assert_eq!(report.decision, StartupDecision::Start);
    assert!(db.catalog().contains_key("orders"));
    Ok(())
}

#[test]
fn an_unencrypted_database_cannot_be_planned_as_encrypted_at_rest() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("plain.redb");
    let spec = orders_spec()?;
    let rust_api = env(ProvisionSurface::RustApi);
    let (db, report) = provision_new_database(
        &spec,
        Some(&path),
        &Confirmation::AutoApproveAdditive,
        &ProvisionOptions::default(),
        &rust_api,
    )?;
    assert_applied(&report);
    let mut encrypted = spec.clone();
    encrypted.guarantees.encryption.at_rest = true;
    let plan = plan_rust_api(&encrypted, &db)?;
    assert_blocked(&plan, "PROVISION_ENCRYPTION_MISMATCH");
    assert!(plan.guarantees.iter().any(|entry| {
        entry.path == "guarantees.encryption.at_rest"
            && entry.status == EnforcementStatus::NotEnforceable
    }));
    assert!(
        plan.drift
            .items
            .iter()
            .any(|item| item.kind == DriftKind::EncryptionMismatch && item.blocking)
    );
    let report = drift(&db, &encrypted, &ProvisionOptions::default())?;
    assert!(!report.in_sync);
    Ok(())
}

#[test]
fn an_encryption_config_with_at_rest_false_is_refused_before_creating_anything() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("encrypted.redb");
    let kek_path = temp_dir.path().join("kek.bin");
    std::fs::write(&kek_path, [7_u8; 32])?;
    let spec = orders_spec()?;
    assert!(!spec.guarantees.encryption.at_rest);
    let environment = ProvisionEnvironment {
        encryption: Some(EncryptionConfig::local_envelope(
            temp_dir.path().join("keyring.json"),
            &kek_path,
        )),
        ..env(ProvisionSurface::RustApi)
    };
    let plan = plan_provision(
        &spec,
        ProvisionTargetRef::Path(&path),
        &ProvisionOptions::default(),
        &environment,
    )?;
    assert_eq!(plan.outcome, PlanOutcome::Blocked);
    assert!(has_issue(
        &plan,
        "PROVISION_ENCRYPTION_MISMATCH",
        ValidationSeverity::Error
    ));
    assert!(!plan.auto_approvable);
    match provision_new_database(
        &spec,
        Some(&path),
        &Confirmation::PlanId(plan.plan_id.clone()),
        &ProvisionOptions::default(),
        &environment,
    ) {
        Err(ProvisionError::Blocked(_)) => {}
        Err(other) => return Err(format!("expected a blocked plan, got {other:?}").into()),
        Ok(_) => return Err("expected a blocked plan, got a new database".into()),
    }
    assert!(!path.exists());
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn rbac_changes_only_managed_names_in_one_batch() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("rbac.redb");
    let base = orders_spec()?;
    let mut db = create_with_profile(&path, &base)?;
    db.ensure_bootstrap_admin_principal()?;
    let admin = db.principal_for_user("admin");
    let policy = db
        .authz_policy()
        .clone()
        .allow(Role::new("auditor").grant(Resource::Database, Permission::Read));
    db.set_authz_policy_as(&admin, policy)?;

    let writer = security(
        base.clone(),
        &json!([{ "object": "orders", "permissions": ["read", "write"] }]),
    )?;
    let before = latest_lsn(&db)?;
    assert_applied(&apply_cli(&mut db, &writer, &ProvisionOptions::default())?);
    let commits = table_commits(&db, before, "__authz_policy")?;
    assert_eq!(commits.len(), 1, "{commits:?}");
    assert!(
        commits[0].contains(b"policy".as_slice()) && commits[0].contains(b"principals".as_slice())
    );
    let roles = db.authz_policy().roles();
    assert!(roles.contains_key("admin") && roles.contains_key("auditor"));
    assert_eq!(
        roles.get("order_writer").map(|role| role.grants().len()),
        Some(1)
    );
    assert_eq!(db.principal_for_user("admin"), admin);
    assert_eq!(
        db.principal_for_user("orders_svc"),
        Principal::new("orders_svc").with_role("order_writer")
    );
    let record = db.provision_state()?.ok_or("a provision record")?;
    assert_eq!(
        record.managed_roles,
        BTreeSet::from(["order_writer".to_owned()])
    );

    // A revoked permission is weakening.
    let reader = security(
        base.clone(),
        &json!([{ "object": "orders", "permissions": ["read"] }]),
    )?;
    assert!(matches!(
        apply_cli(&mut db, &reader, &ProvisionOptions::default()),
        Err(ProvisionError::NeedsFlags(plan))
            if plan.required_flags == ["allow_weakening:security.roles.order_writer"]
    ));
    assert_applied(&apply_cli(
        &mut db,
        &reader,
        &weakening("security.roles.order_writer"),
    )?);
    let order_writer = db
        .authz_policy()
        .roles()
        .get("order_writer")
        .cloned()
        .ok_or("the managed role")?;
    assert!(order_writer.allows(&Resource::Collection("orders".to_owned()), Permission::Read));
    assert!(!order_writer.allows(
        &Resource::Collection("orders".to_owned()),
        Permission::Write
    ));

    // Dropping the managed role and principal is destructive.
    let flags = [
        "security.roles.order_writer",
        "security.principals.orders_svc",
    ];
    match apply_cli(&mut db, &base, &ProvisionOptions::default()) {
        Err(ProvisionError::NeedsFlags(plan)) => assert_eq!(
            plan.required_flags,
            flags
                .iter()
                .map(|path| format!("allow_destructive:{path}"))
                .collect::<Vec<_>>()
        ),
        other => return Err(format!("expected NeedsFlags, got {other:?}").into()),
    }
    assert_applied(&apply_cli(&mut db, &base, &destructive(&flags))?);
    assert!(!db.authz_policy().roles().contains_key("order_writer"));
    assert!(
        !db.principal_registry()
            .principals()
            .contains_key("orders_svc")
    );
    let record = db.provision_state()?.ok_or("a provision record")?;
    assert!(record.managed_roles.is_empty() && record.managed_principals.is_empty());
    drop(db);

    // The bootstrap administrator and the unmanaged role survive a reopen.
    let db = open_database_with_ops(
        DbConfig::on_disk(Profile::Balanced, &path),
        &OperationalConfig::new(),
    )?;
    assert!(db.authz_policy().roles().contains_key("admin"));
    assert!(db.authz_policy().roles().contains_key("auditor"));
    assert_eq!(db.principal_for_user("admin"), admin);
    assert_eq!(plan_open(&base, &db)?.outcome, PlanOutcome::Noop);
    Ok(())
}

/// `physical.max_future_skew_millis` is created, enforced, compared and exported end to end. A
/// series persisted before the bound existed accepts any future timestamp (the runtime reads it
/// back as `i64::MAX`): it conflicts with a spec that leaves the bound at its default, and its
/// export writes the bound as `null` (no bound), never as `i64::MAX`, which JavaScript clients
/// cannot represent. The exported spec round-trips through JSON and adopts the series, and no
/// apply report, provision record, drift report or later export of it carries `i64::MAX`.
#[test]
#[allow(clippy::too_many_lines)]
fn time_series_future_skew_is_provisioned_compared_and_exported() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let series = |physical: JsonValue| {
        spec(
            "balanced",
            &json!([{
                "name": "metrics", "role": "time_series", "domain": "primary",
                "indexes": ["time_series"], "physical": physical
            }]),
        )
    };
    let skew_of = |db: &Database| match db.catalog().get("metrics") {
        Some(CatalogEntry::TimeSeries { config }) => Some(config.max_future_skew_millis),
        _ => None,
    };
    let strict = series(json!({ "max_future_skew_millis": 0 }))?;
    let mut db = create_with_profile(&temp_dir.path().join("skew.redb"), &strict)?;
    assert_applied(&apply_cli(&mut db, &strict, &ProvisionOptions::default())?);
    assert_eq!(skew_of(&db), Some(0));
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let metrics = db.time_series("metrics")?;
    metrics.insert_point(
        "cpu",
        TimePoint {
            timestamp_millis: now - 1_000,
            value: 1.0,
        },
    )?;
    assert!(
        metrics
            .insert_point(
                "cpu",
                TimePoint {
                    timestamp_millis: now + 60_000,
                    value: 2.0,
                },
            )
            .is_err()
    );
    assert_eq!(plan_open(&strict, &db)?.outcome, PlanOutcome::Noop);

    // Any other bound, the default one included, is a conflict: there is no alter API.
    for other in [
        series(json!({}))?,
        series(json!({ "max_future_skew_millis": 5_000 }))?,
    ] {
        let plan = plan_open(&other, &db)?;
        assert_blocked(&plan, "PROVISION_OBJECT_CONFLICT");
        let object = plan
            .objects
            .iter()
            .find(|object| object.name == "metrics")
            .ok_or("metrics")?;
        assert_eq!(
            object
                .diff
                .iter()
                .map(|diff| diff.field.as_str())
                .collect::<Vec<_>>(),
            ["max_future_skew_millis"]
        );
    }
    drop(db);

    let defaulted = series(json!({}))?;
    let legacy_path = temp_dir.path().join("legacy.redb");
    let mut legacy = create_with_profile(&legacy_path, &defaulted)?;
    legacy.create_time_series(TimeSeriesConfig::new("metrics").with_chunk_millis(3_600_000))?;
    // The catalog entry as a build before the bound existed wrote it, without
    // max_future_skew_millis: reopened, the runtime reads the series back as i64::MAX.
    propose_system(
        &legacy,
        Op::Put {
            table: "__catalog__".to_owned(),
            key: b"metrics".to_vec(),
            value: br#"{"TimeSeries":{"config":{"name":"metrics","chunk_millis":3600000,"retention_millis":null}}}"#
                .to_vec(),
        },
    )?;
    drop(legacy);
    let mut legacy = open_database(DbConfig::on_disk(Profile::Balanced, &legacy_path))?;
    assert_eq!(skew_of(&legacy), Some(i64::MAX));
    let conflict = plan_open(&defaulted, &legacy)?;
    assert_blocked(&conflict, "PROVISION_OBJECT_CONFLICT");
    let diff = conflict
        .objects
        .iter()
        .find(|object| object.name == "metrics")
        .and_then(|object| object.diff.first())
        .ok_or("a metrics diff")?;
    assert_eq!(
        (&diff.expected, &diff.actual),
        (&json!(300_000), &JsonValue::Null)
    );
    let exported = export_spec(&legacy)?;
    assert_eq!(
        exported.spec.collections[0]
            .physical
            .as_ref()
            .and_then(|physical| physical.max_future_skew_millis),
        Some(None)
    );
    assert!(
        exported.notes.iter().any(|note| {
            note.contains("accepts any future timestamp")
                && note.contains("max_future_skew_millis: null")
        }),
        "{:#?}",
        exported.notes
    );
    let unbounded = i64::MAX.to_string();
    let json = serde_json::to_string(&exported.spec)?;
    assert!(json.contains("\"max_future_skew_millis\":null"), "{json}");
    let yaml = serde_norway::to_string(&exported.spec)?;
    for text in [&json, &yaml] {
        assert!(!text.contains(&unbounded), "{text}");
    }
    let plan_json = serde_json::to_string(&conflict)?;
    assert!(!plan_json.contains(&unbounded), "{plan_json}");

    // The exported spec, read back as a client would send it, adopts the series, and so does
    // a spec that spells the bound out as i64::MAX.
    let round_tripped = decode_spec_bytes(json.as_bytes(), SpecFormat::Json)?;
    assert_eq!(round_tripped, exported.spec);
    let spelled_out = series(json!({ "max_future_skew_millis": i64::MAX }))?;
    for spec in [&round_tripped, &spelled_out] {
        let plan = plan_open(spec, &legacy)?;
        assert_eq!(plan.outcome, PlanOutcome::Ready, "{:#?}", plan.issues);
        assert!(has_step(
            &plan,
            ProvisionStepKind::AdoptObject,
            StepSafety::Additive
        ));
    }
    // Neither the apply report, nor the stored record and its applied spec, nor drift, nor a
    // second export carry i64::MAX.
    let unbounded_nowhere = |db: &Database, report: &ProvisionApplyReport, spec: &DatabaseSpec| {
        let record = db.provision_state()?.ok_or("a provision record")?;
        let stored = db
            .read(PROVISION_TABLE, RECORD_KEY, ReadConsistency::Strong)?
            .ok_or("the stored record")?;
        assert_eq!(
            serde_json::to_value(&record)?["applied_spec"]["collections"][0]["physical"]["max_future_skew_millis"],
            JsonValue::Null
        );
        for text in [
            serde_json::to_string(report)?,
            serde_json::to_string(&record)?,
            String::from_utf8(stored)?,
            serde_json::to_string(&drift(db, spec, &ProvisionOptions::default())?)?,
            serde_json::to_string(&export_spec(db)?.spec)?,
        ] {
            assert!(!text.contains(&unbounded), "{text}");
        }
        Ok::<_, Error>(())
    };
    let report = apply_cli(&mut legacy, &round_tripped, &ProvisionOptions::default())?;
    assert_applied(&report);
    assert_eq!(skew_of(&legacy), Some(i64::MAX));
    assert_eq!(
        plan_open(&round_tripped, &legacy)?.outcome,
        PlanOutcome::Noop
    );
    unbounded_nowhere(&legacy, &report, &round_tripped)?;

    // A new series built from the exported spec, or from one that spells the bound out as
    // i64::MAX, has no bound either, and its record writes null.
    for (file, spec) in [
        ("rebuilt.redb", &round_tripped),
        ("spelled.redb", &spelled_out),
    ] {
        let mut rebuilt = create_with_profile(&temp_dir.path().join(file), spec)?;
        let report = apply_cli(&mut rebuilt, spec, &ProvisionOptions::default())?;
        assert_applied(&report);
        assert_eq!(skew_of(&rebuilt), Some(i64::MAX));
        assert_eq!(plan_open(spec, &rebuilt)?.outcome, PlanOutcome::Noop);
        unbounded_nowhere(&rebuilt, &report, spec)?;
    }
    Ok(())
}

/// Plain openers reload the persisted RBAC state and the admin bootstrap merges into it, so
/// the roles and principals provisioning applied survive what a `multidb serve` without spec
/// mode does at every start.
#[test]
fn provisioned_rbac_survives_a_plain_reopen_and_the_admin_bootstrap() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("plain.redb");
    let spec = with_security(
        orders_spec()?,
        &json!({
            "roles": [{
                "name": "order_reader",
                "grants": [{ "object": "orders", "permissions": ["read"] }]
            }],
            "principals": [{ "user": "orders_svc", "roles": ["order_reader"] }]
        }),
    )?;
    let mut db = create_with_profile(&path, &spec)?;
    assert_applied(&apply_cli(&mut db, &spec, &ProvisionOptions::default())?);
    drop(db);

    let mut db = open_database(DbConfig::on_disk(Profile::Balanced, &path))?;
    db.ensure_bootstrap_admin_principal()?;
    assert!(db.authz_policy().roles().contains_key("order_reader"));
    assert!(db.authz_policy().roles().contains_key("admin"));
    assert_eq!(
        db.principal_for_user("orders_svc"),
        Principal::new("orders_svc").with_role("order_reader")
    );
    let plan = plan_open(&spec, &db)?;
    assert_eq!(plan.outcome, PlanOutcome::Noop, "{:#?}", plan.steps);
    assert!(plan.drift.in_sync, "{:#?}", plan.drift);
    Ok(())
}

#[test]
fn a_principal_cannot_apply_a_plan_that_locks_it_out() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let base = orders_spec()?;
    let mut db = create_with_profile(&temp_dir.path().join("lockout.redb"), &base)?;
    let ops = |grants: JsonValue| {
        with_security(
            base.clone(),
            &json!({
                "roles": [{ "name": "ops", "grants": grants }],
                "principals": [{ "user": "ops", "roles": ["ops"] }]
            }),
        )
    };
    let administrators = ops(json!([{ "resource": "database", "permissions": ["admin"] }]))?;
    assert_applied(&apply_cli(
        &mut db,
        &administrators,
        &ProvisionOptions::default(),
    )?);

    let narrowed = ops(json!([{ "object": "orders", "permissions": ["read"] }]))?;
    let allow = weakening("security.roles.ops");
    let actor = ProvisionActor::Principal(db.principal_for_user("ops"));
    match apply_as(
        &mut db,
        &actor,
        &narrowed,
        &allow,
        &env(ProvisionSurface::ControlPlane),
    ) {
        Err(ProvisionError::Blocked(plan)) => assert!(has_issue(
            &plan,
            "RBAC_SELF_LOCKOUT",
            ValidationSeverity::Error
        )),
        other => return Err(format!("expected a lockout, got {other:?}").into()),
    }
    assert!(
        db.authz_policy()
            .roles()
            .get("ops")
            .is_some_and(|role| role.allows(&Resource::System, Permission::Admin))
    );
    // A local operator relies on filesystem trust, not on the role.
    assert_applied(&apply_cli(&mut db, &narrowed, &allow)?);
    Ok(())
}

fn assert_self_lockout(result: Result<ProvisionApplyReport, ProvisionError>) -> TestResult {
    match result {
        Err(ProvisionError::Blocked(plan)) => {
            assert!(has_issue(
                &plan,
                "RBAC_SELF_LOCKOUT",
                ValidationSeverity::Error
            ));
            Ok(())
        }
        other => Err(format!("expected a lockout, got {other:?}").into()),
    }
}

#[test]
fn a_principal_cannot_remove_its_own_managed_principal() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let base = orders_spec()?;
    let mut db = create_with_profile(&temp_dir.path().join("lockout.redb"), &base)?;
    let administrators = |users: &[&str]| {
        let principals: Vec<JsonValue> = users
            .iter()
            .map(|user| json!({ "user": user, "roles": ["ops"] }))
            .collect();
        with_security(
            base.clone(),
            &json!({
                "roles": [{
                    "name": "ops",
                    "grants": [{ "resource": "database", "permissions": ["admin"] }]
                }],
                "principals": principals
            }),
        )
    };
    assert_applied(&apply_cli(
        &mut db,
        &administrators(&["ops", "ops2"])?,
        &ProvisionOptions::default(),
    )?);

    let without_ops = administrators(&["ops2"])?;
    let allow = destructive(&["security.principals.ops"]);
    let ops = ProvisionActor::Principal(db.principal_for_user("ops"));
    let control_plane = env(ProvisionSurface::ControlPlane);
    assert_self_lockout(apply_as(
        &mut db,
        &ops,
        &without_ops,
        &allow,
        &control_plane,
    ))?;
    assert!(db.principal_registry().principals().contains_key("ops"));
    // The plan is refused at plan time too, for the principal the Control Plane passes.
    let plan = plan_provision(
        &without_ops,
        ProvisionTargetRef::Open(&db),
        &allow,
        &ProvisionEnvironment {
            principal: Some(db.principal_for_user("ops")),
            ..env(ProvisionSurface::ControlPlane)
        },
    )?;
    assert!(has_issue(
        &plan,
        "RBAC_SELF_LOCKOUT",
        ValidationSeverity::Error
    ));

    // Another administrator may remove it.
    let ops2 = ProvisionActor::Principal(db.principal_for_user("ops2"));
    assert_applied(&apply_as(
        &mut db,
        &ops2,
        &without_ops,
        &allow,
        &control_plane,
    )?);
    assert!(!db.principal_registry().principals().contains_key("ops"));
    Ok(())
}

#[test]
fn self_lockout_resolves_a_principal_named_differently_from_its_user() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let base = orders_spec()?;
    let mut db = create_with_profile(&temp_dir.path().join("lockout.redb"), &base)?;
    let alice = |principal: &str, roles: &[&str]| {
        with_security(
            base.clone(),
            &json!({
                "roles": [{
                    "name": "ops",
                    "grants": [{ "resource": "database", "permissions": ["admin"] }]
                }],
                "principals": [{ "user": "alice", "principal": principal, "roles": roles }]
            }),
        )
    };
    assert_applied(&apply_cli(
        &mut db,
        &alice("alice_p", &["ops"])?,
        &ProvisionOptions::default(),
    )?);
    let actor = db.principal_for_user("alice");
    assert_eq!(actor.name(), "alice_p");
    let actor = ProvisionActor::Principal(actor);
    let allow = weakening("security.principals.alice");
    let control_plane = env(ProvisionSurface::ControlPlane);

    // Stripping the role from the principal alice maps to locks alice out.
    assert_self_lockout(apply_as(
        &mut db,
        &actor,
        &alice("alice_p", &[])?,
        &allow,
        &control_plane,
    ))?;
    assert!(db.principal_for_user("alice").roles().contains("ops"));

    // Renaming the principal keeps alice an administrator, so it is not a lockout.
    assert_applied(&apply_as(
        &mut db,
        &actor,
        &alice("alice_q", &["ops"])?,
        &allow,
        &control_plane,
    )?);
    assert_eq!(db.principal_for_user("alice").name(), "alice_q");
    Ok(())
}

#[tokio::test]
async fn a_spec_granted_role_runs_sql_and_others_are_denied() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let ledger = spec(
        "balanced",
        &json!([{
            "name": "ledger", "role": "document_entity", "domain": "primary", "indexes": ["primary"],
            "physical": {
                "kind": "table",
                "columns": [
                    { "name": "entry_id", "type": "int" },
                    { "name": "amount", "type": "float" }
                ],
                "primary_key": "entry_id"
            }
        }]),
    )?;
    let spec = with_security(
        ledger,
        &json!({
            "roles": [{
                "name": "ledger_reader",
                "grants": [{ "object": "ledger", "permissions": ["read"] }]
            }],
            "principals": [{ "user": "analyst", "roles": ["ledger_reader"] }]
        }),
    )?;
    let mut db = create_with_profile(&temp_dir.path().join("sql.redb"), &spec)?;
    assert_applied(&apply_cli(&mut db, &spec, &ProvisionOptions::default())?);
    db.query("INSERT INTO ledger (entry_id, amount) VALUES (1, 2.5)")
        .await?;

    let analyst = db.principal_for_user("analyst");
    assert_eq!(
        db.query_as(&analyst, "SELECT entry_id FROM ledger").await?,
        SqlOutput::Rows(SqlRows {
            columns: vec!["entry_id".to_owned()],
            rows: vec![vec![Value::Int(1)]],
        })
    );
    assert!(matches!(
        db.query_as(
            &analyst,
            "INSERT INTO ledger (entry_id, amount) VALUES (2, 1.0)"
        )
        .await,
        Err(DbError::AuthzDenied(_))
    ));
    let stranger = db.principal_for_user("stranger");
    assert!(matches!(
        db.query_as(&stranger, "SELECT entry_id FROM ledger").await,
        Err(DbError::AuthzDenied(_))
    ));
    Ok(())
}

/// `SECURITY_INDEX_GRANT_WITHOUT_SOURCE_READ` warns about exactly what the runtime refuses:
/// a role that reads a full-text index but not its source collection cannot search it.
#[tokio::test]
async fn an_index_grant_without_source_read_warns_and_cannot_search() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let spec = with_security(
        full_spec()?,
        &json!({
            "roles": [
                {
                    "name": "searcher",
                    "grants": [{ "object": "orders_notes_fts", "permissions": ["read"] }]
                },
                {
                    "name": "reader",
                    "grants": [
                        { "object": "orders", "permissions": ["read"] },
                        { "object": "orders_notes_fts", "permissions": ["read"] }
                    ]
                }
            ],
            "principals": [
                { "user": "searcher_svc", "roles": ["searcher"] },
                { "user": "reader_svc", "roles": ["reader"] }
            ]
        }),
    )?;
    let mut db = create_with_profile(&temp_dir.path().join("search.redb"), &spec)?;
    let plan = plan_open(&spec, &db)?;
    let warned = plan
        .validation
        .issues
        .iter()
        .filter(|issue| issue.code == "SECURITY_INDEX_GRANT_WITHOUT_SOURCE_READ")
        .map(|issue| issue.path.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        warned,
        vec!["security.roles[0].grants[0]"],
        "{:#?}",
        plan.validation.issues
    );
    assert_applied(&apply_cli(&mut db, &spec, &ProvisionOptions::default())?);
    let id = db
        .collection("orders")?
        .insert(&shipped_order("c-1", "fragile glass", 13.4, 52.5))?;

    let search = "SELECT * FROM match('orders_notes_fts', 'fragile', 10)";
    let searcher = db.principal_for_user("searcher_svc");
    assert!(matches!(
        db.query_as(&searcher, search).await,
        Err(DbError::AuthzDenied(_))
    ));
    let reader = db.principal_for_user("reader_svc");
    let SqlOutput::Rows(rows) = db.query_as(&reader, search).await? else {
        return Err("match() returns rows".into());
    };
    assert_eq!(rows.rows.len(), 1, "{rows:?} for {id:?}");
    Ok(())
}

#[test]
fn releasing_a_managed_object_needs_its_flag_and_keeps_its_data() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let both = with_notes(orders_spec()?)?;
    let mut db = create_with_profile(&temp_dir.path().join("release.redb"), &both)?;
    assert_applied(&apply_cli(&mut db, &both, &ProvisionOptions::default())?);
    let id = db.collection("orders")?.insert(&order_document("c-1"))?;

    let mut notes_only = both.clone();
    notes_only
        .collections
        .retain(|collection| collection.name == "notes");
    assert!(matches!(
        apply_cli(&mut db, &notes_only, &ProvisionOptions::default()),
        Err(ProvisionError::NeedsFlags(plan))
            if plan.required_flags == ["allow_destructive:collections.orders"]
    ));
    let events = db.audit_events()?.len();
    let report = apply_cli(&mut db, &notes_only, &destructive(&["collections.orders"]))?;
    assert_applied(&report);
    assert!(db.collection("orders")?.get(id)?.is_some());
    let record = db.provision_state()?.ok_or("a provision record")?;
    assert!(!record.objects.contains_key("orders"));
    assert!(record.released_objects.contains("orders"));
    assert!(
        record
            .flags
            .contains(&"allow_destructive:collections.orders".to_owned())
    );
    let released = db.audit_events()?.split_off(events);
    assert!(actions(&released).contains(&"provision_release_object"));
    assert!(drift(&db, &notes_only, &ProvisionOptions::default())?.in_sync);
    assert_eq!(plan_open(&notes_only, &db)?.outcome, PlanOutcome::Noop);

    // Declaring it again adopts it back.
    let report = apply_cli(&mut db, &both, &ProvisionOptions::default())?;
    assert_applied(&report);
    assert!(
        report
            .applied_steps
            .iter()
            .any(|step| step.kind == ProvisionStepKind::AdoptObject)
    );
    let record = db.provision_state()?.ok_or("a provision record")?;
    assert!(
        record
            .objects
            .get("orders")
            .is_some_and(|object| object.adopted)
    );
    assert!(record.released_objects.is_empty());
    Ok(())
}

#[test]
fn the_storage_format_upgrade_writes_manifest_and_version_together() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("upgrade.redb");
    let base = orders_spec()?;
    let zstd = with_custom_profile(
        base.clone(),
        "orders_zstd",
        &json!({ "extends": "balanced", "knobs": { "compression": { "algorithm": "zstd" } } }),
    )?;
    let (old, new) = {
        let mut db = create_with_profile(&path, &base)?;
        assert_applied(&apply_cli(&mut db, &base, &ProvisionOptions::default())?);
        let old = db.collection("orders")?.insert(&order_document("old"))?;

        assert!(matches!(
            apply_cli(&mut db, &zstd, &ProvisionOptions::default()),
            Err(ProvisionError::NeedsFlags(plan)) if plan.required_flags == ["allow_format_upgrade"]
        ));
        let allow = ProvisionOptions {
            allow_format_upgrade: true,
            ..ProvisionOptions::default()
        };
        let before = latest_lsn(&db)?;
        let report = apply_cli(&mut db, &zstd, &allow)?;
        assert_applied(&report);
        assert!(report.session_pending_restart);
        let commits = table_commits(&db, before, "__meta")?;
        assert_eq!(
            commits,
            vec![BTreeSet::from([
                b"profile_manifest".to_vec(),
                b"schema_version".to_vec()
            ])]
        );
        let metadata = db.probed_metadata()?;
        assert_eq!(metadata.schema_version, 2);
        let manifest = metadata.manifest.ok_or("a manifest")?;
        assert!(manifest.storage_format.value_framing);
        assert_eq!(manifest.profile, "orders_zstd");
        // The session keeps its unframed engine until the database is opened again.
        assert_eq!(db.engine_kind(), EngineKind::Redb);
        assert_eq!(
            plan_open_with(&zstd, &db, &allow)?
                .drift
                .items
                .iter()
                .map(|item| item.kind)
                .collect::<Vec<_>>(),
            vec![DriftKind::SessionPendingRestart]
        );
        drop(db);

        match crate::db::inspect_database_metadata(&path) {
            crate::db::MetadataProbe::Initialized(probe) => assert_eq!(probe.schema_version, 2),
            other => return Err(format!("expected an initialized database, got {other:?}").into()),
        }
        let db = open_database_with_ops(
            DbConfig::on_disk(Profile::Balanced, &path),
            &OperationalConfig::new(),
        )?;
        assert_eq!(db.engine_kind(), EngineKind::CompressedRedb);
        assert!(db.collection("orders")?.get(old)?.is_some());
        let new = db.collection("orders")?.insert(&order_document("new"))?;
        assert_eq!(plan_open(&zstd, &db)?.outcome, PlanOutcome::Noop);
        assert!(drift(&db, &zstd, &ProvisionOptions::default())?.in_sync);
        let collection_id = match db.catalog().get("orders") {
            Some(CatalogEntry::Collection { collection_id, .. }) => *collection_id,
            other => return Err(format!("orders: {other:?}").into()),
        };
        drop(db);
        (old, (new, collection_id))
    };

    // Old values stay unframed and new ones are framed.
    let (new, collection_id) = new;
    let engine = RedbEngine::open(&path)?;
    let read = engine.begin_read()?;
    let (start, end) = crate::model::collection_range_bounds(collection_id);
    let documents = read
        .range(crate::model::DOCUMENT_TABLE, &start, &end)?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter_map(|(key, value)| {
            crate::model::document_id_from_key(&key).map(|id| (id, value.starts_with(b"MDBC1")))
        })
        .collect::<Vec<_>>();
    assert!(documents.contains(&(old, false)), "{documents:?}");
    assert!(documents.contains(&(new, true)), "{documents:?}");
    Ok(())
}

#[test]
fn backup_and_restore_carry_the_record_and_the_manifest() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let spec = full_spec()?;
    let path = temp_dir.path().join("source.redb");
    let mut db = create_with_profile(&path, &spec)?;
    assert_applied(&apply_cli(&mut db, &spec, &ProvisionOptions::default())?);
    let record = db.provision_state()?;
    let manifest = db.probed_metadata()?.manifest;
    assert!(
        manifest
            .as_ref()
            .is_some_and(|manifest| !manifest.knobs.floors.encryption_at_rest)
    );
    let history = db.provision_history()?;

    let backup = full_backup(
        &db,
        temp_dir.path().join("backups"),
        &BackupConfig::default(),
    )?;
    verify_backup(&backup.path)?;
    drop(db);
    let restored_path = temp_dir.path().join("restored.redb");
    restore_backup(
        &backup.path,
        DbConfig::on_disk(Profile::Balanced, &restored_path),
        RestoreTarget::Latest,
    )?;
    let restored = open_database_with_ops(
        DbConfig::on_disk(Profile::Balanced, &restored_path),
        &OperationalConfig::new(),
    )?;
    assert_eq!(restored.provision_state()?, record);
    assert_eq!(restored.provision_history()?, history);
    assert_eq!(restored.probed_metadata()?.manifest, manifest);
    assert_eq!(plan_open(&spec, &restored)?.outcome, PlanOutcome::Noop);
    Ok(())
}

/// Restoring an incremental chain replays an apply made after the full backup: the restored
/// database holds its record, history, objects and backfilled index entries, and plans as a
/// noop.
#[test]
fn an_incremental_restore_replays_an_apply_after_the_full_backup() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let first = orders_spec()?;
    let mut db = create_with_profile(&temp_dir.path().join("source.redb"), &first)?;
    assert_applied(&apply_cli(&mut db, &first, &ProvisionOptions::default())?);
    let early =
        db.collection("orders")?
            .insert(&shipped_order("c-1", "fragile glass", 13.4, 52.5))?;
    let root = temp_dir.path().join("backups");
    let full = full_backup(&db, &root, &BackupConfig::default())?;

    // The second apply creates and backfills the indexes and builds the other objects.
    let spec = full_spec()?;
    assert_applied(&apply_cli(&mut db, &spec, &ProvisionOptions::default())?);
    let late =
        db.collection("orders")?
            .insert(&shipped_order("c-2", "fragile vase", -70.0, -30.0))?;
    let record = db.provision_state()?;
    let history = db.provision_history()?;
    assert_eq!(history.len(), 2);
    let incremental =
        crate::backup::incremental_backup(&db, &root, &full.path, &BackupConfig::default())?;
    verify_backup(&incremental.path)?;
    drop(db);

    let restored_path = temp_dir.path().join("restored.redb");
    restore_backup(
        &incremental.path,
        DbConfig::on_disk(Profile::Balanced, &restored_path),
        RestoreTarget::Latest,
    )?;
    let restored = open_database_with_ops(
        DbConfig::on_disk(Profile::Balanced, &restored_path),
        &OperationalConfig::new(),
    )?;
    assert_eq!(restored.provision_state()?, record);
    assert_eq!(restored.provision_history()?, history);
    assert_eq!(plan_open(&spec, &restored)?.outcome, PlanOutcome::Noop);
    let mut hits = restored
        .full_text_index("orders_notes_fts")?
        .search("fragile", 10)?
        .into_iter()
        .map(|hit| hit.id)
        .collect::<Vec<_>>();
    hits.sort();
    let mut expected = vec![early, late];
    expected.sort();
    assert_eq!(hits, expected);
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Starting from a spec
// ---------------------------------------------------------------------------------------------

const MODES: [StartupMode; 3] = [
    StartupMode::Verify,
    StartupMode::Create,
    StartupMode::Additive,
];

/// The decision of `plan` in verify, create and additive mode.
fn decisions(plan: &ProvisionPlan) -> [StartupDecision; 3] {
    MODES.map(|mode| serve_startup_decision(plan, mode))
}

fn refuses(decision: &StartupDecision, reason: &str) -> bool {
    matches!(decision, StartupDecision::Refuse(found) if found.contains(reason))
}

/// The refusal of a start, or an error describing what happened instead.
fn refusal(
    result: Result<(Database, StartupReport), ProvisionError>,
) -> Result<StartupRefusal, Error> {
    match result {
        Err(ProvisionError::StartupRefused(refusal)) => Ok(*refusal),
        Err(other) => Err(format!("expected a startup refusal, got {other}").into()),
        Ok((_, report)) => Err(format!("expected a startup refusal, got {report:?}").into()),
    }
}

/// Every row of the startup table in every mode, decided on real plans.
#[test]
#[allow(clippy::too_many_lines)]
fn serve_startup_decision_table() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let serve = env(ProvisionSurface::Serve);
    let defaults = ProvisionOptions::default();
    let base = orders_spec()?;
    let plan_open_on = |spec: &DatabaseSpec, db: &Database, opts: &ProvisionOptions| {
        plan_provision(spec, ProvisionTargetRef::Open(db), opts, &serve)
    };

    // Absent and auto-approvable: verify refuses, create and additive apply.
    let missing = temp_dir.path().join("missing.redb");
    let absent = plan_path(&base, &missing, ProvisionSurface::Serve)?;
    assert!(absent.auto_approvable);
    let [verify, create, additive] = decisions(&absent);
    assert!(refuses(&verify, "database missing at"), "{verify:?}");
    assert!(refuses(&verify, "MULTIDB_SPEC_APPLY=create"), "{verify:?}");
    assert_eq!(create, StartupDecision::ApplyThenStart);
    assert_eq!(additive, StartupDecision::ApplyThenStart);
    let volatile = plan_provision(
        &in_memory(base.clone()),
        ProvisionTargetRef::Volatile,
        &defaults,
        &env(ProvisionSurface::RustApi),
    )?;
    assert!(refuses(
        &serve_startup_decision(&volatile, StartupMode::Verify),
        "StartupMode::Create"
    ));

    // Absent but not auto-approvable: refused in every mode.
    let mut audit_off = base.clone();
    audit_off.guarantees.audit.enabled = false;
    let mut at_rest = base.clone();
    at_rest.guarantees.encryption.at_rest = true;
    let mut invalid = base.clone();
    invalid.collections[0].name = "__orders".to_owned();
    for (name, spec, reason) in [
        ("needs flags", &audit_off, "needs flags a start never gives"),
        ("blocked", &at_rest, "GUARANTEE_NOT_ENFORCEABLE"),
        ("invalid", &invalid, "the spec is invalid"),
    ] {
        let plan = plan_path(spec, &missing, ProvisionSurface::Serve)?;
        assert!(!plan.auto_approvable, "{name}");
        for decision in decisions(&plan) {
            assert!(refuses(&decision, reason), "{name}: {decision:?}");
        }
    }

    // Existing and a noop: start in every mode, also with information-only drift.
    let mut db = create_with_profile(&temp_dir.path().join("startup.redb"), &base)?;
    assert_applied(&apply_cli(&mut db, &base, &defaults)?);
    let noop = plan_open_on(&base, &db, &defaults)?;
    assert_eq!(noop.outcome, PlanOutcome::Noop);
    assert_eq!(decisions(&noop), [const { StartupDecision::Start }; 3]);
    db.create_collection_auto_id("scratch", orders_fields(), Vec::new())?;
    let unmanaged = plan_open_on(&base, &db, &defaults)?;
    assert_eq!(unmanaged.outcome, PlanOutcome::Noop);
    assert!(unmanaged.drift.in_sync && !unmanaged.drift.items.is_empty());
    assert_eq!(decisions(&unmanaged), [const { StartupDecision::Start }; 3]);

    // Existing with an auto-approvable additive plan: applied in additive mode only.
    let additive = plan_open_on(&with_notes(base.clone())?, &db, &defaults)?;
    assert!(additive.auto_approvable);
    let [verify, create, apply] = decisions(&additive);
    for (mode, decision) in [("verify", &verify), ("create", &create)] {
        assert!(
            refuses(decision, &format!("the {mode} startup mode never changes")),
            "{decision:?}"
        );
        assert!(
            refuses(decision, "MULTIDB_SPEC_APPLY=additive"),
            "{decision:?}"
        );
    }
    assert_eq!(apply, StartupDecision::ApplyThenStart);

    // Anything else is refused in every mode: retune, weakening, destructive, format upgrade,
    // RBAC on an existing database, missing flags and blocked plans.
    let retune = with_custom_profile(
        base.clone(),
        "orders_custom",
        &json!({ "extends": "balanced", "knobs": { "cache": { "max_entries": 4096 } } }),
    )?;
    let zstd = with_custom_profile(
        base.clone(),
        "orders_zstd",
        &json!({ "extends": "balanced", "knobs": { "compression": { "algorithm": "zstd" } } }),
    )?;
    let rbac = with_security(
        base.clone(),
        &json!({
            "roles": [{ "name": "auditor", "grants": [{ "resource": "database", "permissions": ["read"] }] }],
            "principals": []
        }),
    )?;
    let mut released = base.clone();
    released.collections.clear();
    let mut conflict = base.clone();
    conflict.collections[0].physical = Some(serde_json::from_value(json!({
        "fields": [{ "name": "customer_id", "path": "customer.id", "type": "int" }],
        "document_indexes": [{ "path": "customer.id" }]
    }))?);
    let format_upgrade = ProvisionOptions {
        allow_format_upgrade: true,
        ..ProvisionOptions::default()
    };
    for (name, spec, opts, reason) in [
        ("retune", &retune, defaults.clone(), "has retune steps"),
        (
            "weakening",
            &audit_off,
            weakening("guarantees.audit.enabled"),
            "has weakening steps",
        ),
        (
            "weakening without its flag",
            &audit_off,
            defaults.clone(),
            "allow_weakening:guarantees.audit.enabled",
        ),
        (
            "destructive",
            &released,
            destructive(&["collections.orders"]),
            "has destructive steps",
        ),
        (
            "format upgrade",
            &zstd,
            format_upgrade,
            "has format_upgrade steps",
        ),
        (
            "rbac",
            &rbac,
            defaults.clone(),
            "changes RBAC on an existing database",
        ),
        (
            "conflict",
            &conflict,
            defaults.clone(),
            "PROVISION_OBJECT_CONFLICT",
        ),
        ("at rest", &at_rest, defaults.clone(), "is blocked"),
    ] {
        let plan = plan_open_on(spec, &db, &opts)?;
        assert!(!plan.auto_approvable, "{name}: {:#?}", plan.steps);
        for decision in decisions(&plan) {
            assert!(refuses(&decision, reason), "{name}: {decision:?}");
        }
    }

    // An interrupted apply whose remaining steps are auto-approvable: additive mode only.
    let notes = with_notes(base.clone())?;
    let mut failed = create_with_profile(&temp_dir.path().join("failed.redb"), &notes)?;
    let index = plan_open_on(&notes, &failed, &defaults)?
        .steps
        .iter()
        .position(|step| step.object.as_deref() == Some("notes"))
        .ok_or("notes is created")?;
    {
        let _failure = InjectedFailure::at(index);
        let report = apply_cli(&mut failed, &notes, &defaults)?;
        assert_eq!(report.status, ProvisionOutcome::PartiallyApplied);
    }
    let interrupted = plan_open_on(&notes, &failed, &defaults)?;
    assert!(interrupted.auto_approvable);
    let [verify, create, apply] = decisions(&interrupted);
    for (mode, decision) in [("verify", &verify), ("create", &create)] {
        assert!(
            refuses(
                decision,
                &format!("the {mode} startup mode never resumes it")
            ),
            "{decision:?}"
        );
    }
    assert_eq!(apply, StartupDecision::ApplyThenStart);
    // ... and one whose remaining plan is not auto-approvable is refused in every mode.
    let rbac_notes = with_security(
        notes,
        &json!({
            "roles": [{ "name": "auditor", "grants": [{ "resource": "database", "permissions": ["read"] }] }],
            "principals": []
        }),
    )?;
    let interrupted_rbac = plan_open_on(&rbac_notes, &failed, &defaults)?;
    for decision in decisions(&interrupted_rbac) {
        assert!(
            refuses(&decision, "changes RBAC on an existing database"),
            "{decision:?}"
        );
    }
    Ok(())
}

#[test]
fn startup_modes_parse_their_slugs() {
    for mode in MODES {
        assert_eq!(StartupMode::from_slug(mode.slug()), Some(mode));
        assert_eq!(
            StartupMode::from_slug(&format!(" {} ", mode.slug().to_ascii_uppercase())),
            Some(mode)
        );
    }
    assert_eq!(StartupMode::default(), StartupMode::Verify);
    assert_eq!(StartupMode::from_slug("apply"), None);
    assert_eq!(StartupMode::from_slug(""), None);
}

#[test]
fn open_from_spec_builds_volatile_databases_on_the_rust_api_only() -> TestResult {
    let spec = in_memory(orders_spec()?);
    let rust = env(ProvisionSurface::RustApi);
    let defaults = ProvisionOptions::default();

    let refused = refusal(open_from_spec(
        &spec,
        None,
        StartupMode::Verify,
        &defaults,
        &rust,
    ))?;
    assert!(
        refused.reason.contains("StartupMode::Create"),
        "{}",
        refused.reason
    );
    assert_eq!(refused.plan.target.kind, TargetKind::NewDatabase);
    assert!(refused.applied.is_none());

    for mode in [StartupMode::Create, StartupMode::Additive] {
        let (db, report) = open_from_spec(&spec, None, mode, &defaults, &rust)?;
        assert_eq!(report.mode, mode);
        assert_eq!(report.decision, StartupDecision::ApplyThenStart);
        let applied = report.applied.as_ref().ok_or("the startup apply")?;
        assert_applied(applied);
        assert_eq!(report.plan_id, applied.plan_id);
        assert_eq!(
            applied
                .record
                .as_ref()
                .map(|record| record.applied_by.as_str()),
            Some("local:embedded")
        );
        assert!(db.config().path.is_none());
        assert!(db.catalog().contains_key("orders"));
        assert_eq!(plan_rust_api(&spec, &db)?.outcome, PlanOutcome::Noop);
    }

    // serve builds durable databases only.
    let refused = refusal(open_from_spec(
        &spec,
        None,
        StartupMode::Create,
        &defaults,
        &env(ProvisionSurface::Serve),
    ))?;
    assert!(has_issue(
        &refused.plan,
        "PROVISION_VOLATILE_ON_SURFACE",
        ValidationSeverity::Error
    ));
    assert!(refused.reason.contains("is blocked"), "{}", refused.reason);

    // The CLI and the Control Plane do not start from specs, and only the Rust API encrypts.
    for surface in [ProvisionSurface::Cli, ProvisionSurface::ControlPlane] {
        assert!(matches!(
            open_from_spec(&spec, None, StartupMode::Create, &defaults, &env(surface)),
            Err(ProvisionError::Config(ConfigError::Unsupported(_)))
        ));
    }
    let temp_dir = tempfile::tempdir()?;
    let encrypted = ProvisionEnvironment {
        encryption: Some(EncryptionConfig::local_envelope(
            temp_dir.path().join("keyring.json"),
            temp_dir.path().join("kek.key"),
        )),
        ..env(ProvisionSurface::Serve)
    };
    assert!(matches!(
        open_from_spec(
            &orders_spec()?,
            Some(&temp_dir.path().join("encrypted.redb")),
            StartupMode::Create,
            &defaults,
            &encrypted
        ),
        Err(ProvisionError::Config(ConfigError::Unsupported(_)))
    ));
    assert!(!temp_dir.path().join("encrypted.redb").exists());
    Ok(())
}

#[test]
fn open_from_spec_keeps_the_audit_chain_on_the_callers_key() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let key_path = temp_dir.path().join("audit.key");
    std::fs::write(&key_path, "5a".repeat(32))?;
    let path = temp_dir.path().join("keyed.redb");
    let spec = orders_spec()?;
    let defaults = ProvisionOptions::default();
    let keyed = ProvisionEnvironment {
        audit: Some(AuditConfig {
            key_path: Some(key_path.clone()),
            anchor_path: None,
        }),
        ..env(ProvisionSurface::RustApi)
    };

    let (db, report) = open_from_spec(&spec, Some(&path), StartupMode::Create, &defaults, &keyed)?;
    assert_eq!(report.decision, StartupDecision::ApplyThenStart);
    assert!(
        report
            .warnings
            .iter()
            .all(|warning| warning.code != "AUDIT_DEVELOPMENT_KEY")
    );
    db.verify_audit_chain()?;
    drop(db);

    // Every later open chains with the same key, so the chain keeps verifying.
    for check in ["first_reopen", "second_reopen"] {
        let (db, report) =
            open_from_spec(&spec, Some(&path), StartupMode::Verify, &defaults, &keyed)?;
        assert_eq!(report.decision, StartupDecision::Start);
        assert!(report.applied.is_none());
        db.record_admin_auth_event(None, check, AuditOutcome::Succeeded, None)?;
        db.verify_audit_chain()?;
    }

    // A handle without the key falls back to the development key: the chain does not verify.
    let plain = open_database_with_ops(
        DbConfig::on_disk(Profile::Balanced, &path),
        &OperationalConfig::new(),
    )?;
    assert!(matches!(
        plain.verify_audit_chain(),
        Err(DbError::AuditIntegrity(_))
    ));
    db_with_development_key_plans_only_noops(&plain, &spec)?;
    drop(plain);

    // With the chain's key the same change applies, and the chain keeps verifying.
    let extended = with_notes(spec.clone())?;
    let (db, report) = open_from_spec(
        &extended,
        Some(&path),
        StartupMode::Additive,
        &defaults,
        &keyed,
    )?;
    assert_eq!(report.decision, StartupDecision::ApplyThenStart);
    assert!(db.catalog().contains_key("notes"));
    db.verify_audit_chain()?;
    drop(db);

    // An unusable key is refused when it is applied, before anything is planned or written.
    std::fs::write(&key_path, "not a key")?;
    assert!(matches!(
        open_from_spec(&spec, Some(&path), StartupMode::Verify, &defaults, &keyed),
        Err(ProvisionError::Db(DbError::AuditIntegrity(_)))
    ));
    Ok(())
}

/// A handle that chains with the development key, on a chain written with a configured key:
/// a noop plan writes nothing and is not blocked, but any plan with steps would append events
/// the chain cannot verify, on every surface.
fn db_with_development_key_plans_only_noops(db: &Database, spec: &DatabaseSpec) -> TestResult {
    assert_eq!(db.audit_tail_matches_key()?, Some(false));
    assert_eq!(plan_open(spec, db)?.outcome, PlanOutcome::Noop);
    let extended = with_notes(spec.clone())?;
    for surface in [ProvisionSurface::Cli, ProvisionSurface::RustApi] {
        let plan = plan_provision(
            &extended,
            ProvisionTargetRef::Open(db),
            &ProvisionOptions::default(),
            &env(surface),
        )?;
        assert_blocked(&plan, "PROVISION_AUDIT_KEY_MISMATCH");
        let Some(found) = plan
            .issues
            .iter()
            .find(|found| found.code == "PROVISION_AUDIT_KEY_MISMATCH")
        else {
            return Err("missing PROVISION_AUDIT_KEY_MISMATCH".into());
        };
        assert_eq!(found.path, "guarantees.audit.enabled");
        assert!(
            found.message.contains("development key"),
            "{}",
            found.message
        );
        assert!(
            found.suggestion.contains("open_from_spec"),
            "{}",
            found.suggestion
        );
    }
    Ok(())
}

#[test]
fn an_audit_chain_on_the_handles_key_does_not_block_a_plan() -> TestResult {
    let spec = in_memory(orders_spec()?);
    let db = create_in_memory(&spec)?;
    let expected = if db.audit_events()?.is_empty() {
        None
    } else {
        Some(true)
    };
    assert_eq!(db.audit_tail_matches_key()?, expected);
    db.record_admin_auth_event(None, "check", AuditOutcome::Succeeded, None)?;
    assert_eq!(db.audit_tail_matches_key()?, Some(true));
    let plan = plan_rust_api(&with_notes(spec)?, &db)?;
    assert!(
        !issue_codes(&plan).contains(&"PROVISION_AUDIT_KEY_MISMATCH"),
        "{:#?}",
        plan.issues
    );
    Ok(())
}

#[test]
fn open_from_spec_resumes_an_interrupted_apply_only_in_additive_mode() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("interrupted.redb");
    let spec = with_notes(orders_spec()?)?;
    let serve = env(ProvisionSurface::Serve);
    let defaults = ProvisionOptions::default();
    let index = plan_path(&spec, &path, ProvisionSurface::Serve)?
        .steps
        .iter()
        .position(|step| step.object.as_deref() == Some("notes"))
        .ok_or("notes is created")?;

    let refused = {
        let _failure = InjectedFailure::at(index);
        refusal(open_from_spec(
            &spec,
            Some(&path),
            StartupMode::Create,
            &defaults,
            &serve,
        ))?
    };
    assert!(
        refused.reason.contains("did not finish"),
        "{}",
        refused.reason
    );
    let applied = refused.applied.ok_or("the partial startup apply")?;
    assert_eq!(applied.status, ProvisionOutcome::PartiallyApplied);
    assert!(path.exists(), "the new database is kept");

    for mode in [StartupMode::Verify, StartupMode::Create] {
        let refused = refusal(open_from_spec(&spec, Some(&path), mode, &defaults, &serve))?;
        assert!(
            refused.reason.contains("DRIFT_INTERRUPTED_APPLY"),
            "{}",
            refused.reason
        );
        assert!(refused.plan.auto_approvable);
        assert!(refused.applied.is_none());
    }

    let (db, report) =
        open_from_spec(&spec, Some(&path), StartupMode::Additive, &defaults, &serve)?;
    assert_eq!(report.decision, StartupDecision::ApplyThenStart);
    let applied = report.applied.as_ref().ok_or("the startup apply")?;
    assert_applied(applied);
    assert_eq!(
        applied
            .record
            .as_ref()
            .map(|record| record.applied_by.as_str()),
        Some("local:serve")
    );
    assert!(db.catalog().contains_key("notes"));
    assert_eq!(
        db.provision_state()?.map(|record| record.status),
        Some(ProvisionRecordStatus::Applied)
    );
    drop(db);

    let (_db, report) = open_from_spec(&spec, Some(&path), StartupMode::Verify, &defaults, &serve)?;
    assert_eq!(report.decision, StartupDecision::Start);
    Ok(())
}
