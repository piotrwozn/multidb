//! Provisioning plans, drift and export against real databases.

use std::collections::BTreeSet;
use std::path::Path;

use serde_json::{Value as JsonValue, json};

use super::live::LiveState;
use super::physical::project;
use super::plan::{PlanIdInputs, canonical_target_path, desired_role, plan_id_for};
use super::record::{RECORD_KEY, RecordState, decode_record};
use super::{
    DriftKind, DriftResolution, EnforcementStatus, ExportSource, ManagedObject, ObjectKind,
    ObjectState, PlanOutcome, ProvisionEnvironment, ProvisionError, ProvisionOptions,
    ProvisionPlan, ProvisionRecord, ProvisionRecordStatus, ProvisionStepKind, ProvisionSurface,
    ProvisionTargetRef, RECORD_VERSION, StepSafety, TargetKind, drift, export_spec, plan_provision,
};
use crate::cdc::{AggregateKind, AggregateSpec, MaterializedViewSpec};
use crate::config_spec::{
    ConflictResolution, ConsistencyDomainSpec, ConsistencyMode, DATABASE_SPEC_VERSION_V2,
    DatabaseSpec, DeploymentMode, ExtensionManifestRef, ExtensionStability, GuaranteeValidator,
    ReplicationMode, ResolvedProfile, RiskLevel, UnmanagedPolicy, ValidationSeverity,
    resolve_profile, spec_digest,
    spec_io::{SpecFormat, decode_spec_bytes},
};
use crate::db::{
    AuditConfig, CatalogEntry, ConfigError, Database, DbConfig, EncryptionConfig,
    OperationalConfig, PROVISION_TABLE, Profile, ReplicationKind, SecurityConfig, create_database,
    create_database_with_ops, create_database_with_profile, open_database, open_database_with_ops,
};
use crate::extension::{LimitPolicy, PolicyConfig};
use crate::geo::GeoIndexConfig;
use crate::graph::GraphId;
use crate::model::{CollectionId, FieldPath, IndexId, IndexSpec, Value};
use crate::query::{
    ColumnDef, ColumnType, DocField, RelIndexExpression, RelIndexSpec, RelPredicate, TableLayout,
    TableSchema,
};
use crate::repl::{Op, ReadConsistency, Replication, propose_system};
use crate::security::{AuthzPolicy, Permission, Principal, PrincipalRegistry, Resource, Role};
use crate::temporal::TemporalRetention;
use crate::text::FullTextIndexConfig;
use crate::timeseries::TimeSeriesConfig;
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
    assert!(plan.required_flags.is_empty());
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
    assert!(plan.issues.iter().any(|issue| issue.path == "topology"));

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
        assert!(plan.steps.is_empty());
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
    let plainly_opened = open_database(DbConfig::on_disk(Profile::Balanced, &stale_path))?;
    assert_blocked(
        &plan_open(&empty, &plainly_opened)?,
        "PROVISION_SECURITY_STATE_STALE",
    );
    drop(plainly_opened);
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
    assert!(!plan.steps.is_empty());
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
        assert!(step.action.contains("3 documents"), "{}", step.action);
        assert_eq!(step.impact.risk, RiskLevel::Medium);
    }
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
