use super::*;
use crate::{
    db::{DbConfig, create_database},
    extension::{MaskingPolicy, PolicyConfig, RowPolicy},
    model::{FieldPath, Value},
    query::{ColumnDef, ColumnType, RelIndexSpec, RelTable, SqlOutput, TableSchema},
    security::{AuditEvent, AuditOutcome, AuthzPolicy, Permission, Principal, Resource, Role},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn schema(value_column: &str) -> TableSchema {
    TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("category", ColumnType::Int, false),
            ColumnDef::new(value_column, ColumnType::Str, false),
        ],
        0,
    )
}

fn database() -> Result<Database, Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let table = database.create_table(
        "items",
        Some(schema("payload")),
        vec![RelIndexSpec::new(1, 1)],
    )?;
    table.insert(vec![
        Value::Int(1),
        Value::Int(7),
        Value::Str("alpha".to_owned()),
    ])?;
    table.insert(vec![
        Value::Int(2),
        Value::Int(9),
        Value::Str("beta".to_owned()),
    ])?;
    Ok(database)
}

fn rows(output: SqlOutput) -> Vec<Vec<Value>> {
    let SqlOutput::Rows(rows) = output else {
        panic!("expected query rows")
    };
    rows.rows
}

#[tokio::test]
async fn catalog_templates_reuse_plans_across_queries_and_audit_commits() -> TestResult {
    let database = database()?;
    let first = database.sql_engine()?;
    assert_eq!(
        rows(
            database
                .query("SELECT id FROM items WHERE category=7")
                .await?
        ),
        vec![vec![Value::Int(1)]]
    );
    let initial_hits = database
        .plan_cache
        .lock()
        .map_err(|_| "plan cache poisoned")?
        .metrics()
        .hits;
    assert_eq!(
        rows(
            database
                .query("SELECT id FROM items WHERE category=9")
                .await?
        ),
        vec![vec![Value::Int(2)]]
    );
    assert!(
        database
            .plan_cache
            .lock()
            .map_err(|_| "plan cache poisoned")?
            .metrics()
            .hits
            > initial_hits
    );
    database.record_audit(&AuditEvent::new(
        None,
        "cache_test",
        Resource::Database,
        AuditOutcome::Succeeded,
        None,
    ))?;
    assert!(Arc::ptr_eq(&first, &database.sql_engine()?));
    Ok(())
}

#[tokio::test]
async fn warm_catalog_never_caches_masks_row_policies_or_grants() -> TestResult {
    let mut database = database()?;
    let resource = Resource::Table("items".to_owned());
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("reader").grant(resource.clone(), Permission::Read)
    ]));
    let principal = Principal::new("reader").with_role("reader");
    let template = database.sql_engine()?;
    let sql = "SELECT id,payload FROM items WHERE category=7";
    assert_eq!(
        rows(database.query_as(&principal, sql).await?)[0][1],
        Value::Str("alpha".to_owned())
    );
    let mut policies = PolicyConfig {
        masking: vec![MaskingPolicy {
            resource: resource.clone(),
            paths: vec![FieldPath::new(["payload"])],
            replacement: Value::Str("***".to_owned()),
        }],
        ..PolicyConfig::default()
    };
    database.set_policy_config(&policies)?;
    assert_eq!(
        rows(database.query_as(&principal, sql).await?)[0][1],
        Value::Str("***".to_owned())
    );
    policies.row_policies.push(RowPolicy {
        resource,
        path: FieldPath::new(["category"]),
        equals: Value::Int(9),
    });
    database.set_policy_config(&policies)?;
    assert_eq!(
        rows(database.query_as(&principal, sql).await?),
        Vec::<Vec<Value>>::new()
    );
    database.set_policy_config(&PolicyConfig::default())?;
    assert_eq!(
        rows(database.query_as(&principal, sql).await?)[0][1],
        Value::Str("alpha".to_owned())
    );
    assert!(Arc::ptr_eq(&template, &database.sql_engine()?));
    database.set_authz_policy(AuthzPolicy::default());
    assert!(database.query_as(&principal, sql).await.is_err());
    Ok(())
}

#[tokio::test]
async fn schema_writes_through_existing_handles_invalidate_catalog_templates() -> TestResult {
    let mut database = database()?;
    database.create_table("dynamic_items", None, Vec::new())?;
    let first = database.sql_engine()?;
    let table = RelTable::create_with_layout(
        database.replication_handle(),
        "dynamic_items",
        Some(schema("payload")),
        Vec::new(),
        crate::query::TableLayout::Row,
    )?;
    table.insert(vec![
        Value::Int(3),
        Value::Int(7),
        Value::Str("alpha".to_owned()),
    ])?;
    let next = database.sql_engine()?;
    assert!(!Arc::ptr_eq(&first, &next));
    assert_eq!(
        rows(
            database
                .query("SELECT payload FROM dynamic_items WHERE id=3")
                .await?
        ),
        vec![vec![Value::Str("alpha".to_owned())]]
    );
    Ok(())
}

#[tokio::test]
async fn catalog_and_governor_changes_refresh_performance_templates() -> TestResult {
    let mut database = database()?;
    let first = database.sql_engine()?;
    database.create_table("other_items", Some(schema("payload")), Vec::new())?;
    let next = database.sql_engine()?;
    assert!(!Arc::ptr_eq(&first, &next));
    database.set_query_governor(1, 20)?;
    let governed = database.sql_engine()?;
    assert!(!Arc::ptr_eq(&next, &governed));
    // Internal configuration changes must not reuse an outdated SQL byte limit either.
    database.performance.query.max_sql_bytes = 8;
    assert!(
        database
            .query("SELECT id FROM items WHERE id=1")
            .await
            .is_err()
    );
    Ok(())
}
