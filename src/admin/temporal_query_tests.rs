//! Authenticated HTTP temporal SQL follows the same dispatch as embedded SQL.
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use tower::ServiceExt;

use super::{
    AUTHORIZATION, AdminState, AuthzPolicy, FieldPath, HEADER_PRINCIPAL, JsonValue, ModelValue,
    Permission, PolicyConfig, Principal, PrincipalRegistry, Resource, Role, Router, StatusCode,
    TableSchema, json, router_with_api_prefix,
};
use crate::{
    db::{DbConfig, DbError, Profile, create_database},
    extension::{MaskingPolicy, RowPolicy},
    query::{ColumnDef, ColumnType},
    repl::{ReadConsistency, Replication},
    temporal::TemporalRetention,
    txn,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn fixture() -> TestResult<(AdminState, u64)> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let schema = TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("owner", ColumnType::Str, false),
            ColumnDef::new("payload", ColumnType::Str, false),
            ColumnDef::new("secret", ColumnType::Str, false),
        ],
        0,
    );
    let table = database.create_table("records", Some(schema.clone()), Vec::new())?;
    database.create_table("hidden", Some(schema), Vec::new())?;
    let row = |id, owner: &str, payload: &str| {
        vec![
            ModelValue::Int(id),
            ModelValue::Str(owner.to_owned()),
            ModelValue::Str(payload.to_owned()),
            ModelValue::Str("private".to_owned()),
        ]
    };
    table.insert(row(1, "alice", "old"))?;
    table.insert(row(2, "bob", "hidden"))?;
    let lsn = txn::decode_txn_id(
        &database
            .read(
                txn::TXN_META_TABLE,
                txn::CURRENT_TXN_ID_KEY,
                ReadConsistency::Strong,
            )?
            .ok_or("current LSN")?,
    )?;
    table.update(row(1, "alice", "new"))?;
    table.insert(row(3, "alice", "later"))?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("reader").grant(Resource::Table("records".to_owned()), Permission::Read)
    ]));
    database.set_principal_registry(
        PrincipalRegistry::new()
            .with_principal("reader", Principal::new("alice").with_role("reader")),
    );
    database.set_policy_config(&PolicyConfig {
        row_policies: vec![RowPolicy {
            resource: Resource::Table("records".to_owned()),
            path: FieldPath::new(["owner"]),
            equals: ModelValue::Str("alice".to_owned()),
        }],
        masking: vec![MaskingPolicy {
            resource: Resource::Table("records".to_owned()),
            paths: vec![FieldPath::new(["secret"])],
            replacement: ModelValue::Str("***".to_owned()),
        }],
        ..PolicyConfig::default()
    })?;
    Ok((
        AdminState::from_database(database).with_admin_token("gateway"),
        lsn,
    ))
}

async fn query(
    app: &Router,
    path: &str,
    principal: &str,
    sql: &str,
) -> TestResult<(StatusCode, JsonValue)> {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header(AUTHORIZATION, "Bearer gateway")
                .header(HEADER_PRINCIPAL, principal)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&json!({"sql": sql}))?))?,
        )
        .await?;
    let status = response.status();
    let body = serde_json::from_slice(&to_bytes(response.into_body(), 1_048_576).await?)?;
    Ok((status, body))
}

#[tokio::test]
async fn http_as_of_suffix_preserves_authorization_policies_and_retention() -> TestResult {
    let (state, lsn) = fixture()?;
    let app = router_with_api_prefix(state.clone());
    for path in ["/sql", "/api/sql"] {
        let sql = format!("SELECT id,payload,secret FROM records ORDER BY id AS OF LSN {lsn};");
        let (status, body) = query(&app, path, "reader", &sql).await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["data"]["output"]["rows"], json!([[1, "old", "***"]]));
        let (status, body) =
            query(&app, path, "reader", "SELECT id FROM records AS OF LSN 0").await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["data"]["output"]["rows"], json!([]));
        let (status, body) = query(&app, path, "unknown", &sql).await?;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        let nested =
            format!("SELECT (SELECT secret FROM hidden LIMIT 1) FROM records AS OF LSN {lsn}");
        let (status, body) = query(&app, path, "reader", &nested).await?;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    }
    let future_ms = std::time::UNIX_EPOCH.elapsed()?.as_millis() + 1_000;
    let (status, body) = query(
        &app,
        "/sql",
        "reader",
        &format!("SELECT id,payload,secret FROM records ORDER BY id AS OF TIMESTAMP {future_ms}"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["data"]["output"]["rows"],
        json!([[1, "new", "***"], [3, "later", "***"]])
    );
    {
        let database = state.database.read().await;
        let reader = database.principal_for_user("reader");
        let sql = format!("SELECT id,payload,secret FROM records AS OF LSN {lsn}");
        let schema = database
            .describe_query_as(&reader, &sql)
            .await?
            .ok_or("Describe schema")?;
        assert_eq!(schema.fields().len(), 3);
        assert!(matches!(
            database
                .describe_query_as(&Principal::new("unknown"), &sql)
                .await,
            Err(DbError::AuthzDenied(_))
        ));
    }
    state.database.write().await.enable_system_versioning(
        "records",
        TemporalRetention {
            min_lsn: lsn + 1,
            keep_history: true,
        },
    )?;
    let (status, body) =
        query(&app, "/sql", "reader", "SELECT id FROM records AS OF LSN 0").await?;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .ok_or("error message")?
            .contains("retention expired"),
        "{body}"
    );
    Ok(())
}
