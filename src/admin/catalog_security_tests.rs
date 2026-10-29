use super::*;
use crate::{
    db::{DbConfig, create_database},
    extension::RowPolicy,
    query::{ColumnDef, ColumnType},
};
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use tower::ServiceExt;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn fixture() -> Result<AdminState, Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    for name in ["visible", "hidden"] {
        let table = database.create_table(
            name,
            Some(TableSchema::new(
                vec![
                    ColumnDef::new("id", ColumnType::Int, false),
                    ColumnDef::new("owner", ColumnType::Str, false),
                ],
                0,
            )),
            Vec::new(),
        )?;
        table.insert(vec![
            ModelValue::Int(1),
            ModelValue::Str("alice".to_owned()),
        ])?;
        table.insert(vec![ModelValue::Int(2), ModelValue::Str("bob".to_owned())])?;
    }
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("reader").grant(Resource::Table("visible".to_owned()), Permission::Read),
        Role::new("administrator").grant(Resource::Database, Permission::Read),
    ]));
    database.set_principal_registry(
        PrincipalRegistry::new()
            .with_principal("alice", Principal::new("alice").with_role("reader"))
            .with_principal("root", Principal::new("root").with_role("administrator")),
    );
    Ok(AdminState::from_database(database).with_admin_token("gateway"))
}

async fn catalog(
    app: Router,
    principal: Option<&str>,
) -> Result<JsonValue, Box<dyn std::error::Error>> {
    let mut request = Request::builder()
        .uri("/catalog")
        .header(AUTHORIZATION, "Bearer gateway");
    if let Some(principal) = principal {
        request = request.header(HEADER_PRINCIPAL, principal);
    }
    let response = app.oneshot(request.body(Body::empty())?).await?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(serde_json::from_slice(
        &to_bytes(response.into_body(), 1_048_576).await?,
    )?)
}

#[tokio::test]
async fn catalog_checks_each_resource_before_exposing_schema_or_counts() -> TestResult {
    let app = router(fixture()?);
    for principal in [None, Some("unknown")] {
        assert_eq!(
            catalog(app.clone(), principal).await?["data"]["objects"],
            json!([])
        );
    }
    let scoped = catalog(app.clone(), Some("alice")).await?;
    assert_eq!(
        scoped["data"]["objects"].as_array().ok_or("objects")?.len(),
        1
    );
    assert_eq!(scoped["data"]["objects"][0]["name"], "visible");
    assert_eq!(scoped["data"]["objects"][0]["row_count"], 2);
    assert!(!scoped.to_string().contains("hidden"));
    assert_eq!(
        catalog(app, Some("root")).await?["data"]["objects"]
            .as_array()
            .ok_or("objects")?
            .len(),
        2
    );
    Ok(())
}

#[tokio::test]
async fn catalog_never_exposes_unfiltered_counts_for_policy_protected_objects() -> TestResult {
    let state = fixture()?;
    state
        .database
        .read()
        .await
        .set_policy_config(&PolicyConfig {
            row_policies: vec![RowPolicy {
                resource: Resource::Table("visible".to_owned()),
                path: FieldPath::new(["owner"]),
                equals: ModelValue::Str("alice".to_owned()),
            }],
            ..PolicyConfig::default()
        })?;
    let response = catalog(router(state), Some("alice")).await?;
    assert_eq!(response["data"]["objects"][0]["row_count"], JsonValue::Null);
    assert!(response["data"]["objects"][0]["schema"].is_object());
    Ok(())
}

#[tokio::test]
async fn ambiguous_proxy_identity_and_authorization_headers_never_choose_a_privileged_value()
-> TestResult {
    let app = router(fixture()?);
    for identities in [["alice", "root"], ["root", "alice"]] {
        let mut request = Request::builder()
            .uri("/catalog")
            .header(AUTHORIZATION, "Bearer gateway")
            .body(Body::empty())?;
        for identity in identities {
            request
                .headers_mut()
                .append(HEADER_PRINCIPAL, HeaderValue::from_str(identity)?);
        }
        let response = app.clone().oneshot(request).await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body: JsonValue =
            serde_json::from_slice(&to_bytes(response.into_body(), 1_048_576).await?)?;
        assert_eq!(body["data"]["objects"], json!([]));
    }
    for tokens in [
        ["Bearer gateway", "Bearer wrong"],
        ["Bearer wrong", "Bearer gateway"],
    ] {
        let mut request = Request::builder()
            .uri("/catalog")
            .header(HEADER_PRINCIPAL, "root")
            .body(Body::empty())?;
        for token in tokens {
            request
                .headers_mut()
                .append(AUTHORIZATION, HeaderValue::from_str(token)?);
        }
        assert_eq!(
            app.clone().oneshot(request).await?.status(),
            StatusCode::UNAUTHORIZED
        );
    }
    Ok(())
}
