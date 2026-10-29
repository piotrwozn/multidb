use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use tower::ServiceExt;

use super::*;
use crate::db::{DbConfig, Profile, create_database};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[allow(clippy::needless_pass_by_value)]
fn request(method: Method, path: &str, token: Option<&str>, body: JsonValue) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header(CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        request = request.header(AUTHORIZATION, format!("Bearer {token}"));
    }
    request
        .body(Body::from(body.to_string()))
        .unwrap_or_else(|error| panic!("test request: {error}"))
}

async fn login(app: &Router, password: &str) -> Result<String, Box<dyn std::error::Error>> {
    let response = app
        .clone()
        .oneshot(request(
            Method::POST,
            "/auth/login",
            None,
            json!({"username":"admin","password":password}),
        ))
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body: JsonValue =
        serde_json::from_slice(&to_bytes(response.into_body(), 1_048_576).await?)?;
    Ok(body["data"]["token"]
        .as_str()
        .ok_or("missing token")?
        .to_owned())
}

async fn state() -> Result<AdminState, Box<dyn std::error::Error>> {
    let state = AdminState::from_database(create_database(DbConfig::new(Profile::InMemory))?);
    state
        .bootstrap_admin_password("old-secret".to_owned(), false)
        .await?;
    Ok(state)
}

#[tokio::test]
async fn password_rotation_revokes_all_sessions_and_preserves_static_bearer() -> TestResult {
    let state = state().await?.with_admin_token("operator-token");
    let app = router(state);
    let first = login(&app, "old-secret").await?;
    let second = login(&app, "old-secret").await?;
    let response = app
        .clone()
        .oneshot(request(
            Method::POST,
            "/auth/change-password",
            Some(&first),
            json!({"current_password":"old-secret","new_password":"new-secret"}),
        ))
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    for token in [first, second] {
        let mut req = request(Method::GET, "/auth/me", Some(&token), JsonValue::Null);
        req.headers_mut()
            .insert(HEADER_PRINCIPAL, HeaderValue::from_static("admin"));
        assert_eq!(
            app.clone().oneshot(req).await?.status(),
            StatusCode::UNAUTHORIZED
        );
    }
    let mut operator = request(
        Method::GET,
        "/auth/me",
        Some("operator-token"),
        JsonValue::Null,
    );
    operator
        .headers_mut()
        .insert(HEADER_PRINCIPAL, HeaderValue::from_static("admin"));
    assert_eq!(
        app.clone().oneshot(operator).await?.status(),
        StatusCode::OK
    );
    let token = login(&app, "new-secret").await?;
    assert_eq!(
        app.oneshot(request(
            Method::GET,
            "/auth/me",
            Some(&token),
            JsonValue::Null
        ))
        .await?
        .status(),
        StatusCode::OK
    );
    Ok(())
}

#[tokio::test]
async fn another_admin_state_rotation_invalidates_persisted_credential_bound_sessions() -> TestResult
{
    let state = state().await?;
    let other = AdminState::from_database_handle(Arc::clone(&state.database));
    let app = router(state);
    let token = login(&app, "old-secret").await?;
    other
        .bootstrap_admin_password("new-secret".to_owned(), true)
        .await?;
    assert_eq!(
        app.clone()
            .oneshot(request(
                Method::GET,
                "/auth/me",
                Some(&token),
                JsonValue::Null
            ))
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert!(
        login(&app, "new-secret")
            .await?
            .starts_with(SESSION_TOKEN_PREFIX)
    );
    Ok(())
}

#[tokio::test]
async fn delayed_verified_login_is_rejected_after_a_concurrent_password_rotation() -> TestResult {
    let state = state().await?;
    let verified = state
        .database
        .read()
        .await
        .admin_credential(ADMIN_USERNAME)?
        .ok_or("missing credential")?;
    // Simulate an Argon2 check that completed using the old record while another
    // request durably rotated it before this login can issue its session.
    state
        .bootstrap_admin_password("new-secret".to_owned(), true)
        .await?;
    let response = complete_admin_login(&state, verified).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(
        state
            .admin_sessions
            .sessions
            .lock()
            .map_err(|_| "session lock")?
            .is_empty()
    );
    Ok(())
}

#[test]
fn sessions_are_bounded_and_clear_releases_capacity() -> TestResult {
    let store = AdminSessionStore::default();
    let credential = AdminCredentialRecord {
        username: "admin".to_owned(),
        password_hash: "hash".to_owned(),
        updated_at_millis: 1,
    };
    for _ in 0..1_024 {
        store.issue(60, &credential)?;
    }
    assert!(store.issue(60, &credential).is_err());
    store.revoke_all();
    assert!(store.issue(60, &credential).is_ok());
    Ok(())
}
