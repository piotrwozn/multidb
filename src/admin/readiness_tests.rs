use super::*;
use crate::db::{AuditConfig, DbConfig, create_database};
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use std::{
    io::Write,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tower::ServiceExt;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn probe(
    app: &Router,
    path: &str,
) -> Result<(StatusCode, JsonValue), Box<dyn std::error::Error>> {
    let request = Request::builder().uri(path).body(Body::empty())?;
    let response =
        tokio::time::timeout(Duration::from_millis(100), app.clone().oneshot(request)).await??;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 4096).await?;
    Ok((status, serde_json::from_slice(&bytes)?))
}

#[tokio::test]
async fn audit_key_failure_withdraws_readiness_without_liveness_restart_or_query_lock() -> TestResult
{
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("audit.key");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(&path)?.write_all(&[1; 32])?;
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.apply_audit_config(AuditConfig {
        key_path: Some(path.clone()),
        ..AuditConfig::default()
    })?;
    let denied = Principal::new("readiness-probe-denied");
    assert!(matches!(
        database.audit_events_as(&denied),
        Err(DbError::AuthzDenied(_))
    ));
    let database = Arc::new(RwLock::new(database));
    let mut state = AdminState::from_database_handle(Arc::clone(&database));
    let gpu = Arc::new(AtomicBool::new(false));
    let snapshot = state
        .runtime_health
        .get()
        .ok_or("missing health snapshot")?
        .clone()
        .with_gpu_for_tests(Arc::clone(&gpu));
    state.runtime_health = Arc::new(OnceLock::from(snapshot));
    let app = router(state);
    assert_eq!(probe(&app, "/ready").await?.0, StatusCode::OK);
    std::fs::write(&path, [2; 32])?;
    assert!(matches!(
        database.read().await.audit_events_as(&denied),
        Err(DbError::AuditIntegrity(_))
    ));
    // A simultaneous process-local GPU stop must not restart a pod whose
    // persistent audit/key problem still requires an operator.
    gpu.store(true, Ordering::Release);
    let _locked = database.write().await;
    let (status, body) = probe(&app, "/ready").await?;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["reason"], "audit_failed");
    assert_eq!(body["recovery"], "operator_required");
    let (status, body) = probe(&app, "/health").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "alive");
    Ok(())
}

#[tokio::test]
async fn strict_gpu_process_stop_fails_both_probes_using_only_the_cached_atomic() -> TestResult {
    let database = Arc::new(RwLock::new(create_database(DbConfig::new(
        Profile::InMemory,
    ))?));
    let mut state = AdminState::from_database_handle(Arc::clone(&database));
    let gpu = Arc::new(AtomicBool::new(false));
    let snapshot = state
        .runtime_health
        .get()
        .ok_or("missing health snapshot")?
        .clone()
        .with_gpu_for_tests(Arc::clone(&gpu));
    state.runtime_health = Arc::new(OnceLock::from(snapshot));
    let app = router(state);
    assert_eq!(probe(&app, "/ready").await?.0, StatusCode::OK);
    gpu.store(true, Ordering::Release);
    let _locked = database.write().await;
    for path in ["/ready", "/health"] {
        let (status, body) = probe(&app, path).await?;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["reason"], "gpu_stopped");
        assert_eq!(body["recovery"], "restart_required");
    }
    Ok(())
}

#[tokio::test]
async fn health_handle_capture_never_waits_for_an_initially_busy_database() -> TestResult {
    let database = Arc::new(RwLock::new(create_database(DbConfig::new(
        Profile::InMemory,
    ))?));
    let locked = database.write().await;
    let state = AdminState::from_database_handle(Arc::clone(&database));
    let app = router(state);
    assert_eq!(
        probe(&app, "/ready").await?.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(probe(&app, "/health").await?.0, StatusCode::OK);
    drop(locked);
    assert_eq!(probe(&app, "/ready").await?.0, StatusCode::OK);
    Ok(())
}
