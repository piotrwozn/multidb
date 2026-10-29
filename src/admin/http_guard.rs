//! Authenticate before JSON extraction and bound slow request bodies.

use std::{sync::Arc, time::Duration};

use axum::{
    body::{Body, to_bytes},
    extract::{Request, State},
    middleware::Next,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::{
    AdminState, MAX_CONTROL_PLANE_BODY_BYTES, Response, StatusCode, api_error, require_admin_token,
};

const MAX_ACTIVE_REQUESTS: usize = 128;
const BODY_TIMEOUT: Duration = Duration::from_secs(30);

tokio::task_local! {
    static REQUEST_PERMIT: Arc<OwnedSemaphorePermit>;
}

pub(super) fn processing_permit() -> Option<Arc<OwnedSemaphorePermit>> {
    REQUEST_PERMIT.try_with(Arc::clone).ok()
}

pub(super) struct Guard {
    permits: Arc<Semaphore>,
    body_timeout: Duration,
}

impl Default for Guard {
    fn default() -> Self {
        Self {
            permits: Arc::new(Semaphore::new(MAX_ACTIVE_REQUESTS)),
            body_timeout: BODY_TIMEOUT,
        }
    }
}

fn public(path: &str) -> bool {
    matches!(
        path.strip_prefix("/api").unwrap_or(path),
        "/health" | "/ready" | "/openapi.json" | "/auth/login"
    )
}

pub(super) async fn protect(
    State(state): State<AdminState>,
    request: Request,
    next: Next,
) -> Response {
    let public = public(request.uri().path());
    if public && matches!(*request.method(), super::Method::GET | super::Method::HEAD) {
        return next.run(request).await;
    }
    let Ok(permit) = Arc::clone(&state.http_guard.permits).try_acquire_owned() else {
        let mut response = api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "busy",
            "control plane request capacity exhausted",
        );
        response
            .headers_mut()
            .insert(super::RETRY_AFTER, super::HeaderValue::from_static("1"));
        return response;
    };
    // Authentication may wait for a database guard (for credential rotation),
    // so it must also hold a bounded admission slot. No body is polled yet.
    if !public && let Some(response) = require_admin_token(&state, request.headers()).await {
        return response;
    }
    let permit = Arc::new(permit);
    // GET/HEAD routes never extract a JSON body. Dropping it also keeps public
    // health probes independent of a maliciously unfinished request stream.
    if matches!(*request.method(), super::Method::GET | super::Method::HEAD) {
        return REQUEST_PERMIT.scope(permit, next.run(request)).await;
    }
    let (parts, body) = request.into_parts();
    let bytes = match tokio::time::timeout(
        state.http_guard.body_timeout,
        to_bytes(body, MAX_CONTROL_PLANE_BODY_BYTES),
    )
    .await
    {
        Err(_) => {
            return api_error(
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "request body deadline exceeded",
            );
        }
        Ok(Err(_)) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_json",
                "invalid or oversized request body",
            );
        }
        Ok(Ok(bytes)) => bytes,
    };
    REQUEST_PERMIT
        .scope(
            permit,
            next.run(Request::from_parts(parts, Body::from(bytes))),
        )
        .await
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use axum::{body::to_bytes, http::Request};
    use futures::stream;
    use tower::ServiceExt;

    use super::*;
    use crate::db::{DbConfig, Profile, create_database};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn state() -> Result<AdminState, Box<dyn std::error::Error>> {
        Ok(
            AdminState::from_database(create_database(DbConfig::new(Profile::InMemory))?)
                .with_admin_token("token"),
        )
    }

    #[tokio::test]
    async fn rejected_token_never_polls_the_request_body_at_either_api_mount() -> TestResult {
        let app = super::super::router_with_api_prefix(state()?);
        for path in ["/sql", "/api/sql"] {
            let polls = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&polls);
            let body = Body::from_stream(stream::poll_fn(move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                std::task::Poll::Ready(Some(Ok::<_, Infallible>(bytes::Bytes::from_static(
                    b"not JSON",
                ))))
            }));
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(path)
                        .header("content-type", "application/json")
                        .header("authorization", "Bearer wrong")
                        .body(body)?,
                )
                .await?;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert_eq!(polls.load(Ordering::SeqCst), 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn authenticated_slow_body_times_out_and_capacity_recovers() -> TestResult {
        let mut state = state()?;
        state.http_guard = Arc::new(Guard {
            permits: Arc::new(Semaphore::new(1)),
            body_timeout: Duration::from_millis(5),
        });
        let app = super::super::router(state);
        let request = Request::builder()
            .method("POST")
            .uri("/sql")
            .header("content-type", "application/json")
            .header("authorization", "Bearer token")
            .body(Body::from_stream(stream::pending::<
                Result<bytes::Bytes, Infallible>,
            >()))?;
        let response = app.clone().oneshot(request).await?;
        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1024).await?)?;
        assert_eq!(body["error"]["code"], "request_timeout");
        let response = app
            .oneshot(Request::builder().uri("/health").body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        Ok(())
    }

    #[tokio::test]
    async fn cancelling_a_request_does_not_release_capacity_while_blocking_work_continues()
    -> TestResult {
        let mut state = state()?;
        let permits = Arc::new(Semaphore::new(1));
        state.http_guard = Arc::new(Guard {
            permits: Arc::clone(&permits),
            body_timeout: BODY_TIMEOUT,
        });
        let (entered, ready) = tokio::sync::oneshot::channel();
        let entered = Arc::new(std::sync::Mutex::new(Some(entered)));
        let (release, gate) = std::sync::mpsc::channel();
        let gate = Arc::new(std::sync::Mutex::new(gate));
        let app = axum::Router::new()
            .route(
                "/work",
                axum::routing::post(move || {
                    let entered = Arc::clone(&entered);
                    let gate = Arc::clone(&gate);
                    async move {
                        super::super::run_blocking(move || {
                            if let Ok(mut entered) = entered.lock()
                                && let Some(entered) = entered.take()
                            {
                                let _ = entered.send(());
                            }
                            if let Ok(gate) = gate.lock() {
                                let _ = gate.recv();
                            }
                            super::super::api_ok(StatusCode::OK, serde_json::json!({}))
                        })
                        .await
                    }
                }),
            )
            .layer(axum::middleware::from_fn_with_state(state, protect));
        let request = || {
            Request::builder()
                .method("POST")
                .uri("/work")
                .header("authorization", "Bearer token")
                .body(Body::empty())
        };
        let pending = tokio::spawn(app.clone().oneshot(request()?));
        tokio::time::timeout(Duration::from_secs(2), ready).await??;
        pending.abort();
        assert!(pending.await.is_err());
        assert_eq!(
            permits.available_permits(),
            0,
            "abandoned worker must remain counted"
        );
        let response = app.oneshot(request()?).await?;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["retry-after"], "1");
        release.send(())?;
        tokio::time::timeout(Duration::from_secs(2), async {
            while permits.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(())
    }
}
