#![allow(clippy::missing_errors_doc)]

use std::{
    collections::BTreeMap,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex},
    time::{SystemTime, UNIX_EPOCH},
};

use argon2::{
    Algorithm, Argon2, Params, PasswordHash, PasswordHasher, PasswordVerifier, Version,
    password_hash::SaltString,
};
use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, OriginalUri, Path as AxumPath, Query as AxumQuery, State,
        rejection::JsonRejection,
    },
    http::{
        HeaderMap, Method, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE},
    },
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::Mutex;

use crate::{
    compat::SERVER_VERSION,
    config_spec::{
        ApplyCheckReport, ApplyStatus, DatabaseSpec, GuaranteeValidator, MigrationPlan,
        MigrationPlanner, built_in_profiles, collection_role_definitions,
        consistency_domain_definition, consistency_domain_definitions, extension_catalog_entries,
    },
    db::{AdminCredentialRecord, CatalogEntry, Database, DbError, Profile, ReplicationKind},
    geo::GeoIndexConfig,
    graph::GraphId,
    model::{CollectionId, DocumentId, FieldPath, IndexId, IndexSpec, Value as ModelValue},
    observability,
    query::{DocField, RelIndexExpression, RelIndexSpec, Row, SqlOutput, TableLayout, TableSchema},
    runtime_advisor::{RuntimeAdviceDecisionRequest, RuntimeAdvicePlanRequest},
    security::{
        AuditEvent, AuditOutcome, AuthzPolicy, Permission, Principal, PrincipalRegistry, Resource,
        Role,
    },
    text::FullTextIndexConfig,
    timeseries::{TimePoint, TimeSeriesConfig},
    vector::{HnswParams, VectorCollectionConfig, VectorHit, VectorMetric},
};

const MAX_CONTROL_PLANE_BODY_BYTES: usize = 1024 * 1024;
const HEADER_PRINCIPAL: &str = "x-multidb-principal";
const DEFAULT_DATA_PAGE_LIMIT: usize = 100;
const MAX_DATA_PAGE_LIMIT: usize = 1_000;
const ADMIN_USERNAME: &str = "admin";
const ADMIN_ROLE: &str = "admin";
const SESSION_TOKEN_PREFIX: &str = "mda1_session_";
const CONTROL_PLANE_OPENAPI_JSON: &str =
    include_str!("../docs/openapi/control-plane-v1.openapi.json");
pub const DEFAULT_ADMIN_SESSION_TTL_SECONDS: u64 = 8 * 60 * 60;
pub const MIN_ADMIN_SESSION_TTL_SECONDS: u64 = 60;
pub const MAX_ADMIN_SESSION_TTL_SECONDS: u64 = 24 * 60 * 60;
pub const DEFAULT_ADMIN_LOGIN_MAX_FAILURES: u32 = 5;
pub const DEFAULT_ADMIN_LOGIN_WINDOW_SECONDS: u64 = 5 * 60;
pub const DEFAULT_ADMIN_LOGIN_LOCKOUT_SECONDS: u64 = 5 * 60;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AdminLoginRateLimitConfig {
    pub max_failures: u32,
    pub window_seconds: u64,
    pub lockout_seconds: u64,
}

impl AdminLoginRateLimitConfig {
    #[must_use]
    pub fn new(max_failures: u32, window_seconds: u64, lockout_seconds: u64) -> Self {
        Self {
            max_failures: max_failures.max(1),
            window_seconds: window_seconds.max(1),
            lockout_seconds: lockout_seconds.max(1),
        }
    }
}

impl Default for AdminLoginRateLimitConfig {
    fn default() -> Self {
        Self::new(
            DEFAULT_ADMIN_LOGIN_MAX_FAILURES,
            DEFAULT_ADMIN_LOGIN_WINDOW_SECONDS,
            DEFAULT_ADMIN_LOGIN_LOCKOUT_SECONDS,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AdminStatus {
    pub server_version: String,
    pub uptime_millis: u64,
    pub profile: Profile,
    pub replication: ReplicationKind,
    pub layout: TableLayout,
    pub engine: String,
    pub catalog_objects: usize,
    pub shard_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub status: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ConfigPlanRequest {
    pub current: DatabaseSpec,
    pub desired: DatabaseSpec,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ConfigApplyRequest {
    pub plan: MigrationPlan,
    pub confirm: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct StudioManifest {
    pub api_version: u32,
    pub openapi_endpoint: &'static str,
    pub physical_migration_supported: bool,
    pub config_apply_data_mutated: bool,
    pub endpoints: Vec<String>,
    pub operations: Vec<ControlPlaneOperation>,
    pub capabilities: Vec<&'static str>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ControlPlaneOperation {
    pub method: &'static str,
    pub path: &'static str,
    pub operation_id: &'static str,
    pub auth_required: bool,
    pub stability: &'static str,
}

impl ControlPlaneOperation {
    fn endpoint(self) -> String {
        format!("{} {}", self.method, self.path)
    }
}

const CONTROL_PLANE_OPERATIONS: &[ControlPlaneOperation] = &[
    ControlPlaneOperation {
        method: "GET",
        path: "/openapi.json",
        operation_id: "getOpenApi",
        auth_required: false,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "GET",
        path: "/health",
        operation_id: "getHealth",
        auth_required: false,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "GET",
        path: "/ready",
        operation_id: "getReady",
        auth_required: false,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/auth/login",
        operation_id: "login",
        auth_required: false,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/auth/logout",
        operation_id: "logout",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/auth/change-password",
        operation_id: "changePassword",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "GET",
        path: "/auth/me",
        operation_id: "getAuthMe",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "GET",
        path: "/status",
        operation_id: "getStatus",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "GET",
        path: "/metrics",
        operation_id: "getMetrics",
        auth_required: true,
        stability: "preview",
    },
    ControlPlaneOperation {
        method: "GET",
        path: "/catalog",
        operation_id: "getCatalog",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/sql",
        operation_id: "executeSql",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "GET",
        path: "/data/tables/{name}/rows",
        operation_id: "listTableRows",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/data/tables/{name}/rows",
        operation_id: "insertTableRow",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "PUT",
        path: "/data/tables/{name}/rows",
        operation_id: "updateTableRow",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "DELETE",
        path: "/data/tables/{name}/rows",
        operation_id: "deleteTableRow",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "GET",
        path: "/data/collections/{name}/documents",
        operation_id: "listDocuments",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/data/collections/{name}/documents",
        operation_id: "createDocument",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "PUT",
        path: "/data/collections/{name}/documents/{id}",
        operation_id: "updateDocument",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "DELETE",
        path: "/data/collections/{name}/documents/{id}",
        operation_id: "deleteDocument",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/data/vectors/{name}/vectors",
        operation_id: "insertVector",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/data/vectors/{name}/search",
        operation_id: "searchVector",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "GET",
        path: "/data/time-series/{name}/points",
        operation_id: "listTimeSeriesPoints",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/data/time-series/{name}/points",
        operation_id: "insertTimeSeriesPoint",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/builder/table",
        operation_id: "createTable",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/builder/collection",
        operation_id: "createCollection",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/builder/vector",
        operation_id: "createVector",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/builder/time-series",
        operation_id: "createTimeSeries",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/builder/full-text",
        operation_id: "createFullText",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/builder/geo",
        operation_id: "createGeoIndex",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/builder/graph",
        operation_id: "createGraph",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "GET",
        path: "/security",
        operation_id: "getSecurity",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/security",
        operation_id: "updateSecurity",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "GET",
        path: "/audit",
        operation_id: "getAudit",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "GET",
        path: "/config",
        operation_id: "getConfig",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/config/validate",
        operation_id: "validateConfig",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/config/plan",
        operation_id: "planConfig",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/config/apply",
        operation_id: "applyConfig",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "GET",
        path: "/profiles",
        operation_id: "getProfiles",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "GET",
        path: "/roles",
        operation_id: "getRoles",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "GET",
        path: "/domains",
        operation_id: "getDomains",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "GET",
        path: "/extensions",
        operation_id: "getExtensions",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "GET",
        path: "/advice",
        operation_id: "getAdvice",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/advice/plan",
        operation_id: "planAdvice",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/advice/decision",
        operation_id: "recordAdviceDecision",
        auth_required: true,
        stability: "stable",
    },
    ControlPlaneOperation {
        method: "GET",
        path: "/studio",
        operation_id: "getStudioManifest",
        auth_required: true,
        stability: "stable",
    },
];

#[must_use]
pub const fn control_plane_operations() -> &'static [ControlPlaneOperation] {
    CONTROL_PLANE_OPERATIONS
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AuthMeResponse {
    pub principal: String,
    pub roles: Vec<String>,
    pub system_admin: bool,
    pub database_admin: bool,
    pub insecure_local_admin: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct LoginResponse {
    pub token: String,
    pub expires_at: String,
    pub expires_at_millis: u64,
    pub principal: String,
    pub roles: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct ChangePasswordRequest {
    pub current_password: String,
    pub new_password: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionRecord {
    pub id: String,
    pub principal: String,
    pub roles: Vec<String>,
    pub expires_at_millis: u64,
    pub revoked: bool,
}

#[derive(Debug, Default)]
struct AdminSessionStore {
    sessions: StdMutex<BTreeMap<String, SessionRecord>>,
}

#[derive(Debug)]
struct AdminLoginRateLimiter {
    config: AdminLoginRateLimitConfig,
    buckets: StdMutex<BTreeMap<String, AdminLoginRateLimitBucket>>,
}

#[derive(Clone, Debug, Default)]
struct AdminLoginRateLimitBucket {
    failures: u32,
    window_started_millis: u64,
    locked_until_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SessionValidation {
    Valid(SessionRecord),
    Expired,
    Invalid,
}

impl AdminSessionStore {
    fn issue(&self, ttl_seconds: u64) -> Result<(String, SessionRecord), String> {
        let token = new_session_token()?;
        let id = session_token_hash(&token);
        let expires_at_millis = now_millis().saturating_add(ttl_seconds.saturating_mul(1_000));
        let record = SessionRecord {
            id: id.clone(),
            principal: ADMIN_USERNAME.to_owned(),
            roles: vec![ADMIN_ROLE.to_owned()],
            expires_at_millis,
            revoked: false,
        };
        self.sessions
            .lock()
            .map_err(|_| "session store lock poisoned".to_owned())?
            .insert(id, record.clone());
        Ok((token, record))
    }

    fn validate(&self, token: &str) -> SessionValidation {
        let id = session_token_hash(token);
        let now = now_millis();
        let Ok(mut sessions) = self.sessions.lock() else {
            return SessionValidation::Invalid;
        };
        let Some(record) = sessions.get(&id).cloned() else {
            return SessionValidation::Invalid;
        };
        if record.revoked {
            sessions.remove(&id);
            return SessionValidation::Invalid;
        }
        if record.expires_at_millis <= now {
            sessions.remove(&id);
            return SessionValidation::Expired;
        }
        SessionValidation::Valid(record)
    }

    fn revoke(&self, token: &str) -> Option<SessionRecord> {
        let id = session_token_hash(token);
        self.sessions.lock().ok()?.remove(&id)
    }
}

impl AdminLoginRateLimiter {
    fn new(config: AdminLoginRateLimitConfig) -> Self {
        Self {
            config,
            buckets: StdMutex::new(BTreeMap::new()),
        }
    }

    fn is_limited(&self, username: &str) -> bool {
        let now = now_millis();
        let user_key = login_username_bucket(username);
        let Ok(mut buckets) = self.buckets.lock() else {
            return true;
        };
        self.bucket_locked(&mut buckets, "login", now)
            || self.bucket_locked(&mut buckets, &user_key, now)
    }

    fn record_failure(&self, username: &str) {
        let now = now_millis();
        let user_key = login_username_bucket(username);
        if let Ok(mut buckets) = self.buckets.lock() {
            self.record_bucket_failure(&mut buckets, "login", now);
            self.record_bucket_failure(&mut buckets, &user_key, now);
        }
    }

    fn record_success(&self, username: &str) {
        let user_key = login_username_bucket(username);
        if let Ok(mut buckets) = self.buckets.lock() {
            buckets.remove("login");
            buckets.remove(&user_key);
        }
    }

    fn bucket_locked(
        &self,
        buckets: &mut BTreeMap<String, AdminLoginRateLimitBucket>,
        key: &str,
        now: u64,
    ) -> bool {
        let Some(bucket) = buckets.get_mut(key) else {
            return false;
        };
        bucket.normalize(now, self.config.window_seconds);
        if bucket.is_empty() {
            buckets.remove(key);
            return false;
        }
        bucket.locked_until_millis > now
    }

    fn record_bucket_failure(
        &self,
        buckets: &mut BTreeMap<String, AdminLoginRateLimitBucket>,
        key: &str,
        now: u64,
    ) {
        let bucket = buckets.entry(key.to_owned()).or_default();
        bucket.normalize(now, self.config.window_seconds);
        if bucket.locked_until_millis > now {
            return;
        }
        if bucket.window_started_millis == 0 {
            bucket.window_started_millis = now;
        }
        bucket.failures = bucket.failures.saturating_add(1);
        if bucket.failures >= self.config.max_failures {
            bucket.locked_until_millis =
                now.saturating_add(seconds_to_millis(self.config.lockout_seconds));
        }
    }
}

impl AdminLoginRateLimitBucket {
    fn normalize(&mut self, now: u64, window_seconds: u64) {
        if self.locked_until_millis != 0 && self.locked_until_millis <= now {
            *self = Self::default();
            return;
        }
        let window_millis = seconds_to_millis(window_seconds);
        if self.locked_until_millis == 0
            && self.window_started_millis != 0
            && now.saturating_sub(self.window_started_millis) > window_millis
        {
            *self = Self::default();
        }
    }

    fn is_empty(&self) -> bool {
        self.failures == 0 && self.window_started_millis == 0 && self.locked_until_millis == 0
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct CatalogObjectSummary {
    pub name: String,
    pub kind: String,
    pub entry: CatalogEntry,
    pub schema: Option<TableSchema>,
    pub row_count: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct CatalogResponse {
    pub objects: Vec<CatalogObjectSummary>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SqlRequest {
    pub sql: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct SqlResponse {
    pub output: JsonValue,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct TableRowsResponse {
    pub table: String,
    pub schema: Option<TableSchema>,
    pub rows: Vec<Vec<JsonValue>>,
    pub offset: usize,
    pub limit: usize,
    pub returned: usize,
    pub has_more: bool,
    pub next_offset: Option<usize>,
    pub capped: bool,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct TableRowRequest {
    pub row: Vec<JsonValue>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct TableRowDeleteRequest {
    pub primary_key: JsonValue,
    pub confirm: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct DocumentListResponse {
    pub collection: String,
    pub documents: Vec<DocumentSummary>,
    pub offset: usize,
    pub limit: usize,
    pub returned: usize,
    pub has_more: bool,
    pub next_offset: Option<usize>,
    pub capped: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct DataPageQuery {
    pub offset: Option<usize>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct DocumentSummary {
    pub id: String,
    pub document: JsonValue,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct DocumentCreateRequest {
    pub document: JsonValue,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct DocumentCreateResponse {
    pub id: String,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct DocumentUpdateRequest {
    pub document: JsonValue,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ConfirmRequest {
    pub confirm: String,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CreateTableRequest {
    pub name: String,
    pub schema: Option<TableSchema>,
    #[serde(default)]
    pub indexes: Vec<CreateRelIndexSpec>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CreateRelIndexSpec {
    pub id: u32,
    pub column: usize,
    pub expression: RelIndexExpression,
    #[serde(default)]
    pub include: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CreateCollectionRequest {
    pub name: String,
    pub collection_id: Option<u32>,
    #[serde(default)]
    pub fields: Vec<DocField>,
    #[serde(default)]
    pub indexes: Vec<CreateDocumentIndexSpec>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CreateDocumentIndexSpec {
    pub id: u32,
    pub path: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CreateVectorRequest {
    pub name: String,
    pub collection_id: Option<u32>,
    pub dim: usize,
    #[serde(default)]
    pub metric: VectorMetric,
    #[serde(default)]
    pub hnsw: HnswParams,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CreateTimeSeriesRequest {
    pub name: String,
    pub chunk_millis: i64,
    pub retention_millis: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CreateFullTextRequest {
    pub name: String,
    pub collection_id: u32,
    pub path: Vec<String>,
    #[serde(default = "default_text_language")]
    pub language: String,
    #[serde(default = "default_refresh_lag_target")]
    pub refresh_lag_target: u64,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CreateGeoIndexRequest {
    pub name: String,
    pub collection_id: u32,
    pub path: Vec<String>,
    #[serde(default = "default_geo_precision")]
    pub precision: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CreateGraphRequest {
    pub name: String,
    pub graph_id: u32,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct VectorInsertRequest {
    pub metadata: JsonValue,
    pub vector: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct VectorSearchRequest {
    pub vector: Vec<f32>,
    pub k: usize,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct VectorSearchResponse {
    pub hits: Vec<VectorHitSummary>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct VectorHitSummary {
    pub id: String,
    pub distance: f32,
    pub metadata: Option<JsonValue>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct TimeSeriesPointRequest {
    pub series: String,
    pub point: TimePoint,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct TimeSeriesRangeQuery {
    pub series: String,
    pub start: i64,
    pub end: i64,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct SecurityStateResponse {
    pub roles: Vec<RoleSummary>,
    pub principals: Vec<PrincipalSummary>,
    pub audit_enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct SecurityStateRequest {
    pub roles: Vec<RoleSummary>,
    pub principals: Vec<PrincipalSummary>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RoleSummary {
    pub name: String,
    pub grants: Vec<GrantSummary>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct PrincipalSummary {
    pub user: String,
    pub principal: String,
    pub roles: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct GrantSummary {
    pub resource: Resource,
    pub permission: Permission,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct AuditResponse {
    pub events: Vec<AuditEvent>,
}

#[derive(Clone)]
pub struct AdminState {
    database: Arc<Mutex<Database>>,
    started_at_millis: u64,
    status: AdminStatus,
    readiness_probe: Arc<dyn Fn() -> bool + Send + Sync>,
    admin_token: Option<String>,
    admin_sessions: Arc<AdminSessionStore>,
    admin_session_ttl_seconds: u64,
    admin_login_rate_limiter: Arc<AdminLoginRateLimiter>,
    insecure_local_admin: bool,
    studio_assets_dir: Option<Arc<PathBuf>>,
}

impl AdminState {
    #[must_use]
    pub fn from_database(database: Database) -> Self {
        let started_at_millis = now_millis();
        let status = status_from_database(&database, started_at_millis);
        Self {
            database: Arc::new(Mutex::new(database)),
            started_at_millis,
            status,
            readiness_probe: Arc::new(|| true),
            admin_token: None,
            admin_sessions: Arc::new(AdminSessionStore::default()),
            admin_session_ttl_seconds: DEFAULT_ADMIN_SESSION_TTL_SECONDS,
            admin_login_rate_limiter: Arc::new(AdminLoginRateLimiter::new(
                AdminLoginRateLimitConfig::default(),
            )),
            insecure_local_admin: false,
            studio_assets_dir: None,
        }
    }

    #[must_use]
    pub fn from_database_handle(database: Arc<Mutex<Database>>) -> Self {
        let started_at_millis = now_millis();
        let status = database.try_lock().map_or_else(
            |_| empty_status(started_at_millis),
            |database| status_from_database(&database, started_at_millis),
        );
        Self {
            database,
            started_at_millis,
            status,
            readiness_probe: Arc::new(|| true),
            admin_token: None,
            admin_sessions: Arc::new(AdminSessionStore::default()),
            admin_session_ttl_seconds: DEFAULT_ADMIN_SESSION_TTL_SECONDS,
            admin_login_rate_limiter: Arc::new(AdminLoginRateLimiter::new(
                AdminLoginRateLimitConfig::default(),
            )),
            insecure_local_admin: false,
            studio_assets_dir: None,
        }
    }

    #[must_use]
    pub fn with_readiness_probe(
        database: Database,
        readiness_probe: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Self {
        let started_at_millis = now_millis();
        let status = status_from_database(&database, started_at_millis);
        Self {
            database: Arc::new(Mutex::new(database)),
            started_at_millis,
            status,
            readiness_probe: Arc::new(readiness_probe),
            admin_token: None,
            admin_sessions: Arc::new(AdminSessionStore::default()),
            admin_session_ttl_seconds: DEFAULT_ADMIN_SESSION_TTL_SECONDS,
            admin_login_rate_limiter: Arc::new(AdminLoginRateLimiter::new(
                AdminLoginRateLimitConfig::default(),
            )),
            insecure_local_admin: false,
            studio_assets_dir: None,
        }
    }

    #[must_use]
    pub fn with_admin_token(mut self, token: impl Into<String>) -> Self {
        self.admin_token = Some(token.into());
        self.insecure_local_admin = false;
        self
    }

    #[must_use]
    pub fn with_admin_session_ttl_seconds(mut self, ttl_seconds: u64) -> Self {
        self.admin_session_ttl_seconds = ttl_seconds;
        self
    }

    #[must_use]
    pub fn with_admin_login_rate_limit(mut self, config: AdminLoginRateLimitConfig) -> Self {
        self.admin_login_rate_limiter = Arc::new(AdminLoginRateLimiter::new(config));
        self
    }

    /// Bootstraps the single password-backed admin account.
    ///
    /// Existing credentials are preserved unless `reset` is true.
    /// # Errors
    /// Fails when the password is empty, hashing fails, or persistence fails.
    pub async fn bootstrap_admin_password(
        &self,
        password: String,
        reset: bool,
    ) -> Result<(), String> {
        if password.trim().is_empty() {
            return Err("admin password must not be empty".to_owned());
        }

        let credential_exists = {
            let database = self.database.lock().await;
            database
                .admin_credential(ADMIN_USERNAME)
                .map_err(|error| error.to_string())?
                .is_some()
        };

        if credential_exists && !reset {
            let mut database = self.database.lock().await;
            database
                .ensure_bootstrap_admin_principal()
                .map_err(|error| error.to_string())?;
            return Ok(());
        }

        let password_hash = hash_admin_password(password).await?;
        let credential = AdminCredentialRecord {
            username: ADMIN_USERNAME.to_owned(),
            password_hash,
            updated_at_millis: now_millis(),
        };
        let mut database = self.database.lock().await;
        database
            .set_admin_credential(&credential)
            .map_err(|error| error.to_string())?;
        database
            .ensure_bootstrap_admin_principal()
            .map_err(|error| error.to_string())?;
        let _ = database.record_admin_auth_event(
            None,
            "admin_password_bootstrap",
            AuditOutcome::Succeeded,
            Some("username: admin"),
        );
        Ok(())
    }

    #[must_use]
    pub fn with_insecure_local_admin(mut self) -> Self {
        self.admin_token = None;
        self.insecure_local_admin = true;
        self.bootstrap_insecure_local_admin();
        self
    }

    #[must_use]
    pub fn with_studio_assets_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.studio_assets_dir = Some(Arc::new(dir.into()));
        self
    }

    #[must_use]
    pub fn is_admin_auth_configured(&self) -> bool {
        self.admin_token.is_some() || self.insecure_local_admin || self.has_admin_credential()
    }

    #[must_use]
    pub fn status(&self) -> AdminStatus {
        let mut status = self.status.clone();
        status.uptime_millis = now_millis().saturating_sub(self.started_at_millis);
        status
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        (self.readiness_probe)()
    }

    fn bootstrap_insecure_local_admin(&self) {
        let Ok(mut database) = self.database.try_lock() else {
            return;
        };
        let admin_role = Role::new("admin")
            .grant(Resource::System, Permission::Admin)
            .grant(Resource::Database, Permission::Read)
            .grant(Resource::Database, Permission::Write)
            .grant(Resource::Database, Permission::Admin);
        let policy = database.authz_policy().clone().allow(admin_role);
        let mut principals = database.principal_registry().clone();
        principals.insert("admin", Principal::new("admin").with_role("admin"));
        principals.insert("root", Principal::new("root").with_role("admin"));
        database.set_authz_policy(policy);
        database.set_principal_registry(principals);
    }

    fn principal_from_headers(&self, headers: &HeaderMap) -> Principal {
        if let Some(principal) = self.session_principal_from_headers(headers) {
            return principal;
        }
        headers
            .get(HEADER_PRINCIPAL)
            .and_then(|value| value.to_str().ok())
            .map_or_else(
                || {
                    if self.insecure_local_admin {
                        Principal::new("admin").with_role("admin")
                    } else {
                        Principal::new("__missing_admin_principal__")
                    }
                },
                |name| {
                    if self.insecure_local_admin && name == "admin" {
                        Principal::new("admin").with_role("admin")
                    } else if let Ok(database) = self.database.try_lock() {
                        database.principal_for_user(name)
                    } else {
                        Principal::new(name)
                    }
                },
            )
    }

    fn has_admin_credential(&self) -> bool {
        self.database.try_lock().is_ok_and(|database| {
            database
                .admin_credential(ADMIN_USERNAME)
                .is_ok_and(|credential| credential.is_some())
        })
    }

    fn session_from_headers(&self, headers: &HeaderMap) -> SessionValidation {
        let Some(token) = bearer_token(headers) else {
            return SessionValidation::Invalid;
        };
        self.admin_sessions.validate(token)
    }

    fn session_principal_from_headers(&self, headers: &HeaderMap) -> Option<Principal> {
        let SessionValidation::Valid(record) = self.session_from_headers(headers) else {
            return None;
        };
        let mut principal = Principal::new(record.principal);
        for role in record.roles {
            principal = principal.with_role(role);
        }
        Some(principal)
    }
}

pub fn router(state: AdminState) -> Router {
    api_router().with_state(state)
}

pub fn router_with_api_prefix(state: AdminState) -> Router {
    Router::new()
        .merge(api_router())
        .nest("/api", api_router())
        .with_state(state)
}

pub fn router_with_studio(state: AdminState) -> Router {
    Router::new()
        .merge(api_router())
        .nest("/api", api_router())
        .fallback(studio_static_handler)
        .with_state(state)
}

fn api_router() -> Router<AdminState> {
    Router::new()
        .route("/openapi.json", get(openapi_handler))
        .route("/health", get(health_handler))
        .route("/ready", get(ready_handler))
        .route("/auth/login", post(auth_login_handler))
        .route("/auth/logout", post(auth_logout_handler))
        .route("/auth/change-password", post(auth_change_password_handler))
        .route("/auth/me", get(auth_me_handler))
        .route("/status", get(status_handler))
        .route("/metrics", get(metrics_handler))
        .route("/catalog", get(catalog_handler))
        .route("/sql", post(sql_handler))
        .route(
            "/data/tables/{name}/rows",
            get(table_rows_handler)
                .post(table_row_create_handler)
                .put(table_row_update_handler)
                .delete(table_row_delete_handler),
        )
        .route(
            "/data/collections/{name}/documents",
            get(documents_handler).post(document_create_handler),
        )
        .route(
            "/data/collections/{name}/documents/{id}",
            put(document_update_handler).delete(document_delete_handler),
        )
        .route("/data/vectors/{name}/vectors", post(vector_insert_handler))
        .route("/data/vectors/{name}/search", post(vector_search_handler))
        .route(
            "/data/time-series/{name}/points",
            get(time_series_range_handler).post(time_series_insert_handler),
        )
        .route("/builder/table", post(create_table_handler))
        .route("/builder/collection", post(create_collection_handler))
        .route("/builder/vector", post(create_vector_handler))
        .route("/builder/time-series", post(create_time_series_handler))
        .route("/builder/full-text", post(create_full_text_handler))
        .route("/builder/geo", post(create_geo_index_handler))
        .route("/builder/graph", post(create_graph_handler))
        .route(
            "/security",
            get(security_handler).post(security_update_handler),
        )
        .route("/audit", get(audit_handler))
        .route("/config", get(config_handler))
        .route("/config/validate", post(config_validate_handler))
        .route("/config/plan", post(config_plan_handler))
        .route("/config/apply", post(config_apply_handler))
        .route("/profiles", get(profiles_handler))
        .route("/roles", get(roles_handler))
        .route("/domains", get(domains_handler))
        .route("/extensions", get(extensions_handler))
        .route("/advice", get(advice_handler))
        .route("/advice/plan", post(advice_plan_handler))
        .route("/advice/decision", post(advice_decision_handler))
        .route("/studio", get(studio_handler))
        .layer(DefaultBodyLimit::max(MAX_CONTROL_PLANE_BODY_BYTES))
}

#[must_use]
pub fn local_insecure_admin_allowed(bind: SocketAddr) -> bool {
    bind.ip().is_loopback()
}

async fn health_handler() -> impl IntoResponse {
    Json(HealthResponse {
        ok: true,
        status: "alive",
    })
}

async fn openapi_handler() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(CONTENT_TYPE, "application/json; charset=utf-8")],
        CONTROL_PLANE_OPENAPI_JSON,
    )
}

async fn studio_static_handler(
    State(state): State<AdminState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
) -> Response {
    if method != Method::GET && method != Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    if uri.path().starts_with("/api/") || uri.path() == "/api" {
        return StatusCode::NOT_FOUND.into_response();
    }

    let Some(root) = state.studio_assets_dir.as_deref() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(candidate) = studio_asset_candidate(root, uri.path()) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let file = if fs::metadata(&candidate).is_ok_and(|metadata| metadata.is_file()) {
        candidate
    } else {
        root.join("index.html")
    };
    let content_type = content_type_for_path(&file);

    match fs::read(file) {
        Ok(bytes) => {
            let body = if method == Method::HEAD {
                Vec::new()
            } else {
                bytes
            };
            ([(CONTENT_TYPE, content_type)], body).into_response()
        }
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn ready_handler(State(state): State<AdminState>) -> impl IntoResponse {
    if !state.is_ready() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthResponse {
                ok: false,
                status: "not_ready",
            }),
        );
    }
    (
        StatusCode::OK,
        Json(HealthResponse {
            ok: true,
            status: "ready",
        }),
    )
}

async fn auth_login_handler(
    State(state): State<AdminState>,
    payload: Result<Json<LoginRequest>, JsonRejection>,
) -> Response {
    let Json(request) = match payload {
        Ok(payload) => payload,
        Err(error) => return invalid_json(&error),
    };
    let username = request.username.trim().to_owned();
    if state.admin_login_rate_limiter.is_limited(&username) {
        audit_auth_event(
            &state,
            None,
            "login_rate_limited",
            AuditOutcome::Denied,
            Some("bucket: login"),
        )
        .await;
        return unauthorized_auth();
    }
    let credential = {
        let database = state.database.lock().await;
        match database.admin_credential(ADMIN_USERNAME) {
            Ok(credential) => credential,
            Err(error) => return db_error(&error),
        }
    };

    let Some(credential) = credential.filter(|credential| username == credential.username) else {
        state.admin_login_rate_limiter.record_failure(&username);
        audit_auth_event(
            &state,
            None,
            "login",
            AuditOutcome::Failed,
            Some(&format!("username: {username}")),
        )
        .await;
        return unauthorized_auth();
    };

    let verified = verify_admin_password(credential.password_hash, request.password)
        .await
        .unwrap_or(false);
    if !verified {
        state.admin_login_rate_limiter.record_failure(&username);
        audit_auth_event(
            &state,
            None,
            "login",
            AuditOutcome::Failed,
            Some(&format!("username: {username}")),
        )
        .await;
        return unauthorized_auth();
    }
    state.admin_login_rate_limiter.record_success(&username);

    let principal = admin_principal();
    {
        let mut database = state.database.lock().await;
        if let Err(error) = database.ensure_bootstrap_admin_principal() {
            return db_error(&error);
        }
        if let Err(error) = database.record_admin_auth_event(
            Some(&principal),
            "login",
            AuditOutcome::Succeeded,
            Some("username: admin"),
        ) {
            return db_error(&error);
        }
    }

    let (token, session) = match state.admin_sessions.issue(state.admin_session_ttl_seconds) {
        Ok(session) => session,
        Err(error) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "session_error",
                format!("session error: {error}"),
            );
        }
    };

    api_ok(
        StatusCode::OK,
        LoginResponse {
            token,
            expires_at: format_rfc3339_millis(session.expires_at_millis),
            expires_at_millis: session.expires_at_millis,
            principal: session.principal,
            roles: session.roles,
        },
    )
}

async fn auth_logout_handler(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    let SessionValidation::Valid(session) = state.session_from_headers(&headers) else {
        return unauthorized_auth();
    };
    let Some(token) = bearer_token(&headers) else {
        return unauthorized_auth();
    };
    let _ = state.admin_sessions.revoke(token);
    let principal = session_principal(&session);
    audit_auth_event(
        &state,
        Some(&principal),
        "logout",
        AuditOutcome::Succeeded,
        Some("username: admin"),
    )
    .await;
    api_ok(StatusCode::OK, json!({}))
}

async fn auth_change_password_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    payload: Result<Json<ChangePasswordRequest>, JsonRejection>,
) -> Response {
    let Json(request) = match payload {
        Ok(payload) => payload,
        Err(error) => return invalid_json(&error),
    };
    if request.new_password.trim().is_empty() {
        return invalid_json_value("new password must not be empty");
    }
    let SessionValidation::Valid(session) = state.session_from_headers(&headers) else {
        return unauthorized_auth();
    };
    let principal = session_principal(&session);
    let credential = {
        let database = state.database.lock().await;
        match database.admin_credential(ADMIN_USERNAME) {
            Ok(Some(credential)) => credential,
            Ok(None) => return unauthorized_auth(),
            Err(error) => return db_error(&error),
        }
    };
    let verified = verify_admin_password(credential.password_hash, request.current_password)
        .await
        .unwrap_or(false);
    if !verified {
        audit_auth_event(
            &state,
            Some(&principal),
            "change_password",
            AuditOutcome::Failed,
            Some("username: admin"),
        )
        .await;
        return unauthorized_auth();
    }

    let password_hash = match hash_admin_password(request.new_password).await {
        Ok(hash) => hash,
        Err(error) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "password_hash_error",
                format!("password hash error: {error}"),
            );
        }
    };
    let credential = AdminCredentialRecord {
        username: ADMIN_USERNAME.to_owned(),
        password_hash,
        updated_at_millis: now_millis(),
    };
    {
        let mut database = state.database.lock().await;
        if let Err(error) = database.set_admin_credential(&credential) {
            return db_error(&error);
        }
        if let Err(error) = database.ensure_bootstrap_admin_principal() {
            return db_error(&error);
        }
        if let Err(error) = database.record_admin_auth_event(
            Some(&principal),
            "change_password",
            AuditOutcome::Succeeded,
            Some("username: admin"),
        ) {
            return db_error(&error);
        }
    }
    api_ok(StatusCode::OK, json!({}))
}

async fn auth_me_handler(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let principal = state.principal_from_headers(&headers);
    let database = state.database.lock().await;
    let system_admin = database
        .authz_policy()
        .authorize(&principal, &Resource::System, Permission::Admin)
        .is_ok();
    let database_admin = database
        .authz_policy()
        .authorize(&principal, &Resource::Database, Permission::Admin)
        .is_ok();
    api_ok(
        StatusCode::OK,
        AuthMeResponse {
            principal: principal.name().to_owned(),
            roles: principal.roles().iter().cloned().collect(),
            system_admin,
            database_admin,
            insecure_local_admin: state.insecure_local_admin,
        },
    )
}

async fn status_handler(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let database = state.database.lock().await;
    api_ok(
        StatusCode::OK,
        status_from_database(&database, state.started_at_millis),
    )
}

async fn metrics_handler(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    match observability::global_registry().render() {
        Ok(metrics) => (StatusCode::OK, metrics).into_response(),
        Err(error) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "metrics_error",
            format!("metrics error: {error}"),
        ),
    }
}

async fn config_handler(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let database = state.database.lock().await;
    api_ok(
        StatusCode::OK,
        DatabaseSpec::from_db_config("current", database.config()),
    )
}

async fn catalog_handler(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let database = state.database.lock().await;
    match catalog_response(&database) {
        Ok(catalog) => api_ok(StatusCode::OK, catalog),
        Err(error) => db_error(&error),
    }
}

async fn sql_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<SqlRequest>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let principal = state.principal_from_headers(&headers);
    let database = state.database.lock().await;
    match database.query_as(&principal, &request.sql).await {
        Ok(output) => api_ok(
            StatusCode::OK,
            SqlResponse {
                output: sql_output_json(output),
            },
        ),
        Err(error) => db_error(&error),
    }
}

async fn table_rows_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
    AxumQuery(query): AxumQuery<DataPageQuery>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let principal = state.principal_from_headers(&headers);
    let database = state.database.lock().await;
    if let Err(error) = database.authz_policy().authorize(
        &principal,
        &Resource::Table(name.clone()),
        Permission::Read,
    ) {
        return db_error(&DbError::from(error));
    }
    let page = data_page(&query);
    match database.table(&name).and_then(|table| {
        let mut rows = table.scan_page(page.offset, page.limit.saturating_add(1))?;
        let has_more = rows.len() > page.limit;
        if has_more {
            rows.truncate(page.limit);
        }
        let returned = rows.len();
        Ok(TableRowsResponse {
            table: name.clone(),
            schema: table.schema().cloned(),
            rows: rows.into_iter().map(model_row_to_json).collect(),
            offset: page.offset,
            limit: page.limit,
            returned,
            has_more,
            next_offset: has_more.then_some(page.offset.saturating_add(returned)),
            capped: page.capped,
        })
    }) {
        Ok(rows) => api_ok(StatusCode::OK, rows),
        Err(error) => db_error(&error),
    }
}

async fn table_row_create_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
    body: Result<Json<TableRowRequest>, JsonRejection>,
) -> Response {
    table_row_write(state, headers, name, body, TableWriteMode::Insert).await
}

async fn table_row_update_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
    body: Result<Json<TableRowRequest>, JsonRejection>,
) -> Response {
    table_row_write(state, headers, name, body, TableWriteMode::Update).await
}

async fn table_row_delete_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
    body: Result<Json<TableRowDeleteRequest>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    if request.confirm != name {
        return api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "confirmation_mismatch",
            format!("type {name} to confirm row deletion"),
        );
    }
    let primary_key = match json_to_model_value(request.primary_key) {
        Ok(value) => value,
        Err(message) => return invalid_json_value(message),
    };
    let principal = state.principal_from_headers(&headers);
    let database = state.database.lock().await;
    if let Err(error) = database.authz_policy().authorize(
        &principal,
        &Resource::Table(name.clone()),
        Permission::Write,
    ) {
        return db_error(&DbError::from(error));
    }
    match database.begin_transaction().and_then(|mut txn| {
        txn.delete_row(&name, &primary_key)?;
        txn.commit()
    }) {
        Ok(()) => api_ok(StatusCode::OK, json!({ "deleted": true })),
        Err(error) => db_error(&error),
    }
}

async fn documents_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
    AxumQuery(query): AxumQuery<DataPageQuery>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let principal = state.principal_from_headers(&headers);
    let database = state.database.lock().await;
    if let Err(error) = database.authz_policy().authorize(
        &principal,
        &Resource::Collection(name.clone()),
        Permission::Read,
    ) {
        return db_error(&DbError::from(error));
    }
    let page = data_page(&query);
    match database.collection(&name).and_then(|collection| {
        let mut documents = collection.scan_page(page.offset, page.limit.saturating_add(1))?;
        let has_more = documents.len() > page.limit;
        if has_more {
            documents.truncate(page.limit);
        }
        let returned = documents.len();
        Ok(DocumentListResponse {
            collection: name.clone(),
            documents: documents
                .into_iter()
                .map(|(id, document)| DocumentSummary {
                    id: document_id_to_hex(id),
                    document: model_value_to_json(&document),
                })
                .collect(),
            offset: page.offset,
            limit: page.limit,
            returned,
            has_more,
            next_offset: has_more.then_some(page.offset.saturating_add(returned)),
            capped: page.capped,
        })
    }) {
        Ok(documents) => api_ok(StatusCode::OK, documents),
        Err(error) => db_error(&error),
    }
}

async fn document_create_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
    body: Result<Json<DocumentCreateRequest>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let document = match json_to_model_value(request.document) {
        Ok(value) => value,
        Err(message) => return invalid_json_value(message),
    };
    let principal = state.principal_from_headers(&headers);
    let database = state.database.lock().await;
    if let Err(error) = database.authz_policy().authorize(
        &principal,
        &Resource::Collection(name.clone()),
        Permission::Write,
    ) {
        return db_error(&DbError::from(error));
    }
    match database.begin_transaction().and_then(|mut txn| {
        let id = txn.insert_document(&name, &document)?;
        txn.commit()?;
        Ok(id)
    }) {
        Ok(id) => api_ok(
            StatusCode::OK,
            DocumentCreateResponse {
                id: document_id_to_hex(id),
            },
        ),
        Err(error) => db_error(&error),
    }
}

async fn document_update_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    AxumPath((name, id)): AxumPath<(String, String)>,
    body: Result<Json<DocumentUpdateRequest>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let id = match document_id_from_hex(&id) {
        Ok(id) => id,
        Err(message) => return api_error(StatusCode::BAD_REQUEST, "invalid_document_id", message),
    };
    let document = match json_to_model_value(request.document) {
        Ok(value) => value,
        Err(message) => return invalid_json_value(message),
    };
    let principal = state.principal_from_headers(&headers);
    let database = state.database.lock().await;
    if let Err(error) = database.authz_policy().authorize(
        &principal,
        &Resource::Collection(name.clone()),
        Permission::Write,
    ) {
        return db_error(&DbError::from(error));
    }
    match database.begin_transaction().and_then(|mut txn| {
        txn.update_document(&name, id, &document)?;
        txn.commit()
    }) {
        Ok(()) => api_ok(StatusCode::OK, json!({ "updated": true })),
        Err(error) => db_error(&error),
    }
}

async fn document_delete_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    AxumPath((name, id)): AxumPath<(String, String)>,
    body: Result<Json<ConfirmRequest>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    if request.confirm != id {
        return api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "confirmation_mismatch",
            "type the document id to confirm deletion",
        );
    }
    let id = match document_id_from_hex(&id) {
        Ok(id) => id,
        Err(message) => return api_error(StatusCode::BAD_REQUEST, "invalid_document_id", message),
    };
    let principal = state.principal_from_headers(&headers);
    let database = state.database.lock().await;
    if let Err(error) = database.authz_policy().authorize(
        &principal,
        &Resource::Collection(name.clone()),
        Permission::Write,
    ) {
        return db_error(&DbError::from(error));
    }
    match database.begin_transaction().and_then(|mut txn| {
        txn.delete_document(&name, id)?;
        txn.commit()
    }) {
        Ok(()) => api_ok(StatusCode::OK, json!({ "deleted": true })),
        Err(error) => db_error(&error),
    }
}

#[derive(Clone, Copy)]
enum TableWriteMode {
    Insert,
    Update,
}

async fn table_row_write(
    state: AdminState,
    headers: HeaderMap,
    name: String,
    body: Result<Json<TableRowRequest>, JsonRejection>,
    mode: TableWriteMode,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let row = match json_row_to_model(request.row) {
        Ok(row) => row,
        Err(message) => return invalid_json_value(message),
    };
    let principal = state.principal_from_headers(&headers);
    let database = state.database.lock().await;
    if let Err(error) = database.authz_policy().authorize(
        &principal,
        &Resource::Table(name.clone()),
        Permission::Write,
    ) {
        return db_error(&DbError::from(error));
    }
    let result = database.begin_transaction().and_then(|mut txn| {
        match mode {
            TableWriteMode::Insert => txn.insert_row(&name, row)?,
            TableWriteMode::Update => txn.update_row(&name, row)?,
        }
        txn.commit()
    });
    match result {
        Ok(()) => api_ok(StatusCode::OK, json!({ "written": true })),
        Err(error) => db_error(&error),
    }
}

async fn vector_insert_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
    body: Result<Json<VectorInsertRequest>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let metadata = match json_to_model_value(request.metadata) {
        Ok(value) => value,
        Err(message) => return invalid_json_value(message),
    };
    let principal = state.principal_from_headers(&headers);
    let database = state.database.lock().await;
    if let Err(error) = database.authz_policy().authorize(
        &principal,
        &Resource::VectorCollection(name.clone()),
        Permission::Write,
    ) {
        return db_error(&DbError::from(error));
    }
    match database
        .vector_collection(&name)
        .and_then(|vectors| Ok(vectors.insert_vector(&metadata, request.vector)?))
    {
        Ok(id) => api_ok(
            StatusCode::OK,
            DocumentCreateResponse {
                id: document_id_to_hex(id),
            },
        ),
        Err(error) => db_error(&error),
    }
}

async fn vector_search_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
    body: Result<Json<VectorSearchRequest>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let principal = state.principal_from_headers(&headers);
    let database = state.database.lock().await;
    if let Err(error) = database.authz_policy().authorize(
        &principal,
        &Resource::VectorCollection(name.clone()),
        Permission::Read,
    ) {
        return db_error(&DbError::from(error));
    }
    match database.vector_collection(&name).and_then(|vectors| {
        let hits = vectors.knn(&request.vector, request.k)?;
        vector_hits_with_metadata(&vectors, hits)
    }) {
        Ok(hits) => api_ok(StatusCode::OK, VectorSearchResponse { hits }),
        Err(error) => db_error(&error),
    }
}

async fn time_series_insert_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
    body: Result<Json<TimeSeriesPointRequest>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let principal = state.principal_from_headers(&headers);
    let database = state.database.lock().await;
    if let Err(error) = database.authz_policy().authorize(
        &principal,
        &Resource::TimeSeries(name.clone()),
        Permission::Write,
    ) {
        return db_error(&DbError::from(error));
    }
    match database
        .time_series(&name)
        .and_then(|series| Ok(series.insert_point(&request.series, request.point)?))
    {
        Ok(()) => api_ok(StatusCode::OK, json!({ "written": true })),
        Err(error) => db_error(&error),
    }
}

async fn time_series_range_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
    AxumQuery(query): AxumQuery<TimeSeriesRangeQuery>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let principal = state.principal_from_headers(&headers);
    let database = state.database.lock().await;
    if let Err(error) = database.authz_policy().authorize(
        &principal,
        &Resource::TimeSeries(name.clone()),
        Permission::Read,
    ) {
        return db_error(&DbError::from(error));
    }
    match database
        .time_series(&name)
        .and_then(|series| Ok(series.range(&query.series, query.start, query.end)?))
    {
        Ok(points) => api_ok(StatusCode::OK, json!({ "points": points })),
        Err(error) => db_error(&error),
    }
}

async fn create_table_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<CreateTableRequest>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let principal = state.principal_from_headers(&headers);
    let mut database = state.database.lock().await;
    let indexes = request
        .indexes
        .into_iter()
        .map(|index| RelIndexSpec {
            id: index.id,
            column: index.column,
            expression: index.expression,
            predicate: None,
            include: index.include,
        })
        .collect();
    match database.create_table_as(&principal, request.name, request.schema, indexes) {
        Ok(table) => api_ok(
            StatusCode::OK,
            json!({ "created": true, "name": table.name(), "kind": "table" }),
        ),
        Err(error) => db_error(&error),
    }
}

async fn create_collection_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<CreateCollectionRequest>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let principal = state.principal_from_headers(&headers);
    let mut database = state.database.lock().await;
    let indexes = request
        .indexes
        .into_iter()
        .map(|index| IndexSpec::new(IndexId::new(index.id), FieldPath::new(index.path)))
        .collect();
    let collection_id = request
        .collection_id
        .unwrap_or_else(|| next_collection_id(&database));
    match database.create_collection_as(
        &principal,
        request.name.clone(),
        CollectionId::new(collection_id),
        request.fields,
        indexes,
    ) {
        Ok(collection) => api_ok(
            StatusCode::OK,
            json!({
                "created": true,
                "name": request.name,
                "kind": "collection",
                "collection_id": collection.collection_id().as_u32()
            }),
        ),
        Err(error) => db_error(&error),
    }
}

async fn create_vector_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<CreateVectorRequest>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let principal = state.principal_from_headers(&headers);
    let mut database = state.database.lock().await;
    let collection_id = request
        .collection_id
        .unwrap_or_else(|| next_collection_id(&database));
    let config = VectorCollectionConfig::new(CollectionId::new(collection_id), request.dim)
        .with_metric(request.metric)
        .with_hnsw(request.hnsw);
    match database.create_vector_collection_as(&principal, request.name.clone(), config) {
        Ok(vectors) => api_ok(
            StatusCode::OK,
            json!({
                "created": true,
                "name": request.name,
                "kind": "vector",
                "collection_id": vectors.collection_id().as_u32()
            }),
        ),
        Err(error) => db_error(&error),
    }
}

async fn create_time_series_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<CreateTimeSeriesRequest>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let principal = state.principal_from_headers(&headers);
    let mut database = state.database.lock().await;
    let config = TimeSeriesConfig {
        name: request.name.clone(),
        chunk_millis: request.chunk_millis,
        retention_millis: request.retention_millis,
    };
    match database.create_time_series_as(&principal, &config) {
        Ok(series) => api_ok(
            StatusCode::OK,
            json!({ "created": true, "name": series.config().name, "kind": "time_series" }),
        ),
        Err(error) => db_error(&error),
    }
}

async fn create_full_text_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<CreateFullTextRequest>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let principal = state.principal_from_headers(&headers);
    let mut database = state.database.lock().await;
    let mut config = FullTextIndexConfig::collection(
        request.name.clone(),
        CollectionId::new(request.collection_id),
        FieldPath::new(request.path),
    );
    config.language = request.language;
    config.refresh_lag_target = request.refresh_lag_target;
    match database.create_full_text_index_as(&principal, &config) {
        Ok(index) => api_ok(
            StatusCode::OK,
            json!({ "created": true, "name": index.config().name, "kind": "full_text_index" }),
        ),
        Err(error) => db_error(&error),
    }
}

async fn create_geo_index_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<CreateGeoIndexRequest>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let principal = state.principal_from_headers(&headers);
    let mut database = state.database.lock().await;
    let mut config = GeoIndexConfig::new(
        request.name.clone(),
        CollectionId::new(request.collection_id),
        FieldPath::new(request.path),
    );
    config.precision = request.precision;
    match database.create_geo_index_as(&principal, &config) {
        Ok(_) => api_ok(
            StatusCode::OK,
            json!({ "created": true, "name": request.name, "kind": "geo_index" }),
        ),
        Err(error) => db_error(&error),
    }
}

async fn create_graph_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<CreateGraphRequest>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let principal = state.principal_from_headers(&headers);
    let mut database = state.database.lock().await;
    match database.create_graph_as(
        &principal,
        request.name.clone(),
        GraphId::new(request.graph_id),
    ) {
        Ok(_) => api_ok(
            StatusCode::OK,
            json!({ "created": true, "name": request.name, "kind": "graph", "graph_id": request.graph_id }),
        ),
        Err(error) => db_error(&error),
    }
}

async fn security_handler(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let principal = state.principal_from_headers(&headers);
    let database = state.database.lock().await;
    if let Err(error) =
        database
            .authz_policy()
            .authorize(&principal, &Resource::System, Permission::Admin)
    {
        return db_error(&DbError::from(error));
    }
    api_ok(StatusCode::OK, security_state(&database))
}

async fn security_update_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<SecurityStateRequest>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let principal = state.principal_from_headers(&headers);
    let mut database = state.database.lock().await;
    let policy = AuthzPolicy::new(request.roles.into_iter().map(role_from_summary));
    let mut registry = PrincipalRegistry::new();
    for principal in request.principals {
        let mut mapped = Principal::new(principal.principal);
        for role in principal.roles {
            mapped = mapped.with_role(role);
        }
        registry.insert(principal.user, mapped);
    }
    if let Err(error) = database.set_authz_policy_as(&principal, policy) {
        return db_error(&error);
    }
    match database.set_principal_registry_as(&principal, registry) {
        Ok(()) => api_ok(StatusCode::OK, security_state(&database)),
        Err(error) => db_error(&error),
    }
}

async fn audit_handler(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let principal = state.principal_from_headers(&headers);
    let database = state.database.lock().await;
    match database.audit_events_as(&principal) {
        Ok(events) => api_ok(StatusCode::OK, AuditResponse { events }),
        Err(error) => db_error(&error),
    }
}

async fn config_validate_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<DatabaseSpec>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(spec) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let report = GuaranteeValidator::validate(&spec);
    let status = if report.valid {
        StatusCode::OK
    } else {
        StatusCode::UNPROCESSABLE_ENTITY
    };
    api_ok(status, report)
}

async fn config_plan_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<ConfigPlanRequest>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let plan = MigrationPlanner::plan(&request.current, &request.desired);
    let status = if plan.valid {
        StatusCode::OK
    } else {
        StatusCode::UNPROCESSABLE_ENTITY
    };
    api_ok(status, plan)
}

async fn config_apply_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<ConfigApplyRequest>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let principal = state.principal_from_headers(&headers);
    let database = state.database.lock().await;
    match database.confirm_config_apply_as(&principal, &request.plan, &request.confirm) {
        Ok(report) => {
            let status = status_for_apply_report(&report);
            api_ok(status, report)
        }
        Err(error) => db_error(&error),
    }
}

async fn profiles_handler(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let profiles = built_in_profiles()
        .iter()
        .map(|profile| {
            json!({
                "slug": profile.slug,
                "aliases": profile.aliases,
                "status": profile.status,
                "description": profile.description,
                "default_domain": consistency_domain_definition(profile.default_domain)
                    .map_or("unknown", |domain| domain.slug),
                "compatible_roles": profile
                    .compatible_roles
                    .iter()
                    .filter_map(|role| {
                        collection_role_definitions()
                            .iter()
                            .find(|definition| definition.role == *role)
                            .map(|definition| definition.slug)
                    })
                    .collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();
    api_ok(StatusCode::OK, profiles)
}

async fn roles_handler(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let roles = collection_role_definitions()
        .iter()
        .map(|role| {
            json!({
                "slug": role.slug,
                "status": role.status,
                "description": role.description,
                "required_capabilities": role.required_capabilities,
                "constraints": role.constraints,
            })
        })
        .collect::<Vec<_>>();
    api_ok(StatusCode::OK, roles)
}

async fn domains_handler(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let domains = consistency_domain_definitions()
        .iter()
        .map(|domain| {
            json!({
                "slug": domain.slug,
                "status": domain.status,
                "guarantees": domain.guarantees,
                "limits": domain.limits,
            })
        })
        .collect::<Vec<_>>();
    api_ok(StatusCode::OK, domains)
}

async fn extensions_handler(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    api_ok(StatusCode::OK, extension_catalog_entries())
}

async fn advice_handler(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let principal = state.principal_from_headers(&headers);
    let database = state.database.lock().await;
    match database.runtime_advice_as(&principal) {
        Ok(advice) => api_ok(StatusCode::OK, advice),
        Err(error) => db_error(&error),
    }
}

async fn advice_plan_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<RuntimeAdvicePlanRequest>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let principal = state.principal_from_headers(&headers);
    let database = state.database.lock().await;
    match database.runtime_advice_plan_as(&principal, &request.advice_id) {
        Ok(plan) => api_ok(StatusCode::OK, plan),
        Err(error) => db_error(&error),
    }
}

async fn advice_decision_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<RuntimeAdviceDecisionRequest>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let principal = state.principal_from_headers(&headers);
    let database = state.database.lock().await;
    match database.record_runtime_advice_decision_as(&principal, request) {
        Ok(decision) => api_ok(StatusCode::OK, decision),
        Err(error) => db_error(&error),
    }
}

async fn studio_handler(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    api_ok(
        StatusCode::OK,
        StudioManifest {
            api_version: 1,
            openapi_endpoint: "/openapi.json",
            physical_migration_supported: false,
            config_apply_data_mutated: false,
            endpoints: control_plane_operations()
                .iter()
                .map(|operation| operation.endpoint())
                .collect(),
            operations: control_plane_operations().to_vec(),
            capabilities: vec![
                "openapi_v1",
                "config_view",
                "config_validate",
                "migration_dry_run",
                "apply_confirm_audit_only",
                "catalog_view",
                "advisor_read_only",
                "runtime_advisor_v2",
                "advisor_decision_memory",
                "admin_login",
                "admin_sessions",
                "admin_login_rate_limit",
                "admin_change_password",
                "auth_me",
                "catalog_browser",
                "sql_console",
                "table_row_crud",
                "document_crud",
                "vector_crud",
                "time_series_crud",
                "resource_builder",
                "full_text_builder",
                "geo_builder",
                "graph_builder",
                "security_rbac",
                "audit_log",
            ],
        },
    )
}

fn catalog_response(database: &Database) -> Result<CatalogResponse, DbError> {
    let mut objects = Vec::new();
    for (name, entry) in database.catalog() {
        let (schema, row_count) = match entry {
            CatalogEntry::Table { .. } => {
                let table = database.table(name)?;
                (table.schema().cloned(), Some(table.scan()?.len()))
            }
            CatalogEntry::Collection { .. } => {
                let collection = database.collection(name)?;
                (None, Some(collection.scan()?.len()))
            }
            _ => (None, None),
        };
        objects.push(CatalogObjectSummary {
            name: name.clone(),
            kind: catalog_kind(entry).to_owned(),
            entry: entry.clone(),
            schema,
            row_count,
        });
    }
    Ok(CatalogResponse { objects })
}

fn catalog_kind(entry: &CatalogEntry) -> &'static str {
    match entry {
        CatalogEntry::Table { .. } => "table",
        CatalogEntry::Collection { .. } => "collection",
        CatalogEntry::Vector { .. } => "vector",
        CatalogEntry::FullTextIndex { .. } => "full_text_index",
        CatalogEntry::TimeSeries { .. } => "time_series",
        CatalogEntry::Graph { .. } => "graph",
        CatalogEntry::GeoIndex { .. } => "geo_index",
        CatalogEntry::ForeignTable { .. } => "foreign_table",
        CatalogEntry::MaterializedView { .. } => "materialized_view",
        CatalogEntry::TemporalTable { .. } => "temporal_table",
    }
}

fn next_collection_id(database: &Database) -> u32 {
    database
        .catalog()
        .values()
        .filter_map(|entry| match entry {
            CatalogEntry::Collection { collection_id, .. }
            | CatalogEntry::Vector { collection_id, .. } => Some(collection_id.as_u32()),
            _ => None,
        })
        .max()
        .unwrap_or(0)
        .saturating_add(1)
}

fn sql_output_json(output: SqlOutput) -> JsonValue {
    match output {
        SqlOutput::Rows(rows) => json!({
            "kind": "rows",
            "columns": rows.columns,
            "rows": rows.rows.into_iter().map(model_row_to_json).collect::<Vec<_>>(),
        }),
        SqlOutput::AffectedRows(rows) => json!({
            "kind": "affected_rows",
            "affected_rows": rows,
        }),
    }
}

fn json_row_to_model(row: Vec<JsonValue>) -> Result<Row, String> {
    row.into_iter().map(json_to_model_value).collect()
}

fn json_to_model_value(value: JsonValue) -> Result<ModelValue, String> {
    match value {
        JsonValue::Null => Ok(ModelValue::Null),
        JsonValue::Bool(value) => Ok(ModelValue::Bool(value)),
        JsonValue::Number(number) => {
            if let Some(value) = number.as_i64() {
                Ok(ModelValue::Int(value))
            } else {
                number
                    .as_f64()
                    .map(ModelValue::Float)
                    .ok_or_else(|| format!("unsupported JSON number: {number}"))
            }
        }
        JsonValue::String(value) => Ok(ModelValue::Str(value)),
        JsonValue::Array(values) => values
            .into_iter()
            .map(json_to_model_value)
            .collect::<Result<Vec<_>, _>>()
            .map(ModelValue::Array),
        JsonValue::Object(values) => values
            .into_iter()
            .map(|(key, value)| json_to_model_value(value).map(|value| (key, value)))
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map(ModelValue::Object),
    }
}

fn model_row_to_json(row: Row) -> Vec<JsonValue> {
    row.into_iter()
        .map(|value| model_value_to_json(&value))
        .collect()
}

fn model_value_to_json(value: &ModelValue) -> JsonValue {
    match value {
        ModelValue::Null => JsonValue::Null,
        ModelValue::Bool(value) => JsonValue::Bool(*value),
        ModelValue::Int(value) => json!(value),
        ModelValue::Float(value) => json!(value),
        ModelValue::Str(value) => JsonValue::String(value.clone()),
        ModelValue::Bytes(value) => JsonValue::String(to_hex(value)),
        ModelValue::Array(values) => {
            JsonValue::Array(values.iter().map(model_value_to_json).collect())
        }
        ModelValue::Object(values) => JsonValue::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), model_value_to_json(value)))
                .collect(),
        ),
        ModelValue::Vector(values) => json!(values),
        ModelValue::GeoPoint { lon, lat } => json!({ "lon": lon, "lat": lat }),
    }
}

fn vector_hits_with_metadata(
    vectors: &crate::vector::VectorCollection<'_, dyn crate::repl::Replication + '_>,
    hits: Vec<VectorHit>,
) -> Result<Vec<VectorHitSummary>, DbError> {
    hits.into_iter()
        .map(|hit| {
            Ok(VectorHitSummary {
                id: document_id_to_hex(hit.id),
                distance: hit.distance,
                metadata: vectors
                    .get_metadata(hit.id)?
                    .map(|metadata| model_value_to_json(&metadata)),
            })
        })
        .collect()
}

fn security_state(database: &Database) -> SecurityStateResponse {
    let roles = database
        .authz_policy()
        .roles()
        .values()
        .map(role_summary)
        .collect();
    let principals = database
        .principal_registry()
        .principals()
        .iter()
        .map(|(user, principal)| PrincipalSummary {
            user: user.clone(),
            principal: principal.name().to_owned(),
            roles: principal.roles().iter().cloned().collect(),
        })
        .collect();
    SecurityStateResponse {
        roles,
        principals,
        audit_enabled: database.audit_enabled(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DataPage {
    offset: usize,
    limit: usize,
    capped: bool,
}

fn data_page(query: &DataPageQuery) -> DataPage {
    let requested = query.limit.unwrap_or(DEFAULT_DATA_PAGE_LIMIT);
    let limit = requested.clamp(1, MAX_DATA_PAGE_LIMIT);
    DataPage {
        offset: query.offset.unwrap_or(0),
        limit,
        capped: requested != limit,
    }
}

fn role_summary(role: &Role) -> RoleSummary {
    let mut grants = Vec::new();
    for (resource, permissions) in role.grants() {
        for permission in permissions {
            grants.push(GrantSummary {
                resource: resource.clone(),
                permission: *permission,
            });
        }
    }
    RoleSummary {
        name: role.name().to_owned(),
        grants,
    }
}

fn role_from_summary(summary: RoleSummary) -> Role {
    summary
        .grants
        .into_iter()
        .fold(Role::new(summary.name), |role, grant| {
            role.grant(grant.resource, grant.permission)
        })
}

fn document_id_to_hex(id: DocumentId) -> String {
    to_hex(&id.as_bytes())
}

fn document_id_from_hex(value: &str) -> Result<DocumentId, String> {
    let value = value.trim();
    if value.len() != 32 {
        return Err("document id must be 32 lowercase hex characters".to_owned());
    }
    let mut bytes = [0_u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let start = index * 2;
        let pair = &value[start..start + 2];
        *byte = u8::from_str_radix(pair, 16)
            .map_err(|_| "document id must contain only hex characters".to_owned())?;
    }
    Ok(DocumentId::from_bytes(bytes))
}

fn status_from_database(database: &Database, started_at_millis: u64) -> AdminStatus {
    let shard_count = database.shard_map().map_or(1, |map| {
        map.placement
            .values()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            .max(1)
    });
    AdminStatus {
        server_version: SERVER_VERSION.to_owned(),
        uptime_millis: now_millis().saturating_sub(started_at_millis),
        profile: database.profile(),
        replication: database.replication_kind(),
        layout: database.layout(),
        engine: format!("{:?}", database.engine_kind()),
        catalog_objects: database.catalog().len(),
        shard_count,
    }
}

fn empty_status(started_at_millis: u64) -> AdminStatus {
    AdminStatus {
        server_version: SERVER_VERSION.to_owned(),
        uptime_millis: now_millis().saturating_sub(started_at_millis),
        profile: Profile::Balanced,
        replication: ReplicationKind::Cp,
        layout: TableLayout::default(),
        engine: "Unknown".to_owned(),
        catalog_objects: 0,
        shard_count: 1,
    }
}

fn require_admin_token(state: &AdminState, headers: &HeaderMap) -> Option<Response> {
    if state.insecure_local_admin {
        return None;
    }
    let Some(actual) = bearer_token(headers) else {
        if !state.is_admin_auth_configured() {
            return Some(api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "admin_auth_not_configured",
                "admin auth is not configured",
            ));
        }
        return Some(api_error(
            StatusCode::UNAUTHORIZED,
            "missing_admin_token",
            "missing Authorization: Bearer token",
        ));
    };
    match state.admin_sessions.validate(actual) {
        SessionValidation::Valid(_) => return None,
        SessionValidation::Expired => return Some(unauthorized_auth()),
        SessionValidation::Invalid => {}
    }
    if actual.starts_with(SESSION_TOKEN_PREFIX) {
        return Some(unauthorized_auth());
    }
    if state
        .admin_token
        .as_deref()
        .is_some_and(|expected| actual == expected)
    {
        return None;
    }
    if state.admin_token.is_none() && !state.has_admin_credential() {
        return Some(api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "admin_auth_not_configured",
            "admin auth is not configured",
        ));
    }
    Some(api_error(
        StatusCode::UNAUTHORIZED,
        "invalid_admin_token",
        "invalid admin bearer token",
    ))
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|token| !token.trim().is_empty())
}

fn unauthorized_auth() -> Response {
    api_error(StatusCode::UNAUTHORIZED, "unauthorized", "unauthorized")
}

fn admin_principal() -> Principal {
    Principal::new(ADMIN_USERNAME).with_role(ADMIN_ROLE)
}

fn session_principal(session: &SessionRecord) -> Principal {
    let mut principal = Principal::new(session.principal.clone());
    for role in &session.roles {
        principal = principal.with_role(role.clone());
    }
    principal
}

async fn audit_auth_event(
    state: &AdminState,
    principal: Option<&Principal>,
    action: &str,
    outcome: AuditOutcome,
    detail: Option<&str>,
) {
    let database = state.database.lock().await;
    if let Err(error) = database.record_admin_auth_event(principal, action, outcome, detail) {
        tracing::warn!("failed to write admin auth audit event: {error}");
    }
}

async fn hash_admin_password(password: String) -> Result<String, String> {
    tokio::task::spawn_blocking(move || {
        let mut salt_bytes = [0_u8; 16];
        getrandom::fill(&mut salt_bytes).map_err(|error| error.to_string())?;
        let salt = SaltString::encode_b64(&salt_bytes).map_err(|error| error.to_string())?;
        let params = Params::new(19 * 1_024, 2, 1, None).map_err(|error| error.to_string())?;
        let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
        argon2
            .hash_password(password.as_bytes(), &salt)
            .map(|hash| hash.to_string())
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| error.to_string())?
}

async fn verify_admin_password(password_hash: String, password: String) -> Result<bool, String> {
    tokio::task::spawn_blocking(move || {
        let parsed_hash = PasswordHash::new(&password_hash).map_err(|error| error.to_string())?;
        let params = Params::new(19 * 1_024, 2, 1, None).map_err(|error| error.to_string())?;
        let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
        Ok(argon2
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok())
    })
    .await
    .map_err(|error| error.to_string())?
}

fn new_session_token() -> Result<String, String> {
    let mut token_bytes = [0_u8; 32];
    getrandom::fill(&mut token_bytes).map_err(|error| error.to_string())?;
    Ok(format!("{SESSION_TOKEN_PREFIX}{}", to_hex(&token_bytes)))
}

fn session_token_hash(token: &str) -> String {
    blake3::hash(token.as_bytes()).to_hex().to_string()
}

fn format_rfc3339_millis(millis: u64) -> String {
    let seconds = millis / 1_000;
    let Some(seconds) = i64::try_from(seconds).ok() else {
        return millis.to_string();
    };
    let Ok(timestamp) = OffsetDateTime::from_unix_timestamp(seconds) else {
        return millis.to_string();
    };
    timestamp
        .format(&Rfc3339)
        .unwrap_or_else(|_| millis.to_string())
}

fn status_for_apply_report(report: &ApplyCheckReport) -> StatusCode {
    match report.status {
        ApplyStatus::Confirmed => StatusCode::OK,
        ApplyStatus::Rejected | ApplyStatus::Unsupported => StatusCode::UNPROCESSABLE_ENTITY,
    }
}

fn invalid_json(error: &JsonRejection) -> Response {
    api_error(
        StatusCode::BAD_REQUEST,
        "invalid_json",
        format!("invalid JSON request body: {error}"),
    )
}

fn invalid_json_value(message: impl Into<String>) -> Response {
    api_error(StatusCode::BAD_REQUEST, "invalid_value", message)
}

fn db_error(error: &DbError) -> Response {
    if matches!(error, DbError::AuthzDenied(_)) {
        return api_error(StatusCode::FORBIDDEN, "forbidden", error.to_string());
    }
    api_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
        error.to_string(),
    )
}

fn api_ok<T: Serialize>(status: StatusCode, data: T) -> Response {
    (status, Json(json!({ "ok": true, "data": data }))).into_response()
}

fn api_error(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Response {
    (
        status,
        Json(json!({
            "ok": false,
            "error": {
                "code": code.into(),
                "message": message.into(),
            },
        })),
    )
        .into_response()
}

fn studio_asset_candidate(root: &Path, uri_path: &str) -> Option<PathBuf> {
    let trimmed = uri_path.trim_start_matches('/');
    if trimmed.is_empty() {
        return Some(root.join("index.html"));
    }

    let mut path = root.to_path_buf();
    for segment in trimmed.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." || segment.contains('\\') {
            return None;
        }
        path.push(segment);
    }
    Some(path)
}

fn content_type_for_path(path: &Path) -> &'static str {
    match path.extension().and_then(std::ffi::OsStr::to_str) {
        Some("css") => "text/css; charset=utf-8",
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("json") => "application/json; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("wasm") => "application/wasm",
        _ => "application/octet-stream",
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn seconds_to_millis(seconds: u64) -> u64 {
    seconds.saturating_mul(1_000)
}

fn login_username_bucket(username: &str) -> String {
    let normalized = username.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        "user:__empty__".to_owned()
    } else {
        format!("user:{normalized}")
    }
}

fn default_text_language() -> String {
    "simple".to_owned()
}

const fn default_refresh_lag_target() -> u64 {
    1
}

const fn default_geo_precision() -> u8 {
    6
}

fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::{
        AdminLoginRateLimitConfig, AdminState, ConfigApplyRequest, ConfigPlanRequest,
        local_insecure_admin_allowed,
    };
    use crate::{
        config_spec::{ApplyStatus, DatabaseSpec, MigrationPlanner},
        db::{DbConfig, Profile, create_database, open_database},
        runtime_advisor::{
            RuntimeAdviceDecisionRequest, RuntimeAdvicePlanRequest, RuntimeAdviceStatus,
        },
        security::{AuthzPolicy, Permission, Principal, PrincipalRegistry, Resource, Role},
        tuning::WorkloadSample,
    };
    use axum::{
        body::{Body, to_bytes},
        http::{Method, Request, StatusCode, header::AUTHORIZATION},
    };
    use serde::Serialize;
    use serde_json::Value;
    use std::{fs, net::SocketAddr, sync::Arc};
    use tempfile::tempdir;
    use tokio::sync::Mutex;
    use tower::ServiceExt;

    #[test]
    fn admin_status_describes_database_without_payloads() -> Result<(), Box<dyn std::error::Error>>
    {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let state = AdminState::from_database(database);
        let status = state.status();
        assert_eq!(status.profile, Profile::InMemory);
        assert_eq!(status.catalog_objects, 0);
        assert_eq!(status.server_version, crate::compat::SERVER_VERSION);
        Ok(())
    }

    #[test]
    fn admin_readiness_uses_live_probe() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let state = AdminState::with_readiness_probe(database, {
            let ready = Arc::clone(&ready);
            move || ready.load(std::sync::atomic::Ordering::SeqCst)
        });

        assert!(!state.is_ready());
        ready.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(state.is_ready());
        Ok(())
    }

    #[test]
    fn insecure_admin_is_limited_to_loopback() -> Result<(), Box<dyn std::error::Error>> {
        let loopback = "127.0.0.1:9090".parse::<SocketAddr>()?;
        let wildcard = "0.0.0.0:9090".parse::<SocketAddr>()?;
        assert!(local_insecure_admin_allowed(loopback));
        assert!(!local_insecure_admin_allowed(wildcard));
        Ok(())
    }

    #[tokio::test]
    async fn protected_endpoints_require_bearer_token() -> Result<(), Box<dyn std::error::Error>> {
        let app = super::router(test_state()?);
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/status")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = json_body(response).await?;
        assert_eq!(body["ok"], false);
        assert_eq!(body["error"]["code"], "missing_admin_token");
        Ok(())
    }

    #[tokio::test]
    async fn missing_admin_credential_and_token_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let app = super::router(AdminState::from_database(admin_database()?));
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/status")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = json_body(response).await?;
        assert_eq!(body["ok"], false);
        assert_eq!(body["error"]["code"], "admin_auth_not_configured");
        Ok(())
    }

    #[tokio::test]
    async fn password_login_issues_session_and_ignores_principal_header()
    -> Result<(), Box<dyn std::error::Error>> {
        let state = AdminState::from_database(admin_database()?).with_admin_session_ttl_seconds(60);
        state
            .bootstrap_admin_password("correct horse battery staple".to_owned(), false)
            .await?;
        let app = super::router(state);

        let login = app
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/auth/login",
                &serde_json::json!({
                    "username": "admin",
                    "password": "correct horse battery staple"
                }),
            )?)
            .await?;

        assert_eq!(login.status(), StatusCode::OK);
        let body = json_body(login).await?;
        let token = body["data"]["token"]
            .as_str()
            .ok_or("login token must be a string")?
            .to_owned();
        assert_eq!(body["data"]["principal"], "admin");

        let me = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/auth/me")
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .header("x-multidb-principal", "root")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(me.status(), StatusCode::OK);
        let body = json_body(me).await?;
        assert_eq!(body["data"]["principal"], "admin");
        assert_eq!(body["data"]["system_admin"], true);
        Ok(())
    }

    #[tokio::test]
    async fn password_login_failure_is_neutral_and_audited()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(Mutex::new(admin_database()?));
        let state = AdminState::from_database_handle(Arc::clone(&database));
        state
            .bootstrap_admin_password("correct".to_owned(), false)
            .await?;
        let app = super::router(state);

        let response = app
            .oneshot(json_request(
                Method::POST,
                "/auth/login",
                &serde_json::json!({
                    "username": "admin",
                    "password": "wrong"
                }),
            )?)
            .await?;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = json_body(response).await?;
        assert_eq!(body["error"]["code"], "unauthorized");
        let database = database.lock().await;
        assert!(database.audit_events()?.iter().any(|event| {
            event.action == "login" && event.outcome == crate::security::AuditOutcome::Failed
        }));
        Ok(())
    }

    #[tokio::test]
    async fn repeated_login_failures_are_rate_limited_neutrally_and_audited()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(Mutex::new(admin_database()?));
        let state = AdminState::from_database_handle(Arc::clone(&database))
            .with_admin_login_rate_limit(AdminLoginRateLimitConfig::new(2, 300, 300));
        state
            .bootstrap_admin_password("correct".to_owned(), false)
            .await?;
        let app = super::router(state);

        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(json_request(
                    Method::POST,
                    "/auth/login",
                    &serde_json::json!({
                        "username": "admin",
                        "password": "wrong"
                    }),
                )?)
                .await?;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            let body = json_body(response).await?;
            assert_eq!(body["error"]["code"], "unauthorized");
        }

        let locked = app
            .oneshot(json_request(
                Method::POST,
                "/auth/login",
                &serde_json::json!({
                    "username": "admin",
                    "password": "correct"
                }),
            )?)
            .await?;
        assert_eq!(locked.status(), StatusCode::UNAUTHORIZED);
        assert!(locked.headers().get("retry-after").is_none());
        let body = json_body(locked).await?;
        assert_eq!(body["error"]["code"], "unauthorized");
        let database = database.lock().await;
        assert!(database.audit_events()?.iter().any(|event| {
            event.action == "login_rate_limited"
                && event.outcome == crate::security::AuditOutcome::Denied
                && event.detail.as_deref() == Some("bucket: login")
        }));
        Ok(())
    }

    #[tokio::test]
    async fn successful_login_clears_prior_rate_limit_failures()
    -> Result<(), Box<dyn std::error::Error>> {
        let state = AdminState::from_database(admin_database()?)
            .with_admin_login_rate_limit(AdminLoginRateLimitConfig::new(2, 300, 300));
        state
            .bootstrap_admin_password("correct".to_owned(), false)
            .await?;
        let app = super::router(state);

        let first_failure = app
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/auth/login",
                &serde_json::json!({
                    "username": "admin",
                    "password": "wrong"
                }),
            )?)
            .await?;
        assert_eq!(first_failure.status(), StatusCode::UNAUTHORIZED);

        let first_success = app
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/auth/login",
                &serde_json::json!({
                    "username": "admin",
                    "password": "correct"
                }),
            )?)
            .await?;
        assert_eq!(first_success.status(), StatusCode::OK);

        let second_failure = app
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/auth/login",
                &serde_json::json!({
                    "username": "admin",
                    "password": "wrong"
                }),
            )?)
            .await?;
        assert_eq!(second_failure.status(), StatusCode::UNAUTHORIZED);

        let second_success = app
            .oneshot(json_request(
                Method::POST,
                "/auth/login",
                &serde_json::json!({
                    "username": "admin",
                    "password": "correct"
                }),
            )?)
            .await?;
        assert_eq!(second_success.status(), StatusCode::OK);
        Ok(())
    }

    #[tokio::test]
    async fn logout_invalidates_session() -> Result<(), Box<dyn std::error::Error>> {
        let state = AdminState::from_database(admin_database()?).with_admin_session_ttl_seconds(60);
        state
            .bootstrap_admin_password("correct".to_owned(), false)
            .await?;
        let app = super::router(state);
        let token = login_token(&app, "correct").await?;

        let logout = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/auth/logout")
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(logout.status(), StatusCode::OK);

        let me = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/auth/me")
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(me.status(), StatusCode::UNAUTHORIZED);
        Ok(())
    }

    #[tokio::test]
    async fn expired_session_returns_unauthorized() -> Result<(), Box<dyn std::error::Error>> {
        let state = AdminState::from_database(admin_database()?).with_admin_session_ttl_seconds(0);
        state
            .bootstrap_admin_password("correct".to_owned(), false)
            .await?;
        let app = super::router(state);
        let token = login_token(&app, "correct").await?;

        let me = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/auth/me")
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(me.status(), StatusCode::UNAUTHORIZED);
        let body = json_body(me).await?;
        assert_eq!(body["error"]["code"], "unauthorized");
        Ok(())
    }

    #[tokio::test]
    async fn change_password_persists_across_reopen() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let db_path = temp.path().join("admin-auth.redb");
        {
            let state = AdminState::from_database(create_database(DbConfig::on_disk(
                Profile::Transactional,
                &db_path,
            ))?);
            state
                .bootstrap_admin_password("old-secret".to_owned(), false)
                .await?;
            let app = super::router(state);
            let token = login_token(&app, "old-secret").await?;
            let response = app
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri("/auth/change-password")
                        .header(AUTHORIZATION, format!("Bearer {token}"))
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&serde_json::json!({
                            "current_password": "old-secret",
                            "new_password": "new-secret"
                        }))?))?,
                )
                .await?;
            assert_eq!(response.status(), StatusCode::OK);
        }

        let reopened = open_database(DbConfig::on_disk(Profile::Transactional, &db_path))?;
        let app = super::router(AdminState::from_database(reopened));
        let failed = app
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/auth/login",
                &serde_json::json!({
                    "username": "admin",
                    "password": "old-secret"
                }),
            )?)
            .await?;
        assert_eq!(failed.status(), StatusCode::UNAUTHORIZED);
        let token = login_token(&app, "new-secret").await?;
        assert!(token.starts_with(super::SESSION_TOKEN_PREFIX));
        Ok(())
    }

    #[tokio::test]
    async fn api_prefix_preserves_admin_envelope() -> Result<(), Box<dyn std::error::Error>> {
        let response = super::router_with_api_prefix(test_state()?)
            .oneshot(authed_request(Method::GET, "/api/status", None::<&()>)?)
            .await?;

        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await?;
        assert_eq!(body["ok"], true);
        assert_eq!(
            body["data"]["server_version"],
            crate::compat::SERVER_VERSION
        );
        Ok(())
    }

    #[test]
    fn phase52_openapi_contract_matches_operation_registry()
    -> Result<(), Box<dyn std::error::Error>> {
        let spec: Value = serde_json::from_str(super::CONTROL_PLANE_OPENAPI_JSON)?;
        assert_eq!(spec["openapi"], "3.1.0");
        assert_eq!(spec["info"]["version"], "1.0.0");
        assert_eq!(spec["x-multidb-api-version"], 1);

        let paths = spec["paths"]
            .as_object()
            .ok_or("OpenAPI paths must be an object")?;
        for operation in super::control_plane_operations() {
            let path_item = paths
                .get(operation.path)
                .ok_or_else(|| format!("missing OpenAPI path {}", operation.path))?;
            let method = operation.method.to_ascii_lowercase();
            let operation_spec = resolve_openapi_operation(&spec, &path_item[&method])?;
            assert_eq!(
                operation_spec["operationId"], operation.operation_id,
                "{} {} operationId drifted",
                operation.method, operation.path
            );
            assert_eq!(
                operation_spec["x-multidb-stability"], operation.stability,
                "{} {} stability drifted",
                operation.method, operation.path
            );
            let security = operation_spec["security"]
                .as_array()
                .ok_or_else(|| format!("missing security for {}", operation.operation_id))?;
            assert_eq!(
                !security.is_empty(),
                operation.auth_required,
                "{} {} auth requirement drifted",
                operation.method,
                operation.path
            );
        }

        assert_eq!(
            spec["components"]["examples"]["SuccessEnvelope"]["value"]["ok"],
            true
        );
        assert_eq!(
            spec["components"]["examples"]["ErrorEnvelope"]["value"]["ok"],
            false
        );
        assert!(
            spec["paths"]["/metrics"]["get"]["responses"]["200"]["content"]
                .as_object()
                .ok_or("metrics response must declare content")?
                .contains_key("text/plain")
        );
        Ok(())
    }

    #[tokio::test]
    async fn phase52_openapi_is_public_and_available_with_api_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let response = super::router(test_state()?)
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/openapi.json")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await?;
        assert_eq!(body["info"]["title"], "MultiDB Control Plane API");

        let prefixed = super::router_with_api_prefix(test_state()?)
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/openapi.json")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(prefixed.status(), StatusCode::OK);
        Ok(())
    }

    #[tokio::test]
    async fn phase52_raw_probe_and_metrics_contracts_are_not_enveloped()
    -> Result<(), Box<dyn std::error::Error>> {
        let app = super::router(test_state()?);
        let health = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/health")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(health.status(), StatusCode::OK);
        let body = json_body(health).await?;
        assert_eq!(body["ok"], true);
        assert!(body.get("data").is_none());

        let metrics = app
            .oneshot(authed_request(Method::GET, "/metrics", None::<&()>)?)
            .await?;
        assert_eq!(metrics.status(), StatusCode::OK);
        let content_type = metrics
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        assert!(content_type.starts_with("text/plain"));
        Ok(())
    }

    #[tokio::test]
    async fn phase52_studio_manifest_reports_openapi_and_operations()
    -> Result<(), Box<dyn std::error::Error>> {
        let response = super::router(test_state()?)
            .oneshot(authed_request(Method::GET, "/studio", None::<&()>)?)
            .await?;

        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await?;
        assert_eq!(body["data"]["openapi_endpoint"], "/openapi.json");
        assert!(
            body["data"]["capabilities"]
                .as_array()
                .is_some_and(|items| items.iter().any(|item| item == "openapi_v1"))
        );
        assert_eq!(
            body["data"]["operations"]
                .as_array()
                .ok_or("operations must be an array")?
                .len(),
            super::control_plane_operations().len()
        );
        assert!(body["data"]["operations"].as_array().is_some_and(|items| {
            items
                .iter()
                .any(|item| item["operation_id"] == "getMetrics" && item["stability"] == "preview")
        }));
        Ok(())
    }

    #[tokio::test]
    async fn phase52_admin_error_contracts_cover_public_statuses()
    -> Result<(), Box<dyn std::error::Error>> {
        let unauthorized = super::router(test_state()?)
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/status")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        let body = json_body(unauthorized).await?;
        assert_eq!(body["ok"], false);
        assert_eq!(body["error"]["code"], "missing_admin_token");

        let not_found = super::router(test_state()?)
            .oneshot(authed_request(Method::GET, "/missing", None::<&()>)?)
            .await?;
        assert_eq!(not_found.status(), StatusCode::NOT_FOUND);
        let bytes = to_bytes(not_found.into_body(), 1024).await?;
        assert!(bytes.is_empty());

        let mut invalid = DatabaseSpec::from_db_config(
            "invalid",
            &DbConfig::on_disk(Profile::Transactional, "invalid.redb"),
        );
        invalid.guarantees.write_ack = crate::config_spec::WriteAck::Local;
        let unprocessable = super::router(test_state()?)
            .oneshot(authed_request(
                Method::POST,
                "/config/validate",
                Some(&invalid),
            )?)
            .await?;
        assert_eq!(unprocessable.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = json_body(unprocessable).await?;
        assert_eq!(body["ok"], true);
        assert_eq!(body["data"]["valid"], false);

        let mut database = admin_database()?;
        database.set_authz_policy(AuthzPolicy::new([
            Role::new("reader").grant(Resource::Database, Permission::Read)
        ]));
        database.set_principal_registry(
            PrincipalRegistry::new()
                .with_principal("root", Principal::new("root").with_role("reader")),
        );
        let forbidden =
            super::router(AdminState::from_database(database).with_admin_token("secret"))
                .oneshot(authed_request(
                    Method::POST,
                    "/builder/table",
                    Some(&serde_json::json!({
                        "name": "blocked",
                        "schema": null,
                        "indexes": []
                    })),
                )?)
                .await?;
        assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);
        let body = json_body(forbidden).await?;
        assert_eq!(body["ok"], false);
        assert_eq!(body["error"]["code"], "forbidden");

        let internal = super::router(AdminState::from_database(admin_database()?))
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/status")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(internal.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = json_body(internal).await?;
        assert_eq!(body["ok"], false);
        assert_eq!(body["error"]["code"], "admin_auth_not_configured");

        Ok(())
    }

    #[tokio::test]
    async fn studio_router_serves_static_index_and_keeps_api_404s_jsonless()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        fs::write(
            temp.path().join("index.html"),
            "<!doctype html><div id=\"root\"></div>",
        )?;
        fs::create_dir(temp.path().join("assets"))?;
        fs::write(
            temp.path().join("assets").join("app.js"),
            "console.log('ok');",
        )?;

        let app = super::router_with_studio(test_state()?.with_studio_assets_dir(temp.path()));
        let index = app
            .clone()
            .oneshot(Request::builder().uri("/").body(Body::empty())?)
            .await?;
        assert_eq!(index.status(), StatusCode::OK);
        let bytes = to_bytes(index.into_body(), 1024 * 1024).await?;
        assert!(String::from_utf8(bytes.to_vec())?.contains("root"));

        let asset = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/assets/app.js")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(asset.status(), StatusCode::OK);
        assert_eq!(
            asset
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("text/javascript; charset=utf-8")
        );

        let missing_api = app
            .oneshot(
                Request::builder()
                    .uri("/api/not-found")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(missing_api.status(), StatusCode::NOT_FOUND);
        Ok(())
    }

    #[tokio::test]
    async fn invalid_json_request_returns_error_envelope() -> Result<(), Box<dyn std::error::Error>>
    {
        let response = super::router(test_state()?)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/config/validate")
                    .header(AUTHORIZATION, "Bearer secret")
                    .header("x-multidb-principal", "root")
                    .header("content-type", "application/json")
                    .body(Body::from("{not-valid-json"))?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = json_body(response).await?;
        assert_eq!(body["ok"], false);
        assert_eq!(body["error"]["code"], "invalid_json");
        Ok(())
    }

    #[tokio::test]
    async fn catalog_endpoints_return_enveloped_json() -> Result<(), Box<dyn std::error::Error>> {
        for path in [
            "/config",
            "/profiles",
            "/roles",
            "/domains",
            "/extensions",
            "/advice",
            "/studio",
        ] {
            let response = super::router(test_state()?)
                .oneshot(authed_request(Method::GET, path, None::<&()>)?)
                .await?;
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            let body = json_body(response).await?;
            assert_eq!(body["ok"], true, "{path}");
            assert!(body.get("data").is_some(), "{path}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn advice_endpoint_returns_runtime_advisor_report()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(Mutex::new(admin_database()?));
        {
            let database = database.lock().await;
            database.record_workload_sample(
                &WorkloadSample::new("SELECT * FROM users WHERE age = 37")
                    .with_observed_rows(1, 2_000)
                    .with_access("users", "age"),
            )?;
        }
        let response = super::router(
            AdminState::from_database_handle(Arc::clone(&database)).with_admin_token("secret"),
        )
        .oneshot(authed_request(Method::GET, "/advice", None::<&()>)?)
        .await?;

        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await?;
        assert_eq!(body["ok"], true);
        assert_eq!(body["data"]["schema_version"], 1);
        assert_eq!(body["data"]["auto_apply_enabled"], false);
        assert!(
            body["data"]["recommendations"]
                .as_array()
                .is_some_and(|recommendations| recommendations
                    .iter()
                    .any(|advice| advice["code"] == "CREATE_INDEX"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn advice_plan_and_decision_are_enveloped_and_audited()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(Mutex::new(admin_database()?));
        let advice = {
            let database = database.lock().await;
            database.record_workload_sample(
                &WorkloadSample::new("SELECT * FROM users WHERE age = 37")
                    .with_observed_rows(1, 2_000)
                    .with_access("users", "age"),
            )?;
            database.runtime_advice()?
        };
        let advice_id = advice
            .recommendations
            .iter()
            .find(|advice| advice.code == "CREATE_INDEX")
            .ok_or("missing advice")?
            .id
            .clone();
        let app = super::router(
            AdminState::from_database_handle(Arc::clone(&database)).with_admin_token("secret"),
        );

        let plan_response = app
            .clone()
            .oneshot(authed_request(
                Method::POST,
                "/advice/plan",
                Some(&RuntimeAdvicePlanRequest {
                    advice_id: advice_id.clone(),
                }),
            )?)
            .await?;
        assert_eq!(plan_response.status(), StatusCode::OK);
        let plan_body = json_body(plan_response).await?;
        assert_eq!(plan_body["ok"], true);
        assert_eq!(plan_body["data"]["valid"], true);

        let decision_response = app
            .oneshot(authed_request(
                Method::POST,
                "/advice/decision",
                Some(&RuntimeAdviceDecisionRequest {
                    advice_id,
                    status: RuntimeAdviceStatus::Rejected,
                    reason: "operator declined".to_owned(),
                }),
            )?)
            .await?;
        assert_eq!(decision_response.status(), StatusCode::OK);
        let decision_body = json_body(decision_response).await?;
        assert_eq!(decision_body["data"]["status"], "rejected");
        let database = database.lock().await;
        assert!(database.audit_events()?.iter().any(|event| {
            event.action == "runtime_advice_decision"
                && event.outcome == crate::security::AuditOutcome::Succeeded
        }));
        Ok(())
    }

    #[tokio::test]
    async fn extensions_endpoint_returns_full_manifest_catalog()
    -> Result<(), Box<dyn std::error::Error>> {
        let response = super::router(test_state()?)
            .oneshot(authed_request(Method::GET, "/extensions", None::<&()>)?)
            .await?;

        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await?;
        let extensions = body["data"]
            .as_array()
            .ok_or_else(|| "extensions data must be an array".to_owned())?;
        let vector = extensions
            .iter()
            .find(|entry| entry["slug"] == "vector_hnsw")
            .ok_or_else(|| "missing vector_hnsw extension".to_owned())?;

        assert_eq!(vector["status"], "stable");
        assert_eq!(vector["manifest"]["name"], "vector_hnsw");
        assert_eq!(vector["manifest"]["core_boundary"]["wal"], "core_owned");
        assert!(
            vector["manifest"]["registries"]["indexes"]
                .as_array()
                .is_some_and(|indexes| indexes.iter().any(|entry| entry["id"] == "hnsw"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn validate_and_plan_map_invalid_reports_to_422() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut invalid = DatabaseSpec::from_db_config(
            "invalid",
            &DbConfig::on_disk(Profile::Transactional, "invalid.redb"),
        );
        invalid.guarantees.write_ack = crate::config_spec::WriteAck::Local;

        let validate = super::router(test_state()?)
            .oneshot(authed_request(
                Method::POST,
                "/config/validate",
                Some(&invalid),
            )?)
            .await?;
        assert_eq!(validate.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let current = DatabaseSpec::from_db_config(
            "current",
            &DbConfig::on_disk(Profile::Balanced, "current.redb"),
        );
        let plan = super::router(test_state()?)
            .oneshot(authed_request(
                Method::POST,
                "/config/plan",
                Some(&ConfigPlanRequest {
                    current,
                    desired: invalid,
                }),
            )?)
            .await?;
        assert_eq!(plan.status(), StatusCode::UNPROCESSABLE_ENTITY);
        Ok(())
    }

    #[tokio::test]
    async fn apply_confirms_and_audits_without_data_mutation()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(Mutex::new(admin_database()?));
        let current = {
            let database = database.lock().await;
            DatabaseSpec::from_db_config("current", database.config())
        };
        let plan = MigrationPlanner::plan(&current, &current);
        let response = super::router(
            AdminState::from_database_handle(Arc::clone(&database)).with_admin_token("secret"),
        )
        .oneshot(authed_request(
            Method::POST,
            "/config/apply",
            Some(&ConfigApplyRequest {
                confirm: plan.required_confirmation.clone(),
                plan,
            }),
        )?)
        .await?;

        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await?;
        assert_eq!(body["data"]["status"], "confirmed");
        assert_eq!(body["data"]["audit_recorded"], true);
        assert_eq!(body["data"]["data_mutated"], false);
        let database = database.lock().await;
        let events = database.audit_events()?;
        assert!(events.iter().any(|event| {
            event.action == "config_apply"
                && event.outcome == crate::security::AuditOutcome::Succeeded
        }));
        Ok(())
    }

    #[tokio::test]
    async fn apply_unsupported_plan_is_422_audited_noop() -> Result<(), Box<dyn std::error::Error>>
    {
        let database = Arc::new(Mutex::new(admin_database()?));
        let current = {
            let database = database.lock().await;
            DatabaseSpec::from_db_config("current", database.config())
        };
        let mut desired = current.clone();
        desired.profile = "secure_app".to_owned();
        desired.domains[0].mode = crate::config_spec::ConsistencyMode::StrongCp;
        let plan = MigrationPlanner::plan(&current, &desired);
        assert!(!plan.apply_supported);

        let response = super::router(
            AdminState::from_database_handle(Arc::clone(&database)).with_admin_token("secret"),
        )
        .oneshot(authed_request(
            Method::POST,
            "/config/apply",
            Some(&ConfigApplyRequest {
                confirm: plan.required_confirmation.clone(),
                plan,
            }),
        )?)
        .await?;

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = json_body(response).await?;
        assert_eq!(body["data"]["status"], "unsupported");
        assert_eq!(body["data"]["audit_recorded"], true);
        assert_eq!(body["data"]["data_mutated"], false);
        let database = database.lock().await;
        assert!(database.audit_events()?.iter().any(|event| {
            event.action == "config_apply" && event.outcome == crate::security::AuditOutcome::Failed
        }));
        Ok(())
    }

    #[tokio::test]
    async fn apply_requires_system_admin_principal() -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(Mutex::new(admin_database()?));
        let current = {
            let database = database.lock().await;
            DatabaseSpec::from_db_config("current", database.config())
        };
        let plan = MigrationPlanner::plan(&current, &current);
        let response = super::router(
            AdminState::from_database_handle(Arc::clone(&database)).with_admin_token("secret"),
        )
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/config/apply")
                .header(AUTHORIZATION, "Bearer secret")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&ConfigApplyRequest {
                    confirm: plan.required_confirmation.clone(),
                    plan,
                })?))?,
        )
        .await?;

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let database = database.lock().await;
        assert!(database.audit_events()?.iter().any(|event| {
            event.action == "config_apply" && event.outcome == crate::security::AuditOutcome::Denied
        }));
        Ok(())
    }

    #[tokio::test]
    async fn auth_me_and_insecure_bootstrap_allow_local_crud()
    -> Result<(), Box<dyn std::error::Error>> {
        let state = AdminState::from_database(create_database(DbConfig::new(Profile::InMemory))?)
            .with_admin_token("secret")
            .with_insecure_local_admin();
        let app = super::router(state);

        let me = app
            .clone()
            .oneshot(authed_request(Method::GET, "/auth/me", None::<&()>)?)
            .await?;
        assert_eq!(me.status(), StatusCode::OK);
        let body = json_body(me).await?;
        assert_eq!(body["data"]["principal"], "root");
        assert_eq!(body["data"]["system_admin"], true);
        assert_eq!(body["data"]["database_admin"], true);

        let create = app
            .clone()
            .oneshot(authed_request(
                Method::POST,
                "/builder/table",
                Some(&serde_json::json!({
                    "name": "users",
                    "schema": {
                        "columns": [
                            { "name": "id", "ty": "Int", "nullable": false },
                            { "name": "name", "ty": "Str", "nullable": false }
                        ],
                        "primary_key": 0
                    },
                    "indexes": []
                })),
            )?)
            .await?;
        assert_eq!(create.status(), StatusCode::OK);

        let write = app
            .oneshot(authed_request(
                Method::POST,
                "/data/tables/users/rows",
                Some(&serde_json::json!({ "row": [1, "Ada"] })),
            )?)
            .await?;
        assert_eq!(write.status(), StatusCode::OK);
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn table_sql_and_document_crud_use_plain_json() -> Result<(), Box<dyn std::error::Error>>
    {
        let app = super::router(test_state()?);

        let create_table = app
            .clone()
            .oneshot(authed_request(
                Method::POST,
                "/builder/table",
                Some(&serde_json::json!({
                    "name": "users",
                    "schema": {
                        "columns": [
                            { "name": "id", "ty": "Int", "nullable": false },
                            { "name": "name", "ty": "Str", "nullable": false }
                        ],
                        "primary_key": 0
                    },
                    "indexes": []
                })),
            )?)
            .await?;
        assert_eq!(create_table.status(), StatusCode::OK);

        let insert = app
            .clone()
            .oneshot(authed_request(
                Method::POST,
                "/data/tables/users/rows",
                Some(&serde_json::json!({ "row": [1, "Ada"] })),
            )?)
            .await?;
        assert_eq!(insert.status(), StatusCode::OK);

        let insert_second = app
            .clone()
            .oneshot(authed_request(
                Method::POST,
                "/data/tables/users/rows",
                Some(&serde_json::json!({ "row": [2, "Lin"] })),
            )?)
            .await?;
        assert_eq!(insert_second.status(), StatusCode::OK);

        let update = app
            .clone()
            .oneshot(authed_request(
                Method::PUT,
                "/data/tables/users/rows",
                Some(&serde_json::json!({ "row": [1, "Grace"] })),
            )?)
            .await?;
        assert_eq!(update.status(), StatusCode::OK);

        let rows = app
            .clone()
            .oneshot(authed_request(
                Method::GET,
                "/data/tables/users/rows",
                None::<&()>,
            )?)
            .await?;
        assert_eq!(rows.status(), StatusCode::OK);
        let body = json_body(rows).await?;
        assert_eq!(body["data"]["rows"][0], serde_json::json!([1, "Grace"]));
        assert_eq!(body["data"]["limit"], serde_json::json!(100));
        assert_eq!(body["data"]["offset"], serde_json::json!(0));
        assert_eq!(body["data"]["has_more"], serde_json::json!(false));

        let paged_rows = app
            .clone()
            .oneshot(authed_request(
                Method::GET,
                "/data/tables/users/rows?limit=1&offset=1",
                None::<&()>,
            )?)
            .await?;
        assert_eq!(paged_rows.status(), StatusCode::OK);
        let body = json_body(paged_rows).await?;
        assert_eq!(body["data"]["rows"], serde_json::json!([[2, "Lin"]]));
        assert_eq!(body["data"]["returned"], serde_json::json!(1));

        let sql = app
            .clone()
            .oneshot(authed_request(
                Method::POST,
                "/sql",
                Some(&serde_json::json!({ "sql": "SELECT * FROM users" })),
            )?)
            .await?;
        assert_eq!(sql.status(), StatusCode::OK);
        let body = json_body(sql).await?;
        assert_eq!(body["data"]["output"]["kind"], "rows");

        let blocked_delete = app
            .clone()
            .oneshot(authed_request(
                Method::DELETE,
                "/data/tables/users/rows",
                Some(&serde_json::json!({ "primary_key": 1, "confirm": "wrong" })),
            )?)
            .await?;
        assert_eq!(blocked_delete.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let delete = app
            .clone()
            .oneshot(authed_request(
                Method::DELETE,
                "/data/tables/users/rows",
                Some(&serde_json::json!({ "primary_key": 1, "confirm": "users" })),
            )?)
            .await?;
        assert_eq!(delete.status(), StatusCode::OK);

        let create_collection = app
            .clone()
            .oneshot(authed_request(
                Method::POST,
                "/builder/collection",
                Some(&serde_json::json!({
                    "name": "docs",
                    "fields": [{ "name": "id", "source": "DocumentId", "ty": "Bytes" }],
                    "indexes": []
                })),
            )?)
            .await?;
        assert_eq!(create_collection.status(), StatusCode::OK);

        let create_doc = app
            .clone()
            .oneshot(authed_request(
                Method::POST,
                "/data/collections/docs/documents",
                Some(&serde_json::json!({ "document": { "name": "Ada" } })),
            )?)
            .await?;
        assert_eq!(create_doc.status(), StatusCode::OK);
        let body = json_body(create_doc).await?;
        let id = body["data"]["id"].as_str().ok_or("missing document id")?;

        let second_doc = app
            .clone()
            .oneshot(authed_request(
                Method::POST,
                "/data/collections/docs/documents",
                Some(&serde_json::json!({ "document": { "name": "Lin" } })),
            )?)
            .await?;
        assert_eq!(second_doc.status(), StatusCode::OK);

        let update_doc = app
            .clone()
            .oneshot(authed_request(
                Method::PUT,
                &format!("/data/collections/docs/documents/{id}"),
                Some(&serde_json::json!({ "document": { "name": "Grace" } })),
            )?)
            .await?;
        assert_eq!(update_doc.status(), StatusCode::OK);

        let docs = app
            .clone()
            .oneshot(authed_request(
                Method::GET,
                "/data/collections/docs/documents",
                None::<&()>,
            )?)
            .await?;
        assert_eq!(docs.status(), StatusCode::OK);
        let body = json_body(docs).await?;
        assert_eq!(body["data"]["documents"][0]["document"]["name"], "Grace");
        assert_eq!(body["data"]["limit"], serde_json::json!(100));

        let paged_docs = app
            .clone()
            .oneshot(authed_request(
                Method::GET,
                "/data/collections/docs/documents?limit=1",
                None::<&()>,
            )?)
            .await?;
        assert_eq!(paged_docs.status(), StatusCode::OK);
        let body = json_body(paged_docs).await?;
        assert_eq!(body["data"]["returned"], serde_json::json!(1));
        assert_eq!(body["data"]["has_more"], serde_json::json!(true));
        assert_eq!(body["data"]["next_offset"], serde_json::json!(1));

        let capped_docs = app
            .clone()
            .oneshot(authed_request(
                Method::GET,
                "/data/collections/docs/documents?limit=999999",
                None::<&()>,
            )?)
            .await?;
        assert_eq!(capped_docs.status(), StatusCode::OK);
        let body = json_body(capped_docs).await?;
        assert_eq!(body["data"]["limit"], serde_json::json!(1_000));
        assert_eq!(body["data"]["capped"], serde_json::json!(true));

        let blocked_doc_delete = app
            .clone()
            .oneshot(authed_request(
                Method::DELETE,
                &format!("/data/collections/docs/documents/{id}"),
                Some(&serde_json::json!({ "confirm": "wrong" })),
            )?)
            .await?;
        assert_eq!(
            blocked_doc_delete.status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );

        let delete_doc = app
            .oneshot(authed_request(
                Method::DELETE,
                &format!("/data/collections/docs/documents/{id}"),
                Some(&serde_json::json!({ "confirm": id })),
            )?)
            .await?;
        assert_eq!(delete_doc.status(), StatusCode::OK);
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn multimodel_builders_vector_and_time_series_work()
    -> Result<(), Box<dyn std::error::Error>> {
        let app = super::router(test_state()?);

        for (path, body) in [
            (
                "/builder/collection",
                serde_json::json!({
                    "name": "posts",
                    "collection_id": 77,
                    "fields": [{ "name": "id", "source": "DocumentId", "ty": "Bytes" }],
                    "indexes": []
                }),
            ),
            (
                "/builder/vector",
                serde_json::json!({
                    "name": "embeddings",
                    "dim": 3,
                    "metric": "Cosine",
                    "hnsw": { "m": 16, "ef_construction": 200, "ef_search": 48 }
                }),
            ),
            (
                "/builder/time-series",
                serde_json::json!({
                    "name": "metrics",
                    "chunk_millis": 60000,
                    "retention_millis": null
                }),
            ),
            (
                "/builder/full-text",
                serde_json::json!({
                    "name": "posts_text",
                    "collection_id": 77,
                    "path": ["body"],
                    "language": "simple",
                    "refresh_lag_target": 1
                }),
            ),
            (
                "/builder/geo",
                serde_json::json!({
                    "name": "posts_geo",
                    "collection_id": 77,
                    "path": ["location"],
                    "precision": 6
                }),
            ),
            (
                "/builder/graph",
                serde_json::json!({ "name": "social", "graph_id": 41 }),
            ),
        ] {
            let response = app
                .clone()
                .oneshot(authed_request(Method::POST, path, Some(&body))?)
                .await?;
            assert_eq!(response.status(), StatusCode::OK, "{path}");
        }

        let insert_vector = app
            .clone()
            .oneshot(authed_request(
                Method::POST,
                "/data/vectors/embeddings/vectors",
                Some(&serde_json::json!({
                    "metadata": { "label": "Ada" },
                    "vector": [1.0, 0.0, 0.0]
                })),
            )?)
            .await?;
        assert_eq!(insert_vector.status(), StatusCode::OK);

        let search = app
            .clone()
            .oneshot(authed_request(
                Method::POST,
                "/data/vectors/embeddings/search",
                Some(&serde_json::json!({ "vector": [1.0, 0.0, 0.0], "k": 1 })),
            )?)
            .await?;
        assert_eq!(search.status(), StatusCode::OK);
        let body = json_body(search).await?;
        assert_eq!(body["data"]["hits"][0]["metadata"]["label"], "Ada");

        let insert_point = app
            .clone()
            .oneshot(authed_request(
                Method::POST,
                "/data/time-series/metrics/points",
                Some(&serde_json::json!({
                    "series": "cpu",
                    "point": { "timestamp_millis": 1000, "value": 0.7 }
                })),
            )?)
            .await?;
        assert_eq!(insert_point.status(), StatusCode::OK);

        let points = app
            .clone()
            .oneshot(authed_request(
                Method::GET,
                "/data/time-series/metrics/points?series=cpu&start=0&end=2000",
                None::<&()>,
            )?)
            .await?;
        assert_eq!(points.status(), StatusCode::OK);
        let body = json_body(points).await?;
        assert_eq!(body["data"]["points"][0]["value"], 0.7);

        let catalog = app
            .oneshot(authed_request(Method::GET, "/catalog", None::<&()>)?)
            .await?;
        assert_eq!(catalog.status(), StatusCode::OK);
        let body = json_body(catalog).await?;
        let kinds: Vec<_> = body["data"]["objects"]
            .as_array()
            .ok_or("catalog objects must be an array")?
            .iter()
            .map(|object| object["kind"].as_str().unwrap_or_default())
            .collect();
        assert!(kinds.contains(&"vector"));
        assert!(kinds.contains(&"time_series"));
        assert!(kinds.contains(&"full_text_index"));
        assert!(kinds.contains(&"geo_index"));
        assert!(kinds.contains(&"graph"));
        Ok(())
    }

    #[tokio::test]
    async fn rbac_denies_and_audits_builder_without_database_admin()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.set_authz_policy(AuthzPolicy::new([
            Role::new("reader").grant(Resource::Database, Permission::Read)
        ]));
        database.set_principal_registry(
            PrincipalRegistry::new()
                .with_principal("root", Principal::new("root").with_role("reader")),
        );
        let database = Arc::new(Mutex::new(database));
        let response = super::router(
            AdminState::from_database_handle(Arc::clone(&database)).with_admin_token("secret"),
        )
        .oneshot(authed_request(
            Method::POST,
            "/builder/table",
            Some(&serde_json::json!({
                "name": "blocked",
                "schema": null,
                "indexes": []
            })),
        )?)
        .await?;

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let database = database.lock().await;
        assert!(database.audit_events()?.iter().any(|event| {
            event.action == "create_table" && event.outcome == crate::security::AuditOutcome::Denied
        }));
        Ok(())
    }

    fn test_state() -> Result<AdminState, Box<dyn std::error::Error>> {
        Ok(AdminState::from_database(admin_database()?).with_admin_token("secret"))
    }

    fn admin_database() -> Result<crate::db::Database, Box<dyn std::error::Error>> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.set_authz_policy(AuthzPolicy::new([Role::new("admin")
            .grant(Resource::System, Permission::Admin)
            .grant(Resource::Database, Permission::Read)
            .grant(Resource::Database, Permission::Write)
            .grant(Resource::Database, Permission::Admin)]));
        database.set_principal_registry(
            PrincipalRegistry::new()
                .with_principal("root", Principal::new("root").with_role("admin")),
        );
        Ok(database)
    }

    fn authed_request<T: Serialize>(
        method: Method,
        uri: &str,
        body: Option<&T>,
    ) -> Result<Request<Body>, Box<dyn std::error::Error>> {
        let mut builder = Request::builder()
            .method(method)
            .uri(uri)
            .header(AUTHORIZATION, "Bearer secret")
            .header("x-multidb-principal", "root");
        let body = if let Some(body) = body {
            builder = builder.header("content-type", "application/json");
            Body::from(serde_json::to_vec(body)?)
        } else {
            Body::empty()
        };
        Ok(builder.body(body)?)
    }

    fn json_request<T: Serialize>(
        method: Method,
        uri: &str,
        body: &T,
    ) -> Result<Request<Body>, Box<dyn std::error::Error>> {
        Ok(Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(body)?))?)
    }

    async fn login_token(
        app: &axum::Router,
        password: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let response = app
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/auth/login",
                &serde_json::json!({
                    "username": "admin",
                    "password": password
                }),
            )?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await?;
        Ok(body["data"]["token"]
            .as_str()
            .ok_or("login token must be a string")?
            .to_owned())
    }

    async fn json_body(
        response: axum::response::Response,
    ) -> Result<Value, Box<dyn std::error::Error>> {
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    fn resolve_openapi_operation<'a>(
        spec: &'a Value,
        operation: &'a Value,
    ) -> Result<&'a Value, Box<dyn std::error::Error>> {
        let Some(reference) = operation.get("$ref").and_then(Value::as_str) else {
            return Ok(operation);
        };
        let pointer = reference
            .strip_prefix('#')
            .ok_or_else(|| format!("unsupported OpenAPI reference {reference}"))?;
        spec.pointer(pointer)
            .ok_or_else(|| format!("missing OpenAPI reference target {reference}").into())
    }

    #[test]
    fn apply_status_serializes_as_confirmed() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            serde_json::to_value(ApplyStatus::Confirmed)?,
            serde_json::Value::String("confirmed".to_owned())
        );
        Ok(())
    }
}
