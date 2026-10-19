#![allow(clippy::missing_errors_doc)]

use std::{
    collections::BTreeMap,
    fs,
    net::{IpAddr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex},
    time::{SystemTime, UNIX_EPOCH},
};

use argon2::{
    Algorithm, Argon2, Params, PasswordHash, PasswordHasher, PasswordVerifier, Version,
    password_hash::SaltString,
};
use axum::{
    Extension, Json, Router,
    extract::{
        ConnectInfo, DefaultBodyLimit, OriginalUri, Path as AxumPath, Query as AxumQuery, State,
        rejection::JsonRejection,
    },
    http::{
        HeaderMap, Method, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE, RETRY_AFTER},
    },
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

use crate::{
    compat::SERVER_VERSION,
    config_spec::{
        ApplyCheckReport, ApplyStatus, DatabaseSpec, GuaranteeValidator, MigrationPlan,
        MigrationPlanner, ProfileKnobOverrides, ProfileKnobs, ValidationReport, ValidationSeverity,
        built_in_profiles, collection_role_definitions, consistency_domain_definition,
        consistency_domain_definitions, extension_catalog_entries, profile_variants,
        resolve_named_profile,
        spec_io::{self, SpecIoError},
    },
    db::{
        AdminCredentialRecord, CatalogEntry, ConfigError, ConfigPlanSource, Database, DbError,
        PersistedProfileManifest, Profile, ReplicationKind,
    },
    geo::GeoIndexConfig,
    graph::GraphId,
    model::{CollectionId, DocumentId, FieldPath, IndexId, IndexSpec, Value as ModelValue},
    observability,
    provision::{
        self, Confirmation, ExportSource, PlanOutcome, ProvisionActor, ProvisionApplyReport,
        ProvisionEnvironment, ProvisionError, ProvisionHistoryEntry, ProvisionOptions,
        ProvisionOutcome, ProvisionPlan, ProvisionRecord, ProvisionSurface, ProvisionTargetRef,
        StartupMode, StepOutcome,
    },
    query::{DocField, RelIndexExpression, RelIndexSpec, Row, SqlOutput, TableLayout, TableSchema},
    runtime_advisor::{RuntimeAdviceDecisionRequest, RuntimeAdvicePlanRequest},
    security::{
        AuditEvent, AuditOutcome, AuthzPolicy, Permission, Principal, PrincipalRegistry, Resource,
        Role,
    },
    text::FullTextIndexConfig,
    timeseries::{DEFAULT_MAX_FUTURE_SKEW_MILLIS, TimePoint, TimeSeriesConfig, TimeSeriesError},
    vector::{HnswParams, VectorCollectionConfig, VectorHit, VectorMetric},
};

const MAX_CONTROL_PLANE_BODY_BYTES: usize = 1024 * 1024;
const HEADER_PRINCIPAL: &str = "x-multidb-principal";
const DEFAULT_DATA_PAGE_LIMIT: usize = 100;
const MAX_DATA_PAGE_LIMIT: usize = 1_000;
const ADMIN_USERNAME: &str = "admin";
const ADMIN_ROLE: &str = "admin";
const SESSION_TOKEN_PREFIX: &str = "mda1_session_";
const MAX_LOGIN_USERNAME_BYTES: usize = 256;
const MAX_ADMIN_PASSWORD_BYTES: usize = 1024;
/// Concurrent Argon2id verifications (19 MiB each) allowed across the process.
const ADMIN_PASSWORD_VERIFY_PERMITS: usize = 2;
/// Server-wide soft ceiling on failed admin password checks per window, as a
/// multiple of the per-client `max_failures`.
const ADMIN_LOGIN_GLOBAL_FAILURE_FACTOR: u32 = 20;
const MAX_ADMIN_LOGIN_RATE_KEYS: usize = 65_536;
const AUDIT_LOGIN_USERNAME_CHARS: usize = 64;
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
    /// Failed admin password checks allowed per client IP address (IPv6
    /// addresses are grouped by /64) and username within one window before
    /// that client is locked out. The server-wide soft ceiling is 20 times this
    /// value per window; past it, only clients without failures in the window
    /// may try again.
    pub max_failures: u32,
    /// Window, in seconds, over which failed checks are counted. It starts at
    /// the first failure and resets once it has elapsed.
    pub window_seconds: u64,
    /// How long, in seconds, a client stays locked out once it reaches
    /// `max_failures`.
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
    /// `/provision/*` and `GET /config/live` are served: the served database can be planned,
    /// applied and checked for drift against a spec (preview).
    pub provisioning_supported: bool,
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
        path: "/config/live",
        operation_id: "getLiveConfig",
        auth_required: true,
        stability: "preview",
    },
    ControlPlaneOperation {
        method: "GET",
        path: "/provision/state",
        operation_id: "getProvisionState",
        auth_required: true,
        stability: "preview",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/provision/plan",
        operation_id: "planProvision",
        auth_required: true,
        stability: "preview",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/provision/apply",
        operation_id: "applyProvision",
        auth_required: true,
        stability: "preview",
    },
    ControlPlaneOperation {
        method: "POST",
        path: "/provision/drift",
        operation_id: "checkProvisionDrift",
        auth_required: true,
        stability: "preview",
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

/// Tracks failed admin password checks per client and username.
///
/// Only attempts that reach Argon2 verification are counted, and they are
/// reserved before hashing starts, so concurrent requests cannot outrun the
/// limit. Usernames that cannot authenticate never create entries and never
/// lock anyone out; they only feed a per-window audit cap.
#[derive(Debug)]
struct AdminLoginRateLimiter {
    config: AdminLoginRateLimitConfig,
    global_max_failures: u32,
    max_keys: usize,
    state: StdMutex<AdminLoginRateLimitState>,
}

#[derive(Debug, Default)]
struct AdminLoginRateLimitState {
    keys: BTreeMap<AdminLoginRateKey, AdminLoginRateLimitBucket>,
    /// Soft ceiling: reserved admin password checks across all clients. It
    /// never locks; `limited_audited` marks that the crossing was reported.
    global: AdminLoginRateLimitBucket,
    /// Failed logins for usernames that cannot authenticate; bounds their
    /// audit rows only.
    unknown_users: AdminLoginRateLimitBucket,
    last_prune_millis: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct AdminLoginRateKey {
    /// Canonical client address, IPv6 masked to its /64. `None` when the
    /// server runs without connection info: all such clients share one key.
    client: Option<IpAddr>,
    user_digest: [u8; 32],
}

#[derive(Clone, Debug, Default)]
struct AdminLoginRateLimitBucket {
    failures: u32,
    window_started_millis: u64,
    locked_until_millis: u64,
    limited_audited: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AdminLoginRateDecision {
    Allowed { ceiling_crossed: bool },
    Limited { audit: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AdminLoginAuditDecision {
    Event,
    Coalesced,
    Skip,
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

impl AdminLoginRateKey {
    fn new(client: Option<IpAddr>, username: &str) -> Self {
        let username = username.trim().to_ascii_lowercase();
        Self {
            client: client.map(login_rate_client),
            user_digest: *blake3::hash(username.as_bytes()).as_bytes(),
        }
    }
}

impl AdminLoginRateLimiter {
    fn new(config: AdminLoginRateLimitConfig) -> Self {
        Self {
            config,
            global_max_failures: config
                .max_failures
                .saturating_mul(ADMIN_LOGIN_GLOBAL_FAILURE_FACTOR),
            max_keys: MAX_ADMIN_LOGIN_RATE_KEYS,
            state: StdMutex::new(AdminLoginRateLimitState::default()),
        }
    }

    #[cfg(test)]
    fn with_limits(
        config: AdminLoginRateLimitConfig,
        max_keys: usize,
        global_max_failures: u32,
    ) -> Self {
        Self {
            max_keys: max_keys.max(1),
            global_max_failures: global_max_failures.max(1),
            ..Self::new(config)
        }
    }

    #[cfg(test)]
    fn tracked_keys(&self) -> usize {
        self.state.lock().map_or(0, |state| state.keys.len())
    }

    fn check(&self, key: &AdminLoginRateKey) -> AdminLoginRateDecision {
        self.check_at(key, now_millis())
    }

    fn reserve(&self, key: &AdminLoginRateKey) -> AdminLoginRateDecision {
        self.reserve_at(key, now_millis())
    }

    fn record_success(&self, key: &AdminLoginRateKey) {
        self.record_success_at(key, now_millis());
    }

    fn record_unknown_user(&self) -> AdminLoginAuditDecision {
        self.record_unknown_user_at(now_millis())
    }

    /// Reports whether `key` may attempt a password check, without counting.
    fn check_at(&self, key: &AdminLoginRateKey, now: u64) -> AdminLoginRateDecision {
        let Ok(mut state) = self.state.lock() else {
            tracing::warn!("admin login rate limiter lock poisoned; refusing login");
            return AdminLoginRateDecision::Limited { audit: false };
        };
        self.prune_if_due(&mut state, now);
        match self.refusal(&mut state, key, now) {
            Some(audit) => AdminLoginRateDecision::Limited { audit },
            None => AdminLoginRateDecision::Allowed {
                ceiling_crossed: false,
            },
        }
    }

    /// Re-checks `key` and, when allowed, counts the attempt as a failure up
    /// front. A later `record_success` takes it back.
    fn reserve_at(&self, key: &AdminLoginRateKey, now: u64) -> AdminLoginRateDecision {
        let Ok(mut state) = self.state.lock() else {
            tracing::warn!("admin login rate limiter lock poisoned; refusing login");
            return AdminLoginRateDecision::Limited { audit: false };
        };
        self.prune_if_due(&mut state, now);
        if let Some(audit) = self.refusal(&mut state, key, now) {
            return AdminLoginRateDecision::Limited { audit };
        }
        if !state.keys.contains_key(key) && state.keys.len() >= self.max_keys {
            self.prune(&mut state, now);
            if state.keys.len() >= self.max_keys {
                evict_admin_login_rate_key(&mut state.keys, now);
            }
        }

        let bucket = state.keys.entry(*key).or_default();
        bucket.count_failure(now);
        bucket.limited_audited = false;
        if bucket.failures >= self.config.max_failures {
            bucket.locked_until_millis =
                now.saturating_add(seconds_to_millis(self.config.lockout_seconds));
        }

        let global = &mut state.global;
        global.count_failure(now);
        let ceiling_crossed =
            global.failures >= self.global_max_failures && !global.limited_audited;
        if ceiling_crossed {
            global.limited_audited = true;
        }
        AdminLoginRateDecision::Allowed { ceiling_crossed }
    }

    fn record_success_at(&self, key: &AdminLoginRateKey, now: u64) {
        let Ok(mut state) = self.state.lock() else {
            tracing::warn!("admin login rate limiter lock poisoned; success not recorded");
            return;
        };
        state.keys.remove(key);
        state.global.normalize(now, self.config.window_seconds);
        state.global.failures = state.global.failures.saturating_sub(1);
    }

    /// Counts a failed login for a username that cannot authenticate and
    /// decides whether it still gets its own audit event in this window.
    fn record_unknown_user_at(&self, now: u64) -> AdminLoginAuditDecision {
        let Ok(mut state) = self.state.lock() else {
            return AdminLoginAuditDecision::Event;
        };
        let bucket = &mut state.unknown_users;
        bucket.normalize(now, self.config.window_seconds);
        bucket.count_failure(now);
        if bucket.failures <= self.global_max_failures {
            AdminLoginAuditDecision::Event
        } else if bucket.limited_audited {
            AdminLoginAuditDecision::Skip
        } else {
            bucket.limited_audited = true;
            AdminLoginAuditDecision::Coalesced
        }
    }

    /// Returns `Some(audit)` when `key` must be refused; `audit` is true only
    /// for the first refusal of the key in its current lockout or window.
    fn refusal(
        &self,
        state: &mut AdminLoginRateLimitState,
        key: &AdminLoginRateKey,
        now: u64,
    ) -> Option<bool> {
        state.global.normalize(now, self.config.window_seconds);
        let ceiling_reached = state.global.failures >= self.global_max_failures;
        let bucket = state.keys.get_mut(key)?;
        bucket.normalize(now, self.config.window_seconds);
        if bucket.is_empty() {
            state.keys.remove(key);
            return None;
        }
        let limited = bucket.locked_until_millis > now || (ceiling_reached && bucket.failures > 0);
        if !limited {
            return None;
        }
        let audit = !bucket.limited_audited;
        bucket.limited_audited = true;
        Some(audit)
    }

    fn prune_if_due(&self, state: &mut AdminLoginRateLimitState, now: u64) {
        if now.saturating_sub(state.last_prune_millis)
            > seconds_to_millis(self.config.window_seconds)
        {
            self.prune(state, now);
        }
    }

    fn prune(&self, state: &mut AdminLoginRateLimitState, now: u64) {
        let window_seconds = self.config.window_seconds;
        state.keys.retain(|_, bucket| {
            bucket.normalize(now, window_seconds);
            !bucket.is_empty()
        });
        state.last_prune_millis = now;
    }
}

/// Drops one entry from a full map, preferring unlocked entries and then the
/// oldest window, so active lockouts survive.
fn evict_admin_login_rate_key(
    keys: &mut BTreeMap<AdminLoginRateKey, AdminLoginRateLimitBucket>,
    now: u64,
) {
    let victim = keys
        .iter()
        .min_by_key(|(_, bucket)| {
            (
                bucket.locked_until_millis > now,
                bucket.window_started_millis,
            )
        })
        .map(|(key, _)| *key);
    if let Some(victim) = victim {
        keys.remove(&victim);
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

    fn count_failure(&mut self, now: u64) {
        if self.window_started_millis == 0 {
            self.window_started_millis = now;
        }
        self.failures = self.failures.saturating_add(1);
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
    #[serde(default)]
    pub max_future_skew_millis: Option<i64>,
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

/// The spec `multidb serve` started from in spec mode, as `GET /provision/state` reports it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ServeSpecMode {
    /// `config_spec::spec_digest` of the spec.
    pub spec_digest: String,
    /// `MULTIDB_SPEC_APPLY`: what a start may change.
    pub apply_mode: StartupMode,
}

/// `GET /provision/state`: what provisioning manages in the served database.
#[derive(Clone, Debug, serde::Serialize)]
pub struct ProvisionStateResponse {
    /// The database has a provision record.
    pub provisioned: bool,
    pub record: Option<ProvisionRecord>,
    /// The apply history, oldest first (at most 100 entries).
    pub history: Vec<ProvisionHistoryEntry>,
    /// The persisted profile manifest; `None` for a legacy database.
    pub manifest: Option<PersistedProfileManifest>,
    /// The fingerprint plans of the database are computed against right now.
    pub live_fingerprint: String,
    /// The spec `multidb serve` started from, in spec mode.
    pub spec_mode: Option<ServeSpecMode>,
}

/// `GET /config/live`: the served database described as a spec (`provision::export_spec`).
#[derive(Clone, Debug, serde::Serialize)]
pub struct LiveConfigResponse {
    pub spec: DatabaseSpec,
    pub source: ExportSource,
    pub notes: Vec<String>,
    pub live_fingerprint: String,
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
    admin_password_verifier: Arc<Semaphore>,
    insecure_local_admin: bool,
    studio_assets_dir: Option<Arc<PathBuf>>,
    spec_mode: Option<ServeSpecMode>,
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
            admin_password_verifier: Arc::new(Semaphore::new(ADMIN_PASSWORD_VERIFY_PERMITS)),
            insecure_local_admin: false,
            studio_assets_dir: None,
            spec_mode: None,
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
            admin_password_verifier: Arc::new(Semaphore::new(ADMIN_PASSWORD_VERIFY_PERMITS)),
            insecure_local_admin: false,
            studio_assets_dir: None,
            spec_mode: None,
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
            admin_password_verifier: Arc::new(Semaphore::new(ADMIN_PASSWORD_VERIFY_PERMITS)),
            insecure_local_admin: false,
            studio_assets_dir: None,
            spec_mode: None,
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
    /// Fails when the password is empty or longer than 1024 bytes (even when an
    /// existing credential would be kept), hashing fails, or persistence fails.
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
        if password.len() > MAX_ADMIN_PASSWORD_BYTES {
            return Err(format!(
                "admin password must be at most {MAX_ADMIN_PASSWORD_BYTES} bytes"
            ));
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

    /// Reports the spec `multidb serve` started from in spec mode (`MULTIDB_SPEC_PATH`) in
    /// `GET /provision/state`: its `spec_digest` and the `MULTIDB_SPEC_APPLY` mode.
    #[must_use]
    pub fn with_spec_mode(
        mut self,
        spec_digest: impl Into<String>,
        apply_mode: StartupMode,
    ) -> Self {
        self.spec_mode = Some(ServeSpecMode {
            spec_digest: spec_digest.into(),
            apply_mode,
        });
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

/// Builds the Control Plane API router.
///
/// Serve it with `into_make_service_with_connect_info::<SocketAddr>()` so the
/// login lockout is tracked per client address; without connection info all
/// clients share a single lockout.
pub fn router(state: AdminState) -> Router {
    api_router().with_state(state)
}

/// Builds the Control Plane API router, also mounted under `/api`.
///
/// Serve it with `into_make_service_with_connect_info::<SocketAddr>()` so the
/// login lockout is tracked per client address.
pub fn router_with_api_prefix(state: AdminState) -> Router {
    Router::new()
        .merge(api_router())
        .nest("/api", api_router())
        .with_state(state)
}

/// Builds the Control Plane API router, also mounted under `/api`, with
/// Studio static assets as the fallback.
///
/// Serve it with `into_make_service_with_connect_info::<SocketAddr>()` so the
/// login lockout is tracked per client address.
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
        .route("/config/live", get(config_live_handler))
        .route("/provision/state", get(provision_state_handler))
        .route("/provision/plan", post(provision_plan_handler))
        .route("/provision/apply", post(provision_apply_handler))
        .route("/provision/drift", post(provision_drift_handler))
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
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    payload: Result<Json<LoginRequest>, JsonRejection>,
) -> Response {
    let Json(request) = match payload {
        Ok(payload) => payload,
        Err(error) => return invalid_json(&error),
    };
    if request.username.len() > MAX_LOGIN_USERNAME_BYTES {
        return invalid_json_value(format!(
            "username must be at most {MAX_LOGIN_USERNAME_BYTES} bytes"
        ));
    }
    if request.password.len() > MAX_ADMIN_PASSWORD_BYTES {
        return invalid_json_value(format!(
            "password must be at most {MAX_ADMIN_PASSWORD_BYTES} bytes"
        ));
    }
    let client = login_client_ip(connect_info);
    let username = request.username.trim();
    if username != ADMIN_USERNAME {
        return reject_unknown_login_user(&state, client, username).await;
    }
    let key = AdminLoginRateKey::new(client, username);
    if let AdminLoginRateDecision::Limited { audit } = state.admin_login_rate_limiter.check(&key) {
        if audit {
            audit_login_refused(&state, client).await;
        }
        return unauthorized_auth();
    }
    if state.admin_password_verifier.available_permits() == 0 {
        return auth_busy();
    }
    let credential = {
        let database = state.database.lock().await;
        match database.admin_credential(ADMIN_USERNAME) {
            Ok(credential) => credential,
            Err(error) => return db_error(&error),
        }
    };
    let Some(credential) = credential.filter(|credential| username == credential.username) else {
        return reject_unknown_login_user(&state, client, username).await;
    };

    let Ok(permit) = Arc::clone(&state.admin_password_verifier).try_acquire_owned() else {
        return auth_busy();
    };
    match state.admin_login_rate_limiter.reserve(&key) {
        AdminLoginRateDecision::Allowed { ceiling_crossed } => {
            if ceiling_crossed {
                audit_auth_event(
                    &state,
                    None,
                    "login_rate_limited",
                    AuditOutcome::Denied,
                    Some("scope: global"),
                )
                .await;
            }
        }
        AdminLoginRateDecision::Limited { audit } => {
            drop(permit);
            if audit {
                audit_login_refused(&state, client).await;
            }
            return unauthorized_auth();
        }
    }
    let verified = verify_admin_password(permit, credential.password_hash, request.password)
        .await
        .unwrap_or(false);
    if !verified {
        audit_login_failure(&state, client, username).await;
        return unauthorized_auth();
    }
    state.admin_login_rate_limiter.record_success(&key);
    complete_admin_login(&state).await
}

/// Rejects a username that cannot authenticate without touching the lockout
/// state, so junk usernames can never lock the admin out.
async fn reject_unknown_login_user(
    state: &AdminState,
    client: Option<IpAddr>,
    username: &str,
) -> Response {
    match state.admin_login_rate_limiter.record_unknown_user() {
        AdminLoginAuditDecision::Event => audit_login_failure(state, client, username).await,
        AdminLoginAuditDecision::Coalesced => {
            audit_auth_event(
                state,
                None,
                "login_audit_coalesced",
                AuditOutcome::Failed,
                Some("scope: unknown_user"),
            )
            .await;
        }
        AdminLoginAuditDecision::Skip => {}
    }
    unauthorized_auth()
}

async fn audit_login_failure(state: &AdminState, client: Option<IpAddr>, username: &str) {
    let detail = format!(
        "client: {} username: {}",
        login_client_label(client),
        audit_login_username(username)
    );
    audit_auth_event(state, None, "login", AuditOutcome::Failed, Some(&detail)).await;
}

async fn audit_login_refused(state: &AdminState, client: Option<IpAddr>) {
    let detail = format!(
        "scope: client client: {} username: {ADMIN_USERNAME}",
        login_client_label(client)
    );
    audit_auth_event(
        state,
        None,
        "login_rate_limited",
        AuditOutcome::Denied,
        Some(&detail),
    )
    .await;
}

async fn complete_admin_login(state: &AdminState) -> Response {
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
    if request.current_password.len() > MAX_ADMIN_PASSWORD_BYTES
        || request.new_password.len() > MAX_ADMIN_PASSWORD_BYTES
    {
        return invalid_json_value(format!(
            "passwords must be at most {MAX_ADMIN_PASSWORD_BYTES} bytes"
        ));
    }
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
    let Ok(permit) = Arc::clone(&state.admin_password_verifier).try_acquire_owned() else {
        return auth_busy();
    };
    let verified =
        verify_admin_password(permit, credential.password_hash, request.current_password)
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
        max_future_skew_millis: request
            .max_future_skew_millis
            .unwrap_or(DEFAULT_MAX_FUTURE_SKEW_MILLIS),
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
    body: Result<Json<JsonValue>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(value) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let spec = match spec_io::decode_spec_value(value) {
        Ok(spec) => spec,
        Err(error) => {
            return match error.validation_report() {
                Some(report) => api_ok(StatusCode::UNPROCESSABLE_ENTITY, report),
                None => invalid_spec_json(&error),
            };
        }
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
    body: Result<Json<JsonValue>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(mut value) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    // The body is a ConfigPlanRequest; each side is decoded through spec_io.
    let current = match decode_plan_side(&mut value, "current") {
        Ok(spec) => spec,
        Err(error) => return plan_side_error("current", error),
    };
    let desired = match decode_plan_side(&mut value, "desired") {
        Ok(spec) => spec,
        Err(error) => return plan_side_error("desired", error),
    };
    let plan = MigrationPlanner::plan(&current, &desired);
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
    body: Result<Json<JsonValue>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(value) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let (plan, confirm, source) = match config_apply_request(value) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    let principal = state.principal_from_headers(&headers);
    let database = state.database.lock().await;
    match database.confirm_config_apply_with_source_as(&principal, &plan, &confirm, source) {
        Ok(report) => {
            let status = status_for_apply_report(&report);
            api_ok(status, report)
        }
        Err(error) => db_error(&error),
    }
}

/// The plan a `POST /config/apply` body confirms. `{plan, confirm}` is a `ConfigApplyRequest`
/// whose plan the client computed (`plan_source: client`); `{current, desired, confirm}` has
/// the server recompute the plan with `MigrationPlanner::plan`, each side decoded through
/// `spec_io` as in `/config/plan` (`plan_source: recomputed`). Either way the apply stays
/// audit-only.
fn config_apply_request(
    mut value: JsonValue,
) -> Result<(MigrationPlan, String, ConfigPlanSource), Box<Response>> {
    if value.get("plan").is_some() {
        return serde_json::from_value::<ConfigApplyRequest>(value)
            .map(|request| (request.plan, request.confirm, ConfigPlanSource::Client))
            .map_err(|error| Box::new(invalid_json_body(error)));
    }
    if value.get("current").is_none() && value.get("desired").is_none() {
        return Err(Box::new(invalid_json_body("missing field `plan`")));
    }
    let current = decode_plan_side(&mut value, "current")
        .map_err(|error| Box::new(plan_side_error("current", error)))?;
    let desired = decode_plan_side(&mut value, "desired")
        .map_err(|error| Box::new(plan_side_error("desired", error)))?;
    let confirm = match value.get("confirm") {
        Some(JsonValue::String(confirm)) => confirm.clone(),
        Some(_) => return Err(Box::new(invalid_json_body("`confirm` must be a string"))),
        None => return Err(Box::new(invalid_json_body("missing field `confirm`"))),
    };
    Ok((
        MigrationPlanner::plan(&current, &desired),
        confirm,
        ConfigPlanSource::Recomputed,
    ))
}

async fn config_live_handler(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let principal = state.principal_from_headers(&headers);
    with_database(&state, |database| {
        if let Err(error) = database.authorize_provision_read(&principal, "config_live") {
            return db_error(&error);
        }
        let exported = match provision::export_spec(database) {
            Ok(exported) => exported,
            Err(error) => return provision_error(&error),
        };
        match provision::live_fingerprint(database) {
            Ok(live_fingerprint) => api_ok(
                StatusCode::OK,
                LiveConfigResponse {
                    spec: exported.spec,
                    source: exported.source,
                    notes: exported.notes,
                    live_fingerprint,
                },
            ),
            Err(error) => provision_error(&error),
        }
    })
    .await
}

async fn provision_state_handler(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let principal = state.principal_from_headers(&headers);
    let spec_mode = state.spec_mode.clone();
    with_database(&state, |database| {
        provision_state_response(database, &principal, spec_mode)
    })
    .await
}

fn provision_state_response(
    database: &Database,
    principal: &Principal,
    spec_mode: Option<ServeSpecMode>,
) -> Response {
    if let Err(error) = database.authorize_provision_read(principal, "provision_state") {
        return db_error(&error);
    }
    let live_fingerprint = match provision::live_fingerprint(database) {
        Ok(live_fingerprint) => live_fingerprint,
        Err(error) => return provision_error(&error),
    };
    let state = database.provision_state().and_then(|record| {
        let history = database.provision_history()?;
        let manifest = database.probed_metadata()?.manifest;
        Ok(ProvisionStateResponse {
            provisioned: record.is_some(),
            record,
            history,
            manifest,
            live_fingerprint,
            spec_mode,
        })
    });
    match state {
        Ok(state) => api_ok(StatusCode::OK, state),
        Err(error) => db_error(&error),
    }
}

async fn provision_plan_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<JsonValue>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(value) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let request = match decode_provision_request(value, false) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    let principal = state.principal_from_headers(&headers);
    with_database(&state, |database| {
        if let Err(error) = database.authorize_provision_read(&principal, "provision_plan") {
            return db_error(&error);
        }
        match plan_served(database, &request, &principal) {
            Ok(plan) if plan.outcome == PlanOutcome::Blocked => blocked_plan_response(&plan),
            Ok(plan) => api_ok(StatusCode::OK, plan),
            Err(error) => provision_error(&error),
        }
    })
    .await
}

async fn provision_apply_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<JsonValue>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(value) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let request = match decode_provision_request(value, true) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    let principal = state.principal_from_headers(&headers);
    // One exclusive guard from the recomputed plan through the last step: nothing else changes
    // the database in between.
    with_database_mut(&state, |database| {
        apply_served(database, principal, &request)
    })
    .await
}

/// Applies a spec to the served database as `principal`, which needs `System:Admin` and
/// `Database:Admin` (checked first, so a caller without them learns nothing about the plan).
/// The plan is always recomputed: `confirm` must be its `prov_` id. Without `confirm` the
/// recomputed plan is returned with 422, except for a noop, which needs no confirmation.
fn apply_served(
    database: &mut Database,
    principal: Principal,
    request: &ProvisionRequest,
) -> Response {
    if let Err(error) = database.authorize_provision_actor(&principal) {
        return db_error(&error);
    }
    let env = control_plane_environment(&principal);
    let confirm = if let Some(confirm) = &request.confirm {
        confirm.clone()
    } else {
        let plan = match provision::plan_provision(
            &request.spec,
            ProvisionTargetRef::Open(database),
            &request.options,
            &env,
        ) {
            Ok(plan) => plan,
            Err(error) => return provision_error(&error),
        };
        match plan.outcome {
            PlanOutcome::Noop => plan.plan_id,
            PlanOutcome::Blocked => return blocked_plan_response(&plan),
            PlanOutcome::NeedsFlags => {
                return api_error_with_data(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "flags_required",
                    flags_message(&plan),
                    &plan,
                );
            }
            PlanOutcome::Ready => {
                return api_error_with_data(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "confirmation_required",
                    format!(
                        "review the plan and confirm it: send confirm {}",
                        plan.plan_id
                    ),
                    &plan,
                );
            }
        }
    };
    match provision::apply_provision(
        database,
        &ProvisionActor::Principal(principal),
        &request.spec,
        &Confirmation::PlanId(confirm),
        &request.options,
        &env,
    ) {
        Ok(report) => apply_report_response(&report),
        Err(error) => provision_error(&error),
    }
}

async fn provision_drift_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<JsonValue>, JsonRejection>,
) -> Response {
    if let Some(response) = require_admin_token(&state, &headers) {
        return response;
    }
    let Json(value) = match body {
        Ok(body) => body,
        Err(error) => return invalid_json(&error),
    };
    let request = match decode_provision_request(value, false) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    let principal = state.principal_from_headers(&headers);
    with_database(&state, |database| {
        if let Err(error) = database.authorize_provision_read(&principal, "provision_drift") {
            return db_error(&error);
        }
        match plan_served(database, &request, &principal) {
            Ok(plan) if !plan.validation.valid => api_error_with_data(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_spec",
                format!("the spec is invalid: {}", describe_report(&plan.validation)),
                &plan.validation,
            ),
            Ok(plan) => api_ok(StatusCode::OK, plan.drift),
            Err(error) => provision_error(&error),
        }
    })
    .await
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
                "variants": profile_variants()
                    .iter()
                    .filter(|variant| variant.product == profile.slug)
                    .map(|variant| {
                        json!({
                            "name": variant.name,
                            "storage_profile": variant.storage_profile,
                            "canonical": variant.canonical,
                        })
                    })
                    .collect::<Vec<_>>(),
                "knobs": canonical_profile_knobs(profile.slug),
            })
        })
        .collect::<Vec<_>>();
    api_ok(StatusCode::OK, profiles)
}

/// The preset knobs of a product profile's canonical variant, as `GET /profiles` lists them.
fn canonical_profile_knobs(slug: &str) -> Option<ProfileKnobs> {
    resolve_named_profile(slug, &ProfileKnobOverrides::default())
        .resolved()
        .map(|resolved| resolved.knobs.clone())
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
            provisioning_supported: true,
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
                "provision_apply",
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

fn auth_busy() -> Response {
    (
        [(RETRY_AFTER, "1")],
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "auth_busy",
            "too many concurrent sign-in attempts; retry shortly",
        ),
    )
        .into_response()
}

fn login_client_ip(connect_info: Option<Extension<ConnectInfo<SocketAddr>>>) -> Option<IpAddr> {
    connect_info.map(|Extension(ConnectInfo(address))| address.ip())
}

fn login_client_label(client: Option<IpAddr>) -> String {
    client.map_or_else(
        || "unknown".to_owned(),
        |address| address.to_canonical().to_string(),
    )
}

fn audit_login_username(username: &str) -> String {
    username.chars().take(AUDIT_LOGIN_USERNAME_CHARS).collect()
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

async fn verify_admin_password(
    permit: OwnedSemaphorePermit,
    password_hash: String,
    password: String,
) -> Result<bool, String> {
    tokio::task::spawn_blocking(move || {
        // The permit lives in the blocking task, not in the request future: a
        // client that disconnects drops the handler, but hashing keeps running
        // and its Argon2 memory must stay counted until it finishes.
        let _permit = permit;
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

/// A spec body that is valid JSON but not a `DatabaseSpec` keeps today's 400 `invalid_json`.
fn invalid_spec_json(error: &SpecIoError) -> Response {
    api_error(
        StatusCode::BAD_REQUEST,
        "invalid_json",
        format!("invalid JSON request body: {error}"),
    )
}

/// Decodes one side of a `ConfigPlanRequest`; the error is `None` when the field is missing.
fn decode_plan_side(
    value: &mut JsonValue,
    side: &str,
) -> Result<DatabaseSpec, Option<SpecIoError>> {
    let spec = value.get_mut(side).map(JsonValue::take).ok_or(None)?;
    spec_io::decode_spec_value(spec).map_err(Some)
}

/// A missing side or a decode error is 400 `invalid_json`; a version error cannot become a
/// `MigrationPlan`, so it is 422 `invalid_spec_version` carrying the report.
fn plan_side_error(side: &str, error: Option<SpecIoError>) -> Response {
    let Some(error) = error else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_json",
            format!("invalid JSON request body: missing field `{side}`"),
        );
    };
    match error.validation_report() {
        Some(report) => api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_spec_version",
            format!("{side}: {}", describe_report(&report)),
        ),
        None => invalid_spec_json(&error),
    }
}

fn describe_report(report: &ValidationReport) -> String {
    report
        .issues
        .iter()
        .map(|issue| format!("{} at {}: {}", issue.code, issue.path, issue.message))
        .collect::<Vec<_>>()
        .join("; ")
}

/// Runs `f` with the served database for reading. The provisioning handlers reach the database
/// only through this helper and [`with_database_mut`], so replacing the global database mutex
/// with a read/write lock changes them in one place.
async fn with_database<T>(state: &AdminState, f: impl FnOnce(&Database) -> T) -> T {
    let database = state.database.lock().await;
    f(&database)
}

/// Runs `f` with exclusive access to the served database, held for the whole of `f`.
async fn with_database_mut<T>(state: &AdminState, f: impl FnOnce(&mut Database) -> T) -> T {
    let mut database = state.database.lock().await;
    f(&mut database)
}

/// The body of `POST /provision/plan`, `/provision/apply` and `/provision/drift`.
struct ProvisionRequest {
    spec: DatabaseSpec,
    options: ProvisionOptions,
    /// The `prov_` id an apply confirms.
    confirm: Option<String>,
}

/// Decodes a provisioning body: `spec` through `spec_io` (a decode error is 400
/// `invalid_json`, a missing or unsupported version 422 `invalid_spec_version` with the report
/// in `data`), `options` as `ProvisionOptions` (unknown fields are 400) and, for an apply, the
/// optional `confirm`. Any other key is 400: there is no client plan, the server always
/// recomputes it.
fn decode_provision_request(
    value: JsonValue,
    accepts_confirm: bool,
) -> Result<ProvisionRequest, Box<Response>> {
    let JsonValue::Object(mut body) = value else {
        return Err(Box::new(invalid_json_body(
            "the request body must be a JSON object",
        )));
    };
    let known =
        |key: &str| matches!(key, "spec" | "options") || (accepts_confirm && key == "confirm");
    if let Some(unknown) = body.keys().find(|key| !known(key)) {
        return Err(Box::new(invalid_json_body(format!(
            "unknown field `{unknown}`"
        ))));
    }
    let Some(spec) = body.remove("spec") else {
        return Err(Box::new(invalid_json_body("missing field `spec`")));
    };
    let spec = spec_io::decode_spec_value(spec).map_err(|error| {
        Box::new(match error.validation_report() {
            Some(report) => api_error_with_data(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_spec_version",
                format!("spec: {}", describe_report(&report)),
                &report,
            ),
            None => invalid_spec_json(&error),
        })
    })?;
    let options = match body.remove("options") {
        None | Some(JsonValue::Null) => ProvisionOptions::default(),
        Some(options) => serde_json::from_value(options)
            .map_err(|error| Box::new(invalid_json_body(format!("invalid options: {error}"))))?,
    };
    let confirm = match body.remove("confirm") {
        None | Some(JsonValue::Null) => None,
        Some(JsonValue::String(confirm)) => Some(confirm),
        Some(_) => return Err(Box::new(invalid_json_body("`confirm` must be a string"))),
    };
    Ok(ProvisionRequest {
        spec,
        options,
        confirm,
    })
}

/// The environment of a Control Plane plan or apply: the `control_plane` surface, with the
/// calling principal checked for `RBAC_SELF_LOCKOUT` (it is not part of the plan id).
fn control_plane_environment(principal: &Principal) -> ProvisionEnvironment {
    ProvisionEnvironment {
        principal: Some(principal.clone()),
        ..ProvisionEnvironment::new(ProvisionSurface::ControlPlane)
    }
}

/// The plan of a request against the served database, on the Control Plane surface.
fn plan_served(
    database: &Database,
    request: &ProvisionRequest,
    principal: &Principal,
) -> Result<ProvisionPlan, ProvisionError> {
    provision::plan_provision(
        &request.spec,
        ProvisionTargetRef::Open(database),
        &request.options,
        &control_plane_environment(principal),
    )
}

/// A blocked plan: 422 with the plan in `data`.
fn blocked_plan_response(plan: &ProvisionPlan) -> Response {
    api_error_with_data(
        StatusCode::UNPROCESSABLE_ENTITY,
        blocked_code(plan),
        format!(
            "provisioning plan {} is blocked: {}",
            plan.plan_id,
            blocking_issues(plan)
        ),
        plan,
    )
}

/// The error code of a blocked plan: `invalid_spec`, `rbac_self_lockout`, `audit_required` or
/// `provision_blocked`.
fn blocked_code(plan: &ProvisionPlan) -> &'static str {
    let has = |code: &str| {
        plan.issues
            .iter()
            .any(|found| found.code == code && found.severity == ValidationSeverity::Error)
    };
    if !plan.validation.valid {
        "invalid_spec"
    } else if has("RBAC_SELF_LOCKOUT") {
        "rbac_self_lockout"
    } else if has("AUDIT_REQUIRED_FOR_APPLY") {
        "audit_required"
    } else {
        "provision_blocked"
    }
}

/// The blocking issues of a plan, as `CODE at path` items.
fn blocking_issues(plan: &ProvisionPlan) -> String {
    let issues = plan
        .validation
        .issues
        .iter()
        .chain(&plan.issues)
        .filter(|found| found.severity == ValidationSeverity::Error)
        .map(|found| format!("{} at {}", found.code, found.path))
        .collect::<Vec<_>>();
    if issues.is_empty() {
        "see the plan".to_owned()
    } else {
        issues.join("; ")
    }
}

fn flags_message(plan: &ProvisionPlan) -> String {
    format!(
        "provisioning plan {} needs flags: {}; send them in options and confirm the plan they \
         give",
        plan.plan_id,
        plan.required_flags.join(", ")
    )
}

/// An apply report: 200 for `applied` and `noop`; with the report in `data`, 422
/// `confirmation_required` for a rejected confirmation, and for a partial apply 409
/// `object_conflict` when a create failed because the object exists, else 500
/// `provision_partially_applied`.
fn apply_report_response(report: &ProvisionApplyReport) -> Response {
    match apply_report_status(report) {
        (status, None) => api_ok(status, report),
        (status, Some(code)) => {
            api_error_with_data(status, code, report.check.message.clone(), report)
        }
    }
}

fn apply_report_status(report: &ProvisionApplyReport) -> (StatusCode, Option<&'static str>) {
    match report.status {
        ProvisionOutcome::Applied | ProvisionOutcome::Noop => (StatusCode::OK, None),
        ProvisionOutcome::Rejected => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Some("confirmation_required"),
        ),
        ProvisionOutcome::Blocked => (StatusCode::UNPROCESSABLE_ENTITY, Some("provision_blocked")),
        ProvisionOutcome::PlanChanged => (StatusCode::CONFLICT, Some("plan_changed")),
        ProvisionOutcome::PartiallyApplied => {
            let conflict = report.applied_steps.iter().any(|step| {
                step.outcome == StepOutcome::Failed
                    && step.error_kind.as_deref() == Some("catalog_object_exists")
            });
            if conflict {
                (StatusCode::CONFLICT, Some("object_conflict"))
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Some("provision_partially_applied"),
                )
            }
        }
    }
}

/// A provisioning error: blocked plans, missing flags and confirmations are 422 with the plan in
/// `data`, a changed plan 409 `plan_changed`, missing permissions 403 `forbidden`, a database
/// provisioning does not support 422 `provision_unsupported`.
fn provision_error(error: &ProvisionError) -> Response {
    let unprocessable = StatusCode::UNPROCESSABLE_ENTITY;
    match error {
        ProvisionError::InvalidSpec(report) => {
            api_error_with_data(unprocessable, "invalid_spec", error.to_string(), report)
        }
        ProvisionError::Blocked(plan) => blocked_plan_response(plan),
        ProvisionError::NeedsFlags(plan) => {
            api_error_with_data(unprocessable, "flags_required", flags_message(plan), plan)
        }
        ProvisionError::ConfirmationRequired(plan) => api_error_with_data(
            unprocessable,
            "confirmation_required",
            format!(
                "provisioning plan {} needs a confirmation: send confirm {}",
                plan.plan_id, plan.plan_id
            ),
            plan,
        ),
        ProvisionError::PlanChanged { confirmed, current } => api_error_with_data(
            StatusCode::CONFLICT,
            "plan_changed",
            error.to_string(),
            json!({ "confirmed_plan_id": confirmed, "current_plan_id": current }),
        ),
        ProvisionError::AuditRequired => {
            api_error(unprocessable, "audit_required", error.to_string())
        }
        ProvisionError::PermissionMissing(_) => {
            api_error(StatusCode::FORBIDDEN, "forbidden", error.to_string())
        }
        ProvisionError::Config(ConfigError::Unsupported(_))
        | ProvisionError::Db(DbError::Config(ConfigError::Unsupported(_))) => {
            api_error(unprocessable, "provision_unsupported", error.to_string())
        }
        ProvisionError::Db(error) => db_error(error),
        ProvisionError::RecordUnreadable(_) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "provision_record_unreadable",
            error.to_string(),
        ),
        ProvisionError::AlreadyInitialized
        | ProvisionError::Locked(_)
        | ProvisionError::StartupRefused(_)
        | ProvisionError::Config(_)
        | ProvisionError::Io(_) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            error.to_string(),
        ),
    }
}

/// A body that is JSON but not the expected request: 400 `invalid_json`.
fn invalid_json_body(message: impl std::fmt::Display) -> Response {
    api_error(
        StatusCode::BAD_REQUEST,
        "invalid_json",
        format!("invalid JSON request body: {message}"),
    )
}

fn invalid_json_value(message: impl Into<String>) -> Response {
    api_error(StatusCode::BAD_REQUEST, "invalid_value", message)
}

fn db_error(error: &DbError) -> Response {
    if matches!(error, DbError::AuthzDenied(_)) {
        return api_error(StatusCode::FORBIDDEN, "forbidden", error.to_string());
    }
    if matches!(
        error,
        DbError::TimeSeries(TimeSeriesError::InvalidConfig(_) | TimeSeriesError::InvalidPoint(_))
    ) {
        return invalid_json_value(error.to_string());
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

/// An error envelope that also carries `data`, such as the blocked plan of a 422 or the report
/// of a partial apply.
fn api_error_with_data<T: Serialize>(
    status: StatusCode,
    code: &str,
    message: impl Into<String>,
    data: T,
) -> Response {
    (
        status,
        Json(json!({
            "ok": false,
            "error": {
                "code": code,
                "message": message.into(),
            },
            "data": data,
        })),
    )
        .into_response()
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

/// Canonicalizes a client address for the login lockout; IPv6 clients are
/// grouped by /64 because a single host usually controls the whole prefix.
fn login_rate_client(address: IpAddr) -> IpAddr {
    match address.to_canonical() {
        IpAddr::V4(address) => IpAddr::V4(address),
        IpAddr::V6(address) => IpAddr::V6(Ipv6Addr::from(u128::from(address) & (!0_u128 << 64))),
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
        ADMIN_PASSWORD_VERIFY_PERMITS, AdminLoginAuditDecision, AdminLoginRateDecision,
        AdminLoginRateKey, AdminLoginRateLimitConfig, AdminLoginRateLimiter, AdminState,
        ConfigApplyRequest, ConfigPlanRequest, local_insecure_admin_allowed, login_client_label,
    };
    use crate::{
        config_spec::{ApplyStatus, DatabaseSpec, MigrationPlanner},
        db::{DbConfig, Profile, create_database, open_database},
        query::SqlOutput,
        runtime_advisor::{
            RuntimeAdviceDecisionRequest, RuntimeAdvicePlanRequest, RuntimeAdviceStatus,
        },
        security::{
            AuditOutcome, AuthzPolicy, Permission, Principal, PrincipalRegistry, Resource, Role,
        },
        tuning::WorkloadSample,
    };
    use axum::{
        body::{Body, to_bytes},
        http::{Method, Request, StatusCode, header::AUTHORIZATION},
    };
    use serde::Serialize;
    use serde_json::Value;
    use std::{
        convert::Infallible,
        fs,
        net::{IpAddr, SocketAddr},
        sync::Arc,
    };
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
                .oneshot(login_request("admin", "wrong", None)?)
                .await?;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            let body = json_body(response).await?;
            assert_eq!(body["error"]["code"], "unauthorized");
        }

        for _ in 0..3 {
            let locked = app
                .clone()
                .oneshot(login_request("admin", "correct", None)?)
                .await?;
            assert_eq!(locked.status(), StatusCode::UNAUTHORIZED);
            assert!(locked.headers().get("retry-after").is_none());
            let body = json_body(locked).await?;
            assert_eq!(body["error"]["code"], "unauthorized");
        }
        assert_eq!(
            audit_details(&database, "login", AuditOutcome::Failed)
                .await?
                .len(),
            2
        );
        assert_eq!(
            audit_details(&database, "login_rate_limited", AuditOutcome::Denied).await?,
            vec!["scope: client client: unknown username: admin".to_owned()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn junk_username_failures_do_not_lock_out_admin() -> Result<(), Box<dyn std::error::Error>>
    {
        let database = Arc::new(Mutex::new(admin_database()?));
        let state = AdminState::from_database_handle(Arc::clone(&database))
            .with_admin_login_rate_limit(AdminLoginRateLimitConfig::new(2, 300, 300));
        state
            .bootstrap_admin_password("correct".to_owned(), false)
            .await?;
        let app = super::router(state);
        let client = "198.51.100.7:40000".parse::<SocketAddr>()?;

        let mut usernames = (0..10)
            .map(|index| format!("ghost-{index}"))
            .collect::<Vec<_>>();
        usernames.extend(std::iter::repeat_n("ghost".to_owned(), 3));
        usernames.extend(std::iter::repeat_n("Admin".to_owned(), 3));
        for username in &usernames {
            let response = app
                .clone()
                .oneshot(login_request(username, "wrong", Some(client))?)
                .await?;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }

        let same_client = app
            .clone()
            .oneshot(login_request("admin", "correct", Some(client))?)
            .await?;
        assert_eq!(same_client.status(), StatusCode::OK);
        let without_connect_info = app
            .oneshot(login_request("admin", "correct", None)?)
            .await?;
        assert_eq!(without_connect_info.status(), StatusCode::OK);
        assert!(
            audit_details(&database, "login_rate_limited", AuditOutcome::Denied)
                .await?
                .is_empty(),
            "expected an empty value, got {:?}",
            audit_details(&database, "login_rate_limited", AuditOutcome::Denied).await?
        );
        Ok(())
    }

    #[tokio::test]
    async fn admin_lockout_is_scoped_to_client_ip() -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(Mutex::new(admin_database()?));
        let state = AdminState::from_database_handle(Arc::clone(&database))
            .with_admin_login_rate_limit(AdminLoginRateLimitConfig::new(2, 300, 300));
        state
            .bootstrap_admin_password("correct".to_owned(), false)
            .await?;
        let app = super::router(state);
        let attacker = "198.51.100.7:40000".parse::<SocketAddr>()?;
        let other = "203.0.113.9:40000".parse::<SocketAddr>()?;

        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(login_request("admin", "wrong", Some(attacker))?)
                .await?;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
        let locked = app
            .clone()
            .oneshot(login_request("admin", "correct", Some(attacker))?)
            .await?;
        assert_eq!(locked.status(), StatusCode::UNAUTHORIZED);
        let other_client = app
            .oneshot(login_request("admin", "correct", Some(other))?)
            .await?;
        assert_eq!(other_client.status(), StatusCode::OK);

        let refused = audit_details(&database, "login_rate_limited", AuditOutcome::Denied).await?;
        assert_eq!(refused.len(), 1);
        assert!(
            refused
                .iter()
                .all(|detail| detail.contains("client: 198.51.100.7"))
        );
        let failures = audit_details(&database, "login", AuditOutcome::Failed).await?;
        assert_eq!(failures.len(), 2);
        assert!(
            failures
                .iter()
                .all(|detail| detail.starts_with("client: 198.51.100.7 username: admin"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn global_ceiling_tightens_failed_clients_but_admits_clean_ones()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(Mutex::new(admin_database()?));
        let mut state = AdminState::from_database_handle(Arc::clone(&database));
        state.admin_login_rate_limiter = Arc::new(AdminLoginRateLimiter::with_limits(
            AdminLoginRateLimitConfig::new(5, 300, 300),
            64,
            2,
        ));
        state
            .bootstrap_admin_password("correct".to_owned(), false)
            .await?;
        let app = super::router(state);
        let first = "198.51.100.1:40000".parse::<SocketAddr>()?;
        let second = "198.51.100.2:40000".parse::<SocketAddr>()?;
        let clean = "198.51.100.3:40000".parse::<SocketAddr>()?;

        for client in [first, second] {
            let response = app
                .clone()
                .oneshot(login_request("admin", "wrong", Some(client))?)
                .await?;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
        assert_eq!(
            audit_details(&database, "login", AuditOutcome::Failed)
                .await?
                .len(),
            2
        );
        assert_eq!(
            audit_details(&database, "login_rate_limited", AuditOutcome::Denied).await?,
            vec!["scope: global".to_owned()]
        );

        let tightened = app
            .clone()
            .oneshot(login_request("admin", "correct", Some(first))?)
            .await?;
        assert_eq!(tightened.status(), StatusCode::UNAUTHORIZED);
        let admitted = app
            .oneshot(login_request("admin", "correct", Some(clean))?)
            .await?;
        assert_eq!(admitted.status(), StatusCode::OK);

        let refused = audit_details(&database, "login_rate_limited", AuditOutcome::Denied).await?;
        assert_eq!(refused.len(), 2);
        assert!(refused.contains(&"scope: global".to_owned()));
        assert!(refused.contains(&"scope: client client: 198.51.100.1 username: admin".to_owned()));
        Ok(())
    }

    #[tokio::test]
    async fn unknown_user_login_audit_is_capped_per_window()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(Mutex::new(admin_database()?));
        let state = AdminState::from_database_handle(Arc::clone(&database))
            .with_admin_login_rate_limit(AdminLoginRateLimitConfig::new(1, 300, 300));
        state
            .bootstrap_admin_password("correct".to_owned(), false)
            .await?;
        let app = super::router(state);

        for index in 1..=25 {
            let client = format!("192.0.2.{index}:40000").parse::<SocketAddr>()?;
            let response = app
                .clone()
                .oneshot(login_request(
                    &format!("ghost-{index}"),
                    "wrong",
                    Some(client),
                )?)
                .await?;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
        assert_eq!(
            audit_details(&database, "login", AuditOutcome::Failed)
                .await?
                .len(),
            20
        );
        assert_eq!(
            audit_details(&database, "login_audit_coalesced", AuditOutcome::Failed).await?,
            vec!["scope: unknown_user".to_owned()]
        );
        assert!(
            audit_details(&database, "login_rate_limited", AuditOutcome::Denied)
                .await?
                .is_empty(),
            "expected an empty value, got {:?}",
            audit_details(&database, "login_rate_limited", AuditOutcome::Denied).await?
        );

        let admin = app
            .oneshot(login_request(
                "admin",
                "correct",
                Some("192.0.2.1:40000".parse::<SocketAddr>()?),
            )?)
            .await?;
        assert_eq!(admin.status(), StatusCode::OK);
        Ok(())
    }

    #[tokio::test]
    async fn saturated_password_verifier_returns_busy_without_counting_attempt()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(Mutex::new(admin_database()?));
        let state = AdminState::from_database_handle(Arc::clone(&database))
            .with_admin_login_rate_limit(AdminLoginRateLimitConfig::new(1, 300, 300));
        state
            .bootstrap_admin_password("correct".to_owned(), false)
            .await?;
        let verifier = Arc::clone(&state.admin_password_verifier);
        let app = super::router(state);

        let mut permits = Vec::new();
        for _ in 0..ADMIN_PASSWORD_VERIFY_PERMITS {
            permits.push(Arc::clone(&verifier).try_acquire_owned()?);
        }
        let busy = app
            .clone()
            .oneshot(login_request("admin", "correct", None)?)
            .await?;
        assert_eq!(busy.status(), StatusCode::SERVICE_UNAVAILABLE);
        let retry_after = busy
            .headers()
            .get("retry-after")
            .map(|value| value.to_str())
            .transpose()?;
        assert_eq!(retry_after, Some("1"));
        let body = json_body(busy).await?;
        assert_eq!(body["error"]["code"], "auth_busy");
        assert!(
            login_audit_actions(&database).await?.is_empty(),
            "expected an empty value, got {:?}",
            login_audit_actions(&database).await?
        );

        drop(permits);
        let admitted = app
            .oneshot(login_request("admin", "correct", None)?)
            .await?;
        assert_eq!(admitted.status(), StatusCode::OK);
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_wrong_password_burst_cannot_outrun_the_limiter()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(Mutex::new(admin_database()?));
        let state = AdminState::from_database_handle(Arc::clone(&database))
            .with_admin_login_rate_limit(AdminLoginRateLimitConfig::new(2, 300, 300));
        state
            .bootstrap_admin_password("correct".to_owned(), false)
            .await?;
        let app = super::router(state);
        let client = "198.51.100.7:40000".parse::<SocketAddr>()?;

        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let app = app.clone();
            let request = login_request("admin", "wrong", Some(client))?;
            tasks.spawn(async move {
                let response = app.oneshot(request).await?;
                Ok::<StatusCode, Infallible>(response.status())
            });
        }
        while let Some(status) = tasks.join_next().await {
            let status = status??;
            assert!(
                status == StatusCode::UNAUTHORIZED || status == StatusCode::SERVICE_UNAVAILABLE,
                "unexpected burst status {status}"
            );
        }
        assert_eq!(
            audit_details(&database, "login", AuditOutcome::Failed)
                .await?
                .len(),
            2
        );

        let locked = app
            .oneshot(login_request("admin", "correct", Some(client))?)
            .await?;
        assert_eq!(locked.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            audit_details(&database, "login_rate_limited", AuditOutcome::Denied)
                .await?
                .len(),
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn oversized_login_fields_are_rejected_before_rate_limit_and_audit()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(Mutex::new(admin_database()?));
        let state = AdminState::from_database_handle(Arc::clone(&database))
            .with_admin_login_rate_limit(AdminLoginRateLimitConfig::new(1, 300, 300));
        state
            .bootstrap_admin_password("correct".to_owned(), false)
            .await?;
        let app = super::router(state);

        let long_username = app
            .clone()
            .oneshot(login_request(&"u".repeat(257), "wrong", None)?)
            .await?;
        assert_eq!(long_username.status(), StatusCode::BAD_REQUEST);
        let body = json_body(long_username).await?;
        assert_eq!(body["error"]["code"], "invalid_value");

        let long_password = app
            .clone()
            .oneshot(login_request("admin", &"p".repeat(1025), None)?)
            .await?;
        assert_eq!(long_password.status(), StatusCode::BAD_REQUEST);
        let body = json_body(long_password).await?;
        assert_eq!(body["error"]["code"], "invalid_value");
        assert!(
            login_audit_actions(&database).await?.is_empty(),
            "expected an empty value, got {:?}",
            login_audit_actions(&database).await?
        );

        let admitted = app
            .oneshot(login_request("admin", "correct", None)?)
            .await?;
        assert_eq!(admitted.status(), StatusCode::OK);
        Ok(())
    }

    #[tokio::test]
    async fn login_failure_audit_keeps_client_with_long_username()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(Mutex::new(admin_database()?));
        let app = super::router(AdminState::from_database_handle(Arc::clone(&database)));
        let client = "198.51.100.7:40000".parse::<SocketAddr>()?;

        let response = app
            .oneshot(login_request(&"u".repeat(256), "wrong", Some(client))?)
            .await?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let failures = audit_details(&database, "login", AuditOutcome::Failed).await?;
        assert_eq!(failures.len(), 1);
        let detail = failures.first().ok_or("missing login failure audit")?;
        assert!(detail.len() <= 160);
        assert_eq!(
            detail.strip_prefix("client: 198.51.100.7 username: "),
            Some("u".repeat(64).as_str())
        );
        Ok(())
    }

    #[tokio::test]
    async fn admin_password_length_is_capped_for_bootstrap_and_change_password()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(Mutex::new(admin_database()?));
        let state = AdminState::from_database_handle(Arc::clone(&database));
        assert!(
            state
                .bootstrap_admin_password("x".repeat(1025), false)
                .await
                .is_err()
        );
        assert!(database.lock().await.admin_credential("admin")?.is_none());
        state
            .bootstrap_admin_password("correct".to_owned(), false)
            .await?;
        // With a stored credential and no reset, the bootstrap value is ignored, so its
        // length must not stop startup; a reset would store it and is refused.
        state
            .bootstrap_admin_password("x".repeat(1025), false)
            .await?;
        assert!(
            state
                .bootstrap_admin_password("x".repeat(1025), true)
                .await
                .is_err()
        );
        let app = super::router(state);

        for request in [
            serde_json::json!({
                "current_password": "correct",
                "new_password": "x".repeat(1025)
            }),
            serde_json::json!({
                "current_password": "x".repeat(1025),
                "new_password": "new-secret"
            }),
        ] {
            let response = app
                .clone()
                .oneshot(json_request(
                    Method::POST,
                    "/auth/change-password",
                    &request,
                )?)
                .await?;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let body = json_body(response).await?;
            assert_eq!(body["error"]["code"], "invalid_value");
        }
        Ok(())
    }

    #[tokio::test]
    async fn login_reads_client_ip_from_connect_info_service()
    -> Result<(), Box<dyn std::error::Error>> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let database = Arc::new(Mutex::new(admin_database()?));
        let state = AdminState::from_database_handle(Arc::clone(&database));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                super::router(state).into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
        });

        let body = serde_json::to_vec(&serde_json::json!({
            "username": "ghost",
            "password": "wrong"
        }))?;
        let exchange = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let mut stream = tokio::net::TcpStream::connect(address).await?;
            let head = format!(
                "POST /auth/login HTTP/1.1\r\nHost: {address}\r\n\
                 Content-Type: application/json\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(head.as_bytes()).await?;
            stream.write_all(&body).await?;
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await?;
            Ok::<Vec<u8>, std::io::Error>(response)
        })
        .await;
        server.abort();
        let response = String::from_utf8(exchange??)?;
        assert!(response.starts_with("HTTP/1.1 401"), "{response}");

        let failures = audit_details(&database, "login", AuditOutcome::Failed).await?;
        assert_eq!(failures.len(), 1);
        assert!(
            failures
                .iter()
                .all(|detail| detail.starts_with("client: 127.0.0.1 username: "))
        );
        Ok(())
    }

    #[test]
    fn admin_login_rate_key_normalizes_client_and_username()
    -> Result<(), Box<dyn std::error::Error>> {
        let key =
            |client: &str, username: &str| -> Result<AdminLoginRateKey, std::net::AddrParseError> {
                Ok(AdminLoginRateKey::new(Some(client.parse()?), username))
            };
        assert_eq!(
            key("2001:db8::1", "admin")?,
            key("2001:db8::ffff", "admin")?
        );
        assert_ne!(
            key("2001:db8::1", "admin")?,
            key("2001:db8:0:1::1", "admin")?
        );
        assert_eq!(
            key("::ffff:192.0.2.1", "admin")?,
            key("192.0.2.1", "admin")?
        );
        assert_eq!(key("192.0.2.1", " Admin ")?, key("192.0.2.1", "admin")?);
        assert_ne!(key("192.0.2.1", "admin")?, key("192.0.2.2", "admin")?);
        assert_ne!(
            AdminLoginRateKey::new(None, "admin"),
            key("192.0.2.1", "admin")?
        );
        assert_eq!(
            login_client_label(Some("::ffff:192.0.2.1".parse::<IpAddr>()?)),
            "192.0.2.1"
        );
        assert_eq!(
            login_client_label(Some("2001:db8::1".parse::<IpAddr>()?)),
            "2001:db8::1"
        );
        assert_eq!(login_client_label(None), "unknown");
        Ok(())
    }

    #[test]
    fn admin_login_limiter_reserves_before_verification_and_resets_on_success() {
        let limiter = AdminLoginRateLimiter::new(AdminLoginRateLimitConfig::new(2, 300, 300));
        let key = AdminLoginRateKey::new(Some(IpAddr::from([198, 51, 100, 7])), "admin");
        let allowed = AdminLoginRateDecision::Allowed {
            ceiling_crossed: false,
        };
        let now = 1_000_000;

        assert_eq!(limiter.reserve_at(&key, now), allowed);
        assert_eq!(limiter.reserve_at(&key, now), allowed);
        assert_eq!(
            limiter.check_at(&key, now),
            AdminLoginRateDecision::Limited { audit: true }
        );
        assert_eq!(
            limiter.reserve_at(&key, now),
            AdminLoginRateDecision::Limited { audit: false }
        );

        limiter.record_success_at(&key, now);
        assert_eq!(limiter.check_at(&key, now), allowed);

        assert_eq!(limiter.reserve_at(&key, now), allowed);
        assert_eq!(limiter.reserve_at(&key, now), allowed);
        assert_eq!(
            limiter.check_at(&key, now + 1),
            AdminLoginRateDecision::Limited { audit: true }
        );
        assert_eq!(limiter.check_at(&key, now + 300_000 + 1), allowed);
    }

    #[test]
    fn admin_login_global_ceiling_is_soft() -> Result<(), Box<dyn std::error::Error>> {
        let limiter = AdminLoginRateLimiter::new(AdminLoginRateLimitConfig::new(2, 300, 300));
        let keys = (0..40_u8)
            .map(|index| AdminLoginRateKey::new(Some(IpAddr::from([198, 51, 100, index])), "admin"))
            .collect::<Vec<_>>();
        let first = keys.first().ok_or("missing first key")?;
        let second = keys.get(1).ok_or("missing second key")?;
        let fresh = AdminLoginRateKey::new(Some(IpAddr::from([203, 0, 113, 9])), "admin");
        let allowed = AdminLoginRateDecision::Allowed {
            ceiling_crossed: false,
        };
        let now = 1_000_000;

        for (index, key) in keys.iter().enumerate() {
            assert_eq!(
                limiter.reserve_at(key, now),
                AdminLoginRateDecision::Allowed {
                    ceiling_crossed: index == 39
                }
            );
        }
        assert_eq!(
            limiter.check_at(first, now),
            AdminLoginRateDecision::Limited { audit: true }
        );
        assert_eq!(
            limiter.check_at(first, now),
            AdminLoginRateDecision::Limited { audit: false }
        );
        assert_eq!(limiter.check_at(&fresh, now), allowed);
        assert_eq!(limiter.reserve_at(&fresh, now), allowed);

        let later = now + 300_000 + 1;
        assert_eq!(limiter.check_at(first, later), allowed);
        for (index, key) in keys.iter().enumerate() {
            assert_eq!(
                limiter.reserve_at(key, later),
                AdminLoginRateDecision::Allowed {
                    ceiling_crossed: index == 39
                }
            );
        }
        assert_eq!(
            limiter.check_at(first, later),
            AdminLoginRateDecision::Limited { audit: true }
        );
        limiter.record_success_at(second, later);
        assert_eq!(limiter.check_at(first, later), allowed);
        Ok(())
    }

    #[test]
    fn admin_login_limiter_bounds_keys_and_keeps_locked_entries() {
        let limiter = AdminLoginRateLimiter::with_limits(
            AdminLoginRateLimitConfig::new(2, 300, 300),
            8,
            1_000,
        );
        let locked = AdminLoginRateKey::new(Some(IpAddr::from([203, 0, 113, 9])), "admin");
        let allowed = AdminLoginRateDecision::Allowed {
            ceiling_crossed: false,
        };
        let now = 1_000_000;

        assert_eq!(limiter.reserve_at(&locked, now), allowed);
        assert_eq!(limiter.reserve_at(&locked, now), allowed);
        for index in 0..20_u8 {
            let key = AdminLoginRateKey::new(Some(IpAddr::from([198, 51, 100, index])), "admin");
            assert_eq!(limiter.reserve_at(&key, now), allowed);
        }
        assert!(limiter.tracked_keys() <= 8);
        assert_eq!(
            limiter.check_at(&locked, now),
            AdminLoginRateDecision::Limited { audit: true }
        );

        assert_eq!(
            limiter.check_at(&locked, now + 300_000 + 300_000 + 1),
            allowed
        );
        assert_eq!(limiter.tracked_keys(), 0);
    }

    #[test]
    fn unknown_user_audit_is_coalesced_per_window() {
        let limiter =
            AdminLoginRateLimiter::with_limits(AdminLoginRateLimitConfig::new(2, 300, 300), 8, 3);
        let now = 1_000_000;

        for _ in 0..3 {
            assert_eq!(
                limiter.record_unknown_user_at(now),
                AdminLoginAuditDecision::Event
            );
        }
        assert_eq!(
            limiter.record_unknown_user_at(now),
            AdminLoginAuditDecision::Coalesced
        );
        assert_eq!(
            limiter.record_unknown_user_at(now),
            AdminLoginAuditDecision::Skip
        );
        assert_eq!(
            limiter.record_unknown_user_at(now),
            AdminLoginAuditDecision::Skip
        );
        assert_eq!(
            limiter.record_unknown_user_at(now + 300_000 + 1),
            AdminLoginAuditDecision::Event
        );
        assert_eq!(limiter.tracked_keys(), 0);
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
    async fn security_state_survives_serve_restart() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let db_path = temp.path().join("serve-security.redb");
        {
            let state = AdminState::from_database(create_database(DbConfig::on_disk(
                Profile::Transactional,
                &db_path,
            ))?);
            state
                .bootstrap_admin_password("secret".to_owned(), false)
                .await?;
            let app = super::router(state);
            let token = login_token(&app, "secret").await?;

            let create_table = app
                .clone()
                .oneshot(session_request(
                    Method::POST,
                    "/builder/table",
                    &token,
                    &serde_json::json!({
                        "name": "orders",
                        "schema": {
                            "columns": [{ "name": "id", "ty": "Int", "nullable": false }],
                            "primary_key": 0
                        },
                        "indexes": []
                    }),
                )?)
                .await?;
            assert_eq!(create_table.status(), StatusCode::OK);
            let insert = app
                .clone()
                .oneshot(session_request(
                    Method::POST,
                    "/data/tables/orders/rows",
                    &token,
                    &serde_json::json!({ "row": [1] }),
                )?)
                .await?;
            assert_eq!(insert.status(), StatusCode::OK);

            let update = app
                .oneshot(session_request(
                    Method::POST,
                    "/security",
                    &token,
                    &app_rw_security_body(),
                )?)
                .await?;
            assert_eq!(update.status(), StatusCode::OK);
        }

        // Second boot mirrors `multidb serve` with MULTIDB_ADMIN_PASSWORD set.
        let database = Arc::new(Mutex::new(create_database(DbConfig::on_disk(
            Profile::Transactional,
            &db_path,
        ))?));
        let state = AdminState::from_database_handle(Arc::clone(&database));
        state
            .bootstrap_admin_password("secret".to_owned(), false)
            .await?;
        let app = super::router(state);
        let token = login_token(&app, "secret").await?;
        let security = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/security")
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(security.status(), StatusCode::OK);
        let body = json_body(security).await?;
        let role_names = body["data"]["roles"]
            .as_array()
            .ok_or("roles must be an array")?
            .iter()
            .filter_map(|role| role["name"].as_str())
            .collect::<Vec<_>>();
        assert_eq!(role_names, ["admin", "app_rw"]);
        let principals = body["data"]["principals"]
            .as_array()
            .ok_or("principals must be an array")?;
        assert!(principals.contains(&serde_json::json!({
            "user": "multidb",
            "principal": "app",
            "roles": ["app_rw"]
        })));
        assert_eq!(body["data"]["audit_enabled"], true);

        let database = database.lock().await;
        let principal = database.principal_for_user("multidb");
        assert_eq!(principal, Principal::new("app").with_role("app_rw"));
        assert!(matches!(
            database
                .query_as(&principal, "SELECT id FROM orders")
                .await?,
            SqlOutput::Rows(_)
        ));
        Ok(())
    }

    /// Keeps the canonical admin role and maps PG user `multidb` to an `orders` read/write role.
    fn app_rw_security_body() -> Value {
        serde_json::json!({
            "roles": [
                {
                    "name": "admin",
                    "grants": [
                        { "resource": "System", "permission": "Admin" },
                        { "resource": "Database", "permission": "Read" },
                        { "resource": "Database", "permission": "Write" },
                        { "resource": "Database", "permission": "Admin" }
                    ]
                },
                {
                    "name": "app_rw",
                    "grants": [
                        { "resource": { "Table": "orders" }, "permission": "Read" },
                        { "resource": { "Table": "orders" }, "permission": "Write" }
                    ]
                }
            ],
            "principals": [
                { "user": "admin", "principal": "admin", "roles": ["admin"] },
                { "user": "multidb", "principal": "app", "roles": ["app_rw"] }
            ]
        })
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

    /// Every 422 body that `/config/validate` and `/config/plan` send has an envelope shape the
    /// `OpenAPI` snapshot documents for that status.
    #[tokio::test]
    async fn phase52_config_unprocessable_bodies_match_openapi()
    -> Result<(), Box<dyn std::error::Error>> {
        let openapi: Value = serde_json::from_str(super::CONTROL_PLANE_OPENAPI_JSON)?;
        let current = DatabaseSpec::from_db_config(
            "current",
            &DbConfig::on_disk(Profile::Balanced, "current.redb"),
        );
        let mut invalid = current.clone();
        invalid.guarantees.write_ack = crate::config_spec::WriteAck::Local;
        let mut version_3 = serde_json::to_value(&current)?;
        version_3["version"] = serde_json::json!(3);

        let cases = [
            ("/config/validate", serde_json::to_value(&invalid)?, true),
            ("/config/validate", version_3.clone(), true),
            (
                "/config/plan",
                serde_json::json!({ "current": current, "desired": invalid }),
                true,
            ),
            (
                "/config/plan",
                serde_json::json!({ "current": current, "desired": version_3 }),
                false,
            ),
        ];
        for (path, request, ok) in cases {
            let response = super::router(test_state()?)
                .oneshot(authed_request(Method::POST, path, Some(&request))?)
                .await?;
            assert_eq!(
                response.status(),
                StatusCode::UNPROCESSABLE_ENTITY,
                "{path}"
            );
            let body = json_body(response).await?;
            assert_eq!(body["ok"], ok, "{path}: {body}");
            assert!(
                openapi_documents_envelope(&openapi, path, "422", ok)?,
                "OpenAPI {path} 422 does not document an ok={ok} envelope"
            );
        }
        Ok(())
    }

    /// Whether the documented `status` response of `POST path` admits an envelope with this
    /// `ok` value: its schema, or one of its `oneOf` alternatives, fixes `ok` to it.
    fn openapi_documents_envelope(
        openapi: &Value,
        path: &str,
        status: &str,
        ok: bool,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let operation = resolve_openapi_operation(openapi, &openapi["paths"][path]["post"])?;
        let response = resolve_openapi_operation(openapi, &operation["responses"][status])?;
        let schema = &response["content"]["application/json"]["schema"];
        let alternatives = match schema.get("oneOf").and_then(Value::as_array) {
            Some(alternatives) => alternatives.iter().collect::<Vec<_>>(),
            None => vec![schema],
        };
        for alternative in alternatives {
            let resolved = resolve_openapi_operation(openapi, alternative)?;
            if resolved["properties"]["ok"]["const"] == ok {
                return Ok(true);
            }
        }
        Ok(false)
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
        assert_eq!(body["data"]["provisioning_supported"], true);
        assert_eq!(body["data"]["physical_migration_supported"], false);
        assert_eq!(body["data"]["config_apply_data_mutated"], false);
        assert!(
            body["data"]["capabilities"]
                .as_array()
                .is_some_and(|items| items.iter().any(|item| item == "provision_apply"))
        );
        for operation in [
            "getProvisionState",
            "planProvision",
            "applyProvision",
            "checkProvisionDrift",
            "getLiveConfig",
        ] {
            assert!(
                body["data"]["operations"].as_array().is_some_and(|items| {
                    items.iter().any(|item| {
                        item["operation_id"] == operation
                            && item["stability"] == "preview"
                            && item["auth_required"] == true
                    })
                }),
                "{operation}"
            );
        }
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

    /// A spec body over the 1 MiB router body limit never reaches `spec_io`: it is HTTP 400
    /// `invalid_json`, not a `SPEC_TOO_LARGE` report (docs/profiles.md, "Reading specs"), on
    /// the provisioning routes as on the config routes.
    #[tokio::test]
    async fn oversized_spec_bodies_are_invalid_json() -> Result<(), Box<dyn std::error::Error>> {
        let padding = "x".repeat(super::MAX_CONTROL_PLANE_BODY_BYTES);
        for uri in [
            "/config/validate",
            "/config/plan",
            "/config/apply",
            "/provision/plan",
            "/provision/apply",
            "/provision/drift",
        ] {
            let body = serde_json::to_vec(&serde_json::json!({ "version": 1, "pad": padding }))?;
            let response = super::router(test_state()?)
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri(uri)
                        .header(AUTHORIZATION, "Bearer secret")
                        .header("x-multidb-principal", "root")
                        .header("content-type", "application/json")
                        .body(Body::from(body))?,
                )
                .await?;

            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
            let body = json_body(response).await?;
            assert_eq!(body["error"]["code"], "invalid_json", "{uri}");
        }
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
    async fn profiles_endpoint_lists_variants_and_canonical_knobs()
    -> Result<(), Box<dyn std::error::Error>> {
        let response = super::router(test_state()?)
            .oneshot(authed_request(Method::GET, "/profiles", None::<&()>)?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await?;
        let profiles = body["data"].as_array().ok_or("profiles is not an array")?;
        assert_eq!(profiles.len(), 6);
        for profile in profiles {
            let variants = profile["variants"]
                .as_array()
                .ok_or("variants is not an array")?;
            assert_eq!(
                variants
                    .iter()
                    .filter(|variant| variant["canonical"] == true)
                    .count(),
                1,
                "{}",
                profile["slug"]
            );
            assert!(profile["knobs"].is_object(), "{}", profile["slug"]);
        }

        let find = |slug: &str| {
            profiles
                .iter()
                .find(|profile| profile["slug"] == slug)
                .ok_or(format!("missing profile {slug}"))
        };
        let game = find("game_local_balanced")?;
        assert_eq!(
            game["variants"],
            serde_json::json!([
                { "name": "balanced", "storage_profile": "Balanced", "canonical": true },
                { "name": "in_memory", "storage_profile": "InMemory", "canonical": false },
            ])
        );
        assert_eq!(game["knobs"]["durability"]["commit"], "immediate");
        assert_eq!(game["knobs"]["storage"]["medium"], "durable");
        assert_eq!(game["knobs"]["limits"]["max_value_bytes"], "unlimited");

        let secure = find("secure_app")?;
        let high_durability = secure["variants"]
            .as_array()
            .and_then(|variants| {
                variants
                    .iter()
                    .find(|variant| variant["name"] == "high_durability")
            })
            .ok_or("missing high_durability variant")?;
        assert_eq!(high_durability["storage_profile"], "HighDurability");
        assert_eq!(high_durability["canonical"], false);
        // The listed knobs are the canonical variant's: transactional commits immediately.
        assert_eq!(secure["knobs"]["durability"]["commit"], "immediate");
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
    async fn validate_accepts_v2_and_reports_version_problems_as_422()
    -> Result<(), Box<dyn std::error::Error>> {
        let base = DatabaseSpec::from_db_config(
            "orders",
            &DbConfig::on_disk(Profile::Transactional, "orders.redb"),
        );
        let mut custom = serde_json::to_value(&base)?;
        custom["version"] = serde_json::json!(2);
        custom["profile"] = serde_json::json!("orders_durable");
        custom["custom_profile"] = serde_json::json!({
            "extends": "high_durability",
            "knobs": { "cache": { "max_entries": 8192 } }
        });
        let mut version_3 = serde_json::to_value(&base)?;
        version_3["version"] = serde_json::json!(3);
        let mut missing = serde_json::to_value(&base)?;
        missing
            .as_object_mut()
            .ok_or("spec must be an object")?
            .remove("version");
        let mut unknown_field = serde_json::to_value(&base)?;
        unknown_field["unexpected"] = serde_json::json!(true);

        let post = |body: serde_json::Value| async move {
            super::router(test_state()?)
                .oneshot(authed_request(
                    Method::POST,
                    "/config/validate",
                    Some(&body),
                )?)
                .await
                .map_err(Box::<dyn std::error::Error>::from)
        };

        let accepted = post(custom).await?;
        assert_eq!(accepted.status(), StatusCode::OK);
        let body = json_body(accepted).await?;
        assert_eq!(body["data"]["valid"], true);
        assert_eq!(body["data"]["status"], "custom");

        for (body, code) in [
            (version_3, "UNSUPPORTED_VERSION"),
            (missing, "MISSING_VERSION"),
        ] {
            let response = post(body).await?;
            assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
            let body = json_body(response).await?;
            assert_eq!(body["ok"], true);
            assert_eq!(body["data"]["valid"], false);
            assert_eq!(body["data"]["issues"][0]["code"], code);
        }

        let rejected = post(unknown_field).await?;
        assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
        let body = json_body(rejected).await?;
        assert_eq!(body["ok"], false);
        assert_eq!(body["error"]["code"], "invalid_json");
        Ok(())
    }

    /// The spec shape Studio's custom-profile builder posts (`studio/src/App.tsx`).
    fn studio_custom_profile_spec(
        extends: &str,
        deployment: &serde_json::Value,
        knobs: Option<serde_json::Value>,
    ) -> serde_json::Value {
        let mut custom_profile = serde_json::json!({
            "extends": extends,
            "description": "Custom operator-managed profile for this environment.",
        });
        if let Some(knobs) = knobs {
            custom_profile["knobs"] = knobs;
        }
        serde_json::json!({
            "version": 2,
            "name": "ops_custom_profile_example",
            "profile": "ops_custom_profile",
            "deployment": deployment,
            "topology": { "replica_count": 1, "shard_count": 1 },
            "defaults": { "consistency_domain": "primary", "replication": "cp" },
            "guarantees": {
                "write_ack": "quorum",
                "conflict_resolution": "none",
                "backup": { "enabled": false, "pitr": false },
                "encryption": { "at_rest": false },
                "audit": { "enabled": true },
                "sensitive_data": false,
                "strict_cross_domain_transactions": true
            },
            "domains": [{ "name": "primary", "mode": "strong_cp" }],
            "collections": [],
            "extensions": [],
            "overrides": {},
            "operation_hints": { "studio.builder": "custom_profile" },
            "custom_profile": custom_profile
        })
    }

    #[tokio::test]
    async fn studio_custom_profile_builder_specs_validate() -> Result<(), Box<dyn std::error::Error>>
    {
        let durable = studio_custom_profile_spec(
            "balanced",
            &serde_json::json!({ "mode": "single_node", "storage_path": "target/new-database.redb" }),
            Some(serde_json::json!({
                "durability": { "commit": "immediate" },
                "compression": { "algorithm": "zstd" },
                "cache": { "max_entries": 8192 }
            })),
        );
        let in_memory = studio_custom_profile_spec(
            "in_memory",
            &serde_json::json!({ "mode": "embedded", "storage_path": null }),
            None,
        );
        for spec in [durable, in_memory] {
            let response = super::router(test_state()?)
                .oneshot(authed_request(
                    Method::POST,
                    "/config/validate",
                    Some(&spec),
                )?)
                .await?;
            assert_eq!(response.status(), StatusCode::OK, "{spec}");
            let body = json_body(response).await?;
            assert_eq!(body["data"]["valid"], true, "{body}");
            assert_eq!(body["data"]["status"], "custom", "{body}");
            let issues = body["data"]["issues"]
                .as_array()
                .ok_or("issues is not an array")?;
            assert!(
                issues
                    .iter()
                    .any(|issue| issue["code"] == "CUSTOM_PROFILE_DEFINED"),
                "{body}"
            );
            assert!(
                !issues.iter().any(|issue| issue["code"]
                    .as_str()
                    .is_some_and(|code| code.starts_with("OVERRIDE_"))),
                "{body}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn plan_reports_unsupported_versions_and_missing_sides()
    -> Result<(), Box<dyn std::error::Error>> {
        let current = DatabaseSpec::from_db_config(
            "current",
            &DbConfig::on_disk(Profile::Balanced, "current.redb"),
        );
        let mut desired = serde_json::to_value(&current)?;
        desired["version"] = serde_json::json!(3);

        let response = super::router(test_state()?)
            .oneshot(authed_request(
                Method::POST,
                "/config/plan",
                Some(&serde_json::json!({ "current": current, "desired": desired })),
            )?)
            .await?;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = json_body(response).await?;
        assert_eq!(body["ok"], false);
        assert_eq!(body["error"]["code"], "invalid_spec_version");
        let message = body["error"]["message"].as_str().unwrap_or_default();
        assert!(
            message.starts_with("desired: UNSUPPORTED_VERSION"),
            "{message}"
        );

        let missing = super::router(test_state()?)
            .oneshot(authed_request(
                Method::POST,
                "/config/plan",
                Some(&serde_json::json!({ "current": current })),
            )?)
            .await?;
        assert_eq!(missing.status(), StatusCode::BAD_REQUEST);
        let body = json_body(missing).await?;
        assert_eq!(body["error"]["code"], "invalid_json");

        let same = super::router(test_state()?)
            .oneshot(authed_request(
                Method::POST,
                "/config/plan",
                Some(&ConfigPlanRequest {
                    current: current.clone(),
                    desired: current,
                }),
            )?)
            .await?;
        assert_eq!(same.status(), StatusCode::OK);
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
    #[allow(clippy::too_many_lines)]
    async fn full_text_and_geo_indexes_follow_document_crud_over_http()
    -> Result<(), Box<dyn std::error::Error>> {
        let app = super::router(test_state()?);
        let post = |path: &str, body: Value| {
            authed_request(Method::POST, path, Some(&body))
                .map(|request| app.clone().oneshot(request))
        };

        let response = post(
            "/builder/collection",
            serde_json::json!({
                "name": "places",
                "collection_id": 88,
                "fields": [{ "name": "id", "source": "DocumentId", "ty": "Bytes" }],
                "indexes": []
            }),
        )?
        .await?;
        assert_eq!(response.status(), StatusCode::OK);

        // JSON integers become `Value::Int` coordinates.
        let response = post(
            "/data/collections/places/documents",
            serde_json::json!({
                "document": { "body": "rust database", "location": { "lon": 21, "lat": 52 } }
            }),
        )?
        .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await?;
        let first = body["data"]["id"]
            .as_str()
            .ok_or("missing document id")?
            .to_owned();

        for (path, body) in [
            (
                "/builder/full-text",
                serde_json::json!({ "name": "places_text", "collection_id": 88, "path": ["body"] }),
            ),
            (
                "/builder/geo",
                serde_json::json!({ "name": "places_geo", "collection_id": 88, "path": ["location"] }),
            ),
        ] {
            let response = post(path, body)?.await?;
            assert_eq!(response.status(), StatusCode::OK, "{path}");
        }
        // Creating the indexes backfilled the existing document.
        assert_eq!(
            sql_ids(&app, "SELECT * FROM match('places_text', 'rust', 10)").await?,
            vec![first.clone()]
        );
        assert_eq!(
            sql_ids(
                &app,
                "SELECT * FROM within_radius('places_geo', 21, 52, 1000)"
            )
            .await?,
            vec![first.clone()]
        );

        let response = post(
            "/data/collections/places/documents",
            serde_json::json!({
                "document": {
                    "body": "rust tokio",
                    "location": { "lon": -0.1276, "lat": 51.5072 }
                }
            }),
        )?
        .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await?;
        let second = body["data"]["id"]
            .as_str()
            .ok_or("missing document id")?
            .to_owned();

        let response = app
            .clone()
            .oneshot(authed_request(
                Method::PUT,
                &format!("/data/collections/places/documents/{first}"),
                Some(&serde_json::json!({
                    "document": { "body": "zig allocator", "location": { "lon": 2.35, "lat": 48.85 } }
                })),
            )?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .clone()
            .oneshot(authed_request(
                Method::DELETE,
                &format!("/data/collections/places/documents/{second}"),
                Some(&serde_json::json!({ "confirm": second })),
            )?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);

        for (sql, expected) in [
            (
                "SELECT * FROM match('places_text', 'zig', 10)",
                vec![first.clone()],
            ),
            ("SELECT * FROM match('places_text', 'rust', 10)", Vec::new()),
            (
                "SELECT * FROM within_radius('places_geo', 2.35, 48.85, 1000)",
                vec![first.clone()],
            ),
            (
                "SELECT * FROM within_radius('places_geo', 21, 52, 1000)",
                Vec::new(),
            ),
        ] {
            assert_eq!(sql_ids(&app, sql).await?, expected, "{sql}");
        }

        let response = post(
            "/builder/full-text",
            serde_json::json!({ "name": "orphan_text", "collection_id": 4242, "path": ["body"] }),
        )?
        .await?;
        assert_ne!(response.status(), StatusCode::OK);
        let body = json_body(response).await?;
        assert_eq!(body["ok"], serde_json::json!(false));
        Ok(())
    }

    #[tokio::test]
    async fn time_series_http_rejects_invalid_retention_and_future_points()
    -> Result<(), Box<dyn std::error::Error>> {
        let app = super::router(test_state()?);

        for body in [
            serde_json::json!({ "name": "negative", "chunk_millis": 1000, "retention_millis": -1 }),
            serde_json::json!({ "name": "short", "chunk_millis": 60000, "retention_millis": 1000 }),
            serde_json::json!({
                "name": "skew",
                "chunk_millis": 1000,
                "retention_millis": null,
                "max_future_skew_millis": -1
            }),
        ] {
            let response = app
                .clone()
                .oneshot(authed_request(
                    Method::POST,
                    "/builder/time-series",
                    Some(&body),
                )?)
                .await?;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{body}");
            let error = json_body(response).await?;
            assert_eq!(error["error"]["code"], "invalid_value", "{body}");
        }

        let create = app
            .clone()
            .oneshot(authed_request(
                Method::POST,
                "/builder/time-series",
                Some(&serde_json::json!({
                    "name": "metrics",
                    "chunk_millis": 1000,
                    "retention_millis": 3_600_000
                })),
            )?)
            .await?;
        assert_eq!(create.status(), StatusCode::OK);

        let now = i64::try_from(super::now_millis())?;
        for (timestamp, expected) in [
            (now, StatusCode::OK),
            (now * 1_000, StatusCode::BAD_REQUEST),
            (now - 7_200_000, StatusCode::BAD_REQUEST),
        ] {
            let response = app
                .clone()
                .oneshot(authed_request(
                    Method::POST,
                    "/data/time-series/metrics/points",
                    Some(&serde_json::json!({
                        "series": "cpu",
                        "point": { "timestamp_millis": timestamp, "value": 0.5 }
                    })),
                )?)
                .await?;
            assert_eq!(response.status(), expected, "{timestamp}");
            if expected == StatusCode::BAD_REQUEST {
                let error = json_body(response).await?;
                assert_eq!(error["error"]["code"], "invalid_value", "{timestamp}");
            }
        }

        let points = app
            .oneshot(authed_request(
                Method::GET,
                "/data/time-series/metrics/points?series=cpu\
                 &start=-9223372036854775808&end=9223372036854775807",
                None::<&()>,
            )?)
            .await?;
        assert_eq!(points.status(), StatusCode::OK);
        let body = json_body(points).await?;
        let points = body["data"]["points"]
            .as_array()
            .ok_or("time-series points must be an array")?;
        assert_eq!(points.len(), 1);
        assert_eq!(points[0]["timestamp_millis"], now);
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

    /// The 5 provisioning operations: their bodies, error envelopes and the spec schema the
    /// request bodies point at are documented in the `OpenAPI` snapshot.
    #[test]
    fn phase52_provisioning_operations_document_their_envelopes()
    -> Result<(), Box<dyn std::error::Error>> {
        let openapi: Value = serde_json::from_str(super::CONTROL_PLANE_OPENAPI_JSON)?;
        for (path, statuses) in [
            ("/provision/plan", &["400", "403", "422"][..]),
            ("/provision/apply", &["400", "403", "409", "422", "500"][..]),
            ("/provision/drift", &["400", "403", "422"][..]),
        ] {
            for status in statuses {
                assert!(
                    openapi_documents_envelope(&openapi, path, status, false)?,
                    "OpenAPI {path} {status} does not document an error envelope"
                );
            }
            assert!(openapi_documents_envelope(&openapi, path, "200", true)?);
            let operation = resolve_openapi_operation(&openapi, &openapi["paths"][path]["post"])?;
            let body = resolve_openapi_operation(&openapi, &operation["requestBody"])?;
            let schema = resolve_openapi_operation(
                &openapi,
                &body["content"]["application/json"]["schema"],
            )?;
            assert_eq!(
                schema["properties"]["spec"]["x-multidb-json-schema"],
                crate::config_spec::database_spec_v2_schema()["$id"],
                "{path}"
            );
        }
        for path in ["/provision/state", "/config/live"] {
            let operation = resolve_openapi_operation(&openapi, &openapi["paths"][path]["get"])?;
            let response = resolve_openapi_operation(&openapi, &operation["responses"]["403"])?;
            assert!(response["content"]["application/json"]["schema"].is_object());
        }
        let config = resolve_openapi_operation(&openapi, &openapi["paths"]["/config"]["get"])?;
        assert!(
            config["description"]
                .as_str()
                .is_some_and(|text| text.contains("/config/live"))
        );
        Ok(())
    }

    /// Plans of the served database: 200 for a ready plan, 422 with the plan in `data` for a
    /// blocked one. Planning writes nothing, not even an audit event.
    #[tokio::test]
    async fn provision_plan_is_ready_or_422_with_the_blocked_plan()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let (app, database) = provisioning_app(temp.path())?;
        let events = database.lock().await.audit_events()?.len();
        let spec = provisioning_spec(&serde_json::json!([document_collection("orders")]))?;

        let (status, body) = post_as(&app, "/provision/plan", &plan_body(&spec), "root").await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["ok"], true);
        let plan = &body["data"];
        assert_eq!(plan["outcome"], "ready");
        assert_eq!(plan["surface"], "control_plane");
        assert_eq!(plan["target"]["kind"], "existing_database");
        assert_eq!(plan["required_confirmation"], plan["plan_id"]);
        assert!(
            plan["plan_id"]
                .as_str()
                .is_some_and(|id| id.starts_with("prov_"))
        );
        assert!(plan["issues"].as_array().is_some_and(|issues| {
            issues.iter().any(|issue| {
                issue["code"] == "PROVISION_STORAGE_PATH_IGNORED" && issue["severity"] == "advice"
            })
        }));

        let mut at_rest = spec.clone();
        at_rest.guarantees.encryption.at_rest = true;
        let (status, body) =
            post_as(&app, "/api/provision/plan", &plan_body(&at_rest), "root").await?;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["ok"], false);
        assert_eq!(body["error"]["code"], "provision_blocked");
        assert_eq!(body["data"]["outcome"], "blocked");
        assert!(
            body["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("at guarantees.encryption.at_rest")),
            "{body}"
        );
        let openapi: Value = serde_json::from_str(super::CONTROL_PLANE_OPENAPI_JSON)?;
        assert!(openapi_documents_envelope(
            &openapi,
            "/provision/plan",
            "422",
            false
        )?);

        let mut invalid = spec.clone();
        invalid.guarantees.write_ack = crate::config_spec::WriteAck::Local;
        let (status, body) = post_as(&app, "/provision/plan", &plan_body(&invalid), "root").await?;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["error"]["code"], "invalid_spec");
        assert_eq!(body["data"]["validation"]["valid"], false);

        let database = database.lock().await;
        assert!(database.catalog().is_empty());
        assert_eq!(database.provision_state()?, None);
        assert_eq!(database.audit_events()?.len(), events);
        Ok(())
    }

    /// Provisioning bodies decode like config specs: a spec decode error, unknown options and
    /// unknown keys (there is no client plan) are 400, an unsupported version is 422.
    #[tokio::test]
    async fn provision_bodies_decode_specs_and_options_strictly()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let (app, database) = provisioning_app(temp.path())?;
        let spec = serde_json::to_value(provisioning_spec(&serde_json::json!([]))?)?;
        let mut unknown_field = spec.clone();
        unknown_field["unknown"] = serde_json::json!(true);
        let mut version_3 = spec.clone();
        version_3["version"] = serde_json::json!(3);
        for path in ["/provision/plan", "/provision/apply", "/provision/drift"] {
            for (body, status, code) in [
                (
                    serde_json::json!({ "spec": unknown_field }),
                    StatusCode::BAD_REQUEST,
                    "invalid_json",
                ),
                (
                    serde_json::json!({ "spec": version_3 }),
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "invalid_spec_version",
                ),
                (
                    serde_json::json!({ "spec": spec, "options": { "allow_everything": true } }),
                    StatusCode::BAD_REQUEST,
                    "invalid_json",
                ),
                (
                    serde_json::json!({ "spec": spec, "plan": {} }),
                    StatusCode::BAD_REQUEST,
                    "invalid_json",
                ),
                (
                    serde_json::json!({ "options": {} }),
                    StatusCode::BAD_REQUEST,
                    "invalid_json",
                ),
                (
                    serde_json::json!([]),
                    StatusCode::BAD_REQUEST,
                    "invalid_json",
                ),
            ] {
                let (actual, response) = post_as(&app, path, &body, "root").await?;
                assert_eq!(actual, status, "{path}: {body}: {response}");
                assert_eq!(response["ok"], false, "{path}");
                assert_eq!(response["error"]["code"], code, "{path}: {response}");
            }
        }
        let (status, body) = post_as(
            &app,
            "/provision/apply",
            &serde_json::json!({ "spec": spec, "confirm": 7 }),
            "root",
        )
        .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        let (status, body) = post_as(
            &app,
            "/provision/plan",
            &serde_json::json!({ "spec": spec, "confirm": "prov_x" }),
            "root",
        )
        .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(database.lock().await.catalog().is_empty());
        Ok(())
    }

    /// An apply confirms the recomputed `prov_` id: it applies as the calling principal, a
    /// second apply is a noop, a stale id or an out-of-band builder call between plan and apply
    /// is 409 `plan_changed`, and a database `Read` principal is 403.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn provision_apply_applies_then_noops_and_refuses_changed_plans()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let (app, database) = provisioning_app(temp.path())?;
        let spec = provisioning_spec(&serde_json::json!([document_collection("orders")]))?;
        let plan_id = planned_id(&app, &spec, &serde_json::json!({})).await?;

        let (status, body) = post_as(
            &app,
            "/provision/apply",
            &apply_body(&spec, &plan_id),
            "root",
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["data"]["status"], "applied");
        assert_eq!(body["data"]["plan_id"], plan_id.as_str());
        assert_eq!(body["data"]["check"]["data_mutated"], true);
        {
            let database = database.lock().await;
            assert!(database.catalog().contains_key("orders"));
            let record = database.provision_state()?.ok_or("a provision record")?;
            assert_eq!(record.applied_by, "root");
            assert!(database.audit_events()?.iter().any(|event| {
                event.action == "create_collection"
                    && event.principal.as_deref() == Some("root")
                    && event.outcome == crate::security::AuditOutcome::Succeeded
            }));
        }

        // The applied id no longer confirms anything: the database changed.
        let (status, body) = post_as(
            &app,
            "/provision/apply",
            &apply_body(&spec, &plan_id),
            "root",
        )
        .await?;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["error"]["code"], "plan_changed");
        assert_eq!(body["data"]["confirmed_plan_id"], plan_id.as_str());
        // A noop needs no confirmation, and its own id confirms it too.
        let (status, body) = post_as(
            &app,
            "/provision/apply",
            &serde_json::json!({ "spec": spec }),
            "root",
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["data"]["status"], "noop");
        assert_eq!(body["data"]["check"]["data_mutated"], false);
        let noop_id = planned_id(&app, &spec, &serde_json::json!({})).await?;
        let events = database.lock().await.audit_events()?.len();
        let (status, body) = post_as(
            &app,
            "/provision/apply",
            &apply_body(&spec, &noop_id),
            "root",
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["data"]["status"], "noop");
        assert_eq!(database.lock().await.audit_events()?.len(), events);

        // An out-of-band builder call between plan and apply changes the plan.
        let both = provisioning_spec(&serde_json::json!([
            document_collection("orders"),
            document_collection("notes")
        ]))?;
        let both_id = planned_id(&app, &both, &serde_json::json!({})).await?;
        let created = app
            .clone()
            .oneshot(request_as(
                Method::POST,
                "/builder/collection",
                Some(&events_collection()),
                "root",
            )?)
            .await?;
        assert_eq!(created.status(), StatusCode::OK);
        let (status, body) = post_as(
            &app,
            "/provision/apply",
            &apply_body(&both, &both_id),
            "root",
        )
        .await?;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["error"]["code"], "plan_changed");
        assert!(!database.lock().await.catalog().contains_key("notes"));

        // A principal with database Read only cannot plan, apply, check drift or read state.
        for (method, path) in [
            (Method::POST, "/provision/plan"),
            (Method::POST, "/provision/apply"),
            (Method::POST, "/provision/drift"),
            (Method::GET, "/provision/state"),
            (Method::GET, "/config/live"),
        ] {
            let body = match path {
                "/provision/apply" => Some(apply_body(&both, &both_id)),
                "/provision/plan" | "/provision/drift" => Some(plan_body(&both)),
                _ => None,
            };
            let response = app
                .clone()
                .oneshot(request_as(method, path, body.as_ref(), "ada")?)
                .await?;
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "{path}");
            let body = json_body(response).await?;
            assert_eq!(body["error"]["code"], "forbidden", "{path}");
        }
        let database = database.lock().await;
        assert!(!database.catalog().contains_key("notes"));
        assert!(database.audit_events()?.iter().any(|event| {
            event.action == "provision_apply"
                && event.principal.as_deref() == Some("ada")
                && event.outcome == crate::security::AuditOutcome::Denied
        }));
        Ok(())
    }

    /// Applies the Control Plane refuses with 422: no confirmation, a confirmation that is not a
    /// `prov_` id, missing flags, a blocked plan, audit kept off and a self-lockout.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn provision_apply_refusals_are_422_with_their_codes()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let (app, database) = provisioning_app(temp.path())?;
        let spec = provisioning_spec(&serde_json::json!([document_collection("orders")]))?;

        let (status, body) = post_as(
            &app,
            "/provision/apply",
            &serde_json::json!({ "spec": spec }),
            "root",
        )
        .await?;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["error"]["code"], "confirmation_required");
        assert_eq!(body["data"]["outcome"], "ready");
        for confirm in ["plan_0123456789abcdef0123456789abcdef", "yes"] {
            let (status, body) = post_as(
                &app,
                "/provision/apply",
                &apply_body(&spec, confirm),
                "root",
            )
            .await?;
            assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
            assert_eq!(body["error"]["code"], "confirmation_required");
            assert_eq!(body["data"]["status"], "rejected");
            assert_eq!(body["data"]["check"]["data_mutated"], false);
        }
        assert!(database.lock().await.catalog().is_empty());

        // A blocked plan is refused with or without its id.
        let mut at_rest = spec.clone();
        at_rest.guarantees.encryption.at_rest = true;
        let (status, planned) =
            post_as(&app, "/provision/plan", &plan_body(&at_rest), "root").await?;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{planned}");
        let blocked_id = planned["data"]["plan_id"].as_str().ok_or("a plan id")?;
        for body in [
            apply_body(&at_rest, blocked_id),
            serde_json::json!({ "spec": at_rest }),
        ] {
            let (status, response) = post_as(&app, "/provision/apply", &body, "root").await?;
            assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{response}");
            assert_eq!(response["error"]["code"], "provision_blocked");
            assert_eq!(response["data"]["outcome"], "blocked");
        }

        // Releasing a managed object is destructive and needs its flag.
        let plan_id = planned_id(&app, &spec, &serde_json::json!({})).await?;
        let (status, body) = post_as(
            &app,
            "/provision/apply",
            &apply_body(&spec, &plan_id),
            "root",
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        let empty = provisioning_spec(&serde_json::json!([]))?;
        let (status, body) = post_as(&app, "/provision/plan", &plan_body(&empty), "root").await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["data"]["outcome"], "needs_flags");
        let release_id = body["data"]["plan_id"]
            .as_str()
            .ok_or("a plan id")?
            .to_owned();
        let (status, body) = post_as(
            &app,
            "/provision/apply",
            &apply_body(&empty, &release_id),
            "root",
        )
        .await?;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["error"]["code"], "flags_required");
        assert!(
            body["data"]["required_flags"]
                .as_array()
                .is_some_and(|flags| flags
                    .iter()
                    .any(|flag| flag == "allow_destructive:collections.orders"))
        );
        let options = serde_json::json!({ "allow_destructive": ["collections.orders"] });
        let release_id = planned_id(&app, &empty, &options).await?;
        let mut body = apply_body(&empty, &release_id);
        body["options"] = options;
        let (status, response) = post_as(&app, "/provision/apply", &body, "root").await?;
        assert_eq!(status, StatusCode::OK, "{response}");
        assert!(database.lock().await.catalog().contains_key("orders"));

        // A principal whose system administration comes from a managed role cannot revoke it.
        let ops = with_security(
            spec.clone(),
            &serde_json::json!({
                "roles": [{ "name": "ops", "grants": [{ "resource": "database", "permissions": ["admin"] }] }],
                "principals": [{ "user": "ops", "roles": ["ops"] }]
            }),
        )?;
        let ops_id = planned_id(&app, &ops, &serde_json::json!({})).await?;
        let (status, body) =
            post_as(&app, "/provision/apply", &apply_body(&ops, &ops_id), "root").await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        let narrowed = with_security(
            spec.clone(),
            &serde_json::json!({
                "roles": [{ "name": "ops", "grants": [{ "resource": "database", "permissions": ["read"] }] }],
                "principals": [{ "user": "ops", "roles": ["ops"] }]
            }),
        )?;
        let options = serde_json::json!({ "allow_weakening": ["security.roles.ops"] });
        let mut body = plan_body(&narrowed);
        body["options"] = options.clone();
        let (status, planned) = post_as(&app, "/provision/plan", &body, "ops").await?;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{planned}");
        assert_eq!(planned["error"]["code"], "rbac_self_lockout");
        let lockout_id = planned["data"]["plan_id"].as_str().ok_or("a plan id")?;
        let mut body = apply_body(&narrowed, lockout_id);
        body["options"] = options;
        let (status, response) = post_as(&app, "/provision/apply", &body, "ops").await?;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{response}");
        assert_eq!(response["error"]["code"], "rbac_self_lockout");
        assert!(
            database
                .lock()
                .await
                .authz_policy()
                .roles()
                .get("ops")
                .is_some_and(|role| role.allows(&Resource::System, Permission::Admin))
        );

        // Audit off, and a spec that keeps it off, refuses every apply.
        {
            let mut database = database.lock().await;
            let root = database.principal_for_user("root");
            database.set_audit_enabled_as(&root, false)?;
        }
        let mut quiet = ops.clone();
        quiet.guarantees.audit.enabled = false;
        quiet.collections = serde_json::from_value(serde_json::json!([
            document_collection("orders"),
            document_collection("notes")
        ]))?;
        let (status, planned) =
            post_as(&app, "/provision/plan", &plan_body(&quiet), "root").await?;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{planned}");
        assert_eq!(planned["error"]["code"], "audit_required");
        let quiet_id = planned["data"]["plan_id"].as_str().ok_or("a plan id")?;
        let (status, response) = post_as(
            &app,
            "/provision/apply",
            &apply_body(&quiet, quiet_id),
            "root",
        )
        .await?;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{response}");
        assert_eq!(response["error"]["code"], "audit_required");
        assert!(!database.lock().await.catalog().contains_key("notes"));
        Ok(())
    }

    /// A step that fails leaves a partial apply: 500 with the report in `data`, and the next
    /// plan applies what remains. The failure hook is thread-local, so this runs on the
    /// current-thread runtime.
    #[tokio::test(flavor = "current_thread")]
    async fn provision_apply_reports_partial_applies_as_500()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let (app, database) = provisioning_app(temp.path())?;
        let spec = provisioning_spec(&serde_json::json!([document_collection("orders")]))?;
        let plan_id = planned_id(&app, &spec, &serde_json::json!({})).await?;

        crate::provision::fail_provision_step_for_tests(Some(0));
        let response = post_as(
            &app,
            "/provision/apply",
            &apply_body(&spec, &plan_id),
            "root",
        )
        .await;
        crate::provision::fail_provision_step_for_tests(None);
        let (status, body) = response?;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
        assert_eq!(body["ok"], false);
        assert_eq!(body["error"]["code"], "provision_partially_applied");
        assert_eq!(body["data"]["status"], "partially_applied");
        assert_eq!(body["data"]["failed_step"], "pstep-001");
        let record = database
            .lock()
            .await
            .provision_state()?
            .ok_or("a failed record")?;
        assert_eq!(
            record.status,
            crate::provision::ProvisionRecordStatus::Failed
        );

        let retry_id = planned_id(&app, &spec, &serde_json::json!({})).await?;
        let (status, body) = post_as(
            &app,
            "/provision/apply",
            &apply_body(&spec, &retry_id),
            "root",
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["data"]["status"], "applied");
        assert!(database.lock().await.catalog().contains_key("orders"));
        Ok(())
    }

    /// A create that fails because the object exists is a conflict, any other partial apply an
    /// internal error; rejected confirmations are 422.
    #[test]
    fn apply_report_status_maps_conflicts_partial_applies_and_rejections() {
        use super::ProvisionOutcome as Outcome;
        use crate::provision::{AppliedStep, ProvisionStepKind};
        let report = |status, error_kind: Option<&str>| super::ProvisionApplyReport {
            status,
            check: crate::config_spec::ApplyCheckReport {
                plan_id: "prov_0".to_owned(),
                status: ApplyStatus::Confirmed,
                valid: true,
                confirmation_matched: true,
                audit_recorded: true,
                data_mutated: true,
                message: "message".to_owned(),
            },
            plan_id: "prov_0".to_owned(),
            applied_steps: vec![AppliedStep {
                step_id: "pstep-001".to_owned(),
                kind: ProvisionStepKind::CreateCollection,
                object: Some("orders".to_owned()),
                outcome: super::StepOutcome::Failed,
                detail: "failed".to_owned(),
                error_kind: error_kind.map(str::to_owned),
            }],
            failed_step: Some("pstep-001".to_owned()),
            record: None,
            warnings: Vec::new(),
            session_pending_restart: false,
        };
        assert_eq!(
            super::apply_report_status(&report(
                Outcome::PartiallyApplied,
                Some("catalog_object_exists")
            )),
            (StatusCode::CONFLICT, Some("object_conflict"))
        );
        assert_eq!(
            super::apply_report_status(&report(Outcome::PartiallyApplied, Some("storage"))),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Some("provision_partially_applied")
            )
        );
        assert_eq!(
            super::apply_report_status(&report(Outcome::Rejected, None)),
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                Some("confirmation_required")
            )
        );
        assert_eq!(
            super::apply_report_status(&report(Outcome::Applied, None)),
            (StatusCode::OK, None)
        );
        assert_eq!(
            super::apply_report_status(&report(Outcome::Noop, None)),
            (StatusCode::OK, None)
        );
    }

    /// State, drift and the live spec describe the served database before and after an apply;
    /// an object a builder call creates is reported as unmanaged.
    #[tokio::test]
    async fn provision_state_drift_and_live_config_follow_the_served_database()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let database = Arc::new(Mutex::new(provisioning_database(
            &temp.path().join("app.redb"),
        )?));
        let app = super::router_with_api_prefix(
            AdminState::from_database_handle(Arc::clone(&database))
                .with_admin_token("secret")
                .with_spec_mode("spec_0123", crate::provision::StartupMode::Verify),
        );

        let (status, state) = get_as(&app, "/api/provision/state", "root").await?;
        assert_eq!(status, StatusCode::OK, "{state}");
        assert_eq!(state["data"]["provisioned"], false);
        assert_eq!(state["data"]["record"], Value::Null);
        assert_eq!(state["data"]["history"], serde_json::json!([]));
        assert_eq!(state["data"]["manifest"]["profile"], "balanced");
        assert_eq!(state["data"]["spec_mode"]["spec_digest"], "spec_0123");
        assert_eq!(state["data"]["spec_mode"]["apply_mode"], "verify");
        let fingerprint = state["data"]["live_fingerprint"].clone();
        assert!(
            fingerprint
                .as_str()
                .is_some_and(|value| value.starts_with("live_"))
        );
        let (status, live) = get_as(&app, "/config/live", "root").await?;
        assert_eq!(status, StatusCode::OK, "{live}");
        assert_eq!(live["data"]["source"], "inferred");
        assert_eq!(live["data"]["spec"]["version"], 2);
        assert_eq!(live["data"]["live_fingerprint"], fingerprint);

        let spec = provisioning_spec(&serde_json::json!([document_collection("orders")]))?;
        let (status, drift) = post_as(&app, "/provision/drift", &plan_body(&spec), "root").await?;
        assert_eq!(status, StatusCode::OK, "{drift}");
        assert_eq!(drift["data"]["in_sync"], false);
        let plan_id = planned_id(&app, &spec, &serde_json::json!({})).await?;
        let (status, body) = post_as(
            &app,
            "/provision/apply",
            &apply_body(&spec, &plan_id),
            "root",
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        let created = app
            .clone()
            .oneshot(request_as(
                Method::POST,
                "/builder/collection",
                Some(&events_collection()),
                "root",
            )?)
            .await?;
        assert_eq!(created.status(), StatusCode::OK);

        let (status, state) = get_as(&app, "/provision/state", "root").await?;
        assert_eq!(status, StatusCode::OK, "{state}");
        assert_eq!(state["data"]["provisioned"], true);
        assert_eq!(state["data"]["record"]["status"], "applied");
        assert_eq!(state["data"]["record"]["plan_id"], plan_id.as_str());
        assert_eq!(state["data"]["history"].as_array().map(Vec::len), Some(1));
        assert_ne!(state["data"]["live_fingerprint"], fingerprint);

        let (status, drift) = post_as(&app, "/provision/drift", &plan_body(&spec), "root").await?;
        assert_eq!(status, StatusCode::OK, "{drift}");
        assert_eq!(drift["data"]["in_sync"], true, "{drift}");
        assert!(
            drift["data"]["items"].as_array().is_some_and(|items| {
                items.iter().any(|item| {
                    item["kind"] == "unmanaged_object" && item["path"] == "database.catalog.events"
                })
            }),
            "{drift}"
        );
        let mut strict = plan_body(&spec);
        strict["options"] = serde_json::json!({ "unmanaged": "fail" });
        let (status, drift) = post_as(&app, "/provision/drift", &strict, "root").await?;
        assert_eq!(status, StatusCode::OK, "{drift}");
        assert_eq!(drift["data"]["in_sync"], false);
        let mut invalid = spec.clone();
        invalid.guarantees.write_ack = crate::config_spec::WriteAck::Local;
        let (status, drift) =
            post_as(&app, "/provision/drift", &plan_body(&invalid), "root").await?;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{drift}");
        assert_eq!(drift["error"]["code"], "invalid_spec");
        assert_eq!(drift["data"]["valid"], false);

        let (status, live) = get_as(&app, "/config/live", "root").await?;
        assert_eq!(status, StatusCode::OK, "{live}");
        assert_eq!(live["data"]["source"], "applied_record");
        assert_eq!(live["data"]["spec"], serde_json::to_value(&spec)?);
        Ok(())
    }

    /// `/config/apply` recomputes the plan from `{current, desired, confirm}` and records
    /// `plan_source: recomputed`; a client plan, even a forged one, is recorded as `client`.
    /// Both stay audit-only.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn config_apply_records_whether_the_plan_was_recomputed()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(Mutex::new(admin_database()?));
        let app = super::router(
            AdminState::from_database_handle(Arc::clone(&database)).with_admin_token("secret"),
        );
        let current = {
            let database = database.lock().await;
            DatabaseSpec::from_db_config("current", database.config())
        };
        let mut desired = current.clone();
        desired.profile = "secure_app".to_owned();
        desired.domains[0].mode = crate::config_spec::ConsistencyMode::StrongCp;
        let plan = MigrationPlanner::plan(&current, &desired);
        assert!(!plan.apply_supported);

        let response = app
            .clone()
            .oneshot(authed_request(
                Method::POST,
                "/config/apply",
                Some(&serde_json::json!({
                    "current": current,
                    "desired": desired,
                    "confirm": plan.required_confirmation,
                })),
            )?)
            .await?;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = json_body(response).await?;
        assert_eq!(body["data"]["status"], "unsupported");
        assert_eq!(body["data"]["data_mutated"], false);

        // A client plan that claims the change is supported is confirmed as before, audit-only.
        let mut forged = plan.clone();
        forged.apply_supported = true;
        let response = app
            .clone()
            .oneshot(authed_request(
                Method::POST,
                "/config/apply",
                Some(&ConfigApplyRequest {
                    confirm: forged.required_confirmation.clone(),
                    plan: forged,
                }),
            )?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await?;
        assert_eq!(body["data"]["status"], "confirmed");
        assert_eq!(body["data"]["data_mutated"], false);
        {
            let database = database.lock().await;
            let details = database
                .audit_events()?
                .into_iter()
                .filter(|event| event.action == "config_apply")
                .filter_map(|event| event.detail)
                .collect::<Vec<_>>();
            assert_eq!(details.len(), 2, "{details:?}");
            assert!(
                details[0].contains("plan_source: recomputed"),
                "{details:?}"
            );
            assert!(details[0].contains("data_mutated: false"), "{details:?}");
            assert!(details[1].contains("plan_source: client"), "{details:?}");
            assert!(database.catalog().is_empty());
        }

        // A forged, overlong plan id cannot push plan_source out of the audit detail.
        let mut long = MigrationPlanner::plan(&current, &current);
        long.plan_id = "p".repeat(300);
        let response = app
            .clone()
            .oneshot(authed_request(
                Method::POST,
                "/config/apply",
                Some(&ConfigApplyRequest {
                    confirm: long.required_confirmation.clone(),
                    plan: long,
                }),
            )?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            database
                .lock()
                .await
                .audit_events()?
                .last()
                .and_then(|event| event.detail.as_deref())
                .is_some_and(|detail| detail.ends_with("plan_source: client"))
        );

        for (body, status, code) in [
            (
                serde_json::json!({ "current": current, "desired": current }),
                StatusCode::BAD_REQUEST,
                "invalid_json",
            ),
            (
                serde_json::json!({ "current": current, "confirm": "x" }),
                StatusCode::BAD_REQUEST,
                "invalid_json",
            ),
            (
                serde_json::json!({ "confirm": "x" }),
                StatusCode::BAD_REQUEST,
                "invalid_json",
            ),
            (
                serde_json::json!({ "plan": { "plan_id": 1 }, "confirm": "x" }),
                StatusCode::BAD_REQUEST,
                "invalid_json",
            ),
        ] {
            let response = app
                .clone()
                .oneshot(authed_request(Method::POST, "/config/apply", Some(&body))?)
                .await?;
            assert_eq!(response.status(), status, "{body}");
            let response = json_body(response).await?;
            assert_eq!(response["error"]["code"], code, "{body}");
        }
        let mut version_3 = serde_json::to_value(&current)?;
        version_3["version"] = serde_json::json!(3);
        let response = app
            .oneshot(authed_request(
                Method::POST,
                "/config/apply",
                Some(&serde_json::json!({
                    "current": current,
                    "desired": version_3,
                    "confirm": "x"
                })),
            )?)
            .await?;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = json_body(response).await?;
        assert_eq!(body["error"]["code"], "invalid_spec_version");
        Ok(())
    }

    /// An on-disk database whose RBAC is persisted: the admin role with the principals `root`
    /// (whom `authed_request` names) and `admin` (the bootstrap administrator), plus `ada`, who
    /// may only read the database; created with the `balanced` profile manifest.
    /// `ensure_bootstrap_admin_principal` alone persists `admin` only, and the RBAC of the
    /// in-memory fixture is not persisted, so provisioning would refuse that one as stale (and
    /// volatile).
    fn provisioning_database(
        path: &std::path::Path,
    ) -> Result<crate::db::Database, Box<dyn std::error::Error>> {
        let spec = provisioning_spec(&serde_json::json!([]))?;
        let resolved = crate::config_spec::resolve_profile(&spec)
            .resolved()
            .cloned()
            .ok_or("the balanced profile resolves")?;
        let binding = resolved.bind(crate::db::ReplicationKind::Cp, Some(path.to_path_buf()))?;
        let security = crate::db::SecurityConfig {
            authz_policy: AuthzPolicy::new([
                Role::new("admin")
                    .grant(Resource::System, Permission::Admin)
                    .grant(Resource::Database, Permission::Read)
                    .grant(Resource::Database, Permission::Write)
                    .grant(Resource::Database, Permission::Admin),
                Role::new("reader").grant(Resource::Database, Permission::Read),
            ]),
            principals: PrincipalRegistry::new()
                .with_principal("root", Principal::new("root").with_role("admin"))
                .with_principal("admin", Principal::new("admin").with_role("admin"))
                .with_principal("ada", Principal::new("ada").with_role("reader")),
            ..crate::db::SecurityConfig::default()
        };
        Ok(crate::db::create_database_with_profile(
            &binding,
            crate::db::OperationalConfig::new().with_security(security),
        )?)
    }

    type SharedDatabase = Arc<Mutex<crate::db::Database>>;

    /// The router over a new provisioning fixture in `dir`, and the database it serves.
    fn provisioning_app(
        dir: &std::path::Path,
    ) -> Result<(axum::Router, SharedDatabase), Box<dyn std::error::Error>> {
        let database = Arc::new(Mutex::new(provisioning_database(&dir.join("app.redb"))?));
        let app = super::router_with_api_prefix(
            AdminState::from_database_handle(Arc::clone(&database)).with_admin_token("secret"),
        );
        Ok((app, database))
    }

    /// A version 2 `balanced` spec with `collections`.
    fn provisioning_spec(collections: &Value) -> Result<DatabaseSpec, Box<dyn std::error::Error>> {
        let mut spec =
            DatabaseSpec::from_db_config("app", &DbConfig::on_disk(Profile::Balanced, "app.redb"));
        spec.version = crate::config_spec::DATABASE_SPEC_VERSION_V2;
        spec.profile = "balanced".to_owned();
        spec.collections = serde_json::from_value(collections.clone())?;
        Ok(spec)
    }

    fn with_security(
        mut spec: DatabaseSpec,
        security: &Value,
    ) -> Result<DatabaseSpec, Box<dyn std::error::Error>> {
        spec.security = Some(serde_json::from_value(security.clone())?);
        Ok(spec)
    }

    fn document_collection(name: &str) -> Value {
        serde_json::json!({
            "name": name, "role": "document_entity", "domain": "primary", "indexes": ["primary"]
        })
    }

    /// A `/builder/collection` body for a collection created outside provisioning.
    fn events_collection() -> Value {
        serde_json::json!({
            "name": "events",
            "fields": [{ "name": "id", "source": "DocumentId", "ty": "Bytes" }],
            "indexes": []
        })
    }

    fn plan_body(spec: &DatabaseSpec) -> Value {
        serde_json::json!({ "spec": spec })
    }

    fn apply_body(spec: &DatabaseSpec, confirm: &str) -> Value {
        serde_json::json!({ "spec": spec, "confirm": confirm })
    }

    /// The `prov_` id of a ready or noop plan of `spec` with `options`, as `root`.
    async fn planned_id(
        app: &axum::Router,
        spec: &DatabaseSpec,
        options: &Value,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let mut body = plan_body(spec);
        body["options"] = options.clone();
        let (status, plan) = post_as(app, "/provision/plan", &body, "root").await?;
        assert_eq!(status, StatusCode::OK, "{plan}");
        Ok(plan["data"]["plan_id"]
            .as_str()
            .ok_or("a plan id")?
            .to_owned())
    }

    async fn post_as(
        app: &axum::Router,
        uri: &str,
        body: &Value,
        principal: &str,
    ) -> Result<(StatusCode, Value), Box<dyn std::error::Error>> {
        let response = app
            .clone()
            .oneshot(request_as(Method::POST, uri, Some(body), principal)?)
            .await?;
        let status = response.status();
        Ok((status, json_body(response).await?))
    }

    async fn get_as(
        app: &axum::Router,
        uri: &str,
        principal: &str,
    ) -> Result<(StatusCode, Value), Box<dyn std::error::Error>> {
        let response = app
            .clone()
            .oneshot(request_as(Method::GET, uri, None::<&Value>, principal)?)
            .await?;
        let status = response.status();
        Ok((status, json_body(response).await?))
    }

    /// `authed_request` as `principal`.
    fn request_as<T: Serialize>(
        method: Method,
        uri: &str,
        body: Option<&T>,
        principal: &str,
    ) -> Result<Request<Body>, Box<dyn std::error::Error>> {
        let mut builder = Request::builder()
            .method(method)
            .uri(uri)
            .header(AUTHORIZATION, "Bearer secret")
            .header("x-multidb-principal", principal);
        let body = if let Some(body) = body {
            builder = builder.header("content-type", "application/json");
            Body::from(serde_json::to_vec(body)?)
        } else {
            Body::empty()
        };
        Ok(builder.body(body)?)
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

    fn session_request<T: Serialize>(
        method: Method,
        uri: &str,
        token: &str,
        body: &T,
    ) -> Result<Request<Body>, Box<dyn std::error::Error>> {
        Ok(Request::builder()
            .method(method)
            .uri(uri)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(body)?))?)
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

    fn login_request(
        username: &str,
        password: &str,
        client: Option<SocketAddr>,
    ) -> Result<Request<Body>, Box<dyn std::error::Error>> {
        let mut request = json_request(
            Method::POST,
            "/auth/login",
            &serde_json::json!({
                "username": username,
                "password": password
            }),
        )?;
        if let Some(client) = client {
            request
                .extensions_mut()
                .insert(axum::extract::ConnectInfo(client));
        }
        Ok(request)
    }

    async fn audit_details(
        database: &Arc<Mutex<crate::db::Database>>,
        action: &str,
        outcome: AuditOutcome,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let database = database.lock().await;
        Ok(database
            .audit_events()?
            .into_iter()
            .filter(|event| event.action == action && event.outcome == outcome)
            .map(|event| event.detail.unwrap_or_default())
            .collect())
    }

    async fn login_audit_actions(
        database: &Arc<Mutex<crate::db::Database>>,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let database = database.lock().await;
        Ok(database
            .audit_events()?
            .into_iter()
            .filter(|event| event.action.starts_with("login"))
            .map(|event| event.action)
            .collect())
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

    /// Runs `/sql` and returns the first column of each row as a string.
    async fn sql_ids(
        app: &axum::Router,
        sql: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let response = app
            .clone()
            .oneshot(authed_request(
                Method::POST,
                "/sql",
                Some(&serde_json::json!({ "sql": sql })),
            )?)
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!("{sql} failed with {}", response.status()).into());
        }
        let body = json_body(response).await?;
        Ok(body["data"]["output"]["rows"]
            .as_array()
            .ok_or("sql rows must be an array")?
            .iter()
            .map(|row| row[0].as_str().unwrap_or_default().to_owned())
            .collect())
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
