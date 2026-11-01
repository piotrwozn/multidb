use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    future::Future,
    io,
    path::Path,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use futures::{Sink, stream};
use pgwire::{
    api::{
        ClientInfo, ClientPortalStore, METADATA_USER, PgWireConnectionState, PgWireServerHandlers,
        Type,
        auth::{
            AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
            sasl::{
                SASLAuthStartupHandler,
                scram::{ScramAuth, gen_salted_password},
            },
        },
        portal::{Format, Portal},
        query::{ExtendedQueryHandler, SimpleQueryHandler},
        results::{
            DataRowEncoder, DescribePortalResponse, DescribeResponse, DescribeStatementResponse,
            FieldFormat, FieldInfo, QueryResponse, Response, Tag,
        },
        stmt::{NoopQueryParser, StoredStatement},
        store::PortalStore,
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::{PgWireBackendMessage, PgWireFrontendMessage},
};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use tokio::{
    net::TcpListener,
    sync::RwLock,
    sync::Semaphore,
    task::{JoinError, JoinSet, spawn_blocking},
};
use tokio_rustls::{TlsAcceptor, rustls::ServerConfig};

use crate::{
    compat,
    db::DbError,
    model::Value,
    observability,
    query::{Row, SqlOutput, SqlRows},
};

mod frame;
mod parameters;
mod session;
mod session_state;

const POSTGRESQL_ALPN: &[u8] = b"postgresql";
const DEFAULT_BLOCKING_TASKS: usize = 32;
const DEFAULT_MAX_CONNECTIONS: usize = 512;
const DEFAULT_CONNECTION_TIMEOUT_MS: u64 = 300_000;
const DEFAULT_HANDSHAKE_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_IDLE_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_AUTH_FAILURE_LIMIT: usize = 10;
const DEFAULT_AUTH_FAILURE_WINDOW_MS: u64 = 60_000;
const MAX_AUTH_RATE_KEYS: usize = 65_536;
const MAX_AUTH_USERNAME_BYTES: usize = 256;

#[derive(Clone)]
pub struct NetworkConfig {
    pub tls: TlsMode,
    pub auth: AuthConfig,
    pub max_blocking_tasks: usize,
    pub max_connections: usize,
    /// Maximum processing time for one protocol request (including response writes).
    /// This used to cap the entire session; active sessions now have no total lifetime cap.
    /// The database query execution/queue budgets are enforced independently.
    pub connection_timeout_ms: u64,
    /// Total TLS/startup/SCRAM deadline, shared across all handshake messages.
    pub handshake_timeout_ms: u64,
    /// Deadline for receiving one next frame, never time spent executing a query.
    pub idle_timeout_ms: u64,
    /// Maximum declared frontend frame length; upstream type-specific limits also apply.
    pub max_frame_bytes: usize,
    pub auth_failure_limit: usize,
    pub auth_failure_window_ms: u64,
}

#[derive(Clone)]
pub enum TlsMode {
    Require(TlsConfig),
    DisabledForTests,
}

pub struct TlsConfig {
    certificates: Vec<CertificateDer<'static>>,
    private_key: Arc<PrivateKeyDer<'static>>,
    certificate_pem: Arc<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct AuthConfig {
    iterations: usize,
    users: Arc<BTreeMap<String, ScramCredential>>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ScramCredential {
    salt: Vec<u8>,
    salted_password: Vec<u8>,
}

pub struct PgServer;

#[derive(thiserror::Error, Debug)]
pub enum NetworkError {
    #[error("io: {0}")]
    Io(#[from] io::Error),

    #[error("tls: {0}")]
    Tls(String),

    #[error("postgres wire: {0}")]
    PgWire(#[from] PgWireError),

    #[error("database: {0}")]
    Db(#[from] DbError),

    #[error("blocking task failed: {0}")]
    Join(String),

    #[error("runtime: {0}")]
    Runtime(String),

    #[error("auth: {0}")]
    Auth(String),
}

#[derive(Clone)]
struct BlockingDbExecutor {
    database: DatabaseHandle,
    permits: Arc<Semaphore>,
}

struct DbPgHandler {
    executor: BlockingDbExecutor,
    parser: Arc<NoopQueryParser>,
}

#[derive(Clone)]
struct DbPgFactory {
    handler: Arc<DbPgHandler>,
    auth: AuthConfig,
    certificate_pem: Option<Arc<Vec<u8>>>,
    tls_required: bool,
    auth_limiter: AuthRateLimiter,
}

struct DbStartupHandler {
    auth: AuthConfig,
    inner: Option<SASLAuthStartupHandler<DefaultServerParameterProvider>>,
    startup_error: Option<String>,
    tls_required: bool,
    auth_limiter: AuthRateLimiter,
    pending_auth: StdMutex<Option<AuthAttempt>>,
    sasl_started: std::sync::atomic::AtomicBool,
}

#[derive(Clone, Debug)]
struct StaticAuthSource {
    auth: AuthConfig,
}

#[derive(Clone, Debug)]
struct AuthRateLimiter {
    state: Arc<StdMutex<HashMap<AuthRateKey, AuthRateEntry>>>,
    limit: usize,
    window: Duration,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct AuthRateKey {
    ip: std::net::IpAddr,
    user: String,
}

#[derive(Clone, Copy, Debug)]
struct AuthRateEntry {
    window_start: Instant,
    failures: usize,
    in_flight: usize,
}

struct AuthAttempt {
    limiter: AuthRateLimiter,
    key: AuthRateKey,
    completed: bool,
}

impl AuthAttempt {
    fn succeed(mut self) {
        self.complete(false);
    }

    fn complete(&mut self, failed: bool) {
        if self.completed {
            return;
        }
        self.completed = true;
        if let Ok(mut state) = self.limiter.state.lock()
            && let Some(entry) = state.get_mut(&self.key)
        {
            entry.in_flight = entry.in_flight.saturating_sub(1);
            if failed {
                entry.failures = entry.failures.saturating_add(1);
            }
            if entry.in_flight == 0 && entry.failures == 0 {
                state.remove(&self.key);
            }
        }
    }
}

impl Drop for AuthAttempt {
    fn drop(&mut self) {
        // Failed or abandoned SCRAM exchanges consume the reserved failure budget.
        self.complete(true);
    }
}

/// Database handle shared between `PostgreSQL` wire and other in-process servers.
///
/// Each statement holds a shared read guard while it runs, so statements execute
/// concurrently. Embedders take the write guard only for `&mut Database` changes
/// (catalog, security or tuning) and must not hold a guard while awaiting another one.
pub type SharedDatabase = Arc<RwLock<crate::db::Database>>;

#[derive(Clone)]
enum DatabaseHandle {
    Direct(Arc<crate::db::Database>),
    Shared(SharedDatabase),
}

impl NetworkConfig {
    #[must_use]
    pub const fn new(tls: TlsMode, auth: AuthConfig) -> Self {
        Self {
            tls,
            auth,
            max_blocking_tasks: DEFAULT_BLOCKING_TASKS,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            connection_timeout_ms: DEFAULT_CONNECTION_TIMEOUT_MS,
            handshake_timeout_ms: DEFAULT_HANDSHAKE_TIMEOUT_MS,
            idle_timeout_ms: DEFAULT_IDLE_TIMEOUT_MS,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            auth_failure_limit: DEFAULT_AUTH_FAILURE_LIMIT,
            auth_failure_window_ms: DEFAULT_AUTH_FAILURE_WINDOW_MS,
        }
    }

    #[must_use]
    pub const fn with_max_blocking_tasks(mut self, max_blocking_tasks: usize) -> Self {
        self.max_blocking_tasks = max_blocking_tasks;
        self
    }

    #[must_use]
    pub const fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Sets the per-protocol-request processing ceiling, not the session lifetime.
    /// Handshake and idle limits have their own settings; the database owns query budgets.
    #[must_use]
    pub const fn with_connection_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.connection_timeout_ms = timeout_ms;
        self
    }

    #[must_use]
    pub const fn with_handshake_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.handshake_timeout_ms = timeout_ms;
        self
    }

    #[must_use]
    pub const fn with_idle_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.idle_timeout_ms = timeout_ms;
        self
    }

    #[must_use]
    pub const fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = max_frame_bytes;
        self
    }

    #[must_use]
    pub const fn with_auth_failure_limit(mut self, limit: usize, window_ms: u64) -> Self {
        self.auth_failure_limit = limit;
        self.auth_failure_window_ms = window_ms;
        self
    }
}

impl Clone for TlsConfig {
    fn clone(&self) -> Self {
        Self {
            certificates: self.certificates.clone(),
            private_key: Arc::clone(&self.private_key),
            certificate_pem: Arc::clone(&self.certificate_pem),
        }
    }
}

impl TlsConfig {
    /// Loads a TLS certificate chain and private key from PEM files.
    /// # Errors
    /// Fails when files cannot be read, PEM cannot be decoded, or no private key exists.
    pub fn from_pem_files(
        cert_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
    ) -> Result<Self, NetworkError> {
        let certificate_pem = std::fs::read(cert_path)?;
        let certificates = CertificateDer::pem_slice_iter(&certificate_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| NetworkError::Tls(error.to_string()))?;
        if certificates.is_empty() {
            return Err(NetworkError::Tls(
                "certificate file does not contain certificates".to_owned(),
            ));
        }

        let key_pem = std::fs::read(key_path)?;
        let private_key = PrivateKeyDer::from_pem_slice(&key_pem)
            .map_err(|error| NetworkError::Tls(error.to_string()))?;

        Ok(Self {
            certificates,
            private_key: Arc::new(private_key),
            certificate_pem: Arc::new(certificate_pem),
        })
    }

    fn acceptor(&self) -> Result<TlsAcceptor, NetworkError> {
        let mut config = ServerConfig::builder_with_provider(Arc::new(
            tokio_rustls::rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|error| NetworkError::Tls(error.to_string()))?
        .with_no_client_auth()
        .with_single_cert(self.certificates.clone(), self.private_key.clone_key())
        .map_err(|error| NetworkError::Tls(error.to_string()))?;
        config.alpn_protocols = vec![POSTGRESQL_ALPN.to_vec()];

        Ok(TlsAcceptor::from(Arc::new(config)))
    }

    #[must_use]
    fn certificate_pem(&self) -> Arc<Vec<u8>> {
        Arc::clone(&self.certificate_pem)
    }
}

impl AuthConfig {
    #[must_use]
    pub fn new<I, U>(iterations: usize, users: I) -> Self
    where
        I: IntoIterator<Item = (U, ScramCredential)>,
        U: Into<String>,
    {
        Self {
            iterations,
            users: Arc::new(
                users
                    .into_iter()
                    .map(|(user, credential)| (user.into(), credential))
                    .collect(),
            ),
        }
    }

    #[must_use]
    pub fn single_user(
        user: impl Into<String>,
        iterations: usize,
        credential: ScramCredential,
    ) -> Self {
        Self::new(iterations, [(user.into(), credential)])
    }

    #[must_use]
    pub const fn iterations(&self) -> usize {
        self.iterations
    }

    #[must_use]
    pub fn contains_user(&self, user: &str) -> bool {
        self.users.contains_key(user)
    }

    fn credential(&self, user: &str) -> Option<&ScramCredential> {
        self.users.get(user)
    }
}

impl Debug for ScramCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ScramCredential")
            .field("salt", &self.salt)
            .field("salted_password", &"[REDACTED]")
            .finish()
    }
}

impl ScramCredential {
    #[must_use]
    pub fn new(salt: Vec<u8>, salted_password: Vec<u8>) -> Self {
        Self {
            salt,
            salted_password,
        }
    }

    #[must_use]
    pub fn from_password_for_tests(password: &str, salt: &[u8], iterations: usize) -> Self {
        Self::new(
            salt.to_vec(),
            gen_salted_password(password, salt, iterations),
        )
    }

    /// Creates a SCRAM credential with a fresh random salt.
    /// # Errors
    /// Fails when the operating system random source is unavailable.
    pub fn from_password(password: &str, iterations: usize) -> Result<Self, NetworkError> {
        let mut salt = [0_u8; 16];
        getrandom::fill(&mut salt).map_err(|error| NetworkError::Auth(error.to_string()))?;
        Ok(Self::new(
            salt.to_vec(),
            gen_salted_password(password, &salt, iterations),
        ))
    }

    #[must_use]
    pub fn salt(&self) -> &[u8] {
        &self.salt
    }

    #[must_use]
    pub fn salted_password(&self) -> &[u8] {
        &self.salted_password
    }
}

impl AuthRateLimiter {
    fn new(limit: usize, window_ms: u64) -> Self {
        Self {
            state: Arc::new(StdMutex::new(HashMap::new())),
            limit: limit.max(1),
            window: Duration::from_millis(window_ms.max(1)),
        }
    }

    fn reserve_attempt(
        &self,
        ip: std::net::IpAddr,
        user: &str,
    ) -> Result<AuthAttempt, NetworkError> {
        if user.len() > MAX_AUTH_USERNAME_BYTES {
            return Err(NetworkError::Auth(
                "authentication username is too long".to_owned(),
            ));
        }
        let key = AuthRateKey {
            ip,
            user: user.to_owned(),
        };
        let now = Instant::now();
        let mut state = self
            .state
            .lock()
            .map_err(|_| NetworkError::Runtime("auth rate limiter lock poisoned".to_owned()))?;
        if !state.contains_key(&key) && state.len() >= MAX_AUTH_RATE_KEYS {
            state.retain(|_, entry| {
                entry.in_flight > 0 || now.duration_since(entry.window_start) < self.window
            });
            if state.len() >= MAX_AUTH_RATE_KEYS {
                return Err(NetworkError::Auth(
                    "authentication rate limit exceeded".to_owned(),
                ));
            }
        }
        let entry = state.entry(key.clone()).or_insert(AuthRateEntry {
            window_start: now,
            failures: 0,
            in_flight: 0,
        });
        if now.duration_since(entry.window_start) >= self.window {
            entry.window_start = now;
            entry.failures = 0;
        }
        // Reserve before verification so concurrent bad proofs cannot outrun the limit.
        if entry.failures.saturating_add(entry.in_flight) >= self.limit {
            return Err(NetworkError::Auth(
                "authentication rate limit exceeded".to_owned(),
            ));
        }
        entry.in_flight += 1;
        Ok(AuthAttempt {
            limiter: self.clone(),
            key,
            completed: false,
        })
    }
}

impl PgServer {
    /// Serves `PostgreSQL` wire connections until the listener fails.
    /// # Errors
    /// Fails when TLS setup fails or accepting a TCP connection fails.
    pub async fn serve(
        listener: TcpListener,
        database: Arc<crate::db::Database>,
        config: NetworkConfig,
    ) -> Result<(), NetworkError> {
        Self::serve_handle_until_shutdown(
            listener,
            DatabaseHandle::Direct(database),
            config,
            std::future::pending(),
        )
        .await
    }

    /// Serves `PostgreSQL` wire connections until the shutdown future completes.
    /// # Errors
    /// Fails when TLS setup fails or accepting a TCP connection fails.
    pub async fn serve_until_shutdown<F>(
        listener: TcpListener,
        database: Arc<crate::db::Database>,
        config: NetworkConfig,
        shutdown: F,
    ) -> Result<(), NetworkError>
    where
        F: Future<Output = ()>,
    {
        Self::serve_handle_until_shutdown(
            listener,
            DatabaseHandle::Direct(database),
            config,
            shutdown,
        )
        .await
    }

    /// Serves `PostgreSQL` wire connections from a shared database handle.
    ///
    /// Statements share a read guard; a held or queued write guard pauses new
    /// statements until it is released.
    /// # Errors
    /// Fails when TLS setup fails or accepting a TCP connection fails.
    pub async fn serve_shared(
        listener: TcpListener,
        database: SharedDatabase,
        config: NetworkConfig,
    ) -> Result<(), NetworkError> {
        Self::serve_shared_until_shutdown(listener, database, config, std::future::pending()).await
    }

    /// Serves `PostgreSQL` wire connections from a shared database handle until shutdown.
    ///
    /// Statements share a read guard; a held or queued write guard pauses new
    /// statements until it is released.
    /// # Errors
    /// Fails when TLS setup fails or accepting a TCP connection fails.
    pub async fn serve_shared_until_shutdown<F>(
        listener: TcpListener,
        database: SharedDatabase,
        config: NetworkConfig,
        shutdown: F,
    ) -> Result<(), NetworkError>
    where
        F: Future<Output = ()>,
    {
        Self::serve_handle_until_shutdown(
            listener,
            DatabaseHandle::Shared(database),
            config,
            shutdown,
        )
        .await
    }

    async fn serve_handle_until_shutdown<F>(
        listener: TcpListener,
        database: DatabaseHandle,
        config: NetworkConfig,
        shutdown: F,
    ) -> Result<(), NetworkError>
    where
        F: Future<Output = ()>,
    {
        let tls_acceptor = match &config.tls {
            TlsMode::Require(tls) => Some(tls.acceptor()?),
            TlsMode::DisabledForTests => None,
        };
        let max_connections = config.max_connections.max(1);
        let session_limits = session::SessionLimits::from_config(&config);
        let connection_permits = Arc::new(Semaphore::new(max_connections));
        let blocking_tasks = u32::try_from(config.max_blocking_tasks.max(1))
            .map_err(|_| NetworkError::Runtime("max_blocking_tasks exceeds u32".to_owned()))?;
        let factory = Arc::new(DbPgFactory::new(database, config));
        let mut connections = JoinSet::new();
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                () = &mut shutdown => {
                    connections.abort_all();
                    while connections.join_next().await.is_some() {}
                    // Aborting socket tasks does not cancel their spawn_blocking work.
                    // Every queued/running database operation owns one of these permits;
                    // acquiring all of them drains that work before the caller flushes.
                    let _drained = Arc::clone(&factory.handler.executor.permits)
                        .acquire_many_owned(blocking_tasks).await
                        .map_err(|error| NetworkError::Runtime(error.to_string()))?;
                    return Ok(());
                },
                _ = connections.join_next(), if !connections.is_empty() => {},
                accepted = listener.accept() => {
                    let (socket, _) = accepted?;
                    let tls_acceptor = tls_acceptor.clone();
                    let factory = Arc::clone(&factory);
                    let permits = Arc::clone(&connection_permits);
                    let Ok(permit) = permits.try_acquire_owned() else {
                        tracing::warn!(max_connections, "pgwire connection limit reached");
                        continue;
                    };
                    connections.spawn(async move {
                        let _permit = permit;
                        if let Err(error) = session::process_socket(socket, tls_acceptor, factory, session_limits).await {
                            tracing::warn!(%error, "pgwire connection failed");
                        }
                    });
                }
            }
        }
    }
}

impl BlockingDbExecutor {
    fn new(database: DatabaseHandle, max_blocking_tasks: usize) -> Self {
        Self {
            database,
            permits: Arc::new(Semaphore::new(max_blocking_tasks.max(1))),
        }
    }

    async fn describe_sql_as_user(
        &self,
        user: String,
        sql: String,
    ) -> PgWireResult<Option<SchemaRef>> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| network_to_pg(NetworkError::Runtime(error.to_string())))?;
        let database = self.database.clone();
        let runtime = tokio::runtime::Handle::current();
        spawn_blocking(move || {
            let _permit = permit;
            let describe = |database: &crate::db::Database| {
                let principal = database.principal_for_user(&user);
                runtime
                    .block_on(database.describe_query_as(&principal, &sql))
                    .map_err(|error| db_error_to_pg(&error))
            };
            match database {
                DatabaseHandle::Direct(database) => describe(&database),
                DatabaseHandle::Shared(database) => {
                    let database = runtime.block_on(database.read());
                    describe(&database)
                }
            }
        })
        .await
        .map_err(|error| network_to_pg(NetworkError::from(error)))?
    }

    async fn execute_sql_as_user(
        &self,
        user: String,
        sql: String,
        enforce_rbac: bool,
    ) -> Result<SqlOutput, NetworkError> {
        let started = Instant::now();
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| NetworkError::Runtime(error.to_string()))?;
        let database = self.database.clone();
        let user_for_query = user.clone();
        let runtime = tokio::runtime::Handle::current();

        let result = spawn_blocking(move || {
            let _permit = permit;
            if enforce_rbac {
                match database {
                    DatabaseHandle::Direct(database) => {
                        let principal = database.principal_for_user(&user_for_query);
                        Ok(runtime.block_on(database.query_as(&principal, &sql))?)
                    }
                    DatabaseHandle::Shared(database) => {
                        let database = runtime.block_on(database.read());
                        let principal = database.principal_for_user(&user_for_query);
                        Ok(runtime.block_on(database.query_as(&principal, &sql))?)
                    }
                }
            } else {
                match database {
                    DatabaseHandle::Direct(database) => Ok(runtime.block_on(database.query(&sql))?),
                    DatabaseHandle::Shared(database) => {
                        let database = runtime.block_on(database.read());
                        Ok(runtime.block_on(database.query(&sql))?)
                    }
                }
            }
        })
        .await
        .map_err(NetworkError::from)?;
        observability::record_network_query(
            started,
            &user,
            if result.is_ok() { "ok" } else { "error" },
        );
        result
    }
}

impl DbPgFactory {
    fn new(database: DatabaseHandle, config: NetworkConfig) -> Self {
        let certificate_pem = match &config.tls {
            TlsMode::Require(tls) => Some(tls.certificate_pem()),
            TlsMode::DisabledForTests => None,
        };
        let tls_required = matches!(config.tls, TlsMode::Require(_));
        let executor = BlockingDbExecutor::new(database, config.max_blocking_tasks);
        let auth_limiter =
            AuthRateLimiter::new(config.auth_failure_limit, config.auth_failure_window_ms);

        Self {
            handler: Arc::new(DbPgHandler {
                executor,
                parser: Arc::new(NoopQueryParser),
            }),
            auth: config.auth,
            certificate_pem,
            tls_required,
            auth_limiter,
        }
    }
}

impl PgWireServerHandlers for DbPgFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.handler)
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::clone(&self.handler)
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(DbStartupHandler::new(
            &self.auth,
            self.certificate_pem.as_deref().map(Vec::as_slice),
            self.tls_required,
            self.auth_limiter.clone(),
        ))
    }
}

impl DbStartupHandler {
    fn reserve_auth_attempt(
        &self,
        ip: std::net::IpAddr,
        user: &str,
    ) -> Result<Option<AuthAttempt>, NetworkError> {
        if user.len() > MAX_AUTH_USERNAME_BYTES {
            return Err(NetworkError::Auth(
                "authentication username is too long".to_owned(),
            ));
        }
        // Unknown names cannot authenticate. Let SCRAM return its ordinary failure,
        // without allowing random names to exhaust the shared known-user limiter.
        self.auth
            .contains_user(user)
            .then(|| self.auth_limiter.reserve_attempt(ip, user))
            .transpose()
    }

    fn new(
        auth: &AuthConfig,
        certificate_pem: Option<&[u8]>,
        tls_required: bool,
        auth_limiter: AuthRateLimiter,
    ) -> Self {
        let mut scram = ScramAuth::new(Arc::new(StaticAuthSource { auth: auth.clone() }));
        scram.set_iterations(auth.iterations());
        let startup_error = certificate_pem
            .and_then(|pem| scram.configure_certificate(pem).err())
            .map(|error| error.to_string());

        let inner = startup_error.as_ref().map_or_else(
            || {
                Some(
                    SASLAuthStartupHandler::new(
                        Arc::new(DefaultServerParameterProvider::default()),
                    )
                    .with_scram(scram),
                )
            },
            |_| None,
        );

        Self {
            auth: auth.clone(),
            inner,
            startup_error,
            tls_required,
            auth_limiter,
            pending_auth: StdMutex::new(None),
            sasl_started: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl StartupHandler for DbStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if self.tls_required && !client.is_secure() {
            return Err(pg_error(
                "FATAL",
                "28000",
                "TLS is required for this server",
            ));
        }

        if let Some(error) = &self.startup_error {
            return Err(pg_error("FATAL", "08006", error));
        }

        if let PgWireFrontendMessage::Startup(startup) = &message {
            let user = startup
                .parameters
                .get("user")
                .map_or("unknown", String::as_str);
            let attempt = self
                .reserve_auth_attempt(client.socket_addr().ip(), user)
                .map_err(|error| pg_error("FATAL", "28000", error.to_string()))?;
            let mut pending = self
                .pending_auth
                .lock()
                .map_err(|_| pg_error("FATAL", "08006", "authentication state lock poisoned"))?;
            if pending.is_some() {
                return Err(pg_error(
                    "FATAL",
                    "08P01",
                    "authentication is already in progress",
                ));
            }
            *pending = attempt;
        }

        let Some(inner) = &self.inner else {
            return Err(pg_error("FATAL", "08006", "startup handler is unavailable"));
        };

        if let PgWireFrontendMessage::PasswordMessageFamily(
            pgwire::messages::startup::PasswordMessageFamily::Raw(body),
        ) = &message
            && !self
                .sasl_started
                .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            frame::validate_sasl_initial(body)?;
        }

        let result = inner.on_startup(client, message).await;
        if result.is_err() || matches!(client.state(), PgWireConnectionState::ReadyForQuery) {
            let attempt = self
                .pending_auth
                .lock()
                .map_err(|_| pg_error("FATAL", "08006", "authentication state lock poisoned"))?
                .take();
            // On error, dropping the attempt records the failed exchange.
            if let Some(attempt) = attempt
                && result.is_ok()
            {
                attempt.succeed();
            }
        }
        result
    }
}

#[async_trait]
impl AuthSource for StaticAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let user = login.user().ok_or(PgWireError::UserNameRequired)?;
        let credential = self
            .auth
            .credential(user)
            .ok_or_else(|| PgWireError::InvalidPassword(user.to_owned()))?;

        Ok(Password::new(
            Some(credential.salt.clone()),
            credential.salted_password.clone(),
        ))
    }
}

#[async_trait]
impl SimpleQueryHandler for DbPgHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let user = client_user(client);
        let output = self
            .executor
            .execute_sql_as_user(user, query.to_owned(), true)
            .await
            .map_err(network_to_pg)?;
        Ok(vec![sql_output_to_response(output, query, None)])
    }
}

#[async_trait]
impl ExtendedQueryHandler for DbPgHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::clone(&self.parser)
    }

    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let parameters = target
            .parameter_types
            .iter()
            .map(|ty| ty.clone().unwrap_or(Type::TEXT))
            .collect::<Vec<_>>();

        let sql = describe_sql_for_statement(target)?.unwrap_or_else(|| target.statement.clone());
        let schema = self
            .executor
            .describe_sql_as_user(client_user(client), sql)
            .await?;
        let fields = schema
            .as_ref()
            .map_or_else(Vec::new, |schema| fields_for_schema(schema, None));
        Ok(DescribeStatementResponse::new(parameters, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = substitute_portal_params(target)?;
        let schema = self
            .executor
            .describe_sql_as_user(client_user(client), sql)
            .await?;
        match schema {
            Some(schema) => {
                validate_result_format(&target.result_column_format, schema.fields().len())?;
                Ok(DescribePortalResponse::new(fields_for_schema(
                    &schema,
                    Some(&target.result_column_format),
                )))
            }
            None => Ok(DescribePortalResponse::no_data()),
        }
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = substitute_portal_params(portal)?;
        if matches!(portal.result_column_format, Format::Individual(_)) {
            let schema = self
                .executor
                .describe_sql_as_user(client_user(client), sql.clone())
                .await?;
            validate_result_format(
                &portal.result_column_format,
                schema.as_ref().map_or(0, |schema| schema.fields().len()),
            )?;
        }
        let output = self
            .executor
            .execute_sql_as_user(client_user(client), sql.clone(), true)
            .await
            .map_err(network_to_pg)?;
        if let SqlOutput::Rows(rows) = &output {
            validate_result_format(&portal.result_column_format, rows.columns.len())?;
            if max_rows > 0 {
                let available = client
                    .metadata()
                    .get(session_state::AVAILABLE_RESULT_BYTES)
                    .and_then(|bytes| bytes.parse::<usize>().ok())
                    .unwrap_or(0);
                let retained = session_state::result_bytes(rows, available).map_err(|_| {
                    pg_error(
                        "ERROR",
                        "53200",
                        "portal result exceeds retained session memory budget",
                    )
                })?;
                client.metadata_mut().insert(
                    session_state::NEW_RESULT_BYTES.to_owned(),
                    retained.to_string(),
                );
            }
        }
        Ok(sql_output_to_response(
            output,
            &sql,
            Some(&portal.result_column_format),
        ))
    }
}

impl From<JoinError> for NetworkError {
    fn from(error: JoinError) -> Self {
        Self::Join(error.to_string())
    }
}

fn client_user(client: &impl ClientInfo) -> String {
    client
        .metadata()
        .get(METADATA_USER)
        .cloned()
        .unwrap_or_else(|| "unknown".to_owned())
}

fn sql_output_to_response(
    output: SqlOutput,
    sql: &str,
    result_format: Option<&Format>,
) -> Response {
    match output {
        SqlOutput::Rows(rows) => Response::Query(rows_to_response(rows, result_format)),
        SqlOutput::AffectedRows(rows) => {
            let tag = if starts_with_keyword(sql, "insert") {
                Tag::new("INSERT").with_oid(0).with_rows(rows)
            } else {
                Tag::new("OK").with_rows(rows)
            };
            Response::Execution(tag)
        }
    }
}

fn rows_to_response(rows: SqlRows, result_format: Option<&Format>) -> QueryResponse {
    let fields = Arc::new(fields_for_rows(&rows, result_format));
    let row_fields = Arc::clone(&fields);
    let encoded_rows = rows
        .rows
        .into_iter()
        .map(move |row| encode_row(Arc::clone(&row_fields), &row));

    QueryResponse::new(fields, stream::iter(encoded_rows))
}

fn fields_for_rows(rows: &SqlRows, result_format: Option<&Format>) -> Vec<FieldInfo> {
    rows.columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            FieldInfo::new(
                column.clone(),
                None,
                None,
                pg_type_for_column(&rows.rows, index),
                result_format.map_or(FieldFormat::Text, |format| format.format_for(index)),
            )
        })
        .collect()
}

fn encode_row(
    fields: Arc<Vec<FieldInfo>>,
    row: &Row,
) -> PgWireResult<pgwire::messages::data::DataRow> {
    let mut encoder = DataRowEncoder::new(fields);
    for value in row {
        encode_value_field(&mut encoder, value)?;
    }

    encoder.finish()
}

fn encode_value_field(encoder: &mut DataRowEncoder, value: &Value) -> PgWireResult<()> {
    match value {
        Value::Null => encoder.encode_field(&Option::<String>::None),
        Value::Bool(value) => encoder.encode_field(value),
        Value::Int(value) => encoder.encode_field(value),
        Value::Float(value) => encoder.encode_field(value),
        Value::Str(value) => encoder.encode_field(value),
        Value::Bytes(value) => encoder.encode_field(value),
        Value::Array(_) | Value::Object(_) | Value::Vector(_) | Value::GeoPoint { .. } => {
            let json = serde_json::to_string(value).map_err(|error| {
                pg_error(
                    "ERROR",
                    "XX000",
                    format!("failed to encode JSON value: {error}"),
                )
            })?;
            encoder.encode_field(&json)
        }
    }
}

fn pg_type_for_column(rows: &[Row], column: usize) -> Type {
    rows.iter()
        .filter_map(|row| row.get(column))
        .find(|value| !matches!(value, Value::Null))
        .map_or(Type::TEXT, pg_type_for_value)
}

fn pg_type_for_value(value: &Value) -> Type {
    match value {
        Value::Null | Value::Str(_) => Type::TEXT,
        Value::Bool(_) => Type::BOOL,
        Value::Int(_) => Type::INT8,
        Value::Float(_) => Type::FLOAT8,
        Value::Bytes(_) => Type::BYTEA,
        Value::Array(_) | Value::Object(_) | Value::Vector(_) | Value::GeoPoint { .. } => {
            Type::JSONB
        }
    }
}

fn fields_for_schema(schema: &SchemaRef, result_format: Option<&Format>) -> Vec<FieldInfo> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let ty = match field.data_type() {
                DataType::Boolean => Type::BOOL,
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64 => Type::INT8,
                DataType::Float16 | DataType::Float32 | DataType::Float64 => Type::FLOAT8,
                DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::FixedSizeBinary(_) => Type::BYTEA,
                DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::Struct(_)
                | DataType::Map(_, _) => Type::JSONB,
                _ => Type::TEXT,
            };
            FieldInfo::new(
                field.name().clone(),
                None,
                None,
                ty,
                result_format.map_or(FieldFormat::Text, |format| format.format_for(index)),
            )
        })
        .collect()
}

fn substitute_portal_params(portal: &Portal<String>) -> PgWireResult<String> {
    if parameters::count(&portal.statement.statement)? != portal.parameter_len() {
        return Err(pg_error(
            "ERROR",
            "08P01",
            "bind parameter count does not match statement",
        ));
    }
    let literals = (0..portal.parameter_len())
        .map(|index| portal_param_literal(portal, index))
        .collect::<PgWireResult<Vec<_>>>()?;

    substitute_params(&portal.statement.statement, &literals)
}

fn describe_sql_for_statement(statement: &StoredStatement<String>) -> PgWireResult<Option<String>> {
    if !parameters::contains(&statement.statement)? {
        return Ok(Some(statement.statement.clone()));
    }

    if statement.parameter_types.is_empty() || statement.parameter_types.iter().any(Option::is_none)
    {
        return Ok(None);
    }

    let literals = statement
        .parameter_types
        .iter()
        .map(|ty| dummy_literal_for_type(ty.as_ref()))
        .collect::<PgWireResult<Vec<_>>>()?;
    substitute_params(&statement.statement, &literals).map(Some)
}

fn dummy_literal_for_type(ty: Option<&Type>) -> PgWireResult<String> {
    match ty {
        Some(&Type::INT2 | &Type::INT4 | &Type::INT8) => Ok("0".to_owned()),
        Some(&Type::FLOAT4 | &Type::FLOAT8) => Ok("0.0".to_owned()),
        Some(&Type::BOOL) => Ok("false".to_owned()),
        Some(&Type::BYTEA) => Ok("X''".to_owned()),
        Some(&Type::TEXT | &Type::VARCHAR | &Type::BPCHAR | &Type::NAME | &Type::UNKNOWN) => {
            Ok("''".to_owned())
        }
        Some(unsupported) => Err(pg_error(
            "ERROR",
            "0A000",
            format!("unsupported parameter type {}", unsupported.name()),
        )),
        None => Ok("NULL".to_owned()),
    }
}

fn portal_param_literal(portal: &Portal<String>, index: usize) -> PgWireResult<String> {
    let ty = portal
        .statement
        .parameter_types
        .get(index)
        .and_then(Clone::clone)
        .unwrap_or(Type::TEXT);

    if portal.parameters.get(index).is_none() {
        return Err(PgWireError::ParameterIndexOutOfBound(index));
    }

    if portal.parameters[index].is_none() {
        return Ok("NULL".to_owned());
    }

    match ty {
        Type::INT2 => Ok(portal
            .parameter::<i16>(index, &Type::INT2)?
            .map_or_else(|| "NULL".to_owned(), |value| value.to_string())),
        Type::INT4 => Ok(portal
            .parameter::<i32>(index, &Type::INT4)?
            .map_or_else(|| "NULL".to_owned(), |value| value.to_string())),
        Type::INT8 => Ok(portal
            .parameter::<i64>(index, &Type::INT8)?
            .map_or_else(|| "NULL".to_owned(), |value| value.to_string())),
        Type::FLOAT4 => float_literal(portal.parameter::<f32>(index, &Type::FLOAT4)?),
        Type::FLOAT8 => float_literal(portal.parameter::<f64>(index, &Type::FLOAT8)?),
        Type::BOOL => Ok(portal
            .parameter::<bool>(index, &Type::BOOL)?
            .map_or_else(|| "NULL".to_owned(), |value| value.to_string())),
        Type::BYTEA => Ok(portal
            .parameter::<Vec<u8>>(index, &Type::BYTEA)?
            .map_or_else(|| "NULL".to_owned(), |value| binary_sql_literal(&value))),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN => Ok(portal
            .parameter::<String>(index, &ty)?
            .map_or_else(|| "NULL".to_owned(), |value| escape_sql_literal(&value))),
        unsupported => Err(pg_error(
            "ERROR",
            "0A000",
            format!("unsupported parameter type {}", unsupported.name()),
        )),
    }
}

fn float_literal(value: Option<impl Into<f64>>) -> PgWireResult<String> {
    let Some(value) = value else {
        return Ok("NULL".to_owned());
    };
    let value = value.into();
    if !value.is_finite() {
        return Err(pg_error("ERROR", "22003", "non-finite float parameter"));
    }

    Ok(value.to_string())
}

fn substitute_params(sql: &str, literals: &[String]) -> PgWireResult<String> {
    parameters::substitute(sql, literals)
}

fn escape_sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn binary_sql_literal(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut literal = String::with_capacity(value.len().saturating_mul(2).saturating_add(3));
    literal.push_str("X'");
    for &byte in value {
        literal.push(char::from(HEX[usize::from(byte >> 4)]));
        literal.push(char::from(HEX[usize::from(byte & 15)]));
    }
    literal.push('\'');
    literal
}

fn starts_with_keyword(sql: &str, keyword: &str) -> bool {
    sql.trim_start()
        .get(..keyword.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
}

fn validate_result_format(format: &Format, columns: usize) -> PgWireResult<()> {
    if let Format::Individual(codes) = format
        && (codes.len() != columns || codes.iter().any(|code| !matches!(code, 0 | 1)))
    {
        return Err(pg_error(
            "ERROR",
            "08P01",
            "result format count does not match output columns",
        ));
    }
    Ok(())
}

fn network_to_pg(error: NetworkError) -> PgWireError {
    match error {
        NetworkError::Db(error) => db_error_to_pg(&error),
        other => pg_error("ERROR", "XX000", other.to_string()),
    }
}

fn db_error_to_pg(error: &DbError) -> PgWireError {
    pg_error("ERROR", db_error_sqlstate(error), error.to_string())
}

fn db_error_sqlstate(error: &DbError) -> &'static str {
    compat::sqlstate_for_db_error(error)
}

fn pg_error(
    severity: impl Into<String>,
    code: impl Into<String>,
    message: impl Into<String>,
) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        severity.into(),
        code.into(),
        message.into(),
    )))
}

#[cfg(test)]
mod tests {
    use std::{io, sync::Arc, time::Duration};

    use super::{
        AuthConfig, NetworkConfig, NetworkError, PgServer, ScramCredential, TlsConfig, TlsMode,
        db_error_sqlstate, escape_sql_literal, pg_type_for_value, substitute_params,
    };
    use crate::{
        db::{DbConfig, DbError, Profile, create_database},
        model::Value,
        query::{ColumnDef, ColumnType, QueryError, TableSchema},
        security::{AuthzPolicy, Permission, Principal, PrincipalRegistry, Resource, Role},
        storage::StorageError,
    };
    use pgwire::api::Type;
    use rcgen::{CertifiedKey, generate_simple_self_signed};
    use tokio::{
        net::TcpListener,
        sync::{RwLock, oneshot},
    };
    use tokio_postgres::config::SslMode;
    use tokio_postgres_rustls::MakeRustlsConnect;
    use tokio_rustls::rustls::{ClientConfig, RootCertStore};

    #[test]
    fn pg_type_mapping_matches_value_shape() {
        assert_eq!(pg_type_for_value(&Value::Int(1)), Type::INT8);
        assert_eq!(pg_type_for_value(&Value::Float(1.0)), Type::FLOAT8);
        assert_eq!(pg_type_for_value(&Value::Str("x".to_owned())), Type::TEXT);
        assert_eq!(pg_type_for_value(&Value::Bool(true)), Type::BOOL);
        assert_eq!(pg_type_for_value(&Value::Bytes(vec![1])), Type::BYTEA);
        assert_eq!(pg_type_for_value(&Value::Array(Vec::new())), Type::JSONB);
    }

    #[test]
    fn parameters_are_escaped_without_touching_string_literals()
    -> Result<(), Box<dyn std::error::Error>> {
        let sql = "select '$1', name from users where age = $1 and name = $2";
        let substituted =
            substitute_params(sql, &["37".to_owned(), escape_sql_literal("Ada's laptop")])?;

        assert_eq!(
            substituted,
            "select '$1', name from users where age = 37 and name = 'Ada''s laptop'"
        );

        Ok(())
    }

    #[test]
    fn scram_config_does_not_store_plaintext_password() {
        let credential =
            ScramCredential::from_password_for_tests("secret", b"fixed-test-salt", 4096);
        let auth = AuthConfig::single_user("alice", 4096, credential.clone());

        assert!(auth.contains_user("alice"));
        assert_ne!(credential.salted_password(), b"secret");
        assert_eq!(credential.salt(), b"fixed-test-salt");
        let verifier = format!("{:?}", credential.salted_password());
        assert!(!format!("{credential:?}").contains(&verifier));
        assert!(!format!("{auth:?}").contains(&verifier));
        assert!(format!("{auth:?}").contains("[REDACTED]"));
    }

    #[test]
    fn unknown_startup_names_cannot_fill_known_user_authentication_capacity()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = super::AuthRateLimiter::new(2, 60_000);
        let handler = super::DbStartupHandler::new(&test_auth(), None, false, limiter.clone());
        let ip = "127.0.0.1".parse()?;
        for index in 0..=super::MAX_AUTH_RATE_KEYS {
            assert!(
                handler
                    .reserve_auth_attempt(ip, &format!("unknown-{index}"))?
                    .is_none()
            );
        }
        assert_eq!(limiter.state.lock().map_err(|_| "limiter lock")?.len(), 0);
        let known = handler
            .reserve_auth_attempt(ip, "alice")?
            .ok_or("known user was not reserved")?;
        known.succeed();
        assert_eq!(limiter.state.lock().map_err(|_| "limiter lock")?.len(), 0);
        Ok(())
    }

    #[test]
    fn database_errors_map_to_sqlstate() {
        for error in [
            crate::tensor::TensorError::Storage(StorageError::Conflict),
            crate::tensor::TensorError::Repl(crate::repl::ReplError::Conflict),
            crate::tensor::TensorError::Repl(crate::repl::ReplError::Storage(
                StorageError::Conflict,
            )),
        ] {
            let error = DbError::Tensor(error);
            assert!(error.is_conflict());
            assert_eq!(db_error_sqlstate(&error), "40001");
        }
        assert_eq!(
            db_error_sqlstate(&DbError::Tensor(crate::tensor::TensorError::Timeout(
                "budget".to_owned()
            ))),
            "57014"
        );
        assert_eq!(
            db_error_sqlstate(&DbError::Query(QueryError::Parse("bad".to_owned()))),
            "42601"
        );
        assert_eq!(
            db_error_sqlstate(&DbError::Query(QueryError::Unsupported("copy".to_owned()))),
            "0A000"
        );
        assert_eq!(
            db_error_sqlstate(&DbError::Storage(StorageError::Conflict)),
            "40001"
        );
    }

    #[test]
    fn invalid_individual_result_formats_are_errors_instead_of_indexing_panics() {
        use pgwire::api::portal::Format;
        assert!(super::validate_result_format(&Format::Individual(vec![0, 1]), 3).is_err());
        assert!(super::validate_result_format(&Format::Individual(vec![0, 2]), 2).is_err());
        assert!(super::validate_result_format(&Format::Individual(vec![0, 1]), 2).is_ok());
    }

    #[test]
    fn network_error_keeps_runtime_failures_separate() {
        let error = NetworkError::Runtime("pool closed".to_owned());
        assert!(error.to_string().contains("runtime"));
    }

    #[test]
    fn auth_rate_limiter_rejects_failed_exchanges() -> Result<(), Box<dyn std::error::Error>> {
        let limiter = super::AuthRateLimiter::new(2, 60_000);
        let ip = "127.0.0.1".parse()?;

        drop(limiter.reserve_attempt(ip, "alice")?);
        drop(limiter.reserve_attempt(ip, "alice")?);
        assert!(matches!(
            limiter.reserve_attempt(ip, "alice"),
            Err(NetworkError::Auth(_))
        ));
        limiter.reserve_attempt(ip, "bob")?.succeed();
        Ok(())
    }

    #[test]
    fn successful_authentication_releases_its_reservation_without_spending_failure_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = super::AuthRateLimiter::new(2, 60_000);
        let ip = "127.0.0.1".parse()?;
        for _ in 0..20 {
            limiter.reserve_attempt(ip, "alice")?.succeed();
        }
        assert_eq!(
            limiter.state.lock().map_err(|_| "limiter poisoned")?.len(),
            0
        );
        let first = limiter.reserve_attempt(ip, "alice")?;
        let second = limiter.reserve_attempt(ip, "alice")?;
        assert!(limiter.reserve_attempt(ip, "alice").is_err());
        first.succeed();
        let third = limiter.reserve_attempt(ip, "alice")?;
        drop(second);
        drop(third);
        assert!(limiter.reserve_attempt(ip, "alice").is_err());
        assert!(limiter.reserve_attempt(ip, &"u".repeat(257)).is_err());
        Ok(())
    }

    async fn plain_client(
        addr: std::net::SocketAddr,
        password: &str,
    ) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
        let (client, connection) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(addr.port())
            .user("alice")
            .password(password)
            .dbname("multidb")
            .ssl_mode(SslMode::Disable)
            .connect(tokio_postgres::NoTls)
            .await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }

    #[tokio::test]
    async fn named_prepared_statement_storm_is_bounded_per_connection()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = NetworkConfig::new(TlsMode::DisabledForTests, test_auth());
        let server = start_session_test_server(config).await?;
        let client = plain_client(server.addr, "secret").await?;
        let mut retained = Vec::new();
        for _ in 0..128 {
            retained.push(
                client
                    .prepare("select name from users where age = 37")
                    .await?,
            );
        }
        assert!(
            client
                .prepare("select name from users where age = 37")
                .await
                .is_err()
        );
        // One over-budget session does not prevent another authenticated client.
        let healthy = plain_client(server.addr, "secret").await?;
        assert!(healthy.simple_query("select name from users").await.is_ok());
        drop(retained);
        server.shutdown().await?;
        Ok(())
    }

    async fn start_session_test_server(
        config: NetworkConfig,
    ) -> Result<TestServer, Box<dyn std::error::Error>> {
        let database = Arc::new(shared_users_database()?);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (shutdown, receive) = oneshot::channel();
        let handle = tokio::spawn(PgServer::serve_until_shutdown(
            listener,
            database,
            config,
            async {
                let _ = receive.await;
            },
        ));
        Ok(TestServer {
            addr,
            shutdown,
            handle,
        })
    }

    #[tokio::test]
    async fn active_session_outlives_request_ceiling_but_idle_session_expires()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = NetworkConfig::new(TlsMode::DisabledForTests, test_auth())
            .with_connection_timeout_ms(250)
            .with_handshake_timeout_ms(2_000)
            .with_idle_timeout_ms(150);
        let server = start_session_test_server(config).await?;
        let client = plain_client(server.addr, "secret").await?;
        // Six individually short requests span more than the former total-session ceiling.
        for _ in 0..6 {
            assert_eq!(
                client.simple_query("select name from users").await?.len(),
                3
            );
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(client.simple_query("select name from users").await.is_err());
        server.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn idle_budget_is_suspended_while_a_request_waits_for_database_access()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(RwLock::new(shared_users_database()?));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (stop, receive) = oneshot::channel();
        let config = NetworkConfig::new(TlsMode::DisabledForTests, test_auth())
            .with_connection_timeout_ms(2_000)
            .with_handshake_timeout_ms(2_000)
            .with_idle_timeout_ms(50);
        let task = tokio::spawn(PgServer::serve_shared_until_shutdown(
            listener,
            Arc::clone(&database),
            config,
            async {
                let _ = receive.await;
            },
        ));
        let client = Arc::new(plain_client(addr, "secret").await?);
        let writer = database.write().await;
        let query = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.simple_query("select name from users").await }
        });
        tokio::time::sleep(Duration::from_millis(150)).await;
        drop(writer);
        assert_eq!(query.await??.len(), 3);
        assert_eq!(
            client.simple_query("select name from users").await?.len(),
            3
        );
        let _ = stop.send(());
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn request_processing_ceiling_closes_a_blocked_session()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(RwLock::new(shared_users_database()?));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (stop, receive) = oneshot::channel();
        let config = NetworkConfig::new(TlsMode::DisabledForTests, test_auth())
            .with_connection_timeout_ms(100)
            .with_handshake_timeout_ms(2_000)
            .with_idle_timeout_ms(2_000);
        let task = tokio::spawn(PgServer::serve_shared_until_shutdown(
            listener,
            Arc::clone(&database),
            config,
            async {
                let _ = receive.await;
            },
        ));
        let client = plain_client(addr, "secret").await?;
        let writer = database.write().await;
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            client.simple_query("select name from users"),
        )
        .await?;
        assert!(result.is_err());
        // Timed-out socket work may already be running in spawn_blocking. Release
        // its database lock before graceful shutdown drains the executor permits.
        drop(writer);
        let _ = stop.send(());
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn handshake_deadline_and_frontend_frame_cap_apply_to_real_sockets()
    -> Result<(), Box<dyn std::error::Error>> {
        use tokio::io::AsyncReadExt;
        let config = NetworkConfig::new(TlsMode::DisabledForTests, test_auth())
            .with_handshake_timeout_ms(100)
            .with_max_frame_bytes(1024);
        let server = start_session_test_server(config).await?;
        let mut stalled = tokio::net::TcpStream::connect(server.addr).await?;
        let mut byte = [0_u8];
        let read = tokio::time::timeout(Duration::from_secs(2), stalled.read(&mut byte)).await??;
        assert_eq!(read, 0);
        let client = plain_client(server.addr, "secret").await?;
        client.simple_query("select name from users").await?;
        assert!(
            client
                .simple_query(&format!("SELECT '{}'", "x".repeat(2048)))
                .await
                .is_err()
        );
        server.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn successful_pgwire_logins_do_not_trigger_failed_authentication_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = NetworkConfig::new(TlsMode::DisabledForTests, test_auth())
            .with_auth_failure_limit(2, 60_000);
        let server = start_session_test_server(config).await?;
        for _ in 0..12 {
            plain_client(server.addr, "secret")
                .await?
                .simple_query("select name from users")
                .await?;
        }
        assert!(plain_client(server.addr, "wrong").await.is_err());
        // A valid login still fits the one remaining failure slot, and does not spend it.
        plain_client(server.addr, "secret")
            .await?
            .simple_query("select name from users")
            .await?;
        assert!(plain_client(server.addr, "wrong").await.is_err());
        let error = plain_client(server.addr, "secret")
            .await
            .err()
            .ok_or("failed auth throttle was bypassed")?;
        server.shutdown().await?;
        let database_error = error
            .as_db_error()
            .ok_or_else(|| format!("expected a pgwire authentication rejection, got {error:?}"))?;
        assert_eq!(database_error.code().code(), "28000");
        assert!(
            database_error
                .message()
                .contains("authentication rate limit exceeded"),
            "unexpected authentication rejection: {database_error:?}"
        );
        Ok(())
    }

    async fn executor_uses_the_existing_runtime() -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(RwLock::new(shared_users_database()?));
        let executor = super::BlockingDbExecutor::new(super::DatabaseHandle::Shared(database), 2);
        assert!(
            executor
                .describe_sql_as_user("alice".to_owned(), "select name from users".to_owned())
                .await?
                .is_some()
        );
        let output = executor
            .execute_sql_as_user(
                "alice".to_owned(),
                "select name from users".to_owned(),
                true,
            )
            .await?;
        assert!(
            matches!(output, crate::query::SqlOutput::Rows(rows) if rows.rows == vec![vec![Value::Str("Ada".to_owned())]])
        );
        Ok(())
    }

    #[tokio::test]
    async fn query_and_describe_work_on_an_existing_current_thread_runtime()
    -> Result<(), Box<dyn std::error::Error>> {
        executor_uses_the_existing_runtime().await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_and_describe_work_on_an_existing_multi_thread_runtime()
    -> Result<(), Box<dyn std::error::Error>> {
        executor_uses_the_existing_runtime().await
    }

    #[tokio::test]
    async fn postgres_client_runs_tls_scram_simple_extended_and_insert()
    -> Result<(), Box<dyn std::error::Error>> {
        let TestTls {
            server,
            client_cert,
        } = test_tls()?;
        let server = start_test_server(TlsMode::Require(server)).await?;
        let client = connect_client(server.addr, client_cert, "secret").await?;

        let rows = client
            .query("select name from users order by id", &[])
            .await?;
        let first = rows
            .first()
            .ok_or_else(|| io::Error::other("missing first row"))?;
        let name: String = first.try_get(0)?;
        assert_eq!(name, "Ada");

        let statement = client
            .prepare_typed(
                "select name from users where age = $1",
                &[tokio_postgres::types::Type::INT8],
            )
            .await?;
        let rows = client.query(&statement, &[&37_i64]).await?;
        let first = rows
            .first()
            .ok_or_else(|| io::Error::other("missing filtered row"))?;
        let name: String = first.try_get(0)?;
        assert_eq!(name, "Ada");

        let insert = client
            .prepare_typed(
                "insert into users (id, name, age, active) values ($1, $2, $3, $4)",
                &[
                    tokio_postgres::types::Type::INT8,
                    tokio_postgres::types::Type::TEXT,
                    tokio_postgres::types::Type::INT8,
                    tokio_postgres::types::Type::BOOL,
                ],
            )
            .await?;
        let inserted = client
            .execute(&insert, &[&2_i64, &"Grace", &85_i64, &true])
            .await?;
        assert_eq!(inserted, 1);

        let rows = client.query("select count(*) from users", &[]).await?;
        let first = rows
            .first()
            .ok_or_else(|| io::Error::other("missing count row"))?;
        let count: i64 = first.try_get(0)?;
        assert_eq!(count, 2);

        server.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn postgres_client_read_only_role_gets_42501_on_insert()
    -> Result<(), Box<dyn std::error::Error>> {
        let TestTls {
            server,
            client_cert,
        } = test_tls()?;
        let server = start_test_server_with_write_access(TlsMode::Require(server), false).await?;
        let client = connect_client(server.addr, client_cert, "secret").await?;

        let rows = client.query("select name from users", &[]).await?;
        assert!(!rows.is_empty());

        let error = client
            .execute(
                "insert into users (id, name, age, active) values (2, 'Grace', 85, true)",
                &[],
            )
            .await
            .err()
            .ok_or_else(|| io::Error::other("insert unexpectedly succeeded"))?;
        assert_eq!(
            error.code(),
            Some(&tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE)
        );

        server.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn typed_bytea_bind_matches_binary_data_without_text_coercion()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut database = shared_users_database()?;
        {
            let blobs = database.create_table(
                "blobs",
                Some(TableSchema::new(
                    vec![
                        ColumnDef::new("id", ColumnType::Int, false),
                        ColumnDef::new("payload", ColumnType::Bytes, true),
                    ],
                    0,
                )),
                Vec::new(),
            )?;
            blobs.insert(vec![Value::Int(1), Value::Bytes(vec![0, 255, 39, 92])])?;
            blobs.insert(vec![Value::Int(2), Value::Bytes(b"00ff275c".to_vec())])?;
        }
        database.set_authz_policy(AuthzPolicy::new([
            Role::new("user").grant(Resource::Table("blobs".to_owned()), Permission::Read)
        ]));
        let database = Arc::new(RwLock::new(database));
        let server = start_shared_test_server(TlsMode::DisabledForTests, database).await?;
        let client = plain_client(server.addr, "secret").await?;
        let statement = client
            .prepare_typed("SELECT id FROM blobs WHERE payload = $1", &[Type::BYTEA])
            .await?;
        let rows = client.query(&statement, &[&vec![0u8, 255, 39, 92]]).await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].try_get::<_, i64>(0)?, 1);
        server.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn untyped_describe_requires_read_and_does_not_execute_queries()
    -> Result<(), Box<dyn std::error::Error>> {
        let TestTls {
            server,
            client_cert,
        } = test_tls()?;
        let mut database = shared_users_database()?;
        database.create_table("secrets", Some(user_schema()), Vec::new())?;
        let database = Arc::new(RwLock::new(database));
        let server =
            start_shared_test_server(TlsMode::Require(server), Arc::clone(&database)).await?;
        let client = connect_client(server.addr, client_cert, "secret").await?;

        // tokio-postgres prepare sends Parse with no parameter OIDs and Describe Statement.
        // Previously the missing OIDs selected the schema fallback without checking RBAC.
        let error = client
            .prepare("select * from secrets where id = $1")
            .await
            .err()
            .ok_or_else(|| io::Error::other("ungranted schema was disclosed"))?;
        assert_eq!(
            error.code(),
            Some(&tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE)
        );
        let statement = client
            .prepare("select name, age from users where age = $1")
            .await?;
        assert_eq!(statement.columns()[0].name(), "name");
        assert_eq!(
            statement.columns()[1].type_(),
            &tokio_postgres::types::Type::INT8
        );

        // There is a row with age=37. Preparing metadata must not evaluate its division by zero.
        let statement = client
            .prepare("select 10 / (age - 37) as quotient from users")
            .await?;
        assert_eq!(statement.columns()[0].name(), "quotient");
        assert_eq!(
            statement.columns()[0].type_(),
            &tokio_postgres::types::Type::INT8
        );
        server.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn describe_executor_checks_current_grants() -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(RwLock::new(shared_users_database()?));
        let executor =
            super::BlockingDbExecutor::new(super::DatabaseHandle::Shared(Arc::clone(&database)), 2);
        let sql = "select name from users".to_owned();
        assert!(
            executor
                .describe_sql_as_user("alice".to_owned(), sql.clone())
                .await?
                .is_some()
        );
        database
            .write()
            .await
            .set_authz_policy(AuthzPolicy::new([]));
        let error = executor
            .describe_sql_as_user("alice".to_owned(), sql)
            .await
            .err()
            .ok_or_else(|| io::Error::other("revoked schema was disclosed"))?;
        let pgwire::error::PgWireError::UserError(info) = error else {
            return Err(io::Error::other("expected authorization error").into());
        };
        assert_eq!(info.code, "42501");
        Ok(())
    }

    #[tokio::test]
    async fn plaintext_is_rejected_when_tls_is_required() -> Result<(), Box<dyn std::error::Error>>
    {
        let TestTls { server, .. } = test_tls()?;
        let server = start_test_server(TlsMode::Require(server)).await?;
        let error = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=alice password=secret sslmode=disable",
                server.addr.port()
            ),
            tokio_postgres::NoTls,
        )
        .await
        .err()
        .ok_or_else(|| io::Error::other("plaintext connection unexpectedly succeeded"))?;

        assert!(error.is_closed() || error.code().is_some());
        server.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn bad_password_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let TestTls {
            server,
            client_cert,
        } = test_tls()?;
        let server = start_test_server(TlsMode::Require(server)).await?;
        let error = connect_client(server.addr, client_cert, "wrong")
            .await
            .err()
            .ok_or_else(|| io::Error::other("bad password unexpectedly succeeded"))?;

        let error = error
            .downcast_ref::<tokio_postgres::Error>()
            .ok_or_else(|| io::Error::other("expected tokio-postgres error"))?;
        assert_eq!(
            error.code(),
            Some(&tokio_postgres::error::SqlState::INVALID_PASSWORD)
        );
        server.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn shared_handle_runs_statements_while_a_reader_holds_it_and_waits_for_writers()
    -> Result<(), Box<dyn std::error::Error>> {
        let TestTls {
            server,
            client_cert,
        } = test_tls()?;
        let database = Arc::new(RwLock::new(shared_users_database()?));
        let server =
            start_shared_test_server(TlsMode::Require(server), Arc::clone(&database)).await?;

        // Statements share the read guard, so a held reader does not block them.
        let reader = database.read().await;
        let client = connect_client(server.addr, client_cert, "secret").await?;
        let rows = tokio::time::timeout(
            Duration::from_secs(10),
            client.query("select name from users", &[]),
        )
        .await??;
        assert_eq!(rows.len(), 1);
        drop(reader);

        // A held writer pauses new statements until it is released.
        let writer = database.write().await;
        let client = Arc::new(client);
        let mut count = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.query_one("select count(*) from users", &[]).await }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut count)
                .await
                .is_err()
        );
        drop(writer);
        let row = tokio::time::timeout(Duration::from_secs(10), count).await???;
        let count: i64 = row.try_get(0)?;
        assert_eq!(count, 1);

        server.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn server_shutdown_closes_idle_connections() -> Result<(), Box<dyn std::error::Error>> {
        let TestTls {
            server,
            client_cert,
        } = test_tls()?;
        let server = start_test_server(TlsMode::Require(server)).await?;
        let client = connect_client(server.addr, client_cert, "secret").await?;
        client.simple_query("select name from users").await?;
        tokio::time::timeout(Duration::from_secs(10), server.shutdown()).await??;
        assert!(client.simple_query("select name from users").await.is_err());
        Ok(())
    }

    async fn start_shared_test_server(
        tls: TlsMode,
        database: super::SharedDatabase,
    ) -> Result<TestServer, Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (shutdown, shutdown_rx) = oneshot::channel();
        let config = NetworkConfig::new(tls, test_auth()).with_max_blocking_tasks(4);
        let handle = tokio::spawn(PgServer::serve_shared_until_shutdown(
            listener,
            database,
            config,
            async {
                let _ = shutdown_rx.await;
            },
        ));

        Ok(TestServer {
            addr,
            shutdown,
            handle,
        })
    }

    fn shared_users_database() -> Result<crate::db::Database, Box<dyn std::error::Error>> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        let users = database.create_table("users", Some(user_schema()), Vec::new())?;
        users.insert(vec![
            Value::Int(1),
            Value::Str("Ada".to_owned()),
            Value::Int(37),
            Value::Bool(true),
        ])?;
        let user_role = Role::new("user")
            .grant(Resource::Table("users".to_owned()), Permission::Read)
            .grant(Resource::Table("users".to_owned()), Permission::Write);
        let mut principals = PrincipalRegistry::new();
        principals.insert("alice", Principal::new("alice").with_role("user"));
        database.set_authz_policy(AuthzPolicy::new([user_role]));
        database.set_principal_registry(principals);
        Ok(database)
    }

    struct TestTls {
        server: TlsConfig,
        client_cert: rustls_pki_types::CertificateDer<'static>,
    }

    struct TestServer {
        addr: std::net::SocketAddr,
        shutdown: oneshot::Sender<()>,
        handle: tokio::task::JoinHandle<Result<(), NetworkError>>,
    }

    impl TestServer {
        async fn shutdown(self) -> Result<(), Box<dyn std::error::Error>> {
            let _ = self.shutdown.send(());
            self.handle.await??;
            Ok(())
        }
    }

    fn test_tls() -> Result<TestTls, Box<dyn std::error::Error>> {
        let CertifiedKey { cert, signing_key } =
            generate_simple_self_signed(vec!["localhost".to_owned()])?;
        let cert_pem = cert.pem();
        let key_pem = signing_key.serialize_pem();
        let client_cert = cert.der().clone();

        let temp_dir = tempfile::tempdir()?;
        let cert_path = temp_dir.path().join("server.crt");
        let key_path = temp_dir.path().join("server.key");
        std::fs::write(&cert_path, cert_pem)?;
        std::fs::write(&key_path, key_pem)?;

        Ok(TestTls {
            server: TlsConfig::from_pem_files(cert_path, key_path)?,
            client_cert,
        })
    }

    async fn start_test_server(tls: TlsMode) -> Result<TestServer, Box<dyn std::error::Error>> {
        start_test_server_with_write_access(tls, true).await
    }

    async fn start_test_server_with_write_access(
        tls: TlsMode,
        write_allowed: bool,
    ) -> Result<TestServer, Box<dyn std::error::Error>> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        let users = database.create_table("users", Some(user_schema()), Vec::new())?;
        users.insert(vec![
            Value::Int(1),
            Value::Str("Ada".to_owned()),
            Value::Int(37),
            Value::Bool(true),
        ])?;
        let mut user_role =
            Role::new("user").grant(Resource::Table("users".to_owned()), Permission::Read);
        if write_allowed {
            user_role = user_role.grant(Resource::Table("users".to_owned()), Permission::Write);
        }
        let mut principals = PrincipalRegistry::new();
        principals.insert("alice", Principal::new("alice").with_role("user"));
        database.set_authz_policy(AuthzPolicy::new([user_role]));
        database.set_principal_registry(principals);

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (shutdown, shutdown_rx) = oneshot::channel();
        let config = NetworkConfig::new(tls, test_auth()).with_max_blocking_tasks(4);
        let handle = tokio::spawn(PgServer::serve_until_shutdown(
            listener,
            Arc::new(database),
            config,
            async {
                let _ = shutdown_rx.await;
            },
        ));

        Ok(TestServer {
            addr,
            shutdown,
            handle,
        })
    }

    async fn connect_client(
        addr: std::net::SocketAddr,
        client_cert: rustls_pki_types::CertificateDer<'static>,
        password: &str,
    ) -> Result<tokio_postgres::Client, Box<dyn std::error::Error>> {
        let mut roots = RootCertStore::empty();
        roots.add(client_cert)?;

        let mut tls_config = ClientConfig::builder_with_provider(Arc::new(
            tokio_rustls::rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
        tls_config.alpn_protocols = vec![b"postgresql".to_vec()];
        let tls = MakeRustlsConnect::new(tls_config);

        let mut config = tokio_postgres::Config::new();
        config
            .host("localhost")
            .hostaddr(addr.ip())
            .port(addr.port())
            .user("alice")
            .password(password)
            .dbname("multidb")
            .ssl_mode(SslMode::Require);

        let (client, connection) = config.connect(tls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        Ok(client)
    }

    fn test_auth() -> AuthConfig {
        AuthConfig::single_user(
            "alice",
            4096,
            ScramCredential::from_password_for_tests("secret", b"fixed-test-salt", 4096),
        )
    }

    fn user_schema() -> TableSchema {
        TableSchema::new(
            vec![
                ColumnDef::new("id", ColumnType::Int, false),
                ColumnDef::new("name", ColumnType::Str, false),
                ColumnDef::new("age", ColumnType::Int, false),
                ColumnDef::new("active", ColumnType::Bool, true),
            ],
            0,
        )
    }

    #[tokio::test]
    async fn bound_text_parameters_cannot_change_statement_structure()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(RwLock::new(shared_users_database()?));
        let server = start_shared_test_server(TlsMode::DisabledForTests, database).await?;
        let client = plain_client(server.addr, "secret").await?;

        // The engine printed `name = '\'' or 1=1 --'` back as `name = '\' or 1=1 --'`,
        // so the rest of the value ran as SQL and commented out `age = $2`.
        let filter = client
            .prepare_typed(
                "select name from users where name = $1 and age = $2",
                &[Type::TEXT, Type::INT8],
            )
            .await?;
        for payload in ["\\' or 1=1 --", "\\'' or 1=1 --", "x' or 'a'='a", "\\"] {
            let rows = client.query(&filter, &[&payload, &37_i64]).await?;
            assert!(rows.is_empty(), "{payload:?} matched {} rows", rows.len());
        }
        // A text parameter in an alias position is a single-quoted name.
        let alias = client
            .prepare_typed(
                "select age as $1 from users where age = $2",
                &[Type::TEXT, Type::INT8],
            )
            .await?;
        match client
            .query(&alias, &[&"a\\' , name from users --", &99_i64])
            .await
        {
            Ok(rows) => assert!(rows.is_empty(), "alias matched {} rows", rows.len()),
            Err(error) => assert_eq!(
                error.code(),
                Some(&tokio_postgres::error::SqlState::FEATURE_NOT_SUPPORTED)
            ),
        }
        let rows = client.query(&alias, &[&"years", &37_i64]).await?;
        let ages = rows
            .iter()
            .map(|row| row.try_get::<_, i64>(0))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(ages, vec![37]);

        // Values holding quotes, backslashes and comment markers round-trip.
        let insert = client
            .prepare_typed(
                "insert into users (id, name, age, active) values ($1, $2, 1, true)",
                &[Type::INT8, Type::TEXT],
            )
            .await?;
        let lookup = client
            .prepare_typed("select id, name from users where name = $1", &[Type::TEXT])
            .await?;
        for (id, value) in [
            (2_i64, "it's \\' -- x"),
            (3, "\\'' or 1=1 --"),
            (4, "a''b\\\\'c"),
        ] {
            client.execute(&insert, &[&id, &value]).await?;
            let rows = client.query(&lookup, &[&value]).await?;
            let found = rows
                .iter()
                .map(|row| Ok((row.try_get::<_, i64>(0)?, row.try_get::<_, String>(1)?)))
                .collect::<Result<Vec<_>, tokio_postgres::Error>>()?;
            assert_eq!(found, vec![(id, value.to_owned())], "{value:?}");
        }
        server.shutdown().await?;
        Ok(())
    }
}
