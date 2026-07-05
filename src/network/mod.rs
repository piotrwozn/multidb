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
use futures::{Sink, stream};
use pgwire::{
    api::{
        ClientInfo, ClientPortalStore, METADATA_USER, PgWireServerHandlers, Type,
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
    tokio::process_socket,
};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use sqlparser::{
    ast::{
        Expr as SqlExpr, ObjectName, ObjectNamePart, SelectItem, SetExpr, Statement, TableFactor,
    },
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use tokio::{
    net::TcpListener,
    sync::Mutex,
    sync::Semaphore,
    task::{JoinError, spawn_blocking},
};
use tokio_rustls::{TlsAcceptor, rustls::ServerConfig};

use crate::{
    compat,
    db::DbError,
    model::Value,
    observability,
    query::{ColumnType, Row, SqlOutput, SqlRows},
};

const POSTGRESQL_ALPN: &[u8] = b"postgresql";
const DEFAULT_BLOCKING_TASKS: usize = 32;
const DEFAULT_MAX_CONNECTIONS: usize = 512;
const DEFAULT_CONNECTION_TIMEOUT_MS: u64 = 300_000;
const DEFAULT_HANDSHAKE_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_IDLE_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_AUTH_FAILURE_LIMIT: usize = 10;
const DEFAULT_AUTH_FAILURE_WINDOW_MS: u64 = 60_000;

#[derive(Clone)]
pub struct NetworkConfig {
    pub tls: TlsMode,
    pub auth: AuthConfig,
    pub max_blocking_tasks: usize,
    pub max_connections: usize,
    pub connection_timeout_ms: u64,
    pub handshake_timeout_ms: u64,
    pub idle_timeout_ms: u64,
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

#[derive(Clone, Debug, PartialEq, Eq)]
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
    inner: Option<SASLAuthStartupHandler<DefaultServerParameterProvider>>,
    startup_error: Option<String>,
    tls_required: bool,
    auth_limiter: AuthRateLimiter,
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
    attempts: usize,
}

pub type SharedDatabase = Arc<Mutex<crate::db::Database>>;

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

    fn record_attempt(&self, ip: std::net::IpAddr, user: &str) -> Result<(), NetworkError> {
        let key = AuthRateKey {
            ip,
            user: user.to_owned(),
        };
        let now = Instant::now();
        let mut state = self
            .state
            .lock()
            .map_err(|_| NetworkError::Runtime("auth rate limiter lock poisoned".to_owned()))?;
        let entry = state.entry(key).or_insert(AuthRateEntry {
            window_start: now,
            attempts: 0,
        });
        if now.duration_since(entry.window_start) >= self.window {
            entry.window_start = now;
            entry.attempts = 0;
        }
        entry.attempts = entry.attempts.saturating_add(1);
        if entry.attempts > self.limit {
            return Err(NetworkError::Auth(
                "authentication rate limit exceeded".to_owned(),
            ));
        }
        Ok(())
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
        let connection_timeout_ms = config.connection_timeout_ms.max(1);
        let connection_timeout = Duration::from_millis(connection_timeout_ms);
        let connection_permits = Arc::new(Semaphore::new(max_connections));
        let factory = Arc::new(DbPgFactory::new(database, config));
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                () = &mut shutdown => return Ok(()),
                accepted = listener.accept() => {
                    let (socket, _) = accepted?;
                    let tls_acceptor = tls_acceptor.clone();
                    let factory = Arc::clone(&factory);
                    let permits = Arc::clone(&connection_permits);
                    let Ok(permit) = permits.try_acquire_owned() else {
                        tracing::warn!(max_connections, "pgwire connection limit reached");
                        continue;
                    };
                    tokio::spawn(async move {
                        let _permit = permit;
                        match tokio::time::timeout(
                            connection_timeout,
                            process_socket(socket, tls_acceptor, factory),
                        )
                        .await
                        {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => {
                                tracing::warn!(error = %error, "pgwire connection failed");
                            }
                            Err(_) => {
                                tracing::warn!(
                                    timeout_ms = connection_timeout_ms,
                                    "pgwire connection timed out"
                                );
                            }
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

    async fn fields_from_select_sql(&self, sql: String) -> PgWireResult<Option<Vec<FieldInfo>>> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| network_to_pg(NetworkError::Runtime(error.to_string())))?;
        let database = self.database.clone();
        let result = spawn_blocking(move || {
            let _permit = permit;
            match database {
                DatabaseHandle::Direct(database) => {
                    fields_from_select_sql(&sql, &database).map_err(|error| error.to_string())
                }
                DatabaseHandle::Shared(database) => {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|error| error.to_string())?;
                    let database = runtime.block_on(database.lock());
                    fields_from_select_sql(&sql, &database).map_err(|error| error.to_string())
                }
            }
        })
        .await
        .map_err(NetworkError::from)
        .map_err(network_to_pg)?;

        result.map_err(|error| pg_error("ERROR", "XX000", error))
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

        let result = spawn_blocking(move || {
            let _permit = permit;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| NetworkError::Runtime(error.to_string()))?;
            if enforce_rbac {
                match database {
                    DatabaseHandle::Direct(database) => {
                        let principal = database.principal_for_user(&user_for_query);
                        Ok(runtime.block_on(database.query_as(&principal, &sql))?)
                    }
                    DatabaseHandle::Shared(database) => {
                        let database = runtime.block_on(database.lock());
                        let principal = database.principal_for_user(&user_for_query);
                        Ok(runtime.block_on(database.query_as(&principal, &sql))?)
                    }
                }
            } else {
                match database {
                    DatabaseHandle::Direct(database) => Ok(runtime.block_on(database.query(&sql))?),
                    DatabaseHandle::Shared(database) => {
                        let database = runtime.block_on(database.lock());
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
            inner,
            startup_error,
            tls_required,
            auth_limiter,
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
            self.auth_limiter
                .record_attempt(client.socket_addr().ip(), user)
                .map_err(|error| pg_error("FATAL", "28000", error.to_string()))?;
        }

        let Some(inner) = &self.inner else {
            return Err(pg_error("FATAL", "08006", "startup handler is unavailable"));
        };

        inner.on_startup(client, message).await
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

        if is_select_like(&target.statement)
            && let Some(describe_sql) = describe_sql_for_statement(target)?
        {
            let output = self
                .executor
                .execute_sql_as_user(client_user(client), describe_sql, true)
                .await
                .map_err(network_to_pg)?;
            if let SqlOutput::Rows(rows) = output {
                if rows.columns.is_empty()
                    && let Some(fields) = self
                        .executor
                        .fields_from_select_sql(target.statement.clone())
                        .await?
                {
                    return Ok(DescribeStatementResponse::new(parameters, fields));
                }
                return Ok(DescribeStatementResponse::new(
                    parameters,
                    fields_for_rows(&rows, None),
                ));
            }
        }

        if is_select_like(&target.statement)
            && let Some(fields) = self
                .executor
                .fields_from_select_sql(target.statement.clone())
                .await?
        {
            return Ok(DescribeStatementResponse::new(parameters, fields));
        }

        Ok(DescribeStatementResponse::new(parameters, Vec::new()))
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
        if !is_select_like(&sql) {
            return Ok(DescribePortalResponse::no_data());
        }

        let output = self
            .executor
            .execute_sql_as_user(client_user(client), sql, true)
            .await
            .map_err(network_to_pg)?;
        if let SqlOutput::Rows(rows) = output {
            return Ok(DescribePortalResponse::new(fields_for_rows(
                &rows,
                Some(&target.result_column_format),
            )));
        }

        Ok(DescribePortalResponse::no_data())
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = substitute_portal_params(portal)?;
        let output = self
            .executor
            .execute_sql_as_user(client_user(client), sql.clone(), true)
            .await
            .map_err(network_to_pg)?;
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

fn fields_from_select_sql(
    sql: &str,
    database: &crate::db::Database,
) -> PgWireResult<Option<Vec<FieldInfo>>> {
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .map_err(|error| pg_error("ERROR", "42601", error.to_string()))?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return Ok(None);
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    let Some(from) = select.from.first() else {
        return Ok(None);
    };
    let TableFactor::Table { name, .. } = &from.relation else {
        return Ok(None);
    };
    let table_name = object_name_to_string(name)?;
    let table = database
        .table(&table_name)
        .map_err(|error| db_error_to_pg(&error))?;
    let Some(schema) = table.schema() else {
        return Ok(None);
    };

    let mut fields = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                fields.extend(schema.columns.iter().map(|column| {
                    FieldInfo::new(
                        column.name.clone(),
                        None,
                        None,
                        pg_type_for_column_type(&column.ty),
                        FieldFormat::Text,
                    )
                }));
            }
            SelectItem::UnnamedExpr(expr) => {
                if let Some(field) = field_for_expr(expr, schema, None)? {
                    fields.push(field);
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Some(field) = field_for_expr(expr, schema, Some(alias.value.clone()))? {
                    fields.push(field);
                }
            }
            SelectItem::ExprWithAliases { .. } => return Ok(None),
        }
    }

    Ok((!fields.is_empty()).then_some(fields))
}

fn field_for_expr(
    expr: &SqlExpr,
    schema: &crate::query::TableSchema,
    alias: Option<String>,
) -> PgWireResult<Option<FieldInfo>> {
    let Some((name, ty)) = expr_name_and_type(expr, schema)? else {
        return Ok(None);
    };

    Ok(Some(FieldInfo::new(
        alias.unwrap_or(name),
        None,
        None,
        ty,
        FieldFormat::Text,
    )))
}

fn expr_name_and_type(
    expr: &SqlExpr,
    schema: &crate::query::TableSchema,
) -> PgWireResult<Option<(String, Type)>> {
    match expr {
        SqlExpr::Identifier(identifier) => {
            let column = schema
                .columns
                .iter()
                .find(|column| column.name == identifier.value)
                .ok_or_else(|| {
                    pg_error(
                        "ERROR",
                        "42703",
                        format!("missing column {}", identifier.value),
                    )
                })?;
            Ok(Some((
                column.name.clone(),
                pg_type_for_column_type(&column.ty),
            )))
        }
        SqlExpr::CompoundIdentifier(parts) => {
            let Some(identifier) = parts.last() else {
                return Ok(None);
            };
            expr_name_and_type(&SqlExpr::Identifier(identifier.clone()), schema)
        }
        SqlExpr::Function(function) => {
            let name = function.name.to_string();
            let ty = if name.eq_ignore_ascii_case("count") {
                Type::INT8
            } else if name.eq_ignore_ascii_case("avg") {
                Type::FLOAT8
            } else {
                Type::TEXT
            };
            Ok(Some((name, ty)))
        }
        _ => Ok(None),
    }
}

fn pg_type_for_column_type(ty: &ColumnType) -> Type {
    match ty {
        ColumnType::Int => Type::INT8,
        ColumnType::Float => Type::FLOAT8,
        ColumnType::Str | ColumnType::Null => Type::TEXT,
        ColumnType::Bool => Type::BOOL,
        ColumnType::Bytes => Type::BYTEA,
    }
}

fn object_name_to_string(name: &ObjectName) -> PgWireResult<String> {
    let [part] = name.0.as_slice() else {
        return Err(pg_error("ERROR", "0A000", name.to_string()));
    };

    match part {
        ObjectNamePart::Identifier(identifier) => Ok(identifier.value.clone()),
        ObjectNamePart::Function(_) => Err(pg_error("ERROR", "0A000", name.to_string())),
    }
}

fn substitute_portal_params(portal: &Portal<String>) -> PgWireResult<String> {
    let literals = (0..portal.parameter_len())
        .map(|index| portal_param_literal(portal, index))
        .collect::<PgWireResult<Vec<_>>>()?;

    substitute_params(&portal.statement.statement, &literals)
}

fn describe_sql_for_statement(statement: &StoredStatement<String>) -> PgWireResult<Option<String>> {
    if !contains_dollar_param(&statement.statement) {
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
    let mut output = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\'' {
            output.push(ch);
            copy_quoted_string(&mut chars, &mut output);
        } else if ch == '$' && chars.peek().is_some_and(char::is_ascii_digit) {
            let mut raw_index = String::new();
            while let Some(next) = chars.peek() {
                if next.is_ascii_digit() {
                    raw_index.push(*next);
                    chars.next();
                } else {
                    break;
                }
            }

            let index = raw_index
                .parse::<usize>()
                .map_err(|error| pg_error("ERROR", "22023", error.to_string()))?;
            let literal = literals
                .get(index.saturating_sub(1))
                .ok_or_else(|| PgWireError::ParameterIndexOutOfBound(index))?;
            output.push_str(literal);
        } else {
            output.push(ch);
        }
    }

    Ok(output)
}

fn copy_quoted_string(chars: &mut std::iter::Peekable<std::str::Chars<'_>>, output: &mut String) {
    while let Some(ch) = chars.next() {
        output.push(ch);
        if ch == '\'' {
            if chars.peek() == Some(&'\'') {
                output.push('\'');
                chars.next();
            } else {
                break;
            }
        }
    }
}

fn escape_sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn starts_with_keyword(sql: &str, keyword: &str) -> bool {
    sql.trim_start()
        .get(..keyword.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
}

fn is_select_like(sql: &str) -> bool {
    starts_with_keyword(sql, "select") || starts_with_keyword(sql, "with")
}

fn contains_dollar_param(sql: &str) -> bool {
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\'' {
            let mut sink = String::new();
            copy_quoted_string(&mut chars, &mut sink);
        } else if ch == '$' && chars.peek().is_some_and(char::is_ascii_digit) {
            return true;
        }
    }

    false
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
    use std::{io, sync::Arc};

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
    use tokio::{net::TcpListener, sync::oneshot};
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
    }

    #[test]
    fn database_errors_map_to_sqlstate() {
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
    fn network_error_keeps_runtime_failures_separate() {
        let error = NetworkError::Runtime("pool closed".to_owned());
        assert!(error.to_string().contains("runtime"));
    }

    #[test]
    fn auth_rate_limiter_rejects_repeated_attempts() -> Result<(), Box<dyn std::error::Error>> {
        let limiter = super::AuthRateLimiter::new(2, 60_000);
        let ip = "127.0.0.1".parse()?;

        assert!(limiter.record_attempt(ip, "alice").is_ok());
        assert!(limiter.record_attempt(ip, "alice").is_ok());
        assert!(matches!(
            limiter.record_attempt(ip, "alice"),
            Err(NetworkError::Auth(_))
        ));
        assert!(limiter.record_attempt(ip, "bob").is_ok());
        Ok(())
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
}
