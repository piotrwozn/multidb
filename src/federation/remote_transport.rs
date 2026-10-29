//! Verified TLS and a plaintext protocol boundary before tokio-postgres's codec.
use super::{Guard, Reservation, await_guarded, check, reserve};
use crate::{federation::FederationError, query::QueryError};
use futures::TryStreamExt;
use rustls_pki_types::{CertificateDer, pem::PemObject};
use socket2::{SockRef, TcpKeepalive};
use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, ready},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    task::JoinHandle,
};
use tokio_postgres::{
    Client, Config, NoTls, SimpleQueryMessage,
    config::{ChannelBinding, Host, LoadBalanceHosts, SslMode, SslNegotiation, TargetSessionAttrs},
    tls::{MakeTlsConnect, TlsConnect, TlsStream},
};
use tokio_postgres_rustls::MakeRustlsConnect;
use tokio_rustls::rustls;

const CA_PATH: &str = "MULTIDB_FOREIGN_PG_CA_CERT_PATH";
const MAX_FRAME: usize = 8 * 1024 * 1024;
// One frame per read prevents codec coalescing. This covers its in-progress
// buffer, bounded response channel, pending response, and consumer-held row,
// including decoded field-index/string expansion for many tiny columns.
const WIRE_EXPANSION: usize = 64;
const CONTROL_LIMIT: usize = 1024 * 1024;
const AUTH_SCRATCH: usize = 8192;
const MAX_SCRAM_ITERATIONS: u32 = 1_000_000;

fn unsupported(message: &'static str) -> FederationError {
    FederationError::Unsupported(message.to_owned())
}
fn remote_error() -> FederationError {
    // Server messages and connection-string parse errors are untrusted and may
    // echo a password or SQL literal. Never expose them through API errors/logs.
    FederationError::Postgres("remote PostgreSQL operation failed".to_owned())
}

struct Wire {
    guard: Guard,
    max_frame: usize,
    control: Mutex<usize>,
    failure: Mutex<Option<QueryError>>,
    _reservation: Mutex<Reservation>,
    _tls_reservation: Mutex<Reservation>,
    #[cfg(test)]
    frames: std::sync::atomic::AtomicUsize,
}
impl Wire {
    fn new(guard: Guard, tls_reservation: Reservation) -> Result<Arc<Self>, FederationError> {
        check(&guard)?;
        let available = guard
            .as_ref()
            .map_or(MAX_FRAME * WIRE_EXPANSION * 4, |guard| {
                guard.remaining_bytes()
            });
        let max_frame = (available / (4 * WIRE_EXPANSION)).min(MAX_FRAME);
        if max_frame < 8 {
            return Err(QueryError::ResourceLimit(
                "insufficient PostgreSQL wire budget".to_owned(),
            )
            .into());
        }
        let reservation = reserve(&guard, max_frame * WIRE_EXPANSION + 2 * AUTH_SCRATCH)?;
        Ok(Arc::new(Self {
            guard,
            max_frame,
            control: Mutex::new(0),
            failure: Mutex::new(None),
            _reservation: Mutex::new(reservation),
            _tls_reservation: Mutex::new(tls_reservation),
            #[cfg(test)]
            frames: std::sync::atomic::AtomicUsize::new(0),
        }))
    }
    fn fail(&self, error: QueryError) -> io::Error {
        let mut failure = self
            .failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if failure.is_none() {
            *failure = Some(error);
        }
        io::Error::other("foreign PostgreSQL read budget exceeded")
    }
    fn check(&self) -> io::Result<()> {
        if let Some(guard) = &self.guard {
            guard.check().map_err(|error| self.fail(error))?;
        }
        Ok(())
    }
    fn header(&self, header: [u8; 5]) -> io::Result<usize> {
        self.check()?;
        #[cfg(test)]
        self.frames
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let length = i32::from_be_bytes([header[1], header[2], header[3], header[4]]);
        let length = usize::try_from(length)
            .ok()
            .filter(|length| *length >= 4)
            .ok_or_else(|| {
                self.fail(QueryError::InvalidValue(
                    "invalid PostgreSQL frame length".to_owned(),
                ))
            })?;
        let total = length
            .checked_add(1)
            .filter(|length| *length <= self.max_frame)
            .ok_or_else(|| {
                self.fail(QueryError::ResourceLimit(
                    "PostgreSQL frame exceeds the statement wire budget".to_owned(),
                ))
            })?;
        if header[0] != b'D' {
            let mut control = self
                .control
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *control = control
                .checked_add(total)
                .filter(|bytes| *bytes <= CONTROL_LIMIT)
                .ok_or_else(|| {
                    self.fail(QueryError::ResourceLimit(
                        "PostgreSQL startup/control message budget exceeded".to_owned(),
                    ))
                })?;
            if let Some(guard) = &self.guard {
                guard
                    .charge(total.saturating_mul(8).saturating_add(256))
                    .map_err(|error| self.fail(error))?;
            }
        }
        Ok(length - 4)
    }
    fn error(&self) -> FederationError {
        self.failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .map_or_else(remote_error, FederationError::Query)
    }
}

struct FrameGuard<S> {
    stream: S,
    wire: Arc<Wire>,
    header: [u8; 5],
    filled: usize,
    exposed: usize,
    remaining: usize,
    validated: bool,
    auth: [u8; AUTH_SCRATCH],
    auth_filled: usize,
    auth_exposed: usize,
    auth_ready: bool,
    body: Vec<u8>,
    body_filled: usize,
    body_exposed: usize,
    body_ready: bool,
}
impl<S> FrameGuard<S> {
    fn new(stream: S, wire: Arc<Wire>) -> Self {
        Self {
            stream,
            wire,
            header: [0; 5],
            filled: 0,
            exposed: 0,
            remaining: 0,
            validated: false,
            auth: [0; AUTH_SCRATCH],
            auth_filled: 0,
            auth_exposed: 0,
            auth_ready: false,
            body: Vec::new(),
            body_filled: 0,
            body_exposed: 0,
            body_ready: false,
        }
    }
    fn finish(&mut self) {
        self.filled = 0;
        self.exposed = 0;
        self.validated = false;
        self.auth_filled = 0;
        self.auth_exposed = 0;
        self.auth_ready = false;
        self.body.clear();
        self.body_filled = 0;
        self.body_exposed = 0;
        self.body_ready = false;
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for FrameGuard<S> {
    #[allow(clippy::too_many_lines)] // One protocol state machine; every exposure follows preflight.
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let originally_filled = output.filled().len();
        self.wire.check()?;
        let this = &mut *self;
        if !this.validated {
            while this.filled < 5 {
                let mut input = ReadBuf::new(&mut this.header[this.filled..]);
                ready!(Pin::new(&mut this.stream).poll_read(cx, &mut input))?;
                let count = input.filled().len();
                if count == 0 {
                    return if this.filled == 0 {
                        Poll::Ready(Ok(()))
                    } else {
                        Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "truncated PostgreSQL frame header",
                        )))
                    };
                }
                this.filled += count;
            }
            this.remaining = this.wire.header(this.header)?;
            this.validated = true;
        }
        if matches!(this.header[0], b'D' | b'T' | b't') && !this.body_ready {
            if this.body.is_empty() && this.remaining != 0 {
                this.body.try_reserve_exact(this.remaining).map_err(|_| {
                    this.wire.fail(QueryError::ResourceLimit(
                        "PostgreSQL frame scratch allocation failed".to_owned(),
                    ))
                })?;
                this.body.resize(this.remaining, 0);
            }
            while this.body_filled < this.body.len() {
                let mut input = ReadBuf::new(&mut this.body[this.body_filled..]);
                ready!(Pin::new(&mut this.stream).poll_read(cx, &mut input))?;
                let count = input.filled().len();
                if count == 0 {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "truncated PostgreSQL row/metadata frame",
                    )));
                }
                this.body_filled += count;
            }
            validate_fields(this.header[0], &this.body).map_err(|error| this.wire.fail(error))?;
            this.body_ready = true;
        }
        if this.header[0] == b'R' && !this.auth_ready {
            if this.remaining < 4 {
                return Poll::Ready(Err(this.wire.fail(QueryError::InvalidValue(
                    "truncated PostgreSQL authentication frame".to_owned(),
                ))));
            }
            loop {
                let sasl = this.auth_filled >= 4 && this.auth[..4] == 11i32.to_be_bytes();
                let target = if sasl { this.remaining } else { 4 };
                if target > AUTH_SCRATCH {
                    return Poll::Ready(Err(this.wire.fail(QueryError::ResourceLimit(
                        "PostgreSQL SCRAM challenge exceeds authentication budget".to_owned(),
                    ))));
                }
                if this.auth_filled < target {
                    let mut input = ReadBuf::new(&mut this.auth[this.auth_filled..target]);
                    ready!(Pin::new(&mut this.stream).poll_read(cx, &mut input))?;
                    let count = input.filled().len();
                    if count == 0 {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "truncated PostgreSQL authentication challenge",
                        )));
                    }
                    this.auth_filled += count;
                    continue;
                }
                if sasl {
                    validate_scram_iterations(&this.auth[4..target])
                        .map_err(|error| this.wire.fail(error))?;
                }
                this.auth_ready = true;
                break;
            }
        }
        if this.exposed < 5 {
            let count = output.remaining().min(5 - this.exposed);
            output.put_slice(&this.header[this.exposed..this.exposed + count]);
            this.exposed += count;
            if this.exposed < 5 || output.remaining() == 0 {
                if this.exposed == 5 && this.remaining == 0 {
                    this.finish();
                }
                return Poll::Ready(Ok(()));
            }
        }
        if this.auth_exposed < this.auth_filled {
            let count = output.remaining().min(this.auth_filled - this.auth_exposed);
            output.put_slice(&this.auth[this.auth_exposed..this.auth_exposed + count]);
            this.auth_exposed += count;
            this.remaining -= count;
            if output.remaining() == 0 {
                if this.remaining == 0 {
                    this.finish();
                }
                return Poll::Ready(Ok(()));
            }
        }
        if this.body_exposed < this.body_filled {
            let count = output.remaining().min(this.body_filled - this.body_exposed);
            output.put_slice(&this.body[this.body_exposed..this.body_exposed + count]);
            this.body_exposed += count;
            this.remaining -= count;
            if this.remaining == 0 {
                this.finish();
            }
            return Poll::Ready(Ok(()));
        }
        if this.remaining != 0 {
            let maximum = this.remaining.min(output.remaining());
            let mut input = ReadBuf::new(output.initialize_unfilled_to(maximum));
            // Header bytes have already been exposed. Returning Pending would
            // lose their progress to AsyncRead callers; let the codec retry.
            match Pin::new(&mut this.stream).poll_read(cx, &mut input) {
                Poll::Pending => {
                    return if output.filled().len() > originally_filled {
                        Poll::Ready(Ok(()))
                    } else {
                        Poll::Pending
                    };
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {
                    let count = input.filled().len();
                    if count == 0 {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "truncated PostgreSQL frame payload",
                        )));
                    }
                    output.advance(count);
                    this.remaining -= count;
                }
            }
        }
        if this.remaining == 0 {
            this.finish();
        }
        Poll::Ready(Ok(()))
    }
}

fn validate_fields(tag: u8, body: &[u8]) -> Result<(), QueryError> {
    let invalid =
        || QueryError::InvalidValue("malformed PostgreSQL row/metadata fields".to_owned());
    let count = body
        .get(..2)
        .map(|bytes| usize::from(u16::from_be_bytes([bytes[0], bytes[1]])))
        .ok_or_else(invalid)?;
    let minimum = match tag {
        b'D' | b't' => 4,
        b'T' => 19,
        _ => return Err(invalid()),
    };
    if count > body.len().saturating_sub(2) / minimum {
        return Err(invalid());
    }
    let mut position = 2usize;
    for _ in 0..count {
        match tag {
            b'D' => {
                let bytes = body
                    .get(position..position.saturating_add(4))
                    .ok_or_else(invalid)?;
                let length = i32::from_be_bytes(bytes.try_into().map_err(|_| invalid())?);
                position += 4;
                if length < -1 {
                    return Err(invalid());
                }
                if length >= 0 {
                    position = position
                        .checked_add(usize::try_from(length).map_err(|_| invalid())?)
                        .filter(|end| *end <= body.len())
                        .ok_or_else(invalid)?;
                }
            }
            b'T' => {
                let end = body
                    .get(position..)
                    .and_then(|tail| tail.iter().position(|byte| *byte == 0))
                    .ok_or_else(invalid)?;
                position = position.checked_add(end + 1).ok_or_else(invalid)?;
                let fields = body
                    .get(position..position.saturating_add(18))
                    .ok_or_else(invalid)?;
                validate_oid(u32::from_be_bytes(
                    fields[6..10].try_into().map_err(|_| invalid())?,
                ))?;
                if !matches!(i16::from_be_bytes([fields[16], fields[17]]), 0 | 1) {
                    return Err(invalid());
                }
                position += 18;
            }
            b't' => {
                let bytes = body
                    .get(position..position.saturating_add(4))
                    .ok_or_else(invalid)?;
                validate_oid(u32::from_be_bytes(bytes.try_into().map_err(|_| invalid())?))?;
                position += 4;
            }
            _ => return Err(invalid()),
        }
    }
    if position != body.len() {
        return Err(invalid());
    }
    Ok(())
}

fn validate_oid(oid: u32) -> Result<(), QueryError> {
    // Exactly the built-in scalar families supported by TableSchema and the
    // session check. Unknown/custom OIDs must never launch unbounded pg_type
    // enum/composite discovery inside the driver.
    if matches!(
        oid,
        16 | 17 | 19 | 20 | 21 | 23 | 25 | 700 | 701 | 705 | 1042 | 1043
    ) {
        Ok(())
    } else {
        Err(QueryError::Unsupported(
            "foreign PostgreSQL requires supported built-in scalar types".to_owned(),
        ))
    }
}

fn validate_scram_iterations(challenge: &[u8]) -> Result<(), QueryError> {
    let invalid =
        || QueryError::InvalidValue("invalid PostgreSQL SCRAM iteration count".to_owned());
    let text = std::str::from_utf8(challenge).map_err(|_| invalid())?;
    let mut iterations = None;
    for field in text.split(',') {
        if let Some(value) = field.strip_prefix("i=") {
            if iterations.is_some()
                || value.is_empty()
                || !value.bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(invalid());
            }
            let count = value.parse::<u32>().map_err(|_| {
                QueryError::ResourceLimit("PostgreSQL SCRAM iteration budget exceeded".to_owned())
            })?;
            if count > MAX_SCRAM_ITERATIONS {
                return Err(QueryError::ResourceLimit(
                    "PostgreSQL SCRAM iteration budget exceeded".to_owned(),
                ));
            }
            if count == 0 {
                return Err(invalid());
            }
            iterations = Some(count);
        }
    }
    iterations.ok_or_else(invalid)?;
    Ok(())
}
impl<S: AsyncWrite + Unpin> AsyncWrite for FrameGuard<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.wire.check()?;
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.wire.check()?;
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
impl<S: TlsStream + Unpin> TlsStream for FrameGuard<S> {
    fn channel_binding(&self) -> tokio_postgres::tls::ChannelBinding {
        self.stream.channel_binding()
    }
}

enum Socket {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
}
impl AsyncRead for Socket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}
impl AsyncWrite for Socket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            Self::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Tcp(s) => Pin::new(s).poll_flush(cx),
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

type RustlsConnector = <MakeRustlsConnect as MakeTlsConnect<Socket>>::TlsConnect;
type RustlsSocket = <MakeRustlsConnect as MakeTlsConnect<Socket>>::Stream;
struct TlsConnector {
    inner: RustlsConnector,
    wire: Arc<Wire>,
}
impl TlsConnect<Socket> for TlsConnector {
    type Stream = FrameGuard<RustlsSocket>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<Self::Stream>> + Send>>;
    fn connect(self, stream: Socket) -> Self::Future {
        Box::pin(async move {
            let stream = self.inner.connect(stream).await?;
            Ok(FrameGuard::new(stream, self.wire))
        })
    }
}

pub(super) struct RemoteConnection {
    client: Client,
    task: JoinHandle<()>,
    wire: Arc<Wire>,
}
impl Drop for RemoteConnection {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl RemoteConnection {
    pub(super) fn client(&self) -> &Client {
        &self.client
    }
    pub(super) fn map_error(&self, _: tokio_postgres::Error) -> FederationError {
        self.wire.error()
    }
}

pub(super) async fn connect(
    secret: &str,
    guard: Guard,
) -> Result<RemoteConnection, FederationError> {
    if secret.len() > 65536 {
        return Err(unsupported(
            "foreign PostgreSQL connection configuration exceeds 64 KiB",
        ));
    }
    let config: Config = secret.parse().map_err(|_| remote_error())?;
    let ca_path = std::env::var_os(CA_PATH);
    let tcp = !config.get_hostaddrs().is_empty()
        || config
            .get_hosts()
            .iter()
            .any(|host| matches!(host, Host::Tcp(_)));
    let (tls, tls_reservation) =
        if let Some(path) = ca_path.filter(|_| tcp && config.get_ssl_mode() != SslMode::Disable) {
            let (config, reservation) = load_tls(std::path::Path::new(&path), &guard).await?;
            (Some(config), reservation)
        } else {
            (None, None)
        };
    Box::pin(connect_config(config, tls, tls_reservation, guard)).await
}

async fn load_tls(
    path: &std::path::Path,
    guard: &Guard,
) -> Result<(rustls::ClientConfig, Reservation), FederationError> {
    let path = path.to_owned();
    let budget = reserve(guard, 20 * 1024 * 1024)?;
    let (pem, budget) = await_guarded(
        tokio::task::spawn_blocking(move || {
            use std::io::Read;
            let mut file = std::fs::File::open(path)?;
            let mut bytes = Vec::new();
            file.by_ref()
                .take(1024 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() > 1024 * 1024 {
                return Err(io::Error::other(
                    "foreign PostgreSQL CA bundle is too large",
                ));
            }
            Ok::<_, io::Error>((bytes, budget))
        }),
        guard,
    )
    .await?
    .map_err(|_| remote_error())?
    .map_err(|_| unsupported("cannot read foreign PostgreSQL CA bundle"))?;
    let mut roots = rustls::RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(&pem) {
        roots
            .add(
                certificate
                    .map_err(|_| unsupported("invalid foreign PostgreSQL CA certificate"))?,
            )
            .map_err(|_| unsupported("invalid foreign PostgreSQL CA certificate"))?;
    }
    if roots.is_empty() {
        return Err(unsupported("foreign PostgreSQL CA bundle is empty"));
    }
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| unsupported("TLS protocol configuration unavailable"))?
    .with_root_certificates(roots)
    .with_no_client_auth();
    config.alpn_protocols = vec![b"postgresql".to_vec()];
    config.resumption = rustls::client::Resumption::disabled();
    Ok((config, budget))
}

#[allow(clippy::too_many_lines)] // Keep endpoint validation and failover ordering together.
async fn connect_config(
    config: Config,
    tls: Option<rustls::ClientConfig>,
    tls_reservation: Reservation,
    guard: Guard,
) -> Result<RemoteConnection, FederationError> {
    check(&guard)?;
    if !matches!(
        config.get_ssl_mode(),
        SslMode::Disable | SslMode::Prefer | SslMode::Require
    ) || !matches!(
        config.get_channel_binding(),
        ChannelBinding::Disable | ChannelBinding::Prefer | ChannelBinding::Require
    ) || !matches!(
        config.get_target_session_attrs(),
        TargetSessionAttrs::Any | TargetSessionAttrs::ReadOnly | TargetSessionAttrs::ReadWrite
    ) || !matches!(
        config.get_ssl_negotiation(),
        SslNegotiation::Postgres | SslNegotiation::Direct
    ) {
        return Err(unsupported("unsupported PostgreSQL connection mode"));
    }
    if config.get_ssl_mode() == SslMode::Require && tls.is_none() {
        return Err(unsupported(
            "verified PostgreSQL TLS requires MULTIDB_FOREIGN_PG_CA_CERT_PATH",
        ));
    }
    if config.get_channel_binding() == ChannelBinding::Require
        && (tls.is_none() || config.get_ssl_mode() == SslMode::Disable)
    {
        return Err(unsupported(
            "PostgreSQL channel_binding=require requires verified TLS",
        ));
    }
    let count = config.get_hosts().len().max(config.get_hostaddrs().len());
    if count == 0
        || (!config.get_hosts().is_empty()
            && !config.get_hostaddrs().is_empty()
            && config.get_hosts().len() != config.get_hostaddrs().len())
        || (config.get_ports().len() > 1 && config.get_ports().len() != count)
    {
        return Err(unsupported(
            "invalid foreign PostgreSQL host/hostaddr/port configuration",
        ));
    }
    let mut order: Vec<_> = (0..count).collect();
    randomize(&mut order, config.get_load_balance_hosts())?;
    let wire = Wire::new(guard.clone(), tls_reservation)?;
    for index in order {
        check(&guard)?;
        let port = config
            .get_ports()
            .get(index)
            .or_else(|| config.get_ports().first())
            .copied()
            .unwrap_or(5432);
        let host = config.get_hosts().get(index);
        let address = config.get_hostaddrs().get(index).copied();
        #[cfg(unix)]
        if address.is_none()
            && let Some(Host::Unix(path)) = host
        {
            if config.get_ssl_mode() == SslMode::Require
                || config.get_channel_binding() == ChannelBinding::Require
            {
                return Err(unsupported(
                    "TLS/channel binding require cannot be used on a Unix PostgreSQL socket",
                ));
            }
            let path = path.join(format!(".s.PGSQL.{port}"));
            if let Ok(socket) = dial(
                tokio::net::UnixStream::connect(path),
                config.get_connect_timeout().copied(),
                &guard,
            )
            .await?
            {
                match Box::pin(start(
                    Socket::Unix(socket),
                    &config,
                    None,
                    None,
                    Arc::clone(&wire),
                ))
                .await
                {
                    Ok(connection) => {
                        if acceptable(&connection, config.get_target_session_attrs(), &guard)
                            .await?
                        {
                            return Ok(connection);
                        }
                    }
                    Err(FederationError::Query(error)) => return Err(error.into()),
                    Err(FederationError::Unsupported(message)) => {
                        return Err(FederationError::Unsupported(message));
                    }
                    Err(_) => {}
                }
            }
            continue;
        }
        let hostname = match host {
            Some(Host::Tcp(host)) => host.clone(),
            _ => address.map(|ip| ip.to_string()).ok_or_else(remote_error)?,
        };
        let mut addresses: Vec<SocketAddr> = if let Some(address) = address {
            vec![SocketAddr::new(address, port)]
        } else {
            match await_guarded(tokio::net::lookup_host((hostname.as_str(), port)), &guard).await? {
                Ok(addresses) => addresses.collect(),
                Err(_) => continue,
            }
        };
        randomize(&mut addresses, config.get_load_balance_hosts())?;
        for address in addresses {
            let Ok(socket) = dial(
                TcpStream::connect(address),
                config.get_connect_timeout().copied(),
                &guard,
            )
            .await?
            else {
                continue;
            };
            configure_socket(&socket, &config).map_err(|_| remote_error())?;
            match Box::pin(start(
                Socket::Tcp(socket),
                &config,
                Some(&hostname),
                tls.clone(),
                Arc::clone(&wire),
            ))
            .await
            {
                Ok(connection) => {
                    if acceptable(&connection, config.get_target_session_attrs(), &guard).await? {
                        return Ok(connection);
                    }
                }
                Err(FederationError::Query(error)) => return Err(error.into()),
                Err(FederationError::Unsupported(message)) => {
                    return Err(FederationError::Unsupported(message));
                }
                Err(_) => {}
            }
        }
    }
    Err(wire.error())
}

fn randomize<T>(items: &mut [T], mode: LoadBalanceHosts) -> Result<(), FederationError> {
    match mode {
        LoadBalanceHosts::Disable => Ok(()),
        LoadBalanceHosts::Random => {
            let mut seed = [0; 32];
            getrandom::fill(&mut seed).map_err(|_| remote_error())?;
            let mut counter = 0u64;
            items.sort_by_cached_key(|_| {
                counter += 1;
                blake3::keyed_hash(&seed, &counter.to_le_bytes())
                    .as_bytes()
                    .to_owned()
            });
            Ok(())
        }
        _ => Err(unsupported(
            "unsupported PostgreSQL host load-balancing mode",
        )),
    }
}

async fn dial<T>(
    future: impl Future<Output = io::Result<T>>,
    timeout: Option<Duration>,
    guard: &Guard,
) -> Result<io::Result<T>, FederationError> {
    await_guarded(
        async {
            match timeout {
                Some(timeout) => tokio::time::timeout(timeout, future)
                    .await
                    .unwrap_or_else(|_| {
                        Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "connection timeout",
                        ))
                    }),
                None => future.await,
            }
        },
        guard,
    )
    .await
}

fn configure_socket(socket: &TcpStream, config: &Config) -> io::Result<()> {
    socket.set_nodelay(true)?;
    let socket = SockRef::from(socket);
    #[cfg(target_os = "linux")]
    if let Some(timeout) = config.get_tcp_user_timeout() {
        socket.set_tcp_user_timeout(Some(*timeout))?;
    }
    if config.get_keepalives() {
        let mut keepalive = TcpKeepalive::new().with_time(config.get_keepalives_idle());
        #[cfg(not(any(
            target_os = "aix",
            target_os = "redox",
            target_os = "solaris",
            target_os = "openbsd"
        )))]
        if let Some(interval) = config.get_keepalives_interval() {
            keepalive = keepalive.with_interval(interval);
        }
        #[cfg(not(any(
            target_os = "aix",
            target_os = "redox",
            target_os = "solaris",
            target_os = "windows",
            target_os = "openbsd"
        )))]
        if let Some(retries) = config.get_keepalives_retries() {
            keepalive = keepalive.with_retries(retries);
        }
        socket.set_tcp_keepalive(&keepalive)?;
    }
    Ok(())
}

async fn start(
    socket: Socket,
    config: &Config,
    hostname: Option<&str>,
    tls: Option<rustls::ClientConfig>,
    wire: Arc<Wire>,
) -> Result<RemoteConnection, FederationError> {
    let secure = hostname.is_some() && config.get_ssl_mode() != SslMode::Disable && tls.is_some();
    if secure {
        let mut config = config.clone();
        config.ssl_mode(SslMode::Require);
        let mut factory = MakeRustlsConnect::new(tls.ok_or_else(remote_error)?);
        let connector = <MakeRustlsConnect as MakeTlsConnect<Socket>>::make_tls_connect(
            &mut factory,
            hostname.ok_or_else(remote_error)?,
        )
        .map_err(|_| remote_error())?;
        let tls = TlsConnector {
            inner: connector,
            wire: Arc::clone(&wire),
        };
        let (client, connection) = await_guarded(
            Box::pin(async {
                Box::pin(config.connect_raw(socket, tls))
                    .await
                    .map(|(client, connection)| (client, Box::new(connection)))
            }),
            &wire.guard,
        )
        .await?
        .map_err(|_| wire.error())?;
        let task = drive(connection);
        Ok(RemoteConnection { client, task, wire })
    } else {
        if config.get_ssl_mode() == SslMode::Require {
            return Err(unsupported(
                "verified PostgreSQL TLS requires MULTIDB_FOREIGN_PG_CA_CERT_PATH",
            ));
        }
        if config.get_channel_binding() == ChannelBinding::Require {
            return Err(unsupported(
                "PostgreSQL channel_binding=require requires verified TLS",
            ));
        }
        if config.get_ssl_negotiation() != SslNegotiation::Postgres {
            return Err(unsupported(
                "direct PostgreSQL TLS negotiation requires verified TLS",
            ));
        }
        let mut config = config.clone();
        config.ssl_mode(SslMode::Disable);
        let stream = FrameGuard::new(socket, Arc::clone(&wire));
        let (client, connection) = await_guarded(
            Box::pin(async {
                Box::pin(config.connect_raw(stream, NoTls))
                    .await
                    .map(|(client, connection)| (client, Box::new(connection)))
            }),
            &wire.guard,
        )
        .await?
        .map_err(|_| wire.error())?;
        let task = drive(connection);
        Ok(RemoteConnection { client, task, wire })
    }
}

fn drive<S, T>(mut connection: Box<tokio_postgres::Connection<S, T>>) -> JoinHandle<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        // Connection::Future logs raw Notice text. Poll messages directly and
        // discard peer-controlled asynchronous contents without logging them.
        while let Some(message) = std::future::poll_fn(|cx| connection.poll_message(cx)).await {
            if message.is_err() {
                break;
            }
        }
    })
}

async fn acceptable(
    connection: &RemoteConnection,
    target: TargetSessionAttrs,
    guard: &Guard,
) -> Result<bool, FederationError> {
    if target == TargetSessionAttrs::Any {
        return Ok(true);
    }
    let rows = await_guarded(
        connection
            .client
            .simple_query_raw("SHOW transaction_read_only"),
        guard,
    )
    .await?
    .map_err(|error| connection.map_error(error))?;
    let mut rows = Box::pin(rows);
    let mut result = None;
    let mut messages = 0usize;
    while let Some(row) = await_guarded(rows.try_next(), guard)
        .await?
        .map_err(|error| connection.map_error(error))?
    {
        messages += 1;
        if messages > 4 {
            return Err(QueryError::ResourceLimit(
                "PostgreSQL session-check response exceeds its fixed message budget".to_owned(),
            )
            .into());
        }
        if let SimpleQueryMessage::Row(row) = row {
            if result.is_some() {
                return Err(QueryError::ResourceLimit(
                    "PostgreSQL session check returned multiple rows".to_owned(),
                )
                .into());
            }
            if row.len() != 1 {
                return Err(remote_error());
            }
            result = Some(match (target, row.get(0)) {
                (TargetSessionAttrs::ReadOnly, Some("on"))
                | (TargetSessionAttrs::ReadWrite, Some("off")) => true,
                (TargetSessionAttrs::ReadOnly | TargetSessionAttrs::ReadWrite, _) => false,
                _ => return Err(unsupported("unsupported PostgreSQL target_session_attrs")),
            });
        }
    }
    result.ok_or_else(remote_error)
}

#[cfg(test)]
#[path = "remote_transport_tests.rs"]
mod tests;
