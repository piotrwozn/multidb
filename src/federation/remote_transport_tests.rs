use super::*;
use crate::federation::{ForeignScanGuard, ForeignScanReservation};
use futures::TryStreamExt;
use rustls_pki_types::PrivatePkcs8KeyDer;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
type TestResult = Result<(), Box<dyn std::error::Error>>;

struct Budget {
    used: Arc<Mutex<usize>>,
    limit: usize,
    cancelled: AtomicBool,
}
struct Token {
    used: Arc<Mutex<usize>>,
    bytes: usize,
}
impl Drop for Token {
    fn drop(&mut self) {
        *self
            .used
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) -= self.bytes;
    }
}
impl ForeignScanGuard for Budget {
    fn check(&self) -> Result<(), QueryError> {
        if self.cancelled.load(Ordering::Acquire) {
            Err(QueryError::Cancelled("test cancellation".to_owned()))
        } else {
            Ok(())
        }
    }
    fn charge(&self, bytes: usize) -> Result<(), QueryError> {
        self.check()?;
        let mut used = self
            .used
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *used = used
            .checked_add(bytes)
            .filter(|n| *n <= self.limit)
            .ok_or_else(|| QueryError::ResourceLimit("test wire budget".to_owned()))?;
        Ok(())
    }
    fn remaining_bytes(&self) -> usize {
        self.limit.saturating_sub(
            *self
                .used
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
    fn reserve(&self, bytes: usize) -> Result<Box<dyn ForeignScanReservation>, QueryError> {
        self.charge(bytes)?;
        Ok(Box::new(Token {
            used: Arc::clone(&self.used),
            bytes,
        }))
    }
}
fn budget() -> Arc<Budget> {
    Arc::new(Budget {
        used: Arc::new(Mutex::new(0)),
        limit: 128 * 1024 * 1024,
        cancelled: AtomicBool::new(false),
    })
}
fn frame(tag: u8, body: &[u8]) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut bytes = vec![tag];
    bytes.extend_from_slice(&u32::try_from(body.len() + 4)?.to_be_bytes());
    bytes.extend_from_slice(body);
    Ok(bytes)
}

#[tokio::test]
async fn oversized_and_negative_headers_fail_before_exposing_or_reading_payload() -> TestResult {
    for length in [i32::MAX, -1, 3] {
        let (mut server, socket) = tokio::io::duplex(16);
        let wire = Wire::new(Some(budget()), None)?;
        let mut guarded = FrameGuard::new(socket, Arc::clone(&wire));
        let mut header = vec![b'D'];
        header.extend_from_slice(&length.to_be_bytes());
        server.write_all(&header).await?;
        let mut result = [0; 10];
        let error = guarded
            .read(&mut result)
            .await
            .err()
            .ok_or("accepted invalid header")?;
        assert!(error.to_string().contains("budget exceeded"));
        assert_eq!(result, [0; 10]);
        assert!(matches!(
            wire.error(),
            FederationError::Query(QueryError::ResourceLimit(_) | QueryError::InvalidValue(_))
        ));
    }
    Ok(())
}

#[tokio::test]
async fn split_headers_zero_body_and_pending_payload_do_not_become_false_eof() -> TestResult {
    let (mut server, socket) = tokio::io::duplex(64);
    let mut guarded = FrameGuard::new(socket, Wire::new(Some(budget()), None)?);
    let zero = frame(b'1', &[])?;
    for byte in &zero {
        server.write_all(&[*byte]).await?;
    }
    let mut header = [0; 5];
    guarded.read_exact(&mut header).await?;
    assert_eq!(header.as_slice(), zero);
    let next = frame(b'd', b"body")?;
    server.write_all(&next[..5]).await?;
    guarded.read_exact(&mut header).await?;
    let mut body = [0; 4];
    assert!(
        tokio::time::timeout(Duration::from_millis(5), guarded.read_exact(&mut body))
            .await
            .is_err()
    );
    server.write_all(&next[5..]).await?;
    guarded.read_exact(&mut body).await?;
    assert_eq!(&body, b"body");
    Ok(())
}

#[tokio::test]
async fn scram_work_is_capped_before_the_challenge_reaches_the_codec() -> TestResult {
    let (mut server, socket) = tokio::io::duplex(128);
    let wire = Wire::new(Some(budget()), None)?;
    let mut guarded = FrameGuard::new(socket, Arc::clone(&wire));
    let mut challenge = 11i32.to_be_bytes().to_vec();
    challenge.extend_from_slice(b"r=nonce,s=c2FsdA==,i=2147483647");
    server.write_all(&frame(b'R', &challenge)?).await?;
    let mut output = [0; 64];
    assert!(guarded.read(&mut output).await.is_err());
    assert_eq!(output, [0; 64]);
    assert!(matches!(
        wire.error(),
        FederationError::Query(QueryError::ResourceLimit(_))
    ));
    validate_scram_iterations(b"r=nonce,s=c2FsdA==,i=600000")?;
    assert!(validate_scram_iterations(b"r=nonce,s=c2FsdA==,i=1,i=2").is_err());
    Ok(())
}

#[test]
fn cumulative_control_messages_cannot_grow_session_maps_without_bound() -> TestResult {
    let wire = Wire::new(None, None)?;
    let mut header = [b'S', 0, 0, 0, 0];
    header[1..].copy_from_slice(&1024i32.to_be_bytes());
    let mut accepted = 0;
    while wire.header(header).is_ok() {
        accepted += 1;
    }
    assert!(accepted <= CONTROL_LIMIT / 1024);
    assert!(matches!(
        wire.error(),
        FederationError::Query(QueryError::ResourceLimit(_))
    ));
    Ok(())
}

#[tokio::test]
async fn malformed_field_counts_and_unknown_oids_never_reach_the_codec() -> TestResult {
    let mut cases = vec![
        (b'D', vec![255, 255]),
        (b'T', vec![255, 255]),
        (b't', vec![255, 255]),
        (b'D', vec![0, 1, 0, 0, 0, 100]),
    ];
    let mut description = vec![0, 1, b'v', 0];
    description.extend_from_slice(&[0; 6]);
    description.extend_from_slice(&999_999_u32.to_be_bytes());
    description.extend_from_slice(&[255; 6]);
    description.extend_from_slice(&[0, 0]);
    cases.push((b'T', description));
    let mut parameters = vec![0, 1];
    parameters.extend_from_slice(&999_999_u32.to_be_bytes());
    cases.push((b't', parameters));
    for (tag, body) in cases {
        let (mut peer, socket) = tokio::io::duplex(128);
        peer.write_all(&frame(tag, &body)?).await?;
        let wire = Wire::new(Some(budget()), None)?;
        let mut guarded = FrameGuard::new(socket, wire);
        let mut output = [0; 64];
        assert!(guarded.read(&mut output).await.is_err());
        assert_eq!(output, [0; 64], "unvalidated {tag} bytes escaped");
    }
    Ok(())
}

#[tokio::test]
async fn target_session_check_rejects_multiple_rows_without_collecting_the_stream() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        serve(&mut stream, usize::MAX).await
    });
    let mut settings = config(port)?;
    settings.target_session_attrs(TargetSessionAttrs::ReadWrite);
    let error = connect_config(settings, None, None, Some(budget()))
        .await
        .err()
        .ok_or("accepted session-check row flood")?;
    assert!(
        matches!(error, FederationError::Query(QueryError::ResourceLimit(_))),
        "{error}"
    );
    peer.abort();
    Ok(())
}

struct CaptureLog(Mutex<Vec<u8>>);
impl log::Log for CaptureLog {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }
    fn log(&self, record: &log::Record<'_>) {
        use std::io::Write;
        let mut bytes = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = writeln!(bytes, "{}", record.args());
    }
    fn flush(&self) {}
}
static LOGS: CaptureLog = CaptureLog(Mutex::new(Vec::new()));

#[tokio::test]
async fn peer_notices_never_reach_the_dependency_logger() -> TestResult {
    const CHILD: &str = "MULTIDB_FOREIGN_NOTICE_LOG_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let executable = std::env::current_exe()?;
        let output = tokio::task::spawn_blocking(move || std::process::Command::new(executable)
            .args(["--exact", "federation::stream::remote_transport::tests::peer_notices_never_reach_the_dependency_logger", "--nocapture"])
            .env(CHILD, "1").output()).await??;
        assert!(
            output.status.success(),
            "isolated logger test failed: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("1 passed"),
            "isolated test filter matched no test"
        );
        return Ok(());
    }
    log::set_logger(&LOGS)?;
    log::set_max_level(log::LevelFilter::Trace);
    log::info!("positive logger control");
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        startup(&mut stream).await?;
        stream
            .write_all(&frame(b'R', &0i32.to_be_bytes()).map_err(io::Error::other)?)
            .await?;
        stream
            .write_all(&frame(b'Z', b"I").map_err(io::Error::other)?)
            .await?;
        stream
            .write_all(
                &frame(b'N', b"SNOTICE\0C00000\0MPRIVATE_PASSWORD_SENTINEL\0\0")
                    .map_err(io::Error::other)?,
            )
            .await?;
        stream
            .write_all(&frame(b'S', b"notice_delivery_marker\0seen\0").map_err(io::Error::other)?)
            .await?;
        futures::future::pending::<io::Result<()>>().await
    });
    let remote = connect_config(config(port)?, None, None, Some(budget())).await?;
    tokio::time::timeout(Duration::from_secs(2), async {
        while remote.wire.frames.load(Ordering::Acquire) < 4 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    drop(remote);
    peer.abort();
    let bytes = LOGS
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let output = String::from_utf8_lossy(&bytes);
    assert!(output.contains("positive logger control"));
    assert!(!output.contains("PRIVATE_PASSWORD_SENTINEL"));
    Ok(())
}

async fn startup(stream: &mut (impl AsyncRead + Unpin)) -> io::Result<()> {
    let length = usize::try_from(stream.read_u32().await?).map_err(io::Error::other)?;
    if !(8..=65536).contains(&length) {
        return Err(io::Error::other("invalid fixture startup"));
    }
    let mut body = vec![0; length - 4];
    stream.read_exact(&mut body).await?;
    Ok(())
}
async fn serve(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    rows: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    startup(stream).await?;
    stream.write_all(&frame(b'R', &0i32.to_be_bytes())?).await?;
    stream.write_all(&frame(b'Z', b"I")?).await?;
    loop {
        let Ok(tag) = stream.read_u8().await else {
            return Ok(());
        };
        let size = usize::try_from(stream.read_u32().await?)?;
        if !(4..=65536).contains(&size) {
            return Err("invalid fixture frontend".into());
        }
        let mut body = vec![0; size - 4];
        stream.read_exact(&mut body).await?;
        if tag == b'X' {
            return Ok(());
        }
        if tag != b'Q' {
            return Err("fixture only supports simple query".into());
        }
        let readonly = body.starts_with(b"SHOW transaction_read_only");
        let mut description = 1u16.to_be_bytes().to_vec();
        description.extend_from_slice(b"value\0");
        description.extend_from_slice(&0u32.to_be_bytes());
        description.extend_from_slice(&0u16.to_be_bytes());
        description.extend_from_slice(&25u32.to_be_bytes());
        description.extend_from_slice(&(-1i16).to_be_bytes());
        description.extend_from_slice(&(-1i32).to_be_bytes());
        description.extend_from_slice(&0u16.to_be_bytes());
        stream.write_all(&frame(b'T', &description)?).await?;
        for _ in 0..if readonly && rows != usize::MAX {
            1
        } else {
            rows.min(10000)
        } {
            let value = if readonly { &b"off"[..] } else { &b"7"[..] };
            let mut row = 1u16.to_be_bytes().to_vec();
            row.extend_from_slice(&u32::try_from(value.len())?.to_be_bytes());
            row.extend_from_slice(value);
            if stream.write_all(&frame(b'D', &row)?).await.is_err() {
                return Ok(());
            }
        }
        stream.write_all(&frame(b'C', b"SELECT 1\0")?).await?;
        stream.write_all(&frame(b'Z', b"I")?).await?;
    }
}
fn config(port: u16) -> Result<Config, tokio_postgres::Error> {
    format!("host=127.0.0.1 port={port} user=test sslmode=disable").parse()
}

#[tokio::test]
async fn ordinary_plain_rows_failover_and_readwrite_target_preserve_behavior() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        serve(&mut stream, 1).await
    });
    let mut settings = config(port)?;
    settings.host("127.0.0.1").port(port); // Preserve host+port list ordering.
    settings.target_session_attrs(TargetSessionAttrs::ReadWrite);
    let remote = connect_config(settings, None, None, Some(budget())).await?;
    let rows = remote.client().simple_query("SELECT 7").await?;
    assert!(
        rows.iter()
            .any(|row| matches!(row, SimpleQueryMessage::Row(row) if row.get(0) == Some("7")))
    );
    drop(remote);
    task.abort();
    Ok(())
}

#[tokio::test]
async fn slow_consumer_keeps_driver_backlog_within_reserved_frame_count() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        serve(&mut stream, 10000).await
    });
    let remote = connect_config(config(port)?, None, None, Some(budget())).await?;
    let before = remote.wire.frames.load(Ordering::Acquire);
    let mut rows = Box::pin(remote.client().simple_query_raw("SELECT 7").await?);
    tokio::time::sleep(Duration::from_millis(10)).await;
    let queued = remote.wire.frames.load(Ordering::Acquire) - before;
    assert!(queued <= 16, "unconsumed driver frames: {queued}");
    assert!(rows.try_next().await?.is_some());
    drop(rows);
    drop(remote);
    task.abort();
    Ok(())
}

fn tls_pair(
    name: &str,
) -> Result<(rustls::ClientConfig, rustls::ServerConfig), Box<dyn std::error::Error>> {
    let certificate = rcgen::generate_simple_self_signed(vec![name.to_owned()])?;
    let leaf = certificate.cert.der().clone();
    let key = PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der());
    let server = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_no_client_auth()
    .with_single_cert(vec![leaf.clone()], key.into())?;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(leaf)?;
    let client = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_root_certificates(roots)
    .with_no_client_auth();
    Ok((client, server))
}
async fn tls_listener(
    server: rustls::ServerConfig,
    seen: Arc<AtomicUsize>,
) -> Result<(u16, JoinHandle<()>), io::Error> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let task = tokio::spawn(async move {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
        let mut request = [0; 8];
        if stream.read_exact(&mut request).await.is_err() {
            return;
        }
        if request != [0, 0, 0, 8, 4, 210, 22, 47] {
            return;
        }
        if stream.write_all(b"S").await.is_err() {
            return;
        }
        if let Ok(mut stream) = tokio_rustls::TlsAcceptor::from(Arc::new(server))
            .accept(stream)
            .await
        {
            seen.fetch_add(1, Ordering::AcqRel);
            let _ = serve(&mut stream, 1).await;
        }
    });
    Ok((port, task))
}

#[tokio::test]
async fn verified_tls_accepts_ip_san_and_refuses_wrong_name_or_unknown_ca() -> TestResult {
    for scenario in ["valid", "valid-dns", "wrong-name", "unknown-ca"] {
        let (mut client, server) = tls_pair(if scenario == "wrong-name" {
            "wrong.example"
        } else if scenario == "valid-dns" {
            "localhost"
        } else {
            "127.0.0.1"
        })?;
        if scenario == "unknown-ca" {
            client = tls_pair("unrelated.example")?.0;
        }
        let seen = Arc::new(AtomicUsize::new(0));
        let (port, task) = tls_listener(server, Arc::clone(&seen)).await?;
        let mut settings = if scenario == "valid-dns" {
            format!("host=localhost port={port} user=test").parse::<Config>()?
        } else {
            config(port)?
        };
        settings.ssl_mode(SslMode::Require);
        let result = connect_config(settings, Some(client), None, Some(budget())).await;
        if scenario.starts_with("valid") {
            let remote = result?;
            let rows = remote.client().simple_query("SELECT 7").await?;
            assert!(
                rows.iter().any(
                    |row| matches!(row, SimpleQueryMessage::Row(row) if row.get(0) == Some("7"))
                )
            );
            assert_eq!(seen.load(Ordering::Acquire), 1);
            drop(remote);
        } else {
            assert!(result.is_err(), "accepted {scenario}");
        }
        task.abort();
    }
    Ok(())
}

#[tokio::test]
async fn tls_required_never_downgrades_and_channel_binding_is_not_ignored() -> TestResult {
    let (client, _) = tls_pair("127.0.0.1")?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let refuses_tls = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut ssl = [0; 8];
        stream.read_exact(&mut ssl).await?;
        stream.write_all(b"N").await?;
        let mut startup = [0; 1];
        stream.read(&mut startup).await
    });
    let mut settings = config(port)?;
    settings.ssl_mode(SslMode::Require);
    assert!(
        connect_config(settings, Some(client), None, Some(budget()))
            .await
            .is_err()
    );
    assert_eq!(
        refuses_tls.await??,
        0,
        "TLS refusal must not send a plaintext startup packet"
    );
    let (client, server) = tls_pair("127.0.0.1")?;
    let seen = Arc::new(AtomicUsize::new(0));
    let (port, task) = tls_listener(server, seen).await?;
    let mut settings = config(port)?;
    settings
        .ssl_mode(SslMode::Require)
        .channel_binding(ChannelBinding::Require);
    // The fixture uses trust authentication, which cannot satisfy binding.
    assert!(
        connect_config(settings, Some(client), None, Some(budget()))
            .await
            .is_err()
    );
    task.abort();
    let mut settings = config(1)?;
    settings.channel_binding(ChannelBinding::Require);
    let error = start(
        Socket::Tcp(
            TcpStream::connect({
                // Use a real peer so refusal is based on binding, not connect failure.
                let listener = TcpListener::bind("127.0.0.1:0").await?;
                let address = listener.local_addr()?;
                tokio::spawn(async move {
                    let _ = listener.accept().await;
                });
                address
            })
            .await?,
        ),
        &settings,
        Some("127.0.0.1"),
        None,
        Wire::new(Some(budget()), None)?,
    )
    .await;
    assert!(matches!(error, Err(FederationError::Unsupported(_))));
    Ok(())
}

#[tokio::test]
async fn driver_rejects_giant_header_without_payload_and_sanitizes_peer_messages() -> TestResult {
    for hostile_message in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            startup(&mut socket).await?;
            if hostile_message {
                socket
                    .write_all(
                        &frame(b'E', b"SERROR\0C28P01\0MPASSWORD_SENTINEL_NEVER_EXPOSE\0\0")
                            .map_err(io::Error::other)?,
                    )
                    .await?;
            } else {
                let mut header = vec![b'R'];
                header.extend_from_slice(&i32::MAX.to_be_bytes());
                socket.write_all(&header).await?;
            }
            futures::future::pending::<io::Result<()>>().await
        });
        let mut settings = config(port)?;
        settings.password("PASSWORD_SENTINEL_NEVER_EXPOSE");
        let error = connect_config(settings, None, None, Some(budget()))
            .await
            .err()
            .ok_or("accepted hostile startup")?;
        if !hostile_message {
            assert!(matches!(
                error,
                FederationError::Query(QueryError::ResourceLimit(_))
            ));
        }
        assert!(!format!("{error:?} {error}").contains("PASSWORD_SENTINEL"));
        peer.abort();
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn explicit_unix_socket_remains_supported() -> TestResult {
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join(".s.PGSQL.5432");
    let listener = tokio::net::UnixListener::bind(socket)?;
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        serve(&mut stream, 1).await
    });
    let mut settings = Config::new();
    settings
        .host_path(directory.path())
        .user("test")
        .ssl_mode(SslMode::Disable);
    let remote = connect_config(settings, None, None, Some(budget())).await?;
    let rows = remote.client().simple_query("SELECT 7").await?;
    assert!(
        rows.iter()
            .any(|row| matches!(row, SimpleQueryMessage::Row(row) if row.get(0) == Some("7")))
    );
    drop(remote);
    task.abort();
    Ok(())
}

#[tokio::test]
async fn cancellation_releases_connection_lease_and_passwords_never_enter_errors() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        startup(&mut stream).await?;
        futures::future::pending::<io::Result<()>>().await
    });
    let budget = budget();
    let cancel = Arc::clone(&budget);
    let cancellation = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel.cancelled.store(true, Ordering::Release);
    });
    let result = connect_config(config(address.port())?, None, None, Some(budget.clone())).await;
    assert!(matches!(
        result,
        Err(FederationError::Query(QueryError::Cancelled(_)))
    ));
    cancellation.await?;
    assert_eq!(
        *budget
            .used
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        0
    );
    task.abort();
    let error = connect("password='PASSWORD_SENTINEL_NEVER_EXPOSE", None)
        .await
        .err()
        .ok_or("accepted malformed connection")?;
    assert!(!format!("{error:?} {error}").contains("PASSWORD_SENTINEL"));
    Ok(())
}
