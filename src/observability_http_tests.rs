use super::*;
use tokio::{io::duplex, net::TcpStream, task::JoinHandle};

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct Fixture {
    address: SocketAddr,
    connections: Arc<Semaphore>,
    server: JoinHandle<Result<(), ObservabilityError>>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn fixture(timeout: Duration) -> Result<Fixture, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let connections = Arc::new(Semaphore::new(1));
    let registry = MetricsRegistry::new();
    registry.increment_counter("test_metric_total", &[], 1)?;
    let server = tokio::spawn(serve_metrics_bounded(
        listener,
        registry,
        Some("secret".to_owned()),
        Arc::clone(&connections),
        timeout,
    ));
    Ok(Fixture {
        address,
        connections,
        server,
    })
}

async fn wait_for_permits(permits: &Semaphore, count: usize) -> TestResult {
    tokio::time::timeout(Duration::from_secs(2), async {
        while permits.available_permits() != count {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn idle_metrics_clients_are_limited_and_expire_before_a_fresh_scrape() -> TestResult {
    let fixture = fixture(Duration::from_secs(1)).await?;
    let mut idle = TcpStream::connect(fixture.address).await?;
    wait_for_permits(&fixture.connections, 0).await?;
    let mut refused = TcpStream::connect(fixture.address).await?;
    let mut byte = [0];
    let refused_result =
        tokio::time::timeout(Duration::from_millis(300), refused.read(&mut byte)).await?;
    assert!(matches!(refused_result, Ok(0)) || refused_result.is_err());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), idle.read(&mut byte)).await??,
        0
    );
    wait_for_permits(&fixture.connections, 1).await?;
    let mut scrape = TcpStream::connect(fixture.address).await?;
    scrape
        .write_all(b"GET /metrics HTTP/1.1\r\nAuthorization: Bearer secret\r\n\r\n")
        .await?;
    let mut response = String::new();
    tokio::time::timeout(Duration::from_secs(1), scrape.read_to_string(&mut response)).await??;
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains("test_metric_total 1"), "{response}");
    Ok(())
}

#[tokio::test]
async fn metrics_headers_can_be_fragmented_and_authorization_names_are_case_insensitive()
-> TestResult {
    let (mut client, server) = duplex(4096);
    let worker = tokio::spawn(async move {
        metrics_connection(
            server,
            &MetricsRegistry::new(),
            Some("secret"),
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await;
    });
    client
        .write_all(b"GET /metrics HTTP/1.1\r\nauthoriz")
        .await?;
    let mut first = [0];
    assert!(
        tokio::time::timeout(Duration::from_millis(20), client.read(&mut first))
            .await
            .is_err()
    );
    client.write_all(b"ation: bearer secret\r\n\r\n").await?;
    let mut response = String::new();
    tokio::time::timeout(Duration::from_secs(1), client.read_to_string(&mut response)).await??;
    worker.await?;
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    Ok(())
}

#[tokio::test]
async fn trickled_headers_do_not_reset_the_absolute_connection_deadline() -> TestResult {
    let (mut client, server) = duplex(4096);
    let started = tokio::time::Instant::now();
    let worker = tokio::spawn(async move {
        metrics_connection(
            server,
            &MetricsRegistry::new(),
            None,
            started + Duration::from_millis(300),
        )
        .await;
    });
    client.write_all(b"GET /metrics HTTP/1.1\r\n").await?;
    tokio::time::sleep(Duration::from_millis(150)).await;
    client.write_all(b"Host: localhost\r\n").await?;
    let mut response = Vec::new();
    tokio::time::timeout(
        Duration::from_millis(220),
        client.read_to_end(&mut response),
    )
    .await??;
    worker.await?;
    assert_eq!(response, Vec::<u8>::new());
    Ok(())
}

#[tokio::test]
async fn a_metrics_client_that_never_reads_cannot_retain_the_writer() -> TestResult {
    let (mut client, server) = duplex(64);
    let worker = tokio::spawn(async move {
        metrics_connection(
            server,
            &MetricsRegistry::new(),
            None,
            tokio::time::Instant::now() + Duration::from_millis(50),
        )
        .await;
    });
    client.write_all(b"GET /metrics HTTP/1.1\r\n\r\n").await?;
    // The response headers exceed the 64-byte pipe. No reader drains it until
    // the writer has hit its deadline and exited.
    tokio::time::timeout(Duration::from_secs(1), worker).await??;
    let mut response = Vec::new();
    client.read_to_end(&mut response).await?;
    assert_eq!(response.len(), 64);
    Ok(())
}

#[tokio::test]
async fn oversized_metrics_headers_are_rejected_at_the_header_budget() -> TestResult {
    let (mut client, server) = duplex(MAX_METRICS_HEADER_BYTES * 2);
    let worker = tokio::spawn(async move {
        metrics_connection(
            server,
            &MetricsRegistry::new(),
            None,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await;
    });
    let mut request = b"GET /metrics HTTP/1.1\r\nX-Fill: ".to_vec();
    request.resize(MAX_METRICS_HEADER_BYTES + 1, b'x');
    client.write_all(&request).await?;
    let mut response = String::new();
    tokio::time::timeout(Duration::from_secs(1), client.read_to_string(&mut response)).await??;
    worker.await?;
    assert!(response.starts_with("HTTP/1.1 431 "), "{response}");
    Ok(())
}

#[test]
fn metrics_authentication_uses_only_complete_headers_and_rejects_ambiguous_credentials() {
    let registry = MetricsRegistry::new();
    assert!(
        metrics_response(b"GET /metrics HTTP/1.1", &registry, Some("secret"))
            .starts_with("HTTP/1.1 401 ")
    );
    assert!(metrics_response(b"GET /metrics HTTP/1.1\r\nAuthorization: Bearer secret\r\nauthorization: Bearer secret", &registry, Some("secret")).starts_with("HTTP/1.1 400 "));
    assert!(
        metrics_response(b"GET /private HTTP/1.1", &registry, None).starts_with("HTTP/1.1 404 ")
    );
    assert!(
        metrics_response(b"POST /metrics HTTP/1.1", &registry, None).starts_with("HTTP/1.1 405 ")
    );
}
