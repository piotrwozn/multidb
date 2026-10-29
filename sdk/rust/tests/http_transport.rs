use multidb_client::ControlPlaneClient;
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::mpsc,
    thread,
    time::Duration,
};

fn read_headers(stream: &mut std::net::TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut headers = Vec::new();
    while !headers.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        stream.read_exact(&mut byte).unwrap();
        headers.push(byte[0]);
        assert!(headers.len() < 8192);
    }
    String::from_utf8(headers).unwrap()
}

#[test]
fn handles_chunked_root_path_without_waiting_for_socket_close() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (release, held) = mpsc::channel();
    let worker = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        assert!(read_headers(&mut stream).starts_with("GET /health HTTP/1.1\r\n"));
        let body = br#"{"ok":true,"status":"alive"}"#;
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n",
            body.len()
        )
        .unwrap();
        stream.write_all(body).unwrap();
        stream.write_all(b"\r\n0\r\n\r\n").unwrap();
        // Reading HTTP bodies must honor framing rather than require EOF.
        held.recv_timeout(Duration::from_secs(3)).unwrap();
    });
    let health = ControlPlaneClient::with_base_url(base).health().unwrap();
    assert_eq!(health.status, "alive");
    release.send(()).unwrap();
    worker.join().unwrap();
}

#[test]
fn refuses_redirect_without_forwarding_credentials() {
    let destination = TcpListener::bind("127.0.0.1:0").unwrap();
    destination.set_nonblocking(true).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let target = destination.local_addr().unwrap();
    let worker = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        assert!(read_headers(&mut stream).contains("Bearer secret"));
        write!(
            stream,
            "HTTP/1.1 302 Found\r\nLocation: http://{target}/stolen\r\nContent-Length: 0\r\n\r\n"
        )
        .unwrap();
    });
    let error = ControlPlaneClient::with_base_url(base)
        .with_token("secret")
        .status()
        .unwrap_err();
    assert_eq!(error.code, "redirect_refused");
    assert_eq!(
        destination.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    worker.join().unwrap();
}

#[test]
fn rejects_header_injection_before_sending_and_non_success_health() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let error = ControlPlaneClient::with_base_url(base)
        .with_token("secret\r\nx-injected: yes")
        .status()
        .unwrap_err();
    assert_eq!(error.code, "transport_error");
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let worker = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_headers(&mut stream);
        let body = br#"{"ok":true,"status":"alive"}"#;
        write!(
            stream,
            "HTTP/1.1 503 Unavailable\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .unwrap();
        stream.write_all(body).unwrap();
    });
    assert_eq!(
        ControlPlaneClient::with_base_url(base)
            .health()
            .unwrap_err()
            .code,
        "http_503"
    );
    worker.join().unwrap();
}
