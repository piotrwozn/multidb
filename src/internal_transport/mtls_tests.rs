//! Runtime-generated keys are test fixtures only; no production credentials.

use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use rcgen::{
    BasicConstraints, CertificateParams, CertificateRevocationListParams, CertifiedIssuer,
    ExtendedKeyUsagePurpose, IsCa, KeyIdMethod, KeyPair, KeyUsagePurpose, RevokedCertParams,
};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

use super::{
    ApRequest, InternalRequest, InternalResponse, InternalTransportClient, InternalTransportError,
    InternalTransportServer, RaftRequest, client_config, read_frame, write_frame,
};
use crate::phase30::{
    InternalPeerTlsConfig, InternalTlsConfig, InternalTransportConfig, InternalTransportSecurity,
};
use crate::repl::{NodeId, VersionedWrite};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(super) struct TestPki {
    directory: tempfile::TempDir,
    ca: CertifiedIssuer<'static, KeyPair>,
    ca_path: PathBuf,
    nodes: BTreeMap<NodeId, (PathBuf, PathBuf)>,
}

impl TestPki {
    pub(super) fn new() -> TestResult<Self> {
        let directory = tempfile::tempdir()?;
        let mut params = CertificateParams::new(Vec::<String>::new())?;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca = CertifiedIssuer::self_signed(params, KeyPair::generate()?)?;
        let ca_path = directory.path().join("ca.pem");
        std::fs::write(&ca_path, ca.pem())?;
        let mut nodes = BTreeMap::new();
        for node in 1..=3 {
            let mut params = CertificateParams::new(vec![format!("node-{node}.multidb.test")])?;
            params.serial_number = Some(node.into());
            params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            params.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ClientAuth,
                ExtendedKeyUsagePurpose::ServerAuth,
            ];
            let key = KeyPair::generate()?;
            let certificate = params.signed_by(&key, &ca)?;
            let cert_path = directory.path().join(format!("node-{node}.pem"));
            let key_path = directory.path().join(format!("node-{node}.key"));
            std::fs::write(&cert_path, certificate.pem())?;
            std::fs::write(&key_path, key.serialize_pem())?;
            nodes.insert(node, (cert_path, key_path));
        }
        Ok(Self {
            directory,
            ca,
            ca_path,
            nodes,
        })
    }

    pub(super) fn tls(&self, node: NodeId, admitted: &[NodeId]) -> InternalTlsConfig {
        let (cert_path, key_path) = &self.nodes[&node];
        InternalTlsConfig {
            cert_path: cert_path.clone(),
            key_path: key_path.clone(),
            ca_cert_path: self.ca_path.clone(),
            local_node: node,
            peers: admitted
                .iter()
                .map(|peer| {
                    (
                        *peer,
                        InternalPeerTlsConfig {
                            server_name: format!("node-{peer}.multidb.test"),
                            cert_path: self.nodes[peer].0.clone(),
                        },
                    )
                })
                .collect(),
            crl_paths: Vec::new(),
        }
    }

    pub(super) fn transport(tls: InternalTlsConfig) -> InternalTransportConfig {
        let mut config =
            InternalTransportConfig::new("127.0.0.1:0", InternalTransportSecurity::Mtls(tls));
        config.connect_timeout_ms = 2_000;
        config.handshake_timeout_ms = 2_000;
        config.request_timeout_ms = 3_000;
        config.idle_timeout_ms = 2_000;
        config
    }

    fn revocations(&self, revoked: NodeId) -> TestResult<PathBuf> {
        let now = time::OffsetDateTime::now_utc();
        let crl = CertificateRevocationListParams {
            this_update: now - time::Duration::minutes(1),
            next_update: now + time::Duration::days(1),
            crl_number: 1_u64.into(),
            issuing_distribution_point: None,
            revoked_certs: vec![RevokedCertParams {
                serial_number: revoked.into(),
                revocation_time: now - time::Duration::minutes(1),
                reason_code: None,
                invalidity_date: None,
            }],
            key_identifier_method: KeyIdMethod::Sha256,
        }
        .signed_by(&self.ca)?;
        let path = self
            .directory
            .path()
            .join(format!("revoked-{revoked}.crl.pem"));
        std::fs::write(&path, crl.pem()?)?;
        Ok(path)
    }
}

struct Server {
    address: String,
    endpoint: Arc<super::tests::FakeApEndpoint>,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<Result<(), InternalTransportError>>,
}

impl Server {
    async fn start(tls: InternalTlsConfig) -> TestResult<Self> {
        let node = tls.local_node;
        let config = TestPki::transport(tls);
        super::validate_node_config(&config, node)?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?.to_string();
        let endpoint = Arc::new(super::tests::FakeApEndpoint::default());
        let (shutdown, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(InternalTransportServer::serve_ap_until_shutdown(
            listener,
            config,
            node,
            endpoint.clone(),
            async {
                let _ = shutdown_rx.await;
            },
        ));
        Ok(Self {
            address,
            endpoint,
            shutdown,
            task,
        })
    }

    fn client(
        &self,
        tls: InternalTlsConfig,
        target: NodeId,
    ) -> TestResult<InternalTransportClient> {
        Ok(InternalTransportClient::new(
            TestPki::transport(tls),
            BTreeMap::from([(target, self.address.clone())]),
        )?)
    }

    fn writes(&self) -> TestResult<Vec<VersionedWrite>> {
        self.endpoint
            .writes
            .lock()
            .map(|writes| writes.clone())
            .map_err(|_| "test endpoint lock poisoned".into())
    }

    async fn stop(self) -> TestResult {
        let _ = self.shutdown.send(());
        self.task.await??;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_binds_both_nodes_and_allows_ap_and_health() -> TestResult {
    let pki = TestPki::new()?;
    let server = Server::start(pki.tls(2, &[1])).await?;
    let client = server.client(pki.tls(1, &[2]), 2)?;
    assert_eq!(
        client
            .request_async(2, InternalRequest::HealthPing { from: 1 })
            .await?,
        InternalResponse::HealthPong { node: 2 }
    );
    let request = InternalRequest::Ap(ApRequest::SendBatch {
        writes: vec![VersionedWrite {
            table: "t".to_owned(),
            key: b"k".to_vec(),
            version: crate::repl::VersionedBytes {
                value: Some(b"v".to_vec()),
                clock: crate::repl::VectorClock::new([(1, 1)]),
                origin: 1,
            },
        }],
    });
    assert!(matches!(
        client.request_async(2, request).await?,
        InternalResponse::Ap(super::ApResponse::Ack)
    ));
    assert_eq!(server.writes()?.len(), 1);
    server.stop().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_rejects_unknown_client_and_wrong_server_node_pin() -> TestResult {
    let pki = TestPki::new()?;
    let server = Server::start(pki.tls(2, &[1])).await?;
    let unknown = server.client(pki.tls(3, &[2]), 2)?;
    assert!(
        unknown
            .request_async(2, InternalRequest::HealthPing { from: 3 })
            .await
            .is_err()
    );
    // CA and SAN are valid for the reached node, but the requested NodeId is 3.
    let mut tls = pki.tls(1, &[3]);
    tls.peers.insert(
        3,
        InternalPeerTlsConfig {
            server_name: "node-2.multidb.test".to_owned(),
            cert_path: pki.nodes[&3].0.clone(),
        },
    );
    let wrong_pin = server.client(tls, 3)?;
    assert!(
        wrong_pin
            .request_async(3, InternalRequest::HealthPing { from: 1 })
            .await
            .is_err()
    );
    assert_eq!(server.writes()?, Vec::new());
    server.stop().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_rejects_claimed_sender_before_dispatch_even_with_a_valid_certificate() -> TestResult {
    let pki = TestPki::new()?;
    let server = Server::start(pki.tls(2, &[1])).await?;
    let tls = pki.tls(1, &[2]);
    let client = server.client(tls.clone(), 2)?;
    assert!(
        client
            .request_async(2, InternalRequest::HealthPing { from: 3 })
            .await
            .is_err()
    );
    // Bypass the friendly client's precheck: the receiving server must enforce it too.
    for request in [
        InternalRequest::HealthPing { from: 3 },
        InternalRequest::FlowAck {
            from: 3,
            frames: 1,
            bytes: 1,
        },
        InternalRequest::Raft(RaftRequest::Vote {
            from: 3,
            term: 1,
            payload: b"{}".to_vec(),
        }),
    ] {
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config(&tls)?));
        let socket = tokio::net::TcpStream::connect(&server.address).await?;
        let mut stream = connector
            .connect("node-2.multidb.test".try_into()?, socket)
            .await?;
        write_frame(&mut stream, 4096, &request).await?;
        assert!(
            read_frame::<_, InternalResponse>(&mut stream, 4096, 2_000)
                .await
                .is_err()
        );
    }
    assert_eq!(server.writes()?, Vec::new());
    server.stop().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_checks_san_in_both_directions() -> TestResult {
    let pki = TestPki::new()?;
    let mut server_tls = pki.tls(2, &[1]);
    server_tls
        .peers
        .get_mut(&1)
        .ok_or("missing peer")?
        .server_name = "wrong.multidb.test".to_owned();
    let server = Server::start(server_tls).await?;
    let client = server.client(pki.tls(1, &[2]), 2)?;
    assert!(
        client
            .request_async(2, InternalRequest::HealthPing { from: 1 })
            .await
            .is_err()
    );
    server.stop().await?;

    let server = Server::start(pki.tls(2, &[1])).await?;
    let mut client_tls = pki.tls(1, &[2]);
    client_tls
        .peers
        .get_mut(&2)
        .ok_or("missing peer")?
        .server_name = "wrong.multidb.test".to_owned();
    let client = server.client(client_tls, 2)?;
    assert!(
        client
            .request_async(2, InternalRequest::HealthPing { from: 1 })
            .await
            .is_err()
    );
    server.stop().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_rejects_removed_ca_even_when_leaf_pin_matches() -> TestResult {
    let pki = TestPki::new()?;
    let other = TestPki::new()?;
    let server = Server::start(pki.tls(2, &[1])).await?;
    let mut tls = pki.tls(1, &[2]);
    tls.ca_cert_path = other.ca_path.clone();
    assert!(
        server
            .client(tls, 2)?
            .request_async(2, InternalRequest::HealthPing { from: 1 })
            .await
            .is_err()
    );
    server.stop().await?;
    let mut tls = pki.tls(2, &[1]);
    tls.ca_cert_path = other.ca_path;
    let server = Server::start(tls).await?;
    assert!(
        server
            .client(pki.tls(1, &[2]), 2)?
            .request_async(2, InternalRequest::HealthPing { from: 1 })
            .await
            .is_err()
    );
    server.stop().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_enforces_crls_for_client_and_server_certificates() -> TestResult {
    let pki = TestPki::new()?;
    let mut tls = pki.tls(2, &[1]);
    tls.crl_paths = vec![pki.revocations(1)?];
    let server = Server::start(tls).await?;
    assert!(
        server
            .client(pki.tls(1, &[2]), 2)?
            .request_async(2, InternalRequest::HealthPing { from: 1 })
            .await
            .is_err()
    );
    server.stop().await?;
    let server = Server::start(pki.tls(2, &[1])).await?;
    let mut tls = pki.tls(1, &[2]);
    tls.crl_paths = vec![pki.revocations(2)?];
    assert!(
        server
            .client(tls, 2)?
            .request_async(2, InternalRequest::HealthPing { from: 1 })
            .await
            .is_err()
    );
    server.stop().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_rejects_no_alpn_and_plaintext_without_downgrade() -> TestResult {
    let pki = TestPki::new()?;
    let server = Server::start(pki.tls(2, &[1])).await?;
    let mut config = client_config(&pki.tls(1, &[2]))?;
    config.alpn_protocols.clear();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let socket = tokio::net::TcpStream::connect(&server.address).await?;
    if let Ok(mut stream) = connector
        .connect("node-2.multidb.test".try_into()?, socket)
        .await
    {
        let result = async {
            write_frame(&mut stream, 4096, &InternalRequest::HealthPing { from: 1 }).await?;
            read_frame::<_, InternalResponse>(&mut stream, 4096, 2_000).await
        }
        .await;
        assert!(result.is_err());
    }
    let mut socket = tokio::net::TcpStream::connect(&server.address).await?;
    write_frame(&mut socket, 4096, &InternalRequest::HealthPing { from: 1 }).await?;
    assert!(
        read_frame::<_, InternalResponse>(&mut socket, 4096, 3_000)
            .await
            .is_err()
    );
    assert_eq!(server.writes()?, Vec::new());
    server.stop().await
}

#[test]
fn mtls_configuration_rejects_ambiguous_malformed_and_legacy_identities() -> TestResult {
    let pki = TestPki::new()?;
    let tls = pki.tls(1, &[2]);
    let config = TestPki::transport(tls.clone());
    assert!(super::validate_node_config(&config, 2).is_err());
    assert!(
        InternalTransportClient::new(config, BTreeMap::from([(3, "127.0.0.1:1".to_owned())]))
            .is_err()
    );
    let mut duplicate = tls.clone();
    duplicate.peers.insert(3, duplicate.peers[&2].clone());
    assert!(super::PeerIdentities::load(&duplicate).is_err());
    let malformed = pki.directory.path().join("malformed.pem");
    std::fs::write(
        &malformed,
        "-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n",
    )?;
    let mut invalid = tls;
    invalid.peers.get_mut(&2).ok_or("missing peer")?.cert_path = malformed;
    assert!(super::PeerIdentities::load(&invalid).is_err());
    assert!(serde_json::from_value::<InternalTlsConfig>(serde_json::json!({
        "cert_path":"node.pem", "key_path":"node.key", "ca_cert_path":"ca.pem", "server_name":"localhost",
    })).is_err());
    Ok(())
}
