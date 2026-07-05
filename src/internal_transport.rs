use std::{
    collections::BTreeMap,
    future::Future,
    io,
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use rustls_pki_types::pem::PemObject;
use serde::{Serialize, de::DeserializeOwned};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};
use tokio_rustls::{
    TlsAcceptor, TlsConnector, TlsStream,
    rustls::{
        ClientConfig, RootCertStore, ServerConfig, client::WebPkiServerVerifier, crypto::ring,
        server::WebPkiClientVerifier,
    },
};

use crate::{
    observability,
    phase30::{
        FlowControlConfig, InternalTlsConfig, InternalTransportConfig, InternalTransportSecurity,
    },
    repl::{
        ApTransport, CpClusterStatus, NodeId, RaftNode, ReplError, VersionedBytes, VersionedRecord,
        VersionedWrite, ap::ApReplicaEndpoint,
    },
    storage::Bytes,
};

const INTERNAL_ALPN: &[u8] = b"multidb-internal/1";
const FRAME_LEN_BYTES: usize = 4;

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum InternalRequest {
    HealthPing {
        from: NodeId,
    },
    FlowAck {
        from: NodeId,
        frames: usize,
        bytes: usize,
    },
    Raft(RaftRequest),
    ClusterAdmin(ClusterAdminRequest),
    Ap(ApRequest),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum InternalResponse {
    HealthPong { node: NodeId },
    FlowAck,
    Raft(RaftResponse),
    ClusterAdmin(ClusterAdminResponse),
    Ap(ApResponse),
    Error(String),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum RaftRequest {
    AppendEntries {
        from: NodeId,
        term: u64,
        payload: Bytes,
    },
    Vote {
        from: NodeId,
        term: u64,
        payload: Bytes,
    },
    PreVote {
        from: NodeId,
        term: u64,
        payload: Bytes,
    },
    InstallSnapshot {
        from: NodeId,
        term: u64,
        snapshot_id: String,
        offset: u64,
        done: bool,
        chunk: Bytes,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum RaftResponse {
    Accepted { term: u64, payload: Bytes },
    Rejected { term: u64, reason: String },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ClusterAdminRequest {
    ChangeMembership {
        voters: Vec<RaftNode>,
        learners: Vec<RaftNode>,
    },
    TransferLeader {
        target: NodeId,
    },
    Status,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ClusterAdminResponse {
    Accepted,
    Status(CpClusterStatus),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ApRequest {
    SendBatch { writes: Vec<VersionedWrite> },
    ReadVersions { table: String, key: Bytes },
    ReadRecords { version_keys: Vec<Bytes> },
    ReadAllVersions,
    ReadMerkle { prefix: Bytes, limit: usize },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ApResponse {
    Ack,
    Versions(Vec<VersionedBytes>),
    Records(Vec<VersionedRecord>),
    Merkle(Vec<(Bytes, Bytes)>),
}

#[derive(thiserror::Error, Debug)]
pub enum InternalTransportError {
    #[error("io: {0}")]
    Io(#[from] io::Error),

    #[error("tls: {0}")]
    Tls(String),

    #[error("codec: {0}")]
    Codec(String),

    #[error("peer {0} is not configured")]
    MissingPeer(NodeId),

    #[error("flow-control backpressure for peer {peer}: {frames} frames/{bytes} bytes inflight")]
    Backpressure {
        peer: NodeId,
        frames: usize,
        bytes: usize,
    },

    #[error("remote error: {0}")]
    Remote(String),

    #[error("runtime: {0}")]
    Runtime(String),
}

#[derive(Clone)]
pub struct InternalTransportClient {
    config: InternalTransportConfig,
    peers: Arc<BTreeMap<NodeId, String>>,
    flow: Arc<Mutex<BTreeMap<NodeId, Inflight>>>,
    client_tls: Option<Arc<ClientConfig>>,
}

pub struct TcpApTransport {
    client: InternalTransportClient,
}

pub struct InternalTransportServer;

pub trait InternalRaftEndpoint: Send + Sync {
    /// Handles one mounted Raft protocol RPC.
    /// # Errors
    /// Returns a replication error when the local Raft runtime rejects the request.
    fn handle_raft(&self, request: RaftRequest) -> Result<RaftResponse, ReplError>;

    /// Handles one mounted cluster-admin RPC.
    /// # Errors
    /// Returns a replication error when the local cluster runtime rejects the request.
    fn handle_cluster_admin(
        &self,
        request: ClusterAdminRequest,
    ) -> Result<ClusterAdminResponse, ReplError>;
}

#[derive(Clone, Copy, Debug, Default)]
struct Inflight {
    frames: usize,
    bytes: usize,
}

struct FlowReservation {
    peer: NodeId,
    bytes: usize,
    flow: Arc<Mutex<BTreeMap<NodeId, Inflight>>>,
}

impl InternalTransportClient {
    /// Creates a reusable node-to-node client over the internal framed protocol.
    ///
    /// # Errors
    /// Fails when TLS configuration cannot be loaded.
    pub fn new(
        config: InternalTransportConfig,
        peers: BTreeMap<NodeId, String>,
    ) -> Result<Self, InternalTransportError> {
        let client_tls = match &config.security {
            InternalTransportSecurity::Mtls(tls) => Some(Arc::new(client_config(tls)?)),
            #[cfg(any(test, feature = "insecure-transport"))]
            InternalTransportSecurity::PlaintextForTests => None,
        };
        Ok(Self {
            config,
            peers: Arc::new(peers),
            flow: Arc::new(Mutex::new(BTreeMap::new())),
            client_tls,
        })
    }

    /// Sends one framed request to a peer and waits for a response.
    ///
    /// # Errors
    /// Fails when the peer is unavailable, rejects the request, or flow-control is exhausted.
    pub fn request(
        &self,
        target: NodeId,
        request: InternalRequest,
    ) -> Result<InternalResponse, InternalTransportError> {
        let client = self.clone();
        if tokio::runtime::Handle::try_current().is_ok() {
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| InternalTransportError::Runtime(error.to_string()))?;
                runtime.block_on(client.request_async(target, request))
            })
            .join()
            .map_err(|_| InternalTransportError::Runtime("transport worker panicked".to_owned()))?
        } else {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| InternalTransportError::Runtime(error.to_string()))?;
            runtime.block_on(self.request_async(target, request))
        }
    }

    /// Sends one framed request asynchronously.
    ///
    /// # Errors
    /// Fails when the peer is missing, flow-control rejects the frame, network I/O fails,
    /// TLS negotiation fails, the frame is invalid, or the peer returns an error.
    pub async fn request_async(
        &self,
        target: NodeId,
        request: InternalRequest,
    ) -> Result<InternalResponse, InternalTransportError> {
        let address = self
            .peers
            .get(&target)
            .cloned()
            .ok_or(InternalTransportError::MissingPeer(target))?;
        let bytes = encode_frame(&request, self.config.max_frame_bytes)?;
        let _reservation = self.reserve(target, bytes.len())?;
        let connect_timeout = Duration::from_millis(self.config.connect_timeout_ms.max(1));
        let request_timeout = Duration::from_millis(self.config.request_timeout_ms.max(1));

        tokio::time::timeout(request_timeout, async {
            let socket = tokio::time::timeout(connect_timeout, TcpStream::connect(&address))
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("connect to {address} timed out"),
                    )
                })??;
            socket.set_nodelay(true)?;
            let mut stream = self.client_stream(socket).await?;
            write_encoded_frame(&mut stream, &bytes).await?;
            let response = read_frame::<_, InternalResponse>(
                &mut stream,
                self.config.max_frame_bytes,
                self.config.idle_timeout_ms,
            )
            .await?;
            match response {
                InternalResponse::Error(error) => Err(InternalTransportError::Remote(error)),
                other => Ok(other),
            }
        })
        .await
        .map_err(|_| {
            InternalTransportError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                "internal transport request timed out",
            ))
        })?
    }

    async fn client_stream(
        &self,
        socket: TcpStream,
    ) -> Result<MaybeTlsStream, InternalTransportError> {
        match &self.config.security {
            InternalTransportSecurity::Mtls(tls) => {
                let Some(config) = &self.client_tls else {
                    return Err(InternalTransportError::Tls(
                        "mTLS client config is unavailable".to_owned(),
                    ));
                };
                let server_name = tls
                    .server_name
                    .clone()
                    .try_into()
                    .map_err(|error| InternalTransportError::Tls(format!("{error:?}")))?;
                let connector = TlsConnector::from(Arc::clone(config));
                let stream = tokio::time::timeout(
                    Duration::from_millis(self.config.handshake_timeout_ms.max(1)),
                    connector.connect(server_name, socket),
                )
                .await
                .map_err(|_| {
                    InternalTransportError::Io(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "mTLS client handshake timed out",
                    ))
                })?
                .map_err(|error| InternalTransportError::Tls(error.to_string()))?;
                Ok(MaybeTlsStream::Tls(Box::new(TlsStream::Client(stream))))
            }
            #[cfg(any(test, feature = "insecure-transport"))]
            InternalTransportSecurity::PlaintextForTests => Ok(MaybeTlsStream::Plain(socket)),
        }
    }

    fn reserve(
        &self,
        peer: NodeId,
        frame_bytes: usize,
    ) -> Result<FlowReservation, InternalTransportError> {
        let flow_config = &self.config.flow_control;
        let mut state = self
            .flow
            .lock()
            .map_err(|_| InternalTransportError::Runtime("flow lock poisoned".to_owned()))?;
        let inflight = state.entry(peer).or_default();
        let next_frames = inflight.frames.saturating_add(1);
        let next_bytes = inflight.bytes.saturating_add(frame_bytes);
        if next_frames > flow_config.max_inflight_frames_per_peer.max(1)
            || next_bytes > flow_config.max_inflight_bytes_per_peer.max(1)
        {
            observability::record_internal_transport_flow_control(peer, "backpressure");
            return Err(InternalTransportError::Backpressure {
                peer,
                frames: inflight.frames,
                bytes: inflight.bytes,
            });
        }
        inflight.frames = next_frames;
        inflight.bytes = next_bytes;
        Ok(FlowReservation {
            peer,
            bytes: frame_bytes,
            flow: Arc::clone(&self.flow),
        })
    }
}

impl TcpApTransport {
    /// Creates a production AP transport backed by the shared internal protocol.
    ///
    /// # Errors
    /// Fails when TLS configuration cannot be loaded.
    pub fn new(
        config: InternalTransportConfig,
        peers: BTreeMap<NodeId, String>,
    ) -> Result<Self, InternalTransportError> {
        Ok(Self {
            client: InternalTransportClient::new(config, peers)?,
        })
    }

    #[must_use]
    pub fn client(&self) -> &InternalTransportClient {
        &self.client
    }

    fn ap_request(&self, target: NodeId, request: ApRequest) -> Result<ApResponse, ReplError> {
        match self
            .client
            .request(target, InternalRequest::Ap(request))
            .map_err(|error| internal_to_repl(&error))?
        {
            InternalResponse::Ap(response) => Ok(response),
            other => Err(ReplError::Transport(format!(
                "unexpected internal transport response {other:?}"
            ))),
        }
    }
}

impl ApTransport for TcpApTransport {
    fn send_batch(&self, target: NodeId, writes: &[VersionedWrite]) -> Result<(), ReplError> {
        match self.ap_request(
            target,
            ApRequest::SendBatch {
                writes: writes.to_vec(),
            },
        )? {
            ApResponse::Ack => Ok(()),
            other => Err(ReplError::Transport(format!(
                "unexpected AP send response {other:?}"
            ))),
        }
    }

    fn read_versions(
        &self,
        target: NodeId,
        table: &str,
        key: &[u8],
    ) -> Result<Vec<VersionedBytes>, ReplError> {
        match self.ap_request(
            target,
            ApRequest::ReadVersions {
                table: table.to_owned(),
                key: key.to_vec(),
            },
        )? {
            ApResponse::Versions(versions) => Ok(versions),
            other => Err(ReplError::Transport(format!(
                "unexpected AP read response {other:?}"
            ))),
        }
    }

    fn read_all_versions(&self, target: NodeId) -> Result<Vec<VersionedRecord>, ReplError> {
        match self.ap_request(target, ApRequest::ReadAllVersions)? {
            ApResponse::Records(records) => Ok(records),
            other => Err(ReplError::Transport(format!(
                "unexpected AP scan response {other:?}"
            ))),
        }
    }

    fn read_merkle(&self, target: NodeId) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        self.read_merkle_range(target, &[], usize::MAX)
    }

    fn read_merkle_range(
        &self,
        target: NodeId,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        match self.ap_request(
            target,
            ApRequest::ReadMerkle {
                prefix: prefix.to_vec(),
                limit,
            },
        )? {
            ApResponse::Merkle(records) => Ok(records),
            other => Err(ReplError::Transport(format!(
                "unexpected AP merkle response {other:?}"
            ))),
        }
    }

    fn read_records_by_version_keys(
        &self,
        target: NodeId,
        version_keys: &[Bytes],
    ) -> Result<Vec<VersionedRecord>, ReplError> {
        match self.ap_request(
            target,
            ApRequest::ReadRecords {
                version_keys: version_keys.to_vec(),
            },
        )? {
            ApResponse::Records(records) => Ok(records),
            other => Err(ReplError::Transport(format!(
                "unexpected AP record response {other:?}"
            ))),
        }
    }
}

impl InternalTransportServer {
    /// Serves AP/health/flow-control frames until shutdown completes.
    ///
    /// # Errors
    /// Fails when accepting connections or setting up TLS fails.
    pub async fn serve_ap_until_shutdown<F>(
        listener: TcpListener,
        config: InternalTransportConfig,
        local_node: NodeId,
        endpoint: Arc<dyn ApReplicaEndpoint>,
        shutdown: F,
    ) -> Result<(), InternalTransportError>
    where
        F: Future<Output = ()>,
    {
        Self::serve_until_shutdown(listener, config, local_node, endpoint, None, shutdown).await
    }

    /// Serves AP, health, flow-control, Raft and cluster-admin frames until shutdown completes.
    ///
    /// # Errors
    /// Fails when accepting connections or setting up TLS fails.
    pub async fn serve_cluster_until_shutdown<F>(
        listener: TcpListener,
        config: InternalTransportConfig,
        local_node: NodeId,
        endpoint: Arc<dyn ApReplicaEndpoint>,
        raft_endpoint: Arc<dyn InternalRaftEndpoint>,
        shutdown: F,
    ) -> Result<(), InternalTransportError>
    where
        F: Future<Output = ()>,
    {
        Self::serve_until_shutdown(
            listener,
            config,
            local_node,
            endpoint,
            Some(raft_endpoint),
            shutdown,
        )
        .await
    }

    async fn serve_until_shutdown<F>(
        listener: TcpListener,
        config: InternalTransportConfig,
        local_node: NodeId,
        endpoint: Arc<dyn ApReplicaEndpoint>,
        raft_endpoint: Option<Arc<dyn InternalRaftEndpoint>>,
        shutdown: F,
    ) -> Result<(), InternalTransportError>
    where
        F: Future<Output = ()>,
    {
        let acceptor = match &config.security {
            InternalTransportSecurity::Mtls(tls) => Some(tls_acceptor(tls)?),
            #[cfg(any(test, feature = "insecure-transport"))]
            InternalTransportSecurity::PlaintextForTests => None,
        };
        let permits = Arc::new(Semaphore::new(config.max_connections.max(1)));
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                () = &mut shutdown => return Ok(()),
                accepted = listener.accept() => {
                    let (socket, peer) = accepted?;
                    let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                        observability::record_internal_transport_connection("limit");
                        continue;
                    };
                    let endpoint = Arc::clone(&endpoint);
                    let raft_endpoint = raft_endpoint.clone();
                    let config = config.clone();
                    let acceptor = acceptor.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        if let Err(error) = handle_connection(
                            socket,
                            peer,
                            config,
                            acceptor,
                            local_node,
                            endpoint,
                            raft_endpoint,
                        )
                        .await
                        {
                            tracing::warn!(error = %error, "internal transport connection failed");
                        }
                    });
                }
            }
        }
    }
}

impl Drop for FlowReservation {
    fn drop(&mut self) {
        if let Ok(mut state) = self.flow.lock()
            && let Some(inflight) = state.get_mut(&self.peer)
        {
            inflight.frames = inflight.frames.saturating_sub(1);
            inflight.bytes = inflight.bytes.saturating_sub(self.bytes);
        }
    }
}

enum MaybeTlsStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => std::pin::Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        match &mut *self {
            Self::Plain(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => std::pin::Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => std::pin::Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => std::pin::Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => std::pin::Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

async fn handle_connection(
    socket: TcpStream,
    peer: SocketAddr,
    config: InternalTransportConfig,
    acceptor: Option<TlsAcceptor>,
    local_node: NodeId,
    endpoint: Arc<dyn ApReplicaEndpoint>,
    raft_endpoint: Option<Arc<dyn InternalRaftEndpoint>>,
) -> Result<(), InternalTransportError> {
    let mut stream = match acceptor {
        Some(acceptor) => {
            let tls = tokio::time::timeout(
                Duration::from_millis(config.handshake_timeout_ms.max(1)),
                acceptor.accept(socket),
            )
            .await
            .map_err(|_| {
                InternalTransportError::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "mTLS server handshake timed out",
                ))
            })?
            .map_err(|error| InternalTransportError::Tls(error.to_string()))?;
            MaybeTlsStream::Tls(Box::new(TlsStream::Server(tls)))
        }
        None => MaybeTlsStream::Plain(socket),
    };

    let request = read_frame::<_, InternalRequest>(
        &mut stream,
        config.max_frame_bytes,
        config.idle_timeout_ms,
    )
    .await?;
    let response = handle_request(
        request,
        local_node,
        endpoint.as_ref(),
        raft_endpoint.as_deref(),
    );
    let response = response.unwrap_or_else(|error| InternalResponse::Error(error.to_string()));
    write_frame(&mut stream, config.max_frame_bytes, &response).await?;
    stream.shutdown().await?;
    observability::record_internal_transport_connection("ok");
    tracing::debug!(%peer, "internal transport request served");
    Ok(())
}

fn handle_request(
    request: InternalRequest,
    local_node: NodeId,
    endpoint: &dyn ApReplicaEndpoint,
    raft_endpoint: Option<&dyn InternalRaftEndpoint>,
) -> Result<InternalResponse, ReplError> {
    match request {
        InternalRequest::HealthPing { .. } => Ok(InternalResponse::HealthPong { node: local_node }),
        InternalRequest::FlowAck { .. } => Ok(InternalResponse::FlowAck),
        InternalRequest::Raft(request) => raft_endpoint
            .ok_or_else(|| {
                ReplError::Unsupported(
                    "Raft RPC endpoint is not mounted on this internal transport server".to_owned(),
                )
            })?
            .handle_raft(request)
            .map(InternalResponse::Raft),
        InternalRequest::ClusterAdmin(request) => raft_endpoint
            .ok_or_else(|| {
                ReplError::Unsupported(
                    "cluster admin RPC endpoint is not mounted on this internal transport server"
                        .to_owned(),
                )
            })?
            .handle_cluster_admin(request)
            .map(InternalResponse::ClusterAdmin),
        InternalRequest::Ap(request) => {
            handle_ap_request(request, endpoint).map(InternalResponse::Ap)
        }
    }
}

fn handle_ap_request(
    request: ApRequest,
    endpoint: &dyn ApReplicaEndpoint,
) -> Result<ApResponse, ReplError> {
    match request {
        ApRequest::SendBatch { writes } => {
            endpoint.receive_batch(&writes)?;
            Ok(ApResponse::Ack)
        }
        ApRequest::ReadVersions { table, key } => endpoint
            .read_versions(&table, &key)
            .map(ApResponse::Versions),
        ApRequest::ReadAllVersions => endpoint.read_all_versions().map(ApResponse::Records),
        ApRequest::ReadRecords { version_keys } => endpoint
            .read_records_by_version_keys(&version_keys)
            .map(ApResponse::Records),
        ApRequest::ReadMerkle { prefix, limit } => endpoint
            .read_merkle_range(&prefix, limit)
            .map(ApResponse::Merkle),
    }
}

async fn read_frame<S, T>(
    stream: &mut S,
    max_frame_bytes: usize,
    idle_timeout_ms: u64,
) -> Result<T, InternalTransportError>
where
    S: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    tokio::time::timeout(Duration::from_millis(idle_timeout_ms.max(1)), async {
        let len = stream.read_u32().await?;
        let len = usize::try_from(len).map_err(|error| {
            InternalTransportError::Codec(format!("invalid frame length: {error}"))
        })?;
        if len > max_frame_bytes.max(FRAME_LEN_BYTES) {
            return Err(InternalTransportError::Codec(format!(
                "frame length {len} exceeds limit {max_frame_bytes}"
            )));
        }
        let mut bytes = vec![0; len];
        stream.read_exact(&mut bytes).await?;
        postcard::from_bytes(&bytes)
            .map_err(|error| InternalTransportError::Codec(error.to_string()))
    })
    .await
    .map_err(|_| {
        InternalTransportError::Io(io::Error::new(
            io::ErrorKind::TimedOut,
            "internal transport idle timeout",
        ))
    })?
}

async fn write_frame<S, T>(
    stream: &mut S,
    max_frame_bytes: usize,
    value: &T,
) -> Result<(), InternalTransportError>
where
    S: AsyncWrite + Unpin,
    T: Serialize,
{
    let bytes = encode_frame(value, max_frame_bytes)?;
    write_encoded_frame(stream, &bytes).await
}

async fn write_encoded_frame<S>(stream: &mut S, bytes: &[u8]) -> Result<(), InternalTransportError>
where
    S: AsyncWrite + Unpin,
{
    let len = u32::try_from(bytes.len())
        .map_err(|error| InternalTransportError::Codec(error.to_string()))?;
    stream.write_u32(len).await?;
    stream.write_all(bytes).await?;
    stream.flush().await?;
    Ok(())
}

fn encode_frame<T>(value: &T, max_frame_bytes: usize) -> Result<Vec<u8>, InternalTransportError>
where
    T: Serialize,
{
    let bytes = postcard::to_allocvec(value)
        .map_err(|error| InternalTransportError::Codec(error.to_string()))?;
    if bytes.len() > max_frame_bytes.max(FRAME_LEN_BYTES) {
        return Err(InternalTransportError::Codec(format!(
            "encoded frame length {} exceeds limit {}",
            bytes.len(),
            max_frame_bytes
        )));
    }
    Ok(bytes)
}

fn tls_acceptor(config: &InternalTlsConfig) -> Result<TlsAcceptor, InternalTransportError> {
    let roots = load_root_store(&config.ca_cert_path)?;
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|error| InternalTransportError::Tls(error.to_string()))?;
    let mut config = ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|error| InternalTransportError::Tls(error.to_string()))?
        .with_client_cert_verifier(verifier)
        .with_single_cert(
            load_certs(&config.cert_path)?,
            load_private_key(&config.key_path)?,
        )
        .map_err(|error| InternalTransportError::Tls(error.to_string()))?;
    config.alpn_protocols = vec![INTERNAL_ALPN.to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn client_config(config: &InternalTlsConfig) -> Result<ClientConfig, InternalTransportError> {
    let roots = load_root_store(&config.ca_cert_path)?;
    let verifier = WebPkiServerVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|error| InternalTransportError::Tls(error.to_string()))?;
    let mut config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|error| InternalTransportError::Tls(error.to_string()))?
        .with_webpki_verifier(verifier)
        .with_client_auth_cert(
            load_certs(&config.cert_path)?,
            load_private_key(&config.key_path)?,
        )
        .map_err(|error| InternalTransportError::Tls(error.to_string()))?;
    config.alpn_protocols = vec![INTERNAL_ALPN.to_vec()];
    Ok(config)
}

fn load_root_store(path: &Path) -> Result<RootCertStore, InternalTransportError> {
    let certs = load_certs(path)?;
    let mut roots = RootCertStore::empty();
    let (valid, invalid) = roots.add_parsable_certificates(certs);
    if valid == 0 {
        return Err(InternalTransportError::Tls(format!(
            "CA file {} did not contain a parsable certificate; invalid={invalid}",
            path.display()
        )));
    }
    Ok(roots)
}

fn load_certs(
    path: &Path,
) -> Result<Vec<rustls_pki_types::CertificateDer<'static>>, InternalTransportError> {
    let bytes = std::fs::read(path)?;
    let certs = rustls_pki_types::CertificateDer::pem_slice_iter(&bytes)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| InternalTransportError::Tls(error.to_string()))?;
    if certs.is_empty() {
        return Err(InternalTransportError::Tls(format!(
            "certificate file {} is empty",
            path.display()
        )));
    }
    Ok(certs)
}

fn load_private_key(
    path: &Path,
) -> Result<rustls_pki_types::PrivateKeyDer<'static>, InternalTransportError> {
    let bytes = std::fs::read(path)?;
    rustls_pki_types::PrivateKeyDer::from_pem_slice(&bytes)
        .map_err(|error| InternalTransportError::Tls(error.to_string()))
}

fn internal_to_repl(error: &InternalTransportError) -> ReplError {
    ReplError::Transport(error.to_string())
}

#[allow(dead_code)]
fn _assert_flow_config_is_send_sync(_: &FlowControlConfig) {}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use tokio::sync::oneshot;

    use super::{
        ClusterAdminRequest, InternalRequest, InternalResponse, InternalTransportServer,
        RaftRequest, TcpApTransport, encode_frame, handle_request,
    };
    use crate::{
        phase30::{InternalTransportConfig, InternalTransportSecurity},
        repl::{
            ApTransport, RaftNode, ReplError, VectorClock, VersionedBytes, VersionedRecord,
            VersionedWrite, ap::ApReplicaEndpoint,
        },
        storage::Bytes,
    };

    #[derive(Default)]
    struct FakeApEndpoint {
        writes: Mutex<Vec<VersionedWrite>>,
    }

    impl ApReplicaEndpoint for FakeApEndpoint {
        fn receive_batch(&self, writes: &[VersionedWrite]) -> Result<(), ReplError> {
            self.writes
                .lock()
                .map_err(|_| ReplError::Transport("fake endpoint lock poisoned".to_owned()))?
                .extend_from_slice(writes);
            Ok(())
        }

        fn read_versions(
            &self,
            _table: &str,
            _key: &[u8],
        ) -> Result<Vec<VersionedBytes>, ReplError> {
            Ok(Vec::new())
        }

        fn read_all_versions(&self) -> Result<Vec<VersionedRecord>, ReplError> {
            Ok(Vec::new())
        }

        fn read_records_by_version_keys(
            &self,
            _version_keys: &[Bytes],
        ) -> Result<Vec<VersionedRecord>, ReplError> {
            Ok(Vec::new())
        }

        fn read_merkle_range(
            &self,
            _prefix: &[u8],
            _limit: usize,
        ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plaintext_loopback_ap_transport_sends_batch() -> Result<(), Box<dyn std::error::Error>>
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let endpoint = Arc::new(FakeApEndpoint::default());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let config = InternalTransportConfig::new(
            addr.to_string(),
            InternalTransportSecurity::PlaintextForTests,
        )
        .with_max_frame_bytes(4096);
        let handle = tokio::spawn(InternalTransportServer::serve_ap_until_shutdown(
            listener,
            config.clone(),
            1,
            endpoint.clone(),
            async {
                let _ = shutdown_rx.await;
            },
        ));

        let transport = TcpApTransport::new(config, BTreeMap::from([(1, addr.to_string())]))?;
        transport.send_batch(
            1,
            &[VersionedWrite {
                table: "t".to_owned(),
                key: b"k".to_vec(),
                version: VersionedBytes {
                    value: Some(b"v".to_vec()),
                    clock: VectorClock::new([(1, 1)]),
                    origin: 1,
                },
            }],
        )?;

        let writes = endpoint
            .writes
            .lock()
            .map_err(|_| ReplError::Transport("fake endpoint lock poisoned".to_owned()))?
            .clone();
        assert_eq!(writes.len(), 1);
        let _ = shutdown_tx.send(());
        handle.await??;
        Ok(())
    }

    #[test]
    fn raft_rpc_frame_roundtrips_and_enforces_frame_limit() -> Result<(), Box<dyn std::error::Error>>
    {
        let request = InternalRequest::Raft(RaftRequest::InstallSnapshot {
            from: 2,
            term: 7,
            snapshot_id: "snap-7".to_owned(),
            offset: 0,
            done: true,
            chunk: b"snapshot".to_vec(),
        });

        let bytes = encode_frame(&request, 1024)?;
        let decoded: InternalRequest = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, request);
        assert!(encode_frame(&request, 8).is_err());
        Ok(())
    }

    #[test]
    fn unmounted_raft_and_admin_rpcs_fail_closed() {
        let endpoint = FakeApEndpoint::default();
        let raft = handle_request(
            InternalRequest::Raft(RaftRequest::AppendEntries {
                from: 2,
                term: 3,
                payload: b"entries".to_vec(),
            }),
            1,
            &endpoint,
            None,
        );
        assert!(matches!(raft, Err(ReplError::Unsupported(_))));

        let admin = handle_request(
            InternalRequest::ClusterAdmin(ClusterAdminRequest::ChangeMembership {
                voters: vec![
                    RaftNode::new(1, "127.0.0.1:7001"),
                    RaftNode::new(2, "127.0.0.1:7002"),
                    RaftNode::new(3, "127.0.0.1:7003"),
                ],
                learners: Vec::new(),
            }),
            1,
            &endpoint,
            None,
        );
        assert!(matches!(admin, Err(ReplError::Unsupported(_))));

        let health =
            match handle_request(InternalRequest::HealthPing { from: 2 }, 1, &endpoint, None) {
                Ok(response) => response,
                Err(error) => panic!("health remains mounted: {error}"),
            };
        assert_eq!(health, InternalResponse::HealthPong { node: 1 });
    }
}
