use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    future::Future,
    io::{self, Cursor, Read},
    ops::{Bound, RangeBounds},
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use futures::{Stream, StreamExt};
use openraft::{
    EntryPayload, RaftLogReader, RaftNetworkFactory, RaftNetworkV2, RaftSnapshotBuilder,
    RaftTypeConfig,
    async_runtime::WatchReceiver,
    error::{RPCError, StreamingError, Unreachable},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
    storage::{EntryResponder, IOFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot},
    type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf},
};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::{
    internal_transport::{
        ClusterAdminRequest, ClusterAdminResponse, InternalRaftEndpoint, InternalRequest,
        InternalResponse, InternalTransportClient, InternalTransportServer, RaftRequest,
        RaftResponse,
    },
    repl::{
        ApTransport, CpClusterRole, CpClusterStatus, NodeId, ReplError, VersionedBytes,
        VersionedRecord, VersionedWrite, ap::ApReplicaEndpoint, condition_matches, dist_txn,
    },
    storage::{Bytes, ReadTransaction, StorageEngine, StorageError, WriteTransaction},
    txn::{self, TxnId, WriteSet},
};

use super::cp::{
    ClusterBootstrap, CpClusterConfig, CpOpenRaftTypeConfig, RAFT_LOG_TABLE, RAFT_SNAPSHOT_TABLE,
    RAFT_STATE_TABLE, RAFT_VOTE_TABLE, RaftNode, ReplApplyResponse, ReplCommand,
};
use super::{
    DIST_TXN_FINISHED_TABLE, DIST_TXN_PARTICIPANT_TABLE, FinishedTxnRecord, PreparedTxnRecord,
};

const LAST_APPLIED_KEY: &[u8] = b"last_applied";
const LAST_COMMITTED_KEY: &[u8] = b"last_committed";
const LAST_TXN_ID_KEY: &[u8] = b"last_txn_id";
const LAST_PURGED_LOG_ID_KEY: &[u8] = b"last_purged_log_id";
const LAST_APPLIED_LOG_ID_KEY: &[u8] = b"last_applied_log_id";
const COMMITTED_LOG_ID_KEY: &[u8] = b"committed_log_id";
const LAST_MEMBERSHIP_KEY: &[u8] = b"last_membership";
const CURRENT_SNAPSHOT_META_KEY: &[u8] = b"current_snapshot_meta";
const CURRENT_SNAPSHOT_DATA_KEY: &[u8] = b"current_snapshot_data";
const VOTE_KEY: &[u8] = b"vote";
const RAFT_MIRROR_TABLE: &str = "__raft_state_machine_mirror";

type CpOpenRaft<S> = openraft::Raft<CpOpenRaftTypeConfig, CpStateMachine<S>>;
type CpEntry = <CpOpenRaftTypeConfig as RaftTypeConfig>::Entry;
type CpSnapshotMeta = SnapshotMetaOf<CpOpenRaftTypeConfig>;
type CpSnapshot = SnapshotOf<CpOpenRaftTypeConfig>;

pub(super) struct LiveCpRuntime<S: StorageEngine> {
    storage: Arc<S>,
    config: CpClusterConfig,
    handle: tokio::runtime::Handle,
    runtime: Mutex<Option<tokio::runtime::Runtime>>,
    raft: CpOpenRaft<S>,
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
    server_task: Mutex<Option<JoinHandle<()>>>,
}

pub(super) struct LiveCpHandle<S: StorageEngine> {
    pub(super) storage: Arc<S>,
    pub(super) runtime: Arc<LiveCpRuntime<S>>,
}

struct CpLogStore<S: StorageEngine> {
    storage: Arc<S>,
    io_lock: Arc<Mutex<()>>,
}

pub(super) struct CpStateMachine<S: StorageEngine> {
    storage: Arc<S>,
    io_lock: Arc<Mutex<()>>,
}

pub(crate) struct CpSnapshotBuilder<S: StorageEngine> {
    storage: Arc<S>,
}

#[derive(Clone)]
struct CpNetworkFactory {
    config: crate::phase30::InternalTransportConfig,
}

struct CpNetwork {
    config: crate::phase30::InternalTransportConfig,
    target: NodeId,
    addr: String,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct CpSnapshotWire {
    vote: VoteOf<CpOpenRaftTypeConfig>,
    meta: CpSnapshotMeta,
    data: Bytes,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct StateSnapshot {
    applied: Option<LogIdOf<CpOpenRaftTypeConfig>>,
    membership: StoredMembershipOf<CpOpenRaftTypeConfig>,
    rows: Vec<(String, Bytes, Bytes)>,
}

struct NoopApEndpoint;

struct WeakCpEndpoint<S: StorageEngine> {
    runtime: Weak<LiveCpRuntime<S>>,
}

impl From<StorageError> for io::Error {
    fn from(error: StorageError) -> Self {
        to_io(error)
    }
}

impl<S: StorageEngine> Clone for CpLogStore<S> {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            io_lock: Arc::clone(&self.io_lock),
        }
    }
}

impl<S: StorageEngine> Clone for CpStateMachine<S> {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            io_lock: Arc::clone(&self.io_lock),
        }
    }
}

impl<S: StorageEngine> LiveCpRuntime<S> {
    pub(super) fn start(storage: S, config: CpClusterConfig) -> Result<LiveCpHandle<S>, ReplError> {
        let storage = Arc::new(storage);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .thread_name(format!("multidb-cp-{}", config.node_id))
            .build()
            .map_err(|error| ReplError::Transport(error.to_string()))?;
        let handle = runtime.handle().clone();
        let storage_for_runtime = Arc::clone(&storage);
        let config_for_runtime = config.clone();
        let raft = handle.block_on(async {
            Self::build_raft(Arc::clone(&storage_for_runtime), &config_for_runtime).await
        })?;

        let runtime = Arc::new(Self {
            storage: Arc::clone(&storage),
            config,
            handle,
            runtime: Mutex::new(Some(runtime)),
            raft,
            shutdown: Mutex::new(None),
            server_task: Mutex::new(None),
        });
        runtime.mount_server()?;
        runtime.bootstrap()?;

        Ok(LiveCpHandle { storage, runtime })
    }

    async fn build_raft(
        storage: Arc<S>,
        config: &CpClusterConfig,
    ) -> Result<CpOpenRaft<S>, ReplError> {
        let transport = config.transport.clone().ok_or_else(|| {
            ReplError::Transport(
                "CP live OpenRaft runtime requires InternalTransportConfig".to_owned(),
            )
        })?;
        let io_lock = Arc::new(Mutex::new(()));
        let log_store = CpLogStore {
            storage: Arc::clone(&storage),
            io_lock: Arc::clone(&io_lock),
        };
        let state_machine = CpStateMachine { storage, io_lock };
        let network = CpNetworkFactory { config: transport };

        openraft::Raft::new(
            config.node_id,
            Arc::new(openraft_config_from_cluster(config)),
            network,
            log_store,
            state_machine,
        )
        .await
        .map_err(|error| ReplError::Transport(error.to_string()))
    }

    fn mount_server(self: &Arc<Self>) -> Result<(), ReplError> {
        let Some(transport) = self.config.transport.clone() else {
            return Err(ReplError::Transport(
                "CP live OpenRaft runtime requires InternalTransportConfig".to_owned(),
            ));
        };
        let bind_addr = transport.bind_addr.clone();
        let bind_addr_for_bind = bind_addr.clone();
        let listener = self
            .block_on(async move { tokio::net::TcpListener::bind(&bind_addr_for_bind).await })
            .map_err(|error| ReplError::Transport(format!("bind {bind_addr}: {error}")))?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        *self
            .shutdown
            .lock()
            .map_err(|_| ReplError::Transport("CP shutdown lock poisoned".to_owned()))? =
            Some(shutdown_tx);

        let local_node = self.config.node_id;
        let raft_endpoint: Arc<dyn InternalRaftEndpoint> = Arc::new(WeakCpEndpoint {
            runtime: Arc::downgrade(self),
        });
        let ap_endpoint: Arc<dyn ApReplicaEndpoint> = Arc::new(NoopApEndpoint);
        let task = self.handle.spawn(async move {
            let result = InternalTransportServer::serve_cluster_until_shutdown(
                listener,
                transport,
                local_node,
                ap_endpoint,
                raft_endpoint,
                async {
                    let _ = shutdown_rx.await;
                },
            )
            .await;
            if let Err(error) = result {
                tracing::warn!(error = %error, "CP internal transport server stopped");
            }
        });
        *self
            .server_task
            .lock()
            .map_err(|_| ReplError::Transport("CP server task lock poisoned".to_owned()))? =
            Some(task);
        Ok(())
    }

    fn bootstrap(&self) -> Result<(), ReplError> {
        if self.config.bootstrap != ClusterBootstrap::Initialize {
            return Ok(());
        }

        let members = self
            .config
            .voters
            .iter()
            .map(|node| (node.id, openraft::BasicNode::new(node.addr.clone())))
            .collect::<BTreeMap<_, _>>();
        let raft = self.raft.clone();
        self.block_on(async move {
            if raft
                .is_initialized()
                .await
                .map_err(|error| ReplError::Transport(error.to_string()))?
            {
                return Ok(());
            }
            match raft.initialize(members).await {
                Ok(()) => Ok(()),
                Err(error) if error.to_string().contains("not allowed") => Ok(()),
                Err(error) => Err(ReplError::Transport(error.to_string())),
            }
        })
    }

    pub(super) fn shutdown(&self) -> Result<(), ReplError> {
        if let Some(sender) = self
            .shutdown
            .lock()
            .map_err(|_| ReplError::Transport("CP shutdown lock poisoned".to_owned()))?
            .take()
        {
            let _ = sender.send(());
        }
        if let Some(task) = self
            .server_task
            .lock()
            .map_err(|_| ReplError::Transport("CP server task lock poisoned".to_owned()))?
            .take()
        {
            task.abort();
        }
        Ok(())
    }

    pub(super) fn propose(&self, command: ReplCommand) -> Result<ReplApplyResponse, ReplError> {
        let raft = self.raft.clone();
        let timeout = self.operation_timeout();
        self.block_on(async move {
            let response = tokio::time::timeout(timeout, raft.client_write(command))
                .await
                .map_err(|_| operation_timed_out("CP Raft client_write", timeout))?
                .map_err(map_raft_error)?
                .data;
            if response.conflict {
                return Err(ReplError::Conflict);
            }
            Ok(response)
        })
    }

    pub(super) fn strong_read_barrier(&self) -> Result<(), ReplError> {
        let raft = self.raft.clone();
        let timeout = self.operation_timeout();
        self.block_on(async move {
            tokio::time::timeout(
                timeout,
                raft.ensure_linearizable(openraft::ReadPolicy::ReadIndex),
            )
            .await
            .map_err(|_| operation_timed_out("CP Raft ReadIndex", timeout))?
            .map(|_| ())
            .map_err(map_raft_error)
        })
    }

    pub(super) fn change_membership(
        &self,
        voters: &[RaftNode],
        learners: &[RaftNode],
    ) -> Result<CpClusterStatus, ReplError> {
        let raft = self.raft.clone();
        let timeout = self.operation_timeout();
        let metrics = self.raft.metrics().borrow_watched().clone();
        let existing_nodes = metrics
            .membership_config
            .membership()
            .nodes()
            .map(|(id, _)| *id)
            .collect::<BTreeSet<_>>();
        let learner_nodes = voters
            .iter()
            .chain(learners)
            .filter(|node| !existing_nodes.contains(&node.id))
            .map(|node| (node.id, openraft::BasicNode::new(node.addr.clone())))
            .collect::<Vec<_>>();
        let voter_ids = voters.iter().map(|node| node.id).collect::<BTreeSet<_>>();

        self.block_on(async move {
            tokio::time::timeout(timeout, async move {
                for (id, node) in learner_nodes {
                    raft.add_learner(id, node, true)
                        .await
                        .map_err(map_raft_error)?;
                }
                raft.change_membership(voter_ids, false)
                    .await
                    .map_err(map_raft_error)?;
                Ok::<(), ReplError>(())
            })
            .await
            .map_err(|_| operation_timed_out("CP Raft change_membership", timeout))?
        })?;
        self.status()
    }

    pub(super) fn transfer_leader(&self, target: NodeId) -> Result<CpClusterStatus, ReplError> {
        let raft = self.raft.clone();
        let timeout = self.operation_timeout();
        self.block_on(async move {
            tokio::time::timeout(timeout, raft.trigger().transfer_leader(target))
                .await
                .map_err(|_| operation_timed_out("CP Raft transfer_leader", timeout))?
                .map_err(|error| ReplError::Transport(error.to_string()))
        })?;
        self.status()
    }

    pub(super) fn status(&self) -> Result<CpClusterStatus, ReplError> {
        let metrics = self.raft.metrics().borrow_watched().clone();
        let voters = metrics
            .membership_config
            .membership()
            .voter_ids()
            .collect::<Vec<_>>();
        let learners = metrics
            .membership_config
            .membership()
            .learner_ids()
            .collect::<Vec<_>>();
        let quorum_size = voters.len() / 2 + 1;
        let nodes = self
            .config
            .voters
            .iter()
            .chain(&self.config.learners)
            .map(|node| {
                let voter = voters.contains(&node.id);
                let role = if metrics.current_leader == Some(node.id) {
                    CpClusterRole::Leader
                } else if voter {
                    CpClusterRole::Follower
                } else {
                    CpClusterRole::Learner
                };
                super::cp::CpClusterNodeStatus {
                    id: node.id,
                    addr: node.addr.clone(),
                    role,
                    voter,
                    reachable: node.id == self.config.node_id
                        || metrics
                            .replication
                            .as_ref()
                            .is_some_and(|r| r.contains_key(&node.id)),
                }
            })
            .collect::<Vec<_>>();
        let reachable_voters = metrics.replication.as_ref().map_or(
            usize::from(metrics.current_leader == Some(self.config.node_id)),
            |replication| {
                replication.keys().filter(|id| voters.contains(id)).count()
                    + usize::from(voters.contains(&self.config.node_id))
            },
        );

        Ok(CpClusterStatus {
            node_id: self.config.node_id,
            leader_id: metrics.current_leader,
            voters,
            learners,
            quorum_size,
            reachable_voters,
            last_committed: read_raft_index(self.storage.as_ref(), LAST_COMMITTED_KEY)?,
            last_applied: read_raft_index(self.storage.as_ref(), LAST_APPLIED_KEY)?,
            nodes,
        })
    }

    pub(super) fn wait_for_openraft_recovery(&self) -> Result<(), ReplError> {
        let raft = self.raft.clone();
        self.block_on(async move {
            raft.wait_for_recovery(Some(Duration::from_secs(10)))
                .await
                .map(|_| ())
                .map_err(|error| ReplError::Transport(error.to_string()))
        })
    }

    fn block_on<F, T>(&self, future: F) -> T
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let handle = self.handle.clone();
        if tokio::runtime::Handle::try_current().is_ok() {
            std::thread::spawn(move || handle.block_on(future))
                .join()
                .unwrap_or_else(|_| panic!("CP runtime helper thread panicked"))
        } else {
            handle.block_on(future)
        }
    }

    fn operation_timeout(&self) -> Duration {
        self.config.transport.as_ref().map_or_else(
            || Duration::from_secs(10),
            |transport| Duration::from_millis(transport.request_timeout_ms.max(1)),
        )
    }
}

impl<S: StorageEngine> Drop for LiveCpRuntime<S> {
    fn drop(&mut self) {
        let timeout = self.operation_timeout();
        let runtime = self.runtime.get_mut().ok().and_then(Option::take);
        let Some(runtime) = runtime else {
            return;
        };

        if tokio::runtime::Handle::try_current().is_ok() {
            let _ = std::thread::Builder::new()
                .name("multidb-cp-runtime-drop".to_owned())
                .spawn(move || runtime.shutdown_timeout(timeout));
        } else {
            runtime.shutdown_timeout(timeout);
        }
    }
}

impl<S: StorageEngine> InternalRaftEndpoint for WeakCpEndpoint<S> {
    fn handle_raft(&self, request: RaftRequest) -> Result<RaftResponse, ReplError> {
        self.runtime
            .upgrade()
            .ok_or_else(|| ReplError::Transport("CP Raft runtime is shutting down".to_owned()))?
            .handle_raft(request)
    }

    fn handle_cluster_admin(
        &self,
        request: ClusterAdminRequest,
    ) -> Result<ClusterAdminResponse, ReplError> {
        self.runtime
            .upgrade()
            .ok_or_else(|| ReplError::Transport("CP Raft runtime is shutting down".to_owned()))?
            .handle_cluster_admin(request)
    }
}

impl<S: StorageEngine> InternalRaftEndpoint for LiveCpRuntime<S> {
    fn handle_raft(&self, request: RaftRequest) -> Result<RaftResponse, ReplError> {
        match request {
            RaftRequest::AppendEntries { payload, .. } => {
                let rpc: AppendEntriesRequest<CpOpenRaftTypeConfig> = decode(&payload)?;
                let raft = self.raft.clone();
                let response = self.block_on(async move {
                    raft.append_entries(rpc)
                        .await
                        .map_err(|error| ReplError::Transport(error.to_string()))
                })?;
                Ok(RaftResponse::Accepted {
                    term: 0,
                    payload: encode(&response)?,
                })
            }
            RaftRequest::Vote { payload, .. } => {
                let rpc: VoteRequest<CpOpenRaftTypeConfig> = decode(&payload)?;
                let raft = self.raft.clone();
                let response = self.block_on(async move {
                    raft.vote(rpc)
                        .await
                        .map_err(|error| ReplError::Transport(error.to_string()))
                })?;
                Ok(RaftResponse::Accepted {
                    term: 0,
                    payload: encode(&response)?,
                })
            }
            RaftRequest::PreVote { payload, .. } => {
                let rpc: VoteRequest<CpOpenRaftTypeConfig> = decode(&payload)?;
                let raft = self.raft.clone();
                let response = self.block_on(async move {
                    raft.pre_vote(rpc)
                        .await
                        .map_err(|error| ReplError::Transport(error.to_string()))
                })?;
                Ok(RaftResponse::Accepted {
                    term: 0,
                    payload: encode(&response)?,
                })
            }
            RaftRequest::InstallSnapshot { chunk, .. } => {
                let wire: CpSnapshotWire = decode(&chunk)?;
                let snapshot = Snapshot {
                    meta: wire.meta,
                    snapshot: Cursor::new(wire.data),
                };
                let raft = self.raft.clone();
                let response = self.block_on(async move {
                    raft.install_full_snapshot(wire.vote, snapshot)
                        .await
                        .map_err(|error| ReplError::Transport(error.to_string()))
                })?;
                Ok(RaftResponse::Accepted {
                    term: 0,
                    payload: encode(&response)?,
                })
            }
        }
    }

    fn handle_cluster_admin(
        &self,
        request: ClusterAdminRequest,
    ) -> Result<ClusterAdminResponse, ReplError> {
        match request {
            ClusterAdminRequest::ChangeMembership { voters, learners } => {
                let _ = self.change_membership(&voters, &learners)?;
                Ok(ClusterAdminResponse::Accepted)
            }
            ClusterAdminRequest::TransferLeader { target } => {
                let _ = self.transfer_leader(target)?;
                Ok(ClusterAdminResponse::Accepted)
            }
            ClusterAdminRequest::Status => self.status().map(ClusterAdminResponse::Status),
        }
    }
}

impl<S: StorageEngine> RaftLogStorage<CpOpenRaftTypeConfig> for CpLogStore<S> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<CpOpenRaftTypeConfig>, io::Error> {
        let read = self.storage.begin_read().map_err(to_io)?;
        let last_purged_log_id = read_log_id(&read, LAST_PURGED_LOG_ID_KEY)?;
        let last_log_id = read_last_log_id(&read)?.or(last_purged_log_id);
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<CpOpenRaftTypeConfig>) -> Result<(), io::Error> {
        let _guard = lock_io(&self.io_lock)?;
        let mut write = self.storage.begin_write().map_err(to_io)?;
        write.put(RAFT_VOTE_TABLE, VOTE_KEY, &encode_io(vote)?)?;
        write.commit().map_err(to_io)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<CpOpenRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let _guard = lock_io(&self.io_lock)?;
        let mut write = self.storage.begin_write().map_err(to_io)?;
        write_optional_log_id(&mut write, COMMITTED_LOG_ID_KEY, committed.as_ref())?;
        if let Some(log_id) = committed {
            write
                .put(
                    RAFT_STATE_TABLE,
                    LAST_COMMITTED_KEY,
                    &log_id.index.to_be_bytes(),
                )
                .map_err(to_io)?;
        }
        write.commit().map_err(to_io)
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<CpOpenRaftTypeConfig>>, io::Error> {
        let read = self.storage.begin_read().map_err(to_io)?;
        read_log_id(&read, COMMITTED_LOG_ID_KEY)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<CpOpenRaftTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = CpEntry> + Send,
        I::IntoIter: Send,
    {
        let _guard = lock_io(&self.io_lock)?;
        let mut write = self.storage.begin_write().map_err(to_io)?;
        for entry in entries {
            write.put(
                RAFT_LOG_TABLE,
                &entry.log_id.index.to_be_bytes(),
                &encode_io(&entry)?,
            )?;
        }
        write.commit().map_err(to_io)?;
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<CpOpenRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let _guard = lock_io(&self.io_lock)?;
        let mut write = self.storage.begin_write().map_err(to_io)?;
        let start = last_log_id
            .as_ref()
            .map_or(0, |log_id| log_id.index.saturating_add(1));
        let keys = write
            .range(RAFT_LOG_TABLE, &start.to_be_bytes(), &[])?
            .map(|entry| entry.map(|(key, _)| key))
            .collect::<Result<Vec<_>, _>>()?;
        for key in keys {
            write.delete(RAFT_LOG_TABLE, &key)?;
        }
        write.commit().map_err(to_io)
    }

    async fn purge(&mut self, log_id: LogIdOf<CpOpenRaftTypeConfig>) -> Result<(), io::Error> {
        let _guard = lock_io(&self.io_lock)?;
        let mut write = self.storage.begin_write().map_err(to_io)?;
        let end = log_id.index.saturating_add(1).to_be_bytes();
        let keys = write
            .range(RAFT_LOG_TABLE, &[], &end)?
            .map(|entry| entry.map(|(key, _)| key))
            .collect::<Result<Vec<_>, _>>()?;
        for key in keys {
            write.delete(RAFT_LOG_TABLE, &key)?;
        }
        write_optional_log_id(&mut write, LAST_PURGED_LOG_ID_KEY, Some(&log_id))?;
        write.commit().map_err(to_io)
    }
}

impl<S: StorageEngine> RaftLogReader<CpOpenRaftTypeConfig> for CpLogStore<S> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<CpEntry>, io::Error> {
        let read = self.storage.begin_read().map_err(to_io)?;
        let (start, end) = range_to_bounds(range);
        let start_key = start.unwrap_or_default().to_be_bytes();
        let end_key = end.map(u64::to_be_bytes).unwrap_or_default();
        read.range(RAFT_LOG_TABLE, &start_key, &end_key)?
            .filter_map(|entry| match entry {
                Ok((key, bytes)) => {
                    let index = decode_index_key(&key).ok()?;
                    if start.is_some_and(|start| index < start)
                        || end.is_some_and(|end| index >= end)
                    {
                        return None;
                    }
                    Some(decode_io::<CpEntry>(&bytes))
                }
                Err(error) => Some(Err(to_io(error))),
            })
            .collect()
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<CpOpenRaftTypeConfig>>, io::Error> {
        let read = self.storage.begin_read().map_err(to_io)?;
        read.get(RAFT_VOTE_TABLE, VOTE_KEY)?
            .map(|bytes| decode_io(&bytes))
            .transpose()
    }
}

impl<S: StorageEngine> RaftStateMachine<CpOpenRaftTypeConfig> for CpStateMachine<S> {
    type SnapshotBuilder = CpSnapshotBuilder<S>;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogIdOf<CpOpenRaftTypeConfig>>,
            StoredMembershipOf<CpOpenRaftTypeConfig>,
        ),
        io::Error,
    > {
        let read = self.storage.begin_read().map_err(to_io)?;
        let applied = read_log_id(&read, LAST_APPLIED_LOG_ID_KEY)?;
        let membership: StoredMembershipOf<CpOpenRaftTypeConfig> = read
            .get(RAFT_STATE_TABLE, LAST_MEMBERSHIP_KEY)?
            .map(|bytes| decode_io(&bytes))
            .transpose()?
            .unwrap_or_default();
        Ok((applied, membership))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<CpOpenRaftTypeConfig>, io::Error>> + Unpin + Send,
    {
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            let response = self.apply_one(entry).map_err(to_io)?;
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    async fn try_create_snapshot_builder(&mut self, _force: bool) -> Option<Self::SnapshotBuilder> {
        Some(CpSnapshotBuilder {
            storage: Arc::clone(&self.storage),
        })
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        CpSnapshotBuilder {
            storage: Arc::clone(&self.storage),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &CpSnapshotMeta,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let data = snapshot.into_inner();
        let decoded: StateSnapshot = decode_io(&data)?;
        let _guard = lock_io(&self.io_lock)?;
        let mut write = self.storage.begin_write().map_err(to_io)?;
        clear_mirrored_state(&mut write)?;
        for (table, key, value) in &decoded.rows {
            write.put(table, key, value)?;
            write.put(RAFT_MIRROR_TABLE, &mirror_key(table, key), value)?;
        }
        write_optional_log_id(
            &mut write,
            LAST_APPLIED_LOG_ID_KEY,
            decoded.applied.as_ref(),
        )?;
        if let Some(log_id) = &decoded.applied {
            write.put(
                RAFT_STATE_TABLE,
                LAST_APPLIED_KEY,
                &log_id.index.to_be_bytes(),
            )?;
        }
        write.put(
            RAFT_STATE_TABLE,
            LAST_MEMBERSHIP_KEY,
            &encode_io(&decoded.membership)?,
        )?;
        write.put(
            RAFT_SNAPSHOT_TABLE,
            CURRENT_SNAPSHOT_META_KEY,
            &encode_io(meta)?,
        )?;
        write.put(RAFT_SNAPSHOT_TABLE, CURRENT_SNAPSHOT_DATA_KEY, &data)?;
        write.commit().map_err(to_io)
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<CpSnapshot>, io::Error> {
        let read = self.storage.begin_read().map_err(to_io)?;
        let Some(meta) = read.get(RAFT_SNAPSHOT_TABLE, CURRENT_SNAPSHOT_META_KEY)? else {
            return Ok(None);
        };
        let data = read
            .get(RAFT_SNAPSHOT_TABLE, CURRENT_SNAPSHOT_DATA_KEY)?
            .unwrap_or_default();
        Ok(Some(Snapshot {
            meta: decode_io(&meta)?,
            snapshot: Cursor::new(data),
        }))
    }
}

impl<S: StorageEngine> CpStateMachine<S> {
    fn apply_one(&self, entry: CpEntry) -> Result<ReplApplyResponse, StorageError> {
        let _guard = self
            .io_lock
            .lock()
            .map_err(|_| StorageError::Backend("CP state machine lock poisoned".to_owned()))?;
        match entry.payload {
            EntryPayload::Blank => self.apply_metadata_only(&entry.log_id, None),
            EntryPayload::Membership(membership) => {
                let stored_membership =
                    StoredMembershipOf::<CpOpenRaftTypeConfig>::new(Some(entry.log_id), membership);
                self.apply_metadata_only(&entry.log_id, Some(&stored_membership))
            }
            EntryPayload::Normal(command) => self.apply_command(&entry.log_id, &command),
        }
    }

    fn apply_metadata_only(
        &self,
        log_id: &LogIdOf<CpOpenRaftTypeConfig>,
        membership: Option<&StoredMembershipOf<CpOpenRaftTypeConfig>>,
    ) -> Result<ReplApplyResponse, StorageError> {
        let mut write = self.storage.begin_write()?;
        let txn_id = txn::current_txn_id_from(&write)?;
        write_apply_metadata(&mut write, log_id, membership, txn_id)?;
        write.commit()?;
        Ok(ReplApplyResponse {
            txn_id,
            conflict: false,
        })
    }

    fn apply_command(
        &self,
        log_id: &LogIdOf<CpOpenRaftTypeConfig>,
        command: &ReplCommand,
    ) -> Result<ReplApplyResponse, StorageError> {
        let write_set = write_set_for_command(command)?;
        let mut write = self.storage.begin_write()?;
        if let ReplCommand::DistTxnPrepare { txn_id, ops, .. } = command {
            let key = dist_txn::txn_key(*txn_id);
            if write.get(DIST_TXN_FINISHED_TABLE, &key)?.is_some() {
                let txn_id = txn::current_txn_id_from(&write)?;
                write_apply_metadata(&mut write, log_id, None, txn_id)?;
                write.commit()?;
                return Ok(ReplApplyResponse {
                    txn_id,
                    conflict: false,
                });
            }
            if let Some(existing) = write.get(DIST_TXN_PARTICIPANT_TABLE, &key)? {
                let existing = dist_txn::decode_prepared(&existing)?;
                let txn_id = txn::current_txn_id_from(&write)?;
                write_apply_metadata(&mut write, log_id, None, txn_id)?;
                write.commit()?;
                return Ok(ReplApplyResponse {
                    txn_id,
                    conflict: existing.ops != *ops,
                });
            }
        }
        if let ReplCommand::Conditional { conditions, .. } = command {
            for condition in conditions {
                if !condition_matches(&write, condition)? {
                    let txn_id = txn::current_txn_id_from(&write)?;
                    write_apply_metadata(&mut write, log_id, None, txn_id)?;
                    write.commit()?;
                    return Ok(ReplApplyResponse {
                        txn_id,
                        conflict: true,
                    });
                }
            }
        }

        let snapshot_id = match command {
            ReplCommand::TxnCommit { snapshot_id, .. } => *snapshot_id,
            _ => txn::current_txn_id_from(&write)?,
        };
        let mirror = write_set.clone();
        match txn::commit_write_set_in_txn_with_extra_authorized(
            write,
            snapshot_id,
            write_set,
            |_| Ok(()),
            |txn, next_txn_id| {
                apply_mirror(txn, mirror)?;
                write_apply_metadata(txn, log_id, None, next_txn_id)
            },
            txn::system_write_authorization(),
        ) {
            Ok(txn_id) => Ok(ReplApplyResponse {
                txn_id,
                conflict: false,
            }),
            Err(StorageError::Conflict) => {
                let mut write = self.storage.begin_write()?;
                let txn_id = txn::current_txn_id_from(&write)?;
                write_apply_metadata(&mut write, log_id, None, txn_id)?;
                write.commit()?;
                Ok(ReplApplyResponse {
                    txn_id,
                    conflict: true,
                })
            }
            Err(error) => Err(error),
        }
    }
}

impl<S: StorageEngine> RaftSnapshotBuilder<CpOpenRaftTypeConfig> for CpSnapshotBuilder<S> {
    async fn build_snapshot(&mut self) -> Result<CpSnapshot, io::Error> {
        let read = self.storage.begin_read().map_err(to_io)?;
        let applied = read_log_id(&read, LAST_APPLIED_LOG_ID_KEY)?;
        let membership: StoredMembershipOf<CpOpenRaftTypeConfig> = read
            .get(RAFT_STATE_TABLE, LAST_MEMBERSHIP_KEY)?
            .map(|bytes| decode_io(&bytes))
            .transpose()?
            .unwrap_or_default();
        let rows = read
            .range(RAFT_MIRROR_TABLE, &[], &[])?
            .map(|entry| {
                let (key, value) = entry?;
                let (table, user_key) = decode_mirror_key(&key)?;
                Ok((table, user_key, value))
            })
            .collect::<Result<Vec<_>, StorageError>>()
            .map_err(to_io)?;
        let snapshot = StateSnapshot {
            applied,
            membership: membership.clone(),
            rows,
        };
        let data = encode_io(&snapshot)?;
        let meta = CpSnapshotMeta {
            last_log_id: applied,
            last_membership: membership,
            snapshot_id: format!("cp-{}-{}", std::process::id(), data.len()),
        };

        let mut write = self.storage.begin_write().map_err(to_io)?;
        write.put(
            RAFT_SNAPSHOT_TABLE,
            CURRENT_SNAPSHOT_META_KEY,
            &encode_io(&meta)?,
        )?;
        write.put(RAFT_SNAPSHOT_TABLE, CURRENT_SNAPSHOT_DATA_KEY, &data)?;
        write.commit().map_err(to_io)?;

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        })
    }
}

impl RaftNetworkFactory<CpOpenRaftTypeConfig> for CpNetworkFactory {
    type Network = CpNetwork;

    async fn new_client(&mut self, target: NodeId, node: &openraft::BasicNode) -> Self::Network {
        CpNetwork {
            config: self.config.clone(),
            target,
            addr: node.addr.clone(),
        }
    }
}

impl RaftNetworkV2<CpOpenRaftTypeConfig> for CpNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<CpOpenRaftTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<AppendEntriesResponse<CpOpenRaftTypeConfig>, RPCError<CpOpenRaftTypeConfig>> {
        self.raft_rpc(RaftRequest::AppendEntries {
            from: 0,
            term: 0,
            payload: encode_rpc(&rpc)?,
        })
        .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<CpOpenRaftTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<VoteResponse<CpOpenRaftTypeConfig>, RPCError<CpOpenRaftTypeConfig>> {
        self.raft_rpc(RaftRequest::Vote {
            from: 0,
            term: 0,
            payload: encode_rpc(&rpc)?,
        })
        .await
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<CpOpenRaftTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<VoteResponse<CpOpenRaftTypeConfig>, RPCError<CpOpenRaftTypeConfig>> {
        self.raft_rpc(RaftRequest::PreVote {
            from: 0,
            term: 0,
            payload: encode_rpc(&rpc)?,
        })
        .await
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<CpOpenRaftTypeConfig>,
        snapshot: CpSnapshot,
        _cancel: impl Future<Output = openraft::error::ReplicationClosed> + Send + 'static,
        _option: openraft::network::RPCOption,
    ) -> Result<SnapshotResponse<CpOpenRaftTypeConfig>, StreamingError<CpOpenRaftTypeConfig>> {
        let mut cursor = snapshot.snapshot;
        cursor.set_position(0);
        let mut data = Vec::new();
        cursor
            .read_to_end(&mut data)
            .map_err(|error| StreamingError::Unreachable(Unreachable::from_string(error)))?;
        let wire = CpSnapshotWire {
            vote,
            meta: snapshot.meta,
            data,
        };
        self.raft_rpc(RaftRequest::InstallSnapshot {
            from: 0,
            term: 0,
            snapshot_id: wire.meta.snapshot_id.clone(),
            offset: 0,
            done: true,
            chunk: encode_rpc(&wire).map_err(StreamingError::from)?,
        })
        .await
        .map_err(StreamingError::from)
    }
}

impl CpNetwork {
    async fn raft_rpc<T>(&self, request: RaftRequest) -> Result<T, RPCError<CpOpenRaftTypeConfig>>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        let client = InternalTransportClient::new(
            self.config.clone(),
            BTreeMap::from([(self.target, self.addr.clone())]),
        )
        .map_err(rpc_unreachable)?;
        match client
            .request_async(self.target, InternalRequest::Raft(request))
            .await
            .map_err(rpc_unreachable)?
        {
            InternalResponse::Raft(RaftResponse::Accepted { payload, .. }) => decode_rpc(&payload),
            InternalResponse::Raft(RaftResponse::Rejected { reason, .. }) => {
                Err(rpc_unreachable(reason))
            }
            InternalResponse::Error(error) => Err(rpc_unreachable(error)),
            other => Err(rpc_unreachable(format!(
                "unexpected internal Raft response {other:?}"
            ))),
        }
    }
}

impl ApReplicaEndpoint for NoopApEndpoint {
    fn receive_batch(&self, _writes: &[VersionedWrite]) -> Result<(), ReplError> {
        Err(ReplError::Unsupported(
            "AP endpoint is not mounted on this CP transport server".to_owned(),
        ))
    }

    fn read_versions(&self, _table: &str, _key: &[u8]) -> Result<Vec<VersionedBytes>, ReplError> {
        Err(ReplError::Unsupported(
            "AP endpoint is not mounted on this CP transport server".to_owned(),
        ))
    }

    fn read_all_versions(&self) -> Result<Vec<VersionedRecord>, ReplError> {
        Err(ReplError::Unsupported(
            "AP endpoint is not mounted on this CP transport server".to_owned(),
        ))
    }

    fn read_records_by_version_keys(
        &self,
        _version_keys: &[Bytes],
    ) -> Result<Vec<VersionedRecord>, ReplError> {
        Err(ReplError::Unsupported(
            "AP endpoint is not mounted on this CP transport server".to_owned(),
        ))
    }

    fn read_merkle_range(
        &self,
        _prefix: &[u8],
        _limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        Err(ReplError::Unsupported(
            "AP endpoint is not mounted on this CP transport server".to_owned(),
        ))
    }
}

impl ApTransport for NoopApEndpoint {
    fn send_batch(&self, _target: NodeId, _writes: &[VersionedWrite]) -> Result<(), ReplError> {
        Err(ReplError::Unsupported(
            "AP endpoint is not mounted on this CP transport server".to_owned(),
        ))
    }

    fn read_versions(
        &self,
        _target: NodeId,
        _table: &str,
        _key: &[u8],
    ) -> Result<Vec<VersionedBytes>, ReplError> {
        Err(ReplError::Unsupported(
            "AP endpoint is not mounted on this CP transport server".to_owned(),
        ))
    }

    fn read_all_versions(&self, _target: NodeId) -> Result<Vec<VersionedRecord>, ReplError> {
        Err(ReplError::Unsupported(
            "AP endpoint is not mounted on this CP transport server".to_owned(),
        ))
    }

    fn read_merkle_range(
        &self,
        _target: NodeId,
        _prefix: &[u8],
        _limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        Err(ReplError::Unsupported(
            "AP endpoint is not mounted on this CP transport server".to_owned(),
        ))
    }

    fn read_records_by_version_keys(
        &self,
        _target: NodeId,
        _version_keys: &[Bytes],
    ) -> Result<Vec<VersionedRecord>, ReplError> {
        Err(ReplError::Unsupported(
            "AP endpoint is not mounted on this CP transport server".to_owned(),
        ))
    }
}

fn openraft_config_from_cluster(cluster: &CpClusterConfig) -> openraft::Config {
    let runtime = &cluster.runtime;
    openraft::Config {
        cluster_name: format!("multidb-cp-{}", cluster.node_id),
        election_timeout_min: runtime.election_timeout_min_ms,
        election_timeout_max: runtime.election_timeout_max_ms,
        heartbeat_interval: runtime.heartbeat_interval_ms,
        install_snapshot_timeout: runtime.install_snapshot_timeout_ms,
        max_payload_entries: runtime.max_payload_entries,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(runtime.snapshot_threshold),
        ..openraft::Config::default()
    }
}

fn write_set_for_command(command: &ReplCommand) -> Result<WriteSet, StorageError> {
    Ok(match command {
        ReplCommand::Batch(ops) | ReplCommand::Conditional { ops, .. } => {
            txn::ops_to_write_set(ops.clone())
        }
        ReplCommand::TxnCommit { writes, .. } => writes
            .iter()
            .cloned()
            .map(|(table, key, value)| ((table, key), value))
            .collect(),
        ReplCommand::DistTxnPrepare {
            txn_id,
            ops,
            deadline_ms,
            ..
        } => {
            let record = PreparedTxnRecord::new(*txn_id, ops.clone(), *deadline_ms);
            BTreeMap::from([(
                (
                    DIST_TXN_PARTICIPANT_TABLE.to_owned(),
                    dist_txn::txn_key(*txn_id).to_vec(),
                ),
                Some(dist_txn::encode_prepared(&record)?),
            )])
        }
        ReplCommand::DistTxnFinish {
            txn_id, decision, ..
        } => BTreeMap::from([
            (
                (
                    DIST_TXN_FINISHED_TABLE.to_owned(),
                    dist_txn::txn_key(*txn_id).to_vec(),
                ),
                Some(dist_txn::encode_finished(&FinishedTxnRecord::new(
                    *txn_id, *decision,
                ))?),
            ),
            (
                (
                    DIST_TXN_PARTICIPANT_TABLE.to_owned(),
                    dist_txn::txn_key(*txn_id).to_vec(),
                ),
                None,
            ),
        ]),
    })
}

fn apply_mirror<T: WriteTransaction>(txn: &mut T, write_set: WriteSet) -> Result<(), StorageError> {
    for ((table, key), value) in write_set {
        let mirror = mirror_key(&table, &key);
        match value {
            Some(bytes) => txn.put(RAFT_MIRROR_TABLE, &mirror, &bytes)?,
            None => txn.delete(RAFT_MIRROR_TABLE, &mirror)?,
        }
    }
    Ok(())
}

fn write_apply_metadata<T: WriteTransaction>(
    txn: &mut T,
    log_id: &LogIdOf<CpOpenRaftTypeConfig>,
    membership: Option<&StoredMembershipOf<CpOpenRaftTypeConfig>>,
    txn_id: TxnId,
) -> Result<(), StorageError> {
    write_optional_log_id(txn, LAST_APPLIED_LOG_ID_KEY, Some(log_id))?;
    txn.put(
        RAFT_STATE_TABLE,
        LAST_APPLIED_KEY,
        &log_id.index.to_be_bytes(),
    )?;
    txn.put(
        RAFT_STATE_TABLE,
        LAST_COMMITTED_KEY,
        &log_id.index.to_be_bytes(),
    )?;
    txn.put(RAFT_STATE_TABLE, LAST_TXN_ID_KEY, &txn_id.to_be_bytes())?;
    if let Some(membership) = membership {
        txn.put(
            RAFT_STATE_TABLE,
            LAST_MEMBERSHIP_KEY,
            &encode_io(membership)?,
        )?;
    }
    Ok(())
}

fn clear_mirrored_state<T: WriteTransaction>(txn: &mut T) -> Result<(), StorageError> {
    let rows = txn
        .range(RAFT_MIRROR_TABLE, &[], &[])?
        .collect::<Result<Vec<_>, _>>()?;
    for (mirror, _) in rows {
        let (table, key) = decode_mirror_key(&mirror)?;
        txn.delete(&table, &key)?;
        txn.delete(RAFT_MIRROR_TABLE, &mirror)?;
    }
    Ok(())
}

fn mirror_key(table: &str, key: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(4 + table.len() + key.len());
    let len = u32::try_from(table.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(table.as_bytes());
    out.extend_from_slice(key);
    out
}

fn decode_mirror_key(bytes: &[u8]) -> Result<(String, Bytes), StorageError> {
    if bytes.len() < 4 {
        return Err(StorageError::Corruption(
            "mirror key must contain table length".to_owned(),
        ));
    }
    let len = u32::from_be_bytes(bytes[..4].try_into().map_err(|_| {
        StorageError::Corruption("mirror key table length must be 4 bytes".to_owned())
    })?) as usize;
    if bytes.len() < 4 + len {
        return Err(StorageError::Corruption(
            "mirror key table bytes are truncated".to_owned(),
        ));
    }
    let table = std::str::from_utf8(&bytes[4..4 + len])
        .map_err(|error| StorageError::Corruption(error.to_string()))?
        .to_owned();
    Ok((table, bytes[4 + len..].to_vec()))
}

fn read_last_log_id(
    read: &impl ReadTransaction,
) -> Result<Option<LogIdOf<CpOpenRaftTypeConfig>>, io::Error> {
    let entries = read
        .range(RAFT_LOG_TABLE, &[], &[])?
        .collect::<Result<Vec<_>, _>>()?;
    let Some((_, bytes)) = entries.last() else {
        return Ok(None);
    };
    let entry: CpEntry = decode_io(bytes)?;
    Ok(Some(entry.log_id))
}

fn read_log_id(
    read: &impl ReadTransaction,
    key: &[u8],
) -> Result<Option<LogIdOf<CpOpenRaftTypeConfig>>, io::Error> {
    read.get(RAFT_STATE_TABLE, key)?
        .map(|bytes| decode_io(&bytes))
        .transpose()
}

fn write_optional_log_id<T: WriteTransaction>(
    txn: &mut T,
    key: &[u8],
    log_id: Option<&LogIdOf<CpOpenRaftTypeConfig>>,
) -> Result<(), StorageError> {
    if let Some(log_id) = log_id {
        txn.put(RAFT_STATE_TABLE, key, &encode(log_id)?)?;
    } else {
        txn.delete(RAFT_STATE_TABLE, key)?;
    }
    Ok(())
}

fn read_raft_index(storage: &impl StorageEngine, key: &[u8]) -> Result<u64, StorageError> {
    let read = storage.begin_read()?;
    read.get(RAFT_STATE_TABLE, key)?
        .map(|bytes| decode_u64(&bytes))
        .transpose()
        .map(Option::unwrap_or_default)
}

fn decode_u64(bytes: &[u8]) -> Result<u64, StorageError> {
    let bytes = bytes
        .try_into()
        .map_err(|_| StorageError::Corruption("u64 metadata must be 8 bytes".to_owned()))?;
    Ok(u64::from_be_bytes(bytes))
}

fn range_to_bounds<RB: RangeBounds<u64>>(range: RB) -> (Option<u64>, Option<u64>) {
    let start = match range.start_bound() {
        Bound::Included(value) => Some(*value),
        Bound::Excluded(value) => value.checked_add(1),
        Bound::Unbounded => None,
    };
    let end = match range.end_bound() {
        Bound::Included(value) => value.checked_add(1),
        Bound::Excluded(value) => Some(*value),
        Bound::Unbounded => None,
    };
    (start, end)
}

fn decode_index_key(key: &[u8]) -> Result<u64, StorageError> {
    let bytes = key
        .try_into()
        .map_err(|_| StorageError::Corruption("raft log key must be 8 bytes".to_owned()))?;
    Ok(u64::from_be_bytes(bytes))
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Bytes, StorageError> {
    serde_json::to_vec(value).map_err(|error| StorageError::Backend(error.to_string()))
}

fn decode<T: for<'de> serde::Deserialize<'de>>(bytes: &[u8]) -> Result<T, ReplError> {
    serde_json::from_slice(bytes).map_err(|error| ReplError::Transport(error.to_string()))
}

fn encode_io<T: serde::Serialize>(value: &T) -> Result<Bytes, io::Error> {
    serde_json::to_vec(value).map_err(to_io)
}

fn decode_io<T: for<'de> serde::Deserialize<'de>>(bytes: &[u8]) -> Result<T, io::Error> {
    serde_json::from_slice(bytes).map_err(to_io)
}

fn encode_rpc<T: serde::Serialize>(value: &T) -> Result<Bytes, RPCError<CpOpenRaftTypeConfig>> {
    serde_json::to_vec(value).map_err(rpc_unreachable)
}

fn decode_rpc<T: for<'de> serde::Deserialize<'de>>(
    bytes: &[u8],
) -> Result<T, RPCError<CpOpenRaftTypeConfig>> {
    serde_json::from_slice(bytes).map_err(rpc_unreachable)
}

fn to_io(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

fn rpc_unreachable(error: impl std::fmt::Display) -> RPCError<CpOpenRaftTypeConfig> {
    RPCError::Unreachable(Unreachable::from_string(error.to_string()))
}

fn map_raft_error(error: impl std::fmt::Display) -> ReplError {
    let message = error.to_string();
    if message.contains("QuorumNotEnough") || message.contains("quorum") {
        ReplError::NoQuorum
    } else {
        ReplError::Transport(message)
    }
}

fn operation_timed_out(operation: &str, timeout: Duration) -> ReplError {
    ReplError::Transport(format!(
        "{operation} timed out after {} ms",
        timeout.as_millis()
    ))
}

fn lock_io(lock: &Mutex<()>) -> Result<std::sync::MutexGuard<'_, ()>, io::Error> {
    lock.lock()
        .map_err(|_| io::Error::other("CP live storage lock poisoned"))
}
