use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    marker::PhantomData,
    sync::{Arc, Mutex, MutexGuard},
};

use crate::{
    phase30::{
        DistTxnConfig, FlowControlConfig, HlcConfig, HlcTimestamp, InternalTransportConfig,
        RaftRuntimeConfig, RegionConfig,
    },
    storage::{Bytes, ReadTransaction, StorageEngine, StorageError, WriteTransaction},
    txn::{self, TxnId, WriteSet},
};

use super::{
    ConditionalBatch, DIST_TXN_COORDINATOR_TABLE, DIST_TXN_FINISHED_TABLE,
    DIST_TXN_PARTICIPANT_TABLE, Decision, DistTxnId, FinishedTxnRecord, Op, PreparedTxnRecord,
    ReadConsistency, ReplError, Replication, Vote, condition_matches,
    cp_live::{LiveCpHandle, LiveCpRuntime},
    dist_txn,
};

pub type NodeId = u64;

pub const RAFT_LOG_TABLE: &str = "__raft_log";
pub const RAFT_STATE_TABLE: &str = "__raft_state";
pub const RAFT_VOTE_TABLE: &str = "__raft_vote";
pub const RAFT_SNAPSHOT_TABLE: &str = "__raft_snapshot";

const LAST_APPLIED_KEY: &[u8] = b"last_applied";
const LAST_COMMITTED_KEY: &[u8] = b"last_committed";
const LAST_TXN_ID_KEY: &[u8] = b"last_txn_id";

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RaftNode {
    pub id: NodeId,
    pub addr: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ClusterBootstrap {
    Initialize,
    JoinExisting,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CpClusterConfig {
    pub node_id: NodeId,
    pub bind_addr: String,
    pub voters: Vec<RaftNode>,
    pub learners: Vec<RaftNode>,
    pub bootstrap: ClusterBootstrap,
    pub transport: Option<InternalTransportConfig>,
    pub runtime: RaftRuntimeConfig,
    pub dist_txn: DistTxnConfig,
    pub hlc: HlcConfig,
    pub region: Option<RegionConfig>,
    pub flow_control: FlowControlConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ReplCommand {
    Batch(Vec<Op>),
    Conditional {
        conditions: Vec<super::WriteCondition>,
        ops: Vec<Op>,
        hlc: Option<HlcTimestamp>,
    },
    TxnCommit {
        snapshot_id: TxnId,
        writes: Vec<(String, Bytes, Option<Bytes>)>,
        #[serde(default)]
        hlc: Option<HlcTimestamp>,
    },
    DistTxnPrepare {
        txn_id: DistTxnId,
        ops: Vec<Op>,
        deadline_ms: u64,
        hlc: Option<HlcTimestamp>,
    },
    DistTxnFinish {
        txn_id: DistTxnId,
        decision: Decision,
        hlc: Option<HlcTimestamp>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ReplApplyResponse {
    pub txn_id: TxnId,
    #[serde(default)]
    pub conflict: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum CpClusterRole {
    Leader,
    Follower,
    Learner,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CpClusterNodeStatus {
    pub id: NodeId,
    pub addr: String,
    pub role: CpClusterRole,
    pub voter: bool,
    pub reachable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CpClusterStatus {
    pub node_id: NodeId,
    pub leader_id: Option<NodeId>,
    pub voters: Vec<NodeId>,
    pub learners: Vec<NodeId>,
    pub quorum_size: usize,
    pub reachable_voters: usize,
    pub last_committed: u64,
    pub last_applied: u64,
    pub nodes: Vec<CpClusterNodeStatus>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CpRecoveryStatus {
    pub last_committed: u64,
    pub last_applied: u64,
    pub in_doubt_dist_txns: usize,
}

pub struct CpClusterHandle<S: StorageEngine> {
    raft: Arc<CpRaft<S>>,
}

openraft::declare_raft_types!(
    pub CpOpenRaftTypeConfig:
        D = ReplCommand,
        R = ReplApplyResponse,
        NodeId = NodeId,
        Node = openraft::BasicNode,
);

pub type CpOpenRaft = openraft::Raft<CpOpenRaftTypeConfig>;

pub struct CpRaft<S: StorageEngine> {
    storage: Arc<S>,
    cluster: CpClusterConfig,
    openraft_config: Arc<openraft::Config>,
    live: Option<Arc<LiveCpRuntime<S>>>,
    commit_lock: Mutex<()>,
    available_voters: Mutex<BTreeSet<NodeId>>,
    healing_evicted_voters: Mutex<BTreeSet<NodeId>>,
    healing_learners: Mutex<BTreeMap<NodeId, RaftNode>>,
    healing_promoted_voters: Mutex<BTreeSet<NodeId>>,
    _openraft: PhantomData<CpOpenRaft>,
}

impl fmt::Display for ReplCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Batch(ops) => write!(f, "batch({} ops)", ops.len()),
            Self::Conditional {
                conditions, ops, ..
            } => write!(
                f,
                "conditional({} conditions, {} ops)",
                conditions.len(),
                ops.len()
            ),
            Self::TxnCommit {
                snapshot_id,
                writes,
                ..
            } => {
                write!(
                    f,
                    "txn_commit(snapshot={snapshot_id}, {} writes)",
                    writes.len()
                )
            }
            Self::DistTxnPrepare { txn_id, ops, .. } => {
                write!(f, "dist_txn_prepare({txn_id}, {} ops)", ops.len())
            }
            Self::DistTxnFinish {
                txn_id, decision, ..
            } => write!(f, "dist_txn_finish({txn_id}, {decision:?})"),
        }
    }
}

impl RaftNode {
    #[must_use]
    pub fn new(id: NodeId, addr: impl Into<String>) -> Self {
        Self {
            id,
            addr: addr.into(),
        }
    }
}

impl CpClusterConfig {
    #[must_use]
    pub fn new(node_id: NodeId, bind_addr: impl Into<String>, voters: Vec<RaftNode>) -> Self {
        Self {
            node_id,
            bind_addr: bind_addr.into(),
            voters,
            learners: Vec::new(),
            bootstrap: ClusterBootstrap::Initialize,
            transport: None,
            runtime: RaftRuntimeConfig::default(),
            dist_txn: DistTxnConfig::default(),
            hlc: HlcConfig::default(),
            region: None,
            flow_control: FlowControlConfig::default(),
        }
    }

    #[must_use]
    pub fn with_learners(mut self, learners: Vec<RaftNode>) -> Self {
        self.learners = learners;
        self
    }

    #[must_use]
    pub const fn with_bootstrap(mut self, bootstrap: ClusterBootstrap) -> Self {
        self.bootstrap = bootstrap;
        self
    }

    #[must_use]
    pub fn with_transport(mut self, transport: InternalTransportConfig) -> Self {
        self.transport = Some(transport);
        self
    }

    #[must_use]
    pub const fn with_runtime(mut self, runtime: RaftRuntimeConfig) -> Self {
        self.runtime = runtime;
        self
    }

    #[must_use]
    pub const fn with_dist_txn(mut self, dist_txn: DistTxnConfig) -> Self {
        self.dist_txn = dist_txn;
        self
    }

    #[must_use]
    pub const fn with_hlc(mut self, hlc: HlcConfig) -> Self {
        self.hlc = hlc;
        self
    }

    #[must_use]
    pub fn with_region(mut self, region: RegionConfig) -> Self {
        self.region = Some(region);
        self
    }

    #[must_use]
    pub const fn with_flow_control(mut self, flow_control: FlowControlConfig) -> Self {
        self.flow_control = flow_control;
        self
    }

    #[must_use]
    pub fn single_node_for_tests(node_id: NodeId) -> Self {
        let addr = format!("in-process-{node_id}");
        Self {
            node_id,
            bind_addr: addr.clone(),
            voters: vec![RaftNode::new(node_id, addr)],
            learners: Vec::new(),
            bootstrap: ClusterBootstrap::Initialize,
            transport: None,
            runtime: RaftRuntimeConfig::default(),
            dist_txn: DistTxnConfig::default(),
            hlc: HlcConfig::default(),
            region: None,
            flow_control: FlowControlConfig::default(),
        }
    }

    #[must_use]
    pub const fn quorum_size(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    #[must_use]
    pub fn is_voter(&self, node_id: NodeId) -> bool {
        self.voters.iter().any(|node| node.id == node_id)
    }
}

impl<S: StorageEngine> Clone for CpClusterHandle<S> {
    fn clone(&self) -> Self {
        Self {
            raft: Arc::clone(&self.raft),
        }
    }
}

impl<S: StorageEngine> CpClusterHandle<S> {
    #[must_use]
    pub fn replication(&self) -> Arc<CpRaft<S>> {
        Arc::clone(&self.raft)
    }
}

/// Starts a managed CP cluster handle while preserving the synchronous replication API.
///
/// # Errors
/// Fails when the cluster topology is invalid.
pub fn start_cp_cluster<S: StorageEngine>(
    storage: S,
    config: CpClusterConfig,
) -> Result<CpClusterHandle<S>, ReplError> {
    validate_cp_cluster_config(&config, false).map_err(ReplError::Transport)?;
    let live = LiveCpRuntime::start(storage, config.clone())?;
    Ok(CpClusterHandle {
        raft: Arc::new(CpRaft::from_live(config, live)),
    })
}

/// Shuts down a managed CP cluster handle.
///
/// The current compatibility backend has no background worker to drain. The function is
/// still part of the stable operator contract so future runtimes can add graceful drains
/// without changing callers.
///
/// # Errors
/// Reserved for future worker drain failures.
pub fn shutdown_cp_cluster<S: StorageEngine>(handle: CpClusterHandle<S>) -> Result<(), ReplError> {
    if let Some(live) = &handle.raft.live {
        live.shutdown()?;
    }
    drop(handle);
    Ok(())
}

/// Returns the current CP cluster status.
///
/// # Errors
/// Fails when storage metadata cannot be read or membership locks are poisoned.
pub fn cluster_status<S: StorageEngine>(
    handle: &CpClusterHandle<S>,
) -> Result<CpClusterStatus, ReplError> {
    if let Some(live) = &handle.raft.live {
        return live.status();
    }
    cp_cluster_status(&handle.raft)
}

/// Replays durable in-doubt distributed transaction state and reports recovery progress.
///
/// # Errors
/// Fails when the replication backend cannot replay recovery state.
pub fn wait_for_recovery<S: StorageEngine>(
    handle: &CpClusterHandle<S>,
) -> Result<CpRecoveryStatus, ReplError> {
    if let Some(live) = &handle.raft.live {
        live.wait_for_openraft_recovery()?;
    }
    handle.raft.recover_dist_txns()?;
    let in_doubt_dist_txns = handle.raft.local_prepared_dist_txns()?.len();
    Ok(CpRecoveryStatus {
        last_committed: read_raft_index(handle.raft.storage(), LAST_COMMITTED_KEY)?,
        last_applied: read_raft_index(handle.raft.storage(), LAST_APPLIED_KEY)?,
        in_doubt_dist_txns,
    })
}

/// Applies a deterministic membership update through the current CP adapter.
///
/// # Errors
/// Fails when the requested membership is invalid, quorum is unavailable, or the change
/// would require an unsupported unsafe runtime transition.
pub fn change_membership<S: StorageEngine>(
    handle: &CpClusterHandle<S>,
    voters: Vec<RaftNode>,
    learners: Vec<RaftNode>,
) -> Result<CpClusterStatus, ReplError> {
    let mut candidate = handle.raft.cluster_config().clone();
    candidate.voters.clone_from(&voters);
    candidate.learners.clone_from(&learners);
    validate_cp_cluster_config(&candidate, false).map_err(ReplError::Unsupported)?;

    if let Some(live) = &handle.raft.live {
        return live.change_membership(&voters, &learners);
    }

    handle.raft.ensure_write_quorum()?;
    let target_voters = voters.iter().map(|node| node.id).collect::<BTreeSet<_>>();
    let current_voters = handle.raft.effective_voter_ids()?;

    for node in current_voters.difference(&target_voters).copied() {
        handle.raft.evict_healing_voter(node)?;
    }

    for node in learners {
        if !target_voters.contains(&node.id) {
            handle.raft.add_healing_learner(node)?;
        }
    }

    for node in voters {
        if !current_voters.contains(&node.id) {
            handle.raft.add_healing_learner(node.clone())?;
            handle.raft.resync_healing_node(node.id)?;
            handle.raft.promote_healing_learner(node.id)?;
        }
    }

    cp_cluster_status(&handle.raft)
}

/// Transfers leadership when the requested target is supported by the mounted runtime.
///
/// # Errors
/// Fails closed for remote targets until the live `OpenRaft` worker is mounted.
pub fn transfer_leader<S: StorageEngine>(
    handle: &CpClusterHandle<S>,
    target: NodeId,
) -> Result<CpClusterStatus, ReplError> {
    if let Some(live) = &handle.raft.live {
        return live.transfer_leader(target);
    }
    let voters = handle.raft.effective_voter_ids()?;
    if !voters.contains(&target) {
        return Err(ReplError::Unsupported(format!(
            "cannot transfer CP leadership to non-voter {target}"
        )));
    }

    handle.raft.ensure_write_quorum()?;
    if target != handle.raft.cluster_config().node_id {
        return Err(ReplError::Unsupported(
            "remote CP leader transfer requires the live OpenRaft runtime; request rejected before state change"
                .to_owned(),
        ));
    }

    cp_cluster_status(&handle.raft)
}

/// Validates CP cluster topology.
/// # Errors
/// Returns a message when the cluster has invalid voters, learners, or addresses.
pub fn validate_cp_cluster_config(
    config: &CpClusterConfig,
    allow_single_node: bool,
) -> Result<(), String> {
    if config.bind_addr.trim().is_empty() {
        return Err("cluster bind address cannot be empty".to_owned());
    }

    let voter_count = config.voters.len();
    if allow_single_node && voter_count == 1 {
        validate_nodes("voter", &config.voters)?;
        validate_nodes("learner", &config.learners)?;
        validate_node_membership(config)?;
        return Ok(());
    }

    if !matches!(voter_count, 3 | 5 | 7) {
        return Err(format!(
            "CP cluster voters must be 3, 5, or 7; got {voter_count}"
        ));
    }

    if config.transport.is_none() {
        return Err(
            "CP multi-node replication requires InternalTransportConfig at startup".to_owned(),
        );
    }

    validate_nodes("voter", &config.voters)?;
    validate_nodes("learner", &config.learners)?;
    validate_node_membership(config)
}

impl<S: StorageEngine> CpRaft<S> {
    /// Creates a CP/Raft replication backend.
    /// # Errors
    /// Fails when the cluster topology is invalid.
    pub fn new(storage: S, cluster: CpClusterConfig) -> Result<Self, ReplError> {
        validate_cp_cluster_config(&cluster, false).map_err(ReplError::Transport)?;
        Ok(Self::with_validated_config(storage, cluster))
    }

    #[must_use]
    pub(crate) fn new_unchecked(storage: S, cluster: CpClusterConfig) -> Self {
        Self::with_validated_config(storage, cluster)
    }

    #[must_use]
    pub fn new_for_test(storage: S, node_id: NodeId) -> Self {
        Self::with_validated_config(storage, CpClusterConfig::single_node_for_tests(node_id))
    }

    #[must_use]
    pub fn storage(&self) -> &S {
        self.storage.as_ref()
    }

    #[must_use]
    pub const fn cluster_config(&self) -> &CpClusterConfig {
        &self.cluster
    }

    #[must_use]
    pub fn openraft_config(&self) -> Arc<openraft::Config> {
        Arc::clone(&self.openraft_config)
    }

    #[must_use]
    pub fn quorum_size(&self) -> usize {
        self.effective_voter_ids().map_or_else(
            |_| self.cluster.quorum_size(),
            |voters| voters.len() / 2 + 1,
        )
    }

    /// Replaces the set of voters considered reachable by this node.
    /// # Errors
    /// Fails when the set contains an unknown voter id or the lock is poisoned.
    pub fn set_available_voters_for_tests(
        &self,
        available: impl IntoIterator<Item = NodeId>,
    ) -> Result<(), ReplError> {
        let known_ids = self.known_node_ids_set()?;
        let mut next = BTreeSet::new();

        for node_id in available {
            if !known_ids.contains(&node_id) {
                return Err(ReplError::Transport(format!("unknown voter id {node_id}")));
            }
            next.insert(node_id);
        }

        *self.available_voters()? = next;
        Ok(())
    }

    /// Returns voters after simulated self-healing membership changes.
    /// # Errors
    /// Fails when healing membership locks are poisoned.
    pub fn effective_voter_ids(&self) -> Result<BTreeSet<NodeId>, ReplError> {
        let evicted = self.healing_evicted_voters()?;
        let promoted = self.healing_promoted_voters()?;
        let mut voters = self
            .cluster
            .voters
            .iter()
            .map(|node| node.id)
            .filter(|node_id| !evicted.contains(node_id))
            .collect::<BTreeSet<_>>();
        voters.extend(
            promoted
                .iter()
                .copied()
                .filter(|node_id| !evicted.contains(node_id)),
        );
        Ok(voters)
    }

    /// Returns every known CP node, including self-healing learners.
    #[must_use]
    pub fn known_node_ids(&self) -> Vec<NodeId> {
        self.known_node_ids_set().map_or_else(
            |_| {
                self.cluster
                    .voters
                    .iter()
                    .chain(&self.cluster.learners)
                    .map(|node| node.id)
                    .collect()
            },
            |nodes| nodes.into_iter().collect(),
        )
    }

    /// Removes a voter through the current self-healing adapter.
    /// # Errors
    /// Fails when quorum is unavailable or healing state cannot be updated.
    pub fn evict_healing_voter(&self, node: NodeId) -> Result<(), ReplError> {
        if !self.effective_voter_ids()?.contains(&node) {
            return Ok(());
        }

        self.ensure_write_quorum()?;
        self.healing_evicted_voters()?.insert(node);
        self.available_voters()?.remove(&node);
        Ok(())
    }

    /// Adds a replacement node as learner.
    /// # Errors
    /// Fails when quorum is unavailable or healing state cannot be updated.
    pub fn add_healing_learner(&self, node: RaftNode) -> Result<(), ReplError> {
        if self.effective_voter_ids()?.contains(&node.id) {
            return Ok(());
        }

        self.ensure_write_quorum()?;
        self.healing_learners()?.insert(node.id, node);
        Ok(())
    }

    /// Marks a learner resync as complete in the current adapter.
    /// # Errors
    /// Fails when the node is unknown.
    pub fn resync_healing_node(&self, node: NodeId) -> Result<(), ReplError> {
        if !self.known_node_ids_set()?.contains(&node) {
            return Err(ReplError::Transport(format!(
                "cannot resync unknown CP node {node}"
            )));
        }
        Ok(())
    }

    /// Promotes a learner to voter through the current self-healing adapter.
    /// # Errors
    /// Fails when quorum is unavailable, the node is unknown, or healing state cannot be updated.
    pub fn promote_healing_learner(&self, node: NodeId) -> Result<(), ReplError> {
        if self.effective_voter_ids()?.contains(&node) {
            return Ok(());
        }

        if !self.known_node_ids_set()?.contains(&node) {
            return Err(ReplError::Transport(format!(
                "cannot promote unknown CP learner {node}"
            )));
        }

        self.ensure_write_quorum()?;
        self.healing_promoted_voters()?.insert(node);
        self.available_voters()?.insert(node);
        Ok(())
    }

    /// Commits a transaction write set through the CP path.
    /// # Errors
    /// Fails when quorum is unavailable or storage detects a conflict.
    pub fn commit_write_set(
        &self,
        snapshot_id: TxnId,
        write_set: WriteSet,
    ) -> Result<TxnId, ReplError> {
        if write_set.is_empty() {
            return Ok(snapshot_id);
        }

        if let Some(live) = &self.live {
            return live
                .propose(ReplCommand::TxnCommit {
                    snapshot_id,
                    writes: write_set_to_entries(write_set),
                    hlc: Some(HlcTimestamp::now()),
                })
                .map(|response| response.txn_id);
        }

        self.ensure_write_quorum()?;
        self.apply_command(&ReplCommand::TxnCommit {
            snapshot_id,
            writes: write_set_to_entries(write_set),
            hlc: Some(HlcTimestamp::now()),
        })
        .map_err(Into::into)
    }

    /// Commits a transaction write set through the CP path with caller-supplied validation.
    /// # Errors
    /// Fails when quorum is unavailable, validation fails, or storage detects a conflict.
    pub fn commit_write_set_with_preflight<'a, F>(
        &'a self,
        snapshot_id: TxnId,
        write_set: WriteSet,
        preflight: F,
    ) -> Result<TxnId, ReplError>
    where
        F: FnOnce(&mut S::WriteTxn<'a>) -> Result<(), StorageError>,
    {
        if write_set.is_empty() {
            return Ok(snapshot_id);
        }

        if self.live.is_some() {
            return Err(ReplError::Unsupported(
                "live CP Raft cannot replicate caller-local preflight closures".to_owned(),
            ));
        }

        self.ensure_write_quorum()?;
        self.apply_command_with_preflight(
            &ReplCommand::TxnCommit {
                snapshot_id,
                writes: write_set_to_entries(write_set),
                hlc: Some(HlcTimestamp::now()),
            },
            preflight,
        )
        .map_err(Into::into)
    }

    fn with_validated_config(storage: S, cluster: CpClusterConfig) -> Self {
        let available_voters = cluster.voters.iter().map(|node| node.id).collect();
        let openraft_config = Arc::new(openraft_config_from_cluster(&cluster));
        Self {
            storage: Arc::new(storage),
            cluster,
            openraft_config,
            live: None,
            commit_lock: Mutex::new(()),
            available_voters: Mutex::new(available_voters),
            healing_evicted_voters: Mutex::new(BTreeSet::new()),
            healing_learners: Mutex::new(BTreeMap::new()),
            healing_promoted_voters: Mutex::new(BTreeSet::new()),
            _openraft: PhantomData,
        }
    }

    fn from_live(cluster: CpClusterConfig, live: LiveCpHandle<S>) -> Self {
        let available_voters = cluster.voters.iter().map(|node| node.id).collect();
        let openraft_config = Arc::new(openraft_config_from_cluster(&cluster));
        Self {
            storage: live.storage,
            cluster,
            openraft_config,
            live: Some(live.runtime),
            commit_lock: Mutex::new(()),
            available_voters: Mutex::new(available_voters),
            healing_evicted_voters: Mutex::new(BTreeSet::new()),
            healing_learners: Mutex::new(BTreeMap::new()),
            healing_promoted_voters: Mutex::new(BTreeSet::new()),
            _openraft: PhantomData,
        }
    }

    fn known_node_ids_set(&self) -> Result<BTreeSet<NodeId>, ReplError> {
        let mut nodes = self
            .cluster
            .voters
            .iter()
            .chain(&self.cluster.learners)
            .map(|node| node.id)
            .collect::<BTreeSet<_>>();
        nodes.extend(self.healing_learners()?.keys().copied());
        nodes.extend(self.healing_promoted_voters()?.iter().copied());
        Ok(nodes)
    }

    fn available_voters(&self) -> Result<MutexGuard<'_, BTreeSet<NodeId>>, ReplError> {
        self.available_voters
            .lock()
            .map_err(|_| ReplError::Transport("CP voter availability lock poisoned".to_owned()))
    }

    fn healing_evicted_voters(&self) -> Result<MutexGuard<'_, BTreeSet<NodeId>>, ReplError> {
        self.healing_evicted_voters
            .lock()
            .map_err(|_| ReplError::Transport("CP healing eviction lock poisoned".to_owned()))
    }

    fn healing_learners(&self) -> Result<MutexGuard<'_, BTreeMap<NodeId, RaftNode>>, ReplError> {
        self.healing_learners
            .lock()
            .map_err(|_| ReplError::Transport("CP healing learner lock poisoned".to_owned()))
    }

    fn healing_promoted_voters(&self) -> Result<MutexGuard<'_, BTreeSet<NodeId>>, ReplError> {
        self.healing_promoted_voters
            .lock()
            .map_err(|_| ReplError::Transport("CP healing promotion lock poisoned".to_owned()))
    }

    fn ensure_write_quorum(&self) -> Result<(), ReplError> {
        let effective_voters = self.effective_voter_ids()?;
        if !effective_voters.contains(&self.cluster.node_id) {
            return Err(ReplError::NoQuorum);
        }

        let available = self
            .available_voters()?
            .iter()
            .filter(|node| effective_voters.contains(node))
            .count();
        if available < effective_voters.len() / 2 + 1 {
            return Err(ReplError::NoQuorum);
        }

        Ok(())
    }

    fn ensure_linearizable(&self, policy: &openraft::ReadPolicy) -> Result<(), ReplError> {
        match policy {
            openraft::ReadPolicy::ReadIndex | openraft::ReadPolicy::LeaseRead => {
                self.ensure_write_quorum()
            }
        }
    }

    fn ensure_strong_read(&self) -> Result<(), ReplError> {
        self.ensure_linearizable(&openraft::ReadPolicy::ReadIndex)
    }

    fn apply_command(&self, command: &ReplCommand) -> Result<TxnId, StorageError> {
        self.apply_command_with_authorization(command, None)
    }

    fn apply_authorized_command(
        &self,
        command: &ReplCommand,
        authorization: txn::WriteAuthorization,
    ) -> Result<TxnId, StorageError> {
        self.apply_command_with_authorization(command, Some(authorization))
    }

    fn apply_command_with_authorization(
        &self,
        command: &ReplCommand,
        authorization: Option<txn::WriteAuthorization>,
    ) -> Result<TxnId, StorageError> {
        self.apply_command_with_authorization_and_preflight(command, authorization, |_| Ok(()))
    }

    fn apply_command_with_preflight<'a, F>(
        &'a self,
        command: &ReplCommand,
        preflight: F,
    ) -> Result<TxnId, StorageError>
    where
        F: FnOnce(&mut S::WriteTxn<'a>) -> Result<(), StorageError>,
    {
        self.apply_command_with_authorization_and_preflight(command, None, preflight)
    }

    fn apply_command_with_authorization_and_preflight<'a, F>(
        &'a self,
        command: &ReplCommand,
        authorization: Option<txn::WriteAuthorization>,
        preflight: F,
    ) -> Result<TxnId, StorageError>
    where
        F: FnOnce(&mut S::WriteTxn<'a>) -> Result<(), StorageError>,
    {
        let _guard = self
            .commit_lock
            .lock()
            .map_err(|_| StorageError::Backend("CP commit lock poisoned".to_owned()))?;

        let write_set = match &command {
            ReplCommand::Batch(ops) | ReplCommand::Conditional { ops, .. } => {
                txn::ops_to_write_set(ops.clone())
            }
            ReplCommand::TxnCommit { writes, .. } => entries_to_write_set(writes.clone()),
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
        };

        if write_set.is_empty() {
            return txn::current_txn_id(self.storage.as_ref());
        }

        let write = self.storage.begin_write()?;
        let snapshot_id = match &command {
            ReplCommand::Batch(_)
            | ReplCommand::Conditional { .. }
            | ReplCommand::DistTxnPrepare { .. }
            | ReplCommand::DistTxnFinish { .. } => txn::current_txn_id_from(&write)?,
            ReplCommand::TxnCommit { snapshot_id, .. } => *snapshot_id,
        };
        let log_index = next_log_index(&write)?;
        let command_bytes = serialize_command(command)?;

        if let Some(authorization) = authorization {
            txn::commit_write_set_in_txn_with_extra_authorized(
                write,
                snapshot_id,
                write_set,
                preflight,
                |txn, next_txn_id| write_raft_metadata(txn, log_index, &command_bytes, next_txn_id),
                authorization,
            )
        } else {
            txn::commit_write_set_in_txn_with_preflight_and_extra(
                write,
                snapshot_id,
                write_set,
                preflight,
                |txn, next_txn_id| write_raft_metadata(txn, log_index, &command_bytes, next_txn_id),
            )
        }
    }

    fn apply_conditional_batch(&self, batch: ConditionalBatch) -> Result<TxnId, StorageError> {
        let _guard = self
            .commit_lock
            .lock()
            .map_err(|_| StorageError::Backend("CP commit lock poisoned".to_owned()))?;

        let ConditionalBatch { conditions, ops } = batch;
        let command = ReplCommand::Conditional {
            conditions: conditions.clone(),
            ops,
            hlc: Some(HlcTimestamp::now()),
        };
        let write_set = match &command {
            ReplCommand::Batch(ops) | ReplCommand::Conditional { ops, .. } => {
                txn::ops_to_write_set(ops.clone())
            }
            ReplCommand::TxnCommit { writes, .. } => entries_to_write_set(writes.clone()),
            ReplCommand::DistTxnPrepare { .. } | ReplCommand::DistTxnFinish { .. } => {
                return Err(StorageError::Backend(
                    "conditional batch cannot be encoded as a dist-txn command".to_owned(),
                ));
            }
        };
        let write = self.storage.begin_write()?;
        for condition in &conditions {
            if !condition_matches(&write, condition)? {
                return Err(StorageError::Conflict);
            }
        }
        if write_set.is_empty() {
            return txn::current_txn_id_from(&write);
        }

        let snapshot_id = txn::current_txn_id_from(&write)?;
        let log_index = next_log_index(&write)?;
        let command_bytes = serialize_command(&command)?;

        txn::commit_write_set_in_txn_with_extra(
            write,
            snapshot_id,
            write_set,
            |txn, next_txn_id| write_raft_metadata(txn, log_index, &command_bytes, next_txn_id),
        )
    }

    fn load_prepared_dist_txn(
        &self,
        txn_id: DistTxnId,
    ) -> Result<Option<PreparedTxnRecord>, StorageError> {
        let read = self.storage.begin_read()?;
        read.get(DIST_TXN_PARTICIPANT_TABLE, &dist_txn::txn_key(txn_id))?
            .map(|bytes| dist_txn::decode_prepared(&bytes))
            .transpose()
    }

    fn load_dist_txn_decision(&self, txn_id: DistTxnId) -> Result<Option<Decision>, StorageError> {
        let read = self.storage.begin_read()?;
        read.get(DIST_TXN_COORDINATOR_TABLE, &dist_txn::txn_key(txn_id))?
            .map(|bytes| dist_txn::decode_decision(&bytes).map(|record| record.decision))
            .transpose()
    }

    fn local_prepared_dist_txns(&self) -> Result<Vec<PreparedTxnRecord>, StorageError> {
        let read = self.storage.begin_read()?;
        read.range(DIST_TXN_PARTICIPANT_TABLE, &[], &[])?
            .map(|entry| {
                let (_, bytes) = entry?;
                dist_txn::decode_prepared(&bytes)
            })
            .collect()
    }

    fn mark_dist_txn_finished(
        &self,
        txn_id: DistTxnId,
        decision: Decision,
    ) -> Result<(), ReplError> {
        if let Some(live) = &self.live {
            live.propose(ReplCommand::DistTxnFinish {
                txn_id,
                decision,
                hlc: Some(HlcTimestamp::now()),
            })?;
            return Ok(());
        }

        self.ensure_write_quorum()?;
        self.apply_authorized_command(
            &ReplCommand::DistTxnFinish {
                txn_id,
                decision,
                hlc: Some(HlcTimestamp::now()),
            },
            txn::system_write_authorization(),
        )?;
        Ok(())
    }
}

impl<S: StorageEngine> Replication for CpRaft<S> {
    fn propose(&self, op: Op) -> Result<(), ReplError> {
        self.propose_batch(vec![op])
    }

    fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
        if ops.is_empty() {
            return Ok(());
        }

        txn::validate_public_ops(&ops)?;
        if let Some(live) = &self.live {
            live.propose(ReplCommand::Batch(ops))?;
            return Ok(());
        }

        self.ensure_write_quorum()?;
        self.apply_command(&ReplCommand::Batch(ops))?;
        Ok(())
    }

    fn propose_authorized_batch(
        &self,
        ops: Vec<Op>,
        authorization: txn::WriteAuthorization,
    ) -> Result<(), ReplError> {
        if ops.is_empty() {
            return Ok(());
        }

        if let Some(live) = &self.live {
            live.propose(ReplCommand::Batch(ops))?;
            return Ok(());
        }

        self.ensure_write_quorum()?;
        self.apply_authorized_command(&ReplCommand::Batch(ops), authorization)?;
        Ok(())
    }

    fn propose_conditional_batch(&self, batch: ConditionalBatch) -> Result<(), ReplError> {
        if batch.conditions.is_empty() {
            return self.propose_batch(batch.ops);
        }

        txn::validate_public_conditions(&batch.conditions)?;
        txn::validate_public_ops(&batch.ops)?;
        if let Some(live) = &self.live {
            live.propose(ReplCommand::Conditional {
                conditions: batch.conditions,
                ops: batch.ops,
                hlc: Some(HlcTimestamp::now()),
            })?;
            return Ok(());
        }

        self.ensure_write_quorum()?;
        match self.apply_conditional_batch(batch) {
            Ok(_) => Ok(()),
            Err(StorageError::Conflict) => Err(ReplError::Conflict),
            Err(error) => Err(error.into()),
        }
    }

    fn prepare_dist_txn(&self, txn_id: DistTxnId, ops: Vec<Op>) -> Result<Vote, ReplError> {
        txn::validate_public_ops(&ops)?;
        let key = dist_txn::txn_key(txn_id);
        {
            let read = self.storage.begin_read()?;
            if read.get(DIST_TXN_FINISHED_TABLE, &key)?.is_some() {
                return Ok(Vote::Yes);
            }
            if let Some(existing) = read.get(DIST_TXN_PARTICIPANT_TABLE, &key)? {
                let existing = dist_txn::decode_prepared(&existing)?;
                return Ok(if existing.ops == ops {
                    Vote::Yes
                } else {
                    Vote::No
                });
            }
        }

        if let Some(live) = &self.live {
            match live.propose(ReplCommand::DistTxnPrepare {
                txn_id,
                ops,
                deadline_ms: self.cluster.dist_txn.prepare_timeout_ms,
                hlc: Some(HlcTimestamp::now()),
            }) {
                Ok(_) => return Ok(Vote::Yes),
                Err(ReplError::Conflict) => return Ok(Vote::No),
                Err(error) => return Err(error),
            }
        }

        self.ensure_write_quorum()?;
        self.apply_authorized_command(
            &ReplCommand::DistTxnPrepare {
                txn_id,
                ops,
                deadline_ms: self.cluster.dist_txn.prepare_timeout_ms,
                hlc: Some(HlcTimestamp::now()),
            },
            txn::system_write_authorization(),
        )?;
        Ok(Vote::Yes)
    }

    fn finish_dist_txn(&self, txn_id: DistTxnId, decision: Decision) -> Result<(), ReplError> {
        let key = dist_txn::txn_key(txn_id);
        {
            let read = self.storage.begin_read()?;
            if read.get(DIST_TXN_FINISHED_TABLE, &key)?.is_some() {
                return Ok(());
            }
        }

        let prepared = self.load_prepared_dist_txn(txn_id)?;
        if let Some(live) = &self.live {
            if let (Decision::Commit, Some(record)) = (decision, prepared) {
                live.propose(ReplCommand::Batch(record.ops))?;
            }
            live.propose(ReplCommand::DistTxnFinish {
                txn_id,
                decision,
                hlc: Some(HlcTimestamp::now()),
            })?;
            return Ok(());
        }

        if let (Decision::Commit, Some(record)) = (decision, prepared) {
            self.apply_authorized_command(
                &ReplCommand::Batch(record.ops),
                txn::system_write_authorization(),
            )?;
        }
        self.mark_dist_txn_finished(txn_id, decision)
    }

    fn recover_dist_txns(&self) -> Result<(), ReplError> {
        for record in self.local_prepared_dist_txns()? {
            let decision = self
                .load_dist_txn_decision(record.txn_id)?
                .unwrap_or(Decision::Abort);
            self.finish_dist_txn(record.txn_id, decision)?;
        }
        Ok(())
    }

    fn read(
        &self,
        table: &str,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Option<Bytes>, ReplError> {
        if consistency == ReadConsistency::Strong {
            if let Some(live) = &self.live {
                live.strong_read_barrier()?;
            } else {
                self.ensure_strong_read()?;
            }
        }

        let txn = self.storage.begin_read()?;
        Ok(txn.get(table, key)?)
    }

    fn range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        if consistency == ReadConsistency::Strong {
            if let Some(live) = &self.live {
                live.strong_read_barrier()?;
            } else {
                self.ensure_strong_read()?;
            }
        }

        let txn = self.storage.begin_read()?;
        txn.range(table, start, end)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn scan_range_batches(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
        batch_rows: usize,
        cancelled: &dyn Fn() -> bool,
        on_batch: &mut dyn FnMut(&[(Bytes, Bytes)]) -> Result<bool, ReplError>,
    ) -> Result<(), ReplError> {
        if consistency == ReadConsistency::Strong {
            if let Some(live) = &self.live {
                live.strong_read_barrier()?;
            } else {
                self.ensure_strong_read()?;
            }
        }

        let txn = self.storage.begin_read()?;
        let mut batch = Vec::with_capacity(batch_rows.max(1));
        for entry in txn.range(table, start, end)? {
            if cancelled() {
                break;
            }
            batch.push(entry?);
            if batch.len() >= batch_rows.max(1) {
                if !on_batch(&batch)? {
                    return Ok(());
                }
                batch.clear();
            }
        }
        if !batch.is_empty() && !cancelled() {
            let _ = on_batch(&batch)?;
        }
        Ok(())
    }
}

fn validate_nodes(label: &str, nodes: &[RaftNode]) -> Result<(), String> {
    let mut ids = BTreeSet::new();

    for node in nodes {
        if node.id == 0 {
            return Err(format!("{label} id cannot be zero"));
        }

        if node.addr.trim().is_empty() {
            return Err(format!("{label} {} address cannot be empty", node.id));
        }

        if !ids.insert(node.id) {
            return Err(format!("duplicate {label} id {}", node.id));
        }
    }

    Ok(())
}

fn validate_node_membership(config: &CpClusterConfig) -> Result<(), String> {
    let voter_ids: BTreeSet<_> = config.voters.iter().map(|node| node.id).collect();
    let learner_ids: BTreeSet<_> = config.learners.iter().map(|node| node.id).collect();

    if !voter_ids.contains(&config.node_id) && !learner_ids.contains(&config.node_id) {
        return Err(format!(
            "local node {} must be a voter or learner",
            config.node_id
        ));
    }

    if let Some(overlap) = voter_ids.intersection(&learner_ids).next() {
        return Err(format!("node {overlap} cannot be both voter and learner"));
    }

    Ok(())
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

fn cp_cluster_status<S: StorageEngine>(raft: &CpRaft<S>) -> Result<CpClusterStatus, ReplError> {
    let voters = raft.effective_voter_ids()?;
    let available = raft.available_voters()?;
    let reachable_voters = available
        .iter()
        .filter(|node| voters.contains(node))
        .count();
    let quorum_size = voters.len() / 2 + 1;
    let leader_id = if voters.contains(&raft.cluster.node_id) && reachable_voters >= quorum_size {
        Some(raft.cluster.node_id)
    } else {
        None
    };

    let evicted = raft.healing_evicted_voters()?;
    let mut nodes = raft
        .cluster
        .voters
        .iter()
        .filter(|node| !evicted.contains(&node.id))
        .chain(&raft.cluster.learners)
        .map(|node| (node.id, node.clone()))
        .collect::<BTreeMap<_, _>>();
    drop(evicted);
    nodes.extend(
        raft.healing_learners()?
            .iter()
            .map(|(id, node)| (*id, node.clone())),
    );
    drop(available);

    let available = raft.available_voters()?;
    let mut node_statuses = nodes
        .values()
        .map(|node| {
            let voter = voters.contains(&node.id);
            let role = if leader_id == Some(node.id) {
                CpClusterRole::Leader
            } else if voter {
                CpClusterRole::Follower
            } else {
                CpClusterRole::Learner
            };
            CpClusterNodeStatus {
                id: node.id,
                addr: node.addr.clone(),
                role,
                voter,
                reachable: !voter || available.contains(&node.id),
            }
        })
        .collect::<Vec<_>>();
    node_statuses.sort_by_key(|node| node.id);

    let learners = node_statuses
        .iter()
        .filter(|node| !node.voter)
        .map(|node| node.id)
        .collect::<Vec<_>>();

    Ok(CpClusterStatus {
        node_id: raft.cluster.node_id,
        leader_id,
        voters: voters.into_iter().collect(),
        learners,
        quorum_size,
        reachable_voters,
        last_committed: read_raft_index(raft.storage(), LAST_COMMITTED_KEY)?,
        last_applied: read_raft_index(raft.storage(), LAST_APPLIED_KEY)?,
        nodes: node_statuses,
    })
}

fn read_raft_index(storage: &impl StorageEngine, key: &[u8]) -> Result<u64, StorageError> {
    let read = storage.begin_read()?;
    read.get(RAFT_STATE_TABLE, key)?
        .map(|bytes| decode_u64(&bytes))
        .transpose()
        .map(Option::unwrap_or_default)
}

fn write_set_to_entries(write_set: WriteSet) -> Vec<(String, Bytes, Option<Bytes>)> {
    write_set
        .into_iter()
        .map(|((table, key), value)| (table, key, value))
        .collect()
}

fn entries_to_write_set(entries: Vec<(String, Bytes, Option<Bytes>)>) -> WriteSet {
    entries
        .into_iter()
        .map(|(table, key, value)| ((table, key), value))
        .collect()
}

fn next_log_index(txn: &impl ReadTransaction) -> Result<u64, StorageError> {
    match txn.get(RAFT_STATE_TABLE, LAST_APPLIED_KEY)? {
        Some(bytes) => decode_u64(&bytes)?
            .checked_add(1)
            .ok_or_else(|| StorageError::Backend("CP Raft log index overflow".to_owned())),
        None => Ok(1),
    }
}

fn write_raft_metadata<T: WriteTransaction>(
    txn: &mut T,
    log_index: u64,
    command_bytes: &[u8],
    txn_id: TxnId,
) -> Result<(), StorageError> {
    txn.put(RAFT_LOG_TABLE, &log_index.to_be_bytes(), command_bytes)?;
    txn.put(RAFT_STATE_TABLE, LAST_APPLIED_KEY, &log_index.to_be_bytes())?;
    txn.put(
        RAFT_STATE_TABLE,
        LAST_COMMITTED_KEY,
        &log_index.to_be_bytes(),
    )?;
    txn.put(RAFT_STATE_TABLE, LAST_TXN_ID_KEY, &txn_id.to_be_bytes())?;
    Ok(())
}

fn serialize_command(command: &ReplCommand) -> Result<Bytes, StorageError> {
    serde_json::to_vec(command).map_err(|error| StorageError::Backend(error.to_string()))
}

fn decode_u64(bytes: &[u8]) -> Result<u64, StorageError> {
    let bytes = bytes
        .try_into()
        .map_err(|_| StorageError::Corruption("u64 metadata must be 8 bytes".to_owned()))?;
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        net::TcpListener,
        sync::{Arc, Mutex, MutexGuard},
        thread,
        time::{Duration, Instant},
    };

    use super::{
        ClusterBootstrap, CpClusterConfig, CpClusterHandle, CpClusterRole, CpClusterStatus, CpRaft,
        NodeId, RAFT_LOG_TABLE, RAFT_STATE_TABLE, RaftNode, change_membership, cluster_status,
        shutdown_cp_cluster, start_cp_cluster, transfer_leader, validate_cp_cluster_config,
        wait_for_recovery,
    };
    use crate::{
        phase30::{InternalTransportConfig, InternalTransportSecurity, RaftRuntimeConfig},
        repl::{
            DIST_TXN_COORDINATOR_TABLE, Decision, Op, ReadConsistency, ReplError, Replication,
            Vote, dist_txn,
        },
        storage::{MemEngine, ReadTransaction, RedbEngine, StorageEngine, WriteTransaction},
    };

    static LIVE_CLUSTER_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn live_cluster_test_guard() -> MutexGuard<'static, ()> {
        match LIVE_CLUSTER_TEST_LOCK.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn three_voter_config() -> CpClusterConfig {
        CpClusterConfig::new(
            1,
            "127.0.0.1:7001",
            vec![
                RaftNode::new(1, "127.0.0.1:7001"),
                RaftNode::new(2, "127.0.0.1:7002"),
                RaftNode::new(3, "127.0.0.1:7003"),
            ],
        )
        .with_transport(InternalTransportConfig::new(
            "127.0.0.1:7001",
            InternalTransportSecurity::PlaintextForTests,
        ))
    }

    fn unused_addr() -> String {
        let listener = match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => listener,
            Err(error) => panic!("bind ephemeral test port: {error}"),
        };
        match listener.local_addr() {
            Ok(addr) => addr.to_string(),
            Err(error) => panic!("read ephemeral test port: {error}"),
        }
    }

    fn live_runtime_config() -> RaftRuntimeConfig {
        RaftRuntimeConfig {
            election_timeout_min_ms: 50,
            election_timeout_max_ms: 100,
            heartbeat_interval_ms: 20,
            snapshot_threshold: 32,
            max_payload_entries: 64,
            install_snapshot_timeout_ms: 5_000,
        }
    }

    fn live_transport(addr: &str) -> InternalTransportConfig {
        let mut transport =
            InternalTransportConfig::new(addr, InternalTransportSecurity::PlaintextForTests);
        transport.connect_timeout_ms = 200;
        transport.request_timeout_ms = 10_000;
        transport.idle_timeout_ms = 5_000;
        transport
    }

    fn live_cluster_configs(node_count: usize) -> Vec<CpClusterConfig> {
        let addrs = (0..node_count).map(|_| unused_addr()).collect::<Vec<_>>();
        let voters = (0..3)
            .map(|idx| RaftNode::new((idx + 1) as u64, addrs[idx].clone()))
            .collect::<Vec<_>>();
        let learners = (3..node_count)
            .map(|idx| RaftNode::new((idx + 1) as u64, addrs[idx].clone()))
            .collect::<Vec<_>>();

        addrs
            .iter()
            .enumerate()
            .map(|(idx, addr)| {
                let config = CpClusterConfig::new((idx + 1) as u64, addr.clone(), voters.clone())
                    .with_learners(learners.clone())
                    .with_transport(live_transport(addr))
                    .with_runtime(live_runtime_config());
                if idx >= 3 {
                    config.with_bootstrap(ClusterBootstrap::JoinExisting)
                } else {
                    config
                }
            })
            .collect()
    }

    fn start_live_cluster(node_count: usize) -> Result<Vec<CpClusterHandle<MemEngine>>, ReplError> {
        live_cluster_configs(node_count)
            .into_iter()
            .map(|config| start_cp_cluster(MemEngine::new(), config))
            .collect()
    }

    fn shutdown_all<S: StorageEngine>(handles: Vec<CpClusterHandle<S>>) {
        for handle in handles {
            let _ = shutdown_cp_cluster(handle);
        }
    }

    fn wait_for_live_leader<S: StorageEngine>(
        handles: &[CpClusterHandle<S>],
    ) -> Result<usize, ReplError> {
        let deadline = Instant::now() + Duration::from_secs(45);
        loop {
            for handle in handles {
                let Ok(status) = cluster_status(handle) else {
                    continue;
                };
                if let Some(leader_id) = status.leader_id
                    && let Some(leader_idx) = handles
                        .iter()
                        .position(|candidate| candidate.raft.cluster.node_id == leader_id)
                {
                    return Ok(leader_idx);
                }
            }

            if Instant::now() >= deadline {
                return Err(ReplError::Transport(
                    "timed out waiting for live CP leader".to_owned(),
                ));
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn wait_for_live_leader_change<S: StorageEngine>(
        handles: &[CpClusterHandle<S>],
        previous_leader: NodeId,
    ) -> Result<usize, ReplError> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let leader_idx = wait_for_live_leader(handles)?;
            if handles[leader_idx].raft.cluster.node_id != previous_leader {
                return Ok(leader_idx);
            }
            if Instant::now() >= deadline {
                return Err(ReplError::Transport(
                    "timed out waiting for live CP leader change".to_owned(),
                ));
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn wait_for_committed_live_config<S: StorageEngine>(
        handle: &CpClusterHandle<S>,
    ) -> Result<(), ReplError> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let status = cluster_status(handle)?;
            if status.last_committed >= 1 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(ReplError::Transport(
                    "timed out waiting for live CP config commit".to_owned(),
                ));
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn wait_for_live_value<S: StorageEngine>(
        handle: &CpClusterHandle<S>,
        key: &[u8],
        expected: &[u8],
        consistency: ReadConsistency,
    ) -> Result<(), ReplError> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match handle.replication().read("t", key, consistency) {
                Ok(Some(value)) if value == expected => return Ok(()),
                Ok(_) | Err(ReplError::NoQuorum | ReplError::Transport(_)) => {}
                Err(error) => return Err(error),
            }
            if Instant::now() >= deadline {
                return Err(ReplError::Transport(format!(
                    "timed out waiting for key {:?} to reach expected value",
                    String::from_utf8_lossy(key)
                )));
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn wait_for_live_membership_metadata<S: StorageEngine>(
        handle: &CpClusterHandle<S>,
    ) -> Result<(), ReplError> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let read = handle.raft.storage().begin_read()?;
            if read.get(RAFT_STATE_TABLE, b"last_membership")?.is_some() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(ReplError::Transport(
                    "timed out waiting for durable CP membership metadata".to_owned(),
                ));
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn change_membership_on_current_leader<S: StorageEngine>(
        handles: &[CpClusterHandle<S>],
        voters: &[RaftNode],
        learners: &[RaftNode],
    ) -> Result<CpClusterStatus, ReplError> {
        let deadline = Instant::now() + Duration::from_secs(45);
        loop {
            let leader_idx = wait_for_live_leader(handles)?;
            match change_membership(&handles[leader_idx], voters.to_vec(), learners.to_vec()) {
                Ok(status) => return Ok(status),
                Err(ReplError::Transport(message)) if is_retryable_live_raft_error(&message) => {}
                Err(error) => return Err(error),
            }
            if Instant::now() >= deadline {
                return Err(ReplError::Transport(
                    "timed out applying membership on current leader".to_owned(),
                ));
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn propose_on_current_leader<S: StorageEngine>(
        handles: &[CpClusterHandle<S>],
        op: &Op,
    ) -> Result<usize, ReplError> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let leader_idx = wait_for_live_leader(handles)?;
            match handles[leader_idx].replication().propose(op.clone()) {
                Ok(()) => return Ok(leader_idx),
                Err(ReplError::Transport(message)) if is_retryable_live_raft_error(&message) => {}
                Err(error) => return Err(error),
            }
            if Instant::now() >= deadline {
                return Err(ReplError::Transport(
                    "timed out proposing on current leader".to_owned(),
                ));
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn is_retryable_live_raft_error(message: &str) -> bool {
        message.contains("forward request")
            || message.contains("client_write timed out")
            || message.contains("change_membership timed out")
    }

    fn start_live_redb_cluster(
        configs: &[CpClusterConfig],
        paths: &[std::path::PathBuf],
    ) -> Result<Vec<CpClusterHandle<RedbEngine>>, ReplError> {
        configs
            .iter()
            .zip(paths)
            .map(|(config, path)| {
                let engine = open_redb_for_live_test(path)?;
                start_cp_cluster(engine, config.clone())
            })
            .collect()
    }

    fn open_redb_for_live_test(path: &std::path::Path) -> Result<RedbEngine, ReplError> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match RedbEngine::open(path) {
                Ok(engine) => return Ok(engine),
                Err(error) if Instant::now() >= deadline => return Err(error.into()),
                Err(_) => thread::sleep(Duration::from_millis(50)),
            }
        }
    }

    fn non_live_cluster_handle() -> Result<CpClusterHandle<MemEngine>, ReplError> {
        Ok(CpClusterHandle {
            raft: Arc::new(CpRaft::new(MemEngine::new(), three_voter_config())?),
        })
    }

    #[test]
    fn validates_voter_counts() {
        assert!(validate_cp_cluster_config(&three_voter_config(), false).is_ok());

        let invalid = CpClusterConfig::new(
            1,
            "127.0.0.1:7001",
            vec![
                RaftNode::new(1, "127.0.0.1:7001"),
                RaftNode::new(2, "127.0.0.1:7002"),
                RaftNode::new(3, "127.0.0.1:7003"),
                RaftNode::new(4, "127.0.0.1:7004"),
            ],
        )
        .with_transport(InternalTransportConfig::new(
            "127.0.0.1:7001",
            InternalTransportSecurity::PlaintextForTests,
        ));
        assert!(validate_cp_cluster_config(&invalid, false).is_err());
    }

    #[test]
    fn single_node_test_helper_reads_and_writes() -> Result<(), Box<dyn std::error::Error>> {
        let raft = CpRaft::new_for_test(MemEngine::new(), 1);

        raft.propose(Op::Put {
            table: "t".to_owned(),
            key: b"a".to_vec(),
            value: b"1".to_vec(),
        })?;

        assert_eq!(
            raft.read("t", b"a", ReadConsistency::Strong)?,
            Some(b"1".to_vec())
        );

        raft.propose(Op::Delete {
            table: "t".to_owned(),
            key: b"a".to_vec(),
        })?;

        assert_eq!(raft.read("t", b"a", ReadConsistency::Strong)?, None);
        Ok(())
    }

    #[test]
    fn strong_writes_require_quorum() -> Result<(), Box<dyn std::error::Error>> {
        let raft = CpRaft::new(MemEngine::new(), three_voter_config())?;
        raft.set_available_voters_for_tests([1])?;

        assert!(matches!(
            raft.propose(Op::Put {
                table: "t".to_owned(),
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            }),
            Err(ReplError::NoQuorum)
        ));

        assert_eq!(raft.read("t", b"a", ReadConsistency::Bounded)?, None);
        assert!(matches!(
            raft.read("t", b"a", ReadConsistency::Strong),
            Err(ReplError::NoQuorum)
        ));

        raft.set_available_voters_for_tests([1, 2])?;
        raft.propose(Op::Put {
            table: "t".to_owned(),
            key: b"a".to_vec(),
            value: b"1".to_vec(),
        })?;

        assert_eq!(
            raft.read("t", b"a", ReadConsistency::Strong)?,
            Some(b"1".to_vec())
        );
        Ok(())
    }

    #[test]
    fn command_log_and_last_applied_are_persistent() -> Result<(), Box<dyn std::error::Error>> {
        let raft = CpRaft::new_for_test(MemEngine::new(), 1);
        raft.propose(Op::Put {
            table: "t".to_owned(),
            key: b"a".to_vec(),
            value: b"1".to_vec(),
        })?;

        let read = raft.storage().begin_read()?;
        assert!(read.get(RAFT_LOG_TABLE, &1_u64.to_be_bytes())?.is_some());
        assert_eq!(
            read.get(RAFT_STATE_TABLE, b"last_applied")?,
            Some(1_u64.to_be_bytes().to_vec())
        );
        assert_eq!(
            read.get(RAFT_STATE_TABLE, b"last_committed")?,
            Some(1_u64.to_be_bytes().to_vec())
        );

        Ok(())
    }

    #[test]
    fn cluster_api_reports_status_and_recovery() -> Result<(), Box<dyn std::error::Error>> {
        let _guard = live_cluster_test_guard();
        let handles = start_live_cluster(3)?;
        let result =
            (|| {
                let leader_idx = propose_on_current_leader(
                    &handles,
                    &Op::Put {
                        table: "t".to_owned(),
                        key: b"a".to_vec(),
                        value: b"1".to_vec(),
                    },
                )?;
                let handle = &handles[leader_idx];

                let status = cluster_status(handle)?;
                assert_eq!(status.leader_id, Some(status.node_id));
                assert_eq!(status.voters, vec![1, 2, 3]);
                assert_eq!(status.quorum_size, 2);
                assert!(status.reachable_voters >= 2);
                assert!(status.last_committed >= 1);
                assert!(status.last_applied >= 1);
                assert!(status.nodes.iter().any(|node| {
                    node.id == status.node_id && node.role == CpClusterRole::Leader
                }));

                let recovery = wait_for_recovery(handle)?;
                assert!(recovery.last_committed >= 1);
                assert_eq!(recovery.in_doubt_dist_txns, 0);
                Ok::<(), Box<dyn std::error::Error>>(())
            })();
        shutdown_all(handles);
        result
    }

    #[test]
    fn cluster_recovery_replays_durable_dist_txn_decision() -> Result<(), Box<dyn std::error::Error>>
    {
        let _guard = live_cluster_test_guard();
        let handles = start_live_cluster(3)?;
        let result = (|| {
            let leader_idx = wait_for_live_leader(&handles)?;
            let handle = &handles[leader_idx];
            let raft = handle.replication();
            assert_eq!(
                raft.prepare_dist_txn(
                    42,
                    vec![Op::Put {
                        table: "t".to_owned(),
                        key: b"k".to_vec(),
                        value: b"v".to_vec(),
                    }],
                )?,
                Vote::Yes
            );
            {
                let mut write = raft.storage().begin_write()?;
                write.put(
                    DIST_TXN_COORDINATOR_TABLE,
                    &dist_txn::txn_key(42),
                    &dist_txn::encode_decision(&dist_txn::CoordinatorDecisionRecord::new(
                        42,
                        Decision::Commit,
                        100,
                    ))?,
                )?;
                write.commit()?;
            }

            let recovery = wait_for_recovery(handle)?;
            assert_eq!(recovery.in_doubt_dist_txns, 0);
            assert_eq!(
                raft.read("t", b"k", ReadConsistency::Strong)?,
                Some(b"v".to_vec())
            );
            Ok::<(), Box<dyn std::error::Error>>(())
        })();
        shutdown_all(handles);
        result
    }

    #[test]
    fn cluster_api_applies_membership_through_live_openraft()
    -> Result<(), Box<dyn std::error::Error>> {
        let _guard = live_cluster_test_guard();
        let handles = start_live_cluster(4)?;
        let result = (|| {
            let leader_idx = wait_for_live_leader(&handles)?;
            let handle = &handles[leader_idx];
            wait_for_committed_live_config(handle)?;
            let leader_id = handle.raft.cluster.node_id;
            let mut next_voters = handles[0]
                .raft
                .cluster
                .voters
                .iter()
                .filter(|node| node.id == leader_id || node.id == 1)
                .cloned()
                .collect::<Vec<_>>();
            if next_voters.len() < 2 {
                let Some(second_voter) = handles[0]
                    .raft
                    .cluster
                    .voters
                    .iter()
                    .find(|node| node.id != leader_id)
                else {
                    return Err(ReplError::Transport("missing second voter".to_owned()).into());
                };
                next_voters.push(second_voter.clone());
            }
            next_voters.push(RaftNode::new(4, handles[3].raft.cluster.bind_addr.clone()));
            next_voters.sort_by_key(|node| node.id);
            let expected_voters = next_voters.iter().map(|node| node.id).collect::<Vec<_>>();
            let status = change_membership_on_current_leader(&handles, &next_voters, &[])?;

            assert_eq!(status.voters, expected_voters);
            assert_eq!(status.quorum_size, 2);
            assert!(status.nodes.iter().any(|node| node.id == 4 && node.voter));
            Ok::<(), Box<dyn std::error::Error>>(())
        })();
        shutdown_all(handles);
        result
    }

    #[test]
    #[ignore = "phase45 Cluster GA smoke runs through scripts/cluster-ga-smoke.ps1"]
    fn phase45_cluster_ga_transfers_leader_and_preserves_writes()
    -> Result<(), Box<dyn std::error::Error>> {
        let _guard = live_cluster_test_guard();
        let handles = start_live_cluster(3)?;
        let result = (|| {
            let leader_idx = propose_on_current_leader(
                &handles,
                &Op::Put {
                    table: "t".to_owned(),
                    key: b"before-leader-change".to_vec(),
                    value: b"committed".to_vec(),
                },
            )?;
            let leader_node = handles[leader_idx].raft.cluster.node_id;
            let leader = &handles[leader_idx];
            wait_for_live_value(
                leader,
                b"before-leader-change",
                b"committed",
                ReadConsistency::Strong,
            )?;

            let target_node = handles
                .iter()
                .map(|handle| handle.raft.cluster.node_id)
                .find(|node_id| *node_id != leader_node)
                .ok_or_else(|| ReplError::Transport("missing transfer target".to_owned()))?;
            transfer_leader(leader, target_node)?;

            let new_leader_idx = wait_for_live_leader_change(&handles, leader_node)?;
            let new_leader = &handles[new_leader_idx];
            assert_ne!(new_leader.raft.cluster.node_id, leader_node);
            let new_leader_idx = propose_on_current_leader(
                &handles,
                &Op::Put {
                    table: "t".to_owned(),
                    key: b"after-leader-change".to_vec(),
                    value: b"still-committed".to_vec(),
                },
            )?;
            let new_leader = &handles[new_leader_idx];
            wait_for_live_value(
                new_leader,
                b"before-leader-change",
                b"committed",
                ReadConsistency::Strong,
            )?;
            wait_for_live_value(
                new_leader,
                b"after-leader-change",
                b"still-committed",
                ReadConsistency::Strong,
            )?;
            Ok::<(), Box<dyn std::error::Error>>(())
        })();
        shutdown_all(handles);
        result
    }

    #[test]
    #[ignore = "phase45 Cluster GA smoke runs through scripts/cluster-ga-smoke.ps1"]
    fn phase45_cluster_ga_rejects_minority_write_after_quorum_loss()
    -> Result<(), Box<dyn std::error::Error>> {
        let _guard = live_cluster_test_guard();
        let mut handles = start_live_cluster(3)?;
        let result = (|| {
            let leader_idx = wait_for_live_leader(&handles)?;
            let leader_node = handles[leader_idx].raft.cluster.node_id;
            let mut stopped = Vec::new();
            while handles.len() > 1 {
                let Some(remove_idx) = handles
                    .iter()
                    .position(|handle| handle.raft.cluster.node_id != leader_node)
                else {
                    return Err(ReplError::Transport("missing non-leader voter".to_owned()).into());
                };
                stopped.push(handles.remove(remove_idx));
            }

            for handle in stopped {
                shutdown_cp_cluster(handle)?;
            }

            let survivor = &handles[0];
            let result = survivor.replication().propose(Op::Put {
                table: "t".to_owned(),
                key: b"minority".to_vec(),
                value: b"must-not-commit".to_vec(),
            });
            assert!(result.is_err(), "minority write unexpectedly committed");
            assert_eq!(
                survivor
                    .replication()
                    .read("t", b"minority", ReadConsistency::Bounded)?,
                None
            );
            Ok::<(), Box<dyn std::error::Error>>(())
        })();
        shutdown_all(handles);
        result
    }

    #[test]
    #[ignore = "phase45 Cluster GA smoke runs through scripts/cluster-ga-smoke.ps1"]
    fn phase45_cluster_ga_persists_membership_metadata_and_read_index()
    -> Result<(), Box<dyn std::error::Error>> {
        let _guard = live_cluster_test_guard();
        let temp_dir = tempfile::tempdir()?;
        let configs = live_cluster_configs(4);
        let paths = (0..4)
            .map(|idx| {
                temp_dir
                    .path()
                    .join(format!("membership-node-{}.redb", idx + 1))
            })
            .collect::<Vec<_>>();
        let handles = start_live_redb_cluster(&configs, &paths)?;
        let result = (|| {
            let leader_idx = wait_for_live_leader(&handles)?;
            let handle = &handles[leader_idx];
            wait_for_committed_live_config(handle)?;
            let leader_id = handle.raft.cluster.node_id;
            let mut next_voters = handles[0]
                .raft
                .cluster
                .voters
                .iter()
                .filter(|node| node.id == leader_id || node.id == 1)
                .cloned()
                .collect::<Vec<_>>();
            if next_voters.len() < 2 {
                let Some(second_voter) = handles[0]
                    .raft
                    .cluster
                    .voters
                    .iter()
                    .find(|node| node.id != leader_id)
                else {
                    return Err(ReplError::Transport("missing second voter".to_owned()).into());
                };
                next_voters.push(second_voter.clone());
            }
            let promoted = RaftNode::new(4, handles[3].raft.cluster.bind_addr.clone());
            next_voters.push(promoted);
            next_voters.sort_by_key(|node| node.id);
            let expected_voters = next_voters.iter().map(|node| node.id).collect::<Vec<_>>();
            let next_voter_ids = next_voters
                .iter()
                .map(|node| node.id)
                .collect::<BTreeSet<_>>();
            let next_learners = handles[0]
                .raft
                .cluster
                .voters
                .iter()
                .filter(|node| !next_voter_ids.contains(&node.id))
                .cloned()
                .collect::<Vec<_>>();
            let status =
                change_membership_on_current_leader(&handles, &next_voters, &next_learners)?;
            assert_eq!(status.voters, expected_voters);
            wait_for_live_membership_metadata(&handles[leader_idx])?;

            let write_leader_idx = propose_on_current_leader(
                &handles,
                &Op::Put {
                    table: "t".to_owned(),
                    key: b"read-index".to_vec(),
                    value: b"fresh".to_vec(),
                },
            )?;
            wait_for_live_value(
                &handles[write_leader_idx],
                b"read-index",
                b"fresh",
                ReadConsistency::Strong,
            )?;
            Ok::<(), Box<dyn std::error::Error>>(())
        })();
        shutdown_all(handles);
        result
    }

    #[test]
    fn cluster_api_rejects_remote_leader_transfer_without_live_worker()
    -> Result<(), Box<dyn std::error::Error>> {
        let handle = non_live_cluster_handle()?;

        let error = match transfer_leader(&handle, 2) {
            Ok(status) => panic!("remote transfer must fail closed: {status:?}"),
            Err(error) => error,
        };
        assert!(matches!(error, ReplError::Unsupported(_)));
        assert_eq!(cluster_status(&handle)?.leader_id, Some(1));
        Ok(())
    }
}
