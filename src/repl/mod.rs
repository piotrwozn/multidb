use std::{
    sync::{Arc, Condvar, Mutex},
    thread,
    time::{Duration, Instant},
};

use crate::observability;
use crate::storage::{Bytes, ReadTransaction, StorageEngine, StorageError, WriteTransaction};
use crate::txn::{self, TxnId, WriteSet};

pub mod ap;
pub mod cp;
mod cp_live;
pub mod dist_txn;
pub mod healing;
pub mod shard;

pub use ap::{
    AP_HINTS_TABLE, AP_MERKLE_TABLE, AP_VERSIONS_TABLE, ApClusterConfig, ApDynamo, ApNode,
    ApTransport, ClockOrdering, ConflictStrategy, ConsistencyLevel, VectorClock, VersionedBytes,
    VersionedRecord, VersionedWrite, validate_ap_cluster_config,
};
pub use cp::{
    ClusterBootstrap, CpClusterConfig, CpClusterHandle, CpClusterNodeStatus, CpClusterRole,
    CpClusterStatus, CpRaft, CpRecoveryStatus, NodeId, RaftNode, ReplCommand, change_membership,
    cluster_status, shutdown_cp_cluster, start_cp_cluster, transfer_leader,
    validate_cp_cluster_config, wait_for_recovery,
};
pub use dist_txn::{
    CoordinatorDecisionRecord, CoordinatorLog, DEFAULT_DIST_TXN_DEADLINE_MS,
    DIST_TXN_COORDINATOR_TABLE, DIST_TXN_FINISHED_TABLE, DIST_TXN_PARTICIPANT_TABLE, Decision,
    DistTxnId, FinishedTxnRecord, InMemoryCoordinatorLog, Participant, PreparedTxnRecord, Vote,
    two_phase_commit,
};
pub use healing::{
    HEALING_EVENTS_TABLE, HEALING_STATE_TABLE, HealingAction, HealingBackend, HealingMode,
    HealingPolicy, HealthConfig, HealthProbe, HealthState, HealthView, ManualHealthProbe,
    NodeHealth, SelfHealingController,
};
pub use shard::{
    DEFAULT_PARTITIONS, MigrationState, PartitionId, PartitionStrategy, SHARD_MIGRATIONS_TABLE,
    ShardId, ShardMap, ShardMigration, ShardedReplication,
};

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Op {
    Put {
        table: String,
        key: Bytes,
        value: Bytes,
    },
    Delete {
        table: String,
        key: Bytes,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum WriteCondition {
    KeyMissing {
        table: String,
        key: Bytes,
    },
    ValueEquals {
        table: String,
        key: Bytes,
        expected: Option<Bytes>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ConditionalBatch {
    pub conditions: Vec<WriteCondition>,
    pub ops: Vec<Op>,
}

impl ConditionalBatch {
    #[must_use]
    pub fn new(conditions: Vec<WriteCondition>, ops: Vec<Op>) -> Self {
        Self { conditions, ops }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ReadConsistency {
    Strong,
    Bounded,
    Eventual,
}

#[derive(thiserror::Error, Debug)]
pub enum ReplError {
    #[error("no quorum")]
    NoQuorum,

    #[error("conflict")]
    Conflict,

    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("transport: {0}")]
    Transport(String),

    #[error("unsupported replication operation: {0}")]
    Unsupported(String),

    #[error(
        "quota exceeded for tenant {tenant}: requested {requested} bytes, remaining {remaining} bytes"
    )]
    QuotaExceeded {
        tenant: String,
        requested: u64,
        remaining: u64,
    },
}

impl ReplError {
    #[must_use]
    pub const fn metric_kind(&self) -> &'static str {
        match self {
            Self::NoQuorum => "no_quorum",
            Self::Conflict => "conflict",
            Self::Storage(_) => "storage",
            Self::Transport(_) => "transport",
            Self::Unsupported(_) => "unsupported",
            Self::QuotaExceeded { .. } => "quota_exceeded",
        }
    }
}

pub trait Replication: Send + Sync {
    /// Proposes one write operation.
    /// # Errors
    /// Fails when the replication backend or storage rejects the write.
    fn propose(&self, op: Op) -> Result<(), ReplError>;

    /// Proposes write operations as one atomic batch.
    /// # Errors
    /// Fails when the replication backend or storage rejects the write batch.
    fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError>;

    #[doc(hidden)]
    /// Proposes internal system writes with crate-local authorization.
    /// # Errors
    /// Fails when the replication backend or storage rejects the write batch.
    fn propose_authorized_batch(
        &self,
        ops: Vec<Op>,
        authorization: txn::WriteAuthorization,
    ) -> Result<(), ReplError>;

    /// Proposes write operations only when every condition still matches.
    /// # Errors
    /// Fails when conditions do not match or the backend does not support conditional writes.
    fn propose_conditional_batch(&self, batch: ConditionalBatch) -> Result<(), ReplError> {
        if batch.conditions.is_empty() {
            self.propose_batch(batch.ops)
        } else {
            Err(ReplError::Unsupported(
                "conditional batches are not supported".to_owned(),
            ))
        }
    }

    /// Persists a prepared distributed transaction participant record.
    /// # Errors
    /// Fails when this backend cannot participate in distributed transactions.
    fn prepare_dist_txn(&self, _txn_id: DistTxnId, _ops: Vec<Op>) -> Result<Vote, ReplError> {
        Err(ReplError::Unsupported(
            "distributed transactions are not supported".to_owned(),
        ))
    }

    /// Applies a durable distributed transaction decision and releases local state.
    /// # Errors
    /// Fails when this backend cannot finish the distributed transaction.
    fn finish_dist_txn(&self, _txn_id: DistTxnId, _decision: Decision) -> Result<(), ReplError> {
        Err(ReplError::Unsupported(
            "distributed transactions are not supported".to_owned(),
        ))
    }

    /// Replays durable in-doubt distributed transaction participant state.
    /// # Errors
    /// Fails when recovery cannot close a local in-doubt transaction.
    fn recover_dist_txns(&self) -> Result<(), ReplError> {
        Ok(())
    }

    /// Reads one key with the requested consistency level.
    /// # Errors
    /// Fails when the replication backend or storage rejects the read.
    fn read(
        &self,
        table: &str,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Option<Bytes>, ReplError>;

    /// Reads a half-open key range with the requested consistency level.
    /// # Errors
    /// Fails when the replication backend or storage rejects the range read.
    fn range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError>;

    /// Scans a half-open key range in batches. Returning `false` from `on_batch`
    /// stops the scan early; `cancelled` is checked between batches.
    /// # Errors
    /// Fails when the replication backend, storage layer, or callback fails.
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
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
        let rows = self.range(table, start, end, consistency)?;
        for batch in rows.chunks(batch_rows.max(1)) {
            if cancelled() || !on_batch(batch)? {
                break;
            }
        }
        Ok(())
    }
}

pub(crate) fn propose_system<R: Replication + ?Sized>(repl: &R, op: Op) -> Result<(), ReplError> {
    propose_system_batch(repl, vec![op])
}

pub(crate) fn propose_system_batch<R: Replication + ?Sized>(
    repl: &R,
    ops: Vec<Op>,
) -> Result<(), ReplError> {
    repl.propose_authorized_batch(ops, txn::system_write_authorization())
}

pub struct SingleNode<S: StorageEngine> {
    storage: S,
    commit_lock: Mutex<()>,
    group_commit: GroupCommitCoordinator,
}

struct GroupCommitCoordinator {
    window: Duration,
    state: Mutex<GroupCommitState>,
}

#[derive(Default)]
struct GroupCommitState {
    leader_active: bool,
    pending: Vec<GroupCommitRequest>,
}

struct GroupCommitRequest {
    ops: Vec<Op>,
    result: Arc<GroupCommitResult>,
}

struct GroupCommitResult {
    value: Mutex<Option<Result<TxnId, StorageError>>>,
    ready: Condvar,
}

impl<S: StorageEngine> SingleNode<S> {
    #[must_use]
    pub fn new(storage: S) -> Self {
        Self::with_group_commit_window(storage, Duration::ZERO)
    }

    #[must_use]
    pub fn with_group_commit_window(storage: S, window: Duration) -> Self {
        Self {
            storage,
            commit_lock: Mutex::new(()),
            group_commit: GroupCommitCoordinator::new(window),
        }
    }

    #[must_use]
    pub const fn storage(&self) -> &S {
        &self.storage
    }

    #[must_use]
    pub fn into_storage(self) -> S {
        self.storage
    }

    /// Reads the latest committed transaction id.
    /// # Errors
    /// Fails when transaction metadata cannot be read.
    pub fn current_txn_id(&self) -> Result<TxnId, StorageError> {
        txn::current_txn_id(&self.storage)
    }

    /// Commits a transaction write set after validating it against a snapshot.
    /// # Errors
    /// Fails with conflict when a written key changed after the snapshot.
    pub fn commit_write_set(
        &self,
        snapshot_id: TxnId,
        write_set: WriteSet,
    ) -> Result<TxnId, StorageError> {
        let _guard = self
            .commit_lock
            .lock()
            .map_err(|_| StorageError::Backend("commit lock poisoned".to_owned()))?;
        txn::commit_write_set(&self.storage, snapshot_id, write_set)
    }

    /// Commits a transaction write set after caller-supplied validation under the commit lock.
    /// # Errors
    /// Fails with conflict when validation or write-write checks reject the commit.
    pub fn commit_write_set_with_preflight<'a, F>(
        &'a self,
        snapshot_id: TxnId,
        write_set: WriteSet,
        preflight: F,
    ) -> Result<TxnId, StorageError>
    where
        F: FnOnce(&mut S::WriteTxn<'a>) -> Result<(), StorageError>,
    {
        let _guard = self
            .commit_lock
            .lock()
            .map_err(|_| StorageError::Backend("commit lock poisoned".to_owned()))?;
        let write = self.storage.begin_write()?;
        txn::commit_write_set_in_txn_with_preflight(write, snapshot_id, write_set, preflight)
    }

    fn commit_ops_at_current(&self, ops: Vec<Op>) -> Result<TxnId, StorageError> {
        self.group_commit
            .commit_ops_at_current(&self.storage, &self.commit_lock, ops)
    }

    fn commit_ops_at_current_authorized(
        &self,
        ops: Vec<Op>,
        authorization: txn::WriteAuthorization,
    ) -> Result<TxnId, StorageError> {
        let _guard = self
            .commit_lock
            .lock()
            .map_err(|_| StorageError::Backend("commit lock poisoned".to_owned()))?;
        txn::commit_ops_at_current_authorized(&self.storage, ops, authorization)
    }

    fn commit_conditional_batch(&self, batch: ConditionalBatch) -> Result<TxnId, StorageError> {
        commit_conditional_batch(&self.storage, &self.commit_lock, batch)
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
    ) -> Result<(), StorageError> {
        let _guard = self
            .commit_lock
            .lock()
            .map_err(|_| StorageError::Backend("commit lock poisoned".to_owned()))?;
        let mut write = self.storage.begin_write()?;
        let key = dist_txn::txn_key(txn_id);
        write.put(
            DIST_TXN_FINISHED_TABLE,
            &key,
            &dist_txn::encode_finished(&FinishedTxnRecord::new(txn_id, decision))?,
        )?;
        write.delete(DIST_TXN_PARTICIPANT_TABLE, &key)?;
        write.commit()
    }
}

impl GroupCommitCoordinator {
    fn new(window: Duration) -> Self {
        Self {
            window,
            state: Mutex::new(GroupCommitState::default()),
        }
    }

    fn commit_ops_at_current<S: StorageEngine>(
        &self,
        storage: &S,
        commit_lock: &Mutex<()>,
        ops: Vec<Op>,
    ) -> Result<TxnId, StorageError> {
        if self.window.is_zero() {
            return commit_ops_direct(storage, commit_lock, ops);
        }

        let result = Arc::new(GroupCommitResult::new());
        let request = GroupCommitRequest {
            ops,
            result: result.clone(),
        };
        let mut state = self
            .state
            .lock()
            .map_err(|_| StorageError::Backend("group commit lock poisoned".to_owned()))?;
        state.pending.push(request);
        if state.leader_active {
            drop(state);
            return result.wait();
        }
        state.leader_active = true;
        drop(state);

        loop {
            thread::sleep(self.window);
            let requests = {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| StorageError::Backend("group commit lock poisoned".to_owned()))?;
                if state.pending.is_empty() {
                    state.leader_active = false;
                    break;
                }
                state.pending.drain(..).collect::<Vec<_>>()
            };

            let started = Instant::now();
            let commit_result = commit_group(storage, commit_lock, requests.iter());
            observability::record_group_commit(requests.len(), started.elapsed());
            for request in &requests {
                request.result.set(clone_commit_result(&commit_result));
            }

            let mut state = self
                .state
                .lock()
                .map_err(|_| StorageError::Backend("group commit lock poisoned".to_owned()))?;
            if state.pending.is_empty() {
                state.leader_active = false;
                break;
            }
        }

        result.wait()
    }
}

impl GroupCommitResult {
    fn new() -> Self {
        Self {
            value: Mutex::new(None),
            ready: Condvar::new(),
        }
    }

    fn set(&self, value: Result<TxnId, StorageError>) {
        if let Ok(mut guard) = self.value.lock() {
            *guard = Some(value);
            self.ready.notify_all();
        }
    }

    fn wait(&self) -> Result<TxnId, StorageError> {
        let mut guard = self
            .value
            .lock()
            .map_err(|_| StorageError::Backend("group commit result lock poisoned".to_owned()))?;
        loop {
            if let Some(value) = guard.take() {
                return value;
            }
            guard = self
                .ready
                .wait(guard)
                .map_err(|_| StorageError::Backend("group commit wait poisoned".to_owned()))?;
        }
    }
}

fn commit_ops_direct<S: StorageEngine>(
    storage: &S,
    commit_lock: &Mutex<()>,
    ops: Vec<Op>,
) -> Result<TxnId, StorageError> {
    let _guard = commit_lock
        .lock()
        .map_err(|_| StorageError::Backend("commit lock poisoned".to_owned()))?;
    txn::commit_ops_at_current(storage, ops)
}

fn commit_conditional_batch<S: StorageEngine>(
    storage: &S,
    commit_lock: &Mutex<()>,
    batch: ConditionalBatch,
) -> Result<TxnId, StorageError> {
    txn::validate_public_conditions(&batch.conditions)?;
    txn::validate_public_ops(&batch.ops)?;
    let _guard = commit_lock
        .lock()
        .map_err(|_| StorageError::Backend("commit lock poisoned".to_owned()))?;

    {
        let read = storage.begin_read()?;
        for condition in &batch.conditions {
            if !condition_matches(&read, condition)? {
                return Err(StorageError::Conflict);
            }
        }
    }

    txn::commit_write_set_at_current(storage, txn::ops_to_write_set(batch.ops))
}

pub(crate) fn condition_matches<T: ReadTransaction>(
    read: &T,
    condition: &WriteCondition,
) -> Result<bool, StorageError> {
    match condition {
        WriteCondition::KeyMissing { table, key } => Ok(read.get(table, key)?.is_none()),
        WriteCondition::ValueEquals {
            table,
            key,
            expected,
        } => Ok(read.get(table, key)? == *expected),
    }
}

fn commit_group<'a, S: StorageEngine>(
    storage: &S,
    commit_lock: &Mutex<()>,
    requests: impl Iterator<Item = &'a GroupCommitRequest>,
) -> Result<TxnId, StorageError> {
    let ops = requests
        .flat_map(|request| request.ops.iter().cloned())
        .collect::<Vec<_>>();
    let _guard = commit_lock
        .lock()
        .map_err(|_| StorageError::Backend("commit lock poisoned".to_owned()))?;
    txn::commit_write_set_at_current(storage, txn::ops_to_write_set(ops))
}

fn clone_commit_result(result: &Result<TxnId, StorageError>) -> Result<TxnId, StorageError> {
    match result {
        Ok(txn_id) => Ok(*txn_id),
        Err(error) => Err(clone_storage_error(error)),
    }
}

fn clone_storage_error(error: &StorageError) -> StorageError {
    match error {
        StorageError::NotFound => StorageError::NotFound,
        StorageError::Io(error) => StorageError::Backend(error.to_string()),
        StorageError::Corruption(message) => StorageError::Corruption(message.clone()),
        StorageError::Conflict => StorageError::Conflict,
        StorageError::Backend(message) => StorageError::Backend(message.clone()),
    }
}

impl<S: StorageEngine> Replication for SingleNode<S> {
    fn propose(&self, op: Op) -> Result<(), ReplError> {
        self.propose_batch(vec![op])
    }

    fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
        self.commit_ops_at_current(ops)?;
        Ok(())
    }

    fn propose_authorized_batch(
        &self,
        ops: Vec<Op>,
        authorization: txn::WriteAuthorization,
    ) -> Result<(), ReplError> {
        self.commit_ops_at_current_authorized(ops, authorization)?;
        Ok(())
    }

    fn propose_conditional_batch(&self, batch: ConditionalBatch) -> Result<(), ReplError> {
        match self.commit_conditional_batch(batch) {
            Ok(_) => Ok(()),
            Err(StorageError::Conflict) => Err(ReplError::Conflict),
            Err(error) => Err(error.into()),
        }
    }

    fn prepare_dist_txn(&self, txn_id: DistTxnId, ops: Vec<Op>) -> Result<Vote, ReplError> {
        txn::validate_public_ops(&ops)?;
        let _guard = self
            .commit_lock
            .lock()
            .map_err(|_| StorageError::Backend("commit lock poisoned".to_owned()))?;
        let mut write = self.storage.begin_write()?;
        let key = dist_txn::txn_key(txn_id);

        if write.get(DIST_TXN_FINISHED_TABLE, &key)?.is_some() {
            write.commit()?;
            return Ok(Vote::Yes);
        }

        if let Some(existing) = write.get(DIST_TXN_PARTICIPANT_TABLE, &key)? {
            let existing = dist_txn::decode_prepared(&existing)?;
            write.commit()?;
            return Ok(if existing.ops == ops {
                Vote::Yes
            } else {
                Vote::No
            });
        }

        let record = PreparedTxnRecord::new(txn_id, ops, DEFAULT_DIST_TXN_DEADLINE_MS);
        write.put(
            DIST_TXN_PARTICIPANT_TABLE,
            &key,
            &dist_txn::encode_prepared(&record)?,
        )?;
        write.commit()?;
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
        if let (Decision::Commit, Some(record)) = (decision, prepared) {
            self.commit_ops_at_current_authorized(record.ops, txn::system_write_authorization())?;
        }
        self.mark_dist_txn_finished(txn_id, decision)?;
        Ok(())
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
        _consistency: ReadConsistency,
    ) -> Result<Option<Bytes>, ReplError> {
        let txn = self.storage.begin_read()?;
        Ok(txn.get(table, key)?)
    }

    fn range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        _consistency: ReadConsistency,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
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
        _consistency: ReadConsistency,
        batch_rows: usize,
        cancelled: &dyn Fn() -> bool,
        on_batch: &mut dyn FnMut(&[(Bytes, Bytes)]) -> Result<bool, ReplError>,
    ) -> Result<(), ReplError> {
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

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Barrier},
        thread,
        time::Duration,
    };

    use super::{
        ConditionalBatch, Op, ReadConsistency, ReplError, Replication, SingleNode, WriteCondition,
    };
    use crate::storage::{MemEngine, RedbEngine};

    fn run_single_node<S>(node: &SingleNode<S>) -> Result<(), Box<dyn std::error::Error>>
    where
        S: crate::storage::StorageEngine,
    {
        node.propose(Op::Put {
            table: "t".to_owned(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        })?;

        assert_eq!(
            node.read("t", b"k", ReadConsistency::Strong)?,
            Some(b"v".to_vec())
        );

        node.propose(Op::Delete {
            table: "t".to_owned(),
            key: b"k".to_vec(),
        })?;

        assert_eq!(node.read("t", b"k", ReadConsistency::Eventual)?, None);
        assert_eq!(node.read("t", b"missing", ReadConsistency::Bounded)?, None);

        Ok(())
    }

    #[test]
    fn single_node_memory() -> Result<(), Box<dyn std::error::Error>> {
        run_single_node(&SingleNode::new(MemEngine::new()))
    }

    #[test]
    fn single_node_redb() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("single-node.redb");
        let engine = RedbEngine::open(path)?;

        run_single_node(&SingleNode::new(engine))
    }

    #[test]
    fn single_node_batch_is_atomic_on_error() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("atomic-batch.redb");
        let node = SingleNode::new(RedbEngine::open(path)?);

        assert!(
            node.propose_batch(vec![
                Op::Put {
                    table: "t".to_owned(),
                    key: b"a".to_vec(),
                    value: b"1".to_vec(),
                },
                Op::Put {
                    table: String::new(),
                    key: b"bad".to_vec(),
                    value: b"2".to_vec(),
                },
            ])
            .is_err()
        );

        assert_eq!(node.read("t", b"a", ReadConsistency::Strong)?, None);

        Ok(())
    }

    #[test]
    fn conditional_batch_conflicts_when_condition_fails() -> Result<(), Box<dyn std::error::Error>>
    {
        let node = SingleNode::new(MemEngine::new());
        node.propose_conditional_batch(ConditionalBatch::new(
            vec![WriteCondition::KeyMissing {
                table: "t".to_owned(),
                key: b"k".to_vec(),
            }],
            vec![Op::Put {
                table: "t".to_owned(),
                key: b"k".to_vec(),
                value: b"first".to_vec(),
            }],
        ))?;

        assert!(matches!(
            node.propose_conditional_batch(ConditionalBatch::new(
                vec![WriteCondition::KeyMissing {
                    table: "t".to_owned(),
                    key: b"k".to_vec(),
                }],
                vec![Op::Put {
                    table: "t".to_owned(),
                    key: b"k".to_vec(),
                    value: b"second".to_vec(),
                }],
            )),
            Err(ReplError::Conflict)
        ));
        assert_eq!(
            node.read("t", b"k", ReadConsistency::Strong)?,
            Some(b"first".to_vec())
        );

        Ok(())
    }

    #[test]
    fn single_node_range_reads_half_open_ranges() -> Result<(), Box<dyn std::error::Error>> {
        let node = SingleNode::new(MemEngine::new());
        node.propose_batch(vec![
            Op::Put {
                table: "t".to_owned(),
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            },
            Op::Put {
                table: "t".to_owned(),
                key: b"b".to_vec(),
                value: b"2".to_vec(),
            },
            Op::Put {
                table: "t".to_owned(),
                key: b"c".to_vec(),
                value: b"3".to_vec(),
            },
        ])?;

        let rows = node.range("t", b"a", b"c", ReadConsistency::Strong)?;
        assert_eq!(
            rows.into_iter().map(|(key, _)| key).collect::<Vec<_>>(),
            vec![b"a".to_vec(), b"b".to_vec()]
        );

        Ok(())
    }

    #[test]
    fn group_commit_batches_concurrent_proposals() -> Result<(), Box<dyn std::error::Error>> {
        let node = Arc::new(SingleNode::with_group_commit_window(
            MemEngine::new(),
            Duration::from_millis(25),
        ));
        let barrier = Arc::new(Barrier::new(5));
        let mut handles = Vec::new();

        for id in 0..4 {
            let node = Arc::clone(&node);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                node.propose(Op::Put {
                    table: "t".to_owned(),
                    key: vec![id],
                    value: vec![id + 10],
                })
            }));
        }

        barrier.wait();
        for handle in handles {
            match handle.join() {
                Ok(result) => result?,
                Err(_) => return Err(std::io::Error::other("writer panicked").into()),
            }
        }

        assert_eq!(node.current_txn_id()?, 1);
        for id in 0..4 {
            assert_eq!(
                node.read("t", &[id], ReadConsistency::Strong)?,
                Some(vec![id + 10])
            );
        }

        Ok(())
    }
}
