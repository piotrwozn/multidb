use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

use super::*;
use crate::{
    repl::{
        ConditionalBatch, CpRaft, ReadConsistency, Replication, SingleNode, WriteCondition,
        propose_system_conditional_batch,
    },
    storage::{MemEngine, RangeIter, ReadTransaction, RedbEngine, StorageEngine, WriteTransaction},
    txn,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn put(key: &[u8], value: &[u8]) -> Op {
    Op::Put {
        table: "rows".to_owned(),
        key: key.to_vec(),
        value: value.to_vec(),
    }
}

#[allow(clippy::needless_pass_by_value)] // Assertions intentionally consume each fallible operation's result.
fn conflict<T>(result: Result<T, ReplError>) -> bool {
    matches!(
        result,
        Err(ReplError::Conflict | ReplError::Storage(StorageError::Conflict))
    )
}

fn participant_invariants(node: &dyn Replication) -> TestResult {
    node.propose_batch(vec![put(b"a", b"old"), put(b"b", b"delete")])?;
    let ops = vec![
        put(b"a", b"new"),
        Op::Delete {
            table: "rows".to_owned(),
            key: b"b".to_vec(),
        },
    ];
    assert_eq!(node.prepare_dist_txn(7, ops.clone())?, Vote::Yes);
    assert_eq!(node.prepare_dist_txn(7, ops.clone())?, Vote::Yes);
    assert_eq!(
        node.prepare_dist_txn(8, vec![put(b"a", b"competing")])?,
        Vote::No
    );
    assert!(conflict(node.propose(put(b"a", b"lost"))));
    assert!(conflict(node.propose_conditional_batch(
        ConditionalBatch::new(
            vec![WriteCondition::ValueEquals {
                table: "rows".to_owned(),
                key: b"a".to_vec(),
                expected: Some(b"old".to_vec())
            }],
            vec![put(b"a", b"lost")],
        )
    )));
    assert!(conflict(node.read("rows", b"a", ReadConsistency::Strong)));
    assert!(conflict(node.read_many(
        "rows",
        &[b"a".to_vec()],
        ReadConsistency::Strong
    )));
    assert!(conflict(node.range(
        "rows",
        &[],
        &[],
        ReadConsistency::Strong
    )));
    assert_eq!(node.table_data_version("rows")?, None);
    node.propose(put(b"unrelated", b"ok"))?;
    node.recover_dist_txns()?;
    assert!(
        conflict(node.read("rows", b"a", ReadConsistency::Strong)),
        "an absent coordinator decision must stay in doubt"
    );
    node.finish_dist_txn(7, Decision::Commit)?;
    assert_eq!(
        node.read("rows", b"a", ReadConsistency::Strong)?,
        Some(b"new".to_vec())
    );
    assert_eq!(node.read("rows", b"b", ReadConsistency::Strong)?, None);
    assert!(node.table_data_version("rows")?.is_some());
    assert_eq!(node.prepare_dist_txn(7, ops)?, Vote::No);
    assert!(node.finish_dist_txn(7, Decision::Abort).is_err());
    node.propose(put(b"a", b"later"))?;
    node.finish_dist_txn(7, Decision::Commit)?;
    assert_eq!(
        node.read("rows", b"a", ReadConsistency::Strong)?,
        Some(b"later".to_vec())
    );
    assert_eq!(
        node.prepare_dist_txn(9, vec![put(b"a", b"never")])?,
        Vote::Yes
    );
    node.finish_dist_txn(9, Decision::Abort)?;
    node.finish_dist_txn(9, Decision::Abort)?;
    assert_eq!(
        node.read("rows", b"a", ReadConsistency::Strong)?,
        Some(b"later".to_vec())
    );
    assert_eq!(node.prepare_dist_txn(9, Vec::new())?, Vote::No);
    Ok(())
}

#[test]
fn native_participants_keep_key_locks_and_immutable_terminal_decisions() -> TestResult {
    participant_invariants(&SingleNode::new(MemEngine::new()))?;
    participant_invariants(&CpRaft::new_for_test(MemEngine::new(), 1))
}

#[derive(Default)]
struct FaultEngine {
    inner: MemEngine,
    commits: AtomicUsize,
    fail_at: AtomicUsize,
    before_write: Mutex<Option<Vec<Op>>>,
}

struct FaultWriter<'a> {
    inner: <MemEngine as StorageEngine>::WriteTxn<'a>,
    engine: &'a FaultEngine,
}

impl StorageEngine for FaultEngine {
    type ReadTxn<'a> = <MemEngine as StorageEngine>::ReadTxn<'a>;
    type WriteTxn<'a> = FaultWriter<'a>;
    fn begin_read(&self) -> Result<Self::ReadTxn<'_>, StorageError> {
        self.inner.begin_read()
    }
    fn begin_write(&self) -> Result<Self::WriteTxn<'_>, StorageError> {
        let injected = self
            .before_write
            .lock()
            .map_err(|_| StorageError::Backend("poisoned test lock".to_owned()))?
            .take();
        if let Some(ops) = injected {
            let mut write = self.inner.begin_write()?;
            for op in ops {
                match op {
                    Op::Put { table, key, value } => write.put(&table, &key, &value)?,
                    Op::Delete { table, key } => write.delete(&table, &key)?,
                }
            }
            write.commit()?;
        }
        Ok(FaultWriter {
            inner: self.inner.begin_write()?,
            engine: self,
        })
    }
}

impl ReadTransaction for FaultWriter<'_> {
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
        self.inner.get(table, key)
    }
    fn range<'a>(
        &'a self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RangeIter<'a>, StorageError> {
        self.inner.range(table, start, end)
    }
}

impl WriteTransaction for FaultWriter<'_> {
    fn put(&mut self, table: &str, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.inner.put(table, key, value)
    }
    fn delete(&mut self, table: &str, key: &[u8]) -> Result<(), StorageError> {
        self.inner.delete(table, key)
    }
    fn commit(self) -> Result<(), StorageError> {
        let number = self.engine.commits.fetch_add(1, Ordering::SeqCst) + 1;
        if number == self.engine.fail_at.load(Ordering::SeqCst) {
            return Err(StorageError::Backend("injected commit failure".to_owned()));
        }
        self.inner.commit()
    }
    fn rollback(self) {
        self.inner.rollback();
    }
}

#[test]
fn participant_finish_is_one_atomic_commit_and_failure_keeps_prepare() -> TestResult {
    let node = SingleNode::new(FaultEngine::default());
    node.propose(put(b"a", b"old"))?;
    node.prepare_dist_txn(1, vec![put(b"a", b"new")])?;
    let before = node.storage().commits.load(Ordering::SeqCst);
    node.storage().fail_at.store(before + 1, Ordering::SeqCst);
    assert!(node.finish_dist_txn(1, Decision::Commit).is_err());
    let read = node.storage().begin_read()?;
    assert_eq!(read.get("rows", b"a")?, Some(b"old".to_vec()));
    assert!(read.get(DIST_TXN_PARTICIPANT_TABLE, &txn_key(1))?.is_some());
    assert_eq!(read.get(DIST_TXN_FINISHED_TABLE, &txn_key(1))?, None);
    drop(read);
    let before = node.storage().commits.load(Ordering::SeqCst);
    node.storage().fail_at.store(before + 2, Ordering::SeqCst);
    node.finish_dist_txn(1, Decision::Commit)?;
    assert_eq!(node.storage().commits.load(Ordering::SeqCst), before + 1);
    let read = node.storage().begin_read()?;
    assert_eq!(read.get("rows", b"a")?, Some(b"new".to_vec()));
    assert!(read.get(DIST_TXN_FINISHED_TABLE, &txn_key(1))?.is_some());
    assert_eq!(read.get(DIST_TXN_PARTICIPANT_TABLE, &txn_key(1))?, None);
    Ok(())
}

#[test]
fn conditional_checks_use_the_writer_snapshot_not_an_earlier_read() -> TestResult {
    let node = SingleNode::new(FaultEngine::default());
    node.propose(put(b"a", b"old"))?;
    *node
        .storage()
        .before_write
        .lock()
        .map_err(|_| "poisoned test lock")? = Some(vec![put(b"a", b"concurrent")]);
    assert!(conflict(node.propose_conditional_batch(
        ConditionalBatch::new(
            vec![WriteCondition::ValueEquals {
                table: "rows".to_owned(),
                key: b"a".to_vec(),
                expected: Some(b"old".to_vec())
            }],
            vec![put(b"a", b"mine")],
        )
    )));
    assert_eq!(
        node.read("rows", b"a", ReadConsistency::Strong)?,
        Some(b"concurrent".to_vec())
    );
    Ok(())
}

#[test]
fn authorized_conditional_metadata_still_protects_public_entry_points() -> TestResult {
    let single = SingleNode::new(MemEngine::new());
    let cp = CpRaft::new_for_test(MemEngine::new(), 1);
    for node in [&single as &dyn Replication, &cp] {
        let condition = WriteCondition::KeyMissing {
            table: "__private".to_owned(),
            key: b"k".to_vec(),
        };
        let op = Op::Put {
            table: "__private".to_owned(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        let batch = ConditionalBatch::new(vec![condition], vec![op]);
        assert!(node.propose_conditional_batch(batch.clone()).is_err());
        propose_system_conditional_batch(node, batch.clone())?;
        assert!(conflict(propose_system_conditional_batch(node, batch)));
        assert_eq!(
            node.read("__private", b"k", ReadConsistency::Strong)?,
            Some(b"v".to_vec())
        );
    }
    Ok(())
}

#[test]
fn prepared_locks_survive_reopen_and_explicit_transaction_writes_cannot_bypass_them() -> TestResult
{
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("prepared.redb");
    let node = SingleNode::new(RedbEngine::open(&path)?);
    node.propose(put(b"a", b"old"))?;
    node.prepare_dist_txn(41, vec![put(b"a", b"new")])?;
    drop(node);
    let reopened = SingleNode::new(RedbEngine::open(&path)?);
    reopened.recover_dist_txns()?;
    assert!(conflict(reopened.read(
        "rows",
        b"a",
        ReadConsistency::Strong
    )));
    assert!(matches!(
        reopened.commit_write_set(
            reopened.current_txn_id()?,
            txn::ops_to_write_set(vec![put(b"a", b"lost")])
        ),
        Err(StorageError::Conflict)
    ));
    reopened.finish_dist_txn(41, Decision::Commit)?;
    assert_eq!(
        reopened.read("rows", b"a", ReadConsistency::Strong)?,
        Some(b"new".to_vec())
    );
    Ok(())
}

#[test]
fn coordinator_decisions_and_record_versions_fail_closed() -> TestResult {
    let mut log = InMemoryCoordinatorLog::new();
    log.record_decision(1, Decision::Commit)?;
    log.record_decision(1, Decision::Commit)?;
    assert!(conflict(log.record_decision(1, Decision::Abort)));
    assert_eq!(log.decision(1), Some(Decision::Commit));
    let mut record = PreparedTxnRecord::new(1, Vec::new(), 1000);
    record.version = u16::MAX;
    assert!(decode_prepared(&encode_prepared(&record)?).is_err());
    Ok(())
}

struct FailingParticipant {
    vote: Option<Vote>,
    finish_error: bool,
    events: std::cell::RefCell<Vec<String>>,
}

impl Participant for FailingParticipant {
    fn prepare(&self, _: DistTxnId) -> Result<Vote, ReplError> {
        self.events.borrow_mut().push("prepare".to_owned());
        self.vote
            .ok_or_else(|| ReplError::Transport("lost prepare acknowledgement".to_owned()))
    }
    fn finish(&self, _: DistTxnId, decision: Decision) -> Result<(), ReplError> {
        self.events
            .borrow_mut()
            .push(format!("finish:{decision:?}"));
        if self.finish_error {
            return Err(ReplError::Transport(
                "lost finish acknowledgement".to_owned(),
            ));
        }
        Ok(())
    }
}

#[test]
fn coordinator_cleans_up_ambiguous_prepares_and_retries_only_the_durable_decision() -> TestResult {
    let first = FailingParticipant {
        vote: Some(Vote::Yes),
        finish_error: false,
        events: std::cell::RefCell::default(),
    };
    let second = FailingParticipant {
        vote: None,
        finish_error: false,
        events: std::cell::RefCell::default(),
    };
    let mut log = InMemoryCoordinatorLog::new();
    assert!(two_phase_commit(1, &[&first, &second], &mut log).is_err());
    assert_eq!(log.decision(1), Some(Decision::Abort));
    assert_eq!(
        first.events.borrow().as_slice(),
        ["prepare", "finish:Abort"]
    );
    assert_eq!(
        second.events.borrow().as_slice(),
        ["prepare", "finish:Abort"]
    );
    assert_eq!(
        two_phase_commit(1, &[&first, &second], &mut log)?,
        Decision::Abort
    );
    assert_eq!(
        first.events.borrow().as_slice(),
        ["prepare", "finish:Abort", "finish:Abort"]
    );

    let failed = FailingParticipant {
        vote: Some(Vote::Yes),
        finish_error: true,
        events: std::cell::RefCell::default(),
    };
    let healthy = FailingParticipant {
        vote: Some(Vote::Yes),
        finish_error: false,
        events: std::cell::RefCell::default(),
    };
    assert!(two_phase_commit(2, &[&failed, &healthy], &mut log).is_err());
    assert_eq!(
        healthy.events.borrow().as_slice(),
        ["prepare", "finish:Commit"]
    );
    assert_eq!(log.decision(2), Some(Decision::Commit));
    Ok(())
}

#[test]
fn commit_log_mvcc_and_hlc_decoders_reject_trailing_bytes() -> TestResult {
    let record = txn::CommitLogRecord {
        txn_id: 1,
        committed_at: std::time::UNIX_EPOCH,
        hlc: None,
        writes: Vec::new(),
    };
    let mut bytes = txn::encode_commit_log_record(&record)?;
    bytes.push(0);
    assert!(txn::decode_commit_log_record(&bytes).is_err());
    let mut bytes = txn::encode_mvcc_record(&txn::MvccRecord { value: None })?;
    bytes.push(0);
    assert!(txn::decode_mvcc_record(&bytes).is_err());
    Ok(())
}
