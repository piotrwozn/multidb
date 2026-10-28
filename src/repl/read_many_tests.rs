use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use super::{
    ApDynamo, CpRaft, Op, PartitionStrategy, ReadConsistency, ReplError, Replication, ShardMap,
    ShardedReplication, SingleNode,
};
use crate::{
    storage::{
        Bytes, MemEngine, RangeIter, ReadTransaction, StorageEngine, StorageError, WriteTransaction,
    },
    txn::WriteAuthorization,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Default)]
struct CountedEngine {
    inner: MemEngine,
    read_transactions: AtomicUsize,
    after_get: Mutex<Option<Vec<Op>>>,
}

struct CountedRead<'a> {
    snapshot: <MemEngine as StorageEngine>::ReadTxn<'a>,
    engine: &'a CountedEngine,
}

impl StorageEngine for CountedEngine {
    type ReadTxn<'a> = CountedRead<'a>;
    type WriteTxn<'a> = <MemEngine as StorageEngine>::WriteTxn<'a>;

    fn begin_read(&self) -> Result<Self::ReadTxn<'_>, StorageError> {
        self.read_transactions.fetch_add(1, Ordering::SeqCst);
        Ok(CountedRead {
            snapshot: self.inner.begin_read()?,
            engine: self,
        })
    }

    fn begin_write(&self) -> Result<Self::WriteTxn<'_>, StorageError> {
        self.inner.begin_write()
    }
}

impl ReadTransaction for CountedRead<'_> {
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
        if key == b"injected_read_error" {
            return Err(StorageError::Backend("injected read failure".to_owned()));
        }
        let result = self.snapshot.get(table, key)?;
        let concurrent = self
            .engine
            .after_get
            .lock()
            .map_err(|_| StorageError::Backend("write queue poisoned".to_owned()))?
            .take();
        if let Some(ops) = concurrent {
            // Commit between successive get calls, after the reader captured
            // its snapshot. A per-key transaction loop would mix both versions.
            let mut write = self.engine.inner.begin_write()?;
            for op in ops {
                match op {
                    Op::Put { table, key, value } => write.put(&table, &key, &value)?,
                    Op::Delete { table, key } => write.delete(&table, &key)?,
                }
            }
            write.commit()?;
        }
        Ok(result)
    }

    fn range<'txn>(
        &'txn self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RangeIter<'txn>, StorageError> {
        self.snapshot.range(table, start, end)
    }
}

fn put(key: &[u8], value: &[u8]) -> Op {
    Op::Put {
        table: "rows".to_owned(),
        key: key.to_vec(),
        value: value.to_vec(),
    }
}

fn assert_one_snapshot(repl: &dyn Replication, storage: &CountedEngine) -> TestResult {
    repl.propose_batch(vec![put(b"a", b"old-a"), put(b"b", b"old-b")])?;
    storage.read_transactions.store(0, Ordering::SeqCst);
    assert_eq!(
        repl.read_many("rows", &[], ReadConsistency::Strong)?,
        Vec::<Option<Bytes>>::new()
    );
    assert_eq!(storage.read_transactions.load(Ordering::SeqCst), 0);
    let keys = vec![
        b"b".to_vec(),
        b"missing".to_vec(),
        b"a".to_vec(),
        b"b".to_vec(),
    ];
    assert_eq!(
        repl.read_many("rows", &keys, ReadConsistency::Strong)?,
        vec![
            Some(b"old-b".to_vec()),
            None,
            Some(b"old-a".to_vec()),
            Some(b"old-b".to_vec())
        ]
    );
    assert_eq!(storage.read_transactions.load(Ordering::SeqCst), 1);

    storage.read_transactions.store(0, Ordering::SeqCst);
    *storage
        .after_get
        .lock()
        .map_err(|_| "write queue poisoned")? =
        Some(vec![put(b"a", b"new-a"), put(b"b", b"new-b")]);
    let keys = vec![b"a".to_vec(), b"b".to_vec(), b"a".to_vec()];
    assert_eq!(
        repl.read_many("rows", &keys, ReadConsistency::Strong)?,
        vec![
            Some(b"old-a".to_vec()),
            Some(b"old-b".to_vec()),
            Some(b"old-a".to_vec())
        ]
    );
    assert_eq!(storage.read_transactions.load(Ordering::SeqCst), 1);
    assert_eq!(
        repl.read_many("rows", &keys, ReadConsistency::Strong)?,
        vec![
            Some(b"new-a".to_vec()),
            Some(b"new-b".to_vec()),
            Some(b"new-a".to_vec())
        ]
    );
    Ok(())
}

#[test]
fn single_node_read_many_uses_one_transaction_and_retains_its_snapshot() -> TestResult {
    let node = SingleNode::new(CountedEngine::default());
    assert_one_snapshot(&node, node.storage())
}

#[test]
fn cp_read_many_uses_one_transaction_and_retains_its_snapshot() -> TestResult {
    let node = CpRaft::new_for_test(CountedEngine::default(), 1);
    assert_one_snapshot(&node, node.storage())
}

#[test]
fn native_read_many_propagates_storage_errors_without_partial_results() -> TestResult {
    let node = SingleNode::new(CountedEngine::default());
    node.propose(put(b"a", b"value"))?;
    node.storage().read_transactions.store(0, Ordering::SeqCst);
    let keys = vec![
        b"a".to_vec(),
        b"injected_read_error".to_vec(),
        b"a".to_vec(),
    ];
    assert!(matches!(
        node.read_many("rows", &keys, ReadConsistency::Strong),
        Err(ReplError::Storage(StorageError::Backend(message))) if message == "injected read failure"
    ));
    assert_eq!(node.storage().read_transactions.load(Ordering::SeqCst), 1);
    Ok(())
}

// An external wrapper which implements only the established read primitive.
// It must retain the default's ordinary sequential semantics.
struct External(SingleNode<CountedEngine>);

impl Replication for External {
    fn propose(&self, op: Op) -> Result<(), ReplError> {
        self.0.propose(op)
    }
    fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
        self.0.propose_batch(ops)
    }
    fn propose_authorized_batch(
        &self,
        ops: Vec<Op>,
        authorization: WriteAuthorization,
    ) -> Result<(), ReplError> {
        self.0.propose_authorized_batch(ops, authorization)
    }
    fn read(
        &self,
        table: &str,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Option<Bytes>, ReplError> {
        self.0.read(table, key, consistency)
    }
    fn range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        self.0.range(table, start, end, consistency)
    }
}

#[test]
fn default_read_many_preserves_external_order_missing_and_sequential_consistency() -> TestResult {
    let external = External(SingleNode::new(CountedEngine::default()));
    external.propose_batch(vec![put(b"a", b"old-a"), put(b"b", b"old-b")])?;
    let storage = external.0.storage();
    storage.read_transactions.store(0, Ordering::SeqCst);
    *storage
        .after_get
        .lock()
        .map_err(|_| "write queue poisoned")? = Some(vec![put(b"b", b"new-b")]);
    let keys = vec![
        b"a".to_vec(),
        b"missing".to_vec(),
        b"b".to_vec(),
        b"a".to_vec(),
    ];
    assert_eq!(
        external.read_many("rows", &keys, ReadConsistency::Eventual)?,
        vec![
            Some(b"old-a".to_vec()),
            None,
            Some(b"new-b".to_vec()),
            Some(b"old-a".to_vec())
        ]
    );
    assert_eq!(storage.read_transactions.load(Ordering::SeqCst), keys.len());
    Ok(())
}

#[test]
fn default_read_many_remains_available_on_ap_and_sharded_backends() -> TestResult {
    let ap = ApDynamo::new_for_test(MemEngine::new(), 1);
    let native: Arc<dyn Replication> = Arc::new(SingleNode::new(MemEngine::new()));
    let sharded = ShardedReplication::new(
        ShardMap::balanced(PartitionStrategy::HashSlots { partitions: 16 }, [1])?,
        BTreeMap::from([(1, native)]),
        1,
    )?;
    let keys = vec![
        b"b".to_vec(),
        b"missing".to_vec(),
        b"a".to_vec(),
        b"b".to_vec(),
    ];
    for repl in [&ap as &dyn Replication, &sharded] {
        repl.propose_batch(vec![put(b"a", b"one"), put(b"b", b"two")])?;
        assert_eq!(
            repl.read_many("rows", &keys, ReadConsistency::Strong)?,
            vec![
                Some(b"two".to_vec()),
                None,
                Some(b"one".to_vec()),
                Some(b"two".to_vec())
            ]
        );
    }
    Ok(())
}
