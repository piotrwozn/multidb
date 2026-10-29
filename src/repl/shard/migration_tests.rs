use super::*;
use crate::{repl::SingleNode, storage::MemEngine};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn router(
    one: Arc<dyn Replication>,
    two: Arc<dyn Replication>,
) -> Result<ShardedReplication, ReplError> {
    ShardedReplication::new(
        ShardMap::balanced(PartitionStrategy::HashSlots { partitions: 1 }, [1])?,
        BTreeMap::from([(1, one), (2, two)]),
        1,
    )
}

#[test]
fn migration_copies_all_registered_models_ff_keys_and_excludes_old_owner_ghosts() -> TestResult {
    let one = Arc::new(SingleNode::new(MemEngine::new()));
    let two = Arc::new(SingleNode::new(MemEngine::new()));
    let repl = router(one.clone(), two.clone())?;
    let tables = [
        "graph_out_edges",
        "graph_in_edges",
        "geo_cells",
        "full_text_postings",
        "tensor_chunks",
        "tensor_revisions",
        "rel_columnar_segment_meta",
        "custom_model",
    ];
    for table in tables {
        repl.propose(Op::Put {
            table: table.to_owned(),
            key: vec![0xff, 0x00],
            value: table.as_bytes().to_vec(),
        })?;
    }
    two.propose(Op::Put {
        table: "stale_target_only".to_owned(),
        key: b"ghost".to_vec(),
        value: b"stale".to_vec(),
    })?;
    repl.migrate_partition(0, 2)?;
    for table in tables {
        assert_eq!(
            repl.read(table, &[0xff, 0x00], ReadConsistency::Strong)?,
            Some(table.as_bytes().to_vec())
        );
        assert_eq!(
            two.read(table, &[0xff, 0x00], ReadConsistency::Strong)?,
            Some(table.as_bytes().to_vec())
        );
    }
    assert_eq!(
        repl.range("stale_target_only", &[], &[], ReadConsistency::Strong)?,
        Vec::new()
    );
    repl.propose(Op::Put {
        table: "custom_model".to_owned(),
        key: vec![0xff, 0x00],
        value: b"new".to_vec(),
    })?;
    assert_eq!(
        repl.range("custom_model", &[], &[], ReadConsistency::Strong)?,
        vec![(vec![0xff, 0x00], b"new".to_vec())]
    );
    repl.propose(Op::Delete {
        table: "custom_model".to_owned(),
        key: vec![0xff, 0x00],
    })?;
    assert_eq!(
        repl.range("custom_model", &[], &[], ReadConsistency::Strong)?,
        Vec::new()
    );
    Ok(())
}

struct Controlled {
    inner: SingleNode<MemEngine>,
    block: AtomicBool,
    fail: AtomicBool,
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl Replication for Controlled {
    fn registered_keyspaces(&self) -> Result<Vec<String>, ReplError> {
        self.inner.registered_keyspaces()
    }
    fn supports_bounded_scans(&self) -> bool {
        true
    }
    fn propose(&self, op: Op) -> Result<(), ReplError> {
        self.propose_batch(vec![op])
    }
    fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
        if ops.iter().any(|op| op_parts(op).0 == "custom_model") {
            if self.fail.swap(false, Ordering::SeqCst) {
                return Err(ReplError::Transport("injected copy failure".to_owned()));
            }
            if self.block.swap(false, Ordering::SeqCst) {
                self.entered
                    .send(())
                    .map_err(|error| ReplError::Transport(error.to_string()))?;
                self.release
                    .lock()
                    .map_err(|_| ReplError::Transport("gate poisoned".to_owned()))?
                    .recv()
                    .map_err(|error| ReplError::Transport(error.to_string()))?;
            }
        }
        self.inner.propose_batch(ops)
    }
    fn propose_authorized_batch(
        &self,
        ops: Vec<Op>,
        authorization: txn::WriteAuthorization,
    ) -> Result<(), ReplError> {
        self.inner.propose_authorized_batch(ops, authorization)
    }
    fn read(
        &self,
        table: &str,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Option<Bytes>, ReplError> {
        self.inner.read(table, key, consistency)
    }
    fn range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        self.inner.range(table, start, end, consistency)
    }
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
        self.inner.scan_range_batches(
            table,
            start,
            end,
            consistency,
            batch_rows,
            cancelled,
            on_batch,
        )
    }
}

fn controlled() -> (Arc<Controlled>, mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (entered, receive) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    (
        Arc::new(Controlled {
            inner: SingleNode::new(MemEngine::new()),
            block: AtomicBool::new(false),
            fail: AtomicBool::new(false),
            entered,
            release: Mutex::new(gate),
        }),
        receive,
        release,
    )
}

#[test]
fn migration_drains_write_already_routed_to_old_owner() -> TestResult {
    let (one, entered, release) = controlled();
    let two = Arc::new(SingleNode::new(MemEngine::new()));
    let repl = Arc::new(router(one.clone(), two)?);
    one.block.store(true, Ordering::SeqCst);
    let writer = {
        let repl = Arc::clone(&repl);
        std::thread::spawn(move || {
            repl.propose(Op::Put {
                table: "custom_model".to_owned(),
                key: b"key".to_vec(),
                value: b"in-flight".to_vec(),
            })
        })
    };
    entered.recv_timeout(std::time::Duration::from_secs(5))?;
    assert!(
        matches!(
            repl.routing_barrier.try_write(),
            Err(std::sync::TryLockError::WouldBlock)
        ),
        "writer must retain its route until backend commit completes"
    );
    let migration = {
        let repl = Arc::clone(&repl);
        std::thread::spawn(move || repl.migrate_partition(0, 2))
    };
    release.send(())?;
    writer.join().map_err(|_| "writer panic")??;
    migration.join().map_err(|_| "migration panic")??;
    assert_eq!(repl.shard_for("custom_model", b"key")?, 2);
    assert_eq!(
        repl.read("custom_model", b"key", ReadConsistency::Strong)?,
        Some(b"in-flight".to_vec())
    );
    Ok(())
}

#[test]
fn failed_copy_keeps_source_owner_and_releases_routing_barrier() -> TestResult {
    let one = Arc::new(SingleNode::new(MemEngine::new()));
    let (two, _, _) = controlled();
    let repl = router(one, two.clone())?;
    repl.propose(Op::Put {
        table: "custom_model".to_owned(),
        key: b"key".to_vec(),
        value: b"old".to_vec(),
    })?;
    two.fail.store(true, Ordering::SeqCst);
    assert!(repl.migrate_partition(0, 2).is_err());
    assert_eq!(repl.shard_for("custom_model", b"key")?, 1);
    assert_eq!(
        repl.migration(0).ok_or("missing aborted migration")?.state,
        MigrationState::Aborted
    );
    repl.propose(Op::Put {
        table: "custom_model".to_owned(),
        key: b"key".to_vec(),
        value: b"after-abort".to_vec(),
    })?;
    assert_eq!(
        repl.read("custom_model", b"key", ReadConsistency::Strong)?,
        Some(b"after-abort".to_vec())
    );
    Ok(())
}

#[test]
fn placement_version_overflow_does_not_change_owner() -> TestResult {
    let mut map = ShardMap::balanced(PartitionStrategy::HashSlots { partitions: 1 }, [1])?;
    map.version = u64::MAX;
    assert!(map.reassign_partition(0, 2).is_err());
    assert_eq!(map.shard_for_partition(0)?, 1);
    Ok(())
}

#[test]
fn migration_refuses_incomplete_backend_inventory_before_changing_owner() -> TestResult {
    let one = Arc::new(SingleNode::new(MemEngine::new()));
    let two = Arc::new(crate::repl::ApDynamo::new_for_test(MemEngine::new(), 2));
    let repl = router(one, two)?;
    repl.propose(Op::Put {
        table: "custom_model".to_owned(),
        key: b"key".to_vec(),
        value: b"source".to_vec(),
    })?;
    assert!(matches!(
        repl.migrate_partition(0, 2),
        Err(ReplError::Unsupported(_))
    ));
    assert_eq!(repl.shard_for("custom_model", b"key")?, 1);
    assert_eq!(
        repl.read("custom_model", b"key", ReadConsistency::Strong)?,
        Some(b"source".to_vec())
    );
    Ok(())
}
