use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use super::*;
use crate::{
    phase30::{InternalTransportConfig, InternalTransportSecurity},
    repl::{
        SingleNode,
        ap::{ApClusterConfig, ApDynamo, ApNode, ConsistencyLevel, InProcessApTransport},
        shard::{PartitionStrategy, ShardMap, ShardedReplication},
    },
    storage::{MemEngine, RangeIter, ReadTransaction, StorageEngine, WriteTransaction},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Default)]
struct ScanWork {
    entries: AtomicUsize,
    owned_reads: AtomicUsize,
    point_visits: AtomicUsize,
    legacy_metadata_reads: AtomicUsize,
    segments: AtomicUsize,
    mvcc: AtomicUsize,
    view_states: AtomicUsize,
    delay_ms: AtomicUsize,
}

#[derive(Default)]
struct CountedStorage {
    inner: MemEngine,
    work: Arc<ScanWork>,
}

struct CountedRead<T> {
    inner: T,
    work: Arc<ScanWork>,
}

impl StorageEngine for CountedStorage {
    fn supports_guarded_reads(&self) -> bool {
        self.inner.supports_guarded_reads()
    }

    type ReadTxn<'a> = CountedRead<<MemEngine as StorageEngine>::ReadTxn<'a>>;
    type WriteTxn<'a> = <MemEngine as StorageEngine>::WriteTxn<'a>;

    fn begin_read(&self) -> Result<Self::ReadTxn<'_>, StorageError> {
        Ok(CountedRead {
            inner: self.inner.begin_read()?,
            work: Arc::clone(&self.work),
        })
    }

    fn begin_write(&self) -> Result<Self::WriteTxn<'_>, StorageError> {
        self.inner.begin_write()
    }
}

impl<T: ReadTransaction> ReadTransaction for CountedRead<T> {
    fn supports_guarded_reads(&self) -> bool {
        self.inner.supports_guarded_reads()
    }

    fn visit_get(
        &self,
        table: &str,
        key: &[u8],
        budget: &dyn crate::storage::ReadBudget,
        visitor: &mut dyn FnMut(Option<&[u8]>) -> Result<bool, StorageError>,
    ) -> Result<(), StorageError> {
        self.inner.visit_get(table, key, budget, &mut |value| {
            if table == crate::cdc::MATERIALIZED_VIEW_STATE_TABLE {
                self.work.view_states.fetch_add(1, Ordering::SeqCst);
                let delay = self.work.delay_ms.load(Ordering::SeqCst);
                if delay > 0 {
                    std::thread::sleep(Duration::from_millis(
                        u64::try_from(delay).unwrap_or(u64::MAX),
                    ));
                }
                budget.check()?;
            }
            if matches!(table, REL_ROWS_TABLE | crate::repl::ap::AP_VERSIONS_TABLE) {
                self.work.point_visits.fetch_add(1, Ordering::SeqCst);
            }
            visitor(value)
        })
    }

    fn visit_range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        budget: &dyn crate::storage::ReadBudget,
        visitor: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool, StorageError>,
    ) -> Result<(), StorageError> {
        self.inner
            .visit_range(table, start, end, budget, &mut |key, value| {
                if table == REL_COLUMNAR_SEGMENTS_TABLE {
                    self.work.segments.fetch_add(1, Ordering::SeqCst);
                }
                if table == crate::txn::TXN_MVCC_TABLE {
                    self.work.mvcc.fetch_add(1, Ordering::SeqCst);
                    let delay = self.work.delay_ms.load(Ordering::SeqCst);
                    if delay > 0 {
                        std::thread::sleep(Duration::from_millis(
                            u64::try_from(delay).unwrap_or(u64::MAX),
                        ));
                    }
                }
                if matches!(table, REL_ROWS_TABLE | crate::repl::ap::AP_VERSIONS_TABLE) {
                    self.work.entries.fetch_add(1, Ordering::SeqCst);
                    let delay = self.work.delay_ms.load(Ordering::SeqCst);
                    if delay > 0 {
                        std::thread::sleep(Duration::from_millis(
                            u64::try_from(delay).unwrap_or(u64::MAX),
                        ));
                    }
                }
                budget.check()?;
                visitor(key, value)
            })
    }

    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
        if matches!(
            table,
            crate::repl::DIST_TXN_PARTICIPANT_TABLE
                | crate::repl::dist_txn::locks::LOCKS_TABLE
                | crate::txn::TXN_META_TABLE
        ) {
            self.work
                .legacy_metadata_reads
                .fetch_add(1, Ordering::SeqCst);
        }
        if matches!(table, REL_ROWS_TABLE | crate::repl::ap::AP_VERSIONS_TABLE) {
            self.work.owned_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(table, key)
    }

    fn range<'a>(
        &'a self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RangeIter<'a>, StorageError> {
        let counted = matches!(table, REL_ROWS_TABLE | crate::repl::ap::AP_VERSIONS_TABLE);
        let metadata = matches!(
            table,
            crate::repl::DIST_TXN_PARTICIPANT_TABLE | crate::repl::dist_txn::locks::LOCKS_TABLE
        );
        let columnar = table == REL_COLUMNAR_SEGMENTS_TABLE;
        Ok(Box::new(self.inner.range(table, start, end)?.inspect(
            move |_| {
                if metadata {
                    self.work
                        .legacy_metadata_reads
                        .fetch_add(1, Ordering::SeqCst);
                }
                if columnar {
                    self.work.segments.fetch_add(1, Ordering::SeqCst);
                }
                if counted {
                    self.work.entries.fetch_add(1, Ordering::SeqCst);
                    let delay = self.work.delay_ms.load(Ordering::SeqCst);
                    if delay > 0 {
                        std::thread::sleep(Duration::from_millis(
                            u64::try_from(delay).unwrap_or(u64::MAX),
                        ));
                    }
                }
            },
        )))
    }
}

fn ap_backend() -> (Arc<dyn Replication>, Arc<ScanWork>) {
    let storage = CountedStorage::default();
    let work = Arc::clone(&storage.work);
    (
        Arc::new(ApDynamo::new_unchecked(
            storage,
            ApClusterConfig::single_node_for_tests(1),
        )),
        work,
    )
}

fn shard_backend() -> Result<(Arc<dyn Replication>, Arc<ScanWork>), ReplError> {
    let work = Arc::new(ScanWork::default());
    let shards = [1, 2]
        .into_iter()
        .map(|id| {
            let storage = CountedStorage {
                inner: MemEngine::new(),
                work: Arc::clone(&work),
            };
            (
                id,
                Arc::new(SingleNode::new(storage)) as Arc<dyn Replication>,
            )
        })
        .collect();
    let map = ShardMap::balanced(PartitionStrategy::HashSlots { partitions: 16 }, [1, 2])?;
    Ok((Arc::new(ShardedReplication::new(map, shards, 1)?), work))
}

fn sql_fixture(repl: Arc<dyn Replication>) -> Result<SqlEngine, QueryError> {
    let mut engine = SqlEngine::new(repl);
    engine.create_table(
        "items",
        Some(TableSchema::new(
            vec![
                ColumnDef::new("id", ColumnType::Int, false),
                ColumnDef::new("payload", ColumnType::Str, false),
            ],
            0,
        )),
        Vec::new(),
    )?;
    let table = engine
        .table("items")
        .ok_or_else(|| QueryError::MissingTable("items".to_owned()))?;
    for id in 0..64 {
        table.insert(vec![Value::Int(id), Value::Str("x".repeat(1_024))])?;
    }
    engine.performance.query.memory_limit_bytes = 16 * 1_024;
    Ok(engine)
}

async fn check_sql_bounds(repl: Arc<dyn Replication>, work: &ScanWork) -> TestResult {
    assert!(repl.supports_bounded_scans());
    let mut engine = sql_fixture(repl)?;
    work.entries.store(0, Ordering::SeqCst);
    assert_eq!(
        engine.execute("SELECT id FROM items LIMIT 3").await?,
        SqlOutput::Rows(SqlRows {
            columns: vec!["id".to_owned()],
            rows: (0..3).map(|id| vec![Value::Int(id)]).collect(),
        })
    );
    assert!(
        work.entries.load(Ordering::SeqCst) <= 6,
        "LIMIT must stop each shard/replica before materializing the table"
    );
    work.entries.store(0, Ordering::SeqCst);
    assert_eq!(
        engine.execute("SELECT COUNT(*) AS n FROM items").await?,
        SqlOutput::Rows(SqlRows {
            columns: vec!["n".to_owned()],
            rows: vec![vec![Value::Int(64)]],
        })
    );
    work.entries.store(0, Ordering::SeqCst);
    assert!(matches!(
        engine.execute("SELECT payload FROM items").await,
        Err(QueryError::ResourceLimit(_))
    ));
    assert!(
        work.entries.load(Ordering::SeqCst) < 16,
        "memory failure must stop storage work before collecting 64 payloads"
    );
    work.entries.store(0, Ordering::SeqCst);
    assert_eq!(
        engine.execute("SELECT payload FROM items LIMIT 0").await?,
        SqlOutput::Rows(SqlRows {
            columns: vec!["payload".to_owned()],
            rows: Vec::new(),
        })
    );
    assert_eq!(work.entries.load(Ordering::SeqCst), 0);
    assert_eq!(
        engine
            .execute("SELECT id FROM items WHERE id >= 100")
            .await?,
        SqlOutput::Rows(SqlRows {
            columns: vec!["id".to_owned()],
            rows: Vec::new(),
        })
    );
    assert_eq!(work.entries.load(Ordering::SeqCst), 0);
    engine.performance.query.timeout_ms = 20;
    work.delay_ms.store(5, Ordering::SeqCst);
    assert!(matches!(
        engine.execute("SELECT SUM(id) FROM items").await,
        Err(QueryError::Timeout(_))
    ));
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        work.entries.load(Ordering::SeqCst) < 16,
        "timed-out blocking source must stop its storage iterator"
    );
    Ok(())
}

#[tokio::test]
async fn ap_sql_stops_storage_for_limit_memory_and_cancellation() -> TestResult {
    let (repl, work) = ap_backend();
    check_sql_bounds(repl, &work).await
}

#[tokio::test]
async fn sharded_sql_stops_storage_for_limit_memory_and_cancellation() -> TestResult {
    let (repl, work) = shard_backend()?;
    check_sql_bounds(repl, &work).await
}

type QuorumNodes = Vec<Arc<ApDynamo<CountedStorage>>>;

fn quorum_fixture() -> Result<(QuorumNodes, Arc<ScanWork>), ReplError> {
    let work = Arc::new(ScanWork::default());
    let transport = Arc::new(InProcessApTransport::new());
    let mut nodes = Vec::new();
    for id in 1..=3 {
        let config = ApClusterConfig::new(
            id,
            format!("ap-{id}"),
            (1..=3)
                .map(|node| ApNode::new(node, format!("ap-{node}")))
                .collect(),
            3,
        )
        .with_default_write(ConsistencyLevel::All)
        .with_transport(InternalTransportConfig::new(
            format!("local-{id}"),
            InternalTransportSecurity::PlaintextForTests,
        ));
        let node = Arc::new(ApDynamo::new_unchecked(
            CountedStorage {
                inner: MemEngine::new(),
                work: Arc::clone(&work),
            },
            config,
        ));
        transport.register(id, node.clone());
        node.set_transport(transport.clone())?;
        nodes.push(node);
    }
    Ok((nodes, work))
}

#[test]
fn ap_quorum_pages_merge_tombstones_prefix_keys_and_repair_without_full_scans() -> TestResult {
    let (nodes, work) = quorum_fixture()?;
    let keys = [
        b"a".to_vec(),
        b"a\0".to_vec(),
        b"a\0\0".to_vec(),
        b"a\xff".to_vec(),
        b"b".to_vec(),
    ];
    nodes[0].propose_batch(
        keys.iter()
            .map(|key| Op::Put {
                table: "rows".to_owned(),
                key: key.clone(),
                value: key.clone(),
            })
            .collect(),
    )?;
    nodes[0].propose(Op::Delete {
        table: "rows".to_owned(),
        key: b"b".to_vec(),
    })?;
    // Remove one physical replica copy to exercise read repair during paging.
    let mut encoded_key = 4_u64.to_be_bytes().to_vec();
    encoded_key.extend_from_slice(b"rowsa");
    let mut write = nodes[2].storage().begin_write()?;
    write.delete(crate::repl::ap::AP_VERSIONS_TABLE, &encoded_key)?;
    write.commit()?;
    work.entries.store(0, Ordering::SeqCst);
    let mut first = Vec::new();
    nodes[0].scan_range_batches(
        "rows",
        &[],
        &[],
        ReadConsistency::Strong,
        1,
        &|| false,
        &mut |batch| {
            first.extend_from_slice(batch);
            Ok(false)
        },
    )?;
    assert_eq!(first, vec![(b"a".to_vec(), b"a".to_vec())]);
    assert_eq!(
        nodes[2].read("rows", b"a", ReadConsistency::Eventual)?,
        Some(b"a".to_vec())
    );
    assert_eq!(
        work.entries.load(Ordering::SeqCst),
        3,
        "one head per replica, no complete range reads"
    );
    let mut all = Vec::new();
    nodes[0].scan_range_batches(
        "rows",
        &[],
        &[],
        ReadConsistency::Strong,
        2,
        &|| false,
        &mut |batch| {
            all.extend_from_slice(batch);
            Ok(true)
        },
    )?;
    assert_eq!(
        all,
        keys[..4]
            .iter()
            .map(|key| (key.clone(), key.clone()))
            .collect::<Vec<_>>()
    );
    let mut called = false;
    work.entries.store(0, Ordering::SeqCst);
    nodes[0].scan_range_batches(
        "rows",
        &[],
        &[],
        ReadConsistency::Strong,
        1,
        &|| true,
        &mut |_| {
            called = true;
            Ok(true)
        },
    )?;
    assert!(!called);
    assert_eq!(work.entries.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn zero_column_batches_keep_rows_and_utf8_conversion_preserves_null_and_json() -> TestResult {
    let rows = vec![Vec::new(), Vec::new(), Vec::new()];
    let batch = rows_to_record_batch(Arc::new(Schema::empty()), &rows)?;
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(record_batches_to_rows(&[batch], Vec::new())?.rows, rows);
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
    let rows = vec![
        vec![Value::Str("héllo".to_owned())],
        vec![Value::Null],
        vec![Value::Int(42)],
    ];
    let batch = rows_to_record_batch(schema, &rows)?;
    assert_eq!(
        record_batches_to_rows(&[batch], Vec::new())?.rows,
        vec![
            vec![Value::Str("héllo".to_owned())],
            vec![Value::Null],
            vec![Value::Str(r#"{"Int":42}"#.to_owned())],
        ]
    );
    Ok(())
}

#[test]
fn native_scan_checks_cancellation_before_pull_and_bounds_batch_allocation() -> TestResult {
    use std::sync::atomic::AtomicBool;

    let storage = CountedStorage::default();
    let work = Arc::clone(&storage.work);
    let node = SingleNode::new(storage);
    node.propose_batch(
        (0..3)
            .map(|key| Op::Put {
                table: REL_ROWS_TABLE.to_owned(),
                key: vec![key],
                value: b"value".to_vec(),
            })
            .collect(),
    )?;
    let cancelled = AtomicBool::new(false);
    node.scan_range_batches(
        REL_ROWS_TABLE,
        &[],
        &[],
        ReadConsistency::Strong,
        1,
        &|| cancelled.load(Ordering::SeqCst),
        &mut |_| {
            cancelled.store(true, Ordering::SeqCst);
            Ok(true)
        },
    )?;
    assert_eq!(
        work.entries.load(Ordering::SeqCst),
        1,
        "cancellation must precede pulling/decrypting the next value"
    );
    cancelled.store(false, Ordering::SeqCst);
    let mut rows = Vec::new();
    node.scan_range_batches(
        REL_ROWS_TABLE,
        &[],
        &[],
        ReadConsistency::Strong,
        usize::MAX,
        &|| false,
        &mut |batch| {
            rows.extend_from_slice(batch);
            Ok(true)
        },
    )?;
    assert_eq!(
        rows.len(),
        3,
        "a large batch hint must not trigger a capacity-overflow allocation"
    );
    Ok(())
}

#[tokio::test]
async fn tiny_wire_arrays_are_rejected_before_decoded_nodes_are_materialized() -> TestResult {
    let (repl, work) = ap_backend();
    let mut engine = SqlEngine::new(repl);
    let mut columns = vec![ColumnDef::new("id", ColumnType::Int, false)];
    columns.extend(
        (0..1_000).map(|index| ColumnDef::new(format!("empty_{index}"), ColumnType::Null, true)),
    );
    engine.create_table("documents", Some(TableSchema::new(columns, 0)), Vec::new())?;
    let mut row = vec![Value::Int(1)];
    row.extend(std::iter::repeat_n(Value::Null, 1_000));
    engine
        .table("documents")
        .ok_or("missing wide table")?
        .insert(row)?;
    engine.performance.query.memory_limit_bytes = 8_192;
    work.entries.store(0, Ordering::SeqCst);
    let result = engine.execute("SELECT id FROM documents").await;
    assert!(
        matches!(&result, Err(QueryError::ResourceLimit(_))),
        "{result:?}"
    );
    assert_eq!(work.entries.load(Ordering::SeqCst), 1);
    Ok(())
}

fn read_budget(bytes: usize) -> Arc<budget::Budget> {
    let config = crate::performance::QueryExecutionConfig {
        memory_limit_bytes: bytes,
        ..crate::performance::QueryExecutionConfig::default()
    };
    budget::Budget::new(&config)
}

#[test]
fn native_point_and_many_reject_before_owned_reads_and_release_reservations() -> TestResult {
    use crate::storage::ReadBudget;

    let storage = CountedStorage::default();
    let work = Arc::clone(&storage.work);
    let node = SingleNode::new(storage);
    node.propose_batch(vec![
        Op::Put {
            table: REL_ROWS_TABLE.to_owned(),
            key: b"a".to_vec(),
            value: b"first".to_vec(),
        },
        Op::Put {
            table: REL_ROWS_TABLE.to_owned(),
            key: b"b".to_vec(),
            value: vec![7; 65_536],
        },
    ])?;
    assert!(node.supports_guarded_reads());
    let budget = read_budget(128);
    let _cancel_on_drop = budget.start(Duration::ZERO)?;
    let mut copied = false;
    let result = node.visit_get(
        REL_ROWS_TABLE,
        b"b",
        ReadConsistency::Strong,
        budget.as_ref(),
        &mut |value| {
            let value = value.ok_or(StorageError::NotFound)?;
            let _lease = ReadBudget::reserve(budget.as_ref(), value.len())?;
            let _copy = value.to_vec();
            copied = true;
            Ok(true)
        },
    );
    assert!(matches!(
        result,
        Err(ReplError::Storage(StorageError::ResourceLimit(_)))
    ));
    assert!(!copied);
    assert_eq!(work.owned_reads.load(Ordering::SeqCst), 0);
    assert_eq!(work.point_visits.load(Ordering::SeqCst), 1);
    assert_eq!(budget.pool.reserved(), 0);

    // A fresh query budget is not poisoned by an earlier failed statement.
    let budget = read_budget(128);
    let _cancel_on_drop = budget.start(Duration::ZERO)?;
    let keys = [
        b"a".to_vec(),
        b"missing".to_vec(),
        b"b".to_vec(),
        b"a".to_vec(),
    ];
    let mut leases = Vec::new();
    let mut values = Vec::new();
    let result = node.visit_many(
        REL_ROWS_TABLE,
        &keys,
        ReadConsistency::Strong,
        budget.as_ref(),
        &mut |index, value| {
            assert_eq!(index, values.len());
            if let Some(value) = value {
                leases.push(ReadBudget::reserve(budget.as_ref(), value.len())?);
            }
            values.push(value.map(<[u8]>::to_vec));
            Ok(true)
        },
    );
    assert!(matches!(
        result,
        Err(ReplError::Storage(StorageError::ResourceLimit(_)))
    ));
    assert_eq!(values, vec![Some(b"first".to_vec()), None]);
    assert_eq!(work.owned_reads.load(Ordering::SeqCst), 0);
    assert_eq!(work.point_visits.load(Ordering::SeqCst), 4);
    drop(values);
    drop(leases);
    assert_eq!(budget.pool.reserved(), 0);
    Ok(())
}

fn check_many_snapshot(node: &dyn Replication) -> TestResult {
    use crate::storage::ReadBudget;
    node.propose_batch(vec![
        Op::Put {
            table: "rows".to_owned(),
            key: b"a".to_vec(),
            value: b"old a".to_vec(),
        },
        Op::Put {
            table: "rows".to_owned(),
            key: b"b".to_vec(),
            value: b"old b".to_vec(),
        },
    ])?;
    let budget = read_budget(1_024);
    let _cancel_on_drop = budget.start(Duration::ZERO)?;
    let keys = [
        b"a".to_vec(),
        b"missing".to_vec(),
        b"a".to_vec(),
        b"b".to_vec(),
    ];
    let mut leases = Vec::new();
    let mut values = Vec::new();
    node.visit_many(
        "rows",
        &keys,
        ReadConsistency::Strong,
        budget.as_ref(),
        &mut |index, value| {
            if let Some(value) = value {
                leases.push(ReadBudget::reserve(budget.as_ref(), value.len())?);
            }
            values.push(value.map(<[u8]>::to_vec));
            if index == 0 {
                node.propose(Op::Put {
                    table: "rows".to_owned(),
                    key: b"b".to_vec(),
                    value: b"new b".to_vec(),
                })?;
            }
            Ok(true)
        },
    )?;
    assert_eq!(
        values,
        vec![
            Some(b"old a".to_vec()),
            None,
            Some(b"old a".to_vec()),
            Some(b"old b".to_vec())
        ]
    );
    assert_eq!(
        node.read("rows", b"b", ReadConsistency::Strong)?,
        Some(b"new b".to_vec())
    );
    Ok(())
}

#[test]
fn native_many_keeps_one_snapshot_and_preserves_missing_and_duplicate_keys() -> TestResult {
    check_many_snapshot(&SingleNode::new(MemEngine::new()))?;
    let directory = tempfile::tempdir()?;
    check_many_snapshot(&SingleNode::new(crate::storage::RedbEngine::open(
        directory.path().join("guarded.redb"),
    )?))
}

#[test]
fn parallel_native_visitors_share_one_atomic_query_memory_pool() -> TestResult {
    use crate::storage::ReadBudget;

    let node = SingleNode::new(MemEngine::new());
    node.propose(Op::Put {
        table: "rows".to_owned(),
        key: b"a".to_vec(),
        value: vec![1; 200],
    })?;
    let budget = read_budget(256);
    let _cancel_on_drop = budget.start(Duration::ZERO)?;
    let barrier = std::sync::Barrier::new(2);
    let copies = AtomicUsize::new(0);
    let outcomes = std::thread::scope(|scope| {
        let run = || {
            let mut retained_lease = None;
            let result = node.visit_get(
                "rows",
                b"a",
                ReadConsistency::Strong,
                budget.as_ref(),
                &mut |value| {
                    let value = value.ok_or(StorageError::NotFound)?;
                    let lease = ReadBudget::reserve(budget.as_ref(), value.len())?;
                    let _copy = value.to_vec();
                    copies.fetch_add(1, Ordering::SeqCst);
                    retained_lease = Some(lease);
                    Ok(true)
                },
            );
            // Retain the admitted output until both sources attempted admission,
            // even when another source fails before reaching its data callback.
            barrier.wait();
            drop(retained_lease);
            result
        };
        let one = scope.spawn(run);
        let two = scope.spawn(run);
        (one.join(), two.join())
    });
    let one = outcomes.0.map_err(|_| "first read panicked")?;
    let two = outcomes.1.map_err(|_| "second read panicked")?;
    assert_eq!(
        copies.load(Ordering::SeqCst),
        1,
        "the shared pool must admit only one owned copy"
    );
    assert!(
        matches!(one, Err(ReplError::Storage(StorageError::ResourceLimit(_))))
            || matches!(two, Err(ReplError::Storage(StorageError::ResourceLimit(_))))
    );
    assert_eq!(budget.pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn analyze_obeys_source_budget_deadline_and_does_not_publish_failed_statistics() -> TestResult
{
    let (repl, work) = ap_backend();
    let mut engine = sql_fixture(repl)?;
    work.entries.store(0, Ordering::SeqCst);
    let result = engine.execute("ANALYZE items").await;
    assert!(
        matches!(&result, Err(QueryError::ResourceLimit(_))),
        "{result:?}"
    );
    assert!(work.entries.load(Ordering::SeqCst) < 16);
    assert_eq!(
        StatsCatalog::read_table(engine.repl.as_ref(), "items")?,
        None
    );
    engine.performance.query.memory_limit_bytes = 4 * 1_024 * 1_024;
    engine.performance.query.timeout_ms = 1;
    work.entries.store(0, Ordering::SeqCst);
    work.delay_ms.store(5, Ordering::SeqCst);
    let result = engine.execute("ANALYZE items").await;
    assert!(matches!(&result, Err(QueryError::Timeout(_))), "{result:?}");
    assert!(work.entries.load(Ordering::SeqCst) < 16);
    assert_eq!(
        StatsCatalog::read_table(engine.repl.as_ref(), "items")?,
        None
    );
    work.delay_ms.store(0, Ordering::SeqCst);
    engine.performance.query.timeout_ms = 30_000;
    engine.execute("ANALYZE items").await?;
    assert_eq!(
        StatsCatalog::read_table(engine.repl.as_ref(), "items")?
            .ok_or("missing stats")?
            .row_count,
        64
    );
    Ok(())
}

struct Legacy(SingleNode<MemEngine>);

impl Replication for Legacy {
    fn propose(&self, op: Op) -> Result<(), ReplError> {
        self.0.propose(op)
    }
    fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
        self.0.propose_batch(ops)
    }
    fn propose_authorized_batch(
        &self,
        ops: Vec<Op>,
        authorization: crate::txn::WriteAuthorization,
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
fn legacy_replication_keeps_visitor_compatibility_without_claiming_native_guards() -> TestResult {
    let node = Legacy(SingleNode::new(MemEngine::new()));
    assert!(!node.supports_guarded_reads());
    node.propose(Op::Put {
        table: "rows".to_owned(),
        key: b"a".to_vec(),
        value: b"value".to_vec(),
    })?;
    let budget = read_budget(4_096);
    let _cancel_on_drop = budget.start(Duration::ZERO)?;
    let keys = [b"a".to_vec(), b"missing".to_vec(), b"a".to_vec()];
    let mut values = Vec::new();
    node.visit_many(
        "rows",
        &keys,
        ReadConsistency::Strong,
        budget.as_ref(),
        &mut |index, value| {
            assert_eq!(index, values.len());
            // This fixture retains tiny values and checks compatibility, not a native bound.
            values.push(value.map(<[u8]>::to_vec));
            Ok(true)
        },
    )?;
    assert_eq!(
        values,
        vec![Some(b"value".to_vec()), None, Some(b"value".to_vec())]
    );
    let mut keys = Vec::new();
    node.visit_range(
        "rows",
        &[],
        &[],
        ReadConsistency::Strong,
        budget.as_ref(),
        &mut |key, _| {
            keys.push(key.to_vec());
            Ok(false)
        },
    )?;
    assert_eq!(keys, vec![b"a".to_vec()]);
    Ok(())
}

#[test]
fn guarded_lock_checks_borrow_large_legacy_prepares_before_rejecting_reads() -> TestResult {
    let storage = CountedStorage::default();
    let work = Arc::clone(&storage.work);
    let node = SingleNode::new(storage);
    crate::repl::propose_system(
        &node,
        Op::Put {
            table: crate::repl::DIST_TXN_PARTICIPANT_TABLE.to_owned(),
            key: 1_u64.to_be_bytes().to_vec(),
            value: vec![9; 65_536],
        },
    )?;
    work.legacy_metadata_reads.store(0, Ordering::SeqCst);
    let budget = read_budget(128);
    let _cancel_on_drop = budget.start(Duration::ZERO)?;
    let mut visited = false;
    let result = node.visit_get(
        REL_ROWS_TABLE,
        b"a",
        ReadConsistency::Strong,
        budget.as_ref(),
        &mut |_| {
            visited = true;
            Ok(true)
        },
    );
    assert!(matches!(
        result,
        Err(ReplError::Storage(StorageError::Conflict))
    ));
    assert!(!visited);
    assert_eq!(work.legacy_metadata_reads.load(Ordering::SeqCst), 0);
    assert_eq!(budget.pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn native_columnar_guards_decoded_expansion_and_preserves_exact_projection() -> TestResult {
    let storage = CountedStorage::default();
    let work = Arc::clone(&storage.work);
    let repl: Arc<dyn Replication> = Arc::new(SingleNode::new(storage));
    let mut engine = SqlEngine::with_layout(Arc::clone(&repl), TableLayout::Columnar);
    engine.create_table(
        "events",
        Some(TableSchema::new(
            vec![
                ColumnDef::new("id", ColumnType::Int, false),
                ColumnDef::new("payload", ColumnType::Str, false),
            ],
            0,
        )),
        Vec::new(),
    )?;
    let table = engine.table("events").ok_or("missing columnar table")?;
    let payload = "x".repeat(16_384);
    let rows = (0..64)
        .map(|id| vec![Value::Int(id), Value::Str(payload.clone())])
        .collect();
    repl.propose_batch(table.columnar_replace_ops(rows)?)?;
    engine.performance.query.memory_limit_bytes = 512 * 1_024;
    work.segments.store(0, Ordering::SeqCst);
    assert_eq!(
        engine.execute("SELECT payload FROM events LIMIT 0").await?,
        SqlOutput::Rows(SqlRows {
            columns: vec!["payload".to_owned()],
            rows: Vec::new(),
        })
    );
    assert_eq!(work.segments.load(Ordering::SeqCst), 0);
    let result = engine.execute("SELECT payload FROM events").await;
    assert!(
        matches!(&result, Err(QueryError::ResourceLimit(_))),
        "{result:?}"
    );
    // COUNT projects no payload pages, despite a decoded payload larger than the budget.
    assert_eq!(
        engine.execute("SELECT COUNT(*) AS n FROM events").await?,
        SqlOutput::Rows(SqlRows {
            columns: vec!["n".to_owned()],
            rows: vec![vec![Value::Int(64)]],
        })
    );
    engine.performance.query.memory_limit_bytes = 64 * 1_024 * 1_024;
    assert_eq!(
        engine
            .execute("SELECT payload,id,payload FROM events LIMIT 1")
            .await?,
        SqlOutput::Rows(SqlRows {
            columns: vec!["payload".to_owned(), "id".to_owned(), "payload".to_owned()],
            rows: vec![vec![
                Value::Str(payload.clone()),
                Value::Int(0),
                Value::Str(payload)
            ]],
        })
    );
    Ok(())
}

fn current_lsn(repl: &dyn Replication) -> Result<u64, Box<dyn std::error::Error>> {
    Ok(crate::txn::decode_txn_id(
        &repl
            .read(
                crate::txn::TXN_META_TABLE,
                crate::txn::CURRENT_TXN_ID_KEY,
                ReadConsistency::Strong,
            )?
            .ok_or("missing current LSN")?,
    )?)
}

fn wide_history_fixture() -> Result<(SqlEngine, Arc<ScanWork>, u64), Box<dyn std::error::Error>> {
    let storage = CountedStorage::default();
    let work = Arc::clone(&storage.work);
    let repl: Arc<dyn Replication> = Arc::new(SingleNode::new(storage));
    let mut engine = SqlEngine::new(Arc::clone(&repl));
    let mut columns = vec![ColumnDef::new("id", ColumnType::Int, false)];
    columns.extend(
        (0..1_000).map(|index| ColumnDef::new(format!("empty_{index}"), ColumnType::Null, true)),
    );
    let schema = TableSchema::new(columns, 0);
    engine.create_table("wide", Some(schema.clone()), Vec::new())?;
    let mut row = vec![Value::Int(1)];
    row.extend(std::iter::repeat_n(Value::Null, 1_000));
    engine.table("wide").ok_or("missing table")?.insert(row)?;
    engine.register_temporal_table(
        "wide_history",
        "wide".to_owned(),
        schema,
        TemporalRetention::default(),
    )?;
    let lsn = current_lsn(repl.as_ref())?;
    Ok((engine, work, lsn))
}

#[tokio::test]
async fn as_of_and_history_preflight_wide_values_and_cancel_before_decode() -> TestResult {
    let (mut engine, work, lsn) = wide_history_fixture()?;
    engine.performance.query.memory_limit_bytes = 16 * 1_024;
    work.mvcc.store(0, Ordering::SeqCst);
    let result = engine
        .clone()
        .with_snapshot_lsn(lsn)
        .execute("SELECT id FROM wide")
        .await;
    assert!(
        matches!(&result, Err(QueryError::ResourceLimit(_))),
        "{result:?}"
    );
    assert_eq!(work.mvcc.load(Ordering::SeqCst), 1);
    work.mvcc.store(0, Ordering::SeqCst);
    let result = engine.execute("SELECT id FROM wide_history").await;
    assert!(
        matches!(&result, Err(QueryError::ResourceLimit(_))),
        "{result:?}"
    );
    assert_eq!(work.mvcc.load(Ordering::SeqCst), 1);
    engine.performance.query.memory_limit_bytes = 2 * 1_024 * 1_024;
    for sql in ["SELECT id FROM wide_history", "SELECT id FROM wide"] {
        let snapshot = if sql.ends_with("wide") {
            engine.clone().with_snapshot_lsn(lsn)
        } else {
            engine.clone()
        };
        assert_eq!(
            snapshot.execute(sql).await?,
            SqlOutput::Rows(SqlRows {
                columns: vec!["id".to_owned()],
                rows: vec![vec![Value::Int(1)]],
            })
        );
    }
    engine.performance.query.timeout_ms = 1;
    work.delay_ms.store(5, Ordering::SeqCst);
    let result = engine
        .with_snapshot_lsn(lsn)
        .execute("SELECT id FROM wide")
        .await;
    assert!(matches!(&result, Err(QueryError::Timeout(_))), "{result:?}");
    Ok(())
}

fn view_fixture(
    source_row: Row,
    legacy_count: bool,
) -> Result<(SqlEngine, Arc<ScanWork>), Box<dyn std::error::Error>> {
    use crate::cdc::{
        AggregateKind, AggregateSpec, AggregateValue, GroupState, MaterializedViewSpec,
        MaterializedViewState,
    };
    let storage = CountedStorage::default();
    let work = Arc::clone(&storage.work);
    let repl: Arc<dyn Replication> = Arc::new(SingleNode::new(storage));
    let mut engine = SqlEngine::new(Arc::clone(&repl));
    let columns = source_row
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let ty = match value {
                Value::Int(_) => ColumnType::Int,
                Value::Str(_) => ColumnType::Str,
                Value::Bytes(_) => ColumnType::Bytes,
                _ => ColumnType::Null,
            };
            ColumnDef::new(format!("c{index}"), ty, index > 1)
        })
        .collect();
    let schema = TableSchema::new(columns, 0);
    engine.create_table("items", Some(schema.clone()), Vec::new())?;
    let spec = MaterializedViewSpec {
        name: "totals".to_owned(),
        source_table: "items".to_owned(),
        filter: None,
        group_by: 1,
        aggregates: vec![AggregateSpec {
            output_name: "n".to_owned(),
            kind: AggregateKind::Count,
            column: None,
        }],
    };
    let state = MaterializedViewState {
        fresh_to_lsn: 0,
        source_rows: BTreeMap::from([("row".to_owned(), source_row)]),
        groups: BTreeMap::from([(
            "7b22537472223a2267227d".to_owned(),
            GroupState {
                group_value: Value::Str("g".to_owned()),
                aggregates: vec![AggregateValue::Count(1)],
                row_count: if legacy_count { None } else { Some(1) },
            },
        )]),
    };
    crate::repl::propose_system_batch(
        repl.as_ref(),
        vec![
            Op::Put {
                table: crate::cdc::MATERIALIZED_VIEWS_TABLE.to_owned(),
                key: b"totals".to_vec(),
                value: serde_json::to_vec(&spec)?,
            },
            Op::Put {
                table: crate::cdc::MATERIALIZED_VIEW_STATE_TABLE.to_owned(),
                key: b"totals".to_vec(),
                value: serde_json::to_vec(&state)?,
            },
        ],
    )?;
    engine.register_materialized_view("totals", spec, &schema)?;
    Ok((engine, work))
}

#[tokio::test]
async fn materialized_view_typed_guard_bounds_nodes_but_admits_real_byte_vectors() -> TestResult {
    let mut wide = vec![Value::Int(1), Value::Str("g".to_owned())];
    wide.extend(std::iter::repeat_n(Value::Null, 1_000));
    let (mut engine, work) = view_fixture(wide, false)?;
    engine.performance.query.memory_limit_bytes = 16 * 1_024;
    work.view_states.store(0, Ordering::SeqCst);
    let result = engine.execute("SELECT n FROM totals").await;
    assert!(
        matches!(&result, Err(QueryError::ResourceLimit(_))),
        "{result:?}"
    );
    assert_eq!(work.view_states.load(Ordering::SeqCst), 1);
    // The trusted API retains its legacy behavior; SQL alone supplies the guard.
    assert_eq!(
        crate::cdc::read_materialized_view(engine.repl.as_ref(), "totals")?
            .rows
            .len(),
        1
    );
    engine.performance.query.memory_limit_bytes = 2 * 1_024 * 1_024;
    assert_eq!(
        engine.execute("SELECT n FROM totals").await?,
        SqlOutput::Rows(SqlRows {
            columns: vec!["n".to_owned()],
            rows: vec![vec![Value::Int(1)]],
        })
    );
    for legacy in [false, true] {
        let (mut engine, work) = view_fixture(
            vec![
                Value::Int(1),
                Value::Str("g".to_owned()),
                Value::Bytes(vec![255; 65_536]),
            ],
            legacy,
        )?;
        engine.performance.query.memory_limit_bytes = 256 * 1_024;
        assert_eq!(
            engine.execute("SELECT n FROM totals").await?,
            SqlOutput::Rows(SqlRows {
                columns: vec!["n".to_owned()],
                rows: vec![vec![Value::Int(1)]],
            })
        );
        work.view_states.store(0, Ordering::SeqCst);
        assert_eq!(
            engine.execute("SELECT n FROM totals LIMIT 0").await?,
            SqlOutput::Rows(SqlRows {
                columns: vec!["n".to_owned()],
                rows: Vec::new(),
            })
        );
        assert_eq!(work.view_states.load(Ordering::SeqCst), 0);
        work.delay_ms.store(5, Ordering::SeqCst);
        engine.performance.query.timeout_ms = 1;
        let result = engine.execute("SELECT n FROM totals").await;
        assert!(matches!(&result, Err(QueryError::Timeout(_))), "{result:?}");
    }
    Ok(())
}

#[test]
fn external_query_errors_keep_types_even_when_context_mentions_memory() {
    for error in [
        QueryError::Unsupported("TLS requires CA trust".to_owned()),
        QueryError::Timeout("deadline".to_owned()),
        QueryError::Cancelled("cancelled".to_owned()),
        QueryError::ResourceLimit("source cap".to_owned()),
    ] {
        let expected = std::mem::discriminant(&error);
        let wrapped = DataFusionError::Context(
            "memory_context".to_owned(),
            Box::new(query_to_datafusion(error)),
        );
        assert_eq!(
            std::mem::discriminant(&datafusion_to_query_error(&wrapped)),
            expected
        );
    }
}
