use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use super::*;
use crate::{
    query::{ColumnDef, ColumnType, RelTable},
    repl::{Op, SingleNode},
    storage::{MemEngine, ReadTransaction, StorageEngine, WriteTransaction},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn history_floor_filters_older_stored_versions_without_expiring_a_valid_read()
-> Result<(), Box<dyn std::error::Error>> {
    let repl = Arc::new(SingleNode::new(MemEngine::new()));
    let schema = TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("name", ColumnType::Str, false),
        ],
        0,
    );
    let table = RelTable::create(repl.clone(), "items", Some(schema.clone()), Vec::new())?;
    table.insert(vec![Value::Int(1), Value::Str("old".to_owned())])?;
    let first = repl.current_txn_id()?;
    table.update(vec![Value::Int(1), Value::Str("new".to_owned())])?;
    let second = repl.current_txn_id()?;
    let retention = TemporalRetention {
        min_lsn: second,
        keep_history: true,
    };
    let history = history_rows_for_table(repl.as_ref(), "items", &schema, retention)?;
    assert_eq!(history.len(), 1);
    assert_eq!(
        &history[0][..2],
        &[Value::Int(1), Value::Str("new".to_owned())]
    );
    assert_eq!(history[0][2], Value::Int(i64::try_from(second)?));
    assert!(matches!(
        table_rows_as_of(
            repl.as_ref(),
            "items",
            &schema,
            TemporalPoint::Lsn(first),
            retention
        ),
        Err(TemporalError::RetentionExpired { .. })
    ));
    assert_eq!(
        table_rows_as_of(
            repl.as_ref(),
            "items",
            &schema,
            TemporalPoint::Lsn(second),
            retention
        )?
        .rows,
        vec![vec![Value::Int(1), Value::Str("new".to_owned())]]
    );
    Ok(())
}

#[test]
fn disabled_history_is_rejected_even_when_the_table_has_no_versions() {
    let repl = SingleNode::new(MemEngine::new());
    let schema = TableSchema::new(vec![ColumnDef::new("id", ColumnType::Int, false)], 0);
    assert!(matches!(
        history_rows_for_table(
            &repl,
            "empty",
            &schema,
            TemporalRetention {
                min_lsn: 0,
                keep_history: false
            }
        ),
        Err(TemporalError::Unsupported(_))
    ));
}

#[test]
fn gc_anchors_remain_valid_for_as_of_but_never_become_apparently_complete_history()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::{
        db::{DbConfig, Profile, create_database},
        txn::history::HistoryGcOptions,
    };

    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let schema = TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("name", ColumnType::Str, false),
        ],
        0,
    );
    let table = database.create_table("items", Some(schema.clone()), Vec::new())?;
    table.insert(vec![Value::Int(1), Value::Str("discarded".to_owned())])?;
    table.update(vec![Value::Int(1), Value::Str("anchor".to_owned())])?;
    let anchor = database.commit_lsn()?;
    table.insert(vec![Value::Int(2), Value::Str("at-floor".to_owned())])?;
    let floor = database.commit_lsn()?;
    let report = database.gc_history(&HistoryGcOptions {
        mvcc_retain_lsns: Some(1),
        ..HistoryGcOptions::default()
    })?;
    assert_eq!(report.history_horizon, floor);
    assert!(report.mvcc_versions_pruned > 0);
    let retention = TemporalRetention {
        min_lsn: floor,
        keep_history: true,
    };
    assert_eq!(
        table_rows_as_of(
            &database,
            "items",
            &schema,
            TemporalPoint::Lsn(floor),
            retention
        )?
        .rows,
        vec![
            vec![Value::Int(1), Value::Str("anchor".to_owned())],
            vec![Value::Int(2), Value::Str("at-floor".to_owned())]
        ]
    );
    assert!(
        matches!(history_rows_for_table(&database, "items", &schema, retention),
        Err(TemporalError::RetentionExpired { requested, earliest }) if requested == anchor && earliest == floor)
    );

    // An unrelated earlier GC does not prevent filtering a complete history
    // whose own versions were all written after that GC horizon.
    let newer = database.create_table("newer", Some(schema.clone()), Vec::new())?;
    newer.insert(vec![Value::Int(1), Value::Str("older-complete".to_owned())])?;
    newer.update(vec![Value::Int(1), Value::Str("current".to_owned())])?;
    let min_lsn = database.commit_lsn()?;
    let rows = history_rows_for_table(
        &database,
        "newer",
        &schema,
        TemporalRetention {
            min_lsn,
            keep_history: true,
        },
    )?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        &rows[0][..2],
        &[Value::Int(1), Value::Str("current".to_owned())]
    );
    Ok(())
}

struct CountingHistoryReads {
    inner: Arc<SingleNode<MemEngine>>,
    commit_reads: AtomicUsize,
    commit_ranges: AtomicUsize,
}

impl Replication for CountingHistoryReads {
    fn propose(&self, op: Op) -> Result<(), ReplError> {
        self.inner.propose(op)
    }

    fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
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
        if table == txn::COMMIT_LOG_TABLE {
            self.commit_reads.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.read(table, key, consistency)
    }

    fn range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        if table == txn::COMMIT_LOG_TABLE {
            self.commit_ranges.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.range(table, start, end, consistency)
    }
}

fn history_schema() -> TableSchema {
    TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("name", ColumnType::Str, false),
        ],
        0,
    )
}

fn set_commit_times(node: &SingleNode<MemEngine>, times: &[(TxnId, u64)]) -> TestResult {
    let mut write = node.storage().begin_write()?;
    for (lsn, millis) in times {
        let bytes = write
            .get(txn::COMMIT_LOG_TABLE, &txn::commit_log_key(*lsn))?
            .ok_or("expected commit record")?;
        let mut record = txn::decode_commit_log_record(&bytes)?;
        record.committed_at = UNIX_EPOCH + Duration::from_millis(*millis);
        write.put(
            txn::COMMIT_LOG_TABLE,
            &txn::commit_log_key(*lsn),
            &txn::encode_commit_log_record(&record)?,
        )?;
    }
    write.commit()?;
    Ok(())
}

#[test]
fn history_intervals_follow_update_delete_reinsert_and_reads_are_stable() -> TestResult {
    let node = Arc::new(SingleNode::new(MemEngine::new()));
    let schema = history_schema();
    let table = RelTable::create(node.clone(), "items", Some(schema.clone()), Vec::new())?;
    let mut ops = table.insert_ops(vec![Value::Int(1), Value::Str("first".to_owned())])?;
    ops.extend(table.insert_ops(vec![Value::Int(2), Value::Str("other".to_owned())])?);
    node.propose_batch(ops)?;
    let inserted = node.current_txn_id()?;
    table.update(vec![Value::Int(1), Value::Str("updated".to_owned())])?;
    let updated = node.current_txn_id()?;
    table.delete(&Value::Int(1))?;
    let deleted = node.current_txn_id()?;
    table.insert(vec![Value::Int(1), Value::Str("reinserted".to_owned())])?;
    let reinserted = node.current_txn_id()?;
    set_commit_times(
        node.as_ref(),
        &[
            (inserted, 111),
            (updated, 222),
            (deleted, 333),
            (reinserted, 444),
        ],
    )?;
    let counted = CountingHistoryReads {
        inner: node,
        commit_reads: AtomicUsize::new(0),
        commit_ranges: AtomicUsize::new(0),
    };
    let rows = history_rows_for_table(&counted, "items", &schema, TemporalRetention::default())?;
    assert_eq!(rows.len(), 4);
    let interval = |id, name: &str, from, to: Option<TxnId>, from_ms, to_ms: Option<i64>| {
        Ok::<_, std::num::TryFromIntError>(vec![
            Value::Int(id),
            Value::Str(name.to_owned()),
            Value::Int(i64::try_from(from)?),
            to.map(i64::try_from)
                .transpose()?
                .map_or(Value::Null, Value::Int),
            Value::Int(from_ms),
            to_ms.map_or(Value::Null, Value::Int),
        ])
    };
    for expected in [
        interval(1, "first", inserted, Some(updated), 111, Some(222))?,
        interval(1, "updated", updated, Some(deleted), 222, Some(333))?,
        interval(1, "reinserted", reinserted, None, 444, None)?,
        interval(2, "other", inserted, None, 111, None)?,
    ] {
        assert!(rows.contains(&expected), "missing {expected:?} in {rows:?}");
    }
    assert_eq!(counted.commit_reads.load(Ordering::Relaxed), 4);
    assert_eq!(counted.commit_ranges.load(Ordering::Relaxed), 0);
    assert_eq!(
        history_rows_for_table(&counted, "items", &schema, TemporalRetention::default())?,
        rows
    );
    assert_eq!(counted.commit_reads.load(Ordering::Relaxed), 8);
    assert_eq!(
        rows_as_of_lsn(&counted, "items", &schema, deleted)?,
        vec![vec![Value::Int(2), Value::Str("other".to_owned())]]
    );
    Ok(())
}

#[test]
fn missing_commit_timestamps_refuse_history_without_inventing_intervals() -> TestResult {
    let node = Arc::new(SingleNode::new(MemEngine::new()));
    let schema = history_schema();
    let table = RelTable::create(node.clone(), "items", Some(schema.clone()), Vec::new())?;
    table.insert(vec![Value::Int(1), Value::Str("first".to_owned())])?;
    table.delete(&Value::Int(1))?;
    let deleted = node.current_txn_id()?;
    // Model an unavailable/pruned timestamp while its MVCC tombstone remains.
    let mut write = node.storage().begin_write()?;
    write.delete(txn::COMMIT_LOG_TABLE, &txn::commit_log_key(deleted))?;
    write.commit()?;
    assert!(matches!(
        history_rows_for_table(node.as_ref(), "items", &schema, TemporalRetention::default()),
        Err(TemporalError::MissingPoint(TemporalPoint::Lsn(lsn))) if lsn == deleted
    ));
    assert_eq!(
        rows_as_of_lsn(node.as_ref(), "items", &schema, deleted)?,
        Vec::<Row>::new()
    );
    Ok(())
}
