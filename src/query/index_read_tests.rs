use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use super::*;
use crate::{
    db::{DbConfig, Profile, create_database},
    txn::WriteAuthorization,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Default)]
struct ReadCounts {
    individual: AtomicUsize,
    batches: Mutex<Vec<usize>>,
}

impl ReadCounts {
    fn reset(&self) -> TestResult {
        self.individual.store(0, Ordering::SeqCst);
        self.batches
            .lock()
            .map_err(|_| "poisoned read counts")?
            .clear();
        Ok(())
    }

    fn batch_lengths(&self) -> Result<Vec<usize>, Box<dyn std::error::Error>> {
        Ok(self
            .batches
            .lock()
            .map_err(|_| "poisoned read counts")?
            .clone())
    }
}

struct CountedReads {
    inner: Arc<dyn Replication>,
    counts: ReadCounts,
    fault: AtomicUsize,
}

impl Replication for CountedReads {
    fn propose(&self, op: Op) -> Result<(), ReplError> {
        self.inner.propose(op)
    }

    fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
        self.inner.propose_batch(ops)
    }

    fn propose_authorized_batch(
        &self,
        ops: Vec<Op>,
        authorization: WriteAuthorization,
    ) -> Result<(), ReplError> {
        self.inner.propose_authorized_batch(ops, authorization)
    }

    fn propose_conditional_batch(&self, batch: ConditionalBatch) -> Result<(), ReplError> {
        self.inner.propose_conditional_batch(batch)
    }

    fn read(
        &self,
        table: &str,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Option<Bytes>, ReplError> {
        if table == REL_ROWS_TABLE {
            self.counts.individual.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.read(table, key, consistency)
    }

    fn read_many(
        &self,
        table: &str,
        keys: &[Bytes],
        consistency: ReadConsistency,
    ) -> Result<Vec<Option<Bytes>>, ReplError> {
        if table == REL_ROWS_TABLE {
            self.counts
                .batches
                .lock()
                .map_err(|_| ReplError::Transport("poisoned read counts".to_owned()))?
                .push(keys.len());
        }
        if self.fault.load(Ordering::SeqCst) == 3 {
            return Err(ReplError::Transport("injected batch failure".to_owned()));
        }
        let mut values = self.inner.read_many(table, keys, consistency)?;
        match self.fault.load(Ordering::SeqCst) {
            1 => {
                values.truncate(values.len().saturating_sub(1));
            }
            2 => values.push(None),
            _ => {}
        }
        Ok(values)
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
}

fn fixture(rows: i64) -> Result<(RelTable, Arc<CountedReads>), Box<dyn std::error::Error>> {
    let counted = Arc::new(CountedReads {
        inner: Arc::new(create_database(DbConfig::new(Profile::InMemory))?),
        counts: ReadCounts::default(),
        fault: AtomicUsize::new(0),
    });
    let repl: Arc<dyn Replication> = counted.clone();
    let table = RelTable::create(
        repl,
        "batch_index",
        Some(TableSchema::new(
            vec![
                ColumnDef::new("id", ColumnType::Int, false),
                ColumnDef::new("category", ColumnType::Int, false),
                ColumnDef::new("value", ColumnType::Int, false),
            ],
            0,
        )),
        vec![RelIndexSpec::new(1, 1)],
    )?;
    let mut ops = Vec::new();
    for id in 0..rows {
        ops.extend(table.insert_ops(vec![
            Value::Int(id),
            Value::Int(id % 4),
            Value::Int(id * 2),
        ])?);
    }
    counted.propose_batch(ops)?;
    counted.counts.reset()?;
    Ok((table, counted))
}

fn category_filter() -> RelPredicate {
    RelPredicate::Eq {
        expression: RelIndexExpression::Column(1),
        value: Value::Int(0),
    }
}

fn index_op(table: &RelTable, id: i64) -> Result<Op, QueryError> {
    Ok(Op::Put {
        table: REL_INDEX_TABLE.to_owned(),
        key: make_index_key(
            &table.name,
            1,
            &Value::Int(0),
            &make_row_key(&table.name, &Value::Int(id))?,
        )?,
        value: Vec::new(),
    })
}

#[test]
fn category_index_fetches_rows_in_bounded_batches() -> TestResult {
    let (table, counted) = fixture(2_400)?;
    let filter = category_filter();
    let result = table.query_filter_with_access_path(
        &filter,
        &[0, 1, 2],
        &AccessPath::BTreeIndex {
            index_id: 1,
            estimated_rows: 600,
            cost: Cost(1.0),
            covering: false,
        },
    )?;
    assert_eq!(result.plan, RelPlanKind::IndexScan(1));
    assert_eq!(result.examined_rows, 600);
    assert_eq!(result.rows.len(), 600);
    assert_eq!(counted.counts.individual.load(Ordering::SeqCst), 0);
    assert_eq!(counted.counts.batch_lengths()?, vec![256, 256, 88]);
    let reference = table.query_filter_full_scan(&filter)?;
    assert_eq!(result.rows, reference.rows);
    Ok(())
}

#[test]
fn batch_index_reads_keep_residuals_missing_duplicates_and_index_order() -> TestResult {
    let (table, counted) = fixture(64)?;
    table.update(vec![Value::Int(4), Value::Int(1), Value::Int(8)])?;
    let prefix = make_index_prefix(&table.name, 1, &Value::Int(0))?;
    let mut malformed = prefix.clone();
    malformed.extend_from_slice(&u64::MAX.to_be_bytes());
    let mut duplicate = match index_op(&table, 8)? {
        Op::Put { key, .. } => key,
        Op::Delete { .. } => return Err("expected put".into()),
    };
    duplicate.insert(prefix.len(), 0xff);
    counted.propose_batch(vec![
        index_op(&table, 4)?,
        index_op(&table, 99_999)?,
        Op::Put {
            table: REL_INDEX_TABLE.to_owned(),
            key: malformed,
            value: Vec::new(),
        },
        Op::Put {
            table: REL_INDEX_TABLE.to_owned(),
            key: duplicate,
            value: Vec::new(),
        },
    ])?;
    let filter = RelPredicate::And(vec![
        category_filter(),
        RelPredicate::Eq {
            expression: RelIndexExpression::Column(2),
            value: Value::Int(16),
        },
    ]);
    // Preserve the former per-key behavior, including duplicate index references.
    let entries = counted.range(
        REL_INDEX_TABLE,
        &prefix,
        &keyenc::range_end(&prefix),
        ReadConsistency::Strong,
    )?;
    let mut expected = Vec::new();
    for (key, _) in entries {
        if let Some(row_key) = row_key_from_index_key(&key)
            && let Some(row) = table.get_by_row_key(row_key)?
            && predicate_matches(&row, &filter)?
        {
            expected.push(row);
        }
    }
    counted.counts.reset()?;
    let result = table.query_filter_index(1, &filter)?;
    assert_eq!(result.examined_rows, 18);
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows, expected);
    assert_eq!(counted.counts.individual.load(Ordering::SeqCst), 0);
    assert_eq!(counted.counts.batch_lengths()?, vec![18]);
    let category_rows = table.query_filter_index(1, &category_filter())?;
    assert_eq!(category_rows.rows.len(), 16);
    assert!(
        category_rows.rows.iter().all(|row| row[1] == Value::Int(0)),
        "stale index references must still pass the complete predicate"
    );
    Ok(())
}

#[test]
fn index_batch_read_rejects_wrong_response_length() -> TestResult {
    let (table, counted) = fixture(4)?;
    for fault in [1, 2] {
        counted.fault.store(fault, Ordering::SeqCst);
        let error = table
            .query_filter_index(1, &category_filter())
            .err()
            .ok_or("invalid batch length must fail")?;
        assert!(
            matches!(&error, QueryError::Storage(StorageError::Corruption(_))),
            "unexpected batch contract error: {error}"
        );
    }
    Ok(())
}

#[test]
fn index_batch_read_propagates_backend_and_row_decode_errors() -> TestResult {
    let (table, counted) = fixture(4)?;
    counted.fault.store(3, Ordering::SeqCst);
    let error = table
        .query_filter_index(1, &category_filter())
        .err()
        .ok_or("batch failure must propagate")?;
    assert!(
        matches!(&error, QueryError::Repl(ReplError::Transport(_))),
        "unexpected backend error: {error}"
    );
    counted.fault.store(0, Ordering::SeqCst);
    counted.propose(Op::Put {
        table: REL_ROWS_TABLE.to_owned(),
        key: make_row_key(&table.name, &Value::Int(0))?,
        value: encode_value(&Value::Int(0))?,
    })?;
    let error = table
        .query_filter_index(1, &category_filter())
        .err()
        .ok_or("corrupt row must fail")?;
    assert!(
        matches!(&error, QueryError::Storage(StorageError::Corruption(_))),
        "unexpected row decode error: {error}"
    );
    Ok(())
}
