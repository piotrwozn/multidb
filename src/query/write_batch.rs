//! Row-layout write preparation. Uniqueness stays inside the atomic commit;
//! unchanged secondary-index entries do not need another MVCC version.

use super::{
    BTreeSet, Bytes, ConditionalBatch, InsertAttempt, Op, QueryError, REL_INDEX_TABLE,
    REL_ROWS_TABLE, RelIndexSpec, RelTable, ReplError, Row, StorageError, Value, WriteCondition,
    encode_index_payload, encode_value, index_expression_value, index_predicate_matches,
    make_index_key,
};

pub(super) fn try_plain_insert(
    table: &RelTable,
    rows: &[Row],
) -> Result<InsertAttempt, QueryError> {
    let mut keys = BTreeSet::new();
    let mut conditions = Vec::with_capacity(rows.len());
    let mut ops = Vec::new();
    for row in rows {
        let row_key = table.row_key_for_row(row)?;
        if !keys.insert(row_key.clone()) {
            return Err(QueryError::DuplicatePrimaryKey);
        }
        table.append_put_ops(&mut ops, row.clone(), &row_key)?;
        conditions.push(WriteCondition::KeyMissing {
            table: REL_ROWS_TABLE.to_owned(),
            key: row_key,
        });
    }
    // A speculative read cannot establish uniqueness. Check each key once, in
    // the same atomic batch that writes the rows and all their index entries.
    commit_insert(table, conditions, ops, rows.to_vec())
}

pub(super) fn commit_insert(
    table: &RelTable,
    conditions: Vec<WriteCondition>,
    ops: Vec<Op>,
    affected: Vec<Row>,
) -> Result<InsertAttempt, QueryError> {
    if !ops.is_empty() {
        match table
            .repl
            .propose_conditional_batch(ConditionalBatch::new(conditions, ops))
        {
            Ok(()) => {}
            Err(ReplError::Conflict | ReplError::Storage(StorageError::Conflict)) => {
                return Ok(InsertAttempt::Conflict);
            }
            Err(ReplError::Unsupported(error)) => {
                return Err(QueryError::Unsupported(format!(
                    "atomic INSERT requires conditional batch support: {error}"
                )));
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(InsertAttempt::Committed(affected))
}

pub(super) fn update_ops_for_key(
    table: &RelTable,
    old: &Row,
    row: Row,
    row_key: &[u8],
) -> Result<Vec<Op>, QueryError> {
    if table.row_key_for_row(&row)? != row_key {
        return Err(QueryError::Unsupported(
            "changing a primary key during an update is unsupported".to_owned(),
        ));
    }
    let mut ops = Vec::new();
    for index in &table.indexes {
        let old_entry = index_entry(table, index, old, row_key)?;
        let new_entry = index_entry(table, index, &row, row_key)?;
        if old_entry == new_entry {
            continue;
        }
        if let Some((old_key, _)) = old_entry {
            // A changed covering payload at the same key needs just a Put.
            if new_entry.as_ref().is_none_or(|(key, _)| *key != old_key) {
                ops.push(Op::Delete {
                    table: REL_INDEX_TABLE.to_owned(),
                    key: old_key,
                });
            }
        }
        if let Some((key, value)) = new_entry {
            ops.push(Op::Put {
                table: REL_INDEX_TABLE.to_owned(),
                key,
                value,
            });
        }
    }
    // Retain the row write even if its contents did not change: affected rows,
    // callbacks and the row's commit history keep their existing semantics.
    ops.push(Op::Put {
        table: REL_ROWS_TABLE.to_owned(),
        key: row_key.to_vec(),
        value: encode_value(&Value::Array(row))?,
    });
    Ok(ops)
}

fn index_entry(
    table: &RelTable,
    index: &RelIndexSpec,
    row: &Row,
    row_key: &[u8],
) -> Result<Option<(Bytes, Bytes)>, QueryError> {
    if !index_predicate_matches(index, row)? {
        return Ok(None);
    }
    let Some(value) = index_expression_value(row, index.expression)? else {
        return Ok(None);
    };
    Ok(Some((
        make_index_key(&table.name, index.id, &value, row_key)?,
        encode_index_payload(row, index)?,
    )))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use crate::{
        query::{
            ColumnDef, ColumnType, RelIndexExpression, RelPredicate, SqlEngine, SqlOutput,
            TableSchema,
        },
        repl::{ReadConsistency, Replication, SingleNode},
        storage::MemEngine,
        temporal, txn,
    };

    struct ObservedRepl {
        inner: SingleNode<MemEngine>,
        row_reads: AtomicUsize,
        batches: Mutex<Vec<ConditionalBatch>>,
        before_commit: Mutex<Option<Vec<Op>>>,
        commit_barrier: Mutex<Option<Arc<Barrier>>>,
    }

    impl ObservedRepl {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: SingleNode::new(MemEngine::new()),
                row_reads: AtomicUsize::new(0),
                batches: Mutex::new(Vec::new()),
                before_commit: Mutex::new(None),
                commit_barrier: Mutex::new(None),
            })
        }

        fn reset(&self) -> Result<(), ReplError> {
            self.row_reads.store(0, Ordering::SeqCst);
            self.batches
                .lock()
                .map_err(|_| ReplError::Transport("batch log poisoned".to_owned()))?
                .clear();
            Ok(())
        }

        fn batches(&self) -> Result<Vec<ConditionalBatch>, ReplError> {
            self.batches
                .lock()
                .map_err(|_| ReplError::Transport("batch log poisoned".to_owned()))
                .map(|batches| batches.clone())
        }
    }

    impl Replication for ObservedRepl {
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

        fn propose_conditional_batch(&self, batch: ConditionalBatch) -> Result<(), ReplError> {
            self.batches
                .lock()
                .map_err(|_| ReplError::Transport("batch log poisoned".to_owned()))?
                .push(batch.clone());
            let barrier = self
                .commit_barrier
                .lock()
                .map_err(|_| ReplError::Transport("barrier poisoned".to_owned()))?
                .clone();
            if let Some(barrier) = barrier {
                barrier.wait();
            }
            let concurrent = self
                .before_commit
                .lock()
                .map_err(|_| ReplError::Transport("write queue poisoned".to_owned()))?
                .take();
            if let Some(ops) = concurrent {
                self.inner.propose_batch(ops)?;
            }
            self.inner.propose_conditional_batch(batch)
        }

        fn read(
            &self,
            table: &str,
            key: &[u8],
            consistency: ReadConsistency,
        ) -> Result<Option<Bytes>, ReplError> {
            if table == REL_ROWS_TABLE {
                self.row_reads.fetch_add(1, Ordering::SeqCst);
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
            self.inner.range(table, start, end, consistency)
        }
    }

    fn schema() -> TableSchema {
        TableSchema::new(
            vec![
                ColumnDef::new("id", ColumnType::Int, false),
                ColumnDef::new("category", ColumnType::Int, false),
                ColumnDef::new("value", ColumnType::Int, false),
                ColumnDef::new("payload", ColumnType::Str, false),
                ColumnDef::new("active", ColumnType::Bool, false),
            ],
            0,
        )
    }

    fn row(id: i64, category: i64, value: i64, payload: &str, active: bool) -> Row {
        vec![
            Value::Int(id),
            Value::Int(category),
            Value::Int(value),
            Value::Str(payload.to_owned()),
            Value::Bool(active),
        ]
    }

    fn setup(
        indexes: Vec<RelIndexSpec>,
    ) -> Result<(Arc<ObservedRepl>, SqlEngine, RelTable), Box<dyn std::error::Error>> {
        let repl = ObservedRepl::new();
        let mut engine = SqlEngine::new(repl.clone());
        engine.create_table("items", Some(schema()), indexes)?;
        let table = engine
            .table("items")
            .ok_or_else(|| QueryError::MissingTable("items".to_owned()))?
            .clone();
        repl.reset()?;
        Ok((repl, engine, table))
    }

    #[tokio::test]
    async fn plain_insert_batches_all_rows_without_speculative_row_reads()
    -> Result<(), Box<dyn std::error::Error>> {
        let (repl, engine, table) = setup(vec![RelIndexSpec::new(1, 0), RelIndexSpec::new(2, 1)])?;
        let values = (0..100)
            .map(|id| format!("({id}, {}, 0, 'payload', true)", id % 3))
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(
            engine
                .execute(&format!("INSERT INTO items VALUES {values}"))
                .await?,
            SqlOutput::AffectedRows(100)
        );
        assert_eq!(repl.row_reads.load(Ordering::SeqCst), 0);
        let batches = repl.batches()?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].conditions.len(), 100);
        assert_eq!(batches[0].ops.len(), 300);
        assert!(batches[0].conditions.iter().all(|condition| matches!(
            condition,
            WriteCondition::KeyMissing { table, .. } if table == REL_ROWS_TABLE
        )));
        assert_eq!(table.scan()?.len(), 100);
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_in_storage_rolls_back_every_row_and_index_in_a_batch()
    -> Result<(), Box<dyn std::error::Error>> {
        let (repl, engine, table) = setup(vec![RelIndexSpec::new(1, 1)])?;
        table.insert(row(2, 2, 2, "existing", true))?;
        let before = repl.inner.current_txn_id()?;
        repl.reset()?;
        assert!(matches!(
            engine
                .execute(
                    "INSERT INTO items VALUES (1, 9, 1, 'first', true), \
                     (2, 9, 2, 'duplicate', true), (3, 9, 3, 'last', true)"
                )
                .await,
            Err(QueryError::DuplicatePrimaryKey)
        ));
        assert_eq!(repl.row_reads.load(Ordering::SeqCst), 0);
        assert_eq!(repl.inner.current_txn_id()?, before);
        assert_eq!(table.scan()?, vec![row(2, 2, 2, "existing", true)]);
        assert_eq!(table.query_eq(1, &Value::Int(9))?.rows, Vec::<Row>::new());
        Ok(())
    }

    #[test]
    fn duplicate_inside_input_is_rejected_before_any_commit()
    -> Result<(), Box<dyn std::error::Error>> {
        let (repl, _, table) = setup(vec![RelIndexSpec::new(1, 1)])?;
        let rows = vec![row(1, 1, 1, "first", true), row(1, 2, 2, "second", true)];
        assert!(matches!(
            try_plain_insert(&table, &rows),
            Err(QueryError::DuplicatePrimaryKey)
        ));
        assert_eq!(repl.row_reads.load(Ordering::SeqCst), 0);
        assert_eq!(repl.batches()?, Vec::new());
        assert_eq!(table.scan()?, Vec::<Row>::new());
        Ok(())
    }

    #[test]
    fn concurrent_plain_insert_commits_one_complete_batch() -> Result<(), Box<dyn std::error::Error>>
    {
        let (repl, _, table) = setup(vec![RelIndexSpec::new(1, 1)])?;
        *repl.commit_barrier.lock().map_err(|_| "barrier poisoned")? =
            Some(Arc::new(Barrier::new(2)));
        let handles = [2, 3].map(|other_id| {
            let table = table.clone();
            std::thread::spawn(move || {
                try_plain_insert(
                    &table,
                    &[
                        row(1, other_id, 1, "shared", true),
                        row(other_id, other_id, 2, "unique", true),
                    ],
                )
            })
        });
        let mut committed = 0;
        let mut conflicted = 0;
        for handle in handles {
            match handle.join().map_err(|_| "writer panicked")?? {
                InsertAttempt::Committed(affected) => {
                    assert_eq!(affected.len(), 2);
                    committed += 1;
                }
                InsertAttempt::Conflict => conflicted += 1,
            }
        }
        assert_eq!((committed, conflicted), (1, 1));
        assert_eq!(repl.row_reads.load(Ordering::SeqCst), 0);
        let rows = table.scan()?;
        assert_eq!(rows.len(), 2);
        let winner_category = &rows[0][1];
        assert_eq!(table.query_eq(1, winner_category)?.rows, rows);
        let loser = if *winner_category == Value::Int(2) {
            3
        } else {
            2
        };
        assert_eq!(
            table.query_eq(1, &Value::Int(loser))?.rows,
            Vec::<Row>::new()
        );
        Ok(())
    }

    #[tokio::test]
    async fn upsert_changes_only_row_when_index_entries_are_unchanged()
    -> Result<(), Box<dyn std::error::Error>> {
        let (repl, engine, table) = setup(vec![RelIndexSpec::new(1, 0), RelIndexSpec::new(2, 1)])?;
        let old = row(1, 2, 3, "payload", true);
        table.insert(old.clone())?;
        let before = repl.inner.current_txn_id()?;
        repl.reset()?;
        assert_eq!(
            engine
                .execute(
                    "INSERT INTO items VALUES (1, 2, 4, 'payload', true) \
                     ON CONFLICT (id) DO UPDATE SET category = EXCLUDED.category, \
                     value = EXCLUDED.value, payload = EXCLUDED.payload RETURNING *"
                )
                .await?,
            SqlOutput::Rows(crate::query::SqlRows {
                columns: vec!["id", "category", "value", "payload", "active"]
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
                rows: vec![row(1, 2, 4, "payload", true)],
            })
        );
        let batches = repl.batches()?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].ops.len(), 1);
        assert!(matches!(&batches[0].ops[0], Op::Put { table, .. } if table == REL_ROWS_TABLE));
        assert!(matches!(
            &batches[0].conditions[..],
            [WriteCondition::ValueEquals {
                expected: Some(_),
                ..
            }]
        ));
        let after = repl.inner.current_txn_id()?;
        let history = txn::history::commit_records_after(&repl.inner, before, after, 10)?;
        assert_eq!(history.records.len(), 1);
        assert_eq!(history.records[0].writes.len(), 1);
        assert_eq!(
            temporal::rows_as_of_lsn(repl.as_ref(), "items", &schema(), before)?,
            vec![old]
        );
        assert_eq!(
            temporal::rows_as_of_lsn(repl.as_ref(), "items", &schema(), after)?,
            vec![row(1, 2, 4, "payload", true)]
        );
        assert_eq!(table.query_eq(1, &Value::Int(2))?.rows, table.scan()?);
        Ok(())
    }

    fn assert_index_contents(table: &RelTable, current: &Row) -> Result<(), QueryError> {
        let mut expected = Vec::new();
        table.append_index_put_ops(&mut expected, current, &table.row_key_for_row(current)?)?;
        let mut expected = expected
            .into_iter()
            .filter_map(|op| match op {
                Op::Put { key, value, .. } => Some((key, value)),
                Op::Delete { .. } => None,
            })
            .collect::<Vec<_>>();
        expected.sort();
        let (start, end) = super::super::table_range_bounds("items");
        assert_eq!(
            table
                .repl
                .range(REL_INDEX_TABLE, &start, &end, ReadConsistency::Strong)?,
            expected
        );
        Ok(())
    }

    #[test]
    fn index_deltas_handle_covering_payload_expression_and_partial_membership()
    -> Result<(), Box<dyn std::error::Error>> {
        let (repl, _, table) = setup(vec![
            RelIndexSpec::new(1, 1),
            RelIndexSpec::new(2, 1).with_include(vec![2]),
            RelIndexSpec::lower_ascii(3, 3),
            RelIndexSpec::new(4, 1).with_predicate(RelPredicate::Eq {
                expression: RelIndexExpression::Column(4),
                value: Value::Bool(true),
            }),
        ])?;
        table.insert(row(1, 2, 10, "MiXeD", true))?;
        repl.reset()?;
        let changed = row(1, 2, 20, "mixed", false);
        table.update(changed.clone())?;
        let batches = repl.batches()?;
        // The covering payload changes in place; partial membership is removed.
        // The plain category and normalized text entries remain untouched.
        assert_eq!(batches[0].ops.len(), 3);
        assert_eq!(
            batches[0]
                .ops
                .iter()
                .filter(|op| matches!(op, Op::Delete { .. }))
                .count(),
            1
        );
        assert_index_contents(&table, &changed)?;

        repl.reset()?;
        let moved = row(1, 3, 20, "other", true);
        table.update(moved.clone())?;
        assert_eq!(repl.batches()?[0].ops.len(), 8);
        assert_index_contents(&table, &moved)?;
        repl.reset()?;
        table.update(moved.clone())?;
        assert_eq!(repl.batches()?[0].ops.len(), 1);
        assert_index_contents(&table, &moved)?;
        Ok(())
    }

    #[tokio::test]
    async fn upsert_retry_uses_fresh_row_and_fresh_index_delta()
    -> Result<(), Box<dyn std::error::Error>> {
        let (repl, engine, table) = setup(vec![RelIndexSpec::new(1, 1)])?;
        table.insert(row(1, 2, 3, "old", true))?;
        let concurrent = row(1, 8, 3, "concurrent", true);
        *repl
            .before_commit
            .lock()
            .map_err(|_| "write queue poisoned")? = Some(table.update_ops(concurrent)?);
        repl.reset()?;
        engine
            .execute(
                "INSERT INTO items VALUES (1, 2, 4, 'ignored', false) \
                 ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value",
            )
            .await?;
        let batches = repl.batches()?;
        assert_eq!(batches.len(), 2);
        assert!(batches.iter().all(|batch| batch.ops.len() == 1));
        assert_ne!(batches[0].conditions, batches[1].conditions);
        assert_eq!(table.scan()?, vec![row(1, 8, 4, "concurrent", true)]);
        assert_index_contents(&table, &row(1, 8, 4, "concurrent", true))?;
        Ok(())
    }

    #[tokio::test]
    async fn repeated_upsert_keys_keep_sequential_results_and_final_index_contents()
    -> Result<(), Box<dyn std::error::Error>> {
        let (repl, engine, table) = setup(vec![RelIndexSpec::new(1, 1).with_include(vec![2])])?;
        table.insert(row(1, 2, 0, "old", true))?;
        repl.reset()?;
        let output = engine
            .execute(
                "INSERT INTO items VALUES (1, 3, 10, 'first', true), \
                 (1, 2, 20, 'second', true) ON CONFLICT (id) DO UPDATE SET \
                 category = EXCLUDED.category, value = EXCLUDED.value RETURNING category, value",
            )
            .await?;
        assert!(matches!(output, SqlOutput::Rows(rows) if rows.rows == vec![
            vec![Value::Int(3), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
        ]));
        let batches = repl.batches()?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].conditions.len(), 1);
        assert_eq!(table.scan()?, vec![row(1, 2, 20, "old", true)]);
        assert_index_contents(&table, &row(1, 2, 20, "old", true))?;
        assert_eq!(table.query_eq(1, &Value::Int(3))?.rows, Vec::<Row>::new());
        Ok(())
    }

    #[tokio::test]
    async fn invalid_upsert_replacements_cannot_change_schema_or_primary_key()
    -> Result<(), Box<dyn std::error::Error>> {
        let (repl, engine, table) = setup(vec![RelIndexSpec::new(1, 1)])?;
        let original = row(1, 2, 3, "original", true);
        table.insert(original.clone())?;
        repl.reset()?;
        assert!(matches!(
            engine
                .execute(
                    "INSERT INTO items VALUES (1, 2, 4, 'new', true) \
                     ON CONFLICT (id) DO UPDATE SET value = 'wrong type'"
                )
                .await,
            Err(QueryError::InvalidRow(_))
        ));
        assert!(matches!(
            engine
                .execute(
                    "INSERT INTO items VALUES (1, 2, 4, 'new', true) \
                     ON CONFLICT (id) DO UPDATE SET id = 2"
                )
                .await,
            Err(QueryError::Unsupported(_))
        ));
        assert_eq!(repl.batches()?, Vec::new());
        assert_eq!(table.scan()?, vec![original.clone()]);
        assert_index_contents(&table, &original)?;
        Ok(())
    }
}
