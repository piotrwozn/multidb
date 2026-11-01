use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use datafusion::error::DataFusionError;
use sqlparser::{
    ast::{BinaryOperator, Expr as SqlExpr, SetExpr, Statement},
    dialect::GenericDialect,
    parser::{Parser, ParserError},
};

use super::{
    AccessPath, AnalyzeMode, AnalyzeTarget, CardinalityEstimator, ColumnDef, ColumnType,
    ExplainOptions, MostCommonValue, QueryError, QueryFingerprint, REL_COLUMNAR_SEGMENTS_TABLE,
    REL_INDEX_TABLE, REL_ROWS_TABLE, RelIndexExpression, RelIndexSpec, RelPlanKind, RelPredicate,
    RelTable, SqlEngine, SqlOutput, SqlReadGuard, SqlReadTarget, StatsCatalog, TableLayout,
    TableSchema, Value, datafusion_to_query_error, make_row_key, parse, render_statement,
};
use crate::{
    db::{DbConfig, Profile, create_database, open_database},
    model::{CollectionId, FieldPath},
    repl::{ConditionalBatch, Op, ReadConsistency, ReplError, Replication, WriteCondition},
    security::{AuthzError, Permission, Resource},
    storage::Bytes,
    txn::WriteAuthorization,
};

fn user_schema() -> TableSchema {
    TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("name", ColumnType::Str, false),
            ColumnDef::new("age", ColumnType::Int, false),
            ColumnDef::new("active", ColumnType::Bool, true),
        ],
        0,
    )
}

fn order_schema() -> TableSchema {
    TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("user_id", ColumnType::Int, false),
            ColumnDef::new("amount", ColumnType::Float, false),
        ],
        0,
    )
}

fn event_schema() -> TableSchema {
    TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("category", ColumnType::Str, false),
            ColumnDef::new("price", ColumnType::Float, true),
            ColumnDef::new("payload", ColumnType::Bytes, true),
            ColumnDef::new("active", ColumnType::Bool, true),
        ],
        0,
    )
}

fn status_schema() -> TableSchema {
    TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("status", ColumnType::Str, false),
        ],
        0,
    )
}

fn memory_repl() -> Result<Arc<dyn Replication>, Box<dyn std::error::Error>> {
    Ok(Arc::new(create_database(DbConfig::new(Profile::InMemory))?))
}

#[test]
fn relational_table_round_trips_on_memory() -> Result<(), Box<dyn std::error::Error>> {
    let table = RelTable::create(memory_repl()?, "users", Some(user_schema()), Vec::new())?;

    table.insert(vec![
        Value::Int(1),
        Value::Str("Ada".to_owned()),
        Value::Int(37),
        Value::Bool(true),
    ])?;

    assert_eq!(
        table.get(&Value::Int(1))?,
        Some(vec![
            Value::Int(1),
            Value::Str("Ada".to_owned()),
            Value::Int(37),
            Value::Bool(true),
        ])
    );

    Ok(())
}

#[test]
fn relational_table_round_trips_on_redb() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("rel.redb");
    let repl: Arc<dyn Replication> = Arc::new(create_database(DbConfig::on_disk(
        Profile::Transactional,
        path,
    ))?);
    let table = RelTable::create(repl, "users", Some(user_schema()), Vec::new())?;

    table.insert(vec![
        Value::Int(2),
        Value::Str("Grace".to_owned()),
        Value::Int(85),
        Value::Bool(false),
    ])?;

    assert_eq!(
        table.scan()?,
        vec![vec![
            Value::Int(2),
            Value::Str("Grace".to_owned()),
            Value::Int(85),
            Value::Bool(false),
        ]]
    );

    Ok(())
}

#[test]
fn columnar_table_round_trips_through_parquet() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("columnar.redb");
    let repl: Arc<dyn Replication> = Arc::new(create_database(DbConfig::on_disk(
        Profile::Analytical,
        path,
    ))?);
    let table = RelTable::create_with_layout(
        repl.clone(),
        "events",
        Some(event_schema()),
        Vec::new(),
        TableLayout::Columnar,
    )?;

    table.insert(vec![
        Value::Int(2),
        Value::Str("b".to_owned()),
        Value::Float(20.0),
        Value::Bytes(vec![2, 3]),
        Value::Bool(false),
    ])?;
    table.insert(vec![
        Value::Int(1),
        Value::Str("a".to_owned()),
        Value::Null,
        Value::Bytes(vec![1]),
        Value::Null,
    ])?;

    assert_eq!(table.layout(), TableLayout::Columnar);
    assert_eq!(
        table.get(&Value::Int(1))?,
        Some(vec![
            Value::Int(1),
            Value::Str("a".to_owned()),
            Value::Null,
            Value::Bytes(vec![1]),
            Value::Null,
        ])
    );
    assert_eq!(
        table
            .scan()?
            .into_iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(1), Value::Int(2)]
    );

    table.update(vec![
        Value::Int(1),
        Value::Str("a".to_owned()),
        Value::Float(10.5),
        Value::Bytes(vec![9]),
        Value::Bool(true),
    ])?;
    assert_eq!(
        table
            .get(&Value::Int(1))?
            .and_then(|row| row.get(2).cloned()),
        Some(Value::Float(10.5))
    );

    table.delete(&Value::Int(2))?;
    assert_eq!(table.get(&Value::Int(2))?, None);
    let (start, end) = table.columnar_segment_bounds();
    assert!(
        !repl
            .range(
                REL_COLUMNAR_SEGMENTS_TABLE,
                &start,
                &end,
                ReadConsistency::Strong
            )?
            .is_empty(),
        "expected a non-empty value"
    );

    Ok(())
}

#[test]
fn columnar_rewrite_uses_multiple_segments() -> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let table = RelTable::create_with_layout(
        repl.clone(),
        "events",
        Some(event_schema()),
        Vec::new(),
        TableLayout::Columnar,
    )?;
    let rows = (0..(super::COLUMNAR_SEGMENT_ROWS + 3))
        .map(|id| {
            vec![
                Value::Int(i64::try_from(id).unwrap_or(i64::MAX)),
                Value::Str(format!("cat{}", id % 4)),
                Value::Float(f64::from(u32::try_from(id).unwrap_or(u32::MAX))),
                Value::Bytes(vec![u8::try_from(id % 255).unwrap_or_default()]),
                Value::Bool(id % 2 == 0),
            ]
        })
        .collect::<Vec<_>>();

    repl.propose_batch(table.columnar_replace_ops(rows.clone())?)?;
    let (start, end) = super::columnar_segment_range_bounds("events");
    let segments = repl.range(
        REL_COLUMNAR_SEGMENTS_TABLE,
        &start,
        &end,
        ReadConsistency::Strong,
    )?;

    assert!(segments.len() > 1);
    assert_eq!(table.scan()?.len(), rows.len());
    assert_eq!(table.scan()?, rows);
    Ok(())
}

#[test]
fn columnar_rewrite_conflicts_when_segments_change_after_the_read()
-> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let table = RelTable::create_with_layout(
        repl.clone(),
        "statuses",
        Some(status_schema()),
        Vec::new(),
        TableLayout::Columnar,
    )?;
    let row = |id: i64, status: &str| vec![Value::Int(id), Value::Str(status.to_owned())];

    // Another writer creates the first segment of an empty table.
    let empty = table.columnar_segments()?;
    table.insert(row(1, "a"))?;
    let stale = table.columnar_rewrite_batch(&empty, vec![row(2, "b")])?;
    assert!(matches!(
        repl.propose_conditional_batch(stale),
        Err(ReplError::Conflict)
    ));

    // Another writer rewrites a segment that was read.
    let read = table.columnar_segments()?;
    table.update(row(1, "changed"))?;
    let stale = table.columnar_rewrite_batch(&read, vec![row(1, "a"), row(2, "b")])?;
    assert!(matches!(
        repl.propose_conditional_batch(stale),
        Err(ReplError::Conflict)
    ));

    // Another writer only adds a segment after the ones that were read.
    let read = table.columnar_segments()?;
    let (_, first_segment) = read.first().cloned().ok_or("missing first segment")?;
    repl.propose_batch(vec![Op::Put {
        table: REL_COLUMNAR_SEGMENTS_TABLE.to_owned(),
        key: super::columnar_segment_key("statuses", 1),
        value: first_segment,
    }])?;
    let stale = table.columnar_rewrite_batch(&read, vec![row(1, "changed"), row(2, "b")])?;
    assert!(matches!(
        repl.propose_conditional_batch(stale),
        Err(ReplError::Conflict)
    ));

    // A rewrite built from the current segments commits and replaces all of them.
    let current = table.columnar_segments()?;
    assert_eq!(current.len(), 2);
    let fresh = table.columnar_rewrite_batch(&current, vec![row(1, "changed"), row(2, "b")])?;
    repl.propose_conditional_batch(fresh)?;
    assert_eq!(table.scan()?, vec![row(1, "changed"), row(2, "b")]);
    assert_eq!(table.columnar_segments()?.len(), 1);

    Ok(())
}

#[test]
fn columnar_append_keeps_closed_segments_and_primary_key_order()
-> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let table = RelTable::create_with_layout(
        repl.clone(),
        "statuses",
        Some(status_schema()),
        Vec::new(),
        TableLayout::Columnar,
    )?;
    let row = |id: i64| vec![Value::Int(id), Value::Str(format!("value-{id}"))];
    let original = (0..2_500).map(row).collect::<Vec<_>>();
    repl.propose_batch(table.columnar_replace_ops(original)?)?;
    let before = table.columnar_segments()?;
    assert_eq!(before.len(), 3);

    let ops = table.columnar_append_ops(&before, &[row(-1)])?;
    assert_eq!(
        ops.len(),
        2,
        "only the partial tail and its metadata are written"
    );
    table.insert(row(-1))?;
    let after = table.columnar_segments()?;
    assert_eq!(
        &after[..2],
        &before[..2],
        "closed Parquet segments must stay byte-for-byte unchanged"
    );
    assert_ne!(after[2], before[2]);
    assert_eq!(table.scan()?, (-1..2_500).map(row).collect::<Vec<_>>());
    assert_eq!(table.row_count()?, 2_501);
    assert_eq!(table.scan_page(0, 2)?, vec![row(-1), row(0)]);
    assert!(matches!(
        table.insert(row(1_500)),
        Err(QueryError::DuplicatePrimaryKey)
    ));
    assert_eq!(
        table.columnar_segments()?,
        after,
        "a rejected duplicate must write nothing"
    );

    // Missing metadata in older/raw-written databases still counts correctly.
    repl.propose(Op::Delete {
        table: super::REL_COLUMNAR_SEGMENT_META_TABLE.to_owned(),
        key: after[1].0.clone(),
    })?;
    assert_eq!(table.row_count()?, 2_501);
    Ok(())
}

#[test]
fn columnar_append_rejects_stale_batches_and_batch_duplicates()
-> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let table = RelTable::create_with_layout(
        repl.clone(),
        "statuses",
        Some(status_schema()),
        Vec::new(),
        TableLayout::Columnar,
    )?;
    let row = |id: i64| vec![Value::Int(id), Value::Str(format!("value-{id}"))];
    repl.propose_batch(table.columnar_replace_ops((0..1_024).map(row).collect())?)?;
    let before = table.columnar_segments()?;
    let stale = table.columnar_append_batch(&before, &[row(2_000)])?;
    table.insert(row(2_001))?;
    assert!(matches!(
        repl.propose_conditional_batch(stale),
        Err(ReplError::Conflict)
    ));
    assert_eq!(table.get(&Value::Int(2_000))?, None);
    assert_eq!(table.get(&Value::Int(2_001))?, Some(row(2_001)));
    assert_eq!(table.columnar_segments()?[0], before[0]);

    let before_duplicate = table.columnar_segments()?;
    assert!(matches!(
        table.append_columnar_rows(&[row(3_000), row(3_000)]),
        Err(QueryError::DuplicatePrimaryKey)
    ));
    assert_eq!(table.columnar_segments()?, before_duplicate);
    Ok(())
}

#[tokio::test]
async fn columnar_sql_insert_appends_without_rewriting_closed_segments()
-> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let mut engine = SqlEngine::with_layout(repl.clone(), TableLayout::Columnar);
    engine.create_table("statuses", Some(status_schema()), Vec::new())?;
    let table = engine.table("statuses").ok_or("missing statuses")?;
    let row = |id: i64| vec![Value::Int(id), Value::Str(format!("value-{id}"))];
    repl.propose_batch(table.columnar_replace_ops((0..2_048).map(row).collect())?)?;
    let before = table.columnar_segments()?;
    engine
        .execute("INSERT INTO statuses VALUES (-1, 'early'), (3000, 'late')")
        .await?;
    let after = table.columnar_segments()?;
    assert_eq!(&after[..2], before.as_slice());
    assert_eq!(after.len(), 3);
    assert_eq!(table.row_count()?, 2_050);
    assert_eq!(table.scan_page(0, 1)?[0][0], Value::Int(-1));
    assert!(matches!(
        engine
            .execute("INSERT INTO statuses VALUES (4000, 'new'), (2, 'duplicate')")
            .await,
        Err(QueryError::DuplicatePrimaryKey)
    ));
    assert_eq!(table.get(&Value::Int(4_000))?, None);
    assert_eq!(table.columnar_segments()?, after);
    Ok(())
}

#[test]
fn columnar_append_keeps_tiered_segments_immutable_and_counts_without_downloading()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::cloud::{ObjectStoreConfig, TieredSegmentPointer, encode_tiered_segment_pointer};

    let repl = memory_repl()?;
    let table = RelTable::create_with_layout(
        repl.clone(),
        "statuses",
        Some(status_schema()),
        Vec::new(),
        TableLayout::Columnar,
    )?;
    let row = |id: i64| vec![Value::Int(id), Value::Str(format!("value-{id}"))];
    repl.propose_batch(table.columnar_replace_ops((0..2_048).map(row).collect())?)?;
    let before = table.columnar_segments()?;
    let objects = tempfile::tempdir()?;
    let object_path = objects.path().join("closed.parquet");
    std::fs::write(&object_path, &before[0].1)?;
    let pointer = encode_tiered_segment_pointer(&TieredSegmentPointer {
        uri: ObjectStoreConfig::local_dir(objects.path()).uri,
        path: "closed.parquet".to_owned(),
        bytes: u64::try_from(before[0].1.len())?,
        checksum: crc32fast::hash(&before[0].1),
    })?;
    repl.propose(Op::Put {
        table: REL_COLUMNAR_SEGMENTS_TABLE.to_owned(),
        key: before[0].0.clone(),
        value: pointer.clone(),
    })?;
    table.insert(row(-1))?;
    assert_eq!(table.columnar_segments()?[0].1, pointer);
    assert_eq!(std::fs::read(&object_path)?, before[0].1);
    assert_eq!(table.row_count()?, 2_049);

    // Catalog counts use the paired row-count metadata even if the object store
    // is unavailable; actual data reads must still fail rather than invent rows.
    std::fs::remove_file(object_path)?;
    assert_eq!(table.row_count()?, 2_049);
    assert!(table.scan().is_err());
    Ok(())
}

#[test]
fn columnar_zone_map_skips_segments_for_simple_filter() -> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let table = RelTable::create_with_layout(
        repl.clone(),
        "events",
        Some(event_schema()),
        Vec::new(),
        TableLayout::Columnar,
    )?;
    let rows = (0..(super::COLUMNAR_SEGMENT_ROWS + 3))
        .map(|id| {
            vec![
                Value::Int(i64::try_from(id).unwrap_or(i64::MAX)),
                Value::Str(format!("cat{}", id % 4)),
                Value::Float(f64::from(u32::try_from(id).unwrap_or(u32::MAX))),
                Value::Bytes(vec![u8::try_from(id % 255).unwrap_or_default()]),
                Value::Bool(id % 2 == 0),
            ]
        })
        .collect::<Vec<_>>();
    repl.propose_batch(table.columnar_replace_ops(rows)?)?;

    let filter = RelPredicate::Eq {
        expression: RelIndexExpression::Column(0),
        value: Value::Int(1),
    };
    let report = table.segment_skip_report(&filter)?;

    assert_eq!(report.scanned_segments, 1);
    assert!(report.skipped_segments >= 1);
    assert!(report.skipped_bytes > 0);
    Ok(())
}

/// Forwards to `inner`, but commits one queued write right after each read of
/// `interleave_after`, the way a concurrent writer lands between the storage
/// reads of one statement, and counts conditional batches that conflicted. With
/// `conditional_writes` off it refuses conditional batches, like backends that
/// cannot check conditions.
struct ScriptedRepl {
    inner: Arc<dyn Replication>,
    interleave_after: &'static str,
    writes: Mutex<VecDeque<Vec<Op>>>,
    conflicts: AtomicUsize,
    conditional_writes: bool,
    interleave_at_commit: AtomicBool,
}

impl ScriptedRepl {
    fn new(
        interleave_after: &'static str,
        conditional_writes: bool,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        Ok(Arc::new(Self {
            inner: memory_repl()?,
            interleave_after,
            writes: Mutex::new(VecDeque::new()),
            conflicts: AtomicUsize::new(0),
            conditional_writes,
            interleave_at_commit: AtomicBool::new(false),
        }))
    }

    fn queue(&self, ops: Vec<Op>) -> Result<(), ReplError> {
        self.writes
            .lock()
            .map_err(|_| ReplError::Transport("write queue poisoned".to_owned()))?
            .push_back(ops);
        Ok(())
    }

    fn queued(&self) -> Result<usize, ReplError> {
        Ok(self
            .writes
            .lock()
            .map_err(|_| ReplError::Transport("write queue poisoned".to_owned()))?
            .len())
    }

    fn conflicts(&self) -> usize {
        self.conflicts.load(Ordering::SeqCst)
    }

    fn interleave(&self, table: &str) -> Result<(), ReplError> {
        if table != self.interleave_after {
            return Ok(());
        }
        let write = self
            .writes
            .lock()
            .map_err(|_| ReplError::Transport("write queue poisoned".to_owned()))?
            .pop_front();
        match write {
            Some(ops) => self.inner.propose_batch(ops),
            None => Ok(()),
        }
    }
}

impl Replication for ScriptedRepl {
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
        if !self.conditional_writes && !batch.conditions.is_empty() {
            return Err(ReplError::Unsupported(
                "conditional batches are not supported".to_owned(),
            ));
        }
        if self.interleave_at_commit.load(Ordering::SeqCst)
            && batch.conditions.iter().any(|condition| match condition {
                WriteCondition::KeyMissing { table, .. }
                | WriteCondition::ValueEquals { table, .. } => table == self.interleave_after,
            })
        {
            self.interleave(self.interleave_after)?;
        }
        let result = self.inner.propose_conditional_batch(batch);
        if matches!(result, Err(ReplError::Conflict)) {
            self.conflicts.fetch_add(1, Ordering::SeqCst);
        }
        result
    }

    fn read(
        &self,
        table: &str,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Option<Bytes>, ReplError> {
        let value = self.inner.read(table, key, consistency)?;
        if !self.interleave_at_commit.load(Ordering::SeqCst) {
            self.interleave(table)?;
        }
        Ok(value)
    }

    fn range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        let rows = self.inner.range(table, start, end, consistency)?;
        if !self.interleave_at_commit.load(Ordering::SeqCst) {
            self.interleave(table)?;
        }
        Ok(rows)
    }
}

#[test]
fn columnar_zone_maps_are_read_from_the_same_commit_as_the_segments()
-> Result<(), Box<dyn std::error::Error>> {
    let repl = ScriptedRepl::new(REL_COLUMNAR_SEGMENTS_TABLE, true)?;
    let table = RelTable::create_with_layout(
        repl.clone(),
        "statuses",
        Some(status_schema()),
        Vec::new(),
        TableLayout::Columnar,
    )?;
    let row = |id: i64| vec![Value::Int(id), Value::Str(format!("s{id}"))];
    let rows = |ids: std::ops::RangeInclusive<i64>| ids.map(row).collect::<Vec<_>>();
    let boundary = i64::try_from(super::COLUMNAR_SEGMENT_ROWS)?;
    let last = boundary * 2 - 48;
    // Segment 0 holds 1..=boundary and segment 1 the rest.
    repl.propose_batch(table.columnar_replace_ops(rows(1..=last))?)?;
    let filter = RelPredicate::Eq {
        expression: RelIndexExpression::Column(0),
        value: Value::Int(boundary),
    };

    // Inserting id 0 between the segment read and the zone map read moves the
    // boundary row into segment 1. Paired with the old segment bytes, the new
    // zone maps would skip the only segment that holds it.
    repl.queue(table.columnar_replace_ops(rows(0..=last))?)?;
    let result = table.query_filter_columnar(&filter)?;
    assert_eq!(repl.queued()?, 0);
    assert_eq!(result.rows, vec![row(boundary)]);
    // The re-read still prunes: only the new segment 1 (boundary..=last) is decoded.
    assert_eq!(result.examined_rows, usize::try_from(last - boundary + 1)?);

    // When commits keep landing between the reads, every segment is scanned.
    // One commit more than there are attempts is queued: each segment read
    // applies one, so the one left over shows that the unpruned scan reuses the
    // segments the last attempt read instead of reading them again.
    for key in 0..=u8::try_from(super::SEGMENT_PRUNING_READ_ATTEMPTS)? {
        repl.queue(vec![Op::Put {
            table: "unrelated".to_owned(),
            key: vec![key],
            value: vec![key],
        }])?;
    }
    let result = table.query_filter_columnar(&filter)?;
    assert_eq!(repl.queued()?, 1);
    assert_eq!(result.rows, vec![row(boundary)]);
    assert_eq!(result.examined_rows, usize::try_from(last + 1)?);
    repl.interleave(REL_COLUMNAR_SEGMENTS_TABLE)?;
    assert_eq!(repl.queued()?, 0);

    let report = table.segment_skip_report(&filter)?;
    assert_eq!(report.scanned_segments, 1);
    assert_eq!(report.skipped_segments, 1);
    Ok(())
}

#[test]
fn columnar_writes_fall_back_to_plain_batches_without_conditional_writes()
-> Result<(), Box<dyn std::error::Error>> {
    let repl = ScriptedRepl::new(REL_COLUMNAR_SEGMENTS_TABLE, false)?;
    let table = RelTable::create_with_layout(
        repl.clone(),
        "statuses",
        Some(status_schema()),
        Vec::new(),
        TableLayout::Columnar,
    )?;
    let row = |id: i64, status: &str| vec![Value::Int(id), Value::Str(status.to_owned())];

    table.insert(row(1, "a"))?;
    table.insert(row(2, "b"))?;
    assert!(matches!(
        table.insert(row(2, "again")),
        Err(QueryError::DuplicatePrimaryKey)
    ));
    table.update(row(1, "changed"))?;
    table.delete(&Value::Int(2))?;

    assert_eq!(table.scan()?, vec![row(1, "changed")]);
    Ok(())
}

#[test]
fn columnar_layout_rejects_schemaless_and_btree_indexes() -> Result<(), Box<dyn std::error::Error>>
{
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("columnar-invalid.redb");
    let repl: Arc<dyn Replication> = Arc::new(create_database(DbConfig::on_disk(
        Profile::Analytical,
        path,
    ))?);

    assert!(matches!(
        RelTable::create_with_layout(
            repl.clone(),
            "events",
            None,
            Vec::new(),
            TableLayout::Columnar
        ),
        Err(QueryError::InvalidSchema(_))
    ));
    assert!(matches!(
        RelTable::create_with_layout(
            repl,
            "events",
            Some(event_schema()),
            vec![RelIndexSpec::new(1, 1)],
            TableLayout::Columnar
        ),
        Err(QueryError::InvalidSchema(_))
    ));

    Ok(())
}

#[test]
fn schema_rejects_bad_rows_and_preserves_primary_key_order()
-> Result<(), Box<dyn std::error::Error>> {
    let table = RelTable::create(memory_repl()?, "users", Some(user_schema()), Vec::new())?;

    assert!(matches!(
        table.insert(vec![Value::Int(1), Value::Str("Bad".to_owned())]),
        Err(QueryError::InvalidRow(_))
    ));

    table.insert(vec![
        Value::Int(10),
        Value::Str("Ten".to_owned()),
        Value::Int(10),
        Value::Null,
    ])?;
    table.insert(vec![
        Value::Int(-10),
        Value::Str("Minus".to_owned()),
        Value::Int(10),
        Value::Null,
    ])?;

    let keys = table
        .scan()?
        .into_iter()
        .map(|row| row[0].clone())
        .collect::<Vec<_>>();
    assert_eq!(keys, vec![Value::Int(-10), Value::Int(10)]);

    Ok(())
}

#[test]
fn schema_metadata_survives_redb_reopen() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("schema.redb");
    let config = DbConfig::on_disk(Profile::Transactional, path);

    {
        let repl: Arc<dyn Replication> = Arc::new(create_database(config.clone())?);
        RelTable::create(repl, "users", Some(user_schema()), Vec::new())?;
    }

    let repl: Arc<dyn Replication> = Arc::new(open_database(config)?);
    let table = RelTable::open(repl, "users", Vec::new())?;
    assert_eq!(table.schema(), Some(&user_schema()));

    Ok(())
}

#[test]
fn schemaless_table_accepts_one_value_column() -> Result<(), Box<dyn std::error::Error>> {
    let table = RelTable::create(memory_repl()?, "events", None, Vec::new())?;
    let row = vec![Value::Str("raw event".to_owned())];

    table.insert(row.clone())?;
    assert_eq!(table.get(&Value::Str("raw event".to_owned()))?, Some(row));

    Ok(())
}

#[test]
fn parser_accepts_select_and_insert() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(parse("select * from users")?.len(), 1);
    assert_eq!(
        parse("insert into users (id, name, age) values (1, 'Ada', 37)")?.len(),
        1
    );

    Ok(())
}

#[test]
fn parser_enforces_phase28_input_limits() {
    let too_many_statements = "select 1;".repeat(33);
    assert!(matches!(
        parse(&too_many_statements),
        Err(QueryError::InputLimit(_))
    ));

    let oversized = format!("select '{}'", "x".repeat(1024 * 1024));
    assert!(matches!(parse(&oversized), Err(QueryError::InputLimit(_))));

    let values = format!(
        "insert into users values {}",
        std::iter::repeat_n("(1)", 10_001)
            .collect::<Vec<_>>()
            .join(",")
    );
    assert!(matches!(parse(&values), Err(QueryError::InputLimit(_))));
}

#[test]
fn datafusion_sql_parser_errors_are_parse_errors() {
    let recursion = DataFusionError::SQL(
        Box::new(ParserError::RecursionLimitExceeded),
        Some(" (current limit: 50)".to_owned()),
    );
    assert!(matches!(
        datafusion_to_query_error(&recursion),
        QueryError::Parse(message) if message.contains("RecursionLimitExceeded")
    ));
    let wrapped = DataFusionError::Context(
        "planning".to_owned(),
        Box::new(DataFusionError::SQL(
            Box::new(ParserError::ParserError(
                "Expected: an expression, found: memory".to_owned(),
            )),
            None,
        )),
    );
    assert!(matches!(
        datafusion_to_query_error(&wrapped),
        QueryError::Parse(_)
    ));

    assert!(matches!(
        datafusion_to_query_error(&DataFusionError::Internal("bug".to_owned())),
        QueryError::DataFusion(_)
    ));
    assert!(matches!(
        datafusion_to_query_error(&DataFusionError::Plan("bad plan".to_owned())),
        QueryError::DataFusion(_)
    ));
    assert!(matches!(
        datafusion_to_query_error(&DataFusionError::ResourcesExhausted("memory".to_owned())),
        QueryError::ResourceLimit(_)
    ));
}

#[test]
fn query_fingerprint_keeps_identifier_digits() {
    assert_ne!(
        QueryFingerprint::new("select * from t1 where id = 1"),
        QueryFingerprint::new("select * from t2 where id = 1")
    );
    assert_eq!(
        QueryFingerprint::new("select * from t1 where id = 1"),
        QueryFingerprint::new("select * from t1 where id = 2")
    );
}

#[tokio::test]
async fn sql_select_where_and_insert_work() -> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let mut engine = SqlEngine::new(repl);
    engine.create_table("users", Some(user_schema()), Vec::new())?;

    assert_eq!(
        engine
            .execute("insert into users (id, name, age, active) values (1, 'Ada', 37, true)")
            .await?,
        SqlOutput::AffectedRows(1)
    );

    let output = engine
        .execute("select name from users where age = 37")
        .await?;

    assert_eq!(
        output,
        SqlOutput::Rows(super::SqlRows {
            columns: vec!["name".to_owned()],
            rows: vec![vec![Value::Str("Ada".to_owned())]],
        })
    );

    Ok(())
}

#[test]
fn create_with_layout_rejects_existing_schema_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    RelTable::create_with_layout(
        repl.clone(),
        "users",
        Some(user_schema()),
        Vec::new(),
        TableLayout::Row,
    )?;
    assert!(matches!(
        RelTable::create_with_layout(repl, "users", Some(user_schema()), Vec::new(), TableLayout::Row),
        Err(QueryError::InvalidSchema(message)) if message.contains("already exists")
    ));
    Ok(())
}

#[tokio::test]
async fn multi_row_insert_is_atomic_when_late_row_duplicates_primary_key()
-> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let mut engine = SqlEngine::new(repl);
    engine.create_table("users", Some(user_schema()), Vec::new())?;

    let result = engine
        .execute(
            "insert into users (id, name, age, active) values \
             (1, 'Ada', 37, true), \
             (2, 'Grace', 85, true), \
             (1, 'Duplicate', 99, false)",
        )
        .await;

    assert!(matches!(result, Err(QueryError::DuplicatePrimaryKey)));
    assert!(matches!(
        engine.execute("select id from users").await?,
        SqlOutput::Rows(rows) if rows.rows.is_empty()
    ));

    Ok(())
}

#[tokio::test]
async fn row_insert_that_loses_a_race_maps_its_conflict_by_on_conflict_clause()
-> Result<(), Box<dyn std::error::Error>> {
    let repl = ScriptedRepl::new(REL_ROWS_TABLE, true)?;
    repl.interleave_at_commit.store(true, Ordering::SeqCst);
    let mut engine = SqlEngine::new(repl.clone());
    engine.create_table("users", Some(user_schema()), Vec::new())?;
    let table = RelTable::open(repl.clone(), "users", Vec::new())?;
    let user = |id: i64, name: &str, age: i64| {
        vec![
            Value::Int(id),
            Value::Str(name.to_owned()),
            Value::Int(age),
            Value::Bool(true),
        ]
    };

    // Another writer inserts the key immediately before the atomic commit: a
    // plain INSERT reports the duplicate key instead of retrying, without a
    // speculative read of that key.
    repl.queue(table.insert_ops(user(1, "Ada", 36))?)?;
    let result = engine
        .execute("INSERT INTO users (id, name, age, active) VALUES (1, 'Mine', 1, true)")
        .await;
    assert!(matches!(result, Err(QueryError::DuplicatePrimaryKey)));
    assert_eq!((repl.queued()?, repl.conflicts()), (0, 1));

    // ON CONFLICT reads the row again and applies its action to the new version.
    repl.queue(table.insert_ops(user(2, "Grace", 85))?)?;
    engine
        .execute(
            "INSERT INTO users (id, name, age, active) VALUES (2, 'Mine', 1, true) \
             ON CONFLICT (id) DO NOTHING",
        )
        .await?;
    repl.queue(table.update_ops(user(1, "Ada Lovelace", 36))?)?;
    engine
        .execute(
            "INSERT INTO users (id, name, age, active) VALUES (1, 'Mine', 37, true) \
             ON CONFLICT (id) DO UPDATE SET age = EXCLUDED.age",
        )
        .await?;
    assert_eq!((repl.queued()?, repl.conflicts()), (0, 3));

    assert_eq!(
        table.scan()?,
        vec![user(1, "Ada Lovelace", 37), user(2, "Grace", 85)]
    );
    Ok(())
}

#[tokio::test]
async fn sql_fast_path_matches_null_numeric_and_fallback_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let mut engine = SqlEngine::new(repl);
    engine.create_table("users", Some(user_schema()), Vec::new())?;
    engine.create_table("orders", Some(order_schema()), Vec::new())?;

    engine
        .execute("insert into users (id, name, age, active) values (1, 'Ada', 37, null)")
        .await?;
    engine
        .execute("insert into orders (id, user_id, amount) values (10, 1, 5.0)")
        .await?;

    let null_cmp = engine
        .execute("select name from users where active = null")
        .await?;
    assert!(matches!(null_cmp, SqlOutput::Rows(rows) if rows.rows.is_empty()));

    let float_cmp = engine
        .execute("select id from orders where amount = 5")
        .await?;
    assert!(matches!(
        float_cmp,
        SqlOutput::Rows(rows) if rows.rows == vec![vec![Value::Int(10)]]
    ));

    let expression_filter = engine
        .execute("select name from users where age + 1 = 38")
        .await?;
    assert!(matches!(
        expression_filter,
        SqlOutput::Rows(rows) if rows.rows == vec![vec![Value::Str("Ada".to_owned())]]
    ));

    Ok(())
}

#[tokio::test]
async fn datafusion_runs_join_and_group_by() -> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let mut engine = SqlEngine::new(repl);
    engine.create_table("users", Some(user_schema()), Vec::new())?;
    engine.create_table("orders", Some(order_schema()), Vec::new())?;

    engine
        .execute(
            "insert into users (id, name, age, active) values \
                 (1, 'Ada', 37, true); \
                 insert into users (id, name, age, active) values \
                 (2, 'Grace', 85, true)",
        )
        .await?;
    engine
        .execute(
            "insert into orders (id, user_id, amount) values (10, 1, 5.5); \
                 insert into orders (id, user_id, amount) values (11, 1, 7.0); \
                 insert into orders (id, user_id, amount) values (12, 2, 3.0)",
        )
        .await?;

    let join = engine
        .execute(
            "select users.name, orders.amount \
                 from users join orders on users.id = orders.user_id \
                 where orders.amount > 5.0",
        )
        .await?;
    assert!(matches!(join, SqlOutput::Rows(rows) if rows.rows.len() == 2));

    let grouped = engine
        .execute("select user_id, count(*) from orders group by user_id")
        .await?;
    assert!(matches!(grouped, SqlOutput::Rows(rows) if rows.rows.len() == 2));

    Ok(())
}

#[tokio::test]
async fn columnar_sql_group_by_matches_row_layout() -> Result<(), Box<dyn std::error::Error>> {
    let row_repl = memory_repl()?;
    let mut row_engine = SqlEngine::new(row_repl);
    row_engine.create_table("events", Some(event_schema()), Vec::new())?;

    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("columnar-sql.redb");
    let columnar_repl: Arc<dyn Replication> = Arc::new(create_database(DbConfig::on_disk(
        Profile::Analytical,
        path,
    ))?);
    let mut columnar_engine = SqlEngine::with_layout(columnar_repl, TableLayout::Columnar);
    columnar_engine.create_table("events", Some(event_schema()), Vec::new())?;

    let inserts = "\
            insert into events (id, category, price, payload, active) values (1, 'a', 10.0, null, true); \
            insert into events (id, category, price, payload, active) values (2, 'a', 20.0, null, true); \
            insert into events (id, category, price, payload, active) values (3, 'b', 7.0, null, false)";
    row_engine.execute(inserts).await?;
    columnar_engine.execute(inserts).await?;

    let sql =
        "select category, avg(price), count(*) from events group by category order by category";
    assert_eq!(
        columnar_engine.execute(sql).await?,
        row_engine.execute(sql).await?
    );

    let projected = columnar_engine
        .execute("select category from events order by category")
        .await?;
    assert!(matches!(projected, SqlOutput::Rows(rows) if rows.columns == vec!["category"]));

    Ok(())
}

#[tokio::test]
async fn unsupported_sql_returns_error() -> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let engine = SqlEngine::new(repl);

    assert!(matches!(
        engine.execute("create trigger t").await,
        Err(QueryError::Parse(_) | QueryError::Unsupported(_))
    ));

    Ok(())
}

#[tokio::test]
async fn datafusion_reads_storage_provider_and_empty_table()
-> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let mut engine = SqlEngine::new(repl);
    engine.create_table("users", Some(user_schema()), Vec::new())?;

    let output = engine.execute("select id from users").await?;
    assert_eq!(
        output,
        SqlOutput::Rows(super::SqlRows {
            columns: vec!["id".to_owned()],
            rows: Vec::new(),
        })
    );

    Ok(())
}

#[tokio::test]
async fn datafusion_fallback_handles_schemaless_table() -> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let mut engine = SqlEngine::new(repl);
    engine.create_table("events", None, Vec::new())?;
    engine
        .execute("insert into events values ('raw event')")
        .await?;

    assert_eq!(
        engine
            .execute("select value from events where value = 'raw event'")
            .await?,
        SqlOutput::Rows(super::SqlRows {
            columns: vec!["value".to_owned()],
            rows: vec![vec![Value::Str("raw event".to_owned())]],
        })
    );

    Ok(())
}

#[tokio::test]
async fn open_table_invalidates_plan_cache() -> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let mut engine = SqlEngine::new(repl);
    engine.create_table("users", Some(user_schema()), vec![RelIndexSpec::new(1, 2)])?;
    engine
        .execute("insert into users (id, name, age, active) values (1, 'Ada', 37, true)")
        .await?;
    engine
        .execute("select name from users where age = 37")
        .await?;
    assert_eq!(engine.plan_cache_metrics().entries, 1);

    engine.open_table_with_layout("users", vec![RelIndexSpec::new(1, 2)], TableLayout::Row)?;
    assert_eq!(engine.plan_cache_metrics().entries, 0);
    Ok(())
}

#[test]
fn relational_index_is_maintained() -> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let table = RelTable::create(
        repl.clone(),
        "users",
        Some(user_schema()),
        vec![RelIndexSpec::new(1, 2)],
    )?;

    table.insert(vec![
        Value::Int(1),
        Value::Str("Ada".to_owned()),
        Value::Int(37),
        Value::Bool(true),
    ])?;
    table.insert(vec![
        Value::Int(2),
        Value::Str("Grace".to_owned()),
        Value::Int(85),
        Value::Bool(true),
    ])?;

    let indexed = table.query_eq(2, &Value::Int(37))?;
    assert_eq!(indexed.plan, RelPlanKind::IndexScan(1));
    assert_eq!(indexed.examined_rows, 1);
    assert_eq!(indexed.rows[0][1], Value::Str("Ada".to_owned()));

    table.update(vec![
        Value::Int(1),
        Value::Str("Ada".to_owned()),
        Value::Int(38),
        Value::Bool(true),
    ])?;
    assert!(
        table.query_eq(2, &Value::Int(37))?.rows.is_empty(),
        "expected an empty value, got {:?}",
        table.query_eq(2, &Value::Int(37))?.rows
    );
    assert_eq!(table.query_eq(2, &Value::Int(38))?.rows.len(), 1);

    table.delete(&Value::Int(1))?;
    assert!(
        table.query_eq(2, &Value::Int(38))?.rows.is_empty(),
        "expected an empty value, got {:?}",
        table.query_eq(2, &Value::Int(38))?.rows
    );

    let row_key = make_row_key("users", &Value::Int(2))?;
    assert!(
        repl.read(REL_ROWS_TABLE, &row_key, ReadConsistency::Strong)?
            .is_some()
    );
    assert!(
        !repl
            .range(REL_INDEX_TABLE, &[], &[0xFF], ReadConsistency::Strong)?
            .is_empty(),
        "expected a non-empty value"
    );

    Ok(())
}

#[test]
fn relational_index_matches_full_scan_for_ff_row_key_and_negative_zero()
-> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let schema = TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Bytes, false),
            ColumnDef::new("score", ColumnType::Float, false),
            ColumnDef::new("name", ColumnType::Str, false),
        ],
        0,
    );
    let indexed = RelTable::create(
        repl.clone(),
        "ff_rows",
        Some(schema),
        vec![RelIndexSpec::new(1, 1)],
    )?;
    indexed.insert(vec![
        Value::Bytes(vec![0xAA, 0xFF, 0x00]),
        Value::Float(-0.0),
        Value::Str("edge".to_owned()),
    ])?;
    let full_scan = RelTable::open(repl, "ff_rows", Vec::new())?;

    let indexed_result = indexed.query_eq(1, &Value::Float(0.0))?;
    let scan_result = full_scan.query_eq(1, &Value::Float(0.0))?;

    assert_eq!(indexed_result.plan, RelPlanKind::IndexScan(1));
    assert_eq!(scan_result.plan, RelPlanKind::FullScan);
    assert_eq!(indexed_result.rows, scan_result.rows);
    assert_eq!(
        indexed_result.rows,
        vec![vec![
            Value::Bytes(vec![0xAA, 0xFF, 0x00]),
            Value::Float(-0.0),
            Value::Str("edge".to_owned()),
        ]]
    );

    Ok(())
}

#[test]
fn analyze_collects_stats_and_costs_choose_index_or_scan() -> Result<(), Box<dyn std::error::Error>>
{
    let table = RelTable::create(
        memory_repl()?,
        "users",
        Some(user_schema()),
        vec![RelIndexSpec::new(1, 2), RelIndexSpec::new(2, 3)],
    )?;

    for id in 0..100 {
        table.insert(vec![
            Value::Int(id),
            Value::Str(format!("user-{id}")),
            Value::Int(if id == 7 { 37 } else { 50 }),
            Value::Bool(id < 95),
        ])?;
    }

    let stats = table.analyze(AnalyzeMode::Full)?;
    assert_eq!(stats.row_count, 100);
    assert_eq!(stats.columns["age"].ndv, 2);
    assert!(stats.columns["age"].histogram.len() <= 10);

    let selective = table.query_eq(2, &Value::Int(37))?;
    assert_eq!(selective.plan, RelPlanKind::IndexScan(1));
    assert_eq!(selective.rows.len(), 1);

    let unselective = table.query_eq(3, &Value::Bool(true))?;
    assert_eq!(unselective.plan, RelPlanKind::FullScan);
    assert_eq!(unselective.rows.len(), 95);
    Ok(())
}

#[tokio::test]
async fn partial_index_is_used_only_when_query_implies_predicate()
-> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let mut engine = SqlEngine::new(repl);
    let active = RelPredicate::Eq {
        expression: RelIndexExpression::Column(3),
        value: Value::Bool(true),
    };
    engine.create_table(
        "users",
        Some(user_schema()),
        vec![RelIndexSpec::new(1, 2).with_predicate(active)],
    )?;
    engine
        .execute("insert into users (id, name, age, active) values (1, 'Ada', 37, true)")
        .await?;
    engine
        .execute("insert into users (id, name, age, active) values (2, 'Bob', 37, false)")
        .await?;

    let implied = engine.explain(
        "select name from users where age = 37 and active = true",
        ExplainOptions::default(),
    )?;
    assert!(matches!(
        implied.nodes[0].access_path,
        AccessPath::BTreeIndex { index_id: 1, .. }
    ));

    let not_implied = engine.explain(
        "select name from users where age = 37",
        ExplainOptions::default(),
    )?;
    assert!(matches!(
        not_implied.nodes[0].access_path,
        AccessPath::SeqScan { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn expression_covering_index_keeps_explicit_ascii_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let mut engine = SqlEngine::new(repl);
    engine.create_table(
        "users",
        Some(user_schema()),
        vec![RelIndexSpec::lower_ascii(1, 1).with_include(vec![0])],
    )?;
    engine
        .execute("insert into users (id, name, age, active) values (1, 'ADA', 37, true)")
        .await?;

    let sql = "select id from users where lower(name) = 'ada'";
    let output = engine.execute(sql).await?;
    assert_eq!(
        output,
        SqlOutput::Rows(super::SqlRows {
            columns: vec!["id".to_owned()],
            rows: vec![vec![Value::Int(1)]],
        })
    );
    // SQL LOWER has Unicode semantics. The explicitly ASCII index remains usable
    // through the relational API, while SQL goes through its full function planner.
    let table = engine.tables.get("users").ok_or("missing users")?;
    let filter = super::RelPredicate::Eq {
        expression: super::RelIndexExpression::LowerAscii(1),
        value: Value::Str("ada".to_owned()),
    };
    let result = table.query_filter_index_only(1, &filter, &[0])?;
    assert_eq!(result.plan, super::RelPlanKind::IndexOnlyScan(1));
    assert_eq!(result.rows.len(), 1);
    let explain = engine.explain(sql, ExplainOptions::default())?;
    assert!(matches!(
        explain.nodes[0].access_path,
        AccessPath::DataFusionPlan
    ));
    Ok(())
}

#[tokio::test]
async fn bitmap_and_combines_selective_indexes() -> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let mut engine = SqlEngine::new(repl);
    engine.create_table(
        "users",
        Some(user_schema()),
        vec![RelIndexSpec::new(1, 2), RelIndexSpec::new(2, 3)],
    )?;
    for id in 0..20 {
        engine
            .execute(&format!(
                "insert into users (id, name, age, active) values ({id}, 'u{id}', {}, {})",
                if id == 7 { 37 } else { 50 },
                if id % 2 == 0 { "true" } else { "false" }
            ))
            .await?;
    }

    let sql = "select name from users where age = 37 and active = false";
    let output = engine.execute(sql).await?;
    assert!(matches!(
        output,
        SqlOutput::Rows(rows) if rows.rows == vec![vec![Value::Str("u7".to_owned())]]
    ));
    let explain = engine.explain(sql, ExplainOptions::default())?;
    assert!(matches!(
        explain.nodes[0].access_path,
        AccessPath::BitmapIndex { .. }
    ));
    Ok(())
}

#[test]
fn histogram_estimates_range_better_than_fallback() -> Result<(), Box<dyn std::error::Error>> {
    let table = RelTable::create(memory_repl()?, "users", Some(user_schema()), Vec::new())?;
    for id in 0..100 {
        table.insert(vec![
            Value::Int(id),
            Value::Str(format!("user-{id}")),
            Value::Int(id),
            Value::Bool(true),
        ])?;
    }

    let stats = table.analyze(AnalyzeMode::Full)?;
    let age = &stats.columns["age"];
    let selectivity =
        CardinalityEstimator::range_selectivity(age, &Value::Int(10), &Value::Int(20));
    assert!(selectivity < 0.30);
    assert!(selectivity > 0.0);
    Ok(())
}

#[tokio::test]
async fn sql_analyze_plan_cache_and_explain_work() -> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let mut engine = SqlEngine::new(repl.clone());
    engine.create_table("users", Some(user_schema()), vec![RelIndexSpec::new(1, 2)])?;
    for id in 0..20 {
        engine
            .execute(&format!(
                "insert into users (id, name, age, active) values ({id}, 'u{id}', {}, true)",
                if id == 3 { 37 } else { 50 }
            ))
            .await?;
    }

    let analyzed = engine.execute("ANALYZE FULL users").await?;
    assert!(matches!(analyzed, SqlOutput::Rows(rows) if rows.rows.len() == 1));

    let sql = "select name from users where age = 37";
    let first = engine.execute(sql).await?;
    let second = engine.execute(sql).await?;
    assert_eq!(first, second);
    assert_eq!(engine.plan_cache_metrics().hits, 1);

    let explain = engine.explain(sql, ExplainOptions { analyze: true })?;
    assert!(matches!(
        explain.nodes[0].access_path,
        AccessPath::BTreeIndex { index_id: 1, .. }
    ));
    assert_eq!(explain.nodes[0].actual_rows, Some(1));
    Ok(())
}

#[test]
fn stats_survive_redb_reopen() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("stats.redb");
    let config = DbConfig::on_disk(Profile::Transactional, path);

    {
        let repl: Arc<dyn Replication> = Arc::new(create_database(config.clone())?);
        let table = RelTable::create(repl.clone(), "users", Some(user_schema()), Vec::new())?;
        table.insert(vec![
            Value::Int(1),
            Value::Str("Ada".to_owned()),
            Value::Int(37),
            Value::Bool(true),
        ])?;
        table.analyze(AnalyzeMode::Full)?;
    }

    let repl: Arc<dyn Replication> = Arc::new(open_database(config)?);
    let stats =
        StatsCatalog::read_table(repl.as_ref(), "users")?.ok_or("missing persisted stats")?;
    assert_eq!(stats.row_count, 1);
    assert!(stats.columns.contains_key("age"));
    Ok(())
}

#[tokio::test]
async fn document_provider_stats_cover_catalog_fields() -> Result<(), Box<dyn std::error::Error>> {
    let repl = memory_repl()?;
    let collection = crate::model::DocumentCollection::new(repl.as_ref(), CollectionId::new(7));
    collection.insert(&Value::Object(BTreeMap::from([
        ("user_id".to_owned(), Value::Int(1)),
        ("city".to_owned(), Value::Str("Warsaw".to_owned())),
        ("ignored".to_owned(), Value::Str("x".to_owned())),
    ])))?;

    let mut engine = SqlEngine::new(repl);
    engine.register_collection(
        "users",
        CollectionId::new(7),
        vec![super::DocField::path(
            "city",
            FieldPath::new(["city"]),
            ColumnType::Str,
        )],
        Vec::new(),
    )?;
    let report = engine.analyze(AnalyzeTarget::Named("users".to_owned()), AnalyzeMode::Full)?;
    assert_eq!(report.analyzed[0].row_count, 1);
    assert!(report.analyzed[0].columns.contains_key("city"));
    assert!(!report.analyzed[0].columns.contains_key("ignored"));
    Ok(())
}

#[tokio::test]
async fn explain_analyze_records_feedback_for_bad_stats() -> Result<(), Box<dyn std::error::Error>>
{
    let repl = memory_repl()?;
    let mut engine = SqlEngine::new(repl.clone());
    engine.create_table("items", Some(status_schema()), Vec::new())?;
    for id in 0..50 {
        engine
            .execute(&format!(
                "insert into items (id, status) values ({id}, 'active')"
            ))
            .await?;
    }

    let bad_stats = super::TableStats {
        object_name: "items".to_owned(),
        object_kind: super::StatsObjectKind::Table,
        row_count: 50,
        sample_count: 50,
        columns: BTreeMap::from([(
            "status".to_owned(),
            super::ColumnStats {
                row_count: 50,
                sample_count: 50,
                ndv: 10_000,
                null_frac: 0.0,
                min: Some(Value::Str("active".to_owned())),
                max: Some(Value::Str("active".to_owned())),
                histogram: Vec::new(),
                mcv: vec![MostCommonValue {
                    value: Value::Str("inactive".to_owned()),
                    count: 1,
                    frequency: 0.02,
                }],
                stats_version: 1,
                analyzed_at_txn: 1,
            },
        )]),
        stats_version: 1,
        analyzed_at_txn: 1,
    };
    StatsCatalog::write_table(repl.as_ref(), &bad_stats)?;

    let report = engine.explain(
        "select id from items where status = 'active'",
        ExplainOptions { analyze: true },
    )?;
    assert_eq!(report.nodes[0].actual_rows, Some(50));
    let feedback = StatsCatalog::read_feedback(repl.as_ref())?;
    assert_eq!(feedback.len(), 1);
    assert!(feedback[0].ratio >= 10.0);
    Ok(())
}

/// Records every read it is asked about and denies catalog reads outside
/// `allowed`, unless `allow_all` is set.
#[derive(Default)]
struct RecordingReadGuard {
    allow_all: bool,
    allowed: BTreeSet<SqlReadTarget>,
    seen: BTreeSet<SqlReadTarget>,
    policies: BTreeMap<String, super::SqlRelationPolicy>,
}

impl RecordingReadGuard {
    fn allowing(names: &[&str]) -> Self {
        Self {
            allowed: names
                .iter()
                .map(|name| SqlReadTarget::Catalog((*name).to_owned()))
                .collect(),
            ..Self::default()
        }
    }

    fn allowing_all() -> Self {
        Self {
            allow_all: true,
            ..Self::default()
        }
    }
}

impl SqlReadGuard for RecordingReadGuard {
    fn relation_policies(&self) -> BTreeMap<String, super::SqlRelationPolicy> {
        self.policies.clone()
    }

    fn authorize_reads(&mut self, reads: &BTreeSet<SqlReadTarget>) -> Result<(), AuthzError> {
        self.seen.extend(reads.iter().cloned());
        for read in reads {
            if let SqlReadTarget::Catalog(name) = read
                && !self.allow_all
                && !self.allowed.contains(read)
            {
                return Err(AuthzError::Denied {
                    principal: "tester".to_owned(),
                    resource: Resource::Table(name.clone()),
                    permission: Permission::Read,
                });
            }
        }
        Ok(())
    }
}

async fn guarded_engine() -> Result<SqlEngine, Box<dyn std::error::Error>> {
    let mut engine = SqlEngine::new(memory_repl()?);
    engine.create_table("public_t", Some(user_schema()), Vec::new())?;
    engine.create_table("secrets", Some(user_schema()), Vec::new())?;
    engine
        .execute("insert into secrets (id, name, age, active) values (1, 'TOPSECRET', 1, true)")
        .await?;
    Ok(engine)
}

#[tokio::test]
async fn describe_checks_all_reads_without_executing_or_writing()
-> Result<(), Box<dyn std::error::Error>> {
    let engine = guarded_engine().await?;
    let mut guard = RecordingReadGuard::allowing(&["public_t"]);
    for sql in [
        "select * from secrets where id = $1",
        "select name from public_t order by (select count(*) from secrets)",
    ] {
        assert!(
            matches!(
                engine.describe_with_read_guard(sql, &mut guard).await,
                Err(QueryError::AuthzDenied(_))
            ),
            "{sql}"
        );
    }
    let mut guard = RecordingReadGuard::allowing_all();
    // The stored row's age is 1: executing this expression would divide by zero.
    let schema = engine
        .describe_with_read_guard("select 10 / (age - 1) as quotient from secrets", &mut guard)
        .await?
        .ok_or("missing SELECT schema")?;
    assert_eq!(schema.field(0).name(), "quotient");
    assert_eq!(
        schema.field(0).data_type(),
        &datafusion::arrow::datatypes::DataType::Int64
    );
    let schema = engine.describe_with_read_guard(
        "insert into secrets (id, name, age, active) values (2, 'new', 2, true) returning id, name as n",
        &mut guard).await?.ok_or("missing RETURNING schema")?;
    assert_eq!(schema.field(1).name(), "n");
    guard.policies.insert(
        "secrets".to_owned(),
        super::SqlRelationPolicy {
            masks: vec![(FieldPath::new(["age"]), Value::Str("hidden".to_owned()))],
            ..super::SqlRelationPolicy::default()
        },
    );
    let schema = engine
        .describe_with_read_guard("select age from secrets", &mut guard)
        .await?
        .ok_or("missing masked schema")?;
    assert_eq!(
        schema.field(0).data_type(),
        &datafusion::arrow::datatypes::DataType::Utf8
    );
    let SqlOutput::Rows(rows) = engine.execute("select id from secrets").await? else {
        return Err("missing rows".into());
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1)]]);
    Ok(())
}

#[tokio::test]
async fn read_guard_reports_planned_tables_in_subqueries() -> Result<(), Box<dyn std::error::Error>>
{
    let engine = guarded_engine().await?;
    let secrets = SqlReadTarget::Catalog("secrets".to_owned());

    // DataFusion plans each of these, so the guard must see `secrets`.
    for sql in [
        "select trim((select name from secrets limit 1)) from public_t",
        "select name from public_t where name like (select name from secrets limit 1)",
        "select (select name from secrets limit 1) from public_t",
        "select name from public_t where exists (select 1 from secrets)",
        "select name from public_t where name in (select name from secrets)",
        "select name from public_t order by (select count(*) from secrets)",
        "select name from public_t union select name from secrets",
        "select n from (select name as n from secrets) as d",
        "with \"Secrets\" as (select 1 as id) select name from secrets",
    ] {
        let mut guard = RecordingReadGuard::allowing(&["public_t"]);
        let result = engine.execute_with_read_guard(sql, Some(&mut guard)).await;
        assert!(
            matches!(result, Err(QueryError::AuthzDenied(_))),
            "{sql}: {result:?}"
        );
        assert!(guard.seen.contains(&secrets), "{sql}: {:?}", guard.seen);
    }

    // DataFusion may reject these shapes while planning; either way nothing
    // may run unless the guard saw `secrets`.
    for sql in [
        "select substring((select name from secrets limit 1) from 1 for 3) from public_t",
        "select name from public_t limit (select count(*) from secrets)",
        "select name from public_t limit 1 offset (select count(*) from secrets)",
        "select count(*) from public_t group by (select name from secrets limit 1)",
        "select * from (values ((select name from secrets limit 1))) as v(leak)",
        "select row_number() over (partition by (select name from secrets limit 1)) from public_t",
    ] {
        let mut guard = RecordingReadGuard::allowing(&["public_t"]);
        let result = engine.execute_with_read_guard(sql, Some(&mut guard)).await;
        assert!(result.is_err(), "{sql}: {result:?}");
        assert!(
            guard.seen.is_empty() || guard.seen.contains(&secrets),
            "{sql}: {:?}",
            guard.seen
        );
    }
    Ok(())
}

#[tokio::test]
async fn read_guard_maps_scans_to_registered_names() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = guarded_engine().await?;
    engine.register_collection(
        "profiles",
        CollectionId::new(7),
        vec![super::DocField::path(
            "city",
            FieldPath::new(["city"]),
            ColumnType::Str,
        )],
        Vec::new(),
    )?;
    engine.create_table("Mixed", Some(status_schema()), Vec::new())?;
    let catalog = |name: &str| BTreeSet::from([SqlReadTarget::Catalog(name.to_owned())]);

    for (sql, expected) in [
        (
            "with c as (select * from public_t) select * from c",
            catalog("public_t"),
        ),
        ("select city from profiles", catalog("profiles")),
        ("select status from MIXED", catalog("Mixed")),
        ("select status from mixed", catalog("Mixed")),
        ("select 1", BTreeSet::new()),
        (
            "with recursive r as (select 1 as n union all select n + 1 from r where n < 3) \
             select * from r",
            BTreeSet::new(),
        ),
    ] {
        let mut guard = RecordingReadGuard::allowing_all();
        engine
            .execute_with_read_guard(sql, Some(&mut guard))
            .await?;
        assert_eq!(guard.seen, expected, "{sql}");
    }

    for sql in [
        "select table_name from information_schema.tables",
        "select * from generate_series(1, 3)",
    ] {
        let mut guard = RecordingReadGuard::allowing_all();
        engine
            .execute_with_read_guard(sql, Some(&mut guard))
            .await?;
        assert!(!guard.seen.is_empty(), "{sql}");
        assert!(
            guard
                .seen
                .iter()
                .all(|read| matches!(read, SqlReadTarget::Builtin(_))),
            "{sql}: {:?}",
            guard.seen
        );
    }
    Ok(())
}

#[tokio::test]
async fn read_guard_denies_before_any_statement_runs() -> Result<(), Box<dyn std::error::Error>> {
    let engine = guarded_engine().await?;

    let mut guard = RecordingReadGuard::allowing(&["public_t"]);
    let result = engine
        .execute_with_read_guard(
            "insert into public_t (id, name, age, active) values (1, 'Ada', 37, true); \
             select name from secrets",
            Some(&mut guard),
        )
        .await;
    assert!(
        matches!(result, Err(QueryError::AuthzDenied(_))),
        "{result:?}"
    );
    assert_eq!(
        engine.execute("select id from public_t").await?,
        SqlOutput::Rows(super::SqlRows {
            columns: vec!["id".to_owned()],
            rows: Vec::new(),
        })
    );

    for sql in [
        "explain select name from secrets where id = 1",
        "explain analyze select name from secrets where id = 1",
    ] {
        let mut guard = RecordingReadGuard::allowing(&["public_t"]);
        let result = engine.execute_with_read_guard(sql, Some(&mut guard)).await;
        assert!(
            matches!(result, Err(QueryError::AuthzDenied(_))),
            "{sql}: {result:?}"
        );
    }

    let mut guard = RecordingReadGuard::allowing(&["public_t"]);
    assert!(matches!(
        engine
            .execute_with_read_guard(
                "explain select name from public_t where id = 1",
                Some(&mut guard)
            )
            .await?,
        SqlOutput::Rows(_)
    ));
    assert_eq!(
        guard.seen,
        BTreeSet::from([SqlReadTarget::Catalog("public_t".to_owned())])
    );

    let mut guard = RecordingReadGuard::allowing(&["public_t"]);
    match engine
        .execute_with_read_guard(
            "explain select name from public_t where name like (select name from secrets)",
            Some(&mut guard),
        )
        .await
    {
        Err(QueryError::AuthzDenied(_)) => {}
        other => panic!("non-simple EXPLAIN must authorize subquery reads, got {other:?}"),
    }
    assert_eq!(
        guard.seen,
        BTreeSet::from([
            SqlReadTarget::Catalog("public_t".to_owned()),
            SqlReadTarget::Catalog("secrets".to_owned()),
        ])
    );
    Ok(())
}

#[tokio::test]
async fn datafusion_read_plans_reject_non_read_nodes() -> Result<(), Box<dyn std::error::Error>> {
    let engine = guarded_engine().await?;

    let result = engine.execute("select name into t2 from public_t").await;
    assert!(
        matches!(result, Err(QueryError::Unsupported(_))),
        "{result:?}"
    );
    Ok(())
}

/// Queries over `public_t` whose filter no row passes. The engine plans each
/// statement from its re-rendered text; printed back with sqlparser's printer,
/// a string literal that holds a backslash before its quote ended early there,
/// and the rest of it ran as SQL that commented out the filter.
const REQUOTED_VALUE_QUERIES: &[&str] = &[
    "select id from public_t where name = '\\'' or 1=1 --' and id = 99",
    "select id from public_t where name = '\\'' or id = 1 --' and id = 99",
    "select id from public_t where name = N'x'' or 1=1 --' and id = 99",
];

/// The same for a quoted name, which has no escaped form.
const REQUOTED_NAME_QUERIES: &[&str] = &[
    "select id as 'a\\'' from public_t --' from public_t where id = 99",
    "select id as \"a\\\"\" from public_t --\" from public_t where id = 99",
];

#[tokio::test]
async fn requoted_values_cannot_drop_a_filter() -> Result<(), Box<dyn std::error::Error>> {
    assert_filters_hold(REQUOTED_VALUE_QUERIES).await
}

#[tokio::test]
async fn requoted_names_cannot_drop_a_filter() -> Result<(), Box<dyn std::error::Error>> {
    assert_filters_hold(REQUOTED_NAME_QUERIES).await
}

/// Runs each of `queries` over two rows that their filters reject.
async fn assert_filters_hold(queries: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    let engine = guarded_engine().await?;
    engine
        .execute(
            "insert into public_t (id, name, age, active) \
             values (1, 'Ada', 37, true), (2, 'Bob', 41, false)",
        )
        .await?;

    for sql in queries {
        match engine.execute(sql).await {
            Ok(SqlOutput::Rows(rows)) => assert!(rows.rows.is_empty(), "{sql}: {:?}", rows.rows),
            Err(QueryError::Unsupported(_)) => {}
            other => return Err(format!("{sql}: {other:?}").into()),
        }
    }
    Ok(())
}

#[tokio::test]
async fn requoted_values_cannot_add_reads() -> Result<(), Box<dyn std::error::Error>> {
    // Reads only `public_t`. Re-rendered with sqlparser's printer, the rest of
    // the value read `secrets`.
    assert_reads_stay_within_public_t(&[
        "select id from public_t where name = '\\'' union select id from secrets --'",
    ])
    .await
}

#[tokio::test]
async fn requoted_names_cannot_add_reads() -> Result<(), Box<dyn std::error::Error>> {
    assert_reads_stay_within_public_t(&[
        "select id as 'a\\'' from secrets --' from public_t",
        "select id as \"a\\\"\" from secrets --\" from public_t",
        "insert into public_t (id, name, age, active) values (5, 'x', 1, true) \
         returning id as \"a\\\"\" from secrets --\"",
    ])
    .await
}

/// Describes, executes and explains each statement with a guard that allows
/// `public_t` only, and checks that nothing planned a read of `secrets`.
async fn assert_reads_stay_within_public_t(
    statements: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let engine = guarded_engine().await?;
    let secrets = SqlReadTarget::Catalog("secrets".to_owned());
    for sql in statements {
        let explained = format!("explain {sql}");
        for statement in [*sql, explained.as_str()] {
            let mut guard = RecordingReadGuard::allowing(&["public_t"]);
            let described = engine.describe_with_read_guard(statement, &mut guard).await;
            assert!(
                !guard.seen.contains(&secrets)
                    && !matches!(described, Err(QueryError::AuthzDenied(_))),
                "describe {statement}: {described:?} {:?}",
                guard.seen
            );
            if statement.starts_with("insert") {
                continue;
            }
            let mut guard = RecordingReadGuard::allowing(&["public_t"]);
            let executed = engine
                .execute_with_read_guard(statement, Some(&mut guard))
                .await;
            assert!(
                !guard.seen.contains(&secrets)
                    && !matches!(executed, Err(QueryError::AuthzDenied(_))),
                "execute {statement}: {executed:?} {:?}",
                guard.seen
            );
        }
    }
    Ok(())
}

/// String values that try to end a literal early, in every escaping style.
const QUOTE_VALUES: &[&str] = &[
    "\\' or 1=1 --",
    "\\'",
    "\\\\'",
    "''",
    "'''",
    "a\\'b",
    "it's",
    "\\",
    "x' or 'a'='a",
    "line\nbreak",
    "tab\there",
];

#[test]
fn render_statement_round_trips_string_values() -> Result<(), Box<dyn std::error::Error>> {
    let limits = crate::performance::QueryExecutionConfig::default();
    for value in QUOTE_VALUES {
        for prefix in ["", "N"] {
            let sql = format!(
                "select name from public_t where name = {prefix}'{}' and id = 1",
                value.replace('\'', "''")
            );
            let [statement] = <[Statement; 1]>::try_from(parse(&sql)?)
                .map_err(|_| std::io::Error::other("expected one statement"))?;
            let rendered = render_statement(statement, &limits)?;

            for (dialect, statements) in [
                ("postgres", parse(&rendered)?),
                ("generic", Parser::parse_sql(&GenericDialect {}, &rendered)?),
            ] {
                assert_eq!(
                    name_filter_literal(&statements).as_deref(),
                    Some(*value),
                    "{dialect}: {value:?} -> {rendered}"
                );
            }
        }
    }

    for sql in [
        "SELECT name FROM public_t WHERE name LIKE 'a\\%' ESCAPE '\\'",
        "SELECT \"Name\", 2 ^ 3 AS p, 'a' || name AS q FROM public_t WHERE id::BIGINT = 1",
        "SELECT name AS 'label', \"odd \"\" name\" FROM public_t",
    ] {
        let [statement] = <[Statement; 1]>::try_from(parse(sql)?)
            .map_err(|_| std::io::Error::other("expected one statement"))?;
        assert_eq!(render_statement(statement, &limits)?, sql);
    }

    // Quoted names have no escaped form; one that would end early is refused.
    for sql in [
        "select id as 'a\\''' from public_t",
        "select id as \"a\\\"\"\" from public_t",
        "select \"a\\\"\"\" from public_t",
        "select id from public_t where name = X'0a''0b'",
    ] {
        let [statement] = <[Statement; 1]>::try_from(parse(sql)?)
            .map_err(|_| std::io::Error::other("expected one statement"))?;
        let result = render_statement(statement, &limits);
        assert!(
            matches!(result, Err(QueryError::Unsupported(_))),
            "{sql}: {result:?}"
        );
    }
    Ok(())
}

/// Returns `v` from a `WHERE name = v AND id = 1` filter, or `None` when the
/// filter has any other shape.
fn name_filter_literal(statements: &[Statement]) -> Option<String> {
    let [Statement::Query(query)] = statements else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let Some(SqlExpr::BinaryOp {
        left,
        op: BinaryOperator::And,
        right,
    }) = &select.selection
    else {
        return None;
    };
    let SqlExpr::BinaryOp {
        left: name,
        op: BinaryOperator::Eq,
        right: literal,
    } = left.as_ref()
    else {
        return None;
    };
    let id_filter = matches!(
        right.as_ref(),
        SqlExpr::BinaryOp { left, op: BinaryOperator::Eq, right }
            if matches!(left.as_ref(), SqlExpr::Identifier(ident) if ident.value == "id")
                && right.to_string() == "1"
    );
    match (name.as_ref(), literal.as_ref()) {
        (SqlExpr::Identifier(ident), SqlExpr::Value(value))
            if ident.value == "name" && id_filter =>
        {
            value.value.clone().into_string()
        }
        _ => None,
    }
}
