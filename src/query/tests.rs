use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use super::{
    AccessPath, AnalyzeMode, AnalyzeTarget, CardinalityEstimator, ColumnDef, ColumnType,
    ExplainOptions, MostCommonValue, QueryError, QueryFingerprint, REL_COLUMNAR_SEGMENTS_TABLE,
    REL_INDEX_TABLE, REL_ROWS_TABLE, RelIndexExpression, RelIndexSpec, RelPlanKind, RelPredicate,
    RelTable, SqlEngine, SqlOutput, StatsCatalog, TableLayout, TableSchema, Value, make_row_key,
    parse,
};
use crate::{
    db::{DbConfig, Profile, create_database, open_database},
    model::{CollectionId, FieldPath},
    repl::{ConditionalBatch, Op, ReadConsistency, ReplError, Replication},
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
        self.interleave(table)?;
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
        self.interleave(table)?;
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

    // Another writer inserts the key after the INSERT read it as missing: a
    // plain INSERT reports the duplicate key instead of retrying.
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
async fn expression_covering_index_supports_index_only_scan()
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
    let explain = engine.explain(sql, ExplainOptions::default())?;
    assert!(matches!(
        explain.nodes[0].access_path,
        AccessPath::IndexOnly { index_id: 1, .. }
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
