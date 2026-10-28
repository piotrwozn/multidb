use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use super::*;
use crate::{
    db::{DbConfig, Profile, create_database},
    extension::{MaskingPolicy, PolicyConfig, RowPolicy},
    model::FieldPath,
    query::{ColumnDef, RelIndexSpec, SqlReadGuard, SqlReadTarget},
    repl::{ConditionalBatch, Op, ReplError, Replication},
    security::{AuthzError, AuthzPolicy, Permission, Principal, Resource, Role},
    storage::Bytes,
    temporal::TemporalPoint,
    txn::WriteAuthorization,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Default)]
struct Counts {
    rows: AtomicUsize,
    gets: AtomicUsize,
    indexes: AtomicUsize,
    delay_ms: AtomicUsize,
}
impl Counts {
    fn reset(&self) {
        self.rows.store(0, Ordering::SeqCst);
        self.gets.store(0, Ordering::SeqCst);
        self.indexes.store(0, Ordering::SeqCst);
    }
}
struct Counted {
    inner: Arc<dyn Replication>,
    counts: Arc<Counts>,
}
impl Replication for Counted {
    fn supports_bounded_scans(&self) -> bool {
        self.inner.supports_bounded_scans()
    }
    fn propose(&self, op: Op) -> Result<(), ReplError> {
        self.inner.propose(op)
    }
    fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
        self.inner.propose_batch(ops)
    }
    fn propose_authorized_batch(
        &self,
        ops: Vec<Op>,
        auth: WriteAuthorization,
    ) -> Result<(), ReplError> {
        self.inner.propose_authorized_batch(ops, auth)
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
            self.counts.gets.fetch_add(1, Ordering::SeqCst);
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
        let result = self.inner.range(table, start, end, consistency)?;
        if table == REL_ROWS_TABLE {
            self.counts.rows.fetch_add(result.len(), Ordering::SeqCst);
        }
        if table == super::super::REL_INDEX_TABLE {
            self.counts
                .indexes
                .fetch_add(result.len(), Ordering::SeqCst);
        }
        Ok(result)
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
            &mut |batch| {
                if table == REL_ROWS_TABLE {
                    self.counts.rows.fetch_add(batch.len(), Ordering::SeqCst);
                    let delay = self.counts.delay_ms.load(Ordering::SeqCst);
                    if delay > 0 {
                        std::thread::sleep(std::time::Duration::from_millis(
                            u64::try_from(delay).unwrap_or(u64::MAX),
                        ));
                    }
                }
                on_batch(batch)
            },
        )
    }
}

fn schema() -> TableSchema {
    TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("category", ColumnType::Int, false),
            ColumnDef::new("value", ColumnType::Int, false),
            ColumnDef::new("payload", ColumnType::Str, false),
        ],
        0,
    )
}

fn fixture() -> Result<(SqlEngine, Arc<Counts>), Box<dyn std::error::Error>> {
    let counts = Arc::new(Counts::default());
    let repl: Arc<dyn Replication> = Arc::new(Counted {
        inner: Arc::new(create_database(DbConfig::new(Profile::InMemory))?),
        counts: Arc::clone(&counts),
    });
    let mut engine = SqlEngine::new(Arc::clone(&repl));
    engine.create_table(
        "bench",
        Some(schema()),
        vec![RelIndexSpec::new(1, 0), RelIndexSpec::new(2, 1)],
    )?;
    let table = engine.tables.get("bench").ok_or("missing table")?;
    let mut ops = Vec::new();
    for id in -1000..2000 {
        ops.extend(table.insert_ops(vec![
            Value::Int(id),
            Value::Int(id.rem_euclid(100)),
            Value::Int(id),
            Value::Str("x".repeat(256)),
        ])?);
    }
    for id in [i64::MIN, i64::MAX] {
        ops.extend(table.insert_ops(vec![
            Value::Int(id),
            Value::Int(0),
            Value::Int(id),
            Value::Str("edge".to_owned()),
        ])?);
    }
    repl.propose_batch(ops)?;
    counts.reset();
    Ok((engine, counts))
}

async fn ids(engine: &SqlEngine, sql: &str) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let SqlOutput::Rows(rows) = engine.execute(sql).await? else {
        return Err("expected rows".into());
    };
    rows.rows
        .into_iter()
        .map(|row| match row.first() {
            Some(Value::Int(value)) => Ok(*value),
            _ => Err("expected integer id".into()),
        })
        .collect()
}

#[tokio::test]
async fn hundred_id_range_reads_only_its_keys_and_point_lookup_skips_secondary_index() -> TestResult
{
    let (engine, counts) = fixture()?;
    assert_eq!(
        ids(
            &engine,
            "SELECT id FROM bench WHERE id>=42 AND id<142 ORDER BY id"
        )
        .await?,
        (42..142).collect::<Vec<_>>()
    );
    assert_eq!(counts.rows.load(Ordering::SeqCst), 100);
    assert_eq!(counts.gets.load(Ordering::SeqCst), 0);
    counts.reset();
    assert_eq!(
        ids(&engine, "SELECT id FROM bench WHERE id=42").await?,
        vec![42]
    );
    assert_eq!(counts.gets.load(Ordering::SeqCst), 1);
    assert_eq!(counts.rows.load(Ordering::SeqCst), 0);
    assert_eq!(counts.indexes.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn ranges_preserve_order_offset_limits_empty_intervals_and_integer_edges() -> TestResult {
    let (engine, counts) = fixture()?;
    for (sql, expected, read) in [
        (
            "SELECT id FROM bench WHERE id>=0 AND id<100 ORDER BY id LIMIT 3 OFFSET 5",
            vec![5, 6, 7],
            8,
        ),
        (
            "SELECT id FROM bench WHERE id>=0 AND id<100 ORDER BY id DESC LIMIT 3 OFFSET 5",
            vec![94, 93, 92],
            100,
        ),
        (
            "SELECT id FROM bench WHERE -4 <= id AND 0 > id ORDER BY id",
            vec![-4, -3, -2, -1],
            4,
        ),
        ("SELECT id FROM bench WHERE id>=5 AND id<5", Vec::new(), 0),
        (
            "SELECT id FROM bench WHERE id>9223372036854775807",
            Vec::new(),
            0,
        ),
        (
            "SELECT id FROM bench WHERE id < -9223372036854775808",
            Vec::new(),
            0,
        ),
        (
            "SELECT id FROM bench WHERE id>=0 ORDER BY id DESC LIMIT 0 OFFSET 8",
            Vec::new(),
            0,
        ),
    ] {
        counts.reset();
        assert_eq!(ids(&engine, sql).await?, expected, "{sql}");
        assert_eq!(counts.rows.load(Ordering::SeqCst), read, "{sql}");
    }
    for value in [i64::MIN, i64::MAX] {
        counts.reset();
        assert_eq!(
            ids(&engine, &format!("SELECT id FROM bench WHERE id={value}")).await?,
            vec![value]
        );
        assert_eq!(counts.gets.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test]
async fn datafusion_residuals_and_projected_batches_keep_range_reads_bounded() -> TestResult {
    let (engine, counts) = fixture()?;
    // Expression projection forces DataFusion; the provider still bounds the physical read.
    assert_eq!(
        ids(
            &engine,
            "SELECT id+0 FROM bench WHERE id>=42 AND id<142 ORDER BY id"
        )
        .await?,
        (42..142).collect::<Vec<_>>()
    );
    assert_eq!(counts.rows.load(Ordering::SeqCst), 100);
    assert_eq!(
        ids(
            &engine,
            "SELECT id FROM bench WHERE id>=0 AND id<100 AND id%2=0 ORDER BY id LIMIT 3 OFFSET 5"
        )
        .await?,
        vec![10, 12, 14]
    );
    let table = engine.tables.get("bench").ok_or("missing table")?;
    let projection = [1_usize, 2];
    let result = batches(
        table,
        &super::super::schema_to_arrow(Some(&schema())),
        Some(&projection),
        &[],
        None,
        127,
    )?;
    assert!(result.len() > 1);
    assert!(
        result
            .iter()
            .all(|batch| batch.num_columns() == 2 && batch.num_rows() <= 127)
    );
    assert_eq!(
        result.iter().map(RecordBatch::num_rows).sum::<usize>(),
        3002
    );
    let SqlOutput::Rows(aggregate) = engine.execute("SELECT category,COUNT(*),SUM(value) FROM bench WHERE id>=0 AND id<2000 GROUP BY category ORDER BY category").await? else { return Err("missing aggregate".into()) };
    assert_eq!(aggregate.rows.len(), 100);
    for (category, row) in aggregate.rows.iter().enumerate() {
        let category = i64::try_from(category)?;
        assert_eq!(
            row,
            &vec![
                Value::Int(category),
                Value::Int(20),
                Value::Int(19000 + category * 20)
            ]
        );
    }
    Ok(())
}

struct Guard {
    policies: BTreeMap<String, super::super::SqlRelationPolicy>,
    allowed: bool,
}
impl SqlReadGuard for Guard {
    fn authorize_reads(
        &mut self,
        _: &std::collections::BTreeSet<SqlReadTarget>,
    ) -> Result<(), AuthzError> {
        if self.allowed {
            Ok(())
        } else {
            Err(AuthzError::Denied {
                principal: "no".to_owned(),
                resource: Resource::Table("bench".to_owned()),
                permission: Permission::Read,
            })
        }
    }
    fn relation_policies(&self) -> BTreeMap<String, super::super::SqlRelationPolicy> {
        self.policies.clone()
    }
}

#[tokio::test]
async fn fast_ranges_are_authorized_and_policy_masks_remain_before_user_predicates() -> TestResult {
    let (engine, _) = fixture()?;
    let sql = "SELECT id FROM bench WHERE id>=42 AND id<142 ORDER BY id";
    let mut denied = Guard {
        policies: BTreeMap::new(),
        allowed: false,
    };
    assert!(matches!(
        engine.execute_with_read_guard(sql, Some(&mut denied)).await,
        Err(QueryError::AuthzDenied(_))
    ));
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let table = database.create_table(
        "people",
        Some(TableSchema::new(
            vec![
                ColumnDef::new("id", ColumnType::Int, false),
                ColumnDef::new("owner", ColumnType::Str, false),
            ],
            0,
        )),
        Vec::new(),
    )?;
    table.insert(vec![Value::Int(10), Value::Str("alice".to_owned())])?;
    table.insert(vec![Value::Int(20), Value::Str("bob".to_owned())])?;
    database.set_authz_policy(AuthzPolicy::new([
        Role::new("reader").grant(Resource::Table("people".to_owned()), Permission::Read)
    ]));
    database.set_policy_config(&PolicyConfig {
        row_policies: vec![RowPolicy {
            resource: Resource::Table("people".to_owned()),
            path: FieldPath::new(["owner"]),
            equals: Value::Str("public".to_owned()),
        }],
        masking: vec![MaskingPolicy {
            resource: Resource::Table("people".to_owned()),
            paths: vec![FieldPath::new(["id"])],
            replacement: Value::Int(0),
        }],
        ..PolicyConfig::default()
    })?;
    let reader = Principal::new("alice").with_role("reader");
    let SqlOutput::Rows(rows) = database
        .query_as(
            &reader,
            "SELECT id FROM people WHERE id>=1 AND id<100 ORDER BY id",
        )
        .await?
    else {
        return Err("expected rows".into());
    };
    assert_eq!(rows.rows, Vec::<Row>::new());
    let SqlOutput::Rows(rows) = database
        .query_as(&reader, "SELECT id FROM people WHERE id=0")
        .await?
    else {
        return Err("expected rows".into());
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(0)]]);
    Ok(())
}

#[tokio::test]
async fn unprojected_string_key_tables_and_snapshot_queries_keep_their_semantics() -> TestResult {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let table = database.create_table(
        "strings",
        Some(TableSchema::new(
            vec![ColumnDef::new("id", ColumnType::Str, false)],
            0,
        )),
        Vec::new(),
    )?;
    table.insert(vec![Value::Str("a".to_owned())])?;
    table.insert(vec![Value::Str("b".to_owned())])?;
    let SqlOutput::Rows(rows) = database.query("SELECT COUNT(*) FROM strings").await? else {
        return Err("expected count".into());
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(2)]]);
    let table = database.create_table(
        "history",
        Some(TableSchema::new(
            vec![
                ColumnDef::new("id", ColumnType::Int, false),
                ColumnDef::new("value", ColumnType::Int, false),
            ],
            0,
        )),
        Vec::new(),
    )?;
    table.insert(vec![Value::Int(1), Value::Int(10)])?;
    let snapshot = crate::cdc::current_resume_token(&database)?.lsn;
    table.update(vec![Value::Int(1), Value::Int(99)])?;
    let SqlOutput::Rows(rows) = database
        .query_as_of(
            "SELECT value FROM history WHERE id>=1 AND id<2 ORDER BY id",
            TemporalPoint::Lsn(snapshot),
        )
        .await?
    else {
        return Err("expected snapshot".into());
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(10)]]);
    Ok(())
}

#[tokio::test]
async fn range_fast_path_cancels_slow_storage_and_limits_retained_projected_output() -> TestResult {
    let (mut engine, counts) = fixture()?;
    engine.performance.query.timeout_ms = 1;
    engine.performance.query.batch_rows = 1;
    counts.delay_ms.store(5, Ordering::SeqCst);
    assert!(matches!(
        engine
            .execute("SELECT id FROM bench WHERE id>=0 AND id<100 ORDER BY id")
            .await,
        Err(QueryError::Timeout(_))
    ));
    assert!(
        counts.rows.load(Ordering::SeqCst) <= 1,
        "expired scan continued reading rows"
    );
    counts.delay_ms.store(0, Ordering::SeqCst);
    counts.reset();
    engine.performance.query.timeout_ms = 30_000;
    engine.performance.query.memory_limit_bytes = 40_000;
    assert_eq!(
        ids(
            &engine,
            "SELECT id FROM bench WHERE id>=0 AND id<100 ORDER BY id"
        )
        .await?,
        (0..100).collect::<Vec<_>>()
    );
    counts.reset();
    assert!(matches!(
        engine
            .execute("SELECT * FROM bench WHERE id>=0 AND id<100 ORDER BY id")
            .await,
        Err(QueryError::ResourceLimit(_))
    ));
    assert!(
        counts.rows.load(Ordering::SeqCst) < 100,
        "over-budget output was fully materialized"
    );
    counts.reset();
    // Descending order reads its bounded interval before reversing; retained rows are
    // still charged, including when LIMIT would make the final output small.
    assert!(matches!(
        engine
            .execute("SELECT * FROM bench WHERE id>=0 AND id<100 ORDER BY id DESC LIMIT 1")
            .await,
        Err(QueryError::ResourceLimit(_))
    ));
    assert!(counts.rows.load(Ordering::SeqCst) < 100);
    Ok(())
}
