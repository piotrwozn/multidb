use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::*;
use crate::{
    db::{DbConfig, Profile, create_database},
    model::{FieldPath, Value},
    query::{ColumnDef, ColumnType, REL_ROWS_TABLE, RelIndexSpec, SqlReadGuard, TableSchema},
    repl::{ConditionalBatch, Op, ReadConsistency, ReplError, Replication},
    security::{AuthzError, Permission, Resource},
    storage::Bytes,
    txn::WriteAuthorization,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct CountReads {
    inner: Arc<dyn Replication>,
    reads: Arc<AtomicUsize>,
}

impl Replication for CountReads {
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
            self.reads.fetch_add(1, Ordering::SeqCst);
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
        if table == REL_ROWS_TABLE {
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.range(table, start, end, consistency)
    }
}

#[derive(Default)]
struct Guard {
    allow: bool,
    policies: BTreeMap<String, SqlRelationPolicy>,
    reads: BTreeSet<SqlReadTarget>,
}

impl SqlReadGuard for Guard {
    fn authorize_reads(&mut self, reads: &BTreeSet<SqlReadTarget>) -> Result<(), AuthzError> {
        self.reads.extend(reads.iter().cloned());
        if !self.allow {
            return Err(AuthzError::Denied {
                principal: "tester".to_owned(),
                resource: Resource::Table("items".to_owned()),
                permission: Permission::Read,
            });
        }
        Ok(())
    }
    fn relation_policies(&self) -> BTreeMap<String, SqlRelationPolicy> {
        self.policies.clone()
    }
}

async fn setup() -> Result<(SqlEngine, Arc<AtomicUsize>), Box<dyn std::error::Error>> {
    let reads = Arc::new(AtomicUsize::new(0));
    let inner: Arc<dyn Replication> = Arc::new(create_database(DbConfig::new(Profile::InMemory))?);
    let repl = Arc::new(CountReads {
        inner,
        reads: reads.clone(),
    });
    let mut engine = SqlEngine::new(repl);
    engine.create_table(
        "items",
        Some(TableSchema::new(
            vec![
                ColumnDef::new("id", ColumnType::Int, false),
                ColumnDef::new("category", ColumnType::Int, false),
            ],
            0,
        )),
        vec![RelIndexSpec::new(1, 1)],
    )?;
    engine
        .execute("insert into items (id, category) values (1, 3), (2, 3), (3, 4)")
        .await?;
    reads.store(0, Ordering::SeqCst);
    Ok((engine, reads))
}

#[tokio::test]
async fn plain_range_and_aggregate_explain_are_metadata_only() -> TestResult {
    let (engine, reads) = setup().await?;
    let range = engine.explain(
        "select id from items where id >= 1 and id < 3 order by id",
        ExplainOptions::default(),
    )?;
    assert!(matches!(
        range.nodes[0].access_path,
        AccessPath::PrimaryKeyRange { .. }
    ));
    assert_eq!(range.nodes[0].estimated_rows, 2);
    assert_eq!(range.nodes[0].actual_rows, None);
    assert!(range.nodes[0].details.contains("interval [1, 2]"));
    let output = engine
        .execute("explain select id from items where id >= 1 and id < 3 order by id")
        .await?;
    let SqlOutput::Rows(rows) = output else {
        return Err("missing range EXPLAIN output".into());
    };
    assert_eq!(rows.rows[0][2], Value::Int(2));
    assert_eq!(rows.rows[0][4], Value::Null);

    // The synchronous API also works from a current-thread Tokio runtime.
    let aggregate = engine.explain(
        "select category, count(*) from items group by category",
        ExplainOptions::default(),
    )?;
    assert_eq!(aggregate.nodes[0].access_path, AccessPath::DataFusionPlan);
    assert!(aggregate.nodes[0].details.contains("Aggregate"));
    assert_eq!(aggregate.nodes[0].actual_rows, None);

    let output = engine
        .execute("explain select category, count(*) from items group by category")
        .await?;
    let SqlOutput::Rows(rows) = output else {
        return Err("missing EXPLAIN output".into());
    };
    assert_eq!(rows.columns.len(), 7);
    assert_eq!(
        rows.rows[0][1],
        Value::Str("datafusion_logical_plan".to_owned())
    );
    assert_eq!(rows.rows[0][2], Value::Null);
    assert_eq!(rows.rows[0][3], Value::Null);
    assert_eq!(rows.rows[0][4], Value::Null);
    assert_eq!(reads.load(Ordering::SeqCst), 0);

    let mut guard = Guard {
        allow: true,
        ..Guard::default()
    };
    let schema = engine
        .describe_with_read_guard("explain select count(*) from items", &mut guard)
        .await?
        .ok_or("missing EXPLAIN schema")?;
    assert_eq!(schema.fields().len(), 7);
    assert!(schema.field(2).is_nullable());
    assert!(schema.field(4).is_nullable());
    assert!(
        guard
            .reads
            .contains(&SqlReadTarget::Catalog("items".to_owned()))
    );
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn explain_analyze_authorizes_before_any_data_read() -> TestResult {
    let (engine, reads) = setup().await?;
    for query in [
        "select id from items where id >= 1 order by id limit 2",
        "select category, count(*) from items group by category",
    ] {
        for prefix in ["explain", "explain analyze"] {
            let mut guard = Guard::default();
            let result = engine
                .execute_with_read_guard(&format!("{prefix} {query}"), Some(&mut guard))
                .await;
            assert!(
                matches!(result, Err(QueryError::AuthzDenied(_))),
                "{result:?}"
            );
            assert!(
                guard
                    .reads
                    .contains(&SqlReadTarget::Catalog("items".to_owned()))
            );
            assert_eq!(reads.load(Ordering::SeqCst), 0);
        }
        let mut guard = Guard {
            allow: true,
            ..Guard::default()
        };
        let output = engine
            .execute_with_read_guard(&format!("explain analyze {query}"), Some(&mut guard))
            .await?;
        let SqlOutput::Rows(rows) = output else {
            return Err("missing EXPLAIN ANALYZE output".into());
        };
        assert_eq!(rows.rows[0][3], Value::Int(2));
        assert!(reads.load(Ordering::SeqCst) > 0);
        reads.store(0, Ordering::SeqCst);
    }
    Ok(())
}

#[tokio::test]
async fn simple_explain_analyze_counts_rows_after_limit() -> TestResult {
    let (engine, _) = setup().await?;
    for (limit, expected_rows) in [(0, 0), (1, 1), (99, 2)] {
        let sql = format!("select id from items where category = 3 limit {limit}");
        let SqlOutput::Rows(actual) = engine.execute(&sql).await? else {
            return Err("missing SELECT output".into());
        };
        assert_eq!(actual.rows.len(), expected_rows);
        let report = engine.explain(&sql, ExplainOptions { analyze: true })?;
        assert_eq!(
            report.nodes[0].actual_rows,
            Some(super::super::usize_to_u64(expected_rows))
        );
        let SqlOutput::Rows(explained) = engine.execute(&format!("explain analyze {sql}")).await?
        else {
            return Err("missing EXPLAIN ANALYZE output".into());
        };
        assert_eq!(
            explained.rows[0][3],
            Value::Int(i64::try_from(expected_rows)?)
        );
    }
    Ok(())
}

#[tokio::test]
async fn explain_policies_fail_closed_for_ranges_aggregates_and_subqueries() -> TestResult {
    let (engine, reads) = setup().await?;
    let policy = SqlRelationPolicy {
        visible_when: vec![vec![(FieldPath::new(["category"]), Value::Int(3))]],
        masks: Vec::new(),
    };
    for query in [
        "select id from items where id > 0 order by id",
        "select category, count(*) from items group by category",
        "select x.id from items x where x.id in (select id from items)",
    ] {
        let mut guard = Guard {
            allow: true,
            policies: BTreeMap::from([("items".to_owned(), policy.clone())]),
            ..Guard::default()
        };
        let result = engine
            .execute_with_read_guard(&format!("explain analyze {query}"), Some(&mut guard))
            .await;
        assert!(
            matches!(result, Err(QueryError::Unsupported(message)) if message.contains("policies"))
        );
        assert_eq!(reads.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn synchronous_fallback_works_inside_multithread_runtime() -> TestResult {
    let (engine, reads) = setup().await?;
    let report = engine.explain(
        "select count(*) from items",
        ExplainOptions { analyze: true },
    )?;
    assert_eq!(report.nodes[0].actual_rows, Some(1));
    assert!(reads.load(Ordering::SeqCst) > 0);
    Ok(())
}
