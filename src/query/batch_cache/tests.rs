use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use crate::{
    db::{Database, DbConfig, Profile, create_database},
    model::{FieldPath, Value, encode_value},
    query::{
        ColumnDef, ColumnType, QueryError, REL_ROWS_TABLE, Row, SqlEngine, SqlOutput, SqlReadGuard,
        SqlReadTarget, SqlRelationPolicy, TableSchema, make_row_key,
    },
    repl::{ConditionalBatch, Op, ReadConsistency, ReplError, Replication, WriteCondition},
    security::{AuthzError, Permission, Principal, Resource},
    storage::Bytes,
    txn::WriteAuthorization,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct Probe {
    database: Arc<Database>,
    row_scans: AtomicUsize,
    versioned: AtomicBool,
    insert_after_scan: AtomicBool,
}

impl Probe {
    fn scans(&self) -> usize {
        self.row_scans.load(Ordering::Relaxed)
    }

    fn after_scan(&self, table: &str) -> Result<(), ReplError> {
        if table == REL_ROWS_TABLE && self.insert_after_scan.swap(false, Ordering::SeqCst) {
            self.database
                .table("items")
                .and_then(|table| table.insert(row(4, "bob", 40)).map_err(Into::into))
                .map_err(|error| ReplError::Transport(error.to_string()))?;
        }
        Ok(())
    }
}

impl Replication for Probe {
    fn table_data_version(&self, table: &str) -> Result<Option<u64>, ReplError> {
        if !self.versioned.load(Ordering::SeqCst) {
            return Ok(None);
        }
        self.database.table_data_version(table)
    }
    fn supports_bounded_scans(&self) -> bool {
        self.database.supports_bounded_scans()
    }
    fn supports_commit_log(&self) -> bool {
        self.database.supports_commit_log()
    }
    fn propose(&self, operation: Op) -> Result<(), ReplError> {
        self.database.propose(operation)
    }
    fn propose_batch(&self, operations: Vec<Op>) -> Result<(), ReplError> {
        self.database.propose_batch(operations)
    }
    fn propose_authorized_batch(
        &self,
        operations: Vec<Op>,
        authorization: WriteAuthorization,
    ) -> Result<(), ReplError> {
        self.database
            .propose_authorized_batch(operations, authorization)
    }
    fn propose_conditional_batch(&self, batch: ConditionalBatch) -> Result<(), ReplError> {
        self.database.propose_conditional_batch(batch)
    }
    fn read(
        &self,
        table: &str,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Option<Bytes>, ReplError> {
        self.database.read(table, key, consistency)
    }
    fn range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        if table == REL_ROWS_TABLE {
            self.row_scans.fetch_add(1, Ordering::Relaxed);
        }
        let rows = self.database.range(table, start, end, consistency)?;
        self.after_scan(table)?;
        Ok(rows)
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
        if table == REL_ROWS_TABLE {
            self.row_scans.fetch_add(1, Ordering::Relaxed);
        }
        self.database.scan_range_batches(
            table,
            start,
            end,
            consistency,
            batch_rows,
            cancelled,
            on_batch,
        )?;
        self.after_scan(table)
    }
}

fn row(id: i64, tenant: &str, amount: i64) -> Row {
    vec![
        Value::Int(id),
        Value::Str(tenant.to_owned()),
        Value::Int(amount),
    ]
}

fn fixture() -> Result<(Arc<Probe>, SqlEngine), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    database.create_table(
        "items",
        Some(TableSchema::new(
            vec![
                ColumnDef::new("id", ColumnType::Int, false),
                ColumnDef::new("tenant", ColumnType::Str, false),
                ColumnDef::new("amount", ColumnType::Int, false),
            ],
            0,
        )),
        Vec::new(),
    )?;
    for values in [row(1, "alice", 10), row(2, "bob", 20), row(3, "alice", 30)] {
        database.table("items")?.insert(values)?;
    }
    let probe = Arc::new(Probe {
        database: Arc::new(database),
        row_scans: AtomicUsize::new(0),
        versioned: AtomicBool::new(true),
        insert_after_scan: AtomicBool::new(false),
    });
    let repl: Arc<dyn Replication> = probe.clone();
    let mut engine = SqlEngine::new(repl);
    engine.open_table("items", Vec::new())?;
    Ok((probe, engine))
}

fn extract_totals(output: SqlOutput) -> Result<(i64, i64), Box<dyn std::error::Error>> {
    match output {
        SqlOutput::Rows(rows) => match rows.rows.as_slice() {
            [values] => match values.as_slice() {
                [Value::Int(count), Value::Int(sum)] => Ok((*count, *sum)),
                other => Err(format!("unexpected aggregate values: {other:?}").into()),
            },
            other => Err(format!("unexpected aggregate rows: {other:?}").into()),
        },
        other @ SqlOutput::AffectedRows(_) => {
            Err(format!("unexpected aggregate output: {other:?}").into())
        }
    }
}

const TOTALS: &str = "SELECT COUNT(*) AS n, SUM(amount) AS total FROM items";

async fn totals(engine: &SqlEngine) -> Result<(i64, i64), Box<dyn std::error::Error>> {
    extract_totals(engine.execute(TOTALS).await?)
}

#[tokio::test]
async fn repeated_aggregates_reuse_raw_batches_without_rereading_rows() -> TestResult {
    let (probe, engine) = fixture()?;
    assert_eq!(totals(&engine).await?, (3, 60));
    let scans = probe.scans();
    assert!(scans > 0);
    assert_eq!(totals(&engine).await?, (3, 60));
    assert_eq!(
        probe.scans(),
        scans,
        "a cached aggregate must not reread row storage"
    );
    Ok(())
}

#[tokio::test]
async fn zero_column_count_batches_keep_row_counts_and_separate_projection_keys() -> TestResult {
    let (probe, engine) = fixture()?;
    let count = "SELECT COUNT(*) AS n FROM items";
    let SqlOutput::Rows(first) = engine.execute(count).await? else {
        return Err("COUNT did not return rows".into());
    };
    assert_eq!(first.rows, vec![vec![Value::Int(3)]]);
    let scans = probe.scans();
    assert!(scans > 0);
    let SqlOutput::Rows(second) = engine.execute(count).await? else {
        return Err("COUNT did not return rows".into());
    };
    assert_eq!(second.rows, first.rows);
    assert_eq!(
        probe.scans(),
        scans,
        "zero-column batches should retain their row counts on a cache hit"
    );
    assert_eq!(
        totals(&engine).await?,
        (3, 60),
        "a projected aggregate must not receive the zero-column cache entry"
    );
    let scans = probe.scans();
    let SqlOutput::Rows(third) = engine.execute(count).await? else {
        return Err("COUNT did not return rows".into());
    };
    assert_eq!(third.rows, first.rows);
    assert_eq!(probe.scans(), scans);
    Ok(())
}

#[tokio::test]
async fn historical_snapshot_forks_do_not_read_or_pollute_the_latest_raw_cache() -> TestResult {
    let (probe, engine) = fixture()?;
    let snapshot = crate::cdc::current_resume_token(probe.database.as_ref())?.lsn;
    assert_eq!(totals(&engine).await?, (3, 60));
    probe.database.table("items")?.insert(row(4, "bob", 40))?;
    assert_eq!(totals(&engine).await?, (4, 100));
    let historical = engine.clone().with_snapshot_lsn(snapshot);
    assert_eq!(
        totals(&historical).await?,
        (3, 60),
        "an AS OF scan must bypass a populated latest-generation cache"
    );
    let scans = probe.scans();
    assert_eq!(
        totals(&engine).await?,
        (4, 100),
        "historical rows must not replace current raw batches"
    );
    assert_eq!(
        probe.scans(),
        scans,
        "the historical fork should preserve the latest cache entry"
    );
    Ok(())
}

#[tokio::test]
async fn committed_handle_raw_and_transaction_writes_invalidate_cached_batches() -> TestResult {
    let (probe, engine) = fixture()?;
    assert_eq!(totals(&engine).await?, (3, 60));
    let mut scans = probe.scans();
    probe.database.table("items")?.insert(row(4, "bob", 40))?;
    assert_eq!(totals(&engine).await?, (4, 100));
    assert!(probe.scans() > scans);
    scans = probe.scans();
    probe.database.table("items")?.delete(&Value::Int(2))?;
    assert_eq!(totals(&engine).await?, (3, 80));
    assert!(probe.scans() > scans);
    scans = probe.scans();
    probe.database.propose(Op::Put {
        table: REL_ROWS_TABLE.to_owned(),
        key: make_row_key("items", &Value::Int(1))?,
        value: encode_value(&Value::Array(row(1, "alice", 99)))?,
    })?;
    assert_eq!(totals(&engine).await?, (3, 169));
    assert!(probe.scans() > scans);
    scans = probe.scans();
    probe
        .database
        .transaction(|transaction| transaction.insert_row("items", row(5, "alice", 50)))?;
    assert_eq!(totals(&engine).await?, (4, 219));
    assert!(probe.scans() > scans);
    Ok(())
}

#[tokio::test]
async fn failed_condition_and_durable_audit_do_not_invalidate_unchanged_user_rows() -> TestResult {
    let (probe, engine) = fixture()?;
    assert_eq!(totals(&engine).await?, (3, 60));
    let scans = probe.scans();
    let version = probe.table_data_version(REL_ROWS_TABLE)?;
    let existing = make_row_key("items", &Value::Int(1))?;
    let failed = probe
        .database
        .propose_conditional_batch(ConditionalBatch::new(
            vec![WriteCondition::KeyMissing {
                table: REL_ROWS_TABLE.to_owned(),
                key: existing,
            }],
            vec![Op::Put {
                table: REL_ROWS_TABLE.to_owned(),
                key: make_row_key("items", &Value::Int(9))?,
                value: encode_value(&Value::Array(row(9, "bob", 90)))?,
            }],
        ));
    assert!(matches!(failed, Err(ReplError::Conflict)));
    assert_eq!(probe.table_data_version(REL_ROWS_TABLE)?, version);
    assert_eq!(totals(&engine).await?, (3, 60));
    assert_eq!(probe.scans(), scans);
    let audits = probe.database.audit_events()?.len();
    assert!(
        probe
            .database
            .query_as(&Principal::new("ungranted"), "SELECT amount FROM items")
            .await
            .is_err()
    );
    assert_eq!(
        probe.database.audit_events()?.len(),
        audits + 1,
        "the denied request must still be durably audited"
    );
    assert_eq!(probe.table_data_version(REL_ROWS_TABLE)?, version);
    assert_eq!(totals(&engine).await?, (3, 60));
    assert_eq!(
        probe.scans(),
        scans,
        "audit commits must not evict unrelated raw data"
    );
    Ok(())
}

struct PolicyGuard {
    policy: SqlRelationPolicy,
    deny: bool,
}

impl SqlReadGuard for PolicyGuard {
    fn authorize_reads(&mut self, _reads: &BTreeSet<SqlReadTarget>) -> Result<(), AuthzError> {
        if self.deny {
            return Err(AuthzError::Denied {
                principal: "denied".to_owned(),
                resource: Resource::Table("items".to_owned()),
                permission: Permission::Read,
            });
        }
        Ok(())
    }
    fn relation_policies(&self) -> BTreeMap<String, SqlRelationPolicy> {
        BTreeMap::from([("items".to_owned(), self.policy.clone())])
    }
}

fn guard(tenant: &str, masked_amount: i64) -> PolicyGuard {
    PolicyGuard {
        policy: SqlRelationPolicy {
            visible_when: vec![vec![(
                FieldPath::new(["tenant"]),
                Value::Str(tenant.to_owned()),
            )]],
            masks: vec![(FieldPath::new(["amount"]), Value::Int(masked_amount))],
        },
        deny: false,
    }
}

#[tokio::test]
async fn warm_raw_cache_preserves_fresh_authorization_row_policies_and_masks() -> TestResult {
    let (probe, engine) = fixture()?;
    assert_eq!(totals(&engine).await?, (3, 60));
    let mut alice = guard("alice", 1);
    assert_eq!(
        extract_totals(
            engine
                .execute_with_read_guard(TOTALS, Some(&mut alice))
                .await?
        )?,
        (2, 2)
    );
    let scans = probe.scans();
    let mut bob = guard("bob", 5);
    assert_eq!(
        extract_totals(
            engine
                .execute_with_read_guard(TOTALS, Some(&mut bob))
                .await?
        )?,
        (1, 5)
    );
    assert_eq!(
        probe.scans(),
        scans,
        "fresh policy wrappers should reuse only the raw source batches"
    );
    assert_eq!(
        totals(&engine).await?,
        (3, 60),
        "masked batches must never overwrite the shared raw cache"
    );
    let mut denied = PolicyGuard {
        policy: SqlRelationPolicy::default(),
        deny: true,
    };
    assert!(matches!(
        engine
            .execute_with_read_guard(TOTALS, Some(&mut denied))
            .await,
        Err(QueryError::AuthzDenied(_))
    ));
    assert_eq!(probe.scans(), scans);
    Ok(())
}

#[tokio::test]
async fn backends_without_a_version_token_never_reuse_cached_rows() -> TestResult {
    let (probe, engine) = fixture()?;
    assert_eq!(totals(&engine).await?, (3, 60));
    probe.versioned.store(false, Ordering::SeqCst);
    probe.database.table("items")?.insert(row(4, "bob", 40))?;
    let scans = probe.scans();
    assert_eq!(totals(&engine).await?, (4, 100));
    assert!(probe.scans() > scans);
    let scans = probe.scans();
    assert_eq!(totals(&engine).await?, (4, 100));
    assert!(probe.scans() > scans);
    Ok(())
}

#[tokio::test]
async fn a_write_during_cache_fill_prevents_publishing_stale_batches() -> TestResult {
    let (probe, engine) = fixture()?;
    probe.insert_after_scan.store(true, Ordering::SeqCst);
    assert_eq!(
        totals(&engine).await?,
        (3, 60),
        "the in-flight read may retain its storage snapshot"
    );
    let scans = probe.scans();
    assert_eq!(
        totals(&engine).await?,
        (4, 100),
        "the next read must observe the committed insertion"
    );
    assert!(probe.scans() > scans, "a raced fill must not be reused");
    let scans = probe.scans();
    assert_eq!(totals(&engine).await?, (4, 100));
    assert_eq!(probe.scans(), scans);
    Ok(())
}
