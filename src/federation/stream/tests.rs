use super::*;
use crate::{
    cloud::ObjectStoreUri,
    federation::{ForeignFilter, ForeignFilterOp},
    query::ColumnDef,
};
use std::sync::atomic::{AtomicBool, AtomicUsize};
type TestResult = Result<(), Box<dyn std::error::Error>>;
struct Counters {
    used: AtomicUsize,
    peak: AtomicUsize,
}
struct TestGuard {
    counters: Arc<Counters>,
    limit: usize,
    cancelled: AtomicBool,
    checks: AtomicUsize,
    cancel_after: usize,
}
struct Token {
    counters: Arc<Counters>,
    bytes: usize,
}
impl Drop for Token {
    fn drop(&mut self) {
        self.counters.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
impl TestGuard {
    fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            counters: Arc::new(Counters {
                used: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            }),
            limit,
            cancelled: AtomicBool::new(false),
            checks: AtomicUsize::new(0),
            cancel_after: usize::MAX,
        })
    }
    fn allocate(&self, bytes: usize) -> Result<(), QueryError> {
        self.check()?;
        // AtomicUsize::try_update is newer than the Rust 1.89 MSRV.
        #[allow(deprecated)]
        let used = self
            .counters
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|n| *n <= self.limit)
            })
            .map_err(|_| QueryError::ResourceLimit("test foreign memory budget".to_owned()))?
            + bytes;
        self.counters.peak.fetch_max(used, Ordering::AcqRel);
        Ok(())
    }
}
impl ForeignScanGuard for TestGuard {
    fn check(&self) -> Result<(), QueryError> {
        if self.cancelled.load(Ordering::Acquire)
            || self.checks.fetch_add(1, Ordering::Relaxed) >= self.cancel_after
        {
            Err(QueryError::Cancelled("test cancellation".to_owned()))
        } else {
            Ok(())
        }
    }
    fn remaining_bytes(&self) -> usize {
        self.limit
            .saturating_sub(self.counters.used.load(Ordering::Acquire))
    }
    fn charge(&self, bytes: usize) -> Result<(), QueryError> {
        self.allocate(bytes)
    }
    fn reserve(&self, bytes: usize) -> Result<Box<dyn ForeignScanReservation>, QueryError> {
        self.allocate(bytes)?;
        Ok(Box::new(Token {
            counters: Arc::clone(&self.counters),
            bytes,
        }))
    }
}
fn schema() -> TableSchema {
    TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("payload", ColumnType::Str, false),
        ],
        0,
    )
}
fn csv(root: &std::path::Path) -> ForeignSource {
    ForeignSource::Csv {
        uri: ObjectStoreUri::from_local_dir(root),
        path: "rows.csv".to_owned(),
        has_header: true,
    }
}

#[test]
fn local_limit_zero_validates_columns_without_opening_missing_file() -> TestResult {
    let temp = tempfile::tempdir()?;
    let source = csv(temp.path());
    let result = scan_local(
        &schema(),
        &source,
        &ForeignScanRequest::new(Some(vec![1, 0, 1]), vec![], Some(0)),
        1,
        &None,
    )?;
    assert_eq!(result.rows, Vec::<Row>::new());
    assert_eq!(result.stats.scanned_bytes, 0);
    assert!(
        scan_local(
            &schema(),
            &source,
            &ForeignScanRequest::new(Some(vec![2]), vec![], Some(0)),
            1,
            &None
        )
        .is_err()
    );
    Ok(())
}
#[test]
fn csv_stops_decoding_at_filtered_limit_and_projects_once() -> TestResult {
    let temp = tempfile::tempdir()?;
    std::fs::write(
        temp.path().join("rows.csv"),
        "id,payload\n1,first\n2,second\nnot-an-id,invalid-tail\n",
    )?;
    let request = ForeignScanRequest::new(
        Some(vec![1, 0, 1]),
        vec![ForeignFilter {
            column: 0,
            op: ForeignFilterOp::Ge,
            value: Value::Int(2),
        }],
        Some(1),
    );
    let result = scan_local(
        &schema(),
        &csv(temp.path()),
        &request,
        1,
        &Some(TestGuard::new(128 * 1024)),
    )?;
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Str("second".to_owned()),
            Value::Int(2),
            Value::Str("second".to_owned())
        ]]
    );
    assert_eq!(result.stats.scanned_rows, 2);
    Ok(())
}
#[test]
fn projected_large_string_streams_without_accumulating_unselected_payloads() -> TestResult {
    use std::fmt::Write as _;
    let temp = tempfile::tempdir()?;
    let payload = "x".repeat(256 * 1024);
    let mut data = String::from("id,payload\n");
    for i in 0..12 {
        writeln!(data, "{i},{payload}")?;
    }
    std::fs::write(temp.path().join("rows.csv"), data)?;
    let guard = TestGuard::new(4 * 1024 * 1024);
    let result = scan_local(
        &schema(),
        &csv(temp.path()),
        &ForeignScanRequest::new(Some(vec![0]), vec![], None),
        256,
        &Some(guard.clone()),
    )?;
    assert_eq!(
        result.rows,
        (0..12).map(|i| vec![Value::Int(i)]).collect::<Vec<_>>()
    );
    assert!(guard.counters.peak.load(Ordering::Acquire) < 3 * 1024 * 1024);
    assert!(
        guard.counters.used.load(Ordering::Acquire) < 64 * 1024,
        "decoder reservations must be released"
    );
    Ok(())
}
#[test]
fn csv_oversized_line_is_rejected_before_unbounded_growth() -> TestResult {
    let temp = tempfile::tempdir()?;
    std::fs::write(
        temp.path().join("rows.csv"),
        format!("id,payload\n1,{}\n", "x".repeat(512 * 1024)),
    )?;
    let guard = TestGuard::new(32 * 1024);
    let result = scan_local(
        &schema(),
        &csv(temp.path()),
        &ForeignScanRequest::new(None, vec![], None),
        256,
        &Some(guard.clone()),
    );
    assert!(matches!(
        result,
        Err(FederationError::Query(QueryError::ResourceLimit(_)))
    ));
    assert!(guard.counters.peak.load(Ordering::Acquire) <= 32 * 1024);
    assert_eq!(guard.counters.used.load(Ordering::Acquire), 0);
    Ok(())
}
#[test]
fn cancellation_interrupts_before_source_io() -> TestResult {
    let temp = tempfile::tempdir()?;
    let guard = TestGuard::new(1024 * 1024);
    guard.cancelled.store(true, Ordering::Release);
    assert!(matches!(
        scan_local(
            &schema(),
            &csv(temp.path()),
            &ForeignScanRequest::new(None, vec![], None),
            256,
            &Some(guard)
        ),
        Err(FederationError::Query(QueryError::Cancelled(_)))
    ));
    Ok(())
}
#[tokio::test]
async fn pending_remote_io_observes_cancellation_and_drops_future() -> TestResult {
    let guard = TestGuard::new(1024 * 1024);
    let cancel = guard.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(25)).await;
        cancel.cancelled.store(true, Ordering::Release);
    });
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        await_guarded(std::future::pending::<()>(), &Some(guard)),
    )
    .await?;
    assert!(matches!(
        result,
        Err(FederationError::Query(QueryError::Cancelled(_)))
    ));
    Ok(())
}
#[test]
fn parquet_projection_and_zero_column_rows_keep_count_with_bounded_reads() -> TestResult {
    use datafusion::arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::arrow::ArrowWriter;
    let temp = tempfile::tempdir()?;
    let file = File::create(temp.path().join("rows.parquet"))?;
    let arrow = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        arrow.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..100)),
            Arc::new(StringArray::from(vec!["payload"; 100])),
        ],
    )?;
    let mut writer = ArrowWriter::try_new(file, arrow, None)?;
    writer.write(&batch)?;
    writer.close()?;
    let source = ForeignSource::Parquet {
        uri: ObjectStoreUri::from_local_dir(temp.path()),
        path: "rows.parquet".to_owned(),
    };
    for projection in [vec![1, 0, 1], vec![]] {
        let result = scan_local(
            &schema(),
            &source,
            &ForeignScanRequest::new(Some(projection.clone()), vec![], Some(3)),
            2,
            &Some(TestGuard::new(1024 * 1024)),
        )?;
        assert_eq!(result.rows.len(), 3);
        for (i, row) in result.rows.iter().enumerate() {
            let full = [
                Value::Int(i64::try_from(i)?),
                Value::Str("payload".to_owned()),
            ];
            assert_eq!(
                row,
                &projection
                    .iter()
                    .map(|i| full[*i].clone())
                    .collect::<Vec<_>>()
            );
        }
    }
    Ok(())
}
#[test]
fn parquet_forged_footer_length_fails_without_large_allocation() -> TestResult {
    let temp = tempfile::tempdir()?;
    std::fs::write(
        temp.path().join("rows.parquet"),
        b"PAR1\xff\xff\xff\xffPAR1",
    )?;
    let source = ForeignSource::Parquet {
        uri: ObjectStoreUri::from_local_dir(temp.path()),
        path: "rows.parquet".to_owned(),
    };
    let guard = TestGuard::new(128 * 1024);
    assert!(
        scan_local(
            &schema(),
            &source,
            &ForeignScanRequest::new(None, vec![], None),
            256,
            &Some(guard.clone())
        )
        .is_err()
    );
    assert_eq!(guard.counters.peak.load(Ordering::Acquire), 0);
    Ok(())
}
#[test]
fn json_structure_budget_counts_nodes_but_not_punctuation_inside_string() -> TestResult {
    let source = ForeignSource::JsonLines {
        uri: ObjectStoreUri::new("file:///unused/"),
        path: "rows".to_owned(),
    };
    let strings = parser_working_bytes(r#"{"payload":"{:,[{:,[{:,["}"#, &source, 2)?;
    let nodes = parser_working_bytes(r#"{"payload":[[{},{}],[{},{}]]}"#, &source, 2)?;
    assert!(nodes > strings);
    Ok(())
}
