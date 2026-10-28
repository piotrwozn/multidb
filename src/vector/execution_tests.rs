use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use super::*;
use crate::{query::QueryError, repl::SingleNode, storage::MemEngine, txn};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn config() -> VectorCollectionConfig {
    VectorCollectionConfig::new(CollectionId::new(42), 2)
        .with_metric(VectorMetric::L2)
        .with_hnsw(HnswParams::new(4, 32, 32))
}

struct Counted<'a> {
    inner: &'a SingleNode<MemEngine>,
    scanned: AtomicUsize,
}
impl Replication for Counted<'_> {
    fn supports_guarded_reads(&self) -> bool {
        true
    }
    fn visit_get(
        &self,
        table: &str,
        key: &[u8],
        consistency: ReadConsistency,
        budget: &dyn ReadBudget,
        visitor: &mut dyn FnMut(Option<&[u8]>) -> Result<bool, ReplError>,
    ) -> Result<(), ReplError> {
        self.inner
            .visit_get(table, key, consistency, budget, visitor)
    }
    fn visit_range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
        budget: &dyn ReadBudget,
        visitor: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool, ReplError>,
    ) -> Result<(), ReplError> {
        self.inner
            .visit_range(table, start, end, consistency, budget, &mut |key, value| {
                self.scanned.fetch_add(1, Ordering::Relaxed);
                visitor(key, value)
            })
    }
    fn supports_bounded_scans(&self) -> bool {
        true
    }
    fn table_data_version(&self, table: &str) -> Result<Option<u64>, ReplError> {
        self.inner.table_data_version(table)
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
        self.inner.read(table, key, consistency)
    }
    fn range(
        &self,
        _table: &str,
        _start: &[u8],
        _end: &[u8],
        _consistency: ReadConsistency,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        panic!("bounded cold load must not materialize a range")
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
                self.scanned.fetch_add(batch.len(), Ordering::Relaxed);
                on_batch(batch)
            },
        )
    }
}

#[test]
fn shared_handles_load_lazily_and_failed_cold_budget_does_not_publish_partial_index() -> TestResult
{
    let backend = SingleNode::new(MemEngine::new());
    let seed = VectorCollection::new(&backend, config())?;
    let mut first = None;
    for index in 0_u16..100 {
        let id = seed.insert_vector(&Value::Int(i64::from(index)), vec![f32::from(index), 0.])?;
        first.get_or_insert(id);
    }
    let counted = Counted {
        inner: &backend,
        scanned: AtomicUsize::new(0),
    };
    let state = Arc::new(RwLock::new(VectorIndexState::new(
        VectorMetric::L2,
        config().hnsw,
        0,
    )));
    let lazy = VectorCollection::with_shared_state(&counted, config(), Arc::clone(&state))?;
    assert_eq!(counted.scanned.load(Ordering::Relaxed), 0);
    assert_eq!(
        lazy.get_metadata(first.ok_or("missing seeded id")?)?,
        Some(Value::Int(0))
    );
    assert_eq!(counted.scanned.load(Ordering::Relaxed), 0);
    let bounded = lazy.with_execution_limits(ExecutionLimits::new(Duration::from_secs(5), 8192)?);
    assert!(matches!(
        bounded.knn(&[0., 0.], 1),
        Err(VectorError::Execution(QueryError::ResourceLimit(_)))
    ));
    assert!(counted.scanned.load(Ordering::Relaxed) <= 64);
    assert!(!state.read().map_err(|_| "poisoned")?.loaded);
    assert_eq!(state.read().map_err(|_| "poisoned")?.vectors.len(), 0);
    let recovered = VectorCollection::with_shared_state(&counted, config(), state)?;
    assert_eq!(
        recovered.knn(&[0., 0.], 1)?[0].id,
        first.ok_or("missing seeded id")?
    );
    Ok(())
}

#[test]
fn separate_handles_cannot_leave_a_native_vector_index_permanently_stale() -> TestResult {
    let backend = SingleNode::new(MemEngine::new());
    let first = VectorCollection::new(&backend, config())?;
    let distant = first.insert_vector(&Value::Int(1), vec![10., 0.])?;
    assert_eq!(first.knn(&[0., 0.], 1)?[0].id, distant);
    let second = VectorCollection::new(&backend, config())?;
    let nearby = second.insert_vector(&Value::Int(2), vec![0., 0.])?;
    assert_eq!(first.knn(&[0., 0.], 1)?[0].id, nearby);
    second.delete_vector(nearby)?;
    assert_eq!(first.knn(&[0., 0.], 1)?[0].id, distant);
    Ok(())
}

#[test]
fn local_insert_cannot_certify_a_cache_that_missed_an_external_commit() -> TestResult {
    let backend = SingleNode::new(MemEngine::new());
    let first = VectorCollection::new(&backend, config())?;
    let distant = first.insert_vector(&Value::Null, vec![10., 0.])?;
    assert_eq!(first.knn(&[0., 0.], 1)?[0].id, distant);
    let second = VectorCollection::new(&backend, config())?;
    let closest = second.insert_vector(&Value::Null, vec![0., 0.])?;
    first.insert_vector(&Value::Null, vec![5., 0.])?;
    assert_eq!(first.knn(&[0., 0.], 1)?[0].id, closest);
    Ok(())
}

#[test]
fn expired_deadline_does_not_wait_for_an_index_lock() -> TestResult {
    let backend = SingleNode::new(MemEngine::new());
    let vectors = VectorCollection::new(&backend, config())?;
    let state = Arc::clone(&vectors.state);
    let _occupied = state.write().map_err(|_| "poisoned")?;
    let bounded =
        vectors.with_execution_limits(ExecutionLimits::new(Duration::from_millis(2), usize::MAX)?);
    let started = Instant::now();
    assert!(matches!(
        bounded.knn(&[0., 0.], 1),
        Err(VectorError::Execution(QueryError::Timeout(_)))
    ));
    assert!(started.elapsed() < Duration::from_secs(2));
    Ok(())
}

#[test]
fn exact_fallback_keeps_the_same_top_k_and_tie_order_with_bounded_storage() -> TestResult {
    let mut state = VectorIndexState::new(VectorMetric::L2, config().hnsw, 128);
    let items = (0_u8..128)
        .map(|index| {
            (
                DocumentId::from_bytes([index; 16]),
                vec![f32::from(index % 7), 1.],
            )
        })
        .collect();
    state.rebuild(items)?;
    let expected = state.exact_hits_until(&[3., 1.], ExecutionLimits::default())?;
    for k in [1, 3, 10, 128] {
        assert_eq!(
            state.exact_top_hits(&[3., 1.], k, ExecutionLimits::default())?,
            expected[..k]
        );
    }
    Ok(())
}

#[test]
fn hnsw_distance_arithmetic_stops_at_deadline_and_restores_the_thread_scope() -> TestResult {
    let limits = ExecutionLimits::new(Duration::ZERO, usize::MAX)?;
    {
        let _scope = limits.distance_scope();
        assert_eq!(StableL2.eval(&[0.], &[1.]), f32::MAX);
        assert!(matches!(limits.check(), Err(QueryError::Timeout(_))));
    }
    assert_eq!(StableL2.eval(&[0.], &[1.]), 1.);
    Ok(())
}

#[test]
fn metadata_preflight_accepts_large_strings_and_rejects_scalar_expansion() -> TestResult {
    let backend = SingleNode::new(MemEngine::new());
    let vectors = VectorCollection::new(&backend, config())?;
    let text = "x".repeat(2 * 1024 * 1024);
    let large = vectors.insert_vector(&Value::Str(text.clone()), vec![0., 0.])?;
    let array = vectors.insert_vector(&Value::Array(vec![Value::Null; 10_000]), vec![1., 0.])?;
    let bounded = vectors.with_execution_limits(ExecutionLimits::new(
        Duration::from_secs(5),
        16 * 1024 * 1024,
    )?);
    assert_eq!(bounded.get_metadata(large)?, Some(Value::Str(text)));
    let bounded =
        bounded.with_execution_limits(ExecutionLimits::new(Duration::from_secs(5), 128 * 1024)?);
    assert!(matches!(
        bounded.get_metadata(array),
        Err(VectorError::Execution(QueryError::ResourceLimit(_)))
    ));
    Ok(())
}
