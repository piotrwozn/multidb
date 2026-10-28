//! Statement-local cancellation and reservations shared with `DataFusion` operators.

use std::{
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation,
};

use super::{QueryError, RecordBatch, Row};
use crate::performance::QueryExecutionConfig;

#[derive(Debug)]
pub(super) struct Budget {
    pub(super) pool: Arc<dyn MemoryPool>,
    reservation: MemoryReservation,
    deadline: OnceLock<Instant>,
    cancelled: AtomicBool,
    timeout_ms: u64,
    limit: usize,
    resource_failed: AtomicBool,
}

impl Budget {
    pub(super) fn new(config: &QueryExecutionConfig) -> Arc<Self> {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(config.memory_limit_bytes));
        let reservation = MemoryConsumer::new("multidb retained source and result").register(&pool);
        Arc::new(Self {
            pool,
            reservation,
            deadline: OnceLock::new(),
            cancelled: AtomicBool::new(false),
            timeout_ms: config.timeout_ms,
            limit: config.memory_limit_bytes,
            resource_failed: AtomicBool::new(false),
        })
    }

    pub(super) fn start(self: &Arc<Self>, planning: Duration) -> Result<CancelOnDrop, QueryError> {
        let remaining = Duration::from_millis(self.timeout_ms).saturating_sub(planning);
        let deadline = Instant::now().checked_add(remaining).ok_or_else(|| {
            QueryError::ResourceLimit("query timeout exceeds the monotonic clock range".to_owned())
        })?;
        self.deadline
            .set(deadline)
            .map_err(|_| QueryError::Cancelled("query budget already started".to_owned()))?;
        self.check()?;
        Ok(CancelOnDrop(Arc::clone(self)))
    }

    pub(super) fn expired(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
            || self
                .deadline
                .get()
                .is_some_and(|deadline| Instant::now() >= *deadline)
    }

    pub(super) fn check(&self) -> Result<(), QueryError> {
        if self.expired() {
            Err(QueryError::Timeout(format!(
                "query exceeded {} ms",
                self.timeout_ms
            )))
        } else if self.resource_failed.load(Ordering::Acquire) {
            Err(QueryError::ResourceLimit(
                "query source exceeds retained memory budget".to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    pub(super) fn timeout_ms(&self) -> u64 {
        self.timeout_ms
    }

    pub(super) fn charge(&self, bytes: usize) -> Result<(), QueryError> {
        self.check()?;
        self.reservation.try_grow(bytes).map_err(|error| {
            self.resource_failed.store(true, Ordering::Release);
            QueryError::ResourceLimit(error.to_string())
        })
    }

    pub(super) fn charge_row(
        &self,
        row: &Row,
        projection: Option<&[usize]>,
    ) -> Result<(), QueryError> {
        let mut bytes = 4 * std::mem::size_of::<Row>();
        let width = projection.map_or(row.len(), <[usize]>::len);
        for index in 0..width {
            let column = projection.map_or(index, |projection| projection[index]);
            let value = row
                .get(column)
                .ok_or_else(|| QueryError::InvalidRow("projection column is missing".to_owned()))?;
            bytes = bytes
                .checked_add(4 * std::mem::size_of::<super::Value>())
                .and_then(|bytes| {
                    bytes.checked_add(super::row_scan::value_heap_bytes(value, self.limit).ok()?)
                })
                .ok_or_else(|| {
                    QueryError::ResourceLimit(
                        "projected source row exceeds query memory limit".to_owned(),
                    )
                })?;
        }
        self.charge(bytes)
    }

    pub(super) fn charge_batch(&self, batch: &RecordBatch) -> Result<(), QueryError> {
        // Include Rust result values as well as Arrow buffers, even for zero-column batches.
        let row_bytes = batch
            .num_columns()
            .checked_mul(4 * std::mem::size_of::<super::Value>())
            .and_then(|bytes| bytes.checked_add(4 * std::mem::size_of::<Row>()))
            .and_then(|bytes| bytes.checked_mul(batch.num_rows()));
        let bytes = row_bytes
            .and_then(|bytes| bytes.checked_add(batch.get_array_memory_size().saturating_mul(4)))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<RecordBatch>()))
            .ok_or_else(|| {
                QueryError::ResourceLimit("query batch exceeds memory limit".to_owned())
            })?;
        self.charge(bytes)
    }

    pub(super) fn charge_cached_source(&self, batch: &RecordBatch) -> Result<(), QueryError> {
        // A cache hit clones Arrow buffer references, not decoded Rust result rows.
        // Charge those rows only at final collection, exactly as on a cache miss.
        self.charge(
            batch
                .get_array_memory_size()
                .saturating_add(std::mem::size_of::<RecordBatch>()),
        )
    }
}

pub(super) struct CancelOnDrop(Arc<Budget>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancelled.store(true, Ordering::Release);
    }
}

/// Legacy providers retain raw records before converting them to Arrow. Bound that
/// materialization at the storage callback, rather than checking after `range` returns.
pub(super) fn replication(
    inner: Arc<dyn super::Replication>,
    budget: &Arc<Budget>,
) -> Arc<dyn super::Replication> {
    Arc::new(BudgetReplication {
        inner,
        budget: Arc::clone(budget),
    })
}

struct BudgetReplication {
    inner: Arc<dyn super::Replication>,
    budget: Arc<Budget>,
}

impl BudgetReplication {
    fn charge(&self, key_bytes: usize, value_bytes: usize) -> Result<(), super::ReplError> {
        let bytes = key_bytes
            .saturating_add(value_bytes)
            .saturating_mul(4)
            .saturating_add(512);
        self.budget
            .charge(bytes)
            .map_err(|error| super::ReplError::Rejected(error.to_string()))
    }
}

impl super::Replication for BudgetReplication {
    fn propose(&self, _op: super::Op) -> Result<(), super::ReplError> {
        Err(super::ReplError::Unsupported(
            "read-only SQL storage adapter".to_owned(),
        ))
    }

    fn propose_batch(&self, _ops: Vec<super::Op>) -> Result<(), super::ReplError> {
        Err(super::ReplError::Unsupported(
            "read-only SQL storage adapter".to_owned(),
        ))
    }

    fn propose_authorized_batch(
        &self,
        _ops: Vec<super::Op>,
        _authorization: crate::txn::WriteAuthorization,
    ) -> Result<(), super::ReplError> {
        Err(super::ReplError::Unsupported(
            "read-only SQL storage adapter".to_owned(),
        ))
    }

    fn supports_bounded_scans(&self) -> bool {
        self.inner.supports_bounded_scans()
    }

    fn table_data_version(&self, table: &str) -> Result<Option<u64>, super::ReplError> {
        self.inner.table_data_version(table)
    }

    fn read(
        &self,
        table: &str,
        key: &[u8],
        consistency: super::ReadConsistency,
    ) -> Result<Option<super::Bytes>, super::ReplError> {
        self.budget
            .check()
            .map_err(|error| super::ReplError::Rejected(error.to_string()))?;
        let value = self.inner.read(table, key, consistency)?;
        if let Some(value) = &value {
            self.charge(key.len(), value.len())?;
        }
        Ok(value)
    }

    fn read_many(
        &self,
        table: &str,
        keys: &[super::Bytes],
        consistency: super::ReadConsistency,
    ) -> Result<Vec<Option<super::Bytes>>, super::ReplError> {
        self.budget
            .check()
            .map_err(|error| super::ReplError::Rejected(error.to_string()))?;
        let values = self.inner.read_many(table, keys, consistency)?;
        for (index, value) in values.iter().enumerate() {
            if let Some(value) = value {
                self.charge(keys.get(index).map_or(0, Vec::len), value.len())?;
            }
        }
        Ok(values)
    }

    fn range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: super::ReadConsistency,
    ) -> Result<Vec<(super::Bytes, super::Bytes)>, super::ReplError> {
        let mut entries = Vec::new();
        self.scan_range_batches(table, start, end, consistency, 1, &|| false, &mut |batch| {
            entries.extend_from_slice(batch);
            Ok(true)
        })?;
        Ok(entries)
    }

    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    fn scan_range_batches(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: super::ReadConsistency,
        _batch_rows: usize,
        cancelled: &dyn Fn() -> bool,
        on_batch: &mut dyn FnMut(&[(super::Bytes, super::Bytes)]) -> Result<bool, super::ReplError>,
    ) -> Result<(), super::ReplError> {
        // Preserve AP/custom-backend query support. Their compatibility scanner may
        // materialize a range internally before the first callback: retained SQL
        // buffers are charged here, but only native bounded backends can also bound
        // that backend's temporary allocation and stop its storage work promptly.
        self.inner.scan_range_batches(
            table,
            start,
            end,
            consistency,
            1,
            &|| cancelled() || self.budget.expired(),
            &mut |batch| {
                for (key, value) in batch {
                    self.charge(key.len(), value.len())?;
                }
                on_batch(batch)
            },
        )?;
        self.budget
            .check()
            .map_err(|error| super::ReplError::Rejected(error.to_string()))
    }
}
