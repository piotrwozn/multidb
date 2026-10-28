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

    pub(super) fn remaining_bytes(&self) -> usize {
        self.limit.saturating_sub(self.pool.reserved())
    }

    pub(super) fn scratch(&self) -> MemoryReservation {
        MemoryConsumer::new("multidb decoder scratch").register(&self.pool)
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

    pub(super) fn charge_arrow_build(
        &self,
        schema: &super::SchemaRef,
        rows: &[Row],
    ) -> Result<(), QueryError> {
        if rows.is_empty() {
            return self.check();
        }
        // Array metadata, null/offset buffers and builder growth coexist with
        // retained Rust rows. Charge their full working set before Arrow allocates.
        let mut bytes = 1_024_usize.saturating_add(schema.fields().len().saturating_mul(512));
        for row in rows {
            self.check()?;
            for (value, field) in row.iter().zip(schema.fields()) {
                let heap = super::row_scan::value_heap_bytes(value, self.limit)?;
                let copies = if field.data_type() == &datafusion::arrow::datatypes::DataType::Utf8
                    && !matches!(value, super::Value::Str(_) | super::Value::Null)
                {
                    32
                } else {
                    2
                };
                bytes = bytes
                    .saturating_add(32)
                    .saturating_add(heap.saturating_mul(copies));
            }
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

fn storage_budget_error(error: QueryError) -> crate::storage::StorageError {
    match error {
        QueryError::ResourceLimit(reason) => crate::storage::StorageError::ResourceLimit(reason),
        QueryError::Timeout(reason) => crate::storage::StorageError::Timeout(reason),
        QueryError::Cancelled(reason) => crate::storage::StorageError::Cancelled(reason),
        other => crate::storage::StorageError::Backend(other.to_string()),
    }
}

impl crate::storage::ReadBudget for Budget {
    fn check(&self) -> Result<(), crate::storage::StorageError> {
        Self::check(self).map_err(storage_budget_error)
    }

    fn reserve(
        &self,
        bytes: usize,
    ) -> Result<Box<dyn crate::storage::ReadReservation>, crate::storage::StorageError> {
        self.check().map_err(storage_budget_error)?;
        let reservation = self.scratch();
        reservation.try_grow(bytes).map_err(|error| {
            self.resource_failed.store(true, Ordering::Release);
            crate::storage::StorageError::ResourceLimit(error.to_string())
        })?;
        Ok(Box::new(reservation))
    }

    fn remaining_bytes(&self) -> usize {
        Self::remaining_bytes(self)
    }
    fn deadline(&self) -> Option<Instant> {
        self.deadline.get().copied()
    }
}

impl crate::federation::ForeignScanGuard for Budget {
    fn check(&self) -> Result<(), QueryError> {
        Self::check(self)
    }

    fn charge(&self, retained_bytes: usize) -> Result<(), QueryError> {
        Self::charge(self, retained_bytes)
    }

    fn remaining_bytes(&self) -> usize {
        Self::remaining_bytes(self)
    }

    fn reserve(
        &self,
        bytes: usize,
    ) -> Result<Box<dyn crate::federation::ForeignScanReservation>, QueryError> {
        self.check()?;
        let reservation =
            MemoryConsumer::new("multidb foreign decoder scratch").register(&self.pool);
        reservation.try_grow(bytes).map_err(|error| {
            self.resource_failed.store(true, Ordering::Release);
            QueryError::ResourceLimit(error.to_string())
        })?;
        Ok(Box::new(reservation))
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
    fn charge(&self, table: &str, key_bytes: usize, value: &[u8]) -> Result<(), super::ReplError> {
        let bytes = key_bytes
            .saturating_add(value.len())
            .saturating_mul(4)
            .saturating_add(512);
        self.budget
            .charge(bytes)
            .map_err(|error| super::ReplError::Storage(storage_budget_error(error)))?;
        if matches!(table, super::REL_ROWS_TABLE | crate::model::DOCUMENT_TABLE) {
            let decoded =
                crate::model::value_decode_memory_bound(value, self.budget.remaining_bytes())?;
            // Legacy collectors retain encoded records and decoded Values together.
            // Bound Value nodes as well as wire bytes before constructing them.
            self.budget
                .charge(decoded.saturating_mul(2))
                .map_err(|error| super::ReplError::Storage(storage_budget_error(error)))?;
        }
        Ok(())
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
        let mut out = None;
        self.inner.visit_get(
            table,
            key,
            consistency,
            self.budget.as_ref(),
            &mut |value| {
                if let Some(value) = value {
                    self.charge(table, key.len(), value)?;
                    out = Some(value.to_vec());
                }
                Ok(false)
            },
        )?;
        Ok(out)
    }

    fn read_many(
        &self,
        table: &str,
        keys: &[super::Bytes],
        consistency: super::ReadConsistency,
    ) -> Result<Vec<Option<super::Bytes>>, super::ReplError> {
        let mut values = Vec::new();
        self.inner.visit_many(
            table,
            keys,
            consistency,
            self.budget.as_ref(),
            &mut |index, value| {
                if index != values.len() || index >= keys.len() {
                    return Err(crate::storage::StorageError::Corruption(
                        "multi-key visitor index mismatch".to_owned(),
                    )
                    .into());
                }
                self.budget
                    .charge(4 * std::mem::size_of::<Option<super::Bytes>>())
                    .map_err(|error| super::ReplError::Storage(storage_budget_error(error)))?;
                if let Some(value) = value {
                    self.charge(table, keys[index].len(), value)?;
                }
                values.push(value.map(<[u8]>::to_vec));
                Ok(true)
            },
        )?;
        if values.len() != keys.len() {
            return Err(crate::storage::StorageError::Corruption(
                "multi-key visitor result length mismatch".to_owned(),
            )
            .into());
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
        self.inner.visit_range(
            table,
            start,
            end,
            consistency,
            self.budget.as_ref(),
            &mut |key, value| {
                self.charge(table, key.len(), value)?;
                entries.push((key.to_vec(), value.to_vec()));
                Ok(true)
            },
        )?;
        Ok(entries)
    }

    fn supports_guarded_reads(&self) -> bool {
        self.inner.supports_guarded_reads()
    }

    fn visit_get(
        &self,
        table: &str,
        key: &[u8],
        consistency: super::ReadConsistency,
        budget: &dyn crate::storage::ReadBudget,
        visitor: &mut dyn FnMut(Option<&[u8]>) -> Result<bool, super::ReplError>,
    ) -> Result<(), super::ReplError> {
        self.inner
            .visit_get(table, key, consistency, budget, visitor)
    }

    fn visit_many(
        &self,
        table: &str,
        keys: &[super::Bytes],
        consistency: super::ReadConsistency,
        budget: &dyn crate::storage::ReadBudget,
        visitor: &mut dyn FnMut(usize, Option<&[u8]>) -> Result<bool, super::ReplError>,
    ) -> Result<(), super::ReplError> {
        self.inner
            .visit_many(table, keys, consistency, budget, visitor)
    }

    fn visit_range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: super::ReadConsistency,
        budget: &dyn crate::storage::ReadBudget,
        visitor: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool, super::ReplError>,
    ) -> Result<(), super::ReplError> {
        self.inner
            .visit_range(table, start, end, consistency, budget, visitor)
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
        if cancelled() {
            return Ok(());
        }
        self.inner.visit_range(
            table,
            start,
            end,
            consistency,
            self.budget.as_ref(),
            &mut |key, value| {
                if cancelled() {
                    return Ok(false);
                }
                self.charge(table, key.len(), value)?;
                on_batch(&[(key.to_vec(), value.to_vec())])
            },
        )
    }
}
