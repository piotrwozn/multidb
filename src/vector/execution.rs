//! Request-scoped bounds shared by vector and graph-assisted reads.

use std::{
    cell::Cell,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use crate::model::Value;
use crate::query::QueryError;
use crate::storage::{ReadBudget, ReadReservation, StorageError};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ExecutionLimits {
    deadline: Option<Instant>,
    memory_bytes: Option<usize>,
}

/// Accounts retained outputs and temporary backend decoding in one request.
pub(crate) struct ExecutionReadBudget {
    limits: ExecutionLimits,
    retained: Arc<AtomicUsize>,
}

impl ExecutionReadBudget {
    pub(crate) fn new(limits: ExecutionLimits) -> Self {
        Self {
            limits,
            retained: Arc::new(AtomicUsize::new(0)),
        }
    }
}

struct ReservedReadBytes {
    retained: Arc<AtomicUsize>,
    bytes: usize,
}

impl Drop for ReservedReadBytes {
    fn drop(&mut self) {
        self.retained.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

impl ReadBudget for ExecutionReadBudget {
    fn check(&self) -> Result<(), StorageError> {
        self.limits
            .check()
            .map_err(|error| StorageError::Timeout(error.to_string()))
    }

    fn reserve(&self, bytes: usize) -> Result<Box<dyn ReadReservation>, StorageError> {
        self.check()?;
        let mut retained = self.retained.load(Ordering::Acquire);
        loop {
            let next = retained.checked_add(bytes).ok_or_else(|| {
                StorageError::ResourceLimit("model read allocation overflows usize".to_owned())
            })?;
            self.limits
                .check_memory(next, "model read buffers")
                .map_err(|error| StorageError::ResourceLimit(error.to_string()))?;
            match self.retained.compare_exchange_weak(
                retained,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(Box::new(ReservedReadBytes {
                        retained: Arc::clone(&self.retained),
                        bytes,
                    }));
                }
                Err(current) => retained = current,
            }
        }
    }

    fn remaining_bytes(&self) -> usize {
        self.limits
            .remaining_memory(self.retained.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    fn deadline(&self) -> Option<Instant> {
        self.limits.deadline
    }
}

impl ExecutionLimits {
    pub(crate) const fn unbounded() -> Self {
        Self {
            deadline: None,
            memory_bytes: None,
        }
    }
    pub(crate) fn new(timeout: Duration, memory_bytes: usize) -> Result<Self, QueryError> {
        Ok(Self {
            deadline: Some(Instant::now().checked_add(timeout).ok_or_else(|| {
                QueryError::ResourceLimit("model timeout exceeds monotonic clock range".to_owned())
            })?),
            memory_bytes: Some(memory_bytes),
        })
    }

    pub(crate) fn check(self) -> Result<(), QueryError> {
        if self.expired() {
            return Err(QueryError::Timeout(
                "model execution deadline exceeded".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn expired(self) -> bool {
        self.deadline.is_some_and(|end| Instant::now() >= end)
    }

    pub(crate) fn bounded(self) -> bool {
        self.deadline.is_some() || self.memory_bytes.is_some()
    }

    pub(crate) fn check_memory(self, bytes: usize, purpose: &str) -> Result<(), QueryError> {
        if self.memory_bytes.is_some_and(|limit| bytes > limit) {
            return Err(QueryError::ResourceLimit(format!(
                "{purpose} exceeds model query memory limit"
            )));
        }
        Ok(())
    }

    pub(crate) fn reserve(self, bytes: usize) -> Result<Self, QueryError> {
        self.check_memory(bytes, "model result reservations")?;
        Ok(Self {
            memory_bytes: self.memory_bytes.map(|limit| limit - bytes),
            ..self
        })
    }

    pub(crate) fn cap_memory(self, bytes: usize) -> Self {
        Self {
            memory_bytes: Some(self.memory_bytes.map_or(bytes, |limit| limit.min(bytes))),
            ..self
        }
    }

    pub(crate) fn remaining_memory(self, retained: usize) -> Result<usize, QueryError> {
        self.check_memory(retained, "retained vector index")?;
        Ok(self
            .memory_bytes
            .map_or(usize::MAX, |limit| limit - retained))
    }

    pub(crate) fn distance_scope(self) -> DistanceDeadline {
        DistanceDeadline(DISTANCE_DEADLINE.replace(self.deadline))
    }
}

thread_local! {
    static DISTANCE_DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };
}

/// The HNSW dependency has no cancellation callback. Once a deadline expires,
/// its distance callback stops arithmetic; the enclosing operation then returns
/// the typed timeout and never publishes a partially built index or ANN answer.
pub(super) fn distance_expired() -> bool {
    DISTANCE_DEADLINE.with(|deadline| deadline.get().is_some_and(|end| Instant::now() >= end))
}

pub(crate) struct DistanceDeadline(Option<Instant>);

impl Drop for DistanceDeadline {
    fn drop(&mut self) {
        DISTANCE_DEADLINE.set(self.0);
    }
}

/// Retained heap capacity, including a conservative allowance for B-tree nodes.
/// This avoids serializing metadata a second time merely to account for it.
pub(crate) fn retained_value_bytes(value: &Value) -> usize {
    let inline = std::mem::size_of::<Value>();
    let allocated = match value {
        Value::Str(text) => text.capacity(),
        Value::Bytes(bytes) => bytes.capacity(),
        Value::Vector(values) => values.capacity().saturating_mul(std::mem::size_of::<f32>()),
        Value::Array(values) => values
            .iter()
            .fold(values.capacity().saturating_mul(inline), |bytes, value| {
                bytes.saturating_add(retained_value_bytes(value).saturating_sub(inline))
            }),
        Value::Object(values) => values.iter().fold(
            if values.is_empty() { 0_usize } else { 1024 },
            |bytes, (key, value)| {
                bytes
                    .saturating_add(256)
                    .saturating_add(key.capacity())
                    .saturating_add(retained_value_bytes(value))
            },
        ),
        _ => 0,
    };
    inline.saturating_add(allocated)
}
