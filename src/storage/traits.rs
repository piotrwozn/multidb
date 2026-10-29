use crate::storage::{Bytes, StorageError};

/// Statement-local checks and reservations for query-owned read allocations.
/// Borrowed storage pages remain owned by the engine. Before allocating a copy
/// or a decoded buffer, a consumer must retain a reservation for its lifetime.
pub trait ReadBudget: Send + Sync {
    /// Checks cancellation and the statement deadline.
    /// # Errors
    /// Fails when the read may no longer continue.
    fn check(&self) -> Result<(), StorageError>;

    /// Atomically reserves bytes in the statement's shared memory pool.
    /// # Errors
    /// Fails before allocation when the memory budget cannot admit the buffer.
    fn reserve(&self, bytes: usize) -> Result<Box<dyn ReadReservation>, StorageError>;

    /// An advisory remaining limit; `reserve` must still enforce concurrent use.
    fn remaining_bytes(&self) -> usize;

    /// The absolute local deadline, when one exists.
    fn deadline(&self) -> Option<std::time::Instant> {
        None
    }
}

/// Releasing this token releases its corresponding memory reservation.
pub trait ReadReservation: Send + Sync {}
impl<T: Send + Sync> ReadReservation for T {}

pub type RangeIter<'txn> = Box<dyn Iterator<Item = Result<(Bytes, Bytes), StorageError>> + 'txn>;

/// Byte-oriented storage engine.
///
/// These compile-fail examples are part of the storage contract: a committed
/// transaction is consumed, and range iterators cannot outlive their snapshot.
///
/// ```compile_fail
/// use multidb::storage::{MemEngine, StorageEngine, WriteTransaction};
///
/// let db = MemEngine::new();
/// let mut tx = db.begin_write().unwrap();
/// tx.put("t", b"k", b"v").unwrap();
/// tx.commit().unwrap();
/// tx.put("t", b"k2", b"v2").unwrap();
/// ```
///
/// ```compile_fail
/// use multidb::storage::{MemEngine, ReadTransaction, StorageEngine};
///
/// let db = MemEngine::new();
/// let rows = {
///     let tx = db.begin_read().unwrap();
///     tx.range("t", b"a", b"z").unwrap()
/// };
/// for row in rows {
///     let _ = row.unwrap();
/// }
/// ```
pub trait StorageEngine: Send + Sync + 'static {
    /// Whether reads implement borrowed visitors before cloning/decoding values.
    /// Legacy custom engines retain their API behavior without claiming this bound.
    fn supports_guarded_reads(&self) -> bool {
        false
    }

    type ReadTxn<'a>: ReadTransaction
    where
        Self: 'a;

    type WriteTxn<'a>: WriteTransaction
    where
        Self: 'a;

    /// Starts a read transaction.
    /// # Errors
    /// Fails if the backend cannot open a snapshot.
    fn begin_read(&self) -> Result<Self::ReadTxn<'_>, StorageError>;

    /// Starts a write transaction.
    ///
    /// Backends must either serialize active writers for the whole write
    /// transaction lifetime or make commit validation reject write-write races
    /// with [`StorageError::Conflict`]. Silent lost updates are not permitted.
    /// # Errors
    /// Fails if the backend cannot open a writer.
    fn begin_write(&self) -> Result<Self::WriteTxn<'_>, StorageError>;
}

pub trait ReadTransaction {
    /// Whether this snapshot implements guarded borrowed reads natively.
    fn supports_guarded_reads(&self) -> bool {
        false
    }

    /// Visits one value without transferring an owned storage copy. The callback
    /// must reserve any output it retains. Compatibility implementations may own
    /// the input first and must not advertise guarded-read support.
    /// # Errors
    /// Fails on read errors, budget rejection, or callback failure.
    // Explicit borrowed callback lifetimes preserve custom implementations while
    // allowing allocation admission before a consumer copies or decodes a value.
    #[allow(clippy::type_complexity)]
    fn visit_get(
        &self,
        table: &str,
        key: &[u8],
        budget: &dyn ReadBudget,
        visitor: &mut dyn FnMut(Option<&[u8]>) -> Result<bool, StorageError>,
    ) -> Result<(), StorageError> {
        budget.check()?;
        let value = self.get(table, key)?;
        budget.check()?;
        let _input = value
            .as_ref()
            .map(|value| budget.reserve(value.len()))
            .transpose()?;
        let _ = visitor(value.as_deref())?;
        budget.check()
    }

    /// Visits keys in order, including missing and duplicate entries, in this
    /// snapshot. Returning false stops before reading the next key.
    /// # Errors
    /// Fails on read errors, budget rejection, or callback failure.
    // Explicit borrowed callback lifetimes preserve custom implementations while
    // allowing allocation admission before a consumer copies or decodes a value.
    #[allow(clippy::type_complexity)]
    fn visit_many(
        &self,
        table: &str,
        keys: &[Bytes],
        budget: &dyn ReadBudget,
        visitor: &mut dyn FnMut(usize, Option<&[u8]>) -> Result<bool, StorageError>,
    ) -> Result<(), StorageError> {
        for (index, key) in keys.iter().enumerate() {
            let mut keep_going = true;
            self.visit_get(table, key, budget, &mut |value| {
                keep_going = visitor(index, value)?;
                Ok(keep_going)
            })?;
            if !keep_going {
                break;
            }
        }
        budget.check()
    }

    /// Visits borrowed records in `[start, end)` (empty end means unbounded),
    /// stopping before the next storage pull after false/cancellation. Native
    /// implementations do not clone record buffers before this callback.
    /// # Errors
    /// Fails on read errors, budget rejection, or callback failure.
    // Explicit borrowed callback lifetimes preserve custom implementations while
    // allowing allocation admission before a consumer copies or decodes a value.
    #[allow(clippy::type_complexity)]
    fn visit_range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        budget: &dyn ReadBudget,
        visitor: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool, StorageError>,
    ) -> Result<(), StorageError> {
        budget.check()?;
        let mut entries = self.range(table, start, end)?;
        loop {
            budget.check()?;
            let Some(entry) = entries.next() else {
                break;
            };
            let (key, value) = entry?;
            let _input = budget.reserve(key.len().saturating_add(value.len()))?;
            if !visitor(&key, &value)? {
                break;
            }
        }
        budget.check()
    }

    /// Reads one key.
    /// # Errors
    /// Fails on backend read errors.
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, StorageError>;

    /// Reads `[start, end)`, or `[start, ..)` when `end` is empty.
    /// # Errors
    /// Fails on backend read errors.
    fn range<'txn>(
        &'txn self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RangeIter<'txn>, StorageError>;
}

pub trait WriteTransaction: ReadTransaction {
    /// Writes one key.
    /// # Errors
    /// Fails if the write cannot be staged.
    fn put(&mut self, table: &str, key: &[u8], value: &[u8]) -> Result<(), StorageError>;

    /// Deletes one key.
    /// # Errors
    /// Fails if the delete cannot be staged.
    fn delete(&mut self, table: &str, key: &[u8]) -> Result<(), StorageError>;

    /// Commits the transaction.
    ///
    /// A backend that allows overlapping writers must validate that staged
    /// writes still match the transaction snapshot and return
    /// [`StorageError::Conflict`] when they do not.
    /// # Errors
    /// Fails if the backend cannot commit.
    fn commit(self) -> Result<(), StorageError>;

    /// Drops staged writes.
    fn rollback(self);
}
