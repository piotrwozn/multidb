use crate::storage::{Bytes, StorageError};

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
