use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::storage::{
    Bytes, RangeIter, ReadTransaction, StorageEngine, StorageError, WriteTransaction,
};

type Table = BTreeMap<Bytes, Bytes>;
type DatabaseMap = BTreeMap<String, Table>;
type PendingMap = BTreeMap<(String, Bytes), Option<Bytes>>;

#[derive(Debug)]
pub struct MemEngine {
    inner: RwLock<Arc<DatabaseMap>>,
    writer: Mutex<()>,
}

#[derive(Debug)]
pub struct MemReadTxn {
    snapshot: Arc<DatabaseMap>,
}

#[derive(Debug)]
pub struct MemWriteTxn<'engine> {
    engine: &'engine MemEngine,
    _writer: MutexGuard<'engine, ()>,
    snapshot: Arc<DatabaseMap>,
    pending: PendingMap,
}

impl MemEngine {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for MemEngine {
    fn default() -> Self {
        Self {
            inner: RwLock::new(Arc::new(DatabaseMap::new())),
            writer: Mutex::new(()),
        }
    }
}

impl StorageEngine for MemEngine {
    type ReadTxn<'a> = MemReadTxn;
    type WriteTxn<'a> = MemWriteTxn<'a>;

    fn begin_read(&self) -> Result<Self::ReadTxn<'_>, StorageError> {
        let snapshot = Arc::clone(&*read_inner(&self.inner)?);
        Ok(MemReadTxn { snapshot })
    }

    fn begin_write(&self) -> Result<Self::WriteTxn<'_>, StorageError> {
        let writer = lock_writer(&self.writer)?;
        let snapshot = Arc::clone(&*read_inner(&self.inner)?);
        Ok(MemWriteTxn {
            engine: self,
            _writer: writer,
            snapshot,
            pending: PendingMap::new(),
        })
    }
}

impl ReadTransaction for MemReadTxn {
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
        Ok(self
            .snapshot
            .get(table)
            .and_then(|data| data.get(key))
            .cloned())
    }

    fn range<'txn>(
        &'txn self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RangeIter<'txn>, StorageError> {
        let rows = self
            .snapshot
            .get(table)
            .map_or_else(Vec::new, |data| collect_range(data, start, end));

        Ok(Box::new(rows.into_iter().map(Ok)))
    }
}

impl ReadTransaction for MemWriteTxn<'_> {
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
        let pending_key = (table.to_owned(), key.to_vec());
        if let Some(value) = self.pending.get(&pending_key) {
            return Ok(value.clone());
        }

        Ok(self
            .snapshot
            .get(table)
            .and_then(|data| data.get(key))
            .cloned())
    }

    fn range<'txn>(
        &'txn self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RangeIter<'txn>, StorageError> {
        let materialized = self.materialized_table(table);
        let rows = collect_range(&materialized, start, end);

        Ok(Box::new(rows.into_iter().map(Ok)))
    }
}

impl WriteTransaction for MemWriteTxn<'_> {
    fn put(&mut self, table: &str, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.pending
            .insert((table.to_owned(), key.to_vec()), Some(value.to_vec()));
        Ok(())
    }

    fn delete(&mut self, table: &str, key: &[u8]) -> Result<(), StorageError> {
        self.pending.insert((table.to_owned(), key.to_vec()), None);
        Ok(())
    }

    fn commit(self) -> Result<(), StorageError> {
        let mut next = self.snapshot.as_ref().clone();

        for ((table, key), value) in self.pending {
            let data = next.entry(table).or_default();
            match value {
                Some(bytes) => {
                    data.insert(key, bytes);
                }
                None => {
                    data.remove(&key);
                }
            }
        }

        *write_inner(&self.engine.inner)? = Arc::new(next);
        Ok(())
    }

    fn rollback(self) {}
}

impl MemWriteTxn<'_> {
    fn materialized_table(&self, table: &str) -> Table {
        let mut materialized = self.snapshot.get(table).cloned().unwrap_or_default();

        for ((pending_table, key), value) in &self.pending {
            if pending_table == table {
                match value {
                    Some(bytes) => {
                        materialized.insert(key.clone(), bytes.clone());
                    }
                    None => {
                        materialized.remove(key);
                    }
                }
            }
        }

        materialized
    }
}

fn collect_range(table: &Table, start: &[u8], end: &[u8]) -> Vec<(Bytes, Bytes)> {
    if end.is_empty() {
        return table
            .range(start.to_vec()..)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
    }

    if start >= end {
        return Vec::new();
    }

    table
        .range(start.to_vec()..end.to_vec())
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn read_inner(
    lock: &RwLock<Arc<DatabaseMap>>,
) -> Result<RwLockReadGuard<'_, Arc<DatabaseMap>>, StorageError> {
    lock.read()
        .map_err(|_| StorageError::Backend("memory lock poisoned".to_owned()))
}

fn write_inner(
    lock: &RwLock<Arc<DatabaseMap>>,
) -> Result<RwLockWriteGuard<'_, Arc<DatabaseMap>>, StorageError> {
    lock.write()
        .map_err(|_| StorageError::Backend("memory lock poisoned".to_owned()))
}

fn lock_writer(lock: &Mutex<()>) -> Result<MutexGuard<'_, ()>, StorageError> {
    lock.lock()
        .map_err(|_| StorageError::Backend("memory writer lock poisoned".to_owned()))
}
