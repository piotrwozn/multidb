use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{collections::BTreeMap, ops::Bound};

use crate::storage::{
    Bytes, RangeIter, ReadBudget, ReadTransaction, StorageEngine, StorageError, WriteTransaction,
};

type SharedBytes = Arc<[u8]>;
type Table = BTreeMap<SharedBytes, SharedBytes>;
type DatabaseMap = BTreeMap<String, Arc<Table>>;
type PendingMap = BTreeMap<(String, Bytes), Option<SharedBytes>>;

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
    fn supports_guarded_reads(&self) -> bool {
        true
    }

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
    fn supports_guarded_reads(&self) -> bool {
        true
    }

    fn visit_get(
        &self,
        table: &str,
        key: &[u8],
        budget: &dyn ReadBudget,
        visitor: &mut dyn FnMut(Option<&[u8]>) -> Result<bool, StorageError>,
    ) -> Result<(), StorageError> {
        budget.check()?;
        let value = self.snapshot.get(table).and_then(|data| data.get(key));
        let _ = visitor(value.map(AsRef::as_ref))?;
        budget.check()
    }

    fn visit_range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        budget: &dyn ReadBudget,
        visitor: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool, StorageError>,
    ) -> Result<(), StorageError> {
        budget.check()?;
        if !end.is_empty() && start >= end {
            return Ok(());
        }
        let Some(data) = self.snapshot.get(table) else {
            return Ok(());
        };
        let upper = if end.is_empty() {
            Bound::Unbounded
        } else {
            Bound::Excluded(end)
        };
        let mut entries = data.range::<[u8], _>((Bound::Included(start), upper));
        loop {
            budget.check()?;
            let Some((key, value)) = entries.next() else {
                break;
            };
            if !visitor(key, value)? {
                break;
            }
        }
        budget.check()
    }

    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
        Ok(self
            .snapshot
            .get(table)
            .and_then(|data| data.get(key))
            .map(|value| value.as_ref().to_vec()))
    }

    fn range<'txn>(
        &'txn self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RangeIter<'txn>, StorageError> {
        let Some(data) = self.snapshot.get(table) else {
            return Ok(Box::new(std::iter::empty()));
        };
        if !end.is_empty() && start >= end {
            return Ok(Box::new(std::iter::empty()));
        }
        let upper = if end.is_empty() {
            Bound::Unbounded
        } else {
            Bound::Excluded(end)
        };
        // Borrow the immutable snapshot: clone only the entry the consumer asks for.
        // Collecting here defeats cancellation and SQL's per-batch memory budget.
        Ok(Box::new(
            data.range::<[u8], _>((Bound::Included(start), upper))
                .map(|(key, value)| Ok((key.as_ref().to_vec(), value.as_ref().to_vec()))),
        ))
    }
}

impl ReadTransaction for MemWriteTxn<'_> {
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
        let pending_key = (table.to_owned(), key.to_vec());
        if let Some(value) = self.pending.get(&pending_key) {
            return Ok(value.as_ref().map(|value| value.as_ref().to_vec()));
        }

        Ok(self
            .snapshot
            .get(table)
            .and_then(|data| data.get(key))
            .map(|value| value.as_ref().to_vec()))
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
        self.pending.insert(
            (table.to_owned(), key.to_vec()),
            Some(SharedBytes::from(value)),
        );
        Ok(())
    }

    fn delete(&mut self, table: &str, key: &[u8]) -> Result<(), StorageError> {
        self.pending.insert((table.to_owned(), key.to_vec()), None);
        Ok(())
    }

    fn commit(self) -> Result<(), StorageError> {
        let Self {
            engine,
            _writer,
            snapshot,
            pending,
        } = self;
        if pending.is_empty() {
            return Ok(());
        }
        let mut current = write_inner(&engine.inner)?;
        // The writer mutex still excludes all other commits. Drop only our own
        // snapshot under the root lock, so make_mut can edit in place when no
        // reader needs that generation. Readers force structural COW, while
        // table and byte Arcs keep unrelated tables and payloads shared.
        drop(snapshot);
        let next = Arc::make_mut(&mut *current);
        for ((table, key), value) in pending {
            let data = Arc::make_mut(next.entry(table).or_default());
            match value {
                Some(value) => {
                    if let Some(existing) = data.get_mut(key.as_slice()) {
                        *existing = value;
                    } else {
                        data.insert(SharedBytes::from(key), value);
                    }
                }
                None => {
                    data.remove(key.as_slice());
                }
            }
        }
        Ok(())
    }

    fn rollback(self) {}
}

impl MemWriteTxn<'_> {
    fn materialized_table(&self, table: &str) -> Table {
        let mut materialized = self
            .snapshot
            .get(table)
            .map(|table| table.as_ref().clone())
            .unwrap_or_default();

        for ((pending_table, key), value) in &self.pending {
            if pending_table == table {
                match value {
                    Some(bytes) => {
                        materialized.insert(SharedBytes::from(key.as_slice()), Arc::clone(bytes));
                    }
                    None => {
                        materialized.remove(key.as_slice());
                    }
                }
            }
        }

        materialized
    }
}

fn collect_range(table: &Table, start: &[u8], end: &[u8]) -> Vec<(Bytes, Bytes)> {
    if !end.is_empty() && start >= end {
        return Vec::new();
    }
    let upper = if end.is_empty() {
        Bound::Unbounded
    } else {
        Bound::Excluded(end)
    };
    table
        .range::<[u8], _>((Bound::Included(start), upper))
        .map(|(key, value)| (key.as_ref().to_vec(), value.as_ref().to_vec()))
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

#[cfg(test)]
mod cow_tests;
