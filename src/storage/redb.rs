use std::path::Path;

use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, TableDefinition, TableError, TableHandle,
};

use crate::storage::{
    Bytes, RangeIter, ReadTransaction, StorageEngine, StorageError, WriteTransaction,
};

type RawTableDefinition<'name> = TableDefinition<'name, &'static [u8], &'static [u8]>;
type RedbReadOnlyTable = redb::ReadOnlyTable<&'static [u8], &'static [u8]>;

pub struct RedbEngine {
    db: redb::Database,
    two_phase_commit: bool,
}

pub struct RedbReadTxn {
    tx: redb::ReadTransaction,
}

pub struct RedbWriteTxn {
    tx: redb::WriteTransaction,
}

impl RedbEngine {
    /// Opens a redb database.
    /// # Errors
    /// Fails if redb cannot create or open the file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Ok(Self {
            db: Database::create(path).map_err(to_storage_err)?,
            two_phase_commit: false,
        })
    }

    /// Opens a redb database with two-phase commit enabled for every write transaction.
    /// # Errors
    /// Fails if redb cannot create or open the file.
    pub fn open_high_durability(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Ok(Self {
            db: Database::create(path).map_err(to_storage_err)?,
            two_phase_commit: true,
        })
    }
}

impl StorageEngine for RedbEngine {
    type ReadTxn<'a> = RedbReadTxn;
    type WriteTxn<'a> = RedbWriteTxn;

    fn begin_read(&self) -> Result<Self::ReadTxn<'_>, StorageError> {
        Ok(RedbReadTxn {
            tx: self.db.begin_read().map_err(to_storage_err)?,
        })
    }

    fn begin_write(&self) -> Result<Self::WriteTxn<'_>, StorageError> {
        let mut tx = self.db.begin_write().map_err(to_storage_err)?;
        tx.set_durability(Durability::Immediate)
            .map_err(to_storage_err)?;
        if self.two_phase_commit {
            tx.set_two_phase_commit(true);
        }
        Ok(RedbWriteTxn { tx })
    }
}

impl ReadTransaction for RedbReadTxn {
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
        let Some(data) = self.open_existing_table(table)? else {
            return Ok(None);
        };

        data.get(key)
            .map(|value| value.map(|bytes| bytes.value().to_vec()))
            .map_err(to_storage_err)
    }

    fn range<'txn>(
        &'txn self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RangeIter<'txn>, StorageError> {
        if !end.is_empty() && start >= end {
            return Ok(Box::new(std::iter::empty()));
        }

        let Some(data) = self.open_existing_table(table)? else {
            return Ok(Box::new(std::iter::empty()));
        };

        let rows = if end.is_empty() {
            data.range(start..)
                .map_err(to_storage_err)?
                .map(|item| {
                    item.map(|(key, value)| (key.value().to_vec(), value.value().to_vec()))
                        .map_err(to_storage_err)
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            data.range(start..end)
                .map_err(to_storage_err)?
                .map(|item| {
                    item.map(|(key, value)| (key.value().to_vec(), value.value().to_vec()))
                        .map_err(to_storage_err)
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        Ok(Box::new(rows.into_iter().map(Ok)))
    }
}

impl ReadTransaction for RedbWriteTxn {
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
        if !self.table_exists(table)? {
            return Ok(None);
        }

        let data = self
            .tx
            .open_table(table_def(table)?)
            .map_err(to_storage_err)?;

        data.get(key)
            .map(|value| value.map(|bytes| bytes.value().to_vec()))
            .map_err(to_storage_err)
    }

    fn range<'txn>(
        &'txn self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RangeIter<'txn>, StorageError> {
        if (!end.is_empty() && start >= end) || !self.table_exists(table)? {
            return Ok(Box::new(std::iter::empty()));
        }

        let data = self
            .tx
            .open_table(table_def(table)?)
            .map_err(to_storage_err)?;
        let rows = if end.is_empty() {
            data.range(start..)
                .map_err(to_storage_err)?
                .map(|item| {
                    item.map(|(key, value)| (key.value().to_vec(), value.value().to_vec()))
                        .map_err(to_storage_err)
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            data.range(start..end)
                .map_err(to_storage_err)?
                .map(|item| {
                    item.map(|(key, value)| (key.value().to_vec(), value.value().to_vec()))
                        .map_err(to_storage_err)
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        Ok(Box::new(rows.into_iter().map(Ok)))
    }
}

impl WriteTransaction for RedbWriteTxn {
    fn put(&mut self, table: &str, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        let mut data = self
            .tx
            .open_table(table_def(table)?)
            .map_err(to_storage_err)?;
        data.insert(key, value).map_err(to_storage_err)?;
        Ok(())
    }

    fn delete(&mut self, table: &str, key: &[u8]) -> Result<(), StorageError> {
        if !self.table_exists(table)? {
            return Ok(());
        }

        let mut data = self
            .tx
            .open_table(table_def(table)?)
            .map_err(to_storage_err)?;
        data.remove(key).map_err(to_storage_err)?;
        Ok(())
    }

    fn commit(self) -> Result<(), StorageError> {
        self.tx.commit().map_err(to_storage_err)
    }

    fn rollback(self) {}
}

impl RedbReadTxn {
    fn open_existing_table(&self, table: &str) -> Result<Option<RedbReadOnlyTable>, StorageError> {
        match self.tx.open_table(table_def(table)?) {
            Ok(data) => Ok(Some(data)),
            Err(TableError::TableDoesNotExist(_)) => Ok(None),
            Err(error) => Err(to_storage_err(error)),
        }
    }
}

impl RedbWriteTxn {
    fn table_exists(&self, table: &str) -> Result<bool, StorageError> {
        let tables = self.tx.list_tables().map_err(to_storage_err)?;
        Ok(tables.into_iter().any(|handle| handle.name() == table))
    }
}

fn table_def(table: &str) -> Result<RawTableDefinition<'_>, StorageError> {
    if table.is_empty() {
        return Err(StorageError::Backend(
            "table name cannot be empty".to_owned(),
        ));
    }

    Ok(TableDefinition::new(table))
}

fn to_storage_err(error: impl std::fmt::Display) -> StorageError {
    StorageError::Backend(error.to_string())
}
