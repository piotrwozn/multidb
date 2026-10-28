use std::{collections::BTreeSet, path::Path};

use redb::{
    Database, DatabaseError, Durability, ReadableDatabase, ReadableTable, TableDefinition,
    TableError, TableHandle,
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

/// A redb file opened read-only. It never creates, repairs or writes the file; it only reads.
pub(crate) struct RedbReadOnly {
    db: redb::ReadOnlyDatabase,
}

/// The outcome of opening an existing redb file read-only.
pub(crate) enum ExistingRedbOpen {
    Opened(RedbReadOnly),
    /// Another handle holds the file open for writing.
    Locked,
    /// The file was not closed cleanly. Only a read-write open, which runs redb's crash repair
    /// and so writes the file (`RedbEngine::recover_existing`), can read it.
    NeedsRepair,
    Failed(StorageError),
}

pub struct RedbReadTxn {
    tx: redb::ReadTransaction,
}

pub struct RedbWriteTxn {
    tx: redb::WriteTransaction,
    // The writer owns an isolated snapshot and is the only way to create tables
    // in it. Keep its catalog local instead of enumerating redb's catalog for
    // every row/version/prepared-lock lookup.
    tables: BTreeSet<String>,
}

impl RedbEngine {
    /// Opens a redb database.
    /// # Errors
    /// Fails if redb cannot create or open the file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        create_private_file(path.as_ref())?;
        Ok(Self {
            db: Database::create(path).map_err(to_storage_err)?,
            two_phase_commit: false,
        })
    }

    /// Opens a redb database with two-phase commit enabled for every write transaction.
    /// # Errors
    /// Fails if redb cannot create or open the file.
    pub fn open_high_durability(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        create_private_file(path.as_ref())?;
        Ok(Self {
            db: Database::create(path).map_err(to_storage_err)?,
            two_phase_commit: true,
        })
    }

    /// Reports whether the database already contains any table.
    /// # Errors
    /// Fails if redb cannot open a read snapshot or list its tables.
    pub(crate) fn has_tables(&self) -> Result<bool, StorageError> {
        let read = self.db.begin_read().map_err(to_storage_err)?;
        Ok(read.list_tables().map_err(to_storage_err)?.next().is_some())
    }

    /// Opens an existing redb file read-write without ever creating one, which runs redb's crash
    /// recovery when the file was not closed cleanly, and closes it again. The recovery restores
    /// the last committed state, as every read-write open does first; nothing else is written.
    /// # Errors
    /// Fails if the file is missing, another handle holds it open, or redb cannot recover it.
    pub(crate) fn recover_existing(path: impl AsRef<Path>) -> Result<(), StorageError> {
        drop(Database::open(path).map_err(to_storage_err)?);
        Ok(())
    }
}

/// New database files can hold credentials and plaintext user data. Protect them
/// from creation, independently of the caller's umask; leave existing modes alone.
fn create_private_file(path: &Path) -> Result<(), StorageError> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(file) => {
            file.sync_all()?;
            crate::fileio::sync_parent(path)?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error.into()),
    }
}

impl RedbReadOnly {
    /// Opens an existing redb file read-only (`redb::ReadOnlyDatabase`): the file is opened
    /// without write access and with a shared lock, never created, and never repaired, so a
    /// file that needs crash recovery is `NeedsRepair`.
    pub(crate) fn open(path: impl AsRef<Path>) -> ExistingRedbOpen {
        match redb::ReadOnlyDatabase::open(path) {
            Ok(db) => ExistingRedbOpen::Opened(Self { db }),
            Err(DatabaseError::DatabaseAlreadyOpen) => ExistingRedbOpen::Locked,
            Err(DatabaseError::RepairAborted) => ExistingRedbOpen::NeedsRepair,
            // `DatabaseError` is non_exhaustive: every other failure is reported as is.
            Err(error) => ExistingRedbOpen::Failed(to_storage_err(error)),
        }
    }

    /// Begins a read transaction on the file.
    /// # Errors
    /// Fails if redb cannot start the read transaction.
    pub(crate) fn begin_read(&self) -> Result<RedbReadTxn, StorageError> {
        Ok(RedbReadTxn {
            tx: self.db.begin_read().map_err(to_storage_err)?,
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
        let tables = tx
            .list_tables()
            .map_err(to_storage_err)?
            .map(|handle| handle.name().to_owned())
            .collect();
        Ok(RedbWriteTxn { tx, tables })
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
        if !self.table_exists(table) {
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
        if (!end.is_empty() && start >= end) || !self.table_exists(table) {
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
        // open_table may have created an empty table even if insert fails.
        // Existing tables need neither another catalog walk nor a name clone.
        if !self.tables.contains(table) {
            self.tables.insert(table.to_owned());
        }
        data.insert(key, value).map_err(to_storage_err)?;
        Ok(())
    }

    fn delete(&mut self, table: &str, key: &[u8]) -> Result<(), StorageError> {
        if !self.table_exists(table) {
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
    fn table_exists(&self, table: &str) -> bool {
        self.tables.contains(table)
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

#[cfg(test)]
mod catalog_tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn absent_write_snapshot_reads_and_deletes_do_not_create_tables() -> TestResult {
        let directory = tempfile::tempdir()?;
        let engine = RedbEngine::open(directory.path().join("catalog.redb"))?;
        let mut write = engine.begin_write()?;
        assert_eq!(write.get("missing", b"key")?, None);
        assert_eq!(write.range("missing", &[], &[])?.count(), 0);
        write.delete("missing", b"key")?;
        write.commit()?;
        assert_eq!(engine.db.begin_read()?.list_tables()?.count(), 0);
        Ok(())
    }

    #[test]
    fn staged_table_creation_is_visible_to_get_range_and_delete() -> TestResult {
        let directory = tempfile::tempdir()?;
        let engine = RedbEngine::open(directory.path().join("catalog.redb"))?;
        let mut write = engine.begin_write()?;
        assert_eq!(write.get("items", b"a")?, None);
        write.put("items", b"a", b"first")?;
        write.put("items", b"b", b"second")?;
        assert_eq!(write.get("items", b"a")?, Some(b"first".to_vec()));
        assert_eq!(
            write
                .range("items", &[], &[])?
                .collect::<Result<Vec<_>, _>>()?,
            vec![
                (b"a".to_vec(), b"first".to_vec()),
                (b"b".to_vec(), b"second".to_vec())
            ]
        );
        write.delete("items", b"a")?;
        assert_eq!(write.get("items", b"a")?, None);
        assert_eq!(
            write
                .range("items", &[], &[])?
                .collect::<Result<Vec<_>, _>>()?,
            vec![(b"b".to_vec(), b"second".to_vec())]
        );
        write.commit()?;
        assert_eq!(
            engine.begin_read()?.get("items", b"b")?,
            Some(b"second".to_vec())
        );
        Ok(())
    }

    #[test]
    fn catalog_is_fresh_after_rollback_and_each_committed_transaction() -> TestResult {
        let directory = tempfile::tempdir()?;
        let engine = RedbEngine::open(directory.path().join("catalog.redb"))?;
        let mut seed = engine.begin_write()?;
        seed.put("existing", b"key", b"original")?;
        seed.commit()?;
        let old_read = engine.begin_read()?;

        let mut aborted = engine.begin_write()?;
        assert_eq!(aborted.get("existing", b"key")?, Some(b"original".to_vec()));
        aborted.put("rolled_back", b"key", b"discarded")?;
        aborted.delete("existing", b"key")?;
        aborted.rollback();

        let mut committed = engine.begin_write()?;
        assert_eq!(committed.get("rolled_back", b"key")?, None);
        assert_eq!(
            committed.get("existing", b"key")?,
            Some(b"original".to_vec())
        );
        committed.put("created", b"key", b"committed")?;
        committed.commit()?;

        let current = engine.begin_write()?;
        assert_eq!(current.get("rolled_back", b"key")?, None);
        assert_eq!(current.get("created", b"key")?, Some(b"committed".to_vec()));
        assert_eq!(current.get("existing", b"key")?, Some(b"original".to_vec()));
        current.rollback();
        assert_eq!(old_read.get("created", b"key")?, None);
        assert_eq!(
            old_read.get("existing", b"key")?,
            Some(b"original".to_vec())
        );
        Ok(())
    }
}
