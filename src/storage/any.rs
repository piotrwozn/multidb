use std::path::Path;

use crate::performance::CompressionConfig;

use super::compressed::{CompressedEngine, CompressedReadTxn, CompressedWriteTxn};
use super::encrypted::{
    ConfiguredKeyProvider, EncryptedEngine, EncryptedReadTxn, EncryptedWriteTxn,
};
use super::memory::{MemEngine, MemReadTxn, MemWriteTxn};
use super::redb::{RedbEngine, RedbReadTxn, RedbWriteTxn};
use super::{RangeIter, ReadTransaction, StorageEngine, StorageError, WriteTransaction};

pub enum AnyEngine {
    Memory(MemEngine),
    Redb(RedbEngine),
    EncryptedMemory(EncryptedEngine<MemEngine, ConfiguredKeyProvider>),
    EncryptedRedb(EncryptedEngine<RedbEngine, ConfiguredKeyProvider>),
    CompressedMemory(CompressedEngine<MemEngine>),
    CompressedRedb(CompressedEngine<RedbEngine>),
    CompressedEncryptedMemory(CompressedEngine<EncryptedEngine<MemEngine, ConfiguredKeyProvider>>),
    CompressedEncryptedRedb(CompressedEngine<EncryptedEngine<RedbEngine, ConfiguredKeyProvider>>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EngineKind {
    Memory,
    Redb,
    EncryptedMemory,
    EncryptedRedb,
    CompressedMemory,
    CompressedRedb,
    CompressedEncryptedMemory,
    CompressedEncryptedRedb,
    Sharded,
}

pub enum AnyReadTxn {
    Memory(MemReadTxn),
    Redb(RedbReadTxn),
    EncryptedMemory(EncryptedReadTxn<MemReadTxn, ConfiguredKeyProvider>),
    EncryptedRedb(EncryptedReadTxn<RedbReadTxn, ConfiguredKeyProvider>),
    CompressedMemory(CompressedReadTxn<MemReadTxn>),
    CompressedRedb(CompressedReadTxn<RedbReadTxn>),
    CompressedEncryptedMemory(
        CompressedReadTxn<EncryptedReadTxn<MemReadTxn, ConfiguredKeyProvider>>,
    ),
    CompressedEncryptedRedb(
        CompressedReadTxn<EncryptedReadTxn<RedbReadTxn, ConfiguredKeyProvider>>,
    ),
}

pub enum AnyWriteTxn<'engine> {
    Memory(MemWriteTxn<'engine>),
    Redb(Box<RedbWriteTxn>),
    EncryptedMemory(EncryptedWriteTxn<MemWriteTxn<'engine>, ConfiguredKeyProvider>),
    EncryptedRedb(Box<EncryptedWriteTxn<RedbWriteTxn, ConfiguredKeyProvider>>),
    CompressedMemory(CompressedWriteTxn<MemWriteTxn<'engine>>),
    CompressedRedb(Box<CompressedWriteTxn<RedbWriteTxn>>),
    CompressedEncryptedMemory(
        CompressedWriteTxn<EncryptedWriteTxn<MemWriteTxn<'engine>, ConfiguredKeyProvider>>,
    ),
    CompressedEncryptedRedb(
        Box<CompressedWriteTxn<EncryptedWriteTxn<RedbWriteTxn, ConfiguredKeyProvider>>>,
    ),
}

impl AnyEngine {
    #[must_use]
    pub fn memory() -> Self {
        Self::Memory(MemEngine::new())
    }

    /// Opens a redb-backed engine.
    /// # Errors
    /// Fails if redb cannot open or create the database file.
    pub fn redb(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Ok(Self::Redb(RedbEngine::open(path)?))
    }

    /// Opens a redb-backed engine with high-durability commit settings.
    /// # Errors
    /// Fails if redb cannot open or create the database file.
    pub fn redb_high_durability(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Ok(Self::Redb(RedbEngine::open_high_durability(path)?))
    }

    /// Opens an encrypted in-memory engine.
    /// # Errors
    /// Fails if the key provider cannot load a valid key.
    pub fn encrypted_memory(key_path: impl Into<std::path::PathBuf>) -> Result<Self, StorageError> {
        let engine =
            EncryptedEngine::new(MemEngine::new(), ConfiguredKeyProvider::file_key(key_path));
        let _ = engine.begin_read()?;
        Ok(Self::EncryptedMemory(engine))
    }

    /// Opens an encrypted in-memory engine using a local envelope keyring.
    /// # Errors
    /// Fails if the keyring or KEK cannot be loaded.
    pub fn encrypted_memory_envelope(
        keyring_path: impl Into<std::path::PathBuf>,
        kek_path: impl Into<std::path::PathBuf>,
    ) -> Result<Self, StorageError> {
        let engine = EncryptedEngine::new(
            MemEngine::new(),
            ConfiguredKeyProvider::local_envelope(keyring_path, kek_path)?,
        );
        let _ = engine.begin_read()?;
        Ok(Self::EncryptedMemory(engine))
    }

    /// Opens an encrypted redb-backed engine.
    /// # Errors
    /// Fails if redb cannot open the file or the key provider is invalid.
    pub fn encrypted_redb(
        path: impl AsRef<Path>,
        key_path: impl Into<std::path::PathBuf>,
    ) -> Result<Self, StorageError> {
        let engine = EncryptedEngine::new(
            RedbEngine::open(path)?,
            ConfiguredKeyProvider::file_key(key_path),
        );
        let _ = engine.begin_read()?;
        Ok(Self::EncryptedRedb(engine))
    }

    /// Opens an encrypted redb-backed engine using a local envelope keyring.
    /// # Errors
    /// Fails if redb, the keyring, or the KEK cannot be opened.
    pub fn encrypted_redb_envelope(
        path: impl AsRef<Path>,
        keyring_path: impl Into<std::path::PathBuf>,
        kek_path: impl Into<std::path::PathBuf>,
    ) -> Result<Self, StorageError> {
        let engine = EncryptedEngine::new(
            RedbEngine::open(path)?,
            ConfiguredKeyProvider::local_envelope(keyring_path, kek_path)?,
        );
        let _ = engine.begin_read()?;
        Ok(Self::EncryptedRedb(engine))
    }

    /// Opens an encrypted high-durability redb-backed engine.
    /// # Errors
    /// Fails if redb cannot open the file or the key provider is invalid.
    pub fn encrypted_redb_high_durability(
        path: impl AsRef<Path>,
        key_path: impl Into<std::path::PathBuf>,
    ) -> Result<Self, StorageError> {
        let engine = EncryptedEngine::new(
            RedbEngine::open_high_durability(path)?,
            ConfiguredKeyProvider::file_key(key_path),
        );
        let _ = engine.begin_read()?;
        Ok(Self::EncryptedRedb(engine))
    }

    /// Opens an encrypted high-durability redb-backed engine using a local envelope keyring.
    /// # Errors
    /// Fails if redb, the keyring, or the KEK cannot be opened.
    pub fn encrypted_redb_high_durability_envelope(
        path: impl AsRef<Path>,
        keyring_path: impl Into<std::path::PathBuf>,
        kek_path: impl Into<std::path::PathBuf>,
    ) -> Result<Self, StorageError> {
        let engine = EncryptedEngine::new(
            RedbEngine::open_high_durability(path)?,
            ConfiguredKeyProvider::local_envelope(keyring_path, kek_path)?,
        );
        let _ = engine.begin_read()?;
        Ok(Self::EncryptedRedb(engine))
    }

    #[must_use]
    pub fn compressed_memory(config: CompressionConfig) -> Self {
        Self::CompressedMemory(CompressedEngine::new(MemEngine::new(), config))
    }

    /// Opens a compressed redb-backed engine.
    /// # Errors
    /// Fails if redb cannot open or create the database file.
    pub fn compressed_redb(
        path: impl AsRef<Path>,
        config: CompressionConfig,
    ) -> Result<Self, StorageError> {
        Ok(Self::CompressedRedb(CompressedEngine::new(
            RedbEngine::open(path)?,
            config,
        )))
    }

    /// Opens a compressed high-durability redb-backed engine.
    /// # Errors
    /// Fails if redb cannot open or create the database file.
    pub fn compressed_redb_high_durability(
        path: impl AsRef<Path>,
        config: CompressionConfig,
    ) -> Result<Self, StorageError> {
        Ok(Self::CompressedRedb(CompressedEngine::new(
            RedbEngine::open_high_durability(path)?,
            config,
        )))
    }

    /// Opens a compressed and encrypted in-memory engine.
    /// # Errors
    /// Fails if the key provider cannot load a valid key.
    pub fn compressed_encrypted_memory(
        key_path: impl Into<std::path::PathBuf>,
        config: CompressionConfig,
    ) -> Result<Self, StorageError> {
        let encrypted =
            EncryptedEngine::new(MemEngine::new(), ConfiguredKeyProvider::file_key(key_path));
        let engine = CompressedEngine::new(encrypted, config);
        let _ = engine.begin_read()?;
        Ok(Self::CompressedEncryptedMemory(engine))
    }

    /// Opens a compressed and encrypted in-memory engine using a local envelope keyring.
    /// # Errors
    /// Fails if the keyring or KEK cannot be loaded.
    pub fn compressed_encrypted_memory_envelope(
        keyring_path: impl Into<std::path::PathBuf>,
        kek_path: impl Into<std::path::PathBuf>,
        config: CompressionConfig,
    ) -> Result<Self, StorageError> {
        let encrypted = EncryptedEngine::new(
            MemEngine::new(),
            ConfiguredKeyProvider::local_envelope(keyring_path, kek_path)?,
        );
        let engine = CompressedEngine::new(encrypted, config);
        let _ = engine.begin_read()?;
        Ok(Self::CompressedEncryptedMemory(engine))
    }

    /// Opens a compressed and encrypted redb-backed engine.
    /// # Errors
    /// Fails if redb cannot open the file or the key provider is invalid.
    pub fn compressed_encrypted_redb(
        path: impl AsRef<Path>,
        key_path: impl Into<std::path::PathBuf>,
        config: CompressionConfig,
    ) -> Result<Self, StorageError> {
        let encrypted = EncryptedEngine::new(
            RedbEngine::open(path)?,
            ConfiguredKeyProvider::file_key(key_path),
        );
        let engine = CompressedEngine::new(encrypted, config);
        let _ = engine.begin_read()?;
        Ok(Self::CompressedEncryptedRedb(engine))
    }

    /// Opens a compressed and encrypted redb-backed engine using a local envelope keyring.
    /// # Errors
    /// Fails if redb, the keyring, or the KEK cannot be opened.
    pub fn compressed_encrypted_redb_envelope(
        path: impl AsRef<Path>,
        keyring_path: impl Into<std::path::PathBuf>,
        kek_path: impl Into<std::path::PathBuf>,
        config: CompressionConfig,
    ) -> Result<Self, StorageError> {
        let encrypted = EncryptedEngine::new(
            RedbEngine::open(path)?,
            ConfiguredKeyProvider::local_envelope(keyring_path, kek_path)?,
        );
        let engine = CompressedEngine::new(encrypted, config);
        let _ = engine.begin_read()?;
        Ok(Self::CompressedEncryptedRedb(engine))
    }

    /// Opens a compressed and encrypted high-durability redb-backed engine.
    /// # Errors
    /// Fails if redb cannot open the file or the key provider is invalid.
    pub fn compressed_encrypted_redb_high_durability(
        path: impl AsRef<Path>,
        key_path: impl Into<std::path::PathBuf>,
        config: CompressionConfig,
    ) -> Result<Self, StorageError> {
        let encrypted = EncryptedEngine::new(
            RedbEngine::open_high_durability(path)?,
            ConfiguredKeyProvider::file_key(key_path),
        );
        let engine = CompressedEngine::new(encrypted, config);
        let _ = engine.begin_read()?;
        Ok(Self::CompressedEncryptedRedb(engine))
    }

    /// Opens a compressed and encrypted high-durability redb engine using an envelope keyring.
    /// # Errors
    /// Fails if redb, the keyring, or the KEK cannot be opened.
    pub fn compressed_encrypted_redb_high_durability_envelope(
        path: impl AsRef<Path>,
        keyring_path: impl Into<std::path::PathBuf>,
        kek_path: impl Into<std::path::PathBuf>,
        config: CompressionConfig,
    ) -> Result<Self, StorageError> {
        let encrypted = EncryptedEngine::new(
            RedbEngine::open_high_durability(path)?,
            ConfiguredKeyProvider::local_envelope(keyring_path, kek_path)?,
        );
        let engine = CompressedEngine::new(encrypted, config);
        let _ = engine.begin_read()?;
        Ok(Self::CompressedEncryptedRedb(engine))
    }

    #[must_use]
    pub const fn kind(&self) -> EngineKind {
        match self {
            Self::Memory(_) => EngineKind::Memory,
            Self::Redb(_) => EngineKind::Redb,
            Self::EncryptedMemory(_) => EngineKind::EncryptedMemory,
            Self::EncryptedRedb(_) => EngineKind::EncryptedRedb,
            Self::CompressedMemory(_) => EngineKind::CompressedMemory,
            Self::CompressedRedb(_) => EngineKind::CompressedRedb,
            Self::CompressedEncryptedMemory(_) => EngineKind::CompressedEncryptedMemory,
            Self::CompressedEncryptedRedb(_) => EngineKind::CompressedEncryptedRedb,
        }
    }

    /// Rotates the current data encryption key for encrypted engines.
    /// # Errors
    /// Fails when the engine is not encrypted or the provider cannot rotate.
    pub fn rotate_dek(&self) -> Result<u64, StorageError> {
        match self {
            Self::EncryptedMemory(engine) => engine.rotate_dek(),
            Self::EncryptedRedb(engine) => engine.rotate_dek(),
            Self::CompressedEncryptedMemory(engine) => engine.inner().rotate_dek(),
            Self::CompressedEncryptedRedb(engine) => engine.inner().rotate_dek(),
            _ => Err(StorageError::Backend(
                "storage engine is not encrypted".to_owned(),
            )),
        }
    }

    /// Destroys one data encryption key version for encrypted engines.
    /// # Errors
    /// Fails when the engine is not encrypted or the key id is unknown.
    pub fn destroy_dek(&self, key_id: u64) -> Result<(), StorageError> {
        match self {
            Self::EncryptedMemory(engine) => engine.destroy_dek(key_id),
            Self::EncryptedRedb(engine) => engine.destroy_dek(key_id),
            Self::CompressedEncryptedMemory(engine) => engine.inner().destroy_dek(key_id),
            Self::CompressedEncryptedRedb(engine) => engine.inner().destroy_dek(key_id),
            _ => Err(StorageError::Backend(
                "storage engine is not encrypted".to_owned(),
            )),
        }
    }

    /// Lists live data encryption key versions for encrypted engines.
    /// # Errors
    /// Fails when the engine is not encrypted or the provider cannot read its keyring.
    pub fn list_deks(&self) -> Result<Vec<u64>, StorageError> {
        match self {
            Self::EncryptedMemory(engine) => engine.list_deks(),
            Self::EncryptedRedb(engine) => engine.list_deks(),
            Self::CompressedEncryptedMemory(engine) => engine.inner().list_deks(),
            Self::CompressedEncryptedRedb(engine) => engine.inner().list_deks(),
            _ => Err(StorageError::Backend(
                "storage engine is not encrypted".to_owned(),
            )),
        }
    }
}

impl StorageEngine for AnyEngine {
    type ReadTxn<'a>
        = AnyReadTxn
    where
        Self: 'a;

    type WriteTxn<'a>
        = AnyWriteTxn<'a>
    where
        Self: 'a;

    fn begin_read(&self) -> Result<Self::ReadTxn<'_>, StorageError> {
        match self {
            Self::Memory(engine) => Ok(AnyReadTxn::Memory(engine.begin_read()?)),
            Self::Redb(engine) => Ok(AnyReadTxn::Redb(engine.begin_read()?)),
            Self::EncryptedMemory(engine) => Ok(AnyReadTxn::EncryptedMemory(engine.begin_read()?)),
            Self::EncryptedRedb(engine) => Ok(AnyReadTxn::EncryptedRedb(engine.begin_read()?)),
            Self::CompressedMemory(engine) => {
                Ok(AnyReadTxn::CompressedMemory(engine.begin_read()?))
            }
            Self::CompressedRedb(engine) => Ok(AnyReadTxn::CompressedRedb(engine.begin_read()?)),
            Self::CompressedEncryptedMemory(engine) => {
                Ok(AnyReadTxn::CompressedEncryptedMemory(engine.begin_read()?))
            }
            Self::CompressedEncryptedRedb(engine) => {
                Ok(AnyReadTxn::CompressedEncryptedRedb(engine.begin_read()?))
            }
        }
    }

    fn begin_write(&self) -> Result<Self::WriteTxn<'_>, StorageError> {
        match self {
            Self::Memory(engine) => Ok(AnyWriteTxn::Memory(engine.begin_write()?)),
            Self::Redb(engine) => Ok(AnyWriteTxn::Redb(Box::new(engine.begin_write()?))),
            Self::EncryptedMemory(engine) => {
                Ok(AnyWriteTxn::EncryptedMemory(engine.begin_write()?))
            }
            Self::EncryptedRedb(engine) => {
                Ok(AnyWriteTxn::EncryptedRedb(Box::new(engine.begin_write()?)))
            }
            Self::CompressedMemory(engine) => {
                Ok(AnyWriteTxn::CompressedMemory(engine.begin_write()?))
            }
            Self::CompressedRedb(engine) => {
                Ok(AnyWriteTxn::CompressedRedb(Box::new(engine.begin_write()?)))
            }
            Self::CompressedEncryptedMemory(engine) => Ok(AnyWriteTxn::CompressedEncryptedMemory(
                engine.begin_write()?,
            )),
            Self::CompressedEncryptedRedb(engine) => Ok(AnyWriteTxn::CompressedEncryptedRedb(
                Box::new(engine.begin_write()?),
            )),
        }
    }
}

impl ReadTransaction for AnyReadTxn {
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<crate::storage::Bytes>, StorageError> {
        match self {
            Self::Memory(txn) => txn.get(table, key),
            Self::Redb(txn) => txn.get(table, key),
            Self::EncryptedMemory(txn) => txn.get(table, key),
            Self::EncryptedRedb(txn) => txn.get(table, key),
            Self::CompressedMemory(txn) => txn.get(table, key),
            Self::CompressedRedb(txn) => txn.get(table, key),
            Self::CompressedEncryptedMemory(txn) => txn.get(table, key),
            Self::CompressedEncryptedRedb(txn) => txn.get(table, key),
        }
    }

    fn range<'txn>(
        &'txn self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RangeIter<'txn>, StorageError> {
        match self {
            Self::Memory(txn) => txn.range(table, start, end),
            Self::Redb(txn) => txn.range(table, start, end),
            Self::EncryptedMemory(txn) => txn.range(table, start, end),
            Self::EncryptedRedb(txn) => txn.range(table, start, end),
            Self::CompressedMemory(txn) => txn.range(table, start, end),
            Self::CompressedRedb(txn) => txn.range(table, start, end),
            Self::CompressedEncryptedMemory(txn) => txn.range(table, start, end),
            Self::CompressedEncryptedRedb(txn) => txn.range(table, start, end),
        }
    }
}

impl ReadTransaction for AnyWriteTxn<'_> {
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<crate::storage::Bytes>, StorageError> {
        match self {
            Self::Memory(txn) => txn.get(table, key),
            Self::Redb(txn) => txn.get(table, key),
            Self::EncryptedMemory(txn) => txn.get(table, key),
            Self::EncryptedRedb(txn) => txn.get(table, key),
            Self::CompressedMemory(txn) => txn.get(table, key),
            Self::CompressedRedb(txn) => txn.get(table, key),
            Self::CompressedEncryptedMemory(txn) => txn.get(table, key),
            Self::CompressedEncryptedRedb(txn) => txn.get(table, key),
        }
    }

    fn range<'txn>(
        &'txn self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RangeIter<'txn>, StorageError> {
        match self {
            Self::Memory(txn) => txn.range(table, start, end),
            Self::Redb(txn) => txn.range(table, start, end),
            Self::EncryptedMemory(txn) => txn.range(table, start, end),
            Self::EncryptedRedb(txn) => txn.range(table, start, end),
            Self::CompressedMemory(txn) => txn.range(table, start, end),
            Self::CompressedRedb(txn) => txn.range(table, start, end),
            Self::CompressedEncryptedMemory(txn) => txn.range(table, start, end),
            Self::CompressedEncryptedRedb(txn) => txn.range(table, start, end),
        }
    }
}

impl WriteTransaction for AnyWriteTxn<'_> {
    fn put(&mut self, table: &str, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        match self {
            Self::Memory(txn) => txn.put(table, key, value),
            Self::Redb(txn) => txn.put(table, key, value),
            Self::EncryptedMemory(txn) => txn.put(table, key, value),
            Self::EncryptedRedb(txn) => txn.put(table, key, value),
            Self::CompressedMemory(txn) => txn.put(table, key, value),
            Self::CompressedRedb(txn) => txn.put(table, key, value),
            Self::CompressedEncryptedMemory(txn) => txn.put(table, key, value),
            Self::CompressedEncryptedRedb(txn) => txn.put(table, key, value),
        }
    }

    fn delete(&mut self, table: &str, key: &[u8]) -> Result<(), StorageError> {
        match self {
            Self::Memory(txn) => txn.delete(table, key),
            Self::Redb(txn) => txn.delete(table, key),
            Self::EncryptedMemory(txn) => txn.delete(table, key),
            Self::EncryptedRedb(txn) => txn.delete(table, key),
            Self::CompressedMemory(txn) => txn.delete(table, key),
            Self::CompressedRedb(txn) => txn.delete(table, key),
            Self::CompressedEncryptedMemory(txn) => txn.delete(table, key),
            Self::CompressedEncryptedRedb(txn) => txn.delete(table, key),
        }
    }

    fn commit(self) -> Result<(), StorageError> {
        match self {
            Self::Memory(txn) => txn.commit(),
            Self::Redb(txn) => (*txn).commit(),
            Self::EncryptedMemory(txn) => txn.commit(),
            Self::EncryptedRedb(txn) => (*txn).commit(),
            Self::CompressedMemory(txn) => txn.commit(),
            Self::CompressedRedb(txn) => (*txn).commit(),
            Self::CompressedEncryptedMemory(txn) => txn.commit(),
            Self::CompressedEncryptedRedb(txn) => (*txn).commit(),
        }
    }

    fn rollback(self) {
        match self {
            Self::Memory(txn) => txn.rollback(),
            Self::Redb(txn) => (*txn).rollback(),
            Self::EncryptedMemory(txn) => txn.rollback(),
            Self::EncryptedRedb(txn) => (*txn).rollback(),
            Self::CompressedMemory(txn) => txn.rollback(),
            Self::CompressedRedb(txn) => (*txn).rollback(),
            Self::CompressedEncryptedMemory(txn) => txn.rollback(),
            Self::CompressedEncryptedRedb(txn) => (*txn).rollback(),
        }
    }
}
