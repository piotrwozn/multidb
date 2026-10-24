use std::path::Path;

use crate::performance::CompressionConfig;

use super::compressed::{CompressedEngine, CompressedReadTxn, CompressedWriteTxn};
use super::encrypted::{
    ConfiguredKeyProvider, EncryptedEngine, EncryptedReadTxn, EncryptedWriteTxn, ReencryptionReport,
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
    ///
    /// A keyring is created only for an empty database file.
    /// # Errors
    /// Fails if redb, the keyring, or the KEK cannot be opened, or the keyring
    /// is missing for a database that already contains tables.
    pub fn encrypted_redb_envelope(
        path: impl AsRef<Path>,
        keyring_path: impl Into<std::path::PathBuf>,
        kek_path: impl Into<std::path::PathBuf>,
    ) -> Result<Self, StorageError> {
        let inner = RedbEngine::open(path)?;
        let provider = local_envelope_for(&inner, keyring_path, kek_path)?;
        let engine = EncryptedEngine::new(inner, provider);
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
    ///
    /// A keyring is created only for an empty database file.
    /// # Errors
    /// Fails if redb, the keyring, or the KEK cannot be opened, or the keyring
    /// is missing for a database that already contains tables.
    pub fn encrypted_redb_high_durability_envelope(
        path: impl AsRef<Path>,
        keyring_path: impl Into<std::path::PathBuf>,
        kek_path: impl Into<std::path::PathBuf>,
    ) -> Result<Self, StorageError> {
        let inner = RedbEngine::open_high_durability(path)?;
        let provider = local_envelope_for(&inner, keyring_path, kek_path)?;
        let engine = EncryptedEngine::new(inner, provider);
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
    ///
    /// A keyring is created only for an empty database file.
    /// # Errors
    /// Fails if redb, the keyring, or the KEK cannot be opened, or the keyring
    /// is missing for a database that already contains tables.
    pub fn compressed_encrypted_redb_envelope(
        path: impl AsRef<Path>,
        keyring_path: impl Into<std::path::PathBuf>,
        kek_path: impl Into<std::path::PathBuf>,
        config: CompressionConfig,
    ) -> Result<Self, StorageError> {
        let inner = RedbEngine::open(path)?;
        let provider = local_envelope_for(&inner, keyring_path, kek_path)?;
        let encrypted = EncryptedEngine::new(inner, provider);
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
    ///
    /// A keyring is created only for an empty database file.
    /// # Errors
    /// Fails if redb, the keyring, or the KEK cannot be opened, or the keyring
    /// is missing for a database that already contains tables.
    pub fn compressed_encrypted_redb_high_durability_envelope(
        path: impl AsRef<Path>,
        keyring_path: impl Into<std::path::PathBuf>,
        kek_path: impl Into<std::path::PathBuf>,
        config: CompressionConfig,
    ) -> Result<Self, StorageError> {
        let inner = RedbEngine::open_high_durability(path)?;
        let provider = local_envelope_for(&inner, keyring_path, kek_path)?;
        let encrypted = EncryptedEngine::new(inner, provider);
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

    /// Reports whether every value this engine writes carries a compression frame.
    #[must_use]
    pub const fn is_value_framed(&self) -> bool {
        matches!(
            self,
            Self::CompressedMemory(_)
                | Self::CompressedRedb(_)
                | Self::CompressedEncryptedMemory(_)
                | Self::CompressedEncryptedRedb(_)
        )
    }

    /// Wraps this engine so every value it writes is compression-framed with `config`.
    ///
    /// The underlying handle is moved, never dropped and reopened. An already framed engine
    /// keeps its inner engine and takes the new `config`; frames are self-describing, so values
    /// written with another algorithm stay readable.
    #[must_use]
    pub fn with_value_framing(self, config: CompressionConfig) -> Self {
        match self {
            Self::Memory(engine) => Self::CompressedMemory(CompressedEngine::new(engine, config)),
            Self::Redb(engine) => Self::CompressedRedb(CompressedEngine::new(engine, config)),
            Self::EncryptedMemory(engine) => {
                Self::CompressedEncryptedMemory(CompressedEngine::new(engine, config))
            }
            Self::EncryptedRedb(engine) => {
                Self::CompressedEncryptedRedb(CompressedEngine::new(engine, config))
            }
            Self::CompressedMemory(engine) => {
                Self::CompressedMemory(CompressedEngine::new(engine.into_inner(), config))
            }
            Self::CompressedRedb(engine) => {
                Self::CompressedRedb(CompressedEngine::new(engine.into_inner(), config))
            }
            Self::CompressedEncryptedMemory(engine) => {
                Self::CompressedEncryptedMemory(CompressedEngine::new(engine.into_inner(), config))
            }
            Self::CompressedEncryptedRedb(engine) => {
                Self::CompressedEncryptedRedb(CompressedEngine::new(engine.into_inner(), config))
            }
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
    ///
    /// Refuses the current key, the last live key, and any key still protecting
    /// stored values; see [`EncryptedEngine::destroy_dek`].
    /// # Errors
    /// Fails when the engine is not encrypted, the key id is unknown, or the
    /// key is current, last, or still in use.
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

    /// Re-encrypts stored values under the current data encryption key in
    /// bounded batches; see [`EncryptedEngine::reencrypt_existing_values`].
    /// # Errors
    /// Fails when the engine is not encrypted, `batch_size` is zero, or a batch
    /// cannot be read, decrypted, or committed.
    pub fn reencrypt_existing_values(
        &self,
        batch_size: usize,
    ) -> Result<ReencryptionReport, StorageError> {
        match self {
            Self::EncryptedMemory(engine) => engine.reencrypt_existing_values(batch_size),
            Self::EncryptedRedb(engine) => engine.reencrypt_existing_values(batch_size),
            Self::CompressedEncryptedMemory(engine) => {
                engine.inner().reencrypt_existing_values(batch_size)
            }
            Self::CompressedEncryptedRedb(engine) => {
                engine.inner().reencrypt_existing_values(batch_size)
            }
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

/// Opens the envelope keyring for a redb file, creating one only when the file
/// holds no tables yet so existing encrypted data never gets a fresh keyring.
fn local_envelope_for(
    inner: &RedbEngine,
    keyring_path: impl Into<std::path::PathBuf>,
    kek_path: impl Into<std::path::PathBuf>,
) -> Result<ConfiguredKeyProvider, StorageError> {
    if inner.has_tables()? {
        ConfiguredKeyProvider::open_local_envelope(keyring_path, kek_path)
    } else {
        ConfiguredKeyProvider::local_envelope(keyring_path, kek_path)
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
