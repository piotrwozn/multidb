use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use chacha20poly1305::{
    ChaCha20Poly1305, Nonce, XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit, Payload},
};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::{
    fileio,
    storage::{Bytes, RangeIter, ReadTransaction, StorageEngine, StorageError, WriteTransaction},
};

const MAGIC_V1: &[u8; 5] = b"MDBE1";
const MAGIC_V2: &[u8; 5] = b"MDBE2";
const MAGIC_V3: &[u8; 5] = b"MDBE3";
const NONCE_LEN_V1_V2: usize = 12;
const NONCE_LEN_V3: usize = 24;
const KEY_LEN: usize = 32;
const LEGACY_KEY_ID: u64 = 1;
const ALG_XCHACHA20_POLY1305: u8 = 1;
const ENCRYPTION_VERSIONS_TABLE: &str = "__enc_versions";
const KEYRING_FORMAT_VERSION: u32 = 1;

pub trait KeyProvider: Send + Sync + Clone + 'static {
    /// Returns the current data encryption key.
    /// # Errors
    /// Fails when the key cannot be loaded or is malformed.
    fn current_key(&self) -> Result<ProtectedKey, StorageError>;

    /// Returns a historical data encryption key by id.
    /// # Errors
    /// Fails when the key id is unknown or has been destroyed.
    fn key_by_id(&self, key_id: u64) -> Result<ProtectedKey, StorageError> {
        let key = self.current_key()?;
        if key.id() == key_id {
            Ok(key)
        } else {
            Err(StorageError::Corruption(format!(
                "unknown encryption key id {key_id}"
            )))
        }
    }

    /// Rotates the current data encryption key.
    /// # Errors
    /// Fails for providers that do not support envelope keyrings.
    fn rotate_dek(&self) -> Result<u64, StorageError> {
        Err(StorageError::Backend(
            "encryption key provider does not support DEK rotation".to_owned(),
        ))
    }

    /// Destroys one data encryption key version.
    /// # Errors
    /// Fails for providers that do not support envelope keyrings.
    fn destroy_dek(&self, _key_id: u64) -> Result<(), StorageError> {
        Err(StorageError::Backend(
            "encryption key provider does not support crypto-shred".to_owned(),
        ))
    }

    /// Lists non-destroyed data encryption key versions.
    /// # Errors
    /// Fails when the provider cannot read its keyring.
    fn list_deks(&self) -> Result<Vec<u64>, StorageError> {
        Ok(vec![self.current_key()?.id()])
    }
}

pub trait KekProvider: Send + Sync + Clone + 'static {
    /// Returns the key-encryption key used to wrap DEKs.
    /// # Errors
    /// Fails when the KEK cannot be loaded or is malformed.
    fn kek(&self) -> Result<ProtectedKey, StorageError>;
}

pub struct ProtectedKey {
    id: u64,
    bytes: Zeroizing<[u8; KEY_LEN]>,
}

#[derive(Clone)]
pub struct StaticKeyProvider {
    key: ProtectedKey,
}

#[derive(Clone, Debug)]
pub struct FileKeyProvider {
    path: Arc<PathBuf>,
    cache: Arc<Mutex<Option<ProtectedKey>>>,
}

#[derive(Clone, Debug)]
pub struct LocalFileKms {
    provider: FileKeyProvider,
}

#[derive(Clone, Debug)]
pub struct VaultKekProvider {
    address: Arc<String>,
    token: Arc<String>,
    secret_path: Arc<String>,
    key_field: Arc<String>,
    timeout_ms: u64,
}

#[derive(Clone)]
pub struct EnvelopeKeyProvider<K> {
    keyring_path: Arc<PathBuf>,
    kek_provider: K,
    keyring: Arc<RwLock<EnvelopeKeyring>>,
    cache: Arc<Mutex<BTreeMap<u64, ProtectedKey>>>,
}

#[derive(Clone)]
pub enum ConfiguredKeyProvider {
    File(FileKeyProvider),
    Envelope(EnvelopeKeyProvider<LocalFileKms>),
    VaultEnvelope(EnvelopeKeyProvider<VaultKekProvider>),
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct KeyRotationPlan {
    pub current_key_id: u64,
    pub live_key_ids: Vec<u64>,
    pub reencrypt_existing_values: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct CryptoShredReport {
    pub destroyed_key_id: u64,
    pub remaining_key_ids: Vec<u64>,
}

pub struct EncryptedEngine<S, K> {
    inner: S,
    key_provider: Arc<K>,
}

pub struct EncryptedReadTxn<T, K> {
    inner: T,
    key_provider: Arc<K>,
}

pub struct EncryptedWriteTxn<T, K> {
    inner: T,
    key_provider: Arc<K>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct EnvelopeKeyring {
    format_version: u32,
    current_key_id: u64,
    keys: Vec<WrappedDataKey>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct WrappedDataKey {
    key_id: u64,
    algorithm: String,
    nonce: Bytes,
    ciphertext: Bytes,
    deleted: bool,
}

impl Clone for ProtectedKey {
    fn clone(&self) -> Self {
        Self::with_id(self.id, *self.bytes)
    }
}

impl std::fmt::Debug for ProtectedKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProtectedKey")
            .field("id", &self.id)
            .field("bytes", &"<redacted>")
            .finish()
    }
}

impl ProtectedKey {
    #[must_use]
    pub fn new(bytes: [u8; KEY_LEN]) -> Self {
        Self::with_id(LEGACY_KEY_ID, bytes)
    }

    #[must_use]
    pub fn with_id(id: u64, bytes: [u8; KEY_LEN]) -> Self {
        Self {
            id,
            bytes: Zeroizing::new(bytes),
        }
    }

    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }

    fn as_slice(&self) -> &[u8] {
        self.bytes.as_ref()
    }

    pub(crate) fn as_slice_for_backup(&self) -> &[u8] {
        self.as_slice()
    }
}

impl StaticKeyProvider {
    #[must_use]
    pub fn new(key: [u8; KEY_LEN]) -> Self {
        Self {
            key: ProtectedKey::new(key),
        }
    }

    #[must_use]
    pub fn with_id(key_id: u64, key: [u8; KEY_LEN]) -> Self {
        Self {
            key: ProtectedKey::with_id(key_id, key),
        }
    }
}

impl KeyProvider for StaticKeyProvider {
    fn current_key(&self) -> Result<ProtectedKey, StorageError> {
        Ok(self.key.clone())
    }
}

impl FileKeyProvider {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: Arc::new(path.into()),
            cache: Arc::new(Mutex::new(None)),
        }
    }

    fn load(&self) -> Result<ProtectedKey, StorageError> {
        let bytes = Zeroizing::new(std::fs::read(self.path.as_ref())?);
        parse_key_file(bytes.as_slice()).map(ProtectedKey::new)
    }
}

impl KeyProvider for FileKeyProvider {
    fn current_key(&self) -> Result<ProtectedKey, StorageError> {
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| StorageError::Backend("key cache lock poisoned".to_owned()))?;
        if let Some(key) = cache.as_ref() {
            return Ok(key.clone());
        }

        let key = self.load()?;
        *cache = Some(key.clone());
        Ok(key)
    }
}

impl LocalFileKms {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            provider: FileKeyProvider::new(path),
        }
    }
}

impl KekProvider for LocalFileKms {
    fn kek(&self) -> Result<ProtectedKey, StorageError> {
        self.provider.current_key()
    }
}

impl VaultKekProvider {
    #[must_use]
    pub fn new(
        address: impl Into<String>,
        token: impl Into<String>,
        secret_path: impl Into<String>,
    ) -> Self {
        Self {
            address: Arc::new(address.into()),
            token: Arc::new(token.into()),
            secret_path: Arc::new(secret_path.into()),
            key_field: Arc::new("key".to_owned()),
            timeout_ms: 2_000,
        }
    }

    /// Builds a Vault provider from `VAULT_ADDR`, `VAULT_TOKEN`, and
    /// `MULTIDB_VAULT_KEK_PATH`.
    /// # Errors
    /// Fails when any required environment variable is absent.
    pub fn from_env() -> Result<Self, StorageError> {
        let address = std::env::var("VAULT_ADDR").map_err(|_| {
            StorageError::Backend("VAULT_ADDR is required for VaultKekProvider".to_owned())
        })?;
        let token = std::env::var("VAULT_TOKEN").map_err(|_| {
            StorageError::Backend("VAULT_TOKEN is required for VaultKekProvider".to_owned())
        })?;
        let secret_path = std::env::var("MULTIDB_VAULT_KEK_PATH").map_err(|_| {
            StorageError::Backend(
                "MULTIDB_VAULT_KEK_PATH is required for VaultKekProvider".to_owned(),
            )
        })?;
        Ok(Self::new(address, token, secret_path))
    }

    #[must_use]
    pub fn with_key_field(mut self, key_field: impl Into<String>) -> Self {
        self.key_field = Arc::new(key_field.into());
        self
    }

    #[must_use]
    pub const fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
}

impl KekProvider for VaultKekProvider {
    fn kek(&self) -> Result<ProtectedKey, StorageError> {
        read_vault_kek(self)
    }
}

impl<K> EnvelopeKeyProvider<K>
where
    K: KekProvider,
{
    /// Opens an existing keyring or creates one with an initial DEK.
    /// # Errors
    /// Fails when the keyring or KEK cannot be read, written, or authenticated.
    pub fn open_or_create(
        keyring_path: impl Into<PathBuf>,
        kek_provider: K,
    ) -> Result<Self, StorageError> {
        let keyring_path = Arc::new(keyring_path.into());
        let keyring = if keyring_path.exists() {
            read_keyring(keyring_path.as_ref())?
        } else {
            let key = ProtectedKey::with_id(LEGACY_KEY_ID, random_key()?);
            let wrapped = wrap_dek(&kek_provider.kek()?, &key)?;
            let keyring = EnvelopeKeyring {
                format_version: KEYRING_FORMAT_VERSION,
                current_key_id: key.id(),
                keys: vec![wrapped],
            };
            write_keyring(keyring_path.as_ref(), &keyring)?;
            keyring
        };

        let provider = Self {
            keyring_path,
            kek_provider,
            keyring: Arc::new(RwLock::new(keyring)),
            cache: Arc::new(Mutex::new(BTreeMap::new())),
        };
        let current = provider.current_key()?;
        provider.cache_key(current)?;
        Ok(provider)
    }

    /// Rewraps every live DEK under a new KEK provider.
    /// # Errors
    /// Fails when any key cannot be unwrapped, wrapped, or persisted.
    pub fn rotate_kek_with<N>(&self, new_kek_provider: &N) -> Result<(), StorageError>
    where
        N: KekProvider,
    {
        let mut keyring = self
            .keyring
            .write()
            .map_err(|_| StorageError::Backend("keyring lock poisoned".to_owned()))?;
        let old_kek = self.kek_provider.kek()?;
        let new_kek = new_kek_provider.kek()?;
        for wrapped in keyring.keys.iter_mut().filter(|key| !key.deleted) {
            let key = unwrap_dek(&old_kek, wrapped)?;
            *wrapped = wrap_dek(&new_kek, &key)?;
        }
        write_keyring(self.keyring_path.as_ref(), &keyring)
    }

    fn cache_key(&self, key: ProtectedKey) -> Result<(), StorageError> {
        self.cache
            .lock()
            .map_err(|_| StorageError::Backend("key cache lock poisoned".to_owned()))?
            .insert(key.id(), key);
        Ok(())
    }
}

impl<K> KeyProvider for EnvelopeKeyProvider<K>
where
    K: KekProvider,
{
    fn current_key(&self) -> Result<ProtectedKey, StorageError> {
        let current_key_id = self
            .keyring
            .read()
            .map_err(|_| StorageError::Backend("keyring lock poisoned".to_owned()))?
            .current_key_id;
        self.key_by_id(current_key_id)
    }

    fn key_by_id(&self, key_id: u64) -> Result<ProtectedKey, StorageError> {
        if let Some(key) = self
            .cache
            .lock()
            .map_err(|_| StorageError::Backend("key cache lock poisoned".to_owned()))?
            .get(&key_id)
            .cloned()
        {
            return Ok(key);
        }

        let wrapped = self
            .keyring
            .read()
            .map_err(|_| StorageError::Backend("keyring lock poisoned".to_owned()))?
            .keys
            .iter()
            .find(|key| key.key_id == key_id && !key.deleted)
            .cloned()
            .ok_or_else(|| {
                StorageError::Corruption(format!("unknown encryption key id {key_id}"))
            })?;
        let key = unwrap_dek(&self.kek_provider.kek()?, &wrapped)?;
        self.cache_key(key.clone())?;
        Ok(key)
    }

    fn rotate_dek(&self) -> Result<u64, StorageError> {
        let mut keyring = self
            .keyring
            .write()
            .map_err(|_| StorageError::Backend("keyring lock poisoned".to_owned()))?;
        let next_id = keyring
            .keys
            .iter()
            .map(|key| key.key_id)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| StorageError::Backend("encryption key id overflow".to_owned()))?;
        let key = ProtectedKey::with_id(next_id, random_key()?);
        let wrapped = wrap_dek(&self.kek_provider.kek()?, &key)?;
        keyring.current_key_id = next_id;
        keyring.keys.push(wrapped);
        write_keyring(self.keyring_path.as_ref(), &keyring)?;
        drop(keyring);
        self.cache_key(key)?;
        Ok(next_id)
    }

    fn destroy_dek(&self, key_id: u64) -> Result<(), StorageError> {
        let mut keyring = self
            .keyring
            .write()
            .map_err(|_| StorageError::Backend("keyring lock poisoned".to_owned()))?;
        let mut found = false;
        for key in &mut keyring.keys {
            if key.key_id == key_id {
                key.deleted = true;
                key.ciphertext.clear();
                key.nonce.clear();
                found = true;
            }
        }
        if !found {
            return Err(StorageError::Corruption(format!(
                "unknown encryption key id {key_id}"
            )));
        }
        if keyring.current_key_id == key_id {
            keyring.current_key_id = keyring
                .keys
                .iter()
                .filter(|key| !key.deleted)
                .map(|key| key.key_id)
                .max()
                .unwrap_or(0);
        }
        write_keyring(self.keyring_path.as_ref(), &keyring)?;
        self.cache
            .lock()
            .map_err(|_| StorageError::Backend("key cache lock poisoned".to_owned()))?
            .remove(&key_id);
        Ok(())
    }

    fn list_deks(&self) -> Result<Vec<u64>, StorageError> {
        Ok(self
            .keyring
            .read()
            .map_err(|_| StorageError::Backend("keyring lock poisoned".to_owned()))?
            .keys
            .iter()
            .filter(|key| !key.deleted)
            .map(|key| key.key_id)
            .collect())
    }
}

impl ConfiguredKeyProvider {
    #[must_use]
    pub fn file_key(path: impl Into<PathBuf>) -> Self {
        Self::File(FileKeyProvider::new(path))
    }

    /// Opens a local envelope keyring protected by a file-backed KEK.
    /// # Errors
    /// Fails when the keyring or KEK cannot be opened.
    pub fn local_envelope(
        keyring_path: impl Into<PathBuf>,
        kek_path: impl Into<PathBuf>,
    ) -> Result<Self, StorageError> {
        Ok(Self::Envelope(EnvelopeKeyProvider::open_or_create(
            keyring_path,
            LocalFileKms::new(kek_path),
        )?))
    }

    /// Opens a local envelope keyring protected by a Vault KV v2 KEK.
    /// # Errors
    /// Fails when the keyring or Vault KEK cannot be opened.
    pub fn vault_envelope(
        keyring_path: impl Into<PathBuf>,
        address: impl Into<String>,
        token: impl Into<String>,
        secret_path: impl Into<String>,
    ) -> Result<Self, StorageError> {
        Ok(Self::VaultEnvelope(EnvelopeKeyProvider::open_or_create(
            keyring_path,
            VaultKekProvider::new(address, token, secret_path),
        )?))
    }
}

impl KeyProvider for ConfiguredKeyProvider {
    fn current_key(&self) -> Result<ProtectedKey, StorageError> {
        match self {
            Self::File(provider) => provider.current_key(),
            Self::Envelope(provider) => provider.current_key(),
            Self::VaultEnvelope(provider) => provider.current_key(),
        }
    }

    fn key_by_id(&self, key_id: u64) -> Result<ProtectedKey, StorageError> {
        match self {
            Self::File(provider) => provider.key_by_id(key_id),
            Self::Envelope(provider) => provider.key_by_id(key_id),
            Self::VaultEnvelope(provider) => provider.key_by_id(key_id),
        }
    }

    fn rotate_dek(&self) -> Result<u64, StorageError> {
        match self {
            Self::File(provider) => provider.rotate_dek(),
            Self::Envelope(provider) => provider.rotate_dek(),
            Self::VaultEnvelope(provider) => provider.rotate_dek(),
        }
    }

    fn destroy_dek(&self, key_id: u64) -> Result<(), StorageError> {
        match self {
            Self::File(provider) => provider.destroy_dek(key_id),
            Self::Envelope(provider) => provider.destroy_dek(key_id),
            Self::VaultEnvelope(provider) => provider.destroy_dek(key_id),
        }
    }

    fn list_deks(&self) -> Result<Vec<u64>, StorageError> {
        match self {
            Self::File(provider) => provider.list_deks(),
            Self::Envelope(provider) => provider.list_deks(),
            Self::VaultEnvelope(provider) => provider.list_deks(),
        }
    }
}

impl<S, K> EncryptedEngine<S, K> {
    #[must_use]
    pub fn new(inner: S, key_provider: K) -> Self {
        Self {
            inner,
            key_provider: Arc::new(key_provider),
        }
    }

    #[must_use]
    pub const fn inner(&self) -> &S {
        &self.inner
    }

    #[must_use]
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S, K> EncryptedEngine<S, K>
where
    K: KeyProvider,
{
    /// Describes the next DEK rotation without mutating key material.
    /// # Errors
    /// Fails when the provider cannot expose current or live key ids.
    pub fn key_rotation_plan(&self) -> Result<KeyRotationPlan, StorageError> {
        Ok(KeyRotationPlan {
            current_key_id: self.key_provider.current_key()?.id(),
            live_key_ids: self.key_provider.list_deks()?,
            reencrypt_existing_values: false,
        })
    }

    /// Rotates the current data encryption key when the provider supports it.
    /// # Errors
    /// Fails when the keyring cannot rotate or persist the new key.
    pub fn rotate_dek(&self) -> Result<u64, StorageError> {
        self.key_provider.rotate_dek()
    }

    /// Destroys one data encryption key version.
    /// # Errors
    /// Fails when the provider does not support crypto-shred or the id is unknown.
    pub fn destroy_dek(&self, key_id: u64) -> Result<(), StorageError> {
        self.key_provider.destroy_dek(key_id)
    }

    /// Destroys one DEK and reports the remaining live key ids.
    /// # Errors
    /// Fails when the provider does not support crypto-shred or the id is unknown.
    pub fn crypto_shred(&self, key_id: u64) -> Result<CryptoShredReport, StorageError> {
        self.destroy_dek(key_id)?;
        Ok(CryptoShredReport {
            destroyed_key_id: key_id,
            remaining_key_ids: self.list_deks()?,
        })
    }

    /// Lists live data encryption key versions.
    /// # Errors
    /// Fails when the provider cannot read its keyring.
    pub fn list_deks(&self) -> Result<Vec<u64>, StorageError> {
        self.key_provider.list_deks()
    }
}

impl<S, K> StorageEngine for EncryptedEngine<S, K>
where
    S: StorageEngine,
    K: KeyProvider,
{
    type ReadTxn<'a>
        = EncryptedReadTxn<S::ReadTxn<'a>, K>
    where
        Self: 'a;

    type WriteTxn<'a>
        = EncryptedWriteTxn<S::WriteTxn<'a>, K>
    where
        Self: 'a;

    fn begin_read(&self) -> Result<Self::ReadTxn<'_>, StorageError> {
        let _ = self.key_provider.current_key()?;
        Ok(EncryptedReadTxn {
            inner: self.inner.begin_read()?,
            key_provider: Arc::clone(&self.key_provider),
        })
    }

    fn begin_write(&self) -> Result<Self::WriteTxn<'_>, StorageError> {
        let _ = self.key_provider.current_key()?;
        Ok(EncryptedWriteTxn {
            inner: self.inner.begin_write()?,
            key_provider: Arc::clone(&self.key_provider),
        })
    }
}

impl<T, K> ReadTransaction for EncryptedReadTxn<T, K>
where
    T: ReadTransaction,
    K: KeyProvider,
{
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
        self.inner
            .get(table, key)?
            .map(|bytes| {
                let version = encryption_version_from(&self.inner, table, key)?;
                decrypt_value(self.key_provider.as_ref(), table, key, version, &bytes)
            })
            .transpose()
    }

    fn range<'txn>(
        &'txn self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RangeIter<'txn>, StorageError> {
        let rows = self
            .inner
            .range(table, start, end)?
            .map(|item| {
                let (key, value) = item?;
                let version = encryption_version_from(&self.inner, table, &key)?;
                let value =
                    decrypt_value(self.key_provider.as_ref(), table, &key, version, &value)?;
                Ok((key, value))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        Ok(Box::new(rows.into_iter().map(Ok)))
    }
}

impl<T, K> ReadTransaction for EncryptedWriteTxn<T, K>
where
    T: WriteTransaction,
    K: KeyProvider,
{
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
        self.inner
            .get(table, key)?
            .map(|bytes| {
                let version = encryption_version_from(&self.inner, table, key)?;
                decrypt_value(self.key_provider.as_ref(), table, key, version, &bytes)
            })
            .transpose()
    }

    fn range<'txn>(
        &'txn self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RangeIter<'txn>, StorageError> {
        let rows = self
            .inner
            .range(table, start, end)?
            .map(|item| {
                let (key, value) = item?;
                let version = encryption_version_from(&self.inner, table, &key)?;
                let value =
                    decrypt_value(self.key_provider.as_ref(), table, &key, version, &value)?;
                Ok((key, value))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        Ok(Box::new(rows.into_iter().map(Ok)))
    }
}

impl<T, K> WriteTransaction for EncryptedWriteTxn<T, K>
where
    T: WriteTransaction,
    K: KeyProvider,
{
    fn put(&mut self, table: &str, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        if table == ENCRYPTION_VERSIONS_TABLE {
            return self.inner.put(table, key, value);
        }

        let version = next_encryption_version(&self.inner, table, key)?;
        let encrypted = encrypt_value(
            &self.key_provider.current_key()?,
            table,
            key,
            version,
            value,
        )?;
        self.inner.put(
            ENCRYPTION_VERSIONS_TABLE,
            &encryption_version_key(table, key)?,
            &version.to_be_bytes(),
        )?;
        self.inner.put(table, key, &encrypted)
    }

    fn delete(&mut self, table: &str, key: &[u8]) -> Result<(), StorageError> {
        if table != ENCRYPTION_VERSIONS_TABLE {
            let version = next_encryption_version(&self.inner, table, key)?;
            self.inner.put(
                ENCRYPTION_VERSIONS_TABLE,
                &encryption_version_key(table, key)?,
                &version.to_be_bytes(),
            )?;
        }
        self.inner.delete(table, key)
    }

    fn commit(self) -> Result<(), StorageError> {
        self.inner.commit()
    }

    fn rollback(self) {
        self.inner.rollback();
    }
}

fn encrypt_value(
    key: &ProtectedKey,
    table: &str,
    row_key: &[u8],
    version: u64,
    value: &[u8],
) -> Result<Bytes, StorageError> {
    let cipher = XChaCha20Poly1305::new_from_slice(key.as_slice())
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    let nonce_bytes = random_nonce_v3()?;
    let aad = aad_for_v3(table, row_key, key.id(), version);
    let nonce = XNonce::try_from(nonce_bytes.as_slice())
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    let ciphertext = cipher
        .encrypt(
            &nonce,
            Payload {
                msg: value,
                aad: &aad,
            },
        )
        .map_err(|error| StorageError::Backend(error.to_string()))?;

    let mut encoded =
        Vec::with_capacity(MAGIC_V3.len() + 1 + 8 + 8 + NONCE_LEN_V3 + ciphertext.len());
    encoded.extend_from_slice(MAGIC_V3);
    encoded.push(ALG_XCHACHA20_POLY1305);
    encoded.extend_from_slice(&key.id().to_be_bytes());
    encoded.extend_from_slice(&version.to_be_bytes());
    encoded.extend_from_slice(&nonce_bytes);
    encoded.extend_from_slice(&ciphertext);
    Ok(encoded)
}

fn decrypt_value<K>(
    key_provider: &K,
    table: &str,
    row_key: &[u8],
    expected_version: Option<u64>,
    value: &[u8],
) -> Result<Bytes, StorageError>
where
    K: KeyProvider,
{
    if value.len() < MAGIC_V3.len() + NONCE_LEN_V1_V2 {
        return Err(StorageError::Corruption(
            "encrypted value has invalid header".to_owned(),
        ));
    }

    let magic = &value[..MAGIC_V3.len()];
    match magic {
        header if header == MAGIC_V3 => {
            decrypt_value_v3(key_provider, table, row_key, expected_version, value)
        }
        header if header == MAGIC_V2 || header == MAGIC_V1 => {
            decrypt_value_v1_v2(&key_provider.current_key()?, table, row_key, value)
        }
        _ => Err(StorageError::Corruption(
            "encrypted value has invalid header".to_owned(),
        )),
    }
}

fn decrypt_value_v3<K>(
    key_provider: &K,
    table: &str,
    row_key: &[u8],
    expected_version: Option<u64>,
    value: &[u8],
) -> Result<Bytes, StorageError>
where
    K: KeyProvider,
{
    let header_len = MAGIC_V3.len() + 1 + 8 + 8 + NONCE_LEN_V3;
    if value.len() < header_len {
        return Err(StorageError::Corruption(
            "encrypted v3 value has invalid header".to_owned(),
        ));
    }
    let algorithm = value[MAGIC_V3.len()];
    if algorithm != ALG_XCHACHA20_POLY1305 {
        return Err(StorageError::Corruption(format!(
            "unsupported encrypted value algorithm {algorithm}"
        )));
    }
    let key_id_start = MAGIC_V3.len() + 1;
    let version_start = key_id_start + 8;
    let nonce_start = version_start + 8;
    let nonce_end = nonce_start + NONCE_LEN_V3;
    let key_id = decode_u64(&value[key_id_start..version_start], "key id")?;
    let version = decode_u64(&value[version_start..nonce_start], "encryption version")?;
    if expected_version != Some(version) {
        return Err(StorageError::Corruption(
            "encrypted value rollback detected".to_owned(),
        ));
    }

    let nonce = XNonce::try_from(&value[nonce_start..nonce_end])
        .map_err(|error| StorageError::Corruption(error.to_string()))?;
    let ciphertext = &value[nonce_end..];
    let key = key_provider.key_by_id(key_id)?;
    let aad = aad_for_v3(table, row_key, key_id, version);
    XChaCha20Poly1305::new_from_slice(key.as_slice())
        .map_err(|error| StorageError::Backend(error.to_string()))?
        .decrypt(
            &nonce,
            Payload {
                msg: ciphertext,
                aad: &aad,
            },
        )
        .map_err(|_| StorageError::Corruption("encrypted value authentication failed".to_owned()))
}

fn decrypt_value_v1_v2(
    key: &ProtectedKey,
    table: &str,
    row_key: &[u8],
    value: &[u8],
) -> Result<Bytes, StorageError> {
    let nonce_start = MAGIC_V2.len();
    let nonce_end = nonce_start + NONCE_LEN_V1_V2;
    if value.len() < nonce_end {
        return Err(StorageError::Corruption(
            "encrypted value has invalid header".to_owned(),
        ));
    }
    let nonce = &value[nonce_start..nonce_end];
    let ciphertext = &value[nonce_end..];
    let aad = aad_for_v1_v2(table, row_key);
    let cipher = ChaCha20Poly1305::new_from_slice(key.as_slice())
        .map_err(|error| StorageError::Backend(error.to_string()))?;

    let nonce =
        Nonce::try_from(nonce).map_err(|error| StorageError::Corruption(error.to_string()))?;

    cipher
        .decrypt(
            &nonce,
            Payload {
                msg: ciphertext,
                aad: &aad,
            },
        )
        .map_err(|_| StorageError::Corruption("encrypted value authentication failed".to_owned()))
}

fn random_key() -> Result<[u8; KEY_LEN], StorageError> {
    let mut key = [0; KEY_LEN];
    getrandom::fill(&mut key)
        .map_err(|error| StorageError::Backend(format!("failed to generate key: {error}")))?;
    Ok(key)
}

fn random_nonce_v3() -> Result<[u8; NONCE_LEN_V3], StorageError> {
    let mut nonce = [0; NONCE_LEN_V3];
    getrandom::fill(&mut nonce)
        .map_err(|error| StorageError::Backend(format!("failed to generate nonce: {error}")))?;
    Ok(nonce)
}

fn next_encryption_version(
    txn: &impl ReadTransaction,
    table: &str,
    row_key: &[u8],
) -> Result<u64, StorageError> {
    encryption_version_from(txn, table, row_key)?
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| StorageError::Backend("encryption version overflow".to_owned()))
}

fn encryption_version_from(
    txn: &impl ReadTransaction,
    table: &str,
    row_key: &[u8],
) -> Result<Option<u64>, StorageError> {
    if table == ENCRYPTION_VERSIONS_TABLE {
        return Ok(None);
    }
    txn.get(
        ENCRYPTION_VERSIONS_TABLE,
        &encryption_version_key(table, row_key)?,
    )?
    .map(|bytes| decode_u64(&bytes, "encryption version"))
    .transpose()
}

fn encryption_version_key(table: &str, row_key: &[u8]) -> Result<Bytes, StorageError> {
    let table_len =
        u64::try_from(table.len()).map_err(|error| StorageError::Backend(error.to_string()))?;
    let mut key = Vec::with_capacity(8 + table.len() + row_key.len());
    key.extend_from_slice(&table_len.to_be_bytes());
    key.extend_from_slice(table.as_bytes());
    key.extend_from_slice(row_key);
    Ok(key)
}

fn decode_u64(bytes: &[u8], name: &str) -> Result<u64, StorageError> {
    let bytes = bytes
        .try_into()
        .map_err(|_| StorageError::Corruption(format!("{name} must be exactly eight bytes")))?;
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(test)]
fn legacy_v1_nonce_for(table: &str, row_key: &[u8], value: &[u8]) -> [u8; NONCE_LEN_V1_V2] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"multidb encrypted storage nonce v1");
    hasher.update(&aad_for_v1_v2(table, row_key));
    hasher.update(value);
    let hash = hasher.finalize();
    let mut nonce = [0; NONCE_LEN_V1_V2];
    nonce.copy_from_slice(&hash.as_bytes()[..NONCE_LEN_V1_V2]);
    nonce
}

#[cfg(test)]
fn encrypt_value_v1_for_tests(
    key: &ProtectedKey,
    table: &str,
    row_key: &[u8],
    value: &[u8],
) -> Result<Bytes, StorageError> {
    let cipher = ChaCha20Poly1305::new_from_slice(key.as_slice())
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    let nonce_bytes = legacy_v1_nonce_for(table, row_key, value);
    let nonce = Nonce::try_from(nonce_bytes.as_slice())
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    let aad = aad_for_v1_v2(table, row_key);
    let ciphertext = cipher
        .encrypt(
            &nonce,
            Payload {
                msg: value,
                aad: &aad,
            },
        )
        .map_err(|error| StorageError::Backend(error.to_string()))?;

    let mut encoded = Vec::with_capacity(MAGIC_V1.len() + NONCE_LEN_V1_V2 + ciphertext.len());
    encoded.extend_from_slice(MAGIC_V1);
    encoded.extend_from_slice(&nonce_bytes);
    encoded.extend_from_slice(&ciphertext);
    Ok(encoded)
}

fn aad_for_v1_v2(table: &str, row_key: &[u8]) -> Bytes {
    let table_bytes = table.as_bytes();
    let mut aad = Vec::with_capacity(8 + table_bytes.len() + row_key.len());
    aad.extend_from_slice(
        &u64::try_from(table_bytes.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    aad.extend_from_slice(table_bytes);
    aad.extend_from_slice(row_key);
    aad
}

fn aad_for_v3(table: &str, row_key: &[u8], key_id: u64, version: u64) -> Bytes {
    let table_bytes = table.as_bytes();
    let mut aad = Vec::with_capacity(33 + table_bytes.len() + row_key.len());
    aad.extend_from_slice(b"multidb encrypted storage v3");
    aad.push(ALG_XCHACHA20_POLY1305);
    aad.extend_from_slice(&key_id.to_be_bytes());
    aad.extend_from_slice(&version.to_be_bytes());
    aad.extend_from_slice(
        &u64::try_from(table_bytes.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    aad.extend_from_slice(table_bytes);
    aad.extend_from_slice(row_key);
    aad
}

fn parse_key_file(bytes: &[u8]) -> Result<[u8; KEY_LEN], StorageError> {
    if bytes.len() == KEY_LEN {
        let mut key = [0; KEY_LEN];
        key.copy_from_slice(bytes);
        return Ok(key);
    }

    let text = std::str::from_utf8(bytes)
        .map(str::trim)
        .map_err(|_| StorageError::Corruption("key file is not valid utf-8".to_owned()))?;
    if text.len() == KEY_LEN * 2 {
        return decode_hex_key(text);
    }

    Err(StorageError::Corruption(
        "key file must contain 32 raw bytes or 64 hex characters".to_owned(),
    ))
}

fn decode_hex_key(text: &str) -> Result<[u8; KEY_LEN], StorageError> {
    let mut key = [0; KEY_LEN];
    for (index, chunk) in text.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_value(chunk[0])?;
        let low = hex_value(chunk[1])?;
        key[index] = (high << 4) | low;
    }
    Ok(key)
}

fn hex_value(byte: u8) -> Result<u8, StorageError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(StorageError::Corruption(
            "key file contains invalid hex".to_owned(),
        )),
    }
}

fn read_vault_kek(provider: &VaultKekProvider) -> Result<ProtectedKey, StorageError> {
    let (host, port) = parse_vault_http_address(&provider.address)?;
    let path = provider.secret_path.as_str().trim_start_matches('/');
    let request = format!(
        "GET /v1/{path} HTTP/1.1\r\nHost: {host}\r\nX-Vault-Token: {}\r\nAccept: application/json\r\nConnection: close\r\n\r\n",
        provider.token.as_str()
    );
    let timeout = Duration::from_millis(provider.timeout_ms.max(1));
    let mut stream = TcpStream::connect((host.as_str(), port))?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    stream.write_all(request.as_bytes())?;

    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let (headers, body) = response.split_once("\r\n\r\n").ok_or_else(|| {
        StorageError::Corruption("Vault response did not contain HTTP headers".to_owned())
    })?;
    if !headers.starts_with("HTTP/1.1 2") && !headers.starts_with("HTTP/1.0 2") {
        let status = headers.lines().next().unwrap_or("HTTP status unavailable");
        return Err(StorageError::Backend(format!(
            "Vault KEK request failed: {status}"
        )));
    }

    let json: serde_json::Value =
        serde_json::from_str(body).map_err(|error| StorageError::Corruption(error.to_string()))?;
    let key_text = vault_key_value(&json, provider.key_field.as_str()).ok_or_else(|| {
        StorageError::Corruption(format!(
            "Vault response missing key field {}",
            provider.key_field
        ))
    })?;
    Ok(ProtectedKey::new(parse_key_file(key_text.as_bytes())?))
}

fn parse_vault_http_address(address: &str) -> Result<(String, u16), StorageError> {
    let Some(rest) = address.strip_prefix("http://") else {
        return Err(StorageError::Backend(
            "VaultKekProvider local dev mode supports http:// addresses only".to_owned(),
        ));
    };
    let authority = rest.split('/').next().unwrap_or(rest).trim();
    if authority.is_empty() {
        return Err(StorageError::Backend(
            "VaultKekProvider address is missing host".to_owned(),
        ));
    }

    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => (
            host.to_owned(),
            port.parse::<u16>()
                .map_err(|error| StorageError::Backend(error.to_string()))?,
        ),
        None => (authority.to_owned(), 80),
    };
    Ok((host, port))
}

fn vault_key_value<'a>(json: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    json.get("data")
        .and_then(|data| data.get("data"))
        .and_then(|data| data.get(field))
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            json.get("data")
                .and_then(|data| data.get(field))
                .and_then(serde_json::Value::as_str)
        })
}

fn read_keyring(path: &Path) -> Result<EnvelopeKeyring, StorageError> {
    let bytes = std::fs::read(path)?;
    let keyring: EnvelopeKeyring = serde_json::from_slice(&bytes)
        .map_err(|error| StorageError::Corruption(error.to_string()))?;
    if keyring.format_version != KEYRING_FORMAT_VERSION {
        return Err(StorageError::Corruption(format!(
            "unsupported keyring format {}",
            keyring.format_version
        )));
    }
    Ok(keyring)
}

fn write_keyring(path: &Path, keyring: &EnvelopeKeyring) -> Result<(), StorageError> {
    let bytes = serde_json::to_vec_pretty(keyring)
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    fileio::atomic_write(path, &bytes).map_err(Into::into)
}

fn wrap_dek(kek: &ProtectedKey, dek: &ProtectedKey) -> Result<WrappedDataKey, StorageError> {
    let nonce = random_nonce_v3()?;
    let aad = envelope_aad(dek.id());
    let ciphertext = XChaCha20Poly1305::new_from_slice(kek.as_slice())
        .map_err(|error| StorageError::Backend(error.to_string()))?
        .encrypt(
            &XNonce::try_from(nonce.as_slice())
                .map_err(|error| StorageError::Backend(error.to_string()))?,
            Payload {
                msg: dek.as_slice(),
                aad: &aad,
            },
        )
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    Ok(WrappedDataKey {
        key_id: dek.id(),
        algorithm: "XChaCha20Poly1305".to_owned(),
        nonce: nonce.to_vec(),
        ciphertext,
        deleted: false,
    })
}

fn unwrap_dek(kek: &ProtectedKey, wrapped: &WrappedDataKey) -> Result<ProtectedKey, StorageError> {
    if wrapped.deleted {
        return Err(StorageError::Corruption(format!(
            "encryption key id {} has been destroyed",
            wrapped.key_id
        )));
    }
    if wrapped.algorithm != "XChaCha20Poly1305" {
        return Err(StorageError::Corruption(format!(
            "unsupported wrapped DEK algorithm {}",
            wrapped.algorithm
        )));
    }
    let aad = envelope_aad(wrapped.key_id);
    let nonce = XNonce::try_from(wrapped.nonce.as_slice())
        .map_err(|error| StorageError::Corruption(error.to_string()))?;
    let plaintext = XChaCha20Poly1305::new_from_slice(kek.as_slice())
        .map_err(|error| StorageError::Backend(error.to_string()))?
        .decrypt(
            &nonce,
            Payload {
                msg: wrapped.ciphertext.as_slice(),
                aad: &aad,
            },
        )
        .map_err(|_| StorageError::Corruption("wrapped DEK authentication failed".to_owned()))?;
    let key = parse_raw_key(&plaintext)?;
    Ok(ProtectedKey::with_id(wrapped.key_id, key))
}

fn parse_raw_key(bytes: &[u8]) -> Result<[u8; KEY_LEN], StorageError> {
    if bytes.len() != KEY_LEN {
        return Err(StorageError::Corruption(
            "wrapped DEK must contain exactly 32 bytes".to_owned(),
        ));
    }
    let mut key = [0; KEY_LEN];
    key.copy_from_slice(bytes);
    Ok(key)
}

fn envelope_aad(key_id: u64) -> Bytes {
    let mut aad = Vec::with_capacity(32);
    aad.extend_from_slice(b"multidb envelope dek v1");
    aad.extend_from_slice(&key_id.to_be_bytes());
    aad
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
    };

    use tempfile::tempdir;

    use super::{
        ConfiguredKeyProvider, EncryptedEngine, EnvelopeKeyProvider, FileKeyProvider, KekProvider,
        KeyProvider, LocalFileKms, ProtectedKey, StaticKeyProvider, VaultKekProvider,
        encrypt_value_v1_for_tests,
    };
    use crate::storage::{MemEngine, ReadTransaction, RedbEngine, StorageEngine, WriteTransaction};

    #[test]
    fn encrypted_memory_round_trips_values() -> Result<(), Box<dyn std::error::Error>> {
        let engine = EncryptedEngine::new(MemEngine::new(), StaticKeyProvider::new([7; 32]));
        let mut write = engine.begin_write()?;
        write.put("t", b"k", b"secret-value")?;
        write.commit()?;

        let read = engine.begin_read()?;
        assert_eq!(read.get("t", b"k")?, Some(b"secret-value".to_vec()));
        Ok(())
    }

    #[test]
    fn encrypted_rewrites_use_random_v3_nonces() -> Result<(), Box<dyn std::error::Error>> {
        let engine = EncryptedEngine::new(MemEngine::new(), StaticKeyProvider::new([7; 32]));
        let mut write = engine.begin_write()?;
        write.put("t", b"k", b"secret-value")?;
        write.commit()?;
        let first = engine
            .inner()
            .begin_read()?
            .get("t", b"k")?
            .unwrap_or_default();

        let mut write = engine.begin_write()?;
        write.put("t", b"k", b"secret-value")?;
        write.commit()?;
        let second = engine
            .inner()
            .begin_read()?
            .get("t", b"k")?
            .unwrap_or_default();

        assert!(first.starts_with(super::MAGIC_V3));
        assert!(second.starts_with(super::MAGIC_V3));
        assert_ne!(first, second);
        Ok(())
    }

    #[test]
    fn old_ciphertext_replay_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let raw = MemEngine::new();
        let engine = EncryptedEngine::new(raw, StaticKeyProvider::new([7; 32]));
        let mut write = engine.begin_write()?;
        write.put("t", b"k", b"first-secret")?;
        write.commit()?;
        let old_ciphertext = engine
            .inner()
            .begin_read()?
            .get("t", b"k")?
            .unwrap_or_default();

        let mut write = engine.begin_write()?;
        write.put("t", b"k", b"second-secret")?;
        write.commit()?;

        let mut raw_write = engine.inner().begin_write()?;
        raw_write.put("t", b"k", &old_ciphertext)?;
        raw_write.commit()?;

        assert!(matches!(
            engine.begin_read()?.get("t", b"k"),
            Err(crate::storage::StorageError::Corruption(_))
        ));
        Ok(())
    }

    #[test]
    fn legacy_v1_ciphertext_still_reads() -> Result<(), Box<dyn std::error::Error>> {
        let raw = MemEngine::new();
        let key = [5; 32];
        let legacy =
            encrypt_value_v1_for_tests(&ProtectedKey::new(key), "t", b"k", b"legacy-secret-value")?;
        let mut write = raw.begin_write()?;
        write.put("t", b"k", &legacy)?;
        write.commit()?;

        let engine = EncryptedEngine::new(raw, StaticKeyProvider::new(key));
        assert_eq!(
            engine.begin_read()?.get("t", b"k")?,
            Some(b"legacy-secret-value".to_vec())
        );
        Ok(())
    }

    #[test]
    fn encrypted_redb_does_not_store_plaintext_values() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let path = dir.path().join("encrypted.redb");
        let engine =
            EncryptedEngine::new(RedbEngine::open(&path)?, StaticKeyProvider::new([9; 32]));
        let mut write = engine.begin_write()?;
        write.put("t", b"k", b"very-secret-value")?;
        write.commit()?;
        drop(engine);

        let raw = RedbEngine::open(&path)?;
        let read = raw.begin_read()?;
        let stored = read.get("t", b"k")?.unwrap_or_default();

        assert_ne!(stored, b"very-secret-value");
        assert!(
            !stored
                .windows(b"very-secret-value".len())
                .any(|window| window == b"very-secret-value")
        );
        Ok(())
    }

    #[test]
    fn wrong_key_returns_corruption() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let path = dir.path().join("wrong-key.redb");
        {
            let engine =
                EncryptedEngine::new(RedbEngine::open(&path)?, StaticKeyProvider::new([1; 32]));
            let mut write = engine.begin_write()?;
            write.put("t", b"k", b"secret")?;
            write.commit()?;
        }

        let engine =
            EncryptedEngine::new(RedbEngine::open(&path)?, StaticKeyProvider::new([2; 32]));
        let read = engine.begin_read()?;

        assert!(matches!(
            read.get("t", b"k"),
            Err(crate::storage::StorageError::Corruption(_))
        ));
        Ok(())
    }

    #[test]
    fn file_key_provider_accepts_hex_keys() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let path = dir.path().join("key.txt");
        std::fs::write(
            &path,
            "0101010101010101010101010101010101010101010101010101010101010101",
        )?;

        let engine = EncryptedEngine::new(MemEngine::new(), FileKeyProvider::new(path));
        let mut write = engine.begin_write()?;
        write.put("t", b"k", b"value")?;
        write.commit()?;

        assert_eq!(
            engine.begin_read()?.get("t", b"k")?,
            Some(b"value".to_vec())
        );
        Ok(())
    }

    #[test]
    fn envelope_key_provider_rotates_and_shreds_deks() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let kek_path = dir.path().join("kek.bin");
        let keyring_path = dir.path().join("keyring.json");
        std::fs::write(&kek_path, [3_u8; 32])?;
        let provider = ConfiguredKeyProvider::local_envelope(&keyring_path, &kek_path)?;
        assert_eq!(provider.list_deks()?, vec![1]);
        let second = provider.rotate_dek()?;
        assert_eq!(second, 2);
        assert_eq!(provider.list_deks()?, vec![1, 2]);
        provider.destroy_dek(1)?;
        assert_eq!(provider.list_deks()?, vec![2]);
        assert!(provider.key_by_id(1).is_err());
        Ok(())
    }

    #[test]
    fn encrypted_engine_reports_rotation_plan_and_crypto_shred()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let kek_path = dir.path().join("kek.bin");
        let keyring_path = dir.path().join("keyring.json");
        std::fs::write(&kek_path, [3_u8; 32])?;
        let provider = ConfiguredKeyProvider::local_envelope(&keyring_path, &kek_path)?;
        let engine = EncryptedEngine::new(MemEngine::new(), provider);

        let mut first = engine.begin_write()?;
        first.put("t", b"old", b"old-value")?;
        first.commit()?;

        let plan = engine.key_rotation_plan()?;
        assert_eq!(plan.current_key_id, 1);
        assert_eq!(plan.live_key_ids, vec![1]);
        assert!(!plan.reencrypt_existing_values);

        assert_eq!(engine.rotate_dek()?, 2);
        let mut second = engine.begin_write()?;
        second.put("t", b"new", b"new-value")?;
        second.commit()?;

        let report = engine.crypto_shred(1)?;
        assert_eq!(report.destroyed_key_id, 1);
        assert_eq!(report.remaining_key_ids, vec![2]);

        let read = engine.begin_read()?;
        assert!(read.get("t", b"old").is_err());
        assert_eq!(read.get("t", b"new")?, Some(b"new-value".to_vec()));
        Ok(())
    }

    #[test]
    fn vault_kek_provider_reads_kv_v2_secret() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let handle = thread::spawn(move || -> Result<(), String> {
            let (mut stream, _) = listener.accept().map_err(|error| error.to_string())?;
            let mut request = [0; 1024];
            let bytes = stream
                .read(&mut request)
                .map_err(|error| error.to_string())?;
            let request = String::from_utf8_lossy(&request[..bytes]);
            if !request.contains("X-Vault-Token: root") {
                return Err("missing Vault token header".to_owned());
            }
            if !request.starts_with("GET /v1/secret/data/multidb/kek ") {
                return Err("unexpected Vault path".to_owned());
            }

            let body = r#"{"data":{"data":{"key":"1111111111111111111111111111111111111111111111111111111111111111"}}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .map_err(|error| error.to_string())
        });

        let provider = VaultKekProvider::new(
            format!("http://{address}"),
            "root",
            "secret/data/multidb/kek",
        );
        let key = provider.kek()?;
        assert_eq!(key.id(), 1);
        assert_eq!(key.as_slice(), &[0x11; 32]);
        handle
            .join()
            .map_err(|_| "Vault fake server panicked")?
            .map_err(|error| format!("Vault fake server failed: {error}"))?;
        Ok(())
    }

    #[test]
    fn envelope_kek_rotation_rewraps_without_changing_dek_ids()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let old_kek_path = dir.path().join("old-kek.bin");
        let new_kek_path = dir.path().join("new-kek.bin");
        let keyring_path = dir.path().join("keyring.json");
        std::fs::write(&old_kek_path, [3_u8; 32])?;
        std::fs::write(&new_kek_path, [4_u8; 32])?;
        let provider =
            EnvelopeKeyProvider::open_or_create(&keyring_path, LocalFileKms::new(&old_kek_path))?;
        let before = provider.current_key()?.id();
        provider.rotate_kek_with(&LocalFileKms::new(&new_kek_path))?;

        let reopened =
            EnvelopeKeyProvider::open_or_create(&keyring_path, LocalFileKms::new(&new_kek_path))?;
        assert_eq!(reopened.current_key()?.id(), before);
        Ok(())
    }
}
