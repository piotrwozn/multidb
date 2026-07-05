use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    fs::OpenOptions,
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    backup::{
        BackupConfig, BackupError, BackupKind, BackupManifest, BackupReport, RestoreConfig,
        RestoreReport, RestoreTarget, VerifyReport, full_backup, incremental_backup,
        restore_backup, restore_backup_with_config, verify_backup, verify_backup_with_config,
    },
    db::{Database, DbConfig},
    fileio, observability,
    query::REL_COLUMNAR_SEGMENTS_TABLE,
    repl::{
        ConditionalBatch, Op, ReadConsistency, ReplError, Replication, propose_system,
        propose_system_batch,
    },
    storage::{Bytes, StorageError},
    txn,
};

pub const CLOUD_SEGMENTS_TABLE: &str = "__cloud_segments";
pub const CLOUD_HIBERNATION_TABLE: &str = "__cloud_hibernation";
pub const TIERED_SEGMENT_MAGIC: &[u8] = b"MULTIDB_TIERED_SEGMENT_V1\n";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectStoreConfig {
    pub uri: ObjectStoreUri,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudConfig {
    pub object_store: ObjectStoreConfig,
    pub tiering: TieringPolicy,
    pub hibernate: Option<HibernateConfig>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectStoreUri {
    raw: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SegmentLocation {
    Local { table: String, key: Bytes },
    Remote { uri: ObjectStoreUri, path: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TieringState {
    Local,
    Uploading,
    Remote,
    DeletePending,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentMetadata {
    pub table: String,
    pub key: Bytes,
    pub location: SegmentLocation,
    pub bytes: u64,
    pub checksum: u32,
    pub tiered_at: SystemTime,
    pub upload_token: String,
    #[serde(default = "default_tiering_state")]
    pub state: TieringState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TieredSegmentPointer {
    pub uri: ObjectStoreUri,
    pub path: String,
    pub bytes: u64,
    pub checksum: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TieringPolicy {
    pub older_than: Duration,
    pub max_local_bytes: u64,
    pub pin_local: bool,
    pub io_budget_bytes_per_second: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TieringReport {
    pub scanned_segments: usize,
    pub uploaded_segments: usize,
    pub recovered_segments: usize,
    pub failed_segments: usize,
    pub skipped_segments: usize,
    pub remote_bytes: u64,
    pub local_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantQuota {
    pub max_storage_bytes: u64,
    pub max_concurrent_queries: usize,
    #[serde(default = "default_max_concurrent_writes")]
    pub max_concurrent_writes: usize,
    pub max_memory_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantConfig {
    pub tenant_id: TenantId,
    pub quota: TenantQuota,
}

#[derive(Clone, Debug)]
pub struct TenantRuntime {
    config: TenantConfig,
    used_storage_bytes: Arc<Mutex<u64>>,
    active_queries: Arc<Mutex<usize>>,
    active_writes: Arc<Mutex<usize>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuotaReservation {
    tenant_id: TenantId,
    bytes: u64,
}

#[derive(Debug)]
pub struct TenantPermit {
    active: Arc<Mutex<usize>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HibernateConflictPolicy {
    Refuse,
    Wait,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HibernateConfig {
    pub object_store: ObjectStoreConfig,
    pub conflict_policy: HibernateConflictPolicy,
    pub wait_timeout: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HibernateReport {
    pub lease_id: String,
    pub backup_id: String,
    pub hibernated_at: SystemTime,
    pub hibernated_lsn: u64,
    pub fencing_token: String,
    pub object_prefix: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeReport {
    pub lease_id: String,
    pub restored_lsn: u64,
    pub time_to_ready_ms: u128,
    pub lazy_loaded_segments: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HibernateMarker {
    pub lease_id: String,
    pub backup_id: String,
    pub hibernated_at: SystemTime,
    pub hibernated_lsn: u64,
    pub tenant_id: Option<TenantId>,
    pub fencing_token: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudLease {
    uri: ObjectStoreUri,
    path: String,
    lease_id: String,
    owner: String,
    expires_at: SystemTime,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudLeaseOptions {
    pub ttl: Duration,
    pub owner: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CloudLeaseFile {
    lease_id: String,
    owner: String,
    expires_at: SystemTime,
}

#[derive(Debug)]
pub struct GuardedResumeSession {
    lease: CloudLease,
    report: ResumeReport,
    fencing_token: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupUri(pub ObjectStoreUri);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupGcPolicy {
    pub keep_last_full: usize,
    pub retain_newer_than: Option<Duration>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupGcReport {
    pub scanned_backups: usize,
    pub kept_backups: usize,
    pub deleted_backups: usize,
    pub deleted_objects: usize,
}

pub trait CloudObjectStore: Send + Sync {
    /// Writes an object atomically from the caller's perspective.
    /// # Errors
    /// Fails when the backing object store cannot persist the bytes.
    fn put(&self, path: &str, bytes: &[u8]) -> Result<(), CloudError>;

    /// Reads a full object payload.
    /// # Errors
    /// Fails when the object cannot be read.
    fn get(&self, path: &str) -> Result<Bytes, CloudError>;

    /// Deletes an object if it exists.
    /// # Errors
    /// Fails when the backing object store rejects the delete.
    fn delete(&self, path: &str) -> Result<(), CloudError>;

    /// Checks whether an object exists.
    /// # Errors
    /// Fails when the backing object store cannot inspect the object.
    fn exists(&self, path: &str) -> Result<bool, CloudError>;

    /// Lists object paths below a prefix.
    /// # Errors
    /// Fails when the backing object store cannot list the prefix.
    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, CloudError>;

    /// Acquires a cloud lease.
    /// # Errors
    /// Fails when a lease already exists or cannot be written.
    fn acquire_lease(&self, name: &str) -> Result<CloudLease, CloudError>;
}

#[derive(Clone, Debug)]
pub struct LocalObjectStore {
    uri: ObjectStoreUri,
}

#[cfg(feature = "cloud-object-store")]
#[derive(Clone, Debug)]
pub struct ObjectStoreProvider;

#[derive(thiserror::Error, Debug)]
pub enum CloudError {
    #[error("io: {0}")]
    Io(#[from] io::Error),

    #[error("url: {0}")]
    Url(String),

    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("backup: {0}")]
    Backup(#[from] BackupError),

    #[error("serialization: {0}")]
    Serde(String),

    #[error(
        "quota exceeded for tenant {tenant}: requested {requested} bytes, remaining {remaining} bytes"
    )]
    QuotaExceeded {
        tenant: String,
        requested: u64,
        remaining: u64,
    },

    #[error("tenant {tenant} exceeded concurrent {kind} limit {limit}")]
    TenantLimitExceeded {
        tenant: String,
        kind: &'static str,
        limit: usize,
    },

    #[error("cloud lease already exists: {0}")]
    LeaseExists(String),

    #[error("cloud lease owner mismatch: {0}")]
    LeaseOwnerMismatch(String),

    #[error("unsupported cloud operation: {0}")]
    Unsupported(String),
}

impl ObjectStoreConfig {
    #[must_use]
    pub fn local_dir(path: impl AsRef<Path>) -> Self {
        Self {
            uri: ObjectStoreUri::from_local_dir(path),
        }
    }
}

impl BackupUri {
    #[must_use]
    pub const fn new(uri: ObjectStoreUri) -> Self {
        Self(uri)
    }

    #[must_use]
    pub const fn as_object_store_uri(&self) -> &ObjectStoreUri {
        &self.0
    }
}

impl Default for BackupGcPolicy {
    fn default() -> Self {
        Self {
            keep_last_full: 1,
            retain_newer_than: None,
        }
    }
}

impl CloudConfig {
    #[must_use]
    pub fn new(object_store: ObjectStoreConfig) -> Self {
        Self {
            object_store,
            tiering: TieringPolicy::default(),
            hibernate: None,
        }
    }

    #[must_use]
    pub fn with_tiering(mut self, tiering: TieringPolicy) -> Self {
        self.tiering = tiering;
        self
    }

    #[must_use]
    pub fn with_hibernate(mut self, hibernate: HibernateConfig) -> Self {
        self.hibernate = Some(hibernate);
        self
    }
}

impl LocalObjectStore {
    #[must_use]
    pub const fn new(uri: ObjectStoreUri) -> Self {
        Self { uri }
    }

    #[must_use]
    pub const fn uri(&self) -> &ObjectStoreUri {
        &self.uri
    }

    fn full_path(&self, path: &str) -> Result<PathBuf, CloudError> {
        Ok(self.uri.local_root()?.join(path))
    }
}

impl CloudObjectStore for LocalObjectStore {
    fn put(&self, path: &str, bytes: &[u8]) -> Result<(), CloudError> {
        let _object_path = ObjectStoreUri::object_store_path(path);
        let full_path = self.full_path(path)?;
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fileio::atomic_write(full_path, bytes)?;
        observability::record_cloud_object("put", "ok", bytes.len() as u64);
        Ok(())
    }

    fn get(&self, path: &str) -> Result<Bytes, CloudError> {
        let _object_path = ObjectStoreUri::object_store_path(path);
        let bytes = fs::read(self.full_path(path)?)?;
        observability::record_cloud_object("get", "ok", bytes.len() as u64);
        Ok(bytes)
    }

    fn delete(&self, path: &str) -> Result<(), CloudError> {
        let full_path = self.full_path(path)?;
        if full_path.exists() {
            fs::remove_file(full_path)?;
        }
        observability::record_cloud_object("delete", "ok", 0);
        Ok(())
    }

    fn exists(&self, path: &str) -> Result<bool, CloudError> {
        Ok(self.full_path(path)?.exists())
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, CloudError> {
        let root = self.uri.local_root()?;
        let prefix_root = if prefix.is_empty() {
            root.clone()
        } else {
            root.join(prefix)
        };
        let mut objects = Vec::new();
        for file in files_under(&prefix_root)? {
            let relative = file
                .strip_prefix(&root)
                .map_err(|error| CloudError::Url(error.to_string()))?;
            objects.push(slash_path(relative));
        }
        objects.sort();
        Ok(objects)
    }

    fn acquire_lease(&self, name: &str) -> Result<CloudLease, CloudError> {
        CloudLease::acquire(self.uri.clone(), name)
    }
}

#[cfg(feature = "cloud-object-store")]
impl ObjectStoreProvider {
    /// Opens the configured object store.
    /// # Errors
    /// Fails when the configured provider is unsupported or cannot be initialized.
    pub fn open(config: &ObjectStoreConfig) -> Result<Arc<dyn CloudObjectStore>, CloudError> {
        open_object_store(config)
    }
}

/// Opens the configured object store provider.
/// # Errors
/// Fails when the object store scheme is unsupported in this build.
pub fn open_object_store(
    config: &ObjectStoreConfig,
) -> Result<Arc<dyn CloudObjectStore>, CloudError> {
    match config.uri.scheme() {
        "file" => Ok(Arc::new(LocalObjectStore::new(config.uri.clone()))),
        scheme => Err(CloudError::Unsupported(format!(
            "object store scheme {scheme} requires enabling an external provider"
        ))),
    }
}

impl ObjectStoreUri {
    #[must_use]
    pub fn new(uri: impl Into<String>) -> Self {
        Self { raw: uri.into() }
    }

    #[must_use]
    pub fn from_local_dir(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref();
        let raw = url::Url::from_directory_path(path).map_or_else(
            |()| format!("file://{}", path.display()),
            |url| url.to_string(),
        );
        Self { raw }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    #[must_use]
    pub fn scheme(&self) -> &str {
        self.raw
            .split_once(':')
            .map_or("file", |(scheme, _)| scheme)
    }

    #[must_use]
    pub fn object_store_path(path: &str) -> object_store::path::Path {
        object_store::path::Path::from(path.to_owned())
    }

    fn local_root(&self) -> Result<PathBuf, CloudError> {
        let url = url::Url::parse(&self.raw).map_err(|error| CloudError::Url(error.to_string()))?;
        if url.scheme() != "file" {
            return Err(CloudError::Unsupported(format!(
                "object store scheme {} requires external runtime configuration",
                url.scheme()
            )));
        }
        url.to_file_path()
            .map_err(|()| CloudError::Url(format!("invalid file uri {}", self.raw)))
    }
}

const fn default_tiering_state() -> TieringState {
    TieringState::Remote
}

const fn default_max_concurrent_writes() -> usize {
    usize::MAX
}

impl Default for TieringPolicy {
    fn default() -> Self {
        Self {
            older_than: Duration::ZERO,
            max_local_bytes: 0,
            pin_local: false,
            io_budget_bytes_per_second: None,
        }
    }
}

impl TenantId {
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl TenantQuota {
    #[must_use]
    pub const fn storage_bytes(max_storage_bytes: u64) -> Self {
        Self {
            max_storage_bytes,
            max_concurrent_queries: usize::MAX,
            max_concurrent_writes: usize::MAX,
            max_memory_bytes: u64::MAX,
        }
    }
}

impl TenantConfig {
    #[must_use]
    pub fn new(tenant_id: impl Into<String>, quota: TenantQuota) -> Self {
        Self {
            tenant_id: TenantId::new(tenant_id),
            quota,
        }
    }
}

impl Default for CloudLeaseOptions {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(30),
            owner: None,
        }
    }
}

impl TenantRuntime {
    #[must_use]
    pub fn new(config: TenantConfig) -> Self {
        Self {
            config,
            used_storage_bytes: Arc::new(Mutex::new(0)),
            active_queries: Arc::new(Mutex::new(0)),
            active_writes: Arc::new(Mutex::new(0)),
        }
    }

    #[must_use]
    pub fn config(&self) -> &TenantConfig {
        &self.config
    }

    #[must_use]
    pub fn tenant_id(&self) -> TenantId {
        self.config.tenant_id.clone()
    }

    /// Acquires one tenant query slot.
    /// # Errors
    /// Fails when the query concurrency limit is already reached.
    pub fn try_begin_query(&self) -> Result<TenantPermit, CloudError> {
        self.try_acquire_limit(
            &self.active_queries,
            self.config.quota.max_concurrent_queries,
            "query",
        )
    }

    /// Acquires one tenant write slot.
    /// # Errors
    /// Fails when the write concurrency limit is already reached.
    pub fn try_begin_write(&self) -> Result<TenantPermit, CloudError> {
        self.try_acquire_limit(
            &self.active_writes,
            self.config.quota.max_concurrent_writes,
            "write",
        )
    }

    /// Reserves tenant storage for a batch of replication operations.
    /// # Errors
    /// Fails when the quota lock is poisoned or the reservation would exceed the configured quota.
    pub fn reserve_ops(&self, ops: &[Op]) -> Result<QuotaReservation, CloudError> {
        let requested = estimate_ops_bytes(ops);
        let mut used = self
            .used_storage_bytes
            .lock()
            .map_err(|_| StorageError::Backend("tenant quota lock poisoned".to_owned()))?;
        let remaining = self.config.quota.max_storage_bytes.saturating_sub(*used);
        if requested > remaining {
            return Err(CloudError::QuotaExceeded {
                tenant: self.config.tenant_id.0.clone(),
                requested,
                remaining,
            });
        }
        *used = used.saturating_add(requested);
        Ok(QuotaReservation {
            tenant_id: self.config.tenant_id.clone(),
            bytes: requested,
        })
    }

    pub fn release(&self, reservation: &QuotaReservation) {
        if let Ok(mut used) = self.used_storage_bytes.lock() {
            *used = used.saturating_sub(reservation.bytes);
        }
    }

    pub fn commit_successful_ops(&self, ops: &[Op], _reservation: &QuotaReservation) {
        let released = estimate_delete_bytes(ops);
        if released == 0 {
            return;
        }
        if let Ok(mut used) = self.used_storage_bytes.lock() {
            *used = used.saturating_sub(released);
        }
    }

    /// Reconciles storage accounting from an externally measured durable usage value.
    pub fn reconcile_storage_bytes(&self, used_storage_bytes: u64) {
        if let Ok(mut used) = self.used_storage_bytes.lock() {
            *used = used_storage_bytes;
        }
    }

    #[must_use]
    pub fn used_storage_bytes(&self) -> u64 {
        self.used_storage_bytes.lock().map_or(0, |used| *used)
    }

    fn try_acquire_limit(
        &self,
        active: &Arc<Mutex<usize>>,
        limit: usize,
        kind: &'static str,
    ) -> Result<TenantPermit, CloudError> {
        let mut count = active
            .lock()
            .map_err(|_| StorageError::Backend("tenant concurrency lock poisoned".to_owned()))?;
        if *count >= limit {
            return Err(CloudError::TenantLimitExceeded {
                tenant: self.config.tenant_id.0.clone(),
                kind,
                limit,
            });
        }
        *count = count.saturating_add(1);
        Ok(TenantPermit {
            active: Arc::clone(active),
        })
    }
}

impl Drop for TenantPermit {
    fn drop(&mut self) {
        if let Ok(mut active) = self.active.lock() {
            *active = active.saturating_sub(1);
        }
    }
}

impl HibernateConfig {
    #[must_use]
    pub fn new(object_store: ObjectStoreConfig) -> Self {
        Self {
            object_store,
            conflict_policy: HibernateConflictPolicy::Refuse,
            wait_timeout: Duration::from_secs(30),
        }
    }
}

impl CloudLease {
    /// Acquires a file-backed cloud lease with create-new semantics.
    /// # Errors
    /// Fails when the object store URI is unsupported, the lease already exists, or the lease file cannot be written.
    pub fn acquire(uri: ObjectStoreUri, name: impl Into<String>) -> Result<Self, CloudError> {
        Self::acquire_with_options(uri, name, CloudLeaseOptions::default())
    }

    /// Acquires a file-backed cloud lease with TTL and owner metadata.
    /// # Errors
    /// Fails when a non-expired lease already exists or the lease cannot be written.
    pub fn acquire_with_options(
        uri: ObjectStoreUri,
        name: impl Into<String>,
        options: CloudLeaseOptions,
    ) -> Result<Self, CloudError> {
        let path = format!("leases/{}.lease", sanitize_object_component(&name.into()));
        let lease_id = Uuid::now_v7().to_string();
        let owner = options.owner.unwrap_or_else(|| lease_id.clone());
        let expires_at = SystemTime::now()
            .checked_add(options.ttl.max(Duration::from_millis(1)))
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let root = uri.local_root()?;
        let full_path = root.join(&path);
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)?;
        }
        if let Some(existing) = read_lease_file(&full_path)?
            && existing.expires_at > SystemTime::now()
        {
            return Err(CloudError::LeaseExists(path));
        }
        if full_path.exists() {
            fs::remove_file(&full_path)?;
        }
        let result = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&full_path);
        match result {
            Ok(mut file) => {
                use std::io::Write as _;
                let record = CloudLeaseFile {
                    lease_id: lease_id.clone(),
                    owner: owner.clone(),
                    expires_at,
                };
                let bytes = serde_json::to_vec(&record)
                    .map_err(|error| CloudError::Serde(error.to_string()))?;
                file.write_all(&bytes)?;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                return Err(CloudError::LeaseExists(path));
            }
            Err(error) => return Err(error.into()),
        }

        Ok(Self {
            uri,
            path,
            lease_id,
            owner,
            expires_at,
        })
    }

    #[must_use]
    pub fn lease_id(&self) -> &str {
        &self.lease_id
    }

    #[must_use]
    pub fn fencing_token(&self) -> &str {
        &self.lease_id
    }

    /// Extends the lease if this handle still owns it.
    /// # Errors
    /// Fails when the backing object cannot be read/written or ownership changed.
    pub fn heartbeat(&mut self, ttl: Duration) -> Result<(), CloudError> {
        self.ensure_owner()?;
        self.expires_at = SystemTime::now()
            .checked_add(ttl.max(Duration::from_millis(1)))
            .unwrap_or(SystemTime::UNIX_EPOCH);
        write_lease_file(
            &self.uri.local_root()?.join(&self.path),
            &CloudLeaseFile {
                lease_id: self.lease_id.clone(),
                owner: self.owner.clone(),
                expires_at: self.expires_at,
            },
        )
    }

    /// Validates that a fencing token still owns the named lease.
    /// # Errors
    /// Fails when the lease is missing, expired, or owned by another token.
    pub fn validate_fencing_token(
        uri: &ObjectStoreUri,
        name: impl Into<String>,
        token: &str,
    ) -> Result<(), CloudError> {
        let path = format!("leases/{}.lease", sanitize_object_component(&name.into()));
        let full_path = uri.local_root()?.join(&path);
        let Some(record) = read_lease_file(&full_path)? else {
            return Err(CloudError::LeaseOwnerMismatch(path));
        };
        if record.lease_id != token || record.expires_at <= SystemTime::now() {
            return Err(CloudError::LeaseOwnerMismatch(path));
        }
        Ok(())
    }

    /// Breaks a named lease regardless of owner.
    /// # Errors
    /// Fails when the backing object cannot be removed.
    pub fn break_lease(uri: &ObjectStoreUri, name: impl Into<String>) -> Result<(), CloudError> {
        let path = format!("leases/{}.lease", sanitize_object_component(&name.into()));
        let full_path = uri.local_root()?.join(path);
        if full_path.exists() {
            fs::remove_file(full_path)?;
        }
        Ok(())
    }

    /// Releases the lease file if it still exists.
    /// # Errors
    /// Fails when the object store URI is unsupported, ownership changed, or the lease file cannot be removed.
    pub fn release(&self) -> Result<(), CloudError> {
        let path = self.uri.local_root()?.join(&self.path);
        self.ensure_owner()?;
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }

    fn ensure_owner(&self) -> Result<(), CloudError> {
        let path = self.uri.local_root()?.join(&self.path);
        let Some(record) = read_lease_file(&path)? else {
            return Ok(());
        };
        if record.lease_id != self.lease_id || record.owner != self.owner {
            return Err(CloudError::LeaseOwnerMismatch(self.path.clone()));
        }
        Ok(())
    }
}

impl GuardedResumeSession {
    #[must_use]
    pub const fn report(&self) -> &ResumeReport {
        &self.report
    }

    #[must_use]
    pub fn fencing_token(&self) -> &str {
        &self.fencing_token
    }

    /// Extends the held resume lease.
    /// # Errors
    /// Fails when the lease has been stolen or cannot be persisted.
    pub fn heartbeat(&mut self, ttl: Duration) -> Result<(), CloudError> {
        self.lease.heartbeat(ttl)
    }

    /// Releases the held resume lease.
    /// # Errors
    /// Fails when ownership changed or the lease cannot be removed.
    pub fn release(self) -> Result<(), CloudError> {
        self.lease.release()
    }
}

impl Drop for GuardedResumeSession {
    fn drop(&mut self) {
        let _ = self.lease.release();
    }
}

fn read_lease_file(path: &Path) -> Result<Option<CloudLeaseFile>, CloudError> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path)?;
    if let Ok(record) = serde_json::from_slice::<CloudLeaseFile>(&bytes) {
        return Ok(Some(record));
    }
    let legacy = String::from_utf8_lossy(&bytes).trim().to_owned();
    if legacy.is_empty() {
        return Ok(None);
    }
    Ok(Some(CloudLeaseFile {
        lease_id: legacy.clone(),
        owner: legacy,
        expires_at: SystemTime::now()
            .checked_add(Duration::from_secs(30))
            .unwrap_or(SystemTime::UNIX_EPOCH),
    }))
}

fn write_lease_file(path: &Path, record: &CloudLeaseFile) -> Result<(), CloudError> {
    let bytes = serde_json::to_vec(record).map_err(|error| CloudError::Serde(error.to_string()))?;
    fs::write(path, bytes)?;
    Ok(())
}

pub struct QuotaReplication {
    inner: Arc<dyn Replication>,
    tenant: TenantRuntime,
}

impl QuotaReplication {
    #[must_use]
    pub fn new(inner: Arc<dyn Replication>, tenant: TenantRuntime) -> Self {
        Self { inner, tenant }
    }
}

impl Replication for QuotaReplication {
    fn propose(&self, op: Op) -> Result<(), ReplError> {
        self.propose_batch(vec![op])
    }

    fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
        let _write_permit = self.tenant.try_begin_write().map_err(repl_quota_error)?;
        let reservation = self.tenant.reserve_ops(&ops).map_err(repl_quota_error)?;
        if let Err(error) = self.inner.propose_batch(ops) {
            self.tenant.release(&reservation);
            return Err(error);
        }
        Ok(())
    }

    fn propose_authorized_batch(
        &self,
        ops: Vec<Op>,
        authorization: txn::WriteAuthorization,
    ) -> Result<(), ReplError> {
        self.inner.propose_authorized_batch(ops, authorization)
    }

    fn propose_conditional_batch(&self, batch: ConditionalBatch) -> Result<(), ReplError> {
        let _write_permit = self.tenant.try_begin_write().map_err(repl_quota_error)?;
        let reservation = self
            .tenant
            .reserve_ops(&batch.ops)
            .map_err(repl_quota_error)?;
        if let Err(error) = self.inner.propose_conditional_batch(batch) {
            self.tenant.release(&reservation);
            return Err(error);
        }
        Ok(())
    }

    fn read(
        &self,
        table: &str,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Option<Bytes>, ReplError> {
        let _query_permit = self.tenant.try_begin_query().map_err(repl_quota_error)?;
        self.inner.read(table, key, consistency)
    }

    fn range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        let _query_permit = self.tenant.try_begin_query().map_err(repl_quota_error)?;
        self.inner.range(table, start, end, consistency)
    }

    fn scan_range_batches(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
        batch_rows: usize,
        cancelled: &dyn Fn() -> bool,
        on_batch: &mut dyn FnMut(&[(Bytes, Bytes)]) -> Result<bool, ReplError>,
    ) -> Result<(), ReplError> {
        let _query_permit = self.tenant.try_begin_query().map_err(repl_quota_error)?;
        self.inner.scan_range_batches(
            table,
            start,
            end,
            consistency,
            batch_rows,
            cancelled,
            on_batch,
        )
    }
}

#[must_use]
pub fn repl_quota_error(error: CloudError) -> ReplError {
    match error {
        CloudError::QuotaExceeded {
            tenant,
            requested,
            remaining,
        } => ReplError::QuotaExceeded {
            tenant,
            requested,
            remaining,
        },
        CloudError::TenantLimitExceeded { tenant, .. } => ReplError::QuotaExceeded {
            tenant,
            requested: 1,
            remaining: 0,
        },
        other => ReplError::Storage(StorageError::Backend(other.to_string())),
    }
}

/// Encodes a remote segment pointer into bytes stored in the normal segment table.
/// # Errors
/// Fails when the pointer cannot be serialized.
pub fn encode_tiered_segment_pointer(
    pointer: &TieredSegmentPointer,
) -> Result<Bytes, StorageError> {
    let mut bytes = TIERED_SEGMENT_MAGIC.to_vec();
    bytes.extend_from_slice(
        &serde_json::to_vec(pointer).map_err(|error| StorageError::Backend(error.to_string()))?,
    );
    Ok(bytes)
}

/// Decodes a tiered segment pointer if the payload uses the tiered marker.
/// # Errors
/// Fails when the marker is present but the JSON pointer is corrupt.
pub fn decode_tiered_segment_pointer(
    bytes: &[u8],
) -> Result<Option<TieredSegmentPointer>, StorageError> {
    let Some(payload) = bytes.strip_prefix(TIERED_SEGMENT_MAGIC) else {
        return Ok(None);
    };
    serde_json::from_slice(payload)
        .map(Some)
        .map_err(|error| StorageError::Corruption(error.to_string()))
}

/// Encodes cloud segment metadata for the system keyspace.
/// # Errors
/// Fails when the metadata cannot be serialized.
pub fn encode_segment_metadata(metadata: &SegmentMetadata) -> Result<Bytes, CloudError> {
    serde_json::to_vec(metadata).map_err(|error| CloudError::Serde(error.to_string()))
}

/// Decodes cloud segment metadata from the system keyspace.
/// # Errors
/// Fails when the metadata is corrupt.
pub fn decode_segment_metadata(bytes: &[u8]) -> Result<SegmentMetadata, CloudError> {
    serde_json::from_slice(bytes).map_err(|error| CloudError::Serde(error.to_string()))
}

/// Resolves a local segment payload or fetches a remote segment referenced by a pointer.
/// # Errors
/// Fails when the pointer is corrupt, the remote object cannot be read, or its checksum does not match.
pub fn resolve_tiered_segment_payload(bytes: &[u8]) -> Result<Bytes, StorageError> {
    let Some(pointer) = decode_tiered_segment_pointer(bytes)? else {
        observability::record_cloud_tier_read("local", bytes.len() as u64);
        return Ok(bytes.to_vec());
    };
    let loaded = read_object(&pointer.uri, &pointer.path)
        .map_err(|error| StorageError::Backend(format!("remote segment read failed: {error}")))?;
    if crc32fast::hash(&loaded) != pointer.checksum {
        return Err(StorageError::Corruption(format!(
            "remote segment checksum mismatch for {}",
            pointer.path
        )));
    }
    observability::record_cloud_tier_read("remote", pointer.bytes);
    Ok(loaded)
}

/// Tiers immutable columnar segments into the configured object store.
/// # Errors
/// Fails when storage, upload, checksum verification, or metadata writes fail.
pub fn tier_columnar_segments(
    database: &Database,
    object_store: &ObjectStoreConfig,
    policy: &TieringPolicy,
) -> Result<TieringReport, CloudError> {
    if policy.pin_local {
        return Ok(TieringReport::default());
    }

    let store = open_object_store(object_store)?;
    let mut report = TieringReport::default();
    recover_tiering(database, store.as_ref(), &mut report)?;

    let started = Instant::now();
    let segments = database.range(
        REL_COLUMNAR_SEGMENTS_TABLE,
        &[],
        &[],
        ReadConsistency::Strong,
    )?;
    let mut local_bytes = 0_u64;

    for (key, value) in segments {
        report.scanned_segments = report.scanned_segments.saturating_add(1);
        if decode_tiered_segment_pointer(&value)?.is_some() {
            report.skipped_segments = report.skipped_segments.saturating_add(1);
            continue;
        }

        let bytes = u64::try_from(value.len()).unwrap_or(u64::MAX);
        local_bytes = local_bytes.saturating_add(bytes);
        if local_bytes <= policy.max_local_bytes {
            report.local_bytes = report.local_bytes.saturating_add(bytes);
            report.skipped_segments = report.skipped_segments.saturating_add(1);
            continue;
        }

        throttle(policy.io_budget_bytes_per_second, bytes, started);
        let checksum = crc32fast::hash(&value);
        let object_path = format!(
            "segments/{}/{}.parquet",
            REL_COLUMNAR_SEGMENTS_TABLE,
            hex_key(&key)
        );
        let upload_token = Uuid::now_v7().to_string();
        let uploading = SegmentMetadata {
            table: REL_COLUMNAR_SEGMENTS_TABLE.to_owned(),
            key: key.clone(),
            location: SegmentLocation::Remote {
                uri: object_store.uri.clone(),
                path: object_path.clone(),
            },
            bytes,
            checksum,
            tiered_at: SystemTime::now(),
            upload_token: upload_token.clone(),
            state: TieringState::Uploading,
        };
        propose_system(
            database,
            Op::Put {
                table: CLOUD_SEGMENTS_TABLE.to_owned(),
                key: key.clone(),
                value: encode_segment_metadata(&uploading)?,
            },
        )?;

        store.put(&object_path, &value)?;
        let verified = store.get(&object_path)?;
        if crc32fast::hash(&verified) != checksum {
            return Err(CloudError::Storage(StorageError::Corruption(format!(
                "uploaded object checksum mismatch for {object_path}"
            ))));
        }

        let pointer = TieredSegmentPointer {
            uri: object_store.uri.clone(),
            path: object_path.clone(),
            bytes,
            checksum,
        };
        let mut metadata = uploading;
        metadata.state = TieringState::Remote;

        propose_system_batch(
            database,
            vec![
                Op::Put {
                    table: REL_COLUMNAR_SEGMENTS_TABLE.to_owned(),
                    key: key.clone(),
                    value: encode_tiered_segment_pointer(&pointer)?,
                },
                Op::Put {
                    table: CLOUD_SEGMENTS_TABLE.to_owned(),
                    key,
                    value: encode_segment_metadata(&metadata)?,
                },
            ],
        )?;
        observability::record_cloud_object("put", "ok", bytes);
        report.uploaded_segments = report.uploaded_segments.saturating_add(1);
        report.remote_bytes = report.remote_bytes.saturating_add(bytes);
    }

    Ok(report)
}

fn recover_tiering(
    database: &Database,
    _store: &dyn CloudObjectStore,
    report: &mut TieringReport,
) -> Result<(), CloudError> {
    let metadata_rows = database.range(CLOUD_SEGMENTS_TABLE, &[], &[], ReadConsistency::Strong)?;
    for (metadata_key, value) in metadata_rows {
        let mut metadata = decode_segment_metadata(&value)?;
        if metadata.table != REL_COLUMNAR_SEGMENTS_TABLE {
            continue;
        }

        match metadata.state {
            TieringState::Remote | TieringState::Local => {}
            TieringState::Uploading | TieringState::DeletePending => {
                recover_remote_candidate(database, &metadata_key, &mut metadata, report)?;
            }
            TieringState::Failed => {
                if segment_has_local_payload(database, &metadata.key)? {
                    metadata.state = TieringState::Local;
                    metadata.location = SegmentLocation::Local {
                        table: REL_COLUMNAR_SEGMENTS_TABLE.to_owned(),
                        key: metadata.key.clone(),
                    };
                    propose_system(
                        database,
                        Op::Put {
                            table: CLOUD_SEGMENTS_TABLE.to_owned(),
                            key: metadata_key,
                            value: encode_segment_metadata(&metadata)?,
                        },
                    )?;
                    report.recovered_segments = report.recovered_segments.saturating_add(1);
                }
            }
        }
    }
    Ok(())
}

fn recover_remote_candidate(
    database: &Database,
    metadata_key: &[u8],
    metadata: &mut SegmentMetadata,
    report: &mut TieringReport,
) -> Result<(), CloudError> {
    let SegmentLocation::Remote { uri, path } = &metadata.location else {
        if segment_has_local_payload(database, &metadata.key)? {
            metadata.state = TieringState::Local;
            report.recovered_segments = report.recovered_segments.saturating_add(1);
        } else {
            metadata.state = TieringState::Failed;
            report.failed_segments = report.failed_segments.saturating_add(1);
        }
        propose_system(
            database,
            Op::Put {
                table: CLOUD_SEGMENTS_TABLE.to_owned(),
                key: metadata_key.to_vec(),
                value: encode_segment_metadata(metadata)?,
            },
        )?;
        return Ok(());
    };

    let store = open_object_store(&ObjectStoreConfig { uri: uri.clone() })?;
    if store.exists(path)? {
        let bytes = store.get(path)?;
        if crc32fast::hash(&bytes) == metadata.checksum {
            metadata.state = TieringState::Remote;
            let pointer = TieredSegmentPointer {
                uri: uri.clone(),
                path: path.clone(),
                bytes: metadata.bytes,
                checksum: metadata.checksum,
            };
            propose_system_batch(
                database,
                vec![
                    Op::Put {
                        table: REL_COLUMNAR_SEGMENTS_TABLE.to_owned(),
                        key: metadata.key.clone(),
                        value: encode_tiered_segment_pointer(&pointer)?,
                    },
                    Op::Put {
                        table: CLOUD_SEGMENTS_TABLE.to_owned(),
                        key: metadata_key.to_vec(),
                        value: encode_segment_metadata(metadata)?,
                    },
                ],
            )?;
            report.recovered_segments = report.recovered_segments.saturating_add(1);
            report.remote_bytes = report.remote_bytes.saturating_add(metadata.bytes);
            return Ok(());
        }
    }

    if segment_has_local_payload(database, &metadata.key)? {
        metadata.state = TieringState::Local;
        metadata.location = SegmentLocation::Local {
            table: REL_COLUMNAR_SEGMENTS_TABLE.to_owned(),
            key: metadata.key.clone(),
        };
        report.recovered_segments = report.recovered_segments.saturating_add(1);
    } else {
        metadata.state = TieringState::Failed;
        report.failed_segments = report.failed_segments.saturating_add(1);
    }
    propose_system(
        database,
        Op::Put {
            table: CLOUD_SEGMENTS_TABLE.to_owned(),
            key: metadata_key.to_vec(),
            value: encode_segment_metadata(metadata)?,
        },
    )?;
    Ok(())
}

fn segment_has_local_payload(database: &Database, key: &[u8]) -> Result<bool, CloudError> {
    let Some(value) = database.read(REL_COLUMNAR_SEGMENTS_TABLE, key, ReadConsistency::Strong)?
    else {
        return Ok(false);
    };
    Ok(decode_tiered_segment_pointer(&value)?.is_none())
}

/// Writes a full backup under an object-store URI.
/// # Errors
/// Fails when the underlying backup fails or any object cannot be written.
pub fn full_backup_to_uri(
    database: &Database,
    uri: &ObjectStoreUri,
    config: &BackupConfig,
) -> Result<BackupReport, CloudError> {
    let temp = tempfile_backup_root();
    let report = full_backup(database, &temp, config)?;
    upload_directory(&report.path, uri, &report.manifest.backup_id)?;
    Ok(BackupReport {
        manifest: report.manifest,
        path: PathBuf::from(uri.as_str()).join(report.path.file_name().unwrap_or_default()),
    })
}

/// Writes an incremental backup under an object-store URI.
/// # Errors
/// Fails when the parent cannot be downloaded, the incremental backup fails, or any object cannot be written.
pub fn incremental_backup_to_uri(
    database: &Database,
    uri: &ObjectStoreUri,
    parent_uri: &ObjectStoreUri,
    parent_backup_id: &str,
    config: &BackupConfig,
) -> Result<BackupReport, CloudError> {
    let parent_temp = tempfile_backup_root().join(parent_backup_id);
    download_directory(parent_uri, parent_backup_id, &parent_temp)?;
    let temp = tempfile_backup_root();
    let report = incremental_backup(database, &temp, &parent_temp, config)?;
    upload_directory(&report.path, uri, &report.manifest.backup_id)?;
    Ok(BackupReport {
        manifest: report.manifest,
        path: PathBuf::from(uri.as_str()).join(report.path.file_name().unwrap_or_default()),
    })
}

/// Restores a backup chain from an object-store URI.
/// # Errors
/// Fails when the object prefix cannot be downloaded or restore validation fails.
pub fn restore_backup_from_uri(
    uri: &ObjectStoreUri,
    backup_id: &str,
    config: DbConfig,
    target: RestoreTarget,
) -> Result<RestoreReport, CloudError> {
    let temp = tempfile_backup_root().join(backup_id);
    download_directory(uri, backup_id, &temp)?;
    restore_backup(&temp, config, target).map_err(Into::into)
}

/// Restores a backup chain from an object-store URI with explicit decryption settings.
/// # Errors
/// Fails when the object prefix cannot be downloaded or restore validation fails.
pub fn restore_backup_from_uri_with_config(
    uri: &ObjectStoreUri,
    backup_id: &str,
    config: DbConfig,
    target: RestoreTarget,
    restore_config: &RestoreConfig,
) -> Result<RestoreReport, CloudError> {
    let temp = tempfile_backup_root().join(backup_id);
    download_directory(uri, backup_id, &temp)?;
    restore_backup_with_config(&temp, config, target, restore_config).map_err(Into::into)
}

/// Verifies a backup chain stored under an object-store URI.
/// # Errors
/// Fails when objects cannot be downloaded, checksums fail, or the restore drill fails.
pub fn verify_backup_uri(
    uri: &ObjectStoreUri,
    backup_id: &str,
) -> Result<VerifyReport, CloudError> {
    let temp = tempfile_backup_root().join(backup_id);
    download_directory(uri, backup_id, &temp)?;
    verify_backup(&temp).map_err(Into::into)
}

/// Verifies a backup chain stored under an object-store URI with explicit decryption settings.
/// # Errors
/// Fails when objects cannot be downloaded, checksums fail, or the restore drill fails.
pub fn verify_backup_uri_with_config(
    uri: &ObjectStoreUri,
    backup_id: &str,
    restore_config: &RestoreConfig,
) -> Result<VerifyReport, CloudError> {
    let temp = tempfile_backup_root().join(backup_id);
    download_directory(uri, backup_id, &temp)?;
    verify_backup_with_config(&temp, restore_config).map_err(Into::into)
}

/// Deletes backup objects that are not retained by the supplied policy.
/// # Errors
/// Fails when manifests cannot be read or retained objects cannot be deleted.
pub fn gc_backup_uri(
    uri: &BackupUri,
    policy: &BackupGcPolicy,
) -> Result<BackupGcReport, CloudError> {
    let store = open_object_store(&ObjectStoreConfig { uri: uri.0.clone() })?;
    let manifests = read_backup_manifests(store.as_ref())?;
    let mut report = BackupGcReport {
        scanned_backups: manifests.len(),
        ..BackupGcReport::default()
    };
    let retained = retained_backup_ids(&manifests, policy);

    for manifest in manifests {
        if retained.contains(&manifest.backup_id) {
            report.kept_backups = report.kept_backups.saturating_add(1);
            continue;
        }

        let prefix = format!("{}/", manifest.backup_id);
        let objects = store.list_prefix(&prefix)?;
        for object in &objects {
            store.delete(object)?;
        }
        report.deleted_objects = report.deleted_objects.saturating_add(objects.len());
        report.deleted_backups = report.deleted_backups.saturating_add(1);
    }

    Ok(report)
}

fn read_backup_manifests(store: &dyn CloudObjectStore) -> Result<Vec<BackupManifest>, CloudError> {
    let mut backup_ids = BTreeSet::new();
    for object in store.list_prefix("")? {
        let Some((backup_id, _)) = object.split_once('/') else {
            continue;
        };
        backup_ids.insert(backup_id.to_owned());
    }

    let mut manifests = Vec::new();
    for backup_id in backup_ids {
        let manifest_path = format!("{backup_id}/manifest.json");
        if !store.exists(&manifest_path)? {
            continue;
        }
        let manifest = serde_json::from_slice::<BackupManifest>(&store.get(&manifest_path)?)
            .map_err(|error| CloudError::Serde(error.to_string()))?;
        manifests.push(manifest);
    }
    manifests.sort_by_key(|manifest| manifest.taken_at);
    Ok(manifests)
}

fn retained_backup_ids(manifests: &[BackupManifest], policy: &BackupGcPolicy) -> BTreeSet<String> {
    let mut retained = BTreeSet::new();
    let now = SystemTime::now();

    if let Some(retain_newer_than) = policy.retain_newer_than {
        for manifest in manifests {
            let keep = now
                .duration_since(manifest.taken_at)
                .map_or(true, |age| age <= retain_newer_than);
            if keep {
                retained.insert(manifest.backup_id.clone());
            }
        }
    }

    let mut fulls = manifests
        .iter()
        .filter(|manifest| manifest.kind == BackupKind::Full)
        .collect::<Vec<_>>();
    fulls.sort_by_key(|manifest| manifest.taken_at);
    for manifest in fulls.into_iter().rev().take(policy.keep_last_full) {
        retained.insert(manifest.backup_id.clone());
    }

    let by_id = manifests
        .iter()
        .map(|manifest| (manifest.backup_id.clone(), manifest))
        .collect::<BTreeMap<_, _>>();
    let mut stack = retained.iter().cloned().collect::<Vec<_>>();
    while let Some(backup_id) = stack.pop() {
        let Some(manifest) = by_id.get(&backup_id) else {
            continue;
        };
        if let Some(parent) = &manifest.parent_backup_id
            && retained.insert(parent.clone())
        {
            stack.push(parent.clone());
        }
    }

    let mut children_by_parent = BTreeMap::<String, Vec<String>>::new();
    for manifest in manifests {
        if let Some(parent) = &manifest.parent_backup_id {
            children_by_parent
                .entry(parent.clone())
                .or_default()
                .push(manifest.backup_id.clone());
        }
    }
    let mut stack = retained
        .iter()
        .filter_map(|backup_id| {
            by_id
                .get(backup_id)
                .filter(|manifest| manifest.kind == BackupKind::Full)
                .map(|_| backup_id.clone())
        })
        .collect::<Vec<_>>();
    while let Some(parent) = stack.pop() {
        let Some(children) = children_by_parent.get(&parent) else {
            continue;
        };
        for child in children {
            if retained.insert(child.clone()) {
                stack.push(child.clone());
            }
        }
    }

    retained
}

/// Hibernates a database by writing a consistent backup and marker under a cloud lease.
/// # Errors
/// Fails when the lease cannot be acquired, backup/upload fails, or the marker cannot be written.
pub fn hibernate_database(
    database: &Database,
    config: &HibernateConfig,
) -> Result<HibernateReport, CloudError> {
    let lease = CloudLease::acquire(config.object_store.uri.clone(), "database")?;
    let lease_id = lease.lease_id().to_owned();
    let backup = full_backup_to_uri(database, &config.object_store.uri, &BackupConfig::default())?;
    let hibernated_at = SystemTime::now();
    let marker = HibernateMarker {
        lease_id: lease_id.clone(),
        backup_id: backup.manifest.backup_id.clone(),
        hibernated_at,
        hibernated_lsn: backup.manifest.end_lsn,
        tenant_id: database.tenant_id(),
        fencing_token: lease_id.clone(),
    };
    let marker_bytes =
        serde_json::to_vec(&marker).map_err(|error| CloudError::Serde(error.to_string()))?;
    write_object(
        &config.object_store.uri,
        "hibernate/hibernated.json",
        &marker_bytes,
    )?;
    lease.release()?;
    Ok(HibernateReport {
        lease_id,
        backup_id: backup.manifest.backup_id,
        hibernated_at,
        hibernated_lsn: backup.manifest.end_lsn,
        fencing_token: marker.fencing_token,
        object_prefix: config.object_store.uri.as_str().to_owned(),
    })
}

/// Reads the hibernation marker from object storage.
/// # Errors
/// Fails when the marker cannot be read or decoded.
pub fn read_hibernation_marker(uri: &ObjectStoreUri) -> Result<HibernateMarker, CloudError> {
    let bytes = read_object(uri, "hibernate/hibernated.json")?;
    serde_json::from_slice(&bytes).map_err(|error| CloudError::Serde(error.to_string()))
}

/// Resumes a hibernated database while holding a fencing lease for the resumed instance.
/// # Errors
/// Fails when the marker/lease/backup cannot be read, restored, or consumed.
pub fn resume_database_guarded(
    uri: &ObjectStoreUri,
    config: DbConfig,
) -> Result<GuardedResumeSession, CloudError> {
    let started = Instant::now();
    let marker = read_hibernation_marker(uri)?;
    let lease = CloudLease::acquire(uri.clone(), "database")?;
    let lease_id = lease.lease_id().to_owned();
    let report = restore_backup_from_uri(uri, &marker.backup_id, config, RestoreTarget::Latest)?;
    let store = open_object_store(&ObjectStoreConfig { uri: uri.clone() })?;
    store.delete("hibernate/hibernated.json")?;
    Ok(GuardedResumeSession {
        lease,
        report: ResumeReport {
            lease_id: lease_id.clone(),
            restored_lsn: report.restored_lsn,
            time_to_ready_ms: started.elapsed().as_millis(),
            lazy_loaded_segments: 0,
        },
        fencing_token: lease_id,
    })
}

/// Resumes a database by restoring a backup from object storage under a cloud lease.
/// # Errors
/// Fails when the lease cannot be acquired or the backup cannot be restored.
pub fn resume_database(
    uri: &ObjectStoreUri,
    backup_id: &str,
    config: DbConfig,
) -> Result<ResumeReport, CloudError> {
    let started = Instant::now();
    let lease = CloudLease::acquire(uri.clone(), "database")?;
    let lease_id = lease.lease_id().to_owned();
    let report = restore_backup_from_uri(uri, backup_id, config, RestoreTarget::Latest)?;
    lease.release()?;
    Ok(ResumeReport {
        lease_id,
        restored_lsn: report.restored_lsn,
        time_to_ready_ms: started.elapsed().as_millis(),
        lazy_loaded_segments: 0,
    })
}

fn write_object(uri: &ObjectStoreUri, path: &str, bytes: &[u8]) -> Result<(), CloudError> {
    let store = open_object_store(&ObjectStoreConfig { uri: uri.clone() })?;
    store.put(path, bytes)
}

fn read_object(uri: &ObjectStoreUri, path: &str) -> Result<Bytes, CloudError> {
    let store = open_object_store(&ObjectStoreConfig { uri: uri.clone() })?;
    store.get(path)
}

fn upload_directory(source: &Path, uri: &ObjectStoreUri, prefix: &str) -> Result<(), CloudError> {
    for file in files_under(source)? {
        let relative = file
            .strip_prefix(source)
            .map_err(|error| CloudError::Url(error.to_string()))?;
        let object_path = format!("{}/{}", prefix, slash_path(relative));
        write_object(uri, &object_path, &fs::read(&file)?)?;
    }
    Ok(())
}

fn download_directory(uri: &ObjectStoreUri, prefix: &str, dest: &Path) -> Result<(), CloudError> {
    let root = uri.local_root()?.join(prefix);
    if !root.exists() {
        return Err(CloudError::Unsupported(format!(
            "missing object prefix {prefix}"
        )));
    }
    for file in files_under(&root)? {
        let relative = file
            .strip_prefix(&root)
            .map_err(|error| CloudError::Url(error.to_string()))?;
        let target = dest.join(relative);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(&file, target)?;
    }
    Ok(())
}

fn files_under(root: &Path) -> Result<Vec<PathBuf>, CloudError> {
    let mut files = Vec::new();
    if !root.exists() {
        return Ok(files);
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            files.extend(files_under(&path)?);
        } else {
            files.push(path);
        }
    }
    Ok(files)
}

fn estimate_ops_bytes(ops: &[Op]) -> u64 {
    ops.iter()
        .map(|op| match op {
            Op::Put { table, key, value } => table.len() + key.len() + value.len(),
            Op::Delete { .. } => 0,
        })
        .map(|bytes| u64::try_from(bytes).unwrap_or(u64::MAX))
        .sum()
}

fn estimate_delete_bytes(ops: &[Op]) -> u64 {
    ops.iter()
        .map(|op| match op {
            Op::Put { .. } => 0,
            Op::Delete { table, key } => table.len() + key.len(),
        })
        .map(|bytes| u64::try_from(bytes).unwrap_or(u64::MAX))
        .sum()
}

fn hex_key(key: &[u8]) -> String {
    let mut out = String::with_capacity(key.len() * 2);
    for byte in key {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn slash_path(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn sanitize_object_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

fn tempfile_backup_root() -> PathBuf {
    std::env::temp_dir().join(format!("multidb-cloud-{}", Uuid::now_v7()))
}

fn throttle(limit: Option<u64>, bytes: u64, started: Instant) {
    let Some(limit) = limit else {
        return;
    };
    if limit == 0 {
        return;
    }
    let expected_nanos = u128::from(bytes)
        .saturating_mul(1_000_000_000)
        .checked_div(u128::from(limit))
        .unwrap_or(u128::MAX);
    let expected = Duration::from_nanos(u64::try_from(expected_nanos).unwrap_or(u64::MAX));
    if let Some(delay) = expected.checked_sub(started.elapsed()) {
        std::thread::sleep(delay);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        time::{Duration, SystemTime},
    };

    use super::{
        decode_tiered_segment_pointer, encode_tiered_segment_pointer, hex_key, retained_backup_ids,
        write_object,
    };

    use crate::{
        backup::{BackupConfig, BackupKind, BackupManifest},
        cloud::{
            BackupGcPolicy, BackupUri, CLOUD_SEGMENTS_TABLE, CloudLease, CloudLeaseOptions,
            ObjectStoreConfig, SegmentLocation, SegmentMetadata, TenantConfig, TenantQuota,
            TenantRuntime, TieredSegmentPointer, TieringPolicy, TieringState,
            encode_segment_metadata, full_backup_to_uri, gc_backup_uri, hibernate_database,
            read_hibernation_marker, resolve_tiered_segment_payload, restore_backup_from_uri,
            resume_database_guarded, tier_columnar_segments, verify_backup_uri,
        },
        db::{
            DbConfig, OperationalConfig, Profile, create_database, create_database_with_ops,
            open_database,
        },
        model::Value,
        query::{ColumnDef, ColumnType, REL_COLUMNAR_SEGMENTS_TABLE, TableSchema},
        repl::{Op, ReadConsistency, Replication, propose_system},
    };

    #[test]
    fn tiered_columnar_segments_read_transparently() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let db_path = temp.path().join("analytics.redb");
        let mut database = create_database(DbConfig::on_disk(Profile::Analytical, &db_path))?;
        let schema = TableSchema::new(
            vec![
                ColumnDef::new("id", ColumnType::Int, false),
                ColumnDef::new("amount", ColumnType::Int, false),
            ],
            0,
        );
        let table = database.create_table("sales", Some(schema), Vec::new())?;
        table.insert(vec![Value::Int(1), Value::Int(10)])?;
        table.insert(vec![Value::Int(2), Value::Int(20)])?;

        let store = ObjectStoreConfig::local_dir(temp.path().join("objects"));
        let report = tier_columnar_segments(&database, &store, &TieringPolicy::default())?;
        assert_eq!(report.uploaded_segments, 1);

        drop(table);
        drop(database);
        let reopened = open_database(DbConfig::on_disk(Profile::Analytical, &db_path))?;
        let table = reopened.table("sales")?;
        assert_eq!(table.scan()?.len(), 2);
        Ok(())
    }

    #[test]
    fn tiering_recovers_uploading_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let db_path = temp.path().join("analytics-recovery.redb");
        let mut database = create_database(DbConfig::on_disk(Profile::Analytical, &db_path))?;
        let schema = TableSchema::new(
            vec![
                ColumnDef::new("id", ColumnType::Int, false),
                ColumnDef::new("amount", ColumnType::Int, false),
            ],
            0,
        );
        database
            .create_table("sales", Some(schema), Vec::new())?
            .insert(vec![Value::Int(1), Value::Int(10)])?;

        let store = ObjectStoreConfig::local_dir(temp.path().join("objects"));
        let segments = database.range(
            REL_COLUMNAR_SEGMENTS_TABLE,
            &[],
            &[],
            ReadConsistency::Strong,
        )?;
        assert_eq!(segments.len(), 1);
        let (key, value) = segments[0].clone();
        let object_path = format!(
            "segments/{}/{}.parquet",
            REL_COLUMNAR_SEGMENTS_TABLE,
            hex_key(&key)
        );
        write_object(&store.uri, &object_path, &value)?;
        let metadata = SegmentMetadata {
            table: REL_COLUMNAR_SEGMENTS_TABLE.to_owned(),
            key: key.clone(),
            location: SegmentLocation::Remote {
                uri: store.uri.clone(),
                path: object_path,
            },
            bytes: u64::try_from(value.len())?,
            checksum: crc32fast::hash(&value),
            tiered_at: SystemTime::now(),
            upload_token: "interrupted-upload".to_owned(),
            state: TieringState::Uploading,
        };
        propose_system(
            &database,
            Op::Put {
                table: CLOUD_SEGMENTS_TABLE.to_owned(),
                key: key.clone(),
                value: encode_segment_metadata(&metadata)?,
            },
        )?;

        let report = tier_columnar_segments(&database, &store, &TieringPolicy::default())?;
        assert_eq!(report.recovered_segments, 1);
        assert_eq!(report.uploaded_segments, 0);
        let pointer_bytes = database
            .read(REL_COLUMNAR_SEGMENTS_TABLE, &key, ReadConsistency::Strong)?
            .ok_or("missing recovered segment pointer")?;
        assert!(decode_tiered_segment_pointer(&pointer_bytes)?.is_some());
        Ok(())
    }

    #[test]
    fn remote_checksum_mismatch_is_corruption() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let uri = ObjectStoreConfig::local_dir(temp.path()).uri;
        write_object(&uri, "segments/bad.parquet", b"corrupt")?;
        let pointer = TieredSegmentPointer {
            uri,
            path: "segments/bad.parquet".to_owned(),
            bytes: 7,
            checksum: 1,
        };
        let bytes = encode_tiered_segment_pointer(&pointer)?;
        assert!(resolve_tiered_segment_payload(&bytes).is_err());
        Ok(())
    }

    #[test]
    fn backup_uri_verify_and_restore_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let source_path = temp.path().join("source.redb");
        let restore_path = temp.path().join("restore.redb");
        let mut database =
            create_database(DbConfig::on_disk(Profile::Transactional, &source_path))?;
        let schema = TableSchema::new(
            vec![
                ColumnDef::new("id", ColumnType::Int, false),
                ColumnDef::new("name", ColumnType::Str, false),
            ],
            0,
        );
        database
            .create_table("users", Some(schema), Vec::new())?
            .insert(vec![Value::Int(1), Value::Str("Ada".to_owned())])?;

        let uri = ObjectStoreConfig::local_dir(temp.path().join("backup")).uri;
        let backup = full_backup_to_uri(&database, &uri, &BackupConfig::default())?;
        let verify = verify_backup_uri(&uri, &backup.manifest.backup_id)?;
        assert_eq!(verify.restored_lsn, backup.manifest.end_lsn);

        restore_backup_from_uri(
            &uri,
            &backup.manifest.backup_id,
            DbConfig::on_disk(Profile::Transactional, &restore_path),
            crate::backup::RestoreTarget::Latest,
        )?;
        let restored = open_database(DbConfig::on_disk(Profile::Transactional, &restore_path))?;
        assert_eq!(restored.table("users")?.scan()?.len(), 1);
        Ok(())
    }

    #[test]
    fn backup_gc_keeps_parent_chain_and_deletes_old_fulls() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp = tempfile::tempdir()?;
        let source_path = temp.path().join("gc-source.redb");
        let mut database =
            create_database(DbConfig::on_disk(Profile::Transactional, &source_path))?;
        let schema = TableSchema::new(
            vec![
                ColumnDef::new("id", ColumnType::Int, false),
                ColumnDef::new("name", ColumnType::Str, false),
            ],
            0,
        );
        let table = database.create_table("users", Some(schema), Vec::new())?;
        table.insert(vec![Value::Int(1), Value::Str("Ada".to_owned())])?;

        let uri = ObjectStoreConfig::local_dir(temp.path().join("backup")).uri;
        let first = full_backup_to_uri(&database, &uri, &BackupConfig::default())?;
        table.insert(vec![Value::Int(2), Value::Str("Grace".to_owned())])?;
        let _second = full_backup_to_uri(&database, &uri, &BackupConfig::default())?;

        let report = gc_backup_uri(
            &BackupUri::new(uri.clone()),
            &BackupGcPolicy {
                keep_last_full: 0,
                retain_newer_than: None,
            },
        )?;
        assert_eq!(report.deleted_backups, 2);
        assert!(
            !uri.local_root()?
                .join(&first.manifest.backup_id)
                .join("manifest.json")
                .exists()
        );

        let now = SystemTime::now();
        let full = fake_manifest(
            "full-parent",
            BackupKind::Full,
            None,
            now.checked_sub(Duration::from_secs(3_600))
                .ok_or("time underflow")?,
        );
        let incremental = fake_manifest(
            "incremental-child",
            BackupKind::Incremental,
            Some("full-parent".to_owned()),
            now,
        );
        let retained = retained_backup_ids(
            &[full, incremental],
            &BackupGcPolicy {
                keep_last_full: 0,
                retain_newer_than: Some(Duration::from_secs(60)),
            },
        );
        assert!(retained.contains("incremental-child"));
        assert!(retained.contains("full-parent"));

        let old_full = fake_manifest(
            "old-full",
            BackupKind::Full,
            None,
            now.checked_sub(Duration::from_secs(7_200))
                .ok_or("time underflow")?,
        );
        let old_incremental = fake_manifest(
            "old-incremental",
            BackupKind::Incremental,
            Some("old-full".to_owned()),
            now.checked_sub(Duration::from_secs(3_600))
                .ok_or("time underflow")?,
        );
        let retained = retained_backup_ids(
            &[old_full, old_incremental],
            &BackupGcPolicy {
                keep_last_full: 1,
                retain_newer_than: None,
            },
        );
        assert!(retained.contains("old-full"));
        assert!(retained.contains("old-incremental"));
        Ok(())
    }

    #[test]
    fn tenant_quota_rejects_large_write() -> Result<(), Box<dyn std::error::Error>> {
        let tenant = TenantConfig::new("tenant-a", TenantQuota::storage_bytes(1_500));
        let ops = OperationalConfig::new().with_tenant(tenant);
        let mut database = create_database_with_ops(DbConfig::new(Profile::InMemory), ops)?;
        let schema = TableSchema::new(
            vec![
                ColumnDef::new("id", ColumnType::Int, false),
                ColumnDef::new("payload", ColumnType::Str, false),
            ],
            0,
        );
        let table = database.create_table("items", Some(schema), Vec::new())?;
        let result = table.insert(vec![Value::Int(1), Value::Str("x".repeat(4_096))]);
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn tenant_concurrency_limit_releases_permit() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = TenantRuntime::new(TenantConfig::new(
            "tenant-a",
            TenantQuota {
                max_storage_bytes: u64::MAX,
                max_concurrent_queries: 1,
                max_concurrent_writes: 1,
                max_memory_bytes: u64::MAX,
            },
        ));

        let query = runtime.try_begin_query()?;
        assert!(runtime.try_begin_query().is_err());
        drop(query);
        assert!(runtime.try_begin_query().is_ok());

        let write = runtime.try_begin_write()?;
        assert!(runtime.try_begin_write().is_err());
        drop(write);
        assert!(runtime.try_begin_write().is_ok());
        Ok(())
    }

    #[test]
    fn tenant_quota_delete_reduces_accounted_storage() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = TenantRuntime::new(TenantConfig::new(
            "tenant-a",
            TenantQuota::storage_bytes(10_000),
        ));
        let put = Op::Put {
            table: "items".to_owned(),
            key: b"1".to_vec(),
            value: vec![7; 100],
        };
        let reservation = runtime.reserve_ops(std::slice::from_ref(&put))?;
        let after_put = runtime.used_storage_bytes();
        assert!(after_put > 100);

        runtime.commit_successful_ops(
            &[Op::Delete {
                table: "items".to_owned(),
                key: b"1".to_vec(),
            }],
            &reservation,
        );
        assert!(runtime.used_storage_bytes() < after_put);
        runtime.reconcile_storage_bytes(42);
        assert_eq!(runtime.used_storage_bytes(), 42);
        Ok(())
    }

    #[test]
    fn cloud_lease_prevents_double_resume() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let uri = ObjectStoreConfig::local_dir(temp.path()).uri;
        let lease = CloudLease::acquire(uri.clone(), "database")?;
        assert!(CloudLease::acquire(uri, "database").is_err());
        lease.release()?;
        Ok(())
    }

    #[test]
    fn cloud_lease_checks_owner_and_heartbeat_ttl() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let uri = ObjectStoreConfig::local_dir(temp.path()).uri;
        let mut lease = CloudLease::acquire_with_options(
            uri.clone(),
            "database",
            CloudLeaseOptions {
                ttl: Duration::from_secs(30),
                owner: Some("writer-a".to_owned()),
            },
        )?;
        CloudLease::validate_fencing_token(&uri, "database", lease.fencing_token())?;

        let mut stolen = lease.clone();
        "other".clone_into(&mut stolen.lease_id);
        assert!(stolen.release().is_err());
        lease.heartbeat(Duration::from_secs(60))?;
        lease.release()?;

        let expired = CloudLease::acquire_with_options(
            uri.clone(),
            "database",
            CloudLeaseOptions {
                ttl: Duration::from_millis(1),
                owner: Some("writer-b".to_owned()),
            },
        )?;
        std::thread::sleep(Duration::from_millis(5));
        let replacement = CloudLease::acquire(uri.clone(), "database")?;
        assert_ne!(expired.lease_id(), replacement.lease_id());
        replacement.release()?;
        Ok(())
    }

    #[test]
    fn hibernation_writes_marker_under_lease() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let db_path = temp.path().join("hibernate.redb");
        let database = create_database(DbConfig::on_disk(Profile::Transactional, &db_path))?;
        let config = crate::cloud::HibernateConfig::new(ObjectStoreConfig::local_dir(
            temp.path().join("objects"),
        ));
        let report = hibernate_database(&database, &config)?;
        assert!(!report.backup_id.is_empty());
        assert_eq!(report.fencing_token, report.lease_id);
        let marker = read_hibernation_marker(&config.object_store.uri)?;
        assert_eq!(marker.backup_id, report.backup_id);
        assert_eq!(marker.hibernated_lsn, report.hibernated_lsn);
        assert_eq!(marker.fencing_token, report.fencing_token);
        Ok(())
    }

    #[test]
    fn guarded_resume_consumes_marker_and_holds_lease() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let source_path = temp.path().join("hibernate-source.redb");
        let restore_path = temp.path().join("hibernate-restore.redb");
        let database = create_database(DbConfig::on_disk(Profile::Transactional, &source_path))?;
        let config = crate::cloud::HibernateConfig::new(ObjectStoreConfig::local_dir(
            temp.path().join("objects"),
        ));
        hibernate_database(&database, &config)?;

        let session = resume_database_guarded(
            &config.object_store.uri,
            DbConfig::on_disk(Profile::Transactional, &restore_path),
        )?;
        assert_eq!(session.report().lease_id, session.fencing_token());
        CloudLease::validate_fencing_token(
            &config.object_store.uri,
            "database",
            session.fencing_token(),
        )?;
        assert!(read_hibernation_marker(&config.object_store.uri).is_err());
        assert!(CloudLease::acquire(config.object_store.uri.clone(), "database").is_err());
        session.release()?;
        assert!(CloudLease::acquire(config.object_store.uri.clone(), "database").is_ok());
        Ok(())
    }

    #[test]
    fn non_pointer_segment_payload_is_returned_as_is() -> Result<(), Box<dyn std::error::Error>> {
        let bytes = b"plain parquet bytes";
        assert_eq!(resolve_tiered_segment_payload(bytes)?, bytes);
        Ok(())
    }

    fn fake_manifest(
        backup_id: &str,
        kind: BackupKind,
        parent_backup_id: Option<String>,
        taken_at: SystemTime,
    ) -> BackupManifest {
        BackupManifest {
            format_version: 1,
            backup_id: backup_id.to_owned(),
            kind,
            parent_backup_id,
            timeline_id: "timeline".to_owned(),
            profile: Profile::Transactional,
            start_lsn: 0,
            end_lsn: 1,
            taken_at,
            engine_version: "test".to_owned(),
            files: Vec::new(),
            keyspace_counts: BTreeMap::new(),
            encryption: None,
        }
    }
}
