use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Cursor,
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant, SystemTime},
};

use chacha20poly1305::{
    XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit, Payload},
};
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::{
    cloud::{CLOUD_HIBERNATION_TABLE, CLOUD_SEGMENTS_TABLE},
    db::{
        ConfigError, Database, DbConfig, EncryptionConfig, EncryptionMode, Profile, create_database,
    },
    fileio,
    model::{DOCUMENT_TABLE, INDEX_TABLE},
    observability,
    query::{
        PLANNER_FEEDBACK_TABLE, PLANNER_META_TABLE, REL_COLUMNAR_SEGMENTS_TABLE, REL_INDEX_TABLE,
        REL_ROWS_TABLE, REL_SCHEMA_TABLE, STATS_TABLE,
    },
    repl::{
        AP_HINTS_TABLE, AP_MERKLE_TABLE, AP_VERSIONS_TABLE, HEALING_EVENTS_TABLE,
        HEALING_STATE_TABLE, ReadConsistency, ReplError, Replication, SHARD_MIGRATIONS_TABLE,
    },
    security::AUDIT_TABLE,
    storage::{
        Bytes, ConfiguredKeyProvider, KeyProvider, ReadTransaction, StorageEngine, StorageError,
        WriteTransaction,
    },
    txn::{self, CommitLogRecord, TxnId},
    vector::VECTOR_TABLE,
};

pub type Lsn = TxnId;
pub type BackupId = String;
pub type TimelineId = String;

const FORMAT_VERSION: u32 = 1;
const MANIFEST_FILE: &str = "manifest.json";
const FULL_DATA_FILE: &str = "data/full.json.zst";
const WAL_DATA_FILE: &str = "wal/incremental.json.zst";
const BACKUP_META_TABLE: &str = "__backup_meta";
const AUTHZ_TABLE: &str = "__authz_policy";
const KEY_TIMELINE_ID: &[u8] = b"timeline_id";
const KEY_RECOVERY_LSN: &[u8] = b"recovery_lsn";
const KEYSPACE_PRESENT_VALUE: &[u8] = b"1";

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum BackupKind {
    Full,
    Incremental,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum RestoreTarget {
    Latest,
    Lsn(Lsn),
    Time(SystemTime),
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct BackupConfig {
    pub max_bytes_per_second: Option<u64>,
    #[serde(default, skip)]
    pub encryption: Option<EncryptionConfig>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RestoreConfig {
    pub encryption: Option<EncryptionConfig>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct BackupManifest {
    pub format_version: u32,
    pub backup_id: BackupId,
    pub kind: BackupKind,
    pub parent_backup_id: Option<BackupId>,
    pub timeline_id: TimelineId,
    pub profile: Profile,
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,
    pub taken_at: SystemTime,
    pub engine_version: String,
    pub files: Vec<BackupFileEntry>,
    pub keyspace_counts: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<BackupEncryptionMetadata>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct BackupFileEntry {
    pub path: String,
    pub bytes: u64,
    pub checksum: u32,
    pub logical_checksum: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<BackupFileEncryption>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct BackupEncryptionMetadata {
    pub algorithm: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct BackupFileEncryption {
    pub algorithm: String,
    pub key_id: u64,
    pub nonce: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackupReport {
    pub manifest: BackupManifest,
    pub path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreReport {
    pub restored_lsn: Lsn,
    pub timeline_id: TimelineId,
    pub applied_commits: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifyReport {
    pub manifest: BackupManifest,
    pub restored_lsn: Lsn,
    pub checked_files: usize,
}

#[derive(thiserror::Error, Debug)]
pub enum BackupError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("config: {0}")]
    Config(#[from] ConfigError),

    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("metadata serialization: {0}")]
    Serde(String),

    #[error("backup corruption: {0}")]
    Corruption(String),

    #[error("missing commit log range: {start}..={end}")]
    MissingLogRange { start: Lsn, end: Lsn },

    #[error("missing parent backup: {0}")]
    MissingParent(BackupId),

    #[error("unsupported backup operation: {0}")]
    Unsupported(String),
}

impl BackupConfig {
    #[must_use]
    pub fn with_encryption(mut self, encryption: EncryptionConfig) -> Self {
        self.encryption = Some(encryption);
        self
    }
}

impl RestoreConfig {
    #[must_use]
    pub fn with_encryption(mut self, encryption: EncryptionConfig) -> Self {
        self.encryption = Some(encryption);
        self
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
struct BackupDataPayload {
    records: Vec<BackupDataRecord>,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct BackupDataRecord {
    table: String,
    key: Bytes,
    value: Bytes,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct CommitLogPayload {
    records: Vec<CommitLogRecord>,
}

/// Takes a consistent full logical backup into a new directory under `dest_root`.
///
/// # Errors
/// Fails when the database has no local storage, storage reads fail, or the backup files cannot be written.
pub fn full_backup(
    database: &Database,
    dest_root: impl AsRef<Path>,
    config: &BackupConfig,
) -> Result<BackupReport, BackupError> {
    let started = Instant::now();
    let backup_id = new_id();
    let timeline_id = current_or_new_timeline(database)?;
    let backup_path = backup_dir(dest_root.as_ref(), &backup_id);
    fs::create_dir_all(&backup_path)?;

    let storage = database.local_storage().ok_or_else(|| {
        BackupError::Unsupported("sharded backup must be taken per shard".to_owned())
    })?;
    let read = storage.begin_read()?;
    let end_lsn = txn::current_txn_id_from(&read)?;
    let keyspaces = read_keyspaces_from(&read)?;
    let payload = read_full_payload(&read, &keyspaces)?;
    let keyspace_counts = keyspace_counts(&payload);
    let backup_keys = backup_key_provider(database, config)?;
    let data_entry = write_compressed_json(
        &backup_path,
        FULL_DATA_FILE,
        &payload,
        config,
        backup_keys.as_ref(),
    )?;

    let manifest = BackupManifest {
        format_version: FORMAT_VERSION,
        backup_id,
        kind: BackupKind::Full,
        parent_backup_id: None,
        timeline_id,
        profile: database.profile(),
        start_lsn: 0,
        end_lsn,
        taken_at: SystemTime::now(),
        engine_version: env!("CARGO_PKG_VERSION").to_owned(),
        files: vec![data_entry],
        keyspace_counts,
        encryption: backup_keys.as_ref().map(|_| BackupEncryptionMetadata {
            algorithm: "XChaCha20Poly1305".to_owned(),
        }),
    };
    write_manifest(&backup_path, &manifest)?;
    observability::record_backup("full", "ok", started.elapsed());

    Ok(BackupReport {
        manifest,
        path: backup_path,
    })
}

/// Takes an incremental logical backup after `parent_backup_dir`.
///
/// # Errors
/// Fails when the parent manifest is invalid, commit logs are missing, or files cannot be written.
pub fn incremental_backup(
    database: &Database,
    dest_root: impl AsRef<Path>,
    parent_backup_dir: impl AsRef<Path>,
    config: &BackupConfig,
) -> Result<BackupReport, BackupError> {
    let started = Instant::now();
    let parent = read_manifest(parent_backup_dir.as_ref())?;
    let storage = database.local_storage().ok_or_else(|| {
        BackupError::Unsupported("sharded backup must be taken per shard".to_owned())
    })?;
    let read = storage.begin_read()?;
    let end_lsn = txn::current_txn_id_from(&read)?;
    if end_lsn < parent.end_lsn {
        return Err(BackupError::Corruption(format!(
            "database lsn {end_lsn} is older than parent backup {}",
            parent.end_lsn
        )));
    }

    let records = read_commit_logs_from(&read, parent.end_lsn, end_lsn)?;
    let backup_id = new_id();
    let backup_path = backup_dir(dest_root.as_ref(), &backup_id);
    fs::create_dir_all(&backup_path)?;
    let payload = CommitLogPayload { records };
    let backup_keys = backup_key_provider(database, config)?;
    let wal_entry = write_compressed_json(
        &backup_path,
        WAL_DATA_FILE,
        &payload,
        config,
        backup_keys.as_ref(),
    )?;

    let manifest = BackupManifest {
        format_version: FORMAT_VERSION,
        backup_id,
        kind: BackupKind::Incremental,
        parent_backup_id: Some(parent.backup_id),
        timeline_id: parent.timeline_id,
        profile: database.profile(),
        start_lsn: parent.end_lsn,
        end_lsn,
        taken_at: SystemTime::now(),
        engine_version: env!("CARGO_PKG_VERSION").to_owned(),
        files: vec![wal_entry],
        keyspace_counts: BTreeMap::new(),
        encryption: backup_keys.as_ref().map(|_| BackupEncryptionMetadata {
            algorithm: "XChaCha20Poly1305".to_owned(),
        }),
    };
    write_manifest(&backup_path, &manifest)?;
    observability::record_backup("incremental", "ok", started.elapsed());

    Ok(BackupReport {
        manifest,
        path: backup_path,
    })
}

/// Restores a full or incremental backup chain into the database described by `config`.
///
/// Incremental commits are replayed through the same routine as live commits, so the restored
/// database gets the source's `__txn_mvcc` history, key versions, commit log and HLC.
///
/// # Errors
/// Fails when the backup chain is corrupt, the target is not empty, or storage rejects restored records.
/// The chain is corrupt when a link is broken (LSN gap or overlap with its parent, timeline
/// mismatch, parent directory holding a different `backup_id`, full backup with a parent, or a
/// cycle), when an incremental commit log skips an LSN or does not end at its manifest's
/// `end_lsn`, or when the full payload's LSN differs from its manifest. A `RestoreTarget::Lsn`
/// or `RestoreTarget::Time` earlier than the full backup fails with `BackupError::Unsupported`.
/// Chain-link and target errors are raised before the target database is created; payload
/// errors, as with checksum and decryption failures, leave an initialised target file behind.
pub fn restore_backup(
    backup_dir: impl AsRef<Path>,
    config: DbConfig,
    target: RestoreTarget,
) -> Result<RestoreReport, BackupError> {
    restore_backup_with_config(backup_dir, config, target, &RestoreConfig::default())
}

/// Restores a backup chain with explicit backup decryption settings.
///
/// # Errors
/// Fails when the backup chain is corrupt, encrypted without a key, the target is not empty,
/// or storage rejects restored records. Chain validation, commit-log contiguity and restore
/// targets earlier than the full backup are checked as described for [`restore_backup`].
pub fn restore_backup_with_config(
    backup_dir: impl AsRef<Path>,
    config: DbConfig,
    target: RestoreTarget,
    restore_config: &RestoreConfig,
) -> Result<RestoreReport, BackupError> {
    let started = Instant::now();
    ensure_restore_target_empty(&config)?;
    let chain = load_chain(backup_dir.as_ref())?;
    let first = chain
        .first()
        .ok_or_else(|| BackupError::Corruption("backup chain is empty".to_owned()))?;
    if first.manifest.kind != BackupKind::Full {
        return Err(BackupError::Corruption(
            "backup chain does not start with a full backup".to_owned(),
        ));
    }

    let backup_keys = restore_key_provider(restore_config)?;
    let target_lsn = resolve_target_lsn(&chain, &target, backup_keys.as_ref())?;
    let database = create_database(config)?;
    let storage = database.local_storage().ok_or_else(|| {
        BackupError::Unsupported("restore target must have local storage".to_owned())
    })?;
    let mut write = storage.begin_write()?;
    let full_payload = read_backup_payload::<BackupDataPayload>(
        &chain[0].path,
        required_file(&chain[0].manifest, FULL_DATA_FILE)?,
        backup_keys.as_ref(),
    )?;
    apply_full_payload(&mut write, &full_payload)?;
    let full_lsn = txn::current_txn_id_from(&write)?;
    if full_lsn != chain[0].manifest.end_lsn {
        return Err(BackupError::Corruption(format!(
            "full backup {} payload is at lsn {full_lsn} but its manifest ends at lsn {}",
            chain[0].manifest.backup_id, chain[0].manifest.end_lsn
        )));
    }

    let applied = replay_incrementals(&mut write, &chain, target_lsn, backup_keys.as_ref())?;

    write_restore_metadata(&mut write, &chain[0].manifest.timeline_id, target_lsn)?;
    write.commit()?;
    observability::record_restore("ok", started.elapsed());

    Ok(RestoreReport {
        restored_lsn: target_lsn,
        timeline_id: chain[0].manifest.timeline_id.clone(),
        applied_commits: applied,
    })
}

/// Verifies backup checksums and performs a temporary restore drill.
///
/// # Errors
/// Fails when any manifest, checksum, or restore step fails, including the chain-link,
/// commit-log contiguity and full payload LSN checks described for [`restore_backup`].
pub fn verify_backup(backup_dir: impl AsRef<Path>) -> Result<VerifyReport, BackupError> {
    verify_backup_with_config(backup_dir, &RestoreConfig::default())
}

/// Verifies backup checksums and performs a temporary restore drill with decryption settings.
///
/// # Errors
/// Fails when any manifest, checksum, decryption, or restore step fails, including the
/// chain-link, commit-log contiguity and full payload LSN checks described for
/// [`restore_backup`].
pub fn verify_backup_with_config(
    backup_dir: impl AsRef<Path>,
    restore_config: &RestoreConfig,
) -> Result<VerifyReport, BackupError> {
    let started = Instant::now();
    let chain = load_chain(backup_dir.as_ref())?;
    let selected = chain
        .last()
        .ok_or_else(|| BackupError::Corruption("backup chain is empty".to_owned()))?;
    let backup_keys = restore_key_provider(restore_config)?;
    let mut checked_files = 0_usize;
    for item in &chain {
        for file in &item.manifest.files {
            verify_file(&item.path, file, backup_keys.as_ref())?;
            checked_files = checked_files.saturating_add(1);
        }
    }

    let restore_path = std::env::temp_dir().join(format!("multidb-verify-{}.redb", new_id()));
    // The drill database holds decrypted data, so it is removed on every path.
    let drill = RemoveOnDrop(restore_path.clone());
    let config = DbConfig::on_disk(selected.manifest.profile, &restore_path);
    let report = restore_backup_with_config(
        &selected.path,
        config,
        RestoreTarget::Latest,
        restore_config,
    )?;
    drop(drill);
    observability::record_backup_verify("ok", started.elapsed());

    Ok(VerifyReport {
        manifest: selected.manifest.clone(),
        restored_lsn: report.restored_lsn,
        checked_files,
    })
}

/// Reads all backup manifests directly below `root`.
///
/// # Errors
/// Fails when the root cannot be read or a manifest is invalid.
pub fn list_backups(root: impl AsRef<Path>) -> Result<Vec<BackupManifest>, BackupError> {
    let mut manifests = Vec::new();
    if !root.as_ref().exists() {
        return Ok(manifests);
    }

    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let manifest_path = entry.path().join(MANIFEST_FILE);
        if manifest_path.exists() {
            manifests.push(read_manifest(&entry.path())?);
        }
    }
    manifests.sort_by_key(|manifest| manifest.taken_at);
    Ok(manifests)
}

/// Removes a temporary file when dropped, including on early error returns.
struct RemoveOnDrop(PathBuf);

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        if self.0.exists() && fs::remove_file(&self.0).is_err() {
            tracing::debug!("temporary verify database could not be removed");
        }
    }
}

#[derive(Clone)]
struct ChainItem {
    path: PathBuf,
    manifest: BackupManifest,
}

fn current_or_new_timeline(database: &Database) -> Result<TimelineId, BackupError> {
    match database.read(BACKUP_META_TABLE, KEY_TIMELINE_ID, ReadConsistency::Strong)? {
        Some(bytes) => {
            String::from_utf8(bytes).map_err(|error| BackupError::Serde(error.to_string()))
        }
        None => Ok(new_id()),
    }
}

fn new_id() -> String {
    Uuid::now_v7().to_string()
}

fn backup_dir(root: &Path, backup_id: &str) -> PathBuf {
    root.join(backup_id)
}

fn read_manifest(dir: &Path) -> Result<BackupManifest, BackupError> {
    let bytes = fs::read(dir.join(MANIFEST_FILE))?;
    let manifest = serde_json::from_slice::<BackupManifest>(&bytes)
        .map_err(|error| BackupError::Serde(error.to_string()))?;
    if manifest.format_version != FORMAT_VERSION {
        return Err(BackupError::Corruption(format!(
            "unsupported backup format {}",
            manifest.format_version
        )));
    }
    Ok(manifest)
}

fn write_manifest(dir: &Path, manifest: &BackupManifest) -> Result<(), BackupError> {
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|error| BackupError::Serde(error.to_string()))?;
    fileio::atomic_write(dir.join(MANIFEST_FILE), &bytes)?;
    Ok(())
}

fn read_keyspaces_from(txn: &impl ReadTransaction) -> Result<BTreeSet<String>, BackupError> {
    let mut keyspaces = default_keyspaces();
    for row in txn.range(txn::KEYSPACE_REGISTRY_TABLE, &[], &[])? {
        let (key, _) = row?;
        let name = String::from_utf8(key).map_err(|error| BackupError::Serde(error.to_string()))?;
        keyspaces.insert(name);
    }
    Ok(keyspaces)
}

fn default_keyspaces() -> BTreeSet<String> {
    [
        "__meta",
        "__catalog__",
        AUTHZ_TABLE,
        AUDIT_TABLE,
        REL_SCHEMA_TABLE,
        REL_ROWS_TABLE,
        REL_INDEX_TABLE,
        REL_COLUMNAR_SEGMENTS_TABLE,
        DOCUMENT_TABLE,
        INDEX_TABLE,
        VECTOR_TABLE,
        STATS_TABLE,
        PLANNER_FEEDBACK_TABLE,
        PLANNER_META_TABLE,
        txn::TXN_META_TABLE,
        txn::TXN_VERSIONS_TABLE,
        txn::COMMIT_LOG_TABLE,
        txn::KEYSPACE_REGISTRY_TABLE,
        "__raft_log",
        "__raft_vote",
        "__raft_state",
        "__raft_snapshot",
        AP_VERSIONS_TABLE,
        AP_HINTS_TABLE,
        AP_MERKLE_TABLE,
        HEALING_STATE_TABLE,
        HEALING_EVENTS_TABLE,
        SHARD_MIGRATIONS_TABLE,
        BACKUP_META_TABLE,
        CLOUD_SEGMENTS_TABLE,
        CLOUD_HIBERNATION_TABLE,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn read_full_payload(
    txn: &impl ReadTransaction,
    keyspaces: &BTreeSet<String>,
) -> Result<BackupDataPayload, BackupError> {
    let mut records = Vec::new();
    for table in keyspaces {
        for row in txn.range(table, &[], &[])? {
            let (key, value) = row?;
            records.push(BackupDataRecord {
                table: table.clone(),
                key,
                value,
            });
        }
    }
    records.sort_by(|left, right| {
        left.table
            .cmp(&right.table)
            .then_with(|| left.key.cmp(&right.key))
    });
    Ok(BackupDataPayload { records })
}

fn read_commit_logs_from(
    txn: &impl ReadTransaction,
    start: Lsn,
    end: Lsn,
) -> Result<Vec<CommitLogRecord>, BackupError> {
    if end <= start {
        return Ok(Vec::new());
    }

    let first = start
        .checked_add(1)
        .ok_or_else(|| BackupError::Corruption("start lsn overflow".to_owned()))?;
    let start_key = txn::commit_log_key(first);
    let end_key = end.checked_add(1).map(txn::commit_log_key);
    let end_bytes = end_key.as_ref().map_or(&[][..], <[u8; 8]>::as_slice);
    let mut expected = first;
    let mut records = Vec::new();
    for row in txn.range(txn::COMMIT_LOG_TABLE, &start_key, end_bytes)? {
        let (_, value) = row?;
        let record = txn::decode_commit_log_record(&value)?;
        if record.txn_id != expected {
            return Err(BackupError::MissingLogRange { start, end });
        }
        expected = expected
            .checked_add(1)
            .ok_or_else(|| BackupError::Corruption("commit log lsn overflow".to_owned()))?;
        records.push(record);
    }

    if expected != end.saturating_add(1) {
        return Err(BackupError::MissingLogRange { start, end });
    }
    Ok(records)
}

fn keyspace_counts(payload: &BackupDataPayload) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for record in &payload.records {
        *counts.entry(record.table.clone()).or_default() += 1;
    }
    counts
}

fn write_compressed_json<T: Serialize>(
    base: &Path,
    relative: &str,
    value: &T,
    config: &BackupConfig,
    encryption: Option<&ConfiguredKeyProvider>,
) -> Result<BackupFileEntry, BackupError> {
    let started = Instant::now();
    let json = serde_json::to_vec(value).map_err(|error| BackupError::Serde(error.to_string()))?;
    let logical_checksum = crc32fast::hash(&json);
    let compressed = zstd::stream::encode_all(Cursor::new(&json), 3)?;
    let (stored, encryption) = if let Some(provider) = encryption {
        encrypt_backup_file(provider, relative, &compressed)?
    } else {
        (compressed, None)
    };
    let checksum = crc32fast::hash(&stored);
    let path = base.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fileio::atomic_write(&path, &stored)?;
    throttle(config, stored.len() as u64, started);
    let bytes =
        u64::try_from(stored.len()).map_err(|error| BackupError::Serde(error.to_string()))?;
    Ok(BackupFileEntry {
        path: relative.replace('\\', "/"),
        bytes,
        checksum,
        logical_checksum,
        encryption,
    })
}

fn read_backup_payload<T: DeserializeOwned>(
    base: &Path,
    entry: &BackupFileEntry,
    encryption: Option<&ConfiguredKeyProvider>,
) -> Result<T, BackupError> {
    let stored = fs::read(base.join(&entry.path))?;
    if crc32fast::hash(&stored) != entry.checksum {
        return Err(BackupError::Corruption(format!(
            "compressed checksum mismatch for {}",
            entry.path
        )));
    }
    let compressed = if let Some(metadata) = &entry.encryption {
        let provider = encryption.ok_or_else(|| {
            BackupError::Unsupported(format!(
                "backup file {} is encrypted and requires a restore key",
                entry.path
            ))
        })?;
        decrypt_backup_file(provider, &entry.path, metadata, &stored)?
    } else {
        stored
    };
    let json = zstd::stream::decode_all(Cursor::new(compressed))?;
    if crc32fast::hash(&json) != entry.logical_checksum {
        return Err(BackupError::Corruption(format!(
            "logical checksum mismatch for {}",
            entry.path
        )));
    }
    serde_json::from_slice(&json).map_err(|error| BackupError::Serde(error.to_string()))
}

fn verify_file(
    base: &Path,
    entry: &BackupFileEntry,
    encryption: Option<&ConfiguredKeyProvider>,
) -> Result<(), BackupError> {
    let _: serde_json::Value = read_backup_payload(base, entry, encryption)?;
    Ok(())
}

fn backup_key_provider(
    database: &Database,
    config: &BackupConfig,
) -> Result<Option<ConfiguredKeyProvider>, BackupError> {
    config
        .encryption
        .as_ref()
        .or_else(|| database.encryption_config())
        .map(configured_key_provider)
        .transpose()
}

fn restore_key_provider(
    config: &RestoreConfig,
) -> Result<Option<ConfiguredKeyProvider>, BackupError> {
    config
        .encryption
        .as_ref()
        .map(configured_key_provider)
        .transpose()
}

fn configured_key_provider(
    encryption: &EncryptionConfig,
) -> Result<ConfiguredKeyProvider, BackupError> {
    Ok(match &encryption.mode {
        EncryptionMode::LegacyFile => ConfiguredKeyProvider::file_key(encryption.key_path.clone()),
        EncryptionMode::LocalEnvelope {
            keyring_path,
            kek_path,
        } => ConfiguredKeyProvider::local_envelope(keyring_path.clone(), kek_path.clone())?,
    })
}

fn encrypt_backup_file(
    provider: &ConfiguredKeyProvider,
    path: &str,
    compressed: &[u8],
) -> Result<(Bytes, Option<BackupFileEncryption>), BackupError> {
    let key = provider.current_key()?;
    let mut nonce = [0_u8; 24];
    getrandom::fill(&mut nonce)
        .map_err(|error| BackupError::Storage(StorageError::Backend(error.to_string())))?;
    let aad = backup_file_aad(path, key.id());
    let ciphertext = XChaCha20Poly1305::new_from_slice(key.as_slice_for_backup())
        .map_err(|error| BackupError::Storage(StorageError::Backend(error.to_string())))?
        .encrypt(
            &XNonce::try_from(nonce.as_slice())
                .map_err(|error| BackupError::Storage(StorageError::Backend(error.to_string())))?,
            Payload {
                msg: compressed,
                aad: &aad,
            },
        )
        .map_err(|error| BackupError::Storage(StorageError::Backend(error.to_string())))?;
    Ok((
        ciphertext,
        Some(BackupFileEncryption {
            algorithm: "XChaCha20Poly1305".to_owned(),
            key_id: key.id(),
            nonce: nonce.to_vec(),
        }),
    ))
}

fn decrypt_backup_file(
    provider: &ConfiguredKeyProvider,
    path: &str,
    metadata: &BackupFileEncryption,
    ciphertext: &[u8],
) -> Result<Bytes, BackupError> {
    if metadata.algorithm != "XChaCha20Poly1305" {
        return Err(BackupError::Corruption(format!(
            "unsupported backup encryption algorithm {}",
            metadata.algorithm
        )));
    }
    let key = provider.key_by_id(metadata.key_id)?;
    let aad = backup_file_aad(path, metadata.key_id);
    let nonce = XNonce::try_from(metadata.nonce.as_slice())
        .map_err(|error| BackupError::Corruption(error.to_string()))?;
    XChaCha20Poly1305::new_from_slice(key.as_slice_for_backup())
        .map_err(|error| BackupError::Storage(StorageError::Backend(error.to_string())))?
        .decrypt(
            &nonce,
            Payload {
                msg: ciphertext,
                aad: &aad,
            },
        )
        .map_err(|_| BackupError::Corruption("backup authentication failed".to_owned()))
}

fn backup_file_aad(path: &str, key_id: u64) -> Bytes {
    let mut aad = Vec::with_capacity(32 + path.len());
    aad.extend_from_slice(b"multidb backup payload v1");
    aad.extend_from_slice(&key_id.to_be_bytes());
    aad.extend_from_slice(path.as_bytes());
    aad
}

fn throttle(config: &BackupConfig, bytes: u64, started: Instant) {
    let Some(limit) = config.max_bytes_per_second else {
        return;
    };
    if limit == 0 {
        return;
    }

    let expected_nanos = u128::from(bytes)
        .saturating_mul(1_000_000_000)
        .checked_div(u128::from(limit))
        .unwrap_or(u128::MAX);
    let nanos = u64::try_from(expected_nanos).unwrap_or(u64::MAX);
    let expected = Duration::from_nanos(nanos);
    let elapsed = started.elapsed();
    if let Some(delay) = expected.checked_sub(elapsed) {
        thread::sleep(delay);
    }
}

fn required_file<'a>(
    manifest: &'a BackupManifest,
    path: &str,
) -> Result<&'a BackupFileEntry, BackupError> {
    manifest
        .files
        .iter()
        .find(|entry| entry.path == path)
        .ok_or_else(|| BackupError::Corruption(format!("missing backup file {path}")))
}

/// Rejects parent ids that are not a single plain directory name, so a manifest cannot point
/// the chain outside the backup root.
fn validate_parent_backup_id(child_id: &str, parent_id: &str) -> Result<(), BackupError> {
    let plain = !parent_id.is_empty()
        && parent_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_');
    if plain {
        Ok(())
    } else {
        Err(BackupError::Corruption(format!(
            "backup {child_id} references an invalid parent id"
        )))
    }
}

fn load_chain(selected_dir: &Path) -> Result<Vec<ChainItem>, BackupError> {
    let mut chain = Vec::new();
    let mut current_path = selected_dir.to_path_buf();
    let mut manifest = read_manifest(&current_path)?;
    let mut seen = BTreeSet::from([manifest.backup_id.clone()]);
    while let Some(parent_id) = manifest.parent_backup_id.clone() {
        if manifest.kind != BackupKind::Incremental {
            return Err(BackupError::Corruption(format!(
                "full backup {} must not reference a parent",
                manifest.backup_id
            )));
        }
        if seen.contains(&parent_id) {
            return Err(BackupError::Corruption(format!(
                "backup chain cycles back to {parent_id}"
            )));
        }
        validate_parent_backup_id(&manifest.backup_id, &parent_id)?;
        let Some(root) = current_path.parent() else {
            return Err(BackupError::MissingParent(parent_id));
        };
        let parent_path = root.join(&parent_id);
        if !parent_path.exists() {
            return Err(BackupError::MissingParent(parent_id));
        }
        let parent = read_manifest(&parent_path)?;
        validate_chain_link(&manifest, &parent_id, &parent)?;
        seen.insert(parent_id);
        chain.push(ChainItem {
            path: current_path,
            manifest,
        });
        current_path = parent_path;
        manifest = parent;
    }
    chain.push(ChainItem {
        path: current_path,
        manifest,
    });

    chain.reverse();
    Ok(chain)
}

fn validate_chain_link(
    child: &BackupManifest,
    parent_id: &str,
    parent: &BackupManifest,
) -> Result<(), BackupError> {
    if parent.backup_id != parent_id {
        return Err(BackupError::Corruption(format!(
            "backup {} references parent {parent_id} but that directory holds backup {}",
            child.backup_id, parent.backup_id
        )));
    }
    if parent.timeline_id != child.timeline_id {
        return Err(BackupError::Corruption(format!(
            "backup {} is on timeline {} but parent {parent_id} is on timeline {}",
            child.backup_id, child.timeline_id, parent.timeline_id
        )));
    }
    if child.start_lsn != parent.end_lsn {
        return Err(BackupError::Corruption(format!(
            "backup {} starts at lsn {} but parent {parent_id} ends at lsn {}",
            child.backup_id, child.start_lsn, parent.end_lsn
        )));
    }
    if child.end_lsn < child.start_lsn {
        return Err(BackupError::Corruption(format!(
            "backup {} ends at lsn {} before it starts at lsn {}",
            child.backup_id, child.end_lsn, child.start_lsn
        )));
    }
    Ok(())
}

fn replay_incrementals(
    write: &mut impl WriteTransaction,
    chain: &[ChainItem],
    target_lsn: Lsn,
    encryption: Option<&ConfiguredKeyProvider>,
) -> Result<usize, BackupError> {
    let mut applied = 0_usize;
    let mut last_lsn = chain
        .first()
        .ok_or_else(|| BackupError::Corruption("backup chain is empty".to_owned()))?
        .manifest
        .end_lsn;
    for item in chain.iter().skip(1) {
        let payload = read_backup_payload::<CommitLogPayload>(
            &item.path,
            required_file(&item.manifest, WAL_DATA_FILE)?,
            encryption,
        )?;
        for record in payload.records {
            let expected = last_lsn
                .checked_add(1)
                .ok_or_else(|| BackupError::Corruption("commit log lsn overflow".to_owned()))?;
            if record.txn_id != expected {
                return Err(BackupError::Corruption(format!(
                    "backup {} has commit lsn {} where {expected} was expected",
                    item.manifest.backup_id, record.txn_id
                )));
            }
            last_lsn = record.txn_id;
            if record.txn_id > target_lsn {
                continue;
            }
            txn::apply_commit_record_in_txn(write, &record)?;
            applied = applied.saturating_add(1);
        }
        if last_lsn != item.manifest.end_lsn {
            return Err(BackupError::Corruption(format!(
                "backup {} commit log ends at lsn {last_lsn} but its manifest ends at lsn {}",
                item.manifest.backup_id, item.manifest.end_lsn
            )));
        }
    }
    Ok(applied)
}

fn resolve_target_lsn(
    chain: &[ChainItem],
    target: &RestoreTarget,
    encryption: Option<&ConfiguredKeyProvider>,
) -> Result<Lsn, BackupError> {
    let first = chain
        .first()
        .ok_or_else(|| BackupError::Corruption("backup chain is empty".to_owned()))?;
    let last = chain
        .last()
        .ok_or_else(|| BackupError::Corruption("backup chain is empty".to_owned()))?;

    match target {
        RestoreTarget::Latest => Ok(last.manifest.end_lsn),
        RestoreTarget::Lsn(lsn) => {
            if *lsn < first.manifest.end_lsn {
                return Err(BackupError::Unsupported(
                    "cannot restore before the full backup snapshot lsn".to_owned(),
                ));
            }
            if *lsn > last.manifest.end_lsn {
                return Err(BackupError::Corruption(format!(
                    "target lsn {lsn} is newer than backup end lsn {}",
                    last.manifest.end_lsn
                )));
            }
            Ok(*lsn)
        }
        RestoreTarget::Time(time) => target_lsn_for_time(chain, *time, encryption),
    }
}

fn target_lsn_for_time(
    chain: &[ChainItem],
    time: SystemTime,
    encryption: Option<&ConfiguredKeyProvider>,
) -> Result<Lsn, BackupError> {
    let first = chain
        .first()
        .ok_or_else(|| BackupError::Corruption("backup chain is empty".to_owned()))?;
    if time < first.manifest.taken_at {
        return Err(BackupError::Unsupported(
            "cannot restore before the full backup snapshot time".to_owned(),
        ));
    }

    let mut selected = first.manifest.end_lsn;

    for item in chain.iter().skip(1) {
        let payload = read_backup_payload::<CommitLogPayload>(
            &item.path,
            required_file(&item.manifest, WAL_DATA_FILE)?,
            encryption,
        )?;
        for record in payload.records {
            if record.committed_at <= time {
                selected = record.txn_id;
            }
        }
    }
    Ok(selected)
}

fn ensure_restore_target_empty(config: &DbConfig) -> Result<(), BackupError> {
    let Some(path) = &config.path else {
        return Ok(());
    };
    if !path.exists() {
        return Ok(());
    }
    let metadata = fs::metadata(path)?;
    if metadata.len() == 0 {
        return Ok(());
    }
    Err(BackupError::Unsupported(
        "restore target path must be empty or absent".to_owned(),
    ))
}

fn apply_full_payload(
    txn: &mut impl WriteTransaction,
    payload: &BackupDataPayload,
) -> Result<(), BackupError> {
    for record in &payload.records {
        txn.put(&record.table, &record.key, &record.value)?;
    }
    Ok(())
}

fn write_restore_metadata(
    txn: &mut impl WriteTransaction,
    timeline_id: &str,
    lsn: Lsn,
) -> Result<(), BackupError> {
    txn.put(
        txn::TXN_META_TABLE,
        txn::CURRENT_TXN_ID_KEY,
        &txn::encode_txn_id(lsn),
    )?;
    txn.put(BACKUP_META_TABLE, KEY_TIMELINE_ID, timeline_id.as_bytes())?;
    txn.put(
        BACKUP_META_TABLE,
        KEY_RECOVERY_LSN,
        &txn::encode_txn_id(lsn),
    )?;
    txn.put(
        txn::KEYSPACE_REGISTRY_TABLE,
        BACKUP_META_TABLE.as_bytes(),
        KEYSPACE_PRESENT_VALUE,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::Path;
    use std::time::{Duration, SystemTime};

    use tempfile::tempdir;

    use super::{
        BackupConfig, BackupError, BackupManifest, BackupReport, CommitLogPayload, Lsn,
        RestoreConfig, RestoreTarget, WAL_DATA_FILE, full_backup, incremental_backup, list_backups,
        new_id, read_backup_payload, read_manifest, required_file, restore_backup,
        restore_backup_with_config, verify_backup, verify_backup_with_config,
        write_compressed_json, write_manifest,
    };
    use crate::db::{
        Database, DbConfig, DbError, EncryptionConfig, OperationalConfig, Profile, create_database,
        create_database_with_ops, open_database,
    };
    use crate::model::{
        CollectionId, DOCUMENT_TABLE, DocumentId, FieldPath, INDEX_TABLE, IndexId, IndexSpec,
        Predicate, Value,
    };
    use crate::phase30::{HLC_LOCAL_KEY, HLC_TABLE, HlcTimestamp};
    use crate::query::{ColumnDef, ColumnType, DocField, REL_ROWS_TABLE, SqlOutput, TableSchema};
    use crate::repl::{Op, ReadConsistency, ReplError, Replication};
    use crate::storage::Bytes;
    use crate::temporal::TemporalPoint;
    use crate::txn;

    #[test]
    fn full_backup_restore_round_trips_redb() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let db_path = temp.path().join("source.redb");
        let database = create_database(DbConfig::on_disk(Profile::Balanced, &db_path))?;
        database.propose(Op::Put {
            table: "user_data".to_owned(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        })?;

        let backup = full_backup(
            &database,
            temp.path().join("backups"),
            &BackupConfig::default(),
        )?;
        let restore_path = temp.path().join("restore.redb");
        let report = restore_backup(
            &backup.path,
            DbConfig::on_disk(Profile::Balanced, &restore_path),
            RestoreTarget::Latest,
        )?;
        let restored = open_database(DbConfig::on_disk(Profile::Balanced, &restore_path))?;

        assert_eq!(
            restored.read("user_data", b"k", ReadConsistency::Strong)?,
            Some(b"v".to_vec())
        );
        assert_eq!(report.restored_lsn, backup.manifest.end_lsn);
        Ok(())
    }

    #[test]
    fn encrypted_database_backup_requires_restore_key() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let db_path = temp.path().join("source.redb");
        let key_path = temp.path().join("key.bin");
        std::fs::write(&key_path, [7_u8; 32])?;
        let encryption = EncryptionConfig::file_key(&key_path);
        let database = create_database_with_ops(
            DbConfig::on_disk(Profile::Balanced, &db_path),
            OperationalConfig::new().with_encryption(encryption.clone()),
        )?;
        database.propose(Op::Put {
            table: "secrets".to_owned(),
            key: b"k".to_vec(),
            value: b"secret-value".to_vec(),
        })?;

        let backup = full_backup(
            &database,
            temp.path().join("backups"),
            &BackupConfig::default(),
        )?;

        assert!(backup.manifest.encryption.is_some());
        assert!(
            backup
                .manifest
                .files
                .iter()
                .all(|entry| entry.encryption.is_some())
        );
        let stored = std::fs::read(backup.path.join("data").join("full.json.zst"))?;
        assert!(
            !stored
                .windows(b"secret-value".len())
                .any(|window| window == b"secret-value")
        );
        assert!(
            restore_backup(
                &backup.path,
                DbConfig::on_disk(
                    Profile::Balanced,
                    temp.path().join("restore-without-key.redb")
                ),
                RestoreTarget::Latest,
            )
            .is_err()
        );

        let restore_path = temp.path().join("restore.redb");
        let restore_config = RestoreConfig::default().with_encryption(encryption);
        restore_backup_with_config(
            &backup.path,
            DbConfig::on_disk(Profile::Balanced, &restore_path),
            RestoreTarget::Latest,
            &restore_config,
        )?;
        let restored = open_database(DbConfig::on_disk(Profile::Balanced, &restore_path))?;
        assert_eq!(
            restored.read("secrets", b"k", ReadConsistency::Strong)?,
            Some(b"secret-value".to_vec())
        );
        verify_backup_with_config(&backup.path, &restore_config)?;
        Ok(())
    }

    #[test]
    fn incremental_restore_to_lsn_skips_later_commits() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let db_path = temp.path().join("source.redb");
        let database = create_database(DbConfig::on_disk(Profile::Balanced, &db_path))?;
        database.propose(Op::Put {
            table: "items".to_owned(),
            key: b"a".to_vec(),
            value: b"1".to_vec(),
        })?;
        let root = temp.path().join("backups");
        let full = full_backup(&database, &root, &BackupConfig::default())?;
        database.propose(Op::Put {
            table: "items".to_owned(),
            key: b"b".to_vec(),
            value: b"2".to_vec(),
        })?;
        let incremental =
            incremental_backup(&database, &root, &full.path, &BackupConfig::default())?;

        let restore_path = temp.path().join("restore-to-full.redb");
        restore_backup(
            &incremental.path,
            DbConfig::on_disk(Profile::Balanced, &restore_path),
            RestoreTarget::Lsn(full.manifest.end_lsn),
        )?;
        let restored = open_database(DbConfig::on_disk(Profile::Balanced, &restore_path))?;

        assert_eq!(
            restored.read("items", b"a", ReadConsistency::Strong)?,
            Some(b"1".to_vec())
        );
        assert_eq!(restored.read("items", b"b", ReadConsistency::Strong)?, None);
        Ok(())
    }

    #[test]
    fn verify_detects_bit_flip() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let db_path = temp.path().join("source.redb");
        let database = create_database(DbConfig::on_disk(Profile::Balanced, &db_path))?;
        database.propose(Op::Put {
            table: "t".to_owned(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        })?;
        let backup = full_backup(
            &database,
            temp.path().join("backups"),
            &BackupConfig::default(),
        )?;
        let data_path = backup.path.join("data").join("full.json.zst");
        let mut file = OpenOptions::new().read(true).write(true).open(data_path)?;
        let mut first = [0_u8; 1];
        file.read_exact(&mut first)?;
        file.seek(SeekFrom::Start(0))?;
        first[0] ^= 0xFF;
        file.write_all(&first)?;

        assert!(verify_backup(&backup.path).is_err());
        Ok(())
    }

    #[test]
    fn list_backups_returns_manifests_sorted() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let db_path = temp.path().join("source.redb");
        let database = create_database(DbConfig::on_disk(Profile::Balanced, &db_path))?;
        database.propose(Op::Put {
            table: "t".to_owned(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        })?;
        let root = temp.path().join("backups");
        let backup = full_backup(&database, &root, &BackupConfig::default())?;

        let manifests = list_backups(&root)?;

        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].backup_id, backup.manifest.backup_id);
        Ok(())
    }

    struct MvccFixture {
        source: Database,
        full: BackupReport,
        incremental: BackupReport,
        doc_id: DocumentId,
        lsn_doc: Lsn,
        lsn_update: Lsn,
        lsn_delete: Lsn,
    }

    struct ChainFixture {
        full: BackupReport,
        first: BackupReport,
        second: BackupReport,
    }

    fn on_disk(path: impl AsRef<Path>) -> DbConfig {
        DbConfig::on_disk(Profile::Balanced, path.as_ref())
    }

    fn account_schema() -> TableSchema {
        TableSchema::new(
            vec![
                ColumnDef::new("id", ColumnType::Int, false),
                ColumnDef::new("name", ColumnType::Str, false),
                ColumnDef::new("age", ColumnType::Int, false),
            ],
            0,
        )
    }

    fn account(id: i64, name: &str, age: i64) -> Vec<Value> {
        vec![Value::Int(id), Value::Str(name.to_owned()), Value::Int(age)]
    }

    fn city_path() -> FieldPath {
        FieldPath::new(["city"])
    }

    fn city_doc(city: &str) -> Value {
        Value::Object([("city".to_owned(), Value::Str(city.to_owned()))].into())
    }

    fn city_eq(city: &str) -> Predicate {
        Predicate::Eq {
            path: city_path(),
            value: Value::Str(city.to_owned()),
        }
    }

    fn put_item(key: &[u8], value: &[u8]) -> Op {
        Op::Put {
            table: "items".to_owned(),
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    fn source_lsn(database: &Database) -> Result<Lsn, Box<dyn Error>> {
        let storage = database
            .local_storage()
            .ok_or("source has no local storage")?;
        Ok(txn::current_txn_id(storage)?)
    }

    fn table_rows(database: &Database, table: &str) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        database.range(table, &[], &[], ReadConsistency::Strong)
    }

    fn mvcc_version(key: &[u8]) -> Result<Lsn, Box<dyn Error>> {
        let split = key
            .len()
            .checked_sub(8)
            .ok_or("mvcc key is missing its version")?;
        Ok(txn::decode_txn_id(&key[split..])?)
    }

    fn edit_manifest(
        dir: &Path,
        edit: impl FnOnce(&mut BackupManifest),
    ) -> Result<(), BackupError> {
        let mut manifest = read_manifest(dir)?;
        edit(&mut manifest);
        write_manifest(dir, &manifest)
    }

    /// Full backup, then a document update, a row update and a row delete in one incremental.
    fn mvcc_restore_fixture(dir: &Path) -> Result<MvccFixture, Box<dyn Error>> {
        let mut source = create_database(on_disk(dir.join("source.redb")))?;
        let accounts = source.create_table("accounts", Some(account_schema()), Vec::new())?;
        accounts.insert(account(1, "Ada", 37))?;
        accounts.insert(account(2, "Grace", 85))?;
        let doc_id = source
            .create_collection(
                "profiles",
                CollectionId::new(10),
                vec![DocField::path("city", city_path(), ColumnType::Str)],
                vec![IndexSpec::new(IndexId::new(1), city_path())],
            )?
            .insert(&city_doc("Warsaw"))?;
        let root = dir.join("backups");
        let full = full_backup(&source, &root, &BackupConfig::default())?;

        source
            .collection("profiles")?
            .update(doc_id, &city_doc("Krakow"))?;
        let lsn_doc = source_lsn(&source)?;
        accounts.update(account(1, "Ada", 38))?;
        let lsn_update = source_lsn(&source)?;
        accounts.delete(&Value::Int(2))?;
        let lsn_delete = source_lsn(&source)?;
        let incremental = incremental_backup(&source, &root, &full.path, &BackupConfig::default())?;

        Ok(MvccFixture {
            source,
            full,
            incremental,
            doc_id,
            lsn_doc,
            lsn_update,
            lsn_delete,
        })
    }

    /// Full backup, an incremental with two commits, then an incremental with one commit.
    fn chain_fixture(dir: &Path) -> Result<ChainFixture, Box<dyn Error>> {
        let database = create_database(on_disk(dir.join("source.redb")))?;
        let root = dir.join("backups");
        database.propose(put_item(b"a", b"1"))?;
        let full = full_backup(&database, &root, &BackupConfig::default())?;
        database.propose(put_item(b"b", b"2"))?;
        database.propose(put_item(b"c", b"3"))?;
        let first = incremental_backup(&database, &root, &full.path, &BackupConfig::default())?;
        database.propose(put_item(b"d", b"4"))?;
        let second = incremental_backup(&database, &root, &first.path, &BackupConfig::default())?;
        Ok(ChainFixture {
            full,
            first,
            second,
        })
    }

    fn assert_chain_rejected(
        selected: &Path,
        restore_path: &Path,
        phrase: &str,
    ) -> Result<(), Box<dyn Error>> {
        match restore_backup(selected, on_disk(restore_path), RestoreTarget::Latest) {
            Err(BackupError::Corruption(message)) => {
                assert!(message.contains(phrase), "unexpected message: {message}");
            }
            other => {
                return Err(format!("expected corruption about {phrase:?}, got {other:?}").into());
            }
        }
        assert!(verify_backup(selected).is_err());
        assert!(!restore_path.exists());
        Ok(())
    }

    async fn as_of_rows(
        database: &Database,
        sql: &str,
        lsn: Lsn,
    ) -> Result<Vec<Vec<Value>>, Box<dyn Error>> {
        match database.query_as_of(sql, TemporalPoint::Lsn(lsn)).await? {
            SqlOutput::Rows(rows) => Ok(rows.rows),
            SqlOutput::AffectedRows(_) => Err("AS OF query did not return rows".into()),
        }
    }

    fn assert_restored_transactions(
        restored: &Database,
        fixture: &MvccFixture,
    ) -> Result<(), Box<dyn Error>> {
        let reader = restored.begin_transaction()?;
        assert_eq!(
            reader.get_document("profiles", fixture.doc_id)?,
            Some(city_doc("Krakow"))
        );
        assert_eq!(
            reader.get_row("accounts", &Value::Int(1))?,
            Some(account(1, "Ada", 38))
        );
        assert_eq!(reader.get_row("accounts", &Value::Int(2))?, None);
        reader.rollback();

        restored.transaction(|txn| txn.insert_row("accounts", account(2, "Grace", 86)))?;
        assert_eq!(
            restored.table("accounts")?.get(&Value::Int(2))?,
            Some(account(2, "Grace", 86))
        );

        restored.transaction(|txn| {
            let row = txn.get_row("accounts", &Value::Int(1))?;
            let Some(Value::Int(age)) = row.as_ref().and_then(|row| row.get(2)) else {
                return Err(DbError::TransactionAborted("missing age".to_owned()));
            };
            txn.update_row("accounts", account(1, "Ada", age + 1))
        })?;
        assert_eq!(
            restored.table("accounts")?.get(&Value::Int(1))?,
            Some(account(1, "Ada", 39))
        );

        assert_eq!(table_rows(restored, INDEX_TABLE)?.len(), 1);
        restored.transaction(|txn| {
            txn.update_document("profiles", fixture.doc_id, &city_doc("Gdansk"))
        })?;
        assert_eq!(table_rows(restored, INDEX_TABLE)?.len(), 1);
        let found = restored.find("profiles", &city_eq("Gdansk"))?;
        assert_eq!(
            found
                .documents
                .iter()
                .map(|(id, _)| *id)
                .collect::<Vec<_>>(),
            vec![fixture.doc_id]
        );
        assert!(
            restored
                .find("profiles", &city_eq("Krakow"))?
                .documents
                .is_empty(),
            "expected an empty value, got {:?}",
            restored.find("profiles", &city_eq("Krakow"))?.documents
        );

        let mut first_writer = restored.begin_transaction()?;
        let mut second_writer = restored.begin_transaction()?;
        first_writer.update_row("accounts", account(1, "Ada", 40))?;
        second_writer.update_row("accounts", account(1, "Ada", 41))?;
        first_writer.commit()?;
        let error = second_writer
            .commit()
            .err()
            .ok_or("concurrent update committed")?;
        assert!(error.is_conflict(), "unexpected error: {error}");
        Ok(())
    }

    #[test]
    fn incremental_restore_rebuilds_mvcc_history() -> Result<(), Box<dyn Error>> {
        let temp = tempdir()?;
        let fixture = mvcc_restore_fixture(temp.path())?;
        let restore_path = temp.path().join("restore.redb");
        let report = restore_backup(
            &fixture.incremental.path,
            on_disk(&restore_path),
            RestoreTarget::Latest,
        )?;
        assert_eq!(report.restored_lsn, fixture.incremental.manifest.end_lsn);
        let restored = open_database(on_disk(&restore_path))?;

        for table in [
            txn::TXN_MVCC_TABLE,
            txn::TXN_VERSIONS_TABLE,
            txn::COMMIT_LOG_TABLE,
            HLC_TABLE,
            REL_ROWS_TABLE,
            DOCUMENT_TABLE,
            INDEX_TABLE,
        ] {
            assert_eq!(
                table_rows(&restored, table)?,
                table_rows(&fixture.source, table)?,
                "{table} differs between source and restore"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn incremental_restore_serves_as_of_and_transactions() -> Result<(), Box<dyn Error>> {
        let temp = tempdir()?;
        let fixture = mvcc_restore_fixture(temp.path())?;
        let restore_path = temp.path().join("restore.redb");
        restore_backup(
            &fixture.incremental.path,
            on_disk(&restore_path),
            RestoreTarget::Latest,
        )?;
        let restored = open_database(on_disk(&restore_path))?;

        let age_sql = "SELECT age FROM accounts WHERE id = 1";
        for (lsn, age) in [
            (fixture.full.manifest.end_lsn, 37),
            (fixture.lsn_doc, 37),
            (fixture.lsn_update, 38),
        ] {
            assert_eq!(
                as_of_rows(&restored, age_sql, lsn).await?,
                vec![vec![Value::Int(age)]],
                "age as of lsn {lsn}"
            );
        }
        let grace_sql = "SELECT id FROM accounts WHERE id = 2";
        assert_eq!(
            as_of_rows(&restored, grace_sql, fixture.lsn_update).await?,
            vec![vec![Value::Int(2)]]
        );
        assert!(
            as_of_rows(&restored, grace_sql, fixture.lsn_delete)
                .await?
                .is_empty(),
            "expected an empty value, got {:?}",
            as_of_rows(&restored, grace_sql, fixture.lsn_delete).await?
        );

        assert_restored_transactions(&restored, &fixture)
    }

    #[test]
    fn incremental_restore_to_intermediate_lsn_rebuilds_mvcc() -> Result<(), Box<dyn Error>> {
        let temp = tempdir()?;
        let fixture = mvcc_restore_fixture(temp.path())?;
        let restore_path = temp.path().join("restore.redb");
        let report = restore_backup(
            &fixture.incremental.path,
            on_disk(&restore_path),
            RestoreTarget::Lsn(fixture.lsn_update),
        )?;
        assert_eq!(report.restored_lsn, fixture.lsn_update);
        assert_eq!(
            report.applied_commits,
            usize::try_from(fixture.lsn_update - fixture.full.manifest.end_lsn)?
        );
        let restored = open_database(on_disk(&restore_path))?;

        let reader = restored.begin_transaction()?;
        assert_eq!(reader.snapshot_id(), fixture.lsn_update);
        assert_eq!(
            reader.get_row("accounts", &Value::Int(1))?,
            Some(account(1, "Ada", 38))
        );
        assert_eq!(
            reader.get_row("accounts", &Value::Int(2))?,
            Some(account(2, "Grace", 85))
        );
        reader.rollback();

        let record = fixture
            .source
            .read(
                txn::COMMIT_LOG_TABLE,
                &txn::commit_log_key(fixture.lsn_update),
                ReadConsistency::Strong,
            )?
            .ok_or("missing source commit record")?;
        let source_hlc = txn::decode_commit_log_record(&record)?
            .hlc
            .ok_or("source commit has no HLC")?;
        let restored_hlc = restored
            .read(HLC_TABLE, HLC_LOCAL_KEY, ReadConsistency::Strong)?
            .ok_or("restored database has no HLC")?;
        assert_eq!(
            postcard::from_bytes::<HlcTimestamp>(&restored_hlc)?,
            source_hlc
        );

        let mut expected_mvcc = Vec::new();
        for (key, value) in table_rows(&fixture.source, txn::TXN_MVCC_TABLE)? {
            if mvcc_version(&key)? <= fixture.lsn_update {
                expected_mvcc.push((key, value));
            }
        }
        assert_eq!(table_rows(&restored, txn::TXN_MVCC_TABLE)?, expected_mvcc);
        Ok(())
    }

    #[test]
    fn restore_and_verify_accept_valid_two_incremental_chain() -> Result<(), Box<dyn Error>> {
        let temp = tempdir()?;
        let chain = chain_fixture(temp.path())?;

        let latest = restore_backup(
            &chain.second.path,
            on_disk(temp.path().join("latest.redb")),
            RestoreTarget::Latest,
        )?;
        assert_eq!(latest.restored_lsn, chain.second.manifest.end_lsn);
        let middle = restore_backup(
            &chain.second.path,
            on_disk(temp.path().join("middle.redb")),
            RestoreTarget::Lsn(chain.first.manifest.end_lsn),
        )?;
        assert_eq!(middle.restored_lsn, chain.first.manifest.end_lsn);
        let verified = verify_backup(&chain.second.path)?;
        assert_eq!(verified.restored_lsn, chain.second.manifest.end_lsn);
        Ok(())
    }

    #[test]
    fn restore_rejects_chain_with_lsn_gap() -> Result<(), Box<dyn Error>> {
        let temp = tempdir()?;
        let chain = chain_fixture(temp.path())?;
        let gap_start = chain.first.manifest.end_lsn + 1;
        edit_manifest(&chain.second.path, |manifest| {
            manifest.start_lsn = gap_start;
        })?;
        assert_chain_rejected(
            &chain.second.path,
            &temp.path().join("restore.redb"),
            "starts at lsn",
        )
    }

    #[test]
    fn restore_rejects_chain_with_wrong_timeline() -> Result<(), Box<dyn Error>> {
        let temp = tempdir()?;
        let chain = chain_fixture(temp.path())?;
        edit_manifest(&chain.second.path, |manifest| {
            manifest.timeline_id = new_id();
        })?;
        assert_chain_rejected(
            &chain.second.path,
            &temp.path().join("restore.redb"),
            "timeline",
        )
    }

    #[test]
    fn restore_rejects_parent_with_mismatched_backup_id() -> Result<(), Box<dyn Error>> {
        let temp = tempdir()?;
        let chain = chain_fixture(temp.path())?;
        edit_manifest(&chain.first.path, |manifest| {
            manifest.backup_id = new_id();
        })?;
        assert_chain_rejected(
            &chain.second.path,
            &temp.path().join("restore.redb"),
            "references parent",
        )
    }

    #[test]
    fn restore_rejects_backup_chain_cycle() -> Result<(), Box<dyn Error>> {
        let temp = tempdir()?;
        let chain = chain_fixture(temp.path())?;
        let second_id = chain.second.manifest.backup_id.clone();
        edit_manifest(&chain.first.path, |manifest| {
            manifest.parent_backup_id = Some(second_id.clone());
        })?;
        assert_chain_rejected(
            &chain.second.path,
            &temp.path().join("two-cycle.redb"),
            "cycle",
        )?;

        edit_manifest(&chain.second.path, |manifest| {
            manifest.parent_backup_id = Some(second_id);
        })?;
        assert_chain_rejected(
            &chain.second.path,
            &temp.path().join("self-loop.redb"),
            "cycle",
        )
    }

    #[test]
    fn restore_rejects_parent_id_outside_backup_root() -> Result<(), Box<dyn Error>> {
        let temp = tempdir()?;
        let chain = chain_fixture(temp.path())?;
        for parent in ["../escape", "", "a/b"] {
            edit_manifest(&chain.second.path, |manifest| {
                manifest.parent_backup_id = Some(parent.to_owned());
            })?;
            assert_chain_rejected(
                &chain.second.path,
                &temp.path().join("restore.redb"),
                "invalid parent id",
            )?;
        }
        Ok(())
    }

    #[test]
    fn restore_rejects_full_backup_with_parent() -> Result<(), Box<dyn Error>> {
        let temp = tempdir()?;
        let chain = chain_fixture(temp.path())?;
        let first_id = chain.first.manifest.backup_id.clone();
        edit_manifest(&chain.full.path, |manifest| {
            manifest.parent_backup_id = Some(first_id);
        })?;
        assert_chain_rejected(
            &chain.full.path,
            &temp.path().join("restore.redb"),
            "must not reference a parent",
        )
    }

    #[test]
    fn restore_rejects_commit_log_gap_inside_incremental() -> Result<(), Box<dyn Error>> {
        let temp = tempdir()?;
        let chain = chain_fixture(temp.path())?;
        let mut payload: CommitLogPayload = read_backup_payload(
            &chain.first.path,
            required_file(&chain.first.manifest, WAL_DATA_FILE)?,
            None,
        )?;
        assert!(payload.records.len() >= 2);
        payload.records.remove(0);
        let entry = write_compressed_json(
            &chain.first.path,
            WAL_DATA_FILE,
            &payload,
            &BackupConfig::default(),
            None,
        )?;
        edit_manifest(&chain.first.path, |manifest| {
            manifest.files = vec![entry];
        })?;

        match restore_backup(
            &chain.second.path,
            on_disk(temp.path().join("restore.redb")),
            RestoreTarget::Latest,
        ) {
            Err(BackupError::Corruption(message)) => {
                assert!(
                    message.contains("was expected"),
                    "unexpected message: {message}"
                );
            }
            other => return Err(format!("expected a commit log gap, got {other:?}").into()),
        }
        Ok(())
    }

    #[test]
    fn restore_rejects_time_target_before_full_backup() -> Result<(), Box<dyn Error>> {
        let temp = tempdir()?;
        let chain = chain_fixture(temp.path())?;
        let before_full = chain
            .full
            .manifest
            .taken_at
            .checked_sub(Duration::from_secs(1))
            .ok_or("time underflow")?;
        let early_path = temp.path().join("early.redb");
        match restore_backup(
            &chain.second.path,
            on_disk(&early_path),
            RestoreTarget::Time(before_full),
        ) {
            Err(BackupError::Unsupported(message)) => {
                assert!(
                    message.contains("before the full backup"),
                    "unexpected message: {message}"
                );
            }
            other => return Err(format!("expected an unsupported target, got {other:?}").into()),
        }
        assert!(!early_path.exists());

        let report = restore_backup(
            &chain.second.path,
            on_disk(temp.path().join("now.redb")),
            RestoreTarget::Time(SystemTime::now()),
        )?;
        assert_eq!(report.restored_lsn, chain.second.manifest.end_lsn);
        Ok(())
    }
}
