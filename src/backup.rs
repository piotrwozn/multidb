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
        ConfigError, Database, DbConfig, EncryptionConfig, EncryptionMode,
        PersistedProfileManifest, Profile, create_restore_target, decode_profile_manifest,
        unmet_profile_floors, validate_manifest,
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
const KEY_AUDIT_ENABLED: &[u8] = b"audit_enabled";
const META_TABLE: &str = "__meta";
const KEY_PROFILE_MANIFEST: &[u8] = b"profile_manifest";
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
    /// Decrypts the backup files, which are encrypted when the backup had backup encryption
    /// (`BackupConfig.encryption`, or by default the encryption config of the database backed
    /// up). The restore target itself is never encrypted at rest.
    pub encryption: Option<EncryptionConfig>,
    /// Restores a backup whose profile manifest sets a floor the restored database would not
    /// meet: `floors.encryption_at_rest`, because a restore target is never encrypted at rest,
    /// or `floors.audit` when audit is off in the backup. Off by default, and then such a
    /// restore fails with `BackupError::ProfileFloorsNotMet` before it creates anything.
    pub allow_unmet_profile_floors: bool,
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

    /// Every restore path returns it, also hibernation resume (`cloud::resume_database`,
    /// `resume_database_guarded`), which takes no `RestoreConfig` and so has no opt-in.
    #[error(
        "the restored database would not meet {} of the backed-up profile manifest, so nothing \
         was restored; to restore it anyway, set RestoreConfig.allow_unmet_profile_floors \
         (restore_backup_with_config, cloud::restore_backup_from_uri_with_config) or pass \
         multidb restore --allow-unmet-floors; hibernation resume (cloud::resume_database, \
         resume_database_guarded) has no such opt-in and cannot resume below a floor",
        describe_unmet_floors(.floors)
    )]
    ProfileFloorsNotMet { floors: Vec<&'static str> },
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

    /// Sets [`allow_unmet_profile_floors`](Self::allow_unmet_profile_floors).
    #[must_use]
    pub fn with_unmet_profile_floors_allowed(mut self, allowed: bool) -> Self {
        self.allow_unmet_profile_floors = allowed;
        self
    }
}

/// Each floor a restore would not meet, with the reason.
fn describe_unmet_floors(floors: &[&'static str]) -> String {
    floors
        .iter()
        .map(|floor| match *floor {
            "floors.encryption_at_rest" => {
                format!("{floor} (a restore target is never encrypted at rest)")
            }
            "floors.audit" => format!("{floor} (audit is off in the backup)"),
            _ => (*floor).to_owned(),
        })
        .collect::<Vec<_>>()
        .join(" and ")
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

/// Restores a full or incremental backup chain into the database described by `config`, with
/// the default [`RestoreConfig`]: unencrypted backup files only, and unmet profile floors
/// refused (see [`restore_backup_with_config`]).
///
/// # Errors
/// Fails when the backup chain is corrupt, the target is not empty, the restored database would
/// not meet a profile floor, or storage rejects restored records.
pub fn restore_backup(
    backup_dir: impl AsRef<Path>,
    config: DbConfig,
    target: RestoreTarget,
) -> Result<RestoreReport, BackupError> {
    restore_backup_with_config(backup_dir, config, target, &RestoreConfig::default())
}

/// Restores a backup chain with explicit backup decryption settings.
///
/// Every backup file is read and checked before the target is created: a first pass reads the
/// incremental payloads one at a time, keeping only what the floor check needs, and the apply
/// pass reads them again, so restore holds the full payload and at most one incremental payload
/// in memory. A backup whose profile manifest sets a floor the restored database would not meet
/// is refused unless `restore_config.allow_unmet_profile_floors` is set (see
/// [`RestoreConfig`]).
///
/// # Errors
/// Fails when the backup chain is corrupt, encrypted without a key, the target is not empty,
/// the restored database would not meet a profile floor (`ProfileFloorsNotMet`), or storage
/// rejects restored records.
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
    let full_payload = read_backup_payload::<BackupDataPayload>(
        &chain[0].path,
        required_file(&chain[0].manifest, FULL_DATA_FILE)?,
        backup_keys.as_ref(),
    )?;
    let manifest = restore_target_manifest(&config, &full_payload)?;
    let audit_flag =
        check_incremental_payloads(&chain, backup_keys.as_ref(), &full_payload, target_lsn)?;
    if let Some(manifest) = &manifest {
        check_restore_floors(manifest, audit_flag.as_deref(), restore_config)?;
    }

    let database = create_restore_target(config, manifest.as_ref())?;
    let storage = database.local_storage().ok_or_else(|| {
        BackupError::Unsupported("restore target must have local storage".to_owned())
    })?;
    let mut write = storage.begin_write()?;
    apply_full_payload(&mut write, &full_payload)?;

    let mut applied = 0_usize;
    for item in chain.iter().skip(1) {
        let payload = read_incremental_payload(item, backup_keys.as_ref())?;
        for record in payload.records {
            if record.txn_id > target_lsn {
                continue;
            }
            apply_commit_record(&mut write, &record)?;
            applied = applied.saturating_add(1);
        }
    }

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
/// Fails when any manifest, checksum, or restore step fails.
pub fn verify_backup(backup_dir: impl AsRef<Path>) -> Result<VerifyReport, BackupError> {
    verify_backup_with_config(backup_dir, &RestoreConfig::default())
}

/// Verifies backup checksums and performs a temporary restore drill with decryption settings.
///
/// The drill restores without checking profile floors, as if
/// `allow_unmet_profile_floors` were set: its target is a temporary redb file under
/// [`std::env::temp_dir`] that only this verification reads and that is deleted after a
/// successful drill, so no restored database keeps running below a floor. Like every restore
/// target, that file is not encrypted at rest: while the drill runs it holds the restored
/// values in plaintext, also when the backup files are encrypted.
///
/// # Errors
/// Fails when any manifest, checksum, decryption, or restore step fails.
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
    let config = DbConfig::on_disk(selected.manifest.profile, &restore_path);
    let drill_config = RestoreConfig {
        allow_unmet_profile_floors: true,
        ..restore_config.clone()
    };
    let report =
        restore_backup_with_config(&selected.path, config, RestoreTarget::Latest, &drill_config)?;
    if fs::remove_file(&restore_path).is_err() {
        tracing::debug!("temporary verify database could not be removed");
    }
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

fn load_chain(selected_dir: &Path) -> Result<Vec<ChainItem>, BackupError> {
    let mut chain = Vec::new();
    let mut current_path = selected_dir.to_path_buf();
    loop {
        let manifest = read_manifest(&current_path)?;
        let parent_id = manifest.parent_backup_id.clone();
        chain.push(ChainItem {
            path: current_path.clone(),
            manifest,
        });

        let Some(parent_id) = parent_id else {
            break;
        };
        let Some(root) = current_path.parent() else {
            return Err(BackupError::MissingParent(parent_id));
        };
        current_path = root.join(&parent_id);
        if !current_path.exists() {
            return Err(BackupError::MissingParent(parent_id));
        }
    }

    chain.reverse();
    Ok(chain)
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
    let mut selected = first.manifest.end_lsn;
    if time <= first.manifest.taken_at {
        return Ok(selected);
    }

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

/// The profile manifest in the full backup payload, which `create_restore_target` builds the
/// target from, so a restored manifest database is framed the way its manifest says; a payload
/// without a manifest gets the target `create_database` creates. Fails, before anything is
/// created, when the manifest is corrupt or describes another storage profile than `config`.
fn restore_target_manifest(
    config: &DbConfig,
    payload: &BackupDataPayload,
) -> Result<Option<PersistedProfileManifest>, BackupError> {
    let manifest = backed_up_profile_manifest(payload)?;
    if let Some(found) = manifest
        .as_ref()
        .map(|manifest| manifest.storage_profile)
        .filter(|found| *found != config.profile)
    {
        return Err(BackupError::Config(ConfigError::ProfileMismatch {
            expected: config.profile,
            found,
        }));
    }
    Ok(manifest)
}

/// The commit-log payload of an incremental backup in the chain.
fn read_incremental_payload(
    item: &ChainItem,
    encryption: Option<&ConfiguredKeyProvider>,
) -> Result<CommitLogPayload, BackupError> {
    read_backup_payload(
        &item.path,
        required_file(&item.manifest, WAL_DATA_FILE)?,
        encryption,
    )
}

/// The first pass over the incremental payloads of `chain`, before the target is created: each
/// payload is read (checksum, decryption, decoding) and dropped before the next one, so a
/// payload that fails leaves nothing behind and restore never holds more than one of them.
/// Returns the stored `__authz_policy.audit_enabled` value the restored database will hold:
/// the last commit-log write at or below `target_lsn` (`None` when it deleted the flag), else
/// the full payload's record, else `None`.
fn check_incremental_payloads(
    chain: &[ChainItem],
    encryption: Option<&ConfiguredKeyProvider>,
    full_payload: &BackupDataPayload,
    target_lsn: Lsn,
) -> Result<Option<Bytes>, BackupError> {
    let is_flag = |table: &str, key: &[u8]| table == AUTHZ_TABLE && key == KEY_AUDIT_ENABLED;
    let mut flag = full_payload
        .records
        .iter()
        .find(|record| is_flag(&record.table, &record.key))
        .map(|record| record.value.clone());
    for item in chain.iter().skip(1) {
        let payload = read_incremental_payload(item, encryption)?;
        for write in payload
            .records
            .iter()
            .filter(|record| record.txn_id <= target_lsn)
            .flat_map(|record| &record.writes)
            .filter(|write| is_flag(&write.table, &write.key))
        {
            flag.clone_from(&write.value);
        }
    }
    Ok(flag)
}

/// Refuses, before the target is created, a restore whose database would not meet a floor of
/// the backed-up profile manifest, unless `restore_config.allow_unmet_profile_floors` is set.
/// A restore target is never encrypted at rest, so `floors.encryption_at_rest` is never met,
/// and audit is restored as backed up, so `floors.audit` is unmet when `audit_flag`, the
/// restored `__authz_policy.audit_enabled` value, is off. A missing flag is on, as every opener
/// reads it; a flag that is not a boolean is backup corruption. `backup` and `pitr` are
/// operator-managed and never checked.
fn check_restore_floors(
    manifest: &PersistedProfileManifest,
    audit_flag: Option<&[u8]>,
    restore_config: &RestoreConfig,
) -> Result<(), BackupError> {
    let floors = manifest.knobs.floors;
    let audit_enabled = !floors.audit
        || audit_flag.map_or(Ok(true), |bytes| {
            serde_json::from_slice(bytes).map_err(|error| {
                BackupError::Corruption(format!("backed-up {AUTHZ_TABLE}.audit_enabled: {error}"))
            })
        })?;
    let unmet = unmet_profile_floors(floors, audit_enabled, false);
    if unmet.is_empty() || restore_config.allow_unmet_profile_floors {
        return Ok(());
    }
    Err(BackupError::ProfileFloorsNotMet { floors: unmet })
}

/// Decodes and re-validates `__meta.profile_manifest` from a full backup payload. Payload values
/// are stored decoded, so the record is plain JSON; anything else is backup corruption.
fn backed_up_profile_manifest(
    payload: &BackupDataPayload,
) -> Result<Option<PersistedProfileManifest>, BackupError> {
    let Some(record) = payload
        .records
        .iter()
        .find(|record| record.table == META_TABLE && record.key == KEY_PROFILE_MANIFEST)
    else {
        return Ok(None);
    };
    let corrupt = |error: ConfigError| {
        BackupError::Corruption(format!("backed-up __meta.profile_manifest: {error}"))
    };
    let manifest = decode_profile_manifest(&record.value).map_err(|error| match error {
        // A manifest written by a newer binary is not corruption; report it as an open would.
        ConfigError::UnsupportedProfileManifest { .. } => BackupError::Config(error),
        error => corrupt(error),
    })?;
    validate_manifest(&manifest, manifest.storage_profile).map_err(corrupt)?;
    Ok(Some(manifest))
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

fn apply_commit_record(
    txn: &mut impl WriteTransaction,
    record: &CommitLogRecord,
) -> Result<(), BackupError> {
    for write in &record.writes {
        match &write.value {
            Some(value) => txn.put(&write.table, &write.key, value)?,
            None => txn.delete(&write.table, &write.key)?,
        }
        txn.put(
            txn::TXN_VERSIONS_TABLE,
            &txn::version_key(&write.table, &write.key)?,
            &txn::encode_txn_id(record.txn_id),
        )?;
        txn.put(
            txn::KEYSPACE_REGISTRY_TABLE,
            write.table.as_bytes(),
            KEYSPACE_PRESENT_VALUE,
        )?;
    }

    for keyspace in [
        txn::TXN_META_TABLE,
        txn::TXN_VERSIONS_TABLE,
        txn::COMMIT_LOG_TABLE,
        txn::KEYSPACE_REGISTRY_TABLE,
    ] {
        txn.put(
            txn::KEYSPACE_REGISTRY_TABLE,
            keyspace.as_bytes(),
            KEYSPACE_PRESENT_VALUE,
        )?;
    }

    txn.put(
        txn::COMMIT_LOG_TABLE,
        &txn::commit_log_key(record.txn_id),
        &txn::encode_commit_log_record(record)?,
    )?;
    txn.put(
        txn::TXN_META_TABLE,
        txn::CURRENT_TXN_ID_KEY,
        &txn::encode_txn_id(record.txn_id),
    )?;
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
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::Path;

    use serde_json::json;
    use tempfile::tempdir;

    use super::{
        BackupConfig, BackupDataPayload, BackupDataRecord, BackupError, KEY_PROFILE_MANIFEST,
        META_TABLE, RestoreConfig, RestoreTarget, full_backup, incremental_backup, list_backups,
        restore_backup, restore_backup_with_config, restore_target_manifest, verify_backup,
        verify_backup_with_config,
    };
    use crate::config_spec::{
        PolicyCompiler, ProfileKnobOverrides, resolve_named_profile,
        spec_io::{SpecFormat, decode_spec_bytes},
    };
    use crate::db::{
        ConfigError, DbConfig, EncryptionConfig, MetadataProbe, OperationalConfig, Profile,
        ReplicationKind, RuntimeProfileBinding, create_database, create_database_with_ops,
        create_database_with_profile, create_restore_target, inspect_database_metadata,
        open_database,
    };
    use crate::performance::CompressionAlgorithm;
    use crate::repl::{Op, ReadConsistency, Replication, propose_system};
    use crate::storage::{EngineKind, ReadTransaction, RedbEngine, StorageEngine};

    /// Binds the built-in profile `name` with knob `overrides` to a database at `path`.
    fn profile_binding(
        name: &str,
        overrides: serde_json::Value,
        path: &Path,
    ) -> Result<RuntimeProfileBinding, Box<dyn std::error::Error>> {
        let overrides: ProfileKnobOverrides = serde_json::from_value(overrides)?;
        let resolution = resolve_named_profile(name, &overrides);
        let resolved = resolution
            .resolved()
            .ok_or_else(|| format!("{name} did not resolve: {:?}", resolution.issues()))?;
        Ok(resolved.bind(ReplicationKind::default(), Some(path.to_path_buf()))?)
    }

    /// Reads a stored value straight from the redb file, without decoding compression frames.
    fn raw_value(
        path: &Path,
        table: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let engine = RedbEngine::open(path)?;
        let txn = engine.begin_read()?;
        Ok(txn.get(table, key)?)
    }

    /// The example custom profile (zstd, `floors.audit`) bound to `path`, with
    /// `floors.encryption_at_rest` and the guarantee that meets it raised when
    /// `encryption_floor` is set.
    fn example_binding(
        path: &Path,
        encryption_floor: bool,
    ) -> Result<RuntimeProfileBinding, Box<dyn std::error::Error>> {
        let mut spec = decode_spec_bytes(
            include_str!("../examples/custom-profile/multidb.yaml").as_bytes(),
            SpecFormat::Yaml,
        )?;
        if encryption_floor {
            spec.custom_profile
                .as_mut()
                .ok_or("the example sets custom_profile")?
                .floors
                .encryption_at_rest = Some(true);
            spec.guarantees.encryption.at_rest = true;
        }
        let compiled = PolicyCompiler::compile(&spec).map_err(|report| format!("{report:?}"))?;
        Ok(compiled.binding(&spec.deployment, Some(path.to_path_buf()))?)
    }

    fn manifest_payload(value: Vec<u8>) -> BackupDataPayload {
        BackupDataPayload {
            records: vec![BackupDataRecord {
                table: META_TABLE.to_owned(),
                key: KEY_PROFILE_MANIFEST.to_vec(),
                value,
            }],
        }
    }

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

    #[test]
    fn restore_preserves_the_value_framing_of_a_profile_manifest_database()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let db_path = temp.path().join("source.redb");
        let binding = profile_binding(
            "high_durability",
            json!({ "compression": { "algorithm": "zstd" } }),
            &db_path,
        )?;
        assert!(binding.manifest.storage_format.value_framing);
        let value = b"framed value ".repeat(256);
        let database = create_database_with_profile(&binding, OperationalConfig::new())?;
        database.propose(Op::Put {
            table: "user_data".to_owned(),
            key: b"k".to_vec(),
            value: value.clone(),
        })?;
        let backup = full_backup(
            &database,
            temp.path().join("backups"),
            &BackupConfig::default(),
        )?;
        assert_eq!(backup.manifest.profile, Profile::HighDurability);

        let restore_path = temp.path().join("restore.redb");
        let report = restore_backup(
            &backup.path,
            DbConfig::on_disk(Profile::HighDurability, &restore_path),
            RestoreTarget::Latest,
        )?;
        assert_eq!(report.restored_lsn, backup.manifest.end_lsn);

        let data = raw_value(&restore_path, "user_data", b"k")?.ok_or("data key missing")?;
        assert!(data.starts_with(b"MDBC1"));
        assert_eq!(data[5], 2, "the restored value is a zstd frame");
        let manifest = raw_value(&restore_path, META_TABLE, KEY_PROFILE_MANIFEST)?
            .ok_or("manifest missing")?;
        assert!(manifest.starts_with(b"MDBC1"));

        let MetadataProbe::Initialized(probed) = inspect_database_metadata(&restore_path) else {
            return Err("the restored database is not initialized".into());
        };
        assert_eq!(probed.schema_version, 2);
        assert_eq!(probed.storage_profile, Profile::HighDurability);
        assert_eq!(probed.manifest.as_ref(), Some(&binding.manifest));
        assert!(probed.values_framed);

        let restored = open_database(DbConfig::on_disk(Profile::HighDurability, &restore_path))?;
        assert_eq!(restored.engine_kind(), EngineKind::CompressedRedb);
        assert_eq!(
            restored.read("user_data", b"k", ReadConsistency::Strong)?,
            Some(value)
        );
        assert_eq!(restored.performance_config(), &binding.performance);
        assert_eq!(
            restored.performance_config().compression.algorithm,
            CompressionAlgorithm::Zstd
        );

        let verified = verify_backup(&backup.path)?;
        assert_eq!(verified.restored_lsn, backup.manifest.end_lsn);
        Ok(())
    }

    #[test]
    fn restored_profile_manifest_chain_reopens_with_manifest_performance()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let db_path = temp.path().join("source.redb");
        let binding = profile_binding(
            "balanced",
            json!({
                "compression": { "algorithm": "lz4" },
                "cache": { "max_entries": 8192 },
            }),
            &db_path,
        )?;
        let database = create_database_with_profile(&binding, OperationalConfig::new())?;
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

        let restore_path = temp.path().join("restore.redb");
        let report = restore_backup(
            &incremental.path,
            DbConfig::on_disk(Profile::Balanced, &restore_path),
            RestoreTarget::Latest,
        )?;
        assert!(report.applied_commits > 0);
        let replayed = raw_value(&restore_path, "items", b"b")?.ok_or("replayed key missing")?;
        assert!(
            replayed.starts_with(b"MDBC1"),
            "commit-log replay writes framed values"
        );

        let restored = open_database(DbConfig::on_disk(Profile::Balanced, &restore_path))?;
        assert_eq!(restored.engine_kind(), EngineKind::CompressedRedb);
        assert_eq!(
            restored.read("items", b"a", ReadConsistency::Strong)?,
            Some(b"1".to_vec())
        );
        assert_eq!(
            restored.read("items", b"b", ReadConsistency::Strong)?,
            Some(b"2".to_vec())
        );
        assert_eq!(restored.performance_config().cache.max_capacity, 8192);
        assert_eq!(restored.performance_config(), &binding.performance);
        verify_backup(&incremental.path)?;
        Ok(())
    }

    #[test]
    fn restore_refuses_a_profile_that_disagrees_with_the_backed_up_manifest()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let db_path = temp.path().join("source.redb");
        let binding = profile_binding(
            "high_durability",
            json!({ "compression": { "algorithm": "zstd" } }),
            &db_path,
        )?;
        let database = create_database_with_profile(&binding, OperationalConfig::new())?;
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

        let restore_path = temp.path().join("restore.redb");
        let result = restore_backup(
            &backup.path,
            DbConfig::on_disk(Profile::Balanced, &restore_path),
            RestoreTarget::Latest,
        );
        assert!(
            matches!(
                result,
                Err(BackupError::Config(ConfigError::ProfileMismatch {
                    expected: Profile::Balanced,
                    found: Profile::HighDurability,
                }))
            ),
            "unexpected result: {result:?}"
        );
        assert!(!restore_path.exists(), "nothing is created on a mismatch");
        Ok(())
    }

    #[test]
    fn restore_target_fails_closed_on_a_corrupt_backed_up_manifest()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let target = temp.path().join("target.redb");
        let config = DbConfig::on_disk(Profile::Balanced, &target);
        let binding = profile_binding("balanced", json!({}), &target)?;
        let manifest = serde_json::to_value(&binding.manifest)?;

        let not_json = restore_target_manifest(&config, &manifest_payload(b"{".to_vec()));
        assert!(
            matches!(not_json, Err(BackupError::Corruption(_))),
            "unexpected result: {:?}",
            not_json.err()
        );

        let mut out_of_range = manifest.clone();
        out_of_range["knobs"]["cache"]["max_entries"] = json!(2_000_000);
        let out_of_range = restore_target_manifest(
            &config,
            &manifest_payload(serde_json::to_vec(&out_of_range)?),
        );
        let message = match out_of_range {
            Err(BackupError::Corruption(message)) => message,
            other => return Err(format!("unexpected result: {:?}", other.err()).into()),
        };
        assert!(message.contains("cache.max_entries"), "{message}");

        let mut newer = manifest;
        newer["manifest_version"] = json!(2);
        let newer =
            restore_target_manifest(&config, &manifest_payload(serde_json::to_vec(&newer)?));
        assert!(
            matches!(
                newer,
                Err(BackupError::Config(
                    ConfigError::UnsupportedProfileManifest { found: 2 }
                ))
            ),
            "unexpected result: {:?}",
            newer.err()
        );
        assert!(
            !target.exists(),
            "nothing is created for a corrupt manifest"
        );

        let legacy = restore_target_manifest(
            &config,
            &BackupDataPayload {
                records: Vec::new(),
            },
        )?;
        assert_eq!(legacy, None);
        let legacy = create_restore_target(config, legacy.as_ref())?;
        assert_eq!(legacy.engine_kind(), EngineKind::Redb);
        drop(legacy);
        let MetadataProbe::Initialized(probed) = inspect_database_metadata(&target) else {
            return Err("the legacy restore target is not initialized".into());
        };
        assert_eq!(probed.schema_version, 1);
        assert_eq!(probed.manifest, None);
        assert!(!probed.values_framed);
        Ok(())
    }

    /// A restore target is never encrypted, so restoring a backup whose manifest sets
    /// `floors.encryption_at_rest` is refused unless the caller opts in; the opted-in restore
    /// keeps the manifest and reports the floor as unmet. `verify_backup` restores into a
    /// temporary file without the opt-in.
    #[test]
    fn restore_refuses_an_unmet_encryption_floor_without_the_opt_in()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let db_path = temp.path().join("source.redb");
        let key_path = temp.path().join("key.bin");
        std::fs::write(&key_path, [5_u8; 32])?;
        let encryption = EncryptionConfig::file_key(&key_path);
        let binding = example_binding(&db_path, true)?;
        let profile = binding.db_config.profile;
        let database = create_database_with_profile(
            &binding,
            OperationalConfig::new().with_encryption(encryption.clone()),
        )?;
        database.propose(Op::Put {
            table: "orders".to_owned(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        })?;
        let backup = full_backup(
            &database,
            temp.path().join("backups"),
            &BackupConfig::default(),
        )?;
        drop(database);
        let restore_config = RestoreConfig::default().with_encryption(encryption);

        let refused_path = temp.path().join("refused.redb");
        let refused = restore_backup_with_config(
            &backup.path,
            DbConfig::on_disk(profile, &refused_path),
            RestoreTarget::Latest,
            &restore_config,
        );
        let message = match &refused {
            Err(error @ BackupError::ProfileFloorsNotMet { floors })
                if floors == &["floors.encryption_at_rest"] =>
            {
                error.to_string()
            }
            other => return Err(format!("unexpected result: {other:?}").into()),
        };
        // The advice names every opt-in, and says hibernation resume has none.
        for advice in [
            "never encrypted at rest",
            "RestoreConfig.allow_unmet_profile_floors (restore_backup_with_config, \
             cloud::restore_backup_from_uri_with_config)",
            "multidb restore --allow-unmet-floors",
            "hibernation resume (cloud::resume_database, resume_database_guarded) has no such \
             opt-in",
        ] {
            assert!(message.contains(advice), "{advice}: {message}");
        }
        assert!(!refused_path.exists(), "nothing is created when refused");

        let restored_path = temp.path().join("restored.redb");
        restore_backup_with_config(
            &backup.path,
            DbConfig::on_disk(profile, &restored_path),
            RestoreTarget::Latest,
            &restore_config
                .clone()
                .with_unmet_profile_floors_allowed(true),
        )?;
        let MetadataProbe::Initialized(probed) = inspect_database_metadata(&restored_path) else {
            return Err("the restored database is not initialized".into());
        };
        assert_eq!(probed.manifest.as_ref(), Some(&binding.manifest));
        assert_eq!(probed.unmet_floors, vec!["floors.encryption_at_rest"]);
        let restored = open_database(DbConfig::on_disk(profile, &restored_path))?;
        assert_eq!(
            restored.read("orders", b"k", ReadConsistency::Strong)?,
            Some(b"v".to_vec())
        );
        drop(restored);

        let verified = verify_backup_with_config(&backup.path, &restore_config)?;
        assert_eq!(verified.restored_lsn, backup.manifest.end_lsn);
        Ok(())
    }

    /// Audit is restored as backed up, so a backup whose audit flag is off below
    /// `floors.audit` (written here as an older binary that ignores the manifest could) is
    /// refused without the opt-in. Only commits up to the restore target count.
    #[test]
    fn restore_refuses_a_backup_whose_audit_is_off_below_the_audit_floor()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let db_path = temp.path().join("source.redb");
        let binding = example_binding(&db_path, false)?;
        assert!(binding.manifest.knobs.floors.audit);
        let profile = binding.db_config.profile;
        let database = create_database_with_profile(&binding, OperationalConfig::new())?;
        let root = temp.path().join("backups");
        let full = full_backup(&database, &root, &BackupConfig::default())?;
        propose_system(
            &database,
            Op::Put {
                table: super::AUTHZ_TABLE.to_owned(),
                key: super::KEY_AUDIT_ENABLED.to_vec(),
                value: b"false".to_vec(),
            },
        )?;
        let incremental =
            incremental_backup(&database, &root, &full.path, &BackupConfig::default())?;
        drop(database);

        let before = temp.path().join("before.redb");
        restore_backup(
            &incremental.path,
            DbConfig::on_disk(profile, &before),
            RestoreTarget::Lsn(full.manifest.end_lsn),
        )?;
        let MetadataProbe::Initialized(probed) = inspect_database_metadata(&before) else {
            return Err("the restored database is not initialized".into());
        };
        assert!(probed.unmet_floors.is_empty(), "{:?}", probed.unmet_floors);

        let refused_path = temp.path().join("refused.redb");
        let refused = restore_backup(
            &incremental.path,
            DbConfig::on_disk(profile, &refused_path),
            RestoreTarget::Latest,
        );
        assert!(
            matches!(
                &refused,
                Err(BackupError::ProfileFloorsNotMet { floors }) if floors == &["floors.audit"]
            ),
            "unexpected result: {refused:?}"
        );
        assert!(!refused_path.exists(), "nothing is created when refused");

        let allowed = temp.path().join("allowed.redb");
        restore_backup_with_config(
            &incremental.path,
            DbConfig::on_disk(profile, &allowed),
            RestoreTarget::Latest,
            &RestoreConfig::default().with_unmet_profile_floors_allowed(true),
        )?;
        let MetadataProbe::Initialized(probed) = inspect_database_metadata(&allowed) else {
            return Err("the restored database is not initialized".into());
        };
        assert_eq!(probed.unmet_floors, vec!["floors.audit"]);
        verify_backup(&incremental.path)?;
        Ok(())
    }

    /// The floor check follows the audit flag across a chain of several incrementals up to the
    /// restore target, and the apply pass, which reads the payloads again, restores every
    /// commit up to it.
    #[test]
    fn restore_checks_the_audit_floor_across_several_incrementals()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let binding = example_binding(&temp.path().join("source.redb"), false)?;
        let profile = binding.db_config.profile;
        let database = create_database_with_profile(&binding, OperationalConfig::new())?;
        let set_audit = |enabled: &[u8]| {
            propose_system(
                &database,
                Op::Put {
                    table: super::AUTHZ_TABLE.to_owned(),
                    key: super::KEY_AUDIT_ENABLED.to_vec(),
                    value: enabled.to_vec(),
                },
            )
        };
        let put = |key: &[u8]| {
            database.propose(Op::Put {
                table: "orders".to_owned(),
                key: key.to_vec(),
                value: b"v".to_vec(),
            })
        };
        let root = temp.path().join("backups");
        let config = BackupConfig::default();
        let full = full_backup(&database, &root, &config)?;
        put(b"a")?;
        set_audit(b"false")?;
        let audit_off = incremental_backup(&database, &root, &full.path, &config)?;
        set_audit(b"true")?;
        put(b"b")?;
        let audit_on = incremental_backup(&database, &root, &audit_off.path, &config)?;
        put(b"c")?;
        let latest = incremental_backup(&database, &root, &audit_on.path, &config)?;
        drop(database);

        let refused_path = temp.path().join("refused.redb");
        let refused = restore_backup(
            &latest.path,
            DbConfig::on_disk(profile, &refused_path),
            RestoreTarget::Lsn(audit_off.manifest.end_lsn),
        );
        assert!(
            matches!(
                &refused,
                Err(BackupError::ProfileFloorsNotMet { floors }) if floors == &["floors.audit"]
            ),
            "unexpected result: {refused:?}"
        );
        assert!(!refused_path.exists(), "nothing is created when refused");

        let restored_path = temp.path().join("restored.redb");
        let report = restore_backup(
            &latest.path,
            DbConfig::on_disk(profile, &restored_path),
            RestoreTarget::Latest,
        )?;
        assert_eq!(report.restored_lsn, latest.manifest.end_lsn);
        let commits = latest
            .manifest
            .end_lsn
            .checked_sub(full.manifest.end_lsn)
            .ok_or("the chain ends before the full backup")?;
        assert_eq!(report.applied_commits, usize::try_from(commits)?);
        let MetadataProbe::Initialized(probed) = inspect_database_metadata(&restored_path) else {
            return Err("the restored database is not initialized".into());
        };
        assert!(probed.unmet_floors.is_empty(), "{:?}", probed.unmet_floors);
        let restored = open_database(DbConfig::on_disk(profile, &restored_path))?;
        for key in [b"a", b"b", b"c"] {
            assert_eq!(
                restored.read("orders", key, ReadConsistency::Strong)?,
                Some(b"v".to_vec())
            );
        }
        Ok(())
    }

    /// Every incremental payload is read and checked before the target is created, also for a
    /// database without a manifest, so a corrupt payload in the middle of a chain leaves
    /// nothing behind.
    #[test]
    fn restore_checks_every_incremental_payload_before_it_creates_the_target()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let database = create_database(DbConfig::on_disk(
            Profile::Balanced,
            temp.path().join("source.redb"),
        ))?;
        let root = temp.path().join("backups");
        let mut chain = vec![full_backup(&database, &root, &BackupConfig::default())?.path];
        for key in [b"a", b"b", b"c"] {
            database.propose(Op::Put {
                table: "items".to_owned(),
                key: key.to_vec(),
                value: b"v".to_vec(),
            })?;
            let parent = chain.last().ok_or("the chain is empty")?;
            let incremental =
                incremental_backup(&database, &root, parent, &BackupConfig::default())?;
            chain.push(incremental.path);
        }
        drop(database);
        let [_, _, middle, last] = chain.as_slice() else {
            return Err("expected a full backup and three incrementals".into());
        };

        let wal_path = middle.join("wal").join("incremental.json.zst");
        let mut stored = std::fs::read(&wal_path)?;
        let last_byte = stored.last_mut().ok_or("the payload is empty")?;
        *last_byte ^= 0xFF;
        std::fs::write(&wal_path, stored)?;
        let target = temp.path().join("restore.redb");
        let result = restore_backup(
            last,
            DbConfig::on_disk(Profile::Balanced, &target),
            RestoreTarget::Latest,
        );
        assert!(
            matches!(&result, Err(BackupError::Corruption(_))),
            "unexpected result: {result:?}"
        );
        assert!(!target.exists(), "nothing is created for a corrupt payload");
        Ok(())
    }
}
