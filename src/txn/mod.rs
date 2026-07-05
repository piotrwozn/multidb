use std::{
    collections::{BTreeMap, BTreeSet},
    time::SystemTime,
};

use crate::phase30::{HLC_LOCAL_KEY, HLC_TABLE, HlcTimestamp};
use crate::repl::{Op, WriteCondition};
use crate::storage::{Bytes, ReadTransaction, StorageEngine, StorageError, WriteTransaction};

pub type TxnId = u64;
pub type WriteKey = (String, Bytes);
pub type WriteSet = BTreeMap<WriteKey, Option<Bytes>>;

pub const RESERVED_KEYSPACE_PREFIX: &str = "__";
pub const TXN_META_TABLE: &str = "__txn_meta";
pub const TXN_VERSIONS_TABLE: &str = "__txn_versions";
pub const TXN_MVCC_TABLE: &str = "__txn_mvcc";
pub const COMMIT_LOG_TABLE: &str = "__commit_log";
pub const KEYSPACE_REGISTRY_TABLE: &str = "__keyspaces";

pub const CURRENT_TXN_ID_KEY: &[u8] = b"current_txn_id";
const KEYSPACE_PRESENT_VALUE: &[u8] = b"1";
const COMMIT_LOG_BINARY_MAGIC: &[u8] = b"MDBCLOG2";

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CommitLogRecord {
    pub txn_id: TxnId,
    pub committed_at: SystemTime,
    #[serde(default)]
    pub hlc: Option<HlcTimestamp>,
    pub writes: Vec<CommitLogWrite>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CommitLogWrite {
    pub table: String,
    pub key: Bytes,
    pub value: Option<Bytes>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct MvccRecord {
    pub value: Option<Bytes>,
}

#[derive(Clone, Copy, Debug)]
pub struct WriteAuthorization {
    _private: (),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IsolationLevel {
    Snapshot,
    ReadCommitted,
    SnapshotIsolation,
    Serializable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TxnOptions {
    pub max_retries: u32,
    pub isolation: IsolationLevel,
}

pub trait Clock {
    fn now(&self) -> SystemTime;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ActiveSnapshotRegistry {
    active: BTreeMap<TxnId, usize>,
    retention_pins: BTreeMap<TxnId, usize>,
}

impl Default for TxnOptions {
    fn default() -> Self {
        Self {
            max_retries: 3,
            isolation: IsolationLevel::SnapshotIsolation,
        }
    }
}

impl ActiveSnapshotRegistry {
    pub fn register(&mut self, snapshot_id: TxnId) {
        *self.active.entry(snapshot_id).or_default() += 1;
    }

    pub fn unregister(&mut self, snapshot_id: TxnId) {
        if let Some(count) = self.active.get_mut(&snapshot_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.active.remove(&snapshot_id);
            }
        }
    }

    pub fn pin_retention_lsn(&mut self, lsn: TxnId) {
        *self.retention_pins.entry(lsn).or_default() += 1;
    }

    pub fn unpin_retention_lsn(&mut self, lsn: TxnId) {
        if let Some(count) = self.retention_pins.get_mut(&lsn) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.retention_pins.remove(&lsn);
            }
        }
    }

    #[must_use]
    pub fn watermark(&self, current: TxnId) -> TxnId {
        self.active
            .keys()
            .chain(self.retention_pins.keys())
            .copied()
            .min()
            .unwrap_or(current)
    }
}

/// Reads the current committed transaction id.
/// # Errors
/// Fails when metadata storage is corrupt or unavailable.
pub fn current_txn_id<S: StorageEngine>(storage: &S) -> Result<TxnId, StorageError> {
    let txn = storage.begin_read()?;
    current_txn_id_from(&txn)
}

/// Converts replication operations into the final write set for one transaction.
#[must_use]
pub fn ops_to_write_set(ops: Vec<Op>) -> WriteSet {
    let mut write_set = WriteSet::new();

    for op in ops {
        match op {
            Op::Put { table, key, value } => {
                write_set.insert((table, key), Some(value));
            }
            Op::Delete { table, key } => {
                write_set.insert((table, key), None);
            }
        }
    }

    write_set
}

#[must_use]
pub fn table_is_reserved(table: &str) -> bool {
    table.starts_with(RESERVED_KEYSPACE_PREFIX)
}

/// Validates that user-supplied operations do not target reserved keyspaces.
/// # Errors
/// Fails when any operation writes to a `__*` table.
pub fn validate_public_ops(ops: &[Op]) -> Result<(), StorageError> {
    for op in ops {
        let table = match op {
            Op::Put { table, .. } | Op::Delete { table, .. } => table,
        };
        validate_public_table(table)?;
    }
    Ok(())
}

/// Validates that public conditional reads do not target reserved keyspaces.
/// # Errors
/// Fails when any condition references a `__*` table.
pub fn validate_public_conditions(conditions: &[WriteCondition]) -> Result<(), StorageError> {
    for condition in conditions {
        let table = match condition {
            WriteCondition::KeyMissing { table, .. }
            | WriteCondition::ValueEquals { table, .. } => table,
        };
        validate_public_table(table)?;
    }
    Ok(())
}

/// Validates that a user-supplied write set does not target reserved keyspaces.
/// # Errors
/// Fails when any write targets a `__*` table.
pub fn validate_public_write_set(write_set: &WriteSet) -> Result<(), StorageError> {
    for (table, _) in write_set.keys() {
        validate_public_table(table)?;
    }
    Ok(())
}

#[must_use]
pub(crate) const fn system_write_authorization() -> WriteAuthorization {
    WriteAuthorization { _private: () }
}

#[must_use]
pub fn commit_log_key(txn_id: TxnId) -> [u8; 8] {
    txn_id.to_be_bytes()
}

/// Decodes one logical commit-log record.
/// # Errors
/// Fails when the record is neither the binary v2 format nor legacy JSON.
pub fn decode_commit_log_record(bytes: &[u8]) -> Result<CommitLogRecord, StorageError> {
    if let Some(payload) = bytes.strip_prefix(COMMIT_LOG_BINARY_MAGIC) {
        return postcard::from_bytes(payload).map_err(|error| {
            StorageError::Corruption(format!("binary commit log record: {error}"))
        });
    }

    serde_json::from_slice(bytes).map_err(|error| {
        StorageError::Corruption(format!(
            "commit log record is not binary v2 or legacy JSON: {error}"
        ))
    })
}

/// Encodes one logical commit-log record.
/// # Errors
/// Fails when binary serialization fails.
pub fn encode_commit_log_record(record: &CommitLogRecord) -> Result<Bytes, StorageError> {
    let mut encoded = Vec::from(COMMIT_LOG_BINARY_MAGIC);
    let mut payload =
        postcard::to_allocvec(record).map_err(|error| StorageError::Backend(error.to_string()))?;
    encoded.append(&mut payload);
    Ok(encoded)
}

/// Commits operations as one transaction using the current database snapshot.
/// # Errors
/// Fails when storage rejects the commit.
pub fn commit_ops_at_current<S: StorageEngine>(
    storage: &S,
    ops: Vec<Op>,
) -> Result<TxnId, StorageError> {
    validate_public_ops(&ops)?;
    commit_write_set_at_current(storage, ops_to_write_set(ops))
}

pub(crate) fn commit_ops_at_current_authorized<S: StorageEngine>(
    storage: &S,
    ops: Vec<Op>,
    authorization: WriteAuthorization,
) -> Result<TxnId, StorageError> {
    commit_write_set_at_current_authorized(storage, ops_to_write_set(ops), authorization)
}

/// Commits a write set using the current database snapshot.
/// # Errors
/// Fails when storage rejects the commit.
pub fn commit_write_set_at_current<S: StorageEngine>(
    storage: &S,
    write_set: WriteSet,
) -> Result<TxnId, StorageError> {
    validate_public_write_set(&write_set)?;
    commit_write_set_at_current_authorized(storage, write_set, system_write_authorization())
}

pub(crate) fn commit_write_set_at_current_authorized<S: StorageEngine>(
    storage: &S,
    write_set: WriteSet,
    authorization: WriteAuthorization,
) -> Result<TxnId, StorageError> {
    if write_set.is_empty() {
        return current_txn_id(storage);
    }

    let txn = storage.begin_write()?;
    let snapshot_id = current_txn_id_from(&txn)?;
    commit_write_set_in_txn_with_extra_authorized(
        txn,
        snapshot_id,
        write_set,
        |_| Ok(()),
        |_, _| Ok(()),
        authorization,
    )
}

/// Commits a write set if no written key changed after `snapshot_id`.
/// # Errors
/// Fails with `StorageError::Conflict` when a write-write conflict is detected.
pub fn commit_write_set<S: StorageEngine>(
    storage: &S,
    snapshot_id: TxnId,
    write_set: WriteSet,
) -> Result<TxnId, StorageError> {
    validate_public_write_set(&write_set)?;
    commit_write_set_authorized(
        storage,
        snapshot_id,
        write_set,
        system_write_authorization(),
    )
}

pub(crate) fn commit_write_set_authorized<S: StorageEngine>(
    storage: &S,
    snapshot_id: TxnId,
    write_set: WriteSet,
    authorization: WriteAuthorization,
) -> Result<TxnId, StorageError> {
    if write_set.is_empty() {
        return Ok(snapshot_id);
    }

    let txn = storage.begin_write()?;
    commit_write_set_in_txn_with_extra_authorized(
        txn,
        snapshot_id,
        write_set,
        |_| Ok(()),
        |_, _| Ok(()),
        authorization,
    )
}

/// Commits a write set inside an already-open storage transaction.
/// # Errors
/// Fails with `StorageError::Conflict` when a written key changed after the snapshot.
pub fn commit_write_set_in_txn<T: WriteTransaction>(
    txn: T,
    snapshot_id: TxnId,
    write_set: WriteSet,
) -> Result<TxnId, StorageError> {
    validate_public_write_set(&write_set)?;
    commit_write_set_in_txn_with_extra(txn, snapshot_id, write_set, |_, _| Ok(()))
}

/// Commits a write set and writes extra metadata atomically before the backend commit.
/// # Errors
/// Fails with `StorageError::Conflict` when a written key changed after the snapshot.
pub fn commit_write_set_in_txn_with_extra<T, F>(
    txn: T,
    snapshot_id: TxnId,
    write_set: WriteSet,
    extra: F,
) -> Result<TxnId, StorageError>
where
    T: WriteTransaction,
    F: FnOnce(&mut T, TxnId) -> Result<(), StorageError>,
{
    validate_public_write_set(&write_set)?;
    commit_write_set_in_txn_with_preflight_and_extra(txn, snapshot_id, write_set, |_| Ok(()), extra)
}

/// Commits a write set after running caller-supplied validation under the same backend write txn.
/// # Errors
/// Fails with `StorageError::Conflict` when validation or write-write checks reject the commit.
pub fn commit_write_set_in_txn_with_preflight<T, F>(
    txn: T,
    snapshot_id: TxnId,
    write_set: WriteSet,
    preflight: F,
) -> Result<TxnId, StorageError>
where
    T: WriteTransaction,
    F: FnOnce(&mut T) -> Result<(), StorageError>,
{
    commit_write_set_in_txn_with_preflight_and_extra(
        txn,
        snapshot_id,
        write_set,
        preflight,
        |_, _| Ok(()),
    )
}

/// Commits a write set with preflight validation and extra metadata writes in one backend txn.
/// # Errors
/// Fails with `StorageError::Conflict` when validation or write-write checks reject the commit.
pub fn commit_write_set_in_txn_with_preflight_and_extra<T, F, E>(
    txn: T,
    snapshot_id: TxnId,
    write_set: WriteSet,
    preflight: F,
    extra: E,
) -> Result<TxnId, StorageError>
where
    T: WriteTransaction,
    F: FnOnce(&mut T) -> Result<(), StorageError>,
    E: FnOnce(&mut T, TxnId) -> Result<(), StorageError>,
{
    validate_public_write_set(&write_set)?;
    commit_write_set_in_txn_with_extra_authorized(
        txn,
        snapshot_id,
        write_set,
        preflight,
        extra,
        system_write_authorization(),
    )
}

pub(crate) fn commit_write_set_in_txn_with_extra_authorized<T, F, E>(
    mut txn: T,
    snapshot_id: TxnId,
    write_set: WriteSet,
    preflight: F,
    extra: E,
    _authorization: WriteAuthorization,
) -> Result<TxnId, StorageError>
where
    T: WriteTransaction,
    F: FnOnce(&mut T) -> Result<(), StorageError>,
    E: FnOnce(&mut T, TxnId) -> Result<(), StorageError>,
{
    for (table, key) in write_set.keys() {
        if last_key_version(&txn, table, key)? > snapshot_id {
            txn.rollback();
            return Err(StorageError::Conflict);
        }
    }

    if let Err(error) = preflight(&mut txn) {
        txn.rollback();
        return Err(error);
    }

    let next_txn_id = current_txn_id_from(&txn)?
        .checked_add(1)
        .ok_or_else(|| StorageError::Backend("transaction id overflow".to_owned()))?;

    txn.put(
        TXN_META_TABLE,
        CURRENT_TXN_ID_KEY,
        &encode_txn_id(next_txn_id),
    )?;

    let hlc = next_hlc_in_txn(&txn)?;
    let commit_log =
        commit_log_record_with_clock_and_hlc(next_txn_id, &write_set, &SystemClock, hlc);
    let keyspaces = keyspaces_for_commit(&write_set);

    for ((table, key), value) in write_set {
        txn.put(
            TXN_MVCC_TABLE,
            &mvcc_key(&table, &key, next_txn_id)?,
            &encode_mvcc_record(&MvccRecord {
                value: value.clone(),
            })?,
        )?;

        match value {
            Some(bytes) => txn.put(&table, &key, &bytes)?,
            None => txn.delete(&table, &key)?,
        }

        txn.put(
            TXN_VERSIONS_TABLE,
            &version_key(&table, &key)?,
            &encode_txn_id(next_txn_id),
        )?;
    }

    for keyspace in keyspaces {
        txn.put(
            KEYSPACE_REGISTRY_TABLE,
            keyspace.as_bytes(),
            KEYSPACE_PRESENT_VALUE,
        )?;
    }

    txn.put(
        COMMIT_LOG_TABLE,
        &commit_log_key(next_txn_id),
        &encode_commit_log_record(&commit_log)?,
    )?;
    txn.put(HLC_TABLE, HLC_LOCAL_KEY, &encode_hlc(hlc)?)?;

    if let Err(error) = extra(&mut txn, next_txn_id) {
        txn.rollback();
        return Err(error);
    }

    txn.commit()?;
    Ok(next_txn_id)
}

/// Reads the current committed transaction id from an existing read snapshot.
/// # Errors
/// Fails when transaction metadata is corrupt or unavailable.
pub fn current_txn_id_from(txn: &impl ReadTransaction) -> Result<TxnId, StorageError> {
    let Some(bytes) = txn.get(TXN_META_TABLE, CURRENT_TXN_ID_KEY)? else {
        return Ok(0);
    };

    decode_txn_id(&bytes)
}

/// Reads the last committed transaction id for one logical key.
/// # Errors
/// Fails when metadata storage is corrupt or unavailable.
pub fn last_key_version_from(
    txn: &impl ReadTransaction,
    table: &str,
    key: &[u8],
) -> Result<TxnId, StorageError> {
    last_key_version(txn, table, key)
}

fn last_key_version(
    txn: &impl ReadTransaction,
    table: &str,
    key: &[u8],
) -> Result<TxnId, StorageError> {
    let Some(bytes) = txn.get(TXN_VERSIONS_TABLE, &version_key(table, key)?)? else {
        return Ok(0);
    };

    decode_txn_id(&bytes)
}

/// Builds the internal version key for one logical record.
/// # Errors
/// Fails when the table name length cannot be encoded.
pub fn version_key(table: &str, key: &[u8]) -> Result<Bytes, StorageError> {
    let table_len =
        u64::try_from(table.len()).map_err(|error| StorageError::Backend(error.to_string()))?;
    let mut out = Vec::with_capacity(8 + table.len() + key.len());
    out.extend_from_slice(&table_len.to_be_bytes());
    out.extend_from_slice(table.as_bytes());
    out.extend_from_slice(key);
    Ok(out)
}

/// Builds the versioned MVCC key for one logical record version.
/// # Errors
/// Fails when the table name length cannot be encoded.
pub fn mvcc_key(table: &str, key: &[u8], txn_id: TxnId) -> Result<Bytes, StorageError> {
    let mut out = version_key(table, key)?;
    out.extend_from_slice(&txn_id.to_be_bytes());
    Ok(out)
}

/// Encodes one MVCC version record.
/// # Errors
/// Fails when binary serialization fails.
pub fn encode_mvcc_record(record: &MvccRecord) -> Result<Bytes, StorageError> {
    postcard::to_allocvec(record).map_err(|error| StorageError::Backend(error.to_string()))
}

/// Decodes one MVCC version record.
/// # Errors
/// Fails when the bytes are corrupt.
pub fn decode_mvcc_record(bytes: &[u8]) -> Result<MvccRecord, StorageError> {
    postcard::from_bytes(bytes)
        .map_err(|error| StorageError::Corruption(format!("mvcc record: {error}")))
}

/// Removes obsolete MVCC versions while preserving the last version before the watermark.
/// # Errors
/// Fails when MVCC metadata is corrupt or storage rejects deletes.
pub fn gc_mvcc_versions_in_txn(
    txn: &mut impl WriteTransaction,
    watermark: TxnId,
) -> Result<usize, StorageError> {
    let rows = txn
        .range(TXN_MVCC_TABLE, &[], &[])?
        .collect::<Result<Vec<_>, _>>()?;
    let mut by_key = BTreeMap::<Bytes, Vec<TxnId>>::new();
    for (key, _) in rows {
        let (logical_key, version) = split_mvcc_version_key(&key)?;
        by_key
            .entry(logical_key.to_vec())
            .or_default()
            .push(version);
    }

    let mut removed = 0_usize;
    for (logical_key, mut versions) in by_key {
        versions.sort_unstable();
        let keep_before_watermark = versions
            .iter()
            .copied()
            .filter(|version| *version < watermark)
            .max();
        for version in versions {
            if version >= watermark || Some(version) == keep_before_watermark {
                continue;
            }
            let mut key = logical_key.clone();
            key.extend_from_slice(&version.to_be_bytes());
            txn.delete(TXN_MVCC_TABLE, &key)?;
            removed = removed.saturating_add(1);
        }
    }
    Ok(removed)
}

#[must_use]
pub const fn encode_txn_id(value: TxnId) -> [u8; 8] {
    value.to_be_bytes()
}

/// Decodes a transaction id from storage bytes.
/// # Errors
/// Fails when the byte length is not exactly eight bytes.
pub fn decode_txn_id(bytes: &[u8]) -> Result<TxnId, StorageError> {
    let bytes = bytes.try_into().map_err(|_| {
        StorageError::Corruption("transaction id must be exactly 8 bytes".to_owned())
    })?;
    Ok(u64::from_be_bytes(bytes))
}

fn validate_public_table(table: &str) -> Result<(), StorageError> {
    if table_is_reserved(table) {
        return Err(StorageError::Backend(format!(
            "reserved keyspace {table} cannot be written through public APIs"
        )));
    }
    Ok(())
}

fn split_mvcc_version_key(key: &[u8]) -> Result<(&[u8], TxnId), StorageError> {
    if key.len() < 8 {
        return Err(StorageError::Corruption(
            "mvcc key is missing version suffix".to_owned(),
        ));
    }
    let split = key.len() - 8;
    Ok((&key[..split], decode_txn_id(&key[split..])?))
}

#[cfg(test)]
fn commit_log_record_with_clock(
    txn_id: TxnId,
    write_set: &WriteSet,
    clock: &impl Clock,
) -> CommitLogRecord {
    commit_log_record_with_clock_and_hlc(txn_id, write_set, clock, HlcTimestamp::now())
}

fn commit_log_record_with_clock_and_hlc(
    txn_id: TxnId,
    write_set: &WriteSet,
    clock: &impl Clock,
    hlc: HlcTimestamp,
) -> CommitLogRecord {
    CommitLogRecord {
        txn_id,
        committed_at: clock.now(),
        hlc: Some(hlc),
        writes: write_set
            .iter()
            .map(|((table, key), value)| CommitLogWrite {
                table: table.clone(),
                key: key.clone(),
                value: value.clone(),
            })
            .collect(),
    }
}

fn next_hlc_in_txn(txn: &impl ReadTransaction) -> Result<HlcTimestamp, StorageError> {
    let current = txn
        .get(HLC_TABLE, HLC_LOCAL_KEY)?
        .map(|bytes| decode_hlc(&bytes))
        .transpose()?
        .unwrap_or_default();
    Ok(current.tick())
}

fn encode_hlc(timestamp: HlcTimestamp) -> Result<Bytes, StorageError> {
    postcard::to_allocvec(&timestamp).map_err(|error| StorageError::Backend(error.to_string()))
}

fn decode_hlc(bytes: &[u8]) -> Result<HlcTimestamp, StorageError> {
    postcard::from_bytes(bytes)
        .map_err(|error| StorageError::Corruption(format!("hlc timestamp: {error}")))
}

fn keyspaces_for_commit(write_set: &WriteSet) -> BTreeSet<String> {
    let mut keyspaces = BTreeSet::new();
    keyspaces.insert(TXN_META_TABLE.to_owned());
    keyspaces.insert(TXN_VERSIONS_TABLE.to_owned());
    keyspaces.insert(TXN_MVCC_TABLE.to_owned());
    keyspaces.insert(COMMIT_LOG_TABLE.to_owned());
    keyspaces.insert(KEYSPACE_REGISTRY_TABLE.to_owned());
    keyspaces.insert(HLC_TABLE.to_owned());
    for (table, _) in write_set.keys() {
        keyspaces.insert(table.clone());
    }
    keyspaces
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::{
        ActiveSnapshotRegistry, COMMIT_LOG_BINARY_MAGIC, Clock, CommitLogRecord, CommitLogWrite,
        MvccRecord, TXN_MVCC_TABLE, WriteSet, commit_log_record_with_clock, commit_ops_at_current,
        decode_commit_log_record, decode_hlc, encode_commit_log_record, encode_mvcc_record,
        gc_mvcc_versions_in_txn, mvcc_key,
    };
    use crate::{
        phase30::{HLC_LOCAL_KEY, HLC_TABLE},
        repl::Op,
        storage::{MemEngine, ReadTransaction, StorageEngine, StorageError, WriteTransaction},
    };

    #[test]
    fn commit_log_binary_round_trips_and_legacy_json_decodes()
    -> Result<(), Box<dyn std::error::Error>> {
        let record = CommitLogRecord {
            txn_id: 42,
            committed_at: UNIX_EPOCH + Duration::from_millis(123),
            hlc: None,
            writes: vec![CommitLogWrite {
                table: "accounts".to_owned(),
                key: b"1".to_vec(),
                value: Some(b"ada".to_vec()),
            }],
        };

        let encoded = encode_commit_log_record(&record)?;
        assert!(encoded.starts_with(COMMIT_LOG_BINARY_MAGIC));
        assert_eq!(decode_commit_log_record(&encoded)?, record);

        let legacy = serde_json::to_vec(&record)?;
        assert_eq!(decode_commit_log_record(&legacy)?, record);

        Ok(())
    }

    #[test]
    fn active_snapshot_registry_uses_oldest_snapshot_or_retention_pin() {
        let mut registry = ActiveSnapshotRegistry::default();
        assert_eq!(registry.watermark(10), 10);

        registry.register(7);
        registry.register(4);
        registry.pin_retention_lsn(6);
        assert_eq!(registry.watermark(10), 4);

        registry.unregister(4);
        assert_eq!(registry.watermark(10), 6);

        registry.unpin_retention_lsn(6);
        assert_eq!(registry.watermark(10), 7);

        registry.unregister(7);
        assert_eq!(registry.watermark(10), 10);
    }

    #[test]
    fn mvcc_gc_keeps_last_version_before_watermark() -> Result<(), StorageError> {
        let engine = MemEngine::new();
        let mut write = engine.begin_write()?;
        for version in 1..=3 {
            write.put(
                TXN_MVCC_TABLE,
                &mvcc_key("t", b"k", version)?,
                &encode_mvcc_record(&MvccRecord {
                    value: Some(vec![u8::try_from(version).unwrap_or_default()]),
                })?,
            )?;
        }
        write.commit()?;

        let mut write = engine.begin_write()?;
        assert_eq!(gc_mvcc_versions_in_txn(&mut write, 3)?, 1);
        write.commit()?;

        let read = engine.begin_read()?;
        let versions = read
            .range(TXN_MVCC_TABLE, &[], &[])?
            .collect::<Result<Vec<_>, _>>()?;
        let key_v2 = mvcc_key("t", b"k", 2)?;
        let key_v3 = mvcc_key("t", b"k", 3)?;
        assert_eq!(versions.len(), 2);
        assert!(versions.iter().any(|(key, _)| key == &key_v2));
        assert!(versions.iter().any(|(key, _)| key == &key_v3));

        Ok(())
    }

    #[test]
    fn commit_log_record_uses_supplied_clock() {
        struct FixedClock(SystemTime);

        impl Clock for FixedClock {
            fn now(&self) -> SystemTime {
                self.0
            }
        }

        let at = UNIX_EPOCH + Duration::from_secs(99);
        let record = commit_log_record_with_clock(7, &WriteSet::new(), &FixedClock(at));

        assert_eq!(record.committed_at, at);
        assert!(record.hlc.is_some());
    }

    #[test]
    fn commits_persist_monotonic_hlc() -> Result<(), Box<dyn std::error::Error>> {
        let engine = MemEngine::new();
        commit_ops_at_current(
            &engine,
            vec![Op::Put {
                table: "t".to_owned(),
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            }],
        )?;
        let first = {
            let read = engine.begin_read()?;
            let bytes = read
                .get(HLC_TABLE, HLC_LOCAL_KEY)?
                .ok_or_else(|| StorageError::Backend("missing first HLC".to_owned()))?;
            decode_hlc(&bytes)?
        };

        commit_ops_at_current(
            &engine,
            vec![Op::Put {
                table: "t".to_owned(),
                key: b"b".to_vec(),
                value: b"2".to_vec(),
            }],
        )?;
        let second = {
            let read = engine.begin_read()?;
            let bytes = read
                .get(HLC_TABLE, HLC_LOCAL_KEY)?
                .ok_or_else(|| StorageError::Backend("missing second HLC".to_owned()))?;
            decode_hlc(&bytes)?
        };

        assert!(second > first);
        Ok(())
    }
}
