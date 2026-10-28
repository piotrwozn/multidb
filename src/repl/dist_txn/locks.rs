use super::{
    DIST_TXN_FINISHED_TABLE, DIST_TXN_PARTICIPANT_TABLE, Decision, DistTxnId, FinishedTxnRecord,
    PreparedTxnRecord, decode_finished, decode_prepared, encode_finished, encode_prepared, txn_key,
};
use crate::{
    repl::Op,
    storage::{ReadTransaction, StorageError},
    txn::{self, WriteSet},
};

pub(crate) const LOCKS_TABLE: &str = "__dist_txn_key_locks";
const LOCK_FORMAT_KEY: &[u8] = b"dist_txn_key_lock_format";
const LOCK_FORMAT: &[u8] = b"1";

fn lock_format(read: &impl ReadTransaction) -> Result<bool, StorageError> {
    match read.get(txn::TXN_META_TABLE, LOCK_FORMAT_KEY)?.as_deref() {
        None => Ok(false),
        Some(LOCK_FORMAT) => Ok(true),
        Some(_) => Err(StorageError::Corruption(
            "unsupported distributed transaction lock format".to_owned(),
        )),
    }
}

/// Legacy in-doubt prepares have no key locks. They must be explicitly aborted
/// before admitting new public writes; silently upgrading one could lose data.
fn reject_legacy_prepares(read: &impl ReadTransaction) -> Result<(), StorageError> {
    if !lock_format(read)?
        && read
            .range(DIST_TXN_PARTICIPANT_TABLE, &[], &[])?
            .next()
            .is_some()
    {
        return Err(StorageError::Conflict);
    }
    Ok(())
}

pub(crate) fn prepare_write_set(
    read: &impl ReadTransaction,
    txn_id: DistTxnId,
    ops: &[Op],
    deadline_ms: u64,
    prepared_at_ms: u64,
) -> Result<WriteSet, StorageError> {
    txn::validate_public_ops(ops)?;
    let key = txn_key(txn_id);
    // A terminal transaction ID is never available for a new prepare, even
    // when its prior decision was Commit: its old payload is no longer stored.
    if let Some(bytes) = read.get(DIST_TXN_FINISHED_TABLE, &key)? {
        let finished = decode_finished(&bytes)?;
        if finished.txn_id != txn_id {
            return Err(StorageError::Corruption(
                "finished transaction id mismatch".to_owned(),
            ));
        }
        return Err(StorageError::Conflict);
    }
    reject_legacy_prepares(read)?;
    let public = txn::ops_to_write_set(ops.to_vec());
    if let Some(bytes) = read.get(DIST_TXN_PARTICIPANT_TABLE, &key)? {
        let prepared = decode_prepared(&bytes)?;
        if prepared.txn_id != txn_id || prepared.ops != ops {
            return Err(StorageError::Conflict);
        }
        for (table, row_key) in public.keys() {
            if read.get(LOCKS_TABLE, &txn::version_key(table, row_key)?)?
                != Some(txn::encode_txn_id(txn_id).to_vec())
            {
                return Err(StorageError::Corruption(
                    "prepared transaction lost a key lock".to_owned(),
                ));
            }
        }
        return Ok(WriteSet::new());
    }
    let mut writes = WriteSet::new();
    for (table, row_key) in public.keys() {
        let lock = txn::version_key(table, row_key)?;
        if read.get(LOCKS_TABLE, &lock)?.is_some() {
            return Err(StorageError::Conflict);
        }
        writes.insert(
            (LOCKS_TABLE.to_owned(), lock),
            Some(txn::encode_txn_id(txn_id).to_vec()),
        );
    }
    writes.insert(
        (txn::TXN_META_TABLE.to_owned(), LOCK_FORMAT_KEY.to_vec()),
        Some(LOCK_FORMAT.to_vec()),
    );
    let mut record = PreparedTxnRecord::new(txn_id, ops.to_vec(), deadline_ms);
    record.prepared_at_ms = prepared_at_ms;
    writes.insert(
        (DIST_TXN_PARTICIPANT_TABLE.to_owned(), key.to_vec()),
        Some(encode_prepared(&record)?),
    );
    Ok(writes)
}

pub(crate) fn finish_write_set(
    read: &impl ReadTransaction,
    txn_id: DistTxnId,
    decision: Decision,
    finished_at_ms: u64,
) -> Result<WriteSet, StorageError> {
    let key = txn_key(txn_id);
    if let Some(bytes) = read.get(DIST_TXN_FINISHED_TABLE, &key)? {
        let finished = decode_finished(&bytes)?;
        if finished.txn_id != txn_id {
            return Err(StorageError::Corruption(
                "finished transaction id mismatch".to_owned(),
            ));
        }
        return if finished.decision == decision {
            Ok(WriteSet::new())
        } else {
            Err(StorageError::Conflict)
        };
    }
    let prepared = read
        .get(DIST_TXN_PARTICIPANT_TABLE, &key)?
        .map(|bytes| decode_prepared(&bytes))
        .transpose()?;
    let mut writes = WriteSet::new();
    if let Some(prepared) = prepared {
        if prepared.txn_id != txn_id {
            return Err(StorageError::Corruption(
                "prepared transaction id mismatch".to_owned(),
            ));
        }
        let public = txn::ops_to_write_set(prepared.ops);
        for (table, row_key) in public.keys() {
            let lock = txn::version_key(table, row_key)?;
            match read.get(LOCKS_TABLE, &lock)? {
                Some(owner) if txn::decode_txn_id(&owner)? == txn_id => {
                    writes.insert((LOCKS_TABLE.to_owned(), lock), None);
                }
                None if decision == Decision::Abort => {}
                _ => {
                    return Err(StorageError::Corruption(
                        "prepared transaction key lock mismatch".to_owned(),
                    ));
                }
            }
        }
        if decision == Decision::Commit {
            writes.extend(public);
        }
    }
    let mut record = FinishedTxnRecord::new(txn_id, decision);
    record.finished_at_ms = finished_at_ms;
    writes.insert(
        (DIST_TXN_FINISHED_TABLE.to_owned(), key.to_vec()),
        Some(encode_finished(&record)?),
    );
    writes.insert((DIST_TXN_PARTICIPANT_TABLE.to_owned(), key.to_vec()), None);
    Ok(writes)
}

pub(crate) fn validate_write_locks(
    read: &impl ReadTransaction,
    writes: &WriteSet,
) -> Result<(), StorageError> {
    if writes
        .keys()
        .all(|(table, _)| txn::table_is_reserved(table))
    {
        return Ok(());
    }
    reject_legacy_prepares(read)?;
    for (table, key) in writes
        .keys()
        .filter(|(table, _)| !txn::table_is_reserved(table))
    {
        let lock = txn::version_key(table, key)?;
        if let Some(owner) = read.get(LOCKS_TABLE, &lock)? {
            let owner = txn::decode_txn_id(&owner)?;
            let finished = writes
                .get(&(DIST_TXN_FINISHED_TABLE.to_owned(), txn_key(owner).to_vec()))
                .and_then(Option::as_deref)
                .map(decode_finished)
                .transpose()?;
            if writes.get(&(LOCKS_TABLE.to_owned(), lock)) != Some(&None)
                || writes.get(&(
                    DIST_TXN_PARTICIPANT_TABLE.to_owned(),
                    txn_key(owner).to_vec(),
                )) != Some(&None)
                || !finished.is_some_and(|record| {
                    record.txn_id == owner && record.decision == Decision::Commit
                })
            {
                return Err(StorageError::Conflict);
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_read_keys(
    read: &impl ReadTransaction,
    table: &str,
    keys: &[crate::storage::Bytes],
) -> Result<(), StorageError> {
    if txn::table_is_reserved(table) {
        return Ok(());
    }
    reject_legacy_prepares(read)?;
    for key in keys {
        if read
            .get(LOCKS_TABLE, &txn::version_key(table, key)?)?
            .is_some()
        {
            return Err(StorageError::Conflict);
        }
    }
    Ok(())
}

pub(crate) fn validate_read_key(
    read: &impl ReadTransaction,
    table: &str,
    key: &[u8],
) -> Result<(), StorageError> {
    if txn::table_is_reserved(table) {
        return Ok(());
    }
    reject_legacy_prepares(read)?;
    if read
        .get(LOCKS_TABLE, &txn::version_key(table, key)?)?
        .is_some()
    {
        return Err(StorageError::Conflict);
    }
    Ok(())
}

pub(crate) fn has_range_locks(
    read: &impl ReadTransaction,
    table: &str,
    start: &[u8],
    end: &[u8],
) -> Result<bool, StorageError> {
    if txn::table_is_reserved(table) {
        return Ok(false);
    }
    reject_legacy_prepares(read)?;
    let prefix = txn::version_key(table, &[])?;
    let first = txn::version_key(table, start)?;
    let last = if end.is_empty() {
        crate::keyenc::range_end(&prefix)
    } else {
        txn::version_key(table, end)?
    };
    if let Some(entry) = read.range(LOCKS_TABLE, &first, &last)?.next() {
        entry?;
        return Ok(true);
    }
    Ok(false)
}

pub(crate) fn validate_read_range(
    read: &impl ReadTransaction,
    table: &str,
    start: &[u8],
    end: &[u8],
) -> Result<(), StorageError> {
    if has_range_locks(read, table, start, end)? {
        return Err(StorageError::Conflict);
    }
    Ok(())
}
