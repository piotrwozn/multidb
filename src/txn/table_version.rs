//! Native data-version tokens for caches. A token covers one physical public
//! keyspace; it is committed with its data and reconstructed by commit replay.

use crate::{
    storage::{ReadTransaction, StorageError},
    txn::{TxnId, decode_txn_id, table_is_reserved},
};

pub const TABLE_DATA_VERSION_TABLE: &str = "__txn_table_data_versions";

pub(crate) fn read_table_data_version_from(
    read: &impl ReadTransaction,
    table: &str,
) -> Result<Option<TxnId>, StorageError> {
    if table_is_reserved(table) {
        return Ok(None);
    }
    if crate::repl::dist_txn::locks::has_range_locks(read, table, &[], &[])? {
        return Ok(None);
    }
    let version = read.get(TABLE_DATA_VERSION_TABLE, table.as_bytes())?;
    // Databases created before tokens were introduced have version zero until
    // their first write under the token-aware commit path.
    Ok(Some(
        version
            .as_deref()
            .map(decode_txn_id)
            .transpose()?
            .unwrap_or(0),
    ))
}

#[cfg(test)]
mod tests;
