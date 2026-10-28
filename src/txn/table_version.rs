//! Native data-version tokens for caches. A token covers one physical public
//! keyspace; it is committed with its data and reconstructed by commit replay.

use crate::{
    repl::{ReadConsistency, ReplError, Replication},
    txn::{TxnId, decode_txn_id, table_is_reserved},
};

pub const TABLE_DATA_VERSION_TABLE: &str = "__txn_table_data_versions";

pub(crate) fn read_table_data_version<R: Replication + ?Sized>(
    repl: &R,
    table: &str,
) -> Result<Option<TxnId>, ReplError> {
    if table_is_reserved(table) {
        return Ok(None);
    }
    let version = repl.read(
        TABLE_DATA_VERSION_TABLE,
        table.as_bytes(),
        ReadConsistency::Strong,
    )?;
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
