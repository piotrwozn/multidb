use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    backup::Lsn,
    keyenc,
    model::Value,
    query::{QueryError, REL_ROWS_TABLE, Row, TableSchema, decode_row_bytes},
    repl::{ReadConsistency, ReplError, Replication},
    storage::{Bytes, StorageError},
    txn::{self, TxnId},
};

mod commit_metadata;
mod guarded;
pub(crate) use guarded::{
    history_rows_for_table_guarded, resolve_temporal_point_guarded, rows_as_of_lsn_guarded,
};

pub const TEMPORAL_TABLES_TABLE: &str = "__temporal_tables";
pub const TEMPORAL_RETENTION_TABLE: &str = "__temporal_retention";

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum TemporalPoint {
    Lsn(Lsn),
    Timestamp(SystemTime),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct TemporalRetention {
    pub min_lsn: Lsn,
    pub keep_history: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct TemporalTableSpec {
    pub base_table: String,
    pub retention: TemporalRetention,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TemporalRows {
    pub snapshot_lsn: Lsn,
    pub rows: Vec<Row>,
}

#[derive(thiserror::Error, Debug)]
pub enum TemporalError {
    #[error("retention expired: requested lsn {requested}, earliest available lsn is {earliest}")]
    RetentionExpired { requested: Lsn, earliest: Lsn },

    #[error("missing historical point: {0:?}")]
    MissingPoint(TemporalPoint),

    #[error("unsupported temporal operation: {0}")]
    Unsupported(String),

    #[error("query: {0}")]
    Query(#[from] QueryError),

    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("metadata serialization: {0}")]
    Serde(String),
}

impl Default for TemporalRetention {
    fn default() -> Self {
        Self {
            min_lsn: 0,
            keep_history: true,
        }
    }
}

/// Resolves a temporal point to an LSN using the commit log when needed.
/// # Errors
/// Fails when a timestamp cannot be mapped to a committed LSN.
pub fn resolve_temporal_point(
    repl: &dyn Replication,
    point: TemporalPoint,
) -> Result<Lsn, TemporalError> {
    match point {
        TemporalPoint::Lsn(lsn) => Ok(lsn),
        TemporalPoint::Timestamp(timestamp) => lsn_for_timestamp(repl, timestamp),
    }
}

/// Verifies that a requested LSN is inside a temporal retention horizon.
/// # Errors
/// Fails when history is disabled or the requested LSN is older than retention.
pub fn validate_retention(
    retention: TemporalRetention,
    requested: Lsn,
) -> Result<(), TemporalError> {
    if requested < retention.min_lsn {
        return Err(TemporalError::RetentionExpired {
            requested,
            earliest: retention.min_lsn,
        });
    }
    if !retention.keep_history {
        return Err(TemporalError::Unsupported(
            "history retention is disabled for this temporal table".to_owned(),
        ));
    }
    Ok(())
}

/// Reads rows for a table at a temporal point.
/// # Errors
/// Fails when the point cannot be resolved, retention is expired, or MVCC data is unavailable.
pub fn table_rows_as_of(
    repl: &dyn Replication,
    table: &str,
    schema: &TableSchema,
    point: TemporalPoint,
    retention: TemporalRetention,
) -> Result<TemporalRows, TemporalError> {
    let snapshot_lsn = resolve_temporal_point(repl, point)?;
    validate_retention(retention, snapshot_lsn)?;
    let rows = rows_as_of_lsn(repl, table, schema, snapshot_lsn)?;
    Ok(TemporalRows { snapshot_lsn, rows })
}

/// Reads rows for a table at a resolved LSN from MVCC history.
/// # Errors
/// Fails when the LSN is below the history GC horizon, or MVCC data is unavailable,
/// corrupt, or incompatible with the table schema.
pub fn rows_as_of_lsn(
    repl: &dyn Replication,
    table: &str,
    schema: &TableSchema,
    snapshot_lsn: Lsn,
) -> Result<Vec<Row>, TemporalError> {
    ensure_history_retained(repl, snapshot_lsn)?;
    let prefix = txn::version_key(REL_ROWS_TABLE, &table_prefix(table))?;
    let end = keyenc::range_end(&prefix);
    let versions = repl.range(txn::TXN_MVCC_TABLE, &prefix, &end, ReadConsistency::Strong)?;
    // History GC may have committed between the first check and the range read.
    ensure_history_retained(repl, snapshot_lsn)?;
    let mut by_key = std::collections::BTreeMap::<Bytes, (TxnId, Option<Bytes>)>::new();
    for (version_key, value) in versions {
        if version_key.len() < prefix.len() + 8 || !version_key.starts_with(&prefix) {
            continue;
        }
        let version = txn::decode_txn_id(&version_key[version_key.len() - 8..])?;
        if version > snapshot_lsn {
            continue;
        }
        let logical_key = version_key[prefix.len()..version_key.len() - 8].to_vec();
        let record = txn::decode_mvcc_record(&value)?;
        if by_key
            .get(&logical_key)
            .is_none_or(|(current_version, _)| version > *current_version)
        {
            by_key.insert(logical_key, (version, record.value));
        }
    }
    let mut rows = Vec::new();
    for (_, value) in by_key.into_values() {
        if let Some(bytes) = value {
            let Some(row) = decode_row_bytes(&bytes)? else {
                continue;
            };
            validate_temporal_row(schema, &row)?;
            rows.push(row);
        }
    }
    rows.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));
    Ok(rows)
}

/// Builds system-versioned history rows for a table.
/// # Errors
/// Fails when MVCC data is unavailable, corrupt, or outside retention, and with
/// `RetentionExpired` when `retention.min_lsn` is below the history GC horizon,
/// or retained pre-GC anchors cannot be listed as a complete version history.
/// Missing commit timestamps fail with `MissingPoint` rather than guessed dates.
pub fn history_rows_for_table(
    repl: &dyn Replication,
    table: &str,
    schema: &TableSchema,
    retention: TemporalRetention,
) -> Result<Vec<Row>, TemporalError> {
    validate_retention(retention, retention.min_lsn)?;
    ensure_history_retained(repl, retention.min_lsn)?;
    let prefix = txn::version_key(REL_ROWS_TABLE, &table_prefix(table))?;
    let end = keyenc::range_end(&prefix);
    let versions = repl.range(txn::TXN_MVCC_TABLE, &prefix, &end, ReadConsistency::Strong)?;
    // History GC may have committed between the first check and the range read.
    let history_horizon = ensure_history_retained(repl, retention.min_lsn)?;
    let mut by_key = BTreeMap::<Bytes, BTreeMap<TxnId, Option<Bytes>>>::new();
    for (version_key, value) in versions {
        if version_key.len() < prefix.len() + 8 || !version_key.starts_with(&prefix) {
            continue;
        }
        let version = txn::decode_txn_id(&version_key[version_key.len() - 8..])?;
        // GC keeps a pre-horizon anchor for AS OF snapshots, but earlier
        // versions may be gone. Omitting it would present an incomplete history
        // as complete; listing it would invent an unbounded validity interval.
        if version < history_horizon {
            return Err(TemporalError::RetentionExpired {
                requested: version,
                earliest: history_horizon,
            });
        }
        if version < retention.min_lsn {
            continue;
        }
        let record = txn::decode_mvcc_record(&value)?;
        let logical_key = version_key[prefix.len()..version_key.len() - 8].to_vec();
        by_key
            .entry(logical_key)
            .or_default()
            .insert(version, record.value);
    }
    let mut timestamps = BTreeMap::new();
    let mut rows = Vec::new();
    for versions in by_key.into_values() {
        let mut versions = versions.into_iter().peekable();
        while let Some((version, bytes)) = versions.next() {
            // A tombstone closes the previous version but is not itself a row.
            let Some(bytes) = bytes else {
                continue;
            };
            let mut row = decode_row_bytes(&bytes)?
                .ok_or_else(|| StorageError::Corruption("temporal row is missing".to_owned()))?;
            validate_temporal_row(schema, &row)?;
            let (from_lsn, from_time) = history_boundary(repl, version, &mut timestamps)?;
            let (to_lsn, to_time) = versions
                .peek()
                .map(|(next_version, _)| history_boundary(repl, *next_version, &mut timestamps))
                .transpose()?
                .unwrap_or((Value::Null, Value::Null));
            row.extend([from_lsn, to_lsn, from_time, to_time]);
            rows.push(row);
        }
    }
    Ok(rows)
}

fn history_boundary(
    repl: &dyn Replication,
    lsn: Lsn,
    timestamps: &mut BTreeMap<Lsn, i64>,
) -> Result<(Value, Value), TemporalError> {
    let sql_lsn = i64::try_from(lsn).map_err(|_| {
        StorageError::Corruption("temporal LSN exceeds the SQL integer range".to_owned())
    })?;
    let millis = if let Some(millis) = timestamps.get(&lsn) {
        *millis
    } else {
        // One point read per distinct LSN, even when a large transaction changed
        // many rows or supplies both sides of adjacent intervals.
        let bytes = repl
            .read(
                txn::COMMIT_LOG_TABLE,
                &txn::commit_log_key(lsn),
                ReadConsistency::Strong,
            )?
            .ok_or(TemporalError::MissingPoint(TemporalPoint::Lsn(lsn)))?;
        let record = txn::decode_commit_log_record(&bytes)?;
        if record.txn_id != lsn {
            return Err(StorageError::Corruption(
                "temporal commit record does not match its LSN".to_owned(),
            )
            .into());
        }
        let millis = system_time_to_millis(record.committed_at)?;
        timestamps.insert(lsn, millis);
        millis
    };
    Ok((Value::Int(sql_lsn), Value::Int(millis)))
}

fn lsn_for_timestamp(repl: &dyn Replication, timestamp: SystemTime) -> Result<Lsn, TemporalError> {
    txn::history::latest_commit_at_or_before(repl, timestamp)?.ok_or(TemporalError::MissingPoint(
        TemporalPoint::Timestamp(timestamp),
    ))
}

/// Rejects reads below the history GC horizon, where MVCC versions may be pruned.
fn ensure_history_retained(repl: &dyn Replication, lsn: Lsn) -> Result<Lsn, TemporalError> {
    let horizon = txn::history::history_horizon(repl)?;
    if lsn < horizon {
        return Err(TemporalError::RetentionExpired {
            requested: lsn,
            earliest: horizon,
        });
    }
    Ok(horizon)
}

fn validate_temporal_row(schema: &TableSchema, row: &Row) -> Result<(), TemporalError> {
    if row.len() != schema.columns.len() {
        return Err(TemporalError::Query(QueryError::InvalidRow(format!(
            "temporal row has {} columns, expected {}",
            row.len(),
            schema.columns.len()
        ))));
    }
    Ok(())
}

#[must_use]
pub fn table_prefix(table: &str) -> Bytes {
    let table = table.as_bytes();
    let mut key = Vec::with_capacity(8 + table.len());
    keyenc::push_len_bytes(&mut key, table);
    key
}

#[must_use]
pub fn system_versioned_schema(schema: &TableSchema) -> TableSchema {
    let mut out = schema.clone();
    out.columns.extend([
        crate::query::ColumnDef::new("valid_from_lsn", crate::query::ColumnType::Int, false),
        crate::query::ColumnDef::new("valid_to_lsn", crate::query::ColumnType::Int, true),
        crate::query::ColumnDef::new("valid_from", crate::query::ColumnType::Int, false),
        crate::query::ColumnDef::new("valid_to", crate::query::ColumnType::Int, true),
    ]);
    out
}

fn system_time_to_millis(value: SystemTime) -> Result<i64, TemporalError> {
    let millis = match value.duration_since(UNIX_EPOCH) {
        Ok(duration) => i128::try_from(duration.as_millis()).ok(),
        Err(before_epoch) => i128::try_from(before_epoch.duration().as_millis())
            .ok()
            .map(|millis| -millis),
    };
    millis
        .and_then(|millis| i64::try_from(millis).ok())
        .ok_or_else(|| {
            StorageError::Corruption("temporal timestamp exceeds the SQL integer range".to_owned())
                .into()
        })
}

#[cfg(test)]
mod tests;
