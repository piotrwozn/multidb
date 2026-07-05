use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    backup::Lsn,
    keyenc,
    model::Value,
    query::{QueryError, REL_ROWS_TABLE, Row, TableSchema, decode_row_bytes},
    repl::{ReadConsistency, ReplError, Replication},
    storage::{Bytes, StorageError},
    txn::{self, TxnId},
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
/// Fails when MVCC data is unavailable, corrupt, or incompatible with the table schema.
pub fn rows_as_of_lsn(
    repl: &dyn Replication,
    table: &str,
    schema: &TableSchema,
    snapshot_lsn: Lsn,
) -> Result<Vec<Row>, TemporalError> {
    let prefix = txn::version_key(REL_ROWS_TABLE, &table_prefix(table))?;
    let end = keyenc::range_end(&prefix);
    let versions = repl.range(txn::TXN_MVCC_TABLE, &prefix, &end, ReadConsistency::Strong)?;
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
/// Fails when MVCC data is unavailable, corrupt, or outside retention.
pub fn history_rows_for_table(
    repl: &dyn Replication,
    table: &str,
    schema: &TableSchema,
    retention: TemporalRetention,
) -> Result<Vec<Row>, TemporalError> {
    let prefix = txn::version_key(REL_ROWS_TABLE, &table_prefix(table))?;
    let end = keyenc::range_end(&prefix);
    let versions = repl.range(txn::TXN_MVCC_TABLE, &prefix, &end, ReadConsistency::Strong)?;
    let mut rows = Vec::new();
    for (version_key, value) in versions {
        if version_key.len() < prefix.len() + 8 || !version_key.starts_with(&prefix) {
            continue;
        }
        let version = txn::decode_txn_id(&version_key[version_key.len() - 8..])?;
        validate_retention(retention, version)?;
        let record = txn::decode_mvcc_record(&value)?;
        let mut row = match record.value {
            Some(bytes) => decode_row_bytes(&bytes)?.unwrap_or_default(),
            None => vec![Value::Null; schema.columns.len()],
        };
        validate_temporal_row(schema, &row)?;
        row.push(Value::Int(i64::try_from(version).unwrap_or(i64::MAX)));
        row.push(Value::Null);
        row.push(Value::Int(system_time_to_millis(SystemTime::now())));
        row.push(Value::Null);
        rows.push(row);
    }
    Ok(rows)
}

fn lsn_for_timestamp(repl: &dyn Replication, timestamp: SystemTime) -> Result<Lsn, TemporalError> {
    let records = repl.range(txn::COMMIT_LOG_TABLE, &[], &[], ReadConsistency::Strong)?;
    let mut selected = None;
    for (_, value) in records {
        let record = txn::decode_commit_log_record(&value)?;
        if record.committed_at <= timestamp {
            selected = Some(record.txn_id);
        }
    }
    selected.ok_or(TemporalError::MissingPoint(TemporalPoint::Timestamp(
        timestamp,
    )))
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

fn system_time_to_millis(value: SystemTime) -> i64 {
    value
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}
