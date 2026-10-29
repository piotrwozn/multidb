//! SQL history reads with borrowed MVCC framing and retained decoded-row leases.
use std::{collections::BTreeMap, time::SystemTime};

use crate::{
    model::value_decode_memory_bound,
    query::{GuardedRows, REL_ROWS_TABLE, Row, TableSchema, decode_row_bytes},
    repl::{ReadConsistency, Replication},
    storage::{Bytes, ReadBudget, ReadReservation, StorageError},
    txn,
};

use super::{
    TemporalError, TemporalPoint, TemporalRetention, system_time_to_millis, validate_temporal_row,
};

struct Version {
    key: Bytes,
    value: Option<Bytes>,
    lsn: u64,
    ordinal: usize,
    _reservation: Box<dyn ReadReservation>,
}

struct Versions {
    entries: Vec<Version>,
    // The Vec buffer must be destroyed before releasing its own admission.
    metadata_reservation: Option<Box<dyn ReadReservation>>,
}

impl Versions {
    fn push(&mut self, entry: Version, budget: &dyn ReadBudget) -> Result<(), StorageError> {
        if self.entries.len() == self.entries.capacity() {
            let capacity = self.entries.capacity().saturating_mul(2).max(4);
            let reservation = budget.reserve(
                capacity
                    .saturating_mul(std::mem::size_of::<Version>())
                    .saturating_add(256),
            )?;
            self.entries
                .try_reserve_exact(capacity - self.entries.len())
                .map_err(|_| {
                    StorageError::ResourceLimit(
                        "cannot allocate historical version metadata".to_owned(),
                    )
                })?;
            self.metadata_reservation = Some(reservation);
        }
        self.entries.push(entry);
        Ok(())
    }
}

#[derive(serde::Deserialize)]
struct BorrowedMvcc<'a> {
    #[serde(borrow)]
    value: Option<&'a [u8]>,
}

fn mvcc(bytes: &[u8]) -> Result<Option<&[u8]>, StorageError> {
    let (record, trailing) = postcard::take_from_bytes::<BorrowedMvcc<'_>>(bytes)
        .map_err(|error| StorageError::Corruption(format!("mvcc record: {error}")))?;
    if !trailing.is_empty() {
        return Err(StorageError::Corruption(
            "mvcc record: trailing bytes".to_owned(),
        ));
    }
    Ok(record.value)
}

fn metadata_lsn(
    repl: &dyn Replication,
    key: &[u8],
    budget: &dyn ReadBudget,
) -> Result<Option<u64>, TemporalError> {
    let mut out = None;
    repl.visit_get(
        txn::TXN_META_TABLE,
        key,
        ReadConsistency::Strong,
        budget,
        &mut |value| {
            out = value.map(txn::decode_txn_id).transpose()?;
            Ok(false)
        },
    )?;
    Ok(out)
}

fn horizon(
    repl: &dyn Replication,
    requested: u64,
    budget: &dyn ReadBudget,
) -> Result<u64, TemporalError> {
    let horizon = metadata_lsn(repl, txn::history::HISTORY_HORIZON_KEY, budget)?.unwrap_or(0);
    if requested < horizon {
        return Err(TemporalError::RetentionExpired {
            requested,
            earliest: horizon,
        });
    }
    Ok(horizon)
}

fn versions(
    repl: &dyn Replication,
    table: &str,
    through: Option<u64>,
    budget: &dyn ReadBudget,
) -> Result<Versions, TemporalError> {
    let _bounds = budget.reserve(table.len().saturating_mul(8).saturating_add(512))?;
    let prefix = txn::version_key(REL_ROWS_TABLE, &super::table_prefix(table))?;
    let end = crate::keyenc::range_end(&prefix);
    let mut entries = Versions {
        entries: Vec::new(),
        metadata_reservation: None,
    };
    repl.visit_range(
        txn::TXN_MVCC_TABLE,
        &prefix,
        &end,
        ReadConsistency::Strong,
        budget,
        &mut |key, bytes| {
            budget.check()?;
            if key.len() < prefix.len() + 8 || !key.starts_with(&prefix) {
                return Ok(true);
            }
            let lsn = txn::decode_txn_id(&key[key.len() - 8..])?;
            if through.is_some_and(|through| lsn > through) {
                return Ok(true);
            }
            let value = mvcc(bytes)?;
            let key = &key[prefix.len()..key.len() - 8];
            let reservation = budget.reserve(
                key.len()
                    .saturating_add(value.map_or(0, <[u8]>::len))
                    .saturating_add(256),
            )?;
            entries.push(
                Version {
                    key: key.to_vec(),
                    value: value.map(<[u8]>::to_vec),
                    lsn,
                    ordinal: entries.entries.len(),
                    _reservation: reservation,
                },
                budget,
            )?;
            Ok(true)
        },
    )?;
    // Also preserve the established behavior of compatibility backends whose
    // range results need not be ordered. Ordinal retains equal-version ties.
    entries.entries.sort_unstable_by(|left, right| {
        left.key
            .cmp(&right.key)
            .then_with(|| left.lsn.cmp(&right.lsn))
            .then_with(|| left.ordinal.cmp(&right.ordinal))
    });
    budget.check()?;
    Ok(entries)
}

fn decode_row(
    bytes: &[u8],
    schema: &TableSchema,
    budget: &dyn ReadBudget,
) -> Result<(Row, Box<dyn ReadReservation>), TemporalError> {
    budget.check()?;
    let _json = if crate::model::value_is_binary_encoded(bytes) {
        None
    } else {
        Some(budget.reserve(crate::query::json_unescape_working_bytes(bytes))?)
    };
    let bound = value_decode_memory_bound(bytes, budget.remaining_bytes())?;
    let reservation = budget.reserve(bound.saturating_add(512))?;
    let row = decode_row_bytes(bytes)?
        .ok_or_else(|| StorageError::Corruption("temporal row is missing".to_owned()))?;
    validate_temporal_row(schema, &row)?;
    budget.check()?;
    Ok((row, reservation))
}

pub(crate) fn rows_as_of_lsn_guarded(
    repl: &dyn Replication,
    table: &str,
    schema: &TableSchema,
    snapshot: u64,
    budget: &dyn ReadBudget,
) -> Result<GuardedRows, TemporalError> {
    horizon(repl, snapshot, budget)?;
    let mut versions = versions(repl, table, Some(snapshot), budget)?;
    // GC may have committed after the scan snapshot, just as in the trusted API.
    horizon(repl, snapshot, budget)?;
    let mut entries = std::mem::take(&mut versions.entries).into_iter().peekable();
    let mut rows = GuardedRows::new();
    while let Some(mut current) = entries.next() {
        budget.check()?;
        while entries.peek().is_some_and(|next| next.key == current.key) {
            let next = entries
                .next()
                .ok_or_else(|| StorageError::Corruption("missing historical version".to_owned()))?;
            // The old AS OF map kept the first copy at an equal LSN.
            if next.lsn > current.lsn {
                current = next;
            }
        }
        if let Some(bytes) = &current.value {
            let (row, reservation) = decode_row(bytes, schema, budget)?;
            rows.push(row, reservation);
        }
    }
    rows.sort_debug(budget)?;
    Ok(rows)
}

struct Timestamps {
    values: BTreeMap<u64, i64>,
    reservations: Vec<Box<dyn ReadReservation>>,
}

impl Timestamps {
    fn boundary(
        &mut self,
        repl: &dyn Replication,
        lsn: u64,
        budget: &dyn ReadBudget,
    ) -> Result<(crate::model::Value, crate::model::Value), TemporalError> {
        use crate::model::Value;
        let sql_lsn = i64::try_from(lsn).map_err(|_| {
            StorageError::Corruption("temporal LSN exceeds the SQL integer range".to_owned())
        })?;
        let millis = if let Some(millis) = self.values.get(&lsn) {
            *millis
        } else {
            let reservation = budget.reserve(1_024)?;
            let mut timestamp = None;
            repl.visit_get(
                txn::COMMIT_LOG_TABLE,
                &txn::commit_log_key(lsn),
                ReadConsistency::Strong,
                budget,
                &mut |value| {
                    if let Some(bytes) = value {
                        let (txn_id, at) =
                            super::commit_metadata::decode_commit_metadata(bytes, budget)?;
                        if txn_id != lsn {
                            return Err(StorageError::Corruption(
                                "temporal commit record does not match its LSN".to_owned(),
                            )
                            .into());
                        }
                        timestamp = Some(at);
                    }
                    Ok(false)
                },
            )?;
            let timestamp =
                timestamp.ok_or(TemporalError::MissingPoint(TemporalPoint::Lsn(lsn)))?;
            let millis = system_time_to_millis(timestamp)?;
            self.values.insert(lsn, millis);
            self.reservations.push(reservation);
            millis
        };
        Ok((Value::Int(sql_lsn), Value::Int(millis)))
    }
}

pub(crate) fn history_rows_for_table_guarded(
    repl: &dyn Replication,
    table: &str,
    schema: &TableSchema,
    retention: TemporalRetention,
    budget: &dyn ReadBudget,
) -> Result<GuardedRows, TemporalError> {
    super::validate_retention(retention, retention.min_lsn)?;
    horizon(repl, retention.min_lsn, budget)?;
    let mut versions = versions(repl, table, None, budget)?;
    let horizon = horizon(repl, retention.min_lsn, budget)?;
    let mut entries = std::mem::take(&mut versions.entries).into_iter().peekable();
    let mut timestamps = Timestamps {
        values: BTreeMap::new(),
        reservations: Vec::new(),
    };
    let mut rows = GuardedRows::new();
    while let Some(mut current) = entries.next() {
        budget.check()?;
        while entries
            .peek()
            .is_some_and(|next| next.key == current.key && next.lsn == current.lsn)
        {
            // The old per-key BTreeMap overwrote equal LSNs with their last copy.
            current = entries
                .next()
                .ok_or_else(|| StorageError::Corruption("missing historical version".to_owned()))?;
        }
        if current.lsn < horizon {
            return Err(TemporalError::RetentionExpired {
                requested: current.lsn,
                earliest: horizon,
            });
        }
        if current.lsn < retention.min_lsn {
            continue;
        }
        let Some(bytes) = &current.value else {
            continue;
        };
        let (mut row, reservation) = decode_row(bytes, schema, budget)?;
        // Extending the decoded Vec may grow its entire Value buffer, not just
        // the four boundary columns. Reserve that growth before Vec::extend.
        let extension = budget.reserve(
            row.len()
                .saturating_add(4)
                .saturating_mul(2 * std::mem::size_of::<crate::model::Value>())
                .saturating_add(256),
        )?;
        let (from_lsn, from_time) = timestamps.boundary(repl, current.lsn, budget)?;
        let (to_lsn, to_time) =
            if let Some(next) = entries.peek().filter(|next| next.key == current.key) {
                timestamps.boundary(repl, next.lsn, budget)?
            } else {
                (crate::model::Value::Null, crate::model::Value::Null)
            };
        row.extend([from_lsn, to_lsn, from_time, to_time]);
        rows.push(row, reservation);
        rows.retain_reservation(extension);
    }
    budget.check()?;
    Ok(rows)
}

pub(crate) fn resolve_temporal_point_guarded(
    repl: &dyn Replication,
    point: TemporalPoint,
    budget: &dyn ReadBudget,
) -> Result<u64, TemporalError> {
    budget.check()?;
    let timestamp = match point {
        TemporalPoint::Lsn(lsn) => return Ok(lsn),
        TemporalPoint::Timestamp(timestamp) => timestamp,
    };
    latest_commit(repl, timestamp, budget)?.ok_or(TemporalError::MissingPoint(point))
}

fn latest_commit(
    repl: &dyn Replication,
    timestamp: SystemTime,
    budget: &dyn ReadBudget,
) -> Result<Option<u64>, TemporalError> {
    let earliest =
        if let Some(floor) = metadata_lsn(repl, txn::history::COMMIT_LOG_FLOOR_KEY, budget)? {
            floor
        } else {
            let mut first = None;
            repl.visit_range(
                txn::COMMIT_LOG_TABLE,
                &[],
                &[],
                ReadConsistency::Strong,
                budget,
                &mut |key, _| {
                    first = Some(txn::decode_txn_id(key)?);
                    Ok(false)
                },
            )?;
            first.unwrap_or(1)
        }
        .max(1);
    let mut last = metadata_lsn(repl, txn::CURRENT_TXN_ID_KEY, budget)?.unwrap_or(0);
    while last >= earliest {
        budget.check()?;
        let width = (budget.remaining_bytes() / 128).clamp(1, 256) as u64;
        let first = last.saturating_sub(width - 1).max(earliest);
        let count = usize::try_from(last - first + 1).unwrap_or(256);
        let _window = budget.reserve(count * 4 * std::mem::size_of::<(u64, SystemTime)>() + 256)?;
        let mut times = Vec::new();
        let end = last.checked_add(1).map(txn::commit_log_key);
        repl.visit_range(
            txn::COMMIT_LOG_TABLE,
            &txn::commit_log_key(first),
            end.as_ref().map_or(&[][..], |end| end.as_slice()),
            ReadConsistency::Strong,
            budget,
            &mut |key, bytes| {
                let (lsn, time) = super::commit_metadata::decode_commit_metadata(bytes, budget)?;
                if txn::decode_txn_id(key)? != lsn {
                    return Err(StorageError::Corruption(
                        "temporal commit record does not match its LSN".to_owned(),
                    )
                    .into());
                }
                times.push((lsn, time));
                Ok(true)
            },
        )?;
        if let Some((lsn, _)) = times.into_iter().rev().find(|(_, at)| *at <= timestamp) {
            return Ok(Some(lsn));
        }
        let Some(previous) = first.checked_sub(1) else {
            break;
        };
        last = previous;
    }
    Ok(None)
}
