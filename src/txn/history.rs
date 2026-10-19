//! Bounded commit-log readers and history garbage collection.
//!
//! Commit-log keys are dense eight-byte big-endian LSNs, so every reader here
//! bounds its key range instead of relying on lazy storage iterators: a window
//! `[commit_log_key(a), commit_log_key(b + 1))` holds at most `b - a + 1` rows.
//!
//! History GC is driven by the commit log. Each batch walks commits after the
//! persisted MVCC cursor, prunes every MVCC version that no `AS OF` read at or
//! above the cursor can observe, and records the cursor as the history horizon
//! in the same storage transaction. Commit-log truncation is a separate,
//! explicitly requested step that never passes the MVCC cursor.

use std::{collections::BTreeMap, time::SystemTime};

use crate::keyenc;
use crate::repl::{ReadConsistency, ReplError, Replication};
use crate::storage::{Bytes, ReadTransaction, StorageError, WriteTransaction};
use crate::txn::{
    COMMIT_LOG_TABLE, CURRENT_TXN_ID_KEY, CommitLogRecord, TXN_META_TABLE, TXN_MVCC_TABLE, TxnId,
    commit_log_key, decode_commit_log_record, decode_mvcc_record, decode_txn_id, encode_txn_id,
    last_key_version_from, mvcc_key, version_key,
};

/// `__txn_meta` key holding the lowest LSN whose `AS OF` view is still intact.
pub const HISTORY_HORIZON_KEY: &[u8] = b"history_horizon";
/// `__txn_meta` key holding the last commit LSN processed by MVCC pruning.
pub const MVCC_GC_CURSOR_KEY: &[u8] = b"mvcc_gc_cursor";
/// `__txn_meta` key holding the lowest retained commit-log LSN after truncation.
pub const COMMIT_LOG_FLOOR_KEY: &[u8] = b"commit_log_floor";
/// Default number of commit-log writes examined by one GC batch.
pub const DEFAULT_HISTORY_GC_BATCH_SIZE: usize = 1_024;

const COMMIT_LOG_SCAN_WINDOW: u64 = 256;
const GC_COMMIT_WINDOW: u64 = 64;
const GC_BATCH_RECORD_BYTES: usize = 8 * 1024 * 1024;
/// Commit-log window reads per GC batch, so gaps in the log (for example a legacy
/// layout with no records below the current LSN) cannot keep one batch, and the
/// commit lock, busy for an unbounded number of empty windows.
const GC_BATCH_WINDOWS: usize = 256;

/// Options for one history GC run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoryGcOptions {
    /// Computes watermarks and pending work without writing anything.
    pub dry_run: bool,
    /// Commit-log writes examined per storage transaction; values below one are treated as one.
    pub batch_size: usize,
    /// Stops after this many batches; `None` runs until the work is complete.
    pub max_batches: Option<usize>,
    /// Keeps `AS OF` history for at least this many most recent LSNs, so the
    /// watermark never passes `current - n + 1`. `None` lets the watermark reach
    /// the current LSN, which removes `AS OF` history below it for every table
    /// without a temporal `keep_history` retention.
    pub mvcc_retain_lsns: Option<u64>,
    /// Keeps at least this many most recent commit-log records; `None` never truncates.
    pub commit_log_retain_lsns: Option<u64>,
}

/// The input that limited a GC watermark or commit-log floor.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum HistoryWatermarkSource {
    CurrentLsn,
    AsOfPin,
    TemporalRetention {
        table: String,
    },
    /// `HistoryGcOptions::mvcc_retain_lsns`.
    MvccRetainLsns,
    /// `HistoryGcOptions::commit_log_retain_lsns`.
    RetainLsns,
    MvccCursor,
    Subscription {
        name: String,
    },
    ContinuousQuery {
        name: String,
    },
    MaterializedView {
        name: String,
    },
    FullTextIndex {
        name: String,
    },
    GeoIndex {
        name: String,
    },
}

/// Result of one history GC run or dry run.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct HistoryGcReport {
    pub dry_run: bool,
    pub current_lsn: TxnId,
    /// MVCC versions are pruned only up to this LSN.
    pub watermark: TxnId,
    pub limited_by: HistoryWatermarkSource,
    /// `AS OF` reads below this LSN fail with `RetentionExpired`.
    pub history_horizon: TxnId,
    pub mvcc_cursor: TxnId,
    /// Lowest retained commit-log LSN.
    pub commit_log_floor: TxnId,
    pub commit_log_floor_target: Option<TxnId>,
    pub commit_log_limited_by: Option<HistoryWatermarkSource>,
    pub batches: u64,
    pub mvcc_versions_pruned: u64,
    pub commit_log_records_pruned: u64,
    pub pending_commits: u64,
    pub pending_commit_log_records: u64,
    pub complete: bool,
    /// Set when the watermark is the current LSN and the caller chose no
    /// `mvcc_retain_lsns`: `AS OF` history below it is pruned for every table
    /// without a temporal `keep_history` retention.
    pub note: Option<String>,
}

/// One bounded page of commit-log records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitRecordsPage {
    pub records: Vec<CommitLogRecord>,
    /// True when at least one more record exists after the returned page.
    pub has_more: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct HistoryGcBatchPlan {
    pub(crate) watermark: TxnId,
    pub(crate) max_writes: usize,
    pub(crate) max_record_bytes: usize,
    /// Commit-log window reads per batch, including empty windows.
    pub(crate) max_windows: usize,
    pub(crate) commit_log_floor_target: Option<TxnId>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct HistoryGcBatchOutcome {
    pub(crate) mvcc_versions_pruned: u64,
    pub(crate) commit_log_records_pruned: u64,
    pub(crate) mvcc_cursor: TxnId,
    pub(crate) history_horizon: TxnId,
    pub(crate) commit_log_floor: TxnId,
    pub(crate) done: bool,
    /// True when the batch staged writes that must be committed.
    pub(crate) changed: bool,
}

/// Persisted GC progress read from one storage snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct HistoryGcState {
    pub(crate) mvcc_cursor: TxnId,
    pub(crate) history_horizon: TxnId,
    pub(crate) commit_log_floor: TxnId,
}

type LatestKeyVersions = BTreeMap<(String, Bytes), TxnId>;

impl Default for HistoryGcOptions {
    fn default() -> Self {
        Self {
            dry_run: false,
            batch_size: DEFAULT_HISTORY_GC_BATCH_SIZE,
            max_batches: None,
            mvcc_retain_lsns: None,
            commit_log_retain_lsns: None,
        }
    }
}

impl HistoryGcBatchPlan {
    #[must_use]
    pub(crate) fn new(
        watermark: TxnId,
        max_writes: usize,
        commit_log_floor_target: Option<TxnId>,
    ) -> Self {
        Self {
            watermark,
            max_writes: max_writes.max(1),
            max_record_bytes: GC_BATCH_RECORD_BYTES,
            max_windows: GC_BATCH_WINDOWS,
            commit_log_floor_target,
        }
    }

    /// Effective truncation target: never above the watermark or past the MVCC cursor.
    fn floor_target(&self, mvcc_cursor: TxnId) -> Option<TxnId> {
        self.commit_log_floor_target.map(|target| {
            target
                .min(self.watermark)
                .min(mvcc_cursor.saturating_add(1))
        })
    }
}

/// Reads one history GC metadata value from `__txn_meta`.
/// # Errors
/// Fails when storage cannot be read or the stored LSN is not eight bytes.
pub fn read_history_meta(
    txn: &impl ReadTransaction,
    key: &[u8],
) -> Result<Option<TxnId>, StorageError> {
    txn.get(TXN_META_TABLE, key)?
        .map(|bytes| decode_txn_id(&bytes))
        .transpose()
}

/// Reads the history horizon; `AS OF` reads below it are no longer intact.
/// # Errors
/// Fails when the replication backend cannot read metadata or it is corrupt.
pub fn history_horizon<R: Replication + ?Sized>(repl: &R) -> Result<TxnId, ReplError> {
    Ok(read_repl_meta(repl, HISTORY_HORIZON_KEY)?.unwrap_or(0))
}

/// Returns the lowest retained commit-log LSN without scanning the log.
///
/// Uses the truncation floor when one was recorded, then a point read of LSN 1.
/// Only legacy layouts without both fall back to a first-key range read. An
/// empty log returns 1.
/// # Errors
/// Fails when the replication backend cannot read the log or metadata is corrupt.
pub fn earliest_commit_lsn<R: Replication + ?Sized>(repl: &R) -> Result<TxnId, ReplError> {
    if let Some(floor) = read_repl_meta(repl, COMMIT_LOG_FLOOR_KEY)? {
        return Ok(floor);
    }
    if repl
        .read(
            COMMIT_LOG_TABLE,
            &commit_log_key(1),
            ReadConsistency::Strong,
        )?
        .is_some()
    {
        return Ok(1);
    }
    let rows = repl.range(COMMIT_LOG_TABLE, &[], &[], ReadConsistency::Strong)?;
    rows.first()
        .map_or(Ok(1), |(key, _)| decode_txn_id(key).map_err(Into::into))
}

/// Reads at most `limit + 1` commit records in `(after, through]`.
///
/// Returns at most `limit` records; `has_more` reports whether another record
/// follows them. Missing LSNs inside the range are skipped. `through` is capped
/// at the current committed LSN.
/// # Errors
/// Fails when the replication backend cannot read the log or a record is corrupt.
pub fn commit_records_after<R: Replication + ?Sized>(
    repl: &R,
    after: TxnId,
    through: TxnId,
    limit: usize,
) -> Result<CommitRecordsPage, ReplError> {
    let limit = limit.max(1);
    let wanted = limit.saturating_add(1);
    let through = through.min(current_lsn(repl)?);
    let mut records = Vec::new();
    let mut next = after.checked_add(1);
    while let Some(first) = next {
        if first > through || records.len() >= wanted {
            break;
        }
        let needed = u64::try_from(wanted - records.len()).unwrap_or(u64::MAX);
        let last = first.saturating_add(needed - 1).min(through);
        for (_, value) in commit_log_window(repl, first, last)? {
            records.push(decode_commit_log_record(&value)?);
        }
        next = last.checked_add(1);
    }
    let has_more = records.len() > limit;
    records.truncate(limit);
    Ok(CommitRecordsPage { records, has_more })
}

/// Returns the highest retained LSN whose commit time is at or before `timestamp`.
///
/// Scans the log backwards in bounded windows, so recent timestamps are cheap
/// and memory stays bounded. Non-monotonic clocks give the same answer as a
/// full forward scan.
/// # Errors
/// Fails when the replication backend cannot read the log or a record is corrupt.
pub fn latest_commit_at_or_before<R: Replication + ?Sized>(
    repl: &R,
    timestamp: SystemTime,
) -> Result<Option<TxnId>, ReplError> {
    let earliest = earliest_commit_lsn(repl)?.max(1);
    let mut last = current_lsn(repl)?;
    while last >= earliest {
        let first = last
            .saturating_sub(COMMIT_LOG_SCAN_WINDOW - 1)
            .max(earliest);
        for (_, value) in commit_log_window(repl, first, last)?.iter().rev() {
            let record = decode_commit_log_record(value)?;
            if record.committed_at <= timestamp {
                return Ok(Some(record.txn_id));
            }
        }
        let Some(previous) = first.checked_sub(1) else {
            break;
        };
        last = previous;
    }
    Ok(None)
}

/// Reads persisted GC progress from one storage snapshot.
/// # Errors
/// Fails when metadata or the commit log cannot be read or is corrupt.
pub(crate) fn read_history_gc_state(
    read: &impl ReadTransaction,
) -> Result<HistoryGcState, StorageError> {
    Ok(HistoryGcState {
        mvcc_cursor: read_history_meta(read, MVCC_GC_CURSOR_KEY)?.unwrap_or(0),
        history_horizon: read_history_meta(read, HISTORY_HORIZON_KEY)?.unwrap_or(0),
        commit_log_floor: earliest_commit_lsn_in(read)?,
    })
}

/// Runs one bounded history GC batch.
///
/// Every read goes through `read`, a snapshot of the base state of `write`; every
/// delete and metadata put is staged on `write`. While the MVCC cursor is below
/// the watermark the batch walks commits after the cursor, keeps the newest
/// version at or below each touched key's latest processed commit and deletes the
/// older ones, then stores the new cursor as the history horizon. Once the cursor
/// reaches the watermark, later batches truncate the commit log below the target
/// floor. `__txn_versions`, the transaction id and versions above the watermark
/// are never touched.
/// # Errors
/// Fails closed, staging nothing the caller should commit, when metadata, MVCC
/// versions or commit records are corrupt, or storage rejects a read or write.
pub(crate) fn gc_history_batch<R: ReadTransaction, W: WriteTransaction>(
    read: &R,
    write: &mut W,
    plan: &HistoryGcBatchPlan,
) -> Result<HistoryGcBatchOutcome, StorageError> {
    let state = read_history_gc_state(read)?;
    let mut outcome = HistoryGcBatchOutcome {
        mvcc_cursor: state.mvcc_cursor,
        history_horizon: state.history_horizon,
        commit_log_floor: state.commit_log_floor,
        ..HistoryGcBatchOutcome::default()
    };

    if state.mvcc_cursor < plan.watermark {
        prune_mvcc_batch(read, write, plan, &mut outcome)?;
    } else if let Some(target) = plan.floor_target(state.mvcc_cursor)
        && state.commit_log_floor < target
    {
        truncate_commit_log_batch(read, write, plan, target, &mut outcome)?;
    }

    outcome.done = outcome.mvcc_cursor >= plan.watermark
        && plan
            .floor_target(outcome.mvcc_cursor)
            .is_none_or(|target| outcome.commit_log_floor >= target);
    Ok(outcome)
}

fn prune_mvcc_batch<R: ReadTransaction, W: WriteTransaction>(
    read: &R,
    write: &mut W,
    plan: &HistoryGcBatchPlan,
    outcome: &mut HistoryGcBatchOutcome,
) -> Result<(), StorageError> {
    let (latest_versions, cursor) = collect_processed_writes(read, outcome.mvcc_cursor, plan)?;
    for ((table, key), newest) in &latest_versions {
        let pruned = prune_key_versions(read, write, table, key, *newest)?;
        outcome.mvcc_versions_pruned = outcome.mvcc_versions_pruned.saturating_add(pruned);
    }

    let horizon = outcome.history_horizon.max(cursor);
    write.put(TXN_META_TABLE, MVCC_GC_CURSOR_KEY, &encode_txn_id(cursor))?;
    write.put(TXN_META_TABLE, HISTORY_HORIZON_KEY, &encode_txn_id(horizon))?;
    outcome.mvcc_cursor = cursor;
    outcome.history_horizon = horizon;
    outcome.changed = true;
    Ok(())
}

/// Walks commits after `cursor` up to the watermark within the batch budget.
/// Returns the latest processed version per key and the last processed LSN; an
/// empty window inside the range also counts as processed, and at most
/// `plan.max_windows` windows are read.
fn collect_processed_writes(
    read: &impl ReadTransaction,
    cursor: TxnId,
    plan: &HistoryGcBatchPlan,
) -> Result<(LatestKeyVersions, TxnId), StorageError> {
    let mut latest = LatestKeyVersions::new();
    let mut examined = 0_usize;
    let mut decoded_bytes = 0_usize;
    let mut windows = 0_usize;
    let mut processed = cursor;
    while processed < plan.watermark && windows < plan.max_windows.max(1) {
        windows = windows.saturating_add(1);
        let first = processed.saturating_add(1);
        let remaining = u64::try_from(plan.max_writes.saturating_sub(examined))
            .unwrap_or(u64::MAX)
            .clamp(1, GC_COMMIT_WINDOW);
        let last = first.saturating_add(remaining - 1).min(plan.watermark);
        let end = commit_log_key(last.saturating_add(1));
        for row in read.range(COMMIT_LOG_TABLE, &commit_log_key(first), &end)? {
            let (key, value) = row?;
            let lsn = decode_txn_id(&key)?;
            let record = decode_commit_log_record(&value)?;
            if record.txn_id != lsn {
                return Err(StorageError::Corruption(format!(
                    "commit log key {lsn} holds record {}",
                    record.txn_id
                )));
            }
            decoded_bytes = decoded_bytes.saturating_add(value.len());
            examined = examined.saturating_add(record.writes.len().max(1));
            for write in record.writes {
                latest.insert((write.table, write.key), lsn);
            }
            processed = lsn;
            if examined >= plan.max_writes || decoded_bytes >= plan.max_record_bytes {
                return Ok((latest, processed));
            }
        }
        processed = last;
    }
    Ok((latest, processed))
}

/// Deletes every MVCC version of one key below `newest`, keeping `newest` itself.
///
/// When `newest` has no MVCC row (for example a database restored before MVCC
/// rebuilds existed) older versions are kept, because they may be the only
/// history left. A kept tombstone is removed too when it is the key's last
/// committed version and the base table agrees that the key is absent.
fn prune_key_versions<R: ReadTransaction, W: WriteTransaction>(
    read: &R,
    write: &mut W,
    table: &str,
    key: &[u8],
    newest: TxnId,
) -> Result<u64, StorageError> {
    let prefix = version_key(table, key)?;
    let start = mvcc_key(table, key, 0)?;
    let end = match newest.checked_add(1) {
        Some(next) => mvcc_key(table, key, next)?,
        None => keyenc::range_end(&prefix),
    };

    let mut older = Vec::new();
    let mut kept: Option<(TxnId, Bytes, Bytes)> = None;
    for row in read.range(TXN_MVCC_TABLE, &start, &end)? {
        let (version_row, value) = row?;
        if version_row.len() != prefix.len() + 8 || !version_row.starts_with(&prefix) {
            continue;
        }
        let version = decode_txn_id(&version_row[prefix.len()..])?;
        if let Some((_, previous, _)) = kept.replace((version, version_row, value)) {
            older.push(previous);
        }
    }

    let Some((kept_version, kept_row, kept_value)) = kept else {
        return Ok(0);
    };
    if kept_version != newest {
        return Ok(0);
    }

    let mut pruned = 0_u64;
    for version_row in older {
        write.delete(TXN_MVCC_TABLE, &version_row)?;
        pruned = pruned.saturating_add(1);
    }
    if decode_mvcc_record(&kept_value)?.value.is_none()
        && last_key_version_from(read, table, key)? == kept_version
        && read.get(table, key)?.is_none()
    {
        write.delete(TXN_MVCC_TABLE, &kept_row)?;
        pruned = pruned.saturating_add(1);
    }
    Ok(pruned)
}

/// Deletes commit-log records in `[floor, target)` within the batch budget and
/// records the new floor. Only keys returned by the window reads are deleted, and
/// at most `plan.max_windows` windows are read, empty ones included.
fn truncate_commit_log_batch<R: ReadTransaction, W: WriteTransaction>(
    read: &R,
    write: &mut W,
    plan: &HistoryGcBatchPlan,
    target: TxnId,
    outcome: &mut HistoryGcBatchOutcome,
) -> Result<(), StorageError> {
    let mut floor = outcome.commit_log_floor;
    let mut deleted = 0_usize;
    let mut deleted_bytes = 0_usize;
    let mut windows = 0_usize;
    while floor < target
        && deleted < plan.max_writes
        && deleted_bytes < plan.max_record_bytes
        && windows < plan.max_windows.max(1)
    {
        windows = windows.saturating_add(1);
        let remaining = u64::try_from(plan.max_writes - deleted)
            .unwrap_or(u64::MAX)
            .clamp(1, GC_COMMIT_WINDOW);
        let end = floor.saturating_add(remaining).min(target);
        for row in read.range(
            COMMIT_LOG_TABLE,
            &commit_log_key(floor),
            &commit_log_key(end),
        )? {
            let (key, value) = row?;
            write.delete(COMMIT_LOG_TABLE, &key)?;
            deleted = deleted.saturating_add(1);
            deleted_bytes = deleted_bytes.saturating_add(value.len());
        }
        floor = end;
    }

    write.put(TXN_META_TABLE, COMMIT_LOG_FLOOR_KEY, &encode_txn_id(floor))?;
    outcome.commit_log_records_pruned = u64::try_from(deleted).unwrap_or(u64::MAX);
    outcome.commit_log_floor = floor;
    outcome.changed = true;
    Ok(())
}

fn earliest_commit_lsn_in(read: &impl ReadTransaction) -> Result<TxnId, StorageError> {
    if let Some(floor) = read_history_meta(read, COMMIT_LOG_FLOOR_KEY)? {
        return Ok(floor);
    }
    if read.get(COMMIT_LOG_TABLE, &commit_log_key(1))?.is_some() {
        return Ok(1);
    }
    let mut rows = read.range(COMMIT_LOG_TABLE, &[], &[])?;
    rows.next()
        .transpose()?
        .map_or(Ok(1), |(key, _)| decode_txn_id(&key))
}

fn commit_log_window<R: Replication + ?Sized>(
    repl: &R,
    first: TxnId,
    last: TxnId,
) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
    let start = commit_log_key(first);
    let end = last.checked_add(1).map(commit_log_key);
    let end = end.as_ref().map_or(&[][..], <[u8; 8]>::as_slice);
    repl.range(COMMIT_LOG_TABLE, &start, end, ReadConsistency::Strong)
}

fn read_repl_meta<R: Replication + ?Sized>(
    repl: &R,
    key: &[u8],
) -> Result<Option<TxnId>, ReplError> {
    repl.read(TXN_META_TABLE, key, ReadConsistency::Strong)?
        .map(|bytes| decode_txn_id(&bytes).map_err(Into::into))
        .transpose()
}

fn current_lsn<R: Replication + ?Sized>(repl: &R) -> Result<TxnId, ReplError> {
    Ok(read_repl_meta(repl, CURRENT_TXN_ID_KEY)?.unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        sync::atomic::{AtomicUsize, Ordering},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use super::{
        COMMIT_LOG_FLOOR_KEY, CommitRecordsPage, GC_BATCH_WINDOWS, GC_COMMIT_WINDOW,
        HISTORY_HORIZON_KEY, HistoryGcBatchOutcome, HistoryGcBatchPlan, MVCC_GC_CURSOR_KEY,
        commit_records_after, earliest_commit_lsn, gc_history_batch, latest_commit_at_or_before,
        read_history_meta,
    };
    use crate::{
        keyenc,
        repl::{Op, ReadConsistency, ReplError, Replication, SingleNode},
        storage::{
            Bytes, EncryptedEngine, MemEngine, RangeIter, ReadTransaction, StaticKeyProvider,
            StorageEngine, StorageError, WriteTransaction,
        },
        txn::{
            COMMIT_LOG_TABLE, CURRENT_TXN_ID_KEY, CommitLogRecord, TXN_META_TABLE, TXN_MVCC_TABLE,
            TxnId, WriteAuthorization, WriteSet, commit_log_key, commit_ops_at_current,
            commit_write_set, decode_mvcc_record, decode_txn_id, encode_commit_log_record,
            encode_txn_id, last_key_version_from, mvcc_key, version_key,
        },
    };

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    struct CountingRead<'a, T> {
        inner: &'a T,
        ranges: RefCell<Vec<(String, usize)>>,
    }

    impl<T: ReadTransaction> ReadTransaction for CountingRead<'_, T> {
        fn get(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
            self.inner.get(table, key)
        }

        fn range<'txn>(
            &'txn self,
            table: &str,
            start: &[u8],
            end: &[u8],
        ) -> Result<RangeIter<'txn>, StorageError> {
            let rows = self
                .inner
                .range(table, start, end)?
                .collect::<Result<Vec<_>, _>>()?;
            self.ranges
                .borrow_mut()
                .push((table.to_owned(), rows.len()));
            Ok(Box::new(rows.into_iter().map(Ok)))
        }
    }

    struct CountingRepl<R> {
        inner: R,
        commit_log_rows: AtomicUsize,
    }

    impl<R> CountingRepl<R> {
        fn new(inner: R) -> Self {
            Self {
                inner,
                commit_log_rows: AtomicUsize::new(0),
            }
        }

        fn take_rows(&self) -> usize {
            self.commit_log_rows.swap(0, Ordering::SeqCst)
        }
    }

    impl<R: Replication> Replication for CountingRepl<R> {
        fn propose(&self, op: Op) -> Result<(), ReplError> {
            self.inner.propose(op)
        }

        fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
            self.inner.propose_batch(ops)
        }

        fn propose_authorized_batch(
            &self,
            ops: Vec<Op>,
            authorization: WriteAuthorization,
        ) -> Result<(), ReplError> {
            self.inner.propose_authorized_batch(ops, authorization)
        }

        fn read(
            &self,
            table: &str,
            key: &[u8],
            consistency: ReadConsistency,
        ) -> Result<Option<Bytes>, ReplError> {
            self.inner.read(table, key, consistency)
        }

        fn range(
            &self,
            table: &str,
            start: &[u8],
            end: &[u8],
            consistency: ReadConsistency,
        ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
            let rows = self.inner.range(table, start, end, consistency)?;
            if table == COMMIT_LOG_TABLE {
                self.commit_log_rows.fetch_add(rows.len(), Ordering::SeqCst);
            }
            Ok(rows)
        }
    }

    fn put<S: StorageEngine>(engine: &S, key: &[u8], value: &[u8]) -> Result<TxnId, StorageError> {
        commit_ops_at_current(
            engine,
            vec![Op::Put {
                table: "t".to_owned(),
                key: key.to_vec(),
                value: value.to_vec(),
            }],
        )
    }

    fn delete<S: StorageEngine>(engine: &S, key: &[u8]) -> Result<TxnId, StorageError> {
        commit_ops_at_current(
            engine,
            vec![Op::Delete {
                table: "t".to_owned(),
                key: key.to_vec(),
            }],
        )
    }

    fn run_batch<S: StorageEngine>(
        engine: &S,
        plan: &HistoryGcBatchPlan,
    ) -> Result<HistoryGcBatchOutcome, StorageError> {
        let mut write = engine.begin_write()?;
        let read = engine.begin_read()?;
        let result = gc_history_batch(&read, &mut write, plan);
        drop(read);
        match result {
            Ok(outcome) => {
                write.commit()?;
                Ok(outcome)
            }
            Err(error) => {
                write.rollback();
                Err(error)
            }
        }
    }

    /// Runs one batch and also returns how many commit-log windows it read.
    fn run_counted_batch<S: StorageEngine>(
        engine: &S,
        plan: &HistoryGcBatchPlan,
    ) -> Result<(HistoryGcBatchOutcome, usize), StorageError> {
        let mut write = engine.begin_write()?;
        let inner = engine.begin_read()?;
        let read = CountingRead {
            inner: &inner,
            ranges: RefCell::new(Vec::new()),
        };
        let result = gc_history_batch(&read, &mut write, plan);
        let windows = read
            .ranges
            .borrow()
            .iter()
            .filter(|(table, _)| table == COMMIT_LOG_TABLE)
            .count();
        drop(read);
        drop(inner);
        match result {
            Ok(outcome) => {
                write.commit()?;
                Ok((outcome, windows))
            }
            Err(error) => {
                write.rollback();
                Err(error)
            }
        }
    }

    fn run_until_done<S: StorageEngine>(
        engine: &S,
        plan: &HistoryGcBatchPlan,
    ) -> Result<Vec<HistoryGcBatchOutcome>, StorageError> {
        let mut outcomes = Vec::new();
        loop {
            let outcome = run_batch(engine, plan)?;
            outcomes.push(outcome);
            if outcome.done {
                return Ok(outcomes);
            }
            if outcomes.len() > 1_000 {
                return Err(StorageError::Backend(
                    "history GC did not finish".to_owned(),
                ));
            }
        }
    }

    fn mvcc_versions(read: &impl ReadTransaction, key: &[u8]) -> Result<Vec<TxnId>, StorageError> {
        let prefix = version_key("t", key)?;
        let end = keyenc::range_end(&prefix);
        let mut versions = Vec::new();
        for row in read.range(TXN_MVCC_TABLE, &prefix, &end)? {
            let (version_row, _) = row?;
            if version_row.len() == prefix.len() + 8 {
                versions.push(decode_txn_id(&version_row[prefix.len()..])?);
            }
        }
        Ok(versions)
    }

    /// Brute-force `AS OF` lookup: the newest MVCC version at or below `at`.
    fn visible_at(
        read: &impl ReadTransaction,
        key: &[u8],
        at: TxnId,
    ) -> Result<Option<(TxnId, Option<Bytes>)>, StorageError> {
        let mut visible = None;
        for version in mvcc_versions(read, key)? {
            if version <= at {
                let bytes = read
                    .get(TXN_MVCC_TABLE, &mvcc_key("t", key, version)?)?
                    .ok_or(StorageError::NotFound)?;
                visible = Some((version, decode_mvcc_record(&bytes)?.value));
            }
        }
        Ok(visible)
    }

    fn commit_log_keys(read: &impl ReadTransaction) -> Result<Vec<TxnId>, StorageError> {
        read.range(COMMIT_LOG_TABLE, &[], &[])?
            .map(|row| row.and_then(|(key, _)| decode_txn_id(&key)))
            .collect()
    }

    fn single_node_with_commits(count: u8) -> Result<SingleNode<MemEngine>, ReplError> {
        let node = SingleNode::new(MemEngine::new());
        for value in 0..count {
            node.propose(Op::Put {
                table: "t".to_owned(),
                key: vec![value],
                value: vec![value],
            })?;
        }
        Ok(node)
    }

    #[test]
    fn gc_batch_keeps_newest_version_at_or_below_watermark() -> TestResult {
        let engine = MemEngine::new();
        for value in 1..=6_u8 {
            put(&engine, b"k", &[value])?;
        }
        put(&engine, b"other", b"7")?;
        let visible = |read: &<MemEngine as StorageEngine>::ReadTxn<'_>| {
            (4..=7)
                .map(|at| Ok((visible_at(read, b"k", at)?, visible_at(read, b"other", at)?)))
                .collect::<Result<Vec<_>, StorageError>>()
        };
        let before = visible(&engine.begin_read()?)?;

        let outcome = run_batch(&engine, &HistoryGcBatchPlan::new(4, 1_024, None))?;
        assert_eq!(outcome.mvcc_cursor, 4);
        assert_eq!(outcome.history_horizon, 4);
        assert_eq!(outcome.mvcc_versions_pruned, 3);
        assert!(outcome.done);

        let read = engine.begin_read()?;
        assert_eq!(mvcc_versions(&read, b"k")?, vec![4, 5, 6]);
        assert_eq!(mvcc_versions(&read, b"other")?, vec![7]);
        assert_eq!(visible(&read)?, before);
        assert_eq!(read_history_meta(&read, MVCC_GC_CURSOR_KEY)?, Some(4));
        assert_eq!(read_history_meta(&read, HISTORY_HORIZON_KEY)?, Some(4));
        assert_eq!(last_key_version_from(&read, "t", b"k")?, 6);
        assert_eq!(commit_log_keys(&read)?, (1..=7).collect::<Vec<_>>());
        Ok(())
    }

    #[test]
    fn gc_batch_is_bounded_resumable_and_dedups_hot_keys() -> TestResult {
        let engine = MemEngine::new();
        for value in 1..=10_u8 {
            put(&engine, b"hot", &[value])?;
        }
        let plan = HistoryGcBatchPlan::new(10, 3, None);
        let mut cursors = Vec::new();
        let mut pruned = 0;
        loop {
            let mut write = engine.begin_write()?;
            let inner = engine.begin_read()?;
            let read = CountingRead {
                inner: &inner,
                ranges: RefCell::new(Vec::new()),
            };
            let outcome = gc_history_batch(&read, &mut write, &plan)?;
            let mvcc_reads = read
                .ranges
                .borrow()
                .iter()
                .filter(|(table, _)| table == TXN_MVCC_TABLE)
                .map(|(_, rows)| *rows)
                .collect::<Vec<_>>();
            drop(read);
            write.commit()?;

            assert_eq!(mvcc_reads.len(), 1, "one MVCC range read per hot key");
            assert!(mvcc_reads.iter().all(|rows| *rows <= 4), "{mvcc_reads:?}");
            let previous = cursors.last().copied().unwrap_or(0);
            assert!(outcome.mvcc_cursor > previous && outcome.mvcc_cursor - previous <= 3);
            assert_eq!(outcome.history_horizon, outcome.mvcc_cursor);
            cursors.push(outcome.mvcc_cursor);
            pruned += outcome.mvcc_versions_pruned;
            if outcome.done {
                break;
            }
            assert!(cursors.len() < 10, "history GC did not finish");
        }

        assert_eq!(cursors, vec![3, 6, 9, 10]);
        assert_eq!(pruned, 9);
        assert_eq!(mvcc_versions(&engine.begin_read()?, b"hot")?, vec![10]);
        Ok(())
    }

    #[test]
    fn gc_batch_drops_tombstone_only_when_unreachable() -> TestResult {
        let engine = MemEngine::new();
        put(&engine, b"a", b"1")?;
        delete(&engine, b"a")?;
        put(&engine, b"b", b"3")?;
        delete(&engine, b"b")?;
        put(&engine, b"c", b"5")?;
        put(&engine, b"b", b"6")?;

        run_until_done(&engine, &HistoryGcBatchPlan::new(5, 1_024, None))?;

        let read = engine.begin_read()?;
        assert!(
            mvcc_versions(&read, b"a")?.is_empty(),
            "expected an empty value, got {:?}",
            mvcc_versions(&read, b"a")?
        );
        assert_eq!(last_key_version_from(&read, "t", b"a")?, 2);
        assert_eq!(mvcc_versions(&read, b"b")?, vec![4, 6]);
        assert_eq!(visible_at(&read, b"b", 5)?, Some((4, None)));
        drop(read);

        let mut stale = WriteSet::new();
        stale.insert(("t".to_owned(), b"a".to_vec()), Some(b"stale".to_vec()));
        assert!(matches!(
            commit_write_set(&engine, 1, stale),
            Err(StorageError::Conflict)
        ));
        Ok(())
    }

    #[test]
    fn gc_batch_keeps_history_when_newest_version_is_missing() -> TestResult {
        let engine = MemEngine::new();
        for value in 1..=3_u8 {
            put(&engine, b"k", &[value])?;
        }
        let mut write = engine.begin_write()?;
        write.delete(TXN_MVCC_TABLE, &mvcc_key("t", b"k", 3)?)?;
        write.commit()?;

        let outcomes = run_until_done(&engine, &HistoryGcBatchPlan::new(3, 1_024, None))?;

        let pruned = outcomes
            .iter()
            .map(|outcome| outcome.mvcc_versions_pruned)
            .sum::<u64>();
        assert_eq!(pruned, 0);
        assert_eq!(mvcc_versions(&engine.begin_read()?, b"k")?, vec![1, 2]);
        Ok(())
    }

    #[test]
    fn gc_batch_fails_closed_on_corrupt_commit_record() -> TestResult {
        let engine = MemEngine::new();
        for value in 1..=3_u8 {
            put(&engine, b"k", &[value])?;
        }
        let mut write = engine.begin_write()?;
        write.put(COMMIT_LOG_TABLE, &commit_log_key(2), b"garbage")?;
        write.commit()?;

        assert!(matches!(
            run_batch(&engine, &HistoryGcBatchPlan::new(3, 1_024, None)),
            Err(StorageError::Corruption(_))
        ));

        let read = engine.begin_read()?;
        assert_eq!(mvcc_versions(&read, b"k")?, vec![1, 2, 3]);
        assert_eq!(read_history_meta(&read, MVCC_GC_CURSOR_KEY)?, None);
        assert_eq!(read_history_meta(&read, HISTORY_HORIZON_KEY)?, None);
        Ok(())
    }

    #[test]
    fn gc_batch_truncates_commit_log_only_below_cursor_and_target() -> TestResult {
        let engine = MemEngine::new();
        for value in 1..=8_u8 {
            put(&engine, b"k", &[value])?;
        }
        let outcomes = run_until_done(&engine, &HistoryGcBatchPlan::new(5, 1_024, Some(8)))?;
        assert_eq!(
            outcomes.len(),
            2,
            "pruning and truncation run in separate batches"
        );
        assert_eq!(outcomes[0].commit_log_records_pruned, 0);
        assert_eq!(outcomes[1].commit_log_records_pruned, 4);
        let read = engine.begin_read()?;
        assert_eq!(read_history_meta(&read, COMMIT_LOG_FLOOR_KEY)?, Some(5));
        assert_eq!(commit_log_keys(&read)?, (5..=8).collect::<Vec<_>>());
        drop(read);

        let engine = MemEngine::new();
        for value in 1..=8_u8 {
            put(&engine, b"k", &[value])?;
        }
        let plan = HistoryGcBatchPlan::new(5, 2, Some(8));
        let mut floors = Vec::new();
        loop {
            let outcome = run_batch(&engine, &plan)?;
            assert!(outcome.commit_log_records_pruned <= 2);
            if outcome.commit_log_records_pruned > 0 {
                let read = engine.begin_read()?;
                let floor = read_history_meta(&read, COMMIT_LOG_FLOOR_KEY)?;
                assert_eq!(floor, commit_log_keys(&read)?.first().copied());
                floors.push(floor);
            }
            if outcome.done {
                break;
            }
            assert!(floors.len() < 10, "history GC did not finish");
        }
        assert_eq!(floors, vec![Some(3), Some(5)]);

        let engine = MemEngine::new();
        for value in 1..=8_u8 {
            put(&engine, b"k", &[value])?;
        }
        run_until_done(&engine, &HistoryGcBatchPlan::new(5, 1_024, None))?;
        let read = engine.begin_read()?;
        assert_eq!(read_history_meta(&read, COMMIT_LOG_FLOOR_KEY)?, None);
        assert_eq!(commit_log_keys(&read)?, (1..=8).collect::<Vec<_>>());
        Ok(())
    }

    #[test]
    fn gc_batch_window_budget_bounds_commit_log_gaps() -> TestResult {
        let engine = MemEngine::new();
        for value in 1..=3_u8 {
            put(&engine, b"k", &[value])?;
        }
        // A legacy layout: the current LSN moved on with no commit records in between.
        let mut write = engine.begin_write()?;
        write.put(TXN_META_TABLE, CURRENT_TXN_ID_KEY, &encode_txn_id(20_000))?;
        write.commit()?;
        let last = put(&engine, b"k", b"last")?;
        assert_eq!(last, 20_001);

        // The default budget ends the first batch inside the gap.
        let plan = HistoryGcBatchPlan::new(last, 1_024, Some(last));
        let (outcome, windows) = run_counted_batch(&engine, &plan)?;
        assert_eq!(windows, GC_BATCH_WINDOWS);
        assert_eq!(
            outcome.mvcc_cursor,
            u64::try_from(GC_BATCH_WINDOWS)? * GC_COMMIT_WINDOW
        );
        assert_eq!(outcome.history_horizon, outcome.mvcc_cursor);
        assert!(!outcome.done);

        // Pruning and truncation resume from the persisted cursor and floor.
        let mut plan = plan;
        plan.max_windows = 4;
        let mut previous = outcome;
        let mut batches = 0;
        while !previous.done {
            let (outcome, windows) = run_counted_batch(&engine, &plan)?;
            assert!(windows <= 4, "{windows} windows in one batch");
            assert!(
                outcome.mvcc_cursor > previous.mvcc_cursor
                    || outcome.commit_log_floor > previous.commit_log_floor,
                "batch made no progress: {outcome:?}"
            );
            previous = outcome;
            batches += 1;
            assert!(batches < 1_000, "history GC did not finish");
        }

        let read = engine.begin_read()?;
        assert_eq!(mvcc_versions(&read, b"k")?, vec![last]);
        assert_eq!(commit_log_keys(&read)?, vec![last]);
        assert_eq!(read_history_meta(&read, MVCC_GC_CURSOR_KEY)?, Some(last));
        assert_eq!(read_history_meta(&read, COMMIT_LOG_FLOOR_KEY)?, Some(last));
        Ok(())
    }

    #[test]
    fn gc_batch_on_encrypted_engine_does_not_create_version_rows() -> TestResult {
        // Mirrors the private encryption-version table name in `storage::encrypted`.
        const ENCRYPTION_VERSIONS_TABLE: &str = "__enc_versions";
        fn version_rows(
            engine: &EncryptedEngine<MemEngine, StaticKeyProvider>,
        ) -> Result<usize, StorageError> {
            Ok(engine
                .inner()
                .begin_read()?
                .range(ENCRYPTION_VERSIONS_TABLE, &[], &[])?
                .count())
        }

        let engine = EncryptedEngine::new(MemEngine::new(), StaticKeyProvider::new([7; 32]));
        for value in 1..=3_u8 {
            put(&engine, b"k", &[value])?;
        }
        // The first run creates the GC cursor and horizon metadata rows.
        run_until_done(&engine, &HistoryGcBatchPlan::new(3, 1_024, None))?;
        for value in 4..=8_u8 {
            put(&engine, b"k", &[value])?;
        }
        let before = version_rows(&engine)?;

        let outcomes = run_until_done(&engine, &HistoryGcBatchPlan::new(8, 1_024, None))?;

        let pruned = outcomes
            .iter()
            .map(|outcome| outcome.mvcc_versions_pruned)
            .sum::<u64>();
        assert_eq!(pruned, 5);
        assert_eq!(version_rows(&engine)?, before);
        assert_eq!(mvcc_versions(&engine.begin_read()?, b"k")?, vec![8]);
        Ok(())
    }

    #[test]
    fn commit_records_after_reads_at_most_limit_plus_one_records() -> TestResult {
        fn lsns(page: &CommitRecordsPage) -> Vec<TxnId> {
            page.records.iter().map(|record| record.txn_id).collect()
        }

        let repl = CountingRepl::new(single_node_with_commits(100)?);

        let page = commit_records_after(&repl, 10, 100, 5)?;
        assert_eq!(lsns(&page), (11..=15).collect::<Vec<_>>());
        assert!(page.has_more);
        assert!(repl.take_rows() <= 6);

        let page = commit_records_after(&repl, 95, 100, 10)?;
        assert_eq!(lsns(&page), (96..=100).collect::<Vec<_>>());
        assert!(!page.has_more);

        let page = commit_records_after(&repl, 98, u64::MAX, 5)?;
        assert_eq!(lsns(&page), vec![99, 100]);
        assert!(!page.has_more);

        let mut write = repl.inner.storage().begin_write()?;
        for lsn in 20..=30 {
            write.delete(COMMIT_LOG_TABLE, &commit_log_key(lsn))?;
        }
        write.commit()?;
        let page = commit_records_after(&repl, 15, 100, 5)?;
        assert_eq!(lsns(&page), vec![16, 17, 18, 19, 31]);
        assert!(page.has_more);
        Ok(())
    }

    #[test]
    fn earliest_commit_lsn_uses_floor_then_point_get() -> TestResult {
        let empty = CountingRepl::new(SingleNode::new(MemEngine::new()));
        assert_eq!(earliest_commit_lsn(&empty)?, 1);

        let repl = CountingRepl::new(single_node_with_commits(50)?);
        repl.take_rows();
        assert_eq!(earliest_commit_lsn(&repl)?, 1);
        assert_eq!(repl.take_rows(), 0);

        let mut write = repl.inner.storage().begin_write()?;
        write.put(TXN_META_TABLE, COMMIT_LOG_FLOOR_KEY, &encode_txn_id(40))?;
        write.commit()?;
        assert_eq!(earliest_commit_lsn(&repl)?, 40);
        assert_eq!(repl.take_rows(), 0);

        let legacy = single_node_with_commits(5)?;
        let mut write = legacy.storage().begin_write()?;
        write.delete(COMMIT_LOG_TABLE, &commit_log_key(1))?;
        write.delete(COMMIT_LOG_TABLE, &commit_log_key(2))?;
        write.commit()?;
        assert_eq!(earliest_commit_lsn(&legacy)?, 3);
        Ok(())
    }

    #[test]
    fn latest_commit_at_or_before_matches_linear_scan_and_stops_early() -> TestResult {
        let node = SingleNode::new(MemEngine::new());
        let mut times = Vec::new();
        let mut write = node.storage().begin_write()?;
        for lsn in 1..=600_u64 {
            // LSN 300 steps the clock back to just after LSN 250.
            let seconds = if lsn == 300 { 2_505 } else { lsn * 10 };
            let committed_at = UNIX_EPOCH + Duration::from_secs(seconds);
            times.push((lsn, committed_at));
            let record = CommitLogRecord {
                txn_id: lsn,
                committed_at,
                hlc: None,
                writes: Vec::new(),
            };
            write.put(
                COMMIT_LOG_TABLE,
                &commit_log_key(lsn),
                &encode_commit_log_record(&record)?,
            )?;
        }
        write.put(TXN_META_TABLE, CURRENT_TXN_ID_KEY, &encode_txn_id(600))?;
        write.commit()?;
        let repl = CountingRepl::new(node);
        let linear = |timestamp: SystemTime| {
            times
                .iter()
                .rev()
                .find(|(_, committed_at)| *committed_at <= timestamp)
                .map(|(lsn, _)| *lsn)
        };

        for seconds in [5, 2_503, 2_505, 4_000, 5_995, 10_000] {
            let timestamp = UNIX_EPOCH + Duration::from_secs(seconds);
            assert_eq!(
                latest_commit_at_or_before(&repl, timestamp)?,
                linear(timestamp),
                "timestamp {seconds}"
            );
        }
        assert_eq!(linear(UNIX_EPOCH + Duration::from_secs(2_505)), Some(300));

        repl.take_rows();
        assert_eq!(
            latest_commit_at_or_before(&repl, UNIX_EPOCH + Duration::from_secs(5_995))?,
            Some(599)
        );
        assert!(repl.take_rows() <= 256);
        Ok(())
    }
}
