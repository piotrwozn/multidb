//! History GC and `AS OF` retention pins for [`Database`].

use std::{
    sync::{Mutex, PoisonError},
    time::Instant,
};

use serde::de::DeserializeOwned;

use super::{CatalogEntry, ConfigError, Database, DatabaseRepl, DbError};
use crate::{
    cdc::{self, SubscriptionState},
    continuous::{CONTINUOUS_QUERY_STATE_TABLE, ContinuousQueryState},
    geo::GeoIndex,
    observability,
    repl::{ReadConsistency, SingleNode},
    storage::{AnyEngine, StorageEngine, StorageError},
    temporal::TemporalError,
    text::FullTextIndex,
    txn::{
        self, TxnId,
        history::{
            self, HistoryGcBatchPlan, HistoryGcOptions, HistoryGcReport, HistoryWatermarkSource,
        },
    },
};

#[cfg(test)]
mod tests;

/// Holds one `AS OF` LSN in the in-process retention registry until dropped,
/// so a concurrent history GC computes its watermark at or below it.
#[must_use = "dropping the pin releases the AS OF retention hold"]
pub(super) struct HistoryPin<'db> {
    pins: &'db Mutex<txn::ActiveSnapshotRegistry>,
    lsn: TxnId,
}

/// MVCC pruning bound and the input that set it.
struct MvccWatermark {
    current: TxnId,
    lsn: TxnId,
    limited_by: HistoryWatermarkSource,
}

/// Commit-log truncation bound and the input that set it.
struct FloorTarget {
    lsn: TxnId,
    limited_by: HistoryWatermarkSource,
}

#[derive(Default)]
struct HistoryGcProgress {
    batches: u64,
    mvcc_versions_pruned: u64,
    commit_log_records_pruned: u64,
}

/// Reads only the freshness of a materialized view state, never its rows.
#[derive(serde::Deserialize)]
struct ViewFreshness {
    fresh_to_lsn: TxnId,
}

impl Drop for HistoryPin<'_> {
    fn drop(&mut self) {
        self.pins
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .unpin_retention_lsn(self.lsn);
    }
}

impl FloorTarget {
    fn limit(&mut self, lsn: TxnId, limited_by: HistoryWatermarkSource) {
        if lsn < self.lsn {
            self.lsn = lsn;
            self.limited_by = limited_by;
        }
    }
}

impl Database {
    /// Prunes MVCC history and, when requested, truncates the commit log in
    /// bounded, resumable batches.
    ///
    /// MVCC versions are pruned below the watermark `min(current LSN, in-process
    /// AS OF pins, min_lsn of every temporal table that keeps history, current
    /// LSN - mvcc_retain_lsns + 1)`. Each batch holds the commit lock for at most
    /// `batch_size` commit-log writes (about 8 MB of decoded records; one larger
    /// commit is always finished) and a bounded number of commit-log window
    /// reads, so gaps in the log end a batch early. It stores its progress in
    /// `__txn_meta` and raises the history horizon. `AS OF` reads below the
    /// horizon then fail with `RetentionExpired`. Without `mvcc_retain_lsns` and
    /// with no temporal tables, GC removes `AS OF` history for every table below
    /// the current LSN, and the report carries a `note` saying so; a temporal
    /// table that keeps history with `min_lsn` 0 makes MVCC pruning a no-op. Open transactions are
    /// unaffected: they read storage snapshots, and conflict checks use
    /// `__txn_versions`, which GC never touches. On encrypted engines each pruned
    /// version keeps its small key-version row in `__enc_versions`.
    ///
    /// The commit log is truncated only when `commit_log_retain_lsns` is set;
    /// use it while no writers are active (`multidb maintenance gc` is offline
    /// anyway), because a truncation racing a live writer makes its after-commit
    /// hook delivery fail closed. The floor never passes the watermark or the
    /// slowest subscription, continuous query, materialized view, full-text
    /// index or geo index position. A missing index position counts as LSN 0,
    /// and an index position only advances when the index is refreshed. After
    /// truncation, changefeed tokens below the floor (including `cdc poll --lsn
    /// 0` and new subscriptions at the default token) fail with `TokenExpired`,
    /// `AS OF TIMESTAMP` before the floor fails with `MissingPoint`, and
    /// incremental backups from older parents fail with `CommitLogTruncated`, so
    /// take a new full backup. New materialized views, full-text indexes and geo
    /// indexes start from the current rows and LSN instead of replaying LSN 0.
    ///
    /// Only local single-node databases are supported, and the redb file does
    /// not shrink. `multidb maintenance gc` runs this offline and needs
    /// exclusive access to the database file, so stop `multidb serve` first;
    /// `serve` does not run GC in the background.
    /// # Errors
    /// Fails with `ConfigError::Unsupported` for replicated or sharded databases
    /// or when `mvcc_retain_lsns` or `commit_log_retain_lsns` is zero. Fails
    /// closed, before any batch writes, when the retention registry is poisoned
    /// or a durable consumer position cannot be decoded; a batch that meets
    /// corrupt metadata, MVCC versions or commit records writes nothing.
    pub fn gc_history(&self, options: &HistoryGcOptions) -> Result<HistoryGcReport, DbError> {
        let started = Instant::now();
        let mut progress = HistoryGcProgress::default();
        let result = self.run_history_gc(options, &mut progress);
        let outcome = match &result {
            Ok(report) if report.dry_run => "dry_run",
            Ok(_) => "ok",
            Err(_) => "error",
        };
        observability::record_history_gc(
            outcome,
            started.elapsed(),
            progress.batches,
            progress.mvcc_versions_pruned,
            progress.commit_log_records_pruned,
        );
        result
    }

    /// Pins `lsn` for an `AS OF` read, then rejects it when history GC already
    /// pruned below it. The pin is released when the returned guard drops.
    /// # Errors
    /// Fails with `RetentionExpired` below the history horizon, or when the
    /// horizon cannot be read.
    pub(super) fn pin_history_lsn(&self, lsn: TxnId) -> Result<HistoryPin<'_>, DbError> {
        self.history_pins
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pin_retention_lsn(lsn);
        let pin = HistoryPin {
            pins: &self.history_pins,
            lsn,
        };
        let horizon = history::history_horizon(self.replication_ref())?;
        if lsn < horizon {
            return Err(TemporalError::RetentionExpired {
                requested: lsn,
                earliest: horizon,
            }
            .into());
        }
        Ok(pin)
    }

    /// True when history GC truncated the commit log, so a changefeed replay from
    /// LSN 0 fails with `TokenExpired`.
    /// # Errors
    /// Fails when the commit-log floor cannot be read.
    pub(super) fn commit_log_truncated(&self) -> Result<bool, DbError> {
        Ok(history::earliest_commit_lsn(self.replication_ref())? > 1)
    }

    /// Rejects a temporal retention whose `min_lsn` is below the history horizon,
    /// where history GC may already have pruned versions it would expose.
    /// # Errors
    /// Fails with `RetentionExpired` below the horizon, or when it cannot be read.
    pub(super) fn ensure_retention_above_history_horizon(
        &self,
        min_lsn: TxnId,
    ) -> Result<(), DbError> {
        let horizon = history::history_horizon(self.replication_ref())?;
        if min_lsn < horizon {
            return Err(TemporalError::RetentionExpired {
                requested: min_lsn,
                earliest: horizon,
            }
            .into());
        }
        Ok(())
    }

    fn run_history_gc(
        &self,
        options: &HistoryGcOptions,
        progress: &mut HistoryGcProgress,
    ) -> Result<HistoryGcReport, DbError> {
        let DatabaseRepl::SingleNode(node) = &self.repl else {
            return Err(ConfigError::Unsupported(
                "history GC is only supported on local single-node databases".to_owned(),
            )
            .into());
        };
        if options.mvcc_retain_lsns == Some(0) {
            return Err(ConfigError::Unsupported(
                "mvcc_retain_lsns must be at least 1; leave it unset to prune up to the current LSN"
                    .to_owned(),
            )
            .into());
        }
        if options.commit_log_retain_lsns == Some(0) {
            return Err(ConfigError::Unsupported(
                "commit_log_retain_lsns must be at least 1; leave it unset to keep the whole commit log"
                    .to_owned(),
            )
            .into());
        }

        let watermark = self.mvcc_watermark(node, options.mvcc_retain_lsns)?;
        let floor_target = options
            .commit_log_retain_lsns
            .map(|retain| self.commit_log_floor_target(&watermark, retain))
            .transpose()?;
        if !options.dry_run {
            let plan = HistoryGcBatchPlan::new(
                watermark.lsn,
                options.batch_size,
                floor_target.as_ref().map(|target| target.lsn),
            );
            run_history_gc_batches(node, &plan, options.max_batches, progress)?;
        }

        let state = history::read_history_gc_state(&node.storage().begin_read()?)?;
        let target_lsn = floor_target.as_ref().map(|target| target.lsn);
        let commit_log_limited_by = floor_target.map(|target| {
            if !options.dry_run && state.mvcc_cursor.saturating_add(1) < target.lsn {
                HistoryWatermarkSource::MvccCursor
            } else {
                target.limited_by
            }
        });
        let pending_commits = watermark.lsn.saturating_sub(state.mvcc_cursor);
        let pending_commit_log_records =
            target_lsn.map_or(0, |target| target.saturating_sub(state.commit_log_floor));
        let watermark_note = current_watermark_note(&watermark, options.mvcc_retain_lsns.is_some());
        Ok(HistoryGcReport {
            dry_run: options.dry_run,
            current_lsn: watermark.current,
            watermark: watermark.lsn,
            limited_by: watermark.limited_by,
            history_horizon: state.history_horizon,
            mvcc_cursor: state.mvcc_cursor,
            commit_log_floor: state.commit_log_floor,
            commit_log_floor_target: target_lsn,
            commit_log_limited_by,
            batches: progress.batches,
            mvcc_versions_pruned: progress.mvcc_versions_pruned,
            commit_log_records_pruned: progress.commit_log_records_pruned,
            pending_commits,
            pending_commit_log_records,
            complete: pending_commits == 0 && pending_commit_log_records == 0,
            note: watermark_note,
        })
    }

    fn mvcc_watermark(
        &self,
        node: &SingleNode<AnyEngine>,
        retain_lsns: Option<u64>,
    ) -> Result<MvccWatermark, DbError> {
        let (current, pinned) = {
            let pins = self.history_pins.lock().map_err(|_| {
                DbError::Storage(StorageError::Backend(
                    "history retention registry poisoned".to_owned(),
                ))
            })?;
            let current = node.current_txn_id()?;
            (current, pins.watermark(current))
        };

        let mut watermark = MvccWatermark {
            current,
            lsn: current,
            limited_by: HistoryWatermarkSource::CurrentLsn,
        };
        if pinned < watermark.lsn {
            watermark.lsn = pinned;
            watermark.limited_by = HistoryWatermarkSource::AsOfPin;
        }
        for entry in self.catalog.values() {
            if let CatalogEntry::TemporalTable {
                base_table,
                retention,
                ..
            } = entry
                && retention.keep_history
                && retention.min_lsn < watermark.lsn
            {
                watermark.lsn = retention.min_lsn;
                watermark.limited_by = HistoryWatermarkSource::TemporalRetention {
                    table: base_table.clone(),
                };
            }
        }
        if let Some(retain) = retain_lsns {
            let kept = current.saturating_add(1).saturating_sub(retain);
            if kept < watermark.lsn {
                watermark.lsn = kept;
                watermark.limited_by = HistoryWatermarkSource::MvccRetainLsns;
            }
        }
        Ok(watermark)
    }

    fn commit_log_floor_target(
        &self,
        watermark: &MvccWatermark,
        retain: u64,
    ) -> Result<FloorTarget, DbError> {
        let mut target = FloorTarget {
            lsn: watermark
                .current
                .saturating_add(1)
                .saturating_sub(retain)
                .max(1),
            limited_by: HistoryWatermarkSource::RetainLsns,
        };
        target.limit(watermark.lsn, watermark.limited_by.clone());

        let repl = self.replication_ref();
        for (_, bytes) in repl.range(
            cdc::CDC_SUBSCRIPTIONS_TABLE,
            &[],
            &[],
            ReadConsistency::Strong,
        )? {
            let state = decode_consumer_state::<SubscriptionState>(&bytes, "subscription")?;
            target.limit(
                state.last_ack.lsn.saturating_add(1),
                HistoryWatermarkSource::Subscription {
                    name: state.config.name,
                },
            );
        }
        for (_, bytes) in repl.range(
            CONTINUOUS_QUERY_STATE_TABLE,
            &[],
            &[],
            ReadConsistency::Strong,
        )? {
            let state = decode_consumer_state::<ContinuousQueryState>(&bytes, "continuous query")?;
            target.limit(
                state.last_ack.lsn.saturating_add(1),
                HistoryWatermarkSource::ContinuousQuery { name: state.name },
            );
        }
        for (key, _) in repl.range(
            cdc::MATERIALIZED_VIEWS_TABLE,
            &[],
            &[],
            ReadConsistency::Strong,
        )? {
            let fresh_to = repl
                .read(
                    cdc::MATERIALIZED_VIEW_STATE_TABLE,
                    &key,
                    ReadConsistency::Strong,
                )?
                .map(|bytes| decode_consumer_state::<ViewFreshness>(&bytes, "materialized view"))
                .transpose()?
                .map_or(0, |state| state.fresh_to_lsn);
            let name = String::from_utf8(key)
                .map_err(|error| DbError::Serde(format!("materialized view name: {error}")))?;
            target.limit(
                fresh_to.saturating_add(1),
                HistoryWatermarkSource::MaterializedView { name },
            );
        }
        self.limit_floor_by_derived_indexes(&mut target)?;
        Ok(target)
    }

    fn limit_floor_by_derived_indexes(&self, target: &mut FloorTarget) -> Result<(), DbError> {
        let repl = self.replication_ref();
        for (name, entry) in &self.catalog {
            if let CatalogEntry::FullTextIndex { config } = entry {
                let refreshed_to = FullTextIndex::new(repl, config.clone())?
                    .state()?
                    .map_or(0, |state| state.refreshed_to.lsn);
                target.limit(
                    refreshed_to.saturating_add(1),
                    HistoryWatermarkSource::FullTextIndex { name: name.clone() },
                );
            } else if let CatalogEntry::GeoIndex { config } = entry {
                let refreshed_to = GeoIndex::new(repl, config.clone())?
                    .state()?
                    .map_or(0, |state| state.refreshed_to.lsn);
                target.limit(
                    refreshed_to.saturating_add(1),
                    HistoryWatermarkSource::GeoIndex { name: name.clone() },
                );
            }
        }
        Ok(())
    }
}

fn run_history_gc_batches(
    node: &SingleNode<AnyEngine>,
    plan: &HistoryGcBatchPlan,
    max_batches: Option<usize>,
    progress: &mut HistoryGcProgress,
) -> Result<(), DbError> {
    let mut executed = 0_usize;
    while max_batches.is_none_or(|max| executed < max) {
        let outcome = node.maintenance_write(|read, write| {
            let outcome = history::gc_history_batch(read, write, plan)?;
            Ok((outcome, outcome.changed))
        })?;
        executed = executed.saturating_add(1);
        if outcome.changed {
            progress.batches = progress.batches.saturating_add(1);
        }
        progress.mvcc_versions_pruned = progress
            .mvcc_versions_pruned
            .saturating_add(outcome.mvcc_versions_pruned);
        progress.commit_log_records_pruned = progress
            .commit_log_records_pruned
            .saturating_add(outcome.commit_log_records_pruned);
        if outcome.done || !outcome.changed {
            break;
        }
    }
    Ok(())
}

/// Explains what a watermark at the current LSN removes, unless the caller
/// already chose a history retention.
fn current_watermark_note(watermark: &MvccWatermark, retain_requested: bool) -> Option<String> {
    (!retain_requested && watermark.lsn == watermark.current && watermark.current > 0).then(|| {
        format!(
            "watermark is the current LSN {}: AS OF history below it is removed for every table without temporal keep_history retention; set mvcc_retain_lsns (CLI --history-retain-lsns) to keep recent AS OF history",
            watermark.current
        )
    })
}

fn decode_consumer_state<T: DeserializeOwned>(bytes: &[u8], kind: &str) -> Result<T, DbError> {
    serde_json::from_slice(bytes).map_err(|error| DbError::Serde(format!("{kind} state: {error}")))
}
