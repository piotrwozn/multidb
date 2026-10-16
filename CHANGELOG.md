# Changelog

All notable public changes will be recorded here.

## Unreleased

- Prepared the repository for public preview publication.
- Added public repository hygiene files, security policy, contribution guide,
  issue templates and release notes.
- Kept local planning history out of the public source baseline.
- Changefeed paging and `AS OF TIMESTAMP` resolution now read bounded
  commit-log windows: catch-up is linear and memory is bounded by the page size
  instead of the whole log. The earliest-LSN lookup reads the recorded floor or
  LSN 1 directly; only a legacy log that has neither falls back to a full scan.
- Added manual history GC: `Database::gc_history` and
  `multidb maintenance gc --db <path> --profile <profile> [--dry-run]
  [--batch-size <n>] [--max-batches <n>] [--history-retain-lsns <n>]
  [--commit-log-retain-lsns <n>]`, with `multidb_history_gc_*` metrics. It
  prunes MVCC versions below `min(current LSN, AS OF pins, temporal min_lsn,
  current LSN - history-retain-lsns + 1)` in batches bounded by writes, record
  bytes and commit-log window reads, and truncates the commit log only with an
  explicit retention, never below durable consumer positions. It is local
  single-node only, needs exclusive file access, and does not shrink the redb
  file. `multidb serve` does not run history GC online or in the background
  yet. Without `--history-retain-lsns` (`mvcc_retain_lsns`), GC removes `AS OF`
  history below the GC watermark (the current LSN unless a temporal
  `keep_history` retention or an `AS OF` pin is older) for every table without a
  temporal `keep_history` retention, and the report carries a `note` saying so. On encrypted databases each pruned
  MVCC version or commit-log record keeps its small key-version row in
  `__enc_versions`, so GC reclaims value bytes but not those rows.
- Commit-log truncation (`--commit-log-retain-lsns`) breaks the incremental
  backup chain: take a new full backup afterwards. Run it while no writers are
  active (the CLI is offline anyway); after-commit hook delivery that meets
  truncated or missing commit records now fails closed instead of skipping
  them. Materialized views, full-text indexes and geo indexes created after
  truncation start from the current rows and LSN instead of replaying LSN 0.
- `AS OF` reads below the GC history horizon fail with `RetentionExpired`, as
  do system-versioned history scans and `enable_system_versioning` whose
  `min_lsn` is below the horizon; changefeed tokens below a truncated commit
  log fail with `TokenExpired`; incremental backups whose parent predates the
  truncated log fail with `BackupError::CommitLogTruncated`. Older binaries
  ignore the horizon, so do not downgrade a GC'd database file.
- Repeated `ack_subscription` calls with the token that is already stored no
  longer write a commit. A subscription worker's own ack still commits, so an
  idle worker keeps adding one commit per poll interval.

## 0.1.0-preview.1

- Initial public-preview target.
- Includes the public-preview support surface documented in
  `docs/support-matrix.md`.
