# Changelog

All notable public changes will be recorded here.

## Unreleased

- Hardened Control Plane `admin` login: failed password checks now lock out
  only the client IP (IPv6 /64) that made them, under a server-wide soft
  ceiling of 20x `MULTIDB_ADMIN_LOGIN_MAX_FAILURES` per window. Other usernames
  no longer lock out `admin`. At most 2 Argon2 password checks run at once, and
  extra login or change-password requests get `503 auth_busy` with
  `Retry-After: 1`. Usernames over 256 bytes and passwords over 1024 bytes are
  rejected with `400`. A `MULTIDB_ADMIN_PASSWORD` longer than 1024 bytes stops
  startup when it would be stored (no stored credential, or a reset). Rate-limited refusals
  and failures for unknown usernames are coalesced in the audit log.
- Prepared the repository for public preview publication.
- Added public repository hygiene files, security policy, contribution guide,
  issue templates and release notes.
- Kept local planning history out of the public source baseline.
- Fixed `multidb serve` / `multidb admin serve` starting with an empty RBAC
  policy after a restart, and admin bootstrap/login overwriting stored roles,
  PG-user mappings and the audit flag; plain database openers now reload
  persisted security state and fail closed if it is corrupt.
- Time-series retention now computes its cutoff from server time instead of
  the incoming point's timestamp, so one future-dated or microsecond-scaled
  point can no longer delete a whole series. Points more than
  `max_future_skew_millis` ahead of server time (default 300000 ms for new
  collections, settable when the collection is created; collections created
  before this change keep accepting future timestamps) and points older than
  `now - retention_millis` are rejected. New collections require a positive
  `retention_millis` that is at least `chunk_millis`; existing collections with
  a non-positive retention stay readable but refuse writes. Time-series
  validation errors from the Control Plane now return HTTP 400
  `invalid_value` instead of 500, and `time_bucket` and range queries no longer
  overflow near the `i64` bounds.
- The public CP cluster constructors (`create_cluster_database`,
  `open_cluster_database`, their `_with_ops` variants) and sharded `Cp` shard
  backends now fail with `Unsupported` instead of returning a database that
  acknowledges writes without replication. Use `repl::start_cp_cluster` for
  replicated CP storage; data written through the old constructors still opens
  with `open_database`. `CpRaft::new` is now test-only.
- AP databases now reject commit-log features they cannot provide
  (incremental backups, changefeeds, CDC subscriptions, continuous queries,
  materialized views, after-commit hooks, system versioning and incremental
  full-text/geo refresh) with `Unsupported` instead of returning empty results.
- SQL statements, including read-only `SELECT`s, no longer make a durable
  system commit per statement for the workload profiler. Samples are aggregated
  in memory per query fingerprint and persisted in one batched write after
  4,096 statements, 256 distinct fingerprints or 1 MiB of buffered fingerprint
  data, or on the first statement once the oldest buffered sample is 10
  seconds old;
  also before index or runtime advice, on `Database::flush_workload_samples`,
  and when a single-node database is dropped. `system.workload` and
  `Database::workload_report` include buffered samples without writing.
  Advisory statistics buffered since the last flush can be lost on crash or
  process stop. `query_as` audit behaviour is unchanged.
- Incremental and point-in-time restore now replays commit records through the
  same routine as live commits. This rebuilds `__txn_mvcc` history and the HLC
  from each record, so transactional reads, inserts after a delete, document
  index maintenance and `AS OF LSN`/`AS OF TIMESTAMP` queries see replayed
  commits. Snapshot transactions now read base rows from their storage
  snapshot. Restore and verify reject broken backup chains: an LSN gap or
  overlap with the parent, a timeline mismatch, a parent directory holding a
  different `backup_id`, a full backup with a parent, a cycle, a commit-log gap
  inside an incremental, or a full payload whose LSN differs from its manifest.
  `RestoreTarget::Time` earlier than the full backup now fails with
  `Unsupported`, as an LSN target already did. Databases restored from
  incremental chains by earlier builds should be restored again to rebuild
  their MVCC history.
- Full-text (`match`) and geo (`within_radius`) indexes are now backfilled when
  they are created and maintained in the same write batch as document inserts,
  updates and deletes made through collection handles, multi-model transactions
  and Control Plane document CRUD. Creating one now requires an existing
  document collection: a missing collection id or a vector collection is
  rejected. `refresh_full` is a bounded repair rebuild and `refresh_incremental`
  coalesces repeated changes to a document when repairing raw-key writes. Geo
  indexes accept integer `lon`/`lat` and skip invalid points (counted in
  `skipped_documents`) instead of failing, and searches no longer return
  zero-score postings or stale geo cells. Indexes created by earlier builds need
  one `refresh_full()`; Control Plane-only users can instead create a new index
  under a new name, which is backfilled on create. Because these searches
  return source document text, ids and points, `match()` and `within_radius()`
  through `query_as` now also require Read on the source collection, and are
  denied while that collection has row or masking policies. Index entries are
  computed from the document as written by the client, so a before-trigger
  that replaces a document leaves its index entries on the pre-trigger value.
- Made the encryption key lifecycle fail closed. `destroy_dek`/`crypto_shred`
  now refuse the current key, the last live key, and any key still used by
  stored values. The new `reencrypt_existing_values` (bounded, rerunnable
  batches) must run between `rotate_dek` and shredding old keys. KEK rotation
  now takes effect in-process and is atomic, and opening a non-empty database
  whose envelope keyring is missing fails instead of minting a new keyring.
- Columnar tiering no longer loses or resurrects rows written while a segment is
  being tiered: pointers are installed with a conditional write, and segments
  that changed are left alone and reported in `conflicted_segments`. Tiered
  objects use immutable, content-addressed paths, and `eventual_ap` databases
  are rejected. Full backups now inline CRC-checked tiered segment bytes, so
  restoring them does not depend on the object store, and fail if a tiered
  object is missing or corrupt. Hibernation uses full backups, so hibernating
  a tenant now downloads its tiered segments into the backup and resume
  restores them as local data until the next tiering run.
- All user writes now run one write pipeline (before triggers, before-commit
  hooks, tenant quota, commit, after-commit hooks and after triggers). SQL and
  pgwire `INSERT`, `RelTable` handles and stored procedures (`CALL`) now run
  WASM triggers, hooks and tenant quotas like `Database` writes do.
  `MultiModelTxn` commits (including Control Plane row and document CRUD) now
  also enforce tenant quotas and the tenant write limit.
- A committed write is never reported as failed. After-commit hook and
  after-trigger failures are logged, counted in
  `multidb_post_commit_failures_total{stage}`, recorded in the
  `__post_commit_failures` table and listed by
  `Database::post_commit_failures()`. Previously a client could see an error for
  a durable write, retry, and create a duplicate.
- `HookTarget::Collection`, `VectorCollection` and `Graph` hooks only match
  writes to the named object. Registering such a hook for a missing object, or
  for an object of another kind, fails with `HookError::InvalidTarget`
  (SQLSTATE `42704`).
- A before trigger `replace` result is validated against the table schema, must
  keep the primary key, and rebuilds the row's secondary-index entries. Replacing
  documents or a row written twice in one batch is rejected. SQL `RETURNING`
  still shows the row as written by the statement, before a trigger `replace`.
- WASM triggers can no longer be registered on columnar tables. A columnar
  write replaces a whole segment, so insert, update and delete events cannot be
  told apart. Triggers stored earlier still cannot replace columnar segments.
- Writes rejected by a before trigger or a before-commit hook fail with the new
  `ReplError::Rejected` variant, reported over pgwire as SQLSTATE `P0001`
  instead of the connection-failure code `08006`, so connection pools keep the
  connection.
- Removed `cdc::HookedReplication` and `cloud::QuotaReplication`; the write
  pipeline replaces them.
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
