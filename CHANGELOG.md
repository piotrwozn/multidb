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
- `multidb serve` and `multidb admin serve` share the database between
  PostgreSQL wire and the Control Plane through a reader-writer lock instead
  of a mutex. Statements and Control Plane data, catalog, audit and advice
  requests now run concurrently. `/builder/*` and `/security` updates take the
  exclusive lock: they wait for in-flight statements, and new requests queue
  behind them. Admin login and change-password take it only when the bootstrap
  `admin` role or principal is missing or differs from the bootstrap
  definition; when both are in place they no longer rewrite the stored
  security state. Control Plane storage work runs on the blocking thread pool
  instead of the async workers.
  The per-database query governor now applies across clients: by default at
  most 4 x available CPUs statements run at once, and others wait up to
  100 ms, then fail with SQLSTATE 53200. `multidb serve` reads
  `MULTIDB_MAX_CONCURRENT_QUERIES` (1 to 4096) and
  `MULTIDB_QUERY_QUEUE_TIMEOUT_MS` (1 to 60000) to change these limits and
  refuses to start on any other value. It admits at most that many
  PostgreSQL wire statements (never more than 32) at a time and queues the
  rest, so wire clients see 53200 only while Control Plane `/sql` requests
  hold slots. A plain `INSERT` that races another
  insert of the same primary key fails with SQLSTATE 23505, as it would if the
  statements ran one after another (or with P0001 if a before trigger rejects
  the insert itself). `INSERT ... ON CONFLICT` and inserts into
  columnar tables read again and retry when another statement commits first,
  and fail with retryable SQLSTATE 40001 only if 64 retries also conflict.
  Control Plane single-row, single-document and time-series point writes also
  retry conflicts internally. A statement keeps running after its client
  disconnects.
  Control Plane error statuses: a saturated query governor is now HTTP 503
  with error code `busy` and `Retry-After: 1`, and a write conflict that
  outlasts the internal retries (SQLSTATE 40001) is HTTP 409 with code
  `conflict`; both were HTTP 500 `internal_error`. A `busy` response can be retried as is; a `conflict` on a multi-statement `/sql` body may follow statements that already committed, so retry only single-statement bodies or idempotent work.
  Time-series validation errors are HTTP 400 `invalid_value` (see above);
  other database errors stay HTTP 500.
  Control Plane authentication no longer depends on lock
  contention: a busy database delays the check instead of returning
  `admin_auth_not_configured` or dropping the roles of `x-multidb-principal`.
  Vector inserts that race a vector-index rebuild are no longer dropped from
  in-memory kNN results; the rebuild (first load or capacity growth) now
  blocks kNN searches and inserts on that collection while it reads the
  stored vectors.
- Columnar table writes (SQL `INSERT` and `RelTable::insert`, `update` and
  `delete`) commit only if no other writer changed the table's segments after
  they were read, and read again and retry otherwise. Concurrent writers could
  previously overwrite each other and silently lose rows. Zone-map pruning for
  filtered columnar reads on single-node and CP databases now reads segments
  and zone maps from the same commit; a write between the two reads could
  previously hide matching rows.
  Any commit in between, including the audit record written after each
  statement, voids the read, so after two such reads the scan decodes every
  segment it read instead of pruning. Because a columnar
  write rewrites the whole table, concurrent writes to one columnar table
  effectively run one at a time. On row tables, `INSERT ... ON CONFLICT` no
  longer fails with a duplicate primary key error when another statement
  inserts or changes the same row at the same time.
- Breaking for embedders: `multidb::network::SharedDatabase` (used by
  `PgServer::serve_shared` and `serve_shared_until_shutdown`) is now
  `Arc<tokio::sync::RwLock<Database>>`, `AdminState::from_database_handle`
  takes `Arc<tokio::sync::RwLock<Database>>`,
  `AdminState::is_admin_auth_configured` is now `async`, and
  `QueryError` has a new `Busy` variant for query governor saturation
  (previously `ResourceLimit`; both map to SQLSTATE 53200), so exhaustive
  matches need an extra arm. `Database::set_query_governor` sets the
  governor's concurrency and queue timeout.
- Transactional row writes (`MultiModelTxn::insert_row`, `update_row`,
  `delete_row`, including Control Plane row CRUD) to columnar tables with more
  than 1,024 rows no longer drop the rows stored beyond the first segment, and
  `MultiModelTxn::get_row` now finds them. Transactions read and rewrite every
  columnar segment of the table: from their snapshot under snapshot and
  serializable isolation, and from current committed data under read
  committed.
- WASM triggers no longer see the wrong event when writes race. Since
  statements and Control Plane requests run concurrently, an `INSERT` whose key
  another statement inserted after the `INSERT` read it was seen as an update
  (and a write whose row was deleted in between as an insert). A before
  trigger for that event could then reject it with SQLSTATE P0001 before the
  commit reported the conflict, so `INSERT ... ON CONFLICT` failed instead of
  retrying, and a plain `INSERT` failed with P0001 instead of 23505.
  Conditional writes (SQL `INSERT`, `RelTable` handles) now take each
  trigger's event and `old` value from the row values their commit checks, and
  `MultiModelTxn` commits (including Control Plane row and document writes)
  take them from the transaction snapshot. At every isolation level, a commit
  that writes a row another writer changed after the transaction began fails
  with its conflict before that row's triggers run, so a lost race ends in the
  commit's conflict and is retried as before. Stored procedure (`CALL`) writes
  and direct `Database::propose` and `propose_batch` writes check nothing at
  commit: they are last-writer-wins, and under concurrent writes the event and
  `old` value their triggers see are best effort.
- Breaking for embedders: with a tenant set (`OperationalConfig::with_tenant`)
  whose `max_concurrent_writes` or `max_concurrent_queries` is limited, Control
  Plane requests (including row and document writes) and PostgreSQL wire
  statements can now exceed those limits, because they run concurrently.
  Tenant write and query slots are taken without waiting, so the extra
  requests fail. Writes and single-table equality `SELECT`s fail with
  `ReplError::QuotaExceeded { requested: 1, remaining: 0, .. }`, which
  PostgreSQL wire reports as SQLSTATE 53100. Other `SELECT`s, which DataFusion
  executes, can instead fail with a `QueryError::DataFusion` whose message
  contains the quota error, which PostgreSQL wire reports as SQLSTATE XX000.
  The Control Plane returns HTTP 500 `internal_error` in both cases. Size the
  limits for the concurrent load or retry such failures. `multidb serve` sets
  no tenant.

## 0.1.0-preview.1

- Initial public-preview target.
- Includes the public-preview support surface documented in
  `docs/support-matrix.md`.
