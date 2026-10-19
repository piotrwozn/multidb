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
- Columnar tiering no longer loses or resurrects rows written while a segment is
  being tiered: pointers are installed with a conditional write, and segments
  that changed are left alone and reported in `conflicted_segments`. Tiered
  objects use immutable, content-addressed paths, and `eventual_ap` databases
  are rejected. Full backups now inline CRC-checked tiered segment bytes, so
  restoring them does not depend on the object store, and fail if a tiered
  object is missing or corrupt. Hibernation uses full backups, so hibernating
  a tenant now downloads its tiered segments into the backup and resume
  restores them as local data until the next tiering run.

## 0.1.0-preview.1

- Initial public-preview target.
- Includes the public-preview support surface documented in
  `docs/support-matrix.md`.
