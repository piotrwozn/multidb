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
  statements ran one after another. `INSERT ... ON CONFLICT` and inserts into
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

## 0.1.0-preview.1

- Initial public-preview target.
- Includes the public-preview support surface documented in
  `docs/support-matrix.md`.
