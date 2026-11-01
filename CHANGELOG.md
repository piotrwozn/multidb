# Changelog

All notable public changes will be recorded here.

## Unreleased

- SQL primary-key equality and ordered Int64 ranges now use bounded row-store
  access, including ASC/DESC, OFFSET and LIMIT, with execution deadlines and
  retained-result memory limits. Row scans build projected Arrow batches rather
  than retaining every payload column. A shared, bounded raw-batch cache uses
  atomically committed table versions; authorization and policies remain fresh.
- Reuse immutable SQL catalog templates and retain the plan cache between queries.
  Fix column-name, primary-key, histogram and MCV cardinality estimates. EXPLAIN
  supports real range paths and metadata-only DataFusion logical plans; unknown
  estimates/costs are NULL and ANALYZE counts rows after LIMIT.
- Native indexed row fetches read up to 256 keys in one storage snapshot. Plain
  INSERT avoids redundant pre-reads while retaining atomic uniqueness checks;
  upserts and transactional updates avoid rewriting unchanged index entries.
- Concurrent audit requests share durable batches without an added single-caller
  wait window. Every event retains its integrity sequence and is acknowledged
  after database and configured anchor persistence; events can share a commit LSN.
- Pgwire reuses its Tokio runtime, applies separate handshake, idle and request
  deadlines instead of a total session lifetime, and checks frontend frame sizes
  before decoding. Successful logins release their authentication reservation
  without spending the failed-login budget. CLI timeout/frame overrides are
  documented in `docs/docker.md`.
- Fixed SQL statements that could change meaning between parsing and
  planning. The engine printed each parsed statement back to SQL text with
  sqlparser's printer and planned that text. The printer leaves a quote that
  follows a backslash undoubled and prints `N'...'` unescaped, so a string
  literal or quoted name holding a backslash before a quote ended early in the
  planned text and the rest of it ran as SQL: it could comment out the rest of
  a `WHERE` clause (a tenant filter, other bound parameters) or add `OR` /
  `UNION` branches. Anyone whose input reaches a
  SQL string could trigger it: an end user of an application that binds text
  parameters over the PostgreSQL wire extended protocol (they are spliced as
  quoted literals, and a parameter in an alias position, `AS $1`, as a quoted
  name), or of an application that quotes literals correctly for simple
  queries, Control Plane `POST /sql`, the SDKs or `Database::query`/`query_as`.
  It allowed no writes or extra statements. Reads stayed limited to what the
  principal may read, with row and masking policies applied, except through
  `Database::query`, which has no principal. String literals that hold a quote
  are now printed as `E'...'`, and the printed statement must parse back to the
  same statement with the PostgreSQL dialect and split into the same tokens
  with DataFusion's generic dialect; otherwise it is refused with `Unsupported`
  (SQLSTATE 0A000), as is a quoted name holding a backslash before its quote.
  This covers queries, `EXPLAIN`, Describe (`INSERT ... RETURNING` is now
  described from its parsed `RETURNING` items) and the permission check of
  `EXPLAIN`.
- Fixed PostgreSQL wire extended-protocol parameters that could change the
  statement they were bound into. A negative number bound right after a minus
  sign, as in `age > 0-$1`, was spliced as `0--5`, which starts a comment, so
  the rest of the line (for example `AND tenant_id = $2`) was dropped; next to a
  unary minus or a higher-precedence operator its sign could also bind
  differently. Any end user controlling a numeric parameter in such a statement
  could trigger it; reads stayed limited to what the principal may read, with
  row and masking policies applied. Negative numbers are now spliced in
  parentheses (`(-5)`), and a space separates a spliced value from a
  neighbouring token. Floats that hold a whole number stay floats (`1.0` was
  spliced as `1`, so `$1 / 2` divided as integers), `INSERT` accepts a bound
  `i64::MIN`, `CALL` and UDF arguments accept parenthesized numbers, and a
  projection such as `SELECT 1, (-5)` that ends with `)` is no longer taken for
  a WASM UDF call.
- Prepared statements are checked when the client parses them (`Parse`). A
  prepared statement holds one SQL statement, as in PostgreSQL (SQLSTATE 42601
  `cannot insert multiple commands into a prepared statement`); before, Execute
  ran every statement of a `;`-separated batch. A placeholder used as a whole
  `ORDER BY` or `GROUP BY` item (also an aggregate's `ORDER BY`) is refused with
  SQLSTATE 0A000: the engine reads a number there as a column position, so a
  bound `2` sorted or grouped by the second column instead of by a constant.
  The check covers statements with an inline `AS OF` suffix and placeholders
  used as aliases. A statement with placeholders that parses neither with them
  nor with quoted values in their place, such as one with a placeholder as a
  type modifier (`varchar($1)`), is refused with SQLSTATE 42601, as in
  PostgreSQL; `CALL` and `CREATE FUNCTION`, which multidb runs from their text,
  are not parsed there. Placeholders run from `$1` to `$65535` (SQLSTATE
  42P02), and a placeholder the client sent no type for is described as `TEXT`
  in `ParameterDescription`, so drivers that prepare without parameter types
  can bind it.
- Pgwire Describe answers `NoData` for a statement that returns no rows
  (`INSERT` without `RETURNING`, `CALL`, `CREATE FUNCTION`) instead of an empty
  row description, and describes `CALL` and `CREATE FUNCTION` without running
  them. It describes the built-in system views (`system.workload`,
  `system.tuning_log`, `system.index_advice`, `system.reprofile_jobs`) and the
  `match`, `time_bucket`, `within_radius` and `graph_neighbors` helpers, reports
  `UNION` and `CASE` columns with the coerced types Execute returns (they had
  their first input's type, so binary clients decoded floats as integers), and
  refuses a WASM UDF call (SQLSTATE 0A000) and a query over a table whose
  masking replacement SQL cannot return, which Execute refuses too.
- Workload fingerprints (`system.workload`) no longer keep part of the value
  of an `E'...'` string literal with an escaped quote, or the `N` / `E` prefix
  of a literal.
- Added a reproducible local before/after SQL benchmark runner with binary/source
  provenance, raw latency verification and all nine supplied workloads. See
  `docs/performance-local-benchmarks.md`.
- Updated the zstd implementation to the fixed 0.14.3 / zstd-safe 8.2 codec
  stack, including Arrow/Parquet callers through a small local compatibility
  re-export; the vulnerable registry codec versions are no longer resolved.
- Updated Wasmtime to 36.0.20 for RUSTSEC-2026-0424.
- Added the preview `tensor_dataset` / `tensor` profile: durable binary typed
  tensor chunks, aligned and ragged samples, atomic append revisions and projected
  reads. GPU search and batch reductions dispatch WGSL kernels through wgpu;
  the profile requires a hardware adapter and refuses CPU fallback. Added six
  authenticated API operations, four SDKs, spec-v2 provisioning and a runnable
  example. Other native profiles can use tensor datasets with explicit CPU
  reference execution. See `docs/tensor-datasets.md` for limits and device support.
- Added preview GraphRAG retrieval: bounded vector seeds, graph expansion,
  deduplicated cited context and source provenance through the Rust API,
  `POST /graphrag/query`, and TypeScript/Python/Go/Rust SDKs. Added a graph-edge
  write endpoint and a runnable local example. Principal retrieval requires both
  vector and graph Read, applies metadata policies before traversal, and refuses
  unsupported graph policies/backends. Vector search responses now also filter
  and mask metadata, closing an alternate read path around GraphRAG policies.


- Control Plane table/document pages now apply row and masking policies before
  pagination. Pgwire Describe authorizes a metadata-only plan, including untyped
  parameters, rather than falling back to unguarded schema lookup.
- Principal changefeeds suppress derived index data from protected collections,
  deletes whose old-row visibility cannot be established, and table events with
  masked primary keys. Principal subscriptions bind ownership, require policy-aware
  delivery and can resolve live user mappings; principal continuous-query creation
  atomically binds its subscription and preserves ACK progress on re-apply.
  Legacy subscription delivery now fails closed until a System:Admin operator
  adopts its owner with `adopt_subscription_as`, preserving the stored ACK.
- Materialized view reads and principal creation fail closed when source policies
  cannot be applied before aggregation. Workload/tuning views and
  `workload_report_as` now require System:Admin (breaking for read-only grants).
- AP capability checks include AP shards and low-level replay APIs; AP restores
  refuse unsupported point-in-time targets and incremental chains before creating
  a destination. Operational create-or-open constructors preserve stored RBAC.
- Serve performs bounded idle time-series retention, periodic profiler flushes,
  opt-in bounded online MVCC GC (`MULTIDB_HISTORY_RETAIN_LSNS`), and SIGINT/SIGTERM
  connection shutdown with a final flush. Online GC serializes history pins with
  horizon updates; commit-log truncation remains offline.
- CLI/serve can open and provision encrypted databases with an existing key file
  selected by `MULTIDB_ENCRYPTION_KEY_PATH`; metadata probes remain read-only.
- Time-series timestamp codecs report unrepresentable deltas instead of
  overflowing. Plain columnar inserts append to the tail instead of rewriting
  all committed segments; primary-key validation still reads existing segments.


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
  take them from the transaction snapshot, so triggers see each row as the
  write read it. A write whose row another writer changed after that read
  fails with the commit's conflict, which is handled as before, unless a
  before trigger rejects it for the event it has. At every isolation level, a
  transaction commit that writes a key another writer changed after the
  transaction began fails with that conflict before any trigger runs, or, if
  the change is made once the commit has started running triggers, after
  them, as before. Stored procedure (`CALL`) writes and direct
  `Database::propose` and `propose_batch` writes check nothing at commit: they
  are last-writer-wins, and under concurrent writes the event and `old` value
  their triggers see are best effort.
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
- Fixed system rows leaking through changefeeds. `Database::poll_changefeed_as`
  and `Database::poll_changefeed_page_as` checked only the filter target, so a
  principal with `Database:Read` could poll the `All` target with
  `ChangefeedOptions { include_system: true }` and read every system write
  still in the commit log (every record history GC has not yet truncated):
  the Control Plane admin credentials (`__admin_auth`, the Argon2id hash of
  every password set, which can be cracked offline), the RBAC policy and
  principal registry (`__authz_policy`), the audit log and its hash-chain
  head, the masking and row-policy definitions (`__policies`), provisioning,
  extension (including WASM modules), CDC, outbox and tuning metadata, and raw
  index keys (`rel_indexes` and document indexes). Index keys and `INCLUDE`
  columns carry indexed values in the clear, so they also bypassed masking and
  row policies. A changefeed poll that can return system rows
  (`include_system`, or a `ChangefeedTarget::System` target as before) now
  needs `System:Admin`, whatever the filter target, and a denied poll is
  audited as `poll_changefeed`. System rows are also dropped from the events
  returned to any principal without `System:Admin`. Only applications that
  embed the crate and let callers choose the options were affected:
  `multidb serve` has no changefeed endpoint on PostgreSQL wire or the Control
  Plane, and SQL cannot name system keyspaces (the `system.*` SQL views, which
  show query fingerprints and tuning data and need `System:Read`, are
  unchanged). Subscriptions, continuous queries and outbox connectors never
  include system rows and are unchanged. The trusted
  `Database::poll_changefeed`, `poll_changefeed_page` and
  `multidb cdc poll --include-system` still return system rows to the local
  operator, credential hashes included, so treat their output like the
  database file.
- Fixed a SQL read authorization bypass. Under RBAC, SQL reads (`query_as`,
  `query_as_of_as`, PostgreSQL wire with RBAC, the Control Plane `/sql` and
  the SDKs that call it) were authorized by a walk over part of the parsed
  statement. The walk missed tables read from many positions: subqueries in
  `ORDER BY`, `LIMIT`/`OFFSET`, `VALUES`, window and `FILTER` clauses, and
  inside expressions such as `LIKE`, `TRIM`, `SUBSTRING` or `POSITION`. A
  quoted CTE name such as `"Secrets"` also hid the table `secrets` from the
  check while the query still read the table, and grants were matched to the
  name as spelled, so a grant on `ORDERS` authorized reads of `orders`. A
  principal with `Read` on any one table could therefore read every other
  table, collection, foreign table, materialized view and `*_history` table.
  Reads are now authorized against every table the planned DataFusion query
  reads, including subqueries in any position, before anything runs, and the
  plan that was authorized is the plan that runs. A multi-statement batch runs
  nothing until all of its reads are authorized. Table reads are matched to
  the catalog object the planner resolves: unquoted names are lowercased, so
  `ORDERS` and `Orders` read `orders` and need `Read` on it, and a quoted
  `"ORDERS"` names no table. SQL shapes the check cannot analyse fail closed
  with `Unsupported` and `SELECT ... INTO` returns `Unsupported`. The check
  counts a recursive CTE's reference to itself as a read of a table named
  like the CTE, so recursive CTEs need Database read (or `Read` on that name),
  and a denial names the CTE. `INSERT ... RETURNING` returns stored rows (for
  `ON CONFLICT DO UPDATE`, the existing row), so it now needs `Read` on the
  table as well as `Write`; before, `Write` alone could read any row by key.
  Extended-protocol Describe over PostgreSQL wire, which returned the column
  names and types of catalog tables (no rows) without an RBAC check, now
  plans the statement under the same checks (see the pgwire Describe entry).
- Fixed SQL bypasses of row and masking policies. SQL run as a principal applied
  them to the query result, matched to its columns by name, so a principal with
  `Read` on a policy-protected table could see the rows its row policies hide
  and the values its masking policies mask: through an alias
  (`SELECT ssn AS x`), an expression (`upper(ssn)`), a literal column named like
  the row policy's column, an aggregate, a subquery over the same table, or a
  filter or sort key on a masked column. Collection policies were matched to SQL
  column names, so a policy on the document path `profile.city` did not mask the
  SQL column `city` that reads it, and a row policy that shows documents whose
  field is null also showed documents without the field or with a value of
  another type, which SQL reads as NULL. A table's `*_history` table was read
  without the table's policies. Row and masking policies are now applied
  where SQL scans the table: the scan returns only the rows the row policies let
  through, with masked values replaced, before any filter, sort, join, aggregate
  or subquery of the query reads them, so a predicate that fails on some value
  (such as a division by zero) cannot reveal a hidden row either. Collection
  row policies are decided on the stored document, as for changefeeds: a
  document is visible when its value at the policy's path equals the policy's
  value or the principal's name, so a document without the path, or with a
  value of another type, stays hidden. A collection mask covers the SQL
  columns that read its path or a path below it. A query that joins a
  policy-protected table with other tables now runs with the policies applied
  instead of being rejected. A masked column takes the type of its
  replacement (a `'***'` mask makes an integer column a string column), and
  SQL reads of a table whose mask replacement is not a scalar (an array,
  object, vector or point) fail. On tables and other relations, whose SQL
  columns hold only scalars, a row policy value that is not a scalar matches
  no row. `EXPLAIN` of a table with row or masking policies and
  `INSERT ... RETURNING` into one return `Unsupported`, since both report
  stored rows. Each SQL scan of such a table reads all of the rows its row
  policies let through before the query filters them, so single-table
  equality `SELECT`s on it run through DataFusion instead of the direct lookup
  path. A table's policies also apply to its `*_history` table, matched to
  its columns by name, together with the history table's own: a row must pass
  both, and the history table's own masks win. A materialized view over a
  protected table needs policies of its own.
  These policies apply to SQL run as a principal, not to `Database::query`.
  This fix does not cover the Control Plane data routes
  `GET /data/tables/{name}/rows` and `GET /data/collections/{name}/documents`,
  or the SDK and Studio calls that use them: they still return stored rows
  and documents, unfiltered and unmasked, to any principal with `Read`.

### Added

- Typed profile model in `multidb::config_spec`: `ProfileKnobs`, the
  `KNOB_REGISTRY` const table (bounds, timing, runtime sink and enforcement of
  every knob), `RESERVED_KNOBS`, built-in variants pinned 1:1 to storage
  profiles (`profile_variants`), and the pure resolver `resolve_profile` /
  `resolve_named_profile`, which records the provenance of every knob and a
  `prof_` digest. Presets equal today's runtime defaults: compression none,
  limits unlimited (planned: the write pipeline does not enforce them yet) and
  query concurrency auto. See `docs/profiles.md`.
- `PerformanceConfig::with_profile_knobs`, `ResolvedProfile::performance_config`,
  `parse_knob_assignment` (`path=value` with did-you-mean suggestions) and
  `built_in_profile_normalized` (trims, lowercases and maps `-` to `_`;
  `built_in_profile` stays an exact match).
- `DatabaseSpec.custom_profile`, a spec version 2 field. A version 1 document
  that sets it fails validation with `FIELD_REQUIRES_SPEC_V2`, and
  `resolve_profile` returns that issue as `ProfileResolution::Invalid`.
  Version 1 documents without it serialize and plan exactly as before (same
  plan ids) and keep their existing issues, order, status and validity,
  gaining only the Advice and Warnings listed under Changed; the frozen
  corpus in `tests/fixtures/spec-v1/` pins this.
- `CollectionRole` derives `PartialOrd` and `Ord`.
- `DatabaseSpec` version 2 (`DATABASE_SPEC_VERSION_V2`,
  `SUPPORTED_SPEC_VERSIONS = 1..=2`): a version 1 spec plus `custom_profile`
  (`extends` a built-in product slug or alias, typed `knobs`, raise-only
  `floors` and `compatible_roles` add/remove). Validation, `PolicyCompiler`
  and `ConfigExplainer` resolve the profile: `CompiledPolicy.resolved_profile`
  carries the resolved knobs, explain reports one `$.profile.knobs.<path>`
  decision per knob (value, source, what applies it and when) plus
  `$.profile.storage_profile` and `$.profile.not_configurable.*`, and
  `config plan` reports `custom_profile` changes as unsupported
  `change_profile` steps. New issue codes include `PROFILE_UNKNOWN`,
  `PROFILE_NOT_PROVISIONABLE`, `GUARANTEE_BELOW_PROFILE_FLOOR`,
  `OVERRIDE_IS_METADATA_ONLY` and `OVERRIDE_SHADOWS_TYPED_KNOB`; see
  `docs/profiles.md`.
- `multidb::config_spec::spec_io`: bounded spec reading (`read_spec_file`,
  `decode_spec_bytes`, `decode_spec_value`) with a 1 MiB limit, a version peek
  before the typed decode and node and scalar-byte budgets for YAML alias
  expansion. The CLI and the Control Plane read every spec through it.
- `database_spec_v2_schema()` and the checked-in
  `docs/schemas/database-spec-v2.schema.json`; knob bounds and enums are
  generated from `KNOB_REGISTRY`. The version 1 schema is unchanged.
- `examples/custom-profile/multidb.yaml`, a version 2 validation and explain
  example covered by `scripts/templates-smoke.ps1`.
- Persisted profiles in `multidb::db`: `ResolvedProfile::bind` and
  `CompiledPolicy::binding` produce a `RuntimeProfileBinding`, and
  `create_database_with_profile` creates a new single-node database that
  stores the resolved knobs in `__meta.profile_manifest`
  (`PersistedProfileManifest`, `StorageFormat`). Every single-node opener,
  and therefore `multidb serve`, re-validates the manifest and re-applies its
  knobs at every open. Compression turns on value framing and
  `schema_version` 2, which older binaries refuse. Without compression the
  database keeps `schema_version` 1: an older binary opens it but ignores the
  manifest, so a downgrade reverts every knob to the storage profile
  defaults. A manifest is refused (`ProfileManifestInvalid`) unless its knobs
  pass the resolver's range and conflict checks, leave the planned
  `limits.*` knobs `unlimited` and match the recorded digest.
  `create_database_with_profile` refuses (`ProfileFloorNotMet`) operational
  settings that miss `floors.encryption_at_rest` (no
  `OperationalConfig.encryption`) or `floors.audit`, and
  `Database::set_audit_enabled_as` refuses to disable audit on a database
  whose manifest sets `floors.audit`. Restore refuses
  (`BackupError::ProfileFloorsNotMet`, naming the floors) a backup whose
  restored database would not meet `floors.encryption_at_rest` (a restore
  target is never encrypted) or `floors.audit` (audit off in the restored
  state), before it creates anything, unless
  `RestoreConfig.allow_unmet_profile_floors` (builder
  `with_unmet_profile_floors_allowed`) or `multidb restore
  --allow-unmet-floors` opts in; hibernation resume has no opt-in, and the
  error message names the opt-ins and says so. `verify_backup` sets
  the opt-in for its own drill, because its temporary, unencrypted target is
  removed after the drill (a failed removal is only logged). `floors.encryption_at_rest` covers the database
  file and is never checked on backup: backup files are encrypted by backup
  encryption (`BackupConfig.encryption`, by default the encryption config of
  the database backed up), so a backup of an encrypted database is
  encrypted and a backup of a database below the floor is not, unless
  `BackupConfig.encryption` is set (see `docs/profiles.md`). Every
  single-node opener logs a `tracing::warn!` naming each manifest floor the
  opened database does not meet; it reaches embedders that install a tracing
  subscriber, while the `multidb` binary, `serve` included, installs none, so
  CLI users see unmet floors through `profile show --db` and `restore
  --allow-unmet-floors`.
  `CompiledPolicy::binding` refuses a path for a volatile
  storage class (`VolatileWithPath`), also for a version 1 spec, where
  validation only warns. CP, AP and sharded openers refuse manifests.
  `inspect_database_metadata` (`MetadataProbe`, `ProbedMetadata`) reads a
  database's metadata through a read-only redb open, so it never creates,
  writes or repairs files; a file that was not closed cleanly is
  `NeedsRecovery`, and `recover_database_file` runs redb's crash recovery on
  it without touching the metadata. `ProbedMetadata.unmet_floors` lists
  `floors.encryption_at_rest` when the manifest sets it, because the probe
  read the metadata without a key, and `floors.audit` when the manifest sets
  it and the stored audit flag is off; `profile show --db` and `restore
  --allow-unmet-floors` print them as warnings. The persistent format registry
  (`FormatRegistry::phase_24_default`) lists database metadata
  `schema_version` 2 and the profile manifest. See `docs/profiles.md`.
- `AnyEngine::is_value_framed` and `AnyEngine::with_value_framing`, which
  wraps an open engine in compression framing without reopening it.
- CLI profile commands: `multidb profile show <profile> [--override
  path=value]...` (resolved knobs with source, timing, enforcement and sink,
  plus what-if overrides with did-you-mean suggestions), `profile show --db
  <path>` (the persisted profile manifest, or "legacy database"), `profile
  explain --spec <file>` (exit 2 when the profile is invalid) and `profile
  knobs` (the knob registry and reserved knobs; a knob that cannot change
  after creation reads `unsupported`, in text as in JSON). The profile name of
  `profile show` may stand anywhere among its flags. `profile list --json` adds
  `variants` and the canonical variant's `knobs`; the text output adds
  `variants=`.
- `multidb config upgrade --spec <in> [--out <file>] [--force] [--json]`
  converts a spec to version 2 and turns Studio's `profile.custom.base` (with
  `profile.custom.description`) into `custom_profile`; the validation
  suggestion for a `profile.custom.*` key it leaves in place says what to
  move by hand. It is idempotent, writes `--out` in the format of its
  extension, refuses to overwrite files without `--force`, writes through a
  new, randomly named sibling file that it renames into place (a planted
  symlink cannot redirect the write), and exits 2 without writing when the
  result does not validate, or with `SPEC_TOO_LARGE` and no output at all
  when the re-serialized result exceeds 1 MiB.
- `multidb init --guided --profile <p> --override path=value...
  [--profile-name <slug>]` writes a version 2 spec with a `custom_profile`
  that extends `<p>`. Without `--override` guided init is unchanged.
- `config_spec::suggest_profile_name`, the did-you-mean lookup over built-in
  profile slugs and aliases.
- Control Plane `GET /profiles` adds `variants` (`name`, `storage_profile`,
  `canonical`) and the canonical variant's resolved `knobs` (a full
  `ProfileKnobs`, including `floors` and `compatible_roles`) to every entry.
  The fields are additive and do not change the OpenAPI snapshot.
- TypeScript SDK types: `DatabaseSpec.custom_profile` (`CustomProfileSpec`),
  `ProfileVariantSummary`, and optional `variants` and `knobs` on
  `ProfileSummary`. `DatabaseSpec.version` stays `number` (1 or 2).
- Spec version 2 provisioning fields (preview): `collections[].physical`
  (`PhysicalObjectSpec`, one flat struct for every kind: `kind`, pinned ids,
  document `fields`, `document_indexes`, `full_text` and `geo` entries, table
  `layout`, `columns`, `primary_key`, `table_indexes`, `system_versioning` and
  `keep_history`, vector `dim`, `metric` and `hnsw`, time-series
  `chunk_millis` and `retention_millis`), `security` (`SecuritySpec`: managed
  roles whose grants name spec objects or the whole database, and principals
  mapping users to roles; no secrets and no system scope) and `provisioning`
  (`ProvisioningPolicySpec.unmanaged`: `ignore`, `report` or `fail`). A
  version 1 document that sets one fails with `FIELD_REQUIRES_SPEC_V2` at its
  path. Defaults are pinned (HNSW 16/200/48, full-text language `simple` with
  refresh lag 1, geo precision 6, index ids at position + 1, chunk and
  retention from the profile knobs), as are the derived names
  `<collection>_<path>_fts`, `<collection>_<path>_geo` and `<table>_history`.
  An explicit `physical.retention_millis: null` is refused when the spec is
  decoded, since it could mean the profile knob or no retention.
  See `docs/provisioning.md`.
- `CollectionIndexKind::Geo` (`geo`), a version 2 index kind that needs
  `physical.geo` and no extension.
- New validation codes: `COLLECTION_NAME_INVALID`,
  `COLLECTION_NAME_RESERVED`, `OBJECT_NAME_COLLISION` and
  `SPEC_LIMIT_EXCEEDED` (Warnings in version 1, Errors in version 2);
  `PHYSICAL_PARAMETER_REQUIRED` (Advice in version 2); the version 2 Errors
  `PHYSICAL_KIND_INCOMPATIBLE_WITH_ROLE`, `PHYSICAL_FIELD_NOT_APPLICABLE`,
  `PHYSICAL_NAME_INVALID`, `PROVISION_INDEX_KIND_MISSING`,
  `INDEX_SOURCE_NOT_DOCUMENT`, `PROVISION_PATH_INVALID`, `ID_CONFLICT`,
  `VECTOR_DIM_OUT_OF_RANGE`, `VECTOR_PARAMS_OUT_OF_RANGE`,
  `GEO_PARAMS_OUT_OF_RANGE`, `TIME_SERIES_PARAMS_INVALID`,
  `TABLE_PRIMARY_KEY_UNKNOWN`, `TABLE_COLUMN_DUPLICATE`,
  `TABLE_INDEX_COLUMN_UNKNOWN`, `SECURITY_NAME_INVALID`,
  `SECURITY_RESERVED_NAME`, `SECURITY_SYSTEM_GRANT_FORBIDDEN`,
  `SECURITY_GRANT_TARGET_INVALID`, `SECURITY_GRANT_UNKNOWN_OBJECT`,
  `SECURITY_PRINCIPAL_UNKNOWN_ROLE` and `SECURITY_DUPLICATE`; and the version
  2 Warnings `SECURITY_BROAD_GRANT` (every grant on the whole database,
  which in this build also covers system reads, writes or administration) and
  `SECURITY_INDEX_GRANT_WITHOUT_SOURCE_READ`.
  `PROFILE_KNOB_NOT_SUPPORTED_BY_RUNTIME` also covers full-text languages
  other than `simple`. `TIME_SERIES_PARAMS_INVALID` follows the runtime's
  time-series rules: a positive `retention_millis` of at least one chunk and
  a `physical.max_future_skew_millis` of at least 0. The future-skew bound
  defaults to the runtime default for new series (300000 ms) when it is left
  out, and an explicit `null` means no bound; it is set at creation, compared
  by every plan and pinned by `export_spec`. A series created before the
  bound existed, which the runtime reads back as `i64::MAX`, exports as
  `null` with a note, and plans show its bound as `null`, since JavaScript
  clients cannot represent `i64::MAX`; plan and apply read `null` as no
  bound, so the exported spec round-trips. A spec that spells the bound out
  as `i64::MAX` is read as `null`, and specs, plans, apply reports, provision
  records and drift reports always write no bound as `null`.
- `database_spec_v2_schema()` and `docs/schemas/database-spec-v2.schema.json`
  describe `physical`, `security`, `provisioning` and `geo`.
- `multidb::provision::physical::project`, which projects a spec onto the
  catalog objects provisioning would build (`DesiredObject`, `DesiredKind`
  with runtime types, `DesiredObject::params_json`,
  `physical::time_series_config`). It is always strict and never touches a
  database: a version 1 collection whose index kinds lack parameters (for
  example a `document` index without `physical.document_indexes`) fails with
  `PHYSICAL_PARAMETER_REQUIRED`.
- `Database::create_table_with_layout` and `create_table_with_layout_as`
  create a table with an explicit layout, so a columnar table can live in a
  row-class database; the layout is persisted with the table and kept across
  reopens. `create_table` delegates with the database layout.
- `config plan` (the version 1 migration planner) reports changes of
  `collections[].physical`, `security` and `provisioning` as unsupported
  steps; specs without them plan exactly as before, with the same plan ids.
- `multidb::provision::plan_provision`, a read-only provisioning planner
  (preview). It computes a `ProvisionPlan` for a spec against an absent
  target (`ProvisionTargetRef::Path` of a missing or empty file, or
  `Volatile`) or an open single-node CP database (`Open`): steps in a fixed
  phase order with a safety class each (`additive`, `metadata`, `retune`,
  `strengthening`, `weakening`, `destructive`, `format_upgrade`,
  `unsupported`), the per-path `allow_weakening` and `allow_destructive`
  flags and `allow_format_upgrade` they need (`ProvisionOptions`), an
  outcome (`noop`, `ready`, `needs_flags`, `blocked`), `auto_approvable`, the
  planned objects with field diffs, how every guarantee is enforced on the
  calling surface (`GuaranteeEnforcement`, for example `operator_managed`
  backups, `enforced_with_development_key` audit, `not_enforceable` encryption
  at rest outside the Rust API) and a `prov_` plan id over the canonical
  spec, a live fingerprint, the options, the surface
  (`ProvisionSurface`), the canonical target path and the crate version.
  Objects that already match are adopted; differing ones are conflicts,
  since there is no alter API; managed objects the spec drops are released
  with their data kept; RBAC is managed-only and never touches the bootstrap
  administrator. Enabling system versioning on a table the plan adopts is
  never auto-approvable, and `deployment.storage_path` is not part of what
  counts as the applied spec. Planning never creates a file or writes a
  database. New plan issue codes include `PROVISION_ENGINE_MISMATCH`,
  `PROVISION_FORMAT_MISMATCH`, `PROVISION_OBJECT_CONFLICT`,
  `PROVISION_SECURITY_STATE_STALE` (RBAC or the audit flag changed in memory
  only, with `set_authz_policy` or by a change whose persist failed, which an
  apply would persist), `RBAC_SELF_LOCKOUT`,
  `AUDIT_REQUIRED_FOR_APPLY`, `PROVISION_AUDIT_KEY_MISMATCH` (a plan with
  steps whose appended audit events would not verify against the existing
  chain), `GUARANTEE_NOT_ENFORCEABLE` and `STEP_REQUIRES_FLAG`, and the
  Warning `SECURITY_ROLE_ALREADY_ASSIGNED` (a created or widened role that
  users the spec does not manage already hold). See `docs/provisioning.md`.
- `provision::drift`, which reports how an open database differs from a spec
  (`DriftReport` with `in_sync` and typed items), and `provision::export_spec`,
  which describes an open database as a version 2 spec: the applied spec of
  its provision record, or one inferred from the catalog, profile manifest,
  RBAC and audit state with every id pinned and what a spec cannot express
  listed in `operation_hints["export.unsupported_objects"]`.
- `Database::provision_state`, which reads the provision record
  (`ProvisionRecord`) from the new system table `__provision`; a record of a
  newer `record_version` or a corrupt one is refused. The persistent format
  registry lists it as `provision record` (`__provision/record_version` 1).
- `config_spec::canonical_spec_json` and `config_spec::spec_digest` (`spec_`
  and 32 hex characters of blake3 over the canonical JSON).
- `multidb::provision::apply_provision`, which applies a spec to an open
  single-node database (preview, Rust API). It recomputes the plan, accepts
  only the current `prov_` id (`Confirmation::PlanId`; another `prov_` id is
  `PlanChanged`, anything else a `rejected` report) or
  `Confirmation::AutoApproveAdditive` for an `auto_approvable` plan, and runs
  as a `ProvisionActor`: a principal with `System:Admin` and `Database:Admin`,
  whose objects are created through the audited `*_as` APIs, or a local
  operator (`local:cli`, `local:serve`, `local:embedded`) audited with
  `provision_create_object`. Audit comes first: it is turned on as the first
  step or the apply is refused (`AuditRequired`), `provision_apply_started` is
  audited before any change and `provision_apply_completed` (or
  `provision_apply_failed`) after it, and turning audit off runs last. The
  provision record is journaled after every step and gets a history entry
  (`Database::provision_history`, at most 100); a failed step gives a
  `partially_applied` report and a `failed` record, and the next plan holds
  only what remains. Once anything changed the database, an apply answers
  with a report, never an error: a step that fails after its write committed
  says so and the record manages the object it created, and a completion
  event that cannot be written leaves the apply `applied` with the Warning
  `PROVISION_COMPLETION_NOT_AUDITED`. Provisioning writes are system writes:
  they run no triggers and are not charged to a tenant quota. The next plan
  compares declared guarantees and metadata with the last finished apply
  (`ProvisionRecord::last_applied`), so a confirmed weakening needs its flag
  again. Objects are only created or adopted,
  releases keep the data, RBAC changes touch managed names only and commit in
  one batch, and the storage format upgrade writes the value-framing manifest
  and `schema_version` 2 together, taking effect at the next open. See
  `docs/provisioning.md`.
- `multidb::provision::provision_new_database`, which creates a database from
  a spec at an absent target with `create_database_with_profile` (audit on,
  the environment's audit key and, on the Rust API, a `LocalEnvelope`
  encryption config, whose keyring is created with the new file unless one
  already exists at its path), checks that the plan recomputed against it
  equals the confirmed one and applies it under the confirmed plan id; once
  the database exists, a plan that differs is a `plan_changed` report with
  `data_mutated: true`, not an error. An encryption
  config for a spec with `at_rest: false` is refused before anything is
  created (`PROVISION_ENCRYPTION_MISMATCH`).
- `Database::set_security_state_as`, which replaces the RBAC policy and the
  principal registry in one persisted batch after checking `System:Admin`,
  audited as `set_security_state`.
- `Database::override_query_governor`, which sets the query governor limits
  that are `Some` and keeps the others. `multidb serve` uses it for each of
  `MULTIDB_MAX_CONCURRENT_QUERIES` and `MULTIDB_QUERY_QUEUE_TIMEOUT_MS` that
  is set, where it used to fill the unset limit from the profile.
- CLI provisioning (preview), as the local operator `local:cli`:
  `multidb config plan --spec <file> [--db <path>] [--out <plan.json>
  [--force]]` (a dry run that never creates the database file; text output
  lists every step with its safety class and flag, every guarantee status and
  the exact `config apply ... --confirm prov_...` command; `--out` never
  replaces an existing file without `--force`, and never the spec or the
  database), `config apply --spec <file>
  [--db <path>] (--confirm <prov_id> | --auto-approve)` (recomputes the plan,
  creates a missing database with `provision_new_database` or converges an
  existing one with `apply_provision`), `config drift --spec <file>`,
  `config status --db <path>` (the provision record with its guarantee
  statuses, the last 20 history entries, the profile manifest and the drift
  against the applied spec) and
  `config export --db <path> [--format yaml|json] [--out <file>] [--force]`
  (the exported `deployment.storage_path` points at the database relative to
  the directory of `--out`, or is its absolute path on stdout; `--out` never
  names the database or another multidb database, even with `--force`; a
  spec over 1 MiB, which `--spec` could not read back, exits 2 with
  `SPEC_TOO_LARGE` and is written nowhere, as in `config upgrade`).
  `--allow-weakening` and `--allow-destructive` (repeatable or
  comma-separated), `--allow-format-upgrade` and `--unmanaged
  ignore|report|fail` are part of the `prov_` id. The target is `--db`, else
  the spec `storage_path` resolved against the spec file's directory; an AP
  database is refused with `PROVISION_REPLICATION_UNSUPPORTED`, and a
  database another process holds open with "database is in use (is multidb
  serve running?)". Exit codes: 0 ready, applied, noop or in sync; 1 a
  partial apply, a runtime error or a locked database; 2 invalid, blocked,
  flags or confirmation missing, or rejected; 3 drift (`config drift` only);
  4 the confirmed plan changed. `--spec` cannot be combined with `--plan`,
  `--current` or `--desired`; the version 1 `config plan --current --desired`
  and `config apply --plan --confirm` flows are unchanged and stay
  audit-only. See `docs/provisioning.md`.
- `multidb init --guided (--profile | --template) ... --apply [--db <path>]
  [--confirm <prov_id>]` writes the files as before and then provisions a new
  database from the spec at `--db`, the spec `storage_path` relative to the
  output directory, or `<out dir>/<name>.redb`, auto-approving a plan that
  only creates, adopts or records. An existing database is refused, and a
  blocked or not auto-approvable plan keeps the written files and exits 2;
  `--json` adds `database` and `provision` to the summary.
- `multidb serve` spec mode (preview), as the local operator `local:serve`:
  `MULTIDB_SPEC_PATH` names the spec the database is built from and checked
  against at every start, and `MULTIDB_SPEC_APPLY` what a start may change:
  `verify` (the default; starts only when the database exists and matches),
  `create` (also creates a missing database from an auto-approvable plan) or
  `additive` (also applies auto-approvable additive plans, including the rest
  of an interrupted apply). The database path is an explicit
  `MULTIDB_DB_PATH`, else the spec `storage_path` relative to the spec file,
  else the default path. An explicit `MULTIDB_PROFILE` that selects another
  storage profile, an explicit `MULTIDB_REPLICATION` (read in spec mode only)
  that contradicts the spec, `encryption.at_rest` and volatile profiles are
  refused. Every refusal comes before a port is bound and prints the plan and
  the exact `multidb config apply ... --confirm prov_...` command (with the
  flags the plan needs) that confirms it, or the `config plan` command for a
  blocked plan. Without `MULTIDB_SPEC_PATH`, `serve` is unchanged. See
  `docs/docker.md` and `docs/provisioning.md`.
- `multidb::provision::open_from_spec`, which opens or creates the database of
  a spec at startup on the Rust API or serve surface: it probes the target
  (with crash recovery), opens an existing database with
  `open_database_with_ops` (with `ProvisionEnvironment.encryption`, also for
  an encrypted database whose metadata cannot be probed without the key, and
  never creating a keyring for it, so a missing keyring fails the start; and
  re-applying `ProvisionEnvironment.audit`, so the audit chain stays on the
  caller's key) and applies an auto-approvable plan
  as `local:embedded` or `local:serve` when the `StartupMode` allows it. The
  pure `serve_startup_decision(plan, mode)` returns the `StartupDecision`
  (`start`, `apply_then_start` or `refuse` with the reason); a refusal is
  `ProvisionError::StartupRefused` (`StartupRefusal`), a start returns a
  `StartupReport`.
- Helm chart spec mode: `spec.enabled` (off by default), `spec.configMap`,
  `spec.key`, `spec.mountPath` and `spec.apply` (`verify` by default) mount
  the spec read-only, set `MULTIDB_SPEC_PATH` and `MULTIDB_SPEC_APPLY`, and
  set `MULTIDB_PROFILE` and `MULTIDB_REPLICATION` to empty values, which
  override the image's `MULTIDB_PROFILE=transactional` and leave the profile to
  the spec;
  `docker-compose.yml` has a commented example.
- Control Plane provisioning (preview) of the served database, as the calling
  principal: `POST /provision/plan` (`{spec, options?}`; a dry run on the
  `control_plane` surface, 200 for ready, noop and needs-flags plans, 422 with
  the plan in `data` when blocked), `POST /provision/apply` (`{spec, confirm,
  options?}`; the server recomputes the plan and applies it only when
  `confirm` is its current `prov_` id: 409 `plan_changed` or
  `object_conflict`, 422 `confirmation_required`, `flags_required`,
  `provision_blocked`, `invalid_spec`, `audit_required` or
  `rbac_self_lockout`, 500 `provision_partially_applied` with the report),
  `POST /provision/drift`, `GET /provision/state` (the provision record,
  history, profile manifest, live fingerprint and the spec mode of `serve`)
  and `GET /config/live` (the served database as a spec, from its record or
  inferred). Plans, drift, state and `/config/live` run under the shared
  `serve` lock next to statements, and an apply holds the exclusive lock from
  its recomputed plan through its last step; a wrapped busy query governor
  stays 503 `busy` and a write conflict 409 `conflict`. A query governor knob
  set for the process (each of `MULTIDB_MAX_CONCURRENT_QUERIES` and
  `MULTIDB_QUERY_QUEUE_TIMEOUT_MS` that is set, through
  `Database::override_query_governor`, or both with
  `Database::set_query_governor`) is not reported as
  `session_pending_restart`; a retune of a governor knob the process did not
  set is, in the apply report and in later plans. Every endpoint needs
  `System:Admin`, and applying also `Database:Admin`; a spec `storage_path` is
  the Advice `PROVISION_STORAGE_PATH_IGNORED`, since the Control Plane never
  creates databases or opens paths. The `OpenAPI` snapshot documents the five
  operations (`x-multidb-stability: preview`, request bodies pointing at the
  version 2 spec schema, error envelopes that carry `data`), and
  `StudioManifest` gains `provisioning_supported: true` and the capability
  `provision_apply`. See `docs/provisioning.md`.
- SDK provisioning methods: TypeScript `provisionState`, `provisionPlan`,
  `provisionApply`, `provisionDrift` and `liveConfig` with the types
  `ProvisionPlan`, `ProvisionStep`, `ProvisionApplyReport`, `DriftReport`,
  `GuaranteeEnforcement`, `ProvisionState`, `ProvisionOptions` and
  `LiveConfig`; Go `ProvisionState`, `ProvisionPlan`, `ProvisionApply`,
  `ProvisionDrift` and `LiveConfig`; Python and Rust `provision_state`,
  `provision_plan`, `provision_apply`, `provision_drift` and `live_config`.
  The TypeScript and Studio `DatabaseSpec` types gain the optional `security`,
  `provisioning` and `collections[].physical` fields and the `geo` index kind,
  and `StudioManifest` the optional `provisioning_supported`.
- `provision::live_fingerprint`, the fingerprint of the live state plans are
  computed against, and `admin::AdminState::with_spec_mode`, which `multidb
  serve` uses in spec mode so `GET /provision/state` reports the spec digest
  and `MULTIDB_SPEC_APPLY` mode it started with.
- `examples/provisioning`: a full version 2 spec that provisioning builds end
  to end (a document collection with fields, document, full-text and geo
  indexes, a system-versioned ledger table, a vector collection, a columnar
  table in a `HighDurability` row-class database, a managed role and
  principal), validated, explained and dry-run planned by
  `scripts/templates-smoke.ps1`.
- `templates::TemplateSpec::physical(collection)` returns the `physical`
  parameters a built-in template gives a collection, and
  `templates::TEMPLATE_SPEC_VERSION` the spec version templates emit.
  `multidb template list --json` adds `spec_version` and each collection's
  `physical`, and the `template explain` text header adds `spec_version`.

### Changed

- Built-in templates emit spec version 2 with the `physical` parameters
  provisioning needs, so `multidb init --guided --template <t> --apply` builds
  `game-save` (document index on `slot`), `desktop-embedded` (`title`, and
  one-hour chunks with 30-day retention for `usage_metrics`), `ai-memory`
  (1536-dimension cosine `memories`, `subject`) and `analytics` (one-hour
  `metrics` chunks, a typed columnar `aggregates` table) on the CLI.
  `secure-saas` (`tenant_id`) keeps `encryption.at_rest: true`, so its plan
  is blocked on the CLI, `serve` and the Control Plane with
  `GUARANTEE_NOT_ENFORCEABLE` and is provisioned through the Rust API with a
  `LocalEnvelope` key. The generated README gains a Provision section with
  the plan, apply and drift commands and the guarantee statuses a CLI plan
  reports, and the known limits name the values fixed at creation. The
  checked-in `examples/<template>/multidb.yaml` and `README.md` are
  regenerated (`seed.json` and `smoke.ps1` are unchanged); the version 1
  originals stay frozen under `tests/fixtures/spec-v1`, still validate, and
  are blocked for provisioning with `PHYSICAL_PARAMETER_REQUIRED`.
- `scripts/templates-smoke.ps1` also validates and explains the frozen
  version 1 specs and `examples/provisioning`, dry-runs the provisioning
  plan of the example, and runs `init --apply`, `config drift` (exit 0) and
  a re-apply (a noop) for every template, with `secure-saas` exiting 2.
  `scripts/preview-smoke.ps1` adds a `config plan --spec --db` dry run of the
  generated ai-memory spec and records its outcome in the summary.
- `POST /config/apply` also accepts `{current, desired, confirm}`: the server
  recomputes the plan with `MigrationPlanner::plan`, decoding each side as
  `/config/plan` does. The `config_apply` audit detail ends with
  `plan_source: recomputed`, or `plan_source: client` for the `{plan,
  confirm}` body (and for `Database::confirm_config_apply_as`), and keeps at
  most 48 characters of a client plan id. The endpoint stays audit-only
  (`data_mutated: false`), and its `OpenAPI` 422 response is documented as a
  success or an error envelope, since a version error in a recomputed side is
  `invalid_spec_version`. `GET /config` is documented as legacy, replaced by
  `GET /config/live`.
- `multidb config upgrade` adds a note for every physical parameter
  provisioning still needs (`PHYSICAL_PARAMETER_REQUIRED`), so an upgraded
  version 1 spec lists what to add before `config apply --spec`.
- The `history.system_versioning`, `history.keep_history`,
  `time_series.default_chunk_millis` and `time_series.default_retention_millis`
  knobs are `enforced` in `KNOB_REGISTRY`: provisioning applies them to the
  tables and series it creates. Provisioned history starts where versioning
  is enabled, never below the history GC horizon, so a plan that adds
  versioning also applies after GC pruned: a table the apply creates starts
  at the newest commit after its creation, and an existing table at the
  horizon (LSN 0 when GC never pruned), keeping the `AS OF` history it still
  has. A table that keeps its history holds the GC watermark at that start,
  so GC keeps every `AS OF` result from there and, while the table exists,
  prunes and truncates only below it; the provision record lives outside the
  commit log, so GC and truncation change no plan, drift or export.
  Explain says that every-open knobs apply to databases created with
  `db::create_database_with_profile` or by provisioning, that
  provisioning-timing knobs are applied by provisioning to the objects it
  creates, and that enabling compression on an existing database is the
  provisioning format upgrade (`allow_format_upgrade`).
- Explain wording: `$.compiled_policy.storage_profile` names the storage class
  and engine instead of "storage profile selected", encryption at rest reads
  "requested; enforced only on the Rust API with an encryption config in this
  build" instead of "encrypted storage required", and `RuntimeLimits` is
  labelled advisory catalog guidance that this build does not enforce (its
  values are unchanged).
- Version 1 specs also gain the Warnings `COLLECTION_NAME_INVALID`,
  `COLLECTION_NAME_RESERVED`, `OBJECT_NAME_COLLISION` and
  `SPEC_LIMIT_EXCEEDED` after every existing issue, for collection names the
  runtime would refuse, duplicate collection names and oversized `overrides`,
  `operation_hints` and collection lists.
- `multidb init` refuses `--db` and `--confirm` without `--apply` as a usage
  error; they were ignored before.
- Version 1 specs gain only Advice and Warnings, no new Errors:
  `PROFILE_NOT_PROVISIONABLE` after `CUSTOM_PROFILE` for unknown profiles,
  `OVERRIDE_IS_METADATA_ONLY` for `overrides` keys in a knob namespace
  (including Studio's `profile.custom.*` keys), and the Warning
  `PROFILE_DEPLOYMENT_MISMATCH` for a volatile profile such as `in_memory`
  with a `deployment.storage_path`. Their existing issues, order, status and
  plan ids are unchanged.
- A missing, non-integer or unsupported spec `version` is a validation issue
  (`MISSING_VERSION`, `UNSUPPORTED_VERSION`): the CLI exits 2 and the Control
  Plane answers 422 (`/config/validate` with the report in a success
  envelope, `/config/plan` with an error envelope whose code is
  `invalid_spec_version`). A body without `version` was HTTP 400
  `invalid_json` and CLI exit 1 before. The OpenAPI snapshot documents the
  `/config/plan` 422 response as a success or an error envelope
  (`JsonObjectOrErrorEnvelope`).
- Spec files over 1 MiB (`SPEC_TOO_LARGE`) or over the YAML alias expansion
  budget (`SPEC_TOO_COMPLEX`) are rejected before the typed decode (CLI exit
  2). Control Plane request bodies over 1 MiB stay HTTP 400 `invalid_json`
  from the router's body limit, and JSON bodies cannot reach
  `SPEC_TOO_COMPLEX`.
- The SQL parser limits of `QueryExecutionConfig` (the `query.max_sql_bytes`,
  `max_statements`, `max_values_rows` and `parser_recursion_limit` knobs) now
  bind the pre-parses of `Database::query` (the PostgreSQL compatibility
  queries), `query_as` and `query_as_of_as` (RBAC) and `query_as_of`
  (temporal retention), which used sqlparser's defaults (recursion depth 50,
  no size or statement limits): a configured lower limit fails every path
  with the same parser error, `SELECT version()` and the catalog views no
  longer bypass `max_sql_bytes` (the whole SQL text counts, trailing
  whitespace and semicolons included), and statements multidb executes
  itself, such as `INSERT` and indexed equality lookups, may nest to the
  configured depth through `query` and `query_as`. DataFusion still parses
  the SELECTs it plans with its own fixed recursion limit of 50; its SQL
  errors (`DataFusionError::SQL`: its parser errors, that limit included, and
  the planner errors it builds from them) are now `QueryError::Parse`
  (SQLSTATE 42601) like the engine's, instead of `QueryError::DataFusion`
  (XX000). The pgwire
  extended-protocol Describe plans the statement through the same parse as
  execution instead of pre-parsing it with sqlparser's defaults.
- Metadata reads tolerate compression frames. A legacy database created with
  `OperationalConfig.performance.compression` and opened without it now fails
  with `StorageFormatMismatch` (previously an opaque serde error). Every
  `__meta` value (`schema_version`, `profile`, `replication`, `layout`) that
  does not parse as JSON is now `ConfigError::MetadataUnreadable` instead of
  `ConfigError::Serde`; for encrypted metadata opened without a key its
  message says "supply OperationalConfig.encryption". `SchemaVersionMismatch`
  now reports `expected: 2`, the newest version this build reads.
- Restore builds its target from the profile manifest in the backup: a
  restored compressed manifest database is value-framed again, as its
  manifest says, and `verify_backup` covers such backups. A restore
  `DbConfig` whose storage profile disagrees with the backed-up manifest
  fails with `BackupError::Config(ProfileMismatch)`, and a corrupt backed-up
  manifest with `BackupError::Corruption`, before the target is created.
  Restore now reads and checks every backup payload, full and incremental,
  before it creates the target, so a payload that fails its checksum or
  decryption no longer leaves an empty initialized target behind. It reads
  each incremental payload twice (three times for a time target), one at a
  time (a check pass that also
  decides the profile floors, then the apply pass), so it still holds at
  most one incremental payload in memory, beside the full payload.
  `restore_backup_from_uri` restores with the default `RestoreConfig`, so it
  refuses unmet profile floors too, and `restore_backup_from_uri_with_config`
  takes the opt-in. Hibernation resume (`cloud::resume_database`,
  `resume_database_guarded`) takes no `RestoreConfig`, so it refuses them
  with no opt-in, before it creates the target or consumes the marker.
- Every CLI `--profile` flag and `MULTIDB_PROFILE` also accept product slugs
  (their canonical variant), aliases in snake or kebab case, and `auto`,
  which reads the storage profile of the existing database read-only
  (`restore` refuses `auto`). A file that was not closed cleanly, such as the
  one a stopped `serve` leaves, first gets redb's crash recovery, as with any
  opener. The CLI and `serve` still open single-node CP databases only: with
  `auto`, an AP database fails with `ReplicationMismatch`, as with a named
  profile. Every value accepted before keeps its meaning.
  Guided init accepts profile names in any case.
- Guided init with an alias of a non-canonical variant (for example
  `high_durability` or `time-series`) prints a warning that the version 1
  spec records the product slug; `--json` adds `warnings`, `variant` and
  `storage_profile` to the summary object.
- The Studio custom-profile builder emits a version 2 spec with a real
  `custom_profile` (`extends` a product slug or variant, plus optional
  `durability.commit`, `storage.layout`, `compression.algorithm` and
  `cache.max_entries` knobs) instead of the `overrides["profile.custom.*"]`
  and `operation_hints["profile.custom.slug"]` keys; an in-memory base gives
  an embedded deployment without a storage path. Studio still validates and
  dry-run plans these specs only. Run `multidb config upgrade` on specs saved
  by older Studio builds. Studio's fallback for a spec without `profile` is
  `balanced` instead of the unknown `durable`.

### Breaking

- `DatabaseSpec` struct literals must set the new `custom_profile`,
  `security` and `provisioning` fields, and `CollectionRoleSpec` literals the
  new `physical` field; `None` keeps today's behaviour.
- `CollectionIndexKind` gains `Geo`; exhaustive matches outside the crate must
  handle it.
- `admin::StudioManifest` struct literals must set the new
  `provisioning_supported` field.
- `ConfigSpecError::UnsupportedVersion` gains a `supported` range field.
- `CompiledPolicy` struct literals must set the new `resolved_profile` field.
- `ConfigError` gains `AlreadyInitialized`, `StorageFormatMismatch`,
  `UnsupportedProfileManifest`, `ProfileManifestInvalid`,
  `ProfileFloorNotMet` and `MetadataUnreadable`. `ConfigError` is not
  `non_exhaustive`, so exhaustive matches outside the crate must handle them.
- `RestoreConfig` gains the public field `allow_unmet_profile_floors`, so
  struct literals must set it (`false` keeps the refusal) or use
  `..RestoreConfig::default()`; `RestoreConfig::default()` and the builders
  are unchanged. `BackupError` gains `ProfileFloorsNotMet { floors }` and is
  not `non_exhaustive` either.
- `compat::execute_compat_sql` and `compat::compat_sql_requirements` take a
  new `&QueryExecutionConfig` argument, the SQL parser limits they apply
  (for example the database's `PerformanceConfig.query`).
- `Database::poll_changefeed_as` and `Database::poll_changefeed_page_as` with
  `ChangefeedOptions { include_system: true }` return `DbError::AuthzDenied`
  unless the principal holds `System:Admin` (a grant on `Resource::System`, or
  `Admin` on `Resource::Database`). Grant it to the principals that need
  system rows, or read them through the trusted `Database::poll_changefeed`.
- `QueryError` gains `AuthzDenied` (SQLSTATE 42501), returned by
  `SqlEngine::execute_with_read_guard` when its read guard refuses a read;
  `Database` methods report it as `DbError::AuthzDenied`.

## 0.1.0-preview.1

- Initial public-preview target.
- Includes the public-preview support surface documented in
  `docs/support-matrix.md`.
