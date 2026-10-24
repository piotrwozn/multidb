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

### Changed

- Explain wording: `$.compiled_policy.storage_profile` names the storage class
  and engine instead of "storage profile selected", encryption at rest reads
  "requested; enforced only on the Rust API with an encryption config in this
  build" instead of "encrypted storage required", and `RuntimeLimits` is
  labelled advisory catalog guidance that this build does not enforce (its
  values are unchanged).
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
  extended-protocol Describe still pre-parses with sqlparser's defaults.
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

- `DatabaseSpec` struct literals must set the new `custom_profile` field;
  `None` keeps today's behaviour.
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

## 0.1.0-preview.1

- Initial public-preview target.
- Includes the public-preview support surface documented in
  `docs/support-matrix.md`.
