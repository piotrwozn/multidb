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

### Added

- Typed profile model in `multidb::config_spec`: `ProfileKnobs`, the
  `KNOB_REGISTRY` const table (bounds, timing, runtime sink and enforcement of
  every knob), `RESERVED_KNOBS`, built-in variants pinned 1:1 to storage
  profiles (`profile_variants`), and the pure resolver `resolve_profile` /
  `resolve_named_profile`, which records the provenance of every knob and a
  `prof_` digest. Presets equal today's runtime defaults: compression none,
  limits unlimited (planned until the single write pipeline lands) and query
  concurrency auto. See `docs/profiles.md`.
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
