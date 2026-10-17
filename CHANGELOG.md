# Changelog

All notable public changes will be recorded here.

## Unreleased

- Prepared the repository for public preview publication.
- Added public repository hygiene files, security policy, contribution guide,
  issue templates and release notes.
- Kept local planning history out of the public source baseline.

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
  `resolve_profile` returns that issue as `ProfileResolution::Invalid`;
  documents without it serialize, validate and plan exactly as before, pinned
  by the frozen corpus in `tests/fixtures/spec-v1/`.
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
  whose manifest sets `floors.audit`. Restore does not check floors: a
  restored `floors.encryption_at_rest` database is not encrypted (see
  `docs/profiles.md`). `CompiledPolicy::binding` refuses a path for a volatile
  storage class (`VolatileWithPath`), also for a version 1 spec, where
  validation only warns. CP, AP and sharded openers refuse manifests.
  `inspect_database_metadata` (`MetadataProbe`, `ProbedMetadata`) reads a
  database's metadata through a read-only redb open, so it never creates,
  writes or repairs files; a file that was not closed cleanly is
  `NeedsRecovery`, and `recover_database_file` runs redb's crash recovery on
  it without touching the metadata. `ProbedMetadata.unmet_floors` lists
  `floors.encryption_at_rest` when the manifest sets it, because the probe
  read the metadata without a key, and `profile show --db` prints it as a
  warning. The persistent format registry
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
  converts a spec to version 2 and turns Studio's `profile.custom.*` keys into
  `custom_profile`; it is idempotent, writes `--out` in the format of its
  extension, refuses to overwrite files without `--force`, writes through a
  new, randomly named sibling file that it renames into place (a planted
  symlink cannot redirect the write), and exits 2 without writing when the
  result does not validate.
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
  2 Warnings `SECURITY_BROAD_GRANT` and
  `SECURITY_INDEX_GRANT_WITHOUT_SOURCE_READ`.
  `PROFILE_KNOB_NOT_SUPPORTED_BY_RUNTIME` also covers
  `physical.max_future_skew_millis` and full-text languages other than
  `simple`.
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
  administrator. Planning never creates a file or writes a database. New
  plan issue codes include `PROVISION_ENGINE_MISMATCH`,
  `PROVISION_FORMAT_MISMATCH`, `PROVISION_OBJECT_CONFLICT`,
  `PROVISION_SECURITY_STATE_STALE`, `RBAC_SELF_LOCKOUT`,
  `AUDIT_REQUIRED_FOR_APPLY`, `GUARANTEE_NOT_ENFORCEABLE` and
  `STEP_REQUIRES_FLAG`. Nothing applies a plan yet. See
  `docs/provisioning.md`.
- `provision::drift`, which reports how an open database differs from a spec
  (`DriftReport` with `in_sync` and typed items), and `provision::export_spec`,
  which describes an open database as a version 2 spec: the applied spec of
  its provision record, or one inferred from the catalog, profile manifest,
  RBAC and audit state with every id pinned and what a spec cannot express
  listed in `operation_hints["export.unsupported_objects"]`.
- `Database::provision_state`, which reads the provision record
  (`ProvisionRecord`) from the new system table `__provision`; a record of a
  newer `record_version` or a corrupt one is refused.
- `config_spec::canonical_spec_json` and `config_spec::spec_digest` (`spec_`
  and 32 hex characters of blake3 over the canonical JSON).

### Changed

- Explain wording: `$.compiled_policy.storage_profile` names the storage class
  and engine instead of "storage profile selected", encryption at rest reads
  "requested; enforced only on the Rust API with an encryption config in this
  build" instead of "encrypted storage required", and `RuntimeLimits` is
  labelled advisory catalog guidance that this build does not enforce (its
  values are unchanged).
- Version 1 specs also gain the Warnings `COLLECTION_NAME_INVALID`,
  `COLLECTION_NAME_RESERVED` and `SPEC_LIMIT_EXCEEDED` after every existing
  issue, for collection names the runtime would refuse and for oversized
  `overrides`, `operation_hints` and collection lists.
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
  Restore now reads the full backup payload before it creates the target, so
  a full payload that fails its checksum or decryption no longer leaves an
  empty initialized target behind.
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
- `ConfigSpecError::UnsupportedVersion` gains a `supported` range field.
- `CompiledPolicy` struct literals must set the new `resolved_profile` field.
- `ConfigError` gains `AlreadyInitialized`, `StorageFormatMismatch`,
  `UnsupportedProfileManifest`, `ProfileManifestInvalid`,
  `ProfileFloorNotMet` and `MetadataUnreadable`. `ConfigError` is not
  `non_exhaustive`, so exhaustive matches outside the crate must handle them.

## 0.1.0-preview.1

- Initial public-preview target.
- Includes the public-preview support surface documented in
  `docs/support-matrix.md`.
