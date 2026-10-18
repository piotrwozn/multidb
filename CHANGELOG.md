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
  `STEP_REQUIRES_FLAG`. See `docs/provisioning.md`.
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
  only what remains; its declared guarantees and metadata are compared with
  the last finished apply (`ProvisionRecord::last_applied`), so a confirmed
  weakening needs its flag again. Objects are only created or adopted,
  releases keep the data, RBAC changes touch managed names only and commit in
  one batch, and the storage format upgrade writes the value-framing manifest
  and `schema_version` 2 together, taking effect at the next open. See
  `docs/provisioning.md`.
- `multidb::provision::provision_new_database`, which creates a database from
  a spec at an absent target with `create_database_with_profile` (audit on,
  the environment's audit key and, on the Rust API, a `LocalEnvelope`
  encryption config), checks that the plan recomputed against it equals the
  confirmed one and applies it under the confirmed plan id. An encryption
  config for a spec with `at_rest: false` is refused before anything is
  created (`PROVISION_ENCRYPTION_MISMATCH`).
- `Database::set_security_state_as`, which replaces the RBAC policy and the
  principal registry in one persisted batch after checking `System:Admin`,
  audited as `set_security_state`.
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
  `config export --db <path> [--format yaml|json] [--out <file>] [--force]`.
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
  `open_database_with_ops` (re-applying `ProvisionEnvironment.audit`, so the
  audit chain stays on the caller's key) and applies an auto-approvable plan
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
  inferred). Every endpoint needs `System:Admin`, and applying also
  `Database:Admin`; a spec `storage_path` is the Advice
  `PROVISION_STORAGE_PATH_IGNORED`, since the Control Plane never creates
  databases or opens paths. The `OpenAPI` snapshot documents the five
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
  tables and series it creates. Explain says that every-open knobs apply to
  databases created with `db::create_database_with_profile` or by
  provisioning, that provisioning-timing knobs are applied by provisioning to
  the objects it creates, and that enabling compression on an existing
  database is the provisioning format upgrade (`allow_format_upgrade`).
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
- `admin::StudioManifest` struct literals must set the new
  `provisioning_supported` field.
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
