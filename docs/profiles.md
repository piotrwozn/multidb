# Profiles

A profile describes how a database is stored and tuned. This page covers the
typed profile model: product profiles, variants, presets, custom profiles, the
knob registry and the resolver that turns a `DatabaseSpec` into concrete knobs.

Status in this build: the model, the knob registry and the resolver are a Rust
API in `multidb::config_spec` (`resolve_profile`, `resolve_named_profile`,
`ResolvedProfile::performance_config`, `KNOB_REGISTRY`,
`parse_knob_assignment`). Specs may declare version 1 or 2; `custom_profile`
is a version 2 field. `multidb config validate`, `config explain`,
`config plan` and the Control Plane `/config/validate` and `/config/plan`
resolve the profile of every spec, report its issues and explain every knob.
Only the Rust API builds a database from a resolved profile:
`db::create_database_with_profile` persists the knobs in
`__meta.profile_manifest`, and every single-node opener, `serve` included,
re-applies them (see [Persistence and format](#persistence-and-format)).
Nothing in the CLI, `serve` or the Control Plane creates a database from a
spec. The CLI inspects profiles and specs (`multidb profile show`, `profile
explain`, `profile knobs`), writes version 2 specs (`init --override`,
`config upgrade`) and reads a database's own profile with `--profile auto`
(see [CLI](#cli)). `GET /profiles` lists the variants and preset knobs, and
the Studio custom-profile builder writes version 2 `custom_profile` specs for
validation and dry-run plans (see
[Control Plane and Studio](#control-plane-and-studio)). Existing version 1
specs keep their validation issues, serialization and plan ids; they only
gain new Advice and Warnings.

## Model

- **Product profile.** One of the six built-in profiles listed by
  `multidb profile list` (`game_local_balanced`, `desktop_app_embedded`,
  `ai_agent_memory`, `secure_app`, `production_cp`, `analytics_columnar`). It
  carries the support status, the default consistency domain and the
  compatible collection roles.
- **Variant.** Every alias of a product profile is a variant pinned 1:1 to a
  storage `Profile`. A product slug selects its canonical variant; an alias
  selects the variant of the same name. `production_cp` has no alias, so its
  only variant is named after itself.
- **Preset.** The knob values of one variant: the variant columns below plus
  the values every preset shares and the product's domain, roles and floors.
- **Custom profile.** A spec version 2 block,
  `custom_profile { extends, description, knobs, floors, compatible_roles }`,
  that extends exactly one built-in slug or alias. Custom profiles cannot
  extend each other, so there are no chains or cycles.

`extends`, `resolve_named_profile` and suggestions normalise names: they trim,
lowercase and map `-` to `_`, so `High-Durability` means `high_durability`.
`spec.profile` without `custom_profile` stays an exact match, as before.

## Variants and presets

| variant | product (canonical marked *) | storage Profile | medium | layout | commit | group commit ms | cache | query memory | floors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| balanced | game_local_balanced* | Balanced | durable | row | immediate | 2 | on / 2048 | 1 GiB | - |
| in_memory | game_local_balanced | InMemory | volatile | row | immediate | 0 | off / 0 | 512 MiB | - |
| document | desktop_app_embedded* | Document | durable | row | immediate | 2 | on / 2048 | 1 GiB | - |
| vector | ai_agent_memory* | Vector | durable | row | immediate | 1 | on / 8192 | 2 GiB | - |
| transactional | secure_app* | Transactional | durable | row | immediate | 2 | on / 2048 | 1 GiB | - |
| high_durability | secure_app | HighDurability | durable | row | two_phase | 0 | on / 2048 | 768 MiB | - |
| production_cp | production_cp* | Transactional | durable | row | immediate | 2 | on / 2048 | 1 GiB | backup |
| analytical | analytics_columnar* | Analytical | durable | columnar | immediate | 0 | on / 4096 | 2 GiB | - |
| time_series | analytics_columnar | TimeSeries | durable | row | immediate | 2 | on / 2048 | 1 GiB | - |

Values every preset shares:

- compression `none`, `min_bytes` 256, `zstd_level` 3. Compression is a
  forward-only on-disk format change (value framing and `schema_version` 2),
  so no preset turns it on;
- query `max_concurrent_queries: auto` (four permits per available thread,
  today's value) and `target_partitions: auto` (the storage profile default);
  the other query limits are the runtime defaults: 1 MiB SQL, 32 statements,
  10 000 VALUES rows, parser recursion 128, batches of 1024 rows, queue
  timeout 100 ms, timeout 30 s and a 2 GiB spill limit;
- limits `unlimited` / `unlimited`, which is what the runtime enforces today;
- history `system_versioning: off`, `keep_history: true`;
- time series chunk 3 600 000 ms, retention `null`;
- `consistency.default_domain` and `compatible_roles` from the product profile.

A test pins the presets to the runtime: for every storage `Profile`, resolving
its alias gives `PerformanceConfig::for_profile` with only the compression
replaced by `none` / 256 / 3, the same table layout as `layout_for`,
two-phase commit exactly for HighDurability and a volatile medium exactly for
InMemory. Each variant's knob digest is pinned as well.

## Capability matrix

The storage `Profile` is derived from `storage.medium`, `storage.layout` and
`durability.commit`; the result is total and deterministic.

| medium | layout | commit | storage Profile |
| --- | --- | --- | --- |
| volatile | row | immediate | InMemory (requires group commit 0 ms) |
| durable | row | two_phase | HighDurability (requires group commit 0 ms) |
| durable | columnar | immediate | Analytical |
| durable | row | immediate | the variant's own class when it is Transactional, Document, Vector, TimeSeries or Balanced; otherwise the product's durable row class |

The durable row class of each product: game_local_balanced Balanced,
desktop_app_embedded Document, ai_agent_memory Vector, secure_app
Transactional, production_cp Transactional, analytics_columnar TimeSeries.

Every other combination is refused with `PROFILE_KNOB_COMBINATION_UNSUPPORTED`:
volatile with columnar layout, volatile with two-phase commit, volatile with a
group commit window above 0, two-phase commit with columnar layout, and
two-phase commit with a group commit window above 0.

## Knob reference

`KNOB_REGISTRY` is the single source of truth for knob paths, bounds and
behaviour. Paths are relative to `custom_profile.knobs` (floors live in
`custom_profile.floors`, roles in `custom_profile.compatible_roles`). Timing
says when a knob takes effect once a database is built from the resolved
profile.

| path | type and bounds | timing | runtime sink | enforcement | after creation |
| --- | --- | --- | --- | --- | --- |
| `storage.medium` | durable \| volatile | creation | `__meta.profile` -> `engine_for` (memory or redb engine) | enforced | unsupported |
| `storage.layout` | row \| columnar | creation | `__meta.layout` / `layout_for` (default layout for new tables) | enforced | unsupported |
| `durability.commit` | immediate \| two_phase | creation | redb two-phase commit (storage profile HighDurability) | enforced | unsupported |
| `durability.group_commit_window_ms` | 0..=50 | every open | `IoConfig.group_commit_window_ms` -> `SingleNode::with_group_commit_window` | enforced | retune |
| `compression.algorithm` | none \| lz4 \| zstd | creation | `CompressionConfig.algorithm` -> `CompressedEngine` value framing | enforced | format upgrade (none to lz4/zstd); lz4/zstd switch is a retune |
| `compression.min_bytes` | 64..=1048576 | every open | `CompressionConfig.min_bytes` | enforced | retune |
| `compression.zstd_level` | 1..=19 | every open | `CompressionConfig.zstd_level` | enforced | retune |
| `cache.enabled` | true \| false | every open | `CacheConfig.enabled` -> `PerformanceCache::new` | enforced | retune |
| `cache.max_entries` | 0..=1048576 | every open | `CacheConfig.max_capacity` -> `PerformanceCache::new` | enforced | retune |
| `query.max_concurrent_queries` | auto \| 1..=4096 | every open | query semaphore | enforced | retune |
| `query.queue_timeout_ms` | 1..=60000 | every open | `acquire_query_permit` | enforced | retune |
| `query.timeout_ms` | 100..=3600000 | every open | SQL execution timeout | enforced | retune |
| `query.memory_limit_bytes` | 64 MiB..=64 GiB | every open | DataFusion memory pool | enforced | retune |
| `query.spill_limit_bytes` | 0..=1 TiB | every open | DataFusion temp directory limit | enforced | retune |
| `query.target_partitions` | auto \| 1..=256 | every open | DataFusion target partitions (`ParallelismConfig.target_partitions`) | enforced | retune |
| `query.batch_rows` | 1..=1048576 | every open | DataFusion batch size | enforced | retune |
| `query.max_sql_bytes` | 1 KiB..=16 MiB | every open | SQL parser limits | enforced | retune |
| `query.max_statements` | 1..=1024 | every open | SQL parser limits | enforced | retune |
| `query.max_values_rows` | 1..=1000000 | every open | SQL parser limits | enforced | retune |
| `query.parser_recursion_limit` | 16..=1024 | every open | SQL parser limits | enforced | retune |
| `limits.max_value_bytes` | unlimited \| 1 KiB..=64 MiB | every open | `LimitPolicy.max_value_bytes` (no user write path checks it yet) | planned (#11 single write pipeline): only `unlimited` is accepted | provisioning |
| `limits.max_batch_ops` | unlimited \| 1..=1000000 | every open | `LimitPolicy.max_batch_ops` (no user write path checks it yet) | planned (#11 single write pipeline): only `unlimited` is accepted | provisioning |
| `history.system_versioning` | off \| typed_tables | provisioning | `enable_system_versioning` on typed tables that provisioning creates | declarative: recorded only until provisioning exists | provisioning |
| `history.keep_history` | true \| false | provisioning | `TemporalRetention.keep_history` of provisioned typed tables | declarative: recorded only until provisioning exists | provisioning |
| `time_series.default_chunk_millis` | 1000..=2678400000 | provisioning | `TimeSeriesConfig.chunk_millis` default of provisioned series | declarative: recorded only until provisioning exists | provisioning |
| `time_series.default_retention_millis` | null \| 1..=9223372036854775807, at least one chunk | provisioning | `TimeSeriesConfig.retention_millis` default of provisioned series | declarative: recorded only until provisioning exists | provisioning |
| `consistency.default_domain` | local_snapshot \| strong_cp \| eventual_ap | generation default | guided init, templates and domain advice | declarative | not applicable |
| `floors.audit` | true \| false, may only be raised | validation | `GUARANTEE_BELOW_PROFILE_FLOOR` (`guarantees.audit.enabled`) | enforced | not applicable |
| `floors.backup` | true \| false, may only be raised | validation | `GUARANTEE_BELOW_PROFILE_FLOOR`, or `PRODUCTION_BACKUP_DISABLED` for production_cp (`guarantees.backup.enabled`) | enforced | not applicable |
| `floors.pitr` | true \| false, may only be raised | validation | `GUARANTEE_BELOW_PROFILE_FLOOR` (`guarantees.backup.pitr`) | enforced | not applicable |
| `floors.encryption_at_rest` | true \| false, may only be raised | validation | `GUARANTEE_BELOW_PROFILE_FLOOR` (`guarantees.encryption.at_rest`) | enforced | not applicable |
| `compatible_roles` | a set of collection roles, changed with `add` / `remove` | validation | `ROLE_OUTSIDE_PROFILE_CERTIFICATION` | enforced | not applicable |

`auto` keeps the storage profile default; `null` retention keeps data forever.
`IoConfig.read_ahead`, `CacheConfig.scan_bypass_threshold_rows` and the
benchmark settings are deliberately not knobs, because nothing reads them at
runtime; they always come from `PerformanceConfig::for_profile`.

`multidb::config_spec::parse_knob_assignment("cache.max_entries=8192")`
parses one `path=value` against the registry: enum names, integers, `auto`,
`unlimited`, `true` / `false`, and `null` (inherit) for the retention. A
misspelled path gets a did-you-mean suggestion; ranges are checked by the
resolver. The CLI `--override` flag uses it, and `multidb profile knobs`
prints this registry.

## Reserved knobs

These names decode, so validation can explain them, but no runtime implements
them and any value is refused with `PROFILE_KNOB_NOT_SUPPORTED_BY_RUNTIME`:

| path | reason |
| --- | --- |
| `history.mvcc_retention_millis` | waits for MVCC and commit-log GC (#12) |
| `cache.ttl_millis` | no runtime implementation |

## Resolution and status rules

The resolver never touches a database. For a spec without `custom_profile`:

- a built-in slug or alias resolves to its variant; the status is the product
  status;
- an unknown name in a version 1 document stays `Unresolved`: validation keeps
  today's `CUSTOM_PROFILE` warning, and the resolver adds the Advice
  `PROFILE_NOT_PROVISIONABLE`;
- an unknown name in a version 2 document is the Error `PROFILE_UNKNOWN`,
  with a did-you-mean suggestion (for example `secure-app` suggests
  `secure_app`).

For a custom profile (version 2 only; a version 1 document that sets
`custom_profile` fails with `FIELD_REQUIRES_SPEC_V2` at `custom_profile`):

| code | severity | rule |
| --- | --- | --- |
| `CUSTOM_PROFILE_SLUG_INVALID` | Error | `profile` must match `^[a-z][a-z0-9_]{2,63}$` |
| `CUSTOM_PROFILE_SHADOWS_BUILTIN` | Error | `profile` equals a built-in slug or alias after normalisation |
| `CUSTOM_PROFILE_DESCRIPTION_TOO_LONG` | Error | `description` is longer than 500 characters |
| `CUSTOM_PROFILE_EXTENDS_UNKNOWN` | Error | `extends` is not a built-in slug or alias (with did-you-mean) |
| `PROFILE_KNOB_OUT_OF_RANGE` | Error | a knob is outside its registry bounds |
| `PROFILE_KNOB_COMBINATION_UNSUPPORTED` | Error | the capability matrix refuses the knobs |
| `PROFILE_KNOB_CONFLICT` | Error | cache enabled with 0 entries; retention shorter than one chunk; a role in both `add` and `remove` |
| `PROFILE_KNOB_NOT_SUPPORTED_BY_RUNTIME` | Error | a reserved knob is set |
| `PROFILE_KNOB_NOT_ENFORCED` | Error | a `limits.*` value other than `unlimited` |
| `PROFILE_FLOOR_WEAKENED` | Error | a floor is lowered below the extended profile |
| `PROFILE_DEPLOYMENT_MISMATCH` | Error in version 2, Warning in version 1 | a volatile medium with a `deployment.storage_path` |
| `PROFILE_KNOB_IRRELEVANT` | Advice | `zstd_level` without zstd, or `min_bytes` without compression |
| `PROFILE_FORMAT_REQUIRES_NEW_BINARY` | Advice | compression means value framing and schema_version 2 |
| `CUSTOM_PROFILE_DEFINED` | Advice | the status is Custom; lists the overridden paths |

A `null` path is never volatile: the embedding application chooses the path.
Volatility comes only from the `in_memory` variant or `storage.medium:
volatile`.

Every resolved profile records the provenance of each knob: `variant` for the
variant columns (`storage.*`, `durability.*`, `cache.*`,
`query.memory_limit_bytes` and the floors), `preset` for everything else, and
`override` for leaves a custom profile sets. Its digest is `prof_` followed by
32 hex characters of blake3 over the knobs and the storage `Profile`, so equal
knobs always give the same digest on every machine.

## Spec version 2 and `custom_profile`

A version 2 `DatabaseSpec` is a version 1 spec with `version: 2` and an
optional `custom_profile` block. The required fields are the same, and nothing
else changes meaning. `examples/custom-profile/multidb.yaml` is a complete
example:

```yaml
version: 2
name: orders
profile: orders_durable
custom_profile:
  extends: high_durability          # alias -> secure_app with two-phase commit
  description: Durable order store with zstd and a larger cache.
  knobs:
    compression: { algorithm: zstd, zstd_level: 3, min_bytes: 512 }
    cache: { max_entries: 8192 }
    query: { max_concurrent_queries: auto, timeout_ms: 10000 }
    history: { system_versioning: typed_tables, keep_history: true }
  floors: { audit: true, backup: true }
  compatible_roles: { add: [time_series] }
deployment: { mode: single_node, storage_path: orders.redb }
# defaults, guarantees, domains, collections, extensions, overrides and
# operation_hints exactly as in version 1
```

It validates as `valid=true status=Custom`, with storage class
HighDurability. Provenance marks compression, cache, query, history,
`floors.audit`, `floors.backup` and `compatible_roles` as `override` and the
other leaves as `variant` or `preset`.

- **`profile`** is the custom slug. It must match `^[a-z][a-z0-9_]{2,63}$` and
  must not equal a built-in slug or alias after normalisation.
- **`extends`** names one built-in product slug (its canonical variant) or
  alias (that variant), in snake or kebab case. Custom profiles cannot extend
  each other.
- **`knobs`** overrides leaves of the knob reference above. Absent leaves
  inherit the extended variant; an unknown leaf fails to decode and names its
  path, for example `unknown field max_entires`.
- **`floors`** may only raise the floors of the extended profile
  (`PROFILE_FLOOR_WEAKENED` otherwise). Validation checks each floor against
  its guarantee: `audit` against `guarantees.audit.enabled`, `backup` against
  `guarantees.backup.enabled`, `pitr` against `guarantees.backup.pitr` and
  `encryption_at_rest` against `guarantees.encryption.at_rest`. An unmet floor
  is the Error `GUARANTEE_BELOW_PROFILE_FLOOR`; profiles of production_cp keep
  the historic `PRODUCTION_BACKUP_DISABLED` for the backup floor.
- **`compatible_roles`** adds and removes collection roles; collections with
  other roles get `ROLE_OUTSIDE_PROFILE_CERTIFICATION`, as for built-ins.

### Version 1 and version 2 severities

New checks never make a version 1 document that decoded before invalid:

| check | version 1 | version 2 |
| --- | --- | --- |
| unknown profile name | `CUSTOM_PROFILE` Warning (unchanged) plus `PROFILE_NOT_PROVISIONABLE` Advice | `PROFILE_UNKNOWN` Error with a did-you-mean |
| volatile medium with a `storage_path` | `PROFILE_DEPLOYMENT_MISMATCH` Warning | `PROFILE_DEPLOYMENT_MISMATCH` Error |
| knob-like `overrides` keys | `OVERRIDE_IS_METADATA_ONLY` Advice | `OVERRIDE_SHADOWS_TYPED_KNOB` Error |
| `custom_profile` | `FIELD_REQUIRES_SPEC_V2` Error | resolved (codes above) |

Version 1 issues keep their order: `PROFILE_NOT_PROVISIONABLE` follows
`CUSTOM_PROFILE`, and other new version 1 issues come last.

`overrides` and `operation_hints` stay metadata only. An `overrides` key in a
knob namespace (`profile.`, `storage.`, `cache.`, `query.`, `limits.`,
`compression.`, `durability.`) is flagged, because nothing at runtime reads
it: the suggestion names the typed knob (for example
`custom_profile.knobs.query.timeout_ms`), or `multidb config upgrade` for the
Studio `profile.custom.*` convention. `operation_hints` are never flagged.

### Spec validation codes

These codes come from reading and validating a whole spec; the custom profile
codes are in the table above.

| code | severity | rule |
| --- | --- | --- |
| `SPEC_TOO_LARGE` | Error | the document is larger than 1 MiB |
| `SPEC_TOO_COMPLEX` | Error | YAML anchors and aliases expand beyond the node or scalar-byte budget |
| `MISSING_VERSION` | Error | no integer `version` at the root of a mapping |
| `UNSUPPORTED_VERSION` | Error | `version` is not 1 or 2 |
| `FIELD_REQUIRES_SPEC_V2` | Error | a version 2 field (`custom_profile`) in a version 1 document |
| `PROFILE_UNKNOWN` | Error (version 2) | `profile` is no built-in slug or alias and there is no `custom_profile` |
| `PROFILE_NOT_PROVISIONABLE` | Advice (version 1) | the same, in a version 1 document; follows `CUSTOM_PROFILE` |
| `GUARANTEE_BELOW_PROFILE_FLOOR` | Error | a resolved floor is not met by its guarantee |
| `OVERRIDE_IS_METADATA_ONLY` | Advice (version 1) | an `overrides` key in a knob namespace |
| `OVERRIDE_SHADOWS_TYPED_KNOB` | Error (version 2) | the same, in a version 2 document |

### Reading specs: size, version and YAML limits

The CLI (`config validate`, `config explain`, `explain config`, `config plan`)
and the Control Plane read every spec through `multidb::config_spec::spec_io`:

- a spec is at most 1 MiB (`MAX_SPEC_BYTES`, the Control Plane body limit);
  larger files are refused before parsing with `SPEC_TOO_LARGE`;
- the version is read before the typed decode. A missing, non-integer or
  non-mapping version is `MISSING_VERSION`; a version other than 1 or 2 is
  `UNSUPPORTED_VERSION`, reported verbatim (`-1`, `3`);
- YAML anchors and aliases are bounded. The version peek walks the document
  without building it and expands aliases exactly as the typed decode does,
  counting at most 2 097 152 nodes (`MAX_SPEC_NODES`) and 2 097 152 scalar
  bytes (`MAX_SPEC_EXPANDED_BYTES`); beyond either budget the spec is
  `SPEC_TOO_COMPLEX`. No alias-free spec of 1 MiB can reach these budgets.
  The YAML parser's own repetition limit still applies;
- the typed decode then reads the original text, so syntax and unknown-field
  errors keep their line and column.

Size, complexity and version problems are validation issues: the CLI exits 2
(with `--json`, the report is printed as JSON) and the Control Plane answers
422. `/config/validate` returns the report as `data`; `/config/plan` cannot
build a plan from such a spec and answers with an error envelope whose code is
`invalid_spec_version`, so the OpenAPI snapshot documents its 422 response as
a success or an error envelope (`JsonObjectOrErrorEnvelope`). Syntax errors
and decode errors keep exit code 1 and HTTP 400 `invalid_json`.

### Compiled policy, explain and plans

- `CompiledPolicy.resolved_profile` carries the resolved profile. It is
  absent only for a version 1 profile string that is no built-in slug or
  alias. `storage_profile` keeps its meaning (the product or custom slug).
- `CompiledPolicy.runtime_limits` keeps its values. It is advisory catalog
  guidance that this build does not enforce; the effective values are the
  knobs.
- `config explain` adds `$.profile.storage_profile` (the derived storage class
  and its engine), one `$.profile.knobs.<path>` decision per knob with its
  value, its source (`preset:<product>`, `variant:<variant>` or `override`)
  and what takes effect when, and `$.profile.not_configurable.*` for what a
  spec cannot set: MVCC and commit-log retention, cache TTL and eviction,
  backup schedules, runtime write acknowledgement and conflict knobs, and a
  per-collection consistency domain. Every-open knobs are applied only to
  databases created with `db::create_database_with_profile`; explain says so,
  and that the CLI, `serve` and the Control Plane do not create databases
  from specs in this build.
- `config plan` reports a `custom_profile` change as an unsupported
  `change_profile` step at `$.custom_profile`, and a change of `version` alone
  as one supported no-op step at `$.version`. Version 1 pairs plan exactly as
  before, with the same plan ids.

### JSON schema

`docs/schemas/database-spec-v2.schema.json` (`database_spec_v2_schema()`) is
the version 1 schema with `version: 2` and `custom_profile`. Its knob bounds,
role enum and index enum are generated from `KNOB_REGISTRY` and the Rust
enums; `auto` and `unlimited` are `oneOf` a constant or an integer range.
`docs/schemas/database-spec-v1.schema.json` is unchanged.

## Persistence and format

`ResolvedProfile::bind(replication, path)` turns a resolved profile into a
`db::RuntimeProfileBinding`: the `DbConfig` (storage class, replication and
path), the `PerformanceConfig` the knobs produce and the
`PersistedProfileManifest`. It keeps the `DbConfig` path rules: a volatile
class takes no path, a durable class needs one. For a compiled spec,
`CompiledPolicy::binding(&deployment, path)` does the same with the path
argument or else `deployment.storage_path`, under the same rules: a volatile
class with either path is `VolatileWithPath`, even for a version 1 spec, where
validation only warns (`PROFILE_DEPLOYMENT_MISMATCH`). It refuses an
unresolved profile, `deployment.mode: cluster` and any topology other than one
replica and one shard with `Unsupported`.

`db::create_database_with_profile(&binding, ops)` creates a new single-node
database from a binding:

- it never opens an existing database: a target that already holds metadata
  is `AlreadyInitialized`;
- the profile owns performance, so `OperationalConfig.performance` must be
  unset (`Unsupported` otherwise); the other operational settings apply as in
  `create_database_with_ops`, including writing `ops.security`;
- `schema_version`, the storage profile, replication, layout and
  `__meta.profile_manifest` are written in one write transaction.

The manifest (`manifest_version: 1`) records the profile slug, its kind, the
product profile, the variant, `extends`, the storage profile, every knob, the
storage format (`value_framing` and the compression settings), the digest and
the crate version that wrote it. It is re-validated on every read: its storage
profile must equal `__meta.profile`, every knob must be within its registry
bounds, the knobs must pass the resolver's conflict checks and leave the
planned `limits.*` knobs `unlimited`, the knobs must derive the recorded
storage class under the recorded variant, and the storage format must agree
with the compression knobs. A failure is `ProfileManifestInvalid`, and a
newer `manifest_version` is `UnsupportedProfileManifest`, so a corrupt or
hand-edited manifest never drives the runtime.

Every single-node opener (`create_database`, `open_database`,
`create_database_with_ops`, `open_database_with_ops`) re-applies the manifest:
the runtime performance is rebuilt from the knobs at every open, so cache,
query and group-commit knobs survive a restart and a tuning rollback returns
to the manifest values. `multidb serve` and the CLI open databases through
these openers, so they honour the manifest too; their `--profile` or
`MULTIDB_PROFILE` must name the database's storage class, or be `auto` to read
it from the database (see [CLI](#cli)). An explicit
`OperationalConfig.performance` wins for that session and logs one warning.
No opener writes security because of a manifest. Databases without a manifest
open exactly as before; plain `create_database` and `serve` never write one.

### Value framing and `schema_version`

- Compression other than `none` turns on value framing: every value,
  `__meta` included, carries a compression frame (`MDBC1`), and the database
  gets `schema_version` 2. Without compression it keeps `schema_version` 1 and
  the unframed format. `schema_version` 2 without a value-framing manifest,
  and a value-framing manifest without `schema_version` 2, are
  `ProfileManifestInvalid`; anything above 2 is `SchemaVersionMismatch`.
- A plain opener wraps the engine of a framed manifest database in place.
  `OperationalConfig.performance.compression` may name another algorithm or
  level; frames are self-describing, so every value stays readable.
- Compression is forward-only. Older binaries fail closed on a framed
  database: they report `SchemaVersionMismatch`, or, because `__meta` itself
  is framed, fail to decode the metadata. Turning framing off is never
  possible. A manifest database created without compression refuses
  `OperationalConfig.performance.compression` with `StorageFormatMismatch`;
  enabling compression later is a provisioning format upgrade, which this
  build does not perform.
- Downgrades of a manifest database without compression do not fail closed:
  it keeps `schema_version` 1, so an older binary opens it, ignores
  `__meta.profile_manifest` and runs with the storage profile defaults
  (`PerformanceConfig::for_profile`). Every knob, including query timeouts,
  `max_sql_bytes`, `max_concurrent_queries` and the cache size, reverts until
  a binary that reads manifests opens the database again.
- Metadata reads tolerate frames. A legacy database (no manifest) created with
  `create_database_with_ops` and compression has framed values and must still
  be opened with `OperationalConfig.performance.compression`; any opener
  without it fails with `StorageFormatMismatch` (an opaque serde error
  before).
- Encrypted metadata read without a key is `MetadataUnreadable`, whose
  message ends with "metadata is encrypted or corrupt; supply
  OperationalConfig.encryption". A wrong key is still storage corruption.
- Known limit: a legacy database created without compression and later opened
  with `OperationalConfig.performance.compression` has unframed metadata but
  framed data. `__meta` cannot reveal this, so such a database behaves as
  before and must keep being opened with compression.

### Backup and restore

Backups read values through the database engine, so a backup of a framed
manifest database holds decoded values, and its `BackupManifest.profile`
stays the storage `Profile`. Restore looks for `__meta.profile_manifest` in
the full backup payload before it creates anything:

- With a manifest, the target is built the way `create_database_with_profile`
  builds it: when the manifest records value framing, the engine frames values
  with the manifest compression, so every restored value, `__meta` included,
  is framed again and the restored database opens with its manifest
  performance. The restored `__meta` records (`schema_version`, the manifest)
  are copied verbatim, as before.
- A restore `DbConfig` whose storage profile disagrees with the backed-up
  manifest fails with `BackupError::Config(ProfileMismatch)`. A backed-up
  manifest that does not decode or fails the read-time validation is
  `BackupError::Corruption`, and one with a newer `manifest_version` is
  `BackupError::Config(UnsupportedProfileManifest)`. In all three cases the
  target is left absent or empty.
- Without a manifest, restore behaves as before.

`verify_backup` restores into a temporary database the same way, so it
works for compressed manifest databases. A legacy database created with
`OperationalConfig.performance.compression` has no manifest; it is restored
unframed, as before.

### Single node only

CP, AP and sharded openers refuse a profile manifest with
`Unsupported("profile manifests are single-node only in this build")`. A
sharded database refuses a manifest on every local shard, not only on the
global one. These openers also refuse framed legacy metadata read without
compression (`StorageFormatMismatch`).

### Inspecting a database

`db::inspect_database_metadata(path)` reads the metadata of a database file
without creating anything:

| result | meaning |
| --- | --- |
| `NotFound` | nothing exists at the path; nothing is opened or created |
| `Uninitialized` | an empty file, or a redb file without database metadata |
| `Locked` | another handle holds the database open |
| `Unreadable { reason }` | the file or its metadata cannot be read, including encrypted metadata (with the hint above) and invalid manifests |
| `Initialized` | `schema_version`, storage profile, replication, layout, the manifest (absent for legacy databases) and whether values are framed |

It opens an existing file with redb, which may run redb's own crash repair
exactly as a normal open would, and drops the handle before returning.

## CLI

`multidb profile` inspects profiles without touching any database:

| command | output | exit code |
| --- | --- | --- |
| `profile list [--json]` | the six product profiles; JSON adds `variants` (`name`, `storage_profile`, `canonical`) and the canonical variant's `knobs`, text adds `variants=` | 0 |
| `profile show <profile> [--override path=value]... [--json]` | the resolved profile: a header naming the product, variant, status and storage profile, the digest, and one row per knob with its value, source (`preset`, `variant`, `override`), timing, enforcement and sink | 0, or 2 when the profile is invalid |
| `profile show --db <path> [--json]` | the profile manifest of the database, or `legacy database: storage profile X, no manifest`; JSON prints the `inspect_database_metadata` result | 0, or 1 when nothing, an uninitialized file, a locked or an unreadable database is found |
| `profile explain --spec <file> [--json]` | `resolve_profile` for a spec read through `spec_io`; JSON has `resolution` (`resolved`, `unresolved`, `invalid`), `profile`, `rows` and `issues` | 0 for resolved and version 1 unresolved profiles, 2 when invalid (including `FIELD_REQUIRES_SPEC_V2` for a version 1 spec that sets `custom_profile`) or when the spec cannot be read (`SPEC_TOO_LARGE`, `UNSUPPORTED_VERSION`, ...) |
| `profile knobs [--json]` | every `KNOB_REGISTRY` entry, then the reserved knobs marked `reserved` | 0 |

`--override path=value` may be repeated or hold a comma-separated list. It
accepts every registry path, including `floors.*`; `compatible_roles` is a
set and is refused with a hint to edit `custom_profile.compatible_roles` in a
spec, and reserved knobs are refused as not supported by this runtime. A
misspelled path is a usage error with a did-you-mean suggestion. With
overrides, `profile show` resolves the what-if custom profile
`<profile>_custom` that extends `<profile>`.

`multidb init --guided --profile <profile> --name <name>` writes the same
version 1 spec as before. With `--override path=value`, it writes a version 2
spec instead: `custom_profile` extends the normalised `<profile>` with the
given knobs and floors, and `profile` is `--profile-name <slug>` or
`<profile>_custom`. Raised floors also raise the matching guarantees
(`floors.pitr` raises `backup.enabled` and `backup.pitr`), a volatile
`storage.medium` gives an embedded deployment without a storage path, and
`consistency.default_domain` sets the domain mode and replication.
`--profile-name` without `--override` is a usage error. Without `--override`,
an alias whose variant is not the product's canonical one, such as
`high_durability` or `time-series`, gets a warning, because a version 1 spec
records the product slug and so resolves to the canonical variant; `--json`
adds `warnings`, `variant` and `storage_profile` to the summary object.

`multidb config upgrade --spec <in> [--out <file>] [--force] [--json]`
converts a spec to version 2:

- it sets `version: 2`;
- Studio's convention, `overrides["profile.custom.base"]`,
  `overrides["profile.custom.description"]` and
  `operation_hints["profile.custom.slug"]`, becomes
  `custom_profile { extends, description }` with the slug as `profile`, and
  those keys are removed; every other key, `studio.builder` included, stays;
- it is deterministic and idempotent: upgrading the output again gives the
  same bytes;
- `--out` gets the format its extension names (`.yaml`/`.yml` is YAML,
  anything else JSON); without `--out` the output keeps the input's format
  and goes to stdout (notes and an invalid validation report go to stderr
  then); `--json` prints `{spec, notes, validation}`, plus `path` and
  `written` with `--out`;
- it refuses to overwrite an existing file, the input included, without
  `--force`, and replaces files by writing a sibling file and renaming it;
- the result is validated: exit 2 when it is invalid, for example
  `PROFILE_UNKNOWN` for a version 1 profile that is not built in and has no
  `profile.custom.base`, and an invalid result is never written to `--out`,
  so `--force` cannot replace a valid input with it.

It re-serializes the spec, so YAML comments and formatting are not kept and
defaulted fields such as `topology` are written out. Knob-like `overrides`
keys are not converted; version 2 validation reports them as
`OVERRIDE_SHADOWS_TYPED_KNOB` with the `custom_profile.knobs` path to use.

Every `--profile` flag (`backup`, `restore`, `advice`, `cdc`, `view`,
`import`, `export`, `migrate`, `admin`) and `MULTIDB_PROFILE` for
`multidb serve` accept:

- the storage profile spellings accepted before (`InMemory`, `in-memory`,
  `memory`, `Transactional`, `transactional`, ... `Balanced`, `balanced`),
  which keep their meaning;
- a product slug, which selects its canonical variant's storage profile, or
  an alias, which selects its variant's, in snake or kebab case;
- `auto`, which reads the storage profile and replication of the existing
  database with `inspect_database_metadata`. It is a usage error when nothing
  exists at the path, the file holds no database metadata, the database is
  locked by another process or its metadata cannot be read. `restore` refuses
  `auto`, because its target must be absent or empty.

`multidb serve` defaults to `MULTIDB_PROFILE=transactional`, so it fails with
`ProfileMismatch` on a database of another storage class, such as a manifest
database built with a non-default profile; set `MULTIDB_PROFILE=auto` or the
exact profile.

## Control Plane and Studio

`GET /profiles` lists the six product profiles. Each entry carries
`variants` (`name`, `storage_profile`, `canonical`; exactly one variant is
canonical) and `knobs`, the fully resolved `ProfileKnobs` of the canonical
variant. Its knob sections use the same leaf names and value forms as
`custom_profile.knobs`, but it also holds `floors` and `compatible_roles` (the
full role set), which a spec sets through `custom_profile.floors` and
`custom_profile.compatible_roles.add`/`remove`, not inside
`custom_profile.knobs`. `/config/validate` and `/config/plan` accept version
1 and version 2 specs (see
[Reading specs](#reading-specs-size-version-and-yaml-limits)). No Control
Plane endpoint creates a database from a spec in this build.

The Studio custom-profile builder writes a version 2 spec: `profile` is the
custom slug and `custom_profile` extends a product slug or a variant, with
optional `durability.commit`, `storage.layout`, `compression.algorithm` and
`cache.max_entries` knobs (an empty field inherits the base value). A base
whose variant is in memory gives an embedded deployment without a storage
path. Studio validates and dry-run plans the spec; it does not create
databases from it. Specs saved by older Studio builds carry
`overrides["profile.custom.*"]` keys instead; `multidb config upgrade`
converts them.
