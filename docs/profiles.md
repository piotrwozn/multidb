# Profiles

A profile describes how a database is stored and tuned. This page covers the
typed profile model: product profiles, variants, presets, custom profiles, the
knob registry and the resolver that turns a `DatabaseSpec` into concrete knobs.

Status in this build: the model, the knob registry and the resolver are a Rust
API in `multidb::config_spec` (`resolve_profile`, `resolve_named_profile`,
`ResolvedProfile::performance_config`, `KNOB_REGISTRY`,
`parse_knob_assignment`). Spec validation accepts only version 1 documents,
and a version 1 document that sets `custom_profile` fails with
`FIELD_REQUIRES_SPEC_V2`. Validation, explain, the CLI, `serve` and the
Control Plane do not consume resolved knobs yet, so existing version 1 specs
validate, explain and plan exactly as before.

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

- compression `none`, `min_bytes` 256, `zstd_level` 3. Plain openers never
  compress today, so a compressed preset would silently change the on-disk
  format;
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
| `compatible_roles` | role set, changed with `add` / `remove` | validation | `ROLE_OUTSIDE_PROFILE_CERTIFICATION` | enforced | not applicable |

`auto` keeps the storage profile default; `null` retention keeps data forever.
`IoConfig.read_ahead`, `CacheConfig.scan_bypass_threshold_rows` and the
benchmark settings are deliberately not knobs, because nothing reads them at
runtime; they always come from `PerformanceConfig::for_profile`.

`multidb::config_spec::parse_knob_assignment("cache.max_entries=8192")`
parses one `path=value` against the registry: enum names, integers, `auto`,
`unlimited`, `true` / `false`, and `null` (inherit) for the retention. A
misspelled path gets a did-you-mean suggestion; ranges are checked by the
resolver.

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

For a custom profile (version 2 only):

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
