# Provisioning

Status: **Preview**. Provisioning builds and converges a database from a
`DatabaseSpec`. This page grows with it: this build has the spec version 2
provisioning fields, their validation, the physical projection
(`multidb::provision::physical::project`), per-table layout
(`Database::create_table_with_layout`), read-only planning, drift and export
(`multidb::provision::{plan_provision, drift, export_spec}`), applying a
plan through the Rust API (`multidb::provision::{apply_provision,
provision_new_database}`, see [Applying plans](#applying-plans)) and the CLI
commands `config plan|apply|drift --spec`, `config status`, `config export`
and `init --apply` (see [CLI](#cli)), and starting from a spec: `multidb
serve` in spec mode and `multidb::provision::open_from_spec` for embedded
apps (see [Serve](#serve)), and the Control Plane endpoints `/provision/*` and
`GET /config/live`, which plan, apply and check the database a running server
serves (see [Control Plane](#control-plane)). `config apply --plan` and
`POST /config/apply` stay audit-only.

## Spec version 2 provisioning fields

A version 2 spec (`version: 2`, see [profiles.md](profiles.md)) may carry the
parameters provisioning needs:

| field | what it describes |
| --- | --- |
| `collections[].physical` | how a collection is built: its kind, fields, indexes, columns, vector dimension, chunk width and pinned ids |
| `collections[].indexes: [geo]` | a geo index kind; needs `physical.geo` |
| `security` | roles and principals that provisioning manages |
| `provisioning` | what a plan does with objects the spec does not declare |

Each is a version 2 field: a version 1 document that sets one fails validation
with `FIELD_REQUIRES_SPEC_V2` at its path (`security`, `provisioning`,
`collections[i].physical`, `collections[i].indexes[j]` for `geo`). Version 1
documents without them validate, serialize and plan exactly as before.

```yaml
version: 2
name: orders
profile: orders_durable
custom_profile: { extends: high_durability, knobs: { cache: { max_entries: 8192 } } }
deployment: { mode: single_node, storage_path: orders.redb }
# defaults, guarantees, domains, extensions, overrides and operation_hints as in version 1
collections:
  - name: orders
    role: document_entity
    domain: primary
    indexes: [primary, document, full_text, geo]
    physical:
      fields: [ { name: customer_id, path: customer.id, type: str }, { name: total, path: total, type: float } ]
      document_indexes: [ { path: customer.id }, { path: status } ]
      full_text: [ { path: notes } ]                         # -> orders_notes_fts
      geo: [ { path: shipping.location, precision: 7 } ]     # -> orders_shipping_location_geo
  - name: ledger
    role: event_log
    domain: primary
    indexes: [primary, document]
    physical:
      kind: table
      columns: [ { name: entry_id, type: int }, { name: order_id, type: str }, { name: amount, type: float } ]
      primary_key: entry_id
      table_indexes: [ { column: order_id } ]
      system_versioning: true                                # -> ledger_history
  - name: memories
    role: vector_memory
    domain: primary
    indexes: [vector]
    physical: { dim: 1536, metric: cosine }
  - name: daily_rollups
    role: analytics
    domain: primary
    indexes: [columnar]
    physical: { columns: [ { name: id, type: str }, { name: day, type: str }, { name: revenue, type: float } ], primary_key: id }
security:
  roles:
    - name: order_writer
      grants:
        - { object: orders, permissions: [read, write] }
        - { object: orders_notes_fts, permissions: [read] }
        - { object: ledger, permissions: [read, write] }
  principals: [ { user: orders_svc, roles: [order_writer] } ]
provisioning: { unmanaged: report }
```

## Roles, kinds and runtime objects

Every collection becomes exactly one container object. Its kind is
`physical.kind`, or the first kind its role allows:

| role | allowed kinds (default first) | notes |
| --- | --- | --- |
| `document_entity`, `key_value`, `event_log`, `cache`, `audit` | `document`, `table` | cache TTL/eviction and append-only event logs are not enforced; `audit` is an application collection, never the system audit log |
| `vector_memory` | `vector` | `physical.dim` is required |
| `graph` | `graph` | Experimental |
| `analytics` | `table` | columnar unless `physical.layout` says otherwise |
| `time_series` | `time_series` | chunk and retention default to the profile `time_series` knobs |

| kind | runtime object | parameters (defaults) |
| --- | --- | --- |
| `document` | `create_collection`, or `create_collection_auto_id` without a pinned id | `collection_id`; `fields` (`{name, path, type}`, empty means one field `id` holding the document id); `document_indexes` (`{path, id}`) |
| `table` | `Database::create_table_with_layout` | `layout` (analytics: columnar; otherwise the database layout); `columns` (`{name, type, nullable: false}`; empty means a schemaless row table); `primary_key`; `table_indexes` (`{column, expression: column \| lower_ascii, include, id}`); `system_versioning`, `keep_history` |
| `vector` | `create_vector_collection`, or `_auto_id` | `collection_id`; `dim` (1..=65536, required); `metric` (`cosine` \| `l2`, cosine); `hnsw` (`m` 1..=255, `ef_construction` >= m, `ef_search` >= 1; 16/200/48) |
| `time_series` | `create_time_series` | `chunk_millis` (1000..=2678400000; `time_series.default_chunk_millis`, 3600000 in every preset); `retention_millis` (> 0 and at least one chunk; `time_series.default_retention_millis`, none in every preset; left out it takes the knob, and an explicit `null` is refused when the spec is decoded, since it could mean either the knob or no retention, so a series cannot ask for no retention when its profile sets a default retention); `max_future_skew_millis` (at least 0; 300000, the runtime default for new series, when left out; an explicit `null` means no bound): how far ahead of the server clock a point's timestamp may lie, later points are refused |
| `graph` | `create_graph`, or `create_graph_auto_id` | `graph_id` |

A document collection also derives:

- one full-text index per `physical.full_text` entry (`{path, name,
  language: simple, refresh_lag_target: 1}`), built with
  `create_full_text_index` on the collection;
- one geo index per `physical.geo` entry (`{path, name, precision: 6}`,
  precision 1..=12), built with `create_geo_index`.

Creating either index backfills the documents its source collection already
holds, and document writes through collection handles, multi-model
transactions and Control Plane document CRUD keep it current. Raw key writes
that bypass those paths need `refresh_incremental` or `refresh_full`. A plan
adopts a matching index that already exists as it is, so an index created
empty by a build without backfill needs one `refresh_full()`.

A typed row table with system versioning derives `<table>_history`
(`enable_system_versioning` with `keep_history`). Versioning comes from
`physical.system_versioning`, or else from the profile knob
`history.system_versioning: typed_tables`, which applies to every typed row
table; `keep_history` defaults to `history.keep_history`.

Field types are `int`, `float`, `str`, `bool` and `bytes`. The full-text
`language` accepts only `simple`, because this build tokenizes every language
the same way.

Time-series parameters follow the runtime's rules, and a breach is
`TIME_SERIES_PARAMS_INVALID`: `retention_millis` must be positive and at
least one chunk, and `max_future_skew_millis` at least 0. All three are fixed
at creation and compared on every plan, the future-skew bound included, so a
series whose bound differs from the spec (also from the 300000 default when
the spec leaves it out) is an object conflict. A series created before the
runtime bounded future timestamps accepts any of them: the runtime reads its
bound back as `i64::MAX`. `export_spec` (and so `config export` and
`GET /config/live`) writes that bound as `max_future_skew_millis: null`, with
a note, and plans show it as `null` in object parameters and field diffs,
because JavaScript clients such as the SDK and Studio cannot represent
`i64::MAX` and would send back a rounded value that no longer fits an `i64`.
Plan and apply read `null` as no bound, so the exported spec adopts the
series, and a series built from it accepts any future timestamp too. A spec
that spells the bound out as `9223372036854775807` (`i64::MAX`, as specs
exported by earlier builds did) is read as `null`, the same spec with the
same digest, and every spec, plan, apply report, provision record and drift
report writes no bound as `null`, never as a number.

These defaults are pinned by tests: a later build projects the same objects from
the same spec, so upgrading never creates false drift.

### Index kinds are requirements

The `indexes` list says what a collection must have; `physical` says how to
build it.

- `document` needs at least one `document_indexes` entry (document kind) or
  `table_indexes` entry (row table).
- `full_text` and `geo` need at least one `physical.full_text` or
  `physical.geo` entry, on a document collection only
  (`INDEX_SOURCE_NOT_DOCUMENT` otherwise).
- `vector`, `graph` and `time_series` need that kind; `columnar` needs a table
  whose effective layout is columnar.
- `primary` is implicit.

An index kind without its parameters is `PHYSICAL_PARAMETER_REQUIRED`: Advice
when a version 2 spec is validated, and blocking for provisioning, whatever the
spec version. Its suggestion names the field to add; `multidb config upgrade`
converts a version 1 spec. Parameters given without their index kind are
`PROVISION_INDEX_KIND_MISSING`. A field set on a kind it does not apply to, or
an index kind the object cannot have, is `PHYSICAL_FIELD_NOT_APPLICABLE`.

So a version 1 spec can be provisioned only when everything can be derived
from it, for example collections with only `primary` indexes. The version 1
specs the built-in templates emitted before this release (frozen under
`tests/fixtures/spec-v1`) are blocked: their `document`, `vector` and
`columnar` index kinds carry no paths, dimension or columns. The built-in
templates now emit version 2 specs with those parameters (see
[Templates](#templates)).

### Names, paths and ids

- **Derived names.** `<collection>_<path with . replaced by _>_fts`,
  `<collection>_<path with . replaced by _>_geo` and `<table>_history`. An
  explicit `name` overrides the derived one.
- **One namespace.** Collections, full-text and geo indexes and history tables
  share the catalog namespace (`OBJECT_NAME_COLLISION`).
- **Catalog rule.** Every name is an ASCII letter or `_`, then letters, digits
  or `_`, at most 128 bytes (`COLLECTION_NAME_INVALID`), and does not start
  with `__`, which is reserved for system tables
  (`COLLECTION_NAME_RESERVED`). Derived names are checked too, so a long path
  cannot produce a name the runtime refuses. Field and column names follow the
  same rule (`PHYSICAL_NAME_INVALID`) and are unique per object
  (`TABLE_COLUMN_DUPLICATE`).
- **Paths.** 1 to 16 dotted segments, each `[A-Za-z_][A-Za-z0-9_]*`
  (`PROVISION_PATH_INVALID`). `$id`, the 16-byte document id, is allowed for
  `physical.fields` only, with type `bytes`.
- **Ids.** Document and table index ids default to their position + 1. Pinned
  `collection_id` values are unique across document and vector collections,
  pinned `graph_id` values across graphs, and index ids within their object
  (`ID_CONFLICT`). Unpinned collection and graph ids are allocated when the
  object is created.

### Columnar tables

`Database::create_table_with_layout` stores the layout with the table
(`CatalogEntry::Table.layout`), so a columnar table can live in a row-class
database and keeps its layout across reopens; SQL reads and writes it like any
table. A table whose effective layout is columnar:

- needs `columns` and `primary_key` (`PHYSICAL_PARAMETER_REQUIRED`);
- takes no B-tree indexes: `table_indexes` and the `document` index kind are
  `PHYSICAL_FIELD_NOT_APPLICABLE`;
- keeps no system-versioned history (history is kept for row tables only), so
  `system_versioning: true` is `PHYSICAL_FIELD_NOT_APPLICABLE`.

Schemaless tables are row tables. System versioning or `keep_history` on a
table without `columns` is `PHYSICAL_PARAMETER_REQUIRED`.

## Security

`security` lists the roles and principals provisioning manages; roles and
principals the spec does not name are left alone.

- **Roles** are named `^[a-z][a-z0-9_]{0,63}$` (`SECURITY_NAME_INVALID`). Each
  grant names exactly one of `object` (a collection, or a full-text, geo or
  history object of the spec) or `resource: database`, and lists
  `permissions` (`read`, `write`, `admin`). Anything else is
  `SECURITY_GRANT_TARGET_INVALID`; an unknown object is
  `SECURITY_GRANT_UNKNOWN_OBJECT` with a did-you-mean suggestion.
- **Principals** map a login `user` to a `principal` name (the user by
  default) and roles declared in `security.roles`
  (`SECURITY_PRINCIPAL_UNKNOWN_ROLE` otherwise). Passwords never appear in a
  spec.
- **Reserved names.** The bootstrap administrator, role `admin` and principals
  `admin` and `root`, cannot be managed or assigned (`SECURITY_RESERVED_NAME`).
- **No system grants.** There is no `system` scope, and objects starting with
  `__` cannot be granted (`SECURITY_SYSTEM_GRANT_FORBIDDEN`). Note that in this
  build a grant on the whole database also covers the system resource: `read`
  covers system reads (SQL system views and workload reports), `write` system
  writes and `admin` system administration (provisioning, RBAC and audit). So
  every `resource: database` grant is the Warning `SECURITY_BROAD_GRANT`,
  which names what it covers.
- **Index grants.** `match()` and `within_radius()` return the source
  documents' text, ids and points, so searching a full-text or geo index
  through `query_as` also needs `read` on its source collection, and is
  denied while that collection has row or masking policies. A role that can
  read the index but not the source gets the Warning
  `SECURITY_INDEX_GRANT_WITHOUT_SOURCE_READ`.
- Duplicate roles, users, grant targets, permissions and role assignments are
  `SECURITY_DUPLICATE`.

`provisioning.unmanaged` is `ignore`, `report` (the default) or `fail`: what a
plan will do with catalog objects the spec does not declare.

## Size limits

Version 2 reports a breached limit as an Error, version 1 as a Warning, and
provisioning always refuses it (`SPEC_LIMIT_EXCEEDED`):

- at most 1024 collections and 4096 catalog objects in total;
- per collection: at most 256 fields or columns, 64 document or table indexes,
  16 full-text entries and 16 geo entries;
- at most 256 roles, 1024 grants per role and 4096 principals;
- `overrides` and `operation_hints`: at most 512 entries, keys of at most 256
  bytes and values of at most 4 KiB.

## Plans

`plan_provision(spec, target, options, environment)` computes a
`ProvisionPlan`: every step it takes to converge the target to the spec, with
its safety class, the flags it needs, how each guarantee is enforced on the
calling surface and how the live database drifts from the spec. Planning is a
dry run: it never creates a file and never writes a database.

- **Targets.** `ProvisionTargetRef::Path` is a database file that does not
  exist yet, or an empty one; it is probed read-only
  (`inspect_database_metadata`). A path that already holds a database is
  refused with `AlreadyInitialized`, one that another handle holds open with
  `Locked`: an existing database is planned through its open handle,
  `ProvisionTargetRef::Open`, so a dry run never opens a database implicitly.
  `ProvisionTargetRef::Volatile` is a new in-memory database (Rust API only).
  Only single-node CP databases are planned: AP and sharded handles are
  refused, and the embedded CP cluster constructors fail closed with
  `Unsupported`, so no CP cluster handle reaches provisioning.
- **Surfaces.** `ProvisionEnvironment.surface` is `rust_api`, `cli`, `serve`
  or `control_plane`. `ProvisionEnvironment.encryption` is accepted on the
  Rust API only, as a `LocalEnvelope` config; with an open database it must
  equal the config the database was opened with. `ProvisionEnvironment.audit`
  must equal the audit config of an open handle (`open_database_with_ops` does
  not apply `OperationalConfig.security.audit`, so a different one would be
  silently ignored). `ProvisionEnvironment.principal`, the principal that will
  apply the plan, is checked for `RBAC_SELF_LOCKOUT`.
- **Options.** `ProvisionOptions` (`allow_weakening` and `allow_destructive`
  path sets, `allow_format_upgrade`, `unmanaged`) are per run and never stored
  in a spec.
- **An invalid spec** gives a blocked plan with the validation report in
  `validation`; provisioning issues are in `issues`.

### Plan ids

`plan_id` is `prov_` and 32 hex characters of blake3 over the canonical spec
JSON (`config_spec::canonical_spec_json`), the live fingerprint (or `absent`),
the canonical options JSON, the surface, the canonical target path (or
`volatile`) and the crate version. So:

- an id computed on one surface never confirms an apply on another, and
  `prov_` and the `plan_` ids of `config plan` never confirm each other;
- any change to the live catalog, table schemas, RBAC state, audit flag or
  key, encryption, extension policies, session performance, profile manifest
  or provision record changes the id, while inserting data does not: the
  fingerprint (`live_` and 32 hex characters) leaves document counts out;
- symlinked and relative spellings of one file give one id: the path is
  canonicalized (the parent directory for a file that does not exist yet).

`spec_digest` is `config_spec::spec_digest`: `spec_` and 32 hex characters of
blake3("multidb-spec-v1" ‖ canonical JSON). `required_confirmation` is the
plan id, or `None` for a noop.

### Steps, safety classes and flags

Steps run in a fixed order and are numbered `pstep-001`, `pstep-002`, ...:

1. create the database (a new target);
2. enable audit;
3. containers: objects with a pinned collection or graph id first, so ids
   allocated when an object is created never take a pinned one; then
   documents, vectors, tables, time series and graphs, in spec order;
4. dependents: full-text and geo indexes after their source, then system
   versioning;
5. the profile manifest;
6. RBAC;
7. releases;
8. recorded guarantees and metadata;
9. the applied spec;
10. disable audit.

| safety | what it means | gate |
| --- | --- | --- |
| `additive` | creates or adopts an object, role or principal | none |
| `metadata` | records labels, guarantees, the applied spec or a manifest rewrite without runtime effect | none |
| `strengthening` | turns audit on | none |
| `retune` | rewrites session knobs in the profile manifest; they take effect at the next open | a confirmation |
| `weakening` | weakens a guarantee, a floor or a grant | `allow_weakening` for the step path |
| `destructive` | removes a managed role or principal, or releases a managed object | `allow_destructive` for the step path |
| `format_upgrade` | turns value framing on (`schema_version` 2), forward-only | `allow_format_upgrade` |
| `unsupported` | no runtime API performs it | blocks the plan |

Flags are listed in `required_flags` as `allow_weakening:<path>`,
`allow_destructive:<path>` or `allow_format_upgrade`; each one the options do
not give is a `STEP_REQUIRES_FLAG` issue. The outcome is:

- `blocked` when the spec is invalid, any issue is an Error (other than
  `STEP_REQUIRES_FLAG`), any step is unsupported or any guarantee is not
  enforceable;
- otherwise `needs_flags` when a flag is missing;
- otherwise `noop` when there is no step, and `ready` when there is.

`auto_approvable` is true for a ready plan whose steps are all additive,
metadata or strengthening, without RBAC steps on an existing database and
without enabling system versioning on a table the plan adopts;
`Confirmation::AutoApproveAdditive` accepts only such plans, and so do the
CLI `--auto-approve` and `init --apply`. Creating
a full-text or geo index on a collection that already holds documents names
the document count in its step, which the index backfills before it is
published: medium CPU and low disk and downtime impact (the apply holds the
database while it runs), low risk, and no backup required.

### What a plan compares

| change | safety |
| --- | --- |
| new database | `additive` (`create_database`) |
| legacy database without a profile manifest, same storage profile | `adopt_profile`: `metadata` when the session already runs the desired knobs, else `retune` |
| storage profile or layout differs | `unsupported`, `PROVISION_ENGINE_MISMATCH` |
| manifest knobs that cannot change after creation (`storage.*`, `durability.commit`) | `unsupported`, `PROVISION_ENGINE_MISMATCH` |
| session knobs (`cache.*`, `query.*`, `durability.group_commit_window_ms`, `compression.min_bytes`, `compression.zstd_level`) | `retune_profile`, `retune` |
| compression algorithm between lz4 and zstd on a value-framed database | `retune` (frames are self-describing) |
| compression `none` to lz4 or zstd, also for a legacy database | `upgrade_storage_format`, `format_upgrade` |
| compression that would turn value framing off, also on a legacy framed database | `unsupported`, `PROVISION_FORMAT_MISMATCH` |
| `history.*`, `time_series.*`, `limits.*`, `consistency.default_domain`, `compatible_roles`, raised floors, profile identity (slug, kind, variant, `extends`) | `adopt_profile`, `metadata`; existing objects keep their parameters, so a new time-series default shows up as an object conflict |
| a lowered floor | `adopt_profile`, `weakening`, flag `custom_profile.floors.<name>` |
| a missing object | `create_*`, `additive` |
| system versioning of an existing table the plan adopts (an unmanaged or released table) | `enable_system_versioning`, `additive`, but the plan is never `auto_approvable`: it changes how a table provisioning did not create records writes, so it needs a typed confirmation (`--auto-approve`, `init --apply` and `MULTIDB_SPEC_APPLY=additive` refuse it) |
| an object that matches the spec and is not managed yet | `adopt_object`, `additive` |
| an object that differs, or has another kind | `unsupported`, `PROVISION_OBJECT_CONFLICT` with a field diff |
| a managed object the spec no longer declares | `release_object`, `destructive`, flag `collections.<name>` (`collections.<collection>.<index or history name>` for derived objects); the object and its data stay |
| audit off, spec on | `enable_audit`, `strengthening`, first |
| audit on, spec off | `disable_audit`, `weakening`, flag `guarantees.audit.enabled`, last; `unsupported` while `floors.audit` is set |
| audit off, spec off | `AUDIT_REQUIRED_FOR_APPLY` unless the plan is a noop |
| a role or principal to create, or permissions to add | `apply_security`, `additive` |
| an unmanaged role or principal identical to the spec | `apply_security`, `metadata` (adopted) |
| an unmanaged role or principal that differs | `unsupported`, `PROVISION_OBJECT_CONFLICT` |
| a revoked permission on a managed role, a narrowed managed principal | `weakening`, flag `security.roles.<name>` or `security.principals.<user>` |
| a managed role or principal the spec dropped | `destructive`, same flag paths |
| a declared guarantee weaker than the last applied spec | `record_guarantees`, `weakening`, flag at its path; stronger ones are `metadata` |
| domains, overrides, operation hints, extension references | `record_metadata`, `metadata` |
| a spec other than the last applied one | `record_applied_spec`, `metadata`, last before audit is disabled |

Matching compares only what a spec controls: kind, fields, index paths
(index ids only when pinned), table layout, schema (when columns are given)
and index set, vector dimension, metric and HNSW parameters, time-series
chunk and retention, full-text and geo source, path, language, lag and
precision, history base table and `keep_history`, and collection and graph
ids when the spec pins them or the record allocated them.
`deployment.storage_path` is never compared: it is a default location, not
identity, so a spec that differs from the last applied one only in its
storage path is still the applied spec (no `record_applied_spec` step and no
`spec_changed` drift). The record keeps the storage path it was applied with,
and `spec_digest` still covers the whole spec.

Live objects the spec does not declare follow `provisioning.unmanaged`
(`ProvisionOptions.unmanaged` overrides it): `ignore` leaves them out,
`report` (the default) gives the Warning `DRIFT_UNMANAGED`, `fail` makes it an
Error. An object released by an earlier apply is information only. A managed
object missing from the database is `DRIFT_ORPHAN`: it is created again when
the spec still declares it, and removed from the record otherwise. An apply
that did not finish is `DRIFT_INTERRUPTED_APPLY`, and the plan holds only what
remains.

### RBAC

RBAC is managed-only: the spec's roles replace their managed namesakes,
managed roles and principals the spec dropped are removed, and every other
role and principal, including the bootstrap administrator (role `admin`,
principals `admin` and `root`), is left alone. Grants are resolved to the
resource of their object (`Collection`, `VectorCollection`, `Table` for
tables and `<table>_history`, `TimeSeries`, `Graph`, `FullTextIndex`,
`GeoIndex`, or `Database`). Authorization resolves roles by name, so a role
the plan creates or widens also authorizes every principal that already lists
its name: users the spec does not manage that hold it (left over from a
removed role, or assigned through the admin API) are named in the step and get
the Warning `SECURITY_ROLE_ALREADY_ASSIGNED`. A plan that would take system
administration away from `ProvisionEnvironment.principal` is
`RBAC_SELF_LOCKOUT`. The check resolves the principal the way authorization
will after the apply: through
the login users that map to it (a user may map to a principal of another
name), so narrowing its roles, removing its principal or remapping its user
all count; when several users map to it, losing administration through any
of them blocks.

A handle whose RBAC state or audit flag differs from what is persisted in the
database is `PROVISION_SECURITY_STATE_STALE` (blocks). Every opener loads the
persisted RBAC state, so this happens only after an in-memory change:
`set_authz_policy` and `set_principal_registry` do not persist (`multidb admin
serve --insecure-local-admin` grants its local administrator that way), and an
RBAC or audit change whose persist failed (`set_authz_policy_as`,
`set_principal_registry_as`, `set_audit_enabled_as`) keeps its in-memory
state. An apply would otherwise persist state the database never had. Persist
the change (`set_authz_policy_as`, `set_principal_registry_as`,
`POST /security`) or reopen the database, and plan again.

### Guarantees

| guarantee | single-node status |
| --- | --- |
| `write_ack`, `conflict_resolution: none`, `strict_cross_domain_transactions`, `local_snapshot` and `strong_cp` domains | `trivially_satisfied`: one replica, redb `Durability::Immediate`, one replication mode |
| `backup.enabled`, `backup.pitr` | `operator_managed` with the `multidb backup`, `verify` and `restore` runbook and the Warning `GUARANTEE_OPERATOR_MANAGED`, when requested; there is no scheduler |
| `encryption.at_rest` | `enforced` on the Rust API with a `LocalEnvelope` config for a new database, or for an open database that is encrypted; otherwise `not_enforceable` (`GUARANTEE_NOT_ENFORCEABLE`, blocks). An existing database whose encryption differs from the spec is `PROVISION_ENCRYPTION_MISMATCH` (blocks): provisioning cannot encrypt or decrypt an existing database (`Database::reencrypt_existing_values` only rewrites the values of an encrypted database under its current key). So is a `ProvisionEnvironment.encryption` for a new database whose spec says `at_rest: false`, refused before anything is created |
| `sensitive_data` | follows `encryption.at_rest`; the development audit key warns but never blocks it |
| `audit.enabled` | `enforced` with a configured audit key; `enforced_with_development_key` and the Warning `AUDIT_DEVELOPMENT_KEY` without one, which is always the case on the CLI and `serve`. A plan with steps against an existing database whose newest audit event does not verify under the handle's key (a chain written with a configured key, planned through a handle that falls back to the development key, or the reverse) is `PROVISION_AUDIT_KEY_MISMATCH` (blocks), since the events the apply appends would break `verify_audit_chain`; a noop is not blocked, since it writes nothing. The check reads the whole audit log |
| `cache` TTL and eviction, `event_log` append-only | `not_enforced`, the Warning `ROLE_CONSTRAINT_NOT_ENFORCED` |

A guarantee the spec does not ask for is `not_requested`.

### Scope

Provisioning builds embedded and single-node CP databases with one replica and
one shard. Everything else fails closed:

- cluster deployments and other topologies: `PROVISION_DEPLOYMENT_UNSUPPORTED`.
  This matches the runtime: the embedded `Database` cluster constructors
  (`create_cluster_database`, `open_cluster_database` and their `_with_ops`
  variants) fail closed with `Unsupported`, and multi-node CP replication runs
  only through `repl::start_cp_cluster`, which provisioning does not drive;
- AP replication (in the spec or in the database) and conflict policies other
  than `none`: `PROVISION_REPLICATION_UNSUPPORTED`; `eventual_ap` domains:
  `DOMAIN_MODE_INCOMPATIBLE_WITH_REPLICATION`. AP databases also reject
  commit-log features such as system versioning with `Unsupported`;
- volatile profiles outside the Rust API: `PROVISION_VOLATILE_ON_SURFACE`; a
  durable profile without a path: `PROVISION_STORAGE_PATH_REQUIRED`; a
  volatile profile at a path: `PROFILE_DEPLOYMENT_MISMATCH`;
- extensions: a built-in manifest with the same version is recorded as
  metadata, an experimental one gives the Warning
  `PROVISION_EXTENSION_EXPERIMENTAL`, a custom or unknown one is
  `PROVISION_EXTENSION_UNAVAILABLE` and another version
  `EXTENSION_VERSION_INCOMPATIBLE`;
- a version 1 profile that does not resolve to built-in knobs:
  `PROVISION_PROFILE_UNRESOLVED`.

## Drift

`drift(db, spec, options)` reports how an open database differs from a spec:
the drift of the plan on the Rust API surface, or `InvalidSpec` for a spec
that does not validate. Each item has a path, a kind (`missing_object`,
`object_conflict`, `unmanaged_object`, `orphaned_object`, `released_object`,
`setting_differs`, `storage_class_mismatch`, `format_mismatch`,
`encryption_mismatch`, `guarantee_not_enforceable`, `session_pending_restart`,
`interrupted_apply` or `spec_changed`), the expected and actual values,
whether it blocks and what resolves it (`create`, `adopt`, `apply`, `retune`,
`release`, `restart`, `unsupported` or `none`). `in_sync` is true when the
plan is not blocked and every item is information only: released objects and
unmanaged objects under `report` or `ignore`. Guarantee statuses such as
`operator_managed` or `enforced_with_development_key` are reported in the
plan, not as drift. `session_pending_restart` means the session runs other
knobs than the profile manifest, for example after a retune; a restart
resolves it. A query governor set for the process is not drift:
`Database::set_query_governor`, which `multidb serve` calls for
`MULTIDB_MAX_CONCURRENT_QUERIES` and `MULTIDB_QUERY_QUEUE_TIMEOUT_MS`, keeps
its two limits for the session, and a restart sets them again, so a retune
of only those knobs leaves nothing pending.

## Export

`export_spec(db)` describes an open database as a spec (`ExportedSpec` with
`spec`, `source` and `notes`):

- a database provisioning applied a spec to returns that spec from its record
  (`source: applied_record`);
- any other database gets an inferred version 2 spec
  (`source: inferred`): document collections become `document_entity`,
  vector collections `vector_memory`, row tables `document_entity` with
  `kind: table`, columnar tables `analytics`, time series `time_series` and
  graphs `graph`; full-text and geo indexes and history tables attach to
  their source; every id and index id is pinned, as are the chunk, the
  retention and the future-skew bound of a time series. The profile is the
  manifest's (a custom profile is rebuilt from its knobs; its description is
  lost), or the storage profile alias for a legacy database. RBAC is exported
  minus the bootstrap administrator. Audit and encryption follow the
  database; backup and PITR are true only when the profile floors require
  them. The required built-in extensions are listed.
- What a spec cannot express is left out and listed in the notes and in
  `operation_hints["export.unsupported_objects"]`: foreign tables,
  materialized views, null-typed fields, partial indexes, `__` names, roles
  with system grants or grants on objects the spec does not declare, and
  principals with such roles. `operation_hints["export.inferred_roles"]`
  lists the inferred roles.

Planning the exported spec against the same database only adopts objects,
the profile and RBAC and records metadata. An unreadable record makes export
fail with `RecordUnreadable`.

`export_spec` returns `deployment.storage_path` as it is: the path the
database was opened with for an inferred spec, and the path the applied spec
named, relative to its own file, for a record. `multidb config export` points
it at the exported database instead, relative to the directory of `--out`
(the canonical absolute path for stdout), because `config plan|apply --spec`
and `serve` resolve it against the directory of the spec file; a caller of the
Rust API that saves the spec should do the same. `GET /config/live` returns it
as the database was opened, since the Control Plane ignores it.

## Applying plans

`apply_provision(db, actor, spec, confirmation, options, environment)` applies
a spec to an open single-node database under exclusive `&mut Database`:

1. **The plan is recomputed** from the live database; a client plan is never
   trusted.
2. **The confirmation is checked** against the recomputed plan.
   `Confirmation::PlanId` must name its `prov_` id: another `prov_` id is
   `ProvisionError::PlanChanged` (the live state, the spec, the options, the
   surface, the target or the build changed since the plan was reviewed), and
   anything else, such as a `plan_` id of `config plan`, gives a report with
   status `rejected` and changes nothing. `Confirmation::AutoApproveAdditive`
   accepts only an `auto_approvable` plan, otherwise `ConfirmationRequired`.
3. **The outcome is checked.** A `noop` returns a `noop` report without
   writing anything, not even an audit event. A `blocked` plan is
   `ProvisionError::Blocked`, or `AuditRequired` when audit is off and the
   spec keeps it off; `needs_flags` is `NeedsFlags`.
4. **The actor is authorized.** `ProvisionActor::Principal` needs
   `System:Admin` and `Database:Admin` (`PermissionMissing` otherwise, the
   denial is audited), and the plan is recomputed with it as
   `ProvisionEnvironment.principal`, so `RBAC_SELF_LOCKOUT` covers the
   principal that applies. `ProvisionActor::LocalOperator` (`local:cli`,
   `local:serve`, `local:embedded`) relies on filesystem trust, like `backup`
   and `restore`; its audit events carry the label as the principal name.
5. **Audit comes first.** When audit is off, the plan's first step turns it on
   (`set_audit_enabled`); without that step the apply is `AuditRequired`.
   Then `provision_apply_started` is audited with the plan id, the surface,
   the step count and the flags, and this write must succeed before anything
   else changes.
6. **The record is journaled** as `in_progress`, holding the spec being
   applied and the spec the last finished apply converged to
   (`last_applied_spec`), and rewritten after every step with its completed
   steps and the objects it owns.
7. **The steps run in plan order.** Each first checks whether it is already
   satisfied. A principal creates through the audited `*_as` APIs (one
   `create_*` event per object, ids allocated by the `_auto_id_as` variants);
   a local operator uses the base creates and audits one
   `provision_create_object` per object. Time series are built only through
   `physical::time_series_config`. Every `apply_security` step commits with
   the first one, in one batch (`Database::set_security_state[_as]`, audited
   as `set_security_state`). Profile steps write the desired profile manifest
   once (`provision_write_profile_manifest`). Releases only change the record
   (`provision_release_object`). Recorded guarantees, metadata and the applied
   spec are written with the final record.
8. **The record becomes `applied`**, a history entry with the flags is
   appended, and `provision_apply_completed` is audited with
   `data_mutated: true`. When that event cannot be written, the apply is
   still `applied`: the report carries the Warning
   `PROVISION_COMPLETION_NOT_AUDITED` and `audit_recorded: false`.
9. **Audit is disabled last**, after the completion event, when the spec asks
   for it and `--allow-weakening guarantees.audit.enabled` was given.

The `ProvisionApplyReport` has the `status` (`applied`, `noop`, `rejected`,
`partially_applied`; `blocked` and `plan_changed` exist for surfaces that
answer with a report, and `plan_changed` is also the report of a new
database whose recomputed plan differs), the confirmation `check`
(`data_mutated` is true only for `applied`, `partially_applied` and that
`plan_changed`), the confirmed `plan_id`, one
`applied_steps` entry per step (`applied`, `already_satisfied` or `failed`,
with an `error_kind` such as `catalog_object_exists` for a failed one), the
`failed_step`, a summary of the record, the plan's Warnings and
`session_pending_restart`.

Every write an apply makes is a system write, like the builder creates: the
catalog entries, index entries, temporal metadata, profile manifest, RBAC
state, audit events and the record bypass the write pipeline, so they run no
triggers and are not charged to a tenant quota.

### Partial applies and recovery

Applying is not atomic across objects: each create is its own batch. Once
anything changed the database, an apply answers with a report, never an
error. A step that fails leaves the steps before it applied: the record says
`failed` with its `failed_step` and `completed_steps`, a history entry is
written, `provision_apply_failed` is audited, and the report is
`partially_applied`. A step that fails after its write committed, such as a
create whose audit event cannot be written, says so in its `detail` and in
the message ("failed after it wrote to the database"), and an object its
create committed is managed by the failed record. When the intent event or
the first journal write fails after audit was turned on by the first step,
the report is `partially_applied` with no `failed_step`; before anything
changed, it is an error. The next plan reports `DRIFT_INTERRUPTED_APPLY` and
holds only what remains: objects the failed apply created are already
managed, so they are satisfied.
Declared guarantees and metadata count only once an apply finishes: the plan
compares them with the spec the last finished apply converged to (the
record's `last_applied_spec`; `ProvisionRecord::last_applied()`), so a
weakening the failed apply confirmed needs its `--allow-weakening` flag again,
and `spec_migration` keeps that baseline. The applied spec is always recorded
again. Applying that plan converges, and a further plan is a
`noop`. When the failure itself cannot be recorded, the report carries the
Warning `PROVISION_FAILURE_NOT_RECORDED` and the record still says
`in_progress`, which the next plan treats the same way. Batching every
container of a plan into one write is a follow-up.

### Releasing objects

Removing a collection from the spec releases it: the step is `destructive`
and needs `--allow-destructive collections.<name>`
(`ProvisionOptions.allow_destructive`). There is no drop API: the object and
its data stay, the record stops managing it and lists it under
`released_objects`, and drift reports it as information only. Declaring it
again adopts it back when it still matches.

### The storage format upgrade

Turning compression on for a database created without it is the
`format_upgrade` step and needs `--allow-format-upgrade`. It writes the
value-framing profile manifest and `schema_version` 2 in one batch. It is
forward-only: older binaries refuse the database, and value framing can never
be turned off again. The running session keeps its unframed engine, so the
report sets `session_pending_restart`; from the next open the engine frames
every value it writes, and values written before the upgrade stay readable
because frames are recognised by their magic. A legacy value that happens to
start with the frame magic (`MDBC1`) fails its checksum and is reported as
corruption, never misread. Retunes are written the same way and also take
effect at the next open.

### New databases

`provision_new_database(spec, path, confirmation, options, environment)`
creates a database from a spec at an absent target: `path`, else
`deployment.storage_path` (relative to the working directory), else a new
in-memory database (Rust API only).

1. The plan is computed against the absent target; a target that holds a
   database is `AlreadyInitialized`, and a confirmation that is not a
   provisioning plan id is `ConfirmationRequired`.
2. The database is created with `create_database_with_profile`: audit on,
   `ProvisionEnvironment.audit` as its audit key and anchor, and
   `ProvisionEnvironment.encryption` (a `LocalEnvelope` config, Rust API only)
   as its encryption. The new file holds no tables yet, so its keyring is
   created at the configured path, or opened when one already exists there
   (with the configured KEK). `create_*_with_ops` is never used on an
   existing database, since it would overwrite its security state.
3. The plan is recomputed against the new database. Its steps must equal the
   confirmed ones minus `create_database` (kinds, paths, objects, safety
   classes and flags), otherwise the apply stops with a `plan_changed` report
   and the new database is kept as it is. `auto_approvable` is not compared:
   RBAC steps on an existing database are never auto-approvable, but a new
   database with roles may be auto-approved.
4. The steps run as the local operator of the surface, under the confirmed
   plan id. The Control Plane never creates databases.

Once the database exists, every outcome is a report with `data_mutated: true`
and the applied `create_database` step, never an error: `plan_changed` when
the recomputed plan differs, `partially_applied` when it cannot be recomputed
or the apply stops before its first step. Plan the new database through its
open handle to see what remains.

## CLI

The CLI provisions as the local operator `local:cli` on the `cli` surface. It
relies on filesystem trust, like `backup` and `restore`, and on redb's
exclusive file lock: a database another process holds open is refused.

| command | what it does |
| --- | --- |
| `multidb config plan --spec <file> [--db <path>] [--out <plan.json> [--force]] [flags] [--json]` | Dry run against the database at the target, or against an absent target. Prints every step with its safety class and flag, the flags, the objects, every guarantee status, the issues, the drift, the impact and the exact `config apply ... --confirm prov_...` command; `--out` writes the plan JSON, never over an existing file without `--force` and never over the spec or the database |
| `multidb config apply --spec <file> [--db <path>] (--confirm <prov_id> \| --auto-approve) [flags] [--json]` | Recomputes the plan and applies it: a missing or empty file is created with `provision_new_database`, an existing database is opened and converged with `apply_provision`. Without `--confirm` or `--auto-approve` a noop exits 0 and any other plan is printed with exit 2 |
| `multidb config drift --spec <file> [--db <path>] [--unmanaged ignore\|report\|fail] [--json]` | How the database differs from the spec, for CI |
| `multidb config status --db <path> [--json]` | The provision record with its guarantee statuses, the last 20 history entries, the profile manifest and the drift of the database against the spec the record applied |
| `multidb config export --db <path> [--format yaml\|json] [--out <file>] [--force]` | `export_spec`: the applied spec, or an inferred version 2 spec, with `deployment.storage_path` pointing at the database relative to the directory of `--out` (the canonical absolute path on stdout), so `config plan --spec <file>` without `--db` plans the exported database. Without `--out` the spec goes to stdout and the notes to stderr; `--out` never replaces a file without `--force`, and never the database or another multidb database. A spec larger than 1 MiB (`MAX_SPEC_BYTES`), which `config plan\|apply --spec` could not read back (the Rust API applies specs of any size), exits 2 with `SPEC_TOO_LARGE` and is written nowhere, neither to `--out` nor to stdout, as in `config upgrade` |
| `multidb init --guided (--profile <p> \| --template <t>) --name <n> ... --apply [--db <path>] [--confirm <prov_id>]` | Writes the files as `init` always does, then provisions a new database from the spec |

The flags of `config plan` and `config apply` are `--allow-weakening
<path>[,<path>]` and `--allow-destructive <path>[,<path>]` (both repeatable),
`--allow-format-upgrade` and `--unmanaged ignore|report|fail`
(`ProvisionOptions`). They are part of the `prov_` id, so `config apply
--confirm` must repeat the flags of the plan it confirms; a plan that needs
flags prints the `config plan` command that adds them. `--spec` cannot be
combined with `--plan`, `--current` or `--desired`, which belong to the
version 1 flow: `config plan --current --desired` and `config apply --plan
--confirm` are unchanged and stay audit-only (`data_mutated: false`).

**The target** is `--db`, else the spec `deployment.storage_path` resolved
against the directory of the spec file (for `init --apply`, against the
output directory; without a storage path, `init --apply` uses
`<out dir>/<sanitized name>.redb` and prints the path it chose). A durable
profile with neither is `PROVISION_STORAGE_PATH_REQUIRED`, and a volatile
profile is `PROVISION_VOLATILE_ON_SURFACE`: the CLI builds durable databases
only. The target is probed first: a missing or empty file is planned as
absent and nothing is created by a dry run. An existing database is opened
with `open_database_with_ops`, which loads its persisted RBAC state; only
single-node CP databases are provisioned, so an AP database is refused with
`PROVISION_REPLICATION_UNSUPPORTED` before it is opened.

**Exit codes:**

| code | meaning |
| --- | --- |
| 0 | the plan is ready or a noop; the apply applied or was a noop; the database is in sync |
| 1 | a partial apply (printed with the failed step and "re-run plan"), a runtime or I/O error, a locked database, a usage error |
| 2 | an invalid spec, a blocked plan, missing flags, a missing or rejected confirmation, a target that already holds a database (`init --apply`), an exported spec over 1 MiB (`config export`, `SPEC_TOO_LARGE`); for `config drift`, an invalid spec or no database path |
| 3 | drift (`config drift` only): not in sync, including a missing database, a blocked plan and unmanaged objects under `--unmanaged fail` |
| 4 | the confirmed `prov_` id is no longer the current plan |

`config drift` treats unmanaged objects under `report` (the default) and
`ignore` as information, as `DriftReport.in_sync` does: an object created
outside provisioning is listed, but the exit code stays 0 unless
`--unmanaged fail` is given.

`init --apply` creates new databases only: an existing one is refused (exit
2) with the `config plan` and `config apply` commands that converge it. The
plan is auto-approved unless `--confirm` names its id; a blocked or not
auto-approvable plan leaves the written files and exits 2 with the reason.
With `--json` the init summary stays one object, with the chosen `database`
path and the `provision` report or error.

`config upgrade` lists, as notes, the physical parameters provisioning still
needs (`PHYSICAL_PARAMETER_REQUIRED`, Advice in version 2 validation), so a
converted version 1 spec says what to add before `config apply --spec`.

### Troubleshooting

- **"database is in use (is multidb serve running?)"** (exit 1): another
  process, usually `multidb serve`, holds the redb lock. Plan and apply
  through that server's Control Plane (`POST /provision/plan`, then `POST
  /provision/apply`, see [Control Plane](#control-plane)), or stop it and use
  the CLI; in spec mode, `MULTIDB_SPEC_APPLY=additive` applies additive
  changes at the next start (see [Serve](#serve)).
- **"the plan changed since it was confirmed"** (exit 4): the database, the
  spec, the flags, the target or the binary changed after the plan was
  reviewed, for example an object created outside provisioning. Nothing was
  applied; run `config plan` again, review it and confirm the new id with the
  same flags.
- **A database that was not closed cleanly** (for example after `serve` was
  killed) first gets redb's crash recovery, which every opener runs; it is
  the only change a dry run can make to a file. A file that still cannot be
  read, or a storage format the CLI cannot open (a legacy framed database
  needs the Rust API), is reported with its reason (exit 1).
- **A partial apply** (exit 1) leaves the steps before the failed one
  applied and a `failed` record; `config plan` shows the remaining steps and
  `config status` the record and history.

## Serve

`multidb serve` provisions in spec mode, as the local operator `local:serve`
on the `serve` surface: `MULTIDB_SPEC_PATH` names the spec, and
`MULTIDB_SPEC_APPLY` (`verify`, the default, `create` or `additive`) what a
start may change. Without `MULTIDB_SPEC_PATH`, `serve` is unchanged. The
container contract (variables, Helm values) is in
[docker.md](docker.md#spec-mode).

1. The spec is read through `spec_io` (1 MiB, version peek), validated and
   compiled. A spec that cannot be read, decoded or validated is refused.
2. The database path is an explicit `MULTIDB_DB_PATH` (set and not blank),
   else the spec `deployment.storage_path` resolved against the directory of
   the spec file, else serve's default `/var/lib/multidb/multidb.redb`. An
   explicit path that wins over a spec `storage_path` is printed as a note.
3. Before anything is opened, `serve` refuses:
   - an explicit `MULTIDB_PROFILE` that resolves to another storage profile
     than the spec profile (`auto` never conflicts). The container image sets
     `MULTIDB_PROFILE=transactional`, so blank it (`MULTIDB_PROFILE=""`, as
     the Helm chart does in spec mode) rather than unset it;
   - an explicit `MULTIDB_REPLICATION` (`cp` or `ap`, read in spec mode only)
     other than the spec `defaults.replication`;
   - `guarantees.encryption.at_rest: true`: encryption at rest is provisioned
     through the Rust API only;
   - a volatile profile.
4. `open_from_spec` on the `serve` surface probes the database (a file left by
   a killed `serve` first gets redb's crash recovery, as with every opener),
   opens an existing one with `open_database_with_ops`, which loads its
   persisted RBAC state, plans the spec against it or against the absent
   target and decides with `serve_startup_decision`:

| target | `verify` | `create` | `additive` |
| --- | --- | --- | --- |
| absent, plan `auto_approvable` | refuse ("database missing; set MULTIDB_SPEC_APPLY=create or run multidb init --apply") | create it | create it |
| existing, plan a noop, with no drift or only information drift (unmanaged objects under `report` or `ignore`, `operator_managed`, `enforced_with_development_key`) | start | start | start |
| existing, plan `auto_approvable` | refuse | refuse | apply it |
| interrupted apply (`DRIFT_INTERRUPTED_APPLY`), remaining plan `auto_approvable` | refuse | refuse | apply it |
| anything else: retune, weakening, destructive, format upgrade, RBAC changes on an existing database, missing flags, blocked | refuse | refuse | refuse |

A start applies only `auto_approvable` plans, with
`Confirmation::AutoApproveAdditive`. Every refusal comes before a port is
bound: `serve` exits with the reason, the plan and the command that resolves
it. For a ready plan, or one that needs flags, that is `multidb config apply
--spec <absolute spec> --db <canonical database path> [exactly the --allow-*
flags the plan needs] --confirm <prov_id>`, where the id comes from a second
plan on the `cli` surface against the same canonical target with those flags,
so running the command (with `serve` stopped, since the database is locked
while it runs) confirms that plan. `prov_` ids are per surface, so this id
differs from the serve plan the refusal prints. For a blocked plan, or what
remains of a startup apply that failed part-way, it is the `multidb config
plan --spec ... --db ...` command that reviews it.

A startup apply that fails part-way leaves the steps before the failed one
applied and a `failed` record, and `serve` does not start; the next start sees
the interrupted apply, which only `additive` resumes. After provisioning, the
admin password bootstrap runs as before; its `admin` and `root` names are
reserved, so provisioning never touches them. The roles and principals
provisioning applied survive any restart, with or without spec mode: every
opener loads the persisted RBAC state, and the bootstrap only adds the
reserved `admin` role and principal when they are missing.

**Known limit:** audit is chained with the development key
(`enforced_with_development_key` and `AUDIT_DEVELOPMENT_KEY`): `serve` has no
audit key setting. A database whose audit chain was written with a configured
key (on the Rust API) is refused with `PROVISION_AUDIT_KEY_MISMATCH` whenever
a start would change it, and `serve` itself appends its admin events with the
development key, so serve such a database only after deciding to give up its
keyed chain.

**Embedded apps** use `open_from_spec(spec, path, mode, options, environment)`
with the same decision table on the `rust_api` surface, as `local:embedded`.
The target is `path`, else `deployment.storage_path` (relative to the working
directory), else a new in-memory database, which is new at every open, so
only `create` and `additive` build it. An existing database is opened with
`ProvisionEnvironment.encryption` (a `LocalEnvelope` config); an encrypted
one, whose metadata cannot be probed without the key, is opened with that
config and the storage profile of the spec, and the open checks both against
the decrypted metadata. The open never creates a keyring for a file that
already holds tables: when the keyring is missing (a path typo or an
unmounted volume), every start mode fails and writes no keyring, since a new
one could never decrypt the data. `ProvisionEnvironment.audit` is applied to the handle
afterwards, because `open_database_with_ops` does not apply an audit config:
the audit chain then stays on the caller's key across opens. An audit key file that cannot be read
or parsed fails the open. A refusal is `ProvisionError::StartupRefused` with
a `StartupRefusal` (`mode`, `reason`, the `plan`, and the `applied` report of
a startup apply that failed part-way); a start returns the database and a
`StartupReport` (`mode`, `decision`, `plan_id`, the `applied` report,
`warnings`).

## Control Plane

A running `multidb serve` (or any server built on `multidb::admin`)
provisions the database it serves through five preview endpoints, mounted at
`/` and `/api` behind the admin token or session like every other operation:

| endpoint | what it does |
| --- | --- |
| `POST /provision/plan` | `{spec, options?}`: the plan of the spec against the served database, on the `control_plane` surface. A dry run: nothing is written, not even an audit event (only a denied request is audited) |
| `POST /provision/apply` | `{spec, confirm, options?}`: recomputes the plan and applies it when `confirm` is its current `prov_` id |
| `POST /provision/drift` | `{spec, options?}`: the `DriftReport` of the plan |
| `GET /provision/state` | `{provisioned, record, history, manifest, live_fingerprint, spec_mode}`: the provision record and its history, the profile manifest, the live fingerprint plans are computed against now, and, in spec mode, the `spec_digest` and `MULTIDB_SPEC_APPLY` mode `serve` started with |
| `GET /config/live` | `{spec, source, notes, live_fingerprint}`: `export_spec` of the served database (`source` is `applied_record` or `inferred`). It replaces the legacy `GET /config`, which only describes the database configuration |

`spec` is decoded through `spec_io` as on `/config/validate` (a body that is
not a spec is 400 `invalid_json`, a missing or unsupported version 422
`invalid_spec_version`), `options` is a `ProvisionOptions` (an unknown field
is 400), and any other key is 400: there is no client plan, the server always
recomputes it. Bodies keep the 1 MiB limit (larger ones are 400
`invalid_json`, as on the config routes).

**Who may provision.** Every endpoint needs `System:Admin` for the calling
principal (403 `forbidden`, audited as denied); applying also needs
`Database:Admin`. The apply runs as that principal: objects are created
through the audited `*_as` APIs, the record names it in `applied_by`, and
`RBAC_SELF_LOCKOUT` is checked against it, already at plan time. A grant on
the whole database includes system administration in this build. A password
session acts as the bootstrap `admin`. A request with the admin token
(`MULTIDB_ADMIN_TOKEN`) acts as the principal its `x-multidb-principal`
header names, looked up in the database's principal registry, so that
principal needs the permissions: `serve` grants them only to the `admin` of
the admin password bootstrap, or a spec grants them to a principal of its
own.

**The flow** is plan, review, confirm:

1. `POST /provision/plan` returns the plan with 200 when it is `ready`, a
   `noop` or `needs_flags`, and with 422 when it is blocked (codes
   `invalid_spec`, `provision_blocked`, `audit_required`, `rbac_self_lockout`)
   in an error envelope whose `data` is the plan.
2. Review the steps, flags and guarantees. A plan that needs flags is planned
   again with them in `options`.
3. `POST /provision/apply` with the same `spec` and `options` and `confirm`
   set to the plan's `prov_` id. The id hashes the live state, the options and
   the `control_plane` surface, so an id from the CLI never confirms here, and
   anything that changed the database since the plan (for example a
   `/builder/*` call) is 409 `plan_changed`: plan again.

`multidb serve` shares one read/write lock on the database between pgwire and
the Control Plane. Plans, drift, `GET /provision/state` and `GET /config/live`
only read, so they take the shared guard and run next to statements. The
apply takes the exclusive guard from the recomputed plan through the last
step: it waits for running statements and reads, new ones (pgwire included)
wait for it, and nothing changes in between. An error that wraps a busy query
governor is 503 `busy` with `Retry-After`, and an optimistic write conflict
409 `conflict`, as on the other endpoints; both can be retried as is. The
apply's answers:

| status | code | when |
| --- | --- | --- |
| 200 | | `applied`, or a `noop` (which needs no `confirm`) |
| 409 | `plan_changed` | `confirm` is a `prov_` id, but not the current plan's; `data` has both ids |
| 409 | `object_conflict` | a create failed because the object exists; `data` is the partial apply report |
| 422 | `confirmation_required` | no `confirm` (`data` is the plan), or one that is not a `prov_` id (`data` is the `rejected` report) |
| 422 | `flags_required` | the plan needs flags the options do not give |
| 422 | `invalid_spec`, `provision_blocked`, `audit_required`, `rbac_self_lockout` | the plan is blocked; `data` is the plan, except for `audit_required` on a confirmed apply |
| 403 | `forbidden` | the principal lacks `System:Admin` or `Database:Admin` |
| 500 | `provision_partially_applied` | a step failed; `data` is the report. The steps before it stay applied and the record says `failed`: plan again to see what remains |

Retunes and the storage format upgrade are written to the profile manifest
and take effect when `serve` restarts (`session_pending_restart` in the
report), except for a query governor knob that `MULTIDB_MAX_CONCURRENT_QUERIES`
or `MULTIDB_QUERY_QUEUE_TIMEOUT_MS` sets for the process. The Control Plane never creates a database and never opens a path
from a request: a spec `deployment.storage_path` is reported as the Advice
`PROVISION_STORAGE_PATH_IGNORED`. Encryption at rest is not enforceable here
(`GUARANTEE_NOT_ENFORCEABLE`), and audit is chained with the development key
(`enforced_with_development_key`).

**Known limits:**

- A served database whose in-memory RBAC differs from its persisted RBAC is
  refused with `PROVISION_SECURITY_STATE_STALE`: for example under `multidb
  admin serve --insecure-local-admin`, which grants its local administrator in
  memory only. `multidb serve` loads the persisted RBAC state at every start,
  with or without spec mode, and the admin password bootstrap persists the
  state it sets, so RBAC applied through the Control Plane survives a restart.
- Studio has no provisioning flow yet; `StudioManifest.provisioning_supported`
  and the capability `provision_apply` announce the endpoints.

## Templates

The built-in templates emit spec version 2 with the physical parameters
provisioning needs, so `multidb init --guided --template <t> --apply` builds
the database in one step. Everything a template leaves out takes the
defaults above.

| template | physical parameters | CLI plan for a new database |
| --- | --- | --- |
| `game-save` | `saves.document_indexes: [{path: slot}]`; embedded with no `storage_path` (the app chooses; `init --apply` uses `<out dir>/<sanitized name>.redb`) | ready |
| `desktop-embedded` | `documents.document_indexes: [{path: title}]`; `usage_metrics` `chunk_millis: 3600000`, `retention_millis: 2592000000` | ready |
| `ai-memory` | `memories: {dim: 1536, metric: cosine}`; `facts.document_indexes: [{path: subject}]` | ready |
| `secure-saas` | `tenants.document_indexes: [{path: tenant_id}]`; `encryption.at_rest: true` | blocked by `GUARANTEE_NOT_ENFORCEABLE` alone; ready on the Rust API with a `LocalEnvelope` key |
| `analytics` | `metrics.chunk_millis: 3600000`; `aggregates`: a typed columnar table (`id`, `bucket`, `metric` str, `value` float, primary key `id`) | ready |

A vector `dim` and table columns are fixed at creation: edit them in the spec
before `--apply`. The version 1 specs the templates emitted before are frozen
under `tests/fixtures/spec-v1` and still validate. `examples/provisioning` is
a full version 2 example: a document collection with fields, document
indexes, a full-text and a geo index, a versioned ledger table, a vector
collection and a columnar table in a `HighDurability` row-class database,
with a managed role and principal. `scripts/templates-smoke.ps1` runs `init
--apply`, `config drift` and a re-apply (a noop) for every template and
checks that `secure-saas` exits 2.

## The provision record

Provisioning keeps what it owns in the system table `__provision`: the
record (`ProvisionRecord`, key `record`) holds the status of the last apply,
its plan id, the applied spec and its digest (while an apply is unfinished,
also the spec the last finished apply converged to), the managed objects (with the
collection or graph id each one got), roles and principals, released
objects, guarantee statuses, completed and failed steps, flags, actor and
binary version; `history/{seq:020}` keys hold one entry per finished or
failed apply, at most 100 (`Database::provision_history()`). Backup and
restore carry the table like every registered keyspace, and older binaries
ignore it. `record_version` is read before the typed decode: a record of a
newer build, or a corrupt one, is `PROVISION_RECORD_UNREADABLE` and blocks
every plan. `Database::provision_state()` returns the record.

## Validation codes

| code | version 1 | version 2 | provisioning |
| --- | --- | --- | --- |
| `FIELD_REQUIRES_SPEC_V2` | Error | – | – |
| `COLLECTION_NAME_INVALID`, `COLLECTION_NAME_RESERVED`, `OBJECT_NAME_COLLISION`, `SPEC_LIMIT_EXCEEDED` | Warning | Error | Error |
| `PHYSICAL_PARAMETER_REQUIRED` | – | Advice | Error |
| `PHYSICAL_KIND_INCOMPATIBLE_WITH_ROLE`, `PHYSICAL_FIELD_NOT_APPLICABLE`, `PHYSICAL_NAME_INVALID`, `PROVISION_INDEX_KIND_MISSING`, `INDEX_SOURCE_NOT_DOCUMENT`, `PROVISION_PATH_INVALID`, `ID_CONFLICT`, `VECTOR_DIM_OUT_OF_RANGE`, `VECTOR_PARAMS_OUT_OF_RANGE`, `GEO_PARAMS_OUT_OF_RANGE`, `TIME_SERIES_PARAMS_INVALID`, `TABLE_PRIMARY_KEY_UNKNOWN`, `TABLE_COLUMN_DUPLICATE`, `TABLE_INDEX_COLUMN_UNKNOWN`, `PROFILE_KNOB_NOT_SUPPORTED_BY_RUNTIME` | – | Error where `physical` is set | Error |
| `SECURITY_NAME_INVALID`, `SECURITY_RESERVED_NAME`, `SECURITY_SYSTEM_GRANT_FORBIDDEN`, `SECURITY_GRANT_TARGET_INVALID`, `SECURITY_GRANT_UNKNOWN_OBJECT`, `SECURITY_PRINCIPAL_UNKNOWN_ROLE`, `SECURITY_DUPLICATE` | – | Error | – |
| `SECURITY_BROAD_GRANT`, `SECURITY_INDEX_GRANT_WITHOUT_SOURCE_READ` | – | Warning | – |

Plans add their own issues, whatever the spec version:

| code | severity | rule |
| --- | --- | --- |
| `PROVISION_PROFILE_UNRESOLVED` | Error | the profile does not resolve to built-in knobs |
| `PROVISION_DEPLOYMENT_UNSUPPORTED`, `PROVISION_REPLICATION_UNSUPPORTED`, `DOMAIN_MODE_INCOMPATIBLE_WITH_REPLICATION` | Error | outside single-node CP with one replica and one shard |
| `PROVISION_VOLATILE_ON_SURFACE`, `PROVISION_STORAGE_PATH_REQUIRED`, `PROFILE_DEPLOYMENT_MISMATCH` | Error | the medium does not fit the surface or target |
| `PROVISION_EXTENSION_UNAVAILABLE`, `EXTENSION_VERSION_INCOMPATIBLE` | Error | not a built-in extension manifest of this version |
| `PROVISION_EXTENSION_EXPERIMENTAL` | Warning | an experimental extension |
| `PROVISION_ENGINE_MISMATCH`, `PROVISION_FORMAT_MISMATCH`, `PROVISION_ENCRYPTION_MISMATCH` | Error | the database cannot be changed to the spec |
| `PROVISION_OBJECT_CONFLICT` | Error | a live object, role or principal differs and cannot be altered |
| `ID_CONFLICT` | Error | a pinned id is used by another live object |
| `PROVISION_RECORD_UNREADABLE`, `PROVISION_SECURITY_STATE_STALE`, `RBAC_SELF_LOCKOUT`, `AUDIT_REQUIRED_FOR_APPLY`, `PROVISION_AUDIT_KEY_MISMATCH` | Error | see above |
| `SECURITY_ROLE_ALREADY_ASSIGNED` | Warning | a role the plan creates or widens is already held by users the spec does not manage |
| `GUARANTEE_NOT_ENFORCEABLE` | Error | a guarantee this surface or database cannot enforce |
| `GUARANTEE_OPERATOR_MANAGED`, `AUDIT_DEVELOPMENT_KEY`, `ROLE_CONSTRAINT_NOT_ENFORCED` | Warning | how a guarantee is enforced |
| `DRIFT_UNMANAGED` | Warning, Error under `unmanaged: fail` | an undeclared, unmanaged live object |
| `DRIFT_ORPHAN`, `DRIFT_INTERRUPTED_APPLY` | Warning | a managed object is missing; the last apply did not finish |
| `STEP_REQUIRES_FLAG` | Error | a step needs a flag the options do not give |
| `PROVISION_FAILURE_NOT_RECORDED` | Warning | in an apply report: a failed apply could not write its `failed` record or audit event |
| `PROVISION_COMPLETION_NOT_AUDITED` | Warning | in an apply report: every step was applied and the record says `applied`, but `provision_apply_completed` could not be audited |
| `PROVISION_STORAGE_PATH_IGNORED` | Advice | on the Control Plane: the spec `deployment.storage_path` is not used, since the served database is provisioned |

Version 1 issues keep their order: the new version 1 Warnings come after every
existing issue. The physical checks of version 2 validation run only on
collections that set `physical`, so a version 2 spec without physical
parameters only gets `PHYSICAL_PARAMETER_REQUIRED` Advice; provisioning checks
every collection.

`config plan` (the version 1 migration planner) reports changes of
`collections[].physical`, `security` and `provisioning` as unsupported steps:
they are applied by provisioning, not by `config apply`.

## Rust API

- `multidb::provision::physical::project(spec, resolved)` returns the
  `DesiredObject`s (`name`, `spec_collection`, `DesiredKind` with runtime
  types such as `DocField`, `IndexSpec`, `TableSchema`, `RelIndexSpec`,
  `HnswParams` and `FieldPath`) in spec order, each collection followed by the
  objects it derives, or every blocking issue. It is always strict and never
  touches a database. `DesiredObject::params_json` shows the parameters in
  spec vocabulary with every default filled in, and
  `physical::time_series_config` builds the `TimeSeriesConfig` of a time
  series.
- `Database::create_table_with_layout(name, schema, indexes, layout)` creates
  a table with an explicit layout; `create_table` uses the database layout.
  `create_table_with_layout_as(principal, ...)` checks `Admin` on the database
  and is audited like `create_table_as`, with the layout in the event detail.
- `multidb::provision::plan_provision(spec, target, options, environment)`
  returns a `ProvisionPlan` (`plan_format` 1, `plan_id`, `surface`, `target`,
  `spec_digest`, `live_fingerprint`, `resolved_profile`, `validation`,
  `issues`, `objects`, `guarantees`, `drift`, `steps`, `spec_migration`,
  `outcome`, `required_flags`, `auto_approvable`, `impact`, `rollback`,
  `required_confirmation`; decoded with `deny_unknown_fields`).
  `provision::drift(db, spec, options)` returns a `DriftReport`, and
  `provision::export_spec(db)` an `ExportedSpec`. Errors are
  `ProvisionError`.
- `Database::provision_state()` returns the `ProvisionRecord`, if any, and
  `Database::provision_history()` its history, oldest first.
- `multidb::provision::apply_provision(db, actor, spec, confirmation,
  options, environment)` applies a spec to an open database and
  `provision_new_database(spec, path, confirmation, options, environment)`
  creates one (`Confirmation`, `ProvisionActor`, `ProvisionApplyReport`,
  `ProvisionOutcome`, `AppliedStep`, `StepOutcome`,
  `ProvisionRecordSummary`).
- `multidb::provision::open_from_spec(spec, path, mode, options,
  environment)` opens or creates the database of a spec at startup on the
  `rust_api` or `serve` surface (`StartupMode`, `StartupReport`,
  `StartupRefusal`, `ProvisionError::StartupRefused`), and the pure
  `serve_startup_decision(plan, mode)` returns its `StartupDecision`: `start`,
  `apply_then_start` or `refuse` with the reason (see [Serve](#serve)).
- `Database::set_security_state_as(principal, policy, principals)` replaces
  the RBAC policy and the principal registry in one persisted batch after
  checking `System:Admin`, audited as `set_security_state`.
- `config_spec::canonical_spec_json` and `config_spec::spec_digest` give the
  canonical form and digest of a spec, and `provision::live_fingerprint(db)`
  the fingerprint of the live state plans are computed against
  (`ProvisionPlan.live_fingerprint`).
- `admin::AdminState::with_spec_mode(spec_digest, apply_mode)` makes
  `GET /provision/state` report the spec a server started from; `multidb
  serve` sets it in spec mode.
- The spec types live in `multidb::config_spec`: `PhysicalObjectSpec`,
  `PhysicalKind`, `SecuritySpec`, `RoleSpec`, `GrantSpec`, `PrincipalSpec`,
  `ProvisioningPolicySpec`, `UnmanagedPolicy`, the pinned `DEFAULT_*`
  constants, the size limits and `allowed_kinds`, `full_text_index_name`,
  `geo_index_name` and `history_table_name`.
