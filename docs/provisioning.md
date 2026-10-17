# Provisioning

Status: **Preview**. Provisioning builds and converges a database from a
`DatabaseSpec`. This page grows with it: this build adds the spec version 2
provisioning fields, their validation, the physical projection
(`multidb::provision::physical::project`) and per-table layout
(`Database::create_table_with_layout`). Planning, applying, drift and export
come in later changes; until then nothing creates or changes a database from a
spec, and `config apply --plan` stays audit-only.

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
| `time_series` | `create_time_series` | `chunk_millis` (1000..=2678400000; `time_series.default_chunk_millis`, 3600000 in every preset); `retention_millis` (> 0 and at least one chunk; `time_series.default_retention_millis`, none in every preset) |
| `graph` | `create_graph`, or `create_graph_auto_id` | `graph_id` |

A document collection also derives:

- one full-text index per `physical.full_text` entry (`{path, name,
  language: simple, refresh_lag_target: 1}`), built with
  `create_full_text_index` on the collection;
- one geo index per `physical.geo` entry (`{path, name, precision: 6}`,
  precision 1..=12), built with `create_geo_index`.

A typed row table with system versioning derives `<table>_history`
(`enable_system_versioning` with `keep_history`). Versioning comes from
`physical.system_versioning`, or else from the profile knob
`history.system_versioning: typed_tables`, which applies to every typed row
table; `keep_history` defaults to `history.keep_history`.

Field types are `int`, `float`, `str`, `bool` and `bytes`. The full-text
`language` accepts only `simple`, because this build tokenizes every language
the same way. `physical.max_future_skew_millis` is reserved: the runtime has no
future-skew bound yet, so it is refused with
`PROFILE_KNOB_NOT_SUPPORTED_BY_RUNTIME`.

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
templates are blocked: their `document`, `vector` and `columnar` index kinds
carry no paths, dimension or columns.

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
  build a grant on the whole database also covers system administration
  (provisioning, RBAC and audit), so `resource: database` with `admin` is the
  Warning `SECURITY_BROAD_GRANT`.
- **Index grants.** Searching a full-text or geo index also needs `read` on
  its source collection; a role that can read the index but not the source
  gets the Warning `SECURITY_INDEX_GRANT_WITHOUT_SOURCE_READ`.
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

## Validation codes

| code | version 1 | version 2 | provisioning |
| --- | --- | --- | --- |
| `FIELD_REQUIRES_SPEC_V2` | Error | – | – |
| `COLLECTION_NAME_INVALID`, `COLLECTION_NAME_RESERVED`, `OBJECT_NAME_COLLISION`, `SPEC_LIMIT_EXCEEDED` | Warning | Error | Error |
| `PHYSICAL_PARAMETER_REQUIRED` | – | Advice | Error |
| `PHYSICAL_KIND_INCOMPATIBLE_WITH_ROLE`, `PHYSICAL_FIELD_NOT_APPLICABLE`, `PHYSICAL_NAME_INVALID`, `PROVISION_INDEX_KIND_MISSING`, `INDEX_SOURCE_NOT_DOCUMENT`, `PROVISION_PATH_INVALID`, `ID_CONFLICT`, `VECTOR_DIM_OUT_OF_RANGE`, `VECTOR_PARAMS_OUT_OF_RANGE`, `GEO_PARAMS_OUT_OF_RANGE`, `TIME_SERIES_PARAMS_INVALID`, `TABLE_PRIMARY_KEY_UNKNOWN`, `TABLE_COLUMN_DUPLICATE`, `TABLE_INDEX_COLUMN_UNKNOWN`, `PROFILE_KNOB_NOT_SUPPORTED_BY_RUNTIME` | – | Error where `physical` is set | Error |
| `SECURITY_NAME_INVALID`, `SECURITY_RESERVED_NAME`, `SECURITY_SYSTEM_GRANT_FORBIDDEN`, `SECURITY_GRANT_TARGET_INVALID`, `SECURITY_GRANT_UNKNOWN_OBJECT`, `SECURITY_PRINCIPAL_UNKNOWN_ROLE`, `SECURITY_DUPLICATE` | – | Error | – |
| `SECURITY_BROAD_GRANT`, `SECURITY_INDEX_GRANT_WITHOUT_SOURCE_READ` | – | Warning | – |

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
- The spec types live in `multidb::config_spec`: `PhysicalObjectSpec`,
  `PhysicalKind`, `SecuritySpec`, `RoleSpec`, `GrantSpec`, `PrincipalSpec`,
  `ProvisioningPolicySpec`, `UnmanagedPolicy`, the pinned `DEFAULT_*`
  constants, the size limits and `allowed_kinds`, `full_text_index_name`,
  `geo_index_name` and `history_table_name`.
