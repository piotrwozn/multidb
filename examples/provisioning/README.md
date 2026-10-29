# Provisioning Example

A version 2 `DatabaseSpec` that provisioning builds end to end: every
collection carries the `physical` parameters the runtime needs, plus a
managed role and principal. See `docs/provisioning.md` for the field
reference, the plan and the flags.

## What It Declares

- **Profile.** `orders_durable` extends the `high_durability` variant of
  `secure_app` (a row-class `HighDurability` database with two-phase commit),
  raises the cache to 8192 entries and adds the `vector_memory` and
  `analytics` roles to its certification.
- **orders** (`document_entity`): a document collection with two SQL fields,
  document indexes on `customer.id` and `status`, the full-text index
  `orders_notes_fts` on `notes` and the geo index
  `orders_shipping_location_geo` on `shipping.location` (precision 7). The
  index names are derived from the collection and the path. Both indexes are
  backfilled when they are created, and document writes keep them current.
- **ledger** (`event_log`): a typed row table with the primary key
  `entry_id`, a B-tree index on `order_id` and system versioning into
  `ledger_history`.
- **memories** (`vector_memory`): a 1536-dimension cosine vector collection
  with the default HNSW parameters (16/200/48).
- **daily_rollups** (`analytics`): a typed columnar table inside the
  row-class database; analytics tables default to the columnar layout.
- **security**: the role `order_writer` (read and write on `orders` and
  `ledger`, read on `orders_notes_fts`) and the user `orders_svc` mapped to
  it. Provisioning manages only these: other roles and principals, including
  the bootstrap `admin`, are left alone. A spec never carries a secret, so
  set the credentials of `orders_svc` separately.
- **provisioning**: objects that exist in the database but not in the spec
  are reported (`unmanaged: report`).

## Validate And Plan

```powershell
multidb config validate --spec .\multidb.yaml
multidb config plan --spec .\multidb.yaml --db .\orders.redb
```

Validation reports `valid=true status=Custom` with the Advice
`CUSTOM_PROFILE_DEFINED` and the Warning `EXPERIMENTAL_EXTENSION` for
`columnar_layout`. `config plan` is a dry run that creates no file: it lists
13 steps (the database, the four collections, the full-text and geo indexes
after their source, system versioning, the role and the principal, then the
metadata and the applied spec) with `outcome=ready auto_approvable=true`.

## Apply And Check

```powershell
multidb config apply --spec .\multidb.yaml --db .\orders.redb --auto-approve
multidb config drift --spec .\multidb.yaml --db .\orders.redb
multidb config status --db .\orders.redb
```

The plan only creates and records, so `--auto-approve` applies it; a
`--confirm <prov_id>` from the plan works as well. A second apply is a noop,
and `config drift` exits 0 while the database matches the spec.

Guarantee statuses a CLI plan reports for this spec:

- `guarantees.backup.enabled` and `guarantees.backup.pitr`:
  `operator_managed`. This build has no backup scheduler, so the plan prints
  the backup and restore runbook (`GUARANTEE_OPERATOR_MANAGED`).
- `guarantees.audit.enabled`: `enforced` with `MULTIDB_AUDIT_KEY_PATH`; without it,
  local CLI usage reports `enforced_with_development_key` (`AUDIT_DEVELOPMENT_KEY`).
  Production `multidb serve` requires an existing private audit key and, for durable
  profiles, `MULTIDB_AUDIT_ANCHOR_PATH`. The Control Plane inherits these runtime
  settings. Existing audit history requires its original signing key; changing the
  path does not re-sign it. See [audit key setup](../../docs/docker.md#audit-signing-and-restart).
- `guarantees.write_ack` and `guarantees.conflict_resolution`:
  `trivially_satisfied` on one replica.
- The `event_log` role of `ledger`: `not_enforced`. The runtime does not
  refuse updates to it (`ROLE_CONSTRAINT_NOT_ENFORCED`).

## Known Limits

- Provisioning creates and adopts objects but never drops or alters them:
  vector `dim`, table columns and index paths are fixed at creation, and
  removing a collection from the spec only releases it from management
  (`--allow-destructive collections.<name>`), keeping its data.
- Encryption at rest is not requested here; a spec that requests it is
  provisioned through the Rust API only.
- Raw key writes that bypass collection handles, transactions and Control
  Plane document CRUD do not reach the full-text and geo indexes of `orders`:
  call `refresh_incremental` or `refresh_full` on them after such writes.
