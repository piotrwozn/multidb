# Desktop Embedded Template

Durable embedded storage for desktop applications.

## Why This Profile

Uses the certified embedded desktop profile to keep local durability, document data and audit visibility in one validated spec.

## Generated Contract

- template: desktop-embedded
- spec_version: 2
- profile: desktop_app_embedded
- validation_status: Stable
- validation_valid: true
- consistency_domain: primary (local_snapshot)
- replication: Cp

## Collections

- documents: role=document_entity indexes=document - Durable user documents and settings payloads.
  - physical: `{"document_indexes":[{"path":"title"}]}`
- settings: role=key_value indexes=primary - Application preferences addressed by stable keys.
- audit_log: role=audit indexes=primary - Tamper-evident local operator and sync audit records.
- usage_metrics: role=time_series indexes=time_series - Local time-series telemetry with bounded retention.
  - physical: `{"chunk_millis":3600000,"retention_millis":2592000000}`

## Validate And Explain

```powershell
.\smoke.ps1
multidb config validate --spec .\multidb.yaml
multidb config explain --spec .\multidb.yaml
```

## Provision

This spec is version 2: it carries the physical parameters provisioning needs to build
every collection (see `docs/provisioning.md`).

`multidb init --guided --template desktop-embedded --name <name> --out <dir> --apply` writes
these files and creates the database in one step. From this directory:

```powershell
multidb config plan --spec .\multidb.yaml --db .\desk_notes.redb
multidb config apply --spec .\multidb.yaml --db .\desk_notes.redb --auto-approve
multidb config drift --spec .\multidb.yaml --db .\desk_notes.redb
```

`config plan` is a dry run. `config apply --auto-approve` applies a plan that only
creates, adopts or records, and a second apply is a noop. `config drift` exits 3
when the database no longer matches the spec.

Guarantee statuses a CLI plan reports for this spec:

- `guarantees.audit.enabled`: `enforced` with `MULTIDB_AUDIT_KEY_PATH`; without it,
  local CLI usage reports `enforced_with_development_key` (`AUDIT_DEVELOPMENT_KEY`).
  Production `multidb serve` requires an existing private audit key and, for durable
  profiles, `MULTIDB_AUDIT_ANCHOR_PATH`. The Control Plane inherits these runtime
  settings. Existing audit history requires its original signing key; changing the
  path does not re-sign it. Follow the repository's `docs/docker.md` audit setup.
- `guarantees.backup.enabled` and `guarantees.backup.pitr`: `not_requested`.
- `guarantees.encryption.at_rest`: `not_requested`.

## Known Limits

- The template is local-first and does not configure remote cluster membership.
- Application migrations still go through config plan and operator review.

Other languages should connect through MultiDB's PostgreSQL wire compatibility; the Rust API remains the first-class SDK surface for this starter.
