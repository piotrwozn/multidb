# Analytics Template

Columnar analytics starter for events, metrics and aggregate scans.

## Why This Profile

Uses the stable analytics profile to make event logs, time-series and columnar aggregate intent explicit in DatabaseSpec.

## Generated Contract

- template: analytics
- spec_version: 2
- profile: analytics_columnar
- validation_status: Experimental
- validation_valid: true
- consistency_domain: primary (local_snapshot)
- replication: Cp

## Collections

- events: role=event_log indexes=primary - Append-oriented raw facts for replay and CDC.
- metrics: role=time_series indexes=time_series - Timestamped measurements for rollups.
  - physical: `{"chunk_millis":3600000}`
- aggregates: role=analytics indexes=columnar - Columnar aggregates for scans and reporting.
  - physical: `{"kind":"table","columns":[{"name":"id","type":"str"},{"name":"bucket","type":"str"},{"name":"metric","type":"str"},{"name":"value","type":"float"}],"primary_key":"id"}`

## Validate And Explain

```powershell
.\smoke.ps1
multidb config validate --spec .\multidb.yaml
multidb config explain --spec .\multidb.yaml
```

## Provision

This spec is version 2: it carries the physical parameters provisioning needs to build
every collection (see `docs/provisioning.md`).

`multidb init --guided --template analytics --name <name> --out <dir> --apply` writes
these files and creates the database in one step. From this directory:

```powershell
multidb config plan --spec .\multidb.yaml --db .\metrics_lake.redb
multidb config apply --spec .\multidb.yaml --db .\metrics_lake.redb --auto-approve
multidb config drift --spec .\multidb.yaml --db .\metrics_lake.redb
```

`config plan` is a dry run. `config apply --auto-approve` applies a plan that only
creates, adopts or records, and a second apply is a noop. `config drift` exits 3
when the database no longer matches the spec.

Guarantee statuses a CLI plan reports for this spec:

- `guarantees.audit.enabled`: `enforced_with_development_key`. Audit is on, but the
  CLI, `multidb serve` and the Control Plane chain audit events with the built-in
  development key (`AUDIT_DEVELOPMENT_KEY`); only the Rust API takes an audit key.
- `guarantees.backup.enabled` and `guarantees.backup.pitr`: `not_requested`.
- `guarantees.encryption.at_rest`: `not_requested`.

## Known Limits

- columnar_layout is currently Experimental in the extension catalog.
- This starter is optimized for read-heavy analytical paths, not OLTP writes.
- Table columns are fixed at creation; edit aggregates physical.columns before --apply.

Other languages should connect through MultiDB's PostgreSQL wire compatibility; the Rust API remains the first-class SDK surface for this starter.
