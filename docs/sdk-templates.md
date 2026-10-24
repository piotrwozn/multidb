# MultiDB SDK And Template Guide

The starter path is:

```powershell
multidb template list
multidb template explain ai-memory --json
multidb init --guided --template ai-memory --name "Agent Memory" --out .\agent-memory --apply
```

Each template writes:

- `multidb.yaml` or `multidb.json`,
- `README.md`,
- `seed.json`,
- `smoke.ps1`.

The generated spec is the source of truth. Validate and explain it before
building application code around it:

```powershell
multidb config validate --spec .\agent-memory\multidb.yaml
multidb config explain --spec .\agent-memory\multidb.yaml
```

## Provisioning

Built-in templates emit spec version 2 with the `physical` parameters
provisioning needs: a document index path for every `document` index kind,
the vector dimension and metric, the time-series chunk width and retention,
and the columns of the analytics table. `init --apply` writes the four files
and then creates the database from the spec, at `--db`, the spec
`storage_path` or `<out dir>/<sanitized name>.redb`:

```powershell
multidb init --guided --template ai-memory --name "Agent Memory" --out .\agent-memory --apply
multidb config drift --spec .\agent-memory\multidb.yaml
multidb config apply --spec .\agent-memory\multidb.yaml --auto-approve
```

`config drift` exits 0 while the database matches the spec, and applying the
same spec again is a noop. A plan reports how each guarantee is enforced: on
the CLI, audit is `enforced_with_development_key` (the CLI has no audit key
option), and backup and PITR are `operator_managed`, with a runbook, only when
the spec asks for them. Values fixed at creation, such as the ai-memory vector
`dim` (1536) and the analytics table columns, must be edited before
`--apply`; provisioning never alters an object. `secure-saas` asks for
encryption at rest, which this build provisions through the Rust API only, so
`init --apply` writes its files and exits 2 with `GUARANTEE_NOT_ENFORCEABLE`.
See `docs/provisioning.md` for the plan, the flags and the exit codes.

## Rust First

Rust remains the first-class SDK surface for embedded and in-process usage. Use
the generated `DatabaseSpec` to choose the profile, collection roles and
extension requirements, then call the existing Rust APIs for documents,
relations, vectors, CDC, backup and advisor workflows.

The templates do not construct low-level `DbConfig` options or catalog objects
by hand. They teach the product layer first: profile, domains, roles,
validator, policy compiler and explain report. The spec then builds the
database: `multidb::provision::provision_new_database` creates it from the
spec (with a `LocalEnvelope` encryption key on the Rust API, which is how
`secure-saas` is provisioned), and `open_from_spec` opens it at startup and
checks it against the spec.

## Other Languages

Other languages should connect through MultiDB's PostgreSQL wire compatibility
where the requested workload fits that surface. Template docs should link users
back to `config validate` and `config explain` rather than duplicating
configuration rules in each client.

## Built-In Templates

- `game-save`: local game state, save documents (indexed on `slot`), key-value
  player state and replayable session events. The app chooses the database
  path; `init --apply` defaults it to `<out dir>/<name>.redb`.
- `desktop-embedded`: durable local documents (indexed on `title`), settings,
  audit and time-series usage metrics (one-hour chunks, 30-day retention).
- `ai-memory`: vector memories (1536 dimensions, cosine), document facts
  (indexed on `subject`), event context and scratch cache.
- `secure-saas`: strong CP intent, encrypted sensitive data, backup, PITR,
  audit and outbox events while keeping enterprise SLA concerns explicit;
  tenants are indexed on `tenant_id`. Provisioned through the Rust API only.
- `analytics`: event logs, time-series metrics (one-hour chunks) and a typed
  columnar aggregates table, with explicit experimental `columnar_layout`
  status.

The version 1 specs these templates emitted before are frozen under
`tests/fixtures/spec-v1` and keep validating; a version 1 spec with `document`,
`vector` or `columnar` index kinds has no physical parameters, so its
provisioning plan is blocked until `multidb config upgrade` and the missing
fields are added. `examples/provisioning` is a full version 2 example with
full-text and geo indexes, a versioned table, a columnar table in a row-class
database, roles and principals.
