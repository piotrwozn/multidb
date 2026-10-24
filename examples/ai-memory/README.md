# AI Memory Template

Vector memory plus document facts and replayable context.

## Why This Profile

Uses the stable AI memory profile because vector collections, document facts, event context and cache data are all part of its support catalog.

## Generated Contract

- template: ai-memory
- spec_version: 2
- profile: ai_agent_memory
- validation_status: Stable
- validation_valid: true
- consistency_domain: primary (local_snapshot)
- replication: Cp

## Collections

- memories: role=vector_memory indexes=vector - Embedding memory for similarity lookup.
  - physical: `{"dim":1536,"metric":"cosine"}`
- facts: role=document_entity indexes=document - Structured facts and long-term notes.
  - physical: `{"document_indexes":[{"path":"subject"}]}`
- conversation_events: role=event_log indexes=primary - Replayable interaction history.
- scratch_cache: role=cache indexes=primary - Regenerable short-lived context.

## Validate And Explain

```powershell
.\smoke.ps1
multidb config validate --spec .\multidb.yaml
multidb config explain --spec .\multidb.yaml
```

## Provision

This spec is version 2: it carries the physical parameters provisioning needs to build
every collection (see `docs/provisioning.md`).

`multidb init --guided --template ai-memory --name <name> --out <dir> --apply` writes
these files and creates the database in one step. From this directory:

```powershell
multidb config plan --spec .\multidb.yaml --db .\agent_memory.redb
multidb config apply --spec .\multidb.yaml --db .\agent_memory.redb --auto-approve
multidb config drift --spec .\multidb.yaml --db .\agent_memory.redb
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

- Embedding generation is intentionally outside the database.
- Vector indexing can lag writes; inspect explain config before treating results as strongly fresh.
- dim is fixed at creation; edit physical.dim to match your embedding model before --apply.

Other languages should connect through MultiDB's PostgreSQL wire compatibility; the Rust API remains the first-class SDK surface for this starter.
