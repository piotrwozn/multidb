# MultiDB SDK And Template Guide

The Phase 47 starter path is:

```powershell
multidb template list
multidb template explain ai-memory --json
multidb init --guided --template ai-memory --name "Agent Memory" --out .\agent-memory
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

## Rust First

Rust remains the first-class SDK surface for embedded and in-process usage. Use
the generated `DatabaseSpec` to choose the profile, collection roles and
extension requirements, then call the existing Rust APIs for documents,
relations, vectors, CDC, backup and advisor workflows.

The templates intentionally avoid constructing low-level `DbConfig` options by
hand. They teach the product layer first: profile, domains, roles, validator,
policy compiler and explain report.

## Other Languages

Other languages should connect through MultiDB's PostgreSQL wire compatibility
where the requested workload fits that surface. Template docs should link users
back to `config validate` and `config explain` rather than duplicating
configuration rules in each client.

## Built-In Templates

- `game-save`: local game state, save documents, key-value player state and
  replayable session events.
- `desktop-embedded`: durable local documents, settings, audit and time-series
  usage metrics.
- `ai-memory`: vector memories, document facts, event context and scratch cache.
- `secure-saas`: strong CP intent, encrypted sensitive data, backup, PITR,
  audit and outbox events while keeping enterprise SLA concerns explicit.
- `analytics`: event logs, time-series metrics and columnar aggregates with
  explicit experimental `columnar_layout` status.
