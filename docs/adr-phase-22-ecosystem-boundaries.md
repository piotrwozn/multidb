# ADR: Phase 22 Ecosystem Boundaries

## Decision

PostgreSQL wire compatibility is the only built-in client protocol for phase
22. MongoDB wire protocol and external connectors are deferred.

## Rationale

The database already has PostgreSQL wire support, SQL execution, authentication,
TLS, changefeeds, backup, and document storage. Extending the PostgreSQL path
gives users immediate access through existing drivers and tools. A Mongo wire
implementation would introduce a second command protocol, a second driver
compatibility matrix, and a second long-term semantic contract.

## Consequences

- Mongo users migrate through `mongodump`/BSON import and then use document APIs
  through multidb or SQL projections.
- Kafka/Grafana/BI connectors should live outside the core and consume stable
  changefeed/admin/export contracts.
- Public formats and admin responses are versioned by release notes and
  compatibility tests.

## Reversal Criteria

Reconsider Mongo wire protocol after repeated real user demand backed by at
least three production migration blockers that cannot be solved through import,
document APIs, or PostgreSQL-compatible access.
