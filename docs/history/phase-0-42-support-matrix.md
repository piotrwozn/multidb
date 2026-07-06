# Phase 0-49 GA/Preview Support Matrix

This snapshot is the GA/preview contract for phases 0-49. Roadmap entries are
complete only inside this checked-in support matrix. Phase 45 now covers the
local/process CP OpenRaft Cluster GA contract; Kubernetes automation,
multi-region placement and enterprise SLA remain outside the preview claim.

Global gates:

- `/config/apply` is confirm/audit-only and does not run physical data
  migration.
- Unsupported PostgreSQL, Mongo, SQL DDL, remote cluster, cloud KMS/HSM, and
  vendor SIEM surfaces fail closed with documented errors instead of executing
  partially.
- Kubernetes-first local operations are represented by kind, Helm, Vault dev,
  and MinIO artifacts under `ops/`.
- Fast verification is `cargo fmt --check`, clippy with all features, all
  feature tests, template smoke, preview smoke, cargo-deny, cargo-vet, ops
  smoke, upgrade smoke, and versioned perf gates. `cargo audit --deny warnings`
  is part of CI/release evidence when the subcommand is installed.

Phase evidence:

| Phase | GA evidence |
| --- | --- |
| 0 | `deny.toml`, `scripts/check.ps1`, license smoke |
| 1 | Storage conformance, crash/fault simulation, verification contracts |
| 2 | Profile metadata, replication kind metadata, fail-closed mappings |
| 3 | Document collection tests and conflict behavior bounded by backend |
| 4 | Document index scans and batch atomicity |
| 5 | SQL subset, DataFusion providers, deterministic unsupported SQL errors |
| 6 | Catalog and cross-model query tests |
| 7 | Snapshot isolation, write conflicts, retry behavior |
| 8 | Parquet round-trip, columnar SQL, benchmark smoke |
| 9 | HNSW tests, rebuild tests, metric ranking |
| 10 | TLS/SCRAM, pgwire subset, extended-query coverage |
| 11 | CP/AP contract tests and internal transport fail-closed Raft/admin RPCs |
| 12 | Health state, healing policies, CP membership adapter |
| 13 | Durable 2PC recovery tests and phase 13 recovery doc |
| 14 | `docs/history/phase-14-ops-ga.md`, kind, Helm, Vault, MinIO, ops smoke |
| 15 | ANALYZE stats, cost-based index-vs-scan, plan cache, EXPLAIN ANALYZE |
| 16 | Versioned perf baselines and release perf gate |
| 17 | Backup, incremental PITR, verify tests, local MinIO target |
| 18 | CDC changefeed, subscription ACK, hooks, materialized views |
| 19 | Full-text, time-series, graph, geo helpers with bounded unsupported scope |
| 20 | WASM UDF sandbox, codec, collation, policy validation |
| 21 | Tuning envelope, advisor, cooldown, rollback/audit |
| 22 | pg_catalog/info_schema subset, SQLSTATE, CSV/JSONL, BSON mapping |
| 23 | Cloud architecture evidence already complete |
| 24 | Hardening invariants, SimStorage, fault injection, fuzz scaffold |
| 25 | Canonical value/key format doc and tests |
| 26 | Transaction core doc and tests |
| 27 | Vault dev KEK, envelope DEK rotation, crypto-shred, audit JSONL |
| 28 | Query production-ready evidence already complete |
| 29 | Multi-model consistency doc, hook tests, WASM sandbox limits |
| 30 | Bounded distributed readiness, cluster APIs, Raft/admin frames |
| 31 | Advanced indexes and vector-columnar execution already complete |
| 32 | Federation, temporal, continuous query, WASM procedures/triggers |
| 33 | Formal verification and distributed testing already complete |
| 34 | Production-ready baseline already complete |
| 35 | Roadmap honesty baseline already complete |
| 36 | Configuration specification already complete |
| 37 | Profiles, roles, consistency domains already complete |
| 38 | Guarantee validator and policy compiler already complete |
| 39 | Explain config and migration planner already complete |
| 40 | CLI product layer already complete |
| 41 | Control plane API already complete |
| 42 | MultiDB Studio read-only UI, validation and migration dry-run already complete |
| 43 | Extension manifest contract, validator, full `/extensions` catalog and Studio rendering already complete |
| 44 | Runtime Advisor V2 report, dry-run refs, decision memory, CLI, Control Plane and Studio rendering already complete |
| 45 | CP OpenRaft Cluster GA smoke: leader handoff, minority rejection, durable membership metadata, read-index |
| 46 | Performance Truth envelope, versioned baselines, profile-aware gate and trend artifacts already complete |
| 47 | Template catalog, examples, template smoke tests and SDK/template guide already complete |
| 48 | Public preview guide, support matrix, known limits, preview smoke and release workflow hook already complete |
| 49 | Docker runtime image, compose quickstart, Helm parity, docker smoke and `multidb serve` already complete |
