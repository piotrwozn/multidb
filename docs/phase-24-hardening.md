# Phase 24 Hardening

Phase 24 turns reliability work into a permanent test surface. It does not
change user-facing data APIs; it adds shared invariants, deterministic
fault-injection, fuzz scaffolding, and persistent-format inventory so future
features can prove they did not weaken phases 0-23.

Implemented in this phase:

- `hardening` exposes `InvariantReport`, reusable invariant checks, deterministic
  trace digests, and a `FormatRegistry`.
- `SimStorage` is a third `StorageEngine` implementation for deterministic
  storage faults. With an empty `FaultPlan` it must pass the same conformance
  tests as memory and redb.
- Format inventory lists durable records and their version policy: metadata,
  value codec, backup manifests, transaction versions, AP versions, shard maps,
  cloud tier pointers and metadata, audit events, extension registry, and
  optimizer stats.
- Fuzz targets live in `fuzz/` and are intentionally outside the normal crate
  build. `scripts/fuzz-smoke.ps1` runs short smoke fuzzing when `cargo-fuzz` is
  installed.

Operational notes:

- Default PR gates stay fast: `cargo test` runs invariants, conformance, and
  deterministic fault tests.
- Long chaos/soak runs remain nightly or operator jobs. They should record seed,
  scenario name, trace digest, and invariant report for every failure.
- Found bugs should be minimized into normal unit tests or fixed seeds before
  being considered closed.

Persistent format registry:

| Format | Version Field | Current | Read Policy | Downgrade |
| --- | --- | ---: | --- | --- |
| Database metadata | `schema_version` | 1 | read 1 | allowed until new format written |
| Value codec | `MDBV/v1`; legacy JSON read-only migration source | 1 | read 1 plus legacy migration reads | restore from backup |
| Backup manifest | `format_version` | 1 | read 1 | restore from backup |
| Transaction versions | `__txn_versions/value-u64-be` | 1 | read 1 | allowed until new format written |
| AP versions | `__ap_versions/json` | 1 | read 1 | restore from backup |
| Shard map | `version` | 1 | read 1 | allowed until new format written |
| Cloud tier pointer | `MULTIDB_TIERED_SEGMENT_V1` | 1 | read 1 | restore from backup |
| Cloud segment metadata | `__cloud_segments/json` | 1 | read 1 | restore from backup |
| Audit event | integrity hash chain | 1 | read 1 | restore from backup |
| Extension registry | ABI version | 1 | read 1 | not supported |
| Optimizer stats | `stats_version` | 1 | read 1 | allowed until new format written |
