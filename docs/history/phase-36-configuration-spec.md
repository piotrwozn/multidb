# Phase 36 Configuration Specification

Phase 36 promotes configuration from low-level engine settings to a public,
versioned product contract. `DatabaseSpec` v1 is the stable shape that later
CLI, Control Plane, Studio, validators and migration tooling can share.

## Implemented Contracts

- Public module `config_spec`, exported from `lib.rs`.
- `DATABASE_SPEC_VERSION` is `1`; unknown versions fail structural validation.
- `DatabaseSpec` includes the product-level fields:
  `name`, `profile`, `deployment`, `defaults`, `guarantees`, `domains`,
  `collections`, `extensions`, `overrides`, and `operation_hints`.
- JSON serde is deterministic for the v1 contract:
  - structs reject unknown fields,
  - enums serialize as snake_case,
  - maps use `BTreeMap`.
- `DatabaseSpec::from_db_config` imports the existing low-level `DbConfig`
  compatibility view:
  - technical profiles map to stable v1 slugs such as `in_memory`,
    `transactional`, `time_series`, and `high_durability`,
  - `InMemory` maps to embedded deployment,
  - durable profiles map to single-node deployment,
  - storage paths are preserved as `deployment.storage_path`,
  - the default `primary` consistency domain is created.
- `DatabaseSpec::validate_structure` checks only v1 shape integrity:
  supported version, non-empty required names, unique domains, collections and
  extensions, valid collection domain references, and non-empty override/hint
  keys.
- `database_spec_v1_schema` exposes the checked-in JSON schema snapshot at
  `docs/schemas/database-spec-v1.schema.json`.

## Boundaries

Phase 36 does not parse YAML, add CLI commands, compile policy, explain
configuration decisions, or validate product guarantees. Phase 38 now owns the
guarantee validator and policy compiler contract; phases 39-40 continue with
explain, dry-run and the guided product CLI.

The module is intentionally pure data and validation. Reading or validating a
specification does not open storage, contact replication, or mutate a database.

## Acceptance Tests

The phase is covered by focused unit tests in `config_spec` and roadmap tests:

- JSON round-trip without information loss.
- Unknown JSON fields are rejected.
- Unknown spec versions are rejected.
- Duplicate domains and missing collection domains are rejected.
- The runtime schema equals the checked-in schema snapshot.
- Every existing `DbConfig` profile imports into a structurally valid
  `DatabaseSpec`.
- Phase 36 is `Complete` in `src/roadmap.rs`; later phase statuses are tracked
  individually by the roadmap source of truth.

Run:

```powershell
cargo test --lib config_spec -- --nocapture
cargo test --lib roadmap -- --nocapture
.\scripts\check.ps1
```
