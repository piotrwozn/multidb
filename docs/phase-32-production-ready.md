# Phase 32 Production Readiness

Phase 32 is implemented as a layered extension over the existing catalog,
DataFusion query providers, MVCC commit log, CDC subscriptions, and WASM
runtime. It keeps the crate `unsafe`-free and preserves the existing
single-node/CP transactional contract.

Implemented in this pass:

- Public modules `federation`, `temporal`, and `continuous`, exported from
  `lib.rs`.
- Catalog entries for `ForeignTable`, `MaterializedView`, and `TemporalTable`,
  with query registration through `SqlEngine`.
- Foreign table support for local Parquet, CSV, and JSON Lines object-store
  sources, plus PG-compatible remote descriptors using `tokio-postgres`.
  Sources store `SecretRef` metadata instead of secret values; named secrets
  return `Unsupported` until encrypted secret storage is provided.
- DataFusion providers for foreign tables, cataloged materialized views, and
  system-versioned history tables.
- MVCC-backed `query_as_of` and `query_as_of_as` for `TemporalPoint::Lsn` and
  `TemporalPoint::Timestamp`. AP and sharded backends explicitly return
  `Unsupported`; retention violations return `RetentionExpired`.
- System-versioned history views with generated `valid_from_lsn`,
  `valid_to_lsn`, `valid_from`, and `valid_to` columns.
- Cataloged incremental materialized views through
  `create_materialized_view_object(_as)`, preserving the existing
  `MaterializedViewSpec` and legacy `create_materialized_view(&self)` API.
- Continuous query metadata backed by durable CDC subscription state, with
  `create_continuous_query(_as)` and `ack_continuous_query(_as)`.
- Durable outbox connector metadata and idempotent outbox event keys of
  `(connector, lsn, target_key)`.
- WASM trigger/procedure metadata reusing the existing `WasmRuntime` module
  validation, fuel, memory, timeout, and module cache. Procedure calls execute
  WASM and host-validate declarative commands before applying them atomically
  as one replication batch.
- WASM trigger firing is wired into `Database::propose_batch`,
  `Database::propose_conditional_batch`, and `MultiModelTxn::commit` for
  cataloged relational tables and document collections. `BEFORE` triggers may
  accept, reject, or replace the candidate value; `AFTER` triggers execute after
  commit and must return `accept`.
- SQL intercepts for `SELECT ... AS OF LSN n`, `SELECT ... AS OF TIMESTAMP
  millis`, and `CALL proc(...)` with literal arguments.
- PG catalog compatibility coverage for foreign, materialized, and temporal
  queryable objects.

Verification refreshed on 2026-07-04:

- `cargo check --lib` passes.
- `cargo test --lib phase32 -- --nocapture` passes: 4 focused phase 32 tests.
- `cargo test --lib phase32_before_wasm_trigger -- --nocapture` passes: 3
  focused trigger firing tests.
- `cargo test --lib phase32_sql_declarations_fail_closed -- --nocapture`
  passes: unsupported Phase 32 DDL declarations return deterministic
  `Unsupported` messages before runtime side effects.
- `cargo test --lib` passes: 388 tests in the current workspace run.
- `cargo fmt --check` passes.
- `cargo clippy --locked --all-targets --all-features -- -D warnings`
  passes.
- `cargo test --locked --all-features` passes: 388 library tests, 14 CLI
  tests, and 2 doctests.
- `cargo deny check` passes with scoped duplicate-version exceptions for the
  `tokio-postgres` transitive stack.
- `cargo vet --locked` passes with the current exemption-based policy.
- `cargo audit --deny warnings` was not executed in this workspace because the
  `cargo-audit` subcommand is not installed.

Production notes:

- Join/window IVM remains intentionally bounded to the existing reversible
  materialized-view core in this pass. Unsupported shapes still return clear
  `Unsupported` errors rather than being planned optimistically.
- Foreign filter pushdown is conservative. Limit pushdown is active; row-level
  filters are still validated by DataFusion for correctness.
- Cross-source joins are statement-scoped and non-atomic across external
  sources.
- SQL DDL declarations for `CREATE FOREIGN TABLE`, `CREATE TRIGGER`,
  `CREATE PROCEDURE`, `CREATE CONTINUOUS QUERY`, `CREATE MATERIALIZED VIEW`,
  and `CREATE TEMPORAL TABLE` intentionally fail closed in the GA SQL matrix.
  Stable Rust APIs are the primary creation contract, with temporal `AS OF` and
  `CALL` available as SQL intercepts.
- Direct `RelTable` handles created outside the `Database` facade still write
  through their lower replication handle. Trigger firing is guaranteed through
  the `Database` facade and `MultiModelTxn` paths covered above.
