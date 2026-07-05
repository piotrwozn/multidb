# Phase 33 Production Readiness

Phase 33 is implemented as the verification layer over phase 24 hardening and
the production-ready features through phase 32. It does not change database
semantics; it adds executable contracts, deterministic scenarios, model
checking, Jepsen-style history checking, fuzz targets, and CI/nightly gates.

Implemented in this pass:

- Public module `verification`, exported from `lib.rs`.
- `PhaseContract`, `ContractRegistry`, and `VerificationReport` as the stable
  contract inventory for executable phase 33 checks.
- `StorageModel`, `StorageAction`, and `DeterministicScenario` for running
  storage workloads against a naive `BTreeMap` oracle and `SimStorage` trace
  digests.
- `OperationRecord`, `History`, and `LinearizabilityChecker` for in-process
  Jepsen-style register/KV histories. The checker detects the known split-brain
  shape: a read after two completed writes returning the older value.
- `FaultPlan::seeded` and `FaultPlan::seeded_with_budget` for deterministic
  seed-driven simulation plans.
- Storage conformance now covers memory, redb, SimStorage, compressed,
  encrypted, compressed+encrypted, and `AnyEngine` wrappers.
- Stateright model checks for bounded CP single-slot quorum safety and 2PC
  decision safety.
- New fuzz targets: `pg_copy_text`, `keyenc_successor`, and
  `internal_request_frame`, bringing the committed smoke set to nine targets.
- CI runs the fast phase 33 gate on PRs; scheduled nightly verification runs
  model/fuzz/perf-gate smoke plus Miri and ThreadSanitizer smoke jobs.
- `scripts/perf.ps1` emits named wall-clock benchmark reports, and
  `scripts/perf_gate.ps1 -SelfTest` verifies that a synthetic 20% regression is
  caught.

Executable contract matrix:

| Contract | Oracle | Gate |
| --- | --- | --- |
| `storage-model` | `StorageModel` vs real storage contents | `cargo test --lib phase33` |
| `index-scan-full-scan` | sorted row-set equality | `cargo test --lib verification` |
| `temporal-mvcc` | temporal rows vs MVCC oracle rows | `cargo test --lib phase32 phase33` |
| `materialized-view-recompute` | materialized rows vs recomputation | `cargo test --lib verification cdc` |
| `codec-round-trip` | decoded bytes equal original bytes | `scripts/fuzz-smoke.ps1` |
| `commit-log-round-trip` | `txn` encode/decode equality | `cargo test --lib verification txn` |
| `pg-copy-text` | PostgreSQL COPY text parser | `cargo fuzz run pg_copy_text` |
| `linearizable-register` | `History` + register checker | `cargo test --lib phase33` |

Verification run on 2026-07-03:

- `cargo test --lib phase33 -- --nocapture` passes: 9 focused phase 33 tests.
- `cargo test --lib verification -- --nocapture` passes: 9 verification tests.
- `cargo test --lib hardening -- --nocapture` passes: 4 hardening tests.
- `cargo test --lib storage::conformance -- --nocapture` passes: 14 storage
  conformance tests across base engines and wrappers.
- `cargo test --lib repl::cp -- --nocapture` passes: 4 CP replication tests.
- `cargo test --lib repl::ap -- --nocapture` passes: 9 AP replication tests.
- `cargo fmt --check` passes.
- `cargo clippy --all-targets --all-features -- -D warnings` passes.
- `cargo test --all-features` passes: 327 unit/integration tests and 2 doctests.
- `cargo deny check` passes.
- `./scripts/fuzz-smoke.ps1` passes: 9 targets, including `pg_copy_text`,
  `keyenc_successor`, and `internal_request_frame`.
- `./scripts/perf_gate.ps1 -SelfTest` passes and catches the synthetic 20%
  regression.

Production notes:

- The Jepsen-style harness is intentionally in-process and trait-based in this
  phase because the repository does not yet contain a Docker/testcontainers
  multi-host cluster harness. Full external nemesis runs remain a nightly or
  operator-level extension.
- Miri and ThreadSanitizer are scheduled smoke jobs rather than PR gates. They
  are kept out of the fast path to avoid turning deterministic verification into
  a flaky default workflow.
- Stateright is a dev-dependency only. It verifies bounded protocol models; it
  complements, but does not replace, the executable code-level history and
  storage-oracle tests.
