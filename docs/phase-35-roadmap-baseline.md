# Phase 35 Roadmap Baseline

Phase 35 is a roadmap-honesty and repository-baseline pass. It does not add a
database feature. Its contract is that the project can describe what exists,
what still has a production gap, and what is only future roadmap work without
turning plans into product claims.

## Source Of Truth

- Rust code and tests are the source of truth for implemented behavior.
- `src/roadmap.rs` is the source of truth for phase readiness metadata.
- Local planning files are historical work packages and are intentionally kept
  outside the public source baseline.
- `docs/phase-*.md` files are human-readable snapshots of implemented or
  explicitly bounded phase contracts.
- `target/perf/*.json` files are local or CI artifacts. They are not release
  baselines unless they are compared against the phase 46 versioned baseline
  policy in `baselines/perf/`.

## Readiness Semantics

- `Complete` means the roadmap entry has implementation evidence and no known
  gaps in the readiness report.
- `ProductionGap` means implemented surface exists, but specific production work
  remains before the project should claim that phase as complete.
- `Deferred` means future roadmap work. It must not be counted as a current
  production gap or as an implemented runtime contract.

Phase 35 marks the product-layer continuation clearly: phases 36-48 are visible
in readiness metadata and checklist indexes, and each later phase owns its
current status in `src/roadmap.rs`. Phase 45 is now complete for the
local/process CP Cluster GA smoke contract; phase 48 is complete for public
preview packaging.

## Repository Baseline

The official project baseline is the Rust crate and its supporting project
material: `Cargo.toml`, `Cargo.lock`, `src/`, `docs/`, `scripts/`,
`.github/`, `benches/`, `fuzz/`, and `supply-chain/`.

Local machine artifacts are not project source. The repository ignores IDE
state, local AgentDB/Ruflo files, local vector database files, build outputs,
logs, and nested fuzz build output. A dirty or mostly untracked local worktree
must not be used as evidence that a phase is implemented or absent; readiness is
decided by code, tests, docs, and `src/roadmap.rs`.

## Boundaries

Phase 35 does not implement `DatabaseSpec`, a policy compiler, the configuration
control plane, Studio, SDK templates, or Cluster GA. Those later phases own
their own evidence in `src/roadmap.rs`.

## Acceptance

The phase is accepted when:

- `src/roadmap.rs` covers phases 0-48.
- Phase 35 is `Complete`.
- Later phase statuses are tracked individually in `src/roadmap.rs`.
- `production_gaps()` returns only current `ProductionGap` entries.
- Public docs cover the phase 35 baseline and phase 48 preview packaging.
- README and workflow docs describe required gates without evergreen pass/fail
  claims.
