# Quality and Performance Workflow

This project keeps fast correctness gates separate from heavier performance
evidence.

## Required local gate

Run before handing off code:

```powershell
.\scripts\check.ps1
```

This is the default acceptance gate for normal changes:

- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-features`
- `scripts/templates-smoke.ps1`
- `scripts/preview-smoke.ps1`
- `scripts/perf_gate.ps1 -SelfTest`
- `cargo deny check`

Document the result of the run in the handoff for a specific change. Do not turn
an old local pass into a timeless documentation claim.

## Refactor gate

For module moves and internal refactors, preserve public paths from `src/lib.rs`
and run the required local gate after each coherent slice. Prefer mechanical
moves first, then behavior changes in separate commits.

Large module boundaries should move toward responsibility-based files while
keeping outward behavior stable:

- `db`: config, catalog, authz/audit, profiles, database facade, tests.
- `query`: table storage, SQL parser/executor, DataFusion provider, optimizer
  integration, tests.
- `config_spec`: schema/types, validator, policy compiler, migration planner,
  extension catalog.

The first pre-Docker slice extracted `db::runtime` so fallible runtime setup is
kept out of the facade constructors while preserving public exports.

## Performance gate

Use `scripts/perf.ps1` to run the Criterion harnesses and write a profile-aware
performance report envelope:

```powershell
.\scripts\perf.ps1 -Profile local-smoke -Rows 1000 -Output target/perf/candidate.json
```

Compare a candidate report with the matching checked-in or archived baseline:

```powershell
.\scripts\perf_gate.ps1 -Baseline baselines/perf/local-smoke.json -Candidate target/perf/candidate.json -SummaryOutput target/perf/gate-summary.json
```

The gate validates profile compatibility by default. Use `-ThresholdPercent`
only for an explicit one-off override, and use `-AllowProfileMismatch` only for
manual investigation. The JSON report is intentionally small so it can travel
through CI and release artifacts. Criterion's detailed output remains in
`target/criterion/` for deeper analysis.

`scripts/perf.ps1` prebuilds each bench with `cargo bench --bench <name>
--no-run` before timing benchmark execution. Candidate reports record the
prebuild duration in `prebuild` and mark benchmark metadata as
`warm_wall_clock_cargo_bench`, so the gate compares runtime evidence instead of
cold Rust compilation. Use `-SkipPrebuild` only when deliberately investigating
the old cold-wall-clock behavior; those reports are marked
`wall_clock_cargo_bench_cold_possible`.

Generate a trend dashboard from two or more reports:

```powershell
.\scripts\perf_trend.ps1 -Reports baselines/perf/release-baseline.json,target/perf/release-candidate.json -Output target/perf/release-trend.json
```

## Support Claims

`docs/support-matrix.md` is the user-facing source for supported surfaces,
preview-only behavior and out-of-scope claims. Keep it aligned with release
gates and avoid turning a local benchmark or smoke run into a broad production
claim.

## Versioned Performance Baselines

The repository keeps explicit performance truth profiles in `baselines/perf/`:

- `local-smoke.json` is the developer smoke profile.
- `ci-gate.json` is the shared-runner gate profile.
- `release-baseline.json` is the release workflow gate profile.

Each baseline uses performance report envelope schema v1:

- `schema_version`, `profile` and `generated_at_utc` identify the report.
- `git` and `environment` record where the evidence came from.
- `thresholds` define throughput and latency regression limits.
- `benchmarks` carries the lightweight benchmark data.
- `calibration_status=smoke-minimum` marks the current baseline maturity.

`scripts/perf.ps1` writes candidate reports with benchmark names that match the
profiles, and `scripts/perf_gate.ps1` compares candidate reports against the
chosen baseline. The current release baseline is a smoke minimum until dedicated
production hardware baselines are calibrated; it is still the official release
blocker so threshold and baseline changes require review instead of relying on
`target/` artifacts.

Local results are diagnostic. CI and release results are policy-bearing only
when they compare a candidate envelope to the matching versioned baseline.

Release performance claims should point to versioned baselines and release
artifacts, not local `target/` output.
