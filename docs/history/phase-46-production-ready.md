# Phase 46 Performance Truth

Phase 46 turns performance evidence into a versioned release contract. Local
Criterion output remains useful diagnostics, while checked-in performance
baselines and profile-aware gates define the official comparison point.

## Implemented Contracts

- `performance` exposes `PerformanceTruthProfile`, `PerformanceThresholds` and
  `PerformanceReportEnvelope` as the public v1 report schema.
- `scripts/perf.ps1` writes envelope reports for:
  - `local-smoke`,
  - `ci-gate`,
  - `release-baseline`.
- Each envelope records schema version, profile, UTC timestamp, git metadata,
  runtime environment, thresholds and benchmark reports.
- `baselines/perf/*.json` are versioned envelope baselines. They currently use
  `calibration_status=smoke-minimum` and keep the existing smoke thresholds.
- `scripts/perf_gate.ps1` accepts old array reports and envelope reports,
  validates profile compatibility by default, compares throughput plus p50,
  p95 and p99, and can write a gate summary JSON.
- `scripts/perf_trend.ps1` produces a compact trend dashboard JSON for release
  artifacts and historical comparison.
- CI runs a `ci-gate` candidate report and publishes perf JSON artifacts.
- Release builds run the `release-baseline` gate and attach candidate, gate
  summary and trend reports to the release.

## Release Policy

The official release blocker is the selected profile baseline in
`baselines/perf/`, not whatever happens to be present under `target/`.

`release-baseline.json` is the release workflow baseline. It is intentionally a
smoke minimum until a dedicated hardware baseline is calibrated, but it is still
versioned and reviewed. Changes to thresholds or baseline values must be code
reviewed like any other release policy change.

`local-smoke.json` is for developer feedback. A local failure is a signal to
inspect, rerun or compare on a cleaner machine; it is not automatically a
release veto.

`ci-gate.json` is for shared runners. It catches large regressions and verifies
that the performance reporting path still works in automation.

## Boundaries

Phase 46 does not add new benchmark workloads. It formalizes the existing
`performance_micro`, `columnar_aggregation` and phase 33 harness marker as a
stable v1 evidence path.

Criterion's detailed measurements remain under `target/criterion/` and are not
committed. Release decisions use the envelope reports and checked-in baselines.

The current release baseline is not a claim that production hardware has been
fully calibrated. Its contract is narrower: official baseline identity,
repeatable gate execution, summary artifacts and trend visibility.

## Acceptance Tests

Run:

```powershell
cargo test --lib performance -- --nocapture
cargo test --lib roadmap -- --nocapture
.\scripts\perf_gate.ps1 -SelfTest
.\scripts\perf_trend.ps1 -Reports baselines/perf/local-smoke.json,baselines/perf/ci-gate.json -Output target/perf/trend-selftest.json
.\scripts\check.ps1
```

Optional local smoke:

```powershell
.\scripts\perf.ps1 -Profile local-smoke -Rows 1000 -Output target/perf/local-candidate.json
.\scripts\perf_gate.ps1 -Baseline baselines/perf/local-smoke.json -Candidate target/perf/local-candidate.json -SummaryOutput target/perf/local-gate-summary.json
```
