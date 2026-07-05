# Phase 44 Runtime Advisor V2

Phase 44 promotes advice from raw index suggestions into a production-ready
operator contract. Runtime Advisor V2 explains recommendations, attaches cost,
risk, expected gain, rollback conditions and a migration dry-run, and remains
read-only by default.

## Implemented Contracts

- `runtime_advisor` exposes a versioned `RuntimeAdviceReport` with
  `schema_version`, `sources`, `suppressed_recommendations`,
  `auto_apply_enabled=false` and typed recommendations.
- Each `RuntimeAdvice` includes:
  - stable `id` and `code`,
  - human-readable message and rationale,
  - `AdviceCost`, `RiskLevel`, expected gain and rollback conditions,
  - a `MigrationPlanRef` containing `plan_id`, operation hint, CLI command,
    control-plane endpoint and the full `MigrationPlan`.
- Advice generation combines:
  - phase 21 workload/index advice,
  - phase 15/28 planner feedback,
  - phase 38 guarantee validation,
  - phase 39 migration planning,
  - phase 43 extension catalog metadata,
  - checked-in performance baseline metadata.
- Runtime index advice is represented as `operation_hints` such as
  `advisor.index.create.<table>.<column>`, so `DatabaseSpec` remains backward
  compatible.
- Rejected advice is persisted in `__runtime_advice_decisions` and suppressed
  for 24 hours. Accepted/rejected decisions are explicit; no automatic apply is
  triggered.
- `Database` exposes `runtime_advice`, `runtime_advice_as`,
  `runtime_advice_plan_as` and `record_runtime_advice_decision_as`.
- The Control Plane exposes:
  - `GET /advice`,
  - `POST /advice/plan`,
  - `POST /advice/decision`.
- The CLI exposes:
  - `multidb advice list`,
  - `multidb advice plan`,
  - `multidb advice reject`.
- Studio renders Runtime Advisor cards with risk, cost, expected gain, plan id
  and operation hint. It still renders no apply button.

## Boundaries

Runtime Advisor V2 does not create indexes, drop indexes, install extensions,
rewrite data, change collection roles, refresh statistics, enable backups,
enable encryption or apply configuration changes by itself.

`/advice/decision` records operator intent and audit metadata only. Applying a
recommendation still goes through the migration dry-run and explicit operator
workflow. Physical online migration remains future phase work.

Performance baselines are listed as a source of evidence for advice context,
and phase 46 owns release-grade performance truth, trend dashboards and
release-blocking perf policy.

## Acceptance Tests

- Missing-index workload produces costed advice with a valid dry-run through
  `operation_hints`.
- Rejected advice is suppressed for the 24-hour decision window.
- Validator-backed advice is skipped when the desired spec would still fail
  validation.
- Planner feedback produces refresh-statistics advice.
- Admin `/advice`, `/advice/plan` and `/advice/decision` return enveloped JSON
  and audit decisions.
- CLI `advice list`, `advice plan --out` and `advice reject` support JSON.
- Studio renders Runtime Advisor cards and still has no apply button.
- Phase 44 is marked `Complete`; phase 45 is complete for CP Cluster GA, phase
  46 is complete for Performance Truth, phase 47 is complete for templates, and
  phase 48 is complete for public preview packaging.

Run:

```powershell
cargo test --lib runtime_advisor -- --nocapture
cargo test --lib admin -- --nocapture
cargo test --bin multidb advice_ -- --nocapture
cargo test --lib roadmap -- --nocapture
.\scripts\studio-check.ps1
.\scripts\check.ps1
```
