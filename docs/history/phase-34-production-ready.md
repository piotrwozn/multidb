# Phase 34 Production Ready

Phase 34 closes the production gaps that were still hidden behind green
baseline checks: import correctness, bounded observability, live readiness,
cloud lease/fencing, quota accounting, PITR retention, self-tuning rollback,
and supply-chain release gates.

## Implemented Contracts

- Import options are operational:
  - `batch_size` commits rows through batched replication proposals.
  - `resume_token_path` checkpoints committed rows and treats identical
    duplicate primary keys as already imported.
  - `reject_path` writes JSONL records as `{line, raw, reason}`.
  - CSV and pg_dump COPY text validate arity and coerce values through table
    schema types.
- Observability is bounded:
  - the metrics registry enforces a maximum number of series,
  - high-cardinality values such as fingerprints, LSNs, byte sizes and counts
    are recorded as metric values or histograms instead of labels,
  - histograms render Prometheus buckets, sum/count and p50/p95/p99 quantiles,
  - the metrics server can bind explicitly and optionally require a bearer
    token, and render errors return HTTP 500 per request.
- Admin readiness is live:
  - `/health` remains liveness,
  - `/ready` uses a live database probe,
  - `multidb admin serve --bind ...` mounts `/health`, `/ready`, `/status`
    and `/metrics`.
- Cloud resume is guarded:
  - leases have TTL, owner checks, heartbeat and break-lease support,
  - guarded resume holds the lease for the session lifetime,
  - fencing tokens can be validated before cloud write paths,
  - hibernation markers are consumed once under lease,
  - backup GC retains PITR parent and descendant chains,
  - tenant quota storage is bootstrapped from existing data and deletes reduce
    accounted usage after commit.
- Self-tuning is rate-limited and reversible:
  - `cooldown_secs` and `max_changes_per_hour` are enforced from
    `__tuning_log`,
  - `Database::evaluate_tuning_regression` uses `RegressionGate` to trigger
    rollback and a system audit entry,
  - reprofile jobs are explicitly `PlanningOnly` until physical shadow copy
    and atomic switch are implemented.
- Supply chain gates are explicit:
  - CI uses `--locked`,
  - an MSRV 1.89 job checks all targets and features,
  - cargo-audit, cargo-deny, cargo-vet and CycloneDX SBOM generation run in CI,
  - GitHub Actions are pinned to commit SHAs,
  - release tags build reproducibly, emit checksums, sign the artifact with
    cosign and request GitHub provenance attestation.

## Acceptance Tests

The phase is covered by targeted regressions in:

- `cargo test --lib migration -- --nocapture`
- `cargo test --lib observability -- --nocapture`
- `cargo test --lib admin -- --nocapture`
- `cargo test --lib cloud -- --nocapture`
- `cargo test --lib tuning -- --nocapture`
- `cargo test --lib phase21_tuning -- --nocapture`
- `cargo vet --locked`

Final release acceptance remains the repository gate:

- `.\scripts\check.ps1`
- `.\scripts\fuzz-smoke.ps1`
- `.\scripts\perf_gate.ps1 -SelfTest`

## Boundaries

Phase 34 does not claim an external multi-host Jepsen suite, provider-specific
cloud service integration, or online physical reprofile rewrite. Those remain
separate hardening tracks. The production contract here is enforced through the
existing object-store trait, deterministic local tests, live probes and CI
release gates.
