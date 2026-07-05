# Phase 39 Explain Config And Migration Planner

Phase 39 makes configuration changes auditable before they can be applied. It
adds a pure explain report for `DatabaseSpec` decisions, a deterministic
dry-run migration plan, and an apply confirmation check that refuses mismatched
or unsupported plans.

## Implemented Contracts

- `ConfigExplainer::explain` returns an `ExplainConfigReport` with:
  validation output, optional compiled policy, and operator-facing decisions.
- Explain decisions cover profile, deployment/storage path, replication,
  write acknowledgement, backup/PITR, encryption, audit, consistency domains,
  collection roles, indexes, extensions, runtime limits and compiler output.
- `MigrationPlanner::plan` compares current and desired `DatabaseSpec` values
  without opening storage or mutating a database.
- `MigrationPlan` includes a deterministic `plan_id`, validation reports,
  ordered steps, impact, rollback notes, `apply_supported`, and
  `required_confirmation`.
- `MigrationPlanner::check_apply` accepts only the exact `required_confirmation`
  for the same plan id. Invalid plans and unsupported physical migrations are
  rejected, and successful checks report `data_mutated=false`.
- `multidb config explain --spec <json> [--output text|json]` explains a spec.
- `multidb config plan --current <json> --desired <json> [--output text|json]`
  produces a dry-run plan.
- `multidb config apply --plan <json> --confirm <plan_id> [--output text|json]`
  checks the confirmation contract only; it does not execute data migration.

## Boundaries

Phase 39 remains JSON-only, matching phase 38. YAML parsing and guided product
CLI flows remain phase 40 work.

The planner is intentionally conservative. Profile changes, deployment/storage
path changes, replication/domain changes, collection role/index changes,
extension changes, encryption changes and other physical migrations are marked
as unsupported for automatic apply in v1. The plan is still useful as an audit
and operator checklist.

Dry-run and apply-check are pure planning contracts. They do not create
collections, build indexes, install extensions, move data, switch layouts, open
the database, or contact replication.

## Acceptance Tests

The phase is covered by focused unit tests in `config_spec`, CLI tests in the
`multidb` binary, and roadmap tests:

- valid explain reports include compiler decisions and required extensions,
- invalid specs keep validation diagnostics and do not produce a compiled
  policy,
- migration plan ids are deterministic and change with desired config content,
- high-risk changes carry impact, rollback and unsupported-apply metadata,
- apply checks reject mismatched confirmation ids and never claim data mutation,
- CLI JSON output covers explain, plan and apply rejection,
- YAML inputs for phase 39 config commands are rejected,
- phase 39 is marked `Complete`; phase 41 adds the authenticated HTTP
  confirm/audit surface over the same no-mutation contract.

Run:

```powershell
cargo test --lib config_spec -- --nocapture
cargo test --bin multidb config_ -- --nocapture
cargo test --lib roadmap -- --nocapture
.\scripts\check.ps1
```
