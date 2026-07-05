# Phase 41 Control Plane API

Phase 41 promotes the admin HTTP surface from health/status endpoints into a
production-ready control plane for configuration, catalog discovery, advice and
Studio integration. The API is authenticated, fail-closed, RBAC-aware and uses
the same `config_spec` contracts as the CLI.

## Implemented Contracts

- `multidb admin serve` mounts:
  - `GET /config`,
  - `POST /config/validate`,
  - `POST /config/plan`,
  - `POST /config/apply`,
  - `GET /profiles`, `/roles`, `/domains`, `/extensions`,
  - `GET /advice`,
  - `GET /studio`,
  - existing `/health`, `/ready`, `/status` and `/metrics`.
- All control-plane endpoints except `/health` and `/ready` require an admin
  bearer token. The token comes from `--admin-token` or
  `MULTIDB_ADMIN_TOKEN`.
- `--insecure-local-admin` is allowed only for loopback development binds.
- JSON API responses use stable envelopes:
  - success: `{ "ok": true, "data": ... }`,
  - error: `{ "ok": false, "error": { "code", "message" } }`.
- `POST /config/validate` and `POST /config/plan` are pure planning endpoints.
  Invalid validation or plan reports return HTTP 422.
- `POST /config/apply` calls `Database::confirm_config_apply_as`, requires
  `Permission::Admin` on `Resource::System`, writes an audit event and returns
  an `ApplyCheckReport`.
- `ApplyCheckReport` uses explicit no-mutation fields:
  `confirmation_matched`, `audit_recorded` and `data_mutated`.
- `/extensions` exposes the phase 41 extension capability catalog.
- `/studio` returns a capability manifest for Studio clients. It is not the
  Studio UI; the separate UI is implemented by phase 42.

## Boundaries

`/config/apply` is a confirmation and audit endpoint only. It never creates
collections, builds or drops indexes, installs extensions, rewrites data,
persists desired config, switches layouts, changes replication, or calls the
physical migration engine.

Plans that require physical or operator-managed migration return `unsupported`
with HTTP 422 even when the confirmation id matches. The response still reports
`data_mutated=false`.

The only storage mutation allowed through `/config/apply` in phase 41 is the
audit event. If audit is disabled, the operation fails closed.

## Acceptance Tests

The phase is covered by focused unit tests in `config_spec`, `db`, `admin`,
CLI tests in the `multidb` binary, and roadmap tests:

- apply check reports serialize `confirmed`, `rejected` and `unsupported`
  without claiming data mutation,
- `Database::confirm_config_apply_as` requires system admin RBAC and audits
  confirmed, rejected and unsupported outcomes,
- protected HTTP endpoints reject missing or invalid bearer tokens,
- catalog endpoints return enveloped JSON,
- invalid validation and plan reports return HTTP 422,
- `/config/apply` returns HTTP 422 for unsupported physical plans and HTTP 200
  for confirmed no-op/metadata-only plans,
- phase 41 is marked `Complete`; phase 42 owns Studio, phase 43 owns extension
  manifests, phase 44 owns Runtime Advisor V2, phase 45 owns CP Cluster GA,
  phase 46 owns Performance Truth, phase 47 adds templates, and phase 48 owns
  public preview packaging.

Run:

```powershell
cargo test --lib config_spec -- --nocapture
cargo test --lib admin -- --nocapture
cargo test --lib config_apply -- --nocapture
cargo test --bin multidb config_ -- --nocapture
cargo test --lib roadmap -- --nocapture
.\scripts\check.ps1
```
