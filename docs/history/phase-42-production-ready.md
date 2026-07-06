# Phase 42 MultiDB Studio

Phase 42 adds MultiDB Studio as a separate operator-facing frontend over the
Phase 41 Control Plane API. Studio is a product-layer application, not core
database logic. V1 is intentionally read-only plus validation and migration
dry-run planning.

## Implemented Contracts

- `studio/` is a React, Vite and TypeScript application with its own npm
  lockfile.
- Studio connects to the admin API through `VITE_MULTIDB_API_BASE`, defaulting
  to `/api` for same-origin reverse proxy deployments.
- The admin bearer token is kept in memory only and sent as
  `Authorization: Bearer <token>`. Studio does not write tokens to
  `localStorage`.
- Studio supports the Phase 41 envelope contract:
  - `GET /status`,
  - `GET /config`,
  - `POST /config/validate`,
  - `POST /config/plan`,
  - `GET /profiles`, `/roles`, `/domains`,
  - `GET /extensions`,
  - `GET /advice`,
  - `GET /studio`.
- The UI contains overview, config, validation, migration dry-run, catalog,
  extensions and advice views.
- The migration view posts `{ current, desired }` to `/config/plan` and renders
  the returned plan id, validation state, impact, steps and rollback notes.

## Boundaries

Studio v1 does not execute config apply, mutate data, install extensions, edit
catalog objects, run query IDE workflows, or bypass validator/planner APIs.
The UI deliberately renders no apply button even though Phase 41 exposes
`/config/apply` for confirm/audit-only server-side checks.

Production deployments should serve Studio and the admin API behind the same
origin and route `/api/*` to `multidb admin serve`. Phase 42 does not add
permissive CORS to the Rust control plane.

## Acceptance Tests

Studio is covered by frontend unit/component tests, a Playwright smoke test,
CI integration and roadmap tests:

- API client tests unwrap success envelopes, surface failed envelopes and send
  bearer/principal headers.
- Component tests verify overview loading, visible validation failures,
  migration dry-run request shape and absence of an apply button.
- Playwright smoke test covers connect, overview, config validation, migration
  dry-run, catalog, extensions and advice with mocked Control Plane responses.
- `scripts/studio-check.ps1` runs `npm ci`, lint, typecheck, Vitest and build.
- CI installs Node, runs the Studio check, installs Chromium and runs the
  Playwright smoke.
- Phase 42 is marked `Complete`; phase 43 is complete for extension manifests,
  phase 44 is complete for Runtime Advisor V2, phase 45 is complete for CP
  Cluster GA, phase 46 is complete for Performance Truth, phase 47 is complete
  for templates, and phase 48 is complete for public preview packaging.

Run:

```powershell
.\scripts\studio-check.ps1
Push-Location studio; npm run e2e; Pop-Location
cargo test --lib roadmap -- --nocapture
.\scripts\check.ps1
```
