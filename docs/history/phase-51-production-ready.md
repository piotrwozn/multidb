# Phase 51 Studio Full UI

Phase 51 hardens MultiDB Studio into the primary operator UI over the existing
Control Plane API. It does not add backend endpoints; it completes the browser
experience for the contracts delivered by phases 41, 42 and 50.

## Implemented Contracts

- Studio signs in with `POST /auth/login`, keeps the session token only in
  memory, calls `POST /auth/logout`, and treats 401 as session expiry.
- 403 responses are shown as forbidden errors without clearing the browser
  session.
- The dashboard shows health, readiness, runtime status, catalog counts,
  shard count, Studio manifest version and recent audit events.
- Data Explorer uses bounded pagination for tables and collections, validates
  JSON before writes, shows server capped-page state, and enables destructive
  deletes only after exact confirmation.
- Row deletion requires the table name as confirmation. Document deletion
  requires the document id as confirmation.
- SQL Console keeps an in-memory history, renders row outputs as tables and
  falls back to JSON for non-row outputs.
- Builder covers tables, collections, vectors, time-series, full-text, geo,
  graph and DatabaseSpec dry-runs without claiming unsupported physical apply.
- Security uses guided RBAC editing, dirty state, validation before save and
  warnings for broad System/Database Admin grants.
- Audit supports basic action, principal and outcome filters plus event detail
  expansion.
- Runtime Advice can load a dry-run plan and record a rejected decision with a
  reason through `/advice/plan` and `/advice/decision`. Studio still exposes no
  apply button.
- Desktop and mobile Playwright projects cover the critical operator flow.

## Boundaries

Phase 51 does not implement marketplace UI, billing, multi-tenant account
management, BI dashboards, OAuth/OIDC/SAML, enterprise schema editing or
physical migration apply. Config remains validate plus migration dry-run; the
UI repeats that topology, profile and replication changes are review/audit only
when unsupported by the runtime.

Secrets remain out of browser persistence. Theme/base URL may use local browser
state, but session tokens and passwords are in memory only.

## Acceptance Tests

Run:

```powershell
Push-Location studio
npm run lint
npm run typecheck
npm test
npm run build
npm run e2e
Pop-Location
cargo test --lib roadmap -- --nocapture
.\scripts\studio-check.ps1
```

CI already runs `scripts/studio-check.ps1` and `npm run e2e`; Phase 51 expands
the Playwright matrix to include a mobile Chromium project.
