# Phase 52 SDK/API Ecosystem

Phase 52 makes the Control Plane API consumable as a stable public developer
contract. It adds OpenAPI v1, official publish-ready SDK packages, examples and
contract tests without adding new database behavior beyond serving the OpenAPI
document.

## Implemented Contracts

- `GET /openapi.json` and `/api/openapi.json` serve the checked-in OpenAPI v1
  document without auth and without touching database state.
- The Control Plane operation registry records method, path, operation id,
  auth requirement and stability for every public endpoint.
- `/studio` reports `openapi_endpoint`, the full operation registry and the
  `openapi_v1` capability.
- `GET /metrics` is explicitly preview raw `text/plain`; health, readiness and
  OpenAPI are raw responses while the rest of the Control Plane remains
  enveloped JSON.
- Official SDK packages exist for TypeScript, Python, Go and Rust with typed
  errors and the same method surface for auth, status, SQL, data CRUD, builder,
  config, security, audit, catalog, extensions and advice.
- Phase 53 adds SDK constants for `CONTROL_PLANE_API_VERSION = 1` and
  `MIN_MULTIDB_VERSION = "0.1.0"` without changing the Phase 52 API surface.
- Studio imports the local TypeScript SDK as its Control Plane client through
  `@multidb/client`.
- SDK examples cover login, table row write, SQL, document create, vector
  search, time-series insert/range and logout.

## Boundaries

Phase 52 does not publish packages to external registries, add ORMs, reimplement
PostgreSQL drivers, add Mongo wire compatibility, add SSO/billing/account
lifecycle APIs or change the config apply mutation contract. Publish-ready
means metadata, build/test/package dry-runs and examples are present in the
repo.

The Rust SDK default transport supports local `http://` Control Plane URLs. It
also exposes an injectable transport for production wrappers that need custom
TLS/proxy behavior.

## Acceptance Tests

Run:

```powershell
cargo test --lib phase52 -- --nocapture
.\scripts\sdk-smoke.ps1
.\scripts\sdk-examples-smoke.ps1 -RequireGo
.\scripts\studio-check.ps1
.\scripts\docker-smoke.ps1
cargo test --lib roadmap -- --nocapture
```

Local developer machines without Go can run `sdk-smoke.ps1` without
`-RequireGo`; CI installs Go and requires Go SDK/example tests.
