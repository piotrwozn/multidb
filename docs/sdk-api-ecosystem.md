# SDK And API Ecosystem

The Control Plane API v1 is the stable developer contract for the current
preview. The HTTP source of truth is `docs/openapi/control-plane-v1.openapi.json`, served at
`GET /openapi.json` and `GET /api/openapi.json`.

## Official SDKs

Official publish-ready packages live in `sdk/`:

| Language | Package | Path | Notes |
| --- | --- | --- | --- |
| TypeScript | `@multidb/client` | `sdk/typescript` | ESM, Node 20+, fetch-injectable |
| Python | `multidb-client` | `sdk/python` | sync client, stdlib runtime |
| Go | `github.com/multidb/multidb/sdk/go` | `sdk/go` | context-aware methods |
| Rust | `multidb-client` | `sdk/rust` | blocking client, injectable transport |

Every SDK exposes the same v1 flow: login, auth/me, health/readiness/status,
SQL, table rows, documents, vectors, time-series, builder endpoints, config
validate/plan/apply, security, audit, profiles, roles, domains, extensions and
runtime advice.

Every SDK also exposes compatibility constants:

- `CONTROL_PLANE_API_VERSION = 1`
- `MIN_MULTIDB_VERSION = "0.1.0"`

SDK errors preserve the HTTP status, stable server `code`, message and raw
body where the language makes that practical. Client-side decode failures use
`invalid_json`; non-envelope JSON from enveloped endpoints uses
`invalid_envelope`.

## Local Runtime

Docker Compose exposes Studio and the Control Plane on
`http://127.0.0.1:8080`. SDK examples use:

```powershell
$env:MULTIDB_CONTROL_PLANE_URL = "http://127.0.0.1:8080/api"
$env:MULTIDB_ADMIN_PASSWORD = "local-dev-admin-password"
```

Run the package gate:

```powershell
.\scripts\sdk-smoke.ps1
```

Run the end-to-end examples against an isolated Docker Compose project:

```powershell
.\scripts\sdk-examples-smoke.ps1
```

CI runs the same gates with Go required.

Release candidates run both SDK gates before publishing signed binary and
Docker artifacts.
