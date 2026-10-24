# Generated Clients From OpenAPI

The stable OpenAPI document is available in the repository at
`docs/openapi/control-plane-v1.openapi.json` and from a running server at
`GET /openapi.json`.

Use the official SDKs when possible. For other languages, generate a client
from the OpenAPI file and preserve these rules:

- Use Bearer auth for every operation whose `security` array contains
  `BearerAuth`.
- Treat `/health`, `/ready` and `/openapi.json` as raw JSON, not Control Plane
  envelopes.
- Treat `/metrics` as preview raw `text/plain`.
- For enveloped endpoints, unwrap `{ "ok": true, "data": ... }`.
- Preserve `{ "ok": false, "error": { "code", "message" } }` as a typed error
  instead of flattening it into a generic HTTP exception.
- Do not hide backend limits. For example, config apply can be confirm/audit
  only and may return supported reports with HTTP 422. `POST /config/apply`
  stays audit-only (`data_mutated: false`) with either body: `{plan, confirm}`
  or `{current, desired, confirm}`, from which the server recomputes the plan.
- Keep the `data` of an error envelope. The preview provisioning endpoints
  answer a blocked plan, missing flags or a missing confirmation with HTTP 422
  and the plan in `data`, a changed plan with 409 `plan_changed`, and a
  partial apply with 500 `provision_partially_applied` and the apply report in
  `data`.

## Provisioning (preview)

The official SDKs wrap the provisioning endpoints of the served database (see
[provisioning.md](provisioning.md#control-plane)):

| endpoint | TypeScript | Go | Python | Rust |
| --- | --- | --- | --- | --- |
| `GET /provision/state` | `provisionState()` | `ProvisionState` | `provision_state` | `provision_state` |
| `POST /provision/plan` | `provisionPlan(spec, options?)` | `ProvisionPlan` | `provision_plan` | `provision_plan` |
| `POST /provision/apply` | `provisionApply(spec, confirm, options?)` | `ProvisionApply` | `provision_apply` | `provision_apply` |
| `POST /provision/drift` | `provisionDrift(spec, options?)` | `ProvisionDrift` | `provision_drift` | `provision_drift` |
| `GET /config/live` | `liveConfig()` | `LiveConfig` | `live_config` | `live_config` |

The TypeScript SDK types the results (`ProvisionPlan`, `ProvisionStep`,
`ProvisionApplyReport`, `DriftReport`, `GuaranteeEnforcement`,
`ProvisionState`, `LiveConfig`) and the options (`ProvisionOptions`); the Go,
Python and Rust SDKs return untyped JSON. An apply confirms the `prov_` id of
a plan computed with the same spec and options: the server recomputes the plan
and refuses an id that is no longer current, so pass the options of the plan
to `provisionApply` as well.

SQL-first applications can also use PostgreSQL wire compatibility instead of
the HTTP SDK when they do not need Control Plane operations.
