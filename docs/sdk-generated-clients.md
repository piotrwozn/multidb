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
  only and may return supported reports with HTTP 422.

SQL-first applications can also use PostgreSQL wire compatibility instead of
the HTTP SDK when they do not need Control Plane operations.
