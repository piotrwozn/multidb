# HTTP access and data policies

The static admin bearer token is an operator or trusted gateway credential. It permits selecting a registered identity with `X-MultiDB-Principal`; it is not an identity-bound tenant token. Password-backed admin sessions ignore that header. Database permissions are checked for the selected identity.

`GET /catalog` returns only objects that identity may read. Table, collection and tensor counts are omitted (`row_count: null`) when row or masking policies protect the object. An unreadable policy configuration fails the request rather than exposing raw metadata.

For `GET /data/time-series/{name}/points`, a point's policy object has `series`, `timestamp_millis` and `value` fields. Row visibility is decided on the original object before masking. The response retains the existing `TimePoint` schema: an integer `timestamp_millis` and a finite numeric `value`. A mask that cannot produce that schema returns `403 forbidden`; original values are never substituted back. The low-level embedded time-series handle is a trusted storage API, not a principal-scoped HTTP endpoint.

Control-plane API authentication runs before JSON body collection. Request bodies retain the 1 MiB limit, have a 30-second collection deadline, and share a bound of 128 active API requests. A disconnected request keeps its capacity slot until any blocking storage/hash work actually finishes. Saturation returns `503 busy` with `Retry-After: 1`; an unfinished body returns `408 request_timeout`. Public health and readiness GETs do not consume that request capacity. PostgreSQL wire and metrics keep their separate connection and protocol budgets.

Studio responses disallow framing and inline scripts. The CSP retains HTTP/HTTPS API connections so the configurable API base continues to work. Deployments exposing the HTTP control plane outside a trusted network still need their configured TLS reverse proxy; the native PostgreSQL listener has its separate TLS configuration.

## Runtime probes

`/ready` combines the existing readiness hook with a cached atomic runtime
health snapshot. It does not read storage or key files, initialize a GPU, or
wait for the database query lock. A handle handed to the HTTP router while
already locked remains unready until its snapshot can be captured with a
nonblocking attempt.

A sealed audit sink or observed audit-key failure makes readiness return 503
with `reason: audit_failed` and `recovery: operator_required`. Restoring the
key does not silently unseal the current process. `/health` remains 200 so
Kubernetes does not restart repeatedly against a persistent corruption or key
problem. The operator must repair configuration/history and reopen the database.

For a profile requiring a GPU, an observed device stop makes both probes
return 503 with `reason: gpu_stopped` and `recovery: restart_required`. This is
a process-local failure for which restarting can reinitialize the device.
A required but unavailable/uninitialized device is an operator-required
readiness failure. An audit failure takes priority over a simultaneous GPU
stop to avoid an automatic restart loop against the persistent audit problem.
