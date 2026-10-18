# Changelog

All notable public changes will be recorded here.

## Unreleased

- Hardened Control Plane `admin` login: failed password checks now lock out
  only the client IP (IPv6 /64) that made them, under a server-wide soft
  ceiling of 20x `MULTIDB_ADMIN_LOGIN_MAX_FAILURES` per window. Other usernames
  no longer lock out `admin`. At most 2 Argon2 password checks run at once, and
  extra login or change-password requests get `503 auth_busy` with
  `Retry-After: 1`. Usernames over 256 bytes and passwords over 1024 bytes are
  rejected with `400`. A `MULTIDB_ADMIN_PASSWORD` longer than 1024 bytes stops
  startup when it would be stored (no stored credential, or a reset). Rate-limited refusals
  and failures for unknown usernames are coalesced in the audit log.
- Prepared the repository for public preview publication.
- Added public repository hygiene files, security policy, contribution guide,
  issue templates and release notes.
- Kept local planning history out of the public source baseline.
- Fixed `multidb serve` / `multidb admin serve` starting with an empty RBAC
  policy after a restart, and admin bootstrap/login overwriting stored roles,
  PG-user mappings and the audit flag; plain database openers now reload
  persisted security state and fail closed if it is corrupt.
- Time-series retention now computes its cutoff from server time instead of
  the incoming point's timestamp, so one future-dated or microsecond-scaled
  point can no longer delete a whole series. Points more than
  `max_future_skew_millis` ahead of server time (default 300000 ms for new
  collections, settable when the collection is created; collections created
  before this change keep accepting future timestamps) and points older than
  `now - retention_millis` are rejected. New collections require a positive
  `retention_millis` that is at least `chunk_millis`; existing collections with
  a non-positive retention stay readable but refuse writes. Time-series
  validation errors from the Control Plane now return HTTP 400
  `invalid_value` instead of 500, and `time_bucket` and range queries no longer
  overflow near the `i64` bounds.
- All user writes now run one write pipeline (before triggers, before-commit
  hooks, tenant quota, commit, after-commit hooks and after triggers). SQL and
  pgwire `INSERT`, `RelTable` handles and stored procedures (`CALL`) now run
  WASM triggers, hooks and tenant quotas like `Database` writes do.
  `MultiModelTxn` commits (including Control Plane row and document CRUD) now
  also enforce tenant quotas and the tenant write limit.
- A committed write is never reported as failed. After-commit hook and
  after-trigger failures are logged, counted in
  `multidb_post_commit_failures_total{stage}`, recorded in the
  `__post_commit_failures` table and listed by
  `Database::post_commit_failures()`. Previously a client could see an error for
  a durable write, retry, and create a duplicate.
- `HookTarget::Collection`, `VectorCollection` and `Graph` hooks only match
  writes to the named object. Registering such a hook for a missing object, or
  for an object of another kind, fails with `HookError::InvalidTarget`
  (SQLSTATE `42704`).
- A before trigger `replace` result is validated against the table schema, must
  keep the primary key, and rebuilds the row's secondary-index entries. Replacing
  documents or a row written twice in one batch is rejected. SQL `RETURNING`
  still shows the row as written by the statement, before a trigger `replace`.
- WASM triggers can no longer be registered on columnar tables. A columnar
  write replaces a whole segment, so insert, update and delete events cannot be
  told apart. Triggers stored earlier still cannot replace columnar segments.
- Writes rejected by a before trigger or a before-commit hook fail with the new
  `ReplError::Rejected` variant, reported over pgwire as SQLSTATE `P0001`
  instead of the connection-failure code `08006`, so connection pools keep the
  connection.
- Removed `cdc::HookedReplication` and `cloud::QuotaReplication`; the write
  pipeline replaces them.

## 0.1.0-preview.1

- Initial public-preview target.
- Includes the public-preview support surface documented in
  `docs/support-matrix.md`.
