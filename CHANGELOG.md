# Changelog

All notable public changes will be recorded here.

## Unreleased

- Prepared the repository for public preview publication.
- Added public repository hygiene files, security policy, contribution guide,
  issue templates and release notes.
- Kept local planning history out of the public source baseline.
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
