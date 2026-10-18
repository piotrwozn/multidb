# Changelog

All notable public changes will be recorded here.

## Unreleased

- Prepared the repository for public preview publication.
- Added public repository hygiene files, security policy, contribution guide,
  issue templates and release notes.
- Kept local planning history out of the public source baseline.
- Fixed `multidb serve` / `multidb admin serve` starting with an empty RBAC
  policy after a restart, and admin bootstrap/login overwriting stored roles,
  PG-user mappings and the audit flag; plain database openers now reload
  persisted security state and fail closed if it is corrupt.
- Columnar tiering no longer loses or resurrects rows written while a segment is
  being tiered: pointers are installed with a conditional write, and segments
  that changed are left alone and reported in `conflicted_segments`. Tiered
  objects use immutable, content-addressed paths, and `eventual_ap` databases
  are rejected. Full backups now inline CRC-checked tiered segment bytes, so
  restoring them does not depend on the object store, and fail if a tiered
  object is missing or corrupt. Hibernation uses full backups, so hibernating
  a tenant now downloads its tiered segments into the backup and resume
  restores them as local data until the next tiering run.

## 0.1.0-preview.1

- Initial public-preview target.
- Includes the public-preview support surface documented in
  `docs/support-matrix.md`.
