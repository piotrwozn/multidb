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
- Transactional row writes (`MultiModelTxn::insert_row`, `update_row`,
  `delete_row`, including Control Plane row CRUD) to columnar tables with more
  than 1,024 rows no longer drop the rows stored beyond the first segment, and
  `MultiModelTxn::get_row` now finds them. Transactions read and rewrite every
  columnar segment of the table: from their snapshot under snapshot and
  serializable isolation, and from current committed data under read
  committed.

## 0.1.0-preview.1

- Initial public-preview target.
- Includes the public-preview support surface documented in
  `docs/support-matrix.md`.
