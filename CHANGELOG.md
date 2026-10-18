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
- Made the encryption key lifecycle fail closed. `destroy_dek`/`crypto_shred`
  now refuse the current key, the last live key, and any key still used by
  stored values. The new `reencrypt_existing_values` (bounded, rerunnable
  batches) must run between `rotate_dek` and shredding old keys. KEK rotation
  now takes effect in-process and is atomic, and opening a non-empty database
  whose envelope keyring is missing fails instead of minting a new keyring.

## 0.1.0-preview.1

- Initial public-preview target.
- Includes the public-preview support surface documented in
  `docs/support-matrix.md`.
