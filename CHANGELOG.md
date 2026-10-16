# Changelog

All notable public changes will be recorded here.

## Unreleased

- Prepared the repository for public preview publication.
- Added public repository hygiene files, security policy, contribution guide,
  issue templates and release notes.
- Kept local planning history out of the public source baseline.
- Incremental and point-in-time restore now replays commit records through the
  same routine as live commits. This rebuilds `__txn_mvcc` history and the HLC
  from each record, so transactional reads, inserts after a delete, document
  index maintenance and `AS OF LSN`/`AS OF TIMESTAMP` queries see replayed
  commits. Snapshot transactions now read base rows from their storage
  snapshot. Restore and verify reject broken backup chains: an LSN gap or
  overlap with the parent, a timeline mismatch, a parent directory holding a
  different `backup_id`, a full backup with a parent, a cycle, a commit-log gap
  inside an incremental, or a full payload whose LSN differs from its manifest.
  `RestoreTarget::Time` earlier than the full backup now fails with
  `Unsupported`, as an LSN target already did. Databases restored from
  incremental chains by earlier builds should be restored again to rebuild
  their MVCC history.

## 0.1.0-preview.1

- Initial public-preview target.
- Includes the public-preview support surface documented in
  `docs/support-matrix.md`.
