# Changelog

All notable public changes will be recorded here.

## Unreleased

- Prepared the repository for public preview publication.
- Added public repository hygiene files, security policy, contribution guide,
  issue templates and release notes.
- Kept local planning history out of the public source baseline.
- Full-text (`match`) and geo (`within_radius`) indexes are now backfilled when
  they are created and maintained in the same write batch as document inserts,
  updates and deletes made through collection handles, multi-model transactions
  and Control Plane document CRUD. Creating one now requires an existing
  document collection: a missing collection id or a vector collection is
  rejected. `refresh_full` is a bounded repair rebuild and `refresh_incremental`
  coalesces repeated changes to a document when repairing raw-key writes. Geo
  indexes accept integer `lon`/`lat` and skip invalid points (counted in
  `skipped_documents`) instead of failing, and searches no longer return
  zero-score postings or stale geo cells. Indexes created by earlier builds need
  one `refresh_full()`; Control Plane-only users can instead create a new index
  under a new name, which is backfilled on create. Because these searches
  return source document text, ids and points, `match()` and `within_radius()`
  through `query_as` now also require Read on the source collection, and are
  denied while that collection has row or masking policies. Index entries are
  computed from the document as written by the client, so a before-trigger
  that replaces a document leaves its index entries on the pre-trigger value.

## 0.1.0-preview.1

- Initial public-preview target.
- Includes the public-preview support surface documented in
  `docs/support-matrix.md`.
