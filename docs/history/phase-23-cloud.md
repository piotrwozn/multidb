# Phase 23 Cloud Architecture

Phase 23 adds the production cloud-facing layer to multidb without changing the
storage contract used by documents, relations, vectors, time-series, and SQL.

Implemented in this phase:

- `CloudObjectStore` is the provider seam. The default build includes a
  deterministic `file://` implementation used by tests and local deployments;
  non-local providers are opened through the same config path and can be added
  behind features without changing model code.
- Tiered storage for columnar segments. A local Parquet segment can be
  uploaded to an object-store URI and replaced by a checked pointer. Reads are
  transparent and verify CRC32 before handing bytes to the Parquet reader.
- Tiered objects are content-addressed at
  `segments/rel_columnar_segments/<segment-key-hex>/<blake3>.parquet` and are
  never rewritten with different bytes; re-tiering identical bytes reuses the
  existing object. Pointers written by earlier builds to the legacy
  `segments/rel_columnar_segments/<segment-key-hex>.parquet` layout stay
  readable because every pointer stores its own path.
- The pointer is installed with a conditional write that requires the segment to
  still hold exactly the uploaded bytes. Segments rewritten or deleted by
  concurrent DML while they are being tiered are left as they are and counted in
  `TieringReport::conflicted_segments`; the next tiering run picks up the new
  content.
- Tiering requires conditional writes (single-node, `strong_cp`, and sharded
  databases with CP or local shards). `eventual_ap` databases are rejected with
  `CloudError::Unsupported` before any upload.
- Tiering state is persisted in `__cloud_segments` with `Local`, `Uploading`,
  `Remote`, `DeletePending`, and `Failed` states. Interrupted uploads are
  recovered idempotently: a valid remote object is finalized only when the
  segment still holds the uploaded bytes (or already holds the same pointer);
  otherwise, and when the object is missing or corrupt, the metadata is
  reconciled with the current segment value (local, remote, or failed when the
  segment no longer exists).
- Full backups (and therefore hibernation and `multidb backup`) fetch,
  CRC-check, and inline tiered segment bytes, together with the matching current
  `__txn_mvcc` versions. They need the object store to be reachable and fail
  closed when an object is missing or corrupt. Restoring a full backup then needs
  no object store for current data; restored segments are local until the next
  tiering run.
- Object-store backup wrappers for full backup, incremental backup, verify, and
  restore. `gc_backup_uri` removes unretained backup objects while preserving
  parent chains required by retained incrementals.
- Tenant quota enforcement through a replication wrapper. Writes reserve bytes
  before commit and return a dedicated quota error instead of hiding the problem
  as a generic backend failure. Query and write permits enforce per-tenant
  concurrency fairness and are released automatically on all exits.
- Scale-to-zero primitives: hibernation writes a consistent backup and marker
  under a cloud lease; resume restores under the same lease discipline. The
  hibernation marker includes `backup_id`, `hibernated_lsn`, `tenant_id` when
  configured, and a fencing token.

The implementation intentionally tiers only immutable artifacts. Mutable B-tree
state, active WAL, and current transaction metadata remain local in v1.

Operational notes:

- Use encrypted storage before tiering when object-store bytes must be treated as
  untrusted.
- The segment value (pointer or local bytes) is authoritative for reads.
  `__cloud_segments` records tiering progress and can be stale after DML or
  restore until the next tiering run reconciles it. Object-store listing is used
  for backup GC, not for deciding whether a segment is remote.
- multidb never deletes tiered segment objects. Objects are immutable, so
  retaining them is safe, but storage grows with every re-tiering. External
  lifecycle rules must not delete objects that are still referenced by live
  pointers, MVCC history, commit-log records, or retained incremental backups.
- Monitor `multidb_cloud_object_operations_total` and
  `multidb_cloud_tier_read_total` to track request and byte cost.
