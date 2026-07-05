# Phase 23 Cloud Architecture

Phase 23 adds the production cloud-facing layer to multidb without changing the
storage contract used by documents, relations, vectors, time-series, and SQL.

Implemented in this phase:

- `CloudObjectStore` is the provider seam. The default build includes a
  deterministic `file://` implementation used by tests and local deployments;
  non-local providers are opened through the same config path and can be added
  behind features without changing model code.
- Tiered storage for immutable columnar segments. A local Parquet segment can be
  uploaded to an object-store URI and replaced by a checked pointer. Reads are
  transparent and verify CRC32 before handing bytes to the Parquet reader.
- Tiering state is persisted in `__cloud_segments` with `Local`, `Uploading`,
  `Remote`, `DeletePending`, and `Failed` states. Interrupted uploads are
  recovered idempotently: valid remote objects are finalized, missing objects
  fall back to local retry, and corrupt objects are marked failed.
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
- Keep `__cloud_segments` as the source of truth for segment placement.
  Object-store listing is used for backup GC, not for deciding whether a segment
  is remote.
- Monitor `multidb_cloud_object_operations_total` and
  `multidb_cloud_tier_read_total` to track request and byte cost.
