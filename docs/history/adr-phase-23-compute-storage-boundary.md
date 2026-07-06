# ADR: Phase 23 Compute/Storage Boundary

## Decision

Phase 23 does not implement full Aurora/Neon-style compute/storage separation.
It implements the seams required for that direction:

- immutable artifacts are addressable by `SegmentLocation`;
- tiering progress is durable through `TieringState` in `__cloud_segments`;
- recovery uses snapshot plus commit log, not accidental local state;
- hibernate/resume uses an object-store lease and fencing token;
- the optimizer can distinguish local and remote segment cost.

## Rationale

Full separation needs a page-server or equivalent shared storage service,
single-writer fencing, log ownership, remote cache invalidation, and multi-node
read-replica lifecycle management. That is team-scale infrastructure work.

The safe solo step is to make cold immutable bytes remote while keeping mutable
state local. This gives the project cloud cost benefits without pretending that
object-store PUT/GET can replace transactional local storage.

## Consequences

- Analytical data can be tiered and read back transparently.
- Interrupted tiering is recoverable without trusting object-store listing as the
  source of truth.
- Backups can live offsite through object-store URIs.
- Backup GC is policy-driven and preserves parent chains for incrementals.
- Serverless-style hibernation is a restore/resume primitive, not a full
  stateless compute runtime.
- Future compute/storage work should extend the lease and segment-location seams
  rather than bypassing them.
