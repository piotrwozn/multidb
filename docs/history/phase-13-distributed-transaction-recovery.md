# Phase 13 Distributed Transaction Recovery

Phase 13 is `Complete` in `src/roadmap.rs`. This snapshot records the closed
fast-gate recovery contract for the durable 2PC path that exists in the
repository; it is not a Cluster GA claim.

Implemented in this pass:

- Cross-shard writes prepare participant records on every touched shard before
  the coordinator records a durable decision.
- Recovery replays durable coordinator decisions and finishes in-doubt
  participants idempotently, including the case where one shard finished before
  a coordinator/router restart and another did not.
- Prepared participants without a durable coordinator decision are aborted by
  recovery, preventing uncommitted in-doubt writes from becoming visible.

Verification refreshed on 2026-07-04:

- `cargo test --lib recover_dist_txns -- --nocapture` passes: 2 focused
  distributed transaction recovery tests.

Production notes:

- The fast gate uses the in-process sharded harness. It proves durable state
  machine behavior through the `Replication` contract, not a full external
  multi-process cluster.
- CP Cluster GA evidence is owned by phase 45; this document should be used
  only for the distributed transaction recovery slice.
