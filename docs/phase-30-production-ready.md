# Phase 30 Production Readiness

Phase 30 is complete for the checked-in GA support matrix. The GA contract is a
bounded distributed-readiness profile: durable 2PC, AP repair/handoff, CDC
resume safety, internal framed transport, and stable CP cluster control APIs.
Phase 45 builds on this surface to close the local/process live OpenRaft Cluster
GA smoke gate.

Implemented in this pass:

- Durable distributed transaction records in system tables for prepared,
  coordinator decision, and finished states.
- Cross-shard public batches now use prepare, durable decision, and idempotent
  finish instead of being rejected.
- `SingleNode` and CP backends can prepare, finish, and recover local in-doubt
  distributed transaction state.
- AP strong range reads gather quorum replies, merge siblings, and repair
  divergent replicas. AP anti-entropy compares Merkle leaves and syncs only
  divergent records. Hints have monotonic per-target sequence numbers, TTL, and
  flow-control backlog limits.
- Multi-node AP and CP configs fail fast without `InternalTransportConfig`.
- Internal node-to-node transport provides length-prefixed `postcard` frames over
  Tokio TCP, production mTLS config, AP RPCs, health/flow-control frames, frame
  limits, per-peer inflight caps, and a test-only plaintext gate.
- Pgwire serving has a connection semaphore, connection timeout, auth
  rate-limiting, task error logging, and describe/schema inference through the
  blocking executor.
- CDC has a paged `ChangefeedPage` API with `has_more`, token pruning without
  `saturating_sub`, durable push offsets after delivery, and materialized view
  refresh in bounded windows.
- CDC timeline records map safe parent timeline resumes and return
  `TimelineForked` for unsafe forks. `SubscriptionWorker::start` owns a bounded
  channel and persists offsets after delivery.
- HLC commit metadata is persisted in `__hlc_clock`, commit log records expose
  HLC to CDC, AP placement prefers the local region while preserving quorum, and
  anti-entropy/hint backlogs are capped.
- CP declares real OpenRaft data/response types, persists committed index
  metadata, uses a read-index-shaped strong-read gate, and exposes a managed
  self-healing background runner.
- Stable CP cluster APIs are exported:
  `start_cp_cluster`, `shutdown_cp_cluster`, `cluster_status`,
  `wait_for_recovery`, `change_membership`, and `transfer_leader`.
- Internal transport includes explicit Raft append/vote/pre-vote/snapshot and
  cluster-admin RPC frames while preserving the mTLS/frame-limit/timeout/flow
  control envelope. Servers without a mounted live Raft endpoint return
  `Unsupported` before side effects.

Bounded behavior in this phase:

- Remote leader transfer without a mounted live OpenRaft runtime returns
  `ReplError::Unsupported` before state change.
- Full CP Cluster GA evidence is owned by phase 45.
- The phase 30 support matrix covers deterministic distributed readiness and
  explicit fail-closed behavior for unsupported remote cluster surfaces.

Current verification:

- `cargo test --locked --all-features cluster_api -- --nocapture` passes:
  status, recovery, membership, and fail-closed leader transfer.
- `cargo test --locked --all-features raft_rpc_frame -- --nocapture` passes:
  Raft frame serialization and frame limit enforcement.
- `cargo test --locked --all-features unmounted_raft -- --nocapture` passes:
  Raft/admin RPCs fail closed when no endpoint is mounted.
