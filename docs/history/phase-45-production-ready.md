# Phase 45 Cluster GA

Phase 45 is complete for the checked-in CP Cluster GA contract. MultiDB now has
a live OpenRaft-backed CP cluster path with internal Raft RPC transport,
durable log/state-machine/snapshot storage, membership changes, read-index
strong reads, leader handoff evidence, and a focused Cluster GA smoke gate.

Implemented in this phase:

- Live `openraft::Raft` runtime mounted behind the stable CP cluster API:
  `start_cp_cluster`, `shutdown_cp_cluster`, `cluster_status`,
  `wait_for_recovery`, `change_membership`, and `transfer_leader`.
- Durable CP log, vote, state-machine metadata and snapshots through the
  `StorageEngine` contract.
- Internal transport serving Raft append/vote/pre-vote/snapshot and
  cluster-admin RPC frames.
- Phase 45 smoke scenarios for leader transfer, minority write rejection,
  durable membership metadata, and read-index strong read.

Current verification:

- `scripts/cluster-smoke.ps1` runs the focused Phase 45 smoke gate.
- The underlying live-cluster tests are ignored in the default cargo suite and
  run one scenario per process through the smoke script.
- `scripts/check.ps1` remains the full local correctness gate.

Boundaries:

- Cluster GA here means the local/process CP OpenRaft contract is complete and
  smoke-certified. It does not claim Kubernetes operator automation,
  multi-region placement, managed cloud service behavior, enterprise SLA, or
  Docker packaging.
- AP Cluster GA and sharded multi-region certification remain separate future
  work.
