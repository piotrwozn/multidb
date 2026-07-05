# Phase 26 Transaction Core

Phase 26 records the production contract for local MVCC transactions. It is
`Complete` in the roadmap because the checked-in code now carries the same
transaction, conflict, durability and conformance guarantees across local
storage backends.

Implemented in this phase:

- Transactions provide read-your-writes, rollback, savepoints, retry loops and
  durable commit records.
- Snapshot isolation keeps stable read views; serializable mode rejects observed
  write skew and phantom range changes.
- Redb-backed transactions survive reopen, while rolled-back writes do not
  become visible.
- Analytical and multi-model transactions share the same commit path, including
  document/vector/relational write sets.
- Active snapshot tracking supports MVCC garbage-collection watermarks.

Verification refreshed on 2026-07-04:

- `cargo test --lib transaction -- --nocapture` covers facade transaction
  behavior, isolation and redb reopen semantics.
- `cargo test --lib txn -- --nocapture` covers low-level MVCC records, HLC
  commits and GC watermark behavior.
- `cargo test --lib storage::conformance -- --nocapture` covers backend
  storage semantics, stale-snapshot conflict detection, writer serialization,
  read fault injection and redb reopen durability.

Production notes:

- AP and sharded snapshot transactions are rejected deterministically rather
  than silently weakening isolation.
- Long-running crash/concurrency soak beyond the checked-in redb reopen and
  conformance tests remains a nightly/operator gate, not a default fast test.
