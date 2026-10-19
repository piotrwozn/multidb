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
- `MultiModelTxn` reads use storage-level snapshots and conflict checks use
  `__txn_versions`, so open transactions do not pin MVCC history. `AS OF`
  queries pin their LSN in the in-process `ActiveSnapshotRegistry` while they
  run.
- History GC is manual and limited to local single-node databases:
  `Database::gc_history` / `multidb maintenance gc` prunes MVCC versions below
  `min(current LSN, AS OF pins, min_lsn of temporal tables that keep history,
  current LSN - --history-retain-lsns + 1)` in bounded, resumable batches.
  Without `--history-retain-lsns`, plain tables lose `AS OF` history below the
  current LSN. The commit log is truncated only with an explicit
  `--commit-log-retain-lsns`, never below the slowest subscription, continuous
  query, materialized view or full-text/geo index position; take a new full
  backup afterwards, because the incremental chain is broken. `AS OF`, system
  versioned history scans and new temporal retentions below the recorded
  history horizon return `RetentionExpired`. `serve` does not run GC online
  yet.

Verification refreshed on 2026-07-04:

- `cargo test --lib transaction -- --nocapture` covers facade transaction
  behavior, isolation and redb reopen semantics.
- `cargo test --lib txn -- --nocapture` covers low-level MVCC records, HLC
  commits, GC watermark behavior and the bounded history GC batches
  (`txn::history`: per-key pruning, tombstones, resumable budgets and
  commit-log truncation).
- `cargo test --lib db::history -- --nocapture` covers `AS OF` results at and
  above the GC watermark, retention pins, durable consumer floors, backups and
  concurrent writers.
- `cargo test --lib storage::conformance -- --nocapture` covers backend
  storage semantics, stale-snapshot conflict detection, writer serialization,
  read fault injection and redb reopen durability.

Production notes:

- AP and sharded snapshot transactions are rejected deterministically rather
  than silently weakening isolation.
- Long-running crash/concurrency soak beyond the checked-in redb reopen and
  conformance tests remains a nightly/operator gate, not a default fast test.
