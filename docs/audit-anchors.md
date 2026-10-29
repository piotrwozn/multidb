# Durable audit anchors

`AuditConfig.anchor_path` adds a local JSONL durability sink to the database audit
chain. A request succeeds only after its audit events and head are committed and
the matching anchor records are synchronized. Concurrent callers can share a
durable batch. Creating a new anchor also synchronizes its parent directory on
Unix.

Before the first anchored batch after reopening, the database verifies the
existing file as a contiguous signed prefix of its stored audit chain. A clean
prefix is extended with the missing, verified stored events before a new batch
can succeed. This also applies when enabling an anchor on an existing database.
Recovery reads bounded storage batches and retains at most 16 MiB of selected
records per recovery window. Each JSONL record is limited to 1 MiB. Recovering a
long missing tail can require multiple scans; this is recovery work, not a
per-request scan during normal operation.

After a successful synchronization, the database retains a verified checkpoint.
Each subsequent batch checks the live file identity, length, modification time,
Unix inode change time (including nanoseconds)
and signed tail, together with the current stored head and audit-key fingerprint.
A changed or rotated file requires full prefix validation again. Platforms
without an inode change timestamp use full validation for every batch. The signing
key and database head are read afresh for every batch. Backends without bounded
audit scans refuse anchor catch-up.

An incomplete, malformed, foreign or inconsistent file is preserved and rejected.
An audit sink failure seals that database handle: pending and later audit callers
fail until the database is reopened. Fixing a path while retaining the same
failed handle does not resume acknowledgements past an uncertain prefix.

To recover damaged evidence, stop the database, preserve the anchor under a
different name, and reopen with the configured path available for a new file.
The next audit operation rebuilds that file from the verified stored chain before
acknowledging another event. Keep the preserved file for investigation. This
procedure cannot repair a corrupted database audit chain; signature, sequence,
stored-record and head mismatches remain errors.

The standalone `FileAuditSink` API appends supplied events. The database anchor
verification and recovery rules described here apply to `AuditConfig.anchor_path`.
