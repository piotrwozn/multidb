# Changelog

All notable public changes will be recorded here.

## Unreleased

- Prepared the repository for public preview publication.
- Added public repository hygiene files, security policy, contribution guide,
  issue templates and release notes.
- Kept local planning history out of the public source baseline.
- SQL statements, including read-only `SELECT`s, no longer make a durable
  system commit per statement for the workload profiler. Samples are aggregated
  in memory per query fingerprint and persisted in one batched write after
  4,096 statements, 256 distinct fingerprints or 1 MiB of buffered fingerprint
  data, or on the first statement once the oldest buffered sample is 10
  seconds old;
  also before index or runtime advice, on `Database::flush_workload_samples`,
  and when a single-node database is dropped. `system.workload` and
  `Database::workload_report` include buffered samples without writing.
  Advisory statistics buffered since the last flush can be lost on crash or
  process stop. `query_as` audit behaviour is unchanged.

## 0.1.0-preview.1

- Initial public-preview target.
- Includes the public-preview support surface documented in
  `docs/support-matrix.md`.
