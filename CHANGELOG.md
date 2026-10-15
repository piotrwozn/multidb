# Changelog

All notable public changes will be recorded here.

## Unreleased

- Prepared the repository for public preview publication.
- Added public repository hygiene files, security policy, contribution guide,
  issue templates and release notes.
- Kept local planning history out of the public source baseline.
- The public CP cluster constructors (`create_cluster_database`,
  `open_cluster_database`, their `_with_ops` variants) and sharded `Cp` shard
  backends now fail with `Unsupported` instead of returning a database that
  acknowledges writes without replication. Use `repl::start_cp_cluster` for
  replicated CP storage; data written through the old constructors still opens
  with `open_database`. `CpRaft::new` is now test-only.
- AP databases now reject commit-log features they cannot provide
  (incremental backups, changefeeds, CDC subscriptions, continuous queries,
  materialized views, after-commit hooks, system versioning and incremental
  full-text/geo refresh) with `Unsupported` instead of returning empty results.

## 0.1.0-preview.1

- Initial public-preview target.
- Includes the public-preview support surface documented in
  `docs/support-matrix.md`.
