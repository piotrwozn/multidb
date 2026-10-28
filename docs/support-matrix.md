# MultiDB Support Matrix

This is the current public-preview support snapshot. It describes which
surfaces are intended to be stable for evaluation, which are preview-only, and
which claims are deliberately out of scope.

## Global Gates

- `scripts/release-smoke.ps1` is the combined local gate.
- CI covers Rust fmt, clippy, all-feature tests, SDK smoke, SDK examples,
  Studio tests, Playwright, Docker smoke, fuzz smoke, perf gate, cargo-deny,
  cargo-audit, cargo-vet and SBOM generation.
- Release tags publish signed Linux and Windows x86_64 binaries, checksums,
  CycloneDX SBOM, provenance, a signed GHCR Docker image and release smoke
  reports.
- `latest` Docker tags are not part of the release policy.

## Product Surface

| Surface | Status | Evidence |
| --- | --- | --- |
| CLI templates/config | Stable preview; built-in templates emit spec v2 with the physical parameters provisioning needs, and `init --template <t> --apply` (Preview, see the provisioning row) builds every template; `secure-saas` requires an explicit `MULTIDB_ENCRYPTION_KEY_PATH` | `scripts/preview-smoke.ps1`, template smoke (including `init --apply`, `config drift` and a re-apply per template), template plan and provisioning tests, config tests |
| Control Plane API v1 | Stable preview | OpenAPI v1, operation registry, SDK contract tests |
| GraphRAG retrieval | Preview: bounded vector-seed and directed graph expansion with policy-filtered, cited context; Rust API, POST `/graphrag/query` and four SDKs; native bounded scans required, AP/sharded refused; applications supply embeddings | GraphRAG core/DB/HTTP tests, bounded graph scans, SDK transport tests, `cargo run --example graphrag`; `docs/graphrag.md` |
| Studio | Stable for documented admin workflows | Vitest, Playwright operator flow, Docker smoke |
| Docker image | Stable for single-node production-shaped runtime | Docker smoke, GHCR release image, signed digest |
| Helm chart | Supported runtime parity artifact | Helm values/templates match Docker env contract |
| SDKs | Stable preview for API v1 client surface in repo packages | TypeScript, Python, Go and Rust package gates |
| Metrics endpoint | Preview | Raw Prometheus text remains marked preview |
| `config apply --plan` | Audit/confirmation only | No physical data migration in v1 |
| `config apply --spec` / spec provisioning (`multidb::provision`, spec v2) | Preview: plan, apply, drift, status and export for embedded and single-node CP databases through the Rust API, the CLI (`config plan\|apply\|drift --spec`, `config status`, `config export`, `init --apply`, as `local:cli`) and `multidb serve` spec mode (`MULTIDB_SPEC_PATH`, `MULTIDB_SPEC_APPLY=verify\|create\|additive`, as `local:serve`; embedded apps use `provision::open_from_spec`) and the Control Plane (see the next row); additive: objects are created or adopted, never dropped or altered; no profile or storage-class switch; encryption at rest through Rust API, CLI and serve with explicit file-key or `LocalEnvelope` configuration (CLI/serve use `MULTIDB_ENCRYPTION_KEY_PATH`) | plan, apply, failure-injection, RBAC, format upgrade and backup/restore provisioning tests; CLI exit-code, dry-run, locked-database and `init --apply` tests; startup decision table, `open_from_spec` and serve spec-mode tests |
| Control Plane provisioning (`GET /provision/state`, `POST /provision/plan`, `POST /provision/apply`, `POST /provision/drift`, `GET /config/live`) | Preview: plans, applies and checks the database the server serves, as the calling principal (`System:Admin`, plus `Database:Admin` to apply), confirmed only by the typed `prov_` id of the recomputed plan; never creates databases or opens paths; retunes and the format upgrade take effect at restart; SDK methods in TypeScript (typed), Go, Python and Rust; Studio has no provisioning flow yet | admin endpoint tests (200/400/403/409/422/500, plan changes, flags, self-lockout, audit, partial applies), OpenAPI contract tests, SDK request tests |
| Composable profiles (`custom_profile`, spec v2) | Preview; single-node profile manifests through the Rust API and CLI provisioning (`config apply --spec`, `init --apply`); compression is forward-only and needs `schema_version` 2; the encryption and audit floors are enforced at creation (audit also afterwards) and on restore, which refuses a backup whose unencrypted target or audit flag would miss them unless `RestoreConfig.allow_unmet_profile_floors` / `restore --allow-unmet-floors` opts in (hibernation resume has no opt-in); `profile show --db` and `restore --allow-unmet-floors` then warn, and every opener logs a `tracing` warning that only embedders with a subscriber see; the encryption floor covers the database file, while backup files are encrypted by backup encryption (by default the database's own encryption config); CLI `profile show/explain/knobs`, `config upgrade`, `init --override` and `--profile auto`; `GET /profiles` variants and knobs; Studio custom-profile builder (validate and dry-run only) | resolver, schema v2 snapshot, v1 compatibility corpus, profile manifest open/refusal and backup/restore tests, CLI profile command tests, Control Plane `/profiles` and Studio builder tests |
| History GC (`multidb maintenance gc`, `Database::gc_history`) | Preview: local single-node only; bounded online MVCC GC via `gc_history_online` or opt-in `MULTIDB_HISTORY_RETAIN_LSNS` in serve; commit-log truncation remains offline (CLI needs exclusive file access); MVCC pruned below min(current, AS OF pins, temporal `min_lsn`, current - `--history-retain-lsns` + 1), so without `--history-retain-lsns` plain tables lose `AS OF` history below the current LSN; commit log truncated only with explicit `--commit-log-retain-lsns`, never below durable consumer positions, and a new full backup is required afterwards (incremental backups fail with `CommitLogTruncated`); on encrypted engines one small `__enc_versions` row stays per pruned version and per truncated commit-log record | `txn::history`, `db::history`, cdc and CLI tests |
| Changefeeds and CDC subscriptions (`Database::poll_changefeed*`, `multidb cdc`) | Preview: embedded Rust API and CLI only, with no PostgreSQL wire or Control Plane endpoint; `poll_changefeed_as` needs `Read` on the filter target, plus `System:Admin` (a `Database:Admin` grant covers it, and for a `System` target it covers that `Read`) whenever the poll can return system rows (`include_system` or a `System` target), which carry admin password hashes, the RBAC policy and principals, the audit log, policy definitions and raw index keys that masking and row policies do not cover; principal feeds hide protected FTS/geo events, protected deletes and masked-primary-key events; principal subscriptions and continuous queries bind ownership and require policy-aware delivery (`*_for_user` resolves live registry mappings); legacy ownerless subscriptions are trusted; subscriptions, continuous queries and outbox connectors never include system rows; the trusted `Database::poll_changefeed` and `multidb cdc poll --include-system` return them to the local operator, like the database file itself | cdc tests (`cdc::tests::system_rows`: RBAC, audit, masking, subscriptions, SQL), CLI and Control Plane tests |

## Profiles And Domains

| Area | Support Status |
| --- | --- |
| Certified local profiles and collection roles | Stable preview inside generated spec and smoke coverage |
| `strong_cp` local/process cluster contract | Stable preview for local/process smoke semantics of the live `repl::start_cp_cluster` OpenRaft runtime (`scripts/cluster-smoke.ps1`); embedded `Database` cluster constructors (`create_cluster_database`, `open_cluster_database`, their `_with_ops` variants and sharded `Cp` shard backends) fail closed with `Unsupported` |
| `eventual_ap` conflict policy | Experimental; workload review required. AP databases reject commit-log features with `Unsupported`: incremental backups (so no point-in-time restore beyond a full backup), changefeeds, CDC subscriptions, continuous queries, materialized views, after-commit hooks, system versioning and incremental full-text/geo refresh; full backups and `refresh_full` remain available |
| Graph role | Experimental |
| Full-text (`match`) and geo (`within_radius`) indexes | Experimental; backfilled on create and maintained synchronously with document writes through collection handles, transactions and Control Plane CRUD; raw key writes need `refresh_full`/`refresh_incremental`; searches need Read on the source collection and are denied under its row or masking policies |

## Out Of Scope

Multi-region guarantees, managed cloud, enterprise SLA, external compliance
certification, public extension marketplace, SSO, full PostgreSQL
compatibility and Mongo wire compatibility are not current support claims.
