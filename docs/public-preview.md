# MultiDB Public Preview

Public preview means MultiDB has an installable, repeatable starter path with
clear support boundaries. It is not a claim that every planned distributed
feature is production-ready.

## Install And Verify

Download the release binary for your platform, `SHA256SUMS`,
`multidb.cdx.json` and the matching `.sig`/`.pem` files from a tagged GitHub
Release. Tagged releases also include the signed GHCR image digest and build
metadata when a Docker image is published.

On Linux/macOS:

```bash
sha256sum --check SHA256SUMS
cosign verify-blob multidb-linux-x86_64 \
  --certificate multidb-linux-x86_64.pem \
  --signature multidb-linux-x86_64.sig \
  --certificate-identity-regexp 'https://github.com/.*/.github/workflows/release.yml@refs/tags/v.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
chmod +x multidb-linux-x86_64
./multidb-linux-x86_64 template list
```

On Windows PowerShell:

```powershell
Get-FileHash .\multidb-windows-x86_64.exe -Algorithm SHA256
Get-Content .\SHA256SUMS
cosign verify-blob .\multidb-windows-x86_64.exe `
  --certificate .\multidb-windows-x86_64.exe.pem `
  --signature .\multidb-windows-x86_64.exe.sig `
  --certificate-identity-regexp 'https://github.com/.*/.github/workflows/release.yml@refs/tags/v.*' `
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
.\multidb-windows-x86_64.exe template list
```

The release also includes a CycloneDX SBOM at `multidb.cdx.json`, GitHub
provenance attestation for the binaries and a signed GHCR image digest. The
release policy intentionally does not publish a Docker `latest` tag.

## Quickstart

```powershell
multidb template list
multidb template explain ai-memory --json
multidb init --guided --template ai-memory --name "Agent Memory" --out .\agent-memory
multidb config validate --spec .\agent-memory\multidb.yaml
multidb config explain --spec .\agent-memory\multidb.yaml
```

To build the database a spec describes (preview, see
[provisioning.md](provisioning.md)):

```powershell
multidb init --guided --template ai-memory --name "Agent Memory" --out .\agent-memory --apply --force
multidb config drift --spec .\agent-memory\multidb.yaml
multidb config apply --spec .\agent-memory\multidb.yaml --auto-approve
multidb init --guided --profile secure_app --name Shop --out .\shop.yaml --apply
multidb config export --db .\shop.redb --out .\exported.yaml
```

`init --apply` writes the spec, then creates a new database from it at
`--db`, the spec `storage_path` or `<out dir>/<name>.redb` when the plan only
creates, adopts or records. After editing a spec, `config plan --spec`
prints the plan and its `prov_` id, `config apply --spec --confirm <prov_id>`
applies it, `config drift` exits 3 when the database no longer matches and
`config status --db` prints the provision record and history. Built-in
templates emit version 2 specs with the physical parameters provisioning
needs (document index paths, the 1536-dimension ai-memory vectors, time-series
chunks and the analytics table columns), so `init --template <t> --apply`
builds every template on the CLI except `secure-saas`: it asks for
encryption at rest, which is provisioned through the Rust API only, so its
plan is blocked with `GUARANTEE_NOT_ENFORCEABLE` (exit 2) and the files are
kept. Values fixed at creation, such as the vector `dim`, are edited before
`--apply`. A CLI plan reports audit as `enforced_with_development_key` and
backup and PITR as `operator_managed` only when the spec asks for them.
Version 1 specs, including the frozen copies of the earlier templates under
`tests/fixtures/spec-v1`, leave out the physical parameters (a document index
path, a vector dimension), so their plan is blocked with
`PHYSICAL_PARAMETER_REQUIRED`; `config upgrade` lists what to add.
`examples/provisioning` is a full version 2 spec to plan and apply.
`multidb serve` starts from a spec too: with `MULTIDB_SPEC_PATH` it checks the
database against the spec at every start, refuses to start when they differ
(`MULTIDB_SPEC_APPLY=verify`, the default) and creates or extends it with
`create` or `additive` (see [docker.md](docker.md#spec-mode)). A running server
provisions the database it serves through the preview Control Plane endpoints
`POST /provision/plan`, `POST /provision/apply` (confirmed with the plan's
`prov_` id), `POST /provision/drift`, `GET /provision/state` and
`GET /config/live`, which the SDKs wrap (see
[provisioning.md](provisioning.md#control-plane)).

For a repository smoke test, run:

```powershell
.\scripts\preview-smoke.ps1
```

To smoke a release binary directly:

```powershell
.\scripts\preview-smoke.ps1 -Bin .\target\release\multidb.exe
```

## Profiles

`multidb profile list` shows the six product profiles with their variants.
Each variant pins one storage profile; `docs/profiles.md` has the full model
and knob reference.

```powershell
multidb profile list --json
multidb profile show high_durability
multidb profile show secure_app --override cache.max_entries=8192 --override floors.audit=true
multidb profile show --db .\orders.redb
multidb profile explain --spec .\orders.yaml
multidb profile knobs
```

- `profile show <profile>` prints the resolved knobs with where each value
  came from, when it applies and whether this build enforces it.
  `--override path=value` (repeatable, or comma-separated) gives a what-if
  view; a misspelled knob gets a did-you-mean suggestion.
- `profile show --db <path>` prints the profile manifest a database was
  created with, or "legacy database" for databases without one. It opens the
  file read-only and never creates or writes a file. It warns about each
  manifest floor the file visibly misses: `floors.encryption_at_rest`, because
  it read the metadata without a key, so the file is not encrypted, and
  `floors.audit` when the stored audit flag is off. `restore` refuses a backup
  whose restored database would miss such a floor (a restore target is never
  encrypted) and creates nothing, unless `--allow-unmet-floors` is given; it
  then prints the same warnings.
- `profile explain --spec <file>` resolves the profile of a spec; it exits 2
  when the profile is invalid, including a version 1 spec that sets
  `custom_profile` (`FIELD_REQUIRES_SPEC_V2`).
- `init --guided --profile <profile> --override path=value [--profile-name <slug>]`
  writes a version 2 spec whose `custom_profile` extends `<profile>`. Without
  `--override`, guided init writes the same version 1 spec as before; an alias
  of a non-canonical variant (for example `high_durability`) gets a warning,
  because a version 1 spec records the product slug.
- `config upgrade --spec <file> [--out <file>] [--force]` converts a spec to
  version 2. When `custom_profile` is unset, it turns the
  `profile.custom.base` override of older Studio specs, with
  `profile.custom.description`, into a real `custom_profile`, and writes the
  `profile.custom.slug` operation hint to the spec's `profile` field (the
  current Studio builder writes
  `custom_profile` itself). Any other `profile.custom.*` key, these keys
  without a base, and these keys beside an existing `custom_profile` stay in
  place; the upgrade notes and the validation suggestions say what to move by
  hand. It is idempotent, writes `--out` in the format of its extension
  (stdout keeps the input format), does not keep YAML comments, refuses to
  overwrite a file without `--force`, and never writes a result that does not
  validate or that grows past 1 MiB when re-serialized (exit 2).
- Every `--profile` flag and `MULTIDB_PROFILE` accept a storage profile name
  (as before), a product slug or alias in snake or kebab case, or `auto`,
  which reads the storage profile of the existing database without writing
  it. A database that was not closed cleanly (`serve` has no graceful
  shutdown, so this follows every stop by a signal) first gets redb's crash
  recovery, as with a named profile; `profile show --db` never writes and
  reports it as needing recovery. The CLI and `serve` open single-node CP
  databases only, so an AP database fails with `ReplicationMismatch` with
  `auto` as with a named profile. `restore` does not accept `auto`, because
  its target must be empty.

Databases are created from profile knobs by the Rust API
(`db::create_database_with_profile`, `provision::provision_new_database` and
`provision::apply_provision`) and by the CLI provisioning commands
(`config apply --spec`, `init --apply`, see [provisioning.md](provisioning.md)).
Every other CLI command opens existing databases with their persisted
profile.

## Docker Quickstart

Use Docker Compose if you do not want to build Rust or Node locally:

```powershell
docker compose up --build
```

The compose file starts the Control Plane API and Studio on
`http://127.0.0.1:8080/` and PostgreSQL wire on `127.0.0.1:5432`, backed by a
named Docker volume. It is a local-dev configuration with explicit development
secrets and plaintext PostgreSQL; production settings and the full runtime env
contract are documented in `docs/docker.md`.

## Support Matrix

Profile status:

| Profile | Status | Preview meaning |
| --- | --- | --- |
| `game_local_balanced` | Certified | Local game and embedded state starter path. |
| `desktop_app_embedded` | Certified | Durable local desktop application data. |
| `ai_agent_memory` | Stable | Vector/document/event memory for agents. |
| `secure_app` | Stable | Audited transactional app state; can use CP semantics without claiming enterprise SLA. |
| `analytics_columnar` | Stable | Analytics starter path with explicit extension limits. |
| `production_cp` | Stable | CP OpenRaft cluster profile covered by the local/process smoke gate for the live `repl::start_cp_cluster` runtime. |

Collection role status:

| Role | Status |
| --- | --- |
| `document_entity` | Certified |
| `key_value` | Certified |
| `cache` | Certified |
| `event_log` | Stable |
| `vector_memory` | Stable |
| `audit` | Stable |
| `analytics` | Stable |
| `time_series` | Stable |
| `graph` | Experimental |

Consistency status:

| Domain | Status | Limit |
| --- | --- | --- |
| `local_snapshot` | Certified | Single-process/local semantics only. |
| `strong_cp` | Stable | CP quorum semantics covered by the local/process smoke gate for the live `repl::start_cp_cluster` runtime. |
| `eventual_ap` | Experimental | Conflict policy requires workload review. |

## Known Limits

- The local/process CP smoke contract is covered; the preview still does not
  claim Kubernetes automation, multi-region placement or enterprise SLA.
- Multi-node CP replication runs only through the live `repl::start_cp_cluster`
  OpenRaft runtime. The embedded `Database` cluster constructors
  (`create_cluster_database`, `open_cluster_database`, their `_with_ops`
  variants and sharded `Cp` shard backends) fail closed with `Unsupported`; use
  `create_database`/`open_database` for a single node or `start_cp_cluster`.
- AP databases do not record writes in the commit log or MVCC history, so they
  reject incremental backups (so no point-in-time restore beyond a full
  backup), changefeeds, CDC subscriptions, continuous
  queries, materialized views, after-commit hooks, system versioning and
  incremental full-text/geo refresh with `Unsupported`. Full backups and
  `refresh_full` remain available.
- `config apply --plan` is confirm/audit-only and does not perform physical
  data migrations. Provisioning (`config apply --spec`, `init --apply`,
  `multidb serve` spec mode and the `/provision/*` Control Plane endpoints) is
  a preview for embedded and single-node CP databases: it creates and adopts
  objects, roles and the profile, never drops or alters them, and refuses
  encryption at rest outside the Rust API. The built-in templates are
  provisionable on the CLI except `secure-saas`, which asks for encryption at
  rest; a vector `dim` or table columns are fixed at creation, so edit them
  before `init --apply`.
- Studio v1 is read-only plus validation, catalog, advice and migration dry-run.
- Mongo wire protocol, full PostgreSQL compatibility, public marketplace,
  cloud KMS/HSM and enterprise SLA are outside preview scope.
- The release performance baseline is a versioned smoke gate, not a dedicated
  production-hardware benchmark claim.
- Time-series retention follows the server wall clock and is enforced at chunk
  granularity only when a series receives a write, so idle series keep expired
  chunks until their next write. Points more than `max_future_skew_millis`
  (default 5 minutes for new collections) ahead of the server clock, or older
  than the retention window, are rejected.
- Workload-profiler statistics (`system.workload`, index advice and the Runtime
  Advisor) are advisory. They are buffered in memory and flushed in batches;
  the 10-second age trigger is checked on the next statement, not by a
  background task. Samples buffered since the last flush are lost on crash or
  when `multidb serve` stops, because there is no graceful-shutdown flush. CP,
  AP and sharded databases do not flush on drop.

## Migration Guide

- From SQLite-style local apps: start with `desktop-embedded` or
  `game-save`, model durable entities as `document_entity` or `key_value`, then
  validate and explain the generated `multidb.yaml` and build the database
  with `init --apply` or `config apply --spec`.
- From PostgreSQL-style data: use the PostgreSQL wire-compatible surface where
  it fits, or import CSV/JSONL/pg COPY text through the CLI. Unsupported DDL
  and catalog features fail closed.
- From Mongo-style documents: migrate through BSON/document import paths and
  use document APIs. Mongo wire protocol is deferred.
- From analytics pipelines: start with `analytics`, keep the columnar extension
  limits visible in explain output, and treat release perf reports as gate
  evidence rather than capacity planning.

## What Public Preview Means Here

For this preview, production-shaped means the package is reproducible, signed,
documented, smoke-tested, guarded by SDK/Docker/Studio/release gates and honest
about limits. It does not mean every planned item is finished or that MultiDB
is ready for critical production data without a separate deployment review.
