# MultiDB

MultiDB is an experimental multi-model database engine written in Rust. It
combines local storage, document and relational models, SQL/DataFusion, vector
indexes, CP/AP replication experiments, sharding, backup/PITR, CDC, a Control
Plane API, SDKs and MultiDB Studio.

This repository is ready for public preview. It is not a claim that MultiDB is
safe for critical production data without a separate deployment review, soak
testing and operational ownership.

## Status

- Current package version: `0.1.0`
- Rust MSRV: `1.89`
- License: `MIT`
- Crate publishing: disabled with `publish = false`
- Public preview guide: `docs/public-preview.md`
- Supported surfaces and limits: `docs/support-matrix.md`

## Quickstart With Docker

```powershell
docker compose up --build
```

Then open Studio:

```text
http://127.0.0.1:8080/
```

The local Docker setup uses development credentials documented in
`docs/docker.md`. It is suitable for evaluation and smoke testing, not as a
production secret model.

## Quickstart From Source

Requirements:

- Rust `1.89` or newer
- Node.js `24` for Studio checks
- Docker for runtime smoke tests
- Go `1.22` or newer for the Go SDK smoke

Run the repository gate:

```powershell
.\scripts\check.ps1
```

Run the full release smoke:

```powershell
.\scripts\release-smoke.ps1
```

If Go is not installed locally, this partial gate still verifies the rest of the
stack:

```powershell
.\scripts\release-smoke.ps1 -SkipGo
```

GitHub Actions runs the Go checks.

## CLI Preview

```powershell
multidb template list
multidb template explain ai-memory --json
multidb init --guided --template ai-memory --name "Agent Memory" --out .\agent-memory --apply
multidb config validate --spec .\agent-memory\multidb.yaml
multidb config explain --spec .\agent-memory\multidb.yaml
multidb config drift --spec .\agent-memory\multidb.yaml
```

Built-in templates emit version 2 specs with the physical parameters
provisioning needs (document index paths, the vector dimension, time-series
chunks and table columns), so `init --template <t> --apply` writes the files
and creates the database in one step; `config drift` exits 0 while it matches
the spec. `secure-saas` requires encryption at rest: select an existing key
file with `MULTIDB_ENCRYPTION_KEY_PATH` before `init --apply`; without a key the
command writes the spec files and exits 2.

Profiles have typed knobs. Inspect them, or write a version 2 spec with a
custom profile that extends a built-in one:

```powershell
multidb profile show high_durability
multidb profile show high_durability --override cache.max_entries=8192
multidb init --guided --profile high_durability --name Orders --override cache.max_entries=8192 --out .\orders.yaml
multidb profile explain --spec .\orders.yaml
```

See `docs/profiles.md` for the knob reference and the profile commands.

Build a database from a spec (preview). `config plan --spec` is a dry run that
prints every step, the flags it needs and how each guarantee is enforced;
`config apply` recomputes the plan and applies it with the `prov_` id it
printed, or with `--auto-approve` when the plan only creates, adopts or
records:

```powershell
multidb init --guided --profile secure_app --name Shop --out .\shop.yaml --apply
# after editing shop.yaml:
multidb config plan --spec .\shop.yaml
multidb config apply --spec .\shop.yaml --confirm <prov_id>
multidb config drift --spec .\shop.yaml
multidb config status --db .\shop.redb
multidb config export --db .\shop.redb --out .\exported.yaml
```

See `docs/provisioning.md` for the spec fields, the plan, the flags and the
exit codes. A running `multidb serve` provisions the database it serves
through the preview Control Plane endpoints `/provision/plan`,
`/provision/apply`, `/provision/drift`, `/provision/state` and `/config/live`.

Checked-in examples are under `examples/`, with SDK examples under
`examples/sdk/`; `examples/provisioning` is a full version 2 spec with
full-text and geo indexes, a versioned table, a columnar table, roles and
principals.

## GraphRAG retrieval

Use vector search to find seed passages, follow their graph relationships and
return context with numbered citations and source provenance. The preview API is
`POST /api/graphrag/query`; TypeScript, Python, Go and Rust SDKs expose the same
operation. Metadata policies apply before text extraction and graph traversal.

```sh
cargo run --locked --example graphrag
```

See [the GraphRAG guide](docs/graphrag.md) for ingestion, API requests, resource
budgets and authorization. Applications supply their embeddings and use the
returned context to answer questions.

## Repository Gates

Main local gate:

```powershell
.\scripts\check.ps1
```

Studio gate:

```powershell
.\scripts\studio-check.ps1
Push-Location studio; npm run e2e; Pop-Location
```

Preview gate:

```powershell
.\scripts\preview-smoke.ps1
```

Release-sensitive gate:

```powershell
.\scripts\release-smoke.ps1
```

## Known Limits

- MultiDB is a public-preview engine, not a mature production database.
- The CP OpenRaft profile is covered by local/process smoke tests; it does not
  claim managed Kubernetes automation, multi-region placement or an enterprise
  SLA. Multi-node CP replication runs through `repl::start_cp_cluster`; the
  embedded `Database` cluster constructors fail closed with `Unsupported`.
- AP databases reject commit-log features (incremental backups, so no PITR,
  changefeeds, subscriptions, continuous queries, materialized views,
  after-commit hooks, system versioning and incremental full-text/geo refresh)
  instead of returning empty results.
- `config apply --plan` (the version 1 migration plan) stays
  confirmation/audit-only and does not perform physical data migrations.
  Provisioning (`config plan|apply --spec`, `init --apply`, `multidb serve`
  spec mode and `POST /provision/apply`) is a preview for embedded and
  single-node CP databases: it
  creates and adopts objects, roles and the profile, but never drops or alters
  an object, switches a profile or storage class, or re-encrypts. CLI and serve
  support encryption with `MULTIDB_ENCRYPTION_KEY_PATH`; Control Plane requests
  use the database's existing encryption configuration (see `docs/docker.md`). Values fixed at creation,
  such as a vector `dim` or table columns, are edited in the spec before the
  first apply.
- Studio v1 focuses on validation, catalog, advice, security views, audit and
  migration dry-run workflows.
- Full PostgreSQL compatibility, Mongo wire protocol, public extension
  marketplace, SSO and external compliance certification are outside the
  current support claim.

## Documentation

- `docs/public-preview.md` - install, verify and first-use guide.
- `docs/support-matrix.md` - supported surfaces and out-of-scope claims.
- `docs/release-and-versioning.md` - release/versioning policy.
- `docs/release-checklist.md` - release checklist.
- `docs/quality-and-performance.md` - benchmark and performance gate workflow.
- `docs/source-baseline.md` - source vs generated/local artifact policy.

## Contributing And Security

Contributions are welcome. Good places to start are issues labeled
`good first issue`, `help wanted` or `contributor-friendly`.

You can help with documentation, examples, SDKs, Studio, tests, fuzzing,
benchmarks, operations docs and database internals. Open an issue for bugs,
feature ideas or design questions, start a GitHub Discussion for broader
proposals, or send a focused pull request.

See `CONTRIBUTING.md` for local checks, review policy and pull request
expectations.

Report vulnerabilities privately through GitHub Security Advisories once the
repository is published. See `SECURITY.md`.
