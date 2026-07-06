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
- Readiness source of truth: `src/roadmap.rs`
- Release contract: `docs/ga-support-matrix.md`
- Public preview guide: `docs/public-preview.md`

The local planning history is intentionally not part of the public source
baseline. Checked-in roadmap evidence lives in source and docs.

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

Run the full Phase 53 public-preview/GA smoke:

```powershell
.\scripts\phase53-ga-smoke.ps1
```

If Go is not installed locally, this partial gate still verifies the rest of the
stack:

```powershell
.\scripts\phase53-ga-smoke.ps1 -SkipGo
```

GitHub Actions runs the Go checks.

## CLI Preview

```powershell
multidb template list
multidb template explain ai-memory --json
multidb init --guided --template ai-memory --name "Agent Memory" --out .\agent-memory
multidb config validate --spec .\agent-memory\multidb.yaml
multidb config explain --spec .\agent-memory\multidb.yaml
```

Checked-in examples are under `examples/`, with SDK examples under
`examples/sdk/`.

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
.\scripts\phase53-ga-smoke.ps1
```

## Known Limits

- MultiDB is a public-preview engine, not a mature production database.
- Phase 45 Cluster GA covers the local/process CP OpenRaft smoke contract; it
  does not claim managed Kubernetes automation, multi-region placement or an
  enterprise SLA.
- `config apply` is confirmation/audit-only for v1 and does not perform
  physical data migrations.
- Studio v1 focuses on validation, catalog, advice, security views, audit and
  migration dry-run workflows.
- Full PostgreSQL compatibility, Mongo wire protocol, public extension
  marketplace, SSO and external compliance certification are outside the
  current support claim.

## Documentation

- `docs/public-preview.md` - install, verify and first-use guide.
- `docs/ga-support-matrix.md` - supported surfaces and out-of-scope claims.
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
