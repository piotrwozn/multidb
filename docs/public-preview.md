# MultiDB Public Preview

Public preview means MultiDB has an installable, repeatable starter path with
clear support boundaries. It is not a claim that every planned distributed
feature is GA.

## Install And Verify

Download the release binary for your platform, `SHA256SUMS`,
`multidb.cdx.json` and the matching `.sig`/`.pem` files from a tagged GitHub
Release. Phase 53 releases also include the signed GHCR image digest and build
metadata.

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

For a repository smoke test, run:

```powershell
.\scripts\preview-smoke.ps1
```

To smoke a release binary directly:

```powershell
.\scripts\preview-smoke.ps1 -Bin .\target\release\multidb.exe
```

## Docker Quickstart

Phase 49 adds a Docker runtime for users who do not want to build Rust or Node
locally:

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
| `production_cp` | Stable | CP OpenRaft cluster profile covered by the Phase 45 local/process smoke gate. |

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
| `strong_cp` | Stable | CP quorum semantics covered by the Phase 45 local/process smoke gate. |
| `eventual_ap` | Experimental | Conflict policy requires workload review. |

## Known Limits

- Phase 45 Cluster GA is complete for the local/process CP smoke contract; the
  preview still does not claim Kubernetes automation, multi-region placement or
  enterprise SLA.
- `config apply` is confirm/audit-only and does not perform physical data
  migrations.
- Studio v1 is read-only plus validation, catalog, advice and migration dry-run.
- Mongo wire protocol, full PostgreSQL compatibility, public marketplace,
  cloud KMS/HSM and enterprise SLA are outside preview scope.
- The release performance baseline is a versioned smoke gate, not a dedicated
  production-hardware benchmark claim.

## Migration Guide

- From SQLite-style local apps: start with `desktop-embedded` or
  `game-save`, model durable entities as `document_entity` or `key_value`, then
  validate and explain the generated `multidb.yaml`.
- From PostgreSQL-style data: use the PostgreSQL wire-compatible surface where
  it fits, or import CSV/JSONL/pg COPY text through the CLI. Unsupported DDL
  and catalog features fail closed.
- From Mongo-style documents: migrate through BSON/document import paths and
  use document APIs. Mongo wire protocol is deferred.
- From analytics pipelines: start with `analytics`, keep the columnar extension
  limits visible in explain output, and treat release perf reports as gate
  evidence rather than capacity planning.

## What Production Ready Means Here

For phase 53, production ready means the package is reproducible, signed,
documented, smoke-tested, guarded by SDK/Docker/Studio/release gates and honest
about limits. It does not mean every roadmap item is GA. The source of truth
remains `src/roadmap.rs`: `Complete` entries have evidence and no gaps in that
entry, `ProductionGap` entries have implemented surface with remaining
production work, and `Deferred` entries are future work.
