# MultiDB GA Support Matrix

This is the GA support snapshot after Phase 53. It supersedes the public
preview packaging language where the same surface is covered by checked-in
tests and release artifacts.

## Global Gates

- `scripts/phase53-ga-smoke.ps1` is the combined local gate.
- CI covers Rust fmt, clippy, all-feature tests, SDK smoke, SDK examples,
  Studio tests, Playwright, Docker smoke, fuzz smoke, perf gate, cargo-deny,
  cargo-audit, cargo-vet and SBOM generation.
- Release tags publish signed Linux and Windows x86_64 binaries, checksums,
  CycloneDX SBOM, provenance, a signed GHCR Docker image and release smoke
  reports.
- `latest` Docker tags are not part of the GA policy.

## Product Surface

| Surface | Status | Evidence |
| --- | --- | --- |
| CLI templates/config | GA | `scripts/preview-smoke.ps1`, template smoke, config tests |
| Control Plane API v1 | GA | OpenAPI v1, operation registry, SDK contract tests |
| Studio | GA for documented admin workflows | Vitest, Playwright operator flow, Docker smoke |
| Docker image | GA for single-node production-shaped runtime | Docker smoke, GHCR release image, signed digest |
| Helm chart | Supported runtime parity artifact | Helm values/templates match Docker env contract |
| SDKs | GA for API v1 client surface in repo packages | TypeScript, Python, Go and Rust package gates |
| Metrics endpoint | Preview | Raw Prometheus text remains marked preview |
| `config apply` | Audit/confirmation only | No physical data migration in v1 |

## Profiles And Domains

| Area | GA Status |
| --- | --- |
| Certified local profiles and collection roles | GA inside generated spec and smoke coverage |
| `strong_cp` local/process cluster contract | GA for Phase 45 smoke semantics |
| `eventual_ap` conflict policy | Experimental; workload review required |
| Graph role | Experimental |

## Out Of Scope

Multi-region guarantees, managed cloud, enterprise SLA, external compliance
certification, public extension marketplace, SSO, full PostgreSQL
compatibility and Mongo wire compatibility are not GA claims.
