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
| CLI templates/config | Stable preview | `scripts/preview-smoke.ps1`, template smoke, config tests |
| Control Plane API v1 | Stable preview | OpenAPI v1, operation registry, SDK contract tests |
| Studio | Stable for documented admin workflows | Vitest, Playwright operator flow, Docker smoke |
| Docker image | Stable for single-node production-shaped runtime | Docker smoke, GHCR release image, signed digest |
| Helm chart | Supported runtime parity artifact | Helm values/templates match Docker env contract |
| SDKs | Stable preview for API v1 client surface in repo packages | TypeScript, Python, Go and Rust package gates |
| Metrics endpoint | Preview | Raw Prometheus text remains marked preview |
| `config apply` | Audit/confirmation only | No physical data migration in v1 |

## Profiles And Domains

| Area | Support Status |
| --- | --- |
| Certified local profiles and collection roles | Stable preview inside generated spec and smoke coverage |
| `strong_cp` local/process cluster contract | Stable preview for local/process smoke semantics |
| `eventual_ap` conflict policy | Experimental; workload review required |
| Graph role | Experimental |

## Out Of Scope

Multi-region guarantees, managed cloud, enterprise SLA, external compliance
certification, public extension marketplace, SSO, full PostgreSQL
compatibility and Mongo wire compatibility are not current support claims.
