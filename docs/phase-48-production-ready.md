# Phase 48 Public Preview Packaging

Phase 48 closes the public preview packaging path. It does not claim that every
future cluster feature is GA; it makes the preview installable, testable and
honest about support boundaries.

## Implemented Contracts

- The public preview guide is checked in at `docs/public-preview.md`.
- The preview path is CLI-first:
  - `multidb template list`,
  - `multidb template explain ai-memory --json`,
  - `multidb init --guided --template ai-memory --name ... --out ...`,
  - `multidb config validate --spec ...`,
  - `multidb config explain --spec ...`.
- `scripts/preview-smoke.ps1` exercises that path against either an installed
  `multidb` binary, an explicitly provided binary, or a debug binary it builds
  locally.
- The smoke writes a versioned JSON summary under `target/preview-smoke/` by
  default and refuses to clear work directories outside `target/`.
- Release tags run the preview smoke against the release binary before SBOM,
  checksum, signature, provenance and GitHub Release publication.
- The support matrix states the preview status of phases 0-48, including the
  phase 45 CP Cluster GA smoke contract.

## Boundaries

Public preview is a product packaging contract, not a new runtime API. It does
not change `DatabaseSpec`, create a second configuration format, publish an
enterprise SLA, or add Kubernetes/multi-region automation on top of the phase 45
CP cluster contract.

Studio remains read-only plus validation and migration dry-run. The preview
guide points users to the existing Studio contract instead of adding a new
apply surface.

## Acceptance Tests

Run:

```powershell
cargo test --lib roadmap -- --nocapture
cargo test --bin multidb template_ -- --nocapture
.\scripts\templates-smoke.ps1
.\scripts\preview-smoke.ps1
.\scripts\check.ps1
```

Release acceptance additionally requires the tag workflow to complete with
preview smoke, performance gate, SBOM, checksums, cosign signature and GitHub
provenance artifacts.
