# Phase 53 GA Hardening

Phase 53 turns the post-preview Docker, Studio and SDK surface into a harder
release contract. GA here means reproducible, signed, smoke-tested and honest
about support boundaries; it does not add enterprise SLA, managed cloud,
multi-region guarantees or external compliance certification.

## Implemented Contracts

- Admin password login has an in-memory rate limiter with separate global and
  normalized-username buckets.
- Login lockout returns the same neutral `401 unauthorized` envelope as normal
  auth failure, does not emit `Retry-After`, skips password verification while
  locked and audits `login_rate_limited` with `Denied`.
- Runtime env controls for the limiter are
  `MULTIDB_ADMIN_LOGIN_MAX_FAILURES`,
  `MULTIDB_ADMIN_LOGIN_WINDOW_SECONDS` and
  `MULTIDB_ADMIN_LOGIN_LOCKOUT_SECONDS`; all clamp to at least `1`.
- `/studio` advertises `admin_login_rate_limit`.
- Official SDKs expose `CONTROL_PLANE_API_VERSION = 1` and
  `MIN_MULTIDB_VERSION = "0.1.0"`.
- Release tags build Linux and Windows x86_64 binaries, publish checksums,
  sign binary blobs, attach provenance, publish a GHCR image tagged only with
  the Git tag, attach image SBOM/provenance and sign the image digest.
- `scripts/phase53-ga-smoke.ps1` is the one-command GA gate over Rust, Studio,
  Playwright, Docker, SDKs, examples, release perf and supply-chain checks.

## Boundaries

Phase 53 does not publish SDK packages to external registries, add SSO, add a
public extension marketplace, claim full PostgreSQL or Mongo wire
compatibility, automate multi-node Kubernetes, add multi-region placement or
claim enterprise support terms.

Docker `latest` is intentionally not published. Consumers should pin exact
Git-tag image tags and verify the signed digest listed in the release.

## Acceptance Tests

Run:

```powershell
cargo test --lib admin -- --nocapture
cargo test --bin multidb serve_config -- --nocapture
cargo test --lib roadmap -- --nocapture
cargo test --all-features
.\scripts\sdk-smoke.ps1 -RequireGo
.\scripts\sdk-examples-smoke.ps1 -RequireGo
.\scripts\studio-check.ps1
Push-Location studio; npm run e2e; Pop-Location
.\scripts\docker-smoke.ps1
.\scripts\phase53-ga-smoke.ps1
```

The release workflow runs the same critical gates on tag pushes and publishes
the signed binary and image artifacts only after the gates pass.
