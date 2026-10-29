# Release Checklist

Use this checklist for every release candidate. Before a production 1.0.0
announcement, complete the scoped [production acceptance gates](production-readiness.md).

## Before Tagging

- Run `.\scripts\release-smoke.ps1`.
- Review `docs/support-matrix.md` and known limits against current tests.
- Confirm no `latest` Docker tag is configured in release workflows.
- Confirm SDK compatibility constants still match OpenAPI v1.
- For stable >=1.0.0, freeze and qualify the exact versioned runtime/build tree
  and both final artifact hashes. Commit complete reviewed evidence under
  `qualification/releases/<version>/manifest.json`; follow the
  [production gate contract](release-and-versioning.md#stable-production-gate).
- Require the separate Kubernetes proof bundle: full rendered profile/rejection
  matrix and actual eight-profile CPU plus encrypted wrong-key/reopen tests on
  the exact final Linux artifact. Bind all raw files and retain explicit Linux
  deployment scope in both platform manifests.
- Keep incomplete GPU, GraphRAG, cluster, Kubernetes, 24-hour/cold/large soak, recovery,
  operations or independent security evidence blocked. An RC is the supported
  publication path while these requirements remain unmet; do not create waivers.

## Release Workflow

- Update the server Cargo version, then tag the identical `vX.Y.Z`; the workflow rejects mismatches.
- Build official artifacts with default secure features, excluding `insecure-transport`.
- Check `release-policy`: RC and zero-major releases must be GitHub prereleases
  with `make_latest: false`.
- For stable >=1.0.0, require exact-artifact qualification after each platform
  build and again for downloaded artifacts before publication. Source CI and
  manifest structure alone cannot replace actual binary hash checks.
- Verify the release workflow completes Linux, Windows and publish jobs.
- Confirm the GitHub Release includes binaries, signatures, checksums, SBOM,
  image digest, image metadata and smoke/perf reports.
- Stable >=1.0.0 also includes the accepted Linux and Windows production
  qualification summaries. Recheck their source/artifact bindings against the
  published binaries; evidence-only commits may differ from the frozen source
  commit only when the complete bound runtime fingerprint stays identical.
- Verify `cosign verify-blob` for both binaries.
- Verify `cosign verify ghcr.io/<owner>/<repo>:vX.Y.Z@<digest>`.

## After Release

- Run the public quickstart from a clean checkout or machine.
- Pull the pinned GHCR image by tag and digest, start it, log in, run SDK
  examples and restart with the same volume.
- Record any support-matrix changes before announcing the release.
