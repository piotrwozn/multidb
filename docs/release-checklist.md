# Release Checklist

Use this checklist for every release candidate.

## Before Tagging

- Run `.\scripts\release-smoke.ps1`.
- Review `docs/support-matrix.md` and known limits against current tests.
- Confirm no `latest` Docker tag is configured in release workflows.
- Confirm SDK compatibility constants still match OpenAPI v1.

## Release Workflow

- Tag `vX.Y.Z`.
- Verify the release workflow completes Linux, Windows and publish jobs.
- Confirm the GitHub Release includes binaries, signatures, checksums, SBOM,
  image digest, image metadata and smoke/perf reports.
- Verify `cosign verify-blob` for both binaries.
- Verify `cosign verify ghcr.io/<owner>/<repo>:vX.Y.Z@<digest>`.

## After Release

- Run the public quickstart from a clean checkout or machine.
- Pull the pinned GHCR image by tag and digest, start it, log in, run SDK
  examples and restart with the same volume.
- Record any support-matrix changes before announcing the release.
