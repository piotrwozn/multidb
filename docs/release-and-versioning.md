# Release And Versioning

## Artifact Policy

Tagged releases build:

- `multidb-linux-x86_64`
- `multidb-windows-x86_64.exe`
- `SHA256SUMS`
- binary cosign certificates and signatures
- `multidb.cdx.json`
- GHCR image `ghcr.io/<owner>/<repo>:<git-tag>`
- image digest and build metadata
- preview smoke and release performance reports

The release workflow signs binary blobs and the pushed image digest with
keyless Sigstore. It does not publish a `latest` image tag.

## Version Policy

- MultiDB server release tags use `vX.Y.Z`.
- The Control Plane API is versioned as API v1 through OpenAPI
  `x-multidb-api-version: 1`.
- Official SDKs expose `CONTROL_PLANE_API_VERSION = 1` and
  `MIN_MULTIDB_VERSION = "0.1.0"`.
- SDK package versions may advance independently, but breaking API changes
  require an OpenAPI version change, migration notes and contract tests for
  the previous behavior.
- Docker and Helm consumers should pin the Git-tag image and verify the signed
  digest from the release.

## Compatibility Rules

- Stable endpoints keep the enveloped JSON error shape and stable `code`
  values.
- Preview endpoints, including `/metrics`, may change only with release notes.
- `config apply` remains confirm/audit-only until a later release implements
  physical migrations.
- New durable formats must update the existing format registry and add
  migration or fail-closed compatibility tests.
