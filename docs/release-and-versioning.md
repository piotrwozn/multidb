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
- production qualification reports for stable versions at or above 1.0.0

The release workflow signs binary blobs and the pushed image digest with
keyless Sigstore. It does not publish a `latest` image tag.

## Version Policy

- MultiDB server release tags use `vX.Y.Z`, or a semantic prerelease such as
  `v1.0.0-rc.1`. The tag must exactly match the Cargo package version.
- Release candidates and all zero-major versions are GitHub prereleases with
  `make_latest: false`. A successful RC build never grants production status.
- Stable versions at or above 1.0.0 require the executable production gate below
  before any GHCR publication and again before GitHub Release publication.
- The Control Plane API is versioned as API v1 through OpenAPI
  `x-multidb-api-version: 1`.
- Official SDKs expose `CONTROL_PLANE_API_VERSION = 1` and
  `MIN_MULTIDB_VERSION = "0.1.0"`.
- SDK package versions may advance independently, but breaking API changes
  require an OpenAPI version change, migration notes and contract tests for
  the previous behavior.
- Docker and Helm consumers should pin the Git-tag image and verify the signed
  digest from the release.

## Stable Production Gate

[`check-production-readiness.py`](../scripts/check-production-readiness.py)
fails closed without complete local, reviewed evidence. It has no URL downloader,
waiver or feature-omission option. GPU tensor execution, GraphRAG and the cluster
features requested for this release all require acceptance, alongside core CI,
sustained load, recovery, operations and independent security review. Missing,
unrun, blocked, failed, duplicate, unknown, conflicting, nonfinite or changed
evidence cannot become a stable release through this workflow.

The workflow checks the **actual final** Linux and Windows executables after
their release builds. All publication waits for both platform jobs. The publish
job rechecks both downloaded executables and the tested image-context binary
before the GHCR push and binary signing/publication. These checks
are distinct from source CI and from checking a manifest without a local binary.
RC builds can proceed while production gates are blocked, but remain prereleases.
No tag or release is created by the local checker.

The default workflow reads
`qualification/releases/<exact-version>/manifest.json` under the checkout. An
operator can use another reviewed local root through `--evidence-root`; remote
URLs, symlink paths, paths escaping that root and hash mismatches are refused.
Both `linux-x86_64` and `windows-x86_64` must be listed with exact SHA256 values.
Each target's gates are checked against that target's actual executable.

```sh
python3 scripts/check-production-readiness.py --policy
python3 scripts/check-production-readiness.py --fingerprint --source-revision <frozen-source-commit>
python3 scripts/check-production-readiness.py --requirements security
python3 scripts/check-production-readiness.py \
  --evidence-root /reviewed/local/root \
  --manifest /reviewed/local/root/qualification/releases/1.0.0/manifest.json \
  --target linux-x86_64 --binary /immutable/final/multidb \
  --output /new/qualification-linux.json
```

The final command returns zero only for accepted evidence for that actual target
artifact. A blocked result is JSON with reasons and a nonzero exit status.
`--policy`, `--fingerprint` and `--requirements` only describe policy/source/schema;
they are not production acceptance and never substitute for the final command.

### Source and evidence commits

Freeze and commit the exact runtime/build/release inputs, including the intended
Cargo version, before building and qualifying the candidate. Record that immutable
commit and the tool's `runtime_fingerprint`. The fingerprint binds Git-tracked
source, Cargo manifests/locks/version, SDKs, frontend inputs, scripts, workflows,
build and registry configuration. It rejects unbound source symlinks/submodules.
Literal Rust `include_str!`/`include_bytes!` inputs remain bound even when they
are documentation. Unresolved include expressions fail closed.

Ordinary narrative Markdown under `docs/`, root `README.md`, and the narrowly
reserved evidence trees `qualification/evidence/`, `qualification/releases/`,
`docs/release-evidence/` and `docs/performance/` are excluded unless compiled in.
Put new qualification reports/raw evidence inside those reserved trees, or keep
them in an explicitly reviewed local directory. Do not put code/build inputs in
an evidence tree. A later evidence-only commit can then have a different commit
SHA without changing the qualified runtime fingerprint. The manifest records the
frozen runtime commit, not its own as-yet-unknown commit hash. Both that commit and
the current checkout must produce the recorded fingerprint, with no uncommitted
runtime/build changes or untracked source inputs.

Version scalars and lockfile versions are **not** normalized away. Moving from an
RC to stable creates a new artifact requiring new qualification. Build candidates
without publishing a tag using the same controlled path/toolchain/build recipe as
the intended release, preserve the exact binaries, run the intended-environment
qualification, then commit reviewed evidence and tag only when every gate is
accepted. A different compiler, build environment or nondeterministic output can
change the artifact hash; the release gate will block and qualification must be
repeated for the actual artifact. Passing two builds on one runner does not prove
cross-platform or cross-machine reproducibility.

### Manifest and accepted report contract

Manifest schema 1 has exactly these fields:

```json
{
  "schema_version": 1,
  "target_version": "1.0.0",
  "source": {
    "runtime_source_revision": "<40 lowercase hex characters>",
    "runtime_fingerprint": "<64 lowercase hex characters>"
  },
  "requested_features": ["physical_gpu", "graphrag", "cluster"],
  "artifacts": {
    "linux-x86_64": {"sha256": "<actual artifact SHA256>", "gates": {}},
    "windows-x86_64": {"sha256": "<actual artifact SHA256>", "gates": {}}
  }
}
```

This skeleton is deliberately incomplete and blocked. Each `gates` object must
contain exactly `core_ci`, `soak`, `recovery`, `operations`, `kubernetes`, `security`,
`physical_gpu`, `graphrag` and `cluster`. An entry has exactly `status`, `report`
and nonempty `raw_proofs`; report/proof references have exactly relative `path`
and `sha256`. Every entry must be accepted. The normalized accepted report has
exactly:

- `schema_version: 1`, `gate`, `status: accepted`;
- `binding` containing `runtime_source_revision`, `runtime_fingerprint`, actual
  `artifact_sha256`, target and exact version;
- `criteria_sha256` from `--requirements <gate>`;
- `checks` containing every required check from that command, each with exactly
  `status: accepted` and a nonempty `proofs` array naming bound raw files;
- `raw_proofs` mapping those paths to their exact hashes; this set must match
  the manifest entry, with no duplicates or self-referential report;
- `review` recording the real reviewer, `acceptance_record` and timezone-aware
  `accepted_at_utc`; security additionally requires `independent: true` and
  an actual review organization;
- `facts` with the applicable executed-environment facts checked by the script.

Required facts include successful `check`/`msrv`/`supply-chain`/`fuzz` and
`secure-default`/`kubernetes-tooling`/`kubernetes-runtime` jobs for
the frozen runtime commit and default secure features; a predeclared **24-hour**
intended-environment soak with zero unexpected errors and verified cold/large
pressure; encrypted source/target recovery, previous-version upgrade and accepted
RPO/RTO; exercised operations and delivered alerts; zero unresolved security
blockers; real GPU identity; GraphRAG mean/minimum recall of at least .95/.8 with
zero policy/citation violations and bounded cold-index execution; and at least
three distinct cluster hosts with certificate-bound identities. Inspect the script
and machine-readable requirements when preparing the normalized reports. A local
15-minute soak, software Vulkan test, same-host cluster or unreviewed template
cannot satisfy these stronger production gates.

The separate `kubernetes` gate parses the actual static and runtime reports;
successful CI job names alone cannot satisfy it. Its `facts` have exactly
`runtime_target: linux-x86_64`, `runtime_artifact_sha256` matching the manifest's
final Linux executable, `runtime_scope: linux_deployment_only`, and three relative
raw-proof paths: `static_report`, `runtime_report`, `image_report`. Keep the
runner's directory layout when copying the proof bundle into the reviewed root.
Every referenced values/rendered/schema-cache/command-output file, runtime log,
and OCI archive must also appear in `raw_proofs` with its exact SHA256. The checker
requires the full positive and negative chart matrix, all eight CPU profiles,
actual TLS/policy/graceful/replacement/readback results, and wrong-key encrypted
startup rejection followed by correct-key readback. Chart, validator, runtime
runner, OCI converter and Dockerfile hashes must match the qualified source.

Both platform entries require this gate, but its deployment scope is explicitly
Linux: a Windows normalized report also references the exact Linux deployment
artifact from the manifest and must not claim Windows execution in Kind. A CI
default-feature binary is reusable only if its bytes match the final Linux
release artifact. Local Kind acceptance does not satisfy physical GPU, HA,
enforcing CNI or intended-production-environment gates.

The [qualification runners](operations/qualification.md) retain the raw local and
external reports. A responsible reviewer normalizes their complete evidence into
this release contract; a wrapper must not mark missing checks accepted. Hashes
bind evidence bytes and source/artifact identity. They do not certify a claimed
external reviewer, establish trustworthy hardware facts by themselves, or replace
independent security/operations acceptance. Where the current environment lacks
those people, platforms or hardware, the correct release status remains blocked.

## Compatibility Rules

- Stable endpoints keep the enveloped JSON error shape and stable `code`
  values.
- Preview endpoints, including `/metrics`, may change only with release notes.
- `config apply --plan` and `POST /config/apply` remain confirm/audit-only
  until a later release implements physical migrations. Spec provisioning
  (`config apply --spec`, `POST /provision/apply`) is a preview that creates
  and adopts objects but never drops or alters them (see
  [provisioning.md](provisioning.md)).
- New durable formats must update the existing format registry and add
  migration or fail-closed compatibility tests.
