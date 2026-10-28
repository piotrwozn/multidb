# MultiDB 1.0.0-rc.1 qualification

This is a release candidate, not a declaration of general production readiness.
Kubernetes is the deployment target for every storage profile. The chart provides
one database writer with persistent storage where the profile requires it.
Increasing the Deployment replica count is not a supported way to create a
replicated database cluster.

The changes remain in PR [#39](https://github.com/piotrwozn/multidb/pull/39).
No stable tag, release or production deployment has been published.

## Implemented scope

- Shared query memory, cancellation and deadlines extend to borrowed native
  reads, encrypted/compressed storage, remote segments, foreign scans,
  historical reads, statistics, graph traversal and materialized views.
- In-memory transactions share unchanged tables and byte buffers with snapshots;
  writers avoid cloning the complete database on each commit.
- Foreign PostgreSQL uses verified TLS, bounded protocol parsing, scalar type
  validation, bounded session checks and secret-safe connection driving. Parquet
  preflight precedes metadata, page and decoder allocations.
- HTTP authorization precedes body decoding; catalog and time-series reads
  respect row policies and masks. Probe handlers use cached health state and
  remain responsive while the database lock is held.
- Production startup requires a private audit signing key and a durable anchor
  for durable profiles. Existing audit history is verified before logical
  writes; a mismatched key never silently re-signs stored records.
- Backup publication and keyring updates handle uncertain filesystem outcomes.
  Restore supports a separately encrypted target, full/incremental recovery and
  PITR, with chain-integrity and key-recovery regressions.
- Internal replication transport authenticates peer identities with mutual TLS,
  certificate pins and revocation checks. Guarded AP/shard scans retain quorum,
  tombstone, ordering and cancellation semantics.
- Tensor, multimodal embedding and GraphRAG APIs remain available. Strict GPU
  profiles do not silently fall back to CPU after a device failure.
- Kubernetes configuration covers all CPU profiles, encrypted configuration,
  explicit volatile storage and NVIDIA/AMD/Intel GPU resource examples. It uses
  non-root containers, read-only root filesystems, explicit resources, private
  Secrets, startup/readiness/liveness probes, scoped service accounts, PVC
  retention, network policies and a single-writer upgrade strategy.
- Stable-release checks require source- and artifact-bound qualification reports,
  including the Kubernetes manifest matrix and actual CPU profile runtime tests.
  Missing, stale, skipped or contradictory evidence fails closed.

## Evidence and interpretation

Final candidate execution results and immutable artifact identifiers are recorded
below as qualification completes. A generated manifest, a unit test, an actual
container run and an intended-production deployment are separate kinds of proof.

| Check | Observed result |
| --- | --- |
| Rust formatting | Passed. |
| Release Clippy, all targets and all features, warnings denied | Passed. This configuration includes test-only insecure transport; official binaries use secure defaults. |
| Studio production build | Passed for `1.0.0-rc.1`. |
| Helm and Kubernetes schema/semantic matrix | Passed: 25 positive and 28 rejection cases; 172 rendered API objects. This includes GPU configuration validation, not GPU execution. |
| Dependency advisory and policy checks | Audit, deny and vet passed; zero known advisories in 649 dependencies. Vet still uses 647 bootstrap exemptions and has zero independent audit entries; see [the exact scope and debt](operations/supply-chain-rc1.md). |
| Final default Rust suite and executable | In progress; earlier test binaries do not qualify this candidate. |
| Kubernetes application runtime | Infrastructure is ready on a private single-node Kind 1.35.8 fixture; application matrix pending. |
| Physical GPU / intended-production cluster | Not available in this environment; not qualified. |

The earlier seven-engine comparison remains available in the
[2026-10-28 benchmark report](performance/2026-10-28-production-audit.md).
Its measurements belong to the exact older binary identified in that report;
they must not be attributed to this release candidate. Neither that comparison
nor an isolated storage benchmark establishes universal performance leadership.

## Requirements before stable 1.0.0

The release gate must receive actual accepted evidence for the intended supported
deployment, in addition to passing the exact candidate's software checks:

- A sustained workload on the selected production hardware, with agreed latency,
  throughput, dataset size, concurrency and resource limits. Include verified
  cold-cache and larger-than-RAM workloads where applicable.
- Production-cluster qualification using the intended Kubernetes version,
  StorageClass, CSI driver, enforcing CNI and topology. Exercise PVC recovery,
  upgrades, shutdown, storage exhaustion, network-policy enforcement and restore.
- Physical GPU qualification for each supported device/backend and strict GPU
  profile, including restart and device-failure behavior. Software Vulkan alone
  is insufficient.
- Real multi-host fault tests and an integrated, supported server topology before
  advertising multi-node high availability. Local process tests and a one-node
  Kind cluster do not prove zone failure tolerance.
- Accepted backup RPO/RTO, key custody and rotation, recovery/upgrade drills and
  operational incident procedures for that deployment.
- An independent security review and explicit resolution of dependency-audit
  debt. A clean advisory scan and cargo-vet bootstrap exemptions do not constitute
  an independent audit.
- Green required CI and provenance for the exact final Linux/Windows artifacts,
  their container image, checksums and signatures, followed by a clean-machine
  installation/rollback check.

These are release criteria, not features that can be certified by changing the
version number. The stable-release workflow refuses absent acceptance evidence.
