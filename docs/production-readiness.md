# Production 1.0.0 acceptance

The server is version 1.0.0-rc.1, a release candidate with production qualification
in progress. A code audit, passing unit
tests and faster local benchmarks are evidence for a release candidate; they do
not establish a production support contract. Do not label this branch 1.0.0 or
publish a production-ready claim until the applicable gates below have recorded
results for the exact release commit and artifact hashes.

## Choose the supported release scope

A feasible first production scope is the native embedded/single-node database
and its explicitly documented API, SDK and SQL subset. Specify supported operating
systems, filesystems, maximum tested data sizes, concurrency, deployment topology,
backup procedure, retention and the compatibility window. Incomplete PostgreSQL
syntax support must remain explicit; a wire-compatible connection is not a full
PostgreSQL implementation.

GraphRAG, GPU tensor datasets, AP replication, sharding, online migration, remote
sources and live multi-node CP each need separate qualification before becoming
part of that contract. They can remain visibly preview features in a 1.0.0 core
release. If the announcement promises them as production features, their gates
are blockers for that announcement too.

## Required evidence for the core

| Gate | Required acceptance evidence |
| --- | --- |
| Exact release artifacts | Green required CI on the release commit: formatting, Clippy, default/all/no-default Rust configurations, MSRV, SDKs, Studio, browser and container smoke, mandatory fuzz smoke, dependency policy and SBOM. Build the release with default secure features; `insecure-transport` is test-only and must not enter official binaries. Verify binary/image signatures and checksums from a clean machine. The tag must equal the embedded Cargo version. |
| Data survival | Repeated abrupt process termination during committed writes, batch conflicts, checkpoint/reopen, restore publication and GC; every acknowledged immediate-durability write survives, and failed atomic writes never partially appear. Exercise storage error injection and disk-full/short-write cases. A process-kill drill is not a physical power-loss or filesystem certification. |
| Backup and upgrade | Perform and record full plus incremental/PITR recovery into a fresh target, compare complete logical data and schema, test damaged/missing chain members, and verify the previous supported version's upgrade path. Plaintext and explicitly configured encrypted restore targets are implemented, with independent source/target keys and fail-closed keyring recovery. The [RC recovery record](production-qualification-1.0.0-rc.1.md) covers actual full plus incremental/PITR recovery, damaged-chain refusal, encryption-key rotation and private audit signatures on the candidate runtime. Keep the required keys and exercise key recovery/rotation. Prior-version upgrades and deployment-specific RPO/RTO acceptance remain required; the local drill does not establish those operational targets. |
| Sustained load | A sustained soak on the intended hardware and storage, including concurrent reads/writes, slow clients, disconnects, reconnects, backup/retention work, large results and denied-policy requests. Set workload-specific p95/p99 latency, error-rate and resource ceilings before the run; accept the result explicitly. Track RSS, disk/WAL growth, file descriptors, queue depth, recovery and cache churn. |
| Security review | Independent review of authentication, authorization/policy boundaries, tenant separation, wire decoding, extension/foreign-source trust, encryption/key lifecycle and audit durability. Resolve confirmed blockers and rerun their regressions. Freeze exposure rules for the HTTP reverse proxy, pgwire TLS, metrics, secrets and operator-only local APIs. |
| Operations | A tested deployment runbook with readiness/shutdown behavior, volume ownership, disk-space alerts, backup scheduling, restore drills, key custody, monitoring, incident response and rollback/restore decisions. Establish the supported versions and vulnerability response policy. |
| Performance claims | Publish reproducible measurements for the release artifact, exact versions/configuration and real workloads, with correctness and failures alongside latency/throughput. Include cold/warm and data-larger-than-RAM scenarios relevant to the deployment. Fit-in-RAM loopback comparisons do not establish production tail latency or universal superiority. |

## Additional feature qualification

| Feature included in a production claim | Additional evidence required |
| --- | --- |
| GPU tensor profile | Real supported NVIDIA/AMD/Intel adapters, appropriate Vulkan/Metal/DX12 platforms and Docker device passthrough; numerical reference checks, device loss, deadlines, restart and memory-pressure tests. Record the explicitly supported f32 numerical range, operations, shapes and batch limits. Software-adapter WGSL validation is useful but does not certify physical GPUs or GPU throughput. This is a tensor dataset implementation, not a Deep Lake wire/SDK compatibility promise. |
| GraphRAG/vector retrieval | Recall against exact references, policy/citation integrity and concurrent mutation cases; qualify cold index loading, index memory and deadline behavior at the supported collection size. Document the absence of a cross-model snapshot and unsupported server-side filtering surfaces. |
| AP/sharded queries | Bounded backend scanning and cancellation beyond native single-node paths, pressure testing, convergence/conflict semantics, partition/failure recovery and durable routing state. Limits on retained query results alone do not bound a legacy backend's temporary materialization. |
| Multi-node CP/2PC | Failover and partition testing on separate processes/hosts with durable decisions, prepared-key isolation, recovery and unique transaction IDs across restarts; authenticate claimed node identities against certified peers. Embedded `Database` constructors must not imply live cluster integration they do not provide. |
| Online shard migration | Prove routing/write barriers, complete model-keyspace transfer, collision handling, failure recovery and consistent reads under concurrent traffic; define ownership of direct backend handles and coordination between router instances. |
| Foreign sources/extensions | Bound source materialization and cancellation, approve network/filesystem/secret access and validate platform/runtime limits. A local operator configuring a source is a different trust boundary from an untrusted SQL caller. |

These are acceptance requirements and scope decisions, not a claim that every
possible code path has been independently proven. Attach execution logs and
artifact hashes to a release-candidate record, keep unsupported features marked
preview, and update this document when a gate obtains reproducible evidence.
