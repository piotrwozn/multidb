# Production audit follow-up — 2026-10-28

This follow-up reviews the code delivered by PR #38 and records the corrected
behaviors, regression coverage and release qualification still required. The
starting tree is equivalent to the squash merge on `main` at
`9df545c143cbadc0bb01f579753a70f8481aab9c`.

The work covers the database, SQL and wire interfaces, model APIs, replication,
storage lifecycle, SDKs, Studio and verification tooling. It is a source audit
with automated regression evidence, not an independent security certification or
a proof of every execution path. The server remains version 0.1.0/public preview.
The acceptance contract is [production readiness](production-readiness.md);
[competitor benchmark methods](performance-competitor-benchmarks.md) describe
what the local performance measurements can establish.

## Corrected behaviors and regression coverage

The table describes the resulting invariants. Test links identify concrete
regressions; their presence is separate from execution of the final integrated
suite, recorded below.

| Area | Resulting behavior | Implementation and regression evidence |
| --- | --- | --- |
| SQL authorization and statistics | `ANALYZE` checks both administrative permission and source reads before executing any statement. Row or masking policies cannot expose raw statistics through unsupported analysis. Graph SQL helpers and materialized reads fail closed when their underlying relations are protected. | [analysis guards](../src/query/analyze_guard_tests.rs), [database authorization regressions](../src/db/review_fixes.rs), [SQL dispatch](../src/query/mod.rs) |
| SQL grammar and cached plans | Optimized paths accept only the grammar they implement; qualifiers, aliases and wildcard modifiers cannot silently change meaning. Unicode string operations use DataFusion's string expressions. A cached plan rechecks a partial index's applicability for each new literal. Nested `VALUES` obey statement limits. | [simple grammar](../src/query/simple_grammar.rs), [parser](../src/query/parser.rs), [optimizer](../src/query/optimizer.rs), [partial-index regression](../src/query/audit_tests.rs), [row-scan regressions](../src/query/row_scan/tests.rs) |
| Query resources | Native query paths share statement memory reservations for source, result and cache data, stop bounded scans early, and carry deadlines/cancellation through execution. Custom dispatchers also enforce SQL text limits. Legacy distributed or external backend materialization has separate qualification limits. | [statement budget](../src/query/budget.rs), [query execution](../src/query/mod.rs), [bounded row scans](../src/query/row_scan.rs), [custom-dispatch regressions](../src/db/review_fixes.rs) |
| Authentication and pgwire | Credential rotation uses a compare-and-set after password verification. Existing password sessions recheck the live credential fingerprint; an old delayed login cannot restore a revoked credential. Frame structure is checked before decoding, parameter substitution respects SQL tokens, and retained statements/portals/results have aggregate limits and release accounting on replacement or close. | [authentication rotation](../src/admin/auth_rotation_tests.rs), [frame checks](../src/network/frame.rs), [parameters](../src/network/parameters.rs), [session accounting](../src/network/session_state.rs), [sessions](../src/network/session.rs) |
| Document/index atomicity | Conditional document mutations do not silently degrade to unconditional writes on unknown or unsupported backends. Plain multi-node AP document LWW remains an explicit, narrowly validated index-free operation. Indexed single-node AP mutations retain atomic source/index updates. | [model mutations](../src/model/mod.rs), [conditional mutation regressions](../src/model/conditional_tests.rs), [AP conditions](../src/repl/ap/conditional.rs), [database catalog guard](../src/db/review_fixes.rs) |
| Full-text refresh | Refresh reconciles the current source document and conditionally updates source/index/configuration/frontier state. A conflicting writer prevents stale publication and frontier advancement. Searches suppress stale or deleted source documents until repair. Full repair's retry/partial-progress behavior remains explicit. | [full-text implementation](../src/text.rs), [interleaved refresh regressions](../src/text_refresh_race_tests.rs) |
| CDC and derived views | Subscription creation/adoption/acknowledgement preserve ownership and monotonic positions under conditional metadata updates. Continuous-query offsets advance with subscription state. Aggregate groups disappear when their last source member leaves; legacy member counts are recovered, stale refreshes cannot overwrite newer state, overflow is rejected, and unsupported columnar sources are refused before metadata publication. | [CDC](../src/cdc.rs), [continuous queries](../src/continuous.rs), [metadata race regressions](../src/cdc_metadata_race_tests.rs), [view integrity regressions](../src/cdc_view_integrity_tests.rs) |
| Temporal history and GC | History uses actual commit timestamps, closes an interval at its next version or deletion, and does not invent a deletion row. Commit metadata is cached by LSN during the request; missing metadata fails closed. A retained pre-GC anchor remains usable for an eligible point-in-time read but cannot masquerade as complete history. Retention filtering preserves complete histories independently of unrelated older GC activity. | [history implementation](../src/temporal.rs), [interval/GC/timestamp regressions](../src/temporal/tests.rs), [provisioning history-floor contract](../src/provision/tests.rs) |
| Backup and file publication | Private files/directories use restrictive permissions. Restore validates paths, records, chain ancestry and bounded payloads before publishing a closed, synced staging database without clobbering a target. Existing empty paths and links are also targets. Backup payload and decompression limits are explicit; volatile restore is refused. A new full backup is required before continuing legacy chains without ancestry metadata. | [backup implementation](../src/backup.rs), [restore regressions](../src/backup/audit_tests.rs), [file publication](../src/fileio.rs), [backup/restore guide](backup-and-restore.md) |
| Encryption and audit durability | Restore does not create a missing keyring. Envelope key publication/rotation uses exclusive publication, a stable sibling lock and stale-state checks; debug output redacts Vault tokens. Audit failure seals the handle, recovery verifies the signed prefix before appending, and checkpoint reuse requires trustworthy file-change evidence. Corrupted evidence is preserved for operator recovery. | [encrypted storage](../src/storage/encrypted.rs), [audit batching](../src/db/audit_batch.rs), [anchor verification](../src/db/audit_batch/anchor.rs), [audit recovery contract](audit-anchors.md) |
| Tensor datasets and GPU execution | Persisted manifests, revisions and chunks are validated before use. Decoded chunks share a capacity-aware cache budget; operations account for outputs and carry deadlines through CPU/GPU batches and serial GPU submission. A timed-out submitted device is stopped and requires restart. Scaled L2 computation preserves small differences beside large equal coordinates; unsupported conversion/subnormal ranges are rejected. A strict GPU profile cannot be bypassed by explicitly requesting CPU search. | [tensor storage/API](../src/tensor.rs), [database tensor integration](../src/db/tensor.rs), [GPU execution](../src/tensor_gpu.rs), [GPU datasets](../src/tensor_gpu/dataset.rs), [kernel](../src/shaders/tensor_compute.wgsl), [documented limits](tensor-datasets.md) |
| Vectors, graph and GraphRAG | Distance accumulation uses stable wider arithmetic and rejects unrepresentable results. Dimensions and search parameters are capped; unsupported HTTP filters are rejected explicitly. Query authorization, governor checks and audit apply to retrieval. Graph expansion uses bounded neighbor scanning. Small indexes can use an exact candidate path only when the complete index fits the request budget; larger indexes retain approximate retrieval semantics. | [vector implementation](../src/vector/mod.rs), [database vector queries](../src/db/vector_search.rs), [graph traversal](../src/graph.rs), [GraphRAG contract](graphrag.md) |
| Prepared transactions and AP writes | Prepared 2PC keys remain isolated from conflicting public reads/writes, ranges and cache tokens. Decisions are durable and immutable, completion is atomic/idempotent, and transaction identities survive restarts; absence of a decision does not imply abort. Single-node AP/RF1 conditional writes atomically update data/index/version/Merkle/clock state. Durable per-node AP clock reservations avoid reuse across concurrent publications and restarts. | [2PC](../src/repl/dist_txn.rs), [prepared locks](../src/repl/dist_txn/locks.rs), [2PC regressions](../src/repl/dist_txn/audit_tests.rs), [AP replication](../src/repl/ap.rs), [AP condition regressions](../src/repl/ap/conditional/tests.rs) |
| Sharding and internal transport | A router's migration barrier covers route selection through backend access; migration copies the registered model keyspaces in bounded batches before changing ownership. Failure preserves the original owner. Scatter reads filter current ownership. Internal messages receive typed nonallocating structural checks with wire/decoded limits and no trailing bytes. | [shard router](../src/repl/shard.rs), [migration regressions](../src/repl/shard/migration_tests.rs), [internal transport](../src/internal_transport.rs), [frame budgets](../src/internal_transport/frame_budget.rs), [transport regressions](../src/internal_transport/frame_budget/tests.rs) |
| Cloud leases and federation | Cloud lease operations share a stable sibling lock; expired/revoked owners cannot overwrite a successor, and TTL overflow is rejected. Federated queries validate complete schemas before applying projection once; invalid columns fail early, and `LIMIT 0` avoids unnecessary source access. | [cloud leases](../src/cloud.rs), [lease regressions](../src/cloud/lease_tests.rs), [federation](../src/federation.rs), [federation regressions](../src/federation/tests.rs) |
| Control-plane resources and Studio assets | Static files and SPA fallback remain inside the canonical asset root, including symlink handling. Asset reads are bounded and offloaded; `HEAD` reports the real length. Metrics connections, headers and request duration are bounded, and counters saturate safely. | [control-plane implementation](../src/admin.rs), [asset regressions](../src/admin/static_asset_tests.rs), [metrics implementation](../src/observability.rs), [HTTP metrics regressions](../src/observability_http_tests.rs) |
| SDK HTTP contracts | The Rust client uses a verified TLS-capable HTTP implementation with connection pooling, deadlines and response-size limits. SDKs refuse redirects by default, preserve typed server failures and intentional provisioning/readiness response contracts, and handle malformed error bodies without hiding the HTTP failure. | [Rust client](../sdk/rust/src/lib.rs), [Rust HTTP regressions](../sdk/rust/tests/http_transport.rs), [Python client/tests](../sdk/python/tests/test_client.py), [Go client/tests](../sdk/go/client_test.go), [TypeScript client/tests](../sdk/typescript/test/client.test.mjs) |
| Release and benchmark gates | Official binaries use default secure features. Release tags must match the embedded version; signature verification identifies this repository's exact release workflow/tag. CI requires fuzz smoke and additional build configurations. Performance gates reject missing, duplicate, nonfinite or inconsistent metrics; competitor reports require successful correctness verification and clean campaign completion. | [release workflow](../.github/workflows/release.yml), [CI](../.github/workflows/ci.yml), [version guard](../scripts/check-release-version.py), [fuzz smoke](../scripts/fuzz-smoke.ps1), [performance gate](../scripts/perf_gate.ps1), [competition runner](../scripts/benchmarks/competition.py), [independent domain verifier](../scripts/benchmarks/verify_domains.py) |

## Completed supporting checks

These checks were executed locally during the audit. Their logs are in the
audit workspace; final artifact identifiers and integration results are recorded
in the release-evidence section below. A dependency-policy pass does not replace
an independent security review.

| Check | Recorded result |
| --- | --- |
| Rust SDK | 14 tests passed: 4 unit, 4 HTTP transport, 3 GraphRAG and 3 tensor tests; SDK MSRV 1.89 check passed. |
| TypeScript SDK | 11 tests passed; npm audit reported zero known advisories. |
| Python SDK | 13 tests passed. |
| Go SDK | Package test suite passed. |
| Studio | Production build passed; 31 Vitest tests and 2 Chromium desktop/mobile browser tests passed. Tests used system Chromium 154.0.8037.57. |
| Studio dependencies | Compatible dependency updates to Vite 8.3.5 and Vitest 4.1.12 plus compatible transitive fixes; npm audit reported zero known advisories. No forced npm upgrade was used. |
| Rust dependency policy | `cargo deny` advisories, bans, licenses and sources gates passed. `cargo audit --deny warnings` completed for 646 root dependencies and 47 Rust SDK dependencies. |
| Supply-chain review metadata | `cargo vet --locked` succeeded with 644 exemptions. These exemptions are not 644 independent audits. |
| Benchmark-tool regressions | 47 tests passed with the optional benchmark dependencies installed. A standard-library-only run passed with 3 optional-dependency skips. Fixture timing output from these tests is not competitor performance evidence. |
| Software GPU kernel validation | The explicitly invoked SwiftShader Vulkan kernel regression passed. This validates a software adapter, not physical GPU compatibility or throughput. |
| Targeted storage lifecycle | Backup/restore 44, encrypted storage 22 and private file publication 3 targeted regressions passed before final integration. Final integrated Rust results supersede these subset counts. |

Additional verification tooling includes tests of the release-version guard,
the PowerShell performance-gate self-test and rejection of missing mandatory
fuzz tooling. Testing that missing fuzz tooling is rejected is distinct from
executing the fuzz targets.

## Final release evidence

Final Rust integration, the default-feature release artifact, the corrected
seven-engine campaign and the process-kill/restore drill are pending at the time
this report is prepared. Populate this section with the exact source commit,
artifact SHA-256, commands, counts, correctness results and evidence links after
those runs finish. Do not infer completion from the source coverage table.

The pre-audit SQL baseline is complete: 117,780 raw samples across 168 sections,
with no setup/correctness failures, independently verified raw results, an
unchanged binary hash and a clean server exit. Its binary SHA-256 is
`3b881025915d07dc147aef7086f1dd56b50d06dd01a2824c01f5cefe1bc3eb59`.
This baseline covers PostgreSQL and MongoDB comparisons only; it is not a
completed comparison against the seven-engine set.

## What still qualifies a production claim

Local tests and fit-in-RAM loopback timings can support a release candidate.
They do not establish a production SLO, independent security sign-off or a
hardware support matrix. Before a 1.0.0 core announcement, the release record
must include the applicable gates in [production readiness](production-readiness.md):

- Green required CI and verified signed artifacts for the exact release commit,
  including container and real mandatory fuzz execution.
- Storage-error and disk-full testing, complete incremental/PITR and previous
  supported-version upgrade drills, key recovery, and accepted RPO/RTO.
- Sustained concurrent load on supported production hardware, with explicit
  latency, error and resource ceilings and operational recovery checks.
- Independent security review and an exercised deployment, monitoring,
  backup/restore, incident-response and rollback runbook.
- Separate qualification for every preview feature included in the production
  promise: physical GPU adapters, GraphRAG/vector scale and recall, distributed
  partitions/failover and peer identity, multi-router migration and bounded
  external/distributed scans.

The current environment has no physical GPU. A process-kill drill is not a
power-loss test, and a CPU tensor comparison is not a GPU benchmark. SQL wire
compatibility and the implemented SQL subset also remain different contracts.
