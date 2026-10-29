# MultiDB production-audit benchmark — 2026-10-28

The qualified comparison completed all six workload domains against the seven
agreed competitors: PostgreSQL, MongoDB, DuckDB, Redis, Qdrant, Neo4j and Deep
Lake. It contains **378 measured sections, 130 aggregated result cells and
156,030 timed requests, with zero recorded timed-request errors**. Each campaign
passed its correctness/raw-data checks, retained the same MultiDB artifact hash
and ended with a clean server exit. The full-suite report combiner accepted the
six campaigns with `--require-full-suite`.

The result is mixed. MultiDB has strong results for the tested 100k-row grouped
SQL aggregate and bounded graph traversals. PostgreSQL and MongoDB lead most
small SQL reads and durable mutations; Redis leads the KV workloads; DuckDB
generally leads analytics; Qdrant leads ANN latency and recall; Deep Lake leads
the tested CPU tensor search and projected reads. The Analytical profile does
not consistently beat MultiDB's Transactional profile on these small fixtures.
These measurements support workload-specific decisions, not a universal engine
ranking or a claim that optimization is complete.

See the [source audit](../production-audit-2026-10-28.md),
[production acceptance gates](../production-readiness.md) and
[reproduction methods](../performance-competitor-benchmarks.md). The complete
table is included below; [JSON](2026-10-28-production-audit.json) and
[CSV](2026-10-28-production-audit.csv) retain unrounded aggregate values. The
[evidence archive](2026-10-28-production-audit-evidence.zip) and its
[SHA-256 file](2026-10-28-production-audit-evidence.zip.sha256) contain the raw
proofs and provenance. The archive checksum is published in that companion file;
it is distinct from the measured executable checksum.

## Measured artifact and accepted campaigns

| Item | Recorded value |
| --- | --- |
| MultiDB engine source commit | `12efdaa5d16c36a922c209236f7e02681197931d` |
| Release executable SHA-256 | `8e29f953a517d3957100a4adb56f0b5bede9b9948d9cb3b06da7372cd6e48db2` |
| Build | Locked release binary, default secure features; `insecure-transport` excluded |
| Server configuration | Native Transactional profile; an additional Analytical server for analytics; authenticated loopback HTTP and pgwire, local-dev transport settings |
| Repetitions | Three per normal workload; SQL initial-load batches have one load trial per dataset/engine |
| Base sizes | SQL, analytics, KV and tensors: 10,000 and 100,000; vector and graph: 1,000 and 10,000 |

The domains ran as separate, fresh campaigns to control disk use and isolate
fixtures. They are combined because every campaign measured the same executable,
not because they ran in one uninterrupted process. Each run records its own
runner-file hashes, commands, dependency versions and configuration. In
particular, the vector campaign records the later explicit `ef_search=1024`
quality setting. Documentation/reporting commits after the engine commit do not
change the identity of the executable measured here.

| Accepted campaign directory in the evidence set | Sections | Raw timed requests | Aggregated cells |
| --- | ---: | ---: | ---: |
| `multidb-audit-qualified2-sql` | 168 | 117,780 | 60 |
| `multidb-audit-qualified2-analytics` | 90 | 8,370 | 30 |
| `multidb-audit-qualified2-kv` | 36 | 5,160 | 12 |
| `multidb-audit-qualified2-graph` | 36 | 7,200 | 12 |
| `multidb-audit-qualified2-tensor` | 36 | 15,120 | 12 |
| `multidb-audit-vector-quality` | 12 | 2,400 | 4 |
| **Total** | **378** | **156,030** | **130** |

Raw CSV consistency, expected workload coverage and completion are checked
independently of aggregate report generation. The adapters separately validate
the returned answers as described below. Zero timed errors does not mean every
untimed capability probe succeeded: unsupported filtering and extreme-number
differences are explicitly recorded in the vector section.

## Versions, deployment and fairness limits

| Engine | Measured version | Deployment/API and relevant settings |
| --- | --- | --- |
| MultiDB | 0.1.0, source commit above | Native process; pgwire simple-query SQL for the original SQL suite, authenticated HTTP/JSON for other domains; durable redb Immediate writes and default durable audit |
| PostgreSQL | 18.1, Debian build `18.1-1.pgdg13+2` | Local Docker service, psycopg simple-query protocol; normal durable PostgreSQL defaults; primary/category indexes in the SQL suite |
| MongoDB | 8.0.17 | Local Docker service, PyMongo/BSON Int64, `w=1,j=true`, ordered bulk writes, WiredTiger cache 1 GiB; primary/category indexes |
| DuckDB | 1.4.3 | Embedded Python API on a durable local file, two DuckDB threads; schema/load excluded from query timing |
| Redis | 8.2.3; redis-py 6.4.0 | Authenticated RESP; AOF enabled, `appendfsync=always`, periodic RDB saves disabled, client retries disabled |
| Qdrant | 1.16.2 | Authenticated HTTP/JSON; two search threads; HNSW readiness verified, `m=16`, construction `ef=128`, search `ef=1024` |
| Neo4j | 5.26.18 | Authenticated transactional HTTP/Cypher; initial heap 256 MiB, maximum heap 512 MiB, page cache 512 MiB |
| Deep Lake | 4.7.4 | Embedded local Python API with committed datasets/revisions; CPU reference operations; its commit API is not asserted equivalent to redb Immediate fsync |

The Docker images are immutable digest pins, not claims to be the latest vendor
release. PostgreSQL and MongoDB reproduce the supplied reference versions.

| Image | Pinned SHA-256 |
| --- | --- |
| `postgres` | `1090bc3a8ccfb0b55f78a494d76f8d603434f7e4553543d6e807bc7bd6bbd17f` |
| `mongo` | `9814652e33f0cf8b9fddea8b46dfc9d8e19b130dcfdd7b510ca58bb0d40c8b71` |
| `redis` | `08ad0b1d280850169a790dba1393ff7a90aef951fc19632cf4d3ce4f78e679ba` |
| `qdrant/qdrant` | `dab6de32f7b2cc599985a7c764db3e8b062f70508fb85ca074aa856f829bf335` |
| `neo4j` | `ef6f1cb1a5594cbfddb82876a9dc67171941eabca3a38100f2bef0b3fc6c05e9` |

The host is Linux x86-64 with a four-CPU cgroup quota and a 16 GiB memory limit;
the allowed CPU set was 0–4. Native servers and container services were pinned
to CPUs 0–1, clients and embedded engines to CPUs 2–3. Each competitor container
had a two-CPU quota and 4 GiB memory limit. MultiDB had server affinity and the
host/cgroup memory limit, without a matching 4 GiB per-process cap. Transactional
and Analytical profile resource defaults also differ. These limits are recorded
and must not be described as identical. CPU governor information was unavailable.

Python was 3.12.14 and NumPy 2.2.6. The environment had no physical GPU. Docker
storage and the host filesystem are part of these local deployment costs; this
is not a hardware/storage certification. Resource sampling records process
diagnostics and container aggregate cgroup CPU/charged memory where available,
with missing scopes explicit. Native RSS, JVM/container charged memory and
embedded runner memory do not support a fair cross-engine RAM ranking.

Transport, parsing, serialization, auditing and durability differ across APIs.
HTTP versus an embedded API especially includes a different service boundary.
The SQL comparators use the same psycopg simple-query mode, but MongoDB uses its
native document protocol. MongoDB ordered bulk writes and Redis `MULTI/EXEC`
have different error/atomicity contracts from SQL transactions; Redis does not
roll back already executed commands after a runtime command error. All measured
errors invalidate their section. No durability option was disabled to win a
timing, but the different durability APIs are not declared equivalent.

## Interpreting samples and timings

The table reports the median of each metric across the recorded repeats;
percentiles are not percentiles of one pooled cross-repeat request stream. The
last column shows the minimum and maximum per-repeat p99. Raw samples and JSON
also retain repeat ranges for throughput and the other percentiles.

Operation latency stops before answer validation. Section requests/s uses wall
time including validation and recording, so it is not an isolated server
throughput ceiling. A batch request counts as one request, not 64 or 100
individual records. The `Rows/vectors` column is the base fixture size, not a
promise that mutation workloads never grow it.

First operations may be cold unless a domain explicitly warms them; these
short, repeated fixtures largely exercise warm, fit-in-RAM behavior. Repeats
can reuse the same index or mutate a fixture. They are not independent fresh
database trials. The small aggregate, scan and batch sections have only 30 or
5 operations per repeat; their p99 values are descriptive observations, not
well-supported production tail estimates. SQL load cells have one repetition.
No confidence interval, larger-than-memory result or sustained-load SLO is
claimed.

## SQL: PostgreSQL and MongoDB

The original reviewed harness uses equal four-field records with a 256-byte
payload and primary/category indexes. It checks durable load, single and
100-record batch inserts, point/category/100-row range reads, grouped count/sum,
upserts and 80/20 mixed traffic at concurrency 1 and 8. Full answer checks and
independent raw CSV verification passed. PostgreSQL and MongoDB plans are
retained; the SQL load is measured separately from the repeated operations.

At 100k base rows, MultiDB's grouped aggregate had p50 **3.0428 ms**, compared
with PostgreSQL **15.8472 ms** and MongoDB **33.7008 ms**. That is a useful result
for this exact integer group-count/sum workload, not a conclusion about general
joins, arbitrary SQL analytics or unseen datasets. At 10k, PostgreSQL's aggregate
was faster: 1.6217 ms versus MultiDB's 2.2597 ms.

PostgreSQL and MongoDB were faster for most reads and writes in this suite.
At 100k, point reads were 0.1082/0.1850/1.1384 ms for PostgreSQL/MongoDB/MultiDB;
100-record inserts were 1.0787/1.1417/9.6787 ms. Concurrency-8 mixed throughput
was 15,001/6,271/2,120 requests/s respectively. MultiDB therefore does not win
the general transactional comparison, even though the aggregate result is
strong. Its authorization, bounded-resource checks and durable audit remain
part of its observed cost.

### Before/audit comparison on the supplied SQL workload

The baseline executable is equivalent to PR #38's merged tree and has SHA-256
`3b881025915d07dc147aef7086f1dd56b50d06dd01a2824c01f5cefe1bc3eb59`.
Its independently verified SQL run also contains 117,780 samples with zero
setup/correctness failures and a clean exit. The following compares that run
with the accepted `qualified2-sql` campaign, never with a failed or earlier
calibration attempt. Percent changes are observations from sequential campaigns,
not causal attribution of every difference to one code change.

The audit preserves most central latencies near the baseline and improves batch
insert latency in this run. It also has regressions: 100k category-read throughput
fell **6.4%**, from 216.33 to 202.39 requests/s, while its p50 rose **8.6%**, from
4.2424 to 4.6065 ms. Those are different metrics and both remain visible. The
correctness/security fixes are not described as a universal performance gain.

| Base rows | Workload | Before p50 ms | Audit p50 ms | p50 change | Before requests/s | Audit requests/s | Throughput change |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | aggregate | 2.2369 | 2.2597 | +1.0% | 444.56 | 437.07 | -1.7% |
| 100,000 | aggregate | 3.0283 | 3.0428 | +0.5% | 327.17 | 318.34 | -2.7% |
| 10,000 | category_read | 1.4742 | 1.4934 | +1.3% | 651.95 | 611.03 | -6.3% |
| 100,000 | category_read | 4.2424 | 4.6065 | +8.6% | 216.33 | 202.39 | -6.4% |
| 10,000 | insert_batch | 10.9521 | 10.0968 | -7.8% | 91.23 | 98.93 | +8.4% |
| 100,000 | insert_batch | 10.2409 | 9.6787 | -5.5% | 94.71 | 102.24 | +8.0% |
| 10,000 | insert_single | 1.7298 | 1.7667 | +2.1% | 560.89 | 556.21 | -0.8% |
| 100,000 | insert_single | 1.9599 | 1.9855 | +1.3% | 503.14 | 489.30 | -2.7% |
| 10,000 | load_batch | 9.4296 | 9.6711 | +2.6% | 106.67 | 105.43 | -1.2% |
| 100,000 | load_batch | 12.4827 | 12.4099 | -0.6% | 80.46 | 79.95 | -0.6% |
| 10,000 | mixed_c1 | 1.0638 | 1.0339 | -2.8% | 853.16 | 886.14 | +3.9% |
| 100,000 | mixed_c1 | 1.1198 | 1.1722 | +4.7% | 782.96 | 780.18 | -0.4% |
| 10,000 | mixed_c8 | 2.8713 | 2.9826 | +3.9% | 2256.59 | 2293.76 | +1.6% |
| 100,000 | mixed_c8 | 3.0776 | 3.1561 | +2.6% | 2161.75 | 2120.04 | -1.9% |
| 10,000 | point_read | 0.9839 | 1.0133 | +3.0% | 987.66 | 923.98 | -6.4% |
| 100,000 | point_read | 1.1431 | 1.1384 | -0.4% | 855.98 | 841.92 | -1.6% |
| 10,000 | range_read | 1.2681 | 1.2366 | -2.5% | 733.38 | 756.75 | +3.2% |
| 100,000 | range_read | 1.3682 | 1.3537 | -1.1% | 695.99 | 694.32 | -0.2% |
| 10,000 | upsert | 1.4297 | 1.4233 | -0.4% | 675.93 | 686.57 | +1.6% |
| 100,000 | upsert | 1.6495 | 1.6204 | -1.8% | 577.90 | 585.01 | +1.2% |

## Analytics: DuckDB and the Analytical profile

The analytical adapter checks exact integer group count/sum, filtered count,
ordered range100, projected full scans and full-payload scans at 10k/100k rows.
It compares DuckDB in process with both MultiDB Transactional row layout and
Analytical columnar layout through HTTP. Schema creation and load are outside
query timing; complete ordered result validation is outside each latency timer.

At 100k, DuckDB grouped at 1.0017 ms p50; MultiDB Transactional took 3.4147 ms
and Analytical 12.2127 ms. The same order held for filtered counts
(1.4638/2.2292/12.4362 ms) and range100 (1.0018/1.4602/13.3692 ms). A profile
called Analytical does not guarantee superiority on every fixture or API path.
Projected scans were closer: Transactional 55.3581 ms, DuckDB 59.4191 ms and
Analytical 63.0347 ms, with overlapping repeat variability and large client
materialization costs. Full-payload scans favored DuckDB at 127.0700 ms versus
Analytical 198.9810 ms and Transactional 213.9019 ms.

These are full answer delivery costs through different APIs. The very small
five-operation-per-repeat full-payload scan sample cannot establish a stable
p99. More profiles, data distributions, compressed/uncompressed storage,
concurrent analytics and larger-than-RAM scans are required before selecting a
production analytical topology.

## KV: Redis

The KV adapter stores equal full logical records with a 256-byte payload and
only a primary key on MultiDB. It compares point GET, full-record upsert and
atomic 100-record insert batches. Redis uses authenticated RESP and
`appendfsync=always`; MultiDB uses authenticated HTTP SQL/JSON with redb Immediate
and durable audit. Record checks occur after the latency timer. Fixture load is
untimed.

At 100k, Redis point GET p50 was **0.0596 ms** versus MultiDB **1.0055 ms**;
full-record upsert was 0.3294 versus 1.5841 ms, and 100-record batch insert
1.1878 versus 5.4698 ms. Redis leads these workloads in this deployment.
The comparison includes the SQL parser/HTTP/JSON boundary on MultiDB and RESP
on Redis, so these ratios cannot isolate storage-engine efficiency.

The campaign also performed a real untimed graceful restart of MultiDB and
Redis. The native PID and Redis run ID changed. Post-restart counts and sampled
full source records matched for both 19,000- and 109,000-record mutated fixtures;
`kv-restart.json` and `kv-persistence.json` record the successful proof. This is
a restart check, not a Redis crash/power-loss comparison.

## Vector search: Qdrant with a fixed recall floor

This is warm approximate HNSW top10 search over deterministic 128-dimensional
L2 vectors at 1k/10k, seed 20261028. Both engines use `m=16`, construction
`ef=128` and **search `ef=1024`**. Qdrant's indexed-vector readiness was checked
before timing, with its full-scan threshold set to 10 KiB. Returned distances
are checked against a NumPy float64 exact reference. Both engines must meet the
unchanged floor of at least 0.8 recall on every query and 0.95 mean recall.

Each size has **200 distinct query vectors repeated three times against the
same loaded index**. The 600 timing samples are not 600 independent quality
queries or three independent index builds. Equal numeric HNSW settings also
do not imply identical implementations or traversal behavior.

| Size | Engine | Mean recall@10 | Minimum query recall@10 | p50 ms | p99 ms |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1,000 | MultiDB | 0.9950 | 0.9 | 1.8742 | 2.3359 |
| 1,000 | Qdrant | 1.0000 | 1.0 | 0.6710 | 0.8044 |
| 10,000 | MultiDB | 0.9805 | 0.8 | 5.6471 | 7.6311 |
| 10,000 | Qdrant | 0.9980 | 0.9 | 1.1370 | 1.4857 |

Both meet the declared floor at the qualified setting, while Qdrant has better
recall and latency here. The table does not imply that the default lower search
effort reaches this quality at every collection size. A production recall SLO
requires a representative embedding distribution, multiple independent builds
and its own effort/latency calibration.

Untimed probes record capability differences separately. MultiDB explicitly
rejects the unsupported HTTP filter with status 400; Qdrant's filter probe
returned recall 1.0. MultiDB passed the tested large/small cosine and
representable-large-L2 references and rejected an unrepresentable L2 result
with 400. The pinned Qdrant deployment differed from the exact oracle on the
specific extreme cosine and large-L2 probes. These bounded numerical probes
are recorded in `vector-graph-correctness.json`; they are not timed ANN errors
or a general claim about all Qdrant numerical behavior. This suite also does
not benchmark full GraphRAG retrieval, citation construction or LLM generation.

## Graph traversal and updates: Neo4j

The deterministic directed graph has 1k/10k nodes and four outgoing edges per
node, at modular offsets 1, 7, 31 and 127. The adapter checks the complete exact
reachable-node set at depths 1 and 2, excluding the source and rejecting
duplicates. MultiDB uses its SQL graph helper over HTTP; Neo4j uses
transactional Cypher over HTTP. Fixture creation is outside timing, and engine
order alternates between repeats.

At 10k nodes, MultiDB depth1/depth2 p50 was **1.0167/1.0796 ms**, versus Neo4j
**1.3479/1.1869 ms**; p99 was 1.6097/1.2927 versus 6.3392/3.8530 ms. MultiDB
also led the measured 1k traversals. These shallow fixed-degree queries do not
cover high-degree graphs, deep/path-pattern queries or Neo4j's wider Cypher
surface.

Neo4j led the durable existing-edge update timings at 10k: **1.6366 ms** p50
versus MultiDB **3.7103 ms**. Write validation is narrower than traversal
validation: it checks successful acknowledgement, and Neo4j additionally checks
exactly one affected relationship. The MultiDB public HTTP API does not expose
edge-property readback, so the adapter does **not** independently read and verify
stored `write_seq` values. These cells must not be presented as full end-to-end
property-persistence proofs.

## Tensor datasets: Deep Lake, CPU reference only

This adapter compares committed batches of named text, U8 RGB images of shape
8×8×3 and F32 embeddings of dimension 16. Appends use 64 samples per request
(with a partial final batch); projected reads fetch 32 samples of text and
embedding; exact CPU L2 top10 uses a NumPy oracle. Source values, dtypes/shapes,
immutable revision reads and isolation from a subsequent append are checked.

The fixture is deliberately simple: binary-exact linearly spaced 16D vectors,
constant-color deterministic RGB images and indexed text. Search reuses only
**eight unique query vectors**, cycling them through each 200-operation repeat.
It does not represent a diverse image/audio/video training corpus or a broad
semantic embedding distribution. MultiDB runs explicit CPU reference mode on
the Transactional server; the strict GPU profile is not being bypassed. Deep
Lake runs in process, while MultiDB uses authenticated HTTP/JSON. Deep Lake's
commit API and redb Immediate plus audit do not have an established equivalence.

At 100k samples, Deep Lake exact L2 search p50 was **55.7262 ms** versus MultiDB
**277.5441 ms**; projected read p50 was **0.2802** versus **0.6216 ms**. Deep Lake
also had lower median append latency: 3.4493 versus 4.6383 ms, although its
observed append p99 was higher: 56.1777 versus 7.6148 ms. That tail difference
is reported for this committed local workload and does not neutralize the
durability/API differences or establish a production tail guarantee.

No GPU throughput result is present. The separate software Vulkan kernel test
does not measure a physical adapter, Docker device passthrough or device-loss
recovery on supported hardware. These tensor APIs also do not promise Deep
Lake wire or SDK compatibility.

## Matched CPU tensor optimization diagnostic

A separate before/after experiment used the same 1,000-sample tensor fixture,
driver, CPU reference mode, 200 operations and three timing repetitions. The
before executable was source `515307a37e8cc353ea1ac9c2817ab80f8969e0ae`, SHA-256
`4b19e8e872d54700f59cdb74cfb572c2e5f110433910fa8776e4c8705305cb8b`;
the after executable was the qualified `12efdaa` artifact above. Both complete
campaigns passed independent verification, with 2,496 timed requests and zero
errors each. They are diagnostic evidence, excluded from the primary 156,030
requests and 130 result cells.

| MultiDB exact CPU L2 top10, 1,000 samples | Before | After |
| --- | ---: | ---: |
| Timed search requests | 600 | 600 |
| Median p50 (ms) | 21.4615 | 2.8554 |
| Median p95 (ms) | 23.8143 | 3.2692 |
| Median p99 (ms) | 26.8205 | 3.5624 |
| Median requests/s | 45.88 | 342.47 |

The observed p50 reduction is **7.52×**, with throughput increasing about
7.46×. The changed runtime caches the physical table catalog within each write
transaction and streams validated tensor chunks while borrowing their numeric
values; CPU search no longer repeatedly decodes/clones the same chunk per
sample. Regression tests check chunk-decode counts, revision/index validation
and near-limit memory behavior. Both runtime changes landed together, so this
measurement does not attribute every timing delta to one change. It does not
extrapolate the ratio to 100k samples, different vector distributions or GPUs.
Both paired campaigns and their unrounded comparison are retained in the ZIP.

## Resource diagnostics and coverage

The [resource summary](2026-10-28-production-audit.resources.json) summarizes
the complete campaign stages, including fixture construction and verification.
RSS is sampled every 0.5 seconds, rather than an isolated timed-query peak;
process high-water marks are recorded separately in the JSON. Minimum free
space covers the output filesystem during collection.

| Campaign | MultiDB peak sampled RSS (MiB) | Minimum free space (GiB) |
| --- | ---: | ---: |
| SQL | 628.6 | 4.72 |
| Analytics, Transactional / Analytical | 573.9 / 563.2 | 6.73 |
| KV | 289.4 | 6.07 |
| Graph | 171.2 | 4.35 |
| Tensor | 433.3 | 4.76 |
| Vector | 148.8 | 6.98 |

Container aggregate CPU/charged-memory counters were unavailable in this
environment: the collector rejected the namespaced cgroup path containing
`..`. The raw trace records that failure explicitly. A single container parent
PID is not substituted for its descendants. DuckDB/Deep Lake process samples
also include the Python driver and reference checks, and process restarts can
reset CPU counters. These heterogeneous and incomplete scopes cannot establish
a cross-engine RAM ranking or an exact campaign CPU total. Accepted campaigns
had at least 4.35 GiB of recorded free space; earlier disk-full attempts remain
excluded rather than being reused as results.

## Invalid attempts, calibration and corrections

The evidence keeps excluded attempts separate from the accepted six campaigns.
Early runs encountered local disk-capacity pressure; those interrupted attempts
cannot qualify for a comparative result. Harness/schema and HTTP keepalive
integration issues were corrected and rerun. A failed or partially completed
campaign is not converted into a successful report by dropping its failed cells.

The initial vector quality calibration at search `ef=256` missed the declared
recall floor. The qualified rerun increased search effort to 1024 **for both
engines**, retaining the same minimum and mean quality floors and recording the
new setting. Its timings are a different calibrated configuration, not evidence
that the original lower-effort setting passed or that changing effort was an
engine optimization. Failed/calibration timings are excluded from final ranking
and from the 156,030-request total.

The source audit corrected confirmed implementation issues and added their
regressions; [the audit report](../production-audit-2026-10-28.md) describes the
resulting behavior. Remaining slower cells are visible above and below. This
record does not reinterpret every performance gap as a correctness defect or
claim that every workload has reached its best possible implementation.

## Crash/recovery and release validation

The current measured executable also passed a separate **20-cycle process-kill
and full-backup/restore drill**. It verified survival of 1,000 acknowledged
single-row writes, checked 20 in-flight 100-row atomic batches after restart,
and confirmed all-or-none visibility for those batches. All 20 in-flight batch
responses were unacknowledged in this execution; the drill does not classify
their final commit outcome from a missing response. A full backup was verified,
restored into a fresh destination and compared with the complete source logical
state. `result.json` records the same executable SHA-256 and `passed=true`.

This establishes the exercised process-death/reopen path, not physical power
loss, disk/controller failure, short writes, disk-full recovery, incremental
PITR, cross-version upgrades or a measured RPO/RTO contract.

The source CI run [40757227491](https://github.com/piotrwozn/multidb/actions/runs/40757227491)
completed all four jobs successfully (check, MSRV, supply-chain and fuzz),
including actual fuzz execution of nine
targets with 256 runs each. Local release evidence and CI are complementary;
this source CI result is not a claim that a version 1.0.0 tag or signed
production release has been published. Sustained hardware-specific SLO tests,
independent security review and the remaining operations/feature qualification
gates remain in [production readiness](../production-readiness.md).

## Complete qualified result table

The following 130 cells are generated directly from the accepted combined
raw-proof summary. Requests/s is per API request; values are rounded here, with
unrounded data in the companion JSON/CSV. All cells have zero timed errors.

| Domain | Workload | Rows/vectors | Engine | Repeats | Samples | Requests/s | p50 ms | p95 ms | p99 ms | p99 repeat range ms |
|---|---|---:|---|---:|---:|---:|---:|---:|---:|---|
| analytics | analytics.count_filter.n10000 | 10000 | duckdb_inprocess | 3 | 600 | 2272.47 | 0.4153 | 0.4881 | 0.7027 | 0.5788–0.7669 |
| analytics | analytics.count_filter.n10000 | 10000 | multidb_analytical_http | 3 | 600 | 395.41 | 2.4651 | 2.9511 | 3.2625 | 3.1704–3.4319 |
| analytics | analytics.count_filter.n10000 | 10000 | multidb_http | 3 | 600 | 511.53 | 1.8995 | 2.3238 | 2.6361 | 2.6238–3.0052 |
| analytics | analytics.count_filter.n100000 | 100000 | duckdb_inprocess | 3 | 600 | 641.78 | 1.4638 | 1.7638 | 2.1946 | 1.9621–2.5118 |
| analytics | analytics.count_filter.n100000 | 100000 | multidb_analytical_http | 3 | 600 | 82.52 | 12.4362 | 13.8036 | 15.1722 | 14.6965–15.6530 |
| analytics | analytics.count_filter.n100000 | 100000 | multidb_http | 3 | 600 | 411.45 | 2.2292 | 2.5618 | 3.3942 | 3.1461–3.9746 |
| analytics | analytics.full_payload_scan.n10000 | 10000 | duckdb_inprocess | 3 | 15 | 78.61 | 7.5127 | 14.0257 | 15.2710 | 7.8803–17.9008 |
| analytics | analytics.full_payload_scan.n10000 | 10000 | multidb_analytical_http | 3 | 15 | 41.53 | 20.4529 | 20.8329 | 20.8832 | 20.6109–23.7858 |
| analytics | analytics.full_payload_scan.n10000 | 10000 | multidb_http | 3 | 15 | 42.80 | 19.3927 | 20.4043 | 20.4204 | 20.1905–29.2438 |
| analytics | analytics.full_payload_scan.n100000 | 100000 | duckdb_inprocess | 3 | 15 | 5.62 | 127.0700 | 144.1675 | 145.2542 | 141.4949–159.7748 |
| analytics | analytics.full_payload_scan.n100000 | 100000 | multidb_analytical_http | 3 | 15 | 4.08 | 198.9810 | 206.8727 | 207.7960 | 207.1629–220.6537 |
| analytics | analytics.full_payload_scan.n100000 | 100000 | multidb_http | 3 | 15 | 3.74 | 213.9019 | 243.2997 | 244.2383 | 233.1204–356.1510 |
| analytics | analytics.group_count_sum.n10000 | 10000 | duckdb_inprocess | 3 | 90 | 1452.80 | 0.5951 | 0.8411 | 1.3341 | 1.2000–1.4297 |
| analytics | analytics.group_count_sum.n10000 | 10000 | multidb_analytical_http | 3 | 90 | 332.48 | 2.9038 | 3.4406 | 3.8714 | 3.8103–4.0873 |
| analytics | analytics.group_count_sum.n10000 | 10000 | multidb_http | 3 | 90 | 393.24 | 2.3523 | 2.8409 | 3.9938 | 3.6142–8.4261 |
| analytics | analytics.group_count_sum.n100000 | 100000 | duckdb_inprocess | 3 | 90 | 915.25 | 1.0017 | 1.1905 | 1.9768 | 1.7660–2.1140 |
| analytics | analytics.group_count_sum.n100000 | 100000 | multidb_analytical_http | 3 | 90 | 77.93 | 12.2127 | 15.3258 | 15.9866 | 13.7957–16.3759 |
| analytics | analytics.group_count_sum.n100000 | 100000 | multidb_http | 3 | 90 | 273.43 | 3.4147 | 4.3566 | 5.7301 | 4.2559–53.0855 |
| analytics | analytics.projection_full_scan.n10000 | 10000 | duckdb_inprocess | 3 | 90 | 138.85 | 3.5485 | 11.1874 | 11.2582 | 10.8019–12.0902 |
| analytics | analytics.projection_full_scan.n10000 | 10000 | multidb_analytical_http | 3 | 90 | 100.96 | 7.3523 | 8.2146 | 8.4269 | 8.1530–9.3778 |
| analytics | analytics.projection_full_scan.n10000 | 10000 | multidb_http | 3 | 90 | 107.24 | 6.7550 | 7.3415 | 7.8739 | 7.7764–17.8512 |
| analytics | analytics.projection_full_scan.n100000 | 100000 | duckdb_inprocess | 3 | 90 | 11.69 | 59.4191 | 74.3345 | 79.4634 | 79.2873–79.5997 |
| analytics | analytics.projection_full_scan.n100000 | 100000 | multidb_analytical_http | 3 | 90 | 11.18 | 63.0347 | 67.4718 | 68.2849 | 68.0343–82.7746 |
| analytics | analytics.projection_full_scan.n100000 | 100000 | multidb_http | 3 | 90 | 11.94 | 55.3581 | 67.7143 | 76.3302 | 74.4434–108.3291 |
| analytics | analytics.range100.n10000 | 10000 | duckdb_inprocess | 3 | 600 | 1266.46 | 0.7569 | 0.8479 | 1.0131 | 1.0029–1.1076 |
| analytics | analytics.range100.n10000 | 10000 | multidb_analytical_http | 3 | 600 | 328.05 | 2.9392 | 3.5022 | 4.5934 | 4.4634–5.3349 |
| analytics | analytics.range100.n10000 | 10000 | multidb_http | 3 | 600 | 733.71 | 1.2781 | 1.5127 | 2.0309 | 1.6299–2.2840 |
| analytics | analytics.range100.n100000 | 100000 | duckdb_inprocess | 3 | 600 | 907.50 | 1.0018 | 1.3482 | 2.0882 | 1.5393–2.1861 |
| analytics | analytics.range100.n100000 | 100000 | multidb_analytical_http | 3 | 600 | 72.39 | 13.3692 | 15.7263 | 17.7215 | 15.9543–17.8028 |
| analytics | analytics.range100.n100000 | 100000 | multidb_http | 3 | 600 | 636.95 | 1.4602 | 1.7736 | 2.3759 | 2.3724–2.5411 |
| graph | graph.durable_edge_update.n1000 | 1000 | multidb_http | 3 | 600 | 279.34 | 3.4864 | 4.0882 | 4.7476 | 3.9477–4.8840 |
| graph | graph.durable_edge_update.n1000 | 1000 | neo4j_http | 3 | 600 | 362.07 | 2.6945 | 3.3789 | 4.6908 | 3.8396–12.8219 |
| graph | graph.durable_edge_update.n10000 | 10000 | multidb_http | 3 | 600 | 263.92 | 3.7103 | 4.1546 | 5.4797 | 4.2905–7.9158 |
| graph | graph.durable_edge_update.n10000 | 10000 | neo4j_http | 3 | 600 | 496.66 | 1.6366 | 3.6421 | 4.2151 | 3.9478–6.7575 |
| graph | graph.neighbors_depth1.n1000 | 1000 | multidb_http | 3 | 600 | 928.98 | 0.9953 | 1.3791 | 3.2945 | 2.5239–4.3736 |
| graph | graph.neighbors_depth1.n1000 | 1000 | neo4j_http | 3 | 600 | 243.55 | 3.7067 | 6.2500 | 9.6667 | 6.8311–10.8415 |
| graph | graph.neighbors_depth1.n10000 | 10000 | multidb_http | 3 | 600 | 966.15 | 1.0167 | 1.1715 | 1.6097 | 1.2025–1.8599 |
| graph | graph.neighbors_depth1.n10000 | 10000 | neo4j_http | 3 | 600 | 544.17 | 1.3479 | 3.5004 | 6.3392 | 4.8066–7.7086 |
| graph | graph.neighbors_depth2.n1000 | 1000 | multidb_http | 3 | 600 | 814.26 | 1.1065 | 2.0996 | 4.9138 | 2.7402–4.9289 |
| graph | graph.neighbors_depth2.n1000 | 1000 | neo4j_http | 3 | 600 | 451.47 | 2.0496 | 2.9180 | 3.2753 | 3.2160–3.9110 |
| graph | graph.neighbors_depth2.n10000 | 10000 | multidb_http | 3 | 600 | 923.01 | 1.0796 | 1.1961 | 1.2927 | 1.2855–1.3718 |
| graph | graph.neighbors_depth2.n10000 | 10000 | neo4j_http | 3 | 600 | 671.09 | 1.1869 | 2.7637 | 3.8530 | 3.3849–6.6867 |
| kv | kv.atomic_insert_batch100.n10000 | 10000 | multidb_http | 3 | 90 | 140.22 | 5.4280 | 6.6619 | 7.8186 | 6.1861–38.9181 |
| kv | kv.atomic_insert_batch100.n10000 | 10000 | redis_resp | 3 | 90 | 547.06 | 1.1575 | 1.4340 | 2.1999 | 1.4999–4.2365 |
| kv | kv.atomic_insert_batch100.n100000 | 100000 | multidb_http | 3 | 90 | 141.13 | 5.4698 | 6.3492 | 6.5505 | 6.4600–8.0363 |
| kv | kv.atomic_insert_batch100.n100000 | 100000 | redis_resp | 3 | 90 | 555.10 | 1.1878 | 1.3916 | 1.5638 | 1.5294–1.7296 |
| kv | kv.full_record_upsert.n10000 | 10000 | multidb_http | 3 | 600 | 383.08 | 1.4592 | 1.8965 | 2.7295 | 2.0538–2.9498 |
| kv | kv.full_record_upsert.n10000 | 10000 | redis_resp | 3 | 600 | 1972.34 | 0.3526 | 0.5188 | 0.6500 | 0.6290–1.8372 |
| kv | kv.full_record_upsert.n100000 | 100000 | multidb_http | 3 | 600 | 361.43 | 1.5841 | 2.4050 | 2.7621 | 2.4901–3.6477 |
| kv | kv.full_record_upsert.n100000 | 100000 | redis_resp | 3 | 600 | 2133.60 | 0.3294 | 0.4655 | 0.6699 | 0.6000–1.0657 |
| kv | kv.point_get.n10000 | 10000 | multidb_http | 3 | 600 | 931.42 | 1.0080 | 1.3494 | 2.0511 | 1.6756–2.3401 |
| kv | kv.point_get.n10000 | 10000 | redis_resp | 3 | 600 | 12917.55 | 0.0657 | 0.0962 | 0.1485 | 0.1173–0.1548 |
| kv | kv.point_get.n100000 | 100000 | multidb_http | 3 | 600 | 969.98 | 1.0055 | 1.2357 | 1.4379 | 1.4345–2.1105 |
| kv | kv.point_get.n100000 | 100000 | redis_resp | 3 | 600 | 13683.44 | 0.0596 | 0.1068 | 0.1951 | 0.1630–0.8938 |
| sql | aggregate | 10000 | mongodb | 3 | 90 | 298.47 | 3.2754 | 3.4895 | 4.1836 | 3.3751–4.9320 |
| sql | aggregate | 10000 | multidb | 3 | 90 | 437.07 | 2.2597 | 2.5154 | 2.6743 | 2.4585–2.7884 |
| sql | aggregate | 10000 | postgres | 3 | 90 | 611.29 | 1.6217 | 1.6948 | 1.7132 | 1.6859–1.7440 |
| sql | aggregate | 100000 | mongodb | 3 | 90 | 29.40 | 33.7008 | 36.0629 | 36.5962 | 35.4862–37.7350 |
| sql | aggregate | 100000 | multidb | 3 | 90 | 318.34 | 3.0428 | 3.8727 | 5.3120 | 3.2734–5.6089 |
| sql | aggregate | 100000 | postgres | 3 | 90 | 61.98 | 15.8472 | 18.8540 | 19.8229 | 15.6596–19.8552 |
| sql | category_read | 10000 | mongodb | 3 | 300 | 1633.76 | 0.5612 | 0.7007 | 0.7984 | 0.6510–1.0036 |
| sql | category_read | 10000 | multidb | 3 | 300 | 611.03 | 1.4934 | 1.9415 | 2.0924 | 1.8118–2.7011 |
| sql | category_read | 10000 | postgres | 3 | 300 | 4023.40 | 0.2235 | 0.2691 | 0.2953 | 0.2630–0.9756 |
| sql | category_read | 100000 | mongodb | 3 | 300 | 275.10 | 3.0929 | 3.6095 | 5.7100 | 4.6491–6.7559 |
| sql | category_read | 100000 | multidb | 3 | 300 | 202.39 | 4.6065 | 5.1401 | 5.9580 | 5.7308–6.0328 |
| sql | category_read | 100000 | postgres | 3 | 300 | 591.30 | 1.3233 | 1.9556 | 2.1219 | 2.0313–2.3493 |
| sql | insert_batch | 10000 | mongodb | 3 | 90 | 866.76 | 1.1076 | 1.2518 | 1.3328 | 1.2626–2.6326 |
| sql | insert_batch | 10000 | multidb | 3 | 90 | 98.93 | 10.0968 | 11.6952 | 12.2168 | 11.9688–12.4018 |
| sql | insert_batch | 10000 | postgres | 3 | 90 | 958.19 | 1.0111 | 1.1384 | 1.1777 | 1.1116–1.2192 |
| sql | insert_batch | 100000 | mongodb | 3 | 90 | 838.59 | 1.1417 | 1.3015 | 1.3467 | 1.2642–3.4386 |
| sql | insert_batch | 100000 | multidb | 3 | 90 | 102.24 | 9.6787 | 11.5921 | 11.8808 | 11.7485–12.3726 |
| sql | insert_batch | 100000 | postgres | 3 | 90 | 900.02 | 1.0787 | 1.2232 | 1.2702 | 1.2233–2.0890 |
| sql | insert_single | 10000 | mongodb | 3 | 900 | 1963.06 | 0.4764 | 0.6578 | 0.9831 | 0.7511–1.3718 |
| sql | insert_single | 10000 | multidb | 3 | 900 | 556.21 | 1.7667 | 2.1471 | 2.4799 | 2.3941–2.8804 |
| sql | insert_single | 10000 | postgres | 3 | 900 | 2767.70 | 0.3459 | 0.5134 | 0.6560 | 0.5658–0.7275 |
| sql | insert_single | 100000 | mongodb | 3 | 900 | 2147.64 | 0.4483 | 0.6219 | 0.7122 | 0.6630–0.9336 |
| sql | insert_single | 100000 | multidb | 3 | 900 | 489.30 | 1.9855 | 2.5078 | 2.9637 | 2.8169–5.2988 |
| sql | insert_single | 100000 | postgres | 3 | 900 | 2956.95 | 0.3196 | 0.4631 | 0.6047 | 0.4781–0.9864 |
| sql | load_batch | 10000 | mongodb | 1 | 100 | 823.08 | 1.1535 | 1.3276 | 1.5158 | 1.5158–1.5158 |
| sql | load_batch | 10000 | multidb | 1 | 100 | 105.43 | 9.6711 | 11.9472 | 12.4722 | 12.4722–12.4722 |
| sql | load_batch | 10000 | postgres | 1 | 100 | 807.42 | 1.1139 | 1.5027 | 1.8836 | 1.8836–1.8836 |
| sql | load_batch | 100000 | mongodb | 1 | 1000 | 819.37 | 1.0917 | 1.3684 | 2.1339 | 2.1339–2.1339 |
| sql | load_batch | 100000 | multidb | 1 | 1000 | 79.95 | 12.4099 | 14.6755 | 17.0045 | 17.0045–17.0045 |
| sql | load_batch | 100000 | postgres | 1 | 1000 | 902.43 | 1.0422 | 1.1895 | 1.4324 | 1.4324–1.4324 |
| sql | mixed_c1 | 10000 | mongodb | 3 | 1500 | 3391.56 | 0.2183 | 0.6175 | 0.7506 | 0.6960–0.8680 |
| sql | mixed_c1 | 10000 | multidb | 3 | 1500 | 886.14 | 1.0339 | 1.6033 | 1.8472 | 1.7327–1.9355 |
| sql | mixed_c1 | 10000 | postgres | 3 | 1500 | 4851.96 | 0.1362 | 0.4536 | 0.5639 | 0.4039–0.5896 |
| sql | mixed_c1 | 100000 | mongodb | 3 | 1500 | 2965.05 | 0.2551 | 0.6817 | 0.8993 | 0.7519–0.9772 |
| sql | mixed_c1 | 100000 | multidb | 3 | 1500 | 780.18 | 1.1722 | 1.8807 | 2.1593 | 2.0981–2.4148 |
| sql | mixed_c1 | 100000 | postgres | 3 | 1500 | 4602.39 | 0.1477 | 0.4725 | 0.6681 | 0.5062–0.9868 |
| sql | mixed_c8 | 10000 | mongodb | 3 | 12000 | 5844.51 | 1.0406 | 2.9981 | 4.3589 | 4.1402–4.9869 |
| sql | mixed_c8 | 10000 | multidb | 3 | 12000 | 2293.76 | 2.9826 | 6.6300 | 8.4442 | 8.4062–9.3818 |
| sql | mixed_c8 | 10000 | postgres | 3 | 12000 | 13877.75 | 0.4078 | 1.2461 | 1.7882 | 1.2984–2.1964 |
| sql | mixed_c8 | 100000 | mongodb | 3 | 12000 | 6270.61 | 0.9353 | 2.8029 | 4.1716 | 3.7330–4.9041 |
| sql | mixed_c8 | 100000 | multidb | 3 | 12000 | 2120.04 | 3.1561 | 7.5773 | 9.3096 | 8.6241–9.9451 |
| sql | mixed_c8 | 100000 | postgres | 3 | 12000 | 15001.25 | 0.3553 | 1.1194 | 1.7136 | 1.5468–3.5903 |
| sql | point_read | 10000 | mongodb | 3 | 3000 | 4943.97 | 0.1818 | 0.2595 | 0.3116 | 0.2970–0.3693 |
| sql | point_read | 10000 | multidb | 3 | 3000 | 923.98 | 1.0133 | 1.2866 | 2.8671 | 1.4856–3.3173 |
| sql | point_read | 10000 | postgres | 3 | 3000 | 10230.94 | 0.0890 | 0.1242 | 0.1587 | 0.1426–0.2484 |
| sql | point_read | 100000 | mongodb | 3 | 3000 | 5027.65 | 0.1850 | 0.2604 | 0.3144 | 0.2776–0.3810 |
| sql | point_read | 100000 | multidb | 3 | 3000 | 841.92 | 1.1384 | 1.4815 | 2.2831 | 1.8124–2.7305 |
| sql | point_read | 100000 | postgres | 3 | 3000 | 8284.13 | 0.1082 | 0.1546 | 0.2099 | 0.1931–0.2402 |
| sql | range_read | 10000 | mongodb | 3 | 300 | 1517.17 | 0.5871 | 0.6583 | 0.7515 | 0.6655–0.8010 |
| sql | range_read | 10000 | multidb | 3 | 300 | 756.75 | 1.2366 | 1.5165 | 1.6890 | 1.4551–1.8079 |
| sql | range_read | 10000 | postgres | 3 | 300 | 3844.97 | 0.2062 | 0.2595 | 0.3131 | 0.2449–0.3602 |
| sql | range_read | 100000 | mongodb | 3 | 300 | 1559.96 | 0.5736 | 0.6328 | 0.8751 | 0.7180–1.0846 |
| sql | range_read | 100000 | multidb | 3 | 300 | 694.32 | 1.3537 | 1.5847 | 1.8432 | 1.6257–2.5389 |
| sql | range_read | 100000 | postgres | 3 | 300 | 3532.38 | 0.2248 | 0.2914 | 0.3103 | 0.3098–0.3156 |
| sql | upsert | 10000 | mongodb | 3 | 900 | 1831.49 | 0.4986 | 0.7170 | 1.0769 | 0.8922–1.7427 |
| sql | upsert | 10000 | multidb | 3 | 900 | 686.57 | 1.4233 | 1.7320 | 2.1854 | 2.1021–2.2636 |
| sql | upsert | 10000 | postgres | 3 | 900 | 2741.78 | 0.3586 | 0.4852 | 0.5446 | 0.5314–0.7432 |
| sql | upsert | 100000 | mongodb | 3 | 900 | 1873.15 | 0.5132 | 0.7315 | 0.9955 | 0.8897–1.1549 |
| sql | upsert | 100000 | multidb | 3 | 900 | 585.01 | 1.6204 | 1.9602 | 2.4111 | 2.3312–3.1602 |
| sql | upsert | 100000 | postgres | 3 | 900 | 2673.51 | 0.3505 | 0.4821 | 0.7752 | 0.5771–0.8262 |
| tensor | tensor_append_batch_cpu_reference_n10000 | 10000 | deeplake | 3 | 471 | 556.58 | 1.4460 | 2.0227 | 9.4193 | 8.9597–11.1353 |
| tensor | tensor_append_batch_cpu_reference_n10000 | 10000 | multidb | 3 | 471 | 228.10 | 4.2259 | 4.9544 | 6.4705 | 6.3452–6.6192 |
| tensor | tensor_append_batch_cpu_reference_n100000 | 100000 | deeplake | 3 | 4689 | 216.14 | 3.4493 | 5.6404 | 56.1777 | 52.8349–56.5714 |
| tensor | tensor_append_batch_cpu_reference_n100000 | 100000 | multidb | 3 | 4689 | 207.86 | 4.6383 | 5.3690 | 7.6148 | 6.6211–8.3392 |
| tensor | tensor_exact_l2_search_cpu_reference_n10000 | 10000 | deeplake | 3 | 600 | 164.67 | 5.9550 | 8.9927 | 9.8821 | 9.4559–11.8012 |
| tensor | tensor_exact_l2_search_cpu_reference_n10000 | 10000 | multidb | 3 | 600 | 37.92 | 26.2545 | 28.2679 | 29.2361 | 29.1804–31.8175 |
| tensor | tensor_exact_l2_search_cpu_reference_n100000 | 100000 | deeplake | 3 | 600 | 15.93 | 55.7262 | 84.3861 | 85.6696 | 83.9768–86.1987 |
| tensor | tensor_exact_l2_search_cpu_reference_n100000 | 100000 | multidb | 3 | 600 | 3.60 | 277.5441 | 292.2477 | 303.2132 | 282.5329–303.4776 |
| tensor | tensor_projected_read_cpu_reference_n10000 | 10000 | deeplake | 3 | 600 | 1855.02 | 0.3294 | 0.4322 | 0.5714 | 0.5591–0.6549 |
| tensor | tensor_projected_read_cpu_reference_n10000 | 10000 | multidb | 3 | 600 | 1209.24 | 0.5934 | 0.7432 | 0.9257 | 0.7723–1.0946 |
| tensor | tensor_projected_read_cpu_reference_n100000 | 100000 | deeplake | 3 | 600 | 1971.15 | 0.2802 | 0.4504 | 0.6396 | 0.5267–0.6952 |
| tensor | tensor_projected_read_cpu_reference_n100000 | 100000 | multidb | 3 | 600 | 1219.80 | 0.6216 | 0.6859 | 0.7281 | 0.6997–0.9835 |
| vector | vector.ann_k10.n1000 | 1000 | multidb_http | 3 | 600 | 470.40 | 1.8742 | 2.1044 | 2.3359 | 2.3025–2.3927 |
| vector | vector.ann_k10.n1000 | 1000 | qdrant_http | 3 | 600 | 1094.59 | 0.6710 | 0.7609 | 0.8044 | 0.7789–0.9769 |
| vector | vector.ann_k10.n10000 | 10000 | multidb_http | 3 | 600 | 165.63 | 5.6471 | 6.6547 | 7.6311 | 7.4212–9.1601 |
| vector | vector.ann_k10.n10000 | 10000 | qdrant_http | 3 | 600 | 719.17 | 1.1370 | 1.2507 | 1.4857 | 1.3483–1.5298 |

Percentiles with fewer than 100 successful samples are descriptive small-sample observations. Load sections may contain only one repetition; they must not be described as three-repeat load trials.
