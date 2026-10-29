# Reproducing the pinned competitor suite

This suite compares the native MultiDB release artifact with a selected, pinned
version of PostgreSQL, MongoDB, DuckDB, Redis, Qdrant, Neo4j and Deep Lake. It does
not enumerate every database or assert that these are each vendor's latest
release. PostgreSQL and MongoDB pins reproduce the reference versions in the
supplied benchmark archive; other pins are explicit in the runner and lock file.

Install `scripts/benchmarks/requirements-competition.txt` into a dedicated Python
virtual environment. The driver intentionally never builds or downloads during
measurements. Install/pull the digest-pinned images from `competition.IMAGES`
before the run. Docker uses its local Unix socket and disposable, uniquely named
containers, with published ports bound only to loopback. The literal credentials
in the runner are public disposable benchmark values and must never be reused
for deployments.

The original SQL harness is external reviewed input. Prepare it with the
hash-checking tool described in [local benchmarks](performance-local-benchmarks.md).
A changed harness must be reviewed and assigned new hashes; it is not executed
merely because an uploaded archive contains a script.

```sh
cargo build --locked --release --bin multidb
python scripts/benchmarks/competition.py \
  --binary /absolute/path/to/immutable/multidb \
  --revision <source-commit> \
  --harness /absolute/path/to/reviewed/harness \
  --output /absolute/path/to/new/results \
  --rows 10000,100000 --repeats 3 --ops 200 --plan
```

Review the printed resource/host plan, then run the same command without
`--plan`. Run on an otherwise idle host; the runner refuses active compiler
processes and detects contamination during collection. Native server and client
CPU affinity are disjoint. Competitor containers have a two-CPU quota and 4 GiB
memory cap; the native server has affinity and the host/cgroup memory limit.
Those memory limits are different and are recorded, not presented as identical.
For the default sizes, budget about 12 GiB of free workspace disk after building;
compiler caches, Docker's storage driver, AOF/journals and disposable fixtures
all consume real space. The resource trace records output-filesystem free space.
A comparator's disk-space failure invalidates the run.
In-process DuckDB/Deep Lake use the client-side CPU affinity. Resource sampling
coverage is recorded; do not treat one server PID as an entire process tree.

The runner copies and hashes the binary before starting, validates SQL readiness,
preserves failures/raw samples, and checks unchanged hashes and graceful exit
before writing `completion.json`. Interrupted/failed campaigns cannot qualify
for a comparative report. Output paths must be fresh. It never flushes a shared
Redis database or overwrites an existing data fixture.

## Workloads and semantics

| Domain | Compared operations and validation |
| --- | --- |
| SQL/PostgreSQL/MongoDB | Identical four-field records with256B payload and primary/category indexes; durable load, single/batch inserts, point/category/range reads, group count/sum, upsert and80/20 mixed traffic at concurrency1/8; three repeated runs at10k/100k; full correctness checks and independent raw CSV verification. SQL uses the same psycopg simple-query protocol. MongoDB uses BSON Int64, w=1/j=true, ordered bulk writes, with its different batch atomicity disclosed. |
| Analytical/DuckDB | Integer group count/sum, ordered range100, projected/full scans and filtered counts; source values and complete answers checked. DuckDB's in-process API includes a different transport/serialization cost from MultiDB HTTP. Schema/load is outside query timing. |
| KV/Redis | Equal full logical records, point GET, full-record upsert and atomic100-record insert batches. Redis requires AOF appendfsync=always, authenticated RESP and no client retries. MultiDB uses HTTP SQL and only the storage primary key. Redis MULTI/EXEC does not provide SQL-style rollback after runtime command errors; errors invalidate sections. |
| Vector/Qdrant |128D L2 HNSW top10 at1k/10k vectors, explicit index/search settings, actual index readiness, exact NumPy references, returned-distance validation and recall thresholds. Server-side filtering and extreme-float behavior are recorded separately; unsupported MultiDB options must be rejected, not silently ignored. |
| Graph/Neo4j | Directed deterministic graph with four outgoing edges per node,1k/10k nodes; exact depth1/2 reachable-node sets, with duplicate/missing-node checks. Fixture creation is outside query latency. Timed durable edge updates validate successful acknowledgements (and one affected Neo4j relationship); MultiDB's public HTTP API cannot read back edge properties, so stored `write_seq` values are not independently verified. |
| Tensor/Deep Lake | Named text, U8 images and F32 embeddings; committed append batches, projected32-sample reads, immutable revisions and exact CPU L2 top10 checked against NumPy. All source values/shapes/dtypes are checked. This is explicitly CPU-reference mode. Deep Lake's local commit API is not asserted equivalent to redb Immediate fsync. |

Operation latency stops before validation. Section throughput includes client
validation and recording, so it is not a pure server throughput limit. The
legacy SQL workload uses warm data; each domain records its own warmup behavior.
Repetitions can reuse/mutate a fixture, as described by the adapter; they are not
all independent fresh database loads. Load may have only one repetition.
HTTP connections are reopened outside timing after another engine's long
fixture load, which can exceed the idle keepalive timeout. Measured requests
are never silently retried.

Inspect p50/p95/p99, errors, sample counts and the range across repetitions.
Very small samples, especially aggregate/full-scan/load tails, do not support
strong p99 claims. Approximate vector searches need recall beside latency.
Auditing, WAL, index maintenance, protocol and library differences remain part
of the observed deployment cost; disclose them before interpreting a ratio.

Generate a compact Markdown/JSON/CSV summary only from a completed campaign:

```sh
python scripts/benchmarks/report_competition.py /absolute/results \
  --output-prefix /absolute/new-summary
```

Domains can instead run as isolated campaigns using `--domains sql`,
`--domains analytics`, `--domains kv`, `--domains graph`, `--domains tensor`
and `--domains vector`. Use a fresh output directory for each, the identical
immutable binary and matching host/workload settings. The qualified audit used
this layout; it did not claim one uninterrupted server process. Combine those
completed campaigns with full-suite coverage required:

```sh
python scripts/benchmarks/report_competition.py \
  /absolute/results-sql /absolute/results-analytics /absolute/results-kv \
  /absolute/results-graph /absolute/results-tensor /absolute/results-vector \
  --require-full-suite --output-prefix /absolute/new-summary
```

The combiner checks artifact identity, compatible settings, duplicate domains,
independent proofs and complete domain coverage; an incomplete or failed
campaign does not become qualified by combining it with successful campaigns.

The full evidence includes source/artifact hashes, dependency versions, image
digests/configuration, resource samples, individual successful and failed
requests, setup errors, raw SQL plans, correctness verification and shutdown
status. Keep failed campaigns when investigating fixes and repeat the affected
workloads after changing the engine.

A loopback, fit-in-RAM run cannot certify cold storage, larger-than-memory
workloads, distributed failure behavior, production tails or hardware GPU
performance. No physical GPU is available in the current audit environment;
software-kernel tests and CPU reference comparisons are reported separately.
