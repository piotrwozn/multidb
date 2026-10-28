# Reproducing local SQL performance comparisons

`scripts/benchmarks/compare.py` runs the reviewed external evaluation harness
against two native Linux release binaries. It does not compile, install dependencies,
start Docker, or modify the supplied harness. It creates a new database for each
phase and preserves all logs and raw observations. Existing output paths are
rejected. Only the loopback PostgreSQL and admin ports 15433 and 18080 are used.

The external campaign's historical baseline is commit `b378f4d`, which is not
evidence for the current source. The local optimization baseline is `b2bde5f`.
Compare binaries built with the same compiler, Cargo.lock, release settings and
default features. Archive each binary before another build overwrites it.
Record compiler version, build command, source revision and any uncommitted
patch with the artifacts. Build both binaries before starting a comparison.

## Preparation

Review `benchmark.py`, `initialize-multidb.py` and `verify-results.py` before
copying them into a separate scratch directory. The runner pins their SHA-256
hashes; changing the harness requires a fresh review and explicit hash update.
The copied directory needs `provenance.json` with its original directory and a
`files` object mapping filenames to hashes. Keep the original input and its
historical results intact. Do not run the supplied `run-benchmarks.sh`: its
fixed output paths and supplement merging belong to the original campaign.

The helper copies only the reviewed files, checks their pinned hashes and writes
the required provenance without executing the harness:

```sh
python scripts/benchmarks/prepare_harness.py \
  --source /workspace/scratch/benchmark-input/multidb-evaluation \
  --destination /workspace/scratch/multidb-benchmark-tools/harness
```

Create an isolated Python 3.12 environment. The benchmark imports both drivers
even with `--engines multidb`; charts and the full plotting stack are unnecessary.

```sh
uv venv /workspace/scratch/multidb-benchmark-tools/.venv
uv pip install --python /workspace/scratch/multidb-benchmark-tools/.venv/bin/python \
  'psycopg[binary]==3.3.6' 'pymongo==4.19.0'
uv pip freeze --python /workspace/scratch/multidb-benchmark-tools/.venv/bin/python
```

The reviewed campaign resolved the additional packages dnspython 2.9.0 and
typing-extensions 4.16.0. Archive the resolved lock/freeze and reuse it for both
phases. The runner saves installed package versions with the result.

The runner selects two CPUs from `sched_getaffinity` for the server and the
remaining allowed CPUs for the client. Explicit, disjoint CPU lists can be
supplied with `--server-cpus` and `--client-cpus`. These are CPU affinities, not
exclusive reservations. The recorded cgroup budget is the actual host limit;
native results do not inherit the historical Docker limit of 2 CPUs/4 GiB.

## Execution

Use `--plan` first to inspect settings without starting anything. Remove it to
run after both builds have finished and other workloads have stopped.

```sh
python scripts/benchmarks/compare.py \
  --before /workspace/scratch/benchmark-binaries/before \
  --after /workspace/scratch/benchmark-binaries/after \
  --before-revision b2bde5f --after-revision CANDIDATE_REVISION \
  --harness /workspace/scratch/multidb-benchmark-tools/harness \
  --python /workspace/scratch/multidb-benchmark-tools/.venv/bin/python \
  --output /workspace/scratch/benchmark-results/UNIQUE_CAMPAIGN --plan
```

Defaults reproduce the full nine-workload campaign at 10,000 and 100,000 rows,
with three measured repetitions: 1,000 point reads, 100 category reads, 100 range
reads, 30 aggregates, 300 single inserts, 30 batches of 100 inserts, 300 upserts,
500 mixed operations at concurrency 1, and 500 per worker at concurrency 8.
Loading each dataset is measured once, in batches of 100. For an initial smoke,
use `--rows 1000 --repeats 1 --read-ops 20 --write-ops 10 --batch-ops 2
--aggregate-ops 2 --mixed-ops 10` and a separate fresh output directory.

Each phase uses the transactional profile, SCRAM authentication, the benchmark
principal with its documented admin role, and the profile's default audit and
durability behavior. Plaintext connections are restricted to loopback under
local-dev mode. Deployment-specific `MULTIDB_*` environment variables are
removed before setting the campaign configuration. This is not a configuration
for an exposed production server.

Phases run sequentially under a process lock. The runner rejects occupied ports
and detects active Rust compilation before and during timing. Do not run other
CPU, disk or database workloads concurrently. The harness and server receive
separate CPU affinities; no compiler is launched by the runner. On failure or
interruption, the runner stops its own child processes and writes `FAILED.json`.
It never stops another server or deletes a database. Default maximum phase time
is two hours; `--max-seconds` can set another explicit limit.

## Interpreting the artifacts

`run.json` contains host limits, CPU affinity, commands and reviewed harness
provenance. Each phase preserves the exact binary and its hash, its revision
label, server log, initializer log, harness log, database, sampled server RSS,
high-water RSS, CPU time and I/O counters. `raw/` includes per-request latencies,
per-repetition summaries, startup/backoff events, plans and correctness failures.

The independent verifier recomputes counts, successful request/record throughput
and interpolated percentiles from the raw CSV, checks expected sections, and
rejects operation/setup/correctness errors. Only verified, complete phases
produce `comparison.json` and `comparison.md`. The comparison includes each
metric's median and minimum/maximum across repetitions. The load section has
only one sample. The throughput ratio is candidate/baseline, so values above one
are faster. The p50 speedup is baseline/candidate. Preserve the sample counts:
30 aggregate samples provide little evidence about p99.

SQL point reads use the primary-key equality filter. Category reads return 1%
of rows using a secondary equality filter. Range reads select 100 consecutive
IDs with `ORDER BY id`. Aggregates group into 100 categories with `COUNT` and
`SUM(value)`. Upserts replace all four columns using `INSERT ... ON CONFLICT`.
Mixed workloads are 80% point reads and 20% full-row upserts, with disjoint keys
per worker. Both primary-key and category indexes exist before loading.
The deterministic seed is 20261028 and the payload is 256 ASCII bytes per row.

Repetitions reuse and mutate their phase's dataset in the same sequence. They
are not three independently recreated databases. Per-operation latency excludes
validation; section throughput includes client loop and validation overhead.
Compare the same sequence on both binaries. A reversed-order second campaign
can help identify host drift; one local result does not establish production
capacity or a cross-database ranking.

## Deterministic regression coverage

The raw Arrow cache is shared across the SQL engine's tables and projections. Its
weighted capacity is the smaller of 64 MiB and one eighth of the configured query
memory budget, with an additional entry-count limit. It admits current, unlimited,
unfiltered native row-store scans. Snapshot, foreign, AP and sharded reads keep
their respective paths. A physical `rel_rows` version conservatively invalidates
cached scans across logical tables when any row table changes. Versions are
committed with the data and replayed; audit/profiler writes do not change them.
Policy transformations run for each request after the raw provider boundary.

SingleNode and CP native `read_many` calls use one read snapshot per bounded
batch. Index discovery and separate batches retain their existing consistency
contract. The SQL fast range path supports literal Int64 primary keys; unsupported
expressions use the regular planner. Warm scan-cache gains do not establish cold
disk performance or gains for projections larger than the cache budget.

The existing Criterion microbenchmarks exercise direct table handles and value
codecs, which do not cover PostgreSQL wire, SQL planning or authorization costs.
`scripts/perf.ps1` currently reports whole `cargo bench` wall time and repeats
that value in its latency fields; `-SkipBenchRun` produces synthetic values.
Those report-envelope checks are useful gate plumbing, but are not request
latency measurements. Keep their existing thresholds intact and use raw loopback
observations or Criterion estimates for performance evidence.

For SQL optimizations, add deterministic tests that count actual storage
operations: primary-key equality should perform bounded key lookups, indexed
category equality should visit matching index/row entries, and an ordered bounded
range should stop after its upper key bound. Assert exact result rows and order,
including empty ranges and boundary keys. An EXPLAIN plan alone does not prove
that execution used the index. Preserve authorization, masking, quotas, audit
and durable write semantics in the same optimized paths. Existing index-versus-
scan, covering-index, optimizer selectivity and read-guard tests are useful
fixtures to extend without making wall-clock assertions in unit tests.

Run the runner's deterministic tests without launching services:

```sh
python -m unittest discover -s scripts/benchmarks -p 'test_*.py'
```
