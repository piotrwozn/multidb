# Local SQL optimization measurements: 2026-10-28

The native local comparison of baseline `b2bde5f` and candidate `6f98d85` improved median request latency and throughput in all 18 non-load workload/dataset cells. At 100,000 rows, the 100-ID range median fell from 110.385 ms to 1.331 ms (82.90×), grouped aggregation from 110.667 ms to 2.771 ms (39.94×), and mixed concurrency-8 throughput rose from 879.98 to 2,003.55 requests/s (2.28×). Two observed p99 increases and higher sampled peak RSS are retained below.

## Configuration and evidence

- Baseline source: `b2bde5f9feeb21d27859281e2c5d1744a1b60d5c`, clean build checkout when built.
- Candidate source: `6f98d85d3b604c947a46c722cdcacfa0c7b8340b`, clean build checkout when built and measured.
- Both native binaries were built with Rust/Cargo 1.99.1 using `cargo build --locked --release --bin multidb`, default features, `CARGO_PROFILE_RELEASE_DEBUG=0` and four build jobs. Rust 1.89 was a separate MSRV check, not the benchmark compiler. Cargo.lock changed only to add the direct `tokio-util` dependency, which was already locked transitively; dependency package versions did not change.
- Transactional profile with immediate durable commits, SCRAM authentication and the benchmark principal mapped to admin. Audit stayed at the profile default. Deployment-specific overrides were removed; no audit, authorization or durability bypass was set. This admin fixture has no row or masking policies.
- Loopback simple PostgreSQL protocol with autocommit and no prepared statements. Local-dev mode disables transport TLS for these loopback connections.
- Native server affinity: CPUs 0,1. Client affinity: CPUs 2,3,4. The shared host cgroup budget was 4 CPUs and 16 GiB RAM. Affinities are not exclusive CPU reservations. There was no per-server Docker memory cap, and no compilation ran during either measured phase.
- Python 3.12.14; psycopg and psycopg-binary 3.3.6; pymongo 4.19.0; dnspython 2.9.0; typing-extensions 4.16.0.
- Identical deterministic seed 20261028, 256-byte repeated-hex payload, primary-key/id and category indexes, batch size 100 and fresh databases. Read fixtures contain 10,000 and 100,000 rows; insert workloads use a separate growing table.
- All nine workloads ran in their original fixed sequence, with three repetitions. Each repeat has 1,000 point reads, 100 category reads, 100 ranges, 30 aggregates, 300 single inserts, 30 insert batches, 300 replacement upserts, 500 mixed operations at concurrency 1 and 500 per worker at concurrency 8. Each dataset was loaded once before the measured repetitions.
- The independent verifier accepted both phases: 56 sections and 39,260 successful measured requests per phase, including 1,100 load batches; zero operation, setup or correctness failures. Both servers exited cleanly with status 0.

The archived build checkout path `/workspace/scratch/multidb-benchmark-before` was reused for candidate compilation only after the immutable baseline binary and its source hashes were archived. The per-phase `source-provenance.json` identifies the actual revisions; the directory name does not identify the candidate as baseline.

## Complete results

Values below are medians of the three per-repeat measurements. Load has one measurement per dataset. Throughput is successful requests per second; a batch request writes 100 records. Ratios above one indicate higher candidate throughput. No workload cell was removed or replaced.

| Rows | Workload | Before p50 ms | After p50 ms | p50 speedup | Before req/s | After req/s | Throughput ratio |
|---:|---|---:|---:|---:|---:|---:|---:|
| 10,000 | aggregate | 8.661 | 2.290 | 3.78× | 113.36 | 420.27 | 3.71× |
| 10,000 | category_read | 1.491 | 1.326 | 1.12× | 632.23 | 712.71 | 1.13× |
| 10,000 | insert_batch | 10.734 | 10.311 | 1.04× | 92.30 | 96.83 | 1.05× |
| 10,000 | insert_single | 1.846 | 1.705 | 1.08× | 529.68 | 575.31 | 1.09× |
| 10,000 | load_batch | 10.066 | 9.775 | 1.03× | 99.05 | 104.25 | 1.05× |
| 10,000 | mixed_c1 | 1.043 | 1.022 | 1.02× | 788.73 | 838.78 | 1.06× |
| 10,000 | mixed_c8 | 7.863 | 2.935 | 2.68× | 987.48 | 2,248.02 | 2.28× |
| 10,000 | point_read | 1.175 | 0.933 | 1.26× | 841.89 | 1,059.23 | 1.26× |
| 10,000 | range_read | 8.257 | 1.209 | 6.83× | 114.75 | 765.64 | 6.67× |
| 10,000 | upsert | 1.762 | 1.395 | 1.26× | 553.33 | 699.94 | 1.26× |
| 100,000 | aggregate | 110.667 | 2.771 | 39.94× | 8.91 | 353.05 | 39.63× |
| 100,000 | category_read | 4.585 | 4.102 | 1.12× | 200.93 | 226.43 | 1.13× |
| 100,000 | insert_batch | 11.326 | 10.263 | 1.10× | 87.91 | 97.04 | 1.10× |
| 100,000 | insert_single | 2.083 | 1.877 | 1.11× | 469.17 | 522.32 | 1.11× |
| 100,000 | load_batch | 13.737 | 12.955 | 1.06× | 73.08 | 77.28 | 1.06× |
| 100,000 | mixed_c1 | 1.269 | 1.152 | 1.10× | 653.81 | 780.61 | 1.19× |
| 100,000 | mixed_c8 | 8.669 | 3.349 | 2.59× | 879.98 | 2,003.55 | 2.28× |
| 100,000 | point_read | 1.253 | 1.064 | 1.18× | 790.19 | 916.96 | 1.16× |
| 100,000 | range_read | 110.385 | 1.331 | 82.90× | 9.01 | 705.41 | 78.26× |
| 100,000 | upsert | 1.972 | 1.596 | 1.24× | 491.58 | 590.36 | 1.20× |

## Tails, resources and limits

The median per-repeat p99 for 100k point reads increased from 1.8008 ms to 1.8761 ms (+4.18%), while its p50 and p95 improved. The 10k load p99 increased from 12.7571 ms to 12.8041 ms (+0.37%). All other observed median p95/p99 values improved. These observations are descriptive; three repetitions do not establish whether small tail differences will recur.

Sampled process peak RSS increased from 590.0 MiB to 625.0 MiB (+5.94%). Overall phase wall time, including initialization, loading and validation, fell from 124.46 s to 57.37 s. One-second resource samples observed approximately 95.04 versus 35.85 seconds of server CPU time within their sampling windows. Resource samples cover whole phases, not individual workloads or complete lifetime accounting.

Per-request latency excludes result assertions; section throughput includes Python loop, bookkeeping and validation overhead. The repetitions reuse and mutate their phase’s fixture in the same sequence rather than creating three independent databases. The payload is compressible and the data fit in memory. There is one before phase followed by one after phase, with no reversed-order campaign to quantify host drift. Small gains and small tail changes warrant further repetition before strong claims.

This run does not measure cold storage, datasets larger than RAM, crash recovery, failover, distributed throughput, row-policy/masking cost, or production tail latency. It is a local comparison of these two revisions. The supplied historical PostgreSQL/MongoDB campaign used another revision and environment; its numbers were not reused as this baseline or rerun here.

## Reproduction and artifact identities

The [machine-readable summary and provenance](2026-10-28-sql-optimization.json) retain every comparison cell and metric. See [the local benchmark runner guide](../performance-local-benchmarks.md) for setup, the reviewed harness, exact default counts, runner checks and deterministic regression-test guidance. The campaign directory is `/workspace/scratch/multidb-local-benchmark-20261028T152921Z/`. It contains `run.json`, the resolved driver lock, immutable per-phase binaries, source and harness provenance, the exact runner source used by each phase, server/resource logs, raw latency CSV, per-repeat summaries and independent verification. `comparison.json` preserves min/max as well as p50/p95/p99 and sample counts; `comparison.md` is the generated complete table.

| Artifact | SHA-256 |
|---|---|
| Baseline binary | `713d72a02d9c4d7cf62fee4a58892ccfb13f97b1a1479afbcd720246c6ee310b` |
| Candidate binary | `3b881025915d07dc147aef7086f1dd56b50d06dd01a2824c01f5cefe1bc3eb59` |
| Reviewed benchmark.py | `1288ed7c8dda92e3f8a9af8556401d3eee14b03560946e74ca860585c699b4f4` |
| Reviewed initialize-multidb.py | `309135a4ad97e66422e31fcd5e6492a26b5a75ead218b824f01a44793e784609` |
| Reviewed verify-results.py | `197827e4922b7f46f98ea864ef104557114dd714d6ecc646e8b54e258101d832` |

The benchmark harness and its inputs were identical across phases. Runner cleanup and late-server-exit checks were strengthened after the baseline; each version is archived and hashed. The baseline had already completed independent verification and clean shutdown. These runner changes did not alter the queries, fixture, operation counts or measured timings.
