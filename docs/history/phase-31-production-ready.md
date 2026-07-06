# Phase 31 Production Readiness

Phase 31 is implemented as a production-ready performance layer over the
existing storage, transaction, and query contracts. It keeps the crate
`unsafe`-free and avoids native ANN/SIMD dependencies.

Implemented in this pass:

- Advanced relational index metadata: column, `lower()` expression, partial
  predicates, covering payloads, and serde-compatible legacy `RelIndexSpec`
  decoding.
- Costed access paths for B-tree, index-only, and bitmap AND/OR scans in the
  simple SELECT fast path, with conservative predicate implication for partial
  indexes.
- Covering index payloads stored in `rel_indexes`, allowing index-only reads
  when the projection is covered by the indexed expression plus `include`
  columns.
- Vector search options with metadata filters, exact filtered rerank semantics,
  SQ/PQ code helpers, DiskANN-style bounded rerank mode, non-blocking rebuild on
  capacity growth, and Euclidean L2 distances.
- Columnar segment metadata in `rel_columnar_segment_meta`: per-segment row
  count, byte size, min/max, and null counts.
- Zone-map skipping for simple columnar equality filters and an explicit
  `SegmentSkipReport` for verification.
- Time-series Gorilla v1 chunk encoding with legacy decoder compatibility.
  Regular series now meet the phase gate of at least 8x compression in unit
  tests.
- Benchmark harness coverage for covering indexes, vector quantization,
  columnar aggregation, and Gorilla encoding.

Verification run on 2026-07-03:

- `scripts/check.ps1` passes, including clippy with `-D warnings`, deny checks,
  `cargo test --all-features`, and doctests.
- `cargo test --all-features` passes: 305 tests plus 2 doctests.
- `cargo bench --bench performance_micro` passes.
- `cargo bench --bench columnar_aggregation` passes.
- `scripts/perf.ps1 -Rows 1000 -Output target/perf/phase31.json` writes the
  phase 31 harness report.

Measured harness output:

- `target/perf/phase31.json`: 1000 rows, benches
  `performance_micro,columnar_aggregation`, feature set
  `covering_index,bitmap_index,vector_quantization,filtered_ann,zone_maps,gorilla`.
- `performance_micro`: covering index lookup p50 around 4.68 us, scalar
  quantized length check around 383 ps, scalar quantization around 269 ns,
  product quantization around 73 ns, Gorilla regular chunk encode around 984 ns.
- `columnar_aggregation`: row-store group-by around 3.97 ms and
  columnar/Arrow/Parquet group-by around 3.51 ms in the local Criterion run.

Criterion reported local baseline changes for some existing microbenchmarks
(`value_encode`, `covering_index_lookup`, and `row_store_redb` group-by in the
final perf run). The benchmark commands completed successfully; those baseline
comparisons should be treated as follow-up performance tracking signals rather
than correctness blockers for this phase.

Production notes:

- Expression indexes intentionally support the deterministic `Column` and
  `LowerAscii` core only. Unsupported expressions fall back to existing query
  execution rather than being planned heuristically.
- DiskANN-style search is implemented inside the existing portable storage
  contract. It does not bind to native DiskANN or FAISS libraries.
- Columnar pushdown is conservative and inexact: segment metadata is used to
  skip impossible segments, while row-level predicates are still validated after
  decode.
