# Native tensor datasets

Status: **Preview**. MultiDB stores aligned, typed tensor samples in binary
column chunks, with immutable append revisions. The `tensor_dataset` product
profile (`tensor` storage profile) requires a real GPU for tensor search and
reductions. It refuses to open when the GPU is unavailable; there is no automatic
CPU fallback.

User samples live in checksummed binary tensor-column chunks, with a sample index
for projected reads. The catalog, schema, revision records and WAL remain normal
database metadata; disk persistence does not require keeping all data in GPU
memory. Kernels receive bounded batches of the requested tensor data.

These are MultiDB APIs. DeepLake wire, SDK and dataset-format compatibility are
not provided.

## Run the example

From a fresh checkout, with a supported native GPU and its driver:

```bash
cargo run --locked --example tensor_dataset -- --db ./tensor-gpu.redb
```

The `tensor-gpu` feature is enabled by default. Production selects discrete,
integrated or virtual GPUs through Vulkan, Metal or DX12, and rejects software
CPU adapters. A container also needs access to the host device and driver; see
[the Docker guide](tensor-datasets-docker.md). A GPU initialization error is
returned before the new database file is created.

For an explicit reference run on a machine without a GPU:

```bash
cargo run --locked --no-default-features --example tensor_dataset -- \
  --cpu-reference --db ./tensor-reference.redb
```

That flag chooses `Profile::Balanced` and CPU reference operations. It does not
emulate the Tensor profile. The example creates a new file, refuses an existing
target, and prints its actual execution device. It appends scalar text, ragged
image bytes and fixed-size embeddings; reads selected columns at revision 1;
appends another sample; and verifies that the older revision stays unchanged.
The search's first hit is sample 0 with distance 0, and the image sums are
`[383, 30]`. The current dataset ends with three samples while revision 1 retains
two. Choose another `--db` path to run again, or remove the example file after
the process closes if you no longer need it.

The implementation is [examples/tensor_dataset.rs](../examples/tensor_dataset.rs).

## Model samples as typed columns

A dataset has a fixed schema. Every append supplies every column with the same
number of samples; one sample position identifies the aligned values across all
columns. Names such as `text`, `image`, `embedding` or `source` are conventions,
not reserved metadata fields. MultiDB does not generate embeddings or interpret
image formats.

| Dtype | Sample data JSON | Meaning |
| --- | --- | --- |
| `u8` | `{"u8":[0,128,255]}` | Unsigned byte values, sent as numbers, not base64. |
| `i64` | `{"i64":[-1,42]}` | Signed 64-bit integers. |
| `f32` | `{"f32":[1.0,0.5]}` | Finite 32-bit floats. |
| `f64` | `{"f64":[1.0,0.5]}` | Finite 64-bit floats. |
| `bool` | `{"bool":[true,false]}` | Boolean values. |
| `utf8` | `{"utf8":["a caption"]}` | UTF-8 strings. |

The data object contains exactly one dtype array. Its dtype must match the
column declaration. Shapes describe one sample:

- Schema `[]` is scalar and needs exactly one data element.
- Schema `[3]` is a fixed vector with three elements.
- Schema `[null]` is ragged; samples can declare shapes such as `[3]` and `[2]`.
- Schema `[null,3]` has a varying first dimension and a fixed second dimension.

Concrete samples contain integer dimensions, never `null`. Their element count
must equal the shape product. Zero dimensions represent empty tensors. Dataset
schemas cannot be changed in place, and this preview provides no sample update
or delete operation.

## Create and append through HTTP

Configure `multidb serve` authentication and transport as described in
[docker.md](docker.md), and select `MULTIDB_PROFILE=tensor_dataset` for GPU
execution. The following paths are relative to the Control Plane `/api` prefix.
Requests use `Authorization: Bearer <session token>` and the standard
`{"ok":true,"data":...}` response envelope. See `/api/openapi.json` for the full
schema. The SDKs expose the same six operations.

Create with `POST /api/builder/tensor-dataset`:

```json
{
  "name": "training",
  "tensors": [
    {"name": "text", "dtype": "utf8", "shape": []},
    {"name": "image", "dtype": "u8", "shape": [null]},
    {"name": "embedding", "dtype": "f32", "shape": [3]}
  ],
  "chunk_samples": 64,
  "max_chunk_bytes": 4194304
}
```

The returned dataset info includes `config`, `samples: 0` and `revision: 0`.
The limits shown are the defaults when omitted. The catalog entry, initial
manifest and empty revision are created atomically.

Append two aligned samples with `POST /api/data/tensor-datasets/training/append`:

```json
{
  "tensors": {
    "text": [
      {"shape": [], "data": {"utf8": ["first caption"]}},
      {"shape": [], "data": {"utf8": ["second caption"]}}
    ],
    "image": [
      {"shape": [3], "data": {"u8": [0,128,255]}},
      {"shape": [2], "data": {"u8": [10,20]}}
    ],
    "embedding": [
      {"shape": [3], "data": {"f32": [1,0,0]}},
      {"shape": [3], "data": {"f32": [0,1,0]}}
    ]
  }
}
```

For a new dataset this returns `{"start":0,"end":2,"revision":1}` inside
`data`. Ranges use inclusive `start` and exclusive `end`. Append commits all
columns, sample indexes and the new revision together. A conflicting writer is
retried internally within a bounded retry budget. Failed validation writes no
partial sample.

JSON is the HTTP interchange format. Stored samples use binary records with
framing and CRC checks, not JSON documents or relational row payloads. Native
Rust callers can build `TensorBatch` directly.

## Read revisions and selected columns

`GET /api/data/tensor-datasets/training` returns current metadata. Add
`?revision=1` to read an earlier append manifest. Revision `0` explicitly means
the empty initial dataset; omitting the revision selects the current one.
Revision numbers are dataset append revisions, not commit-log LSNs or SQL
`AS OF` timestamps.

Read with `POST /api/data/tensor-datasets/training/read`:

```json
{"start":0,"limit":2,"columns":["text","embedding"],"revision":1}
```

The result contains `start`, `end`, `total_samples`, `revision` and `tensors`.
Only requested columns are returned. The immutable sample index locates relevant
chunks directly: unselected columns and preceding chunks are not read or
decoded. A relevant selected chunk may contain additional samples and is decoded
as a chunk. Omit `columns` to select all columns; an explicit empty selection is
invalid.

A later append creates a new revision without replacing earlier chunks or sample
positions. A read at revision 1 retains its original sample count and values.
Current reads capture a revision for that operation; pass the returned revision
to subsequent reads or searches when they need the same snapshot.

## Search on the actual GPU

`POST /api/data/tensor-datasets/training/search`:

```json
{"tensor":"embedding","query":[1,0,0],"metric":"L2","k":2,"start":0,"max_samples":100,"revision":1}
```

Tensor-profile search dispatches distance kernels on the native GPU. This preview
accepts fixed rank-one `f32` columns of dimension 1–16384, with a matching finite
query representable as `f32`. `Cosine` is the default metric; `L2` means Euclidean
distance. Metric strings are case sensitive. Cosine rejects a zero query and
skips zero-valued sample vectors, recording `skipped_samples`.

GPU inputs must be zero or normal finite `f32` values. Subnormal inputs and
subnormal L2 component differences return `tensor_gpu_unsupported`, because
native adapters can flush them to zero. A query that underflows to zero or
overflows when converted from `f64` is invalid. Tensor storage and CPU reference
calculations retain their wider numeric range.

This is bounded exact distance search over the requested sample range, not an
ANN index. Results contain `hits` (`index`, `distance`), `scanned_samples`,
`skipped_samples`, `total_samples`, `revision`, `next_start` and `execution`.
`next_start` is non-null when the range did not reach the end of the captured
revision. To search more ranges, reuse that revision, advance to `next_start`,
and merge their top-k results in the application. Each response is top-k within
its own scanned range. Equal distances are ordered by sample index.

`execution` reports `device`, `adapter` and `backend`. Tensor-profile results
report `device: "gpu"` with the selected adapter. Explicit reference datasets
under other profiles report `device: "cpu"`; CPU reference search supports
fixed rank-one `f32` and `f64`. The SDK forwards the server's metadata unchanged.
GPU calculations use `f32`; CPU reference calculations accumulate in `f64`.
Rounding and cancellation can produce different results, especially for sums
with widely different magnitudes. Search computes every requested distance;
it does not use an approximate nearest-neighbor index.

## Reduce F32 and U8 tensors

`POST /api/data/tensor-datasets/training/compute`:

```json
{"tensor":"image","operation":"sum","start":0,"limit":2,"revision":1}
```

Operations are `sum`, `mean` and `l2_norm`. Each produces one `f32` result per
sample by reducing all elements of that sample, including ragged samples. The
current kernels support `f32` and `u8`; other stored dtypes return
`tensor_gpu_unsupported`. An empty sample has sum/norm zero; its mean is undefined
and is refused. Non-finite inputs or unrepresentable results are refused.

The result contains `start`, `end`, `revision`, `values` and actual `execution`
metadata. The example's two image tensors sum to `[383,30]`. Tensor-profile
operations run WGSL compute kernels on the GPU; data loading, validation and
result packaging still use the host CPU. Explicit reference mode uses the same
request API with CPU calculations and labels its results accordingly.

## Limits and failures

| Surface | Current limit |
| --- | --- |
| Dataset schema | 1–64 unique columns; tensor rank at most 8. |
| Tensor elements | At most 4,000,000 per sample; each dimension is 0–4,000,000. |
| Append | 1–1024 aligned samples per request. |
| Physical chunks | 1–1024 samples; 1024–16777216 encoded bytes per column chunk. Defaults: 64 samples and 4194304 bytes. |
| Decoded payload | 64 MiB tensor-allocation budget for an append/read, including vector/string capacities. A read's decoded chunk cache and returned tensors share this budget. Encoded frames, HTTP serialization and allocator bookkeeping are additional, so this is not a process RSS cap. |
| Read/compute | 1–1024 samples; default 100. Read starts at 0 by default. |
| Search | k 1–100, default 10; max_samples 1–100000, default 10000. |
| GPU search | Fixed rank-one F32, dimension 1–16384. GPU dispatches are internally batched. |
| Execution deadline | Database reads, searches and reductions share the configured `query.timeout_ms` budget across their batches and GPU queue wait. Default: 30000 ms. GPU dispatch also has a 30-second device deadline. |
| HTTP request body | 1 MiB, including JSON overhead. Smaller than the native binary chunk maximum. Split large HTTP appends accordingly. |

The shared query governor also limits tensor read/search/compute concurrency.
Reducing a very large tensor or requesting a batch beyond the decoded or GPU
buffer budget can fail even when each schema dimension is individually valid.

Relevant HTTP errors include:

- `400 invalid_value`: malformed tensor values, mismatched shapes/counts or
  invalid limits. Invalid JSON/body-limit failures also use the existing 400 path.
- `404 not_found`: missing dataset or revision.
- `422 tensor_unsupported`: an unsupported native storage backend.
- `409 conflict`: concurrent appends exhausted the bounded retry budget.
- `422 tensor_gpu_unsupported`: an unsupported GPU tensor dtype, shape, numeric
  range or operation.
- `503 tensor_gpu_unavailable`: the required native GPU cannot be initialized.
- `504 query_timeout`: the configured query execution deadline expired.

Execution checks are cooperative between bounded processing steps; an individual
blocking storage call finishes before its result is checked. A timeout while
waiting for the GPU dispatch lock leaves the running dispatch alone. A timeout
after submission stops the GPU device to cancel outstanding work; subsequent
GPU operations return `tensor_gpu_unavailable` until the server restarts.

Authentication, authorization, query-governor and storage failures retain their
normal Control Plane error handling. Dataset creation uses the existing catalog
name rules; tensor column names must be unique, nonempty, at most 128 bytes and
contain no control characters.

## Access, durability and provisioning

Tensor datasets currently require a local single-node backend with native bounded
reads and conditional commits. AP, sharded databases and the live CP runtime are
outside this preview. The Tensor profile accepts tensor datasets as business
objects; ordinary tables, document/vector collections and graphs are refused.

Authenticated operations use `Resource::Collection(dataset_name)`: `Admin` for
creation, `Write` for append, and `Read` for metadata, reads, search and compute.
These permissions are distinct. Principal APIs enforce audit, query concurrency
and existing write limits/quotas. Value-based row/masking/required-path policies
are not interpreted as tensor transformations: principal operations fail closed
when such policies protect the dataset. Trusted Rust handles retain their
operator-level access.

Append snapshots are retained as native records. Full backup/restore preserves
the schema, binary chunks, sample indexes and revisions. Standard history GC does
not turn append revisions into SQL temporal tables or supply dataset revision GC.
Keep the usual backup and storage operational limits in mind when retaining
long-lived datasets.

Spec v2 supports `physical.kind: tensor_dataset`, `tensors`,
`tensor_chunk_samples` and `tensor_max_chunk_bytes`. Planning/validation can run
without a GPU; opening or applying a new Tensor-profile database still requires
one before file creation. Export, adoption and drift retain the exact schema and
chunk settings. Changing dtype, shape or chunk configuration is a conflict;
there is no alter API. See [provisioning.md](provisioning.md#native-tensor-datasets-and-gpu-profiles).

Client guides: [TypeScript](../sdk/typescript/README.md),
[Python](../sdk/python/README.md), [Go](../sdk/go/README.md),
[Rust](../sdk/rust/README.md).
