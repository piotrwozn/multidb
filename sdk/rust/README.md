# multidb-client

Blocking Rust client for the MultiDB Control Plane API v1.

```rust
use multidb_client::ControlPlaneClient;

let client = ControlPlaneClient::new();
let session = client.login("admin", "local-dev-admin-password")?;
let authed = client.with_token(session.token);
println!("{:?}", authed.status()?);
authed.logout()?;
# Ok::<(), multidb_client::ControlPlaneError>(())
```

The default base URL is `http://127.0.0.1:8080/api`.

## GraphRAG retrieval

The caller supplies embeddings. MultiDB retrieves vector seeds, expands graph
relationships and returns context with source attribution; it calls no LLM or
embedding service. A complete insertion, connection and retrieval workflow:

```rust
use multidb_client::{ControlPlaneClient, GraphEdgeRequest, GraphRagRequest};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ControlPlaneClient::new()
        .with_token(std::env::var("MULTIDB_ADMIN_TOKEN")?);
    // Run creation once, or reuse objects provisioned from a database spec.
    client.create_vector(json!({"name": "chunks", "dim": 2}))?;
    client.create_graph(json!({"name": "knowledge", "graph_id": 1}))?;
    let first = client.insert_vector(
        "chunks",
        json!({"text": "MultiDB supports vector retrieval.", "source": "guide.md"}),
        vec![1.0, 0.0],
    )?;
    let second = client.insert_vector(
        "chunks",
        json!({"text": "Graph edges connect related documents.", "source": "graph.md"}),
        vec![0.0, 1.0],
    )?;
    let first_id = first["id"].as_str().ok_or("missing first vector ID")?;
    let second_id = second["id"].as_str().ok_or("missing second vector ID")?;
    client.add_graph_edge(
        "knowledge",
        &GraphEdgeRequest::new(first_id, "related", second_id),
    )?;
    let mut request = GraphRagRequest::new("chunks", "knowledge", vec![1.0, 0.0]);
    request.seed_k = Some(1);
    let result = client.graph_rag_query(&request)?;
    println!("{}\ntruncated={}", result.context, result.truncated);
    for source in result.sources {
        println!("document={} seed={} depth={}", source.document_id, source.seed_id, source.depth);
    }
    Ok(())
}
```

Graph nodes use the lowercase 32-character hexadecimal IDs returned by
`insert_vector`. Vector metadata supplies `text` and optional `source` strings.
`GraphRagRequest::new` omits optional knobs so server defaults apply: `seed_k=8`,
`max_depth=2`, `max_nodes=64`, `max_edges=256`, `max_context_bytes=16384`, labels
`["related"]`, text path `["text"]`, source path `["source"]`. Set fields to `Some`
to override them. `Some(0)` depth means seeds only; explicit empty labels or paths
are sent unchanged and rejected by the server. `GraphEdgeRequest::new` uses null
properties; set `props` to a JSON value to attach properties.

The response includes typed sources, seed distances/depths, relationships and a
`truncated` flag. Check that flag before passing context to your application or an
LLM. Requests preserve the client's bearer token and optional principal header;
authorization/validation failures return `ControlPlaneError` with server status,
code and message. The default transport is blocking HTTP; custom transports can
implement the existing `Transport` trait.

Run the deterministic transport tests without a server:

```bash
cargo test --locked --manifest-path sdk/rust/Cargo.toml
```

## Typed tensors on a GPU server

Use a server opened with the `tensor_dataset` profile and a supported native GPU.
The profile refuses to open without one. The client never executes local kernels,
silently falls back to CPU, or invents a GPU device flag. Other storage profiles
can host typed datasets with reference CPU operations; that is a separate mode.
These native APIs do not claim DeepLake compatibility.

```rust
use multidb_client::{
    ControlPlaneClient, Tensor, TensorBatch, TensorColumn, TensorComputeRequest,
    TensorDType, TensorData, TensorDatasetConfig, TensorDevice, TensorGpuOperation,
    TensorMetric, TensorReadRequest, TensorSearchRequest,
};

fn tensor_example(client: &ControlPlaneClient) -> Result<(), Box<dyn std::error::Error>> {
    // Create once; tensor_dataset_info("training", None) inspects an existing dataset.
    client.create_tensor_dataset(&TensorDatasetConfig::new("training", vec![
        TensorColumn { name: "text".into(), dtype: TensorDType::Utf8, shape: vec![] },
        TensorColumn { name: "image".into(), dtype: TensorDType::U8, shape: vec![None] },
        TensorColumn { name: "embedding".into(), dtype: TensorDType::F32, shape: vec![Some(2)] },
    ]))?;
    let batch: TensorBatch = [
        ("text".into(), vec![Tensor { shape: vec![], data: TensorData::Utf8(vec!["a caption".into()]) }]),
        ("image".into(), vec![Tensor { shape: vec![3], data: TensorData::U8(vec![0, 128, 255]) }]),
        ("embedding".into(), vec![Tensor { shape: vec![2], data: TensorData::F32(vec![1.0, 0.0]) }]),
    ].into();
    let appended = client.append_tensors("training", &batch)?;
    let snapshot = client.read_tensors("training", &TensorReadRequest {
        limit: Some(1), columns: Some(vec!["text".into(), "embedding".into()]),
        revision: Some(appended.revision), ..TensorReadRequest::default()
    })?;
    let mut search = TensorSearchRequest::new("embedding", vec![1.0, 0.0]);
    search.k = Some(1);
    search.metric = Some(TensorMetric::L2);
    let found = client.search_tensors("training", &search)?;
    let mut compute = TensorComputeRequest::new("embedding", TensorGpuOperation::L2Norm);
    compute.limit = Some(1);
    let reduced = client.compute_tensors("training", &compute)?;
    if found.execution.device != TensorDevice::Gpu || reduced.execution.device != TensorDevice::Gpu {
        return Err("this example requires a Tensor GPU server".into());
    }
    println!("revision={} hits={:?} reductions={:?} execution={:?}",
        snapshot.revision, found.hits, reduced.values, reduced.execution);
    Ok(())
}
```

Every append aligns the sample count across columns. Scalar schemas use `[]`, and
`None` schema dimensions are ragged; concrete sample shapes contain only sizes.
`TensorData` is an externally tagged numeric/string array, e.g. `{"f32":[1,0]}` or
`{"u8":[0,128,255]}`. The six variants preserve their numeric types, and server
storage uses native binary chunks rather than JSON document rows.

Optional request fields are omitted by default, so the server applies its defaults:
64 samples/4194304 bytes per chunk; read start 0/limit 100; search k 10, cosine metric
and at most 10000 scanned samples. `Some(0)` revision is preserved, including
`tensor_dataset_info("training", Some(0))` for the empty initial revision. `None`
selects the current revision. Search and compute results contain the actual server
execution device, adapter and backend. GPU unavailability remains a
`ControlPlaneError` with HTTP 503 and code `tensor_gpu_unavailable`, without CPU fallback.
