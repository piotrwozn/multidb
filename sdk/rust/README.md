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
