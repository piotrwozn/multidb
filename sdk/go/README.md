# MultiDB Go Client

Context-aware Go client for the MultiDB Control Plane API v1.

```go
client := multidb.NewClient()
session, err := client.Login(ctx, "admin", "local-dev-admin-password")
if err != nil {
    panic(err)
}
authed := client.WithToken(session.Token)
status, err := authed.Status(ctx)
```

The default base URL is `http://127.0.0.1:8080/api`.

## GraphRAG retrieval

GraphRAG uses caller-supplied embeddings and returns context, sources and graph
relationships. It does not call an embedding service or LLM. This example creates
two vector documents, connects their returned IDs, and retrieves a bounded context:

```go
package main

import (
    "context"
    "fmt"
    "os"
    "time"

    multidb "github.com/multidb/multidb/sdk/go"
)

func run() error {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    client := multidb.NewClient(multidb.WithToken(os.Getenv("MULTIDB_ADMIN_TOKEN")))
    // Run creation once, or reuse objects provisioned from a database spec.
    if _, err := client.CreateVector(ctx, multidb.JSONObject{"name": "chunks", "dim": 2}); err != nil {
        return err
    }
    if _, err := client.CreateGraph(ctx, multidb.JSONObject{"name": "knowledge", "graph_id": 1}); err != nil {
        return err
    }
    first, err := client.InsertVector(ctx, "chunks",
        multidb.JSONObject{"text": "MultiDB supports vector retrieval.", "source": "guide.md"},
        []float64{1, 0})
    if err != nil {
        return err
    }
    second, err := client.InsertVector(ctx, "chunks",
        multidb.JSONObject{"text": "Graph edges connect related documents.", "source": "graph.md"},
        []float64{0, 1})
    if err != nil {
        return err
    }
    firstID, ok := first["id"].(string)
    if !ok {
        return fmt.Errorf("vector insertion did not return an ID")
    }
    secondID, ok := second["id"].(string)
    if !ok {
        return fmt.Errorf("vector insertion did not return an ID")
    }
    if _, err := client.AddGraphEdge(ctx, "knowledge", multidb.GraphEdgeRequest{
        Source: firstID, Label: "related", Target: secondID,
    }); err != nil {
        return err
    }
    seedK := 1
    result, err := client.GraphRAGQuery(ctx, multidb.GraphRAGRequest{
        VectorCollection: "chunks", Graph: "knowledge", QueryVector: []float32{1, 0},
        SeedK: &seedK,
    })
    if err != nil {
        return err
    }
    fmt.Printf("%s\ntruncated=%v\n", result.Context, result.Truncated)
    for _, source := range result.Sources {
        fmt.Printf("document=%s seed=%s depth=%d\n", source.DocumentID, source.SeedID, source.Depth)
    }
    return nil
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Use the lowercase 32-character hexadecimal document IDs returned by `InsertVector`
as graph node names. Vector metadata supplies `text` and optional `source` strings.
Omitted options default to `seed_k=8`, `max_depth=2`, `max_nodes=64`, `max_edges=256`,
`max_context_bytes=16384`, labels `["related"]`, text path `["text"]` and source path
`["source"]`. For custom paths or labels, pass pointers to string slices. Explicit
empty lists are sent to the server and rejected; a `MaxDepth` pointer to `0` requests
seeds only. Inspect `Truncated` before using the returned context downstream.

The server checks vector and graph permissions. Requests use the client's bearer
token and optional principal header; failures are `*multidb.APIError`, and context
cancellation/timeouts propagate normally.

Run the deterministic HTTP fixture tests without a server:

```bash
cd sdk/go
go test ./...
```

## Typed tensors on a GPU server

Connect to a server opened with the `tensor_dataset` profile and an actual native
GPU. That profile refuses to open without a supported GPU; the SDK does not run
local kernels or substitute CPU execution. Typed datasets can also be stored under
other profiles for reference CPU operations, but that is a different execution mode.
This API does not claim DeepLake compatibility.

Using the authenticated client and timeout context from above:

```go
func tensorExample(ctx context.Context, client *multidb.Client) error {
    dimension := uint64(2)
    config := multidb.TensorDatasetConfig{
        Name: "training",
        Tensors: []multidb.TensorColumn{
            {Name: "text", DType: multidb.TensorUTF8DType, Shape: []*uint64{}},
            {Name: "image", DType: multidb.TensorU8DType, Shape: []*uint64{nil}},
            {Name: "embedding", DType: multidb.TensorF32DType, Shape: []*uint64{&dimension}},
        },
    }
    // Create once; use TensorDatasetInfo to inspect an existing dataset.
    if _, err := client.CreateTensorDataset(ctx, config); err != nil {
        return err
    }
    appended, err := client.AppendTensors(ctx, "training", multidb.TensorBatch{
        "text": {{Shape: []uint64{}, Data: multidb.TensorData{UTF8: []string{"a caption"}}}},
        "image": {{Shape: []uint64{3}, Data: multidb.TensorData{U8: multidb.TensorU8{0, 128, 255}}}},
        "embedding": {{Shape: []uint64{2}, Data: multidb.TensorData{F32: []float32{1, 0}}}},
    })
    if err != nil {
        return err
    }
    limit := 1
    columns := []string{"text", "embedding"}
    snapshot, err := client.ReadTensors(ctx, "training", multidb.TensorReadRequest{
        Limit: &limit, Columns: &columns, Revision: &appended.Revision,
    })
    if err != nil {
        return err
    }
    found, err := client.SearchTensors(ctx, "training", multidb.TensorSearchRequest{
        Tensor: "embedding", Query: []float64{1, 0}, K: &limit, Metric: multidb.TensorMetricL2,
    })
    if err != nil {
        return err
    }
    reduced, err := client.ComputeTensors(ctx, "training", multidb.TensorComputeRequest{
        Tensor: "embedding", Operation: multidb.TensorGpuL2Norm, Limit: &limit,
    })
    if err != nil {
        return err
    }
    if found.Execution.Device != multidb.TensorDeviceGPU || reduced.Execution.Device != multidb.TensorDeviceGPU {
        return fmt.Errorf("this example requires a Tensor GPU server")
    }
    fmt.Printf("revision=%d hits=%v reduction=%v execution=%+v\n",
        snapshot.Revision, found.Hits, reduced.Values, reduced.Execution)
    return nil
}
```

Each column has the same number of samples in an append. The schema's empty shape
means a scalar; a nil dimension means ragged. Concrete sample shapes always contain
numbers. Set exactly one non-nil `TensorData` field (`U8`, `I64`, `F32`, `F64`,
`Bool`, `UTF8`). `TensorU8` sends numeric JSON arrays rather than Go's default base64
byte encoding. Payloads are typed at the API boundary and stored by the server as
native binary chunks.

Omitted chunk settings use 64 samples and 4194304 bytes. Read defaults are start 0
and limit 100; search defaults are k 10, cosine metric and at most 10000 scanned
samples. Pointers preserve explicit zero start/revision, while nil revision selects
the current dataset. `TensorDatasetInfo(ctx, name, &zero)` reads the initial empty
revision when `zero := uint64(0)`. `Execution` reports actual server device/adapter/
backend metadata. GPU failures retain the server's `APIError`, including HTTP 503
`tensor_gpu_unavailable`; the client does not retry with a CPU request.
