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
