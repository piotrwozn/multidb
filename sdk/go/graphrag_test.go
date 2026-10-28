package multidb

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGraphRAGMethodsPreserveAuthenticationPathsAndTypedProvenance(t *testing.T) {
	type call struct {
		method, path string
		body         JSONObject
	}
	calls := make(chan call, 2)
	const first = "00000000000000000000000000000001"
	const second = "00000000000000000000000000000002"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" || r.Header.Get("x-multidb-principal") != "alice" {
			t.Error("GraphRAG request lost authentication/principal headers")
		}
		if r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Accept") != "application/json" {
			t.Error("GraphRAG request lost JSON headers")
		}
		var body JSONObject
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		calls <- call{r.Method, r.URL.EscapedPath(), body}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/graphrag/query" {
			_ = json.NewEncoder(w).Encode(JSONObject{"ok": true, "data": JSONObject{
				"context": "[1] First\n[2] Second", "truncated": true,
				"sources": []JSONObject{
					{"document_id": first, "text": "First", "source": "notes.md", "seed_distance": 0.25, "depth": 0, "seed_id": first},
					{"document_id": second, "text": "Second", "source": nil, "seed_distance": 0.25, "depth": 1, "seed_id": first},
				},
				"relationships": []JSONObject{{"source": first, "label": "related", "target": second}},
			}})
			return
		}
		_ = json.NewEncoder(w).Encode(JSONObject{"ok": true, "data": JSONObject{"written": true}})
	}))
	defer server.Close()
	client := NewClient(WithBaseURL(server.URL+"/api/"), WithToken("secret"), WithPrincipal("alice"))
	result, err := client.GraphRAGQuery(context.Background(), GraphRAGRequest{
		VectorCollection: "chunks", Graph: "knowledge", QueryVector: []float32{1, 0.5},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Truncated || len(result.Sources) != 2 || len(result.Relationships) != 1 {
		t.Fatalf("unexpected result: %#v", result)
	}
	if result.Sources[0].Source == nil || *result.Sources[0].Source != "notes.md" || result.Sources[1].Source != nil {
		t.Fatalf("source attribution was not preserved: %#v", result.Sources)
	}
	if result.Sources[1].SeedID != first || result.Sources[1].Depth != 1 || result.Sources[1].SeedDistance != 0.25 {
		t.Fatalf("seed provenance was not preserved: %#v", result.Sources[1])
	}
	query := <-calls
	if query.method != http.MethodPost || query.path != "/api/graphrag/query" {
		t.Fatalf("unexpected query transport: %#v", query)
	}
	if len(query.body) != 3 || query.body["vector_collection"] != "chunks" || query.body["graph"] != "knowledge" {
		t.Fatalf("optional defaults should be omitted: %#v", query.body)
	}
	written, err := client.AddGraphEdge(context.Background(), "knowledge /?#%ą", GraphEdgeRequest{Source: first, Label: "related", Target: second})
	if err != nil || !written.Written {
		t.Fatalf("edge write failed: %#v, %v", written, err)
	}
	edge := <-calls
	if edge.method != http.MethodPost || edge.path != "/api/data/graphs/knowledge%20%2F%3F%23%25%C4%85/edges" {
		t.Fatalf("graph name was not one encoded path segment: %#v", edge)
	}
	if edge.body["source"] != first || edge.body["target"] != second || edge.body["label"] != "related" {
		t.Fatalf("unexpected edge request: %#v", edge.body)
	}
	if props, present := edge.body["props"]; !present || props != nil {
		t.Fatalf("nil edge properties should be JSON null: %#v", edge.body)
	}
}

func TestGraphRAGOptionsPreserveZeroDepthAndExplicitEmptyLists(t *testing.T) {
	zero, seeds, nodes, edges, contextBytes := 0, 3, 10, 20, 512
	empty := []string{}
	textPath, sourcePath := []string{"document", "text"}, []string{"document", "uri"}
	body, err := json.Marshal(GraphRAGRequest{
		VectorCollection: "chunks", Graph: "knowledge", QueryVector: []float32{1},
		SeedK: &seeds, MaxDepth: &zero, MaxNodes: &nodes, MaxEdges: &edges, MaxContextBytes: &contextBytes,
		Labels: &empty, TextField: &textPath, SourceField: &sourcePath,
	})
	if err != nil {
		t.Fatal(err)
	}
	var payload JSONObject
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["max_depth"] != float64(0) || len(payload["labels"].([]any)) != 0 {
		t.Fatalf("explicit options were replaced with defaults: %s", body)
	}
	if len(payload) != 11 || payload["seed_k"] != float64(3) || payload["max_context_bytes"] != float64(512) {
		t.Fatalf("missing options: %s", body)
	}
}

func TestGraphRAGAndGraphEdgeExposeAuthorizationAndValidationErrors(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusBadRequest} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				_ = json.NewEncoder(w).Encode(JSONObject{"ok": false, "error": JSONObject{"code": "request_denied", "message": "details"}})
			}))
			defer server.Close()
			client := NewClient(WithBaseURL(server.URL))
			_, queryErr := client.GraphRAGQuery(context.Background(), GraphRAGRequest{})
			_, edgeErr := client.AddGraphEdge(context.Background(), "graph", GraphEdgeRequest{})
			for _, err := range []error{queryErr, edgeErr} {
				var apiErr *APIError
				if !errors.As(err, &apiErr) || apiErr.Status != status || apiErr.Code != "request_denied" || apiErr.Message != "details" {
					t.Fatalf("error lost server details: %#v", err)
				}
			}
		})
	}
}

func TestGraphRAGHonorsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := NewClient(WithBaseURL("http://127.0.0.1:1/api"))
	_, queryErr := client.GraphRAGQuery(ctx, GraphRAGRequest{})
	_, edgeErr := client.AddGraphEdge(ctx, "graph", GraphEdgeRequest{})
	for _, err := range []error{queryErr, edgeErr} {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled context was ignored: %v", err)
		}
	}
}
