package multidb

import (
	"context"
	"net/http"
)

// GraphRAGRequest retrieves vector seeds and expands their graph relationships.
// Nil optional fields use server defaults: 8 seeds, depth 2, 64 nodes, 256
// edges, 16384 context bytes, labels ["related"], text field ["text"] and
// source field ["source"]. Pointers preserve explicit zero depth and empty
// lists, which the server validates instead of silently replacing with defaults.
type GraphRAGRequest struct {
	VectorCollection string    `json:"vector_collection"`
	Graph            string    `json:"graph"`
	QueryVector      []float32 `json:"query_vector"`
	SeedK            *int      `json:"seed_k,omitempty"`
	MaxDepth         *int      `json:"max_depth,omitempty"`
	MaxNodes         *int      `json:"max_nodes,omitempty"`
	MaxEdges         *int      `json:"max_edges,omitempty"`
	MaxContextBytes  *int      `json:"max_context_bytes,omitempty"`
	Labels           *[]string `json:"labels,omitempty"`
	TextField        *[]string `json:"text_field,omitempty"`
	SourceField      *[]string `json:"source_field,omitempty"`
}

// GraphRAGSource attributes one retrieved document to its vector seed.
type GraphRAGSource struct {
	DocumentID   string  `json:"document_id"`
	Text         string  `json:"text"`
	Source       *string `json:"source"`
	SeedDistance float32 `json:"seed_distance"`
	Depth        int     `json:"depth"`
	SeedID       string  `json:"seed_id"`
}

type GraphRAGRelationship struct {
	Source string `json:"source"`
	Label  string `json:"label"`
	Target string `json:"target"`
}

// GraphRAGResult contains retrieval context and provenance, without calling an LLM.
type GraphRAGResult struct {
	Context       string                 `json:"context"`
	Sources       []GraphRAGSource       `json:"sources"`
	Relationships []GraphRAGRelationship `json:"relationships"`
	Truncated     bool                   `json:"truncated"`
}

// GraphEdgeRequest connects vector document IDs in a graph. For GraphRAG,
// Source and Target are the lowercase 32-character hexadecimal IDs returned by
// InsertVector. Nil Props is sent as JSON null.
type GraphEdgeRequest struct {
	Source string    `json:"source"`
	Label  string    `json:"label"`
	Target string    `json:"target"`
	Props  JSONValue `json:"props"`
}

type GraphEdgeResponse struct {
	Written bool `json:"written"`
}

// GraphRAGQuery retrieves a bounded context using the request's vector collection
// and graph. The caller supplies embeddings; no embedding or LLM service is invoked.
func (client *Client) GraphRAGQuery(ctx context.Context, body GraphRAGRequest) (GraphRAGResult, error) {
	return request[GraphRAGResult](client, ctx, http.MethodPost, "/graphrag/query", body, true)
}

// AddGraphEdge stores one directed, labeled graph edge.
func (client *Client) AddGraphEdge(ctx context.Context, graph string, body GraphEdgeRequest) (GraphEdgeResponse, error) {
	return request[GraphEdgeResponse](client, ctx, http.MethodPost, "/data/graphs/"+pathEscape(graph)+"/edges", body, true)
}
