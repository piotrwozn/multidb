use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{ControlPlaneClient, ControlPlaneError, path_escape};

/// Vector seeds and graph expansion for bounded retrieval context.
///
/// Omitted knobs use server defaults: 8 seeds, depth 2, 64 nodes, 256 edges,
/// 16384 context bytes, labels `["related"]`, text path `["text"]` and source
/// path `["source"]`. `Some(0)` depth disables expansion; explicit empty lists
/// are sent unchanged and validated by the server.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GraphRagRequest {
    pub vector_collection: String,
    pub graph: String,
    pub query_vector: Vec<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed_k: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_depth: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_nodes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_edges: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_context_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub labels: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_field: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_field: Option<Vec<String>>,
}

impl GraphRagRequest {
    #[must_use]
    pub fn new(
        vector_collection: impl Into<String>,
        graph: impl Into<String>,
        query_vector: Vec<f32>,
    ) -> Self {
        Self {
            vector_collection: vector_collection.into(),
            graph: graph.into(),
            query_vector,
            seed_k: None,
            max_depth: None,
            max_nodes: None,
            max_edges: None,
            max_context_bytes: None,
            labels: None,
            text_field: None,
            source_field: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GraphRagSource {
    pub document_id: String,
    pub text: String,
    pub source: Option<String>,
    pub seed_distance: f32,
    pub depth: usize,
    pub seed_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphRagRelationship {
    pub source: String,
    pub label: String,
    pub target: String,
}

/// Retrieved context, its sources and graph edges. No LLM is called.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GraphRagResult {
    pub context: String,
    pub sources: Vec<GraphRagSource>,
    pub relationships: Vec<GraphRagRelationship>,
    pub truncated: bool,
}

/// A directed edge. GraphRAG nodes use the lowercase 32-hex-character document
/// IDs returned by vector insertion. Missing properties are JSON null.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GraphEdgeRequest {
    pub source: String,
    pub label: String,
    pub target: String,
    #[serde(default)]
    pub props: Value,
}

impl GraphEdgeRequest {
    #[must_use]
    pub fn new(
        source: impl Into<String>,
        label: impl Into<String>,
        target: impl Into<String>,
    ) -> Self {
        Self {
            source: source.into(),
            label: label.into(),
            target: target.into(),
            props: Value::Null,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphEdgeResponse {
    pub written: bool,
}

impl ControlPlaneClient {
    /// Retrieves bounded context from caller-supplied embeddings and graph edges.
    /// # Errors
    /// Fails when serialization, transport, authorization or server validation fails.
    pub fn graph_rag_query(
        &self,
        request: &GraphRagRequest,
    ) -> Result<GraphRagResult, ControlPlaneError> {
        self.request("POST", "/graphrag/query", Some(typed_body(request)?), true)
    }

    /// Stores a directed graph edge using an escaped graph-name path segment.
    /// # Errors
    /// Fails when serialization, transport, authorization or server validation fails.
    pub fn add_graph_edge(
        &self,
        graph: impl AsRef<str>,
        request: &GraphEdgeRequest,
    ) -> Result<GraphEdgeResponse, ControlPlaneError> {
        self.request(
            "POST",
            &format!("/data/graphs/{}/edges", path_escape(graph.as_ref())),
            Some(typed_body(request)?),
            true,
        )
    }
}

fn typed_body(body: &impl Serialize) -> Result<Value, ControlPlaneError> {
    serde_json::to_value(body).map_err(|error| ControlPlaneError {
        status: 0,
        code: "invalid_json".into(),
        message: error.to_string(),
        body: Vec::new(),
    })
}
