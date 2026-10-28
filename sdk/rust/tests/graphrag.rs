use std::sync::{Arc, Mutex};

use multidb_client::{
    ControlPlaneClient, ControlPlaneError, GraphEdgeRequest, GraphRagRequest, HttpRequest,
    HttpResponse, Transport,
};
use serde_json::{Value, json};

const FIRST: &str = "00000000000000000000000000000001";
const SECOND: &str = "00000000000000000000000000000002";

#[derive(Default)]
struct RecordingTransport {
    calls: Mutex<Vec<HttpRequest>>,
}

impl Transport for RecordingTransport {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, ControlPlaneError> {
        let query = request.url.ends_with("/graphrag/query");
        self.calls.lock().unwrap().push(request);
        let data = if query {
            json!({
                "context": "[1] First\n[2] Second",
                "sources": [
                    {"document_id": FIRST, "text": "First", "source": "notes.md", "seed_distance": 0.25, "depth": 0, "seed_id": FIRST},
                    {"document_id": SECOND, "text": "Second", "source": null, "seed_distance": 0.25, "depth": 1, "seed_id": FIRST}
                ],
                "relationships": [{"source": FIRST, "label": "related", "target": SECOND}],
                "truncated": true
            })
        } else {
            json!({"written": true})
        };
        Ok(HttpResponse {
            status: 200,
            body: serde_json::to_vec(&json!({"ok": true, "data": data})).unwrap(),
        })
    }
}

#[test]
fn graph_rag_transport_preserves_headers_paths_and_typed_provenance() {
    let transport = Arc::new(RecordingTransport::default());
    let client = ControlPlaneClient::with_transport("http://unit.test/api/", transport.clone())
        .with_token("secret")
        .with_principal("alice");
    let result = client
        .graph_rag_query(&GraphRagRequest::new("chunks", "knowledge", vec![1.0, 0.5]))
        .unwrap();
    assert_eq!(result.context, "[1] First\n[2] Second");
    assert!(result.truncated);
    assert_eq!(result.sources.len(), 2);
    assert_eq!(result.sources[0].source.as_deref(), Some("notes.md"));
    assert_eq!(result.sources[1].source, None);
    assert_eq!(result.sources[1].document_id, SECOND);
    assert_eq!(result.sources[1].seed_id, FIRST);
    assert_eq!(result.sources[1].seed_distance, 0.25);
    assert_eq!(result.sources[1].depth, 1);
    assert_eq!(result.relationships[0].source, FIRST);
    assert_eq!(result.relationships[0].target, SECOND);
    assert_eq!(result.relationships[0].label, "related");
    assert!(
        client
            .add_graph_edge(
                "knowledge /?#%ą",
                &GraphEdgeRequest::new(FIRST, "related", SECOND)
            )
            .unwrap()
            .written
    );

    let calls = transport.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    for call in calls.iter() {
        assert_eq!(call.method, "POST");
        for (name, value) in [
            ("Authorization", "Bearer secret"),
            ("x-multidb-principal", "alice"),
            ("Content-Type", "application/json"),
            ("Accept", "application/json"),
        ] {
            assert!(
                call.headers
                    .iter()
                    .any(|(key, found)| key == name && found == value),
                "missing {name}"
            );
        }
    }
    assert_eq!(calls[0].url, "http://unit.test/api/graphrag/query");
    assert_eq!(
        serde_json::from_slice::<Value>(&calls[0].body).unwrap(),
        json!({"vector_collection": "chunks", "graph": "knowledge", "query_vector": [1.0, 0.5]})
    );
    assert_eq!(
        calls[1].url,
        "http://unit.test/api/data/graphs/knowledge%20%2F%3F%23%25%C4%85/edges"
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&calls[1].body).unwrap(),
        json!({"source": FIRST, "label": "related", "target": SECOND, "props": null})
    );
}

#[test]
fn graph_rag_options_keep_explicit_zero_depth_and_empty_lists() {
    let transport = Arc::new(RecordingTransport::default());
    let client = ControlPlaneClient::with_transport("http://unit.test/api", transport.clone());
    let mut request = GraphRagRequest::new("chunks", "knowledge", vec![1.0]);
    request.seed_k = Some(3);
    request.max_depth = Some(0);
    request.max_nodes = Some(10);
    request.max_edges = Some(20);
    request.max_context_bytes = Some(512);
    request.labels = Some(vec![]);
    request.text_field = Some(vec!["document".into(), "text".into()]);
    request.source_field = Some(vec!["document".into(), "uri".into()]);
    client.graph_rag_query(&request).unwrap();
    let calls = transport.calls.lock().unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&calls[0].body).unwrap(),
        json!({
            "vector_collection": "chunks", "graph": "knowledge", "query_vector": [1.0],
            "seed_k": 3, "max_depth": 0, "max_nodes": 10, "max_edges": 20,
            "max_context_bytes": 512, "labels": [],
            "text_field": ["document", "text"], "source_field": ["document", "uri"]
        })
    );
}

#[test]
fn graph_rag_and_edges_preserve_authorization_and_validation_errors() {
    struct DeniedTransport(u16);
    impl Transport for DeniedTransport {
        fn send(&self, _: HttpRequest) -> Result<HttpResponse, ControlPlaneError> {
            Ok(HttpResponse {
                status: self.0,
                body: br#"{"ok":false,"error":{"code":"request_denied","message":"details"}}"#
                    .to_vec(),
            })
        }
    }
    for status in [400, 401, 403] {
        let client = ControlPlaneClient::with_transport(
            "http://unit.test/api",
            Arc::new(DeniedTransport(status)),
        );
        let errors = [
            client
                .graph_rag_query(&GraphRagRequest::new("chunks", "knowledge", vec![1.0]))
                .unwrap_err(),
            client
                .add_graph_edge(
                    "knowledge",
                    &GraphEdgeRequest::new(FIRST, "related", SECOND),
                )
                .unwrap_err(),
        ];
        for error in errors {
            assert_eq!(error.status, status);
            assert_eq!(error.code, "request_denied");
            assert_eq!(error.message, "details");
            assert!(!error.body.is_empty());
        }
    }
}

#[test]
fn graph_rag_reports_invalid_response_data_instead_of_silently_losing_it() {
    struct InvalidDataTransport;
    impl Transport for InvalidDataTransport {
        fn send(&self, _: HttpRequest) -> Result<HttpResponse, ControlPlaneError> {
            Ok(HttpResponse {
                status: 200,
                body: br#"{"ok":true,"data":{"context":"text","sources":{},"relationships":[],"truncated":false}}"#
                    .to_vec(),
            })
        }
    }
    let client =
        ControlPlaneClient::with_transport("http://unit.test/api", Arc::new(InvalidDataTransport));
    let error = client
        .graph_rag_query(&GraphRagRequest::new("chunks", "knowledge", vec![1.0]))
        .unwrap_err();
    assert_eq!(error.code, "invalid_json");
    assert_eq!(error.status, 200);
}
