//! Local retrieval of vector seeds and graph-linked evidence, without an LLM dependency.
//!
//! Graph nodes use the canonical lowercase, 32-character hexadecimal vector document id.
//! Visibility is decided on stored vector metadata before either output or traversal. Missing,
//! hidden and textless documents form traversal barriers. Graph properties are never returned.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::{
    graph::{Graph, GraphError, GraphNodeId},
    model::{DocumentId, FieldPath, Value, extract_path},
    repl::Replication,
    vector::{VectorCollection, VectorError},
};

const MAX_SOURCE_BYTES: usize = 1_024;
const MAX_QUERY_DIMENSION: usize = 16_384;

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphRagRequest {
    pub vector_collection: String,
    pub graph: String,
    pub query_vector: Vec<f32>,
    #[serde(default = "default_seed_k")]
    pub seed_k: usize,
    #[serde(default = "default_max_depth")]
    pub max_depth: usize,
    #[serde(default = "default_max_nodes")]
    pub max_nodes: usize,
    #[serde(default = "default_max_edges")]
    pub max_edges: usize,
    #[serde(default = "default_context_bytes")]
    pub max_context_bytes: usize,
    #[serde(default = "default_labels")]
    pub labels: Vec<String>,
    #[serde(default = "default_text_field")]
    pub text_field: Vec<String>,
    #[serde(default = "default_source_field")]
    pub source_field: Vec<String>,
}

impl Default for GraphRagRequest {
    fn default() -> Self {
        Self {
            vector_collection: String::new(),
            graph: String::new(),
            query_vector: Vec::new(),
            seed_k: default_seed_k(),
            max_depth: default_max_depth(),
            max_nodes: default_max_nodes(),
            max_edges: default_max_edges(),
            max_context_bytes: default_context_bytes(),
            labels: default_labels(),
            text_field: default_text_field(),
            source_field: default_source_field(),
        }
    }
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
            ..Self::default()
        }
    }

    /// Validates request limits before any vector or graph reads.
    /// # Errors
    /// Fails for invalid names, paths, vectors, or resource limits.
    pub fn validate(&self) -> Result<(), GraphRagError> {
        validate_name(&self.vector_collection, "vector_collection", 256)?;
        validate_name(&self.graph, "graph", 256)?;
        if self.query_vector.is_empty()
            || self.query_vector.len() > MAX_QUERY_DIMENSION
            || self.query_vector.iter().any(|value| !value.is_finite())
        {
            return invalid("query_vector must contain 1 to 16384 finite numbers");
        }
        if !(1..=64).contains(&self.seed_k) || self.seed_k > self.max_nodes {
            return invalid("seed_k must be between 1 and 64 and no greater than max_nodes");
        }
        if self.max_depth > 8 {
            return invalid("max_depth must be between 0 and 8");
        }
        if !(1..=1_024).contains(&self.max_nodes) {
            return invalid("max_nodes must be between 1 and 1024");
        }
        if !(1..=10_000).contains(&self.max_edges) {
            return invalid("max_edges must be between 1 and 10000");
        }
        if !(1..=1_048_576).contains(&self.max_context_bytes) {
            return invalid("max_context_bytes must be between 1 and 1048576");
        }
        if !(1..=16).contains(&self.labels.len()) {
            return invalid("labels must contain between 1 and 16 labels");
        }
        for label in &self.labels {
            validate_name(label, "label", 128)?;
        }
        validate_path(&self.text_field, "text_field")?;
        validate_path(&self.source_field, "source_field")?;
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphRagResult {
    pub context: String,
    pub sources: Vec<GraphRagSource>,
    pub relationships: Vec<GraphRagRelation>,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphRagSource {
    pub document_id: String,
    pub text: String,
    pub source: Option<String>,
    pub seed_distance: f32,
    pub depth: usize,
    pub seed_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct GraphRagRelation {
    pub source: String,
    pub label: String,
    pub target: String,
}

#[derive(thiserror::Error, Debug)]
pub enum GraphRagError {
    #[error("invalid GraphRAG request: {0}")]
    InvalidRequest(String),
    #[error("vector: {0}")]
    Vector(#[from] VectorError),
    #[error("graph: {0}")]
    Graph(#[from] GraphError),
    #[error("GraphRAG policy: {0}")]
    Policy(String),
}

/// Converts a vector document id to its canonical graph node id.
#[must_use]
pub fn document_node_id(id: DocumentId) -> GraphNodeId {
    GraphNodeId::from_str_id(&document_id_text(id))
}

/// Parses only the canonical lowercase 32-hex-character graph node id.
/// # Errors
/// Fails for noncanonical graph ids, including uppercase, UUID hyphens or binary ids.
pub fn parse_document_node_id(node: &GraphNodeId) -> Result<DocumentId, GraphRagError> {
    let text = node.as_bytes();
    if text.len() != 32 {
        return invalid("graph document ids must be 32 lowercase hexadecimal characters");
    }
    let mut bytes = [0_u8; 16];
    for (index, pair) in text.as_chunks::<2>().0.iter().enumerate() {
        let high = hex_digit(pair[0])?;
        let low = hex_digit(pair[1])?;
        bytes[index] = high * 16 + low;
    }
    Ok(DocumentId::from_bytes(bytes))
}

/// Retrieves vector seeds, then graph-linked evidence in deterministic breadth-first order.
///
/// `max_nodes` counts every distinct metadata lookup, including hidden and missing nodes;
/// `max_edges` counts every scanned edge, including cycles and invalid document ids. The
/// visibility callback receives stored metadata before text extraction, and its returned
/// value is the only metadata used for output. A rejected node is never expanded.
///
/// Context uses numbered citations, remains valid UTF-8 and never exceeds its byte budget.
/// Returned text is clipped to the same citation text; source metadata is separately capped
/// at 1024 bytes. Truncation is conservative when an edge scan exactly fills its budget.
/// Zero depth performs vector-only retrieval. Graph nodes with noncanonical ids are skipped.
/// # Errors
/// Fails for invalid requests, vector/graph read failures, or callback policy errors.
pub fn retrieve<R, F>(
    vectors: &VectorCollection<'_, R>,
    graph: &Graph<'_, R>,
    request: &GraphRagRequest,
    visible_metadata: F,
) -> Result<GraphRagResult, GraphRagError>
where
    R: Replication + ?Sized,
    F: FnMut(DocumentId, Value) -> Result<Option<Value>, GraphRagError>,
{
    request.validate()?;
    let mut candidates = vectors.knn_bounded(&request.query_vector, request.max_nodes)?;
    candidates.sort_by(|left, right| {
        left.distance
            .total_cmp(&right.distance)
            .then_with(|| left.id.cmp(&right.id))
    });
    candidates.dedup_by_key(|hit| hit.id);
    let candidate_cap_reached = candidates.len() == request.max_nodes;
    let labels = request.labels.iter().cloned().collect::<BTreeSet<_>>();
    let mut state = RetrievalState {
        request,
        visible_metadata,
        result: GraphRagResult::default(),
        visited: BTreeMap::new(),
        text_path: FieldPath::new(request.text_field.clone()),
        source_path: FieldPath::new(request.source_field.clone()),
    };
    let mut queue = VecDeque::new();
    let mut seed_count = 0;
    for hit in candidates {
        if !hit.distance.is_finite() {
            return invalid("query_vector produces a non-finite distance for this collection");
        }
        let seed_id = document_id_text(hit.id);
        match state.visit(vectors, hit.id, hit.distance, 0, &seed_id)? {
            Visit::Visible { index, fresh } => {
                if fresh {
                    queue.push_back((hit.id, index));
                    seed_count += 1;
                }
                if seed_count == request.seed_k {
                    break;
                }
            }
            Visit::Blocked => {}
            Visit::Limit => break,
        }
    }
    if candidate_cap_reached && seed_count < request.seed_k {
        state.result.truncated = true;
    }

    let mut examined_edges = 0;
    let mut relationships = BTreeSet::new();
    'walk: while let Some((node, source_index)) = queue.pop_front() {
        let source = &state.result.sources[source_index];
        let depth = source.depth;
        if depth >= request.max_depth {
            continue;
        }
        let source_id = source.document_id.clone();
        let seed_id = source.seed_id.clone();
        let seed_distance = source.seed_distance;
        for label in &labels {
            let remaining = request.max_edges.saturating_sub(examined_edges);
            if remaining == 0 {
                state.result.truncated = true;
                break 'walk;
            }
            let page = graph.neighbor_ids_bounded(&document_node_id(node), label, remaining)?;
            examined_edges += page.scanned_count;
            state.result.truncated |= page.truncated;
            for target_node in page.neighbors {
                let Ok(target) = parse_document_node_id(&target_node) else {
                    continue;
                };
                match state.visit(vectors, target, seed_distance, depth + 1, &seed_id)? {
                    Visit::Visible { index, fresh } => {
                        let target_id = state.result.sources[index].document_id.clone();
                        if relationships.insert((
                            source_id.clone(),
                            label.clone(),
                            target_id.clone(),
                        )) {
                            state.result.relationships.push(GraphRagRelation {
                                source: source_id.clone(),
                                label: label.clone(),
                                target: target_id,
                            });
                        }
                        if fresh {
                            queue.push_back((target, index));
                        }
                    }
                    Visit::Blocked => {}
                    Visit::Limit => break 'walk,
                }
            }
        }
    }
    Ok(state.result)
}

struct RetrievalState<'a, F> {
    request: &'a GraphRagRequest,
    visible_metadata: F,
    result: GraphRagResult,
    visited: BTreeMap<DocumentId, Option<usize>>,
    text_path: FieldPath,
    source_path: FieldPath,
}

enum Visit {
    Visible { index: usize, fresh: bool },
    Blocked,
    Limit,
}

impl<F> RetrievalState<'_, F>
where
    F: FnMut(DocumentId, Value) -> Result<Option<Value>, GraphRagError>,
{
    fn visit<R: Replication + ?Sized>(
        &mut self,
        vectors: &VectorCollection<'_, R>,
        id: DocumentId,
        seed_distance: f32,
        depth: usize,
        seed_id: &str,
    ) -> Result<Visit, GraphRagError> {
        if let Some(seen) = self.visited.get(&id) {
            return Ok(seen.map_or(Visit::Blocked, |index| Visit::Visible {
                index,
                fresh: false,
            }));
        }
        if self.visited.len() >= self.request.max_nodes {
            self.result.truncated = true;
            return Ok(Visit::Limit);
        }
        self.visited.insert(id, None);
        let Some(metadata) = vectors.get_metadata(id)? else {
            return Ok(Visit::Blocked);
        };
        let Some(metadata) = (self.visible_metadata)(id, metadata)? else {
            return Ok(Visit::Blocked);
        };
        let Some(Value::Str(text)) = extract_path(&metadata, &self.text_path) else {
            return Ok(Visit::Blocked);
        };
        if text.trim().is_empty() {
            return Ok(Visit::Blocked);
        }
        let prefix = format!(
            "{}[{}] ",
            if self.result.context.is_empty() {
                ""
            } else {
                "\n\n"
            },
            self.result.sources.len() + 1,
        );
        let remaining = self
            .request
            .max_context_bytes
            .saturating_sub(self.result.context.len());
        let clipped = utf8_prefix(text, remaining.saturating_sub(prefix.len()));
        if prefix.len() >= remaining || clipped.is_empty() {
            self.result.truncated = true;
            return Ok(Visit::Limit);
        }
        self.result.truncated |= clipped.len() != text.len();
        let source = match extract_path(&metadata, &self.source_path) {
            Some(Value::Str(value)) if !value.is_empty() => {
                let clipped = utf8_prefix(value, MAX_SOURCE_BYTES);
                self.result.truncated |= clipped.len() != value.len();
                Some(clipped.to_owned())
            }
            _ => None,
        };
        self.result.context.push_str(&prefix);
        self.result.context.push_str(clipped);
        let index = self.result.sources.len();
        self.result.sources.push(GraphRagSource {
            document_id: document_id_text(id),
            text: clipped.to_owned(),
            source,
            seed_distance,
            depth,
            seed_id: seed_id.to_owned(),
        });
        self.visited.insert(id, Some(index));
        Ok(Visit::Visible { index, fresh: true })
    }
}

fn document_id_text(id: DocumentId) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    id.as_bytes()
        .into_iter()
        .flat_map(|byte| {
            [
                char::from(HEX[usize::from(byte >> 4)]),
                char::from(HEX[usize::from(byte & 15)]),
            ]
        })
        .collect()
}

fn hex_digit(byte: u8) -> Result<u8, GraphRagError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => invalid("graph document ids must be lowercase hexadecimal"),
    }
}

fn utf8_prefix(text: &str, max_bytes: usize) -> &str {
    let mut end = max_bytes.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn validate_name(value: &str, field: &str, max_bytes: usize) -> Result<(), GraphRagError> {
    if value.trim().is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return invalid(format!(
            "{field} must be nonempty, at most {max_bytes} bytes and contain no control characters"
        ));
    }
    Ok(())
}

fn validate_path(path: &[String], field: &str) -> Result<(), GraphRagError> {
    if !(1..=16).contains(&path.len()) {
        return invalid(format!(
            "{field} must contain between 1 and 16 path segments"
        ));
    }
    for segment in path {
        validate_name(segment, field, 128)?;
    }
    Ok(())
}

fn invalid<T>(message: impl Into<String>) -> Result<T, GraphRagError> {
    Err(GraphRagError::InvalidRequest(message.into()))
}

const fn default_seed_k() -> usize {
    8
}
const fn default_max_depth() -> usize {
    2
}
const fn default_max_nodes() -> usize {
    64
}
const fn default_max_edges() -> usize {
    256
}
const fn default_context_bytes() -> usize {
    16_384
}
fn default_labels() -> Vec<String> {
    vec!["related".to_owned()]
}
fn default_text_field() -> Vec<String> {
    vec!["text".to_owned()]
}
fn default_source_field() -> Vec<String> {
    vec!["source".to_owned()]
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, collections::BTreeMap};

    use super::*;
    use crate::{
        db::{Database, DbConfig, Profile, create_database, open_database},
        graph::GraphId,
        model::CollectionId,
        vector::{HnswParams, VectorCollectionConfig, VectorMetric},
    };

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn config() -> VectorCollectionConfig {
        VectorCollectionConfig::new(CollectionId::new(7), 2)
            .with_metric(VectorMetric::L2)
            .with_hnsw(HnswParams::new(8, 32, 32))
    }

    fn metadata(text: &str) -> Value {
        Value::Object(BTreeMap::from([
            ("text".to_owned(), Value::Str(text.to_owned())),
            ("source".to_owned(), Value::Str("local-notes".to_owned())),
        ]))
    }

    fn request() -> GraphRagRequest {
        let mut request = GraphRagRequest::new("evidence", "links", vec![0.0, 0.0]);
        request.seed_k = 1;
        request.max_depth = 8;
        request
    }

    fn populate(
        vectors: &VectorCollection<'_, Database>,
        graph: &Graph<'_, Database>,
    ) -> Result<[DocumentId; 4], Box<dyn std::error::Error>> {
        let seed = vectors.insert_vector(&metadata("nearest seed"), vec![0.0, 0.0])?;
        let decoy = vectors.insert_vector(&metadata("nearest but unrelated"), vec![0.2, 0.0])?;
        let bridge = vectors.insert_vector(&metadata("linked evidence"), vec![8.0, 0.0])?;
        let leaf = vectors.insert_vector(
            &metadata("non-nearest supporting evidence"),
            vec![20.0, 0.0],
        )?;
        for (source, target) in [(seed, bridge), (bridge, leaf), (leaf, seed)] {
            graph.add_edge(
                document_node_id(source),
                "related",
                document_node_id(target),
                &metadata("private edge properties"),
            )?;
        }
        Ok([seed, decoy, bridge, leaf])
    }

    #[test]
    fn graph_expansion_finds_non_nearest_evidence_and_deduplicates_cycles() -> TestResult {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let vectors = VectorCollection::new(&database, config())?;
        let graph = Graph::new(&database, GraphId::new(4));
        let [seed, decoy, bridge, leaf] = populate(&vectors, &graph)?;
        assert_eq!(
            vectors
                .knn(&[0.0, 0.0], 2)?
                .iter()
                .map(|hit| hit.id)
                .collect::<Vec<_>>(),
            vec![seed, decoy]
        );

        let mut request = request();
        request.labels = vec!["related".to_owned(), "related".to_owned()];
        let result = retrieve(&vectors, &graph, &request, |_, value| Ok(Some(value)))?;
        assert_eq!(
            result
                .sources
                .iter()
                .map(|source| source.document_id.clone())
                .collect::<Vec<_>>(),
            [seed, bridge, leaf].map(document_id_text)
        );
        assert_eq!(
            result
                .sources
                .iter()
                .map(|source| source.depth)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert!(
            result
                .sources
                .iter()
                .all(|source| source.seed_id == document_id_text(seed)
                    && source.seed_distance == 0.0)
        );
        assert_eq!(result.relationships.len(), 3);
        assert!(
            result
                .context
                .contains("[3] non-nearest supporting evidence")
        );
        assert!(!result.context.contains("private edge properties"));
        assert!(!result.truncated);
        assert_eq!(
            result,
            retrieve(&vectors, &graph, &request, |_, value| Ok(Some(value)))?
        );
        Ok(())
    }

    #[test]
    fn hidden_missing_and_textless_nodes_are_traversal_barriers() -> TestResult {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let vectors = VectorCollection::new(&database, config())?;
        let graph = Graph::new(&database, GraphId::new(4));
        let [seed, _, bridge, _] = populate(&vectors, &graph)?;
        let result = retrieve(&vectors, &graph, &request(), |id, value| {
            Ok((id != bridge).then_some(value))
        })?;
        assert_eq!(result.sources.len(), 1);
        assert_eq!(result.sources[0].document_id, document_id_text(seed));
        assert_eq!(result.relationships, Vec::new());

        let result = retrieve(&vectors, &graph, &request(), |id, value| {
            Ok(Some(if id == bridge { metadata("") } else { value }))
        })?;
        assert_eq!(result.sources.len(), 1);
        assert_eq!(result.relationships, Vec::new());

        let missing = DocumentId::from_bytes([99; 16]);
        graph.add_edge(
            document_node_id(seed),
            "missing",
            document_node_id(missing),
            &Value::Null,
        )?;
        graph.add_edge(
            document_node_id(missing),
            "missing",
            document_node_id(bridge),
            &Value::Null,
        )?;
        let mut only_missing = request();
        only_missing.labels = vec!["missing".to_owned()];
        let result = retrieve(&vectors, &graph, &only_missing, |_, value| Ok(Some(value)))?;
        assert_eq!(result.sources.len(), 1);
        assert_eq!(result.relationships, Vec::new());
        assert!(matches!(
            retrieve(&vectors, &graph, &request(), |_, _| Err(
                GraphRagError::Policy("refused".to_owned())
            )),
            Err(GraphRagError::Policy(_))
        ));
        Ok(())
    }

    #[test]
    fn node_edge_depth_and_context_budgets_are_global() -> TestResult {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let vectors = VectorCollection::new(&database, config())?;
        let graph = Graph::new(&database, GraphId::new(4));
        populate(&vectors, &graph)?;
        let mut bounded = request();
        bounded.max_nodes = 2;
        let calls = Cell::new(0);
        let result = retrieve(&vectors, &graph, &bounded, |_, value| {
            calls.set(calls.get() + 1);
            Ok(Some(value))
        })?;
        assert_eq!(result.sources.len(), 2);
        assert!(calls.get() <= 2);
        assert!(result.truncated);

        bounded = request();
        bounded.max_edges = 1;
        let result = retrieve(&vectors, &graph, &bounded, |_, value| Ok(Some(value)))?;
        assert_eq!(result.sources.len(), 2);
        assert_eq!(result.relationships.len(), 1);
        assert!(result.truncated);

        bounded = request();
        bounded.max_depth = 0;
        let result = retrieve(&vectors, &graph, &bounded, |_, value| Ok(Some(value)))?;
        assert_eq!(result.sources.len(), 1);
        assert_eq!(result.relationships, Vec::new());
        assert!(!result.truncated);

        for budget in [1, 4, 7, 19, 30] {
            bounded = request();
            bounded.max_context_bytes = budget;
            let result = retrieve(&vectors, &graph, &bounded, |_, value| Ok(Some(value)))?;
            assert!(result.context.len() <= budget);
            assert!(result.truncated);
        }
        Ok(())
    }

    #[test]
    fn hidden_seed_overfetch_and_hidden_nodes_consume_the_node_budget() -> TestResult {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let vectors = VectorCollection::new(&database, config())?;
        let graph = Graph::new(&database, GraphId::new(4));
        let [seed, decoy, _, _] = populate(&vectors, &graph)?;
        let result = retrieve(&vectors, &graph, &request(), |id, value| {
            Ok((id != seed).then_some(value))
        })?;
        assert_eq!(result.sources[0].document_id, document_id_text(decoy));

        let mut bounded = request();
        bounded.max_nodes = 1;
        let calls = Cell::new(0);
        let result = retrieve(&vectors, &graph, &bounded, |_, _| {
            calls.set(calls.get() + 1);
            Ok(None)
        })?;
        assert_eq!(result.sources, Vec::new());
        assert_eq!(calls.get(), 1);
        assert!(result.truncated);
        Ok(())
    }

    #[test]
    fn masked_nested_text_and_unicode_are_used_without_exceeding_byte_budget() -> TestResult {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let vectors = VectorCollection::new(&database, config())?;
        let graph = Graph::new(&database, GraphId::new(4));
        vectors.insert_vector(&metadata("secret original"), vec![0.0, 0.0])?;
        let mut request = request();
        request.max_context_bytes = 9;
        request.text_field = vec!["public".to_owned(), "text".to_owned()];
        let result = retrieve(&vectors, &graph, &request, |_, _| {
            Ok(Some(Value::Object(BTreeMap::from([
                ("public".to_owned(), metadata("😀żółw")),
                ("source".to_owned(), Value::Str("🌍".repeat(1_000))),
            ]))))
        })?;
        assert_eq!(result.context, "[1] 😀");
        assert_eq!(result.sources[0].text, "😀");
        assert_eq!(
            result.sources[0].source.as_ref().map(String::len),
            Some(1_024)
        );
        assert!(result.context.len() <= request.max_context_bytes);
        assert!(result.truncated);
        assert!(!result.context.contains("secret"));
        Ok(())
    }

    #[test]
    fn canonical_document_ids_round_trip_and_other_graph_ids_are_skipped() -> TestResult {
        let id = DocumentId::from_bytes([0xab; 16]);
        assert_eq!(
            document_node_id(id),
            GraphNodeId::from_str_id(&"ab".repeat(16))
        );
        assert_eq!(parse_document_node_id(&document_node_id(id))?, id);
        for invalid in [
            "ab".repeat(15),
            "AB".repeat(16),
            "g".repeat(32),
            "a-b".to_owned(),
        ] {
            assert!(parse_document_node_id(&GraphNodeId::from_str_id(&invalid)).is_err());
        }
        assert!(parse_document_node_id(&GraphNodeId::new(vec![0xab; 16])).is_err());

        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let vectors = VectorCollection::new(&database, config())?;
        let graph = Graph::new(&database, GraphId::new(4));
        let seed = vectors.insert_vector(&metadata("seed"), vec![0.0, 0.0])?;
        graph.add_edge(
            document_node_id(seed),
            "related",
            GraphNodeId::from_str_id("arbitrary-node"),
            &Value::Null,
        )?;
        let result = retrieve(&vectors, &graph, &request(), |_, value| Ok(Some(value)))?;
        assert_eq!(result.sources.len(), 1);
        assert_eq!(result.relationships, Vec::new());
        Ok(())
    }

    #[test]
    fn request_defaults_validation_and_unknown_fields() -> TestResult {
        let defaults: GraphRagRequest = serde_json::from_value(serde_json::json!({
            "vector_collection": "evidence", "graph": "links", "query_vector": [1.0, 0.0]
        }))?;
        defaults.validate()?;
        assert_eq!(defaults.seed_k, 8);
        assert_eq!(defaults.max_depth, 2);
        assert_eq!(defaults.max_context_bytes, 16_384);
        assert!(serde_json::from_value::<GraphRagRequest>(serde_json::json!({
            "vector_collection": "evidence", "graph": "links", "query_vector": [1.0, 0.0], "typo": true
        })).is_err());
        let mut cases = Vec::new();
        let mut bad = defaults.clone();
        bad.seed_k = 0;
        cases.push(bad);
        let mut bad = defaults.clone();
        bad.seed_k = 65;
        cases.push(bad);
        let mut bad = defaults.clone();
        bad.max_nodes = 1;
        cases.push(bad);
        let mut bad = defaults.clone();
        bad.max_nodes = 1_025;
        cases.push(bad);
        let mut bad = defaults.clone();
        bad.max_depth = 9;
        cases.push(bad);
        let mut bad = defaults.clone();
        bad.max_edges = 0;
        cases.push(bad);
        let mut bad = defaults.clone();
        bad.max_edges = 10_001;
        cases.push(bad);
        let mut bad = defaults.clone();
        bad.max_context_bytes = 0;
        cases.push(bad);
        let mut bad = defaults.clone();
        bad.max_context_bytes = 1_048_577;
        cases.push(bad);
        let mut bad = defaults.clone();
        bad.query_vector = vec![f32::NAN];
        cases.push(bad);
        let mut bad = defaults.clone();
        bad.query_vector = vec![1.0; 16_385];
        cases.push(bad);
        let mut bad = defaults.clone();
        bad.graph = "bad\nname".to_owned();
        cases.push(bad);
        let mut bad = defaults.clone();
        bad.labels = Vec::new();
        cases.push(bad);
        let mut bad = defaults.clone();
        bad.labels = vec!["related".to_owned(); 17];
        cases.push(bad);
        let mut bad = defaults.clone();
        bad.text_field = vec![String::new()];
        cases.push(bad);
        let mut bad = defaults;
        bad.source_field = vec!["a".to_owned(); 17];
        cases.push(bad);
        assert!(
            cases
                .into_iter()
                .all(|case| matches!(case.validate(), Err(GraphRagError::InvalidRequest(_))))
        );
        Ok(())
    }

    #[test]
    fn retrieval_rebuilds_vector_index_and_graph_from_persisted_data() -> TestResult {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("graphrag.redb");
        let [seed, _, bridge, leaf] = {
            let database = create_database(DbConfig::on_disk(Profile::Balanced, &path))?;
            let vectors = VectorCollection::new(&database, config())?;
            let graph = Graph::new(&database, GraphId::new(4));
            populate(&vectors, &graph)?
        };
        let database = open_database(DbConfig::on_disk(Profile::Balanced, &path))?;
        let vectors = VectorCollection::new(&database, config())?;
        let graph = Graph::new(&database, GraphId::new(4));
        let result = retrieve(&vectors, &graph, &request(), |_, value| Ok(Some(value)))?;
        assert_eq!(
            result
                .sources
                .iter()
                .map(|source| source.document_id.clone())
                .collect::<Vec<_>>(),
            [seed, bridge, leaf].map(document_id_text)
        );
        assert_eq!(result.relationships.len(), 3);
        Ok(())
    }
}
