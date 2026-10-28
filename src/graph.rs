use std::collections::{BTreeSet, VecDeque};

use crate::{
    keyenc,
    model::{Value, decode_value, encode_value},
    repl::{Op, ReadConsistency, ReplError, Replication},
    storage::{Bytes, StorageError},
};

pub const GRAPH_OUT_EDGES_TABLE: &str = "graph_out_edges";
pub const GRAPH_IN_EDGES_TABLE: &str = "graph_in_edges";

const MAX_DEFAULT_EXPANSION: usize = 10_000;

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct GraphId(u32);

#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct GraphNodeId(Bytes);

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphEdge {
    pub src: GraphNodeId,
    pub label: String,
    pub dst: GraphNodeId,
    pub props: Value,
}

/// One bounded outgoing-edge scan in storage-key order.
#[derive(Clone, Debug, PartialEq)]
pub struct BoundedNeighbors {
    pub neighbors: Vec<GraphEdge>,
    /// All storage records examined, including malformed keys, before filtering/deduplication.
    pub scanned_count: usize,
    /// The scan reached its budget and may have omitted edges. This is conservative:
    /// exactly `max_edges` records, and a zero budget, also report truncation.
    pub truncated: bool,
}

/// A bounded projection of outgoing destinations without decoding edge properties.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundedNeighborIds {
    pub neighbors: Vec<GraphNodeId>,
    pub scanned_count: usize,
    /// Conservative when the scan exactly fills its budget, including zero.
    pub truncated: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraversalOptions {
    pub max_depth: usize,
    pub max_expansion: usize,
}

#[derive(thiserror::Error, Debug)]
pub enum GraphError {
    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("invalid graph input: {0}")]
    InvalidInput(String),

    #[error("traversal limit exceeded")]
    TraversalLimit,
}

pub struct Graph<'repl, R: Replication + ?Sized> {
    repl: &'repl R,
    graph_id: GraphId,
}

impl GraphId {
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

impl GraphNodeId {
    #[must_use]
    pub fn new(bytes: impl Into<Bytes>) -> Self {
        Self(bytes.into())
    }

    #[must_use]
    pub fn from_str_id(value: &str) -> Self {
        Self(value.as_bytes().to_vec())
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Default for TraversalOptions {
    fn default() -> Self {
        Self {
            max_depth: 3,
            max_expansion: MAX_DEFAULT_EXPANSION,
        }
    }
}

impl<'repl, R: Replication + ?Sized> Graph<'repl, R> {
    #[must_use]
    pub const fn new(repl: &'repl R, graph_id: GraphId) -> Self {
        Self { repl, graph_id }
    }

    /// Adds or replaces a directed edge and its mirrored incoming edge.
    /// # Errors
    /// Fails when replication or encoding fails.
    pub fn add_edge(
        &self,
        src: GraphNodeId,
        label: impl Into<String>,
        dst: GraphNodeId,
        props: &Value,
    ) -> Result<(), GraphError> {
        let label = label.into();
        validate_label(&label)?;
        let edge = GraphEdge {
            src,
            label,
            dst,
            props: props.clone(),
        };
        self.repl.propose_batch(vec![
            Op::Put {
                table: GRAPH_OUT_EDGES_TABLE.to_owned(),
                key: out_key(self.graph_id, &edge.src, &edge.label, &edge.dst),
                value: encode_value(&edge.props)?,
            },
            Op::Put {
                table: GRAPH_IN_EDGES_TABLE.to_owned(),
                key: in_key(self.graph_id, &edge.dst, &edge.label, &edge.src),
                value: Vec::new(),
            },
        ])?;
        Ok(())
    }

    /// Deletes a directed edge and its mirror. Missing edges are ignored.
    /// # Errors
    /// Fails when replication fails.
    pub fn delete_edge(
        &self,
        src: &GraphNodeId,
        label: &str,
        dst: &GraphNodeId,
    ) -> Result<(), GraphError> {
        self.repl.propose_batch(vec![
            Op::Delete {
                table: GRAPH_OUT_EDGES_TABLE.to_owned(),
                key: out_key(self.graph_id, src, label, dst),
            },
            Op::Delete {
                table: GRAPH_IN_EDGES_TABLE.to_owned(),
                key: in_key(self.graph_id, dst, label, src),
            },
        ])?;
        Ok(())
    }

    /// Lists outgoing neighbors for one label.
    /// # Errors
    /// Fails when storage cannot be scanned.
    pub fn neighbors(&self, src: &GraphNodeId, label: &str) -> Result<Vec<GraphEdge>, GraphError> {
        let prefix = out_prefix(self.graph_id, src, label);
        let end = keyenc::range_end(&prefix);
        let mut edges = Vec::new();
        for (key, value) in self.repl.range(
            GRAPH_OUT_EDGES_TABLE,
            &prefix,
            &end,
            ReadConsistency::Strong,
        )? {
            let Some(dst) = trailing_node(&key) else {
                continue;
            };
            edges.push(GraphEdge {
                src: src.clone(),
                label: label.to_owned(),
                dst,
                props: decode_value(&value)?,
            });
        }
        Ok(edges)
    }

    /// Reads at most `max_edges` outgoing edge records for one label.
    ///
    /// The backend scan stops at the budget, before applying any caller-side visibility
    /// filters or deduplication. A full budget conservatively reports truncation, without
    /// reading an extra record. A zero budget performs no storage reads.
    /// # Errors
    /// Fails when the label is empty, bounded scans are unsupported, storage cannot be
    /// scanned, or properties cannot decode.
    pub fn neighbors_bounded(
        &self,
        src: &GraphNodeId,
        label: &str,
        max_edges: usize,
    ) -> Result<BoundedNeighbors, GraphError> {
        validate_label(label)?;
        let mut result = BoundedNeighbors {
            neighbors: Vec::new(),
            scanned_count: 0,
            truncated: max_edges == 0,
        };
        if max_edges == 0 {
            return Ok(result);
        }
        if !self.repl.supports_bounded_scans() {
            return Err(ReplError::Unsupported(
                "bounded graph scans are not supported by this replication backend".to_owned(),
            )
            .into());
        }
        let prefix = out_prefix(self.graph_id, src, label);
        let end = keyenc::range_end(&prefix);
        self.repl.scan_range_batches(
            GRAPH_OUT_EDGES_TABLE,
            &prefix,
            &end,
            ReadConsistency::Strong,
            1,
            &|| false,
            &mut |batch| {
                for (key, value) in batch
                    .iter()
                    .take(max_edges.saturating_sub(result.scanned_count))
                {
                    result.scanned_count += 1;
                    let Some(dst) = trailing_node(key) else {
                        continue;
                    };
                    result.neighbors.push(GraphEdge {
                        src: src.clone(),
                        label: label.to_owned(),
                        dst,
                        props: decode_value(value)?,
                    });
                }
                result.truncated = result.scanned_count == max_edges;
                Ok(result.scanned_count < max_edges)
            },
        )?;
        Ok(result)
    }

    /// Reads at most `max_edges` outgoing destinations without retaining edge properties.
    ///
    /// Every storage record counts against the budget, including malformed keys. Properties
    /// are neither decoded nor returned, so graph-assisted retrieval holds only node ids.
    /// # Errors
    /// Fails when the label is empty, bounded scans are unsupported, or storage fails.
    pub fn neighbor_ids_bounded(
        &self,
        src: &GraphNodeId,
        label: &str,
        max_edges: usize,
    ) -> Result<BoundedNeighborIds, GraphError> {
        validate_label(label)?;
        let mut result = BoundedNeighborIds {
            neighbors: Vec::new(),
            scanned_count: 0,
            truncated: max_edges == 0,
        };
        if max_edges == 0 {
            return Ok(result);
        }
        if !self.repl.supports_bounded_scans() {
            return Err(ReplError::Unsupported(
                "bounded graph scans are not supported by this replication backend".to_owned(),
            )
            .into());
        }
        let prefix = out_prefix(self.graph_id, src, label);
        let end = keyenc::range_end(&prefix);
        self.repl.scan_range_batches(
            GRAPH_OUT_EDGES_TABLE,
            &prefix,
            &end,
            ReadConsistency::Strong,
            1,
            &|| false,
            &mut |batch| {
                for (key, _) in batch
                    .iter()
                    .take(max_edges.saturating_sub(result.scanned_count))
                {
                    result.scanned_count += 1;
                    if let Some(dst) = trailing_node(key) {
                        result.neighbors.push(dst);
                    }
                }
                result.truncated = result.scanned_count == max_edges;
                Ok(result.scanned_count < max_edges)
            },
        )?;
        Ok(result)
    }

    /// Traverses up to `max_depth` hops with deduplication.
    /// # Errors
    /// Fails when traversal limits are exceeded or storage fails.
    pub fn k_hop(
        &self,
        start: &GraphNodeId,
        label: &str,
        options: TraversalOptions,
    ) -> Result<Vec<GraphNodeId>, GraphError> {
        let mut visited = BTreeSet::new();
        let mut queue = VecDeque::new();
        let mut result = Vec::new();
        visited.insert(start.clone());
        queue.push_back((start.clone(), 0_usize));
        let mut expanded = 0_usize;

        while let Some((node, depth)) = queue.pop_front() {
            if depth >= options.max_depth {
                continue;
            }
            for edge in self.neighbors(&node, label)? {
                expanded += 1;
                if expanded > options.max_expansion {
                    return Err(GraphError::TraversalLimit);
                }
                if visited.insert(edge.dst.clone()) {
                    result.push(edge.dst.clone());
                    queue.push_back((edge.dst, depth + 1));
                }
            }
        }
        Ok(result)
    }

    /// Finds an unweighted shortest path with BFS.
    /// # Errors
    /// Fails when traversal limits are exceeded or storage fails.
    pub fn shortest_path(
        &self,
        start: &GraphNodeId,
        target: &GraphNodeId,
        label: &str,
        options: TraversalOptions,
    ) -> Result<Option<Vec<GraphNodeId>>, GraphError> {
        let mut visited = BTreeSet::new();
        let mut queue = VecDeque::new();
        visited.insert(start.clone());
        queue.push_back(vec![start.clone()]);
        let mut expanded = 0_usize;

        while let Some(path) = queue.pop_front() {
            let Some(node) = path.last() else {
                continue;
            };
            if node == target {
                return Ok(Some(path));
            }
            if path.len().saturating_sub(1) >= options.max_depth {
                continue;
            }
            for edge in self.neighbors(node, label)? {
                expanded += 1;
                if expanded > options.max_expansion {
                    return Err(GraphError::TraversalLimit);
                }
                if visited.insert(edge.dst.clone()) {
                    let mut next = path.clone();
                    next.push(edge.dst);
                    queue.push_back(next);
                }
            }
        }
        Ok(None)
    }
}

fn validate_label(label: &str) -> Result<(), GraphError> {
    if label.is_empty() {
        return Err(GraphError::InvalidInput(
            "edge label cannot be empty".to_owned(),
        ));
    }
    Ok(())
}

fn out_prefix(graph_id: GraphId, src: &GraphNodeId, label: &str) -> Bytes {
    let mut key = Vec::new();
    key.extend_from_slice(&graph_id.as_u32().to_be_bytes());
    keyenc::push_len_bytes(&mut key, src.as_bytes());
    keyenc::push_len_bytes(&mut key, label.as_bytes());
    key
}

fn out_key(graph_id: GraphId, src: &GraphNodeId, label: &str, dst: &GraphNodeId) -> Bytes {
    let mut key = out_prefix(graph_id, src, label);
    keyenc::push_len_bytes(&mut key, dst.as_bytes());
    key
}

fn in_key(graph_id: GraphId, dst: &GraphNodeId, label: &str, src: &GraphNodeId) -> Bytes {
    let mut key = Vec::new();
    key.extend_from_slice(&graph_id.as_u32().to_be_bytes());
    keyenc::push_len_bytes(&mut key, dst.as_bytes());
    keyenc::push_len_bytes(&mut key, label.as_bytes());
    keyenc::push_len_bytes(&mut key, src.as_bytes());
    key
}

fn trailing_node(key: &[u8]) -> Option<GraphNodeId> {
    let mut cursor = 4_usize;
    skip_len_bytes(key, &mut cursor)?;
    skip_len_bytes(key, &mut cursor)?;
    let bytes = read_len_bytes(key, &mut cursor)?;
    Some(GraphNodeId::new(bytes.to_vec()))
}

fn read_len_bytes<'a>(key: &'a [u8], cursor: &mut usize) -> Option<&'a [u8]> {
    keyenc::read_len_bytes(key, cursor)
}

fn skip_len_bytes(key: &[u8], cursor: &mut usize) -> Option<()> {
    read_len_bytes(key, cursor).map(|_| ())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use crate::{
        db::{DbConfig, Profile, create_database},
        model::Value,
        repl::{Op, ReadConsistency, ReplError, Replication},
        storage::Bytes,
        txn::WriteAuthorization,
    };

    use super::{Graph, GraphId, GraphNodeId, TraversalOptions};

    /// Rejects unbounded range reads and counts records actually delivered by the
    /// real storage-backed batched scanner, including records the caller cannot use.
    struct ScanOnlyRepl {
        inner: Arc<dyn Replication>,
        scanned: AtomicUsize,
        calls: AtomicUsize,
    }

    impl Replication for ScanOnlyRepl {
        fn supports_bounded_scans(&self) -> bool {
            true
        }

        fn propose(&self, op: Op) -> Result<(), ReplError> {
            self.inner.propose(op)
        }

        fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
            self.inner.propose_batch(ops)
        }

        fn propose_authorized_batch(
            &self,
            ops: Vec<Op>,
            authorization: WriteAuthorization,
        ) -> Result<(), ReplError> {
            self.inner.propose_authorized_batch(ops, authorization)
        }

        fn read(
            &self,
            table: &str,
            key: &[u8],
            consistency: ReadConsistency,
        ) -> Result<Option<Bytes>, ReplError> {
            self.inner.read(table, key, consistency)
        }

        fn range(
            &self,
            _table: &str,
            _start: &[u8],
            _end: &[u8],
            _consistency: ReadConsistency,
        ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
            Err(ReplError::Unsupported("unbounded graph scan".to_owned()))
        }

        #[allow(clippy::too_many_arguments, clippy::type_complexity)]
        fn scan_range_batches(
            &self,
            table: &str,
            start: &[u8],
            end: &[u8],
            consistency: ReadConsistency,
            batch_rows: usize,
            cancelled: &dyn Fn() -> bool,
            on_batch: &mut dyn FnMut(&[(Bytes, Bytes)]) -> Result<bool, ReplError>,
        ) -> Result<(), ReplError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.scan_range_batches(
                table,
                start,
                end,
                consistency,
                batch_rows,
                cancelled,
                &mut |batch| {
                    self.scanned.fetch_add(batch.len(), Ordering::SeqCst);
                    on_batch(batch)
                },
            )
        }
    }

    /// Deliberately inherits the legacy eager scan default and capability=false.
    struct UnboundedRepl {
        inner: Arc<dyn Replication>,
        range_calls: AtomicUsize,
    }

    impl Replication for UnboundedRepl {
        fn propose(&self, op: Op) -> Result<(), ReplError> {
            self.inner.propose(op)
        }

        fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
            self.inner.propose_batch(ops)
        }

        fn propose_authorized_batch(
            &self,
            ops: Vec<Op>,
            authorization: WriteAuthorization,
        ) -> Result<(), ReplError> {
            self.inner.propose_authorized_batch(ops, authorization)
        }

        fn read(
            &self,
            table: &str,
            key: &[u8],
            consistency: ReadConsistency,
        ) -> Result<Option<Bytes>, ReplError> {
            self.inner.read(table, key, consistency)
        }

        fn range(
            &self,
            table: &str,
            start: &[u8],
            end: &[u8],
            consistency: ReadConsistency,
        ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
            self.range_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.range(table, start, end, consistency)
        }
    }

    #[test]
    fn bounded_neighbors_refuse_eager_backend_before_reading_any_edges()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let repl = UnboundedRepl {
            inner: Arc::new(database),
            range_calls: AtomicUsize::new(0),
        };
        let graph = Graph::new(&repl, GraphId::new(1));
        let node = GraphNodeId::from_str_id("root");
        graph.add_edge(
            node.clone(),
            "knows",
            GraphNodeId::from_str_id("a"),
            &Value::Null,
        )?;
        assert!(matches!(
            graph.neighbors_bounded(&node, "knows", 1),
            Err(super::GraphError::Repl(ReplError::Unsupported(_)))
        ));
        assert_eq!(repl.range_calls.load(Ordering::SeqCst), 0);
        // No scan is needed for zero budget, even on an eager backend.
        assert_eq!(graph.neighbors_bounded(&node, "knows", 0)?.scanned_count, 0);
        assert_eq!(repl.range_calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[test]
    fn bounded_neighbors_stop_storage_scan_at_the_examined_edge_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let repl = ScanOnlyRepl {
            inner: Arc::new(database),
            scanned: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
        };
        let graph = Graph::new(&repl, GraphId::new(1));
        let node = GraphNodeId::from_str_id("root");
        for id in (0..100).rev() {
            graph.add_edge(
                node.clone(),
                "knows",
                GraphNodeId::from_str_id(&format!("n{id:03}")),
                &Value::Int(id),
            )?;
        }
        graph.add_edge(
            node.clone(),
            "other",
            GraphNodeId::from_str_id("ignored"),
            &Value::Null,
        )?;
        let result = graph.neighbors_bounded(&node, "knows", 7)?;
        assert_eq!(result.scanned_count, 7);
        assert!(result.truncated);
        assert_eq!(repl.scanned.load(Ordering::SeqCst), 7);
        assert_eq!(
            result
                .neighbors
                .iter()
                .map(|e| e.dst.clone())
                .collect::<Vec<_>>(),
            (0..7)
                .map(|id| GraphNodeId::from_str_id(&format!("n{id:03}")))
                .collect::<Vec<_>>()
        );
        assert_eq!(graph.neighbors_bounded(&node, "knows", 7)?, result);
        let all = graph.neighbors_bounded(&node, "knows", 101)?;
        assert_eq!(all.scanned_count, 100);
        assert!(!all.truncated);
        assert_eq!(all.neighbors.len(), 100);
        let calls = repl.calls.load(Ordering::SeqCst);
        let zero = graph.neighbors_bounded(&node, "knows", 0)?;
        assert_eq!(zero.scanned_count, 0);
        assert_eq!(zero.neighbors, Vec::new());
        assert!(zero.truncated);
        assert_eq!(repl.calls.load(Ordering::SeqCst), calls);
        assert_eq!(
            graph.neighbors_bounded(&node, "missing", 7)?.neighbors,
            Vec::new()
        );
        Ok(())
    }

    #[test]
    fn malformed_edge_keys_still_consume_the_bounded_scan_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let node = GraphNodeId::from_str_id("root");
        let id = GraphId::new(1);
        let mut malformed = super::out_prefix(id, &node, "knows");
        malformed.push(0);
        database.propose(Op::Put {
            table: super::GRAPH_OUT_EDGES_TABLE.to_owned(),
            key: malformed,
            value: crate::model::encode_value(&Value::Null)?,
        })?;
        let graph = Graph::new(&database, id);
        graph.add_edge(
            node.clone(),
            "knows",
            GraphNodeId::from_str_id("visible"),
            &Value::Null,
        )?;
        let result = graph.neighbors_bounded(&node, "knows", 1)?;
        assert_eq!(result.scanned_count, 1);
        assert!(result.truncated);
        assert_eq!(result.neighbors, Vec::new());
        assert!(graph.neighbors_bounded(&node, "", 1).is_err());
        Ok(())
    }

    #[test]
    fn bounded_neighbor_ids_do_not_decode_or_retain_edge_properties()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let id = GraphId::new(1);
        let source = GraphNodeId::from_str_id("root");
        let target = GraphNodeId::from_str_id("target");
        database.propose(Op::Put {
            table: super::GRAPH_OUT_EDGES_TABLE.to_owned(),
            key: super::out_key(id, &source, "related", &target),
            value: b"not encoded edge properties".to_vec(),
        })?;
        let graph = Graph::new(&database, id);
        let result = graph.neighbor_ids_bounded(&source, "related", 2)?;
        assert_eq!(result.neighbors, vec![target]);
        assert_eq!(result.scanned_count, 1);
        assert!(!result.truncated);
        assert!(graph.neighbors_bounded(&source, "related", 2).is_err());
        assert_eq!(
            graph.neighbor_ids_bounded(&source, "related", 0)?.neighbors,
            Vec::new()
        );
        Ok(())
    }

    #[test]
    fn graph_edges_are_mirrored_and_traversed() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let repl: Arc<dyn Replication> = Arc::new(database);
        let graph = Graph::new(repl.as_ref(), GraphId::new(1));
        let a = GraphNodeId::from_str_id("a");
        let b = GraphNodeId::from_str_id("b");
        let c = GraphNodeId::from_str_id("c");
        graph.add_edge(a.clone(), "knows", b.clone(), &Value::Null)?;
        graph.add_edge(b.clone(), "knows", c.clone(), &Value::Null)?;
        assert_eq!(graph.neighbors(&a, "knows")?[0].dst, b);
        assert_eq!(
            graph.k_hop(
                &a,
                "knows",
                TraversalOptions {
                    max_depth: 2,
                    max_expansion: 10
                }
            )?,
            vec![GraphNodeId::from_str_id("b"), c.clone()]
        );
        let path = graph.shortest_path(&a, &c, "knows", TraversalOptions::default())?;
        assert_eq!(path.map(|path| path.len()), Some(3));
        graph.delete_edge(&a, "knows", &GraphNodeId::from_str_id("b"))?;
        assert!(
            graph.neighbors(&a, "knows")?.is_empty(),
            "expected an empty value, got {:?}",
            graph.neighbors(&a, "knows")?
        );
        Ok(())
    }
}
