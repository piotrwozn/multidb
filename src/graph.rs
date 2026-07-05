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
    use std::sync::Arc;

    use crate::{
        db::{DbConfig, Profile, create_database},
        model::Value,
        repl::Replication,
    };

    use super::{Graph, GraphId, GraphNodeId, TraversalOptions};

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
        assert!(graph.neighbors(&a, "knows")?.is_empty());
        Ok(())
    }
}
