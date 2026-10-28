//! Run with `cargo run --locked --example graphrag`.

use std::collections::BTreeMap;

use multidb::{
    db::{DbConfig, Profile, create_database},
    graph::GraphId,
    graphrag::{GraphRagRequest, document_node_id},
    model::{CollectionId, Value},
    vector::{VectorCollectionConfig, VectorMetric},
};

fn passage(text: &str, source: &str) -> Value {
    Value::Object(BTreeMap::from([
        ("text".to_owned(), Value::Str(text.to_owned())),
        ("source".to_owned(), Value::Str(source.to_owned())),
    ]))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let (seed, linked) = {
        let vectors = database.create_vector_collection(
            "knowledge",
            VectorCollectionConfig::new(CollectionId::new(1), 2).with_metric(VectorMetric::L2),
        )?;
        let seed = vectors.insert_vector(
            &passage("MultiDB stores document embeddings.", "guide/vectors"),
            vec![1.0, 0.0],
        )?;
        // This passage is far from the query vector; the graph supplies it.
        let linked = vectors.insert_vector(
            &passage("A graph edge connects related passages.", "guide/graphs"),
            vec![0.0, 1.0],
        )?;
        (seed, linked)
    };
    database
        .create_graph("relations", GraphId::new(1))?
        .add_edge(
            document_node_id(seed),
            "related",
            document_node_id(linked),
            &Value::Null,
        )?;
    let mut request = GraphRagRequest::new("knowledge", "relations", vec![1.0, 0.0]);
    request.seed_k = 1;
    request.max_depth = 1;
    let result = database.graph_rag(&request)?;
    assert_eq!(result.sources.len(), 2);
    assert_eq!(result.sources[1].depth, 1);
    println!("{}", result.context);
    for (index, source) in result.sources.iter().enumerate() {
        println!(
            "[{}] {} (depth {})",
            index + 1,
            source.source.as_deref().unwrap_or("unknown"),
            source.depth
        );
    }
    Ok(())
}
