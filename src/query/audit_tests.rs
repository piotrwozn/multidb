use super::*;
use crate::db::{DbConfig, Profile, create_database};

#[tokio::test]
async fn cached_partial_index_is_revalidated_for_new_literals()
-> Result<(), Box<dyn std::error::Error>> {
    let repl: Arc<dyn Replication> = Arc::new(create_database(DbConfig::new(Profile::InMemory))?);
    let mut engine = SqlEngine::new(repl);
    engine.create_table(
        "items",
        Some(TableSchema::new(
            vec![
                ColumnDef::new("id", ColumnType::Int, false),
                ColumnDef::new("category", ColumnType::Int, false),
                ColumnDef::new("value", ColumnType::Int, false),
            ],
            0,
        )),
        vec![RelIndexSpec::new(1, 1).with_predicate(RelPredicate::Eq {
            expression: RelIndexExpression::Column(2),
            value: Value::Int(16),
        })],
    )?;
    engine
        .execute("INSERT INTO items VALUES (1,1,16),(2,1,24),(3,2,24)")
        .await?;
    let first = "SELECT id FROM items WHERE category=1 AND value=16";
    let second = "SELECT id FROM items WHERE category=1 AND value=24";
    assert_eq!(QueryFingerprint::new(first), QueryFingerprint::new(second));
    assert!(matches!(
        engine.explain(first, ExplainOptions::default())?.nodes[0].access_path,
        AccessPath::BTreeIndex { index_id: 1, .. }
    ));
    assert_eq!(
        engine.execute(first).await?,
        SqlOutput::Rows(SqlRows {
            columns: vec!["id".to_owned()],
            rows: vec![vec![Value::Int(1)]],
        })
    );
    assert_eq!(
        engine.execute(second).await?,
        SqlOutput::Rows(SqlRows {
            columns: vec!["id".to_owned()],
            rows: vec![vec![Value::Int(2)]],
        })
    );
    assert!(matches!(
        engine.explain(second, ExplainOptions::default())?.nodes[0].access_path,
        AccessPath::SeqScan { .. }
    ));
    Ok(())
}
