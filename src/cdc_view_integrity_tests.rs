use super::*;
use crate::{
    db::{DbConfig, Profile, create_database},
    query::{ColumnDef, ColumnType, TableSchema},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn schema() -> TableSchema {
    TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("category", ColumnType::Str, false),
            ColumnDef::new("amount", ColumnType::Float, false),
        ],
        0,
    )
}

fn spec(with_count: bool) -> MaterializedViewSpec {
    let mut aggregates = vec![AggregateSpec {
        output_name: "sum".to_owned(),
        kind: AggregateKind::Sum,
        column: Some(2),
    }];
    if with_count {
        aggregates.push(AggregateSpec {
            output_name: "count".to_owned(),
            kind: AggregateKind::Count,
            column: None,
        });
    }
    MaterializedViewSpec {
        name: "totals".to_owned(),
        source_table: "items".to_owned(),
        filter: None,
        group_by: 1,
        aggregates,
    }
}

#[test]
fn sum_groups_disappear_when_the_last_source_row_moves_or_is_deleted() -> TestResult {
    for with_count in [false, true] {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        let table = database.create_table("items", Some(schema()), Vec::new())?;
        table.insert(vec![
            Value::Int(1),
            Value::Str("a".to_owned()),
            Value::Float(10.0),
        ])?;
        table.insert(vec![
            Value::Int(2),
            Value::Str("a".to_owned()),
            Value::Float(-10.0),
        ])?;
        let initial = create_materialized_view(&database, &spec(with_count))?;
        assert_eq!(
            initial.rows.len(),
            1,
            "a zero sum with two contributing rows is not an empty group"
        );
        table.delete(&Value::Int(1))?;
        assert_eq!(
            refresh_materialized_view(&database, "totals")?.rows[0][1],
            Value::Float(-10.0)
        );
        table.update(vec![
            Value::Int(2),
            Value::Str("b".to_owned()),
            Value::Float(5.0),
        ])?;
        let moved = refresh_materialized_view(&database, "totals")?;
        assert_eq!(moved.rows.len(), 1);
        assert_eq!(moved.rows[0][0], Value::Str("b".to_owned()));
        table.delete(&Value::Int(2))?;
        assert_eq!(
            refresh_materialized_view(&database, "totals")?.rows,
            Vec::<Row>::new()
        );
    }
    Ok(())
}

#[test]
fn legacy_sum_groups_recover_member_counts_before_the_next_refresh() -> TestResult {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let table = database.create_table("items", Some(schema()), Vec::new())?;
    table.insert(vec![
        Value::Int(1),
        Value::Str("a".to_owned()),
        Value::Float(10.0),
    ])?;
    create_materialized_view(&database, &spec(false))?;
    let bytes = database
        .read(
            MATERIALIZED_VIEW_STATE_TABLE,
            b"totals",
            ReadConsistency::Strong,
        )?
        .ok_or("missing state")?;
    let mut legacy: serde_json::Value = serde_json::from_slice(&bytes)?;
    for group in legacy
        .get_mut("groups")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or("missing groups")?
        .values_mut()
    {
        group
            .as_object_mut()
            .ok_or("invalid group")?
            .remove("row_count");
    }
    propose_system(
        &database,
        Op::Put {
            table: MATERIALIZED_VIEW_STATE_TABLE.to_owned(),
            key: b"totals".to_vec(),
            value: serde_json::to_vec(&legacy)?,
        },
    )?;
    table.delete(&Value::Int(1))?;
    assert_eq!(
        refresh_materialized_view(&database, "totals")?.rows,
        Vec::<Row>::new()
    );
    Ok(())
}

#[test]
fn stale_materialized_state_cannot_overwrite_a_newer_refresh() -> TestResult {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let table = database.create_table("items", Some(schema()), Vec::new())?;
    table.insert(vec![
        Value::Int(1),
        Value::Str("a".to_owned()),
        Value::Float(10.0),
    ])?;
    let definition = spec(false);
    create_materialized_view(&database, &definition)?;
    let old_spec = database.read(MATERIALIZED_VIEWS_TABLE, b"totals", ReadConsistency::Strong)?;
    let old_state = database.read(
        MATERIALIZED_VIEW_STATE_TABLE,
        b"totals",
        ReadConsistency::Strong,
    )?;
    let stale = decode_view_state(old_state.as_deref().ok_or("missing state")?, &definition)?;
    table.insert(vec![
        Value::Int(2),
        Value::Str("a".to_owned()),
        Value::Float(20.0),
    ])?;
    let latest = refresh_materialized_view(&database, "totals")?;
    assert!(matches!(
        write_view(&database, &definition, &stale, old_spec, old_state),
        Err(MaterializedViewError::Repl(ReplError::Conflict))
    ));
    assert_eq!(read_materialized_view(&database, "totals")?, latest);
    Ok(())
}

#[test]
fn columnar_view_sources_are_rejected_before_writing_empty_metadata() -> TestResult {
    let temp = tempfile::tempdir()?;
    let mut database = create_database(DbConfig::on_disk(
        Profile::Analytical,
        temp.path().join("analytics.redb"),
    ))?;
    database.create_table("items", Some(schema()), Vec::new())?;
    assert!(
        matches!(create_materialized_view(&database, &spec(false)), Err(MaterializedViewError::Unsupported(message)) if message.contains("row-layout"))
    );
    assert_eq!(
        database.read(MATERIALIZED_VIEWS_TABLE, b"totals", ReadConsistency::Strong)?,
        None
    );
    assert_eq!(
        database.read(
            MATERIALIZED_VIEW_STATE_TABLE,
            b"totals",
            ReadConsistency::Strong
        )?,
        None
    );
    Ok(())
}

#[test]
fn aggregate_overflow_is_rejected_without_persisting_invalid_view_state() -> TestResult {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let table = database.create_table("items", Some(schema()), Vec::new())?;
    table.insert(vec![
        Value::Int(1),
        Value::Str("a".to_owned()),
        Value::Float(f64::MAX),
    ])?;
    table.insert(vec![
        Value::Int(2),
        Value::Str("a".to_owned()),
        Value::Float(f64::MAX),
    ])?;
    assert!(
        matches!(create_materialized_view(&database, &spec(false)), Err(MaterializedViewError::Unsupported(message)) if message.contains("finite f64"))
    );
    assert_eq!(
        database.read(
            MATERIALIZED_VIEW_STATE_TABLE,
            b"totals",
            ReadConsistency::Strong
        )?,
        None
    );
    Ok(())
}
