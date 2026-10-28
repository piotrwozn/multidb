use super::*;
use crate::query::ColumnDef;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn schema() -> TableSchema {
    TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("name", ColumnType::Str, false),
            ColumnDef::new("age", ColumnType::Int, false),
        ],
        0,
    )
}

#[test]
fn remote_projection_retains_the_original_decoder_shape_and_applies_projection_once() -> TestResult
{
    let schema = schema();
    for projection in [vec![1], vec![2, 0], vec![1, 1], Vec::new()] {
        let request = ForeignScanRequest::new(
            Some(projection.clone()),
            vec![ForeignFilter {
                column: 2,
                op: ForeignFilterOp::Ge,
                value: Value::Int(30),
            }],
            Some(10),
        );
        let plan = remote_plan(
            &schema,
            "public.people",
            &ForeignTableOptions::default(),
            &request,
        )?;
        let mut needed = projection.clone();
        needed.push(2);
        needed.sort_unstable();
        needed.dedup();
        assert_eq!(plan.needed, needed);
        let columns = needed
            .iter()
            .map(|index| quote_ident(&schema.columns[*index].name))
            .collect::<Vec<_>>()
            .join(", ");
        assert_eq!(
            plan.sql,
            format!("SELECT {columns} FROM \"public\".\"people\" WHERE \"age\" >= $1 LIMIT 10")
        );
        assert_eq!(plan.types, vec![tokio_postgres::types::Type::INT8]);
        let row = vec![Value::Int(1), Value::Str("Ada".to_owned()), Value::Int(37)];
        let expected = projection
            .iter()
            .map(|index| row[*index].clone())
            .collect::<Vec<_>>();
        assert_eq!(
            apply_filters_and_projection(&schema, vec![row], &request)?,
            vec![expected]
        );
    }
    Ok(())
}

#[test]
fn remote_null_and_binary_filters_use_exact_sql_and_typed_binary_parameters() -> TestResult {
    let schema = TableSchema::new(vec![ColumnDef::new("payload", ColumnType::Bytes, true)], 0);
    let null = ForeignScanRequest::new(
        None,
        vec![ForeignFilter {
            column: 0,
            op: ForeignFilterOp::Eq,
            value: Value::Null,
        }],
        Some(1),
    );
    let plan = remote_plan(&schema, "data", &ForeignTableOptions::default(), &null)?;
    assert!(plan.sql.contains("\"payload\" IS NULL LIMIT 1"));
    assert_eq!(plan.params.len(), 0);
    let bytes = vec![0, 255, 39, 92];
    let request = ForeignScanRequest::new(
        None,
        vec![ForeignFilter {
            column: 0,
            op: ForeignFilterOp::Eq,
            value: Value::Bytes(bytes.clone()),
        }],
        Some(1),
    );
    let plan = remote_plan(&schema, "data", &ForeignTableOptions::default(), &request)?;
    assert_eq!(plan.types, vec![tokio_postgres::types::Type::BYTEA]);
    assert!(plan.sql.contains("\"payload\" = $1 LIMIT 1"));
    let mut encoded = bytes::BytesMut::new();
    plan.params[0]
        .to_sql_checked(&tokio_postgres::types::Type::BYTEA, &mut encoded)
        .map_err(|error| error.to_string())?;
    assert_eq!(encoded.as_ref(), bytes);
    let ordering = ForeignScanRequest::new(
        None,
        vec![ForeignFilter {
            column: 0,
            op: ForeignFilterOp::Gt,
            value: Value::Bytes(vec![0]),
        }],
        Some(1),
    );
    let plan = remote_plan(&schema, "data", &ForeignTableOptions::default(), &ordering)?;
    assert!(plan.sql.contains("\"payload\" > $1 LIMIT 1"));
    assert_eq!(
        apply_filters_and_projection(
            &schema,
            vec![vec![Value::Bytes(vec![0])], vec![Value::Bytes(vec![255])]],
            &ordering
        )?,
        vec![vec![Value::Bytes(vec![255])]]
    );
    Ok(())
}

#[test]
fn remote_inexact_collation_and_nan_filters_cannot_consume_output_limit() -> TestResult {
    let schema = schema();
    let request = ForeignScanRequest::new(
        Some(vec![1, 1]),
        vec![ForeignFilter {
            column: 1,
            op: ForeignFilterOp::Eq,
            value: Value::Str("alice".to_owned()),
        }],
        Some(1),
    );
    let plan = remote_plan(&schema, "people", &ForeignTableOptions::default(), &request)?;
    assert!(plan.sql.contains("\"name\" = $1"));
    assert!(!plan.sql.contains("LIMIT"));
    assert_eq!(plan.limit, Some(1));
    let rows = vec![
        vec![Value::Int(1), Value::Str("ALICE".to_owned()), Value::Int(1)],
        vec![Value::Int(2), Value::Str("alice".to_owned()), Value::Int(1)],
    ];
    assert_eq!(
        apply_filters_and_projection(&schema, rows, &request)?,
        vec![vec![Value::Str("alice".to_owned()); 2]]
    );
    let ordering = ForeignScanRequest::new(
        Some(vec![]),
        vec![ForeignFilter {
            column: 1,
            op: ForeignFilterOp::Lt,
            value: Value::Str("a".to_owned()),
        }],
        Some(1),
    );
    let plan = remote_plan(
        &schema,
        "people",
        &ForeignTableOptions::default(),
        &ordering,
    )?;
    assert!(!plan.sql.contains("WHERE"));
    assert!(!plan.sql.contains("LIMIT"));
    let floats = TableSchema::new(vec![ColumnDef::new("value", ColumnType::Float, true)], 0);
    let request = ForeignScanRequest::new(
        None,
        vec![ForeignFilter {
            column: 0,
            op: ForeignFilterOp::Gt,
            value: Value::Float(0.0),
        }],
        Some(1),
    );
    assert!(
        !remote_plan(&floats, "values", &ForeignTableOptions::default(), &request)?
            .sql
            .contains("LIMIT")
    );
    Ok(())
}

#[test]
fn remote_empty_projection_keeps_count_and_quoted_strings_stay_out_of_sql() -> TestResult {
    let request = ForeignScanRequest::new(Some(vec![]), vec![], Some(2));
    let schema = schema();
    assert_eq!(
        remote_plan(&schema, "people", &ForeignTableOptions::default(), &request)?.sql,
        "SELECT 1 FROM \"people\" LIMIT 2"
    );
    let value = "x'\\'; SELECT secret FROM private; --";
    let request = ForeignScanRequest::new(
        None,
        vec![ForeignFilter {
            column: 1,
            op: ForeignFilterOp::Eq,
            value: Value::Str(value.to_owned()),
        }],
        Some(1),
    );
    let plan = remote_plan(&schema, "people", &ForeignTableOptions::default(), &request)?;
    assert!(!plan.sql.contains(value));
    assert!(plan.sql.ends_with("\"name\" = $1"));
    Ok(())
}

#[test]
fn explicit_limit_zero_is_empty_even_when_unlimited_remote_scans_are_enabled() -> TestResult {
    let schema = schema();
    let request = ForeignScanRequest::new(None, Vec::new(), Some(0));
    for unlimited in [false, true] {
        let options = ForeignTableOptions {
            remote_scan_limit: 0,
            allow_remote_without_limit: unlimited,
            ..ForeignTableOptions::default()
        };
        assert!(remote_select_sql(&schema, "people", &options, &request)?.ends_with(" LIMIT 0"));
        assert_eq!(
            apply_filters_and_projection(
                &schema,
                vec![vec![
                    Value::Int(1),
                    Value::Str("Ada".to_owned()),
                    Value::Int(37)
                ]],
                &request
            )?,
            Vec::<Row>::new()
        );
        let unspecified = ForeignScanRequest::new(None, Vec::new(), None);
        assert_eq!(
            remote_select_sql(&schema, "people", &options, &unspecified).is_ok(),
            unlimited
        );
    }
    Ok(())
}

#[tokio::test]
async fn zero_remote_limit_requires_no_secret_resolution_or_network_connection() -> TestResult {
    let source = ForeignSource::Postgres {
        connection: SecretRef::EnvVar(format!("MULTIDB_UNUSED_{}", uuid::Uuid::now_v7().simple())),
        table: "people".to_owned(),
    };
    let result = scan_remote_postgres(
        &schema(),
        &source,
        &ForeignTableOptions::default(),
        &ForeignScanRequest::new(Some(vec![1]), Vec::new(), Some(0)),
    )
    .await?;
    assert_eq!(result.rows, Vec::<Row>::new());
    assert_eq!(result.stats.scanned_rows, 0);
    Ok(())
}

#[test]
fn invalid_columns_are_rejected_even_for_empty_sources_and_zero_limits() {
    let schema = schema();
    for request in [
        ForeignScanRequest::new(Some(vec![3]), Vec::new(), Some(0)),
        ForeignScanRequest::new(
            None,
            vec![ForeignFilter {
                column: 99,
                op: ForeignFilterOp::Eq,
                value: Value::Null,
            }],
            None,
        ),
    ] {
        assert!(apply_filters_and_projection(&schema, Vec::new(), &request).is_err());
        assert!(
            remote_select_sql(&schema, "people", &ForeignTableOptions::default(), &request)
                .is_err()
        );
    }
}
