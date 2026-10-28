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
        assert_eq!(
            remote_select_sql(
                &schema,
                "public.people",
                &ForeignTableOptions::default(),
                &request
            )?,
            "SELECT \"id\", \"name\", \"age\" FROM \"public\".\"people\" WHERE \"age\" >= 30 LIMIT 10"
        );
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
