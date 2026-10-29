//! The time-series HTTP schema stays typed after row and masking policies.

use super::{
    BTreeMap, DbError, ExtensionError, ModelValue, PolicyConfig, Principal, Resource, TimePoint,
};

fn incompatible() -> DbError {
    ExtensionError::PolicyDenied(
        "time-series masking must retain an integer timestamp_millis and finite numeric value"
            .to_owned(),
    )
    .into()
}

pub(super) fn apply(
    policy: &PolicyConfig,
    principal: &Principal,
    name: &str,
    series: &str,
    points: Vec<TimePoint>,
) -> Result<Vec<TimePoint>, DbError> {
    let resource = Resource::TimeSeries(name.to_owned());
    if !policy
        .row_policies
        .iter()
        .any(|rule| rule.resource == resource)
        && !policy.masking.iter().any(|rule| rule.resource == resource)
    {
        return Ok(points);
    }
    let mut visible = Vec::new();
    for point in points {
        let object = ModelValue::Object(BTreeMap::from([
            ("series".to_owned(), ModelValue::Str(series.to_owned())),
            (
                "timestamp_millis".to_owned(),
                ModelValue::Int(point.timestamp_millis),
            ),
            ("value".to_owned(), ModelValue::Float(point.value)),
        ]));
        if !policy.row_visible(principal, &resource, &object) {
            continue;
        }
        let ModelValue::Object(masked) = policy.mask_value(&resource, &object) else {
            return Err(incompatible());
        };
        let Some(ModelValue::Int(timestamp_millis)) = masked.get("timestamp_millis") else {
            return Err(incompatible());
        };
        let value = match masked.get("value") {
            Some(ModelValue::Float(value)) if value.is_finite() => *value,
            // Policy integers have the response's f64 numeric representation.
            Some(ModelValue::Int(value)) => value
                .to_string()
                .parse::<f64>()
                .map_err(|_| incompatible())?,
            _ => return Err(incompatible()),
        };
        visible.push(TimePoint {
            timestamp_millis: *timestamp_millis,
            value,
        });
    }
    Ok(visible)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{DbConfig, Profile, create_database},
        extension::{MaskingPolicy, RowPolicy},
        model::FieldPath,
        security::{AuthzPolicy, Permission, PrincipalRegistry, Role},
        timeseries::{DEFAULT_MAX_FUTURE_SKEW_MILLIS, TimeSeriesConfig},
    };
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use tower::ServiceExt;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn state(
        policy: &PolicyConfig,
    ) -> Result<super::super::AdminState, Box<dyn std::error::Error>> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        let time_series = database.create_time_series(TimeSeriesConfig {
            name: "metrics".to_owned(),
            chunk_millis: 1000,
            retention_millis: None,
            max_future_skew_millis: DEFAULT_MAX_FUTURE_SKEW_MILLIS,
        })?;
        for (timestamp_millis, value) in [(1, 17.0), (2, 99.0)] {
            time_series.insert_point(
                "alice",
                TimePoint {
                    timestamp_millis,
                    value,
                },
            )?;
            time_series.insert_point(
                "bob",
                TimePoint {
                    timestamp_millis,
                    value: value + 100.0,
                },
            )?;
        }
        database.set_policy_config(policy)?;
        database
            .set_authz_policy(AuthzPolicy::new([Role::new("reader")
                .grant(Resource::TimeSeries("metrics".to_owned()), Permission::Read)]));
        database.set_principal_registry(
            PrincipalRegistry::new()
                .with_principal("alice", Principal::new("alice").with_role("reader")),
        );
        Ok(super::super::AdminState::from_database(database).with_admin_token("token"))
    }

    async fn request(
        app: axum::Router,
        series: &str,
    ) -> Result<(axum::http::StatusCode, serde_json::Value), Box<dyn std::error::Error>> {
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/data/time-series/metrics/points?series={series}&start=0&end=1000"
                    ))
                    .header("authorization", "Bearer token")
                    .header("x-multidb-principal", "alice")
                    .body(Body::empty())?,
            )
            .await?;
        let status = response.status();
        Ok((
            status,
            serde_json::from_slice(&to_bytes(response.into_body(), 1_048_576).await?)?,
        ))
    }

    #[tokio::test]
    async fn http_time_series_filters_original_points_before_applying_typed_masks() -> TestResult {
        let policy = PolicyConfig {
            row_policies: vec![RowPolicy {
                resource: Resource::TimeSeries("metrics".to_owned()),
                path: FieldPath::new(["series"]),
                equals: ModelValue::Str("no-shared-series".to_owned()),
            }],
            masking: vec![MaskingPolicy {
                resource: Resource::TimeSeries("metrics".to_owned()),
                paths: vec![FieldPath::new(["value"])],
                replacement: ModelValue::Float(-1.0),
            }],
            ..PolicyConfig::default()
        };
        let app = crate::admin::router(state(&policy)?);
        let (status, visible) = request(app.clone(), "alice").await?;
        assert_eq!(status, axum::http::StatusCode::OK);
        assert_eq!(
            visible["data"]["points"],
            serde_json::json!([
                {"timestamp_millis":1,"value":-1.0}, {"timestamp_millis":2,"value":-1.0}
            ])
        );
        let (status, hidden) = request(app, "bob").await?;
        assert_eq!(status, axum::http::StatusCode::OK);
        assert_eq!(hidden["data"]["points"], serde_json::json!([]));
        Ok(())
    }

    #[tokio::test]
    async fn an_incompatible_mask_never_falls_back_to_the_original_time_series_values() -> TestResult
    {
        let policy = PolicyConfig {
            masking: vec![MaskingPolicy {
                resource: Resource::TimeSeries("metrics".to_owned()),
                paths: vec![FieldPath::new(["value"])],
                replacement: ModelValue::Str("redacted".to_owned()),
            }],
            ..PolicyConfig::default()
        };
        let (status, body) = request(crate::admin::router(state(&policy)?), "alice").await?;
        assert_eq!(status, axum::http::StatusCode::FORBIDDEN);
        assert!(body["data"].is_null());
        Ok(())
    }
}
