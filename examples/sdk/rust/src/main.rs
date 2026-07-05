use std::time::{SystemTime, UNIX_EPOCH};

use multidb_client::ControlPlaneClient;
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base_url = std::env::var("MULTIDB_CONTROL_PLANE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8080/api".to_owned());
    let password = std::env::var("MULTIDB_ADMIN_PASSWORD")
        .unwrap_or_else(|_| "local-dev-admin-password".to_owned());
    let stamp = format!("rs_{}", now_millis());

    let client = ControlPlaneClient::with_base_url(base_url);
    let session = client.login("admin", password)?;
    let db = client.with_token(session.token);

    let table = format!("sdk_users_{stamp}");
    db.create_table(json!({
        "name": table,
        "schema": {
            "columns": [
                { "name": "id", "ty": "Int", "nullable": false },
                { "name": "name", "ty": "Str", "nullable": false }
            ],
            "primary_key": 0
        },
        "indexes": []
    }))?;
    db.insert_table_row(&table, vec![json!(1), json!("Ada")])?;
    db.sql(format!("SELECT * FROM {table}"))?;

    let collection = format!("sdk_docs_{stamp}");
    db.create_collection(json!({
        "name": collection,
        "fields": [{ "name": "name", "source": { "Path": ["name"] }, "ty": "Str" }],
        "indexes": []
    }))?;
    db.create_document(&collection, json!({ "name": "Ada" }))?;

    let vectors = format!("sdk_vectors_{stamp}");
    db.create_vector(json!({ "name": vectors, "dim": 3 }))?;
    db.insert_vector(&vectors, json!({ "label": "Ada" }), vec![1.0, 0.0, 0.0])?;
    db.search_vector(&vectors, vec![1.0, 0.0, 0.0], 1)?;

    let series = format!("sdk_series_{stamp}");
    db.create_time_series(
        json!({ "name": series, "chunk_millis": 60000, "retention_millis": null }),
    )?;
    let now = now_millis();
    db.insert_time_series_point(
        &series,
        "default",
        json!({ "timestamp_millis": now, "value": 42.0 }),
    )?;
    db.time_series_points(&series, "default", now - 1, now + 1)?;

    db.logout()?;
    println!("Rust SDK example completed");
    Ok(())
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
