//! Metadata endpoint for Grafana Prometheus datasource compatibility.

use axum::Json;
use serde_json::{Value, json};

/// GET /api/v1/metadata
///
/// Returns an empty metadata object. Grafana calls this endpoint
/// when setting up a Prometheus datasource to verify connectivity.
pub async fn metadata() -> Json<Value> {
    Json(json!({}))
}
