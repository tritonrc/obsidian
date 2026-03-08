//! Health check endpoint.

use axum::Json;
use serde_json::{json, Value};

/// GET /ready
pub async fn ready() -> Json<Value> {
    Json(json!({"status": "ready"}))
}
