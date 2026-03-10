//! Health check endpoint.

use axum::Json;
use serde_json::{Value, json};

/// GET /ready
pub async fn ready() -> Json<Value> {
    Json(json!({"status": "ready"}))
}
