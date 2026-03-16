//! Store reset endpoint.

use axum::Json;
use axum::extract::{Query, State};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::store::SharedState;

/// Query parameters for the reset endpoint.
#[derive(Deserialize)]
pub struct ResetParams {
    pub service: Option<String>,
}

/// DELETE /api/v1/reset — clears all stores or a specific service's data.
pub async fn reset(
    State(state): State<SharedState>,
    Query(params): Query<ResetParams>,
) -> Json<Value> {
    match params.service {
        Some(service) => {
            state.log_store.write().clear_service(&service);
            state.metric_store.write().clear_service(&service);
            state.trace_store.write().clear_service(&service);
        }
        None => {
            state.log_store.write().clear();
            state.metric_store.write().clear();
            state.trace_store.write().clear();
        }
    }

    Json(json!({}))
}
