//! Axum router setup with all routes.

use axum::routing::{get, post};
use axum::Router;

use crate::api;
use crate::ingest;
use crate::query::{logql, promql, traceql};
use crate::store::SharedState;

/// Build the complete Axum router with all routes.
pub fn build_router(state: SharedState) -> Router {
    Router::new()
        // Ingestion
        .route("/loki/api/v1/push", post(ingest::loki::push_handler))
        .route("/v1/metrics", post(ingest::otlp_metrics::metrics_handler))
        .route("/v1/traces", post(ingest::otlp_traces::traces_handler))
        // LogQL
        .route("/loki/api/v1/query", get(logql::handlers::query))
        .route(
            "/loki/api/v1/query_range",
            get(logql::handlers::query_range),
        )
        .route("/loki/api/v1/labels", get(logql::handlers::labels))
        .route(
            "/loki/api/v1/label/:name/values",
            get(logql::handlers::label_values),
        )
        // PromQL
        .route("/api/v1/query", get(promql::handlers::query))
        .route("/api/v1/query_range", get(promql::handlers::query_range))
        .route("/api/v1/series", get(promql::handlers::series))
        .route("/api/v1/labels", get(promql::handlers::labels))
        .route(
            "/api/v1/label/:name/values",
            get(promql::handlers::label_values),
        )
        // TraceQL
        .route("/api/search", get(traceql::handlers::search))
        .route("/api/traces/:trace_id", get(traceql::handlers::get_trace))
        // Service discovery & health
        .route("/api/v1/services", get(api::services::list_services))
        .route("/api/v1/status", get(api::status::status))
        .route("/ready", get(api::health::ready))
        .with_state(state)
}
