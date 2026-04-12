//! Axum router setup with all routes.

use axum::Router;
use axum::routing::{delete, get, post};

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
        .route("/v1/logs", post(ingest::otlp_logs::logs_handler))
        .route("/v1/traces", post(ingest::otlp_traces::traces_handler))
        .route(
            "/api/v1/write",
            post(ingest::remote_write::remote_write_handler),
        )
        // LogQL
        .route(
            "/loki/api/v1/query",
            get(logql::handlers::query).post(logql::handlers::query_post),
        )
        .route(
            "/loki/api/v1/query_range",
            get(logql::handlers::query_range).post(logql::handlers::query_range_post),
        )
        .route("/loki/api/v1/labels", get(logql::handlers::labels))
        .route(
            "/loki/api/v1/label/{name}/values",
            get(logql::handlers::label_values),
        )
        // PromQL
        .route(
            "/api/v1/query",
            get(promql::handlers::query).post(promql::handlers::query_post),
        )
        .route(
            "/api/v1/query_range",
            get(promql::handlers::query_range).post(promql::handlers::query_range_post),
        )
        .route("/api/v1/series", get(promql::handlers::series))
        .route("/api/v1/labels", get(promql::handlers::labels))
        .route(
            "/api/v1/label/{name}/values",
            get(promql::handlers::label_values),
        )
        // TraceQL
        .route("/api/search", get(traceql::handlers::search))
        .route("/api/traces/{trace_id}", get(traceql::handlers::get_trace))
        // Service discovery, health & management
        .route("/api/v1/diagnose", get(api::diagnose::diagnose))
        .route("/api/v1/catalog", get(api::catalog::catalog))
        .route("/api/v1/reset", delete(api::reset::reset))
        .route("/api/v1/services", get(api::services::list_services))
        .route("/api/v1/status", get(api::status::status))
        .route("/api/v1/summary", get(api::summary::summary))
        .route("/api/v1/metadata", get(api::metadata::metadata))
        .route("/api/v1/openapi.json", get(api::openapi::openapi_spec))
        .route("/ready", get(api::health::ready))
        .with_state(state)
}
