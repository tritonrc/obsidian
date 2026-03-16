//! Per-service metric/label catalog endpoint.

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use rustc_hash::FxHashSet;
use serde::Deserialize;
use serde_json::json;

use crate::store::SharedState;

/// Query parameters for the catalog endpoint.
#[derive(Deserialize)]
pub struct CatalogParams {
    pub service: Option<String>,
}

/// GET /api/v1/catalog?service=X
///
/// Returns the metric names, log label keys, and span attribute keys
/// for a given service. Returns 400 if the `service` parameter is missing.
pub async fn catalog(
    State(state): State<SharedState>,
    Query(params): Query<CatalogParams>,
) -> impl IntoResponse {
    let service = match params.service {
        Some(s) if !s.is_empty() => s,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "status": "error",
                    "error": "missing required parameter: service"
                })),
            );
        }
    };

    // Collect metric names from MetricStore
    let metrics = {
        let store = state.metric_store.read();
        let mut names: Vec<String> = Vec::new();
        let service_spur = store.interner.get("service");
        let service_val_spur = service_spur.and_then(|_| store.interner.get(&service));
        let name_key_spur = store.interner.get("__name__");

        if let (Some(svc_key), Some(svc_val), Some(name_key)) =
            (service_spur, service_val_spur, name_key_spur)
        {
            // Find all series with service=X
            if let Some(pl) = store.label_index.get(&(svc_key, svc_val)) {
                let mut seen = FxHashSet::default();
                for &series_id in pl.ids() {
                    if let Some(series) = store.series.get(&series_id) {
                        for &(k, v) in &series.labels {
                            if k == name_key && seen.insert(v) {
                                names.push(store.interner.resolve(&v).to_string());
                            }
                        }
                    }
                }
            }
        }
        names.sort();
        names
    };

    // Collect log label keys from LogStore
    let log_labels = {
        let store = state.log_store.read();
        let mut label_keys: Vec<String> = Vec::new();
        let service_spur = store.interner.get("service");
        let service_val_spur = service_spur.and_then(|_| store.interner.get(&service));

        if let (Some(svc_key), Some(svc_val)) = (service_spur, service_val_spur)
            && let Some(pl) = store.label_index.get(&(svc_key, svc_val))
        {
            let mut seen = FxHashSet::default();
            for &stream_id in pl.ids() {
                if let Some(stream) = store.streams.get(&stream_id) {
                    for &(k, _) in &stream.labels {
                        if seen.insert(k) {
                            label_keys.push(store.interner.resolve(&k).to_string());
                        }
                    }
                }
            }
        }
        label_keys.sort();
        label_keys
    };

    // Collect span attribute keys from TraceStore
    let span_attributes = {
        let store = state.trace_store.read();
        let mut attr_keys: Vec<String> = Vec::new();
        let service_spur = store.interner.get(&service);

        if let Some(svc_spur) = service_spur
            && let Some(trace_ids) = store.service_index.get(&svc_spur)
        {
            let mut seen = FxHashSet::default();
            for trace_id in trace_ids {
                if let Some(spans) = store.traces.get(trace_id) {
                    for span in spans {
                        if span.service_name == svc_spur {
                            for &(k, _) in &span.attributes {
                                if seen.insert(k) {
                                    attr_keys.push(store.interner.resolve(&k).to_string());
                                }
                            }
                        }
                    }
                }
            }
        }
        attr_keys.sort();
        attr_keys
    };

    (
        StatusCode::OK,
        Json(json!({
            "status": "success",
            "data": {
                "metrics": metrics,
                "log_labels": log_labels,
                "span_attributes": span_attributes,
            }
        })),
    )
}
