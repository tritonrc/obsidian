//! Status endpoint showing store statistics.

use axum::extract::State;
use axum::Json;
use serde_json::{json, Value};

use crate::store::SharedState;

/// GET /api/v1/status
pub async fn status(State(state): State<SharedState>) -> Json<Value> {
    let log_entries = {
        let store = state.log_store.read();
        store.total_entries
    };
    let (metric_series, metric_samples) = {
        let store = state.metric_store.read();
        (store.series.len(), store.total_samples)
    };
    let (total_spans, total_traces) = {
        let store = state.trace_store.read();
        (store.total_spans, store.traces.len())
    };
    let uptime = state.start_time.elapsed().as_secs();

    // Count unique services
    let mut services = rustc_hash::FxHashSet::default();
    {
        let store = state.log_store.read();
        for name in store.get_label_values("service") {
            services.insert(name);
        }
    }
    {
        let store = state.metric_store.read();
        for name in store.get_label_values("service") {
            services.insert(name);
        }
    }
    {
        let store = state.trace_store.read();
        for name in store.service_names() {
            services.insert(name);
        }
    }

    Json(json!({
        "status": "success",
        "data": {
            "totalLogEntries": log_entries,
            "totalMetricSeries": metric_series,
            "totalMetricSamples": metric_samples,
            "totalSpans": total_spans,
            "totalTraces": total_traces,
            "uptimeSeconds": uptime,
            "serviceCount": services.len(),
        }
    }))
}
