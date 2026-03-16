//! Status endpoint showing store statistics.

use axum::Json;
use axum::extract::State;
use serde_json::{Value, json};

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

    // Compute memory estimates from each store
    let log_memory_bytes = {
        let store = state.log_store.read();
        store.memory_estimate_bytes()
    };
    let metric_memory_bytes = {
        let store = state.metric_store.read();
        store.memory_estimate_bytes()
    };
    let trace_memory_bytes = {
        let store = state.trace_store.read();
        store.memory_estimate_bytes()
    };
    let memory_bytes = log_memory_bytes + metric_memory_bytes + trace_memory_bytes;

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
            "memoryBytes": memory_bytes,
            "logMemoryBytes": log_memory_bytes,
            "metricMemoryBytes": metric_memory_bytes,
            "traceMemoryBytes": trace_memory_bytes,
        }
    }))
}
