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

    // Approximate memory usage from store sizes
    let memory_bytes = {
        let log_store = state.log_store.read();
        let metric_store = state.metric_store.read();
        let trace_store = state.trace_store.read();

        // Rough estimate: log entries ~128 bytes each, samples ~16 bytes each, spans ~256 bytes each
        let log_mem = log_store.total_entries * 128;
        let metric_mem = metric_store.total_samples * 16;
        let trace_mem = trace_store.total_spans * 256;
        log_mem + metric_mem + trace_mem
    };

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
        }
    }))
}
