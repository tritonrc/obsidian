//! Service discovery endpoint.

use axum::extract::State;
use axum::Json;
use rustc_hash::{FxHashMap, FxHashSet};
use serde_json::{json, Value};

use crate::store::SharedState;

/// GET /api/v1/services
pub async fn list_services(State(state): State<SharedState>) -> Json<Value> {
    let mut service_signals: FxHashMap<String, FxHashSet<&str>> = FxHashMap::default();

    // Collect from LogStore
    {
        let store = state.log_store.read();
        for name in store.get_label_values("service") {
            service_signals.entry(name).or_default().insert("logs");
        }
    }

    // Collect from MetricStore
    {
        let store = state.metric_store.read();
        for name in store.get_label_values("service") {
            service_signals.entry(name).or_default().insert("metrics");
        }
    }

    // Collect from TraceStore
    {
        let store = state.trace_store.read();
        for name in store.service_names() {
            service_signals.entry(name).or_default().insert("traces");
        }
    }

    let mut services: Vec<Value> = service_signals
        .into_iter()
        .map(|(name, signals)| {
            let mut sigs: Vec<&str> = signals.into_iter().collect();
            sigs.sort();
            json!({
                "name": name,
                "signals": sigs,
            })
        })
        .collect();
    services.sort_by(|a, b| {
        a["name"]
            .as_str()
            .unwrap_or("")
            .cmp(b["name"].as_str().unwrap_or(""))
    });

    Json(json!({
        "status": "success",
        "data": {
            "services": services,
        }
    }))
}
