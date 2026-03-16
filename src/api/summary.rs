//! Unified diagnostic summary endpoint.
//!
//! GET /api/v1/summary?service=<name> returns a JSON summary of errors
//! across logs, metrics, and traces for the given service.

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::store::SharedState;
use crate::store::log_store::{LabelMatchOp, LabelMatcher};
use crate::store::trace_store::SpanStatus;

/// Query parameters for the summary endpoint.
#[derive(Debug, Deserialize)]
pub struct SummaryParams {
    pub service: Option<String>,
}

/// GET /api/v1/summary?service=<name>
pub async fn summary(
    State(state): State<SharedState>,
    Query(params): Query<SummaryParams>,
) -> impl IntoResponse {
    let service = match params.service {
        Some(s) if !s.is_empty() => s,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "missing required query parameter: service"})),
            )
                .into_response();
        }
    };

    // --- Error logs: streams with service=X AND level="error" ---
    let error_logs = {
        let store = state.log_store.read();
        let matchers = vec![
            LabelMatcher {
                name: "service".into(),
                op: LabelMatchOp::Eq,
                value: service.clone(),
            },
            LabelMatcher {
                name: "level".into(),
                op: LabelMatchOp::Eq,
                value: "error".into(),
            },
        ];
        let stream_ids = store.query_streams(&matchers);
        let mut logs: Vec<Value> = Vec::new();
        for sid in stream_ids {
            let entries = store.get_entries(sid, 0, i64::MAX);
            for entry in entries {
                logs.push(json!({
                    "timestamp": entry.timestamp_ns.to_string(),
                    "line": entry.line,
                }));
            }
        }
        logs
    };

    // --- Error metrics: metrics with "error" in the name for this service ---
    let error_metrics = {
        let store = state.metric_store.read();
        // Find all series that have service=<service>
        let matchers = vec![LabelMatcher {
            name: "service".into(),
            op: LabelMatchOp::Eq,
            value: service.clone(),
        }];
        let series_ids = store.select_series(&matchers);
        let mut metrics: Vec<Value> = Vec::new();
        let name_key = store.interner.get("__name__");
        for sid in series_ids {
            if let Some(series) = store.series.get(&sid) {
                // Check if the metric name contains "error"
                let metric_name = name_key.and_then(|nk| {
                    series
                        .labels
                        .iter()
                        .find(|(k, _)| *k == nk)
                        .map(|(_, v)| store.interner.resolve(v).to_string())
                });
                let name = match metric_name {
                    Some(ref n) if n.contains("error") => n.clone(),
                    _ => continue,
                };
                // Build labels map (excluding __name__)
                let mut label_map = serde_json::Map::new();
                for (k, v) in &series.labels {
                    let key = store.interner.resolve(k).to_string();
                    if key == "__name__" {
                        continue;
                    }
                    let val = store.interner.resolve(v).to_string();
                    label_map.insert(key, Value::String(val));
                }
                // Get the latest sample value
                if let Some(sample) = series.samples.last() {
                    metrics.push(json!({
                        "name": name,
                        "labels": label_map,
                        "value": sample.value,
                    }));
                }
            }
        }
        metrics
    };

    // --- Error traces: traces with error status for this service ---
    let error_traces = {
        let store = state.trace_store.read();
        let trace_ids = store.traces_for_service(&service);
        let mut traces: Vec<Value> = Vec::new();
        for tid in &trace_ids {
            if let Some(spans) = store.get_trace(tid) {
                // Check if any span in this trace has error status
                let has_error = spans.iter().any(|s| s.status == SpanStatus::Error);
                if has_error && let Some(result) = store.trace_result(tid) {
                    let hex_id: String = tid.iter().map(|b| format!("{:02x}", b)).collect();
                    traces.push(json!({
                        "traceID": hex_id,
                        "rootServiceName": result.root_service_name,
                        "rootSpanName": result.root_span_name,
                        "startTimeNs": result.start_time_ns,
                        "durationNs": result.duration_ns,
                        "spanCount": result.span_count,
                    }));
                }
            }
        }
        traces
    };

    Json(json!({
        "service": service,
        "errors": {
            "logs": error_logs,
            "metrics": error_metrics,
            "traces": error_traces,
        }
    }))
    .into_response()
}
