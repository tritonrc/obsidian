//! Compact per-service error summary endpoint.

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::json;

use crate::store::SharedState;
use crate::store::trace_store::SpanStatus;
use crate::store::{LabelMatchOp, LabelMatcher};

const ONE_HOUR_NS: i64 = 3_600 * 1_000_000_000;
const MAX_LOG_ITEMS: usize = 20;
const MAX_METRIC_ITEMS: usize = 20;
const MAX_TRACE_ITEMS: usize = 20;

#[derive(Debug, Deserialize)]
pub struct SummaryParams {
    pub service: Option<String>,
}

/// GET /api/v1/summary?service=<name>
///
/// Returns a compact cross-signal error summary for a single service.
pub async fn summary(
    State(state): State<SharedState>,
    Query(params): Query<SummaryParams>,
) -> (StatusCode, Json<serde_json::Value>) {
    let service = match params.service {
        Some(service) if !service.is_empty() => service,
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

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    let lookback_start = now_ns.saturating_sub(ONE_HOUR_NS);

    let logs = summarize_logs(&state, &service, lookback_start, now_ns);
    let metrics = summarize_metrics(&state, &service);
    let traces = summarize_traces(&state, &service);

    (
        StatusCode::OK,
        Json(json!({
            "status": "success",
            "data": {
                "service": service,
                "logs": logs,
                "metrics": metrics,
                "traces": traces,
            }
        })),
    )
}

fn summarize_logs(
    state: &SharedState,
    service: &str,
    start_ns: i64,
    end_ns: i64,
) -> Vec<serde_json::Value> {
    let store = state.log_store.read();
    let matchers = vec![
        LabelMatcher {
            name: "service".into(),
            op: LabelMatchOp::Eq,
            value: service.to_string(),
        },
        LabelMatcher {
            name: "level".into(),
            op: LabelMatchOp::Eq,
            value: "error".into(),
        },
    ];

    let mut logs = Vec::new();
    for stream_id in store.query_streams(&matchers) {
        for entry in store.get_entries(stream_id, start_ns, end_ns) {
            logs.push(json!({
                "timestamp": entry.timestamp_ns.to_string(),
                "line": entry.line,
            }));
        }
    }

    logs.sort_by(|a, b| {
        let a_ts = a["timestamp"].as_str().unwrap_or("0");
        let b_ts = b["timestamp"].as_str().unwrap_or("0");
        b_ts.cmp(a_ts)
    });
    logs.truncate(MAX_LOG_ITEMS);
    logs
}

fn summarize_metrics(state: &SharedState, service: &str) -> Vec<serde_json::Value> {
    let store = state.metric_store.read();
    let mut metrics = Vec::new();
    let service_key = match store.interner.get("service") {
        Some(service_key) => service_key,
        None => return metrics,
    };
    let service_value = match store.interner.get(service) {
        Some(service_value) => service_value,
        None => return metrics,
    };
    let name_key = match store.interner.get("__name__") {
        Some(name_key) => name_key,
        None => return metrics,
    };

    let Some(service_series) = store.label_index.get(&(service_key, service_value)) else {
        return metrics;
    };

    for &series_id in service_series.ids() {
        let Some(series) = store.series.get(&series_id) else {
            continue;
        };
        let Some((_, name_value)) = series.labels.iter().find(|(k, _)| *k == name_key) else {
            continue;
        };
        let metric_name = store.interner.resolve(name_value).to_string();
        let lower_name = metric_name.to_ascii_lowercase();
        if !lower_name.contains("error") && !lower_name.contains("fail") {
            continue;
        }
        let Some(sample) = series.samples.last() else {
            continue;
        };

        let labels = series
            .labels
            .iter()
            .filter(|(k, _)| *k != name_key)
            .map(|(k, v)| {
                (
                    store.interner.resolve(k).to_string(),
                    store.interner.resolve(v).to_string(),
                )
            })
            .collect::<Vec<_>>();

        metrics.push(json!({
            "name": metric_name,
            "timestampMs": sample.timestamp_ms.to_string(),
            "value": sample.value,
            "labels": labels,
        }));
    }

    metrics.sort_by(|a, b| {
        let a_ts = a["timestampMs"].as_str().unwrap_or("0");
        let b_ts = b["timestampMs"].as_str().unwrap_or("0");
        b_ts.cmp(a_ts)
    });
    metrics.truncate(MAX_METRIC_ITEMS);
    metrics
}

fn summarize_traces(state: &SharedState, service: &str) -> Vec<serde_json::Value> {
    let store = state.trace_store.read();
    let mut traces = Vec::new();
    let Some(service_spur) = store.interner.get(service) else {
        return traces;
    };
    let Some(trace_ids) = store.service_index.get(&service_spur) else {
        return traces;
    };

    for trace_id in trace_ids {
        let Some(spans) = store.get_trace(trace_id) else {
            continue;
        };
        let error_span_count = spans
            .iter()
            .filter(|span| span.service_name == service_spur && span.status == SpanStatus::Error)
            .count();
        if error_span_count == 0 {
            continue;
        }
        let Some(summary) = store.trace_result(trace_id) else {
            continue;
        };
        traces.push(json!({
            "traceID": trace_id
                .iter()
                .map(|byte| format!("{:02x}", byte))
                .collect::<String>(),
            "rootSpanName": summary.root_span_name,
            "durationMs": summary.duration_ns as f64 / 1_000_000.0,
            "errorSpanCount": error_span_count,
        }));
    }

    traces.sort_by(|a, b| {
        let a_duration = a["durationMs"].as_f64().unwrap_or_default();
        let b_duration = b["durationMs"].as_f64().unwrap_or_default();
        b_duration
            .partial_cmp(&a_duration)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    traces.truncate(MAX_TRACE_ITEMS);
    traces
}
