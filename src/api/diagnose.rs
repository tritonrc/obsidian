//! Health assessment endpoint.
//!
//! GET /api/v1/diagnose?service=<name> returns a detailed JSON health assessment
//! for a single service including health score, slowest traces, error trends,
//! recent errors, and suggested queries.
//!
//! GET /api/v1/diagnose (no service param) returns a global health overview:
//! a ranked list of all services by health severity.

use axum::Json;
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use rustc_hash::FxHashSet;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::store::SharedState;
use crate::store::log_store::{LabelMatchOp, LabelMatcher};
use crate::store::trace_store::SpanStatus;

/// Query parameters for the diagnose endpoint.
#[derive(Debug, Deserialize)]
pub struct DiagnoseParams {
    pub service: Option<String>,
}

/// Return the current wall-clock time in nanoseconds since the Unix epoch.
fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

/// One hour in nanoseconds — the default lookback window for log scans.
const ONE_HOUR_NS: i64 = 3_600 * 1_000_000_000;

/// Compute a simplified health score and top issue for a service.
/// Returns (health_score, top_issue).
fn compute_service_health(state: &SharedState, service: &str) -> (f64, String) {
    let now = now_ns();
    let lookback_start = now.saturating_sub(ONE_HOUR_NS);

    // --- Error rate from logs (bounded to last 1 hour) ---
    let error_rate_pct = {
        let store = state.log_store.read();
        let all_matchers = vec![LabelMatcher {
            name: "service".into(),
            op: LabelMatchOp::Eq,
            value: service.to_string(),
        }];
        let all_stream_ids = store.query_streams(&all_matchers);
        let total: usize = all_stream_ids
            .iter()
            .map(|sid| store.get_entries(*sid, lookback_start, now).len())
            .sum();

        // Error streams are a subset — filter from already-queried all_stream_ids
        // is not possible because query_streams also filters by label, but we can
        // query once and reuse the IDs.
        let error_matchers = vec![
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
        let error_stream_ids = store.query_streams(&error_matchers);
        let error_count: usize = error_stream_ids
            .iter()
            .map(|sid| store.get_entries(*sid, lookback_start, now).len())
            .sum();

        if total > 0 {
            error_count as f64 / total as f64 * 100.0
        } else {
            0.0
        }
    };

    // --- Span error ratio ---
    let span_error_ratio = {
        let store = state.trace_store.read();
        let trace_ids = store.traces_for_service(service);
        let mut total_spans: usize = 0;
        let mut error_spans: usize = 0;

        for tid in &trace_ids {
            if let Some(spans) = store.get_trace(tid) {
                total_spans += spans.len();
                error_spans += spans
                    .iter()
                    .filter(|s| s.status == SpanStatus::Error)
                    .count();
            }
        }

        if total_spans > 0 {
            error_spans as f64 / total_spans as f64
        } else {
            0.0
        }
    };

    // --- Health score ---
    let mut health_score: f64 = 100.0;
    health_score -= error_rate_pct.min(40.0);
    health_score -= (span_error_ratio * 30.0).min(30.0);
    health_score = health_score.clamp(0.0, 100.0);

    // --- Top issue ---
    let top_issue = if error_rate_pct > 10.0 {
        format!("High log error rate: {:.1}%", error_rate_pct)
    } else if span_error_ratio > 0.1 {
        format!("Span error ratio: {:.1}%", span_error_ratio * 100.0)
    } else if error_rate_pct > 0.0 {
        format!("Log error rate: {:.1}%", error_rate_pct)
    } else if span_error_ratio > 0.0 {
        format!("Span error ratio: {:.1}%", span_error_ratio * 100.0)
    } else {
        "No issues detected".to_string()
    };

    (health_score, top_issue)
}

/// Collect all known service names across all stores.
fn collect_all_services(state: &SharedState) -> Vec<String> {
    let mut services: FxHashSet<String> = FxHashSet::default();

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

    services.into_iter().collect()
}

/// GET /api/v1/diagnose — global overview or per-service health assessment.
pub async fn diagnose(
    State(state): State<SharedState>,
    Query(params): Query<DiagnoseParams>,
) -> impl IntoResponse {
    match params.service {
        Some(ref s) if !s.is_empty() => diagnose_service(&state, s),
        _ => diagnose_global(&state),
    }
}

/// Return a global health overview: ranked list of all services by severity.
fn diagnose_global(state: &SharedState) -> axum::response::Response {
    let service_names = collect_all_services(state);

    let mut entries: Vec<Value> = service_names
        .iter()
        .map(|name| {
            let (health_score, top_issue) = compute_service_health(state, name);
            json!({
                "service": name,
                "health_score": health_score,
                "top_issue": top_issue,
            })
        })
        .collect();

    // Sort by health_score ascending (worst first)
    entries.sort_by(|a, b| {
        let sa = a["health_score"].as_f64().unwrap_or(100.0);
        let sb = b["health_score"].as_f64().unwrap_or(100.0);
        sa.partial_cmp(&sb).unwrap_or(std::cmp::Ordering::Equal)
    });

    Json(json!({
        "services": entries,
    }))
    .into_response()
}

/// Return a detailed health assessment for a single service.
fn diagnose_service(state: &SharedState, service: &str) -> axum::response::Response {
    let now = now_ns();
    let lookback_start = now.saturating_sub(ONE_HOUR_NS);
    let five_min_ns: i64 = 5 * 60 * 1_000_000_000;

    // --- All log data in a single lock acquisition ---
    // We query streams once for all-service and once for error-service,
    // then reuse those cached stream IDs for error_rate, recent_errors, and error_trend.
    let (error_rate_pct, recent_errors, current_5m, previous_5m) = {
        let store = state.log_store.read();

        // Query all streams for this service (cached)
        let all_matchers = vec![LabelMatcher {
            name: "service".into(),
            op: LabelMatchOp::Eq,
            value: service.to_string(),
        }];
        let all_stream_ids = store.query_streams(&all_matchers);

        // Query error streams for this service (cached)
        let error_matchers = vec![
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
        let error_stream_ids = store.query_streams(&error_matchers);

        // Error rate: bounded to last 1 hour
        let total: usize = all_stream_ids
            .iter()
            .map(|sid| store.get_entries(*sid, lookback_start, now).len())
            .sum();
        let error_count: usize = error_stream_ids
            .iter()
            .map(|sid| store.get_entries(*sid, lookback_start, now).len())
            .sum();
        let rate = if total > 0 {
            error_count as f64 / total as f64 * 100.0
        } else {
            0.0
        };

        // Recent errors: bounded to last 1 hour, take top 10 by timestamp desc
        let mut errors: Vec<Value> = Vec::new();
        for sid in &error_stream_ids {
            let entries = store.get_entries(*sid, lookback_start, now);
            for entry in entries {
                errors.push(json!({
                    "timestamp": entry.timestamp_ns.to_string(),
                    "line": entry.line,
                }));
            }
        }
        errors.sort_by(|a, b| {
            let ta = a["timestamp"].as_str().unwrap_or("0");
            let tb = b["timestamp"].as_str().unwrap_or("0");
            tb.cmp(ta)
        });
        errors.truncate(10);

        // Error trend: reuse cached error_stream_ids
        let mut current = 0usize;
        let mut previous = 0usize;
        for sid in &error_stream_ids {
            let entries = store.get_entries(*sid, now.saturating_sub(five_min_ns), now);
            current += entries.len();
            let prev_entries = store.get_entries(
                *sid,
                now.saturating_sub(2 * five_min_ns),
                now.saturating_sub(five_min_ns),
            );
            previous += prev_entries.len();
        }

        (rate, errors, current, previous)
    };

    let direction = if current_5m > previous_5m {
        "increasing"
    } else if current_5m < previous_5m {
        "decreasing"
    } else {
        "stable"
    };

    // --- All trace summaries for the service ---
    let (slowest_traces, span_error_ratio, total_span_count, error_span_count, trace_durations) = {
        let store = state.trace_store.read();
        let trace_ids = store.traces_for_service(service);
        let mut summaries: Vec<Value> = Vec::new();
        let mut total_spans: usize = 0;
        let mut error_spans: usize = 0;
        let mut durations: Vec<i64> = Vec::new();

        for tid in &trace_ids {
            if let Some(spans) = store.get_trace(tid) {
                total_spans += spans.len();
                error_spans += spans
                    .iter()
                    .filter(|s| s.status == SpanStatus::Error)
                    .count();
            }
            if let Some(result) = store.trace_result(tid) {
                let hex_id: String = tid.iter().map(|b| format!("{:02x}", b)).collect();
                durations.push(result.duration_ns);
                summaries.push(json!({
                    "traceID": hex_id,
                    "rootSpanName": result.root_span_name,
                    "durationMs": result.duration_ns as f64 / 1_000_000.0,
                    "spanCount": result.span_count,
                }));
            }
        }

        // Sort by duration descending, take top 5
        summaries.sort_by(|a, b| {
            let da = a["durationMs"].as_f64().unwrap_or(0.0);
            let db = b["durationMs"].as_f64().unwrap_or(0.0);
            db.partial_cmp(&da).unwrap_or(std::cmp::Ordering::Equal)
        });
        summaries.truncate(5);

        let ratio = if total_spans > 0 {
            error_spans as f64 / total_spans as f64
        } else {
            0.0
        };

        (summaries, ratio, total_spans, error_spans, durations)
    };

    // --- Latency p99 (approximate) ---
    let latency_p99_ms = if trace_durations.is_empty() {
        0.0
    } else {
        let mut sorted = trace_durations.clone();
        sorted.sort_unstable();
        let idx = ((sorted.len() as f64 * 0.99).ceil() as usize).min(sorted.len()) - 1;
        sorted[idx] as f64 / 1_000_000.0
    };

    // --- Health score (computed inline from data already gathered) ---
    let mut health_score: f64 = 100.0;

    // Deduct for error rate
    health_score -= error_rate_pct.min(40.0);

    // Deduct for span error ratio
    health_score -= (span_error_ratio * 30.0).min(30.0);

    // Deduct for high p99 latency (>1s starts deducting)
    if latency_p99_ms > 1000.0 {
        let deduction = ((latency_p99_ms - 1000.0) / 1000.0 * 10.0).min(20.0);
        health_score -= deduction;
    }

    health_score = health_score.clamp(0.0, 100.0);

    // --- Key metrics ---
    let key_metrics = {
        let store = state.metric_store.read();
        let matchers = vec![LabelMatcher {
            name: "service".into(),
            op: LabelMatchOp::Eq,
            value: service.to_string(),
        }];
        let series_ids = store.select_series(&matchers);
        let name_key = store.interner.get("__name__");
        let mut metrics: Vec<Value> = Vec::new();

        for sid in series_ids {
            if let Some(series) = store.series.get(&sid) {
                let metric_name = name_key.and_then(|nk| {
                    series
                        .labels
                        .iter()
                        .find(|(k, _)| *k == nk)
                        .map(|(_, v)| store.interner.resolve(v).to_string())
                });
                let name = match metric_name {
                    Some(n) => n,
                    None => continue,
                };
                let mut label_map = serde_json::Map::new();
                for (k, v) in &series.labels {
                    let key = store.interner.resolve(k).to_string();
                    if key == "__name__" {
                        continue;
                    }
                    let val = store.interner.resolve(v).to_string();
                    label_map.insert(key, Value::String(val));
                }
                // Build sparkline from recent samples (last 10)
                let sparkline: Vec<f64> = series
                    .samples
                    .iter()
                    .rev()
                    .take(10)
                    .map(|s| s.value)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect();
                metrics.push(json!({
                    "name": name,
                    "labels": label_map,
                    "sparkline": sparkline,
                }));
            }
        }
        metrics
    };

    // --- Suggested queries ---
    let mut suggested_queries: Vec<Value> = Vec::new();

    if !recent_errors.is_empty() {
        suggested_queries.push(json!({
            "type": "logql",
            "query": format!(r#"{{service="{}", level="error"}}"#, service),
            "reason": "Recent error logs detected",
        }));
    }

    if span_error_ratio > 0.0 {
        suggested_queries.push(json!({
            "type": "traceql",
            "query": format!(r#"{{ resource.service.name = "{}" && status = error }}"#, service),
            "reason": "Span errors detected",
        }));
    }

    if latency_p99_ms > 1000.0 {
        suggested_queries.push(json!({
            "type": "traceql",
            "query": format!(r#"{{ resource.service.name = "{}" && duration > 1s }}"#, service),
            "reason": "High latency traces detected",
        }));
    }

    // Always suggest a general service overview
    if suggested_queries.is_empty() {
        suggested_queries.push(json!({
            "type": "logql",
            "query": format!(r#"{{service="{}"}}"#, service),
            "reason": "General service log overview",
        }));
    }

    let _ = (total_span_count, error_span_count);

    Json(json!({
        "service": service,
        "health_score": health_score,
        "health_factors": {
            "error_rate_pct": error_rate_pct,
            "latency_p99_ms": latency_p99_ms,
            "span_error_ratio": span_error_ratio,
        },
        "slowest_traces": slowest_traces,
        "error_trend": [json!({
            "current_5m": current_5m,
            "previous_5m": previous_5m,
            "direction": direction,
        })],
        "recent_errors": recent_errors,
        "key_metrics": key_metrics,
        "suggested_queries": suggested_queries,
    }))
    .into_response()
}
