//! Axum handlers for TraceQL query endpoints.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Deserialize;
use serde_json::{Value, json};

use super::eval::evaluate_traceql;
use super::parser::parse_traceql;
use crate::store::SharedState;
use crate::store::trace_store::SpanStatus;

/// Hint included in TraceQL parse error responses to help agents construct valid queries.
const TRACEQL_HINT: &str = "Example: { resource.service.name = \"myapp\" && status = error }";

/// Default limit of traces returned when no query is provided.
const DEFAULT_RECENT_TRACES_LIMIT: usize = 20;

#[derive(Debug, Deserialize)]
pub struct SearchParams {
    pub q: Option<String>,
    /// Optional start time filter (epoch seconds or nanoseconds).
    pub start: Option<u64>,
    /// Optional end time filter (epoch seconds or nanoseconds).
    pub end: Option<u64>,
    /// Maximum number of traces to return. Defaults to 20 when no query.
    pub limit: Option<usize>,
}

/// Convert a timestamp parameter to nanoseconds.
///
/// Heuristic: values below 1e15 are treated as epoch seconds, values at
/// or above 1e15 are treated as nanoseconds. This threshold corresponds
/// to roughly the year 33,658,145 in seconds but only year 2001 in nanos,
/// so real-world timestamps will always sort into the correct bucket.
fn param_to_ns(val: u64) -> i64 {
    if val >= 1_000_000_000_000_000 {
        // Already nanoseconds
        val as i64
    } else {
        // Seconds — convert
        (val as i64).saturating_mul(1_000_000_000)
    }
}

/// GET /api/search
pub async fn search(
    State(state): State<SharedState>,
    Query(params): Query<SearchParams>,
) -> impl IntoResponse {
    let store = state.trace_store.read();

    // Convert start/end from epoch seconds (or nanoseconds) for filtering.
    let start_ns = params.start.map(param_to_ns);
    let end_ns = params.end.map(param_to_ns);

    let q = params.q.unwrap_or_default();
    if q.is_empty() {
        // No query: return recent traces, optionally filtered by time range.
        let limit = params.limit.unwrap_or(DEFAULT_RECENT_TRACES_LIMIT);
        let mut recent = store.recent_traces(store.traces.len());

        // Apply time range filter
        if start_ns.is_some() || end_ns.is_some() {
            recent.retain(|tr| {
                let trace_end_ns = tr.start_time_ns + tr.duration_ns;
                let after_start = start_ns.is_none_or(|st| trace_end_ns >= st);
                let before_end = end_ns.is_none_or(|en| tr.start_time_ns <= en);
                after_start && before_end
            });
        }

        recent.truncate(limit);

        let traces: Vec<Value> = recent
            .iter()
            .map(|tr| {
                json!({
                    "traceID": hex_encode(&tr.trace_id),
                    "rootServiceName": tr.root_service_name,
                    "rootTraceName": tr.root_span_name,
                    "startTimeUnixNano": tr.start_time_ns.to_string(),
                    "durationMs": tr.duration_ns / 1_000_000,
                    "spanSets": [],
                })
            })
            .collect();

        return (StatusCode::OK, Json(json!({"traces": traces})));
    }

    let expr = match parse_traceql(&q) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": e.to_string(), "hint": TRACEQL_HINT})),
            );
        }
    };

    let results = evaluate_traceql(&expr, &store);

    // Filter traces by time range: keep only those with at least one span
    // whose time window overlaps [start_ns, end_ns].
    let mut results: Vec<_> = results
        .into_iter()
        .filter(|r| {
            r.matched_spans.iter().any(|s| {
                let span_end = s.start_time_ns + s.duration_ns;
                let after_start = start_ns.is_none_or(|st| span_end >= st);
                let before_end = end_ns.is_none_or(|en| s.start_time_ns <= en);
                after_start && before_end
            })
        })
        .collect();

    if let Some(limit) = params.limit {
        results.truncate(limit);
    }

    let traces: Vec<Value> = results
        .iter()
        .map(|r| {
            let trace_id = hex::encode_upper(&r.trace_id);
            // Find the actual root span from the trace store, not the first matched span
            let root_info = store.trace_result(&r.trace_id);
            let (root_service, root_name, root_start_ns, root_duration_ns) = match &root_info {
                Some(tr) => (
                    tr.root_service_name.as_str(),
                    tr.root_span_name.as_str(),
                    tr.start_time_ns,
                    tr.duration_ns,
                ),
                None => {
                    // Fallback to first matched span if trace_result unavailable
                    let first = r.matched_spans.first();
                    (
                        first.map(|s| s.service_name.as_str()).unwrap_or(""),
                        first.map(|s| s.name.as_str()).unwrap_or(""),
                        first.map(|s| s.start_time_ns).unwrap_or(0),
                        first.map(|s| s.duration_ns).unwrap_or(0),
                    )
                }
            };
            json!({
                "traceID": trace_id.to_lowercase(),
                "rootServiceName": root_service,
                "rootTraceName": root_name,
                "startTimeUnixNano": root_start_ns.to_string(),
                "durationMs": root_duration_ns / 1_000_000,
                "spanSets": [{
                    "spans": r.matched_spans.iter().map(|s| {
                        json!({
                            "spanID": hex_encode(&s.span_id),
                            "name": s.name,
                            "serviceName": s.service_name,
                            "startTimeUnixNano": s.start_time_ns.to_string(),
                            "durationNanos": s.duration_ns.to_string(),
                            "status": match s.status {
                                SpanStatus::Ok => "ok",
                                SpanStatus::Error => "error",
                                SpanStatus::Unset => "unset",
                            },
                        })
                    }).collect::<Vec<Value>>(),
                    "matched": r.matched_spans.len(),
                }],
            })
        })
        .collect();

    (
        StatusCode::OK,
        Json(json!({
            "traces": traces,
        })),
    )
}

/// GET /api/traces/:trace_id
pub async fn get_trace(
    State(state): State<SharedState>,
    Path(trace_id_str): Path<String>,
) -> impl IntoResponse {
    let trace_id = match parse_trace_id(&trace_id_str) {
        Some(id) => id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid trace ID"})),
            );
        }
    };

    let store = state.trace_store.read();
    match store.get_trace(&trace_id) {
        Some(spans) => {
            // Group spans by service_name, preserving insertion order via Vec
            let mut service_order: Vec<String> = Vec::new();
            let mut service_spans: std::collections::HashMap<String, Vec<Value>> =
                std::collections::HashMap::new();

            for span in spans {
                let service_name = store.resolve(&span.service_name).to_string();
                let attrs: Vec<Value> = span
                    .attributes
                    .iter()
                    .map(|(k, v)| {
                        json!({
                            "key": store.resolve(k),
                            "value": {
                                "stringValue": store.resolve_attribute_value(v),
                            }
                        })
                    })
                    .collect();

                let span_value = json!({
                    "traceId": hex_encode(&span.trace_id),
                    "spanId": hex_encode(&span.span_id),
                    "parentSpanId": span.parent_span_id.as_ref().map(|p| hex_encode(p)).unwrap_or_default(),
                    "name": store.resolve(&span.name),
                    "kind": 1,
                    "startTimeUnixNano": span.start_time_ns.to_string(),
                    "endTimeUnixNano": (span.start_time_ns + span.duration_ns).to_string(),
                    "status": {
                        "code": match span.status {
                            SpanStatus::Unset => 0,
                            SpanStatus::Ok => 1,
                            SpanStatus::Error => 2,
                        }
                    },
                    "attributes": attrs,
                });

                if !service_spans.contains_key(&service_name) {
                    service_order.push(service_name.clone());
                }
                service_spans
                    .entry(service_name)
                    .or_default()
                    .push(span_value);
            }

            let batches: Vec<Value> = service_order
                .iter()
                .map(|svc| {
                    json!({
                        "resource": {
                            "attributes": [{
                                "key": "service.name",
                                "value": {"stringValue": svc}
                            }]
                        },
                        "scopeSpans": [{
                            "spans": service_spans.get(svc).unwrap_or(&Vec::new()),
                        }]
                    })
                })
                .collect();

            (
                StatusCode::OK,
                Json(json!({
                    "batches": batches
                })),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "trace not found"})),
        ),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>()
}

fn parse_trace_id(s: &str) -> Option<[u8; 16]> {
    let s = s.trim();
    if s.len() != 32 {
        return None;
    }
    let mut bytes = [0u8; 16];
    for i in 0..16 {
        bytes[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(bytes)
}

// Inline hex encoding module (avoid dependency)
mod hex {
    pub fn encode_upper(bytes: &[u8]) -> String {
        bytes
            .iter()
            .map(|b| format!("{:02X}", b))
            .collect::<String>()
    }
}
