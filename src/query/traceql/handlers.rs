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

#[derive(Debug, Deserialize)]
pub struct SearchParams {
    pub q: String,
}

/// GET /api/search
pub async fn search(
    State(state): State<SharedState>,
    Query(params): Query<SearchParams>,
) -> impl IntoResponse {
    let expr = match parse_traceql(&params.q) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": e.to_string()})),
            );
        }
    };

    let store = state.trace_store.read();
    let results = evaluate_traceql(&expr, &store);

    let traces: Vec<Value> = results
        .iter()
        .map(|r| {
            let trace_id = hex::encode_upper(&r.trace_id);
            // Find the actual root span from the trace store, not the first matched span
            let root_info = store.trace_result(&r.trace_id);
            let (root_service, root_name, start_ns, duration_ns) = match &root_info {
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
                "startTimeUnixNano": start_ns.to_string(),
                "durationMs": duration_ns / 1_000_000,
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
            let span_values: Vec<Value> = spans
                .iter()
                .map(|span| {
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

                    json!({
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
                    })
                })
                .collect();

            let service_name = spans
                .first()
                .map(|s| store.resolve(&s.service_name).to_string())
                .unwrap_or_default();

            (
                StatusCode::OK,
                Json(json!({
                    "batches": [{
                        "resource": {
                            "attributes": [{
                                "key": "service.name",
                                "value": {"stringValue": service_name}
                            }]
                        },
                        "scopeSpans": [{
                            "spans": span_values,
                        }]
                    }]
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
