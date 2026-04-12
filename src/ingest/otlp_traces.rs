//! OTLP/HTTP traces ingestion handler.
//!
//! Decodes `ExportTraceServiceRequest` protobuf and stores spans.

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value;
use prost::Message;
use rustc_hash::FxHashSet;
use smallvec::SmallVec;

use super::label::extract_resource_labels;
use super::{decode_body, is_json_content_type};
use crate::store::SharedState;
use crate::store::trace_store::{AttributeValue, Span, SpanStatus};

#[derive(Debug, Clone)]
enum PreparedAttributeValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

#[derive(Debug, Clone)]
struct PreparedSpan {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_span_id: Option<[u8; 8]>,
    name: String,
    service_name: String,
    start_time_ns: i64,
    duration_ns: i64,
    status: SpanStatus,
    attributes: SmallVec<[(String, PreparedAttributeValue); 8]>,
}

/// Handler for POST /v1/traces.
///
/// Accepts both protobuf (`application/x-protobuf`, default) and JSON
/// (`application/json`) encoded `ExportTraceServiceRequest` bodies.
pub async fn traces_handler(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let body = match decode_body(&headers, &body) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!("failed to decode OTLP traces body: {}", e);
            return StatusCode::BAD_REQUEST.into_response();
        }
    };

    let request = if is_json_content_type(&headers) {
        match serde_json::from_slice::<ExportTraceServiceRequest>(body.as_ref()) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("failed to decode OTLP traces JSON: {}", e);
                return StatusCode::BAD_REQUEST.into_response();
            }
        }
    } else {
        match ExportTraceServiceRequest::decode(body.as_ref()) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("failed to decode OTLP traces: {}", e);
                return StatusCode::BAD_REQUEST.into_response();
            }
        }
    };

    let mut prepared_batches: Vec<Vec<PreparedSpan>> = Vec::new();
    let mut trace_ids = FxHashSet::default();
    let mut total_spans: usize = 0;

    for resource_spans in &request.resource_spans {
        let resource_labels = extract_resource_labels(&resource_spans.resource);
        let service_name = resource_labels
            .iter()
            .find(|(k, _)| k == "service.name")
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| "unknown".to_string());

        let resource_attrs: SmallVec<[(String, PreparedAttributeValue); 8]> = resource_labels
            .iter()
            .map(|(k, v)| {
                let key = format!("resource.{}", k);
                let val = PreparedAttributeValue::String(v.clone());
                (key, val)
            })
            .collect();

        for scope_spans in &resource_spans.scope_spans {
            let mut spans = Vec::with_capacity(scope_spans.spans.len());

            for otlp_span in &scope_spans.spans {
                let trace_id: [u8; 16] = match otlp_span.trace_id.as_slice().try_into() {
                    Ok(id) if id != [0u8; 16] => id,
                    _ => {
                        tracing::warn!(
                            "skipping span with invalid or zero trace_id (length: {})",
                            otlp_span.trace_id.len()
                        );
                        continue;
                    }
                };
                let span_id: [u8; 8] = match otlp_span.span_id.as_slice().try_into() {
                    Ok(id) if id != [0u8; 8] => id,
                    _ => {
                        tracing::warn!(
                            "skipping span with invalid or zero span_id (length: {})",
                            otlp_span.span_id.len()
                        );
                        continue;
                    }
                };
                let parent_span_id = if otlp_span.parent_span_id.is_empty()
                    || otlp_span.parent_span_id.iter().all(|&b| b == 0)
                {
                    None
                } else {
                    otlp_span.parent_span_id.as_slice().try_into().ok()
                };

                let status = match &otlp_span.status {
                    Some(s) => match s.code {
                        0 => SpanStatus::Unset,
                        1 => SpanStatus::Ok,
                        2 => SpanStatus::Error,
                        _ => SpanStatus::Unset,
                    },
                    None => SpanStatus::Unset,
                };

                let duration_ns =
                    otlp_span.end_time_unix_nano as i64 - otlp_span.start_time_unix_nano as i64;

                let mut attributes = resource_attrs.clone();

                // Add span attributes
                for attr in &otlp_span.attributes {
                    if let Some(val) = &attr.value {
                        let key = format!("span.{}", attr.key);
                        if let Some(av) = convert_any_value(val) {
                            attributes.push((key, av));
                        }
                    }
                }

                trace_ids.insert(trace_id);
                spans.push(PreparedSpan {
                    trace_id,
                    span_id,
                    parent_span_id,
                    name: otlp_span.name.clone(),
                    service_name: service_name.clone(),
                    start_time_ns: otlp_span.start_time_unix_nano as i64,
                    duration_ns,
                    status,
                    attributes,
                });
            }

            total_spans += spans.len();
            prepared_batches.push(spans);
        }
    }

    let mut store = state.trace_store.write();
    for prepared_batch in prepared_batches {
        let spans: Vec<Span> = prepared_batch
            .into_iter()
            .map(|prepared| intern_prepared_span(&mut store, prepared))
            .collect();
        store.ingest_spans(spans);
    }

    Json(serde_json::json!({
        "accepted": {
            "traces": trace_ids.len(),
            "spans": total_spans,
        }
    }))
    .into_response()
}

fn convert_any_value(
    val: &opentelemetry_proto::tonic::common::v1::AnyValue,
) -> Option<PreparedAttributeValue> {
    match &val.value {
        Some(any_value::Value::StringValue(s)) => Some(PreparedAttributeValue::String(s.clone())),
        Some(any_value::Value::IntValue(i)) => Some(PreparedAttributeValue::Int(*i)),
        Some(any_value::Value::DoubleValue(f)) => Some(PreparedAttributeValue::Float(*f)),
        Some(any_value::Value::BoolValue(b)) => Some(PreparedAttributeValue::Bool(*b)),
        _ => None,
    }
}

fn intern_prepared_span(store: &mut crate::store::TraceStore, prepared: PreparedSpan) -> Span {
    let name = store.interner.get_or_intern(&prepared.name);
    let service_name = store.interner.get_or_intern(&prepared.service_name);
    let attributes = prepared
        .attributes
        .into_iter()
        .map(|(key, value)| {
            let key = store.interner.get_or_intern(key);
            let value = match value {
                PreparedAttributeValue::String(s) => {
                    AttributeValue::String(store.interner.get_or_intern(s))
                }
                PreparedAttributeValue::Int(i) => AttributeValue::Int(i),
                PreparedAttributeValue::Float(f) => AttributeValue::Float(f),
                PreparedAttributeValue::Bool(b) => AttributeValue::Bool(b),
            };
            (key, value)
        })
        .collect();

    Span {
        trace_id: prepared.trace_id,
        span_id: prepared.span_id,
        parent_span_id: prepared.parent_span_id,
        name,
        service_name,
        start_time_ns: prepared.start_time_ns,
        duration_ns: prepared.duration_ns,
        status: prepared.status,
        attributes,
    }
}
