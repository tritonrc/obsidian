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

    let mut store = state.trace_store.write();
    let mut trace_ids = FxHashSet::default();
    let mut total_spans: usize = 0;

    for resource_spans in &request.resource_spans {
        let resource_labels = extract_resource_labels(&resource_spans.resource);
        let service_name = resource_labels
            .iter()
            .find(|(k, _)| k == "service.name")
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| "unknown".to_string());

        // Pre-intern resource attributes once per resource_spans block
        let resource_attrs: SmallVec<[(lasso::Spur, AttributeValue); 8]> = resource_labels
            .iter()
            .map(|(k, v)| {
                let key = store.interner.get_or_intern(format!("resource.{}", k));
                let val = AttributeValue::String(store.interner.get_or_intern(v));
                (key, val)
            })
            .collect();

        let service_spur = store.interner.get_or_intern(&service_name);

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

                let name_spur = store.interner.get_or_intern(&otlp_span.name);

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

                let mut attributes: SmallVec<[(lasso::Spur, AttributeValue); 8]> =
                    resource_attrs.clone();

                // Add span attributes
                for attr in &otlp_span.attributes {
                    if let Some(val) = &attr.value {
                        let key = store.interner.get_or_intern(format!("span.{}", attr.key));
                        if let Some(av) = convert_any_value(&mut store.interner, val) {
                            attributes.push((key, av));
                        }
                    }
                }

                trace_ids.insert(trace_id);
                spans.push(Span {
                    trace_id,
                    span_id,
                    parent_span_id,
                    name: name_spur,
                    service_name: service_spur,
                    start_time_ns: otlp_span.start_time_unix_nano as i64,
                    duration_ns,
                    status,
                    attributes,
                });
            }

            total_spans += spans.len();
            store.ingest_spans(spans);
        }
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
    interner: &mut lasso::Rodeo,
    val: &opentelemetry_proto::tonic::common::v1::AnyValue,
) -> Option<AttributeValue> {
    match &val.value {
        Some(any_value::Value::StringValue(s)) => {
            Some(AttributeValue::String(interner.get_or_intern(s)))
        }
        Some(any_value::Value::IntValue(i)) => Some(AttributeValue::Int(*i)),
        Some(any_value::Value::DoubleValue(f)) => Some(AttributeValue::Float(*f)),
        Some(any_value::Value::BoolValue(b)) => Some(AttributeValue::Bool(*b)),
        _ => None,
    }
}
