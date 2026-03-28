//! OTLP/HTTP logs ingestion handler.
//!
//! Decodes `ExportLogsServiceRequest` protobuf and stores log entries.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value;
use prost::Message;

use super::label::{extract_resource_labels, promote_service_name};
use super::{decode_body, is_json_content_type};
use crate::store::SharedState;
use crate::store::log_store::LogEntry;

/// Handler for POST /v1/logs.
pub async fn logs_handler(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let body = match decode_body(&headers, &body) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!("failed to decode OTLP logs body: {}", e);
            return StatusCode::BAD_REQUEST;
        }
    };

    let request = if is_json_content_type(&headers) {
        match serde_json::from_slice::<ExportLogsServiceRequest>(body.as_ref()) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("failed to decode OTLP logs JSON: {}", e);
                return StatusCode::BAD_REQUEST;
            }
        }
    } else {
        match ExportLogsServiceRequest::decode(body.as_ref()) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("failed to decode OTLP logs: {}", e);
                return StatusCode::BAD_REQUEST;
            }
        }
    };

    // Prepare all ingestion data outside the write lock.
    type LogBatch = (Vec<(String, String)>, Vec<LogEntry>);
    let mut prepared: Vec<LogBatch> = Vec::new();

    for resource_logs in &request.resource_logs {
        let mut resource_labels = extract_resource_labels(&resource_logs.resource);
        promote_service_name(&mut resource_labels);

        for scope_logs in &resource_logs.scope_logs {
            for log_record in &scope_logs.log_records {
                let mut labels = resource_labels.clone();

                // Map severity number to a "level" label.
                let level = severity_to_level(log_record.severity_number);
                labels.push(("level".to_string(), level.to_string()));

                // Extract the log line from the body field.
                let line = match &log_record.body {
                    Some(val) => any_value_to_string(val),
                    None => String::new(),
                };

                let timestamp_ns = if log_record.time_unix_nano == 0 {
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as i64
                } else {
                    log_record.time_unix_nano as i64
                };

                let entry = LogEntry {
                    timestamp_ns,
                    line,
                };

                prepared.push((labels, vec![entry]));
            }
        }
    }

    // Acquire write lock and ingest.
    let entry_count = prepared.len();
    let mut store = state.log_store.write();
    for (labels, entries) in prepared {
        store.ingest_stream(labels, entries);
    }

    tracing::debug!(entries = entry_count, "ingested OTLP logs");
    StatusCode::NO_CONTENT
}

/// Map OTLP severity number to a human-readable level string.
fn severity_to_level(severity_number: i32) -> &'static str {
    match severity_number {
        1..=4 => "trace",
        5..=8 => "debug",
        9..=12 => "info",
        13..=16 => "warn",
        17..=20 => "error",
        21..=24 => "fatal",
        _ => "unknown",
    }
}

/// Convert an AnyValue to its string representation.
fn any_value_to_string(val: &opentelemetry_proto::tonic::common::v1::AnyValue) -> String {
    match &val.value {
        Some(any_value::Value::StringValue(s)) => s.clone(),
        Some(any_value::Value::IntValue(i)) => i.to_string(),
        Some(any_value::Value::DoubleValue(f)) => f.to_string(),
        Some(any_value::Value::BoolValue(b)) => b.to_string(),
        _ => String::new(),
    }
}
