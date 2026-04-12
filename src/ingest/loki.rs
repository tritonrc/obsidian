//! Loki push API handler for log ingestion.
//!
//! Accepts JSON and snappy-compressed JSON log pushes.

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use serde::Deserialize;

use crate::store::SharedState;
use crate::store::log_store::LogEntry;

/// Loki push JSON format.
#[derive(Debug, Deserialize)]
pub struct LokiPushRequest {
    pub streams: Vec<LokiStream>,
}

/// A single stream in the Loki push format.
///
/// Loki push values are arrays of `[timestamp_ns, line]` or `[timestamp_ns, line, metadata]`.
/// We accept both forms.
#[derive(Debug, Deserialize)]
pub struct LokiStream {
    pub stream: serde_json::Map<String, serde_json::Value>,
    #[serde(deserialize_with = "deserialize_log_values")]
    pub values: Vec<(String, String)>,
}

fn deserialize_log_values<'de, D>(deserializer: D) -> Result<Vec<(String, String)>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw: Vec<Vec<serde_json::Value>> = Vec::deserialize(deserializer)?;
    Ok(raw
        .into_iter()
        .filter_map(|entry| {
            if entry.len() >= 2 {
                let ts = match &entry[0] {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    _ => return None,
                };
                let line = match &entry[1] {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                Some((ts, line))
            } else {
                None
            }
        })
        .collect())
}

/// Handler for POST /loki/api/v1/push.
pub async fn push_handler(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json");

    let request = if content_type.contains("application/x-protobuf")
        || content_type.contains("application/x-snappy")
    {
        // Snappy-compressed JSON path.
        // Loki clients typically send snappy-compressed JSON as application/x-protobuf.
        // Native Loki protobuf is NOT supported — if snappy-JSON decode fails,
        // report that clearly.
        match decode_snappy_json(&body) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    "failed to decode Loki push (expected snappy-compressed JSON): {}",
                    e
                );
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": format!("failed to decode Loki push: {}. Note: native Loki protobuf is not supported, use JSON or snappy-compressed JSON.", e)
                    })),
                ).into_response();
            }
        }
    } else {
        // JSON path
        match serde_json::from_slice::<LokiPushRequest>(&body) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("failed to parse Loki push JSON: {}", e);
                return StatusCode::BAD_REQUEST.into_response();
            }
        }
    };

    let (streams, entries) = ingest_loki_push(&state, request);
    Json(serde_json::json!({
        "accepted": {
            "streams": streams,
            "entries": entries,
        }
    }))
    .into_response()
}

/// Decompress snappy body and parse as JSON.
///
/// Loki's native protobuf push format uses its own proto definitions (not OTLP).
/// We don't implement that -- this path only handles snappy-compressed JSON.
fn decode_snappy_json(body: &[u8]) -> Result<LokiPushRequest, anyhow::Error> {
    let decompressed = super::decode_snappy_body(body)
        .map_err(|e| anyhow::anyhow!("failed to decode snappy body: {}", e))?;

    serde_json::from_slice(&decompressed)
        .map_err(|e| anyhow::anyhow!("failed to parse decompressed push: {}", e))
}

/// Ingest a Loki push request. Returns `(stream_count, entry_count)`.
fn ingest_loki_push(state: &SharedState, request: LokiPushRequest) -> (usize, usize) {
    type StreamData = (Vec<(String, String)>, Vec<LogEntry>);
    let prepared: Vec<StreamData> = request
        .streams
        .into_iter()
        .map(|stream| {
            let labels: Vec<(String, String)> = stream
                .stream
                .into_iter()
                .map(|(k, v)| {
                    let val = match v {
                        serde_json::Value::String(s) => s,
                        other => other.to_string(),
                    };
                    (k, val)
                })
                .collect();
            let entries: Vec<LogEntry> = stream
                .values
                .into_iter()
                .filter_map(|(ts_str, line)| {
                    let timestamp_ns: i64 = ts_str.parse().ok()?;
                    Some(LogEntry { timestamp_ns, line })
                })
                .collect();
            (labels, entries)
        })
        .collect();

    let stream_count = prepared.len();
    let entry_count: usize = prepared.iter().map(|(_, entries)| entries.len()).sum();

    let mut store = state.log_store.write();
    for (labels, entries) in prepared {
        store.ingest_stream(labels, entries);
    }

    (stream_count, entry_count)
}
