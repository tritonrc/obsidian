//! Prometheus remote write ingestion handler.
//!
//! Accepts snappy-compressed protobuf `WriteRequest` at `POST /api/v1/write`.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use prost::Message;

use crate::store::SharedState;
use crate::store::metric_store::Sample;

/// Prometheus remote write `Sample`.
#[derive(Clone, PartialEq, Message)]
pub struct RemoteSample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

/// Prometheus remote write `Label`.
#[derive(Clone, PartialEq, Message)]
pub struct RemoteLabel {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

/// Prometheus remote write `TimeSeries`.
#[derive(Clone, PartialEq, Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<RemoteLabel>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<RemoteSample>,
}

/// Prometheus remote write `WriteRequest`.
#[derive(Clone, PartialEq, Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

/// Handler for POST /api/v1/write (Prometheus remote write).
pub async fn remote_write_handler(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Determine encoding: snappy is the standard for Prometheus remote write
    let is_snappy = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("snappy"));

    let decoded = if is_snappy {
        match snap::raw::Decoder::new().decompress_vec(&body) {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!("failed to snappy-decompress remote write body: {}", e);
                return StatusCode::BAD_REQUEST;
            }
        }
    } else {
        // Also try snappy by default since Prometheus always sends snappy
        // but may not set the header
        match snap::raw::Decoder::new().decompress_vec(&body) {
            Ok(d) => d,
            Err(_) => body.to_vec(),
        }
    };

    let request = match WriteRequest::decode(decoded.as_slice()) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("failed to decode remote write protobuf: {}", e);
            return StatusCode::BAD_REQUEST;
        }
    };

    // Prepare all data outside the write lock
    type MetricData = (String, Vec<(String, String)>, Vec<Sample>);
    let mut prepared: Vec<MetricData> = Vec::with_capacity(request.timeseries.len());

    for ts in &request.timeseries {
        let mut metric_name = String::new();
        let mut labels: Vec<(String, String)> = Vec::new();

        for label in &ts.labels {
            if label.name == "__name__" {
                metric_name.clone_from(&label.value);
            } else {
                labels.push((label.name.clone(), label.value.clone()));
            }
        }

        if metric_name.is_empty() {
            tracing::debug!("skipping timeseries without __name__ label");
            continue;
        }

        let samples: Vec<Sample> = ts
            .samples
            .iter()
            .map(|s| Sample {
                timestamp_ms: s.timestamp,
                value: s.value,
            })
            .collect();

        prepared.push((metric_name, labels, samples));
    }

    // Acquire write lock and ingest
    let mut store = state.metric_store.write();
    for (name, labels, samples) in prepared {
        store.ingest_samples(&name, labels, samples);
    }

    StatusCode::NO_CONTENT
}
