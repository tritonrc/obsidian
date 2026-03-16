//! Ingestion pipeline: Loki push, OTLP metrics, OTLP traces, OTLP logs, remote write.

use std::borrow::Cow;
use std::io::Read;

use axum::body::Bytes;
use axum::http::HeaderMap;

pub mod label;
pub mod loki;
pub mod otlp_logs;
pub mod otlp_metrics;
pub mod otlp_traces;
pub mod remote_write;

/// Decode a request body, decompressing gzip if the `content-encoding` header indicates it.
pub fn decode_body<'a>(headers: &HeaderMap, body: &'a Bytes) -> Result<Cow<'a, [u8]>, String> {
    let is_gzip = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("gzip"));

    if is_gzip {
        let mut decoder = flate2::read::GzDecoder::new(body.as_ref());
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| format!("gzip decompression failed: {}", e))?;
        Ok(Cow::Owned(decompressed))
    } else {
        Ok(Cow::Borrowed(body.as_ref()))
    }
}

/// Check if the Content-Type header indicates JSON encoding.
pub fn is_json_content_type(headers: &HeaderMap) -> bool {
    headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("application/json"))
}
