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

/// Maximum decompressed body size: 64 MiB.
pub const MAX_DECOMPRESSED_SIZE: usize = 64 * 1024 * 1024;

/// Errors that can occur when decoding an ingestion request body.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    /// Gzip decompression of the request body failed.
    #[error("gzip decompression failed: {0}")]
    GzipDecompression(#[from] std::io::Error),

    /// Snappy decompression of the request body failed.
    #[error("snappy decompression failed: {0}")]
    SnappyDecompression(#[from] snap::Error),

    /// Decompressed body exceeds the maximum allowed size.
    #[error("decompressed body exceeds maximum size of 64 MiB")]
    PayloadTooLarge,
}

/// Decode a request body, decompressing gzip if the `content-encoding` header indicates it.
pub fn decode_body<'a>(headers: &HeaderMap, body: &'a Bytes) -> Result<Cow<'a, [u8]>, IngestError> {
    let is_gzip = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("gzip"));

    if is_gzip {
        let decoder = flate2::read::GzDecoder::new(body.as_ref());
        let mut decompressed = Vec::new();
        decoder
            .take(MAX_DECOMPRESSED_SIZE as u64 + 1)
            .read_to_end(&mut decompressed)?;
        if decompressed.len() > MAX_DECOMPRESSED_SIZE {
            return Err(IngestError::PayloadTooLarge);
        }
        Ok(Cow::Owned(decompressed))
    } else {
        Ok(Cow::Borrowed(body.as_ref()))
    }
}

/// Decode a Snappy-compressed request body with a pre-allocation size check.
pub fn decode_snappy_body(body: &[u8]) -> Result<Vec<u8>, IngestError> {
    let decompressed_len = snap::raw::decompress_len(body)?;
    if decompressed_len > MAX_DECOMPRESSED_SIZE {
        return Err(IngestError::PayloadTooLarge);
    }

    let mut decompressed = vec![0; decompressed_len];
    let written = snap::raw::Decoder::new().decompress(body, &mut decompressed)?;
    decompressed.truncate(written);
    Ok(decompressed)
}

/// Check if the Content-Type header indicates JSON encoding.
///
/// Matches `application/json` with optional parameters like `; charset=utf-8`.
/// Returns `false` for `application/x-protobuf`, missing headers, or any other type.
pub fn is_json_content_type(headers: &HeaderMap) -> bool {
    headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            let lower = v.to_ascii_lowercase();
            lower == "application/json"
                || lower.starts_with("application/json;")
                || lower.starts_with("application/json ")
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers_with_content_type(ct: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("content-type", ct.parse().unwrap());
        h
    }

    #[test]
    fn test_is_json_exact_match() {
        let h = headers_with_content_type("application/json");
        assert!(is_json_content_type(&h));
    }

    #[test]
    fn test_is_json_with_charset() {
        let h = headers_with_content_type("application/json; charset=utf-8");
        assert!(is_json_content_type(&h));
    }

    #[test]
    fn test_is_json_case_insensitive() {
        let h = headers_with_content_type("Application/JSON; charset=UTF-8");
        assert!(is_json_content_type(&h));
    }

    #[test]
    fn test_protobuf_is_not_json() {
        let h = headers_with_content_type("application/x-protobuf");
        assert!(!is_json_content_type(&h));
    }

    #[test]
    fn test_missing_content_type_is_not_json() {
        let h = HeaderMap::new();
        assert!(!is_json_content_type(&h));
    }

    #[test]
    fn test_empty_content_type_is_not_json() {
        // Some clients might send empty content-type
        let h = headers_with_content_type("application/x-protobuf");
        assert!(!is_json_content_type(&h));
    }

    #[test]
    fn test_max_decompressed_size_is_64_mib() {
        assert_eq!(MAX_DECOMPRESSED_SIZE, 64 * 1024 * 1024);
        assert_eq!(MAX_DECOMPRESSED_SIZE, 67_108_864);
    }

    #[test]
    fn test_snappy_body_rejects_large_declared_size_before_allocating() {
        let compressed = vec![0x81, 0x80, 0x80, 0x20];
        let err = decode_snappy_body(&compressed).expect_err("should reject >64MiB payload");
        assert!(matches!(err, IngestError::PayloadTooLarge));
    }
}
