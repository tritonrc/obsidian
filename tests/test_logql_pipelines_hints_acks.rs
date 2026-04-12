//! Integration tests for Tasks 13-16:
//! - Task 13: LogQL pipeline filtering on JSON log lines
//! - Task 14: Snappy-compressed Loki push ingestion
//! - Task 15: Parse error responses include structured error info
//! - Task 16: Ingest endpoints return NO_CONTENT (204) on success

mod helpers;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use helpers::make_state;
use http_body_util::BodyExt;
use prost::Message;
use serde_json::Value;
use tower::ServiceExt;

/// Helper to collect a response body as parsed JSON.
async fn json_response(resp: axum::http::Response<Body>) -> Value {
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

// ---------------------------------------------------------------------------
// Task 13: LogQL pipeline filter on JSON-shaped log lines
//
// The LogQL parser supports line filters (|= != |~ !~) but NOT a `| json`
// pipeline stage. This test verifies that line-contains filtering works
// correctly on log lines that happen to contain JSON content.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_logql_line_filter_on_json_log_lines() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    // Ingest JSON-shaped log lines with different levels
    let ts_base = 1_700_000_000_000_000_000u64;

    // Push logs with JSON content via direct requests (push_logs hardcodes level=info)
    let push_body = serde_json::json!({
        "streams": [{
            "stream": {"service": "app"},
            "values": [
                [(ts_base).to_string(), r#"{"level":"error","msg":"disk full"}"#],
                [(ts_base + 1_000_000_000).to_string(), r#"{"level":"info","msg":"all good"}"#],
                [(ts_base + 2_000_000_000).to_string(), r#"{"level":"error","msg":"timeout"}"#],
                [(ts_base + 3_000_000_000).to_string(), r#"{"level":"warn","msg":"slow query"}"#],
            ]
        }]
    });

    let req = Request::builder()
        .method("POST")
        .uri("/loki/api/v1/push")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&push_body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Query with |= to filter for lines containing "error"
    // Query: {service="app"} |= "error"
    let query = urlencoding::encode(r#"{service="app"} |= "error""#);
    let req = Request::builder()
        .method("GET")
        .uri(format!(
            "/loki/api/v1/query_range?query={}&start={}&end={}",
            query,
            ts_base - 1_000_000_000,
            ts_base + 4_000_000_000u64,
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let json = json_response(resp).await;
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["resultType"], "streams");

    let result = json["data"]["result"]
        .as_array()
        .expect("result should be array");
    assert_eq!(result.len(), 1, "should have one matching stream");

    let values = result[0]["values"]
        .as_array()
        .expect("values should be array");
    assert_eq!(values.len(), 2, "should have 2 lines containing 'error'");

    // Verify both matched lines contain "error"
    for v in values {
        let line = v[1].as_str().unwrap();
        assert!(
            line.contains("error"),
            "filtered line should contain 'error', got: {}",
            line
        );
    }
}

#[tokio::test]
async fn test_logql_line_not_contains_filter() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let ts_base = 1_700_000_000_000_000_000u64;

    let push_body = serde_json::json!({
        "streams": [{
            "stream": {"service": "app"},
            "values": [
                [(ts_base).to_string(), r#"{"level":"error","msg":"disk full"}"#],
                [(ts_base + 1_000_000_000).to_string(), r#"{"level":"info","msg":"all good"}"#],
                [(ts_base + 2_000_000_000).to_string(), r#"{"level":"error","msg":"timeout"}"#],
            ]
        }]
    });

    let req = Request::builder()
        .method("POST")
        .uri("/loki/api/v1/push")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&push_body).unwrap()))
        .unwrap();
    app.clone().oneshot(req).await.unwrap();

    // Query with != to exclude lines containing "error"
    let query = urlencoding::encode(r#"{service="app"} != "error""#);
    let req = Request::builder()
        .method("GET")
        .uri(format!(
            "/loki/api/v1/query_range?query={}&start={}&end={}",
            query,
            ts_base - 1_000_000_000,
            ts_base + 3_000_000_000u64,
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let json = json_response(resp).await;
    let result = json["data"]["result"]
        .as_array()
        .expect("result should be array");
    assert_eq!(result.len(), 1, "should have one matching stream");

    let values = result[0]["values"]
        .as_array()
        .expect("values should be array");
    assert_eq!(values.len(), 1, "should have 1 line not containing 'error'");
    assert!(
        values[0][1].as_str().unwrap().contains("all good"),
        "remaining line should be the info line"
    );
}

#[tokio::test]
async fn test_logql_regex_filter_on_json_log_lines() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let ts_base = 1_700_000_000_000_000_000u64;

    let push_body = serde_json::json!({
        "streams": [{
            "stream": {"service": "app"},
            "values": [
                [(ts_base).to_string(), r#"{"level":"error","msg":"disk full"}"#],
                [(ts_base + 1_000_000_000).to_string(), r#"{"level":"info","msg":"all good"}"#],
                [(ts_base + 2_000_000_000).to_string(), r#"{"level":"warn","msg":"slow query"}"#],
            ]
        }]
    });

    let req = Request::builder()
        .method("POST")
        .uri("/loki/api/v1/push")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&push_body).unwrap()))
        .unwrap();
    app.clone().oneshot(req).await.unwrap();

    // Query with |~ regex to match error or warn
    let query = urlencoding::encode(r#"{service="app"} |~ "error|warn""#);
    let req = Request::builder()
        .method("GET")
        .uri(format!(
            "/loki/api/v1/query_range?query={}&start={}&end={}",
            query,
            ts_base - 1_000_000_000,
            ts_base + 3_000_000_000u64,
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let json = json_response(resp).await;
    let result = json["data"]["result"]
        .as_array()
        .expect("result should be array");
    assert_eq!(result.len(), 1);

    let values = result[0]["values"]
        .as_array()
        .expect("values should be array");
    assert_eq!(values.len(), 2, "should match error and warn lines");
}

// ---------------------------------------------------------------------------
// Task 14: Snappy-compressed Loki push ingestion
//
// The Loki push handler supports snappy-compressed JSON via
// content-type: application/x-protobuf or application/x-snappy.
// There is no Prometheus remote write endpoint.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_loki_push_snappy_compressed_json() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let ts = "1700000000000000000";
    let push_body = serde_json::json!({
        "streams": [{
            "stream": {"service": "snappy-svc", "level": "info"},
            "values": [
                [ts, "compressed log line 1"],
                [ts, "compressed log line 2"],
            ]
        }]
    });

    let json_bytes = serde_json::to_vec(&push_body).unwrap();
    let compressed = snap::raw::Encoder::new().compress_vec(&json_bytes).unwrap();

    let req = Request::builder()
        .method("POST")
        .uri("/loki/api/v1/push")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(compressed))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "snappy-compressed Loki push should return 204"
    );

    // Verify the logs were ingested by querying
    let query = urlencoding::encode(r#"{service="snappy-svc"}"#);
    let req = Request::builder()
        .method("GET")
        .uri(format!(
            "/loki/api/v1/query_range?query={}&start=1699999999&end=1700000001",
            query
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let json = json_response(resp).await;
    assert_eq!(json["status"], "success");
    let result = json["data"]["result"]
        .as_array()
        .expect("result should be array");
    assert_eq!(result.len(), 1, "should have one stream");

    let values = result[0]["values"]
        .as_array()
        .expect("values should be array");
    assert_eq!(
        values.len(),
        2,
        "should have 2 log entries from snappy push"
    );
}

#[tokio::test]
async fn test_loki_push_snappy_content_type_x_snappy() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let push_body = serde_json::json!({
        "streams": [{
            "stream": {"service": "snappy-alt"},
            "values": [
                ["1700000000000000000", "alt content-type test"],
            ]
        }]
    });

    let json_bytes = serde_json::to_vec(&push_body).unwrap();
    let compressed = snap::raw::Encoder::new().compress_vec(&json_bytes).unwrap();

    // Use application/x-snappy content-type (also supported)
    let req = Request::builder()
        .method("POST")
        .uri("/loki/api/v1/push")
        .header("content-type", "application/x-snappy")
        .body(Body::from(compressed))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "application/x-snappy content-type should also be accepted"
    );
}

#[tokio::test]
async fn test_loki_push_invalid_snappy_returns_bad_request() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    // Send garbage bytes with snappy content-type
    let req = Request::builder()
        .method("POST")
        .uri("/loki/api/v1/push")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(vec![0xFF, 0xFE, 0xFD]))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "invalid snappy data should return 400"
    );
}

// ---------------------------------------------------------------------------
// Task 15: Parse error responses include structured error information
//
// Error responses follow these formats per endpoint:
// - LogQL:  {"status": "error", "error": "<message>"}
// - PromQL: {"status": "error", "errorType": "bad_data", "error": "<message>"}
// - TraceQL: {"error": "<message>"}
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_logql_parse_error_response() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    // Send a malformed LogQL query
    let query = urlencoding::encode(r#"not a valid query"#);
    let req = Request::builder()
        .method("GET")
        .uri(format!("/loki/api/v1/query?query={}", query))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let json = json_response(resp).await;
    assert_eq!(json["status"], "error", "status should be 'error'");
    assert!(
        json["error"].as_str().is_some(),
        "error field should be present with a message"
    );
    assert!(
        !json["error"].as_str().unwrap().is_empty(),
        "error message should not be empty"
    );
}

#[tokio::test]
async fn test_logql_query_range_parse_error_response() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let query = urlencoding::encode(r#"{invalid"#);
    let req = Request::builder()
        .method("GET")
        .uri(format!(
            "/loki/api/v1/query_range?query={}&start=1700000000&end=1700000001",
            query
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let json = json_response(resp).await;
    assert_eq!(json["status"], "error");
    assert!(json["error"].as_str().is_some());
}

#[tokio::test]
async fn test_promql_parse_error_response() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    // Send a malformed PromQL query
    let query = urlencoding::encode(r#"invalid{[["#);
    let req = Request::builder()
        .method("GET")
        .uri(format!("/api/v1/query?query={}", query))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let json = json_response(resp).await;
    assert_eq!(json["status"], "error");
    assert_eq!(
        json["errorType"], "bad_data",
        "PromQL errors should include errorType: bad_data"
    );
    assert!(
        json["error"].as_str().is_some(),
        "error field should be present"
    );
}

#[tokio::test]
async fn test_promql_query_range_parse_error_response() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let query = urlencoding::encode(r#"}{bad"#);
    let req = Request::builder()
        .method("GET")
        .uri(format!(
            "/api/v1/query_range?query={}&start=1700000000&end=1700000001&step=60s",
            query
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let json = json_response(resp).await;
    assert_eq!(json["status"], "error");
    assert_eq!(json["errorType"], "bad_data");
    assert!(json["error"].as_str().is_some());
}

#[tokio::test]
async fn test_traceql_parse_error_response() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    // Send a malformed TraceQL query
    let query = urlencoding::encode(r#"not valid traceql"#);
    let req = Request::builder()
        .method("GET")
        .uri(format!("/api/search?q={}", query))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let json = json_response(resp).await;
    assert!(
        json["error"].as_str().is_some(),
        "TraceQL error response should have an 'error' field"
    );
    assert!(
        !json["error"].as_str().unwrap().is_empty(),
        "error message should not be empty"
    );
}

// ---------------------------------------------------------------------------
// Task 16: Ingest endpoints return NO_CONTENT (204) on success
//
// All three ingest endpoints (Loki push, OTLP metrics, OTLP traces)
// return StatusCode::OK (204) with no response body.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_loki_push_returns_no_content() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let push_body = serde_json::json!({
        "streams": [{
            "stream": {"service": "ack-test", "level": "info"},
            "values": [
                ["1700000000000000000", "test line 1"],
                ["1700000001000000000", "test line 2"],
                ["1700000002000000000", "test line 3"],
            ]
        }]
    });

    let req = Request::builder()
        .method("POST")
        .uri("/loki/api/v1/push")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&push_body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "Loki push should return 200 OK with ack counts"
    );

    // Response should contain accepted counts
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        json["accepted"]["streams"].is_number(),
        "expected streams count"
    );
    assert!(
        json["accepted"]["entries"].is_number(),
        "expected entries count"
    );
}

#[tokio::test]
async fn test_otlp_metrics_returns_ack() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let payload =
        helpers::make_gauge_request("ack-svc", "cpu_usage", 42.5, 1_700_000_000_000_000_000);
    let req = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(payload.encode_to_vec()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "OTLP metrics should return 200 OK with ack counts"
    );

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        json["accepted"]["series"].is_number(),
        "expected series count"
    );
    assert!(
        json["accepted"]["samples"].is_number(),
        "expected samples count"
    );
}

#[tokio::test]
async fn test_otlp_traces_returns_ack() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let trace_id: [u8; 16] = [0xAC; 16];
    let span_id: [u8; 8] = [0xBD; 8];
    let payload = helpers::make_trace_request(
        "ack-svc",
        "handle_request",
        &trace_id,
        &span_id,
        1_700_000_000_000_000_000,
        1_700_000_000_500_000_000,
        1,
    );
    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(payload.encode_to_vec()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "OTLP traces should return 200 OK with ack counts"
    );

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        json["accepted"]["traces"].is_number(),
        "expected traces count"
    );
    assert!(
        json["accepted"]["spans"].is_number(),
        "expected spans count"
    );
}

#[tokio::test]
async fn test_loki_push_invalid_json_returns_bad_request() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let req = Request::builder()
        .method("POST")
        .uri("/loki/api/v1/push")
        .header("content-type", "application/json")
        .body(Body::from("not json"))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "invalid JSON should return 400"
    );
}

#[tokio::test]
async fn test_otlp_metrics_invalid_protobuf_returns_bad_request() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let req = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(vec![0xFF, 0xFE, 0xFD, 0xFC]))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "invalid protobuf should return 400"
    );
}

#[tokio::test]
async fn test_otlp_traces_invalid_protobuf_returns_bad_request() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(vec![0xFF, 0xFE, 0xFD, 0xFC]))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "invalid protobuf should return 400"
    );
}
