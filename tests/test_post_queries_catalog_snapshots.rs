//! Integration tests for Tasks 5-8:
//! - Task 5: POST support for query endpoints (form-encoded body)
//! - Task 6: Metric/Label catalog endpoint
//! - Task 7: Graceful shutdown snapshot via save_from_state
//! - Task 8: OTLP JSON encoding support for metrics and traces

mod helpers;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use helpers::{ingest_metrics, make_state, push_logs};
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use serde_json::Value;
use tempfile::tempdir;
use tower::ServiceExt;

/// Helper to collect a response body as parsed JSON.
async fn json_response(resp: axum::http::Response<Body>) -> Value {
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

/// Ingest a known set of logs, metrics, and traces for catalog tests.
async fn catalog_data(app: &axum::Router) {
    // Ingest logs for "cart-service"
    push_logs(app, "cart-service", "item added", "1700000000000000000").await;

    // Ingest a metric for "cart-service"
    ingest_metrics(
        app,
        "cart-service",
        "http_requests_total",
        10.0,
        1_700_000_000_000_000_000,
    )
    .await;

    // Ingest a trace for "cart-service" with a span attribute
    let trace_id: [u8; 16] = [0xca; 16];
    let span_id: [u8; 8] = [0x01; 8];
    let trace_req = make_trace_request_with_string_attribute(
        "cart-service",
        "POST /cart",
        &trace_id,
        &span_id,
        1_700_000_000_000_000_000,
        1_700_000_000_500_000_000,
        1,
        "http.method",
        "POST",
    );
    let r = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(trace_req.encode_to_vec()))
        .unwrap();
    app.clone().oneshot(r).await.unwrap();
}

/// Build an ExportTraceServiceRequest with a single span that has one string attribute.
fn make_trace_request_with_string_attribute(
    service_name: &str,
    span_name: &str,
    trace_id: &[u8; 16],
    span_id: &[u8; 8],
    start_ns: u64,
    end_ns: u64,
    status_code: i32,
    attr_key: &str,
    attr_val: &str,
) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(service_name.into())),
                    }),
                }],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: trace_id.to_vec(),
                    span_id: span_id.to_vec(),
                    trace_state: String::new(),
                    parent_span_id: vec![],
                    name: span_name.into(),
                    kind: 1,
                    start_time_unix_nano: start_ns,
                    end_time_unix_nano: end_ns,
                    attributes: vec![KeyValue {
                        key: attr_key.into(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(attr_val.into())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    events: vec![],
                    dropped_events_count: 0,
                    links: vec![],
                    dropped_links_count: 0,
                    status: Some(Status {
                        message: String::new(),
                        code: status_code,
                    }),
                    flags: 0,
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

// ---------------------------------------------------------------------------
// Task 5: POST support for query endpoints
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_loki_query_range_accepts_post_form_body() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    // Ingest a log entry so the query returns results
    push_logs(&app, "web", "hello world", "1700000000000000000").await;

    let query = r#"{service="web"}"#;

    // GET request for baseline
    let encoded_query = urlencoding::encode(query);
    let get_req = Request::builder()
        .method("GET")
        .uri(&format!(
            "/loki/api/v1/query_range?query={}&start=1700000000&end=1700000001",
            encoded_query,
        ))
        .body(Body::empty())
        .unwrap();
    let get_resp = app.clone().oneshot(get_req).await.unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);
    let get_json = json_response(get_resp).await;

    // POST with form body
    let form_body = format!(
        "query={}&start=1700000000&end=1700000001",
        urlencoding::encode(query),
    );
    let post_req = Request::builder()
        .method("POST")
        .uri("/loki/api/v1/query_range")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from(form_body))
        .unwrap();
    let post_resp = app.clone().oneshot(post_req).await.unwrap();
    assert_eq!(post_resp.status(), StatusCode::OK);
    let post_json = json_response(post_resp).await;

    // Both should return same result structure
    assert_eq!(get_json["status"], "success");
    assert_eq!(post_json["status"], "success");
    assert_eq!(
        get_json["data"]["resultType"],
        post_json["data"]["resultType"]
    );
    assert_eq!(
        get_json["data"]["result"].as_array().map(|a| a.len()),
        post_json["data"]["result"].as_array().map(|a| a.len()),
    );
}

#[tokio::test]
async fn test_promql_query_accepts_post_form_body() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    // Ingest a metric
    ingest_metrics(&app, "api", "up", 1.0, 5_000_000_000).await;

    let query = r#"up{service="api"}"#;

    // GET for baseline
    let encoded_query = urlencoding::encode(query);
    let get_req = Request::builder()
        .method("GET")
        .uri(&format!("/api/v1/query?query={}&time=5", encoded_query))
        .body(Body::empty())
        .unwrap();
    let get_resp = app.clone().oneshot(get_req).await.unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);
    let get_json = json_response(get_resp).await;

    // POST with form body
    let form_body = format!("query={}&time=5", urlencoding::encode(query));
    let post_req = Request::builder()
        .method("POST")
        .uri("/api/v1/query")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from(form_body))
        .unwrap();
    let post_resp = app.clone().oneshot(post_req).await.unwrap();
    assert_eq!(post_resp.status(), StatusCode::OK);
    let post_json = json_response(post_resp).await;

    assert_eq!(get_json["status"], "success");
    assert_eq!(post_json["status"], "success");
    assert_eq!(
        get_json["data"]["resultType"],
        post_json["data"]["resultType"]
    );
    assert_eq!(
        get_json["data"]["result"].as_array().map(|a| a.len()),
        post_json["data"]["result"].as_array().map(|a| a.len()),
    );
}

// ---------------------------------------------------------------------------
// Task 6: Metric/Label catalog endpoint
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_catalog_endpoint_returns_signal_catalog_for_service() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    catalog_data(&app).await;

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/catalog?service=cart-service")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let json = json_response(resp).await;

    // Should contain metrics array with at least "http_requests_total"
    let metrics = json["data"]["metrics"]
        .as_array()
        .expect("metrics should be an array");
    let metric_names: Vec<&str> = metrics.iter().filter_map(|v| v.as_str()).collect();
    assert!(
        metric_names.contains(&"http_requests_total"),
        "expected http_requests_total in metrics, got: {:?}",
        metric_names
    );

    // Should contain log_labels array with at least "service" and "level"
    let log_labels = json["data"]["log_labels"]
        .as_array()
        .expect("log_labels should be an array");
    let label_names: Vec<&str> = log_labels.iter().filter_map(|v| v.as_str()).collect();
    assert!(
        label_names.contains(&"service"),
        "expected 'service' in log_labels, got: {:?}",
        label_names
    );

    // Should contain span_attributes array
    let span_attrs = json["data"]["span_attributes"]
        .as_array()
        .expect("span_attributes should be an array");
    assert!(
        !span_attrs.is_empty(),
        "expected at least one span attribute for cart-service"
    );
}

#[tokio::test]
async fn test_catalog_endpoint_returns_empty_arrays_for_unknown_service() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    catalog_data(&app).await;

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/catalog?service=nonexistent")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let json = json_response(resp).await;
    assert_eq!(
        json["data"]["metrics"].as_array().map(|a| a.len()),
        Some(0),
        "unknown service should have empty metrics"
    );
    assert_eq!(
        json["data"]["log_labels"].as_array().map(|a| a.len()),
        Some(0),
        "unknown service should have empty log_labels"
    );
    assert_eq!(
        json["data"]["span_attributes"].as_array().map(|a| a.len()),
        Some(0),
        "unknown service should have empty span_attributes"
    );
}

#[tokio::test]
async fn test_catalog_endpoint_requires_service_param() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/catalog")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// Task 7: Graceful shutdown snapshot via save_from_state
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_save_from_state_creates_snapshot_file() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    // Ingest some data so the snapshot is non-trivial
    push_logs(&app, "snap-svc", "snapshot test", "1700000000000000000").await;
    ingest_metrics(
        &app,
        "snap-svc",
        "requests",
        42.0,
        1_700_000_000_000_000_000,
    )
    .await;

    let dir = tempdir().unwrap();

    // save_from_state does not return a Result; it logs errors internally
    obsidian::snapshot::save_from_state(&state, dir.path());

    let snap_path = dir.path().join("obsidian.snap");
    assert!(
        snap_path.exists(),
        "snapshot file should exist at {:?}",
        snap_path
    );
    let metadata = std::fs::metadata(&snap_path).unwrap();
    assert!(
        metadata.len() > 0,
        "snapshot file should be non-empty, got {} bytes",
        metadata.len()
    );
}

// ---------------------------------------------------------------------------
// Task 8: OTLP JSON encoding for metrics and traces
// ---------------------------------------------------------------------------

/// Build a JSON body matching the OTLP ExportMetricsServiceRequest proto3 JSON mapping.
/// This is the shape the new JSON handler must accept.
fn otlp_metrics_json(service_name: &str, metric_name: &str, value: f64, ts_ns: u64) -> String {
    serde_json::to_string(&serde_json::json!({
        "resourceMetrics": [{
            "resource": {
                "attributes": [{
                    "key": "service.name",
                    "value": { "stringValue": service_name }
                }]
            },
            "scopeMetrics": [{
                "metrics": [{
                    "name": metric_name,
                    "gauge": {
                        "dataPoints": [{
                            "timeUnixNano": ts_ns.to_string(),
                            "asDouble": value
                        }]
                    }
                }]
            }]
        }]
    }))
    .unwrap()
}

/// Build a JSON body matching the OTLP ExportTraceServiceRequest proto3 JSON mapping.
fn otlp_traces_json(
    service_name: &str,
    span_name: &str,
    trace_id_hex: &str,
    span_id_hex: &str,
    start_ns: u64,
    end_ns: u64,
    status_code: i32,
) -> String {
    serde_json::to_string(&serde_json::json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [{
                    "key": "service.name",
                    "value": { "stringValue": service_name }
                }]
            },
            "scopeSpans": [{
                "spans": [{
                    "traceId": trace_id_hex,
                    "spanId": span_id_hex,
                    "name": span_name,
                    "kind": 1,
                    "startTimeUnixNano": start_ns.to_string(),
                    "endTimeUnixNano": end_ns.to_string(),
                    "status": { "code": status_code }
                }]
            }]
        }]
    }))
    .unwrap()
}

#[tokio::test]
async fn test_otlp_metrics_accepts_json_payloads() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let json_body = otlp_metrics_json("billing", "invoice_count", 7.0, 1_700_000_000_000_000_000);

    let req = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header("content-type", "application/json")
        .body(Body::from(json_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "OTLP metrics JSON ingest should return 200"
    );

    // Verify the metric was ingested
    let store = state.metric_store.read();
    assert!(
        !store.series.is_empty(),
        "metric store should have at least one series after JSON ingest"
    );
}

#[tokio::test]
async fn test_otlp_traces_accepts_json_payloads() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    // Use hex-encoded trace/span IDs (proto3 JSON encodes bytes as base64,
    // but the OTLP spec uses hex for trace/span IDs in JSON encoding)
    let json_body = otlp_traces_json(
        "checkout",
        "process_order",
        "abababababababababababababababab",
        "cdcdcdcdcdcdcdcd",
        1_700_000_000_000_000_000,
        1_700_000_000_500_000_000,
        2,
    );

    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "application/json")
        .body(Body::from(json_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "OTLP traces JSON ingest should return 200"
    );

    // Verify the trace was ingested
    let store = state.trace_store.read();
    assert!(
        !store.traces.is_empty(),
        "trace store should have at least one trace after JSON ingest"
    );
}

#[tokio::test]
async fn test_otlp_metrics_protobuf_remains_supported() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let payload =
        helpers::make_gauge_request("legacy-svc", "latency_ms", 55.0, 1_700_000_000_000_000_000);
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
        "protobuf metrics ingest should still return 200"
    );

    let store = state.metric_store.read();
    assert!(
        !store.series.is_empty(),
        "metric store should have data after protobuf ingest"
    );
}

#[tokio::test]
async fn test_otlp_traces_protobuf_remains_supported() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let trace_id: [u8; 16] = [0xee; 16];
    let span_id: [u8; 8] = [0xff; 8];
    let payload = helpers::make_trace_request(
        "legacy-svc",
        "do_work",
        &trace_id,
        &span_id,
        1_700_000_000_000_000_000,
        1_700_000_000_100_000_000,
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
        "protobuf traces ingest should still return 200"
    );

    let store = state.trace_store.read();
    assert!(
        store.get_trace(&trace_id).is_some(),
        "trace should be present after protobuf ingest"
    );
}
