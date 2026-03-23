//! Integration tests for 6 bug fixes found during forge-http integration.
//!
//! Bug 1: OTLP /v1/logs JSON with charset in Content-Type was misrouted to protobuf decoder
//! Bug 2: OTLP /v1/metrics JSON with charset in Content-Type was misrouted to protobuf decoder
//! Bug 3: Content-Type handling — is_json_content_type was too strict
//! Bug 4: GET /api/search with no q parameter returned empty instead of recent traces
//! Bug 5: Time range params (start/end) on /api/search didn't filter correctly
//! Bug 6: /ready endpoint and OpenAPI spec documentation

mod helpers;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use helpers::{ingest_traces, make_state};
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use serde_json::Value;
use tower::ServiceExt;

fn make_resource(service_name: &str) -> Option<Resource> {
    Some(Resource {
        attributes: vec![KeyValue {
            key: "service.name".into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(service_name.into())),
            }),
        }],
        dropped_attributes_count: 0,
        entity_refs: vec![],
    })
}

// ---------- Bug 1: OTLP /v1/logs JSON ingestion ----------

#[tokio::test]
async fn test_bug1_otlp_logs_json_with_charset() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let logs_request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: make_resource("forge-http"),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: 1_000_000_000,
                    severity_number: 9, // info
                    severity_text: "INFO".into(),
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test log message".into())),
                    }),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    flags: 0,
                    trace_id: vec![],
                    span_id: vec![],
                    observed_time_unix_nano: 0,
                    event_name: String::new(),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    // Send as JSON with charset — this was the failing case
    let json_body = serde_json::to_vec(&logs_request).unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header("content-type", "application/json; charset=utf-8")
        .body(Body::from(json_body))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Verify logs were actually stored
    let store = state.log_store.read();
    assert!(
        store.total_entries > 0,
        "totalLogEntries should be > 0 after JSON OTLP ingest"
    );
}

#[tokio::test]
async fn test_bug1_otlp_logs_json_plain() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let logs_request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: make_resource("forge-http"),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: 2_000_000_000,
                    severity_number: 17, // error
                    severity_text: "ERROR".into(),
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("error log message".into())),
                    }),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    flags: 0,
                    trace_id: vec![],
                    span_id: vec![],
                    observed_time_unix_nano: 0,
                    event_name: String::new(),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let json_body = serde_json::to_vec(&logs_request).unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header("content-type", "application/json")
        .body(Body::from(json_body))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let store = state.log_store.read();
    assert!(store.total_entries > 0);
}

// ---------- Bug 2: OTLP /v1/metrics JSON ingestion ----------

#[tokio::test]
async fn test_bug2_otlp_metrics_json_with_charset() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let metrics_request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: make_resource("forge-http"),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "http_requests_total".into(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![],
                            start_time_unix_nano: 0,
                            time_unix_nano: 5_000_000_000,
                            exemplars: vec![],
                            flags: 0,
                            value: Some(
                                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(42.0),
                            ),
                        }],
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    // Send as JSON with charset
    let json_body = serde_json::to_vec(&metrics_request).unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header("content-type", "application/json; charset=utf-8")
        .body(Body::from(json_body))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    // Verify series were actually stored
    assert!(
        json["accepted"]["series"].as_u64().unwrap() > 0,
        "accepted series should be > 0 after JSON OTLP ingest"
    );

    let store = state.metric_store.read();
    assert!(!store.series.is_empty());
}

// ---------- Bug 3: Content-Type routing for protobuf ----------

#[tokio::test]
async fn test_bug3_protobuf_content_type_routes_to_protobuf_decoder() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let trace_request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: make_resource("forge-http"),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                    span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                    trace_state: String::new(),
                    parent_span_id: vec![],
                    name: "test-span".into(),
                    kind: 1,
                    start_time_unix_nano: 1_000_000_000,
                    end_time_unix_nano: 2_000_000_000,
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    events: vec![],
                    dropped_events_count: 0,
                    links: vec![],
                    dropped_links_count: 0,
                    status: Some(Status {
                        message: String::new(),
                        code: 1,
                    }),
                    flags: 0,
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    // Explicit protobuf content type
    let proto_body = prost::Message::encode_to_vec(&trace_request);
    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(proto_body))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["accepted"]["spans"].as_u64().unwrap(), 1);
}

#[tokio::test]
async fn test_bug3_missing_content_type_routes_to_protobuf_decoder() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let trace_request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: make_resource("forge-http"),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: vec![2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                    span_id: vec![2, 2, 3, 4, 5, 6, 7, 8],
                    trace_state: String::new(),
                    parent_span_id: vec![],
                    name: "test-span-2".into(),
                    kind: 1,
                    start_time_unix_nano: 3_000_000_000,
                    end_time_unix_nano: 4_000_000_000,
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    events: vec![],
                    dropped_events_count: 0,
                    links: vec![],
                    dropped_links_count: 0,
                    status: Some(Status {
                        message: String::new(),
                        code: 0,
                    }),
                    flags: 0,
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    // No content-type header at all — should default to protobuf
    let proto_body = prost::Message::encode_to_vec(&trace_request);
    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .body(Body::from(proto_body))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["accepted"]["spans"].as_u64().unwrap(), 1);
}

// ---------- Bug 4: GET /api/search with no q param returns recent traces ----------

#[tokio::test]
async fn test_bug4_search_without_query_returns_recent_traces() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let trace_id1: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let span_id1: [u8; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
    let trace_id2: [u8; 16] = [2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let span_id2: [u8; 8] = [2, 2, 3, 4, 5, 6, 7, 8];

    ingest_traces(
        &app,
        "forge-http",
        "GET /api/health",
        &trace_id1,
        &span_id1,
        1_000_000_000,
        2_000_000_000,
        1, // Ok
    )
    .await;

    ingest_traces(
        &app,
        "forge-http",
        "POST /api/submit",
        &trace_id2,
        &span_id2,
        3_000_000_000,
        4_000_000_000,
        1,
    )
    .await;

    // No q parameter at all
    let req = Request::builder()
        .method("GET")
        .uri("/api/search")
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    let traces = json["traces"].as_array().unwrap();
    assert_eq!(
        traces.len(),
        2,
        "Should return 2 recent traces when no query is given"
    );

    // Most recent trace should be first
    let first_start: i64 = traces[0]["startTimeUnixNano"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let second_start: i64 = traces[1]["startTimeUnixNano"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        first_start >= second_start,
        "Traces should be ordered most recent first"
    );
}

#[tokio::test]
async fn test_bug4_search_with_empty_query_returns_recent_traces() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let trace_id: [u8; 16] = [3, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let span_id: [u8; 8] = [3, 2, 3, 4, 5, 6, 7, 8];

    ingest_traces(
        &app,
        "forge-http",
        "GET /ready",
        &trace_id,
        &span_id,
        1_000_000_000,
        1_500_000_000,
        1,
    )
    .await;

    // Empty q parameter
    let req = Request::builder()
        .method("GET")
        .uri("/api/search?q=")
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    let traces = json["traces"].as_array().unwrap();
    assert_eq!(
        traces.len(),
        1,
        "Should return recent traces when q is empty string"
    );
}

// ---------- Bug 5: Time range filtering on /api/search ----------

#[tokio::test]
async fn test_bug5_search_time_range_filter_seconds() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let old_trace: [u8; 16] = [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let old_span: [u8; 8] = [10, 0, 0, 0, 0, 0, 0, 1];
    let new_trace: [u8; 16] = [20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2];
    let new_span: [u8; 8] = [20, 0, 0, 0, 0, 0, 0, 2];

    // Old trace at t=1s
    ingest_traces(
        &app,
        "forge-http",
        "old-span",
        &old_trace,
        &old_span,
        1_000_000_000, // 1s
        2_000_000_000, // 2s
        1,
    )
    .await;

    // New trace at t=100s
    ingest_traces(
        &app,
        "forge-http",
        "new-span",
        &new_trace,
        &new_span,
        100_000_000_000, // 100s
        101_000_000_000, // 101s
        1,
    )
    .await;

    // Filter with start/end in seconds — should only return the new trace
    let req = Request::builder()
        .method("GET")
        .uri("/api/search?start=50&end=200")
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    let traces = json["traces"].as_array().unwrap();
    assert_eq!(
        traces.len(),
        1,
        "Time range filter should exclude old trace"
    );
    assert_eq!(traces[0]["rootTraceName"], "new-span");
}

#[tokio::test]
async fn test_bug5_search_time_range_filter_nanoseconds() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let old_trace: [u8; 16] = [11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let old_span: [u8; 8] = [11, 0, 0, 0, 0, 0, 0, 1];
    let new_trace: [u8; 16] = [21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2];
    let new_span: [u8; 8] = [21, 0, 0, 0, 0, 0, 0, 2];

    // Use realistic nanosecond timestamps (year 2025 range)
    // old trace at ~1 second in epoch ns
    ingest_traces(
        &app,
        "forge-http",
        "old-span",
        &old_trace,
        &old_span,
        1_000_000_000_000_000, // 1e15 ns = 1000 seconds
        1_001_000_000_000_000,
        1,
    )
    .await;

    // new trace at a later time
    ingest_traces(
        &app,
        "forge-http",
        "new-span",
        &new_trace,
        &new_span,
        5_000_000_000_000_000, // 5e15 ns = 5000 seconds
        5_001_000_000_000_000,
        1,
    )
    .await;

    // Filter with start/end in nanoseconds (>= 1e15 triggers nano detection)
    // Range: 2e15 to 6e15 should include only the new trace
    let req = Request::builder()
        .method("GET")
        .uri("/api/search?start=2000000000000000&end=6000000000000000")
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    let traces = json["traces"].as_array().unwrap();
    assert_eq!(
        traces.len(),
        1,
        "Time range filter with nanoseconds should exclude old trace"
    );
    assert_eq!(traces[0]["rootTraceName"], "new-span");
}

#[tokio::test]
async fn test_bug5_search_time_range_with_query() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let trace1: [u8; 16] = [12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let span1: [u8; 8] = [12, 0, 0, 0, 0, 0, 0, 1];
    let trace2: [u8; 16] = [22, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2];
    let span2: [u8; 8] = [22, 0, 0, 0, 0, 0, 0, 2];

    ingest_traces(
        &app,
        "forge-http",
        "early-error",
        &trace1,
        &span1,
        1_000_000_000,
        2_000_000_000,
        2, // Error
    )
    .await;

    ingest_traces(
        &app,
        "forge-http",
        "late-error",
        &trace2,
        &span2,
        100_000_000_000,
        101_000_000_000,
        2, // Error
    )
    .await;

    // Query for errors with a time range that only includes the late one
    let query = urlencoding::encode(r#"{ status = error }"#);
    let uri = format!("/api/search?q={}&start=50&end=200", query);
    let req = Request::builder()
        .method("GET")
        .uri(&uri)
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    let traces = json["traces"].as_array().unwrap();
    assert_eq!(
        traces.len(),
        1,
        "Time range with query should filter correctly"
    );
}

// ---------- Bug 6: /ready endpoint and OpenAPI spec ----------

#[tokio::test]
async fn test_bug6_ready_endpoint_exists() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/ready")
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "ready");
}

#[tokio::test]
async fn test_bug6_openapi_includes_ready() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/openapi.json")
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    // Verify /ready is documented
    assert!(
        json["paths"]["/ready"].is_object(),
        "/ready should be in the OpenAPI spec"
    );
    assert!(
        json["paths"]["/ready"]["get"].is_object(),
        "/ready should have a GET operation"
    );
}

#[tokio::test]
async fn test_bug6_openapi_search_q_is_optional() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/openapi.json")
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    // Verify /api/search q param is optional
    let search_params = json["paths"]["/api/search"]["get"]["parameters"]
        .as_array()
        .unwrap();
    let q_param = search_params
        .iter()
        .find(|p| p["name"] == "q")
        .expect("q parameter should exist");
    assert_eq!(
        q_param["required"], false,
        "q parameter should be optional in OpenAPI spec"
    );

    // Verify start/end params are documented
    let start_param = search_params.iter().find(|p| p["name"] == "start");
    assert!(
        start_param.is_some(),
        "start parameter should be in OpenAPI spec"
    );
    let end_param = search_params.iter().find(|p| p["name"] == "end");
    assert!(
        end_param.is_some(),
        "end parameter should be in OpenAPI spec"
    );
}
