//! Contract tests for P0/P1 batch 1 features:
//!
//! - Task 1: OTLP Log Ingestion (POST /v1/logs)
//! - Task 2: Unified Diagnostic Summary (GET /api/v1/summary)
//! - Task 3: Time Range Filtering on Trace Search (start/end params on /api/search)
//! - Task 4: topk/bottomk PromQL aggregations
//!
//! These tests define the expected contract for NEW features. They will fail until
//! the corresponding handlers and evaluator changes are implemented.

mod helpers;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use helpers::{ingest_metrics, ingest_traces, make_state};
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use serde_json::Value;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Shared helpers local to this file
// ---------------------------------------------------------------------------

/// Send a request and return (status, parsed JSON body).
async fn send(app: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).unwrap_or(Value::Null)
    };
    (status, json)
}

/// Build a KeyValue with a string value.
fn kv_string(key: &str, val: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(val.into())),
        }),
    }
}

/// Build an AnyValue string.
fn any_string(val: &str) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::StringValue(val.into())),
    }
}

/// Construct an ExportLogsServiceRequest with one log record.
fn make_logs_request(
    service_name: &str,
    body_text: &str,
    severity: i32,
    ts_ns: u64,
) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv_string("service.name", service_name)],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: ts_ns,
                    observed_time_unix_nano: ts_ns,
                    severity_number: severity,
                    severity_text: String::new(),
                    body: Some(any_string(body_text)),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    flags: 0,
                    trace_id: vec![],
                    span_id: vec![],
                    event_name: String::new(),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Build an ExportMetricsServiceRequest with a gauge that carries extra labels.
fn make_gauge_request_with_labels(
    service_name: &str,
    metric_name: &str,
    value: f64,
    ts_ns: u64,
    extra_labels: Vec<(&str, &str)>,
) -> ExportMetricsServiceRequest {
    let attributes: Vec<KeyValue> = extra_labels
        .into_iter()
        .map(|(k, v)| kv_string(k, v))
        .collect();

    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![kv_string("service.name", service_name)],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: metric_name.into(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes,
                            start_time_unix_nano: 0,
                            time_unix_nano: ts_ns,
                            exemplars: vec![],
                            flags: 0,
                            value: Some(
                                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(value),
                            ),
                        }],
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Encode a hex trace-id string from a [u8; 16].
fn hex_trace_id(bytes: &[u8; 16]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

// ===========================================================================
// Task 1: OTLP Log Ingestion (POST /v1/logs)
// ===========================================================================

#[tokio::test]
async fn test_otlp_logs_ingest_and_logql_query() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let ts_ns = 1_700_000_000_000_000_000u64;
    let payload = make_logs_request(
        "auth-svc",
        "user login succeeded",
        SeverityNumber::Info as i32,
        ts_ns,
    );

    let req = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(payload.encode_to_vec()))
        .unwrap();

    let (status, _) = send(&app, req).await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "POST /v1/logs should return 204 on success"
    );

    // Query back via LogQL. The handler should create a stream with a "service"
    // label derived from the resource attribute "service.name".
    let query = urlencoding::encode(r#"{service="auth-svc"}"#);
    let req = Request::builder()
        .method("GET")
        .uri(format!("/loki/api/v1/query?query={}", query))
        .body(Body::empty())
        .unwrap();

    let (status, json) = send(&app, req).await;
    assert_eq!(status, StatusCode::OK);

    let result = &json["data"]["result"];
    let arr = result.as_array().expect("result should be an array");
    assert!(
        !arr.is_empty(),
        "expected at least one stream from OTLP log ingest, got: {}",
        json
    );

    // Verify the log line appears in the values
    let values = arr[0]["values"].as_array().unwrap();
    assert!(!values.is_empty(), "expected at least one log entry");
    let line = values[0][1].as_str().unwrap();
    assert!(
        line.contains("user login succeeded"),
        "log line should contain the ingested body text, got: {}",
        line
    );
}

#[tokio::test]
async fn test_otlp_logs_empty_request_returns_204() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    // Empty request with no resource_logs
    let payload = ExportLogsServiceRequest {
        resource_logs: vec![],
    };

    let req = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(payload.encode_to_vec()))
        .unwrap();

    let (status, _) = send(&app, req).await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "empty OTLP logs request should return 204"
    );
}

#[tokio::test]
async fn test_otlp_logs_malformed_body_returns_400() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let req = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(vec![0xffu8; 64]))
        .unwrap();

    let (status, _) = send(&app, req).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "malformed protobuf should return 400"
    );
}

// ===========================================================================
// Task 2: Diagnose endpoint (GET /api/v1/diagnose) — per-service mode
// ===========================================================================
//
// The diagnose endpoint replaces the former /api/v1/summary endpoint.
// With ?service=<name>, it returns a detailed health assessment including
// recent_errors, key_metrics, slowest_traces, health_score, etc.

#[tokio::test]
async fn test_diagnose_endpoint_returns_error_signals() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let now_ns_str = now_ns.to_string();
    // --- Ingest error log ---
    {
        let push_body = serde_json::json!({
            "streams": [{
                "stream": {"service": "payments", "level": "error"},
                "values": [[&now_ns_str, "payment timeout"]]
            }]
        });
        let req = Request::builder()
            .method("POST")
            .uri("/loki/api/v1/push")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&push_body).unwrap()))
            .unwrap();
        app.clone().oneshot(req).await.unwrap();
    }

    // --- Ingest info log (should NOT appear in recent_errors) ---
    {
        let push_body = serde_json::json!({
            "streams": [{
                "stream": {"service": "payments", "level": "info"},
                "values": [[&now_ns_str, "payment ok"]]
            }]
        });
        let req = Request::builder()
            .method("POST")
            .uri("/loki/api/v1/push")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&push_body).unwrap()))
            .unwrap();
        app.clone().oneshot(req).await.unwrap();
    }

    // --- Ingest error metric (name contains "error") ---
    {
        let ts_ns = now_ns;
        let payload = make_gauge_request_with_labels(
            "payments",
            "http_errors_total",
            5.0,
            ts_ns,
            vec![("code", "500")],
        );
        let req = Request::builder()
            .method("POST")
            .uri("/v1/metrics")
            .header("content-type", "application/x-protobuf")
            .body(Body::from(payload.encode_to_vec()))
            .unwrap();
        app.clone().oneshot(req).await.unwrap();
    }

    // --- Ingest normal metric ---
    ingest_metrics(&app, "payments", "http_requests_total", 100.0, now_ns).await;

    // --- Ingest error trace ---
    let error_trace_id: [u8; 16] = [0xaa; 16];
    ingest_traces(
        &app,
        "payments",
        "charge_card",
        &error_trace_id,
        &[1u8; 8],
        now_ns,
        now_ns + 100_000_000,
        2, // Error
    )
    .await;

    // --- Ingest ok trace ---
    let ok_trace_id: [u8; 16] = [0xbb; 16];
    ingest_traces(
        &app,
        "payments",
        "list_orders",
        &ok_trace_id,
        &[2u8; 8],
        now_ns,
        now_ns + 50_000_000,
        1, // Ok
    )
    .await;

    // --- Query diagnose ---
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/diagnose?service=payments")
        .body(Body::empty())
        .unwrap();

    let (status, json) = send(&app, req).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "diagnose endpoint should return 200"
    );

    // Verify service name
    assert_eq!(json["service"].as_str().unwrap(), "payments");

    // Verify recent_errors present, info logs absent
    let recent_errors = json["recent_errors"].as_array().unwrap();
    assert!(
        !recent_errors.is_empty(),
        "expected error logs in recent_errors, got: {}",
        json
    );
    for log in recent_errors {
        let line = log["line"].as_str().unwrap_or("");
        assert!(
            !line.contains("payment ok"),
            "info log should not appear in recent_errors"
        );
    }

    // Verify slowest_traces present (both traces should appear)
    let slowest = json["slowest_traces"].as_array().unwrap();
    assert!(!slowest.is_empty(), "expected traces in slowest_traces");

    // Health score should be degraded (errors present)
    let health_score = json["health_score"].as_f64().unwrap();
    assert!(
        health_score < 100.0,
        "health_score should be degraded with errors, got: {}",
        health_score
    );
}

#[tokio::test]
async fn test_diagnose_endpoint_unknown_service_returns_healthy() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/diagnose?service=nonexistent")
        .body(Body::empty())
        .unwrap();

    let (status, json) = send(&app, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["service"].as_str().unwrap(), "nonexistent");

    // Unknown service should have perfect health
    let health_score = json["health_score"].as_f64().unwrap();
    assert!(
        (health_score - 100.0).abs() < f64::EPSILON,
        "unknown service should have health_score 100, got: {}",
        health_score
    );

    // Empty arrays for errors/traces
    let recent_errors = json["recent_errors"].as_array().unwrap();
    assert!(
        recent_errors.is_empty(),
        "expected no errors for unknown service"
    );
    let slowest = json["slowest_traces"].as_array().unwrap();
    assert!(slowest.is_empty(), "expected no traces for unknown service");
}

#[tokio::test]
async fn test_diagnose_endpoint_no_service_returns_global_overview() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/diagnose")
        .body(Body::empty())
        .unwrap();

    let (status, json) = send(&app, req).await;
    assert_eq!(status, StatusCode::OK, "global diagnose should return 200");

    // Should have a services array (possibly empty if no data ingested)
    assert!(
        json["services"].is_array(),
        "global diagnose should have services array, got: {}",
        json
    );
}

// ===========================================================================
// Task 3: Time Range Filtering on Trace Search
// ===========================================================================
//
// Expected contract: /api/search accepts optional `start` and `end` query
// params (epoch seconds) alongside `q`. When provided, only traces with spans
// in [start, end] are returned.

#[tokio::test]
async fn test_trace_search_filters_by_epoch_second_range() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    // Three traces at well-separated times
    let t1_ns = 1_700_000_000_000_000_000u64; // epoch second 1700000000
    let t2_ns = 1_700_001_000_000_000_000u64; // +1000s
    let t3_ns = 1_700_002_000_000_000_000u64; // +2000s
    let dur = 100_000_000u64; // 100ms

    let tid1: [u8; 16] = [0x01; 16];
    let tid2: [u8; 16] = [0x02; 16];
    let tid3: [u8; 16] = [0x03; 16];

    ingest_traces(&app, "svc", "op1", &tid1, &[1u8; 8], t1_ns, t1_ns + dur, 2).await;
    ingest_traces(&app, "svc", "op2", &tid2, &[2u8; 8], t2_ns, t2_ns + dur, 2).await;
    ingest_traces(&app, "svc", "op3", &tid3, &[3u8; 8], t3_ns, t3_ns + dur, 2).await;

    // Query with time range that only covers t2
    let query = urlencoding::encode(r#"{ status = error }"#);
    let start_sec = 1_700_000_500u64; // after t1
    let end_sec = 1_700_001_500u64; // before t3
    let uri = format!(
        "/api/search?q={}&start={}&end={}",
        query, start_sec, end_sec
    );

    let req = Request::builder()
        .method("GET")
        .uri(&uri)
        .body(Body::empty())
        .unwrap();

    let (status, json) = send(&app, req).await;
    assert_eq!(status, StatusCode::OK);

    let traces = json["traces"].as_array().unwrap();
    assert_eq!(
        traces.len(),
        1,
        "expected exactly 1 trace in time range, got: {}",
        json
    );
    assert_eq!(
        traces[0]["traceID"].as_str().unwrap(),
        hex_trace_id(&tid2),
        "the trace in range should be tid2"
    );
}

#[tokio::test]
async fn test_trace_search_without_time_range_returns_all() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let t1_ns = 1_700_000_000_000_000_000u64;
    let t2_ns = 1_700_001_000_000_000_000u64;
    let dur = 100_000_000u64;

    let tid1: [u8; 16] = [0x10; 16];
    let tid2: [u8; 16] = [0x20; 16];

    ingest_traces(&app, "svc", "op1", &tid1, &[1u8; 8], t1_ns, t1_ns + dur, 2).await;
    ingest_traces(&app, "svc", "op2", &tid2, &[2u8; 8], t2_ns, t2_ns + dur, 2).await;

    let query = urlencoding::encode(r#"{ status = error }"#);
    let req = Request::builder()
        .method("GET")
        .uri(format!("/api/search?q={}", query))
        .body(Body::empty())
        .unwrap();

    let (status, json) = send(&app, req).await;
    assert_eq!(status, StatusCode::OK);

    let traces = json["traces"].as_array().unwrap();
    assert_eq!(
        traces.len(),
        2,
        "without time range, all matching traces should be returned, got: {}",
        json
    );
}

// ===========================================================================
// Task 4: topk / bottomk PromQL aggregations
// ===========================================================================
//
// Expected contract: topk(k, metric) returns the k series with the highest
// values; bottomk(k, metric) returns the k series with the lowest values.
// The response is a standard Prometheus instant vector.

#[tokio::test]
async fn test_promql_topk_returns_top_k_series() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Ingest 5 series with distinct values via the MetricStore directly
    // to avoid timing issues with the OTLP pipeline.
    {
        let mut store = state.metric_store.write();
        for (i, val) in [10.0, 50.0, 30.0, 20.0, 40.0].iter().enumerate() {
            store.ingest_samples(
                "cpu_usage",
                vec![("instance".into(), format!("node-{}", i))],
                vec![obsidian::store::metric_store::Sample {
                    timestamp_ms: now_ms,
                    value: *val,
                }],
            );
        }
    }

    // topk(3, cpu_usage) should return the 3 highest: 50, 40, 30
    let query = urlencoding::encode("topk(3, cpu_usage)");
    let req = Request::builder()
        .method("GET")
        .uri(format!("/api/v1/query?query={}&time={}", query, now_ms))
        .body(Body::empty())
        .unwrap();

    let (status, json) = send(&app, req).await;
    assert_eq!(status, StatusCode::OK, "topk query should succeed");

    let result = json["data"]["result"].as_array().unwrap();
    assert_eq!(
        result.len(),
        3,
        "topk(3) should return 3 series, got: {}",
        json
    );

    // Extract values and verify they are the top 3
    let mut values: Vec<f64> = result
        .iter()
        .map(|r| r["value"][1].as_str().unwrap().parse::<f64>().unwrap())
        .collect();
    values.sort_by(|a, b| b.partial_cmp(a).unwrap());
    assert_eq!(
        values,
        vec![50.0, 40.0, 30.0],
        "topk(3) values should be [50, 40, 30]"
    );
}

#[tokio::test]
async fn test_promql_bottomk_returns_bottom_k_series() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    {
        let mut store = state.metric_store.write();
        for (i, val) in [10.0, 50.0, 30.0, 20.0, 40.0].iter().enumerate() {
            store.ingest_samples(
                "mem_usage",
                vec![("instance".into(), format!("node-{}", i))],
                vec![obsidian::store::metric_store::Sample {
                    timestamp_ms: now_ms,
                    value: *val,
                }],
            );
        }
    }

    // bottomk(2, mem_usage) should return the 2 lowest: 10, 20
    let query = urlencoding::encode("bottomk(2, mem_usage)");
    let req = Request::builder()
        .method("GET")
        .uri(format!("/api/v1/query?query={}&time={}", query, now_ms))
        .body(Body::empty())
        .unwrap();

    let (status, json) = send(&app, req).await;
    assert_eq!(status, StatusCode::OK, "bottomk query should succeed");

    let result = json["data"]["result"].as_array().unwrap();
    assert_eq!(
        result.len(),
        2,
        "bottomk(2) should return 2 series, got: {}",
        json
    );

    let mut values: Vec<f64> = result
        .iter()
        .map(|r| r["value"][1].as_str().unwrap().parse::<f64>().unwrap())
        .collect();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(
        values,
        vec![10.0, 20.0],
        "bottomk(2) values should be [10, 20]"
    );
}

#[tokio::test]
async fn test_promql_topk_with_k_larger_than_series_count() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    {
        let mut store = state.metric_store.write();
        store.ingest_samples(
            "lone_metric",
            vec![("host".into(), "a".into())],
            vec![obsidian::store::metric_store::Sample {
                timestamp_ms: now_ms,
                value: 42.0,
            }],
        );
    }

    // topk(10, lone_metric) when only 1 series exists should return 1 series
    let query = urlencoding::encode("topk(10, lone_metric)");
    let req = Request::builder()
        .method("GET")
        .uri(format!("/api/v1/query?query={}&time={}", query, now_ms))
        .body(Body::empty())
        .unwrap();

    let (status, json) = send(&app, req).await;
    assert_eq!(status, StatusCode::OK);

    let result = json["data"]["result"].as_array().unwrap();
    assert_eq!(
        result.len(),
        1,
        "topk(10) with 1 series should return 1 series"
    );
    let val: f64 = result[0]["value"][1].as_str().unwrap().parse().unwrap();
    assert!((val - 42.0).abs() < f64::EPSILON);
}
