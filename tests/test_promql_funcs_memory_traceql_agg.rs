//! Integration tests for P2 features:
//! - TASK 17: PromQL offset modifier
//! - TASK 18: label_replace / label_join PromQL functions
//! - TASK 19: Better memory estimation in /api/v1/status
//! - TASK 20: TraceQL count() aggregate
//! - TASK 21: Summary metric type

mod helpers;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use helpers::{ingest_metrics, ingest_traces, make_state, push_logs};
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{
    Metric, ResourceMetrics, ScopeMetrics, Summary, SummaryDataPoint, metric,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use serde_json::Value;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn get_json(app: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    (status, json)
}

async fn instant_query(app: &axum::Router, query: &str, time_s: i64) -> Value {
    let query = urlencoding::encode(query);
    let uri = format!("/api/v1/query?query={query}&time={time_s}");
    let (status, json) = get_json(app, &uri).await;
    assert_eq!(status, StatusCode::OK, "unexpected query response: {json}");
    json
}

async fn status_json(app: &axum::Router) -> Value {
    let (status, json) = get_json(app, "/api/v1/status").await;
    assert_eq!(status, StatusCode::OK, "unexpected status response: {json}");
    json
}

fn single_vector_value(json: &Value) -> f64 {
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(
        results.len(),
        1,
        "expected a single series result, got: {json}"
    );
    results[0]["value"][1].as_str().unwrap().parse().unwrap()
}

fn single_metric_labels(json: &Value) -> &Value {
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(
        results.len(),
        1,
        "expected a single series result, got: {json}"
    );
    &results[0]["metric"]
}

async fn post_metrics(app: &axum::Router, payload: ExportMetricsServiceRequest) -> StatusCode {
    let req = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(payload.encode_to_vec()))
        .unwrap();

    app.clone().oneshot(req).await.unwrap().status()
}

fn make_summary_request(service_name: &str, ts_ns: u64) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(service_name.into())),
                    }),
                }],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "request_latency_seconds".into(),
                    data: Some(metric::Data::Summary(Summary {
                        data_points: vec![SummaryDataPoint {
                            time_unix_nano: ts_ns,
                            count: 3,
                            sum: 4.5,
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

// ---------------------------------------------------------------------------
// TASK 17: PromQL offset modifier
// ---------------------------------------------------------------------------

/// Ingests data at two time windows (around 60-120s and 3660-3720s), then
/// queries with `offset 1h` at time=3720s. The offset should shift the
/// evaluation window back by 1h so it looks at the 60-120s data.
/// rate = (70 - 10) / 300 = 0.2 per second.
#[tokio::test]
async fn test_promql_offset_modifier_looks_back_one_hour() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    // Early window: 60s, 120s
    ingest_metrics(
        &app,
        "payments",
        "http_requests_total",
        10.0,
        60_000_000_000,
    )
    .await;
    ingest_metrics(
        &app,
        "payments",
        "http_requests_total",
        70.0,
        120_000_000_000,
    )
    .await;

    // Late window: 3660s, 3720s
    ingest_metrics(
        &app,
        "payments",
        "http_requests_total",
        1_000.0,
        3_660_000_000_000,
    )
    .await;
    ingest_metrics(
        &app,
        "payments",
        "http_requests_total",
        1_600.0,
        3_720_000_000_000,
    )
    .await;

    let json = instant_query(
        &app,
        r#"rate(http_requests_total{service="payments"}[5m] offset 1h)"#,
        3720,
    )
    .await;

    assert_eq!(json["status"], "success");
    let value = single_vector_value(&json);
    assert!(
        (value - 0.2).abs() < 0.000_001,
        "expected offset rate to use the older window, got {value}"
    );
}

// ---------------------------------------------------------------------------
// TASK 18: label_replace / label_join
// ---------------------------------------------------------------------------

/// label_replace should create a new label derived from an existing one.
#[tokio::test]
async fn test_promql_label_replace_creates_new_label() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    ingest_metrics(&app, "payments", "http_requests_total", 42.0, 5_000_000_000).await;

    let json = instant_query(
        &app,
        r#"label_replace(http_requests_total{service="payments"}, "service_copy", "$1-copy", "service", "(.*)")"#,
        5,
    )
    .await;

    assert_eq!(json["status"], "success");
    let metric = single_metric_labels(&json);
    assert_eq!(metric["service"], "payments");
    assert_eq!(metric["service_copy"], "payments-copy");
}

/// label_join should combine multiple label values into a new label.
#[tokio::test]
async fn test_promql_label_join_combines_labels() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    ingest_metrics(&app, "payments", "http_requests_total", 42.0, 5_000_000_000).await;

    let json = instant_query(
        &app,
        r#"label_join(http_requests_total{service="payments"}, "joined", "/", "__name__", "service")"#,
        5,
    )
    .await;

    assert_eq!(json["status"], "success");
    let metric = single_metric_labels(&json);
    assert_eq!(metric["joined"], "http_requests_total/payments");
}

// ---------------------------------------------------------------------------
// TASK 19: Better memory estimation in /api/v1/status
// ---------------------------------------------------------------------------

/// After ingesting more data the reported memoryBytes must increase.
#[tokio::test]
async fn test_status_memory_bytes_changes_as_more_data_is_ingested() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    // Seed some initial data across all three stores.
    push_logs(&app, "payments", "first log line", "1000000000").await;
    ingest_metrics(&app, "payments", "http_requests_total", 1.0, 1_000_000_000).await;

    let trace_a: [u8; 16] = [0x11; 16];
    let span_a: [u8; 8] = [0x01; 8];
    ingest_traces(
        &app,
        "payments",
        "initial-span",
        &trace_a,
        &span_a,
        1_000_000_000,
        2_000_000_000,
        1,
    )
    .await;

    let first_status = status_json(&app).await;
    let first_memory = first_status["data"]["memoryBytes"].as_u64().unwrap();
    assert!(
        first_memory > 0,
        "expected memoryBytes > 0, got: {first_status}"
    );

    // Ingest more data.
    push_logs(&app, "payments", "second log line", "2000000000").await;
    push_logs(&app, "payments", "third log line", "3000000000").await;
    ingest_metrics(&app, "payments", "http_requests_total", 2.0, 2_000_000_000).await;
    ingest_metrics(&app, "payments", "http_requests_total", 3.0, 3_000_000_000).await;

    let trace_b: [u8; 16] = [0x22; 16];
    let span_b: [u8; 8] = [0x02; 8];
    ingest_traces(
        &app,
        "payments",
        "second-span",
        &trace_b,
        &span_b,
        3_000_000_000,
        4_000_000_000,
        2,
    )
    .await;

    let second_status = status_json(&app).await;
    let second_memory = second_status["data"]["memoryBytes"].as_u64().unwrap();

    assert!(
        second_memory > first_memory,
        "expected memoryBytes to increase after more ingest; before={first_memory}, after={second_memory}"
    );
}

// ---------------------------------------------------------------------------
// TASK 20: TraceQL count() aggregate
// ---------------------------------------------------------------------------

/// Ingest two traces: one with 6 error spans, one with 2 error spans.
/// A query `{ status = error } | count() > 5` should return only the
/// trace with 6 matching spans.
#[tokio::test]
async fn test_traceql_count_aggregate_filters_traces_by_error_span_count() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    // Trace A: 6 error spans (status_code 2 = Error in OTLP)
    let matching_trace_id: [u8; 16] = [0xaa; 16];
    for idx in 0..6u8 {
        let span_id = [idx + 1; 8];
        ingest_traces(
            &app,
            "payments",
            "error-span",
            &matching_trace_id,
            &span_id,
            1_000_000_000 + idx as u64 * 1_000_000,
            2_000_000_000 + idx as u64 * 1_000_000,
            2,
        )
        .await;
    }

    // Trace B: 2 error spans (should NOT match count() > 5)
    let non_matching_trace_id: [u8; 16] = [0xbb; 16];
    for idx in 0..2u8 {
        let span_id = [idx + 11; 8];
        ingest_traces(
            &app,
            "payments",
            "error-span",
            &non_matching_trace_id,
            &span_id,
            3_000_000_000 + idx as u64 * 1_000_000,
            4_000_000_000 + idx as u64 * 1_000_000,
            2,
        )
        .await;
    }

    let query = urlencoding::encode(r#"{ status = error } | count() > 5"#);
    let uri = format!("/api/search?q={query}");
    let (status, json) = get_json(&app, &uri).await;

    assert_eq!(
        status,
        StatusCode::OK,
        "unexpected traceql response: {json}"
    );

    let traces = json["traces"].as_array().unwrap();
    assert_eq!(
        traces.len(),
        1,
        "expected only the high-error trace to match: {json}"
    );
    assert_eq!(
        traces[0]["traceID"].as_str().unwrap(),
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    assert_eq!(traces[0]["spanSets"][0]["matched"], 6);
}

// ---------------------------------------------------------------------------
// TASK 21: Summary metric type
// ---------------------------------------------------------------------------

/// Ingest an ExportMetricsServiceRequest containing Summary data points.
/// After ingestion the derived _count series should be queryable via PromQL.
#[tokio::test]
async fn test_otlp_metrics_support_summary() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let payload = make_summary_request("payments", 10_000_000_000);
    // The metrics handler returns 200 with a JSON body containing accepted counts.
    let status = post_metrics(&app, payload).await;
    assert_eq!(status, StatusCode::OK);

    // Query the derived _count series for the summary metric.
    let summary_count = instant_query(
        &app,
        r#"request_latency_seconds_count{service="payments"}"#,
        10,
    )
    .await;
    assert_eq!(summary_count["status"], "success");
    assert_eq!(single_vector_value(&summary_count), 3.0);
}
