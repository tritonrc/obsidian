//! Integration tests: multi-service trace grouping and Grafana compatibility.
//!
//! TASK 9: GET /api/traces/{traceID} returns all spans for the trace.
//! TASK 12: Grafana-compatible health/query endpoints return expected shapes.
//!
//! NOTE: TASK 10 (store reset via DELETE /api/v1/reset) and TASK 11
//! (GET /api/v1/openapi.json) were removed because those endpoints do not
//! exist in the current router.

mod helpers;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use helpers::{ingest_traces, make_state};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Helper: send a GET request and return (StatusCode, parsed JSON body).
// ---------------------------------------------------------------------------
async fn json_get(app: &axum::Router, uri: &str) -> (StatusCode, Value) {
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

// ===========================================================================
// TASK 9 — Multi-service trace grouping
// ===========================================================================

/// Ingest spans from two different services under the same trace ID, then
/// retrieve via GET /api/traces/{traceID}. The handler returns a single
/// "batches" entry whose scopeSpans contain ALL spans regardless of service.
#[tokio::test]
async fn test_get_trace_multi_service_spans() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let trace_id: [u8; 16] = [
        0xAA, 0xBB, 0xCC, 0xDD, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0xAA,
        0xBB,
    ];
    let span_id_gw: [u8; 8] = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
    let span_id_pay: [u8; 8] = [0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18];

    // Ingest a gateway span and a payments span under the same trace.
    ingest_traces(
        &app,
        "gateway",
        "GET /checkout",
        &trace_id,
        &span_id_gw,
        1_000_000_000,
        2_000_000_000,
        1, // STATUS_CODE_OK
    )
    .await;

    ingest_traces(
        &app,
        "payments",
        "charge_card",
        &trace_id,
        &span_id_pay,
        1_100_000_000,
        1_900_000_000,
        1,
    )
    .await;

    // Fetch the trace by hex-encoded ID.
    let trace_id_hex = "aabbccdd11223344556677889900aabb";
    let (status, json) = json_get(&app, &format!("/api/traces/{}", trace_id_hex)).await;

    assert_eq!(status, StatusCode::OK, "response: {:?}", json);

    // The response has a top-level "batches" array.
    // With T9 grouping fix, spans are grouped by service into separate batches.
    let batches = json["batches"]
        .as_array()
        .expect("batches should be an array");
    assert_eq!(
        batches.len(),
        2,
        "expected 2 batches (one per service), got {}",
        batches.len()
    );

    // Collect all spans from all batches.
    let mut all_span_ids: Vec<String> = Vec::new();
    for batch in batches {
        let scope_spans = batch["scopeSpans"]
            .as_array()
            .expect("scopeSpans should be an array");
        for ss in scope_spans {
            let spans = ss["spans"].as_array().expect("spans should be an array");
            for span in spans {
                all_span_ids.push(span["spanId"].as_str().unwrap().to_string());
            }
        }
    }

    assert_eq!(
        all_span_ids.len(),
        2,
        "expected 2 total spans in the trace, got {}",
        all_span_ids.len()
    );

    // Verify both span IDs are present.
    assert!(
        all_span_ids.contains(&"0102030405060708".to_string()),
        "gateway span ID missing; got {:?}",
        all_span_ids
    );
    assert!(
        all_span_ids.contains(&"1112131415161718".to_string()),
        "payments span ID missing; got {:?}",
        all_span_ids
    );

    // Verify each batch has a proper service.name resource attribute.
    let mut svc_names: Vec<String> = Vec::new();
    for batch in batches {
        let resource_attrs = &batch["resource"]["attributes"];
        assert!(resource_attrs.is_array());
        let svc_attr = resource_attrs
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["key"] == "service.name")
            .expect("service.name attribute missing");
        svc_names.push(
            svc_attr["value"]["stringValue"]
                .as_str()
                .unwrap()
                .to_string(),
        );
    }
    let svc_name = &svc_names[0];
    assert!(
        svc_name == "gateway" || svc_name == "payments",
        "unexpected service name: {}",
        svc_name
    );
}

/// GET /api/traces/{traceID} with a non-existent trace returns 404.
#[tokio::test]
async fn test_get_trace_not_found() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let (status, json) = json_get(&app, "/api/traces/00000000000000000000000000000000").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(json["error"].is_string(), "expected error message in body");
}

/// GET /api/traces/{traceID} with an invalid (wrong-length) ID returns 400.
#[tokio::test]
async fn test_get_trace_invalid_id() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let (status, json) = json_get(&app, "/api/traces/not-a-valid-hex").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(json["error"].is_string());
}

/// Verify span fields in the get_trace response.
#[tokio::test]
async fn test_get_trace_span_fields() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let trace_id: [u8; 16] = [0x01; 16];
    let span_id: [u8; 8] = [0x02; 8];

    ingest_traces(
        &app,
        "myservice",
        "do_work",
        &trace_id,
        &span_id,
        5_000_000_000, // start
        6_000_000_000, // end
        2,             // STATUS_CODE_ERROR
    )
    .await;

    let trace_hex = "01010101010101010101010101010101";
    let (status, json) = json_get(&app, &format!("/api/traces/{}", trace_hex)).await;
    assert_eq!(status, StatusCode::OK);

    let span = &json["batches"][0]["scopeSpans"][0]["spans"][0];

    // traceId and spanId are lowercase hex.
    assert_eq!(span["traceId"].as_str().unwrap(), trace_hex);
    assert_eq!(span["spanId"].as_str().unwrap(), "0202020202020202");

    // Timestamps are stringified nanoseconds.
    assert_eq!(span["startTimeUnixNano"].as_str().unwrap(), "5000000000");
    // endTimeUnixNano = start + duration. The OTLP ingest handler computes
    // duration = end_ns - start_ns = 1_000_000_000.
    assert_eq!(span["endTimeUnixNano"].as_str().unwrap(), "6000000000");

    // Status code 2 maps to SpanStatus::Error.
    assert_eq!(span["status"]["code"], 2);

    // The span name is resolved from the interner.
    assert_eq!(span["name"].as_str().unwrap(), "do_work");
}

// ===========================================================================
// TASK 12 — Grafana compatibility endpoints
// ===========================================================================

/// GET /ready returns {"status": "ready"} — Grafana datasource health check.
#[tokio::test]
async fn test_ready_endpoint() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let (status, json) = json_get(&app, "/ready").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "ready");
}

/// GET /api/v1/status returns store statistics under a "data" key.
#[tokio::test]
async fn test_status_endpoint() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let (status, json) = json_get(&app, "/api/v1/status").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "success");

    let data = &json["data"];
    // All stat fields should be present and zero on a fresh store.
    assert_eq!(data["totalLogEntries"], 0);
    assert_eq!(data["totalMetricSeries"], 0);
    assert_eq!(data["totalMetricSamples"], 0);
    assert_eq!(data["totalSpans"], 0);
    assert_eq!(data["totalTraces"], 0);
    assert!(data["uptimeSeconds"].is_number());
    assert_eq!(data["serviceCount"], 0);
    assert!(data["memoryBytes"].is_number());
}

/// GET /api/v1/query?query=1%2B1 (PromQL "1+1") returns a scalar result.
/// Grafana sends simple arithmetic probes to verify datasource connectivity.
#[tokio::test]
async fn test_promql_scalar_arithmetic() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    // "1+1" URL-encoded is "1%2B1".
    let (status, json) = json_get(&app, "/api/v1/query?query=1%2B1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["resultType"], "scalar");

    // The result is [timestamp_seconds, "value_string"].
    let result = &json["data"]["result"];
    assert!(
        result.is_array(),
        "scalar result should be a 2-element array"
    );
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 2);

    // The value is the string "2".
    assert_eq!(arr[1].as_str().unwrap(), "2");
}

/// GET /api/v1/query?query=42 returns scalar 42 — bare number literal.
#[tokio::test]
async fn test_promql_number_literal() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let (status, json) = json_get(&app, "/api/v1/query?query=42").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["resultType"], "scalar");
    let result = json["data"]["result"].as_array().unwrap();
    assert_eq!(result[1].as_str().unwrap(), "42");
}

/// GET /api/v1/labels on an empty store returns an empty list (not an error).
/// Grafana calls this to populate the label-name dropdown.
#[tokio::test]
async fn test_promql_labels_empty_store() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let (status, json) = json_get(&app, "/api/v1/labels").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "success");
    assert!(json["data"].is_array());
}

/// GET /api/v1/services on an empty store returns an empty list.
#[tokio::test]
async fn test_services_empty_store() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let (status, json) = json_get(&app, "/api/v1/services").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "success");
    let services = json["data"]["services"].as_array().unwrap();
    assert!(services.is_empty());
}
