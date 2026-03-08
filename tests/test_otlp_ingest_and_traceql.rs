//! Integration test: OTLP traces POST -> TraceQL query -> verify response.

mod helpers;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use helpers::{ingest_traces, make_state};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn test_otlp_traces_and_traceql_query() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let trace_id: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let span_id: [u8; 8] = [1, 2, 3, 4, 5, 6, 7, 8];

    ingest_traces(
        &app,
        "payments",
        "process_payment",
        &trace_id,
        &span_id,
        1_000_000_000,
        2_000_000_000,
        2, // Error status
    )
    .await;

    let query = urlencoding::encode(r#"{ status = error }"#);
    let uri = format!("/api/search?q={}", query);
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
    assert_eq!(traces.len(), 1);
}

#[tokio::test]
async fn test_get_trace_by_id() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let trace_id: [u8; 16] = [
        0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67,
        0x89,
    ];
    let span_id: [u8; 8] = [1, 2, 3, 4, 5, 6, 7, 8];

    ingest_traces(
        &app,
        "gateway",
        "GET /api",
        &trace_id,
        &span_id,
        1_000_000_000,
        1_500_000_000,
        1,
    )
    .await;

    let uri = format!("/api/traces/{}", "abcdef0123456789abcdef0123456789");
    let req = Request::builder()
        .method("GET")
        .uri(&uri)
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    let batches = json["batches"].as_array().unwrap();
    assert!(!batches.is_empty());
    let spans = batches[0]["scopeSpans"][0]["spans"].as_array().unwrap();
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0]["name"], "GET /api");
}
