//! Integration test: ingest from multiple services -> GET /api/v1/services -> verify all listed.

mod helpers;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use helpers::{ingest_metrics, ingest_traces, make_state, push_logs};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

#[tokio::test]
async fn test_services_endpoint_all_signals() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    push_logs(&app, "payments", "payment ok", "1700000000000000000").await;
    ingest_metrics(&app, "gateway", "cpu_usage", 0.5, 1_000_000_000).await;
    ingest_traces(
        &app,
        "worker",
        "process_job",
        &[1; 16],
        &[2; 8],
        1_000_000_000,
        2_000_000_000,
        1,
    )
    .await;

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/services")
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "success");
    let services = json["data"]["services"].as_array().unwrap();
    assert_eq!(services.len(), 3);

    assert_eq!(services[0]["name"], "gateway");
    assert!(services[0]["signals"]
        .as_array()
        .unwrap()
        .contains(&json!("metrics")));
    assert_eq!(services[1]["name"], "payments");
    assert!(services[1]["signals"]
        .as_array()
        .unwrap()
        .contains(&json!("logs")));
    assert_eq!(services[2]["name"], "worker");
    assert!(services[2]["signals"]
        .as_array()
        .unwrap()
        .contains(&json!("traces")));
}

#[tokio::test]
async fn test_services_endpoint_multi_signal_service() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    push_logs(&app, "payments", "payment ok", "1700000000000000000").await;
    ingest_metrics(&app, "payments", "request_count", 42.0, 1_000_000_000).await;

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/services")
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    let services = json["data"]["services"].as_array().unwrap();
    assert_eq!(services.len(), 1);
    assert_eq!(services[0]["name"], "payments");

    let signals = services[0]["signals"].as_array().unwrap();
    assert!(signals.contains(&json!("logs")));
    assert!(signals.contains(&json!("metrics")));
}
