//! Integration test: OTLP metrics POST -> PromQL query -> verify response.

mod helpers;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use helpers::{ingest_metrics, make_state};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn test_otlp_metrics_and_promql_query() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    ingest_metrics(&app, "payments", "cpu_usage", 0.75, 5_000_000_000).await;

    let query = urlencoding::encode(r#"cpu_usage{service="payments"}"#);
    let uri = format!("/api/v1/query?query={}&time=5", query);
    let req = Request::builder()
        .method("GET")
        .uri(&uri)
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["resultType"], "vector");
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(results.len(), 1);

    let val: f64 = results[0]["value"][1].as_str().unwrap().parse().unwrap();
    assert!((val - 0.75).abs() < 0.01);
}

#[tokio::test]
async fn test_promql_labels_endpoint() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    ingest_metrics(&app, "api", "http_requests", 100.0, 1_000_000_000).await;

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/labels")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "success");
    let label_strs: Vec<&str> = json["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l.as_str().unwrap())
        .collect();
    assert!(label_strs.contains(&"__name__"));
    assert!(label_strs.contains(&"service"));
}
