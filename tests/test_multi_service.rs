//! Integration test: multiple services -> query each independently and together.

mod helpers;

use axum::body::Body;
use axum::http::Request;
use helpers::{make_state, push_logs};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn test_multi_service_query_independently() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    push_logs(&app, "payments", "payment processed", "1700000000000000000").await;
    push_logs(&app, "gateway", "request received", "1700000000100000000").await;

    // Query payments only
    let query = urlencoding::encode(r#"{service="payments"}"#);
    let uri = format!("/loki/api/v1/query?query={}&time=1700000000200000000", query);
    let req = Request::builder().method("GET").uri(&uri).body(Body::empty()).unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["stream"]["service"], "payments");

    // Query gateway only
    let query = urlencoding::encode(r#"{service="gateway"}"#);
    let uri = format!("/loki/api/v1/query?query={}&time=1700000000200000000", query);
    let req = Request::builder().method("GET").uri(&uri).body(Body::empty()).unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["stream"]["service"], "gateway");
}

#[tokio::test]
async fn test_multi_service_query_with_regex() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    push_logs(&app, "payments", "payment processed", "1700000000000000000").await;
    push_logs(&app, "gateway", "request received", "1700000000100000000").await;
    push_logs(&app, "worker", "job completed", "1700000000200000000").await;

    let query = urlencoding::encode(r#"{service=~"payments|gateway"}"#);
    let uri = format!("/loki/api/v1/query?query={}&time=1700000000300000000", query);
    let req = Request::builder().method("GET").uri(&uri).body(Body::empty()).unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(results.len(), 2);
}
