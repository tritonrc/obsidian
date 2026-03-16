//! Integration test: Loki push JSON -> LogQL query -> verify response.

mod helpers;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use helpers::make_state;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

#[tokio::test]
async fn test_loki_push_and_logql_query() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let push_body = json!({
        "streams": [{
            "stream": {"service": "payments", "level": "error"},
            "values": [
                ["1700000000000000000", "connection timeout to bank API"],
                ["1700000000100000000", "retry 1/3 failed"]
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

    let req = Request::builder()
        .method("GET")
        .uri("/loki/api/v1/query?query=%7Bservice%3D%22payments%22%7D&time=1700000000200000000")
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["resultType"], "streams");
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["stream"]["service"], "payments");
    let values = results[0]["values"].as_array().unwrap();
    assert_eq!(values.len(), 2);
}

#[tokio::test]
async fn test_loki_push_and_logql_query_with_filter() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let push_body = json!({
        "streams": [{
            "stream": {"service": "payments", "level": "error"},
            "values": [
                ["1700000000000000000", "connection timeout"],
                ["1700000000100000000", "healthcheck ok"]
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

    let query = urlencoding::encode(r#"{service="payments"} |= "timeout""#);
    let uri = format!(
        "/loki/api/v1/query?query={}&time=1700000000200000000",
        query
    );
    let req = Request::builder()
        .method("GET")
        .uri(&uri)
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "success");
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(results.len(), 1);
    let values = results[0]["values"].as_array().unwrap();
    assert_eq!(values.len(), 1);
    assert!(values[0][1].as_str().unwrap().contains("timeout"));
}

#[tokio::test]
async fn test_logql_labels_endpoint() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let push_body = json!({
        "streams": [{
            "stream": {"service": "payments", "level": "error"},
            "values": [["1700000000000000000", "test"]]
        }]
    });

    let req = Request::builder()
        .method("POST")
        .uri("/loki/api/v1/push")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&push_body).unwrap()))
        .unwrap();
    app.clone().oneshot(req).await.unwrap();

    let req = Request::builder()
        .method("GET")
        .uri("/loki/api/v1/labels")
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
    assert!(label_strs.contains(&"service"));
    assert!(label_strs.contains(&"level"));

    let req = Request::builder()
        .method("GET")
        .uri("/loki/api/v1/label/service/values")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "success");
    let values = json["data"].as_array().unwrap();
    assert!(values.iter().any(|v| v.as_str() == Some("payments")));
}
