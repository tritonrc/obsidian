mod helpers;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use helpers::{ingest_traces, make_gauge_request, make_state};
use http_body_util::BodyExt;
use prost::Message;
use tower::ServiceExt;

async fn json_response(app: &axum::Router, req: Request<Body>) -> (StatusCode, serde_json::Value) {
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json = if body.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&body).unwrap()
    };
    (status, json)
}

#[tokio::test]
async fn test_summary_endpoint_returns_cross_signal_errors() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    let push_body = serde_json::json!({
        "streams": [{
            "stream": {"service": "payments", "level": "error"},
            "values": [[now_ns.to_string(), "payment timeout"]]
        }]
    });
    let req = Request::builder()
        .method("POST")
        .uri("/loki/api/v1/push")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&push_body).unwrap()))
        .unwrap();
    let (status, _) = json_response(&app, req).await;
    assert_eq!(status, StatusCode::OK);

    let metric_req = make_gauge_request("payments", "http_errors_total", 5.0, now_ns);
    let req = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(metric_req.encode_to_vec()))
        .unwrap();
    let (status, _) = json_response(&app, req).await;
    assert_eq!(status, StatusCode::OK);

    ingest_traces(
        &app,
        "payments",
        "charge_card",
        &[0xaa; 16],
        &[0xbb; 8],
        now_ns - 1_000_000,
        now_ns,
        2,
    )
    .await;

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/summary?service=payments")
        .body(Body::empty())
        .unwrap();
    let (status, json) = json_response(&app, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["service"], "payments");
    assert!(!json["data"]["logs"].as_array().unwrap().is_empty());
    assert!(!json["data"]["metrics"].as_array().unwrap().is_empty());
    assert!(!json["data"]["traces"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_summary_endpoint_requires_service_parameter() {
    let app = obsidian::server::build_router(make_state());
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/summary")
        .body(Body::empty())
        .unwrap();
    let (status, json) = json_response(&app, req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json["status"], "error");
}

#[tokio::test]
async fn test_traceql_limit_is_respected_when_query_present() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    for i in 0..3u8 {
        ingest_traces(
            &app,
            "payments",
            "error-span",
            &[i; 16],
            &[i + 1; 8],
            now_ns - 2_000_000,
            now_ns - 1_000_000,
            2,
        )
        .await;
    }

    let query = urlencoding::encode(r#"{ status = error }"#);
    let req = Request::builder()
        .method("GET")
        .uri(format!("/api/search?q={query}&limit=1"))
        .body(Body::empty())
        .unwrap();
    let (status, json) = json_response(&app, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["traces"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn test_otlp_metric_name_collision_is_rejected() {
    let app = obsidian::server::build_router(make_state());

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    let dotted = make_gauge_request("payments", "http.server.duration", 1.0, now_ns);
    let req = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(dotted.encode_to_vec()))
        .unwrap();
    let (status, _) = json_response(&app, req).await;
    assert_eq!(status, StatusCode::OK);

    let underscored = make_gauge_request("payments", "http_server_duration", 2.0, now_ns + 1);
    let req = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(underscored.encode_to_vec()))
        .unwrap();
    let (status, json) = json_response(&app, req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        json["error"]
            .as_str()
            .unwrap()
            .contains("metric name collision")
    );
}
