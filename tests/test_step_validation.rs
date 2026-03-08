//! Integration tests: step=0 must return 400 for range query endpoints.

mod helpers;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use helpers::make_state;
use tower::ServiceExt;

#[tokio::test]
async fn test_promql_query_range_step_zero() {
    let state = make_state();
    let app = obsidian::server::build_router(state);
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/query_range?query=up&start=0&end=1000&step=0s")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_logql_query_range_step_zero() {
    let state = make_state();
    let app = obsidian::server::build_router(state);
    let query = urlencoding::encode(r#"{service="test"}"#);
    let req = Request::builder()
        .method("GET")
        .uri(&format!(
            "/loki/api/v1/query_range?query={}&start=1000000000.0&end=1700000000.0&step=0s",
            query
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}
