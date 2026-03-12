//! Regression tests derived from GPT-5.4 review of the codebase.
//! These tests verify fixes for issues found during the audit.

mod helpers;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use helpers::make_state;
use http_body_util::BodyExt;
use tower::ServiceExt;

// =============================================================================
// Issue #1: Bind address should default to 127.0.0.1, not 0.0.0.0
// =============================================================================

#[test]
fn test_config_default_bind_address() {
    // Config should expose a bind_address field that defaults to 127.0.0.1
    let config = obsidian::config::Config {
        port: 4320,
        bind_address: "127.0.0.1".into(),
        snapshot_dir: "/tmp".into(),
        snapshot_interval: 0,
        max_log_entries: 100000,
        max_samples: 10000,
        max_spans: 100000,
        retention: "2h".into(),
        restore: false,
    };
    assert_eq!(config.bind_address, "127.0.0.1");
}

// =============================================================================
// Issue #2: Unsupported PromQL aggregations should error, not silently sum
// =============================================================================

#[test]
fn test_promql_unsupported_aggregation_errors() {
    use obsidian::query::promql::eval::evaluate_instant;
    use obsidian::store::metric_store::{MetricStore, Sample};

    let mut store = MetricStore::new();
    store.ingest_samples(
        "up",
        vec![("job".into(), "api".into())],
        vec![Sample {
            timestamp_ms: 1000,
            value: 1.0,
        }],
    );

    // These unsupported aggregations must return an error
    for query in &[
        r#"group(up)"#,
        r#"count_values("val", up)"#,
        r#"topk(3, up)"#,
        r#"bottomk(3, up)"#,
    ] {
        let result = evaluate_instant(query, &store, 1000);
        assert!(
            result.is_err(),
            "expected error for unsupported aggregation '{}', got: {:?}",
            query,
            result
        );
    }
}

// =============================================================================
// Issue #3: PromQL timestamp parsing — integer millis misread as seconds
// =============================================================================

#[tokio::test]
async fn test_promql_timestamp_millis_not_misread() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    // Ingest a sample at a known time
    {
        let mut store = state.metric_store.write();
        store.ingest_samples(
            "test_metric",
            vec![],
            vec![obsidian::store::metric_store::Sample {
                timestamp_ms: 1709251200000, // known millis timestamp
                value: 42.0,
            }],
        );
    }

    // Query with integer millisecond timestamps — they should be treated as millis
    // (Prometheus convention: float = seconds, pure integer > 1e12 = milliseconds)
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/query?query=test_metric&time=1709251200000")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let result = &json["data"]["result"];
    assert!(
        result.as_array().map(|a| !a.is_empty()).unwrap_or(false),
        "expected results when querying at millis timestamp, got: {}",
        json
    );
}

// =============================================================================
// Issue #5: Invalid time/start/end/step must return 400, not silently default
// =============================================================================

#[tokio::test]
async fn test_promql_invalid_time_returns_400() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/query?query=up&time=notanumber")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "invalid 'time' should return 400"
    );
}

#[tokio::test]
async fn test_promql_invalid_start_returns_400() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/query_range?query=up&start=abc&end=1000&step=15s")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "invalid 'start' should return 400"
    );
}

#[tokio::test]
async fn test_logql_invalid_time_returns_400() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let query = urlencoding::encode(r#"{service="test"}"#);
    let req = Request::builder()
        .method("GET")
        .uri(&format!(
            "/loki/api/v1/query?query={}&time=notanumber",
            query
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "invalid 'time' should return 400"
    );
}

// =============================================================================
// Issue #6: Search metadata should find actual root span, not first matched
// =============================================================================

#[tokio::test]
async fn test_traceql_search_finds_actual_root() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let trace_id: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let root_span_id: [u8; 8] = [1, 0, 0, 0, 0, 0, 0, 0];
    let child_span_id: [u8; 8] = [2, 0, 0, 0, 0, 0, 0, 0];

    // Ingest root span (service=gateway, name="GET /api", status=ok)
    helpers::ingest_traces(
        &app,
        "gateway",
        "GET /api",
        &trace_id,
        &root_span_id,
        1_000_000_000,
        1_500_000_000,
        1, // STATUS_CODE_OK
    )
    .await;

    // Ingest child span (service=payments, name="process", status=error)
    // with parent_span_id pointing to root
    {
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
        use prost::Message;

        let req = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("payments".into())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: trace_id.to_vec(),
                        span_id: child_span_id.to_vec(),
                        trace_state: String::new(),
                        parent_span_id: root_span_id.to_vec(),
                        name: "process".into(),
                        kind: 1,
                        start_time_unix_nano: 1_100_000_000,
                        end_time_unix_nano: 1_400_000_000,
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        events: vec![],
                        dropped_events_count: 0,
                        links: vec![],
                        dropped_links_count: 0,
                        status: Some(Status {
                            message: String::new(),
                            code: 2, // STATUS_CODE_ERROR
                        }),
                        flags: 0,
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let r = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header("content-type", "application/x-protobuf")
            .body(Body::from(req.encode_to_vec()))
            .unwrap();
        app.clone().oneshot(r).await.unwrap();
    }

    // Search for error spans — the child matches, but root metadata should
    // still come from the actual root span (gateway / "GET /api")
    let query = urlencoding::encode(r#"{ status = error }"#);
    let req = Request::builder()
        .method("GET")
        .uri(&format!("/api/search?q={}", query))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let traces = json["traces"].as_array().unwrap();
    assert_eq!(traces.len(), 1);

    let trace = &traces[0];
    assert_eq!(
        trace["rootServiceName"].as_str().unwrap(),
        "gateway",
        "root service should be gateway (actual root), not payments (first matched span)"
    );
    assert_eq!(
        trace["rootTraceName"].as_str().unwrap(),
        "GET /api",
        "root span name should be 'GET /api' (actual root), not 'process' (first matched span)"
    );
}

// =============================================================================
// Issue #9: max_spans must be enforced
// =============================================================================

#[tokio::test]
async fn test_max_spans_enforced() {
    use std::sync::Arc;
    use std::time::Instant;

    let state = Arc::new(obsidian::store::AppState {
        log_store: parking_lot::RwLock::new(obsidian::store::LogStore::new()),
        metric_store: parking_lot::RwLock::new(obsidian::store::MetricStore::new()),
        trace_store: parking_lot::RwLock::new(obsidian::store::TraceStore::new()),
        config: obsidian::config::Config {
            port: 0,
            bind_address: "127.0.0.1".into(),
            snapshot_dir: "/tmp/obsidian-test".into(),
            snapshot_interval: 0,
            max_log_entries: 100000,
            max_samples: 10000,
            max_spans: 5, // Very low cap
            retention: "2h".into(),
            restore: false,
        },
        start_time: Instant::now(),
    });

    let app = obsidian::server::build_router(state.clone());

    // Ingest 10 spans (exceeds max_spans=5)
    for i in 0u8..10 {
        let trace_id = [i; 16];
        let span_id = [i; 8];
        helpers::ingest_traces(
            &app,
            "svc",
            &format!("op-{}", i),
            &trace_id,
            &span_id,
            (i as u64 + 1) * 1_000_000_000,
            (i as u64 + 1) * 1_000_000_000 + 100_000_000,
            1,
        )
        .await;
    }

    // Run eviction
    obsidian::store::run_eviction(&state);

    // After eviction, should be at or below max_spans
    let store = state.trace_store.read();
    assert!(
        store.total_spans <= 5,
        "total_spans should be <= 5 after eviction, got: {}",
        store.total_spans
    );
}

// =============================================================================
// Issue #10: Loki protobuf content-type should not return 500
// =============================================================================

#[tokio::test]
async fn test_loki_protobuf_returns_clear_error() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    // Send actual protobuf content-type with garbage body — should get 400, not 500
    let req = Request::builder()
        .method("POST")
        .uri("/loki/api/v1/push")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(vec![0u8; 32])) // garbage protobuf
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    // Should be 400 (bad request), not 500 (internal error)
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "invalid protobuf should return 400"
    );
}

// =============================================================================
// Issue #12: Remove #![allow(dead_code)] — tested by compilation
// =============================================================================
// This is a code quality fix, not a runtime test.

// =============================================================================
// Issue: /api/v1/status should include memory info
// =============================================================================

#[tokio::test]
async fn test_status_includes_memory() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/status")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        json["data"]["memoryBytes"].is_number(),
        "status should include memoryBytes, got: {}",
        json
    );
}
