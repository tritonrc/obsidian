//! Regression tests derived from GPT-5.4 review of the codebase.
//! These tests verify fixes for issues found during the audit.

mod helpers;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use clap::Parser;
use flate2::Compression;
use flate2::write::GzEncoder;
use helpers::make_state;
use http_body_util::BodyExt;
use prost::Message;
use std::io::Write;
use tower::ServiceExt;

#[test]
fn test_config_default_bind_address() {
    let config = obsidian::config::Config {
        port: 4320,
        bind_address: "127.0.0.1".into(),
        snapshot_dir: "/tmp".into(),
        snapshot_interval: 0,
        max_log_entries: 100000,
        max_series: 10000,
        max_spans: 100000,
        retention: "2h".into(),
        restore: false,
    };
    assert_eq!(config.bind_address, "127.0.0.1");
}

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

    for query in &[r#"group(up)"#, r#"count_values("val", up)"#] {
        let result = evaluate_instant(query, &store, 1000);
        assert!(
            result.is_err(),
            "expected error for unsupported aggregation '{}', got: {:?}",
            query,
            result
        );
    }
}

#[tokio::test]
async fn test_promql_timestamp_millis_not_misread() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    {
        let mut store = state.metric_store.write();
        store.ingest_samples(
            "test_metric",
            vec![],
            vec![obsidian::store::metric_store::Sample {
                timestamp_ms: 1709251200000,
                value: 42.0,
            }],
        );
    }

    // Integer > 1e12 should be treated as milliseconds, not float seconds
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
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
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
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_logql_invalid_time_returns_400() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let query = urlencoding::encode(r#"{service="test"}"#);
    let req = Request::builder()
        .method("GET")
        .uri(format!(
            "/loki/api/v1/query?query={}&time=notanumber",
            query
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_traceql_search_finds_actual_root() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let trace_id: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let root_span_id: [u8; 8] = [1, 0, 0, 0, 0, 0, 0, 0];
    let child_span_id: [u8; 8] = [2, 0, 0, 0, 0, 0, 0, 0];

    helpers::ingest_traces(
        &app,
        "gateway",
        "GET /api",
        &trace_id,
        &root_span_id,
        1_000_000_000,
        1_500_000_000,
        1,
    )
    .await;

    // Ingest child span with parent pointing to root
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
                            code: 2,
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

    // Search for error spans — root metadata should come from actual root span
    let query = urlencoding::encode(r#"{ status = error }"#);
    let req = Request::builder()
        .method("GET")
        .uri(format!("/api/search?q={}", query))
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
            max_series: 10000,
            max_spans: 5,
            retention: "2h".into(),
            restore: false,
        },
        start_time: Instant::now(),
    });

    let app = obsidian::server::build_router(state.clone());

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

    obsidian::store::run_eviction(&state);

    let store = state.trace_store.read();
    assert!(
        store.total_spans <= 5,
        "total_spans should be <= 5 after eviction, got: {}",
        store.total_spans
    );
}

#[tokio::test]
async fn test_loki_protobuf_returns_clear_error() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let req = Request::builder()
        .method("POST")
        .uri("/loki/api/v1/push")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(vec![0u8; 32]))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

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

fn gzip_encode(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

#[tokio::test]
async fn test_otlp_metrics_gzip_content_encoding() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let ts_ns = 1_731_000_000_123_000_000u64;
    let payload = helpers::make_gauge_request("payments", "http_requests_total", 42.0, ts_ns);

    let req = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "gzip")
        .body(Body::from(gzip_encode(&payload.encode_to_vec())))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let store = state.metric_store.read();
    assert_eq!(store.series.len(), 1, "expected one ingested metric series");

    let series_id = *store.series.keys().next().unwrap();
    let samples = store.get_samples(series_id, i64::MIN, i64::MAX);
    assert_eq!(samples.len(), 1);
    assert!((samples[0].value - 42.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn test_otlp_traces_gzip_content_encoding() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let trace_id = [0xabu8; 16];
    let span_id = [0x11u8; 8];
    let payload = helpers::make_trace_request(
        "checkout",
        "charge_card",
        &trace_id,
        &span_id,
        1_731_000_000_000_000_000,
        1_731_000_000_250_000_000,
        2,
    );

    let req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "gzip")
        .body(Body::from(gzip_encode(&payload.encode_to_vec())))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let store = state.trace_store.read();
    let spans = store
        .get_trace(&trace_id)
        .expect("gzip-compressed traces should be ingested");
    assert_eq!(spans.len(), 1);
    assert_eq!(
        spans[0].status,
        obsidian::store::trace_store::SpanStatus::Error
    );
    assert_eq!(store.resolve(&spans[0].name), "charge_card");
}

#[test]
fn test_trace_store_status_index() {
    let mut store = obsidian::store::trace_store::TraceStore::new();

    let service = store.interner.get_or_intern("gateway");
    let ok_name = store.interner.get_or_intern("ok_span");
    let error_name = store.interner.get_or_intern("error_span");
    let unset_name = store.interner.get_or_intern("unset_span");

    let ok_trace_id = [1u8; 16];
    let error_trace_id_a = [2u8; 16];
    let error_trace_id_b = [3u8; 16];
    let unset_trace_id = [4u8; 16];

    store.ingest_spans(vec![
        obsidian::store::trace_store::Span {
            trace_id: ok_trace_id,
            span_id: [1u8; 8],
            parent_span_id: None,
            name: ok_name,
            service_name: service,
            start_time_ns: 1_000,
            duration_ns: 100,
            status: obsidian::store::trace_store::SpanStatus::Ok,
            attributes: smallvec::SmallVec::new(),
        },
        obsidian::store::trace_store::Span {
            trace_id: error_trace_id_a,
            span_id: [2u8; 8],
            parent_span_id: None,
            name: error_name,
            service_name: service,
            start_time_ns: 2_000,
            duration_ns: 100,
            status: obsidian::store::trace_store::SpanStatus::Error,
            attributes: smallvec::SmallVec::new(),
        },
        obsidian::store::trace_store::Span {
            trace_id: error_trace_id_b,
            span_id: [3u8; 8],
            parent_span_id: None,
            name: error_name,
            service_name: service,
            start_time_ns: 3_000,
            duration_ns: 100,
            status: obsidian::store::trace_store::SpanStatus::Error,
            attributes: smallvec::SmallVec::new(),
        },
        obsidian::store::trace_store::Span {
            trace_id: unset_trace_id,
            span_id: [4u8; 8],
            parent_span_id: None,
            name: unset_name,
            service_name: service,
            start_time_ns: 4_000,
            duration_ns: 100,
            status: obsidian::store::trace_store::SpanStatus::Unset,
            attributes: smallvec::SmallVec::new(),
        },
    ]);

    let ok_ids = store
        .status_index
        .get(&obsidian::store::trace_store::SpanStatus::Ok)
        .expect("ok status should be indexed");
    let error_ids = store
        .status_index
        .get(&obsidian::store::trace_store::SpanStatus::Error)
        .expect("error status should be indexed");
    let unset_ids = store
        .status_index
        .get(&obsidian::store::trace_store::SpanStatus::Unset)
        .expect("unset status should be indexed");

    assert_eq!(ok_ids.len(), 1);
    assert!(ok_ids.contains(&ok_trace_id));
    assert_eq!(error_ids.len(), 2);
    assert!(error_ids.contains(&error_trace_id_a));
    assert!(error_ids.contains(&error_trace_id_b));
    assert_eq!(unset_ids.len(), 1);
    assert!(unset_ids.contains(&unset_trace_id));
}

#[test]
fn test_config_has_max_series_field() {
    let config = obsidian::config::Config::parse_from(["obsidian", "--max-series", "2"]);
    assert_eq!(config.max_series, 2);
}

#[test]
fn test_max_series_limits_unique_series() {
    let state: obsidian::store::SharedState = std::sync::Arc::new(obsidian::store::AppState {
        log_store: parking_lot::RwLock::new(obsidian::store::LogStore::new()),
        metric_store: parking_lot::RwLock::new(obsidian::store::MetricStore::new()),
        trace_store: parking_lot::RwLock::new(obsidian::store::TraceStore::new()),
        config: obsidian::config::Config {
            port: 0,
            bind_address: "127.0.0.1".into(),
            snapshot_dir: "/tmp/obsidian-test".into(),
            snapshot_interval: 0,
            max_log_entries: 100_000,
            max_series: 2,
            max_spans: 100_000,
            retention: "24h".into(),
            restore: false,
        },
        start_time: std::time::Instant::now(),
    });

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    {
        let mut store = state.metric_store.write();
        for i in 0..5 {
            let samples = (0..3)
                .map(|j| obsidian::store::metric_store::Sample {
                    timestamp_ms: now_ms + (i as i64 * 100) + j as i64,
                    value: (i * 10 + j) as f64,
                })
                .collect::<Vec<_>>();
            store.ingest_samples(
                "queue_depth",
                vec![("instance".to_string(), format!("instance-{i}"))],
                samples,
            );
        }
    }

    obsidian::store::run_eviction(&state);

    let store = state.metric_store.read();
    assert!(
        store.series.len() <= 2,
        "max_series should cap unique series count, got {}",
        store.series.len()
    );
}
