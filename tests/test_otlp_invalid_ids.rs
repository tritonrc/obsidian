//! Integration test: OTLP traces with invalid trace/span IDs are rejected.

mod helpers;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use helpers::make_state;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use tower::ServiceExt;

fn make_trace_request_raw_ids(trace_id: Vec<u8>, span_id: Vec<u8>) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-svc".into())),
                    }),
                }],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id,
                    span_id,
                    trace_state: String::new(),
                    parent_span_id: vec![],
                    name: "test-span".into(),
                    kind: 1,
                    start_time_unix_nano: 1_000_000_000,
                    end_time_unix_nano: 2_000_000_000,
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    events: vec![],
                    dropped_events_count: 0,
                    links: vec![],
                    dropped_links_count: 0,
                    status: Some(Status {
                        message: String::new(),
                        code: 1,
                    }),
                    flags: 0,
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

#[tokio::test]
async fn test_invalid_trace_id_skipped() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    // Send a span with an invalid trace_id (only 8 bytes instead of 16)
    let req =
        make_trace_request_raw_ids(vec![1, 2, 3, 4, 5, 6, 7, 8], vec![1, 2, 3, 4, 5, 6, 7, 8]);
    let r = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(req.encode_to_vec()))
        .unwrap();

    let resp = app.clone().oneshot(r).await.unwrap();
    // Should still return OK (request accepted, invalid spans skipped)
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify no spans were ingested
    let store = state.trace_store.read();
    assert_eq!(store.total_spans, 0);
}

#[tokio::test]
async fn test_invalid_span_id_skipped() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    // Valid trace_id but invalid span_id (only 4 bytes instead of 8)
    let req = make_trace_request_raw_ids(
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        vec![1, 2, 3, 4],
    );
    let r = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(req.encode_to_vec()))
        .unwrap();

    let resp = app.clone().oneshot(r).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let store = state.trace_store.read();
    assert_eq!(store.total_spans, 0);
}

#[tokio::test]
async fn test_all_zero_trace_id_skipped() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let req = make_trace_request_raw_ids(
        vec![0u8; 16], // all-zero trace_id (valid length but invalid per OTLP spec)
        vec![1, 2, 3, 4, 5, 6, 7, 8],
    );
    let r = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(req.encode_to_vec()))
        .unwrap();
    let resp = app.clone().oneshot(r).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let store = state.trace_store.read();
    assert_eq!(store.total_spans, 0);
}

#[tokio::test]
async fn test_valid_and_invalid_spans_mixed() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    // First: ingest a valid span
    let valid_req = make_trace_request_raw_ids(
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        vec![1, 2, 3, 4, 5, 6, 7, 8],
    );
    let r = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(valid_req.encode_to_vec()))
        .unwrap();
    app.clone().oneshot(r).await.unwrap();

    // Second: ingest an invalid span
    let invalid_req = make_trace_request_raw_ids(
        vec![99, 99], // invalid trace_id
        vec![1, 2, 3, 4, 5, 6, 7, 8],
    );
    let r = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(invalid_req.encode_to_vec()))
        .unwrap();
    app.clone().oneshot(r).await.unwrap();

    // Only the valid span should be stored
    let store = state.trace_store.read();
    assert_eq!(store.total_spans, 1);
}
