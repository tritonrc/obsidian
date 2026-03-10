//! Shared test helpers for constructing OTLP requests and test state.

use axum::body::Body;
use axum::http::Request;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use parking_lot::RwLock;
use prost::Message;
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;
use tower::ServiceExt;

pub fn make_state() -> obsidian::store::SharedState {
    Arc::new(obsidian::store::AppState {
        log_store: RwLock::new(obsidian::store::LogStore::new()),
        metric_store: RwLock::new(obsidian::store::MetricStore::new()),
        trace_store: RwLock::new(obsidian::store::TraceStore::new()),
        config: obsidian::config::Config {
            port: 0,
            snapshot_dir: "/tmp/obsidian-test".into(),
            snapshot_interval: 0,
            max_log_entries: 100000,
            max_samples: 10000,
            max_spans: 100000,
            retention: "2h".into(),
            restore: false,
        },
        start_time: Instant::now(),
    })
}

pub fn make_gauge_request(
    service_name: &str,
    metric_name: &str,
    value: f64,
    ts_ns: u64,
) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(service_name.into())),
                    }),
                }],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: metric_name.into(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![],
                            start_time_unix_nano: 0,
                            time_unix_nano: ts_ns,
                            exemplars: vec![],
                            flags: 0,
                            value: Some(
                                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(value),
                            ),
                        }],
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

pub fn make_trace_request(
    service_name: &str,
    span_name: &str,
    trace_id: &[u8; 16],
    span_id: &[u8; 8],
    start_ns: u64,
    end_ns: u64,
    status_code: i32,
) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(service_name.into())),
                    }),
                }],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: trace_id.to_vec(),
                    span_id: span_id.to_vec(),
                    trace_state: String::new(),
                    parent_span_id: vec![],
                    name: span_name.into(),
                    kind: 1,
                    start_time_unix_nano: start_ns,
                    end_time_unix_nano: end_ns,
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    events: vec![],
                    dropped_events_count: 0,
                    links: vec![],
                    dropped_links_count: 0,
                    status: Some(Status {
                        message: String::new(),
                        code: status_code,
                    }),
                    flags: 0,
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

pub async fn push_logs(app: &axum::Router, service: &str, msg: &str, ts: &str) {
    let push_body = json!({
        "streams": [{
            "stream": {"service": service, "level": "info"},
            "values": [[ts, msg]]
        }]
    });

    let req = Request::builder()
        .method("POST")
        .uri("/loki/api/v1/push")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&push_body).unwrap()))
        .unwrap();

    app.clone().oneshot(req).await.unwrap();
}

pub async fn ingest_metrics(
    app: &axum::Router,
    service: &str,
    metric_name: &str,
    value: f64,
    ts_ns: u64,
) {
    let req = make_gauge_request(service, metric_name, value, ts_ns);
    let r = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(req.encode_to_vec()))
        .unwrap();
    app.clone().oneshot(r).await.unwrap();
}

pub async fn ingest_traces(
    app: &axum::Router,
    service: &str,
    span_name: &str,
    trace_id: &[u8; 16],
    span_id: &[u8; 8],
    start_ns: u64,
    end_ns: u64,
    status_code: i32,
) {
    let req = make_trace_request(
        service,
        span_name,
        trace_id,
        span_id,
        start_ns,
        end_ns,
        status_code,
    );
    let r = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(req.encode_to_vec()))
        .unwrap();
    app.clone().oneshot(r).await.unwrap();
}
