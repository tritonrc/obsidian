//! Comprehensive E2E smoke test exercising all ingest paths, query surfaces,
//! API endpoints, and reset functionality against a real TCP server.
//!
//! Uses DISTINCT service names per feature to prevent false positives from
//! cross-feature data leakage. Every ingest is followed by a query that
//! proves the data is retrievable with correct values/labels.

#[allow(dead_code, clippy::too_many_arguments)]
mod helpers;

use helpers::{make_gauge_request, make_trace_request};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Summary, SummaryDataPoint,
    metric, summary_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use parking_lot::RwLock;
use prost::Message;
use serde_json::{Value, json};
use std::io::Write;
use std::sync::Arc;

/// Build a resource with the given service name.
fn make_resource(service_name: &str) -> Option<Resource> {
    Some(Resource {
        attributes: vec![KeyValue {
            key: "service.name".into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(service_name.into())),
            }),
        }],
        dropped_attributes_count: 0,
        entity_refs: vec![],
    })
}

/// Build a gauge metric request with an extra label.
fn make_gauge_with_label(
    service_name: &str,
    metric_name: &str,
    value: f64,
    ts_ns: u64,
    label_key: &str,
    label_val: &str,
) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: make_resource(service_name),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: metric_name.into(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![KeyValue {
                                key: label_key.into(),
                                value: Some(AnyValue {
                                    value: Some(any_value::Value::StringValue(label_val.into())),
                                }),
                            }],
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

/// Build an OTLP logs request.
fn make_logs_request(
    service_name: &str,
    line: &str,
    ts_ns: u64,
    severity: i32,
) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: make_resource(service_name),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: ts_ns,
                    observed_time_unix_nano: ts_ns,
                    severity_number: severity,
                    severity_text: String::new(),
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(line.into())),
                    }),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    flags: 0,
                    trace_id: vec![],
                    span_id: vec![],
                    event_name: String::new(),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Build a Prometheus remote write request (protobuf).
fn make_remote_write_request(
    metric_name: &str,
    service: &str,
    value: f64,
    timestamp_ms: i64,
) -> obsidian::ingest::remote_write::WriteRequest {
    use obsidian::ingest::remote_write::{RemoteLabel, RemoteSample, TimeSeries, WriteRequest};
    WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![
                RemoteLabel {
                    name: "__name__".into(),
                    value: metric_name.into(),
                },
                RemoteLabel {
                    name: "service".into(),
                    value: service.into(),
                },
            ],
            samples: vec![RemoteSample {
                value,
                timestamp: timestamp_ms,
            }],
        }],
    }
}

/// Build a Summary metric request.
fn make_summary_request(
    service_name: &str,
    metric_name: &str,
    ts_ns: u64,
    count: u64,
    sum: f64,
    quantiles: &[(f64, f64)],
) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: make_resource(service_name),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: metric_name.into(),
                    data: Some(metric::Data::Summary(Summary {
                        data_points: vec![SummaryDataPoint {
                            time_unix_nano: ts_ns,
                            count,
                            sum,
                            quantile_values: quantiles
                                .iter()
                                .map(|&(q, v)| summary_data_point::ValueAtQuantile {
                                    quantile: q,
                                    value: v,
                                })
                                .collect(),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[tokio::test]
async fn test_e2e_smoke_all_features() {
    // ======= SERVER SETUP =======
    let tmp_dir = tempfile::tempdir().unwrap();
    let state: obsidian::store::SharedState = Arc::new(obsidian::store::AppState {
        log_store: RwLock::new(obsidian::store::LogStore::new()),
        metric_store: RwLock::new(obsidian::store::MetricStore::new()),
        trace_store: RwLock::new(obsidian::store::TraceStore::new()),
        config: obsidian::config::Config {
            port: 0,
            bind_address: "127.0.0.1".into(),
            snapshot_dir: tmp_dir.path().to_string_lossy().into_owned(),
            snapshot_interval: 0,
            max_log_entries: 100_000,
            max_series: 10_000,
            max_spans: 100_000,
            retention: "2h".into(),
            restore: false,
        },
        start_time: std::time::Instant::now(),
    });

    let app = obsidian::server::build_router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });

    let client = reqwest::Client::new();

    // --- Finding 6: Wait for server to be ready ---
    {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if let Ok(resp) = client.get(format!("{}/ready", base)).send().await {
                if resp.status() == 200 {
                    break;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "server did not become ready within 5 seconds"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let now_ms = (now_ns / 1_000_000) as i64;
    let now_s = now_ns / 1_000_000_000;

    // ======= EDGE CASE: Server starts empty =======
    let json: Value = client
        .get(format!("{}/api/v1/services", base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let initial_services = json["data"]["services"].as_array().unwrap();
    assert!(
        initial_services.is_empty(),
        "server should start with no services, got: {:?}",
        initial_services
    );

    // ======= INGEST PHASE =======

    // --- 1. Loki JSON push (svc-loki) ---
    let svc_loki = "svc-loki";
    let resp = client
        .post(format!("{}/loki/api/v1/push", base))
        .json(&json!({
            "streams": [
                {
                    "stream": {"service": svc_loki, "level": "info"},
                    "values": [[now_ns.to_string(), "loki info line 1"]]
                },
                {
                    "stream": {"service": svc_loki, "level": "error"},
                    "values": [
                        [now_ns.to_string(), "loki error line 1"],
                        [(now_ns + 1).to_string(), "loki error line 2"]
                    ]
                }
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["accepted"]["streams"], 2);
    assert_eq!(body["accepted"]["entries"], 3);

    // --- 2. OTLP metrics protobuf (svc-otlp-metrics) ---
    let svc_otlp_metrics = "svc-otlp-metrics";
    let resp = client
        .post(format!("{}/v1/metrics", base))
        .header("content-type", "application/x-protobuf")
        .body(
            make_gauge_request(svc_otlp_metrics, "otlp_gauge_proto", 42.0, now_ns).encode_to_vec(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["accepted"]["series"], 1);
    assert_eq!(body["accepted"]["samples"], 1);

    // --- 3. OTLP metrics JSON encoding (svc-otlp-metrics-json) ---
    let svc_otlp_metrics_json = "svc-otlp-metrics-json";
    let json_metrics = make_gauge_request(svc_otlp_metrics_json, "otlp_gauge_json", 99.0, now_ns);
    let resp = client
        .post(format!("{}/v1/metrics", base))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&json_metrics).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["accepted"]["series"], 1);

    // --- 4. OTLP traces protobuf (svc-otlp-traces) ---
    let svc_otlp_traces = "svc-otlp-traces";
    let trace_id_1: [u8; 16] = [0xAA; 16];
    let resp = client
        .post(format!("{}/v1/traces", base))
        .header("content-type", "application/x-protobuf")
        .body(
            make_trace_request(
                svc_otlp_traces,
                "trace-proto-span",
                &trace_id_1,
                &[1; 8],
                now_ns - 1_000_000_000,
                now_ns,
                1,
            )
            .encode_to_vec(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["accepted"]["traces"], 1);
    assert_eq!(body["accepted"]["spans"], 1);

    // --- 5. OTLP traces JSON encoding (svc-otlp-traces-json) ---
    let svc_otlp_traces_json = "svc-otlp-traces-json";
    let trace_id_2: [u8; 16] = [0xBB; 16];
    let json_traces = make_trace_request(
        svc_otlp_traces_json,
        "trace-json-span",
        &trace_id_2,
        &[2; 8],
        now_ns - 500_000_000,
        now_ns,
        2,
    );
    let resp = client
        .post(format!("{}/v1/traces", base))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&json_traces).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["accepted"]["spans"], 1);

    // --- 6. OTLP logs protobuf (svc-otlp-logs) ---
    let svc_otlp_logs = "svc-otlp-logs";
    let resp = client
        .post(format!("{}/v1/logs", base))
        .header("content-type", "application/x-protobuf")
        .body(make_logs_request(svc_otlp_logs, "otlp proto log line", now_ns, 9).encode_to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // --- 6b. OTLP logs JSON encoding (svc-otlp-logs-json) ---
    let svc_otlp_logs_json = "svc-otlp-logs-json";
    let json_logs = make_logs_request(svc_otlp_logs_json, "otlp json log line", now_ns, 17);
    let resp = client
        .post(format!("{}/v1/logs", base))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&json_logs).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // --- 7. Prometheus remote write (svc-remote-write) ---
    let svc_rw = "svc-remote-write";
    let rw = make_remote_write_request("rw_gauge", svc_rw, 77.0, now_ms);
    let proto_bytes = rw.encode_to_vec();
    let snappy_bytes = snap::raw::Encoder::new()
        .compress_vec(&proto_bytes)
        .unwrap();
    let resp = client
        .post(format!("{}/api/v1/write", base))
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "snappy")
        .body(snappy_bytes)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // --- 8. Gzip-compressed OTLP metrics (svc-gzip) ---
    let svc_gzip = "svc-gzip";
    let gzip_metrics = make_gauge_request(svc_gzip, "gzip_gauge", 55.0, now_ns);
    let proto_bytes = gzip_metrics.encode_to_vec();
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(&proto_bytes).unwrap();
    let gz_bytes = encoder.finish().unwrap();
    let resp = client
        .post(format!("{}/v1/metrics", base))
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "gzip")
        .body(gz_bytes)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // --- 9. Summary metric (svc-summary) ---
    let svc_summary = "svc-summary";
    let resp = client
        .post(format!("{}/v1/metrics", base))
        .header("content-type", "application/x-protobuf")
        .body(
            make_summary_request(
                svc_summary,
                "request_latency",
                now_ns,
                100,
                450.0,
                &[(0.5, 0.25), (0.9, 0.8), (0.99, 1.2)],
            )
            .encode_to_vec(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    // Summary with 3 quantiles produces 3 series + _sum + _count = 5
    assert_eq!(body["accepted"]["series"], 5);

    // --- 10. Multi-service traces for grouping (svc-group-a, svc-group-b) ---
    let svc_group_a = "svc-group-a";
    let svc_group_b = "svc-group-b";
    let multi_trace_id: [u8; 16] = [0xCC; 16];
    let parent_span_id: [u8; 8] = [0x10; 8];
    let child_span_id: [u8; 8] = [0x20; 8];
    let root_span_req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: make_resource(svc_group_a),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: multi_trace_id.to_vec(),
                    span_id: parent_span_id.to_vec(),
                    trace_state: String::new(),
                    parent_span_id: vec![],
                    name: "root-op".into(),
                    kind: 1,
                    start_time_unix_nano: now_ns - 2_000_000_000,
                    end_time_unix_nano: now_ns,
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
    };
    let resp = client
        .post(format!("{}/v1/traces", base))
        .header("content-type", "application/x-protobuf")
        .body(root_span_req.encode_to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let child_span_req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: make_resource(svc_group_b),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: multi_trace_id.to_vec(),
                    span_id: child_span_id.to_vec(),
                    trace_state: String::new(),
                    parent_span_id: parent_span_id.to_vec(),
                    name: "child-op".into(),
                    kind: 1,
                    start_time_unix_nano: now_ns - 1_500_000_000,
                    end_time_unix_nano: now_ns - 500_000_000,
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
    };
    let resp = client
        .post(format!("{}/v1/traces", base))
        .header("content-type", "application/x-protobuf")
        .body(child_span_req.encode_to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // --- 12. JSON-formatted log lines for | json test (svc-json-pipe) ---
    let svc_json_pipe = "svc-json-pipe";
    let json_log_error = r#"{"level":"error","msg":"disk full","code":500}"#;
    let json_log_info = r#"{"level":"info","msg":"all good","code":200}"#;
    let resp = client
        .post(format!("{}/loki/api/v1/push", base))
        .json(&json!({
            "streams": [{
                "stream": {"service": svc_json_pipe, "level": "info"},
                "values": [
                    [now_ns.to_string(), json_log_error],
                    [(now_ns + 1).to_string(), json_log_info]
                ]
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // --- 13. Metrics with distinct instance labels for topk/bottomk (svc-topk) ---
    let svc_topk = "svc-topk";
    for (i, val) in [10.0, 20.0, 30.0, 40.0, 50.0].iter().enumerate() {
        let resp = client
            .post(format!("{}/v1/metrics", base))
            .header("content-type", "application/x-protobuf")
            .body(
                make_gauge_with_label(
                    svc_topk,
                    "topk_metric",
                    *val,
                    now_ns,
                    "instance",
                    &format!("host-{}", i),
                )
                .encode_to_vec(),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    // --- 14. Offset test: ingest at distinct times (svc-offset) ---
    let svc_offset = "svc-offset";
    // Ingest a sample 10 minutes ago
    let ten_min_ago_ns = now_ns - 600_000_000_000;
    let _ten_min_ago_ms = (ten_min_ago_ns / 1_000_000) as i64;
    let resp = client
        .post(format!("{}/v1/metrics", base))
        .header("content-type", "application/x-protobuf")
        .body(make_gauge_request(svc_offset, "offset_gauge", 111.0, ten_min_ago_ns).encode_to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    // Ingest a sample at current time
    let resp = client
        .post(format!("{}/v1/metrics", base))
        .header("content-type", "application/x-protobuf")
        .body(make_gauge_request(svc_offset, "offset_gauge", 222.0, now_ns).encode_to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // --- 15. Error traces for summary (svc-summary-traces) ---
    let svc_summary_traces = "svc-summary-traces";
    let error_trace_id: [u8; 16] = [0xDD; 16];
    let resp = client
        .post(format!("{}/v1/traces", base))
        .header("content-type", "application/x-protobuf")
        .body(
            make_trace_request(
                svc_summary_traces,
                "error-span",
                &error_trace_id,
                &[3; 8],
                now_ns - 100_000_000,
                now_ns,
                2,
            )
            .encode_to_vec(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Error + info logs for summary (svc-summary-traces)
    let resp = client
        .post(format!("{}/loki/api/v1/push", base))
        .json(&json!({
            "streams": [
                {
                    "stream": {"service": svc_summary_traces, "level": "error"},
                    "values": [[now_ns.to_string(), "summary error line"]]
                },
                {
                    "stream": {"service": svc_summary_traces, "level": "info"},
                    "values": [[now_ns.to_string(), "summary info line"]]
                }
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // --- 16. label_replace / label_join metrics (svc-label-fns) ---
    let svc_label_fns = "svc-label-fns";
    let resp = client
        .post(format!("{}/v1/metrics", base))
        .header("content-type", "application/x-protobuf")
        .body(make_gauge_request(svc_label_fns, "labelfn_gauge", 88.0, now_ns).encode_to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // ======= QUERY PHASE =======

    // --- LogQL: query Loki-ingested logs (svc-loki) ---
    let json: Value = client
        .get(format!(
            "{}/loki/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(r#"{{service="{}"}}"#, svc_loki))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let streams = json["data"]["result"].as_array().unwrap();
    assert_eq!(streams.len(), 2, "expected 2 streams (info + error)");
    // Count total entries across streams
    let total_entries: usize = streams
        .iter()
        .map(|s| s["values"].as_array().unwrap().len())
        .sum();
    assert_eq!(total_entries, 3, "expected 3 total log entries");
    // Verify the error stream has correct lines
    let error_stream = streams
        .iter()
        .find(|s| s["stream"]["level"] == "error")
        .expect("expected an error-level stream");
    let error_values = error_stream["values"].as_array().unwrap();
    assert_eq!(error_values.len(), 2, "expected 2 error log entries");
    assert_eq!(error_values[0][1], "loki error line 1");

    // --- LogQL query_range (GET) ---
    let json_range_get: Value = client
        .get(format!(
            "{}/loki/api/v1/query_range?query={}&start={}&end={}",
            base,
            urlencoding::encode(&format!(r#"{{service="{}"}}"#, svc_loki)),
            now_s - 3600,
            now_s + 60,
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json_range_get["status"], "success");
    let get_result = json_range_get["data"]["result"].as_array().unwrap();
    assert_eq!(get_result.len(), 2, "expected 2 streams in range query");

    // --- LogQL query_range (POST form-encoded) — verify same results ---
    let json_range_post: Value = client
        .post(format!("{}/loki/api/v1/query_range", base))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(format!(
            "query={}&start={}&end={}",
            urlencoding::encode(&format!(r#"{{service="{}"}}"#, svc_loki)),
            now_s - 3600,
            now_s + 60,
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json_range_post["status"], "success");
    assert_eq!(
        json_range_get["data"]["result"].as_array().unwrap().len(),
        json_range_post["data"]["result"].as_array().unwrap().len()
    );

    // --- LogQL: query back OTLP-ingested logs (svc-otlp-logs) ---
    let json: Value = client
        .get(format!(
            "{}/loki/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(r#"{{service="{}"}}"#, svc_otlp_logs))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let streams = json["data"]["result"].as_array().unwrap();
    assert_eq!(streams.len(), 1, "expected 1 stream from OTLP logs proto");
    let entries = streams[0]["values"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0][1], "otlp proto log line");

    // --- LogQL: query back OTLP JSON-ingested logs (svc-otlp-logs-json) ---
    let json: Value = client
        .get(format!(
            "{}/loki/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(r#"{{service="{}"}}"#, svc_otlp_logs_json))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let streams = json["data"]["result"].as_array().unwrap();
    assert_eq!(streams.len(), 1, "expected 1 stream from OTLP logs JSON");
    let entries = streams[0]["values"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0][1], "otlp json log line");
    // Verify it got level=error (severity 17 maps to error)
    assert_eq!(streams[0]["stream"]["level"], "error");

    // --- PromQL: query OTLP protobuf metric (svc-otlp-metrics) ---
    let json: Value = client
        .get(format!(
            "{}/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(
                r#"otlp_gauge_proto{{service="{}"}}"#,
                svc_otlp_metrics
            ))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["resultType"], "vector");
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(results.len(), 1);
    let val: f64 = results[0]["value"][1].as_str().unwrap().parse().unwrap();
    assert!((val - 42.0).abs() < 0.01);
    assert_eq!(results[0]["metric"]["service"], svc_otlp_metrics);

    // --- PromQL: query JSON-encoded metric (svc-otlp-metrics-json) ---
    let json: Value = client
        .get(format!(
            "{}/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(
                r#"otlp_gauge_json{{service="{}"}}"#,
                svc_otlp_metrics_json
            ))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(results.len(), 1);
    let val: f64 = results[0]["value"][1].as_str().unwrap().parse().unwrap();
    assert!((val - 99.0).abs() < 0.01);

    // --- PromQL POST form-encoded ---
    let json: Value = client
        .post(format!("{}/api/v1/query", base))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(format!(
            "query={}",
            urlencoding::encode(&format!(
                r#"otlp_gauge_proto{{service="{}"}}"#,
                svc_otlp_metrics
            ))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let val: f64 = json["data"]["result"][0]["value"][1]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!((val - 42.0).abs() < 0.01);

    // --- PromQL: query remote write metric (svc-remote-write) ---
    let json: Value = client
        .get(format!(
            "{}/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(r#"rw_gauge{{service="{}"}}"#, svc_rw))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(results.len(), 1, "remote write metric should be queryable");
    let val: f64 = results[0]["value"][1].as_str().unwrap().parse().unwrap();
    assert!(
        (val - 77.0).abs() < 0.01,
        "remote write value should be 77.0"
    );
    assert_eq!(results[0]["metric"]["service"], svc_rw);

    // --- PromQL: query gzip metric (svc-gzip) ---
    let json: Value = client
        .get(format!(
            "{}/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(r#"gzip_gauge{{service="{}"}}"#, svc_gzip))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(results.len(), 1, "gzip metric should be queryable");
    let val: f64 = results[0]["value"][1].as_str().unwrap().parse().unwrap();
    assert!((val - 55.0).abs() < 0.01, "gzip value should be 55.0");

    // --- PromQL: query Summary metric (svc-summary) ---
    // Query _sum
    let json: Value = client
        .get(format!(
            "{}/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(
                r#"request_latency_sum{{service="{}"}}"#,
                svc_summary
            ))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(results.len(), 1, "summary _sum should have 1 series");
    let val: f64 = results[0]["value"][1].as_str().unwrap().parse().unwrap();
    assert!((val - 450.0).abs() < 0.01, "summary _sum should be 450.0");

    // Query _count
    let json: Value = client
        .get(format!(
            "{}/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(
                r#"request_latency_count{{service="{}"}}"#,
                svc_summary
            ))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(results.len(), 1, "summary _count should have 1 series");
    let val: f64 = results[0]["value"][1].as_str().unwrap().parse().unwrap();
    assert!((val - 100.0).abs() < 0.01, "summary _count should be 100.0");

    // Query quantile series (p50)
    let json: Value = client
        .get(format!(
            "{}/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(
                r#"request_latency{{service="{}", quantile="0.5"}}"#,
                svc_summary
            ))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(
        results.len(),
        1,
        "summary quantile=0.5 should have 1 series"
    );
    let val: f64 = results[0]["value"][1].as_str().unwrap().parse().unwrap();
    assert!((val - 0.25).abs() < 0.01, "p50 should be 0.25");

    // --- PromQL topk(3, metric) with exact assertions ---
    let json: Value = client
        .get(format!(
            "{}/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(
                r#"topk(3, topk_metric{{service="{}"}})"#,
                svc_topk
            ))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(results.len(), 3, "topk(3) must return exactly 3 series");
    // Sort by value descending (PromQL spec doesn't guarantee topk order)
    let mut top_pairs: Vec<(f64, String)> = results
        .iter()
        .map(|r| {
            let v: f64 = r["value"][1].as_str().unwrap().parse().unwrap();
            let inst = r["metric"]["instance"].as_str().unwrap().to_string();
            (v, inst)
        })
        .collect();
    top_pairs.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
    assert!(
        (top_pairs[0].0 - 50.0).abs() < 0.01,
        "1st topk should be 50.0"
    );
    assert!(
        (top_pairs[1].0 - 40.0).abs() < 0.01,
        "2nd topk should be 40.0"
    );
    assert!(
        (top_pairs[2].0 - 30.0).abs() < 0.01,
        "3rd topk should be 30.0"
    );
    assert_eq!(top_pairs[0].1, "host-4");
    assert_eq!(top_pairs[1].1, "host-3");
    assert_eq!(top_pairs[2].1, "host-2");

    // --- PromQL bottomk(2, metric) with exact assertions ---
    let json: Value = client
        .get(format!(
            "{}/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(
                r#"bottomk(2, topk_metric{{service="{}"}})"#,
                svc_topk
            ))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(results.len(), 2, "bottomk(2) must return exactly 2 series");
    // Sort by value ascending (PromQL spec doesn't guarantee bottomk order)
    let mut bottom_pairs: Vec<(f64, String)> = results
        .iter()
        .map(|r| {
            let v: f64 = r["value"][1].as_str().unwrap().parse().unwrap();
            let inst = r["metric"]["instance"].as_str().unwrap().to_string();
            (v, inst)
        })
        .collect();
    bottom_pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    assert!(
        (bottom_pairs[0].0 - 10.0).abs() < 0.01,
        "1st bottomk should be 10.0"
    );
    assert!(
        (bottom_pairs[1].0 - 20.0).abs() < 0.01,
        "2nd bottomk should be 20.0"
    );
    assert_eq!(bottom_pairs[0].1, "host-0");
    assert_eq!(bottom_pairs[1].1, "host-1");

    // --- topk with k > cardinality returns all series ---
    let json: Value = client
        .get(format!(
            "{}/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(
                r#"topk(100, topk_metric{{service="{}"}})"#,
                svc_topk
            ))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(
        results.len(),
        5,
        "topk(100) with 5 series should return all 5"
    );

    // --- PromQL offset: use time param to query "now", then offset 5m to hit old sample ---
    let json: Value = client
        .get(format!(
            "{}/api/v1/query?query={}&time={}",
            base,
            urlencoding::encode(&format!(
                r#"offset_gauge{{service="{}"}} offset 5m"#,
                svc_offset
            )),
            now_s,
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let results = json["data"]["result"].as_array().unwrap();
    // offset 5m from now_s looks at now_s - 300s. We have a sample at now and at now-600s.
    // The evaluation picks the closest sample <= eval_time - offset = now - 5m.
    // With 5-minute lookback window from (now-5m-5m) to (now-5m), the 10-min-ago sample
    // (111.0) falls exactly at the edge and should be returned.
    assert!(
        !results.is_empty(),
        "offset query should return at least one result"
    );
    let val: f64 = results[0]["value"][1].as_str().unwrap().parse().unwrap();
    assert!(
        (val - 111.0).abs() < 0.01,
        "offset 5m should return the 10-min-ago sample (111.0), got {}",
        val
    );

    // --- PromQL label_replace: assert non-empty and exact transformed label ---
    let json: Value = client
        .get(format!(
            "{}/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(
                r#"label_replace(labelfn_gauge{{service="{}"}}, "dst", "$1", "service", "(.*)")"#,
                svc_label_fns
            ))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(
        results.len(),
        1,
        "label_replace must return non-empty results"
    );
    assert_eq!(
        results[0]["metric"]["dst"], svc_label_fns,
        "label_replace dst should equal service name"
    );

    // --- PromQL label_join: assert non-empty and exact joined label ---
    let json: Value = client
        .get(format!(
            "{}/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(
                r#"label_join(labelfn_gauge{{service="{}"}}, "joined", "-", "service", "__name__")"#,
                svc_label_fns
            ))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(results.len(), 1, "label_join must return non-empty results");
    let joined = results[0]["metric"]["joined"].as_str().unwrap();
    assert_eq!(
        joined,
        &format!("{}-labelfn_gauge", svc_label_fns),
        "joined label should be service-metricname"
    );

    // --- TraceQL search (svc-otlp-traces) ---
    let json: Value = client
        .get(format!(
            "{}/api/search?q={}",
            base,
            urlencoding::encode(&format!(
                r#"{{ resource.service.name = "{}" }}"#,
                svc_otlp_traces
            ))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let traces = json["traces"].as_array().unwrap();
    assert_eq!(
        traces.len(),
        1,
        "expected exactly 1 trace for svc-otlp-traces"
    );
    let trace_id_hex: String = trace_id_1.iter().map(|b| format!("{:02x}", b)).collect();
    assert_eq!(
        traces[0]["traceID"], trace_id_hex,
        "expected exact trace ID"
    );

    // --- TraceQL search with start/end time range ---
    let json: Value = client
        .get(format!(
            "{}/api/search?q={}&start={}&end={}",
            base,
            urlencoding::encode(&format!(
                r#"{{ resource.service.name = "{}" }}"#,
                svc_otlp_traces
            )),
            now_s - 60,
            now_s + 60,
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let traces = json["traces"].as_array().unwrap();
    assert_eq!(traces.len(), 1, "time-ranged search should find the trace");

    // --- TraceQL search with | count() > N ---
    let json: Value = client
        .get(format!(
            "{}/api/search?q={}",
            base,
            urlencoding::encode(&format!(
                r#"{{ resource.service.name = "{}" }} | count() > 0"#,
                svc_otlp_traces
            ))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let traces = json["traces"].as_array().unwrap();
    assert!(!traces.is_empty(), "count() > 0 should match traces");

    // --- TraceQL error hint: invalid TraceQL returns hint and error ---
    let resp = client
        .get(format!(
            "{}/api/search?q={}",
            base,
            urlencoding::encode("this is not valid traceql")
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let json: Value = resp.json().await.unwrap();
    assert!(
        json["error"].is_string() && !json["error"].as_str().unwrap().is_empty(),
        "TraceQL error response must have non-empty error field"
    );
    assert!(
        json["hint"].is_string() && !json["hint"].as_str().unwrap().is_empty(),
        "TraceQL error response must have non-empty hint field"
    );

    // --- GET /api/traces/{traceID} — verify multi-service grouping ---
    let multi_trace_hex = "cccccccccccccccccccccccccccccccc";
    let json: Value = client
        .get(format!("{}/api/traces/{}", base, multi_trace_hex))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let batches = json["batches"].as_array().unwrap();
    assert_eq!(batches.len(), 2, "expected exactly 2 service batches");
    // Find service names by key, not by positional index
    let batch_services: Vec<&str> = batches
        .iter()
        .map(|b| {
            let attrs = b["resource"]["attributes"].as_array().unwrap();
            let svc_attr = attrs
                .iter()
                .find(|a| a["key"] == "service.name")
                .expect("batch must have service.name attribute");
            svc_attr["value"]["stringValue"].as_str().unwrap()
        })
        .collect();
    assert!(
        batch_services.contains(&svc_group_a),
        "expected svc-group-a in batches"
    );
    assert!(
        batch_services.contains(&svc_group_b),
        "expected svc-group-b in batches"
    );
    // Verify span membership
    for batch in batches {
        let svc = batch["resource"]["attributes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["key"] == "service.name")
            .unwrap()["value"]["stringValue"]
            .as_str()
            .unwrap();
        let spans = batch["scopeSpans"][0]["spans"].as_array().unwrap();
        assert_eq!(spans.len(), 1, "each batch should have exactly 1 span");
        if svc == svc_group_a {
            assert_eq!(spans[0]["name"], "root-op");
        } else {
            assert_eq!(spans[0]["name"], "child-op");
        }
    }

    // --- LogQL | json | level="error" — assert ONLY error line returned ---
    let json: Value = client
        .get(format!(
            "{}/loki/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(
                r#"{{service="{}"}} | json | level="error""#,
                svc_json_pipe
            ))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let streams = json["data"]["result"].as_array().unwrap();
    let total_entries: usize = streams
        .iter()
        .map(|s| s["values"].as_array().unwrap().len())
        .sum();
    assert_eq!(
        total_entries, 1,
        "| json | level=\"error\" should return exactly 1 entry (the error line)"
    );
    // Verify the returned line is the error one
    let returned_line = streams[0]["values"].as_array().unwrap()[0][1]
        .as_str()
        .unwrap();
    assert!(
        returned_line.contains("disk full"),
        "expected the error JSON line, got: {}",
        returned_line
    );

    // ======= API PHASE =======

    // --- GET /api/v1/diagnose?service=X — verify error signals, not info ---
    let json: Value = client
        .get(format!(
            "{}/api/v1/diagnose?service={}",
            base, svc_summary_traces
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["service"], svc_summary_traces);
    // Should have recent error logs
    let recent_errors = json["recent_errors"].as_array().unwrap();
    assert!(
        !recent_errors.is_empty(),
        "diagnose should contain recent error logs"
    );
    assert!(
        recent_errors
            .iter()
            .any(|l| l["line"].as_str().unwrap().contains("summary error line")),
        "diagnose recent_errors should contain our error line"
    );
    // Info logs should NOT appear in recent_errors
    assert!(
        !recent_errors
            .iter()
            .any(|l| l["line"].as_str().unwrap().contains("summary info line")),
        "diagnose recent_errors should not contain info lines"
    );
    // Should have slowest traces
    let slowest_traces = json["slowest_traces"].as_array().unwrap();
    assert!(
        !slowest_traces.is_empty(),
        "diagnose should contain slowest traces"
    );
    // Health score should be degraded
    let health_score = json["health_score"].as_f64().unwrap();
    assert!(
        health_score < 100.0,
        "diagnose health_score should be degraded with errors"
    );

    // --- GET /api/v1/catalog — verify specific metric names and label keys ---
    let json: Value = client
        .get(format!(
            "{}/api/v1/catalog?service={}",
            base, svc_otlp_metrics
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let metrics = json["data"]["metrics"].as_array().unwrap();
    let metric_names: Vec<&str> = metrics.iter().filter_map(|m| m.as_str()).collect();
    assert!(
        metric_names.contains(&"otlp_gauge_proto"),
        "catalog should list otlp_gauge_proto, got: {:?}",
        metric_names
    );

    // Catalog for svc-loki should have log labels
    let json: Value = client
        .get(format!("{}/api/v1/catalog?service={}", base, svc_loki))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let log_labels = json["data"]["log_labels"].as_array().unwrap();
    let label_keys: Vec<&str> = log_labels.iter().filter_map(|l| l.as_str()).collect();
    assert!(
        label_keys.contains(&"level"),
        "catalog log_labels should include 'level', got: {:?}",
        label_keys
    );
    assert!(
        label_keys.contains(&"service"),
        "catalog log_labels should include 'service', got: {:?}",
        label_keys
    );

    // --- GET /api/v1/status ---
    let json: Value = client
        .get(format!("{}/api/v1/status", base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    assert!(json["data"]["memoryBytes"].as_u64().unwrap() > 0);
    assert!(json["data"]["logMemoryBytes"].as_u64().unwrap() > 0);
    assert!(json["data"]["metricMemoryBytes"].as_u64().unwrap() > 0);
    assert!(json["data"]["traceMemoryBytes"].as_u64().unwrap() > 0);
    assert!(json["data"]["totalLogEntries"].as_u64().unwrap() > 0);
    assert!(json["data"]["totalMetricSeries"].as_u64().unwrap() > 0);
    assert!(json["data"]["totalSpans"].as_u64().unwrap() > 0);

    // --- GET /api/v1/services ---
    let json: Value = client
        .get(format!("{}/api/v1/services", base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let service_names: Vec<&str> = json["data"]["services"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["name"].as_str())
        .collect();
    // Verify several distinct services are present
    for expected in &[
        svc_loki,
        svc_otlp_metrics,
        svc_otlp_traces,
        svc_rw,
        svc_gzip,
        svc_topk,
        svc_group_a,
        svc_group_b,
    ] {
        assert!(
            service_names.contains(expected),
            "expected service {} in services list, got: {:?}",
            expected,
            service_names
        );
    }

    // --- GET /api/v1/openapi.json ---
    let json: Value = client
        .get(format!("{}/api/v1/openapi.json", base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(json["openapi"].as_str().unwrap().starts_with("3."));
    let paths = json["paths"].as_object().unwrap();
    assert!(paths.len() > 10);
    // Assert specific paths exist
    assert!(
        paths.contains_key("/api/v1/query"),
        "openapi should have /api/v1/query"
    );
    assert!(
        paths.contains_key("/api/search"),
        "openapi should have /api/search"
    );
    assert!(
        paths.contains_key("/loki/api/v1/push"),
        "openapi should have /loki/api/v1/push"
    );
    assert!(
        paths.contains_key("/v1/metrics"),
        "openapi should have /v1/metrics"
    );
    assert!(
        paths.contains_key("/api/v1/reset"),
        "openapi should have /api/v1/reset"
    );

    // --- GET /api/v1/metadata — assert exact empty object ---
    let json: Value = client
        .get(format!("{}/api/v1/metadata", base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        json,
        json!({}),
        "metadata should return exact empty JSON object"
    );

    // --- GET /ready ---
    let json: Value = client
        .get(format!("{}/ready", base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "ready");

    // --- Grafana compat: GET /api/v1/query?query=1+1 ---
    let json: Value = client
        .get(format!(
            "{}/api/v1/query?query={}",
            base,
            urlencoding::encode("1+1")
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let scalar_val: f64 = json["data"]["result"][1].as_str().unwrap().parse().unwrap();
    assert!((scalar_val - 2.0).abs() < 0.01);

    // --- Grafana compat: GET /api/search (no q param) ---
    let json: Value = client
        .get(format!("{}/api/search", base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(json["traces"].as_array().unwrap().is_empty());

    // --- LogQL parse error with hint ---
    let resp = client
        .get(format!(
            "{}/loki/api/v1/query?query={}",
            base,
            urlencoding::encode("this is not valid logql")
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let json: Value = resp.json().await.unwrap();
    assert!(
        json["hint"].is_string() && !json["hint"].as_str().unwrap().is_empty(),
        "LogQL error should have non-empty hint"
    );
    assert!(
        json["error"].is_string() && !json["error"].as_str().unwrap().is_empty(),
        "LogQL error should have non-empty error field"
    );

    // --- PromQL parse error with hint ---
    let resp = client
        .get(format!(
            "{}/api/v1/query?query={}",
            base,
            urlencoding::encode("not valid promql {{{{")
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let json: Value = resp.json().await.unwrap();
    assert!(
        json["hint"].is_string() && !json["hint"].as_str().unwrap().is_empty(),
        "PromQL error should have non-empty hint"
    );
    assert!(
        json["error"].is_string() && !json["error"].as_str().unwrap().is_empty(),
        "PromQL error should have non-empty error field"
    );

    // ======= EDGE CASES =======

    // --- Malformed protobuf to /v1/logs returns 400 ---
    let resp = client
        .post(format!("{}/v1/logs", base))
        .header("content-type", "application/x-protobuf")
        .body(b"this is not valid protobuf".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "malformed protobuf to /v1/logs should return 400"
    );

    // --- Invalid snappy to /api/v1/write returns 400 ---
    let resp = client
        .post(format!("{}/api/v1/write", base))
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "snappy")
        .body(b"not valid snappy data".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "invalid snappy to /api/v1/write should return 400"
    );

    // --- Reset nonexistent service still returns 200 ---
    let resp = client
        .delete(format!(
            "{}/api/v1/reset?service=nonexistent-service-xyz",
            base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "reset of nonexistent service should return 200"
    );

    // ======= RESET PHASE =======

    // --- Finding 4: Per-service reset tests all 3 signal types ---
    // Ingest logs, metrics, and traces for a dedicated reset target service.
    let svc_reset = "svc-reset-target";
    let reset_trace_id: [u8; 16] = [0xEE; 16];

    // Ingest logs for reset target
    let resp = client
        .post(format!("{}/loki/api/v1/push", base))
        .json(&json!({
            "streams": [{
                "stream": {"service": svc_reset, "level": "info"},
                "values": [[now_ns.to_string(), "reset target log line"]]
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Ingest metrics for reset target
    let resp = client
        .post(format!("{}/v1/metrics", base))
        .header("content-type", "application/x-protobuf")
        .body(make_gauge_request(svc_reset, "reset_target_gauge", 999.0, now_ns).encode_to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Ingest traces for reset target
    let resp = client
        .post(format!("{}/v1/traces", base))
        .header("content-type", "application/x-protobuf")
        .body(
            make_trace_request(
                svc_reset,
                "reset-span",
                &reset_trace_id,
                &[0xEE; 8],
                now_ns - 100_000_000,
                now_ns,
                1,
            )
            .encode_to_vec(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify all 3 signal types exist before reset
    let json: Value = client
        .get(format!(
            "{}/loki/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(r#"{{service="{}"}}"#, svc_reset))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !json["data"]["result"].as_array().unwrap().is_empty(),
        "reset target should have logs before reset"
    );

    let json: Value = client
        .get(format!(
            "{}/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(r#"reset_target_gauge{{service="{}"}}"#, svc_reset))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !json["data"]["result"].as_array().unwrap().is_empty(),
        "reset target should have metrics before reset"
    );

    let json: Value = client
        .get(format!(
            "{}/api/search?q={}",
            base,
            urlencoding::encode(&format!(r#"{{ resource.service.name = "{}" }}"#, svc_reset))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !json["traces"].as_array().unwrap().is_empty(),
        "reset target should have traces before reset"
    );

    // DELETE per-service reset
    let resp = client
        .delete(format!("{}/api/v1/reset?service={}", base, svc_reset))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify reset target is gone from services list
    let json: Value = client
        .get(format!("{}/api/v1/services", base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let service_names: Vec<&str> = json["data"]["services"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["name"].as_str())
        .collect();
    assert!(
        !service_names.contains(&svc_reset),
        "svc-reset-target should be gone after per-service reset"
    );
    assert!(
        service_names.contains(&svc_otlp_traces),
        "svc-otlp-traces should still be present after per-service reset"
    );
    assert!(
        service_names.contains(&svc_loki),
        "svc-loki should still be present after per-service reset"
    );

    // Verify ALL 3 signal types are gone for reset target
    // Logs
    let json: Value = client
        .get(format!(
            "{}/loki/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(r#"{{service="{}"}}"#, svc_reset))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        json["data"]["result"].as_array().unwrap().is_empty(),
        "after per-service reset, LogQL should find no logs for svc-reset-target"
    );

    // Metrics
    let json: Value = client
        .get(format!(
            "{}/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(r#"reset_target_gauge{{service="{}"}}"#, svc_reset))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        json["data"]["result"].as_array().unwrap().is_empty(),
        "after per-service reset, PromQL should find no metrics for svc-reset-target"
    );

    // Traces
    let json: Value = client
        .get(format!(
            "{}/api/search?q={}",
            base,
            urlencoding::encode(&format!(r#"{{ resource.service.name = "{}" }}"#, svc_reset))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let traces = json["traces"].as_array().unwrap();
    assert!(
        traces.is_empty(),
        "after per-service reset, TraceQL should find no traces for svc-reset-target"
    );

    // Verify other services' metrics survived the per-service reset
    let json: Value = client
        .get(format!(
            "{}/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(
                r#"otlp_gauge_proto{{service="{}"}}"#,
                svc_otlp_metrics
            ))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(
        results.len(),
        1,
        "other services' metrics should survive per-service reset"
    );

    // --- DELETE /api/v1/reset — verify ALL cleared ---
    let resp = client
        .delete(format!("{}/api/v1/reset", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify all stores are empty via status
    let json: Value = client
        .get(format!("{}/api/v1/status", base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["data"]["totalLogEntries"], 0);
    assert_eq!(json["data"]["totalMetricSeries"], 0);
    assert_eq!(json["data"]["totalSpans"], 0);
    assert_eq!(json["data"]["totalTraces"], 0);

    // Verify services list is empty
    let json: Value = client
        .get(format!("{}/api/v1/services", base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let services = json["data"]["services"].as_array().unwrap();
    assert!(
        services.is_empty(),
        "after full reset, services should be empty, got: {:?}",
        services
    );

    // Verify LogQL returns no data
    let json: Value = client
        .get(format!(
            "{}/loki/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(r#"{{service="{}"}}"#, svc_loki))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let streams = json["data"]["result"].as_array().unwrap();
    assert!(
        streams.is_empty(),
        "after full reset, LogQL should return no streams"
    );

    // Verify PromQL returns no data
    let json: Value = client
        .get(format!(
            "{}/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(
                r#"otlp_gauge_proto{{service="{}"}}"#,
                svc_otlp_metrics
            ))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let results = json["data"]["result"].as_array().unwrap();
    assert!(
        results.is_empty(),
        "after full reset, PromQL should return no series"
    );

    // Verify TraceQL returns no data
    let json: Value = client
        .get(format!(
            "{}/api/search?q={}",
            base,
            urlencoding::encode(&format!(
                r#"{{ resource.service.name = "{}" }}"#,
                svc_otlp_traces
            ))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let traces = json["traces"].as_array().unwrap();
    assert!(
        traces.is_empty(),
        "after full reset, TraceQL should return no traces"
    );

    // ======= SNAPSHOT / RESTORE PHASE (Finding 2) =======
    // First, re-ingest some data so the snapshot has something to save.
    let resp = client
        .post(format!("{}/loki/api/v1/push", base))
        .json(&json!({
            "streams": [{
                "stream": {"service": "svc-snap", "level": "info"},
                "values": [[now_ns.to_string(), "snapshot test line"]]
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client
        .post(format!("{}/v1/metrics", base))
        .header("content-type", "application/x-protobuf")
        .body(make_gauge_request("svc-snap", "snap_gauge", 42.5, now_ns).encode_to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let snap_trace_id: [u8; 16] = [0xFF; 16];
    let resp = client
        .post(format!("{}/v1/traces", base))
        .header("content-type", "application/x-protobuf")
        .body(
            make_trace_request(
                "svc-snap",
                "snap-span",
                &snap_trace_id,
                &[0xFF; 8],
                now_ns - 100_000_000,
                now_ns,
                1,
            )
            .encode_to_vec(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Step 1: Save snapshot from current state
    let snap_dir = tmp_dir.path().join("snap_test");
    obsidian::snapshot::save_from_state(&state, &snap_dir);
    assert!(
        snap_dir.join("obsidian.snap").exists(),
        "snapshot file should exist after save"
    );

    // Step 2: Load snapshot into a new server state
    let (restored_logs, restored_metrics, restored_traces) =
        obsidian::snapshot::load_snapshot(&snap_dir).unwrap();

    let restored_state: obsidian::store::SharedState = Arc::new(obsidian::store::AppState {
        log_store: RwLock::new(restored_logs),
        metric_store: RwLock::new(restored_metrics),
        trace_store: RwLock::new(restored_traces),
        config: obsidian::config::Config {
            port: 0,
            bind_address: "127.0.0.1".into(),
            snapshot_dir: tmp_dir.path().to_string_lossy().into_owned(),
            snapshot_interval: 0,
            max_log_entries: 100_000,
            max_series: 10_000,
            max_spans: 100_000,
            retention: "2h".into(),
            restore: false,
        },
        start_time: std::time::Instant::now(),
    });

    let restored_app = obsidian::server::build_router(restored_state);
    let restored_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let restored_base = format!(
        "http://127.0.0.1:{}",
        restored_listener.local_addr().unwrap().port()
    );
    let restored_server = tokio::spawn(async move {
        axum::serve(restored_listener, restored_app).await.ok();
    });

    // Wait for restored server to be ready
    {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if let Ok(resp) = client.get(format!("{}/ready", restored_base)).send().await {
                if resp.status() == 200 {
                    break;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "restored server did not become ready within 5 seconds"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    // Step 3: Query restored data to verify roundtrip
    // Verify logs survived
    let json: Value = client
        .get(format!(
            "{}/loki/api/v1/query?query={}",
            restored_base,
            urlencoding::encode(r#"{service="svc-snap"}"#)
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let streams = json["data"]["result"].as_array().unwrap();
    assert_eq!(
        streams.len(),
        1,
        "restored server should have 1 log stream for svc-snap"
    );
    let entries = streams[0]["values"].as_array().unwrap();
    assert_eq!(entries[0][1], "snapshot test line");

    // Verify metrics survived
    let json: Value = client
        .get(format!(
            "{}/api/v1/query?query={}",
            restored_base,
            urlencoding::encode(r#"snap_gauge{service="svc-snap"}"#)
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["status"], "success");
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(
        results.len(),
        1,
        "restored server should have snap_gauge metric"
    );
    let val: f64 = results[0]["value"][1].as_str().unwrap().parse().unwrap();
    assert!(
        (val - 42.5).abs() < 0.01,
        "restored snap_gauge should be 42.5, got {}",
        val
    );

    // Verify traces survived
    let snap_trace_hex: String = snap_trace_id.iter().map(|b| format!("{:02x}", b)).collect();
    let json: Value = client
        .get(format!("{}/api/traces/{}", restored_base, snap_trace_hex))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let batches = json["batches"].as_array().unwrap();
    assert_eq!(
        batches.len(),
        1,
        "restored server should have 1 batch for snap trace"
    );

    restored_server.abort();

    server.abort();
}
