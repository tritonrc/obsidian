//! E2E smoke test: starts a real TCP server, ingests all three signals via HTTP,
//! queries each API surface. Validates the full network stack that an agent would use.

mod helpers;

use helpers::{make_gauge_request, make_trace_request};
use parking_lot::RwLock;
use prost::Message;
use serde_json::{Value, json};
use std::sync::Arc;

#[tokio::test]
async fn test_e2e_otlp_to_obsidian() {
    let state: obsidian::store::SharedState = Arc::new(obsidian::store::AppState {
        log_store: RwLock::new(obsidian::store::LogStore::new()),
        metric_store: RwLock::new(obsidian::store::MetricStore::new()),
        trace_store: RwLock::new(obsidian::store::TraceStore::new()),
        config: obsidian::config::Config {
            port: 0,
            snapshot_dir: "/tmp/obsidian-e2e".into(),
            snapshot_interval: 0,
            max_log_entries: 100_000,
            max_samples: 10_000,
            max_spans: 100_000,
            retention: "2h".into(),
            restore: false,
        },
        start_time: std::time::Instant::now(),
    });

    let app = obsidian::server::build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });

    let client = reqwest::Client::new();
    let svc = "e2e-test-svc";
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    // Ingest metrics
    let resp = client
        .post(format!("{}/v1/metrics", base))
        .header("content-type", "application/x-protobuf")
        .body(make_gauge_request(svc, "e2e_gauge", 42.0, now_ns).encode_to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // Ingest traces
    let resp = client
        .post(format!("{}/v1/traces", base))
        .header("content-type", "application/x-protobuf")
        .body(
            make_trace_request(
                svc,
                "e2e-span",
                &[0xe2; 16],
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
    assert_eq!(resp.status(), 204);

    // Ingest logs
    let resp = client.post(format!("{}/loki/api/v1/push", base))
        .json(&json!({"streams": [{"stream": {"service": svc}, "values": [[now_ns.to_string(), "e2e log"]]}]}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 204);

    // PromQL
    let json: Value = client
        .get(format!(
            "{}/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(r#"e2e_gauge{{service="{}"}}"#, svc))
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

    // TraceQL
    let json: Value = client
        .get(format!(
            "{}/api/search?q={}",
            base,
            urlencoding::encode(&format!(r#"{{ resource.service.name = "{}" }}"#, svc))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!json["traces"].as_array().unwrap().is_empty());

    // LogQL
    let json: Value = client
        .get(format!(
            "{}/loki/api/v1/query?query={}",
            base,
            urlencoding::encode(&format!(r#"{{service="{}"}}"#, svc))
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!json["data"]["result"].as_array().unwrap().is_empty());

    // Services — all three signals present
    let json: Value = client
        .get(format!("{}/api/v1/services", base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let svc_entry = json["data"]["services"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"].as_str() == Some(svc))
        .unwrap()
        .clone();
    let signals: Vec<&str> = svc_entry["signals"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s.as_str())
        .collect();
    assert!(signals.contains(&"metrics"));
    assert!(signals.contains(&"traces"));
    assert!(signals.contains(&"logs"));

    server.abort();
}
