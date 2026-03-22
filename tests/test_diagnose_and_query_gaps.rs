//! Contract tests for diagnostic endpoint and query surface gaps:
//!
//! 1. GET /api/v1/diagnose?service=X — health assessment endpoint
//! 2. LogQL sum_over_time, avg_over_time, min_over_time, max_over_time
//! 3. LogQL | logfmt parser stage
//! 4. PromQL absent()
//! 5. PromQL delta(), deriv()
//! 6. PromQL sort(), sort_desc()
//! 7. PromQL clamp(), clamp_min(), clamp_max()
//! 8. PromQL time(), vector(), scalar()
//! 9. TraceQL avg(duration), max(duration), min(duration) aggregates
//!
//! These tests define the expected contract for NEW features. They will fail
//! until the corresponding handlers and evaluator changes are implemented.

mod helpers;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use helpers::{ingest_traces, make_state};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Send a GET request and return (status, parsed JSON body).
async fn get_json(app: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).unwrap_or(Value::Null)
    };
    (status, json)
}

/// Run a PromQL instant query and return the full JSON response.
async fn promql_instant(app: &axum::Router, query: &str, time_s: i64) -> Value {
    let encoded = urlencoding::encode(query);
    let uri = format!("/api/v1/query?query={encoded}&time={time_s}");
    let (status, json) = get_json(app, &uri).await;
    assert_eq!(status, StatusCode::OK, "PromQL query failed: {json}");
    json
}

/// Extract the single vector value from a PromQL instant query result.
fn single_vector_value(json: &Value) -> f64 {
    let results = json["data"]["result"].as_array().unwrap();
    assert_eq!(
        results.len(),
        1,
        "expected exactly one result series, got: {json}"
    );
    results[0]["value"][1].as_str().unwrap().parse().unwrap()
}

/// Run a LogQL query_range and return the full JSON response.
async fn logql_query_range(app: &axum::Router, query: &str, start_ns: u64, end_ns: u64) -> Value {
    let encoded = urlencoding::encode(query);
    let uri = format!("/loki/api/v1/query_range?query={encoded}&start={start_ns}&end={end_ns}");
    let (status, json) = get_json(app, &uri).await;
    assert_eq!(status, StatusCode::OK, "LogQL query_range failed: {json}");
    json
}

/// Encode a [u8; 16] trace ID as a hex string.
fn hex_trace_id(bytes: &[u8; 16]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

// ===========================================================================
// 1. GET /api/v1/diagnose?service=X — health assessment
// ===========================================================================
//
// Expected response:
// {
//   "service": "X",
//   "health_score": 0-100,
//   "slowest_traces": [...],
//   "error_trend": [...],
//   "recent_errors": [...],
//   "suggested_queries": [...]
// }

#[tokio::test]
async fn test_diagnose_returns_health_assessment() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let now_ns_str = now_ns.to_string();

    // Ingest error logs for the service
    let push_body = serde_json::json!({
        "streams": [{
            "stream": {"service": "checkout", "level": "error"},
            "values": [
                [&now_ns_str, "payment processing failed"],
                [&(now_ns + 1_000_000).to_string(), "card declined"],
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

    // Ingest a slow error trace
    let tid: [u8; 16] = [0xdd; 16];
    ingest_traces(
        &app,
        "checkout",
        "charge_card",
        &tid,
        &[1u8; 8],
        now_ns,
        now_ns + 5_000_000_000, // 5 second span
        2,                      // Error
    )
    .await;

    // Ingest an OK trace
    let tid_ok: [u8; 16] = [0xee; 16];
    ingest_traces(
        &app,
        "checkout",
        "list_items",
        &tid_ok,
        &[2u8; 8],
        now_ns,
        now_ns + 50_000_000, // 50ms span
        1,                   // Ok
    )
    .await;

    // Query the diagnose endpoint
    let (status, json) = get_json(&app, "/api/v1/diagnose?service=checkout").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "diagnose endpoint should return 200"
    );

    // Verify required fields
    assert_eq!(json["service"].as_str().unwrap(), "checkout");

    let health_score = json["health_score"].as_f64().unwrap();
    assert!(
        (0.0..=100.0).contains(&health_score),
        "health_score should be 0-100, got: {}",
        health_score
    );

    // With errors present, health_score should be less than 100
    assert!(
        health_score < 100.0,
        "health_score should be degraded with errors, got: {}",
        health_score
    );

    // slowest_traces should be a non-empty array
    let slowest = json["slowest_traces"]
        .as_array()
        .expect("slowest_traces should be an array");
    assert!(
        !slowest.is_empty(),
        "expected at least one slow trace, got: {json}"
    );

    // error_trend should be an array
    assert!(
        json["error_trend"].is_array(),
        "error_trend should be an array"
    );

    // recent_errors should contain our error log lines
    let recent = json["recent_errors"]
        .as_array()
        .expect("recent_errors should be an array");
    assert!(
        !recent.is_empty(),
        "expected at least one recent error, got: {json}"
    );

    // suggested_queries should be a non-empty array of strings
    let suggestions = json["suggested_queries"]
        .as_array()
        .expect("suggested_queries should be an array");
    assert!(
        !suggestions.is_empty(),
        "expected at least one suggested query"
    );
}

#[tokio::test]
async fn test_diagnose_global_returns_service_list() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let now_ns_str = now_ns.to_string();

    // Ingest data for two services
    let push_body = serde_json::json!({
        "streams": [
            {
                "stream": {"service": "svc-a", "level": "error"},
                "values": [[&now_ns_str, "svc-a error"]]
            },
            {
                "stream": {"service": "svc-b", "level": "info"},
                "values": [[&now_ns_str, "svc-b info"]]
            }
        ]
    });
    let req = Request::builder()
        .method("POST")
        .uri("/loki/api/v1/push")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&push_body).unwrap()))
        .unwrap();
    app.clone().oneshot(req).await.unwrap();

    // Query without service param => global overview
    let (status, json) = get_json(&app, "/api/v1/diagnose").await;
    assert_eq!(status, StatusCode::OK, "global diagnose should return 200");

    let services = json["services"]
        .as_array()
        .expect("response should have services array");
    assert!(
        services.len() >= 2,
        "expected at least 2 services, got: {}",
        json
    );

    // Each entry should have service, health_score, top_issue
    for entry in services {
        assert!(
            entry["service"].is_string(),
            "each entry should have a service name"
        );
        assert!(
            entry["health_score"].is_f64()
                || entry["health_score"].is_i64()
                || entry["health_score"].is_u64(),
            "each entry should have a health_score"
        );
        assert!(
            entry["top_issue"].is_string(),
            "each entry should have a top_issue"
        );
    }

    // svc-a has errors, so it should appear before svc-b (lower score first)
    let names: Vec<&str> = services
        .iter()
        .filter_map(|e| e["service"].as_str())
        .collect();
    assert!(names.contains(&"svc-a"), "svc-a should be in the list");
    assert!(names.contains(&"svc-b"), "svc-b should be in the list");

    // svc-a should have a worse (lower) health score than svc-b
    let svc_a_score = services
        .iter()
        .find(|e| e["service"].as_str() == Some("svc-a"))
        .unwrap()["health_score"]
        .as_f64()
        .unwrap();
    let svc_b_score = services
        .iter()
        .find(|e| e["service"].as_str() == Some("svc-b"))
        .unwrap()["health_score"]
        .as_f64()
        .unwrap();
    assert!(
        svc_a_score < svc_b_score,
        "svc-a (errors) should have lower score ({}) than svc-b ({})",
        svc_a_score,
        svc_b_score
    );
}

#[tokio::test]
async fn test_diagnose_unknown_service_returns_healthy() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let (status, json) = get_json(&app, "/api/v1/diagnose?service=nonexistent").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["service"].as_str().unwrap(), "nonexistent");

    // Unknown service should have perfect health (no errors)
    let health_score = json["health_score"].as_f64().unwrap();
    assert!(
        (health_score - 100.0).abs() < f64::EPSILON,
        "unknown service should have health_score 100, got: {}",
        health_score
    );
}

// ===========================================================================
// 2. LogQL sum_over_time, avg_over_time, min_over_time, max_over_time
// ===========================================================================
//
// These functions operate on numeric log lines within a time range.
// Expected: numeric aggregation over matching log entries.

#[tokio::test]
async fn test_logql_sum_over_time_on_numeric_lines() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let ts_base = 1_700_000_000_000_000_000u64;
    let push_body = serde_json::json!({
        "streams": [{
            "stream": {"service": "loadgen", "level": "info"},
            "values": [
                [(ts_base).to_string(), "10"],
                [(ts_base + 1_000_000_000).to_string(), "20"],
                [(ts_base + 2_000_000_000).to_string(), "30"],
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

    // sum_over_time should return 60 (10 + 20 + 30)
    let query = r#"sum_over_time({service="loadgen"} [5s])"#;
    let json = logql_query_range(
        &app,
        query,
        ts_base - 1_000_000_000,
        ts_base + 5_000_000_000,
    )
    .await;

    assert_eq!(json["status"], "success");
    let result = json["data"]["result"].as_array().unwrap();
    assert!(!result.is_empty(), "sum_over_time should return results");

    // Find the sample value(s) — the result should contain a sum of 60
    let values = result[0]["values"].as_array().unwrap();
    let last_val: f64 = values.last().unwrap()[1].as_str().unwrap().parse().unwrap();
    assert!(
        (last_val - 60.0).abs() < f64::EPSILON,
        "sum_over_time should be 60, got: {}",
        last_val
    );
}

#[tokio::test]
async fn test_logql_avg_over_time_on_numeric_lines() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let ts_base = 1_700_000_000_000_000_000u64;
    let push_body = serde_json::json!({
        "streams": [{
            "stream": {"service": "loadgen2", "level": "info"},
            "values": [
                [(ts_base).to_string(), "10"],
                [(ts_base + 1_000_000_000).to_string(), "20"],
                [(ts_base + 2_000_000_000).to_string(), "30"],
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

    // avg_over_time should return 20 (mean of 10, 20, 30)
    let query = r#"avg_over_time({service="loadgen2"} [5s])"#;
    let json = logql_query_range(
        &app,
        query,
        ts_base - 1_000_000_000,
        ts_base + 5_000_000_000,
    )
    .await;

    assert_eq!(json["status"], "success");
    let result = json["data"]["result"].as_array().unwrap();
    assert!(!result.is_empty(), "avg_over_time should return results");

    let values = result[0]["values"].as_array().unwrap();
    let last_val: f64 = values.last().unwrap()[1].as_str().unwrap().parse().unwrap();
    assert!(
        (last_val - 20.0).abs() < f64::EPSILON,
        "avg_over_time should be 20, got: {}",
        last_val
    );
}

#[tokio::test]
async fn test_logql_min_over_time_on_numeric_lines() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let ts_base = 1_700_000_000_000_000_000u64;
    let push_body = serde_json::json!({
        "streams": [{
            "stream": {"service": "loadgen3", "level": "info"},
            "values": [
                [(ts_base).to_string(), "10"],
                [(ts_base + 1_000_000_000).to_string(), "20"],
                [(ts_base + 2_000_000_000).to_string(), "5"],
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

    let query = r#"min_over_time({service="loadgen3"} [5s])"#;
    let json = logql_query_range(
        &app,
        query,
        ts_base - 1_000_000_000,
        ts_base + 5_000_000_000,
    )
    .await;

    assert_eq!(json["status"], "success");
    let result = json["data"]["result"].as_array().unwrap();
    assert!(!result.is_empty(), "min_over_time should return results");

    let values = result[0]["values"].as_array().unwrap();
    let last_val: f64 = values.last().unwrap()[1].as_str().unwrap().parse().unwrap();
    assert!(
        (last_val - 5.0).abs() < f64::EPSILON,
        "min_over_time should be 5, got: {}",
        last_val
    );
}

#[tokio::test]
async fn test_logql_max_over_time_on_numeric_lines() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let ts_base = 1_700_000_000_000_000_000u64;
    let push_body = serde_json::json!({
        "streams": [{
            "stream": {"service": "loadgen4", "level": "info"},
            "values": [
                [(ts_base).to_string(), "10"],
                [(ts_base + 1_000_000_000).to_string(), "50"],
                [(ts_base + 2_000_000_000).to_string(), "30"],
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

    let query = r#"max_over_time({service="loadgen4"} [5s])"#;
    let json = logql_query_range(
        &app,
        query,
        ts_base - 1_000_000_000,
        ts_base + 5_000_000_000,
    )
    .await;

    assert_eq!(json["status"], "success");
    let result = json["data"]["result"].as_array().unwrap();
    assert!(!result.is_empty(), "max_over_time should return results");

    let values = result[0]["values"].as_array().unwrap();
    let last_val: f64 = values.last().unwrap()[1].as_str().unwrap().parse().unwrap();
    assert!(
        (last_val - 50.0).abs() < f64::EPSILON,
        "max_over_time should be 50, got: {}",
        last_val
    );
}

// ===========================================================================
// 3. LogQL | logfmt parser stage
// ===========================================================================
//
// The `| logfmt` stage extracts key=value pairs from logfmt-formatted lines
// and makes them available as labels for further filtering.

#[tokio::test]
async fn test_logql_logfmt_parser_stage() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let ts_base = 1_700_000_000_000_000_000u64;
    let push_body = serde_json::json!({
        "streams": [{
            "stream": {"service": "api-gw"},
            "values": [
                [(ts_base).to_string(), "level=error msg=\"request failed\" status=500"],
                [(ts_base + 1_000_000_000).to_string(), "level=info msg=\"request ok\" status=200"],
                [(ts_base + 2_000_000_000).to_string(), "level=error msg=\"timeout\" status=504"],
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

    // Use | logfmt then filter on extracted "level" label
    let query = r#"{service="api-gw"} | logfmt | level="error""#;
    let json = logql_query_range(
        &app,
        query,
        ts_base - 1_000_000_000,
        ts_base + 5_000_000_000,
    )
    .await;

    assert_eq!(json["status"], "success");
    let result = json["data"]["result"].as_array().unwrap();
    assert!(!result.is_empty(), "logfmt filter should return results");

    let values = result[0]["values"].as_array().unwrap();
    assert_eq!(
        values.len(),
        2,
        "should match 2 lines with level=error, got: {}",
        serde_json::to_string_pretty(&values).unwrap()
    );

    // Both matched lines should contain "error"
    for v in values {
        let line = v[1].as_str().unwrap();
        assert!(
            line.contains("level=error"),
            "expected line with level=error, got: {}",
            line
        );
    }
}

#[tokio::test]
async fn test_logql_logfmt_extracts_numeric_field() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let ts_base = 1_700_000_000_000_000_000u64;
    let push_body = serde_json::json!({
        "streams": [{
            "stream": {"service": "api-gw2"},
            "values": [
                [(ts_base).to_string(), "status=500 duration=120ms"],
                [(ts_base + 1_000_000_000).to_string(), "status=200 duration=5ms"],
                [(ts_base + 2_000_000_000).to_string(), "status=504 duration=30000ms"],
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

    // Use logfmt to extract status, then filter for 5xx
    let query = r#"{service="api-gw2"} | logfmt | status="500""#;
    let json = logql_query_range(
        &app,
        query,
        ts_base - 1_000_000_000,
        ts_base + 5_000_000_000,
    )
    .await;

    assert_eq!(json["status"], "success");
    let result = json["data"]["result"].as_array().unwrap();
    assert!(
        !result.is_empty(),
        "logfmt status filter should return results"
    );

    let values = result[0]["values"].as_array().unwrap();
    assert_eq!(values.len(), 1, "should match 1 line with status=500");
}

// ===========================================================================
// 4. PromQL absent()
// ===========================================================================
//
// absent(metric) returns an empty vector if the metric exists, or a
// single-element vector {__name__="metric"} with value 1 if it does not.

#[tokio::test]
async fn test_promql_absent_returns_1_for_missing_metric() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let now_s = now_ms / 1000;

    let json = promql_instant(&app, "absent(nonexistent_metric)", now_s).await;
    let result = json["data"]["result"].as_array().unwrap();
    assert_eq!(
        result.len(),
        1,
        "absent() for missing metric should return 1 result, got: {json}"
    );
    let val: f64 = result[0]["value"][1].as_str().unwrap().parse().unwrap();
    assert!(
        (val - 1.0).abs() < f64::EPSILON,
        "absent() for missing metric should be 1, got: {}",
        val
    );
}

#[tokio::test]
async fn test_promql_absent_returns_empty_for_existing_metric() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let now_s = now_ms / 1000;

    // Ingest a metric so it exists
    {
        let mut store = state.metric_store.write();
        store.ingest_samples(
            "existing_metric",
            vec![("host".into(), "a".into())],
            vec![obsidian::store::metric_store::Sample {
                timestamp_ms: now_ms,
                value: 42.0,
            }],
        );
    }

    let json = promql_instant(&app, "absent(existing_metric)", now_s).await;
    let result = json["data"]["result"].as_array().unwrap();
    assert!(
        result.is_empty(),
        "absent() for existing metric should return empty vector, got: {json}"
    );
}

// ===========================================================================
// 5. PromQL delta() and deriv()
// ===========================================================================
//
// delta() returns the difference between first and last values in the range.
// deriv() returns the per-second derivative via simple linear regression.

#[tokio::test]
async fn test_promql_delta() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let base_ms = 1_700_000_000_000i64;

    {
        let mut store = state.metric_store.write();
        store.ingest_samples(
            "temperature",
            vec![("sensor".into(), "a".into())],
            vec![
                obsidian::store::metric_store::Sample {
                    timestamp_ms: base_ms,
                    value: 20.0,
                },
                obsidian::store::metric_store::Sample {
                    timestamp_ms: base_ms + 60_000,
                    value: 25.0,
                },
                obsidian::store::metric_store::Sample {
                    timestamp_ms: base_ms + 120_000,
                    value: 30.0,
                },
            ],
        );
    }

    let time_s = (base_ms + 120_000) / 1000;
    let json = promql_instant(&app, "delta(temperature[3m])", time_s).await;
    let result = json["data"]["result"].as_array().unwrap();
    assert!(
        !result.is_empty(),
        "delta should return results, got: {json}"
    );

    let val: f64 = result[0]["value"][1].as_str().unwrap().parse().unwrap();
    // delta = last - first = 30 - 20 = 10
    assert!(
        (val - 10.0).abs() < 1.0,
        "delta should be ~10, got: {}",
        val
    );
}

#[tokio::test]
async fn test_promql_deriv() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let base_ms = 1_700_000_000_000i64;

    {
        let mut store = state.metric_store.write();
        store.ingest_samples(
            "linear_gauge",
            vec![("host".into(), "a".into())],
            vec![
                obsidian::store::metric_store::Sample {
                    timestamp_ms: base_ms,
                    value: 0.0,
                },
                obsidian::store::metric_store::Sample {
                    timestamp_ms: base_ms + 60_000,
                    value: 60.0,
                },
                obsidian::store::metric_store::Sample {
                    timestamp_ms: base_ms + 120_000,
                    value: 120.0,
                },
            ],
        );
    }

    let time_s = (base_ms + 120_000) / 1000;
    let json = promql_instant(&app, "deriv(linear_gauge[3m])", time_s).await;
    let result = json["data"]["result"].as_array().unwrap();
    assert!(
        !result.is_empty(),
        "deriv should return results, got: {json}"
    );

    let val: f64 = result[0]["value"][1].as_str().unwrap().parse().unwrap();
    // Linear increase of 1.0 per second (60 per 60s)
    assert!(
        (val - 1.0).abs() < 0.1,
        "deriv should be ~1.0/s, got: {}",
        val
    );
}

// ===========================================================================
// 6. PromQL sort() and sort_desc()
// ===========================================================================
//
// sort() returns series sorted by value ascending.
// sort_desc() returns series sorted by value descending.

#[tokio::test]
async fn test_promql_sort() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let now_s = now_ms / 1000;

    {
        let mut store = state.metric_store.write();
        for (i, val) in [30.0, 10.0, 50.0, 20.0, 40.0].iter().enumerate() {
            store.ingest_samples(
                "sort_test",
                vec![("instance".into(), format!("node-{}", i))],
                vec![obsidian::store::metric_store::Sample {
                    timestamp_ms: now_ms,
                    value: *val,
                }],
            );
        }
    }

    let json = promql_instant(&app, "sort(sort_test)", now_s).await;
    let result = json["data"]["result"].as_array().unwrap();
    assert_eq!(result.len(), 5, "sort should return all 5 series");

    let values: Vec<f64> = result
        .iter()
        .map(|r| r["value"][1].as_str().unwrap().parse::<f64>().unwrap())
        .collect();
    // Values should already be in ascending order
    for w in values.windows(2) {
        assert!(
            w[0] <= w[1],
            "sort() should return ascending order, got: {:?}",
            values
        );
    }
}

#[tokio::test]
async fn test_promql_sort_desc() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let now_s = now_ms / 1000;

    {
        let mut store = state.metric_store.write();
        for (i, val) in [30.0, 10.0, 50.0, 20.0, 40.0].iter().enumerate() {
            store.ingest_samples(
                "sortdesc_test",
                vec![("instance".into(), format!("node-{}", i))],
                vec![obsidian::store::metric_store::Sample {
                    timestamp_ms: now_ms,
                    value: *val,
                }],
            );
        }
    }

    let json = promql_instant(&app, "sort_desc(sortdesc_test)", now_s).await;
    let result = json["data"]["result"].as_array().unwrap();
    assert_eq!(result.len(), 5, "sort_desc should return all 5 series");

    let values: Vec<f64> = result
        .iter()
        .map(|r| r["value"][1].as_str().unwrap().parse::<f64>().unwrap())
        .collect();
    // Values should be in descending order
    for w in values.windows(2) {
        assert!(
            w[0] >= w[1],
            "sort_desc() should return descending order, got: {:?}",
            values
        );
    }
}

// ===========================================================================
// 7. PromQL clamp(), clamp_min(), clamp_max()
// ===========================================================================

#[tokio::test]
async fn test_promql_clamp() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let now_s = now_ms / 1000;

    {
        let mut store = state.metric_store.write();
        store.ingest_samples(
            "clamp_test",
            vec![("host".into(), "a".into())],
            vec![obsidian::store::metric_store::Sample {
                timestamp_ms: now_ms,
                value: 150.0,
            }],
        );
    }

    // clamp(metric, 0, 100) should clamp 150 down to 100
    let json = promql_instant(&app, "clamp(clamp_test, 0, 100)", now_s).await;
    let val = single_vector_value(&json);
    assert!(
        (val - 100.0).abs() < f64::EPSILON,
        "clamp(150, 0, 100) should be 100, got: {}",
        val
    );
}

#[tokio::test]
async fn test_promql_clamp_min() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let now_s = now_ms / 1000;

    {
        let mut store = state.metric_store.write();
        store.ingest_samples(
            "clampmin_test",
            vec![("host".into(), "a".into())],
            vec![obsidian::store::metric_store::Sample {
                timestamp_ms: now_ms,
                value: -5.0,
            }],
        );
    }

    // clamp_min(metric, 0) should clamp -5 up to 0
    let json = promql_instant(&app, "clamp_min(clampmin_test, 0)", now_s).await;
    let val = single_vector_value(&json);
    assert!(
        val.abs() < f64::EPSILON,
        "clamp_min(-5, 0) should be 0, got: {}",
        val
    );
}

#[tokio::test]
async fn test_promql_clamp_max() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let now_s = now_ms / 1000;

    {
        let mut store = state.metric_store.write();
        store.ingest_samples(
            "clampmax_test",
            vec![("host".into(), "a".into())],
            vec![obsidian::store::metric_store::Sample {
                timestamp_ms: now_ms,
                value: 200.0,
            }],
        );
    }

    // clamp_max(metric, 50) should clamp 200 down to 50
    let json = promql_instant(&app, "clamp_max(clampmax_test, 50)", now_s).await;
    let val = single_vector_value(&json);
    assert!(
        (val - 50.0).abs() < f64::EPSILON,
        "clamp_max(200, 50) should be 50, got: {}",
        val
    );
}

// ===========================================================================
// 8. PromQL time(), vector(), scalar()
// ===========================================================================
//
// time() returns the current evaluation timestamp as a scalar.
// vector(scalar) converts a scalar to a single-element vector.
// scalar(vector) converts a single-element vector to a scalar.

#[tokio::test]
async fn test_promql_time() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let eval_time_s: i64 = 1_700_000_000;
    let json = promql_instant(&app, "time()", eval_time_s).await;

    let result = json["data"]["result"].as_array().unwrap();
    assert!(
        !result.is_empty(),
        "time() should return a result, got: {json}"
    );

    // time() should return the evaluation timestamp
    let val: f64 = result[0]["value"][1].as_str().unwrap().parse().unwrap();
    assert!(
        (val - eval_time_s as f64).abs() < 1.0,
        "time() should return evaluation time {}, got: {}",
        eval_time_s,
        val
    );
}

#[tokio::test]
async fn test_promql_vector() {
    let state = make_state();
    let app = obsidian::server::build_router(state);

    let json = promql_instant(&app, "vector(42)", 1_700_000_000).await;

    let result = json["data"]["result"].as_array().unwrap();
    assert_eq!(
        result.len(),
        1,
        "vector(42) should return exactly 1 result, got: {json}"
    );

    let val: f64 = result[0]["value"][1].as_str().unwrap().parse().unwrap();
    assert!(
        (val - 42.0).abs() < f64::EPSILON,
        "vector(42) should be 42, got: {}",
        val
    );
}

#[tokio::test]
async fn test_promql_scalar() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let now_s = now_ms / 1000;

    {
        let mut store = state.metric_store.write();
        store.ingest_samples(
            "scalar_src",
            vec![("host".into(), "a".into())],
            vec![obsidian::store::metric_store::Sample {
                timestamp_ms: now_ms,
                value: 99.0,
            }],
        );
    }

    // scalar() on a single-element vector should return a scalar result
    let json = promql_instant(&app, "scalar(scalar_src)", now_s).await;

    // Scalar result type: {"resultType": "scalar", "result": [timestamp, "value"]}
    // Or it may come back as a vector with one element depending on implementation
    let result_type = json["data"]["resultType"].as_str().unwrap();
    if result_type == "scalar" {
        let val: f64 = json["data"]["result"][1].as_str().unwrap().parse().unwrap();
        assert!(
            (val - 99.0).abs() < f64::EPSILON,
            "scalar() should return 99, got: {}",
            val
        );
    } else {
        // Some implementations return scalar results as single-element vectors
        let val = single_vector_value(&json);
        assert!(
            (val - 99.0).abs() < f64::EPSILON,
            "scalar() should return 99, got: {}",
            val
        );
    }
}

// ===========================================================================
// 9. TraceQL avg(duration), max(duration), min(duration) aggregates
// ===========================================================================
//
// Expected contract: { ... } | avg(duration) > Xms, etc.
// The search endpoint should filter traces based on aggregate duration criteria.

#[tokio::test]
async fn test_traceql_avg_duration_aggregate() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    // Trace 1: two spans, avg duration = (100ms + 200ms) / 2 = 150ms
    let tid1: [u8; 16] = [0x01; 16];
    ingest_traces(
        &app,
        "backend",
        "handler",
        &tid1,
        &[1u8; 8],
        1_000_000_000,
        1_100_000_000, // 100ms
        1,
    )
    .await;
    // Second span in same trace
    {
        let trace_req = helpers::make_trace_request(
            "backend",
            "db_query",
            &tid1,
            &[2u8; 8],
            1_000_000_000,
            1_200_000_000, // 200ms
            1,
        );
        let r = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header("content-type", "application/x-protobuf")
            .body(Body::from(prost::Message::encode_to_vec(&trace_req)))
            .unwrap();
        app.clone().oneshot(r).await.unwrap();
    }

    // Trace 2: one span, avg duration = 500ms
    let tid2: [u8; 16] = [0x02; 16];
    ingest_traces(
        &app,
        "backend",
        "slow_op",
        &tid2,
        &[3u8; 8],
        2_000_000_000,
        2_500_000_000, // 500ms
        1,
    )
    .await;

    // Query: traces where avg(duration) > 200ms
    let query =
        urlencoding::encode(r#"{ resource.service.name = "backend" } | avg(duration) > 200ms"#);
    let (status, json) = get_json(&app, &format!("/api/search?q={query}")).await;
    assert_eq!(status, StatusCode::OK);

    let traces = json["traces"].as_array().unwrap();
    // Only trace 2 (avg 500ms) should match, trace 1 (avg 150ms) should not
    assert_eq!(
        traces.len(),
        1,
        "expected 1 trace with avg(duration) > 200ms, got: {json}"
    );
    assert_eq!(
        traces[0]["traceID"].as_str().unwrap(),
        hex_trace_id(&tid2),
        "the matching trace should be tid2"
    );
}

#[tokio::test]
async fn test_traceql_max_duration_aggregate() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    // Trace 1: max span duration = 200ms
    let tid1: [u8; 16] = [0x11; 16];
    ingest_traces(
        &app,
        "frontend",
        "render",
        &tid1,
        &[1u8; 8],
        1_000_000_000,
        1_050_000_000, // 50ms
        1,
    )
    .await;
    {
        let trace_req = helpers::make_trace_request(
            "frontend",
            "fetch_data",
            &tid1,
            &[2u8; 8],
            1_000_000_000,
            1_200_000_000, // 200ms
            1,
        );
        let r = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header("content-type", "application/x-protobuf")
            .body(Body::from(prost::Message::encode_to_vec(&trace_req)))
            .unwrap();
        app.clone().oneshot(r).await.unwrap();
    }

    // Trace 2: max span duration = 1s
    let tid2: [u8; 16] = [0x22; 16];
    ingest_traces(
        &app,
        "frontend",
        "heavy_compute",
        &tid2,
        &[3u8; 8],
        2_000_000_000,
        3_000_000_000, // 1s
        1,
    )
    .await;

    // Query: traces where max(duration) > 500ms
    let query =
        urlencoding::encode(r#"{ resource.service.name = "frontend" } | max(duration) > 500ms"#);
    let (status, json) = get_json(&app, &format!("/api/search?q={query}")).await;
    assert_eq!(status, StatusCode::OK);

    let traces = json["traces"].as_array().unwrap();
    assert_eq!(
        traces.len(),
        1,
        "expected 1 trace with max(duration) > 500ms, got: {json}"
    );
    assert_eq!(traces[0]["traceID"].as_str().unwrap(), hex_trace_id(&tid2),);
}

#[tokio::test]
async fn test_traceql_min_duration_aggregate() {
    let state = make_state();
    let app = obsidian::server::build_router(state.clone());

    // Trace 1: min span duration = 10ms
    let tid1: [u8; 16] = [0x31; 16];
    ingest_traces(
        &app,
        "worker",
        "fast_op",
        &tid1,
        &[1u8; 8],
        1_000_000_000,
        1_010_000_000, // 10ms
        1,
    )
    .await;
    {
        let trace_req = helpers::make_trace_request(
            "worker",
            "slow_op",
            &tid1,
            &[2u8; 8],
            1_000_000_000,
            1_500_000_000, // 500ms
            1,
        );
        let r = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header("content-type", "application/x-protobuf")
            .body(Body::from(prost::Message::encode_to_vec(&trace_req)))
            .unwrap();
        app.clone().oneshot(r).await.unwrap();
    }

    // Trace 2: min span duration = 200ms (only span)
    let tid2: [u8; 16] = [0x32; 16];
    ingest_traces(
        &app,
        "worker",
        "medium_op",
        &tid2,
        &[3u8; 8],
        2_000_000_000,
        2_200_000_000, // 200ms
        1,
    )
    .await;

    // Query: traces where min(duration) > 100ms
    // Should only match trace 2 (min=200ms), not trace 1 (min=10ms)
    let query =
        urlencoding::encode(r#"{ resource.service.name = "worker" } | min(duration) > 100ms"#);
    let (status, json) = get_json(&app, &format!("/api/search?q={query}")).await;
    assert_eq!(status, StatusCode::OK);

    let traces = json["traces"].as_array().unwrap();
    assert_eq!(
        traces.len(),
        1,
        "expected 1 trace with min(duration) > 100ms, got: {json}"
    );
    assert_eq!(traces[0]["traceID"].as_str().unwrap(), hex_trace_id(&tid2),);
}
