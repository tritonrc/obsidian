//! Axum handlers for LogQL query endpoints.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Deserialize;
use serde_json::{Value, json};

use super::eval::{LogQLResult, evaluate_logql};
use super::parser::parse_logql;
use crate::store::SharedState;

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub query: String,
    pub time: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct QueryRangeParams {
    pub query: String,
    pub start: Option<String>,
    pub end: Option<String>,
    pub step: Option<String>,
    pub limit: Option<usize>,
}

/// GET /loki/api/v1/query
pub async fn query(
    State(state): State<SharedState>,
    Query(params): Query<QueryParams>,
) -> impl IntoResponse {
    let expr = match parse_logql(&params.query) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"status": "error", "error": e.to_string()})),
            );
        }
    };

    let now_ns = now_ns();
    let end_ns = match params.time.as_deref() {
        Some(t) => match parse_timestamp_ns(t) {
            Some(ns) => ns,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"status": "error", "error": format!("invalid time: {}", t)})),
                );
            }
        },
        None => now_ns,
    };
    let start_ns = end_ns - 3_600_000_000_000; // default 1h lookback

    let store = state.log_store.read();
    let result = evaluate_logql(&expr, &store, start_ns, end_ns, None);
    let limit = params.limit.unwrap_or(1000);

    (StatusCode::OK, Json(format_logql_result(result, limit)))
}

/// GET /loki/api/v1/query_range
pub async fn query_range(
    State(state): State<SharedState>,
    Query(params): Query<QueryRangeParams>,
) -> impl IntoResponse {
    let expr = match parse_logql(&params.query) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"status": "error", "error": e.to_string()})),
            );
        }
    };

    let now_ns = now_ns();
    let start_ns = match params.start.as_deref() {
        Some(s) => match parse_timestamp_ns(s) {
            Some(ns) => ns,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"status": "error", "error": format!("invalid start: {}", s)})),
                );
            }
        },
        None => now_ns - 3_600_000_000_000,
    };
    let end_ns = match params.end.as_deref() {
        Some(s) => match parse_timestamp_ns(s) {
            Some(ns) => ns,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"status": "error", "error": format!("invalid end: {}", s)})),
                );
            }
        },
        None => now_ns,
    };
    let step_ns = match params.step.as_deref() {
        Some(s) => match crate::config::parse_duration(s).map(|d| d.as_nanos() as i64) {
            Some(ns) => Some(ns),
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"status": "error", "error": format!("invalid step: {}", s)})),
                );
            }
        },
        None => Some(60_000_000_000),
    };

    if step_ns == Some(0) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"status": "error", "error": "step must be positive"})),
        );
    }

    let store = state.log_store.read();
    let result = evaluate_logql(&expr, &store, start_ns, end_ns, step_ns);
    let limit = params.limit.unwrap_or(1000);

    (StatusCode::OK, Json(format_logql_result(result, limit)))
}

/// GET /loki/api/v1/labels
pub async fn labels(State(state): State<SharedState>) -> impl IntoResponse {
    let store = state.log_store.read();
    let names = store.label_names();
    Json(json!({
        "status": "success",
        "data": names,
    }))
}

/// GET /loki/api/v1/label/:name/values
pub async fn label_values(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let store = state.log_store.read();
    let values = store.get_label_values(&name);
    Json(json!({
        "status": "success",
        "data": values,
    }))
}

fn format_logql_result(result: LogQLResult, limit: usize) -> Value {
    match result {
        LogQLResult::Streams(streams) => {
            let result_arr: Vec<Value> = streams
                .into_iter()
                .map(|sr| {
                    let labels_map: serde_json::Map<String, Value> = sr
                        .labels
                        .into_iter()
                        .map(|(k, v)| (k, Value::String(v)))
                        .collect();
                    let values: Vec<Value> = sr
                        .entries
                        .into_iter()
                        .take(limit)
                        .map(|(ts, line)| json!([ts.to_string(), line]))
                        .collect();
                    json!({
                        "stream": labels_map,
                        "values": values,
                    })
                })
                .collect();
            json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": result_arr,
                }
            })
        }
        LogQLResult::Matrix(metrics) => {
            let result_arr: Vec<Value> = metrics
                .into_iter()
                .map(|mr| {
                    let labels_map: serde_json::Map<String, Value> = mr
                        .labels
                        .into_iter()
                        .map(|(k, v)| (k, Value::String(v)))
                        .collect();
                    let values: Vec<Value> = mr
                        .samples
                        .into_iter()
                        .map(|(ts, val)| json!([ts as f64 / 1_000_000_000.0, val.to_string()]))
                        .collect();
                    json!({
                        "metric": labels_map,
                        "values": values,
                    })
                })
                .collect();
            json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": result_arr,
                }
            })
        }
    }
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

fn parse_timestamp_ns(s: &str) -> Option<i64> {
    // Try integer first (preserves precision for nanosecond timestamps)
    if let Ok(n) = s.parse::<i64>() {
        return Some(classify_to_ns(n));
    }
    // Try float seconds (e.g., "1700000000.5")
    if let Ok(secs) = s.parse::<f64>() {
        return Some((secs * 1_000_000_000.0) as i64);
    }
    None
}

fn classify_to_ns(n: i64) -> i64 {
    if n > 1_000_000_000_000_000_000 {
        n // already nanoseconds
    } else if n > 1_000_000_000_000_000 {
        n * 1_000 // microseconds -> ns
    } else if n > 1_000_000_000_000 {
        n * 1_000_000 // milliseconds -> ns
    } else {
        n * 1_000_000_000 // seconds -> ns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_timestamp_ns_nanoseconds() {
        assert_eq!(
            parse_timestamp_ns("1700000000000000000"),
            Some(1700000000000000000)
        );
    }

    #[test]
    fn test_parse_timestamp_ns_seconds() {
        assert_eq!(
            parse_timestamp_ns("1700000000"),
            Some(1700000000_000_000_000)
        );
    }

    #[test]
    fn test_parse_timestamp_ns_milliseconds() {
        assert_eq!(
            parse_timestamp_ns("1700000000000"),
            Some(1700000000_000_000_000)
        );
    }

    #[test]
    fn test_parse_timestamp_ns_microseconds() {
        assert_eq!(
            parse_timestamp_ns("1700000000000000"),
            Some(1700000000_000_000_000)
        );
    }

    #[test]
    fn test_parse_timestamp_ns_float_seconds() {
        let result = parse_timestamp_ns("1700000000.5").unwrap();
        assert!((result - 1700000000_500_000_000).abs() < 1000);
    }
}
