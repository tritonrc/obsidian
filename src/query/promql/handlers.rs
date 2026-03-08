//! Axum handlers for PromQL query endpoints.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use super::eval::{evaluate_instant, evaluate_range, PromQLResult, SeriesResult};
use crate::store::log_store::{LabelMatchOp, LabelMatcher};
use crate::store::SharedState;

#[derive(Debug, Deserialize)]
pub struct InstantQueryParams {
    pub query: String,
    pub time: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RangeQueryParams {
    pub query: String,
    pub start: Option<String>,
    pub end: Option<String>,
    pub step: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SeriesParams {
    #[serde(rename = "match[]", default)]
    pub matchers: Vec<String>,
}

/// GET /api/v1/query
pub async fn query(
    State(state): State<SharedState>,
    Query(params): Query<InstantQueryParams>,
) -> impl IntoResponse {
    let now_ms = now_ms();
    let time_ms = params
        .time
        .as_deref()
        .and_then(parse_timestamp_ms)
        .unwrap_or(now_ms);

    let store = state.metric_store.read();
    match evaluate_instant(&params.query, &store, time_ms) {
        Ok(result) => (StatusCode::OK, Json(format_promql_result(result, time_ms))),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"status": "error", "errorType": "bad_data", "error": e.to_string()})),
        ),
    }
}

/// GET /api/v1/query_range
pub async fn query_range(
    State(state): State<SharedState>,
    Query(params): Query<RangeQueryParams>,
) -> impl IntoResponse {
    let now_ms = now_ms();
    let start_ms = params
        .start
        .as_deref()
        .and_then(parse_timestamp_ms)
        .unwrap_or(now_ms - 3_600_000);
    let end_ms = params
        .end
        .as_deref()
        .and_then(parse_timestamp_ms)
        .unwrap_or(now_ms);
    let step_ms = params
        .step
        .as_deref()
        .and_then(|s| crate::config::parse_duration(s).map(|d| d.as_millis() as i64))
        .unwrap_or(60_000); // default 1m

    if step_ms <= 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"status": "error", "errorType": "bad_data", "error": "step must be positive"})),
        );
    }

    let store = state.metric_store.read();
    match evaluate_range(&params.query, &store, start_ms, end_ms, step_ms) {
        Ok(result) => (StatusCode::OK, Json(format_range_result(result))),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"status": "error", "errorType": "bad_data", "error": e.to_string()})),
        ),
    }
}

/// GET /api/v1/series
pub async fn series(
    State(state): State<SharedState>,
    Query(params): Query<SeriesParams>,
) -> (StatusCode, Json<Value>) {
    let store = state.metric_store.read();
    let mut all_series = Vec::new();

    if params.matchers.is_empty() {
        // Return all series
        for id in store.series.keys() {
            if let Some(labels) = store.get_series_labels(*id) {
                let map: serde_json::Map<String, Value> = labels
                    .into_iter()
                    .map(|(k, v)| (k, Value::String(v)))
                    .collect();
                all_series.push(Value::Object(map));
            }
        }
    } else {
        let mut seen_ids = std::collections::HashSet::new();
        for matchers_str in &params.matchers {
            match promql_parser::parser::parse(matchers_str) {
                Ok(promql_parser::parser::Expr::VectorSelector(vs)) => {
                    let matchers: Vec<LabelMatcher> = vs
                        .matchers
                        .matchers
                        .iter()
                        .map(|m| LabelMatcher {
                            name: m.name.clone(),
                            op: match m.op {
                                promql_parser::label::MatchOp::Equal => LabelMatchOp::Eq,
                                promql_parser::label::MatchOp::NotEqual => LabelMatchOp::Neq,
                                promql_parser::label::MatchOp::Re(_) => LabelMatchOp::Regex,
                                promql_parser::label::MatchOp::NotRe(_) => LabelMatchOp::NotRegex,
                            },
                            value: m.value.clone(),
                        })
                        .collect();
                    let ids = store.select_series(&matchers);
                    for id in ids {
                        if seen_ids.insert(id) {
                            if let Some(labels) = store.get_series_labels(id) {
                                let map: serde_json::Map<String, Value> = labels
                                    .into_iter()
                                    .map(|(k, v)| (k, Value::String(v)))
                                    .collect();
                                all_series.push(Value::Object(map));
                            }
                        }
                    }
                }
                _ => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"status": "error", "errorType": "bad_data", "error": format!("invalid match[] selector: {}", matchers_str)})),
                    );
                }
            }
        }
    }

    (StatusCode::OK, Json(json!({
        "status": "success",
        "data": all_series,
    })))
}

/// GET /api/v1/labels
pub async fn labels(State(state): State<SharedState>) -> impl IntoResponse {
    let store = state.metric_store.read();
    let names = store.label_names();
    Json(json!({
        "status": "success",
        "data": names,
    }))
}

/// GET /api/v1/label/:name/values
pub async fn label_values(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let store = state.metric_store.read();
    let values = store.get_label_values(&name);
    Json(json!({
        "status": "success",
        "data": values,
    }))
}

fn format_promql_result(result: PromQLResult, time_ms: i64) -> Value {
    match result {
        PromQLResult::Scalar(v) => {
            json!({
                "status": "success",
                "data": {
                    "resultType": "scalar",
                    "result": [time_ms as f64 / 1000.0, v.to_string()],
                }
            })
        }
        PromQLResult::InstantVector(series) => {
            let result_arr: Vec<Value> = series.into_iter().map(format_instant_series).collect();
            json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": result_arr,
                }
            })
        }
        PromQLResult::RangeVector(series) => {
            let result_arr: Vec<Value> = series.into_iter().map(format_range_series).collect();
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

fn format_range_result(result: PromQLResult) -> Value {
    match result {
        PromQLResult::Scalar(v) => {
            json!({
                "status": "success",
                "data": {
                    "resultType": "scalar",
                    "result": [0, v.to_string()],
                }
            })
        }
        PromQLResult::InstantVector(series) | PromQLResult::RangeVector(series) => {
            let result_arr: Vec<Value> = series.into_iter().map(format_range_series).collect();
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

fn format_instant_series(sr: SeriesResult) -> Value {
    let labels_map: serde_json::Map<String, Value> = sr
        .labels
        .into_iter()
        .map(|(k, v)| (k, Value::String(v)))
        .collect();
    let value = sr
        .samples
        .first()
        .map(|(t, v)| json!([*t as f64 / 1000.0, v.to_string()]));
    json!({
        "metric": labels_map,
        "value": value,
    })
}

fn format_range_series(sr: SeriesResult) -> Value {
    let labels_map: serde_json::Map<String, Value> = sr
        .labels
        .into_iter()
        .map(|(k, v)| (k, Value::String(v)))
        .collect();
    let values: Vec<Value> = sr
        .samples
        .into_iter()
        .map(|(t, v)| json!([t as f64 / 1000.0, v.to_string()]))
        .collect();
    json!({
        "metric": labels_map,
        "values": values,
    })
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn parse_timestamp_ms(s: &str) -> Option<i64> {
    // Try as float seconds (Prometheus convention)
    if let Ok(secs) = s.parse::<f64>() {
        return Some((secs * 1000.0) as i64);
    }
    // Try as integer milliseconds
    if let Ok(ms) = s.parse::<i64>() {
        return Some(ms);
    }
    None
}
