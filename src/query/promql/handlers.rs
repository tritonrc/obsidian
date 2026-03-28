//! Axum handlers for PromQL query endpoints.

use axum::Json;
use axum::extract::Form;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Deserialize;
use serde_json::{Value, json};

use super::eval::{PromQLResult, SeriesResult, evaluate_instant, evaluate_range};
use crate::store::SharedState;
use crate::store::log_store::{LabelMatchOp, LabelMatcher};

/// Hint included in PromQL parse error responses to help agents construct valid queries.
const PROMQL_HINT: &str = "Example: rate(http_requests_total[5m])";

/// Maximum number of steps allowed in a range query. Matches Prometheus default of 11,000.
const MAX_QUERY_STEPS: i64 = 11_000;

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
    pub start: Option<String>,
    pub end: Option<String>,
}

/// GET /api/v1/query
pub async fn query(
    State(state): State<SharedState>,
    Query(params): Query<InstantQueryParams>,
) -> impl IntoResponse {
    query_inner(state, params).await
}

/// POST /api/v1/query
pub async fn query_post(
    State(state): State<SharedState>,
    Form(params): Form<InstantQueryParams>,
) -> impl IntoResponse {
    query_inner(state, params).await
}

async fn query_inner(state: SharedState, params: InstantQueryParams) -> (StatusCode, Json<Value>) {
    let now_ms = now_ms();
    let time_ms = match params.time.as_deref() {
        Some(t) => match parse_timestamp_ms(t) {
            Some(ms) => ms,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(
                        json!({"status": "error", "errorType": "bad_data", "error": format!("invalid time: {}", t)}),
                    ),
                );
            }
        },
        None => now_ms,
    };

    let store = state.metric_store.read();
    match evaluate_instant(&params.query, &store, time_ms) {
        Ok(result) => (StatusCode::OK, Json(format_promql_result(result, time_ms))),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(
                json!({"status": "error", "errorType": "bad_data", "error": e.to_string(), "hint": PROMQL_HINT}),
            ),
        ),
    }
}

/// GET /api/v1/query_range
pub async fn query_range(
    State(state): State<SharedState>,
    Query(params): Query<RangeQueryParams>,
) -> impl IntoResponse {
    query_range_inner(state, params).await
}

/// POST /api/v1/query_range
pub async fn query_range_post(
    State(state): State<SharedState>,
    Form(params): Form<RangeQueryParams>,
) -> impl IntoResponse {
    query_range_inner(state, params).await
}

async fn query_range_inner(
    state: SharedState,
    params: RangeQueryParams,
) -> (StatusCode, Json<Value>) {
    let now_ms = now_ms();
    let start_ms = match params.start.as_deref() {
        Some(s) => match parse_timestamp_ms(s) {
            Some(ms) => ms,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(
                        json!({"status": "error", "errorType": "bad_data", "error": format!("invalid start: {}", s)}),
                    ),
                );
            }
        },
        None => now_ms - 3_600_000,
    };
    let end_ms = match params.end.as_deref() {
        Some(s) => match parse_timestamp_ms(s) {
            Some(ms) => ms,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(
                        json!({"status": "error", "errorType": "bad_data", "error": format!("invalid end: {}", s)}),
                    ),
                );
            }
        },
        None => now_ms,
    };
    let step_ms = match params.step.as_deref() {
        Some(s) => match crate::config::parse_duration(s).map(|d| d.as_millis() as i64) {
            Some(ms) => ms,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(
                        json!({"status": "error", "errorType": "bad_data", "error": format!("invalid step: {}", s)}),
                    ),
                );
            }
        },
        None => 60_000,
    };

    if step_ms <= 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                json!({"status": "error", "errorType": "bad_data", "error": "step must be positive"}),
            ),
        );
    }

    let num_steps = end_ms.saturating_sub(start_ms).max(0) / step_ms;
    if num_steps >= MAX_QUERY_STEPS {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                json!({"status": "error", "errorType": "bad_data", "error": format!("query would produce {} steps, exceeding maximum of {}", num_steps, MAX_QUERY_STEPS)}),
            ),
        );
    }

    let store = state.metric_store.read();
    match evaluate_range(&params.query, &store, start_ms, end_ms, step_ms) {
        Ok(result) => (StatusCode::OK, Json(format_range_result(result))),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(
                json!({"status": "error", "errorType": "bad_data", "error": e.to_string(), "hint": PROMQL_HINT}),
            ),
        ),
    }
}

/// GET /api/v1/series
pub async fn series(
    State(state): State<SharedState>,
    Query(params): Query<SeriesParams>,
) -> (StatusCode, Json<Value>) {
    let store = state.metric_store.read();

    // Parse optional time bounds
    let start_ms = params.start.as_deref().and_then(parse_timestamp_ms);
    let end_ms = params.end.as_deref().and_then(parse_timestamp_ms);

    // Helper: check if a series has any samples in the time window
    let has_samples_in_range = |id: u64| -> bool {
        if start_ms.is_none() && end_ms.is_none() {
            return true; // No time filter
        }
        let s = start_ms.unwrap_or(i64::MIN);
        let e = end_ms.unwrap_or(i64::MAX);
        !store.get_samples(id, s, e).is_empty()
    };

    let mut all_series = Vec::new();

    if params.matchers.is_empty() {
        // Return all series
        for id in store.series.keys() {
            if has_samples_in_range(*id) {
                if let Some(labels) = store.get_series_labels(*id) {
                    let map: serde_json::Map<String, Value> = labels
                        .into_iter()
                        .map(|(k, v)| (k, Value::String(v)))
                        .collect();
                    all_series.push(Value::Object(map));
                }
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
                        if seen_ids.insert(id)
                            && has_samples_in_range(id)
                            && let Some(labels) = store.get_series_labels(id)
                        {
                            let map: serde_json::Map<String, Value> = labels
                                .into_iter()
                                .map(|(k, v)| (k, Value::String(v)))
                                .collect();
                            all_series.push(Value::Object(map));
                        }
                    }
                }
                _ => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(
                            json!({"status": "error", "errorType": "bad_data", "error": format!("invalid match[] selector: {}", matchers_str)}),
                        ),
                    );
                }
            }
        }
    }

    (
        StatusCode::OK,
        Json(json!({
            "status": "success",
            "data": all_series,
        })),
    )
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
    // Try as integer first — classify by magnitude
    if let Ok(n) = s.parse::<i64>() {
        return Some(classify_to_ms(n));
    }
    // Try as float seconds (Prometheus convention: "1700000000.5")
    if let Ok(secs) = s.parse::<f64>() {
        return Some((secs * 1000.0) as i64);
    }
    None
}

/// Classify an integer timestamp to milliseconds based on its magnitude.
fn classify_to_ms(n: i64) -> i64 {
    if n > 1_000_000_000_000_000 {
        n / 1_000_000 // nanoseconds -> ms
    } else if n > 1_000_000_000_000 {
        n // already milliseconds
    } else {
        n.saturating_mul(1000) // seconds -> ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_query_steps_constant() {
        assert_eq!(MAX_QUERY_STEPS, 11_000);
    }

    #[test]
    fn test_step_count_within_limit() {
        // 3600000ms range / 1000ms step = 3600 steps, under the 11000 cap
        let start_ms: i64 = 1_700_000_000_000;
        let end_ms: i64 = start_ms + 3_600_000;
        let step_ms: i64 = 1_000;
        let num_steps = end_ms.saturating_sub(start_ms).max(0) / step_ms;
        assert_eq!(num_steps, 3600);
        assert!(num_steps < MAX_QUERY_STEPS);
    }

    #[test]
    fn test_step_count_exceeds_limit() {
        // 11001000ms range / 1ms step = 11_001_000 steps, way over the cap
        let start_ms: i64 = 1_700_000_000_000;
        let end_ms: i64 = start_ms + 11_001_000;
        let step_ms: i64 = 1;
        let num_steps = end_ms.saturating_sub(start_ms).max(0) / step_ms;
        assert!(num_steps >= MAX_QUERY_STEPS);
    }

    #[test]
    fn test_step_count_exactly_at_limit() {
        // Exactly 11000 steps should be rejected (off-by-one: eval loop is inclusive)
        let start_ms: i64 = 1_700_000_000_000;
        let step_ms: i64 = 1_000;
        let end_ms: i64 = start_ms + step_ms * MAX_QUERY_STEPS;
        let num_steps = end_ms.saturating_sub(start_ms).max(0) / step_ms;
        assert_eq!(num_steps, MAX_QUERY_STEPS);
        assert!(num_steps >= MAX_QUERY_STEPS); // rejected

        // 10999 steps should be allowed
        let end_ms_ok: i64 = start_ms + step_ms * (MAX_QUERY_STEPS - 1);
        let num_steps_ok = end_ms_ok.saturating_sub(start_ms).max(0) / step_ms;
        assert_eq!(num_steps_ok, MAX_QUERY_STEPS - 1);
        assert!(num_steps_ok < MAX_QUERY_STEPS); // allowed
    }

    #[test]
    fn test_step_count_one_over_limit() {
        // 11001 steps should be rejected
        let start_ms: i64 = 1_700_000_000_000;
        let step_ms: i64 = 1_000;
        let end_ms: i64 = start_ms + step_ms * (MAX_QUERY_STEPS + 1);
        let num_steps = end_ms.saturating_sub(start_ms).max(0) / step_ms;
        assert_eq!(num_steps, MAX_QUERY_STEPS + 1);
        assert!(num_steps >= MAX_QUERY_STEPS);
    }

    #[test]
    fn test_step_count_overflow_protection() {
        // Extreme timestamps: saturating_sub prevents overflow
        let start_ms: i64 = -1_000_000_000_000;
        let end_ms: i64 = i64::MAX;
        let step_ms: i64 = 1_000;
        // Without saturating_sub, this would overflow
        let num_steps = end_ms.saturating_sub(start_ms).max(0) / step_ms;
        assert!(num_steps >= MAX_QUERY_STEPS);
    }

    #[test]
    fn test_parse_timestamp_ms_seconds() {
        assert_eq!(parse_timestamp_ms("1700000000"), Some(1_700_000_000_000));
    }

    #[test]
    fn test_parse_timestamp_ms_milliseconds() {
        assert_eq!(parse_timestamp_ms("1700000000000"), Some(1_700_000_000_000));
    }

    #[test]
    fn test_parse_timestamp_ms_float_seconds() {
        assert_eq!(parse_timestamp_ms("1700000000.5"), Some(1_700_000_000_500));
    }
}
