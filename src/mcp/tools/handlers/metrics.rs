use serde_json::{Value, json};

use crate::mcp::tools::args::QueryMetricsArgs;
use crate::mcp::tools::dispatch::{tool_err, tool_ok};
use crate::query::promql::eval::{PromQLResult, evaluate_instant, evaluate_range};
use crate::store::SharedState;

pub(in crate::mcp::tools) fn handle_query_metrics(
    state: &SharedState,
    id: Option<Value>,
    args: &QueryMetricsArgs,
) -> Value {
    let promql = args.promql.as_str();
    if promql.is_empty() {
        return tool_err(id, "`promql` must not be empty".into());
    }
    let start = args.start.as_deref();
    let end = args.end.as_deref();
    let step = args.step.as_deref();
    let store = state.metric_store.read();
    let eval_result = if start.is_some() || end.is_some() || step.is_some() {
        // Range query: all three are required together.
        let (Some(start), Some(end), Some(step)) = (start, end, step) else {
            return tool_err(
                id,
                "a range query requires all of `start`, `end`, `step` (omit all three for an instant query)".into(),
            );
        };
        let start_ms = match crate::query::promql::handlers::parse_timestamp_ms(start) {
            Some(t) => t,
            None => return tool_err(id, format!("invalid `start`: {start}")),
        };
        let end_ms = match crate::query::promql::handlers::parse_timestamp_ms(end) {
            Some(t) => t,
            None => return tool_err(id, format!("invalid `end`: {end}")),
        };
        let step_ms = match crate::config::parse_duration(step).map(|d| {
            let ms = d.as_millis();
            if ms > i64::MAX as u128 {
                i64::MAX
            } else {
                ms as i64
            }
        }) {
            Some(s) if s > 0 => s,
            _ => return tool_err(id, format!("invalid `step`: {step}")),
        };
        // Cap total evaluation steps (mirrors the HTTP range handler) so an agent
        // can't trigger an OOM/hang with a tiny step over a huge window.
        let num_steps = end_ms.saturating_sub(start_ms).max(0) / step_ms;
        if num_steps >= crate::query::promql::handlers::MAX_QUERY_STEPS {
            return tool_err(
                id,
                format!(
                    "range query would produce {num_steps} steps (max {}); increase `step` or narrow start/end",
                    crate::query::promql::handlers::MAX_QUERY_STEPS
                ),
            );
        }
        evaluate_range(promql, &store, start_ms, end_ms, step_ms)
    } else {
        evaluate_instant(promql, &store, crate::query::promql::handlers::now_ms())
    };
    let result = match eval_result {
        Ok(r) => r,
        Err(e) => {
            return tool_err(
                id,
                format!("PromQL error: {e}. Example: rate(http_requests_total[5m])"),
            );
        }
    };
    let series = match result {
        PromQLResult::InstantVector(s) | PromQLResult::RangeVector(s) => s,
        PromQLResult::Scalar(v) => {
            return tool_ok(id, json!({ "scalar": v }), format!("scalar {v}"));
        }
    };
    let out: Vec<Value> = series
        .iter()
        .map(|s| {
            json!({
                "labels": s.labels,
                "samples": s.samples.iter().map(|(t, v)| json!([t.to_string(), v])).collect::<Vec<_>>(),
            })
        })
        .collect();
    let text = format!("{} series", out.len());
    tool_ok(id, json!({ "series": out }), text)
}

#[cfg(test)]
mod tests {
    use crate::mcp::tools::call;
    use crate::store::empty_test_state as tests_state;
    use serde_json::json;

    #[test]
    fn query_metrics_bad_promql_is_error() {
        let st = tests_state();
        let bad = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "query_metrics", "arguments": { "promql": "rate(" } }),
        );
        assert_eq!(bad["result"]["isError"], json!(true));
    }

    #[test]
    fn query_metrics_range_step_count_is_capped() {
        let st = tests_state();
        // 1.7e9 seconds at 1s steps would be ~1.7 billion evaluations.
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "query_metrics", "arguments": {
                "promql": "up", "start": "0", "end": "1700000000", "step": "1s"
            } }),
        );
        assert_eq!(resp["result"]["isError"], json!(true));
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("step"), "should mention step/cap: {text}");
    }

    #[test]
    fn query_metrics_range_invalid_time_is_error() {
        let st = tests_state();
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "query_metrics", "arguments": {
                "promql": "1", "start": "not-a-time", "end": "100", "step": "15s"
            } }),
        );
        assert_eq!(resp["result"]["isError"], json!(true));
    }
}
