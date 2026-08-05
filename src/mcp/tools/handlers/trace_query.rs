use serde_json::{Value, json};

use super::common::escape_quoted;
use crate::mcp::synth;
use crate::mcp::tools::args::QueryTracesArgs;
use crate::mcp::tools::dispatch::{tool_err, tool_ok};
use crate::query::traceql::eval::evaluate_traceql;
use crate::query::traceql::parser::parse_traceql;
use crate::store::SharedState;

pub(in crate::mcp::tools) fn handle_query_traces(
    state: &SharedState,
    id: Option<Value>,
    args: &QueryTracesArgs,
) -> Value {
    let limit = args.limit.unwrap_or(50).min(100) as usize;
    let query = if let Some(raw) = &args.traceql {
        raw.clone()
    } else {
        let mut conds = Vec::new();
        if let Some(s) = &args.service {
            conds.push(format!("resource.service.name = \"{}\"", escape_quoted(s)));
        }
        // The argument type is the enum, so there is no out-of-range case left
        // for this handler to catch.
        if let Some(status) = args.status {
            conds.push(format!("status = {status}"));
        }
        if let Some(d) = &args.min_duration {
            if crate::config::parse_duration(d).is_none() {
                return tool_err(
                    id,
                    format!("invalid `min_duration`: \"{d}\". Example: 100ms, 1s, 500us"),
                );
            }
            conds.push(format!("duration > {d}"));
        }
        if let Some(n) = &args.name {
            conds.push(format!("name = \"{}\"", escape_quoted(n)));
        }
        if conds.is_empty() {
            return tool_err(
                id,
                "provide service/name/status/min_duration, or a raw `traceql`".into(),
            );
        }
        format!("{{ {} }}", conds.join(" && "))
    };
    let expr = match parse_traceql(&query) {
        Ok(e) => e,
        Err(e) => {
            return tool_err(
                id,
                format!(
                    "TraceQL parse error: {e}. Example: {{ resource.service.name = \"api\" && status = error }}"
                ),
            );
        }
    };
    // Collect matched trace IDs under the eval lock, then summarize the page
    // under a single read lock via synth::trace_items (one acquisition instead
    // of N). A concurrent reset between these two phases is possible but
    // benign: affected IDs return None and are skipped.
    let trace_ids: Vec<[u8; 16]> = {
        let store = state.trace_store.read();
        evaluate_traceql(&expr, &store)
            .into_iter()
            .map(|r| r.trace_id)
            .collect()
    };
    let total = trace_ids.len();
    let truncated = total > limit;
    let page: &[[u8; 16]] = &trace_ids[..total.min(limit)];
    let out: Vec<Value> = synth::trace_items(state, page)
        .into_iter()
        .filter_map(|t| serde_json::to_value(&t).ok())
        .collect();
    let shown = out.len();
    let text = if truncated {
        format!("showing {shown} of {total} trace(s) — narrow filters or raise `limit`")
    } else {
        format!("{shown} trace(s)")
    };
    tool_ok(
        id,
        json!({ "traces": out, "shown": shown, "total_count": total, "truncated": truncated }),
        text,
    )
}

#[cfg(test)]
mod tests {
    use crate::mcp::tools::call;
    use crate::mcp::tools::handlers::test_support::seed_error_trace;
    use crate::store::empty_test_state as tests_state;
    use serde_json::json;

    #[test]
    fn query_traces_bad_traceql_is_error() {
        let st = tests_state();
        let bad = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "query_traces", "arguments": { "traceql": "{{{" } }),
        );
        assert_eq!(bad["result"]["isError"], json!(true));
    }

    #[test]
    fn query_traces_invalid_status_is_error() {
        let st = tests_state();
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "query_traces", "arguments": { "service": "api", "status": "boom" } }),
        );
        assert_eq!(resp["result"]["isError"], json!(true));
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("status"), "should name the bad field: {text}");
    }

    #[test]
    fn query_traces_invalid_min_duration_is_error() {
        let st = tests_state();
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "query_traces", "arguments": { "service": "api", "min_duration": "soon" } }),
        );
        assert_eq!(resp["result"]["isError"], json!(true));
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(
            text.contains("min_duration"),
            "should name the bad field: {text}"
        );
    }

    #[test]
    fn query_traces_returns_trace_level_summary() {
        let st = tests_state();
        seed_error_trace(&st);
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "query_traces", "arguments": { "service": "api" } }),
        );
        assert_eq!(resp["result"]["isError"], json!(false));
        let traces = resp["result"]["structuredContent"]["traces"]
            .as_array()
            .unwrap();
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0]["root_span_name"], json!("GET /api"));
        assert_eq!(traces[0]["error_span_count"], json!(1));
        assert!(traces[0]["trace_id"].is_string());
    }
}
