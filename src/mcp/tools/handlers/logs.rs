use serde_json::{Value, json};

use super::common::escape_quoted;
use crate::mcp::tools::args::QueryLogsArgs;
use crate::mcp::tools::dispatch::{tool_err, tool_ok};
use crate::query::logql::eval::{LogQLResult, evaluate_logql_limited};
use crate::query::logql::parser::parse_logql;
use crate::store::SharedState;

pub(in crate::mcp::tools) fn handle_query_logs(
    state: &SharedState,
    id: Option<Value>,
    args: &QueryLogsArgs,
) -> Value {
    let limit = args.limit.unwrap_or(50).min(100) as usize;
    // Raw LogQL escape hatch wins over structured filters.
    let query = if let Some(raw) = &args.logql {
        raw.clone()
    } else {
        let mut sel = Vec::new();
        if let Some(s) = &args.service {
            sel.push(format!("service=\"{}\"", escape_quoted(s)));
        }
        if let Some(l) = &args.level {
            sel.push(format!("level=\"{}\"", escape_quoted(l)));
        }
        if sel.is_empty() {
            return tool_err(
                id,
                "provide at least `service` or `level` (optionally with `contains`), or a raw `logql`".into(),
            );
        }
        let mut q = format!("{{{}}}", sel.join(", "));
        if let Some(c) = &args.contains {
            q.push_str(&format!(" |= \"{}\"", escape_quoted(c)));
        }
        q
    };
    let expr = match parse_logql(&query) {
        Ok(e) => e,
        Err(e) => {
            return tool_err(
                id,
                format!("LogQL parse error: {e}. Example: {{service=\"api\"}} |= \"error\""),
            );
        }
    };
    let store = state.log_store.read();
    // Probe one past the limit so we can report `truncated` accurately.
    let result = evaluate_logql_limited(&expr, &store, i64::MIN, i64::MAX, None, Some(limit + 1));
    let streams = match result {
        LogQLResult::Streams(streams) => streams,
        LogQLResult::Matrix(_) => {
            return tool_err(
                id,
                "LogQL metric queries (count_over_time, rate, etc.) are not supported here; \
                 use query_metrics with a PromQL expression instead."
                    .into(),
            );
        }
    };
    let mut out: Vec<Value> = Vec::new();
    for s in streams {
        for (ts, line, trace_id, span_id, attrs) in s.entries {
            out.push(json!({
                "ts": (ts / 1_000_000).to_string(),
                "line": line,
                "labels": s.labels,
                "trace_id": trace_id,
                "span_id": span_id,
                "attributes": attrs.into_iter()
                    .map(|(k, v)| (k, serde_json::Value::String(v)))
                    .collect::<serde_json::Map<String, Value>>(),
            }));
        }
    }
    let truncated = out.len() > limit;
    // Exact total is only needed (and only worth a full scan) when truncated.
    let total_count = if truncated {
        match evaluate_logql_limited(&expr, &store, i64::MIN, i64::MAX, None, None) {
            LogQLResult::Streams(all) => all.iter().map(|s| s.entries.len()).sum(),
            LogQLResult::Matrix(_) => out.len(),
        }
    } else {
        out.len()
    };
    out.truncate(limit);
    let shown = out.len();
    let text = if truncated {
        format!(
            "showing {shown} of {total_count} log line(s) — narrow `contains`/`level` or raise `limit`"
        )
    } else {
        format!("{shown} log line(s)")
    };
    tool_ok(
        id,
        json!({ "logs": out, "shown": shown, "total_count": total_count, "truncated": truncated }),
        text,
    )
}

#[cfg(test)]
mod tests {
    use crate::mcp::tools::call;
    use crate::store::empty_test_state as tests_state;
    use serde_json::json;
    use smallvec::SmallVec;

    #[test]
    fn query_logs_structured_and_bad_logql() {
        let st = tests_state();
        {
            let mut logs = st.log_store.write();
            logs.ingest_stream(
                vec![("service".into(), "api".into())],
                vec![crate::store::log_store::LogEntry {
                    timestamp_ns: 5,
                    line: "hello".into(),
                    ingest_seq: 0,
                    trace_id: None,
                    span_id: None,
                    severity_number: 0,
                    severity_text: None,
                    attributes: SmallVec::new(),
                }],
            );
        }
        let ok = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "query_logs", "arguments": { "service": "api" } }),
        );
        assert_eq!(ok["result"]["isError"], json!(false));
        assert!(ok["result"]["structuredContent"]["logs"].is_array());

        let bad = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "query_logs", "arguments": { "logql": "{unterminated" } }),
        );
        assert_eq!(bad["result"]["isError"], json!(true));
    }

    #[test]
    fn query_logs_reports_truncated_when_over_limit() {
        let st = tests_state();
        {
            let mut logs = st.log_store.write();
            let entries: Vec<crate::store::log_store::LogEntry> = (1..=60)
                .map(|i| crate::store::log_store::LogEntry {
                    timestamp_ns: i,
                    line: format!("line {i}"),
                    ingest_seq: 0,
                    trace_id: None,
                    span_id: None,
                    severity_number: 0,
                    severity_text: None,
                    attributes: SmallVec::new(),
                })
                .collect();
            logs.ingest_stream(vec![("service".into(), "api".into())], entries);
        }
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "query_logs", "arguments": { "service": "api", "limit": 50 } }),
        );
        let sc = &resp["result"]["structuredContent"];
        assert_eq!(sc["shown"], json!(50));
        assert_eq!(sc["truncated"], json!(true));
    }

    #[test]
    fn query_logs_rejects_metric_logql() {
        let st = tests_state();
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "query_logs", "arguments": { "logql": "count_over_time({service=\"api\"}[5m])" } }),
        );
        assert_eq!(resp["result"]["isError"], json!(true));
    }

    #[test]
    fn query_logs_contains_only_is_error() {
        let st = tests_state();
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "query_logs", "arguments": { "contains": "boom" } }),
        );
        assert_eq!(resp["result"]["isError"], json!(true));
    }

    #[test]
    fn query_logs_reports_total_count_and_hint() {
        let st = tests_state();
        seed_api_logs(&st, 60);
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "query_logs", "arguments": { "service": "api", "limit": 50 } }),
        );
        let sc = &resp["result"]["structuredContent"];
        assert_eq!(sc["shown"], json!(50));
        assert_eq!(sc["total_count"], json!(60));
        assert_eq!(sc["truncated"], json!(true));
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("60"), "hint should mention total: {text}");
    }

    fn seed_api_logs(st: &crate::store::SharedState, n: i64) {
        let mut logs = st.log_store.write();
        let entries: Vec<crate::store::log_store::LogEntry> = (1..=n)
            .map(|i| crate::store::log_store::LogEntry {
                timestamp_ns: i,
                line: format!("line {i}"),
                ingest_seq: 0,
                trace_id: None,
                span_id: None,
                severity_number: 0,
                severity_text: None,
                attributes: SmallVec::new(),
            })
            .collect();
        logs.ingest_stream(vec![("service".into(), "api".into())], entries);
    }
}
