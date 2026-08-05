use serde_json::{Value, json};

use super::common::require_known_service;
use crate::mcp::synth;
use crate::mcp::tools::args::{
    DescribeServiceArgs, Detail, ResetArgs, ResetScope, SummarizeActivityArgs,
};
use crate::mcp::tools::dispatch::{tool_err, tool_ok};
use crate::store::SharedState;

pub(in crate::mcp::tools) fn handle_reset(
    state: &SharedState,
    id: Option<Value>,
    args: &ResetArgs,
) -> Value {
    // Each arm clears its scope and returns the scope-specific structured fields.
    let mut structured = match args.scope {
        ResetScope::All => {
            state.log_store.write().clear();
            state.metric_store.write().clear();
            state.trace_store.write().clear();
            json!({ "scope": args.scope.as_str() })
        }
        ResetScope::Service => {
            // `service` is only required by this scope, so the schema cannot
            // demand it — the check belongs here.
            let service = match args.service.as_deref() {
                Some(s) if !s.is_empty() => s,
                _ => return tool_err(id, "scope='service' requires a non-empty `service`".into()),
            };
            state.log_store.write().clear_service(service);
            state.metric_store.write().clear_service(service);
            state.trace_store.write().clear_service(service);
            json!({ "scope": args.scope.as_str(), "service": service })
        }
    };
    let checkpoint = state.ingest_seq.load(std::sync::atomic::Ordering::Relaxed);
    structured["checkpoint"] = json!(checkpoint);
    tool_ok(
        id,
        structured,
        format!("reset complete; checkpoint={checkpoint}"),
    )
}

pub(in crate::mcp::tools) fn handle_mark_checkpoint(
    state: &SharedState,
    id: Option<Value>,
) -> Value {
    let checkpoint = state.ingest_seq.load(std::sync::atomic::Ordering::Relaxed);
    tool_ok(
        id,
        json!({ "checkpoint": checkpoint }),
        format!("checkpoint={checkpoint} — pass as `since` to scope a later summary"),
    )
}

pub(in crate::mcp::tools) fn handle_summarize_activity(
    state: &SharedState,
    id: Option<Value>,
    args: &SummarizeActivityArgs,
) -> Value {
    if let Err(e) = require_known_service(state, &id, &args.service) {
        return e;
    }
    let detail = matches!(args.detail, Some(Detail::Detailed));
    let activity = synth::summarize_activity(state, &args.service, args.since, detail);
    let text = activity.summary.clone();
    match serde_json::to_value(&activity) {
        Ok(v) => tool_ok(id, v, text),
        Err(e) => tool_err(id, format!("serialization error: {e}")),
    }
}

pub(in crate::mcp::tools) fn handle_check_health(state: &SharedState, id: Option<Value>) -> Value {
    let overview = synth::check_health(state);
    let text = match overview.services.first() {
        Some(worst) => format!(
            "{} service(s) ranked worst-first; worst: {} (health {:.0}, {})",
            overview.services.len(),
            worst.service,
            worst.health_score,
            worst.top_issue
        ),
        None => "no services reporting telemetry yet".to_string(),
    };
    match serde_json::to_value(&overview) {
        Ok(v) => tool_ok(id, v, text),
        Err(e) => tool_err(id, format!("serialization error: {e}")),
    }
}

pub(in crate::mcp::tools) fn handle_describe_service(
    state: &SharedState,
    id: Option<Value>,
    args: &DescribeServiceArgs,
) -> Value {
    if let Err(e) = require_known_service(state, &id, &args.service) {
        return e;
    }
    let cat = synth::describe_service(state, &args.service);
    let text = format!(
        "{} metric(s), {} log label key(s), {} span attr key(s)",
        cat.metrics.len(),
        cat.log_labels.len(),
        cat.span_attributes.len()
    );
    match serde_json::to_value(&cat) {
        Ok(v) => tool_ok(id, v, text),
        Err(e) => tool_err(id, format!("serialization error: {e}")),
    }
}

pub(in crate::mcp::tools) fn handle_list_services(state: &SharedState, id: Option<Value>) -> Value {
    use rustc_hash::{FxHashMap, FxHashSet};

    let mut sig: FxHashMap<String, FxHashSet<&str>> = FxHashMap::default();
    for n in state.log_store.read().get_label_values("service") {
        sig.entry(n).or_default().insert("logs");
    }
    for n in state.metric_store.read().get_label_values("service") {
        sig.entry(n).or_default().insert("metrics");
    }
    for n in state.trace_store.read().service_names() {
        sig.entry(n).or_default().insert("traces");
    }
    let mut services: Vec<Value> = sig
        .into_iter()
        .map(|(name, s)| {
            let mut sigs: Vec<&str> = s.into_iter().collect();
            sigs.sort_unstable();
            json!({ "name": name, "signals": sigs })
        })
        .collect();
    services.sort_by(|a, b| {
        a["name"]
            .as_str()
            .unwrap_or("")
            .cmp(b["name"].as_str().unwrap_or(""))
    });
    let text = format!("{} service(s)", services.len());
    tool_ok(id, json!({ "services": services }), text)
}

#[cfg(test)]
mod tests {
    use crate::mcp::tools::call;
    use crate::store::empty_test_state as tests_state;
    use serde_json::json;
    use smallvec::SmallVec;

    #[test]
    fn reset_all_clears_and_returns_checkpoint() {
        let st = tests_state();
        {
            let mut logs = st.log_store.write();
            logs.ingest_stream(
                vec![("service".into(), "api".into())],
                vec![crate::store::log_store::LogEntry {
                    timestamp_ns: 1,
                    line: "x".into(),
                    ingest_seq: 0,
                    trace_id: None,
                    span_id: None,
                    severity_number: 0,
                    severity_text: None,
                    attributes: SmallVec::new(),
                }],
            );
        }
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "reset", "arguments": { "scope": "all" } }),
        );
        assert_eq!(resp["result"]["isError"], json!(false));
        assert!(resp["result"]["structuredContent"]["checkpoint"].is_u64());
        assert_eq!(st.log_store.read().streams.len(), 0);
    }

    #[test]
    fn reset_service_scope_requires_service() {
        let st = tests_state();
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "reset", "arguments": { "scope": "service" } }),
        );
        assert_eq!(resp["result"]["isError"], json!(true));
    }

    #[test]
    fn mark_checkpoint_returns_current_seq() {
        let st = tests_state();
        st.ingest_seq.store(7, std::sync::atomic::Ordering::Relaxed);
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "mark_checkpoint", "arguments": {} }),
        );
        assert_eq!(resp["result"]["structuredContent"]["checkpoint"], json!(7));
    }

    #[test]
    fn summarize_activity_tool_reports_errors() {
        let st = tests_state();
        {
            let mut logs = st.log_store.write();
            logs.ingest_stream(
                vec![
                    ("service".into(), "api".into()),
                    ("level".into(), "error".into()),
                ],
                vec![crate::store::log_store::LogEntry {
                    timestamp_ns: 1,
                    line: "boom".into(),
                    ingest_seq: 0,
                    trace_id: None,
                    span_id: None,
                    severity_number: 0,
                    severity_text: None,
                    attributes: SmallVec::new(),
                }],
            );
        }
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "summarize_activity", "arguments": { "service": "api" } }),
        );
        assert_eq!(
            resp["result"]["structuredContent"]["logs"]["error_count"],
            json!(1)
        );
    }

    #[test]
    fn summarize_activity_requires_service() {
        let st = tests_state();
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "summarize_activity", "arguments": {} }),
        );
        assert_eq!(resp["result"]["isError"], json!(true));
    }

    #[test]
    fn check_health_tool_returns_services_array() {
        let st = tests_state();
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "check_health", "arguments": {} }),
        );
        assert!(resp["result"]["structuredContent"]["services"].is_array());
    }

    #[test]
    fn list_services_tool_includes_signals() {
        let st = tests_state();
        {
            let mut logs = st.log_store.write();
            logs.ingest_stream(
                vec![("service".into(), "api".into())],
                vec![crate::store::log_store::LogEntry {
                    timestamp_ns: 1,
                    line: "x".into(),
                    ingest_seq: 0,
                    trace_id: None,
                    span_id: None,
                    severity_number: 0,
                    severity_text: None,
                    attributes: SmallVec::new(),
                }],
            );
        }
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "list_services", "arguments": {} }),
        );
        let services = resp["result"]["structuredContent"]["services"]
            .as_array()
            .unwrap();
        assert!(
            services.iter().any(|s| s["name"] == "api"
                && s["signals"].as_array().unwrap().iter().any(|x| x == "logs"))
        );
    }

    #[test]
    fn describe_service_tool_requires_service_and_returns_catalog() {
        let st = tests_state();
        let missing = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "describe_service", "arguments": {} }),
        );
        assert_eq!(missing["result"]["isError"], json!(true));
        {
            let mut logs = st.log_store.write();
            logs.ingest_stream(
                vec![
                    ("service".into(), "api".into()),
                    ("level".into(), "error".into()),
                ],
                vec![crate::store::log_store::LogEntry {
                    timestamp_ns: 1,
                    line: "x".into(),
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
            &json!({ "name": "describe_service", "arguments": { "service": "api" } }),
        );
        assert!(ok["result"]["structuredContent"]["log_labels"].is_array());
    }

    #[test]
    fn summarize_unknown_service_is_error_listing_valid() {
        let st = tests_state();
        seed_api_logs(&st, 1);
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "summarize_activity", "arguments": { "service": "ghost" } }),
        );
        assert_eq!(resp["result"]["isError"], json!(true));
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("ghost"), "echo bad value: {text}");
        assert!(text.contains("api"), "list valid services: {text}");
    }

    #[test]
    fn describe_unknown_service_is_error() {
        let st = tests_state();
        seed_api_logs(&st, 1);
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "describe_service", "arguments": { "service": "ghost" } }),
        );
        assert_eq!(resp["result"]["isError"], json!(true));
    }

    #[test]
    fn reset_service_scope_echoes_service() {
        let st = tests_state();
        seed_api_logs(&st, 1);
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "reset", "arguments": { "scope": "service", "service": "api" } }),
        );
        assert_eq!(resp["result"]["isError"], json!(false));
        assert_eq!(resp["result"]["structuredContent"]["service"], json!("api"));
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
