use serde_json::{Value, json};

use super::descriptors;
use super::handlers::{
    handle_check_health, handle_describe_service, handle_get_trace, handle_list_services,
    handle_mark_checkpoint, handle_query_logs, handle_query_metrics, handle_query_traces,
    handle_reset, handle_summarize_activity,
};
use crate::mcp::protocol;
use crate::store::SharedState;

/// Build a successful `tools/call` result carrying both text and structured content.
pub(super) fn tool_ok(id: Option<Value>, structured: Value, text: String) -> Value {
    protocol::success(
        id,
        json!({
            "content": [ { "type": "text", "text": text } ],
            "structuredContent": structured,
            "isError": false
        }),
    )
}

/// Build a `tools/call` execution-error result (model-visible, self-correcting).
pub(super) fn tool_err(id: Option<Value>, message: String) -> Value {
    protocol::success(
        id,
        json!({
            "content": [ { "type": "text", "text": message } ],
            "isError": true
        }),
    )
}

/// Dispatch a `tools/call` request to the named tool handler.
pub fn call(state: &SharedState, id: Option<Value>, params: &Value) -> Value {
    let name = match params.get("name").and_then(|v| v.as_str()) {
        Some(n) => n,
        None => return protocol::error(id, protocol::INVALID_PARAMS, "missing tool name"),
    };
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if let Some((unknown, accepted)) = descriptors::unknown_argument(name, &args) {
        let accepts = if accepted.is_empty() {
            format!("{name} takes no arguments")
        } else {
            format!("{name} accepts: {}", accepted.join(", "))
        };
        return tool_err(
            id,
            format!(
                "unknown argument `{unknown}` — {accepts}. Undeclared keys are not filters: \
                 they are never applied, so the result would have looked filtered when it \
                 was not. For span/resource attributes use a raw query string."
            ),
        );
    }
    match name {
        "reset" => handle_reset(state, id, &args),
        "mark_checkpoint" => handle_mark_checkpoint(state, id),
        "summarize_activity" => handle_summarize_activity(state, id, &args),
        "check_health" => handle_check_health(state, id),
        "query_logs" => handle_query_logs(state, id, &args),
        "query_traces" => handle_query_traces(state, id, &args),
        "query_metrics" => handle_query_metrics(state, id, &args),
        "get_trace" => handle_get_trace(state, id, &args),
        "list_services" => handle_list_services(state, id),
        "describe_service" => handle_describe_service(state, id, &args),
        _ => protocol::error(id, protocol::INVALID_PARAMS, "unknown tool"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::empty_test_state as tests_state;
    use serde_json::json;

    /// The trap this guards: a filter key the tool never reads used to be
    /// dropped silently, so `total_count` reported the *unfiltered* total and
    /// read exactly like a match.
    #[test]
    fn call_unknown_argument_key_is_tool_error() {
        let st = tests_state();
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({
                "name": "query_traces",
                "arguments": { "service": "api", "environment": "converge" }
            }),
        );
        assert_eq!(resp["result"]["isError"], json!(true));
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(
            text.contains("environment"),
            "message must name the key: {text}"
        );
        assert!(
            text.contains("traceql"),
            "message must list accepted keys: {text}"
        );
        assert!(
            resp["result"]["structuredContent"].is_null(),
            "a rejected call must not return counts"
        );
    }

    #[test]
    fn call_unknown_argument_key_on_no_arg_tool_is_tool_error() {
        let st = tests_state();
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "check_health", "arguments": { "service": "api" } }),
        );
        assert_eq!(resp["result"]["isError"], json!(true));
    }

    #[test]
    fn call_with_only_declared_arguments_is_accepted() {
        let st = tests_state();
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "query_traces", "arguments": { "service": "api", "limit": 5 } }),
        );
        assert_eq!(resp["result"]["isError"], json!(false));
        assert_eq!(resp["result"]["structuredContent"]["total_count"], json!(0));
    }

    #[test]
    fn call_with_absent_or_empty_arguments_is_accepted() {
        let st = tests_state();
        let resp = call(&st, Some(json!(1)), &json!({ "name": "check_health" }));
        assert_eq!(resp["result"]["isError"], json!(false));
    }

    #[test]
    fn call_unknown_tool_is_invalid_params() {
        let st = tests_state();
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "nope", "arguments": {} }),
        );
        assert_eq!(
            resp["error"]["code"],
            json!(crate::mcp::protocol::INVALID_PARAMS)
        );
    }
}
