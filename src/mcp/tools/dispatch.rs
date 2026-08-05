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
    // `arguments` is an object per the request schema. Absent and `null` both
    // mean "no arguments" (clients send either); any other shape is malformed
    // request structure, which is a protocol error, not a tool input the model
    // can correct.
    let args = match params.get("arguments") {
        None | Some(Value::Null) => json!({}),
        Some(v) if v.is_object() => v.clone(),
        Some(_) => {
            return protocol::error(
                id,
                protocol::INVALID_PARAMS,
                "`arguments` must be an object",
            );
        }
    };
    if let Some(problem) = descriptors::argument_problem(name, &args) {
        return tool_err(id, problem);
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

    /// Same trap as an undeclared key: handlers read `as_str()`/`as_u64()`, so a
    /// wrong-typed value reads as absent and its filter is never applied.
    #[test]
    fn call_wrong_typed_argument_is_tool_error() {
        let st = tests_state();
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({
                "name": "query_traces",
                "arguments": { "service": 123, "status": "error" }
            }),
        );
        assert_eq!(resp["result"]["isError"], json!(true));
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(
            text.contains("service"),
            "message must name the key: {text}"
        );
        assert!(
            text.contains("string"),
            "message must name the type: {text}"
        );
    }

    /// A checkpoint token is a non-negative counter; `-5` failed `as_u64()` and
    /// silently widened the summary to all time.
    #[test]
    fn call_negative_integer_argument_is_tool_error() {
        let st = tests_state();
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "summarize_activity", "arguments": { "service": "api", "since": -5 } }),
        );
        assert_eq!(resp["result"]["isError"], json!(true));
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("since"), "message must name the key: {text}");
    }

    /// `detail` is compared against "detailed", so any other string silently
    /// degraded to concise output instead of being questioned.
    #[test]
    fn call_out_of_enum_argument_is_tool_error() {
        let st = tests_state();
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({
                "name": "get_trace",
                "arguments": { "trace_id": "0".repeat(32), "detail": "verbose" }
            }),
        );
        assert_eq!(resp["result"]["isError"], json!(true));
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("detail"), "message must name the key: {text}");
        assert!(
            text.contains("detailed"),
            "message must list valid values: {text}"
        );
    }

    /// `CallToolRequest.arguments` is an object; a list or scalar is a malformed
    /// request, not a self-correctable tool input.
    #[test]
    fn call_non_object_arguments_is_invalid_params() {
        let st = tests_state();
        for bad in [json!([]), json!("service=api"), json!(7)] {
            let resp = call(
                &st,
                Some(json!(1)),
                &json!({ "name": "check_health", "arguments": bad }),
            );
            assert_eq!(
                resp["error"]["code"],
                json!(crate::mcp::protocol::INVALID_PARAMS)
            );
        }
    }

    /// Lenient by choice: clients that send `null` for "no arguments" are common
    /// and unambiguous, so it is treated as `{}` rather than rejected.
    #[test]
    fn call_null_arguments_is_treated_as_empty() {
        let st = tests_state();
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "check_health", "arguments": null }),
        );
        assert_eq!(resp["result"]["isError"], json!(false));
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
