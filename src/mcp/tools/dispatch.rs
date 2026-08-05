use serde_json::{Value, json};

use super::args::{
    CheckHealthArgs, DescribeServiceArgs, GetTraceArgs, ListServicesArgs, MarkCheckpointArgs,
    QueryLogsArgs, QueryMetricsArgs, QueryTracesArgs, ResetArgs, SummarizeActivityArgs, ToolArgs,
    parse,
};
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

/// Deserialize `args` into the handler's argument type, or answer with the
/// problem. Every handler goes through here, so no handler ever sees an argument
/// its type does not declare.
fn with_args<T, F>(id: Option<Value>, args: &Value, handle: F) -> Value
where
    T: ToolArgs,
    F: FnOnce(Option<Value>, &T) -> Value,
{
    match parse::<T>(args) {
        Ok(parsed) => handle(id, &parsed),
        Err(problem) => tool_err(id, problem),
    }
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
    // Each arm names the argument type that tool advertises; `with_args` is what
    // makes the advertised schema and the handler's view of the call the same
    // thing. Tools that take no arguments still parse, so an argument sent to one
    // is refused rather than ignored.
    match name {
        "reset" => with_args(id, &args, |id, a: &ResetArgs| handle_reset(state, id, a)),
        "mark_checkpoint" => with_args(id, &args, |id, _: &MarkCheckpointArgs| {
            handle_mark_checkpoint(state, id)
        }),
        "summarize_activity" => with_args(id, &args, |id, a: &SummarizeActivityArgs| {
            handle_summarize_activity(state, id, a)
        }),
        "check_health" => with_args(id, &args, |id, _: &CheckHealthArgs| {
            handle_check_health(state, id)
        }),
        "query_logs" => with_args(id, &args, |id, a: &QueryLogsArgs| {
            handle_query_logs(state, id, a)
        }),
        "query_traces" => with_args(id, &args, |id, a: &QueryTracesArgs| {
            handle_query_traces(state, id, a)
        }),
        "query_metrics" => with_args(id, &args, |id, a: &QueryMetricsArgs| {
            handle_query_metrics(state, id, a)
        }),
        "get_trace" => with_args(id, &args, |id, a: &GetTraceArgs| {
            handle_get_trace(state, id, a)
        }),
        "list_services" => with_args(id, &args, |id, _: &ListServicesArgs| {
            handle_list_services(state, id)
        }),
        "describe_service" => with_args(id, &args, |id, a: &DescribeServiceArgs| {
            handle_describe_service(state, id, a)
        }),
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

    /// `50.0` satisfies JSON Schema `integer` but not `as_u64`, so accepting it
    /// would put us right back to dropping the value in silence. Rejected on
    /// purpose — with a message that says how to write it instead.
    #[test]
    fn call_fractional_form_of_integer_is_rejected_with_a_correctable_message() {
        let st = tests_state();
        let resp = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "query_traces", "arguments": { "service": "api", "limit": 50.0 } }),
        );
        assert_eq!(resp["result"]["isError"], json!(true));
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("limit"), "message must name the key: {text}");
        assert!(
            text.contains("non-negative integer"),
            "message must state the real contract: {text}"
        );
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
