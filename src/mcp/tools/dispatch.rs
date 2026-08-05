use serde_json::{Value, json};

use super::args::{ToolArgs, parse};
use super::registry;
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
pub(super) fn with_args<T, F>(id: Option<Value>, args: &Value, handle: F) -> Value
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
    // `arguments` is an optional object per the request schema, so a literal
    // `null` is strictly malformed. It is accepted as "no arguments" anyway — a
    // deliberate deviation, because it is unambiguous and a client that emits
    // `null` for an absent optional field could otherwise never call a
    // no-argument tool. Any other non-object shape is malformed request
    // structure: a protocol error, not a tool input the model can correct.
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
    // The table pairs each name with the argument type it advertises and the
    // handler it runs, so there is no name-to-type choice left to make here.
    // `id` is cloned because the table consumes it on the hit path; a request id
    // is a number or a short string, and this happens once per call.
    match registry::dispatch(state, id.clone(), name, &args) {
        Some(response) => response,
        // A name with no table entry: request structure the model cannot fix.
        None => protocol::error(id, protocol::INVALID_PARAMS, "unknown tool"),
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

    /// Same trap as an undeclared key. Handlers used to read values out of a
    /// `&Value`, so a wrong-typed one read as absent and its filter was never
    /// applied; now it cannot deserialize, and the caller is told which key.
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

    /// A checkpoint token is a non-negative counter. `-5` used to fail the read
    /// and silently widen the summary to all time.
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

    /// `50.0` satisfies JSON Schema `integer` but is not a `u64`, so accepting it
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

    /// `detail` used to be compared against the string "detailed", so any other
    /// value silently degraded to concise output instead of being questioned. It
    /// is an enum now, and the validator answers before a handler sees it.
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

    /// Lenient by choice, and a deviation from the request schema: `null` is not
    /// an object, but it is an unambiguous "no arguments", so it is treated as
    /// `{}` rather than refused.
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
