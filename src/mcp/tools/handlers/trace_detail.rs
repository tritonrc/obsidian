use serde_json::Value;

use crate::mcp::synth;
use crate::mcp::tools::args::{Detail, GetTraceArgs};
use crate::mcp::tools::dispatch::{tool_err, tool_ok};
use crate::store::SharedState;

pub(in crate::mcp::tools) fn handle_get_trace(
    state: &SharedState,
    id: Option<Value>,
    args: &GetTraceArgs,
) -> Value {
    let hex = args.trace_id.as_str();
    if hex.len() != 32 {
        return tool_err(id, "trace_id must be exactly 32 hex characters".into());
    }
    let mut bytes = [0u8; 16];
    for (i, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(chunk).unwrap_or("");
        match u8::from_str_radix(pair, 16) {
            Ok(b) => bytes[i] = b,
            Err(_) => return tool_err(id, "trace_id is not valid hex".into()),
        }
    }
    let detailed = matches!(args.detail, Some(Detail::Detailed));
    match synth::build_trace_tree(state, &bytes, detailed) {
        Some(tree) => {
            let text = format!(
                "trace {} with {} root span(s)",
                tree.trace_id,
                tree.roots.len()
            );
            match serde_json::to_value(&tree) {
                Ok(v) => tool_ok(id, v, text),
                Err(e) => tool_err(id, format!("serialization error: {e}")),
            }
        }
        None => tool_err(id, format!("trace {hex} not found")),
    }
}

#[cfg(test)]
mod tests {
    use crate::mcp::tools::call;
    use crate::mcp::tools::handlers::test_support::seed_error_trace;
    use crate::store::empty_test_state as tests_state;
    use serde_json::json;

    #[test]
    fn get_trace_unknown_id_is_error() {
        let st = tests_state();
        let bad = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "get_trace", "arguments": { "trace_id": "zz" } }),
        );
        assert_eq!(bad["result"]["isError"], json!(true));
    }

    #[test]
    fn get_trace_valid_but_missing_id_is_error() {
        let st = tests_state();
        // 32 valid hex chars that do not exist in the store.
        let bad = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "get_trace", "arguments": { "trace_id": "0123456789abcdef0123456789abcdef" } }),
        );
        assert_eq!(bad["result"]["isError"], json!(true));
    }

    #[test]
    fn get_trace_detail_controls_attributes() {
        let st = tests_state();
        let tid = seed_error_trace(&st);
        let hex: String = tid.iter().map(|b| format!("{b:02x}")).collect();
        let concise = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "get_trace", "arguments": { "trace_id": hex } }),
        );
        let roots = concise["result"]["structuredContent"]["roots"]
            .as_array()
            .unwrap();
        assert!(
            roots[0]["attributes"]
                .as_array()
                .map(|a| a.is_empty())
                .unwrap_or(true),
            "concise omits attributes"
        );
        let detailed = call(
            &st,
            Some(json!(1)),
            &json!({ "name": "get_trace", "arguments": { "trace_id": hex, "detail": "detailed" } }),
        );
        let droots = detailed["result"]["structuredContent"]["roots"]
            .as_array()
            .unwrap();
        let attrs = droots[0]["attributes"].as_array().unwrap();
        assert!(!attrs.is_empty(), "detailed includes attributes");
    }
}
