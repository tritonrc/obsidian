//! The tool table: every tool named exactly once, bound to its argument type,
//! its handler, and the metadata `tools/list` advertises.
//!
//! Keeping all four in one entry is what makes the advertised contract and the
//! executed call the same thing. When the name was written once in a descriptor
//! and again in a dispatch match, a tool could advertise another tool's
//! `inputSchema` and still deserialize its own — the client would be told a key
//! was valid and then told it was unknown. There is now no second place to
//! disagree with.

use serde_json::{Value, json};

use super::args::{
    CheckHealthArgs, DescribeServiceArgs, GetTraceArgs, ListServicesArgs, MarkCheckpointArgs,
    QueryLogsArgs, QueryMetricsArgs, QueryTracesArgs, ResetArgs, SummarizeActivityArgs, ToolArgs,
};
use super::dispatch::with_args;
use super::handlers::{
    handle_check_health, handle_describe_service, handle_get_trace, handle_list_services,
    handle_mark_checkpoint, handle_query_logs, handle_query_metrics, handle_query_traces,
    handle_reset, handle_summarize_activity,
};
use crate::mcp::protocol;
use crate::store::SharedState;

/// Declare the tool table: `"name"(ArgsType) => handler { ..advertised metadata }`.
///
/// The name is a literal so it can serve as both the advertised `name` and the
/// dispatch pattern, which is the whole point — one occurrence per tool. The
/// `inputSchema` is generated from the argument type that same entry dispatches
/// through, so the two cannot describe different things.
macro_rules! tools {
    (
        $( $name:literal ( $args:ty ) => $handler:path { $($meta:tt)* } ),* $(,)?
    ) => {
        /// Every tool descriptor, for `tools/list`.
        fn descriptors() -> Vec<Value> {
            vec![
                $( json!({
                    "name": $name,
                    "inputSchema": <$args as ToolArgs>::schema(),
                    $($meta)*
                }) ),*
            ]
        }

        /// Run the named tool, or `None` if no such tool exists.
        pub(super) fn dispatch(
            state: &SharedState,
            id: Option<Value>,
            name: &str,
            args: &Value,
        ) -> Option<Value> {
            match name {
                $(
                    $name => Some(with_args(id, args, |id, a: &$args| {
                        $handler(state, id, a)
                    })),
                )*
                _ => None,
            }
        }
    };
}

/// Server-level instructions injected at `initialize` (the agent loop).
pub const INSTRUCTIONS: &str = "\
Aniani is your local observability instrument for the dev loop. Recommended flow:
1. reset(scope=all) for a clean baseline before a run.
2. Run your code/tests. Telemetry export may lag a moment — if a summary looks empty, wait briefly and retry.
3. summarize_activity(service) to see what the run produced: error logs, failing/slow traces, error metrics, health score.
4. Drill in: describe_service(service) to learn queryable labels/metrics, then query_logs / query_traces / query_metrics / get_trace.
5. To compare iterations without wiping, call mark_checkpoint() before a run and pass the returned token as `since` to summarize_activity.
Use check_health() when you don't yet know which service is in trouble, and list_services() to see what is reporting. \
summarize_activity/describe_service return an error listing known services if you name one that has produced no telemetry.";

/// One read-only tool annotation block (closed in-memory domain).
fn read_only(title: &str) -> Value {
    json!({ "title": title, "readOnlyHint": true, "openWorldHint": false, "idempotentHint": true })
}

tools! {
    "reset"(ResetArgs) => handle_reset {
            "title": "Reset Telemetry Store",
            "description": "Clear telemetry. scope='all' wipes everything; scope='service' clears one service (requires `service`). The only write tool. Returns a fresh checkpoint token.",
            "outputSchema": {
                "type": "object",
                "properties": {
                    "scope": { "type": "string" },
                    "service": { "type": "string" },
                    "checkpoint": { "type": "integer" }
                },
                "required": ["scope", "checkpoint"]
            },
            "annotations": { "title": "Reset Telemetry Store", "readOnlyHint": false, "destructiveHint": true, "idempotentHint": true, "openWorldHint": false }
    },
    "mark_checkpoint"(MarkCheckpointArgs) => handle_mark_checkpoint {
            "title": "Mark Checkpoint",
            "description": "Return an opaque monotonic checkpoint token for 'now'. Pass it later as `since` to scope a summary to telemetry ingested after this point. Not a wall-clock time.",
            "outputSchema": {
                "type": "object",
                "properties": { "checkpoint": { "type": "integer" } },
                "required": ["checkpoint"]
            },
            "annotations": read_only("Mark Checkpoint")
    },
    "summarize_activity"(SummarizeActivityArgs) => handle_summarize_activity {
            "title": "Summarize Service Activity",
            "description": "Triage one service: error logs, failing/slow traces, error metrics, health score, one-line summary. Use after a run to see what it produced. Pass `since` (a checkpoint token) to scope to the latest run. Use this BEFORE drilling in with query_* tools.",
            "outputSchema": {
                "type": "object",
                "properties": {
                    "service": { "type": "string" },
                    "since": { "type": ["integer", "null"] },
                    "observed_through": { "type": "integer" },
                    "health_score": { "type": "number" },
                    "summary": { "type": "string" },
                    "logs": { "type": "object" },
                    "traces": { "type": "object" },
                    "metrics": { "type": "object" },
                    "truncated": { "type": "object" }
                },
                "required": ["service", "health_score", "summary"]
            },
            "annotations": read_only("Summarize Service Activity")
    },
    "check_health"(CheckHealthArgs) => handle_check_health {
            "title": "Check Global Health",
            "description": "Rank every known service by health, worst first. Use when you don't yet know which service is in trouble.",
            "outputSchema": {
                "type": "object",
                "properties": { "services": { "type": "array", "items": { "type": "object" } } },
                "required": ["services"]
            },
            "annotations": read_only("Check Global Health")
    },
    "query_logs"(QueryLogsArgs) => handle_query_logs {
            "title": "Query Logs",
            "description": "Fetch log lines. Provide structured filters (service, level, contains) OR a raw `logql` string (e.g. {service=\"api\"} |= \"error\"). If `logql` is set, structured filters are ignored. To scope to a run, reset first, then query; summarize_activity is the checkpoint-accurate run summary.",
            "outputSchema": {
                "type": "object",
                "properties": {
                    "logs": { "type": "array", "items": { "type": "object" } },
                    "shown": { "type": "integer" },
                    "total_count": { "type": "integer" },
                    "truncated": { "type": "boolean" }
                },
                "required": ["logs", "shown", "total_count", "truncated"]
            },
            "annotations": read_only("Query Logs")
    },
    "query_traces"(QueryTracesArgs) => handle_query_traces {
            "title": "Query Traces",
            "description": "Find traces. Provide structured filters (service, name, status, min_duration) OR a raw `traceql` string. If `traceql` is set, structured filters are ignored.",
            "outputSchema": {
                "type": "object",
                "properties": {
                    "traces": { "type": "array", "items": { "type": "object" } },
                    "shown": { "type": "integer" },
                    "total_count": { "type": "integer" },
                    "truncated": { "type": "boolean" }
                },
                "required": ["traces", "shown", "total_count", "truncated"]
            },
            "annotations": read_only("Query Traces")
    },
    "query_metrics"(QueryMetricsArgs) => handle_query_metrics {
            "title": "Query Metrics",
            "description": "Run a raw PromQL query (e.g. rate(http_requests_total[5m])). For a range query pass all of start/end (unix seconds, or ms/ns) and step (a duration like 15s or 1m); omit all three for an instant query at now.",
            "outputSchema": {
                "type": "object",
                "properties": {
                    "series": { "type": "array", "items": { "type": "object" } },
                    "scalar": { "type": "number" }
                }
            },
            "annotations": read_only("Query Metrics")
    },
    "get_trace"(GetTraceArgs) => handle_get_trace {
            "title": "Get Trace Tree",
            "description": "Return one trace as a parent/child span tree. `trace_id` is 32 hex chars. Use `detail=detailed` to include span attributes.",
            "outputSchema": {
                "type": "object",
                "properties": {
                    "trace_id": { "type": "string" },
                    "roots": { "type": "array", "items": { "type": "object" } }
                },
                "required": ["trace_id", "roots"]
            },
            "annotations": read_only("Get Trace Tree")
    },
    "list_services"(ListServicesArgs) => handle_list_services {
            "title": "List Services",
            "description": "List every service reporting telemetry and which signals (logs/metrics/traces) each has.",
            "outputSchema": {
                "type": "object",
                "properties": { "services": { "type": "array", "items": { "type": "object" } } },
                "required": ["services"]
            },
            "annotations": read_only("List Services")
    },
    "describe_service"(DescribeServiceArgs) => handle_describe_service {
            "title": "Describe Service",
            "description": "Catalog a service's queryable surface: metric names, log label keys + values, span attribute keys. Call this before writing a raw logql/promql/traceql query.",
            "outputSchema": {
                "type": "object",
                "properties": {
                    "service": { "type": "string" },
                    "metrics": { "type": "array" },
                    "log_labels": { "type": "array" },
                    "span_attributes": { "type": "array" }
                },
                "required": ["service"]
            },
            "annotations": read_only("Describe Service")
    },
}

pub fn list(id: Option<Value>) -> Value {
    protocol::success(id, json!({ "tools": descriptors() }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Every advertised tool must actually dispatch, and every advertised
    /// argument must survive the round trip through the type that entry
    /// dispatches to. This walks `tools/list` rather than a hand-kept list, so it
    /// cannot fall out of step with the table the way a third copy of the
    /// name-to-type mapping would.
    #[test]
    fn every_advertised_tool_dispatches_with_its_advertised_arguments() {
        let state = crate::store::empty_test_state();
        for tool in list(Some(json!(1)))["result"]["tools"].as_array().unwrap() {
            let name = tool["name"].as_str().unwrap();
            let props = tool["inputSchema"]["properties"].as_object().unwrap();
            let args: Value = props
                .iter()
                .map(|(key, spec)| (key.clone(), schema_valid_value(spec)))
                .collect::<serde_json::Map<String, Value>>()
                .into();
            let resp = dispatch(&state, Some(json!(1)), name, &args)
                .unwrap_or_else(|| panic!("{name} is advertised but does not dispatch"));
            // The call may well fail on semantics — an empty store knows no
            // services, and "x" is not a trace id. It must not fail on the shape
            // of the arguments the server itself advertised.
            if let Some(text) = resp["result"]["content"][0]["text"].as_str() {
                // Wordings only the argument validator produces. A handler's own
                // "invalid `start`: x" is a semantic verdict and expected here.
                for rejection in [
                    "unknown argument",
                    "missing required argument",
                    "Valid values:",
                    "A value the handler cannot read",
                ] {
                    assert!(
                        !text.contains(rejection),
                        "{name} rejects its own advertised arguments {args}: {text}"
                    );
                }
            }
        }
    }

    /// A value satisfying one advertised property, so the payload above is built
    /// from the schema rather than from an assumption about it.
    fn schema_valid_value(spec: &Value) -> Value {
        if let Some(first) = spec["enum"].as_array().and_then(|values| values.first()) {
            return first.clone();
        }
        match spec["type"].as_str() {
            Some("integer") | Some("number") => json!(1),
            Some("boolean") => json!(true),
            Some("array") => json!([]),
            Some("object") => json!({}),
            _ => json!("x"),
        }
    }

    #[test]
    fn tools_list_contains_all_ten_with_annotations() {
        let resp = list(Some(json!(1)));
        let tools = resp["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 10);
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        for expected in [
            "reset",
            "mark_checkpoint",
            "summarize_activity",
            "check_health",
            "query_logs",
            "query_traces",
            "query_metrics",
            "get_trace",
            "list_services",
            "describe_service",
        ] {
            assert!(names.contains(&expected), "missing tool {expected}");
        }
        let reset = tools.iter().find(|t| t["name"] == "reset").unwrap();
        assert_eq!(reset["annotations"]["destructiveHint"], json!(true));
        assert_eq!(reset["annotations"]["readOnlyHint"], json!(false));
        let logs = tools.iter().find(|t| t["name"] == "query_logs").unwrap();
        assert_eq!(logs["annotations"]["readOnlyHint"], json!(true));
        assert_eq!(logs["annotations"]["openWorldHint"], json!(false));
    }

    /// The dispatcher treats `properties` as an exhaustive allowlist, so the
    /// advertised schema must say so too — otherwise a client validating against
    /// `tools/list` considers a key valid that the server rejects.
    #[test]
    fn every_input_schema_is_closed() {
        let resp = list(Some(json!(1)));
        for t in resp["result"]["tools"].as_array().unwrap() {
            assert_eq!(
                t["inputSchema"]["additionalProperties"],
                json!(false),
                "{} inputSchema must set additionalProperties: false",
                t["name"]
            );
        }
    }

    /// The validator reads integer arguments as `u64`, so it rejects both
    /// negatives. A client validating against `tools/list` must reach the same
    /// verdict — the schema has to carry the bound, not just the code.
    #[test]
    fn every_integer_argument_advertises_its_lower_bound() {
        let resp = list(Some(json!(1)));
        for t in resp["result"]["tools"].as_array().unwrap() {
            let Some(props) = t["inputSchema"]["properties"].as_object() else {
                continue;
            };
            for (key, spec) in props {
                if spec["type"] == json!("integer") {
                    assert_eq!(
                        spec["minimum"],
                        json!(0),
                        "{}.{key} is an integer argument without minimum: 0",
                        t["name"]
                    );
                    assert_eq!(
                        spec["maximum"],
                        json!(u64::MAX),
                        "{}.{key} is an integer argument without its upper bound",
                        t["name"]
                    );
                }
            }
        }
    }

    /// `status` accepts exactly three values; advertising it as a bare string
    /// left callers guessing and kept the check out of the shared validator.
    #[test]
    fn query_traces_status_advertises_its_values() {
        let resp = list(Some(json!(1)));
        let tools = resp["result"]["tools"].as_array().unwrap();
        let traces = tools.iter().find(|t| t["name"] == "query_traces").unwrap();
        assert_eq!(
            traces["inputSchema"]["properties"]["status"]["enum"],
            json!(["error", "ok", "unset"])
        );
    }

    #[test]
    fn instructions_mention_the_loop() {
        assert!(INSTRUCTIONS.contains("reset"));
        assert!(INSTRUCTIONS.contains("summarize_activity"));
        assert!(INSTRUCTIONS.contains("mark_checkpoint"));
    }

    #[test]
    fn every_tool_has_output_schema() {
        let resp = list(Some(json!(1)));
        let tools = resp["result"]["tools"].as_array().unwrap();
        for t in tools {
            assert!(
                t["outputSchema"].is_object(),
                "{} missing outputSchema",
                t["name"]
            );
        }
    }
}
