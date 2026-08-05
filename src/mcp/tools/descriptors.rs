use serde_json::{Value, json};

use super::args::{
    CheckHealthArgs, DescribeServiceArgs, GetTraceArgs, ListServicesArgs, MarkCheckpointArgs,
    QueryLogsArgs, QueryMetricsArgs, QueryTracesArgs, ResetArgs, SummarizeActivityArgs, ToolArgs,
};
use crate::mcp::protocol;

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

/// Build the static list of tool descriptors.
fn descriptors() -> Vec<Value> {
    vec![
        json!({
            "name": "reset",
            "title": "Reset Telemetry Store",
            "description": "Clear telemetry. scope='all' wipes everything; scope='service' clears one service (requires `service`). The only write tool. Returns a fresh checkpoint token.",
            "inputSchema": ResetArgs::schema(),
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
        }),
        json!({
            "name": "mark_checkpoint",
            "title": "Mark Checkpoint",
            "description": "Return an opaque monotonic checkpoint token for 'now'. Pass it later as `since` to scope a summary to telemetry ingested after this point. Not a wall-clock time.",
            "inputSchema": MarkCheckpointArgs::schema(),
            "outputSchema": {
                "type": "object",
                "properties": { "checkpoint": { "type": "integer" } },
                "required": ["checkpoint"]
            },
            "annotations": read_only("Mark Checkpoint")
        }),
        json!({
            "name": "summarize_activity",
            "title": "Summarize Service Activity",
            "description": "Triage one service: error logs, failing/slow traces, error metrics, health score, one-line summary. Use after a run to see what it produced. Pass `since` (a checkpoint token) to scope to the latest run. Use this BEFORE drilling in with query_* tools.",
            "inputSchema": SummarizeActivityArgs::schema(),
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
        }),
        json!({
            "name": "check_health",
            "title": "Check Global Health",
            "description": "Rank every known service by health, worst first. Use when you don't yet know which service is in trouble.",
            "inputSchema": CheckHealthArgs::schema(),
            "outputSchema": {
                "type": "object",
                "properties": { "services": { "type": "array", "items": { "type": "object" } } },
                "required": ["services"]
            },
            "annotations": read_only("Check Global Health")
        }),
        json!({
            "name": "query_logs",
            "title": "Query Logs",
            "description": "Fetch log lines. Provide structured filters (service, level, contains) OR a raw `logql` string (e.g. {service=\"api\"} |= \"error\"). If `logql` is set, structured filters are ignored. To scope to a run, reset first, then query; summarize_activity is the checkpoint-accurate run summary.",
            "inputSchema": QueryLogsArgs::schema(),
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
        }),
        json!({
            "name": "query_traces",
            "title": "Query Traces",
            "description": "Find traces. Provide structured filters (service, name, status, min_duration) OR a raw `traceql` string. If `traceql` is set, structured filters are ignored.",
            "inputSchema": QueryTracesArgs::schema(),
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
        }),
        json!({
            "name": "query_metrics",
            "title": "Query Metrics",
            "description": "Run a raw PromQL query (e.g. rate(http_requests_total[5m])). For a range query pass all of start/end (unix seconds, or ms/ns) and step (a duration like 15s or 1m); omit all three for an instant query at now.",
            "inputSchema": QueryMetricsArgs::schema(),
            "outputSchema": {
                "type": "object",
                "properties": {
                    "series": { "type": "array", "items": { "type": "object" } },
                    "scalar": { "type": "number" }
                }
            },
            "annotations": read_only("Query Metrics")
        }),
        json!({
            "name": "get_trace",
            "title": "Get Trace Tree",
            "description": "Return one trace as a parent/child span tree. `trace_id` is 32 hex chars. Use `detail=detailed` to include span attributes.",
            "inputSchema": GetTraceArgs::schema(),
            "outputSchema": {
                "type": "object",
                "properties": {
                    "trace_id": { "type": "string" },
                    "roots": { "type": "array", "items": { "type": "object" } }
                },
                "required": ["trace_id", "roots"]
            },
            "annotations": read_only("Get Trace Tree")
        }),
        json!({
            "name": "list_services",
            "title": "List Services",
            "description": "List every service reporting telemetry and which signals (logs/metrics/traces) each has.",
            "inputSchema": ListServicesArgs::schema(),
            "outputSchema": {
                "type": "object",
                "properties": { "services": { "type": "array", "items": { "type": "object" } } },
                "required": ["services"]
            },
            "annotations": read_only("List Services")
        }),
        json!({
            "name": "describe_service",
            "title": "Describe Service",
            "description": "Catalog a service's queryable surface: metric names, log label keys + values, span attribute keys. Call this before writing a raw logql/promql/traceql query.",
            "inputSchema": DescribeServiceArgs::schema(),
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
        }),
    ]
}

pub fn list(id: Option<Value>) -> Value {
    protocol::success(id, json!({ "tools": descriptors() }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Every advertised schema must be the one its argument type generates. A
    /// descriptor that spelled its own schema out again — which is how this file
    /// used to read — could describe a property no handler has a field for.
    #[test]
    fn every_advertised_schema_is_the_one_its_argument_type_generates() {
        let resp = list(Some(json!(1)));
        let generated: Vec<(&str, Value)> = vec![
            ("reset", ResetArgs::schema()),
            ("mark_checkpoint", MarkCheckpointArgs::schema()),
            ("summarize_activity", SummarizeActivityArgs::schema()),
            ("check_health", CheckHealthArgs::schema()),
            ("query_logs", QueryLogsArgs::schema()),
            ("query_traces", QueryTracesArgs::schema()),
            ("query_metrics", QueryMetricsArgs::schema()),
            ("get_trace", GetTraceArgs::schema()),
            ("list_services", ListServicesArgs::schema()),
            ("describe_service", DescribeServiceArgs::schema()),
        ];
        let tools = resp["result"]["tools"].as_array().unwrap();
        assert_eq!(generated.len(), tools.len(), "a tool has no argument type");
        for (name, schema) in generated {
            let tool = tools.iter().find(|t| t["name"] == name).unwrap();
            assert_eq!(
                tool["inputSchema"], schema,
                "{name} advertises its own schema"
            );
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

    /// The validator reads integer arguments with `as_u64`, so it rejects
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
