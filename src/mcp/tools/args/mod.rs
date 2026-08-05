//! Typed arguments for every MCP tool — the single source of truth for what a
//! tool accepts.
//!
//! Each `tool_args!` block below produces three things that were previously kept
//! in step by hand: the struct the handler reads, the `inputSchema` advertised by
//! `tools/list`, and the allowlist the validator rejects a call against. Because
//! all three come from one field list, they cannot disagree:
//!
//! - A property cannot be advertised without a field for a handler to read — the
//!   schema is generated from the fields, so there is nowhere to add one.
//! - A field no handler reads is a `field is never read` build failure under
//!   CI's `-D warnings`, so an argument cannot be accepted and then ignored.
//!
//! That second direction is the one that bit us: an argument accepted and
//! dropped is invisible in the response, which still reports a count that reads
//! exactly like a match.
//!
//! Semantic validation stays in the handlers — a well-typed `min_duration` can
//! still be nonsense, and only the store knows which services exist.

mod schema;

use schema::{ArgType, str_enum, tool_args};

pub(in crate::mcp::tools) use schema::{ToolArgs, parse};

str_enum! {
    /// What a `reset` call clears.
    enum ResetScope { All = "all", Service = "service" }
}

str_enum! {
    /// How much detail a summary or trace tree carries.
    enum Detail { Concise = "concise", Detailed = "detailed" }
}

str_enum! {
    /// The span status a trace query filters on.
    enum TraceStatus { Error = "error", Ok = "ok", Unset = "unset" }
}

tool_args! {
    struct ResetArgs for "reset" {
        scope: ResetScope,
        service: Option<String>,
    }
}

tool_args! {
    struct MarkCheckpointArgs for "mark_checkpoint" {}
}

tool_args! {
    struct SummarizeActivityArgs for "summarize_activity" {
        service: String,
        since: Option<u64>,
        detail: Option<Detail>,
    }
}

tool_args! {
    struct CheckHealthArgs for "check_health" {}
}

tool_args! {
    struct QueryLogsArgs for "query_logs" {
        service: Option<String>,
        level: Option<String>,
        contains: Option<String>,
        limit: Option<u64>,
        logql: Option<String>,
    }
}

tool_args! {
    struct QueryTracesArgs for "query_traces" {
        service: Option<String>,
        name: Option<String>,
        status: Option<TraceStatus>,
        min_duration: Option<String>,
        limit: Option<u64>,
        traceql: Option<String>,
    }
}

tool_args! {
    struct QueryMetricsArgs for "query_metrics" {
        promql: String,
        start: Option<String>,
        end: Option<String>,
        step: Option<String>,
    }
}

tool_args! {
    struct GetTraceArgs for "get_trace" {
        trace_id: String,
        detail: Option<Detail>,
    }
}

tool_args! {
    struct ListServicesArgs for "list_services" {}
}

tool_args! {
    struct DescribeServiceArgs for "describe_service" {
        service: String,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    /// Every advertised property must deserialize into its tool's struct. This
    /// is the drift check the schema-generation side cannot make on its own: it
    /// proves the validator and the handler agree on the whole surface, so no
    /// advertised argument can be accepted by one and dropped by the other.
    fn assert_every_property_parses<T: ToolArgs>() {
        let schema = T::schema();
        let props = schema["properties"].as_object().expect("object schema");
        let payload: Value = props
            .iter()
            .map(|(name, spec)| (name.clone(), sample_value(spec)))
            .collect::<serde_json::Map<String, Value>>()
            .into();
        assert!(
            parse::<T>(&payload).is_ok(),
            "{} rejects its own advertised arguments {payload}: {:?}",
            T::TOOL,
            parse::<T>(&payload).err()
        );
    }

    /// A schema-valid value for one property, so the payload above exercises the
    /// declared type rather than a guess about it.
    fn sample_value(spec: &Value) -> Value {
        if let Some(first) = spec
            .get("enum")
            .and_then(|e| e.as_array())
            .and_then(|a| a.first())
        {
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
    fn every_advertised_argument_is_accepted_by_its_typed_struct() {
        assert_every_property_parses::<ResetArgs>();
        assert_every_property_parses::<MarkCheckpointArgs>();
        assert_every_property_parses::<SummarizeActivityArgs>();
        assert_every_property_parses::<CheckHealthArgs>();
        assert_every_property_parses::<QueryLogsArgs>();
        assert_every_property_parses::<QueryTracesArgs>();
        assert_every_property_parses::<QueryMetricsArgs>();
        assert_every_property_parses::<GetTraceArgs>();
        assert_every_property_parses::<ListServicesArgs>();
        assert_every_property_parses::<DescribeServiceArgs>();
    }

    /// `detail` means the same thing wherever it appears; two tools declaring it
    /// from the same type is what keeps that true.
    #[test]
    fn detail_is_declared_once_for_both_tools_that_take_it() {
        assert_eq!(
            SummarizeActivityArgs::schema()["properties"]["detail"],
            GetTraceArgs::schema()["properties"]["detail"]
        );
    }
}
