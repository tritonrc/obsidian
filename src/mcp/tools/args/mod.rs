//! Typed arguments for every MCP tool — the single source of truth for what a
//! tool accepts.
//!
//! Each `tool_args!` block below produces three things that were previously kept
//! in step by hand: the struct the handler reads, the `inputSchema` advertised by
//! `tools/list`, and the allowlist the validator rejects a call against. What
//! that does and does not guarantee, precisely:
//!
//! - A property cannot be advertised without a field behind it. The schema is
//!   generated from the fields, so there is nowhere else to add one, and
//!   [`super::registry`] pairs the advertised schema with the type it dispatches
//!   through in a single entry — advertising one tool's schema against another's
//!   handler is a type error.
//! - A field no handler touches is a `field is never read` build failure under
//!   CI's `-D warnings`. That is a guard, not a proof: the lint cannot tell
//!   reading a field from acting on it, and `let _ = &args.field` or an
//!   `#[allow(dead_code)]` defeats it. It catches the accident — a property
//!   wired up and then forgotten — which is the case that bit us, because an
//!   argument accepted and dropped is invisible in the response, which still
//!   reports a count that reads exactly like a match. Deliberate non-use is a
//!   different thing and is fine: `query_logs` ignores the structured filters
//!   when `logql` is set, and says so in its description.
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

impl Detail {
    /// Whether this level asks for the per-item extras (span attributes, longer
    /// samples).
    ///
    /// Both callers route through here and the match is exhaustive on purpose: a
    /// `matches!(.., Detailed)` at each call site would quietly read a variant
    /// added later as concise, which is the same silent fallback this module
    /// exists to prevent. Adding a variant now fails to compile until every
    /// caller decides what it means.
    pub(in crate::mcp::tools) fn wants_extras(self) -> bool {
        match self {
            Self::Detailed => true,
            Self::Concise => false,
        }
    }
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

    /// Every advertised property must be accepted by the struct that generated
    /// it, for every value the schema calls legal — each property alone as well
    /// as all of them together, so no property is carried by another's presence.
    ///
    /// This catches a property whose name, type, or enum spelling has drifted
    /// from its field. It does not prove a handler acts on the value: reaching a
    /// field is not the same as being applied, and `query_logs` deliberately
    /// ignores the structured filters when `logql` is set. That part is covered
    /// by the handlers' own tests.
    fn assert_advertised_arguments_parse<T: ToolArgs>() {
        let schema = T::schema();
        let props = schema["properties"].as_object().expect("object schema");
        let required: Vec<&str> = schema
            .get("required")
            .and_then(|names| names.as_array())
            .map(|names| names.iter().filter_map(|name| name.as_str()).collect())
            .unwrap_or_default();

        let base: serde_json::Map<String, Value> = required
            .iter()
            .map(|name| ((*name).to_owned(), legal_values(&props[*name])[0].clone()))
            .collect();

        let everything: Value = props
            .iter()
            .map(|(name, spec)| (name.clone(), legal_values(spec)[0].clone()))
            .collect::<serde_json::Map<String, Value>>()
            .into();
        let mut payloads = vec![everything];
        for (name, spec) in props {
            for value in legal_values(spec) {
                let mut payload = base.clone();
                payload.insert(name.clone(), value);
                payloads.push(payload.into());
            }
        }

        for payload in payloads {
            assert!(
                parse::<T>(&payload).is_ok(),
                "{} rejects its own advertised arguments {payload}: {:?}",
                T::TOOL,
                parse::<T>(&payload).err()
            );
        }
    }

    /// Every value the schema declares legal for one property — all of an enum's
    /// values rather than the first, and both ends of an integer's advertised
    /// range, since a bound the server enforces but cannot receive is the same
    /// class of mismatch.
    fn legal_values(spec: &Value) -> Vec<Value> {
        if let Some(values) = spec["enum"].as_array() {
            return values.clone();
        }
        match spec["type"].as_str() {
            Some("integer") => vec![json!(0), json!(1), json!(u64::MAX)],
            Some("number") => vec![json!(1)],
            Some("boolean") => vec![json!(true), json!(false)],
            Some("array") => vec![json!([])],
            Some("object") => vec![json!({})],
            _ => vec![json!("x")],
        }
    }

    #[test]
    fn every_advertised_argument_is_accepted_by_its_typed_struct() {
        assert_advertised_arguments_parse::<ResetArgs>();
        assert_advertised_arguments_parse::<MarkCheckpointArgs>();
        assert_advertised_arguments_parse::<SummarizeActivityArgs>();
        assert_advertised_arguments_parse::<CheckHealthArgs>();
        assert_advertised_arguments_parse::<QueryLogsArgs>();
        assert_advertised_arguments_parse::<QueryTracesArgs>();
        assert_advertised_arguments_parse::<QueryMetricsArgs>();
        assert_advertised_arguments_parse::<GetTraceArgs>();
        assert_advertised_arguments_parse::<ListServicesArgs>();
        assert_advertised_arguments_parse::<DescribeServiceArgs>();
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
