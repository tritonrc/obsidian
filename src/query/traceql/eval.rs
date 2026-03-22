//! TraceQL evaluator against TraceStore.

use rustc_hash::{FxHashMap, FxHashSet};

use crate::store::trace_store::{AttributeValue, Span, SpanStatus, TraceStore};

use super::parser::{
    AttrScope, CompareOp, LogicalOp, PipelineStage, SpanCondition, SpanStatusValue, SpanValue,
    StructuralOp, TraceQLExpr,
};

/// Result of a TraceQL evaluation.
#[derive(Debug, Clone)]
pub struct TraceResult {
    pub trace_id: [u8; 16],
    pub matched_spans: Vec<MatchedSpan>,
}

/// A span that matched the query.
#[derive(Debug, Clone)]
pub struct MatchedSpan {
    pub span_id: [u8; 8],
    pub name: String,
    pub service_name: String,
    pub start_time_ns: i64,
    pub duration_ns: i64,
    pub status: SpanStatus,
    pub attributes: Vec<(String, String)>,
}

/// Evaluate a TraceQL expression against the trace store.
pub fn evaluate_traceql(expr: &TraceQLExpr, store: &TraceStore) -> Vec<TraceResult> {
    match expr {
        TraceQLExpr::SpanSelector {
            conditions,
            logical_ops,
        } => eval_span_selector(conditions, logical_ops, store),
        TraceQLExpr::Structural { op, lhs, rhs } => eval_structural(op, lhs, rhs, store),
        TraceQLExpr::Pipeline {
            inner,
            pipeline_stages,
        } => {
            let mut results = evaluate_traceql(inner, store);
            for stage in pipeline_stages {
                results = apply_pipeline_stage(results, stage);
            }
            results
        }
    }
}

/// Compare a u64 value against a target using the given operator.
fn compare_u64(a: u64, op: &CompareOp, b: u64) -> bool {
    match op {
        CompareOp::Eq => a == b,
        CompareOp::Neq => a != b,
        CompareOp::Gt => a > b,
        CompareOp::Lt => a < b,
        CompareOp::Gte => a >= b,
        CompareOp::Lte => a <= b,
        CompareOp::Regex => false,
    }
}

/// Compare an i64 duration (nanoseconds) against a target using the given operator.
fn compare_duration_ns(a: i64, op: &CompareOp, b: i64) -> bool {
    match op {
        CompareOp::Eq => a == b,
        CompareOp::Neq => a != b,
        CompareOp::Gt => a > b,
        CompareOp::Lt => a < b,
        CompareOp::Gte => a >= b,
        CompareOp::Lte => a <= b,
        CompareOp::Regex => false,
    }
}

/// Apply a pipeline stage to filter trace results.
fn apply_pipeline_stage(results: Vec<TraceResult>, stage: &PipelineStage) -> Vec<TraceResult> {
    match stage {
        PipelineStage::CountFilter { op, value } => results
            .into_iter()
            .filter(|r| {
                let count = r.matched_spans.len() as u64;
                compare_u64(count, op, *value)
            })
            .collect(),
        PipelineStage::AvgDuration { op, value_ns } => results
            .into_iter()
            .filter(|r| {
                if r.matched_spans.is_empty() {
                    return false;
                }
                let total: i64 = r.matched_spans.iter().map(|s| s.duration_ns).sum();
                let avg = total / r.matched_spans.len() as i64;
                compare_duration_ns(avg, op, *value_ns)
            })
            .collect(),
        PipelineStage::MaxDuration { op, value_ns } => results
            .into_iter()
            .filter(|r| {
                if r.matched_spans.is_empty() {
                    return false;
                }
                let max_dur = r
                    .matched_spans
                    .iter()
                    .map(|s| s.duration_ns)
                    .max()
                    .unwrap_or(0);
                compare_duration_ns(max_dur, op, *value_ns)
            })
            .collect(),
        PipelineStage::MinDuration { op, value_ns } => results
            .into_iter()
            .filter(|r| {
                if r.matched_spans.is_empty() {
                    return false;
                }
                let min_dur = r
                    .matched_spans
                    .iter()
                    .map(|s| s.duration_ns)
                    .min()
                    .unwrap_or(0);
                compare_duration_ns(min_dur, op, *value_ns)
            })
            .collect(),
    }
}

fn eval_span_selector(
    conditions: &[SpanCondition],
    logical_ops: &[LogicalOp],
    store: &TraceStore,
) -> Vec<TraceResult> {
    // Pre-compile regex patterns once, indexed by condition position.
    let compiled_regexes = precompile_regexes(conditions);

    let mut results: Vec<TraceResult> = Vec::new();

    for (trace_id, spans) in &store.traces {
        let matched: Vec<MatchedSpan> = spans
            .iter()
            .filter(|span| {
                span_matches_conditions(span, conditions, logical_ops, &compiled_regexes, store)
            })
            .map(|span| span_to_matched(span, store))
            .collect();

        if !matched.is_empty() {
            results.push(TraceResult {
                trace_id: *trace_id,
                matched_spans: matched,
            });
        }
    }

    // Sort by trace_id for deterministic output
    results.sort_by(|a, b| a.trace_id.cmp(&b.trace_id));
    results
}

/// Pre-compile regex patterns from conditions. Returns one Option<Regex> per condition.
fn precompile_regexes(conditions: &[SpanCondition]) -> Vec<Option<regex::Regex>> {
    conditions
        .iter()
        .map(|cond| {
            let (op, pattern) = match cond {
                SpanCondition::Attribute {
                    op,
                    value: SpanValue::String(s),
                    ..
                } => (op, s.as_str()),
                SpanCondition::Name { op, value } => (op, value.as_str()),
                _ => return None,
            };
            if *op == CompareOp::Regex {
                regex::Regex::new(pattern).ok()
            } else {
                None
            }
        })
        .collect()
}

fn eval_structural(
    op: &StructuralOp,
    lhs: &TraceQLExpr,
    rhs: &TraceQLExpr,
    store: &TraceStore,
) -> Vec<TraceResult> {
    match op {
        StructuralOp::Descendant => {
            let lhs_results = evaluate_traceql(lhs, store);
            let rhs_results = evaluate_traceql(rhs, store);

            // Find traces where an lhs span is an ancestor of an rhs span
            let mut results = Vec::new();

            let rhs_by_trace: FxHashMap<[u8; 16], &TraceResult> =
                rhs_results.iter().map(|r| (r.trace_id, r)).collect();

            for lhs_result in &lhs_results {
                if let Some(rhs_result) = rhs_by_trace.get(&lhs_result.trace_id)
                    && let Some(trace_spans) = store.get_trace(&lhs_result.trace_id)
                {
                    // Build span_map ONCE per trace, not per pair
                    let span_map: FxHashMap<[u8; 8], &Span> =
                        trace_spans.iter().map(|s| (s.span_id, s)).collect();

                    let mut matched = Vec::new();

                    for lhs_span in &lhs_result.matched_spans {
                        for rhs_span in &rhs_result.matched_spans {
                            if is_descendant(lhs_span.span_id, rhs_span.span_id, &span_map) {
                                matched.push(rhs_span.clone());
                            }
                        }
                    }

                    if !matched.is_empty() {
                        // Deduplicate
                        matched.sort_by_key(|s| s.span_id);
                        matched.dedup_by_key(|s| s.span_id);
                        results.push(TraceResult {
                            trace_id: lhs_result.trace_id,
                            matched_spans: matched,
                        });
                    }
                }
            }

            results
        }
    }
}

/// Check if `ancestor_span_id` is an ancestor of `descendant_span_id`.
fn is_descendant(
    ancestor_span_id: [u8; 8],
    descendant_span_id: [u8; 8],
    span_map: &FxHashMap<[u8; 8], &Span>,
) -> bool {
    let mut current_id = descendant_span_id;
    let mut visited = FxHashSet::default();

    loop {
        if current_id == ancestor_span_id {
            return false; // same span, not descendant
        }

        if let Some(span) = span_map.get(&current_id) {
            if let Some(parent_id) = span.parent_span_id {
                if parent_id == ancestor_span_id {
                    return true;
                }
                if visited.contains(&parent_id) {
                    return false; // cycle detection
                }
                visited.insert(current_id);
                current_id = parent_id;
            } else {
                return false; // reached root
            }
        } else {
            return false;
        }
    }
}

fn span_matches_conditions(
    span: &Span,
    conditions: &[SpanCondition],
    logical_ops: &[LogicalOp],
    compiled_regexes: &[Option<regex::Regex>],
    store: &TraceStore,
) -> bool {
    if conditions.is_empty() {
        return true; // empty selector `{}` matches all spans
    }

    // Evaluate left-to-right with AND binding tighter than OR.
    // Split by OR first: result is true if ANY OR-group is true.
    // Within each OR-group (connected by AND), ALL conditions must match.
    let mut current_and_result = span_matches_condition(
        span,
        &conditions[0],
        compiled_regexes.first().and_then(|r| r.as_ref()),
        store,
    );

    for i in 0..logical_ops.len() {
        let next_match = span_matches_condition(
            span,
            &conditions[i + 1],
            compiled_regexes.get(i + 1).and_then(|r| r.as_ref()),
            store,
        );
        match logical_ops[i] {
            LogicalOp::And => {
                current_and_result = current_and_result && next_match;
            }
            LogicalOp::Or => {
                // Short-circuit: if the current AND-group matched, the whole expression is true
                if current_and_result {
                    return true;
                }
                // Start a new AND-group
                current_and_result = next_match;
            }
        }
    }

    current_and_result
}

fn span_matches_condition(
    span: &Span,
    condition: &SpanCondition,
    compiled_regex: Option<&regex::Regex>,
    store: &TraceStore,
) -> bool {
    match condition {
        SpanCondition::Duration { op, value } => {
            let span_dur = std::time::Duration::from_nanos(span.duration_ns.max(0) as u64);
            compare_duration(&span_dur, op, value)
        }
        SpanCondition::Status { op, value } => {
            let status_matches = match value {
                SpanStatusValue::Ok => span.status == SpanStatus::Ok,
                SpanStatusValue::Error => span.status == SpanStatus::Error,
                SpanStatusValue::Unset => span.status == SpanStatus::Unset,
            };
            match op {
                CompareOp::Eq => status_matches,
                CompareOp::Neq => !status_matches,
                _ => false,
            }
        }
        SpanCondition::Name { op, value } => {
            let span_name = store.resolve(&span.name);
            compare_string(span_name, op, value, compiled_regex)
        }
        SpanCondition::Attribute {
            scope,
            name,
            op,
            value,
        } => {
            // Build the full attribute key to look up
            let attr_key = match scope {
                AttrScope::Resource => format!("resource.{}", name),
                AttrScope::Span => format!("span.{}", name),
            };

            // Find the attribute
            for (key_spur, attr_val) in &span.attributes {
                let key = store.resolve(key_spur);
                if key == attr_key {
                    return compare_attribute_value(attr_val, op, value, compiled_regex, store);
                }
            }

            // Attribute not found
            matches!(op, CompareOp::Neq)
        }
    }
}

fn compare_duration(
    span_dur: &std::time::Duration,
    op: &CompareOp,
    target: &std::time::Duration,
) -> bool {
    match op {
        CompareOp::Eq => span_dur == target,
        CompareOp::Neq => span_dur != target,
        CompareOp::Gt => span_dur > target,
        CompareOp::Lt => span_dur < target,
        CompareOp::Gte => span_dur >= target,
        CompareOp::Lte => span_dur <= target,
        CompareOp::Regex => false,
    }
}

fn compare_string(
    actual: &str,
    op: &CompareOp,
    expected: &str,
    compiled_regex: Option<&regex::Regex>,
) -> bool {
    match op {
        CompareOp::Eq => actual == expected,
        CompareOp::Neq => actual != expected,
        CompareOp::Regex => compiled_regex
            .map(|re| re.is_match(actual))
            .unwrap_or(false),
        CompareOp::Gt => actual > expected,
        CompareOp::Lt => actual < expected,
        CompareOp::Gte => actual >= expected,
        CompareOp::Lte => actual <= expected,
    }
}

fn compare_attribute_value(
    attr: &AttributeValue,
    op: &CompareOp,
    target: &SpanValue,
    compiled_regex: Option<&regex::Regex>,
    store: &TraceStore,
) -> bool {
    match (attr, target) {
        (AttributeValue::String(s), SpanValue::String(t)) => {
            compare_string(store.resolve(s), op, t, compiled_regex)
        }
        (AttributeValue::Int(i), SpanValue::Int(t)) => compare_i64(*i, op, *t),
        (AttributeValue::Int(i), SpanValue::Float(t)) => compare_f64(*i as f64, op, *t),
        (AttributeValue::Float(f), SpanValue::Float(t)) => compare_f64(*f, op, *t),
        (AttributeValue::Float(f), SpanValue::Int(t)) => compare_f64(*f, op, *t as f64),
        (AttributeValue::String(s), SpanValue::Int(t)) => {
            // Try to parse string as int
            if let Ok(i) = store.resolve(s).parse::<i64>() {
                compare_i64(i, op, *t)
            } else {
                false
            }
        }
        _ => false,
    }
}

fn compare_i64(a: i64, op: &CompareOp, b: i64) -> bool {
    match op {
        CompareOp::Eq => a == b,
        CompareOp::Neq => a != b,
        CompareOp::Gt => a > b,
        CompareOp::Lt => a < b,
        CompareOp::Gte => a >= b,
        CompareOp::Lte => a <= b,
        CompareOp::Regex => false,
    }
}

fn compare_f64(a: f64, op: &CompareOp, b: f64) -> bool {
    match op {
        CompareOp::Eq => (a - b).abs() < f64::EPSILON,
        CompareOp::Neq => (a - b).abs() >= f64::EPSILON,
        CompareOp::Gt => a > b,
        CompareOp::Lt => a < b,
        CompareOp::Gte => a >= b,
        CompareOp::Lte => a <= b,
        CompareOp::Regex => false,
    }
}

fn span_to_matched(span: &Span, store: &TraceStore) -> MatchedSpan {
    let attributes: Vec<(String, String)> = span
        .attributes
        .iter()
        .map(|(k, v)| {
            (
                store.resolve(k).to_string(),
                store.resolve_attribute_value(v),
            )
        })
        .collect();

    MatchedSpan {
        span_id: span.span_id,
        name: store.resolve(&span.name).to_string(),
        service_name: store.resolve(&span.service_name).to_string(),
        start_time_ns: span.start_time_ns,
        duration_ns: span.duration_ns,
        status: span.status,
        attributes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::trace_store::{AttributeValue, Span, SpanStatus};
    use smallvec::SmallVec;

    fn make_store() -> TraceStore {
        let mut store = TraceStore::new();
        let tid1 = [1u8; 16];
        let tid2 = [2u8; 16];

        // Trace 1: gateway -> payments (parent-child)
        let gateway_svc = store.interner.get_or_intern("gateway");
        let payments_svc = store.interner.get_or_intern("payments");
        let get_op = store.interner.get_or_intern("GET /api");
        let process_op = store.interner.get_or_intern("process_payment");

        let res_svc_key = store.interner.get_or_intern("resource.service.name");
        let gateway_val = store.interner.get_or_intern("gateway");
        let payments_val = store.interner.get_or_intern("payments");

        let span1 = Span {
            trace_id: tid1,
            span_id: [1, 0, 0, 0, 0, 0, 0, 0],
            parent_span_id: None,
            name: get_op,
            service_name: gateway_svc,
            start_time_ns: 1_000_000_000,
            duration_ns: 500_000_000, // 500ms
            status: SpanStatus::Ok,
            attributes: SmallVec::from_vec(vec![(
                res_svc_key,
                AttributeValue::String(gateway_val),
            )]),
        };
        let span2 = Span {
            trace_id: tid1,
            span_id: [2, 0, 0, 0, 0, 0, 0, 0],
            parent_span_id: Some([1, 0, 0, 0, 0, 0, 0, 0]),
            name: process_op,
            service_name: payments_svc,
            start_time_ns: 1_100_000_000,
            duration_ns: 300_000_000, // 300ms
            status: SpanStatus::Error,
            attributes: SmallVec::from_vec(vec![(
                res_svc_key,
                AttributeValue::String(payments_val),
            )]),
        };

        // Trace 2: slow span
        let slow_op = store.interner.get_or_intern("slow_query");
        let span3 = Span {
            trace_id: tid2,
            span_id: [3, 0, 0, 0, 0, 0, 0, 0],
            parent_span_id: None,
            name: slow_op,
            service_name: payments_svc,
            start_time_ns: 2_000_000_000,
            duration_ns: 2_000_000_000, // 2s
            status: SpanStatus::Ok,
            attributes: SmallVec::from_vec(vec![(
                res_svc_key,
                AttributeValue::String(payments_val),
            )]),
        };

        store.ingest_spans(vec![span1, span2, span3]);
        store
    }

    #[test]
    fn test_eval_by_service() {
        let store = make_store();
        let expr = crate::query::traceql::parser::parse_traceql(
            r#"{ resource.service.name = "gateway" }"#,
        )
        .unwrap();
        let results = evaluate_traceql(&expr, &store);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].matched_spans[0].service_name, "gateway");
    }

    #[test]
    fn test_eval_by_duration() {
        let store = make_store();
        let expr = crate::query::traceql::parser::parse_traceql(r#"{ duration > 1s }"#).unwrap();
        let results = evaluate_traceql(&expr, &store);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].matched_spans[0].name, "slow_query");
    }

    #[test]
    fn test_eval_by_status() {
        let store = make_store();
        let expr = crate::query::traceql::parser::parse_traceql(r#"{ status = error }"#).unwrap();
        let results = evaluate_traceql(&expr, &store);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].matched_spans[0].name, "process_payment");
    }

    #[test]
    fn test_eval_structural_descendant() {
        let store = make_store();
        let expr = crate::query::traceql::parser::parse_traceql(
            r#"{ resource.service.name = "gateway" } >> { resource.service.name = "payments" }"#,
        )
        .unwrap();
        let results = evaluate_traceql(&expr, &store);
        assert_eq!(results.len(), 1);
        // The matched span should be the payments span (descendant)
        assert_eq!(results[0].matched_spans[0].service_name, "payments");
    }

    #[test]
    fn test_eval_combined_conditions() {
        let store = make_store();
        let expr = crate::query::traceql::parser::parse_traceql(
            r#"{ resource.service.name = "payments" && duration > 1s }"#,
        )
        .unwrap();
        let results = evaluate_traceql(&expr, &store);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].matched_spans[0].name, "slow_query");
    }

    #[test]
    fn test_eval_empty_selector_matches_all() {
        let store = make_store();
        let expr = crate::query::traceql::parser::parse_traceql("{}").unwrap();
        let results = evaluate_traceql(&expr, &store);
        // Should match both traces (3 total spans across 2 traces)
        assert_eq!(results.len(), 2);
        let total_spans: usize = results.iter().map(|r| r.matched_spans.len()).sum();
        assert_eq!(total_spans, 3);
    }

    #[test]
    fn test_eval_count_filter_gt() {
        let store = make_store();
        // {} matches all spans: trace1 has 2 spans, trace2 has 1 span
        // count() > 1 should only keep trace1
        let expr = crate::query::traceql::parser::parse_traceql("{} | count() > 1").unwrap();
        let results = evaluate_traceql(&expr, &store);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].matched_spans.len(), 2);
    }

    #[test]
    fn test_eval_count_filter_eq() {
        let store = make_store();
        // count() = 1 should only keep trace2 (1 span)
        let expr = crate::query::traceql::parser::parse_traceql("{} | count() = 1").unwrap();
        let results = evaluate_traceql(&expr, &store);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].matched_spans.len(), 1);
        assert_eq!(results[0].matched_spans[0].name, "slow_query");
    }

    #[test]
    fn test_eval_count_filter_gte() {
        let store = make_store();
        // count() >= 2 should only keep trace1
        let expr = crate::query::traceql::parser::parse_traceql("{} | count() >= 2").unwrap();
        let results = evaluate_traceql(&expr, &store);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].matched_spans.len(), 2);
    }

    #[test]
    fn test_eval_count_filter_with_conditions() {
        let store = make_store();
        // Match payments spans, then filter by count
        // Trace1 has 1 payments span, trace2 has 1 payments span
        // count() >= 1 keeps both
        let expr = crate::query::traceql::parser::parse_traceql(
            r#"{ resource.service.name = "payments" } | count() >= 1"#,
        )
        .unwrap();
        let results = evaluate_traceql(&expr, &store);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_eval_count_filter_excludes_all() {
        let store = make_store();
        // No trace has more than 2 matching spans
        let expr = crate::query::traceql::parser::parse_traceql("{} | count() > 10").unwrap();
        let results = evaluate_traceql(&expr, &store);
        assert!(results.is_empty());
    }
}
