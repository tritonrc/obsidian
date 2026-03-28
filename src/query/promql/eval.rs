//! PromQL evaluator walking the `promql-parser` AST against MetricStore.

use std::collections::BTreeMap;

use promql_parser::label::{MatchOp as PromMatchOp, Matchers};
use promql_parser::parser::{
    self, AggregateExpr, BinaryExpr, Call, Expr, LabelModifier, MatrixSelector, NumberLiteral,
    Offset, ParenExpr, StringLiteral, UnaryExpr, VectorSelector,
};
use regex::Regex;
use rustc_hash::FxHashMap;
use thiserror::Error;

use crate::store::log_store::{LabelMatchOp, LabelMatcher};
use crate::store::metric_store::{MetricStore, Sample};

/// Convert an optional `Offset` to a signed millisecond value.
/// `Pos` offsets shift the lookup window into the past (positive ms to subtract),
/// `Neg` offsets shift forward (negative ms to subtract).
fn offset_to_ms(offset: &Option<Offset>) -> i64 {
    match offset {
        Some(Offset::Pos(dur)) => dur.as_millis() as i64,
        Some(Offset::Neg(dur)) => -(dur.as_millis() as i64),
        None => 0,
    }
}

/// PromQL evaluation errors.
#[derive(Debug, Error)]
pub enum PromQLError {
    #[error("parse error: {0}")]
    Parse(String),
    #[error("unsupported expression: {0}")]
    Unsupported(String),
    #[error("evaluation error: {0}")]
    Eval(String),
}

/// Result of a PromQL evaluation.
#[derive(Debug, Clone)]
pub enum PromQLResult {
    /// Instant vector: each series has a single (timestamp, value).
    InstantVector(Vec<SeriesResult>),
    /// Range vector: each series has multiple samples.
    RangeVector(Vec<SeriesResult>),
    /// Scalar value.
    Scalar(f64),
}

/// A single series in the result.
#[derive(Debug, Clone)]
pub struct SeriesResult {
    pub labels: Vec<(String, String)>,
    pub samples: Vec<(i64, f64)>, // (timestamp_ms, value)
}

/// Evaluate a PromQL query at a single instant.
pub fn evaluate_instant(
    query: &str,
    store: &MetricStore,
    time_ms: i64,
) -> Result<PromQLResult, PromQLError> {
    let ast = parser::parse(query).map_err(|e| PromQLError::Parse(e.to_string()))?;
    eval_expr(&ast, store, time_ms, time_ms, 0, true)
}

/// Evaluate a PromQL query over a range with step.
pub fn evaluate_range(
    query: &str,
    store: &MetricStore,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Result<PromQLResult, PromQLError> {
    let ast = parser::parse(query).map_err(|e| PromQLError::Parse(e.to_string()))?;
    eval_expr(&ast, store, start_ms, end_ms, step_ms, false)
}

fn eval_expr(
    expr: &Expr,
    store: &MetricStore,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    instant: bool,
) -> Result<PromQLResult, PromQLError> {
    match expr {
        Expr::NumberLiteral(NumberLiteral { val, .. }) => Ok(PromQLResult::Scalar(*val)),

        Expr::VectorSelector(vs) => {
            eval_vector_selector(vs, store, start_ms, end_ms, step_ms, instant)
        }

        Expr::MatrixSelector(ms) => eval_matrix_selector(ms, store, start_ms, end_ms, step_ms),

        Expr::Call(call) => eval_call(call, store, start_ms, end_ms, step_ms, instant),

        Expr::Aggregate(agg) => eval_aggregation(agg, store, start_ms, end_ms, step_ms, instant),

        Expr::Binary(bin) => eval_binary(bin, store, start_ms, end_ms, step_ms, instant),

        Expr::Paren(ParenExpr { expr, .. }) => {
            eval_expr(expr, store, start_ms, end_ms, step_ms, instant)
        }

        Expr::Unary(UnaryExpr { expr, .. }) => {
            let result = eval_expr(expr, store, start_ms, end_ms, step_ms, instant)?;
            match result {
                PromQLResult::Scalar(v) => Ok(PromQLResult::Scalar(-v)),
                PromQLResult::InstantVector(series) => Ok(PromQLResult::InstantVector(
                    series
                        .into_iter()
                        .map(|mut s| {
                            for sample in &mut s.samples {
                                sample.1 = -sample.1;
                            }
                            s
                        })
                        .collect(),
                )),
                other => Ok(other),
            }
        }

        _ => Err(PromQLError::Unsupported(format!("{:?}", expr))),
    }
}

fn eval_vector_selector(
    vs: &VectorSelector,
    store: &MetricStore,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    instant: bool,
) -> Result<PromQLResult, PromQLError> {
    let mut matchers = convert_matchers(&vs.matchers);
    // If the selector has a name (e.g. `http_requests_total`), add __name__ matcher
    if let Some(name) = &vs.name
        && !matchers.iter().any(|m| m.name == "__name__")
    {
        matchers.push(LabelMatcher {
            name: "__name__".to_string(),
            op: LabelMatchOp::Eq,
            value: name.clone(),
        });
    }
    let series_ids = store.select_series(&matchers);
    let offset_ms = offset_to_ms(&vs.offset);

    let mut results = Vec::new();

    if instant || step_ms == 0 {
        // Instant query: find latest sample for each series at or before end_ms
        let lookback_ms = 5 * 60 * 1000; // 5-minute lookback
        let forward_buffer_ms = 1000; // 1s forward tolerance for OTLP/PromQL timestamp rounding
        let effective_end = end_ms - offset_ms;
        for sid in &series_ids {
            let samples = store.get_samples(
                *sid,
                effective_end - lookback_ms,
                effective_end + forward_buffer_ms,
            );
            if let Some(last) = samples.last() {
                let labels = store.get_series_labels(*sid).unwrap_or_default();
                results.push(SeriesResult {
                    labels,
                    samples: vec![(end_ms, last.value)],
                });
            }
        }
        Ok(PromQLResult::InstantVector(results))
    } else {
        // Range query: evaluate at each step
        let lookback_ms = 5 * 60 * 1000;
        for sid in &series_ids {
            let labels = store.get_series_labels(*sid).unwrap_or_default();
            let mut series_samples = Vec::new();
            let mut t = start_ms;
            while t <= end_ms {
                let effective_t = t - offset_ms;
                let samples = store.get_samples(*sid, effective_t - lookback_ms, effective_t);
                if let Some(last) = samples.last() {
                    series_samples.push((t, last.value));
                }
                t += step_ms;
            }
            if !series_samples.is_empty() {
                results.push(SeriesResult {
                    labels,
                    samples: series_samples,
                });
            }
        }
        Ok(PromQLResult::InstantVector(results))
    }
}

fn eval_matrix_selector(
    ms: &MatrixSelector,
    store: &MetricStore,
    _start_ms: i64,
    end_ms: i64,
    _step_ms: i64,
) -> Result<PromQLResult, PromQLError> {
    let vs = &ms.vs;
    let mut matchers = convert_matchers(&vs.matchers);
    if let Some(name) = &vs.name
        && !matchers.iter().any(|m| m.name == "__name__")
    {
        matchers.push(LabelMatcher {
            name: "__name__".to_string(),
            op: LabelMatchOp::Eq,
            value: name.clone(),
        });
    }
    let series_ids = store.select_series(&matchers);
    let range_ms = ms.range.as_millis() as i64;
    let offset_ms = offset_to_ms(&vs.offset);
    let effective_end = end_ms - offset_ms;

    let mut results = Vec::new();
    for sid in &series_ids {
        let labels = store.get_series_labels(*sid).unwrap_or_default();
        let samples = store.get_samples(*sid, effective_end - range_ms, effective_end);
        let sample_tuples: Vec<(i64, f64)> =
            samples.iter().map(|s| (s.timestamp_ms, s.value)).collect();
        if !sample_tuples.is_empty() {
            results.push(SeriesResult {
                labels,
                samples: sample_tuples,
            });
        }
    }
    Ok(PromQLResult::RangeVector(results))
}

fn eval_call(
    call: &Call,
    store: &MetricStore,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    instant: bool,
) -> Result<PromQLResult, PromQLError> {
    let func_name = call.func.name;

    match func_name {
        "rate" | "increase" | "irate" | "delta" | "deriv" => {
            if call.args.args.is_empty() {
                return Err(PromQLError::Eval(format!(
                    "{} requires a range vector argument",
                    func_name
                )));
            }
            let arg = &call.args.args[0];
            eval_rate_like(func_name, arg, store, start_ms, end_ms, step_ms, instant)
        }
        "histogram_quantile" => {
            if call.args.args.len() < 2 {
                return Err(PromQLError::Eval(
                    "histogram_quantile requires 2 arguments".into(),
                ));
            }
            let quantile = match eval_expr(
                &call.args.args[0],
                store,
                start_ms,
                end_ms,
                step_ms,
                instant,
            )? {
                PromQLResult::Scalar(v) => v,
                _ => {
                    return Err(PromQLError::Eval(
                        "first arg to histogram_quantile must be scalar".into(),
                    ));
                }
            };
            let buckets_result = eval_expr(
                &call.args.args[1],
                store,
                start_ms,
                end_ms,
                step_ms,
                instant,
            )?;
            eval_histogram_quantile(quantile, buckets_result)
        }
        "abs" | "ceil" | "floor" | "round" => {
            if call.args.args.is_empty() {
                return Err(PromQLError::Eval(format!(
                    "{} requires an argument",
                    func_name
                )));
            }
            let inner = eval_expr(
                &call.args.args[0],
                store,
                start_ms,
                end_ms,
                step_ms,
                instant,
            )?;
            apply_scalar_func(func_name, inner)
        }
        "label_replace" => eval_label_replace(call, store, start_ms, end_ms, step_ms, instant),
        "label_join" => eval_label_join(call, store, start_ms, end_ms, step_ms, instant),
        "absent" => {
            if call.args.args.is_empty() {
                return Err(PromQLError::Eval("absent requires an argument".into()));
            }
            let inner = eval_expr(
                &call.args.args[0],
                store,
                start_ms,
                end_ms,
                step_ms,
                instant,
            )?;
            match inner {
                PromQLResult::InstantVector(series) if series.is_empty() => {
                    Ok(PromQLResult::InstantVector(vec![SeriesResult {
                        labels: Vec::new(),
                        samples: vec![(end_ms, 1.0)],
                    }]))
                }
                PromQLResult::InstantVector(_) => Ok(PromQLResult::InstantVector(Vec::new())),
                PromQLResult::RangeVector(series) if series.is_empty() => {
                    Ok(PromQLResult::InstantVector(vec![SeriesResult {
                        labels: Vec::new(),
                        samples: vec![(end_ms, 1.0)],
                    }]))
                }
                PromQLResult::RangeVector(_) => Ok(PromQLResult::InstantVector(Vec::new())),
                PromQLResult::Scalar(_) => Ok(PromQLResult::InstantVector(Vec::new())),
            }
        }
        "sort" | "sort_desc" => {
            if call.args.args.is_empty() {
                return Err(PromQLError::Eval(format!(
                    "{} requires an argument",
                    func_name
                )));
            }
            let inner = eval_expr(
                &call.args.args[0],
                store,
                start_ms,
                end_ms,
                step_ms,
                instant,
            )?;
            match inner {
                PromQLResult::InstantVector(mut series) => {
                    if func_name == "sort" {
                        series.sort_by(|a, b| {
                            let a_val = a.samples.last().map(|(_, v)| *v).unwrap_or(f64::NAN);
                            let b_val = b.samples.last().map(|(_, v)| *v).unwrap_or(f64::NAN);
                            a_val
                                .partial_cmp(&b_val)
                                .unwrap_or(std::cmp::Ordering::Equal)
                        });
                    } else {
                        series.sort_by(|a, b| {
                            let a_val = a.samples.last().map(|(_, v)| *v).unwrap_or(f64::NAN);
                            let b_val = b.samples.last().map(|(_, v)| *v).unwrap_or(f64::NAN);
                            b_val
                                .partial_cmp(&a_val)
                                .unwrap_or(std::cmp::Ordering::Equal)
                        });
                    }
                    Ok(PromQLResult::InstantVector(series))
                }
                other => Ok(other),
            }
        }
        "clamp" => {
            if call.args.args.len() < 3 {
                return Err(PromQLError::Eval(
                    "clamp requires 3 arguments: vector, min, max".into(),
                ));
            }
            let inner = eval_expr(
                &call.args.args[0],
                store,
                start_ms,
                end_ms,
                step_ms,
                instant,
            )?;
            let min_val = match eval_expr(
                &call.args.args[1],
                store,
                start_ms,
                end_ms,
                step_ms,
                instant,
            )? {
                PromQLResult::Scalar(v) => v,
                _ => {
                    return Err(PromQLError::Eval(
                        "clamp min argument must be a scalar".into(),
                    ));
                }
            };
            let max_val = match eval_expr(
                &call.args.args[2],
                store,
                start_ms,
                end_ms,
                step_ms,
                instant,
            )? {
                PromQLResult::Scalar(v) => v,
                _ => {
                    return Err(PromQLError::Eval(
                        "clamp max argument must be a scalar".into(),
                    ));
                }
            };
            apply_clamp(inner, Some(min_val), Some(max_val))
        }
        "clamp_min" => {
            if call.args.args.len() < 2 {
                return Err(PromQLError::Eval(
                    "clamp_min requires 2 arguments: vector, min".into(),
                ));
            }
            let inner = eval_expr(
                &call.args.args[0],
                store,
                start_ms,
                end_ms,
                step_ms,
                instant,
            )?;
            let min_val = match eval_expr(
                &call.args.args[1],
                store,
                start_ms,
                end_ms,
                step_ms,
                instant,
            )? {
                PromQLResult::Scalar(v) => v,
                _ => {
                    return Err(PromQLError::Eval(
                        "clamp_min argument must be a scalar".into(),
                    ));
                }
            };
            apply_clamp(inner, Some(min_val), None)
        }
        "clamp_max" => {
            if call.args.args.len() < 2 {
                return Err(PromQLError::Eval(
                    "clamp_max requires 2 arguments: vector, max".into(),
                ));
            }
            let inner = eval_expr(
                &call.args.args[0],
                store,
                start_ms,
                end_ms,
                step_ms,
                instant,
            )?;
            let max_val = match eval_expr(
                &call.args.args[1],
                store,
                start_ms,
                end_ms,
                step_ms,
                instant,
            )? {
                PromQLResult::Scalar(v) => v,
                _ => {
                    return Err(PromQLError::Eval(
                        "clamp_max argument must be a scalar".into(),
                    ));
                }
            };
            apply_clamp(inner, None, Some(max_val))
        }
        "time" => {
            // time() returns the evaluation timestamp as a single-element instant vector
            let time_s = end_ms as f64 / 1000.0;
            Ok(PromQLResult::InstantVector(vec![SeriesResult {
                labels: Vec::new(),
                samples: vec![(end_ms, time_s)],
            }]))
        }
        "vector" => {
            if call.args.args.is_empty() {
                return Err(PromQLError::Eval("vector requires an argument".into()));
            }
            let inner = eval_expr(
                &call.args.args[0],
                store,
                start_ms,
                end_ms,
                step_ms,
                instant,
            )?;
            match inner {
                PromQLResult::Scalar(v) => Ok(PromQLResult::InstantVector(vec![SeriesResult {
                    labels: Vec::new(),
                    samples: vec![(end_ms, v)],
                }])),
                other => Ok(other),
            }
        }
        "scalar" => {
            if call.args.args.is_empty() {
                return Err(PromQLError::Eval("scalar requires an argument".into()));
            }
            let inner = eval_expr(
                &call.args.args[0],
                store,
                start_ms,
                end_ms,
                step_ms,
                instant,
            )?;
            match inner {
                PromQLResult::InstantVector(series) if series.len() == 1 => {
                    let val = series[0]
                        .samples
                        .first()
                        .map(|(_, v)| *v)
                        .unwrap_or(f64::NAN);
                    Ok(PromQLResult::Scalar(val))
                }
                PromQLResult::InstantVector(_) => Ok(PromQLResult::Scalar(f64::NAN)),
                PromQLResult::Scalar(v) => Ok(PromQLResult::Scalar(v)),
                _ => Ok(PromQLResult::Scalar(f64::NAN)),
            }
        }
        _ => Err(PromQLError::Unsupported(format!("function: {}", func_name))),
    }
}

fn eval_rate_like(
    func_name: &str,
    arg: &Expr,
    store: &MetricStore,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    instant: bool,
) -> Result<PromQLResult, PromQLError> {
    // arg should be a MatrixSelector
    let (vs, range_ms) = match arg {
        Expr::MatrixSelector(ms) => (&ms.vs, ms.range.as_millis() as i64),
        _ => {
            return Err(PromQLError::Eval(
                "rate/increase requires matrix selector".into(),
            ));
        }
    };

    let mut matchers = convert_matchers(&vs.matchers);
    if let Some(name) = &vs.name
        && !matchers.iter().any(|m| m.name == "__name__")
    {
        matchers.push(LabelMatcher {
            name: "__name__".to_string(),
            op: LabelMatchOp::Eq,
            value: name.clone(),
        });
    }
    let series_ids = store.select_series(&matchers);
    let offset_ms = offset_to_ms(&vs.offset);

    let mut results = Vec::new();

    if instant || step_ms == 0 {
        let effective_end = end_ms - offset_ms;
        for sid in &series_ids {
            let labels = store.get_series_labels(*sid).unwrap_or_default();
            let samples = store.get_samples(*sid, effective_end - range_ms, effective_end);
            let value = compute_rate_like(func_name, samples, range_ms);
            if let Some(v) = value {
                results.push(SeriesResult {
                    labels,
                    samples: vec![(end_ms, v)],
                });
            }
        }
        Ok(PromQLResult::InstantVector(results))
    } else {
        for sid in &series_ids {
            let labels = store.get_series_labels(*sid).unwrap_or_default();
            let mut series_samples = Vec::new();
            let mut t = start_ms;
            while t <= end_ms {
                let effective_t = t - offset_ms;
                let samples = store.get_samples(*sid, effective_t - range_ms, effective_t);
                if let Some(v) = compute_rate_like(func_name, samples, range_ms) {
                    series_samples.push((t, v));
                }
                t += step_ms;
            }
            if !series_samples.is_empty() {
                results.push(SeriesResult {
                    labels,
                    samples: series_samples,
                });
            }
        }
        Ok(PromQLResult::InstantVector(results))
    }
}

fn compute_rate_like(func_name: &str, samples: &[Sample], range_ms: i64) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let first = samples.first()?;
    let last = samples.last()?;

    match func_name {
        "rate" | "increase" => {
            // Counter reset detection: accumulate increases, treating decreases as resets
            let mut total_increase = 0.0;
            for i in 1..samples.len() {
                let delta = samples[i].value - samples[i - 1].value;
                if delta >= 0.0 {
                    total_increase += delta;
                } else {
                    // Counter reset: assume it went to 0 and then to current value
                    total_increase += samples[i].value;
                }
            }

            let sample_duration_s = (last.timestamp_ms - first.timestamp_ms) as f64 / 1000.0;
            if sample_duration_s <= 0.0 {
                return None;
            }

            if func_name == "increase" {
                // Extrapolate increase to cover the full range
                Some(total_increase * (range_ms as f64 / 1000.0) / sample_duration_s)
            } else {
                // rate = per-second rate based on actual sample duration
                Some(total_increase / sample_duration_s)
            }
        }
        "delta" => {
            // delta: raw difference, no reset detection (gauge metric)
            Some(last.value - first.value)
        }
        "deriv" => {
            // Linear regression using centered x values for numerical stability.
            // slope = Σ((x_i - x_mean)*(y_i - y_mean)) / Σ((x_i - x_mean)^2)
            let n = samples.len() as f64;
            let x_mean: f64 = samples
                .iter()
                .map(|s| s.timestamp_ms as f64 / 1000.0)
                .sum::<f64>()
                / n;
            let y_mean: f64 = samples.iter().map(|s| s.value).sum::<f64>() / n;
            let mut num = 0.0;
            let mut den = 0.0;
            for s in samples {
                let dx = s.timestamp_ms as f64 / 1000.0 - x_mean;
                let dy = s.value - y_mean;
                num += dx * dy;
                den += dx * dx;
            }
            if den.abs() < f64::EPSILON {
                None
            } else {
                Some(num / den)
            }
        }
        "irate" => {
            if samples.len() >= 2 {
                let prev = &samples[samples.len() - 2];
                let curr = &samples[samples.len() - 1];
                let dt = (curr.timestamp_ms - prev.timestamp_ms) as f64 / 1000.0;
                if dt > 0.0 {
                    let delta = curr.value - prev.value;
                    // irate also needs reset detection on last two samples
                    let increase = if delta >= 0.0 { delta } else { curr.value };
                    Some(increase / dt)
                } else {
                    None
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

fn eval_aggregation(
    agg: &AggregateExpr,
    store: &MetricStore,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    instant: bool,
) -> Result<PromQLResult, PromQLError> {
    let inner = eval_expr(&agg.expr, store, start_ms, end_ms, step_ms, instant)?;

    let series = match inner {
        PromQLResult::InstantVector(s) => s,
        PromQLResult::RangeVector(s) => s,
        PromQLResult::Scalar(v) => {
            return Ok(PromQLResult::Scalar(v));
        }
    };

    let op_name = agg.op.to_string();

    // Handle topk/bottomk separately — they select top/bottom k series rather than aggregating
    match op_name.as_str() {
        "topk" | "bottomk" => {
            let k = match &agg.param {
                Some(param_expr) => {
                    match eval_expr(param_expr, store, start_ms, end_ms, step_ms, instant)? {
                        PromQLResult::Scalar(v) => v as usize,
                        _ => {
                            return Err(PromQLError::Eval(
                                "topk/bottomk parameter must be a scalar".into(),
                            ));
                        }
                    }
                }
                None => {
                    return Err(PromQLError::Eval(
                        "topk/bottomk requires a parameter k".into(),
                    ));
                }
            };

            // Sort series by their latest sample value
            let mut sorted_series = series;
            sorted_series.sort_by(|a, b| {
                let a_val = a.samples.last().map(|(_, v)| *v).unwrap_or(f64::NAN);
                let b_val = b.samples.last().map(|(_, v)| *v).unwrap_or(f64::NAN);
                if op_name == "topk" {
                    b_val
                        .partial_cmp(&a_val)
                        .unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    a_val
                        .partial_cmp(&b_val)
                        .unwrap_or(std::cmp::Ordering::Equal)
                }
            });
            sorted_series.truncate(k);
            return Ok(PromQLResult::InstantVector(sorted_series));
        }
        "sum" | "avg" | "max" | "min" | "count" => {}
        other => {
            return Err(PromQLError::Unsupported(format!("aggregation: {}", other)));
        }
    }

    // Group series by their label set after applying modifier
    let mut groups: BTreeMap<Vec<(String, String)>, Vec<SeriesResult>> = BTreeMap::new();

    for sr in series {
        let group_labels = compute_group_labels(&sr.labels, &agg.modifier);
        groups.entry(group_labels).or_default().push(sr);
    }

    let mut results = Vec::new();
    for (group_labels, group_series) in groups {
        let aggregated = aggregate_group(&op_name, &group_series);
        results.push(SeriesResult {
            labels: group_labels,
            samples: aggregated,
        });
    }

    Ok(PromQLResult::InstantVector(results))
}

fn compute_group_labels(
    labels: &[(String, String)],
    modifier: &Option<LabelModifier>,
) -> Vec<(String, String)> {
    match modifier {
        Some(LabelModifier::Include(label_names)) => {
            // `by(labels)` keeps only listed labels
            let names: Vec<&String> = label_names.labels.iter().collect();
            labels
                .iter()
                .filter(|(k, _)| names.contains(&k))
                .cloned()
                .collect()
        }
        Some(LabelModifier::Exclude(label_names)) => {
            // `without(labels)` drops listed labels
            let names: Vec<&String> = label_names.labels.iter().collect();
            labels
                .iter()
                .filter(|(k, _)| !names.contains(&k))
                .cloned()
                .collect()
        }
        None => Vec::new(), // no grouping = single group
    }
}

fn aggregate_group(op: &str, series: &[SeriesResult]) -> Vec<(i64, f64)> {
    if series.is_empty() {
        return Vec::new();
    }

    let mut timestamps: Vec<i64> = series
        .iter()
        .flat_map(|s| s.samples.iter().map(|(t, _)| *t))
        .collect();
    timestamps.sort_unstable();
    timestamps.dedup();

    let lookups: Vec<FxHashMap<i64, f64>> = series
        .iter()
        .map(|s| s.samples.iter().copied().collect())
        .collect();

    timestamps
        .iter()
        .filter_map(|&t| {
            let values: Vec<f64> = lookups.iter().filter_map(|m| m.get(&t).copied()).collect();
            if values.is_empty() {
                return None;
            }
            let result = match op {
                "sum" => values.iter().sum(),
                "avg" => values.iter().sum::<f64>() / values.len() as f64,
                "max" => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                "min" => values.iter().cloned().fold(f64::INFINITY, f64::min),
                "count" => values.len() as f64,
                _ => unreachable!("unsupported aggregations filtered before reaching here"),
            };
            Some((t, result))
        })
        .collect()
}

fn eval_binary(
    bin: &BinaryExpr,
    store: &MetricStore,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    instant: bool,
) -> Result<PromQLResult, PromQLError> {
    let lhs = eval_expr(&bin.lhs, store, start_ms, end_ms, step_ms, instant)?;
    let rhs = eval_expr(&bin.rhs, store, start_ms, end_ms, step_ms, instant)?;

    let op = bin.op.to_string();

    match (&lhs, &rhs) {
        (PromQLResult::Scalar(l), PromQLResult::Scalar(r)) => {
            Ok(PromQLResult::Scalar(apply_binary_op(&op, *l, *r)))
        }
        (PromQLResult::InstantVector(series), PromQLResult::Scalar(scalar))
        | (PromQLResult::Scalar(scalar), PromQLResult::InstantVector(series)) => {
            let is_lhs_scalar = matches!(lhs, PromQLResult::Scalar(_));
            let results: Vec<SeriesResult> = series
                .iter()
                .map(|sr| {
                    let samples: Vec<(i64, f64)> = sr
                        .samples
                        .iter()
                        .filter_map(|&(t, v)| {
                            let result = if is_lhs_scalar {
                                apply_binary_op(&op, *scalar, v)
                            } else {
                                apply_binary_op(&op, v, *scalar)
                            };
                            if is_comparison(&op) && result == 0.0 {
                                None
                            } else if is_comparison(&op) {
                                Some((t, v))
                            } else {
                                Some((t, result))
                            }
                        })
                        .collect();
                    SeriesResult {
                        labels: sr.labels.clone(),
                        samples,
                    }
                })
                .filter(|sr| !sr.samples.is_empty())
                .collect();
            Ok(PromQLResult::InstantVector(results))
        }
        (PromQLResult::InstantVector(lhs_series), PromQLResult::InstantVector(rhs_series)) => {
            // 1:1 vector matching by label set
            let mut results = Vec::new();
            for ls in lhs_series {
                let match_labels: Vec<(String, String)> = ls
                    .labels
                    .iter()
                    .filter(|(k, _)| k != "__name__")
                    .cloned()
                    .collect();
                for rs in rhs_series {
                    let rhs_match: Vec<(String, String)> = rs
                        .labels
                        .iter()
                        .filter(|(k, _)| k != "__name__")
                        .cloned()
                        .collect();
                    if match_labels == rhs_match {
                        let samples: Vec<(i64, f64)> =
                            ls.samples
                                .iter()
                                .filter_map(|&(t, lv)| {
                                    rs.samples.iter().find(|(rt, _)| *rt == t).and_then(
                                        |&(_, rv)| {
                                            let result = apply_binary_op(&op, lv, rv);
                                            if is_comparison(&op) && result == 0.0 {
                                                None
                                            } else if is_comparison(&op) {
                                                Some((t, lv))
                                            } else {
                                                Some((t, result))
                                            }
                                        },
                                    )
                                })
                                .collect();
                        if !samples.is_empty() {
                            results.push(SeriesResult {
                                labels: match_labels.clone(),
                                samples,
                            });
                        }
                    }
                }
            }
            Ok(PromQLResult::InstantVector(results))
        }
        _ => Err(PromQLError::Unsupported(
            "unsupported binary operand types".into(),
        )),
    }
}

fn apply_binary_op(op: &str, l: f64, r: f64) -> f64 {
    match op {
        "+" => l + r,
        "-" => l - r,
        "*" => l * r,
        "/" => {
            if r == 0.0 {
                f64::NAN
            } else {
                l / r
            }
        }
        "%" => {
            if r == 0.0 {
                f64::NAN
            } else {
                l % r
            }
        }
        ">" => {
            if l > r {
                1.0
            } else {
                0.0
            }
        }
        "<" => {
            if l < r {
                1.0
            } else {
                0.0
            }
        }
        ">=" => {
            if l >= r {
                1.0
            } else {
                0.0
            }
        }
        "<=" => {
            if l <= r {
                1.0
            } else {
                0.0
            }
        }
        "==" => {
            if (l - r).abs() < f64::EPSILON {
                1.0
            } else {
                0.0
            }
        }
        "!=" => {
            if (l - r).abs() >= f64::EPSILON {
                1.0
            } else {
                0.0
            }
        }
        "^" => l.powf(r),
        _ => f64::NAN,
    }
}

fn is_comparison(op: &str) -> bool {
    matches!(op, ">" | "<" | ">=" | "<=" | "==" | "!=")
}

fn eval_histogram_quantile(
    quantile: f64,
    buckets_result: PromQLResult,
) -> Result<PromQLResult, PromQLError> {
    let series = match buckets_result {
        PromQLResult::InstantVector(s) => s,
        _ => {
            return Err(PromQLError::Eval(
                "histogram_quantile requires instant vector".into(),
            ));
        }
    };

    type BucketEntry = (f64, Vec<(i64, f64)>);
    // Group by labels excluding `le`
    let mut groups: BTreeMap<Vec<(String, String)>, Vec<BucketEntry>> = BTreeMap::new();

    for sr in &series {
        let le_val = sr
            .labels
            .iter()
            .find(|(k, _)| k == "le")
            .map(|(_, v)| v.as_str());

        let le = match le_val {
            Some("+Inf") => f64::INFINITY,
            Some(v) => v.parse().unwrap_or(f64::INFINITY),
            None => continue,
        };

        let group_labels: Vec<(String, String)> = sr
            .labels
            .iter()
            .filter(|(k, _)| k != "le")
            .cloned()
            .collect();

        groups
            .entry(group_labels)
            .or_default()
            .push((le, sr.samples.clone()));
    }

    let mut results = Vec::new();
    for (group_labels, mut buckets) in groups {
        buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        // For each timestamp, compute quantile
        if let Some((_, first_samples)) = buckets.first() {
            let timestamps: Vec<i64> = first_samples.iter().map(|(t, _)| *t).collect();
            let mut samples = Vec::new();

            for &t in &timestamps {
                let mut bucket_bounds = Vec::new();
                let mut bucket_counts = Vec::new();

                for (le, s) in &buckets {
                    if let Some((_, v)) = s.iter().find(|(ts, _)| *ts == t) {
                        bucket_bounds.push(*le);
                        bucket_counts.push(*v);
                    }
                }

                if bucket_counts.is_empty() {
                    continue;
                }

                let total = *bucket_counts.last().unwrap_or(&0.0);
                if total == 0.0 {
                    continue;
                }

                let target = quantile * total;

                // Linear interpolation between buckets
                let mut prev_count = 0.0;
                let mut prev_bound = 0.0;

                let mut found = false;
                for (i, &count) in bucket_counts.iter().enumerate() {
                    if count >= target {
                        let bound = bucket_bounds[i];
                        if bound.is_infinite() {
                            samples.push((t, prev_bound));
                        } else if count == prev_count {
                            samples.push((t, bound));
                        } else {
                            let fraction = (target - prev_count) / (count - prev_count);
                            let value = prev_bound + fraction * (bound - prev_bound);
                            samples.push((t, value));
                        }
                        found = true;
                        break;
                    }
                    prev_count = count;
                    prev_bound = bucket_bounds[i];
                }

                if !found {
                    samples.push((t, *bucket_bounds.last().unwrap_or(&0.0)));
                }
            }

            if !samples.is_empty() {
                results.push(SeriesResult {
                    labels: group_labels,
                    samples,
                });
            }
        }
    }

    Ok(PromQLResult::InstantVector(results))
}

fn apply_scalar_func(func_name: &str, result: PromQLResult) -> Result<PromQLResult, PromQLError> {
    match result {
        PromQLResult::InstantVector(series) => {
            let mapped: Vec<SeriesResult> = series
                .into_iter()
                .map(|mut sr| {
                    for sample in &mut sr.samples {
                        sample.1 = match func_name {
                            "abs" => sample.1.abs(),
                            "ceil" => sample.1.ceil(),
                            "floor" => sample.1.floor(),
                            "round" => sample.1.round(),
                            _ => sample.1,
                        };
                    }
                    sr
                })
                .collect();
            Ok(PromQLResult::InstantVector(mapped))
        }
        PromQLResult::Scalar(v) => {
            let result = match func_name {
                "abs" => v.abs(),
                "ceil" => v.ceil(),
                "floor" => v.floor(),
                "round" => v.round(),
                _ => v,
            };
            Ok(PromQLResult::Scalar(result))
        }
        other => Ok(other),
    }
}

/// Clamp each sample value in a vector by optional min/max bounds.
fn apply_clamp(
    result: PromQLResult,
    min_val: Option<f64>,
    max_val: Option<f64>,
) -> Result<PromQLResult, PromQLError> {
    match result {
        PromQLResult::InstantVector(series) => {
            let mapped: Vec<SeriesResult> = series
                .into_iter()
                .map(|mut sr| {
                    for sample in &mut sr.samples {
                        if let Some(min) = min_val
                            && sample.1 < min
                        {
                            sample.1 = min;
                        }
                        if let Some(max) = max_val
                            && sample.1 > max
                        {
                            sample.1 = max;
                        }
                    }
                    sr
                })
                .collect();
            Ok(PromQLResult::InstantVector(mapped))
        }
        PromQLResult::Scalar(mut v) => {
            if let Some(min) = min_val
                && v < min
            {
                v = min;
            }
            if let Some(max) = max_val
                && v > max
            {
                v = max;
            }
            Ok(PromQLResult::Scalar(v))
        }
        other => Ok(other),
    }
}

/// Extract a string literal value from an expression.
fn extract_string_arg(expr: &Expr) -> Result<&str, PromQLError> {
    match expr {
        Expr::StringLiteral(StringLiteral { val, .. }) => Ok(val.as_str()),
        _ => Err(PromQLError::Eval("expected string literal argument".into())),
    }
}

/// Evaluate `label_replace(v, dst_label, replacement, src_label, regex)`.
///
/// For each series in the inner vector, if the value of `src_label` matches the
/// full regex, set `dst_label` to `replacement` with `$1`-style capture group
/// substitution. If the regex does not match, the series is returned unchanged.
fn eval_label_replace(
    call: &Call,
    store: &MetricStore,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    instant: bool,
) -> Result<PromQLResult, PromQLError> {
    if call.args.args.len() < 5 {
        return Err(PromQLError::Eval(
            "label_replace requires 5 arguments".into(),
        ));
    }
    let inner = eval_expr(
        &call.args.args[0],
        store,
        start_ms,
        end_ms,
        step_ms,
        instant,
    )?;
    let dst_label = extract_string_arg(&call.args.args[1])?;
    let replacement = extract_string_arg(&call.args.args[2])?;
    let src_label = extract_string_arg(&call.args.args[3])?;
    let regex_str = extract_string_arg(&call.args.args[4])?;

    // Anchor the regex to match the full source label value (Prometheus semantics).
    let anchored = format!("^(?:{})$", regex_str);
    let re = Regex::new(&anchored).map_err(|e| PromQLError::Eval(format!("bad regex: {}", e)))?;

    let series = match inner {
        PromQLResult::InstantVector(s) => s,
        PromQLResult::RangeVector(s) => s,
        other => return Ok(other),
    };

    let results: Vec<SeriesResult> = series
        .into_iter()
        .map(|mut sr| {
            let src_value = sr
                .labels
                .iter()
                .find(|(k, _)| k == src_label)
                .map(|(_, v)| v.clone())
                .unwrap_or_default();

            if let Some(caps) = re.captures(&src_value) {
                // Build replacement string: substitute $1, $2, etc.
                let mut new_value = replacement.to_string();
                for i in (1..caps.len()).rev() {
                    let group_val = caps.get(i).map(|m| m.as_str()).unwrap_or("");
                    new_value = new_value.replace(&format!("${}", i), group_val);
                }

                if new_value.is_empty() {
                    // Empty replacement removes the label
                    sr.labels.retain(|(k, _)| k != dst_label);
                } else {
                    // Set or replace dst_label
                    if let Some(existing) = sr.labels.iter_mut().find(|(k, _)| k == dst_label) {
                        existing.1 = new_value;
                    } else {
                        sr.labels.push((dst_label.to_string(), new_value));
                        sr.labels.sort_by(|a, b| a.0.cmp(&b.0));
                    }
                }
            }
            sr
        })
        .collect();

    Ok(PromQLResult::InstantVector(results))
}

/// Evaluate `label_join(v, dst_label, separator, src_label1, src_label2, ...)`.
///
/// Concatenates the values of the source labels using the separator and stores
/// the result in `dst_label`.
fn eval_label_join(
    call: &Call,
    store: &MetricStore,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    instant: bool,
) -> Result<PromQLResult, PromQLError> {
    if call.args.args.len() < 4 {
        return Err(PromQLError::Eval(
            "label_join requires at least 4 arguments".into(),
        ));
    }
    let inner = eval_expr(
        &call.args.args[0],
        store,
        start_ms,
        end_ms,
        step_ms,
        instant,
    )?;
    let dst_label = extract_string_arg(&call.args.args[1])?;
    let separator = extract_string_arg(&call.args.args[2])?;

    let src_labels: Vec<&str> = call.args.args[3..]
        .iter()
        .map(|e| extract_string_arg(e))
        .collect::<Result<_, _>>()?;

    let series = match inner {
        PromQLResult::InstantVector(s) => s,
        PromQLResult::RangeVector(s) => s,
        other => return Ok(other),
    };

    let results: Vec<SeriesResult> = series
        .into_iter()
        .map(|mut sr| {
            let parts: Vec<String> = src_labels
                .iter()
                .map(|&src| {
                    sr.labels
                        .iter()
                        .find(|(k, _)| k == src)
                        .map(|(_, v)| v.clone())
                        .unwrap_or_default()
                })
                .collect();

            let joined = parts.join(separator);

            if joined.is_empty() {
                sr.labels.retain(|(k, _)| k != dst_label);
            } else if let Some(existing) = sr.labels.iter_mut().find(|(k, _)| k == dst_label) {
                existing.1 = joined;
            } else {
                sr.labels.push((dst_label.to_string(), joined));
                sr.labels.sort_by(|a, b| a.0.cmp(&b.0));
            }
            sr
        })
        .collect();

    Ok(PromQLResult::InstantVector(results))
}

fn convert_matchers(matchers: &Matchers) -> Vec<LabelMatcher> {
    matchers
        .matchers
        .iter()
        .map(|m| LabelMatcher {
            name: m.name.clone(),
            op: match m.op {
                PromMatchOp::Equal => LabelMatchOp::Eq,
                PromMatchOp::NotEqual => LabelMatchOp::Neq,
                PromMatchOp::Re(_) => LabelMatchOp::Regex,
                PromMatchOp::NotRe(_) => LabelMatchOp::NotRegex,
            },
            value: m.value.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::metric_store::Sample;

    fn make_store() -> MetricStore {
        let mut store = MetricStore::new();
        // Counter: http_requests_total with two methods
        for i in 0..10 {
            store.ingest_samples(
                "http_requests_total",
                vec![
                    ("method".into(), "GET".into()),
                    ("service".into(), "api".into()),
                ],
                vec![Sample {
                    timestamp_ms: i * 1000,
                    value: (i * 10) as f64,
                }],
            );
            store.ingest_samples(
                "http_requests_total",
                vec![
                    ("method".into(), "POST".into()),
                    ("service".into(), "api".into()),
                ],
                vec![Sample {
                    timestamp_ms: i * 1000,
                    value: (i * 5) as f64,
                }],
            );
        }
        // Gauge
        store.ingest_samples(
            "memory_usage_bytes",
            vec![("service".into(), "api".into())],
            vec![Sample {
                timestamp_ms: 5000,
                value: 1_000_000.0,
            }],
        );
        store
    }

    #[test]
    fn test_instant_selector() {
        let store = make_store();
        let result =
            evaluate_instant(r#"http_requests_total{method="GET"}"#, &store, 9000).unwrap();
        match result {
            PromQLResult::InstantVector(series) => {
                assert_eq!(series.len(), 1);
                assert_eq!(series[0].samples[0].1, 90.0);
            }
            _ => panic!("expected InstantVector"),
        }
    }

    #[test]
    fn test_rate() {
        let store = make_store();
        let result = evaluate_instant(
            r#"rate(http_requests_total{method="GET"}[10s])"#,
            &store,
            9000,
        )
        .unwrap();
        match result {
            PromQLResult::InstantVector(series) => {
                assert_eq!(series.len(), 1);
                // Samples span 0ms..9000ms (9s actual), increase = 90
                // rate = 90 / 9s = 10.0
                let rate = series[0].samples[0].1;
                assert!((rate - 10.0).abs() < 0.01, "rate was {}", rate);
            }
            _ => panic!("expected InstantVector"),
        }
    }

    #[test]
    fn test_sum_by() {
        let store = make_store();
        let result =
            evaluate_instant(r#"sum(http_requests_total) by (service)"#, &store, 9000).unwrap();
        match result {
            PromQLResult::InstantVector(series) => {
                assert_eq!(series.len(), 1);
                // GET=90 + POST=45 = 135
                assert_eq!(series[0].samples[0].1, 135.0);
            }
            _ => panic!("expected InstantVector"),
        }
    }

    #[test]
    fn test_avg() {
        let store = make_store();
        let result = evaluate_instant(r#"avg(http_requests_total)"#, &store, 9000).unwrap();
        match result {
            PromQLResult::InstantVector(series) => {
                assert_eq!(series.len(), 1);
                // (90+45)/2 = 67.5
                assert_eq!(series[0].samples[0].1, 67.5);
            }
            _ => panic!("expected InstantVector"),
        }
    }

    #[test]
    fn test_binary_scalar() {
        let store = make_store();
        let result =
            evaluate_instant(r#"http_requests_total{method="GET"} / 10"#, &store, 9000).unwrap();
        match result {
            PromQLResult::InstantVector(series) => {
                assert_eq!(series[0].samples[0].1, 9.0);
            }
            _ => panic!("expected InstantVector"),
        }
    }

    #[test]
    fn test_comparison_filter() {
        let store = make_store();
        let result = evaluate_instant(r#"http_requests_total > 50"#, &store, 9000).unwrap();
        match result {
            PromQLResult::InstantVector(series) => {
                // Only GET (90) should pass, POST (45) should be filtered
                assert_eq!(series.len(), 1);
                assert_eq!(series[0].samples[0].1, 90.0);
            }
            _ => panic!("expected InstantVector"),
        }
    }

    #[test]
    fn test_number_literal() {
        let store = make_store();
        let result = evaluate_instant("42", &store, 9000).unwrap();
        match result {
            PromQLResult::Scalar(v) => assert_eq!(v, 42.0),
            _ => panic!("expected Scalar"),
        }
    }

    #[test]
    fn task_18_label_replace_basic() {
        let store = make_store();
        // Replace method label into a new label called "verb"
        let result = evaluate_instant(
            r#"label_replace(http_requests_total{method="GET"}, "verb", "$1", "method", "(.*)")"#,
            &store,
            9000,
        )
        .unwrap();
        match result {
            PromQLResult::InstantVector(series) => {
                assert_eq!(series.len(), 1);
                let verb = series[0]
                    .labels
                    .iter()
                    .find(|(k, _)| k == "verb")
                    .map(|(_, v)| v.as_str());
                assert_eq!(verb, Some("GET"));
            }
            _ => panic!("expected InstantVector"),
        }
    }

    #[test]
    fn task_18_label_replace_no_match() {
        let store = make_store();
        // Regex doesn't match -> series returned unchanged
        let result = evaluate_instant(
            r#"label_replace(http_requests_total{method="GET"}, "verb", "$1", "method", "NOMATCH")"#,
            &store,
            9000,
        )
        .unwrap();
        match result {
            PromQLResult::InstantVector(series) => {
                assert_eq!(series.len(), 1);
                // "verb" label should not exist
                let verb = series[0].labels.iter().find(|(k, _)| k == "verb");
                assert!(verb.is_none());
            }
            _ => panic!("expected InstantVector"),
        }
    }

    #[test]
    fn task_18_label_replace_capture_group() {
        let store = make_store();
        // Extract first 3 chars via capture group
        let result = evaluate_instant(
            r#"label_replace(http_requests_total{method="POST"}, "short", "$1", "method", "(...).*")"#,
            &store,
            9000,
        )
        .unwrap();
        match result {
            PromQLResult::InstantVector(series) => {
                assert_eq!(series.len(), 1);
                let short = series[0]
                    .labels
                    .iter()
                    .find(|(k, _)| k == "short")
                    .map(|(_, v)| v.as_str());
                assert_eq!(short, Some("POS"));
            }
            _ => panic!("expected InstantVector"),
        }
    }

    #[test]
    fn task_18_label_join_basic() {
        let store = make_store();
        // Join method and service with "-"
        let result = evaluate_instant(
            r#"label_join(http_requests_total{method="GET"}, "combined", "-", "method", "service")"#,
            &store,
            9000,
        )
        .unwrap();
        match result {
            PromQLResult::InstantVector(series) => {
                assert_eq!(series.len(), 1);
                let combined = series[0]
                    .labels
                    .iter()
                    .find(|(k, _)| k == "combined")
                    .map(|(_, v)| v.as_str());
                assert_eq!(combined, Some("GET-api"));
            }
            _ => panic!("expected InstantVector"),
        }
    }

    #[test]
    fn task_18_label_join_single_source() {
        let store = make_store();
        // Join with single source label
        let result = evaluate_instant(
            r#"label_join(http_requests_total{method="GET"}, "copy", "", "method")"#,
            &store,
            9000,
        )
        .unwrap();
        match result {
            PromQLResult::InstantVector(series) => {
                assert_eq!(series.len(), 1);
                let copy = series[0]
                    .labels
                    .iter()
                    .find(|(k, _)| k == "copy")
                    .map(|(_, v)| v.as_str());
                assert_eq!(copy, Some("GET"));
            }
            _ => panic!("expected InstantVector"),
        }
    }

    #[test]
    fn task_18_label_replace_static_replacement() {
        let store = make_store();
        // Static replacement (no capture groups)
        let result = evaluate_instant(
            r#"label_replace(http_requests_total{method="GET"}, "env", "production", "method", ".*")"#,
            &store,
            9000,
        )
        .unwrap();
        match result {
            PromQLResult::InstantVector(series) => {
                assert_eq!(series.len(), 1);
                let env = series[0]
                    .labels
                    .iter()
                    .find(|(k, _)| k == "env")
                    .map(|(_, v)| v.as_str());
                assert_eq!(env, Some("production"));
            }
            _ => panic!("expected InstantVector"),
        }
    }

    #[test]
    fn test_rate_with_counter_reset() {
        // Counter: 0, 10, 20, 5 (reset!), 15
        let samples = vec![
            Sample {
                timestamp_ms: 1000,
                value: 0.0,
            },
            Sample {
                timestamp_ms: 2000,
                value: 10.0,
            },
            Sample {
                timestamp_ms: 3000,
                value: 20.0,
            },
            Sample {
                timestamp_ms: 4000,
                value: 5.0,
            }, // reset
            Sample {
                timestamp_ms: 5000,
                value: 15.0,
            },
        ];
        // Total increase = 10 + 10 + 5 + 10 = 35, over 4s = 8.75/s
        let result = compute_rate_like("rate", &samples, 5000);
        assert!(result.is_some());
        let val = result.unwrap();
        assert!(
            (val - 8.75).abs() < 0.01,
            "rate should be ~8.75, got {}",
            val
        );
    }

    #[test]
    fn test_rate_no_reset() {
        let samples = vec![
            Sample {
                timestamp_ms: 1000,
                value: 0.0,
            },
            Sample {
                timestamp_ms: 5000,
                value: 100.0,
            },
        ];
        // rate = 100 / 4s = 25/s
        let result = compute_rate_like("rate", &samples, 5000);
        assert!(result.is_some());
        let val = result.unwrap();
        assert!((val - 25.0).abs() < 0.01, "rate should be 25, got {}", val);
    }

    #[test]
    fn test_irate_with_reset() {
        let samples = vec![
            Sample {
                timestamp_ms: 1000,
                value: 100.0,
            },
            Sample {
                timestamp_ms: 2000,
                value: 5.0,
            }, // reset
        ];
        // irate: delta is negative (reset), so use curr.value = 5, dt = 1s -> 5/s
        let result = compute_rate_like("irate", &samples, 2000);
        assert!(result.is_some());
        let val = result.unwrap();
        assert!((val - 5.0).abs() < 0.01, "irate should be 5, got {}", val);
    }
}
