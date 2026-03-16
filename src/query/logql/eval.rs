//! LogQL evaluator against LogStore.

use std::time::Duration;

use rustc_hash::FxHashMap;

use crate::store::log_store::{LabelMatchOp, LabelMatcher, LogStore};

use super::parser::{LogQLExpr, MatchOp, MetricFunc, PipelineStage};

/// Result of a LogQL evaluation.
#[derive(Debug)]
pub enum LogQLResult {
    /// Log stream results.
    Streams(Vec<StreamResult>),
    /// Metric query results (time series).
    Matrix(Vec<MetricResult>),
}

/// A single stream result with entries.
#[derive(Debug)]
pub struct StreamResult {
    pub labels: Vec<(String, String)>,
    pub entries: Vec<(i64, String)>,
}

/// A single metric result (from count_over_time, rate, etc.).
#[derive(Debug)]
pub struct MetricResult {
    pub labels: Vec<(String, String)>,
    pub samples: Vec<(i64, f64)>,
}

/// Evaluate a LogQL expression against the log store.
pub fn evaluate_logql(
    expr: &LogQLExpr,
    store: &LogStore,
    start_ns: i64,
    end_ns: i64,
    step_ns: Option<i64>,
) -> LogQLResult {
    match expr {
        LogQLExpr::StreamSelector { matchers } => {
            let lm = convert_matchers(matchers);
            let stream_ids = store.query_streams(&lm);
            let mut results = Vec::new();
            for sid in stream_ids {
                let entries = store.get_entries(sid, start_ns, end_ns);
                if entries.is_empty() {
                    continue;
                }
                let labels = store.get_stream_labels(sid).unwrap_or_default();
                let entry_tuples: Vec<(i64, String)> = entries
                    .iter()
                    .map(|e| (e.timestamp_ns, e.line.clone()))
                    .collect();
                results.push(StreamResult {
                    labels,
                    entries: entry_tuples,
                });
            }
            LogQLResult::Streams(results)
        }
        LogQLExpr::Pipeline {
            selector, stages, ..
        } => {
            let inner = evaluate_logql(selector, store, start_ns, end_ns, step_ns);
            match inner {
                LogQLResult::Streams(streams) => {
                    let filtered: Vec<StreamResult> = streams
                        .into_iter()
                        .map(|mut sr| {
                            sr.entries.retain(|(_, line)| apply_stages(line, stages));
                            sr
                        })
                        .filter(|sr| !sr.entries.is_empty())
                        .collect();
                    LogQLResult::Streams(filtered)
                }
                other => other,
            }
        }
        LogQLExpr::MetricQuery {
            function,
            inner,
            range,
        } => {
            let step = step_ns.unwrap_or(range.as_nanos() as i64);
            evaluate_metric_query(function, inner, *range, store, start_ns, end_ns, step)
        }
    }
}

fn evaluate_metric_query(
    function: &MetricFunc,
    inner: &LogQLExpr,
    range: Duration,
    store: &LogStore,
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
) -> LogQLResult {
    let range_ns = range.as_nanos() as i64;

    // Get the selector and optional stages
    let (matchers, stages) = extract_selector_and_stages(inner);
    let lm = convert_matchers(&matchers);
    let stream_ids = store.query_streams(&lm);

    let mut results = Vec::new();

    for sid in &stream_ids {
        let labels = store.get_stream_labels(*sid).unwrap_or_default();
        let mut samples = Vec::new();

        let mut t = start_ns;
        while t <= end_ns {
            let window_start = t - range_ns;
            let window_end = t;
            let entries = store.get_entries(*sid, window_start, window_end);

            let filtered: Vec<_> = entries
                .iter()
                .filter(|e| apply_stages(&e.line, &stages))
                .collect();

            let value = match function {
                MetricFunc::CountOverTime => filtered.len() as f64,
                MetricFunc::Rate => {
                    if range_ns > 0 {
                        filtered.len() as f64 / (range_ns as f64 / 1_000_000_000.0)
                    } else {
                        0.0
                    }
                }
                MetricFunc::BytesOverTime => filtered.iter().map(|e| e.line.len() as f64).sum(),
            };

            samples.push((t, value));
            t += step_ns;
        }

        if !samples.is_empty() {
            results.push(MetricResult { labels, samples });
        }
    }

    LogQLResult::Matrix(results)
}

fn extract_selector_and_stages(
    expr: &LogQLExpr,
) -> (Vec<super::parser::LogQLMatcher>, Vec<PipelineStage>) {
    match expr {
        LogQLExpr::StreamSelector { matchers } => (matchers.clone(), Vec::new()),
        LogQLExpr::Pipeline {
            selector, stages, ..
        } => {
            let (matchers, _) = extract_selector_and_stages(selector);
            (matchers, stages.clone())
        }
        _ => (Vec::new(), Vec::new()),
    }
}

fn apply_stages(line: &str, stages: &[PipelineStage]) -> bool {
    let mut extracted: FxHashMap<String, String> = FxHashMap::default();
    for stage in stages {
        match stage {
            PipelineStage::LineContains(pattern) => {
                if !line.contains(pattern.as_str()) {
                    return false;
                }
            }
            PipelineStage::LineNotContains(pattern) => {
                if line.contains(pattern.as_str()) {
                    return false;
                }
            }
            PipelineStage::LineRegex(_, re) => {
                if !re.is_match(line) {
                    return false;
                }
            }
            PipelineStage::LineNotRegex(_, re) => {
                if re.is_match(line) {
                    return false;
                }
            }
            PipelineStage::JsonExtract => {
                let parsed: serde_json::Value = match serde_json::from_str(line) {
                    Ok(v) => v,
                    Err(_) => return false, // drop lines that fail JSON parsing
                };
                if let serde_json::Value::Object(map) = parsed {
                    for (k, v) in map {
                        let val_str = match &v {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        extracted.insert(k, val_str);
                    }
                }
            }
            PipelineStage::LabelFilter { key, op, value } => {
                let label_val = match extracted.get(key.as_str()) {
                    Some(v) => v.as_str(),
                    None => return false, // label not found => filter fails
                };
                let matches = match op {
                    MatchOp::Eq => label_val == value,
                    MatchOp::Neq => label_val != value,
                    MatchOp::Regex => regex::Regex::new(value)
                        .map(|re| re.is_match(label_val))
                        .unwrap_or(false),
                    MatchOp::NotRegex => regex::Regex::new(value)
                        .map(|re| !re.is_match(label_val))
                        .unwrap_or(false),
                };
                if !matches {
                    return false;
                }
            }
        }
    }
    true
}

fn convert_matchers(matchers: &[super::parser::LogQLMatcher]) -> Vec<LabelMatcher> {
    matchers
        .iter()
        .map(|m| LabelMatcher {
            name: m.name.clone(),
            op: match m.op {
                MatchOp::Eq => LabelMatchOp::Eq,
                MatchOp::Neq => LabelMatchOp::Neq,
                MatchOp::Regex => LabelMatchOp::Regex,
                MatchOp::NotRegex => LabelMatchOp::NotRegex,
            },
            value: m.value.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::log_store::LogEntry;

    fn make_store() -> LogStore {
        let mut store = LogStore::new();
        store.ingest_stream(
            vec![
                ("service".into(), "payments".into()),
                ("level".into(), "error".into()),
            ],
            vec![
                LogEntry {
                    timestamp_ns: 1_000_000_000,
                    line: "connection timeout to bank API".into(),
                },
                LogEntry {
                    timestamp_ns: 2_000_000_000,
                    line: "retry 1/3 failed".into(),
                },
                LogEntry {
                    timestamp_ns: 3_000_000_000,
                    line: "healthcheck ok".into(),
                },
            ],
        );
        store.ingest_stream(
            vec![
                ("service".into(), "gateway".into()),
                ("level".into(), "info".into()),
            ],
            vec![LogEntry {
                timestamp_ns: 1_500_000_000,
                line: "request received".into(),
            }],
        );
        store
    }

    #[test]
    fn test_eval_stream_selector() {
        let store = make_store();
        let expr = crate::query::logql::parser::parse_logql(r#"{service="payments"}"#).unwrap();
        let result = evaluate_logql(&expr, &store, 0, i64::MAX, None);
        match result {
            LogQLResult::Streams(streams) => {
                assert_eq!(streams.len(), 1);
                assert_eq!(streams[0].entries.len(), 3);
            }
            _ => panic!("expected Streams"),
        }
    }

    #[test]
    fn test_eval_pipeline_filter() {
        let store = make_store();
        let expr = crate::query::logql::parser::parse_logql(r#"{service="payments"} |= "timeout""#)
            .unwrap();
        let result = evaluate_logql(&expr, &store, 0, i64::MAX, None);
        match result {
            LogQLResult::Streams(streams) => {
                assert_eq!(streams.len(), 1);
                assert_eq!(streams[0].entries.len(), 1);
                assert!(streams[0].entries[0].1.contains("timeout"));
            }
            _ => panic!("expected Streams"),
        }
    }

    #[test]
    fn test_eval_pipeline_not_contains() {
        let store = make_store();
        let expr =
            crate::query::logql::parser::parse_logql(r#"{service="payments"} != "healthcheck""#)
                .unwrap();
        let result = evaluate_logql(&expr, &store, 0, i64::MAX, None);
        match result {
            LogQLResult::Streams(streams) => {
                assert_eq!(streams[0].entries.len(), 2);
            }
            _ => panic!("expected Streams"),
        }
    }

    #[test]
    fn test_eval_count_over_time() {
        let store = make_store();
        let expr = crate::query::logql::parser::parse_logql(
            r#"count_over_time({service="payments"}[10s])"#,
        )
        .unwrap();
        let result = evaluate_logql(
            &expr,
            &store,
            3_000_000_000,
            3_000_000_000,
            Some(10_000_000_000),
        );
        match result {
            LogQLResult::Matrix(results) => {
                assert_eq!(results.len(), 1);
                // At t=3s, window [3s-10s, 3s] = [-7s, 3s], should capture all 3 entries
                assert!(results[0].samples[0].1 >= 3.0);
            }
            _ => panic!("expected Matrix"),
        }
    }
}
