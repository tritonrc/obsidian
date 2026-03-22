//! LogQL parser using nom combinators.
//!
//! Supports stream selectors, pipeline stages, and metric queries.

use nom::{
    IResult, Parser,
    branch::alt,
    bytes::tag,
    character::{char, multispace0},
    combinator::map,
    multi::separated_list0,
};
use regex::Regex;
use std::time::Duration;
use thiserror::Error;

use crate::config::parse_duration;

/// LogQL parse errors.
#[derive(Debug, Error)]
pub enum LogQLParseError {
    #[error("parse error: {0}")]
    Parse(String),
}

/// Top-level LogQL expression.
#[derive(Debug, Clone)]
pub enum LogQLExpr {
    /// Stream selector: `{ label_matchers }`.
    StreamSelector { matchers: Vec<LogQLMatcher> },
    /// Pipeline: selector followed by filter stages.
    Pipeline {
        selector: Box<LogQLExpr>,
        stages: Vec<PipelineStage>,
    },
    /// Metric query: `func(selector [range])`.
    MetricQuery {
        function: MetricFunc,
        inner: Box<LogQLExpr>,
        range: Duration,
    },
}

/// A label matcher in a stream selector.
#[derive(Debug, Clone, PartialEq)]
pub struct LogQLMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
}

/// Match operator for labels.
#[derive(Debug, Clone, PartialEq)]
pub enum MatchOp {
    Eq,       // =
    Neq,      // !=
    Regex,    // =~
    NotRegex, // !~
}

/// Pipeline filter stage.
#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
pub enum PipelineStage {
    LineContains(String),        // |= "text"
    LineNotContains(String),     // != "text"
    LineRegex(String, Regex),    // |~ "regex"
    LineNotRegex(String, Regex), // !~ "regex"
    JsonExtract,                 // | json
    LogfmtExtract,               // | logfmt
    LabelFilter {
        // | key="value"
        key: String,
        op: MatchOp,
        value: String,
        /// Pre-compiled regex for Regex/NotRegex match ops, None for Eq/Neq.
        compiled_regex: Option<Regex>,
    },
}

/// Metric functions over log streams.
#[derive(Debug, Clone, PartialEq)]
pub enum MetricFunc {
    CountOverTime,
    Rate,
    BytesOverTime,
    SumOverTime,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
}

/// Parse a LogQL expression.
pub fn parse_logql(input: &str) -> Result<LogQLExpr, LogQLParseError> {
    let input = input.trim();

    // Try metric query first
    if let Ok((_, expr)) = parse_metric_query(input) {
        return Ok(expr);
    }

    // Try pipeline or stream selector
    match parse_pipeline_or_selector(input) {
        Ok((remaining, expr)) => {
            let remaining = remaining.trim();
            if remaining.is_empty() {
                Ok(expr)
            } else {
                Err(LogQLParseError::Parse(format!(
                    "unexpected trailing input: {}",
                    remaining
                )))
            }
        }
        Err(e) => Err(LogQLParseError::Parse(format!("{}", e))),
    }
}

fn parse_metric_query(input: &str) -> IResult<&str, LogQLExpr> {
    let (input, func) = alt((
        map(tag("count_over_time"), |_| MetricFunc::CountOverTime),
        map(tag("bytes_over_time"), |_| MetricFunc::BytesOverTime),
        map(tag("sum_over_time"), |_| MetricFunc::SumOverTime),
        map(tag("avg_over_time"), |_| MetricFunc::AvgOverTime),
        map(tag("min_over_time"), |_| MetricFunc::MinOverTime),
        map(tag("max_over_time"), |_| MetricFunc::MaxOverTime),
        map(tag("rate"), |_| MetricFunc::Rate),
    ))
    .parse_complete(input)?;

    let (input, _) = char('(').parse_complete(input)?;
    let (input, _) = multispace0().parse_complete(input)?;
    let (input, inner) = parse_pipeline_or_selector(input)?;
    let (input, _) = multispace0().parse_complete(input)?;
    let (input, range) = parse_range(input)?;
    let (input, _) = multispace0().parse_complete(input)?;
    let (input, _) = char(')').parse_complete(input)?;

    Ok((
        input,
        LogQLExpr::MetricQuery {
            function: func,
            inner: Box::new(inner),
            range,
        },
    ))
}

fn parse_range(input: &str) -> IResult<&str, Duration> {
    let (input, _) = char('[').parse_complete(input)?;
    let (input, dur_str) =
        nom::bytes::take_while1(|c: char| c.is_alphanumeric()).parse_complete(input)?;
    let (input, _) = char(']').parse_complete(input)?;

    let duration = parse_duration(dur_str).ok_or_else(|| {
        nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Fail))
    })?;

    Ok((input, duration))
}

fn parse_pipeline_or_selector(input: &str) -> IResult<&str, LogQLExpr> {
    let (input, selector) = parse_stream_selector(input)?;
    let (input, _) = multispace0().parse_complete(input)?;

    // Try to parse pipeline stages
    let mut stages = Vec::new();
    let mut remaining = input;
    loop {
        let trimmed = remaining.trim_start();
        if let Ok((rest, stage)) = parse_pipeline_stage(trimmed) {
            stages.push(stage);
            remaining = rest;
        } else {
            break;
        }
    }

    if stages.is_empty() {
        Ok((remaining, selector))
    } else {
        Ok((
            remaining,
            LogQLExpr::Pipeline {
                selector: Box::new(selector),
                stages,
            },
        ))
    }
}

fn parse_stream_selector(input: &str) -> IResult<&str, LogQLExpr> {
    let (input, _) = multispace0().parse_complete(input)?;
    let (input, _) = char('{').parse_complete(input)?;
    let (input, _) = multispace0().parse_complete(input)?;
    let (input, matchers) = separated_list0(char(','), parse_matcher).parse_complete(input)?;
    let (input, _) = multispace0().parse_complete(input)?;
    let (input, _) = char('}').parse_complete(input)?;

    Ok((input, LogQLExpr::StreamSelector { matchers }))
}

fn parse_matcher(input: &str) -> IResult<&str, LogQLMatcher> {
    let (input, _) = multispace0().parse_complete(input)?;
    let (input, name) =
        nom::bytes::take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '.')
            .parse_complete(input)?;
    let (input, _) = multispace0().parse_complete(input)?;
    let (input, op) = alt((
        map(tag("=~"), |_| MatchOp::Regex),
        map(tag("!~"), |_| MatchOp::NotRegex),
        map(tag("!="), |_| MatchOp::Neq),
        map(tag("="), |_| MatchOp::Eq),
    ))
    .parse_complete(input)?;
    let (input, _) = multispace0().parse_complete(input)?;
    let (input, value) = parse_quoted_string(input)?;

    Ok((
        input,
        LogQLMatcher {
            name: name.to_string(),
            op,
            value,
        },
    ))
}

fn parse_quoted_string(input: &str) -> IResult<&str, String> {
    let (input, _) = char('"').parse_complete(input)?;
    let mut result = String::new();
    let mut chars = input.char_indices();
    loop {
        match chars.next() {
            Some((i, '"')) => {
                return Ok((&input[i + 1..], result));
            }
            Some((_, '\\')) => {
                if let Some((_, c)) = chars.next() {
                    match c {
                        'n' => result.push('\n'),
                        't' => result.push('\t'),
                        '\\' => result.push('\\'),
                        '"' => result.push('"'),
                        other => {
                            result.push('\\');
                            result.push(other);
                        }
                    }
                }
            }
            Some((_, c)) => result.push(c),
            None => {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Char,
                )));
            }
        }
    }
}

fn parse_pipeline_stage(input: &str) -> IResult<&str, PipelineStage> {
    alt((
        parse_line_contains,
        parse_line_not_contains,
        parse_line_regex,
        parse_line_not_regex,
        parse_json_or_label_filter,
    ))
    .parse(input)
}

fn parse_json_or_label_filter(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = char('|').parse_complete(input)?;
    let (input, _) = multispace0().parse_complete(input)?;

    // Try "json" keyword
    if let Ok((rest, _)) = tag::<&str, &str, nom::error::Error<&str>>("json").parse_complete(input)
    {
        // Make sure "json" is not part of a longer identifier
        let next = rest.chars().next();
        if next.is_none() || (!next.unwrap().is_alphanumeric() && next.unwrap() != '_') {
            return Ok((rest, PipelineStage::JsonExtract));
        }
    }

    // Try "logfmt" keyword
    if let Ok((rest, _)) =
        tag::<&str, &str, nom::error::Error<&str>>("logfmt").parse_complete(input)
    {
        let next = rest.chars().next();
        if next.is_none() || (!next.unwrap().is_alphanumeric() && next.unwrap() != '_') {
            return Ok((rest, PipelineStage::LogfmtExtract));
        }
    }

    // Otherwise parse label filter: key op "value"
    let (input, key) =
        nom::bytes::take_while1(|c: char| c.is_alphanumeric() || c == '_').parse_complete(input)?;
    let (input, _) = multispace0().parse_complete(input)?;
    let (input, op) = alt((
        map(tag("=~"), |_| MatchOp::Regex),
        map(tag("!~"), |_| MatchOp::NotRegex),
        map(tag("!="), |_| MatchOp::Neq),
        map(tag("="), |_| MatchOp::Eq),
    ))
    .parse_complete(input)?;
    let (input, _) = multispace0().parse_complete(input)?;
    let (input, value) = parse_quoted_string(input)?;

    let compiled_regex = match op {
        MatchOp::Regex | MatchOp::NotRegex => {
            let re = Regex::new(&value).map_err(|_| {
                nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?;
            Some(re)
        }
        _ => None,
    };

    Ok((
        input,
        PipelineStage::LabelFilter {
            key: key.to_string(),
            op,
            value,
            compiled_regex,
        },
    ))
}

fn parse_line_contains(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("|=").parse_complete(input)?;
    let (input, _) = multispace0().parse_complete(input)?;
    let (input, pattern) = parse_quoted_string(input)?;
    Ok((input, PipelineStage::LineContains(pattern)))
}

fn parse_line_not_contains(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("!=").parse_complete(input)?;
    let (input, _) = multispace0().parse_complete(input)?;
    let (input, pattern) = parse_quoted_string(input)?;
    Ok((input, PipelineStage::LineNotContains(pattern)))
}

fn parse_line_regex(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("|~").parse_complete(input)?;
    let (input, _) = multispace0().parse_complete(input)?;
    let (input, pattern) = parse_quoted_string(input)?;
    let re = Regex::new(&pattern).map_err(|_| {
        nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
    })?;
    Ok((input, PipelineStage::LineRegex(pattern, re)))
}

fn parse_line_not_regex(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("!~").parse_complete(input)?;
    let (input, _) = multispace0().parse_complete(input)?;
    let (input, pattern) = parse_quoted_string(input)?;
    let re = Regex::new(&pattern).map_err(|_| {
        nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
    })?;
    Ok((input, PipelineStage::LineNotRegex(pattern, re)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_selector_simple() {
        let expr = parse_logql(r#"{service="payments"}"#).unwrap();
        match expr {
            LogQLExpr::StreamSelector { matchers } => {
                assert_eq!(matchers.len(), 1);
                assert_eq!(matchers[0].name, "service");
                assert_eq!(matchers[0].op, MatchOp::Eq);
                assert_eq!(matchers[0].value, "payments");
            }
            _ => panic!("expected StreamSelector"),
        }
    }

    #[test]
    fn test_stream_selector_multiple_matchers() {
        let expr = parse_logql(r#"{service="payments", level="error"}"#).unwrap();
        match expr {
            LogQLExpr::StreamSelector { matchers } => {
                assert_eq!(matchers.len(), 2);
            }
            _ => panic!("expected StreamSelector"),
        }
    }

    #[test]
    fn test_stream_selector_regex() {
        let expr = parse_logql(r#"{service=~"pay.*"}"#).unwrap();
        match expr {
            LogQLExpr::StreamSelector { matchers } => {
                assert_eq!(matchers[0].op, MatchOp::Regex);
            }
            _ => panic!("expected StreamSelector"),
        }
    }

    #[test]
    fn test_stream_selector_neq() {
        let expr = parse_logql(r#"{level!="debug"}"#).unwrap();
        match expr {
            LogQLExpr::StreamSelector { matchers } => {
                assert_eq!(matchers[0].op, MatchOp::Neq);
            }
            _ => panic!("expected StreamSelector"),
        }
    }

    #[test]
    fn test_stream_selector_not_regex() {
        let expr = parse_logql(r#"{level!~"debug|trace"}"#).unwrap();
        match expr {
            LogQLExpr::StreamSelector { matchers } => {
                assert_eq!(matchers[0].op, MatchOp::NotRegex);
            }
            _ => panic!("expected StreamSelector"),
        }
    }

    #[test]
    fn test_pipeline_line_contains() {
        let expr = parse_logql(r#"{service="payments"} |= "timeout""#).unwrap();
        match expr {
            LogQLExpr::Pipeline { stages, .. } => {
                assert_eq!(stages.len(), 1);
                assert!(matches!(&stages[0], PipelineStage::LineContains(s) if s == "timeout"));
            }
            _ => panic!("expected Pipeline"),
        }
    }

    #[test]
    fn test_pipeline_line_not_contains() {
        let expr = parse_logql(r#"{service="payments"} != "healthcheck""#).unwrap();
        match expr {
            LogQLExpr::Pipeline { stages, .. } => {
                assert!(
                    matches!(&stages[0], PipelineStage::LineNotContains(s) if s == "healthcheck")
                );
            }
            _ => panic!("expected Pipeline"),
        }
    }

    #[test]
    fn test_pipeline_line_regex() {
        let expr = parse_logql(r#"{service="payments"} |~ "error|warn""#).unwrap();
        match expr {
            LogQLExpr::Pipeline { stages, .. } => {
                assert!(matches!(&stages[0], PipelineStage::LineRegex(s, _) if s == "error|warn"));
            }
            _ => panic!("expected Pipeline"),
        }
    }

    #[test]
    fn test_pipeline_line_not_regex() {
        let expr = parse_logql(r#"{service="payments"} !~ "debug|trace""#).unwrap();
        match expr {
            LogQLExpr::Pipeline { stages, .. } => {
                assert!(
                    matches!(&stages[0], PipelineStage::LineNotRegex(s, _) if s == "debug|trace")
                );
            }
            _ => panic!("expected Pipeline"),
        }
    }

    #[test]
    fn test_metric_count_over_time() {
        let expr = parse_logql(r#"count_over_time({service="payments"}[5m])"#).unwrap();
        match expr {
            LogQLExpr::MetricQuery {
                function, range, ..
            } => {
                assert_eq!(function, MetricFunc::CountOverTime);
                assert_eq!(range, Duration::from_secs(300));
            }
            _ => panic!("expected MetricQuery"),
        }
    }

    #[test]
    fn test_metric_rate() {
        let expr = parse_logql(r#"rate({service="payments"} |= "error" [1m])"#).unwrap();
        match expr {
            LogQLExpr::MetricQuery {
                function,
                range,
                inner,
            } => {
                assert_eq!(function, MetricFunc::Rate);
                assert_eq!(range, Duration::from_secs(60));
                match *inner {
                    LogQLExpr::Pipeline { stages, .. } => {
                        assert_eq!(stages.len(), 1);
                    }
                    _ => panic!("expected Pipeline inner"),
                }
            }
            _ => panic!("expected MetricQuery"),
        }
    }

    #[test]
    fn test_metric_bytes_over_time() {
        let expr = parse_logql(r#"bytes_over_time({service="payments"}[5m])"#).unwrap();
        match expr {
            LogQLExpr::MetricQuery { function, .. } => {
                assert_eq!(function, MetricFunc::BytesOverTime);
            }
            _ => panic!("expected MetricQuery"),
        }
    }
}
