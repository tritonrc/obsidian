//! LogQL parser using nom combinators.
//!
//! Supports stream selectors, pipeline stages, and metric queries.

use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{char, multispace0},
    combinator::map,
    multi::separated_list0,
    sequence::delimited,
    IResult,
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
}

/// Metric functions over log streams.
#[derive(Debug, Clone, PartialEq)]
pub enum MetricFunc {
    CountOverTime,
    Rate,
    BytesOverTime,
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
        map(tag("rate"), |_| MetricFunc::Rate),
        map(tag("bytes_over_time"), |_| MetricFunc::BytesOverTime),
    ))(input)?;

    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, inner) = parse_pipeline_or_selector(input)?;
    let (input, _) = multispace0(input)?;
    let (input, range) = parse_range(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;

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
    let (input, _) = char('[')(input)?;
    let (input, dur_str) = take_while1(|c: char| c.is_alphanumeric())(input)?;
    let (input, _) = char(']')(input)?;

    let duration = parse_duration(dur_str).ok_or_else(|| {
        nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Fail))
    })?;

    Ok((input, duration))
}

fn parse_pipeline_or_selector(input: &str) -> IResult<&str, LogQLExpr> {
    let (input, selector) = parse_stream_selector(input)?;
    let (input, _) = multispace0(input)?;

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
    let (input, _) = multispace0(input)?;
    let (input, _) = char('{')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, matchers) = separated_list0(
        delimited(multispace0, char(','), multispace0),
        parse_matcher,
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('}')(input)?;

    Ok((input, LogQLExpr::StreamSelector { matchers }))
}

fn parse_matcher(input: &str) -> IResult<&str, LogQLMatcher> {
    let (input, _) = multispace0(input)?;
    let (input, name) = take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '.')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, op) = alt((
        map(tag("=~"), |_| MatchOp::Regex),
        map(tag("!~"), |_| MatchOp::NotRegex),
        map(tag("!="), |_| MatchOp::Neq),
        map(tag("="), |_| MatchOp::Eq),
    ))(input)?;
    let (input, _) = multispace0(input)?;
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
    let (input, _) = char('"')(input)?;
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
    ))(input)
}

fn parse_line_contains(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("|=")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, pattern) = parse_quoted_string(input)?;
    Ok((input, PipelineStage::LineContains(pattern)))
}

fn parse_line_not_contains(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("!=")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, pattern) = parse_quoted_string(input)?;
    Ok((input, PipelineStage::LineNotContains(pattern)))
}

fn parse_line_regex(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("|~")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, pattern) = parse_quoted_string(input)?;
    let re = Regex::new(&pattern).map_err(|_| {
        nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
    })?;
    Ok((input, PipelineStage::LineRegex(pattern, re)))
}

fn parse_line_not_regex(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("!~")(input)?;
    let (input, _) = multispace0(input)?;
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
