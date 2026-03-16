//! TraceQL parser using nom combinators.
//!
//! Supports span selectors with attribute matching, duration filters,
//! logical operators, and structural operators.

use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{char, multispace0},
    combinator::map,
};
use std::time::Duration;
use thiserror::Error;

/// TraceQL parse errors.
#[derive(Debug, Error)]
pub enum TraceQLParseError {
    #[error("parse error: {0}")]
    Parse(String),
}

/// Top-level TraceQL expression.
#[derive(Debug, Clone, PartialEq)]
pub enum TraceQLExpr {
    /// A span selector: `{ conditions }`.
    /// `logical_ops[i]` is the operator between `conditions[i]` and `conditions[i+1]`.
    SpanSelector {
        conditions: Vec<SpanCondition>,
        logical_ops: Vec<LogicalOp>,
    },
    /// Structural operator between two selectors.
    Structural {
        op: StructuralOp,
        lhs: Box<TraceQLExpr>,
        rhs: Box<TraceQLExpr>,
    },
    /// Pipeline with aggregate filter: `{...} | count() > N`.
    Pipeline {
        inner: Box<TraceQLExpr>,
        pipeline_stages: Vec<PipelineStage>,
    },
}

/// A pipeline stage applied after span matching.
#[derive(Debug, Clone, PartialEq)]
pub enum PipelineStage {
    /// `count() op value` — filter traces by the number of matched spans.
    CountFilter { op: CompareOp, value: u64 },
}

/// Structural operators between span selectors.
#[derive(Debug, Clone, PartialEq)]
pub enum StructuralOp {
    /// `>>` descendant
    Descendant,
}

/// A condition within a span selector.
#[derive(Debug, Clone, PartialEq)]
pub enum SpanCondition {
    /// Attribute comparison: `scope.name op value`.
    Attribute {
        scope: AttrScope,
        name: String,
        op: CompareOp,
        value: SpanValue,
    },
    /// Duration comparison: `duration op value`.
    Duration { op: CompareOp, value: Duration },
    /// Status comparison: `status = error|ok|unset`.
    Status {
        op: CompareOp,
        value: SpanStatusValue,
    },
    /// Span name comparison: `name op "value"`.
    Name { op: CompareOp, value: String },
}

/// Attribute scope.
#[derive(Debug, Clone, PartialEq)]
pub enum AttrScope {
    Resource,
    Span,
}

/// Comparison operators.
#[derive(Debug, Clone, PartialEq)]
pub enum CompareOp {
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
    Regex,
}

/// Values in span conditions.
#[derive(Debug, Clone, PartialEq)]
pub enum SpanValue {
    String(String),
    Int(i64),
    Float(f64),
}

/// Logical operators between conditions.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LogicalOp {
    And,
    Or,
}

/// Span status values.
#[derive(Debug, Clone, PartialEq)]
pub enum SpanStatusValue {
    Ok,
    Error,
    Unset,
}

/// Parse a TraceQL expression.
pub fn parse_traceql(input: &str) -> Result<TraceQLExpr, TraceQLParseError> {
    let input = input.trim();
    match parse_top_level(input) {
        Ok((remaining, expr)) => {
            // Try to parse pipeline stages from remaining input
            let remaining = remaining.trim();
            let (remaining, stages) = parse_pipeline_stages(remaining);
            let remaining = remaining.trim();
            if remaining.is_empty() {
                if stages.is_empty() {
                    Ok(expr)
                } else {
                    Ok(TraceQLExpr::Pipeline {
                        inner: Box::new(expr),
                        pipeline_stages: stages,
                    })
                }
            } else {
                Err(TraceQLParseError::Parse(format!(
                    "unexpected trailing input: {}",
                    remaining
                )))
            }
        }
        Err(e) => Err(TraceQLParseError::Parse(format!("{}", e))),
    }
}

fn parse_top_level(input: &str) -> IResult<&str, TraceQLExpr> {
    let (input, lhs) = parse_span_selector(input)?;
    let (input, _) = multispace0(input)?;

    // Check for structural operator
    if let Ok((input, _)) = tag::<&str, &str, nom::error::Error<&str>>(">>")(input) {
        let (input, _) = multispace0(input)?;
        let (input, rhs) = parse_span_selector(input)?;
        Ok((
            input,
            TraceQLExpr::Structural {
                op: StructuralOp::Descendant,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            },
        ))
    } else {
        Ok((input, lhs))
    }
}

fn parse_span_selector(input: &str) -> IResult<&str, TraceQLExpr> {
    let (input, _) = multispace0(input)?;
    let (input, _) = char('{')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, (conditions, logical_ops)) = parse_conditions(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('}')(input)?;
    Ok((
        input,
        TraceQLExpr::SpanSelector {
            conditions,
            logical_ops,
        },
    ))
}

fn parse_conditions(input: &str) -> IResult<&str, (Vec<SpanCondition>, Vec<LogicalOp>)> {
    // Handle empty selector: `{}`
    let (mut input, first) = match parse_condition(input) {
        Ok((rest, cond)) => (rest, cond),
        Err(_) => return Ok((input, (Vec::new(), Vec::new()))),
    };
    let mut conditions = vec![first];
    let mut logical_ops = Vec::new();

    #[allow(clippy::while_let_loop)]
    loop {
        let trimmed = match multispace0::<&str, nom::error::Error<&str>>(input) {
            Ok((rest, _)) => rest,
            Err(_) => break,
        };
        if let Ok((rest, op_str)) =
            alt((tag::<_, _, nom::error::Error<&str>>("&&"), tag("||"))).parse_complete(trimmed)
        {
            let op = if op_str == "&&" {
                LogicalOp::And
            } else {
                LogicalOp::Or
            };
            let rest = match multispace0::<&str, nom::error::Error<&str>>(rest) {
                Ok((r, _)) => r,
                Err(_) => break,
            };
            match parse_condition(rest) {
                Ok((rest, cond)) => {
                    logical_ops.push(op);
                    conditions.push(cond);
                    input = rest;
                }
                Err(_) => break,
            }
        } else {
            break;
        }
    }

    Ok((input, (conditions, logical_ops)))
}

fn parse_condition(input: &str) -> IResult<&str, SpanCondition> {
    let (input, _) = multispace0(input)?;

    // Try duration first
    if let Ok(result) = parse_duration_condition(input) {
        return Ok(result);
    }

    // Try status
    if let Ok(result) = parse_status_condition(input) {
        return Ok(result);
    }

    // Try name
    if let Ok(result) = parse_name_condition(input) {
        return Ok(result);
    }

    // Try attribute
    parse_attribute_condition(input)
}

fn parse_duration_condition(input: &str) -> IResult<&str, SpanCondition> {
    let (input, _) = tag("duration")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, op) = parse_compare_op(input)?;
    let (input, _) = multispace0(input)?;
    let (input, dur) = parse_duration_value(input)?;
    Ok((input, SpanCondition::Duration { op, value: dur }))
}

fn parse_status_condition(input: &str) -> IResult<&str, SpanCondition> {
    let (input, _) = tag("status")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, op) = parse_compare_op(input)?;
    let (input, _) = multispace0(input)?;
    let (input, status) = alt((
        map(tag("error"), |_| SpanStatusValue::Error),
        map(tag("ok"), |_| SpanStatusValue::Ok),
        map(tag("unset"), |_| SpanStatusValue::Unset),
    ))
    .parse_complete(input)?;
    Ok((input, SpanCondition::Status { op, value: status }))
}

fn parse_name_condition(input: &str) -> IResult<&str, SpanCondition> {
    let (input, _) = tag("name")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, op) = parse_compare_op(input)?;
    let (input, _) = multispace0(input)?;
    let (input, value) = parse_quoted_string(input)?;
    Ok((input, SpanCondition::Name { op, value }))
}

fn parse_attribute_condition(input: &str) -> IResult<&str, SpanCondition> {
    let (input, scope_and_name) =
        take_while1(|c: char| c.is_alphanumeric() || c == '.' || c == '_' || c == '-')(input)?;

    let (scope, attr_name) = if let Some(rest) = scope_and_name.strip_prefix("resource.") {
        (AttrScope::Resource, rest.to_string())
    } else if let Some(rest) = scope_and_name.strip_prefix("span.") {
        (AttrScope::Span, rest.to_string())
    } else {
        // Default to resource scope for dotted names, span otherwise
        if scope_and_name.contains('.') {
            (AttrScope::Resource, scope_and_name.to_string())
        } else {
            (AttrScope::Span, scope_and_name.to_string())
        }
    };

    let (input, _) = multispace0(input)?;
    let (input, op) = parse_compare_op(input)?;
    let (input, _) = multispace0(input)?;
    let (input, value) = parse_span_value(input)?;

    Ok((
        input,
        SpanCondition::Attribute {
            scope,
            name: attr_name,
            op,
            value,
        },
    ))
}

fn parse_compare_op(input: &str) -> IResult<&str, CompareOp> {
    alt((
        map(tag(">="), |_| CompareOp::Gte),
        map(tag("<="), |_| CompareOp::Lte),
        map(tag("!="), |_| CompareOp::Neq),
        map(tag("=~"), |_| CompareOp::Regex),
        map(tag("="), |_| CompareOp::Eq),
        map(tag(">"), |_| CompareOp::Gt),
        map(tag("<"), |_| CompareOp::Lt),
    ))
    .parse_complete(input)
}

fn parse_span_value(input: &str) -> IResult<&str, SpanValue> {
    alt((
        map(parse_quoted_string, SpanValue::String),
        parse_numeric_value,
    ))
    .parse_complete(input)
}

fn parse_numeric_value(input: &str) -> IResult<&str, SpanValue> {
    let (input, num_str) =
        take_while1(|c: char| c.is_ascii_digit() || c == '.' || c == '-')(input)?;

    if num_str.contains('.') {
        let val: f64 = num_str.parse().map_err(|_| {
            nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Float))
        })?;
        Ok((input, SpanValue::Float(val)))
    } else {
        let val: i64 = num_str.parse().map_err(|_| {
            nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
        })?;
        Ok((input, SpanValue::Int(val)))
    }
}

fn parse_quoted_string(input: &str) -> IResult<&str, String> {
    let (input, _) = char('"')(input)?;
    let mut result = String::new();
    let mut chars = input.char_indices();
    loop {
        match chars.next() {
            Some((i, '"')) => return Ok((&input[i + 1..], result)),
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

fn parse_duration_value(input: &str) -> IResult<&str, Duration> {
    let (input, num_str) = take_while1(|c: char| c.is_ascii_digit() || c == '.')(input)?;
    let (input, unit) = alt((tag("ms"), tag("s"), tag("m"), tag("h"))).parse_complete(input)?;

    let num: f64 = num_str.parse().map_err(|_| {
        nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Float))
    })?;

    let duration = match unit {
        "ms" => Duration::from_secs_f64(num / 1000.0),
        "s" => Duration::from_secs_f64(num),
        "m" => Duration::from_secs_f64(num * 60.0),
        "h" => Duration::from_secs_f64(num * 3600.0),
        _ => unreachable!(),
    };

    Ok((input, duration))
}

/// Parse zero or more pipeline stages from the remaining input after the base expression.
/// Each stage is `| count() op value`.
fn parse_pipeline_stages(mut input: &str) -> (&str, Vec<PipelineStage>) {
    let mut stages = Vec::new();
    loop {
        let trimmed = input.trim_start();
        if !trimmed.starts_with('|') {
            break;
        }
        let rest = trimmed[1..].trim_start();
        match parse_count_filter(rest) {
            Ok((remaining, stage)) => {
                stages.push(stage);
                input = remaining;
            }
            Err(_) => break,
        }
    }
    (input, stages)
}

/// Parse `count() op value` where op is a comparison and value is a u64.
fn parse_count_filter(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("count()")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, op) = parse_compare_op(input)?;
    let (input, _) = multispace0(input)?;
    let (input, num_str) = take_while1(|c: char| c.is_ascii_digit())(input)?;
    let value: u64 = num_str.parse().map_err(|_| {
        nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
    })?;
    Ok((input, PipelineStage::CountFilter { op, value }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_attribute_selector() {
        let expr = parse_traceql(r#"{ resource.service.name = "payments" }"#).unwrap();
        match expr {
            TraceQLExpr::SpanSelector {
                conditions,
                logical_ops,
            } => {
                assert_eq!(conditions.len(), 1);
                assert!(logical_ops.is_empty());
                match &conditions[0] {
                    SpanCondition::Attribute {
                        scope,
                        name,
                        op,
                        value,
                    } => {
                        assert_eq!(*scope, AttrScope::Resource);
                        assert_eq!(name, "service.name");
                        assert_eq!(*op, CompareOp::Eq);
                        assert_eq!(*value, SpanValue::String("payments".into()));
                    }
                    _ => panic!("expected Attribute"),
                }
            }
            _ => panic!("expected SpanSelector"),
        }
    }

    #[test]
    fn test_name_selector() {
        let expr = parse_traceql(r#"{ name = "POST /api/transfer" }"#).unwrap();
        match expr {
            TraceQLExpr::SpanSelector { conditions, .. } => match &conditions[0] {
                SpanCondition::Name { op, value } => {
                    assert_eq!(*op, CompareOp::Eq);
                    assert_eq!(value, "POST /api/transfer");
                }
                _ => panic!("expected Name"),
            },
            _ => panic!("expected SpanSelector"),
        }
    }

    #[test]
    fn test_status_selector() {
        let expr = parse_traceql(r#"{ status = error }"#).unwrap();
        match expr {
            TraceQLExpr::SpanSelector { conditions, .. } => match &conditions[0] {
                SpanCondition::Status { op, value } => {
                    assert_eq!(*op, CompareOp::Eq);
                    assert_eq!(*value, SpanStatusValue::Error);
                }
                _ => panic!("expected Status"),
            },
            _ => panic!("expected SpanSelector"),
        }
    }

    #[test]
    fn test_duration_selector() {
        let expr = parse_traceql(r#"{ duration > 500ms }"#).unwrap();
        match expr {
            TraceQLExpr::SpanSelector { conditions, .. } => match &conditions[0] {
                SpanCondition::Duration { op, value } => {
                    assert_eq!(*op, CompareOp::Gt);
                    assert_eq!(*value, Duration::from_millis(500));
                }
                _ => panic!("expected Duration"),
            },
            _ => panic!("expected SpanSelector"),
        }
    }

    #[test]
    fn test_duration_seconds() {
        let expr = parse_traceql(r#"{ duration > 1s }"#).unwrap();
        match expr {
            TraceQLExpr::SpanSelector { conditions, .. } => match &conditions[0] {
                SpanCondition::Duration { op, value } => {
                    assert_eq!(*op, CompareOp::Gt);
                    assert_eq!(*value, Duration::from_secs(1));
                }
                _ => panic!("expected Duration"),
            },
            _ => panic!("expected SpanSelector"),
        }
    }

    #[test]
    fn test_and_conditions() {
        let expr =
            parse_traceql(r#"{ duration > 1s && resource.service.name = "payments" }"#).unwrap();
        match expr {
            TraceQLExpr::SpanSelector {
                conditions,
                logical_ops,
            } => {
                assert_eq!(conditions.len(), 2);
                assert_eq!(logical_ops, vec![LogicalOp::And]);
            }
            _ => panic!("expected SpanSelector"),
        }
    }

    #[test]
    fn test_or_conditions() {
        let expr = parse_traceql(r#"{ status = error || span.http.status_code >= 500 }"#).unwrap();
        match expr {
            TraceQLExpr::SpanSelector {
                conditions,
                logical_ops,
            } => {
                assert_eq!(conditions.len(), 2);
                assert_eq!(logical_ops, vec![LogicalOp::Or]);
            }
            _ => panic!("expected SpanSelector"),
        }
    }

    #[test]
    fn test_structural_descendant() {
        let expr = parse_traceql(
            r#"{ resource.service.name = "api-gateway" } >> { resource.service.name = "payments" }"#,
        )
        .unwrap();
        match expr {
            TraceQLExpr::Structural { op, .. } => {
                assert_eq!(op, StructuralOp::Descendant);
            }
            _ => panic!("expected Structural"),
        }
    }

    #[test]
    fn test_span_attribute_int() {
        let expr = parse_traceql(r#"{ span.http.status_code = 500 }"#).unwrap();
        match expr {
            TraceQLExpr::SpanSelector { conditions, .. } => match &conditions[0] {
                SpanCondition::Attribute { value, .. } => {
                    assert_eq!(*value, SpanValue::Int(500));
                }
                _ => panic!("expected Attribute"),
            },
            _ => panic!("expected SpanSelector"),
        }
    }

    #[test]
    fn test_empty_selector() {
        let expr = parse_traceql("{}").unwrap();
        match expr {
            TraceQLExpr::SpanSelector {
                conditions,
                logical_ops,
            } => {
                assert!(conditions.is_empty());
                assert!(logical_ops.is_empty());
            }
            _ => panic!("expected SpanSelector"),
        }
    }

    #[test]
    fn test_empty_selector_with_spaces() {
        let expr = parse_traceql("{  }").unwrap();
        match expr {
            TraceQLExpr::SpanSelector { conditions, .. } => {
                assert!(conditions.is_empty());
            }
            _ => panic!("expected SpanSelector"),
        }
    }

    #[test]
    fn test_float_duration() {
        let expr = parse_traceql(r#"{ duration > 2.5s }"#).unwrap();
        match expr {
            TraceQLExpr::SpanSelector { conditions, .. } => match &conditions[0] {
                SpanCondition::Duration { value, .. } => {
                    assert_eq!(*value, Duration::from_secs_f64(2.5));
                }
                _ => panic!("expected Duration"),
            },
            _ => panic!("expected SpanSelector"),
        }
    }

    #[test]
    fn test_count_filter_gt() {
        let expr = parse_traceql(r#"{ status = error } | count() > 2"#).unwrap();
        match expr {
            TraceQLExpr::Pipeline {
                inner,
                pipeline_stages,
            } => {
                assert!(matches!(*inner, TraceQLExpr::SpanSelector { .. }));
                assert_eq!(pipeline_stages.len(), 1);
                assert_eq!(
                    pipeline_stages[0],
                    PipelineStage::CountFilter {
                        op: CompareOp::Gt,
                        value: 2
                    }
                );
            }
            _ => panic!("expected Pipeline"),
        }
    }

    #[test]
    fn test_count_filter_gte() {
        let expr = parse_traceql(r#"{} | count() >= 3"#).unwrap();
        match expr {
            TraceQLExpr::Pipeline {
                pipeline_stages, ..
            } => {
                assert_eq!(
                    pipeline_stages[0],
                    PipelineStage::CountFilter {
                        op: CompareOp::Gte,
                        value: 3
                    }
                );
            }
            _ => panic!("expected Pipeline"),
        }
    }

    #[test]
    fn test_count_filter_eq() {
        let expr = parse_traceql(r#"{ status = ok } | count() = 1"#).unwrap();
        match expr {
            TraceQLExpr::Pipeline {
                pipeline_stages, ..
            } => {
                assert_eq!(
                    pipeline_stages[0],
                    PipelineStage::CountFilter {
                        op: CompareOp::Eq,
                        value: 1
                    }
                );
            }
            _ => panic!("expected Pipeline"),
        }
    }
}
