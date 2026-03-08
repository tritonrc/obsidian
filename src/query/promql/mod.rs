//! PromQL query engine: evaluator and HTTP handlers.
//!
//! Uses `promql-parser` crate for parsing; we only implement evaluation.

pub mod eval;
pub mod handlers;
