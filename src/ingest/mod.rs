//! Ingestion pipeline: Loki push, OTLP metrics, OTLP traces.

pub mod label;
pub mod loki;
pub mod otlp_metrics;
pub mod otlp_traces;
