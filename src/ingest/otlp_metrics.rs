//! OTLP/HTTP metrics ingestion handler.
//!
//! Decodes `ExportMetricsServiceRequest` protobuf and stores metrics.

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use prost::Message;

use super::label::{extract_resource_labels, promote_service_name};
use super::{decode_body, is_json_content_type};
use crate::store::SharedState;
use crate::store::metric_store::Sample;

/// Handler for POST /v1/metrics.
///
/// Accepts both protobuf (`application/x-protobuf`, default) and JSON
/// (`application/json`) encoded `ExportMetricsServiceRequest` bodies.
pub async fn metrics_handler(
    State(state): State<SharedState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let body = match decode_body(&headers, &body) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!("failed to decode OTLP metrics body: {}", e);
            return StatusCode::BAD_REQUEST.into_response();
        }
    };

    let request = if is_json_content_type(&headers) {
        match serde_json::from_slice::<ExportMetricsServiceRequest>(body.as_ref()) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("failed to decode OTLP metrics JSON: {}", e);
                return StatusCode::BAD_REQUEST.into_response();
            }
        }
    } else {
        match ExportMetricsServiceRequest::decode(body.as_ref()) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("failed to decode OTLP metrics: {}", e);
                return StatusCode::BAD_REQUEST.into_response();
            }
        }
    };

    type MetricData = (String, Vec<(String, String)>, Vec<Sample>);
    let mut prepared: Vec<MetricData> = Vec::new();

    for resource_metrics in &request.resource_metrics {
        let mut resource_labels = extract_resource_labels(&resource_metrics.resource);
        promote_service_name(&mut resource_labels);

        for scope_metrics in &resource_metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                let metric_name = &metric.name;

                match &metric.data {
                    Some(Data::Gauge(gauge)) => {
                        for dp in &gauge.data_points {
                            let labels = build_dp_labels(&resource_labels, &dp.attributes);
                            let value = extract_number_value(dp);
                            let ts_ms = dp.time_unix_nano as i64 / 1_000_000;
                            prepared.push((
                                metric_name.clone(),
                                labels,
                                vec![Sample {
                                    timestamp_ms: ts_ms,
                                    value,
                                }],
                            ));
                        }
                    }
                    Some(Data::Sum(sum)) => {
                        for dp in &sum.data_points {
                            let labels = build_dp_labels(&resource_labels, &dp.attributes);
                            let value = extract_number_value(dp);
                            let ts_ms = dp.time_unix_nano as i64 / 1_000_000;
                            prepared.push((
                                metric_name.clone(),
                                labels,
                                vec![Sample {
                                    timestamp_ms: ts_ms,
                                    value,
                                }],
                            ));
                        }
                    }
                    Some(Data::Histogram(hist)) => {
                        for dp in &hist.data_points {
                            let base_labels = build_dp_labels(&resource_labels, &dp.attributes);
                            let ts_ms = dp.time_unix_nano as i64 / 1_000_000;

                            // Store each bucket as a separate series with `le` label
                            let mut cumulative_count: u64 = 0;
                            for (i, &count) in dp.bucket_counts.iter().enumerate() {
                                cumulative_count += count;
                                let le = if i < dp.explicit_bounds.len() {
                                    format!("{}", dp.explicit_bounds[i])
                                } else {
                                    "+Inf".to_string()
                                };
                                let mut labels = base_labels.clone();
                                labels.push(("le".to_string(), le));
                                let bucket_name = format!("{}_bucket", metric_name);
                                prepared.push((
                                    bucket_name,
                                    labels,
                                    vec![Sample {
                                        timestamp_ms: ts_ms,
                                        value: cumulative_count as f64,
                                    }],
                                ));
                            }

                            // Store _sum and _count
                            prepared.push((
                                format!("{}_sum", metric_name),
                                base_labels.clone(),
                                vec![Sample {
                                    timestamp_ms: ts_ms,
                                    value: dp.sum.unwrap_or(0.0),
                                }],
                            ));
                            prepared.push((
                                format!("{}_count", metric_name),
                                base_labels,
                                vec![Sample {
                                    timestamp_ms: ts_ms,
                                    value: dp.count as f64,
                                }],
                            ));
                        }
                    }
                    Some(Data::ExponentialHistogram(exp_hist)) => {
                        for dp in &exp_hist.data_points {
                            let base_labels = build_dp_labels(&resource_labels, &dp.attributes);
                            let ts_ms = dp.time_unix_nano as i64 / 1_000_000;

                            // Convert exponential buckets to explicit boundaries.
                            // base = 2^(2^-scale)
                            let base = 2.0_f64.powf(2.0_f64.powi(-dp.scale));
                            let mut boundaries: Vec<f64> = Vec::new();
                            let mut bucket_counts: Vec<u64> = Vec::new();

                            // Zero bucket: covers values near zero.
                            // Its upper boundary is base^(positive_offset), the lower
                            // edge of the first positive bucket.
                            if let Some(pos) = &dp.positive {
                                let zero_upper = base.powf(pos.offset as f64);
                                boundaries.push(zero_upper);
                                bucket_counts.push(dp.zero_count);

                                for (i, &count) in pos.bucket_counts.iter().enumerate() {
                                    let idx = pos.offset + i as i32;
                                    let upper = base.powf((idx + 1) as f64);
                                    boundaries.push(upper);
                                    bucket_counts.push(count);
                                }
                            } else {
                                // No positive buckets — just a zero bucket
                                bucket_counts.push(dp.zero_count);
                            }

                            // Emit cumulative bucket series (like explicit histogram)
                            let mut cumulative_count: u64 = 0;
                            for (i, &count) in bucket_counts.iter().enumerate() {
                                cumulative_count += count;
                                let le = if i < boundaries.len() {
                                    format!("{}", boundaries[i])
                                } else {
                                    "+Inf".to_string()
                                };
                                let mut labels = base_labels.clone();
                                labels.push(("le".to_string(), le));
                                let bucket_name = format!("{}_bucket", metric_name);
                                prepared.push((
                                    bucket_name,
                                    labels,
                                    vec![Sample {
                                        timestamp_ms: ts_ms,
                                        value: cumulative_count as f64,
                                    }],
                                ));
                            }

                            // Always emit a +Inf bucket if the last bucket wasn't +Inf
                            if !boundaries.is_empty() {
                                let mut labels = base_labels.clone();
                                labels.push(("le".to_string(), "+Inf".to_string()));
                                prepared.push((
                                    format!("{}_bucket", metric_name),
                                    labels,
                                    vec![Sample {
                                        timestamp_ms: ts_ms,
                                        value: dp.count as f64,
                                    }],
                                ));
                            }

                            // Store _sum and _count
                            prepared.push((
                                format!("{}_sum", metric_name),
                                base_labels.clone(),
                                vec![Sample {
                                    timestamp_ms: ts_ms,
                                    value: dp.sum.unwrap_or(0.0),
                                }],
                            ));
                            prepared.push((
                                format!("{}_count", metric_name),
                                base_labels,
                                vec![Sample {
                                    timestamp_ms: ts_ms,
                                    value: dp.count as f64,
                                }],
                            ));
                        }
                    }
                    Some(Data::Summary(summary)) => {
                        for dp in &summary.data_points {
                            let base_labels = build_dp_labels(&resource_labels, &dp.attributes);
                            let ts_ms = dp.time_unix_nano as i64 / 1_000_000;

                            // Store each quantile as a separate series
                            for qv in &dp.quantile_values {
                                let mut labels = base_labels.clone();
                                labels.push(("quantile".to_string(), format!("{}", qv.quantile)));
                                prepared.push((
                                    metric_name.clone(),
                                    labels,
                                    vec![Sample {
                                        timestamp_ms: ts_ms,
                                        value: qv.value,
                                    }],
                                ));
                            }

                            // Store _sum and _count
                            prepared.push((
                                format!("{}_sum", metric_name),
                                base_labels.clone(),
                                vec![Sample {
                                    timestamp_ms: ts_ms,
                                    value: dp.sum,
                                }],
                            ));
                            prepared.push((
                                format!("{}_count", metric_name),
                                base_labels,
                                vec![Sample {
                                    timestamp_ms: ts_ms,
                                    value: dp.count as f64,
                                }],
                            ));
                        }
                    }
                    _ => {
                        tracing::debug!("unsupported metric type for {}", metric_name);
                    }
                }
            }
        }
    }

    let series_count = prepared.len();
    let sample_count: usize = prepared.iter().map(|(_, _, samples)| samples.len()).sum();

    let mut store = state.metric_store.write();
    for (name, labels, samples) in prepared {
        store.ingest_samples(&name, labels, samples);
    }

    Json(serde_json::json!({
        "accepted": {
            "series": series_count,
            "samples": sample_count,
        }
    }))
    .into_response()
}

fn build_dp_labels(
    resource_labels: &[(String, String)],
    attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue],
) -> Vec<(String, String)> {
    if attrs.is_empty() {
        return resource_labels.to_vec();
    }
    let mut labels = resource_labels.to_vec();
    for attr in attrs {
        if let Some(val) = &attr.value
            && let Some(s) = any_value_to_string(val)
        {
            labels.push((attr.key.clone(), s));
        }
    }
    labels
}

fn extract_number_value(dp: &opentelemetry_proto::tonic::metrics::v1::NumberDataPoint) -> f64 {
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
    match &dp.value {
        Some(Value::AsDouble(d)) => *d,
        Some(Value::AsInt(i)) => *i as f64,
        None => 0.0,
    }
}

fn any_value_to_string(val: &opentelemetry_proto::tonic::common::v1::AnyValue) -> Option<String> {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    match &val.value {
        Some(Value::StringValue(s)) => Some(s.clone()),
        Some(Value::IntValue(i)) => Some(i.to_string()),
        Some(Value::DoubleValue(f)) => Some(f.to_string()),
        Some(Value::BoolValue(b)) => Some(b.to_string()),
        _ => None,
    }
}
