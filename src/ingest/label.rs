//! Shared label extraction and service name promotion utilities.

use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::resource::v1::Resource;

/// Extract labels from an OTLP Resource as (key, value) string pairs.
pub fn extract_resource_labels(resource: &Option<Resource>) -> Vec<(String, String)> {
    match resource {
        Some(r) => extract_key_values(&r.attributes),
        None => Vec::new(),
    }
}

/// Extract string key-value pairs from OTLP KeyValue attributes.
pub fn extract_key_values(attrs: &[KeyValue]) -> Vec<(String, String)> {
    attrs
        .iter()
        .filter_map(|kv| {
            let val = kv.value.as_ref()?;
            let s = any_value_to_string(val)?;
            Some((kv.key.clone(), s))
        })
        .collect()
}

/// Convert an AnyValue to a string representation.
fn any_value_to_string(val: &AnyValue) -> Option<String> {
    match &val.value {
        Some(any_value::Value::StringValue(s)) => Some(s.clone()),
        Some(any_value::Value::IntValue(i)) => Some(i.to_string()),
        Some(any_value::Value::DoubleValue(f)) => Some(f.to_string()),
        Some(any_value::Value::BoolValue(b)) => Some(b.to_string()),
        _ => None,
    }
}

/// Promote `service.name` from resource attributes to a `service` label.
pub fn promote_service_name(labels: &mut Vec<(String, String)>) {
    if let Some(pos) = labels.iter().position(|(k, _)| k == "service.name") {
        let service_value = labels[pos].1.clone();
        // Remove service.name
        labels.remove(pos);
        // Add or replace `service` label
        if let Some(existing) = labels.iter_mut().find(|(k, _)| k == "service") {
            existing.1 = service_value;
        } else {
            labels.push(("service".to_string(), service_value));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_promote_service_name() {
        let mut labels = vec![
            ("service.name".into(), "payments".into()),
            ("host".into(), "server1".into()),
        ];
        promote_service_name(&mut labels);
        assert!(
            labels
                .iter()
                .any(|(k, v)| k == "service" && v == "payments")
        );
        assert!(!labels.iter().any(|(k, _)| k == "service.name"));
    }

    #[test]
    fn test_promote_no_service_name() {
        let mut labels = vec![("host".into(), "server1".into())];
        promote_service_name(&mut labels);
        assert_eq!(labels.len(), 1);
    }
}
