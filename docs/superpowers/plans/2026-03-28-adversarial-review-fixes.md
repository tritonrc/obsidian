# Adversarial Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all P0 and P1 findings from the GPT-5.4 adversarial review, plus user-reported issues (dots in metric names, OTLP logs silent drop).

**Architecture:** Each task is a focused, independently testable fix touching 1-3 files. Tasks are ordered by severity (P0 first), then by dependency (store fixes before query fixes). Every fix gets a regression test before the implementation.

**Tech Stack:** Rust, Axum, nom, promql-parser, opentelemetry-proto, parking_lot, lasso

---

## File Map

| Task | Files Modified | Files Tested |
|------|---------------|-------------|
| 1 | `src/query/promql/handlers.rs`, `src/query/logql/handlers.rs` | unit tests in both files |
| 2 | `src/config.rs` | unit tests in file |
| 3 | `src/ingest/mod.rs`, `src/ingest/loki.rs`, `src/ingest/remote_write.rs` | unit tests in `mod.rs` |
| 4 | `src/ingest/otlp_metrics.rs` | `tests/otlp_metric_name_normalization.rs` |
| 5 | `src/ingest/otlp_logs.rs` | `tests/otlp_logs_reliability.rs` |
| 6 | `src/store/log_store.rs`, `src/store/metric_store.rs` | unit tests in both files |
| 7 | `src/store/trace_store.rs` | unit tests in file |
| 8 | `src/store/trace_store.rs` | unit tests in file |
| 9 | `src/query/promql/eval.rs` | unit tests in file |
| 10 | `src/query/promql/eval.rs` | unit tests in file |
| 11 | `src/query/traceql/parser.rs` | unit tests in file |
| 12 | `src/query/logql/eval.rs`, `src/query/logql/handlers.rs` | unit tests in both files |
| 13 | `src/ingest/loki.rs`, `src/ingest/otlp_metrics.rs` | unit tests in both files |
| 14 | `src/query/promql/eval.rs`, `src/query/promql/handlers.rs` | unit tests in both files |

---

### Task 1: P0 — Cap step count in PromQL and LogQL range queries

Unbounded `(end - start) / step` allows a single request to OOM the process. Add a hard cap of 11,000 steps (matches Prometheus default).

**Files:**
- Modify: `src/query/promql/handlers.rs:99-165`
- Modify: `src/query/logql/handlers.rs:102-170`

- [ ] **Step 1: Write failing tests in `src/query/promql/handlers.rs`**

Add to the existing `#[cfg(test)] mod tests` block (create one if absent — the file has none currently, so add at the bottom):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_to_ms_nanoseconds() {
        assert_eq!(classify_to_ms(1_700_000_000_000_000_000), 1_700_000_000_000);
    }

    #[test]
    fn test_classify_to_ms_milliseconds() {
        assert_eq!(classify_to_ms(1_700_000_000_000), 1_700_000_000_000);
    }

    #[test]
    fn test_classify_to_ms_seconds() {
        assert_eq!(classify_to_ms(1_700_000_000), 1_700_000_000_000);
    }

    /// P0 regression: unbounded step count must be rejected.
    #[test]
    fn test_max_steps_exceeded() {
        // 11001 steps exceeds the 11000 cap
        let steps = compute_step_count(0, 11_001_000, 1000);
        assert!(steps > MAX_QUERY_STEPS);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib query::promql::handlers::tests -- --nocapture`
Expected: FAIL — `MAX_QUERY_STEPS` and `compute_step_count` not defined

- [ ] **Step 3: Add constant and validation in `src/query/promql/handlers.rs`**

After the existing `PROMQL_HINT` constant (line 16), add:

```rust
/// Maximum number of evaluation steps allowed in a range query.
/// Matches Prometheus's default of 11,000 to prevent DoS.
const MAX_QUERY_STEPS: i64 = 11_000;
```

In `query_range_inner`, after the `step_ms <= 0` check (after line 154), add:

```rust
    let num_steps = (end_ms - start_ms) / step_ms;
    if num_steps > MAX_QUERY_STEPS {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                json!({"status": "error", "errorType": "bad_data", "error": format!("query would evaluate {} steps, max is {}", num_steps, MAX_QUERY_STEPS)}),
            ),
        );
    }
```

- [ ] **Step 4: Run PromQL handler tests to verify they pass**

Run: `cargo test --lib query::promql::handlers -- --nocapture`
Expected: PASS

- [ ] **Step 5: Write failing test in `src/query/logql/handlers.rs`**

The file already has a `#[cfg(test)] mod tests` block at line 288. Add to it:

```rust
    /// P0 regression: unbounded step count must be rejected.
    #[test]
    fn test_logql_max_steps_constant() {
        // Verify the constant exists and has a reasonable value
        assert!(MAX_QUERY_STEPS > 0);
        assert!(MAX_QUERY_STEPS <= 11_000);
    }
```

- [ ] **Step 6: Add constant and validation in `src/query/logql/handlers.rs`**

After the `LOGQL_HINT` constant (line 16), add:

```rust
/// Maximum number of evaluation steps allowed in a range query.
const MAX_QUERY_STEPS: i64 = 11_000;
```

In `query_range_inner`, after the `step_ns == Some(0)` check (after line 163), add:

```rust
    if let Some(step) = step_ns {
        let num_steps = (end_ns - start_ns) / step;
        if num_steps > MAX_QUERY_STEPS {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"status": "error", "error": format!("query would evaluate {} steps, max is {}", num_steps, MAX_QUERY_STEPS)})),
            );
        }
    }
```

- [ ] **Step 7: Run all handler tests**

Run: `cargo test --lib query -- --nocapture`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add src/query/promql/handlers.rs src/query/logql/handlers.rs
git commit -m "query: cap range query step count at 11,000 to prevent DoS"
```

---

### Task 2: parse_duration safety — reject negative and non-finite values

`Duration::from_secs_f64` panics on negative, NaN, and infinite inputs. User-controlled `step=` params flow directly into it.

**Files:**
- Modify: `src/config.rs:57-85`

- [ ] **Step 1: Write failing tests in `src/config.rs`**

Add to the existing test module (after line 99):

```rust
    #[test]
    fn test_parse_duration_negative() {
        assert_eq!(parse_duration("-1s"), None);
    }

    #[test]
    fn test_parse_duration_negative_minutes() {
        assert_eq!(parse_duration("-5m"), None);
    }

    #[test]
    fn test_parse_duration_zero() {
        assert_eq!(parse_duration("0s"), Some(Duration::from_secs(0)));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib config::tests -- --nocapture`
Expected: FAIL (or panic) — negative value passed to `Duration::from_secs_f64`

- [ ] **Step 3: Add validation in `parse_duration`**

Replace line 83-84 in `src/config.rs`:

```rust
    let n: f64 = num_str.trim().parse().ok()?;
    Some(Duration::from_secs_f64(n * multiplier as f64))
```

with:

```rust
    let n: f64 = num_str.trim().parse().ok()?;
    if !n.is_finite() || n < 0.0 {
        return None;
    }
    Some(Duration::from_secs_f64(n * multiplier as f64))
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib config::tests -- --nocapture`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/config.rs
git commit -m "config: reject negative and non-finite duration values"
```

---

### Task 3: Decompression size limits

Gzip `read_to_end` and snappy `decompress_vec` are unbounded. A small compressed payload can expand to gigabytes.

**Files:**
- Modify: `src/ingest/mod.rs:24-39`
- Modify: `src/ingest/loki.rs` (snappy path)
- Modify: `src/ingest/remote_write.rs` (snappy path)

- [ ] **Step 1: Write failing test in `src/ingest/mod.rs`**

Add to the existing test module:

```rust
    #[test]
    fn test_max_decompressed_size_constant() {
        assert!(MAX_DECOMPRESSED_SIZE > 0);
        assert!(MAX_DECOMPRESSED_SIZE <= 128 * 1024 * 1024);
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib ingest::tests -- --nocapture`
Expected: FAIL — `MAX_DECOMPRESSED_SIZE` not defined

- [ ] **Step 3: Add size-capped decompression in `src/ingest/mod.rs`**

Add constant after imports:

```rust
/// Maximum decompressed body size: 64 MiB. Protects against decompression bombs.
pub const MAX_DECOMPRESSED_SIZE: usize = 64 * 1024 * 1024;
```

Replace the gzip decompression in `decode_body` (lines 31-35):

```rust
    if is_gzip {
        let mut decoder = flate2::read::GzDecoder::new(body.as_ref());
        let mut decompressed = Vec::new();
        let bytes_read = decoder.take(MAX_DECOMPRESSED_SIZE as u64 + 1).read_to_end(&mut decompressed)
            .map_err(IngestError::GzipDecompression)?;
        if bytes_read > MAX_DECOMPRESSED_SIZE {
            return Err(IngestError::PayloadTooLarge);
        }
        Ok(Cow::Owned(decompressed))
    } else {
        Ok(Cow::Borrowed(body.as_ref()))
    }
```

Add a new error variant to `IngestError`:

```rust
    /// Decompressed body exceeds the size limit.
    #[error("decompressed body exceeds size limit")]
    PayloadTooLarge,
```

- [ ] **Step 4: Update snappy paths in `src/ingest/loki.rs`**

Find the `decode_snappy_json` function. After snappy decompression, add a size check:

```rust
    if decompressed.len() > super::MAX_DECOMPRESSED_SIZE {
        return Err(/* appropriate error */);
    }
```

- [ ] **Step 5: Update snappy path in `src/ingest/remote_write.rs`**

After each `decompress_vec` call, add:

```rust
    if decompressed.len() > super::MAX_DECOMPRESSED_SIZE {
        return (StatusCode::BAD_REQUEST, "decompressed body too large").into_response();
    }
```

- [ ] **Step 6: Run tests**

Run: `cargo test --lib ingest -- --nocapture`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/ingest/mod.rs src/ingest/loki.rs src/ingest/remote_write.rs
git commit -m "ingest: cap decompressed body size at 64 MiB"
```

---

### Task 4: Normalize OTLP metric names — dots to underscores

OTLP uses dots (`http.server.duration`), PromQL grammar rejects dots. Normalize on ingest to match what the OTel Collector Prometheus exporter does.

**Files:**
- Modify: `src/ingest/otlp_metrics.rs:63`
- Create: `tests/otlp_metric_name_normalization.rs`

- [ ] **Step 1: Write failing integration test**

Create `tests/otlp_metric_name_normalization.rs`:

```rust
//! Verify that OTLP metric names with dots are normalized to underscores on ingest.

use obsidian::store::AppState;
use std::sync::Arc;
use std::time::Instant;
use parking_lot::RwLock;

fn make_state() -> Arc<AppState> {
    Arc::new(AppState {
        log_store: RwLock::new(obsidian::store::log_store::LogStore::new()),
        metric_store: RwLock::new(obsidian::store::metric_store::MetricStore::new()),
        trace_store: RwLock::new(obsidian::store::trace_store::TraceStore::new()),
        config: obsidian::config::Config::parse_from::<_, &str>([]),
        start_time: Instant::now(),
    })
}

#[tokio::test]
async fn test_otlp_metric_dots_normalized_to_underscores() {
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::HeaderMap;
    use prost::Message;
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::metrics::v1::{
        ResourceMetrics, ScopeMetrics, Metric, Gauge,
        metric::Data, number_data_point::Value,
    };
    use opentelemetry_proto::tonic::metrics::v1::NumberDataPoint;

    let state = make_state();

    // Build an OTLP metrics request with a dotted metric name
    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "http.server.duration".to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: Vec::new(),
                    data: Some(Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1_700_000_000_000_000_000,
                            value: Some(Value::AsDouble(42.0)),
                            ..Default::default()
                        }],
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let body = Bytes::from(request.encode_to_vec());
    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/x-protobuf".parse().unwrap());

    obsidian::ingest::otlp_metrics::metrics_handler(
        State(state.clone()),
        headers,
        body,
    ).await;

    // Verify: the metric should be stored with underscores
    let store = state.metric_store.read();
    let names = store.label_names();
    assert!(names.contains(&"__name__".to_string()));
    let values = store.get_label_values("__name__");
    assert!(
        values.contains(&"http_server_duration".to_string()),
        "Expected 'http_server_duration' but got: {:?}", values
    );
    assert!(
        !values.contains(&"http.server.duration".to_string()),
        "Dotted name should NOT be stored"
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_otlp_metric_dots_normalized -- --nocapture`
Expected: FAIL — metric stored as `http.server.duration`

- [ ] **Step 3: Normalize metric name in `src/ingest/otlp_metrics.rs`**

At line 63, where `metric_name` is read from the OTLP proto:

```rust
                let metric_name = &metric.name;
```

Change to:

```rust
                let metric_name = metric.name.replace('.', "_");
```

And update all references below from `metric_name.clone()` (which borrows `&String`) to just `metric_name.clone()` (now owns the `String`). The existing code already calls `.clone()` on it, so the types remain compatible.

- [ ] **Step 4: Run test**

Run: `cargo test test_otlp_metric_dots_normalized -- --nocapture`
Expected: PASS

- [ ] **Step 5: Run full test suite**

Run: `cargo test`
Expected: PASS — existing tests should still work (metric names in existing tests don't use dots)

- [ ] **Step 6: Commit**

```bash
git add src/ingest/otlp_metrics.rs tests/otlp_metric_name_normalization.rs
git commit -m "ingest: normalize OTLP metric names — dots to underscores"
```

---

### Task 5: OTLP logs reliability — timestamps and body handling

Zero timestamps cause immediate eviction. Non-string log bodies become empty strings. No debug logging on ingest.

**Files:**
- Modify: `src/ingest/otlp_logs.rs:66-75`

- [ ] **Step 1: Write failing test**

Create `tests/otlp_logs_reliability.rs`:

```rust
//! Verify OTLP log edge cases: zero timestamps get current time, non-string bodies serialize.

use obsidian::store::AppState;
use std::sync::Arc;
use std::time::Instant;
use parking_lot::RwLock;

fn make_state() -> Arc<AppState> {
    Arc::new(AppState {
        log_store: RwLock::new(obsidian::store::log_store::LogStore::new()),
        metric_store: RwLock::new(obsidian::store::metric_store::MetricStore::new()),
        trace_store: RwLock::new(obsidian::store::trace_store::TraceStore::new()),
        config: obsidian::config::Config::parse_from::<_, &str>([]),
        start_time: Instant::now(),
    })
}

#[tokio::test]
async fn test_otlp_logs_zero_timestamp_gets_current_time() {
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::HeaderMap;
    use prost::Message;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::logs::v1::{ResourceLogs, ScopeLogs, LogRecord};
    use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};

    let state = make_state();

    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: None,
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: 0, // Zero timestamp!
                    severity_number: 9, // info
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("hello".to_string())),
                    }),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let body = Bytes::from(request.encode_to_vec());
    let headers = HeaderMap::new();

    obsidian::ingest::otlp_logs::logs_handler(
        State(state.clone()),
        headers,
        body,
    ).await;

    let store = state.log_store.read();
    assert_eq!(store.total_entries, 1, "log entry should be stored");

    // The timestamp should be non-zero (replaced with current time)
    for (_id, stream) in &store.streams {
        for entry in &stream.entries {
            assert!(entry.timestamp_ns > 0, "zero timestamp should be replaced with current time");
        }
    }
}

#[tokio::test]
async fn test_otlp_logs_int_body_not_empty() {
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::HeaderMap;
    use prost::Message;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::logs::v1::{ResourceLogs, ScopeLogs, LogRecord};
    use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};

    let state = make_state();

    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: None,
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    severity_number: 9,
                    body: Some(AnyValue {
                        value: Some(any_value::Value::IntValue(42)),
                    }),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let body = Bytes::from(request.encode_to_vec());
    let headers = HeaderMap::new();

    obsidian::ingest::otlp_logs::logs_handler(
        State(state.clone()),
        headers,
        body,
    ).await;

    let store = state.log_store.read();
    assert_eq!(store.total_entries, 1);

    for (_id, stream) in &store.streams {
        for entry in &stream.entries {
            assert_eq!(entry.line, "42", "int body should be stringified, not empty");
        }
    }
}
```

- [ ] **Step 2: Run to verify test_otlp_logs_zero_timestamp fails**

Run: `cargo test test_otlp_logs_zero_timestamp -- --nocapture`
Expected: FAIL — timestamp_ns is 0

- [ ] **Step 3: Fix zero timestamp and add logging in `src/ingest/otlp_logs.rs`**

Replace lines 72-75:

```rust
                let entry = LogEntry {
                    timestamp_ns: log_record.time_unix_nano as i64,
                    line,
                };
```

with:

```rust
                let timestamp_ns = if log_record.time_unix_nano == 0 {
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as i64
                } else {
                    log_record.time_unix_nano as i64
                };

                let entry = LogEntry {
                    timestamp_ns,
                    line,
                };
```

After the ingestion loop (after line 86), add debug logging:

```rust
    tracing::debug!(entries = prepared.len(), "ingested OTLP logs");
```

- [ ] **Step 4: Run tests**

Run: `cargo test test_otlp_logs -- --nocapture`
Expected: PASS (both tests)

- [ ] **Step 5: Commit**

```bash
git add src/ingest/otlp_logs.rs tests/otlp_logs_reliability.rs
git commit -m "ingest: fix OTLP logs zero timestamps, add debug logging"
```

---

### Task 6: Store sorting correctness — always sort appended batches

The current code only sorts when new entries cross the previous tail timestamp. An internally unsorted batch that's entirely after the tail timestamp won't be sorted, breaking `partition_point`.

**Files:**
- Modify: `src/store/log_store.rs:118-134`
- Modify: `src/store/metric_store.rs:113-129`

- [ ] **Step 1: Write failing test in `src/store/log_store.rs`**

Add to the existing `#[cfg(test)] mod tests` block:

```rust
    #[test]
    fn test_internally_unsorted_batch_after_tail() {
        let mut store = LogStore::new();
        // First batch: sorted
        store.ingest_stream(
            vec![("service".into(), "test".into())],
            vec![
                LogEntry { timestamp_ns: 100, line: "a".into() },
            ],
        );
        // Second batch: all timestamps > 100, but internally unsorted
        store.ingest_stream(
            vec![("service".into(), "test".into())],
            vec![
                LogEntry { timestamp_ns: 300, line: "c".into() },
                LogEntry { timestamp_ns: 200, line: "b".into() },
            ],
        );
        // Query window [150, 250] should return only "b" at 200
        for (_id, stream) in &store.streams {
            let entries = store.get_entries(
                LogStore::compute_stream_id(&stream.labels),
                150,
                250,
            );
            assert_eq!(entries.len(), 1, "should find exactly one entry in [150, 250]");
            assert_eq!(entries[0].line, "b");
        }
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib store::log_store::tests::test_internally_unsorted -- --nocapture`
Expected: FAIL — partition_point returns wrong window because entries are [100, 300, 200]

- [ ] **Step 3: Fix sorting in `src/store/log_store.rs`**

Replace lines 118-134:

```rust
        // Maintain sorted order for partition_point correctness
        if was_empty {
            // First batch: check if it's already sorted
            if !stream
                .entries
                .windows(2)
                .all(|w| w[0].timestamp_ns <= w[1].timestamp_ns)
            {
                stream.entries.sort_by_key(|e| e.timestamp_ns);
            }
        } else if let Some(prev_ts) = prev_last_ts
            && stream.entries[stream.entries.len() - entry_count..]
                .iter()
                .any(|e| e.timestamp_ns < prev_ts)
        {
            stream.entries.sort_by_key(|e| e.timestamp_ns);
        }
```

with:

```rust
        // Maintain sorted order for partition_point correctness.
        // Always check if the full entries vec is sorted after appending.
        if entry_count > 1 || !was_empty {
            let needs_sort = !stream
                .entries
                .windows(2)
                .all(|w| w[0].timestamp_ns <= w[1].timestamp_ns);
            if needs_sort {
                stream.entries.sort_by_key(|e| e.timestamp_ns);
            }
        }
```

- [ ] **Step 4: Run log_store tests**

Run: `cargo test --lib store::log_store -- --nocapture`
Expected: PASS

- [ ] **Step 5: Write failing test and fix in `src/store/metric_store.rs`**

Add identical test to `metric_store.rs` test module:

```rust
    #[test]
    fn test_internally_unsorted_sample_batch_after_tail() {
        let mut store = MetricStore::new();
        store.ingest_samples(
            "test_metric",
            vec![("service".into(), "test".into())],
            vec![Sample { timestamp_ms: 100, value: 1.0 }],
        );
        store.ingest_samples(
            "test_metric",
            vec![("service".into(), "test".into())],
            vec![
                Sample { timestamp_ms: 300, value: 3.0 },
                Sample { timestamp_ms: 200, value: 2.0 },
            ],
        );
        // Verify samples are sorted
        for (_id, series) in &store.series {
            for w in series.samples.windows(2) {
                assert!(
                    w[0].timestamp_ms <= w[1].timestamp_ms,
                    "samples must be sorted: {} > {}",
                    w[0].timestamp_ms,
                    w[1].timestamp_ms
                );
            }
        }
    }
```

Apply the same fix in `metric_store.rs` lines 113-129 — replace the conditional sorting with:

```rust
        // Maintain sorted order for partition_point correctness.
        if sample_count > 1 || !was_empty {
            let needs_sort = !series
                .samples
                .windows(2)
                .all(|w| w[0].timestamp_ms <= w[1].timestamp_ms);
            if needs_sort {
                series.samples.sort_by_key(|s| s.timestamp_ms);
            }
        }
```

- [ ] **Step 6: Run all store tests**

Run: `cargo test --lib store -- --nocapture`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/store/log_store.rs src/store/metric_store.rs
git commit -m "store: fix sorting of internally unsorted appended batches"
```

---

### Task 7: clear_service — remove spans, not entire traces

`clear_service("A")` currently deletes whole traces that contain service A, destroying service B's spans if they share a trace.

**Files:**
- Modify: `src/store/trace_store.rs:301-336`

- [ ] **Step 1: Write failing test in `src/store/trace_store.rs`**

Add to the existing test module:

```rust
    #[test]
    fn test_clear_service_preserves_other_service_spans() {
        let mut store = TraceStore::new();
        let trace_id = [1u8; 16];

        // Ingest spans from two services in the same trace
        let svc_a = store.interner.get_or_intern("service-a");
        let svc_b = store.interner.get_or_intern("service-b");
        let name_a = store.interner.get_or_intern("span-a");
        let name_b = store.interner.get_or_intern("span-b");

        store.ingest_spans(vec![
            Span {
                trace_id,
                span_id: [1u8; 8],
                parent_span_id: None,
                name: name_a,
                service_name: svc_a,
                start_time_ns: 1000,
                duration_ns: 100,
                status: SpanStatus::Ok,
                attributes: SmallVec::new(),
            },
            Span {
                trace_id,
                span_id: [2u8; 8],
                parent_span_id: Some([1u8; 8]),
                name: name_b,
                service_name: svc_b,
                start_time_ns: 1050,
                duration_ns: 50,
                status: SpanStatus::Ok,
                attributes: SmallVec::new(),
            },
        ]);

        assert_eq!(store.total_spans, 2);

        // Clear service-a — should only remove service-a's span
        store.clear_service("service-a");

        assert_eq!(store.total_spans, 1, "only service-a span should be removed");
        let spans = store.get_trace(&trace_id);
        assert!(spans.is_some(), "trace should still exist with service-b span");
        assert_eq!(spans.unwrap().len(), 1);
        assert_eq!(spans.unwrap()[0].service_name, svc_b);
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib store::trace_store::tests::test_clear_service_preserves -- --nocapture`
Expected: FAIL — entire trace removed

- [ ] **Step 3: Rewrite `clear_service` in `src/store/trace_store.rs`**

Replace lines 301-336 with:

```rust
    /// Clear all spans belonging to a specific service, preserving other services' spans.
    pub fn clear_service(&mut self, service: &str) {
        let service_spur = match self.interner.get(service) {
            Some(s) => s,
            None => return,
        };

        // Get trace IDs for this service
        let trace_ids: Vec<[u8; 16]> = match self.service_index.get(&service_spur) {
            Some(set) => set.iter().copied().collect(),
            None => return,
        };

        for trace_id in &trace_ids {
            if let Some(spans) = self.traces.get_mut(trace_id) {
                // Remove only spans belonging to this service
                let before = spans.len();
                spans.retain(|s| s.service_name != service_spur);
                let removed = before - spans.len();
                self.total_spans = self.total_spans.saturating_sub(removed);

                if spans.is_empty() {
                    // Trace is now empty — remove it entirely
                    self.traces.remove(trace_id);
                } else {
                    // Rebuild indexes for surviving spans in this trace
                    // (other services' spans may still reference this trace)
                }
            }
        }

        // Remove the service from the service_index
        self.service_index.remove(&service_spur);

        // Rebuild name_index and status_index from scratch for affected traces
        // (simpler and correct vs. incremental cleanup)
        self.name_index.clear();
        self.status_index.clear();
        let mut new_service_index: FxHashMap<Spur, FxHashSet<[u8; 16]>> = FxHashMap::default();
        for (trace_id, spans) in &self.traces {
            for span in spans {
                new_service_index
                    .entry(span.service_name)
                    .or_default()
                    .insert(*trace_id);
                self.name_index
                    .entry(span.name)
                    .or_default()
                    .insert(*trace_id);
                self.status_index
                    .entry(span.status)
                    .or_default()
                    .insert(*trace_id);
            }
        }
        self.service_index = new_service_index;
    }
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib store::trace_store -- --nocapture`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/store/trace_store.rs
git commit -m "store: clear_service removes only matching spans, preserves cross-service traces"
```

---

### Task 8: Partial trace eviction — clean stale indexes

When `evict_before` removes some spans from a trace but the trace isn't empty, stale service/name/status entries remain indexed.

**Files:**
- Modify: `src/store/trace_store.rs:187-245`

- [ ] **Step 1: Write failing test**

Add to `trace_store.rs` test module:

```rust
    #[test]
    fn test_evict_partial_cleans_stale_indexes() {
        let mut store = TraceStore::new();
        let trace_id = [1u8; 16];

        let svc = store.interner.get_or_intern("my-svc");
        let old_name = store.interner.get_or_intern("old-span");
        let new_name = store.interner.get_or_intern("new-span");

        store.ingest_spans(vec![
            Span {
                trace_id,
                span_id: [1u8; 8],
                parent_span_id: None,
                name: old_name,
                service_name: svc,
                start_time_ns: 100,
                duration_ns: 10,
                status: SpanStatus::Error,
                attributes: SmallVec::new(),
            },
            Span {
                trace_id,
                span_id: [2u8; 8],
                parent_span_id: Some([1u8; 8]),
                name: new_name,
                service_name: svc,
                start_time_ns: 1000,
                duration_ns: 10,
                status: SpanStatus::Ok,
                attributes: SmallVec::new(),
            },
        ]);

        // Evict spans older than 500 — removes old-span but keeps new-span
        store.evict_before(500);

        assert_eq!(store.total_spans, 1);
        // The error status should no longer be in the index
        let error_traces = store.status_index.get(&SpanStatus::Error);
        assert!(
            error_traces.is_none() || error_traces.unwrap().is_empty(),
            "error status should be removed from index after eviction"
        );
        // The old span name should no longer be in the index
        let old_name_traces = store.name_index.get(&old_name);
        assert!(
            old_name_traces.is_none() || old_name_traces.unwrap().is_empty(),
            "old span name should be removed from index after eviction"
        );
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib store::trace_store::tests::test_evict_partial_cleans -- --nocapture`
Expected: FAIL — stale indexes remain

- [ ] **Step 3: Fix `evict_before` in `src/store/trace_store.rs`**

After `spans.retain(|s| s.start_time_ns >= cutoff_ns);` (line 214), when the trace is NOT empty (i.e., partial eviction happened and `removed > 0`), rebuild indexes for that trace:

Replace lines 187-245 with:

```rust
    pub fn evict_before(&mut self, cutoff_ns: i64) {
        let mut empty_traces: Vec<[u8; 16]> = Vec::new();
        let mut partially_evicted: Vec<[u8; 16]> = Vec::new();

        for (trace_id, spans) in &mut self.traces {
            let before = spans.len();
            spans.retain(|s| s.start_time_ns >= cutoff_ns);
            let removed = before - spans.len();
            if removed > 0 {
                self.total_spans = self.total_spans.saturating_sub(removed);
                if spans.is_empty() {
                    empty_traces.push(*trace_id);
                } else {
                    partially_evicted.push(*trace_id);
                }
            }
        }

        // Remove empty traces
        for trace_id in &empty_traces {
            self.traces.remove(trace_id);
        }

        // Rebuild all indexes from surviving spans (simplest correct approach)
        if !empty_traces.is_empty() || !partially_evicted.is_empty() {
            self.service_index.clear();
            self.name_index.clear();
            self.status_index.clear();
            for (trace_id, spans) in &self.traces {
                for span in spans {
                    self.service_index
                        .entry(span.service_name)
                        .or_default()
                        .insert(*trace_id);
                    self.name_index
                        .entry(span.name)
                        .or_default()
                        .insert(*trace_id);
                    self.status_index
                        .entry(span.status)
                        .or_default()
                        .insert(*trace_id);
                }
            }
        }
    }
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib store::trace_store -- --nocapture`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/store/trace_store.rs
git commit -m "store: rebuild trace indexes after partial eviction"
```

---

### Task 9: PromQL rate/increase — counter reset detection

Current `compute_rate_like` divides by requested window width instead of actual sample time, and doesn't detect counter resets.

**Files:**
- Modify: `src/query/promql/eval.rs:616-674`

- [ ] **Step 1: Write failing test**

Add to `src/query/promql/eval.rs` test module (create `#[cfg(test)] mod tests` if not present):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::metric_store::{MetricStore, Sample};

    #[test]
    fn test_rate_with_counter_reset() {
        let mut store = MetricStore::new();
        // Counter: 0, 10, 20, 5 (reset!), 15
        store.ingest_samples("counter", vec![], vec![
            Sample { timestamp_ms: 1000, value: 0.0 },
            Sample { timestamp_ms: 2000, value: 10.0 },
            Sample { timestamp_ms: 3000, value: 20.0 },
            Sample { timestamp_ms: 4000, value: 5.0 },   // reset
            Sample { timestamp_ms: 5000, value: 15.0 },
        ]);

        // rate over 5s window: total increase should account for reset
        // Total increase = 10 + 10 + 5 + 10 = 35, over 4 seconds = 8.75/s
        let result = evaluate_range(
            "rate(counter[5s])",
            &store,
            5000,
            5000,
            1000,
        ).unwrap();

        match result {
            PromQLResult::InstantVector(series) => {
                assert_eq!(series.len(), 1);
                let val = series[0].samples[0].1;
                // With reset detection: (10+10+5+10) / 4s = 8.75
                assert!(val > 0.0, "rate should be positive, got {}", val);
                assert!((val - 8.75).abs() < 0.5, "rate should be ~8.75/s, got {}", val);
            }
            _ => panic!("expected InstantVector"),
        }
    }

    #[test]
    fn test_rate_divides_by_sample_time_not_window() {
        let mut store = MetricStore::new();
        // Two samples 2s apart in a 10s window
        store.ingest_samples("counter", vec![], vec![
            Sample { timestamp_ms: 8000, value: 0.0 },
            Sample { timestamp_ms: 10000, value: 20.0 },
        ]);

        // rate(counter[10s]) at t=10s
        let result = evaluate_range(
            "rate(counter[10s])",
            &store,
            10000,
            10000,
            1000,
        ).unwrap();

        match result {
            PromQLResult::InstantVector(series) => {
                assert_eq!(series.len(), 1);
                let val = series[0].samples[0].1;
                // Prometheus extrapolates: delta=20, sample_time=2s, extrapolated to 10s window
                // But at minimum, rate should divide by actual elapsed, not window
                // 20 / 2s = 10/s (actual) vs 20 / 10s = 2/s (buggy)
                assert!(val > 5.0, "rate should divide by sample elapsed time (~10/s), got {}", val);
            }
            _ => panic!("expected InstantVector"),
        }
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib query::promql::eval::tests -- --nocapture`
Expected: FAIL — rate divides by window width

- [ ] **Step 3: Fix `compute_rate_like` in `src/query/promql/eval.rs`**

Replace `compute_rate_like` (lines 616-674) with:

```rust
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

            if func_name == "increase" {
                // Extrapolate: scale by (range / sample_duration)
                let sample_duration = (last.timestamp_ms - first.timestamp_ms) as f64;
                if sample_duration > 0.0 {
                    Some(total_increase * (range_ms as f64 / sample_duration))
                } else {
                    Some(total_increase)
                }
            } else {
                // rate = increase / range_seconds
                let range_s = range_ms as f64 / 1000.0;
                if range_s > 0.0 {
                    // Extrapolate increase to full range, then divide by range
                    let sample_duration = (last.timestamp_ms - first.timestamp_ms) as f64;
                    if sample_duration > 0.0 {
                        Some(total_increase / (sample_duration / 1000.0))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }
        "delta" => {
            // delta: raw difference, no reset detection
            Some(last.value - first.value)
        }
        "deriv" => {
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
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib query::promql::eval::tests -- --nocapture`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/query/promql/eval.rs
git commit -m "promql: fix rate/increase with counter reset detection"
```

---

### Task 10: PromQL binary ops — add ^ (pow), reject unsupported modifiers

**Files:**
- Modify: `src/query/promql/eval.rs:825-991`

- [ ] **Step 1: Write failing tests**

Add to `src/query/promql/eval.rs` tests:

```rust
    #[test]
    fn test_pow_operator() {
        let store = MetricStore::new();
        let result = evaluate_instant("2 ^ 10", &store, 1000).unwrap();
        match result {
            PromQLResult::Scalar(v) => assert_eq!(v, 1024.0),
            _ => panic!("expected Scalar"),
        }
    }

    #[test]
    fn test_pow_operator_float() {
        let store = MetricStore::new();
        let result = evaluate_instant("4 ^ 0.5", &store, 1000).unwrap();
        match result {
            PromQLResult::Scalar(v) => assert!((v - 2.0).abs() < 0.001),
            _ => panic!("expected Scalar"),
        }
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib query::promql::eval::tests::test_pow -- --nocapture`
Expected: FAIL — `^` falls through to NaN

- [ ] **Step 3: Add `^` to `apply_binary_op`**

In `apply_binary_op` (line 928), add before the `_ => f64::NAN` fallback:

```rust
        "^" => l.powf(r),
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib query::promql::eval -- --nocapture`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/query/promql/eval.rs
git commit -m "promql: add ^ (pow) operator to binary expressions"
```

---

### Task 11: TraceQL parser — reject trailing logical operators

`{ status = error && }` currently silently becomes `{ status = error }`.

**Files:**
- Modify: `src/query/traceql/parser.rs:203-232`

- [ ] **Step 1: Write failing test**

Add to `src/query/traceql/parser.rs` test module:

```rust
    #[test]
    fn test_trailing_and_is_parse_error() {
        let result = parse_traceql("{ status = error && }");
        assert!(result.is_err(), "trailing && should be a parse error");
    }

    #[test]
    fn test_trailing_or_is_parse_error() {
        let result = parse_traceql("{ status = error || }");
        assert!(result.is_err(), "trailing || should be a parse error");
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib query::traceql::parser::tests::test_trailing -- --nocapture`
Expected: FAIL — parses successfully

- [ ] **Step 3: Fix `parse_conditions` in `src/query/traceql/parser.rs`**

In the loop body (lines 221-228), change the `Err(_) => break` to return an error:

```rust
            match parse_condition(rest) {
                Ok((rest, cond)) => {
                    logical_ops.push(op);
                    conditions.push(cond);
                    input = rest;
                }
                Err(_) => {
                    return Err(nom::Err::Failure(nom::error::Error::new(
                        rest,
                        nom::error::ErrorKind::Tag,
                    )));
                }
            }
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib query::traceql::parser -- --nocapture`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/query/traceql/parser.rs
git commit -m "traceql: reject trailing logical operators in span selectors"
```

---

### Task 12: LogQL fixes — global limit + min/max_over_time empty window

**Files:**
- Modify: `src/query/logql/handlers.rs:195-210`
- Modify: `src/query/logql/eval.rs:152-159`

- [ ] **Step 1: Write failing test for min_over_time empty window**

Add to `src/query/logql/eval.rs` test module (create if needed):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::log_store::LogStore;

    #[test]
    fn test_min_over_time_empty_window_not_infinity() {
        let store = LogStore::new();
        // No data in store — query should return empty, not infinity
        let result = evaluate_logql(
            &super::super::parser::parse_logql("min_over_time({service=\"nonexistent\"}[5m])").unwrap(),
            &store,
            0,
            300_000_000_000,
            Some(60_000_000_000),
        );
        match result {
            LogQLResult::Matrix(series) => {
                // Should be empty or have no infinity values
                for s in &series {
                    for &(_, val) in &s.samples {
                        assert!(val.is_finite(), "empty window should not produce infinity, got {}", val);
                    }
                }
            }
            _ => {}
        }
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib query::logql::eval::tests -- --nocapture`
Expected: FAIL (if any streams match, the fold produces INFINITY)

- [ ] **Step 3: Fix min/max_over_time in `src/query/logql/eval.rs`**

Replace lines 152-159:

```rust
                MetricFunc::MinOverTime => filtered
                    .iter()
                    .filter_map(|e| e.line.trim().parse::<f64>().ok())
                    .fold(f64::INFINITY, f64::min),
                MetricFunc::MaxOverTime => filtered
                    .iter()
                    .filter_map(|e| e.line.trim().parse::<f64>().ok())
                    .fold(f64::NEG_INFINITY, f64::max),
```

with:

```rust
                MetricFunc::MinOverTime => {
                    let values: Vec<f64> = filtered
                        .iter()
                        .filter_map(|e| e.line.trim().parse::<f64>().ok())
                        .collect();
                    if values.is_empty() {
                        continue; // Skip this time step — no numeric values
                    }
                    values.into_iter().fold(f64::INFINITY, f64::min)
                }
                MetricFunc::MaxOverTime => {
                    let values: Vec<f64> = filtered
                        .iter()
                        .filter_map(|e| e.line.trim().parse::<f64>().ok())
                        .collect();
                    if values.is_empty() {
                        continue;
                    }
                    values.into_iter().fold(f64::NEG_INFINITY, f64::max)
                }
```

Note: the `continue` skips `samples.push((t, value))` for that time step, which is the correct behavior — an empty window produces no sample.

- [ ] **Step 4: Run tests**

Run: `cargo test --lib query::logql -- --nocapture`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/query/logql/eval.rs
git commit -m "logql: skip empty windows in min/max_over_time instead of producing infinity"
```

---

### Task 13: Reject Loki native protobuf + warn on ExponentialHistogram

The Loki protobuf path snappy-decompresses then JSON-parses — it doesn't actually decode Loki proto format. Reject it explicitly. Also warn (don't silently skip) ExponentialHistogram.

**Files:**
- Modify: `src/ingest/loki.rs`
- Modify: `src/ingest/otlp_metrics.rs`

- [ ] **Step 1: Read current Loki handler to find protobuf content type check**

Read: `src/ingest/loki.rs`

- [ ] **Step 2: Fix Loki handler — reject native protobuf explicitly**

In the Loki push handler, the snappy path currently decompresses and then JSON-parses. This works for Loki's "JSON compressed with snappy" format but NOT for native protobuf. Add a warning if the content type is specifically `application/x-protobuf` without snappy:

Find the content-type check and ensure the handler either:
- Only accepts `application/json` and `application/x-snappy` (which wraps JSON)
- Returns 400 with a helpful message for `application/x-protobuf`

- [ ] **Step 3: Add warning for ExponentialHistogram in `src/ingest/otlp_metrics.rs`**

Find the `ExponentialHistogram` case (around line 142). Change from silent skip to logging:

```rust
                    Some(Data::ExponentialHistogram(_)) => {
                        tracing::warn!(
                            metric = metric_name.as_str(),
                            "skipping ExponentialHistogram — not supported"
                        );
                    }
```

- [ ] **Step 4: Run all tests**

Run: `cargo test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/ingest/loki.rs src/ingest/otlp_metrics.rs
git commit -m "ingest: reject Loki native protobuf, warn on ExponentialHistogram"
```

---

### Task 14: PromQL miscellaneous — remove forward_buffer, add ^ to comparison check

**Files:**
- Modify: `src/query/promql/eval.rs:156`

- [ ] **Step 1: Remove forward_buffer_ms in instant vector selection**

In `eval_vector_selector` (line 156), change:

```rust
        let forward_buffer_ms = 1000; // 1s forward tolerance for timestamp rounding
```

to:

```rust
        let forward_buffer_ms = 0; // Do not look past the evaluation timestamp
```

This ensures instant queries don't leak future data.

- [ ] **Step 2: Run all tests**

Run: `cargo test`
Expected: PASS — if any test depends on forward buffer, it reveals a real issue

- [ ] **Step 3: Commit**

```bash
git add src/query/promql/eval.rs
git commit -m "promql: remove forward_buffer_ms to prevent future data leak in instant queries"
```

---

## Verification

After all tasks are complete:

```bash
# Format and lint
cargo fmt
cargo clippy --all-targets -- -D warnings

# Full test suite
cargo test

# Python e2e (if binary is built)
cargo build --release
python3 tests/e2e_python.py
```

## Deferred Items (P2, not in this plan)

These are documented but deferred to a follow-up plan:

- **P2-18**: `@` time modifier not applied (requires `promql-parser` AST inspection)
- **P2-20**: `/api/v1/series` ignores time bounds (nice-to-have)
- **P2-21**: Diagnose endpoint performance (functional, just slow on large stores)
- **P1-11**: Binary modifiers `on()`/`ignoring()`/`group_left`/`group_right` (significant implementation effort — reject explicitly for now with a TODO)
