# Obsidian: Lightweight Ephemeral Observability Engine

## Overview

A single Rust binary exposing three API surfaces — LogQL, PromQL, and TraceQL — designed for ephemeral, per-worktree observability. A single Obsidian instance serves all services in a worktree (e.g. API gateway, payments engine, worker processes). Services are distinguished by labels (`service`, `resource.service.name`) and queryable independently or in aggregate. Services send telemetry directly to Obsidian via OTLP/HTTP (metrics, traces, logs) and Loki push API (logs). Everything is stored in-memory with optional snapshot-to-disk. Obsidian supports the 80/20 subset of each query language that agents need to reason about runtime behavior.

The name "Obsidian" is a placeholder — dark, glassy, reflects everything clearly.

---

## Architecture

```
                    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
                    │ api-gateway  │  │  payments-   │  │    risk-     │
                    │              │  │  engine       │  │  evaluator   │
                    └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
                           │ OTLP/HTTP       │ OTLP/HTTP       │ OTLP/HTTP
                           └────────┬────────┘                 │
                                    │         ┌────────────────┘
                                    ▼         ▼
┌──────────────────────────────────────────────────────────────────────┐
│                           OBSIDIAN                                   │
│                                                                      │
│  ┌───────────────────────────────────────────────────────────────┐   │
│  │                     Axum HTTP Server                          │   │
│  │                                                               │   │
│  │  Ingest Routes             Query Routes         Management    │   │
│  │  ─────────────             ────────────         ──────────    │   │
│  │  POST /loki/api/v1/push    GET /loki/api/v1/…  GET /ready    │   │
│  │  POST /v1/metrics          GET /api/v1/query…   /api/v1/…    │   │
│  │  POST /v1/traces           GET /api/search      services     │   │
│  │  POST /v1/logs             GET /api/traces/…    status        │   │
│  │  POST /api/v1/write                             diagnose     │   │
│  │                                                 catalog      │   │
│  │                                                 metadata     │   │
│  │                                                 openapi.json │   │
│  │                                                 reset        │   │
│  └──────┬────────────────────────────┬──────────────────────────┘   │
│         │                            │                              │
│  ┌──────▼──────┐  ┌─────────────────▼───────────────────────┐      │
│  │  Ingestion  │  │          Query Engines                  │      │
│  │  Pipeline   │  │  ┌────────┬─────────┬────────┐          │      │
│  │             │  │  │ LogQL  │ PromQL  │TraceQL │          │      │
│  │  • decode   │  │  │ parser │ parser  │ parser │          │      │
│  │  • validate │  │  │ & eval │ & eval  │ & eval │          │      │
│  │  • label    │  │  └───┬────┴────┬────┴───┬────┘          │      │
│  │  • index    │  └──────┼─────────┼────────┼───────────────┘      │
│  └──────┬──────┘         │         │        │                      │
│         │                │         │        │                      │
│  ┌──────▼────────────────▼─────────▼────────▼────────────────┐     │
│  │                   Storage Engine                          │     │
│  │                                                           │     │
│  │  LogStore          MetricStore        TraceStore           │     │
│  │  ─────────         ───────────        ──────────           │     │
│  │  Append-only       Time-series        Span index           │     │
│  │  label index       w/ labels          by traceID           │     │
│  │  inverted idx      inverted idx       service index        │     │
│  │                                                           │     │
│  │  All stores share: lasso::Rodeo interner, sorted posting  │     │
│  │  lists, service label as first-class index key            │     │
│  │                                                           │     │
│  │  ┌─────────────────────────────────────────────────────┐  │     │
│  │  │              Snapshot Manager                       │  │     │
│  │  │  • serialize to bincode on signal/timer             │  │     │
│  │  │  • restore on boot if snapshot exists               │  │     │
│  │  └─────────────────────────────────────────────────────┘  │     │
│  └───────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Crate Dependencies

### Buy — high-value crates that eliminate weeks of work

| Crate | Purpose | What it saves |
|---|---|---|
| `promql-parser` | Full PromQL lexer/parser → typed AST | 2-3 weeks. Maintained by GreptimeTeam (GreptimeDB), compatible with Prometheus v3.8. Covers the entire PromQL spec including aggregations, binary ops, functions, `offset`, subqueries. We only write the evaluation engine that walks the AST against our MetricStore. |
| `opentelemetry-proto` | Protobuf-generated Rust types for OTLP ingest | 1-2 weeks. Gives us `ExportMetricsServiceRequest`, `ExportTraceServiceRequest`, `ExportLogsServiceRequest` and all nested types. No hand-rolling `.proto` files or running `prost-build`. Decode incoming bytes → map to internal types. |
| `lasso` | String interning with `Rodeo` / `ThreadedRodeo` | Compact `Spur` keys (u32 under the hood) for label names/values in posting lists. Battle-tested, benchmarked, supports freeze-to-`RodeoResolver` for contention-free reads. Replaces our hand-rolled `StringInterner`. |

### Build — custom to this project

| Component | Why we're building it |
|---|---|
| LogQL parser | The `logql` crate exists (nom-based, v0.2.7) but has zero documentation and very low adoption. Worth evaluating first — if its AST covers stream selectors + pipeline stages + metric queries, use it. Otherwise hand-roll with `nom`; LogQL's grammar is the simplest of the three. |
| TraceQL parser | Nothing exists in the Rust ecosystem. Tempo's implementation is in Go. Hand-roll with `nom`. The 80/20 subset (span attribute matching + duration filters + boolean logic) is small. |
| Query evaluation engines | No standalone PromQL/LogQL/TraceQL evaluators exist separate from their parent databases. Walking ASTs against our in-memory stores is custom work. |
| Storage engine + inverted index | Too tightly coupled to our data model to buy. Building blocks are standard (`HashMap`, `BTreeMap`, `lasso` for interning). |

### Infrastructure — standard dependencies

| Crate | Purpose |
|---|---|
| `axum` | HTTP server, routing, extractors |
| `tokio` | Async runtime |
| `serde` / `serde_json` | JSON serialization for Loki push API, query responses |
| `bincode` | Snapshot serialization (fast, compact) |
| `nom` | Parser combinators for LogQL and TraceQL |
| `parking_lot` | Fast RwLock for concurrent store access |
| `clap` | CLI argument parsing |
| `tracing` | Internal logging (the irony is not lost) |
| `snap` | Snappy decompression for Loki protobuf push |
| `flate2` | Gzip support for OTLP |
| `regex` | Regex label matchers in LogQL/PromQL |

---

## CLI Interface

```
obsidian [OPTIONS]

OPTIONS:
    --port <PORT>              Base port (default: 4320)
                               Loki push:  {port}
                               OTLP:       {port}
                               Query APIs: {port}
    --snapshot-dir <PATH>      Directory for snapshots (default: .obsidian/)
    --snapshot-interval <SECS> Auto-snapshot interval, 0 to disable (default: 0)
    --max-log-entries <N>      Max log entries before eviction (default: 100_000)
    --max-series <N>           Max metric series (default: 10_000)
    --max-spans <N>            Max trace spans before eviction (default: 100_000)
    --retention <DURATION>     Max age before eviction, e.g. "1h" (default: "2h")
    --restore                  Restore from snapshot on startup if available
```

All three API surfaces share a single port. Routes are disambiguated by path prefix.

---

## Ingestion

### Logs — Loki Push API

**Endpoint:** `POST /loki/api/v1/push`

Accepts the Loki push format (JSON or Protobuf+Snappy).

```json
{
  "streams": [
    {
      "stream": { "service": "payments", "level": "error" },
      "values": [
        ["1700000000000000000", "connection timeout to column bank API"],
        ["1700000000100000000", "retry 1/3 failed"]
      ]
    }
  ]
}
```

**Processing:**
1. Parse label set, intern label names/values via `lasso::Rodeo` (each unique string stored once, referenced by `Spur` key)
2. Append each `(timestamp_ns, line)` entry to the stream's log buffer
3. Update inverted index: `(Spur, Spur)` label pair → sorted posting list of stream IDs

### Metrics — OTLP/HTTP

**Endpoint:** `POST /v1/metrics`

Accepts OTLP `ExportMetricsServiceRequest` (protobuf). Services send this directly from their OpenTelemetry SDK. Protobuf types provided by `opentelemetry-proto` crate — no manual `.proto` compilation needed.

**Processing:**
1. Decode protobuf using `opentelemetry-proto` types, extract resource/scope/metric attributes
2. For each data point, construct a metric identity from name + sorted label set
3. Intern all label names/values via `lasso::Rodeo`, append `(timestamp_ms, value)` to the time-series buffer
4. Support gauge, sum (counter), and histogram data types

### Traces — OTLP/HTTP

**Endpoint:** `POST /v1/traces`

Accepts OTLP `ExportTraceServiceRequest` (protobuf). Types from `opentelemetry-proto` crate.

**Processing:**
1. Decode protobuf using `opentelemetry-proto` types, extract resource/scope/span attributes
2. Store span with full attribute set, intern label strings via `lasso::Rodeo`
3. Index by: trace_id, service.name, span name, status

### Logs — OTLP/HTTP

**Endpoint:** `POST /v1/logs`

Accepts OTLP `ExportLogsServiceRequest` (protobuf). An alternative to the Loki push API for services that emit logs via their OpenTelemetry SDK.

**Processing:**
1. Decode protobuf using `opentelemetry-proto` types, extract resource/scope/log record attributes
2. Map to `LogEntry` with timestamp and body as the log line
3. Promote `resource.service.name` to `service` label, store in `LogStore`

### Metrics — Prometheus Remote Write

**Endpoint:** `POST /api/v1/write`

Accepts Prometheus remote write format for services that use the Prometheus client library directly.

---

## Service Label Conventions

Obsidian normalizes service identity across all three signal types:

| Signal | Source of service identity | Indexed as |
|---|---|---|
| Logs (Loki push) | `service` label in stream labels | `service` label key in LogStore |
| Metrics (OTLP) | `resource.service.name` attribute | `service` label key in MetricStore (promoted from resource attrs) |
| Traces (OTLP) | `resource.service.name` attribute | `service_name` field in Span, indexed in TraceStore |

On OTLP ingest, the `resource.service.name` attribute is promoted to a top-level `service` label in the metric/trace stores. This means `{service="payments-engine"}` works identically in LogQL and PromQL, and `{ resource.service.name = "payments-engine" }` works in TraceQL — consistent with how Loki, Prometheus, and Tempo handle it in production.

**Multi-service query examples:**

```
# LogQL: errors from payments engine only
{service="payments-engine", level="error"} |= "timeout"

# LogQL: errors from any service
{level="error"} |= "timeout"

# PromQL: request rate across two services
sum(rate(http_requests_total{service=~"api-gateway|payments-engine"}[5m])) by (service)

# PromQL: compare latency across all services
avg(request_duration_seconds) by (service)

# TraceQL: slow spans in a specific service
{ resource.service.name = "payments-engine" && duration > 500ms }

# TraceQL: trace a request from gateway → payments (cross-service)
{ resource.service.name = "api-gateway" } >> { resource.service.name = "payments-engine" }
```

---

## Service Discovery API

Agents need to enumerate what services are reporting telemetry before writing targeted queries.

| Endpoint | Description |
|---|---|
| `GET /api/v1/services` | List all known service names across logs, metrics, and traces |
| `GET /api/v1/status` | Summary: service count, entry/sample/span counts, uptime, memory |
| `GET /api/v1/diagnose` | Diagnostic overview of store health, indexing, and configuration |
| `GET /api/v1/catalog` | Enumerate all known metric names, label names, and trace attributes |
| `GET /api/v1/summary` | Compact summary of ingested data across all stores |
| `GET /api/v1/metadata` | Metric type metadata (compatible with Prometheus metadata endpoint) |
| `GET /api/v1/openapi.json` | OpenAPI specification for all Obsidian endpoints |
| `DELETE /api/v1/reset` | Clear all stores (useful for test isolation) |

```json
// GET /api/v1/services
{
  "status": "success",
  "data": {
    "services": [
      {
        "name": "api-gateway",
        "signals": ["logs", "metrics", "traces"]
      },
      {
        "name": "payments-engine",
        "signals": ["logs", "metrics", "traces"]
      },
      {
        "name": "risk-evaluator",
        "signals": ["metrics", "traces"]
      }
    ]
  }
}
```

Implementation: for each store, resolve the `Spur` for the `service` label name, collect all values from `label_values` (LogStore/MetricStore) and `service_index` (TraceStore). Merge across stores. The `signals` array tells the agent which query languages will return data for each service.

---

## Storage Engine

### LogStore

```rust
use lasso::{Rodeo, Spur};
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

struct LogStore {
    /// Stream ID → stream data
    streams: FxHashMap<u64, LogStream>,
    /// Label pair → set of stream IDs (inverted index)
    label_index: FxHashMap<(Spur, Spur), PostingList>,
    /// Label name → set of known values (for /labels endpoints)
    label_values: FxHashMap<Spur, FxHashSet<Spur>>,
    /// String interner (lasso::Rodeo)
    interner: Rodeo,
    /// Total entry count for eviction
    total_entries: usize,
}

struct PostingList {
    ids: Vec<u64>,  // always kept sorted, enables O(n+m) merge intersection
}

struct LogStream {
    labels: SmallVec<[(Spur, Spur); 8]>,
    entries: Vec<LogEntry>,  // sorted by timestamp, oldest evicted first
}

struct LogEntry {
    timestamp_ns: i64,
    line: String,
}
```

**Label matching:** Intersect posting lists from the inverted index using sorted merge. For `=` matchers, direct `HashMap` lookup by `(Spur, Spur)`. For regex matchers, scan known values in the `lasso::Rodeo` interner, resolve each `Spur` to `&str` for regex testing, then union matching posting lists.

### MetricStore

```rust
struct MetricStore {
    /// Series ID → series data
    series: FxHashMap<u64, MetricSeries>,
    /// Metric name → set of series IDs
    name_index: FxHashMap<Spur, PostingList>,
    /// Label pair → set of series IDs
    label_index: FxHashMap<(Spur, Spur), PostingList>,
    /// Label name → set of known values
    label_values: FxHashMap<Spur, FxHashSet<Spur>>,
    interner: Rodeo,
}

struct MetricSeries {
    labels: SmallVec<[(Spur, Spur); 8]>,  // includes __name__
    samples: Vec<Sample>,  // sorted by timestamp
}

struct Sample {
    timestamp_ms: i64,
    value: f64,
}
```

**Counter handling:** Store raw cumulative values. The `PromQLEvaluator` computes `rate()` at query time by taking deltas between adjacent samples in the range vector.

### TraceStore

```rust
struct TraceStore {
    /// trace_id (16 bytes) → list of spans
    traces: FxHashMap<[u8; 16], Vec<Span>>,
    /// Service name → set of trace IDs
    service_index: FxHashMap<Spur, FxHashSet<[u8; 16]>>,
    /// Span name → set of trace IDs
    name_index: FxHashMap<Spur, FxHashSet<[u8; 16]>>,
    interner: Rodeo,
}

struct Span {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_span_id: Option<[u8; 8]>,
    name: Spur,
    service_name: Spur,
    start_time_ns: i64,
    duration_ns: i64,
    status: SpanStatus,
    attributes: Vec<(Spur, AttributeValue)>,
}
```

### Eviction

A background task runs on a configurable interval (default 30s):
1. Remove entries/samples/spans older than `--retention`
2. If total count exceeds max, evict oldest-first until under limit
3. Remove empty streams/series/traces from indexes

### Snapshot Manager

```rust
// On SIGUSR1 or timer:
let snapshot = Snapshot {
    logs: store.logs.read().clone(),
    metrics: store.metrics.read().clone(),
    traces: store.traces.read().clone(),
    timestamp: SystemTime::now(),
};
let bytes = bincode::serialize(&snapshot)?;
fs::write(snapshot_dir.join("obsidian.snap"), bytes)?;

// On --restore:
let bytes = fs::read(snapshot_dir.join("obsidian.snap"))?;
let snapshot: Snapshot = bincode::deserialize(&bytes)?;
// hydrate stores...
```

---

## Query Engines

### LogQL — 80/20 Subset

**Parsing:** Evaluate the `logql` crate (v0.2.7, nom-based) first. If it covers this subset, use it. Otherwise hand-roll with `nom` (~2 days, simplest grammar of the three).

**Supported syntax:**

```
# Stream selectors
{service="payments"}
{service="payments", level="error"}
{service=~"pay.*"}
{level!="debug"}

# Line filters (pipeline after selector)
{service="payments"} |= "timeout"
{service="payments"} != "healthcheck"
{service="payments"} |~ "error|warn"
{service="payments"} !~ "debug|trace"

# Metric queries over logs
count_over_time({service="payments"}[5m])
rate({service="payments"} |= "error" [1m])
bytes_over_time({service="payments"}[5m])
```

**API endpoints:**

| Endpoint | Description |
|---|---|
| `GET /loki/api/v1/query` | Instant query (latest result) |
| `GET /loki/api/v1/query_range` | Range query with `start`, `end`, `step` |
| `GET /loki/api/v1/labels` | List all label names |
| `GET /loki/api/v1/label/{name}/values` | List values for a label |

**Response format:** Matches Loki's JSON response schema so existing tooling (Grafana, curl scripts) works unmodified.

```json
{
  "status": "success",
  "data": {
    "resultType": "streams",
    "result": [
      {
        "stream": { "service": "payments", "level": "error" },
        "values": [
          ["1700000000000000000", "connection timeout"]
        ]
      }
    ]
  }
}
```

### PromQL — 80/20 Subset

**Parsing:** Handled entirely by the `promql-parser` crate (GreptimeTeam). The crate covers the full PromQL spec; we only evaluate the subset below.

**Supported syntax:**

```
# Instant selectors
http_requests_total
http_requests_total{method="GET"}
http_requests_total{method=~"GET|POST"}
http_requests_total{status!="200"}

# Range vectors
http_requests_total{method="GET"}[5m]

# Functions
rate(http_requests_total[5m])
increase(http_requests_total[1h])
sum(rate(http_requests_total[5m])) by (method)
avg(request_duration_seconds) by (endpoint)
max(memory_usage_bytes) by (service)
min(request_duration_seconds)
count(up)
histogram_quantile(0.99, rate(request_duration_bucket[5m]))

# Aggregation
sum by (method) (rate(http_requests_total[5m]))
avg without (instance) (cpu_usage)

# Binary operators
http_requests_total / http_requests_duration_seconds
rate(errors_total[5m]) / rate(requests_total[5m]) * 100
rate(errors_total[5m]) > 0.01

# Comparison (for filtering)
http_requests_total > 100
process_resident_memory_bytes > 1e9
```

**API endpoints:**

| Endpoint | Description |
|---|---|
| `GET /api/v1/query` | Instant query with `query`, `time` |
| `GET /api/v1/query_range` | Range query with `query`, `start`, `end`, `step` |
| `GET /api/v1/series` | Find series by matcher |
| `GET /api/v1/labels` | List label names |
| `GET /api/v1/label/{name}/values` | List label values |

**Response format:** Matches Prometheus JSON response schema.

### TraceQL — 80/20 Subset

**Parsing:** Custom `nom` parser (no Rust crate exists for TraceQL). Smallest of the three grammars for the 80/20 subset.

**Supported syntax:**

```
# Span attribute selectors
{ resource.service.name = "payments" }
{ name = "POST /api/transfer" }
{ span.http.status_code = 500 }
{ status = error }

# Duration filters
{ duration > 500ms }
{ duration > 1s && resource.service.name = "payments" }

# Logical operators within a spanset
{ resource.service.name = "payments" && duration > 200ms }
{ status = error || span.http.status_code >= 500 }

# Structural (if time permits, otherwise defer)
{ resource.service.name = "api-gateway" } >> { resource.service.name = "payments" }
```

**API endpoints:**

| Endpoint | Description |
|---|---|
| `GET /api/search` | Search traces with TraceQL `q` parameter |
| `GET /api/traces/{traceID}` | Get all spans for a trace (OTLP JSON format) |

---

## Parser Architecture

The three query languages use a mix of bought and built parsers, but share a common evaluation pattern:

```
parse (text → AST) → plan (AST → execution plan) → execute (plan → results)
```

### PromQL — bought parser, custom evaluator

The `promql-parser` crate (GreptimeTeam) handles all parsing. It produces a well-typed AST with enums for `Expr`, `VectorSelector`, `MatrixSelector`, `AggregateExpr`, `BinaryExpr`, `Call`, etc. We write a `PromQLEvaluator` that walks this AST against our `MetricStore`.

```rust
use promql_parser::parser;

// Parse — fully handled by crate
let ast = parser::parse("rate(http_requests_total[5m])")?;

// Evaluate — our custom code
let result = evaluator.eval(&ast, &metric_store, eval_time)?;
```

Key evaluation work we write:
- `rate()` / `increase()`: compute deltas between adjacent samples in range
- `sum() by` / `avg() without`: group series by label sets, aggregate
- `histogram_quantile()`: bucket interpolation
- Binary ops: match series by labels, apply arithmetic/comparison

### LogQL — evaluate-then-build parser

Evaluate the `logql` crate (v0.2.7, nom-based) first. If its AST covers our 80/20 subset (stream selectors, pipeline stages, metric queries over logs), use it. Otherwise hand-roll with `nom`:

```rust
// LogQL AST (either from crate or hand-rolled)
enum LogQLExpr {
    StreamSelector { matchers: Vec<LabelMatcher> },
    Pipeline { selector: Box<LogQLExpr>, stages: Vec<PipelineStage> },
    MetricQuery { function: AggFunc, inner: Box<LogQLExpr>, range: Duration },
}

enum PipelineStage {
    LineFilter { op: LineFilterOp, pattern: String },
    // Future: label extraction, JSON parsing, etc.
}
```

### TraceQL — custom `nom` parser

No Rust crate exists. The 80/20 subset is small enough that a hand-rolled `nom` parser is tractable:

```rust
enum TraceQLExpr {
    SpanSelector { conditions: Vec<SpanCondition> },
    Structural { op: StructuralOp, lhs: Box<TraceQLExpr>, rhs: Box<TraceQLExpr> },
}

enum SpanCondition {
    Attribute { scope: AttrScope, name: String, op: CompareOp, value: SpanValue },
    Duration { op: CompareOp, value: Duration },
    Status { op: CompareOp, value: SpanStatus },
}
```

### OTLP ingestion — bought types, custom mapping

The `opentelemetry-proto` crate provides all protobuf-generated types. Ingestion is a mapping function from OTLP types to our internal store types:

```rust
use opentelemetry_proto::tonic::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::trace::v1::ExportTraceServiceRequest;

fn ingest_metrics(req: ExportMetricsServiceRequest, store: &mut MetricStore) {
    for resource_metrics in req.resource_metrics {
        let resource_attrs = extract_labels(&resource_metrics.resource);
        for scope_metrics in resource_metrics.scope_metrics {
            for metric in scope_metrics.metrics {
                // Map OTLP Metric → our MetricSeries + Samples
            }
        }
    }
}
```

### Shared primitives

- `LabelMatcher { name, op: Eq|Neq|Regex|NotRegex, value }` — shared across LogQL and TraceQL (PromQL matchers come from `promql-parser` crate)
- `Duration` parsing: `5m`, `1h`, `30s`, `500ms` — shared across LogQL and TraceQL (PromQL durations handled by crate)
- Timestamp parsing and step alignment for range queries

---

## Worktree Integration

### Multi-Service Model

A typical worktree runs multiple services — an API gateway, a core payments engine, background workers, maybe a risk evaluator. All services send telemetry to a single Obsidian instance. Service identity flows through naturally:

- **Metrics/Traces (OTLP):** The `resource.service.name` attribute is set by each service's OpenTelemetry SDK initialization. Obsidian extracts this on ingest and indexes it as a label, so `{service="payments-engine"}` and `{service="risk-evaluator"}` resolve to different posting lists.
- **Logs (Loki push / OTLP):** The sending service includes a `service` label in stream labels (Loki push) or sets `resource.service.name` (OTLP logs). Obsidian indexes it the same way in both cases.

Agents can query a single service (`{service="payments-engine"} |= "timeout"`), compare across services (`rate(http_requests_total{service=~"api-gateway|payments-engine"}[5m])`), or trace a request across service boundaries via TraceQL's structural operators.

### Service Discovery Endpoint

Agents need to enumerate what services are reporting telemetry:

| Endpoint | Description |
|---|---|
| `GET /api/v1/services` | Returns list of known service names across all three stores |

```json
{
  "status": "success",
  "data": ["api-gateway", "payments-engine", "risk-evaluator", "worker"]
}
```

Implementation: union of unique values for the `service` label key (logs/metrics) and the `service.name` resource attribute (traces) from the interners.

### Boot Script

The boot script starts Obsidian once, then boots all services pointing directly at it:

```bash
#!/bin/bash
TREE_ID=$(basename "$(git rev-parse --show-toplevel)")

if [ ! -f .env.local ]; then
  BASE_PORT=$(( ($(echo "$TREE_ID" | cksum | cut -d' ' -f1) % 1000) + 3000 ))
  OBSIDIAN_PORT=$(( BASE_PORT + 100 ))
  cat > .env.local <<EOF
BASE_PORT=$BASE_PORT
OBSIDIAN_PORT=$OBSIDIAN_PORT
DATABASE_URL=postgres://localhost/myapp_$TREE_ID
EOF
fi

source .env.local

# 1. Boot obsidian (single instance for all services)
obsidian \
  --port "$OBSIDIAN_PORT" \
  --snapshot-dir ".obsidian/" \
  --retention "2h" &

# 2. Boot services (each sends OTLP directly to Obsidian)
OTEL_EXPORTER_OTLP_ENDPOINT="http://127.0.0.1:${OBSIDIAN_PORT}" \
OTEL_SERVICE_NAME="api-gateway" \
  ./target/release/api-gateway --port $((BASE_PORT)) &

OTEL_EXPORTER_OTLP_ENDPOINT="http://127.0.0.1:${OBSIDIAN_PORT}" \
OTEL_SERVICE_NAME="payments-engine" \
  ./target/release/payments-engine --port $((BASE_PORT + 1)) &

OTEL_EXPORTER_OTLP_ENDPOINT="http://127.0.0.1:${OBSIDIAN_PORT}" \
OTEL_SERVICE_NAME="risk-evaluator" \
  ./target/release/risk-evaluator --port $((BASE_PORT + 2)) &

wait
```

---

## Build Plan

Using `promql-parser`, `opentelemetry-proto`, and `lasso` compresses the timeline from ~6 weeks to ~4 weeks by eliminating parser and protobuf work.

### Phase 1: Foundation + Ingestion (Week 1)

- Project scaffolding with `axum` + `tokio`
- CLI parsing with `clap`
- Storage engine structs (`LogStore`, `MetricStore`, `TraceStore`) with `parking_lot::RwLock` and `lasso::Rodeo` interning
- Inverted index with sorted posting lists and merge intersection
- Loki push API ingestion (JSON first, protobuf+snappy later)
- OTLP metrics ingestion using `opentelemetry-proto` types (gauge + counter)
- OTLP traces ingestion using `opentelemetry-proto` types
- Eviction background task
- Health/readiness endpoint (`GET /ready`)
- Service discovery endpoint (`GET /api/v1/services`) and status endpoint (`GET /api/v1/status`)
- Service label promotion: extract `resource.service.name` from OTLP and promote to `service` label

### Phase 2: PromQL (Week 2)

- Integrate `promql-parser` crate — parse incoming queries to AST
- Write `PromQLEvaluator` that walks the `promql-parser` AST against `MetricStore`
- Implement evaluation for: `rate()`, `increase()`, `sum()`, `avg()`, `max()`, `min()`, `count()`
- `by` / `without` grouping via label set operations
- Binary operators: arithmetic (`+ - * /`) and comparison (`> < >= <= == !=`)
- `histogram_quantile()` for p99/p95/p50
- `/api/v1/query`, `/api/v1/query_range`, `/api/v1/series`, labels endpoints
- Prometheus-compatible JSON response formatting

### Phase 3: LogQL (Week 3)

- Evaluate `logql` crate — adopt if AST covers our subset, otherwise `nom` parser (~2 days for the 80/20)
- Stream selectors with all four matcher types (`=`, `!=`, `=~`, `!~`)
- Line filter pipeline stages: `|=`, `!=`, `|~`, `!~`
- `/loki/api/v1/query` and `/query_range` with Loki-compatible response format
- `/loki/api/v1/labels` and `/label/{name}/values`
- `count_over_time()` and `rate()` metric queries over logs

### Phase 4: TraceQL + Snapshot + Polish (Week 4)

- Custom `nom` parser for TraceQL span selectors with attribute matching
- Duration filters (`duration > 500ms`)
- Logical operators `&&`, `||` within spansets
- `GET /api/traces/{traceID}` returning OTLP-compatible JSON
- `GET /api/search` with TraceQL `q` parameter
- Structural operator `>>` (descendant) if tractable
- `bincode` serialization of all stores (snapshot)
- SIGUSR1 handler for on-demand snapshot + `--restore` flag
- Timer-based auto-snapshot
- Loki protobuf+snappy ingestion path
- End-to-end integration test: service → Obsidian → query
- Worktree boot script

---

## What's Intentionally Out of Scope

- **Alerting / recording rules** — agents query on demand, no need for continuous evaluation
- **Multi-tenancy** — one Obsidian instance per worktree. Multiple services within a worktree are supported (separated by labels), but there's no tenant isolation between different users/teams
- **Distributed operation** — single-node only
- **Persistent WAL** — snapshots are sufficient for this use case
- **Full spec compliance** — we implement what agents need, not what Grafana Cloud needs
- **Authentication** — local-only, behind whatever network boundary the worktree boot script provides
- **Dashboard UI** — agents query via HTTP, humans can optionally point Grafana at it for debugging
