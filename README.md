# Obsidian

Lightweight, ephemeral observability engine. A single Rust binary that exposes LogQL, PromQL, and TraceQL query surfaces over in-memory stores — designed to be booted per git worktree so agents can inspect logs, metrics, and traces while they work.

Services send OTLP directly to Obsidian. All three signal types share a single port. Everything lives in memory with optional snapshot-to-disk.

---

## Install

Download a prebuilt binary from [Releases](https://github.com/tritonrc/obsidian/releases):

```bash
# macOS (Apple Silicon)
curl -L https://github.com/tritonrc/obsidian/releases/latest/download/obsidian-macos-arm64.tar.gz | tar xz
chmod +x obsidian && mv obsidian /usr/local/bin/

# Linux (x86_64)
curl -L https://github.com/tritonrc/obsidian/releases/latest/download/obsidian-linux-x86_64.tar.gz | tar xz
chmod +x obsidian && mv obsidian /usr/local/bin/
```

Or build from source:

```bash
cargo build --release
```

---

## Usage

```
obsidian [OPTIONS]

OPTIONS:
    --port <PORT>                  Listen port (default: 4320)
    --snapshot-dir <PATH>          Snapshot directory (default: .obsidian/)
    --snapshot-interval <SECS>     Auto-snapshot interval, 0 to disable (default: 0)
    --max-log-entries <N>          Max log entries before eviction (default: 100000)
    --max-samples <N>              Max metric samples before eviction (default: 10000)
    --max-spans <N>                Max trace spans before eviction (default: 100000)
    --retention <DURATION>         Max age before eviction, e.g. "1h" (default: "2h")
    --restore                      Restore from snapshot on startup
```

Basic start:

```bash
obsidian --port 4320 --retention 2h
```

---

## Ingestion

All ingest endpoints accept data on the same port as queries.

| Signal | Endpoint | Format |
|--------|----------|--------|
| Logs | `POST /loki/api/v1/push` | Loki JSON or Protobuf+Snappy |
| Metrics | `POST /v1/metrics` | OTLP protobuf |
| Traces | `POST /v1/traces` | OTLP protobuf |

Configure your services to export OTLP to `http://localhost:4320`. The `resource.service.name` attribute is promoted to a `service` label in all stores, so `{service="payments"}` works identically in LogQL and PromQL.

---

## Querying

### LogQL

```
GET /loki/api/v1/query?query={service="payments"}&limit=100
GET /loki/api/v1/query_range?query={service="payments"}&start=...&end=...&step=30s
GET /loki/api/v1/labels
GET /loki/api/v1/label/{name}/values
```

Supported syntax:

```logql
# Stream selectors
{service="payments", level="error"}
{service=~"pay.*"}
{level!="debug"}

# Line filters
{service="payments"} |= "timeout"
{service="payments"} |~ "error|warn"
{service="payments"} != "healthcheck"

# Metric queries
rate({service="payments"} |= "error" [1m])
count_over_time({service="payments"}[5m])
```

### PromQL

```
GET /api/v1/query?query=rate(http_requests_total[5m])
GET /api/v1/query_range?query=...&start=...&end=...&step=30s
GET /api/v1/series?match[]=http_requests_total
GET /api/v1/labels
GET /api/v1/label/{name}/values
```

Supported syntax:

```promql
# Selectors and label matchers
http_requests_total{method="GET", status!="500"}
http_requests_total{service=~"api-gateway|payments"}

# Functions
rate(http_requests_total[5m])
increase(http_requests_total[1h])
histogram_quantile(0.99, rate(request_duration_bucket[5m]))

# Aggregation
sum(rate(http_requests_total[5m])) by (service)
avg(request_duration_seconds) by (endpoint)

# Binary operators
rate(errors_total[5m]) / rate(requests_total[5m]) * 100
```

### TraceQL

```
GET /api/search?q={resource.service.name="payments"&&duration>500ms}
GET /api/traces/{traceID}
```

Supported syntax:

```traceql
{ resource.service.name = "payments" }
{ span.http.status_code = 500 }
{ status = error }
{ duration > 500ms }
{ resource.service.name = "payments" && duration > 200ms }
{ status = error || span.http.status_code >= 500 }
```

---

## Service Discovery

```
GET /api/v1/services   — all known service names across logs, metrics, and traces
GET /api/v1/status     — entry/sample/span counts, uptime
GET /ready             — health check
```

```json
// GET /api/v1/services
{
  "status": "success",
  "data": {
    "services": [
      { "name": "api-gateway", "signals": ["logs", "metrics", "traces"] },
      { "name": "payments",    "signals": ["logs", "metrics", "traces"] }
    ]
  }
}
```

---

## Worktree Setup

Start Obsidian once, then point all services at it:

```bash
obsidian --port 4320 --retention 2h &

# Services export OTLP directly to Obsidian
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4320 \
OTEL_SERVICE_NAME=api-gateway \
  ./api-gateway &

OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4320 \
OTEL_SERVICE_NAME=payments \
  ./payments-engine &
```

For logs, configure your logging library or Vector to push to `http://localhost:4320/loki/api/v1/push`.

---

## Snapshots

```bash
# Manual snapshot via signal
kill -USR1 $(pgrep obsidian)

# Auto-snapshot every 60 seconds
obsidian --snapshot-interval 60 --snapshot-dir .obsidian/

# Restore on next boot
obsidian --restore --snapshot-dir .obsidian/
```

---

## What It's Not

- No authentication or TLS — local use only
- No WAL or durable storage (snapshots are sufficient)
- No alerting or recording rules
- No streaming queries
- No gRPC (OTLP/HTTP only)
- Not a replacement for Loki, Prometheus, or Tempo in production
