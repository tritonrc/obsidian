# Obsidian

Getting observability into a development harness is usually expensive: you need a metrics collector, a log aggregator, a trace backend, each with its own config, ports, and lifecycle. When you're running multiple agents in parallel across git worktrees — each worktree a full isolated environment — that cost multiplies.

Obsidian collapses it to a single binary. One process, one port, three query surfaces (LogQL, PromQL, TraceQL) over in-memory stores. Drop it into your worktree boot script and it's gone when the worktree is. Services point their OTLP exporter at it directly — no collector, no sidecar, no config file.

---

## The Worktree Model

Each git worktree runs an independent copy of your services. Obsidian boots alongside them:

```bash
#!/bin/bash
# boot.sh — start everything for this worktree

obsidian --port 4320 --retention 2h &

OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4320 \
OTEL_SERVICE_NAME=api-gateway \
  ./api-gateway &

OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4320 \
OTEL_SERVICE_NAME=payments \
  ./payments-engine &
```

All services in the worktree share one Obsidian instance. Each agent working in its own worktree gets its own isolated Obsidian. When the worktree is discarded, so is the telemetry.

Running 8 agents in parallel across 8 worktrees means 8 independent Obsidian instances — no cross-contamination, no coordination, no shared state to clean up.

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

For parallel worktrees, derive the port from the worktree name to avoid conflicts:

```bash
PORT=$(( ($(basename $(git rev-parse --show-toplevel) | cksum | cut -d' ' -f1) % 1000) + 4000 ))
obsidian --port $PORT --retention 2h &
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:$PORT
```

---

## Ingestion

All ingest endpoints share the same port as queries — no separate collector process.

| Signal | Endpoint | Format |
|--------|----------|--------|
| Logs | `POST /loki/api/v1/push` | Loki JSON or Protobuf+Snappy |
| Metrics | `POST /v1/metrics` | OTLP protobuf |
| Traces | `POST /v1/traces` | OTLP protobuf |

The `resource.service.name` attribute from OTLP is promoted to a `service` label in all stores. This means `{service="payments"}` works the same in LogQL and PromQL, and agents can query by service name without knowing which signal type to look in first.

---

## Querying

### Service Discovery — start here

Before writing targeted queries, find out what's reporting:

```
GET /api/v1/services
```

```json
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

### LogQL

```
GET /loki/api/v1/query?query={service="payments"}&limit=100
GET /loki/api/v1/query_range?query={service="payments"}&start=...&end=...&step=30s
GET /loki/api/v1/labels
GET /loki/api/v1/label/{name}/values
```

```logql
{service="payments", level="error"}
{service=~"pay.*"} |= "timeout"
{service="payments"} |~ "error|warn"
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

```promql
http_requests_total{service=~"api-gateway|payments"}
rate(http_requests_total[5m])
sum(rate(http_requests_total[5m])) by (service)
histogram_quantile(0.99, rate(request_duration_bucket[5m]))
rate(errors_total[5m]) / rate(requests_total[5m]) * 100
```

### TraceQL

```
GET /api/search?q={resource.service.name="payments"&&duration>500ms}
GET /api/traces/{traceID}
```

```traceql
{ resource.service.name = "payments" && duration > 200ms }
{ span.http.status_code = 500 }
{ status = error || span.http.status_code >= 500 }
```

---

## Status and Health

```
GET /api/v1/status    — entry/sample/span counts, uptime
GET /ready            — health check (200 when ready)
```

---

## Snapshots

Useful when an agent needs to hand off state to a new session:

```bash
# On-demand snapshot
kill -USR1 $(pgrep obsidian)

# Auto-snapshot every 60 seconds
obsidian --snapshot-interval 60 --snapshot-dir .obsidian/

# Restore on next boot
obsidian --restore --snapshot-dir .obsidian/
```

---

## What It's Not

Obsidian is purpose-built for ephemeral agent workflows. It is not:

- A production observability backend
- A replacement for Loki, Prometheus, or Tempo
- Persistent (no WAL — snapshots only)
- Authenticated (local use only, no TLS)
- Clustered (single node, single process)
