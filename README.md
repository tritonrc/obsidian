# Obsidian

Getting observability into a development harness is usually expensive: you need a metrics collector, a log aggregator, a trace backend, each with its own config, ports, and lifecycle. When you're running multiple agents in parallel across git worktrees — each worktree a full isolated environment — that cost multiplies.

Obsidian collapses it to a single binary. One process, one port, three query surfaces (LogQL, PromQL, TraceQL) over in-memory stores. Services point their OTLP exporter at it directly — no collector, no sidecar, no config file. Drop it into your worktree boot script and it's gone when the worktree is.

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
    --bind-address <ADDR>          Bind address (default: 127.0.0.1)
    --port <PORT>                  Listen port (default: 4320)
    --snapshot-dir <PATH>          Snapshot directory (default: .obsidian/)
    --snapshot-interval <SECS>     Auto-snapshot interval, 0 to disable (default: 0)
    --max-log-entries <N>          Max log entries before eviction (default: 100000)
    --max-series <N>               Max metric series before eviction (default: 10000)
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

To listen on all interfaces (e.g. when running in a container or accepting traffic from other hosts):

```bash
obsidian --bind-address 0.0.0.0 --port 4320
```

---

## Ingestion

All ingest endpoints share the same port as queries — no separate collector process.

| Signal  | Endpoint                   | Format                                  |
|---------|----------------------------|-----------------------------------------|
| Logs    | `POST /loki/api/v1/push`   | Loki JSON or Snappy-compressed JSON     |
| Logs    | `POST /v1/logs`            | OTLP protobuf or JSON                  |
| Metrics | `POST /v1/metrics`         | OTLP protobuf or JSON                  |
| Metrics | `POST /api/v1/write`       | Prometheus remote write (snappy protobuf) |
| Traces  | `POST /v1/traces`          | OTLP protobuf or JSON                  |

**OTLP JSON support:** All OTLP endpoints (`/v1/logs`, `/v1/metrics`, `/v1/traces`) accept `Content-Type: application/json` in addition to the default protobuf encoding. All OTLP endpoints also accept gzip-compressed bodies (`Content-Encoding: gzip`).

**Ingest responses:** Successful ingestion returns a JSON acknowledgment with counts:

```json
// POST /loki/api/v1/push
{"accepted": {"streams": 2, "entries": 15}}

// POST /v1/metrics
{"accepted": {"series": 4, "samples": 4}}

// POST /v1/traces
{"accepted": {"traces": 1, "spans": 8}}
```

`POST /v1/logs` and `POST /api/v1/write` return `204 No Content`.

The `resource.service.name` attribute from OTLP is promoted to a `service` label in all stores. This means `{service="payments"}` works the same in LogQL and PromQL, and agents can query by service name without knowing which signal type to look in first.

---

## Agent Workflow

Obsidian is designed for programmatic discovery. An agent investigating a system follows this flow:

```
1. GET /api/v1/services          — what services are reporting?
2. GET /api/v1/catalog?service=X — what metrics, labels, and span attributes does X have?
3. GET /api/v1/diagnose?service=X — health score, errors, slow traces, suggested queries
4. Run targeted LogQL/PromQL/TraceQL queries based on the above
5. DELETE /api/v1/reset           — clean up when done (optional)
```

**`/api/v1/diagnose`** returns a health assessment including: health score (0-100), error rate, p99 latency, slowest traces, error trend direction, recent error logs, key metrics with sparklines, and suggested follow-up queries.

**`/api/v1/catalog`** returns the metric names, log label keys, and span attribute keys for a service — so agents know what to query before constructing expressions.

**`/api/v1/summary`** returns all errors (logs, metrics, traces) for a service in one response.

---

## Querying

### LogQL

```
GET  /loki/api/v1/query?query={service="payments"}&limit=100
POST /loki/api/v1/query       (form-encoded: query, limit, time)
GET  /loki/api/v1/query_range?query={service="payments"}&start=...&end=...&step=30s
POST /loki/api/v1/query_range  (form-encoded: query, start, end, step, limit)
GET  /loki/api/v1/labels
GET  /loki/api/v1/label/{name}/values
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
GET  /api/v1/query?query=rate(http_requests_total[5m])
POST /api/v1/query             (form-encoded: query, time)
GET  /api/v1/query_range?query=...&start=...&end=...&step=30s
POST /api/v1/query_range       (form-encoded: query, start, end, step)
GET  /api/v1/series?match[]=http_requests_total
GET  /api/v1/labels
GET  /api/v1/label/{name}/values
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

## Management and Health

```
GET    /api/v1/services        — list services and their signal types
GET    /api/v1/status          — entry/sample/span counts, uptime
GET    /api/v1/diagnose?service=X — health assessment with suggested queries
GET    /api/v1/catalog?service=X  — metric names, log labels, span attributes
GET    /api/v1/summary?service=X  — all errors for a service across signals
DELETE /api/v1/reset            — clear all stores (or ?service=X for one service)
GET    /api/v1/metadata         — empty response (Grafana Prometheus datasource compat)
GET    /api/v1/openapi.json     — OpenAPI 3.0 spec for all endpoints
GET    /ready                   — health check (200 when ready)
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
