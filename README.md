# Aniani

Getting observability into a development harness is usually expensive: you need a metrics collector, a log aggregator, a trace backend, each with its own config, ports, and lifecycle. When you're running multiple agents in parallel across git worktrees — each worktree a full isolated environment — that cost multiplies.

Aniani collapses it to a single binary. One process, one port, three query surfaces (LogQL, PromQL, TraceQL) over in-memory stores. Services point their OTLP exporter at it directly — no collector, no sidecar, no config file. Drop it into your worktree boot script and it's gone when the worktree is.

The name comes from Hawaiian: *aniani* means "mirror; clear, transparent glass." The intent is the same here — reflect a running development environment back clearly, without turning observability into another service stack to operate.

---

## The Worktree Model

Each git worktree runs an independent copy of your services. Aniani boots alongside them:

```bash
#!/bin/bash
# boot.sh — start everything for this worktree

aniani --port 4320 --retention 2h &

OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4320 \
OTEL_SERVICE_NAME=api-gateway \
  ./api-gateway &

OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4320 \
OTEL_SERVICE_NAME=payments \
  ./payments-engine &
```

All services in the worktree share one Aniani instance. Each agent working in its own worktree gets its own isolated Aniani. When the worktree is discarded, so is the telemetry.

Running 8 agents in parallel across 8 worktrees means 8 independent Aniani instances — no cross-contamination, no coordination, no shared state to clean up.

---

## Install

### Homebrew (macOS, Linux) — recommended

```sh
brew install tritonrc/tap/aniani
```

Homebrew is the easiest path and avoids the macOS Gatekeeper "unidentified
developer" prompt that appears when you run a binary downloaded directly from
Releases. Upgrade with `brew upgrade aniani`.

### Direct download

Or grab a prebuilt binary from [Releases](https://github.com/tritonrc/aniani/releases):

```bash
# macOS (Apple Silicon)
curl -L https://github.com/tritonrc/aniani/releases/latest/download/aniani-macos-arm64.tar.gz | tar xz
chmod +x aniani && mv aniani /usr/local/bin/
# First run blocked by Gatekeeper? Clear the download quarantine flag:
#   xattr -d com.apple.quarantine /usr/local/bin/aniani

# Linux (x86_64)
curl -L https://github.com/tritonrc/aniani/releases/latest/download/aniani-linux-x86_64.tar.gz | tar xz
chmod +x aniani && mv aniani /usr/local/bin/
```

On Windows (x86_64), download `aniani-windows-x86_64.zip` from
[Releases](https://github.com/tritonrc/aniani/releases) and extract it (no
`chmod` needed); add the folder to your `PATH` or run `aniani.exe` directly:

```powershell
# PowerShell
Invoke-WebRequest -Uri https://github.com/tritonrc/aniani/releases/latest/download/aniani-windows-x86_64.zip -OutFile aniani.zip
Expand-Archive aniani.zip -DestinationPath .
.\aniani.exe --port 4320
```

### Docker

Multi-arch images (linux/amd64, linux/arm64) are published to GitHub Container
Registry on every release:

```bash
docker run --rm -p 4320:4320 ghcr.io/tritonrc/aniani:latest
```

Tags: `:latest` and `:0.13.2` / `:0.13` for releases, `:edge` for `master` once
its tests pass. The image is a statically linked binary on `scratch` — no shell, no
package manager — running as uid 65532. It defaults to `--bind-address 0.0.0.0`
(the binary's own `127.0.0.1` default is unreachable from outside a container)
and keeps snapshots in `/data`:

```bash
# Persist snapshots across container restarts
docker run --rm -p 4320:4320 -v aniani-data:/data ghcr.io/tritonrc/aniani:latest \
  --bind-address 0.0.0.0 --snapshot-dir /data/ --snapshot-interval 60

# Send telemetry from another container on the same network
docker network create o11y
docker run --rm -d --name aniani --network o11y ghcr.io/tritonrc/aniani:latest
#   ... then point OTEL_EXPORTER_OTLP_ENDPOINT at http://aniani:4320
```

Aniani has no authentication or TLS, so treat the published port as trusted-network
only — bind it to a loopback interface (`-p 127.0.0.1:4320:4320`) if the Docker
host is not private.

Or build from source on any platform:

```bash
cargo build --release
```

To build the container image locally (compiles a musl binary in a `rust:alpine`
container, then assembles the image for your architecture):

```bash
scripts/docker-image.sh aniani:dev
```

---

## Usage

```
aniani [OPTIONS]

OPTIONS:
    --bind-address <ADDR>          Bind address (default: 127.0.0.1)
    --port <PORT>                  Listen port (default: 4320)
    --snapshot-dir <PATH>          Snapshot directory (default: .aniani/)
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
aniani --port $PORT --retention 2h &
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:$PORT
```

The boot and port-derivation snippets above are Unix shell examples. On Windows,
the equivalent in PowerShell:

```powershell
$name = Split-Path -Leaf (git rev-parse --show-toplevel)
$port = ([System.BitConverter]::ToUInt32([System.Security.Cryptography.MD5]::Create().ComputeHash([Text.Encoding]::UTF8.GetBytes($name)), 0) % 1000) + 4000
Start-Process aniani.exe -ArgumentList "--port", $port, "--retention", "2h"
$env:OTEL_EXPORTER_OTLP_ENDPOINT = "http://localhost:$port"
```

To listen on all interfaces (e.g. when running in a container or accepting traffic from other hosts):

```bash
aniani --bind-address 0.0.0.0 --port 4320
```

Aniani disables permissive browser CORS by default. If you expose it beyond loopback, treat it as an unauthenticated internal service and front it with your own network controls or proxy.

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

**OTLP/gRPC:** The same three signals are also accepted over gRPC on the *same port* — gRPC (cleartext HTTP/2) is multiplexed with the HTTP surface, so there's no separate port or flag. Point an SDK's gRPC exporter at the base port with TLS disabled:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4320 \
OTEL_EXPORTER_OTLP_PROTOCOL=grpc \
OTEL_EXPORTER_OTLP_INSECURE=true \
OTEL_SERVICE_NAME=api-gateway \
  ./api-gateway &
```

The services are the standard `opentelemetry.proto.collector.{metrics,trace,logs}.v1.*Service/Export`, and gzip-compressed requests are accepted.

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

Aniani is designed for programmatic discovery. An agent investigating a system follows this flow:

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

### MCP (for agents)

Aniani also speaks the **Model Context Protocol** so a coding agent can drive it as a tool, not via hand-written HTTP. Point any MCP client (Streamable HTTP transport) at:

```
http://127.0.0.1:4320/mcp
```

It is always on — no separate process or port — and exposes 10 intent-level tools (one write). The `initialize` handshake returns `instructions` teaching the dev loop:

```
1. reset(scope=all)             — clean baseline before a run
2. run your code/tests          — telemetry export may lag a moment
3. summarize_activity(service)  — triage what the run produced
4. describe_service + query_*   — drill into logs/traces/metrics
5. mark_checkpoint() → since    — compare iterations without wiping
```

Tools: `reset` (the only write), `mark_checkpoint`, `summarize_activity`, `check_health`, `query_logs`, `query_traces`, `query_metrics`, `get_trace`, `list_services`, `describe_service`. Each returns concise text plus typed `structuredContent`; bad queries or unknown services come back as self-correcting tool errors. See `DESIGN.md` for the full surface.

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
POST   /api/v1/snapshot         — write a snapshot now (cross-platform; portable SIGUSR1 equivalent)
GET    /api/v1/metadata         — empty response (Grafana Prometheus datasource compat)
GET    /api/v1/openapi.json     — OpenAPI 3.0 spec for all endpoints
GET    /ready                   — health check (200 when ready)
```

---

## Web UI

Aniani ships an optional embedded web UI for reviewing logs, metrics, and traces.
It is compiled in by default — browse to:

    http://127.0.0.1:4320/ui

Tabs: **Overview** (services + status), **Logs** (LogQL), **Metrics** (PromQL),
and **Traces** (TraceQL). Enter a query and hit **Run**.

In **Traces**, click a result row to open a Jaeger-style waterfall: spans laid out
over a shared timeline, colored per service, in a collapsible parent/child tree.
Click any span to expand its detail — tags, process (resource) attributes, and a
timeline of events, with recorded **exceptions** surfaced (type, message, and
stack trace). The summary line shows total duration, span count, services, and
error count.

To populate a local instance with a realistic, multi-service trace set (a
checkout fanning out across seven services, including one failing trace with an
exception) plus a little logs/metrics:

    cargo run --example seed                 # targets http://127.0.0.1:4320
    cargo run --example seed -- http://host:port

The UI loads Vue from a CDN, so the **first page load needs internet access**.
To build without the UI (smaller binary, no embedded assets):

    cargo build --release --no-default-features

---

## Snapshots

Useful when an agent needs to hand off state to a new session:

```bash
# On-demand snapshot (cross-platform — works on Windows too)
curl -X POST http://localhost:4320/api/v1/snapshot

# On-demand snapshot via signal (Unix only)
kill -USR1 $(pgrep aniani)

# Auto-snapshot every 60 seconds
aniani --snapshot-interval 60 --snapshot-dir .aniani/

# Restore on next boot
aniani --restore --snapshot-dir .aniani/
```

`POST /api/v1/snapshot` is the portable trigger and is the recommended way to
take an on-demand snapshot on Windows, where there is no `SIGUSR1`. On Windows,
the on-shutdown snapshot also fires for console-close, logoff, and system
shutdown — but those handlers get only a brief grace window, so for large stores
prefer `--snapshot-interval` to guarantee a recent snapshot exists.

---

## What It's Not

Aniani is purpose-built for ephemeral agent workflows. It is not:

- A production observability backend
- A replacement for Loki, Prometheus, or Tempo
- Persistent (no WAL — snapshots only)
- Authenticated (local use only, no TLS)
- Clustered (single node, single process)
