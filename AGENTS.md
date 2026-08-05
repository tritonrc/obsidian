# AGENTS.md — Aniani Development Guidelines

## Project Overview

Aniani is a lightweight, ephemeral observability engine: a single Rust binary exposing LogQL, PromQL, and TraceQL query surfaces over in-memory stores. It receives telemetry via Loki push API, OTLP/HTTP, OTLP/gRPC, and Prometheus remote write — services send directly to Aniani. It is designed to be booted per git worktree for agent-driven development workflows.

Read `DESIGN.md` for the full technical specification before making changes.

---

## Rust Conventions

### Error Handling

- Use `thiserror` for library-style error enums in core modules (storage, parsers, evaluators).
- Use `anyhow` only in `main.rs` and HTTP handler glue where you need to propagate heterogeneous errors.
- Never use `.unwrap()` or `.expect()` in library code. Use `?` propagation.
- `.unwrap()` is acceptable only in tests and in `main()` for startup-fatal conditions (port bind failure, CLI parse failure).
- Define one error enum per module boundary. Don't create a god `Error` type.

```rust
// Good — module-scoped error
#[derive(Debug, thiserror::Error)]
pub enum LogQLError {
    #[error("parse error at position {pos}: {msg}")]
    Parse { pos: usize, msg: String },
    #[error("unknown label: {0}")]
    UnknownLabel(String),
}

// Bad — stringly typed
fn query(q: &str) -> Result<(), String> { ... }
```

### Module Organization

Follow this layout strictly:

```
src/
├── main.rs              # CLI parsing, server startup, signal handling
├── lib.rs               # Public module re-exports
├── server.rs            # Axum router setup, shared state, middleware
├── config.rs            # CLI args → Config struct via clap
├── store/
│   ├── mod.rs           # Re-exports, SharedStore type alias
│   ├── log_store.rs     # LogStore, LogStream, LogEntry
│   ├── metric_store.rs  # MetricStore, MetricSeries, Sample
│   ├── trace_store.rs   # TraceStore, Span, SpanStatus, AttributeValue
│   └── posting_list.rs  # PostingList with sorted insert, merge intersect/union
├── ingest/
│   ├── mod.rs
│   ├── loki.rs          # POST /loki/api/v1/push — JSON and protobuf+snappy
│   ├── otlp_logs.rs     # POST /v1/logs — OTLP protobuf log decoding
│   ├── otlp_metrics.rs  # POST /v1/metrics — OTLP protobuf decoding
│   ├── otlp_traces.rs   # POST /v1/traces — OTLP protobuf decoding
│   ├── remote_write.rs  # POST /api/v1/write — Prometheus remote write (snappy+protobuf)
│   └── label.rs         # Service label promotion, resource attr extraction
├── query/
│   ├── mod.rs
│   ├── logql/
│   │   ├── mod.rs
│   │   ├── parser.rs    # nom parser for LogQL
│   │   ├── eval.rs      # LogQL evaluator against LogStore
│   │   └── handlers.rs  # Axum handlers for /loki/api/v1/*
│   ├── promql/
│   │   ├── mod.rs
│   │   ├── eval.rs      # PromQL evaluator walking promql-parser AST
│   │   └── handlers.rs  # Axum handlers for /api/v1/query, /api/v1/query_range, etc.
│   └── traceql/
│       ├── mod.rs
│       ├── parser.rs    # nom parser for TraceQL
│       ├── eval.rs      # TraceQL evaluator against TraceStore
│       └── handlers.rs  # Axum handlers for /api/search, /api/traces/{traceID}
├── grpc.rs              # OTLP/gRPC collector services (logs, metrics, traces) over the shared HTTP listener
├── snapshot.rs          # Bincode serialize/deserialize, SIGUSR1 handler
├── mcp/                 # MCP server (JSON-RPC over Streamable HTTP) — shares the listener like grpc.rs
│   ├── mod.rs           # routes(): mount POST/GET/DELETE /mcp
│   ├── protocol.rs      # JSON-RPC envelope types, error codes, protocol-version constants
│   ├── server.rs        # POST /mcp dispatch + transport compliance (202/batch/origin/version)
│   ├── tools/           # tool descriptors, tools/list, tools/call dispatch, the 10 handlers, instructions
│   │   ├── mod.rs       # public tools surface: INSTRUCTIONS, list, call
│   │   ├── args/        # one typed struct per tool — the source of both inputSchema and deserialization
│   │   │   ├── mod.rs   # the 10 tool_args! declarations; add a tool's arguments here
│   │   │   └── schema.rs # tool_args!/str_enum! macros, schema generation, argument validation
│   │   ├── registry.rs  # the tool table: name → args type → handler → advertised metadata, once each
│   │   ├── dispatch.rs  # tools/call request shape + typed arg parsing, then hands off to the table
│   │   └── handlers/
│   └── synth/           # transport-free typed cores (summarize_activity, check_health, describe_service, trace tree)
│       ├── mod.rs       # public synth surface
│       ├── activity.rs
│       ├── health.rs
│       ├── catalog.rs
│       └── trace.rs
└── api/
    ├── mod.rs
    ├── catalog.rs       # GET /api/v1/catalog — per-service metric/label catalog
    ├── diagnose.rs      # GET /api/v1/diagnose — health assessment per service
    ├── health.rs        # GET /ready
    ├── metadata.rs      # GET /api/v1/metadata — Grafana Prometheus datasource compat
    ├── openapi.rs       # GET /api/v1/openapi — OpenAPI 3.0 spec
    ├── reset.rs         # POST /api/v1/reset — store reset
    ├── services.rs      # GET /api/v1/services
    ├── status.rs        # GET /api/v1/status
    └── summary.rs       # GET /api/v1/summary — unified error summary across signals
```

### MCP Tool Arguments

Never read a tool argument out of a `&Value` in a handler. Declare it in `src/mcp/tools/args/mod.rs` and let the handler take the typed struct:

```rust
tool_args! {
    struct QueryTracesArgs for "query_traces" {
        service: Option<String>,   // Option<T> → optional
        status: Option<TraceStatus>, // str_enum! type → advertises its own values
        limit: Option<u64>,        // u64 → advertises minimum/maximum
        traceql: Option<String>,
    }
}
```

Then add one entry to the table in `registry.rs`, which is the only place a tool name appears:

```rust
"query_traces"(QueryTracesArgs) => handle_query_traces { "title": ..., "description": ..., ... },
```

The entry that advertises the schema is the entry that dispatches, so a tool cannot advertise a contract it does not honor — mismatching them is a type error. Three consequences to expect: a field no handler touches is a `field is never read` build failure (intentional — an argument accepted and ignored is invisible to the caller, though the lint cannot tell reading from applying, so deliberate non-use like `query_logs`' raw-`logql` branch is fine); a closed value set belongs in a `str_enum!` matched exhaustively, not a string comparison that falls through to a default; and only `String`, `u64`, `str_enum!` types, and `Option` of those are supported field types — anything else needs a deliberate `ArgType` impl. Keep semantic validation — duration parsing, hex length, "does this service exist" — in the handler.

### Naming

- Types: `PascalCase` — `MetricStore`, `PostingList`, `LogQLExpr`
- Functions/methods: `snake_case` — `ingest_metrics`, `merge_intersect`
- Constants: `SCREAMING_SNAKE_CASE` — `DEFAULT_PORT`, `MAX_LABEL_LENGTH`
- Module files: `snake_case` — `log_store.rs`, `otlp_metrics.rs`
- Avoid abbreviations in public APIs. `timestamp_ns` is fine; `ts_n` is not.

### Types and Ownership

- Prefer `&str` over `String` in function parameters.
- Use `Cow<'_, str>` when a function might or might not need to allocate.
- Use newtypes for domain identifiers to prevent mixing them up:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SeriesId(pub u64);
```

- `lasso::Spur` is already a newtype over `u32`. Use it directly for interned strings.
- Derive `Debug` on all types. Derive `Clone, Copy` on small value types.
- Derive `serde::Serialize, serde::Deserialize` on types that appear in JSON responses or snapshots.

### Concurrency

- All stores are behind `parking_lot::RwLock`. The shared state type should be:

```rust
pub struct AppState {
    pub log_store: RwLock<LogStore>,
    pub metric_store: RwLock<MetricStore>,
    pub trace_store: RwLock<TraceStore>,
    pub config: Config,
    pub start_time: Instant,
}

pub type SharedState = Arc<AppState>;
```

- Hold read locks for queries, write locks for ingestion and eviction.
- Never hold a write lock across an `.await` point. Do the computation, then lock, mutate, unlock.
- The eviction background task acquires write locks briefly. Design eviction to collect IDs first (under read lock), then batch-remove (under write lock).

### Performance

- Use `FxHashMap` (from `rustc-hash`) instead of `std::collections::HashMap` for internal data structures. The keys (Spur, u64) don't need DOS-resistant hashing.
- `PostingList` must keep its `Vec<u64>` sorted. Use `binary_search` for insert and lookup. Intersection is O(n+m) sorted merge.
- Avoid allocations in the hot path (ingestion). Pre-allocate `Vec` capacity where sizes are known.
- Use `SmallVec<[T; N]>` (from `smallvec`) for label sets on streams/series — most have ≤8 labels.
- Intern strings on ingest, never in the query path.
- Compile with `lto = true` and `codegen-units = 1` in the release profile.

### Axum Patterns

- Use `State(state): State<SharedState>` extractor in all handlers.
- Use `Query<T>` extractor for query string params. Define a typed struct per endpoint:

```rust
#[derive(Deserialize)]
pub struct QueryRangeParams {
    pub query: String,
    pub start: Option<String>,
    pub end: Option<String>,
    pub step: Option<String>,
}
```

- Return `axum::Json<T>` for all query responses.
- Return appropriate HTTP status codes: 200 for success, 400 for parse errors, 500 for internal errors.
- Use `axum::extract::Path` for path parameters like `{traceID}` and `{name}`.
- Keep handlers thin: parse request → call evaluator → format response. Business logic lives in `eval.rs`, not in handlers.

### Testing

- Every module should have `#[cfg(test)] mod tests { ... }` at the bottom.
- Test parsers with a table of `(input, expected_ast)` pairs.
- Test evaluators by constructing a store with known data, running a query, and asserting the result.
- Test inverted index operations (insert, intersect, union, eviction) in isolation.
- Test HTTP handlers with `axum::test::TestClient` or by calling handler functions directly with constructed extractors.
- Integration tests go in `tests/` directory. Key integration tests:
  - Ingest Loki push JSON → query with LogQL → verify results
  - Ingest OTLP metrics protobuf → query with PromQL → verify results  
  - Ingest OTLP traces protobuf → query with TraceQL → verify results
  - Multi-service: ingest from two services → query each independently → query both with regex
  - Eviction: ingest → wait past retention → verify entries are gone
  - Snapshot: ingest → snapshot → new instance with restore → verify data
- Use `#[tokio::test]` for async tests.
- Name tests descriptively: `test_logql_stream_selector_with_regex_matcher`, not `test_1`.

### Documentation

- All public types and functions get `///` doc comments.
- Module-level `//!` comments explaining the module's purpose and key types.
- No doc comments on private implementation details unless the logic is non-obvious.
- Use `# Examples` sections in doc comments for key public APIs.

---

## Key Dependencies — Version Pinning

Pin these exact versions in `Cargo.toml` to avoid surprises:

```toml
[dependencies]
axum = "0.8"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
clap = { version = "4", features = ["derive"] }
parking_lot = "0.12"
lasso = { version = "0.7", features = ["serialize"] }
promql-parser = "0.8"
opentelemetry-proto = { version = "0.31", features = ["gen-tonic-messages", "with-serde"] }
prost = "0.14"
tonic = { version = "0.14", default-features = false, features = ["server", "router", "gzip"] }
nom = "8"
regex = "1"
bincode = "1"
snap = "1"
flate2 = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
thiserror = "2"
anyhow = "1"
rustc-hash = "2"
smallvec = { version = "1", features = ["serde"] }
bytes = "1"
http = "1"
tower-http = { version = "0.6", features = ["cors"] }

[profile.release]
lto = true
codegen-units = 1
strip = true
```

If `opentelemetry-proto` or `promql-parser` versions have breaking changes, check their changelogs and adapt. The feature flags above are critical — `gen-tonic-messages` on `opentelemetry-proto` generates the OTLP message types *and* the tonic service stubs (client + server); `tonic` is then added directly with only `server` + `router` + `gzip` to supply the gRPC server runtime those stubs plug into, multiplexed onto the existing HTTP listener via `Routes::into_axum_router` — so no separate gRPC port and no `transport` feature are needed (do NOT switch `opentelemetry-proto` to the `gen-tonic` feature; that only adds the client `Channel`); `with-serde` enables serde support on OTLP types; `serialize` on `lasso` enables bincode snapshot support; `cors` on `tower-http` enables CORS middleware for cross-origin access.

---

## Git Conventions

- Commit messages: `module: short description` — e.g., `store/posting_list: add sorted merge intersection`
- One logical change per commit.
- Feature branches off `main`.
- `cargo fmt` and `cargo clippy` must pass before commit.
- `cargo test` must pass before push.

---

## What NOT to Build

- No authentication or TLS. This runs on localhost for ephemeral worktree use. OTLP/gRPC is served cleartext (h2c) only — never enable a tonic `tls-*` feature.
- No WAL or durable storage beyond snapshots.
- No streaming/push query results. All queries are request/response.
- No query caching. Stores are small enough that re-evaluation is fast.
- No custom allocator. The default allocator is fine for this workload.
- Do NOT implement PromQL parsing — use the `promql-parser` crate.
- Do NOT implement OTLP protobuf types — use `opentelemetry-proto`.
- Do NOT implement string interning — use `lasso`.
