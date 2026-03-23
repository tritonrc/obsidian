//! OpenAPI 3.0 specification endpoint.

use axum::Json;
use serde_json::{Map, Value, json};

/// GET /api/v1/openapi.json — returns the OpenAPI 3.0.3 specification for all Obsidian endpoints.
pub async fn openapi_spec() -> Json<Value> {
    Json(spec())
}

fn protobuf_post(summary: &str, tag: &str) -> Value {
    json!({
        "post": {
            "summary": summary,
            "description": "Accepts protobuf (default, Content-Type: application/x-protobuf) or JSON (Content-Type: application/json) encoded OTLP payloads. Gzip compression is supported via Content-Encoding: gzip.",
            "tags": [tag],
            "requestBody": {
                "required": true,
                "content": {
                    "application/x-protobuf": {
                        "schema": { "type": "string", "format": "binary" }
                    },
                    "application/json": {
                        "schema": { "type": "object" }
                    }
                }
            },
            "responses": {
                "200": { "description": "Success" },
                "400": { "description": "Invalid payload" }
            }
        }
    })
}

fn simple_get(summary: &str, tag: &str) -> Value {
    json!({
        "get": {
            "summary": summary,
            "tags": [tag],
            "responses": {
                "200": { "description": "Success", "content": { "application/json": {} } }
            }
        }
    })
}

fn query_endpoint(summary: &str, tag: &str, params: Value) -> Value {
    json!({
        "get": {
            "summary": summary,
            "tags": [tag],
            "parameters": params,
            "responses": {
                "200": { "description": "Query results", "content": {
                    "application/json": { "schema": { "$ref": "#/components/schemas/QueryResponse" } }
                }},
                "400": { "description": "Invalid query" }
            }
        }
    })
}

fn labels_endpoint(summary: &str, tag: &str) -> Value {
    json!({
        "get": {
            "summary": summary,
            "tags": [tag],
            "responses": {
                "200": { "description": "Label names", "content": {
                    "application/json": { "schema": { "$ref": "#/components/schemas/LabelsResponse" } }
                }}
            }
        }
    })
}

/// Build the static OpenAPI 3.0.3 document.
fn spec() -> Value {
    let mut paths = Map::new();

    // Ingestion endpoints
    paths.insert(
        "/loki/api/v1/push".into(),
        json!({
            "post": {
                "summary": "Ingest logs via Loki push API",
                "tags": ["ingestion"],
                "requestBody": {
                    "required": true,
                    "content": {
                        "application/json": {
                            "schema": { "$ref": "#/components/schemas/LokiPushRequest" }
                        }
                    }
                },
                "responses": {
                    "204": { "description": "Logs ingested successfully" },
                    "400": { "description": "Invalid request body" }
                }
            }
        }),
    );
    paths.insert(
        "/v1/metrics".into(),
        protobuf_post("Ingest metrics via OTLP/HTTP", "ingestion"),
    );
    paths.insert(
        "/v1/traces".into(),
        protobuf_post("Ingest traces via OTLP/HTTP", "ingestion"),
    );
    paths.insert(
        "/v1/logs".into(),
        protobuf_post("Ingest logs via OTLP/HTTP", "ingestion"),
    );

    // LogQL query endpoints
    let query_params = json!([
        { "name": "query", "in": "query", "required": true, "schema": { "type": "string" }, "description": "LogQL query expression" },
        { "name": "time", "in": "query", "required": false, "schema": { "type": "string" }, "description": "Evaluation timestamp (RFC3339 or Unix)" },
        { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer" }, "description": "Maximum number of entries to return" }
    ]);
    paths.insert(
        "/loki/api/v1/query".into(),
        query_endpoint("Evaluate a LogQL instant query", "logql", query_params),
    );

    let range_params = json!([
        { "name": "query", "in": "query", "required": true, "schema": { "type": "string" }, "description": "LogQL query expression" },
        { "name": "start", "in": "query", "required": false, "schema": { "type": "string" }, "description": "Start timestamp" },
        { "name": "end", "in": "query", "required": false, "schema": { "type": "string" }, "description": "End timestamp" },
        { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer" }, "description": "Maximum number of entries to return" },
        { "name": "step", "in": "query", "required": false, "schema": { "type": "string" }, "description": "Query step" }
    ]);
    paths.insert(
        "/loki/api/v1/query_range".into(),
        query_endpoint("Evaluate a LogQL range query", "logql", range_params),
    );

    paths.insert(
        "/loki/api/v1/labels".into(),
        labels_endpoint("List all log label names", "logql"),
    );

    paths.insert(
        "/loki/api/v1/label/{name}/values".into(),
        json!({
            "get": {
                "summary": "List values for a log label",
                "tags": ["logql"],
                "parameters": [
                    { "name": "name", "in": "path", "required": true, "schema": { "type": "string" }, "description": "Label name" }
                ],
                "responses": {
                    "200": { "description": "Label values", "content": {
                        "application/json": { "schema": { "$ref": "#/components/schemas/LabelsResponse" } }
                    }}
                }
            }
        }),
    );

    // PromQL query endpoints
    let promql_query_params = json!([
        { "name": "query", "in": "query", "required": true, "schema": { "type": "string" }, "description": "PromQL query expression" },
        { "name": "time", "in": "query", "required": false, "schema": { "type": "string" }, "description": "Evaluation timestamp" }
    ]);
    paths.insert(
        "/api/v1/query".into(),
        query_endpoint(
            "Evaluate a PromQL instant query",
            "promql",
            promql_query_params,
        ),
    );

    let promql_range_params = json!([
        { "name": "query", "in": "query", "required": true, "schema": { "type": "string" }, "description": "PromQL query expression" },
        { "name": "start", "in": "query", "required": false, "schema": { "type": "string" }, "description": "Start timestamp" },
        { "name": "end", "in": "query", "required": false, "schema": { "type": "string" }, "description": "End timestamp" },
        { "name": "step", "in": "query", "required": false, "schema": { "type": "string" }, "description": "Query step" }
    ]);
    paths.insert(
        "/api/v1/query_range".into(),
        query_endpoint(
            "Evaluate a PromQL range query",
            "promql",
            promql_range_params,
        ),
    );

    paths.insert(
        "/api/v1/labels".into(),
        labels_endpoint("List all metric label names", "promql"),
    );

    paths.insert(
        "/api/v1/label/{name}/values".into(),
        json!({
            "get": {
                "summary": "List values for a metric label",
                "tags": ["promql"],
                "parameters": [
                    { "name": "name", "in": "path", "required": true, "schema": { "type": "string" }, "description": "Label name" }
                ],
                "responses": {
                    "200": { "description": "Label values", "content": {
                        "application/json": { "schema": { "$ref": "#/components/schemas/LabelsResponse" } }
                    }}
                }
            }
        }),
    );

    paths.insert(
        "/api/v1/series".into(),
        json!({
            "get": {
                "summary": "Find metric series matching label matchers",
                "tags": ["promql"],
                "parameters": [
                    { "name": "match[]", "in": "query", "required": true, "schema": { "type": "string" }, "description": "Series selector" },
                    { "name": "start", "in": "query", "required": false, "schema": { "type": "string" } },
                    { "name": "end", "in": "query", "required": false, "schema": { "type": "string" } }
                ],
                "responses": {
                    "200": { "description": "Matching series", "content": {
                        "application/json": { "schema": { "$ref": "#/components/schemas/SeriesResponse" } }
                    }}
                }
            }
        }),
    );

    // TraceQL endpoints
    paths.insert(
        "/api/search".into(),
        json!({
            "get": {
                "summary": "Search traces via TraceQL",
                "description": "When the `q` parameter is omitted or empty, returns the most recent traces (up to `limit`, default 20). This is the behavior Grafana Tempo expects for datasource health checks.",
                "tags": ["traceql"],
                "parameters": [
                    { "name": "q", "in": "query", "required": false, "schema": { "type": "string" }, "description": "TraceQL query expression. Omit to list recent traces." },
                    { "name": "start", "in": "query", "required": false, "schema": { "type": "integer" }, "description": "Start of time range — epoch seconds or nanoseconds (auto-detected)" },
                    { "name": "end", "in": "query", "required": false, "schema": { "type": "integer" }, "description": "End of time range — epoch seconds or nanoseconds (auto-detected)" },
                    { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer" }, "description": "Maximum number of traces to return (default 20)" }
                ],
                "responses": {
                    "200": { "description": "Search results", "content": {
                        "application/json": { "schema": { "$ref": "#/components/schemas/TraceSearchResponse" } }
                    }},
                    "400": { "description": "Invalid query" }
                }
            }
        }),
    );

    paths.insert(
        "/api/traces/{traceID}".into(),
        json!({
            "get": {
                "summary": "Get all spans for a trace by ID",
                "tags": ["traceql"],
                "parameters": [
                    { "name": "traceID", "in": "path", "required": true, "schema": { "type": "string" }, "description": "Hex-encoded 128-bit trace ID" }
                ],
                "responses": {
                    "200": { "description": "Trace data", "content": {
                        "application/json": { "schema": { "$ref": "#/components/schemas/TraceResponse" } }
                    }},
                    "400": { "description": "Invalid trace ID" },
                    "404": { "description": "Trace not found" }
                }
            }
        }),
    );

    // Discovery and management endpoints
    paths.insert(
        "/api/v1/services".into(),
        simple_get("List all known service names", "discovery"),
    );
    paths.insert(
        "/api/v1/status".into(),
        simple_get("Get store statistics and health info", "discovery"),
    );
    paths.insert(
        "/api/v1/diagnose".into(),
        json!({
            "get": {
                "summary": "Health assessment: global overview or per-service diagnosis",
                "tags": ["discovery"],
                "parameters": [
                    { "name": "service", "in": "query", "required": false, "schema": { "type": "string" }, "description": "Service name. Omit for global overview." }
                ],
                "responses": {
                    "200": { "description": "Health assessment", "content": { "application/json": {} } }
                }
            }
        }),
    );
    paths.insert(
        "/api/v1/catalog".into(),
        simple_get(
            "Get a catalog of all known metrics, labels, and services",
            "discovery",
        ),
    );
    paths.insert(
        "/api/v1/reset".into(),
        json!({
            "delete": {
                "summary": "Reset all in-memory stores",
                "tags": ["management"],
                "responses": {
                    "200": { "description": "Stores reset successfully" }
                }
            }
        }),
    );
    paths.insert(
        "/api/v1/metadata".into(),
        simple_get("Get metric metadata", "promql"),
    );
    paths.insert(
        "/api/v1/openapi.json".into(),
        simple_get("Get this OpenAPI specification", "discovery"),
    );
    paths.insert(
        "/ready".into(),
        json!({
            "get": {
                "summary": "Health check / readiness probe",
                "tags": ["health"],
                "responses": {
                    "200": { "description": "Service is ready", "content": {
                        "application/json": { "schema": {
                            "type": "object",
                            "properties": {
                                "status": { "type": "string", "example": "ready" }
                            }
                        }}
                    }}
                }
            }
        }),
    );

    // Components / schemas
    let components = json!({
        "schemas": {
            "LokiPushRequest": {
                "type": "object",
                "properties": {
                    "streams": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "stream": { "type": "object", "additionalProperties": { "type": "string" } },
                                "values": { "type": "array", "items": { "type": "array", "items": { "type": "string" } } }
                            }
                        }
                    }
                }
            },
            "QueryResponse": {
                "type": "object",
                "properties": {
                    "status": { "type": "string" },
                    "data": {
                        "type": "object",
                        "properties": {
                            "resultType": { "type": "string" },
                            "result": {}
                        }
                    }
                }
            },
            "LabelsResponse": {
                "type": "object",
                "properties": {
                    "status": { "type": "string" },
                    "data": { "type": "array", "items": { "type": "string" } }
                }
            },
            "SeriesResponse": {
                "type": "object",
                "properties": {
                    "status": { "type": "string" },
                    "data": { "type": "array", "items": { "type": "object", "additionalProperties": { "type": "string" } } }
                }
            },
            "TraceSearchResponse": {
                "type": "object",
                "properties": {
                    "traces": { "type": "array", "items": { "type": "object" } }
                }
            },
            "TraceResponse": {
                "type": "object",
                "properties": {
                    "batches": { "type": "array", "items": { "type": "object" } }
                }
            },
            "ServicesResponse": {
                "type": "object",
                "properties": {
                    "status": { "type": "string" },
                    "data": { "type": "object", "properties": {
                        "services": { "type": "array", "items": { "type": "object" } }
                    }}
                }
            },
            "StatusResponse": {
                "type": "object",
                "properties": {
                    "status": { "type": "string" },
                    "data": { "type": "object", "properties": {
                        "totalLogEntries": { "type": "integer" },
                        "totalMetricSeries": { "type": "integer" },
                        "totalMetricSamples": { "type": "integer" },
                        "totalSpans": { "type": "integer" },
                        "totalTraces": { "type": "integer" },
                        "uptimeSeconds": { "type": "integer" },
                        "serviceCount": { "type": "integer" },
                        "memoryBytes": { "type": "integer" }
                    }}
                }
            }
        }
    });

    let mut doc = Map::new();
    doc.insert("openapi".into(), json!("3.0.3"));
    doc.insert(
        "info".into(),
        json!({
            "title": "Obsidian",
            "description": "Lightweight ephemeral observability engine exposing LogQL, PromQL, and TraceQL query surfaces over in-memory stores.",
            "version": "0.4.0"
        }),
    );
    doc.insert("paths".into(), Value::Object(paths));
    doc.insert("components".into(), components);

    Value::Object(doc)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spec_has_openapi_version() {
        let s = spec();
        assert_eq!(s["openapi"], "3.0.3");
    }

    #[test]
    fn test_spec_has_all_paths() {
        let s = spec();
        let paths = s["paths"].as_object().unwrap();
        let expected = [
            "/loki/api/v1/query",
            "/loki/api/v1/query_range",
            "/loki/api/v1/labels",
            "/loki/api/v1/label/{name}/values",
            "/loki/api/v1/push",
            "/api/v1/query",
            "/api/v1/query_range",
            "/api/v1/labels",
            "/api/v1/label/{name}/values",
            "/api/v1/series",
            "/api/v1/services",
            "/api/v1/status",
            "/api/search",
            "/api/traces/{traceID}",
            "/v1/metrics",
            "/v1/traces",
            "/v1/logs",
            "/api/v1/diagnose",
            "/api/v1/catalog",
            "/api/v1/reset",
            "/api/v1/metadata",
            "/api/v1/openapi.json",
            "/ready",
        ];
        for path in &expected {
            assert!(paths.contains_key(*path), "missing path: {}", path);
        }
    }
}
