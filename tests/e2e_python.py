#!/usr/bin/env python3
"""
External black-box E2E test for Obsidian.

Boots the release binary, sends telemetry via HTTP, queries every endpoint,
and verifies responses. Uses only Python stdlib — no pip dependencies.

Usage:
    cargo build --release
    python3 tests/e2e_python.py
"""

import gzip
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BINARY = os.path.join(os.path.dirname(__file__), "..", "target", "release", "obsidian")
PASS = 0
FAIL = 0
TOTAL = 0


def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http(method, url, body=None, headers=None, expect_status=200):
    """Make an HTTP request, return (status, parsed_json_or_None)."""
    hdrs = headers or {}
    if body is not None and isinstance(body, str):
        body = body.encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=hdrs, method=method)
    try:
        resp = urllib.request.urlopen(req)
        data = resp.read()
        try:
            return resp.status, json.loads(data)
        except (json.JSONDecodeError, ValueError):
            return resp.status, data.decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        data = e.read()
        try:
            return e.code, json.loads(data)
        except (json.JSONDecodeError, ValueError):
            return e.code, data.decode("utf-8", errors="replace")


def check(name, condition, detail=""):
    global PASS, FAIL, TOTAL
    TOTAL += 1
    if condition:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name} — {detail}")


def wait_for_ready(base, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, _ = http("GET", f"{base}/ready")
            if status == 200:
                return True
        except Exception:
            pass
        time.sleep(0.1)
    return False


def main():
    global PASS, FAIL, TOTAL

    port = find_free_port()
    base = f"http://127.0.0.1:{port}"

    # Boot Obsidian
    proc = subprocess.Popen(
        [BINARY, "--port", str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        print(f"\nBooting Obsidian on port {port}...")
        if not wait_for_ready(base):
            print("FATAL: Obsidian did not become ready")
            sys.exit(1)
        print(f"Obsidian ready (PID {proc.pid})\n")

        now_ns = int(time.time() * 1e9)
        now_s = int(time.time())
        now_ms = int(time.time() * 1000)

        # ===================================================================
        # PHASE 1: INGEST
        # ===================================================================
        print("=== INGEST PHASE ===")

        # 1a. Loki JSON push with ack counts
        loki_body = json.dumps({
            "streams": [
                {
                    "stream": {"service": "payment-svc", "level": "error"},
                    "values": [
                        [str(now_ns), "card declined by issuer"],
                        [str(now_ns + 1), "database timeout on charge"],
                    ],
                },
                {
                    "stream": {"service": "payment-svc", "level": "info"},
                    "values": [
                        [str(now_ns + 2), "payment completed ok"],
                    ],
                },
            ]
        })
        status, body = http("POST", f"{base}/loki/api/v1/push", loki_body,
                            {"Content-Type": "application/json"})
        check("Loki push returns 200", status == 200, f"got {status}")
        check("Loki ack streams=2", body.get("accepted", {}).get("streams") == 2,
              f"got {body}")
        check("Loki ack entries=3", body.get("accepted", {}).get("entries") == 3,
              f"got {body}")

        # 1b. OTLP metrics (JSON encoding since we can't use protobuf from Python easily)
        metrics_json = json.dumps({
            "resourceMetrics": [{
                "resource": {
                    "attributes": [{"key": "service.name", "value": {"stringValue": "payment-svc"}}]
                },
                "scopeMetrics": [{
                    "metrics": [{
                        "name": "http_requests_total",
                        "gauge": {
                            "dataPoints": [{
                                "timeUnixNano": str(now_ns),
                                "asDouble": 142.0,
                                "attributes": [
                                    {"key": "method", "value": {"stringValue": "POST"}}
                                ]
                            }]
                        }
                    }]
                }]
            }]
        })
        status, body = http("POST", f"{base}/v1/metrics", metrics_json,
                            {"Content-Type": "application/json"})
        check("OTLP metrics JSON returns 200", status == 200, f"got {status}")
        check("OTLP metrics ack series=1",
              body.get("accepted", {}).get("series") == 1, f"got {body}")

        # 1c. OTLP traces (JSON encoding)
        trace_id_hex = "aa" * 16
        span_id_hex = "bb" * 8
        traces_json = json.dumps({
            "resourceSpans": [{
                "resource": {
                    "attributes": [{"key": "service.name", "value": {"stringValue": "payment-svc"}}]
                },
                "scopeSpans": [{
                    "spans": [{
                        "traceId": trace_id_hex,
                        "spanId": span_id_hex,
                        "name": "charge-card",
                        "kind": 1,
                        "startTimeUnixNano": str(now_ns),
                        "endTimeUnixNano": str(now_ns + 500_000_000),
                        "status": {"code": 2, "message": ""},
                        "attributes": [
                            {"key": "http.method", "value": {"stringValue": "POST"}}
                        ]
                    }]
                }]
            }]
        })
        status, body = http("POST", f"{base}/v1/traces", traces_json,
                            {"Content-Type": "application/json"})
        check("OTLP traces JSON returns 200", status == 200, f"got {status}")
        check("OTLP traces ack spans=1",
              body.get("accepted", {}).get("spans") == 1, f"got {body}")

        # 1d. OTLP logs (JSON encoding)
        logs_json = json.dumps({
            "resourceLogs": [{
                "resource": {
                    "attributes": [{"key": "service.name", "value": {"stringValue": "payment-svc"}}]
                },
                "scopeLogs": [{
                    "logRecords": [{
                        "timeUnixNano": str(now_ns + 10),
                        "severityNumber": 17,
                        "severityText": "ERROR",
                        "body": {"stringValue": "OTLP log: connection refused"},
                        "attributes": []
                    }]
                }]
            }]
        })
        status, body = http("POST", f"{base}/v1/logs", logs_json,
                            {"Content-Type": "application/json"})
        check("OTLP logs JSON returns 204", status == 204, f"got {status}")

        # 1e. Gzip-compressed OTLP metrics
        gzip_metrics = json.dumps({
            "resourceMetrics": [{
                "resource": {
                    "attributes": [{"key": "service.name", "value": {"stringValue": "gzip-svc"}}]
                },
                "scopeMetrics": [{
                    "metrics": [{
                        "name": "gzip_gauge",
                        "gauge": {
                            "dataPoints": [{
                                "timeUnixNano": str(now_ns),
                                "asDouble": 99.0
                            }]
                        }
                    }]
                }]
            }]
        }).encode("utf-8")
        compressed = gzip.compress(gzip_metrics)
        status, body = http("POST", f"{base}/v1/metrics", compressed,
                            {"Content-Type": "application/json", "Content-Encoding": "gzip"})
        check("Gzip OTLP metrics returns 200", status == 200, f"got {status}")

        # 1f. JSON log lines for | json pipeline test
        json_logs = json.dumps({
            "streams": [{
                "stream": {"service": "json-svc"},
                "values": [
                    [str(now_ns + 100), '{"level":"error","msg":"disk full","code":500}'],
                    [str(now_ns + 101), '{"level":"info","msg":"healthy","code":200}'],
                ]
            }]
        })
        status, _ = http("POST", f"{base}/loki/api/v1/push", json_logs,
                         {"Content-Type": "application/json"})
        check("JSON log push returns 200", status == 200, f"got {status}")

        # ===================================================================
        # PHASE 2: QUERY
        # ===================================================================
        print("\n=== QUERY PHASE ===")

        # 2a. LogQL query
        q = urllib.parse.quote('{service="payment-svc"}')
        status, body = http("GET", f"{base}/loki/api/v1/query?query={q}")
        check("LogQL query returns 200", status == 200, f"got {status}")
        check("LogQL query status=success", body.get("status") == "success", f"got {body}")
        results = body.get("data", {}).get("result", [])
        check("LogQL query returns streams", len(results) >= 1, f"got {len(results)} streams")

        # 2b. LogQL query_range
        q = urllib.parse.quote('{service="payment-svc", level="error"}')
        status, body = http("GET",
                            f"{base}/loki/api/v1/query_range?query={q}&start={now_ns - 60_000_000_000}&end={now_ns + 60_000_000_000}&limit=100")
        check("LogQL query_range returns 200", status == 200, f"got {status}")
        entries = sum(len(s.get("values", [])) for s in body.get("data", {}).get("result", []))
        check("LogQL query_range has error entries", entries >= 2, f"got {entries}")

        # 2c. LogQL POST form-encoded
        form_data = urllib.parse.urlencode({
            "query": '{service="payment-svc", level="error"}',
            "start": str(now_ns - 60_000_000_000),
            "end": str(now_ns + 60_000_000_000),
            "limit": "100",
        }).encode("utf-8")
        status, body = http("POST", f"{base}/loki/api/v1/query_range", form_data,
                            {"Content-Type": "application/x-www-form-urlencoded"})
        check("LogQL POST query_range returns 200", status == 200, f"got {status}")
        post_entries = sum(len(s.get("values", [])) for s in body.get("data", {}).get("result", []))
        check("LogQL POST same result as GET", post_entries == entries,
              f"POST={post_entries}, GET={entries}")

        # 2d. LogQL | json pipeline
        q = urllib.parse.quote('{service="json-svc"} | json | level="error"')
        status, body = http("GET",
                            f"{base}/loki/api/v1/query_range?query={q}&start={now_ns - 60_000_000_000}&end={now_ns + 60_000_000_000}&limit=100")
        check("LogQL json pipeline returns 200", status == 200, f"got {status}")
        json_entries = sum(len(s.get("values", [])) for s in body.get("data", {}).get("result", []))
        check("LogQL json pipeline filters to error only", json_entries == 1,
              f"got {json_entries}")

        # 2e. PromQL query
        q = urllib.parse.quote('http_requests_total{service="payment-svc"}')
        status, body = http("GET", f"{base}/api/v1/query?query={q}")
        check("PromQL query returns 200", status == 200, f"got {status}")
        check("PromQL query status=success", body.get("status") == "success")
        prom_results = body.get("data", {}).get("result", [])
        check("PromQL returns 1 series", len(prom_results) == 1,
              f"got {len(prom_results)}")
        if prom_results:
            val = float(prom_results[0].get("value", [0, "0"])[1])
            check("PromQL value=142.0", abs(val - 142.0) < 0.01, f"got {val}")

        # 2f. PromQL POST form-encoded
        form_data = urllib.parse.urlencode({
            "query": 'http_requests_total{service="payment-svc"}',
        }).encode("utf-8")
        status, body = http("POST", f"{base}/api/v1/query", form_data,
                            {"Content-Type": "application/x-www-form-urlencoded"})
        check("PromQL POST returns 200", status == 200, f"got {status}")

        # 2g. PromQL absent() — nonexistent metric
        q = urllib.parse.quote("absent(totally_fake_metric)")
        status, body = http("GET", f"{base}/api/v1/query?query={q}")
        check("PromQL absent(missing) returns 200", status == 200, f"got {status}")
        absent_results = body.get("data", {}).get("result", [])
        check("PromQL absent(missing) returns 1 series", len(absent_results) == 1,
              f"got {len(absent_results)}")
        if absent_results:
            val = float(absent_results[0].get("value", [0, "0"])[1])
            check("PromQL absent(missing) value=1", abs(val - 1.0) < 0.01, f"got {val}")

        # 2h. PromQL absent() — existing metric
        q = urllib.parse.quote("absent(http_requests_total)")
        status, body = http("GET", f"{base}/api/v1/query?query={q}")
        check("PromQL absent(existing) returns 200", status == 200)
        absent_existing = body.get("data", {}).get("result", [])
        check("PromQL absent(existing) returns empty", len(absent_existing) == 0,
              f"got {len(absent_existing)}")

        # 2i. PromQL sort
        q = urllib.parse.quote("sort(http_requests_total)")
        status, body = http("GET", f"{base}/api/v1/query?query={q}")
        check("PromQL sort returns 200", status == 200, f"got {status}")

        # 2j. PromQL time()
        q = urllib.parse.quote("time()")
        status, body = http("GET", f"{base}/api/v1/query?query={q}")
        check("PromQL time() returns 200", status == 200, f"got {status}")
        result_type = body.get("data", {}).get("resultType", "")
        check("PromQL time() has result", result_type in ("scalar", "vector"),
              f"got resultType={result_type}")
        results = body.get("data", {}).get("result", [])
        if results:
            if result_type == "scalar":
                t = float(results[1])
            else:
                t = float(results[0].get("value", [0, "0"])[1])
            check("PromQL time() > 0", t > 0, f"got {t}")

        # 2k. PromQL vector(42)
        q = urllib.parse.quote("vector(42)")
        status, body = http("GET", f"{base}/api/v1/query?query={q}")
        check("PromQL vector(42) returns 200", status == 200)
        vec_results = body.get("data", {}).get("result", [])
        check("PromQL vector(42) returns 1 series", len(vec_results) == 1,
              f"got {len(vec_results)}")
        if vec_results:
            val = float(vec_results[0].get("value", [0, "0"])[1])
            check("PromQL vector(42) value=42", abs(val - 42.0) < 0.01, f"got {val}")

        # 2l. Grafana compat: 1+1
        q = urllib.parse.quote("1+1")
        status, body = http("GET", f"{base}/api/v1/query?query={q}")
        check("Grafana 1+1 returns 200", status == 200)
        check("Grafana 1+1 status=success", body.get("status") == "success")

        # 2m. TraceQL search
        q = urllib.parse.quote('{ resource.service.name = "payment-svc" }')
        status, body = http("GET", f"{base}/api/search?q={q}")
        check("TraceQL search returns 200", status == 200, f"got {status}")
        traces = body.get("traces", [])
        check("TraceQL search finds traces", len(traces) >= 1, f"got {len(traces)}")

        # 2n. TraceQL search with time range
        q = urllib.parse.quote('{ resource.service.name = "payment-svc" }')
        status, body = http("GET",
                            f"{base}/api/search?q={q}&start={now_s - 60}&end={now_s + 60}")
        check("TraceQL time range returns 200", status == 200)
        check("TraceQL time range finds traces", len(body.get("traces", [])) >= 1)

        # 2o. GET /api/traces/{traceID}
        status, body = http("GET", f"{base}/api/traces/{trace_id_hex}")
        check("Get trace by ID returns 200", status == 200, f"got {status}")
        batches = body.get("batches", [])
        check("Trace has batches", len(batches) >= 1, f"got {len(batches)}")
        if batches:
            spans = []
            for batch in batches:
                for ss in batch.get("scopeSpans", []):
                    spans.extend(ss.get("spans", []))
            check("Trace has span 'charge-card'",
                  any(s.get("name") == "charge-card" for s in spans),
                  f"spans: {[s.get('name') for s in spans]}")

        # ===================================================================
        # PHASE 3: API ENDPOINTS
        # ===================================================================
        print("\n=== API PHASE ===")

        # 3a. Services
        status, body = http("GET", f"{base}/api/v1/services")
        check("Services returns 200", status == 200)
        services = [s.get("name") for s in body.get("data", {}).get("services", [])]
        check("Services includes payment-svc", "payment-svc" in services,
              f"got {services}")

        # 3b. Status
        status, body = http("GET", f"{base}/api/v1/status")
        check("Status returns 200", status == 200)
        check("Status has memoryBytes", "memoryBytes" in (body.get("data", {}) or {}),
              f"keys: {list((body.get('data', {}) or {}).keys())}")

        # 3c. Ready
        status, body = http("GET", f"{base}/ready")
        check("Ready returns 200", status == 200)

        # 3d. Labels
        status, body = http("GET", f"{base}/loki/api/v1/labels")
        check("Loki labels returns 200", status == 200)
        check("Loki labels status=success", body.get("status") == "success")

        # 3e. OpenAPI
        status, body = http("GET", f"{base}/api/v1/openapi.json")
        check("OpenAPI returns 200", status == 200)
        check("OpenAPI has version 3.x",
              str(body.get("openapi", "")).startswith("3."))
        paths = body.get("paths", {})
        check("OpenAPI has >10 paths", len(paths) > 10, f"got {len(paths)}")

        # 3f. Metadata (Grafana compat)
        status, body = http("GET", f"{base}/api/v1/metadata")
        check("Metadata returns 200", status == 200)
        check("Metadata is empty object", body == {} or body == "", f"got {body}")

        # 3g. Grafana empty search
        status, body = http("GET", f"{base}/api/search")
        check("Empty search returns 200", status == 200)
        check("Empty search has traces array", "traces" in body, f"got {body}")

        # 3h. Diagnose (per-service)
        status, body = http("GET", f"{base}/api/v1/diagnose?service=payment-svc")
        check("Diagnose returns 200", status == 200, f"got {status}")
        check("Diagnose has service field", body.get("service") == "payment-svc",
              f"got {body}")
        check("Diagnose has health_score",
              isinstance(body.get("health_score"), (int, float)),
              f"got {body.get('health_score')}")

        # 3h2. Diagnose (global — no service param)
        status, body = http("GET", f"{base}/api/v1/diagnose")
        check("Global diagnose returns 200", status == 200, f"got {status}")
        check("Global diagnose has services array",
              isinstance(body.get("services"), list),
              f"got {body}")

        # 3i. Catalog
        status, body = http("GET", f"{base}/api/v1/catalog?service=payment-svc")
        check("Catalog returns 200", status == 200, f"got {status}")
        catalog_data = body.get("data", body)
        check("Catalog has metrics", len(catalog_data.get("metrics", [])) >= 1,
              f"got {body}")

        # 3j. Diagnose
        status, body = http("GET", f"{base}/api/v1/diagnose?service=payment-svc")
        check("Diagnose returns 200", status == 200, f"got {status}")
        check("Diagnose has health_score",
              isinstance(body.get("health_score"), (int, float)),
              f"got {body.get('health_score')}")
        check("Diagnose health_score 0-100",
              0 <= (body.get("health_score", -1)) <= 100,
              f"got {body.get('health_score')}")
        check("Diagnose has slowest_traces",
              isinstance(body.get("slowest_traces"), list))
        check("Diagnose has error_trend",
              isinstance(body.get("error_trend"), (dict, list)),
              f"got type {type(body.get('error_trend'))}")
        check("Diagnose has recent_errors",
              isinstance(body.get("recent_errors"), list))
        check("Diagnose has suggested_queries",
              isinstance(body.get("suggested_queries"), list))

        # 3k. Error hints
        status, body = http("GET", f"{base}/loki/api/v1/query?query=invalid%7B%7B")
        check("Invalid LogQL returns 400", status == 400, f"got {status}")
        check("LogQL error has hint",
              isinstance(body, dict) and "hint" in body and len(body.get("hint", "")) > 0,
              f"got {body}")

        status, body = http("GET", f"{base}/api/v1/query?query=sum(")
        check("Invalid PromQL returns 400", status == 400, f"got {status}")
        check("PromQL error has hint",
              isinstance(body, dict) and "hint" in body and len(body.get("hint", "")) > 0,
              f"got {body}")

        # ===================================================================
        # PHASE 4: RESET
        # ===================================================================
        print("\n=== RESET PHASE ===")

        # 4a. Per-service reset
        status, body = http("DELETE", f"{base}/api/v1/reset?service=gzip-svc")
        check("Per-service reset returns 200", status == 200, f"got {status}")

        # Verify gzip-svc is gone
        status, body = http("GET", f"{base}/api/v1/services")
        services = [s.get("name") for s in body.get("data", {}).get("services", [])]
        check("gzip-svc removed after reset", "gzip-svc" not in services,
              f"still in {services}")
        check("payment-svc still present", "payment-svc" in services,
              f"not in {services}")

        # 4b. Full reset
        status, body = http("DELETE", f"{base}/api/v1/reset")
        check("Full reset returns 200", status == 200, f"got {status}")

        status, body = http("GET", f"{base}/api/v1/services")
        services = body.get("data", {}).get("services", [])
        check("All services cleared after reset", len(services) == 0,
              f"got {services}")

        # ===================================================================
        # RESULTS
        # ===================================================================
        print(f"\n{'=' * 60}")
        print(f"RESULTS: {PASS} passed, {FAIL} failed, {TOTAL} total")
        print(f"{'=' * 60}")

    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        print(f"\nObsidian shut down (PID {proc.pid})")

    sys.exit(1 if FAIL > 0 else 0)


if __name__ == "__main__":
    main()
