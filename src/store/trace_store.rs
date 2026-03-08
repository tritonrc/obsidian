//! Trace storage engine for TraceQL queries.
//!
//! `TraceStore` stores spans indexed by trace ID, service name, and span name.

use lasso::{Rodeo, Spur};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

/// Status of a span.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpanStatus {
    Unset,
    Ok,
    Error,
}

/// Attribute value types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AttributeValue {
    String(Spur),
    Int(i64),
    Float(f64),
    Bool(bool),
}

/// A trace span.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Span {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: Spur,
    pub service_name: Spur,
    pub start_time_ns: i64,
    pub duration_ns: i64,
    pub status: SpanStatus,
    pub attributes: SmallVec<[(Spur, AttributeValue); 8]>,
}

/// Result of a trace search.
#[derive(Debug, Clone)]
pub struct TraceResult {
    pub trace_id: [u8; 16],
    pub root_service_name: String,
    pub root_span_name: String,
    pub start_time_ns: i64,
    pub duration_ns: i64,
    pub span_count: usize,
}

/// In-memory trace storage with indexes.
#[derive(Debug, Serialize, Deserialize)]
pub struct TraceStore {
    /// trace_id -> list of spans.
    pub traces: FxHashMap<[u8; 16], Vec<Span>>,
    /// Service name (Spur) -> set of trace IDs.
    pub service_index: FxHashMap<Spur, FxHashSet<[u8; 16]>>,
    /// Span name (Spur) -> set of trace IDs.
    pub name_index: FxHashMap<Spur, FxHashSet<[u8; 16]>>,
    /// String interner.
    pub interner: Rodeo,
    /// Total span count for eviction.
    pub total_spans: usize,
}

impl TraceStore {
    /// Create a new empty trace store.
    pub fn new() -> Self {
        Self {
            traces: FxHashMap::default(),
            service_index: FxHashMap::default(),
            name_index: FxHashMap::default(),
            interner: Rodeo::default(),
            total_spans: 0,
        }
    }

    /// Ingest a batch of spans.
    pub fn ingest_spans(&mut self, spans: Vec<Span>) {
        for span in spans {
            let trace_id = span.trace_id;
            let service = span.service_name;
            let name = span.name;

            self.service_index
                .entry(service)
                .or_default()
                .insert(trace_id);
            self.name_index.entry(name).or_default().insert(trace_id);

            self.traces.entry(trace_id).or_default().push(span);
            self.total_spans += 1;
        }
    }

    /// Get all spans for a trace.
    pub fn get_trace(&self, trace_id: &[u8; 16]) -> Option<&Vec<Span>> {
        self.traces.get(trace_id)
    }

    /// Get all known service names.
    pub fn service_names(&self) -> Vec<String> {
        self.service_index
            .keys()
            .map(|s| self.interner.resolve(s).to_string())
            .collect()
    }

    /// Get all trace IDs for a service.
    pub fn traces_for_service(&self, service: &str) -> Vec<[u8; 16]> {
        match self.interner.get(service) {
            Some(spur) => match self.service_index.get(&spur) {
                Some(set) => set.iter().copied().collect(),
                None => Vec::new(),
            },
            None => Vec::new(),
        }
    }

    /// Resolve a Spur to a string.
    pub fn resolve(&self, spur: &Spur) -> &str {
        self.interner.resolve(spur)
    }

    /// Resolve an attribute value to a string representation.
    pub fn resolve_attribute_value(&self, val: &AttributeValue) -> String {
        match val {
            AttributeValue::String(s) => self.interner.resolve(s).to_string(),
            AttributeValue::Int(i) => i.to_string(),
            AttributeValue::Float(f) => f.to_string(),
            AttributeValue::Bool(b) => b.to_string(),
        }
    }

    /// Evict oldest traces until total_spans <= max.
    pub fn evict_to_max(&mut self, max_spans: usize) {
        if self.total_spans <= max_spans {
            return;
        }

        // Sort traces by earliest span start time
        let mut trace_ids: Vec<[u8; 16]> = self.traces.keys().copied().collect();
        trace_ids.sort_by_key(|tid| {
            self.traces[tid]
                .iter()
                .map(|s| s.start_time_ns)
                .min()
                .unwrap_or(i64::MAX)
        });

        for tid in trace_ids {
            if self.total_spans <= max_spans {
                break;
            }
            if let Some(spans) = self.traces.remove(&tid) {
                self.total_spans = self.total_spans.saturating_sub(spans.len());
                // Clean up indexes
                for span in &spans {
                    if let Some(set) = self.service_index.get_mut(&span.service_name) {
                        set.remove(&tid);
                    }
                    if let Some(set) = self.name_index.get_mut(&span.name) {
                        set.remove(&tid);
                    }
                }
            }
        }

        // Clean up empty index entries
        self.service_index.retain(|_, v| !v.is_empty());
        self.name_index.retain(|_, v| !v.is_empty());
    }

    /// Evict spans older than the given timestamp.
    pub fn evict_before(&mut self, cutoff_ns: i64) {
        let mut empty_traces: Vec<([u8; 16], FxHashSet<Spur>, FxHashSet<Spur>)> = Vec::new();
        for (trace_id, spans) in &mut self.traces {
            // Collect service/name spurs from spans being evicted before retain
            let evicted_services: FxHashSet<Spur> = spans
                .iter()
                .filter(|s| s.start_time_ns < cutoff_ns)
                .map(|s| s.service_name)
                .collect();
            let evicted_names: FxHashSet<Spur> = spans
                .iter()
                .filter(|s| s.start_time_ns < cutoff_ns)
                .map(|s| s.name)
                .collect();

            let before = spans.len();
            spans.retain(|s| s.start_time_ns >= cutoff_ns);
            let removed = before - spans.len();
            self.total_spans = self.total_spans.saturating_sub(removed);
            if spans.is_empty() {
                empty_traces.push((*trace_id, evicted_services, evicted_names));
            }
        }

        for (trace_id, services, names) in &empty_traces {
            self.traces.remove(trace_id);
            for svc in services {
                if let Some(set) = self.service_index.get_mut(svc) {
                    set.remove(trace_id);
                }
            }
            for name in names {
                if let Some(set) = self.name_index.get_mut(name) {
                    set.remove(trace_id);
                }
            }
        }

        // Clean up empty index entries
        self.service_index.retain(|_, v| !v.is_empty());
        self.name_index.retain(|_, v| !v.is_empty());
    }

    /// Build a TraceResult summary for a trace.
    pub fn trace_result(&self, trace_id: &[u8; 16]) -> Option<TraceResult> {
        let spans = self.traces.get(trace_id)?;
        if spans.is_empty() {
            return None;
        }

        // Find root span (no parent) or earliest span
        let root = spans
            .iter()
            .find(|s| s.parent_span_id.is_none())
            .or_else(|| spans.iter().min_by_key(|s| s.start_time_ns))?;

        let start = spans.iter().map(|s| s.start_time_ns).min().unwrap_or(0);
        let end = spans
            .iter()
            .map(|s| s.start_time_ns + s.duration_ns)
            .max()
            .unwrap_or(0);

        Some(TraceResult {
            trace_id: *trace_id,
            root_service_name: self.interner.resolve(&root.service_name).to_string(),
            root_span_name: self.interner.resolve(&root.name).to_string(),
            start_time_ns: start,
            duration_ns: end - start,
            span_count: spans.len(),
        })
    }
}

impl Default for TraceStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_span(
        interner: &mut Rodeo,
        trace_id: [u8; 16],
        span_id: [u8; 8],
        parent: Option<[u8; 8]>,
        name: &str,
        service: &str,
        start: i64,
        duration: i64,
        status: SpanStatus,
    ) -> Span {
        Span {
            trace_id,
            span_id,
            parent_span_id: parent,
            name: interner.get_or_intern(name),
            service_name: interner.get_or_intern(service),
            start_time_ns: start,
            duration_ns: duration,
            status,
            attributes: SmallVec::new(),
        }
    }

    #[test]
    fn test_ingest_and_get_trace() {
        let mut store = TraceStore::new();
        let tid = [1u8; 16];
        let span = make_span(
            &mut store.interner,
            tid,
            [1u8; 8],
            None,
            "GET /api",
            "gateway",
            1000,
            500,
            SpanStatus::Ok,
        );
        store.ingest_spans(vec![span]);

        let trace = store.get_trace(&tid).unwrap();
        assert_eq!(trace.len(), 1);
        assert_eq!(store.resolve(&trace[0].name), "GET /api");
    }

    #[test]
    fn test_service_index() {
        let mut store = TraceStore::new();
        let tid1 = [1u8; 16];
        let tid2 = [2u8; 16];
        let span1 = make_span(
            &mut store.interner,
            tid1,
            [1u8; 8],
            None,
            "op1",
            "payments",
            1000,
            100,
            SpanStatus::Ok,
        );
        let span2 = make_span(
            &mut store.interner,
            tid2,
            [2u8; 8],
            None,
            "op2",
            "gateway",
            2000,
            200,
            SpanStatus::Ok,
        );
        store.ingest_spans(vec![span1, span2]);

        let traces = store.traces_for_service("payments");
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0], tid1);
    }

    #[test]
    fn test_service_names() {
        let mut store = TraceStore::new();
        let span = make_span(
            &mut store.interner,
            [1u8; 16],
            [1u8; 8],
            None,
            "op",
            "myservice",
            1000,
            100,
            SpanStatus::Ok,
        );
        store.ingest_spans(vec![span]);

        let names = store.service_names();
        assert_eq!(names, vec!["myservice".to_string()]);
    }

    #[test]
    fn test_eviction() {
        let mut store = TraceStore::new();
        let span1 = make_span(
            &mut store.interner,
            [1u8; 16],
            [1u8; 8],
            None,
            "old",
            "svc",
            1000,
            100,
            SpanStatus::Ok,
        );
        let span2 = make_span(
            &mut store.interner,
            [2u8; 16],
            [2u8; 8],
            None,
            "new",
            "svc",
            5000,
            100,
            SpanStatus::Ok,
        );
        store.ingest_spans(vec![span1, span2]);
        assert_eq!(store.total_spans, 2);

        store.evict_before(3000);
        assert_eq!(store.total_spans, 1);
        assert!(store.get_trace(&[1u8; 16]).is_none());
        assert!(store.get_trace(&[2u8; 16]).is_some());
    }

    #[test]
    fn test_evict_to_max() {
        let mut store = TraceStore::new();
        let span1 = make_span(
            &mut store.interner,
            [1u8; 16],
            [1u8; 8],
            None,
            "old",
            "svc",
            1000,
            100,
            SpanStatus::Ok,
        );
        let span2 = make_span(
            &mut store.interner,
            [2u8; 16],
            [2u8; 8],
            None,
            "new",
            "svc",
            5000,
            100,
            SpanStatus::Ok,
        );
        let span3 = make_span(
            &mut store.interner,
            [2u8; 16],
            [3u8; 8],
            Some([2u8; 8]),
            "child",
            "svc",
            5100,
            50,
            SpanStatus::Ok,
        );
        store.ingest_spans(vec![span1, span2, span3]);
        assert_eq!(store.total_spans, 3);
        // Evict to max 2 should remove the oldest trace (1 span)
        store.evict_to_max(2);
        assert_eq!(store.total_spans, 2);
        assert!(store.get_trace(&[1u8; 16]).is_none());
        assert!(store.get_trace(&[2u8; 16]).is_some());
    }

    #[test]
    fn test_trace_result() {
        let mut store = TraceStore::new();
        let tid = [1u8; 16];
        let root = make_span(
            &mut store.interner,
            tid,
            [1u8; 8],
            None,
            "root",
            "svc",
            1000,
            500,
            SpanStatus::Ok,
        );
        let child = make_span(
            &mut store.interner,
            tid,
            [2u8; 8],
            Some([1u8; 8]),
            "child",
            "svc",
            1100,
            200,
            SpanStatus::Ok,
        );
        store.ingest_spans(vec![root, child]);

        let result = store.trace_result(&tid).unwrap();
        assert_eq!(result.root_span_name, "root");
        assert_eq!(result.span_count, 2);
        assert_eq!(result.start_time_ns, 1000);
        assert_eq!(result.duration_ns, 500); // 1000+500=1500 - 1000
    }
}
