//! Trace storage engine for TraceQL queries.
//!
//! `TraceStore` stores spans indexed by trace ID, service name, and span name.

use lasso::{Rodeo, Spur};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

/// Status of a span.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    /// Span status -> set of trace IDs.
    pub status_index: FxHashMap<SpanStatus, FxHashSet<[u8; 16]>>,
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
            status_index: FxHashMap::default(),
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
            self.status_index
                .entry(span.status)
                .or_default()
                .insert(trace_id);

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
                    if let Some(set) = self.status_index.get_mut(&span.status) {
                        set.remove(&tid);
                    }
                }
            }
        }

        // Clean up empty index entries
        self.service_index.retain(|_, v| !v.is_empty());
        self.name_index.retain(|_, v| !v.is_empty());
        self.status_index.retain(|_, v| !v.is_empty());
    }

    /// Evict spans older than the given timestamp.
    pub fn evict_before(&mut self, cutoff_ns: i64) {
        type EvictedEntry = (
            [u8; 16],
            FxHashSet<Spur>,
            FxHashSet<Spur>,
            FxHashSet<SpanStatus>,
        );
        let mut empty_traces: Vec<EvictedEntry> = Vec::new();
        for (trace_id, spans) in &mut self.traces {
            // Collect service/name spurs and statuses from spans being evicted before retain
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
            let evicted_statuses: FxHashSet<SpanStatus> = spans
                .iter()
                .filter(|s| s.start_time_ns < cutoff_ns)
                .map(|s| s.status)
                .collect();

            let before = spans.len();
            spans.retain(|s| s.start_time_ns >= cutoff_ns);
            let removed = before - spans.len();
            self.total_spans = self.total_spans.saturating_sub(removed);
            if spans.is_empty() {
                empty_traces.push((*trace_id, evicted_services, evicted_names, evicted_statuses));
            }
        }

        for (trace_id, services, names, statuses) in &empty_traces {
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
            for status in statuses {
                if let Some(set) = self.status_index.get_mut(status) {
                    set.remove(trace_id);
                }
            }
        }

        // Clean up empty index entries
        self.service_index.retain(|_, v| !v.is_empty());
        self.name_index.retain(|_, v| !v.is_empty());
        self.status_index.retain(|_, v| !v.is_empty());
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

    /// Return summaries of the most recent traces, up to `limit`.
    ///
    /// Traces are sorted by start time descending (most recent first).
    pub fn recent_traces(&self, limit: usize) -> Vec<TraceResult> {
        let mut results: Vec<TraceResult> = self
            .traces
            .keys()
            .filter_map(|tid| self.trace_result(tid))
            .collect();
        results.sort_by(|a, b| b.start_time_ns.cmp(&a.start_time_ns));
        results.truncate(limit);
        results
    }

    /// Clear all data from the store.
    pub fn clear(&mut self) {
        self.traces.clear();
        self.service_index.clear();
        self.name_index.clear();
        self.status_index.clear();
        self.interner = Rodeo::default();
        self.total_spans = 0;
    }

    /// Clear all data belonging to a specific service.
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
            if let Some(spans) = self.traces.remove(trace_id) {
                self.total_spans = self.total_spans.saturating_sub(spans.len());
                // Clean up indexes
                for span in &spans {
                    if let Some(set) = self.service_index.get_mut(&span.service_name) {
                        set.remove(trace_id);
                    }
                    if let Some(set) = self.name_index.get_mut(&span.name) {
                        set.remove(trace_id);
                    }
                    if let Some(set) = self.status_index.get_mut(&span.status) {
                        set.remove(trace_id);
                    }
                }
            }
        }

        // Clean up empty index entries
        self.service_index.retain(|_, v| !v.is_empty());
        self.name_index.retain(|_, v| !v.is_empty());
        self.status_index.retain(|_, v| !v.is_empty());
    }
}

impl TraceStore {
    /// Estimate the memory usage of this store in bytes.
    ///
    /// Accounts for span data, attribute sizes, index overhead, and interner memory.
    pub fn memory_estimate_bytes(&self) -> usize {
        let mut bytes = 0usize;

        // Per-trace: Vec<Span> and each span's attributes
        for spans in self.traces.values() {
            bytes += spans.capacity() * std::mem::size_of::<Span>();
            for span in spans {
                // Attribute key/value pairs stored in SmallVec
                bytes += span.attributes.len() * std::mem::size_of::<(Spur, AttributeValue)>();
            }
        }

        // HashMap overhead for traces
        bytes += self.traces.len()
            * (std::mem::size_of::<[u8; 16]>() + std::mem::size_of::<Vec<Span>>() + 8);

        // Service index: FxHashMap<Spur, FxHashSet<[u8; 16]>>
        for set in self.service_index.values() {
            bytes += set.len() * (std::mem::size_of::<[u8; 16]>() + 8);
        }
        bytes += self.service_index.len() * (std::mem::size_of::<Spur>() + 8);

        // Name index: FxHashMap<Spur, FxHashSet<[u8; 16]>>
        for set in self.name_index.values() {
            bytes += set.len() * (std::mem::size_of::<[u8; 16]>() + 8);
        }
        bytes += self.name_index.len() * (std::mem::size_of::<Spur>() + 8);

        // Status index: FxHashMap<SpanStatus, FxHashSet<[u8; 16]>>
        for set in self.status_index.values() {
            bytes += set.len() * (std::mem::size_of::<[u8; 16]>() + 8);
        }
        bytes += self.status_index.len() * (std::mem::size_of::<SpanStatus>() + 8);

        // Interner memory
        bytes += self.interner.current_memory_usage();

        bytes
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

    #[test]
    fn test_recent_traces_returns_most_recent_first() {
        let mut store = TraceStore::new();

        let old = make_span(
            &mut store.interner,
            [1u8; 16],
            [1u8; 8],
            None,
            "old-span",
            "svc",
            1000,
            100,
            SpanStatus::Ok,
        );
        let mid = make_span(
            &mut store.interner,
            [2u8; 16],
            [2u8; 8],
            None,
            "mid-span",
            "svc",
            5000,
            100,
            SpanStatus::Ok,
        );
        let new = make_span(
            &mut store.interner,
            [3u8; 16],
            [3u8; 8],
            None,
            "new-span",
            "svc",
            9000,
            100,
            SpanStatus::Ok,
        );
        store.ingest_spans(vec![old, mid, new]);

        let recent = store.recent_traces(10);
        assert_eq!(recent.len(), 3);
        // Most recent first
        assert_eq!(recent[0].root_span_name, "new-span");
        assert_eq!(recent[1].root_span_name, "mid-span");
        assert_eq!(recent[2].root_span_name, "old-span");
    }

    #[test]
    fn test_recent_traces_respects_limit() {
        let mut store = TraceStore::new();

        for i in 0..5u8 {
            let span = make_span(
                &mut store.interner,
                [i; 16],
                [i; 8],
                None,
                &format!("span-{}", i),
                "svc",
                i as i64 * 1000,
                100,
                SpanStatus::Ok,
            );
            store.ingest_spans(vec![span]);
        }

        let recent = store.recent_traces(2);
        assert_eq!(recent.len(), 2);
        // Most recent two
        assert_eq!(recent[0].start_time_ns, 4000);
        assert_eq!(recent[1].start_time_ns, 3000);
    }

    #[test]
    fn test_recent_traces_empty_store() {
        let store = TraceStore::new();
        let recent = store.recent_traces(10);
        assert!(recent.is_empty());
    }
}
