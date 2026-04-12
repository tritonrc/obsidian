//! Log storage engine with inverted index for LogQL queries.
//!
//! `LogStore` stores log streams indexed by label pairs using sorted posting lists.
//! Each stream contains an ordered sequence of `LogEntry` items.

use lasso::{Rodeo, Spur};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use super::posting_list::PostingList;
use super::{LabelMatcher, LabelPairs};

/// A single log entry with nanosecond timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp_ns: i64,
    pub line: String,
}

/// A log stream identified by a set of labels.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogStream {
    pub labels: SmallVec<[(Spur, Spur); 8]>,
    pub entries: Vec<LogEntry>,
}

/// In-memory log storage with inverted index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogStore {
    /// Stream ID -> stream data.
    pub streams: FxHashMap<u64, LogStream>,
    /// Label pair (name, value) -> set of stream IDs.
    pub label_index: FxHashMap<(Spur, Spur), PostingList>,
    /// Label name -> set of known values.
    pub label_values: FxHashMap<Spur, FxHashSet<Spur>>,
    /// String interner.
    pub interner: Rodeo,
    /// Total entry count for eviction.
    pub total_entries: usize,
    /// Exact label-set to stream ID mapping to avoid hash-collision merges.
    #[serde(default)]
    pub stream_ids: FxHashMap<LabelPairs, u64>,
    /// Next stream ID for newly observed label sets.
    #[serde(default)]
    pub next_stream_id: u64,
}

impl LogStore {
    /// Create a new empty log store.
    pub fn new() -> Self {
        Self {
            streams: FxHashMap::default(),
            label_index: FxHashMap::default(),
            label_values: FxHashMap::default(),
            interner: Rodeo::default(),
            total_entries: 0,
            stream_ids: FxHashMap::default(),
            next_stream_id: 0,
        }
    }

    /// Rebuild the label-set to stream-ID map after loading older snapshots.
    pub fn rebuild_stream_ids(&mut self) {
        self.stream_ids.clear();
        self.next_stream_id = 0;

        let mut stream_ids: Vec<u64> = self.streams.keys().copied().collect();
        stream_ids.sort_unstable();
        for stream_id in stream_ids {
            if let Some(stream) = self.streams.get(&stream_id) {
                self.stream_ids.insert(stream.labels.clone(), stream_id);
                self.next_stream_id = self.next_stream_id.max(stream_id.saturating_add(1));
            }
        }
    }

    /// Ingest a stream with the given labels and entries.
    pub fn ingest_stream(&mut self, labels: Vec<(String, String)>, entries: Vec<LogEntry>) {
        let interned_labels = super::intern_label_pairs(&mut self.interner, &labels);
        super::track_label_values(&mut self.label_values, &interned_labels);

        let stream_id = match self.stream_ids.get(&interned_labels).copied() {
            Some(stream_id) => stream_id,
            None => {
                let stream_id = self.next_stream_id;
                self.next_stream_id = self.next_stream_id.saturating_add(1);
                self.stream_ids.insert(interned_labels.clone(), stream_id);
                self.streams.insert(
                    stream_id,
                    LogStream {
                        labels: interned_labels.clone(),
                        entries: Vec::new(),
                    },
                );
                for &(k, v) in &interned_labels {
                    self.label_index
                        .entry((k, v))
                        .or_default()
                        .insert(stream_id);
                }
                stream_id
            }
        };

        let entry_count = entries.len();
        let stream = self.streams.get_mut(&stream_id).expect("stream must exist");

        let was_empty = stream.entries.is_empty();
        for entry in entries {
            stream.entries.push(entry);
        }
        self.total_entries += entry_count;

        // Maintain sorted order for partition_point correctness.
        // Always check the full vec after appending — an internally unsorted batch
        // where all timestamps are >= prev_last_ts would otherwise be missed.
        if entry_count > 1 || !was_empty {
            let needs_sort = !stream
                .entries
                .windows(2)
                .all(|w| w[0].timestamp_ns <= w[1].timestamp_ns);
            if needs_sort {
                stream.entries.sort_by_key(|e| e.timestamp_ns);
            }
        }
    }

    /// Query streams matching all the given label matchers.
    pub fn query_streams(&self, matchers: &[LabelMatcher]) -> Vec<u64> {
        super::select_indexed_ids(
            &self.interner,
            &self.label_index,
            &self.label_values,
            || self.streams.keys().copied().collect(),
            matchers,
        )
    }

    /// Get entries for a stream within a time range.
    pub fn get_entries(&self, stream_id: u64, start_ns: i64, end_ns: i64) -> &[LogEntry] {
        match self.streams.get(&stream_id) {
            Some(stream) => {
                let lo = stream
                    .entries
                    .partition_point(|e| e.timestamp_ns < start_ns);
                let hi = stream.entries.partition_point(|e| e.timestamp_ns <= end_ns);
                &stream.entries[lo..hi]
            }
            None => &[],
        }
    }

    /// Get labels for a stream as resolved strings.
    pub fn get_stream_labels(&self, stream_id: u64) -> Option<Vec<(String, String)>> {
        self.streams.get(&stream_id).map(|stream| {
            stream
                .labels
                .iter()
                .map(|(k, v)| {
                    (
                        self.interner.resolve(k).to_string(),
                        self.interner.resolve(v).to_string(),
                    )
                })
                .collect()
        })
    }

    /// Get all label names.
    pub fn label_names(&self) -> Vec<String> {
        self.label_values
            .keys()
            .map(|k| self.interner.resolve(k).to_string())
            .collect()
    }

    /// Get all values for a given label name.
    pub fn get_label_values(&self, name: &str) -> Vec<String> {
        let spur = match self.interner.get(name) {
            Some(s) => s,
            None => return Vec::new(),
        };
        match self.label_values.get(&spur) {
            Some(values) => values
                .iter()
                .map(|v| self.interner.resolve(v).to_string())
                .collect(),
            None => Vec::new(),
        }
    }

    /// Evict entries older than the given timestamp.
    pub fn evict_before(&mut self, cutoff_ns: i64) {
        let mut empty_streams = Vec::new();
        for (&stream_id, stream) in &mut self.streams {
            let before = stream.entries.len();
            let drain_count = stream
                .entries
                .partition_point(|e| e.timestamp_ns < cutoff_ns);
            if drain_count > 0 {
                stream.entries.drain(..drain_count);
            }
            let removed = before - stream.entries.len();
            self.total_entries = self.total_entries.saturating_sub(removed);
            if stream.entries.is_empty() {
                empty_streams.push(stream_id);
            }
        }

        for stream_id in empty_streams {
            if let Some(stream) = self.streams.remove(&stream_id) {
                self.stream_ids.remove(&stream.labels);
                super::remove_from_label_indexes(
                    &mut self.label_index,
                    &mut self.label_values,
                    &stream.labels,
                    stream_id,
                );
            }
        }
    }

    /// Evict oldest entries until total_entries <= max.
    pub fn evict_to_max(&mut self, max_entries: usize) {
        if self.total_entries <= max_entries {
            return;
        }
        let to_remove = self.total_entries - max_entries;

        let mut stream_ids: Vec<u64> = self.streams.keys().copied().collect();
        stream_ids.sort_by_key(|id| {
            self.streams[id]
                .entries
                .first()
                .map(|e| e.timestamp_ns)
                .unwrap_or(i64::MAX)
        });

        let mut remaining = to_remove;
        for sid in stream_ids {
            if remaining == 0 {
                break;
            }
            if let Some(stream) = self.streams.get_mut(&sid) {
                let drain = remaining.min(stream.entries.len());
                stream.entries.drain(..drain);
                remaining -= drain;
                self.total_entries -= drain;
                if stream.entries.is_empty()
                    && let Some(stream) = self.streams.remove(&sid)
                {
                    self.stream_ids.remove(&stream.labels);
                    super::remove_from_label_indexes(
                        &mut self.label_index,
                        &mut self.label_values,
                        &stream.labels,
                        sid,
                    );
                }
            }
        }
    }

    /// Clear all data from the store.
    pub fn clear(&mut self) {
        self.streams.clear();
        self.label_index.clear();
        self.label_values.clear();
        self.interner = Rodeo::default();
        self.stream_ids.clear();
        self.next_stream_id = 0;
        self.total_entries = 0;
    }

    /// Clear all data belonging to a specific service.
    pub fn clear_service(&mut self, service: &str) {
        let service_spur = match self.interner.get(service) {
            Some(s) => s,
            None => return,
        };
        let service_key = match self.interner.get("service") {
            Some(s) => s,
            None => return,
        };

        // Find streams that have service=<value>
        let stream_ids: Vec<u64> = match self.label_index.get(&(service_key, service_spur)) {
            Some(pl) => pl.ids().to_vec(),
            None => return,
        };

        for stream_id in stream_ids {
            if let Some(stream) = self.streams.remove(&stream_id) {
                self.total_entries = self.total_entries.saturating_sub(stream.entries.len());
                self.stream_ids.remove(&stream.labels);
                super::remove_from_label_indexes(
                    &mut self.label_index,
                    &mut self.label_values,
                    &stream.labels,
                    stream_id,
                );
            }
        }
    }
}

impl LogStore {
    /// Estimate the memory usage of this store in bytes.
    ///
    /// Sums actual string lengths in log entries, Vec overhead, label index sizes,
    /// and the interner's memory footprint.
    pub fn memory_estimate_bytes(&self) -> usize {
        let mut bytes = 0usize;

        // Per-stream overhead: SmallVec labels + Vec<LogEntry> bookkeeping
        for stream in self.streams.values() {
            // Labels: SmallVec overhead is inline for <=8, each pair is (Spur, Spur) = 8 bytes
            bytes += stream.labels.len() * std::mem::size_of::<(Spur, Spur)>();
            // Vec<LogEntry> capacity overhead
            bytes += stream.entries.capacity() * std::mem::size_of::<LogEntry>();
            // Actual string data in each entry (heap allocation beyond LogEntry struct)
            for entry in &stream.entries {
                bytes += entry.line.capacity();
            }
        }

        // HashMap overhead for streams: ~(key_size + value_size + 8) * capacity
        // Approximate with entries * overhead_per_bucket
        bytes += self.streams.len()
            * (std::mem::size_of::<u64>() + std::mem::size_of::<LogStream>() + 8);

        // Label index: FxHashMap<(Spur, Spur), PostingList>
        for pl in self.label_index.values() {
            bytes += pl.len() * std::mem::size_of::<u64>();
        }
        bytes += self.label_index.len()
            * (std::mem::size_of::<(Spur, Spur)>() + std::mem::size_of::<PostingList>() + 8);

        // Label values index: FxHashMap<Spur, FxHashSet<Spur>>
        for vals in self.label_values.values() {
            bytes += vals.len() * (std::mem::size_of::<Spur>() + 8);
        }
        bytes += self.label_values.len() * (std::mem::size_of::<Spur>() + 8);

        // Interner memory
        bytes += self.interner.current_memory_usage();

        bytes
    }
}

impl Default for LogStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::LabelMatchOp;

    fn make_entry(ts: i64, line: &str) -> LogEntry {
        LogEntry {
            timestamp_ns: ts,
            line: line.to_string(),
        }
    }

    #[test]
    fn test_ingest_and_query_eq() {
        let mut store = LogStore::new();
        store.ingest_stream(
            vec![
                ("service".into(), "payments".into()),
                ("level".into(), "error".into()),
            ],
            vec![make_entry(1000, "timeout")],
        );
        store.ingest_stream(
            vec![
                ("service".into(), "gateway".into()),
                ("level".into(), "info".into()),
            ],
            vec![make_entry(2000, "ok")],
        );

        let results = store.query_streams(&[LabelMatcher {
            name: "service".into(),
            op: LabelMatchOp::Eq,
            value: "payments".into(),
        }]);
        assert_eq!(results.len(), 1);

        let entries = store.get_entries(results[0], 0, i64::MAX);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].line, "timeout");
    }

    #[test]
    fn test_query_neq() {
        let mut store = LogStore::new();
        store.ingest_stream(
            vec![("level".into(), "error".into())],
            vec![make_entry(1000, "err")],
        );
        store.ingest_stream(
            vec![("level".into(), "info".into())],
            vec![make_entry(2000, "ok")],
        );

        let results = store.query_streams(&[LabelMatcher {
            name: "level".into(),
            op: LabelMatchOp::Neq,
            value: "error".into(),
        }]);
        assert_eq!(results.len(), 1);
        let entries = store.get_entries(results[0], 0, i64::MAX);
        assert_eq!(entries[0].line, "ok");
    }

    #[test]
    fn test_query_regex() {
        let mut store = LogStore::new();
        store.ingest_stream(
            vec![("service".into(), "payments".into())],
            vec![make_entry(1000, "a")],
        );
        store.ingest_stream(
            vec![("service".into(), "gateway".into())],
            vec![make_entry(2000, "b")],
        );
        store.ingest_stream(
            vec![("service".into(), "worker".into())],
            vec![make_entry(3000, "c")],
        );

        let results = store.query_streams(&[LabelMatcher {
            name: "service".into(),
            op: LabelMatchOp::Regex,
            value: "pay.*|gate.*".into(),
        }]);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_query_not_regex() {
        let mut store = LogStore::new();
        store.ingest_stream(
            vec![("level".into(), "debug".into())],
            vec![make_entry(1000, "a")],
        );
        store.ingest_stream(
            vec![("level".into(), "error".into())],
            vec![make_entry(2000, "b")],
        );

        let results = store.query_streams(&[LabelMatcher {
            name: "level".into(),
            op: LabelMatchOp::NotRegex,
            value: "debug".into(),
        }]);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_eviction() {
        let mut store = LogStore::new();
        store.ingest_stream(
            vec![("service".into(), "a".into())],
            vec![make_entry(1000, "old"), make_entry(5000, "new")],
        );
        assert_eq!(store.total_entries, 2);
        store.evict_before(3000);
        assert_eq!(store.total_entries, 1);
    }

    #[test]
    fn test_evict_to_max() {
        let mut store = LogStore::new();
        store.ingest_stream(
            vec![("service".into(), "a".into())],
            vec![
                make_entry(1000, "1"),
                make_entry(2000, "2"),
                make_entry(3000, "3"),
            ],
        );
        assert_eq!(store.total_entries, 3);
        store.evict_to_max(1);
        assert_eq!(store.total_entries, 1);
    }

    #[test]
    fn test_label_names_and_values() {
        let mut store = LogStore::new();
        store.ingest_stream(
            vec![
                ("service".into(), "payments".into()),
                ("level".into(), "error".into()),
            ],
            vec![make_entry(1000, "x")],
        );
        let names = store.label_names();
        assert!(names.contains(&"service".to_string()));
        assert!(names.contains(&"level".to_string()));

        let values = store.get_label_values("service");
        assert_eq!(values, vec!["payments".to_string()]);
    }

    #[test]
    fn test_neq_matches_missing_label() {
        let mut store = LogStore::new();
        store.ingest_stream(
            vec![
                ("service".into(), "payments".into()),
                ("level".into(), "error".into()),
            ],
            vec![make_entry(1000, "err")],
        );
        store.ingest_stream(
            vec![("service".into(), "gateway".into())], // no "level" label
            vec![make_entry(2000, "ok")],
        );
        // {level!="error"} should match gateway (missing label)
        let results = store.query_streams(&[LabelMatcher {
            name: "level".into(),
            op: LabelMatchOp::Neq,
            value: "error".into(),
        }]);
        assert_eq!(results.len(), 1);
        let entries = store.get_entries(results[0], 0, i64::MAX);
        assert_eq!(entries[0].line, "ok");
    }

    #[test]
    fn test_not_regex_matches_missing_label() {
        let mut store = LogStore::new();
        store.ingest_stream(
            vec![
                ("service".into(), "payments".into()),
                ("level".into(), "debug".into()),
            ],
            vec![make_entry(1000, "dbg")],
        );
        store.ingest_stream(
            vec![("service".into(), "gateway".into())], // no "level" label
            vec![make_entry(2000, "ok")],
        );
        // {level!~"debug"} should match gateway (missing label)
        let results = store.query_streams(&[LabelMatcher {
            name: "level".into(),
            op: LabelMatchOp::NotRegex,
            value: "debug".into(),
        }]);
        assert_eq!(results.len(), 1);
        let entries = store.get_entries(results[0], 0, i64::MAX);
        assert_eq!(entries[0].line, "ok");
    }

    #[test]
    fn test_out_of_order_ingest() {
        let mut store = LogStore::new();
        store.ingest_stream(
            vec![("service".into(), "a".into())],
            vec![
                make_entry(3000, "third"),
                make_entry(1000, "first"),
                make_entry(2000, "second"),
            ],
        );
        let ids = store.query_streams(&[LabelMatcher {
            name: "service".into(),
            op: LabelMatchOp::Eq,
            value: "a".into(),
        }]);
        // Verify entries are sorted and range queries work correctly
        let entries = store.get_entries(ids[0], 0, i64::MAX);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].line, "first");
        assert_eq!(entries[1].line, "second");
        assert_eq!(entries[2].line, "third");

        // Verify partition_point works for range queries
        let entries = store.get_entries(ids[0], 1500, 2500);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].line, "second");
    }

    #[test]
    fn test_get_entries_time_range() {
        let mut store = LogStore::new();
        store.ingest_stream(
            vec![("service".into(), "a".into())],
            vec![
                make_entry(1000, "1"),
                make_entry(2000, "2"),
                make_entry(3000, "3"),
            ],
        );
        let ids = store.query_streams(&[LabelMatcher {
            name: "service".into(),
            op: LabelMatchOp::Eq,
            value: "a".into(),
        }]);
        let entries = store.get_entries(ids[0], 1500, 2500);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].line, "2");
    }

    #[test]
    fn test_eviction_prunes_label_values() {
        let mut store = LogStore::new();
        store.ingest_stream(
            vec![("service".into(), "payments".into())],
            vec![make_entry(1000, "test")],
        );
        assert!(!store.label_names().is_empty());
        store.evict_before(2000);
        assert!(store.label_names().is_empty());
        assert!(store.get_label_values("service").is_empty());
    }

    #[test]
    fn test_internally_unsorted_batch_after_tail() {
        let mut store = LogStore::new();
        store.ingest_stream(
            vec![("service".into(), "test".into())],
            vec![LogEntry {
                timestamp_ns: 100,
                line: "a".into(),
            }],
        );
        // Append internally unsorted batch — all > 100
        store.ingest_stream(
            vec![("service".into(), "test".into())],
            vec![
                LogEntry {
                    timestamp_ns: 300,
                    line: "c".into(),
                },
                LogEntry {
                    timestamp_ns: 200,
                    line: "b".into(),
                },
            ],
        );
        // Verify sorted
        for stream in store.streams.values() {
            for w in stream.entries.windows(2) {
                assert!(
                    w[0].timestamp_ns <= w[1].timestamp_ns,
                    "entries must be sorted: {} > {}",
                    w[0].timestamp_ns,
                    w[1].timestamp_ns
                );
            }
        }
    }
}
