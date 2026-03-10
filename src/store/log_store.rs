//! Log storage engine with inverted index for LogQL queries.
//!
//! `LogStore` stores log streams indexed by label pairs using sorted posting lists.
//! Each stream contains an ordered sequence of `LogEntry` items.

use std::hash::{Hash, Hasher};

use lasso::{Rodeo, Spur};
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use super::posting_list::{PostingList, intersect, union};

/// Unique identifier for a log stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamId(pub u64);

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

/// Types of label matchers for queries.
#[derive(Debug, Clone)]
pub enum LabelMatchOp {
    Eq,
    Neq,
    Regex,
    NotRegex,
}

/// A label matcher used in stream selectors.
#[derive(Debug, Clone)]
pub struct LabelMatcher {
    pub name: String,
    pub op: LabelMatchOp,
    pub value: String,
}

/// In-memory log storage with inverted index.
#[derive(Debug, Serialize, Deserialize)]
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
        }
    }

    /// Compute stream ID from a sorted label set.
    fn compute_stream_id(labels: &[(Spur, Spur)]) -> u64 {
        let mut hasher = FxHasher::default();
        for (k, v) in labels {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Ingest a stream with the given labels and entries.
    pub fn ingest_stream(&mut self, labels: Vec<(String, String)>, entries: Vec<LogEntry>) {
        // Intern and sort labels
        let mut interned_labels: SmallVec<[(Spur, Spur); 8]> = labels
            .iter()
            .map(|(k, v)| {
                let ks = self.interner.get_or_intern(k);
                let vs = self.interner.get_or_intern(v);
                (ks, vs)
            })
            .collect();
        interned_labels.sort_by_key(|(k, _)| *k);

        // Track label values
        for &(k, v) in &interned_labels {
            self.label_values.entry(k).or_default().insert(v);
        }

        let stream_id = Self::compute_stream_id(&interned_labels);

        let is_new = !self.streams.contains_key(&stream_id);

        let entry_count = entries.len();
        let stream = self.streams.entry(stream_id).or_insert_with(|| LogStream {
            labels: interned_labels.clone(),
            entries: Vec::new(),
        });

        let was_empty = stream.entries.is_empty();
        let prev_last_ts = stream.entries.last().map(|e| e.timestamp_ns);
        for entry in entries {
            stream.entries.push(entry);
        }
        self.total_entries += entry_count;

        // Maintain sorted order for partition_point correctness
        if was_empty {
            // First batch: check if it's already sorted
            if !stream
                .entries
                .windows(2)
                .all(|w| w[0].timestamp_ns <= w[1].timestamp_ns)
            {
                stream.entries.sort_by_key(|e| e.timestamp_ns);
            }
        } else if let Some(prev_ts) = prev_last_ts
            && stream.entries[stream.entries.len() - entry_count..]
                .iter()
                .any(|e| e.timestamp_ns < prev_ts)
        {
            stream.entries.sort_by_key(|e| e.timestamp_ns);
        }

        // Only update inverted index for new streams
        if is_new {
            for &(k, v) in &interned_labels {
                self.label_index
                    .entry((k, v))
                    .or_default()
                    .insert(stream_id);
            }
        }
    }

    /// Query streams matching all the given label matchers.
    pub fn query_streams(&self, matchers: &[LabelMatcher]) -> Vec<u64> {
        if matchers.is_empty() {
            return self.streams.keys().copied().collect();
        }

        let mut positive_lists: Vec<PostingList> = Vec::new();

        for matcher in matchers {
            let name_spur = match self.interner.get(&matcher.name) {
                Some(s) => s,
                None => {
                    // Label name not known at all
                    match matcher.op {
                        // Negative matchers match everything when label is missing
                        LabelMatchOp::Neq | LabelMatchOp::NotRegex => {
                            let mut all = PostingList::new();
                            let mut all_ids: Vec<u64> = self.streams.keys().copied().collect();
                            all_ids.sort_unstable();
                            for id in all_ids {
                                all.insert(id);
                            }
                            positive_lists.push(all);
                            continue;
                        }
                        // Positive matchers can't match an unknown label
                        _ => return Vec::new(),
                    }
                }
            };

            match matcher.op {
                LabelMatchOp::Eq => {
                    let value_spur = match self.interner.get(&matcher.value) {
                        Some(s) => s,
                        None => return Vec::new(),
                    };
                    match self.label_index.get(&(name_spur, value_spur)) {
                        Some(pl) => positive_lists.push(pl.clone()),
                        None => return Vec::new(),
                    }
                }
                LabelMatchOp::Neq => {
                    let value_spur = self.interner.get(&matcher.value);
                    // Start with ALL stream IDs
                    let mut all_ids: Vec<u64> = self.streams.keys().copied().collect();
                    all_ids.sort_unstable();
                    let mut result = PostingList::new();
                    // Get the IDs to exclude (those that have the exact matching value)
                    let exclude = match value_spur {
                        Some(vs) => self
                            .label_index
                            .get(&(name_spur, vs))
                            .cloned()
                            .unwrap_or_default(),
                        None => PostingList::new(),
                    };
                    for id in all_ids {
                        if !exclude.contains(id) {
                            result.insert(id);
                        }
                    }
                    positive_lists.push(result);
                }
                LabelMatchOp::Regex => {
                    let re = match regex::Regex::new(&format!("^(?:{})$", matcher.value)) {
                        Ok(r) => r,
                        Err(_) => return Vec::new(),
                    };
                    let mut result = PostingList::new();
                    if let Some(values) = self.label_values.get(&name_spur) {
                        for &vs in values {
                            let val_str = self.interner.resolve(&vs);
                            if re.is_match(val_str)
                                && let Some(pl) = self.label_index.get(&(name_spur, vs))
                            {
                                result = union(&result, pl);
                            }
                        }
                    }
                    positive_lists.push(result);
                }
                LabelMatchOp::NotRegex => {
                    let re = match regex::Regex::new(&format!("^(?:{})$", matcher.value)) {
                        Ok(r) => r,
                        Err(_) => return Vec::new(),
                    };
                    // Start with ALL stream IDs
                    let mut all_ids: Vec<u64> = self.streams.keys().copied().collect();
                    all_ids.sort_unstable();
                    let mut result = PostingList::new();
                    // Build exclude set: streams that DO match the regex
                    let mut exclude = PostingList::new();
                    if let Some(values) = self.label_values.get(&name_spur) {
                        for &vs in values {
                            let val_str = self.interner.resolve(&vs);
                            if re.is_match(val_str)
                                && let Some(pl) = self.label_index.get(&(name_spur, vs))
                            {
                                exclude = union(&exclude, pl);
                            }
                        }
                    }
                    for id in all_ids {
                        if !exclude.contains(id) {
                            result.insert(id);
                        }
                    }
                    positive_lists.push(result);
                }
            }
        }

        // Intersect all positive lists
        let refs: Vec<&PostingList> = positive_lists.iter().collect();
        intersect(&refs)
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
                for &(k, v) in &stream.labels {
                    if let Some(pl) = self.label_index.get_mut(&(k, v)) {
                        pl.remove(stream_id);
                        if pl.is_empty() {
                            self.label_index.remove(&(k, v));
                            if let Some(vals) = self.label_values.get_mut(&k) {
                                vals.remove(&v);
                                if vals.is_empty() {
                                    self.label_values.remove(&k);
                                }
                            }
                        }
                    }
                }
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
                    for &(k, v) in &stream.labels {
                        if let Some(pl) = self.label_index.get_mut(&(k, v)) {
                            pl.remove(sid);
                            if pl.is_empty() {
                                self.label_index.remove(&(k, v));
                                if let Some(vals) = self.label_values.get_mut(&k) {
                                    vals.remove(&v);
                                    if vals.is_empty() {
                                        self.label_values.remove(&k);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
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
}
