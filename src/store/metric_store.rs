//! Metric storage engine for PromQL queries.
//!
//! `MetricStore` stores time-series data indexed by metric name and label pairs.

use std::hash::{Hash, Hasher};

use lasso::{Rodeo, Spur};
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use super::posting_list::{PostingList, intersect, union};

/// A single metric sample.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Sample {
    pub timestamp_ms: i64,
    pub value: f64,
}

/// A metric time-series identified by labels (including __name__).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSeries {
    pub labels: SmallVec<[(Spur, Spur); 8]>,
    pub samples: Vec<Sample>,
}

/// In-memory metric storage with inverted index.
#[derive(Debug, Serialize, Deserialize)]
pub struct MetricStore {
    /// Series ID -> series data.
    pub series: FxHashMap<u64, MetricSeries>,
    /// Metric name -> set of series IDs.
    pub name_index: FxHashMap<Spur, PostingList>,
    /// Label pair (name, value) -> set of series IDs.
    pub label_index: FxHashMap<(Spur, Spur), PostingList>,
    /// Label name -> set of known values.
    pub label_values: FxHashMap<Spur, FxHashSet<Spur>>,
    /// String interner.
    pub interner: Rodeo,
    /// Total series count for eviction.
    pub total_samples: usize,
}

impl MetricStore {
    /// Create a new empty metric store.
    pub fn new() -> Self {
        Self {
            series: FxHashMap::default(),
            name_index: FxHashMap::default(),
            label_index: FxHashMap::default(),
            label_values: FxHashMap::default(),
            interner: Rodeo::default(),
            total_samples: 0,
        }
    }

    /// Compute series ID from __name__ + sorted label set.
    fn compute_series_id(labels: &[(Spur, Spur)]) -> u64 {
        let mut hasher = FxHasher::default();
        for (k, v) in labels {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Ingest samples for a metric with given name and labels.
    pub fn ingest_samples(
        &mut self,
        name: &str,
        labels: Vec<(String, String)>,
        samples: Vec<Sample>,
    ) {
        let name_spur = self.interner.get_or_intern(name);
        let name_key = self.interner.get_or_intern("__name__");

        // Intern labels and add __name__
        let mut interned_labels: SmallVec<[(Spur, Spur); 8]> = SmallVec::new();
        interned_labels.push((name_key, name_spur));
        for (k, v) in &labels {
            let ks = self.interner.get_or_intern(k);
            let vs = self.interner.get_or_intern(v);
            interned_labels.push((ks, vs));
        }
        interned_labels.sort_by_key(|(k, _)| *k);

        // Track label values
        for &(k, v) in &interned_labels {
            self.label_values.entry(k).or_default().insert(v);
        }

        let series_id = Self::compute_series_id(&interned_labels);

        let is_new = !self.series.contains_key(&series_id);

        let sample_count = samples.len();
        let series = self
            .series
            .entry(series_id)
            .or_insert_with(|| MetricSeries {
                labels: interned_labels.clone(),
                samples: Vec::new(),
            });

        let was_empty = series.samples.is_empty();
        let prev_last_ts = series.samples.last().map(|s| s.timestamp_ms);
        for sample in samples {
            series.samples.push(sample);
        }
        self.total_samples += sample_count;

        // Maintain sorted order for partition_point correctness
        if was_empty {
            // First batch: check if it's already sorted
            if !series
                .samples
                .windows(2)
                .all(|w| w[0].timestamp_ms <= w[1].timestamp_ms)
            {
                series.samples.sort_by_key(|s| s.timestamp_ms);
            }
        } else if let Some(prev_ts) = prev_last_ts
            && series.samples[series.samples.len() - sample_count..]
                .iter()
                .any(|s| s.timestamp_ms < prev_ts)
        {
            series.samples.sort_by_key(|s| s.timestamp_ms);
        }

        // Only update indexes for new series
        if is_new {
            self.name_index
                .entry(name_spur)
                .or_default()
                .insert(series_id);

            for &(k, v) in &interned_labels {
                self.label_index
                    .entry((k, v))
                    .or_default()
                    .insert(series_id);
            }
        }
    }

    /// Select series matching all the given label matchers.
    /// Matchers work the same as LogStore's label matchers.
    pub fn select_series(&self, matchers: &[super::log_store::LabelMatcher]) -> Vec<u64> {
        use super::log_store::LabelMatchOp;

        if matchers.is_empty() {
            return self.series.keys().copied().collect();
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
                            let mut all_ids: Vec<u64> = self.series.keys().copied().collect();
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
                    // Special case: __name__ matcher uses name_index
                    if matcher.name == "__name__" {
                        let value_spur = match self.interner.get(&matcher.value) {
                            Some(s) => s,
                            None => return Vec::new(),
                        };
                        match self.name_index.get(&value_spur) {
                            Some(pl) => positive_lists.push(pl.clone()),
                            None => return Vec::new(),
                        }
                    } else {
                        let value_spur = match self.interner.get(&matcher.value) {
                            Some(s) => s,
                            None => return Vec::new(),
                        };
                        match self.label_index.get(&(name_spur, value_spur)) {
                            Some(pl) => positive_lists.push(pl.clone()),
                            None => return Vec::new(),
                        }
                    }
                }
                LabelMatchOp::Neq => {
                    let value_spur = self.interner.get(&matcher.value);
                    // Start with ALL series IDs
                    let mut all_ids: Vec<u64> = self.series.keys().copied().collect();
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
                    // Start with ALL series IDs
                    let mut all_ids: Vec<u64> = self.series.keys().copied().collect();
                    all_ids.sort_unstable();
                    let mut result = PostingList::new();
                    // Build exclude set: series that DO match the regex
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

        let refs: Vec<&PostingList> = positive_lists.iter().collect();
        intersect(&refs)
    }

    /// Get samples for a series within a time range (milliseconds).
    pub fn get_samples(&self, series_id: u64, start_ms: i64, end_ms: i64) -> &[Sample] {
        match self.series.get(&series_id) {
            Some(series) => {
                let lo = series
                    .samples
                    .partition_point(|s| s.timestamp_ms < start_ms);
                let hi = series.samples.partition_point(|s| s.timestamp_ms <= end_ms);
                &series.samples[lo..hi]
            }
            None => &[],
        }
    }

    /// Get labels for a series as resolved strings.
    pub fn get_series_labels(&self, series_id: u64) -> Option<Vec<(String, String)>> {
        self.series.get(&series_id).map(|series| {
            series
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

    /// Evict oldest series until series count <= max_series.
    pub fn evict_to_max(&mut self, max_series: usize) {
        if self.series.len() <= max_series {
            return;
        }

        // Sort series by oldest sample timestamp (oldest first)
        let mut series_ids: Vec<u64> = self.series.keys().copied().collect();
        series_ids.sort_by_key(|id| {
            self.series[id]
                .samples
                .first()
                .map(|s| s.timestamp_ms)
                .unwrap_or(i64::MAX)
        });

        let to_remove = self.series.len() - max_series;
        for sid in series_ids.into_iter().take(to_remove) {
            if let Some(series) = self.series.remove(&sid) {
                self.total_samples = self.total_samples.saturating_sub(series.samples.len());

                // Clean up label_index
                for &(k, v) in &series.labels {
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
                // Clean up name_index
                let name_key = self.interner.get("__name__");
                if let Some(name_key) = name_key {
                    for &(k, v) in &series.labels {
                        if k == name_key
                            && let Some(pl) = self.name_index.get_mut(&v)
                        {
                            pl.remove(sid);
                            if pl.is_empty() {
                                self.name_index.remove(&v);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Evict samples older than the given timestamp.
    pub fn evict_before(&mut self, cutoff_ms: i64) {
        let mut empty_series = Vec::new();
        for (&series_id, series) in &mut self.series {
            let before = series.samples.len();
            let drain_count = series
                .samples
                .partition_point(|s| s.timestamp_ms < cutoff_ms);
            if drain_count > 0 {
                series.samples.drain(..drain_count);
            }
            let removed = before - series.samples.len();
            self.total_samples = self.total_samples.saturating_sub(removed);
            if series.samples.is_empty() {
                empty_series.push(series_id);
            }
        }

        for series_id in empty_series {
            if let Some(series) = self.series.remove(&series_id) {
                for &(k, v) in &series.labels {
                    if let Some(pl) = self.label_index.get_mut(&(k, v)) {
                        pl.remove(series_id);
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
                // Also remove from name_index
                let name_key = self.interner.get("__name__");
                if let Some(name_key) = name_key {
                    for &(k, v) in &series.labels {
                        if k == name_key
                            && let Some(pl) = self.name_index.get_mut(&v)
                        {
                            pl.remove(series_id);
                            if pl.is_empty() {
                                self.name_index.remove(&v);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Clear all data from the store.
    pub fn clear(&mut self) {
        self.series.clear();
        self.name_index.clear();
        self.label_index.clear();
        self.label_values.clear();
        self.interner = Rodeo::default();
        self.total_samples = 0;
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

        // Find series that have service=<value>
        let series_ids: Vec<u64> = match self.label_index.get(&(service_key, service_spur)) {
            Some(pl) => pl.ids().to_vec(),
            None => return,
        };

        for series_id in series_ids {
            if let Some(series) = self.series.remove(&series_id) {
                self.total_samples = self.total_samples.saturating_sub(series.samples.len());
                // Clean up label_index and label_values
                for &(k, v) in &series.labels {
                    if let Some(pl) = self.label_index.get_mut(&(k, v)) {
                        pl.remove(series_id);
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
                // Clean up name_index
                let name_key = self.interner.get("__name__");
                if let Some(name_key) = name_key {
                    for &(k, v) in &series.labels {
                        if k == name_key
                            && let Some(pl) = self.name_index.get_mut(&v)
                        {
                            pl.remove(series_id);
                            if pl.is_empty() {
                                self.name_index.remove(&v);
                            }
                        }
                    }
                }
            }
        }
    }
}

impl MetricStore {
    /// Estimate the memory usage of this store in bytes.
    ///
    /// Accounts for series labels, sample data, index overhead, and interner memory.
    pub fn memory_estimate_bytes(&self) -> usize {
        let mut bytes = 0usize;

        // Per-series overhead
        for series in self.series.values() {
            // Labels: SmallVec of (Spur, Spur) pairs
            bytes += series.labels.len() * std::mem::size_of::<(Spur, Spur)>();
            // Samples: Vec<Sample> capacity
            bytes += series.samples.capacity() * std::mem::size_of::<Sample>();
        }

        // HashMap overhead for series
        bytes += self.series.len()
            * (std::mem::size_of::<u64>() + std::mem::size_of::<MetricSeries>() + 8);

        // Name index: FxHashMap<Spur, PostingList>
        for pl in self.name_index.values() {
            bytes += pl.len() * std::mem::size_of::<u64>();
        }
        bytes += self.name_index.len()
            * (std::mem::size_of::<Spur>() + std::mem::size_of::<PostingList>() + 8);

        // Label index: FxHashMap<(Spur, Spur), PostingList>
        for pl in self.label_index.values() {
            bytes += pl.len() * std::mem::size_of::<u64>();
        }
        bytes += self.label_index.len()
            * (std::mem::size_of::<(Spur, Spur)>() + std::mem::size_of::<PostingList>() + 8);

        // Label values index
        for vals in self.label_values.values() {
            bytes += vals.len() * (std::mem::size_of::<Spur>() + 8);
        }
        bytes += self.label_values.len() * (std::mem::size_of::<Spur>() + 8);

        // Interner memory
        bytes += self.interner.current_memory_usage();

        bytes
    }
}

impl Default for MetricStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::log_store::{LabelMatchOp, LabelMatcher};

    #[test]
    fn test_ingest_and_select() {
        let mut store = MetricStore::new();
        store.ingest_samples(
            "http_requests_total",
            vec![("method".into(), "GET".into())],
            vec![Sample {
                timestamp_ms: 1000,
                value: 42.0,
            }],
        );

        let ids = store.select_series(&[LabelMatcher {
            name: "__name__".into(),
            op: LabelMatchOp::Eq,
            value: "http_requests_total".into(),
        }]);
        assert_eq!(ids.len(), 1);

        let samples = store.get_samples(ids[0], 0, i64::MAX);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].value, 42.0);
    }

    #[test]
    fn test_select_by_label() {
        let mut store = MetricStore::new();
        store.ingest_samples(
            "http_requests_total",
            vec![("method".into(), "GET".into())],
            vec![Sample {
                timestamp_ms: 1000,
                value: 10.0,
            }],
        );
        store.ingest_samples(
            "http_requests_total",
            vec![("method".into(), "POST".into())],
            vec![Sample {
                timestamp_ms: 1000,
                value: 5.0,
            }],
        );

        let ids = store.select_series(&[
            LabelMatcher {
                name: "__name__".into(),
                op: LabelMatchOp::Eq,
                value: "http_requests_total".into(),
            },
            LabelMatcher {
                name: "method".into(),
                op: LabelMatchOp::Eq,
                value: "GET".into(),
            },
        ]);
        assert_eq!(ids.len(), 1);
        let samples = store.get_samples(ids[0], 0, i64::MAX);
        assert_eq!(samples[0].value, 10.0);
    }

    #[test]
    fn test_eviction() {
        let mut store = MetricStore::new();
        store.ingest_samples(
            "m",
            vec![],
            vec![
                Sample {
                    timestamp_ms: 1000,
                    value: 1.0,
                },
                Sample {
                    timestamp_ms: 5000,
                    value: 2.0,
                },
            ],
        );
        assert_eq!(store.total_samples, 2);
        store.evict_before(3000);
        assert_eq!(store.total_samples, 1);
    }

    #[test]
    fn test_neq_matches_missing_label() {
        let mut store = MetricStore::new();
        store.ingest_samples(
            "cpu",
            vec![
                ("host".into(), "server1".into()),
                ("env".into(), "prod".into()),
            ],
            vec![Sample {
                timestamp_ms: 1000,
                value: 1.0,
            }],
        );
        store.ingest_samples(
            "cpu",
            vec![("host".into(), "server2".into())], // no "env" label
            vec![Sample {
                timestamp_ms: 2000,
                value: 2.0,
            }],
        );
        // {__name__="cpu", env!="prod"} should match server2 (missing env label)
        let ids = store.select_series(&[
            LabelMatcher {
                name: "__name__".into(),
                op: LabelMatchOp::Eq,
                value: "cpu".into(),
            },
            LabelMatcher {
                name: "env".into(),
                op: LabelMatchOp::Neq,
                value: "prod".into(),
            },
        ]);
        assert_eq!(ids.len(), 1);
        let samples = store.get_samples(ids[0], 0, i64::MAX);
        assert_eq!(samples[0].value, 2.0);
    }

    #[test]
    fn test_not_regex_matches_missing_label() {
        let mut store = MetricStore::new();
        store.ingest_samples(
            "cpu",
            vec![
                ("host".into(), "server1".into()),
                ("env".into(), "staging".into()),
            ],
            vec![Sample {
                timestamp_ms: 1000,
                value: 1.0,
            }],
        );
        store.ingest_samples(
            "cpu",
            vec![("host".into(), "server2".into())], // no "env" label
            vec![Sample {
                timestamp_ms: 2000,
                value: 2.0,
            }],
        );
        // {__name__="cpu", env!~"staging"} should match server2 (missing env label)
        let ids = store.select_series(&[
            LabelMatcher {
                name: "__name__".into(),
                op: LabelMatchOp::Eq,
                value: "cpu".into(),
            },
            LabelMatcher {
                name: "env".into(),
                op: LabelMatchOp::NotRegex,
                value: "staging".into(),
            },
        ]);
        assert_eq!(ids.len(), 1);
        let samples = store.get_samples(ids[0], 0, i64::MAX);
        assert_eq!(samples[0].value, 2.0);
    }

    #[test]
    fn test_out_of_order_ingest() {
        let mut store = MetricStore::new();
        store.ingest_samples(
            "m",
            vec![],
            vec![
                Sample {
                    timestamp_ms: 3000,
                    value: 3.0,
                },
                Sample {
                    timestamp_ms: 1000,
                    value: 1.0,
                },
                Sample {
                    timestamp_ms: 2000,
                    value: 2.0,
                },
            ],
        );
        let ids = store.select_series(&[LabelMatcher {
            name: "__name__".into(),
            op: LabelMatchOp::Eq,
            value: "m".into(),
        }]);
        let samples = store.get_samples(ids[0], 0, i64::MAX);
        assert_eq!(samples.len(), 3);
        assert_eq!(samples[0].value, 1.0);
        assert_eq!(samples[1].value, 2.0);
        assert_eq!(samples[2].value, 3.0);

        // Verify partition_point works for range queries
        let samples = store.get_samples(ids[0], 1500, 2500);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].value, 2.0);
    }

    #[test]
    fn test_evict_to_max_series() {
        let mut store = MetricStore::new();
        // Create 3 series with different oldest timestamps
        store.ingest_samples(
            "m",
            vec![("host".into(), "a".into())],
            vec![Sample {
                timestamp_ms: 1000,
                value: 1.0,
            }],
        );
        store.ingest_samples(
            "m",
            vec![("host".into(), "b".into())],
            vec![Sample {
                timestamp_ms: 2000,
                value: 2.0,
            }],
        );
        store.ingest_samples(
            "m",
            vec![("host".into(), "c".into())],
            vec![
                Sample {
                    timestamp_ms: 3000,
                    value: 3.0,
                },
                Sample {
                    timestamp_ms: 4000,
                    value: 4.0,
                },
            ],
        );
        assert_eq!(store.series.len(), 3);
        assert_eq!(store.total_samples, 4);

        // Evict to max 1 series — should remove the 2 oldest series (a, b)
        store.evict_to_max(1);
        assert_eq!(store.series.len(), 1);
        assert_eq!(store.total_samples, 2); // only series c remains with 2 samples
    }

    #[test]
    fn test_label_names_and_values() {
        let mut store = MetricStore::new();
        store.ingest_samples(
            "cpu",
            vec![("host".into(), "server1".into())],
            vec![Sample {
                timestamp_ms: 1000,
                value: 0.5,
            }],
        );
        let names = store.label_names();
        assert!(names.contains(&"__name__".to_string()));
        assert!(names.contains(&"host".to_string()));

        let values = store.get_label_values("host");
        assert_eq!(values, vec!["server1".to_string()]);
    }

    #[test]
    fn test_eviction_prunes_label_values() {
        let mut store = MetricStore::new();
        store.ingest_samples(
            "cpu",
            vec![("host".into(), "a".into())],
            vec![Sample {
                timestamp_ms: 1000,
                value: 0.5,
            }],
        );
        assert!(!store.label_names().is_empty());
        store.evict_before(2000);
        assert!(store.label_names().is_empty());
    }
}
