//! Metric storage engine for PromQL queries.
//!
//! `MetricStore` stores time-series data indexed by metric name and label pairs.

use lasso::{Rodeo, Spur};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use super::posting_list::PostingList;
use super::{LabelMatcher, LabelPairs};

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

/// Errors that can occur while registering metric identities.
#[derive(Debug, thiserror::Error)]
pub enum MetricStoreError {
    #[error(
        "metric name collision: normalized name `{normalized}` maps to both `{existing}` and `{incoming}`"
    )]
    MetricNameCollision {
        normalized: String,
        existing: String,
        incoming: String,
    },
}

/// In-memory metric storage with inverted index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricStore {
    /// Series ID -> series data.
    pub series: FxHashMap<u64, MetricSeries>,
    /// Label pair (name, value) -> set of series IDs.
    pub label_index: FxHashMap<(Spur, Spur), PostingList>,
    /// Label name -> set of known values.
    pub label_values: FxHashMap<Spur, FxHashSet<Spur>>,
    /// String interner.
    pub interner: Rodeo,
    /// Total series count for eviction.
    pub total_samples: usize,
    /// Exact label-set to series ID mapping to avoid hash-collision merges.
    #[serde(default)]
    pub series_ids: FxHashMap<LabelPairs, u64>,
    /// Next series ID for newly observed label sets.
    #[serde(default)]
    pub next_series_id: u64,
    /// Normalized metric name -> original source metric name.
    #[serde(default)]
    pub normalized_name_sources: FxHashMap<Spur, Spur>,
}

impl MetricStore {
    /// Create a new empty metric store.
    pub fn new() -> Self {
        Self {
            series: FxHashMap::default(),
            label_index: FxHashMap::default(),
            label_values: FxHashMap::default(),
            interner: Rodeo::default(),
            total_samples: 0,
            series_ids: FxHashMap::default(),
            next_series_id: 0,
            normalized_name_sources: FxHashMap::default(),
        }
    }

    /// Rebuild exact series identity maps after loading older snapshots.
    pub fn rebuild_series_ids(&mut self) {
        self.series_ids.clear();
        self.next_series_id = 0;

        let mut series_ids: Vec<u64> = self.series.keys().copied().collect();
        series_ids.sort_unstable();
        for series_id in series_ids {
            if let Some(series) = self.series.get(&series_id) {
                self.series_ids.insert(series.labels.clone(), series_id);
                self.next_series_id = self.next_series_id.max(series_id.saturating_add(1));
            }
        }
    }

    /// Check for a metric-name collision without mutating store state.
    pub fn check_metric_name_collision(
        &self,
        normalized_name: &str,
        source_name: &str,
    ) -> Result<(), MetricStoreError> {
        let Some(normalized_spur) = self.interner.get(normalized_name) else {
            return Ok(());
        };

        match self.normalized_name_sources.get(&normalized_spur).copied() {
            Some(existing) if self.interner.resolve(&existing) != source_name => {
                Err(MetricStoreError::MetricNameCollision {
                    normalized: normalized_name.to_string(),
                    existing: self.interner.resolve(&existing).to_string(),
                    incoming: source_name.to_string(),
                })
            }
            _ => Ok(()),
        }
    }

    /// Register a visible metric name against its original source metric name.
    pub fn register_metric_name(
        &mut self,
        normalized_name: &str,
        source_name: &str,
    ) -> Result<(), MetricStoreError> {
        let normalized_spur = self.interner.get_or_intern(normalized_name);
        let source_spur = self.interner.get_or_intern(source_name);

        match self.normalized_name_sources.get(&normalized_spur).copied() {
            Some(existing) if existing != source_spur => {
                Err(MetricStoreError::MetricNameCollision {
                    normalized: normalized_name.to_string(),
                    existing: self.interner.resolve(&existing).to_string(),
                    incoming: source_name.to_string(),
                })
            }
            Some(_) => Ok(()),
            None => {
                self.normalized_name_sources
                    .insert(normalized_spur, source_spur);
                Ok(())
            }
        }
    }

    /// Ingest samples for a metric with given name and labels.
    pub fn ingest_samples(
        &mut self,
        name: &str,
        labels: Vec<(String, String)>,
        samples: Vec<Sample>,
    ) {
        let mut all_labels = Vec::with_capacity(labels.len() + 1);
        all_labels.push(("__name__".to_string(), name.to_string()));
        all_labels.extend(labels);

        let interned_labels = super::intern_label_pairs(&mut self.interner, &all_labels);
        super::track_label_values(&mut self.label_values, &interned_labels);

        let series_id = match self.series_ids.get(&interned_labels).copied() {
            Some(series_id) => series_id,
            None => {
                let series_id = self.next_series_id;
                self.next_series_id = self.next_series_id.saturating_add(1);
                self.series_ids.insert(interned_labels.clone(), series_id);
                self.series.insert(
                    series_id,
                    MetricSeries {
                        labels: interned_labels.clone(),
                        samples: Vec::new(),
                    },
                );
                for &(k, v) in &interned_labels {
                    self.label_index
                        .entry((k, v))
                        .or_default()
                        .insert(series_id);
                }
                series_id
            }
        };

        let sample_count = samples.len();
        let series = self.series.get_mut(&series_id).expect("series must exist");

        let was_empty = series.samples.is_empty();
        for sample in samples {
            series.samples.push(sample);
        }
        self.total_samples += sample_count;

        // Maintain sorted order for partition_point correctness.
        if sample_count > 1 || !was_empty {
            let needs_sort = !series
                .samples
                .windows(2)
                .all(|w| w[0].timestamp_ms <= w[1].timestamp_ms);
            if needs_sort {
                series.samples.sort_by_key(|s| s.timestamp_ms);
            }
        }
    }

    /// Select series matching all the given label matchers.
    /// Matchers work the same as LogStore's label matchers.
    pub fn select_series(&self, matchers: &[LabelMatcher]) -> Vec<u64> {
        super::select_indexed_ids(
            &self.interner,
            &self.label_index,
            &self.label_values,
            || self.series.keys().copied().collect(),
            matchers,
        )
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
                self.series_ids.remove(&series.labels);
                super::remove_from_label_indexes(
                    &mut self.label_index,
                    &mut self.label_values,
                    &series.labels,
                    sid,
                );
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
                self.series_ids.remove(&series.labels);
                super::remove_from_label_indexes(
                    &mut self.label_index,
                    &mut self.label_values,
                    &series.labels,
                    series_id,
                );
            }
        }
    }

    /// Clear all data from the store.
    pub fn clear(&mut self) {
        self.series.clear();
        self.label_index.clear();
        self.label_values.clear();
        self.interner = Rodeo::default();
        self.total_samples = 0;
        self.series_ids.clear();
        self.next_series_id = 0;
        self.normalized_name_sources.clear();
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
                self.series_ids.remove(&series.labels);
                super::remove_from_label_indexes(
                    &mut self.label_index,
                    &mut self.label_values,
                    &series.labels,
                    series_id,
                );
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

        // Exact identity and normalized-name tracking
        bytes += self.series_ids.len()
            * (std::mem::size_of::<LabelPairs>() + std::mem::size_of::<u64>() + 8);
        bytes += self.normalized_name_sources.len() * (std::mem::size_of::<Spur>() * 2 + 8);

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
    use crate::store::{LabelMatchOp, LabelMatcher};

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

    #[test]
    fn test_internally_unsorted_batch_after_tail() {
        let mut store = MetricStore::new();
        store.ingest_samples(
            "cpu",
            vec![("host".into(), "a".into())],
            vec![Sample {
                timestamp_ms: 100,
                value: 1.0,
            }],
        );
        // Append internally unsorted batch — all > 100
        store.ingest_samples(
            "cpu",
            vec![("host".into(), "a".into())],
            vec![
                Sample {
                    timestamp_ms: 300,
                    value: 3.0,
                },
                Sample {
                    timestamp_ms: 200,
                    value: 2.0,
                },
            ],
        );
        // Verify sorted
        for series in store.series.values() {
            for w in series.samples.windows(2) {
                assert!(
                    w[0].timestamp_ms <= w[1].timestamp_ms,
                    "samples must be sorted: {} > {}",
                    w[0].timestamp_ms,
                    w[1].timestamp_ms
                );
            }
        }
    }
}
