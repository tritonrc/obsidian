//! Storage engine: in-memory stores for logs, metrics, and traces.
//!
//! All stores are behind `parking_lot::RwLock` within a shared `AppState`.

pub mod log_store;
pub mod metric_store;
pub mod posting_list;
pub mod trace_store;

use std::sync::Arc;
use std::time::Instant;

use lasso::{Rodeo, Spur};
use parking_lot::RwLock;
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

use crate::config::Config;
pub use log_store::LogStore;
pub use metric_store::MetricStore;
use posting_list::{PostingList, intersect, union};
pub use trace_store::TraceStore;

/// Shared application state accessible by all handlers.
pub struct AppState {
    pub log_store: RwLock<LogStore>,
    pub metric_store: RwLock<MetricStore>,
    pub trace_store: RwLock<TraceStore>,
    pub config: Config,
    pub start_time: Instant,
}

/// Type alias for the shared state handle.
pub type SharedState = Arc<AppState>;

/// Shared compact label set representation used by indexed stores.
pub type LabelPairs = SmallVec<[(Spur, Spur); 8]>;

/// Types of label matchers for queries.
#[derive(Debug, Clone)]
pub enum LabelMatchOp {
    Eq,
    Neq,
    Regex,
    NotRegex,
}

/// A label matcher used in stream and series selectors.
#[derive(Debug, Clone)]
pub struct LabelMatcher {
    pub name: String,
    pub op: LabelMatchOp,
    pub value: String,
}

pub(crate) fn intern_label_pairs(interner: &mut Rodeo, labels: &[(String, String)]) -> LabelPairs {
    let mut interned: LabelPairs = labels
        .iter()
        .map(|(k, v)| (interner.get_or_intern(k), interner.get_or_intern(v)))
        .collect();
    interned.sort_by_key(|(k, _)| *k);
    interned
}

pub(crate) fn track_label_values(
    label_values: &mut FxHashMap<Spur, FxHashSet<Spur>>,
    labels: &LabelPairs,
) {
    for &(k, v) in labels {
        label_values.entry(k).or_default().insert(v);
    }
}

pub(crate) fn remove_from_label_indexes(
    label_index: &mut FxHashMap<(Spur, Spur), PostingList>,
    label_values: &mut FxHashMap<Spur, FxHashSet<Spur>>,
    labels: &LabelPairs,
    id: u64,
) {
    for &(k, v) in labels {
        if let Some(pl) = label_index.get_mut(&(k, v)) {
            pl.remove(id);
            if pl.is_empty() {
                label_index.remove(&(k, v));
                if let Some(vals) = label_values.get_mut(&k) {
                    vals.remove(&v);
                    if vals.is_empty() {
                        label_values.remove(&k);
                    }
                }
            }
        }
    }
}

pub(crate) fn select_indexed_ids<F>(
    interner: &Rodeo,
    label_index: &FxHashMap<(Spur, Spur), PostingList>,
    label_values: &FxHashMap<Spur, FxHashSet<Spur>>,
    all_ids: F,
    matchers: &[LabelMatcher],
) -> Vec<u64>
where
    F: Fn() -> Vec<u64>,
{
    if matchers.is_empty() {
        return all_ids();
    }

    let mut positive_lists: Vec<PostingList> = Vec::new();

    for matcher in matchers {
        let name_spur = match interner.get(&matcher.name) {
            Some(s) => s,
            None => match matcher.op {
                LabelMatchOp::Neq | LabelMatchOp::NotRegex => {
                    positive_lists.push(all_ids_posting_list(all_ids()));
                    continue;
                }
                _ => return Vec::new(),
            },
        };

        match matcher.op {
            LabelMatchOp::Eq => {
                let value_spur = match interner.get(&matcher.value) {
                    Some(s) => s,
                    None => return Vec::new(),
                };
                match label_index.get(&(name_spur, value_spur)) {
                    Some(pl) => positive_lists.push(pl.clone()),
                    None => return Vec::new(),
                }
            }
            LabelMatchOp::Neq => {
                let value_spur = interner.get(&matcher.value);
                let mut result = all_ids_posting_list(all_ids());
                if let Some(vs) = value_spur
                    && let Some(exclude) = label_index.get(&(name_spur, vs))
                {
                    for &id in exclude.ids() {
                        result.remove(id);
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
                if let Some(values) = label_values.get(&name_spur) {
                    for &vs in values {
                        let val_str = interner.resolve(&vs);
                        if re.is_match(val_str)
                            && let Some(pl) = label_index.get(&(name_spur, vs))
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
                let mut result = all_ids_posting_list(all_ids());
                if let Some(values) = label_values.get(&name_spur) {
                    for &vs in values {
                        let val_str = interner.resolve(&vs);
                        if re.is_match(val_str)
                            && let Some(exclude) = label_index.get(&(name_spur, vs))
                        {
                            for &id in exclude.ids() {
                                result.remove(id);
                            }
                        }
                    }
                }
                positive_lists.push(result);
            }
        }
    }

    let refs: Vec<&PostingList> = positive_lists.iter().collect();
    intersect(&refs)
}

fn all_ids_posting_list(mut ids: Vec<u64>) -> PostingList {
    ids.sort_unstable();
    let mut posting_list = PostingList::new();
    for id in ids {
        posting_list.insert(id);
    }
    posting_list
}

/// Run eviction on all stores based on config.
pub fn run_eviction(state: &AppState) {
    let retention = state.config.retention_duration();
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    let cutoff_ns = now_ns - retention.as_nanos() as i64;
    let cutoff_ms = cutoff_ns / 1_000_000;

    // Evict by time and max count
    {
        let mut logs = state.log_store.write();
        logs.evict_before(cutoff_ns);
        logs.evict_to_max(state.config.max_log_entries);
    }
    {
        let mut metrics = state.metric_store.write();
        metrics.evict_before(cutoff_ms);
        metrics.evict_to_max(state.config.max_series);
    }
    {
        let mut traces = state.trace_store.write();
        traces.evict_before(cutoff_ns);
        traces.evict_to_max(state.config.max_spans);
    }
}
