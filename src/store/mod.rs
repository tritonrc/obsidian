//! Storage engine: in-memory stores for logs, metrics, and traces.
//!
//! All stores are behind `parking_lot::RwLock` within a shared `AppState`.

pub mod log_store;
pub mod metric_store;
pub mod posting_list;
pub mod trace_store;

use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;

use crate::config::Config;
pub use log_store::LogStore;
pub use metric_store::MetricStore;
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
        metrics.evict_to_max(state.config.max_samples);
    }
    {
        let mut traces = state.trace_store.write();
        traces.evict_before(cutoff_ns);
        traces.evict_to_max(state.config.max_spans);
    }
}
