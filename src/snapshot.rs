//! Snapshot manager: serialize/deserialize stores to bincode.

use std::fs;
use std::path::Path;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::store::{LogStore, MetricStore, TraceStore};

/// Snapshot data containing all three stores.
#[derive(Serialize, Deserialize)]
pub struct Snapshot {
    pub log_store: LogStore,
    pub metric_store: MetricStore,
    pub trace_store: TraceStore,
}

/// Save a snapshot of all stores to disk (clones each store internally).
pub fn save_snapshot(
    log_store: &LogStore,
    metric_store: &MetricStore,
    trace_store: &TraceStore,
    dir: &Path,
) -> Result<()> {
    save_snapshot_owned(
        log_store.clone(),
        metric_store.clone(),
        trace_store.clone(),
        dir,
    )
}

/// Save a snapshot from already-owned (cloned) stores.
pub fn save_snapshot_owned(
    log_store: LogStore,
    metric_store: MetricStore,
    trace_store: TraceStore,
    dir: &Path,
) -> Result<()> {
    fs::create_dir_all(dir)?;

    let snapshot = Snapshot {
        log_store,
        metric_store,
        trace_store,
    };

    let bytes = bincode::serialize(&snapshot)?;
    let tmp_path = dir.join("obsidian.snap.tmp");
    let final_path = dir.join("obsidian.snap");

    fs::write(&tmp_path, &bytes)?;
    fs::rename(&tmp_path, &final_path)?;

    tracing::info!(
        "snapshot saved ({} bytes) to {}",
        bytes.len(),
        final_path.display()
    );
    Ok(())
}

/// Load a snapshot from disk.
pub fn load_snapshot(dir: &Path) -> Result<(LogStore, MetricStore, TraceStore)> {
    let path = dir.join("obsidian.snap");
    let bytes = fs::read(&path)?;
    let mut snapshot: Snapshot = bincode::deserialize(&bytes).map_err(|e| {
        anyhow::anyhow!(
            "snapshot deserialization failed (snapshot format may be incompatible with this build): {}",
            e
        )
    })?;
    snapshot.log_store.rebuild_stream_ids();
    snapshot.metric_store.rebuild_series_ids();
    tracing::info!("snapshot restored from {}", path.display());
    Ok((
        snapshot.log_store,
        snapshot.metric_store,
        snapshot.trace_store,
    ))
}

/// Clone all stores and save a snapshot. Acquires read locks briefly to clone,
/// then writes to disk without holding any locks.
pub fn save_from_state(state: &crate::store::AppState, dir: &std::path::Path) {
    let snapshot = Snapshot {
        log_store: state.log_store.read().clone(),
        metric_store: state.metric_store.read().clone(),
        trace_store: state.trace_store.read().clone(),
    };

    if let Err(e) = save_snapshot_owned(
        snapshot.log_store,
        snapshot.metric_store,
        snapshot.trace_store,
        dir,
    ) {
        tracing::error!("snapshot failed: {}", e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::log_store::LogEntry;
    use crate::store::metric_store::Sample;
    use tempfile::tempdir;

    #[test]
    fn test_snapshot_roundtrip() {
        let dir = tempdir().unwrap();

        let mut log_store = LogStore::new();
        log_store.ingest_stream(
            vec![("service".into(), "test".into())],
            vec![LogEntry {
                timestamp_ns: 1000,
                line: "hello".into(),
            }],
        );

        let mut metric_store = MetricStore::new();
        metric_store.ingest_samples(
            "cpu",
            vec![("host".into(), "a".into())],
            vec![Sample {
                timestamp_ms: 1000,
                value: 0.5,
            }],
        );

        let trace_store = TraceStore::new();

        save_snapshot(&log_store, &metric_store, &trace_store, dir.path()).unwrap();
        let (restored_logs, restored_metrics, _restored_traces) =
            load_snapshot(dir.path()).unwrap();

        assert_eq!(restored_logs.total_entries, 1);
        assert_eq!(restored_metrics.total_samples, 1);
    }
}
