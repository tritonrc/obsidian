//! Obsidian: Lightweight ephemeral observability engine.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use parking_lot::RwLock;
use tokio::net::TcpListener;

use obsidian::config::Config;
use obsidian::snapshot;
use obsidian::store::{self, AppState, LogStore, MetricStore, SharedState, TraceStore};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = Config::parse();

    // Initialize stores
    let (log_store, metric_store, trace_store) = if config.restore {
        let snap_dir = PathBuf::from(&config.snapshot_dir);
        match snapshot::load_snapshot(&snap_dir) {
            Ok((ls, ms, ts)) => {
                tracing::info!("restored from snapshot");
                (ls, ms, ts)
            }
            Err(e) => {
                tracing::warn!("failed to restore snapshot: {}, starting fresh", e);
                (LogStore::new(), MetricStore::new(), TraceStore::new())
            }
        }
    } else {
        (LogStore::new(), MetricStore::new(), TraceStore::new())
    };

    let state: SharedState = Arc::new(AppState {
        log_store: RwLock::new(log_store),
        metric_store: RwLock::new(metric_store),
        trace_store: RwLock::new(trace_store),
        config: config.clone(),
        start_time: Instant::now(),
    });

    // Start eviction background task
    let eviction_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            let state = eviction_state.clone();
            tokio::task::spawn_blocking(move || {
                store::run_eviction(&state);
            })
            .await
            .ok();
        }
    });

    // Start snapshot timer if configured
    if config.snapshot_interval > 0 {
        let snap_state = state.clone();
        let snap_dir = PathBuf::from(&config.snapshot_dir);
        let snap_interval = config.snapshot_interval;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(snap_interval));
            loop {
                interval.tick().await;
                let ls = snapshot::clone_log_store(&snap_state.log_store.read());
                let ms = snapshot::clone_metric_store(&snap_state.metric_store.read());
                let ts = snapshot::clone_trace_store(&snap_state.trace_store.read());
                match (ls, ms, ts) {
                    (Ok(ls), Ok(ms), Ok(ts)) => {
                        if let Err(e) = snapshot::save_snapshot_owned(ls, ms, ts, &snap_dir) {
                            tracing::error!("snapshot failed: {}", e);
                        }
                    }
                    _ => tracing::error!("snapshot failed: could not clone stores"),
                }
            }
        });
    }

    // Register SIGUSR1 handler for on-demand snapshots (Unix only)
    #[cfg(unix)]
    {
        let sig_state = state.clone();
        let sig_dir = PathBuf::from(&config.snapshot_dir);
        tokio::spawn(async move {
            let mut signal =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())
                    .expect("failed to register SIGUSR1 handler");
            loop {
                signal.recv().await;
                tracing::info!("SIGUSR1 received, saving snapshot");
                let ls = snapshot::clone_log_store(&sig_state.log_store.read());
                let ms = snapshot::clone_metric_store(&sig_state.metric_store.read());
                let ts = snapshot::clone_trace_store(&sig_state.trace_store.read());
                match (ls, ms, ts) {
                    (Ok(ls), Ok(ms), Ok(ts)) => {
                        if let Err(e) = snapshot::save_snapshot_owned(ls, ms, ts, &sig_dir) {
                            tracing::error!("snapshot failed: {}", e);
                        }
                    }
                    _ => tracing::error!("snapshot failed: could not clone stores"),
                }
            }
        });
    }

    // Build router and start server
    let app = obsidian::server::build_router(state);
    let addr = format!("0.0.0.0:{}", config.port);
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!("obsidian listening on {}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}
