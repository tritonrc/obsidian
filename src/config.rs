//! CLI argument parsing and configuration.

use clap::Parser;
use std::time::Duration;

/// Obsidian: Lightweight ephemeral observability engine.
#[derive(Parser, Debug, Clone)]
#[command(
    name = "obsidian",
    about = "Lightweight ephemeral observability engine"
)]
pub struct Config {
    /// Bind address for the server.
    #[arg(long, default_value = "127.0.0.1")]
    pub bind_address: String,

    /// Base port for all API surfaces.
    #[arg(long, default_value = "4320")]
    pub port: u16,

    /// Directory for snapshots.
    #[arg(long, default_value = ".obsidian/")]
    pub snapshot_dir: String,

    /// Auto-snapshot interval in seconds, 0 to disable.
    #[arg(long, default_value = "0")]
    pub snapshot_interval: u64,

    /// Max log entries before eviction.
    #[arg(long, default_value = "100000")]
    pub max_log_entries: usize,

    /// Max metric series before eviction.
    #[arg(long, default_value = "10000")]
    pub max_series: usize,

    /// Max trace spans before eviction.
    #[arg(long, default_value = "100000")]
    pub max_spans: usize,

    /// Max age before eviction, e.g. "30s", "5m", "1h", "2h".
    #[arg(long, default_value = "2h")]
    pub retention: String,

    /// Restore from snapshot on startup if available.
    #[arg(long, default_value = "false")]
    pub restore: bool,
}

impl Config {
    /// Parse the retention string into a Duration.
    pub fn retention_duration(&self) -> Duration {
        parse_duration(&self.retention).unwrap_or(Duration::from_secs(7200))
    }
}

/// Parse a duration string like "30s", "5m", "1h", "2h" into a Duration.
pub fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Try ms suffix first
    if let Some(num) = s.strip_suffix("ms") {
        let n: u64 = num.trim().parse().ok()?;
        return Some(Duration::from_millis(n));
    }

    let (num_str, multiplier) = if let Some(n) = s.strip_suffix('s') {
        (n, 1u64)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60u64)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 3600u64)
    } else if let Some(n) = s.strip_suffix('d') {
        (n, 86400u64)
    } else {
        // Try as raw seconds
        (s, 1u64)
    };

    let n: f64 = num_str.trim().parse().ok()?;
    Some(Duration::from_secs_f64(n * multiplier as f64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_duration("2h"), Some(Duration::from_secs(7200)));
        assert_eq!(parse_duration("500ms"), Some(Duration::from_millis(500)));
        assert_eq!(parse_duration(""), None);
    }
}
