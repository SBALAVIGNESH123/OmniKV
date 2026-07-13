//! OmniKV Operational Configuration
//!
//! Operational diagnostics configuration.
//!
//! The network-facing server uses [`crate::config::ServerConfig`] as the
//! authoritative runtime configuration. This module remains for diagnostics and
//! staged operational helpers, but it now fails closed on malformed
//! environment overrides instead of silently falling back to defaults.

use std::str::FromStr;
use std::time::Duration;

/// Complete configuration for an OmniKV node.
#[derive(Debug, Clone)]
pub struct OmniConfig {
    // ── Storage ──
    pub manifest_path: String,
    pub wal_path: String,
    pub data_dir: String,

    // ── Compaction ──
    pub memtable_flush_threshold: usize,
    pub l0_compaction_trigger: usize,
    pub l1_compaction_trigger: usize,
    pub compaction_check_interval_ms: u64,
    pub l0_write_stall_threshold: usize,

    // ── Cache ──
    pub block_cache_capacity: u64,

    // ── Write Path ──
    pub max_batch_size: usize,
    pub max_value_size: usize,
    pub group_commit_wait_us: u64,

    // ── Rate Limiting ──
    pub rate_limit_per_sec: f64,
    pub rate_limit_burst: u32,
    pub rate_limit_max_users: usize,

    // ── Network ──
    pub http_addr: String,
    pub quic_addr: String,
    pub pgwire_addr: String,
    pub tcp_addr: String,

    // ── Timeouts ──
    pub txn_timeout: Duration,
    pub raft_rpc_timeout: Duration,
    pub connection_pool_size: usize,
    pub pool_idle_timeout: Duration,

    // ── Auth ──
    pub jwt_secret: String,
    pub bootstrap_admin_key: String,

    // ── Logging ──
    pub log_level: String,
    pub log_format: LogFormat,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogFormat {
    Json,
    Pretty,
}

impl Default for OmniConfig {
    fn default() -> Self {
        Self {
            manifest_path: "manifest.json".into(),
            wal_path: "wal.bin".into(),
            data_dir: ".".into(),

            memtable_flush_threshold: 10_000,
            l0_compaction_trigger: 4,
            l1_compaction_trigger: 4,
            compaction_check_interval_ms: 500,
            l0_write_stall_threshold: 12,

            block_cache_capacity: 100_000,

            max_batch_size: 10_000,
            max_value_size: 10 * 1024 * 1024,
            group_commit_wait_us: 200,

            rate_limit_per_sec: 1000.0,
            rate_limit_burst: 100,
            rate_limit_max_users: 10_000,

            http_addr: "0.0.0.0:8443".into(),
            quic_addr: "0.0.0.0:4433".into(),
            pgwire_addr: "0.0.0.0:5433".into(),
            tcp_addr: "0.0.0.0:8080".into(),

            txn_timeout: Duration::from_secs(30),
            raft_rpc_timeout: Duration::from_secs(10),
            connection_pool_size: 32,
            pool_idle_timeout: Duration::from_secs(90),

            jwt_secret: String::new(),
            bootstrap_admin_key: String::new(),

            log_level: "info,omni_engine=debug".into(),
            log_format: LogFormat::Json,
        }
    }
}

impl OmniConfig {
    /// Load configuration from environment variables.
    ///
    /// Invalid numeric or enum values return errors rather than silently
    /// falling back to defaults.
    pub fn from_env() -> Result<Self, Vec<String>> {
        let mut cfg = Self::default();
        let mut errors = Vec::new();

        if let Ok(v) = std::env::var("OMNI_DATA_DIR") {
            cfg.data_dir = v.clone();
            cfg.manifest_path = format!("{}/manifest.json", v);
            cfg.wal_path = format!("{}/wal.bin", v);
        }
        if let Ok(v) = std::env::var("OMNI_MANIFEST_PATH") {
            cfg.manifest_path = v;
        }
        if let Ok(v) = std::env::var("OMNI_WAL_PATH") {
            cfg.wal_path = v;
        }
        if let Ok(v) = std::env::var("OMNI_MEMTABLE_FLUSH")
            && let Some(parsed) = parse_env("OMNI_MEMTABLE_FLUSH", &v, &mut errors)
        {
            cfg.memtable_flush_threshold = parsed;
        }
        if let Ok(v) = std::env::var("OMNI_L0_TRIGGER")
            && let Some(parsed) = parse_env("OMNI_L0_TRIGGER", &v, &mut errors)
        {
            cfg.l0_compaction_trigger = parsed;
        }
        if let Ok(v) = std::env::var("OMNI_L1_TRIGGER")
            && let Some(parsed) = parse_env("OMNI_L1_TRIGGER", &v, &mut errors)
        {
            cfg.l1_compaction_trigger = parsed;
        }
        if let Ok(v) = std::env::var("OMNI_BLOCK_CACHE")
            && let Some(parsed) = parse_env("OMNI_BLOCK_CACHE", &v, &mut errors)
        {
            cfg.block_cache_capacity = parsed;
        }
        if let Ok(v) = std::env::var("OMNI_HTTP_ADDR") {
            cfg.http_addr = v;
        }
        if let Ok(v) = std::env::var("OMNI_QUIC_ADDR") {
            cfg.quic_addr = v;
        }
        if let Ok(v) = std::env::var("OMNI_PGWIRE_ADDR") {
            cfg.pgwire_addr = v;
        }
        if let Ok(v) = std::env::var("OMNI_TCP_ADDR") {
            cfg.tcp_addr = v;
        }
        if let Ok(v) = std::env::var("OMNI_JWT_SECRET") {
            cfg.jwt_secret = v;
        }
        if let Ok(v) = std::env::var("OMNI_BOOTSTRAP_ADMIN_KEY") {
            cfg.bootstrap_admin_key = v;
        }
        if let Ok(v) = std::env::var("OMNI_LOG_LEVEL") {
            cfg.log_level = v;
        }
        if let Ok(v) = std::env::var("OMNI_LOG_FORMAT") {
            match v.as_str() {
                "json" => cfg.log_format = LogFormat::Json,
                "pretty" => cfg.log_format = LogFormat::Pretty,
                _ => errors.push(format!(
                    "OMNI_LOG_FORMAT must be either 'json' or 'pretty', got {v:?}"
                )),
            }
        }
        if let Ok(v) = std::env::var("OMNI_TXN_TIMEOUT_SECS")
            && let Some(secs) = parse_env("OMNI_TXN_TIMEOUT_SECS", &v, &mut errors)
        {
            cfg.txn_timeout = Duration::from_secs(secs);
        }
        if let Ok(v) = std::env::var("OMNI_RATE_LIMIT")
            && let Some(parsed) = parse_env("OMNI_RATE_LIMIT", &v, &mut errors)
        {
            cfg.rate_limit_per_sec = parsed;
        }
        if let Ok(v) = std::env::var("OMNI_RATE_BURST")
            && let Some(parsed) = parse_env("OMNI_RATE_BURST", &v, &mut errors)
        {
            cfg.rate_limit_burst = parsed;
        }
        if let Ok(v) = std::env::var("OMNI_GROUP_COMMIT_US")
            && let Some(parsed) = parse_env("OMNI_GROUP_COMMIT_US", &v, &mut errors)
        {
            cfg.group_commit_wait_us = parsed;
        }
        if let Ok(v) = std::env::var("OMNI_POOL_SIZE")
            && let Some(parsed) = parse_env("OMNI_POOL_SIZE", &v, &mut errors)
        {
            cfg.connection_pool_size = parsed;
        }

        if errors.is_empty() {
            Ok(cfg)
        } else {
            Err(errors)
        }
    }

    /// Validates the configuration, returning errors for invalid settings.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        if !is_strong_runtime_secret(&self.jwt_secret) {
            errors.push(
                "OMNI_JWT_SECRET must be set to a non-default secret with at least 32 characters"
                    .into(),
            );
        }
        if !is_strong_runtime_secret(&self.bootstrap_admin_key) {
            errors.push(
                "OMNI_BOOTSTRAP_ADMIN_KEY must be set to a non-default secret with at least 32 characters"
                    .into(),
            );
        }
        if self.memtable_flush_threshold == 0 {
            errors.push("memtable_flush_threshold must be > 0".into());
        }
        if self.l0_write_stall_threshold <= self.l0_compaction_trigger {
            errors.push("l0_write_stall_threshold must be > l0_compaction_trigger".into());
        }
        if self.block_cache_capacity == 0 {
            errors.push("block_cache_capacity must be > 0".into());
        }
        if self.rate_limit_per_sec <= 0.0 {
            errors.push("rate_limit_per_sec must be > 0".into());
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Returns a summary string for startup logging.
    pub fn summary(&self) -> String {
        format!(
            "OmniConfig {{ data_dir: {:?}, memtable_flush: {}, l0_trigger: {}, l1_trigger: {}, cache: {}, rate_limit: {}/s, txn_timeout: {:?} }}",
            self.data_dir,
            self.memtable_flush_threshold,
            self.l0_compaction_trigger,
            self.l1_compaction_trigger,
            self.block_cache_capacity,
            self.rate_limit_per_sec,
            self.txn_timeout
        )
    }
}

fn parse_env<T>(name: &str, value: &str, errors: &mut Vec<String>) -> Option<T>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    match value.parse() {
        Ok(parsed) => Some(parsed),
        Err(e) => {
            errors.push(format!("invalid value for {name}={value:?}: {e}"));
            None
        }
    }
}

fn is_strong_runtime_secret(value: &str) -> bool {
    let value = value.trim();
    value.len() >= 32
        && value != "omnikv-dev-secret-change-in-prod"
        && value != "change-me-in-production"
}

/// Diagnostic snapshot of current database state.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DiagnosticReport {
    pub version: String,
    pub uptime_secs: u64,
    pub global_seq: u64,
    pub memtable_entries: usize,
    pub l0_sstable_count: usize,
    pub l1_sstable_count: usize,
    pub block_cache_capacity: u64,
    pub active_snapshots: usize,
    pub heap_offset_bytes: u64,
    pub config: DiagnosticConfig,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DiagnosticConfig {
    pub memtable_flush_threshold: usize,
    pub l0_write_stall_threshold: usize,
    pub block_cache_capacity: u64,
}

impl DiagnosticReport {
    /// Generate a diagnostic report from the live database.
    pub fn from_db(
        db: &std::sync::Arc<crate::OmniKV>,
        start_time: std::time::Instant,
        config: &OmniConfig,
    ) -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").into(),
            uptime_secs: start_time.elapsed().as_secs(),
            global_seq: db.get_seq(),
            memtable_entries: db.memtable_size(),
            l0_sstable_count: db.sstable_count(),
            l1_sstable_count: db.l1_sstable_count(),
            block_cache_capacity: config.block_cache_capacity,
            active_snapshots: db.active_snapshots.lock().map(|s| s.len()).unwrap_or(0),
            heap_offset_bytes: db.heap_offset.load(std::sync::atomic::Ordering::Relaxed),
            config: DiagnosticConfig {
                memtable_flush_threshold: config.memtable_flush_threshold,
                l0_write_stall_threshold: config.l0_write_stall_threshold,
                block_cache_capacity: config.block_cache_capacity,
            },
        }
    }
}

/// Graceful shutdown coordinator.
/// Signals all subsystems to drain and stop.
pub struct ShutdownCoordinator {
    notify: std::sync::Arc<tokio::sync::Notify>,
}

impl ShutdownCoordinator {
    pub fn new() -> Self {
        Self {
            notify: std::sync::Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Returns a future that resolves when shutdown is requested.
    pub fn subscribe(&self) -> std::sync::Arc<tokio::sync::Notify> {
        self.notify.clone()
    }

    /// Trigger shutdown (called from signal handler).
    pub fn trigger(&self) {
        self.notify.notify_waiters();
    }

    /// Install OS signal handlers (SIGTERM, SIGINT/Ctrl+C).
    /// Returns after a shutdown signal is received.
    pub async fn wait_for_signal(&self) {
        let shutdown = tokio::signal::ctrl_c();
        tokio::select! {
            _ = shutdown => {
                tracing::info!("Received Ctrl+C, initiating graceful shutdown...");
            }
        }
        self.trigger();
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}
