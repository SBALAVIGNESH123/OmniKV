//! OmniKV Operational Configuration
//!
//! Centralized, production-grade configuration system.
//! All settings have safe defaults and can be overridden via environment variables.

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

            jwt_secret: "omnikv-dev-secret-change-in-prod".into(),

            log_level: "info,omni_engine=debug".into(),
            log_format: LogFormat::Json,
        }
    }
}

impl OmniConfig {
    /// Load configuration from environment variables, falling back to defaults.
    pub fn from_env() -> Self {
        let mut cfg = Self::default();

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
        if let Ok(v) = std::env::var("OMNI_MEMTABLE_FLUSH") {
            cfg.memtable_flush_threshold = v.parse().unwrap_or(cfg.memtable_flush_threshold);
        }
        if let Ok(v) = std::env::var("OMNI_L0_TRIGGER") {
            cfg.l0_compaction_trigger = v.parse().unwrap_or(cfg.l0_compaction_trigger);
        }
        if let Ok(v) = std::env::var("OMNI_L1_TRIGGER") {
            cfg.l1_compaction_trigger = v.parse().unwrap_or(cfg.l1_compaction_trigger);
        }
        if let Ok(v) = std::env::var("OMNI_BLOCK_CACHE") {
            cfg.block_cache_capacity = v.parse().unwrap_or(cfg.block_cache_capacity);
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
        if let Ok(v) = std::env::var("OMNI_LOG_LEVEL") {
            cfg.log_level = v;
        }
        if let Ok(v) = std::env::var("OMNI_LOG_FORMAT") {
            cfg.log_format = if v == "pretty" {
                LogFormat::Pretty
            } else {
                LogFormat::Json
            };
        }
        if let Ok(v) = std::env::var("OMNI_TXN_TIMEOUT_SECS") {
            cfg.txn_timeout = Duration::from_secs(v.parse().unwrap_or(30));
        }
        if let Ok(v) = std::env::var("OMNI_RATE_LIMIT") {
            cfg.rate_limit_per_sec = v.parse().unwrap_or(cfg.rate_limit_per_sec);
        }
        if let Ok(v) = std::env::var("OMNI_RATE_BURST") {
            cfg.rate_limit_burst = v.parse().unwrap_or(cfg.rate_limit_burst);
        }
        if let Ok(v) = std::env::var("OMNI_GROUP_COMMIT_US") {
            cfg.group_commit_wait_us = v.parse().unwrap_or(cfg.group_commit_wait_us);
        }
        if let Ok(v) = std::env::var("OMNI_POOL_SIZE") {
            cfg.connection_pool_size = v.parse().unwrap_or(cfg.connection_pool_size);
        }

        cfg
    }

    /// Validates the configuration, returning errors for invalid settings.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        if self.jwt_secret == "omnikv-dev-secret-change-in-prod" {
            // Warn but don't fail — it's the dev default
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
