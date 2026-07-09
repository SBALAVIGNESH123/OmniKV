//! OmniKV Configuration
//!
//! TOML-based configuration with sensible defaults for all settings.

use std::path::Path;

/// OmniKV server configuration with production-ready defaults.
#[derive(Debug, Clone)]
pub struct OmniConfig {
    /// Directory for data files (manifest, WAL, SSTables)
    pub data_dir: String,
    /// REST API port
    pub port: u16,
    /// PostgreSQL wire protocol port
    pub pg_port: u16,
    /// Maximum concurrent connections
    pub max_connections: usize,
    /// Query timeout in seconds (0 = no timeout)
    pub query_timeout_secs: u64,
    /// Slow query log threshold in milliseconds
    pub slow_query_threshold_ms: u64,
    /// MVCC GC runs every N compaction cycles
    pub gc_interval_compactions: u32,
    /// Maximum write batch size in bytes
    pub max_write_batch_bytes: usize,
    /// Maximum rows returned by a single query
    pub max_query_result_rows: usize,
    /// Enable encryption at rest
    pub enable_encryption: bool,
    /// Log level: "debug", "info", "warn", "error"
    pub log_level: String,
    /// Compaction check interval in milliseconds
    pub compaction_interval_ms: u64,
    /// Memtable flush threshold in bytes
    pub memtable_flush_threshold: usize,
}

impl Default for OmniConfig {
    fn default() -> Self {
        Self {
            data_dir: "./omnikv_data".into(),
            port: 8080,
            pg_port: 5433,
            max_connections: 256,
            query_timeout_secs: 30,
            slow_query_threshold_ms: 100,
            gc_interval_compactions: 5,
            max_write_batch_bytes: 64 * 1024 * 1024, // 64 MB
            max_query_result_rows: 1_000_000,
            enable_encryption: false,
            log_level: "info".into(),
            compaction_interval_ms: 500,
            memtable_flush_threshold: 4 * 1024 * 1024, // 4 MB
        }
    }
}

impl OmniConfig {
    /// Load configuration from a TOML file.
    /// Unknown keys are silently ignored. Missing keys use defaults.
    pub fn load_from_file(path: &str) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read config file '{}': {}", path, e))?;
        Self::parse_toml(&content)
    }

    /// Load from file if it exists, otherwise use defaults.
    pub fn load_or_default(path: &str) -> Self {
        if Path::new(path).exists() {
            Self::load_from_file(path).unwrap_or_else(|e| {
                eprintln!("[CONFIG] Warning: {}, using defaults", e);
                Self::default()
            })
        } else {
            Self::default()
        }
    }

    /// Parse a TOML string into config. Simple key=value parser.
    fn parse_toml(content: &str) -> Result<Self, String> {
        let mut config = Self::default();

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with('[') {
                continue;
            }

            if let Some((key, value)) = line.split_once('=') {
                let key = key.trim();
                let value = value.trim().trim_matches('"');

                match key {
                    "data_dir" => config.data_dir = value.into(),
                    "port" => config.port = value.parse().unwrap_or(config.port),
                    "pg_port" => config.pg_port = value.parse().unwrap_or(config.pg_port),
                    "max_connections" => {
                        config.max_connections = value.parse().unwrap_or(config.max_connections)
                    }
                    "query_timeout_secs" => {
                        config.query_timeout_secs =
                            value.parse().unwrap_or(config.query_timeout_secs)
                    }
                    "slow_query_threshold_ms" => {
                        config.slow_query_threshold_ms =
                            value.parse().unwrap_or(config.slow_query_threshold_ms)
                    }
                    "gc_interval_compactions" => {
                        config.gc_interval_compactions =
                            value.parse().unwrap_or(config.gc_interval_compactions)
                    }
                    "max_write_batch_bytes" => {
                        config.max_write_batch_bytes =
                            value.parse().unwrap_or(config.max_write_batch_bytes)
                    }
                    "max_query_result_rows" => {
                        config.max_query_result_rows =
                            value.parse().unwrap_or(config.max_query_result_rows)
                    }
                    "enable_encryption" => config.enable_encryption = value == "true",
                    "log_level" => config.log_level = value.into(),
                    "compaction_interval_ms" => {
                        config.compaction_interval_ms =
                            value.parse().unwrap_or(config.compaction_interval_ms)
                    }
                    "memtable_flush_threshold" => {
                        config.memtable_flush_threshold =
                            value.parse().unwrap_or(config.memtable_flush_threshold)
                    }
                    _ => {} // Ignore unknown keys
                }
            }
        }

        Ok(config)
    }

    /// Generate a sample configuration file content.
    pub fn sample_config() -> String {
        r#"# OmniKV Configuration
# See https://github.com/SBALAVIGNESH123/OmniKV for documentation.

# Storage
data_dir = "./omnikv_data"

# Networking
port = 8080
pg_port = 5433
max_connections = 256

# Query Limits
query_timeout_secs = 30
slow_query_threshold_ms = 100
max_query_result_rows = 1000000

# Storage Engine
compaction_interval_ms = 500
memtable_flush_threshold = 4194304
gc_interval_compactions = 5
max_write_batch_bytes = 67108864

# Security
enable_encryption = false

# Logging
log_level = "info"
"#
        .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = OmniConfig::default();
        assert_eq!(config.port, 8080);
        assert_eq!(config.pg_port, 5433);
        assert_eq!(config.query_timeout_secs, 30);
        assert_eq!(config.max_connections, 256);
    }

    #[test]
    fn test_parse_toml() {
        let toml = r#"
port = 9090
pg_port = 5434
data_dir = "/var/lib/omnikv"
query_timeout_secs = 60
enable_encryption = true
log_level = "debug"
"#;
        let config = OmniConfig::parse_toml(toml).unwrap();
        assert_eq!(config.port, 9090);
        assert_eq!(config.pg_port, 5434);
        assert_eq!(config.data_dir, "/var/lib/omnikv");
        assert_eq!(config.query_timeout_secs, 60);
        assert!(config.enable_encryption);
        assert_eq!(config.log_level, "debug");
    }

    #[test]
    fn test_parse_with_comments() {
        let toml = r#"
# This is a comment
[server]
port = 7070
# Another comment
max_connections = 512
"#;
        let config = OmniConfig::parse_toml(toml).unwrap();
        assert_eq!(config.port, 7070);
        assert_eq!(config.max_connections, 512);
    }

    #[test]
    fn test_sample_config() {
        let sample = OmniConfig::sample_config();
        assert!(sample.contains("port = 8080"));
        assert!(sample.contains("pg_port = 5433"));
    }
}
