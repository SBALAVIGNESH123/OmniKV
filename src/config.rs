//! OmniKV server configuration.
//!
//! Loads from `omnikv.toml` (optional) with environment variable overrides.
//!
//! # Quick start (development)
//! ```no_run
//! use omni_engine::config::ServerConfig;
//! let cfg = ServerConfig::load_dev();
//! ```
//!
//! # Quick start (production)
//! ```no_run
//! use omni_engine::config::ServerConfig;
//! let cfg = ServerConfig::load_production().expect("invalid production config");
//! ```

use serde::{Deserialize, Serialize};
use std::fmt;

/// The development JWT secret. Explicitly rejected in production mode.
pub const DEV_JWT_SECRET: &str = "dev-insecure-jwt-secret-do-not-use-in-production";

/// Server operating mode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ServerMode {
    #[default]
    Development,
    Production,
}

impl fmt::Display for ServerMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerMode::Development => write!(f, "development"),
            ServerMode::Production => write!(f, "production"),
        }
    }
}

/// Configuration error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ConfigError: {}", self.0)
    }
}

impl std::error::Error for ConfigError {}

/// Full server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default)]
    pub mode: ServerMode,
    #[serde(default = "default_http_addr")]
    pub http_addr: String,
    #[serde(default = "default_quic_addr")]
    pub quic_addr: String,
    #[serde(default = "default_pgwire_addr")]
    pub pgwire_addr: String,
    #[serde(default = "default_tcp_addr")]
    pub tcp_addr: String,
    #[serde(default = "default_jwt_secret")]
    pub jwt_secret: String,
    #[serde(default)]
    pub tls_cert_path: Option<String>,
    #[serde(default)]
    pub tls_key_path: Option<String>,
    #[serde(default)]
    pub tls_insecure_skip: bool,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_manifest_path")]
    pub manifest_path: String,
    #[serde(default = "default_wal_path")]
    pub wal_path: String,
    #[serde(default = "default_backup_dir")]
    pub backup_dir: String,
    #[serde(default = "default_max_open_files")]
    pub max_open_files: u32,
    #[serde(default = "default_write_buffer_mb")]
    pub write_buffer_mb: u32,
    #[serde(default = "default_compaction_workers")]
    pub compaction_workers: u32,
}

fn default_http_addr() -> String { "127.0.0.1:7070".to_string() }
fn default_quic_addr() -> String { "127.0.0.1:7071".to_string() }
fn default_pgwire_addr() -> String { "127.0.0.1:5432".to_string() }
fn default_tcp_addr() -> String { "127.0.0.1:7072".to_string() }
fn default_jwt_secret() -> String { DEV_JWT_SECRET.to_string() }
fn default_log_level() -> String { "info".to_string() }
fn default_manifest_path() -> String { "./data/manifest.json".to_string() }
fn default_wal_path() -> String { "./data/wal/wal.bin".to_string() }
fn default_backup_dir() -> String { "./data/backups".to_string() }
fn default_max_open_files() -> u32 { 512 }
fn default_write_buffer_mb() -> u32 { 64 }
fn default_compaction_workers() -> u32 { 2 }

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            mode: ServerMode::Development,
            http_addr: default_http_addr(),
            quic_addr: default_quic_addr(),
            pgwire_addr: default_pgwire_addr(),
            tcp_addr: default_tcp_addr(),
            jwt_secret: default_jwt_secret(),
            tls_cert_path: None,
            tls_key_path: None,
            tls_insecure_skip: false,
            log_level: default_log_level(),
            manifest_path: default_manifest_path(),
            wal_path: default_wal_path(),
            backup_dir: default_backup_dir(),
            max_open_files: default_max_open_files(),
            write_buffer_mb: default_write_buffer_mb(),
            compaction_workers: default_compaction_workers(),
        }
    }
}

impl ServerConfig {
    /// Load configuration for development mode.
    pub fn load_dev() -> Self {
        let mut cfg = Self::from_file_or_default("omnikv.toml");
        cfg.apply_env();
        cfg.mode = ServerMode::Development;
        cfg
    }

    /// Load configuration for production mode.
    /// Returns an error if production constraints are violated.
    pub fn load_production() -> Result<Self, ConfigError> {
        let mut cfg = Self::from_file_or_default("omnikv.toml");
        cfg.apply_env();
        cfg.mode = ServerMode::Production;
        cfg.validate_production()?;
        Ok(cfg)
    }

    /// Validate production constraints. Returns `Err` on first violation.
    pub fn validate_production(&self) -> Result<(), ConfigError> {
        if self.jwt_secret == DEV_JWT_SECRET {
            return Err(ConfigError(
                "OMNIKV_JWT_SECRET must not be the development default in production".to_string(),
            ));
        }
        if self.jwt_secret.len() < 32 {
            return Err(ConfigError(
                "OMNIKV_JWT_SECRET must be at least 32 characters in production".to_string(),
            ));
        }
        if !self.tls_insecure_skip {
            match (&self.tls_cert_path, &self.tls_key_path) {
                (Some(cert), Some(key)) => {
                    if !std::path::Path::new(cert).exists() {
                        return Err(ConfigError(format!(
                            "TLS cert file not found: {cert}"
                        )));
                    }
                    if !std::path::Path::new(key).exists() {
                        return Err(ConfigError(format!(
                            "TLS key file not found: {key}"
                        )));
                    }
                }
                _ => {
                    return Err(ConfigError(
                        "Production requires OMNIKV_TLS_CERT_PATH + OMNIKV_TLS_KEY_PATH \
                         or OMNIKV_TLS_INSECURE_SKIP=true"
                            .to_string(),
                    ));
                }
            }
        } else {
            eprintln!(
                "WARNING: OMNIKV_TLS_INSECURE_SKIP=true — TLS is disabled. \
                 Do not use in production without a TLS terminator."
            );
        }
        Ok(())
    }

    /// Apply environment variable overrides.
    pub fn apply_env(&mut self) {
        macro_rules! env_str {
            ($var:expr, $field:expr) => {
                if let Ok(v) = std::env::var($var) {
                    *$field = v;
                }
            };
        }
        env_str!("OMNIKV_HTTP_ADDR", &mut self.http_addr);
        env_str!("OMNIKV_QUIC_ADDR", &mut self.quic_addr);
        env_str!("OMNIKV_PGWIRE_ADDR", &mut self.pgwire_addr);
        env_str!("OMNIKV_TCP_ADDR", &mut self.tcp_addr);
        env_str!("OMNIKV_JWT_SECRET", &mut self.jwt_secret);
        env_str!("OMNIKV_LOG_LEVEL", &mut self.log_level);
        env_str!("OMNIKV_MANIFEST_PATH", &mut self.manifest_path);
        env_str!("OMNIKV_WAL_PATH", &mut self.wal_path);
        env_str!("OMNIKV_BACKUP_DIR", &mut self.backup_dir);
        if let Ok(v) = std::env::var("OMNIKV_TLS_CERT_PATH") {
            self.tls_cert_path = Some(v);
        }
        if let Ok(v) = std::env::var("OMNIKV_TLS_KEY_PATH") {
            self.tls_key_path = Some(v);
        }
        if let Ok(v) = std::env::var("OMNIKV_TLS_INSECURE_SKIP") {
            self.tls_insecure_skip = v.eq_ignore_ascii_case("true") || v == "1";
        }
        if let Ok(v) = std::env::var("OMNIKV_MODE") {
            self.mode = match v.to_lowercase().as_str() {
                "production" => ServerMode::Production,
                _ => ServerMode::Development,
            };
        }
        if let Ok(v) = std::env::var("OMNIKV_MAX_OPEN_FILES") {
            if let Ok(n) = v.parse() {
                self.max_open_files = n;
            }
        }
        if let Ok(v) = std::env::var("OMNIKV_WRITE_BUFFER_MB") {
            if let Ok(n) = v.parse() {
                self.write_buffer_mb = n;
            }
        }
        if let Ok(v) = std::env::var("OMNIKV_COMPACTION_WORKERS") {
            if let Ok(n) = v.parse() {
                self.compaction_workers = n;
            }
        }
    }

    fn from_file_or_default(path: &str) -> Self {
        match std::fs::read_to_string(path) {
            Ok(raw) => match toml::from_str::<Self>(&raw) {
                Ok(cfg) => cfg,
                Err(e) => {
                    eprintln!("WARNING: failed to parse {path}: {e} — using defaults");
                    Self::default()
                }
            },
            Err(_) => Self::default(),
        }
    }
}
