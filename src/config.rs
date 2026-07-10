//! OmniKV server configuration.
//!
//! Loads settings from an optional `omnikv.toml` file, then applies
//! environment-variable overrides. Two convenience constructors are provided:
//! [`ServerConfig::load_dev`] (always succeeds) and
//! [`ServerConfig::load_production`] (fails fast when security constraints
//! are not met).
//!
//! # Quick start (development)
//! ```
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

/// The well-known development JWT secret. Rejected in production mode.
pub const DEV_JWT_SECRET: &str = "omnikv-dev-secret-do-not-use-in-production";

/// Runtime operation mode.
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

impl std::str::FromStr for ServerMode {
    type Err = ConfigError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "development" | "dev" => Ok(ServerMode::Development),
            "production" | "prod" => Ok(ServerMode::Production),
            other => Err(ConfigError::InvalidValue {
                key: "OMNIKV_MODE".into(),
                value: other.into(),
                reason: "must be development or production".into(),
            }),
        }
    }
}

/// Storage-layer tuning parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub data_dir: String,
    pub wal_path: String,
    pub manifest_path: String,
    pub backup_dir: String,
    pub log_dir: String,
    pub max_open_files: u32,
    pub write_buffer_mb: u32,
    pub compaction_workers: u32,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "./data".into(),
            wal_path: "./data/wal/wal.bin".into(),
            manifest_path: "./data/manifest.json".into(),
            backup_dir: "./data/backups".into(),
            log_dir: "./logs".into(),
            max_open_files: 512,
            write_buffer_mb: 64,
            compaction_workers: 2,
        }
    }
}

/// TLS configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TlsConfig {
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    /// When `true` TLS is skipped. Must be set explicitly; never defaulted.
    pub insecure_skip: bool,
}

/// Full server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub mode: ServerMode,
    pub http_addr: String,
    pub quic_addr: String,
    pub pgwire_addr: String,
    pub tcp_addr: String,
    pub jwt_secret: String,
    pub log_level: String,
    pub storage: StorageConfig,
    pub tls: TlsConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            mode: ServerMode::Development,
            http_addr: "127.0.0.1:7070".into(),
            quic_addr: "127.0.0.1:7443".into(),
            pgwire_addr: "127.0.0.1:5432".into(),
            tcp_addr: "127.0.0.1:7071".into(),
            jwt_secret: DEV_JWT_SECRET.into(),
            log_level: "info".into(),
            storage: StorageConfig::default(),
            tls: TlsConfig::default(),
        }
    }
}

/// Configuration error variants.
#[derive(Debug)]
pub enum ConfigError {
    ProductionDevSecret,
    JwtSecretTooShort { len: usize },
    TlsNotConfigured,
    TlsCertMissing { path: String },
    TlsKeyMissing { path: String },
    InvalidValue { key: String, value: String, reason: String },
    ParseError(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::ProductionDevSecret =>
                write!(f, "OMNIKV_JWT_SECRET must not be the development default in production mode"),
            ConfigError::JwtSecretTooShort { len } =>
                write!(f, "OMNIKV_JWT_SECRET is {len} chars; production requires >= 32"),
            ConfigError::TlsNotConfigured =>
                write!(f, "TLS is required in production mode; set OMNIKV_TLS_CERT_PATH + OMNIKV_TLS_KEY_PATH or set OMNIKV_TLS_INSECURE_SKIP=true"),
            ConfigError::TlsCertMissing { path } =>
                write!(f, "TLS cert file not found: {path}"),
            ConfigError::TlsKeyMissing { path } =>
                write!(f, "TLS key file not found: {path}"),
            ConfigError::InvalidValue { key, value, reason } =>
                write!(f, "invalid value for {key}={value}: {reason}"),
            ConfigError::ParseError(msg) =>
                write!(f, "config parse error: {msg}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl ServerConfig {
    /// Load dev configuration. Always succeeds. Never validates secrets.
    pub fn load_dev() -> Self {
        let mut cfg = Self::from_file_or_default("omnikv.toml");
        cfg.apply_env();
        // Honour OMNIKV_MODE if set; otherwise default to Development.
        if cfg.mode != ServerMode::Production {
            cfg.mode = ServerMode::Development;
        }
        cfg
    }

    /// Load production configuration.
    ///
    /// Applies env overrides, then enforces all production constraints.
    /// Returns `Err` with a clear message if any constraint is violated.
    pub fn load_production() -> Result<Self, ConfigError> {
        let mut cfg = Self::from_file_or_default("omnikv.toml");
        cfg.apply_env();
        cfg.mode = ServerMode::Production;
        cfg.validate_production()?;
        Ok(cfg)
    }

    /// Validate production constraints. Call after `apply_env`.
    pub fn validate_production(&self) -> Result<(), ConfigError> {
        if self.jwt_secret == DEV_JWT_SECRET {
            return Err(ConfigError::ProductionDevSecret);
        }
        if self.jwt_secret.len() < 32 {
            return Err(ConfigError::JwtSecretTooShort { len: self.jwt_secret.len() });
        }
        if !self.tls.insecure_skip {
            match (&self.tls.cert_path, &self.tls.key_path) {
                (None, _) | (_, None) => return Err(ConfigError::TlsNotConfigured),
                (Some(cert), Some(key)) => {
                    if !std::path::Path::new(cert).exists() {
                        return Err(ConfigError::TlsCertMissing { path: cert.clone() });
                    }
                    if !std::path::Path::new(key).exists() {
                        return Err(ConfigError::TlsKeyMissing { path: key.clone() });
                    }
                }
            }
        } else {
            eprintln!("WARNING: TLS is disabled via OMNIKV_TLS_INSECURE_SKIP=true. This is not recommended for production.");
        }
        Ok(())
    }

    /// Apply environment-variable overrides.
    pub fn apply_env(&mut self) {
        if let Ok(v) = std::env::var("OMNIKV_MODE") {
            if let Ok(m) = v.parse::<ServerMode>() {
                self.mode = m;
            }
        }
        if let Ok(v) = std::env::var("OMNIKV_HTTP_ADDR") { self.http_addr = v; }
        if let Ok(v) = std::env::var("OMNIKV_QUIC_ADDR") { self.quic_addr = v; }
        if let Ok(v) = std::env::var("OMNIKV_PGWIRE_ADDR") { self.pgwire_addr = v; }
        if let Ok(v) = std::env::var("OMNIKV_TCP_ADDR") { self.tcp_addr = v; }
        if let Ok(v) = std::env::var("OMNIKV_JWT_SECRET") { self.jwt_secret = v; }
        if let Ok(v) = std::env::var("OMNIKV_LOG_LEVEL") { self.log_level = v; }
        if let Ok(v) = std::env::var("OMNIKV_DATA_DIR") { self.storage.data_dir = v; }
        if let Ok(v) = std::env::var("OMNIKV_WAL_PATH") { self.storage.wal_path = v; }
        if let Ok(v) = std::env::var("OMNIKV_MANIFEST_PATH") { self.storage.manifest_path = v; }
        if let Ok(v) = std::env::var("OMNIKV_BACKUP_DIR") { self.storage.backup_dir = v; }
        if let Ok(v) = std::env::var("OMNIKV_LOG_DIR") { self.storage.log_dir = v; }
        if let Ok(v) = std::env::var("OMNIKV_MAX_OPEN_FILES") {
            if let Ok(n) = v.parse() { self.storage.max_open_files = n; }
        }
        if let Ok(v) = std::env::var("OMNIKV_WRITE_BUFFER_MB") {
            if let Ok(n) = v.parse() { self.storage.write_buffer_mb = n; }
        }
        if let Ok(v) = std::env::var("OMNIKV_COMPACTION_WORKERS") {
            if let Ok(n) = v.parse() { self.storage.compaction_workers = n; }
        }
        if let Ok(v) = std::env::var("OMNIKV_TLS_CERT_PATH") { self.tls.cert_path = Some(v); }
        if let Ok(v) = std::env::var("OMNIKV_TLS_KEY_PATH") { self.tls.key_path = Some(v); }
        if let Ok(v) = std::env::var("OMNIKV_TLS_INSECURE_SKIP") {
            self.tls.insecure_skip = v.to_lowercase() == "true";
        }
    }

    /// Load from a TOML file, falling back to defaults.
    ///
    /// If the file exists but is malformed, logs the error and uses defaults.
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
