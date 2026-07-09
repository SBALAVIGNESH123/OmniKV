//! Production-grade configuration for OmniKV.
//!
//! Loads config from a TOML file (`omnikv.toml` by default) and then applies
//! environment-variable overrides.  A strict **production mode** refuses to
//! start when the default JWT secret is present or TLS is unconfigured.
//!
//! # Quick start (development)
//! ```
//! let cfg = ServerConfig::load_dev();
//! ```
//!
//! # Quick start (production)
//! Set the required env vars and call:
//! ```no_run
//! let cfg = ServerConfig::load_production().expect("invalid production config");
//! ```

use std::path::PathBuf;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// The development-only JWT secret.  Production mode hard-rejects this value.
pub const DEV_JWT_SECRET: &str = "omnikv-dev-secret-change-in-prod";

/// Runtime mode selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServerMode {
    Development,
    Production,
}

impl Default for ServerMode {
    fn default() -> Self {
        Self::Development
    }
}

impl FromStr for ServerMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "production" | "prod" => Ok(Self::Production),
            "development" | "dev" => Ok(Self::Development),
            other => Err(format!("unknown mode: {other}")),
        }
    }
}

/// Log level.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl Default for LogLevel {
    fn default() -> Self {
        Self::Info
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        };
        write!(f, "{s}")
    }
}

/// TLS configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// PEM certificate file path.
    pub cert_path: PathBuf,
    /// PEM private key file path.
    pub key_path: PathBuf,
}

/// Storage tuning knobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Path to the manifest JSON file.
    pub manifest_path: PathBuf,
    /// Path to the WAL binary file.
    pub wal_path: PathBuf,
    /// Path to the backup directory.
    pub backup_dir: PathBuf,
    /// Maximum memtable size in bytes before an SSTable flush is triggered.
    pub max_memtable_bytes: u64,
    /// Maximum number of SSTables before compaction is triggered.
    pub max_sstables: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            manifest_path: PathBuf::from("manifest.json"),
            wal_path: PathBuf::from("wal.bin"),
            backup_dir: PathBuf::from("backups"),
            max_memtable_bytes: 64 * 1024 * 1024, // 64 MiB
            max_sstables: 8,
        }
    }
}

/// Complete server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Runtime mode.
    #[serde(default)]
    pub mode: ServerMode,
    /// HTTP/1.1 + HTTP/2 TLS listen address.
    pub http_addr: String,
    /// QUIC / HTTP3 listen address.
    pub quic_addr: String,
    /// PostgreSQL wire protocol listen address.
    pub pgwire_addr: String,
    /// Plain TCP command interface address.
    pub tcp_addr: String,
    /// JWT HMAC-SHA256 signing secret.
    pub jwt_secret: String,
    /// Optional TLS certificate and key.  Required in production unless
    /// `tls_insecure_skip` is set to `true`.
    pub tls: Option<TlsConfig>,
    /// Allow skipping TLS in production.  Must be `true` explicitly.
    #[serde(default)]
    pub tls_insecure_skip: bool,
    /// Log level.
    #[serde(default)]
    pub log_level: LogLevel,
    /// Storage configuration.
    #[serde(default)]
    pub storage: StorageConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            mode: ServerMode::Development,
            http_addr: "0.0.0.0:8443".to_string(),
            quic_addr: "0.0.0.0:4433".to_string(),
            pgwire_addr: "0.0.0.0:5433".to_string(),
            tcp_addr: "0.0.0.0:8080".to_string(),
            jwt_secret: DEV_JWT_SECRET.to_string(),
            tls: None,
            tls_insecure_skip: false,
            log_level: LogLevel::Info,
            storage: StorageConfig::default(),
        }
    }
}

/// Errors produced during configuration loading or validation.
#[derive(Debug)]
pub struct ConfigError(pub String);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OmniKV config error: {}", self.0)
    }
}

impl std::error::Error for ConfigError {}

impl ServerConfig {
    /// Load configuration for **development**.
    ///
    /// Attempts to read `omnikv.toml` from the current directory, falls back
    /// to built-in defaults, then applies env-var overrides.  Does **not**
    /// validate production constraints.
    pub fn load_dev() -> Self {
        let mut cfg = Self::from_file_or_default("omnikv.toml");
        cfg.apply_env();
        cfg.mode = ServerMode::Development;
        cfg
    }

    /// Load configuration for **production**.
    ///
    /// Same as `load_dev` but additionally calls `validate_production`, which
    /// returns an error if any production constraint is violated.
    pub fn load_production() -> Result<Self, ConfigError> {
        let mut cfg = Self::from_file_or_default("omnikv.toml");
        cfg.apply_env();
        cfg.mode = ServerMode::Production;
        cfg.validate_production()?;
        Ok(cfg)
    }

    /// Load from a specific TOML file path.
    pub fn load_from_file(path: &std::path::Path) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| ConfigError(format!("cannot read {}: {e}", path.display())))?;
        toml::from_str(&raw)
            .map_err(|e| ConfigError(format!("invalid TOML in {}: {e}", path.display())))
    }

    /// Parse from a TOML string (useful in tests).
    pub fn from_toml_str(s: &str) -> Result<Self, ConfigError> {
        toml::from_str(s).map_err(|e| ConfigError(format!("invalid TOML: {e}")))
    }

    /// Validate all production constraints.  Returns the first violation.
    pub fn validate_production(&self) -> Result<(), ConfigError> {
        // Reject the development JWT secret.
        if self.jwt_secret == DEV_JWT_SECRET {
            return Err(ConfigError(
                "OMNIKV_JWT_SECRET must be changed from the development default before \
                 running in production. Set it to a secret of at least 32 characters."
                    .to_string(),
            ));
        }
        // Require a sufficiently long secret.
        if self.jwt_secret.len() < 32 {
            return Err(ConfigError(format!(
                "OMNIKV_JWT_SECRET must be at least 32 characters (got {})",
                self.jwt_secret.len()
            )));
        }
        // Require TLS unless explicitly skipped.
        if !self.tls_insecure_skip {
            match &self.tls {
                None => {
                    return Err(ConfigError(
                        "TLS is required in production. Set OMNIKV_TLS_CERT_PATH and \
                         OMNIKV_TLS_KEY_PATH, or set OMNIKV_TLS_INSECURE_SKIP=true \
                         to opt out (not recommended)."
                            .to_string(),
                    ));
                }
                Some(tls) => {
                    if !tls.cert_path.exists() {
                        return Err(ConfigError(format!(
                            "TLS cert file not found: {}",
                            tls.cert_path.display()
                        )));
                    }
                    if !tls.key_path.exists() {
                        return Err(ConfigError(format!(
                            "TLS key file not found: {}",
                            tls.key_path.display()
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    fn from_file_or_default(path: &str) -> Self {
        match std::fs::read_to_string(path) {
            Ok(raw) => toml::from_str(&raw).unwrap_or_default(),
            Err(_) => Self::default(),
        }
    }

    /// Apply environment-variable overrides.
    ///
    /// | Variable | Field |
    /// |---|---|
    /// | `OMNIKV_MODE` | `mode` |
    /// | `OMNIKV_HTTP_ADDR` | `http_addr` |
    /// | `OMNIKV_QUIC_ADDR` | `quic_addr` |
    /// | `OMNIKV_PGWIRE_ADDR` | `pgwire_addr` |
    /// | `OMNIKV_TCP_ADDR` | `tcp_addr` |
    /// | `OMNIKV_JWT_SECRET` | `jwt_secret` |
    /// | `OMNIKV_TLS_CERT_PATH` | `tls.cert_path` |
    /// | `OMNIKV_TLS_KEY_PATH` | `tls.key_path` |
    /// | `OMNIKV_TLS_INSECURE_SKIP` | `tls_insecure_skip` |
    /// | `OMNIKV_LOG_LEVEL` | `log_level` |
    /// | `OMNIKV_MANIFEST_PATH` | `storage.manifest_path` |
    /// | `OMNIKV_WAL_PATH` | `storage.wal_path` |
    /// | `OMNIKV_BACKUP_DIR` | `storage.backup_dir` |
    pub fn apply_env(&mut self) {
        if let Ok(v) = std::env::var("OMNIKV_MODE") {
            if let Ok(m) = v.parse::<ServerMode>() {
                self.mode = m;
            }
        }
        if let Ok(v) = std::env::var("OMNIKV_HTTP_ADDR") {
            self.http_addr = v;
        }
        if let Ok(v) = std::env::var("OMNIKV_QUIC_ADDR") {
            self.quic_addr = v;
        }
        if let Ok(v) = std::env::var("OMNIKV_PGWIRE_ADDR") {
            self.pgwire_addr = v;
        }
        if let Ok(v) = std::env::var("OMNIKV_TCP_ADDR") {
            self.tcp_addr = v;
        }
        if let Ok(v) = std::env::var("OMNIKV_JWT_SECRET") {
            self.jwt_secret = v;
        }
        if let Ok(v) = std::env::var("OMNIKV_TLS_INSECURE_SKIP") {
            self.tls_insecure_skip = matches!(v.to_lowercase().as_str(), "true" | "1" | "yes");
        }
        // TLS cert + key — both must be set to activate TLS via env.
        let cert = std::env::var("OMNIKV_TLS_CERT_PATH").ok();
        let key = std::env::var("OMNIKV_TLS_KEY_PATH").ok();
        if let (Some(cert), Some(key)) = (cert, key) {
            self.tls = Some(TlsConfig {
                cert_path: PathBuf::from(cert),
                key_path: PathBuf::from(key),
            });
        }
        if let Ok(v) = std::env::var("OMNIKV_LOG_LEVEL") {
            self.log_level = match v.to_lowercase().as_str() {
                "trace" => LogLevel::Trace,
                "debug" => LogLevel::Debug,
                "info" => LogLevel::Info,
                "warn" | "warning" => LogLevel::Warn,
                "error" => LogLevel::Error,
                _ => self.log_level.clone(),
            };
        }
        if let Ok(v) = std::env::var("OMNIKV_MANIFEST_PATH") {
            self.storage.manifest_path = PathBuf::from(v);
        }
        if let Ok(v) = std::env::var("OMNIKV_WAL_PATH") {
            self.storage.wal_path = PathBuf::from(v);
        }
        if let Ok(v) = std::env::var("OMNIKV_BACKUP_DIR") {
            self.storage.backup_dir = PathBuf::from(v);
        }
    }
}
