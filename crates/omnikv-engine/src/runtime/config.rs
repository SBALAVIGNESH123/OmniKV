use std::{fmt, path::Path};

use serde::{Deserialize, Serialize};

pub const DEV_JWT_SECRET: &str = "dev-secret-do-not-use-in-production";
pub const DEV_BOOTSTRAP_ADMIN_KEY: &str = "dev-bootstrap-admin-key-do-not-use-in-production";

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
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "production" | "prod" => Ok(ServerMode::Production),
            "development" | "dev" => Ok(ServerMode::Development),
            other => Err(format!("unknown mode: {other}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub manifest_path: String,
    pub wal_path: String,
    pub backup_dir: String,
    pub max_open_files: u32,
    pub write_buffer_mb: u32,
    pub compaction_workers: u32,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            manifest_path: "manifest.json".into(),
            wal_path: "wal.bin".into(),
            backup_dir: "./data/backups".into(),
            max_open_files: 512,
            write_buffer_mb: 64,
            compaction_workers: 2,
        }
    }
}

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
    #[serde(default = "default_bootstrap_admin_key")]
    pub bootstrap_admin_key: String,
    #[serde(default = "default_rate_limit_per_sec")]
    pub rate_limit_per_sec: f64,
    #[serde(default = "default_rate_limit_burst")]
    pub rate_limit_burst: u32,
    #[serde(default = "default_rate_limit_max_users")]
    pub rate_limit_max_users: usize,
    #[serde(default)]
    pub tls_cert_path: Option<String>,
    #[serde(default)]
    pub tls_key_path: Option<String>,
    #[serde(default)]
    pub tls_insecure_skip: bool,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default)]
    pub storage: StorageConfig,
}

fn default_http_addr() -> String {
    "127.0.0.1:7070".into()
}

fn default_quic_addr() -> String {
    "127.0.0.1:7071".into()
}

fn default_pgwire_addr() -> String {
    "127.0.0.1:5432".into()
}

fn default_tcp_addr() -> String {
    "127.0.0.1:7072".into()
}

fn default_jwt_secret() -> String {
    DEV_JWT_SECRET.into()
}

fn default_bootstrap_admin_key() -> String {
    DEV_BOOTSTRAP_ADMIN_KEY.into()
}

fn default_rate_limit_per_sec() -> f64 {
    1000.0
}

fn default_rate_limit_burst() -> u32 {
    100
}

fn default_rate_limit_max_users() -> usize {
    10_000
}

fn default_log_level() -> String {
    "info".into()
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            mode: ServerMode::default(),
            http_addr: default_http_addr(),
            quic_addr: default_quic_addr(),
            pgwire_addr: default_pgwire_addr(),
            tcp_addr: default_tcp_addr(),
            jwt_secret: default_jwt_secret(),
            bootstrap_admin_key: default_bootstrap_admin_key(),
            rate_limit_per_sec: default_rate_limit_per_sec(),
            rate_limit_burst: default_rate_limit_burst(),
            rate_limit_max_users: default_rate_limit_max_users(),
            tls_cert_path: None,
            tls_key_path: None,
            tls_insecure_skip: false,
            log_level: default_log_level(),
            storage: StorageConfig::default(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "config error: {}", self.0)
    }
}

impl std::error::Error for ConfigError {}

impl ServerConfig {
    pub fn load_dev() -> Self {
        let mut cfg = Self::from_file_or_default("omnikv.toml");
        cfg.apply_env();
        cfg.mode = ServerMode::Development;
        cfg
    }

    pub fn load_production() -> Result<Self, ConfigError> {
        let mut cfg = Self::from_file_or_default("omnikv.toml");
        cfg.apply_env();
        cfg.mode = ServerMode::Production;
        cfg.validate_production()?;
        Ok(cfg)
    }

    pub fn apply_env(&mut self) {
        if let Ok(v) = std::env::var("OMNIKV_MODE")
            && let Ok(m) = v.parse()
        {
            self.mode = m;
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
        } else if let Ok(v) = std::env::var("OMNI_JWT_SECRET") {
            self.jwt_secret = v;
        }
        if let Ok(v) = std::env::var("OMNIKV_BOOTSTRAP_ADMIN_KEY") {
            self.bootstrap_admin_key = v;
        } else if let Ok(v) = std::env::var("OMNI_BOOTSTRAP_ADMIN_KEY") {
            self.bootstrap_admin_key = v;
        }
        if let Ok(v) =
            std::env::var("OMNIKV_RATE_LIMIT_PER_SEC").or_else(|_| std::env::var("OMNI_RATE_LIMIT"))
        {
            self.rate_limit_per_sec = v.parse().unwrap_or(self.rate_limit_per_sec);
        }
        if let Ok(v) =
            std::env::var("OMNIKV_RATE_LIMIT_BURST").or_else(|_| std::env::var("OMNI_RATE_BURST"))
        {
            self.rate_limit_burst = v.parse().unwrap_or(self.rate_limit_burst);
        }
        if let Ok(v) = std::env::var("OMNIKV_RATE_LIMIT_MAX_USERS") {
            self.rate_limit_max_users = v.parse().unwrap_or(self.rate_limit_max_users);
        }
        if let Ok(v) = std::env::var("OMNIKV_TLS_CERT_PATH") {
            self.tls_cert_path = Some(v);
        }
        if let Ok(v) = std::env::var("OMNIKV_TLS_KEY_PATH") {
            self.tls_key_path = Some(v);
        }
        if let Ok(v) = std::env::var("OMNIKV_TLS_INSECURE_SKIP") {
            self.tls_insecure_skip = v.to_lowercase() == "true";
        }
        if let Ok(v) = std::env::var("OMNIKV_LOG_LEVEL") {
            self.log_level = v;
        }
        if let Ok(v) = std::env::var("OMNIKV_MANIFEST_PATH") {
            self.storage.manifest_path = v;
        }
        if let Ok(v) = std::env::var("OMNIKV_WAL_PATH") {
            self.storage.wal_path = v;
        }
        if let Ok(v) = std::env::var("OMNIKV_BACKUP_DIR") {
            self.storage.backup_dir = v;
        }
    }

    pub fn validate_production(&self) -> Result<(), ConfigError> {
        if self.jwt_secret == DEV_JWT_SECRET {
            return Err(ConfigError(
                "production mode requires a non-default JWT secret".into(),
            ));
        }
        if self.jwt_secret.len() < 32 {
            return Err(ConfigError(
                "JWT secret must be at least 32 characters in production".into(),
            ));
        }
        if self.bootstrap_admin_key == DEV_BOOTSTRAP_ADMIN_KEY {
            return Err(ConfigError(
                "production mode requires a non-default bootstrap admin key".into(),
            ));
        }
        if self.bootstrap_admin_key.len() < 32 {
            return Err(ConfigError(
                "bootstrap admin key must be at least 32 characters in production".into(),
            ));
        }
        if self.bootstrap_admin_key == self.jwt_secret {
            return Err(ConfigError(
                "bootstrap admin key must be different from the JWT secret".into(),
            ));
        }
        if self.rate_limit_per_sec <= 0.0 {
            return Err(ConfigError(
                "OMNIKV_RATE_LIMIT_PER_SEC must be greater than 0".into(),
            ));
        }
        if self.rate_limit_burst == 0 {
            return Err(ConfigError(
                "OMNIKV_RATE_LIMIT_BURST must be greater than 0".into(),
            ));
        }
        if self.rate_limit_max_users == 0 {
            return Err(ConfigError(
                "OMNIKV_RATE_LIMIT_MAX_USERS must be greater than 0".into(),
            ));
        }
        if !self.tls_insecure_skip {
            match (&self.tls_cert_path, &self.tls_key_path) {
                (Some(cert), Some(key)) => {
                    if !Path::new(cert).exists() {
                        return Err(ConfigError(format!("TLS cert not found: {cert}")));
                    }
                    if !Path::new(key).exists() {
                        return Err(ConfigError(format!("TLS key not found: {key}")));
                    }
                }
                _ => {
                    return Err(ConfigError(
                        "production mode requires TLS cert+key or OMNIKV_TLS_INSECURE_SKIP=true"
                            .into(),
                    ));
                }
            }
        } else {
            eprintln!("WARNING: TLS verification is disabled (OMNIKV_TLS_INSECURE_SKIP=true)");
        }
        Ok(())
    }

    fn from_file_or_default(path: &str) -> Self {
        match std::fs::read_to_string(path) {
            Ok(raw) => match toml::from_str(&raw) {
                Ok(cfg) => cfg,
                Err(e) => {
                    eprintln!("WARNING: failed to parse {path}: {e} -- using defaults");
                    Self::default()
                }
            },
            Err(_) => Self::default(),
        }
    }
}

/// Query-engine configuration used by the SQL layer and integration tests.
/// For full server deployment configuration use [`ServerConfig`].
#[derive(Debug, Clone)]
pub struct OmniConfig {
    /// HTTP REST API port.
    pub port: u16,
    /// PostgreSQL wire-protocol port.
    pub pg_port: u16,
    /// Maximum seconds a query may run before being cancelled.
    pub query_timeout_secs: u64,
    /// Maximum number of concurrent client connections.
    pub max_connections: usize,
    /// Maximum bytes allowed in a single write batch.
    pub max_write_batch_bytes: usize,
}

impl Default for OmniConfig {
    fn default() -> Self {
        Self {
            port: 8080,
            pg_port: 5433,
            query_timeout_secs: 30,
            max_connections: 256,
            max_write_batch_bytes: 64 * 1024 * 1024, // 64 MiB
        }
    }
}
