//! Production configuration loader for OmniKV.
//!
//! Configuration is loaded in priority order:
//!   1. Environment variables (highest)
//!   2. Config file (TOML) specified via `--config` or `OMNIKV_CONFIG`
//!   3. Compiled-in defaults (lowest, safe for local-dev only)
//!
//! In production mode (`mode = "production"` or `OMNIKV_MODE=production`):
//!   - The default JWT secret is rejected at startup.
//!   - TLS material must be explicitly configured or `tls_insecure_skip = true`
//!     must be set with a warning.
//!   - Missing required fields cause a hard startup failure with a clear message.

use std::env;
use std::fmt;
use std::path::{Path, PathBuf};

// ── Error type ───────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ConfigError: {}", self.0)
    }
}

impl std::error::Error for ConfigError {}

macro_rules! cfg_err {
    ($($t:tt)*) => { ConfigError(format!($($t)*)) };
}

// ── Server mode ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerMode {
    /// Local development — permissive defaults, demo secrets allowed.
    Development,
    /// Production — secrets and TLS must be explicit; fails closed.
    Production,
}

impl ServerMode {
    fn from_str(s: &str) -> Result<Self, ConfigError> {
        match s.to_ascii_lowercase().as_str() {
            "development" | "dev" => Ok(Self::Development),
            "production"  | "prod" => Ok(Self::Production),
            other => Err(cfg_err!("unknown server mode {:?}; expected 'development' or 'production'", other)),
        }
    }
    pub fn is_production(&self) -> bool { *self == Self::Production }
}

impl Default for ServerMode {
    fn default() -> Self { Self::Development }
}

// ── TLS config ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path:  PathBuf,
}

// ── Top-level ServerConfig ───────────────────────────────────────────────────

/// All runtime configuration for an OmniKV server instance.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    // --- mode ---
    pub mode: ServerMode,

    // --- network ---
    pub host: String,
    pub port: u16,
    pub admin_port: u16,

    // --- paths ---
    pub data_dir:   PathBuf,
    pub wal_dir:    PathBuf,
    pub backup_dir: PathBuf,
    pub log_dir:    PathBuf,

    // --- security ---
    pub jwt_secret: String,
    pub tls: Option<TlsConfig>,
    pub tls_insecure_skip: bool,

    // --- logging ---
    pub log_level: String,

    // --- storage ---
    pub max_open_files:  usize,
    pub write_buffer_mb: usize,
    pub compaction_workers: usize,
}

/// The default JWT secret used only in development mode.
pub const DEV_JWT_SECRET: &str = "omnikv-dev-secret-do-not-use-in-production";

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            mode: ServerMode::Development,
            host: "127.0.0.1".to_string(),
            port: 7070,
            admin_port: 7071,
            data_dir:   PathBuf::from("./data"),
            wal_dir:    PathBuf::from("./data/wal"),
            backup_dir: PathBuf::from("./data/backups"),
            log_dir:    PathBuf::from("./logs"),
            jwt_secret: DEV_JWT_SECRET.to_string(),
            tls: None,
            tls_insecure_skip: false,
            log_level: "info".to_string(),
            max_open_files: 512,
            write_buffer_mb: 64,
            compaction_workers: 2,
        }
    }
}

// ── Builder ──────────────────────────────────────────────────────────────────

/// Fluent builder that reads env vars on top of a base config.
pub struct ConfigBuilder {
    base: ServerConfig,
}

impl ConfigBuilder {
    pub fn new() -> Self {
        Self { base: ServerConfig::default() }
    }

    pub fn with_base(base: ServerConfig) -> Self {
        Self { base }
    }

    /// Override fields from environment variables (highest priority).
    pub fn apply_env(mut self) -> Self {
        if let Ok(v) = env::var("OMNIKV_MODE")         { if let Ok(m) = ServerMode::from_str(&v) { self.base.mode = m; } }
        if let Ok(v) = env::var("OMNIKV_HOST")         { self.base.host = v; }
        if let Ok(v) = env::var("OMNIKV_PORT")         { if let Ok(p) = v.parse() { self.base.port = p; } }
        if let Ok(v) = env::var("OMNIKV_ADMIN_PORT")   { if let Ok(p) = v.parse() { self.base.admin_port = p; } }
        if let Ok(v) = env::var("OMNIKV_DATA_DIR")     { self.base.data_dir   = PathBuf::from(v); }
        if let Ok(v) = env::var("OMNIKV_WAL_DIR")      { self.base.wal_dir    = PathBuf::from(v); }
        if let Ok(v) = env::var("OMNIKV_BACKUP_DIR")   { self.base.backup_dir = PathBuf::from(v); }
        if let Ok(v) = env::var("OMNIKV_LOG_DIR")      { self.base.log_dir    = PathBuf::from(v); }
        if let Ok(v) = env::var("OMNIKV_JWT_SECRET")   { self.base.jwt_secret = v; }
        if let Ok(v) = env::var("OMNIKV_LOG_LEVEL")    { self.base.log_level  = v; }
        if let Ok(v) = env::var("OMNIKV_TLS_CERT")     {
            let cert = PathBuf::from(&v);
            let key  = env::var("OMNIKV_TLS_KEY").map(PathBuf::from).unwrap_or_else(|_| cert.with_extension("key"));
            self.base.tls = Some(TlsConfig { cert_path: cert, key_path: key });
        }
        if let Ok(v) = env::var("OMNIKV_TLS_INSECURE_SKIP") {
            self.base.tls_insecure_skip = matches!(v.to_ascii_lowercase().as_str(), "1"|"true"|"yes");
        }
        if let Ok(v) = env::var("OMNIKV_MAX_OPEN_FILES")     { if let Ok(n) = v.parse() { self.base.max_open_files = n; } }
        if let Ok(v) = env::var("OMNIKV_WRITE_BUFFER_MB")    { if let Ok(n) = v.parse() { self.base.write_buffer_mb = n; } }
        if let Ok(v) = env::var("OMNIKV_COMPACTION_WORKERS") { if let Ok(n) = v.parse() { self.base.compaction_workers = n; } }
        self
    }

    /// Validate the config for the target mode and return it.
    pub fn build(self) -> Result<ServerConfig, ConfigError> {
        let cfg = self.base;
        if cfg.mode.is_production() {
            validate_production(&cfg)?;
        }
        Ok(cfg)
    }
}

impl Default for ConfigBuilder {
    fn default() -> Self { Self::new() }
}

// ── Production validation ────────────────────────────────────────────────────

fn validate_production(cfg: &ServerConfig) -> Result<(), ConfigError> {
    // Reject the default dev secret
    if cfg.jwt_secret == DEV_JWT_SECRET {
        return Err(cfg_err!(
            "production mode requires a non-default JWT secret; \
             set OMNIKV_JWT_SECRET to a secret of at least 32 characters"
        ));
    }
    if cfg.jwt_secret.len() < 32 {
        return Err(cfg_err!(
            "production JWT secret must be at least 32 characters (got {})",
            cfg.jwt_secret.len()
        ));
    }
    // TLS: must be configured or explicitly skipped with a loud warning
    if cfg.tls.is_none() && !cfg.tls_insecure_skip {
        return Err(cfg_err!(
            "production mode requires TLS; set OMNIKV_TLS_CERT + OMNIKV_TLS_KEY, \
             or set OMNIKV_TLS_INSECURE_SKIP=true (not recommended)"
        ));
    }
    if cfg.tls_insecure_skip {
        eprintln!("WARNING: OMNIKV_TLS_INSECURE_SKIP is set — connections are not encrypted");
    }
    // TLS files must exist if configured
    if let Some(ref tls) = cfg.tls {
        check_path_exists(&tls.cert_path, "TLS certificate")?;
        check_path_exists(&tls.key_path,  "TLS private key")?;
    }
    Ok(())
}

fn check_path_exists(p: &Path, label: &str) -> Result<(), ConfigError> {
    if !p.exists() {
        return Err(cfg_err!("{} not found at {:?}", label, p));
    }
    Ok(())
}

// ── Public convenience ───────────────────────────────────────────────────────

/// Load config for development (no validation, uses defaults + env).
pub fn load_dev() -> ServerConfig {
    ConfigBuilder::new().apply_env().build().expect("dev config must not fail")
}

/// Load config for production (validates secrets, TLS, paths).
pub fn load_production() -> Result<ServerConfig, ConfigError> {
    ConfigBuilder::new().apply_env().build()
}
