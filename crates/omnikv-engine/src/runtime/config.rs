use std::{
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
};

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

const DEFAULT_CONFIG_PATH: &str = "omnikv.toml";
const OMNIKV_CONFIG_ENV: &str = "OMNIKV_CONFIG";
const LEGACY_OMNI_CONFIG_ENV: &str = "OMNI_CONFIG";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
    /// Load development configuration with the normal runtime precedence:
    ///
    /// defaults < config file < environment variables.
    pub fn load_dev() -> Result<Self, ConfigError> {
        let mut cfg = Self::load_from_runtime_sources(std::iter::empty::<String>())?;
        cfg.mode = ServerMode::Development;
        cfg.validate_common()?;
        Ok(cfg)
    }

    /// Load production configuration with the normal runtime precedence, then
    /// force production validation. This is retained for callers that require
    /// fail-closed production startup regardless of file/env mode.
    pub fn load_production() -> Result<Self, ConfigError> {
        let mut cfg = Self::load_from_runtime_sources(std::iter::empty::<String>())?;
        cfg.mode = ServerMode::Production;
        cfg.validate_production()?;
        Ok(cfg)
    }

    /// Load server configuration from CLI/config/env/defaults.
    ///
    /// Precedence is: defaults < config file < environment variables. CLI
    /// currently controls the config file path via `--config <path>` or
    /// `--config=<path>` and has higher precedence than `OMNIKV_CONFIG` /
    /// legacy `OMNI_CONFIG` for selecting that file.
    pub fn load_server_from_args<I, S>(args: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let cfg = Self::load_from_runtime_sources(args)?;
        cfg.validate_runtime()?;
        Ok(cfg)
    }

    pub fn apply_env(&mut self) -> Result<(), ConfigError> {
        if let Ok(v) = std::env::var("OMNIKV_MODE") {
            self.mode = parse_env_value("OMNIKV_MODE", &v)?;
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
            self.rate_limit_per_sec = parse_env_value(
                if std::env::var("OMNIKV_RATE_LIMIT_PER_SEC").is_ok() {
                    "OMNIKV_RATE_LIMIT_PER_SEC"
                } else {
                    "OMNI_RATE_LIMIT"
                },
                &v,
            )?;
        }
        if let Ok(v) =
            std::env::var("OMNIKV_RATE_LIMIT_BURST").or_else(|_| std::env::var("OMNI_RATE_BURST"))
        {
            self.rate_limit_burst = parse_env_value(
                if std::env::var("OMNIKV_RATE_LIMIT_BURST").is_ok() {
                    "OMNIKV_RATE_LIMIT_BURST"
                } else {
                    "OMNI_RATE_BURST"
                },
                &v,
            )?;
        }
        if let Ok(v) = std::env::var("OMNIKV_RATE_LIMIT_MAX_USERS") {
            self.rate_limit_max_users = parse_env_value("OMNIKV_RATE_LIMIT_MAX_USERS", &v)?;
        }
        if let Ok(v) = std::env::var("OMNIKV_TLS_CERT_PATH") {
            self.tls_cert_path = Some(v);
        }
        if let Ok(v) = std::env::var("OMNIKV_TLS_KEY_PATH") {
            self.tls_key_path = Some(v);
        }
        if let Ok(v) = std::env::var("OMNIKV_TLS_INSECURE_SKIP") {
            self.tls_insecure_skip = parse_env_value("OMNIKV_TLS_INSECURE_SKIP", &v)?;
        }
        if let Ok(v) = std::env::var("OMNIKV_LOG_LEVEL") {
            self.log_level = v;
        }
        if let Ok(v) = std::env::var("OMNIKV_DATA_DIR") {
            let data_dir = PathBuf::from(v);
            self.storage.manifest_path = path_to_string(data_dir.join("manifest.json"))?;
            self.storage.wal_path = path_to_string(data_dir.join("wal.bin"))?;
            self.storage.backup_dir = path_to_string(data_dir.join("backups"))?;
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
        if let Ok(v) = std::env::var("OMNIKV_MAX_OPEN_FILES") {
            self.storage.max_open_files = parse_env_value("OMNIKV_MAX_OPEN_FILES", &v)?;
        }
        if let Ok(v) = std::env::var("OMNIKV_WRITE_BUFFER_MB") {
            self.storage.write_buffer_mb = parse_env_value("OMNIKV_WRITE_BUFFER_MB", &v)?;
        }
        if let Ok(v) = std::env::var("OMNIKV_COMPACTION_WORKERS") {
            self.storage.compaction_workers = parse_env_value("OMNIKV_COMPACTION_WORKERS", &v)?;
        }
        Ok(())
    }

    pub fn validate_runtime(&self) -> Result<(), ConfigError> {
        self.validate_common()?;
        if self.mode == ServerMode::Production {
            self.validate_production()?;
        }
        Ok(())
    }

    pub fn validate_production(&self) -> Result<(), ConfigError> {
        self.validate_common()?;
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

    fn validate_common(&self) -> Result<(), ConfigError> {
        validate_addr("http_addr", &self.http_addr)?;
        validate_addr("quic_addr", &self.quic_addr)?;
        validate_addr("pgwire_addr", &self.pgwire_addr)?;
        validate_addr("tcp_addr", &self.tcp_addr)?;

        if self.log_level.trim().is_empty() {
            return Err(ConfigError("log_level must not be empty".into()));
        }
        if self.storage.manifest_path.trim().is_empty() {
            return Err(ConfigError(
                "storage.manifest_path must not be empty".into(),
            ));
        }
        if self.storage.wal_path.trim().is_empty() {
            return Err(ConfigError("storage.wal_path must not be empty".into()));
        }
        if self.storage.backup_dir.trim().is_empty() {
            return Err(ConfigError("storage.backup_dir must not be empty".into()));
        }
        if self.storage.max_open_files == 0 {
            return Err(ConfigError(
                "storage.max_open_files must be greater than 0".into(),
            ));
        }
        if self.storage.write_buffer_mb == 0 {
            return Err(ConfigError(
                "storage.write_buffer_mb must be greater than 0".into(),
            ));
        }
        if self.storage.compaction_workers == 0 {
            return Err(ConfigError(
                "storage.compaction_workers must be greater than 0".into(),
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
        Ok(())
    }

    fn load_from_runtime_sources<I, S>(args: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let config_path = resolve_config_path(args)?;
        let mut cfg = Self::from_optional_config_file(config_path.as_deref())?;
        cfg.apply_env()?;
        Ok(cfg)
    }

    fn from_optional_config_file(path: Option<&Path>) -> Result<Self, ConfigError> {
        if let Some(path) = path {
            return Self::from_config_file(path);
        }
        let default_path = Path::new(DEFAULT_CONFIG_PATH);
        if default_path.exists() {
            Self::from_config_file(default_path)
        } else {
            Ok(Self::default())
        }
    }

    fn from_config_file(path: &Path) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path).map_err(|e| {
            ConfigError(format!(
                "failed to read config file {}: {e}",
                path.display()
            ))
        })?;
        toml::from_str(&raw).map_err(|e| {
            ConfigError(format!(
                "failed to parse config file {}: {e}",
                path.display()
            ))
        })
    }
}

fn resolve_config_path<I, S>(args: I) -> Result<Option<PathBuf>, ConfigError>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut args = args.into_iter().map(Into::into);
    let mut cli_path = None;

    while let Some(arg) = args.next() {
        if arg == "--config" {
            let path = args
                .next()
                .ok_or_else(|| ConfigError("--config requires a file path".into()))?;
            cli_path = Some(PathBuf::from(path));
        } else if let Some(path) = arg.strip_prefix("--config=") {
            if path.is_empty() {
                return Err(ConfigError("--config requires a file path".into()));
            }
            cli_path = Some(PathBuf::from(path));
        } else {
            return Err(ConfigError(format!("unknown server argument: {arg}")));
        }
    }

    if let Some(path) = cli_path {
        return Ok(Some(path));
    }
    if let Ok(path) = std::env::var(OMNIKV_CONFIG_ENV) {
        return Ok(Some(PathBuf::from(path)));
    }
    if let Ok(path) = std::env::var(LEGACY_OMNI_CONFIG_ENV) {
        return Ok(Some(PathBuf::from(path)));
    }
    Ok(None)
}

fn parse_env_value<T>(name: &str, value: &str) -> Result<T, ConfigError>
where
    T: FromStr,
    T::Err: fmt::Display,
{
    value
        .parse()
        .map_err(|e| ConfigError(format!("invalid value for {name}={value:?}: {e}")))
}

fn validate_addr(name: &str, value: &str) -> Result<(), ConfigError> {
    value
        .parse::<std::net::SocketAddr>()
        .map(|_| ())
        .map_err(|e| ConfigError(format!("{name} must be a valid socket address: {e}")))
}

fn path_to_string(path: PathBuf) -> Result<String, ConfigError> {
    path.into_os_string().into_string().map_err(|path| {
        ConfigError(format!(
            "path is not valid UTF-8: {}",
            PathBuf::from(path).display()
        ))
    })
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
