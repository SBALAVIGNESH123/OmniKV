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
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    pub manifest_path: String,
    pub wal_path: String,
    pub backup_dir: String,
    pub max_open_files: u32,
    pub write_buffer_mb: u32,
    pub compaction_workers: u32,
    pub memtable_flush_threshold: usize,
    pub l0_compaction_trigger: usize,
    pub l1_compaction_trigger: usize,
    pub l0_write_stall_threshold: usize,
    pub write_stall_wait_attempts: u32,
    pub write_stall_wait_ms: u64,
    pub compaction_check_interval_ms: u64,
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
            memtable_flush_threshold: 10_000,
            l0_compaction_trigger: 4,
            l1_compaction_trigger: 4,
            l0_write_stall_threshold: 12,
            write_stall_wait_attempts: 50,
            write_stall_wait_ms: 100,
            compaction_check_interval_ms: 500,
        }
    }
}

impl StorageConfig {
    pub fn compaction_policy(&self) -> crate::CompactionPolicy {
        crate::CompactionPolicy {
            l0_compaction_trigger: self.l0_compaction_trigger,
            l1_compaction_trigger: self.l1_compaction_trigger,
            l0_write_stall_threshold: self.l0_write_stall_threshold,
            write_stall_wait_attempts: self.write_stall_wait_attempts,
            write_stall_wait_ms: self.write_stall_wait_ms,
        }
    }
}

/// Cluster (Raft) configuration. Absent — `raft_addr` and `node_id`
/// both unset — the server runs as an independent single-node engine,
/// exactly as it always has. Present, the server boots an openraft node
/// on `raft_addr` and every write goes through consensus before it is
/// acknowledged.
///
/// `peers` are the OTHER nodes' raft addresses ("host:port"), used only
/// at bootstrap to seed cluster membership; a node joining an existing
/// cluster learns the full membership from the leader.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftConfig {
    /// This node's cluster ID (openraft NodeId). 1-based.
    pub node_id: Option<u64>,
    /// The plaintext listener for consensus traffic (etcd's peer-port
    /// model: client TLS never terminates here). This is the BIND
    /// address — `0.0.0.0:port` is valid here.
    pub raft_addr: Option<String>,
    /// What PEERS dial to reach this node — the address that goes into
    /// cluster membership. Must be routable FROM other nodes; a
    /// wildcard (0.0.0.0/::) is refused by validation because it would
    /// resolve to the dialer itself. When unset, the bind address is
    /// advertised (fine for specific-IP/loopback binds; a wildcard bind
    /// REQUIRES this override).
    #[serde(default)]
    pub advertise_addr: Option<String>,
    /// The other initial members, "host:port" per entry. Empty means
    /// single-node cluster when raft_addr is set.
    pub peers: Vec<String>,
    /// Override election/heartbeat tuning (defaults suit tests and LANs).
    pub heartbeat_interval_ms: Option<u64>,
    pub election_timeout_min_ms: Option<u64>,
    pub election_timeout_max_ms: Option<u64>,
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
    /// Explicit opt-in for binding the TCP command interface on a
    /// non-loopback address. The interface is JWT-gated either way, but a
    /// public bind still exposes an unrestricted read/write path to every
    /// host that can reach the port, so it never happens by accident.
    #[serde(default)]
    pub tcp_bind_public: bool,
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
    #[serde(default)]
    pub raft: RaftConfig,
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
            tcp_bind_public: false,
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
            raft: RaftConfig::default(),
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
        if let Ok(v) = std::env::var("OMNIKV_TCP_BIND_PUBLIC") {
            self.tcp_bind_public = parse_env_value("OMNIKV_TCP_BIND_PUBLIC", &v)?;
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
        if let Ok(v) = std::env::var("OMNIKV_MEMTABLE_FLUSH_THRESHOLD") {
            self.storage.memtable_flush_threshold =
                parse_env_value("OMNIKV_MEMTABLE_FLUSH_THRESHOLD", &v)?;
        }
        if let Ok(v) = std::env::var("OMNIKV_L0_COMPACTION_TRIGGER") {
            self.storage.l0_compaction_trigger =
                parse_env_value("OMNIKV_L0_COMPACTION_TRIGGER", &v)?;
        }
        if let Ok(v) = std::env::var("OMNIKV_L1_COMPACTION_TRIGGER") {
            self.storage.l1_compaction_trigger =
                parse_env_value("OMNIKV_L1_COMPACTION_TRIGGER", &v)?;
        }
        if let Ok(v) = std::env::var("OMNIKV_L0_WRITE_STALL_THRESHOLD") {
            self.storage.l0_write_stall_threshold =
                parse_env_value("OMNIKV_L0_WRITE_STALL_THRESHOLD", &v)?;
        }
        if let Ok(v) = std::env::var("OMNIKV_WRITE_STALL_WAIT_ATTEMPTS") {
            self.storage.write_stall_wait_attempts =
                parse_env_value("OMNIKV_WRITE_STALL_WAIT_ATTEMPTS", &v)?;
        }
        if let Ok(v) = std::env::var("OMNIKV_WRITE_STALL_WAIT_MS") {
            self.storage.write_stall_wait_ms = parse_env_value("OMNIKV_WRITE_STALL_WAIT_MS", &v)?;
        }
        if let Ok(v) = std::env::var("OMNIKV_COMPACTION_CHECK_INTERVAL_MS") {
            self.storage.compaction_check_interval_ms =
                parse_env_value("OMNIKV_COMPACTION_CHECK_INTERVAL_MS", &v)?;
        }

        // ── Cluster (Raft) ── OMNIKV_* names with the OMNI_NODE_ID /
        // OMNI_PEERS legacy names the docker-compose files already set.
        if let Ok(v) = std::env::var("OMNIKV_RAFT_ADDR") {
            self.raft.raft_addr = if v.is_empty() { None } else { Some(v) };
        }
        if let Ok(v) = std::env::var("OMNIKV_RAFT_ADVERTISE_ADDR") {
            self.raft.advertise_addr = if v.is_empty() { None } else { Some(v) };
        }
        if let Ok(v) = std::env::var("OMNIKV_NODE_ID").or_else(|_| std::env::var("OMNI_NODE_ID")) {
            self.raft.node_id = Some(
                v.parse()
                    .map_err(|_| ConfigError("Invalid OMNIKV_NODE_ID".into()))?,
            );
        }
        if let Ok(v) = std::env::var("OMNIKV_RAFT_PEERS").or_else(|_| std::env::var("OMNI_PEERS")) {
            self.raft.peers = v
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect();
        }
        if let Ok(v) = std::env::var("OMNIKV_RAFT_HEARTBEAT_MS") {
            self.raft.heartbeat_interval_ms = Some(
                v.parse()
                    .map_err(|_| ConfigError("Invalid OMNIKV_RAFT_HEARTBEAT_MS".into()))?,
            );
        }
        if let Ok(v) = std::env::var("OMNIKV_RAFT_ELECTION_MIN_MS") {
            self.raft.election_timeout_min_ms = Some(
                v.parse()
                    .map_err(|_| ConfigError("Invalid OMNIKV_RAFT_ELECTION_MIN_MS".into()))?,
            );
        }
        if let Ok(v) = std::env::var("OMNIKV_RAFT_ELECTION_MAX_MS") {
            self.raft.election_timeout_max_ms = Some(
                v.parse()
                    .map_err(|_| ConfigError("Invalid OMNIKV_RAFT_ELECTION_MAX_MS".into()))?,
            );
        }
        Ok(())
    }

    pub fn validate_runtime(&self) -> Result<(), ConfigError> {
        // A node id without a raft listener, or a listener without a
        // node id, is a half-configured cluster: refuse to boot rather
        // than silently degrade to single-node.
        if self.raft.node_id.is_some() != self.raft.raft_addr.is_some() {
            return Err(ConfigError(
                "raft.node_id and raft.raft_addr must be set together (OMNIKV_NODE_ID / OMNIKV_RAFT_ADDR)".into(),
            ));
        }
        if self.raft.election_timeout_min_ms.unwrap_or(0)
            > self.raft.election_timeout_max_ms.unwrap_or(u64::MAX)
        {
            return Err(ConfigError(
                "raft.election_timeout_min_ms must be <= election_timeout_max_ms".into(),
            ));
        }
        // The ADVERTISED address is what peers dial — it must never be
        // a wildcard. When the explicit override is set, validate it;
        // when it is not, the bind address is the advertised one, so a
        // wildcard bind requires the override (a cluster member bound
        // to 0.0.0.0 without a routable advertise address would poison
        // every peer's routing table with an address that resolves to
        // the dialer itself).
        let advertised = self
            .raft
            .advertise_addr
            .as_deref()
            .or(self.raft.raft_addr.as_deref());
        if let Some(addr) = advertised
            && is_wildcard_addr(addr)
        {
            return Err(ConfigError(
                "raft advertise address must be routable by peers (got a wildcard; set \
                 OMNIKV_RAFT_ADVERTISE_ADDR to this node's reachable host:port)"
                    .into(),
            ));
        }
        // Shape check on the explicit override only: "host:port" with a
        // non-empty host and a valid port. Hostnames (the container
        // pattern: omni-node-1:9090) are valid here — peers resolve them
        // through the compose network.
        if let Some(advertise) = &self.raft.advertise_addr {
            validate_raft_endpoint(advertise, "raft.advertise_addr")?;
        }
        // Each network address must map to exactly one peer identity:
        // duplicate peer endpoints (or a peer that duplicates this node's
        // advertised address) let openraft route several member ids to one
        // listener, which breaks quorum arithmetic in subtle ways.
        //
        // Every peer is also dialed exactly as written (boot_cluster_node
        // copies it into openraft::BasicNode.addr and OmniNetwork builds
        // the RPC URL from it), so a malformed, wildcard, or port-zero
        // peer is not a typo a operator can recover from later — it is a
        // member that can never be reached. Validate each one up front,
        // with the same rules as the advertised address.
        if let Some(me) = advertised {
            let mut seen = std::collections::HashSet::new();
            for peer in &self.raft.peers {
                if peer == me {
                    return Err(ConfigError(
                        "raft peers must not include this node's own advertised address".into(),
                    ));
                }
                validate_raft_endpoint(peer, "raft peer")?;
                if !seen.insert(peer) {
                    return Err(ConfigError(format!(
                        "raft peers must be unique (duplicate: {peer})"
                    )));
                }
            }
        }
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
        // The TCP command interface grants full read/write access once
        // authenticated. Loopback is the safe default; anything reachable
        // from other hosts has to be turned on deliberately.
        if !self.tcp_bind_public
            && let Ok(sock) = self.tcp_addr.parse::<std::net::SocketAddr>()
            && !sock.ip().is_loopback()
        {
            return Err(ConfigError(
                "tcp_addr is bound to a non-loopback address; set \
                 OMNIKV_TCP_BIND_PUBLIC=true to confirm you want the command \
                 interface reachable from other hosts"
                    .into(),
            ));
        }

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
        if self.storage.memtable_flush_threshold == 0 {
            return Err(ConfigError(
                "storage.memtable_flush_threshold must be greater than 0".into(),
            ));
        }
        if self.storage.compaction_check_interval_ms == 0 {
            return Err(ConfigError(
                "storage.compaction_check_interval_ms must be greater than 0".into(),
            ));
        }
        self.storage
            .compaction_policy()
            .validate()
            .map_err(|err| ConfigError(err.to_string()))?;
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

/// Whether an address string is a wildcard ANY-address ("0.0.0.0:port",
/// "[::]:port") — valid to BIND, never valid to ADVERTISE (peers dialing
/// it reach themselves). Unparseable strings (bare hostnames) are not
/// wildcards; the advertise-shape check handles those separately.
fn is_wildcard_addr(addr: &str) -> bool {
    addr.parse::<std::net::SocketAddr>()
        .is_ok_and(|sock| sock.ip().is_unspecified())
}

/// Validates a raft endpoint exactly as it will be dialed: a non-wildcard
/// `host:port` with a non-empty host and an explicit nonzero port. Hostnames
/// (the container pattern: `omni-node-1:9090`) are valid — peers resolve them
/// through the compose network. Applies to both this node's advertised address
/// and to every peer, since both are copied into cluster membership verbatim.
fn validate_raft_endpoint(addr: &str, field: &str) -> Result<(), ConfigError> {
    if is_wildcard_addr(addr) {
        return Err(ConfigError(format!(
            "{field} must be routable by peers (got a wildcard: {addr})"
        )));
    }
    match addr.rsplit_once(':') {
        Some((host, port)) => {
            // Port 0 means "ephemeral" to a BIND, but in an ADVERTISED or
            // PEER address it tells the dialer to hit a random port — never
            // reachable. Require an explicit port.
            if host.is_empty() || port.parse::<u16>().map_or(true, |p| p == 0) {
                return Err(ConfigError(format!(
                    "{field} must be a valid host:port with a nonzero port (got {addr})"
                )));
            }
        }
        None => {
            return Err(ConfigError(format!(
                "{field} must be a valid host:port (got {addr})"
            )));
        }
    }
    Ok(())
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
