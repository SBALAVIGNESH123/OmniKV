//! Integration tests for ServerConfig.
//!
//! These tests exercise the real config loading, env-override, and production
//! validation paths.  No mocking — every code path is driven as the binary
//! would drive it.

use omni_engine::config::{ConfigError, LogLevel, ServerConfig, ServerMode, DEV_JWT_SECRET};
use std::io::Write;
use tempfile::NamedTempFile;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Run a closure with specific env vars set, then restore previous values.
fn with_env<F: FnOnce()>(vars: &[(&str, &str)], f: F) {
    let saved: Vec<(&str, Option<String>)> = vars
        .iter()
        .map(|(k, _)| (*k, std::env::var(k).ok()))
        .collect();
    for (k, v) in vars {
        std::env::set_var(k, v);
    }
    f();
    for (k, prev) in saved {
        match prev {
            Some(v) => std::env::set_var(k, v),
            None => std::env::remove_var(k),
        }
    }
}

fn prod_secret() -> &'static str {
    "a-sufficiently-long-production-jwt-secret-value"
}

// ── Default / dev mode ───────────────────────────────────────────────────────

#[test]
fn test_default_mode_is_development() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.mode, ServerMode::Development);
}

#[test]
fn test_default_jwt_secret_is_dev_constant() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.jwt_secret, DEV_JWT_SECRET);
}

#[test]
fn test_default_http_addr() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.http_addr, "0.0.0.0:8443");
}

#[test]
fn test_default_quic_addr() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.quic_addr, "0.0.0.0:4433");
}

#[test]
fn test_default_pgwire_addr() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.pgwire_addr, "0.0.0.0:5433");
}

#[test]
fn test_default_tcp_addr() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.tcp_addr, "0.0.0.0:8080");
}

#[test]
fn test_default_tls_is_none() {
    let cfg = ServerConfig::default();
    assert!(cfg.tls.is_none());
}

#[test]
fn test_default_storage_paths() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.storage.manifest_path.to_str().unwrap(), "manifest.json");
    assert_eq!(cfg.storage.wal_path.to_str().unwrap(), "wal.bin");
}

// ── TOML parsing ─────────────────────────────────────────────────────────────

#[test]
fn test_toml_parse_minimal() {
    let toml = r#"
        http_addr = "127.0.0.1:9443"
        jwt_secret = "test-secret"
    "#;
    let cfg = ServerConfig::from_toml_str(toml).unwrap();
    assert_eq!(cfg.http_addr, "127.0.0.1:9443");
    assert_eq!(cfg.jwt_secret, "test-secret");
}

#[test]
fn test_toml_parse_all_fields() {
    let toml = r#"
        mode = "production"
        http_addr = "0.0.0.0:443"
        quic_addr = "0.0.0.0:443"
        pgwire_addr = "0.0.0.0:5432"
        tcp_addr = "0.0.0.0:9090"
        jwt_secret = "my-super-secret-production-value-here"
        tls_insecure_skip = true
        log_level = "debug"

        [storage]
        manifest_path = "/data/manifest.json"
        wal_path = "/data/wal.bin"
        backup_dir = "/backups"
        max_memtable_bytes = 134217728
        max_sstables = 16
    "#;
    let cfg = ServerConfig::from_toml_str(toml).unwrap();
    assert_eq!(cfg.mode, ServerMode::Production);
    assert_eq!(cfg.log_level, LogLevel::Debug);
    assert_eq!(cfg.storage.max_sstables, 16);
    assert_eq!(cfg.storage.max_memtable_bytes, 134_217_728);
    assert!(cfg.tls_insecure_skip);
}

#[test]
fn test_toml_invalid_returns_error() {
    let result = ServerConfig::from_toml_str("not valid toml [[[");
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("OmniKV config error"));
}

// ── Environment variable overrides ───────────────────────────────────────────

#[test]
fn test_env_override_jwt_secret() {
    with_env(&[("OMNIKV_JWT_SECRET", "env-secret-value")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert_eq!(cfg.jwt_secret, "env-secret-value");
    });
}

#[test]
fn test_env_override_http_addr() {
    with_env(&[("OMNIKV_HTTP_ADDR", "127.0.0.1:9000")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert_eq!(cfg.http_addr, "127.0.0.1:9000");
    });
}

#[test]
fn test_env_override_manifest_and_wal() {
    with_env(
        &[
            ("OMNIKV_MANIFEST_PATH", "/tmp/test-manifest.json"),
            ("OMNIKV_WAL_PATH", "/tmp/test-wal.bin"),
        ],
        || {
            let mut cfg = ServerConfig::default();
            cfg.apply_env();
            assert_eq!(
                cfg.storage.manifest_path.to_str().unwrap(),
                "/tmp/test-manifest.json"
            );
            assert_eq!(cfg.storage.wal_path.to_str().unwrap(), "/tmp/test-wal.bin");
        },
    );
}

#[test]
fn test_env_override_log_level_warn() {
    with_env(&[("OMNIKV_LOG_LEVEL", "warn")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert_eq!(cfg.log_level, LogLevel::Warn);
    });
}

#[test]
fn test_env_mode_production() {
    with_env(&[("OMNIKV_MODE", "production")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert_eq!(cfg.mode, ServerMode::Production);
    });
}

#[test]
fn test_env_tls_insecure_skip_true() {
    with_env(&[("OMNIKV_TLS_INSECURE_SKIP", "true")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert!(cfg.tls_insecure_skip);
    });
}

// ── Production validation ─────────────────────────────────────────────────────

#[test]
fn test_prod_rejects_dev_jwt_secret() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.tls_insecure_skip = true;
    // jwt_secret is still DEV_JWT_SECRET
    let result = cfg.validate_production();
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("JWT") || msg.contains("jwt") || msg.contains("secret"));
}

#[test]
fn test_prod_rejects_short_jwt_secret() {
    let mut cfg = ServerConfig::default();
    cfg.tls_insecure_skip = true;
    cfg.jwt_secret = "tooshort".to_string();
    let result = cfg.validate_production();
    assert!(result.is_err());
}

#[test]
fn test_prod_rejects_missing_tls_when_not_skipped() {
    let mut cfg = ServerConfig::default();
    cfg.jwt_secret = prod_secret().to_string();
    cfg.tls = None;
    cfg.tls_insecure_skip = false;
    let result = cfg.validate_production();
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("TLS") || msg.contains("tls"));
}

#[test]
fn test_prod_accepts_insecure_skip_with_good_secret() {
    let mut cfg = ServerConfig::default();
    cfg.jwt_secret = prod_secret().to_string();
    cfg.tls_insecure_skip = true;
    assert!(cfg.validate_production().is_ok());
}

#[test]
fn test_prod_rejects_missing_cert_file() {
    use omni_engine::config::TlsConfig;
    let mut cfg = ServerConfig::default();
    cfg.jwt_secret = prod_secret().to_string();
    cfg.tls = Some(TlsConfig {
        cert_path: std::path::PathBuf::from("/nonexistent/cert.pem"),
        key_path: std::path::PathBuf::from("/nonexistent/key.pem"),
    });
    let result = cfg.validate_production();
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("cert") || msg.contains("TLS"));
}

#[test]
fn test_prod_accepts_existing_tls_files() {
    use omni_engine::config::TlsConfig;
    let cert = NamedTempFile::new().unwrap();
    let key = NamedTempFile::new().unwrap();
    let mut cfg = ServerConfig::default();
    cfg.jwt_secret = prod_secret().to_string();
    cfg.tls = Some(TlsConfig {
        cert_path: cert.path().to_path_buf(),
        key_path: key.path().to_path_buf(),
    });
    assert!(cfg.validate_production().is_ok());
}

// ── Load from TOML file ───────────────────────────────────────────────────────

#[test]
fn test_load_from_toml_file() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, r#"http_addr = "0.0.0.0:9443""#).unwrap();
    writeln!(f, r#"jwt_secret = "file-loaded-secret""#).unwrap();
    let cfg = ServerConfig::load_from_file(f.path()).unwrap();
    assert_eq!(cfg.http_addr, "0.0.0.0:9443");
    assert_eq!(cfg.jwt_secret, "file-loaded-secret");
}

#[test]
fn test_load_from_nonexistent_file_returns_error() {
    let result = ServerConfig::load_from_file(std::path::Path::new("/no/such/file.toml"));
    assert!(result.is_err());
}

// ── Error display ─────────────────────────────────────────────────────────────

#[test]
fn test_config_error_display() {
    let e = ConfigError("something went wrong".to_string());
    assert!(e.to_string().contains("something went wrong"));
}

// ── LogLevel display ──────────────────────────────────────────────────────────

#[test]
fn test_log_level_display() {
    assert_eq!(LogLevel::Info.to_string(), "info");
    assert_eq!(LogLevel::Warn.to_string(), "warn");
    assert_eq!(LogLevel::Debug.to_string(), "debug");
}
