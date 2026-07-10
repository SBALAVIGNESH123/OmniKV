//! Integration tests for OmniKV server configuration.
//!
//! All env-var tests serialise access through ENV_MUTEX to prevent races
//! in parallel test execution.

use omni_engine::config::{ConfigError, DEV_JWT_SECRET, ServerConfig, ServerMode};
use std::sync::Mutex;

static ENV_MUTEX: Mutex<()> = Mutex::new(());

/// Run `f` with env vars set, restoring originals even on panic.
fn with_env<F: FnOnce() -> R, R>(vars: &[(&str, &str)], f: F) -> R {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    let saved: Vec<(&str, Option<String>)> = vars
        .iter()
        .map(|(k, _)| (*k, std::env::var(k).ok()))
        .collect();
    for (k, v) in vars {
        unsafe { std::env::set_var(k, v) };
    }
    struct Restore<'a>(Vec<(&'a str, Option<String>)>);
    impl<'a> Drop for Restore<'a> {
        fn drop(&mut self) {
            for (k, prev) in &self.0 {
                match prev {
                    Some(v) => unsafe { std::env::set_var(k, v) },
                    None => unsafe { std::env::remove_var(k) },
                }
            }
        }
    }
    let _restore = Restore(saved);
    f()
}

// ── defaults ──────────────────────────────────────────────────────────────────

#[test]
fn test_default_mode_is_development() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.mode, ServerMode::Development);
}

#[test]
fn test_default_jwt_is_dev_secret() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.jwt_secret, DEV_JWT_SECRET);
}

#[test]
fn test_default_http_addr() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.http_addr, "127.0.0.1:7070");
}

#[test]
fn test_default_storage_paths() {
    let cfg = ServerConfig::default();
    assert!(cfg.storage.data_dir.contains("data"));
    assert!(cfg.storage.wal_path.contains("wal"));
    assert!(cfg.storage.manifest_path.contains("manifest"));
}

// ── env overrides ─────────────────────────────────────────────────────────────

#[test]
fn test_env_override_jwt_secret() {
    with_env(&[("OMNIKV_JWT_SECRET", "supersecret123456789012345678901")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert_eq!(cfg.jwt_secret, "supersecret123456789012345678901");
    });
}

#[test]
fn test_env_override_http_addr() {
    with_env(&[("OMNIKV_HTTP_ADDR", "0.0.0.0:8080")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert_eq!(cfg.http_addr, "0.0.0.0:8080");
    });
}

#[test]
fn test_env_override_log_level() {
    with_env(&[("OMNIKV_LOG_LEVEL", "debug")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert_eq!(cfg.log_level, "debug");
    });
}

#[test]
fn test_env_override_mode_production() {
    with_env(&[("OMNIKV_MODE", "production")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert_eq!(cfg.mode, ServerMode::Production);
    });
}

#[test]
fn test_env_override_backup_dir() {
    with_env(&[("OMNIKV_BACKUP_DIR", "/var/lib/omnikv/backups")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert_eq!(cfg.storage.backup_dir, "/var/lib/omnikv/backups");
    });
}

#[test]
fn test_env_override_max_open_files() {
    with_env(&[("OMNIKV_MAX_OPEN_FILES", "1024")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert_eq!(cfg.storage.max_open_files, 1024);
    });
}

// ── production validation ─────────────────────────────────────────────────────

#[test]
fn test_production_rejects_dev_secret() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    let err = cfg.validate_production().unwrap_err();
    assert!(matches!(err, ConfigError::ProductionDevSecret));
}

#[test]
fn test_production_rejects_short_secret() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.jwt_secret = "tooshort".into();
    let err = cfg.validate_production().unwrap_err();
    assert!(matches!(err, ConfigError::JwtSecretTooShort { .. }));
}

#[test]
fn test_production_rejects_missing_tls() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.jwt_secret = "a".repeat(32);
    let err = cfg.validate_production().unwrap_err();
    assert!(matches!(err, ConfigError::TlsNotConfigured));
}

#[test]
fn test_production_accepts_insecure_skip() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.jwt_secret = "a".repeat(32);
    cfg.tls.insecure_skip = true;
    assert!(cfg.validate_production().is_ok());
}

#[test]
fn test_production_rejects_missing_cert_file() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.jwt_secret = "a".repeat(32);
    cfg.tls.cert_path = Some("/nonexistent/cert.pem".into());
    cfg.tls.key_path = Some("/nonexistent/key.pem".into());
    let err = cfg.validate_production().unwrap_err();
    assert!(matches!(err, ConfigError::TlsCertMissing { .. }));
}

// ── mode parsing ──────────────────────────────────────────────────────────────

#[test]
fn test_mode_parse_development() {
    let m: ServerMode = "development".parse().unwrap();
    assert_eq!(m, ServerMode::Development);
}

#[test]
fn test_mode_parse_production() {
    let m: ServerMode = "production".parse().unwrap();
    assert_eq!(m, ServerMode::Production);
}

#[test]
fn test_mode_parse_dev_alias() {
    let m: ServerMode = "dev".parse().unwrap();
    assert_eq!(m, ServerMode::Development);
}

#[test]
fn test_mode_parse_invalid() {
    let err = "staging".parse::<ServerMode>();
    assert!(err.is_err());
}

// ── error display ─────────────────────────────────────────────────────────────

#[test]
fn test_error_display_dev_secret() {
    let e = ConfigError::ProductionDevSecret;
    assert!(e.to_string().contains("OMNIKV_JWT_SECRET"));
}

#[test]
fn test_error_display_tls_not_configured() {
    let e = ConfigError::TlsNotConfigured;
    assert!(e.to_string().contains("TLS"));
}

#[test]
fn test_error_display_jwt_too_short() {
    let e = ConfigError::JwtSecretTooShort { len: 8 };
    assert!(e.to_string().contains("8"));
}

// ── storage config ────────────────────────────────────────────────────────────

#[test]
fn test_storage_defaults() {
    let s = omni_engine::config::StorageConfig::default();
    assert_eq!(s.max_open_files, 512);
    assert_eq!(s.write_buffer_mb, 64);
    assert_eq!(s.compaction_workers, 2);
}

// ── load_dev ──────────────────────────────────────────────────────────────────

#[test]
fn test_load_dev_succeeds() {
    let cfg = ServerConfig::load_dev();
    assert_eq!(cfg.mode, ServerMode::Development);
}
