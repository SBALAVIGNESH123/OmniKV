//! Integration tests for ServerConfig.
//!
//! All env-mutating tests acquire ENV_MUTEX to prevent races in parallel test runs.

use omni_engine::config::{ConfigError, DEV_JWT_SECRET, ServerConfig, ServerMode};
use std::sync::{Mutex, MutexGuard};

/// Global mutex serialising all tests that mutate environment variables.
static ENV_MUTEX: Mutex<()> = Mutex::new(());

/// Run `f` with `vars` set, then restore previous values.
/// Holds `ENV_MUTEX` for the duration and restores even on panic.
fn with_env<F: FnOnce()>(vars: &[(&str, &str)], f: F) {
    let _guard: MutexGuard<()> = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let saved: Vec<(&str, Option<String>)> = vars
        .iter()
        .map(|(k, _)| (*k, std::env::var(k).ok()))
        .collect();
    for (k, v) in vars {
        std::env::set_var(k, v);
    }
    // Use catch_unwind so env vars are restored even if the test panics.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    for (k, prev) in &saved {
        match prev {
            Some(v) => std::env::set_var(k, v),
            None => std::env::remove_var(k),
        }
    }
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

// ── Default values ────────────────────────────────────────────────────────────

#[test]
fn test_defaults_http_addr() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.http_addr, "127.0.0.1:7070");
}

#[test]
fn test_defaults_quic_addr() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.quic_addr, "127.0.0.1:7071");
}

#[test]
fn test_defaults_mode_is_development() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.mode, ServerMode::Development);
}

#[test]
fn test_defaults_jwt_is_dev_secret() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.jwt_secret, DEV_JWT_SECRET);
}

#[test]
fn test_defaults_tls_disabled() {
    let cfg = ServerConfig::default();
    assert!(cfg.tls_cert_path.is_none());
    assert!(cfg.tls_key_path.is_none());
    assert!(!cfg.tls_insecure_skip);
}

#[test]
fn test_defaults_storage_params() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.max_open_files, 512);
    assert_eq!(cfg.write_buffer_mb, 64);
    assert_eq!(cfg.compaction_workers, 2);
}

// ── Environment variable overrides ───────────────────────────────────────────

#[test]
fn test_env_override_http_addr() {
    with_env(&[("OMNIKV_HTTP_ADDR", "0.0.0.0:9090")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert_eq!(cfg.http_addr, "0.0.0.0:9090");
    });
}

#[test]
fn test_env_override_jwt_secret() {
    with_env(&[("OMNIKV_JWT_SECRET", "my-very-long-production-secret-value")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert_eq!(cfg.jwt_secret, "my-very-long-production-secret-value");
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
fn test_env_override_tls_insecure_skip_true() {
    with_env(&[("OMNIKV_TLS_INSECURE_SKIP", "true")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert!(cfg.tls_insecure_skip);
    });
}

#[test]
fn test_env_override_tls_cert_path() {
    with_env(&[("OMNIKV_TLS_CERT_PATH", "/etc/ssl/cert.pem")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert_eq!(cfg.tls_cert_path, Some("/etc/ssl/cert.pem".to_string()));
    });
}

#[test]
fn test_env_override_max_open_files() {
    with_env(&[("OMNIKV_MAX_OPEN_FILES", "1024")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert_eq!(cfg.max_open_files, 1024);
    });
}

// ── Production validation ─────────────────────────────────────────────────────

#[test]
fn test_prod_rejects_dev_jwt_secret() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.tls_insecure_skip = true;
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("development default"), "{}", err.0);
}

#[test]
fn test_prod_rejects_short_jwt_secret() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.jwt_secret = "tooshort".to_string();
    cfg.tls_insecure_skip = true;
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("32 characters"), "{}", err.0);
}

#[test]
fn test_prod_rejects_missing_tls() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.jwt_secret = "a-long-enough-production-secret-32+chars".to_string();
    cfg.tls_insecure_skip = false;
    cfg.tls_cert_path = None;
    cfg.tls_key_path = None;
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("TLS"), "{}", err.0);
}

#[test]
fn test_prod_accepts_insecure_skip() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.jwt_secret = "a-long-enough-production-secret-32+chars".to_string();
    cfg.tls_insecure_skip = true;
    assert!(cfg.validate_production().is_ok());
}

#[test]
fn test_prod_rejects_missing_cert_file() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.jwt_secret = "a-long-enough-production-secret-32+chars".to_string();
    cfg.tls_insecure_skip = false;
    cfg.tls_cert_path = Some("/nonexistent/cert.pem".to_string());
    cfg.tls_key_path = Some("/nonexistent/key.pem".to_string());
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("cert file not found"), "{}", err.0);
}

// ── ServerMode display ────────────────────────────────────────────────────────

#[test]
fn test_mode_display_development() {
    assert_eq!(ServerMode::Development.to_string(), "development");
}

#[test]
fn test_mode_display_production() {
    assert_eq!(ServerMode::Production.to_string(), "production");
}

// ── ConfigError display ───────────────────────────────────────────────────────

#[test]
fn test_config_error_display() {
    let e = ConfigError("test error".to_string());
    assert_eq!(e.to_string(), "ConfigError: test error");
}

// ── load_dev ──────────────────────────────────────────────────────────────────

#[test]
fn test_load_dev_mode_is_development() {
    with_env(&[], || {
        let cfg = ServerConfig::load_dev();
        assert_eq!(cfg.mode, ServerMode::Development);
    });
}
