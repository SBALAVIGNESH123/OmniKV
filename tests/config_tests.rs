use omni_engine::config::{ConfigError, ServerConfig, ServerMode, DEV_JWT_SECRET};
use std::sync::Mutex;

/// Serialize all env-mutating tests through this lock.
static ENV_MUTEX: Mutex<()> = Mutex::new(());

/// Helper: set env vars, run closure, restore previous values.
/// Panic-safe: uses a drop guard to ensure restoration.
fn with_env<F: FnOnce()>(vars: &[(&str, &str)], f: F) {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    let saved: Vec<(&str, Option<String>)> = vars
        .iter()
        .map(|(k, _)| (*k, std::env::var(k).ok()))
        .collect();
    for (k, v) in vars {
        // SAFETY: single-threaded via ENV_MUTEX
        unsafe { std::env::set_var(k, v) };
    }
    // Use catch_unwind so we always restore even on panic
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    for (k, prev) in &saved {
        match prev {
            Some(v) => unsafe { std::env::set_var(k, v) },
            None => unsafe { std::env::remove_var(k) },
        }
    }
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

// ── Defaults ─────────────────────────────────────────────────────────────────

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
fn test_defaults_pgwire_addr() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.pgwire_addr, "127.0.0.1:5432");
}

#[test]
fn test_defaults_tcp_addr() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.tcp_addr, "127.0.0.1:7072");
}

#[test]
fn test_defaults_jwt_secret() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.jwt_secret, DEV_JWT_SECRET);
}

#[test]
fn test_defaults_mode_is_development() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.mode, ServerMode::Development);
}

#[test]
fn test_defaults_tls_disabled() {
    let cfg = ServerConfig::default();
    assert!(cfg.tls_cert_path.is_none());
    assert!(cfg.tls_key_path.is_none());
    assert!(!cfg.tls_insecure_skip);
}

// ── Env overrides ─────────────────────────────────────────────────────────────

#[test]
fn test_env_override_http_addr() {
    with_env(&[("OMNIKV_HTTP_ADDR", "0.0.0.0:8080")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert_eq!(cfg.http_addr, "0.0.0.0:8080");
    });
}

#[test]
fn test_env_override_jwt_secret() {
    with_env(&[("OMNIKV_JWT_SECRET", "my-test-secret-value-here-x")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert_eq!(cfg.jwt_secret, "my-test-secret-value-here-x");
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
fn test_env_override_tls_insecure_skip() {
    with_env(&[("OMNIKV_TLS_INSECURE_SKIP", "true")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert!(cfg.tls_insecure_skip);
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
fn test_env_override_storage_manifest() {
    with_env(&[("OMNIKV_MANIFEST_PATH", "/data/manifest.json")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert_eq!(cfg.storage.manifest_path, "/data/manifest.json");
    });
}

#[test]
fn test_env_override_backup_dir() {
    with_env(&[("OMNIKV_BACKUP_DIR", "/backups")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env();
        assert_eq!(cfg.storage.backup_dir, "/backups");
    });
}

// ── Production validation ─────────────────────────────────────────────────────

#[test]
fn test_prod_rejects_dev_secret() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.tls_insecure_skip = true;
    let err = cfg.validate_production().unwrap_err();
    assert!(
        err.0.contains("non-default"),
        "expected non-default error, got: {err}"
    );
}

#[test]
fn test_prod_rejects_short_secret() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.jwt_secret = "short".into();
    cfg.tls_insecure_skip = true;
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("32 characters"), "got: {err}");
}

#[test]
fn test_prod_rejects_missing_tls() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.jwt_secret = "a-very-long-secret-value-here-ok".into();
    cfg.tls_insecure_skip = false;
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("TLS"), "got: {err}");
}

#[test]
fn test_prod_accepts_insecure_skip() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.jwt_secret = "a-very-long-secret-value-here-ok".into();
    cfg.tls_insecure_skip = true;
    assert!(cfg.validate_production().is_ok());
}

#[test]
fn test_prod_rejects_missing_cert_file() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.jwt_secret = "a-very-long-secret-value-here-ok".into();
    cfg.tls_cert_path = Some("/nonexistent/cert.pem".into());
    cfg.tls_key_path = Some("/nonexistent/key.pem".into());
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("cert"), "got: {err}");
}

// ── Mode parsing ──────────────────────────────────────────────────────────────

#[test]
fn test_mode_display_development() {
    assert_eq!(ServerMode::Development.to_string(), "development");
}

#[test]
fn test_mode_display_production() {
    assert_eq!(ServerMode::Production.to_string(), "production");
}

#[test]
fn test_mode_parse_prod() {
    let m: ServerMode = "production".parse().unwrap();
    assert_eq!(m, ServerMode::Production);
}

#[test]
fn test_mode_parse_dev() {
    let m: ServerMode = "development".parse().unwrap();
    assert_eq!(m, ServerMode::Development);
}

#[test]
fn test_mode_parse_unknown() {
    let r: Result<ServerMode, _> = "staging".parse();
    assert!(r.is_err());
}

// ── Error display ─────────────────────────────────────────────────────────────

#[test]
fn test_config_error_display() {
    let e = ConfigError("something went wrong".into());
    assert_eq!(e.to_string(), "config error: something went wrong");
}

// ── load_dev serialization ────────────────────────────────────────────────────

#[test]
fn test_load_dev_succeeds() {
    // Must hold ENV_MUTEX to prevent mode leaking from parallel env tests
    with_env(&[], || {
        let cfg = ServerConfig::load_dev();
        assert_eq!(cfg.mode, ServerMode::Development);
    });
}
