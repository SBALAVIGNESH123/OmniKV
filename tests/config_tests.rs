//! Integration tests for OmniKV production configuration.

use omnikv::config::{ConfigBuilder, ConfigError, DEV_JWT_SECRET, ServerMode};
use std::env;

// Helper: run a closure with env vars set, restore afterwards.
fn with_env<F: FnOnce()>(vars: &[(&str, &str)], f: F) {
    for (k, v) in vars { env::set_var(k, v); }
    f();
    for (k, _) in vars { env::remove_var(k); }
}

// ── Development mode (default) ───────────────────────────────────────────────

#[test]
fn test_dev_mode_default() {
    let cfg = ConfigBuilder::new().build().unwrap();
    assert_eq!(cfg.mode, ServerMode::Development);
    assert_eq!(cfg.port, 7070);
    assert_eq!(cfg.jwt_secret, DEV_JWT_SECRET);
}

#[test]
fn test_dev_mode_allows_default_secret() {
    // Development mode must not reject the default secret.
    let cfg = ConfigBuilder::new().build();
    assert!(cfg.is_ok(), "dev mode must accept default secret");
}

// ── Environment variable overrides ───────────────────────────────────────────

#[test]
fn test_env_override_port() {
    with_env(&[("OMNIKV_PORT", "9999")], || {
        let cfg = ConfigBuilder::new().apply_env().build().unwrap();
        assert_eq!(cfg.port, 9999);
    });
}

#[test]
fn test_env_override_host() {
    with_env(&[("OMNIKV_HOST", "0.0.0.0")], || {
        let cfg = ConfigBuilder::new().apply_env().build().unwrap();
        assert_eq!(cfg.host, "0.0.0.0");
    });
}

#[test]
fn test_env_override_log_level() {
    with_env(&[("OMNIKV_LOG_LEVEL", "debug")], || {
        let cfg = ConfigBuilder::new().apply_env().build().unwrap();
        assert_eq!(cfg.log_level, "debug");
    });
}

#[test]
fn test_env_override_data_dir() {
    with_env(&[("OMNIKV_DATA_DIR", "/tmp/omnikv_test_data")], || {
        let cfg = ConfigBuilder::new().apply_env().build().unwrap();
        assert_eq!(cfg.data_dir.to_str().unwrap(), "/tmp/omnikv_test_data");
    });
}

#[test]
fn test_env_override_jwt_secret() {
    with_env(&[("OMNIKV_JWT_SECRET", "my-custom-secret-value")], || {
        let cfg = ConfigBuilder::new().apply_env().build().unwrap();
        assert_eq!(cfg.jwt_secret, "my-custom-secret-value");
    });
}

#[test]
fn test_env_override_mode_production_str() {
    with_env(&[
        ("OMNIKV_MODE", "production"),
        ("OMNIKV_JWT_SECRET", "a-very-long-production-secret-value-here"),
        ("OMNIKV_TLS_INSECURE_SKIP", "true"),
    ], || {
        let cfg = ConfigBuilder::new().apply_env().build().unwrap();
        assert_eq!(cfg.mode, ServerMode::Production);
    });
}

// ── Production mode validation ───────────────────────────────────────────────

#[test]
fn test_production_rejects_default_secret() {
    with_env(&[
        ("OMNIKV_MODE", "production"),
        ("OMNIKV_TLS_INSECURE_SKIP", "true"),
    ], || {
        // jwt_secret is still the default
        let result = ConfigBuilder::new().apply_env().build();
        assert!(result.is_err());
        let msg = result.unwrap_err().0;
        assert!(msg.contains("JWT"), "error must mention JWT: {}", msg);
    });
}

#[test]
fn test_production_rejects_short_secret() {
    with_env(&[
        ("OMNIKV_MODE", "production"),
        ("OMNIKV_JWT_SECRET", "short"),
        ("OMNIKV_TLS_INSECURE_SKIP", "true"),
    ], || {
        let result = ConfigBuilder::new().apply_env().build();
        assert!(result.is_err());
        let msg = result.unwrap_err().0;
        assert!(msg.contains("32"), "error must mention 32: {}", msg);
    });
}

#[test]
fn test_production_rejects_missing_tls() {
    with_env(&[
        ("OMNIKV_MODE", "production"),
        ("OMNIKV_JWT_SECRET", "a-very-long-production-secret-value-here"),
    ], || {
        let result = ConfigBuilder::new().apply_env().build();
        assert!(result.is_err());
        let msg = result.unwrap_err().0;
        assert!(msg.contains("TLS"), "error must mention TLS: {}", msg);
    });
}

#[test]
fn test_production_tls_insecure_skip_allowed() {
    with_env(&[
        ("OMNIKV_MODE", "production"),
        ("OMNIKV_JWT_SECRET", "a-very-long-production-secret-value-here"),
        ("OMNIKV_TLS_INSECURE_SKIP", "true"),
    ], || {
        let result = ConfigBuilder::new().apply_env().build();
        assert!(result.is_ok(), "tls_insecure_skip must be accepted: {:?}", result);
    });
}

#[test]
fn test_production_tls_missing_cert_file() {
    with_env(&[
        ("OMNIKV_MODE", "production"),
        ("OMNIKV_JWT_SECRET", "a-very-long-production-secret-value-here"),
        ("OMNIKV_TLS_CERT", "/nonexistent/cert.pem"),
        ("OMNIKV_TLS_KEY",  "/nonexistent/key.pem"),
    ], || {
        let result = ConfigBuilder::new().apply_env().build();
        assert!(result.is_err());
        let msg = result.unwrap_err().0;
        assert!(msg.contains("not found"), "error must mention not found: {}", msg);
    });
}

// ── Mode parsing ─────────────────────────────────────────────────────────────

#[test]
fn test_mode_from_env_dev() {
    with_env(&[("OMNIKV_MODE", "dev")], || {
        let cfg = ConfigBuilder::new().apply_env().build().unwrap();
        assert_eq!(cfg.mode, ServerMode::Development);
    });
}

#[test]
fn test_mode_from_env_prod() {
    with_env(&[
        ("OMNIKV_MODE", "prod"),
        ("OMNIKV_JWT_SECRET", "a-very-long-production-secret-value-here"),
        ("OMNIKV_TLS_INSECURE_SKIP", "true"),
    ], || {
        let cfg = ConfigBuilder::new().apply_env().build().unwrap();
        assert_eq!(cfg.mode, ServerMode::Production);
    });
}

// ── Storage tuning params ────────────────────────────────────────────────────

#[test]
fn test_storage_params_default() {
    let cfg = ConfigBuilder::new().build().unwrap();
    assert_eq!(cfg.max_open_files, 512);
    assert_eq!(cfg.write_buffer_mb, 64);
    assert_eq!(cfg.compaction_workers, 2);
}

#[test]
fn test_storage_params_env_override() {
    with_env(&[
        ("OMNIKV_MAX_OPEN_FILES", "1024"),
        ("OMNIKV_WRITE_BUFFER_MB", "128"),
        ("OMNIKV_COMPACTION_WORKERS", "4"),
    ], || {
        let cfg = ConfigBuilder::new().apply_env().build().unwrap();
        assert_eq!(cfg.max_open_files, 1024);
        assert_eq!(cfg.write_buffer_mb, 128);
        assert_eq!(cfg.compaction_workers, 4);
    });
}

// ── is_production helper ──────────────────────────────────────────────────────

#[test]
fn test_is_production_flag() {
    let cfg = ConfigBuilder::new().build().unwrap();
    assert!(!cfg.mode.is_production());
}

// ── ConfigError display ───────────────────────────────────────────────────────

#[test]
fn test_config_error_display() {
    let e = ConfigError("test error".to_string());
    let s = format!("{}", e);
    assert!(s.contains("test error"));
}
