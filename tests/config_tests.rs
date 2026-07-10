#![allow(clippy::field_reassign_with_default)]
use std::sync::Mutex;

use omni_engine::config::{
    ConfigError, DEV_BOOTSTRAP_ADMIN_KEY, DEV_JWT_SECRET, ServerConfig, ServerMode,
};

static ENV_MUTEX: Mutex<()> = Mutex::new(());

fn with_env<F: FnOnce()>(vars: &[(&str, &str)], f: F) {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    let saved: Vec<(&str, Option<String>)> = vars
        .iter()
        .map(|(k, _)| (*k, std::env::var(k).ok()))
        .collect();
    for (k, v) in vars {
        unsafe { std::env::set_var(k, v) };
    }
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

fn with_env_removed<F: FnOnce()>(vars: &[(&str, &str)], removed: &[&str], f: F) {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    let saved_set: Vec<(&str, Option<String>)> = vars
        .iter()
        .map(|(k, _)| (*k, std::env::var(k).ok()))
        .collect();
    let saved_removed: Vec<(&str, Option<String>)> = removed
        .iter()
        .map(|k| (*k, std::env::var(k).ok()))
        .collect();
    for (k, v) in vars {
        unsafe { std::env::set_var(k, v) };
    }
    for k in removed {
        unsafe { std::env::remove_var(k) };
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    for (k, prev) in saved_set.iter().chain(saved_removed.iter()) {
        match prev {
            Some(v) => unsafe { std::env::set_var(k, v) },
            None => unsafe { std::env::remove_var(k) },
        }
    }
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

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
fn test_defaults_bootstrap_admin_key() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.bootstrap_admin_key, DEV_BOOTSTRAP_ADMIN_KEY);
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
    with_env(
        &[("OMNIKV_JWT_SECRET", "my-test-secret-value-here-x")],
        || {
            let mut cfg = ServerConfig::default();
            cfg.apply_env();
            assert_eq!(cfg.jwt_secret, "my-test-secret-value-here-x");
        },
    );
}

#[test]
fn test_env_override_bootstrap_admin_key() {
    with_env(
        &[(
            "OMNIKV_BOOTSTRAP_ADMIN_KEY",
            "bootstrap-admin-key-value-123456",
        )],
        || {
            let mut cfg = ServerConfig::default();
            cfg.apply_env();
            assert_eq!(cfg.bootstrap_admin_key, "bootstrap-admin-key-value-123456");
        },
    );
}

#[test]
fn test_env_legacy_omni_secret_aliases() {
    with_env_removed(
        &[
            ("OMNI_JWT_SECRET", "legacy-jwt-secret-value-12345678"),
            (
                "OMNI_BOOTSTRAP_ADMIN_KEY",
                "legacy-bootstrap-key-value-12345",
            ),
        ],
        &["OMNIKV_JWT_SECRET", "OMNIKV_BOOTSTRAP_ADMIN_KEY"],
        || {
            let mut cfg = ServerConfig::default();
            cfg.apply_env();
            assert_eq!(cfg.jwt_secret, "legacy-jwt-secret-value-12345678");
            assert_eq!(cfg.bootstrap_admin_key, "legacy-bootstrap-key-value-12345");
        },
    );
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

#[test]
fn test_prod_rejects_dev_secret() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.tls_insecure_skip = true;
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("non-default"), "got: {err}");
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
fn test_prod_rejects_default_bootstrap_admin_key() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.jwt_secret = "a-very-long-secret-value-here-ok".into();
    cfg.tls_insecure_skip = true;
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("bootstrap admin key"), "got: {err}");
}

#[test]
fn test_prod_rejects_short_bootstrap_admin_key() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.jwt_secret = "a-very-long-secret-value-here-ok".into();
    cfg.bootstrap_admin_key = "short".into();
    cfg.tls_insecure_skip = true;
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("32 characters"), "got: {err}");
}

#[test]
fn test_prod_rejects_matching_jwt_and_bootstrap_admin_key() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.jwt_secret = "shared-secret-value-long-enough-123".into();
    cfg.bootstrap_admin_key = cfg.jwt_secret.clone();
    cfg.tls_insecure_skip = true;
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("different"), "got: {err}");
}

#[test]
fn test_prod_rejects_missing_tls() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.jwt_secret = "a-very-long-secret-value-here-ok".into();
    cfg.bootstrap_admin_key = "bootstrap-admin-key-value-here-ok".into();
    cfg.tls_insecure_skip = false;
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("TLS"), "got: {err}");
}

#[test]
fn test_prod_accepts_insecure_skip() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.jwt_secret = "a-very-long-secret-value-here-ok".into();
    cfg.bootstrap_admin_key = "bootstrap-admin-key-value-here-ok".into();
    cfg.tls_insecure_skip = true;
    assert!(cfg.validate_production().is_ok());
}

#[test]
fn test_prod_rejects_missing_cert_file() {
    let mut cfg = ServerConfig::default();
    cfg.mode = ServerMode::Production;
    cfg.jwt_secret = "a-very-long-secret-value-here-ok".into();
    cfg.bootstrap_admin_key = "bootstrap-admin-key-value-here-ok".into();
    cfg.tls_cert_path = Some("/nonexistent/cert.pem".into());
    cfg.tls_key_path = Some("/nonexistent/key.pem".into());
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("cert"), "got: {err}");
}

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

#[test]
fn test_config_error_display() {
    let e = ConfigError("something went wrong".into());
    assert_eq!(e.to_string(), "config error: something went wrong");
}

#[test]
fn test_load_dev_succeeds() {
    with_env(&[], || {
        let cfg = ServerConfig::load_dev();
        assert_eq!(cfg.mode, ServerMode::Development);
    });
}
