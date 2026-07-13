use std::sync::Mutex;

use omni_engine::config::{
    ConfigError, DEV_BOOTSTRAP_ADMIN_KEY, DEV_JWT_SECRET, ServerConfig, ServerMode,
};

static ENV_MUTEX: Mutex<()> = Mutex::new(());

fn with_env<F: FnOnce()>(vars: &[(&str, &str)], f: F) {
    let _guard = ENV_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
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
    let _guard = ENV_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
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
fn test_defaults_rate_limits_are_enabled() {
    let cfg = ServerConfig::default();
    assert!((cfg.rate_limit_per_sec - 1000.0).abs() < f64::EPSILON);
    assert_eq!(cfg.rate_limit_burst, 100);
    assert_eq!(cfg.rate_limit_max_users, 10_000);
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
        cfg.apply_env().unwrap();
        assert_eq!(cfg.http_addr, "0.0.0.0:8080");
    });
}

#[test]
fn test_env_override_jwt_secret() {
    with_env(
        &[("OMNIKV_JWT_SECRET", "my-test-secret-value-here-x")],
        || {
            let mut cfg = ServerConfig::default();
            cfg.apply_env().unwrap();
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
            cfg.apply_env().unwrap();
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
            cfg.apply_env().unwrap();
            assert_eq!(cfg.jwt_secret, "legacy-jwt-secret-value-12345678");
            assert_eq!(cfg.bootstrap_admin_key, "legacy-bootstrap-key-value-12345");
        },
    );
}

#[test]
fn test_env_override_log_level() {
    with_env(&[("OMNIKV_LOG_LEVEL", "debug")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env().unwrap();
        assert_eq!(cfg.log_level, "debug");
    });
}

#[test]
fn test_env_override_tls_insecure_skip() {
    with_env(&[("OMNIKV_TLS_INSECURE_SKIP", "true")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env().unwrap();
        assert!(cfg.tls_insecure_skip);
    });
}

#[test]
fn test_env_override_mode_production() {
    with_env(&[("OMNIKV_MODE", "production")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env().unwrap();
        assert_eq!(cfg.mode, ServerMode::Production);
    });
}

#[test]
fn test_env_override_rate_limits() {
    with_env(
        &[
            ("OMNIKV_RATE_LIMIT_PER_SEC", "42.5"),
            ("OMNIKV_RATE_LIMIT_BURST", "9"),
            ("OMNIKV_RATE_LIMIT_MAX_USERS", "1234"),
        ],
        || {
            let mut cfg = ServerConfig::default();
            cfg.apply_env().unwrap();
            assert!((cfg.rate_limit_per_sec - 42.5).abs() < f64::EPSILON);
            assert_eq!(cfg.rate_limit_burst, 9);
            assert_eq!(cfg.rate_limit_max_users, 1234);
        },
    );
}

#[test]
fn test_env_legacy_rate_limit_aliases() {
    with_env_removed(
        &[("OMNI_RATE_LIMIT", "24"), ("OMNI_RATE_BURST", "8")],
        &["OMNIKV_RATE_LIMIT_PER_SEC", "OMNIKV_RATE_LIMIT_BURST"],
        || {
            let mut cfg = ServerConfig::default();
            cfg.apply_env().unwrap();
            assert!((cfg.rate_limit_per_sec - 24.0).abs() < f64::EPSILON);
            assert_eq!(cfg.rate_limit_burst, 8);
        },
    );
}

#[test]
fn test_env_override_storage_manifest() {
    with_env(&[("OMNIKV_MANIFEST_PATH", "/data/manifest.json")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env().unwrap();
        assert_eq!(cfg.storage.manifest_path, "/data/manifest.json");
    });
}

#[test]
fn test_env_override_data_dir_derives_storage_paths() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_string_lossy().to_string();
    with_env(&[("OMNIKV_DATA_DIR", &data_dir)], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env().unwrap();
        assert_eq!(
            std::path::PathBuf::from(&cfg.storage.manifest_path),
            dir.path().join("manifest.json")
        );
        assert_eq!(
            std::path::PathBuf::from(&cfg.storage.wal_path),
            dir.path().join("wal.bin")
        );
        assert_eq!(
            std::path::PathBuf::from(&cfg.storage.backup_dir),
            dir.path().join("backups")
        );
    });
}

#[test]
fn test_env_override_storage_numeric_settings() {
    with_env(
        &[
            ("OMNIKV_MAX_OPEN_FILES", "2048"),
            ("OMNIKV_WRITE_BUFFER_MB", "128"),
            ("OMNIKV_COMPACTION_WORKERS", "8"),
        ],
        || {
            let mut cfg = ServerConfig::default();
            cfg.apply_env().unwrap();
            assert_eq!(cfg.storage.max_open_files, 2048);
            assert_eq!(cfg.storage.write_buffer_mb, 128);
            assert_eq!(cfg.storage.compaction_workers, 8);
        },
    );
}

#[test]
fn test_env_override_backup_dir() {
    with_env(&[("OMNIKV_BACKUP_DIR", "/backups")], || {
        let mut cfg = ServerConfig::default();
        cfg.apply_env().unwrap();
        assert_eq!(cfg.storage.backup_dir, "/backups");
    });
}

#[test]
fn test_invalid_numeric_env_fails_closed() {
    with_env(&[("OMNIKV_RATE_LIMIT_BURST", "not-a-number")], || {
        let mut cfg = ServerConfig::default();
        let err = cfg.apply_env().unwrap_err();
        assert!(err.0.contains("OMNIKV_RATE_LIMIT_BURST"), "got: {err}");
    });
}

#[test]
fn test_invalid_bool_env_fails_closed() {
    with_env(&[("OMNIKV_TLS_INSECURE_SKIP", "sometimes")], || {
        let mut cfg = ServerConfig::default();
        let err = cfg.apply_env().unwrap_err();
        assert!(err.0.contains("OMNIKV_TLS_INSECURE_SKIP"), "got: {err}");
    });
}

#[test]
fn test_invalid_storage_numeric_env_fails_closed() {
    with_env(&[("OMNIKV_COMPACTION_WORKERS", "many")], || {
        let mut cfg = ServerConfig::default();
        let err = cfg.apply_env().unwrap_err();
        assert!(err.0.contains("OMNIKV_COMPACTION_WORKERS"), "got: {err}");
    });
}

#[test]
fn test_legacy_omni_config_env_is_honored() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("omni.toml");
    std::fs::write(&config, "http_addr = \"127.0.0.1:9191\"\n").unwrap();

    with_env_removed(
        &[("OMNI_CONFIG", config.to_str().unwrap())],
        &["OMNIKV_CONFIG"],
        || {
            let cfg = ServerConfig::load_server_from_args(std::iter::empty::<String>()).unwrap();
            assert_eq!(cfg.http_addr, "127.0.0.1:9191");
        },
    );
}

#[test]
fn test_cli_config_path_wins_over_env_config_path() {
    let dir = tempfile::tempdir().unwrap();
    let env_config = dir.path().join("env.toml");
    let cli_config = dir.path().join("cli.toml");
    std::fs::write(&env_config, "http_addr = \"127.0.0.1:9292\"\n").unwrap();
    std::fs::write(&cli_config, "http_addr = \"127.0.0.1:9393\"\n").unwrap();

    with_env(&[("OMNIKV_CONFIG", env_config.to_str().unwrap())], || {
        let cfg = ServerConfig::load_server_from_args([
            "--config".to_string(),
            cli_config.to_string_lossy().to_string(),
        ])
        .unwrap();
        assert_eq!(cfg.http_addr, "127.0.0.1:9393");
    });
}

#[test]
fn test_env_values_override_config_file_values() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("omni.toml");
    std::fs::write(&config, "http_addr = \"127.0.0.1:9494\"\n").unwrap();

    with_env(
        &[
            ("OMNIKV_CONFIG", config.to_str().unwrap()),
            ("OMNIKV_HTTP_ADDR", "127.0.0.1:9595"),
        ],
        || {
            let cfg = ServerConfig::load_server_from_args(std::iter::empty::<String>()).unwrap();
            assert_eq!(cfg.http_addr, "127.0.0.1:9595");
        },
    );
}

#[test]
fn test_unknown_config_key_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("bad.toml");
    std::fs::write(&config, "unknown_key = true\n").unwrap();

    let err = ServerConfig::load_server_from_args([
        "--config".to_string(),
        config.to_string_lossy().to_string(),
    ])
    .unwrap_err();
    assert!(err.0.contains("unknown"), "got: {err}");
}

#[test]
fn test_invalid_config_file_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("bad.toml");
    std::fs::write(&config, "http_addr = [not valid toml\n").unwrap();

    let err = ServerConfig::load_server_from_args([
        "--config".to_string(),
        config.to_string_lossy().to_string(),
    ])
    .unwrap_err();
    assert!(err.0.contains("failed to parse config file"), "got: {err}");
}

#[test]
fn test_missing_explicit_config_file_fails_closed() {
    with_env_removed(
        &[("OMNI_CONFIG", "definitely-not-present.toml")],
        &["OMNIKV_CONFIG"],
        || {
            let err =
                ServerConfig::load_server_from_args(std::iter::empty::<String>()).unwrap_err();
            assert!(err.0.contains("failed to read config file"), "got: {err}");
        },
    );
}

#[test]
fn test_prod_rejects_dev_secret() {
    let cfg = ServerConfig {
        mode: ServerMode::Production,
        tls_insecure_skip: true,
        ..Default::default()
    };
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("non-default"), "got: {err}");
}

#[test]
fn test_prod_rejects_short_secret() {
    let cfg = ServerConfig {
        mode: ServerMode::Production,
        jwt_secret: "short".into(),
        tls_insecure_skip: true,
        ..Default::default()
    };
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("32 characters"), "got: {err}");
}

#[test]
fn test_prod_rejects_default_bootstrap_admin_key() {
    let cfg = ServerConfig {
        mode: ServerMode::Production,
        jwt_secret: "a-very-long-secret-value-here-ok".into(),
        tls_insecure_skip: true,
        ..Default::default()
    };
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("bootstrap admin key"), "got: {err}");
}

#[test]
fn test_prod_rejects_short_bootstrap_admin_key() {
    let cfg = ServerConfig {
        mode: ServerMode::Production,
        jwt_secret: "a-very-long-secret-value-here-ok".into(),
        bootstrap_admin_key: "short".into(),
        tls_insecure_skip: true,
        ..Default::default()
    };
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("32 characters"), "got: {err}");
}

#[test]
fn test_prod_rejects_matching_jwt_and_bootstrap_admin_key() {
    let shared_secret = "shared-secret-value-long-enough-123";
    let cfg = ServerConfig {
        mode: ServerMode::Production,
        jwt_secret: shared_secret.into(),
        bootstrap_admin_key: shared_secret.into(),
        tls_insecure_skip: true,
        ..Default::default()
    };
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("different"), "got: {err}");
}

#[test]
fn test_prod_rejects_missing_tls() {
    let cfg = ServerConfig {
        mode: ServerMode::Production,
        jwt_secret: "a-very-long-secret-value-here-ok".into(),
        bootstrap_admin_key: "bootstrap-admin-key-value-here-ok".into(),
        tls_insecure_skip: false,
        ..Default::default()
    };
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("TLS"), "got: {err}");
}

#[test]
fn test_prod_rejects_disabled_rate_per_second() {
    let cfg = ServerConfig {
        mode: ServerMode::Production,
        jwt_secret: "a-very-long-secret-value-here-ok".into(),
        bootstrap_admin_key: "bootstrap-admin-key-value-here-ok".into(),
        tls_insecure_skip: true,
        rate_limit_per_sec: 0.0,
        ..Default::default()
    };
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("RATE_LIMIT_PER_SEC"), "got: {err}");
}

#[test]
fn test_prod_rejects_disabled_rate_limit_burst() {
    let cfg = ServerConfig {
        mode: ServerMode::Production,
        jwt_secret: "a-very-long-secret-value-here-ok".into(),
        bootstrap_admin_key: "bootstrap-admin-key-value-here-ok".into(),
        tls_insecure_skip: true,
        rate_limit_burst: 0,
        ..Default::default()
    };
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("RATE_LIMIT_BURST"), "got: {err}");
}

#[test]
fn test_prod_rejects_disabled_rate_limit_identity_capacity() {
    let cfg = ServerConfig {
        mode: ServerMode::Production,
        jwt_secret: "a-very-long-secret-value-here-ok".into(),
        bootstrap_admin_key: "bootstrap-admin-key-value-here-ok".into(),
        tls_insecure_skip: true,
        rate_limit_max_users: 0,
        ..Default::default()
    };
    let err = cfg.validate_production().unwrap_err();
    assert!(err.0.contains("RATE_LIMIT_MAX_USERS"), "got: {err}");
}

#[test]
fn test_prod_accepts_insecure_skip() {
    let cfg = ServerConfig {
        mode: ServerMode::Production,
        jwt_secret: "a-very-long-secret-value-here-ok".into(),
        bootstrap_admin_key: "bootstrap-admin-key-value-here-ok".into(),
        tls_insecure_skip: true,
        ..Default::default()
    };
    assert!(cfg.validate_production().is_ok());
}

#[test]
fn test_prod_rejects_missing_cert_file() {
    let cfg = ServerConfig {
        mode: ServerMode::Production,
        jwt_secret: "a-very-long-secret-value-here-ok".into(),
        bootstrap_admin_key: "bootstrap-admin-key-value-here-ok".into(),
        tls_cert_path: Some("/nonexistent/cert.pem".into()),
        tls_key_path: Some("/nonexistent/key.pem".into()),
        ..Default::default()
    };
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
    with_env_removed(&[], &["OMNIKV_CONFIG", "OMNI_CONFIG"], || {
        let cfg = ServerConfig::load_dev().unwrap();
        assert_eq!(cfg.mode, ServerMode::Development);
    });
}
