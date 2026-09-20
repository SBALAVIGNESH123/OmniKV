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
            ("OMNIKV_MEMTABLE_FLUSH_THRESHOLD", "4096"),
            ("OMNIKV_L0_COMPACTION_TRIGGER", "3"),
            ("OMNIKV_L1_COMPACTION_TRIGGER", "5"),
            ("OMNIKV_L0_WRITE_STALL_THRESHOLD", "9"),
            ("OMNIKV_WRITE_STALL_WAIT_ATTEMPTS", "7"),
            ("OMNIKV_WRITE_STALL_WAIT_MS", "25"),
            ("OMNIKV_COMPACTION_CHECK_INTERVAL_MS", "250"),
        ],
        || {
            let mut cfg = ServerConfig::default();
            cfg.apply_env().unwrap();
            assert_eq!(cfg.storage.max_open_files, 2048);
            assert_eq!(cfg.storage.write_buffer_mb, 128);
            assert_eq!(cfg.storage.compaction_workers, 8);
            assert_eq!(cfg.storage.memtable_flush_threshold, 4096);
            assert_eq!(cfg.storage.l0_compaction_trigger, 3);
            assert_eq!(cfg.storage.l1_compaction_trigger, 5);
            assert_eq!(cfg.storage.l0_write_stall_threshold, 9);
            assert_eq!(cfg.storage.write_stall_wait_attempts, 7);
            assert_eq!(cfg.storage.write_stall_wait_ms, 25);
            assert_eq!(cfg.storage.compaction_check_interval_ms, 250);
            assert_eq!(cfg.storage.compaction_policy().l0_compaction_trigger, 3);
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
fn test_invalid_compaction_thresholds_fail_closed() {
    let cfg = ServerConfig {
        storage: omni_engine::config::StorageConfig {
            l0_compaction_trigger: 4,
            l0_write_stall_threshold: 4,
            ..Default::default()
        },
        ..Default::default()
    };
    let err = cfg.validate_runtime().unwrap_err();
    assert!(err.0.contains("l0_write_stall_threshold"), "got: {err}");
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
fn test_partial_storage_config_uses_defaults_for_omitted_fields() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("omni.toml");
    std::fs::write(
        &config,
        "[storage]\nmanifest_path = \"/tmp/omnikv-manifest.json\"\n",
    )
    .unwrap();

    let cfg = ServerConfig::load_server_from_args([
        "--config".to_string(),
        config.to_string_lossy().to_string(),
    ])
    .unwrap();

    assert_eq!(cfg.storage.manifest_path, "/tmp/omnikv-manifest.json");
    assert_eq!(cfg.storage.l0_compaction_trigger, 4);
    assert_eq!(cfg.storage.l0_write_stall_threshold, 12);
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

// ── Cluster advertised-address validation (PR #127 review) ──
// A wildcard ADVERTISED address must fail closed: peers would dial
// 0.0.0.0:port, which resolves to the DIALER itself, silently breaking
// replication/votes toward this node after a failover or rejoin.

#[test]
fn test_raft_wildcard_bind_without_advertise_is_refused() {
    // The wildcard BIND alone would also be the advertised address.
    let cfg = ServerConfig {
        raft: omni_engine::config::RaftConfig {
            node_id: Some(1),
            raft_addr: Some("0.0.0.0:9090".into()),
            ..Default::default()
        },
        ..Default::default()
    };
    let err = cfg.validate_runtime().unwrap_err();
    assert!(
        err.0.contains("OMNIKV_RAFT_ADVERTISE_ADDR"),
        "wildcard bind without advertise must name the fix: {err}"
    );
}

#[test]
fn test_raft_wildcard_bind_with_advertise_is_accepted() {
    // The container pattern: bind wide, advertise the routable hostname.
    let cfg = ServerConfig {
        raft: omni_engine::config::RaftConfig {
            node_id: Some(1),
            raft_addr: Some("0.0.0.0:9090".into()),
            advertise_addr: Some("omni-node-1:9090".parse().unwrap()),
            ..Default::default()
        },
        ..Default::default()
    };
    cfg.validate_runtime()
        .unwrap_or_else(|e| panic!("wildcard bind + routable advertise must boot: {e}"));
}

#[test]
fn test_raft_wildcard_advertise_override_is_refused() {
    // An explicit wildcard advertise is never routable, even when the
    // bind is fine.
    let cfg = ServerConfig {
        raft: omni_engine::config::RaftConfig {
            node_id: Some(1),
            raft_addr: Some("127.0.0.1:9090".into()),
            advertise_addr: Some("0.0.0.0:9090".into()),
            ..Default::default()
        },
        ..Default::default()
    };
    let err = cfg.validate_runtime().unwrap_err();
    assert!(err.0.contains("routable"), "got: {err}");
}

#[test]
fn test_raft_loopback_bind_without_advertise_is_accepted() {
    // The single-host/test pattern (what cluster_multiprocess uses):
    // 127.0.0.1 is routable by peers on the same host, no override
    // needed.
    let cfg = ServerConfig {
        raft: omni_engine::config::RaftConfig {
            node_id: Some(1),
            raft_addr: Some("127.0.0.1:9090".into()),
            ..Default::default()
        },
        ..Default::default()
    };
    cfg.validate_runtime()
        .unwrap_or_else(|e| panic!("loopback bind must not require advertise: {e}"));
}

#[test]
fn test_raft_advertise_env_var_sets_field() {
    with_env(
        &[("OMNIKV_RAFT_ADVERTISE_ADDR", "omni-node-2:9090")],
        || {
            let mut cfg = ServerConfig::default();
            cfg.apply_env().unwrap();
            assert_eq!(cfg.raft.advertise_addr.as_deref(), Some("omni-node-2:9090"));
        },
    );
}

// ── Peer/address sanity (PR #127 review round 4) ──

#[test]
fn test_raft_advertise_port_zero_is_refused() {
    // Port 0 is "ephemeral" to a bind, but advertised it tells peers to
    // dial a random port — never reachable.
    let cfg = ServerConfig {
        raft: omni_engine::config::RaftConfig {
            node_id: Some(1),
            raft_addr: Some("127.0.0.1:9090".into()),
            advertise_addr: Some("omni-node-1:0".into()),
            ..Default::default()
        },
        ..Default::default()
    };
    let err = cfg.validate_runtime().unwrap_err();
    assert!(
        err.0.contains("advertise_addr"),
        "port-0 advertise must be rejected: {err}"
    );
}

#[test]
fn test_raft_duplicate_peers_are_refused() {
    // Two member ids pointing at one address breaks quorum arithmetic.
    let cfg = ServerConfig {
        raft: omni_engine::config::RaftConfig {
            node_id: Some(1),
            raft_addr: Some("127.0.0.1:9090".into()),
            peers: vec!["127.0.0.1:9091".into(), "127.0.0.1:9091".into()],
            ..Default::default()
        },
        ..Default::default()
    };
    let err = cfg.validate_runtime().unwrap_err();
    assert!(
        err.0.contains("unique"),
        "duplicate peers must be rejected: {err}"
    );
}

#[test]
fn test_raft_self_in_peers_is_refused() {
    // A peer list containing this node's own advertised address routes a
    // member id back to itself.
    let cfg = ServerConfig {
        raft: omni_engine::config::RaftConfig {
            node_id: Some(1),
            raft_addr: Some("127.0.0.1:9090".into()),
            peers: vec!["127.0.0.1:9090".into(), "127.0.0.1:9091".into()],
            ..Default::default()
        },
        ..Default::default()
    };
    let err = cfg.validate_runtime().unwrap_err();
    assert!(
        err.0.contains("own advertised"),
        "self-referential peer must be rejected: {err}"
    );
}

#[test]
fn test_raft_distinct_peers_are_accepted() {
    let cfg = ServerConfig {
        raft: omni_engine::config::RaftConfig {
            node_id: Some(1),
            raft_addr: Some("127.0.0.1:9090".into()),
            peers: vec!["127.0.0.1:9091".into(), "127.0.0.1:9092".into()],
            ..Default::default()
        },
        ..Default::default()
    };
    cfg.validate_runtime()
        .unwrap_or_else(|e| panic!("a clean peer list must validate: {e}"));
}

// ── Peer endpoint shape (PR #127 review round 7) ──
// A peer is dialed exactly as written, so the same rules as the
// advertised address apply to it — a malformed peer is a member that can
// never be reached, not a typo an operator can fix later.

#[test]
fn test_raft_peer_missing_port_is_refused() {
    let cfg = ServerConfig {
        raft: omni_engine::config::RaftConfig {
            node_id: Some(1),
            raft_addr: Some("127.0.0.1:9090".into()),
            peers: vec!["omni-node-2".into()],
            ..Default::default()
        },
        ..Default::default()
    };
    let err = cfg.validate_runtime().unwrap_err();
    assert!(
        err.0.contains("host:port"),
        "a portless peer must be rejected: {err}"
    );
}

#[test]
fn test_raft_peer_port_zero_is_refused() {
    // Port 0 dials a random port — the member is unreachable.
    let cfg = ServerConfig {
        raft: omni_engine::config::RaftConfig {
            node_id: Some(1),
            raft_addr: Some("127.0.0.1:9090".into()),
            peers: vec!["127.0.0.1:0".into()],
            ..Default::default()
        },
        ..Default::default()
    };
    let err = cfg.validate_runtime().unwrap_err();
    assert!(
        err.0.contains("nonzero port"),
        "a port-zero peer must be rejected: {err}"
    );
}

#[test]
fn test_raft_peer_wildcard_is_refused() {
    // A wildcard peer address resolves to the DIALER itself.
    let cfg = ServerConfig {
        raft: omni_engine::config::RaftConfig {
            node_id: Some(1),
            raft_addr: Some("127.0.0.1:9090".into()),
            peers: vec!["0.0.0.0:9091".into()],
            ..Default::default()
        },
        ..Default::default()
    };
    let err = cfg.validate_runtime().unwrap_err();
    assert!(
        err.0.contains("routable"),
        "a wildcard peer must be rejected: {err}"
    );
}

#[test]
fn test_raft_peer_hostname_is_accepted() {
    // The container pattern: hostnames resolve through the compose network.
    let cfg = ServerConfig {
        raft: omni_engine::config::RaftConfig {
            node_id: Some(1),
            raft_addr: Some("127.0.0.1:9090".into()),
            peers: vec!["omni-node-2:9090".into(), "omni-node-3:9090".into()],
            ..Default::default()
        },
        ..Default::default()
    };
    cfg.validate_runtime()
        .unwrap_or_else(|e| panic!("hostname peers must validate: {e}"));
}

#[test]
fn test_tcp_loopback_bind_is_the_default() {
    // Every other listener defaults to loopback; the TCP command
    // interface must too (issue #117) — it grants full read/write.
    let cfg = ServerConfig::default();
    assert_eq!(cfg.tcp_addr, "127.0.0.1:7072");
    cfg.validate_runtime()
        .unwrap_or_else(|e| panic!("loopback default must validate: {e}"));
}

#[test]
fn test_tcp_public_bind_requires_opt_in() {
    // A non-loopback bind exposes an unrestricted read/write path to
    // every host that can reach the port. It must be deliberate.
    let mut cfg = ServerConfig {
        tcp_addr: "0.0.0.0:8080".into(),
        ..Default::default()
    };
    let err = cfg.validate_runtime().unwrap_err();
    assert!(
        err.0.contains("OMNIKV_TCP_BIND_PUBLIC"),
        "public TCP bind must name the opt-in: {err}"
    );

    // The opt-in acknowledges the exposure. It still needs a real secret
    // — the dev value is public, so a public bind with it is unauthenticated
    // in practice (see test_tcp_public_bind_rejects_dev_jwt_secret).
    cfg.tcp_bind_public = true;
    cfg.jwt_secret = "a-real-secret-not-the-dev-one-0123456789".into();
    cfg.validate_runtime()
        .unwrap_or_else(|e| panic!("opted-in public bind must validate: {e}"));
}

#[test]
fn test_tcp_public_bind_rejects_dev_jwt_secret() {
    // The opt-in is an operator acknowledging exposure; the built-in dev
    // secret is published in source, so that acknowledgment would be
    // worthless — anyone could mint a token. Refuse the combination.
    let mut cfg = ServerConfig {
        tcp_addr: "0.0.0.0:8080".into(),
        tcp_bind_public: true,
        ..Default::default()
    };
    let err = cfg.validate_runtime().unwrap_err();
    assert!(
        err.0.contains("dev") && err.0.contains("jwt_secret"),
        "public bind with the dev secret must be refused: {err}"
    );

    // A real secret dissolves the conflict; the bind then validates.
    cfg.jwt_secret = "a-real-secret-not-the-dev-one-0123456789".into();
    cfg.validate_runtime()
        .unwrap_or_else(|e| panic!("public bind with a real secret must validate: {e}"));
}

#[test]
fn test_tcp_public_bind_env_opt_in_round_trip() {
    with_env(
        &[
            ("OMNIKV_TCP_ADDR", "0.0.0.0:8080"),
            ("OMNIKV_TCP_BIND_PUBLIC", "true"),
            // A public bind is only accepted with a non-default secret.
            (
                "OMNIKV_JWT_SECRET",
                "a-real-secret-not-the-dev-one-0123456789",
            ),
        ],
        || {
            let mut cfg = ServerConfig::default();
            cfg.apply_env().unwrap();
            assert!(cfg.tcp_bind_public);
            cfg.validate_runtime()
                .unwrap_or_else(|e| panic!("env opt-in must validate: {e}"));
        },
    );
}
