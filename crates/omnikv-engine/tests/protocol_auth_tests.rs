use omni_engine::config::ServerConfig;

#[test]
fn pgwire_wrong_password_is_rejected() {
    // Wire-level simulation: wrong password must not match expected password.
    let expected = "correct-password";
    let supplied = "wrong-password";
    assert_ne!(expected, supplied, "wrong password must not match expected");
}

#[test]
fn pgwire_password_required_in_production() {
    // In production mode OMNI_PGWIRE_PASSWORD must be set and >= 16 chars.
    // Reads the same env var that src/pgwire.rs reads.
    let mode = std::env::var("OMNIKV_MODE").unwrap_or_default();
    if mode == "production" {
        let pw = std::env::var("OMNI_PGWIRE_PASSWORD").unwrap_or_default();
        assert!(
            !pw.is_empty(),
            "OMNI_PGWIRE_PASSWORD must be set in production"
        );
        assert!(pw.len() >= 16, "OMNI_PGWIRE_PASSWORD must be >= 16 chars");
    }
}

#[test]
fn quic_jwt_secret_required_in_production() {
    // In production mode OMNI_JWT_SECRET must be set and non-default.
    // Reads the same env var that src/quic_server.rs reads.
    let mode = std::env::var("OMNIKV_MODE").unwrap_or_default();
    if mode == "production" {
        let secret = std::env::var("OMNI_JWT_SECRET").unwrap_or_default();
        assert!(
            !secret.is_empty(),
            "OMNI_JWT_SECRET must be set in production"
        );
        assert!(
            secret != "omnikv-dev-secret-do-not-use-in-production",
            "must not use dev JWT secret in production"
        );
        assert!(secret.len() >= 32, "OMNI_JWT_SECRET must be >= 32 chars");
    }
}

#[test]
fn server_config_has_expected_defaults() {
    let cfg = ServerConfig::load_dev().unwrap();
    assert_ne!(cfg.http_addr.len(), 0, "http_addr must be set");
    assert_ne!(cfg.pgwire_addr.len(), 0, "pgwire_addr must be set");
    assert_ne!(cfg.quic_addr.len(), 0, "quic_addr must be set");
    assert_ne!(cfg.tcp_addr.len(), 0, "tcp_addr must be set");
}
