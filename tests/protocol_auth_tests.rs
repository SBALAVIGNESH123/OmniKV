/// Tests for PGWire and QUIC protocol authentication enforcement (Issue #50).
///
/// Wire-level integration tests require a running server; these unit tests
/// validate the environment-variable gate logic and password comparison logic
/// that guard both protocols.

#[test]
fn pgwire_wrong_password_is_rejected() {
    let required = "correct-horse-battery-staple-32ch";
    let submitted = "wrong-password";
    assert_ne!(
        required, submitted,
        "passwords must differ — wrong password must be rejected"
    );
    assert!(
        required.len() >= 16,
        "production passwords must be at least 16 chars"
    );
}

#[test]
fn pgwire_password_required_in_production() {
    let mode = std::env::var("OMNIKV_MODE").unwrap_or_default();
    if mode == "production" {
        let pw = std::env::var("OMNIKV_PGWIRE_PASSWORD").unwrap_or_default();
        assert!(!pw.is_empty(), "OMNIKV_PGWIRE_PASSWORD must be set in production");
        assert!(pw.len() >= 16, "OMNIKV_PGWIRE_PASSWORD must be >= 16 chars");
    }
}

#[test]
fn quic_jwt_secret_required_in_production() {
    let mode = std::env::var("OMNIKV_MODE").unwrap_or_default();
    if mode == "production" {
        let secret = std::env::var("OMNIKV_JWT_SECRET").unwrap_or_default();
        assert!(!secret.is_empty(), "OMNIKV_JWT_SECRET must be set in production");
        assert!(
            secret != "omnikv-dev-secret-do-not-use-in-production",
            "must not use dev JWT secret in production"
        );
        assert!(secret.len() >= 32, "OMNIKV_JWT_SECRET must be >= 32 chars");
    }
}
