//! JWT Authentication Middleware
//!
//! Provides token-based authentication for the REST API and QUIC protocol.
//! Supports both API key validation and JWT bearer tokens.

use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};

/// JWT claims payload.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: String,
    /// "read", "write", "backup", "restore", "cluster", or "admin"
    pub role: String,
    pub exp: u64,
    pub iat: u64,
}

/// Generate a signed JWT token.
pub fn generate_token(
    sub: &str,
    role: &str,
    secret: &str,
    ttl_secs: u64,
) -> Result<String, String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let claims = Claims {
        sub: sub.to_string(),
        role: role.to_string(),
        exp: now + ttl_secs,
        iat: now,
    };

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|e| format!("Token encode error: {}", e))
}

/// Verify and decode a JWT token.
pub fn verify_token(token: &str, secret: &str) -> Result<Claims, String> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;

    decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )
    .map(|data| data.claims)
    .map_err(|e| format!("Token verify error: {}", e))
}

/// What a route or command requires of a caller's role. Shared by the
/// REST middleware and the TCP command interface so the two cannot drift
/// apart: the same token grants the same access over either protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequiredRole {
    Read,
    Write,
    Backup,
    Admin,
}

impl RequiredRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Backup => "backup",
            Self::Admin => "admin",
        }
    }

    /// Whether a token with this role satisfies the requirement. `admin`
    /// satisfies everything; otherwise a role must outrank the requirement
    /// (a `write` token may read, but a `read` token may not write).
    pub fn allows(self, role: &str) -> bool {
        match self {
            Self::Read => matches!(role, "read" | "write" | "admin"),
            Self::Write => matches!(role, "write" | "admin"),
            Self::Backup => matches!(role, "backup" | "admin"),
            Self::Admin => role == "admin",
        }
    }
}

/// Validate a raw API key against the expected key.
///
/// Hash-then-compare: both sides become fixed 32-byte digests first, so
/// the comparison time leaks nothing about key length or content.
pub fn validate_api_key(provided: &str, expected: &str) -> bool {
    use sha2::{Digest, Sha256};

    let hash_expected = Sha256::digest(expected.as_bytes());
    let hash_provided = Sha256::digest(provided.as_bytes());

    hash_expected
        .as_slice()
        .iter()
        .zip(hash_provided.as_slice().iter())
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

/// Extract bearer token from Authorization header.
pub fn extract_bearer(header_value: &str) -> Option<&str> {
    header_value
        .strip_prefix("Bearer ")
        .or_else(|| header_value.strip_prefix("bearer "))
}
