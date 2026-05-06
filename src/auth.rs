//! JWT Authentication Middleware
//!
//! Provides token-based authentication for the REST API and QUIC protocol.
//! Supports both API key validation and JWT bearer tokens.

use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};

/// JWT claims payload.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: String,  // Subject (user/service ID)
    pub role: String, // "admin", "read", "write"
    pub exp: u64,     // Expiration (UNIX timestamp)
    pub iat: u64,     // Issued at
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

/// Validate a raw API key against the expected key.
pub fn validate_api_key(provided: &str, expected: &str) -> bool {
    // Constant-time comparison to prevent timing attacks
    if provided.len() != expected.len() {
        return false;
    }
    provided
        .as_bytes()
        .iter()
        .zip(expected.as_bytes().iter())
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

/// Extract bearer token from Authorization header.
pub fn extract_bearer(header_value: &str) -> Option<&str> {
    header_value
        .strip_prefix("Bearer ")
        .or_else(|| header_value.strip_prefix("bearer "))
}
