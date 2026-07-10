//! AES-256-GCM At-Rest Encryption
//!
//! Provides authenticated encryption for backup files and sensitive data.
//! Key derivation uses Argon2id (memory-hard) for brute-force resistance.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};

/// Encryption format version.
/// v0 = SHA-256 key derivation (legacy, insecure)
/// v1 = Argon2id key derivation (current)
const ENCRYPTION_VERSION: u8 = 0x01;

/// Derive a 256-bit key from a passphrase using Argon2id.
///
/// Argon2id is memory-hard, making brute-force attacks expensive.
/// The salt is derived from a fixed domain separator — this is acceptable
/// for backup encryption where the passphrase is the primary secret.
/// For multi-user password hashing, use a random per-user salt instead.
pub fn derive_key(passphrase: &str) -> [u8; 32] {
    use argon2::Argon2;

    // Fixed salt for deterministic key derivation from the same passphrase.
    // This is safe because:
    // 1. Each backup is encrypted with a unique random nonce (AES-GCM)
    // 2. Argon2id's memory-hardness prevents brute-force even with known salt
    let salt = b"OmniKV-backup-key-v1";
    let mut key = [0u8; 32];
    Argon2::default()
        .hash_password_into(passphrase.as_bytes(), salt, &mut key)
        .expect("Argon2id key derivation failed");
    key
}

/// Legacy SHA-256 key derivation for backward-compatible decryption of v0 backups.
fn derive_key_legacy(passphrase: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(passphrase.as_bytes());
    let result = hasher.finalize();
    let mut key = [0u8; 32];
    key.copy_from_slice(&result);
    key
}

/// Encrypt data with AES-256-GCM (Argon2id key derivation).
/// Returns: [1-byte version | 12-byte nonce | ciphertext]
pub fn encrypt(data: &[u8], passphrase: &str) -> Result<Vec<u8>, String> {
    let key_bytes = derive_key(passphrase);
    let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
    let cipher = Aes256Gcm::new(key);

    let nonce_bytes: [u8; 12] = rand::random();
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, data)
        .map_err(|e| format!("Encryption failed: {}", e))?;

    let mut output = Vec::with_capacity(1 + 12 + ciphertext.len());
    output.push(ENCRYPTION_VERSION);
    output.extend_from_slice(&nonce_bytes);
    output.extend_from_slice(&ciphertext);
    Ok(output)
}

/// Decrypt data encrypted with AES-256-GCM.
/// Supports both v0 (legacy SHA-256) and v1 (Argon2id) formats.
///
/// v0 format: [12-byte nonce | ciphertext]
/// v1 format: [0x01 | 12-byte nonce | ciphertext]
pub fn decrypt(data: &[u8], passphrase: &str) -> Result<Vec<u8>, String> {
    if data.len() < 13 {
        return Err("Data too short for AES-GCM".to_string());
    }

    // Detect format version
    let (key_bytes, nonce_start) = if data[0] == ENCRYPTION_VERSION {
        // v1: Argon2id
        (derive_key(passphrase), 1)
    } else {
        // v0: Legacy SHA-256 (backward compatibility)
        (derive_key_legacy(passphrase), 0)
    };

    let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
    let cipher = Aes256Gcm::new(key);

    let nonce = Nonce::from_slice(&data[nonce_start..nonce_start + 12]);
    let ciphertext = &data[nonce_start + 12..];

    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| format!("Decryption failed: {}", e))
}
