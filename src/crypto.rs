//! AES-256-GCM At-Rest Encryption
//!
//! Provides authenticated encryption for backup files and sensitive data.

use aes_gcm::{Aes256Gcm, Key, Nonce};
use aes_gcm::aead::{Aead, KeyInit};
use sha2::{Sha256, Digest};

/// Derive a 256-bit key from a passphrase using SHA-256.
pub fn derive_key(passphrase: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(passphrase.as_bytes());
    let result = hasher.finalize();
    let mut key = [0u8; 32];
    key.copy_from_slice(&result);
    key
}

/// Encrypt data with AES-256-GCM.
/// Returns: [12-byte nonce | ciphertext]
pub fn encrypt(data: &[u8], passphrase: &str) -> Result<Vec<u8>, String> {
    let key_bytes = derive_key(passphrase);
    let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
    let cipher = Aes256Gcm::new(key);

    let nonce_bytes: [u8; 12] = rand::random();
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, data)
        .map_err(|e| format!("Encryption failed: {}", e))?;

    let mut output = Vec::with_capacity(12 + ciphertext.len());
    output.extend_from_slice(&nonce_bytes);
    output.extend_from_slice(&ciphertext);
    Ok(output)
}

/// Decrypt data encrypted with AES-256-GCM.
/// Input: [12-byte nonce | ciphertext]
pub fn decrypt(data: &[u8], passphrase: &str) -> Result<Vec<u8>, String> {
    if data.len() < 12 {
        return Err("Data too short for AES-GCM".to_string());
    }

    let key_bytes = derive_key(passphrase);
    let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
    let cipher = Aes256Gcm::new(key);

    let nonce = Nonce::from_slice(&data[..12]);
    let ciphertext = &data[12..];

    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| format!("Decryption failed: {}", e))
}
