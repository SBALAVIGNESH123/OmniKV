//! AES-256-GCM At-Rest Encryption
//!
//! Provides authenticated encryption for backup files and sensitive data.
//! Key derivation uses Argon2id (memory-hard) for brute-force resistance.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};

/// Encryption format version.
/// v0 = SHA-256 key derivation (legacy, insecure)
/// v1 = Argon2id with the original fixed salt (legacy compatibility)
/// v2 = Argon2id with a random per-backup salt
const ENCRYPTION_VERSION: u8 = 0x02;
const LEGACY_ARGON2_VERSION: u8 = 0x01;
const SALT_LEN: usize = 16;
const NONCE_LEN: usize = 12;
const V1_FIXED_SALT: &[u8] = b"OmniKV-backup-key-v1";

/// Derive a 256-bit key from a passphrase using Argon2id.
///
/// Argon2id is memory-hard, making brute-force attacks expensive.
pub fn derive_key(passphrase: &str, salt: &[u8]) -> Result<[u8; 32], String> {
    use argon2::Argon2;

    let mut key = [0u8; 32];
    Argon2::default()
        .hash_password_into(passphrase.as_bytes(), salt, &mut key)
        .map_err(|e| format!("Argon2id key derivation failed: {e}"))?;
    Ok(key)
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

/// Encrypt data with AES-256-GCM and a random per-backup Argon2id salt.
/// Returns: [1-byte version | 16-byte salt | 12-byte nonce | ciphertext]
pub fn encrypt(data: &[u8], passphrase: &str) -> Result<Vec<u8>, String> {
    let salt: [u8; SALT_LEN] = rand::random();
    let key_bytes = derive_key(passphrase, &salt)?;
    let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
    let cipher = Aes256Gcm::new(key);

    let nonce_bytes: [u8; NONCE_LEN] = rand::random();
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, data)
        .map_err(|e| format!("Encryption failed: {}", e))?;

    let mut output = Vec::with_capacity(1 + SALT_LEN + NONCE_LEN + ciphertext.len());
    output.push(ENCRYPTION_VERSION);
    output.extend_from_slice(&salt);
    output.extend_from_slice(&nonce_bytes);
    output.extend_from_slice(&ciphertext);
    Ok(output)
}

/// Decrypt data encrypted with AES-256-GCM.
/// Supports v0 (legacy SHA-256), v1 (fixed-salt Argon2id), and v2
/// (per-backup-salt Argon2id) formats.
///
/// v0 format: [12-byte nonce | ciphertext]
/// v1 format: [0x01 | 12-byte nonce | ciphertext]
/// v2 format: [0x02 | 16-byte salt | 12-byte nonce | ciphertext]
pub fn decrypt(data: &[u8], passphrase: &str) -> Result<Vec<u8>, String> {
    let versioned_error = if data.first() == Some(&ENCRYPTION_VERSION) {
        match decrypt_v2(data, passphrase) {
            Ok(plaintext) => return Ok(plaintext),
            Err(err) => Some(err),
        }
    } else if data.first() == Some(&LEGACY_ARGON2_VERSION) {
        match decrypt_v1(data, passphrase) {
            Ok(plaintext) => return Ok(plaintext),
            Err(err) => Some(err),
        }
    } else {
        None
    };

    match decrypt_v0(data, passphrase) {
        Ok(plaintext) => Ok(plaintext),
        Err(v0_error) => Err(match versioned_error {
            Some(versioned_error) => {
                format!(
                    "Decryption failed for versioned backup ({versioned_error}) and v0 ({v0_error})"
                )
            }
            None => v0_error,
        }),
    }
}

fn decrypt_v1(data: &[u8], passphrase: &str) -> Result<Vec<u8>, String> {
    if data.len() < 1 + NONCE_LEN + 16 {
        return Err("Data too short for AES-GCM v1".to_string());
    }

    let key_bytes = derive_key(passphrase, V1_FIXED_SALT)?;
    decrypt_with_key(&key_bytes, &data[1..=NONCE_LEN], &data[1 + NONCE_LEN..])
}

fn decrypt_v2(data: &[u8], passphrase: &str) -> Result<Vec<u8>, String> {
    if data.len() < 1 + SALT_LEN + NONCE_LEN + 16 {
        return Err("Data too short for AES-GCM v2".to_string());
    }

    let salt_start = 1;
    let nonce_start = salt_start + SALT_LEN;
    let ciphertext_start = nonce_start + NONCE_LEN;
    let key_bytes = derive_key(passphrase, &data[salt_start..nonce_start])?;
    decrypt_with_key(
        &key_bytes,
        &data[nonce_start..ciphertext_start],
        &data[ciphertext_start..],
    )
}

fn decrypt_v0(data: &[u8], passphrase: &str) -> Result<Vec<u8>, String> {
    if data.len() < NONCE_LEN + 16 {
        return Err("Data too short for AES-GCM v0".to_string());
    }

    let key_bytes = derive_key_legacy(passphrase);
    decrypt_with_key(&key_bytes, &data[..NONCE_LEN], &data[NONCE_LEN..])
}

fn decrypt_with_key(
    key_bytes: &[u8; 32],
    nonce_bytes: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, String> {
    let key = Key::<Aes256Gcm>::from_slice(key_bytes);
    let cipher = Aes256Gcm::new(key);

    let nonce = Nonce::from_slice(nonce_bytes);

    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| format!("Decryption failed: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decrypts_legacy_v0_when_nonce_starts_with_version_byte() {
        let passphrase = "correct horse battery staple";
        let plaintext = b"legacy backup payload";
        let key_bytes = derive_key_legacy(passphrase);
        let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
        let cipher = Aes256Gcm::new(key);
        let nonce_bytes = [LEGACY_ARGON2_VERSION, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = cipher.encrypt(nonce, plaintext.as_ref()).unwrap();

        let mut legacy_payload = nonce_bytes.to_vec();
        legacy_payload.extend_from_slice(&ciphertext);

        let decrypted = decrypt(&legacy_payload, passphrase).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn v2_backups_use_a_unique_salt_and_round_trip() {
        let passphrase = "correct horse battery staple";
        let first = encrypt(b"payload", passphrase).unwrap();
        let second = encrypt(b"payload", passphrase).unwrap();

        assert_eq!(first[0], ENCRYPTION_VERSION);
        assert_eq!(second[0], ENCRYPTION_VERSION);
        assert_ne!(&first[1..=SALT_LEN], &second[1..=SALT_LEN]);
        assert_eq!(decrypt(&first, passphrase).unwrap(), b"payload");
        assert_eq!(decrypt(&second, passphrase).unwrap(), b"payload");
    }

    #[test]
    fn v2_wrong_passphrase_fails_without_panicking() {
        let encrypted = encrypt(b"payload", "correct horse battery staple").unwrap();
        assert!(decrypt(&encrypted, "wrong passphrase").is_err());
    }

    #[test]
    fn decrypts_legacy_v1_fixed_salt_format() {
        let passphrase = "correct horse battery staple";
        let plaintext = b"legacy argon2 backup payload";
        let key_bytes = derive_key(passphrase, V1_FIXED_SALT).unwrap();
        let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
        let cipher = Aes256Gcm::new(key);
        let nonce_bytes = [9u8; NONCE_LEN];
        let ciphertext = cipher
            .encrypt(Nonce::from_slice(&nonce_bytes), plaintext.as_ref())
            .unwrap();

        let mut legacy_payload = vec![LEGACY_ARGON2_VERSION];
        legacy_payload.extend_from_slice(&nonce_bytes);
        legacy_payload.extend_from_slice(&ciphertext);

        assert_eq!(decrypt(&legacy_payload, passphrase).unwrap(), plaintext);
    }
}
