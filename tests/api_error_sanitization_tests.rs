use omni_engine::{OmniError, OmniKV};
use tempfile::tempdir;

/// Verify OmniError variants exist and are named correctly.
/// The sanitization logic is tested via unit tests in src/api.rs.
#[test]
fn omni_error_key_not_found_variant_exists() {
    // KeyNotFound is a unit variant — no tuple argument
    let err = OmniError::KeyNotFound;
    let msg = format!("{:?}", err);
    assert!(msg.contains("KeyNotFound"), "variant name changed: {msg}");
}

#[test]
fn omnikv_open_and_read_missing_key_returns_no_panic() {
    let dir = tempdir().unwrap();
    let manifest = dir.path().join("manifest.json");
    let wal = dir.path().join("wal.bin");
    let db = OmniKV::open(&manifest.to_string_lossy(), &wal.to_string_lossy()).unwrap();
    let result = db.find_latest_internal("nonexistent_key_xyz");
    // Either Ok(None) or Err — must not panic
    match result {
        Ok(None) | Ok(Some(_)) | Err(_) => {} // any result is acceptable, no panic
    }
}
