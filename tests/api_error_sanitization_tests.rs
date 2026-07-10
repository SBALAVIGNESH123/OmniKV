use omni_engine::{OmniError, OmniKV};
use tempfile::tempdir;

/// Verify OmniError::KeyNotFound variant exists and is named correctly.
#[test]
fn omni_error_key_not_found_variant_exists() {
    let err = OmniError::KeyNotFound;
    let msg = format!("{:?}", err);
    assert!(msg.contains("KeyNotFound"), "variant name changed: {msg}");
}

/// Verify find_latest_internal returns Ok(None) for a missing key — not Ok(Some(_)).
/// Ok(Some(_)) for a never-inserted key would be a data integrity bug.
#[test]
fn omnikv_missing_key_returns_none_not_some() {
    let dir = tempdir().unwrap();
    let manifest = dir.path().join("manifest.json");
    let wal = dir.path().join("wal.bin");
    let db = OmniKV::open(&manifest.to_string_lossy(), &wal.to_string_lossy()).unwrap();
    let result = db.find_latest_internal("nonexistent_key_xyz");
    match result {
        Ok(None) => {} // correct — key does not exist
        Ok(Some(v)) => panic!("data integrity bug: got value {v:?} for never-inserted key"),
        Err(_) => {} // also acceptable
    }
}

/// Verify OmniError variants used in sanitize_storage_err match are accessible.
#[test]
fn omni_error_variants_are_accessible() {
    // These must compile — if any variant is renamed this test fails
    let _ = OmniError::KeyNotFound;
    matches!(OmniError::KeyNotFound, OmniError::KeyNotFound);
}
