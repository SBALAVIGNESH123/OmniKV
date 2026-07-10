//! Storage format versioning tests — covers issue #14.
//!
//! Run with: `cargo test --test storage_format_versioning -- --test-threads=1`

use std::fs;

use omni_engine::{Manifest, OmniError, MANIFEST_FORMAT_VERSION};
use tempfile::TempDir;

#[test]
fn test_manifest_format_version_constant() {
    assert_eq!(MANIFEST_FORMAT_VERSION, 1);
}

#[test]
fn test_legacy_manifest_no_version_field_loads_as_v1() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("manifest.json").to_string_lossy().to_string();
    // Old-style manifest: no format_version field at all
    let legacy = r#"{"heap_path":"heap.bin","base_path":"/tmp","sstables":[],"max_seq":0}"#;
    fs::write(&path, legacy).unwrap();
    let m = Manifest::load(&path).expect("legacy manifest must load");
    assert_eq!(m.format_version, 1, "missing field must default to v1");
}

#[test]
fn test_manifest_v1_explicit_loads() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("manifest.json").to_string_lossy().to_string();
    let v1 = r#"{"heap_path":"h","base_path":"/t","sstables":["s.sst"],"max_seq":42,"format_version":1}"#;
    fs::write(&path, v1).unwrap();
    let m = Manifest::load(&path).expect("v1 manifest must load");
    assert_eq!(m.format_version, 1);
    assert_eq!(m.max_seq, 42);
}

#[test]
fn test_future_manifest_version_rejected() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("manifest.json").to_string_lossy().to_string();
    let future =
        r#"{"heap_path":"h","base_path":"/t","sstables":[],"max_seq":0,"format_version":2}"#;
    fs::write(&path, future).unwrap();
    let result = Manifest::load(&path);
    assert!(result.is_err(), "manifest with future version must be rejected");
    match result.unwrap_err() {
        OmniError::UnsupportedVersion { found, supported } => {
            assert_eq!(found, 2);
            assert_eq!(supported, MANIFEST_FORMAT_VERSION);
        }
        other => panic!("expected UnsupportedVersion, got {other}"),
    }
}

#[test]
fn test_saved_manifest_includes_format_version() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("manifest.json").to_string_lossy().to_string();
    let m = Manifest {
        heap_path: "heap.bin".to_string(),
        base_path: dir.path().to_string_lossy().to_string(),
        sstables: vec![],
        l1_sstables: vec![],
        max_seq: 0,
        format_version: 0, // intentionally wrong — save() must override
    };
    m.save(&path).expect("save must succeed");
    let raw = fs::read_to_string(&path).expect("read saved manifest");
    assert!(
        raw.contains(""format_version":1"),
        "saved manifest must contain format_version:1, got: {raw}"
    );
}

#[test]
fn test_manifest_round_trip() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("manifest.json").to_string_lossy().to_string();
    let original = Manifest {
        heap_path: "heap.bin".to_string(),
        base_path: "/data".to_string(),
        sstables: vec!["a.sst".to_string(), "b.sst".to_string()],
        l1_sstables: vec!["c.sst".to_string()],
        max_seq: 12345,
        format_version: MANIFEST_FORMAT_VERSION,
    };
    original.save(&path).expect("save");
    let loaded = Manifest::load(&path).expect("load");
    assert_eq!(loaded.heap_path, original.heap_path);
    assert_eq!(loaded.sstables, original.sstables);
    assert_eq!(loaded.max_seq, original.max_seq);
    assert_eq!(loaded.format_version, MANIFEST_FORMAT_VERSION);
}

#[test]
fn test_golden_v1_fixture_accepted() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("manifest.json").to_string_lossy().to_string();
    // Golden v1 fixture — changing this test is a breaking-change signal.
    let golden = r#"{"heap_path":"h","base_path":"/v","sstables":["l0/s.sst"],"max_seq":1,"format_version":1}"#;
    fs::write(&path, golden.as_bytes()).unwrap();
    let m = Manifest::load(&path).expect("golden v1 fixture must load");
    assert_eq!(m.format_version, 1);
    assert_eq!(m.max_seq, 1);
}

#[test]
fn test_corrupt_manifest_returns_err() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("manifest.json").to_string_lossy().to_string();
    fs::write(&path, b"{corrupt{{").unwrap();
    assert!(Manifest::load(&path).is_err(), "corrupt manifest must return Err");
}

#[test]
fn test_empty_manifest_returns_err() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("manifest.json").to_string_lossy().to_string();
    fs::write(&path, b"").unwrap();
    assert!(Manifest::load(&path).is_err(), "empty manifest must return Err");
}
