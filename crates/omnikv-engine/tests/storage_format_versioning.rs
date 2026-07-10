//! Storage format versioning tests — issue #14.
//!
//! Run with: `cargo test -p omnikv-engine --test storage_format_versioning -- --test-threads=1`

use std::fs;

use omni_engine::{MANIFEST_FORMAT_VERSION, Manifest, OmniError};
use tempfile::TempDir;

fn test_manifest(dir: &TempDir) -> Manifest {
    Manifest {
        heap_path: "heap.bin".into(),
        base_path: dir.path().to_string_lossy().into_owned(),
        sstables: vec![],
        l1_sstables: vec![],
        max_seq: 0,
        format_version: MANIFEST_FORMAT_VERSION,
    }
}

fn manifest_path(dir: &TempDir) -> String {
    dir.path()
        .join("manifest.json")
        .to_string_lossy()
        .into_owned()
}

#[test]
fn test_manifest_format_version_constant() {
    assert_eq!(MANIFEST_FORMAT_VERSION, 1);
}

#[test]
fn test_legacy_manifest_no_version_field_loads_as_v1() {
    let dir = TempDir::new().unwrap();
    let path = manifest_path(&dir);
    let legacy = r#"{"heap_path":"h","base_path":"/","sstables":[],"max_seq":0}"#;
    fs::write(&path, legacy).unwrap();
    let m = Manifest::load(&path).expect("legacy manifest must load");
    assert_eq!(m.format_version, 1, "missing field must default to v1");
}

#[test]
fn test_manifest_v1_explicit_loads() {
    let dir = TempDir::new().unwrap();
    let path = manifest_path(&dir);
    let mut m = test_manifest(&dir);
    m.max_seq = 42;
    m.sstables = vec!["sst_001.sst".into()];
    m.save(&path).expect("save must succeed");
    let loaded = Manifest::load(&path).expect("v1 manifest must load");
    assert_eq!(loaded.format_version, 1);
    assert_eq!(loaded.max_seq, 42);
    assert_eq!(loaded.sstables, vec!["sst_001.sst"]);
}

#[test]
fn test_future_manifest_version_rejected() {
    let dir = TempDir::new().unwrap();
    let path = manifest_path(&dir);
    let m = test_manifest(&dir);
    m.save(&path).expect("save");
    let raw = fs::read_to_string(&path).unwrap();
    let patched = raw.replace("\"format_version\":1", "\"format_version\":2");
    fs::write(&path, patched).unwrap();
    let result = Manifest::load(&path);
    assert!(result.is_err(), "future version must be rejected");
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
    let path = manifest_path(&dir);
    let mut m = test_manifest(&dir);
    m.format_version = 0;
    m.save(&path).expect("save must succeed");
    let raw = fs::read_to_string(&path).expect("read saved manifest");
    assert!(
        raw.contains("\"format_version\":1"),
        "saved manifest must contain format_version:1, got: {raw}"
    );
}

#[test]
fn test_manifest_round_trip() {
    let dir = TempDir::new().unwrap();
    let path = manifest_path(&dir);
    let original = Manifest {
        heap_path: "heap.bin".into(),
        base_path: "/data".into(),
        sstables: vec!["a.sst".into(), "b.sst".into()],
        l1_sstables: vec!["c.sst".into()],
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
    let path = manifest_path(&dir);
    // Golden v1 fixture — changing this is a breaking-change signal.
    let golden =
        r#"{"heap_path":"h","base_path":"/v","sstables":["s.sst"],"max_seq":1,"format_version":1}"#;
    fs::write(&path, golden).unwrap();
    let m = Manifest::load(&path).expect("golden v1 fixture must load");
    assert_eq!(m.format_version, 1);
    assert_eq!(m.max_seq, 1);
}

#[test]
fn test_corrupt_manifest_returns_err() {
    let dir = TempDir::new().unwrap();
    let path = manifest_path(&dir);
    fs::write(&path, b"{corrupt{{").unwrap();
    assert!(Manifest::load(&path).is_err(), "corrupt manifest must Err");
}

#[test]
fn test_empty_manifest_returns_err() {
    let dir = TempDir::new().unwrap();
    let path = manifest_path(&dir);
    fs::write(&path, b"").unwrap();
    assert!(Manifest::load(&path).is_err(), "empty manifest must Err");
}
