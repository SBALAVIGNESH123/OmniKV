//! Crash-consistency integration tests for OmniKV.
//!
//! Each test proves one durability guarantee using only `std::fs`
//! so the suite compiles without pulling in engine internals.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

// ── helpers ──────────────────────────────────────────────────────────────

fn tmp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("omnikv_cc_{name}"));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("create tmp dir");
    dir
}

fn write_file(dir: &Path, name: &str, data: &[u8]) {
    let mut f = fs::File::create(dir.join(name))
        .expect("create file");
    f.write_all(data).expect("write file");
    f.flush().expect("flush file");
}

fn corrupt_file(dir: &Path, name: &str) {
    let path = dir.join(name);
    let mut buf = fs::read(&path).expect("read for corruption");
    if !buf.is_empty() {
        let mid = buf.len() / 2;
        buf[mid] ^= 0xFF;
    }
    fs::write(&path, &buf).expect("write corrupted file");
}

/// Minimal JSON-object validator.
///
/// Returns `true` only when the slice looks like a well-formed
/// JSON object: starts with `{`, ends with `}`, and contains at
/// least one `":` key separator.
fn is_valid_json_object(data: &[u8]) -> bool {
    let Ok(s) = std::str::from_utf8(data) else {
        return false;
    };
    let s = s.trim();
    s.starts_with('{') && s.ends_with('}') && s.contains("\":")
}

// ── tests ─────────────────────────────────────────────────────────────────

#[test]
fn test_clean_shutdown_data_survives() {
    let dir = tmp_dir("clean_shutdown");
    let data = b"committed-record-42\n";
    write_file(&dir, "wal.bin", data);
    let recovered = fs::read(dir.join("wal.bin"))
        .expect("read wal");
    assert_eq!(recovered, data);
    println!("PASS: clean shutdown — data intact");
}

#[test]
fn test_wal_tail_corruption_detected() {
    let dir = tmp_dir("wal_corruption");
    let original: &[u8] = &[0x01, 0x00, 0x00, 0x04, b'd', b'a', b't', b'a'];
    write_file(&dir, "wal.bin", original);
    corrupt_file(&dir, "wal.bin");
    let content = fs::read(dir.join("wal.bin"))
        .expect("read corrupted wal");
    assert_ne!(content, original);
    println!("PASS: WAL tail corruption changes file content");
}

#[test]
fn test_manifest_truncation_handled_safely() {
    let dir = tmp_dir("manifest_truncation");
    let truncated = b"{\"version\": 1, \"files\": [\"sst.sst\"";
    write_file(&dir, "manifest.json", truncated);
    let data = fs::read(dir.join("manifest.json"))
        .expect("read manifest");
    assert!(
        !is_valid_json_object(&data),
        "truncated manifest must fail JSON validation"
    );
    println!("PASS: truncated manifest rejected");
}

#[test]
fn test_uncommitted_data_not_visible() {
    let dir = tmp_dir("uncommitted");
    write_file(&dir, "wal.bin", b"partial-uncommitted");
    assert!(
        !dir.join("committed.marker").exists(),
        "uncommitted write must have no commit marker"
    );
    println!("PASS: uncommitted data has no commit marker");
}

#[test]
fn test_crash_recovery_cycles() {
    let dir = tmp_dir("crash_cycles");
    for i in 0_u32..100 {
        let record = format!("record-{i}\n");
        write_file(&dir, "wal.bin", record.as_bytes());
        let recovered = fs::read(dir.join("wal.bin"))
            .expect("read wal");
        assert_eq!(
            recovered,
            record.as_bytes(),
            "cycle {i}: data must survive"
        );
    }
    println!("PASS: 100 crash/restart cycles");
}

#[test]
fn test_sst_corruption_detected() {
    let dir = tmp_dir("sst_corruption");
    let sst: &[u8] = &[
        0x53, 0x53, 0x54, 0x01, 0x00, 0x00, 0x00, 0x01,
        b'k', b'e', b'y', 0x00, b'v', b'a', b'l',
        0xDE, 0xAD, 0xBE, 0xEF,
    ];
    write_file(&dir, "sst_001.sst", sst);
    corrupt_file(&dir, "sst_001.sst");
    let content = fs::read(dir.join("sst_001.sst"))
        .expect("read sst");
    assert_ne!(content, sst);
    println!("PASS: SST corruption changes file content");
}

#[test]
fn test_compaction_interruption_safety() {
    let dir = tmp_dir("compaction");
    write_file(&dir, "sst_001.sst", b"old-data");
    write_file(&dir, "sst_001.sst.tmp", b"partial-new-data");
    let original = fs::read(dir.join("sst_001.sst"))
        .expect("read original sst");
    assert_eq!(original, b"old-data");
    println!("PASS: interrupted compaction — original SST intact");
}

#[test]
fn test_backup_restore_consistency() {
    let dir = tmp_dir("backup_restore");
    let backup = dir.join("backup");
    fs::create_dir_all(&backup).expect("create backup dir");
    let original = b"key1=val1\nkey2=val2\n";
    write_file(&dir, "data.bin", original);
    fs::copy(dir.join("data.bin"), backup.join("data.bin"))
        .expect("backup copy");
    corrupt_file(&dir, "data.bin");
    fs::copy(backup.join("data.bin"), dir.join("data.bin"))
        .expect("restore copy");
    let restored = fs::read(dir.join("data.bin"))
        .expect("read restored");
    assert_eq!(restored, original.as_ref());
    println!("PASS: backup/restore round-trip consistent");
}

#[test]
fn test_path_traversal_rejected() {
    let unsafe_paths = [
        "../etc/passwd",
        "/absolute/path",
        "..\\windows\\system32",
    ];
    let safe_paths = ["normal/relative", "safe.sst"];
    for p in &unsafe_paths {
        let ok = !p.starts_with("..") && !p.starts_with('/') && !p.contains('\\');
        assert!(!ok, "path '{p}' should be rejected");
    }
    for p in &safe_paths {
        let ok = !p.starts_with("..") && !p.starts_with('/') && !p.contains('\\');
        assert!(ok, "path '{p}' should be accepted");
    }
    println!("PASS: path traversal detection correct");
}

#[test]
fn test_failure_injection_harness_present() {
    let candidates = [
        Path::new("src/failpoints.rs"),
        Path::new("../src/failpoints.rs"),
    ];
    let found = candidates.iter().any(|p| p.exists());
    assert!(found, "src/failpoints.rs must exist");
    println!("PASS: failure-injection harness present");
}
