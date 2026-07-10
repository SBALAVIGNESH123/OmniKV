//! Crash-consistency tests — these tests drive the **real OmniKV engine**.
//!
//! Every test:
//!   1. Opens the engine via `OmniKV::open()`
//!   2. Writes real data through `WriteBatch` + `commit_batch()`
//!   3. Simulates a crash (drop the engine, optionally corrupt on-disk files)
//!   4. Reopens the engine and asserts recovery semantics
//!
//! Run with: `cargo test --test crash_consistency -- --test-threads=1`

use omni_engine::{OmniKV, WriteBatch};
use std::fs;
use std::io::Write;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a fresh temp directory and return (dir, manifest_path, wal_path).
fn fresh_db_paths(dir: &TempDir) -> (String, String) {
    let base = dir.path();
    let manifest = base.join("manifest.json").to_string_lossy().to_string();
    let wal = base.join("wal.bin").to_string_lossy().to_string();
    (manifest, wal)
}

/// Open the engine, write one key-value pair, flush, then drop (simulates clean shutdown).
fn write_and_close(manifest: &str, wal: &str, key: &str, value: &str) {
    let db = OmniKV::open(manifest, wal).expect("open must succeed");
    let mut batch = WriteBatch::new();
    batch.set(key, value.to_string()).expect("batch.set");
    db.commit_batch(&batch).expect("commit_batch");
    db.sync_all().expect("sync_all");
    // drop(db) — engine released, files flushed
}

// ---------------------------------------------------------------------------
// Test 1 — clean shutdown preserves all written data
// ---------------------------------------------------------------------------
#[test]
fn test_clean_shutdown_no_data_loss() {
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    write_and_close(&manifest, &wal, "k1", "v1");

    // Reopen — data must survive
    let db2 = OmniKV::open(&manifest, &wal).expect("reopen after clean shutdown");
    let snap = db2.snapshot();
    let got = db2
        .find_latest_internal("k1")
        .expect("find after clean shutdown");
    assert_eq!(got.as_deref(), Some("v1"), "value must survive clean shutdown");
    db2.unregister_snapshot(snap);
}

// ---------------------------------------------------------------------------
// Test 2 — WAL tail corruption: engine must recover committed data
// ---------------------------------------------------------------------------
#[test]
fn test_wal_tail_corruption_committed_data_survives() {
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    // Write and close cleanly
    write_and_close(&manifest, &wal, "k2", "v2");

    // Simulate crash: append garbage bytes to the WAL tail
    let mut f = fs::OpenOptions::new()
        .append(true)
        .open(&wal)
        .expect("open wal for corruption");
    f.write_all(&[0xFF, 0xFE, 0xFD, 0xAA, 0xBB])
        .expect("write corruption bytes");
    f.sync_all().expect("sync corruption");
    drop(f);

    // Reopen — engine must skip the corrupt tail batch and return committed data
    let db2 = OmniKV::open(&manifest, &wal)
        .expect("engine must open despite WAL tail corruption");
    let got = db2
        .find_latest_internal("k2")
        .expect("find after WAL tail corruption");
    assert_eq!(
        got.as_deref(),
        Some("v2"),
        "committed data must survive WAL tail corruption"
    );
}

// ---------------------------------------------------------------------------
// Test 3 — WAL completely corrupted: engine opens with empty state
// ---------------------------------------------------------------------------
#[test]
fn test_wal_fully_corrupted_opens_empty() {
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    // Write and close
    write_and_close(&manifest, &wal, "k3", "v3");

    // Overwrite WAL with pure garbage (simulate full corruption)
    fs::write(&wal, b"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX").expect("corrupt wal");

    // Engine must open (not panic/crash) — data may be lost but must not hang
    let result = OmniKV::open(&manifest, &wal);
    // Either succeeds with empty state or returns a recoverable error — no panic
    match result {
        Ok(db2) => {
            // If it opens, committed data is gone (WAL fully corrupt) — acceptable
            let _ = db2.find_latest_internal("k3");
        }
        Err(e) => {
            // A clean error is also acceptable — must NOT be a panic
            eprintln!("Engine returned error on fully corrupt WAL (acceptable): {e}");
        }
    }
}

// ---------------------------------------------------------------------------
// Test 4 — manifest truncation: engine returns error, does not panic
// ---------------------------------------------------------------------------
#[test]
fn test_manifest_truncation_returns_error() {
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    // Write and close
    write_and_close(&manifest, &wal, "k4", "v4");

    // Truncate manifest mid-write (simulate crash during atomic rename)
    fs::write(&manifest, b"{\"heap_path\":\"heap").expect("truncate manifest");

    // Engine must return an error — must NOT panic
    let result = OmniKV::open(&manifest, &wal);
    assert!(
        result.is_err(),
        "truncated manifest must cause open() to return Err, not Ok"
    );
}

// ---------------------------------------------------------------------------
// Test 5 — uncommitted write is not visible after crash
// ---------------------------------------------------------------------------
#[test]
fn test_uncommitted_write_not_visible_after_crash() {
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    // Write k5=committed
    write_and_close(&manifest, &wal, "k5_committed", "committed_val");

    // Open engine, start a batch but do NOT commit — simulate crash by dropping
    {
        let db = OmniKV::open(&manifest, &wal).expect("open for uncommitted write test");
        let mut batch = WriteBatch::new();
        batch
            .set("k5_uncommitted", "should_not_appear".to_string())
            .expect("batch.set");
        // Intentionally NOT calling commit_batch — drop simulates crash
        drop(batch);
        drop(db);
    }

    // Reopen — uncommitted key must not be visible
    let db2 = OmniKV::open(&manifest, &wal).expect("reopen after uncommitted crash");
    let got = db2
        .find_latest_internal("k5_uncommitted")
        .expect("find uncommitted key");
    assert!(
        got.is_none(),
        "uncommitted write must not be visible after crash"
    );

    // Committed key must still be visible
    let committed = db2
        .find_latest_internal("k5_committed")
        .expect("find committed key");
    assert_eq!(committed.as_deref(), Some("committed_val"));
}

// ---------------------------------------------------------------------------
// Test 6 — multiple keys survive clean shutdown
// ---------------------------------------------------------------------------
#[test]
fn test_multiple_keys_survive_clean_shutdown() {
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    // Write 50 keys in one batch
    {
        let db = OmniKV::open(&manifest, &wal).expect("open");
        let mut batch = WriteBatch::new();
        for i in 0..50u32 {
            batch
                .set(&format!("key_{i:03}"), format!("val_{i}"))
                .expect("batch.set");
        }
        db.commit_batch(&batch).expect("commit_batch");
        db.sync_all().expect("sync_all");
    }

    // Reopen and verify all 50 keys
    let db2 = OmniKV::open(&manifest, &wal).expect("reopen");
    for i in 0..50u32 {
        let got = db2
            .find_latest_internal(&format!("key_{i:03}"))
            .expect("find");
        assert_eq!(
            got.as_deref(),
            Some(format!("val_{i}").as_str()),
            "key_{i:03} must survive"
        );
    }
}

// ---------------------------------------------------------------------------
// Test 7 — delete is durable across restart
// ---------------------------------------------------------------------------
#[test]
fn test_delete_is_durable_across_restart() {
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    // Write then delete k7
    {
        let db = OmniKV::open(&manifest, &wal).expect("open");
        let mut batch = WriteBatch::new();
        batch.set("k7", "to_be_deleted".to_string()).expect("set");
        db.commit_batch(&batch).expect("commit set");
        db.sync_all().expect("sync");

        let mut batch2 = WriteBatch::new();
        batch2.delete("k7").expect("delete");
        db.commit_batch(&batch2).expect("commit delete");
        db.sync_all().expect("sync after delete");
    }

    // Reopen — deleted key must be gone
    let db2 = OmniKV::open(&manifest, &wal).expect("reopen after delete");
    let got = db2.find_latest_internal("k7").expect("find deleted key");
    assert!(got.is_none(), "deleted key must not reappear after restart");
}

// ---------------------------------------------------------------------------
// Test 8 — 100 open/write/close/reopen cycles (durability stress)
// ---------------------------------------------------------------------------
#[test]
fn test_100_crash_recovery_cycles() {
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    for i in 0u32..100 {
        let key = format!("cycle_key_{i:03}");
        let val = format!("cycle_val_{i}");

        // Write
        {
            let db = OmniKV::open(&manifest, &wal).expect("open in cycle");
            let mut batch = WriteBatch::new();
            batch.set(&key, val.clone()).expect("set in cycle");
            db.commit_batch(&batch).expect("commit in cycle");
            db.sync_all().expect("sync in cycle");
        }

        // Verify immediately after reopen
        let db2 = OmniKV::open(&manifest, &wal).expect("reopen in cycle");
        let got = db2.find_latest_internal(&key).expect("find in cycle");
        assert_eq!(
            got.as_deref(),
            Some(val.as_str()),
            "cycle {i}: value must survive open/write/close/reopen"
        );
    }
}

// ---------------------------------------------------------------------------
// Test 9 — WAL replay rebuilds memtable correctly (multi-batch)
// ---------------------------------------------------------------------------
#[test]
fn test_wal_replay_multi_batch() {
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    // Write 3 separate batches
    {
        let db = OmniKV::open(&manifest, &wal).expect("open");

        for round in 0u32..3 {
            let mut batch = WriteBatch::new();
            for i in 0u32..10 {
                batch
                    .set(&format!("batch{round}_key{i}"), format!("v{round}_{i}"))
                    .expect("set");
            }
            db.commit_batch(&batch).expect("commit batch");
        }
        db.sync_all().expect("sync");
    }

    // Reopen — all 30 keys must be present
    let db2 = OmniKV::open(&manifest, &wal).expect("reopen after multi-batch");
    for round in 0u32..3 {
        for i in 0u32..10 {
            let got = db2
                .find_latest_internal(&format!("batch{round}_key{i}"))
                .expect("find");
            assert_eq!(
                got.as_deref(),
                Some(format!("v{round}_{i}").as_str()),
                "batch{round}_key{i} must survive WAL replay"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Test 10 — overwrite survives restart (latest value wins)
// ---------------------------------------------------------------------------
#[test]
fn test_overwrite_survives_restart() {
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    // Write k10 = "original"
    write_and_close(&manifest, &wal, "k10", "original");

    // Reopen, overwrite k10 = "updated"
    {
        let db = OmniKV::open(&manifest, &wal).expect("reopen for overwrite");
        let mut batch = WriteBatch::new();
        batch
            .set("k10", "updated".to_string())
            .expect("overwrite set");
        db.commit_batch(&batch).expect("commit overwrite");
        db.sync_all().expect("sync overwrite");
    }

    // Reopen again — must see "updated"
    let db2 = OmniKV::open(&manifest, &wal).expect("final reopen");
    let got = db2.find_latest_internal("k10").expect("find overwritten key");
    assert_eq!(
        got.as_deref(),
        Some("updated"),
        "overwritten value must survive restart"
    );
}
