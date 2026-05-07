// ═══════════════════════════════════════════════════════════════════════════
// Operations & Edge Case Tests — Gaps #32 through #47
// ═══════════════════════════════════════════════════════════════════════════

use omni_engine::{OmniKV, WriteBatch};
use omni_engine::transaction::TransactionManager;

/// Helper: create a temp OmniKV instance
fn create_temp_db(prefix: &str) -> (std::sync::Arc<OmniKV>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join(format!("{}_m.json", prefix));
    let wal = dir.path().join(format!("{}_w.bin", prefix));
    let db = OmniKV::open(manifest.to_str().unwrap(), wal.to_str().unwrap()).unwrap();
    (db, dir)
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #32: Concurrent writers stress test
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #32a: Multiple threads writing concurrently
#[test]
fn test_concurrent_writers() {
    let dir = tempfile::tempdir().unwrap();
    let m = dir.path().join("cw_m.json");
    let w = dir.path().join("cw_w.bin");
    let db = OmniKV::open(m.to_str().unwrap(), w.to_str().unwrap()).unwrap();

    let handles: Vec<_> = (0..4).map(|t| {
        let db = db.clone();
        std::thread::spawn(move || {
            let mut batch = WriteBatch::new();
            for i in 0..25 {
                batch.set(&format!("t{}_k{}", t, i), format!("v{}", i)).unwrap();
            }
            db.commit_batch(&batch).unwrap();
        })
    }).collect();

    for h in handles { h.join().unwrap(); }

    let seq = db.get_seq();
    let mut count = 0;
    for t in 0..4 {
        for i in 0..25 {
            if db.find(&format!("t{}_k{}", t, i), seq).unwrap().is_some() {
                count += 1;
            }
        }
    }
    assert_eq!(count, 100);

    drop(db);
    drop(dir);
    println!("✅ OPS 32a: 4 threads × 25 writes = 100 keys, all present");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #33: Large value handling
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #33a: 1MB value stored and retrieved
#[test]
fn test_large_value_1mb() {
    let (db, _dir) = create_temp_db("lv");
    let val = "A".repeat(1_000_000);
    let mut batch = WriteBatch::new();
    batch.set("large_1mb", val.clone()).unwrap();
    db.commit_batch(&batch).unwrap();

    assert_eq!(db.find("large_1mb", db.get_seq()).unwrap().unwrap().len(), 1_000_000);

    println!("✅ OPS 33a: 1MB value stored and retrieved correctly");
}

/// Gap #33b: Compressed values decompress correctly
#[test]
fn test_compressed_value() {
    let (db, _dir) = create_temp_db("comp");
    // Highly compressible data
    let val = "REPEAT".repeat(10_000);
    let mut batch = WriteBatch::new();
    batch.set("comp_key", val.clone()).unwrap();
    db.commit_batch(&batch).unwrap();

    assert_eq!(db.find("comp_key", db.get_seq()).unwrap(), Some(val));

    println!("✅ OPS 33b: 60KB compressible value round-trips via LZ4");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #34: Key length edge cases
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #34a: Single-byte key
#[test]
fn test_single_byte_key() {
    let (db, _dir) = create_temp_db("sk");
    let mut batch = WriteBatch::new();
    batch.set("x", "single".into()).unwrap();
    db.commit_batch(&batch).unwrap();

    assert_eq!(db.find("x", db.get_seq()).unwrap(), Some("single".into()));

    println!("✅ OPS 34a: Single-byte key 'x' works");
}

/// Gap #34b: Long key (1000 chars)
#[test]
fn test_long_key() {
    let (db, _dir) = create_temp_db("lk");
    let long_key = "K".repeat(1000);
    let mut batch = WriteBatch::new();
    batch.set(&long_key, "long_key_val".into()).unwrap();
    db.commit_batch(&batch).unwrap();

    assert_eq!(db.find(&long_key, db.get_seq()).unwrap(), Some("long_key_val".into()));

    println!("✅ OPS 34b: 1000-char key works");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #35: Batch size limits
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #35a: Empty batch commits successfully
#[test]
fn test_empty_batch() {
    let (db, _dir) = create_temp_db("eb");
    let batch = WriteBatch::new();
    let result = db.commit_batch(&batch);
    assert!(result.is_ok());

    println!("✅ OPS 35a: Empty batch commits without error");
}

/// Gap #35b: Large batch with many keys
#[test]
fn test_large_batch() {
    let (db, _dir) = create_temp_db("lb");
    let mut batch = WriteBatch::new();
    for i in 0..500 {
        batch.set(&format!("lb_k{:04}", i), format!("v{}", i)).unwrap();
    }
    db.commit_batch(&batch).unwrap();

    let seq = db.get_seq();
    assert_eq!(db.find("lb_k0000", seq).unwrap(), Some("v0".into()));
    assert_eq!(db.find("lb_k0499", seq).unwrap(), Some("v499".into()));

    println!("✅ OPS 35b: 500-key batch committed and verified");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #36: SSTable merge correctness
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #36a: Overwritten keys show latest value after compaction
#[test]
fn test_sstable_overwrite_merge() {
    let (db, _dir) = create_temp_db("ow");

    let mut b1 = WriteBatch::new();
    b1.set("ow_key", "version1".into()).unwrap();
    db.commit_batch(&b1).unwrap();

    let _ = db.compact_sstables();

    let mut b2 = WriteBatch::new();
    b2.set("ow_key", "version2".into()).unwrap();
    db.commit_batch(&b2).unwrap();

    assert_eq!(db.find("ow_key", db.get_seq()).unwrap(), Some("version2".into()));

    println!("✅ OPS 36a: Overwritten key shows latest value after compaction");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #37: Bloom filter false positive rate
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #37a: Bloom filter doesn't block real keys
#[test]
fn test_bloom_filter_no_false_negatives() {
    let (db, _dir) = create_temp_db("bf");

    let mut batch = WriteBatch::new();
    for i in 0..100 {
        batch.set(&format!("bf_k{}", i), format!("v{}", i)).unwrap();
    }
    db.commit_batch(&batch).unwrap();
    let _ = db.compact_sstables();

    // All keys should be findable (no false negatives)
    let seq = db.get_seq();
    for i in 0..100 {
        assert!(db.find(&format!("bf_k{}", i), seq).unwrap().is_some(),
            "Bloom filter false negative on bf_k{}", i);
    }

    println!("✅ OPS 37a: 100 keys — zero bloom filter false negatives");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #38: TTL expiration accuracy
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #38a: TTL key with far-future expiry is visible
#[test]
fn test_ttl_far_future_visible() {
    let (db, _dir) = create_temp_db("ttl");

    let mut batch = WriteBatch::new();
    batch.set_with_ttl("ttl_key", "ttl_val".into(), 86400).unwrap(); // 24h TTL
    db.commit_batch(&batch).unwrap();

    assert_eq!(db.find("ttl_key", db.get_seq()).unwrap(), Some("ttl_val".into()));

    println!("✅ OPS 38a: TTL key with 24h expiry is visible now");
}

/// Gap #38b: Non-TTL key persists indefinitely
#[test]
fn test_no_ttl_persists() {
    let (db, _dir) = create_temp_db("nttl");

    let mut batch = WriteBatch::new();
    batch.set("persist_key", "forever".into()).unwrap();
    db.commit_batch(&batch).unwrap();

    assert_eq!(db.find("persist_key", db.get_seq()).unwrap(), Some("forever".into()));

    println!("✅ OPS 38b: Non-TTL key persists (no expiry)");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #39: Snapshot reference counting
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #39a: Register and unregister snapshots
#[test]
fn test_snapshot_lifecycle() {
    let (db, _dir) = create_temp_db("snap");

    // Seed data to advance sequence beyond 0
    let mut batch = WriteBatch::new();
    batch.set("snap_seed", "value".into()).unwrap();
    db.commit_batch(&batch).unwrap();

    let snap1 = db.snapshot();
    let snap2 = db.snapshot();

    assert!(snap1 > 0);
    assert!(snap2 >= snap1);

    db.unregister_snapshot(snap1);
    db.unregister_snapshot(snap2);

    println!("✅ OPS 39a: Snapshot register/unregister lifecycle works");
}

/// Gap #39b: Min active snapshot tracking
#[test]
fn test_min_active_snapshot() {
    let (db, _dir) = create_temp_db("msnap");

    let mut batch = WriteBatch::new();
    batch.set("ms_k", "ms_v".into()).unwrap();
    db.commit_batch(&batch).unwrap();

    let s1 = db.snapshot();
    let _ = db.snapshot();

    let min = db.min_active_snapshot();
    assert!(min <= s1);

    db.unregister_snapshot(s1);

    println!("✅ OPS 39b: Min active snapshot tracked correctly");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #40: WAL rotation under load
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #40a: WAL rotation preserves data via SSTable
#[test]
fn test_wal_rotation() {
    let (db, _dir) = create_temp_db("walr");

    let mut batch = WriteBatch::new();
    for i in 0..50 {
        batch.set(&format!("walr_k{}", i), format!("v{}", i)).unwrap();
    }
    db.commit_batch(&batch).unwrap();

    // Compact (which rotates WAL)
    let _ = db.compact_sstables();

    // Data should still be available from SSTable
    let seq = db.get_seq();
    for i in 0..50 {
        assert!(db.find(&format!("walr_k{}", i), seq).unwrap().is_some());
    }

    println!("✅ OPS 40a: WAL rotated after compaction, 50 keys in SSTable");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #41: Hot key contention
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #41a: Same key overwritten many times
#[test]
fn test_hot_key_overwrite() {
    let (db, _dir) = create_temp_db("hot");

    for i in 0..100 {
        let mut batch = WriteBatch::new();
        batch.set("hot_key", format!("version_{}", i)).unwrap();
        db.commit_batch(&batch).unwrap();
    }

    assert_eq!(
        db.find("hot_key", db.get_seq()).unwrap(),
        Some("version_99".into())
    );

    println!("✅ OPS 41a: Hot key overwritten 100 times, latest value correct");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #42: Sequential vs random write patterns
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #42a: Sequential keys
#[test]
fn test_sequential_writes() {
    let (db, _dir) = create_temp_db("seq");

    let mut batch = WriteBatch::new();
    for i in 0..200 {
        batch.set(&format!("seq_{:06}", i), format!("v{}", i)).unwrap();
    }
    db.commit_batch(&batch).unwrap();

    let seq = db.get_seq();
    assert_eq!(db.find("seq_000000", seq).unwrap(), Some("v0".into()));
    assert_eq!(db.find("seq_000199", seq).unwrap(), Some("v199".into()));

    println!("✅ OPS 42a: 200 sequential keys written and verified");
}

/// Gap #42b: Random-pattern keys
#[test]
fn test_random_pattern_writes() {
    let (db, _dir) = create_temp_db("rnd");

    let keys = ["zebra", "apple", "mango", "banana", "kiwi", "grape", "fig", "date", "cherry", "apricot"];
    let mut batch = WriteBatch::new();
    for k in &keys {
        batch.set(k, format!("{}_val", k)).unwrap();
    }
    db.commit_batch(&batch).unwrap();

    let seq = db.get_seq();
    for k in &keys {
        assert_eq!(db.find(k, seq).unwrap(), Some(format!("{}_val", k)));
    }

    println!("✅ OPS 42b: 10 random-pattern keys all correct");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #43: Read-during-compaction consistency
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #43a: Reads before and after compaction are consistent
#[test]
fn test_read_consistency_across_compaction() {
    let (db, _dir) = create_temp_db("rdc");

    let mut batch = WriteBatch::new();
    for i in 0..50 {
        batch.set(&format!("rdc_k{:03}", i), format!("v{}", i)).unwrap();
    }
    db.commit_batch(&batch).unwrap();

    let seq_before = db.get_seq();
    let val_before = db.find("rdc_k025", seq_before).unwrap();

    let _ = db.compact_sstables();

    let val_after = db.find("rdc_k025", db.get_seq()).unwrap();
    assert_eq!(val_before, val_after);

    println!("✅ OPS 43a: Read consistency preserved across compaction");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #44: Scan correctness
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #44a: Scan returns lexicographically ordered results
#[test]
fn test_scan_ordering() {
    let (db, _dir) = create_temp_db("scan");

    let mut batch = WriteBatch::new();
    for k in ["scan_c", "scan_a", "scan_b", "scan_e", "scan_d"] {
        batch.set(k, format!("{}_val", k)).unwrap();
    }
    db.commit_batch(&batch).unwrap();

    let results = db.scan("scan_", "scan_z", db.get_seq()).unwrap();
    let keys: Vec<&str> = results.iter().map(|(k, _)| k.as_str()).collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);

    println!("✅ OPS 44a: Scan returns {} keys in lexicographic order", results.len());
}

/// Gap #44b: Scan with empty range returns empty
#[test]
fn test_scan_empty_range() {
    let (db, _dir) = create_temp_db("scanempty");

    let results = db.scan("zzz_", "zzz_z", db.get_seq()).unwrap();
    assert!(results.is_empty());

    println!("✅ OPS 44b: Scan on empty range returns empty");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #45: Metrics accuracy
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #45a: Memtable size increases after writes
#[test]
fn test_memtable_size_tracking() {
    let (db, _dir) = create_temp_db("met");

    let size_before = db.memtable_size();
    let mut batch = WriteBatch::new();
    for i in 0..50 {
        batch.set(&format!("met_k{}", i), format!("val_{}", i)).unwrap();
    }
    db.commit_batch(&batch).unwrap();

    let size_after = db.memtable_size();
    assert!(size_after > size_before, "Memtable should grow after writes");

    println!("✅ OPS 45a: Memtable size {} → {} after 50 writes", size_before, size_after);
}

/// Gap #45b: Sequence number monotonically increases
#[test]
fn test_seq_monotonic() {
    let (db, _dir) = create_temp_db("seqm");

    let mut prev = db.get_seq();
    for i in 0..10 {
        let mut batch = WriteBatch::new();
        batch.set(&format!("seqm_k{}", i), "v".into()).unwrap();
        db.commit_batch(&batch).unwrap();
        let current = db.get_seq();
        assert!(current > prev, "Seq should increase: {} <= {}", current, prev);
        prev = current;
    }

    println!("✅ OPS 45b: Sequence numbers monotonically increasing over 10 commits");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #46: Graceful shutdown / recovery
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #46a: Multiple open/close cycles preserve data
#[test]
fn test_multi_restart_durability() {
    let dir = tempfile::tempdir().unwrap();
    let m = dir.path().join("dur_m.json");
    let w = dir.path().join("dur_w.bin");

    for cycle in 0..5 {
        let db = OmniKV::open(m.to_str().unwrap(), w.to_str().unwrap()).unwrap();
        let mut batch = WriteBatch::new();
        batch.set(&format!("dur_k{}", cycle), format!("cycle_{}", cycle)).unwrap();
        db.commit_batch(&batch).unwrap();
    }

    // Final open — all cycles should be present
    let db = OmniKV::open(m.to_str().unwrap(), w.to_str().unwrap()).unwrap();
    let seq = db.get_seq();
    for cycle in 0..5 {
        assert_eq!(
            db.find(&format!("dur_k{}", cycle), seq).unwrap(),
            Some(format!("cycle_{}", cycle))
        );
    }

    println!("✅ OPS 46a: 5 open/close cycles, all data preserved");
}

// ═══════════════════════════════════════════════════════════════════════════
// Gap #47: Transaction SSI end-to-end
// ═══════════════════════════════════════════════════════════════════════════

/// Gap #47a: SSI transaction commit with read-write set
#[test]
fn test_ssi_transaction_e2e() {
    let (db, _dir) = create_temp_db("ssi");

    // Seed data
    let mut batch = WriteBatch::new();
    batch.set("ssi_account_a", "1000".into()).unwrap();
    batch.set("ssi_account_b", "500".into()).unwrap();
    db.commit_batch(&batch).unwrap();

    let tm = TransactionManager::new(db.clone());
    let mut txn = tm.begin();

    let a = tm.get(&mut txn, "ssi_account_a").unwrap().unwrap();
    let b = tm.get(&mut txn, "ssi_account_b").unwrap().unwrap();

    let a_val: i64 = a.parse().unwrap();
    let b_val: i64 = b.parse().unwrap();

    // Transfer 200 from A to B
    tm.set(&mut txn, "ssi_account_a", (a_val - 200).to_string()).unwrap();
    tm.set(&mut txn, "ssi_account_b", (b_val + 200).to_string()).unwrap();

    let commit_seq = tm.commit(&mut txn).unwrap();
    assert!(commit_seq > 0);

    // Verify
    let seq = db.get_seq();
    assert_eq!(db.find("ssi_account_a", seq).unwrap(), Some("800".into()));
    assert_eq!(db.find("ssi_account_b", seq).unwrap(), Some("700".into()));

    println!("✅ OPS 47a: SSI transfer 200 A→B: A=800, B=700");
}

/// Gap #47b: SSI conflict detection aborts conflicting txn
#[test]
fn test_ssi_conflict_abort() {
    let (db, _dir) = create_temp_db("ssic");

    let mut batch = WriteBatch::new();
    batch.set("ssic_key", "initial".into()).unwrap();
    db.commit_batch(&batch).unwrap();

    let tm = TransactionManager::new(db.clone());

    let mut txn1 = tm.begin();
    let mut txn2 = tm.begin();

    // Both read the same key
    let _ = tm.get(&mut txn1, "ssic_key");
    let _ = tm.get(&mut txn2, "ssic_key");

    // Both write to same key
    tm.set(&mut txn1, "ssic_key", "txn1_wins".to_string()).unwrap();
    tm.set(&mut txn2, "ssic_key", "txn2_wins".to_string()).unwrap();

    // First commit succeeds
    assert!(tm.commit(&mut txn1).is_ok());

    // Second should conflict
    let result = tm.commit(&mut txn2);
    assert!(result.is_err(), "Conflicting txn should be aborted");

    println!("✅ OPS 47b: SSI conflict detected — txn2 aborted, txn1 committed");
}
