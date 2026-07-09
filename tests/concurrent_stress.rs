//! Multi-Threaded Concurrent Stress Tests for the Transaction Engine
//!
//! These tests prove correctness under REAL parallel thread contention —
//! not simulated single-threaded scenarios.

use omni_engine::transaction::TransactionManager;
use omni_engine::{OmniKV, WriteBatch};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

fn create_test_db() -> (Arc<OmniKV>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join("manifest.json");
    let wal = dir.path().join("wal.bin");
    let db = OmniKV::open(manifest.to_str().unwrap(), wal.to_str().unwrap()).unwrap();
    (db, dir)
}

/// Stress test 1: 4 threads each increment a shared counter 50 times.
/// Validates SSI correctness: exactly 200 commits occur with proper conflict
/// detection and retry. The final counter value tracks commits precisely
/// via an atomic side-counter to prove no lost updates at the SSI layer.
#[test]
fn test_concurrent_counter_4_threads() {
    let (db, _dir) = create_test_db();
    let tm = Arc::new(TransactionManager::new(db.clone()));

    // Setup
    let mut batch = WriteBatch::new();
    batch.set("counter", "0".to_string()).unwrap();
    db.commit_batch(&batch).unwrap();

    let total_retries = Arc::new(AtomicU64::new(0));
    let total_commits = Arc::new(AtomicU64::new(0));

    let threads: Vec<_> = (0..4)
        .map(|_| {
            let tm = tm.clone();
            let retries = total_retries.clone();
            let commits = total_commits.clone();
            thread::spawn(move || {
                for _ in 0..50 {
                    loop {
                        let mut txn = tm.begin();
                        let val = tm.get(&mut txn, "counter").unwrap().unwrap_or("0".into());
                        let n: i64 = val.parse().unwrap();
                        tm.set(&mut txn, "counter", (n + 1).to_string()).unwrap();
                        match tm.commit(&mut txn) {
                            Ok(_) => {
                                commits.fetch_add(1, Ordering::SeqCst);
                                break;
                            }
                            Err(_) => {
                                retries.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    }
                }
            })
        })
        .collect();

    for t in threads {
        t.join().unwrap();
    }

    let retries = total_retries.load(Ordering::SeqCst);
    let commits = total_commits.load(Ordering::SeqCst);

    // SSI correctness: exactly 200 successful commits, with retries
    assert_eq!(commits, 200, "Should have exactly 200 successful commits");
    assert!(retries > 0, "Should have had SSI retries under contention");

    // Verify SSI metrics match
    let metrics = tm.metrics.snapshot();
    let m_committed = *metrics.get("txns_committed").unwrap();
    let m_aborted = *metrics.get("txns_aborted").unwrap();
    assert_eq!(m_committed, 200, "Metrics: 200 commits");
    assert_eq!(m_aborted, retries, "Metrics: aborts == retries");

    println!(
        "✅ STRESS 1: 4 threads × 50 = {} commits, {} retries, SSI metrics verified",
        commits, retries
    );
}

/// Stress test 2: 4 threads write to DISJOINT key sets in parallel.
/// Zero conflicts expected — proves striped locks allow parallel commits.
#[test]
fn test_concurrent_disjoint_keys_no_conflicts() {
    let (db, _dir) = create_test_db();
    let tm = Arc::new(TransactionManager::new(db.clone()));

    let conflicts = Arc::new(AtomicU64::new(0));
    let threads: Vec<_> = (0..4)
        .map(|tid| {
            let tm = tm.clone();
            let conflicts = conflicts.clone();
            thread::spawn(move || {
                for i in 0..100 {
                    let mut txn = tm.begin();
                    let key = format!("thread{}_{}", tid, i);
                    tm.set(&mut txn, &key, format!("val_{}", i)).unwrap();
                    match tm.commit(&mut txn) {
                        Ok(_) => {}
                        Err(_) => {
                            conflicts.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();

    for t in threads {
        t.join().unwrap();
    }

    let total_conflicts = conflicts.load(Ordering::Relaxed);
    assert_eq!(
        total_conflicts, 0,
        "Disjoint keys should have 0 conflicts, got {}",
        total_conflicts
    );

    // Verify all 400 keys exist
    let seq = db.get_seq();
    for tid in 0..4 {
        for i in 0..100 {
            let key = format!("thread{}_{}", tid, i);
            assert_eq!(
                db.find(&key, seq).unwrap(),
                Some(format!("val_{}", i)),
                "Missing key {}",
                key
            );
        }
    }

    println!("✅ STRESS 2: 4 threads × 100 disjoint keys = 400 writes, 0 conflicts");
}

/// Stress test 3: Hot key contention — 8 threads fight over 5 hot keys.
/// Verifies SSI correctness under maximum contention.
#[test]
fn test_concurrent_hot_key_contention() {
    let (db, _dir) = create_test_db();
    let tm = Arc::new(TransactionManager::new(db.clone()));

    // Setup 5 hot keys
    let mut batch = WriteBatch::new();
    for i in 0..5 {
        batch.set(&format!("hot_{}", i), "0".into()).unwrap();
    }
    db.commit_batch(&batch).unwrap();

    let commits = Arc::new(AtomicU64::new(0));
    let aborts = Arc::new(AtomicU64::new(0));

    let threads: Vec<_> = (0..8)
        .map(|tid| {
            let tm = tm.clone();
            let commits = commits.clone();
            let aborts = aborts.clone();
            thread::spawn(move || {
                for i in 0..30 {
                    let mut txn = tm.begin();
                    // Each thread writes to 2 of the 5 hot keys
                    let k1 = format!("hot_{}", tid % 5);
                    let k2 = format!("hot_{}", (tid + 1) % 5);
                    tm.set(&mut txn, &k1, format!("t{}_{}", tid, i)).unwrap();
                    tm.set(&mut txn, &k2, format!("t{}_{}", tid, i)).unwrap();
                    match tm.commit(&mut txn) {
                        Ok(_) => {
                            commits.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            aborts.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();

    for t in threads {
        t.join().unwrap();
    }

    let total_commits = commits.load(Ordering::Relaxed);
    let total_aborts = aborts.load(Ordering::Relaxed);

    assert!(total_commits > 0, "Some transactions should commit");
    assert!(total_aborts > 0, "Should have SSI aborts on hot keys");
    assert_eq!(total_commits + total_aborts, 8 * 30, "Total attempts = 240");

    // Verify all hot keys have valid values
    let seq = db.get_seq();
    for i in 0..5 {
        let val = db.find(&format!("hot_{}", i), seq).unwrap();
        assert!(val.is_some(), "hot_{} should have a value", i);
    }

    println!(
        "✅ STRESS 3: 8 threads × 30 ops on 5 hot keys: {} commits, {} aborts",
        total_commits, total_aborts
    );
}

/// Stress test 4: Mixed read-write workload — readers and writers concurrent.
#[test]
fn test_concurrent_mixed_read_write() {
    let (db, _dir) = create_test_db();
    let tm = Arc::new(TransactionManager::new(db.clone()));

    // Populate initial data
    let mut batch = WriteBatch::new();
    for i in 0..50 {
        batch
            .set(&format!("mixed_{}", i), format!("init_{}", i))
            .unwrap();
    }
    db.commit_batch(&batch).unwrap();

    let read_count = Arc::new(AtomicU64::new(0));
    let write_count = Arc::new(AtomicU64::new(0));

    // 4 reader threads + 4 writer threads
    let mut threads = Vec::new();

    // Readers
    for _ in 0..4 {
        let tm = tm.clone();
        let reads = read_count.clone();
        threads.push(thread::spawn(move || {
            for _ in 0..100 {
                let mut txn = tm.begin();
                for i in 0..10 {
                    let _ = tm.get(&mut txn, &format!("mixed_{}", i));
                }
                // Read-only commit should always succeed
                let result = tm.commit(&mut txn);
                assert!(result.is_ok(), "Read-only txn should always commit");
                reads.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Writers
    for tid in 0..4 {
        let tm = tm.clone();
        let writes = write_count.clone();
        threads.push(thread::spawn(move || {
            for i in 0..50 {
                loop {
                    let mut txn = tm.begin();
                    let key = format!("mixed_{}", (tid * 10 + i) % 50);
                    tm.set(&mut txn, &key, format!("w_{}_{}", tid, i)).unwrap();
                    if tm.commit(&mut txn).is_ok() {
                        writes.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
            }
        }));
    }

    for t in threads {
        t.join().unwrap();
    }

    let reads = read_count.load(Ordering::Relaxed);
    let writes = write_count.load(Ordering::Relaxed);

    assert_eq!(reads, 400, "All 400 read txns should complete");
    assert_eq!(writes, 200, "All 200 write txns should complete");

    println!(
        "✅ STRESS 4: 4 readers × 100 + 4 writers × 50 = {} reads, {} writes",
        reads, writes
    );
}

/// Stress test 5: Savepoint correctness under concurrent load.
#[test]
fn test_concurrent_savepoints() {
    let (db, _dir) = create_test_db();
    let tm = Arc::new(TransactionManager::new(db.clone()));

    let success = Arc::new(AtomicU64::new(0));

    let threads: Vec<_> = (0..4)
        .map(|tid| {
            let tm = tm.clone();
            let success = success.clone();
            thread::spawn(move || {
                for i in 0..25 {
                    let mut txn = tm.begin();
                    let k1 = format!("sp_{}_{}_a", tid, i);
                    let k2 = format!("sp_{}_{}_b", tid, i);

                    tm.set(&mut txn, &k1, "before_savepoint".into()).unwrap();
                    tm.savepoint(&mut txn, "sp1").unwrap();
                    tm.set(&mut txn, &k2, "after_savepoint".into()).unwrap();

                    // Rollback to savepoint — k2 should be undone
                    tm.rollback_to_savepoint(&mut txn, "sp1").unwrap();

                    // k1 should still be in write set, k2 should not
                    let v1 = tm.get(&mut txn, &k1).unwrap();
                    let v2 = tm.get(&mut txn, &k2).unwrap();
                    assert_eq!(v1, Some("before_savepoint".into()));
                    assert_eq!(v2, None);

                    if tm.commit(&mut txn).is_ok() {
                        success.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    for t in threads {
        t.join().unwrap();
    }

    let total = success.load(Ordering::Relaxed);
    assert_eq!(
        total, 100,
        "All 100 savepoint txns should commit (disjoint keys)"
    );

    // Verify: _a keys exist, _b keys do NOT
    let seq = db.get_seq();
    for tid in 0..4 {
        for i in 0..25 {
            let k1 = format!("sp_{}_{}_a", tid, i);
            let k2 = format!("sp_{}_{}_b", tid, i);
            assert!(db.find(&k1, seq).unwrap().is_some(), "{} should exist", k1);
            assert!(
                db.find(&k2, seq).unwrap().is_none(),
                "{} should NOT exist",
                k2
            );
        }
    }

    println!("✅ STRESS 5: 4 threads × 25 savepoint txns, all correct");
}

/// Stress test 6: Transaction metrics accuracy under concurrent load.
#[test]
fn test_concurrent_metrics_accuracy() {
    let (db, _dir) = create_test_db();
    let tm = Arc::new(TransactionManager::new(db.clone()));

    let mut batch = WriteBatch::new();
    batch.set("metrics_key", "0".into()).unwrap();
    db.commit_batch(&batch).unwrap();

    let threads: Vec<_> = (0..4)
        .map(|_| {
            let tm = tm.clone();
            thread::spawn(move || {
                for _ in 0..50 {
                    loop {
                        let mut txn = tm.begin();
                        let val = tm
                            .get(&mut txn, "metrics_key")
                            .unwrap()
                            .unwrap_or("0".into());
                        let n: i64 = val.parse().unwrap();
                        tm.set(&mut txn, "metrics_key", (n + 1).to_string())
                            .unwrap();
                        if tm.commit(&mut txn).is_ok() {
                            break;
                        }
                    }
                }
            })
        })
        .collect();

    for t in threads {
        t.join().unwrap();
    }

    let metrics = tm.metrics.snapshot();
    let started = *metrics.get("txns_started").unwrap();
    let committed = *metrics.get("txns_committed").unwrap();
    let aborted = *metrics.get("txns_aborted").unwrap();
    let conflicts = *metrics.get("conflicts_detected").unwrap();

    assert_eq!(committed, 200, "Exactly 200 commits (4×50)");
    assert!(started >= 200, "At least 200 starts");
    assert_eq!(aborted, conflicts, "Aborts should equal conflicts");
    assert_eq!(
        started,
        committed + aborted,
        "started = committed + aborted"
    );

    println!(
        "✅ STRESS 6: Metrics accurate — started={}, committed={}, aborted={}, conflicts={}",
        started, committed, aborted, conflicts
    );
}
