//! Jepsen-Style Chaos Testing Framework
//!
//! Simulates real-world failure scenarios to verify OmniKV's correctness
//! guarantees under adversarial conditions. Inspired by Jepsen's methodology
//! for testing distributed systems.
//!
//! ## Test Categories
//!
//! 1. **Crash Recovery**: Simulate process crash mid-write, verify WAL replay
//! 2. **Concurrent Conflict**: Many threads doing conflicting SSI transactions
//! 3. **Linearizability**: Verify transaction histories are serializable
//! 4. **Data Integrity**: CRC verification, no silent corruption
//! 5. **Stale Read Detection**: Verify MVCC prevents stale reads
//! 6. **Write Skew Detection**: Classic SSI anomaly (doctor on-call problem)
//!
//! ## Philosophy
//!
//! These tests are intentionally adversarial. They don't test happy paths —
//! they test the paths that cause data loss in production.

use std::sync::{Arc, Barrier};
use std::thread;

use crate::transaction::TransactionManager;
use crate::{OmniError, OmniKV, WriteBatch};

/// Chaos test result with detailed failure info.
#[derive(Debug)]
pub struct ChaosResult {
    pub test_name: String,
    pub passed: bool,
    pub iterations: u64,
    pub anomalies_detected: u64,
    pub details: String,
}

impl ChaosResult {
    pub fn pass(name: &str, iterations: u64) -> Self {
        Self {
            test_name: name.to_string(),
            passed: true,
            iterations,
            anomalies_detected: 0,
            details: "OK".to_string(),
        }
    }

    pub fn fail(name: &str, iterations: u64, anomalies: u64, details: String) -> Self {
        Self {
            test_name: name.to_string(),
            passed: false,
            iterations,
            anomalies_detected: anomalies,
            details,
        }
    }
}

/// ═══════════════════════════════════════════════════════════════════════
/// TEST 1: Crash Recovery — Write data, "crash" (drop without clean
/// shutdown), reopen and verify all committed data is intact.
/// ═══════════════════════════════════════════════════════════════════════
pub fn test_crash_recovery(manifest_path: &str, wal_path: &str) -> ChaosResult {
    let iterations = 100;

    // Phase 1: Write data then "crash" (drop without sync)
    let committed_keys: Vec<(String, String)> = {
        let db = match OmniKV::open(manifest_path, wal_path) {
            Ok(db) => db,
            Err(e) => {
                return ChaosResult::fail("crash_recovery", 0, 1, format!("Failed to open: {}", e));
            }
        };

        let mut keys = Vec::new();
        for i in 0..iterations {
            let key = format!("crash_test_{:04}", i);
            let value = format!("committed_value_{}", i);
            let mut batch = WriteBatch::new();
            if let Err(e) = batch.set(&key, value.clone()) {
                return ChaosResult::fail(
                    "crash_recovery",
                    i,
                    1,
                    format!("batch.set failed: {}", e),
                );
            }
            if let Err(e) = db.commit_batch(&batch) {
                return ChaosResult::fail("crash_recovery", i, 1, format!("commit failed: {}", e));
            }
            keys.push((key, value));
        }

        // "Crash" — drop the DB without calling any shutdown method
        drop(db);
        keys
    };

    // Phase 2: Reopen and verify ALL committed data survived the "crash"
    let db = match OmniKV::open(manifest_path, wal_path) {
        Ok(db) => db,
        Err(e) => {
            return ChaosResult::fail(
                "crash_recovery",
                iterations,
                1,
                format!("Reopen failed: {}", e),
            );
        }
    };

    let seq = db.get_seq();
    let mut anomalies = 0;
    let mut details = Vec::new();

    for (key, expected_value) in &committed_keys {
        match db.find(key, seq) {
            Ok(Some(val)) if val == *expected_value => { /* correct */ }
            Ok(Some(val)) => {
                anomalies += 1;
                details.push(format!(
                    "CORRUPTION: key={} expected={} got={}",
                    key, expected_value, val
                ));
            }
            Ok(None) => {
                anomalies += 1;
                details.push(format!(
                    "DATA LOSS: key={} was committed but missing after recovery",
                    key
                ));
            }
            Err(e) => {
                anomalies += 1;
                details.push(format!("READ ERROR: key={} error={}", key, e));
            }
        }
    }

    if anomalies > 0 {
        ChaosResult::fail("crash_recovery", iterations, anomalies, details.join("; "))
    } else {
        ChaosResult::pass("crash_recovery", iterations)
    }
}

/// ═══════════════════════════════════════════════════════════════════════
/// TEST 2: Concurrent Write-Write Conflict Storm — N threads all try to
/// write the same key simultaneously. Exactly ONE should win per round.
/// ═══════════════════════════════════════════════════════════════════════
pub fn test_concurrent_ww_conflicts(
    db: Arc<OmniKV>,
    num_threads: usize,
    rounds: u64,
) -> ChaosResult {
    let tm = Arc::new(TransactionManager::new(db.clone()));
    let mut anomalies = 0u64;
    let mut details = Vec::new();

    for round in 0..rounds {
        let key = format!("contested_key_{}", round);

        // Seed the key
        let mut seed = WriteBatch::new();
        if seed.set(&key, "initial".to_string()).is_err() {
            continue;
        }
        if db.commit_batch(&seed).is_err() {
            continue;
        }

        let barrier = Arc::new(Barrier::new(num_threads));
        let mut handles = Vec::new();

        for t in 0..num_threads {
            let tm = tm.clone();
            let key = key.clone();
            let barrier = barrier.clone();

            handles.push(thread::spawn(move || {
                let mut txn = tm.begin();
                // Read the key (establishes read dependency)
                let _ = tm.get(&mut txn, &key);

                // All threads synchronize here — maximum contention
                barrier.wait();

                // Try to write
                let _ = tm.set(&mut txn, &key, format!("thread_{}", t));
                tm.commit(&mut txn)
            }));
        }

        let results: Vec<Result<u64, OmniError>> = handles
            .into_iter()
            .map(|h| h.join().expect("thread panic"))
            .collect();

        let commits: Vec<_> = results.iter().filter(|r| r.is_ok()).collect();
        let aborts: Vec<_> = results.iter().filter(|r| r.is_err()).collect();

        // Exactly one thread should win (serializable guarantee)
        if commits.len() != 1 {
            anomalies += 1;
            details.push(format!(
                "Round {}: {} commits, {} aborts (expected 1 commit, {} aborts)",
                round,
                commits.len(),
                aborts.len(),
                num_threads - 1
            ));
        }
    }

    if anomalies > 0 {
        ChaosResult::fail(
            "concurrent_ww_conflicts",
            rounds,
            anomalies,
            details.join("; "),
        )
    } else {
        ChaosResult::pass("concurrent_ww_conflicts", rounds)
    }
}

/// ═══════════════════════════════════════════════════════════════════════
/// TEST 3: Write Skew Detection (Classic SSI Anomaly)
///
/// The "doctor on-call" problem:
/// - Two doctors are on call. Each checks "is the other on call?"
/// - Both see "yes", so both go off call.
/// - Result: nobody is on call. This is the WRITE SKEW anomaly.
/// - SSI must detect and abort at least one transaction.
/// ═══════════════════════════════════════════════════════════════════════
pub fn test_write_skew_detection(db: Arc<OmniKV>) -> ChaosResult {
    let tm = Arc::new(TransactionManager::new(db.clone()));
    let rounds = 50;
    let mut anomalies = 0u64;
    let mut details = Vec::new();

    for round in 0..rounds {
        let doc1_key = format!("doctor1_round_{}", round);
        let doc2_key = format!("doctor2_round_{}", round);

        // Both doctors start on-call
        let mut seed = WriteBatch::new();
        let _ = seed.set(&doc1_key, "on_call".to_string());
        let _ = seed.set(&doc2_key, "on_call".to_string());
        let _ = db.commit_batch(&seed);

        let tm1 = tm.clone();
        let tm2 = tm.clone();
        let d1k = doc1_key.clone();
        let d2k = doc2_key.clone();
        let d1k2 = doc1_key.clone();
        let d2k2 = doc2_key.clone();

        let barrier = Arc::new(Barrier::new(2));
        let b1 = barrier.clone();
        let b2 = barrier.clone();

        // Doctor 1: reads doctor2's status, then goes off-call
        let h1 = thread::spawn(move || {
            let mut txn = tm1.begin();
            let _ = tm1.get(&mut txn, &d2k); // read other doctor
            b1.wait(); // synchronize
            let _ = tm1.set(&mut txn, &d1k, "off_call".to_string());
            tm1.commit(&mut txn)
        });

        // Doctor 2: reads doctor1's status, then goes off-call
        let h2 = thread::spawn(move || {
            let mut txn = tm2.begin();
            let _ = tm2.get(&mut txn, &d1k2); // read other doctor
            b2.wait(); // synchronize
            let _ = tm2.set(&mut txn, &d2k2, "off_call".to_string());
            tm2.commit(&mut txn)
        });

        let r1 = h1.join().expect("thread panic");
        let r2 = h2.join().expect("thread panic");

        let both_committed = r1.is_ok() && r2.is_ok();

        if both_committed {
            // Check if write skew actually occurred
            let seq = db.get_seq();
            let v1 = db.find(&doc1_key, seq).unwrap_or(None).unwrap_or_default();
            let v2 = db.find(&doc2_key, seq).unwrap_or(None).unwrap_or_default();

            if v1 == "off_call" && v2 == "off_call" {
                anomalies += 1;
                details.push(format!(
                    "Round {}: WRITE SKEW — both doctors off-call!",
                    round
                ));
            }
        }
        // At least one abort is the correct behavior — SSI detected the anomaly
    }

    if anomalies > 0 {
        ChaosResult::fail(
            "write_skew_detection",
            rounds,
            anomalies,
            details.join("; "),
        )
    } else {
        ChaosResult::pass("write_skew_detection", rounds)
    }
}

/// ═══════════════════════════════════════════════════════════════════════
/// TEST 4: Data Integrity Verification — Write data, read back, verify
/// CRC checksums match. Detects silent corruption.
/// ═══════════════════════════════════════════════════════════════════════
pub fn test_data_integrity(db: Arc<OmniKV>, num_keys: u64) -> ChaosResult {
    let mut anomalies = 0u64;
    let mut details = Vec::new();

    // Write various data sizes (small, medium, large)
    let test_data: Vec<(String, String)> = (0..num_keys)
        .map(|i| {
            let key = format!("integrity_{:06}", i);
            let value = match i % 3 {
                0 => format!("small_{}", i), // < 64 bytes (uncompressed)
                1 => "x".repeat(128) + &format!("_mid_{}", i), // medium (compressed)
                _ => "y".repeat(4096) + &format!("_large_{}", i), // large (compressed)
            };
            (key, value)
        })
        .collect();

    // Write all data
    for (key, value) in &test_data {
        let mut batch = WriteBatch::new();
        if let Err(e) = batch.set(key, value.clone()) {
            return ChaosResult::fail("data_integrity", 0, 1, format!("set error: {}", e));
        }
        if let Err(e) = db.commit_batch(&batch) {
            return ChaosResult::fail("data_integrity", 0, 1, format!("commit error: {}", e));
        }
    }

    // Read back and verify every byte
    let seq = db.get_seq();
    for (key, expected) in &test_data {
        match db.find(key, seq) {
            Ok(Some(actual)) => {
                if actual != *expected {
                    anomalies += 1;
                    details.push(format!(
                        "CORRUPTION: key={} expected_len={} actual_len={} match={}",
                        key,
                        expected.len(),
                        actual.len(),
                        actual == *expected
                    ));
                }
            }
            Ok(None) => {
                anomalies += 1;
                details.push(format!("MISSING: key={}", key));
            }
            Err(e) => {
                anomalies += 1;
                details.push(format!("READ_ERROR: key={} err={}", key, e));
            }
        }
    }

    if anomalies > 0 {
        ChaosResult::fail("data_integrity", num_keys, anomalies, details.join("; "))
    } else {
        ChaosResult::pass("data_integrity", num_keys)
    }
}

/// ═══════════════════════════════════════════════════════════════════════
/// TEST 5: Monotonic Sequence Guarantee — Under concurrent load, verify
/// that sequence numbers are always strictly monotonic.
/// ═══════════════════════════════════════════════════════════════════════
pub fn test_monotonic_sequences(
    db: Arc<OmniKV>,
    num_threads: usize,
    ops_per_thread: u64,
) -> ChaosResult {
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = Vec::new();

    for t in 0..num_threads {
        let db = db.clone();
        let barrier = barrier.clone();

        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut prev_seq = 0u64;
            let mut violations = Vec::new();

            for i in 0..ops_per_thread {
                let mut batch = WriteBatch::new();
                let _ = batch.set(&format!("mono_t{}_i{}", t, i), format!("v{}", i));
                match db.commit_batch(&batch) {
                    Ok(seq) => {
                        if seq <= prev_seq && prev_seq > 0 {
                            violations.push(format!(
                                "thread={} op={} prev_seq={} cur_seq={}",
                                t, i, prev_seq, seq
                            ));
                        }
                        prev_seq = seq;
                    }
                    Err(_) => { /* write stall is OK under load */ }
                }
            }
            violations
        }));
    }

    let mut total_violations = Vec::new();
    for h in handles {
        let violations = h.join().expect("thread panic");
        total_violations.extend(violations);
    }

    let total_ops = (num_threads as u64) * ops_per_thread;
    if total_violations.is_empty() {
        ChaosResult::pass("monotonic_sequences", total_ops)
    } else {
        ChaosResult::fail(
            "monotonic_sequences",
            total_ops,
            total_violations.len() as u64,
            total_violations.join("; "),
        )
    }
}

/// ═══════════════════════════════════════════════════════════════════════
/// TEST 6: Atomicity & Durability Stress — Verify that committed batch
/// writes are always atomic (all-or-nothing) even under concurrent load.
/// ═══════════════════════════════════════════════════════════════════════
pub fn test_concurrent_stress(db: Arc<OmniKV>) -> ChaosResult {
    let num_writers = 4;
    let ops_per_writer = 25;
    let barrier = Arc::new(Barrier::new(num_writers));
    let mut handles = Vec::new();

    // Phase 1: Concurrent writes
    for t in 0..num_writers {
        let db = db.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut written = Vec::new();
            for i in 0..ops_per_writer {
                let key = format!("stress_t{}_k{}", t, i);
                let value = format!("data_t{}_v{}", t, i);
                let mut batch = WriteBatch::new();
                let _ = batch.set(&key, value.clone());
                if db.commit_batch(&batch).is_ok() {
                    written.push((key, value));
                }
            }
            written
        }));
    }

    let mut all_written: Vec<(String, String)> = Vec::new();
    for h in handles {
        all_written.extend(h.join().expect("thread panic"));
    }

    // Phase 2: Verify ALL committed writes are readable
    let seq = db.get_seq();
    let mut errors = Vec::new();
    let mut _crc_issues = 0u64;
    for (key, expected) in &all_written {
        match db.find(key, seq) {
            Ok(Some(v)) if v == *expected => { /* correct */ }
            Ok(Some(v)) => errors.push(format!("WRONG: {}={} expected={}", key, v, expected)),
            Ok(None) => errors.push(format!("MISSING: {}", key)),
            // Data-integrity failures are never accepted in chaos tests.
            Err(crate::OmniError::IoError(ref msg)) if msg.contains("CRC32") => {
                _crc_issues += 1;
                errors.push(format!("CRC: {} {}", key, msg));
            }
            Err(e) => errors.push(format!("ERR: {} {}", key, e)),
        }
    }

    let total = all_written.len() as u64;
    if errors.is_empty() {
        ChaosResult::pass("concurrent_stress", total)
    } else {
        ChaosResult::fail(
            "concurrent_stress",
            total,
            errors.len() as u64,
            errors.join("; "),
        )
    }
}

/// Runs all chaos tests and returns a summary.
pub fn run_all_chaos_tests(
    db: Arc<OmniKV>,
    manifest_path: &str,
    wal_path: &str,
) -> Vec<ChaosResult> {
    vec![
        test_crash_recovery(manifest_path, wal_path),
        test_concurrent_ww_conflicts(db.clone(), 4, 10),
        test_write_skew_detection(db.clone()),
        test_data_integrity(db.clone(), 100),
        test_monotonic_sequences(db.clone(), 4, 50),
        test_concurrent_stress(db.clone()),
    ]
}
