//! Operational Maturity Tests
//!
//! Tests for configuration, diagnostics, metrics, health, rate limiting,
//! group commit, graceful shutdown, and crash recovery.

use omni_engine::hardening::{GroupCommitEngine, RateLimiter};
use omni_engine::metrics_prometheus;
use omni_engine::ops::{DiagnosticReport, LogFormat, OmniConfig};
use omni_engine::{OmniKV, WriteBatch};
use std::sync::Arc;
use std::time::Instant;

fn create_db() -> (Arc<OmniKV>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let m = dir.path().join("manifest.json");
    let w = dir.path().join("wal.bin");
    let db = OmniKV::open(m.to_str().unwrap(), w.to_str().unwrap()).unwrap();
    (db, dir)
}

// ─── Configuration ──────────────────────────────────────────

#[test]
fn test_config_defaults() {
    let cfg = OmniConfig::default();
    assert_eq!(cfg.memtable_flush_threshold, 10_000);
    assert_eq!(cfg.l0_compaction_trigger, 4);
    assert_eq!(cfg.l0_write_stall_threshold, 12);
    assert_eq!(cfg.block_cache_capacity, 100_000);
    assert_eq!(cfg.http_addr, "0.0.0.0:8443");
    assert_eq!(cfg.pgwire_addr, "0.0.0.0:5433");
    assert_eq!(cfg.log_format, LogFormat::Json);
    assert!(
        cfg.validate().is_err(),
        "production validation must fail without explicit runtime secrets"
    );
    println!("✅ OPS: Config defaults are sane and fail closed without secrets");
}

#[test]
fn test_config_from_env() {
    // Set some env vars
    unsafe {
        std::env::set_var("OMNI_MEMTABLE_FLUSH", "5000");
        std::env::set_var("OMNI_LOG_FORMAT", "pretty");
        std::env::set_var("OMNI_RATE_LIMIT", "500");
        std::env::set_var("OMNI_JWT_SECRET", "0123456789abcdef0123456789abcdef");
        std::env::set_var(
            "OMNI_BOOTSTRAP_ADMIN_KEY",
            "bootstrap-admin-key-0123456789abcdef",
        );
    }

    let cfg = OmniConfig::from_env().unwrap();
    assert_eq!(cfg.memtable_flush_threshold, 5000);
    assert_eq!(cfg.log_format, LogFormat::Pretty);
    assert!((cfg.rate_limit_per_sec - 500.0).abs() < f64::EPSILON);
    assert!(cfg.validate().is_ok());

    // Cleanup
    unsafe {
        std::env::remove_var("OMNI_MEMTABLE_FLUSH");
        std::env::remove_var("OMNI_LOG_FORMAT");
        std::env::remove_var("OMNI_RATE_LIMIT");
        std::env::remove_var("OMNI_JWT_SECRET");
        std::env::remove_var("OMNI_BOOTSTRAP_ADMIN_KEY");
    }
    println!("✅ OPS: Config loads from env vars correctly");
}

#[test]
fn test_config_from_env_rejects_invalid_numeric_values() {
    unsafe {
        std::env::set_var("OMNI_RATE_LIMIT", "fast");
    }

    let errors = OmniConfig::from_env().unwrap_err();
    assert!(
        errors.iter().any(|e| e.contains("OMNI_RATE_LIMIT")),
        "got: {errors:?}"
    );

    unsafe {
        std::env::remove_var("OMNI_RATE_LIMIT");
    }
}

#[test]
fn test_config_from_env_rejects_invalid_log_format() {
    unsafe {
        std::env::set_var("OMNI_LOG_FORMAT", "xml");
    }

    let errors = OmniConfig::from_env().unwrap_err();
    assert!(
        errors.iter().any(|e| e.contains("OMNI_LOG_FORMAT")),
        "got: {errors:?}"
    );

    unsafe {
        std::env::remove_var("OMNI_LOG_FORMAT");
    }
}

#[test]
fn test_config_validation_catches_errors() {
    let cfg = OmniConfig {
        memtable_flush_threshold: 0,
        ..OmniConfig::default()
    };
    let result = cfg.validate();
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(
        errors
            .iter()
            .any(|e| e.contains("memtable_flush_threshold"))
    );
    println!("✅ OPS: Config validation catches invalid settings");
}

#[test]
fn test_config_summary() {
    let cfg = OmniConfig::default();
    let summary = cfg.summary();
    assert!(summary.contains("memtable_flush"));
    assert!(summary.contains("cache"));
    assert!(summary.contains("rate_limit"));
    println!("✅ OPS: Config summary: {summary}");
}

// ─── Diagnostics ────────────────────────────────────────────

#[test]
fn test_diagnostic_report() {
    let (db, _d) = create_db();
    let cfg = OmniConfig::default();
    let start = Instant::now();

    // Write some data
    let mut b = WriteBatch::new();
    b.set("diag_key", "diag_value".into()).unwrap();
    db.commit_batch(&b).unwrap();

    let report = DiagnosticReport::from_db(&db, start, &cfg);
    assert_eq!(report.version, env!("CARGO_PKG_VERSION"));
    assert!(report.global_seq > 0);
    assert!(report.memtable_entries > 0);
    assert_eq!(report.l0_sstable_count, 0);
    assert!(report.heap_offset_bytes > 0);

    // Verify it serializes to JSON
    let json = serde_json::to_string_pretty(&report).unwrap();
    assert!(json.contains("global_seq"));
    assert!(json.contains("memtable_entries"));
    println!("✅ OPS: Diagnostic report generated:\n{json}");
}

#[test]
fn test_diagnostic_after_compaction() {
    let (db, _d) = create_db();
    let cfg = OmniConfig::default();
    let start = Instant::now();

    for i in 0..100 {
        let mut b = WriteBatch::new();
        b.set(&format!("diag_{i}"), format!("v_{i}")).unwrap();
        db.commit_batch(&b).unwrap();
    }
    db.compact_sstables().unwrap();

    let report = DiagnosticReport::from_db(&db, start, &cfg);
    assert!(report.l0_sstable_count >= 1);
    println!(
        "✅ OPS: Diagnostics reflect compaction state (L0={})",
        report.l0_sstable_count
    );
}

// ─── Prometheus Metrics ─────────────────────────────────────

#[test]
fn test_prometheus_metrics_render() {
    // Force lazy_static metric registration
    metrics_prometheus::COMMIT_RATE.inc();
    let output = metrics_prometheus::render_metrics();
    assert!(!output.is_empty(), "Prometheus output should not be empty");
    assert!(
        output.contains("omnikv_"),
        "Output should contain omnikv_ metrics"
    );
    println!("? OPS: Prometheus metrics render ({} bytes)", output.len());
}

#[test]
fn test_metrics_increment_on_writes() {
    let (db, _d) = create_db();
    let mut b = WriteBatch::new();
    b.set("metric_test", "value".into()).unwrap();
    db.commit_batch(&b).unwrap();

    // The commit_batch function updates COMMIT_RATE, WRITE_LATENCY, etc.
    let output = metrics_prometheus::render_metrics();
    assert!(output.contains("omnikv_commits_total") || output.contains("omnikv_write_latency"));
    println!("✅ OPS: Metrics update on write operations");
}

// ─── Rate Limiting ──────────────────────────────────────────

#[test]
fn test_rate_limiter_allows_within_burst() {
    let rl = RateLimiter::new(10.0, 5, 100);
    // First 5 should succeed (burst capacity)
    for _ in 0..5 {
        assert!(rl.try_acquire("user1").is_ok());
    }
    // 6th should be rate limited
    assert!(rl.try_acquire("user1").is_err());
    println!("✅ OPS: Rate limiter allows burst, then throttles");
}

#[test]
fn test_rate_limiter_per_user_isolation() {
    let rl = RateLimiter::new(10.0, 3, 100);
    // User1 exhausts their tokens
    for _ in 0..3 {
        rl.try_acquire("user1").unwrap();
    }
    assert!(rl.try_acquire("user1").is_err());

    // User2 still has tokens
    assert!(rl.try_acquire("user2").is_ok());
    assert_eq!(rl.tracked_users(), 2);
    println!("✅ OPS: Rate limiter isolates per-user buckets");
}

#[test]
fn test_rate_limiter_refill() {
    let rl = RateLimiter::new(1000.0, 2, 100);
    rl.try_acquire("user1").unwrap();
    rl.try_acquire("user1").unwrap();
    assert!(rl.try_acquire("user1").is_err());

    // Wait for refill
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert!(rl.try_acquire("user1").is_ok());
    println!("✅ OPS: Rate limiter refills tokens over time");
}

#[test]
fn test_rate_limiter_eviction() {
    let rl = RateLimiter::new(10.0, 5, 3);
    rl.try_acquire("user1").unwrap();
    rl.try_acquire("user2").unwrap();
    rl.try_acquire("user3").unwrap();
    assert_eq!(rl.tracked_users(), 3);

    // Adding user4 should evict the oldest
    rl.try_acquire("user4").unwrap();
    assert_eq!(rl.tracked_users(), 3);
    println!("✅ OPS: Rate limiter evicts oldest user at capacity");
}

#[test]
fn test_rate_limiter_reset() {
    let rl = RateLimiter::new(10.0, 2, 100);
    rl.try_acquire("user1").unwrap();
    rl.try_acquire("user1").unwrap();
    assert!(rl.try_acquire("user1").is_err());

    rl.reset_user("user1");
    assert!(rl.try_acquire("user1").is_ok()); // Fresh bucket
    println!("✅ OPS: Rate limiter user reset works");
}

// ─── Group Commit ───────────────────────────────────────────

#[test]
fn test_group_commit_single_leader() {
    let gc = GroupCommitEngine::new(100);
    let guard = gc.join_group();
    assert!(guard.is_leader);
    guard.mark_synced();
    let (epoch, pending) = gc.stats();
    assert!(epoch > 0);
    assert_eq!(pending, 0);
    println!("✅ OPS: Group commit single writer becomes leader");
}

#[test]
fn test_group_commit_stats() {
    let gc = GroupCommitEngine::new(50);
    let (epoch_before, _) = gc.stats();

    let guard = gc.join_group();
    guard.mark_synced();

    let (epoch_after, _) = gc.stats();
    assert!(epoch_after > epoch_before);
    println!("✅ OPS: Group commit epoch advances on sync");
}

// ─── WAL Recovery ───────────────────────────────────────────

#[test]
fn test_group_commit_late_joiner_waits_for_next_epoch() {
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::time::Duration;

    let gc = Arc::new(GroupCommitEngine::new(50));
    let first = gc.join_group();
    assert!(first.is_leader);

    let (tx, rx) = mpsc::channel();
    let gc_late = Arc::clone(&gc);
    let handle = std::thread::spawn(move || {
        let late = gc_late.join_group();
        let was_leader = late.is_leader;
        if was_leader {
            late.mark_synced();
        }
        tx.send(was_leader).expect("send late joiner result");
    });

    std::thread::sleep(Duration::from_millis(25));
    assert!(
        rx.try_recv().is_err(),
        "late joiner must not be released by the in-flight sync"
    );

    first.mark_synced();

    let late_was_leader = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("late joiner should complete next epoch");
    assert!(
        late_was_leader,
        "late joiner should lead the next safe sync epoch"
    );
    handle.join().expect("late joiner thread");

    let (epoch, pending) = gc.stats();
    assert_eq!(epoch, 2);
    assert_eq!(pending, 0);
}

#[test]
fn test_wal_crash_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let m = dir.path().join("manifest.json");
    let w = dir.path().join("wal.bin");

    // Write data and close
    {
        let db = OmniKV::open(m.to_str().unwrap(), w.to_str().unwrap()).unwrap();
        let mut b = WriteBatch::new();
        b.set("survive_crash", "important_data".into()).unwrap();
        db.commit_batch(&b).unwrap();
        // db drops here (simulates unclean shutdown)
    }

    // Reopen — WAL should recover the data
    {
        let db = OmniKV::open(m.to_str().unwrap(), w.to_str().unwrap()).unwrap();
        let seq = db.get_seq();
        let val = db.find("survive_crash", seq).unwrap();
        assert_eq!(val, Some("important_data".into()));
    }
    println!("✅ OPS: WAL crash recovery restores committed data");
}

#[test]
fn test_wal_recovery_multiple_batches() {
    let dir = tempfile::tempdir().unwrap();
    let m = dir.path().join("manifest.json");
    let w = dir.path().join("wal.bin");

    {
        let db = OmniKV::open(m.to_str().unwrap(), w.to_str().unwrap()).unwrap();
        for i in 0..50 {
            let mut b = WriteBatch::new();
            b.set(&format!("wal_{i:04}"), format!("v_{i}")).unwrap();
            db.commit_batch(&b).unwrap();
        }
    }

    {
        let db = OmniKV::open(m.to_str().unwrap(), w.to_str().unwrap()).unwrap();
        let seq = db.get_seq();
        for i in 0..50 {
            let v = db.find(&format!("wal_{i:04}"), seq).unwrap();
            assert!(v.is_some(), "Missing key wal_{i:04}");
        }
    }
    println!("✅ OPS: WAL recovery handles 50 batches across restart");
}

// ─── Snapshot Management ────────────────────────────────────

#[test]
fn test_snapshot_lifecycle() {
    let (db, _d) = create_db();
    let mut b = WriteBatch::new();
    b.set("snap_key", "v1".into()).unwrap();
    db.commit_batch(&b).unwrap();

    let snap = db.snapshot();
    assert!(snap > 0);

    // Snapshot should be tracked
    let min = db.min_active_snapshot();
    assert!(min <= snap);

    db.unregister_snapshot(snap);
    println!("✅ OPS: Snapshot register/unregister lifecycle works");
}

// ─── Connection Pool ────────────────────────────────────────

#[test]
fn test_connection_pool_creation() {
    let client = omni_engine::hardening::create_pooled_client(16, 5, 60);
    // Just verify it doesn't panic and is usable
    drop(client);

    let default_client = omni_engine::hardening::default_raft_client();
    drop(default_client);
    println!("✅ OPS: Connection pool creates with proper tuning");
}

// ─── Write Backpressure ─────────────────────────────────────

#[test]
fn test_write_stall_returns_error() {
    // Verify the WriteStall error type exists and formats correctly
    let err = omni_engine::OmniError::WriteStall;
    let msg = format!("{err}");
    assert!(msg.contains("WriteStall"));
    println!("✅ OPS: WriteStall error type works for backpressure signaling");
}

// ─── HdrHistogram Metrics ───────────────────────────────────

#[test]
fn test_hdr_histogram_latency_tracking() {
    let (db, _d) = create_db();

    // Do some writes
    for i in 0..10 {
        let mut b = WriteBatch::new();
        b.set(&format!("hist_{i}"), format!("v_{i}")).unwrap();
        db.commit_batch(&b).unwrap();
    }

    // Check commit latency histogram
    let hist = db.metrics.commit_latencies.lock().unwrap();
    assert!(!hist.is_empty(), "Should have recorded commit latencies");
    let p50 = hist.value_at_quantile(0.5);
    let p99 = hist.value_at_quantile(0.99);
    drop(hist);
    assert!(p99 >= p50, "p99 should be >= p50");
    println!("✅ OPS: HdrHistogram latency tracking — p50={p50}µs p99={p99}µs");
}

#[test]
fn test_hdr_histogram_read_latency() {
    let (db, _d) = create_db();
    let mut b = WriteBatch::new();
    b.set("read_lat", "value".into()).unwrap();
    db.commit_batch(&b).unwrap();

    let seq = db.get_seq();
    for _ in 0..10 {
        db.find("read_lat", seq).unwrap();
    }

    let hist = db.metrics.read_latencies.lock().unwrap();
    assert!(!hist.is_empty());
    let samples = hist.len();
    drop(hist);
    println!("✅ OPS: Read latency histogram records {samples} samples");
}

// ─── Error Handling ─────────────────────────────────────────

#[test]
fn test_error_types_display() {
    let errors = vec![
        omni_engine::OmniError::IoError("disk full".into()),
        omni_engine::OmniError::BatchTooLarge(10000),
        omni_engine::OmniError::ValueTooLarge(10_000_000),
        omni_engine::OmniError::KeyNotFound,
        omni_engine::OmniError::HashCollision,
        omni_engine::OmniError::LockPoisoned("test".into()),
        omni_engine::OmniError::WriteStall,
    ];

    for err in &errors {
        let msg = format!("{err}");
        assert!(!msg.is_empty());
    }
    println!("✅ OPS: All {} error types display correctly", errors.len());
}

#[test]
fn test_batch_too_large_rejected() {
    let mut b = WriteBatch::new();
    for i in 0..10_001 {
        if let Err(omni_engine::OmniError::BatchTooLarge(_)) = b.set(&format!("k{i}"), "v".into()) {
            println!("✅ OPS: BatchTooLarge rejected at {i} entries");
            return;
        }
    }
    panic!("Should have rejected batch at 10,000 entries");
}
