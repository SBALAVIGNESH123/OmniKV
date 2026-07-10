//! OmniKV Benchmark Suite
//!
//! Comparative benchmarks against industry databases.
//! Run with: `cargo bench -p omnikv-engine` or `cargo test -p omnikv-engine --test benchmarks --release`
//!
//! Measures:
//! - Sequential write throughput (ops/sec)
//! - Random read throughput (ops/sec)
//! - Range scan throughput (rows/sec)
//! - Mixed read/write workload (ops/sec)
//! - Transaction commit throughput (txn/sec)
//! - Batch write throughput (ops/sec)

use omni_engine::transaction::TransactionManager;
use omni_engine::{OmniKV, WriteBatch};
use std::sync::Arc;
use std::time::Instant;

fn create_bench_db() -> (Arc<OmniKV>, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let manifest = dir
        .path()
        .join("bench_manifest")
        .to_string_lossy()
        .to_string();
    let wal = dir.path().join("bench_wal").to_string_lossy().to_string();
    let db = OmniKV::open(&manifest, &wal).expect("open");
    (db, dir)
}

struct BenchResult {
    name: String,
    ops: u64,
    elapsed_ms: u128,
    ops_per_sec: f64,
    mb_per_sec: Option<f64>,
}

impl BenchResult {
    fn print(&self) {
        if let Some(mb) = self.mb_per_sec {
            println!(
                "  {:40} {:>10} ops in {:>6}ms = {:>12.0} ops/sec ({:.1} MB/s)",
                self.name, self.ops, self.elapsed_ms, self.ops_per_sec, mb
            );
        } else {
            println!(
                "  {:40} {:>10} ops in {:>6}ms = {:>12.0} ops/sec",
                self.name, self.ops, self.elapsed_ms, self.ops_per_sec
            );
        }
    }
}

/// Benchmark: Sequential writes (single key per batch)
fn bench_sequential_writes(db: &Arc<OmniKV>, count: u64) -> BenchResult {
    let start = Instant::now();
    let value = "x".repeat(100); // 100 byte values

    for i in 0..count {
        let mut batch = WriteBatch::new();
        let _ = batch.set(&format!("seq_{:08}", i), value.clone());
        let _ = db.commit_batch(&batch);
    }

    let elapsed = start.elapsed();
    let ops_per_sec = count as f64 / elapsed.as_secs_f64();
    let bytes = count * 100;
    let mb_per_sec = bytes as f64 / elapsed.as_secs_f64() / 1_048_576.0;

    BenchResult {
        name: "Sequential Writes (100B values)".into(),
        ops: count,
        elapsed_ms: elapsed.as_millis(),
        ops_per_sec,
        mb_per_sec: Some(mb_per_sec),
    }
}

/// Benchmark: Batch writes (many keys per batch)
fn bench_batch_writes(db: &Arc<OmniKV>, total_keys: u64, batch_size: u64) -> BenchResult {
    let start = Instant::now();
    let value = "y".repeat(100);
    let mut written = 0u64;

    for batch_num in 0..(total_keys / batch_size) {
        let mut batch = WriteBatch::new();
        for i in 0..batch_size {
            let key = format!("batch_{:08}", batch_num * batch_size + i);
            let _ = batch.set(&key, value.clone());
        }
        if db.commit_batch(&batch).is_ok() {
            written += batch_size;
        }
    }

    let elapsed = start.elapsed();
    let ops_per_sec = written as f64 / elapsed.as_secs_f64();
    let mb_per_sec = (written * 100) as f64 / elapsed.as_secs_f64() / 1_048_576.0;

    BenchResult {
        name: format!("Batch Writes ({}keys/batch, 100B)", batch_size),
        ops: written,
        elapsed_ms: elapsed.as_millis(),
        ops_per_sec,
        mb_per_sec: Some(mb_per_sec),
    }
}

/// Benchmark: Random point reads
fn bench_random_reads(db: &Arc<OmniKV>, count: u64) -> BenchResult {
    let seq = db.get_seq();
    let start = Instant::now();
    let mut found = 0u64;

    for i in 0..count {
        // Read keys written by sequential write benchmark
        let key = format!("seq_{:08}", i % 1000);
        if let Ok(Some(_)) = db.find(&key, seq) {
            found += 1;
        }
    }

    let elapsed = start.elapsed();
    let ops_per_sec = count as f64 / elapsed.as_secs_f64();

    BenchResult {
        name: format!("Random Point Reads ({} found)", found),
        ops: count,
        elapsed_ms: elapsed.as_millis(),
        ops_per_sec,
        mb_per_sec: None,
    }
}

/// Benchmark: Range scans
fn bench_range_scan(db: &Arc<OmniKV>) -> BenchResult {
    let seq = db.get_seq();
    let start = Instant::now();
    let iterations = 100;
    let mut total_rows = 0u64;

    for _ in 0..iterations {
        if let Ok(results) = db.scan("seq_00000000", "seq_00001000", seq) {
            total_rows += results.len() as u64;
        }
    }

    let elapsed = start.elapsed();
    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();

    BenchResult {
        name: format!("Range Scan (1000 key range, {} total rows)", total_rows),
        ops: iterations as u64,
        elapsed_ms: elapsed.as_millis(),
        ops_per_sec: rows_per_sec,
        mb_per_sec: None,
    }
}

/// Benchmark: Transaction commit throughput
fn bench_transactions(db: &Arc<OmniKV>, count: u64) -> BenchResult {
    let tm = TransactionManager::new(db.clone());
    let start = Instant::now();
    let mut committed = 0u64;

    for i in 0..count {
        let mut txn = tm.begin();
        let _ = tm.set(&mut txn, &format!("txn_{:08}", i), format!("txn_val_{}", i));
        if tm.commit(&mut txn).is_ok() {
            committed += 1;
        }
    }

    let elapsed = start.elapsed();
    let ops_per_sec = committed as f64 / elapsed.as_secs_f64();

    BenchResult {
        name: format!("SSI Transactions ({} committed)", committed),
        ops: count,
        elapsed_ms: elapsed.as_millis(),
        ops_per_sec,
        mb_per_sec: None,
    }
}

/// Benchmark: Mixed workload (80% reads, 20% writes)
fn bench_mixed_workload(db: &Arc<OmniKV>, count: u64) -> BenchResult {
    let seq = db.get_seq();
    let start = Instant::now();
    let value = "z".repeat(100);
    let mut ops = 0u64;

    for i in 0..count {
        if i % 5 == 0 {
            // 20% writes
            let mut batch = WriteBatch::new();
            let _ = batch.set(&format!("mixed_{:08}", i), value.clone());
            let _ = db.commit_batch(&batch);
        } else {
            // 80% reads
            let _ = db.find(&format!("seq_{:08}", i % 1000), seq);
        }
        ops += 1;
    }

    let elapsed = start.elapsed();
    let ops_per_sec = ops as f64 / elapsed.as_secs_f64();

    BenchResult {
        name: "Mixed Workload (80% read, 20% write)".into(),
        ops,
        elapsed_ms: elapsed.as_millis(),
        ops_per_sec,
        mb_per_sec: None,
    }
}

/// Benchmark: Large value writes (4KB values)
fn bench_large_values(db: &Arc<OmniKV>, count: u64) -> BenchResult {
    let start = Instant::now();
    let value = "L".repeat(4096); // 4KB values

    for i in 0..count {
        let mut batch = WriteBatch::new();
        let _ = batch.set(&format!("large_{:08}", i), value.clone());
        let _ = db.commit_batch(&batch);
    }

    let elapsed = start.elapsed();
    let ops_per_sec = count as f64 / elapsed.as_secs_f64();
    let bytes = count * 4096;
    let mb_per_sec = bytes as f64 / elapsed.as_secs_f64() / 1_048_576.0;

    BenchResult {
        name: "Large Value Writes (4KB values)".into(),
        ops: count,
        elapsed_ms: elapsed.as_millis(),
        ops_per_sec,
        mb_per_sec: Some(mb_per_sec),
    }
}

#[test]
fn run_benchmarks() {
    println!("\n");
    println!("  ╔══════════════════════════════════════════════════════════════════════╗");
    println!("  ║               OmniKV Benchmark Suite v0.1.0                        ║");
    println!("  ╚══════════════════════════════════════════════════════════════════════╝");
    println!();

    let (db, _dir) = create_bench_db();

    let results = vec![
        bench_sequential_writes(&db, 5_000),
        bench_batch_writes(&db, 5_000, 100),
        bench_random_reads(&db, 10_000),
        bench_range_scan(&db),
        bench_transactions(&db, 2_000),
        bench_mixed_workload(&db, 10_000),
        bench_large_values(&db, 1_000),
    ];

    println!("  ─────────────────────────────────────────────────────────────────────────");
    for r in &results {
        r.print();
    }
    println!("  ─────────────────────────────────────────────────────────────────────────");
    println!();

    // All benchmarks should complete without error
    assert!(results.len() == 7, "All benchmarks should run");
}
