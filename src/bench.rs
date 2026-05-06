//! OmniKV Standalone Benchmark Binary
//!
//! Runs comprehensive benchmarks and outputs results in a format
//! suitable for publishing in README and documentation.

use std::sync::Arc;
use std::time::Instant;
use omni_engine::{OmniKV, WriteBatch};

fn main() {
    println!("╔══════════════════════════════════════════════╗");
    println!("║       OmniKV Benchmark Suite v{}       ║", env!("CARGO_PKG_VERSION"));
    println!("╚══════════════════════════════════════════════╝\n");

    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let manifest = dir.path().join("manifest.json");
    let wal = dir.path().join("wal.bin");
    let db = OmniKV::open(
        manifest.to_str().unwrap(),
        wal.to_str().unwrap(),
    ).expect("Failed to open DB");

    bench_sequential_writes(&db);
    bench_batch_writes(&db);
    bench_sequential_reads(&db);
    bench_random_reads(&db);
    bench_scan(&db);
    bench_mixed_workload(&db);
    bench_transaction_overhead(&db);

    println!("\n✅ All benchmarks complete.");
}

fn bench_sequential_writes(db: &Arc<OmniKV>) {
    let count = 100_000;
    let start = Instant::now();
    for i in 0..count {
        let mut batch = WriteBatch::new();
        batch.set(&format!("bench_seq_{:08}", i), format!("value_{}", i)).unwrap();
        db.commit_batch(&batch).unwrap();
    }
    let elapsed = start.elapsed();
    let ops = count as f64 / elapsed.as_secs_f64();
    println!("Sequential Writes:  {:>10} ops  {:>8.2}s  {:>10.0} ops/sec", count, elapsed.as_secs_f64(), ops);
}

fn bench_batch_writes(db: &Arc<OmniKV>) {
    let batches = 1_000;
    let per_batch = 100;
    let start = Instant::now();
    for i in 0..batches {
        let mut batch = WriteBatch::new();
        for j in 0..per_batch {
            batch.set(
                &format!("bench_batch_{}_{:06}", i, j),
                format!("payload_{}", j),
            ).unwrap();
        }
        db.commit_batch(&batch).unwrap();
    }
    let elapsed = start.elapsed();
    let total = batches * per_batch;
    let ops = total as f64 / elapsed.as_secs_f64();
    println!("Batch Writes:       {:>10} ops  {:>8.2}s  {:>10.0} ops/sec  ({}×{})", total, elapsed.as_secs_f64(), ops, batches, per_batch);
}

fn bench_sequential_reads(db: &Arc<OmniKV>) {
    let count = 100_000;
    let seq = db.get_seq();
    let start = Instant::now();
    let mut found = 0;
    for i in 0..count {
        if let Ok(Some(_)) = db.find(&format!("bench_seq_{:08}", i), seq) {
            found += 1;
        }
    }
    let elapsed = start.elapsed();
    let ops = count as f64 / elapsed.as_secs_f64();
    println!("Sequential Reads:   {:>10} ops  {:>8.2}s  {:>10.0} ops/sec  (found: {})", count, elapsed.as_secs_f64(), ops, found);
}

fn bench_random_reads(db: &Arc<OmniKV>) {
    let count = 50_000;
    let seq = db.get_seq();
    let start = Instant::now();
    let mut found = 0;
    for i in 0..count {
        let key = format!("bench_seq_{:08}", (i * 7919) % 100_000); // pseudo-random
        if let Ok(Some(_)) = db.find(&key, seq) {
            found += 1;
        }
    }
    let elapsed = start.elapsed();
    let ops = count as f64 / elapsed.as_secs_f64();
    println!("Random Reads:       {:>10} ops  {:>8.2}s  {:>10.0} ops/sec  (found: {})", count, elapsed.as_secs_f64(), ops, found);
}

fn bench_scan(db: &Arc<OmniKV>) {
    let seq = db.get_seq();
    let start = Instant::now();
    let results = db.scan("bench_seq_00000000", "bench_seq_00010000", seq).unwrap();
    let elapsed = start.elapsed();
    println!("Range Scan (10K):   {:>10} rows {:>8.2}s  {:>10.0} rows/sec", results.len(), elapsed.as_secs_f64(), results.len() as f64 / elapsed.as_secs_f64());
}

fn bench_mixed_workload(db: &Arc<OmniKV>) {
    let ops = 50_000;
    let start = Instant::now();
    for i in 0..ops {
        if i % 5 == 0 {
            // 20% writes
            let mut batch = WriteBatch::new();
            batch.set(&format!("bench_mixed_{:08}", i), format!("v{}", i)).unwrap();
            db.commit_batch(&batch).unwrap();
        } else {
            // 80% reads
            let seq = db.get_seq();
            let _ = db.find(&format!("bench_seq_{:08}", i % 100_000), seq);
        }
    }
    let elapsed = start.elapsed();
    let rate = ops as f64 / elapsed.as_secs_f64();
    println!("Mixed (80R/20W):    {:>10} ops  {:>8.2}s  {:>10.0} ops/sec", ops, elapsed.as_secs_f64(), rate);
}

fn bench_transaction_overhead(db: &Arc<OmniKV>) {
    use omni_engine::transaction::TransactionManager;
    let tm = TransactionManager::new(db.clone());
    let count = 10_000;
    let start = Instant::now();
    for i in 0..count {
        let mut txn = tm.begin();
        tm.set(&mut txn, &format!("txn_bench_{:06}", i), format!("v{}", i)).unwrap();
        tm.commit(&mut txn).unwrap();
    }
    let elapsed = start.elapsed();
    let rate = count as f64 / elapsed.as_secs_f64();
    println!("SSI Transactions:   {:>10} txns {:>8.2}s  {:>10.0} txns/sec", count, elapsed.as_secs_f64(), rate);
}
