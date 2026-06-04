//! OmniKV Benchmark Suite
//!
//! Produces measured, reproducible benchmark results.
//! Outputs: ops/sec, latency percentiles (p50/p95/p99), thread scaling.
//!
//! Usage:
//!   cargo run --bin omni_bench --release
//!   cargo run --bin omni_bench --release -- --soak 600   # 10-min soak

use omni_engine::{OmniKV, WriteBatch};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let soak_secs = args
        .iter()
        .position(|a| a == "--soak")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse::<u64>().ok());

    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║          OmniKV Benchmark Suite v{}               ║", env!("CARGO_PKG_VERSION"));
    println!("╚══════════════════════════════════════════════════════════╝\n");

    let dir = tempfile::tempdir().expect("tmpdir");
    let manifest = dir.path().join("manifest.json");
    let wal = dir.path().join("wal.bin");
    let db = OmniKV::open(manifest.to_str().unwrap(), wal.to_str().unwrap())
        .expect("Failed to open DB");

    println!("── Single-Thread Benchmarks ─────────────────────────────\n");

    bench_sequential_writes(&db, 100_000);
    bench_batch_writes(&db, 1_000, 100);
    bench_sequential_reads(&db, 100_000);
    bench_random_reads(&db, 50_000, 100_000);
    bench_point_read_miss(&db, 50_000);
    bench_scan(&db, 10_000);
    bench_mixed_workload(&db, 50_000);
    bench_transaction_overhead(&db, 10_000);

    println!("\n── Thread Scaling (writes) ──────────────────────────────\n");

    // Thread scaling needs a fresh DB each time to avoid write stalls
    for threads in &[1, 2, 4, 8] {
        let tdir = tempfile::tempdir().expect("tmpdir");
        let tm = tdir.path().join("manifest.json");
        let tw = tdir.path().join("wal.bin");
        let tdb = OmniKV::open(tm.to_str().unwrap(), tw.to_str().unwrap()).expect("open");
        bench_threaded_writes(&tdb, *threads, 10_000);
    }

    println!("\n── Thread Scaling (reads) ───────────────────────────────\n");

    // Pre-populate for read scaling
    let read_dir = tempfile::tempdir().expect("tmpdir");
    let rm = read_dir.path().join("manifest.json");
    let rw = read_dir.path().join("wal.bin");
    let read_db = OmniKV::open(rm.to_str().unwrap(), rw.to_str().unwrap()).expect("open");
    for i in 0..50_000u64 {
        let mut b = WriteBatch::new();
        b.set(&format!("rscale:{:08}", i), format!("v{}", i)).unwrap();
        read_db.commit_batch(&b).unwrap();
    }

    for threads in &[1, 2, 4, 8] {
        bench_threaded_reads(&read_db, *threads, 50_000);
    }

    if let Some(secs) = soak_secs {
        println!("\n── Soak Test ({} seconds) ───────────────────────────────\n", secs);
        let soak_dir = tempfile::tempdir().expect("tmpdir");
        let sm = soak_dir.path().join("manifest.json");
        let sw = soak_dir.path().join("wal.bin");
        let soak_db = OmniKV::open(sm.to_str().unwrap(), sw.to_str().unwrap()).expect("open");
        run_soak_test(&soak_db, Duration::from_secs(secs));
    }

    println!("\n✅ All benchmarks complete.");
}

// ═══════════════════════════════════════════════════════════════════════
// Latency tracking
// ═══════════════════════════════════════════════════════════════════════

struct LatencyTracker {
    samples: Vec<u64>, // nanoseconds
}

impl LatencyTracker {
    fn new() -> Self {
        Self { samples: Vec::with_capacity(100_000) }
    }

    fn record(&mut self, duration: Duration) {
        self.samples.push(duration.as_nanos() as u64);
    }

    fn percentile(&mut self, pct: f64) -> Duration {
        if self.samples.is_empty() {
            return Duration::ZERO;
        }
        self.samples.sort_unstable();
        let idx = ((pct / 100.0) * self.samples.len() as f64) as usize;
        let idx = idx.min(self.samples.len() - 1);
        Duration::from_nanos(self.samples[idx])
    }

    fn report(&mut self, label: &str, count: u64, total_elapsed: Duration) {
        let ops = count as f64 / total_elapsed.as_secs_f64();
        let p50 = self.percentile(50.0);
        let p95 = self.percentile(95.0);
        let p99 = self.percentile(99.0);
        println!(
            "{:<22} {:>8} ops  {:>7.2}s  {:>10.0} ops/sec  p50={:>6.1}µs  p95={:>7.1}µs  p99={:>7.1}µs",
            label,
            count,
            total_elapsed.as_secs_f64(),
            ops,
            p50.as_nanos() as f64 / 1000.0,
            p95.as_nanos() as f64 / 1000.0,
            p99.as_nanos() as f64 / 1000.0,
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Individual benchmarks
// ═══════════════════════════════════════════════════════════════════════

fn bench_sequential_writes(db: &Arc<OmniKV>, count: u64) {
    let mut lat = LatencyTracker::new();
    let total_start = Instant::now();
    for i in 0..count {
        let start = Instant::now();
        let mut batch = WriteBatch::new();
        batch.set(&format!("seq_w:{:08}", i), format!("value_{}", i)).unwrap();
        db.commit_batch(&batch).unwrap();
        lat.record(start.elapsed());
    }
    lat.report("Sequential Writes", count, total_start.elapsed());
}

fn bench_batch_writes(db: &Arc<OmniKV>, batches: u64, per_batch: u64) {
    let mut lat = LatencyTracker::new();
    let total_start = Instant::now();
    for i in 0..batches {
        let start = Instant::now();
        let mut batch = WriteBatch::new();
        for j in 0..per_batch {
            batch.set(
                &format!("batch:{}:{:06}", i, j),
                format!("payload_{}", j),
            ).unwrap();
        }
        db.commit_batch(&batch).unwrap();
        lat.record(start.elapsed());
    }
    let total = batches * per_batch;
    let elapsed = total_start.elapsed();
    let ops = total as f64 / elapsed.as_secs_f64();
    let p50 = lat.percentile(50.0);
    let p95 = lat.percentile(95.0);
    let p99 = lat.percentile(99.0);
    println!(
        "{:<22} {:>8} ops  {:>7.2}s  {:>10.0} ops/sec  p50={:>6.1}µs  p95={:>7.1}µs  p99={:>7.1}µs  ({}×{})",
        "Batch Writes", total, elapsed.as_secs_f64(), ops,
        p50.as_nanos() as f64 / 1000.0,
        p95.as_nanos() as f64 / 1000.0,
        p99.as_nanos() as f64 / 1000.0,
        batches, per_batch,
    );
}

fn bench_sequential_reads(db: &Arc<OmniKV>, count: u64) {
    let seq = db.get_seq();
    let mut lat = LatencyTracker::new();
    let mut found = 0u64;
    let total_start = Instant::now();
    for i in 0..count {
        let start = Instant::now();
        if let Ok(Some(_)) = db.find(&format!("seq_w:{:08}", i), seq) {
            found += 1;
        }
        lat.record(start.elapsed());
    }
    let elapsed = total_start.elapsed();
    let ops = count as f64 / elapsed.as_secs_f64();
    let p50 = lat.percentile(50.0);
    let p95 = lat.percentile(95.0);
    let p99 = lat.percentile(99.0);
    println!(
        "{:<22} {:>8} ops  {:>7.2}s  {:>10.0} ops/sec  p50={:>6.1}µs  p95={:>7.1}µs  p99={:>7.1}µs  (hit: {})",
        "Sequential Reads", count, elapsed.as_secs_f64(), ops,
        p50.as_nanos() as f64 / 1000.0,
        p95.as_nanos() as f64 / 1000.0,
        p99.as_nanos() as f64 / 1000.0,
        found,
    );
}

fn bench_random_reads(db: &Arc<OmniKV>, count: u64, keyspace: u64) {
    let seq = db.get_seq();
    let mut lat = LatencyTracker::new();
    let mut found = 0u64;
    let total_start = Instant::now();
    for i in 0..count {
        let key_idx = (i.wrapping_mul(7919)) % keyspace;
        let start = Instant::now();
        if let Ok(Some(_)) = db.find(&format!("seq_w:{:08}", key_idx), seq) {
            found += 1;
        }
        lat.record(start.elapsed());
    }
    let elapsed = total_start.elapsed();
    let ops = count as f64 / elapsed.as_secs_f64();
    let p50 = lat.percentile(50.0);
    let p95 = lat.percentile(95.0);
    let p99 = lat.percentile(99.0);
    println!(
        "{:<22} {:>8} ops  {:>7.2}s  {:>10.0} ops/sec  p50={:>6.1}µs  p95={:>7.1}µs  p99={:>7.1}µs  (hit: {})",
        "Random Reads", count, elapsed.as_secs_f64(), ops,
        p50.as_nanos() as f64 / 1000.0,
        p95.as_nanos() as f64 / 1000.0,
        p99.as_nanos() as f64 / 1000.0,
        found,
    );
}

fn bench_point_read_miss(db: &Arc<OmniKV>, count: u64) {
    let seq = db.get_seq();
    let mut lat = LatencyTracker::new();
    let total_start = Instant::now();
    for i in 0..count {
        let start = Instant::now();
        let _ = db.find(&format!("NONEXIST:{:08}", i), seq);
        lat.record(start.elapsed());
    }
    lat.report("Point Read (miss)", count, total_start.elapsed());
}

fn bench_scan(db: &Arc<OmniKV>, range_size: u64) {
    let seq = db.get_seq();
    let start = Instant::now();
    let results = db.scan("seq_w:00000000", &format!("seq_w:{:08}", range_size), seq)
        .unwrap_or_default();
    let elapsed = start.elapsed();
    println!(
        "{:<22} {:>8} rows {:>7.2}s  {:>10.0} rows/sec",
        "Range Scan",
        results.len(),
        elapsed.as_secs_f64(),
        results.len() as f64 / elapsed.as_secs_f64(),
    );
}

fn bench_mixed_workload(db: &Arc<OmniKV>, ops: u64) {
    let mut lat = LatencyTracker::new();
    let total_start = Instant::now();
    for i in 0..ops {
        let start = Instant::now();
        if i % 5 == 0 {
            let mut batch = WriteBatch::new();
            batch.set(&format!("mixed:{:08}", i), format!("v{}", i)).unwrap();
            db.commit_batch(&batch).unwrap();
        } else {
            let seq = db.get_seq();
            let _ = db.find(&format!("seq_w:{:08}", i % 100_000), seq);
        }
        lat.record(start.elapsed());
    }
    lat.report("Mixed (80R/20W)", ops, total_start.elapsed());
}

fn bench_transaction_overhead(db: &Arc<OmniKV>, count: u64) {
    use omni_engine::transaction::TransactionManager;
    let tm = TransactionManager::new(db.clone());
    let mut lat = LatencyTracker::new();
    let total_start = Instant::now();
    for i in 0..count {
        let start = Instant::now();
        let mut txn = tm.begin();
        tm.set(&mut txn, &format!("txn:{:06}", i), format!("v{}", i)).unwrap();
        tm.commit(&mut txn).unwrap();
        lat.record(start.elapsed());
    }
    lat.report("SSI Transactions", count, total_start.elapsed());
}

// ═══════════════════════════════════════════════════════════════════════
// Thread scaling benchmarks
// ═══════════════════════════════════════════════════════════════════════

fn bench_threaded_writes(db: &Arc<OmniKV>, num_threads: usize, ops_per_thread: u64) {
    let total_ops = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let handles: Vec<_> = (0..num_threads)
        .map(|tid| {
            let db = db.clone();
            let total = total_ops.clone();
            std::thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let mut batch = WriteBatch::new();
                    batch.set(
                        &format!("tw:{}:{:08}", tid, i),
                        format!("v{}", i),
                    ).unwrap();
                    if db.commit_batch(&batch).is_ok() {
                        total.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let completed = total_ops.load(Ordering::Relaxed);
    let ops = completed as f64 / elapsed.as_secs_f64();
    println!(
        "  {:>2} threads × {:>6} ops = {:>8} total  {:>7.2}s  {:>10.0} ops/sec",
        num_threads, ops_per_thread, completed, elapsed.as_secs_f64(), ops,
    );
}

fn bench_threaded_reads(db: &Arc<OmniKV>, num_threads: usize, total_keys: u64) {
    let total_ops = Arc::new(AtomicU64::new(0));
    let ops_per_thread = total_keys / num_threads as u64;
    let start = Instant::now();

    let handles: Vec<_> = (0..num_threads)
        .map(|tid| {
            let db = db.clone();
            let total = total_ops.clone();
            std::thread::spawn(move || {
                let seq = db.get_seq();
                let offset = tid as u64 * ops_per_thread;
                for i in 0..ops_per_thread {
                    let key = format!("rscale:{:08}", (offset + i) % total_keys);
                    let _ = db.find(&key, seq);
                    total.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let completed = total_ops.load(Ordering::Relaxed);
    let ops = completed as f64 / elapsed.as_secs_f64();
    println!(
        "  {:>2} threads × {:>6} ops = {:>8} total  {:>7.2}s  {:>10.0} ops/sec",
        num_threads, ops_per_thread, completed, elapsed.as_secs_f64(), ops,
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Soak test
// ═══════════════════════════════════════════════════════════════════════

fn run_soak_test(db: &Arc<OmniKV>, duration: Duration) {
    let stop = Arc::new(AtomicBool::new(false));
    let total_writes = Arc::new(AtomicU64::new(0));
    let total_reads = Arc::new(AtomicU64::new(0));
    let total_errors = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    // Spawn 4 writer threads
    let writer_handles: Vec<_> = (0..4)
        .map(|tid| {
            let db = db.clone();
            let stop = stop.clone();
            let writes = total_writes.clone();
            let errors = total_errors.clone();
            std::thread::spawn(move || {
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let mut batch = WriteBatch::new();
                    batch.set(
                        &format!("soak:{}:{:010}", tid, i),
                        format!("v_{}", i),
                    ).unwrap();
                    match db.commit_batch(&batch) {
                        Ok(_) => { writes.fetch_add(1, Ordering::Relaxed); }
                        Err(_) => { errors.fetch_add(1, Ordering::Relaxed); }
                    }
                    i += 1;
                }
            })
        })
        .collect();

    // Spawn 4 reader threads
    let reader_handles: Vec<_> = (0..4)
        .map(|_tid| {
            let db = db.clone();
            let stop = stop.clone();
            let reads = total_reads.clone();
            std::thread::spawn(move || {
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let seq = db.get_seq();
                    let _ = db.find(&format!("soak:0:{:010}", i % 1_000_000), seq);
                    reads.fetch_add(1, Ordering::Relaxed);
                    i += 1;
                }
            })
        })
        .collect();

    // Spawn compaction thread
    let compact_handle = {
        let db = db.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut compactions = 0u64;
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_secs(5));
                if db.sstable_count() >= 4 {
                    let _ = db.compact_sstables();
                    compactions += 1;
                }
                if db.l1_sstable_count() >= 4 {
                    let _ = db.compact_l0_to_l1();
                    compactions += 1;
                }
            }
            compactions
        })
    };

    // Progress reporter
    let progress_stop = stop.clone();
    let pw = total_writes.clone();
    let pr = total_reads.clone();
    let pe = total_errors.clone();
    let progress_handle = std::thread::spawn(move || {
        let mut last_w = 0u64;
        let mut last_r = 0u64;
        let mut interval = 0u64;
        while !progress_stop.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_secs(10));
            interval += 10;
            let w = pw.load(Ordering::Relaxed);
            let r = pr.load(Ordering::Relaxed);
            let e = pe.load(Ordering::Relaxed);
            let w_rate = (w - last_w) as f64 / 10.0;
            let r_rate = (r - last_r) as f64 / 10.0;
            println!(
                "  [{:>4}s] writes: {:>8} ({:>8.0}/s)  reads: {:>8} ({:>8.0}/s)  errors: {}",
                interval, w, w_rate, r, r_rate, e
            );
            last_w = w;
            last_r = r;
        }
    });

    // Wait for soak duration
    std::thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);

    for h in writer_handles {
        h.join().unwrap();
    }
    for h in reader_handles {
        h.join().unwrap();
    }
    let compactions = compact_handle.join().unwrap();
    let _ = progress_handle.join();

    let elapsed = start.elapsed();
    let w = total_writes.load(Ordering::Relaxed);
    let r = total_reads.load(Ordering::Relaxed);
    let e = total_errors.load(Ordering::Relaxed);

    println!("\n  ── Soak Results ──");
    println!("  Duration:     {:>10.1}s", elapsed.as_secs_f64());
    println!("  Writes:       {:>10} ({:.0}/s)", w, w as f64 / elapsed.as_secs_f64());
    println!("  Reads:        {:>10} ({:.0}/s)", r, r as f64 / elapsed.as_secs_f64());
    println!("  Compactions:  {:>10}", compactions);
    println!("  Errors:       {:>10}", e);
    println!("  Verdict:      {}", if e == 0 { "✅ PASS — zero errors" } else { "❌ FAIL — errors detected" });
}
