//! Repeated range-scan benchmark for the storage scan buffer pool.
//!
//! This is intentionally a tiny, dependency-light harness instead of a
//! criterion benchmark: CI can compile it quickly, and maintainers can run it
//! before/after scan iterator changes to compare allocation-sensitive paths.
//!
//! Usage:
//!   cargo bench -p omnikv-engine --bench `scan_buffer_pool`
//!   cargo bench -p omnikv-engine --bench `scan_buffer_pool` -- --rows 20000 --rounds 20

use omni_engine::{OmniKV, WriteBatch};
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[expect(
    clippy::cast_precision_loss,
    reason = "Benchmark harness reports approximate rows/sec; f64 precision is sufficient for comparative diagnostics."
)]
fn rows_per_second(rows: usize, elapsed: Duration) -> f64 {
    rows as f64 / elapsed.as_secs_f64()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let rows = arg_value(&args, "--rows").unwrap_or(10_000);
    let rounds = arg_value(&args, "--rounds").unwrap_or(10);

    let (_dir, db) = create_db();
    load_rows(&db, rows);
    db.compact_sstables().expect("compact rows into an SSTable");

    let mut total_rows = 0usize;
    let mut checksum = 0u64;
    let mut elapsed = Duration::default();

    for _ in 0..rounds {
        let start = Instant::now();
        let mut round_rows = 0usize;
        for (key, value) in db
            .scan_iter("bench_scan_000000", "bench_scan_999999", db.get_seq())
            .expect("scan benchmark range")
        {
            round_rows += 1;
            checksum = checksum.wrapping_add(key.len() as u64);
            checksum = checksum.wrapping_add(value.len() as u64);
        }
        assert_eq!(round_rows, rows);
        total_rows += round_rows;
        elapsed += start.elapsed();
    }

    let rows_per_sec = rows_per_second(total_rows, elapsed);
    println!("Scan buffer pool benchmark");
    println!("Rows loaded: {rows}");
    println!("Rounds: {rounds}");
    println!("Rows scanned: {total_rows}");
    println!("Rows/sec: {rows_per_sec:.0}");
    println!("Checksum: {checksum}");
    println!(
        "Available pooled buffers after benchmark: {}",
        db.scan_buffer_pool_available()
    );
}

fn arg_value(args: &[String], name: &str) -> Option<usize> {
    args.iter()
        .position(|arg| arg == name)
        .and_then(|idx| args.get(idx + 1))
        .and_then(|value| value.parse::<usize>().ok())
}

fn create_db() -> (TempDir, std::sync::Arc<OmniKV>) {
    let dir = TempDir::new().expect("tempdir");
    let manifest = dir
        .path()
        .join("bench_manifest")
        .to_string_lossy()
        .to_string();
    let wal = dir.path().join("bench_wal").to_string_lossy().to_string();
    let db = OmniKV::open(&manifest, &wal).expect("open benchmark database");
    (dir, db)
}

fn load_rows(db: &OmniKV, rows: usize) {
    let mut batch = WriteBatch::new();
    for i in 0..rows {
        batch
            .set(
                &format!("bench_scan_{i:06}"),
                format!("value-{i:06}-{}", "x".repeat(64)),
            )
            .expect("add benchmark row");
    }
    db.commit_batch(&batch).expect("commit benchmark rows");
}
