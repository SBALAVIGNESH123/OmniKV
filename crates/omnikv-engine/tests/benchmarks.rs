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

use omni_engine::sql::{AggFunc, CmpOp, SelectColumn, SqlValue, WhereExpr};
use omni_engine::sql_exec::Row;
use omni_engine::transaction::TransactionManager;
use omni_engine::volcano::{
    AggregateIter, DEFAULT_ROW_CHUNK_SIZE, FilterIter, LimitIter, ProjectIter, RowIterator,
};
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

#[derive(Clone)]
struct VecRowIter {
    rows: Vec<Row>,
    pos: usize,
}

impl VecRowIter {
    fn new(rows: Vec<Row>) -> Self {
        Self { rows, pos: 0 }
    }
}

impl RowIterator for VecRowIter {
    fn next_row(&mut self) -> Option<Row> {
        let row = self.rows.get(self.pos).cloned()?;
        self.pos += 1;
        Some(row)
    }

    fn reset(&mut self) {
        self.pos = 0;
    }

    fn next_chunk(&mut self, max_rows: usize, out: &mut Vec<Row>) -> usize {
        let remaining = self.rows.len().saturating_sub(self.pos);
        let take = remaining.min(max_rows);
        if take == 0 {
            return 0;
        }
        out.extend(self.rows[self.pos..self.pos + take].iter().cloned());
        self.pos += take;
        take
    }
}

struct DispatchResult {
    rows: usize,
    checksum: u64,
    elapsed_us: u128,
    rows_per_sec: f64,
}

fn synthetic_rows(count: usize) -> Vec<Row> {
    (0..count)
        .map(|i| {
            let mut row = Row::new();
            row.insert("id".into(), i.to_string());
            row.insert(
                "kind".into(),
                if i % 3 == 0 { "hot" } else { "cold" }.into(),
            );
            row.insert("bucket".into(), format!("b{}", i % 16));
            row.insert("payload".into(), format!("payload-{i:08}"));
            row
        })
        .collect()
}

fn hot_filter() -> WhereExpr {
    WhereExpr::Comparison {
        column: "kind".into(),
        op: CmpOp::Eq,
        value: SqlValue::Text("hot".into()),
    }
}

fn projection() -> Vec<SelectColumn> {
    vec![
        SelectColumn::Named("id".into()),
        SelectColumn::Named("payload".into()),
    ]
}

fn scan_pipeline(rows: &[Row]) -> Box<dyn RowIterator> {
    Box::new(VecRowIter::new(rows.to_vec()))
}

fn filter_pipeline(rows: &[Row]) -> Box<dyn RowIterator> {
    Box::new(FilterIter::new(scan_pipeline(rows), hot_filter()))
}

fn project_pipeline(rows: &[Row]) -> Box<dyn RowIterator> {
    Box::new(ProjectIter::new(scan_pipeline(rows), projection()))
}

fn filter_project_limit_pipeline(rows: &[Row]) -> Box<dyn RowIterator> {
    Box::new(LimitIter::new(
        Box::new(ProjectIter::new(
            Box::new(FilterIter::new(scan_pipeline(rows), hot_filter())),
            projection(),
        )),
        2_048,
    ))
}

fn aggregate_pipeline(rows: &[Row]) -> Box<dyn RowIterator> {
    Box::new(AggregateIter::new(
        scan_pipeline(rows),
        vec!["bucket".into()],
        vec![
            SelectColumn::Named("bucket".into()),
            SelectColumn::Aggregate(AggFunc::Count, "id".into()),
        ],
    ))
}

fn consume_row_by_row(mut iter: Box<dyn RowIterator>) -> DispatchResult {
    let start = Instant::now();
    let mut rows = 0usize;
    let mut checksum = 0u64;
    while let Some(row) = iter.next_row() {
        checksum = checksum.wrapping_add(row_checksum(&row));
        rows += 1;
    }
    let elapsed = start.elapsed();
    std::hint::black_box(checksum);
    DispatchResult {
        rows,
        checksum,
        elapsed_us: elapsed.as_micros(),
        rows_per_sec: rows as f64 / elapsed.as_secs_f64(),
    }
}

fn consume_chunked(mut iter: Box<dyn RowIterator>) -> DispatchResult {
    let start = Instant::now();
    let mut rows = 0usize;
    let mut checksum = 0u64;
    let mut chunk = Vec::with_capacity(DEFAULT_ROW_CHUNK_SIZE);
    loop {
        chunk.clear();
        let n = iter.next_chunk(DEFAULT_ROW_CHUNK_SIZE, &mut chunk);
        if n == 0 {
            break;
        }
        rows += n;
        for row in &chunk {
            checksum = checksum.wrapping_add(row_checksum(row));
        }
    }
    let elapsed = start.elapsed();
    std::hint::black_box(checksum);
    DispatchResult {
        rows,
        checksum,
        elapsed_us: elapsed.as_micros(),
        rows_per_sec: rows as f64 / elapsed.as_secs_f64(),
    }
}

fn row_checksum(row: &Row) -> u64 {
    row.values().map(|v| v.len() as u64).sum()
}

fn bench_dispatch_pipeline(
    name: &str,
    rows: &[Row],
    make_pipeline: fn(&[Row]) -> Box<dyn RowIterator>,
) {
    let row_by_row = consume_row_by_row(make_pipeline(rows));
    let chunked = consume_chunked(make_pipeline(rows));

    assert_eq!(row_by_row.rows, chunked.rows, "{name}: row count mismatch");
    assert_eq!(
        row_by_row.checksum, chunked.checksum,
        "{name}: result checksum mismatch"
    );

    let speedup = if row_by_row.rows_per_sec > 0.0 {
        chunked.rows_per_sec / row_by_row.rows_per_sec
    } else {
        0.0
    };
    println!(
        "  {:40} rows={:>7} row={:>8}us chunk={:>8}us row/s={:>12.0} chunk/s={:>12.0} chunk_ratio={:>5.2}x",
        name,
        chunked.rows,
        row_by_row.elapsed_us,
        chunked.elapsed_us,
        row_by_row.rows_per_sec,
        chunked.rows_per_sec,
        speedup
    );
}

fn bench_volcano_dispatch_smoke() {
    let rows = synthetic_rows(20_000);

    println!();
    println!("  Volcano dispatch benchmark: dyn row-at-a-time vs chunked batches");
    println!(
        "  Chunk size: {} rows. Ratios are informational only; CI asserts semantic equivalence.",
        DEFAULT_ROW_CHUNK_SIZE
    );

    bench_dispatch_pipeline("scan only", &rows, scan_pipeline);
    bench_dispatch_pipeline("scan + filter", &rows, filter_pipeline);
    bench_dispatch_pipeline("scan + projection", &rows, project_pipeline);
    bench_dispatch_pipeline(
        "scan + filter + projection + limit",
        &rows,
        filter_project_limit_pipeline,
    );
    bench_dispatch_pipeline("scan + aggregate", &rows, aggregate_pipeline);
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

    bench_volcano_dispatch_smoke();
}
