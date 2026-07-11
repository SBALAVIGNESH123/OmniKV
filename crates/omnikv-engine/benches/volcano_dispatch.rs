//! Volcano executor dispatch benchmark.
//!
//! Compares the original row-at-a-time dynamic-dispatch consumption path with
//! the chunked consumption path used by built-in streaming operators.
//!
//! Usage:
//!   cargo bench -p omnikv-engine --bench volcano_dispatch
//!   cargo bench -p omnikv-engine --bench volcano_dispatch -- --rows 200000 --rounds 5

#![expect(
    clippy::cast_precision_loss,
    clippy::doc_markdown,
    clippy::missing_const_for_fn,
    reason = "Dispatch benchmark favors readable throughput math and CLI documentation over style-only rewrites."
)]

use omni_engine::sql::{AggFunc, CmpOp, SelectColumn, SqlValue, WhereExpr};
use omni_engine::sql_exec::Row;
use omni_engine::volcano::{
    AggregateIter, DEFAULT_ROW_CHUNK_SIZE, FilterIter, LimitIter, ProjectIter, RowIterator,
};
use std::time::{Duration, Instant};

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

#[derive(Default)]
struct ResultStats {
    rows: usize,
    checksum: u64,
    elapsed: Duration,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let rows = arg_value(&args, "--rows").unwrap_or(100_000);
    let rounds = arg_value(&args, "--rounds").unwrap_or(3);
    let data = synthetic_rows(rows);

    println!("Volcano dispatch benchmark");
    println!("Rows: {rows}");
    println!("Rounds: {rounds}");
    println!("Chunk size: {DEFAULT_ROW_CHUNK_SIZE}");
    println!();
    println!(
        "{:<38} {:>10} {:>12} {:>12} {:>10}",
        "pipeline", "rows", "row/s", "chunk/s", "ratio"
    );

    run_case("scan only", &data, rounds, scan_pipeline);
    run_case("scan + filter", &data, rounds, filter_pipeline);
    run_case("scan + projection", &data, rounds, project_pipeline);
    run_case(
        "scan + filter + projection + limit",
        &data,
        rounds,
        filter_project_limit_pipeline,
    );
    run_case("scan + aggregate", &data, rounds, aggregate_pipeline);
}

fn arg_value(args: &[String], name: &str) -> Option<usize> {
    args.iter()
        .position(|arg| arg == name)
        .and_then(|idx| args.get(idx + 1))
        .and_then(|value| value.parse::<usize>().ok())
}

fn run_case(
    name: &str,
    rows: &[Row],
    rounds: usize,
    make_pipeline: fn(&[Row]) -> Box<dyn RowIterator>,
) {
    let mut row_total = ResultStats::default();
    let mut chunk_total = ResultStats::default();

    for _ in 0..rounds {
        let row = consume_row_by_row(make_pipeline(rows));
        let chunk = consume_chunked(make_pipeline(rows));
        assert_eq!(row.rows, chunk.rows, "{name}: row count mismatch");
        assert_eq!(row.checksum, chunk.checksum, "{name}: checksum mismatch");

        row_total.rows = row.rows;
        row_total.checksum = row.checksum;
        row_total.elapsed += row.elapsed;

        chunk_total.rows = chunk.rows;
        chunk_total.checksum = chunk.checksum;
        chunk_total.elapsed += chunk.elapsed;
    }

    let row_per_sec = (row_total.rows * rounds) as f64 / row_total.elapsed.as_secs_f64();
    let chunk_per_sec = (chunk_total.rows * rounds) as f64 / chunk_total.elapsed.as_secs_f64();
    let ratio = chunk_per_sec / row_per_sec;
    println!(
        "{:<38} {:>10} {:>12.0} {:>12.0} {:>9.2}x",
        name, chunk_total.rows, row_per_sec, chunk_per_sec, ratio
    );
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
        rows.len() / 4,
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

fn consume_row_by_row(mut iter: Box<dyn RowIterator>) -> ResultStats {
    let start = Instant::now();
    let mut rows = 0usize;
    let mut checksum = 0u64;
    while let Some(row) = iter.next_row() {
        checksum = checksum.wrapping_add(row_checksum(&row));
        rows += 1;
    }
    std::hint::black_box(checksum);
    ResultStats {
        rows,
        checksum,
        elapsed: start.elapsed(),
    }
}

fn consume_chunked(mut iter: Box<dyn RowIterator>) -> ResultStats {
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
    std::hint::black_box(checksum);
    ResultStats {
        rows,
        checksum,
        elapsed: start.elapsed(),
    }
}

fn row_checksum(row: &Row) -> u64 {
    row.values().map(|value| value.len() as u64).sum()
}
