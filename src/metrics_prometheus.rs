//! Prometheus Metrics Exporter
//!
//! Exposes OmniKV internal metrics in Prometheus text format.

use lazy_static::lazy_static;
use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, TextEncoder, register_histogram,
    register_int_counter, register_int_gauge,
};

lazy_static! {
    pub static ref WRITES_TOTAL: IntCounter =
        register_int_counter!("omnikv_writes_total", "Total number of write operations").unwrap();
    pub static ref READS_TOTAL: IntCounter =
        register_int_counter!("omnikv_reads_total", "Total number of read operations").unwrap();
    pub static ref ACTIVE_TRANSACTIONS: IntGauge = register_int_gauge!(
        "omnikv_active_transactions",
        "Number of currently active transactions"
    )
    .unwrap();
    pub static ref COMPACTIONS_TOTAL: IntCounter = register_int_counter!(
        "omnikv_compactions_total",
        "Total number of compaction runs"
    )
    .unwrap();
    pub static ref WRITE_LATENCY: Histogram = register_histogram!(HistogramOpts::new(
        "omnikv_write_latency_seconds",
        "Write operation latency"
    ))
    .unwrap();
    pub static ref READ_LATENCY: Histogram = register_histogram!(HistogramOpts::new(
        "omnikv_read_latency_seconds",
        "Read operation latency"
    ))
    .unwrap();
    pub static ref MEMTABLE_SIZE: IntGauge = register_int_gauge!(
        "omnikv_memtable_size_bytes",
        "Current memtable size in bytes"
    )
    .unwrap();
    pub static ref SSTABLE_COUNT: IntGauge =
        register_int_gauge!("omnikv_sstable_count", "Total number of SSTables (L0 + L1)").unwrap();
    pub static ref DB_SEQUENCE: IntGauge = register_int_gauge!(
        "omnikv_db_sequence",
        "Current database write sequence number"
    )
    .unwrap();
    pub static ref UPTIME_SECONDS: IntGauge =
        register_int_gauge!("omnikv_uptime_seconds", "Server uptime in seconds").unwrap();
    pub static ref COMMIT_RATE: IntCounter =
        register_int_counter!("omnikv_commits_total", "Total number of committed batches").unwrap();
}

/// Render all metrics in Prometheus text format.
pub fn render_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder
        .encode(&metric_families, &mut buffer)
        .unwrap_or_default();
    String::from_utf8(buffer).unwrap_or_default()
}

/// Update DB-level gauges. Call this periodically or per-request.
pub fn record_db_stats(seq: u64, sstable_count: usize, uptime_secs: u64) {
    DB_SEQUENCE.set(seq as i64);
    SSTABLE_COUNT.set(sstable_count as i64);
    UPTIME_SECONDS.set(uptime_secs as i64);
}
