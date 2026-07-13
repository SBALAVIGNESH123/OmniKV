//! Prometheus Metrics Exporter
//!
//! Exposes OmniKV internal metrics in Prometheus text format.

use lazy_static::lazy_static;
use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    TextEncoder, register_histogram, register_histogram_vec, register_int_counter,
    register_int_counter_vec, register_int_gauge,
};
use std::io::ErrorKind;
use std::time::Duration;

lazy_static! {
    pub static ref WRITES_TOTAL: IntCounter =
        register_int_counter!("omnikv_writes_total", "Total number of write operations").expect(
            "OmniKV metric registration failed at startup: duplicate or invalid metric name"
        );
    pub static ref READS_TOTAL: IntCounter =
        register_int_counter!("omnikv_reads_total", "Total number of read operations").expect(
            "OmniKV metric registration failed at startup: duplicate or invalid metric name"
        );
    pub static ref ACTIVE_TRANSACTIONS: IntGauge = register_int_gauge!(
        "omnikv_active_transactions",
        "Number of currently active transactions"
    )
    .expect("OmniKV metric registration failed at startup: duplicate or invalid metric name");
    pub static ref COMPACTIONS_TOTAL: IntCounter = register_int_counter!(
        "omnikv_compactions_total",
        "Total number of compaction runs"
    )
    .expect("OmniKV metric registration failed at startup: duplicate or invalid metric name");
    pub static ref COMPACTION_LATENCY_SECONDS: HistogramVec = register_histogram_vec!(
        HistogramOpts::new(
            "omnikv_compaction_latency_seconds",
            "Compaction latency by compaction stage"
        ),
        &["stage"]
    )
    .expect("OmniKV metric registration failed at startup: duplicate or invalid metric name");
    pub static ref COMPACTION_BYTES_REWRITTEN_TOTAL: IntCounterVec = register_int_counter_vec!(
        "omnikv_compaction_bytes_rewritten_total",
        "Total bytes rewritten by compaction stage",
        &["stage"]
    )
    .expect("OmniKV metric registration failed at startup: duplicate or invalid metric name");
    pub static ref COMPACTION_TOMBSTONES_TOTAL: IntCounterVec = register_int_counter_vec!(
        "omnikv_compaction_tombstones_total",
        "Total tombstone records observed or dropped by compaction stage",
        &["stage"]
    )
    .expect("OmniKV metric registration failed at startup: duplicate or invalid metric name");
    pub static ref COMPACTION_EXPIRED_RECORDS_DROPPED_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "omnikv_compaction_expired_records_dropped_total",
            "Total expired records dropped by compaction stage",
            &["stage"]
        )
        .expect("OmniKV metric registration failed at startup: duplicate or invalid metric name");
    pub static ref COMPACTION_BACKLOG_SSTABLES: IntGauge = register_int_gauge!(
        "omnikv_compaction_backlog_sstables",
        "Current compaction backlog measured as L0 plus L1 SSTables"
    )
    .expect("OmniKV metric registration failed at startup: duplicate or invalid metric name");
    pub static ref WRITE_STALLS_TOTAL: IntCounter = register_int_counter!(
        "omnikv_write_stalls_total",
        "Total writes rejected because compaction backlog exceeded the write-stall threshold"
    )
    .expect("OmniKV metric registration failed at startup: duplicate or invalid metric name");
    pub static ref WRITE_LATENCY: Histogram = register_histogram!(HistogramOpts::new(
        "omnikv_write_latency_seconds",
        "Write operation latency"
    ))
    .expect("OmniKV metric registration failed at startup: duplicate or invalid metric name");
    pub static ref READ_LATENCY: Histogram = register_histogram!(HistogramOpts::new(
        "omnikv_read_latency_seconds",
        "Read operation latency"
    ))
    .expect("OmniKV metric registration failed at startup: duplicate or invalid metric name");
    pub static ref MEMTABLE_SIZE: IntGauge = register_int_gauge!(
        "omnikv_memtable_size_bytes",
        "Current memtable size in bytes"
    )
    .expect("OmniKV metric registration failed at startup: duplicate or invalid metric name");
    pub static ref SSTABLE_COUNT: IntGauge =
        register_int_gauge!("omnikv_sstable_count", "Total number of SSTables (L0 + L1)").expect(
            "OmniKV metric registration failed at startup: duplicate or invalid metric name"
        );
    pub static ref DB_SEQUENCE: IntGauge = register_int_gauge!(
        "omnikv_db_sequence",
        "Current database write sequence number"
    )
    .expect("OmniKV metric registration failed at startup: duplicate or invalid metric name");
    pub static ref UPTIME_SECONDS: IntGauge =
        register_int_gauge!("omnikv_uptime_seconds", "Server uptime in seconds").expect(
            "OmniKV metric registration failed at startup: duplicate or invalid metric name"
        );
    pub static ref COMMIT_RATE: IntCounter =
        register_int_counter!("omnikv_commits_total", "Total number of committed batches").expect(
            "OmniKV metric registration failed at startup: duplicate or invalid metric name"
        );
    pub static ref RATE_LIMIT_REJECTIONS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "omnikv_rate_limit_rejections_total",
        "Total number of rejected requests by protocol due to rate limiting",
        &["protocol"]
    )
    .expect("OmniKV metric registration failed at startup: duplicate or invalid metric name");
    pub static ref CLEANUP_DELETE_FAILURES_TOTAL: IntCounterVec = register_int_counter_vec!(
        "omnikv_cleanup_delete_failures_total",
        "Total number of obsolete-file cleanup delete failures by context and error kind",
        &["context", "error_kind"]
    )
    .expect("OmniKV metric registration failed at startup: duplicate or invalid metric name");
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

/// Record a completed compaction or garbage-collection maintenance pass.
pub fn record_compaction(
    stage: &str,
    elapsed: Duration,
    bytes_rewritten: u64,
    tombstones: u64,
    expired_records_dropped: u64,
    backlog_sstables: usize,
) {
    COMPACTIONS_TOTAL.inc();
    COMPACTION_LATENCY_SECONDS
        .with_label_values(&[stage])
        .observe(elapsed.as_secs_f64());
    COMPACTION_BYTES_REWRITTEN_TOTAL
        .with_label_values(&[stage])
        .inc_by(bytes_rewritten);
    COMPACTION_TOMBSTONES_TOTAL
        .with_label_values(&[stage])
        .inc_by(tombstones);
    COMPACTION_EXPIRED_RECORDS_DROPPED_TOTAL
        .with_label_values(&[stage])
        .inc_by(expired_records_dropped);
    record_compaction_backlog(backlog_sstables);
}

/// Update the observable compaction backlog gauge.
pub fn record_compaction_backlog(backlog_sstables: usize) {
    COMPACTION_BACKLOG_SSTABLES.set(backlog_sstables as i64);
}

/// Record a write stall caused by an excessive compaction backlog.
pub fn record_write_stall() {
    WRITE_STALLS_TOTAL.inc();
}

/// Record that a public protocol rejected a request because of rate limiting.
pub fn record_rate_limit_rejection(protocol: &str) {
    RATE_LIMIT_REJECTIONS_TOTAL
        .with_label_values(&[protocol])
        .inc();
}

/// Record that best-effort obsolete-file cleanup failed to delete a file.
pub fn record_cleanup_delete_failure(context: &str, error_kind: ErrorKind) {
    let error_kind = format!("{error_kind:?}");
    CLEANUP_DELETE_FAILURES_TOTAL
        .with_label_values(&[context, &error_kind])
        .inc();
}
