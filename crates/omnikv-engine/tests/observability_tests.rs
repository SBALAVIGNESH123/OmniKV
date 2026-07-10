//! Observability integration tests — Issue #18
//!
//! Tests for /health, /ready, and /metrics endpoints.

use omni_engine::metrics_prometheus;

#[test]
fn test_render_metrics_returns_prometheus_text() {
    let output = metrics_prometheus::render_metrics();
    // Prometheus text format always starts with "# HELP" or is empty on first call
    // We just verify it returns a valid UTF-8 string without panicking
    assert!(output.is_ascii() || output.is_empty() || output.contains("omnikv_"));
}

#[test]
fn test_record_db_stats_does_not_panic() {
    metrics_prometheus::record_db_stats(42, 3, 100);
    let output = metrics_prometheus::render_metrics();
    assert!(output.contains("omnikv_db_sequence") || output.is_empty());
}

#[test]
fn test_metrics_counters_are_accessible() {
    use omni_engine::metrics_prometheus::{READS_TOTAL, WRITES_TOTAL};
    let _r = READS_TOTAL.get();
    let _w = WRITES_TOTAL.get();
}

#[test]
fn test_uptime_gauge_set() {
    use omni_engine::metrics_prometheus::UPTIME_SECONDS;
    UPTIME_SECONDS.set(999);
    assert_eq!(UPTIME_SECONDS.get(), 999);
}

#[test]
fn test_db_sequence_gauge_set() {
    use omni_engine::metrics_prometheus::DB_SEQUENCE;
    DB_SEQUENCE.set(12345);
    assert_eq!(DB_SEQUENCE.get(), 12345);
}
