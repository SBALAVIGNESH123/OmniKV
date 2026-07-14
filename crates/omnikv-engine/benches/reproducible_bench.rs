//! Reproducible `OmniKV` benchmark harness.
//!
//! This benchmark is intentionally a small no-framework binary so it can run in
//! CI smoke mode and on contributor laptops without extra tooling.
//!
//! Examples:
//!
//! ```bash
//! cargo bench -p omnikv-engine --bench reproducible_bench -- --profile smoke --json-out target/omnikv-benchmark-smoke.json
//! cargo bench -p omnikv-engine --bench reproducible_bench -- --profile standard --json-out target/omnikv-benchmark-standard.json
//! ```

#![expect(
    clippy::print_stdout,
    reason = "Benchmark binaries intentionally print human progress and keep workload definitions in one auditable file."
)]

use omni_engine::raft_storage::OmniRaftStorage;
use omni_engine::transaction::TransactionManager;
use omni_engine::{OmniKV, WriteBatch};
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy)]
enum Profile {
    Smoke,
    Standard,
}

impl Profile {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Standard => "standard",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "smoke" => Some(Self::Smoke),
            "standard" => Some(Self::Standard),
            _ => None,
        }
    }

    const fn scale(self) -> WorkloadScale {
        match self {
            Self::Smoke => WorkloadScale {
                write_ops: 300,
                read_seed: 400,
                read_ops: 800,
                mixed_seed: 400,
                mixed_ops: 800,
                scan_rows: 600,
                scan_rounds: 12,
                compaction_segments: 4,
                compaction_writes_per_segment: 150,
                transaction_ops: 250,
                raft_entries: 200,
            },
            Self::Standard => WorkloadScale {
                write_ops: 10_000,
                read_seed: 10_000,
                read_ops: 50_000,
                mixed_seed: 10_000,
                mixed_ops: 50_000,
                scan_rows: 20_000,
                scan_rounds: 100,
                compaction_segments: 8,
                compaction_writes_per_segment: 2_000,
                transaction_ops: 5_000,
                raft_entries: 5_000,
            },
        }
    }
}

#[derive(Clone, Copy, Serialize)]
struct WorkloadScale {
    write_ops: u64,
    read_seed: u64,
    read_ops: u64,
    mixed_seed: u64,
    mixed_ops: u64,
    scan_rows: u64,
    scan_rounds: u64,
    compaction_segments: u64,
    compaction_writes_per_segment: u64,
    transaction_ops: u64,
    raft_entries: u64,
}

struct Options {
    profile: Profile,
    json_out: Option<PathBuf>,
}

#[derive(Serialize)]
struct BenchmarkReport {
    schema_version: u32,
    generated_at_unix_seconds: u64,
    metadata: BenchmarkMetadata,
    workload_scale: WorkloadScale,
    results: Vec<WorkloadResult>,
}

#[derive(Serialize)]
struct BenchmarkMetadata {
    package_version: &'static str,
    profile: String,
    os: &'static str,
    arch: &'static str,
    rustc_version: Option<String>,
    git_commit: Option<String>,
    notes: Vec<String>,
}

#[derive(Serialize)]
struct WorkloadResult {
    name: &'static str,
    mode: &'static str,
    operations: u64,
    successful_operations: u64,
    errors: u64,
    elapsed_ms: u128,
    throughput_ops_per_sec: f64,
    latency_us: LatencySummary,
    resources: ResourceSummary,
    compaction: Option<CompactionSummary>,
    notes: Vec<String>,
}

#[derive(Default, Serialize)]
struct LatencySummary {
    p50: f64,
    p95: f64,
    p99: f64,
}

#[derive(Serialize)]
struct ResourceSummary {
    rss_bytes_start: Option<u64>,
    rss_bytes_end: Option<u64>,
    cpu_user_ms_delta: Option<u64>,
    cpu_system_ms_delta: Option<u64>,
    data_dir_bytes_start: u64,
    data_dir_bytes_end: u64,
    disk_growth_bytes: i128,
    wal_bytes_end: u64,
}

#[derive(Serialize)]
struct CompactionSummary {
    memtable_to_l0_ms: u128,
    l0_to_l1_ms: u128,
    l0_sstables_before_l1: usize,
    l0_sstables_after_l1: usize,
    l1_sstables_after_l1: usize,
}

struct LatencyTracker {
    samples: Vec<u64>,
}

impl LatencyTracker {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            samples: Vec::with_capacity(capacity),
        }
    }

    fn record(&mut self, duration: Duration) {
        self.samples
            .push(u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX));
    }

    fn summary(&mut self) -> LatencySummary {
        if self.samples.is_empty() {
            return LatencySummary::default();
        }
        self.samples.sort_unstable();
        LatencySummary {
            p50: nanos_to_micros(self.percentile_nanos(50)),
            p95: nanos_to_micros(self.percentile_nanos(95)),
            p99: nanos_to_micros(self.percentile_nanos(99)),
        }
    }

    fn percentile_nanos(&self, percentile: usize) -> u64 {
        let index = percentile.saturating_mul(self.samples.len()) / 100;
        self.samples[index.min(self.samples.len() - 1)]
    }
}

struct BenchDb {
    _dir: tempfile::TempDir,
    root: PathBuf,
    wal: PathBuf,
    db: Arc<OmniKV>,
}

impl BenchDb {
    fn new(label: &str) -> Self {
        let dir = tempfile::Builder::new()
            .prefix(&format!("omnikv-bench-{label}-"))
            .tempdir()
            .expect("create benchmark tempdir");
        let root = dir.path().to_path_buf();
        let manifest = root.join("manifest.json");
        let wal = root.join("wal.bin");
        let db = OmniKV::open(
            manifest.to_string_lossy().as_ref(),
            wal.to_string_lossy().as_ref(),
        )
        .expect("open benchmark database");
        Self {
            _dir: dir,
            root,
            wal,
            db,
        }
    }
}

#[derive(Clone, Copy)]
struct ResourceSample {
    rss_bytes: Option<u64>,
    cpu_user_ms: Option<u64>,
    cpu_system_ms: Option<u64>,
    data_dir_bytes: u64,
}

fn main() {
    let options = parse_options();
    let scale = options.profile.scale();
    let mut results = Vec::new();

    println!(
        "OmniKV reproducible benchmark: profile={} version={}",
        options.profile.as_str(),
        env!("CARGO_PKG_VERSION")
    );

    results.push(bench_write_heavy(scale.write_ops));
    results.push(bench_read_heavy(scale.read_seed, scale.read_ops));
    results.push(bench_mixed(scale.mixed_seed, scale.mixed_ops));
    results.push(bench_range_scan(scale.scan_rows, scale.scan_rounds));
    results.push(bench_compaction(
        scale.compaction_segments,
        scale.compaction_writes_per_segment,
    ));
    results.push(bench_transactions(scale.transaction_ops));
    results.push(bench_replicated_simulated(scale.raft_entries));

    let report = BenchmarkReport {
        schema_version: 1,
        generated_at_unix_seconds: unix_seconds(),
        metadata: BenchmarkMetadata {
            package_version: env!("CARGO_PKG_VERSION"),
            profile: options.profile.as_str().to_string(),
            os: std::env::consts::OS,
            arch: std::env::consts::ARCH,
            rustc_version: command_output("rustc", &["--version"]),
            git_commit: command_output("git", &["rev-parse", "HEAD"]),
            notes: vec![
                "single-node workloads use one local OmniKV instance".into(),
                "replicated-simulated workload uses three local OmniRaftStorage instances without network transport".into(),
                "CPU and RSS are process-level samples; use external profilers for formal publication".into(),
            ],
        },
        workload_scale: scale,
        results,
    };

    let json = serde_json::to_string_pretty(&report).expect("serialize benchmark report");
    if let Some(path) = options.json_out {
        let path = resolve_workspace_path(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create benchmark JSON parent directory");
        }
        std::fs::write(&path, json).expect("write benchmark JSON");
        println!("JSON report written to {}", path.display());
    } else {
        println!("{json}");
    }
}

fn bench_write_heavy(operations: u64) -> WorkloadResult {
    let bench = BenchDb::new("write");
    let start_sample = sample_resources(&bench.root);
    let mut latencies = LatencyTracker::with_capacity(to_capacity(operations));
    let started = Instant::now();
    let mut success = 0u64;
    let mut errors = 0u64;

    for i in 0..operations {
        let mut batch = WriteBatch::new();
        batch
            .set(&format!("write:{i:010}"), "x".repeat(128))
            .expect("valid benchmark key");
        let op_started = Instant::now();
        match bench.db.commit_batch(&batch) {
            Ok(_) => success += 1,
            Err(_) => errors += 1,
        }
        latencies.record(op_started.elapsed());
    }

    make_result(
        "write-heavy-128b",
        "single-node",
        operations,
        success,
        errors,
        started.elapsed(),
        latencies,
        &bench,
        start_sample,
        None,
        vec!["single key per committed batch".into()],
    )
}

fn bench_read_heavy(seed: u64, operations: u64) -> WorkloadResult {
    let bench = BenchDb::new("read");
    seed_rows(&bench.db, "read", seed);
    let start_sample = sample_resources(&bench.root);
    let mut latencies = LatencyTracker::with_capacity(to_capacity(operations));
    let started = Instant::now();
    let read_seq = bench.db.get_seq();
    let mut success = 0u64;
    let mut errors = 0u64;

    for i in 0..operations {
        let key = format!("read:{:010}", pseudo_random_index(i, seed));
        let op_started = Instant::now();
        match bench.db.find(&key, read_seq) {
            Ok(Some(_)) => success += 1,
            Ok(None) | Err(_) => errors += 1,
        }
        latencies.record(op_started.elapsed());
    }

    make_result(
        "read-heavy-random-hit",
        "single-node",
        operations,
        success,
        errors,
        started.elapsed(),
        latencies,
        &bench,
        start_sample,
        None,
        vec![format!("prepopulated {seed} keys outside measured window")],
    )
}

fn bench_mixed(seed: u64, operations: u64) -> WorkloadResult {
    let bench = BenchDb::new("mixed");
    seed_rows(&bench.db, "mixed-seed", seed);
    let start_sample = sample_resources(&bench.root);
    let mut latencies = LatencyTracker::with_capacity(to_capacity(operations));
    let started = Instant::now();
    let mut success = 0u64;
    let mut errors = 0u64;

    for i in 0..operations {
        let op_started = Instant::now();
        let result = if i % 5 == 0 {
            let mut batch = WriteBatch::new();
            batch
                .set(&format!("mixed-write:{i:010}"), "m".repeat(128))
                .expect("valid benchmark key");
            bench.db.commit_batch(&batch).map(|_| Some(String::new()))
        } else {
            let key = format!("mixed-seed:{:010}", pseudo_random_index(i, seed));
            bench.db.find(&key, bench.db.get_seq())
        };
        match result {
            Ok(_) => success += 1,
            Err(_) => errors += 1,
        }
        latencies.record(op_started.elapsed());
    }

    make_result(
        "mixed-80r-20w",
        "single-node",
        operations,
        success,
        errors,
        started.elapsed(),
        latencies,
        &bench,
        start_sample,
        None,
        vec![format!(
            "prepopulated {seed} read keys outside measured window"
        )],
    )
}

fn bench_range_scan(rows: u64, rounds: u64) -> WorkloadResult {
    let bench = BenchDb::new("scan");
    seed_rows(&bench.db, "scan", rows);
    let start_sample = sample_resources(&bench.root);
    let mut latencies = LatencyTracker::with_capacity(to_capacity(rounds));
    let started = Instant::now();
    let mut success = 0u64;
    let mut errors = 0u64;
    let mut returned_rows = 0u64;

    for _ in 0..rounds {
        let op_started = Instant::now();
        match bench.db.scan(
            "scan:0000000000",
            &format!("scan:{rows:010}"),
            bench.db.get_seq(),
        ) {
            Ok(results) => {
                success += 1;
                returned_rows += u64::try_from(results.len()).unwrap_or(u64::MAX);
            }
            Err(_) => errors += 1,
        }
        latencies.record(op_started.elapsed());
    }

    make_result(
        "range-scan-full-prefix",
        "single-node",
        rounds,
        success,
        errors,
        started.elapsed(),
        latencies,
        &bench,
        start_sample,
        None,
        vec![format!(
            "{returned_rows} rows returned across {rounds} scans"
        )],
    )
}

fn bench_compaction(segments: u64, writes_per_segment: u64) -> WorkloadResult {
    let bench = BenchDb::new("compaction");
    let start_sample = sample_resources(&bench.root);
    let mut latencies =
        LatencyTracker::with_capacity(to_capacity(segments.saturating_mul(writes_per_segment)));
    let started = Instant::now();
    let mut success = 0u64;
    let mut errors = 0u64;
    let mut memtable_to_l0_ms = 0u128;

    for segment in 0..segments {
        for i in 0..writes_per_segment {
            let mut batch = WriteBatch::new();
            batch
                .set(&format!("compact:{segment:04}:{i:010}"), "c".repeat(256))
                .expect("valid benchmark key");
            let op_started = Instant::now();
            match bench.db.commit_batch(&batch) {
                Ok(_) => success += 1,
                Err(_) => errors += 1,
            }
            latencies.record(op_started.elapsed());
        }
        let compact_started = Instant::now();
        if bench.db.compact_sstables().is_ok() {
            memtable_to_l0_ms += compact_started.elapsed().as_millis();
        } else {
            errors += 1;
        }
    }

    let l0_before = bench.db.sstable_count();
    let l0_l1_started = Instant::now();
    if bench.db.compact_l0_to_l1().is_err() {
        errors += 1;
    }
    let l0_to_l1_ms = l0_l1_started.elapsed().as_millis();

    let compaction = CompactionSummary {
        memtable_to_l0_ms,
        l0_to_l1_ms,
        l0_sstables_before_l1: l0_before,
        l0_sstables_after_l1: bench.db.sstable_count(),
        l1_sstables_after_l1: bench.db.l1_sstable_count(),
    };

    make_result(
        "compaction-l0-to-l1",
        "single-node",
        segments.saturating_mul(writes_per_segment),
        success,
        errors,
        started.elapsed(),
        latencies,
        &bench,
        start_sample,
        Some(compaction),
        vec![format!(
            "{segments} memtable flushes before L0-to-L1 compaction"
        )],
    )
}

fn bench_transactions(operations: u64) -> WorkloadResult {
    let bench = BenchDb::new("txn");
    let manager = TransactionManager::new(bench.db.clone());
    let start_sample = sample_resources(&bench.root);
    let mut latencies = LatencyTracker::with_capacity(to_capacity(operations));
    let started = Instant::now();
    let mut success = 0u64;
    let mut errors = 0u64;

    for i in 0..operations {
        let op_started = Instant::now();
        let mut txn = manager.begin();
        let result = manager
            .set(&mut txn, &format!("txn:{i:010}"), format!("value-{i}"))
            .and_then(|()| manager.commit(&mut txn));
        match result {
            Ok(_) => success += 1,
            Err(_) => errors += 1,
        }
        latencies.record(op_started.elapsed());
    }

    make_result(
        "transaction-commit",
        "single-node",
        operations,
        success,
        errors,
        started.elapsed(),
        latencies,
        &bench,
        start_sample,
        None,
        vec!["SSI transaction manager set+commit".into()],
    )
}

fn bench_replicated_simulated(entries: u64) -> WorkloadResult {
    let leader = BenchDb::new("raft-leader");
    let follower_a = BenchDb::new("raft-follower-a");
    let follower_b = BenchDb::new("raft-follower-b");
    let node_leader = OmniRaftStorage::new(leader.db.clone());
    let node_a = OmniRaftStorage::new(follower_a.db);
    let node_b = OmniRaftStorage::new(follower_b.db);

    let start_sample = sample_resources(&leader.root);
    let mut latencies = LatencyTracker::with_capacity(to_capacity(entries));
    let started = Instant::now();
    let mut success = 0u64;
    let mut errors = 0u64;

    for i in 1..=entries {
        let command = format!("SET replicated:{i:010} value-{i}");
        let op_started = Instant::now();
        let result = node_leader
            .append_log(i, &command)
            .and_then(|()| node_a.append_log(i, &command))
            .and_then(|()| node_b.append_log(i, &command))
            .and_then(|()| node_leader.apply_write(&command))
            .and_then(|()| node_a.apply_write(&command))
            .and_then(|()| node_b.apply_write(&command))
            .and_then(|()| node_leader.mark_applied(i))
            .and_then(|()| node_a.mark_applied(i))
            .and_then(|()| node_b.mark_applied(i));
        match result {
            Ok(()) => success += 1,
            Err(_) => errors += 1,
        }
        latencies.record(op_started.elapsed());
    }

    let mut notes = vec![
        "three local OmniRaftStorage instances".into(),
        "measures log append, fan-out to two followers, apply, and mark_applied".into(),
        "does not include network transport, election, or real process isolation".into(),
    ];
    notes.push(format!(
        "last_applied leader={} follower_a={} follower_b={}",
        node_leader.last_applied_index(),
        node_a.last_applied_index(),
        node_b.last_applied_index()
    ));

    make_result(
        "raft-replicated-simulated",
        "replicated-simulated",
        entries,
        success,
        errors,
        started.elapsed(),
        latencies,
        &leader,
        start_sample,
        None,
        notes,
    )
}

fn seed_rows(db: &Arc<OmniKV>, prefix: &str, rows: u64) {
    for i in 0..rows {
        let mut batch = WriteBatch::new();
        batch
            .set(&format!("{prefix}:{i:010}"), format!("value-{i}"))
            .expect("valid seed key");
        db.commit_batch(&batch).expect("seed benchmark row");
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "Benchmark result construction keeps every measured field explicit at call sites."
)]
fn make_result(
    name: &'static str,
    mode: &'static str,
    operations: u64,
    success: u64,
    errors: u64,
    elapsed: Duration,
    mut latencies: LatencyTracker,
    bench: &BenchDb,
    start_sample: ResourceSample,
    compaction: Option<CompactionSummary>,
    notes: Vec<String>,
) -> WorkloadResult {
    let end_sample = sample_resources(&bench.root);
    let resources = ResourceSummary {
        rss_bytes_start: start_sample.rss_bytes,
        rss_bytes_end: end_sample.rss_bytes,
        cpu_user_ms_delta: option_delta(start_sample.cpu_user_ms, end_sample.cpu_user_ms),
        cpu_system_ms_delta: option_delta(start_sample.cpu_system_ms, end_sample.cpu_system_ms),
        data_dir_bytes_start: start_sample.data_dir_bytes,
        data_dir_bytes_end: end_sample.data_dir_bytes,
        disk_growth_bytes: i128::from(end_sample.data_dir_bytes)
            - i128::from(start_sample.data_dir_bytes),
        wal_bytes_end: file_len(&bench.wal),
    };

    let result = WorkloadResult {
        name,
        mode,
        operations,
        successful_operations: success,
        errors,
        elapsed_ms: elapsed.as_millis(),
        throughput_ops_per_sec: per_second(success, elapsed),
        latency_us: latencies.summary(),
        resources,
        compaction,
        notes,
    };

    println!(
        "{:<28} {:<20} ops={:<8} ok={:<8} err={:<4} {:>10.0} ops/s p99={:>8.1}us",
        result.name,
        result.mode,
        result.operations,
        result.successful_operations,
        result.errors,
        result.throughput_ops_per_sec,
        result.latency_us.p99,
    );
    result
}

fn parse_options() -> Options {
    let mut profile = Profile::Smoke;
    let mut json_out = None;
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--profile" => {
                let value = args
                    .get(i + 1)
                    .unwrap_or_else(|| panic!("--profile requires smoke or standard"));
                profile = Profile::from_str(value)
                    .unwrap_or_else(|| panic!("unsupported profile: {value}"));
                i += 2;
            }
            "--json-out" => {
                let value = args
                    .get(i + 1)
                    .unwrap_or_else(|| panic!("--json-out requires a file path"));
                json_out = Some(PathBuf::from(value));
                i += 2;
            }
            "--help" | "-h" => {
                println!(
                    "Usage: cargo bench -p omnikv-engine --bench reproducible_bench -- --profile smoke|standard --json-out <path>"
                );
                std::process::exit(0);
            }
            _ => {
                i += 1;
            }
        }
    }
    Options { profile, json_out }
}

fn resolve_workspace_path(path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        return path;
    }
    workspace_root().join(path)
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("omnikv-engine should live under workspace_root/crates/omnikv-engine")
        .to_path_buf()
}

fn sample_resources(root: &Path) -> ResourceSample {
    let (cpu_user_ms, cpu_system_ms) = process_cpu_ms();
    ResourceSample {
        rss_bytes: process_rss_bytes(),
        cpu_user_ms,
        cpu_system_ms,
        data_dir_bytes: dir_size(root),
    }
}

fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    for entry in entries.flatten() {
        let entry_path = entry.path();
        if let Ok(metadata) = entry.metadata() {
            if metadata.is_dir() {
                total = total.saturating_add(dir_size(&entry_path));
            } else {
                total = total.saturating_add(metadata.len());
            }
        }
    }
    total
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).map_or(0, |metadata| metadata.len())
}

#[cfg(target_os = "linux")]
fn process_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
            return Some(kb.saturating_mul(1024));
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
const fn process_rss_bytes() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn process_cpu_ms() -> (Option<u64>, Option<u64>) {
    const ASSUMED_CLK_TCK: u64 = 100;
    let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else {
        return (None, None);
    };
    let Some(end_comm) = stat.rfind(") ") else {
        return (None, None);
    };
    let fields: Vec<&str> = stat[end_comm + 2..].split_whitespace().collect();
    let Some(utime_ticks) = fields.get(11).and_then(|value| value.parse::<u64>().ok()) else {
        return (None, None);
    };
    let Some(stime_ticks) = fields.get(12).and_then(|value| value.parse::<u64>().ok()) else {
        return (None, None);
    };
    (
        Some(utime_ticks.saturating_mul(1000) / ASSUMED_CLK_TCK),
        Some(stime_ticks.saturating_mul(1000) / ASSUMED_CLK_TCK),
    )
}

#[cfg(not(target_os = "linux"))]
const fn process_cpu_ms() -> (Option<u64>, Option<u64>) {
    (None, None)
}

fn command_output(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    Some(text.trim().to_string())
}

const fn pseudo_random_index(i: u64, keyspace: u64) -> u64 {
    if keyspace == 0 {
        return 0;
    }
    i.wrapping_mul(1_103_515_245).wrapping_add(12_345) % keyspace
}

fn option_delta(start: Option<u64>, end: Option<u64>) -> Option<u64> {
    Some(end?.saturating_sub(start?))
}

#[expect(
    clippy::cast_precision_loss,
    reason = "Benchmark reports are comparative diagnostics; f64 throughput is appropriate."
)]
fn per_second(count: u64, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return 0.0;
    }
    count as f64 / elapsed.as_secs_f64()
}

#[expect(
    clippy::cast_precision_loss,
    reason = "Benchmark latency reports are approximate microsecond summaries."
)]
fn nanos_to_micros(nanos: u64) -> f64 {
    nanos as f64 / 1_000.0
}

fn to_capacity(value: u64) -> usize {
    usize::try_from(value.min(200_000)).unwrap_or(200_000)
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
