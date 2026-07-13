//! Real-data replay harness for `OmniKV`.
//!
//! This binary imports a JSONL export into a fresh `OmniKV` workdir, verifies
//! exact read-back, reopens the database, optionally runs compaction/GC, and
//! emits a machine-readable report. It is intentionally offline-first: run it
//! against copied or shadow data, never the only copy of critical production
//! data.

use crc32fast::Hasher;
use omni_engine::{OmniKV, WriteBatch};
use serde::Serialize;
use serde_json::Value;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

const MARKER_FILE: &str = ".omnikv-real-data-replay";
const DEFAULT_BATCH_SIZE: usize = 1_000;
const DEFAULT_KEY_PREFIX: &str = "real:";

#[derive(Debug)]
struct Config {
    input: PathBuf,
    workdir: PathBuf,
    report: Option<PathBuf>,
    key_field: Option<String>,
    key_prefix: String,
    batch_size: usize,
    compact: bool,
    reset: bool,
}

#[derive(Debug)]
struct ReplayRecord {
    line_number: u64,
    key: String,
    value: String,
}

#[derive(Debug, Serialize)]
struct ReplayReport {
    status: &'static str,
    input: String,
    workdir: String,
    key_mode: String,
    rows_imported: u64,
    source_checksum: u64,
    import_elapsed_ms: u128,
    verify_after_import: VerifyReport,
    verify_after_reopen: VerifyReport,
    verify_after_compaction: Option<VerifyReport>,
}

#[derive(Debug, Serialize)]
struct VerifyReport {
    phase: &'static str,
    rows_checked: u64,
    checksum: u64,
    mismatches: u64,
    elapsed_ms: u128,
    samples: Vec<String>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("real_data_replay failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), DynError> {
    let config = parse_args(std::env::args().skip(1))?;
    prepare_workdir(&config.workdir, config.reset)?;

    let manifest_path = config.workdir.join("manifest.json");
    let wal_path = config.workdir.join("data.wal");

    let started = Instant::now();
    let db = OmniKV::open(path_str(&manifest_path)?, path_str(&wal_path)?)?;
    let source = import_records(&config, &db)?;
    let import_elapsed_ms = started.elapsed().as_millis();
    let verify_after_import = verify_records("after_import", &config, &db)?;
    drop(db);

    let db = OmniKV::open(path_str(&manifest_path)?, path_str(&wal_path)?)?;
    let verify_after_reopen = verify_records("after_reopen", &config, &db)?;
    let verify_after_compaction = if config.compact {
        db.compact_sstables()?;
        db.compact_l0_to_l1()?;
        db.compact_l1_to_l2()?;
        db.run_garbage_collection()?;
        Some(verify_records("after_compaction", &config, &db)?)
    } else {
        None
    };

    let report = ReplayReport {
        status: if report_passed(
            &verify_after_import,
            &verify_after_reopen,
            verify_after_compaction.as_ref(),
        ) {
            "passed"
        } else {
            "failed"
        },
        input: config.input.display().to_string(),
        workdir: config.workdir.display().to_string(),
        key_mode: config.key_field.as_ref().map_or_else(
            || "line-number".to_string(),
            |field| format!("json-field:{field}"),
        ),
        rows_imported: source.rows_checked,
        source_checksum: source.checksum,
        import_elapsed_ms,
        verify_after_import,
        verify_after_reopen,
        verify_after_compaction,
    };

    let json = serde_json::to_string_pretty(&report)?;
    if let Some(path) = &config.report {
        fs::write(path, &json)?;
    }
    println!("{json}");

    if report.status == "passed" {
        Ok(())
    } else {
        Err(make_error("replay verification failed"))
    }
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Config, DynError> {
    let mut input = None;
    let mut workdir = PathBuf::from("target/omnikv-real-data-replay");
    let mut report = None;
    let mut key_field = None;
    let mut key_prefix = DEFAULT_KEY_PREFIX.to_string();
    let mut batch_size = DEFAULT_BATCH_SIZE;
    let mut compact = true;
    let mut reset = false;

    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            "--input" => input = Some(PathBuf::from(take_value(&mut args, "--input")?)),
            "--workdir" => workdir = PathBuf::from(take_value(&mut args, "--workdir")?),
            "--report" => report = Some(PathBuf::from(take_value(&mut args, "--report")?)),
            "--key-field" => key_field = Some(take_value(&mut args, "--key-field")?),
            "--key-prefix" => key_prefix = take_value(&mut args, "--key-prefix")?,
            "--batch-size" => {
                let value = take_value(&mut args, "--batch-size")?;
                batch_size = value.parse::<usize>()?;
                if batch_size == 0 {
                    return Err(make_error("--batch-size must be greater than 0"));
                }
            }
            "--no-compact" => compact = false,
            "--reset" => reset = true,
            unknown => return Err(make_error(format!("unknown argument: {unknown}"))),
        }
    }

    let input = input.ok_or_else(|| make_error("--input is required"))?;
    Ok(Config {
        input,
        workdir,
        report,
        key_field,
        key_prefix,
        batch_size,
        compact,
        reset,
    })
}

fn take_value(
    args: &mut std::iter::Peekable<impl Iterator<Item = String>>,
    flag: &str,
) -> Result<String, DynError> {
    args.next()
        .filter(|value| !value.starts_with("--"))
        .ok_or_else(|| make_error(format!("{flag} requires a value")))
}

fn print_usage() {
    println!(
        "Usage: real_data_replay --input events.jsonl [--workdir target/omnikv-real-data-replay] [options]\n\
\n\
Options:\n\
  --input PATH        JSONL input file. Each non-empty line must be valid JSON.\n\
  --workdir PATH      Fresh OmniKV work directory for replay evidence.\n\
  --report PATH       Write the JSON report to this path as well as stdout.\n\
  --key-field FIELD   Use a top-level JSON field as the record key.\n\
  --key-prefix TEXT   Prefix generated keys. Default: real:\n\
  --batch-size N      Commit every N records. Default: 1000.\n\
  --no-compact        Skip compaction and heap-GC verification.\n\
  --reset             Reuse a previously harness-marked workdir.\n\
  --help              Show this help text."
    );
}

fn prepare_workdir(workdir: &Path, reset: bool) -> Result<(), DynError> {
    if workdir.exists() {
        let marker = workdir.join(MARKER_FILE);
        if reset {
            if marker.exists() || directory_is_empty(workdir)? {
                fs::remove_dir_all(workdir)?;
            } else {
                return Err(make_error(format!(
                    "refusing to reset unmarked non-empty workdir: {}",
                    workdir.display()
                )));
            }
        } else if !directory_is_empty(workdir)? {
            return Err(make_error(format!(
                "workdir is not empty; use a fresh directory or --reset: {}",
                workdir.display()
            )));
        }
    }

    fs::create_dir_all(workdir)?;
    fs::write(
        workdir.join(MARKER_FILE),
        "created by OmniKV real_data_replay\n",
    )?;
    Ok(())
}

fn directory_is_empty(path: &Path) -> Result<bool, DynError> {
    Ok(fs::read_dir(path)?.next().is_none())
}

fn import_records(config: &Config, db: &Arc<OmniKV>) -> Result<VerifyReport, DynError> {
    let started = Instant::now();
    let mut batch = WriteBatch::new();
    let mut rows = 0;
    let mut checksum = 0;

    for record in records(config)? {
        let record = record?;
        update_checksum(&mut checksum, &record.key, &record.value);
        batch.set(&record.key, record.value)?;
        rows += 1;

        if rows % config.batch_size as u64 == 0 {
            db.commit_batch(&batch)?;
            batch = WriteBatch::new();
        }
    }

    if !batch.is_empty() {
        db.commit_batch(&batch)?;
    }

    Ok(VerifyReport {
        phase: "source_import",
        rows_checked: rows,
        checksum,
        mismatches: 0,
        elapsed_ms: started.elapsed().as_millis(),
        samples: Vec::new(),
    })
}

fn verify_records(
    phase: &'static str,
    config: &Config,
    db: &Arc<OmniKV>,
) -> Result<VerifyReport, DynError> {
    let started = Instant::now();
    let seq = db.get_seq();
    let mut rows = 0;
    let mut checksum = 0;
    let mut mismatches = 0;
    let mut samples = Vec::new();

    for record in records(config)? {
        let record = record?;
        rows += 1;
        update_checksum(&mut checksum, &record.key, &record.value);
        match db.find(&record.key, seq)? {
            Some(actual) if actual == record.value => {}
            Some(actual) => {
                mismatches += 1;
                push_sample(
                    &mut samples,
                    format!(
                        "line {} key {} value mismatch: expected {} bytes, got {} bytes",
                        record.line_number,
                        record.key,
                        record.value.len(),
                        actual.len()
                    ),
                );
            }
            None => {
                mismatches += 1;
                push_sample(
                    &mut samples,
                    format!("line {} key {} missing", record.line_number, record.key),
                );
            }
        }
    }

    Ok(VerifyReport {
        phase,
        rows_checked: rows,
        checksum,
        mismatches,
        elapsed_ms: started.elapsed().as_millis(),
        samples,
    })
}

fn records(
    config: &Config,
) -> Result<impl Iterator<Item = Result<ReplayRecord, DynError>>, DynError> {
    let file = File::open(&config.input)?;
    let reader = BufReader::new(file);
    let key_field = config.key_field.clone();
    let key_prefix = config.key_prefix.clone();

    Ok(reader.lines().enumerate().filter_map(move |(index, line)| {
        let line_number = index as u64 + 1;
        Some(parse_record(
            line_number,
            line,
            key_field.as_deref(),
            &key_prefix,
        ))
        .filter(|result| !matches!(result, Ok(None)))
        .map(|result| result.and_then(|record| record.ok_or_else(|| make_error("empty record"))))
    }))
}

fn parse_record(
    line_number: u64,
    line: std::io::Result<String>,
    key_field: Option<&str>,
    key_prefix: &str,
) -> Result<Option<ReplayRecord>, DynError> {
    let value = line?.trim_end_matches('\r').to_string();
    if value.trim().is_empty() {
        return Ok(None);
    }

    let json: Value = serde_json::from_str(&value).map_err(|error| {
        make_error(format!("line {line_number}: invalid JSONL record: {error}"))
    })?;
    let key = derive_key(&json, line_number, key_field, key_prefix)?;
    Ok(Some(ReplayRecord {
        line_number,
        key,
        value,
    }))
}

fn derive_key(
    json: &Value,
    line_number: u64,
    key_field: Option<&str>,
    key_prefix: &str,
) -> Result<String, DynError> {
    let suffix = if let Some(field) = key_field {
        let object = json
            .as_object()
            .ok_or_else(|| make_error(format!("--key-field {field} requires JSON object lines")))?;
        let value = object
            .get(field)
            .ok_or_else(|| make_error(format!("missing key field {field}")))?;
        json_value_to_key(value)?
    } else {
        format!("{line_number:020}")
    };
    Ok(format!("{key_prefix}{suffix}"))
}

fn json_value_to_key(value: &Value) -> Result<String, DynError> {
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Number(number) => Ok(number.to_string()),
        Value::Bool(flag) => Ok(flag.to_string()),
        Value::Null => Ok("null".to_string()),
        Value::Array(_) | Value::Object(_) => Ok(serde_json::to_string(value)?),
    }
}

fn update_checksum(checksum: &mut u64, key: &str, value: &str) {
    let mut hasher = Hasher::new();
    hasher.update(key.as_bytes());
    hasher.update(&[0]);
    hasher.update(value.as_bytes());
    *checksum = checksum.wrapping_add(u64::from(hasher.finalize()));
}

fn report_passed(
    after_import: &VerifyReport,
    after_reopen: &VerifyReport,
    after_compaction: Option<&VerifyReport>,
) -> bool {
    after_import.mismatches == 0
        && after_reopen.mismatches == 0
        && after_import.rows_checked == after_reopen.rows_checked
        && after_import.checksum == after_reopen.checksum
        && after_compaction.is_none_or(|report| {
            report.mismatches == 0
                && report.rows_checked == after_import.rows_checked
                && report.checksum == after_import.checksum
        })
}

fn push_sample(samples: &mut Vec<String>, sample: String) {
    if samples.len() < 10 {
        samples.push(sample);
    }
}

fn path_str(path: &Path) -> Result<&str, DynError> {
    path.to_str()
        .ok_or_else(|| make_error(format!("path is not valid UTF-8: {}", path.display())))
}

fn make_error(message: impl Into<String>) -> DynError {
    std::io::Error::other(message.into()).into()
}
