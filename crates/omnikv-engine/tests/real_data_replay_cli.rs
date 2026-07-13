use serde_json::Value;
use std::process::Command;

#[test]
fn real_data_replay_imports_reopens_and_compacts_jsonl() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let input = dir.path().join("events.jsonl");
    let workdir = dir.path().join("replay-workdir");
    let report = dir.path().join("report.json");

    std::fs::write(
        &input,
        [
            r#"{"id":"evt-1","tenant":"acme","service":"api","latency_ms":42}"#,
            r#"{"id":"evt-2","tenant":"acme","service":"worker","latency_ms":330}"#,
            r#"{"id":"evt-3","tenant":"globex","service":"api","latency_ms":17}"#,
        ]
        .join("\n"),
    )
    .expect("write JSONL fixture");

    let output = Command::new(env!("CARGO_BIN_EXE_real_data_replay"))
        .arg("--input")
        .arg(&input)
        .arg("--workdir")
        .arg(&workdir)
        .arg("--report")
        .arg(&report)
        .arg("--key-field")
        .arg("id")
        .arg("--key-prefix")
        .arg("event:")
        .output()
        .expect("run real_data_replay");

    assert!(
        output.status.success(),
        "real_data_replay failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let report_json: Value =
        serde_json::from_slice(&output.stdout).expect("stdout should be JSON report");
    assert_eq!(report_json["status"], "passed");
    assert_eq!(report_json["rows_imported"], 3);
    assert_eq!(report_json["verify_after_import"]["mismatches"], 0);
    assert_eq!(report_json["verify_after_reopen"]["mismatches"], 0);
    assert_eq!(
        report_json["verify_after_compaction"]["mismatches"], 0,
        "compaction verification should pass"
    );

    let report_file = std::fs::read_to_string(&report).expect("report file should exist");
    assert!(
        report_file.contains("\"status\": \"passed\""),
        "report file should contain passed status"
    );
}
