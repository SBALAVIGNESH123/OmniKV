//! Panic policy audit — CI gate for bare `.unwrap()` in production source.
//!
//! This test scans production `src/` files and fails if bare `.unwrap()` appears
//! outside of approved locations.  See `docs/PANIC_POLICY.md` for the full policy.

use std::fs;
use std::path::Path;

/// Source files that must not contain bare `.unwrap()` without justification.
const PRODUCTION_FILES: &[&str] = &[
    "src/raft/raft_storage.rs",
    "src/query/catalog.rs",
    "src/storage/transaction.rs",
    "src/storage/core.rs",
    "src/raft/raft_network.rs",
];

/// Patterns that are allowed (preceded by a SAFETY comment or in an approved form).
fn is_allowed_unwrap(line: &str, prev_line: &str) -> bool {
    // Strip unwrap_or* variants first, then check for bare .unwrap()
    let stripped = line
        .replace(".unwrap_or_else", "")
        .replace(".unwrap_or_default", "")
        .replace(".unwrap_or", "");
    // unwrap_or / unwrap_or_default / unwrap_or_else are fine (when no bare unwrap remains)
    if !stripped.contains(".unwrap()") && line.contains(".unwrap_or") {
        return true;
    }
    // Lines preceded by a SAFETY comment
    if prev_line.trim().starts_with("// SAFETY:") {
        return true;
    }
    // expect() with a message is the required form — not a bare unwrap
    if !line.contains(".unwrap()") {
        return true;
    }
    false
}

#[test]
fn no_bare_unwrap_in_production_sources() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let mut violations: Vec<String> = Vec::new();

    for rel_path in PRODUCTION_FILES {
        let full_path = Path::new(manifest_dir).join(rel_path);
        let content = match fs::read_to_string(&full_path) {
            Ok(c) => c,
            Err(e) => {
                // File not found is a violation — production files must exist
                violations.push(format!("Cannot read {rel_path}: {e}"));
                continue;
            }
        };

        let lines: Vec<&str> = content.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            let prev = if i > 0 { lines[i - 1] } else { "" };
            if line.contains(".unwrap()") && !is_allowed_unwrap(line, prev) {
                violations.push(format!(
                    "{}:{}: bare .unwrap() — use .expect(\"reason\") or propagate with ?\n  {}",
                    rel_path,
                    i + 1,
                    line.trim()
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "Panic policy violations found (see docs/PANIC_POLICY.md):\n\n{}",
        violations.join("\n")
    );
}

#[test]
fn lock_acquires_have_expect_messages() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let mut violations: Vec<String> = Vec::new();

    for rel_path in PRODUCTION_FILES {
        let full_path = Path::new(manifest_dir).join(rel_path);
        let Ok(content) = fs::read_to_string(&full_path) else {
            violations.push(format!(
                "{rel_path}: file missing or unreadable — cannot audit lock acquires"
            ));
            continue;
        };

        for (i, line) in content.lines().enumerate() {
            if (line.contains(".lock().unwrap()")
                || line.contains(".read().unwrap()")
                || line.contains(".write().unwrap()"))
                && !line.contains(".unwrap_or")
            {
                violations.push(format!(
                    "{}:{}: lock acquire uses .unwrap() — use .expect(\"lock poisoned: ...\")\n  {}",
                    rel_path,
                    i + 1,
                    line.trim()
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "Lock acquire policy violations (see docs/PANIC_POLICY.md):\n\n{}",
        violations.join("\n")
    );
}

#[test]
fn panic_policy_doc_exists() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let doc_path = Path::new(manifest_dir).join("../../docs/PANIC_POLICY.md");
    assert!(
        doc_path.exists(),
        "docs/PANIC_POLICY.md must exist — see issue #46"
    );
    let content = fs::read_to_string(&doc_path).unwrap();
    assert!(
        content.contains("Fatal Invariant"),
        "PANIC_POLICY.md must document Fatal Invariant category"
    );
    assert!(
        content.contains("Startup-Only"),
        "PANIC_POLICY.md must document Startup-Only category"
    );
}
