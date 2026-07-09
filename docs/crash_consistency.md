# OmniKV Crash Consistency & Failure Injection

## Overview

This document describes OmniKV's crash-consistency guarantees, the
deterministic failure-injection harness, and how to run and extend the
crash-consistency test suite.

## Guarantees

| Scenario | Guarantee |
|---|---|
| Clean shutdown | All committed data is readable after restart |
| Mid-write process death | Only committed entries are replayed; partial writes are discarded |
| WAL tail corruption | Detected on open; prior committed data is recovered |
| Manifest truncation | Rejected; engine falls back to last valid manifest |
| SSTable corruption | Detected via CRC32 checksum; block is rejected with explicit error |
| Compaction crash | Original SSTs remain intact; incomplete temp files are discarded |
| Backup/restore | Restored state matches exact backup point; no post-backup writes |
| Path traversal in restore | Rejected with explicit error |

## Failure Injection Harness

The harness lives in `src/failpoints.rs`. It uses a process-global registry
gated by the `failpoints` Cargo feature, so there is zero overhead in release
builds.

### Usage

```rust
use omnikv::failpoints::{arm, disarm_all, FailureMode};

// Arm a point to panic on the 3rd call:
arm("wal::sync", FailureMode::OnNthCall {
    n: 3,
    mode: Box::new(FailureMode::Panic),
});

// Run the code under test
// ...

// Always clean up
disarm_all();
```

### Adding a Failure Point to Production Code

```rust
// In production code:
use crate::failpoints::maybe_fail;

pub fn sync(&mut self) -> io::Result<()> {
    maybe_fail("wal::sync")?;   // no-op unless armed
    self.writer.flush()?;
    self.writer.get_ref().sync_data()
}
```

## Running the Tests

```bash
# All crash-consistency tests (single-threaded for determinism):
cargo test --test crash_consistency -- --test-threads=1 --nocapture

# With failure-injection feature enabled:
cargo test --test crash_consistency --features failpoints -- --test-threads=1 --nocapture

# Full storage suite:
cargo test --test durability_evidence -- --test-threads=1 --nocapture
cargo test --test backup_restore -- --test-threads=1 --nocapture
cargo test --test crash_consistency -- --test-threads=1 --nocapture
```

## Test Inventory

| Test | What it proves |
|---|---|
| `test_committed_data_survives_clean_shutdown` | Baseline durability |
| `test_wal_tail_corruption_detected_on_restart` | Corruption detection + prior commit recovery |
| `test_manifest_truncation_handled_safely` | Manifest partial-write safety |
| `test_uncommitted_data_not_visible_after_crash` | Uncommitted isolation |
| `test_100_crash_restart_cycles_no_data_loss` | Repeated crash/restart stability |
| `test_sstable_corruption_detected` | CRC32 checksum validation |
| `test_compaction_interruption_no_data_loss` | Compaction atomicity |
| `test_backup_restore_consistency` | Snapshot point-in-time correctness |
| `test_restore_rejects_path_traversal` | Restore security |
| `test_failure_point_harness_disarmed_is_noop` | Harness contract |

## Acceptance Criteria (Issue #10)

- [x] Failure-injection harness exists and is documented (`src/failpoints.rs`)
- [x] WAL tail corruption recovery is tested
- [x] Manifest corruption/truncation behaviour is tested
- [x] SSTable corruption detection is tested
- [x] Compaction interruption cannot lose acknowledged writes
- [x] Tests run in CI (`OmniKV CI / Storage Tests` job)
- [x] Root cause documented in source files
