#![cfg(feature = "failpoints")]
//! Process-isolated failpoint crash oracle for `OmniKV`.
//!
//! Kept in its own integration-test binary so process-global `FailRegistry`
//! hooks cannot race concurrent tests in `storage_oracle.rs` even when run
//! without `--test-threads=1`.

use omni_engine::failpoints::FailRegistry;
use omni_engine::{CompactionPolicy, OmniKV, WriteBatch};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

const KEY_COUNT: u8 = 10;

/// Serializes failpoint tests inside this binary because `FailRegistry` is
/// process-global.
static FAILPOINT_TEST_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Debug)]
enum BatchItem {
    SetSmall { key: u8, tag: u8 },
    SetCompressed { key: u8, tag: u8, repeat: u8 },
    Delete { key: u8 },
}

fn key_name(id: u8) -> String {
    format!("k{id:02}")
}

fn small_val(tag: u8, step: usize) -> String {
    format!("s{tag:02}_{step:03}")
}

fn compressed_val(tag: u8, repeat: u8, step: usize) -> String {
    let chunk = format!("blk-{tag:02}-{step:03}-");
    chunk.repeat(usize::from(repeat).max(4))
}

fn open_db(dir: &Path) -> Arc<OmniKV> {
    let manifest = dir.join("manifest.json");
    let wal = dir.join("wal.bin");
    let db = OmniKV::open(
        manifest.to_string_lossy().as_ref(),
        wal.to_string_lossy().as_ref(),
    )
    .expect("open oracle db");
    db.set_compaction_policy(CompactionPolicy {
        l0_compaction_trigger: 4,
        l1_compaction_trigger: 4,
        l0_write_stall_threshold: 256,
        write_stall_wait_attempts: 1,
        write_stall_wait_ms: 1,
    })
    .expect("configure oracle compaction policy");
    db
}

fn normalize_model(model: &BTreeMap<String, Option<String>>) -> BTreeMap<String, Option<String>> {
    model
        .iter()
        .filter_map(|(k, v)| v.as_ref().map(|s| (k.clone(), Some(s.clone()))))
        .collect()
}

fn read_db_state(db: &OmniKV) -> BTreeMap<String, Option<String>> {
    let seq = db.get_seq();
    let mut state = BTreeMap::new();
    for id in 0..KEY_COUNT {
        let k = key_name(id);
        let val = db.find(&k, seq).expect("find recovered key");
        if val.is_some() {
            state.insert(k, val);
        }
    }
    let scanned = db.scan("k00", "k99", seq).expect("scan recovered db");
    let expected_scan: Vec<(String, String)> = state
        .iter()
        .filter_map(|(k, v)| v.as_ref().map(|s| (k.clone(), s.clone())))
        .collect();
    assert_eq!(
        scanned, expected_scan,
        "recovered find() and scan() diverged"
    );
    state
}

fn apply_multi_batch(
    db: &OmniKV,
    model: &mut BTreeMap<String, Option<String>>,
    items: &[BatchItem],
    step: usize,
) {
    let mut seen = BTreeSet::new();
    let mut batch = WriteBatch::new();
    let mut staged = Vec::new();
    for (idx, item) in items.iter().enumerate() {
        let k_id = match item {
            BatchItem::SetSmall { key, .. }
            | BatchItem::SetCompressed { key, .. }
            | BatchItem::Delete { key } => *key,
        };
        if !seen.insert(k_id) {
            continue;
        }
        let k = key_name(k_id);
        match item {
            BatchItem::SetSmall { tag, .. } => {
                let v = small_val(*tag, step * 10 + idx);
                batch.set(&k, v.clone()).expect("stage small set");
                staged.push((k, Some(v)));
            }
            BatchItem::SetCompressed { tag, repeat, .. } => {
                let v = compressed_val(*tag, *repeat, step * 10 + idx);
                batch.set(&k, v.clone()).expect("stage compressed set");
                staged.push((k, Some(v)));
            }
            BatchItem::Delete { .. } => {
                batch.delete(&k).expect("stage delete");
                staged.push((k, None));
            }
        }
    }
    db.commit_batch(&batch).expect("commit multi-op batch");
    for (k, v) in staged {
        model.insert(k, v);
    }
}

fn seed_failpoint_state(db: &OmniKV) -> BTreeMap<String, Option<String>> {
    let mut committed = BTreeMap::new();
    let seed_batches = [
        vec![
            BatchItem::SetSmall { key: 0, tag: 1 },
            BatchItem::SetCompressed {
                key: 1,
                tag: 2,
                repeat: 12,
            },
        ],
        vec![
            BatchItem::SetSmall { key: 2, tag: 3 },
            BatchItem::Delete { key: 0 },
        ],
        vec![
            BatchItem::SetCompressed {
                key: 3,
                tag: 4,
                repeat: 10,
            },
            BatchItem::SetSmall { key: 4, tag: 5 },
        ],
    ];
    for (i, items) in seed_batches.iter().enumerate() {
        apply_multi_batch(db, &mut committed, items, i + 1);
        if i == 0 {
            db.compact_sstables().expect("seed L0");
            db.compact_l0_to_l1().expect("seed L1");
        } else if i == 1 {
            db.compact_sstables().expect("seed second L0");
        }
    }
    normalize_model(&committed)
}

#[test]
fn failpoint_injected_crashes_preserve_durability_and_atomicity() {
    let _serial = FAILPOINT_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let registry = FailRegistry::new();
    registry.reset();

    let failpoints = [
        ("wal::sync", "commit"),
        ("heap::sync_data", "commit"),
        ("manifest::before_rename", "flush"),
        ("manifest::before_rename", "l1"),
        ("manifest::before_rename", "l2"),
        ("manifest::before_rename", "gc"),
        ("manifest::sync_dir_after_rename", "flush"),
        ("manifest::sync_dir_after_rename", "l1"),
        ("manifest::sync_dir_after_rename", "l2"),
        ("manifest::sync_dir_after_rename", "gc"),
    ];

    for (fp, phase) in failpoints {
        let dir = TempDir::new().expect("failpoint dir");
        let db = open_db(dir.path());
        let expected_before = seed_failpoint_state(&db);

        registry.arm(fp);
        let outcome = match phase {
            "commit" => {
                let mut failing = WriteBatch::new();
                failing
                    .set("k07", compressed_val(9, 14, 99))
                    .expect("stage fail commit");
                failing.delete("k02").expect("stage fail delete");
                db.commit_batch(&failing).map(|_| ())
            }
            "flush" => db.compact_sstables(),
            "l1" => db.compact_l0_to_l1(),
            "l2" => db.compact_l1_to_l2(),
            "gc" => db.run_garbage_collection(),
            _ => unreachable!(),
        };
        registry.reset();

        assert!(
            outcome.is_err(),
            "failpoint {fp} during {phase} must return an error"
        );
        if fp == "manifest::sync_dir_after_rename" {
            assert!(
                db.is_write_fenced(),
                "post-rename sync failure during {phase} must fence further writes"
            );
        }

        // Simulate crash + restart: committed state must be 100% intact.
        drop(db);
        let reopened = open_db(dir.path());
        assert_eq!(
            read_db_state(&reopened),
            expected_before,
            "state corrupted after failpoint {fp} during {phase}"
        );

        // Post-recovery writes must succeed cleanly and survive a subsequent restart.
        let mut post = WriteBatch::new();
        post.set("k08", format!("ok_{phase}"))
            .expect("stage post-failpoint write");
        reopened
            .commit_batch(&post)
            .expect("commit after failpoint recovery");
        drop(reopened);

        let reopened2 = open_db(dir.path());
        let mut expected_after = expected_before;
        expected_after.insert("k08".into(), Some(format!("ok_{phase}")));
        assert_eq!(
            read_db_state(&reopened2),
            expected_after,
            "post-recovery commit lost on restart after failpoint {fp} during {phase}"
        );
    }
}

fn assert_all_mutators_fenced(db: &OmniKV, context: &str) {
    assert!(
        db.is_write_fenced(),
        "{context}: engine write fence must be tripped"
    );
    let mut blocked = WriteBatch::new();
    blocked.set("k09", "blocked".into()).expect("stage blocked");
    assert!(
        db.commit_batch(&blocked).is_err(),
        "{context}: commit_batch must reject writes when fenced"
    );
    assert!(
        db.compact_sstables().is_err(),
        "{context}: compact_sstables must reject when fenced"
    );
    assert!(
        db.compact_l0_to_l1().is_err(),
        "{context}: compact_l0_to_l1 must reject when fenced"
    );
    assert!(
        db.compact_l1_to_l2().is_err(),
        "{context}: compact_l1_to_l2 must reject when fenced"
    );
    assert!(
        db.run_garbage_collection().is_err(),
        "{context}: run_garbage_collection must reject when fenced"
    );
}

#[test]
fn wal_rollback_failure_after_fsync_trips_engine_write_fence() {
    let _serial = FAILPOINT_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let registry = FailRegistry::new();
    registry.reset();

    for sync_fp in ["wal::sync", "heap::sync_data"] {
        let dir = TempDir::new().expect("rollback fence dir");
        let db = open_db(dir.path());
        let expected_before = seed_failpoint_state(&db);

        // Fail BOTH fsync and the subsequent `discard_undurable()` rollback so
        // the WAL enters `Fenced` state and must trip the engine-wide fence.
        registry.arm(sync_fp);
        registry.arm("wal::truncate_open");
        let mut failing = WriteBatch::new();
        failing
            .set("k06", "uncertain_a".into())
            .expect("stage failing write a");
        failing
            .set("k07", "uncertain_b".into())
            .expect("stage failing write b");
        let err = db
            .commit_batch(&failing)
            .expect_err("commit with failed fsync + rollback must fail");
        registry.reset();
        let _ = err;

        assert_all_mutators_fenced(&db, sync_fp);
        drop(db);

        // Reopen must recover atomically (either the exact pre-failure state
        // or the complete atomic failing batch since rollback truncation was
        // blocked on the live OS page cache) and allow normal writes again.
        let reopened = open_db(dir.path());
        let recovered = read_db_state(&reopened);
        let mut expected_with_batch = expected_before.clone();
        expected_with_batch.insert("k06".into(), Some("uncertain_a".into()));
        expected_with_batch.insert("k07".into(), Some("uncertain_b".into()));
        assert!(
            recovered == expected_before || recovered == expected_with_batch,
            "{sync_fp}: post-fence reopen must recover an exact atomic prefix; got {recovered:?}"
        );
        let mut post = WriteBatch::new();
        post.set("k08", "recovered_ok".into())
            .expect("stage post write");
        reopened
            .commit_batch(&post)
            .expect("post-reopen commit succeeds");
    }
}

#[test]
fn fsync_failure_with_successful_wal_rollback_fences_engine_and_stops_phantom_seq_advances() {
    let _serial = FAILPOINT_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let registry = FailRegistry::new();
    registry.reset();

    let dir = TempDir::new().expect("fsync fence dir");
    let db = open_db(dir.path());
    let expected_before = seed_failpoint_state(&db);

    // Fail `wal::sync` while letting `wal.discard_undurable()` SUCCEED.
    // Because `group_commit` is now permanently poisoned until reopen, the
    // engine write fence must ALSO be tripped immediately so subsequent
    // commits cannot advance `global_seq`, write to heap, or pollute SSI.
    registry.arm("wal::sync");
    let mut failing = WriteBatch::new();
    failing
        .set("k07", "discarded_write".into())
        .expect("stage discarded write");
    assert!(db.commit_batch(&failing).is_err());
    registry.reset();

    let seq_after_failure = db.get_seq();
    assert_all_mutators_fenced(&db, "wal::sync with successful discard_undurable");
    assert_eq!(
        db.get_seq(),
        seq_after_failure,
        "fenced commits must reject before Phase 2 without advancing global_seq"
    );
    drop(db);

    let reopened = open_db(dir.path());
    assert_eq!(read_db_state(&reopened), expected_before);
}

#[test]
fn direct_wal_append_batch_rolls_back_when_sync_fails() {
    use omni_engine::OmniRecord;
    use omni_engine::wal::WriteAheadLog;

    let _serial = FAILPOINT_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let registry = FailRegistry::new();
    registry.reset();

    let dir = TempDir::new().expect("direct wal dir");
    let wal_path = dir.path().join("direct.wal");
    let wal_str = wal_path.to_string_lossy().to_string();
    let batch = |k: &str, seq: u64| {
        vec![
            (
                OmniRecord::new(seq, k.as_bytes().to_vec(), 0, 0, 0, 0),
                None,
            ),
            (
                OmniRecord::new(seq + 1, b"__COMMIT_MARKER__".to_vec(), 1, 0, 0, 0),
                None,
            ),
        ]
    };

    let mut wal = WriteAheadLog::new(&wal_str).expect("open wal");
    wal.append_batch(&batch("durable_1", 1))
        .expect("append durable batch");

    registry.arm("wal::sync");
    assert!(
        wal.append_batch(&batch("rejected_2", 3)).is_err(),
        "append_batch must fail when wal::sync fails"
    );
    registry.reset();

    wal.append_batch(&batch("durable_3", 5))
        .expect("subsequent append_batch after clean rollback succeeds");
    drop(wal);

    let keys: Vec<String> = WriteAheadLog::replay(&wal_str, "")
        .expect("replay wal")
        .into_iter()
        .map(|r| String::from_utf8_lossy(&r.key).to_string())
        .collect();
    assert_eq!(
        keys,
        vec!["durable_1", "durable_3"],
        "rejected batch must be rolled back instead of silently committing with durable_3"
    );
}

#[test]
fn wal_rotate_sync_failure_after_truncation_fences_wal_and_engine() {
    let _serial = FAILPOINT_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let registry = FailRegistry::new();
    registry.reset();

    for op in ["compact_sstables", "run_garbage_collection"] {
        let dir = TempDir::new().expect("rotate fence dir");
        let db = open_db(dir.path());
        let expected_before = seed_failpoint_state(&db);

        registry.arm("wal::rotate_sync");
        let res = match op {
            "compact_sstables" => db.compact_sstables(),
            "run_garbage_collection" => db.run_garbage_collection(),
            _ => unreachable!(),
        };
        registry.reset();

        assert!(
            res.is_err(),
            "{op}: WAL rotation sync failure must not be silently swallowed"
        );
        assert_all_mutators_fenced(&db, op);
        drop(db);

        let reopened = open_db(dir.path());
        assert_eq!(
            read_db_state(&reopened),
            expected_before,
            "{op}: committed state must survive post-truncation WAL rotate sync failure"
        );
        let mut post = WriteBatch::new();
        post.set("k08", format!("after_{op}"))
            .expect("stage post rotate write");
        reopened
            .commit_batch(&post)
            .expect("post-reopen commit after rotate fence succeeds");
    }
}
