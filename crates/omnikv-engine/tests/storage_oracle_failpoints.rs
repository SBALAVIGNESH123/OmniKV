#![cfg(feature = "failpoints")]
//! Process-isolated failpoint crash oracle for `OmniKV`.
//!
//! Kept in its own integration-test binary so process-global `FailRegistry`
//! hooks cannot race concurrent tests in `storage_oracle.rs` even when run
//! without `--test-threads=1`.

mod common;

use common::{
    BatchItem, apply_multi_batch, compressed_val, normalize_model, open_db, read_db_state,
};
use omni_engine::failpoints::FailRegistry;
use omni_engine::{OmniKV, WriteBatch};
use std::collections::BTreeMap;
use std::sync::Mutex;
use tempfile::TempDir;

/// Serializes failpoint tests inside this binary because `FailRegistry` is
/// process-global.
static FAILPOINT_TEST_LOCK: Mutex<()> = Mutex::new(());

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
        // Live in-memory state must remain untouched on failed commit/compaction.
        assert_eq!(
            read_db_state(&db),
            expected_before,
            "in-memory state mutated after failpoint {fp} during {phase}"
        );
        let expect_fenced = matches!(
            fp,
            "wal::sync" | "heap::sync_data" | "manifest::sync_dir_after_rename"
        );
        assert_eq!(
            db.is_write_fenced(),
            expect_fenced,
            "unexpected write-fence state for {fp} during {phase}"
        );

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
    let seq_before = db.get_seq();
    db.insert_replica_record(b"k09".to_vec(), seq_before + 10, 0, 1u64 << 63, 0, 0);
    assert_eq!(
        db.get_seq(),
        seq_before,
        "{context}: insert_replica_record must not advance global_seq when fenced"
    );
    assert_eq!(
        db.find("k09", seq_before + 20)
            .expect("find k09 on fenced db"),
        None,
        "{context}: insert_replica_record must not mutate memtable when fenced"
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
        assert!(
            err.to_string().contains(sync_fp),
            "{sync_fp}: failing commit must report injected fsync error, got: {err}"
        );

        // Before reopening, prove that `discard_undurable()` actually reached
        // the injected `wal::truncate_open` failure and recorded that rollback
        // error in the engine write fence (not merely the fsync failure).
        let mut fenced_probe = WriteBatch::new();
        fenced_probe
            .set("k09", "fenced_probe".into())
            .expect("stage fenced probe");
        let fenced_err = db
            .commit_batch(&fenced_probe)
            .expect_err("fenced engine must reject subsequent commit");
        let fenced_msg = fenced_err.to_string();
        assert!(
            fenced_msg.contains("WAL rollback after fsync failure")
                && fenced_msg.contains("wal::truncate_open"),
            "{sync_fp}: fenced mutation must report injected wal::truncate_open rollback failure, got: {fenced_msg}"
        );

        assert_all_mutators_fenced(&db, sync_fp);
        drop(db);

        // Reopen must recover atomically (either the exact pre-failure state
        // or the complete atomic failing batch since rollback truncation was
        // blocked on the live OS page cache) and allow durable writes again.
        let reopened = open_db(dir.path());
        let mut recovered = read_db_state(&reopened);
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
        recovered.insert("k08".into(), Some("recovered_ok".into()));
        drop(reopened);

        let reopened2 = open_db(dir.path());
        assert_eq!(
            read_db_state(&reopened2),
            recovered,
            "{sync_fp}: post-recovery commit must survive a second restart"
        );
    }
}

#[test]
fn fsync_failure_with_successful_wal_rollback_fences_engine_and_stops_phantom_seq_advances() {
    let _serial = FAILPOINT_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let registry = FailRegistry::new();
    registry.reset();

    for sync_fp in ["wal::sync", "heap::sync_data"] {
        let dir = TempDir::new().expect("fsync fence dir");
        let db = open_db(dir.path());
        let expected_before = seed_failpoint_state(&db);

        // Fail fsync while letting `wal.discard_undurable()` SUCCEED.
        // Because `group_commit` is permanently poisoned until reopen, the
        // engine write fence must ALSO be tripped immediately so subsequent
        // commits cannot advance `global_seq`, write to heap, or pollute SSI.
        registry.arm(sync_fp);
        let mut failing = WriteBatch::new();
        failing
            .set("k07", "discarded_write".into())
            .expect("stage discarded write");
        let err = db
            .commit_batch(&failing)
            .expect_err("commit with failed fsync must fail");
        registry.reset();
        assert!(
            err.to_string().contains(sync_fp),
            "{sync_fp}: expected fsync failpoint error, got: {err}"
        );

        let seq_after_failure = db.get_seq();
        assert_all_mutators_fenced(&db, sync_fp);
        assert_eq!(
            db.get_seq(),
            seq_after_failure,
            "{sync_fp}: fenced commits must reject before Phase 2 without advancing global_seq"
        );
        drop(db);

        let reopened = open_db(dir.path());
        assert_eq!(read_db_state(&reopened), expected_before);
    }
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

    // Failing BOTH `wal::sync` and `wal::truncate_open` on direct `append_batch`
    // must report the rollback error and fence the WAL against further writes.
    registry.arm("wal::sync");
    registry.arm("wal::truncate_open");
    let rb_err = wal
        .append_batch(&batch("fenced_4", 7))
        .expect_err("append_batch with failed sync + rollback must error");
    registry.reset();
    let rb_msg = rb_err.to_string();
    assert!(
        rb_msg.contains("wal::sync") && rb_msg.contains("wal::truncate_open"),
        "expected combined sync + rollback error, got: {rb_msg}"
    );
    assert!(wal.is_fenced(), "WAL must be fenced after failed rollback");
    assert!(
        wal.append_batch(&batch("after_fence_5", 9)).is_err(),
        "fenced WAL must reject all subsequent append_batch calls"
    );
    drop(wal);

    let keys: Vec<String> = WriteAheadLog::replay(&wal_str, "")
        .expect("replay wal")
        .into_iter()
        .map(|r| String::from_utf8_lossy(&r.key).to_string())
        .collect();
    assert!(
        keys == vec!["durable_1", "durable_3"]
            || keys == vec!["durable_1", "durable_3", "fenced_4"],
        "rejected_2 must be absent and after_fence_5 must never be written; got {keys:?}"
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
        drop(reopened);

        let reopened2 = open_db(dir.path());
        let mut expected_after = expected_before;
        expected_after.insert("k08".into(), Some(format!("after_{op}")));
        assert_eq!(
            read_db_state(&reopened2),
            expected_after,
            "{op}: post-recovery commit must survive a second restart"
        );
    }
}
