//! Crash-consistency tests — these tests drive the **real `OmniKV` engine**.
//!
//! Every test:
//!   1. Opens the engine via `OmniKV::open()`
//!   2. Writes real data through `WriteBatch` + `commit_batch()`
//!   3. Simulates a crash (drop the engine, optionally corrupt on-disk files)
//!   4. Reopens the engine and asserts recovery semantics
//!
//! Run with: `cargo test -p omnikv-engine --test crash_consistency -- --test-threads=1`

use std::fs;
use std::io::Write;

#[cfg(feature = "failpoints")]
use omni_engine::failpoints::FailRegistry;
use omni_engine::{OmniKV, WriteBatch};
use tempfile::TempDir;

static TEST_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a fresh temp directory and return (`manifest_path`, `wal_path`).
fn fresh_db_paths(dir: &TempDir) -> (String, String) {
    let base = dir.path();
    let manifest = base.join("manifest.json").to_string_lossy().to_string();
    let wal = base.join("wal.bin").to_string_lossy().to_string();
    (manifest, wal)
}

/// Open the engine, write one key-value pair, sync, then drop (clean shutdown).
fn write_and_close(manifest: &str, wal: &str, key: &str, value: &str) {
    let db = OmniKV::open(manifest, wal).expect("open must succeed");
    let mut batch = WriteBatch::new();
    batch.set(key, value.to_string()).expect("batch.set");
    db.commit_batch(&batch).expect("commit_batch");
    db.sync_all().expect("sync_all");
}

#[cfg(feature = "failpoints")]
#[test]
fn armed_wal_sync_fails_the_commit_instead_of_acknowledging_durability() {
    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);
    let db = OmniKV::open(&manifest, &wal).expect("open must succeed");
    let registry = FailRegistry::new();
    registry.reset();
    registry.arm("wal::sync");

    let mut batch = WriteBatch::new();
    batch
        .set("failpoint-key", "value".to_string())
        .expect("batch.set");
    let result = db.commit_batch(&batch);

    registry.reset();
    assert!(
        result.is_err(),
        "a WAL sync failure must be returned instead of acknowledging the commit"
    );
}

// ---------------------------------------------------------------------------
// Test 1 — clean shutdown preserves all written data
// ---------------------------------------------------------------------------
#[test]
fn test_clean_shutdown_no_data_loss() {
    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    write_and_close(&manifest, &wal, "k1", "v1");

    let db2 = OmniKV::open(&manifest, &wal).expect("reopen after clean shutdown");
    let got = db2
        .find_latest_internal("k1")
        .expect("find after clean shutdown");
    assert_eq!(
        got.as_deref(),
        Some("v1"),
        "value must survive clean shutdown"
    );
}

// ---------------------------------------------------------------------------
// Test 2 — WAL tail corruption: committed data survives
// ---------------------------------------------------------------------------
#[test]
fn test_wal_tail_corruption_committed_data_survives() {
    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    write_and_close(&manifest, &wal, "k2", "v2");

    // Append garbage bytes to WAL tail (simulate crash mid-write)
    let mut f = fs::OpenOptions::new()
        .append(true)
        .open(&wal)
        .expect("open wal for corruption");
    f.write_all(&[0xFF, 0xFE, 0xFD, 0xAA, 0xBB])
        .expect("write corruption bytes");
    f.sync_all().expect("sync corruption");
    drop(f);

    let db2 = OmniKV::open(&manifest, &wal).expect("engine must open despite WAL tail corruption");
    let got = db2
        .find_latest_internal("k2")
        .expect("find after WAL tail corruption");
    assert_eq!(
        got.as_deref(),
        Some("v2"),
        "committed data must survive WAL tail corruption"
    );
}

// ---------------------------------------------------------------------------
// Test 3 — fully corrupted WAL: engine opens without panic
// ---------------------------------------------------------------------------
#[test]
fn test_wal_fully_corrupted_opens_empty() {
    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    write_and_close(&manifest, &wal, "k3", "v3");

    fs::write(&wal, b"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX").expect("corrupt wal");

    let result = OmniKV::open(&manifest, &wal);
    match result {
        Ok(db2) => {
            let _ = db2.find_latest_internal("k3");
        }
        Err(e) => {
            eprintln!("Engine returned error on fully corrupt WAL (acceptable): {e}");
        }
    }
}

// ---------------------------------------------------------------------------
// Test 4 — manifest truncation: open returns Err, does not panic
// ---------------------------------------------------------------------------
#[test]
fn test_manifest_truncation_returns_error() {
    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    write_and_close(&manifest, &wal, "k4", "v4");

    fs::write(&manifest, b"{\"heap_path\":\"heap").expect("truncate manifest");

    let result = OmniKV::open(&manifest, &wal);
    assert!(
        result.is_err(),
        "truncated manifest must cause open() to return Err"
    );
}

// ---------------------------------------------------------------------------
// Test 5 — uncommitted write is not visible after crash
// ---------------------------------------------------------------------------
#[test]
fn test_uncommitted_write_not_visible_after_crash() {
    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    write_and_close(&manifest, &wal, "k5_committed", "committed_val");

    {
        let db = OmniKV::open(&manifest, &wal).expect("open for uncommitted write test");
        let mut batch = WriteBatch::new();
        batch
            .set("k5_uncommitted", "should_not_appear".to_string())
            .expect("batch.set");
        // Intentionally NOT calling commit_batch — drop simulates crash
        drop(batch);
        drop(db);
    }

    let db2 = OmniKV::open(&manifest, &wal).expect("reopen after uncommitted crash");
    let got = db2
        .find_latest_internal("k5_uncommitted")
        .expect("find uncommitted key");
    assert!(
        got.is_none(),
        "uncommitted write must not be visible after crash"
    );

    let committed = db2
        .find_latest_internal("k5_committed")
        .expect("find committed key");
    assert_eq!(committed.as_deref(), Some("committed_val"));
}

// ---------------------------------------------------------------------------
// Test 6 — 50 keys in one batch survive clean shutdown
// ---------------------------------------------------------------------------
#[test]
fn test_multiple_keys_survive_clean_shutdown() {
    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    {
        let db = OmniKV::open(&manifest, &wal).expect("open");
        let mut batch = WriteBatch::new();
        for i in 0..50u32 {
            batch
                .set(&format!("key_{i:03}"), format!("val_{i}"))
                .expect("batch.set");
        }
        db.commit_batch(&batch).expect("commit_batch");
        db.sync_all().expect("sync_all");
    }

    let db2 = OmniKV::open(&manifest, &wal).expect("reopen");
    for i in 0..50u32 {
        let got = db2
            .find_latest_internal(&format!("key_{i:03}"))
            .expect("find");
        assert_eq!(
            got.as_deref(),
            Some(format!("val_{i}").as_str()),
            "key_{i:03} must survive"
        );
    }
}

// ---------------------------------------------------------------------------
// Test 7 — delete is durable across restart
// ---------------------------------------------------------------------------
#[test]
fn test_delete_is_durable_across_restart() {
    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    {
        let db = OmniKV::open(&manifest, &wal).expect("open");
        let mut batch = WriteBatch::new();
        batch.set("k7", "to_be_deleted".to_string()).expect("set");
        db.commit_batch(&batch).expect("commit set");
        db.sync_all().expect("sync");

        let mut batch2 = WriteBatch::new();
        batch2.delete("k7").expect("delete");
        db.commit_batch(&batch2).expect("commit delete");
        db.sync_all().expect("sync after delete");
    }

    let db2 = OmniKV::open(&manifest, &wal).expect("reopen after delete");
    let got = db2.find_latest_internal("k7").expect("find deleted key");
    assert!(got.is_none(), "deleted key must not reappear after restart");
}

// ---------------------------------------------------------------------------
// Test 8 — 100 open/write/close/reopen cycles (durability stress)
// ---------------------------------------------------------------------------
#[test]
fn test_100_crash_recovery_cycles() {
    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    for i in 0u32..100 {
        let key = format!("cycle_key_{i:03}");
        let val = format!("cycle_val_{i}");

        {
            let db = OmniKV::open(&manifest, &wal).expect("open in cycle");
            let mut batch = WriteBatch::new();
            batch.set(&key, val.clone()).expect("set in cycle");
            db.commit_batch(&batch).expect("commit in cycle");
            db.sync_all().expect("sync in cycle");
        }

        let db2 = OmniKV::open(&manifest, &wal).expect("reopen in cycle");
        let got = db2.find_latest_internal(&key).expect("find in cycle");
        assert_eq!(
            got.as_deref(),
            Some(val.as_str()),
            "cycle {i}: value must survive open/write/close/reopen"
        );
    }
}

// ---------------------------------------------------------------------------
// Test 9 — WAL replay rebuilds memtable correctly across 3 batches
// ---------------------------------------------------------------------------
#[test]
fn test_wal_replay_multi_batch() {
    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    {
        let db = OmniKV::open(&manifest, &wal).expect("open");
        for round in 0u32..3 {
            let mut batch = WriteBatch::new();
            for i in 0u32..10 {
                batch
                    .set(&format!("batch{round}_key{i}"), format!("v{round}_{i}"))
                    .expect("set");
            }
            db.commit_batch(&batch).expect("commit batch");
        }
        db.sync_all().expect("sync");
    }

    let db2 = OmniKV::open(&manifest, &wal).expect("reopen after multi-batch");
    for round in 0u32..3 {
        for i in 0u32..10 {
            let got = db2
                .find_latest_internal(&format!("batch{round}_key{i}"))
                .expect("find");
            assert_eq!(
                got.as_deref(),
                Some(format!("v{round}_{i}").as_str()),
                "batch{round}_key{i} must survive WAL replay"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Test 10 — overwrite survives restart (latest value wins)
// ---------------------------------------------------------------------------
#[test]
fn test_overwrite_survives_restart() {
    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    write_and_close(&manifest, &wal, "k10", "original");

    {
        let db = OmniKV::open(&manifest, &wal).expect("reopen for overwrite");
        let mut batch = WriteBatch::new();
        batch
            .set("k10", "updated".to_string())
            .expect("overwrite set");
        db.commit_batch(&batch).expect("commit overwrite");
        db.sync_all().expect("sync overwrite");
    }

    let db2 = OmniKV::open(&manifest, &wal).expect("final reopen");
    let got = db2
        .find_latest_internal("k10")
        .expect("find overwritten key");
    assert_eq!(
        got.as_deref(),
        Some("updated"),
        "overwritten value must survive restart"
    );
}

// ---------------------------------------------------------------------------
// Test 11 — 2PC PREPARED key reservations survive crash/reopen before
//           TwoPhaseParticipant::new() is even instantiated
// ---------------------------------------------------------------------------
#[test]
fn test_2pc_prepared_reservations_survive_crash_reopen_before_participant_recovery() {
    use omni_engine::dist_txn::{DistWrite, TwoPhaseParticipant, Vote};
    use omni_engine::transaction::TransactionManager;

    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = TempDir::new().unwrap();
    let (manifest, wal) = fresh_db_paths(&dir);

    {
        let db = OmniKV::open(&manifest, &wal).expect("open before prepare");
        let participant = TwoPhaseParticipant::new(2, db.clone());
        let prep = participant.prepare(
            (1, 901),
            &[DistWrite {
                node_id: 2,
                key: "acct:escrow".into(),
                value: Some("1000".into()),
                ttl: 0,
            }],
        );
        assert_eq!(prep.vote, Vote::Commit);
        db.sync_all().expect("sync prepared record");
        // Drop without committing or aborting: simulates crash during 2PC in-doubt window.
    }

    // Reopen ONLY OmniKV (do NOT instantiate TwoPhaseParticipant yet).
    {
        let db = OmniKV::open(&manifest, &wal).expect("reopen after crash");

        let mut rogue = WriteBatch::new();
        rogue
            .set("acct:escrow", "clobbered_plain".into())
            .expect("stage plain write");
        let plain_err = db
            .commit_batch(&rogue)
            .expect_err("plain write to a durably PREPARED key must be rejected after reopen");
        assert!(
            plain_err.to_string().contains("SSI CONFLICT (2PC)"),
            "expected SSI CONFLICT (2PC) on plain write, got: {plain_err}"
        );

        let tm = TransactionManager::new(db.clone());
        let mut ssi = tm.begin();
        tm.set(&mut ssi, "acct:escrow", "clobbered_ssi".into())
            .expect("stage ssi write");
        let ssi_err = tm
            .commit(&mut ssi)
            .expect_err("SSI commit on a durably PREPARED key must be rejected after reopen");
        assert!(
            ssi_err.to_string().contains("SSI CONFLICT (2PC)"),
            "expected SSI CONFLICT (2PC) on SSI commit, got: {ssi_err}"
        );
        assert_eq!(
            db.find_latest_internal("acct:escrow").expect("find"),
            None,
            "rejected writes must not have mutated acct:escrow before 2PC commit"
        );

        // Now recover the participant and commit the in-doubt transaction.
        let participant = TwoPhaseParticipant::new(2, db.clone());
        participant
            .commit((1, 901))
            .expect("recovered participant must commit prepared txn");
        assert_eq!(
            db.find_latest_internal("acct:escrow").expect("find"),
            Some("1000".into())
        );

        // Once committed, reservations are released and normal writes succeed.
        let mut after = WriteBatch::new();
        after
            .set("acct:escrow", "2000".into())
            .expect("stage post-commit write");
        db.commit_batch(&after)
            .expect("plain write must succeed after 2PC commit releases reservation");
        db.sync_all().expect("sync post-commit write");
    }

    let db_final = OmniKV::open(&manifest, &wal).expect("final reopen");
    assert_eq!(
        db_final
            .find_latest_internal("acct:escrow")
            .expect("find final"),
        Some("2000".into())
    );
}

#[cfg(feature = "failpoints")]
#[test]
fn coordinator_indoubt_txn_unmarks_live_in_flight_for_recovery() {
    use omni_engine::dist_txn::{TwoPhaseCoordinator, TwoPhaseParticipant, Vote};

    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let registry = FailRegistry::new();
    registry.reset();

    let coord_dir = TempDir::new().expect("coord tempdir");
    let (coord_manifest, coord_wal) = fresh_db_paths(&coord_dir);
    let coord_db = OmniKV::open(&coord_manifest, &coord_wal).expect("open coord db");

    let part_dir = TempDir::new().expect("participant tempdir");
    let (part_manifest, part_wal) = fresh_db_paths(&part_dir);
    let part_db = OmniKV::open(&part_manifest, &part_wal).expect("open part db");

    let primary = TwoPhaseCoordinator::new(1, coord_db.clone(), 5_000);
    let peer = TwoPhaseCoordinator::new(1, coord_db.clone(), 5_000);
    let participant = TwoPhaseParticipant::new(10, part_db.clone());

    let txn = primary.begin().expect("begin");
    primary
        .add_write(txn, 10, "indoubt_k".into(), Some("v".into()), 0)
        .expect("add_write");
    primary.prepare(txn).expect("prepare");
    let writes = primary
        .get_participant_writes(txn, 10)
        .expect("participant writes");
    let vote = participant.prepare(txn, &writes);
    assert_eq!(vote.vote, Vote::Commit);

    // Fail the coordinator's COMMIT log sync so `txn` transitions to InDoubt.
    registry.arm("wal::sync");
    let vote_res = primary.receive_vote(txn, vote);
    registry.reset();
    assert!(
        vote_res.is_err(),
        "receive_vote must return Err when COMMIT WAL sync fails"
    );

    // Neither handle may skip the InDoubt transaction as still collecting votes:
    // both must attempt durable ABORT recovery (hitting the poisoned WAL error
    // instead of returning `Ok(0 completed)`).
    assert!(
        primary.recover_participants(&[&participant]).is_err(),
        "primary coordinator must attempt recovery for InDoubt txn instead of skipping it"
    );
    assert!(
        peer.recover_participants(&[&participant]).is_err(),
        "peer coordinator must attempt recovery for InDoubt txn instead of skipping it"
    );

    // Reopen the coordinator DB after the fsync failure and confirm recovery
    // completes the presumed ABORT and releases the participant's reservation.
    drop(primary);
    drop(peer);
    drop(coord_db);
    let reopened_coord_db = OmniKV::open(&coord_manifest, &coord_wal).expect("reopen coord db");
    let recovered_coord = TwoPhaseCoordinator::new(1, reopened_coord_db, 5_000);
    let report = recovered_coord
        .recover_participants(&[&participant])
        .expect("reopened coordinator must complete InDoubt recovery");
    assert_eq!(report.completed, vec![(txn, "ABORT".to_string())]);
    assert_eq!(report.pending.len(), 0);

    let mut plain = WriteBatch::new();
    plain
        .set("indoubt_k", "after_indoubt_abort".into())
        .expect("stage plain write");
    part_db
        .commit_batch(&plain)
        .expect("participant reservation must be released after InDoubt ABORT");
}

fn seed_db_with_base_heap_l0_and_l1(dir: &TempDir) -> (String, String, omni_engine::Manifest) {
    let (manifest_path, wal_path) = fresh_db_paths(dir);
    {
        let db = OmniKV::open(&manifest_path, &wal_path).expect("open db");
        let mut b1 = WriteBatch::new();
        b1.set("account:balance", "100".into()).expect("set 100");
        db.commit_batch(&b1).expect("commit b1");
        db.compact_sstables().expect("flush first L0");
        db.compact_l0_to_l1().expect("promote L0 to L1");

        let mut b2 = WriteBatch::new();
        b2.set("account:balance", "50".into()).expect("set 50");
        db.commit_batch(&b2).expect("commit b2");
        db.compact_sstables().expect("flush second L0");
        assert_eq!(db.l1_sstable_count(), 1, "expected 1 L1 SSTable");
        assert_eq!(db.sstable_count(), 1, "expected 1 L0 SSTable");
    }
    let manifest = omni_engine::Manifest::load(&manifest_path).expect("load manifest");
    (manifest_path, wal_path, manifest)
}

#[test]
fn reopen_fails_closed_when_base_heap_l0_or_l1_file_is_missing() {
    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    for (kind, select_target, expected_substr) in [
        (
            "base",
            (|m: &omni_engine::Manifest| m.base_path.clone())
                as fn(&omni_engine::Manifest) -> String,
            "missing or unreadable base SSTable",
        ),
        (
            "l0",
            (|m: &omni_engine::Manifest| m.sstables[0].clone())
                as fn(&omni_engine::Manifest) -> String,
            "missing or unreadable L0 SSTable",
        ),
        (
            "l1",
            (|m: &omni_engine::Manifest| m.l1_sstables[0].clone())
                as fn(&omni_engine::Manifest) -> String,
            "missing or unreadable L1 SSTable",
        ),
        (
            "heap",
            (|m: &omni_engine::Manifest| m.heap_path.clone())
                as fn(&omni_engine::Manifest) -> String,
            "missing or unreadable heap file",
        ),
    ] {
        let dir = TempDir::new().expect("tempdir");
        let (manifest_path, wal_path, manifest) = seed_db_with_base_heap_l0_and_l1(&dir);
        let target = select_target(&manifest);
        fs::remove_file(&target).expect("remove referenced storage file");

        let Err(err) = OmniKV::open(&manifest_path, &wal_path) else {
            panic!("reopening with missing {kind} file '{target}' must fail closed");
        };
        assert!(
            err.to_string().contains(expected_substr),
            "missing {kind} file produced unexpected error: {err}"
        );
    }
}

#[cfg(feature = "failpoints")]
async fn build_template_snapshot() -> (
    openraft::SnapshotMeta<u64, omni_engine::raft_impl::OmniNode>,
    Vec<u8>,
) {
    use omni_engine::raft_storage::OmniRaftStorage;
    use openraft::{
        CommittedLeaderId, Entry, EntryPayload, LogId, RaftSnapshotBuilder, RaftStorage,
    };

    let src_dir = TempDir::new().expect("src tempdir");
    let (src_m, src_w) = fresh_db_paths(&src_dir);
    let src_db = OmniKV::open(&src_m, &src_w).expect("open src db");
    let mut src_storage = OmniRaftStorage::new(src_db);
    src_storage
        .apply_to_state_machine(&[Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
            payload: EntryPayload::Normal("SET k snap_new".into()),
        }])
        .await
        .expect("apply src");
    let template_snap = src_storage.build_snapshot().await.expect("build snapshot");
    (template_snap.meta, template_snap.snapshot.into_inner())
}

#[cfg(feature = "failpoints")]
#[test]
fn snapshot_promotion_rollback_on_rename_and_directory_sync_failures() {
    use omni_engine::raft_storage::OmniRaftStorage;
    use openraft::RaftStorage;

    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async {
        let registry = FailRegistry::new();
        registry.reset();
        let (snap_meta, snap_bytes) = build_template_snapshot().await;

        for fp in [
            "snapshot::after_move_to_old",
            "snapshot::sync_old_dir",
            "snapshot::sync_data_dir_pre_promote",
            "snapshot::after_promote_to_live",
            "snapshot::sync_data_dir_post_promote",
        ] {
            let dst_dir = TempDir::new().expect("dst tempdir");
            let (dst_m, dst_w) = fresh_db_paths(&dst_dir);
            {
                let dst_db = OmniKV::open(&dst_m, &dst_w).expect("open dst db");
                let mut wb = WriteBatch::new();
                wb.set("k", "original".into()).expect("set original");
                dst_db.commit_batch(&wb).expect("commit original");
                let mut dst_storage = OmniRaftStorage::new(dst_db);

                registry.reset();
                registry.arm(fp);
                let err = dst_storage
                    .install_snapshot(
                        &snap_meta,
                        Box::new(std::io::Cursor::new(snap_bytes.clone())),
                    )
                    .await
                    .expect_err("armed failpoint must fail install_snapshot");
                registry.reset();
                assert!(
                    err.to_string().contains(fp),
                    "expected error to identify armed failpoint '{fp}', got: {err}"
                );
            }
            let reopened = OmniKV::open(&dst_m, &dst_w)
                .unwrap_or_else(|e| panic!("reopen after rollback at {fp} failed: {e}"));
            assert_eq!(
                reopened.find_latest_internal("k").expect("read k"),
                Some("original".into()),
                "rollback at {fp} must preserve original DB"
            );
        }
    });
}

#[cfg(feature = "failpoints")]
#[test]
fn snapshot_promotion_fails_closed_when_rollback_sync_or_marker_fails() {
    use omni_engine::raft_storage::OmniRaftStorage;
    use openraft::RaftStorage;

    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async {
        let registry = FailRegistry::new();
        registry.reset();
        let (snap_meta, snap_bytes) = build_template_snapshot().await;

        // Case 1: post-promote sync fails AND rollback directory sync fails.
        let dir1 = TempDir::new().expect("dir1");
        let (m1, w1) = fresh_db_paths(&dir1);
        {
            let db1 = OmniKV::open(&m1, &w1).expect("open db1");
            let mut wb = WriteBatch::new();
            wb.set("k", "original".into()).expect("set");
            db1.commit_batch(&wb).expect("commit");
            let mut storage1 = OmniRaftStorage::new(db1);
            registry.arm("snapshot::sync_data_dir_post_promote");
            registry.arm("snapshot::rollback_sync_data_dir");
            let err = storage1
                .install_snapshot(
                    &snap_meta,
                    Box::new(std::io::Cursor::new(snap_bytes.clone())),
                )
                .await;
            registry.reset();
            assert!(err.is_err());
        }
        assert!(
            OmniKV::open(&m1, &w1).is_err(),
            "failed rollback sync must leave .rollback_failed and fail reopen closed"
        );

        // Case 2: `.install_complete` marker persistence fails AFTER temp file creation.
        let dir2 = TempDir::new().expect("dir2");
        let (m2, w2) = fresh_db_paths(&dir2);
        {
            let db2 = OmniKV::open(&m2, &w2).expect("open db2");
            let mut wb = WriteBatch::new();
            wb.set("k", "original".into()).expect("set");
            db2.commit_batch(&wb).expect("commit");
            let mut storage2 = OmniRaftStorage::new(db2.clone());
            registry.arm("snapshot::write_install_complete_after_create");
            let err = storage2
                .install_snapshot(&snap_meta, Box::new(std::io::Cursor::new(snap_bytes)))
                .await
                .expect_err("Phase H marker write failure must surface error");
            registry.reset();
            assert!(err.to_string().contains("operator recovery required"));
            assert!(
                err.to_string()
                    .contains("snapshot::write_install_complete_after_create"),
                "expected armed marker failpoint in error: {err}"
            );
            assert_eq!(
                db2.find_latest_internal("k").expect("read live k"),
                Some("snap_new".into())
            );
        }
        assert!(
            OmniKV::open(&m2, &w2).is_err(),
            "failed marker write after temp creation must leave no .install_complete and fail reopen closed"
        );
    });
}

#[cfg(feature = "failpoints")]
fn commit_kv(db: &OmniKV, key: &str, value: &str) -> Result<u64, omni_engine::OmniError> {
    let mut wb = WriteBatch::new();
    wb.set(key, value.to_string()).expect("wb.set");
    db.commit_batch(&wb)
}

#[cfg(feature = "failpoints")]
#[test]
fn gc_post_rename_manifest_sync_failure_fences_writes_and_preserves_reopen() {
    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let registry = FailRegistry::new();
    registry.reset();

    let dir = TempDir::new().expect("tempdir");
    let (manifest_path, wal_path) = fresh_db_paths(&dir);
    let db = OmniKV::open(&manifest_path, &wal_path).expect("open db");
    commit_kv(&db, "k1", "v1_old").expect("commit k1 old");
    commit_kv(&db, "k2", "v2").expect("commit k2");
    commit_kv(&db, "k1", "v1_new").expect("commit k1 new");
    db.compact_sstables().expect("flush L0");
    commit_kv(&db, "k3", "v3_memtable").expect("commit k3");

    let pre_gc = omni_engine::Manifest::load(&manifest_path).expect("pre-GC manifest");
    registry.arm("manifest::sync_dir_after_rename");
    let gc_err = db
        .run_garbage_collection()
        .expect_err("post-rename sync failure must fail GC");
    registry.reset();
    assert!(
        gc_err
            .to_string()
            .contains("manifest::sync_dir_after_rename"),
        "expected armed failpoint in GC error: {gc_err}"
    );

    let post_gc = omni_engine::Manifest::load(&manifest_path).expect("post-GC manifest");
    assert_ne!(
        post_gc.heap_path, pre_gc.heap_path,
        "new manifest must be visible"
    );
    assert_ne!(
        post_gc.base_path, pre_gc.base_path,
        "new base must be visible"
    );
    assert!(
        std::path::Path::new(&post_gc.heap_path).exists(),
        "new heap must be preserved"
    );
    assert!(
        std::path::Path::new(&post_gc.base_path).exists(),
        "new base must be preserved"
    );

    // No write or compaction may be accepted against the superseded old heap.
    let post_write_err = commit_kv(&db, "k_rejected", "must_not_land")
        .expect_err("writes after failed GC must be fenced");
    assert!(
        post_write_err.to_string().contains("writes halted"),
        "unexpected fenced write error: {post_write_err}"
    );
    assert!(
        db.compact_sstables().is_err(),
        "compaction must stay fenced"
    );
    assert!(db.run_garbage_collection().is_err(), "GC must stay fenced");

    // Live reads remain consistent against preserved pre-GC files.
    for (k, v) in [("k1", "v1_new"), ("k2", "v2"), ("k3", "v3_memtable")] {
        assert_eq!(
            db.find_latest_internal(k).expect("live read"),
            Some(v.to_string())
        );
    }

    assert_fenced_and_reopen_recovers(
        db,
        &manifest_path,
        &wal_path,
        &[("k1", "v1_new"), ("k2", "v2"), ("k3", "v3_memtable")],
    );
    let reopened = OmniKV::open(&manifest_path, &wal_path).expect("second reopen");
    assert_eq!(
        reopened
            .find_latest_internal("k_rejected")
            .expect("k_rejected"),
        None
    );
    assert_eq!(
        reopened
            .find_latest_internal("after_reopen")
            .expect("after_reopen"),
        Some("ok".into())
    );
}

#[cfg(feature = "failpoints")]
fn assert_fenced_and_reopen_recovers(
    db: std::sync::Arc<OmniKV>,
    manifest_path: &str,
    wal_path: &str,
    expected_entries: &[(&str, &str)],
) {
    assert!(
        db.is_write_fenced(),
        "post-rename failure must fence writes"
    );
    assert!(
        commit_kv(&db, "fenced_write", "must_fail").is_err(),
        "subsequent write must be rejected while fenced"
    );
    drop(db);

    let reopened = OmniKV::open(manifest_path, wal_path).expect("reopen must succeed");
    assert!(
        !reopened.is_write_fenced(),
        "reopen must resume writable state"
    );
    for (k, v) in expected_entries {
        assert_eq!(
            reopened
                .find_latest_internal(k)
                .expect("read recovered key"),
            Some((*v).to_string()),
            "acknowledged value for '{k}' must survive reopen"
        );
    }
    assert_eq!(
        reopened
            .find_latest_internal("fenced_write")
            .expect("read fenced"),
        None
    );
    commit_kv(&reopened, "after_reopen", "ok").expect("write after reopen must succeed");
}

#[cfg(feature = "failpoints")]
#[test]
fn compaction_publishers_post_rename_sync_failure_preserve_outputs_and_reopen() {
    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let registry = FailRegistry::new();
    registry.reset();

    // 1. Memtable-to-L0 flush
    {
        let dir = TempDir::new().expect("l0 tempdir");
        let (m, w) = fresh_db_paths(&dir);
        let db = OmniKV::open(&m, &w).expect("open l0 db");
        commit_kv(&db, "m1", "vm1").expect("commit m1");
        commit_kv(&db, "m2", "vm2").expect("commit m2");
        registry.arm("manifest::sync_dir_after_rename");
        let err = db.compact_sstables().expect_err("L0 flush must fail");
        registry.reset();
        assert!(err.to_string().contains("manifest::sync_dir_after_rename"));
        let disk = omni_engine::Manifest::load(&m).expect("load L0 manifest");
        assert_eq!(
            disk.sstables.len(),
            1,
            "manifest must reference new L0 SSTable"
        );
        assert!(
            std::path::Path::new(&disk.sstables[0]).exists(),
            "L0 SSTable must not be deleted"
        );
        assert_fenced_and_reopen_recovers(db, &m, &w, &[("m1", "vm1"), ("m2", "vm2")]);
    }

    // 2. L0-to-L1 compaction
    {
        let dir = TempDir::new().expect("l1 tempdir");
        let (m, w) = fresh_db_paths(&dir);
        let db = OmniKV::open(&m, &w).expect("open l1 db");
        commit_kv(&db, "a1", "va1").expect("commit a1");
        db.compact_sstables().expect("flush L0 #1");
        commit_kv(&db, "a2", "va2").expect("commit a2");
        db.compact_sstables().expect("flush L0 #2");
        registry.arm("manifest::sync_dir_after_rename");
        let err = db.compact_l0_to_l1().expect_err("L0->L1 must fail");
        registry.reset();
        assert!(err.to_string().contains("manifest::sync_dir_after_rename"));
        let disk = omni_engine::Manifest::load(&m).expect("load L1 manifest");
        assert_eq!(
            disk.sstables.len(),
            0,
            "L0 list cleared in visible manifest"
        );
        assert_eq!(
            disk.l1_sstables.len(),
            1,
            "manifest must reference new L1 SSTable"
        );
        assert!(
            std::path::Path::new(&disk.l1_sstables[0]).exists(),
            "L1 SSTable must not be deleted"
        );
        assert_fenced_and_reopen_recovers(db, &m, &w, &[("a1", "va1"), ("a2", "va2")]);
    }

    // 3. L1-to-base compaction
    {
        let dir = TempDir::new().expect("base tempdir");
        let (m, w) = fresh_db_paths(&dir);
        let db = OmniKV::open(&m, &w).expect("open base db");
        commit_kv(&db, "b1", "vb1").expect("commit b1");
        db.compact_sstables().expect("flush L0");
        db.compact_l0_to_l1().expect("compact L0->L1");
        let old_base = omni_engine::Manifest::load(&m)
            .expect("old manifest")
            .base_path;
        registry.arm("manifest::sync_dir_after_rename");
        let err = db.compact_l1_to_l2().expect_err("L1->base must fail");
        registry.reset();
        assert!(err.to_string().contains("manifest::sync_dir_after_rename"));
        let disk = omni_engine::Manifest::load(&m).expect("load base manifest");
        assert_ne!(
            disk.base_path, old_base,
            "visible manifest must reference new base"
        );
        assert!(
            std::path::Path::new(&disk.base_path).exists(),
            "new base must not be deleted"
        );
        assert_fenced_and_reopen_recovers(db, &m, &w, &[("b1", "vb1")]);
    }
}

#[cfg(feature = "failpoints")]
#[test]
fn pre_rename_manifest_failure_cleans_outputs_without_fencing_writes() {
    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let registry = FailRegistry::new();
    registry.reset();

    let dir = TempDir::new().expect("tempdir");
    let (m, w) = fresh_db_paths(&dir);
    {
        let db = OmniKV::open(&m, &w).expect("open db");
        commit_kv(&db, "k1", "v1").expect("commit k1");
        db.compact_sstables().expect("flush L0");

        registry.arm("manifest::before_rename");
        let err = db
            .compact_l0_to_l1()
            .expect_err("pre-rename failure must fail compaction");
        registry.reset();
        assert!(err.to_string().contains("manifest::before_rename"));
        assert!(
            !db.is_write_fenced(),
            "pre-rename failure must NOT fence writes"
        );

        let disk = omni_engine::Manifest::load(&m).expect("load unchanged manifest");
        assert_eq!(
            disk.sstables.len(),
            1,
            "original L0 must remain in manifest"
        );
        assert_eq!(disk.l1_sstables.len(), 0, "no L1 must appear in manifest");
        let orphan_l1_exists = fs::read_dir(dir.path())
            .expect("read_dir")
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().contains("_l1_"));
        assert!(
            !orphan_l1_exists,
            "pre-rename output guard must remove unreferenced L1 output"
        );

        commit_kv(&db, "k2", "v2").expect("writes must remain allowed after pre-rename failure");
    }

    let reopened = OmniKV::open(&m, &w).expect("reopen after pre-rename failure");
    assert_eq!(
        reopened.find_latest_internal("k1").expect("k1"),
        Some("v1".into())
    );
    assert_eq!(
        reopened.find_latest_internal("k2").expect("k2"),
        Some("v2".into())
    );
}

#[cfg(feature = "failpoints")]
fn seed_fp_uq_table(
    db: &std::sync::Arc<OmniKV>,
    table: &str,
    count: usize,
) -> omni_engine::sql_exec::SqlExecutor {
    use omni_engine::catalog::Catalog;
    use omni_engine::sql::parse_sql;
    use omni_engine::sql_exec::SqlExecutor;
    use std::sync::Arc;

    let executor = SqlExecutor::new(db.clone(), Arc::new(Catalog::new(db.clone())));
    executor
        .execute(
            &parse_sql(&format!(
                "CREATE TABLE {table} (id TEXT PRIMARY KEY, code TEXT)"
            ))
            .unwrap(),
        )
        .expect("create table");
    let mut seed = WriteBatch::new();
    for i in 1..=count {
        let pk = format!("{i:04}");
        let row = serde_json::json!({ "id": pk, "code": format!("c{i:04}") });
        seed.set(&format!("\u{0}T\u{0}{table}\u{0}{pk}"), row.to_string())
            .expect("set");
        if seed.op_count() >= 500 {
            db.commit_batch(&seed).expect("commit seed");
            seed = WriteBatch::new();
        }
    }
    if !seed.is_empty() {
        db.commit_batch(&seed).expect("commit tail");
    }
    executor
}

#[cfg(feature = "failpoints")]
#[test]
fn partial_unique_index_rebuild_failpoint_still_enforces_uniqueness() {
    use omni_engine::secondary_index::{IndexFieldType, IndexManager};
    use omni_engine::sql::parse_sql;

    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let registry = FailRegistry::new();
    registry.reset();

    let dir = TempDir::new().expect("tempdir");
    let (m, w) = fresh_db_paths(&dir);
    let db = OmniKV::open(&m, &w).expect("open db");
    let executor = seed_fp_uq_table(&db, "fp_uq", 1005);

    let idx_mgr = IndexManager::new(db);
    let idx_id = idx_mgr
        .create_sql_index(
            "fp_uq_code",
            "fp_uq",
            vec![("code".into(), IndexFieldType::String)],
            true,
        )
        .expect("create unique index");

    registry.arm("index::rebuild_after_partial_commit");
    let rebuild_err = idx_mgr
        .rebuild_index("fp_uq_code")
        .expect_err("partial rebuild failpoint must fail rebuild");
    registry.reset();
    assert!(
        rebuild_err
            .to_string()
            .contains("index::rebuild_after_partial_commit"),
        "unexpected rebuild err: {rebuild_err}"
    );
    assert!(!idx_mgr.catalog_snapshot().expect("snap").is_ready(idx_id));
    assert!(
        idx_mgr
            .lookup("fp_uq_code", &[serde_json::json!("c1005")])
            .is_err(),
        "public lookup on unready index after partial rebuild failure must fail closed"
    );
    let Err(dup_err) = executor
        .execute(&parse_sql("INSERT INTO fp_uq (id, code) VALUES ('2000', 'c1005')").unwrap())
    else {
        panic!("duplicate insert against unindexed tail row must fail");
    };
    assert!(dup_err.contains("UNIQUE CONSTRAINT"), "err: {dup_err}");

    assert_eq!(idx_mgr.rebuild_index("fp_uq_code").expect("rebuild"), 1005);
    assert!(idx_mgr.catalog_snapshot().expect("snap").is_ready(idx_id));
}

#[cfg(feature = "failpoints")]
#[test]
fn partial_add_field_failpoint_defers_catalog_and_recovers_on_retry() {
    use omni_engine::schema::{Migration, SchemaManager, SchemaOp};
    use omni_engine::secondary_index::{IndexManager, IndexNamespace};
    use std::sync::Arc;

    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let registry = FailRegistry::new();
    registry.reset();

    let dir = TempDir::new().expect("tempdir");
    let (m, w) = fresh_db_paths(&dir);
    let db = OmniKV::open(&m, &w).expect("open db");
    let _executor = seed_fp_uq_table(&db, "fp_add", WriteBatch::MAX_OPS / 2 + 25);

    let idx_mgr = Arc::new(IndexManager::new(db.clone()));
    let schema = SchemaManager::new(db.clone(), idx_mgr);
    let mig = Migration {
        version: 1,
        description: "large add + rename".into(),
        up: vec![
            SchemaOp::AddField {
                collection: "fp_add".into(),
                field_name: "old_tag".into(),
                default_value: serde_json::json!("t0"),
            }
            .in_namespace(IndexNamespace::Sql),
            SchemaOp::RenameField {
                collection: "fp_add".into(),
                old_name: "old_tag".into(),
                new_name: "new_tag".into(),
            }
            .in_namespace(IndexNamespace::Sql),
        ],
        down: vec![],
    };

    registry.arm("schema::add_field_after_partial_batch");
    let add_err = schema
        .migrate_up(&mig)
        .expect_err("partial add_field failpoint must fail migration");
    registry.reset();
    assert!(
        add_err
            .to_string()
            .contains("schema::add_field_after_partial_batch"),
        "unexpected add err: {add_err}"
    );
    let mid_cat = db
        .find("\u{0}CATALOG\u{0}fp_add", db.get_seq())
        .expect("cat")
        .expect("table exists");
    assert!(!mid_cat.contains("old_tag"));

    schema.migrate_up(&mig).expect("retry must succeed");
    let final_cat = db
        .find("\u{0}CATALOG\u{0}fp_add", db.get_seq())
        .expect("cat")
        .expect("table exists");
    assert!(!final_cat.contains("old_tag"));
    assert!(final_cat.contains("new_tag"));
}

#[cfg(feature = "failpoints")]
fn seed_and_interrupt_large_drop(manifest: &str, wal: &str, registry: &FailRegistry) -> u32 {
    use omni_engine::catalog::Catalog;
    use omni_engine::secondary_index::{IndexFieldType, IndexManager};

    let db = OmniKV::open(manifest, wal).expect("open db");
    let _executor = seed_fp_uq_table(&db, "fp_big_drop", WriteBatch::MAX_OPS + 120);
    let idx_mgr = IndexManager::new(db.clone());
    let id = idx_mgr
        .create_sql_index(
            "fp_big_drop_code_idx",
            "fp_big_drop",
            vec![("code".into(), IndexFieldType::String)],
            false,
        )
        .expect("create index");
    idx_mgr
        .rebuild_index("fp_big_drop_code_idx")
        .expect("rebuild");
    let mut legacy_stage = WriteBatch::new();
    legacy_stage
        .set(
            "\u{0}SCHEMA\u{0}rename_stage:11:fp_big_drop:3:old:3:new",
            "1".into(),
        )
        .expect("stage key");
    db.commit_batch(&legacy_stage).expect("commit legacy stage");

    let catalog = Catalog::new(db);
    registry.arm("catalog::drop_table_after_partial_chunk");
    let drop_err = catalog
        .drop_table("fp_big_drop")
        .expect_err("armed failpoint must interrupt large drop_table mid-chunk");
    registry.reset();
    assert!(
        drop_err.contains("catalog::drop_table_after_partial_chunk"),
        "unexpected drop_err: {drop_err}"
    );
    id
}

#[cfg(feature = "failpoints")]
#[test]
fn interrupted_large_drop_table_hides_table_and_recovers_without_resurrection() {
    use omni_engine::catalog::Catalog;
    use omni_engine::secondary_index::IndexManager;
    use omni_engine::sql::parse_sql;
    use omni_engine::sql_exec::{ExecResult, SqlExecutor};
    use std::collections::HashMap;
    use std::sync::Arc;

    let _lock = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let registry = FailRegistry::new();
    registry.reset();

    let dir = TempDir::new().expect("tempdir");
    let (m, w) = fresh_db_paths(&dir);
    let idx_id = seed_and_interrupt_large_drop(&m, &w, &registry);

    let db2 = OmniKV::open(&m, &w).expect("reopen db");
    let catalog2 = Arc::new(Catalog::new(db2.clone()));
    assert!(catalog2.get_table("fp_big_drop").is_none());
    assert!(
        db2.find(&Catalog::catalog_key("fp_big_drop"), db2.get_seq())
            .expect("find catalog key")
            .is_none()
    );
    assert!(
        db2.find(&Catalog::dropping_table_key("fp_big_drop"), db2.get_seq())
            .expect("find dropping marker")
            .is_some()
    );
    assert!(
        IndexManager::new(db2.clone())
            .definition("fp_big_drop_code_idx")
            .is_err()
    );

    // A transaction opened before cleanup completes rejects CREATE TABLE (so
    // later SELECTs at its stale snapshot cannot read pre-drain rows) while
    // finishing the pending drop cleanly for subsequent statements.
    let stale_txn_exec = SqlExecutor::with_transaction(
        db2.clone(),
        catalog2.clone(),
        db2.get_seq(),
        &HashMap::new(),
    );
    let Err(txn_err) = stale_txn_exec
        .execute(&parse_sql("CREATE TABLE fp_big_drop (id TEXT PRIMARY KEY, code TEXT)").unwrap())
    else {
        panic!("stale-snapshot transactional CREATE TABLE must fail closed after draining");
    };
    assert!(
        txn_err.contains("interrupted DROP TABLE"),
        "unexpected stale-txn error: {txn_err}"
    );
    assert!(
        db2.find(&Catalog::dropping_table_key("fp_big_drop"), db2.get_seq())
            .expect("dropping marker cleared")
            .is_none()
    );
    assert!(
        db2.find(&format!("\u{0}IDX_RETIRED\u{0}{idx_id}"), db2.get_seq())
            .expect("retired index cleared")
            .is_none()
    );
    assert!(
        db2.find(
            "\u{0}SCHEMA\u{0}rename_stage:11:fp_big_drop:3:old:3:new",
            db2.get_seq()
        )
        .expect("legacy rename stage cleared")
        .is_none()
    );

    let exec2 = SqlExecutor::new(db2, catalog2.clone());
    exec2
        .execute(&parse_sql("CREATE TABLE fp_big_drop (id TEXT PRIMARY KEY, code TEXT)").unwrap())
        .expect("recreate table after cleanup");
    let ExecResult::Rows { rows, .. } = exec2
        .execute(&parse_sql("SELECT id FROM fp_big_drop").unwrap())
        .expect("select recreated table")
    else {
        panic!("expected rows");
    };
    assert_eq!(rows.len(), 0, "no old rows may resurrect after recreate");
    assert_eq!(catalog2.recover_pending_drops().expect("recover idle"), 0);
}
