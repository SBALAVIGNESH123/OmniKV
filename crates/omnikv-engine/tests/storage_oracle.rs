//! Database-company storage correctness oracle.
//!
//! Drives the real `OmniKV` storage engine with randomized multi-step state
//! machines, exhaustive byte-offset WAL truncation, WAL bit-flip corruption,
//! and failpoint-injected crashes across L0/L1/L2 compaction, heap GC, MVCC
//! snapshots, range scans, and cold restarts.

use omni_engine::{CompactionPolicy, OmniKV, SSTableReader, WriteBatch};
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

const KEY_COUNT: u8 = 10;

#[derive(Clone, Debug)]
enum BatchItem {
    SetEmpty { key: u8 },
    SetSmall { key: u8, tag: u8 },
    SetCompressed { key: u8, tag: u8, repeat: u8 },
    Delete { key: u8 },
}

#[derive(Clone, Debug)]
enum OracleOp {
    SetEmpty { key: u8 },
    SetSmall { key: u8, tag: u8 },
    SetCompressed { key: u8, tag: u8, repeat: u8 },
    Delete { key: u8 },
    MultiBatch(Vec<BatchItem>),
    FlushL0,
    CompactL0ToL1,
    CompactL1ToL2,
    RunHeapGc,
    PinSnapshot,
    ReleaseOldestSnapshot,
    Reopen,
}

fn batch_item_strategy() -> impl Strategy<Value = BatchItem> {
    prop_oneof![
        1 => (0u8..KEY_COUNT).prop_map(|key| BatchItem::SetEmpty { key }),
        4 => (0u8..KEY_COUNT, 0u8..64).prop_map(|(key, tag)| BatchItem::SetSmall { key, tag }),
        3 => (0u8..KEY_COUNT, 0u8..64, 4u8..24)
            .prop_map(|(key, tag, repeat)| BatchItem::SetCompressed { key, tag, repeat }),
        2 => (0u8..KEY_COUNT).prop_map(|key| BatchItem::Delete { key }),
    ]
}

fn oracle_op_strategy() -> impl Strategy<Value = OracleOp> {
    prop_oneof![
        2 => (0u8..KEY_COUNT).prop_map(|key| OracleOp::SetEmpty { key }),
        5 => (0u8..KEY_COUNT, 0u8..64).prop_map(|(key, tag)| OracleOp::SetSmall { key, tag }),
        3 => (0u8..KEY_COUNT, 0u8..64, 4u8..24)
            .prop_map(|(key, tag, repeat)| OracleOp::SetCompressed { key, tag, repeat }),
        3 => (0u8..KEY_COUNT).prop_map(|key| OracleOp::Delete { key }),
        4 => prop::collection::vec(batch_item_strategy(), 2..6).prop_map(OracleOp::MultiBatch),
        2 => Just(OracleOp::FlushL0),
        2 => Just(OracleOp::CompactL0ToL1),
        1 => Just(OracleOp::CompactL1ToL2),
        1 => Just(OracleOp::RunHeapGc),
        2 => Just(OracleOp::PinSnapshot),
        1 => Just(OracleOp::ReleaseOldestSnapshot),
        1 => Just(OracleOp::Reopen),
    ]
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

fn expected_range(
    model: &BTreeMap<String, Option<String>>,
    start: &str,
    end: &str,
) -> Vec<(String, String)> {
    model
        .iter()
        .filter(|(k, _)| k.as_str() >= start && k.as_str() <= end)
        .filter_map(|(k, v)| v.as_ref().map(|val| (k.clone(), val.clone())))
        .collect()
}

fn assert_state_matches_model(
    db: &OmniKV,
    model: &BTreeMap<String, Option<String>>,
    read_seq: u64,
    context: &str,
) {
    for id in 0..KEY_COUNT {
        let k = key_name(id);
        let expected = model.get(&k).and_then(Clone::clone);
        let actual = db
            .find(&k, read_seq)
            .unwrap_or_else(|e| panic!("find({k}, {read_seq}) failed ({context}): {e:?}"));
        assert_eq!(
            actual, expected,
            "point-read mismatch for {k} at seq {read_seq} ({context})"
        );
    }

    let full_actual = db
        .scan("k00", "k99", read_seq)
        .unwrap_or_else(|e| panic!("full scan failed ({context}): {e:?}"));
    let full_expected = expected_range(model, "k00", "k99");
    assert_eq!(
        full_actual, full_expected,
        "full range scan mismatch at seq {read_seq} ({context})"
    );

    let sub_actual = db
        .scan("k02", "k07", read_seq)
        .unwrap_or_else(|e| panic!("sub-range scan failed ({context}): {e:?}"));
    let sub_expected = expected_range(model, "k02", "k07");
    assert_eq!(
        sub_actual, sub_expected,
        "sub-range scan mismatch at seq {read_seq} ({context})"
    );

    let lim_actual = db
        .scan_limit("k00", "k99", read_seq, 3)
        .unwrap_or_else(|e| panic!("limited scan failed ({context}): {e:?}"));
    let lim_expected: Vec<(String, String)> = full_expected.into_iter().take(3).collect();
    assert_eq!(
        lim_actual, lim_expected,
        "scan_limit(3) mismatch at seq {read_seq} ({context})"
    );
}

fn copy_dir_files(src: &Path, dst: &Path) {
    for entry in fs::read_dir(src).expect("read src dir") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if path.is_file() {
            let name = entry.file_name();
            fs::copy(&path, dst.join(name)).expect("copy db file");
        }
    }
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

fn normalize_model(model: &BTreeMap<String, Option<String>>) -> BTreeMap<String, Option<String>> {
    model
        .iter()
        .filter_map(|(k, v)| v.as_ref().map(|s| (k.clone(), Some(s.clone()))))
        .collect()
}

fn apply_multi_batch(
    db: &OmniKV,
    model: &mut BTreeMap<String, Option<String>>,
    items: &[BatchItem],
    step: usize,
) {
    // Keep keys distinct within one batch so the model's atomic outcome is well-defined.
    let mut seen = BTreeSet::new();
    let mut batch = WriteBatch::new();
    let mut staged = Vec::new();
    for (idx, item) in items.iter().enumerate() {
        let k_id = match item {
            BatchItem::SetEmpty { key }
            | BatchItem::SetSmall { key, .. }
            | BatchItem::SetCompressed { key, .. }
            | BatchItem::Delete { key } => *key,
        };
        if !seen.insert(k_id) {
            continue;
        }
        let k = key_name(k_id);
        match item {
            BatchItem::SetEmpty { .. } => {
                batch.set(&k, String::new()).expect("stage empty set");
                staged.push((k, Some(String::new())));
            }
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
    if batch.is_empty() {
        return;
    }
    db.commit_batch(&batch).expect("commit multi-op batch");
    for (k, v) in staged {
        model.insert(k, v);
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 32,
        max_shrink_iters: 128,
        ..ProptestConfig::default()
    })]

    #[test]
    fn storage_state_machine_oracle_survives_l0_l1_l2_heap_gc_snapshots_and_reopens(
        ops in proptest::collection::vec(oracle_op_strategy(), 1..48)
    ) {
        let dir = TempDir::new().expect("temp dir");
        let mut db = open_db(dir.path());
        let mut latest: BTreeMap<String, Option<String>> = BTreeMap::new();
        let mut snapshots: Vec<(u64, BTreeMap<String, Option<String>>)> = Vec::new();

        for (step, op) in ops.into_iter().enumerate() {
            match op {
                OracleOp::SetEmpty { key } => {
                    let k = key_name(key);
                    let mut batch = WriteBatch::new();
                    batch.set(&k, String::new()).expect("stage empty set");
                    db.commit_batch(&batch).expect("commit empty set");
                    latest.insert(k, Some(String::new()));
                }
                OracleOp::SetSmall { key, tag } => {
                    let k = key_name(key);
                    let v = small_val(tag, step);
                    let mut batch = WriteBatch::new();
                    batch.set(&k, v.clone()).expect("stage set");
                    db.commit_batch(&batch).expect("commit set");
                    latest.insert(k, Some(v));
                }
                OracleOp::SetCompressed { key, tag, repeat } => {
                    let k = key_name(key);
                    let v = compressed_val(tag, repeat, step);
                    let mut batch = WriteBatch::new();
                    batch.set(&k, v.clone()).expect("stage large set");
                    db.commit_batch(&batch).expect("commit large set");
                    latest.insert(k, Some(v));
                }
                OracleOp::Delete { key } => {
                    let k = key_name(key);
                    let mut batch = WriteBatch::new();
                    batch.delete(&k).expect("stage delete");
                    db.commit_batch(&batch).expect("commit delete");
                    latest.insert(k, None);
                }
                OracleOp::MultiBatch(items) => {
                    apply_multi_batch(&db, &mut latest, &items, step);
                }
                OracleOp::FlushL0 => {
                    db.compact_sstables().expect("flush L0");
                }
                OracleOp::CompactL0ToL1 => {
                    db.compact_sstables().expect("flush before L1");
                    db.compact_l0_to_l1().expect("compact L0->L1");
                }
                OracleOp::CompactL1ToL2 => {
                    db.compact_sstables().expect("flush before L2");
                    db.compact_l0_to_l1().expect("L0->L1 before L2");
                    db.compact_l1_to_l2().expect("compact L1->L2");
                }
                OracleOp::RunHeapGc => {
                    db.run_garbage_collection().expect("run heap GC");
                }
                OracleOp::PinSnapshot => {
                    let snap = db.snapshot();
                    snapshots.push((snap, latest.clone()));
                }
                OracleOp::ReleaseOldestSnapshot => {
                    if !snapshots.is_empty() {
                        let (snap, _) = snapshots.remove(0);
                        db.unregister_snapshot(snap);
                    }
                }
                OracleOp::Reopen => {
                    for (snap, _) in std::mem::take(&mut snapshots) {
                        db.unregister_snapshot(snap);
                    }
                    drop(db);
                    db = open_db(dir.path());
                }
            }

            assert_state_matches_model(&db, &latest, db.get_seq(), "latest");
            for (snap_seq, snap_model) in &snapshots {
                assert_state_matches_model(&db, snap_model, *snap_seq, "pinned snapshot");
            }
        }

        for (snap, _) in snapshots {
            db.unregister_snapshot(snap);
        }
        drop(db);
        let reopened = open_db(dir.path());
        assert_state_matches_model(&reopened, &latest, reopened.get_seq(), "final reopen");
    }
}

#[test]
fn wal_every_byte_truncation_recovers_exact_atomic_batch_prefix() {
    let dir = TempDir::new().expect("src temp dir");
    let db = open_db(dir.path());
    let mut model: BTreeMap<String, Option<String>> = BTreeMap::new();

    // Flush batch 1 + 2 to SSTable so recovery spans SSTables + WAL.
    let mut b1 = WriteBatch::new();
    b1.set("k00", "base_a".to_string()).expect("b1 k00");
    b1.set("k01", compressed_val(1, 16, 1)).expect("b1 k01");
    b1.set("k02", "base_c".to_string()).expect("b1 k02");
    db.commit_batch(&b1).expect("commit b1");
    model.insert("k00".into(), Some("base_a".into()));
    model.insert("k01".into(), Some(compressed_val(1, 16, 1)));
    model.insert("k02".into(), Some("base_c".into()));
    db.compact_sstables().expect("flush b1 to L0");

    let mut valid_prefixes = vec![normalize_model(&model)];

    // Subsequent atomic multi-key batches stay in WAL.
    let wal_batches: Vec<Vec<BatchItem>> = vec![
        vec![
            BatchItem::SetSmall { key: 3, tag: 10 },
            BatchItem::SetCompressed {
                key: 4,
                tag: 11,
                repeat: 12,
            },
            BatchItem::Delete { key: 2 },
        ],
        vec![
            BatchItem::SetSmall { key: 0, tag: 20 },
            BatchItem::SetSmall { key: 5, tag: 21 },
            BatchItem::Delete { key: 1 },
        ],
        vec![
            BatchItem::SetCompressed {
                key: 1,
                tag: 30,
                repeat: 10,
            },
            BatchItem::SetSmall { key: 6, tag: 31 },
            BatchItem::Delete { key: 3 },
        ],
    ];
    for (step, items) in wal_batches.iter().enumerate() {
        apply_multi_batch(&db, &mut model, items, step + 10);
        valid_prefixes.push(normalize_model(&model));
    }
    db.sync_all().expect("sync wal");
    drop(db);

    let wal_bytes = fs::read(dir.path().join("wal.bin")).expect("read wal bytes");
    assert!(
        !wal_bytes.is_empty(),
        "WAL must contain uncommitted batches"
    );

    for cut in 0..=wal_bytes.len() {
        let trial = TempDir::new().expect("trial dir");
        copy_dir_files(dir.path(), trial.path());
        fs::write(trial.path().join("wal.bin"), &wal_bytes[..cut]).expect("truncate wal");

        let recovered = open_db(trial.path());
        let actual = read_db_state(&recovered);
        assert!(
            valid_prefixes.contains(&actual),
            "WAL truncation at byte {cut}/{} exposed non-atomic state: {actual:?}",
            wal_bytes.len()
        );
    }

    // Verify writing after recovery from a mid-batch truncated WAL stays durable.
    let mid = wal_bytes.len() / 2;
    let trial = TempDir::new().expect("post-truncation write trial");
    copy_dir_files(dir.path(), trial.path());
    fs::write(trial.path().join("wal.bin"), &wal_bytes[..mid]).expect("truncate mid wal");
    let recovered = open_db(trial.path());
    let mut after = read_db_state(&recovered);
    let mut post = WriteBatch::new();
    post.set("k09", "post_recovery".to_string())
        .expect("stage post-recovery");
    recovered.commit_batch(&post).expect("commit post-recovery");
    after.insert("k09".into(), Some("post_recovery".into()));
    drop(recovered);

    let reopened = open_db(trial.path());
    assert_eq!(read_db_state(&reopened), after);
}

#[test]
fn wal_tail_bitflip_corruption_never_panics_or_exposes_half_batch() {
    let dir = TempDir::new().expect("bitflip dir");
    let db = open_db(dir.path());
    let mut model: BTreeMap<String, Option<String>> = BTreeMap::new();

    let mut valid_prefixes = vec![normalize_model(&model)];
    let batch1 = vec![
        BatchItem::SetSmall { key: 0, tag: 1 },
        BatchItem::SetCompressed {
            key: 1,
            tag: 2,
            repeat: 10,
        },
    ];
    apply_multi_batch(&db, &mut model, &batch1, 1);
    valid_prefixes.push(normalize_model(&model));

    let wal_after_b1 = fs::read(dir.path().join("wal.bin"))
        .expect("wal after b1")
        .len();

    let batch2 = vec![
        BatchItem::SetSmall { key: 2, tag: 3 },
        BatchItem::SetSmall { key: 3, tag: 4 },
        BatchItem::Delete { key: 0 },
    ];
    apply_multi_batch(&db, &mut model, &batch2, 2);
    valid_prefixes.push(normalize_model(&model));
    db.sync_all().expect("sync wal");
    drop(db);

    let wal_bytes = fs::read(dir.path().join("wal.bin")).expect("read full wal");
    assert!(wal_bytes.len() > wal_after_b1);

    // Flip a byte at every offset inside batch 2 and assert atomic recovery.
    for pos in wal_after_b1..wal_bytes.len() {
        let trial = TempDir::new().expect("bitflip trial");
        copy_dir_files(dir.path(), trial.path());
        let mut corrupted = wal_bytes.clone();
        corrupted[pos] ^= 0x5A;
        fs::write(trial.path().join("wal.bin"), &corrupted).expect("write corrupted wal");

        let recovered = open_db(trial.path());
        let actual = read_db_state(&recovered);
        assert!(
            valid_prefixes.contains(&actual),
            "bitflip at WAL offset {pos} exposed non-atomic/corrupt state: {actual:?}"
        );
    }
}

#[test]
fn wal_mid_stream_crc_corruption_stops_at_atomic_prefix_and_preserves_post_recovery_writes() {
    let dir = TempDir::new().expect("mid-stream dir");
    let db = open_db(dir.path());
    let mut model: BTreeMap<String, Option<String>> = BTreeMap::new();

    // Batch 1 (valid prefix)
    apply_multi_batch(
        &db,
        &mut model,
        &[BatchItem::SetSmall { key: 0, tag: 1 }],
        1,
    );
    let prefix_after_b1 = normalize_model(&model);
    let b1_end = fs::read(dir.path().join("wal.bin"))
        .expect("wal after b1")
        .len();

    // Batch 2 (will be CRC-corrupted in the middle of the WAL)
    apply_multi_batch(
        &db,
        &mut model,
        &[
            BatchItem::SetSmall { key: 0, tag: 2 },
            BatchItem::SetSmall { key: 1, tag: 2 },
        ],
        2,
    );
    let b2_end = fs::read(dir.path().join("wal.bin"))
        .expect("wal after b2")
        .len();

    // Batch 3 (causally after Batch 2 — must NOT replay if Batch 2 is corrupt!)
    apply_multi_batch(
        &db,
        &mut model,
        &[BatchItem::SetSmall { key: 2, tag: 3 }],
        3,
    );
    db.sync_all().expect("sync wal");
    drop(db);

    // Flip the last byte of Batch 2 (its CRC32 trailer) so Batch 2 fails CRC while
    // Batch 3 remains byte-valid behind it.
    let wal_path = dir.path().join("wal.bin");
    let mut bytes = fs::read(&wal_path).expect("read wal");
    assert!(b2_end > b1_end && bytes.len() > b2_end);
    bytes[b2_end - 1] ^= 0xFF;
    fs::write(&wal_path, &bytes).expect("write corrupted wal");

    // Reopen: must stop strictly at Batch 1 (never applying Batch 3 over corrupt Batch 2)
    // and heal/truncate the corrupted tail.
    let recovered = open_db(dir.path());
    assert_eq!(
        read_db_state(&recovered),
        prefix_after_b1,
        "replay must stop at first CRC-corrupted batch instead of applying Batch 3 out of order"
    );

    // New writes after recovery must survive a subsequent restart.
    let mut post = WriteBatch::new();
    post.set("k09", "survived_restart".to_string())
        .expect("stage post-heal write");
    recovered
        .commit_batch(&post)
        .expect("commit post-heal write");
    drop(recovered);

    let reopened = open_db(dir.path());
    let mut expected_final = prefix_after_b1;
    expected_final.insert("k09".into(), Some("survived_restart".into()));
    assert_eq!(
        read_db_state(&reopened),
        expected_final,
        "post-recovery commits must survive subsequent restart after WAL healing"
    );
}

#[cfg(feature = "failpoints")]
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

#[cfg(feature = "failpoints")]
#[test]
fn failpoint_injected_crashes_preserve_durability_and_atomicity() {
    use omni_engine::failpoints::FailRegistry;

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

#[test]
fn sstable_multiversion_hot_key_across_blocks_preserves_latest_and_snapshots() {
    let dir = TempDir::new().expect("multiversion dir");
    let db = open_db(dir.path());

    let mut snap_early = None;
    let mut snap_mid = None;
    let total_updates: usize = 180;
    for i in 0..total_updates {
        let mut batch = WriteBatch::new();
        batch
            .set("hot_key", format!("val_{i:04}"))
            .expect("stage hot_key update");
        db.commit_batch(&batch).expect("commit hot_key update");
        if i == 1 {
            snap_early = Some(db.snapshot());
        } else if i == 90 {
            snap_mid = Some(db.snapshot());
        }
    }
    let snap_early = snap_early.expect("early snapshot");
    let snap_mid = snap_mid.expect("mid snapshot");
    let latest_val = format!("val_{:04}", total_updates - 1);

    let check_all = |db: &OmniKV, stage: &str| {
        let latest_seq = db.get_seq();
        assert_eq!(
            db.find("hot_key", latest_seq).expect("find latest"),
            Some(latest_val.clone()),
            "latest find mismatch after {stage}"
        );
        assert_eq!(
            db.scan("hot_key", "hot_key", latest_seq)
                .expect("scan latest"),
            vec![("hot_key".to_string(), latest_val.clone())],
            "latest scan mismatch after {stage}"
        );
        assert_eq!(
            db.find("hot_key", snap_early).expect("find early snap"),
            Some("val_0001".to_string()),
            "early snapshot find mismatch after {stage}"
        );
        assert_eq!(
            db.scan("hot_key", "hot_key", snap_early)
                .expect("scan early snap"),
            vec![("hot_key".to_string(), "val_0001".to_string())],
            "early snapshot scan mismatch after {stage}"
        );
        assert_eq!(
            db.find("hot_key", snap_mid).expect("find mid snap"),
            Some("val_0090".to_string()),
            "mid snapshot find mismatch after {stage}"
        );
        assert!(
            db.get_seq_for_key("hot_key", latest_seq) >= snap_mid,
            "get_seq_for_key missed latest version after {stage}"
        );
    };

    check_all(&db, "memtable");
    db.compact_sstables().expect("flush L0");
    check_all(&db, "L0 flush");
    db.compact_l0_to_l1().expect("compact L0->L1");
    check_all(&db, "L0->L1");
    db.compact_l1_to_l2().expect("compact L1->L2");
    check_all(&db, "L1->L2");
    db.run_garbage_collection().expect("run heap GC");
    check_all(&db, "heap GC");

    db.unregister_snapshot(snap_early);
    db.unregister_snapshot(snap_mid);
    drop(db);

    let reopened = open_db(dir.path());
    assert_eq!(
        reopened
            .find("hot_key", reopened.get_seq())
            .expect("find after reopen"),
        Some(latest_val),
        "latest value lost after reopen"
    );
}

#[test]
fn sstable_two_version_key_straddling_block_boundary_preserves_both_versions() {
    let dir = TempDir::new().expect("straddle dir");
    let db = open_db(dir.path());
    for i in 0..200 {
        let mut batch = WriteBatch::new();
        batch
            .set(&format!("k_{i:04}"), format!("v_{i:04}_a"))
            .expect("stage first version");
        db.commit_batch(&batch).expect("commit first version");
    }
    let snap_v1 = db.snapshot();
    for i in 0..200 {
        let mut batch = WriteBatch::new();
        batch
            .set(&format!("k_{i:04}"), format!("v_{i:04}_b"))
            .expect("stage second version");
        db.commit_batch(&batch).expect("commit second version");
    }
    db.compact_sstables().expect("flush straddle L0");
    for stage in ["L0", "L1", "L2", "GC"] {
        match stage {
            "L0" => {}
            "L1" => db.compact_l0_to_l1().expect("straddle L0->L1"),
            "L2" => db.compact_l1_to_l2().expect("straddle L1->L2"),
            "GC" => db.run_garbage_collection().expect("straddle GC"),
            _ => unreachable!(),
        }
        let latest = db.get_seq();
        for i in 0..200 {
            let key = format!("k_{i:04}");
            assert_eq!(
                db.find(&key, latest).expect("find straddle latest"),
                Some(format!("v_{i:04}_b")),
                "straddle latest mismatch for {key} after {stage}"
            );
            assert_eq!(
                db.find(&key, snap_v1).expect("find straddle snap"),
                Some(format!("v_{i:04}_a")),
                "straddle snap mismatch for {key} after {stage}"
            );
            assert_eq!(
                db.scan(&key, &key, latest).expect("scan straddle latest"),
                vec![(key.clone(), format!("v_{i:04}_b"))],
                "straddle latest scan mismatch for {key} after {stage}"
            );
            assert_eq!(
                db.scan(&key, &key, snap_v1).expect("scan straddle snap"),
                vec![(key.clone(), format!("v_{i:04}_a"))],
                "straddle snap scan mismatch for {key} after {stage}"
            );
        }
    }
    db.unregister_snapshot(snap_v1);
}

fn verify_concurrent_reader_loop(
    db: &OmniKV,
    done: &std::sync::atomic::AtomicBool,
    writers: usize,
) {
    use std::sync::atomic::Ordering;

    let mut last_seen_val = vec![0usize; writers];
    let mut last_seen_seq = vec![0u64; writers];
    while !done.load(Ordering::Acquire) {
        let snap = db.snapshot();
        let full_scan = db.scan("cw_0", "cw_9", snap).expect("concurrent scan");
        let limited_scan = db
            .scan_limit("cw_0", "cw_9", snap, 2)
            .expect("concurrent scan_limit");
        let expected_limit: Vec<(String, String)> = full_scan.iter().take(2).cloned().collect();
        assert_eq!(
            limited_scan, expected_limit,
            "scan_limit(2) diverged from scan() under concurrent compaction at seq {snap}"
        );
        let scan_map: BTreeMap<String, String> = full_scan.into_iter().collect();
        for writer_id in 0..writers {
            let key = format!("cw_{writer_id}");
            let found = db.find(&key, snap).expect("concurrent find");
            assert_eq!(
                found.as_ref(),
                scan_map.get(&key),
                "find() and scan() diverged under concurrent compaction for {key} at seq {snap}"
            );
            let key_seq = db.get_seq_for_key(&key, snap);
            assert!(
                key_seq >= last_seen_seq[writer_id],
                "get_seq_for_key regressed for {key}: {key_seq} < {} at seq {snap}",
                last_seen_seq[writer_id]
            );
            last_seen_seq[writer_id] = key_seq;
            if let Some(val) = found {
                assert!(
                    key_seq > 0,
                    "get_seq_for_key returned 0 for existing key {key} at seq {snap}"
                );
                let parsed: usize = val.parse().expect("numeric value");
                assert!(
                    parsed >= last_seen_val[writer_id],
                    "value regressed for {key}: {parsed} < {} at seq {snap}",
                    last_seen_val[writer_id]
                );
                last_seen_val[writer_id] = parsed;
            }
        }
        db.unregister_snapshot(snap);
    }
}

#[test]
fn concurrent_writers_readers_and_compactions_preserve_mvcc_monotonicity_and_find_scan_equivalence()
{
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;

    const WRITERS: usize = 4;
    const UPDATES_PER_WRITER: usize = 80;

    let dir = TempDir::new().expect("concurrent oracle dir");
    let db = open_db(dir.path());
    let done = Arc::new(AtomicBool::new(false));

    let handles: Vec<_> = (0..WRITERS)
        .map(|writer_id| {
            let db = db.clone();
            thread::spawn(move || {
                let key = format!("cw_{writer_id}");
                for step in 1..=UPDATES_PER_WRITER {
                    let mut batch = WriteBatch::new();
                    batch
                        .set(&key, format!("{step:06}"))
                        .expect("stage concurrent write");
                    db.commit_batch(&batch).expect("commit concurrent write");
                }
            })
        })
        .collect();

    let compactor = {
        let db = db.clone();
        let done = done.clone();
        thread::spawn(move || {
            let mut round = 0usize;
            while !done.load(Ordering::Acquire) {
                db.compact_sstables().expect("concurrent L0 flush");
                if round.is_multiple_of(3) {
                    db.compact_l0_to_l1().expect("concurrent L0->L1");
                }
                if round.is_multiple_of(6) {
                    db.compact_l1_to_l2().expect("concurrent L1->L2");
                }
                if round.is_multiple_of(10) {
                    db.run_garbage_collection().expect("concurrent GC");
                }
                round += 1;
            }
        })
    };

    let readers: Vec<_> = (0..2)
        .map(|_| {
            let db = db.clone();
            let done = done.clone();
            thread::spawn(move || verify_concurrent_reader_loop(&db, &done, WRITERS))
        })
        .collect();

    for h in handles {
        h.join().expect("writer thread");
    }
    done.store(true, Ordering::Release);
    compactor.join().expect("compactor thread");
    for r in readers {
        r.join().expect("reader thread");
    }

    let final_seq = db.get_seq();
    for writer_id in 0..WRITERS {
        let key = format!("cw_{writer_id}");
        assert_eq!(
            db.find(&key, final_seq).expect("final concurrent find"),
            Some(format!("{UPDATES_PER_WRITER:06}"))
        );
    }
    drop(db);

    let reopened = open_db(dir.path());
    let reopen_seq = reopened.get_seq();
    for writer_id in 0..WRITERS {
        let key = format!("cw_{writer_id}");
        assert_eq!(
            reopened
                .find(&key, reopen_seq)
                .expect("reopen concurrent find"),
            Some(format!("{UPDATES_PER_WRITER:06}"))
        );
    }
}

#[test]
fn empty_string_values_do_not_poison_neighboring_keys_in_block_cache() {
    // Test both read orders (`empty -> neighbor` and `neighbor -> empty`) across
    // memtable, L0, L1, L2, heap GC, and cold restart.
    for read_empty_first in [true, false] {
        let dir = TempDir::new().expect("empty value dir");
        let db = open_db(dir.path());

        let mut b = WriteBatch::new();
        b.set("k_empty_a", String::new()).expect("stage empty a");
        b.set("k_neighbor", "real_payload".to_string())
            .expect("stage neighbor");
        b.set("k_empty_b", String::new()).expect("stage empty b");
        b.set("k_large", compressed_val(7, 16, 1))
            .expect("stage large");
        db.commit_batch(&b).expect("commit empty + neighbor batch");

        let assert_stage = |db: &OmniKV, stage: &str| {
            let seq = db.get_seq();
            if read_empty_first {
                assert_eq!(
                    db.find("k_empty_a", seq).expect("find empty a"),
                    Some(String::new()),
                    "empty a mismatch ({stage}, empty_first={read_empty_first})"
                );
                assert_eq!(
                    db.find("k_neighbor", seq).expect("find neighbor"),
                    Some("real_payload".to_string()),
                    "neighbor poisoned by empty a ({stage}, empty_first={read_empty_first})"
                );
                assert_eq!(
                    db.find("k_empty_b", seq).expect("find empty b"),
                    Some(String::new()),
                    "empty b mismatch ({stage}, empty_first={read_empty_first})"
                );
                assert_eq!(
                    db.find("k_large", seq).expect("find large"),
                    Some(compressed_val(7, 16, 1)),
                    "large value poisoned by empty b ({stage}, empty_first={read_empty_first})"
                );
            } else {
                assert_eq!(
                    db.find("k_neighbor", seq).expect("find neighbor"),
                    Some("real_payload".to_string()),
                    "neighbor mismatch ({stage}, empty_first={read_empty_first})"
                );
                assert_eq!(
                    db.find("k_empty_a", seq).expect("find empty a"),
                    Some(String::new()),
                    "empty a poisoned by neighbor ({stage}, empty_first={read_empty_first})"
                );
                assert_eq!(
                    db.find("k_large", seq).expect("find large"),
                    Some(compressed_val(7, 16, 1)),
                    "large mismatch ({stage}, empty_first={read_empty_first})"
                );
                assert_eq!(
                    db.find("k_empty_b", seq).expect("find empty b"),
                    Some(String::new()),
                    "empty b poisoned by large ({stage}, empty_first={read_empty_first})"
                );
            }
            assert_eq!(
                db.scan("k_empty_a", "k_neighbor", seq)
                    .expect("scan empty + neighbor"),
                vec![
                    ("k_empty_a".to_string(), String::new()),
                    ("k_empty_b".to_string(), String::new()),
                    ("k_large".to_string(), compressed_val(7, 16, 1)),
                    ("k_neighbor".to_string(), "real_payload".to_string()),
                ],
                "scan mismatch ({stage}, empty_first={read_empty_first})"
            );
        };

        assert_stage(&db, "memtable");
        db.compact_sstables().expect("L0 flush");
        assert_stage(&db, "L0");
        db.compact_l0_to_l1().expect("L0->L1");
        assert_stage(&db, "L1");
        db.compact_l1_to_l2().expect("L1->L2");
        assert_stage(&db, "L2");
        db.run_garbage_collection().expect("GC");
        assert_stage(&db, "GC");
        drop(db);

        let reopened = open_db(dir.path());
        assert_stage(&reopened, "reopen");
    }
}

#[test]
fn corrupt_sstable_trailer_index_offset_in_trailer_window_never_panics() {
    // Construct corrupt OMNIV2** blobs whose `index_offset` lies in
    // `(len - 16)..len` (previously panicked on `&data[index_offset..len - 16]`)
    // or at `u64::MAX`.
    for bad_offset in [48u64, 56, 63, 64, u64::MAX] {
        let mut blob = vec![0u8; 64];
        blob[48..56].copy_from_slice(&bad_offset.to_le_bytes());
        blob[56..64].copy_from_slice(b"OMNIV2**");
        let reader = SSTableReader::new(&blob);
        assert!(reader.find(b"any_key", u64::MAX).is_none());
        assert_eq!(reader.iter_from(b"any_key").count(), 0);
    }
}

#[test]
fn expired_ttl_keys_stay_invisible_and_do_not_resurrect_older_versions() {
    let dir = TempDir::new().expect("ttl dir");
    let db = open_db(dir.path());

    // Write live values first and flush to L0 so older versions sit in SSTables.
    let mut b1 = WriteBatch::new();
    b1.set("ttl_overwrite", "old_live".to_string())
        .expect("stage old live");
    b1.set_with_expiry("ttl_future", "still_alive".to_string(), u64::MAX / 2)
        .expect("stage future ttl");
    db.commit_batch(&b1).expect("commit initial batch");
    db.compact_sstables().expect("flush initial L0");

    // Overwrite `ttl_overwrite` with an already-expired timestamp (`expiry = 1`),
    // and insert a brand-new already-expired key.
    let mut b2 = WriteBatch::new();
    b2.set_with_expiry("ttl_overwrite", "expired_new".to_string(), 1)
        .expect("stage expired overwrite");
    b2.set_with_expiry("ttl_only_expired", "gone".to_string(), 1)
        .expect("stage expired key");
    db.commit_batch(&b2).expect("commit expired batch");

    let assert_ttl = |db: &OmniKV, stage: &str| {
        let seq = db.get_seq();
        assert_eq!(
            db.find("ttl_overwrite", seq).expect("find overwritten ttl"),
            None,
            "expired overwrite must not resurrect old live value ({stage})"
        );
        assert_eq!(
            db.find("ttl_only_expired", seq).expect("find expired key"),
            None,
            "expired key must be invisible ({stage})"
        );
        assert_eq!(
            db.find("ttl_future", seq).expect("find future ttl"),
            Some("still_alive".to_string()),
            "future ttl key must remain visible ({stage})"
        );
        assert_eq!(
            db.scan("ttl_", "ttl_~", seq).expect("scan ttl range"),
            vec![("ttl_future".to_string(), "still_alive".to_string())],
            "scan must match find for TTL keys ({stage})"
        );
        assert_eq!(
            db.scan_limit("ttl_", "ttl_~", seq, 5)
                .expect("scan_limit ttl range"),
            vec![("ttl_future".to_string(), "still_alive".to_string())],
            "scan_limit must match find for TTL keys ({stage})"
        );
    };

    assert_ttl(&db, "memtable");
    db.compact_sstables().expect("flush L0");
    assert_ttl(&db, "L0");
    db.compact_l0_to_l1().expect("L0->L1");
    assert_ttl(&db, "L1");
    db.compact_l1_to_l2().expect("L1->L2");
    assert_ttl(&db, "L2");
    db.run_garbage_collection().expect("GC");
    assert_ttl(&db, "GC");
    drop(db);

    let reopened = open_db(dir.path());
    assert_ttl(&reopened, "reopen");
}
