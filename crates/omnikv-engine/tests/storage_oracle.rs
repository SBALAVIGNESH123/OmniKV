//! Database-company storage correctness oracle.
//!
//! Drives the real `OmniKV` storage engine with randomized multi-step state
//! machines, exhaustive byte-offset WAL truncation, WAL bit-flip corruption,
//! and failpoint-injected crashes across L0/L1/L2 compaction, heap GC, MVCC
//! snapshots, range scans, and cold restarts.

use omni_engine::{
    CompactionPolicy, OmniError, OmniKV, OmniRecord, SSTableReader, SSTableWriter, WriteBatch,
};
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

    let mut boundaries: Vec<(usize, BTreeMap<String, Option<String>>)> =
        vec![(0, normalize_model(&model))];

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
        let wal_end = fs::read(dir.path().join("wal.bin"))
            .expect("read wal after batch")
            .len();
        boundaries.push((wal_end, normalize_model(&model)));
    }
    db.sync_all().expect("sync wal");
    drop(db);

    let wal_bytes = fs::read(dir.path().join("wal.bin")).expect("read wal bytes");
    assert!(
        !wal_bytes.is_empty(),
        "WAL must contain uncommitted batches"
    );
    assert_eq!(
        boundaries.last().map(|(end, _)| *end),
        Some(wal_bytes.len()),
        "final batch boundary must match full WAL length"
    );

    for cut in 0..=wal_bytes.len() {
        let trial = TempDir::new().expect("trial dir");
        copy_dir_files(dir.path(), trial.path());
        fs::write(trial.path().join("wal.bin"), &wal_bytes[..cut]).expect("truncate wal");

        let recovered = open_db(trial.path());
        let actual = read_db_state(&recovered);
        let (_, expected) = boundaries
            .iter()
            .rfind(|(end, _)| *end <= cut)
            .expect("boundary at offset 0 always exists");
        assert_eq!(
            &actual,
            expected,
            "WAL truncation at byte {cut}/{} diverged from exact atomic prefix",
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

    let batch1 = vec![
        BatchItem::SetSmall { key: 0, tag: 1 },
        BatchItem::SetCompressed {
            key: 1,
            tag: 2,
            repeat: 10,
        },
    ];
    apply_multi_batch(&db, &mut model, &batch1, 1);
    let expected_after_b1 = normalize_model(&model);

    let wal_after_b1 = fs::read(dir.path().join("wal.bin"))
        .expect("wal after b1")
        .len();

    let batch2 = vec![
        BatchItem::SetSmall { key: 2, tag: 3 },
        BatchItem::SetSmall { key: 3, tag: 4 },
        BatchItem::Delete { key: 0 },
    ];
    apply_multi_batch(&db, &mut model, &batch2, 2);
    db.sync_all().expect("sync wal");
    drop(db);

    let wal_bytes = fs::read(dir.path().join("wal.bin")).expect("read full wal");
    assert!(wal_bytes.len() > wal_after_b1);

    // Flip a byte at every offset inside batch 2: batch 1 must survive intact
    // and corrupted batch 2 must never replay (including CRC trailer flips).
    for pos in wal_after_b1..wal_bytes.len() {
        let trial = TempDir::new().expect("bitflip trial");
        copy_dir_files(dir.path(), trial.path());
        let mut corrupted = wal_bytes.clone();
        corrupted[pos] ^= 0x5A;
        fs::write(trial.path().join("wal.bin"), &corrupted).expect("write corrupted wal");

        let recovered = open_db(trial.path());
        let actual = read_db_state(&recovered);
        assert_eq!(
            actual, expected_after_b1,
            "bitflip at WAL offset {pos} must preserve batch 1 and reject batch 2"
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

#[test]
fn sstable_multiversion_hot_key_across_blocks_preserves_latest_and_snapshots() {
    let dir = TempDir::new().expect("multiversion dir");
    let db = open_db(dir.path());

    let mut snap_early = None;
    let mut early_key_seq = None;
    let mut snap_mid = None;
    let mut mid_key_seq = None;
    let mut latest_key_seq = None;
    let total_updates: usize = 180;
    for i in 0..total_updates {
        let mut batch = WriteBatch::new();
        batch
            .set("hot_key", format!("val_{i:04}"))
            .expect("stage hot_key update");
        let commit_marker_seq = db.commit_batch(&batch).expect("commit hot_key update");
        // Single-op batch reserves [value_seq, commit_marker_seq].
        let value_seq = commit_marker_seq - 1;
        if i == 1 {
            snap_early = Some(db.snapshot());
            early_key_seq = Some(value_seq);
        } else if i == 90 {
            snap_mid = Some(db.snapshot());
            mid_key_seq = Some(value_seq);
        }
        if i + 1 == total_updates {
            latest_key_seq = Some(value_seq);
        }
    }
    let snap_early = snap_early.expect("early snapshot");
    let early_key_seq = early_key_seq.expect("early key seq");
    let snap_mid = snap_mid.expect("mid snapshot");
    let mid_key_seq = mid_key_seq.expect("mid key seq");
    let latest_key_seq = latest_key_seq.expect("latest key seq");
    let latest_val = format!("val_{:04}", total_updates - 1);

    let check_all = |db: &OmniKV, stage: &str| {
        let checks = [
            ("latest", db.get_seq(), latest_val.as_str(), latest_key_seq),
            ("mid snap", snap_mid, "val_0090", mid_key_seq),
            ("early snap", snap_early, "val_0001", early_key_seq),
        ];
        for (label, read_seq, expected_val, expected_key_seq) in checks {
            assert_eq!(
                db.find("hot_key", read_seq).expect("find hot_key"),
                Some(expected_val.to_string()),
                "{label} find mismatch after {stage}"
            );
            assert_eq!(
                db.scan("hot_key", "hot_key", read_seq)
                    .expect("scan hot_key"),
                vec![("hot_key".to_string(), expected_val.to_string())],
                "{label} scan mismatch after {stage}"
            );
            assert_eq!(
                db.get_seq_for_key("hot_key", read_seq),
                expected_key_seq,
                "{label} get_seq_for_key mismatch after {stage}"
            );
        }
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

fn assert_snapshot_equivalence(
    db: &OmniKV,
    snap: u64,
    writers: usize,
    last_seen_val: &mut [usize],
    last_seen_seq: &mut [u64],
) -> BTreeMap<String, String> {
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
    scan_map
}

fn verify_concurrent_reader_loop(
    db: &OmniKV,
    done: &std::sync::atomic::AtomicBool,
    compactions: &std::sync::atomic::AtomicUsize,
    reader_checks: &std::sync::atomic::AtomicUsize,
    verified_pinned: &std::sync::atomic::AtomicUsize,
    writers: usize,
) {
    use std::sync::atomic::Ordering;

    let mut last_seen_val = vec![0usize; writers];
    let mut last_seen_seq = vec![0u64; writers];
    let mut checked_across_compaction = false;
    while !done.load(Ordering::Acquire) {
        let snap = db.snapshot();
        let first_view =
            assert_snapshot_equivalence(db, snap, writers, &mut last_seen_val, &mut last_seen_seq);
        let compactions_after_first = compactions.load(Ordering::Acquire);

        if !checked_across_compaction {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while compactions.load(Ordering::Acquire) == compactions_after_first
                && !done.load(Ordering::Acquire)
                && std::time::Instant::now() < deadline
            {
                std::thread::yield_now();
            }
            if compactions.load(Ordering::Acquire) > compactions_after_first {
                let second_view = assert_snapshot_equivalence(
                    db,
                    snap,
                    writers,
                    &mut last_seen_val,
                    &mut last_seen_seq,
                );
                assert_eq!(
                    second_view, first_view,
                    "pinned snapshot view changed across live compaction at seq {snap}"
                );
                checked_across_compaction = true;
                verified_pinned.fetch_add(1, Ordering::Release);
            }
        }

        db.unregister_snapshot(snap);
        reader_checks.fetch_add(1, Ordering::Release);
    }
}

fn run_concurrent_compactor_loop(
    db: &OmniKV,
    done: &std::sync::atomic::AtomicBool,
    compactions: &std::sync::atomic::AtomicUsize,
) {
    use std::sync::atomic::Ordering;

    let mut round = 1usize;
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
        compactions.fetch_add(1, Ordering::Release);
    }
}

fn run_concurrent_writer_loop(
    db: &OmniKV,
    writer_id: usize,
    updates: usize,
    reader_checks: &std::sync::atomic::AtomicUsize,
    verified_pinned: &std::sync::atomic::AtomicUsize,
) {
    use std::sync::atomic::Ordering;

    let key = format!("cw_{writer_id}");
    for step in 1..=updates {
        let mut batch = WriteBatch::new();
        batch
            .set(&key, format!("{step:06}"))
            .expect("stage concurrent write");
        db.commit_batch(&batch).expect("commit concurrent write");
        if step == updates / 2 {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while (verified_pinned.load(Ordering::Acquire) < 1
                || reader_checks.load(Ordering::Acquire) < 2)
                && std::time::Instant::now() < deadline
            {
                std::thread::yield_now();
            }
        }
    }
}

#[test]
fn concurrent_writers_readers_and_compactions_preserve_mvcc_monotonicity_and_find_scan_equivalence()
{
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::thread;

    const WRITERS: usize = 4;
    const UPDATES_PER_WRITER: usize = 80;

    let dir = TempDir::new().expect("concurrent oracle dir");
    let db = open_db(dir.path());
    let done = Arc::new(AtomicBool::new(false));
    let compactions = Arc::new(AtomicUsize::new(0));
    let reader_checks = Arc::new(AtomicUsize::new(0));
    let verified_pinned = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..WRITERS)
        .map(|writer_id| {
            let db = db.clone();
            let reader_checks = reader_checks.clone();
            let verified_pinned = verified_pinned.clone();
            thread::spawn(move || {
                run_concurrent_writer_loop(
                    &db,
                    writer_id,
                    UPDATES_PER_WRITER,
                    &reader_checks,
                    &verified_pinned,
                );
            })
        })
        .collect();

    let compactor = {
        let db = db.clone();
        let done = done.clone();
        let compactions = compactions.clone();
        thread::spawn(move || run_concurrent_compactor_loop(&db, &done, &compactions))
    };
    let readers: Vec<_> = (0..2)
        .map(|_| {
            let db = db.clone();
            let done = done.clone();
            let compactions = compactions.clone();
            let reader_checks = reader_checks.clone();
            let verified_pinned = verified_pinned.clone();
            thread::spawn(move || {
                verify_concurrent_reader_loop(
                    &db,
                    &done,
                    &compactions,
                    &reader_checks,
                    &verified_pinned,
                    WRITERS,
                );
            })
        })
        .collect();

    let mut writer_results = Vec::with_capacity(handles.len());
    for h in handles {
        writer_results.push(h.join());
    }
    done.store(true, Ordering::Release);
    let compactor_result = compactor.join();
    let mut reader_results = Vec::with_capacity(readers.len());
    for r in readers {
        reader_results.push(r.join());
    }
    for res in writer_results {
        res.expect("writer thread");
    }
    compactor_result.expect("compactor thread");
    for res in reader_results {
        res.expect("reader thread");
    }
    assert!(
        verified_pinned.load(Ordering::Acquire) >= 1 && reader_checks.load(Ordering::Acquire) >= 2,
        "readers must compare pinned snapshots across compaction during writer phase"
    );

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
    // Construct corrupt OMNIV2** blobs containing a genuinely decodable record
    // followed by a trailer whose `index_offset` lies in `(len - 23)..=len` or
    // `u64::MAX`. Malformed trailer offsets must fail closed (returning 0 data
    // bytes) rather than decoding across index/trailer bytes.
    let valid_rec = OmniRecord::new(1, b"any_key".to_vec(), 0, 8, 42, 0).encode();
    let blob_len = 80usize;
    assert!(valid_rec.len() <= blob_len - 24);
    for bad_offset in [57u64, 64, 65, 72, 79, 80, u64::MAX] {
        let mut blob = vec![0u8; blob_len];
        blob[..valid_rec.len()].copy_from_slice(&valid_rec);
        // Precondition: the prefix record itself is valid & decodable, proving
        // the `None`/`0` outcomes below come from fail-closed trailer validation.
        assert!(
            OmniRecord::decode(&blob[..valid_rec.len()]).is_some(),
            "prefix record must be genuinely decodable"
        );
        blob[blob_len - 16..blob_len - 8].copy_from_slice(&bad_offset.to_le_bytes());
        blob[blob_len - 8..blob_len].copy_from_slice(b"OMNIV2**");
        let reader = SSTableReader::new(&blob);
        assert!(reader.find(b"any_key", u64::MAX).is_none());
        assert_eq!(reader.iter_from(b"any_key").count(), 0);
        assert_eq!(reader.iter_from(b"").count(), 0);
    }
}

#[test]
fn sstable_decoder_never_reads_past_data_end_into_sparse_index() {
    let dir = TempDir::new().expect("data_end dir");
    let sst_path = dir.path().join("boundary.sst");
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .read(true)
        .open(&sst_path)
        .expect("create boundary sst");
    let mut writer = SSTableWriter::new(&file);
    let rec1 = OmniRecord::new(1, b"k_a".to_vec(), 0, 4, 11, 0);
    let rec2 = OmniRecord::new(2, b"k_b".to_vec(), 4, 4, 22, 0);
    writer.append(&rec1).expect("append rec1");
    writer.append(&rec2).expect("append rec2");
    writer.finish().expect("finish boundary sst");
    drop(file);

    let mut bytes = fs::read(&sst_path).expect("read boundary sst");
    let index_offset = usize::try_from(u64::from_le_bytes(
        bytes[bytes.len() - 16..bytes.len() - 8]
            .try_into()
            .expect("trailer offset"),
    ))
    .expect("index offset fits in usize");
    let rec1_len = rec1.encode().len();
    assert!(rec1_len < index_offset);
    // In `OmniRecord::encode`, `seq` is bytes 0..8, `key_len: u16` is bytes 8..10,
    // and total encoded length is `42 + key_len` (ending with a 4-byte CRC32).
    // Forge a valid-CRC record starting at `rec1_len` whose payload straddles
    // `index_offset` by 4 bytes into the sparse index: decoding `&data[rec1_len..]`
    // succeeds and emits sparse-index bytes as a valid record, whereas decoding
    // strictly from `&data[rec1_len..data_end]` stops cleanly at `data_end`.
    let rec2_avail = index_offset - rec1_len;
    assert!(rec2_avail >= 42);
    let oversized_key_len = u16::try_from(rec2_avail - 42 + 4).expect("key_len fits in u16");
    let straddle_end = rec1_len + 42 + usize::from(oversized_key_len);
    assert!(straddle_end <= bytes.len() - 16);
    bytes[rec1_len + 8..rec1_len + 10].copy_from_slice(&oversized_key_len.to_le_bytes());
    let forged_crc = crc32fast::hash(&bytes[rec1_len..straddle_end - 4]);
    bytes[straddle_end - 4..straddle_end].copy_from_slice(&forged_crc.to_le_bytes());

    // Anti-vacuity precondition: without the `..data_end` slice bound, `decode`
    // WOULD return a forged `is_valid()` record spanning into the sparse index.
    let (forged_rec, _) = OmniRecord::decode(&bytes[rec1_len..])
        .expect("unbounded slice must decode the straddling forged record");
    assert!(forged_rec.is_valid());

    let reader = SSTableReader::new(&bytes);
    let decoded: Vec<OmniRecord> = reader.iter_from(b"").collect();
    assert_eq!(
        decoded.len(),
        1,
        "decoder must stop at data_end without consuming sparse-index bytes"
    );
    assert_eq!(decoded[0].key, b"k_a");
    assert_eq!(reader.find(b"k_a", u64::MAX), Some((0, 4, 11, 0)));
    assert_eq!(reader.find(&forged_rec.key, u64::MAX), None);
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

fn parse_sstable_index(bytes: &[u8]) -> Vec<(Vec<u8>, usize)> {
    assert!(bytes.len() >= 24 && &bytes[bytes.len() - 8..] == b"OMNIV2**");
    let index_offset = usize::try_from(u64::from_le_bytes(
        bytes[bytes.len() - 16..bytes.len() - 8]
            .try_into()
            .expect("trailer index_offset"),
    ))
    .expect("index_offset fits in usize");
    let index_data = &bytes[index_offset..bytes.len() - 16];
    let count = usize::try_from(u64::from_le_bytes(
        index_data[0..8].try_into().expect("entry count"),
    ))
    .expect("entry count fits in usize");
    let mut entries = Vec::with_capacity(count);
    let mut curr = 8;
    for _ in 0..count {
        let key_len = usize::from(u16::from_le_bytes(
            index_data[curr..curr + 2].try_into().expect("key len"),
        ));
        curr += 2;
        let key = index_data[curr..curr + key_len].to_vec();
        curr += key_len;
        let offset = usize::try_from(u64::from_le_bytes(
            index_data[curr..curr + 8].try_into().expect("block offset"),
        ))
        .expect("block offset fits in usize");
        curr += 8;
        entries.push((key, offset));
    }
    entries
}

#[test]
fn sstable_corrupt_preceding_block_falls_back_to_indexed_target_block() {
    let dir = TempDir::new().expect("corrupt block fallback dir");
    let sst_path = dir.path().join("fallback.sst");
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .read(true)
        .open(&sst_path)
        .expect("create fallback sst");

    let mut expected_ptrs = BTreeMap::new();
    let mut writer = SSTableWriter::new(&file);
    for i in 0u32..180 {
        let key = format!("key_{i:04}").into_bytes();
        let ptr = (u64::from(i) * 16, 8, 1234 + i, 0);
        expected_ptrs.insert(key.clone(), ptr);
        let rec = OmniRecord::new(u64::from(i) + 1, key, ptr.0, ptr.1, ptr.2, ptr.3);
        writer.append(&rec).expect("append sstable rec");
    }
    writer.finish().expect("finish sstable");
    drop(file);

    let mut bytes = fs::read(&sst_path).expect("read sstable bytes");
    let index = parse_sstable_index(&bytes);
    assert!(index.len() >= 2, "fixture must span at least 2 blocks");
    // Derive the exact first nonzero indexed key so the strict `<` seek starts
    // inside Block 0 (at offset 0) and MUST encounter the corruption there.
    let (block1_key, block1_offset) = (&index[1].0, index[1].1);
    assert!(block1_offset > 0);
    bytes[0..12].fill(0xFF);
    let reader = SSTableReader::new(&bytes);

    let expected_ptr = *expected_ptrs
        .get(block1_key)
        .expect("block1 key pointer exists");
    assert_eq!(
        reader.find(block1_key, u64::MAX),
        Some(expected_ptr),
        "find() must fall back from corrupt Block 0 to Block 1 with exact pointer"
    );
    let iter_rec = reader
        .iter_from(block1_key)
        .find(|rec| rec.key == *block1_key)
        .expect("iter_from() must recover block1 key after Block 0 decode failure");
    assert_eq!(
        (
            iter_rec.offset,
            iter_rec.length,
            iter_rec.payload_crc32,
            iter_rec.expiry
        ),
        expected_ptr
    );
}

#[test]
fn sstable_corrupt_first_duplicate_key_block_advances_to_later_intact_block() {
    let dir = TempDir::new().expect("dup key fallback dir");
    let sst_path = dir.path().join("dup_fallback.sst");
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .read(true)
        .open(&sst_path)
        .expect("create dup fallback sst");

    let mut writer = SSTableWriter::new(&file);
    // Prefix key so Block 0 starts with "aaa", followed by enough versions of
    // "dup_key" to create at least two indexed blocks starting with "dup_key".
    writer
        .append(&OmniRecord::new(1, b"aaa".to_vec(), 0, 8, 1, 0))
        .expect("append prefix");
    let total_versions = 220u32;
    let mut latest_ptr = (0, 0, 0, 0);
    for seq in 2..=total_versions + 1 {
        let seq_u64 = u64::from(seq);
        let ptr = (seq_u64 * 32, 16, seq, 0);
        latest_ptr = ptr;
        writer
            .append(&OmniRecord::new(
                seq_u64,
                b"dup_key".to_vec(),
                ptr.0,
                ptr.1,
                ptr.2,
                ptr.3,
            ))
            .expect("append dup_key version");
    }
    writer.finish().expect("finish dup sstable");
    drop(file);

    let mut bytes = fs::read(&sst_path).expect("read dup sstable");
    let dup_blocks: Vec<usize> = parse_sstable_index(&bytes)
        .into_iter()
        .filter_map(|(k, off)| (k == b"dup_key").then_some(off))
        .collect();
    assert!(
        dup_blocks.len() >= 2,
        "dup_key must start at least two indexed blocks, got {dup_blocks:?}"
    );
    // Corrupt the FIRST indexed block for "dup_key"; the second indexed block
    // (carrying the highest seq) remains intact and must still be readable.
    let first_dup_block = dup_blocks[0];
    bytes[first_dup_block..first_dup_block + 12].fill(0xFF);
    let reader = SSTableReader::new(&bytes);

    assert_eq!(
        reader.find(b"dup_key", u64::MAX),
        Some(latest_ptr),
        "find() must skip corrupt first dup_key block and recover latest version in later block"
    );
    assert!(
        reader
            .iter_from(b"dup_key")
            .any(|rec| rec.key == b"dup_key" && rec.seq == u64::from(total_versions) + 1),
        "iter_from() must advance past corrupt first dup_key block to intact later block"
    );
}

#[test]
fn corrupt_lz4_declared_size_above_max_value_size_returns_error_without_oom() {
    let dir = TempDir::new().expect("lz4 cap dir");
    let db = open_db(dir.path());

    // Craft a compressed payload with a valid CRC32 whose 4-byte LZ4 header
    // declares an oversized 1 GiB uncompressed length.
    let mut forged = Vec::new();
    forged.extend_from_slice(&(1024u32 * 1024 * 1024).to_le_bytes());
    forged.extend_from_slice(&[0x00, 0x01, 0x02, 0x03]);
    let crc = crc32fast::hash(&forged);
    let offset = db.append_to_heap(&forged).expect("append forged payload");
    let seq = db.get_seq() + 1;
    // Note: UNCOMPRESSED_FLAG is NOT set, so the engine treats it as LZ4.
    db.insert_replica_record(
        b"forged_lz4".to_vec(),
        seq,
        offset,
        forged.len() as u64,
        crc,
        0,
    );

    let err = db
        .find("forged_lz4", seq)
        .expect_err("oversized declared LZ4 length must fail closed");
    assert!(
        matches!(err, OmniError::IoError(ref msg) if msg.contains("exceeds maximum")),
        "expected oversized LZ4 length error, got {err:?}"
    );
}
