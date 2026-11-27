//! Database-company storage correctness oracle.
//!
//! Drives the real `OmniKV` storage engine with randomized multi-step state
//! machines, exhaustive byte-offset WAL truncation, WAL bit-flip corruption,
//! `SSTable` block-straddle & corrupt-input invariants, and concurrent
//! writer/reader/compactor verification across L0/L1/L2 compaction, heap GC,
//! MVCC snapshots, range scans, and cold restarts.
//!
//! Process-global failpoint crash injection lives in `storage_oracle_failpoints.rs`.

mod common;

use common::{
    BatchItem, KEY_COUNT, apply_multi_batch, compressed_val, copy_isolated_db_dir, key_name,
    normalize_model, open_db, read_db_state, small_val,
};
use omni_engine::{
    Manifest, OmniError, OmniKV, OmniRecord, SSTableReader, SSTableWriter, WriteBatch,
};
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

const UNCOMPRESSED_FLAG: u64 = 1 << 63;
const MAX_VALUE_SIZE: u64 = 10 * 1024 * 1024;

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
        3 => (0u8..KEY_COUNT, 0u8..64, 6u8..24)
            .prop_map(|(key, tag, repeat)| BatchItem::SetCompressed { key, tag, repeat }),
        2 => (0u8..KEY_COUNT).prop_map(|key| BatchItem::Delete { key }),
    ]
}

fn oracle_op_strategy() -> impl Strategy<Value = OracleOp> {
    prop_oneof![
        2 => (0u8..KEY_COUNT).prop_map(|key| OracleOp::SetEmpty { key }),
        5 => (0u8..KEY_COUNT, 0u8..64).prop_map(|(key, tag)| OracleOp::SetSmall { key, tag }),
        3 => (0u8..KEY_COUNT, 0u8..64, 6u8..24)
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

/// Maps `key -> (value_opt, exact_record_seq, may_be_reclaimed)`.
/// For deleted keys (`None`), `may_be_reclaimed` stays `false` until an
/// unpinned `CompactL1ToL2` or `RunHeapGc` runs, so `get_seq_for_key` is
/// strictly required to return the exact tombstone `del_seq` while the
/// tombstone cannot yet have been reclaimed.
type OracleModel = BTreeMap<String, (Option<String>, u64, bool)>;

fn expected_range(model: &OracleModel, start: &str, end: &str) -> Vec<(String, String)> {
    model
        .iter()
        .filter(|(k, _)| k.as_str() >= start && k.as_str() <= end)
        .filter_map(|(k, (v, _, _))| v.as_ref().map(|val| (k.clone(), val.clone())))
        .collect()
}

fn assert_state_matches_model(db: &OmniKV, model: &OracleModel, read_seq: u64, context: &str) {
    for id in 0..KEY_COUNT {
        let k = key_name(id);
        let entry = model.get(&k);
        let expected_val = entry.and_then(|(v, _, _)| v.clone());
        let actual = db
            .find(&k, read_seq)
            .unwrap_or_else(|e| panic!("find({k}, {read_seq}) failed ({context}): {e:?}"));
        assert_eq!(
            actual, expected_val,
            "point-read mismatch for {k} at seq {read_seq} ({context})"
        );
        let key_seq = db.get_seq_for_key(&k, read_seq);
        match entry {
            Some((Some(_), put_seq, _)) => {
                assert_eq!(
                    key_seq, *put_seq,
                    "get_seq_for_key({k}, {read_seq}) did not match exact winning PUT seq ({context})"
                );
            }
            Some((None, del_seq, false)) => {
                assert_eq!(
                    key_seq, *del_seq,
                    "get_seq_for_key({k}, {read_seq}) must return exact un-reclaimed tombstone seq ({context})"
                );
            }
            Some((None, del_seq, true)) => {
                assert!(
                    key_seq == 0 || key_seq == *del_seq,
                    "get_seq_for_key({k}, {read_seq}) returned {key_seq}, expected 0 (reclaimed) or exact delete seq {del_seq} ({context})"
                );
            }
            None => {
                assert_eq!(
                    key_seq, 0,
                    "get_seq_for_key({k}, {read_seq}) must be 0 for never-written key ({context})"
                );
            }
        }
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
        let mut latest: OracleModel = BTreeMap::new();
        let mut snapshots: Vec<(u64, OracleModel)> = Vec::new();

        for (step, op) in ops.into_iter().enumerate() {
            match op {
                OracleOp::SetEmpty { key } => {
                    let k = key_name(key);
                    let mut batch = WriteBatch::new();
                    batch.set(&k, String::new()).expect("stage empty set");
                    let commit_seq = db.commit_batch(&batch).expect("commit empty set");
                    latest.insert(k, (Some(String::new()), commit_seq - 1, false));
                }
                OracleOp::SetSmall { key, tag } => {
                    let k = key_name(key);
                    let v = small_val(tag, step);
                    let mut batch = WriteBatch::new();
                    batch.set(&k, v.clone()).expect("stage set");
                    let commit_seq = db.commit_batch(&batch).expect("commit set");
                    latest.insert(k, (Some(v), commit_seq - 1, false));
                }
                OracleOp::SetCompressed { key, tag, repeat } => {
                    let k = key_name(key);
                    let v = compressed_val(tag, repeat, step);
                    let mut batch = WriteBatch::new();
                    batch.set(&k, v.clone()).expect("stage large set");
                    let commit_seq = db.commit_batch(&batch).expect("commit large set");
                    latest.insert(k, (Some(v), commit_seq - 1, false));
                }
                OracleOp::Delete { key } => {
                    let k = key_name(key);
                    let mut batch = WriteBatch::new();
                    batch.delete(&k).expect("stage delete");
                    let commit_seq = db.commit_batch(&batch).expect("commit delete");
                    latest.insert(k, (None, commit_seq - 1, false));
                }
                OracleOp::MultiBatch(items) => {
                    let mut scratch = BTreeMap::new();
                    for (k, v_opt, op_seq) in apply_multi_batch(&db, &mut scratch, &items, step) {
                        latest.insert(k, (v_opt, op_seq, false));
                    }
                }
                OracleOp::FlushL0 => {
                    db.compact_sstables().expect("flush L0");
                }
                OracleOp::CompactL0ToL1 => {
                    db.compact_l0_to_l1().expect("compact L0->L1");
                }
                OracleOp::CompactL1ToL2 => {
                    db.compact_l1_to_l2().expect("compact L1->L2");
                    if snapshots.is_empty() {
                        for (val, _, reclaimed) in latest.values_mut() {
                            if val.is_none() {
                                *reclaimed = true;
                            }
                        }
                    }
                }
                OracleOp::RunHeapGc => {
                    db.run_garbage_collection().expect("run heap GC");
                    if snapshots.is_empty() {
                        for (val, _, reclaimed) in latest.values_mut() {
                            if val.is_none() {
                                *reclaimed = true;
                            }
                        }
                    }
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

    // Flush batch 1 to SSTable so recovery spans SSTables + WAL.
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
        copy_isolated_db_dir(dir.path(), trial.path());
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
    copy_isolated_db_dir(dir.path(), trial.path());
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
        copy_isolated_db_dir(dir.path(), trial.path());
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

fn assert_sstable_has_straddling_key(path: &Path, stage: &str) {
    let bytes = fs::read(path).expect("read sstable for straddle check");
    let index = parse_sstable_index(&bytes);
    assert!(
        index.len() >= 2,
        "{stage}: sstable must span at least 2 blocks, got {}",
        index.len()
    );
    let data_end = usize::try_from(u64::from_le_bytes(
        bytes[bytes.len() - 16..bytes.len() - 8]
            .try_into()
            .expect("trailer index_offset"),
    ))
    .expect("index_offset fits in usize");
    let mut straddle_count = 0usize;
    for win in index.windows(2) {
        let (_, prev_start) = &win[0];
        let (next_key, next_start) = &win[1];
        let mut cursor = *prev_start;
        let mut last_prev: Option<OmniRecord> = None;
        while cursor < *next_start {
            let (rec, len) = OmniRecord::decode(&bytes[cursor..*next_start])
                .expect("decode record before block boundary");
            cursor += len;
            last_prev = Some(rec);
        }
        assert_eq!(cursor, *next_start, "{stage}: block boundary misaligned");
        let (first_next, _) = OmniRecord::decode(&bytes[*next_start..data_end])
            .expect("decode first record of next block");
        if let Some(prev) = last_prev
            && prev.key == *next_key
            && first_next.key == *next_key
            && first_next.seq > prev.seq
        {
            straddle_count += 1;
        }
    }
    assert!(
        straddle_count >= 1,
        "{stage}: expected at least one 2-version key to straddle an SSTable block boundary"
    );
}

#[test]
fn sstable_two_version_key_straddling_block_boundary_preserves_both_versions() {
    // 9-byte keys (`key_00000`) encode to 51-byte records (`42 + 9`), so each
    // 4096-byte index block holds 81 records (odd), guaranteeing every other
    // block boundary splits a 2-version key across adjacent blocks.
    let dir = TempDir::new().expect("straddle dir");
    let db = open_db(dir.path());
    for i in 0..200 {
        let mut batch = WriteBatch::new();
        batch
            .set(&format!("key_{i:05}"), format!("v_{i:04}_a"))
            .expect("stage first version");
        db.commit_batch(&batch).expect("commit first version");
    }
    let snap_v1 = db.snapshot();
    for i in 0..200 {
        let mut batch = WriteBatch::new();
        batch
            .set(&format!("key_{i:05}"), format!("v_{i:04}_b"))
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
        let roots = db.load_roots();
        let sst_path = match stage {
            "L0" => Path::new(&roots.sstables[0].2),
            "L1" => Path::new(&roots.l1_sstables[0].2),
            "L2" | "GC" => Path::new(&roots.manifest.base_path),
            _ => unreachable!(),
        };
        assert_sstable_has_straddling_key(sst_path, stage);
        let latest = db.get_seq();
        for i in 0..200 {
            let key = format!("key_{i:05}");
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

/// Locates the exact `(winning_seq, tier, backing_id)` (`"memtable"`,
/// `"frozen"`, `"l0"`, `"l1"`, or `"base"`) holding the winning MVCC version
/// (`max seq <= snap`) of `key`.
fn locate_winning_key_tier(
    db: &OmniKV,
    key: &str,
    snap: u64,
) -> Option<(u64, &'static str, String)> {
    let key_bytes = key.as_bytes();
    let roots = db.load_roots();
    // SkipMap orders entries for `key` by `Reverse(seq)` ascending (descending
    // `seq`), so an inclusive lower bound at `Reverse(snap)` lands directly on
    // the highest `seq <= snap` regardless of concurrent writes at `seq > snap`.
    let start = std::cmp::Reverse(snap);
    let mut best: Option<(u64, &'static str, String)> = None;
    let mut consider = |seq: u64, tier: &'static str, id: String| {
        if best.as_ref().is_none_or(|(best_seq, _, _)| seq > *best_seq) {
            best = Some((seq, tier, id));
        }
    };

    for shard in roots.memtable.iter() {
        if let Some(entry) = shard.range((key_bytes.to_vec(), start)..).next()
            && entry.key().0.as_slice() == key_bytes
            && entry.key().1.0 <= snap
        {
            consider(
                entry.key().1.0,
                "memtable",
                format!("{:p}", Arc::as_ptr(&roots.memtable)),
            );
        }
    }
    for frozen in roots.frozen_memtables.iter() {
        for shard in frozen.iter() {
            if let Some(entry) = shard.range((key_bytes.to_vec(), start)..).next()
                && entry.key().0.as_slice() == key_bytes
                && entry.key().1.0 <= snap
            {
                consider(
                    entry.key().1.0,
                    "frozen",
                    format!("{:p}", Arc::as_ptr(frozen)),
                );
            }
        }
    }
    for (tier, tables) in [("l0", &roots.sstables), ("l1", &roots.l1_sstables)] {
        for (mmap, _, path) in tables.iter() {
            let reader = SSTableReader::from_mmap(mmap.clone());
            for rec in reader.iter_from(key_bytes) {
                if rec.key.as_slice() > key_bytes {
                    break;
                }
                if rec.key.as_slice() == key_bytes && rec.is_valid() && rec.seq <= snap {
                    consider(rec.seq, tier, path.clone());
                }
            }
        }
    }
    let base_reader = SSTableReader::from_mmap(roots.base_mmap.clone());
    for rec in base_reader.iter_from(key_bytes) {
        if rec.key.as_slice() > key_bytes {
            break;
        }
        if rec.key.as_slice() == key_bytes && rec.is_valid() && rec.seq <= snap {
            consider(rec.seq, "base", roots.manifest.base_path.clone());
        }
    }
    best
}

fn verify_concurrent_reader_loop(
    db: &OmniKV,
    done: &std::sync::atomic::AtomicBool,
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

        if !checked_across_compaction
            && let Some((tracked_key, _)) = first_view.iter().next()
            && let Some(tier_before) = locate_winning_key_tier(db, tracked_key, snap)
        {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
            let mut tier_moved = false;
            while !done.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
                if let Some(tier_after) = locate_winning_key_tier(db, tracked_key, snap)
                    && tier_after.0 == tier_before.0
                    && (tier_after.1 != tier_before.1
                        || (tier_before.1 == "base" && tier_after.2 != tier_before.2))
                {
                    tier_moved = true;
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            if tier_moved {
                let second_view = assert_snapshot_equivalence(
                    db,
                    snap,
                    writers,
                    &mut last_seen_val,
                    &mut last_seen_seq,
                );
                assert_eq!(
                    second_view, first_view,
                    "pinned snapshot view changed across live tier migration of {tracked_key} at seq {snap}"
                );
                checked_across_compaction = true;
                verified_pinned.fetch_add(1, Ordering::Release);
            }
        }

        db.unregister_snapshot(snap);
        reader_checks.fetch_add(1, Ordering::Release);
    }
}

fn run_concurrent_compactor_loop(db: &OmniKV, done: &std::sync::atomic::AtomicBool) {
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
        std::thread::sleep(std::time::Duration::from_millis(2));
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
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
            while (verified_pinned.load(Ordering::Acquire) < 1
                || reader_checks.load(Ordering::Acquire) < 2)
                && std::time::Instant::now() < deadline
            {
                std::thread::sleep(std::time::Duration::from_millis(2));
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
        thread::spawn(move || run_concurrent_compactor_loop(&db, &done))
    };
    let readers: Vec<_> = (0..2)
        .map(|_| {
            let db = db.clone();
            let done = done.clone();
            let reader_checks = reader_checks.clone();
            let verified_pinned = verified_pinned.clone();
            thread::spawn(move || {
                verify_concurrent_reader_loop(
                    &db,
                    &done,
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
    let rec1 = OmniRecord::new(1, b"k_a_padding_01234567".to_vec(), 0, 4, 11, 0);
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
    // `index_offset` by 14 bytes into entry 0's key bytes (keeping `num_entries`,
    // `key_len`, and `offset` in the sparse index intact): decoding
    // `&data[rec1_len..]` succeeds and emits sparse-index bytes as a valid
    // record, whereas decoding `&data[rec1_len..data_end]` stops cleanly.
    let rec2_avail = index_offset - rec1_len;
    assert!(rec2_avail >= 42);
    let oversized_key_len = u16::try_from(rec2_avail - 42 + 14).expect("key_len fits in u16");
    let straddle_end = rec1_len + 42 + usize::from(oversized_key_len);
    assert!(straddle_end <= bytes.len() - 16);
    bytes[rec1_len + 8..rec1_len + 10].copy_from_slice(&oversized_key_len.to_le_bytes());
    let forged_crc = crc32fast::hash(&bytes[rec1_len..straddle_end - 4]);
    bytes[straddle_end - 4..straddle_end].copy_from_slice(&forged_crc.to_le_bytes());

    assert_eq!(
        parse_sstable_index(&bytes).len(),
        1,
        "sparse index structure must remain intact while forged record straddles into it"
    );
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
    assert_eq!(decoded[0].key, b"k_a_padding_01234567");
    assert_eq!(
        reader.find(b"k_a_padding_01234567", u64::MAX),
        Some((0, 4, 11, 0))
    );
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

#[test]
fn wal_batch_with_missing_or_zeroed_heap_payload_is_rejected_atomically_on_reopen() {
    // Simulates a power-loss crash where the kernel persisted a complete
    // checksummed WAL batch (including `__COMMIT_MARKER__`) before its heap
    // payloads reached stable storage (either truncated heap EOF or zero-filled
    // sparse allocation). Recovery must reject the ENTIRE batch atomically —
    // neither overwriting `k_keep` with a dangling heap pointer nor partially
    // applying `delete("k_del_target")` — and heal the WAL for future writes.
    for zero_fill_instead_of_truncate in [false, true] {
        let dir = TempDir::new().expect("wal heap crash dir");
        let db = open_db(dir.path());

        let mut durable = WriteBatch::new();
        durable
            .set("k_keep", "durable_old".into())
            .expect("stage durable k_keep");
        durable
            .set("k_del_target", "must_survive".into())
            .expect("stage durable k_del_target");
        db.commit_batch(&durable).expect("commit durable baseline");

        let heap_path = db.load_roots().manifest.heap_path.clone();
        let heap_len_before = fs::metadata(&heap_path).expect("heap metadata").len();

        let k_keep_new = "unpersisted_new_value";
        let k_new_val = "unpersisted_new_key";
        let mut unpersisted = WriteBatch::new();
        unpersisted
            .set("k_keep", k_keep_new.into())
            .expect("stage unpersisted overwrite");
        unpersisted
            .set("k_new", k_new_val.into())
            .expect("stage unpersisted insert");
        unpersisted
            .delete("k_del_target")
            .expect("stage unpersisted delete");
        db.commit_batch(&unpersisted)
            .expect("write WAL+heap batch before crash simulation");
        let heap_len_after = fs::metadata(&heap_path).expect("heap metadata after").len();
        // Both values are < 64 bytes (uncompressed) and written contiguously:
        // keep the first PUT (`k_keep`) 100% intact and damage ONLY the second
        // PUT (`k_new`) so a buggy replay loop that applies records before
        // validating the whole batch would overwrite `k_keep` and fail below.
        let expected_added =
            u64::try_from(k_keep_new.len() + k_new_val.len()).expect("len fits in u64");
        assert_eq!(heap_len_after, heap_len_before + expected_added);
        drop(db);

        let mut heap_bytes = fs::read(&heap_path).expect("read heap bytes");
        let k_new_offset =
            usize::try_from(heap_len_before).expect("heap len fits in usize") + k_keep_new.len();
        assert_eq!(
            &heap_bytes[k_new_offset..],
            k_new_val.as_bytes(),
            "mutation target must isolate only k_new's payload while keeping k_keep intact"
        );
        if zero_fill_instead_of_truncate {
            heap_bytes[k_new_offset..].fill(0);
        } else {
            heap_bytes.truncate(k_new_offset);
        }
        fs::write(&heap_path, &heap_bytes).expect("simulate dropped k_new heap write");

        let reopened = open_db(dir.path());
        let seq = reopened.get_seq();
        assert_eq!(
            reopened.find("k_keep", seq).expect("find k_keep"),
            Some("durable_old".into()),
            "k_keep must retain prior durable value (zero_fill={zero_fill_instead_of_truncate})"
        );
        assert_eq!(
            reopened
                .find("k_del_target", seq)
                .expect("find k_del_target"),
            Some("must_survive".into()),
            "DELETE in rejected batch must NOT apply partially (zero_fill={zero_fill_instead_of_truncate})"
        );
        assert_eq!(
            reopened.find("k_new", seq).expect("find k_new"),
            None,
            "unpersisted insert must be absent (zero_fill={zero_fill_instead_of_truncate})"
        );

        // Post-recovery writes must commit cleanly and survive a second reopen,
        // proving `replay_and_heal_since` truncated the rejected WAL batch.
        let mut post = WriteBatch::new();
        post.set("k_post", "survived_heal".into())
            .expect("stage post write");
        reopened
            .commit_batch(&post)
            .expect("commit after heap-validated WAL heal");
        drop(reopened);

        let reopened2 = open_db(dir.path());
        assert_eq!(
            reopened2
                .find("k_post", reopened2.get_seq())
                .expect("find k_post after second reopen"),
            Some("survived_heal".into())
        );
        assert_eq!(
            reopened2
                .find("k_del_target", reopened2.get_seq())
                .expect("find k_del_target after second reopen"),
            Some("must_survive".into())
        );
    }
}

#[test]
fn corrupt_heap_payload_guards_reject_bad_empty_crc_oversized_and_short_lz4() {
    let dir = TempDir::new().expect("heap guards dir");
    let db = open_db(dir.path());

    // 1. Empty uncompressed payload with a corrupted CRC must NOT short-circuit to Ok("").
    let seq1 = db.get_seq() + 1;
    db.insert_replica_record(
        b"bad_empty_crc".to_vec(),
        seq1,
        0,
        UNCOMPRESSED_FLAG,
        0xDEAD_BEEF,
        0,
    );
    let err1 = db
        .find("bad_empty_crc", seq1)
        .expect_err("corrupt empty-value CRC must fail closed");
    assert!(
        matches!(err1, OmniError::IoError(ref msg) if msg.contains("CRC32")),
        "expected CRC32 mismatch for bad empty payload, got {err1:?}"
    );

    // 2. Encoded lengths above `2 * MAX_VALUE_SIZE` (both raw and compressed) must fail
    //    upfront before allocating or seeking.
    let oversized_len = MAX_VALUE_SIZE * 2 + 1;
    for (idx, length_with_flag) in [UNCOMPRESSED_FLAG | oversized_len, oversized_len]
        .into_iter()
        .enumerate()
    {
        let key = format!("oversized_{idx}");
        let seq = db.get_seq() + 1;
        db.insert_replica_record(key.as_bytes().to_vec(), seq, 0, length_with_flag, 0, 0);
        let err = db
            .find(&key, seq)
            .expect_err("oversized heap length must fail closed without OOM");
        assert!(
            matches!(err, OmniError::IoError(ref msg) if msg.contains("exceeds maximum")),
            "expected oversized length error, got {err:?}"
        );
    }

    // 3. Compressed payload shorter than the 4-byte LZ4 size prefix must fail upfront.
    let short_payload = [0x11u8, 0x22, 0x33];
    let short_off = db
        .append_to_heap(&short_payload)
        .expect("append short payload");
    let seq3 = db.get_seq() + 1;
    db.insert_replica_record(
        b"short_lz4".to_vec(),
        seq3,
        short_off,
        short_payload.len() as u64,
        crc32fast::hash(&short_payload),
        0,
    );
    let err3 = db
        .find("short_lz4", seq3)
        .expect_err("sub-4-byte compressed payload must fail closed");
    assert!(
        matches!(err3, OmniError::IoError(ref msg) if msg.contains("LZ4 size prefix")),
        "expected missing LZ4 size prefix error, got {err3:?}"
    );
}

#[test]
fn wal_mid_stream_dropped_heap_stops_at_atomic_prefix_and_does_not_apply_later_batch() {
    let dir = TempDir::new().expect("mid-stream heap dir");
    let db = open_db(dir.path());
    let mut model = BTreeMap::new();

    // Batch 1 (valid durable prefix)
    apply_multi_batch(
        &db,
        &mut model,
        &[BatchItem::SetSmall { key: 0, tag: 1 }],
        1,
    );
    let expected_b1 = normalize_model(&model);
    let heap_path = db.load_roots().manifest.heap_path.clone();
    let heap_after_b1 = usize::try_from(fs::metadata(&heap_path).expect("heap meta b1").len())
        .expect("heap offset fits in usize");

    // Batch 2 (whose heap range will be zeroed to simulate an unpersisted heap write)
    apply_multi_batch(
        &db,
        &mut model,
        &[
            BatchItem::SetCompressed {
                key: 1,
                tag: 2,
                repeat: 8,
            },
            BatchItem::Delete { key: 0 },
        ],
        2,
    );
    let heap_after_b2 = usize::try_from(fs::metadata(&heap_path).expect("heap meta b2").len())
        .expect("heap offset fits in usize");
    assert!(heap_after_b2 > heap_after_b1);

    // Batch 3 (intact WAL + intact heap — must NOT replay past invalid Batch 2!)
    apply_multi_batch(
        &db,
        &mut model,
        &[BatchItem::SetSmall { key: 2, tag: 3 }],
        3,
    );
    db.sync_all().expect("sync all batches");
    drop(db);

    let mut heap_bytes = fs::read(&heap_path).expect("read heap bytes");
    heap_bytes[heap_after_b1..heap_after_b2].fill(0);
    fs::write(&heap_path, &heap_bytes).expect("zero batch 2 heap range");

    let recovered = open_db(dir.path());
    assert_eq!(
        read_db_state(&recovered),
        expected_b1,
        "WAL replay must stop at Batch 1 and must not apply Batch 3 past invalid Batch 2"
    );

    let mut post = WriteBatch::new();
    post.set("k09", "post_mid_heap_heal".into())
        .expect("stage post write");
    recovered.commit_batch(&post).expect("commit post write");
    drop(recovered);

    let reopened = open_db(dir.path());
    let mut expected_final = expected_b1;
    expected_final.insert("k09".into(), Some("post_mid_heap_heal".into()));
    assert_eq!(read_db_state(&reopened), expected_final);
}

fn assert_corrupt_sstable_tier_fails_closed(src_dir: &Path, target: &str, zero_fill: bool) {
    let trial = TempDir::new().expect("corrupt trial dir");
    copy_isolated_db_dir(src_dir, trial.path());
    let manifest_str = trial
        .path()
        .join("manifest.json")
        .to_string_lossy()
        .to_string();
    let wal_str = trial.path().join("wal.bin").to_string_lossy().to_string();
    let manifest_before = Manifest::load(&manifest_str).expect("load trial manifest");
    let (victim, corrupt_key) = match target {
        "l0" => (manifest_before.sstables[0].clone(), "k02_b"),
        "l1" => (manifest_before.l1_sstables[0].clone(), "k01_b"),
        "base" => (manifest_before.base_path.clone(), "k00_b"),
        _ => unreachable!(),
    };
    if zero_fill {
        // 4096-byte zeroed L0/L1 and 8192-byte zeroed base must all fail
        // closed immediately at load time on `OmniKV::open`.
        let zero_len = if target == "base" { 8192 } else { 4096 };
        fs::write(&victim, vec![0u8; zero_len]).expect("zero-fill victim sstable");
        assert!(
            OmniKV::open(&manifest_str, &wal_str).is_err(),
            "{target} (zero_fill=true): OmniKV::open must reject zero-filled SSTable at load time"
        );
        return;
    }

    let mut raw = fs::read(&victim).expect("read victim sstable");
    let (_, rec0_len) = OmniRecord::decode(&raw).expect("decode first record in victim");
    // Corrupt Record 1 inside Block 0 while leaving Block 0's start record,
    // sparse index, and footer intact.
    raw[rec0_len..rec0_len + 12].fill(0xFF);
    fs::write(&victim, &raw).expect("corrupt mid-block record");

    let reopened = open_db(trial.path());
    let read_seq = reopened.get_seq();
    assert!(
        reopened.find(corrupt_key, read_seq).is_err(),
        "{target}: point read reaching corrupt mid-block record must fail closed"
    );
    assert!(
        reopened.scan("k00_a", "k99_z", read_seq).is_err(),
        "{target}: scan reaching corrupt mid-block record must fail closed"
    );
    assert!(
        reopened.scan_limit("k00_a", "k99_z", read_seq, 32).is_err(),
        "{target}: scan_limit reaching corrupt mid-block record must fail closed"
    );
    assert_eq!(
        reopened.get_seq_for_key(corrupt_key, read_seq),
        u64::MAX,
        "{target}: get_seq_for_key reaching corrupt mid-block record must fail closed"
    );

    let primary_res = match target {
        "l0" => reopened.compact_l0_to_l1(),
        "l1" => reopened.compact_l1_to_l2(),
        "base" => reopened.run_garbage_collection(),
        _ => unreachable!(),
    };
    assert!(
        primary_res.is_err(),
        "{target} (zero_fill=false) primary compaction/GC must fail closed on corrupt SSTable"
    );
    let secondary_res = match target {
        "l0" | "l1" => reopened.run_garbage_collection(),
        "base" => reopened.compact_l1_to_l2(),
        _ => unreachable!(),
    };
    assert!(
        secondary_res.is_err(),
        "{target} (zero_fill=false) secondary compaction/GC must fail closed on corrupt SSTable"
    );
    let manifest_after = Manifest::load(&manifest_str).expect("reload trial manifest");
    assert_eq!(
        manifest_after.base_path, manifest_before.base_path,
        "{target}: corrupt compaction must not overwrite base manifest pointer"
    );
    assert_eq!(
        manifest_after.l1_sstables, manifest_before.l1_sstables,
        "{target}: corrupt compaction must not drop L1 manifest pointers"
    );
    assert_eq!(
        manifest_after.sstables, manifest_before.sstables,
        "{target}: corrupt compaction must not drop L0 manifest pointers"
    );
}

#[test]
fn compaction_and_gc_fail_closed_on_corrupt_sstable() {
    let dir = TempDir::new().expect("corrupt sst dir");
    let db = open_db(dir.path());

    // Place 2 records per tier so mid-block record corruption (Record 1, not
    // the Block 0 start validated at open time) exercises post-open read,
    // compaction, and GC fail-closed paths, while zero-filled files exercise
    // immediate load-time rejection on `OmniKV::open`.
    let mut b_base = WriteBatch::new();
    b_base
        .set("k00_a", "in_base_a".into())
        .expect("stage base a");
    b_base
        .set("k00_b", "in_base_b".into())
        .expect("stage base b");
    db.commit_batch(&b_base).expect("commit base");
    db.compact_sstables().expect("flush base L0");
    db.compact_l0_to_l1().expect("compact base L1");
    db.compact_l1_to_l2().expect("compact base L2");

    let mut b_l1 = WriteBatch::new();
    b_l1.set("k01_a", "in_l1_a".into()).expect("stage l1 a");
    b_l1.set("k01_b", "in_l1_b".into()).expect("stage l1 b");
    db.commit_batch(&b_l1).expect("commit l1");
    db.compact_sstables().expect("flush l1 L0");
    db.compact_l0_to_l1().expect("compact l1");

    let mut b_l0 = WriteBatch::new();
    b_l0.set("k02_a", "in_l0_a".into()).expect("stage l0 a");
    b_l0.set("k02_b", "in_l0_b".into()).expect("stage l0 b");
    db.commit_batch(&b_l0).expect("commit l0");
    db.compact_sstables().expect("flush l0");
    drop(db);

    for (target, zero_fill) in [
        ("l0", false),
        ("l1", false),
        ("base", false),
        ("l0", true),
        ("l1", true),
        ("base", true),
    ] {
        assert_corrupt_sstable_tier_fails_closed(dir.path(), target, zero_fill);
    }
}

#[test]
fn sstable_strict_validation_rejects_zero_and_shifted_footer_offsets() {
    let dir = TempDir::new().expect("footer corrupt dir");
    let db = open_db(dir.path());

    // Two distinct keys in a single L0 table so shifting `index_offset` from
    // `rec0.len() + rec1.len()` down to `rec0.len()` lands on a valid record
    // boundary and would silently drop `rec1` without full index validation.
    let mut batch = WriteBatch::new();
    batch.set("k_first", "val_1".into()).expect("stage first");
    batch
        .set("k_second_longer_key", "val_2".into())
        .expect("stage second");
    db.commit_batch(&batch).expect("commit two keys");
    db.compact_sstables().expect("flush L0");
    drop(db);

    let src_manifest = Manifest::load(dir.path().join("manifest.json").to_string_lossy().as_ref())
        .expect("load src manifest");
    let src_l0_bytes = fs::read(&src_manifest.sstables[0]).expect("read L0 bytes");
    let (rec0, rec0_len) =
        OmniRecord::decode(&src_l0_bytes).expect("decode first record in L0 table");
    assert_eq!(rec0.key, b"k_first");
    let shifted_offset = u64::try_from(rec0_len).expect("rec0 len fits in u64");

    for (label, forged_index_offset) in [("zero_offset", 0u64), ("shifted_to_rec1", shifted_offset)]
    {
        let trial = TempDir::new().expect("footer trial dir");
        copy_isolated_db_dir(dir.path(), trial.path());
        let manifest_str = trial
            .path()
            .join("manifest.json")
            .to_string_lossy()
            .to_string();
        let wal_str = trial.path().join("wal.bin").to_string_lossy().to_string();
        let manifest_before = Manifest::load(&manifest_str).expect("load trial manifest");
        let l0_path = &manifest_before.sstables[0];
        let mut bytes = fs::read(l0_path).expect("read trial L0");
        let len = bytes.len();
        bytes[len - 16..len - 8].copy_from_slice(&forged_index_offset.to_le_bytes());
        fs::write(l0_path, &bytes).expect("write forged footer L0");

        assert!(
            OmniKV::open(&manifest_str, &wal_str).is_err(),
            "{label}: OmniKV::open must fail closed immediately on forged v2 footer offset"
        );
        let manifest_after = Manifest::load(&manifest_str).expect("reload trial manifest");
        assert_eq!(
            manifest_after.sstables, manifest_before.sstables,
            "{label}: failed open must preserve original L0 manifest entry"
        );
        assert_eq!(
            manifest_after.l1_sstables,
            Vec::<String>::new(),
            "{label}: failed open must not modify L1 manifest entries"
        );
    }
}

#[test]
fn l1_to_l2_compaction_aborts_on_misaligned_l0_sparse_index_offset() {
    let dir = TempDir::new().expect("misaligned l0 dir");
    let db = open_db(dir.path());

    // Reserve an older seq for `k_target` (which will sit in Block 1 of a
    // multi-block L0 table), then commit a newer DELETE of `k_target` into L1.
    let older_seq = db.get_seq();
    let mut del = WriteBatch::new();
    del.set("k_anchor", "anchor".into()).expect("stage anchor");
    del.delete("k_target").expect("stage delete");
    db.commit_batch(&del).expect("commit newer delete");
    db.compact_sstables().expect("flush delete L0");
    db.compact_l0_to_l1().expect("compact delete L1");

    // Populate enough keys before `k_target` to push `k_target` into Block 1
    // of a fresh L0 table, placing the older PUT of `k_target` in L0.
    for i in 0..100 {
        let mut b = WriteBatch::new();
        b.set(&format!("a_pad_{i:04}"), "pad".into())
            .expect("stage pad");
        db.commit_batch(&b).expect("commit pad");
    }
    let payload = b"stale_older_put";
    let off = db.append_to_heap(payload).expect("append stale payload");
    db.insert_replica_record(
        b"k_target".to_vec(),
        older_seq,
        off,
        UNCOMPRESSED_FLAG | u64::try_from(payload.len()).expect("len fits in u64"),
        crc32fast::hash(payload),
        0,
    );
    db.compact_sstables().expect("flush multi-block L0");
    drop(db);

    // Corrupt Block 1's sparse-index offset by +1 byte so a best-effort seek
    // would fail to decode Block 1 and return `None` for `k_target`.
    let manifest_str = dir
        .path()
        .join("manifest.json")
        .to_string_lossy()
        .to_string();
    let wal_str = dir.path().join("wal.bin").to_string_lossy().to_string();
    let manifest_before = Manifest::load(&manifest_str).expect("load manifest");
    let l0_path = &manifest_before.sstables[0];
    let mut bytes = fs::read(l0_path).expect("read L0");
    let index = parse_sstable_index(&bytes);
    assert!(index.len() >= 2, "L0 table must span at least 2 blocks");
    let index_offset = usize::try_from(u64::from_le_bytes(
        bytes[bytes.len() - 16..bytes.len() - 8]
            .try_into()
            .expect("footer index offset"),
    ))
    .expect("index offset fits in usize");
    // Entry 0 occupies `8 + 2 + entry0_key_len + 8`; Entry 1's offset field
    // sits immediately after `2 + entry1_key_len`.
    let entry1_off_pos = index_offset + 8 + 2 + index[0].0.len() + 8 + 2 + index[1].0.len();
    let bad_block1_off = u64::try_from(index[1].1 + 1).expect("offset fits in u64");
    bytes[entry1_off_pos..entry1_off_pos + 8].copy_from_slice(&bad_block1_off.to_le_bytes());
    fs::write(l0_path, &bytes).expect("write misaligned L0");

    assert!(
        OmniKV::open(&manifest_str, &wal_str).is_err(),
        "OmniKV::open must fail closed immediately when an L0 sparse-index block offset is misaligned"
    );
    let manifest_after = Manifest::load(&manifest_str).expect("reload manifest");
    assert_eq!(manifest_after.l1_sstables, manifest_before.l1_sstables);
    assert_eq!(manifest_after.base_path, manifest_before.base_path);
}

#[test]
fn zero_filled_4096_byte_generated_base_fails_closed_while_legacy_empty_base_migrates() {
    // 1. A generated base SSTable with one 2010-byte key has exact length
    //    `42 + 2010 (record) + 8 + 2 + 2010 + 8 (index) + 16 (footer) = 4096`.
    //    Zero-filling it in place without changing its length MUST fail closed
    //    immediately on `OmniKV::open`.
    let dir = TempDir::new().expect("gen 4096 base dir");
    let db = open_db(dir.path());
    let key_2010 = "b".repeat(2010);
    let mut b1 = WriteBatch::new();
    b1.set(&key_2010, "base_live_val".into())
        .expect("stage 2010B key");
    db.commit_batch(&b1).expect("commit 2010B key");
    db.compact_sstables().expect("flush L0");
    db.compact_l0_to_l1().expect("compact L0->L1");
    db.compact_l1_to_l2().expect("compact L1->L2");

    let generated_base = db.load_roots().manifest.base_path.clone();
    assert_eq!(
        fs::metadata(&generated_base).expect("base meta").len(),
        4096,
        "generated 1-record base with 2010-byte key must be exactly 4096 bytes"
    );
    // Stage an L1 table so manifest contains active upper-tier entries too.
    let mut b2 = WriteBatch::new();
    b2.set("k_l1", "l1_val".into()).expect("stage l1 key");
    db.commit_batch(&b2).expect("commit l1 key");
    db.compact_sstables().expect("flush second L0");
    db.compact_l0_to_l1().expect("compact second L1");
    drop(db);

    let trial = TempDir::new().expect("zero 4096 trial dir");
    copy_isolated_db_dir(dir.path(), trial.path());
    let manifest_str = trial
        .path()
        .join("manifest.json")
        .to_string_lossy()
        .to_string();
    let wal_str = trial.path().join("wal.bin").to_string_lossy().to_string();
    let manifest_before = Manifest::load(&manifest_str).expect("load trial manifest");
    assert_eq!(
        fs::metadata(&manifest_before.base_path)
            .expect("trial base meta")
            .len(),
        4096
    );
    fs::write(&manifest_before.base_path, vec![0u8; 4096])
        .expect("zero-fill 4096B generated base without changing length");

    assert!(
        OmniKV::open(&manifest_str, &wal_str).is_err(),
        "length-preserving zero-fill of a 4096-byte generated base must fail closed on open"
    );
    let manifest_after = Manifest::load(&manifest_str).expect("reload trial manifest");
    assert_eq!(manifest_after.base_path, manifest_before.base_path);
    assert_eq!(manifest_after.heap_path, manifest_before.heap_path);

    // 2. A genuine untouched pre-v2 legacy placeholder (`{manifest_path}_base.bin`
    //    of 4096 zero bytes, `wal_checkpoint_seq == 0`, `read_seq_floor == 0`)
    //    must still open and migrate cleanly through L1->L2 and GC.
    let legacy_dir = TempDir::new().expect("legacy base dir");
    let db = open_db(legacy_dir.path());
    let mut lb = WriteBatch::new();
    lb.set("k_legacy", "migrated_ok".into())
        .expect("stage legacy key");
    db.commit_batch(&lb).expect("commit legacy key");
    db.compact_sstables().expect("flush legacy L0");
    db.compact_l0_to_l1().expect("compact legacy L1");
    let legacy_base = db.load_roots().manifest.base_path.clone();
    drop(db);

    fs::write(&legacy_base, vec![0u8; 4096]).expect("write legacy 4096B zero placeholder");

    // 2a. Direct GC migration of the untouched 4096B legacy placeholder (no
    //     preceding `compact_l1_to_l2`), plus 2b. L1->L2 followed by GC.
    let legacy_gc_dir = TempDir::new().expect("legacy direct gc dir");
    copy_isolated_db_dir(legacy_dir.path(), legacy_gc_dir.path());
    let direct_gc = open_db(legacy_gc_dir.path());
    direct_gc
        .run_garbage_collection()
        .expect("legacy placeholder must migrate cleanly via direct GC");
    assert_eq!(
        direct_gc
            .find("k_legacy", direct_gc.get_seq())
            .expect("find direct-gc migrated key"),
        Some("migrated_ok".into())
    );

    let migrated = open_db(legacy_dir.path());
    migrated
        .compact_l1_to_l2()
        .expect("legacy placeholder must migrate cleanly via L1->L2");
    migrated
        .run_garbage_collection()
        .expect("migrated legacy DB must pass GC");
    assert_eq!(
        migrated
            .find("k_legacy", migrated.get_seq())
            .expect("find migrated key"),
        Some("migrated_ok".into())
    );
}

#[test]
fn oversized_key_above_u16_max_rejected_at_staging_and_commit_boundary_while_u16_max_succeeds() {
    use omni_engine::wal::WriteAheadLog;

    let dir = TempDir::new().expect("key len dir");
    let db = open_db(dir.path());

    // Exact maximum representable key (`u16::MAX == 65_535` bytes) must work
    // end-to-end across commit, compaction, and cold reopen.
    let max_key = "m".repeat(usize::from(u16::MAX));
    let mut ok_batch = WriteBatch::new();
    ok_batch
        .set(&max_key, "max_key_ok".into())
        .expect("stage u16::MAX key");
    db.commit_batch(&ok_batch).expect("commit u16::MAX key");

    // 65,536-byte key (`u16::MAX + 1`) must fail at every staging and commit entry point.
    let too_long_str = "x".repeat(usize::from(u16::MAX) + 1);
    let too_long_bytes = too_long_str.as_bytes().to_vec();
    let mut staging = WriteBatch::new();
    assert!(staging.set(&too_long_str, "v".into()).is_err());
    assert!(staging.set_with_ttl(&too_long_str, "v".into(), 60).is_err());
    assert!(
        staging
            .set_with_expiry(&too_long_str, "v".into(), 999)
            .is_err()
    );
    assert!(staging.delete(&too_long_str).is_err());

    let direct_write = WriteBatch {
        buffered_writes: vec![(too_long_bytes.clone(), "v".into(), 0)],
        buffered_deletes: Vec::new(),
    };
    let direct_delete = WriteBatch {
        buffered_writes: Vec::new(),
        buffered_deletes: vec![too_long_bytes.clone()],
    };
    assert!(
        db.commit_batch(&direct_write).is_err(),
        "commit_batch must reject direct WriteBatch with key > u16::MAX"
    );
    assert!(
        db.commit_batch(&direct_delete).is_err(),
        "commit_batch must reject direct delete WriteBatch with key > u16::MAX"
    );

    // Direct WAL and SSTableWriter APIs must also reject > u16::MAX keys.
    let direct_wal_path = dir.path().join("direct_keylen.wal");
    let mut wal =
        WriteAheadLog::new(direct_wal_path.to_string_lossy().as_ref()).expect("open direct wal");
    let bad_wal_batch = vec![
        (OmniRecord::new(1, too_long_bytes.clone(), 0, 0, 0, 0), None),
        (
            OmniRecord::new(2, b"__COMMIT_MARKER__".to_vec(), 1, 0, 0, 0),
            None,
        ),
    ];
    assert!(wal.append_batch(&bad_wal_batch).is_err());

    let sst_file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(dir.path().join("direct_keylen.sst"))
        .expect("create direct sst");
    let mut sst_writer = SSTableWriter::new(&sst_file);
    assert!(
        sst_writer
            .append(&OmniRecord::new(1, too_long_bytes, 0, 0, 0, 0))
            .is_err()
    );

    // A subsequent valid batch must commit and survive cold reopen with `max_key` intact.
    let mut follow_up = WriteBatch::new();
    follow_up
        .set("k_after", "intact".into())
        .expect("stage follow-up");
    db.commit_batch(&follow_up).expect("commit follow-up");
    drop(db);

    let reopened = open_db(dir.path());
    let seq = reopened.get_seq();
    assert_eq!(
        reopened.find(&max_key, seq).expect("find u16::MAX key"),
        Some("max_key_ok".into())
    );
    assert_eq!(
        reopened.find("k_after", seq).expect("find follow-up key"),
        Some("intact".into())
    );
    reopened.compact_sstables().expect("flush u16::MAX L0");
    reopened.compact_l0_to_l1().expect("compact u16::MAX L1");
    reopened.compact_l1_to_l2().expect("compact u16::MAX L2");
    assert_eq!(
        reopened
            .find(&max_key, reopened.get_seq())
            .expect("find u16::MAX key after L2"),
        Some("max_key_ok".into())
    );
    drop(reopened);

    // Cold reopen after L2 compaction exercises on-disk base/bloom load for u16::MAX key.
    let reopened2 = open_db(dir.path());
    assert_eq!(
        reopened2
            .find(&max_key, reopened2.get_seq())
            .expect("find u16::MAX key after cold L2 reopen"),
        Some("max_key_ok".into())
    );
}

#[test]
fn heap_gc_rejects_oversized_heap_length_before_allocating() {
    let dir = TempDir::new().expect("gc len cap dir");
    let db = open_db(dir.path());

    // Flagged empty value + pinned tombstone + normal value must all pass GC cleanly.
    let mut valid = WriteBatch::new();
    valid.set("k_live", "ok".into()).expect("stage live");
    valid.set("k_empty", String::new()).expect("stage empty");
    valid.set("k_tomb", "gone".into()).expect("stage tomb");
    db.commit_batch(&valid).expect("commit valid");
    let snap_before_del = db.snapshot();
    let mut del = WriteBatch::new();
    del.delete("k_tomb").expect("stage delete");
    db.commit_batch(&del).expect("commit delete");

    db.run_garbage_collection()
        .expect("GC with empty value and pinned tombstone must succeed");
    let seq = db.get_seq();
    assert_eq!(db.find("k_live", seq).expect("live"), Some("ok".into()));
    assert_eq!(db.find("k_empty", seq).expect("empty"), Some(String::new()));
    assert_eq!(db.find("k_tomb", seq).expect("tomb latest"), None);
    assert_eq!(
        db.find("k_tomb", snap_before_del).expect("tomb historical"),
        Some("gone".into())
    );
    db.unregister_snapshot(snap_before_del);
    db.run_garbage_collection()
        .expect("GC after releasing snapshot must reclaim tombstone cleanly");
    assert_eq!(
        db.find("k_tomb", db.get_seq()).expect("tomb after reclaim"),
        None
    );

    // Inject a live record with a valid record checksum (flushed into L0 so
    // `try_iter_all` yields it) whose encoded heap length exceeds
    // `MAX_ENCODED_VALUE_SIZE` (20 MiB + 1, both uncompressed and compressed).
    let huge_len = MAX_VALUE_SIZE * 2 + 1;
    for (idx, flagged_len) in [UNCOMPRESSED_FLAG | huge_len, huge_len]
        .into_iter()
        .enumerate()
    {
        let trial = TempDir::new().expect("gc oversized trial");
        let trial_db = open_db(trial.path());
        let bad_seq = trial_db.get_seq() + 1;
        trial_db.insert_replica_record(
            format!("huge_gc_{idx}").into_bytes(),
            bad_seq,
            0,
            flagged_len,
            0,
            0,
        );
        trial_db
            .compact_sstables()
            .expect("flush forged record to L0");
        let err = trial_db
            .run_garbage_collection()
            .expect_err("GC must reject oversized heap length before allocating");
        assert!(
            matches!(err, OmniError::IoError(ref msg) if msg.contains("exceeds maximum")),
            "expected oversized GC length error, got {err:?}"
        );
    }
}

#[test]
fn heap_gc_rejects_bad_empty_crc_short_lz4_huge_lz4_and_offset_overflow() {
    // 1. Bad CRC on flagged empty value (`UNCOMPRESSED_FLAG | 0`).
    let dir1 = TempDir::new().expect("gc bad empty crc");
    let db1 = open_db(dir1.path());
    let seq1 = db1.get_seq() + 1;
    db1.insert_replica_record(b"bad_empty".to_vec(), seq1, 0, UNCOMPRESSED_FLAG, 0xBAD, 0);
    db1.compact_sstables().expect("flush bad empty");
    let err1 = db1
        .run_garbage_collection()
        .expect_err("GC must reject corrupt empty-value CRC");
    assert!(matches!(err1, OmniError::IoError(ref msg) if msg.contains("CRC32")));

    // 2. Compressed payload shorter than 4-byte LZ4 size prefix.
    let dir2 = TempDir::new().expect("gc short lz4");
    let db2 = open_db(dir2.path());
    let short_bytes = [1u8, 2, 3];
    let off2 = db2.append_to_heap(&short_bytes).expect("append short");
    let seq2 = db2.get_seq() + 1;
    db2.insert_replica_record(
        b"short_lz4".to_vec(),
        seq2,
        off2,
        u64::try_from(short_bytes.len()).expect("len"),
        crc32fast::hash(&short_bytes),
        0,
    );
    db2.compact_sstables().expect("flush short lz4");
    let err2 = db2
        .run_garbage_collection()
        .expect_err("GC must reject < 4-byte compressed payload");
    assert!(matches!(err2, OmniError::IoError(ref msg) if msg.contains("LZ4 size prefix")));

    // 3. Valid-CRC compressed payload declaring > MAX_VALUE_SIZE (1 GiB).
    let dir3 = TempDir::new().expect("gc huge lz4");
    let db3 = open_db(dir3.path());
    let mut forged = Vec::new();
    forged.extend_from_slice(&(1024u32 * 1024 * 1024).to_le_bytes());
    forged.extend_from_slice(&[0x00, 0x01, 0x02, 0x03]);
    let off3 = db3.append_to_heap(&forged).expect("append forged lz4");
    let seq3 = db3.get_seq() + 1;
    db3.insert_replica_record(
        b"huge_lz4".to_vec(),
        seq3,
        off3,
        u64::try_from(forged.len()).expect("len"),
        crc32fast::hash(&forged),
        0,
    );
    db3.compact_sstables().expect("flush huge lz4");
    let err3 = db3
        .run_garbage_collection()
        .expect_err("GC must reject oversized declared LZ4 header");
    assert!(matches!(err3, OmniError::IoError(ref msg) if msg.contains("exceeds maximum")));

    // 4. Overflowing heap pointer (`u64::MAX - 2 + 8`) must fail before allocating.
    let dir4 = TempDir::new().expect("gc overflow");
    let db4 = open_db(dir4.path());
    let seq4 = db4.get_seq() + 1;
    let overflow_len = 8u64;
    db4.insert_replica_record(
        b"overflow_ptr".to_vec(),
        seq4,
        u64::MAX - 2,
        UNCOMPRESSED_FLAG | overflow_len,
        0,
        0,
    );
    db4.compact_sstables().expect("flush overflow ptr");
    let err4 = db4
        .run_garbage_collection()
        .expect_err("GC must reject overflowing heap offset + len");
    assert!(matches!(err4, OmniError::IoError(ref msg) if msg.contains("exceeds heap size")));
}

#[test]
fn block_cache_key_includes_length_and_crc_to_prevent_corrupt_alias_hit() {
    let dir = TempDir::new().expect("cache key dir");
    let db = open_db(dir.path());

    let payload = b"legitimate_cached_payload";
    let off = db.append_to_heap(payload).expect("append real payload");
    let good_len = UNCOMPRESSED_FLAG | u64::try_from(payload.len()).expect("len fits in u64");
    let good_crc = crc32fast::hash(payload);
    let seq1 = db.get_seq() + 1;
    db.insert_replica_record(b"real_k".to_vec(), seq1, off, good_len, good_crc, 0);
    // Warm `block_cache` for `(heap_generation, off, good_len, good_crc)`.
    assert_eq!(
        db.find("real_k", seq1).expect("read real key"),
        Some("legitimate_cached_payload".into())
    );

    // Alias pointing at the EXACT same `off` with a corrupted CRC must miss
    // cache and fail CRC validation instead of returning the cached neighbor.
    let seq2 = db.get_seq() + 1;
    db.insert_replica_record(
        b"bad_crc_alias".to_vec(),
        seq2,
        off,
        good_len,
        good_crc ^ 1,
        0,
    );
    let err_crc = db
        .find("bad_crc_alias", seq2)
        .expect_err("bad CRC at cached offset must not hit block_cache");
    assert!(matches!(err_crc, OmniError::IoError(ref msg) if msg.contains("CRC32")));

    // Alias pointing at the EXACT same `off` with a shorter length must also miss cache.
    let short_raw_len = 4u64;
    let short_len = UNCOMPRESSED_FLAG | short_raw_len;
    let short_crc = crc32fast::hash(&payload[..4]);
    let seq3 = db.get_seq() + 1;
    db.insert_replica_record(
        b"short_len_alias".to_vec(),
        seq3,
        off,
        short_len,
        short_crc,
        0,
    );
    assert_eq!(
        db.find("short_len_alias", seq3).expect("read short alias"),
        Some("legi".into()),
        "shorter length at cached offset must not return full neighbor value from block_cache"
    );
}
