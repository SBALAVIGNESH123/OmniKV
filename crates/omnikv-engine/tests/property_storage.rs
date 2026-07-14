use omni_engine::{CompactionPolicy, OmniKV, WriteBatch};
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Debug)]
enum ModelOp {
    Set { key: u8, value: u8 },
    Delete { key: u8 },
    FlushMemtable,
    CompactL0,
}

fn model_op_strategy() -> impl Strategy<Value = ModelOp> {
    prop_oneof![
        6 => (0u8..8, 0u8..64).prop_map(|(key, value)| ModelOp::Set { key, value }),
        2 => (0u8..8).prop_map(|key| ModelOp::Delete { key }),
        1 => Just(ModelOp::FlushMemtable),
        1 => Just(ModelOp::CompactL0),
    ]
}

fn open_property_db() -> (tempfile::TempDir, Arc<OmniKV>) {
    let dir = tempfile::tempdir().expect("temp db dir");
    let manifest = dir.path().join("manifest.json");
    let wal = dir.path().join("wal.bin");
    let db = OmniKV::open(
        manifest.to_string_lossy().as_ref(),
        wal.to_string_lossy().as_ref(),
    )
    .expect("open property db");
    db.set_compaction_policy(CompactionPolicy {
        l0_compaction_trigger: 4,
        l1_compaction_trigger: 4,
        l0_write_stall_threshold: 256,
        write_stall_wait_attempts: 1,
        write_stall_wait_ms: 1,
    })
    .expect("set relaxed property compaction policy");
    (dir, db)
}

fn key(id: u8) -> String {
    format!("k{id:02}")
}

fn value(id: u8, step: usize) -> String {
    format!("v{id:02}_{step:03}")
}

fn expected_value(state: &BTreeMap<String, Option<String>>, key: &str) -> Option<String> {
    state.get(key).and_then(Clone::clone)
}

fn assert_find_matches_model(db: &OmniKV, state: &BTreeMap<String, Option<String>>, read_seq: u64) {
    for id in 0..8 {
        let key = key(id);
        assert_eq!(
            db.find(&key, read_seq).expect("find should not fail"),
            expected_value(state, &key),
            "key {key} visibility mismatch at seq {read_seq}"
        );
    }
}

fn expected_scan(state: &BTreeMap<String, Option<String>>) -> Vec<(String, String)> {
    state
        .iter()
        .filter_map(|(key, value)| value.clone().map(|value| (key.clone(), value)))
        .collect()
}

fn assert_scan_matches_latest(db: &OmniKV, state: &BTreeMap<String, Option<String>>) {
    let read_seq = db.get_seq();
    let actual = db
        .scan("k00", "k99", read_seq)
        .expect("latest range scan should not fail");
    let expected = expected_scan(state);
    assert_eq!(actual, expected, "latest range scan should match model");
    assert!(
        actual.windows(2).all(|pair| pair[0].0 <= pair[1].0),
        "range scan keys should stay sorted"
    );
}

fn commit_set(db: &OmniKV, key: &str, value: String) -> u64 {
    let mut batch = WriteBatch::new();
    batch.set(key, value).expect("stage set");
    db.commit_batch(&batch).expect("commit set")
}

fn commit_delete(db: &OmniKV, key: &str) -> u64 {
    let mut batch = WriteBatch::new();
    batch.delete(key).expect("stage delete");
    db.commit_batch(&batch).expect("commit delete")
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 48,
        max_shrink_iters: 256,
        ..ProptestConfig::default()
    })]

    #[test]
    fn storage_visibility_survives_flush_l1_compaction_and_snapshots(
        ops in proptest::collection::vec(model_op_strategy(), 1..64)
    ) {
        let (_dir, db) = open_property_db();
        let mut latest: BTreeMap<String, Option<String>> = BTreeMap::new();
        let mut pinned_snapshots: Vec<(u64, BTreeMap<String, Option<String>>)> = Vec::new();

        for (step, op) in ops.into_iter().enumerate() {
            match op {
                ModelOp::Set { key: key_id, value: value_id } => {
                    let key = key(key_id);
                    let value = value(value_id, step);
                    let committed_seq = commit_set(&db, &key, value.clone());
                    latest.insert(key, Some(value));
                    let snapshot = db.snapshot();
                    prop_assert!(snapshot >= committed_seq);
                    pinned_snapshots.push((snapshot, latest.clone()));
                }
                ModelOp::Delete { key: key_id } => {
                    let key = key(key_id);
                    let committed_seq = commit_delete(&db, &key);
                    latest.insert(key, None);
                    let snapshot = db.snapshot();
                    prop_assert!(snapshot >= committed_seq);
                    pinned_snapshots.push((snapshot, latest.clone()));
                }
                ModelOp::FlushMemtable => {
                    db.compact_sstables().expect("flush memtable to L0");
                }
                ModelOp::CompactL0 => {
                    db.compact_sstables().expect("flush memtable before L1");
                    db.compact_l0_to_l1().expect("compact L0 to L1");
                }
            }

            assert_find_matches_model(&db, &latest, db.get_seq());
            assert_scan_matches_latest(&db, &latest);
        }

        db.compact_sstables().expect("final memtable flush");
        db.compact_l0_to_l1().expect("final L0 compaction");

        assert_find_matches_model(&db, &latest, db.get_seq());
        assert_scan_matches_latest(&db, &latest);

        for (read_seq, state) in &pinned_snapshots {
            assert_find_matches_model(&db, state, *read_seq);
        }

        for (read_seq, _) in pinned_snapshots {
            db.unregister_snapshot(read_seq);
        }
    }
}
