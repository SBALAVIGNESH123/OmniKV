//! Shared storage oracle test harness used by `storage_oracle.rs` and
//! `storage_oracle_failpoints.rs`.

#![allow(dead_code)]

use omni_engine::{CompactionPolicy, Manifest, OmniKV, WriteBatch};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;

pub const KEY_COUNT: u8 = 10;

#[derive(Clone, Debug)]
pub enum BatchItem {
    SetEmpty { key: u8 },
    SetSmall { key: u8, tag: u8 },
    SetCompressed { key: u8, tag: u8, repeat: u8 },
    Delete { key: u8 },
}

pub fn key_name(id: u8) -> String {
    format!("k{id:02}")
}

pub fn small_val(tag: u8, step: usize) -> String {
    format!("s{tag:02}_{step:03}")
}

/// Generates a value guaranteed to exceed the 64-byte LZ4 compression threshold
/// (`11 * repeat >= 66` bytes).
pub fn compressed_val(tag: u8, repeat: u8, step: usize) -> String {
    let chunk = format!("blk-{tag:02}-{step:03}-");
    let val = chunk.repeat(usize::from(repeat).max(6));
    debug_assert!(val.len() >= 64);
    val
}

pub fn open_db(dir: &Path) -> Arc<OmniKV> {
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

pub fn normalize_model(
    model: &BTreeMap<String, Option<String>>,
) -> BTreeMap<String, Option<String>> {
    model
        .iter()
        .filter_map(|(k, v)| v.as_ref().map(|s| (k.clone(), Some(s.clone()))))
        .collect()
}

pub fn read_db_state(db: &OmniKV) -> BTreeMap<String, Option<String>> {
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

/// Commits a multi-key batch (deduplicating keys within the batch) and updates
/// `model`. Returns `(key, value_opt, exact_record_seq)` for every committed
/// operation in exact engine sequence order.
pub fn apply_multi_batch(
    db: &OmniKV,
    model: &mut BTreeMap<String, Option<String>>,
    items: &[BatchItem],
    step: usize,
) -> Vec<(String, Option<String>, u64)> {
    // Keep keys distinct within one batch so the model's atomic outcome is well-defined.
    let mut seen = BTreeSet::new();
    let mut batch = WriteBatch::new();
    let mut staged_writes: Vec<(String, String)> = Vec::new();
    let mut staged_deletes: Vec<String> = Vec::new();
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
                staged_writes.push((k, String::new()));
            }
            BatchItem::SetSmall { tag, .. } => {
                let v = small_val(*tag, step * 10 + idx);
                batch.set(&k, v.clone()).expect("stage small set");
                staged_writes.push((k, v));
            }
            BatchItem::SetCompressed { tag, repeat, .. } => {
                let v = compressed_val(*tag, *repeat, step * 10 + idx);
                batch.set(&k, v.clone()).expect("stage compressed set");
                staged_writes.push((k, v));
            }
            BatchItem::Delete { .. } => {
                batch.delete(&k).expect("stage delete");
                staged_deletes.push(k);
            }
        }
    }
    if batch.is_empty() {
        return Vec::new();
    }
    // `commit_batch` assigns contiguous sequences: all `buffered_writes` first,
    // then all `buffered_deletes`, followed by `__COMMIT_MARKER__` at `commit_seq`.
    let commit_seq = db.commit_batch(&batch).expect("commit multi-op batch");
    let num_ops = u64::try_from(staged_writes.len() + staged_deletes.len())
        .expect("batch op count fits in u64");
    let base_seq = commit_seq - num_ops;
    let mut applied = Vec::with_capacity(staged_writes.len() + staged_deletes.len());
    for (i, (k, v)) in staged_writes.into_iter().enumerate() {
        let op_seq = base_seq + u64::try_from(i).expect("write idx fits in u64");
        model.insert(k.clone(), Some(v.clone()));
        applied.push((k, Some(v), op_seq));
    }
    let write_count = applied.len();
    for (j, k) in staged_deletes.into_iter().enumerate() {
        let op_seq = base_seq + u64::try_from(write_count + j).expect("del idx fits in u64");
        model.insert(k.clone(), None);
        applied.push((k, None, op_seq));
    }
    applied
}

/// Copies a database directory into an isolated trial directory, skipping
/// `LOCK` and rewriting absolute `Manifest` paths (`heap_path`, `base_path`,
/// `sstables`, `l1_sstables`) from `src` to `dst` so trial opens and writes
/// can never mutate `src`.
pub fn copy_isolated_db_dir(src: &Path, dst: &Path) {
    let rewrite = |stored: &str| -> String {
        let name = Path::new(stored)
            .file_name()
            .expect("manifest file path has filename");
        dst.join(name).to_string_lossy().to_string()
    };

    for entry in fs::read_dir(src).expect("read src dir") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = entry.file_name();
        if name == "LOCK" {
            continue;
        }
        fs::copy(&path, dst.join(&name)).expect("copy db file");
    }

    let manifest_dst = dst.join("manifest.json");
    if manifest_dst.exists() {
        let manifest_str = manifest_dst.to_string_lossy().to_string();
        let mut manifest = Manifest::load(&manifest_str).expect("load copied manifest");
        manifest.heap_path = rewrite(&manifest.heap_path);
        manifest.base_path = rewrite(&manifest.base_path);
        manifest.sstables = manifest.sstables.iter().map(|p| rewrite(p)).collect();
        manifest.l1_sstables = manifest.l1_sstables.iter().map(|p| rewrite(p)).collect();
        let encoded = serde_json::to_vec(&manifest).expect("serialize isolated manifest");
        fs::write(&manifest_dst, encoded).expect("write rewritten isolated manifest");
    }
}
