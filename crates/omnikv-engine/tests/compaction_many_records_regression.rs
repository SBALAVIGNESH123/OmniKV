use omni_engine::{OmniKV, WriteBatch};

#[test]
fn compaction_preserves_many_committed_keys() {
    let dir = tempfile::TempDir::new().unwrap();
    let manifest = dir
        .path()
        .join("manifest.json")
        .to_string_lossy()
        .to_string();
    let wal = dir.path().join("data.wal").to_string_lossy().to_string();
    let db = OmniKV::open(&manifest, &wal).unwrap();

    for i in 0u64..200 {
        let mut b = WriteBatch::new();
        b.set(&format!("ckey{i:05}"), format!("cval{i}")).unwrap();
        db.commit_batch(&b).unwrap();
    }

    db.compact_sstables().unwrap();
    let snap = db.snapshot();
    for i in [0u64, 99, 199] {
        let key = format!("ckey{i:05}");
        let got = db.find(&key, snap).unwrap();
        assert_eq!(
            got,
            Some(format!("cval{i}")),
            "manual compaction must preserve {key}"
        );
    }
    db.unregister_snapshot(snap);
}
