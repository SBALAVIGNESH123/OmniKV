use omni_engine::{OmniKV, WriteBatch};

#[test]
fn compaction_preserves_single_committed_key() {
    let dir = tempfile::TempDir::new().unwrap();
    let manifest = dir
        .path()
        .join("manifest.json")
        .to_string_lossy()
        .to_string();
    let wal = dir.path().join("data.wal").to_string_lossy().to_string();
    let db = OmniKV::open(&manifest, &wal).unwrap();

    let mut b = WriteBatch::new();
    b.set("ckey00001", "cval1".to_string()).unwrap();
    db.commit_batch(&b).unwrap();

    db.compact_sstables().unwrap();

    let snap = db.snapshot();
    let got = db.find("ckey00001", snap).unwrap();
    db.unregister_snapshot(snap);

    assert_eq!(
        got,
        Some("cval1".to_string()),
        "manual compaction must preserve the committed key"
    );
}
