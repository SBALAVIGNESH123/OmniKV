use omni_engine::{OmniKV, WriteBatch};
#[test]
fn test_debug_compaction() {
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

    println!("Before compaction, sstable count: {}", db.sstable_count());
    db.compact_sstables().unwrap();
    println!("After compaction, sstable count: {}", db.sstable_count());

    let snap = db.snapshot();
    let got = db.find("ckey00001", snap).unwrap();
    println!("Found: {got:?}");
}
