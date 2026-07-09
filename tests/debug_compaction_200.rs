use omni_engine::{OmniKV, WriteBatch};
#[test]
fn test_debug_compaction_200() {
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
        b.set(&format!("ckey{:05}", i), format!("cval{}", i))
            .unwrap();
        db.commit_batch(&b).unwrap();
    }

    db.compact_sstables().unwrap();
    let snap = db.snapshot();
    let got = db.find("ckey00000", snap).unwrap();
    println!("GOT: {:?}", got);
}
