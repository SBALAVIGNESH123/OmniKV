use omni_engine::{OmniKV, WriteBatch};
#[test]
fn test_debug_reopen() {
    let dir = tempfile::TempDir::new().unwrap();
    let manifest = dir
        .path()
        .join("manifest.json")
        .to_string_lossy()
        .to_string();
    let wal = dir.path().join("data.wal").to_string_lossy().to_string();

    {
        let db = OmniKV::open(&manifest, &wal).unwrap();
        for i in 0u64..200 {
            let mut b = WriteBatch::new();
            b.set(&format!("ckey{:05}", i), format!("cval{}", i))
                .unwrap();
            db.commit_batch(&b).unwrap();
        }
        db.compact_sstables().unwrap();
        let md = std::fs::metadata(&wal).unwrap();
        println!("WAL size before drop: {}", md.len());
    }

    {
        let md = std::fs::metadata(&wal).unwrap();
        println!("WAL size before reopen: {}", md.len());
        let db = OmniKV::open(&manifest, &wal).unwrap();
        println!("After reopen seq: {}", db.get_seq());
    }
}
