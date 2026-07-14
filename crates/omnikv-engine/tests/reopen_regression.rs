use omni_engine::{OmniKV, WriteBatch};

#[test]
fn reopen_after_compaction_preserves_values_and_sequence() {
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
            b.set(&format!("ckey{i:05}"), format!("cval{i}")).unwrap();
            db.commit_batch(&b).unwrap();
        }
        db.compact_sstables().unwrap();
    }

    {
        let db = OmniKV::open(&manifest, &wal).unwrap();
        assert!(
            db.get_seq() >= 200,
            "reopening must recover sequence progress; got {}",
            db.get_seq()
        );

        let seq = db.get_seq();
        for i in [0u64, 99, 199] {
            let key = format!("ckey{i:05}");
            let got = db.find(&key, seq).unwrap();
            assert_eq!(
                got,
                Some(format!("cval{i}")),
                "reopen after compaction must preserve {key}"
            );
        }
    }
}
