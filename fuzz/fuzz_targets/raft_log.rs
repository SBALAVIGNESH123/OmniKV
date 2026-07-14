#![no_main]

use libfuzzer_sys::fuzz_target;
use omni_engine::{OmniKV, raft_storage::OmniRaftStorage};

fuzz_target!(|data: &[u8]| {
    if data.len() > 4 * 1024 {
        return;
    }

    let Ok(dir) = tempfile::tempdir() else {
        return;
    };
    let manifest = dir.path().join("manifest.json");
    let wal = dir.path().join("wal.bin");
    let Ok(db) = OmniKV::open(manifest.to_string_lossy().as_ref(), wal.to_string_lossy().as_ref())
    else {
        return;
    };
    let storage = OmniRaftStorage::new(db);

    for chunk in data.chunks(4).take(64) {
        let op = chunk.first().copied().unwrap_or_default() % 4;
        let raw_index = chunk.get(1).copied().unwrap_or(1);
        let index = u64::from(raw_index % 64) + 1;

        match op {
            0 => {
                let key_id = chunk.get(2).copied().unwrap_or_default() % 16;
                let value_id = chunk.get(3).copied().unwrap_or_default();
                let _ = storage.append_log(index, &format!("SET fuzz_k{key_id} fuzz_v{value_id}"));
            }
            1 => {
                let _ = storage.read_log(index);
            }
            2 => {
                if let Some(entry) = storage.read_log(index) {
                    let _ = storage.apply_write(&entry);
                }
            }
            _ => {
                let end = index.saturating_add(u64::from(chunk.get(2).copied().unwrap_or(0) % 8));
                let _ = storage.delete_log_range(index, end);
            }
        }
        assert!(storage.last_applied_index() <= 64);
    }
});
