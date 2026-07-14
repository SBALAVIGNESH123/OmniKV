#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() > 64 * 1024 {
        return;
    }

    let _ = omni_engine::OmniRecord::decode(data);

    if let Ok(dir) = tempfile::tempdir() {
        let wal_path = dir.path().join("candidate.wal");
        if std::fs::write(&wal_path, data).is_ok() {
            let _ = omni_engine::wal::WriteAheadLog::replay(
                wal_path.to_string_lossy().as_ref(),
                dir.path().join("heap.bin").to_string_lossy().as_ref(),
            );
        }
    }
});
