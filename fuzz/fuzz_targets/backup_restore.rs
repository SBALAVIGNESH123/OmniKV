#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() > 128 * 1024 {
        return;
    }

    if let Ok(dir) = tempfile::tempdir() {
        let backup_path = dir.path().join("candidate.tar.gz");
        let restore_dir = dir.path().join("restore");
        if std::fs::write(&backup_path, data).is_ok() {
            let _ = omni_engine::backup::restore_backup(
                backup_path.to_string_lossy().as_ref(),
                restore_dir.to_string_lossy().as_ref(),
            );
        }
    }
});
