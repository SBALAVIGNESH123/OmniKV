use omni_engine::{OmniKV, OmniRecord, backup, raft_storage::OmniRaftStorage, sql};
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("engine crate should live below repo root")
        .to_path_buf()
}

fn corpus_paths(target: &str) -> Vec<PathBuf> {
    ["corpus", "regressions"]
        .into_iter()
        .map(|kind| repo_root().join("fuzz").join(kind).join(target))
        .filter(|path| path.exists())
        .flat_map(|dir| {
            std::fs::read_dir(dir)
                .expect("read fuzz corpus directory")
                .map(|entry| entry.expect("read fuzz corpus entry").path())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn corpus_bytes(target: &str) -> Vec<Vec<u8>> {
    let bytes = corpus_paths(target)
        .into_iter()
        .filter(|path| path.is_file())
        .map(|path| {
            std::fs::read(&path).unwrap_or_else(|err| {
                panic!("read {}: {err}", path.display());
            })
        })
        .collect::<Vec<_>>();
    assert!(!bytes.is_empty(), "expected checked-in corpus for {target}");
    bytes
}

#[test]
fn sql_parser_corpus_does_not_panic() {
    for bytes in corpus_bytes("sql_parser") {
        if let Ok(sql) = std::str::from_utf8(&bytes) {
            let _ = sql::parse_sql(sql);
        }
    }
}

#[test]
fn wal_record_corpus_does_not_panic() {
    for bytes in corpus_bytes("wal_record") {
        let _ = OmniRecord::decode(&bytes);

        let dir = tempfile::tempdir().expect("temp wal corpus dir");
        let wal_path = dir.path().join("candidate.wal");
        std::fs::write(&wal_path, &bytes).expect("write candidate wal");
        let _ = omni_engine::wal::WriteAheadLog::replay(
            wal_path.to_string_lossy().as_ref(),
            dir.path().join("heap.bin").to_string_lossy().as_ref(),
        );
    }
}

#[test]
fn backup_restore_corpus_does_not_panic() {
    for bytes in corpus_bytes("backup_restore") {
        let dir = tempfile::tempdir().expect("temp backup corpus dir");
        let backup_path = dir.path().join("candidate.tar.gz");
        let restore_dir = dir.path().join("restore");
        std::fs::write(&backup_path, &bytes).expect("write candidate backup");
        let _ = backup::restore_backup(
            backup_path.to_string_lossy().as_ref(),
            restore_dir.to_string_lossy().as_ref(),
        );
    }
}

#[test]
fn raft_log_corpus_does_not_panic() {
    for bytes in corpus_bytes("raft_log") {
        let dir = tempfile::tempdir().expect("temp raft corpus dir");
        let manifest = dir.path().join("manifest.json");
        let wal = dir.path().join("wal.bin");
        let db = OmniKV::open(
            manifest.to_string_lossy().as_ref(),
            wal.to_string_lossy().as_ref(),
        )
        .expect("open raft corpus db");
        let storage = OmniRaftStorage::new(db);

        for chunk in bytes.chunks(4).take(64) {
            let op = chunk.first().copied().unwrap_or_default() % 4;
            let raw_index = chunk.get(1).copied().unwrap_or(1);
            let index = u64::from(raw_index % 64) + 1;
            match op {
                0 => {
                    let key_id = chunk.get(2).copied().unwrap_or_default() % 16;
                    let value_id = chunk.get(3).copied().unwrap_or_default();
                    let _ =
                        storage.append_log(index, &format!("SET fuzz_k{key_id} fuzz_v{value_id}"));
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
                    let end =
                        index.saturating_add(u64::from(chunk.get(2).copied().unwrap_or(0) % 8));
                    let _ = storage.delete_log_range(index, end);
                }
            }
            assert!(storage.last_applied_index() <= 64);
        }
    }
}
