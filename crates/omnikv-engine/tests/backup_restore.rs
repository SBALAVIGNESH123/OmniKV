use flate2::Compression;
use flate2::write::GzEncoder;
use omni_engine::backup::{
    create_backup_with_wal, create_encrypted_backup_with_wal, restore_backup,
    restore_encrypted_backup,
};
use omni_engine::{OmniKV, WriteBatch};
use tar::{Builder, EntryType, Header};

fn open_db(dir: &tempfile::TempDir) -> (std::sync::Arc<OmniKV>, String, String) {
    let manifest = dir
        .path()
        .join("manifest.json")
        .to_string_lossy()
        .to_string();
    let wal = dir.path().join("wal.bin").to_string_lossy().to_string();
    let db = OmniKV::open(&manifest, &wal).expect("open database");
    (db, manifest, wal)
}

fn put_keys(db: &OmniKV, prefix: &str, count: usize) {
    let mut batch = WriteBatch::new();
    for i in 0..count {
        batch
            .set(&format!("{prefix}:{i:04}"), format!("value-{i}"))
            .expect("buffer write");
    }
    db.commit_batch(&batch).expect("commit batch");
}

fn verify_keys(db: &OmniKV, prefix: &str, count: usize) {
    let seq = db.get_seq();
    for i in 0..count {
        let key = format!("{prefix}:{i:04}");
        let expected = format!("value-{i}");
        assert_eq!(db.find(&key, seq).expect("find key"), Some(expected));
    }
}

#[test]
fn plain_backup_restore_roundtrip_uses_public_contract() {
    let source_dir = tempfile::tempdir().expect("source dir");
    let restore_dir = tempfile::tempdir().expect("restore dir");
    let backup_path = source_dir.path().join("backup.tar.gz");

    let (db, manifest, wal) = open_db(&source_dir);
    put_keys(&db, "plain", 128);

    create_backup_with_wal(&db, &manifest, &wal, backup_path.to_str().unwrap())
        .expect("create backup");

    restore_backup(
        backup_path.to_str().unwrap(),
        restore_dir.path().to_str().unwrap(),
    )
    .expect("restore backup");

    let restored_manifest = restore_dir
        .path()
        .join("manifest.json")
        .to_string_lossy()
        .to_string();
    let restored_wal = restore_dir
        .path()
        .join("wal.bin")
        .to_string_lossy()
        .to_string();
    let restored = OmniKV::open(&restored_manifest, &restored_wal).expect("open restored backup");

    verify_keys(&restored, "plain", 128);
}

#[test]
fn encrypted_backup_restore_roundtrip_and_wrong_passphrase_rejection() {
    let source_dir = tempfile::tempdir().expect("source dir");
    let restore_dir = tempfile::tempdir().expect("restore dir");
    let wrong_restore_dir = tempfile::tempdir().expect("wrong restore dir");
    let backup_path = source_dir.path().join("backup.omnikv.enc");

    let (db, manifest, wal) = open_db(&source_dir);
    put_keys(&db, "encrypted", 64);

    create_encrypted_backup_with_wal(
        &db,
        &manifest,
        &wal,
        backup_path.to_str().unwrap(),
        "correct horse battery staple",
    )
    .expect("create encrypted backup");

    let wrong_passphrase = restore_encrypted_backup(
        backup_path.to_str().unwrap(),
        wrong_restore_dir.path().to_str().unwrap(),
        "not the passphrase",
    );
    assert!(
        wrong_passphrase.is_err(),
        "wrong passphrase must not restore encrypted backups"
    );

    restore_encrypted_backup(
        backup_path.to_str().unwrap(),
        restore_dir.path().to_str().unwrap(),
        "correct horse battery staple",
    )
    .expect("restore encrypted backup");

    let restored_manifest = restore_dir
        .path()
        .join("manifest.json")
        .to_string_lossy()
        .to_string();
    let restored_wal = restore_dir
        .path()
        .join("wal.bin")
        .to_string_lossy()
        .to_string();
    let restored = OmniKV::open(&restored_manifest, &restored_wal).expect("open restored backup");

    verify_keys(&restored, "encrypted", 64);
}

#[test]
fn restore_rejects_path_traversal_entries() {
    let archive_dir = tempfile::tempdir().expect("archive dir");
    let restore_dir = tempfile::tempdir().expect("restore dir");
    let archive_path = archive_dir.path().join("malicious.tar.gz");
    let evil_path = restore_dir
        .path()
        .parent()
        .expect("restore parent")
        .join("evil.txt");

    let file = std::fs::File::create(&archive_path).expect("malicious archive");
    let encoder = GzEncoder::new(file, Compression::default());
    let mut tar = Builder::new(encoder);
    let mut header = Header::new_gnu();
    header.set_entry_type(EntryType::Symlink);
    header.set_size(0);
    header.set_mode(0o600);
    header.set_cksum();
    tar.append_link(&mut header, "safe-looking-link", "../evil.txt")
        .expect("append malicious symlink");
    tar.finish().expect("finish tar");
    let encoder = tar.into_inner().expect("finish tar stream");
    encoder.finish().expect("finish gzip");

    let err = restore_backup(
        archive_path.to_str().unwrap(),
        restore_dir.path().to_str().unwrap(),
    )
    .expect_err("path traversal archive must be rejected");

    assert!(
        err.contains("Unsupported backup entry type"),
        "unexpected restore error: {err}"
    );
    assert!(!evil_path.exists(), "restore must not write outside target");
}
