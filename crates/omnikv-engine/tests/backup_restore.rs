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

fn repack_backup_archive_with_overrides(
    src_archive: &std::path::Path,
    dst_archive: &std::path::Path,
    replace_base: Option<&[u8]>,
    omit_base_bloom: bool,
    force_wal_checkpoint_seq: Option<u64>,
    force_read_seq_floor: Option<u64>,
) {
    use flate2::read::GzDecoder;
    use omni_engine::backup::BackupMetadata;
    use std::io::Read;
    use tar::Archive;

    let src_file = std::fs::File::open(src_archive).expect("open src archive");
    let mut archive = Archive::new(GzDecoder::new(src_file));
    let mut entries: Vec<(String, Vec<u8>)> = Vec::new();
    let mut orig_meta: Option<BackupMetadata> = None;

    for entry in archive.entries().expect("src archive entries") {
        let mut entry = entry.expect("src entry");
        let path = entry
            .path()
            .expect("src entry path")
            .to_string_lossy()
            .replace('\\', "/");
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).expect("read src entry");
        if path == "omnikv-backup.json" {
            orig_meta = Some(serde_json::from_slice(&bytes).expect("parse src metadata"));
            continue;
        }
        if omit_base_bloom && path == "base.bloom" {
            continue;
        }
        if path == "base.bin"
            && let Some(replacement) = replace_base
        {
            replacement.clone_into(&mut bytes);
        }
        if path == "manifest.json"
            && (force_wal_checkpoint_seq.is_some() || force_read_seq_floor.is_some())
        {
            let mut manifest: omni_engine::Manifest =
                serde_json::from_slice(&bytes).expect("parse archive manifest");
            if let Some(checkpoint_seq) = force_wal_checkpoint_seq {
                manifest.wal_checkpoint_seq = checkpoint_seq;
            }
            if let Some(read_seq_floor) = force_read_seq_floor {
                manifest.read_seq_floor = read_seq_floor;
            }
            bytes = serde_json::to_vec_pretty(&manifest).expect("serialize archive manifest");
        }
        entries.push((path, bytes));
    }

    let mut metadata = orig_meta.expect("src metadata");
    metadata.files.clear();

    let dst_file = std::fs::File::create(dst_archive).expect("create dst archive");
    let encoder = GzEncoder::new(dst_file, Compression::default());
    let mut tar = Builder::new(encoder);

    for (path, bytes) in &entries {
        let mut header = Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o600);
        header.set_cksum();
        tar.append_data(&mut header, path, bytes.as_slice())
            .expect("append repacked entry");
        metadata
            .files
            .push(omni_engine::backup::BackupFileMetadata {
                path: path.clone(),
                bytes: bytes.len() as u64,
                crc32: crc32fast::hash(bytes),
            });
    }

    let meta_bytes = serde_json::to_vec_pretty(&metadata).expect("serialize repacked metadata");
    let mut meta_header = Header::new_gnu();
    meta_header.set_size(meta_bytes.len() as u64);
    meta_header.set_mode(0o600);
    meta_header.set_cksum();
    tar.append_data(
        &mut meta_header,
        "omnikv-backup.json",
        meta_bytes.as_slice(),
    )
    .expect("append repacked metadata");

    tar.finish().expect("finish repacked tar");
    tar.into_inner()
        .expect("repacked tar inner")
        .finish()
        .expect("finish repacked gzip");
}

#[test]
fn restored_pre_v2_legacy_zero_base_backup_with_l0_and_l1_opens_compacts_and_runs_gc() {
    let source_dir = tempfile::tempdir().expect("source dir");
    let (db, manifest, wal) = open_db(&source_dir);

    put_keys(&db, "l1_data", 16);
    db.compact_sstables().expect("flush first L0");
    db.compact_l0_to_l1().expect("compact to L1");

    put_keys(&db, "l0_data", 16);
    db.compact_sstables().expect("flush second L0");

    let source_base_path = db.load_roots().manifest.base_path.clone();
    drop(db);

    // Replace the untouched initial base with a pre-v2 4096-byte zero placeholder.
    std::fs::write(&source_base_path, vec![0u8; 4096]).expect("write legacy 4096B zero base");
    let db = OmniKV::open(&manifest, &wal).expect("reopen pre-v2 source DB");

    // 1. Fresh backup of the pre-v2 DB upgrades `base.bin` cleanly on creation.
    let fresh_backup = source_dir.path().join("fresh_legacy_backup.tar.gz");
    create_backup_with_wal(&db, &manifest, &wal, fresh_backup.to_str().unwrap())
        .expect("create backup from pre-v2 DB");
    drop(db);

    let fresh_restore = tempfile::tempdir().expect("fresh restore dir");
    restore_backup(
        fresh_backup.to_str().unwrap(),
        fresh_restore.path().to_str().unwrap(),
    )
    .expect("restore fresh backup");
    let fresh_db = OmniKV::open(
        fresh_restore.path().join("manifest.json").to_str().unwrap(),
        fresh_restore.path().join("wal.bin").to_str().unwrap(),
    )
    .expect("open fresh restored DB");
    fresh_db
        .run_garbage_collection()
        .expect("fresh restored DB must pass GC");
    verify_keys(&fresh_db, "l1_data", 16);
    verify_keys(&fresh_db, "l0_data", 16);
    drop(fresh_db);

    // 2. Pre-existing pre-v2 archive whose `base.bin` is 4096 all-zero bytes
    //    alongside L0 and L1 tables — tested WITH empty `base.bloom`, WITHOUT
    //    `base.bloom`, and with `read_seq_floor > 0` (pre-v2 Raft snapshot
    //    `data_base.bin` backup), exercising both GC and L1->L2 compaction.
    let zero_base = vec![0u8; 4096];
    for (idx, (omit_base_bloom, read_seq_floor)) in [(false, None), (true, None), (true, Some(10))]
        .into_iter()
        .enumerate()
    {
        let legacy_archive = source_dir.path().join(format!("legacy_case_{idx}.tar.gz"));
        repack_backup_archive_with_overrides(
            &fresh_backup,
            &legacy_archive,
            Some(&zero_base),
            omit_base_bloom,
            None,
            read_seq_floor,
        );

        for op in ["gc", "l1_to_l2"] {
            let restore_dir = tempfile::tempdir().expect("legacy restore dir");
            restore_backup(
                legacy_archive.to_str().unwrap(),
                restore_dir.path().to_str().unwrap(),
            )
            .expect("restore pre-v2 archive");

            let restored_manifest = restore_dir.path().join("manifest.json");
            let restored_wal = restore_dir.path().join("wal.bin");
            let restored = OmniKV::open(
                restored_manifest.to_str().unwrap(),
                restored_wal.to_str().unwrap(),
            )
            .expect("open restored pre-v2 archive");

            match op {
                "gc" => restored
                    .run_garbage_collection()
                    .expect("restored pre-v2 archive with L0/L1 must run GC"),
                "l1_to_l2" => {
                    restored
                        .compact_l1_to_l2()
                        .expect("restored pre-v2 archive with L0/L1 must run L1->L2");
                    restored
                        .run_garbage_collection()
                        .expect("restored pre-v2 archive must pass GC after L1->L2");
                }
                _ => unreachable!(),
            }
            verify_keys(&restored, "l1_data", 16);
            verify_keys(&restored, "l0_data", 16);
        }
    }
}

#[test]
fn restore_does_not_upgrade_corrupted_zero_filled_4096_byte_generated_base() {
    let source_dir = tempfile::tempdir().expect("source dir");
    let (db, manifest, wal) = open_db(&source_dir);

    // One 2010-byte key produces an exact 4096-byte generated L2 base table.
    let key_2010 = "g".repeat(2010);
    let mut b1 = WriteBatch::new();
    b1.set(&key_2010, "live_base_val".into())
        .expect("stage 2010B key");
    db.commit_batch(&b1).expect("commit 2010B key");
    db.compact_sstables().expect("flush L0");
    db.compact_l0_to_l1().expect("compact L0->L1");
    db.compact_l1_to_l2().expect("compact L1->L2");
    assert_eq!(
        std::fs::metadata(&db.load_roots().manifest.base_path)
            .expect("generated base metadata")
            .len(),
        4096
    );

    put_keys(&db, "l0_extra", 4);
    db.compact_sstables().expect("flush extra L0");

    let valid_backup = source_dir.path().join("valid_4096_base.tar.gz");
    create_backup_with_wal(&db, &manifest, &wal, valid_backup.to_str().unwrap())
        .expect("create valid backup");
    drop(db);

    let zero_base = vec![0u8; 4096];

    // Case A: Zero-filled 4096B generated base with populated `base.bloom`
    // (L1->L2 base where `wal_checkpoint_seq == 0`) must NOT be upgraded.
    let corrupt_with_bloom = source_dir.path().join("corrupt_with_bloom.tar.gz");
    repack_backup_archive_with_overrides(
        &valid_backup,
        &corrupt_with_bloom,
        Some(&zero_base),
        false,
        None,
        None,
    );
    let restore_a = tempfile::tempdir().expect("restore a");
    restore_backup(
        corrupt_with_bloom.to_str().unwrap(),
        restore_a.path().to_str().unwrap(),
    )
    .expect("unpack archive with matching CRCs");
    assert!(
        OmniKV::open(
            restore_a.path().join("manifest.json").to_str().unwrap(),
            restore_a.path().join("wal.bin").to_str().unwrap(),
        )
        .is_err(),
        "zero-filled 4096B generated base with populated bloom must fail closed on open"
    );

    // Case B: Zero-filled 4096B base with `wal_checkpoint_seq > 0` (GC base)
    // even when `base.bloom` is omitted must NOT be upgraded.
    let corrupt_gc_base = source_dir.path().join("corrupt_gc_base.tar.gz");
    repack_backup_archive_with_overrides(
        &valid_backup,
        &corrupt_gc_base,
        Some(&zero_base),
        true,
        Some(5),
        None,
    );
    let restore_b = tempfile::tempdir().expect("restore b");
    restore_backup(
        corrupt_gc_base.to_str().unwrap(),
        restore_b.path().to_str().unwrap(),
    )
    .expect("unpack gc archive");
    assert!(
        OmniKV::open(
            restore_b.path().join("manifest.json").to_str().unwrap(),
            restore_b.path().join("wal.bin").to_str().unwrap(),
        )
        .is_err(),
        "zero-filled 4096B base with non-zero wal_checkpoint_seq must fail closed on open"
    );
}
