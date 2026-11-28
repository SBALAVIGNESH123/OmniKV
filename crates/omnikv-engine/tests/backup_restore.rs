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

fn read_archive_entry(archive_path: &std::path::Path, target_path: &str) -> Vec<u8> {
    use flate2::read::GzDecoder;
    use std::io::Read;
    use tar::Archive;

    let file = std::fs::File::open(archive_path).expect("open archive for inspection");
    let mut archive = Archive::new(GzDecoder::new(file));
    for entry in archive.entries().expect("inspect archive entries") {
        let mut entry = entry.expect("inspect entry");
        let path = entry
            .path()
            .expect("inspect entry path")
            .to_string_lossy()
            .replace('\\', "/");
        if path == target_path {
            let mut bytes = Vec::new();
            entry.read_to_end(&mut bytes).expect("read archive entry");
            return bytes;
        }
    }
    panic!("archive missing expected entry {target_path}");
}

fn assert_restored_legacy_archive_runs_gc_and_l1_to_l2(legacy_archive: &std::path::Path) {
    for op in ["gc", "l1_to_l2"] {
        let restore_dir = tempfile::tempdir().expect("legacy restore dir");
        restore_backup(
            legacy_archive.to_str().unwrap(),
            restore_dir.path().to_str().unwrap(),
        )
        .expect("restore pre-v2 archive");

        let upgraded_base =
            std::fs::read(restore_dir.path().join("base.bin")).expect("read restored base");
        assert_eq!(upgraded_base.len(), 24);
        assert_eq!(&upgraded_base[16..24], b"OMNIV2**");

        let restored = OmniKV::open(
            restore_dir.path().join("manifest.json").to_str().unwrap(),
            restore_dir.path().join("wal.bin").to_str().unwrap(),
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

    // 1. Fresh backup of the pre-v2 DB writes a valid 24-byte `OMNIV2**` `base.bin`.
    let fresh_backup = source_dir.path().join("fresh_legacy_backup.tar.gz");
    create_backup_with_wal(&db, &manifest, &wal, fresh_backup.to_str().unwrap())
        .expect("create backup from pre-v2 DB");
    drop(db);

    let archived_base = read_archive_entry(&fresh_backup, "base.bin");
    assert_eq!(archived_base.len(), 24);
    assert_eq!(&archived_base[16..24], b"OMNIV2**");
    assert_restored_legacy_archive_runs_gc_and_l1_to_l2(&fresh_backup);

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
        assert_restored_legacy_archive_runs_gc_and_l1_to_l2(&legacy_archive);
    }
}

#[test]
fn already_restored_legacy_empty_backup_upgrades_on_open_and_survives_flush_backup_and_reopen() {
    let dir = tempfile::tempdir().expect("pre-restored legacy empty backup dir");
    let manifest_path = dir.path().join("manifest.json");
    let base_path = dir.path().join("base.bin");
    let heap_path = dir.path().join("heap.bin");
    let wal_path = dir.path().join("wal.bin");

    std::fs::write(&base_path, vec![0u8; 4096]).expect("write legacy 4096B zero base.bin");
    std::fs::write(&heap_path, b"").expect("write empty heap.bin");
    let manifest = omni_engine::Manifest {
        format_version: omni_engine::MANIFEST_FORMAT_VERSION,
        heap_path: heap_path.to_string_lossy().to_string(),
        base_path: base_path.to_string_lossy().to_string(),
        sstables: Vec::new(),
        l1_sstables: Vec::new(),
        max_seq: 0,
        wal_checkpoint_seq: 0,
        read_seq_floor: 0,
    };
    manifest
        .save(manifest_path.to_str().unwrap())
        .expect("save legacy empty restored manifest");

    let db = OmniKV::open(manifest_path.to_str().unwrap(), wal_path.to_str().unwrap())
        .expect("open already-restored empty pre-v2 backup");
    let upgraded_base = std::fs::read(&base_path).expect("read upgraded base.bin");
    assert_eq!(upgraded_base.len(), 24);
    assert_eq!(&upgraded_base[16..24], b"OMNIV2**");

    // Committing and flushing to L0 must NOT self-brick live reads, backup,
    // compaction, GC, or reopen when `manifest.sstables` becomes non-empty.
    put_keys(&db, "post_open", 8);
    db.compact_sstables().expect("first L0 flush must succeed");
    assert_eq!(
        db.scan("post_open:", "post_open:~", db.get_seq())
            .expect("scan after first L0 flush")
            .len(),
        8
    );

    let backup_out = dir.path().join("after_flush.tar.gz");
    create_backup_with_wal(
        &db,
        manifest_path.to_str().unwrap(),
        wal_path.to_str().unwrap(),
        backup_out.to_str().unwrap(),
    )
    .expect("backup after first L0 flush must succeed");
    drop(db);

    let reopened = OmniKV::open(manifest_path.to_str().unwrap(), wal_path.to_str().unwrap())
        .expect("reopen after first L0 flush must not self-brick");
    reopened.compact_l0_to_l1().expect("L0->L1 after reopen");
    reopened.compact_l1_to_l2().expect("L1->L2 after reopen");
    reopened.run_garbage_collection().expect("GC after reopen");
    verify_keys(&reopened, "post_open", 8);
}

#[test]
fn create_backup_rejects_corrupt_source_tables_without_leaving_partial_archive() {
    let source_dir = tempfile::tempdir().expect("corrupt backup source dir");
    let (db, manifest_str, wal_str) = open_db(&source_dir);

    put_keys(&db, "live", 8);
    db.compact_sstables().expect("flush L0");

    let corrupt_file = source_dir.path().join("corrupt_table.sst");
    std::fs::write(&corrupt_file, vec![0xFF; 64]).expect("write corrupt table");

    let orig_manifest = omni_engine::Manifest::load(&manifest_str).expect("load orig manifest");

    for (label, mutate_l0) in [("corrupt_base", false), ("corrupt_l0", true)] {
        let mut tampered = orig_manifest.clone();
        if mutate_l0 {
            tampered.sstables[0] = corrupt_file.to_string_lossy().to_string();
        } else {
            tampered.base_path = corrupt_file.to_string_lossy().to_string();
        }
        tampered
            .save(&manifest_str)
            .expect("save tampered manifest");

        let out_path = source_dir.path().join(format!("{label}.tar.gz"));
        let err = create_backup_with_wal(&db, &manifest_str, &wal_str, out_path.to_str().unwrap())
            .expect_err("backup must reject corrupt source SSTable");
        assert!(
            err.contains("Validate source"),
            "{label}: expected source SSTable validation error, got {err}"
        );
        assert!(
            !out_path.exists(),
            "{label}: failed backup must not leave partial archive at output path"
        );
        let staged_leftovers: Vec<String> = std::fs::read_dir(source_dir.path())
            .expect("read source dir")
            .filter_map(|entry| {
                let name = entry.ok()?.file_name().to_string_lossy().to_string();
                name.starts_with(&format!("{label}.tar.gz."))
                    .then_some(name)
            })
            .collect();
        assert_eq!(
            staged_leftovers,
            Vec::<String>::new(),
            "{label}: failed backup must clean up temporary staging files"
        );
    }
    orig_manifest
        .save(&manifest_str)
        .expect("restore original manifest");
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
