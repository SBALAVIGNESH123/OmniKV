use flate2::Compression;
use flate2::write::GzEncoder;
use omni_engine::backup::{
    create_backup_with_wal, create_encrypted_backup_with_wal, restore_backup,
    restore_encrypted_backup, restore_encrypted_legacy_backup_allowing_zero_base,
    restore_legacy_backup_allowing_zero_base,
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

#[derive(Default)]
struct RepackOverrides<'a> {
    replace_base: Option<&'a [u8]>,
    replace_base_bloom: Option<&'a [u8]>,
    omit_base_bloom: bool,
    omit_metadata: bool,
    force_legacy_format: bool,
    force_wal_checkpoint_seq: Option<u64>,
    force_read_seq_floor: Option<u64>,
}

fn append_repacked_entry<W: std::io::Write>(tar: &mut Builder<W>, path: &str, bytes: &[u8]) {
    let mut header = Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_mode(0o600);
    header.set_cksum();
    tar.append_data(&mut header, path, bytes)
        .expect("append repacked entry");
}

fn repack_backup_archive_with_overrides(
    src_archive: &std::path::Path,
    dst_archive: &std::path::Path,
    overrides: &RepackOverrides<'_>,
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
        if overrides.omit_base_bloom && path == "base.bloom" {
            continue;
        }
        if path == "base.bloom"
            && let Some(replacement) = overrides.replace_base_bloom
        {
            replacement.clone_into(&mut bytes);
        }
        if path == "base.bin"
            && let Some(replacement) = overrides.replace_base
        {
            replacement.clone_into(&mut bytes);
        }
        if path == "manifest.json"
            && (overrides.force_wal_checkpoint_seq.is_some()
                || overrides.force_read_seq_floor.is_some())
        {
            let mut manifest: omni_engine::Manifest =
                serde_json::from_slice(&bytes).expect("parse archive manifest");
            if let Some(checkpoint_seq) = overrides.force_wal_checkpoint_seq {
                manifest.wal_checkpoint_seq = checkpoint_seq;
            }
            if let Some(read_seq_floor) = overrides.force_read_seq_floor {
                manifest.read_seq_floor = read_seq_floor;
            }
            bytes = serde_json::to_vec_pretty(&manifest).expect("serialize archive manifest");
        }
        entries.push((path, bytes));
    }

    let mut metadata = orig_meta.expect("src metadata");
    if overrides.force_legacy_format {
        metadata.base_format_v2 = false;
    }
    metadata.files.clear();

    let dst_file = std::fs::File::create(dst_archive).expect("create dst archive");
    let mut tar = Builder::new(GzEncoder::new(dst_file, Compression::default()));
    for (path, bytes) in &entries {
        append_repacked_entry(&mut tar, path, bytes);
        metadata
            .files
            .push(omni_engine::backup::BackupFileMetadata {
                path: path.clone(),
                bytes: bytes.len() as u64,
                crc32: crc32fast::hash(bytes),
            });
    }
    if !overrides.omit_metadata {
        let meta_bytes = serde_json::to_vec_pretty(&metadata).expect("serialize repacked metadata");
        append_repacked_entry(&mut tar, "omnikv-backup.json", &meta_bytes);
    }

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

fn assert_restored_legacy_archive_runs_gc_and_l1_to_l2(
    legacy_archive: &std::path::Path,
    use_legacy_opt_in: bool,
) {
    for op in ["gc", "l1_to_l2"] {
        let restore_dir = tempfile::tempdir().expect("legacy restore dir");
        if use_legacy_opt_in {
            restore_legacy_backup_allowing_zero_base(
                legacy_archive.to_str().unwrap(),
                restore_dir.path().to_str().unwrap(),
            )
            .expect("restore bloom-less pre-v2 archive via explicit opt-in");
        } else {
            restore_backup(
                legacy_archive.to_str().unwrap(),
                restore_dir.path().to_str().unwrap(),
            )
            .expect("restore pre-v2 archive");
        }

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

fn assert_zero_base_untouched_and_open_fails_base_validation(
    restore_dir: &std::path::Path,
    base_rel: &str,
    reason: &str,
) {
    let base_bytes = std::fs::read(restore_dir.join(base_rel)).expect("read restored zero base");
    assert_eq!(
        base_bytes.len(),
        4096,
        "{reason}: restored {base_rel} must remain 4096 bytes untouched"
    );
    assert!(
        base_bytes.iter().all(|&b| b == 0),
        "{reason}: restored {base_rel} must remain all-zero bytes"
    );
    let err = OmniKV::open(
        restore_dir.join("manifest.json").to_str().unwrap(),
        restore_dir.join("wal.bin").to_str().unwrap(),
    )
    .err()
    .expect(reason);
    let msg = err.to_string();
    assert!(
        msg.contains("corrupt base SSTable"),
        "{reason}: expected base SSTable validation error, got {msg}"
    );
}

fn assert_corrupt_archive_fails_closed_on_open(
    archive: &std::path::Path,
    also_reject_legacy_opt_in: bool,
    reason: &str,
) {
    let default_dir = tempfile::tempdir().expect("default corrupt restore dir");
    restore_backup(
        archive.to_str().unwrap(),
        default_dir.path().to_str().unwrap(),
    )
    .expect("unpack archive with matching CRCs");
    assert_zero_base_untouched_and_open_fails_base_validation(
        default_dir.path(),
        "base.bin",
        &format!("default restore: {reason}"),
    );

    if also_reject_legacy_opt_in {
        let opt_in_dir = tempfile::tempdir().expect("opt-in corrupt restore dir");
        restore_legacy_backup_allowing_zero_base(
            archive.to_str().unwrap(),
            opt_in_dir.path().to_str().unwrap(),
        )
        .expect("unpack archive under legacy opt-in");
        assert_zero_base_untouched_and_open_fails_base_validation(
            opt_in_dir.path(),
            "base.bin",
            &format!("legacy opt-in restore: {reason}"),
        );
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

    // 1. Fresh backup of the pre-v2 DB writes a valid 24-byte `OMNIV2**` `base.bin`
    //    and records `base_format_v2: true`.
    let fresh_backup = source_dir.path().join("fresh_legacy_backup.tar.gz");
    create_backup_with_wal(&db, &manifest, &wal, fresh_backup.to_str().unwrap())
        .expect("create backup from pre-v2 DB");
    drop(db);

    let archived_base = read_archive_entry(&fresh_backup, "base.bin");
    assert_eq!(archived_base.len(), 24);
    assert_eq!(&archived_base[16..24], b"OMNIV2**");
    let fresh_meta: omni_engine::backup::BackupMetadata =
        serde_json::from_slice(&read_archive_entry(&fresh_backup, "omnikv-backup.json"))
            .expect("parse fresh metadata");
    assert!(fresh_meta.base_format_v2);
    assert_restored_legacy_archive_runs_gc_and_l1_to_l2(&fresh_backup, false);

    // 2. Pre-existing pre-v2 archive (`base_format_v2: false`) with positive
    //    empty `base.bloom` evidence auto-upgrades via default `restore_backup`.
    let zero_base = vec![0u8; 4096];
    let legacy_with_empty_bloom = source_dir.path().join("legacy_empty_bloom.tar.gz");
    repack_backup_archive_with_overrides(
        &fresh_backup,
        &legacy_with_empty_bloom,
        &RepackOverrides {
            replace_base: Some(&zero_base),
            force_legacy_format: true,
            ..RepackOverrides::default()
        },
    );
    assert_restored_legacy_archive_runs_gc_and_l1_to_l2(&legacy_with_empty_bloom, false);

    // 3. Pre-v2 archives WITHOUT `base.bloom` lack positive empty-base metadata
    //    when L0/L1 tables exist: default `restore_backup` fails closed on open
    //    (even if a leftover empty `base.bloom` already sits in `restore_dir`),
    //    while explicit opt-in restores both plain and encrypted archives.
    for (idx, read_seq_floor) in [None, Some(10)].into_iter().enumerate() {
        let legacy_archive = source_dir
            .path()
            .join(format!("legacy_bloomless_{idx}.tar.gz"));
        repack_backup_archive_with_overrides(
            &fresh_backup,
            &legacy_archive,
            &RepackOverrides {
                replace_base: Some(&zero_base),
                omit_base_bloom: true,
                force_legacy_format: true,
                force_read_seq_floor: read_seq_floor,
                ..RepackOverrides::default()
            },
        );
        assert_corrupt_archive_fails_closed_on_open(
            &legacy_archive,
            false,
            "bloom-less non-empty pre-v2 archive must not auto-upgrade without opt-in",
        );
        assert_restored_legacy_archive_runs_gc_and_l1_to_l2(&legacy_archive, true);
    }

    assert_leftover_bloom_and_encrypted_legacy_restore(
        source_dir.path(),
        &source_dir.path().join("legacy_bloomless_0.tar.gz"),
        &legacy_with_empty_bloom,
    );
}

fn assert_leftover_bloom_and_encrypted_legacy_restore(
    scratch_dir: &std::path::Path,
    bloomless_archive: &std::path::Path,
    verified_pre_v2_archive: &std::path::Path,
) {
    // 1. Leftover empty `base.bloom` in `restore_dir` must not bypass `base_bloom_verified`.
    let dirty_restore_dir = tempfile::tempdir().expect("dirty restore dir");
    omni_engine::BloomFilter::new(100)
        .save(
            dirty_restore_dir
                .path()
                .join("base.bloom")
                .to_str()
                .unwrap(),
        )
        .expect("plant leftover empty base.bloom");
    restore_backup(
        bloomless_archive.to_str().unwrap(),
        dirty_restore_dir.path().to_str().unwrap(),
    )
    .expect("unpack into dirty restore dir");
    assert_zero_base_untouched_and_open_fails_base_validation(
        dirty_restore_dir.path(),
        "base.bin",
        "untracked leftover empty base.bloom must not authorize silent zero-base upgrade",
    );

    // 2. Leftover `omnikv-backup.json` + empty `base.bloom` from a prior pre-v2
    //    restore must NOT be inherited by a later metadata-less, bloom-less archive.
    let retained_meta_dir = tempfile::tempdir().expect("retained metadata restore dir");
    let prev_meta = read_archive_entry(verified_pre_v2_archive, "omnikv-backup.json");
    let prev_bloom = read_archive_entry(verified_pre_v2_archive, "base.bloom");
    std::fs::write(
        retained_meta_dir.path().join("omnikv-backup.json"),
        prev_meta,
    )
    .expect("plant retained pre-v2 metadata");
    std::fs::write(retained_meta_dir.path().join("base.bloom"), prev_bloom)
        .expect("plant retained empty base.bloom");
    let metadata_less_bloomless = scratch_dir.join("metadata_less_bloomless.tar.gz");
    repack_backup_archive_with_overrides(
        bloomless_archive,
        &metadata_less_bloomless,
        &RepackOverrides {
            omit_base_bloom: true,
            omit_metadata: true,
            ..RepackOverrides::default()
        },
    );
    restore_backup(
        metadata_less_bloomless.to_str().unwrap(),
        retained_meta_dir.path().to_str().unwrap(),
    )
    .expect("unpack metadata-less archive into dirty dir");
    assert_zero_base_untouched_and_open_fails_base_validation(
        retained_meta_dir.path(),
        "base.bin",
        "retained metadata and sidecar from prior restore must not qualify current archive",
    );

    assert_dirty_dir_opt_in_and_manifest_less_reject(scratch_dir, bloomless_archive);

    // 4. Encrypted bloom-less pre-v2 archive: default fails closed, opt-in restores.
    let encrypted_legacy = scratch_dir.join("legacy_bloomless.omnikv.enc");
    let plain_bytes = std::fs::read(bloomless_archive).expect("read bloomless archive");
    let enc_bytes = omni_engine::crypto::encrypt(&plain_bytes, "legacy-pass").expect("encrypt");
    std::fs::write(&encrypted_legacy, enc_bytes).expect("write encrypted legacy archive");

    let enc_default_dir = tempfile::tempdir().expect("enc default dir");
    restore_encrypted_backup(
        encrypted_legacy.to_str().unwrap(),
        enc_default_dir.path().to_str().unwrap(),
        "legacy-pass",
    )
    .expect("unpack encrypted legacy archive");
    assert_zero_base_untouched_and_open_fails_base_validation(
        enc_default_dir.path(),
        "base.bin",
        "default encrypted restore must fail closed on bloom-less pre-v2 archive",
    );

    let enc_opt_in_dir = tempfile::tempdir().expect("enc opt-in dir");
    restore_encrypted_legacy_backup_allowing_zero_base(
        encrypted_legacy.to_str().unwrap(),
        enc_opt_in_dir.path().to_str().unwrap(),
        "legacy-pass",
    )
    .expect("opt-in encrypted legacy restore");
    let restored = OmniKV::open(
        enc_opt_in_dir
            .path()
            .join("manifest.json")
            .to_str()
            .unwrap(),
        enc_opt_in_dir.path().join("wal.bin").to_str().unwrap(),
    )
    .expect("open opt-in encrypted legacy restore");
    verify_keys(&restored, "l1_data", 16);
    verify_keys(&restored, "l0_data", 16);
}

fn assert_dirty_dir_opt_in_and_manifest_less_reject(
    scratch_dir: &std::path::Path,
    bloomless_archive: &std::path::Path,
) {
    // Retained populated `base.bloom` from an earlier DB must NOT block
    // explicit opt-in restore of a bloom-less pre-v2 archive, and a
    // manifest-less archive must never silently reuse a retained manifest.
    let populated_dirty_dir = tempfile::tempdir().expect("populated dirty dir");
    let mut populated_bloom = omni_engine::BloomFilter::new(100);
    populated_bloom.add(0xDEAD_BEEF);
    populated_bloom
        .save(
            populated_dirty_dir
                .path()
                .join("base.bloom")
                .to_str()
                .unwrap(),
        )
        .expect("plant leftover populated base.bloom");
    restore_legacy_backup_allowing_zero_base(
        bloomless_archive.to_str().unwrap(),
        populated_dirty_dir.path().to_str().unwrap(),
    )
    .expect("opt-in restore ignores retained populated base.bloom");
    let recovered = OmniKV::open(
        populated_dirty_dir
            .path()
            .join("manifest.json")
            .to_str()
            .unwrap(),
        populated_dirty_dir.path().join("wal.bin").to_str().unwrap(),
    )
    .expect("open opt-in restore over dirty populated bloom");
    verify_keys(&recovered, "l1_data", 16);
    verify_keys(&recovered, "l0_data", 16);
    drop(recovered);

    let manifest_less_archive = scratch_dir.join("manifest_less.tar.gz");
    let file = std::fs::File::create(&manifest_less_archive).expect("create manifest-less tar");
    let mut tar = Builder::new(GzEncoder::new(file, Compression::default()));
    append_repacked_entry(&mut tar, "base.bin", &[0u8; 4096]);
    tar.finish().expect("finish manifest-less tar");
    tar.into_inner().expect("inner").finish().expect("gzip");
    let missing_manifest_err = restore_backup(
        manifest_less_archive.to_str().unwrap(),
        populated_dirty_dir.path().to_str().unwrap(),
    )
    .expect_err("manifest-less archive must not reuse retained manifest.json");
    assert!(
        missing_manifest_err.contains("missing manifest.json"),
        "expected missing manifest.json error, got {missing_manifest_err}"
    );
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

    // Also build a table whose Block 0 start record, sparse index, and footer
    // remain intact, while non-indexed Record 1 inside Block 0 is corrupted.
    let mut mid_block_corrupt =
        std::fs::read(&orig_manifest.sstables[0]).expect("read live L0 table");
    let (_, rec0_len) =
        omni_engine::OmniRecord::decode(&mid_block_corrupt).expect("decode indexed first record");
    mid_block_corrupt[rec0_len..rec0_len + 12].fill(0xFF);
    let mid_corrupt_file = source_dir.path().join("mid_block_corrupt.sst");
    std::fs::write(&mid_corrupt_file, &mid_block_corrupt).expect("write mid-block corrupt table");

    // Pre-create a healthy backup at `existing_out` to prove failed validation
    // never overwrites an existing good backup archive.
    let existing_out = source_dir.path().join("existing_good.tar.gz");
    create_backup_with_wal(&db, &manifest_str, &wal_str, existing_out.to_str().unwrap())
        .expect("create initial healthy backup");
    let existing_good_bytes = std::fs::read(&existing_out).expect("read healthy backup");

    for (label, mutate_l0, victim_path, target_out) in [
        ("corrupt_base", false, &corrupt_file, None),
        ("corrupt_l0", true, &corrupt_file, None),
        (
            "corrupt_l0_non_indexed_record",
            true,
            &mid_corrupt_file,
            Some(&existing_out),
        ),
        (
            "corrupt_base_non_indexed_record",
            false,
            &mid_corrupt_file,
            Some(&existing_out),
        ),
    ] {
        let mut tampered = orig_manifest.clone();
        if mutate_l0 {
            tampered.sstables[0] = victim_path.to_string_lossy().to_string();
        } else {
            tampered.base_path = victim_path.to_string_lossy().to_string();
        }
        tampered
            .save(&manifest_str)
            .expect("save tampered manifest");

        let fresh_out = source_dir.path().join(format!("{label}.tar.gz"));
        let out_path = target_out.unwrap_or(&fresh_out);
        let err = create_backup_with_wal(&db, &manifest_str, &wal_str, out_path.to_str().unwrap())
            .expect_err("backup must reject corrupt source SSTable");
        assert!(
            err.contains("Validate source"),
            "{label}: expected source SSTable validation error, got {err}"
        );
        if target_out.is_some() {
            assert_eq!(
                std::fs::read(out_path).expect("read preserved backup"),
                existing_good_bytes,
                "{label}: failed backup must not overwrite existing good archive"
            );
        } else {
            assert!(
                !out_path.exists(),
                "{label}: failed backup must not leave partial archive at output path"
            );
        }
        let prefix = format!("{}.", out_path.file_name().unwrap().to_string_lossy());
        let staged_leftovers: Vec<String> = std::fs::read_dir(source_dir.path())
            .expect("read source dir")
            .filter_map(|entry| {
                let name = entry.ok()?.file_name().to_string_lossy().to_string();
                name.starts_with(&prefix).then_some(name)
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
    let empty_bloom_json =
        serde_json::to_vec(&omni_engine::BloomFilter::new(100)).expect("serialize empty bloom");

    let cases: [(&str, RepackOverrides<'_>, bool); 5] = [
        (
            "modern_populated_bloom",
            RepackOverrides {
                replace_base: Some(&zero_base),
                ..RepackOverrides::default()
            },
            true,
        ),
        (
            "modern_missing_bloom",
            RepackOverrides {
                replace_base: Some(&zero_base),
                omit_base_bloom: true,
                ..RepackOverrides::default()
            },
            true,
        ),
        (
            "modern_empty_bloom",
            RepackOverrides {
                replace_base: Some(&zero_base),
                replace_base_bloom: Some(&empty_bloom_json),
                ..RepackOverrides::default()
            },
            true,
        ),
        (
            "legacy_nonzero_checkpoint",
            RepackOverrides {
                replace_base: Some(&zero_base),
                omit_base_bloom: true,
                force_legacy_format: true,
                force_wal_checkpoint_seq: Some(5),
                ..RepackOverrides::default()
            },
            true,
        ),
        (
            "metadata_stripped_empty_bloom",
            RepackOverrides {
                replace_base: Some(&zero_base),
                replace_base_bloom: Some(&empty_bloom_json),
                omit_metadata: true,
                ..RepackOverrides::default()
            },
            false,
        ),
    ];

    for (label, overrides, also_reject_opt_in) in cases {
        let archive = source_dir.path().join(format!("{label}.tar.gz"));
        repack_backup_archive_with_overrides(&valid_backup, &archive, &overrides);
        assert_corrupt_archive_fails_closed_on_open(
            &archive,
            also_reject_opt_in,
            &format!("{label}: zero-filled 4096B generated base must fail closed on open"),
        );
    }

    assert_encrypted_modern_missing_bloom_fails_closed(
        source_dir.path(),
        &source_dir.path().join("modern_missing_bloom.tar.gz"),
    );
    assert_nested_zero_base_not_authorized_by_top_level_bloom(
        source_dir.path(),
        &valid_backup,
        &zero_base,
        &empty_bloom_json,
    );
}

fn assert_nested_zero_base_not_authorized_by_top_level_bloom(
    scratch_dir: &std::path::Path,
    valid_backup: &std::path::Path,
    zero_base: &[u8],
    empty_bloom_json: &[u8],
) {
    use flate2::read::GzDecoder;
    use omni_engine::backup::BackupMetadata;
    use std::io::Read;
    use tar::Archive;

    // Build a pre-v2 archive containing verified top-level `base.bin` + empty
    // `base.bloom`, while `manifest.json` points to `nested/base.bin` alongside
    // an unverified empty `nested/base.bloom`. Top-level verification must NOT
    // authorize converting `nested/base.bin`.
    let src_file = std::fs::File::open(valid_backup).expect("open valid backup");
    let mut archive = Archive::new(GzDecoder::new(src_file));
    let mut entries: Vec<(String, Vec<u8>)> = Vec::new();
    let mut meta: Option<BackupMetadata> = None;
    for entry in archive.entries().expect("entries") {
        let mut entry = entry.expect("entry");
        let path = entry
            .path()
            .expect("path")
            .to_string_lossy()
            .replace('\\', "/");
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).expect("read");
        if path == "omnikv-backup.json" {
            meta = Some(serde_json::from_slice(&bytes).expect("parse meta"));
            continue;
        }
        if path == "manifest.json" {
            let mut m: omni_engine::Manifest = serde_json::from_slice(&bytes).expect("manifest");
            m.base_path = "nested/base.bin".to_string();
            bytes = serde_json::to_vec_pretty(&m).expect("serialize manifest");
        } else if path == "base.bloom" {
            empty_bloom_json.clone_into(&mut bytes);
        }
        entries.push((path, bytes));
    }

    let mut metadata = meta.expect("meta");
    metadata.base_format_v2 = false;
    metadata.files.clear();

    let nested_archive = scratch_dir.join("nested_zero_base.tar.gz");
    let dst_file = std::fs::File::create(&nested_archive).expect("create nested archive");
    let mut tar = Builder::new(GzEncoder::new(dst_file, Compression::default()));
    for (path, bytes) in &entries {
        append_repacked_entry(&mut tar, path, bytes);
        metadata
            .files
            .push(omni_engine::backup::BackupFileMetadata {
                path: path.clone(),
                bytes: bytes.len() as u64,
                crc32: crc32fast::hash(bytes),
            });
    }
    // Add unverified `nested/base.bin` (4096 zeros) and `nested/base.bloom` (empty).
    append_repacked_entry(&mut tar, "nested/base.bin", zero_base);
    append_repacked_entry(&mut tar, "nested/base.bloom", empty_bloom_json);
    let meta_bytes = serde_json::to_vec_pretty(&metadata).expect("serialize nested metadata");
    append_repacked_entry(&mut tar, "omnikv-backup.json", &meta_bytes);
    tar.finish().expect("finish tar");
    tar.into_inner().expect("inner").finish().expect("gzip");

    for opt_in in [false, true] {
        let restore_dir = tempfile::tempdir().expect("nested restore dir");
        if opt_in {
            restore_legacy_backup_allowing_zero_base(
                nested_archive.to_str().unwrap(),
                restore_dir.path().to_str().unwrap(),
            )
            .expect("restore nested archive under opt-in");
        } else {
            restore_backup(
                nested_archive.to_str().unwrap(),
                restore_dir.path().to_str().unwrap(),
            )
            .expect("restore nested archive");
        }
        assert_zero_base_untouched_and_open_fails_base_validation(
            restore_dir.path(),
            "nested/base.bin",
            &format!(
                "nested base must not be converted using top-level evidence (opt_in={opt_in})"
            ),
        );
    }
}

fn assert_encrypted_modern_missing_bloom_fails_closed(
    scratch_dir: &std::path::Path,
    modern_missing_archive: &std::path::Path,
) {
    let enc_modern_missing = scratch_dir.join("modern_missing_bloom.omnikv.enc");
    let raw_missing = std::fs::read(modern_missing_archive).expect("read modern_missing_bloom");
    let enc_missing =
        omni_engine::crypto::encrypt(&raw_missing, "modern-pass").expect("encrypt modern");
    std::fs::write(&enc_modern_missing, enc_missing).expect("write encrypted modern");
    for opt_in in [false, true] {
        let restore_dir = tempfile::tempdir().expect("enc modern restore dir");
        if opt_in {
            restore_encrypted_legacy_backup_allowing_zero_base(
                enc_modern_missing.to_str().unwrap(),
                restore_dir.path().to_str().unwrap(),
                "modern-pass",
            )
            .expect("unpack encrypted modern under opt-in");
        } else {
            restore_encrypted_backup(
                enc_modern_missing.to_str().unwrap(),
                restore_dir.path().to_str().unwrap(),
                "modern-pass",
            )
            .expect("unpack encrypted modern");
        }
        assert_zero_base_untouched_and_open_fails_base_validation(
            restore_dir.path(),
            "base.bin",
            &format!("encrypted modern missing-bloom archive must fail closed (opt_in={opt_in})"),
        );
    }
}

#[cfg(unix)]
#[test]
fn backup_preserves_existing_destination_mode_without_forcing_new_mode() {
    use std::os::unix::fs::PermissionsExt;

    let source_dir = tempfile::tempdir().expect("source dir");
    let (db, manifest, wal) = open_db(&source_dir);
    put_keys(&db, "perm", 4);

    // Reference mode produced by default process umask.
    let ref_file = source_dir.path().join("umask_ref");
    std::fs::File::create(&ref_file).expect("create reference file");
    let default_mode = std::fs::metadata(&ref_file)
        .expect("ref metadata")
        .permissions()
        .mode()
        & 0o777;

    // New plain and encrypted backups keep the default umask mode.
    let plain_path = source_dir.path().join("plain.tar.gz");
    let enc_path = source_dir.path().join("enc.omnikv.enc");
    create_backup_with_wal(&db, &manifest, &wal, plain_path.to_str().unwrap())
        .expect("create new plain backup");
    create_encrypted_backup_with_wal(
        &db,
        &manifest,
        &wal,
        enc_path.to_str().unwrap(),
        "passphrase",
    )
    .expect("create new encrypted backup");
    assert_eq!(
        std::fs::metadata(&plain_path)
            .expect("new plain metadata")
            .permissions()
            .mode()
            & 0o777,
        default_mode
    );
    assert_eq!(
        std::fs::metadata(&enc_path)
            .expect("new encrypted metadata")
            .permissions()
            .mode()
            & 0o777,
        default_mode
    );

    // Overwriting existing `0o600` destinations preserves `0o600` for both.
    std::fs::set_permissions(&plain_path, std::fs::Permissions::from_mode(0o600))
        .expect("chmod plain 0600");
    std::fs::set_permissions(&enc_path, std::fs::Permissions::from_mode(0o600))
        .expect("chmod encrypted 0600");

    create_backup_with_wal(&db, &manifest, &wal, plain_path.to_str().unwrap())
        .expect("overwrite 0600 plain backup");
    create_encrypted_backup_with_wal(
        &db,
        &manifest,
        &wal,
        enc_path.to_str().unwrap(),
        "passphrase",
    )
    .expect("overwrite 0600 encrypted backup");

    assert_eq!(
        std::fs::metadata(&plain_path)
            .expect("overwritten plain metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        std::fs::metadata(&enc_path)
            .expect("overwritten encrypted metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}
