//! Backup and restore support.
//!
//! The backup format is a gzip-compressed tar archive containing a rewritten
//! database manifest, the storage files referenced by that manifest, an optional
//! WAL, and a small metadata file used for restore-time verification.

use crate::{Manifest, OmniKV};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use tar::{Archive, Builder, Header};

const BACKUP_FORMAT_VERSION: u32 = 1;
const MANIFEST_ENTRY: &str = "manifest.json";
const METADATA_ENTRY: &str = "omnikv-backup.json";
const HEAP_ENTRY: &str = "heap.bin";
const BASE_ENTRY: &str = "base.bin";
const BASE_BLOOM_ENTRY: &str = "base.bloom";
const WAL_ENTRY: &str = "wal.bin";

/// Metadata stored inside every OmniKV backup archive.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupMetadata {
    pub format_version: u32,
    pub created_at_unix_secs: u64,
    pub includes_wal: bool,
    pub source_manifest_path: String,
    pub source_wal_path: Option<String>,
    pub files: Vec<BackupFileMetadata>,
}

/// Per-file restore verification metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupFileMetadata {
    pub path: String,
    pub bytes: u64,
    pub crc32: u32,
}

/// Create a compressed backup of the database.
///
/// This compatibility wrapper preserves the original API. Prefer
/// [`create_backup_with_wal`] for production use so the restore directory can
/// receive a WAL file alongside the manifest and SSTable data.
pub fn create_backup(
    db: &Arc<OmniKV>,
    manifest_path: &str,
    output_path: &str,
) -> Result<String, String> {
    create_backup_internal(db, manifest_path, None, output_path)
}

/// Create a compressed backup that also includes the WAL file.
///
/// The function briefly takes the storage transition barrier, flushes the
/// active memtable to disk, syncs the heap, then archives the manifest-owned
/// files. This gives callers a quiesced, self-contained restore point.
pub fn create_backup_with_wal(
    db: &Arc<OmniKV>,
    manifest_path: &str,
    wal_path: &str,
    output_path: &str,
) -> Result<String, String> {
    create_backup_internal(db, manifest_path, Some(wal_path), output_path)
}

fn create_backup_internal(
    db: &Arc<OmniKV>,
    manifest_path: &str,
    wal_path: Option<&str>,
    output_path: &str,
) -> Result<String, String> {
    let _backup_guard = db
        .transition_guard
        .write()
        .map_err(|_| "storage transition lock poisoned while creating backup".to_string())?;

    db.compact_sstables()
        .map_err(|e| format!("Flush active memtable before backup: {e}"))?;
    db.sync_all()
        .map_err(|e| format!("Sync storage before backup: {e}"))?;

    let source_manifest =
        Manifest::load(manifest_path).map_err(|e| format!("Load manifest for backup: {e}"))?;
    let source_manifest_dir = Path::new(manifest_path)
        .parent()
        .unwrap_or_else(|| Path::new("."));

    let (backup_manifest, file_plan) =
        build_backup_plan(&source_manifest, source_manifest_dir, wal_path)?;

    let file = File::create(output_path).map_err(|e| format!("Cannot create backup file: {e}"))?;
    let encoder = GzEncoder::new(file, Compression::default());
    let mut tar = Builder::new(encoder);

    let mut metadata = BackupMetadata {
        format_version: BACKUP_FORMAT_VERSION,
        created_at_unix_secs: unix_timestamp_secs(),
        includes_wal: wal_path.is_some(),
        source_manifest_path: manifest_path.to_string(),
        source_wal_path: wal_path.map(ToOwned::to_owned),
        files: Vec::new(),
    };

    let manifest_bytes = serde_json::to_vec_pretty(&backup_manifest)
        .map_err(|e| format!("Serialize backup manifest: {e}"))?;
    append_bytes(&mut tar, MANIFEST_ENTRY, &manifest_bytes, &mut metadata)?;

    for (source, archive_path) in file_plan {
        append_existing_file(&mut tar, &source, &archive_path, &mut metadata)?;
    }

    let metadata_bytes = serde_json::to_vec_pretty(&metadata)
        .map_err(|e| format!("Serialize backup metadata: {e}"))?;
    append_bytes(&mut tar, METADATA_ENTRY, &metadata_bytes, &mut metadata)?;

    tar.finish()
        .map_err(|e| format!("Finalize backup archive: {e}"))?;
    let encoder = tar
        .into_inner()
        .map_err(|e| format!("Finalize backup tar stream: {e}"))?;
    encoder
        .finish()
        .map_err(|e| format!("Finalize compressed backup: {e}"))?;

    Ok(output_path.to_string())
}

fn build_backup_plan(
    source_manifest: &Manifest,
    source_manifest_dir: &Path,
    wal_path: Option<&str>,
) -> Result<(Manifest, Vec<(PathBuf, String)>), String> {
    let mut plan = Vec::new();
    let mut backup_manifest = source_manifest.clone();

    backup_manifest.heap_path = HEAP_ENTRY.to_string();
    backup_manifest.base_path = BASE_ENTRY.to_string();

    plan.push((
        resolve_source_path(source_manifest_dir, &source_manifest.heap_path)?,
        HEAP_ENTRY.to_string(),
    ));
    plan.push((
        resolve_source_path(source_manifest_dir, &source_manifest.base_path)?,
        BASE_ENTRY.to_string(),
    ));

    let source_base_bloom = bloom_path_for_base(&source_manifest.base_path);
    if let Ok(path) = resolve_source_path(source_manifest_dir, &source_base_bloom)
        && path.exists()
    {
        plan.push((path, BASE_BLOOM_ENTRY.to_string()));
    }

    backup_manifest.sstables = source_manifest
        .sstables
        .iter()
        .enumerate()
        .map(|(idx, source_sst)| {
            let entry = format!("sstables/l0_{idx:06}.sst");
            plan.push((
                resolve_source_path(source_manifest_dir, source_sst)?,
                entry.clone(),
            ));

            let source_bloom = bloom_path_for_sstable(source_sst);
            if let Ok(path) = resolve_source_path(source_manifest_dir, &source_bloom)
                && path.exists()
            {
                plan.push((path, bloom_path_for_sstable(&entry)));
            }

            Ok(entry)
        })
        .collect::<Result<Vec<_>, String>>()?;

    backup_manifest.l1_sstables = source_manifest
        .l1_sstables
        .iter()
        .enumerate()
        .map(|(idx, source_sst)| {
            let entry = format!("sstables/l1_{idx:06}.sst");
            plan.push((
                resolve_source_path(source_manifest_dir, source_sst)?,
                entry.clone(),
            ));

            let source_bloom = bloom_path_for_sstable(source_sst);
            if let Ok(path) = resolve_source_path(source_manifest_dir, &source_bloom)
                && path.exists()
            {
                plan.push((path, bloom_path_for_sstable(&entry)));
            }

            Ok(entry)
        })
        .collect::<Result<Vec<_>, String>>()?;

    if let Some(wal_path) = wal_path {
        plan.push((
            resolve_source_path(source_manifest_dir, wal_path)?,
            WAL_ENTRY.to_string(),
        ));
    }

    Ok((backup_manifest, plan))
}

/// Restore a database from a compressed backup.
pub fn restore_backup(backup_path: &str, restore_dir: &str) -> Result<(), String> {
    let file = File::open(backup_path).map_err(|e| format!("Cannot open backup: {e}"))?;
    restore_backup_reader(file, restore_dir)
}

/// Create an encrypted backup.
///
/// This compatibility wrapper preserves the original API. Prefer
/// [`create_encrypted_backup_with_wal`] when creating a restore point intended
/// to be opened directly.
pub fn create_encrypted_backup(
    db: &Arc<OmniKV>,
    manifest_path: &str,
    output_path: &str,
    passphrase: &str,
) -> Result<String, String> {
    create_encrypted_backup_internal(db, manifest_path, None, output_path, passphrase)
}

/// Create an encrypted backup that includes the WAL file.
pub fn create_encrypted_backup_with_wal(
    db: &Arc<OmniKV>,
    manifest_path: &str,
    wal_path: &str,
    output_path: &str,
    passphrase: &str,
) -> Result<String, String> {
    create_encrypted_backup_internal(db, manifest_path, Some(wal_path), output_path, passphrase)
}

fn create_encrypted_backup_internal(
    db: &Arc<OmniKV>,
    manifest_path: &str,
    wal_path: Option<&str>,
    output_path: &str,
    passphrase: &str,
) -> Result<String, String> {
    let temp_path = format!("{output_path}.tmp");
    create_backup_internal(db, manifest_path, wal_path, &temp_path)?;

    let mut data = Vec::new();
    File::open(&temp_path)
        .and_then(|mut f| f.read_to_end(&mut data))
        .map_err(|e| format!("Read temporary backup: {e}"))?;

    let encrypted = crate::crypto::encrypt(&data, passphrase)?;

    File::create(output_path)
        .and_then(|mut f| f.write_all(&encrypted))
        .map_err(|e| format!("Write encrypted backup: {e}"))?;

    let _ = std::fs::remove_file(&temp_path);

    Ok(output_path.to_string())
}

/// Restore a database from an encrypted backup.
pub fn restore_encrypted_backup(
    encrypted_backup_path: &str,
    restore_dir: &str,
    passphrase: &str,
) -> Result<(), String> {
    let mut encrypted = Vec::new();
    File::open(encrypted_backup_path)
        .and_then(|mut f| f.read_to_end(&mut encrypted))
        .map_err(|e| format!("Read encrypted backup: {e}"))?;

    let plaintext = crate::crypto::decrypt(&encrypted, passphrase)?;
    restore_backup_reader(Cursor::new(plaintext), restore_dir)
}

fn restore_backup_reader<R: Read>(reader: R, restore_dir: &str) -> Result<(), String> {
    std::fs::create_dir_all(restore_dir)
        .map_err(|e| format!("Cannot create restore directory: {e}"))?;
    let restore_root = Path::new(restore_dir)
        .canonicalize()
        .map_err(|e| format!("Canonicalize restore directory: {e}"))?;

    let decoder = GzDecoder::new(reader);
    let mut archive = Archive::new(decoder);

    for entry in archive
        .entries()
        .map_err(|e| format!("Read backup archive: {e}"))?
    {
        let mut entry = entry.map_err(|e| format!("Read backup entry: {e}"))?;
        let entry_path = entry
            .path()
            .map_err(|e| format!("Read backup entry path: {e}"))?
            .into_owned();
        let safe_path = validate_archive_path(&entry_path)?;
        let output_path = restore_root.join(safe_path);

        if entry.header().entry_type().is_dir() {
            std::fs::create_dir_all(&output_path)
                .map_err(|e| format!("Create restore directory entry: {e}"))?;
            continue;
        }

        if !entry.header().entry_type().is_file() {
            return Err(format!(
                "Unsupported backup entry type for {}",
                entry_path.display()
            ));
        }

        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Create restore parent directory: {e}"))?;
        }
        entry
            .unpack(&output_path)
            .map_err(|e| format!("Unpack {}: {e}", entry_path.display()))?;
    }

    verify_restored_metadata(&restore_root)?;
    rewrite_restored_manifest(&restore_root)?;

    Ok(())
}

fn append_existing_file<W: Write>(
    tar: &mut Builder<W>,
    source_path: &Path,
    archive_path: &str,
    metadata: &mut BackupMetadata,
) -> Result<(), String> {
    let bytes = std::fs::read(source_path)
        .map_err(|e| format!("Read backup source {}: {e}", source_path.display()))?;
    append_bytes(tar, archive_path, &bytes, metadata)
}

fn append_bytes<W: Write>(
    tar: &mut Builder<W>,
    archive_path: &str,
    bytes: &[u8],
    metadata: &mut BackupMetadata,
) -> Result<(), String> {
    validate_archive_path(Path::new(archive_path))?;

    let mut header = Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_mode(0o600);
    header.set_mtime(unix_timestamp_secs());
    header.set_cksum();

    tar.append_data(&mut header, archive_path, Cursor::new(bytes))
        .map_err(|e| format!("Append {archive_path} to backup: {e}"))?;

    metadata.files.push(BackupFileMetadata {
        path: archive_path.to_string(),
        bytes: bytes.len() as u64,
        crc32: crc32(bytes),
    });

    Ok(())
}

fn verify_restored_metadata(restore_root: &Path) -> Result<(), String> {
    let metadata_path = restore_root.join(METADATA_ENTRY);
    if !metadata_path.exists() {
        return Ok(());
    }

    let metadata_bytes =
        std::fs::read(&metadata_path).map_err(|e| format!("Read backup metadata: {e}"))?;
    let metadata: BackupMetadata = serde_json::from_slice(&metadata_bytes)
        .map_err(|e| format!("Parse backup metadata: {e}"))?;

    if metadata.format_version != BACKUP_FORMAT_VERSION {
        return Err(format!(
            "Unsupported backup format version {}",
            metadata.format_version
        ));
    }

    for file in metadata.files {
        if file.path == METADATA_ENTRY {
            continue;
        }

        let relative = validate_archive_path(Path::new(&file.path))?;
        let restored_path = restore_root.join(relative);
        let bytes = std::fs::read(&restored_path)
            .map_err(|e| format!("Read restored file {}: {e}", file.path))?;

        if bytes.len() as u64 != file.bytes {
            return Err(format!(
                "Restored file {} has {} bytes, expected {}",
                file.path,
                bytes.len(),
                file.bytes
            ));
        }

        let actual_crc = crc32(&bytes);
        if actual_crc != file.crc32 {
            return Err(format!(
                "Restored file {} has crc32 {}, expected {}",
                file.path, actual_crc, file.crc32
            ));
        }
    }

    Ok(())
}

fn rewrite_restored_manifest(restore_root: &Path) -> Result<(), String> {
    let manifest_path = restore_root.join(MANIFEST_ENTRY);
    let manifest_path_str = manifest_path
        .to_str()
        .ok_or_else(|| "Restore manifest path is not valid UTF-8".to_string())?;
    let mut manifest =
        Manifest::load(manifest_path_str).map_err(|e| format!("Load restored manifest: {e}"))?;

    manifest.heap_path = rebase_restored_path(restore_root, &manifest.heap_path)?;
    manifest.base_path = rebase_restored_path(restore_root, &manifest.base_path)?;
    manifest.sstables = manifest
        .sstables
        .iter()
        .map(|path| rebase_restored_path(restore_root, path))
        .collect::<Result<Vec<_>, String>>()?;
    manifest.l1_sstables = manifest
        .l1_sstables
        .iter()
        .map(|path| rebase_restored_path(restore_root, path))
        .collect::<Result<Vec<_>, String>>()?;

    manifest
        .save(manifest_path_str)
        .map_err(|e| format!("Rewrite restored manifest: {e}"))?;

    Ok(())
}

fn rebase_restored_path(restore_root: &Path, stored_path: &str) -> Result<String, String> {
    let path = Path::new(stored_path);
    let relative = if path.is_absolute() {
        path.file_name()
            .map(PathBuf::from)
            .ok_or_else(|| format!("Cannot rebase absolute restored path {stored_path}"))?
    } else {
        validate_archive_path(path)?
    };

    restore_root
        .join(relative)
        .to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| "Restored storage path is not valid UTF-8".to_string())
}

fn validate_archive_path(path: &Path) -> Result<PathBuf, String> {
    let mut safe = PathBuf::new();

    for component in path.components() {
        match component {
            Component::Normal(part) => safe.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(format!("Unsafe backup archive path: {}", path.display()));
            }
        }
    }

    if safe.as_os_str().is_empty() {
        return Err("Backup archive path cannot be empty".to_string());
    }

    Ok(safe)
}

fn resolve_source_path(manifest_dir: &Path, stored_path: &str) -> Result<PathBuf, String> {
    let path = PathBuf::from(stored_path);
    if path.exists() || path.is_absolute() {
        return Ok(path);
    }

    let relative_to_manifest = manifest_dir.join(stored_path);
    if relative_to_manifest.exists() {
        return Ok(relative_to_manifest);
    }

    Err(format!("Backup source path does not exist: {stored_path}"))
}

fn bloom_path_for_base(path: &str) -> String {
    path.replace(".bin", ".bloom")
}

fn bloom_path_for_sstable(path: &str) -> String {
    path.replace(".sst", ".bloom")
}

fn unix_timestamp_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}
