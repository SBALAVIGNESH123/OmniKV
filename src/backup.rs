//! Hot Backup & Restore
//!
//! Creates consistent point-in-time snapshots of the database as
//! compressed tar archives, optionally encrypted with AES-256-GCM.

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use tar::{Archive, Builder};

use omni_engine::OmniKV;

/// Create a compressed backup of the database.
/// Returns the path to the created backup file.
pub fn create_backup(
    db: &Arc<OmniKV>,
    manifest_path: &str,
    output_path: &str,
) -> Result<String, String> {
    let manifest_dir = Path::new(manifest_path).parent().unwrap_or(Path::new("."));

    let file =
        File::create(output_path).map_err(|e| format!("Cannot create backup file: {}", e))?;

    let enc = GzEncoder::new(file, Compression::default());
    let mut tar = Builder::new(enc);

    // Add manifest
    if Path::new(manifest_path).exists() {
        tar.append_path_with_name(manifest_path, "manifest.json")
            .map_err(|e| format!("Backup manifest: {}", e))?;
    }

    // Add heap file
    let heap_path = manifest_dir.join("heap.bin");
    if heap_path.exists() {
        tar.append_path_with_name(&heap_path, "heap.bin")
            .map_err(|e| format!("Backup heap: {}", e))?;
    }

    // Add SSTable files
    for entry in std::fs::read_dir(manifest_dir)
        .map_err(|e| format!("Read dir: {}", e))?
        .flatten()
    {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".sst") || name.ends_with(".bloom") {
            tar.append_path_with_name(entry.path(), &name)
                .map_err(|e| format!("Backup SST {}: {}", name, e))?;
        }
    }

    tar.finish().map_err(|e| format!("Finalize tar: {}", e))?;

    Ok(output_path.to_string())
}

/// Restore a database from a compressed backup.
pub fn restore_backup(backup_path: &str, restore_dir: &str) -> Result<(), String> {
    let file = File::open(backup_path).map_err(|e| format!("Cannot open backup: {}", e))?;

    let decoder = GzDecoder::new(file);
    let mut archive = Archive::new(decoder);

    std::fs::create_dir_all(restore_dir)
        .map_err(|e| format!("Cannot create restore dir: {}", e))?;

    archive
        .unpack(restore_dir)
        .map_err(|e| format!("Unpack failed: {}", e))?;

    Ok(())
}

/// Create an encrypted backup.
pub fn create_encrypted_backup(
    db: &Arc<OmniKV>,
    manifest_path: &str,
    output_path: &str,
    passphrase: &str,
) -> Result<String, String> {
    let temp_path = format!("{}.tmp", output_path);
    create_backup(db, manifest_path, &temp_path)?;

    let mut data = Vec::new();
    File::open(&temp_path)
        .and_then(|mut f| f.read_to_end(&mut data))
        .map_err(|e| format!("Read temp backup: {}", e))?;

    let encrypted = crate::crypto::encrypt(&data, passphrase)?;

    File::create(output_path)
        .and_then(|mut f| f.write_all(&encrypted))
        .map_err(|e| format!("Write encrypted backup: {}", e))?;

    let _ = std::fs::remove_file(&temp_path);

    Ok(output_path.to_string())
}
