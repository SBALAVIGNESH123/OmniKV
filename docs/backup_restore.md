# Backup and restore

OmniKV provides a library-level backup and restore contract for creating self-contained restore points.

The backup format is a gzip-compressed tar archive containing:

- `manifest.json` rewritten for restore portability
- heap and base storage files
- L0/L1 SSTables and Bloom filters referenced by the manifest
- optional `wal.bin`
- `omnikv-backup.json` metadata with format version, file sizes, and CRC32 checksums

## Status

This is a beta-grade recovery feature. It is suitable for development, demos, and non-critical validation flows.

For strict production use, pair this with the remaining durability work:

- deterministic failure-injection tests
- restore drills under crash/restart loops
- long-soak validation
- documented upgrade and migration guarantees

## Plain backup

Prefer `create_backup_with_wal` so the restore directory contains both `manifest.json` and `wal.bin`.

```rust
use omni_engine::backup::{create_backup_with_wal, restore_backup};
use omni_engine::OmniKV;

let db = OmniKV::open("data/manifest.json", "data/wal.bin")?;

create_backup_with_wal(
    &db,
    "data/manifest.json",
    "data/wal.bin",
    "backups/omnikv.tar.gz",
)?;

restore_backup("backups/omnikv.tar.gz", "restore")?;

let restored = OmniKV::open("restore/manifest.json", "restore/wal.bin")?;
```

## Encrypted backup

Encrypted backups use AES-256-GCM with Argon2id-based key derivation.

```rust
use omni_engine::backup::{
    create_encrypted_backup_with_wal,
    restore_encrypted_backup,
};

create_encrypted_backup_with_wal(
    &db,
    "data/manifest.json",
    "data/wal.bin",
    "backups/omnikv.tar.gz.enc",
    "correct horse battery staple",
)?;

restore_encrypted_backup(
    "backups/omnikv.tar.gz.enc",
    "restore",
    "correct horse battery staple",
)?;
```

## Restore safety

Restore rejects archive entries that are unsafe for a database restore target:

- absolute paths
- `..` parent traversal
- Windows path prefixes
- symlinks and other non-file/non-directory entries
- metadata checksum or size mismatches
- unsupported backup format versions

After unpacking, OmniKV rewrites restored manifest paths to point inside the restore directory. This lets callers open the restored database with:

```rust
let restored = OmniKV::open("restore/manifest.json", "restore/wal.bin")?;
```

## Operational guidance

- Store backups outside the active data directory.
- Test restores regularly; a backup is not proven until it has been restored.
- Treat encrypted backup passphrases as production secrets.
- Keep backup archives immutable after creation.
- Do not rely on this feature for critical production data until the P0 durability roadmap is complete.
