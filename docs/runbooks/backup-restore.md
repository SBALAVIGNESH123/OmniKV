# Backup and restore runbook

This runbook turns the library-level backup APIs into an operator workflow.
For API details, see [Backup and restore](../backup_restore.md).

## Current backup contract

OmniKV backups are gzip-compressed tar archives containing:

- a portable `manifest.json`;
- heap and base storage files;
- L0/L1 SSTables and Bloom filters referenced by the manifest;
- optional `wal.bin`;
- `omnikv-backup.json` metadata with format version, file sizes, and CRC32
  checksums.

Prefer backup APIs that include the WAL:

- `create_backup_with_wal`
- `create_encrypted_backup_with_wal`

## Backup policy

Recommended beta policy:

- take backups outside `OMNIKV_DATA_DIR`;
- encrypt backups when they leave the host;
- keep at least one off-host copy;
- make backup archives immutable after creation;
- run a restore drill before announcing a release;
- store the exact OmniKV version and Git SHA beside every backup.

## Manual restore drill

1. Start from a database with known keys or a replay fixture.
2. Create a backup with WAL.
3. Restore into an empty directory.
4. Open the restored database using the restored `manifest.json` and `wal.bin`.
5. Verify known keys.
6. Record the backup path, restore path, OmniKV version, command output, and
   result.

The current automated restore drill is:

```bash
cargo test -p omnikv-engine --test backup_restore -- --test-threads=1 --nocapture
```

That test validates:

- plain backup/restore roundtrip;
- encrypted backup/restore roundtrip;
- wrong passphrase rejection;
- path traversal rejection;
- restored database open through the public `OmniKV::open` contract.

## Recovery from a backup

1. Stop write traffic.
2. Preserve the current data directory before changing anything.
3. Restore the backup into a new empty directory.
4. Start OmniKV pointed at the restored directory.
5. Run read smoke checks against known keys.
6. Run write smoke checks only after reads pass.
7. Promote the restored directory by updating deployment config or volume
   mapping.

Do not restore over an active data directory.

## Corruption response

If logs mention manifest, WAL, heap, SSTable, CRC, or decode corruption:

1. Stop the process or remove write traffic.
2. Copy the entire data directory for forensic inspection.
3. Do not edit files in place.
4. Attempt restore from the latest known-good backup into a new directory.
5. Run the restore drill.
6. If the restore works, promote it and keep the old directory for analysis.

## Backup failure response

If backup creation fails:

- check free disk space in the backup directory;
- check permissions on the data and backup directories;
- ensure no second OmniKV process owns the same database directory;
- inspect logs for compaction cleanup failures;
- retry only after fixing the underlying cause.

If restore fails:

- verify the archive was not modified after creation;
- verify the restore target is empty and writable;
- verify the passphrase for encrypted backups;
- preserve the failed restore directory and logs;
- try the previous backup.

## Release evidence

Every release candidate should include:

```bash
cargo test -p omnikv-engine --test backup_restore -- --test-threads=1 --nocapture
cargo test -p omnikv-engine --test crash_consistency -- --test-threads=1 --nocapture
```

For storage changes, also run:

```bash
cargo test -p omnikv-engine --test durability_evidence -- --test-threads=1 --nocapture
```
