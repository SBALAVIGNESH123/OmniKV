# Upgrades and rollbacks runbook

Use this runbook when moving OmniKV between versions.

## Storage format compatibility

The current manifest format is `MANIFEST_FORMAT_VERSION = 1`.

Compatibility behavior:

| Manifest `format_version` | Current behavior |
| --- | --- |
| missing | treated as v1 for backward compatibility |
| `1` | loads normally |
| `> 1` | fails with `UnsupportedVersion` |

See [Storage format reference](../storage-format.md).

## Upgrade preflight

Before upgrading:

1. Read the release notes for storage, WAL, manifest, SSTable, backup, SQL, and
   Raft changes.
2. Confirm whether the release changes on-disk format.
3. Run a backup and restore drill.
4. Run the Docker Compose smoke against the target image.
5. Record the current image digest, binary version, Git SHA, and config.
6. Confirm rollback is possible for the target release.

Validation commands:

```bash
cargo test -p omnikv-engine --test backup_restore -- --test-threads=1 --nocapture
cargo test -p omnikv-engine --test storage_format_versioning -- --test-threads=1
bash scripts/docker-compose-smoke.sh
```

On Windows:

```powershell
.\scripts\docker-compose-smoke.ps1
```

## Single-node upgrade

1. Stop write traffic or enter a maintenance window.
2. Create a backup with WAL.
3. Run the restore drill into a temporary directory.
4. Stop the old process.
5. Start the new binary or container with the same `OMNIKV_DATA_DIR`.
6. Check `/health`, `/ready`, and `/metrics`.
7. Run authenticated write/read smoke.
8. Watch compaction backlog, write stalls, latency, and errors for at least one
   operational window.

## Rollback

Rollback is safe only if the previous binary can read the files written by the
new binary.

If the release did not change storage format:

1. Stop the new process.
2. Start the previous binary with the same data directory.
3. Run read smoke checks.
4. Run a write smoke check.
5. Watch `/ready`, latency, and compaction metrics.

If the release changed storage format:

1. Do not start the older binary against the upgraded data directory unless the
   release notes explicitly say downgrade is supported.
2. Restore the pre-upgrade backup into a new directory.
3. Start the older binary against the restored directory.
4. Validate reads and writes.

## Storage-format change checklist

Any PR that changes manifest, WAL, heap, SSTable, backup archive, or Raft state
encoding must:

- bump or document format compatibility deliberately;
- add tests for old version loading;
- add tests for future version rejection if applicable;
- update [Storage format reference](../storage-format.md);
- update this runbook if rollback behavior changes;
- prove backup/restore compatibility;
- mention downgrade support or non-support in release notes.

## Failed upgrade response

Rollback or restore if:

- the process fails to start;
- `/ready` remains unavailable;
- storage format errors appear;
- read smoke fails;
- write smoke fails;
- compaction backlog grows without recovery;
- Raft membership or leader state becomes ambiguous in a multi-node setup.
