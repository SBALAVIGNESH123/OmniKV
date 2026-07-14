# Incident runbook

Use this runbook during production-style evaluation incidents.

## First five minutes

1. Identify severity from [README](README.md#severity-guide).
2. Preserve logs and exact versions.
3. Stop write traffic if corruption, disk exhaustion, or repeated crash loops
   are suspected.
4. Check `/health`, `/ready`, and `/metrics`.
5. Check host disk, file descriptors, memory, and container restart count.
6. Decide whether to continue, restart, roll back, or restore.

## Health or readiness failure

Commands:

```bash
curl -k https://127.0.0.1:8443/health
curl -k https://127.0.0.1:8443/ready
curl -k https://127.0.0.1:8443/metrics
```

Actions:

- inspect logs around startup;
- check TLS/JWT config in production mode;
- check whether another process owns the database `LOCK`;
- check disk and permissions for `OMNIKV_DATA_DIR`;
- run the Docker smoke script against the exact image if containerized.

## Disk full or disk pressure

Symptoms:

- low free space on the data volume;
- failed writes;
- failed backup creation;
- compaction cleanup failures;
- increasing latency.

Actions:

1. Reduce or stop write traffic.
2. Confirm whether the backup directory is outside the data directory.
3. Move old backups or logs off the data volume.
4. Inspect `omnikv_cleanup_delete_failures_total`.
5. Watch `omnikv_compaction_backlog_sstables` and `omnikv_write_stalls_total`.
6. Add disk capacity before resuming normal writes.

Do not delete SSTables, heap files, WAL files, manifest files, or `LOCK`.

## WAL corruption

Current evidence shows WAL tail corruption is detected and valid prior batches
are replayed. Full corruption may still require restore from backup.

Actions:

1. Stop the process.
2. Copy the data directory.
3. Start a copy in an isolated environment if possible.
4. If it opens, run read smoke checks.
5. If reads fail or the process refuses to open, restore from backup.

Relevant validation:

```bash
cargo test -p omnikv-engine --test crash_consistency -- --test-threads=1 --nocapture
```

## Manifest, heap, or SSTable corruption

Actions:

1. Stop write traffic immediately.
2. Preserve the entire data directory.
3. Do not edit files in place.
4. Restore the latest known-good backup into a clean directory.
5. Validate restored reads and writes.
6. Open a GitHub issue with logs, file sizes, version, and reproduction steps.

## Compaction backlog or write stalls

Metrics:

- `omnikv_compaction_backlog_sstables`
- `omnikv_write_stalls_total`
- `omnikv_compaction_latency_seconds{stage=...}`
- `omnikv_compaction_bytes_rewritten_total{stage=...}`

Actions:

1. Reduce write pressure.
2. Check disk and I/O saturation.
3. Check cleanup delete failures.
4. Increase disk capacity if compaction needs working space.
5. If stalls continue after pressure drops, capture logs and open an issue.

## High latency

Actions:

1. Compare read and write latency histograms.
2. Check compaction backlog and write stalls.
3. Check disk I/O and CPU saturation.
4. Check rate-limit rejections.
5. Check whether a benchmark or bulk load is running.
6. If latency started after a deploy, follow [Upgrades and rollbacks](upgrades-rollbacks.md).

## Backup or restore failure

Follow [Backup and restore](backup-restore.md). Treat restore failure as SEV1 if
the backup is needed for recovery.

## Security incident

Actions:

1. Rotate `OMNIKV_JWT_SECRET`.
2. Rotate TLS certificates if private keys may be exposed.
3. Rotate `OMNIKV_BOOTSTRAP_ADMIN_KEY`.
4. Revoke exposed deployment secrets and re-mint scoped short-lived tokens.
5. Preserve logs, including `omnikv.audit` records.
6. Increase rate-limit strictness if under abuse.
7. Rebuild and redeploy from a known commit if binary integrity is uncertain.

## Evidence to capture

Always capture:

- OmniKV version and Git SHA;
- container image digest;
- config file with secrets redacted;
- logs around the incident window;
- `/metrics` snapshot;
- data directory file listing and sizes;
- backup archive metadata if restore is involved;
- exact commands used during recovery.
