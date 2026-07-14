# OmniKV operational runbooks

These runbooks describe how to operate OmniKV safely during development,
release validation, and early production-style evaluation.

OmniKV is still a beta database engine. Treat these runbooks as the operating
contract for the current codebase, not as a claim that every production database
failure mode has already been proven under years of real traffic.

## Runbook index

| Runbook | Use it when |
| --- | --- |
| [Install and deploy](install-deploy.md) | Starting OmniKV locally, in Docker, or from the Kubernetes sample |
| [Backup and restore](backup-restore.md) | Creating restore points, running restore drills, or recovering a node |
| [Upgrades and rollbacks](upgrades-rollbacks.md) | Moving between releases or validating storage-format compatibility |
| [Incidents](incidents.md) | Handling disk pressure, corruption, WAL recovery, latency, or failed health checks |
| [SLOs and alerts](slo-alerts.md) | Building dashboards and alerts from `/metrics`, host metrics, and synthetic checks |
| [Raft failover](raft-failover.md) | Evaluating leader failover and multi-node behavior |
| [Database release checklist](release-checklist.md) | Preparing storage, WAL, SQL, Raft, or packaging changes for release |

## Current operational evidence

The current codebase includes:

- database-directory locking to prevent two OmniKV processes from mutating the
  same database files at once;
- WAL CRC checks and tail-corruption recovery;
- manifest format version checks;
- read-only mmap invariants for SSTables and base tables;
- crash-consistency and durability evidence tests;
- plain and encrypted backup/restore tests;
- Docker build and Compose smoke tests;
- `/health`, `/ready`, and Prometheus `/metrics` endpoints;
- single-process Raft cluster correctness tests.

Use the commands in [Test suite taxonomy](../test-suite-taxonomy.md) to choose
the right validation scope for a change.

## Operator rules

Safe operations:

- Run one OmniKV process per database directory.
- Keep backups outside the active data directory.
- Restore into an empty directory first, then promote after validation.
- Prefer immutable backup archives and off-host copies.
- Use `OMNIKV_MODE=production` with explicit TLS, JWT secret, bootstrap admin
  key, data directory, backup directory, and rate limits for production-style
  deployments.
- Follow the REST role and token guidance in [Security model](../security.md).
- Run restore drills before trusting a backup policy.

Unsafe operations:

- Do not delete the `LOCK` file while an OmniKV process is alive.
- Do not edit `manifest.json`, WAL files, heap files, or SSTables in place.
- Do not copy a live data directory with ordinary filesystem copy tools and
  treat it as a proven backup.
- Do not roll back across a storage-format change unless the release notes say
  the older binary can read the newer files.
- Do not run two OmniKV binaries against the same `OMNIKV_DATA_DIR`.
- Do not rely on the current multi-node path for critical production workloads
  until partition and failover testing is completed.

## Severity guide

| Severity | Examples | Initial response |
| --- | --- | --- |
| SEV1 | data unavailable, restore failure, suspected corruption, repeated crash loop | Stop write traffic if possible, preserve evidence, validate backup, page maintainer |
| SEV2 | high write latency, write stalls, compaction backlog, disk below 15% free | Reduce write pressure, inspect metrics, plan controlled restart or scale-out |
| SEV3 | single failed smoke check, non-critical alert, documentation drift | Triage during working hours and create an issue with logs |

## Evidence before promotion

Before presenting a release as production-grade, collect:

1. exact Git commit SHA;
2. test commands and passing output;
3. Docker image digest;
4. restore-drill result;
5. benchmark profile for the target workload;
6. known caveats and open issues.

For launch material, phrase OmniKV as an evidence-driven beta database unless
the deployment has survived real user traffic, restore drills, upgrades, and
long-running soak tests.
