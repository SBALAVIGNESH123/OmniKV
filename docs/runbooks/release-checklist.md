# Database release checklist

Use this checklist for any OmniKV release, especially changes that touch
storage, WAL, manifest, backup, SQL, Raft, Docker, or Kubernetes.

## Release metadata

Record:

- release version;
- Git commit SHA;
- Docker image digest;
- Rust toolchain;
- target platforms;
- known caveats;
- linked issues and PRs.

## Required local gates

Run formatting, linting, and build:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo build --all-targets
```

Run storage and recovery gates:

```bash
cargo test -p omnikv-engine --test embedded_api -- --test-threads=1
cargo test -p omnikv-engine --test backup_restore -- --test-threads=1 --nocapture
cargo test -p omnikv-engine --test crash_consistency -- --test-threads=1 --nocapture
cargo test -p omnikv-engine --test durability_evidence -- --test-threads=1 --nocapture
cargo test -p omnikv-engine --test storage_format_versioning -- --test-threads=1
```

Run SQL and operational gates:

```bash
cargo test -p omnikv-engine --test sql_v3_features -- --test-threads=1
cargo test -p omnikv-engine --test ops_maturity -- --test-threads=1 --nocapture
```

Run distributed gate for Raft changes:

```bash
cargo test -p omnikv-engine --test raft_cluster -- --test-threads=1
```

Run packaging smoke:

```bash
bash scripts/docker-compose-smoke.sh
```

Run reproducible benchmark smoke:

```bash
cargo bench -p omnikv-engine --bench reproducible_bench -- --profile smoke --json-out target/omnikv-benchmark-smoke.json
```

## Storage-format gate

If the change touches disk format:

- update [Storage format reference](../storage-format.md);
- update [Upgrades and rollbacks](upgrades-rollbacks.md);
- add old-version load tests;
- add unsupported future-version rejection tests;
- document whether rollback to the previous binary is safe;
- run backup/restore tests;
- include migration notes in release notes.

## Backup/restore gate

For every release candidate:

1. create a backup from the candidate;
2. restore into a clean directory;
3. verify reads;
4. verify writes after restore;
5. record the output.

Automated baseline:

```bash
cargo test -p omnikv-engine --test backup_restore -- --test-threads=1 --nocapture
```

## Security gate

Confirm:

- no production secrets are committed;
- `OMNIKV_MODE=production` fails closed on weak config;
- TLS config is documented;
- JWT secret rotation guidance is current;
- bootstrap admin key rotation guidance is current;
- REST role boundaries and audit events are documented;
- rate-limit behavior is tested;
- security audit is green or documented with accepted risk.

## Observability gate

Confirm docs and dashboards cover:

- disk space;
- WAL size;
- compaction backlog;
- compaction latency;
- write stalls;
- read/write latency;
- error rate;
- cleanup failures;
- rate-limit rejections;
- Raft health.

## Release notes template

```markdown
## OmniKV vX.Y.Z

Commit: <sha>
Image digest: <digest>

### Changed

- ...

### Compatibility

- Manifest format:
- WAL format:
- SSTable format:
- Backup format:
- Downgrade support:

### Validation

- cargo fmt:
- cargo clippy:
- cargo build:
- backup/restore:
- crash consistency:
- durability evidence:
- benchmark smoke:
- Docker smoke:
- security audit:

### Known caveats

- ...
```
