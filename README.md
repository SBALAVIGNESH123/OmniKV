# OmniKV

**A Rust database engine built from the ground up, focused on trust, durability, and practical systems learning.**

![Rust](https://img.shields.io/badge/language-Rust-orange?style=for-the-badge&logo=rust)
![CI](https://img.shields.io/badge/CI-green-brightgreen?style=for-the-badge)
![Crash](https://img.shields.io/badge/crash%20cycles-1000%20%2F%200%20lost-brightgreen?style=for-the-badge)
![Soak](https://img.shields.io/badge/soak-10%20min%20%2F%200%20errors-brightgreen?style=for-the-badge)
![License](https://img.shields.io/badge/license-MIT-blue?style=for-the-badge)

OmniKV is an experimental database engine written from scratch in Rust. It is not a wrapper around RocksDB and it is not a fork of SQLite. It includes its own storage engine, write-ahead log, transaction manager, SQL parser, and Raft-oriented consensus work.

The primary goal is correctness and durability. The project includes crash-recovery checks, corruption-detection tests, concurrent stress tests, SQL coverage, operational diagnostics, and Raft cluster tests.

For SketchLog, OmniKV is the durable embedded storage foundation: local telemetry replay buffers, sketch-state persistence, backup/restore, and future edge/offline storage.

> Status: beta / research-grade. OmniKV is suitable for learning, experiments, demos, and non-critical prototypes. Do not put critical production data on it until the remaining consistency, fuzzing, long-soak, and operational hardening work is complete.

## Key features

- Storage: custom LSM-tree with a lock-free SkipMap memtable and tiered compaction.
- Integrity: CRC32 checks on WAL records and heap file entries.
- Concurrency: lock-free reads through `ArcSwap` and Multi-Version Concurrency Control.
- Transactions: Serializable Snapshot Isolation with savepoint support.
- SQL engine: recursive-descent parser, cost-based optimizer, and iterator-based execution work.
- Consensus: Raft integration for multi-node replication and leader failover experiments.
- Operations: health checks, metrics, diagnostics, Docker packaging, and example config.
- Recovery: portable plain/encrypted backup and restore APIs with restore-time metadata validation.
- Embedded API: stable directory-based Rust facade and Python/native bridge with namespaces, batch writes, snapshots, scans, backup/restore, SQL execution, and SketchLog-oriented integration docs.

## Quick start

```bash
git clone https://github.com/SBALAVIGNESH123/OmniKV.git
cd OmniKV
cargo build --workspace --release
export OMNIKV_JWT_SECRET="$(openssl rand -hex 32)"
export OMNIKV_BOOTSTRAP_ADMIN_KEY="$(openssl rand -hex 32)"
export OMNIKV_TLS_INSECURE_SKIP=true # local quick start only
cargo run -p omnikv-server --release
```

Connect using standard `psql`:

```bash
psql -h localhost -p 5433
```

## Docker

```bash
docker build -t omnikv:local .
docker run --rm -p 8443:8443 -p 5433:5433 \
  -e OMNIKV_HTTP_ADDR="0.0.0.0:8443" \
  -e OMNIKV_PGWIRE_ADDR="0.0.0.0:5433" \
  -e OMNIKV_TLS_INSECURE_SKIP=true \
  -e OMNIKV_JWT_SECRET="$(openssl rand -hex 32)" \
  -e OMNIKV_BOOTSTRAP_ADMIN_KEY="$(openssl rand -hex 32)" \
  omnikv:local
```

The container runs as a non-root user and ships with `omni.toml.example` copied to `/etc/omni/omni.toml`. Set secrets such as `OMNIKV_JWT_SECRET` and `OMNIKV_BOOTSTRAP_ADMIN_KEY` through your runtime, secret manager, or local environment. The older `OMNI_JWT_SECRET` and `OMNI_BOOTSTRAP_ADMIN_KEY` aliases are still accepted for compatibility.

For the local Docker Compose demo, make the TLS posture explicit:

```bash
export OMNIKV_JWT_SECRET="$(openssl rand -hex 32)"
export OMNIKV_BOOTSTRAP_ADMIN_KEY="$(openssl rand -hex 32)"
export OMNIKV_TLS_INSECURE_SKIP=true # local demo/self-signed certificates only
docker compose up --build
```

Compose validates that the variables are present. OmniKV validates at startup that the secrets are strong, at least 32 characters, and not reused.

For CI-style package validation, run the single-node authenticated write/read/restart smoke:

```bash
bash scripts/docker-compose-smoke.sh
```

On Windows PowerShell:

```powershell
.\scripts\docker-compose-smoke.ps1
```

For Kubernetes examples, release smoke, and SBOM guidance, see [Docker, Compose, Kubernetes, and release smoke](docs/docker-kubernetes-release.md).

## Embedded library example

```rust
use omni_engine::{EmbeddedBatch, EmbeddedConfig, EmbeddedOmniKv};

let store = EmbeddedOmniKv::open(
    EmbeddedConfig::new("./data/omnikv").namespace("sketchlog"),
).unwrap();

store.write_batch(
    EmbeddedBatch::new()
        .put("telemetry/api/00000000000000000001", r#"{"latency_ms":42}"#)
        .put("sketches/api/p99", "91.4"),
).unwrap();

let p99 = store.get("sketches/api/p99").unwrap();
let replay = store.scan_prefix("telemetry/api/", Some(1000)).unwrap();
```

For the full SketchLog integration contract, see [Embedded API for SketchLog integration](docs/embedded-api.md).

Python callers can install the native bridge and use the same embedded storage
contract:

```bash
python -m pip install "maturin>=1.14,<2"
python -m pip install ./bindings/python
```

```python
import omnikv

store = omnikv.open_embedded("./data/omnikv", namespace="sketchlog")
store.put("sketches/api/p99", "91.4")
assert store.get("sketches/api/p99") == "91.4"
store.sync()
store.close()
```

For packaging and SketchLog environment variables, see [Python embedded bridge](docs/python-bridge.md).

## Testing

OmniKV has a broad test suite covering storage durability, SQL features, concurrent stress, operations, and Raft cluster behavior.

```bash
cargo test --workspace --all-targets
```

For production-readiness work, use the suite taxonomy instead of treating every test as the same kind of evidence. Release gates, regression tests, demonstration tests, performance smoke tests, and manual long-running checks are documented in [Test suite taxonomy](docs/test-suite-taxonomy.md).

On Windows/MSVC, full debug test linking can require significant free disk space because large PDB/debug artifacts are generated. If you hit linker errors such as `LNK1140` or `os error 112`, free disk space or run smaller test groups from `CONTRIBUTING.md`.

## Operations docs

- [Operational runbooks](docs/runbooks/README.md)
- [API and client compatibility](docs/api-compatibility.md)
- [Backup and restore](docs/backup_restore.md)
- [Security model](docs/security.md)
- [Distributed correctness](docs/distributed-correctness.md)
- [Embedded API for SketchLog integration](docs/embedded-api.md)
- [Python embedded bridge](docs/python-bridge.md)
- [Fuzzing and property testing](docs/fuzzing.md)
- [Reproducible benchmarks](docs/benchmarks.md)
- [Protocol and result-size limits](docs/protocol-limits.md)
- [Real-data replay harness](docs/real-data-replay.md)
- [SQL support matrix](docs/sql-support.md)
- [Test suite taxonomy](docs/test-suite-taxonomy.md)
- [Docker, Compose, Kubernetes, and release smoke](docs/docker-kubernetes-release.md)

## Compatibility status

| Surface | Current status | Guardrail |
| --- | --- | --- |
| Embedded Rust API | Beta | Stable facade tests for namespacing, reopen durability, backup/restore, encrypted backup/restore, SQL, plus workspace build, clippy, storage, durability, transaction, and ops tests |
| REST API | Beta | Stable JSON envelope and golden response contract tests |
| PgWire protocol | Beta subset | SQLSTATE, command tag, ReadyForQuery, and result-limit contract tests |
| Rust REST client | Beta | HTTP smoke tests against stable REST response envelopes |
| Python embedded bridge | Beta | PyO3 package exposes the SketchLog-compatible `open_embedded`, `EmbeddedOmniKv.open/open_dir`, key-value methods, sync, close, and stats contract |
| Go client | Not official yet | No compatibility promise until it lives in this repo and runs in CI |

## Workspace layout

OmniKV is organized as a Cargo workspace:

- `crates/omnikv-engine` — embeddable library crate, exposed to Rust code as `omni_engine`.
- `crates/omnikv-server` — executable server crate for REST, QUIC, TCP, and PgWire.
- `omni-client` — Rust client package.

- `bindings/python` is the PyO3/maturin package exposed to Python as
  `omnikv`.

The engine source is grouped by domain under `storage/`, `query/`, `raft/`, and `runtime/`. The benchmark driver lives under `crates/omnikv-engine/benches/` instead of the library source root.

## Known limitations

- Distributed transactions are not Jepsen-tested. The 2PC protocol is implemented, but has not been rigorously tested against network partitions or coordinator crashes.
- Multi-node Raft has deterministic partition, failover, membership, snapshot, and restart evidence, but it is not yet Jepsen-grade or validated under real multi-process network faults.
- SeqScan currently materializes rows before yielding them. True streaming directly from the storage engine is planned.
- Long-running stability still needs a 24-hour soak test.
- Fuzz/property testing is now seeded for SQL, API JSON, WAL, backup restore, Raft log operations, and storage visibility. It still needs long-duration corpus growth before fuzzing can be treated as mature assurance.
- The embedded API is ready for SketchLog integration experiments and non-critical durable telemetry paths, but the project is not yet recommended as the default storage engine for critical production workloads.

## Roadmap

- [x] Phase 1 - Correctness: fixed critical bugs such as GC data loss and non-atomic compaction.
- [x] Phase 2 - Security: Argon2id, constant-time API key comparison, and safer SDK TLS defaults.
- [x] Phase 3 - Durability: crash-recovery cycles and corruption detection.
- [x] Phase 4 - Benchmarks: throughput measurements and short soak tests.
- [x] Phase 5 - Multi-node: Raft cluster tests and partition-handling experiments.
- [x] Phase 6 - Integration: stable embedded API for SketchLog-style durable telemetry state.
- [ ] Phase 7 - Consistency: Jepsen-style testing and failure-model documentation.
- [ ] Phase 8 - Production: long-duration fuzz corpus growth, 24-hour soak, operational runbooks, repeated restore drills, and migration guarantees.

## Contributing

Feedback, bug reports, and pull requests are welcome.

[Star on GitHub](https://github.com/SBALAVIGNESH123/OmniKV/stargazers) | [Report an issue](https://github.com/SBALAVIGNESH123/OmniKV/issues) | [Open a pull request](https://github.com/SBALAVIGNESH123/OmniKV/pulls)

## License

OmniKV is licensed under the MIT License. See [LICENSE](LICENSE).
