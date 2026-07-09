# OmniKV

**A Rust database engine built from the ground up, focused on trust, durability, and practical systems learning.**

![Rust](https://img.shields.io/badge/language-Rust-orange?style=for-the-badge&logo=rust)
![Tests](https://img.shields.io/badge/tests-323%20passing-brightgreen?style=for-the-badge)
![Crash](https://img.shields.io/badge/crash%20cycles-1000%20%2F%200%20lost-brightgreen?style=for-the-badge)
![Soak](https://img.shields.io/badge/soak-10%20min%20%2F%200%20errors-brightgreen?style=for-the-badge)
![License](https://img.shields.io/badge/license-MIT-blue?style=for-the-badge)

OmniKV is an experimental database engine written from scratch in Rust. It is not a wrapper around RocksDB and it is not a fork of SQLite. It includes its own storage engine, write-ahead log, transaction manager, SQL parser, and Raft-oriented consensus work.

The primary goal is correctness and durability. The project includes crash-recovery checks, corruption-detection tests, concurrent stress tests, SQL coverage, operational diagnostics, and Raft cluster tests.

> Status: beta / research-grade. OmniKV is suitable for learning, experiments, demos, and non-critical prototypes. Do not put critical production data on it until the remaining consistency, fuzzing, long-soak, and operational hardening work is complete.

## Key features

- Storage: custom LSM-tree with a lock-free SkipMap memtable and tiered compaction.
- Integrity: CRC32 checks on WAL records and heap file entries.
- Concurrency: lock-free reads through `ArcSwap` and Multi-Version Concurrency Control.
- Transactions: Serializable Snapshot Isolation with savepoint support.
- SQL engine: recursive-descent parser, cost-based optimizer, and iterator-based execution work.
- Consensus: Raft integration for multi-node replication and leader failover experiments.
- Operations: health checks, metrics, diagnostics, Docker packaging, and example config.

## Quick start

```bash
git clone https://github.com/SBALAVIGNESH123/OmniKV.git
cd OmniKV
cargo build --release
cargo run --release
```

Connect using standard `psql`:

```bash
psql -h localhost -p 5433
```

## Docker

```bash
docker build -t omnikv:local .
docker run --rm -p 8443:8443 -p 5433:5433 -e OMNI_JWT_SECRET="$(openssl rand -hex 32)" omnikv:local
```

The container runs as a non-root user and ships with `omni.toml.example` copied to `/etc/omni/omni.toml`. Set secrets such as `OMNI_JWT_SECRET` through your runtime, secret manager, or local environment.

## Embedded library example

```rust
use omni_engine::{OmniKV, WriteBatch};

let db = OmniKV::open("manifest.json", "data.wal").unwrap();

let mut batch = WriteBatch::new();
batch.set("user:1", r#"{"name":"Alice"}"#.into()).unwrap();
db.commit_batch(&batch).unwrap();

let snap = db.snapshot();
let alice = db.find("user:1", snap).unwrap();
db.unregister_snapshot(snap);
```

## Testing

OmniKV has a broad test suite covering storage durability, SQL features, concurrent stress, operations, and Raft cluster behavior.

```bash
cargo test --all-targets
```

On Windows/MSVC, full debug test linking can require significant free disk space because large PDB/debug artifacts are generated. If you hit linker errors such as `LNK1140` or `os error 112`, free disk space or run smaller test groups from `CONTRIBUTING.md`.

## Known limitations

- Distributed transactions are not Jepsen-tested. The 2PC protocol is implemented, but has not been rigorously tested against network partitions or coordinator crashes.
- SeqScan currently materializes rows before yielding them. True streaming directly from the storage engine is planned.
- Long-running stability still needs a 24-hour soak test.
- Fuzz testing is not in place yet for the SQL parser, wire protocol, and storage file formats.
- The project is not yet recommended as the default storage engine for critical production workloads.

## Roadmap

- [x] Phase 1 - Correctness: fixed critical bugs such as GC data loss and non-atomic compaction.
- [x] Phase 2 - Security: Argon2id, constant-time API key comparison, and safer SDK TLS defaults.
- [x] Phase 3 - Durability: crash-recovery cycles and corruption detection.
- [x] Phase 4 - Benchmarks: throughput measurements and short soak tests.
- [x] Phase 5 - Multi-node: Raft cluster tests and partition-handling experiments.
- [ ] Phase 6 - Consistency: Jepsen-style testing and failure-model documentation.
- [ ] Phase 7 - Production: fuzz testing, 24-hour soak, operational runbooks, backups, and migration guarantees.

## Contributing

Feedback, bug reports, and pull requests are welcome.

[Star on GitHub](https://github.com/SBALAVIGNESH123/OmniKV/stargazers) | [Report an issue](https://github.com/SBALAVIGNESH123/OmniKV/issues) | [Open a pull request](https://github.com/SBALAVIGNESH123/OmniKV/pulls)

## License

OmniKV is licensed under the MIT License. See [LICENSE](LICENSE).
