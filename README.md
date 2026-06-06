
# OmniKV

**A database engine built from the ground up, focused on trust and durability.**

![Rust](https://img.shields.io/badge/language-Rust-orange?style=for-the-badge&logo=rust) ![Tests](https://img.shields.io/badge/tests-323%20passing-brightgreen?style=for-the-badge) ![Crash](https://img.shields.io/badge/crash%20cycles-1000%20·%200%20lost-brightgreen?style=for-the-badge) ![Soak](https://img.shields.io/badge/soak-10%20min%20·%200%20errors-brightgreen?style=for-the-badge) ![License](https://img.shields.io/badge/license-MIT-blue?style=for-the-badge)

---

OmniKV is a complete database engine written from scratch in Rust. It is not a wrapper around RocksDB or a fork of SQLite. It includes its own storage engine, write-ahead log, transaction manager, SQL parser, and consensus layer. 

The primary goal of OmniKV is **correctness and durability**. It has survived 1,000 crash-recovery cycles with zero data loss, handles manual file corruption safely, and runs continuously under heavy read/write loads without error.

## Key Features

- **Storage**: Custom LSM-tree with a lock-free SkipMap memtable and tiered compaction.
- **Integrity**: CRC32 checks on every WAL record and heap file entry.
- **Concurrency**: Lock-free reads via `ArcSwap` and Multi-Version Concurrency Control (MVCC).
- **Transactions**: Serializable Snapshot Isolation (SSI) with savepoint support.
- **SQL Engine**: Recursive-descent parser, cost-based query optimizer, and a Volcano-style iterator execution pipeline.
- **Consensus**: Raft integration for multi-node replication and leader failover.

## Quick Start

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

### Using as an Embedded Library

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

OmniKV is verified by a comprehensive suite of **323 isolated tests**, covering storage durability, SQL features, concurrent stress, and Raft cluster behavior.

```bash
$ cargo test --all-targets
323 passed; 0 failed; 0 ignored
```

## Known Limitations

- **Distributed transactions are not Jepsen-tested.** The 2PC protocol is implemented, but hasn't been rigorously tested against network partitions or coordinator crashes.
- **SeqScan currently materializes all rows.** The `SeqScanIter` loads all table rows into memory before streaming. True streaming from the storage engine is planned.
- **Long-running stability.** While the 10-minute soak test passes perfectly, a full 24-hour soak test has not been run yet.
- **No fuzz-testing yet.** Fuzzing the SQL parser and wire protocol is planned to catch edge cases.

## Roadmap

- [x] **Phase 1 — Correctness**: Fixed critical bugs (GC data loss, non-atomic compaction).
- [x] **Phase 2 — Security**: Argon2id, constant-time API key comparison.
- [x] **Phase 3 — Durability**: Crash-recovery cycles, corruption detection.
- [x] **Phase 4 — Benchmarks**: Throughput measurements, soak tests.
- [x] **Phase 5 — Multi-Node**: Raft cluster tests, partition handling.
- [ ] **Phase 6 — Consistency**: Jepsen-style testing.
- [ ] **Phase 7 — Production**: Fuzz testing, 24-hour soak, connection pooling.

---

## Contribute

Feedback, bug reports, and pull requests are always welcome!

[⭐ Star us on GitHub](https://github.com/SBALAVIGNESH123/OmniKV/stargazers) · [Report an Issue](https://github.com/SBALAVIGNESH123/OmniKV/issues) · [Contribute](https://github.com/SBALAVIGNESH123/OmniKV/pulls)

*MIT License*
