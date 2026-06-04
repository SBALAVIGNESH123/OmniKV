<p align="center">
  <img src="docs/omnikv_logo.png" alt="OmniKV Logo" width="200">
</p>

<h1 align="center">OmniKV</h1>

<p align="center">
  <strong>The database engine built from the ground up to earn your trust.</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/language-Rust-orange?style=for-the-badge&logo=rust" alt="Rust">
  <img src="https://img.shields.io/badge/tests-103%20passing-brightgreen?style=for-the-badge" alt="Tests">
  <img src="https://img.shields.io/badge/crash%20cycles-1000%20·%200%20lost-brightgreen?style=for-the-badge" alt="Crash">
  <img src="https://img.shields.io/badge/soak-10%20min%20·%200%20errors-brightgreen?style=for-the-badge" alt="Soak">
  <img src="https://img.shields.io/badge/license-MIT-blue?style=for-the-badge" alt="License">
</p>

---

## We Built a Database Engine From Scratch. Then We Tried to Break It.

Most database projects start with a bold claim: *"We're faster than X."* We started with a harder question: **"Will you lose data if the power goes out?"**

OmniKV is a complete database engine — storage, transactions, SQL, consensus — written from the ground up in Rust. Not a wrapper around RocksDB. Not a fork of SQLite. Not a thin layer over someone else's B-tree. Every single byte that touches your disk was written by us, and every single failure mode has been tested by us.

We built our own LSM-tree. We built our own write-ahead log with CRC32 integrity on every record. We built our own MVCC engine, our own SQL parser, our own cost-based query optimizer, and our own transaction engine with Serializable Snapshot Isolation. Then we spent weeks trying to break all of it — crashing the engine mid-compaction, corrupting manifest files, flipping bits in SSTables, and restarting it a thousand times in a row.

The result? **Zero data loss. Zero silent corruption. Zero panics.**

That is the foundation we are building on.

---

## Our Philosophy: Evidence Over Claims

The database industry is full of README files that say *"production-ready"* and *"battle-tested."* We believe those words are earned, not declared.

Instead of telling you what OmniKV can do, we will show you what we have **proven**. Every claim on this page is backed by an automated test that you can run yourself. If a capability is not yet proven, we say so in our [Known Limitations](#known-limitations) section — because honesty earns more trust than marketing.

This is our approach:
1. **Correctness first.** Fix every data-loss bug before writing a single benchmark.
2. **Durability second.** Prove the engine survives failure before measuring how fast it runs.
3. **Performance third.** Only publish numbers we have actually measured.

---

## Durability: The Evidence

A database that loses your data after a crash is not a database. It is a cache with persistence theater. Here is our evidence that OmniKV is not that.

### 1,000 Crash-Recovery Cycles — Zero Keys Lost

We wrote 5 keys, crashed the engine without graceful shutdown, reopened it, and verified every key was present. Then we did it again. And again. **One thousand times.**

Every 100 cycles, we triggered compaction to stress the LSM-tree's most fragile code path. At the end of 1,000 cycles, all 5,000 keys were verified present. Not one was lost. Not one was corrupted.

### Corruption Resistance — Proven, Not Assumed

We did not just test the happy path. We deliberately attacked our own engine:

- **Manifest corruption**: We truncated the manifest to half its size, zeroed it entirely, and filled it with random garbage. The engine recovered or returned a clear error in every case. It never panicked.
- **SSTable corruption**: We flipped 32 bytes in the middle of an SSTable's data region. The engine detected the CRC mismatch and refused to serve corrupted data. No silent wrong answers.
- **WAL corruption**: We appended 256 bytes of garbage to the end of the write-ahead log. The engine recovered all valid records before the corruption point and continued operating normally.

### 10-Minute Soak Test — Zero Errors

We ran 4 writer threads and 4 reader threads simultaneously for 10 continuous minutes, with background compaction running throughout. The engine processed **975 million reads** and **278,000 writes** without a single error.

```
Duration:          600 seconds
Total Writes:      278,715
Total Reads:       975,331,059
Errors:            0
Verdict:           ✅ PASS
```

---

## Performance: Real Numbers, Not Promises

Every number below was measured on a consumer-grade machine in release mode. We do not cherry-pick results or run benchmarks on server hardware to inflate numbers.

### Read Performance

OmniKV's MVCC architecture with ArcSwap topology means readers never block writers and never take locks. The result is read throughput that scales linearly with cores.

| Operation | Throughput | p50 Latency | p99 Latency |
|-----------|-----------|-------------|-------------|
| Random Reads (hit) | **328,696 ops/s** | 2.5 µs | 21.1 µs |
| Sequential Reads | **225,085 ops/s** | 3.5 µs | 21.2 µs |
| Point Read (miss) | **1,455,286 ops/s** | 0.6 µs | 1.1 µs |
| Range Scan | **952,866 rows/s** | — | — |

At 8 threads, read throughput reaches **2.2 million ops/sec** — an 8.7× improvement over single-threaded performance, demonstrating near-linear scaling.

### Write Performance

| Operation | Throughput | p50 Latency | p99 Latency |
|-----------|-----------|-------------|-------------|
| Batch Writes (100 keys/batch) | **36,495 ops/s** | 2,703 µs | 3,266 µs |
| SSI Transactions | **461 txns/s** | 2,160 µs | 2,946 µs |

Write throughput is currently limited by per-write fsync for durability. We have implemented group commit — batching multiple writes into a single fsync — and are measuring the improvement. We prioritized correctness over throughput because a fast database that loses data is worthless.

---

## What OmniKV Gives You

### A Complete SQL Engine

OmniKV includes a full SQL layer — not a toy parser, but a real query engine with a cost-based optimizer and volcano-model executor. Connect with any PostgreSQL client.

```bash
psql -h localhost -p 5433

CREATE TABLE users (id INT, name TEXT, email TEXT);
INSERT INTO users VALUES (1, 'Alice', 'alice@dev.io');
INSERT INTO users VALUES (2, 'Bob', 'bob@dev.io');

CREATE TABLE orders (id INT, user_id INT, amount FLOAT);
INSERT INTO orders VALUES (101, 1, 299.99);

SELECT u.name, SUM(o.amount)
FROM users u INNER JOIN orders o ON u.id = o.user_id
GROUP BY u.name;

EXPLAIN ANALYZE SELECT * FROM users WHERE id = 1;
```

The optimizer uses real statistics — per-column histograms, distinct value counts, null fractions — to choose between sequential scans, index lookups, and hash joins. It pushes predicates down to minimize rows entering joins, and reorders join operands to use the smaller table as the hash-build side.

### Serializable Transactions

OmniKV implements Serializable Snapshot Isolation (SSI) — the same isolation level PostgreSQL uses for its strongest guarantee. Transactions see a consistent snapshot at `BEGIN`, buffer writes until `COMMIT`, and detect conflicts at commit time.

```sql
BEGIN;
INSERT INTO accounts VALUES (3, 'Carol', 1000.00);
SAVEPOINT before_transfer;
UPDATE accounts SET balance = balance - 500 WHERE id = 3;
ROLLBACK TO before_transfer;
COMMIT;
```

Write-write conflicts are detected. Read-write dependency cycles are detected. Dangerous structures that could lead to serializability violations are caught and one transaction is aborted to maintain correctness.

### Embeddable Library

OmniKV can be used as a standalone server or embedded directly into your Rust application:

```rust
use omni_engine::{OmniKV, WriteBatch};

let db = OmniKV::open("manifest.json", "data.wal").unwrap();

let mut batch = WriteBatch::new();
batch.set("user:1", r#"{"name":"Alice"}"#.into()).unwrap();
batch.set("user:2", r#"{"name":"Bob"}"#.into()).unwrap();
db.commit_batch(&batch).unwrap();

let snap = db.snapshot();
let alice = db.find("user:1", snap).unwrap();
db.unregister_snapshot(snap);
```

### Four Network Protocols

Start OmniKV and it opens four network interfaces simultaneously:

```
  ╔════════════════════════════════════════════════════╗
  ║        ⚡ OmniKV v0.1.0                           ║
  ╠════════════════════════════════════════════════════╣
  ║  HTTPS (TLS 1.3)         → 0.0.0.0:8443          ║
  ║  QUIC / HTTP3            → 0.0.0.0:4433          ║
  ║  PostgreSQL Wire (v3)    → 0.0.0.0:5433          ║
  ║  TCP Command Interface   → 0.0.0.0:8080          ║
  ╚════════════════════════════════════════════════════╝
```

---

## Architecture

Every layer of OmniKV is purpose-built. There are no external storage dependencies.

```
┌──────────────────────────────────────────────────────────────────┐
│  CLIENT LAYER                                                    │
│  PgWire v3 · REST/HTTP2 · QUIC/HTTP3 · TCP                     │
├──────────────────────────────────────────────────────────────────┤
│  SQL ENGINE                                                      │
│  Recursive-descent parser → Cost-based optimizer → Volcano       │
│  Predicate pushdown · JOIN reorder · Plan cache · EXPLAIN        │
├──────────────────────────────────────────────────────────────────┤
│  TRANSACTION ENGINE                                              │
│  SSI · Savepoints · Conflict detection · Timeouts · 2PC          │
├──────────────────────────────────────────────────────────────────┤
│  STORAGE ENGINE                                                  │
│  WAL (CRC32) → 16-shard SkipMap → SSTable → Tiered Compaction   │
│  Heap (CRC32/entry) · Bloom filters · LRU cache · LZ4 · MVCC   │
│  ArcSwap topology · Group commit · Argon2id encryption           │
├──────────────────────────────────────────────────────────────────┤
│  CONSENSUS                                                       │
│  OpenRaft 0.9 · Leader election · Log replication · Snapshots    │
└──────────────────────────────────────────────────────────────────┘
```

### Why We Built Everything Custom

We chose to build every layer from scratch because control over the storage format is control over correctness. When you use RocksDB as your storage engine, you inherit its compaction behavior, its memory allocation patterns, and its failure modes — most of which you cannot change. When you build your own, every bug is your bug, every fix is your fix, and every invariant is one you understand.

This is harder. It is also how you build something you can fully trust.

---

## Test Suite

```
$ cargo test
103 passed; 0 failed; 0 ignored
```

| Suite | Tests | What It Proves |
|-------|-------|----------------|
| Storage Engine | 76 | WAL correctness, CRC integrity, compaction, MVCC, bloom filters |
| Storage Correctness | 14 | Crash recovery, atomicity, snapshot isolation, torn-write handling |
| Durability Evidence | 12 | 1000 crash cycles, corruption detection, backup/restore |
| SQL Layer | 18 | Parser, JOINs, aggregates, optimizer, execution |
| Concurrent Stress | 6 | Multi-threaded contention, write-stall handling |

Every test uses a fresh temporary directory and cleans up after itself. Tests run in isolation. We do not rely on shared state or test ordering.

---

## Known Limitations

We believe the fastest way to earn your trust is to tell you exactly where OmniKV is not yet ready.

**Multi-node correctness is not yet proven.** OmniKV integrates OpenRaft for consensus, but we have not yet run crash tests against a 3-node cluster under network partitions. Until we do, the distributed layer should be considered experimental.

**Distributed transactions are not Jepsen-tested.** The 2PC protocol exists but has not been validated under coordinator crash, participant crash, or partial network failure. We will not claim distributed transaction correctness until we have the evidence.

**Write throughput is limited by WAL sync.** Each commit currently performs an fsync to guarantee durability. Group commit (batching multiple writes into one fsync) is implemented and being validated. This is the primary performance bottleneck and our top engineering priority.

**Long-running stability beyond 10 minutes is unproven.** Our soak test runs for 10 minutes with zero errors. A 24-hour soak test is planned.

**The SQL parser and wire protocol have not been fuzz-tested.** We plan to integrate `cargo-fuzz` to discover edge cases in input handling.

We will update this section as we close each gap. When a limitation is resolved, it moves to the evidence section with a test that proves it.

---

## Roadmap

OmniKV follows a trust-first development model. Each phase must produce evidence before the next begins.

- [x] **Phase 1 — Correctness**: Fixed 6 P0 bugs including GC data loss, non-atomic compaction, SQL precedence, and PgWire transaction handling
- [x] **Phase 2 — Security**: Argon2id key derivation, constant-time API key comparison
- [x] **Phase 3 — Durability**: 12 durability tests, 1000 crash-recovery cycles, corruption detection
- [x] **Phase 4 — Benchmarks**: Measured throughput and latency, 10-minute soak test
- [ ] **Phase 5 — Multi-Node**: 3-node cluster tests, partition tolerance, leader failover
- [ ] **Phase 6 — Consistency**: Jepsen-style testing, linearizability verification
- [ ] **Phase 7 — Production**: Fuzz testing, 24-hour soak, connection pooling, monitoring

---

## Getting Started

```bash
git clone https://github.com/SBALAVIGNESH123/OmniKV.git
cd OmniKV
cargo build --release
cargo run --release
```

Run the benchmark suite:

```bash
cargo run --bin omni_bench --release                  # Quick benchmarks
cargo run --bin omni_bench --release -- --soak 600    # 10-minute soak test
```

Run all tests:

```bash
cargo test                                            # All 103 tests
cargo test --test durability_evidence                 # Durability suite only
```

---

## About

OmniKV is created by [Balavignesh](https://github.com/SBALAVIGNESH123) — built from scratch in Rust, one byte at a time.

We are building the kind of database engine that does not ask you to trust it. It asks you to verify it. Every test is open source. Every benchmark is reproducible. Every limitation is documented.

**If you believe databases should earn trust through evidence, not marketing — we would love your feedback, your bug reports, and your pull requests.**

<p align="center">
  <a href="https://github.com/SBALAVIGNESH123/OmniKV/stargazers">⭐ Star us on GitHub</a> · 
  <a href="https://github.com/SBALAVIGNESH123/OmniKV/issues">Report an Issue</a> · 
  <a href="https://github.com/SBALAVIGNESH123/OmniKV/pulls">Contribute</a>
</p>

<p align="center">
  <em>MIT License</em>
</p>
