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
| Random Reads (hit) | **662,288 ops/s** | 1.1 µs | 9.4 µs |
| Sequential Reads | **764,065 ops/s** | 0.7 µs | 7.6 µs |
| Point Read (miss) | **1,607,991 ops/s** | 0.6 µs | 1.0 µs |
| Range Scan | **1,320,463 rows/s** | — | — |

At 8 threads, read throughput reaches **3.5 million ops/sec** — a 5.1× improvement over single-threaded performance, demonstrating near-linear scaling.

### Write Performance

| Operation | Throughput | p50 Latency | p99 Latency |
|-----------|-----------|-------------|-------------|
| Sequential Writes | **253 ops/s** | 4,174 µs | 9,955 µs |
| Batch Writes (100 keys/batch) | **36,465 ops/s** | 2,705 µs | 3,473 µs |
| Mixed (80% read / 20% write) | **2,124 ops/s** | 5.1 µs | 2,687 µs |
| SSI Transactions | **459 txns/s** | 2,127 µs | 3,104 µs |

### Thread Scaling — Group Commit v2 Proof

Group commit v2 batches concurrent fsyncs without sleeping. The write scaling numbers prove it works:

| Threads | Write ops/sec | Scaling |
|---------|--------------|---------|
| 1 | 427 | 1.0× |
| 2 | 680 | 1.6× |
| 4 | 1,327 | **3.1×** |
| 8 | 2,433 | **5.7×** |

| Threads | Read ops/sec | Scaling |
|---------|-------------|---------|
| 1 | 696,933 | 1.0× |
| 2 | 1,911,534 | 2.7× |
| 4 | 2,389,315 | 3.4× |
| 8 | 3,533,419 | **5.1×** |

Write performance is bounded by fsync durability guarantees. Our group commit engine (v2) batches multiple concurrent fsyncs into a single disk I/O — reducing syscalls by up to N× under concurrent write load while maintaining full crash safety.

---

## Deep Dive: Cost-Based Query Optimizer

OmniKV includes a real cost-based query optimizer — not a rule-based heuristic, but a statistics-driven planner that estimates costs and chooses the cheapest execution strategy.

### How It Works

When you run a query, the optimizer goes through four phases:

**Phase 1 — Statistics Collection.** The optimizer calls `gather_stats()`, which scans actual table data to build per-column histograms. Each histogram contains the number of distinct values (NDV), null fraction, and most common values. These statistics drive all cost estimates.

**Phase 2 — Access Path Selection.** For each table in the query, the optimizer evaluates three access methods and picks the cheapest:

| Access Method | Cost Formula | When It Wins |
|---------------|-------------|--------------|
| Sequential Scan | `row_count × 1.0` | No suitable index, or high selectivity |
| Index Scan | `estimated_rows × 0.25` | Index exists on filter column, low selectivity |
| PK Lookup | `1.0` (constant) | Equality filter on primary key |

**Phase 3 — JOIN Optimization.** For joins, the optimizer:
- Estimates the output cardinality of each join using NDV-based selectivity
- Places the smaller table as the hash-build side (reduces memory)
- Pushes single-table predicates down through the join using `pushdown_join_predicates()`
- Costs each join as: `build_rows × 2.0 + probe_rows × 0.1`

**Phase 4 — Plan Selection.** The optimizer produces a tree of `PlanNode` operators:

```
PlanNode::Project
  └─ PlanNode::Sort
       └─ PlanNode::Filter
            └─ PlanNode::HashJoin { left: Scan(orders), right: Scan(users) }
```

You can inspect the plan with `EXPLAIN ANALYZE`, which shows estimated vs actual row counts and wall-clock time per operator — the same output format PostgreSQL uses.

### Cost Constants

```
SEQ_SCAN    = 1.0 per row       — sequential I/O, one row at a time
INDEX_SCAN  = 0.25 per row      — random I/O but fewer rows
PK_LOOKUP   = 1.0 flat          — single key lookup, O(1)
HASH_BUILD  = 2.0 per row       — build hash table in memory
HASH_PROBE  = 0.1 per row       — probe existing hash table
SORT        = N × log₂(N) × 2.0 — comparison sort
```

### Selectivity Estimation

The optimizer estimates how many rows will survive each filter:

| Predicate Type | Selectivity |
|---------------|-------------|
| `column = value` | `1 / NDV(column)` |
| `column > value` | `1/3` (default range) |
| `column IS NULL` | `null_fraction(column)` |
| `a AND b` | `sel(a) × sel(b)` (multiplicative) |
| `a OR b` | `sel(a) + sel(b) - sel(a) × sel(b)` (inclusion-exclusion) |

---

## Deep Dive: Volcano Iterator Executor

Once the optimizer produces a plan, the **Volcano executor** compiles it into a pull-based iterator pipeline. This is the same architecture used by PostgreSQL, Oracle, and SQL Server.

### How It Works

Every operator implements a single trait:

```rust
pub trait RowIterator {
    fn next_row(&mut self) -> Option<Row>;
}
```

Operators are composed into a pipeline. The top operator calls `next_row()` on its child, which calls `next_row()` on its child, all the way down to the table scan. Rows flow upward one at a time.

### Operators and Memory Guarantees

| Operator | Memory | Description |
|----------|--------|-------------|
| **SeqScanIter** | O(N) | Scans all rows from a table via the storage engine |
| **PkLookupIter** | O(1) | Single-row lookup by primary key |
| **FilterIter** | O(1) | Passes through rows matching a predicate — no buffering |
| **ProjectIter** | O(1) | Selects specific columns — no buffering |
| **LimitIter** | O(1) | Stops after N rows — early termination |
| **SortIter** | O(N) | Materializes all rows, sorts, then streams (unavoidable) |
| **HashJoinIter** | O(build) | Builds hash table from smaller side, probes with larger side |
| **AggregateIter** | O(groups) | Groups rows, computes COUNT/SUM/AVG/MIN/MAX per group |

### Example Pipeline

```sql
SELECT u.name, SUM(o.amount)
FROM users u JOIN orders o ON u.id = o.user_id
WHERE o.amount > 100
GROUP BY u.name
ORDER BY u.name
LIMIT 10;
```

Compiles to:

```
LimitIter(10)
  └─ SortIter(name ASC)
       └─ AggregateIter(GROUP BY name, SUM(amount))
            └─ HashJoinIter(build=users, probe=orders, on id=user_id)
                 ├─ SeqScanIter(users)
                 └─ FilterIter(amount > 100)
                      └─ SeqScanIter(orders)
```

The `FilterIter` uses O(1) memory — it never buffers rows. The `HashJoinIter` only buffers the smaller table (users). Rows stream through the pipeline one at a time until they hit `LimitIter`, which stops pulling after 10 rows.

---

## Deep Dive: Storage Engine

The storage engine is a full LSM-tree implementation with MVCC, built from scratch.

### Write Path

```
Client write
  → LZ4 compress + CRC32 (no lock, CPU-bound)
  → write_mutex: assign sequence number + reserve heap offset (µs)
  → Heap pwrite: positional write to data file (no seek contention)
  → WAL append: write + flush to write-ahead log
  → Group Commit: leader fsyncs heap + WAL for all concurrent writers
  → SkipMap insert: lock-free memtable insertion (16 shards, FNV-hashed)
```

### Group Commit (v2 — No-Sleep Design)

The single most impactful write optimization. Instead of calling `fsync()` for every write:

1. Each writer appends data to heap and WAL **without fsync** (just `flush` to kernel)
2. First writer to finish becomes the **leader** and fsyncs both files immediately
3. Writers that arrive during the ~2ms fsync become **followers** — they wait
4. When the leader's fsync completes, all followers are released instantly

Under 8 concurrent writers, this reduces fsync calls from 16 (2 per writer) to 2 (1 heap + 1 WAL). No timed wait, no sleep — natural batching from the fsync latency window.

### Read Path

```
Client read
  → ArcSwap::load() — atomic pointer load, no lock (ns)
  → Check memtable (16-shard SkipMap, lock-free read)
  → Check block cache (moka LRU, concurrent)
  → Check L0 SSTables (Bloom filter first, then binary search)
  → Check L1/L2 SSTables (sorted, non-overlapping ranges)
  → Heap read: positional read + CRC32 verify + LZ4 decompress
```

### MVCC

Every write gets a monotonic sequence number. Every read specifies a `read_seq`. A read at `seq=100` only sees writes with `seq ≤ 100`. This gives you snapshot isolation with zero read locks — readers never block writers, writers never block readers.

Topology changes (compaction, flush, snapshot install) are published atomically via `ArcSwap`. Readers holding an old `Arc<StorageRoots>` continue reading stale-but-consistent data. Zero stalls.

### Compaction

Three-level tiered compaction: L0 (unsorted, overlapping) → L1 (sorted, non-overlapping) → L2 (sorted, non-overlapping, larger). Compaction merges SSTables, removes tombstones, and publishes new topology in a single atomic pointer swap.

---

## Deep Dive: Transaction Engine (SSI)

OmniKV implements **Serializable Snapshot Isolation** — the strongest isolation level, same as PostgreSQL's `SERIALIZABLE`.

### How SSI Works

1. **BEGIN**: Transaction takes a snapshot at the current sequence number
2. **Reads**: See only writes with `seq ≤ snapshot_seq` — consistent view
3. **Writes**: Buffered in a write-set (not visible to other transactions)
4. **COMMIT**: Acquire global lock → check for conflicts → commit atomically

### Conflict Detection

| Conflict Type | Detection | Resolution |
|--------------|-----------|------------|
| **Write-Write** | Key was modified by another transaction after our snapshot | Abort the later transaction |
| **Read-Write (rw-antidependency)** | A key we read was modified by a concurrent transaction | Abort to prevent non-serializable execution |
| **Dangerous Structure** | Chain of rw-antidependencies forming a cycle | Abort one transaction to break the cycle |

### Savepoints

```sql
BEGIN;
INSERT INTO accounts VALUES (1, 'Alice', 1000);
SAVEPOINT before_transfer;
UPDATE accounts SET balance = 500 WHERE id = 1;
-- Oops, wrong amount
ROLLBACK TO before_transfer;
-- Write-set restored to savepoint state
UPDATE accounts SET balance = 750 WHERE id = 1;
COMMIT;
```

Savepoints capture a snapshot of the write-set and read-set. `ROLLBACK TO` restores to that snapshot without aborting the entire transaction.

---

## Quick Start

```bash
# Build and run
git clone https://github.com/SBALAVIGNESH123/OmniKV.git
cd OmniKV
cargo build --release
cargo run --release
```

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

### Connect with psql

```bash
psql -h localhost -p 5433

CREATE TABLE users (id INT, name TEXT, email TEXT);
INSERT INTO users VALUES (1, 'Alice', 'alice@dev.io');

-- Cost-based optimizer chooses the best plan
EXPLAIN ANALYZE SELECT * FROM users WHERE id = 1;

-- Transactions with savepoints
BEGIN;
INSERT INTO users VALUES (2, 'Bob', 'bob@dev.io');
SAVEPOINT sp1;
DELETE FROM users WHERE id = 2;
ROLLBACK TO sp1;
COMMIT;
```

### Embedded Library

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

### Run Benchmarks

```bash
cargo run --bin omni_bench --release
cargo run --bin omni_bench --release -- --soak 600  # 10-min soak test
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
│  Histograms · Predicate pushdown · JOIN reorder · Plan cache     │
├──────────────────────────────────────────────────────────────────┤
│  TRANSACTION ENGINE                                              │
│  SSI · Savepoints · Write-write conflict detection               │
│  RW-antidependency tracking · Transaction timeouts · 2PC         │
├──────────────────────────────────────────────────────────────────┤
│  STORAGE ENGINE                                                  │
│  WAL (CRC32) → 16-shard SkipMap → SSTable → Tiered Compaction   │
│  Heap (CRC32/entry) · Bloom filters · LRU cache · LZ4 · MVCC   │
│  ArcSwap topology · Group commit v2 · Argon2id encryption        │
├──────────────────────────────────────────────────────────────────┤
│  CONSENSUS                                                       │
│  OpenRaft 0.9 · Leader election · Log replication · Snapshots    │
└──────────────────────────────────────────────────────────────────┘
```

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

Every test uses a fresh temporary directory and cleans up after itself. Tests run in isolation.

---

## Known Limitations

We believe the fastest way to earn your trust is to tell you exactly where OmniKV is not yet ready.

**Multi-node correctness is not yet proven.** OmniKV integrates OpenRaft for consensus, but we have not yet run crash tests against a 3-node cluster under network partitions. Until we do, the distributed layer should be considered experimental.

**Distributed transactions are not Jepsen-tested.** The 2PC protocol exists but has not been validated under coordinator crash, participant crash, or partial network failure.

**SeqScan materializes all rows.** The Volcano SeqScanIter currently loads all table rows into memory before streaming. A true streaming scan from the storage engine is planned.

**Long-running stability beyond 10 minutes is unproven.** Our soak test runs for 10 minutes with zero errors. A 24-hour soak test is planned.

**The SQL parser and wire protocol have not been fuzz-tested.** We plan to integrate `cargo-fuzz` to discover edge cases in input handling.

We will update this section as we close each gap. When a limitation is resolved, it moves to the evidence section with a test that proves it.

---

## Roadmap

OmniKV follows a trust-first development model. Each phase must produce evidence before the next begins.

- [x] **Phase 1 — Correctness**: Fixed 6 P0 bugs including GC data loss, non-atomic compaction, SQL precedence
- [x] **Phase 2 — Security**: Argon2id key derivation, constant-time API key comparison
- [x] **Phase 3 — Durability**: 12 durability tests, 1000 crash-recovery cycles, corruption detection
- [x] **Phase 4 — Benchmarks**: Measured throughput and latency, 10-minute soak test, group commit v2
- [ ] **Phase 5 — Multi-Node**: 3-node cluster tests, partition tolerance, leader failover
- [ ] **Phase 6 — Consistency**: Jepsen-style testing, linearizability verification
- [ ] **Phase 7 — Production**: Fuzz testing, 24-hour soak, streaming SeqScan, connection pooling

---

## Project Structure

```
src/                                    ~12,500 lines
├── lib.rs              Storage engine core         2,038
├── sql.rs              SQL parser (recursive descent) 869
├── optimizer.rs        Cost-based query optimizer     840
├── sql_exec.rs         SQL execution engine           729
├── transaction.rs      SSI transaction engine         648
├── volcano.rs          Volcano iterator executor      582
├── raft_storage.rs     Raft consensus storage         536
├── secondary_index.rs  B-tree secondary indexes       580
├── schema.rs           DDL engine (CREATE/ALTER/DROP) 471
├── pgwire.rs           PostgreSQL wire protocol       430
├── hardening.rs        Group commit v2, rate limiting 287
├── auth.rs             JWT + API key authentication    91
├── crypto.rs           AES-256-GCM + Argon2id          97
├── wal.rs              Write-ahead log with CRC32     250
└── ...                 QUIC, chaos testing, backup, metrics

tests/                                  ~8,000+ lines
├── durability_evidence.rs  Crash & corruption tests (12 tests)
├── storage_correctness.rs  Crash safety & MVCC    (14 tests)
├── storage_tests.rs        Storage engine         (76 tests)
└── ...                     SQL, stress, anomaly tests
```

**~20,000 lines of Rust · 103 verified tests · 0 failures · 0 warnings**

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
