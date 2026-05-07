# OmniKV — Distributed Transactional Key-Value Engine

<p align="center">
  <img src="logo.svg" alt="OmniKV Logo" width="500">
</p>

<p align="center">
  <strong>Embeddable. Transactional. Distributed.</strong><br>
  <em>A production-grade distributed database engine written from scratch in Rust</em>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/language-Rust-orange" alt="Rust">
  <img src="https://img.shields.io/badge/tests-223%20passing-brightgreen" alt="Tests">
  <img src="https://img.shields.io/badge/source-10K%2B%20lines-blue" alt="Lines">
  <img src="https://img.shields.io/badge/license-MIT-blue" alt="License">
  <img src="https://img.shields.io/badge/protocol-PostgreSQL%20Wire-purple" alt="PgWire">
  <a href="https://discord.gg/cqfzNzGMt"><img src="https://img.shields.io/badge/Discord-Join%20us-5865F2?logo=discord&logoColor=white" alt="Discord"></a>
</p>

---

## What is OmniKV?

OmniKV is a **production-grade, distributed transactional key-value store** built entirely from scratch in Rust — no RocksDB wrapper, no SQLite fork. Every byte is ours.

### Core Features

- 🔒 **SSI Transactions** — Full Serializable Snapshot Isolation with rw-dependency graphs, dangerous structure detection (PostgreSQL-compatible), savepoints, and configurable timeouts
- ⚡ **Striped Commit Locks** — 64-stripe lock array allows non-overlapping transactions to commit in parallel
- 🌐 **Raft Consensus** — Battle-tested with 58 distributed tests covering network partitions, clock skew, membership changes, and rolling upgrades
- 🔄 **2PC Distributed Transactions** — Two-phase commit with WAL-backed coordinator recovery, cross-shard atomicity, and SSI integration
- 📡 **PostgreSQL Wire Protocol** — Connect from any language (psql, JDBC, Python, Go)
- ⚡ **LSM-Tree Storage** — Custom 3-phase pipelined writes with bloom filters and tiered compaction
- 🗜️ **Automatic Compression** — LZ4 for large values, inline for small values
- 🔍 **Secondary Indexes** — Composite indexes with range scan support
- 📊 **Prepared Statements** — LRU plan cache eliminates re-parsing overhead
- 🔄 **Online Schema Evolution** — Zero-downtime migrations with rollback
- 🧪 **Crash Recovery** — WAL-based recovery, CRC verification, snapshot installation
- 🚀 **QUIC Transport** — UDP-based networking layer (Quinn)
- 📈 **Prometheus Metrics** — Built-in observability for transactions, storage, and Raft

---

## Quick Start

### Connect via psql

```bash
# Start OmniKV
cargo run --release

# Connect with any PostgreSQL client
psql -h localhost -p 5433 -U omni

# Execute queries
omni=> INSERT INTO users (id, name, email) VALUES ('1', 'Alice', 'alice@example.com');
INSERT 0 1

omni=> SELECT * FROM users WHERE name = 'Alice';
 key  |                          value
------+----------------------------------------------------------
 1    | {"id":"1","name":"Alice","email":"alice@example.com"}
(1 row)
```

### Embedded Mode (Rust)

```rust
use omni_engine::{OmniKV, WriteBatch};

let db = OmniKV::open("data/manifest", "data/wal")?;

// Write
let mut batch = WriteBatch::new();
batch.set("users/1", r#"{"name": "Alice"}"#.to_string())?;
db.commit_batch(&batch)?;

// Read
let value = db.find("users/1", db.get_seq())?;
println!("{:?}", value); // Some("{\"name\": \"Alice\"}")
```

### Transactions (SSI)

```rust
use omni_engine::transaction::TransactionManager;

let tm = TransactionManager::new(db.clone());
let mut txn = tm.begin();

// Read-your-own-writes
tm.set(&mut txn, "account/A", "balance:900".into())?;
tm.set(&mut txn, "account/B", "balance:1100".into())?;

// Savepoint support
tm.savepoint(&mut txn, "before_transfer")?;
tm.set(&mut txn, "account/C", "balance:500".into())?;
tm.rollback_to_savepoint(&mut txn, "before_transfer")?; // undo C

tm.commit(&mut txn)?; // Atomic — both A and B, but not C
```

### Distributed Transactions (2PC)

```rust
use omni_engine::dist_txn::{TwoPhaseCoordinator, TwoPhaseParticipant};

let coordinator = TwoPhaseCoordinator::new(1, db.clone(), 5000);
let txn_id = coordinator.begin();

// Add writes across shards
coordinator.add_write(txn_id, 10, "alice".into(), Some("800".into()), 0)?;
coordinator.add_write(txn_id, 20, "bob".into(), Some("1200".into()), 0)?;

// Prepare → Vote → Commit (atomic across all participants)
coordinator.prepare(txn_id)?;
// ... participants vote COMMIT ...
coordinator.finalize_commit(txn_id)?;
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      Client Layer                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐     │
│  │  psql    │  │  JDBC    │  │ Rust API │  │  QUIC    │     │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘     │
│       └──────────────┼────────────┘              │           │
│              ┌───────┴───────┐          ┌────────┴────────┐  │
│              │ PgWire Server │          │  QUIC Server    │  │
│              └───────┬───────┘          └────────┬────────┘  │
├──────────────────────┼───────────────────────────┘           │
│              ┌───────┴───────┐                               │
│              │ Query Engine  │  SQL Parser, Plan Cache,      │
│              │               │  Prepared Stmts, JOINs        │
│              └───────┬───────┘                               │
├──────────────────────┼───────────────────────────────────────┤
│              ┌───────┴───────┐                               │
│              │  Transaction  │  SSI + Striped Locks          │
│              │   Manager     │  Savepoints, Timeouts         │
│              │               │  RW-Dependency Graph          │
│              └───────┬───────┘                               │
├──────────────────────┼───────────────────────────────────────┤
│   ┌──────────────────┼──────────────────────┐                │
│   │          ┌───────┴───────┐              │                │
│   │          │   OmniKV      │              │                │
│   │          │  Storage Core │              │                │
│   │  ┌───────┴───────┐ ┌────┴──────┐       │                │
│   │  │ Sharded       │ │ Heap File │       │                │
│   │  │ SkipMap       │ │ Manager   │       │                │
│   │  └───────┬───────┘ └────┬──────┘       │                │
│   │  ┌───────┴───────┐ ┌────┴──────┐       │                │
│   │  │ L0/L1 SSTs    │ │ Bloom     │       │                │
│   │  │ + Compaction  │ │ Filters   │       │                │
│   │  └───────────────┘ └───────────┘       │                │
│   └────────────────────────────────────────┘                │
├──────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────┐    │
│  │  WAL Engine │  │ Raft Cluster │  │  2PC Coordinator │    │
│  │  (CRC32)    │  │ (OpenRaft)   │  │  + Participants  │    │
│  └─────────────┘  └──────────────┘  └──────────────────┘    │
└──────────────────────────────────────────────────────────────┘
```

---

## Module Map

| Module | Description | Lines |
|--------|-------------|-------|
| `lib.rs` | Core storage engine — LSM-tree, sharded memtable, MVCC, compaction, bloom filters | 2,257 |
| `sql.rs` | SQL parser v2 — CREATE TABLE, JOIN, GROUP BY, aggregates, WHERE OR/IN/LIKE | 932 |
| `prepared.rs` | Prepared statements with LRU plan cache | 732 |
| `sql_exec.rs` | SQL executor with hash JOIN, aggregation, ORDER BY | 720 |
| `dist_txn.rs` | 2PC distributed transactions — coordinator, participant, WAL recovery | 666 |
| `transaction.rs` | SSI transaction manager — striped locks, rw-deps, savepoints, metrics | 647 |
| `raft_storage.rs` | Raft storage trait implementation — log, snapshots, compaction | 604 |
| `secondary_index.rs` | B-tree secondary indexes with composite key support | 579 |
| `chaos.rs` | Chaos testing framework | 522 |
| `schema.rs` | Online schema evolution with zero-downtime migrations | 522 |
| `pgwire.rs` | PostgreSQL wire protocol v3 server | 488 |
| `query.rs` | Legacy KV query parser | 303 |
| `hardening.rs` | Group commit engine, rate limiting | 286 |
| `quic_server.rs` | QUIC/HTTP3 binary protocol (Quinn) | 265 |
| `main.rs` | Multi-protocol server (HTTP/2 + QUIC + PgWire + TCP) | 239 |
| `wal.rs` | Write-ahead log with CRC32 checksums | 203 |
| `catalog.rs` | Table catalog with typed columns (INT, TEXT, BOOL, FLOAT) | 182 |
| `bench.rs` | Benchmark harness | 188 |
| `api.rs` | REST API (Axum) — CRUD, batch, scan, backup, auth | 330 |
| `raft_network.rs` | Raft HTTP networking with connection pooling | 88 |
| `raft_routes.rs` | Raft RPC HTTP route handlers | 77 |
| `raft_init.rs` | Raft cluster initialization | 63 |
| **Total** | | **~10,800** |

---

## Test Suite

```
223 tests, 0 failures

Storage Engine .................. 76 tests  (storage_tests.rs)
Raft Consensus .................. 58 tests  (raft_cluster.rs)
  ├── Core replication & election ........... 7
  ├── Network partitions (5-node) .......... 5
  ├── Message reordering ................... 5
  ├── Clock skew tolerance ................. 5
  ├── 2PC distributed transactions ......... 5
  ├── SSI conflict detection ............... 5
  ├── Transaction intents .................. 5
  ├── Retry with backoff ................... 5
  ├── Range queries ........................ 5
  ├── Membership changes ................... 5
  └── Rolling upgrades ..................... 5
Operations ...................... 25 tests  (operations.rs)
Storage Engine .................. 19 tests  (storage_engine.rs)
SQL Layer ....................... 18 tests  (sql_layer.rs)
Storage Correctness ............. 14 tests  (storage_correctness.rs)
Concurrent Stress ............... 6 tests   (concurrent_stress.rs)
  ├── 4-thread counter contention .......... 1
  ├── Disjoint key parallelism ............. 1
  ├── Hot key contention (8 threads) ....... 1
  ├── Mixed read-write workload ............ 1
  ├── Savepoints under concurrency ......... 1
  └── Metrics accuracy .................... 1
SSI Anomaly Prevention .......... 4 tests   (anomaly_demos.rs)
  ├── Write skew prevention ................ 1
  ├── Lost update prevention ............... 1
  ├── Snapshot consistency ................. 1
  └── Concurrent counter correctness ....... 1
Debug/Misc ...................... 3 tests
```

---

## Production Readiness

| Area | Status | Detail |
|------|--------|--------|
| **Storage Engine** | ✅ Production | LSM-tree with bloom filters, compaction, CRC-verified WAL |
| **SSI Transactions** | ✅ Production | Striped locks, rw-dependency graph, savepoints, timeouts, metrics |
| **Raft Consensus** | ✅ Hardened | 58 tests: partitions, clock skew, membership changes, rolling upgrades |
| **2PC Distributed** | ✅ Tested | Coordinator WAL recovery, cross-shard atomicity, concurrent txns |
| **Concurrency** | ✅ Stress-tested | 6 multi-threaded tests under real parallel contention |
| **SQL Engine** | ✅ Growing | JOINs, aggregates, WHERE OR/IN/LIKE. No subqueries yet. |
| **PgWire** | ⚠️ Thread-per-conn | Fine for <100 connections. Async migration planned. |
| **Benchmarks** | ✅ Honest | In-memory, single-thread numbers (see below). |

---

## Benchmarks

Single machine, single-threaded, in-memory dataset:

| Operation | Throughput | Notes |
|-----------|-----------|-------|
| Sequential Writes (100B) | 809 ops/sec | Individual commits with WAL fsync |
| Batch Writes (100 keys) | 49,381 ops/sec (4.7 MB/s) | Amortized WAL cost |
| Random Point Reads | 540,809 ops/sec | MVCC-filtered |
| Range Scan (1K range) | 988,043 rows/sec | Sequential iteration |
| SSI Transactions | 888 txns/sec | Full conflict detection |
| Mixed (80/20 R/W) | 4,255 ops/sec | Realistic workload |

```bash
cargo test --test benchmarks --release -- --nocapture
```

---

## Building

```bash
# Build
cargo build --release

# Run all tests
cargo test -- --test-threads=1

# Run specific test suites
cargo test --test storage_tests -- --test-threads=1
cargo test --test raft_cluster -- --test-threads=1
cargo test --test concurrent_stress -- --nocapture
cargo test --test anomaly_demos -- --nocapture

# Run benchmarks
cargo test --test benchmarks --release -- --nocapture
```

## Protocols & Ports

| Protocol | Port | Description |
|----------|------|-------------|
| HTTP/1.1 + HTTP/2 (TLS) | 8443 | REST API with JWT auth |
| QUIC/HTTP3 | 4433 | Binary protocol |
| PostgreSQL Wire | 5433 | SQL interface |
| TCP Command | 8080 | Telnet/debug interface |

---

## License

MIT

---

*Built from scratch in Rust. No RocksDB wrapper. No SQLite fork. Every byte is ours.*
