# OmniKV — Distributed Transactional Key-Value Engine

<p align="center">
  <img src="logo.svg" alt="OmniKV Logo" width="500">
</p>

<p align="center">
  <strong>Embeddable. Transactional. Distributed.</strong><br>
  <em>An experimental distributed database engine written from scratch in Rust</em>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/language-Rust-orange" alt="Rust">
  <img src="https://img.shields.io/badge/tests-80%20passing-brightgreen" alt="Tests">
  <img src="https://img.shields.io/badge/license-MIT-blue" alt="License">
  <img src="https://img.shields.io/badge/protocol-PostgreSQL%20Wire-purple" alt="PgWire">
  <a href="https://discord.gg/cqfzNzGMt"><img src="https://img.shields.io/badge/Discord-Join%20us-5865F2?logo=discord&logoColor=white" alt="Discord"></a>
</p>

---

## What is OmniKV?

OmniKV is an **embeddable, transactional, distributed key-value store** with:

- 🔒 **SSI Transactions** — Serializable snapshot isolation (SSI-inspired, conservative validation)
- 🌐 **PostgreSQL Wire Protocol** — Connect from any language (psql, JDBC, Python)
- ⚡ **LSM-Tree Storage** — Custom 3-phase pipelined writes for high throughput
- 🗜️ **Automatic Compression** — LZ4 for large values, inline for small values
- 🔍 **Secondary Indexes** — Composite indexes with range scan support
- 📊 **Prepared Statements** — LRU plan cache eliminates re-parsing overhead
- 🔄 **Online Schema Evolution** — Zero-downtime migrations with rollback
- 🧪 **Crash Recovery & Concurrency Testing** — Write skew detection, anomaly prevention demos, CRC verification
- 🚀 **Experimental QUIC Transport** — UDP-based networking layer (Quinn)
- 📈 **Prometheus Metrics** — Built-in observability

## Quick Start

### Connect via psql

```bash
# Start OmniKV with PostgreSQL wire protocol
cargo run -- --pgwire-port 5433

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

tm.set(&mut txn, "account/A", "balance:900".into())?;
tm.set(&mut txn, "account/B", "balance:1100".into())?;

tm.commit(&mut txn)?; // Atomic commit — both or neither
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Client Layer                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │  psql    │  │  JDBC    │  │ Rust API │              │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘              │
│       └──────────────┼────────────┘                     │
│              ┌───────┴───────┐                          │
│              │ PgWire Server │ (PostgreSQL Wire Proto)  │
│              └───────┬───────┘                          │
├──────────────────────┼──────────────────────────────────┤
│              ┌───────┴───────┐                          │
│              │ Query Engine  │                           │
│              │  Parser       │                           │
│              │  Plan Cache   │                           │
│              │  Prepared Stmts│                          │
│              └───────┬───────┘                          │
├──────────────────────┼──────────────────────────────────┤
│              ┌───────┴───────┐                          │
│              │  Transaction  │  SSI (Serializable       │
│              │   Manager     │   Snapshot Isolation)    │
│              └───────┬───────┘                          │
├──────────────────────┼──────────────────────────────────┤
│              ┌───────┴───────┐                          │
│              │   OmniKV      │  Storage Engine          │
│              │               │                          │
│   ┌──────────┼──────────┐    │                          │
│   │ SkipMap  │  Heap    │    │  In-Memory               │
│   │ Memtable │  File    │    │                          │
│   └──────────┼──────────┘    │                          │
│   ┌──────────┼──────────┐    │                          │
│   │  L0 SST  │  L1 SST │    │  On-Disk (LSM-Tree)     │
│   │  + Bloom │  + Bloom │    │                          │
│   └──────────┼──────────┘    │                          │
│              └───────┬───────┘                          │
├──────────────────────┼──────────────────────────────────┤
│              ┌───────┴───────┐                          │
│              │   WAL + Raft  │  Durability + Consensus  │
│              │  (OpenRaft)   │                          │
│              └───────────────┘                          │
└─────────────────────────────────────────────────────────┘
```

## Module Map

| Module | Description | Lines |
|--------|-------------|-------|
| `lib.rs` | Core storage engine (LSM-tree, memtable, compaction, MVCC) | ~1525 |
| `transaction.rs` | SSI transaction manager with rw-dependency graph | ~330 |
| `dist_txn.rs` | Distributed two-phase commit (2PC) protocol | ~547 |
| `sql.rs` | SQL parser v2 (CREATE TABLE, JOIN, GROUP BY, aggregates) | ~450 |
| `sql_exec.rs` | SQL executor with hash JOIN, WHERE OR/IN/LIKE | ~320 |
| `catalog.rs` | Table catalog with typed columns (INT, TEXT, BOOL, etc.) | ~170 |
| `query.rs` | Legacy KV query parser | ~241 |
| `pgwire.rs` | PostgreSQL wire protocol v3 (wired to SQL v2 engine) | ~460 |
| `prepared.rs` | Prepared statements with LRU plan cache | ~635 |
| `secondary_index.rs` | B-tree secondary indexes | ~542 |
| `schema.rs` | Online schema evolution | ~503 |
| `hardening.rs` | Group commit, rate limiting | ~285 |
| `chaos.rs` | Chaos testing framework | ~461 |

## Test Suite

```
80 tests, 0 failures

Storage Engine ............ 16 tests
Query Parser .............. 9 tests
SSI Transactions .......... 8 tests
SSI Anomaly Demos ......... 4 tests (write skew, lost update, snapshot, counter)
Secondary Indexes ......... 7 tests
Prepared Statements ....... 12 tests
Schema Evolution .......... 9 tests
Production Hardening ...... 9 tests
Chaos/Safety .............. 6 tests
```

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

## Limitations

| Area | Status | Detail |
|------|--------|--------|
| SSI | ✅ Real | rw-dependency graph. Aborts more aggressively than PostgreSQL (safe). |
| 2PC | ✅ Real | Timeout-based abort on coordinator failure. |
| Raft | ⚠️ Scaffolded | Not end-to-end tested in multi-node yet. |
| PgWire | ⚠️ Thread-per-conn | Fine for <100 connections. |
| SQL | ✅ Growing | JOINs, aggregates work. No subqueries yet. |
| Benchmarks | ✅ Honest | In-memory, single-thread numbers. |

## Building

```bash
cargo build --release
cargo test --test storage_tests -- --test-threads=1
cargo test --test anomaly_demos -- --test-threads=1
cargo test --test benchmarks --release -- --nocapture
```

## License

MIT

---

*Built from scratch in Rust. No RocksDB wrapper. No SQLite fork. Every byte is ours.*


