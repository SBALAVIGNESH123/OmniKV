# OmniKV — Distributed Transactional Key-Value Engine

<p align="center">
  <img src="logo.svg" alt="OmniKV Logo" width="500">
</p>

<p align="center">
  <strong>Embeddable. Distributed. Unstoppable.</strong><br>
  <em>A production-grade distributed database written from scratch in Rust</em>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/language-Rust-orange" alt="Rust">
  <img src="https://img.shields.io/badge/tests-76%20passing-brightgreen" alt="Tests">
  <img src="https://img.shields.io/badge/license-MIT-blue" alt="License">
  <img src="https://img.shields.io/badge/protocol-PostgreSQL%20Wire-purple" alt="PgWire">
  <a href="https://discord.gg/cqfzNzGMt"><img src="https://img.shields.io/badge/Discord-Join%20us-5865F2?logo=discord&logoColor=white" alt="Discord"></a>
</p>

---

## What is OmniKV?

OmniKV is an **embeddable, transactional, distributed key-value store** with:

- 🔒 **SSI Transactions** — PostgreSQL-grade serializable snapshot isolation
- 🌐 **PostgreSQL Wire Protocol** — Connect from any language (psql, JDBC, Python)
- ⚡ **LSM-Tree Storage** — Custom 3-phase pipelined writes for high throughput
- 🗜️ **Automatic Compression** — LZ4 for large values, inline for small values
- 🔍 **Secondary Indexes** — Composite indexes with range scan support
- 📊 **Prepared Statements** — LRU plan cache eliminates re-parsing overhead
- 🔄 **Online Schema Evolution** — Zero-downtime migrations with rollback
- 🧪 **Jepsen-Style Chaos Testing** — Crash recovery, write skew detection, CRC verification
- 🚀 **QUIC Transport** — Modern UDP-based networking for Raft consensus
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
| `lib.rs` | Core storage engine (LSM-tree, memtable, compaction, MVCC) | ~1500 |
| `transaction.rs` | SSI transaction manager with conflict detection | ~300 |
| `query.rs` | SQL parser (SELECT, INSERT, UPDATE, DELETE) | ~250 |
| `prepared.rs` | Prepared statements with LRU plan cache | ~250 |
| `secondary_index.rs` | B-tree secondary indexes with composite key support | ~250 |
| `schema.rs` | Online schema evolution (migrations, backfill) | ~300 |
| `pgwire.rs` | PostgreSQL wire protocol v3 server | ~400 |
| `hardening.rs` | Group commit, rate limiting, connection pooling | ~250 |
| `chaos.rs` | Jepsen-style chaos testing framework | ~450 |
| `wal.rs` | Write-ahead log with segmented rotation | ~200 |
| `metrics_prometheus.rs` | Prometheus metrics exporter | ~50 |
| `raft_network.rs` | QUIC/HTTP transport for Raft consensus | ~80 |

## Test Suite

```
68 tests, 0 failures

Storage Engine ............ 16 tests
Query Parser .............. 9 tests
SSI Transactions .......... 8 tests
Secondary Indexes ......... 7 tests
Prepared Statements ....... 12 tests
Schema Evolution .......... 9 tests
Production Hardening ...... 9 tests (group commit, rate limiting, error handling)
Chaos/Safety .............. 7 tests (crash recovery, write skew, CRC integrity)
```

## Benchmarks

Run with:
```bash
cargo test --test benchmarks --release -- --nocapture
```

## Building

```bash
# Build
cargo build --release

# Run tests
cargo test --test storage_tests -- --test-threads=1

# Run benchmarks
cargo test --test benchmarks --release -- --nocapture
```

## License

MIT

---

*Built from scratch in Rust. No RocksDB wrapper. No SQLite fork. Every byte is ours.*
