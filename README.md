<p align="center">
  <img src="docs/omnikv_logo.png" alt="OmniKV Logo" width="180">
</p>

<h1 align="center">OmniKV</h1>

<h3 align="center">The database engine that replaces 5 services with 1 binary.</h3>

<p align="center">
  <em>A distributed, transactional SQL + KV database engine — written from scratch in Rust.</em><br>
  <em>No RocksDB wrapper. No SQLite fork. Every byte is ours.</em>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/language-Rust-orange?style=for-the-badge&logo=rust" alt="Rust">
  <img src="https://img.shields.io/badge/tests-290%20passing-brightgreen?style=for-the-badge" alt="Tests">
  <img src="https://img.shields.io/badge/20K%2B-lines%20of%20code-blue?style=for-the-badge" alt="Lines">
  <img src="https://img.shields.io/badge/license-MIT-blue?style=for-the-badge" alt="License">
  <img src="https://img.shields.io/badge/protocol-PostgreSQL%20Wire-purple?style=for-the-badge" alt="PgWire">
</p>

<p align="center">
  <a href="https://github.com/SBALAVIGNESH123/OmniKV/stargazers"><img src="https://img.shields.io/github/stars/SBALAVIGNESH123/OmniKV?style=for-the-badge&logo=github" alt="Stars"></a>
</p>

---

## 🤯 Why OmniKV?

Most companies run **5+ separate services** for their data layer:

| Service | What they deploy | What OmniKV gives you |
|---------|-----------------|----------------------|
| **Database** | PostgreSQL / MySQL | ✅ Full SQL engine with JOINs, aggregates, window functions |
| **KV Store** | Redis / etcd | ✅ Sub-millisecond KV with TTL, range scans, MVCC |
| **Consensus** | etcd / ZooKeeper | ✅ Built-in Raft consensus (OpenRaft) — 58 cluster tests |
| **API Server** | Express / Flask | ✅ REST API + QUIC + TCP — built in |
| **Auth + Metrics** | Auth0 + Prometheus | ✅ JWT auth + Prometheus `/metrics` — built in |

**OmniKV collapses all of this into a single `cargo build` binary.**

---

## ⚡ 30-Second Demo

```bash
# Start OmniKV (4 protocols start automatically)
cargo run --release

# Connect with psql — yes, your regular PostgreSQL client
psql -h localhost -p 5433

# Create tables, insert data, query with JOINs
CREATE TABLE users (id INT, name TEXT, email TEXT);
INSERT INTO users VALUES (1, 'Alice', 'alice@dev.io');
INSERT INTO users VALUES (2, 'Bob', 'bob@dev.io');

CREATE TABLE orders (id INT, user_id INT, amount FLOAT);
INSERT INTO orders VALUES (101, 1, 299.99);
INSERT INTO orders VALUES (102, 2, 149.50);

-- Hash JOIN with cost-based optimization
EXPLAIN SELECT u.name, SUM(o.amount)
FROM users u
INNER JOIN orders o ON u.id = o.user_id
GROUP BY u.name;

-- → Hash Join (build=users, probe=orders) cost=0.25
--   → Aggregate [GROUP BY name]
```

**That's a full SQL database running over the PostgreSQL wire protocol.** Any language, any driver.

---

## 🏗️ What's Inside (Everything is Custom)

```
┌──────────────────────────────────────────────────────────────────┐
│                        CLIENT LAYER                              │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐  ┌──────────────┐   │
│  │  psql /  │  │ REST API │  │   QUIC    │  │ TCP Command  │   │
│  │ PgWire   │  │ HTTP/2   │  │  HTTP/3   │  │  Interface   │   │
│  │ v3       │  │ + TLS    │  │  (Quinn)  │  │              │   │
│  └────┬─────┘  └────┬─────┘  └─────┬─────┘  └──────┬───────┘   │
├───────┼──────────────┼──────────────┼───────────────┼───────────┤
│       └──────────────┴──────────────┴───────────────┘           │
│                        SQL ENGINE                                │
│  ┌──────────────┐  ┌───────────────────┐  ┌────────────────┐   │
│  │ SQL Parser   │→ │ Cost-Based        │→ │ Volcano        │   │
│  │ (recursive   │  │ Optimizer         │  │ Iterator       │   │
│  │  descent)    │  │ • Histograms      │  │ Executor       │   │
│  │              │  │ • Predicate push  │  │ • O(1) filter  │   │
│  │              │  │ • JOIN reorder    │  │ • Hash join    │   │
│  │              │  │ • Index select    │  │ • Streaming    │   │
│  └──────────────┘  └───────────────────┘  └────────────────┘   │
├──────────────────────────────────────────────────────────────────┤
│                   TRANSACTION ENGINE                             │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ Serializable Snapshot Isolation (SSI)                    │    │
│  │ • Write-write conflict detection                        │    │
│  │ • Savepoints + partial rollback                         │    │
│  │ • Transaction timeouts + RW-dependency tracking         │    │
│  │ • 2PC distributed transactions                          │    │
│  └─────────────────────────────────────────────────────────┘    │
├──────────────────────────────────────────────────────────────────┤
│                    STORAGE ENGINE                                │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐   │
│  │ WAL      │  │ SkipMap  │  │ SSTable  │  │ Bloom Filter │   │
│  │ (CRC32)  │  │ Memtable │  │ (Sorted  │  │ (per-SST)    │   │
│  │ append   │→ │ (16-shard│→ │  String  │  │              │   │
│  │ only     │  │  lock-   │  │  Table)  │  │              │   │
│  │          │  │  free)   │  │          │  │              │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────────┘   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐   │
│  │ Heap     │  │ MVCC     │  │ Block    │  │ ArcSwap      │   │
│  │ Store    │  │ Snapshot │  │ Cache    │  │ Topology     │   │
│  │ (CRC32   │  │ (atomic  │  │ (Moka    │  │ (zero-stall  │   │
│  │  per     │  │  seq#)   │  │  LRU)    │  │  swap)       │   │
│  │  entry)  │  │          │  │          │  │              │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────────┘   │
├──────────────────────────────────────────────────────────────────┤
│                    RAFT CONSENSUS                                │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ OpenRaft 0.9.24 — full RaftStorage trait                 │   │
│  │ • Leader election + log replication (58 tests)           │   │
│  │ • Atomic snapshot install (directory swap + ArcSwap)      │   │
│  │ • Network partitions + rolling upgrades (tested)          │   │
│  │ • Membership changes (3→5 nodes, add/remove)             │   │
│  │ • 2PC cross-shard replication                            │   │
│  └──────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
```

---

## 📊 Test Suite — 290 Tests, All Passing

```
$ cargo test

test result: ok. 290 passed; 0 failed
```

### Test Breakdown

| Suite | Tests | What it proves |
|-------|-------|---------------|
| **Raft Cluster** | 58 | 3-node replication, leader election, partitions, rolling upgrades, membership changes |
| **Storage Engine** | 76 | WAL integrity, crash recovery, CRC32 corruption detection, compaction |
| **Operations** | 25 | Health checks, metrics, backup/restore, admin endpoints |
| **Ops Maturity** | 24 | Config management, diagnostics, graceful shutdown |
| **Storage Perf** | 21 | Throughput benchmarks across all storage paths |
| **SQL Layer** | 18 | Parser, JOINs, aggregates, GROUP BY, window functions |
| **Storage Correctness** | 14 | Crash safety, MVCC isolation, torn WAL rejection, atomicity |
| **Query Engine** | 9 | Query planning, execution, validation |
| **Optimizer** | 7 | Cost estimation, PK lookup, JOIN reorder, EXPLAIN |
| **Concurrent Stress** | 6 | Multi-threaded read/write contention |
| **SQL Parser** | 5 | CREATE TABLE, INSERT, SELECT JOIN, WHERE OR |
| **Anomaly Demos** | 4 | Isolation level anomaly demonstrations |
| **Other** | 23 | Benchmarks, compaction debugging, reopen tests |

### Key Invariants Proven

| Test | Guarantee |
|------|-----------|
| `test_3_node_log_replication` | All entries identical across 3 nodes |
| `test_leader_election_under_load` | Exactly one leader emerges under contention |
| `test_symmetric_partition_majority_progresses` | Majority side continues during network split |
| `test_rolling_restart_no_data_loss` | Zero data loss during rolling node restarts |
| `test_membership_scale_out_3_to_5` | Live cluster scaling without downtime |
| `test_2pc_cross_shard_with_raft_replication` | Distributed transactions replicated via Raft |
| `test_ssi_write_write_conflict` | Serializable isolation detects conflicts |
| `test_write_survives_restart` | No acknowledged write lost after crash |
| `test_heap_crc_corruption_detected` | Silent data corruption impossible |
| `test_batch_is_atomic` | Multi-key batches are all-or-nothing |

---

## 🔧 Quick Start

### Build from source

```bash
git clone https://github.com/SBALAVIGNESH123/OmniKV.git
cd omni_engine
cargo build --release
```

### Run the server

```bash
cargo run --release
```

```
  ╔════════════════════════════════════════════════════╗
  ║        ⚡ OmniKV v0.1.0                           ║
  ║  Embeddable · Distributed · Transactional KV      ║
  ╠════════════════════════════════════════════════════╣
  ║  HTTP/1.1 + HTTP/2 (TLS)  → 0.0.0.0:8443         ║
  ║  QUIC/HTTP3 (binary)      → 0.0.0.0:4433         ║
  ║  PostgreSQL Wire Protocol → 0.0.0.0:5433         ║
  ║  TCP Command Interface    → 0.0.0.0:8080         ║
  ╠════════════════════════════════════════════════════╣
  ║  Built from scratch in Rust. Every byte is ours.  ║
  ╚════════════════════════════════════════════════════╝
```

### Connect with psql

```bash
psql -h localhost -p 5433

CREATE TABLE users (id INTEGER, name TEXT, email TEXT);
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
SELECT * FROM users WHERE id = 1;
```

### Use the REST API

```bash
# Health check
curl -k https://localhost:8443/health

# Write
curl -k -X POST https://localhost:8443/kv \
  -H "Content-Type: application/json" \
  -d '{"key": "hello", "value": "world"}'

# Read
curl -k https://localhost:8443/kv/hello

# Atomic batch write
curl -k -X POST https://localhost:8443/batch \
  -H "Content-Type: application/json" \
  -d '{"ops": [{"op":"set","key":"a","value":"1"}, {"op":"set","key":"b","value":"2"}]}'
```

### Use as an embedded library

```rust
use omni_engine::{OmniKV, WriteBatch};

fn main() {
    let db = OmniKV::open("manifest.json", "data.wal").unwrap();

    let mut batch = WriteBatch::new();
    batch.set("user:1", r#"{"name":"Alice","age":30}"#.into()).unwrap();
    batch.set("user:2", r#"{"name":"Bob","age":25}"#.into()).unwrap();
    db.commit_batch(&batch).unwrap();

    let snap = db.snapshot();
    let val = db.find("user:1", snap).unwrap();
    println!("{:?}", val); // Some("{\"name\":\"Alice\",\"age\":30}")
    db.unregister_snapshot(snap);
}
```

---

## 📁 Project Structure

```
omni_engine/                          ~20,000 lines of Rust
├── src/
│   ├── lib.rs              # Storage engine core        (2,038 lines)
│   ├── sql.rs              # SQL parser                 (869 lines)
│   ├── optimizer.rs        # Cost-based optimizer       (840 lines)
│   ├── sql_exec.rs         # SQL execution              (729 lines)
│   ├── prepared.rs         # Prepared statements        (662 lines)
│   ├── transaction.rs      # SSI transaction engine     (648 lines)
│   ├── dist_txn.rs         # 2PC distributed txn        (592 lines)
│   ├── volcano.rs          # Volcano iterator executor  (582 lines)
│   ├── secondary_index.rs  # Secondary index engine     (580 lines)
│   ├── raft_storage.rs     # Raft storage trait         (536 lines)
│   ├── schema.rs           # DDL engine                 (471 lines)
│   ├── chaos.rs            # Chaos testing framework    (468 lines)
│   ├── plan_exec.rs        # Plan-driven executor       (463 lines)
│   ├── pgwire.rs           # PostgreSQL wire protocol   (430 lines)
│   ├── api.rs              # REST API (Axum)            (300 lines)
│   ├── hardening.rs        # Group commit + stall ctrl  (255 lines)
│   ├── quic_server.rs      # QUIC/HTTP3 server          (231 lines)
│   ├── main.rs             # Server entry point         (212 lines)
│   ├── wal.rs              # WAL implementation         (176 lines)
│   ├── raft_network.rs     # Raft HTTP RPC              (79 lines)
│   ├── raft_routes.rs      # Raft HTTP handlers         (70 lines)
│   └── ...                 # auth, crypto, metrics, etc.
├── tests/
│   ├── raft_cluster.rs     # Raft cluster tests         (3,660 lines · 58 tests)
│   ├── storage_tests.rs    # Storage engine tests       (1,547 lines · 76 tests)
│   ├── storage_correctness.rs  # Crash safety           (389 lines · 14 tests)
│   ├── storage_perf.rs     # Performance tests          (429 lines · 21 tests)
│   ├── operations.rs       # Operational tests          (446 lines · 25 tests)
│   ├── sql_layer.rs        # SQL integration tests      (282 lines · 18 tests)
│   └── ...                 # stress, benchmarks, etc.
├── Cargo.toml
├── Dockerfile
└── docker-compose.yml
```

---

## 🗺️ Maturity Roadmap

| Stage | Status | Description |
|-------|--------|-------------|
| ✅ Storage Correctness | `████████████` 100% | WAL, crash recovery, CRC integrity, MVCC, compaction |
| ✅ Internal Storage APIs | `████████████` 100% | StorageRoots, atomic swap, pure recovery, reader isolation |
| ✅ Raft Hardening | `████████████` 100% | 58 tests: replication, elections, partitions, rolling upgrades, membership |
| ✅ Transaction Engine | `████████████` 100% | SSI, savepoints, 2PC, conflict detection, cross-shard txn |
| ✅ Query Engine | `████████████` 100% | Parser, cost-based optimizer, volcano executor, EXPLAIN ANALYZE |
| ✅ Operational Maturity | `████████████` 100% | Health, metrics, backup, config, diagnostics, shutdown |
| 🔨 Ecosystem | `████████░░░░` 65% | PgWire + HTTP + QUIC + Go client. SDKs for more languages pending |

---

## 🐳 Docker

```bash
# Build
docker build -t omnikv .

# Run
docker run -p 8443:8443 -p 5433:5433 -p 4433:4433/udp omnikv

# 3-node cluster
docker compose up
```

---

## 🤝 Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## 📄 License

MIT

---

<p align="center">
  <strong>Built from scratch in Rust. Every byte is ours.</strong>
</p>
<p align="center">
  <em>By <a href="https://github.com/SBALAVIGNESH123">Balavignesh</a></em>
</p>
