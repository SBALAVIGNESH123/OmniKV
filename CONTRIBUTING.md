# Contributing to OmniKV

Thank you for your interest in contributing to OmniKV!

## Getting Started

```bash
git clone https://github.com/SBALAVIGNESH123/OmniKV.git
cd OmniKV
cargo build
cargo test --test storage_tests -- --test-threads=1
```

## Running the Full Test Suite

```bash
# Storage (130+ tests)
cargo test --test storage_tests -- --test-threads=1
cargo test --test storage_correctness -- --test-threads=1
cargo test --test storage_engine -- --test-threads=1
cargo test --test storage_perf -- --test-threads=1

# Transactions & Concurrency
cargo test --test anomaly_demos -- --test-threads=1
cargo test --test concurrent_stress -- --test-threads=1

# SQL
cargo test --test sql_layer -- --test-threads=1

# Raft Consensus
cargo test --test raft_cluster -- --test-threads=1

# Operational
cargo test --test ops_maturity -- --test-threads=1
```

## Architecture

- `src/lib.rs` — Core LSM-tree storage engine
- `src/transaction.rs` — SSI transaction manager
- `src/dist_txn.rs` — 2PC distributed transactions
- `src/sql.rs` + `src/sql_exec.rs` — SQL parser and executor
- `src/pgwire.rs` — PostgreSQL wire protocol
- `src/raft_storage.rs` — Raft consensus storage
- `src/ops.rs` — Operational config and diagnostics

## Code Style

- Run `cargo fmt` before committing
- Run `cargo clippy --lib` and fix warnings
- All new features must have tests

## Pull Request Process

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Write tests for your changes
4. Ensure all tests pass
5. Submit a pull request

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
