# Contributing to OmniKV

Thank you for your interest in contributing to OmniKV.

## Getting started

```bash
git clone https://github.com/SBALAVIGNESH123/OmniKV.git
cd OmniKV
cargo build --workspace
cargo test -p omnikv-engine --test storage_tests -- --test-threads=1
```

## Running the full test suite

```bash
# Storage
cargo test -p omnikv-engine --test storage_tests -- --test-threads=1
cargo test -p omnikv-engine --test storage_correctness -- --test-threads=1
cargo test -p omnikv-engine --test storage_engine -- --test-threads=1
cargo test -p omnikv-engine --test storage_perf -- --test-threads=1

# Transactions and concurrency
cargo test -p omnikv-engine --test anomaly_demos -- --test-threads=1
cargo test -p omnikv-engine --test concurrent_stress -- --test-threads=1

# SQL
cargo test -p omnikv-engine --test sql_layer -- --test-threads=1

# Raft consensus
cargo test -p omnikv-engine --test raft_cluster -- --test-threads=1

# Operational
cargo test -p omnikv-engine --test ops_maturity -- --test-threads=1
```

## Architecture

- `crates/omnikv-engine/src/lib.rs` — thin engine crate entry point and public re-exports.
- `crates/omnikv-engine/src/storage/` — LSM-tree storage, WAL, backup, crypto, and transactions.
- `crates/omnikv-engine/src/query/` — SQL parser, optimizer, executor, PgWire, and iterator engine.
- `crates/omnikv-engine/src/raft/` — Raft consensus types, network, and storage.
- `crates/omnikv-engine/src/runtime/` — configuration, diagnostics, hardening, metrics, and runtime helpers.
- `crates/omnikv-server/src/` — server executable, REST API, auth, QUIC, TCP, and startup wiring.
- `omni-client/` — Rust client package.

## Code style

- Run `cargo fmt --all` before committing.
- Run `cargo clippy --workspace --all-targets -- -D warnings` and fix warnings.
- All new features must have tests.

## Pull request process

1. Fork the repository.
2. Create a feature branch.
3. Write tests for your changes.
4. Ensure relevant checks pass.
5. Submit a pull request.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
