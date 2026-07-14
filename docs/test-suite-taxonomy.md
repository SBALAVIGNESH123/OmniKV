# Test suite taxonomy

OmniKV treats tests as evidence, not decoration. This page explains which suites are release gates, which suites are demonstrations, and which suites are performance or manual evidence.

## Release-gate suites

These suites are expected to pass before a production-readiness PR is merged:

| Gate | Purpose | Command |
| --- | --- | --- |
| Format | Rust formatting consistency | `cargo fmt --all -- --check` |
| Clippy | Lints every workspace target | `cargo clippy --workspace --all-targets -- -D warnings` |
| Build | Compiles libraries, binaries, tests, benches, and examples | `cargo build --workspace --all-targets` |
| Storage correctness | Core storage API behavior | `cargo test -p omnikv-engine --test storage_tests -- --test-threads=1` |
| Storage invariants | Storage edge cases and format behavior | `cargo test -p omnikv-engine --test storage_correctness -- --test-threads=1` |
| Storage engine contracts | LSM, WAL, and engine behavior | `cargo test -p omnikv-engine --test storage_engine -- --test-threads=1` |
| Backup/restore | Backup, restore, and metadata validation | `cargo test -p omnikv-engine --test backup_restore -- --test-threads=1` |
| Transactions/concurrency | SSI, operations, and concurrent stress coverage | `cargo test -p omnikv-engine --test concurrent_stress -- --test-threads=1` |
| SQL and operations | SQL layer, SQL v3 features, and ops maturity | `cargo test -p omnikv-engine --test sql_layer -- --test-threads=1` |
| API contracts | REST, PgWire, and Rust client contract tests | `cargo test -p omnikv-server rest_contract -- --test-threads=1` |
| Raft consensus | Single-process Raft cluster behavior, including partition-style scenarios, membership changes, and snapshot install catch-up | `cargo test -p omnikv-engine --test raft_cluster -- --test-threads=1` |
| Security audit | Dependency vulnerability gate | `cargo audit --deny warnings` |
| Docker build | Container image buildability | `docker build --pull --tag omnikv:ci .` |

CI names these jobs by purpose so a green run is easier to interpret as evidence.

## Regression suites

Regression suites are small, deterministic tests created from previously suspicious or debug-only scenarios. They must assert behavior directly and should not rely on console output.

```bash
cargo test -p omnikv-engine --test compaction_regression -- --test-threads=1
cargo test -p omnikv-engine --test compaction_many_records_regression -- --test-threads=1
cargo test -p omnikv-engine --test reopen_regression -- --test-threads=1
```

These cover:

- manual compaction preserving a committed key;
- many-record compaction preserving edge and middle records;
- reopen after compaction preserving values and sequence progress.

## Demonstration suites

Demonstration suites are executable examples with assertions. They are useful for explaining behavior, but they are not a substitute for deeper model checking, fuzzing, or distributed failure testing.

```bash
cargo test -p omnikv-engine --test anomaly_demos -- --test-threads=1
```

The SSI anomaly demonstrations show write-skew prevention, lost-update prevention, snapshot behavior, and retry-based counter correctness.

## Performance smoke suites

Performance smoke tests are deterministic enough for CI, but their thresholds are intentionally conservative because GitHub-hosted runners vary. Treat them as regression tripwires, not formal benchmark claims.

```bash
cargo test -p omnikv-engine --test storage_perf -- --test-threads=1 --nocapture
cargo test -p omnikv-engine --test benchmarks --release -- --test-threads=1 --nocapture
cargo bench -p omnikv-engine --bench reproducible_bench -- --profile smoke --json-out target/omnikv-benchmark-smoke.json
```

Formal performance claims should come from the [reproducible benchmark workflow](benchmarks.md) and should record hardware, OS, commit SHA, dataset size, workload shape, and raw output.

## Manual and long-running evidence

These checks are valuable before a release announcement, but they are not normal pull-request gates:

- real-data replay against exported JSONL workloads;
- long soak tests and recovery loops;
- cargo bench runs with recorded hardware metadata;
- partition/failover testing for distributed behavior;
- fuzzing and property-based testing.

For the current distributed guarantee boundary, see
[Distributed correctness](distributed-correctness.md).

When closing a production-readiness issue, include the PR number, commit SHA, commands run, and a short explanation of what the evidence proves and what it does not prove.
