# Fuzzing and property testing

OmniKV has two complementary fuzzing layers:

1. fast deterministic CI smoke tests that build the fuzz targets and replay
   checked-in corpus/regression inputs;
2. longer `cargo-fuzz` runs that execute manually or on the scheduled
   `OmniKV Fuzzing` workflow.

## Targets

| Target | Surface |
| --- | --- |
| `sql_parser` | SQL parser entry point |
| `api_json` | REST/cluster request JSON deserialization |
| `wal_record` | WAL record decoding and WAL replay |
| `backup_restore` | compressed backup restore parser |
| `raft_log` | bounded Raft log append/read/apply/delete state transitions |

## Fast local checks

```bash
cargo check --manifest-path fuzz/Cargo.toml --bins
cargo test -p omnikv-engine --test property_storage -- --test-threads=1
cargo test -p omnikv-engine --test fuzz_regressions -- --test-threads=1
cargo test -p omnikv-server --test fuzz_api_regressions -- --test-threads=1
```

These checks run in the main CI workflow.

## Longer fuzzing

Install `cargo-fuzz` and run one target:

```bash
cargo install cargo-fuzz --locked
cargo fuzz run sql_parser fuzz/corpus/sql_parser -- -max_total_time=300
```

Run all targets through GitHub Actions using the manual `OmniKV Fuzzing`
workflow. The same workflow also runs weekly on a schedule.

## Regression workflow

When a fuzz target finds a crash or hang:

```bash
cargo fuzz tmin <target> fuzz/artifacts/<target>/<artifact>
```

Then copy the minimized input into:

```text
fuzz/regressions/<target>/
```

If the issue is semantic rather than a panic, add a deterministic assertion in
`crates/omnikv-engine/tests/fuzz_regressions.rs` or
`crates/omnikv-server/tests/fuzz_api_regressions.rs`.

Checked-in regression files are part of the required CI smoke path and should
never be deleted without replacing them with stronger coverage.
