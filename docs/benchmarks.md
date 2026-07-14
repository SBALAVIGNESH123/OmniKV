# Reproducible benchmarks

OmniKV publishes a reproducible benchmark workflow so performance discussions
can refer to raw, structured evidence instead of screenshots or one-off claims.

## Quick smoke

```bash
cargo bench -p omnikv-engine --bench reproducible_bench -- --profile smoke --json-out target/omnikv-benchmark-smoke.json
```

The smoke profile is small enough for local validation and CI. It proves that
the harness builds, runs all workload categories, and emits structured JSON.

## Standard local run

```bash
cargo bench -p omnikv-engine --bench reproducible_bench -- --profile standard --json-out target/omnikv-benchmark-standard.json
```

Use the standard profile before publishing performance numbers. Always record:

- OmniKV version and Git commit SHA;
- OS, kernel, architecture, and filesystem;
- CPU model and core count;
- memory size;
- disk model or cloud volume type;
- Rust version;
- build flags;
- thermal/power mode for laptops;
- whether the run used an empty machine or shared workstation.

## Workloads

| Workload | Mode | What it measures |
| --- | --- | --- |
| `write-heavy-128b` | single-node | one 128-byte key/value per committed batch |
| `read-heavy-random-hit` | single-node | random point reads over a preloaded keyspace |
| `mixed-80r-20w` | single-node | mixed workload with 80% reads and 20% writes |
| `range-scan-full-prefix` | single-node | repeated range scans over a preloaded prefix |
| `compaction-l0-to-l1` | single-node | memtable flushes plus L0-to-L1 compaction cost |
| `transaction-commit` | single-node | SSI transaction set+commit overhead |
| `raft-replicated-simulated` | replicated-simulated | local Raft log append, two-follower fan-out, apply, and mark-applied |

`replicated-simulated` is intentionally not called a full cluster benchmark. It
does not include network transport, real leader election, separate processes,
or partition behavior. Use it as a repeatable lower-level Raft storage signal.

## JSON schema

The benchmark emits:

```json
{
  "schema_version": 1,
  "generated_at_unix_seconds": 0,
  "metadata": {
    "package_version": "0.3.0",
    "profile": "smoke",
    "os": "linux",
    "arch": "x86_64",
    "rustc_version": "rustc ...",
    "git_commit": "...",
    "notes": []
  },
  "workload_scale": {},
  "results": [
    {
      "name": "write-heavy-128b",
      "mode": "single-node",
      "operations": 300,
      "successful_operations": 300,
      "errors": 0,
      "elapsed_ms": 0,
      "throughput_ops_per_sec": 0.0,
      "latency_us": { "p50": 0.0, "p95": 0.0, "p99": 0.0 },
      "resources": {
        "rss_bytes_start": null,
        "rss_bytes_end": null,
        "cpu_user_ms_delta": null,
        "cpu_system_ms_delta": null,
        "data_dir_bytes_start": 0,
        "data_dir_bytes_end": 0,
        "disk_growth_bytes": 0,
        "wal_bytes_end": 0
      },
      "compaction": null,
      "notes": []
    }
  ]
}
```

Linux runs populate process RSS and CPU deltas from `/proc`. Other operating
systems may emit `null` for those fields; use OS-native profilers for formal
publication.

## Regression process

Use benchmark results as trend evidence, not single-run truth.

Recommended process:

1. Run the standard profile at least five times on the same quiet machine.
2. Compare medians for throughput and p99 latency.
3. Treat a sustained throughput drop greater than 10% as a warning.
4. Treat a sustained p99 latency increase greater than 20% as a warning.
5. Treat any new benchmark errors as a blocking regression.
6. Attach raw JSON files to the PR, release, or issue.
7. If a regression is expected, document the tradeoff in the PR.

CI runs a non-blocking benchmark smoke so benchmark code stays buildable without
turning noisy hosted-runner variance into failed pull requests.

## Checked-in reports

- [2026-07-14 smoke report](benchmark-reports/2026-07-14-smoke.md)

Published benchmark numbers should live in release notes or `docs/benchmark-reports/`
with the raw JSON attached or copied alongside the summary.
