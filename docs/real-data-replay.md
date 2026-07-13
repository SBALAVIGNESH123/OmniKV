# Real-data replay harness

The real-data replay harness turns copied production-like data into repeatable
OmniKV evidence. It imports JSONL records, verifies exact reads, reopens the
database, optionally runs compaction and heap garbage collection, then verifies
again.

Use this against exported copies or shadow data. Do not use the only copy of
critical production data as input.

## Quick start

Create a JSONL file:

```jsonl
{"id":"evt-1","tenant":"acme","service":"api","latency_ms":42}
{"id":"evt-2","tenant":"acme","service":"worker","latency_ms":330}
{"id":"evt-3","tenant":"globex","service":"api","latency_ms":17}
```

Run the harness:

```bash
cargo run -p omnikv-engine --bin real_data_replay -- \
  --input events.jsonl \
  --workdir target/omnikv-real-data-replay \
  --reset \
  --key-field id \
  --key-prefix event: \
  --report target/omnikv-real-data-replay-report.json
```

The command prints a JSON report:

```json
{
  "status": "passed",
  "rows_imported": 3,
  "verify_after_import": { "mismatches": 0 },
  "verify_after_reopen": { "mismatches": 0 },
  "verify_after_compaction": { "mismatches": 0 }
}
```

## What it proves

For every non-empty input line, the harness:

1. validates that the line is valid JSON,
2. derives a deterministic key,
3. writes the exact raw JSON line as the OmniKV value,
4. verifies exact read-back after import,
5. drops and reopens the database, then verifies again,
6. runs memtable flush, L0-to-L1 compaction, L1-to-base compaction, and heap
   garbage collection, then verifies again.

The report includes row counts, checksums, elapsed timings, mismatch counts, and
up to ten mismatch samples.

## Key selection

By default, keys are generated from line numbers:

```text
real:00000000000000000001
real:00000000000000000002
```

For real event exports, prefer a stable top-level JSON id:

```bash
--key-field id --key-prefix event:
```

That stores records under keys such as:

```text
event:evt-1
event:evt-2
```

## Workdir safety

The harness refuses to reuse a non-empty workdir by default. `--reset` only
deletes a directory that is empty or already marked by this harness with
`.omnikv-real-data-replay`.

This prevents accidental deletion of unrelated data while still allowing repeat
runs in `target/omnikv-real-data-replay`.

## Recommended evidence ladder

Start small and grow:

1. 1,000 copied records.
2. 100,000 copied records.
3. Full exported non-critical dataset.
4. Shadow writes from a real application while reads still use the existing
   database.
5. Canary reads for one non-critical tenant or workload.

For each run, keep the report file as evidence and compare row counts,
checksums, and mismatch samples over time.
