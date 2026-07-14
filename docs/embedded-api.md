# Embedded API for SketchLog integration

OmniKV exposes a stable embedded Rust facade through `omni_engine::embedded`.
This is the recommended integration path for applications such as SketchLog.
It avoids coupling callers to manifest paths, WAL paths, SSTable internals, or
low-level storage batches.

Status: beta. The embedded facade is intended for durable local state,
telemetry replay buffers, edge collectors, demos, and non-critical prototypes.
It is not yet a recommendation to store critical production data without an
operator-managed backup, restore, and soak-test process.

## Open a namespaced store

```rust
use omni_engine::{EmbeddedConfig, EmbeddedOmniKv};

let store = EmbeddedOmniKv::open(
    EmbeddedConfig::new("./data/omnikv").namespace("sketchlog"),
)?;
```

The namespace is applied to key-value operations. This lets SketchLog separate
product state, tenant state, stream state, and local replay buffers inside one
database without exposing the raw prefix to callers.

```rust
let tenant_a = store.scoped("tenant_a")?;
let tenant_b = store.scoped("tenant_b")?;

tenant_a.put("streams/api/latest-p95", "42.5")?;
tenant_b.put("streams/api/latest-p95", "97.2")?;
```

## Store telemetry and sketch state

```rust
store.put(
    "telemetry/api/00000000000000000001",
    r#"{"latency_ms":42,"status":200}"#,
)?;

store.put("sketches/api/p99", "91.4")?;

let latest = store.get("sketches/api/p99")?;
```

For multiple updates, use an atomic batch:

```rust
use omni_engine::EmbeddedBatch;

store.write_batch(
    EmbeddedBatch::new()
        .put("streams/api/cardinality", "128")
        .put("streams/api/p95", "42.5")
        .put("streams/api/p99", "91.4"),
)?;
```

## Scan replay buffers

SketchLog can scan a stream prefix after restart to rebuild bounded-memory
sketches or replay recent buffered telemetry.

```rust
let rows = store.scan_prefix("telemetry/api/", Some(1_000))?;
for row in rows {
    println!("{} {}", row.key, row.value);
}
```

## Snapshot reads

Snapshots provide repeatable reads across a sequence. The embedded snapshot is
RAII-based; dropping it unregisters the snapshot.

```rust
let snapshot = store.snapshot();
let before = store.get_at("sketches/api/p99", &snapshot)?;
store.put("sketches/api/p99", "95.0")?;
let still_before = store.get_at("sketches/api/p99", &snapshot)?;
drop(snapshot);
```

## Backup and restore

Backups include the WAL so restored directories can be opened directly.

```rust
let backup = store.create_backup("./backups/sketchlog.tar.gz")?;
drop(store);

let restored = EmbeddedOmniKv::restore_from_backup(
    backup,
    "./restore/omnikv",
)?
.scoped("sketchlog")?;
```

Encrypted backups are also available:

```rust
store.create_encrypted_backup("./backups/sketchlog.enc", passphrase)?;

let restored = EmbeddedOmniKv::restore_from_encrypted_backup(
    "./backups/sketchlog.enc",
    "./restore/omnikv",
    passphrase,
)?
.scoped("sketchlog")?;
```

## SQL execution

`execute_sql` is available for engine-global SQL tables:

```rust
store.execute_sql("CREATE TABLE metrics (id INT PRIMARY KEY, name TEXT)")?;
store.execute_sql("INSERT INTO metrics (id, name) VALUES (1, 'api')")?;
let rows = store.execute_sql("SELECT name FROM metrics WHERE id = 1")?;
```

Key-value namespaces do not rewrite SQL table/catalog storage. Use SQL for
engine-global analytical tables. Use `put`, `write_batch`, and `scan_prefix`
for namespaced SketchLog event and sketch-state payloads.

## Operational stats

```rust
let stats = store.stats();
println!(
    "seq={} memtable={} l0={} l1={}",
    stats.sequence,
    stats.memtable_size,
    stats.l0_sstables,
    stats.l1_sstables
);
```

These stats are intended for local health checks, dashboards, and release smoke
tests. Formal performance claims should still use the reproducible benchmark
workflow.

## Contract tests

The embedded API is guarded by:

```bash
cargo test -p omnikv-engine --test embedded_api -- --test-threads=1
```

The test suite covers namespaced writes, scoped tenant isolation, reopen
durability, plain backup/restore, encrypted backup/restore, and SQL execution.
