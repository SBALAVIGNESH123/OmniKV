# OmniKV Storage Format Reference

## Manifest (`manifest.json`)

**Version:** `MANIFEST_FORMAT_VERSION = 1`

### Schema (v1)

```json
{
  "heap_path":      "<string> — path to heap/value log",
  "base_path":      "<string> — directory containing SSTables",
  "sstables":       ["<string>"] — L0 SSTable names",
  "l1_sstables":    ["<string>"] — L1 SSTable names",
  "max_seq":        <u64> — highest committed sequence number",
  "format_version": <u32> — missing field treated as v1"
}
```

## Database Directory Lock (`LOCK`)

Every OmniKV database directory contains a `LOCK` file. It is not part of the
logical data model; it exists to protect the physical files in the directory.

On `OmniKV::open()`, the engine:

1. resolves the parent directory of the manifest path,
2. creates or opens `<database-directory>/LOCK`,
3. acquires a non-blocking exclusive OS lock,
4. only then starts manifest recovery, WAL replay, heap opening, and SSTable
   mmap creation.

If another live engine already owns the lock, open fails with
`OmniError::DatabaseAlreadyOpen`. The lock is held for the full lifetime of the
`OmniKV` handle and is released only after mmap-bearing storage roots and file
handles are dropped.

### Version compatibility

| `format_version` | Behaviour |
|---|---|
| Missing | Treated as v1 (backward compat) |
| 1 | Loads normally |
| > 1 | Hard failure: `OmniError::UnsupportedVersion` |

---

## WAL (`wal.bin`) — v2 layout

```
[record_count: u32 LE][record_0]...[record_n][batch_crc32: u32 LE]
```

CRC32 covers `record_count` through all encoded records. Corrupt batches
are skipped; all valid batches before corruption are replayed.

---

## Heap (`heap.bin`)

Append-only raw value blobs. Each value pointer in the memtable holds
`(offset, length, crc32, expiry)`. CRC32 mismatch signals corruption.

---

## SSTables (`*.sst`)

Sorted key-value pairs with Bloom filter footer. No explicit version byte
in v1 — future incompatible changes require a `SSTABLE_FORMAT_VERSION` magic prefix.

### mmap Safety Invariants

OmniKV maps SSTable/base files read-only. The format relies on these invariants:

- A database-directory `LOCK` prevents two OmniKV processes from opening and
  mutating the same database files concurrently.
- Mapped SSTable/base files are immutable. New content is written to new files,
  synced, mapped, and then installed through an atomic root swap.
- Compaction and garbage collection never truncate a file that might still have
  a live mapping. Old files are removed only after the root has moved away, and
  live readers keep `Arc<Mmap>` handles until they finish.
- Corrupt or truncated SSTable records fail closed during decode. Readers stop
  at the invalid record instead of panicking or reading past the mapped slice.
- Unix, Linux, macOS, and Windows all use the same high-level invariant: no
  mutable writer may operate on an actively mapped database file. Windows may
  reject deletion of a still-mapped file; cleanup treats that as a safe deferred
  deletion rather than a correctness failure.

### MVCC Compaction and Tombstone Retention

Compaction must preserve every version that can still be observed by an active
snapshot or by a lagging replica that has pinned a retention floor. For each
key, OmniKV keeps:

- the latest version overall,
- every version at or newer than the oldest retention floor, and
- the newest predecessor at or before the oldest retention floor.

That predecessor rule is important. If the oldest active snapshot is sequence
`7` and a key has versions at `1`, `5`, `8`, and `10`, snapshot `7` must still
see version `5`; keeping only versions `>= 7` would break snapshot isolation.

Replica catch-up uses the same retention rule. Replication code can call
`pin_replica_retention(replica_id, seq)` while a follower still needs history at
or after `seq`, then `release_replica_retention(replica_id)` after the follower
catches up or switches to snapshot install. The effective compaction floor is
the minimum of active local snapshots and replica retention pins.

Tombstones and expired-value markers are deletion markers. L0-to-L1 compaction
preserves them so lower-level values cannot reappear. L1-to-base compaction and
heap garbage collection may drop deletion markers only when no active snapshot
or replica retention floor requires retained history; while any retention floor
is active, deletion markers remain in the compacted table so latest reads do not
resurrect older values.

### Scan Iterator Ownership and Buffer Reuse

SSTable range iterators own a lightweight handle to their backing table data.
Production table reads keep an `Arc<Mmap>` handle; tests and byte-slice callers
copy into `Arc<[u8]>`. This keeps iterator state independent from temporary
reader objects and prevents references into mapped pages from escaping their
owning storage handle.

`scan_iter` still performs candidate collection and newest-version
deduplication before lazy heap reads, but the heap-read scratch buffer is now
borrowed from a small per-database pool. A range scan reuses one `Vec<u8>`
across all yielded values, then returns it to the pool when the iterator is
dropped. This removes repeated per-row heap-read buffer allocation from the
scan hot path while preserving the existing lazy I/O behavior.

The `scan_buffer_pool` benchmark can be used to compare future scan changes:

```bash
cargo bench -p omnikv-engine --bench scan_buffer_pool -- --rows 20000 --rounds 20
```

---

## Checksums

| File | Algorithm | Scope |
|---|---|---|
| WAL batch | CRC32 | `record_count` + all records |
| Heap payload | CRC32 | raw value bytes |
| Manifest | none | atomic rename provides safety |
