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

---

## Checksums

| File | Algorithm | Scope |
|---|---|---|
| WAL batch | CRC32 | `record_count` + all records |
| Heap payload | CRC32 | raw value bytes |
| Manifest | none | atomic rename provides safety |
