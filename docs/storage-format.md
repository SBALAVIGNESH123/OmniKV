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

---

## Checksums

| File | Algorithm | Scope |
|---|---|---|
| WAL batch | CRC32 | `record_count` + all records |
| Heap payload | CRC32 | raw value bytes |
| Manifest | none | atomic rename provides safety |
