# OmniKV Panic Policy

## Overview

This document classifies all `unwrap()` / `expect()` / `panic!()` uses in OmniKV
production code and establishes rules for future contributors.

## Allowed Panic Categories

### 1. Fatal Invariant — Lock Poison

**Pattern:** `lock().expect("... lock poisoned: fatal invariant")`

A poisoned `Mutex` or `RwLock` means a thread panicked while holding the lock,
leaving shared state in an undefined condition.  Continuing is **not safe** —
a hard crash is the correct response.

**Examples:**
- `self.meta.lock().expect("RaftStorage meta lock poisoned: fatal invariant")`
- `self.cache.write().expect("catalog cache RwLock poisoned: fatal invariant")`

### 2. Startup-Only Registration

**Pattern:** `register_*(...).expect("metric registration failed at startup: ...")`

Prometheus metric registration fails only if the same metric name is registered
twice or the name is invalid.  Both are programming errors that must be caught at
startup, not silently ignored.

**Examples:**
- `register_int_counter!(...).expect("OmniKV metric registration failed at startup: ...")`

### 3. Infallible After Guard

**Pattern:** `// SAFETY: <reason>` comment before `.unwrap()`

When the code above guarantees the value is `Some` or the conversion cannot fail,
a guarded `.unwrap()` is acceptable.  A `// SAFETY:` comment is required.

**Examples:**
- `.duration_since(UNIX_EPOCH).unwrap_or_default()` — clock is always ≥ epoch

### 4. Unsafe / mmap Safety Comments

Every `unsafe` block in production storage code must carry a nearby
`// SAFETY:` comment that states the invariant making it sound. For mmap usage,
the comment must explain why the mapped file cannot be concurrently truncated or
mutated while the mapping is alive.

Current storage mmap invariants:

- `OmniKV::open()` acquires an exclusive database-directory `LOCK` file before
  manifest recovery, WAL replay, heap opening, or SSTable mmap creation.
- A second live `OmniKV` instance for the same database directory returns
  `OmniError::DatabaseAlreadyOpen`.
- SSTable/base files are immutable after they are mapped. Compaction writes a new
  file, fsyncs it, creates a read-only mmap, and publishes it with an atomic root
  swap.
- Old mmap handles are reference-counted through `Arc<Mmap>`. Cleanup may remove
  old pathnames later, but reader-owned mappings stay alive until the last reader
  drops them.
- The database lock is stored as the final `OmniKV` field so mapped files and
  file handles are dropped before the lock file is unlocked and closed.

## Not Allowed

| Pattern | Required replacement |
|---|---|
| `serde_json::to_string(...).unwrap()` in a `Result`-returning fn | `.map_err(\|e\| StorageError::write(...))? ` |
| `batch.set(...).unwrap()` in a `Result`-returning fn | `.map_err(...)? ` |
| `option.unwrap()` without a preceding guard | `.ok_or_else(...)? ` |
| `expect(...)` with no reason string | Add a clear invariant explanation |

## CI Enforcement

The `tests/panic_policy_audit.rs` test scans production source files and fails
the build if bare `.unwrap()` appears outside of approved locations (tests,
benchmarks, build scripts, `// SAFETY:` guarded sites).

The workspace also enables the `clippy::all`, `clippy::pedantic`, and
`clippy::nursery` lint groups through Cargo lint configuration. CI enforces the
policy with:

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

New lint suppressions must use `#[expect(..., reason = "...")]` instead of
`#[allow(...)]`. `expect` is intentional: CI fails when the lint no longer
triggers, which prevents stale suppressions from silently accumulating.

Allowed clippy expectations must be narrow and justified:

- Prefer fixing production-code lints directly when the change is low-risk.
- Use crate-level expectations only for documented legacy debt that spans many
  existing call sites.
- Use item-level expectations for local invariants such as parser enum sizing,
  explicit SQL clause argument lists, or test-only diagnostic formatting.
- Do not add `#[allow(...)]` in Rust source without a follow-up issue and a
  specific reason this cannot be expressed as `#[expect(...)]`.

## Adding a New Panic

Before adding a new `unwrap()` or `expect()`:

1. Identify the category above.
2. If **recoverable** — propagate the error with `?`.
3. If **fatal invariant** — use `.expect("clear reason why this cannot be recovered")`.
4. If **startup-only** — use `.expect("startup: <what failed and why it is fatal>")`.
5. Update this document if a new category is needed.

## Adding a New mmap or Unsafe Storage Path

Before adding a new mmap or unsafe storage path:

1. Acquire or prove the relevant database-directory lock is already held.
2. Ensure the mapped file is immutable for the full lifetime of the mapping.
3. Do not write through mmap. OmniKV writes with normal file I/O, fsyncs, then
   maps read-only.
4. Keep old mapped files alive through `Arc<Mmap>` until readers finish.
5. Add a regression test for truncation/corruption behavior and any root-swap or
   compaction lifetime behavior the change touches.
