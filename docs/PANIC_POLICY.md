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

## Adding a New Panic

Before adding a new `unwrap()` or `expect()`:

1. Identify the category above.
2. If **recoverable** — propagate the error with `?`.
3. If **fatal invariant** — use `.expect("clear reason why this cannot be recovered")`.
4. If **startup-only** — use `.expect("startup: <what failed and why it is fatal>")`.
5. Update this document if a new category is needed.
