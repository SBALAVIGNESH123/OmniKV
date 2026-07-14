# Changelog

## OmniKV v0.4.0 - 2026-07-15

OmniKV v0.4.0 is an evidence and integration release. It keeps OmniKV in beta,
but gives SketchLog and other Rust applications a stable embedded facade instead
of requiring direct use of storage internals.

### Added

- Stable `omni_engine::embedded` API:
  - directory-based open config;
  - key-value namespaces for SketchLog and tenant isolation;
  - put, put-with-TTL, get, delete, atomic batch, range scan, prefix scan, and scan-all helpers;
  - RAII snapshot reads;
  - compaction and sync helpers;
  - plain and encrypted backup/restore helpers;
  - SQL execution wrapper;
  - lightweight embedded stats.
- Embedded API contract tests covering namespace isolation, reopen durability,
  backup/restore, encrypted backup/restore, and SQL execution.
- Documentation for SketchLog-oriented embedded usage in `docs/embedded-api.md`.

### Production-readiness evidence included since v0.3.0

- Full CI gates across build, formatting, clippy, security audit, storage,
  transactions, SQL/ops, API contracts, Raft, benchmarks, Docker, and fuzz/property smoke.
- Real engine crash-consistency and durability evidence.
- Backup/restore hardening with restore-time validation and path traversal rejection.
- Production config fail-closed behavior for secrets, TLS posture, and runtime settings.
- REST, PgWire, and QUIC authentication/rate-limit hardening.
- Stable API/client compatibility tests and sanitized public errors.
- Docker/Compose/Kubernetes release smoke path.
- Reproducible benchmark workflow and benchmark documentation.
- Raft/distributed correctness tests for partition-style scenarios and failover.
- Seeded fuzz/property testing for SQL, API JSON, WAL, backup restore, Raft log operations,
  and storage visibility.

### Compatibility

- Manifest format: unchanged (`1`).
- WAL format: unchanged.
- SSTable format: unchanged.
- Backup format: unchanged (`1`).
- Rust API: new embedded facade is additive. Existing low-level engine exports remain available.

### Known caveats

- OmniKV remains beta and is not yet recommended as the default storage engine
  for critical production data.
- Distributed behavior is tested with deterministic in-process scenarios, not Jepsen-grade
  external fault injection.
- Long-duration fuzzing, 24-hour soak, and repeated restore drills are still future release gates.
- SQL is a focused embedded-database subset, not full PostgreSQL compatibility.

## OmniKV v0.3.0 - 2026-06-04

- SQL engine overhaul.
- Query/parser/planner improvements.
- Earlier beta release before the July production-readiness hardening track.

## OmniKV v0.2.0 - 2026-06-04

- Group commit v2.
- Bug fixes.
- Zero-warning cleanup at that point in the project history.

## OmniKV v0.1.0 - 2026-06-04

- Initial durability evidence release.
