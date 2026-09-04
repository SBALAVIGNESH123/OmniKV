# Changelog

## Unreleased

### Fixed

- SQL keywords and transaction statements now behave identically in any case
  or spacing combination (issue #109). DBAPI drivers (psycopg2, pg8000) with
  autocommit off implicitly send lowercase `begin transaction` at session
  start, which previously failed with SQLSTATE `42601`; the PgWire command
  dispatcher now accepts the full PostgreSQL variant set — `BEGIN
  [WORK|TRANSACTION]`, `START TRANSACTION`, `COMMIT [WORK|TRANSACTION]`,
  `END [WORK|TRANSACTION]`, `ROLLBACK [WORK|TRANSACTION]`,
  `ABORT [WORK|TRANSACTION]`, with the optional `AND [NO] CHAIN` suffix
  on the termination commands (COMMIT, END, ROLLBACK, ABORT) only, plus `SET` and the `SELECT 1` health shortcut, in any
  case/spacing, via whitespace-normalized dispatch. `AND CHAIN` opens a new
  transaction immediately after the commit or rollback and errors with
  `25P01` when no transaction is open, matching PostgreSQL. The legacy KV
  query parser's `key` filter column is also case-insensitive now, matching
  the prepared-statement parser. Verified live end to end with pg8000 in
  non-autocommit mode: implicit `begin transaction`, lowercase DML, and
  mixed-case statements all work. (Extended-protocol `commit()` is a
  separate gap, tracked in #119.)

- PgWire connections now handle the PostgreSQL SSLRequest and GSSENCRequest
  negotiation packets that libpq-based clients (psql, JDBC, psycopg2, pg8000,
  node-postgres) send by default before the StartupMessage. The server
  previously misparsed the 8-byte SSLRequest as a StartupMessage and
  desynchronized the protocol, so every default-configured client failed at
  connection time with `sslmode=prefer` (issue #108). The listener now answers
  negotiation with a single-byte `'N'` and completes the handshake on the same
  plaintext connection, matching PostgreSQL's fallback behavior.
- Startup messages with unknown protocol codes are now rejected with SQLSTATE
  `08P01` instead of producing a framing-dependent failure, and cancel-request
  connections for unknown backend keys are drained and closed.
- The pre-auth negotiation window is bounded (8 packets); hostile clients can
  no longer spin the listener in an unbounded SSLRequest loop, the bound counts
  only negotiation packets so a client that sends eight of them can still
  complete the handshake, and the ninth negotiation packet is answered with
  SQLSTATE `08P01` before the connection is closed.
- PgWire cleartext authentication is no longer served on externally reachable
  binds in production mode: the server fails closed at startup with
  `PermissionDenied` unless the PgWire address is loopback or a private (RFC
  1918 / ULA) address, matching the trusted-network guidance. Development mode
  is unchanged.
- CI regressions fixed on the new Rust stable (1.98) toolchain: documented
  `#[expect]` suppressions for `unused_async_trait_impl` in the openraft
  storage/network adapters (async is required by the trait signatures) and
  `result_large_err` on the axum RBAC middleware signatures.
- `cargo audit` failures resolved: `h2` 0.4.14 → 0.4.19
  (RUSTSEC-2026-0258), `rust_decimal` 1.41 → 1.43 (drops the vulnerable
  `rkyv` 0.7.46 dependency, RUSTSEC-2026-0235), plus `chacha20` and `validit`
  yanked-version bumps.

### Added

- `PgWireServer::serve(listener)` accept-loop API and
  `PgWireServer::with_password(...)` constructor so protocol tests can drive
  the production accept path on an OS-assigned port without mutating the
  process environment.
- `PgWireSecurityPolicy` (`AllowCleartextAnywhere` / `RequirePrivateBind`) with
  `PgWireServer::with_security_policy(...)` and
  `PgWireServer::validate_security_policy()`, enforcing the production
  cleartext-auth bind policy described above.
- `pgwire_compat` release-gate test suite: real-socket libpq handshake
  conformance covering SSLRequest/GSSENCRequest refusal, plaintext fallback,
  dual-negotiation sequences, the eight-packet negotiation boundary
  (StartupMessage still accepted; ninth packet rejected with `08P01`),
  protocol-violation rejection, wrong-password rejection, the pre-auth
  negotiation loop bound, and the production cleartext bind policy. Registered
  in CI and in the test suite taxonomy as a release gate.
- Connection negotiation rules documented in `docs/protocol-limits.md`,
  including the production cleartext-auth bind policy and the
  trusted-network guidance for cleartext passwords until PgWire TLS support
  lands.

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
