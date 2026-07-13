# API and client compatibility

This page defines the public contracts OmniKV intends to keep stable while the
project is in beta. The goal is practical compatibility: application code should
not break silently when REST responses, PgWire behavior, or the Rust client are
changed.

OmniKV is still beta software. Breaking changes can happen, but they must be
intentional, documented, and covered by migration notes.

## Compatibility matrix

| Surface | Status | Compatibility contract | CI coverage |
| --- | --- | --- | --- |
| Embedded Rust API (`omni_engine`) | Beta | Public exported types and core storage operations should change only with release notes. | Workspace build, clippy, storage, durability, SQL, transaction, and ops tests. |
| REST API | Beta | JSON envelopes use `success`, `data`, and `error`. Stable error strings are used for auth, scan limits, and sanitized storage failures. | `cargo test -p omnikv-server rest_contract -- --test-threads=1` |
| PgWire protocol | Beta subset | Startup, query result frames, command-complete tags, ReadyForQuery state bytes, SQLSTATE error fields, and result-size limits are tested. | `cargo test -p omnikv-engine pgwire_contract --lib -- --test-threads=1` |
| Rust REST client (`omni-client`) | Beta | The client must continue to deserialize stable REST envelopes, map missing keys to `Ok(None)`, and preserve API error codes. | `cargo test -p omni-client client_contract -- --test-threads=1` |
| Python / Go clients | Not official yet | No compatibility promise until packages are added to this repository and CI. | Not applicable. |

## REST response envelope

REST JSON responses use the same top-level shape for success and error cases:

```json
{
  "success": true,
  "data": {},
  "error": null
}
```

Error responses keep `success: false`, `data: null`, and a client-readable
`error` string:

```json
{
  "success": false,
  "data": null,
  "error": "missing bearer token"
}
```

Internal storage and I/O details must not be exposed directly through the REST
API. Storage failures are mapped to stable client-facing codes such as
`NOT_FOUND`, `BATCH_TOO_LARGE`, `VALUE_TOO_LARGE`, `UNSUPPORTED_VERSION`, or
`STORAGE_ERROR`.

## PgWire compatibility

OmniKV implements a focused PostgreSQL wire-protocol subset. The current
compatibility contract covers:

- cleartext password authentication controlled by `OMNI_PGWIRE_PASSWORD`;
- `ReadyForQuery` status bytes `I`, `T`, and `E`;
- SQLSTATE-bearing error frames;
- command-complete tags such as `SELECT n`, `INSERT 0 1`, `UPDATE n`, and
  `DELETE n`;
- bounded result windows for both legacy query parsing and SQL v3 parsing.

Full PostgreSQL dialect compatibility is not a current goal. See
[SQL support matrix](sql-support.md) for the supported SQL subset.

## Breaking-change policy

Any change that alters one of these public contracts must include:

1. a versioned migration note in the pull request or release notes;
2. updated documentation on this page or the linked protocol page;
3. updated contract tests or explicitly removed contract tests with rationale;
4. a clear client impact statement.

Examples of breaking changes include renaming REST response fields, changing
stable error codes/messages, changing PgWire SQLSTATEs for common errors,
removing a Rust client method, or changing the behavior of missing-key reads.

## Adding a new official client

A new official client is not considered supported until it has:

- source code in this repository;
- a documented compatibility status in the matrix above;
- smoke tests in CI;
- examples covering auth, get, set, scan, and error handling.
