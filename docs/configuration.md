# OmniKV Configuration

OmniKV loads one authoritative server runtime configuration. The precedence is:

1. Built-in defaults
2. Config file
3. Environment-variable overrides

The server accepts `--config <path>` or `--config=<path>` to select a config
file. Without CLI selection, `OMNIKV_CONFIG` is used first, then legacy
`OMNI_CONFIG`, then `./omnikv.toml` if it exists. Explicit config paths fail
startup if they cannot be read or parsed.

Two modes are supported: **development** (default, permissive) and
**production** (fail-closed).

---

## Quick start — development

```bash
cargo run -p omnikv-server
# Uses all defaults: 127.0.0.1:7070, dev JWT secret, no TLS required
```

---

## Quick start — production

```bash
export OMNIKV_MODE=production
export OMNIKV_JWT_SECRET="$(openssl rand -hex 32)"
export OMNIKV_BOOTSTRAP_ADMIN_KEY="$(openssl rand -hex 32)"
export OMNIKV_TLS_CERT_PATH=/etc/omnikv/tls/cert.pem
export OMNIKV_TLS_KEY_PATH=/etc/omnikv/tls/key.pem
export OMNIKV_DATA_DIR=/var/lib/omnikv/data
export OMNIKV_BACKUP_DIR=/var/lib/omnikv/backups
cargo run -p omnikv-server -- --config /etc/omnikv/omnikv.toml
```

---

## Environment variables reference

| Variable | Default | Description |
|---|---|---|
| `OMNIKV_CONFIG` | _(none)_ | Path to the server TOML config file |
| `OMNI_CONFIG` | _(none)_ | Legacy alias for `OMNIKV_CONFIG` |
| `OMNIKV_MODE` | `development` | `development` or `production` |
| `OMNIKV_HTTP_ADDR` | `127.0.0.1:7070` | HTTP/1.1 + HTTP/2 bind address |
| `OMNIKV_QUIC_ADDR` | `127.0.0.1:7071` | QUIC/HTTP3 bind address |
| `OMNIKV_PGWIRE_ADDR` | `127.0.0.1:5432` | PostgreSQL wire protocol address |
| `OMNIKV_TCP_ADDR` | `127.0.0.1:7072` | TCP command interface address |
| `OMNIKV_JWT_SECRET` | dev default | JWT signing secret (≥ 32 chars required in production) |
| `OMNIKV_BOOTSTRAP_ADMIN_KEY` | dev default | Bootstrap key for `POST /auth/token` (≥ 32 chars required in production) |
| `OMNIKV_TLS_CERT_PATH` | _(none)_ | Path to TLS certificate (PEM) |
| `OMNIKV_TLS_KEY_PATH` | _(none)_ | Path to TLS private key (PEM) |
| `OMNIKV_TLS_INSECURE_SKIP` | `false` | Skip TLS checks — prints a warning; not recommended |
| `OMNIKV_LOG_LEVEL` | `info` | `trace`, `debug`, `info`, `warn`, `error` |
| `OMNIKV_DATA_DIR` | _(none)_ | Convenience base directory; derives manifest, WAL, and backup paths unless those are overridden |
| `OMNIKV_WAL_PATH` | `wal.bin` | WAL file path |
| `OMNIKV_MANIFEST_PATH` | `manifest.json` | Manifest file path |
| `OMNIKV_BACKUP_DIR` | `./data/backups` | Backup directory |
| `OMNIKV_MAX_OPEN_FILES` | `512` | Max open file descriptors |
| `OMNIKV_WRITE_BUFFER_MB` | `64` | Write buffer size (MB) |
| `OMNIKV_COMPACTION_WORKERS` | `2` | Compaction worker threads |
| `OMNIKV_MEMTABLE_FLUSH_THRESHOLD` | `10000` | Memtable entry count that triggers a background flush to L0 |
| `OMNIKV_L0_COMPACTION_TRIGGER` | `4` | L0 SSTable count that triggers L0-to-L1 compaction |
| `OMNIKV_L1_COMPACTION_TRIGGER` | `4` | L1 SSTable count that triggers L1-to-base compaction |
| `OMNIKV_L0_WRITE_STALL_THRESHOLD` | `12` | L0 SSTable count that stalls/rejects writes until compaction catches up |
| `OMNIKV_WRITE_STALL_WAIT_ATTEMPTS` | `50` | Number of wait attempts before a stalled write returns `WriteStall` |
| `OMNIKV_WRITE_STALL_WAIT_MS` | `100` | Milliseconds to wait between write-stall attempts |
| `OMNIKV_COMPACTION_CHECK_INTERVAL_MS` | `500` | Background compaction worker polling interval |
| `OMNIKV_RATE_LIMIT_PER_SEC` | `1000` | Sustained requests per second per user/IP across REST, PgWire, and QUIC |
| `OMNIKV_RATE_LIMIT_BURST` | `100` | Maximum burst tokens per user/IP |
| `OMNIKV_RATE_LIMIT_MAX_USERS` | `10000` | Maximum tracked rate-limit identities before oldest-bucket eviction |

---

## Production mode constraints

`OMNIKV_MODE=production` enforces the following at startup:

- **Config files** are parsed strictly. Invalid TOML or unknown keys fail
  startup instead of being ignored.
- **Environment overrides** are parsed strictly. Invalid numeric or boolean
  values fail startup instead of falling back to defaults.
- **JWT secret** must not be the development default.
- **JWT secret** must be ≥ 32 characters.
- **Bootstrap admin key** must not be the development default.
- **Bootstrap admin key** must be ≥ 32 characters and different from the JWT
  secret.
- **TLS** must be configured via `OMNIKV_TLS_CERT_PATH` + `OMNIKV_TLS_KEY_PATH`,
  or `OMNIKV_TLS_INSECURE_SKIP=true` must be set explicitly
  (a warning is printed to stderr when this override is active).
- **Missing TLS files** cause a hard startup failure with a clear error message.
- **Rate-limit settings** must be positive so production cannot accidentally
  start with disabled or nonsensical throttling.
- **Compaction policy** must be internally consistent: L0/L1 compaction
  triggers and the compaction check interval must be positive, and
  `l0_write_stall_threshold` must be greater than `l0_compaction_trigger`.

---

## Compaction and write-stall observability

The server starts a background compaction worker at startup. The worker flushes
the memtable to L0, compacts L0 into L1, compacts L1 into the immutable base
table, and periodically runs heap garbage collection.

Prometheus metrics expose maintenance health:

- `omnikv_compaction_latency_seconds{stage=...}`
- `omnikv_compaction_bytes_rewritten_total{stage=...}`
- `omnikv_compaction_tombstones_total{stage=...}`
- `omnikv_compaction_expired_records_dropped_total{stage=...}`
- `omnikv_compaction_backlog_sstables`
- `omnikv_replica_retention_floor` (`0` means no replica retention pin is active)
- `omnikv_write_stalls_total`

---

## Security notes

- Never commit `omnikv.toml` containing real secrets to version control.
- Rotate `OMNIKV_JWT_SECRET` and `OMNIKV_BOOTSTRAP_ADMIN_KEY` on a regular
  schedule in production.
- Use a secrets manager (Vault, AWS Secrets Manager, K8s Secrets) rather than
  plain environment variables in production deployments.
- Use scoped, short-lived REST tokens for read, write, backup, and admin
  automation. See [Security model](security.md).
- Tune rate limits for your workload and alert on
  `omnikv_rate_limit_rejections_total{protocol=...}`.
- Alert on `omnikv_cleanup_delete_failures_total{context=...,error_kind=...}`;
  non-zero values usually mean obsolete compaction or GC files could not be
  deleted due to permissions, file locks, or storage problems.
- Alert on sustained `omnikv_compaction_backlog_sstables` growth or any
  increase in `omnikv_write_stalls_total`; both indicate compaction is not
  keeping up with writes.
