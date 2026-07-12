# OmniKV Configuration

OmniKV loads configuration from an optional `omnikv.toml` file and then
applies environment-variable overrides. Two modes are supported:
**development** (default, permissive) and **production** (fail-closed).

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
export OMNIKV_TLS_CERT_PATH=/etc/omnikv/tls/cert.pem
export OMNIKV_TLS_KEY_PATH=/etc/omnikv/tls/key.pem
export OMNIKV_DATA_DIR=/var/lib/omnikv/data
export OMNIKV_BACKUP_DIR=/var/lib/omnikv/backups
cargo run -p omnikv-server
```

---

## Environment variables reference

| Variable | Default | Description |
|---|---|---|
| `OMNIKV_MODE` | `development` | `development` or `production` |
| `OMNIKV_HTTP_ADDR` | `127.0.0.1:7070` | HTTP/1.1 + HTTP/2 bind address |
| `OMNIKV_QUIC_ADDR` | `127.0.0.1:7443` | QUIC/HTTP3 bind address |
| `OMNIKV_PGWIRE_ADDR` | `127.0.0.1:5432` | PostgreSQL wire protocol address |
| `OMNIKV_TCP_ADDR` | `127.0.0.1:7071` | TCP command interface address |
| `OMNIKV_JWT_SECRET` | dev default | JWT signing secret (≥ 32 chars required in production) |
| `OMNIKV_TLS_CERT_PATH` | _(none)_ | Path to TLS certificate (PEM) |
| `OMNIKV_TLS_KEY_PATH` | _(none)_ | Path to TLS private key (PEM) |
| `OMNIKV_TLS_INSECURE_SKIP` | `false` | Skip TLS checks — prints a warning; not recommended |
| `OMNIKV_LOG_LEVEL` | `info` | `trace`, `debug`, `info`, `warn`, `error` |
| `OMNIKV_DATA_DIR` | `./data` | Data directory |
| `OMNIKV_WAL_PATH` | `./data/wal/wal.bin` | WAL file path |
| `OMNIKV_MANIFEST_PATH` | `./data/manifest.json` | Manifest file path |
| `OMNIKV_BACKUP_DIR` | `./data/backups` | Backup directory |
| `OMNIKV_LOG_DIR` | `./logs` | Log directory |
| `OMNIKV_MAX_OPEN_FILES` | `512` | Max open file descriptors |
| `OMNIKV_WRITE_BUFFER_MB` | `64` | Write buffer size (MB) |
| `OMNIKV_COMPACTION_WORKERS` | `2` | Compaction worker threads |
| `OMNIKV_RATE_LIMIT_PER_SEC` | `1000` | Sustained requests per second per user/IP across REST, PgWire, and QUIC |
| `OMNIKV_RATE_LIMIT_BURST` | `100` | Maximum burst tokens per user/IP |
| `OMNIKV_RATE_LIMIT_MAX_USERS` | `10000` | Maximum tracked rate-limit identities before oldest-bucket eviction |

---

## Production mode constraints

`OMNIKV_MODE=production` enforces the following at startup:

- **JWT secret** must not be the development default.
- **JWT secret** must be ≥ 32 characters.
- **TLS** must be configured via `OMNIKV_TLS_CERT_PATH` + `OMNIKV_TLS_KEY_PATH`,
  or `OMNIKV_TLS_INSECURE_SKIP=true` must be set explicitly
  (a warning is printed to stderr when this override is active).
- **Missing TLS files** cause a hard startup failure with a clear error message.
- **Rate-limit settings** must be positive so production cannot accidentally
  start with disabled or nonsensical throttling.

---

## Security notes

- Never commit `omnikv.toml` containing real secrets to version control.
- Rotate `OMNIKV_JWT_SECRET` on a regular schedule in production.
- Use a secrets manager (Vault, AWS Secrets Manager, K8s Secrets) rather than
  plain environment variables in production deployments.
- Tune rate limits for your workload and alert on
  `omnikv_rate_limit_rejections_total{protocol=...}`.
