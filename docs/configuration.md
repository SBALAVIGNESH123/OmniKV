# OmniKV Configuration Guide

OmniKV loads configuration in priority order:

1. **Environment variables** (highest priority)
2. **Config file** (`OMNIKV_CONFIG` or `--config`)
3. **Compiled-in defaults** (local-dev safe only)

---

## Quick start (development)

```bash
# All defaults — runs on localhost:7070
cargo run --bin omnikv-server
```

## Quick start (production)

```bash
export OMNIKV_MODE=production
export OMNIKV_JWT_SECRET="$(openssl rand -hex 32)"
export OMNIKV_TLS_CERT=/etc/omnikv/tls/cert.pem
export OMNIKV_TLS_KEY=/etc/omnikv/tls/key.pem
export OMNIKV_DATA_DIR=/var/lib/omnikv/data
export OMNIKV_BACKUP_DIR=/var/lib/omnikv/backups
cargo run --bin omnikv-server
```

---

## Environment variables reference

| Variable | Default | Description |
|---|---|---|
| `OMNIKV_MODE` | `development` | `development` or `production` |
| `OMNIKV_HOST` | `127.0.0.1` | Bind address |
| `OMNIKV_PORT` | `7070` | Client port |
| `OMNIKV_ADMIN_PORT` | `7071` | Admin/metrics port |
| `OMNIKV_DATA_DIR` | `./data` | Data directory |
| `OMNIKV_WAL_DIR` | `./data/wal` | WAL directory |
| `OMNIKV_BACKUP_DIR` | `./data/backups` | Backup directory |
| `OMNIKV_LOG_DIR` | `./logs` | Log directory |
| `OMNIKV_JWT_SECRET` | dev default | JWT signing secret (≥32 chars in production) |
| `OMNIKV_TLS_CERT` | _(none)_ | Path to TLS certificate |
| `OMNIKV_TLS_KEY` | _(none)_ | Path to TLS private key |
| `OMNIKV_TLS_INSECURE_SKIP` | `false` | Skip TLS (not recommended in production) |
| `OMNIKV_LOG_LEVEL` | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `OMNIKV_MAX_OPEN_FILES` | `512` | Max open file descriptors |
| `OMNIKV_WRITE_BUFFER_MB` | `64` | Write buffer size (MB) |
| `OMNIKV_COMPACTION_WORKERS` | `2` | Compaction worker threads |

---

## Production mode rules

Production mode (`OMNIKV_MODE=production`) enforces:

- **JWT secret** must be set explicitly and be ≥ 32 characters. The default dev secret is rejected.
- **TLS** must be configured via `OMNIKV_TLS_CERT` + `OMNIKV_TLS_KEY`, or `OMNIKV_TLS_INSECURE_SKIP=true` must be set explicitly (with a printed warning).
- **Missing TLS files** cause a hard startup failure with a clear error message.

---

## Development mode

Development mode (`OMNIKV_MODE=development`, the default) is permissive:
- Default JWT secret is accepted.
- TLS is optional.
- All paths default to local `./data` and `./logs`.

> **Never run development mode in production.**

---

## Security notes

- Store `OMNIKV_JWT_SECRET` in a secrets manager (Vault, AWS Secrets Manager, Kubernetes Secret).
- Rotate TLS certificates regularly.
- Use `OMNIKV_HOST=0.0.0.0` only when behind a load balancer or firewall.
- The admin port (`OMNIKV_ADMIN_PORT`) should not be exposed publicly.
