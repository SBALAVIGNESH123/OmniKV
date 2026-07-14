# OmniKV security model

OmniKV is still beta software, but exposed server surfaces should fail closed
and make security boundaries explicit. This document describes the current REST
security posture and the production settings expected before any public or
shared deployment.

## Auth modes

| Mode | Intended use | Behavior |
| --- | --- | --- |
| Development | Local experiments and demos | Uses development defaults unless overridden. TLS can be skipped. Do not expose this mode to untrusted networks. |
| Production | Production-style evaluation | Requires non-default secrets, positive rate limits, and TLS certificate/key paths unless `OMNIKV_TLS_INSECURE_SKIP=true` is set deliberately for a controlled test environment. |

Production startup validates:

- `OMNIKV_JWT_SECRET` is set, is not the development default, and is at least
  32 characters.
- `OMNIKV_BOOTSTRAP_ADMIN_KEY` is set, is not the development default, is at
  least 32 characters, and is different from the JWT secret.
- TLS certificate and key paths exist, unless the local-only insecure override
  is explicitly enabled.
- Rate-limit settings are positive.

## Token issuance and rotation

REST clients authenticate with JWT bearer tokens. Tokens are minted through
`POST /auth/token` using the `x-omni-admin-key` bootstrap header.

The bootstrap key is only for initial token issuance and emergency rotation. It
must be stored in a secret manager and rotated whenever a token leak is
suspected.

Token TTL behavior:

- default TTL: 3600 seconds
- maximum TTL: 86400 seconds
- `ttl_seconds=0` or values above 86400 are rejected

Recommended rotation:

1. Generate a new `OMNIKV_JWT_SECRET`.
2. Restart the server with the new secret during a maintenance window.
3. Re-mint service tokens with short role-specific TTLs.
4. Rotate `OMNIKV_BOOTSTRAP_ADMIN_KEY` after emergency access or team changes.

## Roles and REST permissions

Tokens contain a `role` claim. Current valid roles are:

| Role | Current REST permissions | Notes |
| --- | --- | --- |
| `read` | `GET /kv/:key`, `GET /scan` | Read-only data-plane access. |
| `write` | Read routes plus `POST /kv`, `DELETE /kv/:key`, `POST /batch` | Data-plane mutation. Does not allow backup or admin operations. |
| `backup` | `POST /admin/backup` | Backup-only operational access. Does not allow compaction or metrics. |
| `restore` | Reserved for future restore endpoints | Restore endpoints must require `restore` or `admin` and emit restore audit events before release. |
| `cluster` | Reserved for future cluster-management endpoints | Cluster endpoints must require `cluster` or `admin` and emit cluster audit events before release. |
| `admin` | All REST roles plus admin endpoints | Full administrative access. Keep TTLs short. |

Admin-only endpoints currently include:

- `GET /metrics`
- `POST /admin/compact`

Backup is intentionally separate from admin. A backup token can create a backup
but cannot compact the database or access unrelated admin endpoints.

## Audit logging

Security audit events are emitted through the `omnikv.audit` tracing target.
Audit records include event name, outcome, subject, role, route, and reason.
They intentionally do not log bearer tokens, bootstrap keys, request bodies, or
user data values.

Audited events include:

| Event | When it is emitted |
| --- | --- |
| `auth.failure` | Missing, malformed, or invalid bearer token. |
| `authz.denied` | Valid token with insufficient role for the route. |
| `auth.token.created` | Bootstrap key successfully minted a JWT. |
| `auth.token.denied` | Bootstrap token request rejected. |
| `backup.created` | Backup request succeeds or fails. |
| `admin.compact` | Compaction request succeeds or fails. |
| `data.delete` | Direct REST delete succeeds or fails. |
| `data.batch_delete` | Batch containing deletes succeeds or fails. |

Future restore/config endpoints must add audit events before they are exposed.
Use event names such as `restore.started`, `restore.completed`,
`restore.failed`, and `config.changed`.

## Abuse controls

REST has two request guards:

- Rate limiting is applied before route handling. Authenticated requests are
  limited by JWT subject; unauthenticated requests fall back to peer or
  forwarded IP identity.
- JSON body size is capped at 1 MiB.

REST scan results are capped:

- default limit: 1000 rows
- maximum limit: 10000 rows

Operators should alert on `omnikv_rate_limit_rejections_total{protocol="rest"}`
and tune `OMNIKV_RATE_LIMIT_PER_SEC`, `OMNIKV_RATE_LIMIT_BURST`, and
`OMNIKV_RATE_LIMIT_MAX_USERS` for their environment.

## Production deployment checklist

- Run with `OMNIKV_MODE=production`.
- Set `OMNIKV_JWT_SECRET` and `OMNIKV_BOOTSTRAP_ADMIN_KEY` through a secret
  manager, not a committed config file.
- Use TLS certificate and key files for every network-facing deployment.
- Only use `OMNIKV_TLS_INSECURE_SKIP=true` for local demos or isolated test
  networks.
- Issue separate short-lived tokens for read, write, backup, and admin
  automation.
- Keep admin tokens rare and short-lived.
- Ship `omnikv.audit` logs to the same retention pipeline as other security
  logs.
- Test backup creation and restore drills before relying on backups.
- Keep rate limits enabled and monitor rejection counters.

