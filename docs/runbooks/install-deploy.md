# Install and deploy runbook

Use this runbook to start OmniKV locally, validate the Docker image, or apply
the Kubernetes sample.

## Local development

```bash
cargo run -p omnikv-server
```

Default development settings:

- HTTP/1.1 and HTTP/2: `127.0.0.1:7070`
- QUIC/HTTP3: `127.0.0.1:7071`
- PostgreSQL wire: `127.0.0.1:5432`
- TCP command interface: `127.0.0.1:7072`
- development JWT secret
- TLS not required

Development mode is intentionally permissive. Do not use the defaults for an
internet-facing deployment.

## Production-style local start

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

Production mode fails closed for invalid config, weak secrets, missing TLS
files, disabled rate limits, and internally inconsistent compaction settings.
See [Configuration](../configuration.md) for the full environment variable
reference.

## Health checks

After startup:

```bash
curl -k https://127.0.0.1:8443/health
curl -k https://127.0.0.1:8443/ready
ADMIN_TOKEN="$(
  curl -sk -X POST https://127.0.0.1:8443/auth/token \
    -H "x-omni-admin-key: ${OMNIKV_BOOTSTRAP_ADMIN_KEY}" \
    -H "content-type: application/json" \
    -d '{"username":"ops-smoke","role":"admin","ttl_seconds":300}' \
  | jq -r '.data'
)"
curl -k -H "Authorization: Bearer ${ADMIN_TOKEN}" https://127.0.0.1:8443/metrics
```

`/health` confirms the process is alive and can report storage stats.
`/ready` is the readiness gate. `/metrics` exposes Prometheus text metrics and
requires an admin bearer token.

## Docker image smoke

Build locally:

```bash
docker build --pull --tag omnikv:local .
```

Run the CI-equivalent single-node smoke:

```bash
bash scripts/docker-compose-smoke.sh
```

On Windows PowerShell:

```powershell
.\scripts\docker-compose-smoke.ps1
```

The smoke validates startup, health, token minting, authenticated write/read,
container restart, and post-restart read durability.

To smoke a published image:

```bash
export OMNIKV_IMAGE=ghcr.io/sbalavignesh123/omnikv:vX.Y.Z
export OMNIKV_SMOKE_BUILD=false
bash scripts/docker-compose-smoke.sh
```

## Kubernetes sample

The sample manifest is single-node and intended for evaluation:

```bash
kubectl apply -f deploy/kubernetes/omnikv-single-node.yaml
```

Read `deploy/kubernetes/README.md` before use. Do not treat the sample as a
high-availability production deployment.

## Deployment preflight

Before a production-style rollout:

- choose a dedicated `OMNIKV_DATA_DIR`;
- choose a backup directory outside the active data directory;
- configure TLS, a JWT secret of at least 32 characters, and a bootstrap admin
  key of at least 32 characters that is different from the JWT secret;
- set rate limits appropriate for the expected workload;
- verify the process has enough file descriptors and disk space;
- configure log collection;
- scrape `/metrics`;
- configure disk, WAL, compaction, latency, error, and Raft health alerts from
  [SLOs and alerts](slo-alerts.md);
- run the Docker smoke script against the exact image digest.

## Deployment rollback trigger

Rollback if any of these happen after deployment:

- `/ready` remains unhealthy after the expected startup window;
- write/read smoke fails;
- write stalls increase immediately and do not recover;
- compaction backlog grows continuously;
- logs show manifest, WAL, heap, or SSTable decode errors;
- Raft nodes disagree about leadership or applied index in a multi-node test.
