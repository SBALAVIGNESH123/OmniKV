# Docker, Compose, Kubernetes, and release smoke

This document explains how OmniKV packaging is validated and what evidence each path provides.

## Docker image contract

The Docker image:

- builds `omnikv-server` in a Rust builder stage;
- runs as the non-root `omnikv` user;
- stores runtime data under `/data`;
- ships `/etc/omni/omni.toml` without embedded secrets;
- requires `OMNIKV_JWT_SECRET` and `OMNIKV_BOOTSTRAP_ADMIN_KEY` to be supplied by the runtime;
- includes a container healthcheck against `https://127.0.0.1:8443/health`.

Build locally:

```bash
docker build --pull --tag omnikv:local .
```

## Single-node Compose smoke

The single-node smoke stack is intended for CI and release validation:

```bash
bash scripts/docker-compose-smoke.sh
```

On Windows PowerShell:

```powershell
.\scripts\docker-compose-smoke.ps1
```

The smoke script verifies:

1. the container starts and `/health` is reachable;
2. a write token can be minted through `/auth/token`;
3. an authenticated write succeeds;
4. an authenticated read returns the same value;
5. the container can restart;
6. the same value is readable after restart.

To smoke a published image instead of building locally:

```bash
export OMNIKV_IMAGE=ghcr.io/sbalavignesh123/omnikv:vX.Y.Z
export OMNIKV_SMOKE_BUILD=false
bash scripts/docker-compose-smoke.sh
```

The `OmniKV Release Smoke` GitHub Actions workflow runs the same smoke against a published image. It can be triggered manually with an image tag or automatically when a GitHub release is published. The workflow also uploads an SPDX JSON SBOM for the image.

## Multi-node Compose demo

`docker-compose.yml` remains the multi-node local demo stack with three OmniKV nodes plus Prometheus and Grafana.

```bash
export OMNIKV_JWT_SECRET="$(openssl rand -hex 32)"
export OMNIKV_BOOTSTRAP_ADMIN_KEY="$(openssl rand -hex 32)"
export OMNIKV_TLS_INSECURE_SKIP=true
docker compose up --build
```

The multi-node stack is useful for development and demonstration, but it is not a substitute for partition/failover testing.

## Kubernetes

The Kubernetes sample lives under `deploy/kubernetes/`.

```bash
kubectl apply -f deploy/kubernetes/omnikv-single-node.yaml
```

Read `deploy/kubernetes/README.md` before using it. The sample is single-node and includes explicit production caveats.

## SBOM

If `syft` is installed, generate an SPDX JSON SBOM for an image:

```bash
export OMNIKV_IMAGE=omnikv:local
bash scripts/generate-sbom.sh
```

Release artifacts should include:

- image digest;
- SBOM file;
- smoke-test log;
- Git commit SHA;
- any known caveats for that release.
