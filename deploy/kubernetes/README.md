# Kubernetes deployment notes

OmniKV includes a single-node Kubernetes example for smoke testing and evaluation. Treat it as a starting point, not a production operator.

## What the example provides

- non-root pod security context;
- persistent volume for `/data`;
- secrets injected through Kubernetes Secret references;
- HTTPS, PgWire, and TCP service ports;
- liveness and readiness probes against `/health` and `/ready`.

Apply it with:

```bash
kubectl apply -f deploy/kubernetes/omnikv-single-node.yaml
```

Before applying, replace the placeholder values in `omnikv-secrets` or create the Secret out of band:

```bash
kubectl -n omnikv create secret generic omnikv-secrets \
  --from-literal=jwt-secret="$(openssl rand -hex 32)" \
  --from-literal=bootstrap-admin-key="$(openssl rand -hex 32)"
```

## Production caveats

- The sample is single-node. It does not prove distributed safety under Kubernetes partitions or pod churn.
- `OMNIKV_TLS_INSECURE_SKIP=true` is included only because the current server generates self-signed certificates. Replace this with real TLS material before real deployments.
- Backups, restore drills, upgrades, pod disruption budgets, resource limits, topology spread, and volume snapshot policies must be designed for your environment.
- Use the Compose smoke script and release smoke checks before promoting an image.
