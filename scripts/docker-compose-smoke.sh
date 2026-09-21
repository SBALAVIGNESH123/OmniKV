#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${OMNIKV_SMOKE_COMPOSE_FILE:-$ROOT_DIR/docker-compose.smoke.yml}"
PROJECT_NAME="${OMNIKV_SMOKE_PROJECT:-omnikv-smoke}"
IMAGE="${OMNIKV_IMAGE:-omnikv:smoke}"
HTTP_PORT="${OMNIKV_HTTP_PORT:-18443}"
BUILD_IMAGE="${OMNIKV_SMOKE_BUILD:-true}"

export OMNIKV_IMAGE="$IMAGE"
export OMNIKV_HTTP_PORT="$HTTP_PORT"
export OMNIKV_JWT_SECRET="${OMNIKV_JWT_SECRET:-omnikv-smoke-jwt-secret-0123456789abcdef}"
export OMNIKV_BOOTSTRAP_ADMIN_KEY="${OMNIKV_BOOTSTRAP_ADMIN_KEY:-omnikv-smoke-admin-key-0123456789abcdef}"

cleanup() {
  docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [[ "$BUILD_IMAGE" != "false" ]]; then
  docker build --pull --tag "$IMAGE" "$ROOT_DIR"
fi

cleanup
docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" up -d

base_url="https://127.0.0.1:${HTTP_PORT}"

wait_for_health() {
  for _ in $(seq 1 60); do
    if curl -kfsS "${base_url}/health" >/dev/null; then
      return 0
    fi
    sleep 2
  done

  docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" logs --tail=200
  echo "OmniKV did not become healthy at ${base_url}/health" >&2
  return 1
}

# The REST envelope is {"success":bool,"data":...,"error":...}. A JSON
# parser would be cleaner, but python3 is not guaranteed where this runs
# (it is absent on Windows hosts), and the fields are simple enough that
# grep is exact: the token is base64url with no quotes, and the value's
# closing quote stops a prefix from matching a longer value.
json_field() {
  grep -o '"data":"[^"]*"' | cut -d'"' -f4
}

assert_value() {
  local expected="$1" body
  body="$(cat)"
  if [[ "$body" != *'"success":true'* ]]; then
    echo "response was not a success: $body" >&2
    return 1
  fi
  if [[ "$body" != *"\"value\":\"$expected\""* ]]; then
    echo "expected value $expected, got: $body" >&2
    return 1
  fi
}

wait_for_health

token="$(
  curl -kfsS \
    -X POST "${base_url}/auth/token" \
    -H "content-type: application/json" \
    -H "x-omni-admin-key: ${OMNIKV_BOOTSTRAP_ADMIN_KEY}" \
    --data '{"username":"compose-smoke","role":"write"}' \
    | json_field
)"

key="smoke:$(date +%s)"
value="compose-smoke-value"

curl -kfsS \
  -X POST "${base_url}/kv" \
  -H "authorization: Bearer ${token}" \
  -H "content-type: application/json" \
  --data "{\"key\":\"${key}\",\"value\":\"${value}\"}" \
  >/dev/null

curl -kfsS \
  -H "authorization: Bearer ${token}" \
  "${base_url}/kv/${key}" \
  | assert_value "$value"

docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" restart omnikv >/dev/null
wait_for_health

curl -kfsS \
  -H "authorization: Bearer ${token}" \
  "${base_url}/kv/${key}" \
  | assert_value "$value"

echo "OmniKV Docker Compose smoke passed: authenticated write/read survived restart."
