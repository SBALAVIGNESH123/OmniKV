#!/usr/bin/env bash
# Cluster failover smoke (issue #113): the 3-node compose demo must
# elect a leader, replicate a write to every node, survive a hard kill
# of the leader container, and keep the data on the new leader.
#
# Mirrors scripts/docker-compose-smoke.sh conventions (env-configurable
# image/ports, cleanup trap, loud failure with container logs).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${OMNIKV_SMOKE_COMPOSE_FILE:-$ROOT_DIR/docker-compose.cluster-smoke.yml}"
PROJECT_NAME="${OMNIKV_SMOKE_PROJECT:-omnikv-cluster-smoke}"
IMAGE="${OMNIKV_IMAGE:-omnikv:cluster-smoke}"
BUILD_IMAGE="${OMNIKV_SMOKE_BUILD:-true}"

export OMNIKV_IMAGE="$IMAGE"
export OMNIKV_TLS_INSECURE_SKIP="${OMNIKV_TLS_INSECURE_SKIP:-true}"
export OMNIKV_JWT_SECRET="${OMNIKV_JWT_SECRET:-omnikv-smoke-jwt-secret-0123456789abcdef}"
export OMNIKV_BOOTSTRAP_ADMIN_KEY="${OMNIKV_BOOTSTRAP_ADMIN_KEY:-omnikv-smoke-admin-key-0123456789abcdef}"

# Host TCP ports the compose file maps for the three nodes.
TCP_PORTS=(18080 18081 18082)
NODES=(omni-cluster-smoke-node-1 omni-cluster-smoke-node-2 omni-cluster-smoke-node-3)
# Node 1's published HTTPS port — the harness mints its JWT here.
HTTP_PORT="${OMNIKV_HTTP_PORT:-18443}"

cleanup() {
  docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [[ "$BUILD_IMAGE" != "false" ]]; then
  docker build --pull --tag "$IMAGE" "$ROOT_DIR"
fi

cleanup
docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" up -d

# Mint the JWT the TCP interface requires (issue #117): same flow a real
# client uses — POST /auth/token with the bootstrap admin key over TLS.
# The nodes share one secret, so a token from node 1 is valid on all three
# and survives the later kill (it is only minted once, up front).
mint_token() {
  curl -sk -X POST "https://127.0.0.1:${HTTP_PORT}/auth/token" \
    -H "x-omni-admin-key: ${OMNIKV_BOOTSTRAP_ADMIN_KEY}" \
    -H 'content-type: application/json' \
    -d '{"username":"cluster-smoke","role":"admin","ttl_seconds":3600}' \
    | grep -o '"data":"[^"]*"' | cut -d'"' -f4
}

# One TCP command round trip: AUTH and the command are pipelined on one
# connection (the server frames by newline), and the reply we want is the
# second line — the command's own answer.
tcp_cmd() {
  local port="$1" cmd="$2"
  # shellcheck disable=SC2086
  printf 'AUTH %s\n%s\n' "$TOKEN" "$cmd" \
    | timeout 10 bash -c "exec 3<>/dev/tcp/127.0.0.1/$port; cat >&3; head -2 <&3 | tail -1"
}

# Same wire, deliberately no AUTH: the interface must refuse. Guards the
# regression this script caught when the AUTH gate landed.
tcp_cmd_unauthenticated() {
  local port="$1" cmd="$2"
  # shellcheck disable=SC2086
  printf '%s\n' "$cmd" | timeout 10 bash -c "exec 3<>/dev/tcp/127.0.0.1/$port; cat >&3; head -1 <&3"
}

# The leadership oracle: SET succeeds only on the leader (followers
# answer "not the leader; ..."), matching the Rust failover test.
find_leader_port() {
  for port in "${TCP_PORTS[@]}"; do
    if [[ "$(tcp_cmd "$port" 'SET __cluster_probe__ x')" == OK* ]]; then
      echo "$port"
      return 0
    fi
  done
  return 1
}

# Wait for all three TCP listeners, then for a leader (election may
# take a few seconds with the default timers).
wait_for_cluster() {
  for _ in $(seq 1 60); do
    local up=true
    for port in "${TCP_PORTS[@]}"; do
      if ! timeout 2 bash -c "exec 3<>/dev/tcp/127.0.0.1/$port" 2>/dev/null; then
        up=false
        break
      fi
    done
    if [[ "$up" == true ]]; then
      return 0
    fi
    sleep 2
  done
  return 1
}

if ! wait_for_cluster; then
  docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" logs --tail=200
  echo "FAIL: cluster nodes did not all come up" >&2
  exit 1
fi

# The token endpoint is ready once the listeners are; retry past startup.
# The assignment is guarded: with set -e, a failing curl inside the
# substitution would abort the script before the retry and the diagnostics
# below could ever run.
TOKEN=""
for _ in $(seq 1 30); do
  if ! TOKEN="$(mint_token)"; then
    TOKEN=""
  fi
  [[ -n "$TOKEN" ]] && break
  sleep 1
done
if [[ -z "$TOKEN" ]]; then
  docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" logs --tail=200
  echo "FAIL: could not mint a review token from https://127.0.0.1:${HTTP_PORT}/auth/token" >&2
  exit 1
fi

# The AUTH gate itself: a command with no token must be refused.
for port in "${TCP_PORTS[@]}"; do
  refused="$(tcp_cmd_unauthenticated "$port" 'GET cluster-smoke:key')"
  if [[ "$refused" != *"AUTH_REQUIRED"* ]]; then
    echo "FAIL: node on :$port accepted an unauthenticated command (${refused})" >&2
    exit 1
  fi
done
echo "PASS: all nodes refuse unauthenticated commands"

leader_port=""
for _ in $(seq 1 60); do
  if leader_port="$(find_leader_port)"; then
    break
  fi
  sleep 2
done
if [[ -z "$leader_port" ]]; then
  docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" logs --tail=200
  echo "FAIL: no leader elected within the timeout" >&2
  exit 1
fi
leader_idx=0
for i in "${!TCP_PORTS[@]}"; do
  if [[ "${TCP_PORTS[$i]}" == "$leader_port" ]]; then
    leader_idx="$i"
  fi
done
leader_node="${NODES[$leader_idx]}"
echo "PASS: leader elected (${leader_node}, TCP :${leader_port})"

# Replicate a write; every node must eventually serve it.
tcp_cmd "$leader_port" 'SET cluster-smoke:key replicated' | grep -q '^OK' || {
  echo "FAIL: leader write rejected" >&2
  exit 1
}
for port in "${TCP_PORTS[@]}"; do
  seen=""
  for _ in $(seq 1 10); do
    if [[ "$(tcp_cmd "$port" 'GET cluster-smoke:key')" == "OK: replicated" ]]; then
      seen=yes
      break
    fi
    sleep 1
  done
  if [[ "$seen" != yes ]]; then
    echo "FAIL: node on :$port never observed the replicated write" >&2
    exit 1
  fi
done
echo "PASS: write replicated to all 3 nodes"

# Followers must refuse writes.
follower_port="${TCP_PORTS[$(( (leader_idx + 1) % 3 ))]}"
follower_reply="$(tcp_cmd "$follower_port" 'SET cluster-smoke:other v')"
if [[ "$follower_reply" != *"leader"* ]]; then
  echo "FAIL: follower accepted a write (${follower_reply})" >&2
  exit 1
fi
echo "PASS: follower refused a write (${follower_reply})"

# Kill the leader container — no graceful shutdown.
docker kill "$leader_node" >/dev/null
echo "PASS: leader container ${leader_node} killed"

survivor_ports=("${TCP_PORTS[@]}")
unset "survivor_ports[$leader_idx]"
new_leader_port=""
for _ in $(seq 1 60); do
  for port in "${survivor_ports[@]}"; do
    if [[ "$(tcp_cmd "$port" 'SET __probe_after__ x')" == OK* ]]; then
      new_leader_port="$port"
      break 2
    fi
  done
  sleep 2
done
if [[ -z "$new_leader_port" ]]; then
  docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" logs --tail=200
  echo "FAIL: no new leader elected after killing ${leader_node}" >&2
  exit 1
fi
echo "PASS: new leader elected (TCP :${new_leader_port})"

# No data loss: the pre-kill write survived, and a fresh write works.
readback="$(tcp_cmd "$new_leader_port" 'GET cluster-smoke:key')"
if [[ "$readback" != "OK: replicated" ]]; then
  echo "FAIL: DATA LOSS on the new leader (GET cluster-smoke:key → ${readback})" >&2
  exit 1
fi
if ! tcp_cmd "$new_leader_port" 'SET cluster-smoke:post after-failover' | grep -q '^OK'; then
  echo "FAIL: write on the new leader rejected" >&2
  exit 1
fi
echo "PASS: no data loss; cluster fully alive after failover"

echo "ALL CLUSTER SMOKE CHECKS PASSED"
