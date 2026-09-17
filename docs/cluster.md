# Cluster mode (Raft consensus)

OmniKV ships a real 3-node (or more) replicated mode: every client write
goes through Raft consensus before it is acknowledged, a leader is
elected among the nodes, and killing the leader triggers a failover
that keeps committed data. This page is the operating documentation.

The evidence that this works as described runs in CI:

- `cargo test -p omnikv-server --test cluster_multiprocess` — spawns
  three REAL `omnikv-server` processes, elects a leader, replicates a
  write to all nodes, **hard-kills the leader process**, and asserts a
  new leader is elected with zero data loss.
- `scripts/cluster-compose-smoke.sh` — the same story with Docker
  containers (kill the leader container).
- `docker compose up` — the 3-node demo with Prometheus/Grafana.

## Enabling cluster mode

Two environment variables make a server a cluster member; both must be
set together (the config validator refuses a half-configured cluster):

```bash
OMNIKV_NODE_ID=1          # this node's Raft id (1, 2, 3, …)
OMNIKV_RAFT_ADDR=0.0.0.0:9090  # the BIND address for the peer listener
# When the bind address is a wildcard, peers also need the routable
# address to DIAL — see "Bind vs advertised address" below.
OMNIKV_RAFT_ADVERTISE_ADDR=node-1.example:9090
```

Optional tuning (defaults suit tests and LANs):

```bash
OMNIKV_RAFT_HEARTBEAT_MS=500
OMNIKV_RAFT_ELECTION_MIN_MS=1500
OMNIKV_RAFT_ELECTION_MAX_MS=3000
```

A server started **without** these variables is a fully independent
single-node engine — no consensus machinery runs at all.

## Bootstrap

Node 1 of a fresh cluster carries the complete initial membership in
`OMNIKV_RAFT_PEERS` (peer ids are assigned 2, 3, … in declaration
order):

```bash
OMNIKV_RAFT_PEERS="node-2.example:9090,node-3.example:9090"
```

Node 1 calls openraft's `initialize` with itself plus every peer as
voters. Nodes 2 and 3 start blank and join the cluster as the leader's
replication reaches them — the same path openraft learners join through.
A node that restarts with persisted membership re-joins automatically.

The `docker-compose.yml` demo wires exactly this: all nodes use the
same internal raft port (9090) on the private compose network, node 1
lists the other two, and each node advertises its compose hostname
(`omni-node-N:9090`) so peers can dial it. The consensus ports are NOT
published to the host — nothing off the compose network needs them
(Prometheus scrapes the client HTTPS port); keeping them private limits
exposure to the plaintext peer traffic (see "Known limitations").

## Bind vs advertised address

`OMNIKV_RAFT_ADDR` is what the listener **binds** to; a wildcard
(`0.0.0.0:9090`) is correct inside a container. The address that goes
into cluster membership — what peers **dial** to reach this node — is
`OMNIKV_RAFT_ADVERTISE_ADDR`, falling back to the bind address when
unset. A wildcard must never be advertised: peers dialing `0.0.0.0:9090`
reach *themselves*, silently breaking votes, replication, and catch-up
toward this node after any failover or restart. The config validator
fails closed on a wildcard advertised address (explicit override or
inferred from the bind), naming the variable to set. A specific IP or
loopback bind needs no override; the container pattern (bind wide,
advertise the hostname) is exactly why the split exists.

## The write path

Every mutating client request — REST, QUIC, TCP, pgwire SQL, the
embedded API — lands in `OmniKV::commit_batch`, which (in cluster mode)
routes through the **cluster gateway**:

1. The gateway's single **flight lock** serializes proposals on the
   leader: at most one client write is in the air at a time.
2. SSI transactions re-run their full conflict validation **under that
   lock**, against the live committed history.
3. The write is proposed as ONE Raft log entry — atomic cluster-wide.
4. The gateway waits until the entry is applied by the local state
   machine, and the SSI commit record is appended **still under the
   lock**, so the next writer's validation can never miss it.
5. Only then is the client acknowledged. The ack means: **durable on a
   quorum and applied on this node (the leader)**. It does NOT mean every
   follower has applied it yet — followers apply asynchronously and their
   reads can lag by one replication round (see Read semantics). The
   entry's atomicity is never in question (one Raft log entry, applied in
   order everywhere), only the *timing* on followers.

Followers reject client writes with an error naming the current leader
(`not the leader; the leader is node 1`) — clients reconnect there. The
leader can move after a failover; the error always names the current
one.

The flight lock serializes proposals, but it does not bound how many
client requests are *waiting* on one: REST/QUIC handlers are async and
park their own runtime's worker while a proposal runs. Consensus
(openraft's internal tasks — heartbeats, elections, replication, the
apply loop — and the peer RPC listener) therefore runs on a **dedicated
consensus runtime**, never the client-facing server runtime. Any number
of concurrent writers can park server workers without starving the tasks
that must finish their proposals to unpark them; the failover test drives
16 concurrent writers through the leader as part of its evidence.

## Read semantics

Reads (GET, scans, SQL SELECT) are served from the node's local
applied state:

- On the **leader**, reads are current: the write path applies entries
  locally before acknowledging them.
- On **followers**, reads are eventually consistent — they see every
  committed write the follower has applied, which can lag the leader by
  one replication round. Read-your-writes is guaranteed only on the
  node you wrote to (and after a failover, until you reconnect to the
  new leader).

For topology (not data), `GET /cluster/status` is a public endpoint
(same posture as `/health`) reporting `mode`, `node_id`, `leader_id`,
`term`, `last_applied_index`, and the member list — the shape the
failover smoke and monitoring dashboards watch.

## Failover

When the leader dies (process crash, `docker kill`, host loss), the
remaining nodes elect a new leader after the election timeout (default
1.5–3 s; the smoke tests override to 150–600 ms). Committed data is
never lost: a write is only acknowledged after a quorum stored it, so
any electable leader holds everything the old leader acknowledged.

Writes sent to a follower during a leaderless window fail with
`no leader elected yet` until the election completes.

## Known limitations (honest, tracked)

- **SSI commit history is leader-local.** The committed-transaction
  history that powers serializable conflict detection lives on the
  leader. A transaction that began before a failover and commits after
  it may miss a conflict with a write committed by the previous leader.
  Tracked as #124; the fix is replicating SSI commit records inside the
  Raft command.
- **Peer traffic is plaintext HTTP.** The raft listener is a dedicated
  port following etcd's peer-port model: client TLS never terminates
  there, and consensus nodes authenticate by cluster membership. Keep
  it on a trusted network — the compose demos publish only the client
  ports to the host and leave the consensus port on the private compose
  network, so nothing off-network can reach it. Mutual TLS for peer
  traffic is tracked as #125, alongside the client-TLS work.
- **One write in flight cluster-wide.** The flight lock trades write
  throughput for a gap-free serialization point. Pipelined proposals
  are tracked as #126.
- **Follower reads lag.** See Read semantics above. Read-index
  (linearizable follower reads) is follow-up work.
- **Membership changes are restart-scoped.** Initial membership comes
  from node 1's env; adding/removing nodes at runtime needs the
  operator flow (add-learner / change-membership), which is follow-up
  work (the engine-side helpers exist in `raft_init.rs`).
- Not Jepsen-tested. The kill-the-leader evidence in CI is real but
  narrow; partitions, clocks, and long soaks are Phase 7 work.
