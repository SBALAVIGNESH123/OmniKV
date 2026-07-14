# Raft failover runbook

Use this runbook for multi-node evaluation and failure drills.

## Current status

OmniKV includes Raft-oriented storage, network, RPC routes, and cluster tests.
This is not yet a fully proven production high-availability deployment path.
Treat multi-node mode as an active hardening area until partition, failover,
membership, and long-running distributed tests are complete.

## What is currently tested

Run:

```bash
cargo test -p omnikv-engine --test raft_cluster -- --test-threads=1
```

This suite exercises single-process Raft cluster behavior, log replication,
snapshot-related paths, partition-style scenarios, and logical ordering checks.
For the exact guarantee boundary and non-goals, see
[Distributed correctness](../distributed-correctness.md).

## Operator expectations

In a healthy Raft deployment:

- exactly one leader should be active for a term;
- followers should continue applying committed entries;
- commit index and applied index should converge;
- lagging replicas should either catch up or install a snapshot;
- writes should be routed to the leader.

## Leader failure drill

1. Start a test cluster.
2. Identify the leader from logs or future Raft health metrics.
3. Stop the leader process.
4. Wait for a new leader.
5. Run write/read smoke against the new leader path.
6. Restart the old leader.
7. Verify it catches up before accepting normal traffic.

## Network partition drill

1. Start a cluster with an odd number of nodes.
2. Isolate a minority partition.
3. Confirm the majority side elects or retains a leader.
4. Confirm the minority side does not accept committed writes.
5. Heal the partition.
6. Confirm logs converge.
7. Run read smoke on every node.

This drill is covered in the deterministic test harness. Real multi-process
partition drills are still required before multi-node mode is marketed as
production high availability.

## Incident response

If leadership is unknown:

1. Stop client writes.
2. Capture logs from all nodes.
3. Capture `/metrics` and process health.
4. Check network connectivity between nodes.
5. Check clock and resource pressure.
6. Restart only one node at a time.
7. If a majority cannot be restored, recover from backup rather than forcing
   unsafe writes.

Unsafe actions:

- do not manually edit Raft log keys under `__sys__/raft/`;
- do not copy one node's data directory over another live node;
- do not force writes through a minority partition;
- do not remove replica retention pins without understanding catch-up state.

## Required future metrics

Before production multi-node use, expose:

- leader known / leader id;
- current term;
- commit index;
- applied index;
- replication lag per peer;
- unreachable peers;
- snapshot install count and failures;
- membership change status.

See [SLOs and alerts](slo-alerts.md) for alert examples.
