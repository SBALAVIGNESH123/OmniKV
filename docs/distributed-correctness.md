# Distributed correctness

OmniKV includes Raft-oriented storage, network/RPC scaffolding, and a
single-process distributed test harness. This document states what the current
evidence supports, what it does not support, and which checks are required
before stronger production high-availability claims.

## Current guarantee boundary

The current automated evidence supports these lower-level properties:

- committed Raft log entries can be replicated and applied consistently across
  multiple `OmniRaftStorage` instances;
- stale minority-partition entries are not treated as committed data in the
  tested partition scenarios;
- majority-side entries converge to lagging replicas after a partition heals;
- committed data and Raft metadata survive node reopen/restart scenarios;
- membership add/remove/re-add flows catch nodes up in the current harness;
- snapshot install can recover a lagging node after leader log compaction, then
  survive restart and continue applying post-snapshot logs.

These are evidence claims for the current implementation and deterministic test
harness. They are not a claim that OmniKV has completed production-grade,
multi-process, networked consensus validation.

## Consistency semantics currently expected

For the tested Raft path:

- writes are considered safe only after they are present on a quorum and applied
  through the Raft log path;
- minority-only writes are treated as uncommitted and must not be surfaced as
  durable committed data after reconciliation;
- lagging nodes must either catch up from retained logs or install a snapshot;
- after reconciliation, all reachable nodes should agree on the committed
  key/value state for the tested key ranges.

Reads from an explicitly stale or partitioned node are outside the current
linearizable-read guarantee. Production deployments must route reads through a
leader or a documented read-index/lease mechanism before advertising
linearizable reads.

## Automated evidence

Run the distributed gate:

```bash
cargo test -p omnikv-engine --test raft_cluster -- --test-threads=1
```

Important coverage includes:

| Area | Evidence |
| --- | --- |
| Log replication | 3-node replication and log consistency checks |
| Leader failover | leader crash/recovery and leader-under-load scenarios |
| Partitions | majority progress, stale leader supersession, asymmetric and cascading partition scenarios |
| Snapshot catch-up | simulated snapshot catch-up plus real snapshot install after partition, compaction, and restart |
| Membership | add, remove, re-add, scale-out, and data-integrity scenarios |
| Restarts | rolling restart, full cluster restart, and post-restart writes |
| Ordering | logical Raft index ordering despite local sequence skew |

## Non-goals for the current release

The current evidence does not yet prove:

- Jepsen-grade linearizability under real process/network faults;
- production leader leases or read-index behavior;
- multi-process cluster orchestration with real packet loss, asymmetric latency,
  disk stalls, and process crashes;
- long-running split-brain resistance under continuous client traffic;
- production-grade snapshot transfer over real network streams;
- operational Raft health metrics for leader, term, commit index, applied index,
  peer lag, and snapshot failures.

## Production-readiness requirements before stronger claims

Before OmniKV is marketed as production HA, collect additional evidence:

1. Multi-process partition tests with real network isolation.
2. Linearizable read/write checks against a generated history.
3. Long-running failover soak under continuous write and read traffic.
4. Snapshot transfer tests over real transport with interrupted transfer and
   retry.
5. Membership change tests while writes continue.
6. Native Raft health metrics and alerts.
7. A documented client routing policy for leader-only writes and safe reads.

Until those are complete, describe OmniKV's distributed mode as an active
hardening area with meaningful lower-level Raft evidence, not as a fully proven
production HA database.

