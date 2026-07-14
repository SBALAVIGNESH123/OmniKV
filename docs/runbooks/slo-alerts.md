# SLOs and alerts runbook

This runbook defines beta SLOs and alert examples for OmniKV.

The current server exposes Prometheus metrics at `/metrics`. Some operational
signals, especially disk and WAL file size, should come from host exporters or a
textfile collector until OmniKV exports native gauges for them.

## Suggested beta SLOs

| Area | Suggested SLO | Notes |
| --- | --- | --- |
| Availability | `/ready` succeeds for 99.5% of 1-minute probes | beta target for evaluation environments |
| Write latency | p99 write latency below 100 ms over 5 minutes | tune per hardware and workload |
| Read latency | p99 read latency below 50 ms over 5 minutes | tune per hardware and workload |
| Error rate | 5xx or storage errors below 1% over 5 minutes | use ingress/proxy metrics plus logs |
| Durability | every release candidate passes backup/restore and crash consistency tests | not a runtime SLO; release gate |
| Restore | latest backup restores successfully in a scheduled drill | recommended at least weekly for demos |

## Native OmniKV metrics

Current native metrics include:

- `omnikv_writes_total`
- `omnikv_reads_total`
- `omnikv_commits_total`
- `omnikv_write_latency_seconds`
- `omnikv_read_latency_seconds`
- `omnikv_memtable_size_bytes`
- `omnikv_sstable_count`
- `omnikv_db_sequence`
- `omnikv_uptime_seconds`
- `omnikv_compactions_total`
- `omnikv_compaction_latency_seconds{stage=...}`
- `omnikv_compaction_bytes_rewritten_total{stage=...}`
- `omnikv_compaction_tombstones_total{stage=...}`
- `omnikv_compaction_expired_records_dropped_total{stage=...}`
- `omnikv_compaction_backlog_sstables`
- `omnikv_replica_retention_floor`
- `omnikv_write_stalls_total`
- `omnikv_rate_limit_rejections_total{protocol=...}`
- `omnikv_cleanup_delete_failures_total{context=...,error_kind=...}`

## Alert examples

### Disk space

Use node exporter, cAdvisor, or cloud disk metrics:

```promql
node_filesystem_avail_bytes{mountpoint="/var/lib/omnikv"}
/
node_filesystem_size_bytes{mountpoint="/var/lib/omnikv"}
< 0.15
```

Page below 10%. Warn below 15%.

### WAL size

OmniKV does not currently expose a native WAL size gauge. Until it does, export
one from the host with a textfile collector or sidecar:

```promql
omnikv_wal_size_bytes > 1073741824
```

Tune the threshold by workload. A fast-growing WAL should trigger a check of
write volume, flush behavior, disk pressure, and backup policy.

### Compaction backlog

```promql
omnikv_compaction_backlog_sstables > 8
```

Warn when backlog remains above the normal operating range for 10 minutes.
Page if write stalls begin:

```promql
increase(omnikv_write_stalls_total[5m]) > 0
```

### Compaction latency

```promql
histogram_quantile(
  0.99,
  rate(omnikv_compaction_latency_seconds_bucket[10m])
) > 5
```

Tune by dataset size and disk class.

### Read/write latency

```promql
histogram_quantile(0.99, rate(omnikv_write_latency_seconds_bucket[5m])) > 0.1
```

```promql
histogram_quantile(0.99, rate(omnikv_read_latency_seconds_bucket[5m])) > 0.05
```

Use sustained windows to avoid paging on short benchmark spikes.

### Errors and cleanup failures

If OmniKV is behind an ingress or proxy:

```promql
sum(rate(http_requests_total{job="omnikv",status=~"5.."}[5m]))
/
sum(rate(http_requests_total{job="omnikv"}[5m]))
> 0.01
```

Native cleanup failure alert:

```promql
increase(omnikv_cleanup_delete_failures_total[10m]) > 0
```

Rate-limit alert:

```promql
increase(omnikv_rate_limit_rejections_total[5m]) > 100
```

### Raft health

The current code has Raft tests and RPC routes, but production-grade native
Raft health metrics are still part of the distributed hardening track. Until
native metrics are added, use synthetic checks and logs. Recommended future
metrics:

- `omnikv_raft_leader_known`
- `omnikv_raft_current_term`
- `omnikv_raft_commit_index`
- `omnikv_raft_applied_index`
- `omnikv_raft_replication_lag`
- `omnikv_raft_unreachable_peers`

Example alert once exported:

```promql
omnikv_raft_leader_known == 0
```

```promql
omnikv_raft_replication_lag > 1000
```

For current releases, validate Raft changes with:

```bash
cargo test -p omnikv-engine --test raft_cluster -- --test-threads=1
```

## Dashboard starter panels

Create panels for:

- readiness probe success;
- read and write p50/p95/p99 latency;
- write throughput and read throughput;
- commit rate;
- memtable size;
- SSTable count;
- compaction backlog;
- write stalls;
- cleanup failures;
- rate-limit rejections;
- disk usage;
- WAL file size;
- Raft leader and replication health when native metrics are added.
