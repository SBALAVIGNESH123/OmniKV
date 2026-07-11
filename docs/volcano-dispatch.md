# Volcano Executor Dispatch

OmniKV's SQL executor uses a volcano-style pull pipeline. Operators expose
`RowIterator::next_row()` and can be composed behind `Box<dyn RowIterator>`.

That dynamic-dispatch design is intentionally extensible, but a row-at-a-time
hot path can pay a virtual call at every operator boundary for every emitted row.
The first production hardening step is therefore benchmark-driven chunking, not
an unconditional rewrite to enum-based dispatch.

## Current decision

Built-in streaming operators keep the object-safe `RowIterator` trait and now
also expose `next_chunk(max_rows, out)` for measurement and future targeted
batch consumers.

The default SQL execution path remains row-at-a-time dynamic dispatch:

```text
SQL execute
  -> iterator.next_row()
  -> ProjectIter::next_row()
  -> FilterIter::next_row()
  -> SeqScanIter::next_row()
```

Initial smoke and local measurements showed mixed results: chunking can help
some filter/projection-heavy pipelines, but it can also regress scan-only,
limit-heavy, and aggregate-shaped pipelines with the current `HashMap` row
representation. The extra scratch-buffer movement can dominate the saved vtable
calls. That supports the reviewer guidance: the dynamic call itself is not
necessarily the bottleneck when the work behind each call is larger than a few
cycles.

Chunked execution therefore stays as an explicit benchmarked alternative, not as
the default hot path. `collect_all()` also remains row-at-a-time so materializing
operators such as sort, aggregate, and hash-join do not silently change behavior
until a targeted benchmark justifies it. Hash join keeps the row-at-a-time
implementation for now because its match buffering behavior is more complex and
should be optimized with join-specific benchmarks.

## Why not enum dispatch yet?

Enum dispatch is still a valid future option, but it has tradeoffs:

- it couples the executor core to a closed set of operator variants,
- it increases match boilerplate as operators grow,
- it can increase compile-time and code-size pressure,
- it only helps if inlining enables meaningful cross-operator simplification.

Chunking was the smaller, lower-risk prototype to measure first. The current
evidence does not justify replacing the default row-at-a-time path with chunked
execution globally.

## How to measure

CI runs a release-mode smoke benchmark through:

```bash
cargo test -p omnikv-engine --test benchmarks --release -- --test-threads=1 --nocapture
```

For local fuller measurement:

```bash
cargo bench -p omnikv-engine --bench volcano_dispatch
cargo bench -p omnikv-engine --bench volcano_dispatch -- --rows 200000 --rounds 5
```

The benchmark compares row-at-a-time dynamic dispatch with chunked dispatch for:

- scan only
- scan + filter
- scan + projection
- scan + filter + projection + limit
- scan + aggregate

The CI smoke benchmark asserts semantic equivalence and prints throughput ratios;
it does not fail on throughput thresholds because those are host-dependent.
