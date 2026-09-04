# Badger value-log GC

Dingo's Badger blob store runs value-log GC every five minutes with a discard
ratio of `0.5`. This is separate from Badger's automatic LSM compaction. The
default remains this conservative policy until production measurements show a
better tradeoff; disabling LSM compaction is not part of the policy options.

## Repeatable measurement procedure

Run the synthetic ratio benchmark from an isolated checkout:

```sh
GOWORK=off GOCACHE=/tmp/dingo-gc-cache \\
  go test ./database/plugin/blob/badger \\
  -run '^$' -bench '^BenchmarkValueLogGC$' -benchmem -count=5
```

The benchmark compares discard ratios `0.25`, `0.50`, and `0.75` with the
background ticker disabled. Record `ns/op`, allocations, and the GC metrics
from a registry-enabled store. For production-shaped evidence, repeat the
same comparison while loading a fixed dataset for each workload: from-genesis
sync, Mithril/bootstrap load, API backfill, history expiry/tombstones, and
steady-state relay/API traffic. Keep the dataset, hardware, Dingo revision,
Badger options, and workload duration fixed. Record throughput, disk growth,
LSM/vlog sizes, GC CPU/I/O, query latency, and write-stall/compaction metrics.

Compare these policies:

| Policy | Purpose |
| --- | --- |
| `5m / 0.50` | Current default baseline |
| GC disabled during load, enabled afterward | Bulk-load control |
| Longer interval | Lower steady-state GC interference |
| `0.25` or `0.75` discard ratio | Reclaim-efficiency sensitivity |
| Adaptive interval/ratio | Candidate only if measurements justify its complexity |

The default decision is to retain `5m / 0.50` until every workload has a
reproducible result showing that another policy improves disk growth and
throughput without unacceptable latency, CPU, I/O, or shutdown impact. Operators
can disable GC during controlled bulk loads with the existing `gc: false`
setting, then re-enable it before steady-state operation.

## Metrics

When Prometheus metrics are configured, inspect:

- `database_blob_gc_attempts_total`, `..._successes_total`,
  `..._no_rewrite_total`, and `..._errors_total`;
- `database_blob_gc_duration_seconds`;
- `database_blob_gc_lsm_bytes`, `..._vlog_bytes`, and
  `..._reclaimed_bytes`;
- `database_blob_gc_consecutive_successes` and
  `database_blob_gc_last_success_timestamp_seconds`.

## Shutdown behavior

Badger cannot cancel an in-flight value-log rewrite. `CloseContext` stops new
GC passes, waits for the current rewrite, and returns the caller's deadline
error if that rewrite does not finish in time; the one-time cleanup then closes
the database after the rewrite drains. The regression test
`TestProviderStopDeadlineDuringValueLogGC` injects a blocked rewrite and proves
that the stop call returns at its deadline, then the store closes after the
rewrite is released.
