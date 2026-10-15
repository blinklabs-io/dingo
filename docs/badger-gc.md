# Badger value-log GC

Dingo's Badger blob store runs value-log GC every five minutes with a discard
ratio of `0.5`. Configure these values with the Badger provider options
`gcInterval` (at least one second, such as `10m`; zero keeps the default) and
`gcDiscardRatio` (greater than 0 and less than 1). Invalid values are rejected
at startup. Set `gc: false` during a controlled bulk load and re-enable it
before serving traffic. Value-log GC is separate from Badger's automatic LSM
compaction.

Keep the defaults unless measurements on the node's own dataset and storage
show a better tradeoff. Use the procedure and comparison fields below before
changing them.

## Representative benchmark

`BenchmarkBadgerGCPolicy` replays all 59,315 blocks in
`database/immutable/testdata` (79,866,190 bytes of block CBOR) through the
production Badger blob-store methods. It is opt-in and requires
`-benchtime=1x`. Its workloads represent:

| Workload | Operations represented |
| --- | --- |
| `core-load` | The block-blob writes shared by from-genesis sync and Mithril bootstrap/load. Both loaders reach the same blob-store boundary. |
| `api-backfill` | Historical block-blob reads in chain order. Metadata backfill writes use the metadata store and are outside value-log GC. |
| `history-expiry` | Replacement of the oldest 75% of block values with history tombstones. |
| `steady-state` | Random block reads followed by rewrites of 10% of the fixture. |

One `core-load` pass is append-only and has no obsolete block values for GC to
reclaim. Repeating the fixture writes the same block keys and creates obsolete
versions. The tombstone and rewrite workloads also create obsolete values.
Keep those cases distinct when interpreting the results.

The policy syntax is a comma-separated list:

| Policy | Benchmark behavior |
| --- | --- |
| `off` | Disable background value-log GC. |
| `post/0.5` | Close and reopen after the workload, then drain GC immediately. This can report `ErrNoRewrite` before LSM compaction has produced discard statistics. |
| `settled/0.25`, `settled/0.5`, `settled/0.75` | Close and reopen, force LSM compaction, then drain GC at the selected ratio. This measures reclaim after compaction and reports the compaction duration separately. |
| `adaptive/0.5` | Benchmark-only candidate: after closing the workload, if its database has more than one SSTable and its value log is larger than the configured value-log file size, reopen it, force compaction, and drain GC. |
| `5m/0.5`, `15m/0.5` | Exercise the production background worker with the named interval and ratio. |
| `50ms/0.5` | Controlled scaled-interval mode used to expose interference and no-rewrite attempts in a short run. It is not a proposed operator value. |

The adaptive candidate is deliberately limited to the benchmark. Its two
signals establish that compaction can consolidate versions and that the value
log has grown by at least one configured file. Forced compaction is a
substantial extra operation, so the candidate must still show an end-to-end
benefit before it becomes production behavior.

### Calibrate the method

Start with one workload and two policies. This should finish in well under a
minute on a development machine:

```sh
GOCACHE=/tmp/dingo-badger-gc-cache \
  DINGO_BENCH_BADGER_GC=1 \
  DINGO_BENCH_BADGER_GC_WORKLOADS=history-expiry \
  DINGO_BENCH_BADGER_GC_POLICIES=off,50ms/0.5 \
  go test ./internal/integration -run '^$' \
  -bench '^BenchmarkBadgerGCPolicy$' -benchtime=1x -count=1
```

Check that `input-blocks`, `input-bytes`, `max-block-bytes`, final LSM/value-log
sizes, workload rates, and GC counters are nonzero where expected before
running a matrix. The benchmark removes each temporary database after its
sub-benchmark. Set `DINGO_BENCH_BADGER_GC_DATADIR` to put those temporary
databases on the filesystem being measured.

### Production options and real intervals

This command keeps the production value threshold, value-log file size, and
128 MiB memtable. Three passes write 239,598,570 bytes of block CBOR, exceeding
the memtable size and creating obsolete values under the same keys:

```sh
for policy in off post/0.5 adaptive/0.5 50ms/0.5 5m/0.5 15m/0.5; do
  /usr/bin/time -v env \
    GOCACHE=/tmp/dingo-badger-gc-cache \
    DINGO_BENCH_BADGER_GC=1 \
    DINGO_BENCH_BADGER_GC_REPEATS=3 \
    DINGO_BENCH_BADGER_GC_WORKLOADS=history-expiry \
    DINGO_BENCH_BADGER_GC_POLICIES="$policy" \
    go test ./internal/integration -run '^$' \
    -bench '^BenchmarkBadgerGCPolicy$' -benchtime=1x -count=3
done
```

A representative workload normally finishes before a five- or fifteen-minute
ticker fires. Such a result must report zero attempts; it must not be treated
as evidence that those intervals performed GC. Use a scaled interval in the
same run to prove the worker is active and to measure interference. For a
causal measurement of a real interval, lengthen the workload past that
interval rather than extrapolating from the scaled result.

### Controlled value-log stress

The real fixture's block values are below the production 1 MiB value threshold
and normally remain in the LSM. To compare discard ratios with actual
value-log rewrites, use a controlled stress configuration and label it as
calibration rather than a production forecast:

```sh
for policy in off post/0.5 settled/0.25 settled/0.5 settled/0.75 adaptive/0.5 50ms/0.5; do
  /usr/bin/time -v env \
    GOCACHE=/tmp/dingo-badger-gc-cache \
    DINGO_BENCH_BADGER_GC=1 \
    DINGO_BENCH_BADGER_GC_VALUE_THRESHOLD=1 \
    DINGO_BENCH_BADGER_GC_VLOG_FILE_SIZE=16777216 \
    DINGO_BENCH_BADGER_GC_MEMTABLE_SIZE=4194304 \
    DINGO_BENCH_BADGER_GC_WORKLOADS=history-expiry \
    DINGO_BENCH_BADGER_GC_POLICIES="$policy" \
    go test ./internal/integration -run '^$' \
    -bench '^BenchmarkBadgerGCPolicy$' -benchtime=1x -count=3
done
```

`post` and `settled` answer different questions. Report both. An immediate
post-load attempt can find no rewrite because Badger learns discardable value
pointers during LSM compaction. `settled` includes that compaction and reports
its time as `settle-compaction-s`.

Available environment controls are:

- `DINGO_BENCH_BADGER_GC_WORKLOADS`
- `DINGO_BENCH_BADGER_GC_POLICIES`
- `DINGO_BENCH_BADGER_GC_REPEATS`
- `DINGO_BENCH_BADGER_GC_VALUE_THRESHOLD`
- `DINGO_BENCH_BADGER_GC_VLOG_FILE_SIZE`
- `DINGO_BENCH_BADGER_GC_MEMTABLE_SIZE`
- `DINGO_BENCH_BADGER_GC_DATADIR`

## Measured comparison and default decision

The reference measurements used Linux/arm64, a 128-core Neoverse-N1 host,
Go 1.26.9, an ext filesystem, and revision `ad0c4b9666d6` plus this benchmark.
Each table reports the median of three runs. CPU is the whole command's
`/usr/bin/time -v` percentage; process I/O is the median per sub-benchmark.
Cached reads commonly report zero physical read bytes. Throughput varied
enough that the GC counters, final size, and latency comparisons carry more
weight than small differences in load rate.

The production-options `history-expiry` run repeated the fixture three times.
It wrote 239,598,570 bytes of block CBOR, exceeded the 128 MiB memtable, and
created three SSTables. The largest block was 64,072 bytes, below the 1 MiB
value threshold.

| Policy | Final disk | Load / tombstones per second | CPU | Process read/write | GC result |
| --- | ---: | ---: | ---: | ---: | --- |
| `off` | 212,157,251 B | 8,682 / 105,081 | 111% | 0 / 882,819,072 B | no attempts |
| `post/0.5` | 212,157,271 B | 7,963 / 101,180 | 93% | 4,096 / 882,839,552 B | 1 no-rewrite; 0 B reclaimed |
| `adaptive/0.5` | 212,157,271 B | 9,235 / 104,627 | 124% | 0 / 882,839,552 B | gate did not trigger; no attempts |
| `50ms/0.5` | 212,157,251 B | 9,801 / 99,076 | 111% | 0 / 882,819,072 B | 372 no-rewrite attempts; 0 B reclaimed |
| `5m/0.5` | 212,157,251 B | 8,514 / 95,702 | 107% | 0 / 882,819,072 B | no attempts before first tick |
| `15m/0.5` | 212,157,251 B | 10,245 / 53,836 | 103% | 0 / 882,819,072 B | no attempts before first tick |

The final value log was only its 20-byte header, or 40 bytes after a reopen.
Neither a lower interval nor an immediate post-load attempt could reclaim LSM
values. The adaptive gate correctly skipped compaction because there was no
meaningful value-log growth.

The production-options read workloads compare GC disabled with the 50 ms
scaled control. The CPU values cover the combined API and steady-state command.

| Workload / policy | Final disk | Read p95 | Reads/s | Other throughput | CPU | Process read/write | GC result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| API backfill / `off` | 77,786,275 B | 22,000 ns | 60,353 | 13,655 load blocks/s | 142% | 0 / 298,356,736 B | no attempts |
| API backfill / `50ms/0.5` | 77,786,275 B | 25,641 ns | 52,923 | 15,010 load blocks/s | 165% | 0 / 298,356,736 B | 101 no-rewrite attempts |
| Steady state / `off` | 82,038,288 B | 21,761 ns | 54,716 | 23,348 rewrites/s | 142% | 0 / 324,284,416 B | no attempts |
| Steady state / `50ms/0.5` | 82,038,288 B | 21,360 ns | 63,643 | 24,019 rewrites/s | 165% | 0 / 324,284,416 B | 86 no-rewrite attempts |

The scaled control increased API read p95 by 16.6% and reduced API read
throughput by 12.3%, while steady-state results remained within run-to-run
variation. It increased command CPU without changing disk size or process I/O.

The controlled `history-expiry` stress put every value in the value log and
used a 4 MiB memtable so compaction produced discard statistics. These results
calibrate ratio and adaptive behavior; they are not a production forecast.

| Policy | Final disk | Load / tombstones per second | CPU | Process read/write | GC result |
| --- | ---: | ---: | ---: | ---: | --- |
| `off` | 111,119,573 B | 13,147 / 64,079 | 91% | 0 / 375,201,792 B | no attempts |
| `post/0.5` | 111,118,602 B | 12,667 / 118,820 | 119% | 0 / 375,357,440 B | 1 no-rewrite; 0 B reclaimed |
| `settled/0.25` | 62,526,058 B | 13,063 / 126,114 | 122% | 0 / 408,580,096 B | 4 rewrites; 63,789,734 B reclaimed; 2.518 s GC |
| `settled/0.5` | 62,525,425 B | 14,270 / 127,948 | 142% | 0 / 408,584,192 B | 4 rewrites; 63,789,741 B reclaimed; 2.729 s GC |
| `settled/0.75` | 73,193,458 B | 18,136 / 132,614 | 154% | 0 / 400,953,344 B | 3 rewrites; 46,994,903 B reclaimed; 2.161 s GC |
| `adaptive/0.5` | 62,524,775 B | 20,776 / 133,361 | 153% | 0 / 408,584,192 B | gate triggered; 4 rewrites; 63,790,176 B reclaimed; 2.469 s GC |
| `50ms/0.5` | 111,119,337 B | 5,454 / 61,153 | 63% | 0 / 375,205,888 B | 232 no-rewrite attempts; 0 B reclaimed |

Ratios `0.25` and `0.5` reached the same final size and rewrite count. Ratio
`0.75` saved one rewrite and 0.568 seconds of median GC time relative to `0.5`,
but left 10,668,033 more bytes on disk. The adaptive candidate matched settled
`0.5` only after adding a forced compaction, so it showed no additional benefit.
The frequent ticker attempted GC before discard statistics were available and
cut median load throughput by 58.5% relative to `off` without reclaiming data.

Keep the five-minute interval and `0.5` ratio. The real blocks did not enter
the value log, so more frequent GC added attempts and API latency without a
disk benefit. The controlled ratio comparison does not show a production
benefit large enough to change the ratio, and the adaptive candidate adds a
forced-compaction decision without improving the settled result. Operators
should continue disabling GC during controlled bulk load and restoring the
default for serving, where it remains a low-frequency precaution for values
larger than the threshold.

## Reported evidence

The benchmark reports:

- workload volume and rates: `input-blocks`, `input-bytes`,
  `max-block-bytes`, `load-blocks/s`, `reads/s`, `tombstones/s`, and
  `rewrite-blocks/s`;
- read latency: `read-p50-ns`, `read-p95-ns`, `read-p99-ns`, and
  `read-max-ns`;
- storage: `lsm-bytes`, `vlog-bytes`, `disk-bytes`, `allocated-bytes`, and
  `sst-tables`;
- GC: attempts, rewrites, `ErrNoRewrite` results, errors, duration, reclaimed
  bytes, settling time, and whether the adaptive candidate triggered;
- pressure: maximum pending memtables, maximum concurrent LSM compactions,
  compaction write bytes, process read/write bytes, RSS, and shutdown time.

Process I/O and RSS use Linux `/proc`; the metrics are omitted when those
files are unavailable. Wrap the benchmark in `/usr/bin/time -v` to record CPU
percentage, maximum RSS, filesystem I/O, and wall time for the whole command.
The Go benchmark `ns/op` includes fixture loading and shutdown.

For a running node, inspect:

- `database_blob_gc_attempts_total`, `..._successes_total`,
  `..._no_rewrite_total`, and `..._errors_total`;
- `database_blob_gc_duration_seconds`;
- `database_blob_gc_lsm_bytes`, `..._vlog_bytes`, and
  `..._reclaimed_bytes`, read from the on-disk file sizes around each rewrite;
- `database_blob_gc_consecutive_successes` and
  `database_blob_gc_last_success_timestamp_seconds`.

## Shutdown behavior

Badger cannot cancel an in-flight value-log rewrite. `CloseContext` stops new
GC passes, waits for the current rewrite, and returns the caller's deadline
error if that rewrite does not finish in time. Cleanup then closes the database
after the rewrite drains. `TestProviderStopDeadlineDuringValueLogGC` injects a
blocked rewrite and proves that the stop call returns at its deadline, then the
store closes after the rewrite is released. The representative benchmark also
reports normal `shutdown-s` for every policy.
