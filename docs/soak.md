# Soak monitor

`cmd/soak` samples a running node's metrics to CSV, decides whether goroutines
or RSS grew steadily over the run, and summarises repeated WARN and ERROR log
messages. Use it for multi-day runs (72 hours or more) on preview or preprod.

The node serves `go_goroutines`, `process_resident_memory_bytes`,
`go_memstats_heap_*` and `go_gc_duration_seconds` on `/metrics` (default
`metricsPort` 12798) through the default Prometheus registerer.

## Sample

```sh
make soak-sample SOAK_CSV=soak.csv SOAK_ARGS='-interval 1m'
```

Flags for `soak sample`:

| Flag | Default | Meaning |
| --- | --- | --- |
| `-metrics-url` | `http://127.0.0.1:12798/metrics` | Node metrics endpoint |
| `-interval` | `1m` | Sampling interval |
| `-duration` | `0` | Stop after this long; 0 runs until interrupted |
| `-debug-url` | empty | pprof listener base URL (the node's `debugPort`); enables snapshots |
| `-snapshot-dir` | `soak-pprof` | Where goroutine and heap profiles are written |
| `-snapshot-every` | `60` | Snapshot every N samples |

Samples go to `$(SOAK_CSV)`; follow it with `tail -f`. A failed scrape is
logged to stderr and skipped, so a single outage does not end the run. Gaps
are visible in the CSV timestamps.

## Analyse

```sh
make soak-analyse SOAK_CSV=soak.csv
```

The first 25% of the run is excluded as warmup. Over the remaining plateau the
analyser fits a least-squares line to goroutines and RSS. A metric is reported
as growing, and the command exits 1, when its fitted growth exceeds 1% of the
plateau's starting value per hour and the fit has R^2 of at least 0.5. The R^2
floor keeps a noisy or sawtooth series from being called a leak. Tune with
`-warmup` (between 0 and 1), `-max-growth-percent-per-hour` (positive) and
`-min-r2` (above 0, at most 1); a value outside those ranges exits 2.

A change in `process_start_time_seconds`, or a decrease in the cumulative GC
count, means the process restarted; the run is then not one continuous soak
and the command exits 1. GC cycles per hour and
mean pause over the plateau are reported but do not gate the result. Fewer
than 10 plateau samples is an error (exit 2).

## Logs

```sh
go run ./cmd/soak logs -min-count 10 node.log
```

Reads slog JSON or text lines and lists WARN and ERROR messages seen at least
`-min-count` times, most frequent first. Hex strings of 16 or more characters,
IPv4 addresses and numbers are masked so one message with varying values counts
as one.

## Reading the result

Compare the growing metric with the goroutine snapshots: `go tool pprof` on a
heap profile, or diff two `goroutine-*.pprof` files to see which stacks
accumulated.
