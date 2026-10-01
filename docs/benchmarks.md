# Benchmarks and profiling

Run benchmark commands from the repository root. The Makefile targets include
the default `dingo_extra_plugins` build tag and report memory allocations.

## Benchmarks

```sh
make bench
make bench-mempool
make bench-mempool-normal
make bench-mempool-degenerate
make bench-mempool-revalidation
```

`make bench` runs Go benchmarks across the repository. To focus on one
benchmark, package, or subsystem, use `go test` directly:

```sh
go test -run='^$' -bench='^BenchmarkName$' -benchmem -count=5 ./ledger
```

`make bench-ci` runs the curated benchmark set ten times and a separate
lock-contention sweep across several `GOMAXPROCS` values. It is intended for
benchmark comparisons and can take much longer than the regular suite.

## CPU and memory profiles

`make test-load-profile` builds Dingo, loads the checked-in immutable test data,
and writes `cpu.prof` and `mem.prof` in the repository root. It removes the
contents of `.dingo/` before loading; preserve any database data there before
running it.

Inspect the profiles with:

```sh
go tool pprof cpu.prof
go tool pprof mem.prof
```

## Timed chunk load

`make test-load-timing` runs `dingo load` against an immutable directory in a
fresh temporary working directory (so the database is new and `.dingo/` is not
touched) and prints the chunk count, block count, wall time, blocks per second
and peak RSS.

| Variable | Effect |
|----------|--------|
| `DINGO_LOAD_IMMUTABLE_DIR` | Immutable directory to load. Defaults to `database/immutable/testdata` (300 chunks). Point it at the `immutable/` directory of a Mithril snapshot to time larger sets. |
| `DINGO_LOAD_PROFILE` | When non-empty, also writes `cpu.prof` and `mem.prof` in the repository root. |
| `DINGO_LOAD_MAX_SECONDS` | When set, the run fails if the load takes longer. |
| `DINGO_LOAD_KEEP` | When non-empty, keeps the run directory and log. |

To time a larger set, take the first N chunks of a snapshot's `immutable/`
directory (each chunk is a `.chunk`, `.primary` and `.secondary` file) and load
them:

```sh
mkdir -p /scratch/immutable-3k
for n in $(seq -f '%05g' 0 2999); do
  ln -s /path/to/snapshot/immutable/$n.* /scratch/immutable-3k/
done
DINGO_LOAD_IMMUTABLE_DIR=/scratch/immutable-3k DINGO_LOAD_MAX_SECONDS=60 \
  make test-load-timing
```

Targets:

| Chunks | Target |
|--------|--------|
| 300 | under 5 seconds |
| 3,000 | under 1 minute |
| 24,000 | under 15 minutes |

Timings depend on the host, storage and build; compare runs from the same
host. See [timed load results](benchmarks/benchmark_results_load_timing.md)
for recorded runs.

## Recorded results

These reports describe specific runs and hardware; use them as historical
measurements, not as current performance guarantees.

- [Ledger and database results](benchmarks/benchmark_results.md)
- [Targeted comparison](benchmarks/benchmark_results_targeted.md)
- [Block producer sizing](benchmarks/benchmark_results_bp_pi.md)
- [Mithril API backfill](benchmarks/benchmark_results_api_backfill.md)
- [Timed chunk load](benchmarks/benchmark_results_load_timing.md)

`./generate_benchmarks.sh --write` updates the ledger and database report.
`./generate_bp_pi_report.sh` updates the block producer sizing report.
