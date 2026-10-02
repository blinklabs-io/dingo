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

## Recorded results

These reports describe specific runs and hardware; use them as historical
measurements, not as current performance guarantees.

- [Ledger and database results](benchmarks/benchmark_results.md)
- [Targeted comparison](benchmarks/benchmark_results_targeted.md)
- [Block producer sizing](benchmarks/benchmark_results_bp_pi.md)
- [Mithril API backfill](benchmarks/benchmark_results_api_backfill.md)

`./generate_benchmarks.sh --write` updates the ledger and database report.
`./generate_bp_pi_report.sh` updates the block producer sizing report.
