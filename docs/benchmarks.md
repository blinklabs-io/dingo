# Benchmarks and profiling

Run benchmark commands from the repository root. The Makefile targets include
the default `dingo_extra_plugins` build tag. Go test benchmark targets report
memory allocations; `make bench-leios-db` prints workload timings only.

## Benchmarks

```sh
make bench
make bench-leios-db
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

`make bench-leios-db` runs the concurrent LeiosDB workload corresponding to the
[upstream LeiosDB benchmark entrypoint](https://github.com/IntersectMBO/ouroboros-consensus/blob/7ee63a2240320189545c43519efc6542ea35745c/ouroboros-consensus/bench/leios-db-bench/Main.hs).
Its defaults match the reference workload: 500 preloaded EBs with 200
transactions each, three concurrent writers and readers, 50 closure reads,
one warmup, and five timed iterations. The command accepts flags for adjusting
the workload counts and transaction size and prints the minimum, average, and
maximum iteration time.

Dingo transaction fixtures retain the reference's 16 KiB body size. Dingo
manifest references contain the Blake2b-256 hash and serialized size of each
CBOR transaction, as required by Dingo's Leios fetch validation. The reference
fixture instead uses synthetic hashes and records 200 bytes per manifest
reference, so those manifest fields differ between the two workloads.

The logical workload is aligned, while the storage implementations differ:
Dingo persists Leios EBs in its Badger blob store, whereas the reference
entrypoint uses SQLite. Dingo writes the manifest and complete CBOR transaction
list in one Badger blob transaction, and its transaction-read API loads the
stored list before selecting the requested offsets. The reference benchmark's
current GC call is a no-op; Dingo uses the matching no-op tick while Badger's
periodic value-log collection remains active.

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
