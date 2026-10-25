# Benchmarks and profiling

Run benchmark commands from the repository root. The Makefile targets include
the default `dingo_extra_plugins` build tag. Go test benchmark targets report
memory allocations; `make bench-leios-db` prints latency percentiles only.

## Benchmarks

```sh
make bench
make bench-leios-db
make bench-leios-db-cached
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

`make bench-leios-db` measures the latency of Dingo's LeiosDB storage steps,
mirroring the
[upstream LeiosDB latency benchmark](https://github.com/IntersectMBO/ouroboros-consensus/pull/2424).
It tracks 100 measured EBs, one at a time, over a database of 1000 EBs of 2857
transactions of 350 bytes each. Each EB is fetched from 20 upstream peers,
read once for voting, read by 100 downstream peers, and read once more for
block application. The command prints the p50, p99, maximum, and count for each
step. `-burst` adds background bursts of 10 EBs every 5 seconds that are
fetched, voted on, and served alongside the measured EBs, and `-h` lists the
flags that change the workload. Set `GOMAXPROCS` and `TMPDIR` to compare
against the reference's `-N` setting and storage device.

`make bench-leios-db-cached` (or `-cached-reads` on the command) shares the first
persisted manifest and transaction-closure read for each EB across its readers. Transaction requests
copy only their selected transactions out of the shared closure, using the
same packed response-copy routine as the Leios-fetch responder, and vote and
apply reads copy the full closure. The first load and concurrent waiters remain
timed. Measured EB caches are released after apply; background caches live with
their in-flight work. This models good read-through-cache conditions without
using the generated input as the read result. It does not exercise the network
protocol's cache admission, TTL, eviction or wire encoding. Report it separately
from the default cache-bypassing database workload, including with `-burst`.
Both Make targets forward `LEIOS_DB_BENCH_ARGS`, for example
`make bench-leios-db-cached LEIOS_DB_BENCH_ARGS=-burst`.

Use `-cpuprofile=<file>` for a diagnostic run to profile the workload after
database prepopulation. Keep profile runs separate from comparison timings.
Use `-heapprofile=<file>` to sample runtime memory through prepopulation and
the workload and write a final heap profile without forcing a collection.
Its runtime memory counters exclude Badger's mmap-backed files and other
non-Go allocations; compare them with process RSS in a diagnostic run.

Dingo persists Leios EBs in its Badger blob store, whereas the reference uses
SQLite, so the steps map as follows:

- Fetch: the reference writes each transaction chunk as it arrives. Dingo
  assembles the chunks in memory (`Chunks`) and commits the EB once with
  `SetLeiosEB` (`Commit`).
- Serve: each downstream peer reads the manifest (`Body`), then issues the
  transaction requests (`Txs request`). Dingo's transaction read loads the
  stored list before selecting the requested offsets. Dingo normally serves
  cached EBs from memory, so this measures the database reload path.
- Vote and apply reads load the transaction list.
- Pruning with `PruneLeiosEBBeforeSlot` every 10 fetched EBs keeps the stored
  window near 1000 EBs, up to 9 more between prunes. The prune cutoff never
  passes an EB whose readers are still running.

Dingo manifest references contain the Blake2b-256 hash and serialized size of
each CBOR transaction, as required by Dingo's Leios fetch validation, so the
transaction fixtures differ from the reference's synthetic ones.

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
- [Mithril Core sync](benchmarks/benchmark_results_mithril_core.md)
- [Mithril API backfill](benchmarks/benchmark_results_api_backfill.md)

`./generate_benchmarks.sh --write` updates the ledger and database report.
`./generate_bp_pi_report.sh` updates the block producer sizing report.
