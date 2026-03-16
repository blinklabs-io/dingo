# Four-Thread Block Producer Sizing Report (Non-Pi Host)

Assumptions:
- `storageMode=core`
- `runMode=serve`, `blob=badger`, `metadata=sqlite`
- `GOMAXPROCS=4`
- immutable fixture: `database/immutable/testdata`
- measurements were collected on non-Pi hardware and are a sizing proxy, not Raspberry Pi device benchmarks
- block producer peer count remains small; this is not a relay profile

## Load Footprint

| Profile | Elapsed | Peak RSS | Notes |
| --- | --- | --- | --- |
| `core-default` | `100.35s` | `568.1 MB` | Current default `serve/core` cache profile |

## Recommendation

For Raspberry Pi-class sizing in `storageMode=core`, the current default `serve/core` profile is already in the measured low-memory range on this four-thread non-Pi host.
Explicit Badger cache settings still override these defaults if an operator wants to tune further.
Treat this as a sizing baseline rather than a direct Raspberry Pi hardware measurement.

# Four-Thread Block Producer Sizing Benchmark Results (Non-Pi Host)

## Latest Results

### Test Environment
- **Date**: March 15, 2026
- **Go Version**: 1.25.8
- **OS**: Linux
- **Architecture**: aarch64
- **CPU Cores**: 128
- **Data Source**: database/immutable/testdata plus local immutable load runs
- **Scope**: Four-thread block producer path (GOMAXPROCS=4) in core mode on a non-Pi host

### Benchmark Results

All benchmarks run with `-benchmem` flag. Iterations are Go benchmark iteration counts; benchmark-specific throughput metrics (for example `blocks/sec`) are reported separately.

| Benchmark | Iterations | Time/op | Extra Metrics | Memory/op | Allocs/op |
|-----------|------------|---------|---------------|-----------|-----------|
| ledger:Blockfetch Near Tip Queued Header Predecoded | 155935 | 36391ns | 27479 blocks/sec | 2KB | 51 |
| ledger:Blockfetch Near Tip Throughput | 181371 | 66974ns | 14931 blocks/sec | 25KB | 128 |
| ledger:Blockfetch Near Tip Throughput Predecoded | 328588 | 36882ns | 27113 blocks/sec | 2KB | 50 |
| ledger:Verify Block Header/direct | 5946 | 1005264ns | - | 2KB | 29 |
| ledger:Verify Block Header/ledger_state | 6008 | 997998ns | - | 2KB | 29 |

## Performance Changes

No previous results found. This is the first benchmark run.
