# Four-Thread Block Producer Sizing Report (Non-Pi Host)

Assumptions:
- `storageMode=core`
- `runMode=serve`, `blob=badger`, `metadata=sqlite`
- `GOMAXPROCS=4`
- target peak RSS: `<= 1024 MB`
- immutable fixture: `database/immutable/testdata`
- measurements were collected on non-Pi hardware and are a sizing proxy, not Raspberry Pi device benchmarks
- block producer peer count remains small; this is not a relay profile

## Load Footprint

| Profile | Elapsed | Peak RSS | Notes |
| --- | --- | --- | --- |
| `core-default` | `99.00s` | `713.4 MB` | Current default `serve/core` cache profile |

## Recommendation

For Raspberry Pi-class sizing in `storageMode=core`, keep production defaults aligned with normal Badger behavior and use explicit memory/cache constraints only in this harness when validating low-memory targets.
This run stayed under the configured `1024 MB` RSS limit.
Explicit Badger cache settings still override these defaults if an operator wants to tune further.
Treat this as a sizing baseline rather than a direct Raspberry Pi hardware measurement.

# Four-Thread Block Producer Sizing Benchmark Results (Non-Pi Host)

## Latest Results

### Test Environment
- **Date**: June 12, 2026
- **Go Version**: 1.26.1
- **OS**: Linux
- **Architecture**: aarch64
- **CPU Cores**: 128
- **Data Source**: database/immutable/testdata plus local immutable load runs
- **Scope**: Four-thread block producer path (GOMAXPROCS=4) in core mode on a non-Pi host

### Benchmark Results

All benchmarks run with `-benchmem` flag. Iterations are Go benchmark iteration counts; benchmark-specific throughput metrics (for example `blocks/sec`) are reported separately.

| Benchmark | Iterations | Time/op | Extra Metrics | Memory/op | Allocs/op |
|-----------|------------|---------|---------------|-----------|-----------|
| ledger:Blockfetch Near Tip Flush Only Predecoded | 30390 | 40276ns | 24829 blocks/sec | 2KB | 59 |
| ledger:Blockfetch Near Tip Queued Header Predecoded | 1870012 | 616.8ns | 1621211 blocks/sec | 418B | 2 |
| ledger:Blockfetch Near Tip Throughput | 36349 | 35741ns | 27979 blocks/sec | 21KB | 74 |
| ledger:Blockfetch Near Tip Throughput Predecoded | 24396732 | 48.72ns | 20524367 blocks/sec | 0B | 0 |
| ledger:Verify Block Header/direct | 1189 | 997996ns | - | 2KB | 28 |
| ledger:Verify Block Header/ledger_state | 1147 | 1032909ns | - | 2KB | 30 |

## Performance Changes

Changes since **March 15, 2026**:

### Load Footprint

| Metric | Previous | Current | Change |
|--------|----------|---------|--------|
| Profile | `core-default` | `core-default` | - |
| Elapsed | `100.35s` | `99.00s` | `-1.35s (-1.3%)` |
| Peak RSS | `568.1 MB` | `713.4 MB` | `+145.3 MB (+25.6%)` |

### Summary
- **Faster benchmarks**: 2
- **Slower benchmarks**: 3
- **New benchmarks**: 1
- **Removed benchmarks**: 0

### Top Improvements
- ledger:Blockfetch Near Tip Throughput Predecoded (+7324%)
- ledger:Blockfetch Near Tip Queued Header Predecoded (+1099%)

### Performance Regressions
- ledger:Verify Block Header/ledger_state (-80%)
- ledger:Verify Block Header/direct (-80%)
- ledger:Blockfetch Near Tip Throughput (-79%)

### New Benchmarks Added
- ledger:Blockfetch Near Tip Flush Only Predecoded

## Historical Results

### March 15, 2026

| Benchmark | Iterations | Time/op | Extra Metrics | Memory/op | Allocs/op |
|-----------|------------|---------|---------------|-----------|-----------|
| ledger:Blockfetch Near Tip Queued Header Predecoded | 155935 | 36391ns | 27479 blocks/sec | 2KB | 51 |
| ledger:Blockfetch Near Tip Throughput | 181371 | 66974ns | 14931 blocks/sec | 25KB | 128 |
| ledger:Blockfetch Near Tip Throughput Predecoded | 328588 | 36882ns | 27113 blocks/sec | 2KB | 50 |
| ledger:Verify Block Header/direct | 5946 | 1005264ns | - | 2KB | 29 |
| ledger:Verify Block Header/ledger_state | 6008 | 997998ns | - | 2KB | 29 |
