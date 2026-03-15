# Targeted Runtime Baseline

## Latest Results

### Test Environment
- **Date**: March 14, 2026
- **Go Version**: 1.25.8
- **OS**: Linux
- **Architecture**: aarch64
- **CPU Cores**: 128
- **Data Source**: database/immutable/testdata
- **Scope**: BP runtime path

### Benchmark Results

All benchmarks run with `-benchmem` flag. Iterations are Go benchmark iteration counts; benchmark-specific throughput metrics (for example `blocks/sec`) are reported separately.

| Benchmark | Iterations | Time/op | Extra Metrics | Memory/op | Allocs/op |
|-----------|------------|---------|---------------|-----------|-----------|
| ledger:Block Processing Throughput | 13000 | 96796ns | 10331 blocks/sec | 25KB | 133 |
| ledger:Block Processing Throughput Predecoded | 27357 | 41209ns | 24267 blocks/sec | 2KB | 52 |
| ledger:Blockfetch Near Tip Queued Header Predecoded | 29880 | 39553ns | 25283 blocks/sec | 2KB | 52 |
| ledger:Blockfetch Near Tip Throughput | 14515 | 86009ns | 11627 blocks/sec | 25KB | 128 |
| ledger:Blockfetch Near Tip Throughput Predecoded | 33068 | 38485ns | 25984 blocks/sec | 2KB | 50 |
| ledger:Blockfetch Verified Header Dispatch | 31767780 | 37.72ns | - | 0B | 0 |
| ledger:Storage Mode Ingest/api | 3 | 337822776ns | 532.8 blocks_ingested/sec, 592.0 txs_ingested/sec | 15884KB | 192682 |
| ledger:Storage Mode Ingest/core | 5 | 240989770ns | 746.9 blocks_ingested/sec, 829.9 txs_ingested/sec | 7550KB | 100660 |
| ledger:Verify Block Header/direct | 1122 | 1047255ns | - | 2KB | 29 |
| ledger:Verify Block Header/ledger_state | 1122 | 1047068ns | - | 2KB | 29 |
## Performance Changes

Changes since **March 13, 2026**:

### Summary
- **Faster benchmarks**: 6
- **Slower benchmarks**: 2
- **New benchmarks**: 0
- **Removed benchmarks**: 0

### Top Improvements
- ledger:Verify Block Header/ledger_state (+1%)
- ledger:Blockfetch Verified Header Dispatch (+21%)
- ledger:Blockfetch Near Tip Throughput Predecoded (+8%)
- ledger:Blockfetch Near Tip Throughput (+2%)
- ledger:Block Processing Throughput Predecoded (+0%)

### Performance Regressions
- ledger:Blockfetch Near Tip Queued Header Predecoded (-1%)
- ledger:Verify Block Header/direct (-2%)


## Historical Results

### March 13, 2026

| Benchmark | Iterations | Time/op | Extra Metrics | Memory/op | Allocs/op |
|-----------|------------|---------|---------------|-----------|-----------|
| ledger:Block Processing Throughput | 11126 | 108824ns | 9189 blocks/sec | 25KB | 133 |
| ledger:Block Processing Throughput Predecoded | 27195 | 41884ns | 23876 blocks/sec | 2KB | 52 |
| ledger:Blockfetch Near Tip Queued Header Predecoded | 30206 | 38792ns | 25779 blocks/sec | 2KB | 51 |
| ledger:Blockfetch Near Tip Throughput | 14179 | 88029ns | 11360 blocks/sec | 25KB | 128 |
| ledger:Blockfetch Near Tip Throughput Predecoded | 30481 | 39802ns | 25125 blocks/sec | 2KB | 50 |
| ledger:Blockfetch Verified Header Dispatch | 26182030 | 45.85ns | - | 0B | 0 |
| ledger:Storage Mode Ingest/api | 3 | 367883050ns | 489.3 blocks_ingested/sec, 543.7 txs_ingested/sec | 15919KB | 192784 |
| ledger:Storage Mode Ingest/core | 5 | 204910867ns | 878.4 blocks_ingested/sec, 976.0 txs_ingested/sec | 7505KB | 100573 |
| ledger:Verify Block Header/direct | 1154 | 1028815ns | - | 2KB | 29 |
| ledger:Verify Block Header/ledger_state | 1110 | 1052555ns | - | 2KB | 29 |
