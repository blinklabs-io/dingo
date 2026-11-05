# Targeted Runtime Baseline

## Latest Results

### Test Environment
- **Date**: June 12, 2026
- **Go Version**: 1.26.1
- **OS**: Linux
- **Architecture**: aarch64
- **CPU Cores**: 128
- **Data Source**: database/immutable/testdata plus synthetic relay fanout/control-loop benchmarks
- **Scope**: BP and relay runtime paths

### Benchmark Results

All benchmarks run with `-benchmem` flag. Iterations are Go benchmark iteration counts; benchmark-specific throughput metrics (for example `blocks/sec`) are reported separately.

| Benchmark | Iterations | Time/op | Extra Metrics | Memory/op | Allocs/op |
|-----------|------------|---------|---------------|-----------|-----------|
| connmanager:Has Inbound Peer Address/10 | 1806908 | 592.3ns | - | 40B | 2 |
| connmanager:Has Inbound Peer Address/100 | 1805614 | 665.6ns | - | 40B | 2 |
| connmanager:Has Inbound Peer Address/1000 | 1757652 | 647.8ns | - | 40B | 2 |
| connmanager:Has Inbound Peer Address/500 | 1759791 | 656.7ns | - | 40B | 2 |
| connmanager:Try Reserve Inbound Slot Parallel/10 | 1321866 | 787.9ns | - | 0B | 0 |
| connmanager:Try Reserve Inbound Slot Parallel/100 | 1488516 | 869.2ns | - | 0B | 0 |
| connmanager:Try Reserve Inbound Slot Parallel/500 | 1299157 | 796.1ns | - | 0B | 0 |
| connmanager:Update Connection Metrics/10 | 40943979 | 30.85ns | - | 0B | 0 |
| connmanager:Update Connection Metrics/100 | 40637336 | 28.86ns | - | 0B | 0 |
| connmanager:Update Connection Metrics/1000 | 33163863 | 30.67ns | - | 0B | 0 |
| connmanager:Update Connection Metrics/500 | 37643042 | 30.34ns | - | 0B | 0 |
| event:Publish Subscribers/1 | 5566287 | 206.5ns | - | 0B | 0 |
| event:Publish Subscribers/10 | 1576297 | 794.1ns | - | 0B | 0 |
| event:Publish Subscribers/100 | 125047 | 8942ns | - | 0B | 0 |
| event:Publish Subscribers/500 | 15193 | 79365ns | - | 0B | 0 |
| ledger:Block Processing Throughput | 10000 | 113423ns | 8817 blocks/sec | 24KB | 136 |
| ledger:Block Processing Throughput Predecoded | 25467 | 42734ns | 23401 blocks/sec | 2KB | 61 |
| ledger:Blockfetch Near Tip Flush Only Predecoded | 32299 | 39575ns | 25269 blocks/sec | 2KB | 59 |
| ledger:Blockfetch Near Tip Queued Header Predecoded | 1883409 | 599.9ns | 1666820 blocks/sec | 418B | 2 |
| ledger:Blockfetch Near Tip Throughput | 32918 | 31718ns | 31528 blocks/sec | 21KB | 74 |
| ledger:Blockfetch Near Tip Throughput Predecoded | 22904923 | 48.08ns | 20799215 blocks/sec | 0B | 0 |
| ledger:Blockfetch Verified Header Dispatch | 178266688 | 6.724ns | - | 0B | 0 |
| ledger:Verify Block Header/direct | 1147 | 1032274ns | - | 2KB | 28 |
| ledger:Verify Block Header/ledger_state | 1149 | 1031635ns | - | 2KB | 30 |
| peergov:Reconcile/100 | 9492 | 136118ns | - | 21KB | 166 |
| peergov:Reconcile/1000 | 526 | 2275401ns | - | 281KB | 2199 |
| peergov:Reconcile/500 | 1232 | 975706ns | - | 132KB | 991 |

## Performance Changes

Changes since **March 15, 2026**:

### Summary
- **Faster benchmarks**: 5
- **Slower benchmarks**: 17
- **New benchmarks**: 5
- **Removed benchmarks**: 4

### Top Improvements
- ledger:Blockfetch Verified Header Dispatch (+82.48%)
- ledger:Blockfetch Near Tip Throughput Predecoded (+75,665.75%)
- event:Publish Subscribers/500 (+17%)
- event:Publish Subscribers/100 (+104%)
- event:Publish Subscribers/10 (+111%)

### Performance Regressions
- event:Publish Subscribers/1 (-30%)
- connmanager:Try Reserve Inbound Slot Parallel/100 (-78%)
- connmanager:Update Connection Metrics/10 (-79%)
- ledger:Verify Block Header/ledger_state (-80%)
- ledger:Verify Block Header/direct (-80%)

### New Benchmarks Added
- connmanager:Has Inbound Peer Address/10
- connmanager:Has Inbound Peer Address/100
- connmanager:Has Inbound Peer Address/1000
- connmanager:Has Inbound Peer Address/500
- ledger:Blockfetch Near Tip Queued Header Predecoded

### Benchmarks Removed
- connmanager:Has Inbound From Host/10
- connmanager:Has Inbound From Host/100
- connmanager:Has Inbound From Host/1000
- connmanager:Has Inbound From Host/500


## Historical Results

### March 15, 2026

| Benchmark | Iterations | Time/op | Extra Metrics | Memory/op | Allocs/op |
|-----------|------------|---------|---------------|-----------|-----------|
| connmanager:Has Inbound From Host/10 | 81306320 | 62.31ns | - | 0B | 0 |
| connmanager:Has Inbound From Host/100 | 94509072 | 70.91ns | - | 0B | 0 |
| connmanager:Has Inbound From Host/1000 | 92533866 | 64.39ns | - | 0B | 0 |
| connmanager:Has Inbound From Host/500 | 99476421 | 59.57ns | - | 0B | 0 |
| connmanager:Try Reserve Inbound Slot Parallel/10 | 15721854 | 388.8ns | - | 0B | 0 |
| connmanager:Try Reserve Inbound Slot Parallel/100 | 7024470 | 823.6ns | - | 0B | 0 |
| connmanager:Try Reserve Inbound Slot Parallel/500 | 7173524 | 840.6ns | - | 0B | 0 |
| connmanager:Update Connection Metrics/10 | 199656918 | 29.24ns | - | 0B | 0 |
| connmanager:Update Connection Metrics/100 | 203453912 | 29.82ns | - | 0B | 0 |
| connmanager:Update Connection Metrics/1000 | 203063875 | 30.09ns | - | 0B | 0 |
| connmanager:Update Connection Metrics/500 | 204545601 | 29.69ns | - | 0B | 0 |
| event:Publish Subscribers/1 | 7974799 | 650.6ns | - | 0B | 0 |
| event:Publish Subscribers/10 | 746738 | 7520ns | - | 0B | 0 |
| event:Publish Subscribers/100 | 61120 | 94657ns | - | 0B | 0 |
| event:Publish Subscribers/500 | 12976 | 489401ns | - | 0B | 0 |
| ledger:Block Processing Throughput | 54142 | 95123ns | 10513 blocks/sec | 25KB | 133 |
| ledger:Block Processing Throughput Predecoded | 144867 | 40441ns | 24728 blocks/sec | 2KB | 51 |
| ledger:Blockfetch Near Tip Flush Only Predecoded | 342222 | 37234ns | 26857 blocks/sec | 2KB | 50 |
| ledger:Blockfetch Near Tip Throughput | 172838 | 69818ns | 14323 blocks/sec | 25KB | 129 |
| ledger:Blockfetch Near Tip Throughput Predecoded | 340428 | 36428ns | 27452 blocks/sec | 2KB | 50 |
| ledger:Blockfetch Verified Header Dispatch | 156298824 | 38.37ns | - | 0B | 0 |
| ledger:Verify Block Header/direct | 5907 | 1008310ns | - | 2KB | 29 |
| ledger:Verify Block Header/ledger_state | 5935 | 1006660ns | - | 2KB | 29 |
| peergov:Reconcile/100 | 64309 | 93859ns | - | 19KB | 124 |
| peergov:Reconcile/1000 | 3626 | 1632793ns | - | 250KB | 1597 |
| peergov:Reconcile/500 | 7881 | 733098ns | - | 124KB | 697 |
