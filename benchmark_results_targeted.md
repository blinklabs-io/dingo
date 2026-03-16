# Targeted Runtime Baseline

## Latest Results

### Test Environment
- **Date**: March 15, 2026
- **Go Version**: 1.25.8
- **OS**: Linux
- **Architecture**: aarch64
- **CPU Cores**: 128
- **Data Source**: database/immutable/testdata plus synthetic relay fanout/control-loop benchmarks
- **Scope**: BP and relay runtime paths

### Benchmark Results

All benchmarks run with `-benchmem` flag. Iterations are Go benchmark iteration counts; benchmark-specific throughput metrics (for example `blocks/sec`) are reported separately.

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
| ledger:Blockfetch Near Tip Throughput | 172838 | 69818ns | 14323 blocks/sec | 25KB | 129 |
| ledger:Blockfetch Near Tip Flush Only Predecoded | 342222 | 37234ns | 26857 blocks/sec | 2KB | 50 |
| ledger:Blockfetch Near Tip Throughput Predecoded | 340428 | 36428ns | 27452 blocks/sec | 2KB | 50 |
| ledger:Blockfetch Verified Header Dispatch | 156298824 | 38.37ns | - | 0B | 0 |
| ledger:Verify Block Header/direct | 5907 | 1008310ns | - | 2KB | 29 |
| ledger:Verify Block Header/ledger_state | 5935 | 1006660ns | - | 2KB | 29 |
| peergov:Reconcile/100 | 64309 | 93859ns | - | 19KB | 124 |
| peergov:Reconcile/1000 | 3626 | 1632793ns | - | 250KB | 1597 |
| peergov:Reconcile/500 | 7881 | 733098ns | - | 124KB | 697 |

## Performance Changes

Changes relative to the earlier **March 15, 2026** baseline below:

### Summary
- Focused timed reruns were used for the connmanager and peergov rows in this update.
- `Update Connection Metrics` now uses incremental cached counts instead of rescanning all connections.
- The default `serve/core` Badger profile now uses the previously measured Pi-tight cache sizing.
- `peergov.reconcile` still benefits heavily from keeping per-peer churn logs below `Info`.
- `peergov.reconcile` now reuses one reconcile timestamp and caches under-valency for warm-promotion candidates.
- `peergov.enforceStateLimit` now sorts only removable peers and rebuilds the slice once instead of repeatedly deleting from it.
- `peergov.reconcile` now skips valency-status scans entirely unless debug logging is enabled.

### Top Improvements
- connmanager:Update Connection Metrics/1000: `291625ns -> 30.09ns`
- connmanager:Update Connection Metrics/500: `138551ns -> 29.69ns`
- connmanager:Update Connection Metrics/100: `24518ns -> 29.82ns`
- connmanager:Update Connection Metrics/10: `3123ns -> 29.24ns`
- ledger:Blockfetch Near Tip Throughput: `86320ns -> 69818ns`
- ledger:Blockfetch Near Tip Throughput Predecoded: `40031ns -> 36428ns`
- peergov:Reconcile/1000: `2587803ns -> 1632793ns`
- peergov:Reconcile/500: `1072577ns -> 733098ns`
- peergov:Reconcile/100: `130667ns -> 93859ns`


## Historical Results

### Earlier March 15, 2026 Baseline

| Benchmark | Iterations | Time/op | Extra Metrics | Memory/op | Allocs/op |
|-----------|------------|---------|---------------|-----------|-----------|
| connmanager:Has Inbound From Host/10 | 102497121 | 63.18ns | - | 0B | 0 |
| connmanager:Has Inbound From Host/100 | 93197125 | 57.95ns | - | 0B | 0 |
| connmanager:Has Inbound From Host/1000 | 99058254 | 60.09ns | - | 0B | 0 |
| connmanager:Has Inbound From Host/500 | 91016371 | 65.75ns | - | 0B | 0 |
| connmanager:Try Reserve Inbound Slot Parallel/10 | 15616777 | 403.8ns | - | 0B | 0 |
| connmanager:Try Reserve Inbound Slot Parallel/100 | 7449546 | 779.0ns | - | 0B | 0 |
| connmanager:Try Reserve Inbound Slot Parallel/500 | 7706102 | 772.6ns | - | 0B | 0 |
| connmanager:Update Connection Metrics/10 | 1876092 | 3123ns | - | 456B | 3 |
| connmanager:Update Connection Metrics/100 | 239965 | 24518ns | - | 3KB | 3 |
| connmanager:Update Connection Metrics/1000 | 20446 | 291625ns | - | 54KB | 5 |
| connmanager:Update Connection Metrics/500 | 43100 | 138551ns | - | 27KB | 3 |
| event:Publish Subscribers/1 | 7974799 | 650.6ns | - | 0B | 0 |
| event:Publish Subscribers/10 | 746738 | 7520ns | - | 0B | 0 |
| event:Publish Subscribers/100 | 61120 | 94657ns | - | 0B | 0 |
| event:Publish Subscribers/500 | 12976 | 489401ns | - | 0B | 0 |
| ledger:Block Processing Throughput | 54142 | 95123ns | 10513 blocks/sec | 25KB | 133 |
| ledger:Block Processing Throughput Predecoded | 144867 | 40441ns | 24728 blocks/sec | 2KB | 51 |
| ledger:Blockfetch Near Tip Throughput | 76536 | 86320ns | 11585 blocks/sec | 25KB | 128 |
| ledger:Blockfetch Near Tip Throughput Predecoded | 160242 | 40031ns | 24981 blocks/sec | 2KB | 50 |
| ledger:Blockfetch Verified Header Dispatch | 156298824 | 38.37ns | - | 0B | 0 |
| ledger:Verify Block Header/direct | 5907 | 1008310ns | - | 2KB | 29 |
| ledger:Verify Block Header/ledger_state | 5935 | 1006660ns | - | 2KB | 29 |
| peergov:Reconcile/100 | 45027 | 130667ns | - | 16KB | 185 |
| peergov:Reconcile/1000 | 2343 | 2587803ns | - | 202KB | 1662 |
| peergov:Reconcile/500 | 5739 | 1072577ns | - | 100KB | 760 |
