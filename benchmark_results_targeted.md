# Dingo Targeted Performance Baseline

## Latest Results

### Test Environment
- **Date**: March 10, 2026
- **Go Version**: 1.25.8
- **OS**: Linux
- **Architecture**: aarch64
- **CPU Cores**: 128
- **Data Source**: Real immutable testdata blocks from database/immutable/testdata
- **Scope**: Current optimization baseline for core/api ingest, block processing, and batched load paths

### Benchmark Results

All benchmarks run with `-benchmem` flag. Iterations are Go benchmark iteration counts; benchmark-specific throughput metrics (for example `blocks/sec`) are reported separately.

| Benchmark | Iterations | Time/op | Extra Metrics | Memory/op | Allocs/op |
|-----------|------------|---------|---------------|-----------|-----------|
| Block Batch Processing Throughput | 3 | 592689ns | 84361 blocks/sec | 91KB | 2088 |
| Block Processing Throughput | 3 | 214857ns | 4654 blocks/sec | 26KB | 137 |
| Block Processing Throughput Predecoded | 3 | 60121ns | 16633 blocks/sec | 2KB | 58 |
| Raw Block Batch Processing Throughput | 3 | 515408ns | 97011 blocks/sec | 81KB | 1938 |
| Storage Mode Ingest Steady State/api | 3 | 375174723ns | 479.8 blocks_ingested/sec, 533.1 txs_ingested/sec | 15761KB | 186666 |
| Storage Mode Ingest Steady State/core | 3 | 152858484ns | 1178 blocks_ingested/sec, 1308 txs_ingested/sec | 7234KB | 94403 |
| Storage Mode Ingest/api | 3 | 362809147ns | 496.1 blocks_ingested/sec, 551.3 txs_ingested/sec | 15935KB | 193665 |
| Storage Mode Ingest/core | 3 | 235819272ns | 763.3 blocks_ingested/sec, 848.1 txs_ingested/sec | 7514KB | 101522 |
## Performance Changes

Changes since **March 10, 2026** (initial baseline):

### Summary
- **Faster benchmarks**: 4
  - Block Batch Processing Throughput (+32%)
  - Block Processing Throughput Predecoded (+91%)
  - Storage Mode Ingest Steady State/api (+8%)
  - Storage Mode Ingest Steady State/core (+55%)
- **Slower benchmarks**: 4
  - Block Processing Throughput (-56%)
  - Raw Block Batch Processing Throughput (-17%)
  - Storage Mode Ingest/api (-18%)
  - Storage Mode Ingest/core (-37%)
- **New benchmarks**: 0
- **Removed benchmarks**: 0

> **Note**: These are two runs from the same session establishing the baseline.
> Variance is expected; treat this as a single baseline snapshot, not a regression.


## Historical Results

### March 10, 2026

| Benchmark | Iterations | Time/op | Extra Metrics | Memory/op | Allocs/op |
|-----------|------------|---------|---------------|-----------|-----------|
| Block Batch Processing Throughput | 3 | 781159ns | 64007 blocks/sec | 99KB | 2138 |
| Block Processing Throughput | 3 | 93495ns | 10696 blocks/sec | 26KB | 138 |
| Block Processing Throughput Predecoded | 3 | 114562ns | 8729 blocks/sec | 2KB | 60 |
| Raw Block Batch Processing Throughput | 3 | 427847ns | 116864 blocks/sec | 81KB | 1938 |
| Storage Mode Ingest Steady State/api | 3 | 406074114ns | 443.3 blocks_ingested/sec, 492.5 txs_ingested/sec | 15743KB | 186725 |
| Storage Mode Ingest Steady State/core | 3 | 236358805ns | 761.6 blocks_ingested/sec, 846.2 txs_ingested/sec | 7245KB | 94503 |
| Storage Mode Ingest/api | 3 | 298992974ns | 602.0 blocks_ingested/sec, 668.9 txs_ingested/sec | 15861KB | 193541 |
| Storage Mode Ingest/core | 3 | 148714628ns | 1210 blocks_ingested/sec, 1345 txs_ingested/sec | 7449KB | 101364 |
