# Dingo Ledger & Database Benchmark Results

## Latest Results

### Test Environment
- **Date**: November 26, 2025
- **Go Version**: 1.24.1
- **OS**: Linux
- **Architecture**: aarch64
- **CPU Cores**: 128
- **Data Source**: Real Cardano preview testnet data (40k+ blocks, slots 0-863,996)

### Benchmark Results

All benchmarks run with `-benchmem` flag showing memory allocations and operation counts.

| Benchmark | Operations/sec | Time/op | Memory/op | Allocs/op |
|-----------|----------------|---------|-----------|-----------|
| Pool Lookup By Key Hash No Data | 36231 | 33604ns | 4KB | 79 |
| Pool Registration Lookups No Data | 24210 | 46595ns | 10KB | 93 |
| Account Lookup By Stake Key Real Data | 39950 | 34026ns | 4KB | 75 |
| Utxo Lookup By Address No Data | 109825 | 16460ns | 2KB | 19 |
| Storage Backends/memory | 32593 | 34818ns | 13KB | 70 |
| Transaction Validation | 230958193 | 5.238ns | 0B | 0 |
| Real Block Reading | 22 | 53427289ns | 2183KB | 74472 |
| Block Retrieval By Index Real Data | 303981 | 3868ns | 472B | 11 |
| Index Building Time | 14078 | 87080ns | 17KB | 119 |
| Chain Sync From Genesis | 74 | 15096067ns | 100.0blocks_processed | 2247880 |
| Block Retrieval By Index No Data | 315110 | 3652ns | 472B | 11 |
| Transaction Create | 77013 | 16824ns | 2KB | 18 |
| Utxo Lookup By Address Real Data | 82125 | 16014ns | 2KB | 19 |
| Era Transition Performance | 459890545 | 2.178ns | 0B | 0 |
| Protocol Parameters Lookup By Epoch Real Data | 44181 | 32903ns | 5KB | 62 |
| Pool Registration Lookups Real Data | 22616 | 48729ns | 10KB | 93 |
| Stake Registration Lookups Real Data | 38761 | 33724ns | 5KB | 69 |
| Era Transition Performance Real Data | 4340 | 267246ns | 83KB | 490 |
| Real Block Processing | 13651 | 84993ns | 17KB | 119 |
| Utxo Lookup By Ref No Data | 10461 | 119506ns | 8KB | 131 |
| Storage Backends/disk | 33004 | 32561ns | 13KB | 70 |
| Test Load/memory | 1912 | 650898ns | 260KB | 1400 |
| Stake Registration Lookups No Data | 33963 | 33887ns | 5KB | 69 |
| Utxo Lookup By Ref Real Data | 10000 | 122404ns | 8KB | 131 |
| Protocol Parameters Lookup By Epoch No Data | 37996 | 32298ns | 5KB | 62 |
| Datum Lookup By Hash No Data | 32635 | 34072ns | 4KB | 69 |
| Block Processing Throughput | 3904 | 290370ns | 3444blocks/sec | 22462 |
| Real Data Queries | 68570 | 17752ns | 5KB | 43 |
| Block Nonce Lookup Real Data | 34851 | 37815ns | 4KB | 73 |
| Test Load/disk | 2068 | 536686ns | 260KB | 1400 |
| Pool Lookup By Key Hash Real Data | 40214 | 33631ns | 4KB | 79 |
| D Rep Lookup By Key Hash No Data | 35266 | 36094ns | 4KB | 77 |
| Transaction History Queries No Data | 34167 | 33889ns | 4KB | 78 |
| Datum Lookup By Hash Real Data | 44810 | 33686ns | 4KB | 69 |
| Block Nonce Lookup No Data | 32011 | 36515ns | 4KB | 73 |
| Account Lookup By Stake Key No Data | 34898 | 35142ns | 4KB | 75 |
| Transaction History Queries Real Data | 37575 | 34812ns | 4KB | 78 |
| Concurrent Queries | 15408 | 68781ns | 14581queries/sec | 3943 |
| D Rep Lookup By Key Hash Real Data | 39360 | 34907ns | 4KB | 77 |
| Block Memory Usage | 29292 | 44369ns | 14KB | 49 |

## Performance Changes

Changes since **November 26, 2025**:

### Summary
- **Faster benchmarks**: 19
- **Slower benchmarks**: 20
- **New benchmarks**: 0
- **Removed benchmarks**: 0

### Top Improvements
- Utxo Lookup By Address No Data (+44%)
- Transaction Validation (+0%)
- Test Load/memory (+19%)
- Test Load/disk (+31%)
- Storage Backends/memory (+3%)

### Performance Regressions
- Pool Lookup By Key Hash No Data (-0%)
- Index Building Time (-0%)
- Account Lookup By Stake Key No Data (-0%)
- Transaction History Queries No Data (-1%)
- Stake Registration Lookups No Data (-1%)


## Historical Results

### November 26, 2025

| Benchmark | Operations/sec | Time/op | Memory/op | Allocs/op |
|-----------|----------------|---------|-----------|-----------|
| Pool Lookup By Key Hash No Data | 36328 | 33556ns | 4KB | 79 |
| Pool Registration Lookups No Data | 24898 | 51298ns | 10KB | 93 |
| Account Lookup By Stake Key Real Data | 39936 | 33871ns | 4KB | 75 |
| Utxo Lookup By Address No Data | 75966 | 16953ns | 2KB | 19 |
| Storage Backends/memory | 31369 | 33394ns | 13KB | 70 |
| Transaction Validation | 230637091 | 5.213ns | 0B | 0 |
| Real Block Reading | 20 | 55046083ns | 2183KB | 74472 |
| Block Retrieval By Index Real Data | 312492 | 4102ns | 472B | 11 |
| Index Building Time | 14168 | 86378ns | 17KB | 119 |
| Chain Sync From Genesis | 100 | 14189212ns | 100.0blocks_processed | 2247966 |
| Block Retrieval By Index No Data | 277410 | 4025ns | 472B | 11 |
| Transaction Create | 92761 | 16456ns | 2KB | 18 |
| Utxo Lookup By Address Real Data | 86467 | 15705ns | 2KB | 19 |
| Era Transition Performance | 499031763 | 2.139ns | 0B | 0 |
| Protocol Parameters Lookup By Epoch Real Data | 44150 | 30066ns | 5KB | 62 |
| Pool Registration Lookups Real Data | 23724 | 48201ns | 10KB | 93 |
| Stake Registration Lookups Real Data | 40082 | 32844ns | 5KB | 69 |
| Era Transition Performance Real Data | 3525 | 305364ns | 83KB | 490 |
| Real Block Processing | 13509 | 87846ns | 17KB | 119 |
| Utxo Lookup By Ref No Data | 10724 | 111792ns | 8KB | 131 |
| Storage Backends/disk | 28960 | 37643ns | 13KB | 70 |
| Test Load/memory | 1605 | 669272ns | 260KB | 1400 |
| Stake Registration Lookups No Data | 34359 | 36417ns | 5KB | 69 |
| Utxo Lookup By Ref Real Data | 10000 | 111756ns | 8KB | 131 |
| Protocol Parameters Lookup By Epoch No Data | 37886 | 32905ns | 5KB | 62 |
| Datum Lookup By Hash No Data | 40302 | 29076ns | 4KB | 69 |
| Block Processing Throughput | 4632 | 287053ns | 3484blocks/sec | 22466 |
| Real Data Queries | 62532 | 19011ns | 5KB | 43 |
| Block Nonce Lookup Real Data | 35782 | 38514ns | 4KB | 73 |
| Test Load/disk | 1574 | 711525ns | 260KB | 1400 |
| Pool Lookup By Key Hash Real Data | 39812 | 33599ns | 4KB | 79 |
| D Rep Lookup By Key Hash No Data | 35139 | 35088ns | 4KB | 77 |
| Transaction History Queries No Data | 34530 | 36887ns | 4KB | 78 |
| Datum Lookup By Hash Real Data | 36972 | 31369ns | 4KB | 69 |
| Block Nonce Lookup No Data | 32977 | 36460ns | 4KB | 73 |
| Account Lookup By Stake Key No Data | 35020 | 32890ns | 4KB | 75 |
| Transaction History Queries Real Data | 38739 | 35904ns | 4KB | 78 |
| D Rep Lookup By Key Hash Real Data | 38977 | 36859ns | 4KB | 77 |
| Concurrent Queries | 19104 | 59527ns | 168280queries/sec | 3851 |
| Block Memory Usage | 26552 | 46212ns | 14KB | 49 |
