# Dingo Ledger & Database Benchmark Results

## Latest Results

### Test Environment
- **Date**: June 12, 2026
- **Go Version**: 1.26.1
- **OS**: Linux
- **Architecture**: aarch64
- **CPU Cores**: 128
- **Data Source**: Real Cardano preview testnet data (40k+ blocks, slots 0-863,996)

### Benchmark Results

All benchmarks run with `-benchmem` flag. Iterations are Go benchmark iteration counts; benchmark-specific throughput metrics (for example `blocks/sec`) are reported separately.

| Benchmark | Iterations | Time/op | Extra Metrics | Memory/op | Allocs/op |
|-----------|------------|---------|---------------|-----------|-----------|
| connmanager:Has Inbound Peer Address/10 | 1876215 | 645.4ns | - | 40B | 2 |
| connmanager:Has Inbound Peer Address/100 | 2049895 | 653.5ns | - | 40B | 2 |
| connmanager:Has Inbound Peer Address/1000 | 2212045 | 595.4ns | - | 40B | 2 |
| connmanager:Has Inbound Peer Address/500 | 1976878 | 635.2ns | - | 40B | 2 |
| connmanager:Try Reserve Inbound Slot Parallel/10 | 2844913 | 442.9ns | - | 0B | 0 |
| connmanager:Try Reserve Inbound Slot Parallel/100 | 1329261 | 918.2ns | - | 0B | 0 |
| connmanager:Try Reserve Inbound Slot Parallel/500 | 1305675 | 882.4ns | - | 0B | 0 |
| connmanager:Update Connection Metrics/10 | 39124141 | 30.51ns | - | 0B | 0 |
| connmanager:Update Connection Metrics/100 | 37243389 | 30.55ns | - | 0B | 0 |
| connmanager:Update Connection Metrics/1000 | 38756811 | 30.31ns | - | 0B | 0 |
| connmanager:Update Connection Metrics/500 | 40533412 | 30.28ns | - | 0B | 0 |
| database:Batch Resolution Hot Hits | 2638 | 426745ns | - | 110KB | 254 |
| database:Block L R U Cache Get | 21083229 | 55.46ns | - | 0B | 0 |
| database:Block L R U Cache Put | 1930647 | 625.2ns | - | 96B | 2 |
| database:Cached Block Extract | 62241637 | 18.20ns | - | 0B | 0 |
| database:Cbor Offset Decode | 10259774 | 120.7ns | - | 48B | 1 |
| database:Cbor Offset Encode | 145843496 | 7.755ns | - | 0B | 0 |
| database:Hot Cache Get | 2163880 | 560.5ns | - | 192B | 2 |
| database:Hot Cache Get Miss | 122696038 | 9.175ns | - | 0B | 0 |
| database:Hot Cache Parallel Get | 10000 | 129295ns | - | 438KB | 57 |
| database:Hot Cache Put | 10000 | 2008500ns | - | 638KB | 40 |
| database:Make Utxo Key | 185591302 | 6.447ns | - | 0B | 0 |
| database:Metrics Increment | 134105025 | 8.441ns | - | 0B | 0 |
| database:Metrics Increment With Prometheus | 87684171 | 13.40ns | - | 0B | 0 |
| database:Tiered Cache Hot Hit | 1855422 | 644.0ns | - | 191B | 1 |
| database:Transaction Create | 69379 | 16209ns | - | 2KB | 21 |
| database:Tx Batch Resolution Hot Hits | 2960 | 392791ns | - | 149KB | 254 |
| event:Publish Subscribers/1 | 9006843 | 155.2ns | - | 0B | 0 |
| event:Publish Subscribers/10 | 1379077 | 850.1ns | - | 0B | 0 |
| event:Publish Subscribers/100 | 122960 | 9315ns | - | 0B | 0 |
| event:Publish Subscribers/500 | 14185 | 81714ns | - | 0B | 0 |
| integration:Storage Backends/ S3 | 51 | 23474469ns | - | 413KB | 5604 |
| integration:Storage Backends/disk | 20888 | 60056ns | - | 36KB | 120 |
| integration:Storage Backends/memory | 16039 | 72909ns | - | 36KB | 120 |
| integration:Test Load/ S3 | 3 | 469521190ns | - | 8223KB | 111670 |
| integration:Test Load/disk | 1117 | 1155046ns | - | 681KB | 2021 |
| integration:Test Load/memory | 1447 | 1070303ns | - | 681KB | 2021 |
| leader:Is Leader For Slot | 16403265 | 62.15ns | - | 0B | 0 |
| ledger:Account Lookup By Stake Key No Data | 30961 | 45169ns | - | 5KB | 82 |
| ledger:Account Lookup By Stake Key Real Data | 32756 | 40402ns | - | 5KB | 82 |
| ledger:Block Batch Processing Throughput | 1738 | 710552ns | 70368 blocks/sec | 97KB | 2238 |
| ledger:Block Memory Usage | 17202 | 69987ns | - | 22KB | 81 |
| ledger:Block Nonce Lookup No Data | 31898 | 40346ns | - | 4KB | 66 |
| ledger:Block Nonce Lookup Real Data | 38780 | 32720ns | - | 4KB | 66 |
| ledger:Block Processing Throughput | 10000 | 113856ns | 8783 blocks/sec | 24KB | 137 |
| ledger:Block Processing Throughput Predecoded | 24138 | 47821ns | 20911 blocks/sec | 2KB | 61 |
| ledger:Block Retrieval By Index No Data | 290994 | 4070ns | - | 490B | 10 |
| ledger:Block Retrieval By Index Real Data | 358546 | 3791ns | - | 490B | 10 |
| ledger:Blockfetch Near Tip Flush Only Predecoded | 27274 | 45207ns | 22120 blocks/sec | 2KB | 59 |
| ledger:Blockfetch Near Tip Queued Header Predecoded | 1539733 | 711.2ns | 1406054 blocks/sec | 418B | 2 |
| ledger:Blockfetch Near Tip Throughput | 28318 | 35968ns | 27802 blocks/sec | 21KB | 74 |
| ledger:Blockfetch Near Tip Throughput Predecoded | 24760303 | 47.30ns | 21143182 blocks/sec | 0B | 0 |
| ledger:Blockfetch Verified Header Dispatch | 164117104 | 7.003ns | - | 0B | 0 |
| ledger:Chain Sync From Genesis | 100 | 16226844ns | 100.0 blocks_processed | 3079KB | 20971 |
| ledger:Concurrent Queries | 13389 | 87594ns | 11452 queries/sec | 4KB | 56 |
| ledger:D Rep Lookup By Key Hash No Data | 29600 | 45740ns | - | 4KB | 78 |
| ledger:D Rep Lookup By Key Hash Real Data | 33402 | 40542ns | - | 4KB | 78 |
| ledger:Datum Lookup By Hash No Data | 42435 | 31087ns | - | 4KB | 62 |
| ledger:Datum Lookup By Hash Real Data | 44158 | 31732ns | - | 4KB | 62 |
| ledger:Era Transition Performance | 510968528 | 2.139ns | - | 0B | 0 |
| ledger:Era Transition Performance Real Data | 1788 | 632574ns | - | 115KB | 604 |
| ledger:Index Building Time | 12878 | 96172ns | - | 25KB | 147 |
| ledger:Pool Lookup By Key Hash No Data | 30160 | 38061ns | - | 4KB | 71 |
| ledger:Pool Lookup By Key Hash Real Data | 38528 | 35611ns | - | 4KB | 71 |
| ledger:Pool Registration Lookups No Data | 24674 | 59497ns | - | 15KB | 116 |
| ledger:Pool Registration Lookups Real Data | 22318 | 56090ns | - | 15KB | 116 |
| ledger:Protocol Parameters Lookup By Epoch No Data | 15066 | 81706ns | - | 5KB | 62 |
| ledger:Protocol Parameters Lookup By Epoch Real Data | 14772 | 78482ns | - | 5KB | 62 |
| ledger:Raw Block Batch Processing Throughput | 1774 | 685615ns | 72927 blocks/sec | 94KB | 2138 |
| ledger:Real Block Processing | 10000 | 113666ns | - | 25KB | 147 |
| ledger:Real Block Reading | 19 | 60340634ns | - | 4307KB | 93886 |
| ledger:Real Data Queries | 106478 | 16850ns | - | 5KB | 41 |
| ledger:Stake Registration Lookups No Data | 37182 | 35447ns | - | 5KB | 60 |
| ledger:Stake Registration Lookups Real Data | 35109 | 37608ns | - | 5KB | 60 |
| ledger:Storage Mode Ingest Steady State/api | 3 | 455072561ns | 395.5 blocks_ingested/sec, 439.5 txs_ingested/sec | 16134KB | 196320 |
| ledger:Storage Mode Ingest Steady State/core | 4 | 261284205ns | 688.9 blocks_ingested/sec, 765.5 txs_ingested/sec | 9934KB | 125256 |
| ledger:Storage Mode Ingest/api | 3 | 389263279ns | 462.4 blocks_ingested/sec, 513.8 txs_ingested/sec | 16274KB | 204481 |
| ledger:Storage Mode Ingest/core | 5 | 235316116ns | 764.9 blocks_ingested/sec, 849.9 txs_ingested/sec | 10111KB | 133483 |
| ledger:Transaction History Queries No Data | 29394 | 39802ns | - | 5KB | 73 |
| ledger:Transaction History Queries Real Data | 35984 | 36650ns | - | 5KB | 73 |
| ledger:Transaction Validation | 3714817 | 306.0ns | - | 64B | 2 |
| ledger:Utxo Lookup By Address No Data | 76494 | 17271ns | - | 2KB | 22 |
| ledger:Utxo Lookup By Address Real Data | 73520 | 16779ns | - | 2KB | 22 |
| ledger:Utxo Lookup By Ref No Data | 8665 | 123559ns | - | 9KB | 123 |
| ledger:Utxo Lookup By Ref Real Data | 10000 | 142173ns | - | 9KB | 123 |
| ledger:Verify Block Header/direct | 1074 | 1070831ns | - | 2KB | 28 |
| ledger:Verify Block Header/ledger_state | 1075 | 1089516ns | - | 2KB | 30 |
| ouroboros:Blockfetch Client Block Metrics | 3696294 | 284.8ns | - | 0B | 0 |
| peergov:Reconcile/100 | 7358 | 186636ns | - | 22KB | 166 |
| peergov:Reconcile/1000 | 488 | 2386170ns | - | 291KB | 2197 |
| peergov:Reconcile/500 | 1044 | 1185364ns | - | 138KB | 992 |
| sqlite:Get Utxo Address Keys Batch/address-keys | 54 | 21874618ns | - | 858KB | 11710 |
| sqlite:Get Utxo Address Keys Batch/full-utxo | 33 | 30965584ns | - | 1388KB | 20740 |
| sqlite:Import Utxos | 10 | 104772186ns | - | 1252KB | 11248 |
| sqlite:Import Utxos G O R M | 10 | 119981616ns | - | 1373KB | 21278 |
| sqlite:Insert Key Witnesses | 63 | 24106513ns | - | 406KB | 3652 |
| sqlite:Insert Key Witnesses G O R M | 36 | 38921330ns | - | 932KB | 17394 |

## Performance Changes

Changes since **November 26, 2025**:

### Summary
- **Faster benchmarks**: 0
- **Slower benchmarks**: 0
- **New benchmarks**: 97
- **Removed benchmarks**: 40

### New Benchmarks Added
- connmanager:Has Inbound Peer Address/10
- connmanager:Has Inbound Peer Address/100
- connmanager:Has Inbound Peer Address/1000
- connmanager:Has Inbound Peer Address/500
- connmanager:Try Reserve Inbound Slot Parallel/10
- connmanager:Try Reserve Inbound Slot Parallel/100
- connmanager:Try Reserve Inbound Slot Parallel/500
- connmanager:Update Connection Metrics/10
- connmanager:Update Connection Metrics/100
- connmanager:Update Connection Metrics/1000
- connmanager:Update Connection Metrics/500
- database:Batch Resolution Hot Hits
- database:Block L R U Cache Get
- database:Block L R U Cache Put
- database:Cached Block Extract
- database:Cbor Offset Decode
- database:Cbor Offset Encode
- database:Hot Cache Get
- database:Hot Cache Get Miss
- database:Hot Cache Parallel Get
- database:Hot Cache Put
- database:Make Utxo Key
- database:Metrics Increment
- database:Metrics Increment With Prometheus
- database:Tiered Cache Hot Hit
- database:Transaction Create
- database:Tx Batch Resolution Hot Hits
- event:Publish Subscribers/1
- event:Publish Subscribers/10
- event:Publish Subscribers/100
- event:Publish Subscribers/500
- integration:Storage Backends/ S3
- integration:Storage Backends/disk
- integration:Storage Backends/memory
- integration:Test Load/ S3
- integration:Test Load/disk
- integration:Test Load/memory
- leader:Is Leader For Slot
- ledger:Account Lookup By Stake Key No Data
- ledger:Account Lookup By Stake Key Real Data
- ledger:Block Batch Processing Throughput
- ledger:Block Memory Usage
- ledger:Block Nonce Lookup No Data
- ledger:Block Nonce Lookup Real Data
- ledger:Block Processing Throughput
- ledger:Block Processing Throughput Predecoded
- ledger:Block Retrieval By Index No Data
- ledger:Block Retrieval By Index Real Data
- ledger:Blockfetch Near Tip Flush Only Predecoded
- ledger:Blockfetch Near Tip Queued Header Predecoded
- ledger:Blockfetch Near Tip Throughput
- ledger:Blockfetch Near Tip Throughput Predecoded
- ledger:Blockfetch Verified Header Dispatch
- ledger:Chain Sync From Genesis
- ledger:Concurrent Queries
- ledger:D Rep Lookup By Key Hash No Data
- ledger:D Rep Lookup By Key Hash Real Data
- ledger:Datum Lookup By Hash No Data
- ledger:Datum Lookup By Hash Real Data
- ledger:Era Transition Performance
- ledger:Era Transition Performance Real Data
- ledger:Index Building Time
- ledger:Pool Lookup By Key Hash No Data
- ledger:Pool Lookup By Key Hash Real Data
- ledger:Pool Registration Lookups No Data
- ledger:Pool Registration Lookups Real Data
- ledger:Protocol Parameters Lookup By Epoch No Data
- ledger:Protocol Parameters Lookup By Epoch Real Data
- ledger:Raw Block Batch Processing Throughput
- ledger:Real Block Processing
- ledger:Real Block Reading
- ledger:Real Data Queries
- ledger:Stake Registration Lookups No Data
- ledger:Stake Registration Lookups Real Data
- ledger:Storage Mode Ingest Steady State/api
- ledger:Storage Mode Ingest Steady State/core
- ledger:Storage Mode Ingest/api
- ledger:Storage Mode Ingest/core
- ledger:Transaction History Queries No Data
- ledger:Transaction History Queries Real Data
- ledger:Transaction Validation
- ledger:Utxo Lookup By Address No Data
- ledger:Utxo Lookup By Address Real Data
- ledger:Utxo Lookup By Ref No Data
- ledger:Utxo Lookup By Ref Real Data
- ledger:Verify Block Header/direct
- ledger:Verify Block Header/ledger_state
- ouroboros:Blockfetch Client Block Metrics
- peergov:Reconcile/100
- peergov:Reconcile/1000
- peergov:Reconcile/500
- sqlite:Get Utxo Address Keys Batch/address-keys
- sqlite:Get Utxo Address Keys Batch/full-utxo
- sqlite:Import Utxos
- sqlite:Import Utxos G O R M
- sqlite:Insert Key Witnesses
- sqlite:Insert Key Witnesses G O R M

### Benchmarks Removed
- Account Lookup By Stake Key No Data
- Account Lookup By Stake Key Real Data
- Block Memory Usage
- Block Nonce Lookup No Data
- Block Nonce Lookup Real Data
- Block Processing Throughput
- Block Retrieval By Index No Data
- Block Retrieval By Index Real Data
- Chain Sync From Genesis
- Concurrent Queries
- D Rep Lookup By Key Hash No Data
- D Rep Lookup By Key Hash Real Data
- Datum Lookup By Hash No Data
- Datum Lookup By Hash Real Data
- Era Transition Performance
- Era Transition Performance Real Data
- Index Building Time
- Pool Lookup By Key Hash No Data
- Pool Lookup By Key Hash Real Data
- Pool Registration Lookups No Data
- Pool Registration Lookups Real Data
- Protocol Parameters Lookup By Epoch No Data
- Protocol Parameters Lookup By Epoch Real Data
- Real Block Processing
- Real Block Reading
- Real Data Queries
- Stake Registration Lookups No Data
- Stake Registration Lookups Real Data
- Storage Backends/disk
- Storage Backends/memory
- Test Load/disk
- Test Load/memory
- Transaction Create
- Transaction History Queries No Data
- Transaction History Queries Real Data
- Transaction Validation
- Utxo Lookup By Address No Data
- Utxo Lookup By Address Real Data
- Utxo Lookup By Ref No Data
- Utxo Lookup By Ref Real Data


## Historical Results

### November 26, 2025

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
