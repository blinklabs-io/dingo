// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package benchci

// CuratedBenchmarks lists the fixed-GOMAXPROCS benchmarks tracked across
// issue #1895's four dimensions: block validation throughput, sync speed,
// network throughput, and resource usage. Keep this in sync with the
// Makefile bench-ci target's first `go test -bench` regex.
var CuratedBenchmarks = []string{
	// Block validation throughput (ledger/benchmark_test.go).
	"BenchmarkBlockProcessingThroughput",
	"BenchmarkBlockProcessingThroughputPredecoded",
	"BenchmarkBlockBatchProcessingThroughput",
	"BenchmarkRawBlockBatchProcessingThroughput",
	"BenchmarkVerifyBlockHeader",
	"BenchmarkTransactionValidation",

	// Sync speed.
	"BenchmarkChainSyncFromGenesis",             // ledger/benchmark_test.go
	"BenchmarkRealBlockProcessing",              // ledger/benchmark_test.go
	"BenchmarkEraTransitionPerformanceRealData", // ledger/benchmark_test.go
	"BenchmarkTestLoad",                         // internal/integration/benchmark_test.go

	// Network throughput.
	"BenchmarkBlockfetchNearTipThroughput",             // ledger/benchmark_test.go
	"BenchmarkBlockfetchNearTipThroughputPredecoded",   // ledger/benchmark_test.go
	"BenchmarkBlockfetchNearTipFlushOnlyPredecoded",    // ledger/benchmark_test.go
	"BenchmarkBlockfetchNearTipQueuedHeaderPredecoded", // ledger/benchmark_test.go
	"BenchmarkBlockfetchVerifiedHeaderDispatch",        // ledger/benchmark_test.go
	"BenchmarkBlockfetchClientBlockMetrics",            // ouroboros/blockfetch_test.go
	"BenchmarkUpdateConnectionMetrics",                 // connmanager/benchmark_test.go
	"BenchmarkHasInboundPeerAddress",                   // connmanager/benchmark_test.go
	"BenchmarkReconcile",                               // peergov/benchmark_test.go
	"BenchmarkPublishSubscribers",                      // event/benchmark_test.go

	// Resource usage.
	"BenchmarkBlockMemoryUsage",             // ledger/benchmark_test.go
	"BenchmarkHotCacheGet",                  // database/cbor_cache_bench_test.go
	"BenchmarkHotCachePut",                  // database/cbor_cache_bench_test.go
	"BenchmarkHotCacheGetMiss",              // database/cbor_cache_bench_test.go
	"BenchmarkBlockLRUCacheGet",             // database/cbor_cache_bench_test.go
	"BenchmarkBlockLRUCachePut",             // database/cbor_cache_bench_test.go
	"BenchmarkTieredCacheHotHit",            // database/cbor_cache_bench_test.go
	"BenchmarkCachedBlockExtract",           // database/cbor_cache_bench_test.go
	"BenchmarkCborOffsetEncode",             // database/cbor_cache_bench_test.go
	"BenchmarkCborOffsetDecode",             // database/cbor_cache_bench_test.go
	"BenchmarkStorageModeIngest",            // ledger/benchmark_test.go
	"BenchmarkStorageModeIngestSteadyState", // ledger/benchmark_test.go
}

// LockContentionBenchmarks lists the GOMAXPROCS lock-contention sweep
// benchmarks, run under -cpu=1,4,8,16 by the Makefile bench-ci target's
// second `go test -bench` invocation. BenchmarkBlockLRUParallel* is the
// literal LRU-cache incident (a single mutex made the cache ~8x slower at 16
// cores before sharding). BenchmarkTipSnapshotReadOnly and
// BenchmarkTipSnapshotReadUnderWriter are the dedicated #2601 sentinel: they
// exercise the exact atomic.Pointer[consensusSnapshot]/[tipSnapshot]
// read-under-concurrent-writer pattern that #2601 fixed (a plain RWMutex on
// that path scaled backwards -- ~591ns at 16 cores with a concurrent writer
// vs ~133ns read-only). BenchmarkConcurrentQueries is kept alongside them as
// a broader database-query-under-concurrency check, not a substitute. Keep
// this list in sync with that invocation's -bench regex.
var LockContentionBenchmarks = []string{
	"BenchmarkBlockLRUParallelReadHeavy",     // database/block_lru_cache_parallel_bench_test.go
	"BenchmarkBlockLRUParallelBalanced",      // database/block_lru_cache_parallel_bench_test.go
	"BenchmarkBlockLRUParallelReadOnly",      // database/block_lru_cache_parallel_bench_test.go
	"BenchmarkHotCacheParallelGet",           // database/cbor_cache_bench_test.go
	"BenchmarkTryReserveInboundSlotParallel", // connmanager/benchmark_test.go
	"BenchmarkConcurrentQueries",             // ledger/benchmark_test.go
	"BenchmarkTipSnapshotReadOnly",           // ledger/snapshot_parallel_bench_test.go
	"BenchmarkTipSnapshotReadUnderWriter",    // ledger/snapshot_parallel_bench_test.go
}

// TrackedBenchmarks is the full set of benchmarks compared for CI regression
// detection: CuratedBenchmarks plus LockContentionBenchmarks. The Makefile
// bench-ci target concatenates both `go test` invocations' output into a
// single run file, so benchcheck compares against this combined list.
var TrackedBenchmarks = append(
	append([]string{}, CuratedBenchmarks...),
	LockContentionBenchmarks...,
)
