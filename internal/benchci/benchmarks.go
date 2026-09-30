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
	// Block validation throughput (ledger/tests_61443820_test.go).
	"BenchmarkBlockProcessingThroughput",
	"BenchmarkBlockProcessingThroughputPredecoded",
	"BenchmarkBlockBatchProcessingThroughput",
	"BenchmarkRawBlockBatchProcessingThroughput",
	"BenchmarkVerifyBlockHeader",
	"BenchmarkTransactionValidation",

	// Sync speed.
	"BenchmarkChainSyncFromGenesis",             // ledger/tests_61443820_test.go
	"BenchmarkRealBlockProcessing",              // ledger/tests_61443820_test.go
	"BenchmarkEraTransitionPerformanceRealData", // ledger/tests_61443820_test.go
	"BenchmarkTestLoad",                         // internal/integration/tests_7465a5ab_test.go

	// Network throughput.
	"BenchmarkBlockfetchNearTipThroughput",             // ledger/tests_61443820_test.go
	"BenchmarkBlockfetchNearTipThroughputPredecoded",   // ledger/tests_61443820_test.go
	"BenchmarkBlockfetchNearTipFlushOnlyPredecoded",    // ledger/tests_61443820_test.go
	"BenchmarkBlockfetchNearTipQueuedHeaderPredecoded", // ledger/tests_61443820_test.go
	"BenchmarkBlockfetchVerifiedHeaderDispatch",        // ledger/tests_61443820_test.go
	"BenchmarkBlockfetchClientBlockMetrics",            // ouroboros/blockfetch_test.go
	"BenchmarkUpdateConnectionMetrics",                 // connmanager/tests_test.go
	"BenchmarkHasInboundPeerAddress",                   // connmanager/tests_test.go
	"BenchmarkReconcile",                               // peergov/tests_test.go
	"BenchmarkPublishSubscribers",                      // event/tests_13d10404_test.go

	// Resource usage.
	"BenchmarkBlockMemoryUsage",             // ledger/tests_61443820_test.go
	"BenchmarkHotCacheGet",                  // database/cbor_cache_test.go
	"BenchmarkHotCachePut",                  // database/cbor_cache_test.go
	"BenchmarkHotCacheGetMiss",              // database/cbor_cache_test.go
	"BenchmarkBlockLRUCacheGet",             // database/cbor_cache_test.go
	"BenchmarkBlockLRUCachePut",             // database/cbor_cache_test.go
	"BenchmarkTieredCacheHotHit",            // database/cbor_cache_test.go
	"BenchmarkCachedBlockExtract",           // database/cbor_cache_test.go
	"BenchmarkCborOffsetEncode",             // database/cbor_cache_test.go
	"BenchmarkCborOffsetDecode",             // database/cbor_cache_test.go
	"BenchmarkStorageModeIngest",            // ledger/tests_61443820_test.go
	"BenchmarkStorageModeIngestSteadyState", // ledger/tests_61443820_test.go
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
	"BenchmarkBlockLRUParallelReadHeavy",     // database/block_lru_cache_test.go
	"BenchmarkBlockLRUParallelBalanced",      // database/block_lru_cache_test.go
	"BenchmarkBlockLRUParallelReadOnly",      // database/block_lru_cache_test.go
	"BenchmarkHotCacheParallelGet",           // database/cbor_cache_test.go
	"BenchmarkTryReserveInboundSlotParallel", // connmanager/tests_test.go
	"BenchmarkConcurrentQueries",             // ledger/tests_61443820_test.go
	"BenchmarkTipSnapshotReadOnly",           // ledger/tests_61443820_test.go
	"BenchmarkTipSnapshotReadUnderWriter",    // ledger/tests_61443820_test.go
}

// ledger/tests_61443820_test.go's RealData query benchmarks are deliberately
// absent from both lists. Seeding writes fixture blocks to the block store,
// but the accounts, pools, DReps, datums, protocol-parameter, nonce and
// registration tables they query are populated by applying a block rather
// than storing one, so each still times the same table miss its NoData twin
// times. There is no hit path for benchcheck to detect a regression in.

// TrackedBenchmarks is the full set of benchmarks compared for CI regression
// detection: CuratedBenchmarks plus LockContentionBenchmarks. The Makefile
// bench-ci target concatenates both `go test` invocations' output into a
// single run file, so benchcheck compares against this combined list.
var TrackedBenchmarks = append(
	append([]string{}, CuratedBenchmarks...),
	LockContentionBenchmarks...,
)
