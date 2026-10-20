// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/immutable"
	blobbadger "github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	badgerdb "github.com/dgraph-io/badger/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

const (
	envBadgerGCBench          = "DINGO_BENCH_BADGER_GC"
	envBadgerGCPolicies       = "DINGO_BENCH_BADGER_GC_POLICIES"
	envBadgerGCWorkloads      = "DINGO_BENCH_BADGER_GC_WORKLOADS"
	envBadgerGCRepeats        = "DINGO_BENCH_BADGER_GC_REPEATS"
	envBadgerGCValueThreshold = "DINGO_BENCH_BADGER_GC_VALUE_THRESHOLD"
	envBadgerGCVlogFileSize   = "DINGO_BENCH_BADGER_GC_VLOG_FILE_SIZE"
	envBadgerGCMemTableSize   = "DINGO_BENCH_BADGER_GC_MEMTABLE_SIZE"
	envBadgerGCDataDir        = "DINGO_BENCH_BADGER_GC_DATADIR"
)

var defaultBadgerGCWorkloads = []string{
	"core-load",
	"api-backfill",
	"history-expiry",
	"steady-state",
}

type badgerGCPolicy struct {
	name     string
	enabled  bool
	postLoad bool
	settled  bool
	adaptive bool
	interval time.Duration
	discard  float64
}

type badgerGCBenchConfig struct {
	policies       []badgerGCPolicy
	workloads      []string
	repeats        int
	valueThreshold int64
	vlogFileSize   int64
	memTableSize   int64
	dataDir        string
}

func parseBadgerGCPolicies(raw string) ([]badgerGCPolicy, error) {
	if raw == "" {
		raw = "off,post/0.5,settled/0.25,settled/0.5," +
			"settled/0.75,adaptive/0.5,5m/0.5,15m/0.5"
	}
	var ret []badgerGCPolicy
	seen := make(map[string]struct{})
	for entry := range strings.SplitSeq(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			return nil, errors.New("empty Badger GC policy")
		}
		if _, ok := seen[entry]; ok {
			return nil, fmt.Errorf("duplicate Badger GC policy %q", entry)
		}
		seen[entry] = struct{}{}
		if entry == "off" {
			ret = append(ret, badgerGCPolicy{
				name: entry, discard: blobbadger.DefaultGCDiscardRatio,
			})
			continue
		}
		parts := strings.Split(entry, "/")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid Badger GC policy %q", entry)
		}
		ratio, err := strconv.ParseFloat(parts[1], 64)
		if err != nil || !(ratio > 0 && ratio < 1) {
			return nil, fmt.Errorf(
				"invalid Badger GC discard ratio in %q", entry,
			)
		}
		if parts[0] == "post" || parts[0] == "settled" ||
			parts[0] == "adaptive" {
			ret = append(ret, badgerGCPolicy{
				name: entry, postLoad: true,
				settled:  parts[0] == "settled",
				adaptive: parts[0] == "adaptive", discard: ratio,
			})
			continue
		}
		interval, err := time.ParseDuration(parts[0])
		if err != nil || interval <= 0 {
			return nil, fmt.Errorf(
				"invalid Badger GC interval in %q", entry,
			)
		}
		ret = append(ret, badgerGCPolicy{
			name: entry, enabled: true, interval: interval, discard: ratio,
		})
	}
	return ret, nil
}

func parseBadgerGCWorkloads(raw string) ([]string, error) {
	if raw == "" {
		return defaultBadgerGCWorkloads, nil
	}
	allowed := make(map[string]struct{}, len(defaultBadgerGCWorkloads))
	for _, name := range defaultBadgerGCWorkloads {
		allowed[name] = struct{}{}
	}
	var ret []string
	seen := make(map[string]struct{})
	for name := range strings.SplitSeq(raw, ",") {
		name = strings.TrimSpace(name)
		if _, ok := allowed[name]; !ok {
			return nil, fmt.Errorf("unknown Badger GC workload %q", name)
		}
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("duplicate Badger GC workload %q", name)
		}
		seen[name] = struct{}{}
		ret = append(ret, name)
	}
	if len(ret) == 0 {
		return nil, errors.New("no Badger GC workloads selected")
	}
	return ret, nil
}

func badgerGCEnvInt64(
	getenv func(string) string,
	name string,
	def int64,
) (int64, error) {
	raw := getenv(name)
	if raw == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid %s=%q", name, raw)
	}
	return n, nil
}

func badgerGCBenchConfigFrom(
	getenv func(string) string,
) (badgerGCBenchConfig, error) {
	var cfg badgerGCBenchConfig
	var err error
	if cfg.policies, err = parseBadgerGCPolicies(
		getenv(envBadgerGCPolicies),
	); err != nil {
		return cfg, err
	}
	if cfg.workloads, err = parseBadgerGCWorkloads(
		getenv(envBadgerGCWorkloads),
	); err != nil {
		return cfg, err
	}
	repeats, err := badgerGCEnvInt64(getenv, envBadgerGCRepeats, 1)
	if err != nil {
		return cfg, err
	}
	if repeats > int64(^uint(0)>>1) {
		return cfg, fmt.Errorf("%s overflows int", envBadgerGCRepeats)
	}
	cfg.repeats = int(repeats)
	if cfg.valueThreshold, err = badgerGCEnvInt64(
		getenv, envBadgerGCValueThreshold,
		blobbadger.DefaultValueThreshold,
	); err != nil {
		return cfg, err
	}
	if cfg.vlogFileSize, err = badgerGCEnvInt64(
		getenv, envBadgerGCVlogFileSize,
		blobbadger.DefaultValueLogFileSize,
	); err != nil {
		return cfg, err
	}
	if cfg.memTableSize, err = badgerGCEnvInt64(
		getenv, envBadgerGCMemTableSize,
		blobbadger.DefaultMemTableSize,
	); err != nil {
		return cfg, err
	}
	cfg.dataDir = getenv(envBadgerGCDataDir)
	return cfg, nil
}

type badgerGCBlock struct {
	slot     uint64
	hash     []byte
	cbor     []byte
	typeID   uint
	height   uint64
	prevHash []byte
}

func loadBadgerGCBlocks(tb testing.TB) ([]badgerGCBlock, int64, int) {
	tb.Helper()
	imm, err := immutable.New(filepath.Join(
		"..", "..", "database", "immutable", "testdata",
	))
	require.NoError(tb, err)
	iter, err := imm.BlocksFromPoint(ocommon.NewPointOrigin())
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, iter.Close()) })
	var (
		blocks    []badgerGCBlock
		inputSize int64
		maxSize   int
		prevHash  []byte
	)
	for {
		block, err := iter.Next()
		require.NoError(tb, err)
		if block == nil {
			break
		}
		require.NotEmpty(tb, block.Cbor, "real block fixture is empty")
		blocks = append(blocks, badgerGCBlock{
			slot: block.Slot, hash: block.Hash, cbor: block.Cbor,
			typeID: block.Type, height: uint64(len(blocks) + 1),
			prevHash: append([]byte(nil), prevHash...),
		})
		prevHash = append(prevHash[:0], block.Hash...)
		inputSize += int64(len(block.Cbor))
		maxSize = max(maxSize, len(block.Cbor))
	}
	require.NotEmpty(tb, blocks, "real ImmutableDB fixture has no blocks")
	require.Positive(tb, inputSize, "real ImmutableDB fixture has no CBOR")
	return blocks, inputSize, maxSize
}

type badgerGCStore struct {
	store    *blobbadger.BlobStoreBadger
	registry *prometheus.Registry
	dir      string
}

func newBadgerGCStore(
	tb testing.TB,
	cfg badgerGCBenchConfig,
	policy badgerGCPolicy,
	compactMetadata bool,
) badgerGCStore {
	tb.Helper()
	dir := tb.TempDir()
	if cfg.dataDir != "" {
		require.NoError(tb, os.MkdirAll(cfg.dataDir, 0o750))
		var err error
		dir, err = os.MkdirTemp(cfg.dataDir, "badger-gc-*")
		require.NoError(tb, err)
		tb.Cleanup(func() { require.NoError(tb, os.RemoveAll(dir)) })
	}
	registry := prometheus.NewRegistry()
	store, err := blobbadger.New(
		blobbadger.WithDataDir(dir),
		blobbadger.WithGc(policy.enabled),
		blobbadger.WithGcInterval(policy.interval),
		blobbadger.WithGcDiscardRatio(policy.discard),
		blobbadger.WithPromRegistry(registry),
		blobbadger.WithValueThreshold(cfg.valueThreshold),
		blobbadger.WithValueLogFileSize(cfg.vlogFileSize),
		blobbadger.WithMemTableSize(cfg.memTableSize),
		blobbadger.WithCompactBlockMetadata(compactMetadata),
	)
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, store.Close()) })
	return badgerGCStore{store: store, registry: registry, dir: dir}
}

func writeBadgerGCBlocks(
	tb testing.TB,
	store *blobbadger.BlobStoreBadger,
	blocks []badgerGCBlock,
	repeats int,
) (int, time.Duration) {
	tb.Helper()
	const batchSize = 64
	maxBatchPayloadBytes := store.DB().MaxBatchSize() / 2
	require.Positive(tb, maxBatchPayloadBytes)
	batchEnds := badgerGCBlockBatchEnds(
		blocks, batchSize, maxBatchPayloadBytes,
	)
	start := time.Now()
	for range repeats {
		lo := 0
		for _, hi := range batchEnds {
			txn := store.NewTransaction(true)
			for i := lo; i < hi; i++ {
				block := blocks[i]
				if err := store.SetBlock(
					txn, block.slot, block.hash, block.cbor,
					block.height, block.typeID, block.height,
					block.prevHash,
				); err != nil {
					_ = txn.Rollback()
					tb.Fatalf("write block %d: %v", i, err)
				}
			}
			require.NoError(tb, txn.Commit())
			require.NoError(tb, store.Sync())
			lo = hi
		}
	}
	return len(blocks) * repeats, time.Since(start)
}

func badgerGCBlockBatchEnds(
	blocks []badgerGCBlock,
	maxBlocks int,
	maxPayloadBytes int64,
) []int {
	var ends []int
	start := 0
	var payloadBytes int64
	for i, block := range blocks {
		blockBytes := int64(len(block.cbor))
		if i > start && (i-start >= maxBlocks ||
			blockBytes > maxPayloadBytes-payloadBytes) {
			ends = append(ends, i)
			start = i
			payloadBytes = 0
		}
		payloadBytes += blockBytes
	}
	if start < len(blocks) {
		ends = append(ends, len(blocks))
	}
	return ends
}

func readBadgerGCBlocks(
	tb testing.TB,
	store *blobbadger.BlobStoreBadger,
	blocks []badgerGCBlock,
	order []int,
	recorder *latencyRecorder,
) time.Duration {
	tb.Helper()
	start := time.Now()
	for _, i := range order {
		block := blocks[i]
		txn := store.NewTransaction(false)
		readStart := time.Now()
		cborData, _, err := store.GetBlock(txn, block.slot, block.hash)
		recorder.record(time.Since(readStart))
		require.NoError(tb, txn.Rollback())
		require.NoError(tb, err)
		require.Len(tb, cborData, len(block.cbor))
	}
	return time.Since(start)
}

func tombstoneBadgerGCBlocks(
	tb testing.TB,
	store *blobbadger.BlobStoreBadger,
	blocks []badgerGCBlock,
) time.Duration {
	tb.Helper()
	const batchSize = 1000
	limit := len(blocks) * 3 / 4
	start := time.Now()
	for lo := 0; lo < limit; lo += batchSize {
		hi := min(lo+batchSize, limit)
		txn := store.NewTransaction(true)
		for i := lo; i < hi; i++ {
			block := blocks[i]
			if err := store.TombstoneBlock(
				txn, block.slot, block.hash,
			); err != nil {
				_ = txn.Rollback()
				tb.Fatalf("tombstone block %d: %v", i, err)
			}
		}
		require.NoError(tb, txn.Commit())
		require.NoError(tb, store.Sync())
	}
	return time.Since(start)
}

type badgerGCFileSizes struct {
	lsm       int64
	vlog      int64
	sstTables int
}

func badgerGCFileSize(tb testing.TB, dir string) badgerGCFileSizes {
	tb.Helper()
	var ret badgerGCFileSizes
	err := filepath.Walk(filepath.Join(dir, "blob"), func(
		path string,
		info os.FileInfo,
		err error,
	) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		switch filepath.Ext(path) {
		case ".sst":
			ret.lsm += info.Size()
			ret.sstTables++
		case ".vlog":
			ret.vlog += info.Size()
		}
		return nil
	})
	require.NoError(tb, err)
	return ret
}

func shouldTriggerAdaptiveGC(
	sizes badgerGCFileSizes,
	vlogFileSize int64,
) bool {
	return sizes.sstTables > 1 && sizes.vlog > vlogFileSize
}

type badgerGCDrain struct {
	attempts  int
	rewrites  int
	noRewrite int
	settle    time.Duration
	duration  time.Duration
	reclaimed int64
	adaptive  bool
}

func drainBadgerGC(
	tb testing.TB,
	benchStore badgerGCStore,
	ratio float64,
) badgerGCDrain {
	tb.Helper()
	before := badgerGCFileSize(tb, benchStore.dir)
	start := time.Now()
	var ret badgerGCDrain
	for range 256 {
		ret.attempts++
		err := benchStore.store.DB().RunValueLogGC(ratio)
		if errors.Is(err, badgerdb.ErrNoRewrite) {
			ret.noRewrite++
			break
		}
		require.NoError(tb, err)
		ret.rewrites++
	}
	ret.duration = time.Since(start)
	require.Less(tb, ret.attempts, 256, "GC did not drain within 256 passes")
	after := badgerGCFileSize(tb, benchStore.dir)
	ret.reclaimed = max(before.lsm+before.vlog-after.lsm-after.vlog, 0)
	return ret
}

// reopenBadgerGCStore models the load-command lifecycle: bulk load closes the
// store, then serve mode reopens it before running value-log GC. Closing Badger
// flushes the memtable and makes overwritten or tombstoned value pointers
// visible to its discard accounting; an immediate GC against the still-open
// load store cannot make the same policy decision.
func reopenBadgerGCStore(
	tb testing.TB,
	benchStore badgerGCStore,
	cfg badgerGCBenchConfig,
	policy badgerGCPolicy,
	compactMetadata bool,
) (badgerGCStore, badgerGCFileSizes) {
	tb.Helper()
	require.NoError(tb, benchStore.store.Close())
	closedSizes := badgerGCFileSize(tb, benchStore.dir)
	store, err := blobbadger.New(
		blobbadger.WithDataDir(benchStore.dir),
		blobbadger.WithGc(false),
		blobbadger.WithGcInterval(policy.interval),
		blobbadger.WithGcDiscardRatio(policy.discard),
		blobbadger.WithPromRegistry(benchStore.registry),
		blobbadger.WithValueThreshold(cfg.valueThreshold),
		blobbadger.WithValueLogFileSize(cfg.vlogFileSize),
		blobbadger.WithMemTableSize(cfg.memTableSize),
		blobbadger.WithCompactBlockMetadata(compactMetadata),
	)
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, store.Close()) })
	benchStore.store = store
	return benchStore, closedSizes
}

func gatherMetricSum(
	tb testing.TB,
	registry *prometheus.Registry,
	name string,
) float64 {
	tb.Helper()
	families, err := registry.Gather()
	require.NoError(tb, err)
	var total float64
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			switch family.GetType().String() {
			case "COUNTER":
				total += metric.GetCounter().GetValue()
			case "GAUGE":
				total += metric.GetGauge().GetValue()
			case "HISTOGRAM":
				total += metric.GetHistogram().GetSampleSum()
			}
		}
	}
	return total
}

type badgerGCPressure struct {
	maxPending     float64
	maxCompactions float64
	compactionIO   float64
}

type badgerGCMetricsSnapshot struct {
	attempts  float64
	rewrites  float64
	noRewrite float64
	errors    float64
	duration  float64
}

func snapshotBadgerGCMetrics(
	tb testing.TB,
	registry *prometheus.Registry,
) badgerGCMetricsSnapshot {
	tb.Helper()
	return badgerGCMetricsSnapshot{
		attempts: gatherMetricSum(
			tb, registry, "database_blob_gc_attempts_total",
		),
		rewrites: gatherMetricSum(
			tb, registry, "database_blob_gc_successes_total",
		),
		noRewrite: gatherMetricSum(
			tb, registry, "database_blob_gc_no_rewrite_total",
		),
		errors: gatherMetricSum(
			tb, registry, "database_blob_gc_errors_total",
		),
		duration: gatherMetricSum(
			tb, registry, "database_blob_gc_duration_seconds",
		),
	}
}

func sampleBadgerGCPressure(
	ctx context.Context,
	registry *prometheus.Registry,
) <-chan badgerGCPressure {
	ret := make(chan badgerGCPressure, 1)
	go func() {
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		startIO := gatherMetricSumNoFail(
			registry, "database_blob_write_bytes_compaction",
		)
		var pressure badgerGCPressure
		for {
			pressure.maxPending = max(
				pressure.maxPending,
				gatherMetricSumNoFail(
					registry, "database_blob_write_pending_num_memtable",
				),
			)
			pressure.maxCompactions = max(
				pressure.maxCompactions,
				gatherMetricSumNoFail(
					registry, "database_blob_compaction_current_num_lsm",
				),
			)
			select {
			case <-ctx.Done():
				pressure.compactionIO = max(
					gatherMetricSumNoFail(
						registry,
						"database_blob_write_bytes_compaction",
					)-startIO,
					0,
				)
				ret <- pressure
				return
			case <-ticker.C:
			}
		}
	}()
	return ret
}

func gatherMetricSumNoFail(
	registry *prometheus.Registry,
	name string,
) float64 {
	families, err := registry.Gather()
	if err != nil {
		return 0
	}
	var total float64
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if gauge := metric.GetGauge(); gauge != nil {
				total += gauge.GetValue()
			}
			if counter := metric.GetCounter(); counter != nil {
				total += counter.GetValue()
			}
		}
	}
	return total
}

type procIOCounters struct {
	readBytes  uint64
	writeBytes uint64
	ok         bool
}

func readProcIO() procIOCounters {
	data, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return procIOCounters{}
	}
	var ret procIOCounters
	for line := range strings.SplitSeq(string(data), "\n") {
		name, raw, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		value, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return procIOCounters{}
		}
		switch name {
		case "read_bytes":
			ret.readBytes = value
		case "write_bytes":
			ret.writeBytes = value
		}
	}
	ret.ok = true
	return ret
}

func reportBadgerGCPolicyMetrics(
	b *testing.B,
	benchStore badgerGCStore,
	inputBlocks int,
	inputSize int64,
	maxBlockSize int,
	loadDuration time.Duration,
	tombstones int,
	tombstoneDuration time.Duration,
	rewrites int,
	rewriteDuration time.Duration,
	reads int,
	readDuration time.Duration,
	readSamples *latencyRecorder,
	shutdownDuration time.Duration,
	gcMetrics badgerGCMetricsSnapshot,
	drain badgerGCDrain,
	pressure badgerGCPressure,
	beforeIO, afterIO procIOCounters,
) {
	b.Helper()
	sizes := badgerGCFileSize(b, benchStore.dir)
	b.ReportMetric(float64(inputBlocks), "input-blocks")
	b.ReportMetric(float64(inputSize), "input-bytes")
	b.ReportMetric(float64(maxBlockSize), "max-block-bytes")
	b.ReportMetric(float64(sizes.lsm), "lsm-bytes")
	b.ReportMetric(float64(sizes.vlog), "vlog-bytes")
	b.ReportMetric(float64(sizes.sstTables), "sst-tables")
	b.ReportMetric(float64(sizes.lsm+sizes.vlog), "disk-bytes")
	if loadDuration > 0 {
		b.ReportMetric(
			float64(inputBlocks)/loadDuration.Seconds(), "load-blocks/s",
		)
	}
	if tombstoneDuration > 0 {
		b.ReportMetric(
			float64(tombstones)/tombstoneDuration.Seconds(), "tombstones/s",
		)
	}
	if rewriteDuration > 0 {
		b.ReportMetric(
			float64(rewrites)/rewriteDuration.Seconds(), "rewrite-blocks/s",
		)
	}
	if readDuration > 0 && reads > 0 {
		readSamples.report(b, "read")
		b.ReportMetric(
			float64(reads)/readDuration.Seconds(),
			"reads/s",
		)
	}
	attempts := gcMetrics.attempts + float64(drain.attempts)
	gcRewrites := gcMetrics.rewrites + float64(drain.rewrites)
	b.ReportMetric(attempts, "gc-attempts")
	b.ReportMetric(gcRewrites, "gc-rewrites")
	b.ReportMetric(
		gcMetrics.noRewrite+float64(drain.noRewrite),
		"gc-no-rewrite",
	)
	b.ReportMetric(
		gcMetrics.errors,
		"gc-errors",
	)
	b.ReportMetric(
		gcMetrics.duration+drain.duration.Seconds(),
		"gc-duration-s",
	)
	b.ReportMetric(float64(drain.reclaimed), "post-gc-reclaimed-bytes")
	b.ReportMetric(drain.settle.Seconds(), "settle-compaction-s")
	b.ReportMetric(shutdownDuration.Seconds(), "shutdown-s")
	if drain.adaptive {
		b.ReportMetric(1, "adaptive-triggered")
	} else {
		b.ReportMetric(0, "adaptive-triggered")
	}
	b.ReportMetric(pressure.maxPending, "max-pending-memtables")
	b.ReportMetric(pressure.maxCompactions, "max-lsm-compactions")
	b.ReportMetric(pressure.compactionIO, "compaction-write-bytes")
	if beforeIO.ok && afterIO.ok {
		b.ReportMetric(
			float64(afterIO.readBytes-beforeIO.readBytes), "process-read-bytes",
		)
		b.ReportMetric(
			float64(afterIO.writeBytes-beforeIO.writeBytes), "process-write-bytes",
		)
	}
	reportDirAllocatedBytes(b, filepath.Join(benchStore.dir, "blob"), "allocated-bytes")
	reportRSS(b)
}

// BenchmarkBadgerGCPolicy replays the repository's real ImmutableDB blocks
// through production Badger operations. The four workloads cover the blob
// boundary shared by from-genesis and Mithril loads, API historical reads,
// history-expiry tombstones, and mixed steady-state reads and rewrites.
//
// This benchmark is opt-in because its default matrix writes the 79,866,190
// byte fixture once per workload and policy. Run it with -benchtime=1x; the
// environment controls are documented in docs/badger-gc.md.
func BenchmarkBadgerGCPolicy(b *testing.B) {
	if os.Getenv(envBadgerGCBench) != "1" {
		b.Skip("set " + envBadgerGCBench + "=1 to run the Badger GC matrix")
	}
	if b.N != 1 {
		b.Fatalf("run with -benchtime=1x, got b.N=%d", b.N)
	}
	cfg, err := badgerGCBenchConfigFrom(os.Getenv)
	require.NoError(b, err)
	blocks, inputSize, maxBlockSize := loadBadgerGCBlocks(b)
	for _, workload := range cfg.workloads {
		for _, policy := range cfg.policies {
			b.Run(workload+"/"+policy.name, func(b *testing.B) {
				compactMetadata := workload == "core-load" ||
					workload == "steady-state"
				benchStore := newBadgerGCStore(
					b, cfg, policy, compactMetadata,
				)
				beforeIO := readProcIO()
				pressureCtx, stopPressure := context.WithCancel(
					context.Background(),
				)
				pressureDone := sampleBadgerGCPressure(
					pressureCtx, benchStore.registry,
				)
				readSamples := newLatencyRecorder(len(blocks))
				var (
					tombstones, rewrites, reads                      int
					tombstoneDuration, rewriteDuration, readDuration time.Duration
				)

				b.ResetTimer()
				inputBlocks, loadDuration := writeBadgerGCBlocks(
					b, benchStore.store, blocks, cfg.repeats,
				)
				switch workload {
				case "core-load":
				case "api-backfill":
					order := make([]int, len(blocks))
					for i := range order {
						order[i] = i
					}
					readDuration = readBadgerGCBlocks(
						b, benchStore.store, blocks, order, readSamples,
					)
					reads = len(order)
				case "history-expiry":
					tombstones = len(blocks) * 3 / 4
					tombstoneDuration = tombstoneBadgerGCBlocks(
						b, benchStore.store, blocks,
					)
				case "steady-state":
					rng := rand.New(rand.NewPCG(2701, 4861))
					order := make([]int, len(blocks))
					for i := range order {
						order[i] = rng.IntN(len(blocks))
					}
					readDuration = readBadgerGCBlocks(
						b, benchStore.store, blocks, order, readSamples,
					)
					reads = len(order)
					rewrites, rewriteDuration = writeBadgerGCBlocks(
						b, benchStore.store, blocks[:len(blocks)/10], 1,
					)
				default:
					b.Fatalf("unhandled workload %q", workload)
				}
				var drain badgerGCDrain
				if policy.postLoad {
					var closedSizes badgerGCFileSizes
					benchStore, closedSizes = reopenBadgerGCStore(
						b, benchStore, cfg, policy, compactMetadata,
					)
					if policy.settled ||
						(policy.adaptive &&
							shouldTriggerAdaptiveGC(
								closedSizes,
								cfg.vlogFileSize,
							)) {
						drain.adaptive = policy.adaptive
						settleStart := time.Now()
						require.NoError(b, benchStore.store.DB().Flatten(1))
						drain.settle = time.Since(settleStart)
					}
					if !policy.adaptive || drain.adaptive {
						measuredDrain := drainBadgerGC(
							b, benchStore, policy.discard,
						)
						measuredDrain.settle = drain.settle
						measuredDrain.adaptive = drain.adaptive
						drain = measuredDrain
					}
				}
				stopPressure()
				pressure := <-pressureDone
				gcMetrics := snapshotBadgerGCMetrics(
					b, benchStore.registry,
				)
				shutdownStart := time.Now()
				require.NoError(b, benchStore.store.Close())
				shutdownDuration := time.Since(shutdownStart)
				b.StopTimer()
				afterIO := readProcIO()
				reportBadgerGCPolicyMetrics(
					b, benchStore, inputBlocks,
					inputSize*int64(cfg.repeats), maxBlockSize,
					loadDuration,
					tombstones, tombstoneDuration,
					rewrites, rewriteDuration, reads,
					readDuration, readSamples, shutdownDuration,
					gcMetrics, drain, pressure,
					beforeIO, afterIO,
				)
			})
		}
	}
}

func TestBadgerGCBenchConfig(t *testing.T) {
	t.Parallel()
	env := map[string]string{
		envBadgerGCPolicies:       "off,post/0.5,settled/0.5,adaptive/0.5,25ms/0.25,1s/0.75",
		envBadgerGCWorkloads:      "core-load,history-expiry",
		envBadgerGCRepeats:        "3",
		envBadgerGCValueThreshold: "1024",
		envBadgerGCVlogFileSize:   "1048576",
		envBadgerGCMemTableSize:   "2097152",
		envBadgerGCDataDir:        "/bench-volume",
	}
	cfg, err := badgerGCBenchConfigFrom(func(k string) string { return env[k] })
	require.NoError(t, err)
	require.Len(t, cfg.policies, 6)
	require.Equal(t, []string{"core-load", "history-expiry"}, cfg.workloads)
	require.Equal(t, 3, cfg.repeats)
	require.Equal(t, int64(1024), cfg.valueThreshold)
	require.Equal(t, int64(1048576), cfg.vlogFileSize)
	require.Equal(t, int64(2097152), cfg.memTableSize)
	require.Equal(t, "/bench-volume", cfg.dataDir)
	require.False(t, cfg.policies[0].enabled)
	require.True(t, cfg.policies[1].postLoad)
	require.True(t, cfg.policies[2].settled)
	require.True(t, cfg.policies[3].adaptive)
	require.Equal(t, 25*time.Millisecond, cfg.policies[4].interval)
	require.InDelta(t, 0.75, cfg.policies[5].discard, 0)
}

func TestBadgerGCBenchConfigRejectsInvalidValues(t *testing.T) {
	t.Parallel()
	for name, env := range map[string]map[string]string{
		"policy":    {envBadgerGCPolicies: "five-minutes"},
		"ratio":     {envBadgerGCPolicies: "5m/1"},
		"workload":  {envBadgerGCWorkloads: "made-up"},
		"repeats":   {envBadgerGCRepeats: "0"},
		"threshold": {envBadgerGCValueThreshold: "-1"},
		"vlog size": {envBadgerGCVlogFileSize: "zero"},
		"memtable":  {envBadgerGCMemTableSize: "0"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := badgerGCBenchConfigFrom(
				func(k string) string { return env[k] },
			)
			require.Error(t, err)
		})
	}
}

func TestSnapshotBadgerGCMetrics(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	registerCounter := func(name string, value float64) {
		counter := prometheus.NewCounter(prometheus.CounterOpts{Name: name})
		registry.MustRegister(counter)
		counter.Add(value)
	}
	registerCounter("database_blob_gc_attempts_total", 7)
	registerCounter("database_blob_gc_successes_total", 3)
	registerCounter("database_blob_gc_no_rewrite_total", 2)
	registerCounter("database_blob_gc_errors_total", 1)
	duration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "database_blob_gc_duration_seconds",
	})
	registry.MustRegister(duration)
	duration.Observe(0.25)
	duration.Observe(0.75)

	snapshot := snapshotBadgerGCMetrics(t, registry)
	require.Equal(t, float64(7), snapshot.attempts)
	require.Equal(t, float64(3), snapshot.rewrites)
	require.Equal(t, float64(2), snapshot.noRewrite)
	require.Equal(t, float64(1), snapshot.errors)
	require.Equal(t, float64(1), snapshot.duration)
}

func TestShouldTriggerAdaptiveGC(t *testing.T) {
	t.Parallel()
	const vlogFileSize = int64(16 * 1024 * 1024)
	require.False(t, shouldTriggerAdaptiveGC(
		badgerGCFileSizes{sstTables: 1, vlog: vlogFileSize + 1},
		vlogFileSize,
	))
	require.False(t, shouldTriggerAdaptiveGC(
		badgerGCFileSizes{sstTables: 2, vlog: 20},
		vlogFileSize,
	))
	require.True(t, shouldTriggerAdaptiveGC(
		badgerGCFileSizes{sstTables: 2, vlog: vlogFileSize + 1},
		vlogFileSize,
	))
}

func TestBadgerGCBlockBatchEndsBoundsCountAndPayload(t *testing.T) {
	blocks := make([]badgerGCBlock, 6)
	for i, size := range []int{4, 4, 7, 1, 9, 2} {
		blocks[i].cbor = make([]byte, size)
	}

	ends := badgerGCBlockBatchEnds(blocks, 3, 8)
	require.Equal(t, []int{2, 4, 5, 6}, ends)
	lo := 0
	for _, hi := range ends {
		payloadBytes := int64(0)
		for _, block := range blocks[lo:hi] {
			payloadBytes += int64(len(block.cbor))
		}
		require.LessOrEqual(t, hi-lo, 3)
		require.True(t, payloadBytes <= 8 || hi-lo == 1)
		lo = hi
	}
}

func TestReadProcIO(t *testing.T) {
	t.Parallel()
	before := readProcIO()
	if !before.ok {
		t.Skip("process I/O counters are unavailable")
	}
	f, err := os.Open(filepath.Join(
		"..", "..", "database", "immutable", "testdata", "00000.chunk",
	))
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	after := readProcIO()
	require.True(t, after.ok)
	require.GreaterOrEqual(t, after.readBytes, before.readBytes)
	require.GreaterOrEqual(t, after.writeBytes, before.writeBytes)
}
