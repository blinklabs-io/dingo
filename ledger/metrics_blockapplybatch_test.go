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

package ledger

import (
	"context"
	"sync"
	"testing"
	"time"

	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// blockApplyBatchLatencySample returns the sample count and sum recorded by
// dingo_ledger_block_apply_batch_latency_seconds.
func blockApplyBatchLatencySample(
	t *testing.T,
	m *stateMetrics,
) (count uint64, sum float64) {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, m.blockApplyBatchLatency.Write(metric))
	return metric.GetHistogram().GetSampleCount(), metric.GetHistogram().
		GetSampleSum()
}

// blockApplyBatchSizeSample returns the sample count and sum recorded by
// dingo_ledger_block_apply_batch_size.
func blockApplyBatchSizeSample(
	t *testing.T,
	m *stateMetrics,
) (count uint64, sum float64) {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, m.blockApplyBatchSize.Write(metric))
	return metric.GetHistogram().GetSampleCount(), metric.GetHistogram().
		GetSampleSum()
}

// blockApplyBatchMaxLatencyValue returns the value reg exports for
// dingo_ledger_block_apply_batch_max_latency_seconds. It reads the registry
// rather than a collector handle held on stateMetrics, because the metric
// deliberately keeps no exported-value state of its own to hold: the
// GaugeFunc collector reads the running-maximum atomic at scrape time.
func blockApplyBatchMaxLatencyValue(
	t *testing.T,
	reg *prometheus.Registry,
) float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "dingo_ledger_block_apply_batch_max_latency_seconds" {
			continue
		}
		require.Len(t, family.GetMetric(), 1)
		return family.GetMetric()[0].GetGauge().GetValue()
	}
	t.Fatalf(
		"no dingo_ledger_block_apply_batch_max_latency_seconds series found",
	)
	return 0
}

// largestFiniteBucket returns the widest finite upper bound of a histogram.
func largestFiniteBucket(t *testing.T, h prometheus.Metric) float64 {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, h.Write(metric))
	buckets := metric.GetHistogram().GetBucket()
	require.NotEmpty(t, buckets, "histogram has no finite bucket boundary")
	return buckets[len(buckets)-1].GetUpperBound()
}

// TestBlockApplyBatchLatencyBucketsCoverBlockStageRange requires the batch
// histogram to reach at least as far as
// dingo_ledger_block_stage_duration_seconds. A batch window contains the
// validate and apply stages of every block in the chunk, so a range narrower
// than the per-stage one would put in +Inf a batch whose single stage the
// stage histogram still resolves.
func TestBlockApplyBatchLatencyBucketsCoverBlockStageRange(t *testing.T) {
	t.Parallel()

	var m stateMetrics
	m.init(prometheus.NewRegistry())

	stageObserver, ok := m.blockStageApply.(prometheus.Metric)
	require.True(t, ok, "stage observer must be a collectable histogram")
	stageLargest := largestFiniteBucket(t, stageObserver)
	batchLargest := largestFiniteBucket(t, m.blockApplyBatchLatency)
	assert.GreaterOrEqual(
		t,
		batchLargest,
		stageLargest,
		"batch latency range (%vs) is narrower than the block stage range (%vs)",
		batchLargest,
		stageLargest,
	)
}

// TestObserveBlockApplyBatchRecordsLatencyAndSize is the regression test for
// the observeBlockApplyBatch wiring: each call must record one sample under
// both the latency histogram and the companion batch-size histogram, using
// the values passed in.
func TestObserveBlockApplyBatchRecordsLatencyAndSize(t *testing.T) {
	t.Parallel()

	var m stateMetrics
	m.init(prometheus.NewRegistry())

	m.observeBlockApplyBatch(1, 10*time.Millisecond)
	m.observeBlockApplyBatch(50, 200*time.Millisecond)

	count, sum := blockApplyBatchLatencySample(t, &m)
	assert.Equal(t, uint64(2), count)
	assert.InDelta(t, 0.210, sum, 0.0001)

	sizeCount, sizeSum := blockApplyBatchSizeSample(t, &m)
	assert.Equal(t, uint64(2), sizeCount)
	assert.InDelta(t, 51.0, sizeSum, 0.0001, "1 + 50 blocks observed")
}

// TestObserveBlockApplyBatchNoopWhenMetricsDisabled confirms the observe
// helper is safe to call on a *stateMetrics that was never initialized
// (metrics disabled), matching the other observe helpers in this file.
func TestObserveBlockApplyBatchNoopWhenMetricsDisabled(t *testing.T) {
	t.Parallel()

	var m stateMetrics
	// m.init is never called: blockApplyBatchLatency and friends stay nil.
	m.observeBlockApplyBatch(1, time.Millisecond)
}

// TestBlockApplyBatchLatencyMaxDurationExportsRunningMaximum drives the
// exported max gauge through observeBlockApplyBatch: it must start at zero,
// rise on a larger observation, and hold (not fall) on a smaller one.
func TestBlockApplyBatchLatencyMaxDurationExportsRunningMaximum(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	var m stateMetrics
	m.init(reg)

	assert.Equal(
		t,
		0.0,
		blockApplyBatchMaxLatencyValue(t, reg),
		"must export zero before any observation",
	)

	m.observeBlockApplyBatch(1, 5*time.Second)
	assert.Equal(t, 5.0, blockApplyBatchMaxLatencyValue(t, reg))

	m.observeBlockApplyBatch(1, 2*time.Second)
	assert.Equal(
		t,
		5.0,
		blockApplyBatchMaxLatencyValue(t, reg),
		"a smaller observation after a larger one must not lower the record",
	)

	m.observeBlockApplyBatch(1, 9*time.Second)
	assert.Equal(
		t,
		9.0,
		blockApplyBatchMaxLatencyValue(t, reg),
		"a larger observation must raise the record",
	)
}

// TestBlockApplyBatchLatencyMaxDurationExportedValueEqualsRecord hammers the
// running maximum from many goroutines at once and requires the exported
// GaugeFunc value to equal the largest duration any writer submitted --
// exactly, not merely bounded by it. Exact equality is what distinguishes
// reading the running-maximum atomic at scrape time from pushing each new
// maximum into a separately-updated Gauge: with a pushed Gauge, two writers
// can each win the compare-and-swap in updateMaxDuration and then land their
// Set calls in the other order, leaving the exported value below the record
// with nothing to recover it (see updateMaxDuration's doc comment). Run with
// -race, which also covers the compare-and-swap loop under concurrent
// writers.
func TestBlockApplyBatchLatencyMaxDurationExportedValueEqualsRecord(
	t *testing.T,
) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	var m stateMetrics
	m.init(reg)

	const writers = 64
	var wg sync.WaitGroup
	wg.Add(writers)
	for i := range writers {
		// Each writer submits a larger value and then a smaller one, so
		// both directions race against every other writer.
		observed := time.Duration(i) * time.Millisecond
		go func() {
			defer wg.Done()
			m.observeBlockApplyBatch(1, observed)
			m.observeBlockApplyBatch(1, observed/2)
		}()
	}
	wg.Wait()

	want := (time.Duration(writers-1) * time.Millisecond).Seconds()
	assert.Equal(
		t,
		want,
		blockApplyBatchMaxLatencyValue(t, reg),
		"the exported value must equal the largest observed duration, "+
			"regardless of goroutine interleaving",
	)
}

// TestLedgerProcessBlocksFromSourceObservesBlockApplyBatchLatency drives one
// real block through the actual ledgerProcessBlocksFromSource batch-apply
// loop (the call site observeBlockApplyBatch was added to in state.go), not
// just the isolated observe helper, so this proves the instrumentation is
// actually wired into the production apply path rather than merely present
// as a metric type. Reuses newByronShelleyBoundaryLedger's harness (real
// LedgerState, real sqlite-backed database, a real decodable Shelley block)
// from byron_shelley_boundary_test.go: applying firstShelley there commits
// exactly one block through the same DB-transaction chunk this metric times.
func TestLedgerProcessBlocksFromSourceObservesBlockApplyBatchLatency(
	t *testing.T,
) {
	t.Parallel()

	ls, _, firstShelley := newByronShelleyBoundaryLedger(t)

	count, _ := blockApplyBatchLatencySample(t, &ls.metrics)
	require.Zero(t, count, "no batch applied yet")

	results := make(chan readChainResult, 1)
	results <- readChainResult{blocks: []gledger.Block{firstShelley}}
	close(results)

	require.NoError(t, ls.ledgerProcessBlocksFromSource(
		context.Background(),
		results,
	))
	require.Equal(
		t,
		firstShelley.SlotNumber(),
		ls.currentTip.Point.Slot,
		"the block must have actually committed and advanced the tip",
	)

	latencyCount, latencySum := blockApplyBatchLatencySample(t, &ls.metrics)
	require.Equal(
		t,
		uint64(1),
		latencyCount,
		"applying one batch must record exactly one latency observation",
	)
	assert.Greater(
		t,
		latencySum,
		0.0,
		"a real DB-backed apply must take measurable wall-clock time",
	)

	sizeCount, sizeSum := blockApplyBatchSizeSample(t, &ls.metrics)
	require.Equal(t, uint64(1), sizeCount)
	assert.Equal(
		t,
		1.0,
		sizeSum,
		"the committed chunk contained exactly one block",
	)
}
