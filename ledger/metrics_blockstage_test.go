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
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// blockStageSampleCount returns the sample count and sum recorded under the
// given stage label of dingo_ledger_block_stage_duration_seconds.
func blockStageSampleCount(
	t *testing.T,
	m *stateMetrics,
	stage string,
) (count uint64, sum float64) {
	t.Helper()
	metric := &dto.Metric{}
	obs, ok := m.blockStageDuration.WithLabelValues(stage).(prometheus.Histogram)
	require.True(t, ok, "stage observer must be a prometheus.Histogram")
	require.NoError(t, obs.Write(metric))
	return metric.GetHistogram().GetSampleCount(), metric.GetHistogram().
		GetSampleSum()
}

// TestObserveBlockStageRecordsUnderEachLabel is the regression test for the
// per-block stage histogram wiring: each of the four known stages must
// record its own duration sample under its own label, so a dashboard can
// break down where per-block ledger time goes.
func TestObserveBlockStageRecordsUnderEachLabel(t *testing.T) {
	t.Parallel()

	var m stateMetrics
	m.init(prometheus.NewRegistry())

	m.observeBlockStage(blockStageHeaderVerify, 10*time.Millisecond)
	m.observeBlockStage(blockStageValidate, 20*time.Millisecond)
	m.observeBlockStage(blockStageApply, 30*time.Millisecond)
	m.observeBlockStage(blockStageValidate, 5*time.Millisecond)
	m.observeBlockStage(blockStageEpochRollover, 40*time.Second)

	count, _ := blockStageSampleCount(t, &m, blockStageHeaderVerify)
	assert.Equal(t, uint64(1), count)
	count, sum := blockStageSampleCount(t, &m, blockStageValidate)
	assert.Equal(t, uint64(2), count)
	assert.InDelta(t, 0.025, sum, 0.0001)
	count, _ = blockStageSampleCount(t, &m, blockStageApply)
	assert.Equal(t, uint64(1), count)
	count, _ = blockStageSampleCount(t, &m, blockStageEpochRollover)
	assert.Equal(t, uint64(1), count)

	// Four distinct label series, no more.
	assert.Equal(
		t,
		4,
		testutil.CollectAndCount(m.blockStageDuration),
	)
}

// TestObserveBlockStageIgnoresUnknownStage confirms an unrecognized stage
// label is silently dropped rather than panicking or creating a stray label
// series. init pre-materializes the four known stage series, so this
// checks their sample counts stay zero rather than the series count, which
// is already 4 regardless of any observation.
func TestObserveBlockStageIgnoresUnknownStage(t *testing.T) {
	t.Parallel()

	var m stateMetrics
	m.init(prometheus.NewRegistry())

	m.observeBlockStage("not_a_real_stage", time.Millisecond)

	assert.Equal(
		t,
		4,
		testutil.CollectAndCount(m.blockStageDuration),
		"init pre-materializes exactly the four known stage series",
	)
	for _, stage := range []string{
		blockStageHeaderVerify,
		blockStageValidate,
		blockStageApply,
		blockStageEpochRollover,
	} {
		count, _ := blockStageSampleCount(t, &m, stage)
		assert.Zerof(
			t, count,
			"stage %q must not have recorded an unknown-stage observation",
			stage,
		)
	}
}

// TestObserveBlockStageNoopWhenMetricsDisabled confirms the observe helper is
// safe to call on a *stateMetrics that was never initialized (metrics
// disabled), matching the other observe helpers in this file.
func TestObserveBlockStageNoopWhenMetricsDisabled(t *testing.T) {
	t.Parallel()

	var m stateMetrics
	// m.init is never called: blockStageDuration and friends stay nil.
	m.observeBlockStage(blockStageHeaderVerify, time.Millisecond)
}

// TestBlockStageDurationBucketsCoverTailStalls pins the histogram's upper
// range to the epoch-boundary stalls it has to resolve.
// blinklabs-io/dingo#4364 measured block application blocked for 25s to 318s
// across preview boundaries; with the old ExponentialBuckets(0.0001, 2, 16)
// ceiling of ~3.3s every one of those landed in +Inf, indistinguishable from
// each other.
func TestBlockStageDurationBucketsCoverTailStalls(t *testing.T) {
	t.Parallel()

	var m stateMetrics
	m.init(prometheus.NewRegistry())

	metric := &dto.Metric{}
	obs, ok := m.blockStageDuration.WithLabelValues(blockStageApply).(prometheus.Histogram)
	require.True(t, ok, "stage observer must be a prometheus.Histogram")
	require.NoError(t, obs.Write(metric))

	buckets := metric.GetHistogram().GetBucket()
	require.NotEmpty(
		t,
		buckets,
		"histogram must have at least one finite bucket boundary",
	)
	largest := buckets[len(buckets)-1].GetUpperBound()
	assert.GreaterOrEqual(
		t,
		largest,
		318.0,
		"largest finite bucket boundary (%vs) must resolve the 318s "+
			"epoch-boundary stall measured in #4364",
		largest,
	)
}

// blockStageMaxDurationValue returns the value reg exports for the given
// stage label of dingo_ledger_block_stage_max_duration_seconds. It reads the
// registry rather than a collector handle held on stateMetrics, because the
// metric deliberately keeps no exported-value state of its own to hold: the
// three GaugeFunc collectors read the running-maximum atomics at scrape time.
func blockStageMaxDurationValue(
	t *testing.T,
	reg *prometheus.Registry,
	stage string,
) float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "dingo_ledger_block_stage_max_duration_seconds" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "stage" &&
					label.GetValue() == stage {
					return metric.GetGauge().GetValue()
				}
			}
		}
	}
	t.Fatalf(
		"no dingo_ledger_block_stage_max_duration_seconds series for stage %q",
		stage,
	)
	return 0
}

// TestBlockStageMaxDurationExportsRunningMaximum drives the exported metric
// through observeBlockStage: it must start at zero, rise on a larger
// observation, hold on a smaller one, and move only the stage that was
// observed.
func TestBlockStageMaxDurationExportsRunningMaximum(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	var m stateMetrics
	m.init(reg)

	for _, stage := range []string{
		blockStageHeaderVerify,
		blockStageValidate,
		blockStageApply,
		blockStageEpochRollover,
	} {
		assert.Equal(
			t,
			0.0,
			blockStageMaxDurationValue(t, reg, stage),
			"stage %q must export zero before any observation",
			stage,
		)
	}

	m.observeBlockStage(blockStageApply, 5*time.Second)
	assert.Equal(t, 5.0, blockStageMaxDurationValue(t, reg, blockStageApply))

	m.observeBlockStage(blockStageApply, 2*time.Second)
	assert.Equal(
		t,
		5.0,
		blockStageMaxDurationValue(t, reg, blockStageApply),
		"a smaller observation after a larger one must not lower the record",
	)

	m.observeBlockStage(blockStageApply, 9*time.Second)
	assert.Equal(
		t,
		9.0,
		blockStageMaxDurationValue(t, reg, blockStageApply),
		"a larger observation must raise the record",
	)

	for _, stage := range []string{
		blockStageHeaderVerify,
		blockStageValidate,
		blockStageEpochRollover,
	} {
		assert.Equal(
			t,
			0.0,
			blockStageMaxDurationValue(t, reg, stage),
			"observing %q must not move stage %q",
			blockStageApply,
			stage,
		)
	}
}

// TestBlockStageMaxDurationExportedValueEqualsRecord hammers one stage from
// many goroutines at once and requires the exported value to equal the
// largest duration any writer submitted -- exactly, not merely to be bounded
// by it.
//
// Exact equality is the point. It is what distinguishes reading the
// running-maximum atomic at scrape time from pushing each new maximum into a
// Gauge: with a pushed Gauge two writers can each win the compare-and-swap
// and land their Set calls in the other order, leaving the exported value
// below the record with nothing to recover it until an observation beats the
// record itself (see updateMaxDuration). Run with -race, which also covers
// the compare-and-swap loop under concurrent writers.
func TestBlockStageMaxDurationExportedValueEqualsRecord(t *testing.T) {
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
			m.observeBlockStage(blockStageApply, observed)
			m.observeBlockStage(blockStageApply, observed/2)
		}()
	}
	wg.Wait()

	want := (time.Duration(writers-1) * time.Millisecond).Seconds()
	assert.Equal(
		t,
		want,
		blockStageMaxDurationValue(t, reg, blockStageApply),
		"the exported value must equal the largest observed duration, "+
			"regardless of goroutine interleaving",
	)
}
