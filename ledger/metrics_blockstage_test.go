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
	"math"
	"sync"
	"sync/atomic"
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
// per-block stage histogram wiring: each of the three known stages must
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

	count, _ := blockStageSampleCount(t, &m, blockStageHeaderVerify)
	assert.Equal(t, uint64(1), count)
	count, sum := blockStageSampleCount(t, &m, blockStageValidate)
	assert.Equal(t, uint64(2), count)
	assert.InDelta(t, 0.025, sum, 0.0001)
	count, _ = blockStageSampleCount(t, &m, blockStageApply)
	assert.Equal(t, uint64(1), count)

	// Three distinct label series, no more.
	assert.Equal(
		t,
		3,
		testutil.CollectAndCount(m.blockStageDuration),
	)
}

// TestObserveBlockStageIgnoresUnknownStage confirms an unrecognized stage
// label is silently dropped rather than panicking or creating a stray label
// series. init pre-materializes the three known stage series, so this
// checks their sample counts stay zero rather than the series count, which
// is already 3 regardless of any observation.
func TestObserveBlockStageIgnoresUnknownStage(t *testing.T) {
	t.Parallel()

	var m stateMetrics
	m.init(prometheus.NewRegistry())

	m.observeBlockStage("not_a_real_stage", time.Millisecond)

	assert.Equal(
		t,
		3,
		testutil.CollectAndCount(m.blockStageDuration),
		"init pre-materializes exactly the three known stage series",
	)
	for _, stage := range []string{
		blockStageHeaderVerify,
		blockStageValidate,
		blockStageApply,
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

// TestBlockStageDurationBucketsCoverTailStalls is the regression test for the
// blinklabs-io/dingo#4364 observability gap: with the old
// ExponentialBuckets(0.0001, 2, 16) config the largest finite bucket
// boundary was ~3.2768s, so a multi-second epoch-boundary stall (#4364
// measured 4-8s live) landed in the +Inf overflow bucket, indistinguishable
// from a 4s, 8s, or 60s stall. The new range must comfortably clear the
// thresholds at which the rest of the system already treats a stall as
// "something is wrong" -- blockfetchBusyTimeout and
// noProgressStuckBackoffMax, both 30s (chainsync.go/state.go) -- or a stall
// large enough to matter is still unresolvable here.
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
		30.0,
		"largest finite bucket boundary (%vs) must be at least as large as "+
			"blockfetchBusyTimeout/noProgressStuckBackoffMax (30s), or a "+
			"stall the rest of the system already treats as stuck still "+
			"lands in +Inf here",
		largest,
	)
}

// TestUpdateMaxDurationTracksRunningMaximum drives updateMaxDuration
// sequentially: it must start unset (zero), move up on a larger
// observation, and hold on a smaller one.
func TestUpdateMaxDurationTracksRunningMaximum(t *testing.T) {
	t.Parallel()

	var max atomic.Uint64
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "test_block_stage_max_duration_seconds",
	})

	assert.Equal(
		t,
		0.0,
		testutil.ToFloat64(gauge),
		"gauge must start at zero before any observation",
	)

	updateMaxDuration(&max, gauge, 5.0)
	assert.Equal(t, 5.0, testutil.ToFloat64(gauge))

	updateMaxDuration(
		&max,
		gauge,
		2.0,
	) // smaller: must not move the record down
	assert.Equal(
		t,
		5.0,
		testutil.ToFloat64(gauge),
		"a smaller observation after a larger one must not decrease the record",
	)

	updateMaxDuration(&max, gauge, 9.0) // larger: moves the record up
	assert.Equal(t, 9.0, testutil.ToFloat64(gauge))
}

// TestUpdateMaxDurationConcurrentWritersNeverLowerTheRecord hammers
// updateMaxDuration from many goroutines at once (run with -race: the
// point is to prove the atomic.Uint64 compare-and-swap loop is actually
// race-free under concurrent writers, not just correct single-threaded).
// Each writer submits a larger value and then a smaller one, so both
// directions are exercised concurrently against every other writer.
//
// The assertion is on the atomic record itself, not a snapshot of the
// exposed gauge: two writers can each win a CAS in close succession and
// race their gauge.Set calls in the other order (documented on
// updateMaxDuration), so the gauge is only asserted never to exceed the
// true record, not to equal it exactly at this specific instant. The
// atomic record is the correctness invariant the CAS loop actually
// guarantees.
func TestUpdateMaxDurationConcurrentWritersNeverLowerTheRecord(t *testing.T) {
	t.Parallel()

	var max atomic.Uint64
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "test_concurrent_block_stage_max_duration_seconds",
	})

	const writers = 64
	var wg sync.WaitGroup
	wg.Add(writers)
	for i := range writers {
		observed := float64(i)
		go func() {
			defer wg.Done()
			updateMaxDuration(&max, gauge, observed)
			updateMaxDuration(&max, gauge, observed/2)
		}()
	}
	wg.Wait()

	gotMax := math.Float64frombits(max.Load())
	assert.Equal(
		t,
		float64(writers-1),
		gotMax,
		"the atomic record must equal the largest value any writer observed, "+
			"regardless of goroutine interleaving",
	)
	exposed := testutil.ToFloat64(gauge)
	assert.LessOrEqual(
		t,
		exposed,
		gotMax,
		"the exposed gauge must never show a value above the true record",
	)
	assert.GreaterOrEqual(t, exposed, 0.0)
}
