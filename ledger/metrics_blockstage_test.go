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
