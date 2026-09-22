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

package ouroboros

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
)

// TestBlockfetchClientBlockRawObservesDecodeStageOnCacheMissOnly is the
// regression test for the decode-stage histogram wired into
// blockfetchClientBlockRaw: the decode callback given to the shared cache is
// only invoked on a cache miss, so dingo_blockfetch_stage_duration_seconds
// must record exactly one "decode" sample for two deliveries of identical
// bytes, not two -- the second delivery is served from cache and does no
// decode work of its own.
func TestBlockfetchClientBlockRawObservesDecodeStageOnCacheMissOnly(
	t *testing.T,
) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{PromRegistry: prometheus.NewRegistry()})
	blockType, raw := conwayBlockFixtureBytes(t)
	ctx := blockfetch.CallbackContext{}

	require.NoError(t, o.blockfetchClientBlockRaw(ctx, blockType, raw))
	require.NoError(t, o.blockfetchClientBlockRaw(ctx, blockType, raw))

	metric := &dto.Metric{}
	require.NoError(
		t,
		o.blockfetchMetrics.stageDecode.(prometheus.Histogram).Write(metric),
	)
	assert.Equal(
		t,
		uint64(1),
		metric.GetHistogram().GetSampleCount(),
		"only the cache-miss delivery should record a decode observation",
	)
}

// TestBlockfetchClientBlockRawNoopDecodeStageWhenMetricsDisabled confirms the
// decode timing wrapper does not panic when no PromRegistry is configured,
// matching the nil-safe pattern used by the other blockfetch/protocol
// metrics in this package.
func TestBlockfetchClientBlockRawNoopDecodeStageWhenMetricsDisabled(
	t *testing.T,
) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{}) // no PromRegistry -> no metrics
	blockType, raw := conwayBlockFixtureBytes(t)
	ctx := blockfetch.CallbackContext{}

	require.NoError(t, o.blockfetchClientBlockRaw(ctx, blockType, raw))
	assert.Nil(t, o.blockfetchMetrics)
}

// TestBlockfetchStageDurationBucketsCoverTailStalls is the sibling
// regression test to
// ledger.TestBlockStageDurationBucketsCoverTailStalls: dingo_blockfetch_stage_duration_seconds
// shared the identical narrow ExponentialBuckets(0.0001, 2, 16) config
// (largest boundary ~3.2768s) as dingo_ledger_block_stage_duration_seconds,
// which is the metric blinklabs-io/dingo#4364's multi-second epoch-boundary
// stalls overflowed into +Inf. The two histograms are documented as sharing
// the same bucket range so they stay comparable across the same block's
// stages, so this must widen in lockstep.
func TestBlockfetchStageDurationBucketsCoverTailStalls(t *testing.T) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{PromRegistry: prometheus.NewRegistry()})

	metric := &dto.Metric{}
	require.NoError(
		t,
		o.blockfetchMetrics.stageDecode.(prometheus.Histogram).Write(metric),
	)

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
		"largest finite bucket boundary (%vs) must match "+
			"dingo_ledger_block_stage_duration_seconds's widened range",
		largest,
	)
}
