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

package mithril

import (
	"context"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func newTestPipelineMetrics(t *testing.T) *PipelineMetrics {
	t.Helper()
	m, err := NewPipelineMetrics(prometheus.NewRegistry())
	require.NoError(t, err)
	require.NotNil(t, m)
	return m
}

func histogramCount(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()
	var out dto.Metric
	require.NoError(t, h.Write(&out))
	return out.GetHistogram().GetSampleCount()
}

func TestPipelineMetricsNilRegistry(t *testing.T) {
	t.Parallel()
	m, err := NewPipelineMetrics(nil)
	require.NoError(t, err)
	require.Nil(t, m)
	// A nil value records nothing and must not panic.
	m.setWindowOccupancy(1)
	m.chunkDownloaded()
	m.chunkApplied()
	m.observeBatchCommit(0)
	m.staleRestart()
}

func TestPipelineMetricsReuseRegistry(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	first, err := NewPipelineMetrics(reg)
	require.NoError(t, err)
	second, err := NewPipelineMetrics(reg)
	require.NoError(t, err)
	second.chunkDownloaded()
	require.InDelta(t, 1.0,
		promtestutil.ToFloat64(first.chunksDownloaded), 0)
}

func TestPipelineMetricsWindowOccupancy(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 4, nil)
	cfg := feedConfig(fixture, t.TempDir(), 0, 2, 2)
	m := newTestPipelineMetrics(t)
	cfg.Bootstrap.Metrics = m
	feed := openFeed(t, t.Context(), cfg)

	occupancy := func() float64 {
		return promtestutil.ToFloat64(m.windowOccupancy)
	}
	testutil.WaitForCondition(t, func() bool { return occupancy() == 2 },
		testutil.AsyncWait, "window fills")
	chunk, err := feed.Next(nextCtx(t))
	require.NoError(t, err)
	require.NoError(t, feed.Release(chunk.Num))
	// The release frees one slot, which a worker claims at once.
	testutil.WaitForCondition(t, func() bool { return occupancy() == 2 },
		testutil.AsyncWait, "window refills after a release")
	require.NoError(t, feed.Close())
	require.Zero(t, occupancy(), "a closed feed holds no trios")
}

func TestPipelineMetricsStreamCatchUp(t *testing.T) {
	t.Parallel()
	opts := streamFixtureOptions(4, nil)
	fixture := newV2Fixture(t, opts)
	db := newSyncModeTestDB(t)
	cfg := streamConfig(fixture, t.TempDir(), db, &fakeBatchApplier{}, 1, 2)
	m := newTestPipelineMetrics(t)
	cfg.Feed.Bootstrap.Metrics = m

	_, err := RunStreamCatchUp(t.Context(), cfg)
	require.NoError(t, err)

	require.InDelta(t, 5.0, promtestutil.ToFloat64(m.chunksDownloaded), 0)
	require.InDelta(t, 5.0, promtestutil.ToFloat64(m.chunksApplied), 0)
	require.Equal(t, uint64(5), histogramCount(t, m.batchCommitSeconds),
		"one commit per one-chunk batch")
	require.Zero(t, promtestutil.ToFloat64(m.staleRestarts))
	require.Zero(t, promtestutil.ToFloat64(m.windowOccupancy))
}

func TestPipelineMetricsStaleRestart(t *testing.T) {
	t.Parallel()
	opts := streamFixtureOptions(3, nil)
	chain := readStreamChain(t, opts)
	fixture := newV2Fixture(t, opts)
	db := newSyncModeTestDB(t)
	fired := false
	applier := &fakeBatchApplier{}
	applier.beforeApply = func(
		a *fakeBatchApplier, blocks []ledger.Block,
	) error {
		if fired || chain.chunkOf(blockPoint(blocks[0])) != 2 {
			return nil
		}
		fired = true
		a.applyLocked(blocks)
		return fmt.Errorf("fake: competing writer: %w", ErrStaleBatch)
	}
	cfg := streamConfig(fixture, t.TempDir(), db, applier, 1, 2)
	m := newTestPipelineMetrics(t)
	cfg.Feed.Bootstrap.Metrics = m

	res, err := RunStreamCatchUp(t.Context(), cfg)
	require.NoError(t, err)
	require.Equal(t, 1, res.Restarts)
	require.InDelta(t, 1.0, promtestutil.ToFloat64(m.staleRestarts), 0)
}

func TestPipelineMetricsManualBootstrap(t *testing.T) {
	t.Parallel()
	fixture := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 2})
	cfg := fixture.bootstrapConfig(t.TempDir())
	m := newTestPipelineMetrics(t)
	cfg.Metrics = m

	_, err := Bootstrap(context.Background(), cfg)
	require.NoError(t, err)
	require.InDelta(t, 3.0, promtestutil.ToFloat64(m.chunksDownloaded), 0)
	// The manual path's window is unbounded, so it reports no occupancy.
	require.Zero(t, promtestutil.ToFloat64(m.windowOccupancy))

	// A rerun reuses every verified trio in place and downloads nothing.
	_, err = Bootstrap(context.Background(), cfg)
	require.NoError(t, err)
	require.InDelta(t, 3.0, promtestutil.ToFloat64(m.chunksDownloaded), 0)
}
