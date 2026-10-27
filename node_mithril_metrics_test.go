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

package dingo

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/promutil"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func newTestMithrilNodeMetrics(t *testing.T) *mithrilNodeMetrics {
	t.Helper()
	r := promutil.NewRegistration(prometheus.NewRegistry())
	m := newMithrilNodeMetrics(r)
	require.NoError(t, r.Err())
	return m
}

func decisionCount(m *mithrilNodeMetrics, source string) float64 {
	return promtestutil.ToFloat64(m.decisions.WithLabelValues(source))
}

func fallbackCount(m *mithrilNodeMetrics, reason string) float64 {
	return promtestutil.ToFloat64(m.fallbacks.WithLabelValues(reason))
}

// Both label sets are fixed and every series exists before the first event,
// so the metric's cardinality never grows with what the node sees.
func TestMithrilNodeMetricsFixedLabels(t *testing.T) {
	t.Parallel()
	m := newTestMithrilNodeMetrics(t)
	require.Equal(t, 2, promtestutil.CollectAndCount(m.decisions))
	require.Equal(t, 4, promtestutil.CollectAndCount(m.fallbacks))
	// A nil value records nothing and must not panic.
	var none *mithrilNodeMetrics
	none.decision(mithrilSourceMithril)
	none.fallback(mithrilFallbackProbe)
}

func TestCoordinatorCountsDecisionsAndFallbacks(t *testing.T) {
	t.Parallel()
	l := &fakeMithrilLedger{tip: tipAt(0, 10)}
	h := newCoordinatorHarness(t, l, 0, func(
		context.Context, TrustedBatchApplier,
	) error {
		return errors.New("fake: digest mismatch")
	})
	m := newTestMithrilNodeMetrics(t)
	h.c.metrics = m

	h.src.probeErr = errors.New("fake: aggregator down")
	h.c.cycle(t.Context())
	require.InDelta(t, 1.0, fallbackCount(m, mithrilFallbackProbe), 0)
	require.Zero(t, decisionCount(m, mithrilSourceMithril))
	require.Zero(t, decisionCount(m, mithrilSourceOuroboros))

	// No gap: Ouroboros keeps syncing.
	h.src.mu.Lock()
	h.src.probeErr = nil
	h.src.mu.Unlock()
	h.clock.Advance(h.c.cfg.RetryBackoff)
	h.c.cycle(t.Context())
	require.InDelta(t, 1.0, decisionCount(m, mithrilSourceOuroboros), 0)

	// A gap worth syncing whose run fails.
	h.src.mu.Lock()
	h.src.target = 100
	h.src.mu.Unlock()
	h.clock.Advance(time.Minute)
	h.c.cycle(t.Context())
	require.InDelta(t, 1.0, decisionCount(m, mithrilSourceMithril), 0)
	require.InDelta(t, 1.0, fallbackCount(m, mithrilFallbackCatchUp), 0)
	require.Zero(t, fallbackCount(m, mithrilFallbackSnapshotRestore))
}

func TestSnapshotStartupCountsFallback(t *testing.T) {
	t.Parallel()
	src := &fakeSnapshotSource{
		states: []MithrilSnapshotState{MithrilSnapshotFresh},
		restores: []func(context.Context) (ocommon.Point, error){
			restoreReturns(ocommon.Point{}, errors.New("fake: bad manifest")),
		},
	}
	h := newSnapshotStartupHarness(src, false)
	m := newTestMithrilNodeMetrics(t)
	h.s.metrics = m
	_, err := h.s.run(t.Context())
	require.NoError(t, err)
	require.InDelta(t, 1.0, fallbackCount(m, mithrilFallbackSnapshotRestore), 0)
}

// The Mithril half is built once per node and never rebuilt, so the metrics it
// registers must survive the collector cleanup that precedes every live
// Restore or Truncate rebuild, as must the node's own Mithril metrics.
func TestMithrilMetricsSurviveLiveRebuildCleanup(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	cardanoCfg := newNodeTestCardanoNodeCfg(t)
	n, err := New(NewConfig(
		WithDatabasePath(t.TempDir()),
		WithCardanoNodeConfig(cardanoCfg),
		WithNetworkMagic(cardanoCfg.ShelleyGenesis().NetworkMagic),
		WithPrometheusRegistry(reg),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp",
			ListenAddress: "127.0.0.1:0",
		}),
	))
	require.NoError(t, err)
	n.config.mithrilCatchUp = func(p MithrilCatchUpParams) (MithrilCatchUp, error) {
		// Stands in for nodemithril.New, which registers the pipeline
		// metrics on the registry it is given.
		if err := p.PromRegistry.Register(prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "dingo_mithril_chunks_downloaded_total",
				Help: "test",
			},
		)); err != nil {
			return nil, err
		}
		return &fakeMithrilSource{}, nil
	}
	n.mithrilSource = n.newMithrilSource()
	require.NotNil(t, n.mithrilSource)

	n.rebuildableMetrics.unregisterAll()

	families, err := reg.Gather()
	require.NoError(t, err)
	names := map[string]bool{}
	for _, f := range families {
		names[f.GetName()] = true
	}
	for _, name := range []string{
		"dingo_mithril_chunks_downloaded_total",
		"dingo_mithril_snapshot_restore_state",
		"dingo_mithril_source_decisions_total",
		"dingo_mithril_fallbacks_total",
	} {
		require.True(t, names[name], "%s vanished after the cleanup", name)
	}
}
