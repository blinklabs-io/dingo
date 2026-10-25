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
	"time"

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// headerArrivalRing returns block number -> {delay, slot} from a scrape.
func headerArrivalRing(
	t *testing.T,
	reg *prometheus.Registry,
) map[uint64][2]float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	byIdx := map[string]map[string]float64{}
	for _, mf := range families {
		for _, m := range mf.GetMetric() {
			var idx string
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "idx" {
					idx = lp.GetValue()
				}
			}
			if idx == "" {
				continue
			}
			if byIdx[idx] == nil {
				byIdx[idx] = map[string]float64{}
			}
			byIdx[idx][mf.GetName()] = m.GetGauge().GetValue()
		}
	}
	out := map[uint64][2]float64{}
	for _, v := range byIdx {
		out[uint64(v[recentHeaderBlockNumberMetricName])] = [2]float64{
			v[recentHeaderDelayMetricName],
			v[recentHeaderBlockSlotMetricName],
		}
	}
	return out
}

func headerArrivalCount(t *testing.T, reg *prometheus.Registry) uint64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() == headerArrivalDelayMetricName {
			return mf.GetMetric()[0].GetHistogram().GetSampleCount()
		}
	}
	t.Fatalf("%s not registered", headerArrivalDelayMetricName)
	return 0
}

func newTestHeaderArrivals(
	t *testing.T,
) (*headerArrivalMetrics, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{PromRegistry: reg})
	require.NotNil(t, o.headerArrivalMetrics)
	return o.headerArrivalMetrics, reg
}

// The first header for a block is recorded against its slot time; the same
// header from another peer later keeps the first arrival and is not counted
// again.
func TestHeaderArrivalKeepsFirstArrivalPerBlock(t *testing.T) {
	t.Parallel()
	m, reg := newTestHeaderArrivals(t)
	slotTime := time.Unix(1_700_000_000, 0)

	m.observeHeaderArrival(
		true, 500, 9000, testBlockHash(1), slotTime,
		slotTime.Add(800*time.Millisecond),
	)
	m.observeHeaderArrival(
		true, 500, 9000, testBlockHash(1), slotTime,
		slotTime.Add(2*time.Second),
	)

	got := headerArrivalRing(t, reg)
	require.Contains(t, got, uint64(500))
	assert.InDelta(t, 0.8, got[500][0], 1e-9)
	assert.InDelta(t, 9000, got[500][1], 1e-9)
	assert.Equal(t, uint64(1), headerArrivalCount(t, reg))
}

// A different block at the same height (a fork) replaces the entry and is
// counted; a lower block landing in a slot that already holds a newer one is
// stale and dropped.
func TestHeaderArrivalForkReplacesAndStaleIsDropped(t *testing.T) {
	t.Parallel()
	m, reg := newTestHeaderArrivals(t)
	slotTime := time.Unix(1_700_000_000, 0)

	m.observeHeaderArrival(
		true, 600, 9100, testBlockHash(1), slotTime,
		slotTime.Add(time.Second),
	)
	m.observeHeaderArrival(
		true, 600, 9101, testBlockHash(2), slotTime,
		slotTime.Add(1500*time.Millisecond),
	)
	m.observeHeaderArrival(
		true, 600-recentHeaderArrivalSlots, 8900, testBlockHash(3), slotTime,
		slotTime.Add(100*time.Millisecond),
	)

	got := headerArrivalRing(t, reg)
	require.Len(t, got, 1)
	assert.InDelta(t, 1.5, got[600][0], 1e-9)
	assert.InDelta(t, 9101, got[600][1], 1e-9)
	assert.Equal(t, uint64(2), headerArrivalCount(t, reg))
}

// Catch-up headers are naturally late, so nothing is recorded before the node
// reaches tip, and backfilled headers well past their slot are skipped too.
func TestHeaderArrivalSkipsCatchUpAndBackfill(t *testing.T) {
	t.Parallel()
	m, reg := newTestHeaderArrivals(t)
	slotTime := time.Unix(1_700_000_000, 0)

	m.observeHeaderArrival(
		false, 700, 9200, testBlockHash(1), slotTime,
		slotTime.Add(time.Second),
	)
	m.observeHeaderArrival(
		true, 701, 9201, testBlockHash(2), slotTime,
		slotTime.Add(10*time.Minute),
	)

	assert.Empty(t, headerArrivalRing(t, reg))
	assert.Equal(t, uint64(0), headerArrivalCount(t, reg))
}

// A node built without a registry has no header arrival metrics, and the
// call site must not panic.
func TestHeaderArrivalNilMetricsIsSafe(t *testing.T) {
	t.Parallel()
	var m *headerArrivalMetrics
	assert.NotPanics(t, func() {
		m.observeHeaderArrival(
			true, 1, 1, testBlockHash(1), time.Unix(0, 0), time.Unix(1, 0),
		)
	})
}

// The chainsync roll-forward path records the header's arrival against its
// slot time once the node is at tip.
func TestChainsyncRollForwardObservesHeaderArrival(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	o := newOuroboros(OuroborosConfig{
		EventBus:     bus,
		PromRegistry: reg,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	require.NotNil(t, o.headerArrivalMetrics)
	slotStart := time.Now().Add(-1200 * time.Millisecond)
	o.headerArrivalMetrics.slotClock = func(uint64) (time.Time, bool, bool) {
		return slotStart, true, true
	}
	connID := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	header := newTestBlockHeader(100, 7, 0xab)
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connID},
		0,
		header,
		ochainsync.Tip{
			Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
			BlockNumber: 7,
		},
	))

	got := headerArrivalRing(t, reg)
	require.Contains(t, got, uint64(7))
	assert.GreaterOrEqual(t, got[7][0], 1.2)
	assert.Less(t, got[7][0], 5.0)
	assert.InDelta(t, 100, got[7][1], 1e-9)
}
