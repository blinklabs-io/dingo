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
	"testing"

	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// gatherAdoptionRing returns idx -> value for the named ring metric.
func gatherAdoptionRing(
	t *testing.T,
	r *recentAdoptions,
	name string,
) map[string]float64 {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(r))
	families, err := reg.Gather()
	require.NoError(t, err)
	out := map[string]float64{}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			require.Len(t, m.GetLabel(), 1)
			require.Equal(t, "idx", m.GetLabel()[0].GetName())
			out[m.GetLabel()[0].GetValue()] = m.GetGauge().GetValue()
		}
	}
	return out
}

func adoptionHistogramCount(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()
	var out dto.Metric
	require.NoError(t, h.Write(&out))
	return out.GetHistogram().GetSampleCount()
}

func TestRecentAdoptionsRing(t *testing.T) {
	t.Parallel()

	r := newRecentAdoptions()
	require.Empty(t, gatherAdoptionRing(t, r, recentAdoptionDelayMetricName))

	r.record(17, "a", 1.5)
	r.record(18, "b", 2.5)
	delays := gatherAdoptionRing(t, r, recentAdoptionDelayMetricName)
	numbers := gatherAdoptionRing(t, r, recentAdoptionNumberMetricName)
	require.Equal(t, map[string]float64{"1": 1.5, "2": 2.5}, delays)
	require.Equal(t, map[string]float64{"1": 17, "2": 18}, numbers)

	// Same block again keeps the first value.
	r.record(17, "a", 9)
	// A stale lower block never overwrites a newer one in its slot.
	r.record(1, "old", 8)
	// A different block at the same height (fork) replaces it.
	r.record(18, "b2", 3.5)
	// A newer block wrapping the ring replaces the slot.
	r.record(33, "c", 4.5)
	delays = gatherAdoptionRing(t, r, recentAdoptionDelayMetricName)
	numbers = gatherAdoptionRing(t, r, recentAdoptionNumberMetricName)
	require.Equal(t, map[string]float64{"1": 4.5, "2": 3.5}, delays)
	require.Equal(t, map[string]float64{"1": 33, "2": 18}, numbers)
}

func TestAdoptionDelayBucketsSpanHundredMsToTwoMinutes(t *testing.T) {
	t.Parallel()

	var m stateMetrics
	m.init(nil)
	m.observeBlockAdoption(5, []byte{1}, 0.05)
	m.observeBlockAdoption(6, []byte{2}, 44)
	m.observeBlockAdoption(7, []byte{3}, 500)
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(m.blockAdoptionDelay))
	families, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	h := families[0].GetMetric()[0].GetHistogram()
	require.EqualValues(t, 3, h.GetSampleCount())
	buckets := h.GetBucket()
	require.InDelta(t, 0.1, buckets[0].GetUpperBound(), 1e-9)
	require.InDelta(t, 120, buckets[len(buckets)-1].GetUpperBound(), 1e-9)
}

// TestBlockAdoptionRecordedOnlyAtTip applies one real block through the
// pipeline and checks the adoption metrics are written when the ledger is at
// tip and left untouched during catch-up.
func TestBlockAdoptionRecordedOnlyAtTip(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		atTip bool
	}{
		{name: "at tip", atTip: true},
		{name: "catch-up", atTip: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ls, _, firstShelley := newByronShelleyBoundaryLedger(t)
			if tc.atTip {
				ls.reachedTip.Store(true)
			}
			results := make(chan readChainResult, 1)
			results <- readChainResult{
				blocks: []gledger.Block{firstShelley},
			}
			close(results)
			require.NoError(t, ls.ledgerProcessBlocksFromSource(
				context.Background(),
				results,
			))

			wantCount := 0
			if tc.atTip {
				wantCount = 1
			}
			require.EqualValues(
				t,
				wantCount,
				adoptionHistogramCount(t, ls.metrics.blockAdoptionDelay),
			)
			numbers := gatherAdoptionRing(
				t,
				ls.metrics.recentAdoptions,
				recentAdoptionNumberMetricName,
			)
			delays := gatherAdoptionRing(
				t,
				ls.metrics.recentAdoptions,
				recentAdoptionDelayMetricName,
			)
			if !tc.atTip {
				require.Empty(t, numbers)
				require.Empty(t, delays)
				return
			}
			idx := "0"
			for k, v := range numbers {
				idx = k
				require.EqualValues(t, firstShelley.BlockNumber(), v)
			}
			require.Len(t, numbers, 1)
			// Mainnet slot time is years in the past, so the delay is large
			// and positive.
			require.Greater(t, delays[idx], 1e6)
		})
	}
}
