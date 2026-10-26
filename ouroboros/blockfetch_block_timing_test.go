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
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/internal/leiosheader"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// gatherRecentBlockSlots returns idx -> RB slot for the block-slot metric.
func gatherRecentBlockSlots(
	t *testing.T,
	reg *prometheus.Registry,
) map[string]float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	out := map[string]float64{}
	for _, mf := range families {
		if mf.GetName() != recentBlockSlotMetricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			require.Len(t, m.GetLabel(), 1)
			require.Equal(t, "idx", m.GetLabel()[0].GetName())
			out[m.GetLabel()[0].GetValue()] = m.GetGauge().GetValue()
		}
	}
	return out
}

// The RB slot is exported in the same ring slot as the delay, replaced
// together with it, and not exported for an entry recorded without one.
func TestRecentBlockDelaysExportsBlockSlotPerSlot(t *testing.T) {
	t.Parallel()
	r, reg := newTestRecentDelays(t)
	require.Empty(t, gatherRecentBlockSlots(t, reg))

	idx := func(n uint64) string {
		return strconv.FormatUint(n%recentBlockDelaySlots, 10)
	}
	r.record(100, testBlockHash(1), 0.3, 0.3, withBlockSlot(5000))
	r.record(101, testBlockHash(2), 0.3, 0.3, withBlockSlot(5003))
	r.record(102, testBlockHash(3), 0.3, 0.3) // no slot recorded
	require.Equal(t, map[string]float64{
		idx(100): 5000,
		idx(101): 5003,
	}, gatherRecentBlockSlots(t, reg))

	// A different block at the same height replaces the slot with it; a
	// repeat of the same block keeps the first delivery's slot.
	r.record(100, testBlockHash(9), 0.5, 0.5, withBlockSlot(5001))
	r.record(101, testBlockHash(2), 0.9, 0.9, withBlockSlot(9999))
	require.Equal(t, map[string]float64{
		idx(100): 5001,
		idx(101): 5003,
	}, gatherRecentBlockSlots(t, reg))

	// A replacement without a slot must not inherit the old entry's slot.
	r.record(101, testBlockHash(7), 0.9, 0.9)
	require.NotContains(t, gatherRecentBlockSlots(t, reg), idx(101))
}

func gatherBlockDelayHistograms(
	t *testing.T,
	reg *prometheus.Registry,
) map[string]*dto.Histogram {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	out := map[string]*dto.Histogram{}
	for _, mf := range families {
		if mf.GetName() != blockDelayByKindMetricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			require.Len(t, m.GetLabel(), 1)
			require.Equal(t, "kind", m.GetLabel()[0].GetName())
			out[m.GetLabel()[0].GetValue()] = m.GetHistogram()
		}
	}
	return out
}

func bucketCount(h *dto.Histogram, le float64) (uint64, bool) {
	for _, b := range h.GetBucket() {
		if b.GetUpperBound() == le {
			return b.GetCumulativeCount(), true
		}
	}
	return 0, false
}

// Each at-tip block observes its delay into the histogram series of its
// kind, the same value that goes into the ring, with exact bucket edges at
// the thresholds dashboards read.
func TestBlockfetchBlockDelayHistogramByKind(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{PromRegistry: reg})
	m := o.blockfetchMetrics
	require.NotNil(t, m)

	both := leiosheader.KindAnnouncesEB | leiosheader.KindCertifiesEB
	m.recordAtTipBlock(100, 5000, testBlockHash(1), 0, 0.25, 0.2)
	m.recordAtTipBlock(101, 5003, testBlockHash(2), 0, 4.0, 0.2)
	m.recordAtTipBlock(
		102, 5004, testBlockHash(3), leiosheader.KindAnnouncesEB, 1.0, 0.2,
	)
	m.recordAtTipBlock(
		103, 5005, testBlockHash(4), leiosheader.KindCertifiesEB, 5.0, 0.2,
	)
	m.recordAtTipBlock(104, 5006, testBlockHash(5), both, 44.0, 0.2)

	got := gatherBlockDelayHistograms(t, reg)
	require.Len(t, got, 4)
	counts := map[string]uint64{}
	sums := map[string]float64{}
	for kind, h := range got {
		counts[kind] = h.GetSampleCount()
		sums[kind] = h.GetSampleSum()
	}
	require.Equal(t, map[string]uint64{
		"plain": 2, "announce": 1, "certify": 1, "announce_certify": 1,
	}, counts)
	require.Equal(t, map[string]float64{
		"plain": 4.25, "announce": 1.0, "certify": 5.0,
		"announce_certify": 44.0,
	}, sums)

	// Exact bucket edges.
	var edges []float64
	for _, b := range got["plain"].GetBucket() {
		edges = append(edges, b.GetUpperBound())
	}
	require.Equal(t, []float64{
		0.1, 0.25, 0.5, 1, 2, 3, 5, 7.5, 10, 15, 20, 30, 45, 60, 90, 120, 300,
	}, edges)

	// A value exactly on an edge counts in that bucket (le is inclusive),
	// so dashboards reading le="1"/"5" see 1.0s and 5.0s as within.
	c, ok := bucketCount(got["announce"], 1)
	require.True(t, ok)
	require.Equal(t, uint64(1), c)
	c, ok = bucketCount(got["certify"], 3)
	require.True(t, ok)
	require.Equal(t, uint64(0), c)
	c, ok = bucketCount(got["certify"], 5)
	require.True(t, ok)
	require.Equal(t, uint64(1), c)
	c, ok = bucketCount(got["plain"], 0.25)
	require.True(t, ok)
	require.Equal(t, uint64(1), c)

	// The ring received the same delay and slot for each block.
	delays := gatherRecentDelays(t, reg)
	require.InDelta(
		t, 44.0, delays[strconv.FormatUint(104%recentBlockDelaySlots, 10)][1],
		1e-9,
	)
	require.Equal(
		t, 5006.0,
		gatherRecentBlockSlots(t, reg)[strconv.FormatUint(
			104%recentBlockDelaySlots, 10)],
	)
}

// Every kind label exists before any block is seen, so a rate over a kind
// that has not occurred yet is zero rather than absent.
func TestBlockfetchBlockDelayHistogramSeedsAllKinds(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	_ = newOuroboros(OuroborosConfig{PromRegistry: reg})
	got := gatherBlockDelayHistograms(t, reg)
	require.Len(t, got, 4)
	for _, kind := range []string{
		"plain", "announce", "certify", "announce_certify",
	} {
		require.Contains(t, got, kind)
		require.Zero(t, got[kind].GetSampleCount())
	}
}

func TestBlockDelayKindLabel(t *testing.T) {
	t.Parallel()
	require.Equal(t, "plain", blockDelayKindLabel(0))
	require.Equal(
		t, "announce", blockDelayKindLabel(leiosheader.KindAnnouncesEB),
	)
	require.Equal(
		t, "certify", blockDelayKindLabel(leiosheader.KindCertifiesEB),
	)
	require.Equal(t, "announce_certify", blockDelayKindLabel(3))
	// Unknown high bits do not index out of range.
	require.Equal(t, "plain", blockDelayKindLabel(0xfc))
}
