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

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gatherRecentDelays scrapes reg and returns, per idx label, the block number
// and delay exported for that ring slot. It fails the test if a slot exports
// only one of the two gauges, since consumers pair them by idx.
func gatherRecentDelays(
	t *testing.T,
	reg *prometheus.Registry,
) map[string][2]float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	delays := map[string]float64{}
	blocks := map[string]float64{}
	for _, mf := range families {
		var dst map[string]float64
		switch mf.GetName() {
		case recentBlockDelayMetricName:
			dst = delays
		case recentBlockNumberMetricName:
			dst = blocks
		default:
			continue
		}
		for _, m := range mf.GetMetric() {
			var idx string
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "idx" {
					idx = lp.GetValue()
				}
			}
			require.NotEmpty(
				t,
				idx,
				"%s sample without idx label",
				mf.GetName(),
			)
			dst[idx] = m.GetGauge().GetValue()
		}
	}
	require.Len(
		t,
		blocks,
		len(delays),
		"delay and block-number slots must match",
	)
	out := make(map[string][2]float64, len(delays))
	for idx, d := range delays {
		b, ok := blocks[idx]
		require.True(t, ok, "idx %s has a delay but no block number", idx)
		out[idx] = [2]float64{b, d}
	}
	return out
}

func testBlockHash(n byte) lcommon.Blake2b256 {
	var h lcommon.Blake2b256
	h[0] = n
	return h
}

func newTestRecentDelays(
	t *testing.T,
) (*recentBlockDelays, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	r := newRecentBlockDelays()
	require.NoError(t, reg.Register(r))
	return r, reg
}

// Unfilled ring slots must not be exported: a zero delay for block 0 would be
// indistinguishable from a real sample on a dashboard.
func TestRecentBlockDelaysExportsNothingUntilRecorded(t *testing.T) {
	t.Parallel()
	_, reg := newTestRecentDelays(t)
	assert.Empty(t, gatherRecentDelays(t, reg))
}

// The reason the ring exists: several blocks recorded between two scrapes are
// all visible in the next scrape, each paired with its own delay.
func TestRecentBlockDelaysKeepsEveryBlockBetweenScrapes(t *testing.T) {
	t.Parallel()
	r, reg := newTestRecentDelays(t)
	r.record(100, testBlockHash(1), 0.31)
	require.Len(t, gatherRecentDelays(t, reg), 1)

	r.record(101, testBlockHash(2), 0.28)
	r.record(102, testBlockHash(3), 3.40)
	r.record(103, testBlockHash(4), 0.45)

	got := gatherRecentDelays(t, reg)
	want := map[uint64]float64{100: 0.31, 101: 0.28, 102: 3.40, 103: 0.45}
	require.Len(t, got, len(want))
	for blockNum, delay := range want {
		idx := strconv.FormatUint(blockNum%recentBlockDelaySlots, 10)
		require.Contains(t, got, idx)
		assert.Equal(t, float64(blockNum), got[idx][0])
		assert.InDelta(t, delay, got[idx][1], 1e-9)
	}
}

// Once full, each new block overwrites the slot of the block N heights below
// it, so exactly the newest N blocks are exported.
func TestRecentBlockDelaysOverwritesOldestWhenFull(t *testing.T) {
	t.Parallel()
	r, reg := newTestRecentDelays(t)
	const first = 1000
	total := recentBlockDelaySlots + 3
	for i := range total {
		n := uint64(first + i)
		r.record(n, testBlockHash(byte(i)), float64(i))
	}

	got := gatherRecentDelays(t, reg)
	require.Len(t, got, recentBlockDelaySlots)
	oldestKept := uint64(first + total - recentBlockDelaySlots)
	for _, v := range got {
		assert.GreaterOrEqual(t, uint64(v[0]), oldestKept)
		assert.InDelta(
			t,
			v[0]-first,
			v[1],
			1e-9,
			"delay must stay paired with its block",
		)
	}
}

// A second delivery of the same block (e.g. from another peer) arrives later
// and would overstate the delay, so the first delivery wins. A different block
// at the same height (a rollback or slot battle) replaces it.
func TestRecentBlockDelaysSameBlockKeepsFirstDelivery(t *testing.T) {
	t.Parallel()
	r, reg := newTestRecentDelays(t)
	idx := strconv.FormatUint(200%recentBlockDelaySlots, 10)

	r.record(200, testBlockHash(7), 0.40)
	r.record(200, testBlockHash(7), 2.50)
	assert.InDelta(t, 0.40, gatherRecentDelays(t, reg)[idx][1], 1e-9)

	r.record(200, testBlockHash(8), 1.10)
	assert.InDelta(t, 1.10, gatherRecentDelays(t, reg)[idx][1], 1e-9)
}

// The ring is wired into the blockfetch metrics and registered with the
// configured registry alongside the cardano-node compatible gauges.
func TestBlockfetchMetricsRegistersRecentBlockDelays(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{PromRegistry: reg})
	require.NotNil(t, o.blockfetchMetrics)
	require.NotNil(t, o.blockfetchMetrics.recentDelays)

	o.blockfetchMetrics.recentDelays.record(5, testBlockHash(1), 0.5)
	got := gatherRecentDelays(t, reg)
	require.Contains(t, got, strconv.FormatUint(5%recentBlockDelaySlots, 10))
}
