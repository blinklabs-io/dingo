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

// gatherRecentDelays scrapes reg and returns, per idx label, the block
// number, delay, and fetch duration exported for that ring slot ([0]=block
// number, [1]=delay, [2]=fetch duration). It fails the test if a slot
// exports only some of the three gauges, since consumers pair them by idx.
func gatherRecentDelays(
	t *testing.T,
	reg *prometheus.Registry,
) map[string][3]float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	delays := map[string]float64{}
	blocks := map[string]float64{}
	fetchDurations := map[string]float64{}
	for _, mf := range families {
		var dst map[string]float64
		switch mf.GetName() {
		case recentBlockDelayMetricName:
			dst = delays
		case recentBlockNumberMetricName:
			dst = blocks
		case recentFetchDurationMetricName:
			dst = fetchDurations
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
	require.Len(
		t,
		fetchDurations,
		len(delays),
		"fetch-duration and delay slots must match",
	)
	out := make(map[string][3]float64, len(delays))
	for idx, d := range delays {
		b, ok := blocks[idx]
		require.True(t, ok, "idx %s has a delay but no block number", idx)
		f, ok := fetchDurations[idx]
		require.True(t, ok, "idx %s has a delay but no fetch duration", idx)
		out[idx] = [3]float64{b, d, f}
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
	r.record(100, testBlockHash(1), 0.31, 0.30)
	require.Len(t, gatherRecentDelays(t, reg), 1)

	r.record(101, testBlockHash(2), 0.28, 0.27)
	r.record(102, testBlockHash(3), 3.40, 0.09)
	r.record(103, testBlockHash(4), 0.45, 0.44)

	got := gatherRecentDelays(t, reg)
	want := map[uint64][2]float64{
		100: {0.31, 0.30},
		101: {0.28, 0.27},
		102: {3.40, 0.09},
		103: {0.45, 0.44},
	}
	require.Len(t, got, len(want))
	for blockNum, vals := range want {
		idx := strconv.FormatUint(blockNum%recentBlockDelaySlots, 10)
		require.Contains(t, got, idx)
		assert.Equal(t, float64(blockNum), got[idx][0])
		assert.InDelta(t, vals[0], got[idx][1], 1e-9)
		assert.InDelta(t, vals[1], got[idx][2], 1e-9)
	}
}

// The reason fetch duration is tracked separately from delay: a block whose
// request was dispatched late (e.g. the ledger held off requesting a fork's
// replacement block until a rollback finished) can show a large delay
// alongside a small fetch duration, and both numbers must survive intact so
// a dashboard can tell "the fetch was fast, something upstream was slow"
// apart from "the fetch itself was slow".
func TestRecentBlockDelaysKeepsFetchDurationIndependentOfDelay(t *testing.T) {
	t.Parallel()
	r, reg := newTestRecentDelays(t)
	r.record(200, testBlockHash(1), 5.76, 0.093)

	idx := strconv.FormatUint(200%recentBlockDelaySlots, 10)
	got := gatherRecentDelays(t, reg)[idx]
	assert.InDelta(t, 5.76, got[1], 1e-9, "delay must stay as recorded")
	assert.InDelta(
		t,
		0.093,
		got[2],
		1e-9,
		"fetch duration must stay independent of the larger delay",
	)
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
		r.record(n, testBlockHash(byte(i)), float64(i), float64(i))
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

	r.record(200, testBlockHash(7), 0.40, 0.35)
	r.record(200, testBlockHash(7), 2.50, 2.45)
	got := gatherRecentDelays(t, reg)[idx]
	assert.InDelta(t, 0.40, got[1], 1e-9)
	assert.InDelta(t, 0.35, got[2], 1e-9)

	r.record(200, testBlockHash(8), 1.10, 1.05)
	got = gatherRecentDelays(t, reg)[idx]
	assert.InDelta(t, 1.10, got[1], 1e-9)
	assert.InDelta(t, 1.05, got[2], 1e-9)
}

// The ring is wired into the blockfetch metrics and registered with the
// configured registry alongside the cardano-node compatible gauges.
func TestBlockfetchMetricsRegistersRecentBlockDelays(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{PromRegistry: reg})
	require.NotNil(t, o.blockfetchMetrics)
	require.NotNil(t, o.blockfetchMetrics.recentDelays)

	o.blockfetchMetrics.recentDelays.record(5, testBlockHash(1), 0.5, 0.4)
	got := gatherRecentDelays(t, reg)
	require.Contains(t, got, strconv.FormatUint(5%recentBlockDelaySlots, 10))
}

// A late delivery of a block that has already been overwritten by a newer
// block in the same slot (the ring wrapped) is stale and must not replace the
// newer sample.
func TestRecentBlockDelaysWrappedSlotIgnoresStaleDelivery(t *testing.T) {
	t.Parallel()
	r, reg := newTestRecentDelays(t)
	idx := strconv.FormatUint(100%recentBlockDelaySlots, 10)

	r.record(100, testBlockHash(1), 0.40, 0.35)
	r.record(100+recentBlockDelaySlots, testBlockHash(2), 0.55, 0.50)
	r.record(
		100,
		testBlockHash(1),
		3.00,
		2.95,
	) // late repeat of the older block
	r.record(100, testBlockHash(9), 2.00, 1.95) // older height, different hash

	got := gatherRecentDelays(t, reg)[idx]
	assert.Equal(t, float64(100+recentBlockDelaySlots), got[0])
	assert.InDelta(t, 0.55, got[1], 1e-9)
	assert.InDelta(t, 0.50, got[2], 1e-9)
}
