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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gatherRecentForkBattles scrapes reg and returns, per idx label, the
// participant count and distinct-slot count exported for that ring slot
// ([0]=participants, [1]=distinct slots).
func gatherRecentForkBattles(
	t *testing.T,
	reg *prometheus.Registry,
) map[string][2]float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	participants := map[string]float64{}
	distinctSlots := map[string]float64{}
	for _, mf := range families {
		var dst map[string]float64
		switch mf.GetName() {
		case recentForkParticipantsMetricName:
			dst = participants
		case recentForkDistinctSlotsMetricName:
			dst = distinctSlots
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
		distinctSlots,
		len(participants),
		"participants and distinct-slots slots must match",
	)
	out := make(map[string][2]float64, len(participants))
	for idx, p := range participants {
		d, ok := distinctSlots[idx]
		require.True(
			t,
			ok,
			"idx %s has participants but no distinct-slots",
			idx,
		)
		out[idx] = [2]float64{p, d}
	}
	return out
}

func newTestRecentForkBattles(
	t *testing.T,
) (*recentForkBattles, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	r := newRecentForkBattles()
	require.NoError(t, reg.Register(r))
	return r, reg
}

// Unfilled ring slots must not be exported, and a single delivery with no
// competitor is not a battle: one participant, one slot.
func TestRecentForkBattlesExportsNothingUntilRecorded(t *testing.T) {
	t.Parallel()
	_, reg := newTestRecentForkBattles(t)
	assert.Empty(t, gatherRecentForkBattles(t, reg))
}

func TestRecentForkBattlesSingleDeliveryIsNotABattle(t *testing.T) {
	t.Parallel()
	r, reg := newTestRecentForkBattles(t)
	r.recordParticipant(100, 1000, testBlockHash(1))

	idx := strconv.FormatUint(100%recentBlockDelaySlots, 10)
	got := gatherRecentForkBattles(t, reg)[idx]
	assert.Equal(t, 1.0, got[0], "one participant")
	assert.Equal(t, 1.0, got[1], "one distinct slot")
}

// Two different blocks at the same slot (a slot battle) are two participants
// sharing one slot.
func TestRecentForkBattlesSlotBattle(t *testing.T) {
	t.Parallel()
	r, reg := newTestRecentForkBattles(t)
	r.recordParticipant(100, 1000, testBlockHash(1))
	r.recordParticipant(100, 1000, testBlockHash(2))

	idx := strconv.FormatUint(100%recentBlockDelaySlots, 10)
	got := gatherRecentForkBattles(t, reg)[idx]
	assert.Equal(t, 2.0, got[0], "two participants")
	assert.Equal(t, 1.0, got[1], "one distinct slot")
}

// Two blocks at different slots racing for the same height (a height battle)
// are two participants across two distinct slots.
func TestRecentForkBattlesHeightBattle(t *testing.T) {
	t.Parallel()
	r, reg := newTestRecentForkBattles(t)
	r.recordParticipant(100, 1000, testBlockHash(1))
	r.recordParticipant(100, 1002, testBlockHash(2))

	idx := strconv.FormatUint(100%recentBlockDelaySlots, 10)
	got := gatherRecentForkBattles(t, reg)[idx]
	assert.Equal(t, 2.0, got[0], "two participants")
	assert.Equal(t, 2.0, got[1], "two distinct slots")
}

// A three-way battle (e.g. multiple pools winning the same slot by VRF
// chance, or a stacked short fork) is captured the same way, without any
// special-casing of the two-competitor case.
func TestRecentForkBattlesThreeWay(t *testing.T) {
	t.Parallel()
	r, reg := newTestRecentForkBattles(t)
	r.recordParticipant(100, 1000, testBlockHash(1))
	r.recordParticipant(100, 1000, testBlockHash(2))
	r.recordParticipant(100, 1002, testBlockHash(3))

	idx := strconv.FormatUint(100%recentBlockDelaySlots, 10)
	got := gatherRecentForkBattles(t, reg)[idx]
	assert.Equal(t, 3.0, got[0], "three participants")
	assert.Equal(t, 2.0, got[1], "two distinct slots")
}

// A repeat delivery of the same block (same height and hash) must not
// inflate the participant count.
func TestRecentForkBattlesDedupesSameHash(t *testing.T) {
	t.Parallel()
	r, reg := newTestRecentForkBattles(t)
	r.recordParticipant(100, 1000, testBlockHash(1))
	r.recordParticipant(100, 1000, testBlockHash(1))

	idx := strconv.FormatUint(100%recentBlockDelaySlots, 10)
	got := gatherRecentForkBattles(t, reg)[idx]
	assert.Equal(t, 1.0, got[0])
	assert.Equal(t, 1.0, got[1])
}

// A new height landing in the same ring slot resets the set: it is a
// different battle (or no battle at all), not a continuation of the old one.
func TestRecentForkBattlesNewHeightResetsSlot(t *testing.T) {
	t.Parallel()
	r, reg := newTestRecentForkBattles(t)
	idx := strconv.FormatUint(100%recentBlockDelaySlots, 10)

	r.recordParticipant(100, 1000, testBlockHash(1))
	r.recordParticipant(100, 1002, testBlockHash(2))
	got := gatherRecentForkBattles(t, reg)[idx]
	assert.Equal(t, 2.0, got[0])

	r.recordParticipant(100+recentBlockDelaySlots, 2000, testBlockHash(3))
	got = gatherRecentForkBattles(t, reg)[idx]
	assert.Equal(t, 1.0, got[0], "new height must not inherit the old battle")
	assert.Equal(t, 1.0, got[1])
}

// A late delivery for a height the ring slot has already moved past (the
// ring wrapped) is stale and must not resurrect or mutate that slot.
func TestRecentForkBattlesWrappedSlotIgnoresStaleDelivery(t *testing.T) {
	t.Parallel()
	r, reg := newTestRecentForkBattles(t)
	idx := strconv.FormatUint(100%recentBlockDelaySlots, 10)

	r.recordParticipant(100+recentBlockDelaySlots, 2000, testBlockHash(1))
	r.recordParticipant(100, 1000, testBlockHash(2)) // stale, older height

	got := gatherRecentForkBattles(t, reg)[idx]
	assert.Equal(t, 1.0, got[0])
	assert.Equal(t, 1.0, got[1])
}

// A single ring slot must not grow without bound from a pathological number
// of distinct hashes reported for one height.
func TestRecentForkBattlesBoundsParticipantCount(t *testing.T) {
	t.Parallel()
	r, reg := newTestRecentForkBattles(t)
	idx := strconv.FormatUint(100%recentBlockDelaySlots, 10)

	for i := range recentForkMaxParticipants + 5 {
		r.recordParticipant(100, uint64(1000+i), testBlockHash(byte(i)))
	}

	got := gatherRecentForkBattles(t, reg)[idx]
	assert.Equal(t, float64(recentForkMaxParticipants), got[0])
}

// RecordForkBattleParticipants exists so a battle is visible even when the
// eventual winner never passes through blockfetchClientBlock (a locally
// forged block); it registers a real, decodable rolled-back block by
// decoding it to reach the block number/slot/hash recordParticipant needs.
func TestRecordForkBattleParticipantsRegistersRolledBackBlocks(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{PromRegistry: reg})
	blockType, raw := conwayBlockFixtureBytes(t)
	decoded, err := models.Block{Type: blockType, Cbor: raw}.Decode()
	require.NoError(t, err)

	o.RecordForkBattleParticipants([]models.Block{{Type: blockType, Cbor: raw}})

	idx := strconv.FormatUint(
		decoded.BlockNumber()%recentBlockDelaySlots,
		10,
	)
	got := gatherRecentForkBattles(t, reg)[idx]
	assert.Equal(t, 1.0, got[0])
	assert.Equal(t, 1.0, got[1])
}

// A block that fails to decode never reached the ledger as a real
// competitor, so it is skipped rather than recorded or causing a panic.
func TestRecordForkBattleParticipantsSkipsUndecodableBlocks(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{PromRegistry: reg})

	assert.NotPanics(t, func() {
		o.RecordForkBattleParticipants(
			[]models.Block{{Type: 99, Cbor: []byte{0xff, 0xff}}},
		)
	})
	assert.Empty(t, gatherRecentForkBattles(t, reg))
}

// With metrics disabled (no PromRegistry configured), recentForks is never
// initialized; RecordForkBattleParticipants must not panic on a nil ring.
func TestRecordForkBattleParticipantsNilMetricsSafe(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{})
	blockType, raw := conwayBlockFixtureBytes(t)

	assert.NotPanics(t, func() {
		o.RecordForkBattleParticipants(
			[]models.Block{{Type: blockType, Cbor: raw}},
		)
	})
}
