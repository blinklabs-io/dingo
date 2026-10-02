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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (m *mockForgedBlockChecker) WasForgedByUs(
	slot uint64,
) ([]byte, bool) {
	hash, ok := m.forgedSlots[slot]
	return hash, ok
}

// TestTimeToSlot_FutureTimeWithEmptyCacheReturnsError pins that when the
// epoch cache is empty, TimeToSlot rejects arbitrary future times instead of
// silently returning a "now"-ish approximation.
//
// The implementation falls through to nearNowSlot whenever `time.Since(t) <
// 5*time.Second`. Because `time.Since` is `now - t`, that is NEGATIVE (and
// therefore always `< 5*time.Second`) for any t in the future — so a caller
// asking about a time one day ahead gets the current slot, not an error.
func TestTimeToSlot_FutureTimeWithEmptyCacheReturnsError(t *testing.T) {
	t.Parallel()

	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"slotLength": 1,
		"epochLength": 432000,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)))

	ls := &LedgerState{
		// epochCache empty — HardForkSummary will error.
		config: LedgerStateConfig{CardanoNodeConfig: cfg},
	}
	ls.publishSnapshotsLocked()

	// Far-future time; well past any "near now" tolerance.
	future := time.Now().Add(24 * time.Hour)
	_, err := ls.TimeToSlot(future)
	assert.Error(
		t,
		err,
		"TimeToSlot must reject far-future times when the epoch cache is empty; "+
			"the nearNowSlot fallback is only for times within ±5s of now",
	)
}

// crossEraLedger returns a LedgerState with two eras:
//   - Byron-ish: EraId=0, 20s slots, 100 slots/epoch, 2 epochs → slots 0..199
//   - Shelley-ish: EraId=1, 1s slots, 432 slots/epoch, 2 epochs → slots 200..1063
//
// Total Byron time = 2 × 100 × 20s = 4000s.
func crossEraLedger(t *testing.T) *LedgerState {
	t.Helper()
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				SlotLength:    20_000,
				LengthInSlots: 100,
				EraId:         0,
			},
			{
				EpochId:       1,
				StartSlot:     100,
				SlotLength:    20_000,
				LengthInSlots: 100,
				EraId:         0,
			},
			{
				EpochId:       2,
				StartSlot:     200,
				SlotLength:    1000,
				LengthInSlots: 432,
				EraId:         1,
			},
			{
				EpochId:       3,
				StartSlot:     632,
				SlotLength:    1000,
				LengthInSlots: 432,
				EraId:         1,
			},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: minimalShelleyGenesisCfg(t),
		},
	}
	ls.publishSnapshotsLocked()
	return ls
}

// TestSlotToTime_CrossEra covers multi-era chains where per-era slot length
// differs. Any implementation that reads slot length from the epoch currently
// being traversed — whether the legacy loop or the new Summary-backed
// delegation — must return the same absolute times.
func TestSlotToTime_CrossEra(t *testing.T) {
	t.Parallel()

	ls := crossEraLedger(t)
	sysStart := time.Date(2022, 10, 25, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		slot uint64
		want time.Duration // offset from SystemStart
	}{
		{"byron genesis", 0, 0},
		{"byron mid", 50, 1000 * time.Second}, // 50 × 20s
		{"byron end", 199, 3980 * time.Second},
		{"boundary", 200, 4000 * time.Second}, // Shelley's first slot
		{"shelley mid", 250, 4050 * time.Second},
		{"shelley end of first epoch", 631, 4431 * time.Second},
		{"shelley second epoch", 700, 4500 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ls.SlotToTime(tc.slot)
			require.NoError(t, err)
			assert.Equal(t, sysStart.Add(tc.want), got)
		})
	}
}

// TestTimeToSlot_CrossEra round-trips cross-era slot→time→slot.
func TestTimeToSlot_CrossEra(t *testing.T) {
	t.Parallel()

	ls := crossEraLedger(t)
	for _, slot := range []uint64{0, 50, 199, 200, 250, 631, 700} {
		t.Run((time.Duration(slot) * time.Second).String(), func(t *testing.T) {
			tt, err := ls.SlotToTime(slot)
			require.NoError(t, err)
			got, err := ls.TimeToSlot(tt)
			require.NoError(t, err)
			assert.Equal(t, slot, got)
		})
	}
}

// TestSlotToEpoch_CrossEra verifies epoch lookup spans both eras correctly.
func TestSlotToEpoch_CrossEra(t *testing.T) {
	t.Parallel()

	ls := crossEraLedger(t)
	tests := []struct {
		name      string
		slot      uint64
		wantEpoch uint64
		wantStart uint64
		wantEra   uint
	}{
		{"byron epoch 0", 50, 0, 0, 0},
		{"byron epoch 1", 150, 1, 100, 0},
		{"shelley epoch 2", 250, 2, 200, 1},
		{"shelley epoch 3", 700, 3, 632, 1},
		// Project forward using Shelley params (432 slots/epoch, 1s each).
		{"future projected epoch", 1200, 4, 1064, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ls.SlotToEpoch(tc.slot)
			require.NoError(t, err)
			assert.Equal(t, tc.wantEpoch, got.EpochId)
			assert.Equal(t, tc.wantStart, got.StartSlot)
			assert.Equal(t, tc.wantEra, got.EraId)
		})
	}
}

func TestSlotCalc(t *testing.T) {
	t.Parallel()

	testLedgerState := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				SlotLength:    1000,
				LengthInSlots: 86400,
				EraId:         1,
			},
			{
				EpochId:       1,
				StartSlot:     86400,
				SlotLength:    1000,
				LengthInSlots: 86400,
				EraId:         1,
			},
			{
				EpochId:       2,
				StartSlot:     172800,
				SlotLength:    1000,
				LengthInSlots: 86400,
				EraId:         1,
			},
			{
				EpochId:       3,
				StartSlot:     259200,
				SlotLength:    1000,
				LengthInSlots: 86400,
				EraId:         1,
			},
			{
				EpochId:       4,
				StartSlot:     345600,
				SlotLength:    1000,
				LengthInSlots: 86400,
				EraId:         1,
			},
			{
				EpochId:       5,
				StartSlot:     432000,
				SlotLength:    1000,
				LengthInSlots: 86400,
				EraId:         1,
			},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
		},
		currentTip: ochainsync.Tip{Point: ocommon.NewPoint(432001, []byte("tip"))},
	}
	testLedgerState.publishSnapshotsLocked()
	testDefs := []struct {
		slot     uint64
		slotTime time.Time
		epoch    uint64
	}{
		{
			slot:     0,
			slotTime: time.Date(2022, time.October, 25, 0, 0, 0, 0, time.UTC),
			epoch:    0,
		},
		{
			slot: 86399,
			slotTime: time.Date(
				2022,
				time.October,
				25,
				23,
				59,
				59,
				0,
				time.UTC,
			),
			epoch: 0,
		},
		{
			slot:     86400,
			slotTime: time.Date(2022, time.October, 26, 0, 0, 0, 0, time.UTC),
			epoch:    1,
		},
		{
			slot:     432001,
			slotTime: time.Date(2022, time.October, 30, 0, 0, 1, 0, time.UTC),
			epoch:    5,
		},
	}
	for _, testDef := range testDefs {
		// Slot to time
		tmpSlotToTime, err := testLedgerState.SlotToTime(testDef.slot)
		if err != nil {
			t.Errorf("unexpected error converting slot to time: %s", err)
		}
		if !tmpSlotToTime.Equal(testDef.slotTime) {
			t.Errorf(
				"did not get expected time from slot: got %s, wanted %s",
				tmpSlotToTime,
				testDef.slotTime,
			)
		}
		// Time to slot
		tmpTimeToSlot, err := testLedgerState.TimeToSlot(testDef.slotTime)
		if err != nil {
			t.Errorf("unexpected error converting time to slot: %s", err)
		}
		if tmpTimeToSlot != testDef.slot {
			t.Errorf(
				"did not get expected slot from time: got %d, wanted %d",
				tmpTimeToSlot,
				testDef.slot,
			)
		}
		// Slot to epoch
		tmpSlotToEpoch, err := testLedgerState.SlotToEpoch(testDef.slot)
		if err != nil {
			t.Errorf("unexpected error getting epoch from slot: %s", err)
		}
		if tmpSlotToEpoch.EpochId != testDef.epoch {
			t.Errorf(
				"did not get expected epoch from slot: got %d, wanted %d",
				tmpSlotToEpoch.EpochId,
				testDef.epoch,
			)
		}
	}
}

func TestSlotToEpochProjection(t *testing.T) {
	t.Parallel()

	// Test that SlotToEpoch correctly projects future epochs beyond known epochs
	testLedgerState := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				SlotLength:    1000,
				LengthInSlots: 100, // 100 slots per epoch for easier math
				EraId:         1,
			},
			{
				EpochId:       1,
				StartSlot:     100,
				SlotLength:    1000,
				LengthInSlots: 100,
				EraId:         1,
			},
			{
				EpochId:       2,
				StartSlot:     200,
				SlotLength:    1000,
				LengthInSlots: 100,
				EraId:         1,
			},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
		},
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(250, []byte("tip")),
		},
	}
	testLedgerState.publishSnapshotsLocked()

	testCases := []struct {
		name          string
		slot          uint64
		expectedEpoch uint64
		expectedStart uint64
	}{
		{
			name:          "within known epoch 0",
			slot:          50,
			expectedEpoch: 0,
			expectedStart: 0,
		},
		{
			name:          "within known epoch 2 (last known)",
			slot:          250,
			expectedEpoch: 2,
			expectedStart: 200,
		},
		{
			name:          "first slot of projected epoch 3",
			slot:          300,
			expectedEpoch: 3,
			expectedStart: 300,
		},
		{
			name:          "middle of projected epoch 3",
			slot:          350,
			expectedEpoch: 3,
			expectedStart: 300,
		},
		{
			name:          "last slot of projected epoch 3",
			slot:          399,
			expectedEpoch: 3,
			expectedStart: 300,
		},
		{
			name:          "first slot of projected epoch 4",
			slot:          400,
			expectedEpoch: 4,
			expectedStart: 400,
		},
		{
			name:          "far future epoch 10",
			slot:          1050,
			expectedEpoch: 10,
			expectedStart: 1000,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			epoch, err := testLedgerState.SlotToEpoch(tc.slot)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if epoch.EpochId != tc.expectedEpoch {
				t.Errorf(
					"expected epoch %d, got %d",
					tc.expectedEpoch,
					epoch.EpochId,
				)
			}
			if epoch.StartSlot != tc.expectedStart {
				t.Errorf(
					"expected start slot %d, got %d",
					tc.expectedStart,
					epoch.StartSlot,
				)
			}
			// Verify the slot falls within the returned epoch
			if tc.slot < epoch.StartSlot ||
				tc.slot >= epoch.StartSlot+uint64(epoch.LengthInSlots) {
				t.Errorf(
					"slot %d not within returned epoch (start=%d, length=%d)",
					tc.slot,
					epoch.StartSlot,
					epoch.LengthInSlots,
				)
			}
		})
	}
}

func TestSlotToEpochEmptyCache(t *testing.T) {
	t.Parallel()

	testLedgerState := &LedgerState{
		epochCache: []models.Epoch{},
	}
	testLedgerState.publishSnapshotsLocked()

	_, err := testLedgerState.SlotToEpoch(100)
	if err == nil {
		t.Error("expected error for empty epoch cache")
	}
	if err.Error() != "no epochs in cache" {
		t.Errorf("unexpected error message: %s", err.Error())
	}
}

func TestSlotToEpochBeforeFirstEpoch(t *testing.T) {
	t.Parallel()

	// Test that slots before the first known epoch return an error
	testLedgerState := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       5, // First known epoch is not epoch 0
				StartSlot:     500,
				SlotLength:    1000,
				LengthInSlots: 100,
				EraId:         1,
			},
			{
				EpochId:       6,
				StartSlot:     600,
				SlotLength:    1000,
				LengthInSlots: 100,
				EraId:         1,
			},
		},
	}
	testLedgerState.publishSnapshotsLocked()

	// Slot before first known epoch should error
	_, err := testLedgerState.SlotToEpoch(100)
	if err == nil {
		t.Error("expected error for slot before first known epoch")
	}
	if !errors.Is(err, hardfork.ErrPastHorizon) {
		t.Errorf("expected ErrPastHorizon, got: %v", err)
	}
	if !strings.Contains(err.Error(), "slot is outside the known epoch range") {
		t.Errorf("unexpected error message: %s", err.Error())
	}

	// Slot at first epoch boundary should work
	epoch, err := testLedgerState.SlotToEpoch(500)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if epoch.EpochId != 5 {
		t.Errorf("expected epoch 5, got %d", epoch.EpochId)
	}
}

// TestSlotToTimeExtrapolatesNextSlotOnLongSlotEras covers eras whose slot length
// exceeds the fixed 5s near-now window. Byron is 20s per slot in real Cardano
// shapes, so the next slot boundary sits 20s in the future: gating on a fixed
// window rejected it, SlotToTime returned ErrPastHorizon, and the clock fell
// back into the 100ms error-retry loop this fallback exists to avoid.
func TestSlotToTimeExtrapolatesNextSlotOnLongSlotEras(t *testing.T) {
	t.Parallel()

	const (
		byronSlotLengthMs = 20_000
		byronSlotLength   = 20 * time.Second
	)
	ls, nowSlot, now := slotToTimeBehindHorizonState(
		t, byronSlotLengthMs, 5_000_000,
	)
	nextSlot := nowSlot + 1

	sum, err := ls.HardForkSummary()
	require.NoError(t, err)
	_, horizonErr := sum.SlotToTime(nextSlot)
	require.ErrorIs(t, horizonErr, hardfork.ErrPastHorizon, "premise")

	// The next boundary is a full 20s ahead -- well outside the 5s window.
	when, err := ls.SlotToTime(nextSlot)
	require.NoError(t, err,
		"a 20s-slot era's next boundary must still resolve")
	assert.Equal(t, now.Add(byronSlotLength), when)
	assert.Greater(t, when.Sub(now), operationalWindow,
		"premise: the next boundary is beyond the fixed near-now window")

	// The inverse must accept the same boundary: SlotToTime and TimeToSlot are
	// a pair, so a fixed window on the reverse direction would reject the very
	// time SlotToTime just returned.
	backSlot, err := ls.TimeToSlot(when)
	require.NoError(t, err,
		"TimeToSlot must accept the boundary SlotToTime returned")
	assert.Equal(t, nextSlot, backSlot, "the pair must round-trip")

	// Still bounded in both directions: many slot lengths away is rejected.
	_, err = ls.SlotToTime(nextSlot + 100)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon)
	_, err = ls.TimeToSlot(now.Add(100 * byronSlotLength))
	require.ErrorIs(t, err, hardfork.ErrPastHorizon,
		"a time many slot lengths ahead must still be refused as past-horizon")
}

// newShelleyOnlyForecastLedger builds a LedgerState whose epoch cache covers
// slots [100_000, 532_000) but whose config cannot produce a hard-fork shape.
func newShelleyOnlyForecastLedger(t testing.TB) *LedgerState {
	t.Helper()
	ls := &LedgerState{
		epochCache: []models.Epoch{{
			EpochId:       500,
			StartSlot:     100_000,
			SlotLength:    1_000,
			LengthInSlots: 432_000,
			EraId:         eras.ConwayEraDesc.Id,
			Nonce:         []byte("nonce"),
		}},
		currentEra: eras.ConwayEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       500,
			StartSlot:     100_000,
			LengthInSlots: 432_000,
		},
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(200_000, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: shelleyOnlyGenesisCfg(t),
		},
	}
	ls.publishSnapshotsLocked()
	return ls
}

// TestSlotToTime_CachedSlotWithoutForecast pins SlotToTime against
// SlotToEpoch: a slot the epoch cache already covers has known era parameters
// and must convert without a forecast. Returning the summary-build error
// verbatim leaves the slot clock (ledger/slot_clock.go) unable to resolve a
// slot boundary, retrying every 100ms for the life of the process.
func TestSlotToTime_CachedSlotWithoutForecast(t *testing.T) {
	t.Parallel()

	ls := newShelleyOnlyForecastLedger(t)

	// SlotToEpoch already answers from the cache alone.
	epoch, err := ls.SlotToEpoch(200_000)
	require.NoError(t, err)
	assert.Equal(t, uint64(500), epoch.EpochId)

	// The same slot must convert to a time without a forecast. The cache
	// anchors relative time at its first entry's StartSlot, exactly as
	// hardForkSummaryAnchoredAt does, so slot 200_000 is 100_000 slots of
	// 1000ms past SystemStart.
	when, err := ls.SlotToTime(200_000)
	require.NoError(t, err,
		"a slot inside the epoch cache must not require a forecast")
	assert.Equal(
		t,
		time.Date(2022, 10, 25, 0, 0, 0, 0, time.UTC).
			Add(100_000*time.Second),
		when.UTC(),
	)

	// The absence case: a slot the cache does NOT cover has no known era
	// parameters, so it must still fail rather than be extrapolated.
	_, err = ls.SlotToTime(532_000)
	require.Error(t, err,
		"a slot past the epoch cache must not be answered without a forecast")
	_, err = ls.SlotToTime(99_999)
	require.Error(t, err,
		"a slot before the epoch cache must not be answered without a forecast")
}

func TestSlotToTime_NearNowWithoutForecast(t *testing.T) {
	t.Parallel()

	ls := newShelleyOnlyForecastLedger(t)
	const slot = uint64(532_000)
	want := ls.config.CardanoNodeConfig.ShelleyGenesis().SystemStart.Add(
		time.Duration(slot) * time.Second,
	)
	ls.timeConv().nowFunc = func() time.Time { return want }

	when, err := ls.SlotToTime(slot)
	require.NoError(t, err)
	assert.Equal(t, want, when)
}
