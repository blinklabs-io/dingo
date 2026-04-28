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

package hardfork_test

import (
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// systemStart used by most summary tests.
var testSysStart = time.Date(2022, 10, 25, 0, 0, 0, 0, time.UTC)

// boundedAt is a helper to build a Bound.
func boundedAt(rel time.Duration, slot, epoch uint64) hardfork.Bound {
	return hardfork.Bound{RelativeTime: rel, Slot: slot, Epoch: epoch}
}

// byronParams mimic Byron mainnet (20s slots, 21600 slots per epoch).
var byronParams = hardfork.EraParams{
	EpochSize:     21_600,
	SlotLength:    20 * time.Second,
	SafeZoneSlots: 4320,
	GenesisWindow: 4320,
}

// shelleyParams mimic Shelley mainnet (1s slots, 432000 slots per epoch).
var shelleyParams = hardfork.EraParams{
	EpochSize:     432_000,
	SlotLength:    time.Second,
	SafeZoneSlots: 25_920,
	GenesisWindow: 25_920,
}

// twoEraSummary builds a Summary with Byron closed at slot 100 epoch 5 and
// Shelley open, unbounded.
func twoEraSummary() hardfork.Summary {
	end := boundedAt(100*20*time.Second, 100, 5)
	return hardfork.Summary{
		SystemStart: testSysStart,
		Eras: []hardfork.EraSummary{
			{
				EraID:  0,
				Start:  hardfork.Bound{},
				End:    &end,
				Params: byronParams,
			},
			{
				EraID:  6,
				Start:  end,
				End:    nil, // unbounded
				Params: shelleyParams,
			},
		},
	}
}

// ------------------------------------------------------------------ SlotToTime

func TestSummary_SlotToTime_Genesis(t *testing.T) {
	s := twoEraSummary()
	got, err := s.SlotToTime(0)
	require.NoError(t, err)
	assert.Equal(t, testSysStart, got)
}

func TestSummary_SlotToTime_InFirstEra(t *testing.T) {
	s := twoEraSummary()
	// Byron slot 50 → 50 * 20s = 1000s past system start.
	got, err := s.SlotToTime(50)
	require.NoError(t, err)
	want := testSysStart.Add(1000 * time.Second)
	assert.Equal(t, want, got)
}

func TestSummary_SlotToTime_AtEraBoundary(t *testing.T) {
	s := twoEraSummary()
	// Slot 100 is the start of the Shelley era.
	got, err := s.SlotToTime(100)
	require.NoError(t, err)
	want := testSysStart.Add(100 * 20 * time.Second) // 2000s
	assert.Equal(t, want, got)
}

func TestSummary_SlotToTime_InSecondEra(t *testing.T) {
	s := twoEraSummary()
	// Slot 150 = 100 (era start) + 50 Shelley slots * 1s each.
	got, err := s.SlotToTime(150)
	require.NoError(t, err)
	want := testSysStart.Add(100*20*time.Second + 50*time.Second)
	assert.Equal(t, want, got)
}

func TestSummary_SlotToTime_PastHorizon_BoundedLastEra(t *testing.T) {
	// Summary whose only era has a closed End at slot 100.
	end := boundedAt(100*20*time.Second, 100, 5)
	s := hardfork.Summary{
		SystemStart: testSysStart,
		Eras: []hardfork.EraSummary{{
			EraID:  0,
			Start:  hardfork.Bound{},
			End:    &end,
			Params: byronParams,
		}},
	}
	_, err := s.SlotToTime(100)
	// Slot 100 is *at* the boundary: treated as not belonging to this era.
	assert.ErrorIs(t, err, hardfork.ErrPastHorizon)
}

// ------------------------------------------------------------------ TimeToSlot

func TestSummary_TimeToSlot_Genesis(t *testing.T) {
	s := twoEraSummary()
	got, err := s.TimeToSlot(testSysStart)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), got)
}

func TestSummary_TimeToSlot_BeforeGenesis(t *testing.T) {
	s := twoEraSummary()
	_, err := s.TimeToSlot(testSysStart.Add(-time.Second))
	assert.True(t, errors.Is(err, hardfork.ErrBeforeGenesis))
}

func TestSummary_TimeToSlot_InFirstEra(t *testing.T) {
	s := twoEraSummary()
	got, err := s.TimeToSlot(testSysStart.Add(1000 * time.Second))
	require.NoError(t, err)
	assert.Equal(t, uint64(50), got) // 1000s / 20s
}

func TestSummary_TimeToSlot_InSecondEra(t *testing.T) {
	s := twoEraSummary()
	// Shelley starts at 2000s; 2000s + 50s → slot 150.
	got, err := s.TimeToSlot(testSysStart.Add(2050 * time.Second))
	require.NoError(t, err)
	assert.Equal(t, uint64(150), got)
}

func TestSummary_TimeToSlot_AtEraBoundary(t *testing.T) {
	s := twoEraSummary()
	// Exactly at boundary → belongs to second era at slot 100.
	got, err := s.TimeToSlot(testSysStart.Add(2000 * time.Second))
	require.NoError(t, err)
	assert.Equal(t, uint64(100), got)
}

// ------------------------------------------------------------------ SlotToEpoch

func TestSummary_SlotToEpoch_InFirstEra(t *testing.T) {
	// Byron era starts at epoch 0 slot 0. Epoch size 21_600.
	end := boundedAt(100_000*20*time.Second, 100_000, 20)
	s := hardfork.Summary{
		SystemStart: testSysStart,
		Eras: []hardfork.EraSummary{{
			EraID:  0,
			Start:  hardfork.Bound{},
			End:    &end,
			Params: byronParams,
		}},
	}
	got, err := s.SlotToEpoch(21_600 + 100)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), got.Epoch)
	assert.Equal(t, uint64(21_600), got.StartSlot)
	assert.Equal(t, uint64(21_600), got.LengthInSlots)
	assert.Equal(t, 20*time.Second, got.SlotLength)
	assert.Equal(t, uint(0), got.EraID)
}

func TestSummary_SlotToEpoch_InSecondEra(t *testing.T) {
	s := twoEraSummary()
	// Slot 100 is Shelley epoch 5 (carried over from Byron's end).
	got, err := s.SlotToEpoch(100)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), got.Epoch)
	assert.Equal(t, uint64(100), got.StartSlot)
	assert.Equal(t, uint64(432_000), got.LengthInSlots)
	assert.Equal(t, time.Second, got.SlotLength)
	assert.Equal(t, uint(6), got.EraID)
}

func TestSummary_SlotToEpoch_PastHorizon(t *testing.T) {
	end := boundedAt(100*20*time.Second, 100, 5)
	s := hardfork.Summary{
		SystemStart: testSysStart,
		Eras: []hardfork.EraSummary{{
			EraID:  0,
			Start:  hardfork.Bound{},
			End:    &end,
			Params: byronParams,
		}},
	}
	_, err := s.SlotToEpoch(101)
	assert.ErrorIs(t, err, hardfork.ErrPastHorizon)
}

// ------------------------------------------------------------------ misc

func TestSummary_CurrentEra(t *testing.T) {
	s := twoEraSummary()
	cur := s.CurrentEra()
	require.NotNil(t, cur)
	assert.Equal(t, uint(6), cur.EraID)
	assert.Nil(t, cur.End, "current era is unbounded in this summary")
}

func TestSummary_Validate_Empty(t *testing.T) {
	s := hardfork.Summary{SystemStart: testSysStart}
	assert.Error(t, s.Validate())
}

func TestSummary_Validate_BoundsLineUp(t *testing.T) {
	s := twoEraSummary()
	assert.NoError(t, s.Validate())
}

func TestSummary_Validate_BoundsMismatch(t *testing.T) {
	// Byron.End.Slot = 100; Shelley.Start.Slot = 101 — mismatch.
	end := boundedAt(100*20*time.Second, 100, 5)
	badStart := boundedAt(100*20*time.Second, 101, 5)
	s := hardfork.Summary{
		SystemStart: testSysStart,
		Eras: []hardfork.EraSummary{
			{EraID: 0, Start: hardfork.Bound{}, End: &end, Params: byronParams},
			{EraID: 6, Start: badStart, End: nil, Params: shelleyParams},
		},
	}
	assert.Error(t, s.Validate())
}
