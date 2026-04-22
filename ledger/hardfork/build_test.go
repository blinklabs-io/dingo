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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ------------------------------------------------------------------ helpers

// singleEraShape returns a shape with only the provided era, useful for
// unit-testing Summary construction without having to reason about past eras.
func singleEraShape(entry hardfork.ShapeEntry) hardfork.Shape {
	return hardfork.Shape{SystemStart: testSysStart, Eras: []hardfork.ShapeEntry{entry}}
}

func conwayShapeEntry() hardfork.ShapeEntry {
	return hardfork.ShapeEntry{
		EraID:           6,
		EraName:         "Conway",
		MinMajorVersion: 9,
		MaxMajorVersion: 10,
		Params:          shelleyParams,
	}
}

// ------------------------------------------------------------------ single era

// BuildSummary with no past eras and TransitionUnknown: the current era's
// End is tipSlot + SafeZoneSlots snapped to the next epoch boundary.
func TestBuildSummary_SingleEra_TransitionUnknown(t *testing.T) {
	shape := singleEraShape(conwayShapeEntry())
	current := hardfork.EraSummary{
		EraID:  6,
		Start:  hardfork.Bound{RelativeTime: 0, Slot: 100_000, Epoch: 500},
		End:    nil,
		Params: shelleyParams,
	}
	tipSlot := uint64(200_000)
	// SafeZoneSlots = 25_920 → safeEnd = 225_920. Snap to next epoch boundary.
	// Epoch 500 spans slots [100_000, 532_000). 225_920 is inside epoch 500,
	// so the end snaps to 532_000 (epoch 501).
	s, err := hardfork.BuildSummary(shape, nil, current, tipSlot, hardfork.NewTransitionUnknown())
	require.NoError(t, err)
	require.Len(t, s.Eras, 1)

	cur := s.Eras[0]
	require.NotNil(t, cur.End, "TransitionUnknown with finite SafeZoneSlots produces bounded end")
	assert.Equal(t, uint64(532_000), cur.End.Slot)
	assert.Equal(t, uint64(501), cur.End.Epoch)
}

// BuildSummary with TransitionKnown: the end is set to the KnownEpoch's start.
func TestBuildSummary_SingleEra_TransitionKnown(t *testing.T) {
	shape := singleEraShape(conwayShapeEntry())
	current := hardfork.EraSummary{
		EraID:  6,
		Start:  hardfork.Bound{RelativeTime: 0, Slot: 100_000, Epoch: 500},
		End:    nil,
		Params: shelleyParams,
	}
	tipSlot := uint64(200_000)
	s, err := hardfork.BuildSummary(shape, nil, current, tipSlot, hardfork.NewTransitionKnown(501))
	require.NoError(t, err)
	require.Len(t, s.Eras, 1)

	cur := s.Eras[0]
	require.NotNil(t, cur.End)
	assert.Equal(t, uint64(532_000), cur.End.Slot, "TransitionKnown should pin the end at epoch 501's start")
	assert.Equal(t, uint64(501), cur.End.Epoch)
}

// BuildSummary with TransitionImpossible: safe zone applies from era start.
func TestBuildSummary_SingleEra_TransitionImpossible(t *testing.T) {
	shape := singleEraShape(conwayShapeEntry())
	current := hardfork.EraSummary{
		EraID:  6,
		Start:  hardfork.Bound{RelativeTime: 0, Slot: 100_000, Epoch: 500},
		End:    nil,
		Params: shelleyParams,
	}
	tipSlot := uint64(200_000)
	s, err := hardfork.BuildSummary(shape, nil, current, tipSlot, hardfork.NewTransitionImpossible())
	require.NoError(t, err)
	require.Len(t, s.Eras, 1)

	cur := s.Eras[0]
	require.NotNil(t, cur.End)
	// Safe zone from era start slot 100_000: 100_000 + 25_920 = 125_920, snap
	// to next epoch boundary. Epoch 500 = [100_000, 532_000).
	assert.Equal(t, uint64(532_000), cur.End.Slot)
	assert.Equal(t, uint64(501), cur.End.Epoch)
}

// SafeZoneSlots == 0 denotes UnsafeIndefiniteSafeZone: unbounded.
func TestBuildSummary_UnsafeIndefiniteSafeZone(t *testing.T) {
	entry := conwayShapeEntry()
	entry.Params.SafeZoneSlots = 0
	shape := singleEraShape(entry)
	current := hardfork.EraSummary{
		EraID:  6,
		Start:  hardfork.Bound{RelativeTime: 0, Slot: 100_000, Epoch: 500},
		End:    nil,
		Params: entry.Params,
	}
	s, err := hardfork.BuildSummary(shape, nil, current, 200_000, hardfork.NewTransitionUnknown())
	require.NoError(t, err)
	require.Len(t, s.Eras, 1)
	assert.Nil(t, s.Eras[0].End, "zero SafeZoneSlots ⇒ unbounded end")
}

// A past era gets carried through verbatim.
func TestBuildSummary_WithPastEra(t *testing.T) {
	shape := sampleShape()
	// Byron: closed past era, slot 0..100 (5 epochs: each epoch = 20s * 21_600 = 432_000s).
	// We keep the arithmetic simple by using a short synthetic Byron.
	byronStart := hardfork.Bound{}
	byronEnd := hardfork.Bound{
		RelativeTime: 100 * 20 * time.Second,
		Slot:         100,
		Epoch:        1,
	}
	past := []hardfork.EraSummary{{
		EraID:  0,
		Start:  byronStart,
		End:    &byronEnd,
		Params: byronParams,
	}}
	current := hardfork.EraSummary{
		EraID:  1,
		Start:  byronEnd,
		End:    nil,
		Params: shelleyParams,
	}
	// tipSlot = 150 (50 Shelley slots past the era start).
	s, err := hardfork.BuildSummary(shape, past, current, 150, hardfork.NewTransitionUnknown())
	require.NoError(t, err)
	require.Len(t, s.Eras, 2)

	assert.Equal(t, uint(0), s.Eras[0].EraID)
	require.NotNil(t, s.Eras[0].End)
	assert.Equal(t, uint64(100), s.Eras[0].End.Slot)

	assert.Equal(t, uint(1), s.Eras[1].EraID)
	require.NotNil(t, s.Eras[1].End, "TransitionUnknown + finite SafeZoneSlots ⇒ bounded end")
	// safe end = 150 + 25_920 = 26_070. Shelley epoch 1 = [100, 432_100).
	// snap to next epoch boundary → 432_100, epoch 2.
	assert.Equal(t, uint64(432_100), s.Eras[1].End.Slot)
	assert.Equal(t, uint64(2), s.Eras[1].End.Epoch)
}

// BuildSummary copies over transition info onto the summary.
func TestBuildSummary_CarriesTransitionInfo(t *testing.T) {
	shape := singleEraShape(conwayShapeEntry())
	current := hardfork.EraSummary{
		EraID:  6,
		Start:  hardfork.Bound{RelativeTime: 0, Slot: 100_000, Epoch: 500},
		Params: shelleyParams,
	}
	ti := hardfork.NewTransitionKnown(501)
	s, err := hardfork.BuildSummary(shape, nil, current, 200_000, ti)
	require.NoError(t, err)
	assert.Equal(t, ti, s.Transition)
}

// Sanity-check: after BuildSummary, SlotToTime/TimeToSlot work on the result.
func TestBuildSummary_RoundTripSlotTime(t *testing.T) {
	shape := singleEraShape(conwayShapeEntry())
	current := hardfork.EraSummary{
		EraID:  6,
		Start:  hardfork.Bound{RelativeTime: 0, Slot: 100_000, Epoch: 500},
		Params: shelleyParams,
	}
	s, err := hardfork.BuildSummary(shape, nil, current, 200_000, hardfork.NewTransitionKnown(501))
	require.NoError(t, err)

	when, err := s.SlotToTime(150_000)
	require.NoError(t, err)
	// 150_000 - 100_000 = 50_000 seconds past system start.
	assert.Equal(t, testSysStart.Add(50_000*time.Second), when)

	back, err := s.TimeToSlot(when)
	require.NoError(t, err)
	assert.Equal(t, uint64(150_000), back)
}
