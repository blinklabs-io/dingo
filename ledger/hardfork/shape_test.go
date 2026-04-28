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

	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sampleShape() hardfork.Shape {
	return hardfork.Shape{
		SystemStart: testSysStart,
		Eras: []hardfork.ShapeEntry{
			{EraID: 0, EraName: "Byron", MinMajorVersion: 0, MaxMajorVersion: 1, Params: byronParams, NextEraTrigger: hardfork.NewTriggerAtVersion(2)},
			{EraID: 1, EraName: "Shelley", MinMajorVersion: 2, MaxMajorVersion: 2, Params: shelleyParams, NextEraTrigger: hardfork.NewTriggerAtVersion(3)},
			{EraID: 2, EraName: "Allegra", MinMajorVersion: 3, MaxMajorVersion: 3, Params: shelleyParams, NextEraTrigger: hardfork.NewTriggerAtVersion(4)},
			{EraID: 3, EraName: "Mary", MinMajorVersion: 4, MaxMajorVersion: 4, Params: shelleyParams, NextEraTrigger: hardfork.NewTriggerAtVersion(5)},
			{EraID: 4, EraName: "Alonzo", MinMajorVersion: 5, MaxMajorVersion: 6, Params: shelleyParams, NextEraTrigger: hardfork.NewTriggerAtVersion(7)},
			{EraID: 5, EraName: "Babbage", MinMajorVersion: 7, MaxMajorVersion: 8, Params: shelleyParams, NextEraTrigger: hardfork.NewTriggerAtVersion(9)},
			{EraID: 6, EraName: "Conway", MinMajorVersion: 9, MaxMajorVersion: 10, Params: shelleyParams, NextEraTrigger: hardfork.NewTriggerNotDuringThisExecution()},
		},
	}
}

func TestShape_Validate_OK(t *testing.T) {
	s := sampleShape()
	assert.NoError(t, s.Validate())
}

func TestShape_Validate_RejectsEmpty(t *testing.T) {
	s := hardfork.Shape{SystemStart: testSysStart}
	assert.Error(t, s.Validate())
}

func TestShape_Validate_RejectsVersionGap(t *testing.T) {
	// Byron's MaxMajorVersion=1, but Shelley's Min=3 (gap at version 2).
	s := hardfork.Shape{
		SystemStart: testSysStart,
		Eras: []hardfork.ShapeEntry{
			{EraID: 0, EraName: "Byron", MinMajorVersion: 0, MaxMajorVersion: 1, Params: byronParams},
			{EraID: 1, EraName: "Shelley", MinMajorVersion: 3, MaxMajorVersion: 3, Params: shelleyParams},
		},
	}
	assert.Error(t, s.Validate())
}

func TestShape_Validate_RejectsVersionOverlap(t *testing.T) {
	// Byron.Max=2, Shelley.Min=2 — overlap.
	s := hardfork.Shape{
		SystemStart: testSysStart,
		Eras: []hardfork.ShapeEntry{
			{EraID: 0, EraName: "Byron", MinMajorVersion: 0, MaxMajorVersion: 2, Params: byronParams},
			{EraID: 1, EraName: "Shelley", MinMajorVersion: 2, MaxMajorVersion: 2, Params: shelleyParams},
		},
	}
	assert.Error(t, s.Validate())
}

func TestShape_EraForVersion(t *testing.T) {
	s := sampleShape()
	tests := []struct {
		version uint
		wantID  uint
		found   bool
	}{
		{0, 0, true},   // Byron
		{1, 0, true},   // Byron
		{2, 1, true},   // Shelley
		{3, 2, true},   // Allegra
		{9, 6, true},   // Conway
		{10, 6, true},  // Conway
		{11, 0, false}, // unknown
	}
	for _, tc := range tests {
		entry, ok := s.EraForVersion(tc.version)
		assert.Equal(t, tc.found, ok, "version=%d", tc.version)
		if ok {
			assert.Equal(t, tc.wantID, entry.EraID)
		}
	}
}

func TestShape_EraForID(t *testing.T) {
	s := sampleShape()
	entry, ok := s.EraForID(1)
	require.True(t, ok)
	assert.Equal(t, "Shelley", entry.EraName)

	_, ok = s.EraForID(99)
	assert.False(t, ok)
}

func TestShape_EraIndex(t *testing.T) {
	s := sampleShape()
	idx, ok := s.EraIndex(0)
	require.True(t, ok)
	assert.Equal(t, 0, idx)

	idx, ok = s.EraIndex(6)
	require.True(t, ok)
	assert.Equal(t, 6, idx)

	_, ok = s.EraIndex(99)
	assert.False(t, ok)
}

// Validate must reject a shape whose non-final era carries
// TriggerNotDuringThisExecution — that variant is reserved for the final era.
func TestShape_Validate_RejectsNonFinalNotDuringThisExecution(t *testing.T) {
	s := sampleShape()
	// Force Byron (non-final) to carry NotDuringThisExecution.
	s.Eras[0].NextEraTrigger = hardfork.NewTriggerNotDuringThisExecution()
	err := s.Validate()
	assert.ErrorContains(t, err, "NotDuringThisExecution")
}

// Validate must reject a shape whose final era carries a trigger other than
// NotDuringThisExecution.
func TestShape_Validate_RejectsFinalWithSuccessorTrigger(t *testing.T) {
	s := sampleShape()
	// Force Conway (final) to carry AtEpoch.
	s.Eras[len(s.Eras)-1].NextEraTrigger = hardfork.NewTriggerAtEpoch(100)
	err := s.Validate()
	assert.ErrorContains(t, err, "final")
}

// AtEpoch triggers on non-final eras are accepted.
func TestShape_Validate_AcceptsAtEpochOnNonFinal(t *testing.T) {
	s := sampleShape()
	s.Eras[0].NextEraTrigger = hardfork.NewTriggerAtEpoch(5)
	assert.NoError(t, s.Validate())
}

// Validate must reject a trigger whose Kind is not one of the three known
// variants, so future refactors / external constructors can't quietly produce
// a shape that passes the final/non-final checks with a bogus kind.
func TestShape_Validate_RejectsUnknownTriggerKind(t *testing.T) {
	s := sampleShape()
	s.Eras[0].NextEraTrigger = hardfork.TriggerHardFork{Kind: 99}
	err := s.Validate()
	assert.ErrorContains(t, err, "unknown kind")
}
