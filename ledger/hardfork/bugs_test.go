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
	"math"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSummary_Validate_RejectsZeroEpochSize ensures Summary.Validate() catches
// a per-era EraParams with EpochSize==0 before SlotToEpoch panics on division
// by zero. Without per-era Params validation inside Validate(), a malformed
// Summary can be "valid" and then panic at use.
func TestSummary_Validate_RejectsZeroEpochSize(t *testing.T) {
	s := hardfork.Summary{
		SystemStart: time.Now(),
		Eras: []hardfork.EraSummary{{
			EraID:  1,
			Start:  hardfork.Bound{},
			End:    nil,
			Params: hardfork.EraParams{EpochSize: 0, SlotLength: time.Second},
		}},
	}
	assert.Error(t, s.Validate(),
		"Validate must reject a Summary with a zero-EpochSize era (SlotToEpoch would divide by zero)")
}

// TestSummary_Validate_RejectsZeroSlotLength mirrors the EpochSize check for
// SlotLength, which TimeToSlot divides by.
func TestSummary_Validate_RejectsZeroSlotLength(t *testing.T) {
	s := hardfork.Summary{
		SystemStart: time.Now(),
		Eras: []hardfork.EraSummary{{
			EraID:  1,
			Start:  hardfork.Bound{},
			End:    nil,
			Params: hardfork.EraParams{EpochSize: 10, SlotLength: 0},
		}},
	}
	assert.Error(t, s.Validate(),
		"Validate must reject a Summary with a zero-SlotLength era (TimeToSlot would divide by zero)")
}

// TestSummary_Validate_ErrorMessageFormatsEraIDAsDecimal pins that the
// "unbounded but not last" error uses %d, not %q. %q on a uint formats as a
// rune literal (e.g., '\x05'), which is unreadable and incorrect for an
// ordinal era ID.
func TestSummary_Validate_ErrorMessageFormatsEraIDAsDecimal(t *testing.T) {
	end := hardfork.Bound{RelativeTime: 100, Slot: 10, Epoch: 1}
	s := hardfork.Summary{
		SystemStart: time.Now(),
		Eras: []hardfork.EraSummary{
			{
				EraID:  5,
				Start:  hardfork.Bound{},
				End:    nil, // unbounded, but not the last era
				Params: hardfork.EraParams{EpochSize: 10, SlotLength: time.Second},
			},
			{
				EraID:  7,
				Start:  end,
				End:    nil,
				Params: hardfork.EraParams{EpochSize: 10, SlotLength: time.Second},
			},
		},
	}
	err := s.Validate()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "5",
		"error message should include the decimal era ID (5), got: %s", msg)
	// %q on uint(5) produces '\x05' — should never appear in a well-formatted
	// error message.
	assert.NotContains(t, msg, `'\x05'`,
		"error message should NOT contain a rune literal, got: %s", msg)
}

// TestShape_Validate_VersionContiguityHandlesUintOverflow guards against the
// subtle bug in `prev.MaxMajorVersion + 1` when MaxMajorVersion is MaxUint:
// the arithmetic wraps to 0 and a subsequent era with MinMajorVersion=0 is
// incorrectly accepted.
func TestShape_Validate_VersionContiguityHandlesUintOverflow(t *testing.T) {
	s := hardfork.Shape{
		SystemStart: time.Now(),
		Eras: []hardfork.ShapeEntry{
			{
				EraID: 0, EraName: "A",
				MinMajorVersion: 0, MaxMajorVersion: math.MaxUint,
				Params: hardfork.EraParams{EpochSize: 10, SlotLength: time.Second, SafeZoneSlots: 1},
			},
			{
				EraID: 1, EraName: "B",
				MinMajorVersion: 0, MaxMajorVersion: 0,
				Params: hardfork.EraParams{EpochSize: 10, SlotLength: time.Second, SafeZoneSlots: 1},
			},
		},
	}
	err := s.Validate()
	require.Error(t, err,
		"Validate must reject a shape where the contiguity check would overflow uint (prev.Max=MaxUint, cur.Min=0)")
	// The message should reference the era name so the operator can spot the bad entry.
	assert.True(t, strings.Contains(err.Error(), "\"B\"") || strings.Contains(err.Error(), "B"),
		"error should identify the offending era, got: %s", err.Error())
}

