// Copyright 2025 Blink Labs Software
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
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
)

func TestSlotCalc(t *testing.T) {
	testLedgerState := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				SlotLength:    1000,
				LengthInSlots: 86400,
			},
			{
				EpochId:       1,
				StartSlot:     86400,
				SlotLength:    1000,
				LengthInSlots: 86400,
			},
			{
				EpochId:       2,
				StartSlot:     172800,
				SlotLength:    1000,
				LengthInSlots: 86400,
			},
			{
				EpochId:       3,
				StartSlot:     259200,
				SlotLength:    1000,
				LengthInSlots: 86400,
			},
			{
				EpochId:       4,
				StartSlot:     345600,
				SlotLength:    1000,
				LengthInSlots: 86400,
			},
			{
				EpochId:       5,
				StartSlot:     432000,
				SlotLength:    1000,
				LengthInSlots: 86400,
			},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: &cardano.CardanoNodeConfig{},
		},
	}
	testShelleyGenesis := `{"systemStart": "2022-10-25T00:00:00Z"}`
	if err := testLedgerState.config.CardanoNodeConfig.LoadShelleyGenesisFromReader(strings.NewReader(testShelleyGenesis)); err != nil {
		t.Fatalf("unexpected error loading cardano node config: %s", err)
	}
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
	}

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
	testLedgerState := &LedgerState{
		epochCache: []models.Epoch{},
	}

	_, err := testLedgerState.SlotToEpoch(100)
	if err == nil {
		t.Error("expected error for empty epoch cache")
	}
	if err.Error() != "no epochs in cache" {
		t.Errorf("unexpected error message: %s", err.Error())
	}
}

func TestSlotToEpochBeforeFirstEpoch(t *testing.T) {
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

	// Slot before first known epoch should error
	_, err := testLedgerState.SlotToEpoch(100)
	if err == nil {
		t.Error("expected error for slot before first known epoch")
	}
	if err.Error() != "slot is before the first known epoch" {
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
