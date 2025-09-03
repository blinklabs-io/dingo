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
	"github.com/blinklabs-io/dingo/database"
)

func TestSlotCalc(t *testing.T) {
	testLedgerState := &LedgerState{
		epochCache: []database.Epoch{
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
