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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// crossEraLedger returns a LedgerState with two eras:
//   - Byron-ish: EraId=0, 20s slots, 100 slots/epoch, 2 epochs → slots 0..199
//   - Shelley-ish: EraId=1, 1s slots, 432 slots/epoch, 2 epochs → slots 200..1063
//
// Total Byron time = 2 × 100 × 20s = 4000s.
func crossEraLedger(t *testing.T) *LedgerState {
	t.Helper()
	return &LedgerState{
		epochCache: []models.Epoch{
			{EpochId: 0, StartSlot: 0, SlotLength: 20_000, LengthInSlots: 100, EraId: 0},
			{EpochId: 1, StartSlot: 100, SlotLength: 20_000, LengthInSlots: 100, EraId: 0},
			{EpochId: 2, StartSlot: 200, SlotLength: 1000, LengthInSlots: 432, EraId: 1},
			{EpochId: 3, StartSlot: 632, SlotLength: 1000, LengthInSlots: 432, EraId: 1},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: minimalShelleyGenesisCfg(t),
		},
	}
}

// TestSlotToTime_CrossEra covers multi-era chains where per-era slot length
// differs. Any implementation that reads slot length from the epoch currently
// being traversed — whether the legacy loop or the new Summary-backed
// delegation — must return the same absolute times.
func TestSlotToTime_CrossEra(t *testing.T) {
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
