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

package database

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// seedEpoch persists a single epoch row via the public SetEpoch API,
// matching the convention used by ledger/account_expiry_rollback_test.go.
func seedEpoch(
	t *testing.T,
	db *Database,
	epochId, startSlot uint64,
	lengthInSlots uint,
) {
	t.Helper()
	require.NoError(t, db.SetEpoch(
		startSlot,
		epochId,
		nil, nil, nil, nil,
		0,
		1,
		lengthInSlots,
		nil,
	))
}

// TestEpochBySlot_Boundaries pins EpochBySlot's lookup rules for a
// contiguous epoch table: each epoch owns [StartSlot, StartSlot+LengthInSlots),
// and slots before the first epoch or at or past the last epoch's end are
// errors rather than being attributed to the nearest epoch.
func TestEpochBySlot_Boundaries(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	// Three contiguous epochs: [10,110), [110,260), [260,460).
	seedEpoch(t, db, 0, 10, 100)
	seedEpoch(t, db, 1, 110, 150)
	seedEpoch(t, db, 2, 260, 200)
	tests := []struct {
		name      string
		slot      uint64
		wantEpoch uint64
		wantErr   bool
	}{
		{"before first epoch", 9, 0, true},
		{"lower boundary of epoch 0", 10, 0, false},
		{"upper boundary of epoch 0", 109, 0, false},
		{"lower boundary of epoch 1", 110, 1, false},
		{"middle of epoch 1", 200, 1, false},
		{"upper boundary of epoch 1", 259, 1, false},
		{"lower boundary of epoch 2", 260, 2, false},
		{"upper boundary of epoch 2", 459, 2, false},
		{"first slot past last epoch", 460, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EpochBySlot(db, tt.slot, nil)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantEpoch, got.EpochId)
		})
	}
}
