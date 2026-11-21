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

package utxoset_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// A range below the journal's floor is served from the DReps added in it, so
// each segment of a replay reads its own DReps and not the table.
func TestDrepsChangedInServesARangeBelowTheFloor(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	const n = 20
	for slot := uint64(1); slot <= n; slot++ {
		applyAt(t, set, slot, ledger.LedgerChanges{
			DReps: []ledger.DRepChange{{Slot: slot, Drep: models.Drep{
				Credential: bytes.Repeat([]byte{byte(slot)}, 28),
				AddedSlot:  slot, Active: true,
			}}},
		})
	}
	set.UpdateAnchor(slotPoint(n))
	require.NoError(t, set.PruneJournalForTest(n))

	var slots []uint64
	visited := set.RangeRowsVisited()
	_, covered, done, err := set.DrepsChangedIn(3, 6, nil, 1000,
		func(d *models.Drep, slot uint64, _ []byte) error {
			require.Equal(t, d.AddedSlot, slot)
			slots = append(slots, slot)
			return nil
		})
	require.NoError(t, err)
	require.True(t, covered, "the range is below the journal's floor")
	require.True(t, done)
	require.Equal(t, []uint64{3, 4, 5, 6}, slots)
	require.LessOrEqual(t, set.RangeRowsVisited()-visited, uint64(5))
}
