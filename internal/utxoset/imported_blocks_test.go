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

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/stretchr/testify/require"
)

// The imported block counts a set loads from the metadata store are kept as
// imported, with the slot they were captured at, and a rollback below that
// slot drops them as the metadata store does.
func TestLoadedImportedBlocksAreDroppedByARollbackBelowTheirCapture(
	t *testing.T,
) {
	t.Parallel()
	set := openRecoverySet(t, utxoset.Config{})
	applySlots(t, set, 8)
	db := ledgerAt(t, slotPoint(8))
	pool := bytes.Repeat([]byte{0x31}, 28)
	meta := db.Metadata()
	for _, c := range []struct{ epoch, slot, blocks uint64 }{
		{2, 3, 4}, {3, 7, 5},
	} {
		require.NoError(t, meta.SaveImportedPoolBlockCounts(
			[]models.ImportedPoolBlockCount{{
				PoolKeyHash: pool, Epoch: c.epoch, BlocksProduced: c.blocks,
				CapturedSlot: c.slot,
			}}, nil,
		))
		require.NoError(t, meta.SaveImportedEpochBlockTotal(
			c.epoch, c.blocks, c.slot, nil,
		))
	}

	require.NoError(t, set.LoadLedgerState(t.Context(), dbadapter.New(db)))
	got, err := set.ImportedBlocks()
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, uint64(5), got[3].Total)
	require.Equal(t, uint64(7), got[3].CapturedSlot)
	require.Equal(t, uint64(5), got[3].Pools[string(pool)])
	loaded, err := set.ImportedBlocksLoaded()
	require.NoError(t, err)
	require.True(t, loaded)

	_, err = set.Rollback(slotPoint(7))
	require.NoError(t, err)
	got, err = set.ImportedBlocks()
	require.NoError(t, err)
	require.Len(t, got, 2, "a rollback to the capture slot keeps the rows")

	_, err = set.Rollback(slotPoint(5))
	require.NoError(t, err)
	got, err = set.ImportedBlocks()
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Contains(t, got, uint64(2))
}
