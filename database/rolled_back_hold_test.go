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
	"bytes"
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func retainRolledBackTestBlocks(t *testing.T, db *Database, ids ...uint64) {
	t.Helper()
	for _, id := range ids {
		block := rollbackIntentTestBlock(id)
		require.NoError(t, db.RetainRolledBackBlock(
			block.Slot, block.Hash, block.Cbor,
		))
	}
}

// While the IndexerDB's tip is one of the blocks a rollback removed, its rows
// still name those blocks, so release keeps every body up to the tip's slot
// and deletes the rest. Once the writer has rolled back and its tip is a
// chain block, the held bodies go too.
func TestReleaseRolledBackBlocksHoldsBodiesTheIndexerStillNames(t *testing.T) {
	t.Parallel()
	db := rollbackIntentTestDB(t)
	retainRolledBackTestBlocks(t, db, 1, 2, 3)
	tip := rollbackIntentTestBlock(2)
	hold := func() (ocommon.Point, bool, error) {
		return ocommon.Point{Slot: tip.Slot, Hash: tip.Hash}, true, nil
	}
	db.SetRolledBackBlockHold(hold)

	deleted, err := db.ReleaseRolledBackBlocks(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, deleted, "only the body above the tip is released")
	require.Equal(t, 2, rolledBackBlockCount(t, db))
	for _, id := range []uint64{1, 2} {
		block := rollbackIntentTestBlock(id)
		_, found, err := db.RolledBackBlock(block.Slot, block.Hash)
		require.NoError(t, err)
		require.True(t, found, "body %d was released while held", id)
	}

	// The writer rolled back to a block the chain holds.
	chainTip := ocommon.Point{Slot: 5, Hash: bytes.Repeat([]byte{0xee}, 32)}
	db.SetRolledBackBlockHold(
		func() (ocommon.Point, bool, error) { return chainTip, true, nil },
	)
	deleted, err = db.ReleaseRolledBackBlocks(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, deleted)
	require.Zero(t, rolledBackBlockCount(t, db))
}

func TestReleaseRolledBackBlocksWithoutAHoldDeletesEveryBody(t *testing.T) {
	t.Parallel()
	db := rollbackIntentTestDB(t)
	retainRolledBackTestBlocks(t, db, 1, 2, 3)
	db.SetRolledBackBlockHold(
		func() (ocommon.Point, bool, error) {
			return ocommon.Point{}, false, nil
		},
	)
	deleted, err := db.ReleaseRolledBackBlocks(t.Context())
	require.NoError(t, err)
	require.Equal(t, 3, deleted)
	require.Zero(t, rolledBackBlockCount(t, db))
}
