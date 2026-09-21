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

package lifecycle_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestTruncateRejectsTargetBeforeConsumedUtxoRetentionFloor reproduces the
// same defect class fixed for block_nonce retention
// (TestTruncateAfterSlotRejectsTargetWithPrunedNonce), found by auditing
// TruncateAfterSlot's other retention-window interactions.
//
// During normal operation, ledger.LedgerState's cleanupConsumedUtxos
// periodically hard-deletes spent UTxO rows once their DeletedSlot falls
// at-or-behind a floor it durably records in sync_state
// (database.ConsumedUtxoPruneFloorSyncKey). TruncateAfterSlot's doc comment
// promises that "UTxOs spent after point.Slot are restored as unspent" --
// but a spent UTxO whose row has already been hard-deleted by that cleanup
// cannot be restored: UtxosUnspend's bulk UPDATE simply matches zero rows
// for it, and TruncateAfterSlot returns success anyway. A disaster-recovery
// truncate (database/lifecycle.Truncate, which unlike the security-
// parameter-bounded live ledger rollback may target a point far older than
// the retention window) can hit this silently, permanently losing UTxOs
// the surviving chain still needs and corrupting balances/validation with
// no error at truncate time to explain why.
//
// This test does not need real cleanup to run: it writes the durable
// marker directly (exactly as persistConsumedUtxoPruneFloor would) to
// simulate "cleanup already pruned everything at or below this slot," then
// shows Truncate must refuse a target older than that marker.
func TestTruncateRejectsTargetBeforeConsumedUtxoRetentionFloor(t *testing.T) {
	t.Parallel()

	f := buildTestChain(t, 5)
	floorSlot := f.blocks[3].Slot
	require.NoError(t, f.db.SetSyncState(
		database.ConsumedUtxoPruneFloorSyncKey,
		strconv.FormatUint(floorSlot, 10),
		nil,
	))

	// blocks[2] is strictly before the floor: a UTxO consumed between its
	// slot and the floor may already be physically gone. Before the fix,
	// this silently succeeded (returning a Tip whose live UTxO set was
	// missing whatever cleanup had already deleted); it must now be
	// refused before any mutation happens.
	blocksRemoved, err := lifecycle.Truncate(
		context.Background(),
		f.db,
		f.blocks[2],
		0,
		false,
		0,
	)
	require.Error(t, err,
		"Truncate must reject a target older than the consumed-UTxO "+
			"retention floor instead of silently proceeding -- routine "+
			"cleanup may have already hard-deleted UTxOs TruncateAfterSlot "+
			"promises to restore as unspent")
	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)
	require.Contains(t, err.Error(), "consumed-UTxO retention floor")
	require.Equal(t, uint64(0), blocksRemoved)

	// Nothing was mutated: the tip and every block must be untouched.
	tip, tipErr := f.db.GetTip(nil)
	require.NoError(t, tipErr)
	require.Equal(t, f.blocks[4].Slot, tip.Point.Slot)

	// blocks[3] is exactly at the floor and must be allowed (mirrors
	// TestTruncateRejectsTargetBeforeMithrilBoundary's boundary case).
	blocksRemoved, err = lifecycle.Truncate(
		context.Background(), f.db, f.blocks[3], 0, false, 0,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(1), blocksRemoved)
}

// TestTruncateAllowsDeepTargetInApiStorageMode verifies the consumed-UTxO
// retention floor check does not apply in API storage mode, where
// cleanupConsumedUtxos never hard-deletes spent rows at all (they are
// retained indefinitely for historical queries) -- so a floor marker
// existing at all in that mode would be surprising, but even if present it
// must not block an otherwise-valid deep truncate.
func TestTruncateAllowsDeepTargetInApiStorageMode(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     t.TempDir(),
		StorageMode: types.StorageModeAPI,
	})
	require.NoError(t, err)

	blocks := make([]models.Block, 0, 5)
	for id := uint64(1); id <= 5; id++ {
		block := testBlock(id, byte(id))
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
	}
	last := blocks[len(blocks)-1]
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: last.Slot, Hash: last.Hash},
		BlockNumber: last.Number,
	}, nil))

	// A floor deeper than every block in the chain: if the check applied
	// here, every target below it (all of them) would be refused.
	require.NoError(t, db.SetSyncState(
		database.ConsumedUtxoPruneFloorSyncKey,
		strconv.FormatUint(last.Slot+1000, 10),
		nil,
	))

	blocksRemoved, err := lifecycle.Truncate(
		context.Background(), db, blocks[2], 0, false, 0,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(2), blocksRemoved)
}
