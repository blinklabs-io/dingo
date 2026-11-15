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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// seedRewardEpoch gives the metadata store every reward row of epoch, all
// captured at slot.
func seedRewardEpoch(
	t *testing.T,
	db *database.Database,
	epoch, slot uint64,
) {
	t.Helper()
	require.NoError(t, db.SetEpoch(
		slot, epoch, nil, nil, nil, nil, 6, 1000, 100, nil,
	))
	meta := db.Metadata()
	pool := bytes.Repeat([]byte{0x31}, 28)
	cred := bytes.Repeat([]byte{0x41}, 28)
	require.NoError(t, meta.SaveRewardAdaPots(
		&models.RewardAdaPots{Epoch: epoch, CapturedSlot: slot}, nil,
	))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch: epoch, SnapshotType: "mark", CapturedSlot: slot,
		BoundarySlot: slot, Authoritative: true,
	}, nil))
	require.NoError(t, meta.SaveRewardSeedFailure(
		epoch, "mark", "imported basis", slot, nil,
	))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{{
		Epoch: epoch, PoolKeyHash: pool, CapturedSlot: slot, BoundarySlot: slot,
	}}, nil))
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{{
		Epoch: epoch, PoolKeyHash: pool, StakingKey: cred, Stake: 5,
		CapturedSlot: slot, BoundarySlot: slot,
	}}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{{
		Epoch: epoch, PoolKeyHash: pool, CapturedSlot: slot,
		BoundarySlot: slot + 1,
	}}, nil))
	require.NoError(t, meta.SaveRewardAccountOutputs(
		[]*models.RewardAccountOutput{{
			Epoch: epoch, PoolKeyHash: pool, StakingKey: cred,
			RewardType: "member", Amount: 3, Spendable: true,
			CapturedSlot: slot, BoundarySlot: slot + 1,
		}}, nil,
	))
	require.NoError(t, db.SetSyncState(
		ledger.RewardPrecomputeCursorKey(epoch),
		fmt.Sprintf(`{"captured_slot":%d}`, slot), nil,
	))
}

// sqlRewardRows lists what the metadata store holds of epoch's reward rows.
func sqlRewardRows(
	t *testing.T,
	db *database.Database,
	epoch uint64,
) string {
	t.Helper()
	meta := db.Metadata()
	pots, err := meta.GetRewardAdaPots(epoch, nil)
	require.NoError(t, err)
	snap, err := meta.GetRewardSnapshot(epoch, "mark", nil)
	require.NoError(t, err)
	failure, err := meta.GetRewardSeedFailure(epoch, "mark", nil)
	require.NoError(t, err)
	pools, err := meta.GetRewardPoolInputs(epoch, nil)
	require.NoError(t, err)
	stakes, err := meta.GetRewardStakeInputs(epoch, nil)
	require.NoError(t, err)
	poolOut, err := meta.GetRewardPoolOutputs(epoch, nil)
	require.NoError(t, err)
	accOut, err := meta.GetRewardAccountOutputs(epoch, nil)
	require.NoError(t, err)
	return fmt.Sprintf(
		"pots=%v snap=%v failure=%v pools=%d stakes=%d poolOut=%d accOut=%d",
		pots != nil, snap != nil, failure != "", len(pools), len(stakes),
		len(poolOut), len(accOut),
	)
}

func setRewardRows(
	t *testing.T,
	set *utxoset.Set,
	epoch uint64,
) string {
	t.Helper()
	pots, err := set.RewardPots(epoch, nil)
	require.NoError(t, err)
	snap, err := set.RewardSnapshotRow(epoch, "mark", nil)
	require.NoError(t, err)
	failure, _, err := set.RewardFailureRow(epoch, "mark", nil)
	require.NoError(t, err)
	pools, err := set.RewardPoolRows(epoch, nil)
	require.NoError(t, err)
	stakes, err := set.RewardStakeRows(epoch, nil, nil, nil)
	require.NoError(t, err)
	poolOut, err := set.RewardPoolOutputRows(epoch, nil)
	require.NoError(t, err)
	accOut, err := set.RewardAccountOutputRows(epoch, nil)
	require.NoError(t, err)
	return fmt.Sprintf(
		"pots=%v snap=%v failure=%v pools=%d stakes=%d poolOut=%d accOut=%d",
		pots != nil, snap != nil, failure != "", len(pools), len(stakes),
		len(poolOut), len(accOut),
	)
}

// hasSetCursor reports whether the set holds epoch's precompute cursor. The
// metadata store's cursor is a sync key its rollback does not touch, so it is
// not compared with the store's.
func hasSetCursor(t *testing.T, set *utxoset.Set, epoch uint64) bool {
	t.Helper()
	cursor, ok, err := set.RewardCursorRow(epoch, nil)
	require.NoError(t, err)
	return ok && len(cursor) > 0
}

// Reward rows a set loads from the metadata store on upgrade are dropped by a
// rollback below the slot they were captured at, as the metadata store drops
// them, and kept by one at or above it.
func TestLoadedRewardRowsAreDroppedByARollbackBelowTheirCapture(t *testing.T) {
	t.Parallel()
	set := openRecoverySet(t, utxoset.Config{})
	applySlots(t, set, 8)
	db := ledgerAt(t, slotPoint(8))
	seedRewardEpoch(t, db, 2, 3)
	seedRewardEpoch(t, db, 3, 7)

	require.NoError(t, set.LoadLedgerState(t.Context(), dbadapter.New(db)))
	for _, epoch := range []uint64{2, 3} {
		require.Equal(
			t, sqlRewardRows(t, db, epoch), setRewardRows(t, set, epoch),
			"epoch %d as loaded", epoch,
		)
	}
	require.Contains(t, setRewardRows(t, set, 3), "stakes=1 poolOut=1 accOut=1")
	require.True(t, hasSetCursor(t, set, 3))

	for _, p := range []uint64{7, 5} {
		_, err := set.Rollback(slotPoint(p))
		require.NoError(t, err)
		require.NoError(t, db.DeleteRewardStateAfterSlot(p, nil))
		for _, epoch := range []uint64{2, 3} {
			require.Equal(
				t, sqlRewardRows(t, db, epoch), setRewardRows(t, set, epoch),
				"epoch %d after a rollback to %d", epoch, p,
			)
		}
	}
	require.Equal(
		t,
		"pots=false snap=false failure=false pools=0 stakes=0 poolOut=0 "+
			"accOut=0",
		setRewardRows(t, set, 3),
	)
	require.False(t, hasSetCursor(t, set, 3))
	require.True(t, hasSetCursor(t, set, 2))
	require.Contains(t, setRewardRows(t, set, 2), "pots=true snap=true")
}
