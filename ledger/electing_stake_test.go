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

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func electingTestPool(b byte) lcommon.PoolKeyHash {
	var pool lcommon.PoolKeyHash
	pool[0] = b
	return pool
}

func TestElectingPoolStakeReadsThePraosMarkSnapshot(t *testing.T) {
	t.Parallel()
	ls, db := newEligibilityTestLedger(t, nil)
	staked := electingTestPool(1)
	// The ledger is in epoch 5, which mark[4] elects. A row in mark[5] is
	// the distribution for the next epoch and must not authorize anyone yet.
	seedPoolStakeSnapshotOfType(
		t, db, 4, models.PoolStakeSnapshotTypeMark, staked[:], 700, 0,
	)
	late := electingTestPool(2)
	seedPoolStakeSnapshotOfType(
		t, db, 5, models.PoolStakeSnapshotTypeMark, late[:], 900, 0,
	)

	stake, err := ls.ElectingPoolStake(10, staked)
	require.NoError(t, err)
	require.Equal(t, uint64(700), stake)

	stake, err = ls.ElectingPoolStake(10, late)
	require.NoError(t, err, "absence from a populated snapshot is an answer")
	require.Zero(t, stake)
}

func TestElectingPoolStakeEmptySnapshotIsUnavailable(t *testing.T) {
	t.Parallel()
	ls, _ := newEligibilityTestLedger(t, nil)

	_, err := ls.ElectingPoolStake(10, electingTestPool(1))
	require.ErrorIs(t, err, ErrLeaderStakeSnapshotUnavailable)
}

func TestElectingPoolStakeSlotBeyondLedgerEpochIsUnavailable(t *testing.T) {
	t.Parallel()
	ls, db := newEligibilityTestLedger(t, nil)
	pool := electingTestPool(1)
	seedPoolStakeSnapshotOfType(
		t, db, 4, models.PoolStakeSnapshotTypeMark, pool[:], 700, 0,
	)

	// Epoch 5 ends at slot 1,000,000. A wall clock past it is in an epoch
	// whose electing snapshot the ledger has not captured.
	_, err := ls.ElectingPoolStake(1_000_000, pool)
	require.ErrorIs(t, err, ErrLeaderStakeSnapshotUnavailable)
}

func TestElectingPoolStakeUsesImportedActiveDistribution(t *testing.T) {
	t.Parallel()
	tb := createTestBlock(t, [32]byte{42}, 0, tamperNone)
	ls, db := newImportedActiveLedger(t, tb)
	pool := electingTestPool(1)
	seedPoolStakeSnapshotOfType(
		t, db, 4, models.PoolStakeSnapshotTypeMark, pool[:], 1, 0,
	)
	seedPoolStakeSnapshotOfType(
		t, db, 5, models.PoolStakeSnapshotTypeActive, pool[:], 500, 1_000,
	)

	stake, err := ls.ElectingPoolStake(tb.block.slot, pool)
	require.NoError(t, err)
	require.Equal(t, uint64(500), stake)
}

func TestElectingPoolStakeImportedZeroDenominatorIsUnavailable(t *testing.T) {
	t.Parallel()
	tb := createTestBlock(t, [32]byte{43}, 0, tamperNone)
	ls, db := newImportedActiveLedger(t, tb)
	pool := electingTestPool(1)
	seedPoolStakeSnapshotOfType(
		t, db, 5, models.PoolStakeSnapshotTypeActive, pool[:], 500, 0,
	)

	_, err := ls.ElectingPoolStake(tb.block.slot, pool)
	require.ErrorIs(t, err, ErrLeaderStakeSnapshotUnavailable)
}

// A mark row reconstructed after the boundary it stands for cannot say which
// pools that boundary elected, so header verification treats it as
// unevaluable unless SkipLeaderStakeThresholdCheck overrides it.
func TestElectingPoolStakeReconstructedMarkFollowsHeaderVerification(
	t *testing.T,
) {
	t.Parallel()
	for _, override := range []bool{false, true} {
		ls, db := newEligibilityTestLedger(t, nil)
		ls.config.SkipLeaderStakeThresholdCheck = override
		ls.epochCache = []models.Epoch{
			{EpochId: 3, StartSlot: 300, LengthInSlots: 100},
			{EpochId: 4, StartSlot: 400, LengthInSlots: 100},
			{EpochId: 5, StartSlot: 500, LengthInSlots: 100},
		}
		ls.mithrilLedgerSlot = 450
		ls.publishSnapshotsLocked()
		pool := electingTestPool(1)
		seedPoolStakeSnapshotOfTypeAtSlot(
			t, db, 4, models.PoolStakeSnapshotTypeMark, pool[:], 700, 0, 400,
		)

		stake, err := ls.ElectingPoolStake(550, pool)
		if override {
			require.NoError(t, err)
			require.Equal(t, uint64(700), stake)
			continue
		}
		require.ErrorIs(t, err, ErrLeaderStakeSnapshotUnavailable)
	}
}
