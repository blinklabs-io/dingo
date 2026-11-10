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
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lateBlockRewardWindow is 4k/f for the genesis below (k=2, f=0.2). It is
// smaller than the 100-slot fixture epoch, so a block can land after it.
const lateBlockRewardWindow = uint64(40)

func lateBlockRewardNodeConfig(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, completeTestShelleyGenesis(cfg).LoadShelleyGenesisFromReader(
		strings.NewReader(completeTestShelleyGenesisJSON(t, map[string]any{
			"activeSlotsCoeff":  0.2,
			"epochLength":       100,
			"maxLovelaceSupply": 100010000,
			"securityParam":     2,
			"slotLength":        1,
			"systemStart":       "2022-10-25T00:00:00Z",
		}, nil)),
	))
	return cfg
}

// applyLateBlockRewardRound applies the newEpoch 4 reward round over the
// retention fixture, whose ended epoch 3 starts at slot 200, after seeding
// one block_nonce row per entry of blockSlots, and reports the owner's reward
// before and after, the number of pool outputs the round persisted, and how
// many seeded blocks sit after epochStart + 4k/f of the ended epoch.
func applyLateBlockRewardRound(
	t *testing.T,
	blockSlots []uint64,
) (before, after uint64, poolOutputs int, lateBlocks int) {
	t.Helper()
	fixture := seedRetentionReconstructionFixture(t)
	ls, db := fixture.ls, fixture.db
	ls.config.CardanoNodeConfig = lateBlockRewardNodeConfig(t)
	require.Equal(t, lateBlockRewardWindow, ls.rewardUpdateStabilityWindow())

	for i, slot := range blockSlots {
		require.NoError(t, db.SetBlockNonce(
			bytes.Repeat([]byte{byte(0xb0 + i)}, 32), slot, nil, false, nil,
		))
	}

	const endedEpochStart, endedEpochLength = uint64(200), uint64(100)
	late, err := db.GetBlockNoncesInSlotRange(
		endedEpochStart+lateBlockRewardWindow+1,
		endedEpochStart+endedEpochLength,
		nil,
	)
	require.NoError(t, err)

	owner, err := db.GetAccountByCredential(
		context.Background(), 0, fixture.ownerKey, false, nil,
	)
	require.NoError(t, err)

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(
			context.Background(), txn, retentionNewEpoch, retentionBoundarySlot,
		)
	}))
	settleRewardCredits(t, ls)

	owner2, err := db.GetAccountByCredential(
		context.Background(), 0, fixture.ownerKey, false, nil,
	)
	require.NoError(t, err)
	outputs, err := db.Metadata().GetRewardPoolOutputs(
		retentionRewardSnapshotEpoch, nil,
	)
	require.NoError(t, err)
	return uint64(owner.Reward), uint64(owner2.Reward), len(outputs), len(late)
}

// TestRewardUpdateAppliesWithBlockAfterStabilityWindow is the control: the
// reference applies the reward update when a block lands after
// epochStart + 4k/f in the ended epoch. It asserts the late block is present
// in the fixture, but the reward round applies with or without it today, so
// it only guards against a late-block gate that rejects the present-block case.
func TestRewardUpdateAppliesWithBlockAfterStabilityWindow(t *testing.T) {
	t.Parallel()
	const endedEpochStart = uint64(200)
	before, after, outputs, lateBlocks := applyLateBlockRewardRound(
		t, []uint64{endedEpochStart + lateBlockRewardWindow + 10},
	)
	require.Equal(t, 1, lateBlocks, "fixture must hold a late-window block")
	assert.Greater(t, after, before, "owner must be credited")
	assert.Equal(t, 1, outputs)
}

// TestRewardUpdateRequiresLateEpochBlock pins the reference rule (RUPD runs
// only once a block is ticked past epochStart + 4k/f): when no block was
// applied in the late window of the ended epoch, no reward update applies,
// whether the epoch had no blocks at all or only early ones.
func TestRewardUpdateRequiresLateEpochBlock(t *testing.T) {
	t.Parallel()
	t.Skip("known divergence from the reference ledger, see https://github.com/blinklabs-io/dingo/issues/6244")
	const endedEpochStart = uint64(200)
	tests := []struct {
		name       string
		blockSlots []uint64
	}{
		{name: "no blocks in the ended epoch"},
		{
			name:       "only blocks before the stability window",
			blockSlots: []uint64{endedEpochStart + 5, endedEpochStart + 30},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			before, after, outputs, lateBlocks := applyLateBlockRewardRound(
				t, tt.blockSlots,
			)
			require.Zero(t, lateBlocks, "fixture must hold no late-window block")
			assert.Equal(t, before, after,
				"no reward may be credited without a late-window block")
			assert.Zero(t, outputs,
				"no reward round may be persisted without a late-window block")
		})
	}
}
