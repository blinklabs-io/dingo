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
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The imported counts are not an approximation: for the same epoch they
// reproduce the distribution the node would have computed from its own block
// history, pool output for pool output and account output for account output.
func TestStakeRewardRoundFromImportedBlockCountsMatchesObservedHistory(
	t *testing.T,
) {
	t.Parallel()

	observed := stakeRewardApplicationForTest(t, false)
	imported := stakeRewardApplicationForTest(t, true)

	require.Len(t, observed.poolOutputs, 1)
	require.Len(t, imported.poolOutputs, len(observed.poolOutputs))
	for i, want := range observed.poolOutputs {
		got := imported.poolOutputs[i]
		assert.Equal(t, want.PoolKeyHash, got.PoolKeyHash)
		assert.Equal(t, want.TotalReward, got.TotalReward)
		assert.Equal(t, want.LeaderReward, got.LeaderReward)
		assert.Equal(
			t,
			want.ApparentPerformance.String(),
			got.ApparentPerformance.String(),
		)
	}
	require.NotEmpty(t, observed.accountOutputs)
	require.Len(t, imported.accountOutputs, len(observed.accountOutputs))
	for i, want := range observed.accountOutputs {
		got := imported.accountOutputs[i]
		assert.Equal(t, want.StakingKey, got.StakingKey)
		assert.Equal(t, want.RewardType, got.RewardType)
		assert.Equal(t, want.Amount, got.Amount)
	}
	assert.Positive(t, uint64(observed.poolOutputs[0].TotalReward))
	assert.Equal(t, observed.effectiveRewards, imported.effectiveRewards)
}

// stakeRewardApplicationForTest computes the epoch 4 reward round twice over
// the same state: once from the ten blocks the node itself applied in epoch 2,
// and once with those blocks hidden behind a trust anchor and supplied instead
// as the snapshot's imported counts for the same epoch.
func stakeRewardApplicationForTest(
	t *testing.T,
	fromImport bool,
) *stakeRewardApplication {
	t.Helper()
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()
	if fromImport {
		require.NoError(t, meta.SetSyncState(
			mithrilLedgerSlotSyncKey,
			"199",
			nil,
		))
		require.NoError(t, meta.SaveImportedPoolBlockCounts(
			[]models.ImportedPoolBlockCount{
				{
					Epoch:          2,
					PoolKeyHash:    rewardCalcHash(0x4a),
					BlocksProduced: 10,
					CapturedSlot:   199,
				},
			},
			nil,
		))
		require.NoError(t, meta.SaveImportedEpochBlockTotal(2, 10, 199, nil))
	}
	txn := db.Transaction(context.Background(), false)
	t.Cleanup(func() { _ = txn.Rollback() })
	app, ok, err := ls.calculateStakeRewardApplication(
		txn,
		4,
		1_200,
		1_200,
		true,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, app)
	return app
}
