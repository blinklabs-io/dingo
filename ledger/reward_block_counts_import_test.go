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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/ledger/epochengine"
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

	require.Len(t, observed.PoolOutputs, 1)
	require.Len(t, imported.PoolOutputs, len(observed.PoolOutputs))
	for i, want := range observed.PoolOutputs {
		got := imported.PoolOutputs[i]
		assert.Equal(t, want.PoolKeyHash, got.PoolKeyHash)
		assert.Equal(t, want.TotalReward, got.TotalReward)
		assert.Equal(t, want.LeaderReward, got.LeaderReward)
		assert.Equal(
			t,
			want.ApparentPerformance.String(),
			got.ApparentPerformance.String(),
		)
	}
	require.NotEmpty(t, observed.AccountOutputs)
	require.Len(t, imported.AccountOutputs, len(observed.AccountOutputs))
	for i, want := range observed.AccountOutputs {
		got := imported.AccountOutputs[i]
		assert.Equal(t, want.StakingKey, got.StakingKey)
		assert.Equal(t, want.RewardType, got.RewardType)
		assert.Equal(t, want.Amount, got.Amount)
	}
	assert.Positive(t, uint64(observed.PoolOutputs[0].TotalReward))
	assert.Equal(t, observed.EffectiveRewards, imported.EffectiveRewards)
}

// stakeRewardApplicationForTest computes the epoch 4 reward round twice over
// the same state: once from the ten blocks the node itself applied in epoch 2,
// and once with those blocks hidden behind a trust anchor and supplied instead
// as the snapshot's imported counts for the same epoch.
func stakeRewardApplicationForTest(
	t *testing.T,
	fromImport bool,
) *epochengine.StakeRewardApplication {
	t.Helper()
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	if fromImport {
		require.NoError(t, db.SetChainState(
			mithrilLedgerSlotSyncKey,
			"199",
			nil,
		))
		require.NoError(t, fxImporter(t, db).ReplaceImportedBlockCounts(
			2,
			map[string]uint64{string(rewardCalcHash(0x4a)): 10},
			10,
			199,
			nil,
		))
	}
	txn := db.Transaction(context.Background(), false)
	t.Cleanup(func() { _ = txn.Rollback() })
	app, ok, err := ls.engine().CalculateStakeRewardApplication(
		context.Background(),
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
