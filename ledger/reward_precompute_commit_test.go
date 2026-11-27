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

	"github.com/blinklabs-io/dingo/ledger/epochengine"
	"github.com/stretchr/testify/require"
)

func TestRewardPrecomputeCursorFollowsCommit(t *testing.T) {
	t.Parallel()
	for _, chunked := range []bool{false, true} {
		for _, commit := range []bool{false, true} {
			name := "single"
			if chunked {
				name = "chunked"
			}
			if commit {
				name += "/commit"
			} else {
				name += "/rollback"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				ls, _ := seedMultiPoolRewardPrecomputeFixture(t, 1, 1, 7)
				ldb := ls.ledgerDB()
				round, ok, err := ls.engine().ResolveStakeRewardPrecomputeRound(
					4,
					200,
					1200,
				)
				require.NoError(t, err)
				require.True(t, ok)
				txn := ldb.Begin(t.Context(), true)
				defer txn.Release()
				if chunked {
					done, err := ls.engine().StakeRewardPrecomputeChunksInTxn(
						t.Context(),
						txn,
						round,
						0,
					)
					require.NoError(t, err)
					require.True(t, done)
				} else {
					app, ok, err := ls.engine().PrecomputeStakeRewardsCalculate(txn, 4, 200, 1200)
					require.NoError(t, err)
					require.True(t, ok)
					require.NoError(t, ls.engine().SaveStakeRewardPrecompute(ldb, txn, app, 4, 200, 1200))
				}
				if !commit {
					require.NoError(t, txn.Rollback())
					cursor, err := epochengine.LoadRewardPrecomputeCursor(ldb, nil, 1)
					require.NoError(t, err)
					require.Nil(t, cursor)
					return
				}
				require.NoError(t, txn.Commit())
				cursor, err := epochengine.LoadRewardPrecomputeCursor(ldb, nil, 1)
				require.NoError(t, err)
				require.NotNil(t, cursor, "a committed round has a cursor")
				require.True(t, cursor.Done)
				require.Equal(t, len(round.PoolInputs), cursor.CompletedPools)
				require.Equal(t, round.InputFingerprint, cursor.InputFingerprint)
				require.Equal(t, uint64(200), cursor.CapturedSlot)
				rows, err := ldb.GetRewardPoolOutputs(1, nil)
				require.NoError(t, err)
				require.Equal(t, epochengine.RewardPoolOutputFingerprint(rows), cursor.OutputFingerprint)
				require.NotEmpty(
					t,
					rows,
					"a committed round has reward outputs",
				)
			})
		}
	}
}

func TestSinglePassShelleyRewardCompletion(t *testing.T) {
	t.Parallel()
	ls, _ := seedMultiPoolRewardPrecomputeFixture(t, 1, 1, 2)
	ldb := ls.ledgerDB()
	txn := ldb.Begin(t.Context(), true)
	defer txn.Release()
	app, ok, err := ls.engine().PrecomputeStakeRewardsCalculate(txn, 4, 1200, 1200)
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, app.Params.SupportsPerPoolMemberRewards())
	require.NoError(t, ls.engine().SaveStakeRewardPrecompute(ldb, txn, app, 4, 1200, 1200))
	require.NoError(t, txn.Commit())
	cursor, err := epochengine.LoadRewardPrecomputeCursor(ldb, nil, 1)
	require.NoError(t, err)
	require.NotNil(t, cursor)
	require.True(t, cursor.Done)
	require.NotEmpty(t, cursor.InputFingerprint)
	require.Equal(t, 1, cursor.CompletedPools)
}
