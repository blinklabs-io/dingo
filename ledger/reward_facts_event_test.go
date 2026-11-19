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
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestRewardFactsEventFollowsPrecomputeCommit(t *testing.T) {
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
				ls, db := seedMultiPoolRewardPrecomputeFixture(t, 1, 1, 7)
				bus := event.NewEventBus(nil, nil)
				defer bus.Close()
				ls.config.EventBus = bus
				received := make(chan RewardFactsEvent, 1)
				bus.SubscribeFunc(RewardFactsEventType, func(ev event.Event) {
					received <- ev.Data.(RewardFactsEvent)
				})
				ldb := ls.ledgerDB()
				round, ok, err := ls.resolveStakeRewardPrecomputeRound(
					4,
					200,
					1200,
				)
				require.NoError(t, err)
				require.True(t, ok)
				txn := ldb.Begin(t.Context(), true)
				defer txn.Release()
				if chunked {
					done, err := ls.stakeRewardPrecomputeChunksInTxn(
						t.Context(),
						txn,
						round,
						0,
					)
					require.NoError(t, err)
					require.True(t, done)
				} else {
					app, ok, err := ls.precomputeStakeRewardsCalculate(txn, 4, 200, 1200)
					require.NoError(t, err)
					require.True(t, ok)
					require.NoError(t, ls.saveStakeRewardPrecompute(ldb, txn, app, 4, 200, 1200))
				}
				testutil.RequireNoReceive(
					t,
					received,
					20*time.Millisecond,
					"uncommitted facts",
				)
				if !commit {
					require.NoError(t, txn.Rollback())
					cursor, err := loadRewardPrecomputeCursor(ldb, nil, 1)
					require.NoError(t, err)
					require.Nil(t, cursor)
					testutil.RequireNoReceive(
						t,
						received,
						20*time.Millisecond,
						"rolled-back facts",
					)
					return
				}
				require.NoError(t, txn.Commit())
				ev := testutil.RequireReceive(
					t,
					received,
					5*time.Second,
					"completed reward facts",
				)
				require.Equal(
					t,
					RewardFactsEvent{SnapshotEpoch: 1, CapturedSlot: 200},
					ev,
				)
				cursor, err := loadRewardPrecomputeCursor(ldb, nil, 1)
				require.NoError(t, err)
				require.NotNil(t, cursor, "event readers can discover the round")
				require.True(t, cursor.Done)
				require.Equal(t, len(round.poolInputs), cursor.CompletedPools)
				require.Equal(t, round.inputFingerprint, cursor.InputFingerprint)
				require.Equal(t, uint64(200), cursor.CapturedSlot)
				rows, err := db.Metadata().GetRewardPoolOutputs(1, nil)
				require.NoError(t, err)
				require.Equal(t, rewardPoolOutputFingerprint(rows), cursor.OutputFingerprint)
				require.NotEmpty(
					t,
					rows,
					"event readers can see committed rewards",
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
	app, ok, err := ls.precomputeStakeRewardsCalculate(txn, 4, 1200, 1200)
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, app.params.SupportsPerPoolMemberRewards())
	require.NoError(t, ls.saveStakeRewardPrecompute(ldb, txn, app, 4, 1200, 1200))
	require.NoError(t, txn.Commit())
	cursor, err := loadRewardPrecomputeCursor(ldb, nil, 1)
	require.NoError(t, err)
	require.NotNil(t, cursor)
	require.True(t, cursor.Done)
	require.NotEmpty(t, cursor.InputFingerprint)
	require.Equal(t, 1, cursor.CompletedPools)
}
