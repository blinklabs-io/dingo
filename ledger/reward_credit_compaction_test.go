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
	"math"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// agedCreditedRound returns the dump fixture's credited round with newer
// rounds recorded after it, so compaction treats it as due.
func agedCreditedRound(t *testing.T) *epochBoundaryBenchFixture {
	t.Helper()
	f := creditedRewardRound(t)
	ageCreditedRound(t, f)
	return f
}

// ageCreditedRound records rewardCreditRoundsKeptUnfolded rounds after the
// one f's boundary credited.
func ageCreditedRound(t *testing.T, f *epochBoundaryBenchFixture) {
	t.Helper()
	boundary := epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch + 1)
	for i := range uint64(rewardCreditRoundsKeptUnfolded) {
		require.NoError(t, seedDB(t, f.db).AddAppliedRewardCreditRound(
			models.RewardCreditRound{
				SnapshotEpoch: 9 + i,
				BoundarySlot:  boundary + (i+1)*epochBoundaryBenchEpochLength,
			}, nil,
		))
	}
}

func unfoldedCreditCount(t *testing.T, f *epochBoundaryBenchFixture) int {
	t.Helper()
	// The set keeps an output only while it is spendable, unguarded and
	// unfolded.
	r := f.ls.rewardOutputReader(nil)
	require.NotNil(t, r, "the fixture's set must keep reward outputs")
	epochs, err := r.RewardOutputEpochs(nil)
	require.NoError(t, err)
	n := 0
	for _, epoch := range epochs {
		outputs, err := r.EpochRewardOutputs(epoch, math.MaxInt32, nil)
		require.NoError(t, err)
		n += len(outputs)
	}
	return n
}

// A fully folded aged round is not due even though its round marker remains
// for rollback.
func TestRewardCreditCompactionWithFoldedRoundIsNotDue(t *testing.T) {
	t.Parallel()
	f := agedCreditedRound(t)
	require.NoError(t, f.ls.compactRewardCreditRounds(context.Background()))
	require.Zero(t, unfoldedCreditCount(t, f))
	due, err := f.ls.rewardCreditRoundDue(context.Background())
	require.NoError(t, err)
	require.False(t, due)
}

// The probe is bounded to rounds older than the newest two, which in steady
// state hold unfolded credits; an unbounded probe would report due at every
// start and credited round.
func TestRewardCreditCompactionIgnoresUnfoldedKeptRounds(t *testing.T) {
	t.Parallel()
	f := agedCreditedRound(t)
	require.NoError(t, f.ls.compactRewardCreditRounds(context.Background()))
	require.Zero(t, unfoldedCreditCount(t, f))
	for i := range uint64(rewardCreditRoundsKeptUnfolded) {
		require.NoError(t, seedDB(t, f.db).SaveRewardAccountOutputs(
			[]*models.RewardAccountOutput{{
				StakingKey:  bytes.Repeat([]byte{byte(0xa0 + i)}, 28),
				PoolKeyHash: bytes.Repeat([]byte{0xb0}, 28),
				RewardType:  "member",
				Epoch:       9 + i,
				Amount:      1_000_000,
				Spendable:   true,
			}}, nil,
		))
	}
	require.Equal(t, rewardCreditRoundsKeptUnfolded, unfoldedCreditCount(t, f))

	due, err := f.ls.rewardCreditRoundDue(context.Background())
	require.NoError(t, err)
	require.False(t, due, "credits in the newest two rounds are kept unfolded")
}

// TestRewardCreditCompactionFoldsDueRoundsExactly pins compaction: a due
// round is folded into account rows in bounded, resumable steps, every reader
// reads the same balances before, during and after, and the account rows and
// journal end exactly as folding every credit at once leaves them.
func TestRewardCreditCompactionFoldsDueRoundsExactly(t *testing.T) {
	t.Parallel()
	f := agedCreditedRound(t)
	credits := rewardedCredentials(t, f)
	keys := make([]string, 0, len(credits))
	for key := range credits {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	txn := f.db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		for _, key := range keys[:len(keys)/5] {
			if err := f.ls.foldRewardCreditFor(context.Background(), txn, 0, []byte(key)); err != nil {
				return err
			}
		}
		return nil
	}))
	before := observePendingRoundReaders(t, f, credits)
	unfolded := unfoldedCreditCount(t, f)
	require.Positive(t, unfolded)

	done, err := f.ls.compactRewardCreditChunk(context.Background(), 3)
	require.NoError(t, err)
	require.False(t, done)
	require.Equal(t, unfolded-3, unfoldedCreditCount(t, f),
		"one step folds one bounded chunk")
	require.Equal(t, before, observePendingRoundReaders(t, f, credits),
		"a partly compacted round reads the same balances")

	require.NoError(t, f.ls.compactRewardCreditRounds(context.Background()))
	require.Zero(t, unfoldedCreditCount(t, f))
	require.Equal(t, before, observePendingRoundReaders(t, f, credits),
		"a compacted round reads the same balances")

	want := creditedRewardRound(t)
	settleRewardCredits(t, want.ls)
	require.Zero(t, unfoldedCreditCount(t, want))
	require.Equal(t, accountRewards(t, want, credits),
		accountRewards(t, f, credits),
		"compaction writes what folding every credit writes")
}

// TestRewardCreditCompactionLeavesNewestRounds pins the age rule: the newest
// rewardCreditRoundsKeptUnfolded rounds stay unfolded.
func TestRewardCreditCompactionLeavesNewestRounds(t *testing.T) {
	t.Parallel()
	f := creditedRewardRound(t)
	unfolded := unfoldedCreditCount(t, f)
	require.NoError(t, f.ls.compactRewardCreditRounds(context.Background()))
	require.Equal(t, unfolded, unfoldedCreditCount(t, f))
}

// TestRewardCreditCompactionRollbackBelowBoundary pins rollback over a
// compacted round: the folded credits' journal rows and balances revert and
// the round's outputs are unfolded credits again.
func TestRewardCreditCompactionRollbackBelowBoundary(t *testing.T) {
	t.Parallel()
	f, lastOfEpoch := journaledRewardRound(t)
	ageCreditedRound(t, f)
	credits := rewardedCredentials(t, f)
	stored := accountRewards(t, f, credits)
	unfolded := unfoldedCreditCount(t, f)
	require.Positive(t, unfolded)
	require.NoError(t, f.ls.compactRewardCreditRounds(context.Background()))
	require.Zero(t, unfoldedCreditCount(t, f))
	fxRollback(t, f.ls, lastOfEpoch)
	require.Equal(t, stored, accountRewards(t, f, credits))
	require.Equal(t, unfolded, unfoldedCreditCount(t, f),
		"the round's outputs are unfolded credits again")
}
