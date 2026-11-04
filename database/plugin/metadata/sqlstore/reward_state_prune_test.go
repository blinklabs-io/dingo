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

package sqlstore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// seedRewardPruneFixture writes reward rows for epochs 0..5 whose flags
// exercise every branch of the prune predicate: credited rounds 1 and 3
// with unfolded credits (kept), folded credits (pruned), guarded and
// unspendable outputs (pruned), and outputs of rounds never credited
// (pruned). Every epoch also carries stake inputs.
func seedRewardPruneFixture(t *testing.T, store *Store) {
	t.Helper()
	require.NoError(t, store.SetPendingRewardCreditRounds(
		[]models.RewardCreditRound{
			{SnapshotEpoch: 1, BoundarySlot: 100},
			{SnapshotEpoch: 3, BoundarySlot: 300},
		}, nil,
	))
	var outputs []*models.RewardAccountOutput
	var inputs []*models.RewardStakeInput
	for epoch := range uint64(6) {
		for i := range 7 {
			key := bytesRepeat(0xd0, 28)
			key[26] = byte(epoch)
			key[27] = byte(i)
			outputs = append(outputs, &models.RewardAccountOutput{
				Epoch: epoch, CredentialTag: uint8(i % 2), StakingKey: key,
				PoolKeyHash: bytesRepeat(0xd1, 28), RewardType: "member",
				Amount:    types.Uint64(uint64(10*epoch) + uint64(i) + 1),
				Spendable: i%3 != 0, Guarded: i == 4,
				CapturedSlot: epoch * 100, BoundarySlot: epoch * 100,
			})
			inputs = append(inputs, &models.RewardStakeInput{
				Epoch: epoch, CredentialTag: uint8(i % 2), StakingKey: key,
				PoolKeyHash: bytesRepeat(0xd1, 28),
				Stake:       types.Uint64(uint64(i) + 1), Registered: true,
				CapturedSlot: epoch * 100, BoundarySlot: epoch * 100,
			})
		}
	}
	require.NoError(t, store.SaveRewardAccountOutputs(outputs, nil))
	require.NoError(t, store.SaveRewardStakeInputs(inputs, nil))
	_, err := store.writeDB.Exec(
		`UPDATE reward_account_output SET folded = TRUE WHERE credential_tag = 1 AND epoch IN (1, 3)`,
	)
	require.NoError(t, err)
}

// rewardRowsSnapshot lists every remaining reward row with the columns the
// prune predicate reads, in a fixed order, for comparing two stores.
func rewardRowsSnapshot(t *testing.T, store *Store) []string {
	t.Helper()
	var ret []string
	for _, query := range []string{
		`SELECT 'output', epoch, credential_tag, staking_key, amount,
        spendable, guarded, folded
 FROM reward_account_output
 ORDER BY epoch, credential_tag, staking_key, amount`,
		`SELECT 'input', epoch, credential_tag, staking_key, stake, 0, 0, 0
 FROM reward_stake_input
 ORDER BY epoch, credential_tag, staking_key, stake`,
	} {
		rows, err := store.readDB.QueryContext(context.Background(), query)
		require.NoError(t, err)
		for rows.Next() {
			var kind, amount string
			var epoch, tag int64
			var key []byte
			var spendable, guarded, folded bool
			require.NoError(t, rows.Scan(
				&kind, &epoch, &tag, &key, &amount,
				&spendable, &guarded, &folded,
			))
			ret = append(ret, fmt.Sprintf("%s %d %d %x %s %v %v %v",
				kind, epoch, tag, key, amount, spendable, guarded, folded))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	return ret
}

// TestDeleteRewardStateBeforeEpochBatchesMatchSingleDelete pins that the
// batched prune, which commits a bounded number of rows per transaction so
// block application can interleave, leaves exactly the rows the single
// statement inside a caller's transaction leaves.
func TestDeleteRewardStateBeforeEpochBatchesMatchSingleDelete(t *testing.T) {
	t.Parallel()
	single := newManagementTestStore(t)
	seedRewardPruneFixture(t, single)
	txn := single.Transaction(context.Background())
	require.NoError(
		t,
		single.DeleteRewardStateBeforeEpoch(context.Background(), 4, txn),
	)
	require.NoError(t, txn.Commit())
	want := rewardRowsSnapshot(t, single)
	require.NotEmpty(t, want)

	batched := newManagementTestStore(t)
	seedRewardPruneFixture(t, batched)
	before := rewardRowsSnapshot(t, batched)
	require.Greater(t, len(before), len(want), "the fixture has rows to prune")
	batched.rewardStateDeleteBatchSize = 3
	batches := 0
	batched.rewardStateDeleteBatchDone = func(rewardStatePruneBatch) error {
		batches++
		return nil
	}
	require.NoError(
		t,
		batched.DeleteRewardStateBeforeEpoch(context.Background(), 4, nil),
	)
	require.Equal(t, want, rewardRowsSnapshot(t, batched))
	require.Greater(t, batches, 2,
		"the prune must have crossed several batch boundaries")

	for _, row := range want {
		var epoch, tag int
		var kind string
		_, err := fmt.Sscanf(row, "%s %d %d", &kind, &epoch, &tag)
		require.NoError(t, err)
		if kind == "input" {
			require.GreaterOrEqual(
				t,
				epoch,
				4,
				"stake inputs keep only the window: %s",
				row,
			)
			continue
		}
		if epoch < 4 {
			require.Contains(
				t,
				[]int{1, 3},
				epoch,
				"only a credited round's rows survive below the window: %s",
				row,
			)
			require.Contains(t, row, " true false false",
				"only unfolded spendable unguarded credits survive: %s", row)
		}
	}
}

// TestDeleteRewardStateBeforeEpochResumesAfterInterruptedBatch pins the
// crash-safety argument behind batching: a prune that stops between two
// committed batches leaves nothing inconsistent, and the next prune of the
// same epoch finishes the job to the identical end state.
func TestDeleteRewardStateBeforeEpochResumesAfterInterruptedBatch(
	t *testing.T,
) {
	t.Parallel()
	single := newManagementTestStore(t)
	seedRewardPruneFixture(t, single)
	txn := single.Transaction(context.Background())
	require.NoError(
		t,
		single.DeleteRewardStateBeforeEpoch(context.Background(), 4, txn),
	)
	require.NoError(t, txn.Commit())
	want := rewardRowsSnapshot(t, single)

	store := newManagementTestStore(t)
	seedRewardPruneFixture(t, store)
	initial := rewardRowsSnapshot(t, store)
	store.rewardStateDeleteBatchSize = 4
	stop := errors.New("interrupted between batches")
	store.rewardStateDeleteBatchDone = func(rewardStatePruneBatch) error {
		return stop
	}
	err := store.DeleteRewardStateBeforeEpoch(context.Background(), 4, nil)
	require.ErrorIs(t, err, stop)
	partial := rewardRowsSnapshot(t, store)
	require.Less(t, len(partial), len(initial), "the first batch committed")
	require.Greater(t, len(partial), len(want), "the prune was cut short")
	for _, row := range want {
		require.Contains(t, partial, row,
			"an interrupted prune never removes a row the full prune keeps")
	}

	store.rewardStateDeleteBatchDone = nil
	require.NoError(
		t,
		store.DeleteRewardStateBeforeEpoch(context.Background(), 4, nil),
	)
	require.Equal(t, want, rewardRowsSnapshot(t, store))
	require.NoError(
		t,
		store.DeleteRewardStateBeforeEpoch(context.Background(), 4, nil),
		"a prune with nothing left to do succeeds",
	)
	require.Equal(t, want, rewardRowsSnapshot(t, store))
}

// TestRewardStatePruneBatchesAreSizedByHoldTime pins the adaptive sizing:
// the batch grows while a transaction holds the write connection for less
// than the target, shrinks in proportion when it holds it for longer, moves
// by at most a factor of two per step within the bounds, and the size a
// prune settles on is where the next prune of the same table starts.
func TestRewardStatePruneBatchesAreSizedByHoldTime(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	const initial = 4
	store.rewardStatePruneInitialRows = initial
	// A scripted clock: the n-th committed batch is reported as having held
	// the connection for holds[n].
	holds := []time.Duration{
		100 * time.Millisecond,  // 4 rows, fast: grow x2 -> 8
		100 * time.Millisecond,  // 8 rows, fast: grow x2 -> 16
		3 * time.Second,         // 16 rows, slow: 16*0.75/3 = 4, floored at half -> 8
		750 * time.Millisecond,  // 8 rows, on target -> 8
		1500 * time.Millisecond, // 8 rows, twice the target -> 4
		10 * time.Second,        // 4 rows, far too slow: halving floor -> 2
		0,                       // 2 rows, instant: grow x2 -> 4
		0,                       // 4 rows, instant -> 8
		0,                       // 8 requested, 6 left: short batch ends it, stays 8
		0,                       // next prune: 8 requested, none left, stays 8
	}
	calls := 0
	now := time.Unix(1_700_000_000, 0)
	store.rewardStatePruneNow = func() time.Time {
		calls++
		if calls%2 == 0 {
			now = now.Add(holds[calls/2-1])
		}
		return now
	}
	var inputs []*models.RewardStakeInput
	for i := range 60 {
		key := bytesRepeat(0xe0, 28)
		key[27] = byte(i)
		inputs = append(inputs, &models.RewardStakeInput{
			Epoch: 1, CredentialTag: 0, StakingKey: key,
			PoolKeyHash: bytesRepeat(0xe1, 28),
			Stake:       types.Uint64(uint64(i) + 1), Registered: true,
			CapturedSlot: 100, BoundarySlot: 100,
		})
	}
	require.NoError(t, store.SaveRewardStakeInputs(inputs, nil))
	var requested []int
	store.rewardStateDeleteBatchDone = func(b rewardStatePruneBatch) error {
		require.Equal(t, "reward_stake_input", b.table)
		requested = append(requested, b.requested)
		return nil
	}
	require.NoError(
		t,
		store.DeleteRewardStakeInputBeforeEpoch(context.Background(), 2, nil),
	)
	require.Equal(t, []int{4, 8, 16, 8, 8, 4, 2, 4, 8}, requested,
		"4+8+16+8+8+4+2+4 = 54 rows, then a short batch of 6 ends the prune")
	require.Equal(t, 8, store.rewardStatePruneBatchRows("reward_stake_input"),
		"the next prune starts from the size the last full batch settled on, "+
			"not one scaled from the short final batch")
	require.Equal(
		t,
		initial,
		store.rewardStatePruneBatchRows("reward_account_output"),
		"each table learns its own size",
	)

	requested = nil
	require.NoError(t, store.DeleteRewardStakeInputBeforeEpoch(
		context.Background(), 2, nil,
	))
	require.Equal(t, []int{8}, requested)
	require.Equal(t, 8, store.rewardStatePruneBatchRows("reward_stake_input"),
		"an empty batch does not resize either")

	var remaining int
	require.NoError(t, store.readDB.QueryRow(
		`SELECT count(*) FROM reward_stake_input`).Scan(&remaining))
	require.Zero(t, remaining)
}

// TestNextRewardStatePruneBatchRowsBounds pins the sizing bounds as
// literal row counts: growth caps at twenty times the initial size and
// shrinking floors at a fifth of it, but never below one row.
func TestNextRewardStatePruneBatchRowsBounds(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	store.rewardStatePruneInitialRows = 50
	require.Equal(t, 1000, store.nextRewardStatePruneBatchRows(800, 0),
		"growth stops at twenty times the initial size")
	require.Equal(t, 1000, store.nextRewardStatePruneBatchRows(1000, 0))
	require.Equal(t, 10, store.nextRewardStatePruneBatchRows(12, time.Hour),
		"shrinking stops at a fifth of the initial size")
	store.rewardStatePruneInitialRows = 4
	require.Equal(t, 1, store.nextRewardStatePruneBatchRows(1, time.Hour),
		"a fifth of a tiny initial size rounds up to one row")
}

// TestDeleteRewardStateBeforeEpochStopsWhenContextCancelled pins that the
// batched prune honours its caller's context between batches: a cancelled
// prune returns the cancellation and leaves the rest for the next boundary.
func TestDeleteRewardStateBeforeEpochStopsWhenContextCancelled(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	seedRewardPruneFixture(t, store)
	initial := rewardRowsSnapshot(t, store)
	store.rewardStateDeleteBatchSize = 3
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batches := 0
	store.rewardStateDeleteBatchDone = func(rewardStatePruneBatch) error {
		batches++
		cancel()
		return nil
	}
	err := store.DeleteRewardStateBeforeEpoch(ctx, 4, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, batches, "no batch starts after cancellation")
	require.Len(t, rewardRowsSnapshot(t, store), len(initial)-3,
		"only the batch committed before cancellation was removed")
}
