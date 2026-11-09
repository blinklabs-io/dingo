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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

func rollbackAccount(t *testing.T, store *Store, key []byte, reward uint64) {
	t.Helper()
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: key, Active: true, Reward: types.Uint64(reward),
	}))
}

// seedWithdrawingTransaction stores the transaction row a withdrawal by hash
// is applied against.
func seedWithdrawingTransaction(t *testing.T, store *Store, hash []byte) {
	t.Helper()
	require.NoError(t, store.SetGenesisTransaction(hash, hash, nil, nil))
}

func rewardOf(t *testing.T, store *Store, key []byte) uint64 {
	t.Helper()
	account, err := store.GetAccountByCredential(0, key, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	return uint64(account.Reward)
}

// TestRollbackRestoresBalanceAroundWithdrawals walks one account through a
// credit, a withdrawal and a later credit and rolls back to every point
// between them: a rollback before the withdrawal returns the balance the
// withdrawal recorded, one after it undoes only the later credit.
func TestRollbackRestoresBalanceAroundWithdrawals(t *testing.T) {
	t.Parallel()
	// ApplyAccountRewardWithdrawal clears the whole balance, so the
	// withdrawal at 200 leaves 0 and the credit at 250 leaves 10.
	for _, tc := range []struct {
		name     string
		rollback uint64
		want     uint64
	}{
		{"before the first credit", 50, 7},
		{"between credit and withdrawal", 150, 57},
		{"at the withdrawal's slot", 200, 0},
		{"between withdrawal and later credit", 220, 0},
		{"at the later credit", 250, 10},
		{"above everything", 300, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newManagementTestStore(t)
			key := bytesRepeat(0x61, 28)
			rollbackAccount(t, store, key, 7)
			require.NoError(t, store.AddAccountRewardByCredential(
				0, key, 50, 100, bytesRepeat(0x01, 32), nil))
			require.Equal(t, uint64(57), rewardOf(t, store, key))
			seedWithdrawingTransaction(t, store, bytesRepeat(0x02, 32))
			require.NoError(t, store.ApplyAccountRewardWithdrawal(
				0, key, 30, 200, bytesRepeat(0x02, 32), nil))
			require.Equal(t, uint64(0), rewardOf(t, store, key))
			require.NoError(t, store.AddAccountRewardByCredential(
				0, key, 10, 250, bytesRepeat(0x03, 32), nil))
			require.NoError(
				t,
				store.DeleteAccountRewardsAfterSlot(tc.rollback, nil),
			)
			require.Equal(t, tc.want, rewardOf(t, store, key))
		})
	}
}

// A credit and the withdrawal that spends it in one slot roll back together:
// the credit at the withdrawal's slot counts as before it.
func TestRollbackCreditAtWithdrawalSlot(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := bytesRepeat(0x62, 28)
	rollbackAccount(t, store, key, 4)
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 20, 200, bytesRepeat(0x11, 32), nil))
	seedWithdrawingTransaction(t, store, bytesRepeat(0x12, 32))
	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0, key, 24, 200, bytesRepeat(0x12, 32), nil))
	require.Equal(t, uint64(0), rewardOf(t, store, key))
	require.NoError(t, store.DeleteAccountRewardsAfterSlot(199, nil))
	require.Equal(t, uint64(4), rewardOf(t, store, key))
}

// Two stake-reward rounds, one partly folded: a rollback between them removes
// the later round's journal rows and leaves the earlier round's, and a rollback
// below both returns the balance from before the journaled credits.
func TestRollbackAcrossStakeRewardRounds(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := bytesRepeat(0x63, 28)
	other := bytesRepeat(0x64, 28)
	rollbackAccount(t, store, key, 10)
	rollbackAccount(t, store, other, 10)
	credit := func(slot uint64, output uint, who []byte, amount uint64) {
		t.Helper()
		require.NoError(t, store.AddAccountRewardsByCredential(
			[]models.AccountRewardCredit{{
				StakingKey: who, Amount: amount, Slot: slot, OutputID: output,
			}}, nil))
	}
	credit(300, 1, key, 100)
	credit(300, 2, key, 5)
	credit(300, 3, other, 40)
	credit(400, 4, key, 200)
	require.Equal(t, uint64(315), rewardOf(t, store, key))

	require.NoError(t, store.DeleteAccountRewardsAfterSlot(350, nil))
	require.Equal(t, uint64(115), rewardOf(t, store, key))
	require.Equal(t, uint64(50), rewardOf(t, store, other))
	var slots int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM reward_credit_slot`).Scan(&slots))
	require.Equal(t, 1, slots)

	require.NoError(t, store.DeleteAccountRewardsAfterSlot(299, nil))
	require.Equal(t, uint64(10), rewardOf(t, store, key))
	require.Equal(t, uint64(10), rewardOf(t, store, other))
	var credits int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM reward_credit`).Scan(&credits))
	require.Zero(t, credits)

	// Replaying a rolled-back credit applies it again: its row is gone.
	credit(300, 1, key, 100)
	require.Equal(t, uint64(110), rewardOf(t, store, key))
}

// A rollback that would take a balance below zero fails the whole rollback
// and leaves the journal as it was.
func TestRollbackUnderflowFailsWithoutChangingRows(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := bytesRepeat(0x65, 28)
	rollbackAccount(t, store, key, 0)
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 9, 100, bytesRepeat(0x21, 32), nil))
	_, err := store.writeDB.Exec(
		`UPDATE account SET reward = '4' WHERE staking_key = ?`,
		key,
	)
	require.NoError(t, err)
	require.ErrorContains(
		t,
		store.DeleteAccountRewardsAfterSlot(50, nil),
		"rollback underflow",
	)
	require.Equal(t, uint64(4), rewardOf(t, store, key))
	var events int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM reward_event_credit`).Scan(&events))
	require.Equal(t, 1, events)
}

// A round credited to some accounts and still pending for others rolls back
// to the balances from before the round: journaled credits are subtracted, and
// DeleteRewardStateAfterSlot drops the round so the pending ones go with it.
func TestRollbackOfPartlyFoldedRound(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	require.NoError(t, store.SetPendingRewardCreditRounds(
		[]models.RewardCreditRound{{SnapshotEpoch: 3, BoundarySlot: 500}}, nil))
	keys := [][]byte{
		bytesRepeat(0x71, 28),
		bytesRepeat(0x72, 28),
		bytesRepeat(0x73, 28),
	}
	var outputs []*models.RewardAccountOutput
	for _, key := range keys {
		rollbackAccount(t, store, key, 100)
		outputs = append(outputs, &models.RewardAccountOutput{
			Epoch: 3, StakingKey: key, PoolKeyHash: bytesRepeat(0x31, 28),
			RewardType: "member", Amount: 25, Spendable: true,
			CapturedSlot: 499, BoundarySlot: 500,
		})
	}
	require.NoError(t, store.SaveRewardAccountOutputs(outputs, nil))
	claimed, err := store.ClaimUnfoldedRewardCredits(3, 0, 2, nil)
	require.NoError(t, err)
	require.Len(t, claimed, 2)
	var credits []models.AccountRewardCredit
	for _, output := range claimed {
		credits = append(credits, models.AccountRewardCredit{
			StakingKey: output.StakingKey, Amount: uint64(output.Amount),
			Slot: output.BoundarySlot, OutputID: output.ID, AccountID: output.AccountID,
		})
	}
	require.NoError(t, store.AddAccountRewardsByCredential(credits, nil))
	require.Equal(t, uint64(125), rewardOf(t, store, keys[0]))
	require.Equal(t, uint64(125), rewardOf(t, store, keys[1]))
	require.Equal(t, uint64(100), rewardOf(t, store, keys[2]), "still pending")

	require.NoError(t, store.DeleteAccountRewardsAfterSlot(499, nil))
	require.NoError(t, store.DeleteRewardStateAfterSlot(499, nil))
	for _, key := range keys {
		require.Equal(t, uint64(100), rewardOf(t, store, key))
	}
	has, err := store.HasPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	require.False(
		t,
		has,
		"the round is gone, so its surviving outputs are not pending credits",
	)
	// Replaying the round credits every output once.
	require.NoError(t, store.AddAppliedRewardCreditRound(
		models.RewardCreditRound{SnapshotEpoch: 3, BoundarySlot: 500}, nil))
	claimed, err = store.ClaimUnfoldedRewardCredits(3, 0, 10, nil)
	require.NoError(t, err)
	require.Len(
		t,
		claimed,
		3,
		"outputs captured before the rollback slot survive and are claimable again",
	)
}

// With several withdrawals above the rollback slot, the earliest one anchors
// the balance: its recorded balance already includes everything before it.
func TestRollbackAnchorsOnEarliestWithdrawal(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := bytesRepeat(0x66, 28)
	rollbackAccount(t, store, key, 2)
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 50, 100, bytesRepeat(0x31, 32), nil))
	seedWithdrawingTransaction(t, store, bytesRepeat(0x32, 32))
	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0, key, 52, 200, bytesRepeat(0x32, 32), nil))
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 30, 220, bytesRepeat(0x33, 32), nil))
	seedWithdrawingTransaction(t, store, bytesRepeat(0x34, 32))
	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0, key, 30, 300, bytesRepeat(0x34, 32), nil))
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 8, 350, bytesRepeat(0x35, 32), nil))
	require.NoError(t, store.DeleteAccountRewardsAfterSlot(150, nil))
	require.Equal(t, uint64(52), rewardOf(t, store, key))
}
