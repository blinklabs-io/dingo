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

	"github.com/stretchr/testify/require"
)

// A Dijkstra direct deposit is applied after its transaction's withdrawals, so
// an event credit can follow a withdrawal in one slot. Rollback and the
// historical reward read must leave such a credit out of the balance the
// withdrawal recorded, while a credit applied before the withdrawal in the same
// slot still counts toward it.
func TestSameSlotCreditOrderAroundWithdrawal(t *testing.T) {
	t.Parallel()
	exerciseSameSlotCreditOrder(t, newManagementTestStore)
}

func exerciseSameSlotCreditOrder(
	t *testing.T,
	newStore func(*testing.T) *Store,
) {
	t.Helper()
	for _, tc := range []struct {
		name   string
		start  uint64
		before uint64
		after  uint64
	}{
		{"credit after withdrawal", 100, 0, 30},
		{"credit after withdrawal exceeds the withdrawn balance", 100, 0, 150},
		{"credits on both sides of the withdrawal", 4, 20, 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newStore(t)
			key := bytesRepeat(0x67, 28)
			rollbackAccount(t, store, key, tc.start)
			if tc.before > 0 {
				require.NoError(t, store.AddAccountRewardByCredential(
					0, key, tc.before, 200, bytesRepeat(0x41, 32), nil))
			}
			withdrawal := bytesRepeat(0x42, 32)
			seedWithdrawingTransaction(t, store, withdrawal)
			require.NoError(t, store.ApplyAccountRewardWithdrawal(
				0, key, tc.start+tc.before, 200, withdrawal, nil))
			// The deposit's source is the withdrawing transaction itself.
			require.NoError(t, store.AddAccountRewardByCredential(
				0, key, tc.after, 200, withdrawal, nil))
			require.Equal(t, tc.after, rewardOf(t, store, key))

			ref := historicalRewardKey{tag: 0, key: string(key)}
			db, ctx, err := store.readDBFromTxn(nil)
			require.NoError(t, err)
			historical, err := store.historicalRewards(
				ctx, db, 199, map[historicalRewardKey]struct{}{ref: {}},
			)
			require.NoError(t, err)
			require.Equal(t, tc.start, historical[ref])

			require.NoError(t, store.DeleteAccountRewardsAfterSlot(199, nil))
			require.Equal(t, tc.start, rewardOf(t, store, key))
		})
	}
}

// A Leios closure transaction executes at its context slot but is journaled at
// the certifying block's later slot, so its direct deposit follows every
// withdrawal journaled between the two. The historical read must order the
// deposit by its journal slot, as rollback does, and leave it out of the
// balance such a withdrawal recorded.
func TestClosureCreditFollowsWithdrawalAtItsContextSlot(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := bytesRepeat(0x68, 28)
	rollbackAccount(t, store, key, 100)
	withdrawal := bytesRepeat(0x43, 32)
	seedWithdrawingTransaction(t, store, withdrawal)
	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0, key, 100, 200, withdrawal, nil))
	closure := bytesRepeat(0x44, 32)
	seedWithdrawingTransaction(t, store, closure)
	_, err := store.writeDB.Exec(`INSERT INTO leios_transaction_context
(transaction_id, slot) SELECT id, 200 FROM "transaction" WHERE hash = ?`,
		closure)
	require.NoError(t, err)
	require.NoError(t, store.AddAccountRewardByCredential(
		0, key, 30, 400, closure, nil))

	ref := historicalRewardKey{tag: 0, key: string(key)}
	db, ctx, err := store.readDBFromTxn(nil)
	require.NoError(t, err)
	historical, err := store.historicalRewards(
		ctx, db, 199, map[historicalRewardKey]struct{}{ref: {}},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(100), historical[ref])

	require.NoError(t, store.DeleteAccountRewardsAfterSlot(199, nil))
	require.Equal(t, uint64(100), rewardOf(t, store, key))
}
