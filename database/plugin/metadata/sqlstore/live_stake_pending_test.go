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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

type pendingLiveStake struct {
	utxo   uint64
	reward uint64
	total  uint64
}

// TestReSumCountsPendingBatchedStakeOnce queues a batched payment to
// credential C and then, before the accumulator flushes, runs each write
// that re-sums C's live stake from the utxo table: a reward withdrawal (with
// the accumulator bound to an outer transaction, and with none), a direct
// reward credit, and a batch of reward credits. The re-sum already sees the
// payment's utxo row, so after the flush reward_live_stake must count it
// once.
func TestReSumCountsPendingBatchedStakeOnce(t *testing.T) {
	t.Parallel()
	const (
		startReward = uint64(5_000_000)
		payment     = uint64(2_000_000)
		amount      = uint64(1_000_000)
	)
	stakingKey := lcommon.NewBlake2b224([]byte("pending-stake-key"))
	paymentKey := lcommon.NewBlake2b224([]byte("pending-payment-key"))
	credential := models.NewStakeCredentialRef(0, stakingKey.Bytes())
	baseAddress, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		paymentKey.Bytes(),
		stakingKey.Bytes(),
	)
	require.NoError(t, err)
	rewardAddress, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakingKey.Bytes(),
	)
	require.NoError(t, err)

	pay := func(
		t *testing.T,
		store *Store,
		acc types.MetadataBatchAccumulator,
		txn types.Txn,
	) {
		t.Helper()
		output, err := mockledger.NewTransactionOutputBuilder().
			WithAddress(baseAddress.String()).
			WithLovelace(payment).
			Build()
		require.NoError(t, err)
		txID := make([]byte, 32)
		txID[0] = 1
		tx := mockledger.NewTransactionBuilder()
		tx.WithId(txID)
		tx.WithOutputs(output)
		tx.WithValid(true)
		require.NoError(t, store.SetTransactionBatched(
			tx, ocommon.Point{Slot: 100, Hash: txID}, 0, nil, true, acc, txn, 0,
		))
	}
	withdraw := func(
		t *testing.T,
		store *Store,
		acc types.MetadataBatchAccumulator,
		txn types.Txn,
	) {
		t.Helper()
		txID := make([]byte, 32)
		txID[0] = 2
		tx := mockledger.NewTransactionBuilder()
		tx.WithId(txID)
		tx.WithValid(true)
		tx.WithWithdrawals(map[*lcommon.Address]uint64{&rewardAddress: amount})
		require.NoError(t, store.SetTransactionBatched(
			tx, ocommon.Point{Slot: 100, Hash: txID}, 1, nil, true, acc, txn, 0,
		))
	}

	for _, tc := range []struct {
		name     string
		boundTxn bool
		reSum    func(
			*testing.T, *Store, types.MetadataBatchAccumulator, types.Txn,
		)
		wantReward uint64
	}{
		{
			name:       "withdrawal_in_outer_txn",
			boundTxn:   true,
			reSum:      withdraw,
			wantReward: startReward - amount,
		},
		{
			name:       "withdrawal_without_txn",
			reSum:      withdraw,
			wantReward: startReward - amount,
		},
		{
			name:     "reward_credit",
			boundTxn: true,
			reSum: func(
				t *testing.T,
				store *Store,
				_ types.MetadataBatchAccumulator,
				txn types.Txn,
			) {
				t.Helper()
				require.NoError(t, store.AddAccountRewardByCredential(
					credential.Tag, credential.Key, amount, 100,
					[]byte{0x03}, txn,
				))
			},
			wantReward: startReward + amount,
		},
		{
			name:     "reward_credit_batch",
			boundTxn: true,
			reSum: func(
				t *testing.T,
				store *Store,
				_ types.MetadataBatchAccumulator,
				txn types.Txn,
			) {
				t.Helper()
				require.NoError(t, store.AddAccountRewardsByCredential(
					[]models.AccountRewardCredit{{
						StakingKey:    credential.Key,
						CredentialTag: credential.Tag,
						SourceHash:    []byte{0x04},
						Amount:        amount,
						Slot:          100,
					}},
					txn,
				))
			},
			wantReward: startReward + amount,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newAPIModeSQLiteStore(t, nil)
			require.NoError(t, store.CreateAccount(nil, &models.Account{
				StakingKey:    credential.Key,
				CredentialTag: credential.Tag,
				Reward:        types.Uint64(startReward),
				Active:        true,
			}))
			acc := store.NewBatchAccumulator()
			defer acc.Reset()
			var txn types.Txn
			if tc.boundTxn {
				txn = store.Transaction(t.Context())
				t.Cleanup(func() { _ = txn.Rollback() })
			}
			pay(t, store, acc, txn)
			tc.reSum(t, store, acc, txn)
			require.NoError(t, store.FlushBatch(acc, txn))

			want := pendingLiveStake{
				utxo:   payment,
				reward: tc.wantReward,
				total:  payment + tc.wantReward,
			}
			db, ctx, err := store.dbFromTxn(txn)
			require.NoError(t, err)
			var independent pendingLiveStake
			require.NoError(t, db.QueryRowContext(ctx, `
SELECT COALESCE(SUM(CAST(amount AS INTEGER)), 0) FROM utxo
WHERE credential_tag = ? AND staking_key = ? AND deleted_slot = 0`,
				credential.Tag, credential.Key,
			).Scan(&independent.utxo))
			require.NoError(t, db.QueryRowContext(ctx, `
SELECT CAST(reward AS INTEGER) FROM account
WHERE credential_tag = ? AND staking_key = ?`,
				credential.Tag, credential.Key,
			).Scan(&independent.reward))
			independent.total = independent.utxo + independent.reward
			require.Equal(t, want, independent,
				"final UTxO set and reward balance")
			var stored pendingLiveStake
			require.NoError(t, db.QueryRowContext(ctx, `
SELECT CAST(utxo_stake AS INTEGER), CAST(reward_stake AS INTEGER),
       CAST(total_stake AS INTEGER)
FROM reward_live_stake WHERE credential_tag = ? AND staking_key = ?`,
				credential.Tag, credential.Key,
			).Scan(&stored.utxo, &stored.reward, &stored.total))
			require.Equal(t, want, stored,
				"reward_live_stake must count the queued payment once")
		})
	}
}
