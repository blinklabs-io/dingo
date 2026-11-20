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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

var withdrawalDebitStakeKey = lcommon.Blake2b224{0xa1, 0xa2, 0xa3}

func withdrawalDebitRegistration(
	t *testing.T,
) (lcommon.Transaction, ocommon.Point) {
	t.Helper()
	txID := make([]byte, 32)
	txID[0] = 0x31
	tx := mockledger.NewTransactionBuilder().WithCertificates(
		&lcommon.StakeRegistrationCertificate{
			CertType: uint(lcommon.CertificateTypeStakeRegistration),
			StakeCredential: lcommon.Credential{
				CredType:   lcommon.CredentialTypeAddrKeyHash,
				Credential: withdrawalDebitStakeKey,
			},
		},
	)
	tx.WithId(txID)
	tx.WithValid(true)
	return tx, ocommon.Point{Slot: 100, Hash: txID}
}

func withdrawalDebitWithdrawal(
	t *testing.T,
	amount uint64,
) (lcommon.Transaction, ocommon.Point) {
	t.Helper()
	txID := make([]byte, 32)
	txID[0] = 0x32
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		withdrawalDebitStakeKey.Bytes(),
	)
	require.NoError(t, err)
	tx := mockledger.NewTransactionBuilder()
	tx.WithId(txID)
	tx.WithValid(true)
	tx.WithWithdrawals(map[*lcommon.Address]uint64{&addr: amount})
	return tx, ocommon.Point{Slot: 300, Hash: txID}
}

func accountReward(t *testing.T, store *Store, txn types.Txn) string {
	t.Helper()
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	var reward string
	require.NoError(t, db.QueryRowContext(
		ctx, `SELECT COALESCE(reward, '0') FROM account`,
	).Scan(&reward))
	return reward
}

// withdrawalScenario registers an account, credits it, and withdraws part of
// the credit through apply, returning the stored balance.
func withdrawalScenario(
	t *testing.T,
	apply func(store *Store, txn types.Txn, tx lcommon.Transaction, point ocommon.Point),
) string {
	t.Helper()
	store := newAPIModeSQLiteStore(t, nil)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	reg, regPoint := withdrawalDebitRegistration(t)
	require.NoError(t, store.SetTransaction(
		reg, regPoint, 0, map[int]uint64{0: 2_000_000}, false, txn, 0,
	))
	require.NoError(t, store.AddAccountRewardsByCredential(
		[]models.AccountRewardCredit{{
			StakingKey: withdrawalDebitStakeKey.Bytes(),
			SourceHash: []byte{0x77},
			Amount:     5_000_000,
			Slot:       200,
		}}, txn,
	))
	require.Equal(t, "5000000", accountReward(t, store, txn))
	tx, point := withdrawalDebitWithdrawal(t, 2_000_000)
	apply(store, txn, tx, point)
	return accountReward(t, store, txn)
}

// TestHistoricalWithdrawalDebitsOnlyWhenRequested compares the balance a
// replayed withdrawal leaves with the live apply path's.
func TestHistoricalWithdrawalDebitsOnlyWhenRequested(t *testing.T) {
	t.Parallel()
	live := withdrawalScenario(
		t,
		func(store *Store, txn types.Txn, tx lcommon.Transaction, point ocommon.Point) {
			require.NoError(
				t,
				store.SetTransaction(tx, point, 0, nil, false, txn, 0),
			)
		},
	)
	require.Equal(t, "3000000", live)

	historical := func(debit bool) string {
		return withdrawalScenario(
			t,
			func(store *Store, txn types.Txn, tx lcommon.Transaction, point ocommon.Point) {
				acc := store.NewBatchAccumulator()
				defer acc.Reset()
				setter, ok := acc.(interface{ SetDebitWithdrawals(bool) })
				require.True(
					t,
					ok,
					"the accumulator must offer withdrawal debits",
				)
				setter.SetDebitWithdrawals(debit)
				require.NoError(t, store.SetTransactionBatchedHistorical(
					tx, point, 0, nil, false, true, acc, txn, 0,
				))
				require.NoError(t, store.FlushBatch(acc, txn))
			},
		)
	}
	require.Equal(t, live, historical(true),
		"a replayed withdrawal must debit the account as live apply does")
	require.Equal(t, "5000000", historical(false),
		"without the debit the trusted boundary balance is left alone")
}

// A certified closure's withdrawal follows the replay's choice exactly as a
// replayed block's does: debited when the replay replays the credits, and
// leaving the trusted boundary balance alone when it does not.
func TestClosureWithdrawalFollowsTheReplayDebitChoice(t *testing.T) {
	t.Parallel()
	closure := func(debit bool) string {
		return withdrawalScenario(
			t,
			func(store *Store, txn types.Txn, tx lcommon.Transaction, point ocommon.Point) {
				acc := store.NewLeiosClosureBatchAccumulator()
				defer acc.Reset()
				setter, ok := acc.(interface{ SetDebitWithdrawals(bool) })
				require.True(t, ok)
				setter.SetDebitWithdrawals(debit)
				require.NoError(t, store.SetTransactionLeiosClosureBatched(
					tx, point, 0, nil, false, nil, acc, txn, 0,
				))
				require.NoError(t, store.FlushBatch(acc, txn))
			},
		)
	}
	require.Equal(t, "3000000", closure(true))
	require.Equal(t, "5000000", closure(false),
		"a replay that trusts the imported balance does not debit a closure either")
}
