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

package mempool

import (
	"bytes"
	"context"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// withdrawalTxCbor encodes a Conway transaction spending a distinct input and
// withdrawing amount from the reward account of stakeKey.
func withdrawalTxCbor(
	t *testing.T,
	seed byte,
	stakeKey []byte,
	amount uint64,
) []byte {
	t.Helper()
	rewardAddress := append([]byte{0xe1}, stakeKey...)
	body := map[uint]any{
		0: cbor.Tag{
			Number: 258,
			Content: []any{
				[]any{bytes.Repeat([]byte{seed}, 32), uint64(0)},
			},
		},
		1: []any{[]any{
			append([]byte{0x61}, make([]byte, 28)...),
			uint64(1_000_000),
		}},
		2: uint64(200_000),
		5: map[cbor.ByteString]uint64{
			cbor.NewByteString(rewardAddress): amount,
		},
	}
	txCbor, err := cbor.Encode([]any{body, map[uint]any{}, true, nil})
	require.NoError(t, err)
	_, err = conway.NewConwayTransactionFromCbor(txCbor)
	require.NoError(t, err)
	return txCbor
}

// balanceValidator runs the Shelley reward-withdrawal rule against a stored
// balance, seen through whatever the pending-state overlay has applied.
type balanceValidator struct {
	balance atomic.Uint64
	// reads counts reward balance reads against the stored state.
	reads atomic.Int64
	// tip is the slot of the ledger tip the stored balance belongs to.
	tip atomic.Uint64
}

type countingLedger struct {
	lcommon.LedgerState
	reads *atomic.Int64
}

func (l countingLedger) RewardAccountBalance(
	cred lcommon.Credential,
) (*uint64, error) {
	l.reads.Add(1)
	return l.LedgerState.RewardAccountBalance(cred)
}

func (v *balanceValidator) ValidateTx(tx gledger.Transaction) error {
	return v.ValidateTxWithOverlay(tx, nil, nil, nil)
}

func (v *balanceValidator) ValidateTxWithOverlay(
	tx gledger.Transaction,
	_ map[utxoref.Key]struct{},
	_ map[utxoref.Key]lcommon.Utxo,
	pending *utxoref.StateOverlay,
) error {
	var stakeKey lcommon.Blake2b224
	copy(stakeKey[:], withdrawalStakeKey)
	base := countingLedger{
		LedgerState: mockledger.NewLedgerStateBuilder().
			WithRewardAccountBalance(stakeKey, v.balance.Load()).
			Build(),
		reads: &v.reads,
	}
	state, err := pending.View(base, nil, ocommon.Point{Slot: v.tip.Load()})
	if err != nil {
		return err
	}
	return shelley.UtxoValidateWithdrawals(tx, 0, state, nil)
}

func (v *balanceValidator) WithTxValidationSession(
	fn func(
		func(
			gledger.Transaction,
			map[utxoref.Key]struct{},
			map[utxoref.Key]lcommon.Utxo,
			*utxoref.StateOverlay,
		) error,
		func() bool,
	) error,
) error {
	return fn(v.ValidateTxWithOverlay, func() bool { return true })
}

func newWithdrawalPool(
	t *testing.T,
	implementation Implementation,
	balance uint64,
) (*Mempool, *balanceValidator) {
	t.Helper()
	validator := &balanceValidator{}
	validator.balance.Store(balance)
	pool, err := newMempool(MempoolConfig{
		Validator:       validator,
		MempoolCapacity: 1 << 20,
		PromRegistry:    prometheus.NewRegistry(),
	}, implementation)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Stop(context.Background()) })
	return pool, validator
}

var withdrawalStakeKey = bytes.Repeat([]byte{0xa1}, 28)

func TestAdmissionAccountsForPendingWithdrawals(t *testing.T) {
	t.Parallel()
	for _, implementation := range []Implementation{
		ImplementationFIFO,
		ImplementationDAG,
	} {
		t.Run(string(implementation), func(t *testing.T) {
			t.Parallel()
			pool, _ := newWithdrawalPool(t, implementation, 100)
			first := withdrawalTxCbor(t, 0x01, withdrawalStakeKey, 100)
			second := withdrawalTxCbor(t, 0x02, withdrawalStakeKey, 100)
			require.NoError(
				t,
				pool.AddTransaction(uint(conway.EraIdConway), first),
			)

			err := pool.AddTransaction(uint(conway.EraIdConway), second)
			var incorrect shelley.IncorrectWithdrawalAmountError
			require.ErrorAs(
				t,
				err,
				&incorrect,
				"a second withdrawal must be checked against the balance "+
					"the pending one leaves",
			)
			require.Zero(t, incorrect.Balance)
			require.Len(t, pool.Transactions(), 1)

			firstTx, err := gledger.NewTransactionFromCbor(
				uint(conway.EraIdConway),
				first,
			)
			require.NoError(t, err)
			pool.RemoveTransaction(firstTx.Hash().String())
			require.NoError(
				t,
				pool.AddTransaction(uint(conway.EraIdConway), second),
				"removing the pending withdrawal releases its balance",
			)
		})
	}
}

func TestRevalidationAccountsForPendingWithdrawals(t *testing.T) {
	t.Parallel()
	for _, implementation := range []Implementation{
		ImplementationFIFO,
		ImplementationDAG,
	} {
		t.Run(string(implementation), func(t *testing.T) {
			t.Parallel()
			pool, _ := newWithdrawalPool(t, implementation, 100)
			first := withdrawalTxCbor(t, 0x01, withdrawalStakeKey, 100)
			second := withdrawalTxCbor(t, 0x02, withdrawalStakeKey, 0)
			require.NoError(
				t,
				pool.AddTransaction(uint(conway.EraIdConway), first),
			)
			require.NoError(
				t,
				pool.AddTransaction(uint(conway.EraIdConway), second),
				"the first withdrawal drains the account the second reads",
			)

			require.NoError(t, pool.rebuildOverlay())
			require.Len(
				t,
				pool.Transactions(),
				2,
				"revalidation must apply the first withdrawal before the second, "+
					"or the stored balance makes the second one invalid",
			)
		})
	}
}

func TestPendingStateWorkGrowsLinearlyWithPoolSize(t *testing.T) {
	t.Parallel()
	const count = 40
	pool, validator := newWithdrawalPool(t, ImplementationFIFO, 100)
	for i := range count {
		amount := uint64(0)
		if i == 0 {
			amount = 100
		}
		require.NoError(
			t,
			pool.AddTransaction(
				uint(conway.EraIdConway),
				withdrawalTxCbor(t, byte(i+1), withdrawalStakeKey, amount),
			),
		)
	}
	// Each validation folds the one transaction admitted since the last and
	// reads a constant number of balances; replaying the whole pool each time
	// would read about count*count/2.
	const perValidation = 6
	require.LessOrEqual(
		t,
		validator.reads.Load(),
		int64(count*perValidation),
		"admission re-applied pending transactions it had already folded",
	)

	validator.reads.Store(0)
	require.NoError(t, pool.rebuildOverlay())
	require.LessOrEqual(
		t,
		validator.reads.Load(),
		int64(count*perValidation),
		"revalidation work must grow linearly with the pool",
	)
}

func TestRemovalKeepsStateOverlayUnlessStatefulTxDropped(t *testing.T) {
	t.Parallel()
	pool, _ := newWithdrawalPool(t, ImplementationFIFO, 100)
	utxoOnly, _, utxoOnlyHash, _ := getDependentTestTxBytes(t)
	require.NoError(
		t,
		pool.AddTransaction(uint(conway.EraIdConway), utxoOnly),
	)
	stateful := withdrawalTxCbor(t, 0x01, withdrawalStakeKey, 100)
	require.NoError(
		t,
		pool.AddTransaction(uint(conway.EraIdConway), stateful),
	)
	statefulTx, err := gledger.NewTransactionFromCbor(
		uint(conway.EraIdConway),
		stateful,
	)
	require.NoError(t, err)
	before := pool.overlay.accounts

	pool.RemoveTransaction(utxoOnlyHash)
	require.Same(
		t,
		before,
		pool.overlay.accounts,
		"dropping a transaction with no state effects must keep the folded state",
	)

	pool.RemoveTransaction(statefulTx.Hash().String())
	require.NotSame(t, before, pool.overlay.accounts)
	require.Zero(t, pool.overlay.accounts.Len())
}

func TestAdmissionFollowsLedgerTipWithoutRebuild(t *testing.T) {
	t.Parallel()
	pool, validator := newWithdrawalPool(t, ImplementationFIFO, 0)
	// Zero withdrawals keep the pool's folded state populated with the balance
	// the base reported. The fold is lazy, so the second admission is the
	// validation that folds the first.
	for _, seed := range []byte{0x01, 0x03} {
		require.NoError(
			t,
			pool.AddTransaction(
				uint(conway.EraIdConway),
				withdrawalTxCbor(t, seed, withdrawalStakeKey, 0),
			),
		)
	}

	// A new block funds the account; the pool has not rebuilt yet.
	validator.balance.Store(100)
	validator.tip.Store(1)

	require.NoError(
		t,
		pool.AddTransaction(
			uint(conway.EraIdConway),
			withdrawalTxCbor(t, 0x02, withdrawalStakeKey, 100),
		),
		"admission read a balance from before the tip changed",
	)
}
