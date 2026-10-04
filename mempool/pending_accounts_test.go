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
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
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

// balanceValidator applies the exact reward-withdrawal rule to a stored
// balance, reduced by whatever the pending-account overlay already drains.
type balanceValidator struct {
	balance atomic.Uint64
}

func (v *balanceValidator) ValidateTx(tx gledger.Transaction) error {
	return v.ValidateTxWithOverlay(tx, nil, nil, nil)
}

func (v *balanceValidator) ValidateTxWithOverlay(
	tx gledger.Transaction,
	_ map[utxoref.Key]struct{},
	_ map[utxoref.Key]lcommon.Utxo,
	accounts *utxoref.AccountOverlay,
) error {
	for address, amount := range tx.Withdrawals() {
		credential, ok := address.StakeCredential()
		if !ok {
			continue
		}
		available := accounts.Balance(credential, v.balance.Load())
		if amount.Uint64() > available {
			return fmt.Errorf(
				"withdrawal %s exceeds balance %d",
				amount,
				available,
			)
		}
	}
	return nil
}

func (v *balanceValidator) WithTxValidationSession(
	fn func(
		func(
			gledger.Transaction,
			map[utxoref.Key]struct{},
			map[utxoref.Key]lcommon.Utxo,
			*utxoref.AccountOverlay,
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
			require.ErrorContains(
				t,
				err,
				"exceeds balance 0",
				"a second withdrawal must be checked against the balance "+
					"the pending one leaves",
			)
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
			pool, validator := newWithdrawalPool(t, implementation, 1000)
			first := withdrawalTxCbor(t, 0x01, withdrawalStakeKey, 60)
			second := withdrawalTxCbor(t, 0x02, withdrawalStakeKey, 60)
			require.NoError(
				t,
				pool.AddTransaction(uint(conway.EraIdConway), first),
			)
			require.NoError(
				t,
				pool.AddTransaction(uint(conway.EraIdConway), second),
			)

			validator.balance.Store(100)
			require.NoError(t, pool.rebuildOverlay())

			firstTx, err := gledger.NewTransactionFromCbor(
				uint(conway.EraIdConway),
				first,
			)
			require.NoError(t, err)
			remaining := pool.Transactions()
			require.Len(
				t,
				remaining,
				1,
				"the second withdrawal no longer fits the balance left by the first",
			)
			require.Equal(t, firstTx.Hash().String(), remaining[0].Hash)
		})
	}
}
