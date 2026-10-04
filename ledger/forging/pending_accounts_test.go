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

package forging

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
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
			cbor.NewByteString(append([]byte{0xe1}, stakeKey...)): amount,
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
	balance uint64
}

func (v *balanceValidator) ValidateTx(tx ledger.Transaction) error {
	return v.ValidateTxWithOverlay(tx, nil, nil, nil)
}

func (v *balanceValidator) ValidateTxWithOverlay(
	tx ledger.Transaction,
	_ map[utxoref.Key]struct{},
	_ map[utxoref.Key]lcommon.Utxo,
	pending *utxoref.StateOverlay,
) error {
	var stakeKey lcommon.Blake2b224
	copy(stakeKey[:], bytes.Repeat([]byte{0xa1}, 28))
	base := mockledger.NewLedgerStateBuilder().
		WithRewardAccountBalance(stakeKey, v.balance).
		Build()
	state, err := pending.View(base, nil)
	if err != nil {
		return err
	}
	return shelley.UtxoValidateWithdrawals(tx, 0, state, nil)
}

func twoWithdrawalsOfOneBalance(
	t *testing.T,
) ([]MempoolTransaction, *balanceValidator) {
	t.Helper()
	stakeKey := bytes.Repeat([]byte{0xa1}, 28)
	txs := make([]MempoolTransaction, 0, 2)
	for _, seed := range []byte{0x61, 0x62} {
		txCbor := withdrawalTxCbor(t, seed, stakeKey, 100)
		tx, err := conway.NewConwayTransactionFromCbor(txCbor)
		require.NoError(t, err)
		txs = append(txs, MempoolTransaction{
			Hash: tx.Hash().String(),
			Cbor: txCbor,
			Type: conway.TxTypeConway,
		})
	}
	return txs, &balanceValidator{balance: 100}
}

func TestBuildBlockAccountsForSelectedWithdrawals(t *testing.T) {
	t.Parallel()
	txs, validator := twoWithdrawalsOfOneBalance(t)
	builder := newSelectionTestBuilder(
		t,
		&mockMempool{transactions: txs},
		selectionTestChainTip(),
		validator,
	)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.Len(
		t,
		block.Transactions(),
		1,
		"the second withdrawal of the same balance must not be selected",
	)
	require.Equal(t, txs[0].Hash, block.Transactions()[0].Hash().String())
}

func TestSelectValidLeiosTransactionsAccountsForSelectedWithdrawals(
	t *testing.T,
) {
	t.Parallel()
	txs, validator := twoWithdrawalsOfOneBalance(t)

	selected, _, err := selectValidLeiosTransactions(
		txs,
		validator,
		leiosSelectionLimits{},
	)
	require.NoError(t, err)
	require.Equal(t, txs[:1], selected)
}
