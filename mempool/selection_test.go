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
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// selectionTxSpec describes a Conway transaction for selection-order tests.
// outputs controls the encoded size; each output is also a spendable output
// another spec can depend on.
type selectionTxSpec struct {
	fee        uint64
	outputs    int
	inputs     []shelley.ShelleyTransactionInput
	references []shelley.ShelleyTransactionInput
	collateral []shelley.ShelleyTransactionInput
}

func selectionTestInput(
	t *testing.T,
	seed byte,
) shelley.ShelleyTransactionInput {
	t.Helper()
	input, err := shelley.NewShelleyTransactionInput(
		string(bytes.Repeat([]byte{"0123456789abcdef"[seed%16]}, 64)),
		int(seed),
	)
	require.NoError(t, err)
	return input
}

func selectionOutputOf(
	t *testing.T,
	tx MempoolTransaction,
	index int,
) shelley.ShelleyTransactionInput {
	t.Helper()
	input, err := shelley.NewShelleyTransactionInput(tx.Hash, index)
	require.NoError(t, err)
	return input
}

func newSelectionTx(t *testing.T, spec selectionTxSpec) MempoolTransaction {
	t.Helper()
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x01}, 28),
		nil,
	)
	require.NoError(t, err)
	outputs := make([]babbage.BabbageTransactionOutput, spec.outputs)
	for i := range outputs {
		outputs[i] = babbage.BabbageTransactionOutput{
			OutputAddress: address,
			OutputAmount: mary.MaryTransactionOutputValue{
				Amount: 1_000_000 + uint64(i),
			},
		}
	}
	tx := conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxInputs:  conway.NewConwayTransactionInputSet(spec.inputs),
			TxOutputs: outputs,
			TxFee:     spec.fee,
		},
		TxIsValid: true,
	}
	if len(spec.references) > 0 {
		tx.Body.TxReferenceInputs = cbor.NewSetType(spec.references, false)
	}
	if len(spec.collateral) > 0 {
		tx.Body.TxCollateral = cbor.NewSetType(spec.collateral, false)
	}
	txCbor, err := cbor.Encode(&tx)
	require.NoError(t, err)
	decoded, err := gledger.NewTransactionFromCbor(
		uint(conway.EraIdConway),
		txCbor,
	)
	require.NoError(t, err)
	require.Equal(t, spec.fee, decoded.Fee().Uint64())
	return MempoolTransaction{
		Hash: decoded.Hash().String(),
		Cbor: txCbor,
		Type: uint(conway.EraIdConway),
	}
}

func selectionHashes(txs []MempoolTransaction) []string {
	hashes := make([]string, len(txs))
	for i, tx := range txs {
		hashes[i] = tx.Hash
	}
	return hashes
}

func TestOrderByFeeDensityPrefersFeePerByte(t *testing.T) {
	t.Parallel()
	low := newSelectionTx(t, selectionTxSpec{
		fee: 200_000, outputs: 1,
		inputs: []shelley.ShelleyTransactionInput{selectionTestInput(t, 1)},
	})
	// The largest absolute fee, spread over many outputs, so the lowest
	// fee per byte.
	large := newSelectionTx(t, selectionTxSpec{
		fee: 400_000, outputs: 40,
		inputs: []shelley.ShelleyTransactionInput{selectionTestInput(t, 2)},
	})
	high := newSelectionTx(t, selectionTxSpec{
		fee: 300_000, outputs: 1,
		inputs: []shelley.ShelleyTransactionInput{selectionTestInput(t, 3)},
	})
	require.Greater(t, len(large.Cbor), 4*len(high.Cbor))

	ordered := OrderByFeeDensity([]MempoolTransaction{low, large, high})

	require.Equal(
		t,
		selectionHashes([]MempoolTransaction{high, low, large}),
		selectionHashes(ordered),
	)
}

func TestOrderByFeeDensityKeepsParentsFirst(t *testing.T) {
	t.Parallel()
	parent := newSelectionTx(t, selectionTxSpec{
		fee: 170_000, outputs: 3,
		inputs: []shelley.ShelleyTransactionInput{selectionTestInput(t, 1)},
	})
	independent := newSelectionTx(t, selectionTxSpec{
		fee: 250_000, outputs: 1,
		inputs: []shelley.ShelleyTransactionInput{selectionTestInput(t, 2)},
	})
	spender := newSelectionTx(t, selectionTxSpec{
		fee: 900_000, outputs: 1,
		inputs: []shelley.ShelleyTransactionInput{
			selectionOutputOf(t, parent, 0),
		},
	})
	referencer := newSelectionTx(t, selectionTxSpec{
		fee: 800_000, outputs: 1,
		inputs:     []shelley.ShelleyTransactionInput{selectionTestInput(t, 3)},
		references: []shelley.ShelleyTransactionInput{selectionOutputOf(t, parent, 1)},
	})
	collateralUser := newSelectionTx(t, selectionTxSpec{
		fee: 700_000, outputs: 1,
		inputs:     []shelley.ShelleyTransactionInput{selectionTestInput(t, 4)},
		collateral: []shelley.ShelleyTransactionInput{selectionOutputOf(t, parent, 2)},
	})
	grandchild := newSelectionTx(t, selectionTxSpec{
		fee: 990_000, outputs: 1,
		inputs: []shelley.ShelleyTransactionInput{
			selectionOutputOf(t, spender, 0),
		},
	})

	ordered := OrderByFeeDensity([]MempoolTransaction{
		parent, independent, spender, referencer, collateralUser, grandchild,
	})

	// The independent transaction outranks the lower-density parent. Each
	// child, though paying more than either, waits for the parent whose
	// output it spends, references, or pledges as collateral, and the
	// grandchild waits for the spender. Ready children then go by fee per
	// byte.
	require.Equal(
		t,
		selectionHashes([]MempoolTransaction{
			independent, parent, spender, grandchild, referencer, collateralUser,
		}),
		selectionHashes(ordered),
	)
}

func TestOrderByFeeDensityTiesKeepAdmissionOrder(t *testing.T) {
	t.Parallel()
	txs := make([]MempoolTransaction, 4)
	for i := range txs {
		txs[i] = newSelectionTx(t, selectionTxSpec{
			fee: 200_000, outputs: 1,
			inputs: []shelley.ShelleyTransactionInput{
				selectionTestInput(t, byte(i+1)),
			},
		})
		require.Len(t, txs[i].Cbor, len(txs[0].Cbor))
	}

	require.Equal(t, selectionHashes(txs), selectionHashes(OrderByFeeDensity(txs)))
}

func TestOrderByFeeDensityKeepsUndecodableTransactions(t *testing.T) {
	t.Parallel()
	paying := newSelectionTx(t, selectionTxSpec{
		fee: 200_000, outputs: 1,
		inputs: []shelley.ShelleyTransactionInput{selectionTestInput(t, 1)},
	})
	garbage := MempoolTransaction{
		Hash: "garbage",
		Cbor: []byte{0xff, 0x00},
		Type: uint(conway.EraIdConway),
	}

	ordered := OrderByFeeDensity([]MempoolTransaction{garbage, paying})

	require.Equal(t, []string{paying.Hash, "garbage"}, selectionHashes(ordered))
}

func TestBlockCandidatesFollowSelectionPolicy(t *testing.T) {
	t.Parallel()
	low := newSelectionTx(t, selectionTxSpec{
		fee: 200_000, outputs: 1,
		inputs: []shelley.ShelleyTransactionInput{selectionTestInput(t, 1)},
	})
	high := newSelectionTx(t, selectionTxSpec{
		fee: 400_000, outputs: 1,
		inputs: []shelley.ShelleyTransactionInput{selectionTestInput(t, 2)},
	})
	admitted := selectionHashes([]MempoolTransaction{low, high})
	for _, tc := range []struct {
		selection SelectionPolicy
		want      []string
	}{
		{selection: "", want: admitted},
		{
			selection: SelectionFeeDensity,
			want:      selectionHashes([]MempoolTransaction{high, low}),
		},
	} {
		for name, factory := range map[string]backendContractFactory{
			"fifo": fifoContractFactory,
			"dag":  dagContractFactory,
		} {
			pool := factory(t, MempoolConfig{
				Validator:       newMockValidator(),
				MempoolCapacity: 1 << 20,
				Selection:       tc.selection,
			})
			t.Cleanup(func() { _ = pool.Stop(context.Background()) })
			for _, tx := range []MempoolTransaction{low, high} {
				require.NoError(t, pool.AddTransaction(
					t.Context(), tx.Type, tx.Cbor,
				))
			}
			candidates := pool.(interface {
				BlockCandidates() []MempoolTransaction
			}).BlockCandidates()
			require.Equal(
				t, tc.want, selectionHashes(candidates),
				"%s selection %q", name, tc.selection,
			)
		}
	}
}
