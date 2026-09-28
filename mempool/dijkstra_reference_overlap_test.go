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
	"context"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

type dijkstraOverlapValidator struct{}

func (dijkstraOverlapValidator) ValidateTx(tx gledger.Transaction) error {
	dijkstraTx, ok := tx.(*dijkstra.DijkstraTransaction)
	if !ok {
		return fmt.Errorf("expected Dijkstra transaction, got %T", tx)
	}
	inputs := dijkstraTx.Inputs()
	referenceInputs := dijkstraTx.ReferenceInputs()
	if len(inputs) != 1 || len(referenceInputs) != 1 ||
		inputs[0].String() != referenceInputs[0].String() {
		return fmt.Errorf("expected one overlapping spend and reference input")
	}
	return nil
}

func (v dijkstraOverlapValidator) ValidateTxWithOverlay(
	tx gledger.Transaction,
	_ map[utxoref.Key]struct{},
	_ map[utxoref.Key]lcommon.Utxo,
) error {
	return v.ValidateTx(tx)
}

func TestAddTransactionAcceptsDijkstraSpendReferenceOverlap(t *testing.T) {
	t.Parallel()
	input := shelley.NewShelleyTransactionInput(
		"a228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee11",
		0,
	)
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		make([]byte, lcommon.Blake2b224Size),
		nil,
	)
	require.NoError(t, err)
	tx := &dijkstra.DijkstraTransaction{
		Body: dijkstra.DijkstraTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{input},
			),
			TxOutputs: []dijkstra.DijkstraTransactionOutput{{
				Output: &mary.MaryTransactionOutput{
					OutputAddress: address,
					OutputAmount: mary.MaryTransactionOutputValue{
						Amount: 1_000_000,
					},
				},
			}},
			TxReferenceInputs: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{input},
				true,
			),
		},
		TxIsValid: true,
	}
	bodyCbor, err := cbor.Encode(tx.Body)
	require.NoError(t, err)
	tx.Body.SetCbor(bodyCbor)
	txCbor, err := tx.MarshalCBOR()
	require.NoError(t, err)

	pool := newTestMempoolWithValidator(t, dijkstraOverlapValidator{})
	defer pool.Stop(context.Background())
	require.NoError(t, pool.AddTransaction(uint(dijkstra.TxTypeDijkstra), txCbor))
	require.Len(t, pool.Transactions(), 1)
}
