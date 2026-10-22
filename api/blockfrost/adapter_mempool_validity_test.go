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

package blockfrost

import (
	"context"
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/btcsuite/btcd/btcutil/bech32"
	"github.com/stretchr/testify/require"
)

// failedMempoolTx builds a Conway transaction marked as failing phase 2: a
// regular input and output, a collateral input and a collateral return, and
// spend, mint and voting redeemers.
func failedMempoolTx(
	t *testing.T,
	input, collateral []byte,
	payee, collateralPayee lcommon.Address,
) mempool.MempoolTransaction {
	t.Helper()
	redeemer := lcommon.RedeemerValue{
		Data:    lcommon.Datum{Data: data.NewInteger(big.NewInt(1))},
		ExUnits: lcommon.ExUnits{Memory: 1, Steps: 2},
	}
	collateralReturn := babbage.BabbageTransactionOutput{
		OutputAddress: collateralPayee,
		OutputAmount:  mary.MaryTransactionOutputValue{Amount: 3_000_000},
	}
	tx := &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{
					shelley.MustNewShelleyTransactionInput(
						hex.EncodeToString(input), 0,
					),
				},
			),
			TxOutputs: []babbage.BabbageTransactionOutput{{
				OutputAddress: payee,
				OutputAmount: mary.MaryTransactionOutputValue{
					Amount: 4_000_000,
				},
			}},
			TxFee: 1,
			TxCollateral: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{
					shelley.MustNewShelleyTransactionInput(
						hex.EncodeToString(collateral), 0,
					),
				},
				false,
			),
			TxCollateralReturn: &collateralReturn,
		},
		WitnessSet: conway.ConwayTransactionWitnessSet{
			WsRedeemers: conway.ConwayRedeemers{
				Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
					{Tag: lcommon.RedeemerTagVoting, Index: 0}: redeemer,
					{Tag: lcommon.RedeemerTagMint, Index: 0}:   redeemer,
					{Tag: lcommon.RedeemerTagSpend, Index: 0}:  redeemer,
				},
			},
		},
		TxIsValid: false,
	}
	raw, err := tx.MarshalCBOR()
	require.NoError(t, err)
	decoded, err := gledger.NewTransactionFromCbor(
		uint(gledger.TxTypeConway), raw,
	)
	require.NoError(t, err)
	require.False(t, decoded.IsValid())
	return mempool.MempoolTransaction{
		Hash: decoded.Hash().String(),
		Cbor: raw,
		Type: uint(gledger.TxTypeConway),
	}
}

func TestNodeAdapterMempoolTransactionFailingPhaseTwo(t *testing.T) {
	t.Parallel()
	adapter, _, _ := newDBBackedAdapter(t)
	payee, _, _ := testKeyAddress(t, 0xc1, 0)
	collateralPayee, _, _ := testKeyAddress(t, 0xc2, 0)
	entry := failedMempoolTx(
		t, fill32(0xc3), fill32(0xc4), payee, collateralPayee,
	)
	adapter.mempool = &fakeMempool{txs: []mempool.MempoolTransaction{entry}}

	hash, err := hex.DecodeString(entry.Hash)
	require.NoError(t, err)
	got, err := adapter.MempoolTransaction(context.Background(), hash)
	require.NoError(t, err)
	require.False(t, got.Tx.ValidContract)
	require.Len(t, got.Outputs, 1, "only the collateral return is produced")
	require.Equal(t, collateralPayee.String(), got.Outputs[0].Address)
	require.Equal(t, uint32(1), got.Outputs[0].OutputIndex)
	require.True(t, got.Outputs[0].Collateral)
	require.Equal(t, []AddressAmountInfo{
		{Unit: "lovelace", Quantity: "3000000"},
	}, got.Tx.OutputAmount)
	require.Equal(
		t, 3, got.Tx.UtxoCount, "input, collateral, collateral return",
	)

	purposes := make([]string, 0, len(got.Redeemers))
	for _, redeemer := range got.Redeemers {
		purposes = append(purposes, redeemer.Purpose)
	}
	require.Equal(t, []string{"spend", "mint", "voting"}, purposes)
}

func TestNodeAdapterMempoolAddressTransactionsRejectsCredential(t *testing.T) {
	t.Parallel()
	adapter, _, _ := newDBBackedAdapter(t)
	adapter.mempool = &fakeMempool{}
	payee, _, _ := testKeyAddress(t, 0xe1, 0)
	conv, err := bech32.ConvertBits(payee.PaymentKeyHash().Bytes(), 8, 5, true)
	require.NoError(t, err)
	credential, err := bech32.Encode("addr_vkh", conv)
	require.NoError(t, err)
	_, _, err = adapter.MempoolAddressTransactions(
		context.Background(),
		credential,
		PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc},
	)
	require.ErrorIs(t, err, ErrInvalidAddress)
}
