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

package node

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

func dijkstraCopyTestOutput(
	t *testing.T,
	fill byte,
	amount uint64,
) dijkstra.DijkstraTransactionOutput {
	t.Helper()
	address, err := lcommon.NewAddressFromBytes(
		append([]byte{0x60}, bytes.Repeat([]byte{fill}, 28)...),
	)
	require.NoError(t, err)
	return dijkstra.DijkstraTransactionOutput{
		Output: &shelley.ShelleyTransactionOutput{
			OutputAddress: address,
			OutputAmount:  amount,
		},
	}
}

// CDDL Dijkstra blocks carry phase-2 validity as each transaction's trailing
// is_valid field, not as a block-level invalid_transactions list. The raw
// copy must therefore store what DijkstraTransaction.Produced() yields: for an
// is_valid=false transaction only its collateral return, at index
// len(outputs); for a valid one its outputs and those of its
// sub-transactions, keyed by the sub-transaction body hash.
func TestStoreRawBlockUtxoOffsetsDijkstraTransactionValidity(t *testing.T) {
	t.Parallel()

	collateralReturn := dijkstraCopyTestOutput(t, 0x13, 3_000_000)
	invalidTx := dijkstra.DijkstraTransaction{
		Body: dijkstra.DijkstraTransactionBody{
			TxOutputs: []dijkstra.DijkstraTransactionOutput{
				dijkstraCopyTestOutput(t, 0x11, 2_000_000),
			},
			TxFee:              1,
			TxCollateralReturn: &collateralReturn,
		},
		TxIsValid: false,
	}
	validTx := dijkstra.DijkstraTransaction{
		Body: dijkstra.DijkstraTransactionBody{
			TxOutputs: []dijkstra.DijkstraTransactionOutput{
				dijkstraCopyTestOutput(t, 0x21, 4_000_000),
			},
			TxFee: 2,
			TxSubTransactions: cbor.NewSetType(
				[]dijkstra.DijkstraSubTransaction{{
					Body: dijkstra.DijkstraSubTransactionBody{
						TxOutputs: []dijkstra.DijkstraTransactionOutput{
							dijkstraCopyTestOutput(t, 0x22, 5_000_000),
						},
					},
				}},
				true,
			),
		},
		TxIsValid: true,
	}

	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					Slot: 11,
					ProtoVersion: babbage.BabbageProtoVersion{
						Major: dijkstra.MinProtocolVersionDijkstra,
					},
				},
			},
		},
		BlockBody: dijkstra.DijkstraBlockBody{
			Transactions: []dijkstra.DijkstraTransaction{invalidTx, validTx},
		},
	}
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodyHash = block.BlockBody.Hash()
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	decoded, err := dijkstra.NewDijkstraBlockFromCbor(blockCbor)
	require.NoError(t, err)
	txs := decoded.Transactions()
	require.Len(t, txs, 2)
	require.False(t, txs[0].IsValid())
	require.True(t, txs[1].IsValid())
	invalidHash := txs[0].Hash()
	validHash := txs[1].Hash()
	subTxs := decoded.BlockBody.Transactions[1].Body.TxSubTransactions.Items()
	require.Len(t, subTxs, 1)
	subHash := subTxs[0].Body.Id()

	db := newTestDB(t)
	txn := db.BlobTxn(true)
	defer txn.Rollback() //nolint:errcheck

	stored, err := storeRawBlockUtxoOffsets(txn, chain.RawBlock{
		Slot: decoded.SlotNumber(),
		Hash: decoded.Hash().Bytes(),
		Cbor: blockCbor,
		Type: gledger.BlockTypeDijkstra,
	})
	require.NoError(t, err)

	requireOffset := func(
		label string,
		txHash lcommon.Blake2b256,
		idx uint32,
		want []byte,
	) {
		t.Helper()
		data, err := db.Blob().GetUtxo(txn.Blob(), txHash.Bytes(), idx)
		require.NoError(t, err, label)
		offset, err := database.DecodeUtxoOffset(data)
		require.NoError(t, err, label)
		end := offset.ByteOffset + offset.ByteLength
		require.Equal(t, want, blockCbor[offset.ByteOffset:end], label)
	}
	requireAbsent := func(label string, txHash lcommon.Blake2b256, idx uint32) {
		t.Helper()
		_, err := db.Blob().GetUtxo(txn.Blob(), txHash.Bytes(), idx)
		require.ErrorIs(t, err, types.ErrBlobKeyNotFound, label)
	}

	invalidBody := decoded.BlockBody.Transactions[0].Body
	validBody := decoded.BlockBody.Transactions[1].Body
	requireAbsent("invalid tx regular output", invalidHash, 0)
	requireOffset(
		"invalid tx collateral return",
		invalidHash,
		1,
		invalidBody.TxCollateralReturn.Cbor(),
	)
	requireOffset("valid tx output", validHash, 0, validBody.TxOutputs[0].Cbor())
	requireAbsent("valid tx collateral return slot", validHash, 1)
	requireOffset(
		"sub-transaction output",
		subHash,
		0,
		subTxs[0].Body.TxOutputs[0].Cbor(),
	)
	require.Equal(t, 3, stored)
}
