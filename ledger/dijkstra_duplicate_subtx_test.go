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

package ledger

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

const duplicateSubTxBodyError = "duplicate Dijkstra sub-transaction body"

// dupSubTxBody encodes a sub-transaction body whose identity is set by ttl.
func dupSubTxBody(t *testing.T, ttl uint64) []byte {
	t.Helper()
	body, err := cbor.Encode(map[uint]any{0: []any{}, 1: []any{}, 3: ttl})
	require.NoError(t, err)
	return body
}

func dupSubTx(t *testing.T, body []byte, witnesses, aux any) []byte {
	t.Helper()
	out, err := cbor.Encode([]any{cbor.RawMessage(body), witnesses, aux})
	require.NoError(t, err)
	return out
}

func dupBatchTx(t *testing.T, subTxs ...[]byte) []byte {
	t.Helper()
	raw := make([]cbor.RawMessage, len(subTxs))
	for i := range subTxs {
		raw[i] = subTxs[i]
	}
	body, err := cbor.Encode(map[uint]any{
		0:  []any{},
		1:  []any{},
		2:  uint64(0),
		23: cbor.NewSetType(raw, true),
	})
	require.NoError(t, err)
	txCbor, err := cbor.Encode(
		[]any{cbor.RawMessage(body), map[uint]any{}, nil},
	)
	require.NoError(t, err)
	return txCbor
}

// TestDijkstraTransactionRejectsDuplicateSubTransactionBodyID covers the
// decode path every live entry point shares: two key-23 entries with one body
// and different witnesses or auxiliary data are not distinct transactions.
func TestDijkstraTransactionRejectsDuplicateSubTransactionBodyID(
	t *testing.T,
) {
	t.Parallel()
	body := dupSubTxBody(t, 10)
	witnesses := map[uint]any{
		0: [][]any{{bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 64)}},
	}
	for _, tc := range []struct {
		name         string
		first, other []byte
	}{
		{
			"different witnesses",
			dupSubTx(t, body, map[uint]any{}, nil),
			dupSubTx(t, body, witnesses, nil),
		},
		{
			"different auxiliary data",
			dupSubTx(t, body, map[uint]any{}, nil),
			dupSubTx(t, body, map[uint]any{}, map[uint]any{1: "metadata"}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := gledger.NewTransactionFromCbor(
				gledger.TxTypeDijkstra,
				dupBatchTx(t, tc.first, tc.other),
			)
			require.ErrorContains(t, err, duplicateSubTxBodyError)
		})
	}

	t.Run("distinct bodies are accepted", func(t *testing.T) {
		t.Parallel()
		tx, err := gledger.NewTransactionFromCbor(
			gledger.TxTypeDijkstra,
			dupBatchTx(
				t,
				dupSubTx(t, dupSubTxBody(t, 10), map[uint]any{}, nil),
				dupSubTx(t, dupSubTxBody(t, 11), map[uint]any{}, nil),
			),
		)
		require.NoError(t, err)
		require.Len(
			t,
			tx.(*dijkstra.DijkstraTransaction).Body.TxSubTransactions.Items(),
			2,
		)
	})
}

// TestDijkstraBlockRejectsDuplicateSubTransactionBodyID decodes a block, the
// form replay and backfill read, whose batch repeats a child body with
// different witnesses.
func TestDijkstraBlockRejectsDuplicateSubTransactionBodyID(t *testing.T) {
	t.Parallel()
	bodyA := dupSubTxBody(t, 10)
	// Same length as bodyA so the block can be patched in place below.
	bodyB := dupSubTxBody(t, 11)
	require.Len(t, bodyB, len(bodyA))
	batchBody, err := cbor.Encode(map[uint]any{
		0: []any{},
		1: []any{},
		2: uint64(0),
		23: cbor.NewSetType([]cbor.RawMessage{
			dupSubTx(t, bodyA, map[uint]any{}, nil),
			dupSubTx(t, bodyB, map[uint]any{
				0: [][]any{{
					bytes.Repeat([]byte{1}, 32),
					bytes.Repeat([]byte{2}, 64),
				}},
			}, nil),
		}, true),
	})
	require.NoError(t, err)
	// A block transaction is [body, witnesses, auxiliary data, is_valid].
	blockTx, err := cbor.Encode(
		[]any{cbor.RawMessage(batchBody), map[uint]any{}, nil, true},
	)
	require.NoError(t, err)
	blockBodyCbor, err := cbor.Encode(
		[]any{[]cbor.RawMessage{blockTx}, nil, nil},
	)
	require.NoError(t, err)
	var blockBody dijkstra.DijkstraBlockBody
	require.NoError(t, blockBody.UnmarshalCBOR(blockBodyCbor))
	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber:  1,
					Slot:         1,
					ProtoVersion: babbage.BabbageProtoVersion{Major: 12},
				},
			},
		},
		BlockBody: blockBody,
	}
	oldBodyHash := blockBody.Hash()
	block.BlockHeader.Body.BlockBodyHash = oldBodyHash
	block.BlockHeader.Body.BlockBodySize = uint64(len(blockBodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)

	decoded, err := gledger.NewBlockFromCbor(
		uint(dijkstra.BlockTypeDijkstra),
		blockCbor,
	)
	require.NoError(t, err, "distinct child bodies form a valid block")
	require.NotNil(t, decoded)

	// Patch the body and the header's body hash together so the duplicate is
	// the only thing wrong with the block.
	require.Equal(t, 1, bytes.Count(blockCbor, bodyB))
	patchedBody := bytes.Replace(blockBodyCbor, bodyB, bodyA, 1)
	require.NotEqual(t, blockBodyCbor, patchedBody)
	require.Equal(t, 1, bytes.Count(blockCbor, oldBodyHash.Bytes()))
	duplicated := bytes.Replace(blockCbor, bodyB, bodyA, 1)
	duplicated = bytes.Replace(
		duplicated,
		oldBodyHash.Bytes(),
		common.Blake2b256Hash(patchedBody).Bytes(),
		1,
	)
	_, err = gledger.NewBlockFromCbor(
		uint(dijkstra.BlockTypeDijkstra),
		duplicated,
	)
	require.ErrorContains(t, err, duplicateSubTxBodyError)
}
