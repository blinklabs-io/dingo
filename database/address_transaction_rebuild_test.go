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

package database

import (
	"bytes"
	"context"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

func rebuildTestOutput(payment byte, amount uint64) map[uint]any {
	return map[uint]any{
		0: append([]byte{0x60}, bytes.Repeat([]byte{payment}, 28)...),
		1: amount,
	}
}

func rebuildTestInput(ref byte) []any {
	return []any{bytes.Repeat([]byte{ref}, 32), uint64(0)}
}

// seedRebuildSource stores the output an input spends, at a key address whose
// payment key repeats payment.
func seedRebuildSource(t *testing.T, db *Database, ref byte, payment byte) {
	t.Helper()
	txn := db.MetadataTxn(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *Txn) error {
		return db.Metadata().CreateUtxo(txn.Metadata(), &models.Utxo{
			TxId:       bytes.Repeat([]byte{ref}, 32),
			Address:    []byte{'a', ref},
			PaymentKey: bytes.Repeat([]byte{payment}, 28),
			AddedSlot:  1,
			Amount:     types.Uint64(1),
		})
	}))
}

func rebuildPayments(rows []models.AddressTransaction) []byte {
	ret := make([]byte, 0, len(rows))
	for _, row := range rows {
		require.False(nil, row.PaymentScript)
		ret = append(ret, row.PaymentKey[0])
	}
	slices.Sort(ret)
	return ret
}

// A valid Dijkstra transaction stores each sub-transaction as a row ahead of
// itself, so a row's block_index counts those levels rather than indexing the
// block's transactions. Every row resolves to its own level by hash.
func TestRebuildAddressTransactionResolvesDijkstraLevels(t *testing.T) {
	t.Parallel()
	db := newTestDBWithMode(t, types.StorageModeAPI)
	for ref, payment := range map[byte]byte{0xa1: 0x11, 0xa2: 0x22, 0xa3: 0x33, 0xa4: 0x44} {
		seedRebuildSource(t, db, ref, payment)
	}

	childBody, err := cbor.Encode(map[uint]any{
		0: []any{rebuildTestInput(0xa1)},
		1: []any{rebuildTestOutput(0x51, 1_000_000)},
	})
	require.NoError(t, err)
	child, err := cbor.Encode(
		[]any{cbor.RawMessage(childBody), map[uint]any{}, nil},
	)
	require.NoError(t, err)
	batchBody, err := cbor.Encode(map[uint]any{
		0:  []any{rebuildTestInput(0xa2)},
		1:  []any{rebuildTestOutput(0x52, 2_000_000)},
		2:  uint64(5),
		13: []any{rebuildTestInput(0xa3)},
		23: cbor.NewSetType([]cbor.RawMessage{child}, true),
	})
	require.NoError(t, err)
	plainBody, err := cbor.Encode(map[uint]any{
		0: []any{rebuildTestInput(0xa4)},
		1: []any{rebuildTestOutput(0x54, 3_000_000)},
		2: uint64(5),
	})
	require.NoError(t, err)
	var transactions []dijkstra.DijkstraTransaction
	for _, body := range [][]byte{batchBody, plainBody} {
		txCbor, err := cbor.Encode(
			[]any{cbor.RawMessage(body), map[uint]any{}, true, nil},
		)
		require.NoError(t, err)
		tx, err := dijkstra.NewDijkstraTransactionFromCbor(txCbor)
		require.NoError(t, err)
		transactions = append(transactions, *tx)
	}
	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: 1,
					Slot:        100,
					VrfKey:      make([]byte, 32),
					VrfResult: lcommon.VrfResult{
						Output: make([]byte, 64),
						Proof:  make([]byte, 80),
					},
					OpCert: babbage.BabbageOpCert{
						HotVkey:   make([]byte, 32),
						Signature: make([]byte, 64),
					},
					ProtoVersion: babbage.BabbageProtoVersion{
						Major: dijkstra.MinProtocolVersionDijkstra,
					},
				},
				Signature: make([]byte, 448),
			},
		},
		BlockBody: dijkstra.DijkstraBlockBody{Transactions: transactions},
	}
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodyHash = block.BlockBody.Hash()
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	decoded, err := models.DecodeBlockCbor(gledger.BlockTypeDijkstra, blockCbor)
	require.NoError(t, err)
	const slot = uint64(100)
	blockHash := bytes.Repeat([]byte{0xbb}, 32)
	blobTxn := db.BlobTxn(true)
	require.NoError(t, blobTxn.Do(func(txn *Txn) error {
		return db.Blob().SetBlock(
			txn.Blob(), slot, blockHash, blockCbor, 1,
			gledger.BlockTypeDijkstra, 1, nil,
		)
	}))

	batch := decoded.Transactions()[0]
	subBodies := lcommon.SubTransactionBodiesFromTransaction(batch)
	require.Len(t, subBodies, 1)
	plain := decoded.Transactions()[1]
	// The ledger's storage order: the sub-transaction, its batch, the next
	// transaction.
	for index, tc := range []struct {
		hash []byte
		want []byte
	}{
		{subBodies[0].Id().Bytes(), []byte{0x11, 0x51}},
		{batch.Hash().Bytes(), []byte{0x22, 0x33, 0x52}},
		{plain.Hash().Bytes(), []byte{0x44, 0x54}},
	} {
		rows, err := db.rebuildAddressTransaction(context.Background(), models.Transaction{
			Hash:       tc.hash,
			BlockHash:  blockHash,
			Slot:       slot,
			BlockIndex: uint32(index), //nolint:gosec // three rows
			Valid:      true,
		})
		require.NoError(t, err, "row %d", index)
		require.Equal(t, tc.want, rebuildPayments(rows), "row %d", index)
	}
}

// A certified Leios closure transaction is stored under its ranking block's
// point, but its offset names the endorser block's standalone blob, which
// holds only its body. The row's stored validity decides whether it produced
// its outputs or only its collateral return; its regular inputs count either
// way.
func TestRebuildAddressTransactionReadsClosureBody(t *testing.T) {
	t.Parallel()
	db := newTestDBWithMode(t, types.StorageModeAPI)
	for ref, payment := range map[byte]byte{0xa1: 0x11, 0xa2: 0x22} {
		seedRebuildSource(t, db, ref, payment)
	}
	body, err := cbor.Encode(map[uint]any{
		0:  []any{rebuildTestInput(0xa1)},
		1:  []any{rebuildTestOutput(0x51, 1_000_000)},
		2:  uint64(5),
		13: []any{rebuildTestInput(0xa2)},
		16: rebuildTestOutput(0x56, 2_000_000),
	})
	require.NoError(t, err)
	hash := lcommon.Blake2b256Hash(body).Bytes()
	const ebSlot, rbSlot = uint64(90), uint64(100)
	ebHash := bytes.Repeat([]byte{0xee}, 32)
	var ebHashArr [32]byte
	copy(ebHashArr[:], ebHash)
	require.NoError(t, db.SetGenesisCbor(ebSlot, ebHash, body, nil))
	blobTxn := db.BlobTxn(true)
	require.NoError(t, blobTxn.Do(func(txn *Txn) error {
		return db.Blob().SetTx(txn.Blob(), hash, EncodeTxOffset(&CborOffset{
			BlockSlot:  ebSlot,
			BlockHash:  ebHashArr,
			ByteLength: uint32(len(body)), //nolint:gosec // small test body
		}))
	}))

	for _, tc := range []struct {
		valid bool
		want  []byte
	}{
		{true, []byte{0x11, 0x22, 0x51}},
		{false, []byte{0x11, 0x22, 0x56}},
	} {
		rows, err := db.rebuildAddressTransaction(context.Background(), models.Transaction{
			Hash:      hash,
			BlockHash: bytes.Repeat([]byte{0xbb}, 32),
			Slot:      rbSlot,
			Valid:     tc.valid,
		})
		require.NoError(t, err, "valid=%v", tc.valid)
		require.Equal(t, tc.want, rebuildPayments(rows), "valid=%v", tc.valid)
	}
}
