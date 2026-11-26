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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	mockfixtures "github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

func TestIndexerMaterializesDijkstraSubtransactionOutput(t *testing.T) {
	t.Parallel()

	fixture := newIndexerTestDB(t, nil)
	const slot uint64 = 100
	blockHash := bytes.Repeat([]byte{0xbb}, 32)
	blockCbor, block := pruneDijkstraBlock(t, slot)
	decoded, err := models.DecodeBlockCbor(gledger.BlockTypeDijkstra, blockCbor)
	require.NoError(t, err)
	offsets, err := NewBlockIndexer(slot, blockHash).ComputeOffsets(
		blockCbor,
		decoded,
	)
	require.NoError(t, err)
	require.Len(t, offsets.UtxoOffsets, 2)
	var ref UtxoRef
	copy(
		ref.TxId[:],
		block.BlockBody.Transactions[0].Body.TxSubTransactions.Items()[0].Body.Id().Bytes(),
	)
	outputOffset := offsets.UtxoOffsets[ref]
	outputCbor := append([]byte(nil), blockCbor[outputOffset.ByteOffset:outputOffset.ByteOffset+outputOffset.ByteLength]...)

	ledgerTxn := fixture.ledger.BlobTxn(true)
	require.NoError(t, ledgerTxn.Do(func(txn *Txn) error {
		return fixture.ledger.Blob().SetBlock(
			txn.Blob(), slot, blockHash, blockCbor, 1,
			gledger.BlockTypeDijkstra, 1, nil,
		)
	}))
	indexerTxn := fixture.indexer.BlobTxn(true)
	require.NoError(t, indexerTxn.Do(func(txn *Txn) error {
		return fixture.indexer.Blob().SetUtxo(
			txn.Blob(), ref.TxId[:], ref.OutputIdx,
			EncodeUtxoOffset(&outputOffset),
		)
	}))

	// The offset is keyed by the Dijkstra subtransaction hash, not the parent
	// transaction hash used by an ordinary UTxO row.
	n, err := fixture.indexer.MaterializeBlockUtxos(
		context.Background(), slot, blockHash,
	)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	readTxn := fixture.indexer.BlobTxn(false)
	defer readTxn.Release()
	got, err := fixture.indexer.Blob().GetUtxo(
		readTxn.Blob(), ref.TxId[:], ref.OutputIdx,
	)
	require.NoError(t, err)
	require.Equal(t, outputCbor, got)
}

func TestIndexerMaterializesBlockOffsetsBeyondMetadataRows(t *testing.T) {
	t.Parallel()

	fixture := newIndexerTestDB(t, nil)
	const slot uint64 = 100
	block := pruneConwayBlockWithTwoOutputs(t, slot)
	blockHash := block.Hash().Bytes()
	blockCbor := block.Cbor()
	offsets, err := NewBlockIndexer(slot, blockHash).ComputeOffsets(
		blockCbor,
		block,
	)
	require.NoError(t, err)
	require.Len(t, offsets.UtxoOffsets, 2)

	refs := make([]UtxoRef, 0, len(offsets.UtxoOffsets))
	for ref := range offsets.UtxoOffsets {
		refs = append(refs, ref)
	}
	ledgerTxn := fixture.ledger.BlobTxn(true)
	require.NoError(t, ledgerTxn.Do(func(txn *Txn) error {
		return fixture.ledger.Blob().SetBlock(
			txn.Blob(), slot, blockHash, blockCbor, 1,
			gledger.BlockTypeConway, 1, nil,
		)
	}))
	indexerTxn := fixture.indexer.BlobTxn(true)
	require.NoError(t, indexerTxn.Do(func(txn *Txn) error {
		for _, ref := range refs {
			offset := offsets.UtxoOffsets[ref]
			if err := fixture.indexer.Blob().SetUtxo(
				txn.Blob(), ref.TxId[:], ref.OutputIdx,
				EncodeUtxoOffset(&offset),
			); err != nil {
				return err
			}
		}
		return nil
	}))
	metadataTxn := fixture.indexer.MetadataTxn(context.Background(), true)
	require.NoError(t, metadataTxn.Do(func(txn *Txn) error {
		ref := refs[0]
		return fixture.indexer.Metadata().CreateUtxo(
			txn.Metadata(),
			&models.Utxo{
				TxId:      ref.TxId[:],
				OutputIdx: ref.OutputIdx,
				AddedSlot: slot,
				Amount:    1,
				Address:   []byte("indexed-output"),
			},
		)
	}))
	metadataRead := fixture.indexer.MetadataTxn(context.Background(), false)
	sqlRefs, err := fixture.indexer.Metadata().GetUtxosBySlot(
		slot,
		metadataRead.Metadata(),
	)
	metadataRead.Release()
	require.NoError(t, err)
	require.Len(t, sqlRefs, 1)

	// The SQL UTxO rows expose one output, while the IndexerDB has a live offset
	// for the second output as well, modeling an output restored by rollback.
	n, err := fixture.indexer.MaterializeBlockUtxos(
		context.Background(), slot, blockHash,
	)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	readTxn := fixture.indexer.BlobTxn(false)
	defer readTxn.Release()
	for _, ref := range refs {
		got, err := fixture.indexer.Blob().GetUtxo(
			readTxn.Blob(), ref.TxId[:], ref.OutputIdx,
		)
		require.NoError(t, err)
		offset := offsets.UtxoOffsets[ref]
		want := blockCbor[offset.ByteOffset : offset.ByteOffset+offset.ByteLength]
		require.Equal(t, want, got)
	}
}

func pruneConwayBlockWithTwoOutputs(
	t *testing.T,
	slot uint64,
) gledger.Block {
	t.Helper()

	generated, err := testfixtures.GenerateConwayChainWithTransactions(2)
	require.NoError(t, err)
	transactions := []lcommon.Transaction{
		generated[0].Transactions()[0],
		generated[1].Transactions()[0],
	}
	block, err := mockfixtures.NewBlockBuilder(gledger.GetEraById(6)).
		WithBlockNumber(1).
		WithSlot(slot).
		WithTransactions(transactions...).
		Build()
	require.NoError(t, err)
	return block
}

func pruneDijkstraBlock(
	t *testing.T,
	slot uint64,
) ([]byte, *dijkstra.DijkstraBlock) {
	t.Helper()

	childBody, err := cbor.Encode(map[uint]any{
		0: []any{rebuildTestInput(0xa1)},
		1: []any{map[uint]any{
			0: append([]byte{0x60}, bytes.Repeat([]byte{0x51}, 28)...),
			1: uint64(1_000_000),
		}},
	})
	require.NoError(t, err)
	childCbor, err := cbor.Encode(
		[]any{cbor.RawMessage(childBody), map[uint]any{}, nil},
	)
	require.NoError(t, err)
	transactionCbor, err := cbor.Encode([]any{
		cbor.RawMessage(mapBytes(t, map[uint]any{
			0: []any{rebuildTestInput(0xa2)},
			1: []any{rebuildTestOutput(0x52, 2_000_000)},
			2: uint64(5),
			23: cbor.NewSetType(
				[]cbor.RawMessage{childCbor},
				true,
			),
		})),
		map[uint]any{}, true, nil,
	})
	require.NoError(t, err)
	transaction, err := dijkstra.NewDijkstraTransactionFromCbor(transactionCbor)
	require.NoError(t, err)

	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: 1,
					Slot:        slot,
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
		BlockBody: dijkstra.DijkstraBlockBody{
			Transactions: []dijkstra.DijkstraTransaction{*transaction},
		},
	}
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodyHash = block.BlockBody.Hash()
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	return blockCbor, block
}

func mapBytes(t *testing.T, value map[uint]any) []byte {
	t.Helper()
	encoded, err := cbor.Encode(value)
	require.NoError(t, err)
	return encoded
}
