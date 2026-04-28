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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// seedPrunableBlock writes a synthetic block plus offset-based blob entries
// for each (utxoIdx, payload) pair. The block CBOR is the concatenation of
// payloads in order, so each UTxO's offset within the block is the prefix
// sum of preceding payload lengths. Returns the block hash and the resolved
// offsets keyed by output index.
func seedPrunableBlock(
	t *testing.T,
	db *Database,
	slot uint64,
	txId []byte,
	payloads map[uint32][]byte,
) (blockHash []byte, blockCbor []byte) {
	t.Helper()
	blockHash = randomHash(t)

	// Concatenate payloads into the block CBOR in deterministic output-idx
	// order, recording each payload's offset.
	type entry struct {
		idx     uint32
		payload []byte
		offset  uint32
	}
	var entries []entry
	// Ordered iteration: idxs 0..N
	maxIdx := uint32(0)
	for idx := range payloads {
		if idx > maxIdx {
			maxIdx = idx
		}
	}
	var buf bytes.Buffer
	for i := uint32(0); i <= maxIdx; i++ {
		payload, ok := payloads[i]
		if !ok {
			continue
		}
		off := uint32(buf.Len()) //nolint:gosec
		buf.Write(payload)
		entries = append(entries, entry{idx: i, payload: payload, offset: off})
	}
	blockCbor = buf.Bytes()

	// Persist the block.
	insertTestBlock(t, db, slot, blockHash, blockCbor)

	// Persist each UTxO's blob entry as a CborOffset reference.
	blobTxn := db.BlobTxn(true)
	require.NoError(t, blobTxn.Do(func(txn *Txn) error {
		var blockHashArr [32]byte
		copy(blockHashArr[:], blockHash)
		for _, e := range entries {
			off := &CborOffset{
				BlockSlot:  slot,
				BlockHash:  blockHashArr,
				ByteOffset: e.offset,
				ByteLength: uint32(len(e.payload)), //nolint:gosec
			}
			if err := db.Blob().SetUtxo(
				txn.Blob(), txId, e.idx, EncodeUtxoOffset(off),
			); err != nil {
				return err
			}
		}
		return nil
	}))
	return blockHash, blockCbor
}

// seedUtxoMetadata inserts a UTxO row with the given added/deleted slot.
func seedUtxoMetadata(
	t *testing.T,
	db *Database,
	txId []byte,
	outputIdx uint32,
	addedSlot, deletedSlot uint64,
) {
	t.Helper()
	mdTxn := db.MetadataTxn(true)
	require.NoError(t, mdTxn.Do(func(txn *Txn) error {
		return db.Metadata().CreateUtxo(txn.Metadata(), &models.Utxo{
			TxId:        txId,
			OutputIdx:   outputIdx,
			AddedSlot:   addedSlot,
			DeletedSlot: deletedSlot,
			Amount:      types.Uint64(1),
		})
	}))
}

func TestPruneBlock_MaterializesLiveUtxoAndDeletesBlock(t *testing.T) {
	db := newTestDB(t)

	const slot uint64 = 100
	txId := bytes.Repeat([]byte{0xAA}, 32)
	livePayload := []byte("LIVE-utxo-cbor-payload")
	spentPayload := []byte("spent-utxo-cbor-payload")

	hash, blockCbor := seedPrunableBlock(t, db, slot, txId, map[uint32][]byte{
		0: livePayload,
		1: spentPayload,
	})

	// Live UTxO at slot 100; spent UTxO at slot 100, consumed at slot 150
	// (still alive at the prune call so we can test it doesn't appear in
	// GetLiveUtxosBySlot).
	seedUtxoMetadata(t, db, txId, 0, slot, 0)
	seedUtxoMetadata(t, db, txId, 1, slot, 150)

	// Sanity: pre-prune both UTxO blob entries are offset-encoded and the
	// block is retrievable.
	blobTxn := db.BlobTxn(false)
	preBytes0, err := db.Blob().GetUtxo(blobTxn.Blob(), txId, 0)
	require.NoError(t, err)
	require.True(t, IsUtxoOffsetStorage(preBytes0))
	preBytes1, err := db.Blob().GetUtxo(blobTxn.Blob(), txId, 1)
	require.NoError(t, err)
	require.True(t, IsUtxoOffsetStorage(preBytes1))
	preBlock, _, err := db.Blob().GetBlock(blobTxn.Blob(), slot, hash)
	require.NoError(t, err)
	assert.Equal(t, blockCbor, preBlock)
	blobTxn.Release()

	// Prune the block.
	n, err := db.PruneBlock(slot, hash)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "exactly one live UTxO should be materialized")

	// Block CBOR is gone.
	blobTxn = db.BlobTxn(false)
	defer blobTxn.Release()
	_, _, err = db.Blob().GetBlock(blobTxn.Blob(), slot, hash)
	assert.ErrorIs(t, err, types.ErrBlobKeyNotFound)

	// Live UTxO entry is now raw CBOR matching the in-block slice.
	postLive, err := db.Blob().GetUtxo(blobTxn.Blob(), txId, 0)
	require.NoError(t, err)
	assert.False(
		t, IsUtxoOffsetStorage(postLive),
		"live UTxO blob entry must no longer be offset-encoded",
	)
	assert.Equal(t, livePayload, postLive)

	// Spent UTxO entry is untouched: GetLiveUtxosBySlot did not include
	// it, so PruneBlock left it as offset storage. (In a real run, the
	// blob entry would have been deleted at consume time; here we kept
	// it to verify materialization is gated on the metadata query.)
	postSpent, err := db.Blob().GetUtxo(blobTxn.Blob(), txId, 1)
	require.NoError(t, err)
	assert.True(
		t, IsUtxoOffsetStorage(postSpent),
		"spent UTxO blob entry should be left untouched by PruneBlock",
	)
}

func TestPruneBlock_ResolverReadsMaterializedUtxoAfterPrune(t *testing.T) {
	db := newTestDB(t)

	const slot uint64 = 100
	txId := bytes.Repeat([]byte{0xBB}, 32)
	payload := []byte("post-prune-resolution-payload")

	hash, _ := seedPrunableBlock(t, db, slot, txId, map[uint32][]byte{
		0: payload,
	})
	seedUtxoMetadata(t, db, txId, 0, slot, 0)

	_, err := db.PruneBlock(slot, hash)
	require.NoError(t, err)

	// Snapshot cold-extraction count to prove the resolver does NOT have to
	// fetch the (now-missing) block.
	pre := db.CborCache().Metrics().ColdExtractions.Load()

	cbor, err := db.CborCache().ResolveUtxoCbor(txId, 0)
	require.NoError(t, err)
	assert.Equal(t, payload, cbor)

	post := db.CborCache().Metrics().ColdExtractions.Load()
	assert.Equal(
		t, pre, post,
		"resolver should not perform a cold block extraction for a "+
			"materialized UTxO",
	)
}

func TestPruneBlock_SkipsAlreadyMaterializedUtxo(t *testing.T) {
	db := newTestDB(t)

	const slot uint64 = 100
	txId := bytes.Repeat([]byte{0xCC}, 32)
	payload := []byte("already-raw-payload")

	hash, _ := seedPrunableBlock(t, db, slot, txId, map[uint32][]byte{
		0: payload,
	})
	// Overwrite the offset entry with raw CBOR before prune to simulate a
	// UTxO that was previously materialized (or stored by a legacy code
	// path).
	rawWriteTxn := db.BlobTxn(true)
	require.NoError(t, rawWriteTxn.Do(func(txn *Txn) error {
		return db.Blob().SetUtxo(txn.Blob(), txId, 0, payload)
	}))
	seedUtxoMetadata(t, db, txId, 0, slot, 0)

	n, err := db.PruneBlock(slot, hash)
	require.NoError(t, err)
	assert.Equal(t, 0, n,
		"already-raw UTxO should not be counted as materialized")
}
