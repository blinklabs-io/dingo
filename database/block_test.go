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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

func testIndexedBlock(slot, id uint64, hashByte byte) models.Block {
	return models.Block{
		ID:     id,
		Slot:   slot,
		Hash:   bytes.Repeat([]byte{hashByte}, 32),
		Cbor:   []byte{0x80},
		Number: id,
		Type:   1,
	}
}

func TestBlockBySlotReturnsHighestIndexedBlockForSlot(t *testing.T) {
	db := newTestDB(t)
	const slot = uint64(42)

	lowerIDBlock := testIndexedBlock(slot, 10, 0x10)
	higherIDBlock := testIndexedBlock(slot, 11, 0x11)
	require.NoError(t, db.BlockCreate(lowerIDBlock, nil))
	require.NoError(t, db.BlockCreate(higherIDBlock, nil))

	block, err := BlockBySlot(db, slot)
	require.NoError(t, err)
	require.Equal(t, higherIDBlock.ID, block.ID)
	require.Equal(t, higherIDBlock.Hash, block.Hash)
}

func TestBlockBySlotSkipsStaleSameSlotIndex(t *testing.T) {
	db := newTestDB(t)
	const slot = uint64(42)

	lowerIDBlock := testIndexedBlock(slot, 10, 0x10)
	higherIDBlock := testIndexedBlock(slot, 11, 0x11)
	require.NoError(t, db.BlockCreate(lowerIDBlock, nil))
	require.NoError(t, db.BlockCreate(higherIDBlock, nil))

	txn := db.BlobTxn(true)
	require.NoError(t, txn.Do(func(txn *Txn) error {
		return db.Blob().Set(
			txn.Blob(),
			types.BlockBlobIndexKey(higherIDBlock.ID),
			types.BlockBlobKey(lowerIDBlock.Slot, lowerIDBlock.Hash),
		)
	}))

	block, err := BlockBySlot(db, slot)
	require.NoError(t, err)
	require.Equal(t, lowerIDBlock.ID, block.ID)
	require.Equal(t, lowerIDBlock.Hash, block.Hash)
}

// TestBlockBeforeSlotSkipsSyntheticBlobs verifies BlockBeforeSlot returns the
// highest real ranking block before a slot and skips synthetic blobs. Genesis
// CBOR and Leios endorser blocks are persisted at block-blob keys via
// SetGenesisCbor with ID=0 and an empty PrevHash; returning one to the
// epoch-nonce lab computation saves an empty lastEpochBlockNonce, which
// collapses the next epoch's nonce to the NeutralNonce identity and fails every
// leader-VRF check (the Dijkstra/Leios at-tip wedge).
func TestBlockBeforeSlotSkipsSyntheticBlobs(t *testing.T) {
	db := newTestDB(t)

	realBlock := models.Block{
		ID:       7,
		Slot:     100,
		Hash:     bytes.Repeat([]byte{0xaa}, 32),
		PrevHash: bytes.Repeat([]byte{0xbb}, 32),
		Cbor:     []byte{0x80},
		Number:   7,
		Type:     6,
	}
	require.NoError(t, db.BlockCreate(realBlock, nil))

	// Synthetic endorser-block blob at a HIGHER slot than the real block but
	// still before the query slot. SetGenesisCbor stores it with ID=0 and a
	// nil PrevHash.
	ebHash := bytes.Repeat([]byte{0xcc}, 32)
	require.NoError(t, db.SetGenesisCbor(110, ebHash, []byte{0x80}, nil))

	got, err := BlockBeforeSlot(db, 120)
	require.NoError(t, err)
	require.Equal(
		t,
		realBlock.Slot,
		got.Slot,
		"BlockBeforeSlot must skip the synthetic blob at slot 110 and return "+
			"the real ranking block at slot 100",
	)
	require.Equal(t, realBlock.Hash, got.Hash)
	require.Equal(
		t,
		realBlock.PrevHash,
		got.PrevHash,
		"the real block's PrevHash must survive — it feeds the epoch-nonce lab",
	)
}

// TestBlockBeforeSlotSyntheticOnlyNotFound verifies that when only synthetic
// blobs precede the slot (no real ranking block), BlockBeforeSlot reports
// ErrBlockNotFound rather than returning a synthetic blob.
func TestBlockBeforeSlotSyntheticOnlyNotFound(t *testing.T) {
	db := newTestDB(t)

	ebHash := bytes.Repeat([]byte{0xcc}, 32)
	require.NoError(t, db.SetGenesisCbor(110, ebHash, []byte{0x80}, nil))

	_, err := BlockBeforeSlot(db, 120)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
}
