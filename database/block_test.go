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
