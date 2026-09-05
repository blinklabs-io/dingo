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
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type countingBlockReadStore struct {
	blob.BlobStore
	getBlockCalls int
}

func (s *countingBlockReadStore) GetBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	s.getBlockCalls++
	return s.BlobStore.GetBlock(txn, slot, hash)
}

type localBlockReadStore struct {
	blob.BlobStore
	archiveFallbackCalls int
}

func (s *localBlockReadStore) GetBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	s.archiveFallbackCalls++
	return s.BlobStore.GetBlock(txn, slot, hash)
}

func (s *localBlockReadStore) GetBlockLocal(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	return s.BlobStore.GetBlock(txn, slot, hash)
}

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

func TestBlockPointByIndexDoesNotReadBlockContent(t *testing.T) {
	db := newTestDB(t)
	block := testIndexedBlock(42, 7, 0x42)
	require.NoError(t, db.BlockCreate(block, nil))

	store := &countingBlockReadStore{BlobStore: db.blob}
	db.blob = store

	point, err := db.BlockPointByIndex(block.ID, nil)
	require.NoError(t, err)
	require.Equal(t, block.Slot, point.Slot)
	require.Equal(t, block.Hash, point.Hash)
	require.Zero(
		t,
		store.getBlockCalls,
		"point-only lookup must not load block CBOR or trigger archive fallback",
	)
}

func TestBlockIDByPointLocalBypassesArchiveFallback(t *testing.T) {
	db := newTestDB(t)
	block := testIndexedBlock(42, 7, 0x42)
	require.NoError(t, db.BlockCreate(block, nil))

	store := &localBlockReadStore{BlobStore: db.blob}
	db.blob = store

	blockID, err := BlockIDByPointLocal(
		db,
		ocommon.NewPoint(block.Slot, block.Hash),
	)
	require.NoError(t, err)
	require.Equal(t, block.ID, blockID)
	require.Zero(t, store.archiveFallbackCalls)

	_, err = BlockIDByPointLocal(
		db,
		ocommon.NewPoint(block.Slot, bytes.Repeat([]byte{0xff}, 32)),
	)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
	require.Zero(t, store.archiveFallbackCalls)

	txn := db.BlobTxn(true)
	require.NoError(t, txn.Do(func(txn *Txn) error {
		return db.Blob().TombstoneBlock(
			txn.Blob(), block.Slot, block.Hash,
		)
	}))
	blockID, err = BlockIDByPointLocal(
		db,
		ocommon.NewPoint(block.Slot, block.Hash),
	)
	require.NoError(t, err)
	require.Equal(t, block.ID, blockID)
	require.Zero(t, store.archiveFallbackCalls)

	// Older cloud tombstones did not retain metadata. They cannot recover a
	// local ID and must remain a non-match instead of aliasing block ID zero.
	txn = db.BlobTxn(true)
	require.NoError(t, txn.Do(func(txn *Txn) error {
		return db.Blob().Delete(
			txn.Blob(),
			types.BlockBlobMetadataKey(
				types.BlockBlobKey(block.Slot, block.Hash),
			),
		)
	}))
	_, err = BlockIDByPointLocal(
		db,
		ocommon.NewPoint(block.Slot, block.Hash),
	)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
	require.Zero(t, store.archiveFallbackCalls)
}

func TestBlockAtOrAfterIndexSkipsSparseIndexes(t *testing.T) {
	db := newTestDB(t)
	blocks := []models.Block{
		testIndexedBlock(10, 1, 0x01),
		testIndexedBlock(20, 1_000_000, 0x02),
	}
	for _, block := range blocks {
		require.NoError(t, db.BlockCreate(block, nil))
	}

	block, err := db.BlockAtOrAfterIndex(2, nil)
	require.NoError(t, err)
	require.Equal(t, blocks[1].ID, block.ID)
	require.Equal(t, blocks[1].Hash, block.Hash)

	_, err = db.BlockAtOrAfterIndex(blocks[1].ID+1, nil)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
}

func TestBlockAtOrAfterIndexSkipsInvalidIndexMappings(t *testing.T) {
	db := newTestDB(t)
	olderBlock := testIndexedBlock(10, 1, 0x01)
	nextBlock := testIndexedBlock(30, 300, 0x03)
	require.NoError(t, db.BlockCreate(olderBlock, nil))
	require.NoError(t, db.BlockCreate(nextBlock, nil))

	txn := db.BlobTxn(true)
	require.NoError(t, txn.Do(func(txn *Txn) error {
		// This index resolves, but its target belongs to an older index.
		if err := db.Blob().Set(
			txn.Blob(),
			types.BlockBlobIndexKey(100),
			types.BlockBlobKey(olderBlock.Slot, olderBlock.Hash),
		); err != nil {
			return err
		}
		// This index points at a block that is no longer present.
		return db.Blob().Set(
			txn.Blob(),
			types.BlockBlobIndexKey(200),
			types.BlockBlobKey(20, bytes.Repeat([]byte{0x02}, 32)),
		)
	}))

	block, err := db.BlockAtOrAfterIndex(2, nil)
	require.NoError(t, err)
	require.Equal(t, nextBlock.ID, block.ID)
	require.Equal(t, nextBlock.Hash, block.Hash)
}

// TestBlockBeforeSlotSkipsSyntheticBlobs verifies BlockBeforeSlot returns the
// highest real ranking block before a slot and skips synthetic blobs. Genesis
// CBOR and Leios endorser blocks are persisted at block-blob keys via
// SetGenesisCbor with ID=0 and no chain index; returning one to the
// epoch-nonce lab computation saves a non-chain hash. Older PrevHash-based lab
// lookup also saved an empty lastEpochBlockNonce here, collapsing the new
// epoch's nonce to the NeutralNonce identity and failing leader-VRF checks.
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
		"the real block's PrevHash must survive for chain continuity",
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

// TestBlockByNumberResolvesEveryIndexedBlock pins the height-identifier
// lookup the bark archive service needs: block numbers are not indexed in
// the blob store, so the resolution is a binary search over the block-ID
// space and every number in the chain must come back as its own block.
func TestBlockByNumberResolvesEveryIndexedBlock(t *testing.T) {
	db := newTestDB(t)
	blocks := make([]models.Block, 0, 5)
	for i := uint64(1); i <= 5; i++ {
		block := testIndexedBlock(i*10, i, byte(i))
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
	}

	for _, want := range blocks {
		got, err := BlockByNumber(db, want.Number)
		require.NoError(t, err)
		require.Equal(t, want.ID, got.ID)
		require.Equal(t, want.Slot, got.Slot)
		require.True(t, bytes.Equal(want.Hash, got.Hash))
	}
}

// TestBlockByNumberSkipsSparseIndexGap proves the search tolerates gaps in
// the block-ID space, which a Mithril bootstrap or drain import leaves
// behind, rather than treating a missing probe as the end of the range. A
// target above the gap is the discriminating case: a fallback that merely
// shrinks the upper bound on a missing probe converges into the low range
// and never finds it.
func TestBlockByNumberSkipsSparseIndexGap(t *testing.T) {
	db := newTestDB(t)
	ids := []uint64{1, 2, 3, 1000, 1001, 1002}
	blocks := make([]models.Block, 0, len(ids))
	for i, id := range ids {
		// #nosec G115 -- fixed small test fixture values.
		block := testIndexedBlock(id*10, id, byte(i+1))
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
	}

	below, err := BlockByNumber(db, blocks[1].Number)
	require.NoError(t, err)
	require.Equal(t, blocks[1].ID, below.ID)

	above, err := BlockByNumber(db, blocks[4].Number)
	require.NoError(t, err)
	require.Equal(t, blocks[4].ID, above.ID)
}

// TestResolveBlockNumberBoundIsSeparableFromTheSearch pins the split that
// keeps a batch of block-number lookups from re-reading the chain tip once
// per number. Resolving the bound is a reverse iteration over the block
// index, which the s3 and gcs plugins answer by listing every block-index
// object in the bucket, so the bound is resolved by the caller and carried
// into each search.
func TestResolveBlockNumberBoundIsSeparableFromTheSearch(t *testing.T) {
	db := newTestDB(t)

	empty, err := ResolveBlockNumberBound(db)
	require.NoError(t, err)
	require.False(t, empty.Resolved, "an empty chain bounds nothing")

	blocks := make([]models.Block, 0, 5)
	for i := uint64(1); i <= 5; i++ {
		block := testIndexedBlock(i*10, i, byte(i))
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
	}

	bound, err := ResolveBlockNumberBound(db)
	require.NoError(t, err)
	require.True(t, bound.Resolved)
	require.Equal(t, blocks[4].ID, bound.HighestID)
	require.Equal(t, blocks[4].Number, bound.HighestNumber)

	// One bound answers every number in the chain.
	for _, want := range blocks {
		got, err := BlockByNumberBounded(db, want.Number, bound)
		require.NoError(t, err)
		require.Equal(t, want.ID, got.ID)
		require.Equal(t, want.Slot, got.Slot)
		require.True(t, bytes.Equal(want.Hash, got.Hash))
	}

	_, err = BlockByNumberBounded(db, blocks[4].Number+1, bound)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
}

// TestBlockByNumberBoundedRejectsUnresolvedBound pins the fail-closed zero
// value: a caller that never resolved a bound must get ErrBlockNotFound
// rather than a search over an ID space the bound says is empty, which
// would report the same thing for the wrong reason.
func TestBlockByNumberBoundedRejectsUnresolvedBound(t *testing.T) {
	db := newTestDB(t)
	for i := uint64(1); i <= 3; i++ {
		block := testIndexedBlock(i*10, i, byte(i))
		require.NoError(t, db.BlockCreate(block, nil))
	}

	_, err := BlockByNumberBounded(db, 1, BlockNumberBound{})
	require.ErrorIs(t, err, models.ErrBlockNotFound)
}

// TestBlockByNumberReportsMissingNumbersAsNotFound proves an absent height
// is reported as models.ErrBlockNotFound rather than a generic error, which
// is what lets the bark archive service classify it as a not_found
// reference instead of failing the whole batch.
func TestBlockByNumberReportsMissingNumbersAsNotFound(t *testing.T) {
	db := newTestDB(t)

	_, err := BlockByNumber(db, 1)
	require.ErrorIs(t, err, models.ErrBlockNotFound)

	for i := uint64(1); i <= 3; i++ {
		block := testIndexedBlock(i*10, i, byte(i))
		require.NoError(t, db.BlockCreate(block, nil))
	}

	_, err = BlockByNumber(db, 99)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
}
