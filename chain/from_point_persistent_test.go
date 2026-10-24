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

package chain_test

import (
	"bytes"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// localProbeBlobStore stands in for a cloud blob store wrapper: GetBlock is
// the read that may fall through to a remote archive, and GetBlockLocal is
// the bounded local probe. Hooks let a test mutate the chain inside the
// window FromPoint resolves a point without holding the chain lock.
type localProbeBlobStore struct {
	blob.BlobStore
	getBlockCalls *atomic.Int64
	localCalls    *atomic.Int64
	// afterLocal runs once, after the next local probe returns.
	afterLocal *atomic.Pointer[func()]
	// afterIndexGet runs once, after the next read of indexKey returns.
	indexKey      []byte
	afterIndexGet *atomic.Pointer[func()]
}

func newLocalProbeBlobStore(base blob.BlobStore) localProbeBlobStore {
	return localProbeBlobStore{
		BlobStore:     base,
		getBlockCalls: &atomic.Int64{},
		localCalls:    &atomic.Int64{},
		afterLocal:    &atomic.Pointer[func()]{},
		afterIndexGet: &atomic.Pointer[func()]{},
	}
}

func (s localProbeBlobStore) GetBlock(
	txn dbtypes.Txn,
	slot uint64,
	hash []byte,
) ([]byte, dbtypes.BlockMetadata, error) {
	s.getBlockCalls.Add(1)
	return s.BlobStore.GetBlock(txn, slot, hash)
}

func (s localProbeBlobStore) GetBlockLocal(
	txn dbtypes.Txn,
	slot uint64,
	hash []byte,
) ([]byte, dbtypes.BlockMetadata, error) {
	s.localCalls.Add(1)
	cbor, metadata, err := s.BlobStore.GetBlock(txn, slot, hash)
	if hook := s.afterLocal.Swap(nil); hook != nil {
		(*hook)()
	}
	return cbor, metadata, err
}

func (s localProbeBlobStore) Get(txn dbtypes.Txn, key []byte) ([]byte, error) {
	val, err := s.BlobStore.Get(txn, key)
	if s.indexKey != nil && bytes.Equal(key, s.indexKey) {
		if hook := s.afterIndexGet.Swap(nil); hook != nil {
			(*hook)()
		}
	}
	return val, err
}

func newLocalProbeDB(
	t *testing.T,
	indexKey []byte,
) (*database.Database, localProbeBlobStore) {
	t.Helper()
	base := newTestDB(t)
	store := newLocalProbeBlobStore(base.Blob())
	store.indexKey = indexKey
	db, err := database.New(t.Context(), base.Config(), database.Stores{
		Blob:     store,
		Metadata: base.Metadata(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db, store
}

func syntheticBlock(id uint64, slot uint64, hashByte byte) models.Block {
	return models.Block{
		ID:     id,
		Slot:   slot,
		Hash:   bytes.Repeat([]byte{hashByte}, 32),
		Number: id,
		Type:   1,
		Cbor:   []byte{0x80},
	}
}

func syntheticPoint(b models.Block) ocommon.Point {
	return ocommon.NewPoint(b.Slot, b.Hash)
}

// A point whose block entry sits at an index past the chain tip is not on the
// chain, even when its index entry still names it.
func TestPersistentFromPointRejectsIndexPastTip(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	b1 := syntheticBlock(1, 10, 0x01)
	b2 := syntheticBlock(2, 20, 0x02)
	require.NoError(t, db.BlockCreate(b1, nil))
	require.NoError(t, db.BlockCreate(b2, nil))
	cm, err := chain.NewManager(t.Context(), db, nil)
	require.NoError(t, err)
	c := cm.PrimaryChain()
	require.Equal(t, b2.Slot, c.Tip().Point.Slot)

	// Stored without the chain adopting it, as a rolled-back block whose
	// entries outlive the tip move.
	b3 := syntheticBlock(3, 30, 0x03)
	require.NoError(t, db.BlockCreate(b3, nil))

	iter, err := c.FromPoint(syntheticPoint(b3), true)
	if iter != nil {
		iter.Cancel()
	}
	require.ErrorIs(t, err, models.ErrBlockNotFound)
}

// A point whose block metadata names an index now held by a different block
// was rolled back and replaced; it must not start an iterator at that index.
func TestPersistentFromPointRejectsReplacedIndexEntry(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	stale := syntheticBlock(3, 30, 0x03)
	active := syntheticBlock(3, 31, 0x33)
	for _, b := range []models.Block{
		syntheticBlock(1, 10, 0x01),
		syntheticBlock(2, 20, 0x02),
		stale,
		active,
	} {
		require.NoError(t, db.BlockCreate(b, nil))
	}
	cm, err := chain.NewManager(t.Context(), db, nil)
	require.NoError(t, err)
	c := cm.PrimaryChain()
	require.Equal(t, active.Slot, c.Tip().Point.Slot)

	iter, err := c.FromPoint(syntheticPoint(stale), true)
	if iter != nil {
		iter.Cancel()
	}
	require.ErrorIs(t, err, models.ErrBlockNotFound)
}

// An absent point is answered from the local probe; it must never reach the
// block read that can fall through to a remote archive.
func TestPersistentFromPointMissingPointStaysLocal(t *testing.T) {
	t.Parallel()
	db, store := newLocalProbeDB(t, nil)
	require.NoError(t, db.BlockCreate(syntheticBlock(1, 10, 0x01), nil))
	require.NoError(t, db.BlockCreate(syntheticBlock(2, 20, 0x02), nil))
	cm, err := chain.NewManager(t.Context(), db, nil)
	require.NoError(t, err)
	c := cm.PrimaryChain()

	store.getBlockCalls.Store(0)
	store.localCalls.Store(0)
	iter, err := c.FromPoint(
		syntheticPoint(syntheticBlock(2, 25, 0x25)),
		true,
	)
	if iter != nil {
		iter.Cancel()
	}
	require.ErrorIs(t, err, models.ErrBlockNotFound)
	require.Equal(t, int64(1), store.localCalls.Load(),
		"missing point must be answered by the local probe")
	require.Zero(t, store.getBlockCalls.Load(),
		"missing point reached a block read that may use an archive")
}

// A chain mutation between the unlocked resolve and taking the lock forces
// the resolve to run again against the mutated chain.
func TestPersistentFromPointRetriesAfterConcurrentMutation(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, store := newLocalProbeDB(t, nil)
	cm, err := chain.NewManager(ctx, db, nil)
	require.NoError(t, err)
	mustSetLedger(t, cm, 5)
	c := cm.PrimaryChain()
	var origin common.Blake2b256
	blocks := generateTestChain(t, 1, origin, 20, 20, 3)
	require.NoError(t, c.AddBlocks(ctx, blocks[:2]))

	hook := func() {
		require.NoError(t, c.AddBlock(ctx, blocks[2], nil))
	}
	store.localCalls.Store(0)
	store.afterLocal.Store(&hook)
	iter, err := c.FromPoint(pointOfBlock(blocks[1]), true)
	require.NoError(t, err)
	t.Cleanup(iter.Cancel)
	require.Nil(t, store.afterLocal.Load(), "mutation hook did not run")
	require.Equal(t, int64(2), store.localCalls.Load(),
		"a mutation inside the unlocked resolve must force a retry")
	next, err := iter.Next(false)
	require.NoError(t, err)
	require.Equal(t, blocks[1].Hash().Bytes(), next.Block.Hash)
}

// A rollback and replacement that lands after the index entry was checked
// leaves a resolved index that now names a different block. The generation
// recheck must catch it rather than start an iterator on the new block.
func TestPersistentFromPointRejectsReplacementAfterIndexCheck(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var origin common.Blake2b256
	blocks := generateTestChain(t, 1, origin, 20, 20, 2)
	fork := generateTestChain(t, 2, blocks[0].Hash(), 30, 20, 1)
	// The primary chain assigns block index 1 to the first block.
	db, store := newLocalProbeDB(t, dbtypes.BlockBlobIndexKey(2))
	cm, err := chain.NewManager(ctx, db, nil)
	require.NoError(t, err)
	mustSetLedger(t, cm, 5)
	c := cm.PrimaryChain()
	require.NoError(t, c.AddBlocks(ctx, blocks))

	hook := func() {
		require.NoError(t, c.Rollback(ctx, pointOfBlock(blocks[0])))
		require.NoError(t, c.AddBlock(ctx, fork[0], nil))
	}
	store.afterIndexGet.Store(&hook)
	iter, err := c.FromPoint(pointOfBlock(blocks[1]), true)
	if iter != nil {
		iter.Cancel()
	}
	require.Nil(t, store.afterIndexGet.Load(), "replacement hook did not run")
	require.ErrorIs(t, err, models.ErrBlockNotFound)
}
