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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// commitFailingBlobStore wraps a real blob store and hands out read-write
// transactions whose Commit always fails. It is the only way to reach
// AddBlocks' commit-failure path: the closure passed to txn.Do runs to
// completion and returns nil, and txn.Do only then calls Commit -- after the
// chain locks the closure held have been released.
type commitFailingBlobStore struct {
	blob.BlobStore
	err error
}

func (s commitFailingBlobStore) NewTransaction(readWrite bool) dbtypes.Txn {
	txn := s.BlobStore.NewTransaction(readWrite)
	if !readWrite {
		return txn
	}
	return &commitFailingBlobTxn{Txn: txn, err: s.err}
}

// SetBlock is the only store call AddBlocks makes with the wrapped
// transaction, so it is the only one that has to unwrap before delegating.
func (s commitFailingBlobStore) SetBlock(
	txn dbtypes.Txn,
	slot uint64,
	hash []byte,
	cbor []byte,
	id uint64,
	blockType uint,
	height uint64,
	prevHash []byte,
) error {
	return s.BlobStore.SetBlock(
		unwrapCommitFailingBlobTxn(txn),
		slot,
		hash,
		cbor,
		id,
		blockType,
		height,
		prevHash,
	)
}

func (s commitFailingBlobStore) SetCommitTimestamp(
	timestamp int64,
	txn dbtypes.Txn,
) error {
	return s.BlobStore.SetCommitTimestamp(
		timestamp,
		unwrapCommitFailingBlobTxn(txn),
	)
}

type commitFailingBlobTxn struct {
	dbtypes.Txn
	err error
}

func (t *commitFailingBlobTxn) Commit() error {
	_ = t.Txn.Rollback()
	return t.err
}

func unwrapCommitFailingBlobTxn(txn dbtypes.Txn) dbtypes.Txn {
	if wrapped, ok := txn.(*commitFailingBlobTxn); ok {
		return wrapped.Txn
	}
	return txn
}

func pointOfBlock(b ledger.Block) ocommon.Point {
	return ocommon.NewPoint(b.SlotNumber(), b.Hash().Bytes())
}

// TestAddBlocksRestoresChainStateWhenBatchFails covers the batch-failure half
// of the staged-commit contract: addBlockLocked advances c.currentTip /
// c.tipBlockIndex for every block it accepts, but those mutations only become
// durable when txn.Do commits. When a later block in the batch is rejected the
// transaction is rolled back, so every block the batch already wrote is gone
// from the database -- and the in-memory tip must go back with it, or the chain
// reports a tip whose block it does not store and splices every later block
// onto an absent parent.
//
// AddRawBlocks already snapshots and restores here; AddBlocks did not.
func TestAddBlocksRestoresChainStateWhenBatchFails(t *testing.T) {
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	c := cm.PrimaryChain()
	require.NotNil(t, c)

	var origin common.Blake2b256
	blocks := generateTestChain(t, 1, origin, 20, 20, 4)
	require.Len(t, blocks, 4)

	// Seed the chain so the failing batch has a real tip to be restored to.
	require.NoError(t, c.AddBlocks(blocks[:1]))
	tipBefore := c.Tip()
	require.Equal(t, blocks[0].SlotNumber(), tipBefore.Point.Slot)

	// blocks[1] fits the tip and is written; blocks[3] does not (its parent,
	// blocks[2], was never added), so the closure fails and txn.Do rolls the
	// whole batch back.
	err = c.AddBlocks([]ledger.Block{blocks[1], blocks[3]})
	require.Error(t, err)

	require.Equal(
		t,
		tipBefore,
		c.Tip(),
		"a rolled-back batch must leave the in-memory tip where it was; "+
			"the blocks it wrote are no longer in the database",
	)

	// The rolled-back block must not be reachable, and the restored chain
	// must still accept the continuation it was left expecting.
	_, err = database.BlockByPoint(db, pointOfBlock(blocks[1]))
	require.Error(
		t,
		err,
		"the failed batch's blocks must not survive in the database",
	)
	require.NoError(t, c.AddBlocks(blocks[1:3]))
	require.Equal(t, blocks[2].SlotNumber(), c.Tip().Point.Slot)
}

// TestAddBlocksRestoresChainStateWhenCommitFails covers the commit-failure
// half. The closure returns nil and releases the chain locks; txn.Do then calls
// Commit, which fails. Nothing the batch wrote is durable, so the in-memory tip
// must not stay advanced.
func TestAddBlocksRestoresChainStateWhenCommitFails(t *testing.T) {
	base := newTestDB(t)
	commitErr := errors.New("injected blob commit failure")
	db, err := database.New(
		base.Config(),
		database.Stores{
			Blob: commitFailingBlobStore{
				BlobStore: base.Blob(),
				err:       commitErr,
			},
			Metadata: base.Metadata(),
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	c := cm.PrimaryChain()
	require.NotNil(t, c)

	tipBefore := c.Tip()

	var origin common.Blake2b256
	blocks := generateTestChain(t, 1, origin, 20, 20, 2)
	err = c.AddBlocks(blocks)
	require.ErrorIs(t, err, commitErr)

	require.Equal(
		t,
		tipBefore,
		c.Tip(),
		"a batch whose commit failed must not leave the in-memory tip "+
			"advanced past the last durable block",
	)
}
