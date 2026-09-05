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
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
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
	// armed selects which read-write transaction fails: the next one opened
	// after the test sets it. Arming explicitly lets a test seed the chain
	// first, and lets a mutation injected by beforeFail commit for real.
	armed *atomic.Bool
	// beforeFail runs inside Commit, after the batch closure's deferred
	// unlocks have released the chain locks and before the injected error
	// is returned -- exactly the window the outer restore has to survive.
	beforeFail func()
}

func (s commitFailingBlobStore) NewTransaction(readWrite bool) dbtypes.Txn {
	txn := s.BlobStore.NewTransaction(readWrite)
	if !readWrite {
		return txn
	}
	if s.armed == nil || !s.armed.CompareAndSwap(true, false) {
		return txn
	}
	return &commitFailingBlobTxn{
		Txn:        txn,
		err:        s.err,
		beforeFail: s.beforeFail,
	}
}

// SetBlock is the only store call reached with the wrapped transaction, so it
// is the only override needed. AddBlocks runs on a blob-only transaction
// (Database.BlobTxn), and Txn.Commit updates the commit timestamp -- the other
// call that would receive this transaction -- only when a metadata transaction
// is present too, so Blob().SetCommitTimestamp is never reached from here.
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

type commitFailingBlobTxn struct {
	dbtypes.Txn
	err        error
	beforeFail func()
}

func (t *commitFailingBlobTxn) Commit() error {
	_ = t.Txn.Rollback()
	if t.beforeFail != nil {
		t.beforeFail()
	}
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
	armed := &atomic.Bool{}
	db, err := database.New(
		base.Config(),
		database.Stores{
			Blob: commitFailingBlobStore{
				BlobStore: base.Blob(),
				err:       commitErr,
				armed:     armed,
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
	armed.Store(true)
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

// rollbackObservingBlobStore records the chain tip at the moment txn.Do rolls
// the transaction back after a closure error. That call happens after the
// closure's deferred unlocks have released the chain locks and before txn.Do
// returns to the batch function, so it is the exact instant at which a reader
// could observe a tip the batch has already abandoned.
type rollbackObservingBlobStore struct {
	blob.BlobStore
	observe func()
}

func (s rollbackObservingBlobStore) NewTransaction(readWrite bool) dbtypes.Txn {
	txn := s.BlobStore.NewTransaction(readWrite)
	if !readWrite {
		return txn
	}
	return &rollbackObservingBlobTxn{Txn: txn, observe: s.observe}
}

// SetBlock unwraps for the same reason commitFailingBlobStore.SetBlock does.
func (s rollbackObservingBlobStore) SetBlock(
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
		unwrapRollbackObservingBlobTxn(txn),
		slot,
		hash,
		cbor,
		id,
		blockType,
		height,
		prevHash,
	)
}

type rollbackObservingBlobTxn struct {
	dbtypes.Txn
	observe func()
}

func (t *rollbackObservingBlobTxn) Rollback() error {
	if t.observe != nil {
		t.observe()
	}
	return t.Txn.Rollback()
}

func unwrapRollbackObservingBlobTxn(txn dbtypes.Txn) dbtypes.Txn {
	if wrapped, ok := txn.(*rollbackObservingBlobTxn); ok {
		return wrapped.Txn
	}
	return txn
}

// TestAddBlocksRestoresChainStateInsideClosureOnBatchFailure pins *where* the
// batch-failure restore happens, not just that it happens. The closure holds
// c.mutex and c.manager.mutex for the whole batch; restoring after txn.Do
// returns means the deferred unlocks publish the rejected batch's tip first and
// the restore has to take the locks back, so a reader in that window sees a tip
// whose blocks the transaction is discarding, and the restore itself can land
// on top of a mutation that got in. addRawBlocks restores inside the closure;
// AddBlocks now does too.
func TestAddBlocksRestoresChainStateInsideClosureOnBatchFailure(t *testing.T) {
	base := newTestDB(t)
	var (
		c           *chain.Chain
		observed    ochainsync.Tip
		observedSet bool
	)
	db, err := database.New(
		base.Config(),
		database.Stores{
			Blob: rollbackObservingBlobStore{
				BlobStore: base.Blob(),
				observe: func() {
					if c == nil || observedSet {
						return
					}
					observed = c.Tip()
					observedSet = true
				},
			},
			Metadata: base.Metadata(),
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	c = cm.PrimaryChain()
	require.NotNil(t, c)

	var origin common.Blake2b256
	blocks := generateTestChain(t, 1, origin, 20, 20, 4)
	require.Len(t, blocks, 4)

	require.NoError(t, c.AddBlocks(blocks[:1]))
	tipBefore := c.Tip()

	// blocks[1] is accepted and advances the tip; blocks[3] does not fit, so
	// the closure fails and txn.Do rolls back -- calling the observer.
	require.Error(t, c.AddBlocks([]ledger.Block{blocks[1], blocks[3]}))

	require.True(
		t,
		observedSet,
		"the rollback observer must have run; without it the test proves "+
			"nothing",
	)
	require.Equal(
		t,
		tipBefore,
		observed,
		"the rejected batch's tip was still published when txn.Do rolled "+
			"back, so the restore ran after the closure released the "+
			"chain locks instead of under them",
	)
	require.Equal(t, tipBefore, c.Tip())
}

// TestAddBlocksReportsConcurrentMutationWhenCommitFails covers the one restore
// that cannot be made atomic: txn.Do calls Commit after the closure's deferred
// unlocks have run, so the commit-failure restore must re-take c.mutex, and a
// chain mutation can land in between. Restoring over it would roll the
// in-memory chain back past a durable commit -- behind storage rather than
// level with it -- so the mismatch is reported instead of silently applied.
func TestAddBlocksReportsConcurrentMutationWhenCommitFails(t *testing.T) {
	base := newTestDB(t)
	commitErr := errors.New("injected blob commit failure")

	armed := &atomic.Bool{}
	var (
		c         *chain.Chain
		interject func()
	)
	db, err := database.New(
		base.Config(),
		database.Stores{
			Blob: commitFailingBlobStore{
				BlobStore: base.Blob(),
				err:       commitErr,
				armed:     armed,
				beforeFail: func() {
					if interject != nil {
						interject()
					}
				},
			},
			Metadata: base.Metadata(),
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	c = cm.PrimaryChain()
	require.NotNil(t, c)

	var origin common.Blake2b256
	blocks := generateTestChain(t, 1, origin, 20, 20, 3)
	require.Len(t, blocks, 3)

	// The failing batch is blocks[:2]. While its Commit is failing -- chain
	// locks free, exactly where a concurrent mutation fits -- add blocks[2]
	// on top of the tip the batch published. That add opens its own
	// transaction, which the one-shot store lets commit for real.
	var (
		interjected    bool
		interjectedErr error
	)
	interject = func() {
		if interjected {
			return
		}
		interjected = true
		interjectedErr = c.AddBlocks(blocks[2:])
	}

	armed.Store(true)
	err = c.AddBlocks(blocks[:2])
	require.True(t, interjected, "the interjected mutation must have run")
	require.NoError(
		t,
		interjectedErr,
		"the interjected mutation must have committed; otherwise there is "+
			"nothing for the restore to clobber",
	)
	require.ErrorIs(t, err, commitErr, "the commit failure must be reported")
	require.ErrorIs(
		t,
		err,
		chain.ErrChainStateChangedDuringCommit,
		"a commit-failure restore that would overwrite a later chain "+
			"mutation must be reported, not applied",
	)
	require.Equal(
		t,
		blocks[2].SlotNumber(),
		c.Tip().Point.Slot,
		"the later mutation's tip must survive; rolling it back would put "+
			"the in-memory chain behind durable storage",
	)
}

// TestAddRawBlocksRestoresChainStateWhenCommitFails pins the sibling path. Both
// batch functions share chainStateSnapshot and restoreAfterCommitFailure now,
// which is what keeps them from drifting apart again -- this PR exists because
// the staging was added to addRawBlocks alone and AddBlocks kept advancing its
// tip past a rolled-back batch.
func TestAddRawBlocksRestoresChainStateWhenCommitFails(t *testing.T) {
	base := newTestDB(t)
	commitErr := errors.New("injected blob commit failure")
	armed := &atomic.Bool{}
	db, err := database.New(
		base.Config(),
		database.Stores{
			Blob: commitFailingBlobStore{
				BlobStore: base.Blob(),
				err:       commitErr,
				armed:     armed,
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
	rawBlocks := make([]chain.RawBlock, 0, len(blocks))
	for _, b := range blocks {
		rawBlocks = append(rawBlocks, chain.RawBlock{
			Slot:        b.SlotNumber(),
			Hash:        b.Hash().Bytes(),
			BlockNumber: b.BlockNumber(),
			Type:        uint(b.Type()),
			PrevHash:    b.PrevHash().Bytes(),
			Cbor:        b.Cbor(),
		})
	}

	armed.Store(true)
	err = c.AddRawBlocks(rawBlocks)
	require.ErrorIs(t, err, commitErr)
	require.Equal(
		t,
		tipBefore,
		c.Tip(),
		"a raw batch whose commit failed must not leave the in-memory tip "+
			"advanced past the last durable block",
	)
}

// TestAddBlocksReportsIndistinguishableMutationWhenCommitFails covers the
// intervening mutation the staged fields cannot show. Comparing tip, index and
// queue lengths answers "does the chain still look like what the batch
// published", not "has anything happened since" -- and the two differ. A retry
// that re-commits the same blocks reproduces every staged field exactly while
// its writes are durable and the failed batch's are not, so the comparison
// matches and the restore rewinds past a committed mutation.
//
// ClearHeaders on an already-empty queue is the same case reduced to one call:
// a real, reachable mutation (the active peer changed) that leaves every staged
// field identical. Only chain.mutationSeq distinguishes it.
func TestAddBlocksReportsIndistinguishableMutationWhenCommitFails(t *testing.T) {
	base := newTestDB(t)
	commitErr := errors.New("injected blob commit failure")
	armed := &atomic.Bool{}
	var (
		c         *chain.Chain
		interject func()
	)
	db, err := database.New(
		base.Config(),
		database.Stores{
			Blob: commitFailingBlobStore{
				BlobStore: base.Blob(),
				err:       commitErr,
				armed:     armed,
				beforeFail: func() {
					if interject != nil {
						interject()
					}
				},
			},
			Metadata: base.Metadata(),
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	c = cm.PrimaryChain()
	require.NotNil(t, c)

	var origin common.Blake2b256
	blocks := generateTestChain(t, 1, origin, 20, 20, 2)

	var interjected bool
	interject = func() {
		if interjected {
			return
		}
		interjected = true
		// Mutates the chain without changing any staged field.
		c.ClearHeaders()
	}

	armed.Store(true)
	err = c.AddBlocks(blocks)
	require.True(t, interjected, "the interjected mutation must have run")
	require.ErrorIs(t, err, commitErr)
	require.ErrorIs(
		t,
		err,
		chain.ErrChainStateChangedDuringCommit,
		"a mutation that leaves every staged field identical is still a "+
			"mutation; the restore must not assume it is undoing only "+
			"its own work",
	)
}
