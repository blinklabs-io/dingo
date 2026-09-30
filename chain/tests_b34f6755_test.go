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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestAddBlockWithPointDeferredIfRefusesWithoutMutating pins the fail-closed
// half of the admission predicate: a false answer abandons the add before the
// chain is read or written, and says so with a distinguishable error rather
// than one that looks like an invalid block.
func TestAddBlockWithPointDeferredIfRefusesWithoutMutating(t *testing.T) {
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks[:3] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	tipBefore := c.Tip()

	next := testBlocks[3]
	point := ocommon.NewPoint(next.MockSlot, next.Hash().Bytes())
	calls := 0
	_, err = c.AddBlockWithPointDeferredIf(next, point, nil, func() bool {
		calls++
		return false
	})
	if !errors.Is(err, chain.ErrBlockAddNotAdmitted) {
		t.Fatalf("expected ErrBlockAddNotAdmitted, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("admit must be evaluated exactly once, got %d", calls)
	}
	if got := c.Tip(); !bytes.Equal(got.Point.Hash, tipBefore.Point.Hash) {
		t.Fatalf(
			"a refused add must not move the tip: %x -> %x",
			tipBefore.Point.Hash,
			got.Point.Hash,
		)
	}

	// The same block is added once the predicate admits it, so the refusal
	// above was the predicate's doing and not an unrelated rejection.
	if _, err := c.AddBlockWithPointDeferredIf(
		next, point, nil, func() bool { return true },
	); err != nil {
		t.Fatalf("unexpected error adding admitted block: %s", err)
	}
	if got := c.Tip(); !bytes.Equal(got.Point.Hash, next.Hash().Bytes()) {
		t.Fatalf("admitted block did not become the tip: %x", got.Point.Hash)
	}
}

// TestAddBlockWithPointDeferredIfEvaluatesAdmitUnderTheChainMutex is the
// property the predicate exists for. A caller that tests its own precondition
// and then calls an ordinary add has released nothing but holds nothing
// either: a concurrent mutation can land in between and make the test stale.
// Evaluating the predicate inside the add, under the mutex that serializes
// every chain mutation, is what makes the test and the mutation it guards one
// atomic step.
//
// Proven by observing that another goroutine's add cannot make progress while
// the predicate is running: if the predicate ran outside the lock, that add
// would complete immediately.
func TestAddBlockWithPointDeferredIfEvaluatesAdmitUnderTheChainMutex(
	t *testing.T,
) {
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks[:3] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}

	next := testBlocks[3]
	point := ocommon.NewPoint(next.MockSlot, next.Hash().Bytes())
	competitor := &MockBlock{
		MockBlockNumber: next.MockBlockNumber,
		MockSlot:        next.MockSlot + 1,
		MockHash:        testHashPrefix + "00fd",
		MockPrevHash:    testBlocks[2].MockHash,
	}
	competitorDone := make(chan struct{})
	blockedDuringAdmit := false

	_, err = c.AddBlockWithPointDeferredIf(next, point, nil, func() bool {
		go func() {
			defer close(competitorDone)
			_ = c.AddBlock(competitor, nil)
		}()
		// The competing add must be parked on the chain mutex we are
		// holding. A generous window keeps this from depending on
		// scheduling speed: the assertion is that it does NOT finish.
		select {
		case <-competitorDone:
			blockedDuringAdmit = false
		case <-time.After(250 * time.Millisecond):
			blockedDuringAdmit = true
		}
		return true
	})
	if err != nil {
		t.Fatalf("unexpected error adding block: %s", err)
	}
	<-competitorDone
	if !blockedDuringAdmit {
		t.Fatal(
			"a concurrent add completed while the admission predicate was " +
				"running, so the predicate is not evaluated under the chain mutex",
		)
	}
}

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

// TestAddRawBlocksRestoresChainStateWhenCommitFails pins the sibling path. Both
// batch functions stage the same fields and gate the commit-failure restore on
// batchRestoreIsSafeLocked, so this test and TestAddBlocksRestoresChainStateWhenCommitFails
// cover the two copies of that sequence -- the staging was added to addRawBlocks
// alone, and AddBlocks kept advancing its tip past a rolled-back batch.
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

// lockedBuffer serializes the chain logger's writes against the test
// goroutine reading them.
type lockedBuffer struct {
	mutex sync.Mutex
	buf   bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buf.String()
}

// TestSkippedBatchRestoreIsRecorded pins that addRawBlocks records the one
// divergence it deliberately declines to repair.
//
// When a batch's commit fails, addRawBlocks restores the snapshot it took
// before the batch -- but only while the chain still shows exactly what that
// batch left behind. When something moved the chain in between, restoring
// would write that snapshot over the mutation, raising tipBlockIndex back
// above blocks a rollback deleted, so the restore is skipped. Skipping is the
// right choice and it is also the moment the in-memory chain starts claiming a
// batch the commit discarded, which surfaces later as a missing block or an
// inflated fork depth. It has to be attributable to the commit failure that
// caused it rather than only to the symptom.
//
// Each round below reads a key its batch does not write and then commits a
// separate write to that key, so the batch applies to the in-memory chain and
// its commit fails on conflict; a block add parked on the chain lock moves the
// chain in the window between the two. Whether the parked add or the batch's
// own restore reaches that lock first is scheduling, so rounds repeat until
// the add wins -- what the assertion pins is that when it does, the skip is on
// the record.
// Not t.Parallel: swaps slog.SetDefault, so a concurrent test's
// slog.Default() calls would be redirected into this test's buffer.
func TestSkippedBatchRestoreIsRecorded(t *testing.T) {
	const (
		securityParam = 100
		rounds        = 40
	)

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	mustSetLedger(t, cm, securityParam)
	pc := cm.PrimaryChain()

	logged := &lockedBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(
		logged,
		&slog.HandlerOptions{Level: slog.LevelError},
	)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	seed := chain.RawBlock{
		Slot:        10,
		Hash:        pendingCommitHash("restore-skip-seed"),
		BlockNumber: 1,
		Type:        1,
		Cbor:        []byte{0x80},
	}
	if err := pc.AddRawBlocks([]chain.RawBlock{seed}); err != nil {
		t.Fatalf("AddRawBlocks(seed): %v", err)
	}

	skipped := false
	for round := range rounds {
		// An index no chain block occupies, used only to conflict this
		// round's batch commit.
		conflictIndex := uint64(900001 + round)
		tip := pc.Tip()
		doomed := chain.RawBlock{
			Slot: tip.Point.Slot + 10,
			Hash: pendingCommitHash(
				fmt.Sprintf("restore-skip-doomed-%d", round),
			),
			BlockNumber: tip.BlockNumber + 1,
			Type:        1,
			PrevHash:    tip.Point.Hash,
			Cbor:        []byte{0x80},
		}
		// AddBlock takes the chain lock as its first action, so a goroutine
		// parked on it holds a place in that lock's queue rather than still
		// setting up behind it.
		mover := generateTestChain(
			t,
			doomed.BlockNumber+1,
			common.NewBlake2b256(doomed.Hash),
			doomed.Slot+10,
			10,
			1,
		)[0]

		applying := make(chan struct{})
		release := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			err := pc.AddRawBlocksWithCallback(
				[]chain.RawBlock{doomed},
				func(_ chain.RawBlock, txn *database.Txn) error {
					// Registers a read of a key this transaction does not
					// write, so a write committed to it before this
					// transaction commits fails that commit on conflict.
					_, _ = txn.DB().BlockByIndex(conflictIndex, txn)
					close(applying)
					<-release
					return nil
				},
			)
			if err == nil {
				t.Errorf("round %d: expected a commit failure", round)
			}
		})
		<-applying
		if err := db.BlockCreate(models.Block{
			ID:     conflictIndex,
			Slot:   conflictIndex,
			Hash:   pendingCommitHash(fmt.Sprintf("restore-skip-conflict-%d", round)),
			Number: conflictIndex,
			Type:   1,
			Cbor:   []byte{0x80},
		}, nil); err != nil {
			t.Fatalf("round %d: conflicting write: %v", round, err)
		}
		moved := make(chan error, 1)
		wg.Go(func() {
			moved <- pc.AddBlock(mover, nil)
		})
		// Release the batch only once the add is queued on the chain lock it
		// holds, so the add runs before the batch's restore reacquires it.
		waitUntilParkedIn(t, "chain.(*Chain).addBlockInternal")
		close(release)
		wg.Wait()
		if <-moved == nil {
			// The add reached the chain first, so the batch found the chain
			// moved and skipped its restore.
			skipped = true
			break
		}
	}
	if !skipped {
		t.Skip("the chain never moved inside the commit-failure window")
	}

	record := logged.String()
	if !strings.Contains(
		record,
		"skipped in-memory restore after batch commit failure",
	) {
		t.Fatalf("skipped restore was not recorded; log was:\n%s", record)
	}
	for _, field := range []string{
		"applied_tip_block_index=",
		"tip_block_index=",
		"applied_generation=",
		"mutation_generation=",
	} {
		if !strings.Contains(record, field) {
			t.Errorf(
				"skipped-restore record is missing %q; log was:\n%s",
				field,
				record,
			)
		}
	}
}

// semaphoreFrame is the runtime frame a goroutine blocked on a sync.Mutex or
// sync.RWMutex sits in. Matching the frame rather than the goroutine's wait
// reason keeps this independent of how the runtime spells that reason, which
// has changed between Go releases.
const semaphoreFrame = "sync.runtime_Semacquire"

// waitUntilParkedIn blocks until some goroutine is parked on a lock taken
// inside symbol.
//
// The tests below have to release a chain lock only once a second goroutine is
// already queued on it, because what they exercise is the window that opens
// when that lock is handed over. Being queued on a lock is not observable
// through the chain's own API and a wall-clock delay would leave the round
// silently unexercised whenever the goroutine was slower than the delay, so
// this reads the condition off the runtime's own goroutine dump.
func waitUntilParkedIn(t *testing.T, symbol string) {
	t.Helper()
	buf := make([]byte, 1<<20)
	testutil.WaitForConditionWithInterval(
		t,
		func() bool {
			dump := string(buf[:runtime.Stack(buf, true)])
			for g := range strings.SplitSeq(dump, "\n\ngoroutine ") {
				if strings.Contains(g, semaphoreFrame) &&
					strings.Contains(g, symbol) {
					return true
				}
			}
			return false
		},
		5*time.Second,
		time.Millisecond,
		"no goroutine parked on a lock in "+symbol,
	)
}

// waitUntilGoroutineIn waits until a goroutine has reached symbol, including
// waits on channels rather than only sync locks.
func waitUntilGoroutineIn(t *testing.T, symbol string) {
	t.Helper()
	buf := make([]byte, 1<<20)
	testutil.WaitForConditionWithInterval(
		t,
		func() bool {
			dump := string(buf[:runtime.Stack(buf, true)])
			return strings.Contains(dump, symbol)
		},
		5*time.Second,
		time.Millisecond,
		"no goroutine reached "+symbol,
	)
}

// headerRestoreChain builds a persistent primary chain holding the first three
// test blocks and queues headers for the remaining three on top of that tip.
func headerRestoreChain(t *testing.T) (*database.Database, *chain.Chain) {
	t.Helper()
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	mustSetLedger(t, cm, 100)
	c := cm.PrimaryChain()
	for i, testBlock := range testBlocks[:3] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("AddBlock(%d): %v", i, err)
		}
	}
	for i, testBlock := range testBlocks[3:] {
		if err := c.AddBlockHeader(testBlock); err != nil {
			t.Fatalf("AddBlockHeader(%d): %v", i+3, err)
		}
	}
	return db, c
}

func headerPoint(b *MockBlock) ocommon.Point {
	return ocommon.Point{Slot: b.SlotNumber(), Hash: b.Hash().Bytes()}
}

// TestQueuedHeaderRollbackIsNotUndoneByACallerTransaction pins that a
// queued-header rollback survives a caller-supplied transaction rolling back
// underneath it.
//
// rollbackLocked answers a rollback whose point is a queued header without
// waiting for caller transactions, because it removes no persistent block. It
// does trim the queued-header list, and that list is exactly what
// finishCallerTxnAdd restores when a caller transaction concludes without
// committing. Nothing counted the trim as a chain mutation, so the restore
// guard that refuses to write over a moved chain did not see it, and the
// pre-add queue went back over the trim: a header the rollback had already
// published a HeaderInvalidationRollback for was re-queued.
func TestQueuedHeaderRollbackIsNotUndoneByACallerTransaction(t *testing.T) {
	t.Parallel()

	db, c := headerRestoreChain(t)
	// The queue is [3 4 5] over a tip of testBlocks[2].
	if got := c.HeaderCount(); got != 3 {
		t.Fatalf("queued %d headers, want 3", got)
	}
	// The add matches the first queued header, so it consumes it and leaves
	// [4 5]. Its store write stays inside txn until txn concludes.
	txn := db.BlobTxn(true)
	defer txn.Release()
	if err := c.AddBlock(testBlocks[3], txn); err != nil {
		t.Fatalf("AddBlock on caller transaction: %v", err)
	}
	if got := c.HeaderCount(); got != 2 {
		t.Fatalf("after the add the queue holds %d headers, want 2", got)
	}

	// Roll back to the first remaining queued header. That discards the
	// header for testBlocks[5] and removes no block.
	done := make(chan error, 1)
	go func() { done <- c.Rollback(headerPoint(testBlocks[4])) }()

	// The rollback may answer straight away or wait for the caller
	// transaction; both are permitted, and the assertions below hold either
	// way. What is not permitted is trimming the queue and then having the
	// trim written back over.
	var rollbackErr error
	answered := false
	select {
	case rollbackErr = <-done:
		answered = true
	case <-time.After(2 * time.Second):
	}

	if err := txn.Rollback(); err != nil {
		t.Fatalf("roll back the caller transaction: %v", err)
	}

	if !answered {
		select {
		case rollbackErr = <-done:
		case <-time.After(60 * time.Second):
			t.Fatal("queued-header rollback did not finish")
		}
	}
	if rollbackErr != nil {
		t.Fatalf("queued-header rollback: %v", rollbackErr)
	}

	// The header for testBlocks[3] returns, because the block that consumed
	// it was never committed. The header for testBlocks[5] must not: the
	// rollback discarded it and published its invalidation.
	if got := c.HeaderCount(); got != 2 {
		t.Fatalf(
			"queue holds %d headers after the rollback, want 2; a third is the discarded header re-queued",
			got,
		)
	}
	start, end := c.HeaderRange(10)
	if start.Slot != testBlocks[3].SlotNumber() {
		t.Fatalf(
			"queue starts at slot %d, want %d",
			start.Slot,
			testBlocks[3].SlotNumber(),
		)
	}
	if end.Slot != testBlocks[4].SlotNumber() {
		t.Fatalf(
			"queue ends at slot %d, want %d: the discarded header was resurrected",
			end.Slot,
			testBlocks[4].SlotNumber(),
		)
	}
	if tip := c.Tip(); tip.Point.Slot != testBlocks[2].SlotNumber() {
		t.Fatalf("unexpected tip after rollback: %+v", tip)
	}
}

// TestRejectedFirstCallerAddLeavesNoBarrierHold pins that a caller transaction
// whose only add was rejected does not hold the pending-add barrier. The hold
// exists so a removal path waits for a chain add whose store write it cannot
// see; an add rejected before it mutated anything leaves no such write, so a
// rollback that waits pendingAddDrainTimeout for it and then aborts is waiting
// for nothing.
func TestRejectedFirstCallerAddLeavesNoBarrierHold(t *testing.T) {
	t.Parallel()

	db, c := callerTxnChain(t)
	// Queue the header the chain expects next, then offer a different block.
	if err := c.AddBlockHeader(testBlocks[4]); err != nil {
		t.Fatalf("AddBlockHeader: %v", err)
	}
	txn := db.BlobTxn(true)
	defer txn.Release()
	if err := c.AddBlock(testBlocks[5], txn); err == nil {
		t.Fatal(
			"expected the add to be rejected for not matching the queued header",
		)
	}

	done := make(chan error, 1)
	go func() { done <- c.Rollback(rollbackPoint()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("rollback after a rejected caller add: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal(
			"rollback waited on a caller transaction that carries no chain add",
		)
	}
}

// TestClearHeadersDuringCallerTxnSurvivesAbort pins that restoring an aborted
// block add never resurrects a header queue cleared after that add.
func TestClearHeadersDuringCallerTxnSurvivesAbort(t *testing.T) {
	t.Parallel()

	db, c := headerRestoreChain(t)
	txn := db.BlobTxn(true)
	defer txn.Release()
	if err := c.AddBlock(testBlocks[3], txn); err != nil {
		t.Fatalf("AddBlock on caller transaction: %v", err)
	}
	c.ClearHeaders()
	if err := txn.Rollback(); err != nil {
		t.Fatalf("rollback caller transaction: %v", err)
	}
	if got := c.HeaderCount(); got != 0 {
		t.Fatalf("rollback resurrected %d cleared headers", got)
	}
	if tip := c.Tip(); tip.Point.Slot != testBlocks[2].SlotNumber() {
		t.Fatalf("aborted add left the tip advanced: %+v", tip)
	}
}

// TestHeaderQueuedDuringCallerTxnSurvivesAbort pins the other header-only
// mutation: a later header must not be overwritten by the add's old snapshot.
func TestHeaderQueuedDuringCallerTxnSurvivesAbort(t *testing.T) {
	t.Parallel()

	db, c := headerRestoreChain(t)
	txn := db.BlobTxn(true)
	defer txn.Release()
	if err := c.AddBlock(testBlocks[3], txn); err != nil {
		t.Fatalf("AddBlock on caller transaction: %v", err)
	}
	next := &MockBlock{
		MockBlockNumber: 7,
		MockSlot:        120,
		MockHash:        testHashPrefix + "0007",
		MockPrevHash:    testHashPrefix + "0006",
	}
	if err := c.AddBlockHeader(next); err != nil {
		t.Fatalf("AddBlockHeader during caller transaction: %v", err)
	}
	if err := txn.Rollback(); err != nil {
		t.Fatalf("rollback caller transaction: %v", err)
	}
	if got := c.HeaderCount(); got != 3 {
		t.Fatalf("queue holds %d headers after abort, want 3", got)
	}
	_, end := c.HeaderRange(10)
	if end.Slot != next.SlotNumber() {
		t.Fatalf("rollback dropped later header: end slot %d, want %d", end.Slot, next.SlotNumber())
	}
	if tip := c.Tip(); tip.Point.Slot != testBlocks[2].SlotNumber() {
		t.Fatalf("aborted add left the tip advanced: %+v", tip)
	}
}

// TestQueuedHeaderRollbackWithoutSecurityParamStillRefuses verifies that the
// pre-wait fast path preserves Rollback's persistent-chain configuration gate.
func TestQueuedHeaderRollbackWithoutSecurityParamStillRefuses(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	c := cm.PrimaryChain()
	for i, block := range testBlocks[:3] {
		if err := c.AddBlock(block, nil); err != nil {
			t.Fatalf("AddBlock(%d): %v", i, err)
		}
	}
	for i, block := range testBlocks[3:] {
		if err := c.AddBlockHeader(block); err != nil {
			t.Fatalf("AddBlockHeader(%d): %v", i+3, err)
		}
	}
	err = c.Rollback(headerPoint(testBlocks[3]))
	if !errors.Is(err, chain.ErrSecurityParamNotConfigured) {
		t.Fatalf("Rollback error = %v, want ErrSecurityParamNotConfigured", err)
	}
	if got := c.HeaderCount(); got != 3 {
		t.Fatalf("refused rollback trimmed queue to %d headers, want 3", got)
	}
}

// spliceForkHashPrefix keeps competing fork blocks distinct from the shared
// testBlocks fixtures while staying 32 bytes wide.
const spliceForkHashPrefix = "00004744abababababababababababababababababababababababababab"

func mockBlockPoint(b *MockBlock) ocommon.Point {
	return ocommon.Point{
		Slot: b.SlotNumber(),
		Hash: b.Hash().Bytes(),
	}
}

// buildAbandonedForkChain sets up the exact state that precedes the #3005
// cross-fork splice:
//
//	index 1: testBlocks[0]        (shared ancestor)
//	index 2: testBlocks[1]        (shared ancestor, rollback target)
//	index 3: forkB[0]             (fork B, currently on the chain)
//	index 4: forkB[1]             (fork B tip)
//
// while testBlocks[2] and testBlocks[3] (fork A) were rolled back off the chain
// and therefore only survive in the manager's retained block cache, still
// carrying their old indices 3 and 4.
//
// It returns the chain plus fork A's first rolled-back block.
func buildAbandonedForkChain(
	t *testing.T,
	db *database.Database,
) (*chain.Chain, *MockBlock, []*MockBlock) {
	t.Helper()
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	mustSetLedger(t, cm, 100)
	c := cm.PrimaryChain()
	// Fork A: the shared ancestors plus two blocks we later abandon.
	for _, testBlock := range testBlocks[:4] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding fork A block: %s", err)
		}
	}
	// Abandon fork A back to the shared ancestor at index 2.
	sharedAncestor := testBlocks[1]
	if err := c.Rollback(mockBlockPoint(sharedAncestor)); err != nil {
		t.Fatalf("unexpected error rolling back to shared ancestor: %s", err)
	}
	// Fork B replaces indices 3 and 4 with different blocks.
	forkB := []*MockBlock{
		{
			MockBlockNumber: 3,
			MockSlot:        sharedAncestor.MockSlot + 1,
			MockHash:        spliceForkHashPrefix + "0003",
			MockPrevHash:    sharedAncestor.MockHash,
		},
		{
			MockBlockNumber: 4,
			MockSlot:        sharedAncestor.MockSlot + 2,
			MockHash:        spliceForkHashPrefix + "0004",
			MockPrevHash:    spliceForkHashPrefix + "0003",
		},
	}
	for _, forkBlock := range forkB {
		if err := c.AddBlock(forkBlock, nil); err != nil {
			t.Fatalf("unexpected error adding fork B block: %s", err)
		}
	}
	// Precondition for the wedge: fork A's abandoned block is still
	// resolvable by point out of the retained block cache, and it still
	// reports the index that fork B now occupies.
	abandoned := testBlocks[2]
	cachedBlock, err := c.BlockByPoint(mockBlockPoint(abandoned), nil)
	if err != nil {
		t.Fatalf(
			"expected abandoned fork A block to stay resolvable by point: %s",
			err,
		)
	}
	if cachedBlock.ID != 3 {
		t.Fatalf(
			"expected retained fork A block to keep index 3, got %d",
			cachedBlock.ID,
		)
	}
	return c, abandoned, forkB
}

// assertChainPrevHashContiguous walks the persisted chain from origin and
// verifies every block's PrevHash matches the hash of the block stored at the
// preceding index. A cross-fork splice shows up here as a block whose parent is
// not the block that physically precedes it, which is precisely the shape that
// leaves a spender on the chain whose producing block was never applied.
func assertChainPrevHashContiguous(t *testing.T, c *chain.Chain) {
	t.Helper()
	iter, err := c.FromPoint(ocommon.NewPointOrigin(), false)
	if err != nil {
		t.Fatalf("unexpected error creating chain iterator: %s", err)
	}
	defer iter.Cancel()
	var prevHash []byte
	var prevSlot uint64
	for {
		next, err := iter.Next(false)
		if errors.Is(err, chain.ErrIteratorChainTip) {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error iterating chain: %s", err)
		}
		if next == nil {
			t.Fatal("unexpected nil iterator result")
		}
		blk := next.Block
		if prevHash != nil && !bytes.Equal(blk.PrevHash, prevHash) {
			t.Fatalf(
				"cross-fork splice: block at index %d (slot %d) has prev hash %s "+
					"but the preceding chain block (slot %d) has hash %s",
				blk.ID,
				blk.Slot,
				hex.EncodeToString(blk.PrevHash),
				prevSlot,
				hex.EncodeToString(prevHash),
			)
		}
		prevHash = blk.Hash
		prevSlot = blk.Slot
	}
}

// TestRollbackRejectsPointNotOnChain covers the root cause of issue #3005.
//
// Chain.rollbackLocked resolves the rollback point through
// ChainManager.blockByPoint, which answers from the retained block cache before
// the database. A block this chain already rolled back therefore still resolves
// and still reports its old block index, an index another fork now occupies.
// Rolling back to it truncates to that stale index and moves currentTip to a
// point the chain does not hold, so the next block is appended above a block
// that is not its parent: a cross-fork splice.
func TestRollbackRejectsPointNotOnChain(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	c, abandoned, forkB := buildAbandonedForkChain(t, db)
	forkBTip := mockBlockPoint(forkB[len(forkB)-1])

	err := c.Rollback(mockBlockPoint(abandoned))
	if err == nil {
		t.Fatal(
			"expected Rollback to reject a point this chain no longer holds",
		)
	}
	if !errors.Is(err, models.ErrBlockNotFound) {
		t.Fatalf("expected ErrBlockNotFound, got: %s", err)
	}
	if !errors.Is(err, chain.ErrRollbackPointNotOnChain) {
		t.Fatalf("expected ErrRollbackPointNotOnChain, got: %s", err)
	}
	tip := c.Tip()
	if tip.Point.Slot != forkBTip.Slot ||
		!bytes.Equal(tip.Point.Hash, forkBTip.Hash) {
		t.Fatalf(
			"rejected rollback must leave the tip untouched: got %d/%s, want %d/%s",
			tip.Point.Slot,
			hex.EncodeToString(tip.Point.Hash),
			forkBTip.Slot,
			hex.EncodeToString(forkBTip.Hash),
		)
	}
	assertChainPrevHashContiguous(t, c)
}

// TestValidateRollbackRejectsPointNotOnChain covers the pre-check the chainsync
// rollback-loop detector uses to decide whether a repeated peer rollback is
// "crossable". A point resolvable only through the retained cache must not be
// reported as crossable, otherwise the detector keeps re-applying the very
// rollback that splices the chain.
func TestValidateRollbackRejectsPointNotOnChain(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	c, abandoned, _ := buildAbandonedForkChain(t, db)

	err := c.ValidateRollback(mockBlockPoint(abandoned))
	if err == nil {
		t.Fatal(
			"expected ValidateRollback to reject a point this chain no longer holds",
		)
	}
	if !errors.Is(err, models.ErrBlockNotFound) {
		t.Fatalf("expected ErrBlockNotFound, got: %s", err)
	}
	if !errors.Is(err, chain.ErrRollbackPointNotOnChain) {
		t.Fatalf("expected ErrRollbackPointNotOnChain, got: %s", err)
	}
}

// TestRollbackToRetainedPointDoesNotSpliceChain drives the full defect: after
// the bad rollback the chain accepts a continuation built on the abandoned fork
// while the block physically stored at the preceding index belongs to the other
// fork. Iterating the chain then yields a block whose parent is absent, which is
// what leaves the ledger unable to resolve the producer of that block's inputs.
func TestRollbackToRetainedPointDoesNotSpliceChain(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	c, abandoned, _ := buildAbandonedForkChain(t, db)

	// A peer serving fork A rolls us back to its own block, then feeds the
	// next block on fork A.
	if err := c.Rollback(mockBlockPoint(abandoned)); err == nil {
		// Only the unfixed code reaches here; keep going so the assertion
		// below reports the splice rather than a bare "expected error".
		continuation := testBlocks[3]
		if addErr := c.AddBlock(continuation, nil); addErr != nil {
			t.Fatalf(
				"unexpected error adding fork A continuation: %s",
				addErr,
			)
		}
	}
	assertChainPrevHashContiguous(t, c)
}

// TestRollbackRejectsPointAheadOfTip covers the other shape of the same defect.
//
// After a rollback the abandoned blocks keep their original, higher block
// indexes in the retained cache, so a peer can hand back a point that resolves
// above the current tip. Obeying it set tipBlockIndex above the last block the
// chain actually stores and moved currentTip to a block absent from the chain,
// leaving a hole: the next block was written past the gap, chain iteration
// stopped short of it, and prev-hash contiguity was enforced against a phantom
// tip. Like the stale-index shape, the chain must refuse rather than adopt a
// tip it does not hold.
//
// It must be refused as "point not found", never as an over-K rollback: issue
// #3035 was a node permanently denying every peer because this case was
// misclassified as exceeding the security parameter. Not-on-chain re-intersects
// and recovers; over-K does not.
func TestRollbackRejectsPointAheadOfTip(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	mustSetLedger(t, cm, 2)
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	// Roll back the last two blocks (index 6 -> 4). The removed blocks stay in
	// the manager's cache carrying indexes 5 and 6.
	if err := c.Rollback(mockBlockPoint(testBlocks[len(testBlocks)-3])); err != nil {
		t.Fatalf("unexpected error rolling back chain: %s", err)
	}
	tipBefore := c.Tip()
	aheadPoint := mockBlockPoint(testBlocks[len(testBlocks)-1])

	for _, tc := range []struct {
		name string
		call func() error
	}{
		{
			name: "ValidateRollback",
			call: func() error { return c.ValidateRollback(aheadPoint) },
		},
		{
			name: "Rollback",
			call: func() error { return c.Rollback(aheadPoint) },
		},
	} {
		err := tc.call()
		if err == nil {
			t.Fatalf(
				"%s: expected rejection of a point ahead of the chain tip",
				tc.name,
			)
		}
		if errors.Is(err, chain.ErrRollbackExceedsSecurityParam) {
			t.Fatalf(
				"%s: a point ahead of the tip must not be reported as "+
					"exceeding security param K (issue #3035): %s",
				tc.name,
				err,
			)
		}
		if !errors.Is(err, chain.ErrRollbackPointNotOnChain) {
			t.Fatalf(
				"%s: expected ErrRollbackPointNotOnChain, got: %s",
				tc.name,
				err,
			)
		}
		if !errors.Is(err, models.ErrBlockNotFound) {
			t.Fatalf(
				"%s: rejection must wrap ErrBlockNotFound so callers "+
					"re-intersect, got: %s",
				tc.name,
				err,
			)
		}
	}

	tipAfter := c.Tip()
	if tipAfter.Point.Slot != tipBefore.Point.Slot ||
		!bytes.Equal(tipAfter.Point.Hash, tipBefore.Point.Hash) {
		t.Fatalf(
			"rejected rollback must leave the tip untouched: got %d/%s, want %d/%s",
			tipAfter.Point.Slot,
			hex.EncodeToString(tipAfter.Point.Hash),
			tipBefore.Point.Slot,
			hex.EncodeToString(tipBefore.Point.Hash),
		)
	}
	assertChainPrevHashContiguous(t, c)
}

// TestRollbackStillAcceptsPointsOnChain guards against the fix over-rejecting:
// ordinary rollbacks to blocks the chain still holds must keep working, and a
// rollback to origin must remain possible.
func TestRollbackStillAcceptsPointsOnChain(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	mustSetLedger(t, cm, 100)
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	target := mockBlockPoint(testBlocks[2])
	if err := c.ValidateRollback(target); err != nil {
		t.Fatalf("unexpected error validating on-chain rollback: %s", err)
	}
	if err := c.Rollback(target); err != nil {
		t.Fatalf("unexpected error rolling back to on-chain point: %s", err)
	}
	tip := c.Tip()
	if tip.Point.Slot != target.Slot ||
		!bytes.Equal(tip.Point.Hash, target.Hash) {
		t.Fatalf(
			"expected tip at rollback point %d, got %d",
			target.Slot,
			tip.Point.Slot,
		)
	}
	assertChainPrevHashContiguous(t, c)
	if err := c.Rollback(ocommon.NewPointOrigin()); err != nil {
		t.Fatalf("unexpected error rolling back to origin: %s", err)
	}
	if c.Tip().Point.Slot != 0 {
		t.Fatalf("expected origin tip, got slot %d", c.Tip().Point.Slot)
	}
}

// publishMode selects how a handler publishes the chain.update it deferred while
// holding its mutex, so one scenario can model both the pre-fix and post-fix
// ledger callers exactly.
type publishMode int

const (
	// perHandlerFlush is the pre-fix caller: each handler publishes ITS OWN
	// returned chain.update event(s) from its OWN pendingPublishes after
	// releasing its mutex. The two handlers' queues are independent, so a
	// handler that mutated the chain later can still flush first.
	perHandlerFlush publishMode = iota
	// sharedSequencer is the fix: each handler drains the chain-level
	// sequencer (chain.PublishPendingChainUpdates). Whichever handler flushes
	// first publishes the WHOLE queue in enqueue (== mutation) order, so the
	// two handlers can no longer invert.
	sharedSequencer
)

// observedUpdate labels a published chain.update by kind so the test can assert
// the block-add update and the rollback update come out in mutation order.
type observedUpdate string

const (
	updateAdd      observedUpdate = "add"
	updateRollback observedUpdate = "rollback"
)

// runDeferredOrderingScenario drives the exact concurrency wolf31o2 flagged: a
// blockfetch-style deferred add and a chainsync-style deferred rollback, each
// mutating the primary chain under its OWN mutex (the real handlers hold two
// different mutexes -- chainsyncBlockfetchMutex and chainsyncMutex -- so they do
// not mutually exclude), then publishing their deferred chain.update AFTER
// releasing that mutex.
//
// The interleaving is pinned deterministically with channels, no sleeps:
//
//  1. the add mutates the chain first (tip -> block C), enqueuing update "add";
//  2. the rollback then mutates the chain (tip C -> block B, discarding C),
//     enqueuing update "rollback"; so the true chain-mutation order is
//     [add, rollback];
//  3. the rollback PUBLISHES first, then the add publishes.
//
// Step 3 is the inversion trigger: the handler that mutated second publishes
// first. With perHandlerFlush each handler emits only its own event, so the
// subscriber observes [rollback, add] -- the reverse of the mutation order.
// With sharedSequencer the rollback's flush drains the shared queue in
// enqueue order and emits [add, rollback]; the add's later flush finds the
// queue already empty.
//
// It returns the chain.update kinds in the exact order the subscriber received
// them.
func runDeferredOrderingScenario(
	t *testing.T,
	mode publishMode,
) []observedUpdate {
	t.Helper()

	eventBus := event.NewEventBus(nil, nil)
	t.Cleanup(eventBus.Stop)

	cm, err := chain.NewManager(nil, eventBus)
	require.NoError(t, err)
	c := cm.PrimaryChain()
	require.NotNil(t, c)

	blocks, err := testfixtures.GenerateConwayChain(3)
	require.NoError(t, err)
	require.Len(t, blocks, 3)

	pointOf := func(i int) ocommon.Point {
		return ocommon.Point{
			Slot: blocks[i].SlotNumber(),
			Hash: blocks[i].Hash().Bytes(),
		}
	}

	// Seed the chain up to block B (blocks[1]) BEFORE subscribing, so the
	// scenario's subscriber records only the add/rollback under test and not
	// the seed updates.
	require.NoError(t, c.AddBlock(blocks[0], nil))
	require.NoError(t, c.AddBlock(blocks[1], nil))
	require.Equal(t, blocks[1].SlotNumber(), c.Tip().Point.Slot)

	// Lossless subscriber with generous buffer: it never drops and preserves
	// per-subscriber delivery order, so the slice it records is the true
	// publish order.
	subId, ch := eventBus.SubscribeWithBufferPolicy(
		chain.ChainUpdateEventType,
		16,
		event.SubscriberBackpressureBlock,
	)
	require.NotZero(t, subId)

	classify := func(evt event.Event) (observedUpdate, bool) {
		switch evt.Data.(type) {
		case chain.ChainBlockEvent:
			return updateAdd, true
		case chain.ChainRollbackEvent:
			return updateRollback, true
		default:
			return "", false
		}
	}

	// Two independent mutexes standing in for chainsyncBlockfetchMutex
	// (add) and chainsyncMutex (rollback): the point of the bug is that these
	// are DIFFERENT locks, so the two handlers' critical sections and their
	// deferred publishes are not serialized against each other.
	var addMutex, rollbackMutex sync.Mutex

	addMutated := make(chan struct{})        // add finished its chain mutation
	rollbackPublished := make(chan struct{}) // rollback finished publishing

	var wg sync.WaitGroup
	wg.Add(2)

	// Blockfetch-style deferred add: extend the chain with block C
	// (blocks[2]) on top of block B.
	go func() {
		defer wg.Done()
		addMutex.Lock()
		addEvt, addErr := c.AddBlockWithPointDeferred(
			blocks[2],
			pointOf(2),
			nil,
		)
		addMutex.Unlock()
		require.NoError(t, addErr)
		require.Equal(
			t,
			event.EventType(chain.ChainUpdateEventType),
			addEvt.Type,
		)
		// The chain was mutated first; signal the rollback to proceed.
		close(addMutated)
		// Publish only AFTER the rollback has published, forcing the
		// second-mutating handler to publish first.
		<-rollbackPublished
		switch mode {
		case perHandlerFlush:
			eventBus.Publish(addEvt.Type, addEvt)
		case sharedSequencer:
			c.PublishPendingChainUpdates()
		}
	}()

	// Chainsync-style deferred rollback: rewind block C, back to block B.
	go func() {
		defer wg.Done()
		<-addMutated // ensure the add mutated the chain first
		rollbackMutex.Lock()
		rbEvts, rbErr := c.RollbackDeferred(pointOf(1))
		rollbackMutex.Unlock()
		require.NoError(t, rbErr)
		require.NotEmpty(t, rbEvts)
		switch mode {
		case perHandlerFlush:
			for _, evt := range rbEvts {
				eventBus.Publish(evt.Type, evt)
			}
		case sharedSequencer:
			c.PublishPendingChainUpdates()
		}
		close(rollbackPublished)
	}()

	wg.Wait()
	// The chain really ended at block B: the add was applied then rolled back.
	require.Equal(t, blocks[1].SlotNumber(), c.Tip().Point.Slot)

	// Collect exactly the two chain.update events the scenario publishes.
	var got []observedUpdate
	deadline := time.After(5 * time.Second)
	for len(got) < 2 {
		select {
		case evt := <-ch:
			if kind, ok := classify(evt); ok {
				got = append(got, kind)
			}
		case <-deadline:
			t.Fatalf(
				"timed out: observed %d of 2 chain.update events (%v)",
				len(got), got,
			)
		}
	}
	// Both publishing goroutines have completed, so no third update can still
	// be in flight. Check the subscriber without using a timing window.
	select {
	case evt := <-ch:
		if kind, ok := classify(evt); ok {
			t.Fatalf("unexpected extra chain.update published: %s", kind)
		}
	default:
	}
	return got
}

// TestDeferredChainUpdatesPublishInChainMutationOrder is the regression guard
// for wolf31o2's blocking review (ledger/chainsync.go:531): deferred chain.update
// publication must follow chain-mutation order across the blockfetch-add and
// chainsync-rollback handlers, which hold different mutexes and previously
// flushed independent pendingPublishes queues.
//
// The shared chain-level sequencer makes the handler that flushes first publish
// the whole queue in enqueue (== mutation) order, so the observed order is
// [add, rollback] -- the order the chain was actually mutated -- regardless of
// which handler flushes first.
//
// To see this FAIL against the pre-fix per-instance-queue behaviour, change the
// mode below to perHandlerFlush: the second-mutating rollback flushes its own
// queue first and the subscriber observes [rollback, add], the inverse of the
// chain-mutation order. TestDeferredPerHandlerFlushInvertsAcrossHandlers pins
// that inversion so this guard is not vacuous.
func TestDeferredChainUpdatesPublishInChainMutationOrder(t *testing.T) {
	t.Parallel()

	got := runDeferredOrderingScenario(t, sharedSequencer)
	require.Equal(
		t,
		[]observedUpdate{updateAdd, updateRollback},
		got,
		"deferred chain.update publication must match chain-mutation order "+
			"[add, rollback] across the two handlers",
	)
}

// TestDeferredPerHandlerFlushInvertsAcrossHandlers proves the concurrency the
// fix addresses is real, not hypothetical: with the pre-fix per-handler flush,
// the identical interleaving publishes the rollback BEFORE the add even though
// the chain was mutated add-then-rollback. This is the exact ordering that
// drove the chain.update subscriber's block apply/undo notifications out of
// order, and it is what the shared sequencer (asserted above) prevents.
func TestDeferredPerHandlerFlushInvertsAcrossHandlers(t *testing.T) {
	t.Parallel()

	got := runDeferredOrderingScenario(t, perHandlerFlush)
	require.Equal(
		t,
		[]observedUpdate{updateRollback, updateAdd},
		got,
		"per-handler flush is expected to invert publish order relative to "+
			"chain-mutation order; if this no longer inverts the guard test "+
			"has stopped exercising the race",
	)
}

// TestDeferredAddAndRollbackDoNotPublish is the chain-package half of the
// chainsync/blockfetch drain deadlock fix.
//
// The ledger calls into the chain while holding chainsyncBlockfetchMutex /
// chainsyncMutex. To keep a backpressured publish from stalling under those
// locks, the mutex-holding paths use AddBlockWithPointDeferred and
// RollbackDeferred, which return the chain.update event(s) for the ledger to
// publish AFTER the mutex is released (see ledger.pendingPublishes) instead of
// publishing inline.
//
// This test installs a lossless (SubscriberBackpressureBlock) chain.update
// subscriber whose single buffer slot is filled and never drained: any inline
// Publish would block on it forever. It then drives blocks and a rollback
// through the deferred methods and requires each to return promptly and to
// publish nothing. A regression that published inline from these methods would
// block on the stalled subscriber and fail the timeout, or would leak an event
// onto the subscriber channel and fail the "published nothing" check.
func TestDeferredAddAndRollbackDoNotPublish(t *testing.T) {
	t.Parallel()

	eventBus := event.NewEventBus(nil, nil)
	t.Cleanup(eventBus.Stop)

	subId, ch := eventBus.SubscribeWithBufferPolicy(
		chain.ChainUpdateEventType,
		1,
		event.SubscriberBackpressureBlock,
	)
	require.NotZero(t, subId)
	require.NotNil(t, ch)

	// Fill the subscriber's only buffer slot; from here an inline Publish
	// blocks.
	eventBus.Publish(
		chain.ChainUpdateEventType,
		event.NewEvent(chain.ChainUpdateEventType, chain.ChainBlockEvent{}),
	)

	cm, err := chain.NewManager(nil, eventBus)
	require.NoError(t, err)
	c := cm.PrimaryChain()
	require.NotNil(t, c)

	blocks, err := testfixtures.GenerateConwayChain(2)
	require.NoError(t, err)
	require.Len(t, blocks, 2)

	pointOf := func(i int) ocommon.Point {
		return ocommon.Point{
			Slot: blocks[i].SlotNumber(),
			Hash: blocks[i].Hash().Bytes(),
		}
	}

	// Add both blocks through the deferred API. Each call must return promptly
	// (it never touches the bus) and hand back a populated chain.update event.
	for i := range blocks {
		type result struct {
			evt event.Event
			err error
		}
		done := make(chan result, 1)
		go func() {
			evt, addErr := c.AddBlockWithPointDeferred(
				blocks[i],
				pointOf(i),
				nil,
			)
			done <- result{evt: evt, err: addErr}
		}()
		select {
		case r := <-done:
			require.NoError(t, r.err)
			require.Equal(
				t,
				event.EventType(chain.ChainUpdateEventType),
				r.evt.Type,
				"deferred add must return the chain.update event to publish",
			)
		case <-time.After(5 * time.Second):
			t.Fatalf(
				"AddBlockWithPointDeferred(%d) blocked: it must return the "+
					"event, not publish it inline under a stalled subscriber",
				i,
			)
		}
	}
	require.Equal(t, blocks[1].SlotNumber(), c.Tip().Point.Slot)

	// Roll back the newest block through the deferred API. It must also return
	// promptly and hand back its chain.update event(s).
	rbDone := make(chan []event.Event, 1)
	rbErr := make(chan error, 1)
	go func() {
		evts, err := c.RollbackDeferred(pointOf(0))
		rbErr <- err
		rbDone <- evts
	}()
	select {
	case err := <-rbErr:
		require.NoError(t, err)
		evts := <-rbDone
		require.NotEmpty(t, evts)
	case <-time.After(5 * time.Second):
		t.Fatal(
			"RollbackDeferred blocked: it must return the events, not publish " +
				"them inline under a stalled subscriber",
		)
	}
	require.Equal(t, blocks[0].SlotNumber(), c.Tip().Point.Slot)

	// Nothing beyond the single pre-fill event may have reached the subscriber:
	// the deferred methods published nothing.
	select {
	case <-ch:
		// the pre-fill event
	default:
		t.Fatal("expected the pre-fill event in the subscriber buffer")
	}
	select {
	case extra := <-ch:
		t.Fatalf(
			"deferred add/rollback published %v to the chain.update "+
				"subscriber; they must defer publication to the caller",
			extra.Type,
		)
	default:
	}
}

// TestChainHoldsPoint verifies HoldsPoint is true only for blocks currently
// on the chain: not for unknown points, a wrong hash at a held slot, or a
// block that was rolled back but stays resolvable from the block cache.
func TestChainHoldsPoint(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	mustSetLedger(t, cm, 100)
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	last := testBlocks[len(testBlocks)-1]
	held := blockPoint(last)
	if !c.HoldsPoint(held) {
		t.Fatal("expected chain to hold its tip block")
	}
	if c.HoldsPoint(ocommon.Point{Slot: held.Slot, Hash: []byte{0xde, 0xad}}) {
		t.Fatal("expected wrong hash at a held slot to not be held")
	}
	if c.HoldsPoint(
		ocommon.Point{Slot: held.Slot + 1_000_000, Hash: held.Hash},
	) {
		t.Fatal("expected unknown point to not be held")
	}
	surviving := blockPoint(testBlocks[len(testBlocks)-2])
	if err := c.Rollback(surviving); err != nil {
		t.Fatalf("unexpected error rolling back chain: %s", err)
	}
	if c.HoldsPoint(held) {
		t.Fatal("expected rolled-back block to not be held")
	}
	if !c.HoldsPoint(surviving) {
		t.Fatal("expected surviving block to remain held")
	}
}

// behindPeerChainLength is long enough that the intersect ladder reaches
// several sparse rungs past the dense band.
const behindPeerChainLength = 200

// behindPeerSecurityParam is the security parameter K used by these tests. It
// sits above the dense band (32 points) so the ladder's sparse rungs, not the
// dense band, decide where a lagging peer intersects — the same relationship
// every real network has (K = 108/432/2160 against a fixed 32-point band).
const behindPeerSecurityParam = 40

// newBehindPeerChain builds a persistent chain of behindPeerChainLength linked
// blocks with K = behindPeerSecurityParam and returns it with its headers.
func newBehindPeerChain(t *testing.T) (*chain.Chain, []*MockBlock) {
	t.Helper()
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	mustSetLedger(t, cm, behindPeerSecurityParam)
	c := cm.PrimaryChain()
	headers := makeLinkedHeaders(behindPeerChainLength, 0, 1, "")
	for _, header := range headers {
		require.NoError(t, c.AddBlock(header, nil))
	}
	return c, headers
}

// peerIntersectAnswer models what a chainsync server holding every block up to
// peerTipSlot answers to our FindIntersect. Our points are descending, and the
// server replies with the first point it holds, so the answer is the shallowest
// ladder rung at or below the peer's tip.
func peerIntersectAnswer(
	t *testing.T,
	points []ocommon.Point,
	peerTipSlot uint64,
) ocommon.Point {
	t.Helper()
	for _, point := range points {
		if point.Slot <= peerTipSlot {
			return point
		}
	}
	t.Fatalf(
		"peer holding slot %d matched none of our intersect points",
		peerTipSlot,
	)
	return ocommon.Point{}
}

// makeLinkedHeaders spaces blocks 20 slots apart, so a point's depth below the
// tip in blocks is recoverable from its slot.
func behindPeerDepth(headers []*MockBlock, point ocommon.Point) uint64 {
	tipSlot := headers[len(headers)-1].MockSlot
	return (tipSlot - point.Slot) / 20
}

// A peer that is behind us by fewer than K blocks is inside the security
// parameter by every measure that matters, yet the intersect ladder resolves
// its best common point to the next power-of-two rung, which can sit past K.
// The rollback the peer then asks for is refused as a deeper-than-K fork even
// though the peer's chain is a strict prefix of ours and nothing diverged.
//
// The ladder must offer a rung at K itself so that any peer within K of our
// tip resolves to a point at most K back.
func TestIntersectPointsKeepPeerWithinSecurityParamCrossable(t *testing.T) {
	t.Parallel()

	c, headers := newBehindPeerChain(t)

	// The peer is 35 blocks behind: comfortably inside K=40.
	const peerLag = 35
	peerTip := headers[len(headers)-1-peerLag]

	points := c.IntersectPoints(100)
	answer := peerIntersectAnswer(t, points, peerTip.MockSlot)
	depth := behindPeerDepth(headers, answer)

	require.LessOrEqualf(
		t,
		depth,
		uint64(behindPeerSecurityParam),
		"a peer only %d blocks behind must resolve to an intersect within "+
			"K=%d, got depth %d",
		peerLag,
		behindPeerSecurityParam,
		depth,
	)
	require.NoErrorf(
		t,
		c.ValidateRollback(answer),
		"rollback to the intersect a peer %d blocks behind resolves to "+
			"must be crossable with K=%d",
		peerLag,
		behindPeerSecurityParam,
	)
}

// A peer further behind than K still resolves to a rung past K, and the chain
// layer still refuses that rollback: from the chain's point of view the depth
// is real. Nothing about the peer is divergent, though — its chain is a strict
// prefix of ours — so this rejection must not be turned into an eviction. That
// classification belongs to the ledger's chainsync handler; this test pins the
// chain-layer half of the contract so the ladder change above is not mistaken
// for a full fix.
func TestIntersectPointsStillExceedKForPeerBehindBeyondK(t *testing.T) {
	t.Parallel()

	c, headers := newBehindPeerChain(t)

	// The peer is 45 blocks behind, past K=40.
	const peerLag = 45
	peerTip := headers[len(headers)-1-peerLag]

	points := c.IntersectPoints(100)
	answer := peerIntersectAnswer(t, points, peerTip.MockSlot)

	require.Greater(
		t,
		behindPeerDepth(headers, answer),
		uint64(behindPeerSecurityParam),
	)
	require.ErrorIs(
		t,
		c.ValidateRollback(answer),
		chain.ErrRollbackExceedsSecurityParam,
		"expected the chain layer to refuse a rollback deeper than K",
	)
}

// The K rung is merged into a list the chainsync protocol requires to be
// ordered newest-first, and it must land in its sorted position for every
// relationship between K, the dense band and the chain length — including
// chains shorter than the next doubling rung, where the loop that emits the
// sparse rungs stops early.
func TestIntersectPointsStayDescendingWithSecurityRung(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		chainLength int
		securityK   int
	}{
		{name: "k inside dense band", chainLength: 200, securityK: 8},
		{name: "k at dense band edge", chainLength: 200, securityK: 32},
		{name: "k between rungs", chainLength: 200, securityK: 40},
		{name: "k on a rung", chainLength: 200, securityK: 64},
		{name: "k past chain", chainLength: 40, securityK: 100},
		{name: "chain shorter than next rung", chainLength: 45, securityK: 40},
		{name: "chain equal to k", chainLength: 41, securityK: 40},
		{name: "tiny chain", chainLength: 3, securityK: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newTestDB(t)
			cm, err := chain.NewManager(db, nil)
			require.NoError(t, err)
			mustSetLedger(t, cm, tc.securityK)
			c := cm.PrimaryChain()
			headers := makeLinkedHeaders(tc.chainLength, 0, 1, "")
			for _, header := range headers {
				require.NoError(t, c.AddBlock(header, nil))
			}

			points := c.IntersectPoints(100)
			require.NotEmpty(t, points)
			for i := 0; i+1 < len(points); i++ {
				require.Greaterf(
					t,
					points[i].Slot,
					points[i+1].Slot,
					"intersect points must be strictly descending at %d",
					i,
				)
			}

			// Whenever the chain is longer than K, a peer exactly K
			// behind must find a point at most K deep, so the rollback
			// it asks for stays inside the security parameter.
			if tc.chainLength > tc.securityK+1 {
				peerTip := headers[len(headers)-1-tc.securityK]
				answer := peerIntersectAnswer(t, points, peerTip.MockSlot)
				require.LessOrEqual(
					t,
					behindPeerDepth(headers, answer),
					uint64(tc.securityK),
				)
				require.NoError(t, c.ValidateRollback(answer))
			}
		})
	}
}

// originContinuityCase describes one candidate offered as the first block of a
// chain that a rollback has emptied back to origin.
type originContinuityCase struct {
	name        string
	blockNumber uint64
	prevHash    string
	wantReject  bool
}

// originContinuityCases covers both directions: a chain emptied back to origin
// must still accept the network's genuine first block, and must reject a block
// from further along the chain -- which is what a peer whose chainsync cursor
// survived the rollback offers next (issue #4202).
//
// The chain package does not know the network's genesis hash, so the anchor
// available at origin is the block number, and the only value that leaves no
// gap is 0: Ouroboros numbers the first block after genesis 0 -- the Byron
// epoch-boundary block on a Byron network, the first block of the starting era
// on a post-Byron genesis network. Number 1 is rejected rather than tolerated,
// because the ordinary contiguity check compares a candidate against the
// accepted tip: a chain anchored at 1 is self-consistent from its second block
// on and stays short block 0 forever. See
// TestAddBlockAfterRollbackToOriginRejectsChainShortOfBlockZero.
var originContinuityCases = []originContinuityCase{
	{
		name:        "genuine first block number 0 accepted",
		blockNumber: 0,
		prevHash:    "",
		wantReject:  false,
	},
	{
		name:        "first block number 1 rejected",
		blockNumber: 1,
		prevHash:    "",
		wantReject:  true,
	},
	{
		name:        "block from further along the chain rejected",
		blockNumber: 160,
		prevHash:    testHashPrefix + "00a0",
		wantReject:  true,
	},
	{
		name:        "second block of the chain rejected",
		blockNumber: 2,
		prevHash:    testHashPrefix + "0001",
		wantReject:  true,
	},
}

func originCandidate(c originContinuityCase) *MockBlock {
	return &MockBlock{
		MockBlockNumber: c.blockNumber,
		MockSlot:        3360,
		MockHash:        testHashPrefix + "beef",
		MockPrevHash:    c.prevHash,
	}
}

func originCandidateRawBlock(c originContinuityCase) chain.RawBlock {
	candidate := originCandidate(c)
	return chain.RawBlock{
		Slot:        candidate.MockSlot,
		Hash:        candidate.Hash().Bytes(),
		BlockNumber: candidate.MockBlockNumber,
		Type:        uint(candidate.Type()),
		PrevHash:    decodeHex(candidate.MockPrevHash),
		Cbor:        []byte{0x80},
	}
}

// chainEmptiedToOrigin returns a chain that held blocks and was then rolled
// back to origin -- the state that makes the continuity checks inapplicable
// and that a peer with a stale chainsync cursor rolls forward into.
func chainEmptiedToOrigin(t *testing.T) *chain.Chain {
	t.Helper()
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	mustSetLedger(t, cm, 10)
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	if err := c.Rollback(ocommon.NewPointOrigin()); err != nil {
		t.Fatalf("unexpected error rolling back to origin: %s", err)
	}
	if tip := c.Tip(); len(tip.Point.Hash) != 0 {
		t.Fatalf(
			"expected an empty chain after rollback to origin, got tip %d.%x",
			tip.Point.Slot,
			tip.Point.Hash,
		)
	}
	if c.HeaderCount() != 0 {
		t.Fatalf(
			"expected no queued headers after rollback to origin, got %d",
			c.HeaderCount(),
		)
	}
	return c
}

func assertOriginResult(
	t *testing.T,
	tc originContinuityCase,
	op string,
	err error,
) {
	t.Helper()
	if !tc.wantReject {
		if err != nil {
			t.Fatalf(
				"%s: must accept the network's first block (number %d): %s",
				op,
				tc.blockNumber,
				err,
			)
		}
		return
	}
	if err == nil {
		t.Fatalf(
			"%s: accepted block number %d as the chain's first block; "+
				"the chain would grow with a missing prefix",
			op,
			tc.blockNumber,
		)
	}
	var notFitErr chain.BlockNotFitChainTipError
	if !errors.As(err, &notFitErr) {
		t.Fatalf(
			"%s: expected BlockNotFitChainTipError, got %T: %s",
			op,
			err,
			err,
		)
	}
}

// TestAddBlockHeaderAfterRollbackToOriginRequiresFirstBlock is the reported
// defect: a rollback to origin empties the chain mid-run, and the next roll
// forward from a peer whose chainsync cursor is still ahead must not be
// accepted as the chain's first block (issue #4202).
func TestAddBlockHeaderAfterRollbackToOriginRequiresFirstBlock(t *testing.T) {
	t.Parallel()

	for _, tc := range originContinuityCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := chainEmptiedToOrigin(t)
			assertOriginResult(
				t,
				tc,
				"AddBlockHeader after rollback to origin",
				c.AddBlockHeader(originCandidate(tc)),
			)
		})
	}
}

// TestAddBlockAfterRollbackToOriginRequiresFirstBlock covers the block-apply
// path (Chain.AddBlock -> addBlockLocked), which skipped the same check.
func TestAddBlockAfterRollbackToOriginRequiresFirstBlock(t *testing.T) {
	t.Parallel()

	for _, tc := range originContinuityCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := chainEmptiedToOrigin(t)
			assertOriginResult(
				t,
				tc,
				"AddBlock after rollback to origin",
				c.AddBlock(originCandidate(tc), nil),
			)
		})
	}
}

// TestAddRawBlockAfterRollbackToOriginRequiresFirstBlock covers the raw-block
// path (Chain.AddRawBlocks -> addRawBlockLocked), which skipped the same check.
func TestAddRawBlockAfterRollbackToOriginRequiresFirstBlock(t *testing.T) {
	t.Parallel()

	for _, tc := range originContinuityCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := chainEmptiedToOrigin(t)
			assertOriginResult(
				t,
				tc,
				"AddRawBlocks after rollback to origin",
				c.AddRawBlocks(
					[]chain.RawBlock{originCandidateRawBlock(tc)},
				),
			)
		})
	}
}

// TestFreshChainAcceptsFirstBlockFromAnySource pins the initial-sync side: a
// chain that has never been mutated has no anchor to check against -- it is
// the state the bulk block importer fills from a local immutable database --
// so it keeps accepting whatever it is given as its first block. Only a chain
// emptied back to origin is anchored.
func TestFreshChainAcceptsFirstBlockFromAnySource(t *testing.T) {
	t.Parallel()

	for _, tc := range originContinuityCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			cm, err := chain.NewManager(db, nil)
			if err != nil {
				t.Fatalf("unexpected error creating chain manager: %s", err)
			}
			if err := cm.PrimaryChain().AddBlockHeader(
				originCandidate(tc),
			); err != nil {
				t.Fatalf(
					"fresh chain must accept its first header (number %d): %s",
					tc.blockNumber,
					err,
				)
			}
			if err := cm.PrimaryChain().AddRawBlocks(
				[]chain.RawBlock{originCandidateRawBlock(tc)},
			); err != nil {
				t.Fatalf(
					"fresh chain must accept its first raw block (number %d): %s",
					tc.blockNumber,
					err,
				)
			}
		})
	}
}

// queuedFirstHeader is the chain's genuine first header (block number 0), the
// one a chain emptied back to origin legitimately accepts into its queue.
func queuedFirstHeader() *MockBlock {
	return &MockBlock{
		MockBlockNumber: 0,
		MockSlot:        3360,
		MockHash:        testHashPrefix + "beef",
	}
}

// rawBlockForHeader builds a raw block carrying the queued header's hash and
// slot but the caller's block number and prev hash, so a test can offer a
// block that matches the header by hash yet belongs further along the chain.
func rawBlockForHeader(
	header *MockBlock,
	blockNumber uint64,
	prevHash string,
) chain.RawBlock {
	return chain.RawBlock{
		Slot:        header.MockSlot,
		Hash:        header.Hash().Bytes(),
		BlockNumber: blockNumber,
		Type:        uint(header.Type()),
		PrevHash:    decodeHex(prevHash),
		Cbor:        []byte{0x80},
	}
}

func assertStillAtOriginWithQueuedHeader(t *testing.T, c *chain.Chain) {
	t.Helper()
	if tip := c.Tip(); len(tip.Point.Hash) != 0 {
		t.Fatalf(
			"chain advanced past origin on a rejected block: tip %d.%x",
			tip.Point.Slot,
			tip.Point.Hash,
		)
	}
	if got := c.HeaderCount(); got != 1 {
		t.Fatalf(
			"rejected block consumed the queued header: header count %d, want 1",
			got,
		)
	}
}

// TestAddRawBlocksAfterRollbackToOriginWithQueuedHeader pins the origin check
// to the chain's exported API rather than to the shape of the header queue.
// A queued header binds the next block's hash, not its block number, so the
// sequence rollback-to-origin -> AddBlockHeader(first header) ->
// AddRawBlocks(same hash, block number 2) must still be rejected: accepting it
// would delete the queued header and persist block number 2 as the chain's
// first block, leaving the missing prefix of issue #4202. The variant with no
// header queued is covered by
// TestAddRawBlockAfterRollbackToOriginRequiresFirstBlock.
func TestAddRawBlocksAfterRollbackToOriginWithQueuedHeader(t *testing.T) {
	t.Parallel()

	c := chainEmptiedToOrigin(t)
	header := queuedFirstHeader()
	if err := c.AddBlockHeader(header); err != nil {
		t.Fatalf("chain must accept its first header after rollback: %s", err)
	}
	if got := c.HeaderCount(); got != 1 {
		t.Fatalf("expected 1 queued header, got %d", got)
	}
	// Same hash as the queued header, but a block number from further along
	// the chain.
	err := c.AddRawBlocks(
		[]chain.RawBlock{
			rawBlockForHeader(header, 2, testHashPrefix+"0001"),
		},
	)
	if err == nil {
		t.Fatal(
			"AddRawBlocks accepted block number 2 as the chain's first block " +
				"because a header was queued; the chain would grow with a " +
				"missing prefix",
		)
	}
	var notFitErr chain.BlockNotFitChainTipError
	if !errors.As(err, &notFitErr) {
		t.Fatalf("expected BlockNotFitChainTipError, got %T: %s", err, err)
	}
	assertStillAtOriginWithQueuedHeader(t, c)
	// The consistent raw block for that same header is still accepted and
	// clears the queue.
	if err := c.AddRawBlocks(
		[]chain.RawBlock{rawBlockForHeader(header, 0, "")},
	); err != nil {
		t.Fatalf(
			"chain must accept the raw block matching its queued first header: %s",
			err,
		)
	}
	if got := c.HeaderCount(); got != 0 {
		t.Fatalf("accepted block left %d queued headers, want 0", got)
	}
	tip := c.Tip()
	if !bytes.Equal(tip.Point.Hash, header.Hash().Bytes()) {
		t.Fatalf(
			"tip hash %x after accepted block, want %x",
			tip.Point.Hash,
			header.Hash().Bytes(),
		)
	}
	if tip.BlockNumber != 0 {
		t.Fatalf("tip block number %d after accepted block, want 0", tip.BlockNumber)
	}
}

// TestAddBlockAfterRollbackToOriginUsesQueuedHeaderBlockNumber pins why the
// decoded-block path needs no separate guard for the same sequence: when a
// header is queued, addBlockLocked takes the block number (and prev hash) from
// that header, which the origin check above already anchored at queue time. So
// even a block whose own body claims a mid-chain number enters the chain as
// the header's block number 0, not as a truncated prefix. RawBlock carries a
// caller-supplied BlockNumber that is not derived from the queued header,
// which is why AddRawBlocks is checked directly.
func TestAddBlockAfterRollbackToOriginUsesQueuedHeaderBlockNumber(t *testing.T) {
	t.Parallel()

	c := chainEmptiedToOrigin(t)
	header := queuedFirstHeader()
	if err := c.AddBlockHeader(header); err != nil {
		t.Fatalf("chain must accept its first header after rollback: %s", err)
	}
	midChainBlock := &MockBlock{
		MockBlockNumber: 2,
		MockSlot:        header.MockSlot,
		MockHash:        header.MockHash,
		MockPrevHash:    testHashPrefix + "0001",
	}
	if err := c.AddBlock(midChainBlock, nil); err != nil {
		t.Fatalf(
			"block matching the queued first header must be accepted: %s",
			err,
		)
	}
	if got := c.HeaderCount(); got != 0 {
		t.Fatalf("accepted block left %d queued headers, want 0", got)
	}
	if tip := c.Tip(); tip.BlockNumber != 0 {
		t.Fatalf(
			"chain recorded block number %d, want the queued header's 0",
			tip.BlockNumber,
		)
	}
}

// TestAddBlockAfterRollbackToOriginRejectsChainShortOfBlockZero pins what an
// accepted number-1 first block would cost, which the table above does not: the
// ordinary contiguity check compares a candidate against the accepted tip, so a
// chain anchored at block number 1 stays self-consistent forever. Block 2
// chains onto block 1 and is accepted, and nothing downstream ever notices that
// block 0 is missing -- the chain is permanently short its first block, which
// is the same truncated prefix issue #4202 reports.
//
// Both halves are asserted here: the number-1 first block is rejected, and the
// number-2 block that would have cemented the short chain is rejected too,
// because the chain is still at origin. The positive control adds the same pair
// anchored at block number 0 and requires it to be accepted.
func TestAddBlockAfterRollbackToOriginRejectsChainShortOfBlockZero(
	t *testing.T,
) {
	t.Parallel()

	c := chainEmptiedToOrigin(t)
	// A peer whose chainsync cursor survived the rollback offers a block from
	// further along the chain. The prev hash is arbitrary: at origin there is
	// no tip to compare it against.
	short := &MockBlock{
		MockBlockNumber: 1,
		MockSlot:        3360,
		MockHash:        testHashPrefix + "beef",
		MockPrevHash:    testHashPrefix + "00a0",
	}
	err := c.AddBlock(short, nil)
	if err == nil {
		t.Fatal(
			"AddBlock accepted block number 1 as the chain's first block; " +
				"the chain is then permanently short block 0",
		)
	}
	var notFitErr chain.BlockNotFitChainTipError
	if !errors.As(err, &notFitErr) {
		t.Fatalf("expected BlockNotFitChainTipError, got %T: %s", err, err)
	}
	assertStillAtOrigin(t, c, "rejected number-1 first block")
	// This is the block that would make the gap permanent: it chains onto the
	// number-1 block, so blockNumberContiguous is satisfied and the missing
	// block 0 is never noticed again. It must be rejected as well, because the
	// chain is still at origin.
	next := &MockBlock{
		MockBlockNumber: 2,
		MockSlot:        3380,
		MockHash:        testHashPrefix + "cafe",
		MockPrevHash:    short.MockHash,
	}
	if err := c.AddBlock(next, nil); err == nil {
		t.Fatal(
			"AddBlock accepted block number 2 on top of a number-1 first " +
				"block; the chain is anchored at block number 1 and is " +
				"permanently short block 0",
		)
	}
	assertStillAtOrigin(t, c, "rejected number-2 second block")
	// Positive control: the same two-block sequence anchored at block number 0
	// is the chain the network actually has, and it is accepted.
	genesisBlock := &MockBlock{
		MockBlockNumber: 0,
		MockSlot:        3340,
		MockHash:        testHashPrefix + "0aa0",
	}
	if err := c.AddBlock(genesisBlock, nil); err != nil {
		t.Fatalf(
			"chain must accept the network's first block (number 0): %s",
			err,
		)
	}
	secondBlock := &MockBlock{
		MockBlockNumber: 1,
		MockSlot:        3360,
		MockHash:        testHashPrefix + "beef",
		MockPrevHash:    genesisBlock.MockHash,
	}
	if err := c.AddBlock(secondBlock, nil); err != nil {
		t.Fatalf("chain must accept block 1 on top of block 0: %s", err)
	}
	if tip := c.Tip(); tip.BlockNumber != 1 {
		t.Fatalf("tip block number %d, want 1", tip.BlockNumber)
	}
	if got := firstBlockNumberOnChain(t, c); got != 0 {
		t.Fatalf(
			"chain's first block is number %d, want 0: the chain is short "+
				"its first block",
			got,
		)
	}
}

// assertStillAtOrigin fails unless the chain is still empty, so a rejected
// candidate is proven not to have advanced the chain.
func assertStillAtOrigin(t *testing.T, c *chain.Chain, after string) {
	t.Helper()
	if tip := c.Tip(); len(tip.Point.Hash) != 0 {
		t.Fatalf(
			"chain advanced past origin after %s: tip %d.%x",
			after,
			tip.Point.Slot,
			tip.Point.Hash,
		)
	}
}

// firstBlockNumberOnChain returns the block number of the chain's first block,
// read back from origin rather than from the tip, so that a chain missing its
// prefix is visible.
func firstBlockNumberOnChain(t *testing.T, c *chain.Chain) uint64 {
	t.Helper()
	iter, err := c.FromPoint(ocommon.NewPointOrigin(), false)
	if err != nil {
		t.Fatalf("unexpected error creating chain iterator: %s", err)
	}
	defer iter.Cancel()
	next, err := iter.Next(false)
	if err != nil {
		t.Fatalf("unexpected error reading the chain's first block: %s", err)
	}
	if next == nil {
		t.Fatal("chain iterator returned no first block")
	}
	return next.Block.Number
}

// callerTxnChain builds a persistent primary chain holding the first four test
// blocks, each written and committed by the chain itself, and returns it with
// its database.
func callerTxnChain(t *testing.T) (*database.Database, *chain.Chain) {
	t.Helper()
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	// Larger than the chain this builds, so no rollback below is refused for
	// exceeding K and every one reaches its removal loop.
	mustSetLedger(t, cm, 100)
	c := cm.PrimaryChain()
	for i, testBlock := range testBlocks[:4] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("AddBlock(%d): %v", i, err)
		}
	}
	return db, c
}

// addOnCallerTxn adds testBlocks[4] through txn, leaving block index 5 on the
// in-memory chain and out of the store until txn concludes.
func addOnCallerTxn(
	t *testing.T,
	db *database.Database,
	c *chain.Chain,
) *database.Txn {
	t.Helper()
	txn := db.BlobTxn(true)
	if err := c.AddBlock(testBlocks[4], txn); err != nil {
		_ = txn.Rollback()
		t.Fatalf("AddBlock on caller transaction: %v", err)
	}
	if tip := c.Tip(); tip.Point.Slot != testBlocks[4].SlotNumber() {
		_ = txn.Rollback()
		t.Fatalf("in-memory tip did not advance: %+v", tip)
	}
	if _, err := db.BlockByIndex(5, nil); !errors.Is(
		err,
		models.ErrBlockNotFound,
	) {
		_ = txn.Rollback()
		t.Fatalf(
			"expected block index 5 to be invisible outside the caller transaction, got %v",
			err,
		)
	}
	return txn
}

// rollbackPoint is the point of testBlocks[2], block index 3: a rollback there
// removes indices 5 and 4, so its removal loop starts at the index the caller
// transaction still holds.
func rollbackPoint() ocommon.Point {
	return ocommon.Point{
		Slot: testBlocks[2].SlotNumber(),
		Hash: testBlocks[2].Hash().Bytes(),
	}
}

// TestRollbackWaitsForUncommittedCallerTransaction pins that a rollback never
// resolves a block index whose store write is still sitting in an uncommitted
// caller-supplied transaction.
//
// addBlockLocked writes the block through whichever transaction it is given and
// then advances c.tipBlockIndex under c.mutex. With a caller-supplied
// transaction the chain neither performs nor observes the commit, so between
// the tip advancing and the caller committing there is an index the in-memory
// chain legitimately holds and the store cannot serve:
// ChainManager.removeBlockByIndex opens its own transaction, and no transaction
// sees another's uncommitted writes. rollbackLocked's removal loop starts at
// c.tipBlockIndex, so without the barrier it fails its very first iteration
// with "remove block at index 5: block not found". Chain.batchCommitMutex
// closes this window for the batch transactions the chain owns and left it open
// here (issue #4005).
func TestRollbackWaitsForUncommittedCallerTransaction(t *testing.T) {
	db, c := callerTxnChain(t)
	txn := addOnCallerTxn(t, db, c)

	done := make(chan error, 1)
	go func() { done <- c.Rollback(rollbackPoint()) }()

	// The rollback must still be waiting: reaching its removal loop now is
	// exactly the defect, and it reports it as a not-found index rather than
	// by blocking.
	waitUntilGoroutineIn(t, "chain.(*pendingAddBarrier).awaitDrained")
	testutil.RequireNoReceive(
		t,
		done,
		50*time.Millisecond,
		"rollback resolved an uncommitted caller transaction",
	)

	if err := txn.Commit(); err != nil {
		t.Fatalf("commit caller transaction: %v", err)
	}

	if err := testutil.RequireReceive(
		t,
		done,
		30*time.Second,
		"rollback did not resume after the caller transaction committed",
	); err != nil {
		t.Fatalf("rollback after the caller transaction committed: %v", err)
	}

	if tip := c.Tip(); tip.Point.Slot != testBlocks[2].SlotNumber() {
		t.Fatalf("unexpected tip after rollback: %+v", tip)
	}
	for _, idx := range []uint64{4, 5} {
		if _, err := db.BlockByIndex(idx, nil); !errors.Is(
			err,
			models.ErrBlockNotFound,
		) {
			t.Fatalf("rollback left block index %d in the store: %v", idx, err)
		}
	}
}

// TestRollbackResumesWhenCallerTransactionRollsBack pins the failure mode of
// the release hook: a transaction that rolls back must free the barrier just as
// a commit does. Releasing from AfterCommit, which fires only on a durable
// commit, would strand the record and leave every later rollback waiting out
// the drain timeout.
func TestRollbackResumesWhenCallerTransactionRollsBack(t *testing.T) {
	db, c := callerTxnChain(t)
	txn := addOnCallerTxn(t, db, c)
	if err := txn.Rollback(); err != nil {
		t.Fatalf("rollback caller transaction: %v", err)
	}

	// The rollback reports the index the abandoned transaction never wrote;
	// what is pinned here is that it gets there at all. The drain timeout is
	// 30s, so a bound well inside it separates "released by the transaction
	// ending" from "waited the barrier out".
	done := make(chan error, 1)
	go func() { done <- c.Rollback(rollbackPoint()) }()
	if err := testutil.RequireReceive(
		t,
		done,
		15*time.Second,
		"rollback did not resume after the caller transaction rolled back",
	); err != nil {
		t.Fatalf("rollback after caller transaction rollback: %v", err)
	}
	if tip := c.Tip(); tip.Point.Slot != testBlocks[2].SlotNumber() {
		t.Fatalf("unexpected tip after rollback of caller transaction: %+v", tip)
	}
}

// TestStandaloneAddDoesNotBuildOnAbortedCallerAdd ensures a committed
// standalone add cannot extend a block that is still held by a caller
// transaction. The caller rollback restores the tip before the standalone add
// proceeds, so the dependent block is rejected against the restored parent.
func TestStandaloneAddDoesNotBuildOnAbortedCallerAdd(t *testing.T) {
	db, c := callerTxnChain(t)
	txn := addOnCallerTxn(t, db, c)

	result := make(chan error, 1)
	go func() { result <- c.AddBlock(testBlocks[5], nil) }()
	waitUntilGoroutineIn(t, "chain.(*pendingAddBarrier).awaitDrained")
	testutil.RequireNoReceive(
		t,
		result,
		50*time.Millisecond,
		"dependent standalone add completed before caller transaction",
	)

	if err := txn.Rollback(); err != nil {
		t.Fatalf("rollback caller transaction: %v", err)
	}
	if err := testutil.RequireReceive(
		t,
		result,
		5*time.Second,
		"dependent standalone add did not resume after caller transaction rollback",
	); err == nil {
		t.Fatal("dependent standalone add succeeded after caller transaction rollback")
	}
	if tip := c.Tip(); tip.Point.Slot != testBlocks[3].SlotNumber() {
		t.Fatalf("unexpected tip after dependent add: %+v", tip)
	}
}

// TestRejectedCallerAddDoesNotHideEarlierAdd ensures a rejected add does not
// leave a no-op snapshot in front of the successful add it follows. Otherwise
// rollback restoration stops at the rejected attempt and leaves memory ahead
// of the store when the transaction aborts.
func TestRejectedCallerAddDoesNotHideEarlierAdd(t *testing.T) {
	db, c := callerTxnChain(t)
	txn := addOnCallerTxn(t, db, c)
	if err := c.AddBlock(testBlocks[4], txn); err == nil {
		t.Fatalf("expected repeated caller add to fail")
	}
	if err := txn.Rollback(); err != nil {
		t.Fatalf("rollback caller transaction: %v", err)
	}
	if tip := c.Tip(); tip.Point.Slot != testBlocks[3].SlotNumber() {
		t.Fatalf("unexpected tip after rejected caller add: %+v", tip)
	}
}

// newForkChainFixture builds a persistent primary chain of primaryCount
// blocks and an ephemeral fork anchored at primary block index forkIdx+1,
// carrying forkCount blocks of its own.
func newForkChainFixture(
	t *testing.T,
	primaryCount, forkIdx, forkCount int,
) (primaryBlocks, forkBlocks []ledger.Block, forkChain *chain.Chain) {
	t.Helper()
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("NewManager: %s", err)
	}
	mustSetLedger(t, cm, 5)
	primaryChain := cm.PrimaryChain()

	var origin common.Blake2b256
	// Start at slot 20, not 0: rollbackLocked treats slot 0 as origin, so a
	// fixture block there would never resolve to a block index.
	primaryBlocks = generateTestChain(t, 1, origin, 20, 20, primaryCount)
	for i, b := range primaryBlocks {
		if err := primaryChain.AddBlock(b, nil); err != nil {
			t.Fatalf("AddBlock primary[%d]: %s", i, err)
		}
	}

	forkPoint := ocommon.Point{
		Slot: primaryBlocks[forkIdx].SlotNumber(),
		Hash: primaryBlocks[forkIdx].Hash().Bytes(),
	}
	forkChain, err = cm.NewChainFromIntersect([]ocommon.Point{forkPoint})
	if err != nil {
		t.Fatalf("NewChainFromIntersect: %s", err)
	}

	forkBlocks = generateTestChain(
		t,
		uint64(forkIdx+2), //nolint:gosec
		primaryBlocks[forkIdx].Hash(),
		primaryBlocks[forkIdx].SlotNumber()+20,
		20,
		forkCount,
	)
	for i, b := range forkBlocks {
		if err := forkChain.AddBlock(b, nil); err != nil {
			t.Fatalf("AddBlock fork[%d]: %s", i, err)
		}
	}
	return primaryBlocks, forkBlocks, forkChain
}

// TestChainRollbackEphemeralAtAndBeforeForkPoint covers rolling an ephemeral
// fork chain back to a point inside its own blocks, to its fork point, and to
// a common-prefix block before its fork point. The before-fork case walks the
// deletion loop onto the fork point itself, where the in-memory buffer holds
// no entry for the block; computing a buffer index there yields -1.
func TestChainRollbackEphemeralAtAndBeforeForkPoint(t *testing.T) {
	t.Parallel()

	const (
		primaryCount = 6
		forkIdx      = 2 // primary block index 3
		forkCount    = 3
	)

	for _, testDef := range []struct {
		name string
		// target picks the rollback point from the fixture's blocks.
		target func(primary, fork []ledger.Block) ledger.Block
		// want lists the blocks the chain must still deliver afterwards.
		want func(primary, fork []ledger.Block) []ledger.Block
	}{
		{
			name:   "within fork",
			target: func(_, fork []ledger.Block) ledger.Block { return fork[0] },
			want: func(primary, fork []ledger.Block) []ledger.Block {
				return append(slices.Clone(primary[:forkIdx+1]), fork[0])
			},
		},
		{
			name: "at fork point",
			target: func(primary, _ []ledger.Block) ledger.Block {
				return primary[forkIdx]
			},
			want: func(primary, _ []ledger.Block) []ledger.Block {
				return slices.Clone(primary[:forkIdx+1])
			},
		},
		{
			name: "one before fork point",
			target: func(primary, _ []ledger.Block) ledger.Block {
				return primary[forkIdx-1]
			},
			want: func(primary, _ []ledger.Block) []ledger.Block {
				return slices.Clone(primary[:forkIdx])
			},
		},
		{
			name: "two before fork point",
			target: func(primary, _ []ledger.Block) ledger.Block {
				return primary[forkIdx-2]
			},
			want: func(primary, _ []ledger.Block) []ledger.Block {
				return slices.Clone(primary[:forkIdx-1])
			},
		},
	} {
		t.Run(testDef.name, func(t *testing.T) {
			primaryBlocks, forkBlocks, forkChain := newForkChainFixture(
				t, primaryCount, forkIdx, forkCount,
			)
			target := testDef.target(primaryBlocks, forkBlocks)
			rollbackPoint := ocommon.Point{
				Slot: target.SlotNumber(),
				Hash: target.Hash().Bytes(),
			}
			if err := forkChain.Rollback(rollbackPoint); err != nil {
				t.Fatalf("Rollback: %s", err)
			}
			gotTip := forkChain.Tip()
			if gotTip.Point.Slot != target.SlotNumber() {
				t.Fatalf(
					"tip slot after rollback: got %d want %d",
					gotTip.Point.Slot, target.SlotNumber(),
				)
			}
			if gotTip.BlockNumber != target.BlockNumber() {
				t.Fatalf(
					"tip block number after rollback: got %d want %d",
					gotTip.BlockNumber, target.BlockNumber(),
				)
			}
			// The chain must remain usable: iterating from origin has to
			// deliver exactly the blocks up to the rollback target.
			iter, err := forkChain.FromPoint(ocommon.NewPointOrigin(), false)
			if err != nil {
				t.Fatalf("FromPoint: %s", err)
			}
			want := testDef.want(primaryBlocks, forkBlocks)
			for i, wantBlock := range want {
				next, err := iter.Next(false)
				if err != nil {
					t.Fatalf("iter.Next idx %d: %s", i, err)
				}
				if next == nil || next.Rollback {
					t.Fatalf("iter.Next idx %d unexpected: %+v", i, next)
				}
				if next.Block.Number != wantBlock.BlockNumber() {
					t.Fatalf(
						"iter idx %d block number: got %d want %d",
						i, next.Block.Number, wantBlock.BlockNumber(),
					)
				}
			}
			// Nothing may survive past the rollback target: a stale
			// in-memory buffer entry would surface as an extra block
			// here rather than as the chain tip.
			if _, err := iter.Next(false); !errors.Is(
				err, chain.ErrIteratorChainTip,
			) {
				t.Fatalf(
					"expected chain tip after %d blocks, got err %v",
					len(want), err,
				)
			}
		})
	}
}

// pendingCommitHash builds a distinct block hash for a label.
func pendingCommitHash(label string) []byte {
	sum := sha256.Sum256([]byte(label))
	return sum[:]
}

// TestRollbackDoesNotResolveUncommittedBlockIndex pins that a rollback never
// resolves a block index that a chain-owned batch has applied to the in-memory
// chain but not yet committed.
//
// addRawBlocks advances c.tipBlockIndex inside its transaction's closure,
// under c.mutex, and txn.Do commits only after that closure has returned and
// both chain locks are released. A rollback that took c.mutex in between read
// a tip index the store could not serve -- ChainManager.removeBlockByIndex
// opens its own transaction, which cannot see another transaction's
// uncommitted writes -- and rollbackLocked's removal loop failed its first
// iteration with "remove block at index N: block not found" at an index the
// chain legitimately held. That is issue #3979, observed on CI as an
// intermittent failure of
// ledger.TestWindowedRewindConvergesWhilePrimaryChainExtends, whose appender
// goroutine and windowed rewind are the same pairing.
//
// The rollback target is origin so the rollback reaches its removal loop
// through the shortest path available (rollbackPointBlock is skipped for
// origin), which is what makes the pre-fix window observable often enough to
// be a regression test rather than a lottery: without the batch-commit
// barrier this reports a not-found index in roughly half of the rounds below.
func TestRollbackDoesNotResolveUncommittedBlockIndex(t *testing.T) {
	t.Parallel()

	const (
		// Larger than any chain this test builds, so a rollback to origin is
		// never refused for exceeding K and every round exercises the
		// removal loop.
		securityParam = 5000
		// blockImportBatchSize, so the whole batch lands in one transaction.
		batch   = 50
		payload = 4 * 1024
		rounds  = 40
	)
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	mustSetLedger(t, cm, securityParam)
	pc := cm.PrimaryChain()

	cbor := make([]byte, payload)
	cbor[0] = 0x80
	seq := 0
	nextBatch := func(n int) []chain.RawBlock {
		tip := pc.Tip()
		out := make([]chain.RawBlock, 0, n)
		prev := tip.Point.Hash
		slot := tip.Point.Slot
		num := tip.BlockNumber
		// Every round after the first starts from a chain the rollback
		// emptied back to origin, and such a chain only accepts block
		// number 0 as its first block (see the origin continuity check in
		// chain.go), so the batch that regrows it must start there rather
		// than at 1. Later blocks increment as usual.
		atOrigin := len(tip.Point.Hash) == 0
		for i := range n {
			seq++
			slot++
			if !atOrigin || i > 0 {
				num++
			}
			hash := pendingCommitHash(fmt.Sprintf("pending-commit-%d", seq))
			out = append(out, chain.RawBlock{
				Slot:        slot,
				Hash:        hash,
				BlockNumber: num,
				Type:        1,
				PrevHash:    prev,
				Cbor:        cbor,
			})
			prev = hash
		}
		return out
	}

	for round := range rounds {
		blocks := nextBatch(batch)
		applying := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once
		var wg sync.WaitGroup
		wg.Go(func() {
			// The callback runs inside the batch transaction, under both
			// chain locks, so it marks the point at which the in-memory
			// chain has started moving ahead of the store.
			if err := pc.AddRawBlocksWithCallback(
				blocks,
				func(_ chain.RawBlock, _ *database.Txn) error {
					once.Do(func() {
						close(applying)
						<-release
					})
					return nil
				},
			); err != nil {
				t.Errorf("round %d: AddRawBlocksWithCallback: %v", round, err)
			}
		})
		<-applying
		var rollbackErr error
		wg.Go(func() {
			rollbackErr = pc.Rollback(ocommon.Point{})
		})
		// Release the batch only once the rollback is queued on the lock it
		// holds, so the rollback runs in the window between that lock being
		// handed over and the batch's transaction committing.
		waitUntilParkedIn(t, "chain.(*Chain).rollbackLocked")
		close(release)
		wg.Wait()
		if errors.Is(rollbackErr, models.ErrBlockNotFound) {
			t.Fatalf(
				"round %d: rollback resolved a block index the batch had not committed: %v",
				round,
				rollbackErr,
			)
		}
		if rollbackErr != nil {
			t.Fatalf("round %d: rollback: %v", round, rollbackErr)
		}
	}
}

// TestAddBlocksRestoresMemoryAfterBatchFailure ensures a failed AddBlocks
// transaction cannot leave the in-memory tip ahead of the durable store.
// The first three blocks fit; the fourth has a mismatched parent, so the
// transaction rolls back after the chain has already advanced in memory.
func TestAddBlocksRestoresMemoryAfterBatchFailure(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	mustSetLedger(t, cm, 100)
	pc := cm.PrimaryChain()

	invalid := *testBlocks[3]
	invalid.MockPrevHash = testHashPrefix + "ffff"
	blocks := []ledger.Block{
		testBlocks[0],
		testBlocks[1],
		testBlocks[2],
		&invalid,
	}
	if err := pc.AddBlocks(blocks); err == nil {
		t.Fatal("expected AddBlocks to reject a mismatched parent")
	}

	tip := pc.Tip()
	if tip.Point.Slot != 0 || len(tip.Point.Hash) != 0 || tip.BlockNumber != 0 {
		t.Fatalf("failed batch advanced in-memory tip: %+v", tip)
	}
	if _, err := db.BlockByIndex(3, nil); !errors.Is(
		err,
		models.ErrBlockNotFound,
	) {
		t.Fatalf(
			"expected failed batch block to be absent from storage, got %v",
			err,
		)
	}
	if err := pc.Rollback(ocommon.Point{}); err != nil {
		t.Fatalf("rollback after failed batch: %v", err)
	}
}

// slotZeroHashPoint is a rollback point carrying a real 32-byte hash at slot 0.
// Slot 0 is a legitimate slot -- it is where a Byron-era genesis-adjacent point
// sits -- so a point is not "empty" merely because its slot is zero. Only a
// point with neither a slot nor a hash is.
func slotZeroHashPoint() ocommon.Point {
	hash, err := hex.DecodeString(
		"00004744abababababababababababababababababababababababab5107",
	)
	if err != nil {
		panic(err)
	}
	// Pad to the 32 bytes a block hash occupies.
	full := make([]byte, 32)
	copy(full, hash)
	return ocommon.Point{Slot: 0, Hash: full}
}

// TestValidateRollbackChecksMembershipOfSlotZeroHashPoint covers the gate on
// the rollback-point lookup. Both ValidateRollback and rollbackLocked resolved
// the point only when point.Slot > 0, so a point at slot 0 carrying a hash the
// chain does not hold skipped rollbackPointBlock entirely and was reported as
// valid.
func TestValidateRollbackChecksMembershipOfSlotZeroHashPoint(t *testing.T) {
	db := newTestDB(t)
	c := buildSlotZeroTestChain(t, db)

	err := c.ValidateRollback(slotZeroHashPoint())
	if err == nil {
		t.Fatal(
			"expected ValidateRollback to reject a slot-0 point whose hash " +
				"this chain does not hold; a hash-bearing point must have its " +
				"membership checked regardless of slot",
		)
	}
	if !errors.Is(err, models.ErrBlockNotFound) {
		t.Fatalf("expected ErrBlockNotFound, got: %s", err)
	}
}

// TestRollbackChecksMembershipOfSlotZeroHashPoint drives the same gap through
// the mutating path. Without the fix the lookup is skipped, rollbackBlockIndex
// stays 0, the fork depth is computed against index zero, and the rollback
// deletes the chain tail while setting currentTip to a point whose block the
// chain does not retain.
func TestRollbackChecksMembershipOfSlotZeroHashPoint(t *testing.T) {
	db := newTestDB(t)
	c := buildSlotZeroTestChain(t, db)
	tipBefore := c.Tip()

	err := c.Rollback(slotZeroHashPoint())
	if err == nil {
		t.Fatal(
			"expected Rollback to reject a slot-0 point whose hash this " +
				"chain does not hold",
		)
	}
	if !errors.Is(err, models.ErrBlockNotFound) {
		t.Fatalf("expected ErrBlockNotFound, got: %s", err)
	}

	tip := c.Tip()
	if tip.Point.Slot != tipBefore.Point.Slot ||
		!bytes.Equal(tip.Point.Hash, tipBefore.Point.Hash) {
		t.Fatalf(
			"a rejected rollback must leave the tip untouched: got %d/%s, "+
				"want %d/%s",
			tip.Point.Slot,
			hex.EncodeToString(tip.Point.Hash),
			tipBefore.Point.Slot,
			hex.EncodeToString(tipBefore.Point.Hash),
		)
	}
	if tip.BlockNumber != tipBefore.BlockNumber {
		t.Fatalf(
			"a rejected rollback must not move the block number: got %d, want %d",
			tip.BlockNumber,
			tipBefore.BlockNumber,
		)
	}
}

// TestRollbackStillAcceptsGenuineEmptyPoint pins the case the slot > 0 gate was
// there to serve: a point with neither slot nor hash names no block, so it must
// still skip the lookup rather than fail it.
func TestRollbackStillAcceptsGenuineEmptyPoint(t *testing.T) {
	db := newTestDB(t)
	c := buildSlotZeroTestChain(t, db)

	if err := c.ValidateRollback(ocommon.Point{}); err != nil {
		t.Fatalf(
			"an empty point names no block and must not require a lookup: %s",
			err,
		)
	}
}

func buildSlotZeroTestChain(t *testing.T, db *database.Database) *chain.Chain {
	t.Helper()
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	mustSetLedger(t, cm, 100)
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks[:4] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block: %s", err)
		}
	}
	return c
}

// TestIteratorResumesAfterSlotZeroRollbackPoint covers the member of this class
// that the fix above makes reachable. testBlocks[0] sits at slot 0 with a real
// hash, so once a hash-bearing slot-0 point resolves properly it can reach the
// iterator's pending-rollback branch -- which gated the same way and would
// otherwise treat it as "rolling back to origin" and replay the chain from
// genesis instead of resuming after the rolled-back-to block.
func TestIteratorResumesAfterSlotZeroRollbackPoint(t *testing.T) {
	db := newTestDB(t)
	c := buildSlotZeroTestChain(t, db)

	genesisPoint := mockBlockPoint(testBlocks[0])
	if genesisPoint.Slot != 0 {
		t.Fatalf(
			"fixture precondition: expected slot 0, got %d",
			genesisPoint.Slot,
		)
	}
	if len(genesisPoint.Hash) == 0 {
		t.Fatal("fixture precondition: expected a non-empty hash at slot 0")
	}

	iter, err := c.FromPoint(genesisPoint, true)
	if err != nil {
		t.Fatalf("unexpected error creating iterator: %s", err)
	}
	// Advance the iterator past the rollback target so the rollback is queued
	// against it rather than landing where it already sits.
	for range 3 {
		if _, err := iter.Next(false); err != nil {
			t.Fatalf("unexpected error advancing iterator: %s", err)
		}
	}

	if err := c.Rollback(genesisPoint); err != nil {
		t.Fatalf("rollback to a real slot-0 block must succeed: %s", err)
	}

	res, err := iter.Next(false)
	if err != nil {
		t.Fatalf("unexpected iterator error: %s", err)
	}
	if res == nil {
		t.Fatal("expected a rollback result from the iterator")
	}
	if !res.Rollback {
		t.Fatal("expected the pending rollback to be delivered first")
	}
	if res.Point.Slot != genesisPoint.Slot ||
		!bytes.Equal(res.Point.Hash, genesisPoint.Hash) {
		t.Fatalf(
			"rollback point mismatch: got %d/%s, want %d/%s",
			res.Point.Slot,
			hex.EncodeToString(res.Point.Hash),
			genesisPoint.Slot,
			hex.EncodeToString(genesisPoint.Hash),
		)
	}

	// The rollback truncated the chain to the target, so resuming correctly
	// means the iterator sits *after* it. Add a fresh block and require that
	// the iterator delivers that one -- not testBlocks[0] replayed, which is
	// what resetting to the initial block index would produce.
	nextBlock := &MockBlock{
		MockBlockNumber: 2,
		MockSlot:        21,
		MockHash:        spliceForkHashPrefix + "0021",
		MockPrevHash:    testBlocks[0].MockHash,
	}
	if err := c.AddBlock(nextBlock, nil); err != nil {
		t.Fatalf("unexpected error adding block after rollback: %s", err)
	}

	next, err := iter.Next(false)
	if err != nil {
		t.Fatalf("unexpected iterator error: %s", err)
	}
	if next == nil {
		t.Fatal("expected the iterator to deliver the block after the rollback")
	}
	if next.Block.Slot == genesisPoint.Slot {
		t.Fatal(
			"iterator replayed the slot-0 rollback target instead of resuming " +
				"after it: the pending-rollback branch treated a hash-bearing " +
				"slot-0 point as a rollback to origin",
		)
	}
	if next.Block.Slot != nextBlock.MockSlot {
		t.Fatalf(
			"iterator must resume after the slot-0 rollback point: got slot "+
				"%d, want %d",
			next.Block.Slot,
			nextBlock.MockSlot,
		)
	}
}

// blockPoint builds the chain point for a mock test block.
func blockPoint(b *MockBlock) ocommon.Point {
	return ocommon.Point{
		Slot: b.SlotNumber(),
		Hash: b.Hash().Bytes(),
	}
}

// TestFromPointRejectsRolledBackPointPersistent verifies that a forward
// iterator cannot be created from a point that this chain rolled back.
//
// Rolling back removes the block row but retains the block in the manager's
// LRU cache so non-primary chains can still reconcile against it, which keeps
// the block resolvable by point long after it left the chain. An iterator
// built from such a point is positioned at an index the chain no longer has,
// so it can never yield the block the caller asked for. Blockfetch turns that
// into a StartBatch/BatchDone pair carrying no blocks, which the requesting
// peer cannot distinguish from a served range, so it re-requests the same
// single-block range forever instead of trying another peer.
func TestFromPointRejectsRolledBackPointPersistent(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	mustSetLedger(t, cm, 100)
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	rolledBackPoint := blockPoint(testBlocks[len(testBlocks)-1])
	survivingPoint := blockPoint(testBlocks[len(testBlocks)-2])
	if err := c.Rollback(survivingPoint); err != nil {
		t.Fatalf("unexpected error rolling back chain: %s", err)
	}
	// Precondition for the wedge: the rolled-back block is still
	// resolvable by point out of the retained block cache.
	if _, err := c.BlockByPoint(rolledBackPoint, nil); err != nil {
		t.Fatalf(
			"expected rolled-back block to stay resolvable by point: %s",
			err,
		)
	}
	iter, err := c.FromPoint(rolledBackPoint, true)
	if err == nil {
		iter.Cancel()
		t.Fatal(
			"expected FromPoint to reject a start point this chain rolled back",
		)
	}
	if !errors.Is(err, models.ErrBlockNotFound) {
		t.Fatalf("expected ErrBlockNotFound, got: %s", err)
	}
	// A reverse iterator shares the same start-point resolution and must
	// reject the rolled-back point too.
	revIter, err := c.FromPointReverse(rolledBackPoint, true)
	if err == nil {
		revIter.Cancel()
		t.Fatal(
			"expected FromPointReverse to reject a rolled-back start point",
		)
	}
	if !errors.Is(err, models.ErrBlockNotFound) {
		t.Fatalf("expected ErrBlockNotFound from reverse, got: %s", err)
	}
	// The surviving tip must remain iterable: the guard rejects points the
	// chain dropped, not points it still holds.
	tipIter, err := c.FromPoint(survivingPoint, true)
	if err != nil {
		t.Fatalf("unexpected error iterating from surviving tip: %s", err)
	}
	defer tipIter.Cancel()
	next, err := tipIter.Next(false)
	if err != nil {
		t.Fatalf("unexpected error reading surviving tip block: %s", err)
	}
	if next == nil || next.Point.Slot != survivingPoint.Slot {
		t.Fatalf(
			"expected surviving tip block at slot %d, got %+v",
			survivingPoint.Slot,
			next,
		)
	}
}

// TestFromPointRejectsRolledBackPointInMemory covers the same guard on a
// non-persistent chain, where rolled-back blocks stay in the manager's LRU
// cache instead of a database.
func TestFromPointRejectsRolledBackPointInMemory(t *testing.T) {
	t.Parallel()

	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	rolledBackPoint := blockPoint(testBlocks[len(testBlocks)-1])
	survivingPoint := blockPoint(testBlocks[len(testBlocks)-2])
	if err := c.Rollback(survivingPoint); err != nil {
		t.Fatalf("unexpected error rolling back chain: %s", err)
	}
	iter, err := c.FromPoint(rolledBackPoint, true)
	if err == nil {
		iter.Cancel()
		t.Fatal(
			"expected FromPoint to reject a start point this chain rolled back",
		)
	}
	if !errors.Is(err, models.ErrBlockNotFound) {
		t.Fatalf("expected ErrBlockNotFound, got: %s", err)
	}
}

// TestFromPointAcceptsCommonPointOnInMemoryFork verifies that a fork iterator
// can start at its in-memory primary chain intersection. The common prefix is
// not stored in the fork's blocks slice, and an in-memory manager has no
// database index for blockByIndex to query directly.
func TestFromPointAcceptsCommonPointOnInMemoryFork(t *testing.T) {
	t.Parallel()

	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	primary := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := primary.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding primary block: %s", err)
		}
	}

	commonPoint := blockPoint(testBlocks[1])
	fork, err := cm.NewChain(commonPoint)
	if err != nil {
		t.Fatalf("unexpected error creating fork: %s", err)
	}
	if err := fork.AddBlock(testBlocks[2], nil); err != nil {
		t.Fatalf("unexpected error extending fork: %s", err)
	}
	// The fork tail remains indexed in the fork itself; the membership guard
	// must continue to accept it while resolving the common prefix via primary.
	tailPoint := blockPoint(testBlocks[2])
	tailIter, err := fork.FromPoint(tailPoint, true)
	if err != nil {
		t.Fatalf("unexpected error creating tail iterator: %s", err)
	}
	next, err := tailIter.Next(false)
	tailIter.Cancel()
	if err != nil || next == nil || next.Point.Slot != tailPoint.Slot ||
		!bytes.Equal(next.Point.Hash, tailPoint.Hash) {
		t.Fatalf("expected fork tail point, got result=%+v err=%v", next, err)
	}

	for _, test := range []struct {
		name      string
		reverse   bool
		inclusive bool
		want      *MockBlock
	}{
		{name: "forward inclusive", inclusive: true, want: testBlocks[1]},
		{name: "forward exclusive", inclusive: false, want: testBlocks[2]},
		{name: "reverse inclusive", reverse: true, inclusive: true, want: testBlocks[1]},
		{name: "reverse exclusive", reverse: true, inclusive: false, want: testBlocks[0]},
	} {
		t.Run(test.name, func(t *testing.T) {
			var (
				iter *chain.ChainIterator
				err  error
			)
			if test.reverse {
				iter, err = fork.FromPointReverse(commonPoint, test.inclusive)
			} else {
				iter, err = fork.FromPoint(commonPoint, test.inclusive)
			}
			if err != nil {
				t.Fatalf("unexpected error creating iterator: %s", err)
			}
			defer iter.Cancel()
			next, err := iter.Next(false)
			if err != nil {
				t.Fatalf("unexpected error reading common point: %s", err)
			}
			want := blockPoint(test.want)
			if next == nil || next.Rollback || next.Point.Slot != want.Slot ||
				!bytes.Equal(next.Point.Hash, want.Hash) {
				t.Fatalf("expected %v, got %+v", want, next)
			}
		})
	}

	// The same point remains in the cache after the primary rollback, but it
	// is no longer part of the primary chain and must still be rejected.
	if err := primary.Rollback(blockPoint(testBlocks[0])); err != nil {
		t.Fatalf("unexpected primary rollback error: %s", err)
	}
	if _, err := fork.FromPoint(commonPoint, true); !errors.Is(
		err,
		models.ErrBlockNotFound,
	) {
		t.Fatalf(
			"expected rolled-back common point to be rejected, got: %v",
			err,
		)
	}
}

// TestTipPredecessorReportsAlternativeBlockContext covers the context an
// equal-slot alternative is built on: the tip that would be competed with, and
// the tip's immediate predecessor as the alternative's parent. This is what
// ouroboros-consensus' mkCurrentBlockContext returns for its EQ case.
func TestTipPredecessorReportsAlternativeBlockContext(t *testing.T) {
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks[:3] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}

	parent, tip, ok := c.TipPredecessor()
	if !ok {
		t.Fatal("expected a resolvable tip predecessor")
	}
	wantParent := testBlocks[1]
	wantTip := testBlocks[2]
	if parent.Slot != wantParent.MockSlot ||
		!bytes.Equal(parent.Hash, wantParent.Hash().Bytes()) {
		t.Fatalf(
			"parent = %d.%x, want %d.%s",
			parent.Slot,
			parent.Hash,
			wantParent.MockSlot,
			wantParent.MockHash,
		)
	}
	if tip.BlockNumber != wantTip.MockBlockNumber ||
		!bytes.Equal(tip.Point.Hash, wantTip.Hash().Bytes()) {
		t.Fatalf(
			"tip = %d/%x, want %d/%s",
			tip.BlockNumber,
			tip.Point.Hash,
			wantTip.MockBlockNumber,
			wantTip.MockHash,
		)
	}
	// The whole point of the context: our block's parent slot is strictly
	// below the slot we would forge at, which is exactly what binding the
	// live tip as parent fails to give at an equal slot.
	if parent.Slot >= tip.Point.Slot {
		t.Fatalf(
			"parent slot %d must be below the contested slot %d",
			parent.Slot,
			tip.Point.Slot,
		)
	}
}

// TestTipPredecessorRefusesWithoutAResolvablePredecessor pins the fail-closed
// contract. A caller that treated !ok as "use the live tip" would sign a block
// whose parent slot equals its own.
func TestTipPredecessorRefusesWithoutAResolvablePredecessor(t *testing.T) {
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()

	if _, _, ok := c.TipPredecessor(); ok {
		t.Fatal("empty chain must not report a tip predecessor")
	}

	if err := c.AddBlock(testBlocks[0], nil); err != nil {
		t.Fatalf("unexpected error adding block to chain: %s", err)
	}
	if _, _, ok := c.TipPredecessor(); ok {
		t.Fatal(
			"first block on the chain must not report a tip predecessor",
		)
	}
}

// TestTipPredecessorRefusesAParentAtOrAboveTheContestedSlot pins the strict
// slot-ordering half of the contract. The alternative built on this context is
// signed against the parent it names, so a parent whose slot is not strictly
// below the contested tip's would produce a block that Praos ordering and
// ledger envelope validation both reject. The chain does not enforce
// increasing slots on add, so this state is reachable; the answer is "no
// context", never a context the signer cannot use.
func TestTipPredecessorRefusesAParentAtOrAboveTheContestedSlot(t *testing.T) {
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks[:3] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	if _, _, ok := c.TipPredecessor(); !ok {
		t.Fatal("expected a resolvable tip predecessor before the same-slot tip")
	}
	// A tip that reuses its parent's slot. Its predecessor is resolvable and
	// the hash linkage is intact, so only the slot check can reject it.
	sameSlotTip := &MockBlock{
		MockBlockNumber: testBlocks[2].MockBlockNumber + 1,
		MockSlot:        testBlocks[2].MockSlot,
		MockHash:        testHashPrefix + "00fe",
		MockPrevHash:    testBlocks[2].MockHash,
	}
	if err := c.AddBlock(sameSlotTip, nil); err != nil {
		t.Fatalf("unexpected error adding same-slot block: %s", err)
	}
	if parent, tip, ok := c.TipPredecessor(); ok {
		t.Fatalf(
			"expected no context for a tip at its parent's slot, got parent %d.%x tip %d",
			parent.Slot,
			parent.Hash,
			tip.Point.Slot,
		)
	}
}

// TestAddLocalBlockDeferredAdoptsSiblingAfterRollback exercises the two chain
// operations an equal-slot alternative needs in sequence: roll the contested
// block off the tip, then adopt the locally forged sibling that shares its
// parent and block number. The sibling is not an extension of the tip the
// chain had when it was forged, which is precisely why AddLocalBlock's
// extend-only path cannot take it.
func TestAddLocalBlockDeferredAdoptsSiblingAfterRollback(t *testing.T) {
	eventBus := event.NewEventBus(nil, nil)
	cm, err := chain.NewManager(nil, eventBus)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	mustSetLedger(t, cm, 100)
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks[:3] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	rival := testBlocks[2]
	parent, tip, ok := c.TipPredecessor()
	if !ok {
		t.Fatal("expected a resolvable tip predecessor")
	}

	// Our alternative: same slot and block number as the rival at the tip,
	// the rival's predecessor as parent.
	sibling := &MockBlock{
		MockBlockNumber: tip.BlockNumber,
		MockSlot:        tip.Point.Slot,
		MockHash:        testHashPrefix + "0aa1",
		MockPrevHash:    testBlocks[1].MockHash,
	}

	// Extending the live tip is refused, which is the blocker this pair of
	// operations exists to get past.
	if err := c.AddLocalBlock(sibling); err == nil {
		t.Fatal("expected the sibling to be refused as a tip extension")
	}

	if _, err := c.RollbackDeferred(parent); err != nil {
		t.Fatalf("unexpected error rolling back to the sibling parent: %s", err)
	}
	evt, err := c.AddLocalBlockDeferred(sibling)
	if err != nil {
		t.Fatalf("unexpected error adopting local sibling: %s", err)
	}
	if evt.Type == "" {
		t.Fatal("expected a chain.update event for the adopted sibling")
	}

	newTip := c.Tip()
	if !bytes.Equal(newTip.Point.Hash, sibling.Hash().Bytes()) {
		t.Fatalf(
			"tip = %x, want the adopted sibling %s",
			newTip.Point.Hash,
			sibling.MockHash,
		)
	}
	if newTip.BlockNumber != rival.MockBlockNumber {
		t.Fatalf(
			"adopted sibling block number = %d, want the rival's %d",
			newTip.BlockNumber,
			rival.MockBlockNumber,
		)
	}
	if newTip.Point.Slot != rival.MockSlot {
		t.Fatalf(
			"adopted sibling slot = %d, want the contested slot %d",
			newTip.Point.Slot,
			rival.MockSlot,
		)
	}

	// The context now describes the newly adopted tip, so a second contest
	// at the same slot would build on the same parent again.
	parentAfter, tipAfter, ok := c.TipPredecessor()
	if !ok {
		t.Fatal("expected a resolvable tip predecessor after adoption")
	}
	if !bytes.Equal(parentAfter.Hash, parent.Hash) {
		t.Fatalf(
			"parent after adoption = %x, want the unchanged fork point %x",
			parentAfter.Hash,
			parent.Hash,
		)
	}
	if !bytes.Equal(tipAfter.Point.Hash, sibling.Hash().Bytes()) {
		t.Fatalf(
			"tip after adoption = %x, want %s",
			tipAfter.Point.Hash,
			sibling.MockHash,
		)
	}
	// Both mutations were deferred, so nothing was published inline; the
	// caller drains them once its ledger mutex is released.
	c.PublishPendingChainUpdates()
}

// TestIsFirstOnHeaderChain pins the anchor Byron header validation uses to
// decide "first block of a from-genesis chain": only the head of the queue at
// an origin primary tip is first, and an applied block or a rollback to origin
// changes the answer.
func TestIsFirstOnHeaderChain(t *testing.T) {
	t.Parallel()

	first := &MockBlock{
		MockBlockNumber: 0,
		MockSlot:        0,
		MockHash:        testHashPrefix + "0f01",
		MockPrevHash:    "",
	}
	second := &MockBlock{
		MockBlockNumber: 1,
		MockSlot:        0,
		MockHash:        testHashPrefix + "0f02",
		MockPrevHash:    first.MockHash,
	}
	firstHash := first.Hash().Bytes()
	secondHash := second.Hash().Bytes()

	var nilChain *chain.Chain
	if nilChain.IsFirstOnHeaderChain(firstHash) {
		t.Fatal("nil chain must not report a first header")
	}

	t.Run("empty queue at origin", func(t *testing.T) {
		t.Parallel()
		c := chainEmptiedToOrigin(t)
		if !c.IsFirstOnHeaderChain(firstHash) {
			t.Fatal("any header is first when nothing is queued at origin")
		}
	})

	t.Run("queued header stays first, successor is not", func(t *testing.T) {
		t.Parallel()
		c := chainEmptiedToOrigin(t)
		if err := c.AddBlockHeader(first); err != nil {
			t.Fatalf("add first header: %s", err)
		}
		if err := c.AddBlockHeader(second); err != nil {
			t.Fatalf("add second header: %s", err)
		}
		if !c.IsFirstOnHeaderChain(firstHash) {
			t.Fatal("queued head header must still be first")
		}
		if c.IsFirstOnHeaderChain(secondHash) {
			t.Fatal("successor of a queued header must not be first")
		}
	})

	t.Run("rollback to origin drops the queue", func(t *testing.T) {
		t.Parallel()
		c := chainEmptiedToOrigin(t)
		if err := c.AddBlockHeader(first); err != nil {
			t.Fatalf("add first header: %s", err)
		}
		if err := c.Rollback(ocommon.NewPointOrigin()); err != nil {
			t.Fatalf("rollback to origin: %s", err)
		}
		if !c.IsFirstOnHeaderChain(secondHash) {
			t.Fatal("queue must be empty again after rollback to origin")
		}
	})

	t.Run("applied block is never first", func(t *testing.T) {
		t.Parallel()
		db := newTestDB(t)
		cm, err := chain.NewManager(db, nil)
		if err != nil {
			t.Fatalf("chain manager: %s", err)
		}
		mustSetLedger(t, cm, 10)
		c := cm.PrimaryChain()
		if err := c.AddBlock(testBlocks[0], nil); err != nil {
			t.Fatalf("add block: %s", err)
		}
		if c.IsFirstOnHeaderChain(firstHash) {
			t.Fatal("no header is first once the primary tip has a block")
		}
	})
}
