// Copyright 2025 Blink Labs Software
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

package chain

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockfixtures "github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

func TestPersistentIteratorDrainsAlreadyAheadPrimaryChainAcrossSparseIndex(
	t *testing.T,
) {
	db, err := database.New(&database.Config{
		DataDir:        t.TempDir(),
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	blocks := []models.Block{
		{
			ID:     initialBlockIndex,
			Slot:   10,
			Hash:   bytes.Repeat([]byte{0x01}, 32),
			Number: 1,
			Type:   1,
			Cbor:   []byte{0x80},
		},
		{
			ID:       initialBlockIndex + 1,
			Slot:     20,
			Hash:     bytes.Repeat([]byte{0x02}, 32),
			PrevHash: bytes.Repeat([]byte{0x01}, 32),
			Number:   2,
			Type:     1,
			Cbor:     []byte{0x80},
		},
		{
			ID:       initialBlockIndex + 3,
			Slot:     30,
			Hash:     bytes.Repeat([]byte{0x03}, 32),
			PrevHash: bytes.Repeat([]byte{0x02}, 32),
			Number:   3,
			Type:     1,
			Cbor:     []byte{0x80},
		},
		{
			ID:       initialBlockIndex + 4,
			Slot:     40,
			Hash:     bytes.Repeat([]byte{0x04}, 32),
			PrevHash: bytes.Repeat([]byte{0x03}, 32),
			Number:   4,
			Type:     1,
			Cbor:     []byte{0x80},
		},
	}
	for _, block := range blocks {
		require.NoError(t, db.BlockCreate(block, nil))
	}

	cm, err := NewManager(db, nil)
	require.NoError(t, err)
	c := cm.PrimaryChain()
	require.Equal(t, blocks[len(blocks)-1].Slot, c.Tip().Point.Slot)

	iter, err := c.FromPoint(
		ocommon.NewPoint(blocks[1].Slot, blocks[1].Hash),
		false,
	)
	require.NoError(t, err)
	defer iter.Cancel()

	next, err := iter.Next(false)
	require.NoError(t, err)
	require.Equal(t, blocks[2].Slot, next.Point.Slot)
	require.Equal(t, blocks[2].Hash, next.Point.Hash)

	next, err = iter.Next(false)
	require.NoError(t, err)
	require.Equal(t, blocks[3].Slot, next.Point.Slot)
	require.Equal(t, blocks[3].Hash, next.Point.Hash)

	next, err = iter.Next(false)
	require.Nil(t, next)
	require.ErrorIs(t, err, ErrIteratorChainTip)
}

func TestPersistentIteratorRejectsSparseIndexWithHashDiscontinuity(
	t *testing.T,
) {
	db, err := database.New(&database.Config{
		DataDir:        t.TempDir(),
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ledgerTipHash := bytes.Repeat([]byte{0x02}, 32)
	blocks := []models.Block{
		{
			ID:     initialBlockIndex,
			Slot:   10,
			Hash:   bytes.Repeat([]byte{0x01}, 32),
			Number: 1,
			Type:   1,
			Cbor:   []byte{0x80},
		},
		{
			ID:       initialBlockIndex + 1,
			Slot:     20,
			Hash:     ledgerTipHash,
			PrevHash: bytes.Repeat([]byte{0x01}, 32),
			Number:   2,
			Type:     1,
			Cbor:     []byte{0x80},
		},
		{
			ID:       initialBlockIndex + 3,
			Slot:     30,
			Hash:     bytes.Repeat([]byte{0x03}, 32),
			PrevHash: bytes.Repeat([]byte{0xff}, 32),
			Number:   3,
			Type:     1,
			Cbor:     []byte{0x80},
		},
	}
	for _, block := range blocks {
		require.NoError(t, db.BlockCreate(block, nil))
	}

	cm, err := NewManager(db, nil)
	require.NoError(t, err)
	c := cm.PrimaryChain()

	iter, err := c.FromPoint(ocommon.NewPoint(blocks[1].Slot, ledgerTipHash), false)
	require.NoError(t, err)
	defer iter.Cancel()

	next, err := iter.Next(false)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrIteratorChainTip)
	require.False(t, next != nil && next.Point.Slot == blocks[2].Slot)
}

func TestIteratorCancelRemovesFromChain(t *testing.T) {
	// Create a chain manager without a database (in-memory only)
	eventBus := event.NewEventBus(nil, nil)
	cm, err := NewManager(nil, eventBus)
	require.NoError(t, err)

	// Get the primary chain
	c := cm.PrimaryChain()
	require.NotNil(t, c)

	// Initially no iterators
	c.mutex.RLock()
	initialLen := len(c.iterators)
	c.mutex.RUnlock()
	assert.Equal(t, 0, initialLen)

	// Create an iterator
	iter, err := c.FromPoint(ocommon.Point{}, true)
	require.NoError(t, err)
	require.NotNil(t, iter)

	// Should have one iterator now
	c.mutex.RLock()
	afterCreateLen := len(c.iterators)
	c.mutex.RUnlock()
	assert.Equal(t, 1, afterCreateLen)

	// Cancel the iterator
	iter.Cancel()

	// Iterator should be removed
	c.mutex.RLock()
	afterCancelLen := len(c.iterators)
	c.mutex.RUnlock()
	assert.Equal(t, 0, afterCancelLen)
}

func TestIteratorCancelMultiple(t *testing.T) {
	// Create a chain manager without a database (in-memory only)
	eventBus := event.NewEventBus(nil, nil)
	cm, err := NewManager(nil, eventBus)
	require.NoError(t, err)

	c := cm.PrimaryChain()
	require.NotNil(t, c)

	// Create multiple iterators
	iter1, err := c.FromPoint(ocommon.Point{}, true)
	require.NoError(t, err)
	iter2, err := c.FromPoint(ocommon.Point{}, true)
	require.NoError(t, err)
	iter3, err := c.FromPoint(ocommon.Point{}, true)
	require.NoError(t, err)

	// Should have 3 iterators
	c.mutex.RLock()
	assert.Equal(t, 3, len(c.iterators))
	c.mutex.RUnlock()

	// Cancel the middle one
	iter2.Cancel()

	// Should have 2 iterators, and iter2 should not be in the list
	c.mutex.RLock()
	assert.Equal(t, 2, len(c.iterators))
	for _, it := range c.iterators {
		assert.False(t, it == iter2)
	}
	c.mutex.RUnlock()

	// Cancel remaining
	iter1.Cancel()
	iter3.Cancel()

	c.mutex.RLock()
	assert.Equal(t, 0, len(c.iterators))
	c.mutex.RUnlock()
}

func TestIteratorCancelIdempotent(t *testing.T) {
	// Create a chain manager without a database (in-memory only)
	eventBus := event.NewEventBus(nil, nil)
	cm, err := NewManager(nil, eventBus)
	require.NoError(t, err)

	c := cm.PrimaryChain()
	require.NotNil(t, c)

	// Create an iterator
	iter, err := c.FromPoint(ocommon.Point{}, true)
	require.NoError(t, err)

	// Cancel multiple times should not panic
	iter.Cancel()
	iter.Cancel()
	iter.Cancel()

	c.mutex.RLock()
	assert.Equal(t, 0, len(c.iterators))
	c.mutex.RUnlock()
}

// TestIteratorParentContextCancelUnblocksNext verifies that a blocking
// iterator created with a parent context exits when that parent is canceled.
func TestIteratorParentContextCancelUnblocksNext(t *testing.T) {
	eventBus := event.NewEventBus(nil, nil)
	cm, err := NewManager(nil, eventBus)
	require.NoError(t, err)

	c := cm.PrimaryChain()
	require.NotNil(t, c)

	ctx, cancel := context.WithCancel(context.Background())
	iter, err := c.FromPointContext(ctx, ocommon.Point{}, true)
	require.NoError(t, err)
	defer iter.Cancel()

	type iterResult struct {
		result *ChainIteratorResult
		err    error
	}
	resultCh := make(chan iterResult, 1)

	go func() {
		res, err := iter.Next(true)
		resultCh <- iterResult{result: res, err: err}
	}()

	testutil.WaitForCondition(t, func() bool {
		c.waitingChanMutex.Lock()
		defer c.waitingChanMutex.Unlock()
		return c.waitingChan != nil
	}, 2*time.Second, "iterator should block waiting for a chain update")

	cancel()

	r := testutil.RequireReceive(
		t,
		resultCh,
		2*time.Second,
		"parent context cancellation should unblock iterator",
	)
	require.Nil(t, r.result)
	require.Error(t, r.err)
	require.True(t, errors.Is(r.err, context.Canceled))

	testutil.WaitForCondition(t, func() bool {
		c.mutex.RLock()
		defer c.mutex.RUnlock()
		return len(c.iterators) == 0
	}, 2*time.Second, "parent context cancellation should remove iterator")
}

// TestIterNextSpuriousWakeups verifies that the blocking
// iterator survives many spurious wake-ups (channel closes
// with no new block available) without stack overflow.
//
// Before the fix, iterNext called itself recursively on each
// wake-up. With thousands of spurious signals, this would
// overflow the goroutine stack. The iterative loop fix makes
// this safe regardless of wake-up count.
func TestIterNextSpuriousWakeups(t *testing.T) {
	eventBus := event.NewEventBus(nil, nil)
	cm, err := NewManager(nil, eventBus)
	require.NoError(t, err)

	c := cm.PrimaryChain()
	require.NotNil(t, c)

	// Create a blocking iterator from origin
	iter, err := c.FromPoint(ocommon.Point{}, true)
	require.NoError(t, err)
	defer iter.Cancel()

	// Channel to receive the iterator result from the
	// blocking goroutine.
	type iterResult struct {
		result *ChainIteratorResult
		err    error
	}
	resultCh := make(chan iterResult, 1)

	// Start a goroutine that calls Next(blocking=true).
	// It will block until a block is available.
	go func() {
		res, err := iter.Next(true)
		resultCh <- iterResult{result: res, err: err}
	}()

	// Fire many spurious wake-ups. Each wake-up closes the
	// waitingChan without adding a block, forcing the
	// iterator to loop again. With the old recursive code,
	// 10 000 wake-ups would overflow the stack.
	const spuriousCount = 10_000
	for i := range spuriousCount {
		// Give the iterator goroutine a moment to register
		// its wait channel before we close it.
		require.Eventually(t, func() bool {
			c.waitingChanMutex.Lock()
			defer c.waitingChanMutex.Unlock()
			return c.waitingChan != nil
		}, 2*time.Second, time.Millisecond,
			"iterator should create waitingChan "+
				"(wake-up %d)", i,
		)

		// Close the waiting channel (spurious wake-up)
		c.waitingChanMutex.Lock()
		close(c.waitingChan)
		c.waitingChan = nil
		c.waitingChanMutex.Unlock()
	}

	// Verify the iterator has not returned yet (still
	// blocking because no block was added).
	select {
	case r := <-resultCh:
		t.Fatalf(
			"iterator should still be blocking "+
				"after spurious wake-ups, got: %+v",
			r,
		)
	case <-time.After(50 * time.Millisecond):
		// Expected: still blocking
	}

	// Now cancel the iterator to unblock the goroutine.
	iter.Cancel()

	// The blocked Next() should return a context
	// cancellation error.
	require.Eventually(t, func() bool {
		select {
		case r := <-resultCh:
			resultCh <- r // put it back for final assertion
			return true
		default:
			return false
		}
	}, 2*time.Second, time.Millisecond,
		"iterator should return after cancel",
	)

	r := <-resultCh
	require.Error(t, r.err,
		"cancelled iterator should return an error",
	)
}

func TestIterNextRegistersWaitBeforeChainUpdateCanCommit(t *testing.T) {
	eventBus := event.NewEventBus(nil, nil)
	cm, err := NewManager(nil, eventBus)
	require.NoError(t, err)

	c := cm.PrimaryChain()
	require.NotNil(t, c)

	iter, err := c.FromPoint(ocommon.Point{}, true)
	require.NoError(t, err)
	defer iter.Cancel()

	type iterResult struct {
		result *ChainIteratorResult
		err    error
	}
	resultCh := make(chan iterResult, 1)
	addDone := make(chan error, 1)

	c.waitingChanMutex.Lock()
	waitMutexLocked := true
	defer func() {
		if waitMutexLocked {
			c.waitingChanMutex.Unlock()
		}
	}()

	go func() {
		res, err := iter.Next(true)
		resultCh <- iterResult{result: res, err: err}
	}()

	testutil.WaitForCondition(t, func() bool {
		if c.mutex.TryLock() {
			c.mutex.Unlock()
			return false
		}
		return true
	}, 2*time.Second, "iterator should hold chain lock at tip")

	blockFixture, err := mockfixtures.NewHarness(
		mockfixtures.HarnessConfig{},
	).Fixture(
		"ouroboros-consensus/ouroboros-consensus-cardano/" +
			"golden/cardano/CardanoNodeToNodeVersion2/Block_Conway",
	)
	require.NoError(t, err)
	block, err := blockFixture.DecodeLedgerBlock()
	require.NoError(t, err)
	blockHash := block.Hash().Bytes()

	go func() {
		addDone <- c.AddBlock(block, nil)
	}()

	testutil.RequireNoReceive(
		t,
		addDone,
		50*time.Millisecond,
		"chain update committed before iterator registered wait channel",
	)

	c.waitingChanMutex.Unlock()
	waitMutexLocked = false

	addErr := testutil.RequireReceive(
		t,
		addDone,
		2*time.Second,
		"chain update should complete after wait registration",
	)
	require.NoError(t, addErr)

	r := testutil.RequireReceive(
		t,
		resultCh,
		2*time.Second,
		"iterator should receive newly added block",
	)
	require.NoError(t, r.err)
	require.NotNil(t, r.result)
	assert.Equal(t, block.BlockNumber(), r.result.Block.Number)
	assert.Equal(t, blockHash, r.result.Point.Hash)
}
