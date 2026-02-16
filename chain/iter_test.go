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
	"testing"
	"time"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/event"
)

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
		assert.NotEqual(t, iter2, it)
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
