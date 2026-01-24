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

	"github.com/blinklabs-io/dingo/event"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
