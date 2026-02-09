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

package forging

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSlotTrackerRecordAndLookup(t *testing.T) {
	st := NewSlotTracker()

	hash := []byte{0x01, 0x02, 0x03}
	st.RecordForgedBlock(1000, hash)

	got, ok := st.WasForgedByUs(1000)
	require.True(t, ok, "slot 1000 should be tracked")
	assert.Equal(t, hash, got)
}

func TestSlotTrackerLookupMissing(t *testing.T) {
	st := NewSlotTracker()

	got, ok := st.WasForgedByUs(999)
	assert.False(t, ok, "untracked slot should return false")
	assert.Nil(t, got)
}

func TestSlotTrackerDoesNotAlias(t *testing.T) {
	st := NewSlotTracker()

	hash := []byte{0x01, 0x02, 0x03}
	st.RecordForgedBlock(1000, hash)

	// Mutate the original slice
	hash[0] = 0xFF

	got, ok := st.WasForgedByUs(1000)
	require.True(t, ok)
	assert.Equal(t, byte(0x01), got[0],
		"tracker should store a copy, not alias the original slice")
}

func TestSlotTrackerEvictsOldestWhenFull(t *testing.T) {
	st := NewSlotTrackerWithCapacity(3)

	st.RecordForgedBlock(1, []byte{0x01})
	st.RecordForgedBlock(2, []byte{0x02})
	st.RecordForgedBlock(3, []byte{0x03})
	assert.Equal(t, 3, st.Len())

	// Adding a 4th entry should evict slot 1
	st.RecordForgedBlock(4, []byte{0x04})
	assert.Equal(t, 3, st.Len())

	_, ok := st.WasForgedByUs(1)
	assert.False(t, ok, "slot 1 should have been evicted")

	got, ok := st.WasForgedByUs(4)
	require.True(t, ok)
	assert.Equal(t, []byte{0x04}, got)

	// Slots 2 and 3 should still be present
	_, ok = st.WasForgedByUs(2)
	assert.True(t, ok)
	_, ok = st.WasForgedByUs(3)
	assert.True(t, ok)
}

func TestSlotTrackerUpdateExistingSlot(t *testing.T) {
	st := NewSlotTrackerWithCapacity(3)

	st.RecordForgedBlock(1, []byte{0x01})
	st.RecordForgedBlock(1, []byte{0xFF})

	got, ok := st.WasForgedByUs(1)
	require.True(t, ok)
	assert.Equal(t, []byte{0xFF}, got,
		"updating an existing slot should overwrite the hash")
	assert.Equal(t, 1, st.Len(),
		"updating should not increase size")
}

func TestSlotTrackerConcurrency(t *testing.T) {
	st := NewSlotTracker()
	const goroutines = 50
	const slotsPerGoroutine = 20

	var wg sync.WaitGroup
	wg.Add(goroutines * 2) // writers + readers

	// Writers
	for g := range goroutines {
		go func(base uint64) {
			defer wg.Done()
			for i := range slotsPerGoroutine {
				slot := base*slotsPerGoroutine + uint64(i)
				st.RecordForgedBlock(slot, []byte{byte(slot % 256)})
			}
		}(uint64(g))
	}

	// Readers
	for range goroutines {
		go func() {
			defer wg.Done()
			for i := range slotsPerGoroutine {
				st.WasForgedByUs(uint64(i))
			}
		}()
	}

	wg.Wait()

	// Just verify no panics and the tracker has a reasonable size
	assert.LessOrEqual(t, st.Len(), defaultMaxTrackedSlots)
}

func TestSlotTrackerZeroCapacity(t *testing.T) {
	// Zero/negative capacity should fall back to default
	st := NewSlotTrackerWithCapacity(0)
	assert.NotNil(t, st)

	st.RecordForgedBlock(1, []byte{0x01})
	_, ok := st.WasForgedByUs(1)
	assert.True(t, ok)
}
