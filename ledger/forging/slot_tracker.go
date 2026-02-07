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

import "sync"

const (
	// defaultMaxTrackedSlots is the maximum number of recently forged
	// slots to keep in memory. Once this limit is reached, the oldest
	// entry is evicted.
	defaultMaxTrackedSlots = 100
)

// ForgedBlockRecord stores the hash of a block we forged for a given slot.
type ForgedBlockRecord struct {
	BlockHash []byte
}

// SlotTracker is a thread-safe tracker for recently forged block
// slots and their hashes. It allows chainsync to detect slot
// battles when an incoming block from a peer occupies a slot for
// which the local node has already forged a block.
type SlotTracker struct {
	mu       sync.RWMutex
	forged   map[uint64]ForgedBlockRecord
	order    []uint64 // insertion order for eviction
	maxSlots int
}

// NewSlotTracker creates a new SlotTracker with the default capacity.
func NewSlotTracker() *SlotTracker {
	return NewSlotTrackerWithCapacity(defaultMaxTrackedSlots)
}

// NewSlotTrackerWithCapacity creates a new SlotTracker with the
// given maximum capacity.
func NewSlotTrackerWithCapacity(maxSlots int) *SlotTracker {
	if maxSlots <= 0 {
		maxSlots = defaultMaxTrackedSlots
	}
	return &SlotTracker{
		forged:   make(map[uint64]ForgedBlockRecord, maxSlots),
		order:    make([]uint64, 0, maxSlots),
		maxSlots: maxSlots,
	}
}

// RecordForgedBlock records that the local node forged a block with
// the given hash at the given slot. If the tracker is at capacity,
// the oldest entry is evicted.
func (st *SlotTracker) RecordForgedBlock(slot uint64, blockHash []byte) {
	st.mu.Lock()
	defer st.mu.Unlock()

	// If we already have an entry for this slot, update it
	if _, exists := st.forged[slot]; exists {
		hashCopy := make([]byte, len(blockHash))
		copy(hashCopy, blockHash)
		st.forged[slot] = ForgedBlockRecord{BlockHash: hashCopy}
		return
	}

	// Evict oldest entry if at capacity
	if len(st.order) >= st.maxSlots {
		oldest := st.order[0]
		st.order = st.order[1:]
		delete(st.forged, oldest)
	}

	// Store a copy of the hash to avoid aliasing
	hashCopy := make([]byte, len(blockHash))
	copy(hashCopy, blockHash)

	st.forged[slot] = ForgedBlockRecord{BlockHash: hashCopy}
	st.order = append(st.order, slot)
}

// WasForgedByUs checks whether the local node forged a block for
// the given slot. If so, it returns the block hash and true.
// Otherwise it returns nil, false.
func (st *SlotTracker) WasForgedByUs(
	slot uint64,
) (blockHash []byte, ok bool) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	record, exists := st.forged[slot]
	if !exists {
		return nil, false
	}
	hashCopy := make([]byte, len(record.BlockHash))
	copy(hashCopy, record.BlockHash)
	return hashCopy, true
}

// Len returns the number of tracked forged slots.
func (st *SlotTracker) Len() int {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return len(st.forged)
}
