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

package ledger

import (
	"sync"

	"github.com/blinklabs-io/dingo/consensus/praos"
)

// acquiredPointMaxUtxoHoldWindows bounds how far an acquired point may hold
// consumed-UTxO cleanup behind its normal floor, in stability windows. Without
// it a client that acquires a point and never releases or disconnects would
// stop spent-UTxO pruning for good. It mirrors poolSnapshotRetentionMaxDepth,
// the equivalent backstop for pool-stake snapshots, which already caps how far
// any pin may hold those back: a point older than the backstop stops being
// protected, and queries against it degrade to the retention rejection they
// would have hit with no pin at all.
const acquiredPointMaxUtxoHoldWindows uint64 = 4

// acquiredPointPins records the slots of currently acquired LocalStateQuery
// points, so the pruning paths can retain what those points still need.
//
// The mutex closes the race between verifying an acquired point and
// recording it. Pruning reads the oldest pin,
// caps its floor at it, and announces that floor all under mu, then releases
// mu and only afterwards deletes. Acquire registers its pin under the same mu
// before verifying. So either the pin lands first and the prune retains its
// data, or the prune announces first and the later verify sees the announced
// floor and refuses the point at Acquire -- where the wire protocol has a
// clean AcquireFailure -- rather than letting it fail mid-query, where it has
// none.
//
// No I/O ever happens under mu. Holding a lock across the delete is what
// deadlocked the earlier pool-snapshot retention guard on SQLite's single
// write connection (see PrunePoolSnapshotsWithRetentionFloor), and announcing
// the floor in memory is what lets this avoid it.
//
// The zero value is ready to use.
type acquiredPointPins struct {
	mu   sync.Mutex
	next uint64
	pins map[uint64]uint64 // pin id -> acquired slot

	// Floors announced by the pruning paths, monotonic. Each is published
	// under mu before the corresponding delete, so a verify that runs after
	// its own pin was registered can never miss one.
	utxoFloorSlot          uint64
	poolSnapshotFloorEpoch uint64
}

// PinAcquiredPoint records slot as currently acquired and returns a function
// that releases it. The caller must call release exactly once, on Release,
// re-Acquire, an Acquire of a tip, or disconnect. Calling it again is a
// no-op, so a release on an already-cleared path is safe.
//
// Register the pin before verifying the point, not after: the pin is what
// stops a concurrent prune from removing state the verify just approved.
func (ls *LedgerState) PinAcquiredPoint(slot uint64) (release func()) {
	p := &ls.acquiredPins
	p.mu.Lock()
	if p.pins == nil {
		p.pins = make(map[uint64]uint64)
	}
	p.next++
	id := p.next
	p.pins[id] = slot
	p.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			p.mu.Lock()
			delete(p.pins, id)
			p.mu.Unlock()
		})
	}
}

// oldestPinnedSlotLocked returns the lowest acquired slot. p.mu must be held.
func (p *acquiredPointPins) oldestPinnedSlotLocked() (uint64, bool) {
	var (
		oldest uint64
		found  bool
	)
	for _, slot := range p.pins {
		if !found || slot < oldest {
			oldest = slot
			found = true
		}
	}
	return oldest, found
}

// capUtxoPruneFloor lowers floor to the oldest acquired point, clamps it back
// up to the hold backstop, and announces the result. It returns the floor the
// caller may prune below. The caller must delete only after this returns.
func (ls *LedgerState) capUtxoPruneFloor(
	floor, stabilityWindow uint64,
) uint64 {
	var backstop uint64
	if hold := stabilityWindow * acquiredPointMaxUtxoHoldWindows; floor > hold {
		backstop = floor - hold
	}
	p := &ls.acquiredPins
	p.mu.Lock()
	defer p.mu.Unlock()
	if oldest, ok := p.oldestPinnedSlotLocked(); ok && oldest < floor {
		floor = max(oldest, backstop)
	}
	if floor > p.utxoFloorSlot {
		p.utxoFloorSlot = floor
	}
	return floor
}

// capPoolSnapshotPruneBefore lowers before -- the epoch below which pool-stake
// snapshots are pruned -- so no acquired point loses the mark snapshot its
// stake distribution is computed from, clamps it back up to minBefore, and
// announces the result. epochOf maps an acquired slot to its epoch; a slot it
// cannot map retains everything, the same conservative answer the deferred
// header pin gives an unmappable slot.
func (ls *LedgerState) capPoolSnapshotPruneBefore(
	before, minBefore uint64,
	epochOf func(slot uint64) (uint64, bool),
) uint64 {
	p := &ls.acquiredPins
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, slot := range p.pins {
		epoch, ok := epochOf(slot)
		if !ok {
			before = 0
			break
		}
		if snap := praos.StakeSnapshotEpoch(epoch); snap < before {
			before = snap
		}
	}
	if before < minBefore {
		before = minBefore
	}
	if before > p.poolSnapshotFloorEpoch {
		p.poolSnapshotFloorEpoch = before
	}
	return before
}

// announcedPruneFloors returns the floors the pruning paths have announced.
// A verify reads these after registering its own pin, which is what makes the
// check race-free: any prune it does not see here had not yet announced when
// the pin landed, and so will see the pin and retain the point.
func (ls *LedgerState) announcedPruneFloors() (
	utxoFloorSlot, poolSnapshotFloorEpoch uint64,
) {
	p := &ls.acquiredPins
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.utxoFloorSlot, p.poolSnapshotFloorEpoch
}

// AcquiredPointPinCountForTesting reports how many acquired-point pins are
// live, so a test outside this package can prove every path that clears an
// acquired point also releases its pin. A leaked pin is otherwise invisible
// until it has held pruning back for days.
func (ls *LedgerState) AcquiredPointPinCountForTesting() int {
	p := &ls.acquiredPins
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pins)
}
