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

package database

import (
	"context"
	"sync"
)

// retainedBound holds the slot where retained block history starts, as last
// computed by RefreshOldestRetainedSlot. Reading it does no I/O, so a request
// path can name the bound in an error without scanning the blob store.
type retainedBound struct {
	// refreshMu serializes refreshes.
	refreshMu sync.Mutex
	mu        sync.Mutex
	slot      uint64
	known     bool
	// While a refresh scans, removed records the lowest slot whose block
	// was expired or deleted since the scan began. A scan result at or
	// above it may name a block that is gone, so it is not stored.
	scanning   bool
	removed    bool
	removedMin uint64
}

// OldestRetainedSlot returns the slot of the oldest block whose body is still
// stored, as last computed by RefreshOldestRetainedSlot. ok is false before
// the first refresh, when history expiry has never run, when the bound could
// not be found, and after the block at or below the bound was expired or
// deleted, until the next refresh. It reads only memory.
func (d *Database) OldestRetainedSlot() (slot uint64, ok bool) {
	b := &d.retained
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.slot, b.known
}

// RefreshOldestRetainedSlot recomputes the bound OldestRetainedSlot returns by
// walking from the history-expiry cursor past expired blocks, at most
// oldestRetainedScanLimit of them. The history-expiry pruner calls it in the
// background after each round. A block expired or deleted during the walk at
// or below the slot it finds leaves the bound unknown, since that block may
// be the one found.
func (d *Database) RefreshOldestRetainedSlot(ctx context.Context) error {
	return d.refreshOldestRetainedSlot(
		ctx,
		func(ctx context.Context) (uint64, bool, error) {
			return d.oldestRetainedSlot(ctx, oldestRetainedScanLimit)
		},
	)
}

// refreshOldestRetainedSlot is RefreshOldestRetainedSlot with the scan as a
// parameter.
func (d *Database) refreshOldestRetainedSlot(
	ctx context.Context,
	scan func(context.Context) (uint64, bool, error),
) error {
	b := &d.retained
	b.refreshMu.Lock()
	defer b.refreshMu.Unlock()
	b.mu.Lock()
	b.scanning = true
	b.removed = false
	b.mu.Unlock()
	slot, ok, err := scan(ctx)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.scanning = false
	if err != nil {
		return err
	}
	if ok && b.removed && b.removedMin <= slot {
		ok = false
	}
	if !ok {
		slot = 0
	}
	b.slot, b.known = slot, ok
	return nil
}

// retainedBlockRemoved records that the block at slot is being expired or
// deleted in txn. If that block may be the oldest retained one, the bound
// becomes unknown, both now and again once txn commits: a refresh that runs
// before the commit still reads the block and would otherwise store it.
func (d *Database) retainedBlockRemoved(txn *Txn, slot uint64) {
	d.retained.blockRemoved(slot)
	if txn != nil {
		txn.AfterCommit(func() { d.retained.blockRemoved(slot) })
	}
}

func (b *retainedBound) blockRemoved(slot uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.known && slot <= b.slot {
		b.slot, b.known = 0, false
	}
	if b.scanning && (!b.removed || slot < b.removedMin) {
		b.removed = true
		b.removedMin = slot
	}
}

// resetRetainedBound forgets the bound, for a change of blob store.
func (d *Database) resetRetainedBound() {
	b := &d.retained
	b.mu.Lock()
	defer b.mu.Unlock()
	b.slot, b.known = 0, false
	if b.scanning {
		b.removed = true
		b.removedMin = 0
	}
}
