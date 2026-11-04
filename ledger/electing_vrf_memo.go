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

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

// electingVrfMemoMaxEntries bounds the memo; it is cleared when full.
const electingVrfMemoMaxEntries = 4096

type electingVrfMemoKey struct {
	pool         lcommon.PoolKeyHash
	cutoffSlot   uint64
	capturedSlot uint64
}

// electingVrfMemo caches resolved electing VRF key hashes. Entries are only
// stored for slots at or below the applied tip minus the stability window
// (see electingVrfMemoStable), so registration history up to them can no
// longer change through a rollback or a late-applied block.
//
// A ledger rollback clears the memo after it publishes the rolled-back tip.
// A lookup captures the generation before it reads the tip, so a lookup that
// judged stability against the pre-rollback tip carries a generation that
// clear has since advanced, and its put is dropped instead of reinstalling a
// pre-rollback answer.
type electingVrfMemo struct {
	mu         sync.RWMutex
	entries    map[electingVrfMemoKey]lcommon.Blake2b256
	generation uint64
}

// currentGeneration returns the generation a lookup must pass to put.
func (m *electingVrfMemo) currentGeneration() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.generation
}

func (m *electingVrfMemo) get(
	key electingVrfMemoKey,
) (lcommon.Blake2b256, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	hash, ok := m.entries[key]
	return hash, ok
}

// put stores hash unless clear has run since generation was captured.
func (m *electingVrfMemo) put(
	generation uint64,
	key electingVrfMemoKey,
	hash lcommon.Blake2b256,
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if generation != m.generation {
		return
	}
	if m.entries == nil || len(m.entries) >= electingVrfMemoMaxEntries {
		m.entries = make(map[electingVrfMemoKey]lcommon.Blake2b256)
	}
	m.entries[key] = hash
}

func (m *electingVrfMemo) clear() {
	m.mu.Lock()
	m.entries = nil
	m.generation++
	m.mu.Unlock()
}

// electingVrfMemoStable reports whether registration history through
// cutoffSlot and capturedSlot is immutable: both must lie at least one
// stability window (the conservative 4k/f) behind the applied tip. Peer
// headers can be validated ahead of the applied tip, so the tip, not the
// header slot, bounds the answer. An unknown window disables the memo.
func (ls *LedgerState) electingVrfMemoStable(
	cutoffSlot uint64,
	capturedSlot uint64,
) bool {
	window := ls.nonceStabilityWindow(conway.EraIdConway)
	if window == 0 {
		return false
	}
	tipSnapshot := ls.loadTipSnapshot()
	if tipSnapshot == nil {
		return false
	}
	tipSlot := tipSnapshot.currentTip.Point.Slot
	if tipSlot < window {
		return false
	}
	bound := tipSlot - window
	return cutoffSlot <= bound && capturedSlot <= bound
}
