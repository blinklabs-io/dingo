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

// Package hardfork provides the HardFork Combinator primitives used by the
// ledger to reason about multi-era chain time, epoch, and slot conversions.
//
// This package mirrors the structure of Ouroboros.Consensus.HardFork.History
// and Ouroboros.Consensus.HardFork.Combinator in the Haskell consensus
// implementation, but is deliberately decoupled from any specific era or
// genesis configuration. Callers build an EraParams + Shape + Summary from
// their own sources and then use the conversion methods on Summary.
package hardfork

import (
	"errors"
	"time"
)

// EraParams describes the fixed per-era protocol parameters needed to
// translate between slot, epoch, and wall-clock time.
//
// SafeZoneSlots == 0 denotes UnsafeIndefiniteSafeZone: the era has no known
// upper bound, and Summary will produce a nil End for it.
type EraParams struct {
	// EpochSize is the number of slots per epoch in this era.
	EpochSize uint64
	// SlotLength is the wall-clock duration of one slot.
	SlotLength time.Duration
	// SafeZoneSlots is the number of slots from the ledger tip (or era start)
	// within which no hard fork can occur. Zero means unbounded (the era has
	// no forecasted end).
	SafeZoneSlots uint64
	// GenesisWindow is the Ouroboros Genesis density-disconnection window,
	// also expressed in slots. Unused by Summary today but carried for
	// future callers.
	GenesisWindow uint64
}

// Validate returns nil if the EraParams values are internally consistent.
func (p EraParams) Validate() error {
	if p.EpochSize == 0 {
		return errors.New("hardfork: EraParams.EpochSize must be > 0")
	}
	if p.SlotLength <= 0 {
		return errors.New("hardfork: EraParams.SlotLength must be > 0")
	}
	return nil
}

// Bound identifies a point in chain coordinates along all three axes: relative
// time (since SystemStart), absolute slot, and absolute epoch. Every EraSummary
// has a Start Bound, and a closed era also has an End Bound.
type Bound struct {
	RelativeTime time.Duration
	Slot         uint64
	Epoch        uint64
}
