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

package hardfork

import (
	"fmt"
	"time"
)

// BuildSummary constructs a Summary from a Shape, the confirmed closed past
// eras, the current (open) era, the ledger tip slot, and the current
// TransitionInfo.
//
// The current era's End is computed per the HFC safe-zone rules:
//
//   - TransitionUnknown    — apply SafeZoneSlots from max(tipSlot+1, era start),
//     then snap up to the next epoch boundary.
//   - TransitionKnown      — pin the end at KnownEpoch's start (mkUpperBound).
//   - TransitionImpossible — apply SafeZoneSlots from the era start, snapped.
//
// If Params.SafeZoneSlots == 0 the era is treated as UnsafeIndefiniteSafeZone
// and its End remains nil.
//
// Mirrors Ouroboros.Consensus.HardFork.Combinator.State.Infra.reconstructSummary.
func BuildSummary(
	shape Shape,
	past []EraSummary,
	current EraSummary,
	tipSlot uint64,
	transition TransitionInfo,
) (Summary, error) {
	if err := current.Params.Validate(); err != nil {
		return Summary{}, fmt.Errorf(
			"hardfork: current era params invalid: %w",
			err,
		)
	}
	for i, p := range past {
		if p.End == nil {
			return Summary{}, fmt.Errorf(
				"hardfork: past era %d (%d) is unbounded",
				i, p.EraID,
			)
		}
	}

	// Compute the current era's End.
	var end *Bound
	switch transition.State {
	case TransitionKnown:
		if transition.KnownEpoch <= current.Start.Epoch {
			return Summary{}, fmt.Errorf(
				"hardfork: TransitionKnown epoch %d must be > era start epoch %d",
				transition.KnownEpoch,
				current.Start.Epoch,
			)
		}
		b := mkUpperBound(current.Params, current.Start, transition.KnownEpoch)
		end = &b
	case TransitionUnknown:
		// Measure safe zone from max(tipSlot+1, era start).
		fromSlot := max(tipSlot+1, current.Start.Slot)
		end = applySafeZone(current.Params, current.Start, fromSlot)
	case TransitionImpossible:
		end = applySafeZone(current.Params, current.Start, current.Start.Slot)
	default:
		return Summary{}, fmt.Errorf(
			"hardfork: unknown TransitionState %v",
			transition.State,
		)
	}
	current.End = end

	eras := make([]EraSummary, 0, len(past)+1)
	eras = append(eras, past...)
	eras = append(eras, current)

	return Summary{
		SystemStart: shape.SystemStart,
		Eras:        eras,
		Transition:  transition,
	}, nil
}

// SuccessorEra builds the era that follows a bounded era whose End is an
// announced transition boundary. anchorSlot is the same live tip/horizon
// anchor BuildSummary receives (the published tip, or a block-apply caller's
// more recent applied predecessor); the safe zone is measured from
// max(anchorSlot+1, start.Slot), exactly mirroring BuildSummary's
// TransitionUnknown branch. While the tip has not yet reached the boundary
// (the ordinary case: TransitionKnown announces a future era before it
// starts), anchorSlot+1 <= start.Slot and this reduces to the safe zone
// measured from start, identical to what BuildSummary computes for
// TransitionImpossible.
//
// The anchor matters once the boundary itself is at or behind the live tip:
// a caller reconstructing a Summary for a slot beyond the boundary (this
// era's own epoch-cache group has not been populated yet, so BuildSummary
// cannot compute this era as "current" the ordinary way, but the events that
// would flip TransitionKnown back to TransitionUnknown for it have not run
// either) must still roll the successor's horizon forward with the tip the
// same way BuildSummary does for every other era, or the horizon freezes at
// boundary+safeZone forever regardless of how far the chain has actually
// progressed past the boundary. Measuring only from start (the previous
// behavior) reproduced that freeze.
//
// The bound always snaps up to at least the next epoch boundary, so the
// successor covers the whole first post-boundary epoch even when anchorSlot
// is behind start. A zero SafeZoneSlots means UnsafeIndefiniteSafeZone and
// leaves the successor open, matching BuildSummary.
//
// Mirrors the recursion into the next era in
// Ouroboros.Consensus.HardFork.Combinator.State.Infra.reconstructSummary.
func SuccessorEra(
	start Bound,
	eraID uint,
	params EraParams,
	anchorSlot uint64,
) EraSummary {
	fromSlot := max(anchorSlot+1, start.Slot)
	return EraSummary{
		EraID:  eraID,
		Start:  start,
		End:    applySafeZone(params, start, fromSlot),
		Params: params,
	}
}

// mkUpperBound computes a Bound at the start of hiEpoch, given the era's
// lower bound lo and its params. Mirrors Haskell's mkUpperBound.
func mkUpperBound(params EraParams, lo Bound, hiEpoch uint64) Bound {
	epochsInEra := hiEpoch - lo.Epoch
	slotsInEra := epochsInEra * params.EpochSize
	// slotsInEra is bounded by the era's epoch span; real-chain values remain
	// well under int64 (292 years at 1s/slot).
	// #nosec G115
	inEraTime := time.Duration(slotsInEra) * params.SlotLength
	return Bound{
		RelativeTime: lo.RelativeTime + inEraTime,
		Slot:         lo.Slot + slotsInEra,
		Epoch:        hiEpoch,
	}
}

// slotToEpochBound returns the smallest absolute epoch whose first slot is
// >= hiSlot. Mirrors Haskell's slotToEpochBound.
func slotToEpochBound(params EraParams, lo Bound, hiSlot uint64) uint64 {
	if hiSlot < lo.Slot {
		return lo.Epoch
	}
	slotsFromLo := hiSlot - lo.Slot
	epochs := slotsFromLo / params.EpochSize
	inEpoch := slotsFromLo % params.EpochSize
	bump := uint64(0)
	if inEpoch != 0 {
		bump = 1
	}
	return lo.Epoch + epochs + bump
}

// applySafeZone returns the era End for StandardSafeZone, computed from the
// given fromSlot. Returns nil if SafeZoneSlots == 0 (UnsafeIndefiniteSafeZone).
func applySafeZone(params EraParams, lo Bound, fromSlot uint64) *Bound {
	if params.SafeZoneSlots == 0 {
		return nil
	}
	target := fromSlot + params.SafeZoneSlots
	hiEpoch := slotToEpochBound(params, lo, target)
	b := mkUpperBound(params, lo, hiEpoch)
	return &b
}
