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

// TransitionState encodes which of the three HFC TransitionInfo cases currently
// applies to the open (current) era. Mirrors Haskell's
// Ouroboros.Consensus.HardFork.Combinator.State.Types.TransitionInfo.
type TransitionState uint8

const (
	// TransitionUnknown means no confirmed upcoming hard fork is known. The
	// open era's EraEnd will be capped at tipSlot + SafeZoneSlots, snapped to
	// the next epoch boundary.
	TransitionUnknown TransitionState = iota
	// TransitionKnown means the epoch-rollover logic has observed a
	// protocol-version bump confirmed to be stable. KnownEpoch is the first
	// epoch of the next era; its start slot is the exact era boundary.
	TransitionKnown
	// TransitionImpossible means a hard fork cannot happen within the current
	// safe zone (e.g. we're past the epoch's voting deadline, or in the
	// final known era). The safe zone applies from the era start.
	TransitionImpossible
)

// String returns a human-readable label for the state.
func (s TransitionState) String() string {
	switch s {
	case TransitionUnknown:
		return "TransitionUnknown"
	case TransitionKnown:
		return "TransitionKnown"
	case TransitionImpossible:
		return "TransitionImpossible"
	default:
		return "TransitionState(?)"
	}
}

// TransitionInfo describes the current era's known-transition state. It mirrors
// Haskell's HFC TransitionInfo but uses an explicit tag + payload rather than
// a sum type.
type TransitionInfo struct {
	State      TransitionState
	KnownEpoch uint64 // valid when State == TransitionKnown
}

// NewTransitionUnknown constructs the TransitionUnknown variant.
func NewTransitionUnknown() TransitionInfo {
	return TransitionInfo{State: TransitionUnknown}
}

// NewTransitionKnown constructs the TransitionKnown variant with the given
// target epoch (the first epoch of the next era).
func NewTransitionKnown(epoch uint64) TransitionInfo {
	return TransitionInfo{State: TransitionKnown, KnownEpoch: epoch}
}

// NewTransitionImpossible constructs the TransitionImpossible variant.
func NewTransitionImpossible() TransitionInfo {
	return TransitionInfo{State: TransitionImpossible}
}
