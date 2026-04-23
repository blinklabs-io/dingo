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
	"errors"
	"fmt"
	"time"
)

// ErrBeforeGenesis is returned by TimeToSlot when the given time is before
// the chain's SystemStart.
var ErrBeforeGenesis = errors.New("hardfork: time is before system start")

// ErrPastHorizon is returned when a slot or time falls past the last bounded
// era's end and no unbounded era follows. Mirrors Haskell's PastHorizonException.
var ErrPastHorizon = errors.New("hardfork: slot/time past era horizon")

// EraSummary describes the bounds and parameters of a single era within a
// chain's confirmed history.
//
// End == nil denotes an unbounded era (the current era under
// UnsafeIndefiniteSafeZone). Every past era MUST have a non-nil End.
type EraSummary struct {
	EraID  uint
	Start  Bound
	End    *Bound // nil ⇒ unbounded
	Params EraParams
}

// EpochInfo is a lightweight SlotToEpoch answer combining the absolute epoch
// number with the era parameters needed to reason about it.
type EpochInfo struct {
	Epoch         uint64
	StartSlot     uint64
	LengthInSlots uint64
	SlotLength    time.Duration
	EraID         uint
}

// Summary is the confirmed per-chain era history. It is derived from the
// ledger state + TransitionInfo by BuildSummary and then consulted for all
// slot/epoch/time conversions.
//
// Summary mirrors the semantics of Ouroboros.Consensus.HardFork.History.Summary
// but in a Go-idiomatic, non-generic form.
type Summary struct {
	SystemStart time.Time
	Eras        []EraSummary // chronological; last element is the current era
	Transition  TransitionInfo
}

// CurrentEra returns a pointer to the last (open) era, or nil if the Summary
// has no eras.
func (s *Summary) CurrentEra() *EraSummary {
	if len(s.Eras) == 0 {
		return nil
	}
	return &s.Eras[len(s.Eras)-1]
}

// Validate checks structural invariants:
//   - Summary has at least one era.
//   - Each non-first era's Start equals the previous era's End.
//   - Only the last era may have a nil End.
//
// It does NOT check the per-era INV-1b / INV-2b invariants (end.Slot ==
// start.Slot + epochs*epochSize, etc.) — those are left to the builder.
func (s *Summary) Validate() error {
	if len(s.Eras) == 0 {
		return errors.New("hardfork: Summary must have at least one era")
	}
	for i, era := range s.Eras {
		if err := era.Params.Validate(); err != nil {
			return fmt.Errorf(
				"hardfork: era %d (id=%d) params: %w",
				i, era.EraID, err,
			)
		}
	}
	for i := 1; i < len(s.Eras); i++ {
		prev := s.Eras[i-1]
		cur := s.Eras[i]
		if prev.End == nil {
			return fmt.Errorf("hardfork: era %d (id=%d) is unbounded but not last",
				i-1, prev.EraID)
		}
		if *prev.End != cur.Start {
			return fmt.Errorf(
				"hardfork: era %d end %+v does not match era %d start %+v",
				i-1, *prev.End, i, cur.Start,
			)
		}
	}
	return nil
}

// eraForSlot returns the era containing the given absolute slot, or
// ErrPastHorizon if the slot falls outside every era's range.
func (s *Summary) eraForSlot(slot uint64) (*EraSummary, error) {
	for i := range s.Eras {
		era := &s.Eras[i]
		if slot < era.Start.Slot {
			return nil, ErrPastHorizon
		}
		if era.End == nil || slot < era.End.Slot {
			return era, nil
		}
	}
	return nil, ErrPastHorizon
}

// eraForRelTime returns the era containing the given relative time.
func (s *Summary) eraForRelTime(rt time.Duration) (*EraSummary, error) {
	for i := range s.Eras {
		era := &s.Eras[i]
		if rt < era.Start.RelativeTime {
			return nil, ErrPastHorizon
		}
		if era.End == nil || rt < era.End.RelativeTime {
			return era, nil
		}
	}
	return nil, ErrPastHorizon
}

// SlotToTime converts an absolute slot number to its wall-clock start time.
func (s *Summary) SlotToTime(slot uint64) (time.Time, error) {
	era, err := s.eraForSlot(slot)
	if err != nil {
		return time.Time{}, err
	}
	slotsIntoEra := slot - era.Start.Slot
	// slotsIntoEra is bounded by the era slot range; at Cardano slot lengths
	// the product stays well within time.Duration's int64 nanosecond range.
	// #nosec G115
	rel := era.Start.RelativeTime + time.Duration(slotsIntoEra)*era.Params.SlotLength
	return s.SystemStart.Add(rel), nil
}

// TimeToSlot converts a wall-clock time to the slot in which it falls.
// Returns ErrBeforeGenesis if t is before SystemStart.
func (s *Summary) TimeToSlot(t time.Time) (uint64, error) {
	rel := t.Sub(s.SystemStart)
	if rel < 0 {
		return 0, ErrBeforeGenesis
	}
	era, err := s.eraForRelTime(rel)
	if err != nil {
		return 0, err
	}
	relInEra := rel - era.Start.RelativeTime
	// eraForRelTime guarantees rel >= era.Start.RelativeTime, and Validate
	// requires SlotLength > 0, so the quotient is non-negative.
	// #nosec G115
	slotsInEra := uint64(relInEra / era.Params.SlotLength)
	return era.Start.Slot + slotsInEra, nil
}

// SlotToEpoch converts an absolute slot number to an EpochInfo.
func (s *Summary) SlotToEpoch(slot uint64) (EpochInfo, error) {
	era, err := s.eraForSlot(slot)
	if err != nil {
		return EpochInfo{}, err
	}
	slotsIntoEra := slot - era.Start.Slot
	epochsIntoEra := slotsIntoEra / era.Params.EpochSize
	return EpochInfo{
		Epoch:         era.Start.Epoch + epochsIntoEra,
		StartSlot:     era.Start.Slot + epochsIntoEra*era.Params.EpochSize,
		LengthInSlots: era.Params.EpochSize,
		SlotLength:    era.Params.SlotLength,
		EraID:         era.EraID,
	}, nil
}
