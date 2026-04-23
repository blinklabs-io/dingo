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

// ShapeEntry describes one era's static identity and parameters: the era ID,
// the span of protocol major versions it covers, its EraParams, and the
// trigger that arms the transition OUT of this era into its successor.
//
// Protocol major version ranges must be contiguous and non-overlapping across
// a valid Shape. NextEraTrigger mirrors Haskell's per-era TriggerHardFork:
// the final entry must be TriggerNotDuringThisExecution; every other entry
// should carry either TriggerAtVersion (the default, keyed on the next era's
// MinMajorVersion) or TriggerAtEpoch (the TestXHardForkAtEpoch override).
type ShapeEntry struct {
	EraID           uint
	EraName         string
	MinMajorVersion uint
	MaxMajorVersion uint
	Params          EraParams
	NextEraTrigger  TriggerHardFork
}

// Shape is the static, compile-time-known era table: the set of eras this node
// binary knows how to handle, in chronological order. Mirrors Haskell's
// Ouroboros.Consensus.HardFork.History.Summary.Shape.
type Shape struct {
	SystemStart time.Time
	Eras        []ShapeEntry
}

// Validate returns nil if the Shape is structurally consistent:
//   - At least one era entry.
//   - Each entry's protocol-version range is well-formed (min ≤ max).
//   - Consecutive entries' version ranges meet without gap and without overlap
//     (cur.Min == prev.Max + 1).
//   - Each entry's EraParams validate.
//   - The final entry carries TriggerNotDuringThisExecution; no other entry
//     does.
func (s Shape) Validate() error {
	if len(s.Eras) == 0 {
		return errors.New("hardfork: Shape must have at least one era")
	}
	for i, e := range s.Eras {
		if e.MinMajorVersion > e.MaxMajorVersion {
			return fmt.Errorf(
				"hardfork: era %q: MinMajorVersion (%d) > MaxMajorVersion (%d)",
				e.EraName, e.MinMajorVersion, e.MaxMajorVersion,
			)
		}
		if err := e.Params.Validate(); err != nil {
			return fmt.Errorf("hardfork: era %q: %w", e.EraName, err)
		}
		isLast := i == len(s.Eras)-1
		switch {
		case e.NextEraTrigger.Kind != TriggerAtVersion &&
			e.NextEraTrigger.Kind != TriggerAtEpoch &&
			e.NextEraTrigger.Kind != TriggerNotDuringThisExecution:
			return fmt.Errorf(
				"hardfork: era %q: NextEraTrigger has unknown kind %d",
				e.EraName, e.NextEraTrigger.Kind,
			)
		case isLast && e.NextEraTrigger.Kind != TriggerNotDuringThisExecution:
			return fmt.Errorf(
				"hardfork: era %q (final): NextEraTrigger must be NotDuringThisExecution, got %s",
				e.EraName, e.NextEraTrigger,
			)
		case !isLast && e.NextEraTrigger.Kind == TriggerNotDuringThisExecution:
			return fmt.Errorf(
				"hardfork: era %q (non-final): NextEraTrigger must not be NotDuringThisExecution",
				e.EraName,
			)
		}
		if i == 0 {
			continue
		}
		prev := s.Eras[i-1]
		// Check contiguity without computing prev.MaxMajorVersion+1 directly —
		// that addition can wrap uint if MaxMajorVersion == math.MaxUint and
		// silently accept e.MinMajorVersion == 0 as a valid successor.
		if e.MinMajorVersion == 0 || e.MinMajorVersion-1 != prev.MaxMajorVersion {
			return fmt.Errorf(
				"hardfork: era %q: MinMajorVersion (%d) must be prev (%q) MaxMajorVersion (%d) + 1",
				e.EraName, e.MinMajorVersion, prev.EraName, prev.MaxMajorVersion,
			)
		}
	}
	return nil
}

// EraForVersion returns the ShapeEntry whose version range contains
// majorVersion, or (_, false) if no era covers it.
func (s Shape) EraForVersion(majorVersion uint) (ShapeEntry, bool) {
	for _, e := range s.Eras {
		if majorVersion >= e.MinMajorVersion && majorVersion <= e.MaxMajorVersion {
			return e, true
		}
	}
	return ShapeEntry{}, false
}

// EraForID returns the ShapeEntry with the given EraID, or (_, false).
func (s Shape) EraForID(eraID uint) (ShapeEntry, bool) {
	for _, e := range s.Eras {
		if e.EraID == eraID {
			return e, true
		}
	}
	return ShapeEntry{}, false
}

// EraIndex returns the zero-based position of the era with the given EraID in
// the shape, or (_, false) if no such era is present.
func (s Shape) EraIndex(eraID uint) (int, bool) {
	for i, e := range s.Eras {
		if e.EraID == eraID {
			return i, true
		}
	}
	return 0, false
}
