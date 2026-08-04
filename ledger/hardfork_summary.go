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
	"errors"
	"time"

	"github.com/blinklabs-io/dingo/ledger/hardfork"
)

// HardForkSummary constructs a hardfork.Summary describing the chain's era
// history from the LedgerState's current epoch cache, tip, current era, and
// transition info.
//
// The returned Summary's past eras are closed with bounds computed by walking
// the epoch cache grouped by EraId. The current era is passed through
// hardfork.BuildSummary with the safe zone from the configured era Shape and
// the ledger's current TransitionInfo. This gives in-memory callers the same
// bounded forecast inputs used by the NtC HardForkEraHistory query.
func (ls *LedgerState) HardForkSummary() (*hardfork.Summary, error) {
	// SystemStart is sourced from the Shelley genesis when available. When it
	// isn't (e.g. SlotToEpoch-style callers that work from the epoch cache
	// alone), SystemStart stays at the zero time.Time and callers must avoid
	// using Summary.SlotToTime / TimeToSlot.
	var systemStart time.Time
	if ls.config.CardanoNodeConfig != nil {
		if sg := ls.config.CardanoNodeConfig.ShelleyGenesis(); sg != nil {
			systemStart = sg.SystemStart
		}
	}

	consensusState, tipState := ls.loadStateSnapshots()
	cache := consensusState.epochCache
	transitionInfo := consensusState.transitionInfo
	tipSlot := tipState.currentTip.Point.Slot

	if len(cache) == 0 {
		return nil, errors.New("ledger: no epochs in cache")
	}

	// Walk the epoch cache grouping contiguous epochs by EraId. Each group
	// becomes one EraSummary; its Start is derived from the first epoch of
	// the group, and its End (for past eras) is the Start of the next group.
	eraSummaries := make([]hardfork.EraSummary, 0, len(cache))
	relTime := time.Duration(0)

	i := 0
	for i < len(cache) {
		first := cache[i]
		eraID := first.EraId
		// Per-epoch params within an era are expected to be constant; we use
		// the first epoch's values as the era-level params.
		// first.SlotLength is protocol-bounded (milliseconds per slot).
		// #nosec G115
		slotLen := time.Duration(first.SlotLength) * time.Millisecond
		epochSize := uint64(first.LengthInSlots)

		start := hardfork.Bound{
			RelativeTime: relTime,
			Slot:         first.StartSlot,
			Epoch:        first.EpochId,
		}

		// Advance through all contiguous epochs with the same EraId,
		// accumulating relTime.
		j := i
		for j < len(cache) && cache[j].EraId == eraID {
			ep := cache[j]
			// LengthInSlots and SlotLength are protocol-bounded uints.
			// #nosec G115
			relTime += time.Duration(ep.LengthInSlots) *
				time.Duration(ep.SlotLength) * time.Millisecond
			j++
		}

		last := cache[j-1]
		end := hardfork.Bound{
			RelativeTime: relTime,
			Slot:         last.StartSlot + uint64(last.LengthInSlots),
			Epoch:        last.EpochId + 1,
		}

		eraSummary := hardfork.EraSummary{
			EraID: eraID,
			Start: start,
			Params: hardfork.EraParams{
				EpochSize:     epochSize,
				SlotLength:    slotLen,
				SafeZoneSlots: 0,
				GenesisWindow: 0,
			},
		}

		isLast := j == len(cache)
		if !isLast {
			// Close this era at the next era's start.
			eraSummary.End = &end
		}
		eraSummaries = append(eraSummaries, eraSummary)

		i = j
	}

	current := eraSummaries[len(eraSummaries)-1]
	past := eraSummaries[:len(eraSummaries)-1]

	// Use the same configured safe-zone source as the NtC era-history query.
	// Tests and early bootstrap callers may construct a LedgerState without a
	// complete node configuration; in that case eraShape is unavailable and
	// the legacy indefinite safe zone remains the only defensible answer.
	shape := ls.eraShape()
	shapeEntry, shapeAvailable := shape.EraForID(current.EraID)
	if shapeAvailable {
		current.Params.SafeZoneSlots = shapeEntry.Params.SafeZoneSlots
		current.Params.GenesisWindow = shapeEntry.Params.GenesisWindow
		if !shape.SystemStart.IsZero() {
			systemStart = shape.SystemStart
		}
	}
	if !shapeAvailable {
		return &hardfork.Summary{
			SystemStart: systemStart,
			Eras:        append(past, current),
			Transition:  transitionInfo,
		}, nil
	}

	effectiveTransition := transitionInfo
	if transitionInfo.State == hardfork.TransitionImpossible {
		// queryHardForkEraHistory can stop at the confirmed current epoch
		// boundary because it serves a point-in-time answer. Live slot and
		// header processing must remain able to cross that boundary in the
		// same era. Apply the rolling safe zone from the tip while preserving
		// TransitionImpossible on the returned Summary.
		effectiveTransition = hardfork.NewTransitionUnknown()
	}

	summary, err := hardfork.BuildSummary(
		hardfork.Shape{SystemStart: systemStart},
		past,
		current,
		tipSlot,
		effectiveTransition,
	)
	if err != nil {
		return nil, err
	}
	summary.Transition = transitionInfo

	// When a known transition is armed, BuildSummary bounds the current era at
	// the announced epoch boundary (mkUpperBound) and appends no successor era.
	// That leaves the summary's last era bounded, so eraForSlot / SlotToEpoch
	// return ErrPastHorizon for every slot at or past the boundary. The header
	// forecast-horizon gate in verify_header.go then hard-rejects the first
	// header of the post-boundary epoch, and the node can never apply the block
	// that would consume the transition and extend era history — a liveness
	// deadlock at the boundary. Unlike the NtC era-history query, which serves a
	// point-in-time answer and is intentionally bounded, live header
	// verification must see the horizon extend at least one epoch past a known
	// transition, because the rollover is deterministic within the stability
	// window. Append the successor era as an open (unbounded) era starting at
	// the announced boundary so the horizon covers the first post-boundary
	// epoch. Use the next era's params from the shape; when the current era is
	// the last modeled era (the transition re-arms an era the ledger already
	// occupies), reuse the current era's params — epoch length and slot length
	// are constant across post-Byron eras, which is all SlotToEpoch needs.
	// Mirrors the successor-era append in Haskell HFC reconstructSummary.
	if transitionInfo.State == hardfork.TransitionKnown && len(summary.Eras) > 0 {
		bounded := summary.Eras[len(summary.Eras)-1]
		if bounded.End != nil {
			succEraID := bounded.EraID
			succParams := bounded.Params
			if next, ok := shape.EraForID(bounded.EraID + 1); ok {
				succEraID = next.EraID
				succParams = next.Params
			}
			summary.Eras = append(summary.Eras, hardfork.EraSummary{
				EraID: succEraID,
				Start: *bounded.End,
				End:   nil,
				Params: hardfork.EraParams{
					EpochSize:     succParams.EpochSize,
					SlotLength:    succParams.SlotLength,
					SafeZoneSlots: succParams.SafeZoneSlots,
					GenesisWindow: succParams.GenesisWindow,
				},
			})
		}
	}
	return &summary, nil
}
