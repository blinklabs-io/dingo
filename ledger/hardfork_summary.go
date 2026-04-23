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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
)

// HardForkSummary constructs a hardfork.Summary describing the chain's era
// history from the LedgerState's current epoch cache, tip, current era, and
// transition info.
//
// The returned Summary's past eras are closed with bounds computed by walking
// the epoch cache grouped by EraId. The current era (the last group) is left
// unbounded (SafeZoneSlots == 0), preserving the existing "project forward
// using the current era's parameters" behavior of LedgerState.SlotToTime and
// friends. Once dingo tracks per-era safe zones end-to-end, this will flip to
// a bounded end sourced from BuildSummary + the real TransitionInfo.
func (ls *LedgerState) HardForkSummary() (*hardfork.Summary, error) {
	if ls.config.CardanoNodeConfig == nil {
		return nil, errors.New("ledger: nil CardanoNodeConfig")
	}
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return nil, errors.New("ledger: Shelley genesis unavailable")
	}

	ls.RLock()
	cache := make([]models.Epoch, len(ls.epochCache))
	copy(cache, ls.epochCache)
	transitionInfo := ls.transitionInfo
	ls.RUnlock()

	if len(cache) == 0 {
		return nil, errors.New("ledger: no epochs in cache")
	}

	// Walk the epoch cache grouping contiguous epochs by EraId. Each group
	// becomes one EraSummary; its Start is derived from the first epoch of
	// the group, and its End (for past eras) is the Start of the next group.
	var eras []hardfork.EraSummary
	relTime := time.Duration(0)

	i := 0
	for i < len(cache) {
		first := cache[i]
		eraID := first.EraId
		// Per-epoch params within an era are expected to be constant; we use
		// the first epoch's values as the era-level params.
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

		era := hardfork.EraSummary{
			EraID: eraID,
			Start: start,
			Params: hardfork.EraParams{
				EpochSize:     epochSize,
				SlotLength:    slotLen,
				SafeZoneSlots: 0, // UnsafeIndefiniteSafeZone — preserve projection
				GenesisWindow: 0,
			},
		}

		isLast := j == len(cache)
		if !isLast {
			// Close this era at the next era's start.
			era.End = &end
		}
		eras = append(eras, era)

		i = j
	}

	return &hardfork.Summary{
		SystemStart: shelleyGenesis.SystemStart,
		Eras:        eras,
		Transition:  transitionInfo,
	}, nil
}
