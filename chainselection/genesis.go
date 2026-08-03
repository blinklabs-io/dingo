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

package chainselection

import "math"

const defaultGenesisWindowSlots uint64 = 6480

// SelectionMode describes the chain-selection strategy currently in use.
type SelectionMode uint8

const (
	SelectionModePraos SelectionMode = iota
	SelectionModeGenesis
)

func (m SelectionMode) String() string {
	switch m {
	case SelectionModePraos:
		return "praos"
	case SelectionModeGenesis:
		return "genesis"
	default:
		return "praos"
	}
}

// GenesisWindowSlotsForParams returns the Genesis density window in slots.
// Shelley-style networks use 3k/f, where k is the security parameter and f is
// the active slot coefficient.
func GenesisWindowSlotsForParams(
	securityParam uint64,
	activeSlotsCoeff float64,
) uint64 {
	if securityParam == 0 ||
		activeSlotsCoeff <= 0 ||
		math.IsNaN(activeSlotsCoeff) ||
		math.IsInf(activeSlotsCoeff, 0) {
		return defaultGenesisWindowSlots
	}
	window := math.Ceil(3 * float64(securityParam) / activeSlotsCoeff)
	if window <= 0 {
		return defaultGenesisWindowSlots
	}
	if window >= float64(math.MaxUint64) {
		return math.MaxUint64
	}
	return uint64(window)
}

// DensityFromIntersection counts candidate blocks in the Genesis window that
// starts immediately after the common intersection and ends at
// intersectionSlot+window, inclusive. The intersection block itself belongs
// to both candidates and is therefore excluded.
//
// Slots must describe one candidate path in ascending order. Values outside
// the window are ignored so callers may pass the complete fetched fragment.
func DensityFromIntersection(
	intersectionSlot uint64,
	window uint64,
	slots []uint64,
) uint64 {
	if window == 0 || len(slots) == 0 {
		return 0
	}
	windowEnd := intersectionSlot + window
	if windowEnd < intersectionSlot {
		windowEnd = math.MaxUint64
	}
	var density uint64
	for _, slot := range slots {
		if slot <= intersectionSlot {
			continue
		}
		if slot > windowEnd {
			break
		}
		density++
	}
	return density
}
