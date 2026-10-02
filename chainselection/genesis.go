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

import (
	"math"
	"math/big"
)

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
// Shelley-style networks use ceil(3k/f), where k is the security parameter and
// f is the active slot coefficient, matching the reference node's
// computeStabilityWindow. The division is exact: f must be the genesis
// rational, because a float64 approximation of a value such as 3/10000 rounds
// the quotient across an integer boundary and widens the window by one slot.
// It returns the default window when k is zero or f is nil or not positive,
// saturates at math.MaxUint64, and does not modify activeSlotsCoeff.
func GenesisWindowSlotsForParams(
	securityParam uint64,
	activeSlotsCoeff *big.Rat,
) uint64 {
	if securityParam == 0 ||
		activeSlotsCoeff == nil ||
		activeSlotsCoeff.Sign() <= 0 {
		return defaultGenesisWindowSlots
	}
	// 3k / (num/denom) = 3k*denom / num, rounded up.
	numerator := new(big.Int).SetUint64(securityParam)
	numerator.Mul(numerator, big.NewInt(3))
	numerator.Mul(numerator, activeSlotsCoeff.Denom())
	window, remainder := new(big.Int).QuoRem(
		numerator,
		activeSlotsCoeff.Num(),
		new(big.Int),
	)
	if remainder.Sign() != 0 {
		window.Add(window, big.NewInt(1))
	}
	if !window.IsUint64() {
		return math.MaxUint64
	}
	return window.Uint64()
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
