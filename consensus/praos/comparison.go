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

package praos

import (
	"bytes"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// ChainComparisonResult indicates the result of comparing two chains.
type ChainComparisonResult int

const (
	ChainEqual             ChainComparisonResult = 0
	ChainABetter           ChainComparisonResult = 1
	ChainBBetter           ChainComparisonResult = -1
	ChainComparisonUnknown ChainComparisonResult = 2
)

// ComparePraosTips compares two Praos-era tips using cardano-node's
// equal-length tiebreaker shape:
//  1. Higher block number wins.
//  2. At equal block number, prefer a candidate with the same issuer and slot
//     only when it has a higher opcert issue number.
//  3. Otherwise compare the tip VRF only when the era's VRF tiebreaker flavor
//     is armed. Conway restricts this to tips at most 5 slots apart.
//
// If neither reference implementation rule applies, this returns ChainEqual so
// callers keep the incumbent.
func ComparePraosTips(
	tipA, tipB ochainsync.Tip,
	viewA, viewB PraosTiebreakerView,
) ChainComparisonResult {
	if tipA.BlockNumber > tipB.BlockNumber {
		return ChainABetter
	}
	if tipB.BlockNumber > tipA.BlockNumber {
		return ChainBBetter
	}

	if tipA.Point.Slot == tipB.Point.Slot &&
		bytes.Equal(tipA.Point.Hash, tipB.Point.Hash) {
		return ChainEqual
	}

	if PreferPraosCandidate(viewB, viewA) {
		return ChainABetter
	}
	if PreferPraosCandidate(viewA, viewB) {
		return ChainBBetter
	}
	return ChainEqual
}

// PreferPraosCandidate mirrors ouroboros-consensus'
// preferCandidate cfg ours cand for equal-length Praos chains.
func PreferPraosCandidate(
	ours, cand PraosTiebreakerView,
) bool {
	if praosIssueNoArmed(ours, cand) {
		if cand.IssueNo > ours.IssueNo {
			return true
		}
		if cand.IssueNo < ours.IssueNo {
			return false
		}
	}
	if !praosVRFArmed(ours, cand) {
		return false
	}
	return CompareVRFOutputs(
		cand.TieBreakVRF,
		ours.TieBreakVRF,
	) == ChainABetter
}

func praosIssueNoArmed(ours, cand PraosTiebreakerView) bool {
	return ours.Slot == cand.Slot &&
		ours.hasIssuerIssueNo() &&
		cand.hasIssuerIssueNo() &&
		bytes.Equal(ours.Issuer, cand.Issuer)
}

func praosVRFArmed(ours, cand PraosTiebreakerView) bool {
	config, ok := praosTiebreakerConfig(ours, cand)
	if !ok {
		return false
	}
	switch config.Flavor {
	case PraosTiebreakerUnknown:
		return false
	case PraosTiebreakerUnrestricted:
		return true
	case PraosTiebreakerRestricted:
		return praosSlotDistance(ours.Slot, cand.Slot) <=
			config.MaxSlotDistance
	default:
		return false
	}
}

func praosTiebreakerConfig(
	ours, cand PraosTiebreakerView,
) (PraosTiebreakerConfig, bool) {
	if ours.TiebreakerConfig.known() {
		return ours.TiebreakerConfig, true
	}
	if cand.TiebreakerConfig.known() {
		return cand.TiebreakerConfig, true
	}
	return PraosTiebreakerConfig{}, false
}

func praosSlotDistance(a, b uint64) uint64 {
	if a >= b {
		return a - b
	}
	return b - a
}
