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

// ComparePraosTips compares two tips using cardano-node's equal-length
// tiebreaker shape:
//  1. Higher block number wins.
//  2. At equal block number, a Byron epoch-boundary block beats a Byron
//     regular block sharing its predecessor's block number: canonical Byron
//     PBFT counts the boundary block as an additional block despite the
//     shared number. This rule only fires when at least one side is a Byron
//     header (view.Byron != ByronBlockKindNone); Shelley-family tips are
//     unaffected and fall through to the rules below exactly as before.
//  3. Otherwise, for a Praos-era view, prefer a candidate with the same
//     issuer and slot only when it has a higher opcert issue number.
//  4. Otherwise compare the tip VRF only when the era's VRF tiebreaker flavor
//     is armed. Conway restricts this to tips at most 5 slots apart.
//
// If no rule applies, this returns ChainEqual so callers keep the incumbent.
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

	if result := compareByronBlockKind(viewA.Byron, viewB.Byron); result != ChainEqual {
		return result
	}

	if PreferPraosCandidate(viewB, viewA) {
		return ChainABetter
	}
	if PreferPraosCandidate(viewA, viewB) {
		return ChainBBetter
	}
	return ChainEqual
}

// compareByronBlockKind applies the Byron EBB-over-regular tiebreak. It is a
// no-op (ChainEqual) unless both sides are Byron headers: a Shelley-family
// tip always reports ByronBlockKindNone, and mixing a Byron kind with None
// (an era-transition tip pair) is left to the existing Praos rules, which
// already return ChainEqual for a view with no issuer/VRF data.
func compareByronBlockKind(a, b ByronBlockKind) ChainComparisonResult {
	if a == ByronBlockKindNone || b == ByronBlockKindNone || a == b {
		return ChainEqual
	}
	if a == ByronBlockKindEBB {
		return ChainABetter
	}
	return ChainBBetter
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
