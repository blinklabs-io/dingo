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
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// ChainComparisonResult indicates the result of comparing two chains.
type ChainComparisonResult int

const (
	ChainEqual   ChainComparisonResult = 0
	ChainABetter ChainComparisonResult = 1
	ChainBBetter ChainComparisonResult = -1
)

// CompareChains compares two chain tips according to Ouroboros Praos rules:
// 1. Higher block number wins (longer chain)
// 2. At equal block number, lower slot wins (denser chain)
//
// Returns:
//   - ChainABetter (1) if tipA represents a better chain
//   - ChainBBetter (-1) if tipB represents a better chain
//   - ChainEqual (0) if they are equal
func CompareChains(tipA, tipB ochainsync.Tip) ChainComparisonResult {
	// Rule 1: Higher block number wins (longer chain)
	if tipA.BlockNumber > tipB.BlockNumber {
		return ChainABetter
	}
	if tipB.BlockNumber > tipA.BlockNumber {
		return ChainBBetter
	}

	// Rule 2: At equal block number, lower slot wins (denser chain)
	if tipA.Point.Slot < tipB.Point.Slot {
		return ChainABetter
	}
	if tipB.Point.Slot < tipA.Point.Slot {
		return ChainBBetter
	}

	// Chains are equal
	return ChainEqual
}

// IsBetterChain returns true if newTip represents a better chain than
// currentTip according to Ouroboros Praos rules.
func IsBetterChain(newTip, currentTip ochainsync.Tip) bool {
	return CompareChains(newTip, currentTip) == ChainABetter
}

// IsSignificantlyBetter returns true if newTip is better than currentTip by
// at least the specified minimum block difference. This can be used to avoid
// frequent chain switches for marginal improvements.
func IsSignificantlyBetter(
	newTip, currentTip ochainsync.Tip,
	minBlockDiff uint64,
) bool {
	if newTip.BlockNumber <= currentTip.BlockNumber {
		return false
	}
	return newTip.BlockNumber-currentTip.BlockNumber >= minBlockDiff
}

// CompareChainsWithDensity compares two chain tips using density-based
// comparison according to Ouroboros Praos rules:
// 1. Higher block number wins (longer chain)
// 2. At equal block number, higher density wins (more blocks in 3k/f window)
// 3. At equal density, lower slot wins (tie-breaker)
//
// The blocksInWindowA and blocksInWindowB parameters represent the number of
// blocks in the stability window (3k/f slots) for each chain tip. These values
// should be computed by counting blocks within the window ending at each tip.
//
// Returns:
//   - ChainABetter (1) if tipA represents a better chain
//   - ChainBBetter (-1) if tipB represents a better chain
//   - ChainEqual (0) if they are equal
func CompareChainsWithDensity(
	tipA, tipB ochainsync.Tip,
	blocksInWindowA, blocksInWindowB uint64,
) ChainComparisonResult {
	// Rule 1: Higher block number wins (longer chain)
	if tipA.BlockNumber > tipB.BlockNumber {
		return ChainABetter
	}
	if tipB.BlockNumber > tipA.BlockNumber {
		return ChainBBetter
	}

	// Rule 2: At equal block number, higher density wins
	// (more blocks in 3k/f window)
	if blocksInWindowA > blocksInWindowB {
		return ChainABetter
	}
	if blocksInWindowB > blocksInWindowA {
		return ChainBBetter
	}

	// Rule 3: At equal density, lower slot wins (tie-breaker)
	if tipA.Point.Slot < tipB.Point.Slot {
		return ChainABetter
	}
	if tipB.Point.Slot < tipA.Point.Slot {
		return ChainBBetter
	}

	// Chains are equal
	return ChainEqual
}

// IsBetterChainWithDensity returns true if newTip represents a better chain
// than currentTip according to Ouroboros Praos density-based rules.
func IsBetterChainWithDensity(
	newTip, currentTip ochainsync.Tip,
	blocksInWindowNew, blocksInWindowCurrent uint64,
) bool {
	return CompareChainsWithDensity(
		newTip,
		currentTip,
		blocksInWindowNew,
		blocksInWindowCurrent,
	) == ChainABetter
}

// CompareChainsWithVRF compares two chain tips using the complete Ouroboros
// Praos chain selection algorithm including VRF tie-breaking:
// 1. Higher block number wins (longer chain)
// 2. At equal block number, higher density wins (more blocks in 3k/f window)
// 3. At equal density, lower VRF output wins (deterministic tie-breaker)
//
// The vrfOutputA and vrfOutputB parameters are the VRF outputs from the tip
// blocks of each chain. These should be extracted using GetVRFOutput() from
// the block headers. If VRF outputs are nil, falls back to slot-based comparison.
//
// Returns:
//   - ChainABetter (1) if tipA represents a better chain
//   - ChainBBetter (-1) if tipB represents a better chain
//   - ChainEqual (0) if they are equal
func CompareChainsWithVRF(
	tipA, tipB ochainsync.Tip,
	blocksInWindowA, blocksInWindowB uint64,
	vrfOutputA, vrfOutputB []byte,
) ChainComparisonResult {
	// Rule 1: Higher block number wins (longer chain)
	if tipA.BlockNumber > tipB.BlockNumber {
		return ChainABetter
	}
	if tipB.BlockNumber > tipA.BlockNumber {
		return ChainBBetter
	}

	// Rule 2: At equal block number, higher density wins
	// (more blocks in 3k/f window)
	if blocksInWindowA > blocksInWindowB {
		return ChainABetter
	}
	if blocksInWindowB > blocksInWindowA {
		return ChainBBetter
	}

	// Rule 3: At equal density, lower VRF output wins (per Ouroboros Praos)
	if vrfOutputA != nil && vrfOutputB != nil {
		vrfResult := CompareVRFOutputs(vrfOutputA, vrfOutputB)
		if vrfResult != ChainEqual {
			return vrfResult
		}
	}

	// Fallback: At equal VRF (or if VRF unavailable), lower slot wins
	if tipA.Point.Slot < tipB.Point.Slot {
		return ChainABetter
	}
	if tipB.Point.Slot < tipA.Point.Slot {
		return ChainBBetter
	}

	// Chains are equal
	return ChainEqual
}

// IsBetterChainWithVRF returns true if newTip represents a better chain than
// currentTip according to the complete Ouroboros Praos rules including VRF.
func IsBetterChainWithVRF(
	newTip, currentTip ochainsync.Tip,
	blocksInWindowNew, blocksInWindowCurrent uint64,
	vrfOutputNew, vrfOutputCurrent []byte,
) bool {
	return CompareChainsWithVRF(
		newTip,
		currentTip,
		blocksInWindowNew,
		blocksInWindowCurrent,
		vrfOutputNew,
		vrfOutputCurrent,
	) == ChainABetter
}
