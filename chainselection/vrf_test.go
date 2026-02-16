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
	"testing"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
)

func TestCompareVRFOutputs(t *testing.T) {
	testCases := []struct {
		name     string
		vrfA     []byte
		vrfB     []byte
		expected ChainComparisonResult
	}{
		{
			name:     "full 64-byte VRF - A lower wins",
			vrfA:     make64ByteVRF(0x00),
			vrfB:     make64ByteVRF(0x01),
			expected: ChainABetter,
		},
		{
			name:     "full 64-byte VRF - B lower wins",
			vrfA:     make64ByteVRF(0x01),
			vrfB:     make64ByteVRF(0x00),
			expected: ChainBBetter,
		},
		{
			name:     "full 64-byte VRF - equal",
			vrfA:     make64ByteVRF(0x50),
			vrfB:     make64ByteVRF(0x50),
			expected: ChainEqual,
		},
		{
			name:     "64-byte VRF first byte different - A lower wins",
			vrfA:     make64ByteVRFFirstByte(0x00),
			vrfB:     make64ByteVRFFirstByte(0xFF),
			expected: ChainABetter,
		},
		{
			name:     "64-byte VRF first byte different - B lower wins",
			vrfA:     make64ByteVRFFirstByte(0xFF),
			vrfB:     make64ByteVRFFirstByte(0x00),
			expected: ChainBBetter,
		},
		{
			name:     "vrfA nil - equal",
			vrfA:     nil,
			vrfB:     make64ByteVRF(0x01),
			expected: ChainEqual,
		},
		{
			name:     "vrfB nil - equal",
			vrfA:     make64ByteVRF(0x01),
			vrfB:     nil,
			expected: ChainEqual,
		},
		{
			name:     "both nil - equal",
			vrfA:     nil,
			vrfB:     nil,
			expected: ChainEqual,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := CompareVRFOutputs(tc.vrfA, tc.vrfB)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestCompareChainsWithVRF(t *testing.T) {
	testCases := []struct {
		name            string
		tipA            ochainsync.Tip
		tipB            ochainsync.Tip
		blocksInWindowA uint64
		blocksInWindowB uint64
		vrfA            []byte
		vrfB            []byte
		expected        ChainComparisonResult
	}{
		{
			name: "higher block number wins regardless of VRF",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100},
				BlockNumber: 50,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100},
				BlockNumber: 40,
			},
			blocksInWindowA: 100,
			blocksInWindowB: 100,
			vrfA:            make64ByteVRF(0xFF), // Higher VRF
			vrfB:            make64ByteVRF(0x00), // Lower VRF
			expected:        ChainABetter,        // Block number wins over VRF
		},
		{
			name: "equal block number - higher density wins regardless of VRF",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100},
				BlockNumber: 50,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100},
				BlockNumber: 50,
			},
			blocksInWindowA: 100,                 // Higher density
			blocksInWindowB: 80,                  // Lower density
			vrfA:            make64ByteVRF(0xFF), // Higher VRF
			vrfB:            make64ByteVRF(0x00), // Lower VRF
			expected:        ChainABetter,        // Density wins over VRF
		},
		{
			name: "equal block number and density - VRF tie-breaker A wins",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100},
				BlockNumber: 50,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100},
				BlockNumber: 50,
			},
			blocksInWindowA: 100,
			blocksInWindowB: 100,
			vrfA:            make64ByteVRF(0x00), // Lower VRF wins
			vrfB:            make64ByteVRF(0xFF),
			expected:        ChainABetter,
		},
		{
			name: "equal block number and density - VRF tie-breaker B wins",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100},
				BlockNumber: 50,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100},
				BlockNumber: 50,
			},
			blocksInWindowA: 100,
			blocksInWindowB: 100,
			vrfA:            make64ByteVRF(0xFF), // Higher VRF loses
			vrfB:            make64ByteVRF(0x00), // Lower VRF wins
			expected:        ChainBBetter,
		},
		{
			name: "equal VRF - falls back to slot comparison A wins",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 90},
				BlockNumber: 50,
			}, // Lower slot
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100},
				BlockNumber: 50,
			}, // Higher slot
			blocksInWindowA: 100,
			blocksInWindowB: 100,
			vrfA:            make64ByteVRF(0x50),
			vrfB:            make64ByteVRF(0x50), // Equal VRF
			expected:        ChainABetter,        // Lower slot wins
		},
		{
			name: "nil VRF - falls back to slot comparison",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 90},
				BlockNumber: 50,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100},
				BlockNumber: 50,
			},
			blocksInWindowA: 100,
			blocksInWindowB: 100,
			vrfA:            nil,
			vrfB:            nil,
			expected:        ChainABetter, // Lower slot wins as fallback
		},
		{
			name: "one nil VRF - falls back to slot comparison",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100},
				BlockNumber: 50,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 90},
				BlockNumber: 50,
			},
			blocksInWindowA: 100,
			blocksInWindowB: 100,
			vrfA:            make64ByteVRF(0x00), // Has VRF but B is nil
			vrfB:            nil,
			expected:        ChainBBetter, // B has lower slot, VRF comparison skipped
		},
		{
			name: "completely equal chains",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100},
				BlockNumber: 50,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100},
				BlockNumber: 50,
			},
			blocksInWindowA: 100,
			blocksInWindowB: 100,
			vrfA:            make64ByteVRF(0x50),
			vrfB:            make64ByteVRF(0x50),
			expected:        ChainEqual,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := CompareChainsWithVRF(
				tc.tipA, tc.tipB,
				tc.blocksInWindowA, tc.blocksInWindowB,
				tc.vrfA, tc.vrfB,
			)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestIsBetterChainWithVRF(t *testing.T) {
	// Test that IsBetterChainWithVRF correctly wraps CompareChainsWithVRF
	newTip := ochainsync.Tip{Point: ocommon.Point{Slot: 100}, BlockNumber: 50}
	currentTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100},
		BlockNumber: 50,
	}

	// New chain has lower VRF - should be better
	assert.True(t, IsBetterChainWithVRF(
		newTip, currentTip,
		100, 100, // Equal density
		make64ByteVRF(0x00), make64ByteVRF(0xFF), // New has lower VRF
	))

	// New chain has higher VRF - should not be better
	assert.False(t, IsBetterChainWithVRF(
		newTip, currentTip,
		100, 100, // Equal density
		make64ByteVRF(0xFF), make64ByteVRF(0x00), // New has higher VRF
	))
}

func TestCompareVRFOutputs_InvalidLength(t *testing.T) {
	testCases := []struct {
		name     string
		vrfA     []byte
		vrfB     []byte
		expected ChainComparisonResult
	}{
		{
			name:     "short vrfA should not win despite lower value",
			vrfA:     []byte{0x00},
			vrfB:     make64ByteVRF(0x01),
			expected: ChainEqual,
		},
		{
			name:     "short vrfB should not win despite lower value",
			vrfA:     make64ByteVRF(0x01),
			vrfB:     []byte{0x00},
			expected: ChainEqual,
		},
		{
			name:     "both short - equal",
			vrfA:     []byte{0x00, 0x00, 0x00, 0x01},
			vrfB:     []byte{0x00, 0x00, 0x00, 0x02},
			expected: ChainEqual,
		},
		{
			name:     "empty vrfA - equal",
			vrfA:     []byte{},
			vrfB:     make64ByteVRF(0x01),
			expected: ChainEqual,
		},
		{
			name:     "empty vrfB - equal",
			vrfA:     make64ByteVRF(0x01),
			vrfB:     []byte{},
			expected: ChainEqual,
		},
		{
			name:     "both empty - equal",
			vrfA:     []byte{},
			vrfB:     []byte{},
			expected: ChainEqual,
		},
		{
			name:     "oversized vrfA - equal",
			vrfA:     make([]byte, VRFOutputSize+1),
			vrfB:     make64ByteVRF(0xFF),
			expected: ChainEqual,
		},
		{
			name:     "oversized vrfB - equal",
			vrfA:     make64ByteVRF(0xFF),
			vrfB:     make([]byte, VRFOutputSize+1),
			expected: ChainEqual,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := CompareVRFOutputs(tc.vrfA, tc.vrfB)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// Helper function to create a 64-byte VRF output filled with a specific value
func make64ByteVRF(fill byte) []byte {
	vrf := make([]byte, VRFOutputSize)
	for i := range vrf {
		vrf[i] = fill
	}
	return vrf
}

// Helper to create a 64-byte VRF with only the first byte set, rest zeros
func make64ByteVRFFirstByte(first byte) []byte {
	vrf := make([]byte, VRFOutputSize)
	vrf[0] = first
	return vrf
}
