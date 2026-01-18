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

func TestCompareChains(t *testing.T) {
	tests := []struct {
		name     string
		tipA     ochainsync.Tip
		tipB     ochainsync.Tip
		expected ChainComparisonResult
	}{
		{
			name: "higher block number wins",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("a")},
				BlockNumber: 50,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("b")},
				BlockNumber: 40,
			},
			expected: ChainABetter,
		},
		{
			name: "lower block number loses",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("a")},
				BlockNumber: 40,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("b")},
				BlockNumber: 50,
			},
			expected: ChainBBetter,
		},
		{
			name: "equal block number lower slot wins",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 90, Hash: []byte("a")},
				BlockNumber: 50,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("b")},
				BlockNumber: 50,
			},
			expected: ChainABetter,
		},
		{
			name: "equal block number higher slot loses",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("a")},
				BlockNumber: 50,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 90, Hash: []byte("b")},
				BlockNumber: 50,
			},
			expected: ChainBBetter,
		},
		{
			name: "equal chains",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("a")},
				BlockNumber: 50,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("b")},
				BlockNumber: 50,
			},
			expected: ChainEqual,
		},
		{
			name: "origin tips",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 0, Hash: nil},
				BlockNumber: 0,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 0, Hash: nil},
				BlockNumber: 0,
			},
			expected: ChainEqual,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareChains(tt.tipA, tt.tipB)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsBetterChain(t *testing.T) {
	newTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("new")},
		BlockNumber: 50,
	}
	currentTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 90, Hash: []byte("current")},
		BlockNumber: 45,
	}
	equalTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("equal")},
		BlockNumber: 50,
	}

	assert.True(t, IsBetterChain(newTip, currentTip))
	assert.False(t, IsBetterChain(currentTip, newTip))
	assert.False(t, IsBetterChain(newTip, equalTip))
}

func TestIsSignificantlyBetter(t *testing.T) {
	tests := []struct {
		name         string
		newTip       ochainsync.Tip
		currentTip   ochainsync.Tip
		minBlockDiff uint64
		expected     bool
	}{
		{
			name: "significantly better",
			newTip: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("new")},
				BlockNumber: 55,
			},
			currentTip: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 90, Hash: []byte("current")},
				BlockNumber: 50,
			},
			minBlockDiff: 5,
			expected:     true,
		},
		{
			name: "not significantly better - below threshold",
			newTip: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("new")},
				BlockNumber: 52,
			},
			currentTip: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 90, Hash: []byte("current")},
				BlockNumber: 50,
			},
			minBlockDiff: 5,
			expected:     false,
		},
		{
			name: "equal block numbers",
			newTip: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("new")},
				BlockNumber: 50,
			},
			currentTip: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 90, Hash: []byte("current")},
				BlockNumber: 50,
			},
			minBlockDiff: 1,
			expected:     false,
		},
		{
			name: "new tip worse",
			newTip: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("new")},
				BlockNumber: 45,
			},
			currentTip: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 90, Hash: []byte("current")},
				BlockNumber: 50,
			},
			minBlockDiff: 1,
			expected:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsSignificantlyBetter(
				tt.newTip,
				tt.currentTip,
				tt.minBlockDiff,
			)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCompareChainsWithDensity(t *testing.T) {
	tests := []struct {
		name            string
		tipA            ochainsync.Tip
		tipB            ochainsync.Tip
		blocksInWindowA uint64
		blocksInWindowB uint64
		expected        ChainComparisonResult
	}{
		{
			name: "higher block number wins regardless of density",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("a")},
				BlockNumber: 50,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("b")},
				BlockNumber: 40,
			},
			blocksInWindowA: 10,
			blocksInWindowB: 20, // B has higher density but lower block number
			expected:        ChainABetter,
		},
		{
			name: "lower block number loses regardless of density",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("a")},
				BlockNumber: 40,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("b")},
				BlockNumber: 50,
			},
			blocksInWindowA: 20, // A has higher density but lower block number
			blocksInWindowB: 10,
			expected:        ChainBBetter,
		},
		{
			name: "equal block number higher density wins",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("a")},
				BlockNumber: 50,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("b")},
				BlockNumber: 50,
			},
			blocksInWindowA: 25,
			blocksInWindowB: 20,
			expected:        ChainABetter,
		},
		{
			name: "equal block number lower density loses",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("a")},
				BlockNumber: 50,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("b")},
				BlockNumber: 50,
			},
			blocksInWindowA: 20,
			blocksInWindowB: 25,
			expected:        ChainBBetter,
		},
		{
			name: "equal block number and density lower slot wins",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 90, Hash: []byte("a")},
				BlockNumber: 50,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("b")},
				BlockNumber: 50,
			},
			blocksInWindowA: 20,
			blocksInWindowB: 20,
			expected:        ChainABetter,
		},
		{
			name: "equal block number and density higher slot loses",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("a")},
				BlockNumber: 50,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 90, Hash: []byte("b")},
				BlockNumber: 50,
			},
			blocksInWindowA: 20,
			blocksInWindowB: 20,
			expected:        ChainBBetter,
		},
		{
			name: "completely equal chains",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("a")},
				BlockNumber: 50,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 100, Hash: []byte("b")},
				BlockNumber: 50,
			},
			blocksInWindowA: 20,
			blocksInWindowB: 20,
			expected:        ChainEqual,
		},
		{
			name: "origin tips with zero density",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 0, Hash: nil},
				BlockNumber: 0,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 0, Hash: nil},
				BlockNumber: 0,
			},
			blocksInWindowA: 0,
			blocksInWindowB: 0,
			expected:        ChainEqual,
		},
		{
			name: "density matters only at equal block number",
			tipA: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 1000, Hash: []byte("a")},
				BlockNumber: 100,
			},
			tipB: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 900, Hash: []byte("b")},
				BlockNumber: 100,
			},
			blocksInWindowA: 50,
			blocksInWindowB: 40,
			expected:        ChainABetter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareChainsWithDensity(
				tt.tipA,
				tt.tipB,
				tt.blocksInWindowA,
				tt.blocksInWindowB,
			)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsBetterChainWithDensity(t *testing.T) {
	newTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("new")},
		BlockNumber: 50,
	}
	currentTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 90, Hash: []byte("current")},
		BlockNumber: 45,
	}
	equalBlockTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("equal")},
		BlockNumber: 50,
	}

	// New tip has higher block number - wins regardless of density
	assert.True(
		t,
		IsBetterChainWithDensity(newTip, currentTip, 10, 20),
	)
	assert.False(
		t,
		IsBetterChainWithDensity(currentTip, newTip, 20, 10),
	)

	// Equal block numbers - density decides
	assert.True(
		t,
		IsBetterChainWithDensity(newTip, equalBlockTip, 25, 20),
	)
	assert.False(
		t,
		IsBetterChainWithDensity(newTip, equalBlockTip, 20, 25),
	)

	// Equal block numbers and density - slot decides
	assert.False(
		t,
		IsBetterChainWithDensity(newTip, equalBlockTip, 20, 20),
	)
}
