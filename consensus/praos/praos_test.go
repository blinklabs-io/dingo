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
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- comparison tests -------------------------------------------------------

func TestComparePraosTipsLongerChainWins(t *testing.T) {
	shorter := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("shorter")},
		BlockNumber: 40,
	}
	longer := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 101, Hash: []byte("longer")},
		BlockNumber: 41,
	}

	assert.Equal(t, ChainBBetter,
		ComparePraosTips(shorter, longer, PraosTiebreakerView{}, PraosTiebreakerView{}))
	assert.Equal(t, ChainABetter,
		ComparePraosTips(longer, shorter, PraosTiebreakerView{}, PraosTiebreakerView{}))
}

func TestComparePraosTipsEqualLengthDoesNotUseSlotOrHashFallback(t *testing.T) {
	lowerSlotLowerHash := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 90, Hash: []byte("a")},
		BlockNumber: 50,
	}
	higherSlotHigherHash := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("z")},
		BlockNumber: 50,
	}

	assert.Equal(t, ChainEqual,
		ComparePraosTips(lowerSlotLowerHash, higherSlotHigherHash,
			PraosTiebreakerView{}, PraosTiebreakerView{}))
}

func TestComparePraosTipsSameIssuerSlotHigherOCertWins(t *testing.T) {
	ours := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("ours")},
		BlockNumber: 50,
	}
	candidate := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("candidate")},
		BlockNumber: 50,
	}
	issuer := []byte("issuer")
	oursView := NewPraosTiebreakerViewFull(
		ours, issuer, 1, make64ByteVRFFirstByte(0xAA), PraosTiebreakerConfigConway(),
	)
	candidateView := NewPraosTiebreakerViewFull(
		candidate, issuer, 2, make64ByteVRFFirstByte(0xAA), PraosTiebreakerConfigConway(),
	)

	assert.Equal(t, ChainBBetter, ComparePraosTips(ours, candidate, oursView, candidateView),
		"higher opcert issue number must win at equal slot+issuer")
}

func TestComparePraosTipsVRFPrecedesSlot(t *testing.T) {
	lowerSlotHigherVRF := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 111962097, Hash: []byte("ours")},
		BlockNumber: 4275844,
	}
	laterSlotLowerVRF := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 111962102, Hash: []byte("candidate")},
		BlockNumber: 4275844,
	}

	result := ComparePraosTips(
		lowerSlotHigherVRF,
		laterSlotLowerVRF,
		PraosTiebreakerViewFromTip(lowerSlotHigherVRF, make64ByteVRFFirstByte(0xBD), PraosTiebreakerConfigConway()),
		PraosTiebreakerViewFromTip(laterSlotLowerVRF, make64ByteVRFFirstByte(0x6E), PraosTiebreakerConfigConway()),
	)
	assert.Equal(t, ChainBBetter, result,
		"equal-height Praos comparison must use VRF before slot")
}

func TestComparePraosTipsDoesNotInventFallbackWithoutVRF(t *testing.T) {
	lowerSlot := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 90, Hash: []byte("lower-slot")},
		BlockNumber: 50,
	}
	higherSlot := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("higher-slot")},
		BlockNumber: 50,
	}

	result := ComparePraosTips(
		lowerSlot, higherSlot,
		PraosTiebreakerViewFromTip(lowerSlot, nil, PraosTiebreakerConfigConway()),
		PraosTiebreakerViewFromTip(higherSlot, nil, PraosTiebreakerConfigConway()),
	)
	assert.Equal(t, ChainEqual, result)
}

func TestComparePraosTipsConwayRestrictionDisarmsDistantVRF(t *testing.T) {
	ours := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("ours")},
		BlockNumber: 50,
	}
	candidate := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 106, Hash: []byte("candidate")},
		BlockNumber: 50,
	}

	result := ComparePraosTips(
		ours, candidate,
		PraosTiebreakerViewFromTip(ours, make64ByteVRFFirstByte(0xBD), PraosTiebreakerConfigConway()),
		PraosTiebreakerViewFromTip(candidate, make64ByteVRFFirstByte(0x6E), PraosTiebreakerConfigConway()),
	)
	assert.Equal(t, ChainEqual, result,
		"Conway restriction must disarm VRF tiebreaker when tips are >5 slots apart")
}

func TestComparePraosTipsSlotBattleWithoutSelectViewDoesNotInventHashTieBreak(t *testing.T) {
	dingoTip := ochainsync.Tip{
		BlockNumber: 11,
		Point: ocommon.Point{
			Slot: 40,
			Hash: []byte{
				0x5c, 0x25, 0x9f, 0x44, 0x42, 0xe1, 0x49, 0x33,
				0x72, 0xe2, 0x07, 0x83, 0xaa, 0xb3, 0x06, 0x39,
				0x13, 0x75, 0x44, 0x42, 0xe5, 0x98, 0xa0, 0x54,
				0xad, 0xe8, 0xb6, 0x26, 0xef, 0xe7, 0x4c, 0xe1,
			},
		},
	}
	cardanoTip := ochainsync.Tip{
		BlockNumber: 11,
		Point: ocommon.Point{
			Slot: 40,
			Hash: []byte{
				0x0c, 0x54, 0x11, 0x82, 0xc9, 0xa9, 0xa6, 0x6d,
				0xa2, 0xf7, 0xc3, 0x1a, 0x1f, 0x9b, 0x9e, 0x4a,
				0xeb, 0xf0, 0xfe, 0x0f, 0xed, 0x41, 0xe8, 0x04,
				0xb7, 0x21, 0xac, 0xf5, 0x78, 0x3a, 0x64, 0x03,
			},
		},
	}

	got := ComparePraosTips(
		dingoTip, cardanoTip,
		PraosTiebreakerViewFromTip(dingoTip, nil, PraosTiebreakerConfigConway()),
		PraosTiebreakerViewFromTip(cardanoTip, nil, PraosTiebreakerConfigConway()),
	)
	require.Equal(t, ChainEqual, got)
}

// --- VRF comparison tests ---------------------------------------------------

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
			assert.Equal(t, tc.expected, CompareVRFOutputs(tc.vrfA, tc.vrfB))
		})
	}
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
			assert.Equal(t, tc.expected, CompareVRFOutputs(tc.vrfA, tc.vrfB))
		})
	}
}

// TestGetVRFOutput_ByronReturnsNil pins the cross-era no-tiebreaker invariant
// for Byron<->Shelley boundaries. Byron blocks have no VRF (PBFT, not Praos),
// so any chain-selection tie that includes a Byron tip must not use a
// VRF-derived field.
func TestGetVRFOutput_ByronReturnsNil(t *testing.T) {
	byronHeader := &byron.ByronMainBlockHeader{}
	assert.Nil(t, GetVRFOutput(byronHeader),
		"Byron headers must yield nil VRF output (no VRF in PBFT)")

	shelleyHeader := &shelley.ShelleyBlockHeader{}
	cmp := CompareVRFOutputs(
		GetVRFOutput(byronHeader),
		GetVRFOutput(shelleyHeader),
	)
	assert.Equal(t, ChainEqual, cmp,
		"a tie that includes a Byron tip must not be broken by VRF "+
			"(NoTiebreakerAcrossEras at Byron<->Shelley)")
	cmp = CompareVRFOutputs(
		GetVRFOutput(shelleyHeader),
		GetVRFOutput(byronHeader),
	)
	assert.Equal(t, ChainEqual, cmp,
		"NoTiebreakerAcrossEras must be symmetric")
}

// --- helpers ----------------------------------------------------------------

func make64ByteVRF(fill byte) []byte {
	vrf := make([]byte, VRFOutputSize)
	for i := range vrf {
		vrf[i] = fill
	}
	return vrf
}

func make64ByteVRFFirstByte(first byte) []byte {
	vrf := make([]byte, VRFOutputSize)
	vrf[0] = first
	return vrf
}
