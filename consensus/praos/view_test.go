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

	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dijkstraHeaderForView(t *testing.T) *dijkstra.DijkstraBlockHeader {
	t.Helper()
	issuer := common.IssuerVkey{}
	issuer[0] = 0xAB
	return &dijkstra.DijkstraBlockHeader{
		BabbageBlockHeader: babbage.BabbageBlockHeader{
			Body: babbage.BabbageBlockHeaderBody{
				BlockNumber: 7,
				Slot:        200,
				IssuerVkey:  issuer,
				VrfResult: common.VrfResult{
					Output: make64ByteVRF(0x11),
				},
				OpCert: babbage.BabbageOpCert{
					SequenceNumber: 3,
				},
			},
		},
	}
}

// TestGetVRFOutput_Dijkstra pins that a Dijkstra header's VRF output is
// extracted, since DijkstraBlockHeader is a distinct concrete type embedding
// BabbageBlockHeader and Go type switches do not fall through to the
// embedded type's case.
func TestGetVRFOutput_Dijkstra(t *testing.T) {
	header := dijkstraHeaderForView(t)
	got := GetVRFOutput(header)
	assert.Equal(t, header.Body.VrfResult.Output, got)
	assert.Len(t, got, VRFOutputSize)
}

// TestGetPraosTiebreakerView_Dijkstra pins that GetPraosTiebreakerView
// returns a complete, usable view for a Dijkstra header rather than the
// (PraosTiebreakerView{}, false) fallback that a missing type-switch case
// produces.
func TestGetPraosTiebreakerView_Dijkstra(t *testing.T) {
	header := dijkstraHeaderForView(t)

	view, ok := GetPraosTiebreakerView(header)
	require.True(t, ok, "Dijkstra header must produce a valid tiebreaker view")

	assert.Equal(t, header.SlotNumber(), view.Slot)
	assert.Equal(t, uint64(3), view.IssueNo)
	issuer := header.IssuerVkey()
	assert.Equal(t, issuer[:], view.Issuer)
	assert.Equal(t, header.Body.VrfResult.Output, view.TieBreakVRF)
	assert.True(t, view.hasIssuerIssueNo())
	assert.Equal(t, PraosTiebreakerConfigConway(), view.TiebreakerConfig,
		"Dijkstra must use the Conway-compatible restricted VRF tiebreaker config")
}

// TestComparePraosTipsDijkstraVRFTiebreak confirms the extracted Dijkstra
// view actually participates in chain-selection comparison: two equal-length
// Dijkstra tips within the restricted-tiebreaker slot distance must be
// decided by VRF output, not treated as an unresolved tie.
func TestComparePraosTipsDijkstraVRFTiebreak(t *testing.T) {
	lowerVRFHeader := dijkstraHeaderForView(t)
	lowerVRFHeader.Body.VrfResult.Output = make64ByteVRFFirstByte(0x01)
	higherVRFHeader := dijkstraHeaderForView(t)
	higherVRFHeader.Body.VrfResult.Output = make64ByteVRFFirstByte(0xFF)
	higherVRFHeader.Body.Slot = 202

	ours := ochainsync.Tip{
		Point:       ocommon.Point{Slot: lowerVRFHeader.Body.Slot, Hash: []byte("ours")},
		BlockNumber: 50,
	}
	candidate := ochainsync.Tip{
		Point:       ocommon.Point{Slot: higherVRFHeader.Body.Slot, Hash: []byte("candidate")},
		BlockNumber: 50,
	}

	oursView, ok := GetPraosTiebreakerView(lowerVRFHeader)
	require.True(t, ok)
	candidateView, ok := GetPraosTiebreakerView(higherVRFHeader)
	require.True(t, ok)

	got := ComparePraosTips(ours, candidate, oursView, candidateView)
	assert.Equal(t, ChainABetter, got,
		"lower VRF output must win the Dijkstra equal-length tiebreak")

	got = ComparePraosTips(candidate, ours, candidateView, oursView)
	assert.Equal(t, ChainBBetter, got, "VRF tiebreak must be symmetric")
}
