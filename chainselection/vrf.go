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
	"bytes"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// VRFOutputSize is the expected size of a VRF output in bytes.
const VRFOutputSize = 64

// GetVRFOutput extracts the VRF output from a block header.
// Returns nil if the header type is not supported or doesn't contain VRF data.
//
// The VRF output is used for tie-breaking in chain selection when two chains
// have equal block numbers and equal density. The chain with the LOWER VRF
// output (lexicographically) wins.
func GetVRFOutput(header ledger.BlockHeader) []byte {
	switch h := header.(type) {
	case *shelley.ShelleyBlockHeader:
		return h.Body.LeaderVrf.Output
	case *allegra.AllegraBlockHeader:
		return h.Body.LeaderVrf.Output
	case *mary.MaryBlockHeader:
		return h.Body.LeaderVrf.Output
	case *alonzo.AlonzoBlockHeader:
		return h.Body.LeaderVrf.Output
	case *babbage.BabbageBlockHeader:
		return h.Body.VrfResult.Output
	case *conway.ConwayBlockHeader:
		return h.Body.VrfResult.Output
	default:
		return nil
	}
}

// CompareVRFOutputs compares two VRF outputs for chain selection tie-breaking.
// Returns:
//   - ChainABetter (1) if vrfA is lower (A wins)
//   - ChainBBetter (-1) if vrfB is lower (B wins)
//   - ChainEqual (0) if equal or either is nil
//
// Per Ouroboros Praos: the chain with the LOWER VRF output wins the tie-break.
func CompareVRFOutputs(vrfA, vrfB []byte) ChainComparisonResult {
	// Can't compare if either is nil
	if vrfA == nil || vrfB == nil {
		return ChainEqual
	}

	// Lower VRF output wins
	cmp := bytes.Compare(vrfA, vrfB)
	if cmp < 0 {
		return ChainABetter // vrfA is lower, A wins
	}
	if cmp > 0 {
		return ChainBBetter // vrfB is lower, B wins
	}
	return ChainEqual
}

// CompareHeaders compares two block headers for chain selection tie-breaking
// using their VRF outputs. This is a convenience wrapper around GetVRFOutput
// and CompareVRFOutputs.
func CompareHeaders(headerA, headerB ledger.BlockHeader) ChainComparisonResult {
	vrfA := GetVRFOutput(headerA)
	vrfB := GetVRFOutput(headerB)
	return CompareVRFOutputs(vrfA, vrfB)
}
