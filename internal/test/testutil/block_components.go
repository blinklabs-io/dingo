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

package testutil

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// BuildBlockBytesFromComponents builds a Shelley-through-Conway block whose
// body components are the caller's exact CBOR items, with a header carrying
// the matching block body hash. The components are not re-encoded, so a test
// can place bytes that gouroboros' own encoders would never emit (an
// auxiliary-data entry at an index past the transaction list, a tagged map
// with an unknown field) and still get past the decoder's body-hash check.
//
// components must be complete CBOR items in wire order: the array of
// transaction bodies, the array of witness sets, the index-to-auxiliary-data
// map, and, from Alonzo on, the array of invalid transaction indexes. The
// header carries the era's protocol major version but no valid VRF, KES or
// operational-certificate material, so the block only suits decode tests.
func BuildBlockBytesFromComponents(
	t *testing.T,
	blockType uint,
	slot, blockNumber uint64,
	components ...[]byte,
) []byte {
	t.Helper()
	var bodyHashes []byte
	for _, component := range components {
		h := lcommon.Blake2b256Hash(component)
		bodyHashes = append(bodyHashes, h.Bytes()...)
	}
	bodyHash := lcommon.Blake2b256Hash(bodyHashes)
	vrf := lcommon.VrfResult{
		Output: make([]byte, 64),
		Proof:  make([]byte, 80),
	}
	var header any
	wantComponents := 4
	switch blockType {
	case gledger.BlockTypeShelley, gledger.BlockTypeAllegra,
		gledger.BlockTypeMary, gledger.BlockTypeAlonzo:
		if blockType != gledger.BlockTypeAlonzo {
			wantComponents = 3
		}
		header = &shelley.ShelleyBlockHeader{
			Body: shelley.ShelleyBlockHeaderBody{
				BlockNumber:       blockNumber,
				Slot:              slot,
				VrfKey:            make([]byte, 32),
				NonceVrf:          vrf,
				LeaderVrf:         vrf,
				BlockBodyHash:     bodyHash,
				OpCertHotVkey:     make([]byte, 32),
				OpCertSignature:   make([]byte, 64),
				ProtoMajorVersion: blockProtocolMajor[blockType],
			},
			Signature: make([]byte, 448),
		}
	case gledger.BlockTypeBabbage, gledger.BlockTypeConway:
		header = &babbage.BabbageBlockHeader{
			Body: babbage.BabbageBlockHeaderBody{
				BlockNumber:   blockNumber,
				Slot:          slot,
				BlockBodyHash: bodyHash,
				VrfKey:        make([]byte, 32),
				VrfResult:     vrf,
				OpCert: babbage.BabbageOpCert{
					HotVkey:   make([]byte, 32),
					Signature: make([]byte, 64),
				},
				ProtoVersion: babbage.BabbageProtoVersion{
					Major: blockProtocolMajor[blockType],
				},
			},
			Signature: make([]byte, 448),
		}
	default:
		require.FailNow(t, "unsupported block type", "block type %d", blockType)
	}
	require.Len(t, components, wantComponents, "block body component count")
	headerRaw, err := cbor.Encode(header)
	require.NoError(t, err)
	// Definite-length array of the header and the body components: four
	// items before Alonzo, five from Alonzo on.
	raw := []byte{0x84}
	if wantComponents == 4 {
		raw[0] = 0x85
	}
	raw = append(raw, headerRaw...)
	for _, component := range components {
		raw = append(raw, component...)
	}
	return raw
}

// blockProtocolMajor is the last protocol major version of each era.
var blockProtocolMajor = map[uint]uint64{
	gledger.BlockTypeShelley: 2,
	gledger.BlockTypeAllegra: 3,
	gledger.BlockTypeMary:    4,
	gledger.BlockTypeAlonzo:  6,
	gledger.BlockTypeBabbage: 8,
	gledger.BlockTypeConway:  10,
}
