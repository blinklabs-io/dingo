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
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// BuildConwayBlockBytesFromComponents builds a Conway block whose body
// components are the caller's exact CBOR items, with a header carrying the
// matching block body hash. The components are not re-encoded, so a test can
// place bytes that gouroboros' own encoders would never emit (an
// auxiliary-data entry at an index past the transaction list, a tagged map
// with an unknown field) and still reach the decoder's body-hash check.
//
// Each of bodies, witnesses, auxData and invalid must already be a complete
// CBOR item: an array of transaction bodies, an array of witness sets, the
// index-to-auxiliary-data map, and the array of invalid transaction indexes.
func BuildConwayBlockBytesFromComponents(
	t *testing.T,
	slot, blockNumber uint64,
	bodies, witnesses, auxData, invalid []byte,
) []byte {
	t.Helper()
	components := [][]byte{bodies, witnesses, auxData, invalid}
	var concat []byte
	for _, component := range components {
		h := lcommon.Blake2b256Hash(component)
		concat = append(concat, h.Bytes()...)
	}
	header := &conway.ConwayBlockHeader{
		BabbageBlockHeader: babbage.BabbageBlockHeader{
			Body: babbage.BabbageBlockHeaderBody{
				BlockNumber:   blockNumber,
				Slot:          slot,
				BlockBodyHash: lcommon.Blake2b256Hash(concat),
				VrfKey:        make([]byte, 32),
				VrfResult: lcommon.VrfResult{
					Output: make([]byte, 32),
					Proof:  make([]byte, 80),
				},
				OpCert: babbage.BabbageOpCert{
					HotVkey:   make([]byte, 32),
					Signature: make([]byte, 64),
				},
				ProtoVersion: babbage.BabbageProtoVersion{Major: 10},
			},
			Signature: make([]byte, 448),
		},
	}
	headerRaw, err := cbor.Encode(header)
	require.NoError(t, err)
	// Definite-length array of five items: header plus the four components.
	raw := []byte{0x85}
	raw = append(raw, headerRaw...)
	for _, component := range components {
		raw = append(raw, component...)
	}
	return raw
}
