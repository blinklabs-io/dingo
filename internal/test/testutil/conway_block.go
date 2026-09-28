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

// BuildDecodableConwayBlockBytes constructs a minimal, valid (10-field
// header), individually decodable Conway block with a correct block body
// hash and empty transaction components, for the given slot and block
// number. It is shared by tests across packages (database/models, ledger)
// that need a cheap, uniquely-identifiable, real Conway block rather than a
// hand-rolled mock.
func BuildDecodableConwayBlockBytes(
	t *testing.T,
	slot, blockNumber uint64,
) []byte {
	t.Helper()
	block := &conway.ConwayBlock{
		BlockHeader: &conway.ConwayBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: blockNumber,
					Slot:        slot,
					VrfKey:      make([]byte, 32),
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
		},
	}
	// First pass: encode to obtain the exact body-component bytes so we can
	// compute a correct block body hash (blake2b256 over the concatenated
	// component hashes, per the Cardano spec / ValidateBlockBodyHash).
	tmp, err := cbor.Encode(block)
	require.NoError(t, err)
	var comps []cbor.RawMessage
	_, err = cbor.Decode(tmp, &comps)
	require.NoError(t, err)
	require.Len(t, comps, 5)
	var concat []byte
	for i := 1; i < 5; i++ {
		h := lcommon.Blake2b256Hash(comps[i])
		concat = append(concat, h.Bytes()...)
	}
	block.BlockHeader.Body.BlockBodyHash = lcommon.Blake2b256Hash(concat)
	raw, err := cbor.Encode(block)
	require.NoError(t, err)
	return raw
}

// ExtendConwayHeaderWithLeios rewrites a standard (10-field header) Conway
// block so its header body carries the two trailing Musashi/Leios fields
// (leios_certified = false, leios_announcement = nil), producing a 12-field
// header body. The transaction components (and therefore the block body
// hash) are left untouched.
func ExtendConwayHeaderWithLeios(t *testing.T, standardRaw []byte) []byte {
	t.Helper()
	return ExtendConwayHeaderBody(
		t,
		standardRaw,
		cbor.RawMessage{0xf4},
		cbor.RawMessage{0xf6},
	)
}

// ExtendConwayHeaderBody rewrites a standard (10-field header) Conway block
// so its header body carries extraFields appended after the 10 standard
// Babbage fields, for constructing malformed-extension fixtures (a field
// count other than the real 12-field Musashi/Leios extension) as well as
// the genuine extension itself.
func ExtendConwayHeaderBody(
	t *testing.T,
	standardRaw []byte,
	extraFields ...cbor.RawMessage,
) []byte {
	t.Helper()
	var comps []cbor.RawMessage
	_, err := cbor.Decode(standardRaw, &comps)
	require.NoError(t, err)
	require.Len(t, comps, 5)
	var headerParts []cbor.RawMessage
	_, err = cbor.Decode(comps[0], &headerParts)
	require.NoError(t, err)
	require.Len(t, headerParts, 2)
	var bodyElems []cbor.RawMessage //nolint:prealloc
	_, err = cbor.Decode(headerParts[0], &bodyElems)
	require.NoError(t, err)
	require.Len(t, bodyElems, 10)
	extendedElems := append(bodyElems, extraFields...)
	extendedBody, err := cbor.Encode(extendedElems)
	require.NoError(t, err)
	extendedHeader, err := cbor.Encode([]any{
		cbor.RawMessage(extendedBody),
		headerParts[1],
	})
	require.NoError(t, err)
	extendedRaw, err := cbor.Encode([]any{
		cbor.RawMessage(extendedHeader),
		comps[1],
		comps[2],
		comps[3],
		comps[4],
	})
	require.NoError(t, err)
	return extendedRaw
}
