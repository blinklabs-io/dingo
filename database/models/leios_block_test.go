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

package models

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildStandardConwayBlock constructs a minimal, valid (10-field header) Conway
// block with empty transaction components and a correct block body hash.
func buildStandardConwayBlock(t *testing.T) []byte {
	t.Helper()
	return testutil.BuildDecodableConwayBlockBytes(t, 42, 7)
}

// extendConwayHeaderWithLeios rewrites a standard Conway block so its header
// body carries the two trailing Musashi/Leios fields (leios_certified = false,
// leios_announcement = nil), producing a 12-field header body. The transaction
// components (and therefore the block body hash) are left untouched.
func extendConwayHeaderWithLeios(t *testing.T, standardRaw []byte) []byte {
	t.Helper()
	return extendConwayHeaderBody(
		t,
		standardRaw,
		cbor.RawMessage{0xf4},
		cbor.RawMessage{0xf6},
	)
}

func extendConwayHeaderBody(
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
	var bodyElems []cbor.RawMessage
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

func TestDecodeConwayBlockStandard(t *testing.T) {
	standardRaw := buildStandardConwayBlock(t)
	// Sanity: the fixture is a valid standard Conway block.
	_, err := conway.NewConwayBlockFromCbor(standardRaw)
	require.NoError(t, err)

	block, err := DecodeConwayBlock(standardRaw)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), block.SlotNumber())
	assert.Equal(t, uint64(7), block.BlockNumber())
	assert.Equal(t, 7, block.Type())
	assert.True(
		t,
		bytes.Equal(standardRaw, block.Cbor()),
		"stored CBOR must be verbatim",
	)
}

func TestDecodeConwayBlockLeiosExtendedHeader(t *testing.T) {
	standardRaw := buildStandardConwayBlock(t)
	extendedRaw := extendConwayHeaderWithLeios(t, standardRaw)

	// gouroboros' strict Conway decoder must reject the 12-field header.
	_, err := gledger.NewBlockFromCbor(gledger.BlockTypeConway, extendedRaw)
	require.Error(
		t,
		err,
		"strict Conway decode must reject the extended header",
	)

	// dingo's Leios-aware decoder must accept it.
	block, err := DecodeConwayBlock(extendedRaw)
	require.NoError(t, err)

	// The block hash must equal the real 12-field header hash, i.e. what the
	// chain-sync header path (gdijkstra header decode) computes for the same
	// block. Otherwise prev-hash chain linkage between the header and body
	// paths would break.
	comps := make([]cbor.RawMessage, 0)
	_, err = cbor.Decode(extendedRaw, &comps)
	require.NoError(t, err)
	require.NotEmpty(t, comps)
	hdr, err := gdijkstra.NewDijkstraBlockHeaderFromCbor(comps[0])
	require.NoError(t, err)
	assert.Equal(
		t,
		hdr.Hash(),
		block.Hash(),
		"block hash must match extended 12-field header hash",
	)

	assert.Equal(t, uint64(42), block.SlotNumber())
	assert.Equal(t, uint64(7), block.BlockNumber())
	assert.Equal(t, 7, block.Type())
	assert.True(
		t,
		bytes.Equal(extendedRaw, block.Cbor()),
		"stored CBOR must be the verbatim 12-field-header block",
	)

	// Re-decoding the stored CBOR (the models.Block.Decode path) must yield the
	// same hash, since blocks are re-decoded from storage on the application
	// and serving paths.
	reBlock, err := DecodeConwayBlock(block.Cbor())
	require.NoError(t, err)
	assert.Equal(t, block.Hash(), reBlock.Hash())

	// The Block.Decode() router (keyed on block Type) must take the same path.
	stored := Block{Type: 7, Cbor: extendedRaw}
	routed, err := stored.Decode()
	require.NoError(t, err)
	assert.Equal(t, block.Hash(), routed.Hash())
}

func TestDecodeConwayBlockRejectsMalformedLeiosHeaderExtension(t *testing.T) {
	standardRaw := buildStandardConwayBlock(t)
	for _, tc := range []struct {
		name        string
		extraFields []cbor.RawMessage
	}{
		{
			name:        "one extra field",
			extraFields: []cbor.RawMessage{{0xf4}},
		},
		{
			name: "three extra fields",
			extraFields: []cbor.RawMessage{
				{0xf4},
				{0xf6},
				{0xf6},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := extendConwayHeaderBody(t, standardRaw, tc.extraFields...)
			_, err := DecodeConwayBlock(raw)
			require.Error(t, err)
		})
	}
}
