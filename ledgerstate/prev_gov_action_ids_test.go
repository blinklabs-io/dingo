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

package ledgerstate

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// encodeRoots encodes a 4-element GovRelation array of StrictMaybe
// (GovPurposeId p era), in the order [PParamUpdate, HardFork,
// Committee, Constitution]. ids[i] of nil encodes as SNothing ([]).
func encodeRoots(t *testing.T, ids [4]*ParsedGovActionId) []byte {
	t.Helper()
	encoded := make([]any, 4)
	for i, id := range ids {
		if id == nil {
			encoded[i] = []any{}
			continue
		}
		encoded[i] = []any{
			[]any{id.TxHash, uint64(id.ActionIndex)},
		}
	}
	data, err := cbor.Encode(encoded)
	require.NoError(t, err)
	return data
}

func TestParseProposalsRootsEmpty(t *testing.T) {
	got, err := parseProposalsRoots(nil)
	require.NoError(t, err)
	require.Nil(t, got)

	emptyArr, err := cbor.Encode([]any{})
	require.NoError(t, err)
	got, err = parseProposalsRoots(emptyArr)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestParseProposalsRootsAllSNothing(t *testing.T) {
	data := encodeRoots(t, [4]*ParsedGovActionId{nil, nil, nil, nil})
	got, err := parseProposalsRoots(data)
	require.NoError(t, err)
	require.Nil(t, got, "all SNothing should map to nil result")
}

func TestParseProposalsRootsAllSet(t *testing.T) {
	pp := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0x11}, 32),
		ActionIndex: 0,
	}
	hf := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0x22}, 32),
		ActionIndex: 1,
	}
	cm := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0x33}, 32),
		ActionIndex: 2,
	}
	cn := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0x44}, 32),
		ActionIndex: 3,
	}

	data := encodeRoots(t, [4]*ParsedGovActionId{pp, hf, cm, cn})
	got, err := parseProposalsRoots(data)
	require.NoError(t, err)
	require.NotNil(t, got)

	require.NotNil(t, got.PParamUpdate)
	assert.Equal(t, pp.TxHash, got.PParamUpdate.TxHash)
	assert.Equal(t, pp.ActionIndex, got.PParamUpdate.ActionIndex)

	require.NotNil(t, got.HardFork)
	assert.Equal(t, hf.TxHash, got.HardFork.TxHash)
	assert.Equal(t, hf.ActionIndex, got.HardFork.ActionIndex)

	require.NotNil(t, got.Committee)
	assert.Equal(t, cm.TxHash, got.Committee.TxHash)
	assert.Equal(t, cm.ActionIndex, got.Committee.ActionIndex)

	require.NotNil(t, got.Constitution)
	assert.Equal(t, cn.TxHash, got.Constitution.TxHash)
	assert.Equal(t, cn.ActionIndex, got.Constitution.ActionIndex)
}

func TestParseProposalsRootsPartial(t *testing.T) {
	hf := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0xAA}, 32),
		ActionIndex: 7,
	}
	data := encodeRoots(t, [4]*ParsedGovActionId{nil, hf, nil, nil})
	got, err := parseProposalsRoots(data)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Nil(t, got.PParamUpdate)
	require.NotNil(t, got.HardFork)
	assert.Equal(t, hf.TxHash, got.HardFork.TxHash)
	assert.Equal(t, hf.ActionIndex, got.HardFork.ActionIndex)
	assert.Nil(t, got.Committee)
	assert.Nil(t, got.Constitution)
}

func TestParseProposalsRootsBadShape(t *testing.T) {
	// 3-element array is not a valid GovRelation.
	data, err := cbor.Encode([]any{[]any{}, []any{}, []any{}})
	require.NoError(t, err)
	got, err := parseProposalsRoots(data)
	require.Error(t, err)
	require.Nil(t, got)
	require.Contains(t, err.Error(), "GovRelation has 3 elements")
}

func TestParseStrictMaybeGovActionIdNullSentinel(t *testing.T) {
	// CBOR null (0xf6) is treated as SNothing.
	got, err := parseStrictMaybeGovActionId([]byte{0xf6})
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestParseStrictMaybeGovActionIdDirectGovActionId(t *testing.T) {
	// Tolerate a non-canonical encoder that emits the GovActionId
	// directly without the SJust 1-element wrapper.
	txHash := bytes.Repeat([]byte{0x55}, 32)
	data, err := cbor.Encode([]any{txHash, uint64(9)})
	require.NoError(t, err)
	got, err := parseStrictMaybeGovActionId(data)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, txHash, got.TxHash)
	assert.Equal(t, uint32(9), got.ActionIndex)
}

func TestParseStrictMaybeGovActionIdShortTxHash(t *testing.T) {
	short := bytes.Repeat([]byte{0x77}, 16)
	data, err := cbor.Encode([]any{[]any{short, uint64(0)}})
	require.NoError(t, err)
	got, err := parseStrictMaybeGovActionId(data)
	require.Error(t, err)
	require.Nil(t, got)
	require.Contains(t, err.Error(), "txHash has 16 bytes")
}

func TestParseProposalsIncludesRoots(t *testing.T) {
	// End-to-end: parseProposals returns both the OMap proposals
	// and the per-purpose roots.
	pp := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0xBC}, 32),
		ActionIndex: 4,
	}
	roots := encodeRootsAsAny(t, [4]*ParsedGovActionId{pp, nil, nil, nil})

	// One Info proposal so the OMap is non-empty.
	infoProp := []any{
		[]any{bytes.Repeat([]byte{0x01}, 32), uint64(0)},
		map[uint64]uint64{},
		map[uint64]uint64{},
		map[uint64]uint64{},
		[]any{
			uint64(0),
			bytes.Repeat([]byte{0x02}, 29),
			[]any{uint8(6)}, // GovActionType Info = 6
			[]any{
				"https://example.com/info",
				bytes.Repeat([]byte{0x03}, 32),
			},
		},
		uint64(10),
		uint64(20),
	}
	container, err := cbor.Encode([]any{roots, []any{infoProp}})
	require.NoError(t, err)

	props, prevIds, err := parseProposals(container)
	require.NoError(t, err)
	require.Len(t, props, 1)
	require.NotNil(t, prevIds)
	require.NotNil(t, prevIds.PParamUpdate)
	assert.Equal(t, pp.TxHash, prevIds.PParamUpdate.TxHash)
	assert.Equal(t, pp.ActionIndex, prevIds.PParamUpdate.ActionIndex)
}

// encodeRootsAsAny returns the GovRelation as []any so it can be
// embedded in a larger container before CBOR-encoding.
func encodeRootsAsAny(t *testing.T, ids [4]*ParsedGovActionId) []any {
	t.Helper()
	out := make([]any, 4)
	for i, id := range ids {
		if id == nil {
			out[i] = []any{}
			continue
		}
		out[i] = []any{
			[]any{id.TxHash, uint64(id.ActionIndex)},
		}
	}
	return out
}
