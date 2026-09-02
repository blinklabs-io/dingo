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

package models_test

import (
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

// musashiDijkstraBlock returns the Musashi block fixture, which is a verbatim
// copy of gouroboros v0.202.4's ledger/dijkstra/testdata/musashi_dijkstra_block.hex.
//
// It is a real block from the Musashi prototype network rather than a
// constructed one, and its shape matches what the live network serves: fetched
// from leios-node.play.dev.cardano.org:3001 (network magic 164) while
// diagnosing #3761, a tip block decoded to the same two top-level components
// and twelve-field header body this fixture carries.
func musashiDijkstraBlock(t *testing.T) []byte {
	t.Helper()
	encoded, err := os.ReadFile("testdata/musashi_dijkstra_block.hex")
	require.NoError(t, err)
	raw, err := hex.DecodeString(strings.TrimSpace(string(encoded)))
	require.NoError(t, err)
	return raw
}

// TestDecodeConwayBlockAcceptsDijkstraLayout is the regression for #3761: a
// from-genesis Musashi sync could not advance past origin because every
// BlockFetch block failed to decode.
//
// The respun chain carries the Dijkstra two-component block layout while still
// tagging blocks as Conway (NtN block type 7) on the wire, so neither the
// strict Conway decoder nor the five-component Leios-extended reconstruct
// recognized them, and the strict Conway error surfaced:
//
//	cbor: cannot unmarshal array into Go value of type conway.tmpConwayBlock
//	(cannot decode CBOR array to struct with different number of elements)
func TestDecodeConwayBlockAcceptsDijkstraLayout(t *testing.T) {
	raw := musashiDijkstraBlock(t)

	block, err := models.DecodeConwayBlock(raw)
	require.NoError(
		t,
		err,
		"a Musashi block in the Dijkstra layout must decode; without this a "+
			"from-genesis sync never advances past origin",
	)
	require.NotNil(t, block)
	require.Equal(t, uint64(566037), block.SlotNumber())
}

// TestMusashiFixtureHasDijkstraLayout pins the shape the fix depends on, so a
// fixture swapped for a differently-shaped block fails here with a clear
// reason rather than making the regression above pass for the wrong one.
func TestMusashiFixtureHasDijkstraLayout(t *testing.T) {
	raw := musashiDijkstraBlock(t)

	var components []cbor.RawMessage
	_, err := cbor.Decode(raw, &components)
	require.NoError(t, err)
	require.Len(
		t,
		components,
		2,
		"Dijkstra blocks are [header, block_body]; the five-component "+
			"Leios-extended Conway reconstruct cannot apply to them",
	)

	var headerParts []cbor.RawMessage
	_, err = cbor.Decode(components[0], &headerParts)
	require.NoError(t, err)
	require.Len(t, headerParts, 2)

	var bodyElems []cbor.RawMessage
	_, err = cbor.Decode(headerParts[0], &bodyElems)
	require.NoError(t, err)
	require.Len(
		t,
		bodyElems,
		12,
		"the Leios header extension adds leios_certified and "+
			"leios_announcement to the 10 standard Babbage fields",
	)
}

// TestDecodeConwayBlockRejectsUnrecognizedBlock proves the added fallback did
// not turn the decoder into one that accepts anything: input matching none of
// the three shapes still fails, and still reports the strict Conway error,
// which is the meaningful one for real Conway networks.
func TestDecodeConwayBlockRejectsUnrecognizedBlock(t *testing.T) {
	notABlock, err := cbor.Encode([]any{1, 2, 3})
	require.NoError(t, err)

	block, err := models.DecodeConwayBlock(notABlock)
	require.Error(t, err)
	require.Nil(t, block)
	require.Contains(t, err.Error(), "decode Conway block error")
}
