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
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

// musashiDijkstraBlock returns the Musashi block fixture, which is a verbatim
// copy of gouroboros v0.202.4's
// ledger/dijkstra/testdata/musashi_dijkstra_block.hex.
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

// TestDecodeConwayBlockRejectsLegacyDijkstraLayout ensures the obsolete
// four-component Dijkstra body cannot be accepted through Conway storage or
// replay decoding.
func TestDecodeConwayBlockRejectsLegacyDijkstraLayout(t *testing.T) {
	raw := musashiDijkstraBlock(t)

	block, err := models.DecodeConwayBlock(raw)
	require.Error(t, err)
	require.Nil(t, block)

	block, err = models.DecodeConwayPeerBlock(raw)
	require.Error(t, err)
	require.Nil(t, block)
}

func TestDecodeStoredConwayBlockAcceptsLegacyDijkstraLayout(t *testing.T) {
	raw := musashiDijkstraBlock(t)

	block, err := (models.Block{
		Type: ledger.BlockTypeConway,
		Cbor: raw,
	}).Decode()
	require.NoError(t, err)
	_, ok := block.(*dijkstra.DijkstraBlock)
	require.True(t, ok)
	require.Equal(t, raw, block.Cbor())
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

// TestDecodeConwayBlockRejectsUnrecognizedBlock ensures unrecognized input
// still reports the strict Conway decode error.
func TestDecodeConwayBlockRejectsUnrecognizedBlock(t *testing.T) {
	notABlock, err := cbor.Encode([]any{1, 2, 3})
	require.NoError(t, err)

	block, err := models.DecodeConwayBlock(notABlock)
	require.Error(t, err)
	require.Nil(t, block)
	require.Contains(t, err.Error(), "decode Conway block error")
}

// TestDecodeConwayBlockRejectsTwoComponentNonDijkstra ensures a two-component
// array is not accepted as a Conway block.
func TestDecodeConwayBlockRejectsTwoComponentNonDijkstra(t *testing.T) {
	twoThings, err := cbor.Encode([]any{1, 2})
	require.NoError(t, err)

	block, err := models.DecodeConwayBlock(twoThings)
	require.Error(t, err)
	require.Nil(t, block)
	require.Contains(
		t,
		err.Error(),
		"decode Conway block error",
		"a two-component array that is not a Leios-extended Dijkstra block "+
			"must still report the strict Conway error",
	)
}

// TestDecodeConwayBlockRejectsUnextendedDijkstraShape ensures a legacy
// two-component block is rejected even with a standard-width header.
func TestDecodeConwayBlockRejectsUnextendedDijkstraShape(t *testing.T) {
	raw := musashiDijkstraBlock(t)

	var components []cbor.RawMessage
	_, err := cbor.Decode(raw, &components)
	require.NoError(t, err)
	var headerParts []cbor.RawMessage
	require.Len(t, components, 2)
	_, err = cbor.Decode(components[0], &headerParts)
	require.NoError(t, err)
	var bodyElems []cbor.RawMessage
	require.Len(t, headerParts, 2)
	_, err = cbor.Decode(headerParts[0], &bodyElems)
	require.NoError(t, err)
	require.Len(t, bodyElems, 12)

	// Drop the two Leios extension fields, leaving a well-formed two-component
	// block that is no longer the Musashi shape.
	truncatedBody, err := cbor.Encode(bodyElems[:10])
	require.NoError(t, err)
	truncatedHeader, err := cbor.Encode(
		[]any{cbor.RawMessage(truncatedBody), headerParts[1]},
	)
	require.NoError(t, err)
	rebuilt, err := cbor.Encode(
		[]any{cbor.RawMessage(truncatedHeader), components[1]},
	)
	require.NoError(t, err)

	block, err := models.DecodeConwayBlock(rebuilt)
	require.Error(t, err)
	require.Nil(t, block)
}
