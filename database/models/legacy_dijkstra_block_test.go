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

func TestDecodeDijkstraBlockPreservesLegacyMusashiBody(t *testing.T) {
	rawText, err := os.ReadFile("testdata/musashi_dijkstra_block.hex")
	require.NoError(t, err)
	raw, err := hex.DecodeString(strings.TrimSpace(string(rawText)))
	require.NoError(t, err)

	_, err = dijkstra.NewDijkstraBlockFromCbor(raw)
	require.ErrorContains(t, err, "expected 3 components, got 4")

	block, err := models.DecodeDijkstraBlock(raw)
	require.NoError(t, err)
	dijkstraBlock, ok := block.(*dijkstra.DijkstraBlock)
	require.True(t, ok)
	require.Equal(t, raw, block.Cbor())
	encoded, err := dijkstraBlock.MarshalCBOR()
	require.NoError(t, err)
	require.Equal(t, raw, encoded)

	var components []cbor.RawMessage
	_, err = cbor.Decode(raw, &components)
	require.NoError(t, err)
	var legacyBody []cbor.RawMessage
	_, err = cbor.Decode(components[1], &legacyBody)
	require.NoError(t, err)
	require.Len(t, legacyBody, 4)
	require.Equal(t, []byte(components[1]), dijkstraBlock.BlockBody.Cbor())
	require.Equal(
		t,
		dijkstraBlock.BlockHeader.BlockBodyHash(),
		dijkstraBlock.CalculatedBlockBodyHash(),
	)

	var invalidSet cbor.SetType[uint64]
	if len(legacyBody[0]) != 1 || legacyBody[0][0] != 0xf6 {
		_, err = cbor.Decode(legacyBody[0], &invalidSet)
		require.NoError(t, err)
	}
	invalidIndexes := make(map[uint64]struct{}, len(invalidSet.Items()))
	for _, index := range invalidSet.Items() {
		invalidIndexes[index] = struct{}{}
	}
	for index, tx := range block.Transactions() {
		_, wasInvalid := invalidIndexes[uint64(index)]
		require.Equal(t, !wasInvalid, tx.IsValid(), "transaction %d", index)
	}

	stored, err := (models.Block{
		Type: ledger.BlockTypeDijkstra,
		Cbor: raw,
	}).Decode()
	require.NoError(t, err)
	require.Equal(t, block.Hash(), stored.Hash())
}
