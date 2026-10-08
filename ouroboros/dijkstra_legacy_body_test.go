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

package ouroboros

import (
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	gcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

// legacyMusashiBlockFixture is a block from before the block-body respin. Its
// body has four components and its header commits to that body, so only the
// layout distinguishes it from a current block. Archival decoding keeps it
// readable; a peer must not be able to deliver it.
const legacyMusashiBlockFixture = "../database/models/testdata/musashi_dijkstra_block.hex"

func legacyMusashiBlock(t *testing.T) (block, header []byte) {
	t.Helper()
	block = readHexFixture(t, legacyMusashiBlockFixture)
	var parts []cbor.RawMessage
	_, err := cbor.Decode(block, &parts)
	require.NoError(t, err)
	require.Len(t, parts, 2)
	var body []cbor.RawMessage
	_, err = cbor.Decode(parts[1], &body)
	require.NoError(t, err)
	require.Len(t, body, 4, "the fixture must carry the legacy four-field body")
	decoded, err := dijkstra.NewDijkstraBlockHeaderFromCbor(parts[0])
	require.NoError(t, err)
	require.Equal(
		t,
		decoded.BlockBodyHash(),
		gcommon.Blake2b256Hash(parts[1]),
		"the header must commit to the four-field body",
	)
	return block, parts[0]
}

func TestDecodeBlockfetchBlockRejectsLegacyDijkstraBodyWithMatchingHash(
	t *testing.T,
) {
	t.Parallel()
	raw, _ := legacyMusashiBlock(t)
	for _, magic := range []uint32{musashiNetworkMagic, mainnetMagic} {
		o := newOuroboros(OuroborosConfig{
			Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
			NetworkMagic: magic,
		})
		block, err := o.decodeBlockfetchBlock(gledger.BlockTypeDijkstra, raw)
		require.ErrorContains(t, err, "expected 3 components, got 4")
		require.Nil(t, block)
	}
}

// The rejection holds through the real block-fetch client, which is how a
// peer delivers a block, and the rejected block is never published.
func TestBlockfetchClientRejectsLegacyDijkstraBody(t *testing.T) {
	t.Parallel()
	raw, header := legacyMusashiBlock(t)
	block, _, _, err := runMusashiBlockfetchClientDeliveryRaw(
		t,
		gledger.BlockTypeDijkstra,
		raw,
		header,
	)
	require.ErrorContains(t, err, "expected 3 components, got 4")
	require.Nil(t, block)
}
