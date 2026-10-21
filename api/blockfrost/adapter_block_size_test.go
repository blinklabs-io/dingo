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

package blockfrost

import (
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

// goldenBlock loads one ouroboros-consensus golden block as stored by the
// metadata database.
func goldenBlock(t *testing.T, root string, name string) gledger.Block {
	t.Helper()
	fixture, err := fixtures.NewFixture(
		root,
		root+"/ouroboros-consensus/ouroboros-consensus-cardano/golden/"+
			"cardano/CardanoNodeToNodeVersion2/"+name,
	)
	require.NoError(t, err)
	raw, err := fixture.ConsensusLedgerBlockBytes()
	require.NoError(t, err)
	blockType, err := fixture.LedgerBlockType()
	require.NoError(t, err)
	block, err := gledger.NewBlockFromCbor(blockType, raw)
	require.NoError(t, err)
	return block
}

// TestBlockInfoSizeMatchesDbSync covers the size reported by /blocks/{id}
// and /blocks/latest. cardano-db-sync stores the header's block_body_size
// for Shelley-based eras and the whole encoded block for Byron, and the
// Blockfrost backend serves that column.
func TestBlockInfoSizeMatchesDbSync(t *testing.T) {
	t.Parallel()
	root, err := fixtures.ExtractEmbeddedFixtures(t.TempDir())
	require.NoError(t, err)
	nodeConfig, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		cardano.EmbeddedConfigPath("preview"),
	)
	require.NoError(t, err)

	dijkstraBlock, err := fixtures.NewDijkstraBlockBuilder().
		WithBlockNumber(7).
		WithSlot(70).
		Build()
	require.NoError(t, err)

	cases := []struct {
		name  string
		block gledger.Block
		byron bool
	}{
		{"byron ebb", goldenBlock(t, root, "Block_Byron_EBB"), true},
		{"byron main", goldenBlock(t, root, "Block_Byron_regular"), true},
		{"shelley", goldenBlock(t, root, "Block_Shelley"), false},
		{"allegra", goldenBlock(t, root, "Block_Allegra"), false},
		{"mary", goldenBlock(t, root, "Block_Mary"), false},
		{"alonzo", goldenBlock(t, root, "Block_Alonzo"), false},
		{"babbage", goldenBlock(t, root, "Block_Babbage"), false},
		{"conway", goldenBlock(t, root, "Block_Conway"), false},
		{"dijkstra", dijkstraBlock, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			block := tc.block
			want := uint64(len(block.Cbor()))
			if !tc.byron {
				want = block.Header().BlockBodySize()
				require.NotEqual(
					t,
					uint64(len(block.Cbor())),
					want,
					"fixture must tell the body size from the block length",
				)
			}

			adapter, _, db := newDBBackedAdapter(t, nodeConfig)
			require.NoError(t, db.BlockCreate(models.Block{
				Hash:     block.Hash().Bytes(),
				PrevHash: block.PrevHash().Bytes(),
				Slot:     block.SlotNumber(),
				Number:   block.BlockNumber(),
				ID:       block.BlockNumber() + database.BlockInitialIndex,
				Type:     uint(block.Type()),
				Cbor:     block.Cbor(),
			}, nil))
			require.NoError(t, db.SetEpoch(
				0, 0, nil, nil, nil, nil, 6, 1000,
				uint(block.SlotNumber()+1), nil,
			))
			require.NoError(t, db.SetTip(ochainsync.Tip{
				Point: ocommon.NewPoint(
					block.SlotNumber(), block.Hash().Bytes(),
				),
				BlockNumber: block.BlockNumber(),
			}, nil))
			require.NoError(
				t,
				adapter.ledgerState.PrepareEpochCacheForStartup(t.Context()),
			)

			info, err := adapter.BlockByHashOrNumber(block.Hash().String())
			require.NoError(t, err)
			require.Equal(t, want, info.Size)

			latest, err := adapter.LatestBlock()
			require.NoError(t, err)
			require.Equal(t, block.Hash().String(), latest.Hash)
			require.Equal(t, want, latest.Size)
		})
	}
}
