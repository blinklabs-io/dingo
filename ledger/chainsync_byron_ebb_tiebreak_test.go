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

package ledger

import (
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

// loadRealByronEBB returns a genuine Byron epoch-boundary block from the
// shared ouroboros-consensus golden fixtures, mirroring loadRealByronMainBlock
// in block_pipeline_validate_test.go.
func loadRealByronEBB(t *testing.T) models.Block {
	t.Helper()
	root, err := fixtures.ExtractEmbeddedFixtures(t.TempDir())
	require.NoError(t, err)
	fixture, err := fixtures.NewFixture(
		root,
		root+"/ouroboros-consensus/ouroboros-consensus-cardano/golden/"+
			"cardano/CardanoNodeToNodeVersion2/Block_Byron_EBB",
	)
	require.NoError(t, err)
	raw, err := fixture.ConsensusLedgerBlockBytes()
	require.NoError(t, err)
	blockType, err := fixture.LedgerBlockType()
	require.NoError(t, err)
	require.Equal(t, uint(gledger.BlockTypeByronEbb), blockType)
	decoded, err := gledger.NewBlockFromCbor(blockType, raw)
	require.NoError(t, err)
	return models.Block{
		Slot:   decoded.SlotNumber(),
		Hash:   decoded.Hash().Bytes(),
		Number: decoded.BlockNumber(),
		Type:   blockType,
		Cbor:   raw,
	}
}

// TestCompareIncomingHeaderToLocalTip_ByronEBBBeatsRegularTip exercises the
// real chain-selection caller (ledger/chainsync.go's
// compareIncomingHeaderToLocalTip) end to end for the exact scenario
// blinklabs-io/dingo#4413 describes: a locally applied Byron regular tip
// against a peer's EBB successor sharing its block number. Canonical Byron
// PBFT counts the boundary block as an additional block despite the shared
// number, so the incoming EBB must beat the local regular tip.
//
// The local tip is a real Byron main block round-tripped through storage
// (database.BlockByHash -> models.Block.Decode, same as
// TestCompareIncomingHeaderToLocalTip_Dijkstra), and the incoming header is a
// real Byron EBB header as chainsync delivers it. Both come from the shared
// ouroboros-consensus golden fixtures, not hand-built CBOR. Only the block
// number the comparator sees is adjusted (a tip's BlockNumber is supplied by
// the caller and by the header's own field, independent of the fixture's
// original block number) so the two tips fall at an equal height, which is
// the only condition this tiebreak needs.
func TestCompareIncomingHeaderToLocalTip_ByronEBBBeatsRegularTip(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	const sharedBlockNumber = 12345

	localBlock := loadRealByronMainBlock(t)
	require.NoError(t, db.BlockCreate(localBlock, nil))
	localTip := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: localBlock.Slot,
			Hash: localBlock.Hash,
		},
		BlockNumber: sharedBlockNumber,
	}

	ebbBlock := loadRealByronEBB(t)
	decodedEBB, err := ebbBlock.Decode()
	require.NoError(t, err)
	ebbHeader, ok := decodedEBB.Header().(*byron.ByronEpochBoundaryBlockHeader)
	require.True(t, ok)
	// The fixture's own block number is irrelevant to the tiebreak; only
	// equal height against the local tip matters here.
	ebbHeader.ConsensusData.Difficulty.Value = sharedBlockNumber

	event := ChainsyncEvent{
		BlockHeader: ebbHeader,
		Point: ocommon.Point{
			Slot: ebbHeader.SlotNumber(),
			Hash: []byte("incoming-ebb-hash"),
		},
	}

	result := ls.compareIncomingHeaderToLocalTip(event, localTip)
	require.Equal(
		t,
		praos.ChainABetter,
		result,
		"a peer's Byron EBB successor must beat the local regular tip at the same block number",
	)
}
