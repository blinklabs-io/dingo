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

package chain_test

import (
	"os"
	"path/filepath"
	"testing"

	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/chain"
)

const eraRegressionRule = "precedes the era of its parent"

func newEraOrderTestChain(t *testing.T) *chain.Chain {
	t.Helper()
	cm, err := chain.NewManager(newTestDB(t), nil)
	require.NoError(t, err)
	mustSetLedger(t, cm, 10)
	return cm.PrimaryChain()
}

func craftedByronEbbHeader(
	prev common.Blake2b256,
	blockNumber uint64,
) *byron.ByronEpochBoundaryBlockHeader {
	header := &byron.ByronEpochBoundaryBlockHeader{PrevBlock: prev}
	header.ConsensusData.Difficulty.Value = blockNumber
	return header
}

func craftedByronMainHeader(
	prev common.Blake2b256,
	blockNumber uint64,
) *byron.ByronMainBlockHeader {
	header := &byron.ByronMainBlockHeader{PrevBlock: prev}
	header.ConsensusData.Difficulty.Value = blockNumber
	return header
}

// TestAddBlockHeaderRejectsEraRegression covers headers from an earlier era
// than the block or header they extend. The hard-fork combinator only moves a
// chain's ledger state forward, so the reference rejects such a header as
// HardForkEnvelopeErrWrongEra; a Byron epoch-boundary header carries no
// signature and the Byron block-number rule accepts its parent's number, so
// nothing else here refuses it.
func TestAddBlockHeaderRejectsEraRegression(t *testing.T) {
	t.Parallel()

	conwayTip := testBlocks[0]
	conwayHeader := testBlocks[1]
	cases := []struct {
		name         string
		queueConway  bool
		byronHeader  func() gledger.BlockHeader
		parentNumber uint64
	}{
		{
			name: "Byron EBB after a Conway tip block",
			byronHeader: func() gledger.BlockHeader {
				return craftedByronEbbHeader(
					conwayTip.Hash(),
					conwayTip.BlockNumber(),
				)
			},
		},
		{
			name: "Byron main header after a Conway tip block",
			byronHeader: func() gledger.BlockHeader {
				return craftedByronMainHeader(
					conwayTip.Hash(),
					conwayTip.BlockNumber()+1,
				)
			},
		},
		{
			name:        "Byron EBB after a queued Conway header",
			queueConway: true,
			byronHeader: func() gledger.BlockHeader {
				return craftedByronEbbHeader(
					conwayHeader.Hash(),
					conwayHeader.BlockNumber(),
				)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := newEraOrderTestChain(t)
			require.NoError(t, c.AddBlock(conwayTip, nil))
			if tc.queueConway {
				require.NoError(t, c.AddBlockHeader(conwayHeader))
			}
			err := c.AddBlockHeader(tc.byronHeader())
			require.ErrorContains(t, err, eraRegressionRule)
		})
	}
}

// TestAddBlockHeaderAcceptsByronShelleyBoundary is the honest control: the
// first Shelley header of each network with a Byron prefix still extends the
// last Byron block.
func TestAddBlockHeaderAcceptsByronShelleyBoundary(t *testing.T) {
	t.Parallel()

	fixtures := []struct {
		name, byronFile, shelleyFile string
	}{
		{
			"preprod",
			"preprod-byron-last-84242.cbor",
			"preprod-shelley-first-86400.cbor",
		},
		{
			"mainnet",
			"mainnet-byron-last-4492799.cbor",
			"mainnet-shelley-first-4492800.cbor",
		},
	}
	load := func(t *testing.T, file string, blockType uint) gledger.Block {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join("..", "ledger", "testdata", file))
		require.NoError(t, err)
		block, err := gledger.NewBlockFromCbor(blockType, raw)
		require.NoError(t, err)
		return block
	}
	for _, fx := range fixtures {
		t.Run(fx.name, func(t *testing.T) {
			t.Parallel()
			lastByron := load(t, fx.byronFile, gledger.BlockTypeByronMain)
			firstShelley := load(t, fx.shelleyFile, gledger.BlockTypeShelley)
			c := newEraOrderTestChain(t)
			require.NoError(t, c.AddBlock(lastByron, nil))
			require.NoError(t, c.AddBlockHeader(firstShelley.Header()))
		})
	}
}
