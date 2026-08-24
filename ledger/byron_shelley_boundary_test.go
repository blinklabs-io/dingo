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
	"os"
	"path/filepath"
	"testing"

	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// boundaryFixture is the last Byron block and the first Shelley block of a
// network's fork epoch, taken from the chain itself over NtN blockfetch.
type boundaryFixture struct {
	name             string
	byronFile        string
	byronType        uint
	byronSlot        uint64
	shelleyFile      string
	shelleyType      uint
	shelleySlot      uint64
	maxBlockBodySize uint
	maxHeaderSize    uint
}

// The two shipped networks that have a Byron prefix. Both fixtures were
// fetched with blockfetch over the range spanning the boundary, which delivers
// Byron epoch boundary blocks as well -- verified against mainnet's Byron
// epoch 0/1 boundary at slot 21600, where the range does contain an EBB
// (block number 21586, shared with its parent, followed by the regular block
// 21587 at the same slot). Neither fork boundary below contains one.
var boundaryFixtures = []boundaryFixture{
	{
		name:        "preprod",
		byronFile:   "preprod-byron-last-84242.cbor",
		byronType:   1,
		byronSlot:   84_242,
		shelleyFile: "preprod-shelley-first-86400.cbor",
		shelleyType: 2,
		shelleySlot: 86_400,
		// preprod Shelley genesis protocolParams.
		maxBlockBodySize: 65_536,
		maxHeaderSize:    1_100,
	},
	{
		name:        "mainnet",
		byronFile:   "mainnet-byron-last-4492799.cbor",
		byronType:   1,
		byronSlot:   4_492_799,
		shelleyFile: "mainnet-shelley-first-4492800.cbor",
		shelleyType: 2,
		shelleySlot: 4_492_800,
		// mainnet Shelley genesis protocolParams.
		maxBlockBodySize: 65_536,
		maxHeaderSize:    1_100,
	},
}

func loadBoundaryBlock(
	t *testing.T,
	file string,
	blockType uint,
) gledger.Block {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", file))
	require.NoError(t, err)
	block, err := gledger.NewBlockFromCbor(blockType, raw)
	require.NoError(t, err)
	return block
}

// TestByronShelleyBoundaryHasNoEpochBoundaryBlock pins the chain shape the
// era-transition path depends on: the first block of the fork epoch is a
// Shelley block, not a Byron EBB.
//
// ledgerProcessBlocksFromSource ends a batch at the first block whose slot
// reaches the epoch end and takes nextEpochEraId from that block's era. A
// Byron EBB carries the Byron era and its parent's block number, so an EBB
// leading the fork epoch would defer the era transition by one block and leave
// the first Shelley block to be validated under a Byron era with nil protocol
// parameters. Both shipped networks with a Byron prefix rule that out: the
// Shelley block links directly to the last Byron block.
func TestByronShelleyBoundaryHasNoEpochBoundaryBlock(t *testing.T) {
	for _, tc := range boundaryFixtures {
		t.Run(tc.name, func(t *testing.T) {
			last := loadBoundaryBlock(t, tc.byronFile, tc.byronType)
			first := loadBoundaryBlock(t, tc.shelleyFile, tc.shelleyType)

			require.Equal(t, tc.byronSlot, last.SlotNumber())
			require.Equal(t, tc.shelleySlot, first.SlotNumber())
			assert.EqualValues(t, byron.EraIdByron, last.Era().Id)
			assert.EqualValues(t, shelley.EraIdShelley, first.Era().Id)

			// Neither block is an EBB, and no EBB can sit between them: the
			// Shelley block names the Byron block as its parent and takes the
			// next block number. An EBB would share 'last' block number and
			// break that link.
			_, lastIsEbb := last.(*byron.ByronEpochBoundaryBlock)
			_, firstIsEbb := first.(*byron.ByronEpochBoundaryBlock)
			assert.False(t, lastIsEbb)
			assert.False(t, firstIsEbb)
			assert.Equal(
				t,
				last.Hash().String(),
				first.PrevHash().String(),
				"first Shelley block must link directly to the last Byron block",
			)
			assert.Equal(t, last.BlockNumber()+1, first.BlockNumber())
		})
	}
}

// TestByronShelleyBoundaryEnvelopeRequiresProtocolParameters pins that the
// first Shelley block is validated with protocol parameters, not exempted from
// validation for lacking them.
//
// validateInboundBlockEnvelope runs before validateBlockHeaderProtocolVersion
// in ledgerProcessBlock, and it reaches validateBlockSizes for any non-Byron
// block. So nil parameters at this point are a rejection, not a bypass: there
// is no ordering in which a "Byron era, nil pparams" allowance for the first
// Shelley block can take effect, and adding one to the envelope check would
// drop maxBlockHeaderSize and maxBlockBodySize for a real Shelley block.
//
// The era transition supplies those parameters. It runs at the epoch break
// ahead of this block precisely because the block above is Shelley, which
// TestByronShelleyBoundaryHasNoEpochBoundaryBlock pins.
func TestByronShelleyBoundaryEnvelopeRequiresProtocolParameters(t *testing.T) {
	for _, tc := range boundaryFixtures {
		t.Run(tc.name, func(t *testing.T) {
			last := loadBoundaryBlock(t, tc.byronFile, tc.byronType)
			first := loadBoundaryBlock(t, tc.shelleyFile, tc.shelleyType)
			parent := envelopeParentFromBlock(last)

			// The Byron parent itself needs no parameters: Byron returns
			// before the size checks.
			require.NoError(
				t,
				validateInboundBlockEnvelope(last, nil, envelopeParent{
					origin: true,
				}),
			)

			err := validateInboundBlockEnvelope(first, nil, parent)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"block size validation unsupported for protocol parameters",
			)

			pp := &shelley.ShelleyProtocolParameters{
				MaxBlockBodySize:   tc.maxBlockBodySize,
				MaxBlockHeaderSize: tc.maxHeaderSize,
			}
			assert.NoError(t, validateInboundBlockEnvelope(first, pp, parent))

			// The block's declared body size is what the size check measures,
			// so a limit one byte below it must reject. This keeps the
			// positive case above from passing on an unenforced limit.
			tooSmall := &shelley.ShelleyProtocolParameters{
				//nolint:gosec // fixture body size is well under uint range
				MaxBlockBodySize:   uint(first.BlockBodySize()) - 1,
				MaxBlockHeaderSize: tc.maxHeaderSize,
			}
			assert.ErrorContains(
				t,
				validateInboundBlockEnvelope(first, tooSmall, parent),
				"exceeds maxBlockBodySize",
			)
		})
	}
}

// TestByronBlockHeaderProtocolVersionSkippedWithoutPParams pins that a
// validated Byron block does not require protocol parameters.
//
// Byron headers carry no ProtVer field, so HeaderProtocolMajor reports no
// version for them and ValidateHeaderProtocolVersion already skips them. The
// LedgerState wrapper must reach that skip rather than failing earlier on
// GetProtocolVersion(nil): with ValidateHistorical enabled on a network that
// has a Byron prefix, every block of the prefix is validated while
// currentPParams is nil, so demanding parameters here rejects the entire
// Byron era.
//
// The Shelley half of the test keeps the wrapper fail-closed for headers that
// do carry a version, which is the case validateInboundBlockEnvelope also
// rejects.
func TestByronBlockHeaderProtocolVersionSkippedWithoutPParams(t *testing.T) {
	for _, tc := range boundaryFixtures {
		t.Run(tc.name, func(t *testing.T) {
			ls := newLedgerStateForNetwork(t, "Testnet", 42)

			byronHeader := loadBoundaryBlock(
				t, tc.byronFile, tc.byronType,
			).Header()
			_, hasVersion := HeaderProtocolMajor(byronHeader)
			require.False(
				t,
				hasVersion,
				"a Byron header carries no protocol major version",
			)
			assert.NoError(
				t,
				ls.validateBlockHeaderProtocolVersion(byronHeader, nil),
			)

			shelleyHeader := loadBoundaryBlock(
				t, tc.shelleyFile, tc.shelleyType,
			).Header()
			_, hasVersion = HeaderProtocolMajor(shelleyHeader)
			require.True(
				t,
				hasVersion,
				"a Shelley header carries a protocol major version",
			)
			assert.ErrorContains(
				t,
				ls.validateBlockHeaderProtocolVersion(shelleyHeader, nil),
				"protocol parameters are nil",
			)
		})
	}
}
