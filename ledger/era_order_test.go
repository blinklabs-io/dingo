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
	"testing"

	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

const eraRegressionRule = "precedes the era of its parent"

// headerEntryPoints are the four ways a peer header reaches ledger header
// validation: the chainsync header queue, blockfetch before apply, chain
// selection, and Leios announcements.
var headerEntryPoints = []struct {
	name   string
	verify func(*LedgerState, gledger.Block) error
}{
	{"chainsync header", func(ls *LedgerState, b gledger.Block) error {
		return ls.verifyBlockHeaderOnlyCrypto(b.Header())
	}},
	{"fetched block", func(ls *LedgerState, b gledger.Block) error {
		return ls.verifyBlockHeaderCryptoBeforeApply(b)
	}},
	{"chain selection header", func(ls *LedgerState, b gledger.Block) error {
		return ls.ValidateChainSelectionHeaderCrypto(b.Header())
	}},
	{"announced header", func(ls *LedgerState, b gledger.Block) error {
		return ls.ValidateBlockHeaderCrypto(b.Header())
	}},
}

// newEraOrderTestLedger builds a mainnet-configured LedgerState whose primary
// chain tip is tip.
func newEraOrderTestLedger(t *testing.T, tip gledger.Block) *LedgerState {
	t.Helper()
	ls, primaryChain := newByronGenesisAnchorTestLedger(
		t,
		lcommon.Blake2b256Hash([]byte("mainnet byron genesis")).String(),
	)
	require.NoError(t, primaryChain.AddBlock(tip, nil))
	return ls
}

func craftedByronEbbAfter(
	parent gledger.Block,
	epoch uint64,
) *byron.ByronEpochBoundaryBlock {
	header := &byron.ByronEpochBoundaryBlockHeader{PrevBlock: parent.Hash()}
	header.ConsensusData.Epoch = epoch
	header.ConsensusData.Difficulty.Value = parent.BlockNumber()
	return &byron.ByronEpochBoundaryBlock{BlockHeader: header}
}

// TestHeaderEraRegressionRejectedAtEveryEntryPoint feeds Byron headers that
// extend the first mainnet Shelley block to every header entry point. The
// hard-fork combinator only moves the ledger state forward, and the reference
// rejects a header from any era but its parent ledger view's as
// HardForkEnvelopeErrWrongEra. The crafted EBB is unsigned; without an era
// rule it passes on the current-slot bound alone.
func TestHeaderEraRegressionRejectedAtEveryEntryPoint(t *testing.T) {
	t.Parallel()

	shelleyTip := loadBoundaryBlock(
		t,
		"mainnet-shelley-first-4492800.cbor",
		gledger.BlockTypeShelley,
	)
	mainHeader := &byron.ByronMainBlockHeader{PrevBlock: shelleyTip.Hash()}
	mainHeader.ConsensusData.Difficulty.Value = shelleyTip.BlockNumber() + 1
	inputs := []struct {
		name  string
		block gledger.Block
	}{
		{"crafted EBB after Shelley", craftedByronEbbAfter(shelleyTip, 0)},
		{
			"Byron main block after Shelley",
			&byron.ByronMainBlock{BlockHeader: mainHeader},
		},
	}
	for _, input := range inputs {
		for _, entry := range headerEntryPoints {
			t.Run(input.name+"/"+entry.name, func(t *testing.T) {
				t.Parallel()
				ls := newEraOrderTestLedger(t, shelleyTip)
				err := entry.verify(ls, input.block)
				require.ErrorContains(t, err, eraRegressionRule)
			})
		}
	}
}

// TestHeaderEraOrderAcceptsByronShelleyBoundary is the honest control: the
// first Shelley header extending the last Byron block must not trip the era
// rule at any entry point. Later checks may still defer it here, because this
// ledger has no epoch nonce for the Shelley slot.
func TestHeaderEraOrderAcceptsByronShelleyBoundary(t *testing.T) {
	t.Parallel()

	for _, fx := range boundaryFixtures {
		lastByron := loadBoundaryBlock(t, fx.byronFile, fx.byronType)
		firstShelley := loadBoundaryBlock(t, fx.shelleyFile, fx.shelleyType)
		for _, entry := range headerEntryPoints {
			t.Run(fx.name+"/"+entry.name, func(t *testing.T) {
				t.Parallel()
				ls := newEraOrderTestLedger(t, lastByron)
				if err := entry.verify(ls, firstShelley); err != nil {
					require.NotContains(t, err.Error(), eraRegressionRule)
				}
			})
		}
	}
}

// TestInboundEnvelopeRejectsEraRegression pins the block-level rule at ledger
// apply, where the parent is either the stored tip or the previous block of
// the batch. The EBB is otherwise well placed: it sits on its epoch boundary
// slot, after the parent's slot, with the parent's block number.
func TestInboundEnvelopeRejectsEraRegression(t *testing.T) {
	t.Parallel()

	lastByron := loadBoundaryBlock(
		t,
		"mainnet-byron-last-4492799.cbor",
		gledger.BlockTypeByronMain,
	)
	shelley := loadBoundaryBlock(
		t,
		"mainnet-shelley-first-4492800.cbor",
		gledger.BlockTypeShelley,
	)
	ebb := craftedByronEbbAfter(shelley, 209)
	require.Greater(t, ebb.SlotNumber(), shelley.SlotNumber())

	parents := map[string]envelopeParent{
		"stored tip": envelopeParentFromTip(
			shelley.SlotNumber(),
			shelley.BlockNumber(),
			shelley.Hash().Bytes(),
			uint(gledger.BlockTypeShelley),
			true,
		),
		"previous block in batch": envelopeParentFromBlock(shelley),
	}
	for name, parent := range parents {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := validateInboundBlockEnvelope(ebb, nil, nil, parent)
			require.ErrorContains(t, err, eraRegressionRule)
		})
	}
	require.NoError(
		t,
		validateBlockOrder(shelley, envelopeParentFromBlock(lastByron)),
	)
}
