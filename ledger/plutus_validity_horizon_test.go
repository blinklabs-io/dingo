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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	omockfixtures "github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// previewWedgeLedgerState reproduces the ledger state the from-genesis Preview
// replay was in when it wedged on issue #3844: epoch 40 of the Babbage era, a
// published tip at block 168143 (slot 3516450), and the next two blocks not yet
// reflected in that tip because their batch had not committed. Preview's
// genesis gives the 25920-slot safe zone (see newTestEraHistoryCfg).
func previewWedgeLedgerState(t testing.TB) *LedgerState {
	t.Helper()
	nodeConfig := newTestEraHistoryCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls := &LedgerState{
		epochCache: []models.Epoch{{
			EpochId:       previewEraStartEpoch,
			StartSlot:     previewEraStartSlot,
			SlotLength:    1_000,
			LengthInSlots: previewEpochSize,
			EraId:         eras.BabbageEraDesc.Id,
		}},
		currentEra: eras.BabbageEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				previewPublishedTipSlot,
				[]byte("published-tip"),
			),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            testLogger(),
		},
	}
	ls.publishSnapshotsLocked()
	return ls
}

// TestLedgerViewSlotToTimeUsesHorizonAnchor pins the routing at the call site
// the #3844 fix changes. LedgerView.SlotToTime is the converter every Plutus
// script context translates its validity interval through, so the anchor has to
// reach the summary from there and the horizon has to survive the trip.
func TestLedgerViewSlotToTimeUsesHorizonAnchor(t *testing.T) {
	ls := previewWedgeLedgerState(t)

	// Unanchored, this is the wedge: the view falls back to the published tip
	// and refuses the transaction's validity bound.
	unanchored := &LedgerView{ls: ls}
	_, err := unanchored.SlotToTime(previewTxUpperBound)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon,
		"the published tip must still leave this bound past the horizon; "+
			"if it does not, the fixture no longer reproduces #3844")

	// Anchored at the applied block's predecessor, the same bound converts.
	anchored := &LedgerView{ls: ls, horizonAnchorSlot: previewParentSlot}
	when, err := anchored.SlotToTime(previewTxUpperBound)
	require.NoError(t, err,
		"a Plutus validity bound inside the predecessor-anchored horizon "+
			"must translate")
	expected, err := ls.hardForkSummaryAnchoredAt(previewParentSlot)
	require.NoError(t, err)
	wantTime, err := expected.SlotToTime(previewTxUpperBound)
	require.NoError(t, err)
	assert.Equal(t, wantTime, when)

	// The anchor moves the horizon; it does not remove it. cardano-ledger
	// fails a Plutus transaction whose bound cannot be translated
	// (TimeTranslationPastHorizon), so this must stay an error rather than
	// become an in-era extrapolation.
	_, err = anchored.SlotToTime(previewParentHorizon)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon,
		"a bound past the anchored horizon must still be refused")
}

// errHorizonProbeDone stops ledgerProcessBlock right after the probe has run,
// so the assertion is about the LedgerView it was handed rather than about
// everything block application does afterwards.
var errHorizonProbeDone = errors.New("horizon probe complete")

// TestLedgerProcessBlockAnchorsValidationHorizonAtParent proves the anchor is
// actually wired from block application, not merely available on LedgerView.
// The reference implementation ticks from the applied block's immediate
// predecessor, so that predecessor — not the published tip, which lags by a
// whole block batch during replay — is what the safe zone must be measured
// from.
func TestLedgerProcessBlockAnchorsValidationHorizonAtParent(t *testing.T) {
	tests := []struct {
		name       string
		parentSlot uint64
		wantErr    error
	}{
		{
			// Block 168145's real predecessor on Preview is block 168144 at
			// slot 3516496, so this is the case that has to succeed.
			name:       "applied predecessor",
			parentSlot: previewParentSlot,
		},
		{
			// The published tip trails by one block. Before the fix this was
			// the only anchor available, and it rejected the block.
			name:       "published tip",
			parentSlot: previewPublishedTipSlot,
			wantErr:    hardfork.ErrPastHorizon,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := newTestDB(t)
			ls := previewWedgeLedgerState(t)
			ls.db = db

			var probeErr error
			var probed bool
			testEra := eras.BabbageEraDesc
			testEra.ValidateTxFunc = func(
				_ lcommon.Transaction,
				_ uint64,
				view lcommon.LedgerState,
				_ lcommon.ProtocolParameters,
			) error {
				lv, ok := view.(*LedgerView)
				require.True(t, ok,
					"block application must hand the era validator the "+
						"LedgerView that carries the horizon anchor")
				probed = true
				_, probeErr = lv.SlotToTime(previewTxUpperBound)
				return errHorizonProbeDone
			}
			ls.activeEras = []eras.EraDesc{testEra}

			blocks, err := omockfixtures.GenerateBabbageChain(
				168_145, lcommon.Blake2b256{}, previewBlockSlot, 1, 1,
			)
			require.NoError(t, err)
			block, ok := blocks[0].(*babbage.BabbageBlock)
			require.True(t, ok)
			block.TransactionBodies = []babbage.BabbageTransactionBody{{}}
			block.TransactionWitnessSets = []babbage.BabbageTransactionWitnessSet{{}}
			pparams := &babbage.BabbageProtocolParameters{
				ProtocolMajor:      8,
				MaxBlockBodySize:   100_000,
				MaxBlockHeaderSize: 100_000,
			}
			processErr := db.Transaction(true).
				Do(func(txn *database.Txn) error {
					_, err := ls.ledgerProcessBlock(
						txn,
						ocommon.NewPoint(
							previewBlockSlot,
							block.Hash().Bytes(),
						),
						block,
						true,
						false,
						false,
						nil,
						envelopeParent{
							slot:        test.parentSlot,
							blockNumber: 168_144,
						},
						&database.BlockIngestionResult{},
						testEra,
						pparams,
						nil,
						previewEraStartEpoch,
					)
					return err
				})
			require.ErrorIs(t, processErr, errHorizonProbeDone)
			require.True(t, probed)
			if test.wantErr != nil {
				require.ErrorIs(t, probeErr, test.wantErr)
				return
			}
			require.NoError(t, probeErr,
				"the block that wedged the Preview replay must convert its "+
					"Plutus validity bound")
		})
	}
}
