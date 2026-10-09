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
	"bytes"
	"context"
	"crypto/ed25519"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// newReplayTestLedger returns a LedgerState whose primary chain holds block
// and which replays it through ledgerProcessBlocksFromSource with historical
// validation on, starting from an empty tip in epoch 0 of era.
func newReplayTestLedger(
	t *testing.T,
	db *database.Database,
	block gledger.Block,
	blockType uint,
	era eras.EraDesc,
	pparams lcommon.ProtocolParameters,
) *LedgerState {
	t.Helper()
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(context.Background(), []chain.RawBlock{{
		Slot:        block.SlotNumber(),
		Hash:        block.Hash().Bytes(),
		BlockNumber: block.BlockNumber(),
		Type:        blockType,
		PrevHash:    block.PrevHash().Bytes(),
		Cbor:        block.Cbor(),
	}}))
	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:              db,
		ChainManager:          cm,
		CardanoNodeConfig:     nodeConfig,
		Logger:                testLogger(),
		PromRegistry:          prometheus.NewRegistry(),
		ValidateHistorical:    true,
		EnableDijkstra:        era.Id == gdijkstra.EraIdDijkstra,
		ManualBlockProcessing: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	setReplayTestLedgerOrigin(ls, era, pparams)
	require.NoError(t, cm.SetLedger(ls))
	return ls
}

// setReplayTestLedgerOrigin places ls at an empty tip in epoch 0 of era.
func setReplayTestLedgerOrigin(
	ls *LedgerState,
	era eras.EraDesc,
	pparams lcommon.ProtocolParameters,
) {
	ls.currentEra = era
	ls.currentPParams = pparams
	ls.currentEpoch = models.Epoch{
		SlotLength:    1_000,
		LengthInSlots: 1_000,
		EraId:         era.Id,
	}
	ls.epochCache = []models.Epoch{ls.currentEpoch}
	ls.currentTip = ochainsync.Tip{}
	// The rolling nonce of a block that applies is derived from its parent's.
	ls.currentTipBlockNonce = make([]byte, 32)
	ls.publishSnapshotsLocked()
}

func replayTestBlock(ls *LedgerState, block gledger.Block) error {
	results := make(chan readChainResult, 1)
	results <- readChainResult{blocks: []gledger.Block{block}}
	close(results)
	return ls.ledgerProcessBlocksFromSource(context.Background(), results)
}

// alwaysSucceedsV1 is a Plutus V1 spending validator that ignores its datum,
// redeemer and context.
func alwaysSucceedsV1(t *testing.T) lcommon.PlutusV1Script {
	t.Helper()
	program := &syn.Program[syn.DeBruijn]{
		Version: lang.LanguageVersion{1, 0, 0},
		Term: &syn.Lambda[syn.DeBruijn]{Body: &syn.Lambda[syn.DeBruijn]{
			Body: &syn.Lambda[syn.DeBruijn]{
				Body: &syn.Constant{Con: &syn.Unit{}},
			},
		}},
	}
	flat, err := syn.Encode(program)
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flat)
	require.NoError(t, err)
	return lcommon.PlutusV1Script(scriptBytes)
}

// signedDatumSpend returns the body and witness set of a signed, balanced
// transaction that spends the fixture's V1 script input, with a correct script
// integrity hash. Only withWitnessDatum decides whether phase 1 accepts it.
func signedDatumSpend(
	t *testing.T,
	f *requiredDatumFixture,
	costModels map[uint][]int64,
	withWitnessDatum bool,
) (cbor.RawMessage, map[uint]any) {
	t.Helper()
	publicKey := f.key.Public().(ed25519.PublicKey)
	keyAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		lcommon.Blake2b224Hash(publicKey).Bytes(),
		nil,
	)
	require.NoError(t, err)
	keyAddrBytes, err := keyAddr.Bytes()
	require.NoError(t, err)

	redeemersCbor, err := cbor.Encode(map[any]any{
		[2]uint64{uint64(lcommon.RedeemerTagSpend), 0}: []any{
			f.datum, []uint64{1_000_000, 1_000_000_000},
		},
	})
	require.NoError(t, err)
	langViews, err := lcommon.EncodeLangViews(
		map[uint]struct{}{0: {}},
		costModels,
	)
	require.NoError(t, err)
	var datumsCbor []byte
	if withWitnessDatum {
		datumsCbor, err = cbor.Encode([]any{f.datum})
		require.NoError(t, err)
	}
	integrity := lcommon.Blake2b256Hash(
		bytes.Join([][]byte{redeemersCbor, datumsCbor, langViews}, nil),
	)
	bodyCbor, err := cbor.Encode(map[uint]any{
		0: f.inputSet(),
		1: []any{map[uint]any{0: keyAddrBytes, 1: uint64(9_999_998)}},
		// Covers the redeemer budget at the test execution prices.
		2:  uint64(2),
		11: integrity.Bytes(),
		13: f.collateralSet(),
	})
	require.NoError(t, err)
	bodyHash := lcommon.Blake2b256Hash(bodyCbor)
	witnessSet := f.witnessSetMap(false)
	witnessSet[0] = []any{[]any{
		[]byte(publicKey),
		ed25519.Sign(f.key, bodyHash.Bytes()),
	}}
	witnessSet[5] = cbor.RawMessage(redeemersCbor)
	if withWitnessDatum {
		witnessSet[4] = cbor.RawMessage(datumsCbor)
	}
	return cbor.RawMessage(bodyCbor), witnessSet
}

// TestLedgerReplayRejectsMissingRequiredSpendingDatumBeforeStateMutation
// replays, through ledgerProcessBlocksFromSource, a Conway or Dijkstra block
// whose transaction spends a Plutus V1 script input without the datum its
// hash names. Phase 1 must reject it for either isValid value: as
// isValid=false the script fails for want of the datum, which would otherwise
// match the declared failure and consume the collateral. Neither the ledger
// tip nor either input may move. The control carries the datum and applies,
// so the fixture fails on nothing else.
func TestLedgerReplayRejectsMissingRequiredSpendingDatumBeforeStateMutation(
	t *testing.T,
) {
	t.Parallel()

	const slot = dijkstraCollateralReturnTestSlot
	for _, era := range []eras.EraDesc{eras.ConwayEraDesc, eras.DijkstraEraDesc} {
		for _, tc := range []struct {
			name         string
			witnessDatum bool
			valid        bool
		}{
			{name: "missing datum/isValid=true", valid: true},
			{name: "missing datum/isValid=false", valid: false},
			{name: "datum present/isValid=true", witnessDatum: true, valid: true},
		} {
			t.Run(era.Name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				f := newRequiredDatumFixture(t, alwaysSucceedsV1(t), true)
				costModels := map[uint][]int64{
					0: blockV3MachineCostModel(t, lang.LanguageVersionV1),
				}
				body, witnessSet := signedDatumSpend(
					t, f, costModels, tc.witnessDatum,
				)
				conwayPP := conway.ConwayProtocolParameters{
					ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
						Major: lcommon.ProtocolVersionPlomin,
					},
					MaxBlockBodySize:     100_000,
					MaxBlockHeaderSize:   100_000,
					MaxTxSize:            16_384,
					MaxValueSize:         5_000,
					MaxCollateralInputs:  3,
					CollateralPercentage: 150,
					CostModels:           costModels,
					ExecutionCosts: lcommon.ExUnitPrice{
						MemPrice:  &cbor.Rat{Rat: big.NewRat(1, 1_000_000_000)},
						StepPrice: &cbor.Rat{Rat: big.NewRat(1, 1_000_000_000)},
					},
					MaxTxExUnits: lcommon.ExUnits{
						Memory: 10_000_000, Steps: 10_000_000_000,
					},
					MaxBlockExUnits: lcommon.ExUnits{
						Memory: 50_000_000, Steps: 50_000_000_000,
					},
				}
				var ls *LedgerState
				var block gledger.Block
				if era.Id == gdijkstra.EraIdDijkstra {
					txCbor, err := cbor.Encode([]any{body, witnessSet, nil})
					require.NoError(t, err)
					tx, err := gdijkstra.NewDijkstraTransactionFromCbor(txCbor)
					require.NoError(t, err)
					// The standalone transaction encoding has no is_valid
					// field; a block carries it per transaction.
					tx.TxIsValid = tc.valid
					block = newDijkstraCollateralReturnBlock(t, tx)
					require.Equal(t, tc.valid, block.Transactions()[0].IsValid())
					dijkstraPP := dijkstraTestProtocolParameters()
					conwayPP.ProtocolVersion = dijkstraPP.ProtocolVersion
					dijkstraPP.ConwayProtocolParameters = conwayPP
					ls = newReplayTestLedger(
						t, f.db, block, uint(gledger.BlockTypeDijkstra),
						era, dijkstraPP,
					)
				} else {
					txCbor, err := cbor.Encode(
						[]any{body, witnessSet, tc.valid, nil},
					)
					require.NoError(t, err)
					block, _ = conwayTestBlock(
						t, txCbor, uint(lcommon.ProtocolVersionPlomin), slot,
					)
					ls = newReplayTestLedger(
						t, f.db, block, uint(gledger.BlockTypeConway),
						era, &conwayPP,
					)
				}
				inputIds := [][]byte{f.spendTxId, f.collateralTxId}
				before := make([]*models.Utxo, 0, len(inputIds))
				for _, id := range inputIds {
					utxo, err := f.db.UtxoByRef(context.Background(), id, 0, nil)
					require.NoError(t, err)
					before = append(before, utxo)
				}

				err := replayTestBlock(ls, block)
				tip, tipErr := f.db.GetTip(nil)
				require.NoError(t, tipErr)
				if tc.witnessDatum {
					require.NoError(t, err)
					require.Equal(t, block.Hash().Bytes(), tip.Point.Hash)
					_, err = f.db.UtxoByRef(context.Background(), f.spendTxId, 0, nil)
					require.ErrorIs(t, err, types.ErrUtxoNotFound)
					return
				}
				var missing lcommon.MissingDatumForSpendingScriptError
				require.ErrorAs(t, err, &missing)
				require.Equal(t, f.script.Hash(), missing.ScriptHash)
				require.Equal(t, ochainsync.Tip{}, tip)
				for i, id := range inputIds {
					utxo, err := f.db.UtxoByRef(context.Background(), id, 0, nil)
					require.NoError(t, err)
					require.Equal(t, before[i], utxo)
				}
			})
		}
	}
}
