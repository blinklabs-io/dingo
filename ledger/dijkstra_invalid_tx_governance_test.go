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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// newDijkstraHardForkFixture returns the collateral-return fixture with a
// HardForkInitiation proposal for a protocol version that cannot follow the
// enacted one, declared with the given phase-2 validity and re-signed.
func newDijkstraHardForkFixture(
	t *testing.T,
	valid bool,
) *dijkstraCollateralReturnFixture {
	t.Helper()
	fx := newDijkstraCollateralReturnFixture(t, lcommon.AddressTypeKeyNone)

	rewardAccount, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		make([]byte, lcommon.AddressHashSize),
	)
	require.NoError(t, err)
	action := &lcommon.HardForkInitiationGovAction{
		Type: uint(lcommon.GovActionTypeHardForkInitiation),
	}
	action.ProtocolVersion.Major = gdijkstra.MinProtocolVersionDijkstra
	action.ProtocolVersion.Minor = 2
	fx.tx.Body.TxProposalProcedures = []gdijkstra.DijkstraProposalProcedure{{
		PPRewardAccount: rewardAccount,
		PPGovAction: gdijkstra.DijkstraGovAction{
			Type:   uint(lcommon.GovActionTypeHardForkInitiation),
			Action: action,
		},
	}}
	addDijkstraFailingScriptSpend(t, fx)
	fx.tx.Body.SetCbor(nil)
	fx.tx.SetCbor(nil)

	privateKey := ed25519.NewKeyFromSeed(
		bytes.Repeat([]byte{0x91}, ed25519.SeedSize),
	)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	bodyCbor, err := cbor.Encode(fx.tx.Body)
	require.NoError(t, err)
	fx.tx.Body.SetCbor(bodyCbor)
	fx.tx.WitnessSet.VkeyWitnesses = cbor.NewSetType(
		[]lcommon.VkeyWitness{{
			Vkey:      publicKey,
			Signature: ed25519.Sign(privateKey, fx.tx.Hash().Bytes()),
		}},
		false,
	)
	fx.tx.WitnessSet.SetCbor(nil)
	fx.tx.SetCbor(nil)
	txCbor, err := fx.tx.MarshalCBOR()
	require.NoError(t, err)
	fx.txCbor = txCbor
	fx.tx, err = gdijkstra.NewDijkstraTransactionFromCbor(txCbor)
	require.NoError(t, err)

	// Only the block_transaction wire form carries is_valid, and only the
	// block encoder writes it, so the declared validity is set on the
	// transaction the block is built from and read back from the decoded block.
	blockTx := *fx.tx
	blockTx.TxIsValid = valid
	blockTx.SetCbor(nil)
	fx.block = newDijkstraCollateralReturnBlock(t, &blockTx)
	decodedTx := fx.block.BlockBody.Transactions[0]
	fx.tx = &decodedTx
	require.Equal(t, valid, fx.tx.IsValid())
	var txHash [32]byte
	copy(txHash[:], fx.tx.Hash().Bytes())
	fx.offsets = &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			txHash: {
				BlockSlot:  dijkstraCollateralReturnTestSlot,
				ByteLength: uint32(len(fx.tx.Cbor())), // #nosec G115 -- bounded fixture
			},
		},
		UtxoOffsets: map[database.UtxoRef]database.CborOffset{},
	}
	for _, utxo := range fx.tx.Produced() {
		var producedHash [32]byte
		copy(producedHash[:], utxo.Id.Id().Bytes())
		fx.offsets.UtxoOffsets[database.UtxoRef{
			TxId:      producedHash,
			OutputIdx: uint32(utxo.Id.Index()), // #nosec G115 -- bounded fixture
		}] = database.CborOffset{
			BlockSlot:  dijkstraCollateralReturnTestSlot,
			ByteLength: 1,
		}
	}
	// Block nonce evolution seeds from the Shelley genesis hash.
	fx.ls.config.CardanoNodeConfig.ShelleyGenesisHash = strings.Repeat("00", 32)
	return fx
}

// addDijkstraFailingScriptSpend makes the fixture's regular input a Plutus V3
// script output, with a witnessed script, redeemer and script data hash, so a
// declared-invalid transaction has the phase-2 evidence the validity flag
// rule requires. Block application skips Plutus evaluation, so the script is
// never run there.
func addDijkstraFailingScriptSpend(
	t *testing.T,
	fx *dijkstraCollateralReturnFixture,
) {
	t.Helper()
	program := &syn.Program[syn.DeBruijn]{
		Version: lang.LanguageVersion{1, 1, 0},
		Term:    &syn.Lambda[syn.DeBruijn]{Body: &syn.Error{}},
	}
	flat, err := syn.Encode(program)
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flat)
	require.NoError(t, err)
	plutusScript := lcommon.PlutusV3Script(scriptBytes)
	scriptHash := plutusScript.Hash()
	scriptAddress, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeScriptNone,
		lcommon.AddressNetworkTestnet,
		scriptHash[:],
		nil,
	)
	require.NoError(t, err)
	outputCbor, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
		OutputAddress: scriptAddress,
		OutputAmount:  10_000_000,
	})
	require.NoError(t, err)
	require.NoError(t, fx.db.Transaction(t.Context(), true).Do(func(txn *database.Txn) error {
		return fx.db.Blob().SetUtxo(txn.Blob(), fx.inputIds[0], 0, outputCbor)
	}))

	datumBytes, err := data.Encode(data.NewInteger(big.NewInt(42)))
	require.NoError(t, err)
	var datum lcommon.Datum
	_, err = cbor.Decode(datumBytes, &datum)
	require.NoError(t, err)
	fx.tx.WitnessSet.WsPlutusV3Scripts = cbor.NewSetType(
		[]lcommon.PlutusV3Script{plutusScript},
		false,
	)
	fx.tx.WitnessSet.WsRedeemers = gdijkstra.DijkstraRedeemers{
		Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
			{Tag: lcommon.RedeemerTagSpend, Index: 0}: {
				Data:    datum,
				ExUnits: lcommon.ExUnits{Steps: 1_000, Memory: 1_000},
			},
		},
	}
	params, ok := fx.ls.currentPParams.(*gdijkstra.DijkstraProtocolParameters)
	require.True(t, ok)
	params.CostModels = map[uint][]int64{
		2: make([]int64, len(lang.GetParamNamesForVersion(lang.LanguageVersionV3))),
	}
	params.ExecutionCosts = lcommon.ExUnitPrice{
		MemPrice:  &cbor.Rat{Rat: big.NewRat(1, 1000)},
		StepPrice: &cbor.Rat{Rat: big.NewRat(1, 1000)},
	}
	params.MaxTxExUnits = lcommon.ExUnits{Steps: 1_000_000, Memory: 1_000_000}
	params.MaxBlockExUnits = lcommon.ExUnits{Steps: 1_000_000, Memory: 1_000_000}
	redeemersCbor, err := cbor.Encode(fx.tx.WitnessSet.WsRedeemers.Redeemers)
	require.NoError(t, err)
	fx.tx.WitnessSet.WsRedeemers.SetCbor(redeemersCbor)
	langViews, err := lcommon.EncodeLangViews(
		map[uint]struct{}{2: {}},
		params.CostModels,
	)
	require.NoError(t, err)
	scriptDataHash := lcommon.Blake2b256Hash(append(redeemersCbor, langViews...))
	fx.tx.Body.TxScriptDataHash = &scriptDataHash
}

// processDijkstraHardForkBlock applies the fixture block through the live
// import path with phase-2 evaluation skipped, so the declared validity flag
// is the only input that changes which rules apply.
func processDijkstraHardForkBlock(
	ctx context.Context,
	fx *dijkstraCollateralReturnFixture,
) error {
	return fx.db.Transaction(ctx, true).Do(func(txn *database.Txn) error {
		_, err := fx.ls.ledgerProcessBlock(
			ctx,
			txn,
			ocommon.NewPoint(
				dijkstraCollateralReturnTestSlot,
				fx.block.Hash().Bytes(),
			),
			fx.block,
			true,
			false,
			true,
			nil,
			envelopeParent{origin: true},
			fx.offsets,
			eras.DijkstraEraDesc,
			fx.ls.currentPParams,
			nil,
			0,
			0,
			false,
		)
		return err
	})
}

// newDijkstraHardForkReplayFixture builds the same transaction on a ledger
// whose chain holds the block, so the replay path reads it back from storage.
func newDijkstraHardForkReplayFixture(
	t *testing.T,
	valid bool,
) *dijkstraCollateralReturnFixture {
	t.Helper()
	fx := newDijkstraHardForkFixture(t, valid)
	cm, err := chain.NewManager(t.Context(), fx.db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(t.Context(), []chain.RawBlock{fx.rawBlock()}),
	)
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:              fx.db,
		ChainManager:          cm,
		CardanoNodeConfig:     fx.ls.config.CardanoNodeConfig,
		Logger:                fx.ls.config.Logger,
		PromRegistry:          prometheus.NewRegistry(),
		ValidateHistorical:    true,
		EnableDijkstra:        true,
		ManualBlockProcessing: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	seedDijkstraHardForkLedger(ls, fx.ls)
	require.NoError(t, cm.SetLedger(ls))
	fx.ls = ls
	return fx
}

// seedDijkstraHardForkLedger puts a fresh ledger state in the Dijkstra era at
// epoch zero with the fixture's protocol parameters and an empty tip.
func seedDijkstraHardForkLedger(ls, template *LedgerState) {
	ls.currentEra = eras.DijkstraEraDesc
	ls.currentPParams = template.currentPParams
	ls.currentEpoch = template.currentEpoch
	ls.epochCache = []models.Epoch{ls.currentEpoch}
	ls.currentTip = ochainsync.Tip{}
	ls.publishSnapshotsLocked()
}

func replayDijkstraHardForkBlock(
	t *testing.T,
	fx *dijkstraCollateralReturnFixture,
) error {
	t.Helper()
	results := make(chan readChainResult, 1)
	results <- readChainResult{blocks: []gledger.Block{fx.block}}
	close(results)
	return fx.ls.ledgerProcessBlocksFromSource(context.Background(), results)
}

// requireDijkstraCollateralOnlyEffect asserts the reference result for a
// phase-2-invalid transaction: the collateral input is spent, the collateral
// return is created at index len(outputs), the regular input survives, and no
// governance proposal is recorded.
func requireDijkstraCollateralOnlyEffect(
	t *testing.T,
	fx *dijkstraCollateralReturnFixture,
) {
	t.Helper()
	_, err := fx.db.UtxoByRef(t.Context(), fx.inputIds[0], 0, nil)
	require.NoError(t, err, "regular input must stay unspent")
	_, err = fx.db.UtxoByRef(t.Context(), fx.inputIds[1], 0, nil)
	require.ErrorIs(t, err, database.ErrUtxoNotFound, "collateral input must be spent")
	collateral, err := fx.db.UtxoByRefIncludingSpent(t.Context(), fx.inputIds[1], 0, nil)
	require.NoError(t, err)
	require.NotZero(t, collateral.DeletedSlot)
	collateralReturn, err := fx.db.UtxoByRef(
		t.Context(),
		fx.tx.Hash().Bytes(),
		uint32(len(fx.tx.Outputs())), // #nosec G115 -- fixture output count
		nil,
	)
	require.NoError(t, err)
	require.Zero(t, collateralReturn.DeletedSlot)
	regularOutput, err := fx.db.UtxoByRefIncludingSpent(
		t.Context(),
		fx.tx.Hash().Bytes(), 0, nil,
	)
	require.NoError(t, err)
	require.Nil(t, regularOutput, "regular outputs must not be created")
	_, err = fx.db.GetGovernanceProposal(t.Context(), fx.tx.Hash().Bytes(), 0, nil)
	require.ErrorIs(t, err, models.ErrGovernanceProposalNotFound)
}

func TestDijkstraBlockApplicationScopesGovernanceToDeclaredValidity(
	t *testing.T,
) {
	t.Parallel()

	t.Run("live import", func(t *testing.T) {
		t.Parallel()
		valid := newDijkstraHardForkFixture(t, true)
		var badVersion conway.BadHardForkProtocolVersionError
		require.ErrorAs(t, processDijkstraHardForkBlock(t.Context(), valid), &badVersion)

		invalid := newDijkstraHardForkFixture(t, false)
		require.NoError(t, processDijkstraHardForkBlock(t.Context(), invalid))
	})

	t.Run("forged block revalidation", func(t *testing.T) {
		t.Parallel()
		valid := newDijkstraHardForkFixture(t, true)
		var badVersion conway.BadHardForkProtocolVersionError
		require.ErrorAs(t, valid.ls.validateForgedTxs(t.Context(), valid.block), &badVersion)

		invalid := newDijkstraHardForkFixture(t, false)
		require.NoError(t, invalid.ls.validateForgedTxs(t.Context(), invalid.block))
	})

	t.Run("mempool validation", func(t *testing.T) {
		t.Parallel()
		valid := newDijkstraHardForkFixture(t, true)
		var badVersion conway.BadHardForkProtocolVersionError
		require.ErrorAs(t, valid.ls.ValidateTx(valid.tx), &badVersion)

		// The mempool transaction form has no is_valid field, so a
		// declared-invalid transaction cannot reach mempool validation.
		bodyFields := map[uint]any{0: []any{}, 1: []any{}, 2: uint64(0)}
		for _, declared := range []bool{true, false} {
			raw, err := cbor.Encode(
				[]any{bodyFields, map[uint]any{}, declared, nil},
			)
			require.NoError(t, err)
			_, err = gdijkstra.NewDijkstraTransactionFromCbor(raw)
			if declared {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "is_valid=false")
			}
		}
	})

	t.Run("replay and rollback reapply", func(t *testing.T) {
		t.Parallel()
		valid := newDijkstraHardForkReplayFixture(t, true)
		var badVersion conway.BadHardForkProtocolVersionError
		require.ErrorAs(
			t,
			replayDijkstraHardForkBlock(t, valid),
			&badVersion,
		)

		invalid := newDijkstraHardForkReplayFixture(t, false)
		require.NoError(t, replayDijkstraHardForkBlock(t, invalid))
		requireDijkstraCollateralOnlyEffect(t, invalid)

		template := &LedgerState{
			currentPParams: invalid.ls.currentPParams,
			currentEpoch:   invalid.ls.currentEpoch,
		}
		require.NoError(t, invalid.ls.chain.Rollback(t.Context(), ocommon.Point{}))
		require.NoError(t, invalid.ls.rollback(t.Context(), ocommon.Point{}))
		seedDijkstraHardForkLedger(invalid.ls, template)
		_, err := invalid.db.UtxoByRef(t.Context(), invalid.inputIds[1], 0, nil)
		require.NoError(t, err, "rollback must restore the spent collateral")
		require.NoError(
			t,
			invalid.ls.chain.AddRawBlocks(t.Context(), []chain.RawBlock{invalid.rawBlock()}),
		)
		require.NoError(t, replayDijkstraHardForkBlock(t, invalid))
		requireDijkstraCollateralOnlyEffect(t, invalid)
	})
}
