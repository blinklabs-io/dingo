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
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	omockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// errOverDeclaredBudget is the stand-in for the real
//
//	"script exceeded declared budget: used (...), declared (...)"
//
// that conway.go's restrictive post-check produces. A LedgerView that trusts
// the producer's declared-budget verdict tolerates it; a strict one rejects it.
var errOverDeclaredBudget = errors.New("script exceeded declared budget")

// budgetTrustProbeEra returns a Dijkstra era whose phase-2 validator rejects an
// over-declared-budget script UNLESS the LedgerView trusts the producer's
// budget verdict. This mirrors the real conway.go behavior (#3627) at the
// block-application boundary without standing up a full Plutus evaluation, so
// the test isolates exactly which apply paths turn the trust on.
func budgetTrustProbeEra(sawTrust *bool, ran *bool) eras.EraDesc {
	testEra := eras.DijkstraEraDesc
	testEra.ValidateTxFunc = func(
		_ lcommon.Transaction,
		_ uint64,
		state lcommon.LedgerState,
		_ lcommon.ProtocolParameters,
	) error {
		if ran != nil {
			*ran = true
		}
		truster, ok := state.(interface {
			TrustProducerPlutusBudget() bool
		})
		trusted := ok && truster.TrustProducerPlutusBudget()
		if sawTrust != nil {
			*sawTrust = trusted
		}
		if trusted {
			// Producer trusted: tolerate the over-declared budget.
			return nil
		}
		// Strict: reject exactly as the restrictive post-check would.
		return conway.PlutusScriptFailedError{Err: errOverDeclaredBudget}
	}
	return testEra
}

func budgetTrustProbePParams() *gdijkstra.DijkstraProtocolParameters {
	return &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
			MaxBlockBodySize:   100_000,
			MaxBlockHeaderSize: 100_000,
		},
	}
}

func budgetTrustProbeBlock() *validityOutcomeTestBlock {
	tx := omockledger.NewTransactionBuilder()
	tx.WithType(gdijkstra.TxTypeDijkstra)
	tx.WithValid(true)
	return &validityOutcomeTestBlock{
		header: &gdijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: 1,
					Slot:        10,
					ProtoVersion: babbage.BabbageProtoVersion{
						Major: gdijkstra.MinProtocolVersionDijkstra,
					},
				},
			},
		},
		txs: []lcommon.Transaction{tx},
		era: gdijkstra.EraDijkstra,
	}
}

func budgetTrustProbeOffsets(
	block *validityOutcomeTestBlock,
) *database.BlockIngestionResult {
	var txHash [32]byte
	copy(txHash[:], block.txs[0].Hash().Bytes())
	return &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			txHash: {BlockSlot: 10, ByteLength: 1},
		},
	}
}

// runBudgetTrustProbe applies one over-declared-budget block through
// ledgerProcessBlock with the given trust flag and returns the apply error and
// whether the phase-2 validator observed the trust.
func runBudgetTrustProbe(
	t *testing.T,
	trustProducerPlutusBudget bool,
) (error, bool) {
	t.Helper()
	db := newTestDB(t)
	var sawTrust, ran bool
	testEra := budgetTrustProbeEra(&sawTrust, &ran)
	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls := &LedgerState{
		db:         db,
		activeEras: []eras.EraDesc{testEra},
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            testLogger(),
		},
		currentEra: testEra,
	}
	block := budgetTrustProbeBlock()
	offsets := budgetTrustProbeOffsets(block)
	// shouldValidate + reachesTip true, phase-2 NOT skipped, so the phase-2
	// validator runs and the trust flag is the only thing under test.
	err := db.Transaction(true).Do(func(txn *database.Txn) error {
		_, perr := ls.ledgerProcessBlock(
			txn,
			ocommon.NewPoint(10, block.Hash().Bytes()),
			block,
			true,
			true,
			false,
			nil,
			envelopeParent{origin: true},
			offsets,
			testEra,
			budgetTrustProbePParams(),
			nil,
			trustProducerPlutusBudget,
		)
		return perr
	})
	require.True(t, ran, "phase-2 validator must actually run")
	return err, sawTrust
}

// The live followed-chain apply path (trust=true) TOLERATES a producer-valid
// over-declared-budget script, while the historical-replay path (trust=false)
// still REJECTS it. This is the #3625/#3627 interaction: replay routes phase-2
// through ledgerProcessBlock too, and must NOT inherit the live-follower trust.
func TestLedgerProcessBlockProducerTrustGatedByCaller(t *testing.T) {
	t.Run("live apply trusts producer budget", func(t *testing.T) {
		err, sawTrust := runBudgetTrustProbe(t, true)
		require.NoError(
			t,
			err,
			"live followed-chain apply must tolerate the producer's declared-budget verdict",
		)
		require.True(t, sawTrust, "phase-2 validator must see trust enabled on live apply")
	})

	t.Run("historical replay stays strict", func(t *testing.T) {
		err, sawTrust := runBudgetTrustProbe(t, false)
		require.Error(
			t,
			err,
			"historical replay must NOT trust the producer budget and must reject the overage",
		)
		var plutusErr conway.PlutusScriptFailedError
		require.ErrorAs(t, err, &plutusErr)
		require.ErrorIs(t, plutusErr.Err, errOverDeclaredBudget)
		require.False(t, sawTrust, "phase-2 validator must see trust disabled on replay")
	})
}
