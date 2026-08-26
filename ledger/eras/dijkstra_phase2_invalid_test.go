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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package eras

import (
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/require"
)

type dijkstraTxMarkedPhase2Valid struct {
	lcommon.Transaction
}

func (dijkstraTxMarkedPhase2Valid) IsValid() bool {
	return true
}

func newFailingDijkstraMintTx(t *testing.T) *gdijkstra.DijkstraTransaction {
	t.Helper()
	program := &syn.Program[syn.DeBruijn]{
		Version: lang.LanguageVersionV2,
		Term: &syn.Lambda[syn.DeBruijn]{
			Body: &syn.Lambda[syn.DeBruijn]{
				Body: &syn.Error{},
			},
		},
	}
	flatProgram, err := syn.Encode(program)
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flatProgram)
	require.NoError(t, err)

	plutusScript := lcommon.PlutusV2Script(scriptBytes)
	scriptHash := plutusScript.Hash()
	assetMint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
			lcommon.Blake2b224(scriptHash): {
				cbor.NewByteString([]byte("asset")): big.NewInt(1),
			},
		},
	)
	return &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxFee:  200_000,
			TxMint: &assetMint,
		},
		WitnessSet: gdijkstra.DijkstraTransactionWitnessSet{
			WsPlutusV2Scripts: cbor.NewSetType(
				[]lcommon.PlutusV2Script{plutusScript},
				false,
			),
			WsRedeemers: gdijkstra.DijkstraRedeemers{
				Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
					{Tag: lcommon.RedeemerTagMint, Index: 0}: {
						Data: lcommon.Datum{
							Data: data.NewInteger(big.NewInt(0)),
						},
						ExUnits: lcommon.ExUnits{
							Memory: 10_000_000,
							Steps:  10_000_000_000,
						},
					},
				},
			},
		},
		TxIsValid: false,
	}
}

func TestValidateTxDijkstraPhase2InvalidSkipsFailingScriptEvaluation(
	t *testing.T,
) {
	tx := newFailingDijkstraMintTx(t)
	pparams := &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
			CostModels: map[uint][]int64{
				1: DefaultPlutusV2CostModel,
			},
		},
	}
	ls := newMockLedgerState()

	origAll := dijkstraUtxoValidationRules
	origPhase1 := dijkstraPhase1UtxoValidationRules
	t.Cleanup(func() {
		dijkstraUtxoValidationRules = origAll
		dijkstraPhase1UtxoValidationRules = origPhase1
	})
	evaluations := 0
	phase2Rule := indexedUtxoValidationRule{
		index: dijkstraUtxoValidatePlutusScriptsRuleIndex,
		validationFunc: func(
			tx lcommon.Transaction,
			slot uint64,
			ls lcommon.LedgerState,
			pp lcommon.ProtocolParameters,
		) error {
			evaluations++
			return gdijkstra.UtxoValidatePlutusScripts(
				dijkstraTxMarkedPhase2Valid{Transaction: tx},
				slot,
				ls,
				pp,
			)
		},
	}
	dijkstraUtxoValidationRules = []indexedUtxoValidationRule{phase2Rule}
	dijkstraPhase1UtxoValidationRules = nil

	controlErr := phase2Rule.validationFunc(tx, 0, ls, pparams)
	var plutusErr conway.PlutusScriptFailedError
	require.ErrorAs(t, controlErr, &plutusErr)
	require.Equal(t, 1, evaluations)

	evaluations = 0
	require.NoError(t, ValidateTxDijkstra(tx, 0, ls, pparams))
	require.Zero(t, evaluations)
}
