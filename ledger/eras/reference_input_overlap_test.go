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

package eras

import (
	"crypto/ed25519"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/require"
)

func TestValidateTxConwayReferenceInputOverlapStartsAtPV11(t *testing.T) {
	input := shelley.NewShelleyTransactionInput(
		"a228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee33",
		0,
	)
	const balance = uint64(10_000_000)
	tx := &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{input},
			),
			TxReferenceInputs: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{input},
				false,
			),
			TxFee: balance,
		},
		TxIsValid: true,
	}
	state := newMockLedgerState()
	state.addUtxo(input, newTestOutput(balance))

	for _, tc := range []struct {
		name       string
		major      uint
		wantReject bool
	}{
		{name: "PV10 retains global disjointness", major: lcommon.ProtocolVersionPlomin, wantReject: true},
		{name: "PV11 permits overlap without Plutus", major: lcommon.ProtocolVersionVanRossem},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTxConway(
				tx,
				0,
				state,
				&conway.ConwayProtocolParameters{
					MaxTxSize: 16_384,
					ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
						Major: tc.major,
					},
				},
			)
			if tc.wantReject {
				var overlap babbage.NonDisjointRefInputsError
				require.ErrorAs(t, err, &overlap)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestValidateTxDijkstraAllowsSpendReferenceInputOverlap(t *testing.T) {
	input := shelley.NewShelleyTransactionInput(
		"b228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee44",
		0,
	)
	const balance = uint64(10_000_000)
	tx := &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{input},
			),
			TxReferenceInputs: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{input},
				false,
			),
			TxFee: balance,
		},
		TxIsValid: true,
	}
	state := newMockLedgerState()
	state.addUtxo(input, newTestOutput(balance))
	protocolParams := &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MaxTxSize: 16_384,
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
		},
	}

	require.NoError(t, ValidateTxDijkstra(tx, 0, state, protocolParams))
}

func TestValidateTxPlutusConwayReferenceOverlapIsAllowedForExecutedPlutusV1AndV2(
	t *testing.T,
) {
	for _, tc := range []struct {
		name    string
		version lang.LanguageVersion
	}{
		{name: "PlutusV1", version: lang.LanguageVersionV1},
		{name: "PlutusV2", version: lang.LanguageVersionV2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scriptBytes := referenceOverlapFailingScriptBytes(t, tc.version)
			var plutusScript lcommon.Script
			switch tc.version {
			case lang.LanguageVersionV1:
				plutusScript = lcommon.PlutusV1Script(scriptBytes)
			case lang.LanguageVersionV2:
				plutusScript = lcommon.PlutusV2Script(scriptBytes)
			}
			tx, state, params := newConwayReferenceOverlapScriptTx(t, plutusScript, false)
			// The script deliberately fails. Declaring the transaction invalid
			// makes this a successful phase-2 outcome while proving the V1/V2
			// execution path accepts the overlapping input sets.
			require.NoError(t, ValidateTxPlutusConway(tx, 0, state, params))
		})
	}
}

func TestValidateTxPlutusConwayRejectsOverlapWhenPlutusV3Executes(t *testing.T) {
	scriptBytes := referenceOverlapFailingScriptBytes(t, lang.LanguageVersion{1, 1, 0})
	v3Script := lcommon.PlutusV3Script(scriptBytes)
	tx, state, params := newConwayReferenceOverlapScriptTx(
		t,
		v3Script,
		true,
	)

	err := ValidateTxPlutusConway(tx, 0, state, params)
	require.ErrorContains(t, err, "is also a regular input")
}

func TestValidateTxPlutusConwayDoesNotBuildV3ContextForUnusedWitness(t *testing.T) {
	input := shelley.NewShelleyTransactionInput(
		"c228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee55",
		0,
	)
	const balance = uint64(10_000_000)
	tx := &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{input},
			),
			TxReferenceInputs: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{input},
				false,
			),
			TxFee: balance,
		},
		WitnessSet: conway.ConwayTransactionWitnessSet{
			WsPlutusV3Scripts: cbor.NewSetType(
				[]lcommon.PlutusV3Script{
					referenceOverlapFailingScriptBytes(
						t,
						lang.LanguageVersion{1, 1, 0},
					),
				},
				false,
			),
		},
		TxIsValid: true,
	}
	state := newMockLedgerState()
	state.addUtxo(input, testAddressOutput{
		testOutput: newTestOutput(balance),
		addr:       newTestKeyAddress(t),
	})
	params := &conway.ConwayProtocolParameters{
		MaxTxSize: 16_384,
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: lcommon.ProtocolVersionVanRossem,
		},
	}

	require.NoError(t, ValidateTxPlutusConway(tx, 0, state, params))
}

func TestValidateTxDijkstraPlutusV3ContextStillRejectsInputOverlap(t *testing.T) {
	input := shelley.NewShelleyTransactionInput(
		"d228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee66",
		0,
	)
	collateralInput := shelley.NewShelleyTransactionInput(
		"e228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee77",
		0,
	)
	datum, datumHash := referenceOverlapDatum(t)
	v3Script := lcommon.PlutusV3Script(
		referenceOverlapFailingScriptBytes(t, lang.LanguageVersion{1, 1, 0}),
	)
	seed := make([]byte, ed25519.SeedSize)
	seed[0] = 0x77
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	paymentHash := lcommon.Blake2b224Hash(publicKey)
	collateralAddress, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		paymentHash[:],
		nil,
	)
	require.NoError(t, err)
	state := newMockLedgerState()
	state.networkId = uint(lcommon.AddressNetworkTestnet)
	state.addUtxo(input, referenceOverlapScriptOutput{
		testAddressScriptOutput: testAddressScriptOutput{
			testOutput: newTestOutput(10_000_000),
			addr:       newTestScriptAddress(t, v3Script),
		},
		datumHash: &datumHash,
	})
	state.addUtxo(collateralInput, testAddressOutput{
		testOutput: newTestOutput(3_000_000),
		addr:       collateralAddress,
	})
	tx := &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{input},
			),
			TxOutputs: []gdijkstra.DijkstraTransactionOutput{{
				Output: babbage.BabbageTransactionOutput{
					OutputAddress: newTestKeyAddress(t),
					OutputAmount:  mary.MaryTransactionOutputValue{Amount: 8_000_000},
				},
			}},
			TxFee: 2_000_000,
			TxCollateral: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{collateralInput},
				false,
			),
			TxTotalCollateral: 3_000_000,
			TxReferenceInputs: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{input},
				false,
			),
		},
		WitnessSet: gdijkstra.DijkstraTransactionWitnessSet{
			WsPlutusV3Scripts: cbor.NewSetType(
				[]lcommon.PlutusV3Script{v3Script},
				false,
			),
			WsPlutusData: cbor.NewSetType([]lcommon.Datum{datum}, false),
			WsRedeemers: gdijkstra.DijkstraRedeemers{
				Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
					{Tag: lcommon.RedeemerTagSpend, Index: 0}: {
						Data:    datum,
						ExUnits: lcommon.ExUnits{Steps: 1_000_000, Memory: 1_000_000},
					},
				},
			},
		},
		TxIsValid: true,
	}
	params := &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
			MaxTxSize:            16_384,
			MaxValueSize:         5_000,
			MaxTxExUnits:         lcommon.ExUnits{Steps: 1_000_000, Memory: 1_000_000},
			CollateralPercentage: 150,
			MaxCollateralInputs:  3,
			ExecutionCosts: lcommon.ExUnitPrice{
				MemPrice:  &cbor.Rat{Rat: big.NewRat(1, 1)},
				StepPrice: &cbor.Rat{Rat: big.NewRat(1, 1)},
			},
			CostModels: map[uint][]int64{
				0: defaultMachineCostModel(t, lang.LanguageVersionV1),
				1: defaultMachineCostModel(t, lang.LanguageVersionV2),
				2: defaultMachineCostModel(t, lang.LanguageVersionV3),
			},
		},
		MaxRefScriptSizePerTx: 100_000,
	}
	redeemersCbor, err := cbor.Encode(tx.WitnessSet.WsRedeemers.Redeemers)
	require.NoError(t, err)
	tx.WitnessSet.WsRedeemers.SetCbor(redeemersCbor)
	datumsCbor, err := cbor.Encode(tx.WitnessSet.WsPlutusData.Items())
	require.NoError(t, err)
	tx.WitnessSet.WsPlutusData.SetCbor(datumsCbor)
	langViewsCbor, err := lcommon.EncodeLangViews(
		map[uint]struct{}{2: {}},
		params.CostModels,
	)
	require.NoError(t, err)
	scriptData := append(redeemersCbor, datumsCbor...)
	scriptData = append(scriptData, langViewsCbor...)
	scriptDataHash := lcommon.Blake2b256Hash(scriptData)
	tx.Body.TxScriptDataHash = &scriptDataHash
	bodyCbor, err := cbor.Encode(tx.Body)
	require.NoError(t, err)
	tx.Body.SetCbor(bodyCbor)
	txHash := tx.Hash()
	tx.WitnessSet.VkeyWitnesses = cbor.NewSetType(
		[]lcommon.VkeyWitness{{
			Vkey:      publicKey,
			Signature: ed25519.Sign(privateKey, txHash[:]),
		}},
		false,
	)

	err = ValidateTxDijkstra(tx, 0, state, params)
	require.ErrorContains(t, err, "is also a regular input")
}

type referenceOverlapScriptOutput struct {
	testAddressScriptOutput
	datumHash *lcommon.Blake2b256
}

func (o referenceOverlapScriptOutput) DatumHash() *lcommon.Blake2b256 {
	return o.datumHash
}

func newConwayReferenceOverlapScriptTx(
	t *testing.T,
	plutusScript lcommon.Script,
	valid bool,
) (*conway.ConwayTransaction, *mockLedgerState, *conway.ConwayProtocolParameters) {
	t.Helper()
	input := shelley.NewShelleyTransactionInput(
		"f228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee88",
		0,
	)
	collateralInput := shelley.NewShelleyTransactionInput(
		"f328b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee99",
		0,
	)
	datum, datumHash := referenceOverlapDatum(t)
	state := newMockLedgerState()
	state.networkId = uint(lcommon.AddressNetworkTestnet)
	state.addUtxo(input, referenceOverlapScriptOutput{
		testAddressScriptOutput: testAddressScriptOutput{
			testOutput: newTestOutput(10_000_000),
			addr:       newTestScriptAddress(t, plutusScript),
		},
		datumHash: &datumHash,
	})
	state.addUtxo(collateralInput, testAddressOutput{
		testOutput: newTestOutput(2_000_000),
		addr:       newTestKeyAddress(t),
	})
	witnesses := conway.ConwayTransactionWitnessSet{
		WsPlutusData: cbor.NewSetType([]lcommon.Datum{datum}, false),
		WsRedeemers: conway.ConwayRedeemers{
			Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
				{Tag: lcommon.RedeemerTagSpend, Index: 0}: {
					Data:    datum,
					ExUnits: lcommon.ExUnits{Steps: 1_000_000, Memory: 1_000_000},
				},
			},
		},
	}
	switch script := plutusScript.(type) {
	case lcommon.PlutusV1Script:
		witnesses.WsPlutusV1Scripts = cbor.NewSetType([]lcommon.PlutusV1Script{script}, false)
	case lcommon.PlutusV2Script:
		witnesses.WsPlutusV2Scripts = cbor.NewSetType([]lcommon.PlutusV2Script{script}, false)
	case lcommon.PlutusV3Script:
		witnesses.WsPlutusV3Scripts = cbor.NewSetType([]lcommon.PlutusV3Script{script}, false)
	default:
		t.Fatalf("unexpected Plutus script type %T", plutusScript)
	}
	tx := &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{input},
			),
			TxOutputs: []babbage.BabbageTransactionOutput{{
				OutputAddress: newTestKeyAddress(t),
				OutputAmount:  mary.MaryTransactionOutputValue{Amount: 9_000_000},
			}},
			TxFee: 1_000_000,
			TxCollateral: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{collateralInput},
				false,
			),
			TxTotalCollateral: 2_000_000,
			TxReferenceInputs: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{input},
				false,
			),
		},
		WitnessSet: witnesses,
		TxIsValid:  valid,
	}
	params := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: lcommon.ProtocolVersionVanRossem,
		},
		MaxTxSize:            16_384,
		MaxValueSize:         5_000,
		MaxTxExUnits:         lcommon.ExUnits{Steps: 1_000_000, Memory: 1_000_000},
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
		CostModels: map[uint][]int64{
			0: defaultMachineCostModel(t, lang.LanguageVersionV1),
			1: defaultMachineCostModel(t, lang.LanguageVersionV2),
			2: defaultMachineCostModel(t, lang.LanguageVersionV3),
		},
	}
	return tx, state, params
}

func referenceOverlapDatum(t *testing.T) (lcommon.Datum, lcommon.Blake2b256) {
	t.Helper()
	datumBytes, err := data.Encode(data.NewInteger(big.NewInt(42)))
	require.NoError(t, err)
	var datum lcommon.Datum
	_, err = cbor.Decode(datumBytes, &datum)
	require.NoError(t, err)
	return datum, datum.Hash()
}

func referenceOverlapFailingScriptBytes(
	t *testing.T,
	version lang.LanguageVersion,
) []byte {
	t.Helper()
	program := &syn.Program[syn.DeBruijn]{
		Version: version,
		Term: &syn.Lambda[syn.DeBruijn]{
			Body: &syn.Lambda[syn.DeBruijn]{
				Body: &syn.Lambda[syn.DeBruijn]{
					Body: &syn.Error{},
				},
			},
		},
	}
	flatProgram, err := syn.Encode(program)
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flatProgram)
	require.NoError(t, err)
	return scriptBytes
}
