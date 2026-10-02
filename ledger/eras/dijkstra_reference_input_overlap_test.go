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
	"github.com/blinklabs-io/gouroboros/ledger/common/script"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/require"
)

func TestValidateTxDijkstraAllowsSpendReferenceOverlapWithoutPlutus(t *testing.T) {
	t.Parallel()
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

func TestValidateTxDijkstraAllowsSubtransactionSpendReferenceOverlapWithoutPlutus(
	t *testing.T,
) {
	t.Parallel()
	input := shelley.NewShelleyTransactionInput(
		"c228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee55",
		0,
	)
	const balance = uint64(10_000_000)
	tx := &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{
					shelley.NewShelleyTransactionInput(
						"f228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee88",
						0,
					),
				},
			),
			TxOutputs: []gdijkstra.DijkstraTransactionOutput{{
				Output: babbage.BabbageTransactionOutput{
					OutputAddress: newTestKeyAddress(t),
					OutputAmount:  mary.MaryTransactionOutputValue{Amount: balance},
				},
			}},
			TxSubTransactions: cbor.NewSetType(
				[]gdijkstra.DijkstraSubTransaction{{
					Body: gdijkstra.DijkstraSubTransactionBody{
						TxInputs: conway.NewConwayTransactionInputSet(
							[]shelley.ShelleyTransactionInput{input},
						),
						TxOutputs: []gdijkstra.DijkstraTransactionOutput{{
							Output: babbage.BabbageTransactionOutput{
								OutputAddress: newTestKeyAddress(t),
								OutputAmount:  mary.MaryTransactionOutputValue{Amount: balance},
							},
						}},
						TxReferenceInputs: cbor.NewSetType(
							[]shelley.ShelleyTransactionInput{input},
							false,
						),
					},
				}},
				false,
			),
		},
		TxIsValid: true,
	}
	state := newMockLedgerState()
	state.addUtxo(
		tx.Body.TxInputs.Items()[0],
		newTestOutput(balance),
	)
	state.addUtxo(input, newTestOutput(balance))
	protocolParams := &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MaxTxSize:    16_384,
			MaxValueSize: 5_000,
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
		},
	}

	require.NoError(t, ValidateTxDijkstra(tx, 0, state, protocolParams))
}

func newDijkstraBatchChildControlFixture(
	t *testing.T,
) (*gdijkstra.DijkstraTransaction, *mockLedgerState, *gdijkstra.DijkstraProtocolParameters, shelley.ShelleyTransactionInput) {
	t.Helper()
	const balance = uint64(10_000_000)
	topInput := shelley.NewShelleyTransactionInput(
		"a228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee11",
		0,
	)
	childInput := shelley.NewShelleyTransactionInput(
		"b228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee22",
		0,
	)
	output := gdijkstra.DijkstraTransactionOutput{
		Output: babbage.BabbageTransactionOutput{
			OutputAddress: newTestKeyAddress(t),
			OutputAmount:  mary.MaryTransactionOutputValue{Amount: balance},
		},
	}
	firstChild := gdijkstra.DijkstraSubTransaction{
		Body: gdijkstra.DijkstraSubTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{childInput},
			),
			TxOutputs: []gdijkstra.DijkstraTransactionOutput{output},
		},
	}
	tx := &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{topInput},
			),
			TxOutputs: []gdijkstra.DijkstraTransactionOutput{output},
			TxSubTransactions: cbor.NewSetType(
				[]gdijkstra.DijkstraSubTransaction{firstChild},
				true,
			),
		},
		TxIsValid: true,
	}
	state := newMockLedgerState()
	state.addUtxo(topInput, newTestOutput(balance))
	state.addUtxo(childInput, newTestOutput(balance))
	protocolParams := &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MaxTxSize:    16_384,
			MaxValueSize: 5_000,
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
		},
	}
	return tx, state, protocolParams, childInput
}

func TestValidateTxDijkstraAllowsSiblingSpendAndReferenceOfOriginalInput(
	t *testing.T,
) {
	t.Parallel()
	tx, state, protocolParams, childInput := newDijkstraBatchChildControlFixture(t)
	subTransactions := tx.Body.TxSubTransactions.Items()
	require.Len(t, subTransactions, 1)
	secondInput := shelley.NewShelleyTransactionInput(
		"c228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee33",
		0,
	)
	state.addUtxo(secondInput, newTestOutput(10_000_000))
	subTransactions = append(subTransactions, gdijkstra.DijkstraSubTransaction{
		Body: gdijkstra.DijkstraSubTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{secondInput},
			),
			TxOutputs: []gdijkstra.DijkstraTransactionOutput{{
				Output: babbage.BabbageTransactionOutput{
					OutputAddress: newTestKeyAddress(t),
					OutputAmount:  mary.MaryTransactionOutputValue{Amount: 10_000_000},
				},
			}},
			TxReferenceInputs: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{childInput},
				false,
			),
		},
	})
	tx.Body.TxSubTransactions = cbor.NewSetType(subTransactions, true)

	require.NoError(t, ValidateTxDijkstra(tx, 0, state, protocolParams))
}

func TestValidateTxDijkstraRejectsChildOutputChaining(t *testing.T) {
	t.Parallel()
	t.Run("spend", func(t *testing.T) {
		tx, state, protocolParams, _ := newDijkstraBatchChildControlFixture(t)
		firstChild := tx.Body.TxSubTransactions.Items()[0]
		firstBodyCbor, err := cbor.Encode(firstChild.Body)
		require.NoError(t, err)
		firstChild.Body.SetCbor(firstBodyCbor)
		producedInput := shelley.ShelleyTransactionInput{
			TxId:        firstChild.Body.Id(),
			OutputIndex: 0,
		}
		secondChild := gdijkstra.DijkstraSubTransaction{
			Body: gdijkstra.DijkstraSubTransactionBody{
				TxInputs: conway.NewConwayTransactionInputSet(
					[]shelley.ShelleyTransactionInput{producedInput},
				),
				TxOutputs: firstChild.Body.TxOutputs,
			},
		}
		tx.Body.TxSubTransactions = cbor.NewSetType(
			[]gdijkstra.DijkstraSubTransaction{firstChild, secondChild},
			true,
		)

		err = ValidateTxDijkstra(tx, 0, state, protocolParams)
		var badInputs shelley.BadInputsUtxoError
		require.ErrorAs(t, err, &badInputs)
	})
	t.Run("reference", func(t *testing.T) {
		tx, state, protocolParams, _ := newDijkstraBatchChildControlFixture(t)
		firstChild := tx.Body.TxSubTransactions.Items()[0]
		firstBodyCbor, err := cbor.Encode(firstChild.Body)
		require.NoError(t, err)
		firstChild.Body.SetCbor(firstBodyCbor)
		producedInput := shelley.ShelleyTransactionInput{
			TxId:        firstChild.Body.Id(),
			OutputIndex: 0,
		}
		secondInput := shelley.NewShelleyTransactionInput(
			"d228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee44",
			0,
		)
		state.addUtxo(secondInput, newTestOutput(10_000_000))
		secondChild := gdijkstra.DijkstraSubTransaction{
			Body: gdijkstra.DijkstraSubTransactionBody{
				TxInputs: conway.NewConwayTransactionInputSet(
					[]shelley.ShelleyTransactionInput{secondInput},
				),
				TxOutputs: []gdijkstra.DijkstraTransactionOutput{{
					Output: babbage.BabbageTransactionOutput{
						OutputAddress: newTestKeyAddress(t),
						OutputAmount:  mary.MaryTransactionOutputValue{Amount: 10_000_000},
					},
				}},
				TxReferenceInputs: cbor.NewSetType(
					[]shelley.ShelleyTransactionInput{producedInput},
					false,
				),
			},
		}
		tx.Body.TxSubTransactions = cbor.NewSetType(
			[]gdijkstra.DijkstraSubTransaction{firstChild, secondChild},
			true,
		)

		err = ValidateTxDijkstra(tx, 0, state, protocolParams)
		var referenceErr lcommon.ReferenceInputResolutionError
		require.ErrorAs(t, err, &referenceErr)
	})
}

func TestValidateTxDijkstraRejectsDoubleSpendAcrossValidChildren(t *testing.T) {
	t.Parallel()
	tx, state, protocolParams, childInput := newDijkstraBatchChildControlFixture(t)
	firstChild := tx.Body.TxSubTransactions.Items()[0]
	secondBody := gdijkstra.DijkstraSubTransactionBody{
		TxInputs: conway.NewConwayTransactionInputSet(
			[]shelley.ShelleyTransactionInput{childInput},
		),
		TxOutputs: firstChild.Body.TxOutputs,
		Ttl:       1,
	}
	tx.Body.TxSubTransactions = cbor.NewSetType(
		[]gdijkstra.DijkstraSubTransaction{
			firstChild,
			{Body: secondBody},
		},
		true,
	)
	tx.TxIsValid = true

	err := ValidateTxDijkstra(tx, 0, state, protocolParams)
	var duplicateErr shelley.DuplicateInputError
	require.ErrorAs(t, err, &duplicateErr)
	require.Equal(t, "regular", duplicateErr.InputType)
}

func TestValidateTxDijkstraAllowsSpendReferenceOverlapForPlutusV1AndV2(
	t *testing.T,
) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		version lang.LanguageVersion
	}{
		{name: "PlutusV1", version: lang.LanguageVersionV1},
		{name: "PlutusV2", version: lang.LanguageVersionV2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			plutusScript := dijkstraReferenceOverlapScript(t, tc.version)
			tx, state, params := newDijkstraReferenceOverlapScriptTx(
				t,
				plutusScript,
				false,
			)
			// The deliberately failing script must match the invalid flag. A
			// context overlap error is not a successful phase-2 failure.
			require.NoError(t, ValidateTxDijkstra(tx, 0, state, params))
		})
	}
}

func TestValidateTxDijkstraRejectsSpendReferenceOverlapForPlutusV3(
	t *testing.T,
) {
	t.Parallel()
	plutusScript := dijkstraReferenceOverlapScript(
		t,
		lang.LanguageVersionV3,
	)
	tx, state, params := newDijkstraReferenceOverlapScriptTx(
		t,
		plutusScript,
		true,
	)
	require.Len(t, tx.Inputs(), 1)
	require.Len(t, tx.ReferenceInputs(), 1)
	require.Equal(t, tx.Inputs()[0].String(), tx.ReferenceInputs()[0].String())
	require.ErrorContains(
		t,
		script.ValidatePlutusV3ReferenceInputs(
			tx,
			params.ProtocolVersion.Major,
		),
		"is also a regular input",
	)
	err := gdijkstra.UtxoValidatePlutusScripts(tx, 0, state, params)
	require.ErrorContains(t, err, "is also a regular input")
	require.ErrorContains(t, ValidateTxDijkstra(tx, 0, state, params), "is also a regular input")
}

func TestDijkstraPlutusValidationRejectsPlutusV3ChildWithOverlap(
	t *testing.T,
) {
	t.Parallel()
	plutusScript := dijkstraReferenceOverlapScript(
		t,
		lang.LanguageVersionV3,
	)
	tx, state, params := newDijkstraReferenceOverlapScriptTx(
		t,
		plutusScript,
		true,
	)

	// gOuroboros v0.208.0 rejects Plutus V1-V3 execution in Dijkstra child
	// transactions before constructing a script context. The overlap must not
	// mask that earlier, era-specific rule.
	child := gdijkstra.DijkstraSubTransaction{
		Body: gdijkstra.DijkstraSubTransactionBody{
			TxInputs:          tx.Body.TxInputs,
			TxOutputs:         tx.Body.TxOutputs,
			TxScriptDataHash:  tx.Body.TxScriptDataHash,
			TxReferenceInputs: tx.Body.TxReferenceInputs,
		},
		WitnessSet: tx.WitnessSet,
	}
	tx = &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxSubTransactions: cbor.NewSetType(
				[]gdijkstra.DijkstraSubTransaction{child},
				false,
			),
		},
		TxIsValid: true,
	}

	err := gdijkstra.UtxoValidatePlutusScripts(tx, 0, state, params)
	var unsupported gdijkstra.UnsupportedScriptInSubtransactionError
	require.ErrorAs(t, err, &unsupported)
	require.Equal(t, uint(2), unsupported.Version)
}

func TestDijkstraPlutusValidationV3OverlapIgnoresUnusedPlutusV1AndV2Scripts(
	t *testing.T,
) {
	t.Parallel()
	plutusV3Script := dijkstraReferenceOverlapScript(
		t,
		lang.LanguageVersionV3,
	)
	tx, state, params := newDijkstraReferenceOverlapScriptTx(
		t,
		plutusV3Script,
		true,
	)
	unusedV1Script := dijkstraReferenceOverlapScript(
		t,
		lang.LanguageVersionV1,
	).(lcommon.PlutusV1Script)
	unusedV2Script := dijkstraReferenceOverlapScript(
		t,
		lang.LanguageVersionV2,
	).(lcommon.PlutusV2Script)
	tx.WitnessSet.WsPlutusV1Scripts = cbor.NewSetType(
		[]lcommon.PlutusV1Script{unusedV1Script},
		false,
	)
	tx.WitnessSet.WsPlutusV2Scripts = cbor.NewSetType(
		[]lcommon.PlutusV2Script{unusedV2Script},
		false,
	)
	unusedV1Input := shelley.NewShelleyTransactionInput(
		"1128b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee88",
		0,
	)
	unusedV2Input := shelley.NewShelleyTransactionInput(
		"1228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee88",
		0,
	)
	state.addUtxo(unusedV1Input, testAddressScriptOutput{
		testOutput: newTestOutput(1_000_000),
		addr:       newTestKeyAddress(t),
		scriptRef:  unusedV1Script,
	})
	state.addUtxo(unusedV2Input, testAddressScriptOutput{
		testOutput: newTestOutput(1_000_000),
		addr:       newTestKeyAddress(t),
		scriptRef:  unusedV2Script,
	})
	tx.Body.TxReferenceInputs = cbor.NewSetType(
		[]shelley.ShelleyTransactionInput{
			tx.Body.TxInputs.Items()[0],
			unusedV1Input,
			unusedV2Input,
		},
		false,
	)
	refreshDijkstraTransactionBodyAndSignature(t, tx)
	fullValidationErr := ValidateTxDijkstra(tx, 0, state, params)
	var extraneousErr lcommon.ExtraneousScriptWitnessesError
	require.ErrorAs(t, fullValidationErr, &extraneousErr)

	// Dijkstra phase one rejects unused script witnesses. Invoke the phase-2
	// evaluator directly to isolate whether unused available reference scripts
	// change the V3 context's executed-language rule.
	err := script.ValidatePlutusV3ReferenceInputs(
		tx,
		params.ProtocolVersion.Major,
	)
	require.ErrorContains(t, err, "is also a regular input")
	err = gdijkstra.UtxoValidatePlutusScripts(tx, 0, state, params)
	var contextErr conway.ScriptContextConstructionError
	require.ErrorAs(t, err, &contextErr)
	require.ErrorContains(t, err, "is also a regular input")
}

func TestValidateTxDijkstraRejectsSpendReferenceOverlapForPlutusV4(
	t *testing.T,
) {
	t.Parallel()
	plutusScript := dijkstraReferenceOverlapScript(
		t,
		lang.LanguageVersionV4,
	)
	tx, state, params := newDijkstraReferenceOverlapScriptTx(
		t,
		plutusScript,
		true,
	)

	err := ValidateTxDijkstra(tx, 0, state, params)
	var contextErr conway.ScriptContextConstructionError
	require.ErrorAs(t, err, &contextErr)
	require.ErrorContains(t, err, "plutus V4 reference input")
	require.ErrorContains(t, err, "is also a regular input")
}

func TestDijkstraPlutusV4OverlapRequiresLedgerState(t *testing.T) {
	t.Parallel()
	plutusScript := dijkstraReferenceOverlapScript(
		t,
		lang.LanguageVersionV4,
	)
	tx, _, _ := newDijkstraReferenceOverlapScriptTx(
		t,
		plutusScript,
		true,
	)
	err := validateDijkstraPlutusV4ReferenceInputOverlap(tx, nil)
	require.ErrorContains(
		t,
		err,
		"ledger state is required for Dijkstra script validation",
	)
}

func TestValidateTxDijkstraIgnoresUnusedPlutusV4ReferenceScriptForSpendReferenceOverlap(
	t *testing.T,
) {
	t.Parallel()
	plutusScript := dijkstraReferenceOverlapScript(
		t,
		lang.LanguageVersionV1,
	)
	tx, state, params := newDijkstraReferenceOverlapScriptTx(
		t,
		plutusScript,
		false,
	)
	v4Script := dijkstraReferenceOverlapScript(
		t,
		lang.LanguageVersionV4,
	).(lcommon.PlutusV4Script)
	unusedV4Input := shelley.NewShelleyTransactionInput(
		"f228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee88",
		0,
	)
	state.addUtxo(unusedV4Input, testAddressScriptOutput{
		testOutput: newTestOutput(1_000_000),
		addr:       newTestKeyAddress(t),
		scriptRef:  v4Script,
	})
	tx.Body.TxReferenceInputs = cbor.NewSetType(
		[]shelley.ShelleyTransactionInput{
			tx.Body.TxInputs.Items()[0],
			unusedV4Input,
		},
		false,
	)
	refreshDijkstraTransactionBodyAndSignature(t, tx)

	require.NoError(t, ValidateTxDijkstra(tx, 0, state, params))
}

func TestValidateDijkstraPlutusV4ReferenceOverlapChecksSubtransactions(
	t *testing.T,
) {
	t.Parallel()
	v4Script := dijkstraReferenceOverlapScript(
		t,
		lang.LanguageVersionV4,
	).(lcommon.PlutusV4Script)
	input := shelley.NewShelleyTransactionInput(
		"d228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee66",
		0,
	)
	datum, datumHash := referenceOverlapDatum(t)
	state := newMockLedgerState()
	state.networkId = uint(lcommon.AddressNetworkTestnet)
	state.addUtxo(input, referenceOverlapScriptOutput{
		testAddressScriptOutput: testAddressScriptOutput{
			testOutput: newTestOutput(10_000_000),
			addr:       newTestScriptAddress(t, v4Script),
			scriptRef:  v4Script,
		},
		datumHash: &datumHash,
	})
	subTransaction := gdijkstra.DijkstraSubTransaction{
		Body: gdijkstra.DijkstraSubTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{input},
			),
			TxReferenceInputs: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{input},
				false,
			),
		},
		WitnessSet: gdijkstra.DijkstraTransactionWitnessSet{
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
	}
	tx := &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxSubTransactions: cbor.NewSetType(
				[]gdijkstra.DijkstraSubTransaction{subTransaction},
				false,
			),
		},
	}

	err := validateDijkstraPlutusV4ReferenceInputOverlap(tx, state)
	var contextErr conway.ScriptContextConstructionError
	require.ErrorAs(t, err, &contextErr)
	require.ErrorContains(t, err, "plutus V4 reference input")
}

type referenceOverlapScriptOutput struct {
	testAddressScriptOutput
	datumHash *lcommon.Blake2b256
}

func (o referenceOverlapScriptOutput) DatumHash() *lcommon.Blake2b256 {
	return o.datumHash
}

func dijkstraReferenceOverlapScript(
	t *testing.T,
	version lang.LanguageVersion,
) lcommon.Script {
	t.Helper()
	scriptBytes := referenceOverlapFailingScriptBytes(t, version)
	switch version {
	case lang.LanguageVersionV1:
		return lcommon.PlutusV1Script(scriptBytes)
	case lang.LanguageVersionV2:
		return lcommon.PlutusV2Script(scriptBytes)
	case lang.LanguageVersionV3:
		return lcommon.PlutusV3Script(scriptBytes)
	case lang.LanguageVersionV4:
		return lcommon.PlutusV4Script(scriptBytes)
	default:
		t.Fatalf("unsupported overlap test language version %v", version)
		return nil
	}
}

func newDijkstraReferenceOverlapScriptTx(
	t *testing.T,
	plutusScript lcommon.Script,
	valid bool,
) (*gdijkstra.DijkstraTransaction, *mockLedgerState, *gdijkstra.DijkstraProtocolParameters) {
	t.Helper()
	input := shelley.NewShelleyTransactionInput(
		"d228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee66",
		0,
	)
	collateralInput := shelley.NewShelleyTransactionInput(
		"e228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee77",
		0,
	)
	// Keep the input IDs valid, fixed-length hashes. The script/context behavior
	// under test does not depend on their values.
	datum, datumHash := referenceOverlapDatum(t)
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
	var referenceScript lcommon.Script
	if v4Script, ok := plutusScript.(lcommon.PlutusV4Script); ok {
		referenceScript = v4Script
	}
	state.addUtxo(input, referenceOverlapScriptOutput{
		testAddressScriptOutput: testAddressScriptOutput{
			testOutput: newTestOutput(10_000_000),
			addr:       newTestScriptAddress(t, plutusScript),
			scriptRef:  referenceScript,
		},
		datumHash: &datumHash,
	})
	state.addUtxo(collateralInput, testAddressOutput{
		testOutput: newTestOutput(3_100_000),
		addr:       collateralAddress,
	})
	witnessSet := gdijkstra.DijkstraTransactionWitnessSet{
		WsPlutusData: cbor.NewSetType([]lcommon.Datum{datum}, false),
		WsRedeemers: gdijkstra.DijkstraRedeemers{
			Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
				{Tag: lcommon.RedeemerTagSpend, Index: 0}: {
					Data:    datum,
					ExUnits: lcommon.ExUnits{Steps: 1_000_000, Memory: 1_000_000},
				},
			},
		},
	}
	var languageID uint
	switch script := plutusScript.(type) {
	case lcommon.PlutusV1Script:
		languageID = 0
		witnessSet.WsPlutusV1Scripts = cbor.NewSetType(
			[]lcommon.PlutusV1Script{script},
			false,
		)
	case lcommon.PlutusV2Script:
		languageID = 1
		witnessSet.WsPlutusV2Scripts = cbor.NewSetType(
			[]lcommon.PlutusV2Script{script},
			false,
		)
	case lcommon.PlutusV3Script:
		languageID = 2
		witnessSet.WsPlutusV3Scripts = cbor.NewSetType(
			[]lcommon.PlutusV3Script{script},
			false,
		)
	case lcommon.PlutusV4Script:
		languageID = 3
	default:
		t.Fatalf("unsupported overlap test script %T", plutusScript)
	}
	tx := &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{input},
			),
			TxOutputs: []gdijkstra.DijkstraTransactionOutput{{
				Output: babbage.BabbageTransactionOutput{
					OutputAddress: newTestKeyAddress(t),
					OutputAmount:  mary.MaryTransactionOutputValue{Amount: 7_999_000},
				},
			}},
			TxFee: 2_001_000,
			TxCollateral: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{collateralInput},
				false,
			),
			TxTotalCollateral: 3_100_000,
			TxReferenceInputs: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{input},
				false,
			),
		},
		WitnessSet: witnessSet,
		TxIsValid:  valid,
	}
	params := &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MinFeeRefScriptCostPerByte: &cbor.Rat{Rat: big.NewRat(1, 1)},
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
				3: defaultMachineCostModel(t, lang.LanguageVersionV4),
			},
		},
		MaxRefScriptSizePerTx: 100_000,
		RefScriptCostStride:   25_600,
		RefScriptCostMultiplier: &cbor.Rat{
			Rat: big.NewRat(6, 5),
		},
	}
	redeemersCbor, err := cbor.Encode(tx.WitnessSet.WsRedeemers.Redeemers)
	require.NoError(t, err)
	tx.WitnessSet.WsRedeemers.SetCbor(redeemersCbor)
	datumsCbor, err := cbor.Encode(tx.WitnessSet.WsPlutusData.Items())
	require.NoError(t, err)
	tx.WitnessSet.WsPlutusData.SetCbor(datumsCbor)
	langViewsCbor, err := lcommon.EncodeLangViews(
		map[uint]struct{}{languageID: {}},
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

	return tx, state, params
}

func refreshDijkstraTransactionBodyAndSignature(
	t *testing.T,
	tx *gdijkstra.DijkstraTransaction,
) {
	t.Helper()
	tx.Body.SetCbor(nil)
	bodyCbor, err := cbor.Encode(tx.Body)
	require.NoError(t, err)
	tx.Body.SetCbor(bodyCbor)
	seed := make([]byte, ed25519.SeedSize)
	seed[0] = 0x77
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	txHash := tx.Hash()
	tx.WitnessSet.VkeyWitnesses = cbor.NewSetType(
		[]lcommon.VkeyWitness{{
			Vkey:      publicKey,
			Signature: ed25519.Sign(privateKey, txHash[:]),
		}},
		false,
	)
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
	uplcVersion := version
	if version == lang.LanguageVersionV3 || version == lang.LanguageVersionV4 {
		uplcVersion = [3]uint32{1, 1, 0}
	}
	var body syn.Term[syn.DeBruijn] = &syn.Error{}
	argumentCount := 3
	if version == lang.LanguageVersionV4 {
		argumentCount = 1
	}
	for range argumentCount {
		body = &syn.Lambda[syn.DeBruijn]{Body: body}
	}
	program := &syn.Program[syn.DeBruijn]{
		Version: uplcVersion,
		Term:    body,
	}
	flatProgram, err := syn.Encode(program)
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flatProgram)
	require.NoError(t, err)
	return scriptBytes
}
