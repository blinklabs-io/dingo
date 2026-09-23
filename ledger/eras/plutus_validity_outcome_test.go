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
	"encoding/json"
	"errors"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/require"
)

// uplcProgramVersion110 is UPLC program version 1.1.0, the version Plutus V3
// and V4 scripts are encoded with.
var uplcProgramVersion110 = lang.LanguageVersion{1, 1, 0}

type declaredValidityConwayTx struct {
	*mockConwayFeeTx
	valid bool
}

func newDijkstraGuardingValidityOutcomeTx(
	t *testing.T,
	valid bool,
	version lang.LanguageVersion,
	scriptFails bool,
	exUnits lcommon.ExUnits,
) *gdijkstra.DijkstraTransaction {
	t.Helper()
	program := &syn.Program[syn.DeBruijn]{
		// The UPLC program version is not the ledger language version. Plutus
		// V3 and V4 scripts carry UPLC 1.1.0; lang.LanguageVersionV3/V4
		// ({1,2,0} and {1,3,0}) select the cost model and are not valid
		// program versions.
		Version: uplcProgramVersion110,
		Term: &syn.Lambda[syn.DeBruijn]{
			Body: &syn.Constant{Con: &syn.Unit{}},
		},
	}
	flatProgram, err := syn.Encode(program)
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flatProgram)
	require.NoError(t, err)
	if scriptFails {
		// The deliberately malformed Flat payload reaches the concrete Dijkstra
		// guarding evaluator and is reported as PlutusScriptFailedError.
		scriptBytes = []byte{0x41, 0x00}
	}

	var script lcommon.Script
	var subTxWitnesses gdijkstra.DijkstraTransactionWitnessSet
	switch version {
	case lang.LanguageVersionV3:
		plutusScript := lcommon.PlutusV3Script(scriptBytes)
		script = plutusScript
		subTxWitnesses.WsPlutusV3Scripts = cbor.NewSetType(
			[]lcommon.PlutusV3Script{plutusScript},
			false,
		)
	case lang.LanguageVersionV4:
		plutusScript := lcommon.PlutusV4Script(scriptBytes)
		script = plutusScript
		subTxWitnesses.WsPlutusV4Scripts = cbor.NewSetType(
			[]lcommon.PlutusV4Script{plutusScript},
			false,
		)
	default:
		t.Fatalf("unsupported guarding script version %v", version)
	}

	return &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxGuards: &gdijkstra.DijkstraGuards{
				Credentials: []lcommon.Credential{{
					CredType:   lcommon.CredentialTypeScriptHash,
					Credential: script.Hash(),
				}},
			},
			TxSubTransactions: cbor.NewSetType(
				[]gdijkstra.DijkstraSubTransaction{{
					WitnessSet: subTxWitnesses,
				}},
				false,
			),
		},
		WitnessSet: gdijkstra.DijkstraTransactionWitnessSet{
			WsRedeemers: gdijkstra.DijkstraRedeemers{
				Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
					{Tag: lcommon.RedeemerTagGuarding, Index: 0}: {
						ExUnits: exUnits,
					},
				},
			},
		},
		TxIsValid: valid,
	}
}

// dijkstraValidityOutcomeV4CostModelPad covers the 7 PlutusV4 cost-model
// parameters (multiIndexArray, assetCount) that plutigo's CostModelParamNamesV4
// appends after PlutusV3's own 350 names, in the same order. No script in this
// file calls either builtin, so these placeholders never affect a measured
// cost; they only have to be present and finite. See
// plutigo lang.CostModelParamNamesV4.
var dijkstraValidityOutcomeV4CostModelPad = []int64{
	100,
	100,
	100,
	100,
	100,
	100,
	100,
}

// dijkstraValidityOutcomePParams builds real, non-empty cost models for every
// Plutus language version invoked by this file's tests. plutigo v0.7.2's
// costModelFromList costs any parameter beyond the supplied list at
// math.MaxInt64 rather than leaving it at a zero-valued default (matching
// upstream's real "an uncosted parameter cannot run within any budget"
// semantics, see plutigo#415's cek/cost_model.go change) -- an empty
// CostModels map, as this fixture carried before, therefore made every CEK
// machine step cost MaxInt64 and exhausted any real budget on the first step,
// independent of the trivial guarding script's own true cost. PlutusV3 (index
// 2) reuses the real Preview epoch-672 cost model; PlutusV4 (index 3) reuses
// the same list, since PlutusV4's own names are PlutusV3's 350 plus the 7
// multiIndexArray/assetCount entries above.
func dijkstraValidityOutcomePParams(
	t *testing.T,
) *gdijkstra.DijkstraProtocolParameters {
	t.Helper()
	var costModels struct {
		PlutusV1 []int64 `json:"PlutusV1"`
		PlutusV2 []int64 `json:"PlutusV2"`
		PlutusV3 []int64 `json:"PlutusV3"`
	}
	require.NoError(t, json.Unmarshal(
		readErasFixture(t, previewConwayCostModels),
		&costModels,
	))
	v4CostModel := append(
		append([]int64{}, costModels.PlutusV3...),
		dijkstraValidityOutcomeV4CostModelPad...,
	)
	return &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
			CostModels: map[uint][]int64{
				0: costModels.PlutusV1,
				1: costModels.PlutusV2,
				2: costModels.PlutusV3,
				3: v4CostModel,
			},
		},
	}
}

func TestValidateTxDijkstraRequiresDeclaredValidityToMatchGuardingExecution(
	t *testing.T,
) {
	originalPhase1 := dijkstraPhase1UtxoValidationRules
	dijkstraPhase1UtxoValidationRules = nil
	t.Cleanup(func() { dijkstraPhase1UtxoValidationRules = originalPhase1 })

	for _, scriptVersion := range []struct {
		name    string
		version lang.LanguageVersion
	}{
		{name: "Plutus V3", version: lang.LanguageVersionV3},
		{name: "Plutus V4", version: lang.LanguageVersionV4},
	} {
		t.Run(scriptVersion.name, func(t *testing.T) {
			for _, outcome := range []struct {
				name          string
				declaredValid bool
				scriptFails   bool
				exUnits       lcommon.ExUnits
				assert        func(*testing.T, error)
			}{
				{
					name:          "declared valid and script passes",
					declaredValid: true,
					scriptFails:   false,
					exUnits: lcommon.ExUnits{
						Steps: 10_000_000, Memory: 10_000_000,
					},
					assert: func(t *testing.T, err error) { require.NoError(t, err) },
				},
				{
					name:          "declared invalid and script fails",
					declaredValid: false,
					scriptFails:   true,
					exUnits:       lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
					assert:        func(t *testing.T, err error) { require.NoError(t, err) },
				},
				{
					name:          "declared valid and script fails",
					declaredValid: true,
					scriptFails:   true,
					exUnits:       lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
					assert: func(t *testing.T, err error) {
						var scriptErr conway.PlutusScriptFailedError
						require.ErrorAs(t, err, &scriptErr)
					},
				},
				{
					name:          "declared invalid and script passes",
					declaredValid: false,
					scriptFails:   false,
					exUnits: lcommon.ExUnits{
						Steps: 10_000_000, Memory: 10_000_000,
					},
					assert: func(t *testing.T, err error) {
						require.ErrorContains(
							t,
							err,
							"declared invalid but Plutus scripts succeeded",
						)
					},
				},
			} {
				t.Run(outcome.name, func(t *testing.T) {
					tx := newDijkstraGuardingValidityOutcomeTx(
						t,
						outcome.declaredValid,
						scriptVersion.version,
						outcome.scriptFails,
						outcome.exUnits,
					)
					err := ValidateTxDijkstra(
						tx,
						0,
						newMockLedgerState(),
						dijkstraValidityOutcomePParams(t),
					)
					outcome.assert(t, err)
				})
			}
		})
	}
}

func TestValidateTxDijkstraDoesNotTreatPhase1FailureAsPhase2Failure(
	t *testing.T,
) {
	originalPhase1 := dijkstraPhase1UtxoValidationRules
	phase1Sentinel := errors.New("Dijkstra phase-1 sentinel")
	dijkstraPhase1UtxoValidationRules = []indexedUtxoValidationRule{{
		index: 0,
		validationFunc: func(
			lcommon.Transaction,
			uint64,
			lcommon.LedgerState,
			lcommon.ProtocolParameters,
		) error {
			return phase1Sentinel
		},
	}}
	t.Cleanup(func() { dijkstraPhase1UtxoValidationRules = originalPhase1 })

	tx := newDijkstraGuardingValidityOutcomeTx(
		t,
		false,
		lang.LanguageVersionV4,
		true,
		lcommon.ExUnits{},
	)
	err := ValidateTxDijkstra(
		tx,
		0,
		newMockLedgerState(),
		dijkstraValidityOutcomePParams(t),
	)
	require.ErrorIs(t, err, phase1Sentinel)
}

// TestValidateTxDijkstraSkipPhase2StillValidatesRequiredRedeemers pins the
// Dijkstra phase-1 boundary through Dingo's production entry point. Historical
// replay may skip script execution, but a reference-script spend must still
// carry its required redeemer.
func TestValidateTxDijkstraSkipPhase2StillValidatesRequiredRedeemers(
	t *testing.T,
) {
	requiredRedeemerIndex := noUtxoValidationRuleIndex
	for index, descriptor := range gdijkstra.UtxoValidationRuleDescriptors() {
		if descriptor.Id == lcommon.UtxoValidationRuleRequiredRedeemers {
			requiredRedeemerIndex = index
			break
		}
	}
	require.NotEqual(
		t,
		noUtxoValidationRuleIndex,
		requiredRedeemerIndex,
		"Dijkstra must declare the required-redeemer rule",
	)

	var requiredRedeemerRule *indexedUtxoValidationRule
	for _, rule := range dijkstraPhase1UtxoValidationRules {
		if rule.index == requiredRedeemerIndex {
			ruleCopy := rule
			requiredRedeemerRule = &ruleCopy
			break
		}
	}
	require.NotNil(
		t,
		requiredRedeemerRule,
		"Dijkstra phase-1 validation must retain the required-redeemer rule",
	)

	originalPhase1 := dijkstraPhase1UtxoValidationRules
	dijkstraPhase1UtxoValidationRules = []indexedUtxoValidationRule{
		*requiredRedeemerRule,
	}
	t.Cleanup(func() { dijkstraPhase1UtxoValidationRules = originalPhase1 })

	plutusScript := lcommon.PlutusV1Script{0x01, 0x02, 0x03}
	scriptAddr := newTestScriptAddress(t, plutusScript)
	spendInput := shelley.NewShelleyTransactionInput(
		"6666666666666666666666666666666666666666666666666666666666666666",
		0,
	)
	ls := newMockLedgerState()
	ls.skipPhase2Validation = true
	ls.addUtxo(
		spendInput,
		testAddressScriptOutput{
			testOutput: newTestOutput(1_000),
			addr:       scriptAddr,
			scriptRef:  plutusScript,
		},
	)

	newTx := func() *gdijkstra.DijkstraTransaction {
		return &gdijkstra.DijkstraTransaction{
			Body: gdijkstra.DijkstraTransactionBody{
				TxInputs: conway.NewConwayTransactionInputSet(
					[]shelley.ShelleyTransactionInput{spendInput},
				),
			},
			TxIsValid: true,
		}
	}

	t.Run("missing redeemer", func(t *testing.T) {
		err := ValidateTxDijkstra(
			newTx(),
			0,
			ls,
			dijkstraValidityOutcomePParams(t),
		)
		var missing lcommon.MissingRedeemerForScriptError
		require.ErrorAs(t, err, &missing)
		require.Equal(t, plutusScript.Hash(), missing.ScriptHash)
		require.Equal(t, lcommon.RedeemerTagSpend, missing.Tag)
		require.Equal(t, uint32(0), missing.Index)
	})

	t.Run("matching redeemer", func(t *testing.T) {
		tx := newTx()
		tx.WitnessSet = gdijkstra.DijkstraTransactionWitnessSet{
			WsRedeemers: gdijkstra.DijkstraRedeemers{
				Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
					{Tag: lcommon.RedeemerTagSpend, Index: 0}: {
						ExUnits: lcommon.ExUnits{Steps: 1, Memory: 1},
					},
				},
			},
		}
		require.NoError(t, ValidateTxDijkstra(
			tx,
			0,
			ls,
			dijkstraValidityOutcomePParams(t),
		))
	})
}

func (t *declaredValidityConwayTx) IsValid() bool {
	return t.valid
}

type validityOutcomeRedeemers struct {
	*mockRedeemers
}

func (r *validityOutcomeRedeemers) Value(
	idx uint,
	tag lcommon.RedeemerTag,
) lcommon.RedeemerValue {
	for _, entry := range r.entries {
		if entry.key.Index == uint32(idx) && entry.key.Tag == tag {
			return entry.val
		}
	}
	return lcommon.RedeemerValue{}
}

func newConwayValidityOutcomeTx(
	t *testing.T,
	valid bool,
	version lang.LanguageVersion,
	scriptFails bool,
	exUnits lcommon.ExUnits,
) *declaredValidityConwayTx {
	t.Helper()
	program := &syn.Program[syn.DeBruijn]{
		Version: lang.LanguageVersionV1,
		Term: &syn.Lambda[syn.DeBruijn]{
			Body: &syn.Lambda[syn.DeBruijn]{
				Body: &syn.Constant{Con: &syn.Unit{}},
			},
		},
	}
	flatProgram, err := syn.Encode(program)
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flatProgram)
	require.NoError(t, err)
	if scriptFails {
		// This malformed Flat payload reaches the evaluator rather than the
		// budget check, exercising the execution-error outcome path.
		scriptBytes = []byte{0x41, 0x00}
	}

	var script lcommon.Script
	witnesses := &mockWitnessSet{redeemers: &validityOutcomeRedeemers{
		mockRedeemers: &mockRedeemers{
			entries: []struct {
				key lcommon.RedeemerKey
				val lcommon.RedeemerValue
			}{
				{
					key: lcommon.RedeemerKey{
						Tag:   lcommon.RedeemerTagMint,
						Index: 0,
					},
					val: lcommon.RedeemerValue{ExUnits: exUnits},
				},
			},
		},
	}}
	switch version {
	case lang.LanguageVersionV1:
		plutusScript := lcommon.PlutusV1Script(scriptBytes)
		script = plutusScript
		witnesses.plutusV1Scripts = []lcommon.PlutusV1Script{plutusScript}
	case lang.LanguageVersionV2:
		plutusScript := lcommon.PlutusV2Script(scriptBytes)
		script = plutusScript
		witnesses.plutusV2Scripts = []lcommon.PlutusV2Script{plutusScript}
	default:
		t.Fatalf("unsupported Plutus version %v", version)
	}
	scriptHash := script.Hash()
	assetMint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
			lcommon.Blake2b224(scriptHash): {
				cbor.NewByteString([]byte("asset")): big.NewInt(1),
			},
		},
	)
	return &declaredValidityConwayTx{
		valid: valid,
		mockConwayFeeTx: &mockConwayFeeTx{
			mockFeeTx: mockFeeTx{
				txType:    txTypeAlonzo,
				witnesses: witnesses,
			},
			assetMint: &assetMint,
		},
	}
}

func TestValidateTxRequiresDeclaredValidityToMatchExecution(
	t *testing.T,
) {
	origAlonzo := alonzoUtxoValidationRules
	origBabbage := babbageUtxoValidationRules
	origAll := conwayUtxoValidationRules
	origPhase1 := conwayPhase1UtxoValidationRules
	t.Cleanup(func() {
		alonzoUtxoValidationRules = origAlonzo
		babbageUtxoValidationRules = origBabbage
		conwayUtxoValidationRules = origAll
		conwayPhase1UtxoValidationRules = origPhase1
	})
	// Keep the test focused on the phase-2 outcome contract. Phase-1 behavior
	// is covered separately by the validation-rule suite.
	alonzoUtxoValidationRules = nil
	babbageUtxoValidationRules = nil
	conwayUtxoValidationRules = nil
	conwayPhase1UtxoValidationRules = nil

	tests := []struct {
		name     string
		version  lang.LanguageVersion
		validate func(lcommon.Transaction) error
	}{
		{
			name:    "alonzo Plutus V1",
			version: lang.LanguageVersionV1,
			validate: func(tx lcommon.Transaction) error {
				return ValidateTxAlonzo(
					tx,
					0,
					newMockLedgerState(),
					&alonzo.AlonzoProtocolParameters{
						ProtocolMajor: 5,
						MaxTxExUnits: lcommon.ExUnits{
							Steps:  10_000_000,
							Memory: 10_000_000,
						},
						CostModels: map[uint][]int64{
							0: syntheticFullCostModel(
								t,
								lang.LanguageVersionV1,
							),
						},
					},
				)
			},
		},
		{
			name:    "babbage Plutus V1",
			version: lang.LanguageVersionV1,
			validate: func(tx lcommon.Transaction) error {
				return ValidateTxBabbage(
					tx,
					0,
					newMockLedgerState(),
					&babbage.BabbageProtocolParameters{
						ProtocolMajor: 7,
						MaxTxExUnits: lcommon.ExUnits{
							Steps:  10_000_000,
							Memory: 10_000_000,
						},
						CostModels: map[uint][]int64{
							0: syntheticFullCostModel(
								t,
								lang.LanguageVersionV1,
							),
						},
					},
				)
			},
		},
		{
			name:    "babbage Plutus V2",
			version: lang.LanguageVersionV2,
			validate: func(tx lcommon.Transaction) error {
				return ValidateTxBabbage(
					tx,
					0,
					newMockLedgerState(),
					&babbage.BabbageProtocolParameters{
						ProtocolMajor: 7,
						MaxTxExUnits: lcommon.ExUnits{
							Steps:  10_000_000,
							Memory: 10_000_000,
						},
						CostModels: map[uint][]int64{
							1: syntheticFullCostModel(
								t,
								lang.LanguageVersionV2,
							),
						},
					},
				)
			},
		},
		{
			name:    "conway Plutus V1",
			version: lang.LanguageVersionV1,
			validate: func(tx lcommon.Transaction) error {
				return ValidateTxConway(
					tx,
					0,
					newMockLedgerState(),
					&conway.ConwayProtocolParameters{
						ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
							Major: 9,
						},
						MaxTxExUnits: lcommon.ExUnits{
							Steps:  10_000_000,
							Memory: 10_000_000,
						},
						CostModels: map[uint][]int64{
							0: syntheticFullCostModel(
								t,
								lang.LanguageVersionV1,
							),
						},
					},
				)
			},
		},
		{
			name:    "conway Plutus V2",
			version: lang.LanguageVersionV2,
			validate: func(tx lcommon.Transaction) error {
				return ValidateTxConway(
					tx,
					0,
					newMockLedgerState(),
					&conway.ConwayProtocolParameters{
						ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
							Major: 9,
						},
						MaxTxExUnits: lcommon.ExUnits{
							Steps:  10_000_000,
							Memory: 10_000_000,
						},
						CostModels: map[uint][]int64{
							1: syntheticFullCostModel(
								t,
								lang.LanguageVersionV2,
							),
						},
					},
				)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Run("declared valid and scripts succeed", func(t *testing.T) {
				tx := newConwayValidityOutcomeTx(
					t,
					true,
					test.version,
					false,
					lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
				)
				require.NoError(t, test.validate(tx))
			})

			t.Run("declared invalid but scripts succeed", func(t *testing.T) {
				tx := newConwayValidityOutcomeTx(
					t,
					false,
					test.version,
					false,
					lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
				)
				err := test.validate(tx)
				require.ErrorContains(
					t,
					err,
					"declared invalid but Plutus scripts succeeded",
				)
			})

			t.Run("declared valid but scripts fail", func(t *testing.T) {
				tx := newConwayValidityOutcomeTx(
					t,
					true,
					test.version,
					false,
					lcommon.ExUnits{},
				)
				err := test.validate(tx)
				_, ok := errors.AsType[conway.PlutusScriptFailedError](err)
				require.True(
					t,
					ok,
					"expected Plutus script failure, got %v",
					err,
				)
			})

			t.Run("declared invalid and scripts fail", func(t *testing.T) {
				tx := newConwayValidityOutcomeTx(
					t,
					false,
					test.version,
					false,
					lcommon.ExUnits{},
				)
				require.NoError(t, test.validate(tx))
			})

			t.Run("declared valid but evaluator errors", func(t *testing.T) {
				tx := newConwayValidityOutcomeTx(
					t,
					true,
					test.version,
					true,
					lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
				)
				var scriptErr conway.PlutusScriptFailedError
				require.ErrorAs(t, test.validate(tx), &scriptErr)
			})

			t.Run("declared invalid and evaluator errors", func(t *testing.T) {
				tx := newConwayValidityOutcomeTx(
					t,
					false,
					test.version,
					true,
					lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
				)
				require.NoError(t, test.validate(tx))
			})
		})
	}
}
