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
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/require"
)

// TestValidateTxDijkstraRejectsParameterChangeProtocolVersion is the
// Dijkstra analogue of TestValidateTxConwayRejectsParameterChangeProtocolVersion,
// through the production ValidateTxDijkstra entry point.
func TestValidateTxDijkstraRejectsParameterChangeProtocolVersion(t *testing.T) {
	originalRules := dijkstraPhase1UtxoValidationRules
	dijkstraPhase1UtxoValidationRules = nil
	t.Cleanup(func() { dijkstraPhase1UtxoValidationRules = originalRules })

	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{witnesses: &mockWitnessSet{}},
		proposalProcedures: []lcommon.ProposalProcedure{
			dijkstraParameterChangeProposal(
				&lcommon.ProtocolParametersProtocolVersion{
					Major: gdijkstra.MinProtocolVersionDijkstra,
				},
			),
		},
	}
	err := ValidateTxDijkstra(
		tx,
		0,
		newMockLedgerState(),
		&gdijkstra.DijkstraProtocolParameters{
			ConwayProtocolParameters: *conwayDivergencePparams(),
		},
	)
	var protocolVersionErr ParameterChangeProtocolVersionError
	require.ErrorAs(t, err, &protocolVersionErr)
}

// TestPParamsUpdateDijkstraIgnoresProtocolVersion is the Dijkstra analogue
// of TestPParamsUpdateConwayIgnoresProtocolVersion.
func TestPParamsUpdateDijkstraIgnoresProtocolVersion(t *testing.T) {
	current := &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
		},
	}
	minFeeA := uint(500)
	updated, err := PParamsUpdateDijkstra(
		current,
		gdijkstra.DijkstraProtocolParameterUpdate{
			MinFeeA: &minFeeA,
			ProtocolVersion: &lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra + 1,
			},
		},
	)
	require.NoError(t, err)
	dijkstraUpdated, ok := updated.(*gdijkstra.DijkstraProtocolParameters)
	require.True(t, ok)
	require.Equal(
		t,
		uint(gdijkstra.MinProtocolVersionDijkstra),
		dijkstraUpdated.ProtocolVersion.Major,
		"ParameterChange must not move protocol version",
	)
	require.Equal(
		t,
		minFeeA,
		dijkstraUpdated.MinFeeA,
		"other fields must still apply",
	)
}

func (s *pastHorizonLedgerState) SlotToTime(
	slot uint64,
) (time.Time, error) {
	s.slotToTimeCalls++
	if slot >= s.horizonSlot {
		return time.Time{}, hardfork.ErrPastHorizon
	}
	// #nosec G115 -- test slots are small
	return time.Unix(int64(slot), 0), nil
}

// uplcProgramVersion110 is UPLC program version 1.1.0, the version Plutus V3
// and V4 scripts are encoded with.
var uplcProgramVersion110 = lang.LanguageVersion{1, 1, 0}

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

// dijkstraValidityOutcomePParams supplies real Preview epoch-672 cost models.
// plutigo costs every parameter missing from a supplied list at
// math.MaxInt64, as plutus-ledger-api does, so an empty CostModels map makes
// the first CEK machine step exhaust any budget. PlutusV4 reuses the PlutusV3
// list: its machine-step parameters are costed, and the builtins it leaves at
// MaxInt64 are never called by these scripts.
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
	return &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
			CostModels: map[uint][]int64{
				0: costModels.PlutusV1,
				1: costModels.PlutusV2,
				2: costModels.PlutusV3,
				3: costModels.PlutusV3,
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

func (m *mockConwayFeeTxV3) CurrentTreasuryValue() *big.Int {
	return nil
}

func (m *mockConwayFeeTxV3) Donation() *big.Int {
	return nil
}

// disablePhase1RulesForTest replaces every era's phase-1 UTXO validation
// rule table with nil for the duration of t, restoring the originals on
// cleanup. Mirrors TestValidateTxRequiresDeclaredValidityToMatchExecution's
// setup: these tests exercise only the phase-2 ErrNoCostModelForPlutusV2
// check, not the full phase-1 rule suite, which needs a much more complete
// (fee/TTL/deposit-correct) transaction than these fixtures build.
func disablePhase1RulesForTest(t *testing.T) {
	t.Helper()
	origBabbage := babbageUtxoValidationRules
	origConwayAll := conwayUtxoValidationRules
	origConwayPhase1 := conwayPhase1UtxoValidationRules
	origDijkstra := dijkstraPhase1UtxoValidationRules
	t.Cleanup(func() {
		babbageUtxoValidationRules = origBabbage
		conwayUtxoValidationRules = origConwayAll
		conwayPhase1UtxoValidationRules = origConwayPhase1
		dijkstraPhase1UtxoValidationRules = origDijkstra
	})
	babbageUtxoValidationRules = nil
	conwayUtxoValidationRules = nil
	conwayPhase1UtxoValidationRules = nil
	dijkstraPhase1UtxoValidationRules = nil
}

// dijkstraSyntheticV2Tx returns a minimal lcommon.Transaction -- not a
// concrete *gdijkstra.DijkstraTransaction -- witnessing a PlutusV2 script.
// dijkstraSyntheticV2CostModelGuard's version detection
// (gdijkstra.UtxoValidateCostModelsPresent's usedPlutusVersions) falls back
// to a plain witness-set scan for any non-concrete transaction type, so this
// avoids needing a fully-shaped Dijkstra transaction with real spend/redeemer
// wiring just to prove the guard fires.
func dijkstraSyntheticV2Tx() *mockConwayFeeTx {
	return &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			witnesses: &mockWitnessSet{
				plutusV2Scripts: []lcommon.PlutusV2Script{{0x01}},
			},
		},
	}
}

// dijkstraGuardPParams returns Dijkstra protocol parameters carrying a
// PlutusV2 cost model, standing in for HardForkBabbage's fabricated default
// that dijkstraSyntheticV2CostModelGuard prunes before checking presence.
func dijkstraGuardPParams() *gdijkstra.DijkstraProtocolParameters {
	return &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
			CostModels: map[uint][]int64{
				1: {1, 2, 3},
			},
		},
	}
}

// TestValidateTxDijkstraRejectsPlutusV2WhenSyntheticNormalPath covers a
// human-reviewer finding on PR: ValidateTxDijkstra
// delegates phase-2 validation entirely to
// gdijkstra.UtxoValidatePlutusScripts, which has no idea about Dingo's
// synthetic marker -- without dijkstraSyntheticV2CostModelGuard, a
// transaction using a synthetic-cost-model PlutusV2 script would reach that
// delegate and be priced against the fabricated value instead of rejected.
func TestValidateTxDijkstraRejectsPlutusV2WhenSyntheticNormalPath(
	t *testing.T,
) {
	disablePhase1RulesForTest(t)

	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true

	err := ValidateTxDijkstra(
		dijkstraSyntheticV2Tx(),
		0,
		ls,
		dijkstraGuardPParams(),
	)

	require.ErrorIs(t, err, ErrNoCostModelForPlutusV2)
}

// TestValidateTxDijkstraRejectsPlutusV2WhenSyntheticSkipPhase2Path covers
// the other half of the same finding: shouldSkipPhase2Validation returns nil
// before ever reaching the delegate, so the guard must run before that
// shortcut too, not only before the delegation call.
func TestValidateTxDijkstraRejectsPlutusV2WhenSyntheticSkipPhase2Path(
	t *testing.T,
) {
	disablePhase1RulesForTest(t)

	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true
	ls.skipPhase2Validation = true

	err := ValidateTxDijkstra(
		dijkstraSyntheticV2Tx(),
		0,
		ls,
		dijkstraGuardPParams(),
	)

	require.ErrorIs(t, err, ErrNoCostModelForPlutusV2)
}

// TestValidateTxDijkstraAllowsNonPlutusV2TxWhenSynthetic proves the guard is
// additive: a transaction that never witnesses a PlutusV2 script is not
// rejected merely because the synthetic marker happens to be set.
func TestValidateTxDijkstraAllowsNonPlutusV2TxWhenSynthetic(t *testing.T) {
	disablePhase1RulesForTest(t)

	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true
	ls.skipPhase2Validation = true

	err := ValidateTxDijkstra(
		&mockConwayFeeTx{},
		0,
		ls,
		dijkstraGuardPParams(),
	)

	require.NoError(t, err)
}

// TestValidateTxDijkstraRejectsPlutusV2WhenSyntheticSubTransaction verifies
// the concrete Dijkstra transaction path, in addition to the mock path used
// by other tests. The earlier Dijkstra tests all used a *mockConwayFeeTx (not
// a concrete *gdijkstra.DijkstraTransaction),
// which takes usedPlutusVersions' plain witness-set-scan fallback rather
// than the dijkstraScriptLevels resolution a real Dijkstra transaction
// actually goes through -- leaving the sub-transaction and reference-script
// resolution paths dijkstraSyntheticV2CostModelGuard's doc comment claims to
// cover unverified. This uses a real concrete transaction with the PlutusV2
// script witnessed and needed only inside a sub-transaction's own spend
// input, empirically confirming dijkstraScriptLevels' per-level Needed
// computation (which dijkstraConwayFeatureTransaction scopes to each level's
// own body/witnesses) folds a sub-transaction's needed languages into the
// top-level usedPlutusVersions result -- the same mechanism
// UtxoValidatePlutusScripts itself already depends on to enforce
// UnsupportedScriptInSubtransactionError, so this guard's coverage cannot
// regress silently if that upstream mechanism ever changed.
func TestValidateTxDijkstraRejectsPlutusV2WhenSyntheticSubTransaction(
	t *testing.T,
) {
	disablePhase1RulesForTest(t)

	plutusV2Script := lcommon.PlutusV2Script([]byte{0x01})
	scriptAddr := newTestScriptAddress(t, plutusV2Script)
	subTxInput := shelley.NewShelleyTransactionInput(
		strings.Repeat("ab", 32),
		0,
	)

	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true
	ls.skipPhase2Validation = true
	ls.utxos[subTxInput.Id().String()+"#0"] = lcommon.Utxo{
		Id: subTxInput,
		Output: testAddressOutput{
			testOutput: newTestOutput(1_000_000),
			addr:       scriptAddr,
		},
	}

	tx := &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxSubTransactions: cbor.NewSetType(
				[]gdijkstra.DijkstraSubTransaction{{
					Body: gdijkstra.DijkstraSubTransactionBody{
						TxInputs: conway.NewConwayTransactionInputSet(
							[]shelley.ShelleyTransactionInput{subTxInput},
						),
					},
					WitnessSet: gdijkstra.DijkstraTransactionWitnessSet{
						WsPlutusV2Scripts: cbor.NewSetType(
							[]lcommon.PlutusV2Script{plutusV2Script},
							false,
						),
						WsRedeemers: gdijkstra.DijkstraRedeemers{
							Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
								{Tag: lcommon.RedeemerTagSpend, Index: 0}: {
									ExUnits: lcommon.ExUnits{
										Steps:  1,
										Memory: 1,
									},
								},
							},
						},
					},
				}},
				false,
			),
		},
		TxIsValid: true,
	}

	err := ValidateTxDijkstra(
		tx,
		0,
		ls,
		dijkstraGuardPParams(),
	)

	require.ErrorIs(t, err, ErrNoCostModelForPlutusV2)
}

// TestValidateTxDijkstraRejectsPlutusV2WhenSyntheticReferenceScript is
// TestValidateTxDijkstraRejectsPlutusV2WhenSyntheticSubTransaction's
// counterpart for the reference-script resolution path: a real concrete
// transaction whose PlutusV2 script is never directly witnessed, only
// resolved from a reference input, and needed by a separate spend input at
// that script's address.
func TestValidateTxDijkstraRejectsPlutusV2WhenSyntheticReferenceScript(
	t *testing.T,
) {
	disablePhase1RulesForTest(t)

	plutusV2Script := lcommon.PlutusV2Script([]byte{0x01})
	scriptAddr := newTestScriptAddress(t, plutusV2Script)
	spendInput := shelley.NewShelleyTransactionInput(
		strings.Repeat("cd", 32),
		0,
	)
	refInput := shelley.NewShelleyTransactionInput(
		strings.Repeat("ef", 32),
		0,
	)

	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true
	ls.skipPhase2Validation = true
	ls.utxos[spendInput.Id().String()+"#0"] = lcommon.Utxo{
		Id: spendInput,
		Output: testAddressOutput{
			testOutput: newTestOutput(1_000_000),
			addr:       scriptAddr,
		},
	}
	ls.utxos[refInput.Id().String()+"#0"] = lcommon.Utxo{
		Id: refInput,
		Output: testAddressScriptOutput{
			testOutput: newTestOutput(1_000_000),
			addr:       newTestKeyAddress(t),
			scriptRef:  plutusV2Script,
		},
	}

	tx := &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{spendInput},
			),
			TxReferenceInputs: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{refInput},
				false,
			),
		},
		WitnessSet: gdijkstra.DijkstraTransactionWitnessSet{
			WsRedeemers: gdijkstra.DijkstraRedeemers{
				Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
					{Tag: lcommon.RedeemerTagSpend, Index: 0}: {
						ExUnits: lcommon.ExUnits{Steps: 1, Memory: 1},
					},
				},
			},
		},
		TxIsValid: true,
	}

	err := ValidateTxDijkstra(
		tx,
		0,
		ls,
		dijkstraGuardPParams(),
	)

	require.ErrorIs(t, err, ErrNoCostModelForPlutusV2)
}
