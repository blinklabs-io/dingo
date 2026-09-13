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
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/stretchr/testify/require"
)

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

// TestValidateTxBabbageRejectsPlutusV2WhenSynthetic covers blinklabs-io/dingo#3962:
// real cardano-ledger rejects a transaction using a PlutusV2 script outright,
// at the UTXOW level before any script evaluation runs, whenever PlutusV2 has
// no real cost model configured yet (NoCostModel, the formal rule "languages
// txw ⊆ dom(costmdls pp)"). Dingo's HardForkBabbage instead fabricates a
// value specifically so internal validation always has one, which -- absent
// this check -- would let Dingo accept the same transaction a real network
// rejects.
func TestValidateTxBabbageRejectsPlutusV2WhenSynthetic(t *testing.T) {
	disablePhase1RulesForTest(t)

	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true
	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	err := ValidateTxBabbage(
		tx,
		0,
		ls,
		&babbage.BabbageProtocolParameters{
			ProtocolMajor: 7,
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
		},
	)

	require.ErrorIs(t, err, ErrNoCostModelForPlutusV2)
}

// TestValidateTxBabbageAllowsPlutusV2WhenNotSynthetic covers the common case
// once real data exists (or on a database predating this tracking, where the
// bootstrap fallback resolves to false for a real, non-default value): the
// same PlutusV2 transaction must validate exactly as it did before this
// check existed.
func TestValidateTxBabbageAllowsPlutusV2WhenNotSynthetic(t *testing.T) {
	disablePhase1RulesForTest(t)

	ls := newMockLedgerState()
	// syntheticV2CostModel left at its zero value (false).
	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	err := ValidateTxBabbage(
		tx,
		0,
		ls,
		&babbage.BabbageProtocolParameters{
			ProtocolMajor: 7,
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
			// A real (non-synthetic) PlutusV2 cost model, same as production
			// carries once an on-chain update lands. requiredCostModel
			// (issue #3528) fails closed on a missing entry, so this must be
			// populated for the "not synthetic" case to actually reach
			// evaluation instead of being rejected before it ever does.
			CostModels: map[uint][]int64{
				1: defaultMachineCostModel(t, lang.LanguageVersionV2),
			},
		},
	)

	require.NoError(t, err)
}

// TestValidateTxBabbageAllowsPlutusV2WhenLedgerStateReportsNothing covers the
// fail-safe default: an lcommon.LedgerState implementation that does not
// implement syntheticV2CostModelReporter at all (any caller other than
// ledger.LedgerView's own *ledger.LedgerView) must not be treated as
// synthetic -- this check is additive and must never fire for a caller that
// simply doesn't carry the signal.
func TestValidateTxBabbageAllowsPlutusV2WhenLedgerStateReportsNothing(
	t *testing.T,
) {
	disablePhase1RulesForTest(t)

	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	err := ValidateTxBabbage(
		tx,
		0,
		plainMockLedgerState{newMockLedgerState()},
		&babbage.BabbageProtocolParameters{
			ProtocolMajor: 7,
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
			CostModels: map[uint][]int64{
				1: defaultMachineCostModel(t, lang.LanguageVersionV2),
			},
		},
	)

	require.NoError(t, err)
}

// plainMockLedgerState embeds the lcommon.LedgerState INTERFACE (not the
// concrete *mockLedgerState type), so method promotion exposes exactly that
// interface's method set and nothing more -- in particular, not
// SyntheticV2CostModelInEffect, even though the concrete value stored in it
// (a *mockLedgerState) happens to have that extra method. This stands in for
// any lcommon.LedgerState implementation other than *ledger.LedgerView,
// which is the only production type this check's type assertion expects to
// see.
type plainMockLedgerState struct {
	lcommon.LedgerState
}

// TestEvaluateTxBabbageRejectsPlutusV2WhenSynthetic covers the fee/ex-units
// estimation counterpart: a transaction ValidateTxBabbage would reject must
// not be quoted a fee estimate implying it's valid.
func TestEvaluateTxBabbageRejectsPlutusV2WhenSynthetic(t *testing.T) {
	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true
	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	_, _, _, err := EvaluateTxBabbage(
		tx,
		ls,
		&babbage.BabbageProtocolParameters{
			ProtocolMajor: 7,
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
		},
	)

	require.ErrorIs(t, err, ErrNoCostModelForPlutusV2)
}

// TestEvaluateTxBabbageAllowsPlutusV2WhenNotSynthetic mirrors
// TestValidateTxBabbageAllowsPlutusV2WhenNotSynthetic for EvaluateTxBabbage.
func TestEvaluateTxBabbageAllowsPlutusV2WhenNotSynthetic(t *testing.T) {
	ls := newMockLedgerState()
	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	_, _, _, err := EvaluateTxBabbage(
		tx,
		ls,
		&babbage.BabbageProtocolParameters{
			ProtocolMajor: 7,
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
			CostModels: map[uint][]int64{
				1: defaultMachineCostModel(t, lang.LanguageVersionV2),
			},
		},
	)

	require.NoError(t, err)
}

// TestValidateTxConwayRejectsPlutusV2WhenSynthetic mirrors
// TestValidateTxBabbageRejectsPlutusV2WhenSynthetic for the Conway era: the
// synthetic marker persists across era transitions until real data actually
// clears it (LedgerState.syntheticV2CostModel's doc comment), so a chain
// that reaches Conway without ever receiving a real PlutusV2 update has the
// identical exposure ValidateTxBabbage does.
func TestValidateTxConwayRejectsPlutusV2WhenSynthetic(t *testing.T) {
	disablePhase1RulesForTest(t)

	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true
	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	err := ValidateTxConway(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 9,
			},
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
		},
	)

	require.ErrorIs(t, err, ErrNoCostModelForPlutusV2)
}

// TestValidateTxConwayAllowsPlutusV2WhenNotSynthetic mirrors
// TestValidateTxBabbageAllowsPlutusV2WhenNotSynthetic for Conway.
func TestValidateTxConwayAllowsPlutusV2WhenNotSynthetic(t *testing.T) {
	disablePhase1RulesForTest(t)

	ls := newMockLedgerState()
	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	err := ValidateTxConway(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 9,
			},
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
			CostModels: map[uint][]int64{
				1: defaultMachineCostModel(t, lang.LanguageVersionV2),
			},
		},
	)

	require.NoError(t, err)
}

// TestEvaluateTxConwayRejectsPlutusV2WhenSynthetic mirrors
// TestEvaluateTxBabbageRejectsPlutusV2WhenSynthetic for Conway.
func TestEvaluateTxConwayRejectsPlutusV2WhenSynthetic(t *testing.T) {
	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true
	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	_, _, _, err := EvaluateTxConway(
		tx,
		ls,
		&conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 9,
			},
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
		},
	)

	require.ErrorIs(t, err, ErrNoCostModelForPlutusV2)
}

// TestValidateTxConwayRejectsPlutusV2WhenSyntheticEvenIfDeclaredInvalid
// covers a gap CodeRabbit flagged in review: validatePlutusOutcome
// (ledger/eras/validation.go) treats a failed script as the expected,
// acceptable outcome for a transaction declared invalid -- but only when
// the phase-2 error is a conway.PlutusScriptFailedError specifically.
// ErrNoCostModelForPlutusV2 is a hard UTXOW-level rejection (real
// cardano-ledger raises it before any script runs), not a script-execution
// failure, so it must still reject the transaction outright even when the
// transaction declares itself invalid and provides collateral -- it must
// not be silently accepted as "failed as declared."
func TestValidateTxConwayRejectsPlutusV2WhenSyntheticEvenIfDeclaredInvalid(
	t *testing.T,
) {
	disablePhase1RulesForTest(t)

	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true
	tx := newConwayValidityOutcomeTx(
		t,
		false,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	err := ValidateTxConway(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 9,
			},
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
		},
	)

	require.ErrorIs(t, err, ErrNoCostModelForPlutusV2)
}

// TestEvaluateTxConwayAllowsPlutusV2WhenNotSynthetic mirrors
// TestEvaluateTxBabbageAllowsPlutusV2WhenNotSynthetic for Conway.
func TestEvaluateTxConwayAllowsPlutusV2WhenNotSynthetic(t *testing.T) {
	ls := newMockLedgerState()
	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	_, _, _, err := EvaluateTxConway(
		tx,
		ls,
		&conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 9,
			},
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
			CostModels: map[uint][]int64{
				1: defaultMachineCostModel(t, lang.LanguageVersionV2),
			},
		},
	)

	require.NoError(t, err)
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
// human-reviewer finding on blinklabs-io/dingo#3962's PR: ValidateTxDijkstra
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

// TestValidateTxDijkstraRejectsPlutusV2WhenSyntheticSubTransaction covers a
// Cubic finding on blinklabs-io/dingo#3962's PR: the earlier Dijkstra tests
// all used a *mockConwayFeeTx (not a concrete *gdijkstra.DijkstraTransaction),
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
