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
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockConwayFeeTxV3 adds CurrentTreasuryValue, which
// script.NewTxInfoV3FromTransaction requires and mockConwayFeeTx does not
// implement.
type mockConwayFeeTxV3 struct {
	mockConwayFeeTx
}

func (m *mockConwayFeeTxV3) CurrentTreasuryValue() *big.Int {
	return nil
}

func (m *mockConwayFeeTxV3) Donation() *big.Int {
	return nil
}

// TestRequiredCostModelWiringMissingEntryFailsClosed is a regression test
// for a human-review finding: requiredCostModel (issue #3528) is exercised
// by nine call sites across alonzo.go, babbage.go and conway.go, but before
// this test only one of them -- conway.go's PlutusV1 branch, via
// TestConwayPlutusBudgetComparisonIncludesFinalSlippageBatch -- had a test
// that failed if it were reverted to a bare, unguarded map index. The other
// eight (ValidateTxAlonzo, EvaluateTxAlonzo, ValidateTxBabbage's V1 and V2
// branches, EvaluateTxBabbage's V1 and V2 branches, and
// evaluateConwayPlutusScript's V2 and V3 branches) could each silently
// regress to evaluating a missing cost model under plutigo's built-in
// defaults without any test noticing. This pins all eight remaining sites
// to the same fail-closed contract.
func TestRequiredCostModelWiringMissingEntryFailsClosed(t *testing.T) {
	// A minimal unit-returning program. Its behavior under evaluation is
	// irrelevant here: a missing cost model must be rejected by
	// requiredCostModel before plutigo's evaluator ever runs.
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

	spendRedeemers := func() *mockRedeemers {
		return &mockRedeemers{
			entries: []struct {
				key lcommon.RedeemerKey
				val lcommon.RedeemerValue
			}{
				{
					key: lcommon.RedeemerKey{
						Tag:   lcommon.RedeemerTagSpend,
						Index: 0,
					},
					val: lcommon.RedeemerValue{ExUnits: lcommon.ExUnits{}},
				},
			},
		}
	}

	// newSpendFixture builds a transaction spending a single UTxO locked by
	// script, whose witness set is witnesses. Alonzo and Babbage phase-2
	// validation resolve the script purpose from this spent input.
	newSpendFixture := func(
		script lcommon.Script,
		witnesses *mockWitnessSet,
	) (*mockConwayFeeTx, *mockLedgerState) {
		spendInput := newTestInput(0x01, 0)
		addr := newTestScriptAddress(t, script)
		tx := &mockConwayFeeTx{
			mockFeeTx: mockFeeTx{
				txType:    txTypeAlonzo,
				witnesses: witnesses,
			},
			inputs: []lcommon.TransactionInput{spendInput},
		}
		ls := newMockLedgerState()
		ls.addUtxo(
			spendInput,
			testAddressOutput{
				testOutput: newTestOutput(1_000_000),
				addr:       addr,
			},
		)
		return tx, ls
	}

	maxTxExUnits := lcommon.ExUnits{Steps: 1_000_000, Memory: 1_000_000}

	t.Run("alonzo", func(t *testing.T) {
		v1Script := lcommon.PlutusV1Script(scriptBytes)
		witnesses := &mockWitnessSet{
			redeemers:       spendRedeemers(),
			plutusV1Scripts: []lcommon.PlutusV1Script{v1Script},
		}

		t.Run(
			"ValidateTxAlonzo missing PlutusV1 cost model",
			func(t *testing.T) {
				// Clear the phase-1 rule set so the missing-cost-model failure
				// is what's under test, not a fee or metadata check the mock
				// transaction can't support (mirrors
				// TestPlutusBudgetComparisonIncludesFinalSlippageBatch).
				origRules := alonzoUtxoValidationRules
				t.Cleanup(func() { alonzoUtxoValidationRules = origRules })
				alonzoUtxoValidationRules = nil

				tx, ls := newSpendFixture(v1Script, witnesses)
				err := ValidateTxAlonzo(
					tx,
					0,
					ls,
					&alonzo.AlonzoProtocolParameters{
						ProtocolMajor: 5,
						MaxTxExUnits:  maxTxExUnits,
					},
				)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "missing PlutusV1 cost model")
			},
		)

		t.Run(
			"EvaluateTxAlonzo missing PlutusV1 cost model",
			func(t *testing.T) {
				tx, ls := newSpendFixture(v1Script, witnesses)
				_, _, _, err := EvaluateTxAlonzo(
					tx,
					ls,
					&alonzo.AlonzoProtocolParameters{
						ProtocolMajor: 5,
						MaxTxExUnits:  maxTxExUnits,
					},
				)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "missing PlutusV1 cost model")
			},
		)
	})

	t.Run("babbage", func(t *testing.T) {
		v1Script := lcommon.PlutusV1Script(scriptBytes)
		v1Witnesses := &mockWitnessSet{
			redeemers:       spendRedeemers(),
			plutusV1Scripts: []lcommon.PlutusV1Script{v1Script},
		}
		v2Script := lcommon.PlutusV2Script(scriptBytes)
		v2Witnesses := &mockWitnessSet{
			redeemers:       spendRedeemers(),
			plutusV2Scripts: []lcommon.PlutusV2Script{v2Script},
		}

		t.Run(
			"ValidateTxBabbage missing PlutusV1 cost model",
			func(t *testing.T) {
				origRules := babbageUtxoValidationRules
				t.Cleanup(func() { babbageUtxoValidationRules = origRules })
				babbageUtxoValidationRules = nil

				tx, ls := newSpendFixture(v1Script, v1Witnesses)
				err := ValidateTxBabbage(
					tx,
					0,
					ls,
					&babbage.BabbageProtocolParameters{
						ProtocolMajor: 7,
						MaxTxExUnits:  maxTxExUnits,
					},
				)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "missing PlutusV1 cost model")
			},
		)

		t.Run(
			"ValidateTxBabbage missing PlutusV2 cost model",
			func(t *testing.T) {
				origRules := babbageUtxoValidationRules
				t.Cleanup(func() { babbageUtxoValidationRules = origRules })
				babbageUtxoValidationRules = nil

				tx, ls := newSpendFixture(v2Script, v2Witnesses)
				err := ValidateTxBabbage(
					tx,
					0,
					ls,
					&babbage.BabbageProtocolParameters{
						ProtocolMajor: 7,
						MaxTxExUnits:  maxTxExUnits,
					},
				)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "missing PlutusV2 cost model")
			},
		)

		t.Run(
			"EvaluateTxBabbage missing PlutusV1 cost model",
			func(t *testing.T) {
				tx, ls := newSpendFixture(v1Script, v1Witnesses)
				_, _, _, err := EvaluateTxBabbage(
					tx,
					ls,
					&babbage.BabbageProtocolParameters{
						ProtocolMajor: 7,
						MaxTxExUnits:  maxTxExUnits,
					},
				)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "missing PlutusV1 cost model")
			},
		)

		t.Run(
			"EvaluateTxBabbage missing PlutusV2 cost model",
			func(t *testing.T) {
				tx, ls := newSpendFixture(v2Script, v2Witnesses)
				_, _, _, err := EvaluateTxBabbage(
					tx,
					ls,
					&babbage.BabbageProtocolParameters{
						ProtocolMajor: 7,
						MaxTxExUnits:  maxTxExUnits,
					},
				)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "missing PlutusV2 cost model")
			},
		)
	})

	// Conway's V1 branch of evaluateConwayPlutusScript already has this
	// coverage via TestConwayPlutusBudgetComparisonIncludesFinalSlippageBatch.
	// This covers the V2 and V3 branches the same way that test covers V1:
	// a minting purpose, so the same unit-returning program applies to
	// every language version without needing a per-version datum shape.
	t.Run("conway", func(t *testing.T) {
		newMintFixture := func(
			script lcommon.Script,
			witnesses *mockWitnessSet,
		) *mockConwayFeeTx {
			scriptHash := script.Hash()
			assetMint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
				map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
					lcommon.Blake2b224(scriptHash): {
						cbor.NewByteString([]byte("asset")): big.NewInt(1),
					},
				},
			)
			return &mockConwayFeeTx{
				mockFeeTx: mockFeeTx{
					txType:    txTypeAlonzo,
					witnesses: witnesses,
				},
				assetMint: &assetMint,
			}
		}
		mintRedeemers := func() *mockRedeemers {
			return &mockRedeemers{
				entries: []struct {
					key lcommon.RedeemerKey
					val lcommon.RedeemerValue
				}{
					{
						key: lcommon.RedeemerKey{
							Tag:   lcommon.RedeemerTagMint,
							Index: 0,
						},
						val: lcommon.RedeemerValue{ExUnits: lcommon.ExUnits{}},
					},
				},
			}
		}
		pp := &conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 9,
			},
			MaxTxExUnits: maxTxExUnits,
		}
		clearConwayRules := func(t *testing.T) {
			t.Helper()
			origAll := conwayUtxoValidationRules
			origPhase1 := conwayPhase1UtxoValidationRules
			t.Cleanup(func() {
				conwayUtxoValidationRules = origAll
				conwayPhase1UtxoValidationRules = origPhase1
			})
			conwayUtxoValidationRules = nil
			conwayPhase1UtxoValidationRules = nil
		}

		t.Run("missing PlutusV2 cost model", func(t *testing.T) {
			clearConwayRules(t)
			v2Script := lcommon.PlutusV2Script(scriptBytes)
			tx := newMintFixture(v2Script, &mockWitnessSet{
				redeemers:       mintRedeemers(),
				plutusV2Scripts: []lcommon.PlutusV2Script{v2Script},
			})
			err := ValidateTxConway(tx, 0, newMockLedgerState(), pp)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "missing PlutusV2 cost model")
		})

		t.Run("missing PlutusV3 cost model", func(t *testing.T) {
			clearConwayRules(t)
			v3Script := lcommon.PlutusV3Script(scriptBytes)
			base := newMintFixture(v3Script, &mockWitnessSet{
				redeemers:       mintRedeemers(),
				plutusV3Scripts: []lcommon.PlutusV3Script{v3Script},
			})
			tx := &mockConwayFeeTxV3{mockConwayFeeTx: *base}
			err := ValidateTxConway(tx, 0, newMockLedgerState(), pp)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "missing PlutusV3 cost model")
		})
	})
}
