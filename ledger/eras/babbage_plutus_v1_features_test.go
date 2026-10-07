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
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/stretchr/testify/require"
)

// v1FeatureCase describes a Babbage-era feature a transaction can carry
// alongside a Plutus script.
type v1FeatureCase struct {
	name string
	// apply adds the feature to tx and registers any UTxO it needs in ls.
	apply func(
		t *testing.T,
		tx *mockProducedValidityTx,
		ls *mockLedgerState,
	)
	// wantV1Err asserts the error a PlutusV1 transaction gets in Babbage.
	wantV1Err func(t *testing.T, err error)
	// conwayAllowsV1 is true when Conway accepts the feature with V1.
	conwayAllowsV1 bool
}

func v1FeatureCases(t *testing.T) []v1FeatureCase {
	t.Helper()
	keyAddr := newTestKeyAddress(t)
	refScript := lcommon.PlutusV2Script{0x0a, 0x0b}
	return []v1FeatureCase{
		{
			name: "reference input",
			apply: func(t *testing.T, tx *mockProducedValidityTx, ls *mockLedgerState) {
				ref := newTestInput(0xe1, 0)
				tx.referenceInputs = []lcommon.TransactionInput{ref}
				ls.addUtxo(ref, newTestOutput(1_000_000))
			},
			wantV1Err: func(t *testing.T, err error) {
				var target babbage.PlutusV1ReferenceInputsNotSupportedError
				require.ErrorAs(t, err, &target)
			},
			conwayAllowsV1: true,
		},
		{
			name: "reference script on spent input",
			apply: func(t *testing.T, tx *mockProducedValidityTx, ls *mockLedgerState) {
				spent := newTestInput(0xe2, 0)
				tx.inputs = append(tx.inputs, spent)
				ls.addUtxo(spent, testAddressScriptOutput{
					testOutput: newTestOutput(1_000_000),
					addr:       keyAddr,
					scriptRef:  refScript,
				})
			},
			wantV1Err: func(t *testing.T, err error) {
				var target babbage.PlutusV1ReferenceScriptsNotSupportedError
				require.ErrorAs(t, err, &target)
			},
			conwayAllowsV1: true,
		},
		{
			name: "reference script on output",
			apply: func(t *testing.T, tx *mockProducedValidityTx, _ *mockLedgerState) {
				tx.outputs = append(tx.outputs, testAddressScriptOutput{
					testOutput: newTestOutput(1_000_000),
					addr:       keyAddr,
					scriptRef:  refScript,
				})
			},
			wantV1Err: func(t *testing.T, err error) {
				var target babbage.PlutusV1ReferenceScriptsNotSupportedError
				require.ErrorAs(t, err, &target)
			},
			conwayAllowsV1: true,
		},
		{
			name: "inline datum on output",
			apply: func(t *testing.T, tx *mockProducedValidityTx, _ *mockLedgerState) {
				tx.outputs = append(
					tx.outputs,
					newBabbageInlineDatumOutput(t, keyAddr),
				)
			},
			wantV1Err: func(t *testing.T, err error) {
				var target lcommon.InlineDatumsNotSupportedError
				require.ErrorAs(t, err, &target)
			},
		},
		{
			name: "inline datum on spent input",
			apply: func(t *testing.T, tx *mockProducedValidityTx, ls *mockLedgerState) {
				spent := newTestInput(0xe3, 0)
				tx.inputs = append(tx.inputs, spent)
				ls.addUtxo(spent, newBabbageInlineDatumOutput(t, keyAddr))
			},
			wantV1Err: func(t *testing.T, err error) {
				var target lcommon.InlineDatumsNotSupportedError
				require.ErrorAs(t, err, &target)
			},
		},
	}
}

func newV1FeatureTx(
	t *testing.T,
	version lang.LanguageVersion,
	txType int,
	feature *v1FeatureCase,
) (*mockProducedValidityTx, *mockLedgerState, uint) {
	t.Helper()
	plutusScript, costModelIndex := newConwayOverlapMintScript(t, version)
	tx, input := newContextMintTx(t, plutusScript, true, nil, nil, nil)
	tx.txType = txType
	ls := newMockLedgerState()
	ls.addUtxo(input, newTestOutput(10_000_000))
	if feature != nil {
		feature.apply(t, tx, ls)
	}
	return tx, ls, costModelIndex
}

// TestValidateTxBabbageRejectsPlutusV1Features drives ValidateTxBabbage with
// the production Babbage rule list. In the Babbage era a transaction that runs
// a PlutusV1 script cannot carry reference inputs, reference scripts on
// inputs or outputs, or inline datums. The same features are accepted with
// PlutusV2, and a datum-hash output is accepted with PlutusV1.
// Not t.Parallel: this test swaps the package-level Babbage rule list.
func TestValidateTxBabbageRejectsPlutusV1Features(t *testing.T) {
	keepBabbageProductionRules(
		t, lcommon.UtxoValidationRuleInlineDatumsWithPlutusV1,
	)
	pp := &babbage.BabbageProtocolParameters{
		ProtocolMajor: 7,
		CostModels: map[uint][]int64{
			0: defaultMachineCostModel(t, lang.LanguageVersionV1),
			1: defaultMachineCostModel(t, lang.LanguageVersionV2),
		},
		MaxTxExUnits: lcommon.ExUnits{Memory: 10_000_000, Steps: 100_000_000},
	}

	features := v1FeatureCases(t)
	for i := range features {
		feature := &features[i]
		t.Run("V1 "+feature.name, func(t *testing.T) {
			tx, ls, _ := newV1FeatureTx(
				t, lang.LanguageVersionV1, babbage.TxTypeBabbage, feature,
			)
			feature.wantV1Err(t, ValidateTxBabbage(tx, 0, ls, pp))
		})
		t.Run("V2 "+feature.name+" accepted", func(t *testing.T) {
			tx, ls, _ := newV1FeatureTx(
				t, lang.LanguageVersionV2, babbage.TxTypeBabbage, feature,
			)
			require.NoError(t, ValidateTxBabbage(tx, 0, ls, pp))
		})
	}

	t.Run("V1 datum hash output accepted", func(t *testing.T) {
		tx, ls, _ := newV1FeatureTx(
			t, lang.LanguageVersionV1, babbage.TxTypeBabbage, nil,
		)
		tx.outputs = []lcommon.TransactionOutput{testDatumHashOutput{
			testOutput: newTestOutput(1_000_000),
			addr:       newTestKeyAddress(t),
		}}
		require.NoError(t, ValidateTxBabbage(tx, 0, ls, pp))
	})
	t.Run("V1 without features accepted", func(t *testing.T) {
		tx, ls, _ := newV1FeatureTx(
			t, lang.LanguageVersionV1, babbage.TxTypeBabbage, nil,
		)
		require.NoError(t, ValidateTxBabbage(tx, 0, ls, pp))
	})
}

// TestValidateTxConwayAllowsPlutusV1ReferenceFeatures is the control for the
// Babbage rejections: the Conway era accepts reference inputs and reference
// scripts with PlutusV1 but still rejects inline datums.
// Not t.Parallel: this test swaps the package-level Conway rule lists.
func TestValidateTxConwayAllowsPlutusV1ReferenceFeatures(t *testing.T) {
	keepConwayProductionRules(
		t, lcommon.UtxoValidationRuleInlineDatumsWithPlutusV1,
	)
	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: lcommon.ProtocolVersionVanRossem,
		},
		CostModels: map[uint][]int64{
			0: defaultMachineCostModel(t, lang.LanguageVersionV1),
		},
		MaxTxExUnits: lcommon.ExUnits{Memory: 10_000_000, Steps: 100_000_000},
	}
	features := v1FeatureCases(t)
	for i := range features {
		feature := &features[i]
		t.Run(feature.name, func(t *testing.T) {
			tx, ls, _ := newV1FeatureTx(
				t, lang.LanguageVersionV1, txTypeAlonzo, feature,
			)
			err := ValidateTxConway(tx, 0, ls, pp)
			if feature.conwayAllowsV1 {
				require.NoError(t, err)
				return
			}
			var target lcommon.InlineDatumsNotSupportedError
			require.ErrorAs(t, err, &target)
		})
	}
}
