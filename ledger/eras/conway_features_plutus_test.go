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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockConwayFeaturesTx struct {
	mockConwayFeeTx
	currentTreasuryValue *big.Int
}

func (m *mockConwayFeaturesTx) CurrentTreasuryValue() *big.Int {
	return m.currentTreasuryValue
}

func TestConwayFeaturesRuleAllowsUnneededPlutusV1V2(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		script lcommon.Script
	}{
		{name: "PlutusV1", script: lcommon.PlutusV1Script{0x01}},
		{name: "PlutusV2", script: lcommon.PlutusV2Script{0x02}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := newTestInput(0x81, 0)
			tx := newConwayFeaturesTestTx(input)
			ls := newMockLedgerState()
			ls.addUtxo(input, testAddressScriptOutput{
				testOutput: newTestOutput(1_000_000),
				addr:       newTestKeyAddress(t),
				scriptRef:  tc.script,
			})

			require.NoError(t, conwayFeaturesRule(t)(
				tx,
				0,
				ls,
				&conway.ConwayProtocolParameters{},
			))
		})
	}
}

func TestConwayFeaturesRuleDefersPV11PlutusV3ReferenceInputCheck(
	t *testing.T,
) {
	t.Parallel()
	input := newTestInput(0x83, 0)
	tx := &mockConwayFeaturesTx{mockConwayFeeTx: mockConwayFeeTx{
		mockFeeTx: mockFeeTx{fee: big.NewInt(0)},
	}}
	tx.inputs = []lcommon.TransactionInput{input}
	tx.referenceInputs = []lcommon.TransactionInput{input}
	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: lcommon.ProtocolVersionVanRossem,
		},
	}
	require.NoError(t, conwayFeaturesRule(t)(
		tx,
		0,
		newMockLedgerState(),
		pp,
	))
}

// TestValidateTxConwayPV11ReferenceInputOverlapUsesExecutedLanguage exercises
// the production validator with the relevant reference-overlap rules.
// Not t.Parallel: this test swaps the package-level Conway validation rule sets.
func TestValidateTxConwayPV11ReferenceInputOverlapUsesExecutedLanguage(
	t *testing.T,
) {
	useConwayReferenceOverlapRules(t)

	t.Run("PV10 remains transaction-wide", func(t *testing.T) {
		input := newTestInput(0x84, 0)
		tx := newConwayOverlapTx(input, nil, nil)
		ls := newMockLedgerState()
		ls.addUtxo(input, newTestOutput(2_000_000))
		pp := &conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: lcommon.ProtocolVersionPlomin,
			},
		}
		require.ErrorContains(
			t,
			ValidateTxConway(tx, 0, ls, pp),
			"non-disjoint reference inputs",
		)
	})

	t.Run("PV11 without Plutus is allowed", func(t *testing.T) {
		input := newTestInput(0x85, 0)
		tx := newConwayOverlapTx(input, nil, nil)
		ls := newMockLedgerState()
		ls.addUtxo(input, newTestOutput(2_000_000))
		require.NoError(t, ValidateTxConway(
			tx,
			0,
			ls,
			conwayOverlapProtocolParams(
				t,
				lcommon.ProtocolVersionVanRossem,
				0,
				lang.LanguageVersionV1,
			),
		))
		_, _, _, err := EvaluateTxConway(
			tx,
			ls,
			conwayOverlapProtocolParams(
				t,
				lcommon.ProtocolVersionVanRossem,
				0,
				lang.LanguageVersionV1,
			),
		)
		require.NoError(t, err)
	})

	for _, tc := range []struct {
		name           string
		version        lang.LanguageVersion
		costModelIndex uint
	}{
		{name: "PlutusV1", version: lang.LanguageVersionV1, costModelIndex: 0},
		{name: "PlutusV2", version: lang.LanguageVersionV2, costModelIndex: 1},
	} {
		t.Run("PV11 "+tc.name+" execution is allowed", func(t *testing.T) {
			input := newTestInput(0x86, 0)
			script, _ := newConwayOverlapMintScript(t, tc.version)
			tx := newConwayOverlapTx(input, script, nil)
			ls := newMockLedgerState()
			ls.addUtxo(input, newTestOutput(2_000_000))
			require.NoError(t, ValidateTxConway(
				tx,
				0,
				ls,
				conwayOverlapProtocolParams(
					t,
					lcommon.ProtocolVersionVanRossem,
					tc.costModelIndex,
					tc.version,
				),
			))
			_, _, _, err := EvaluateTxConway(
				tx,
				ls,
				conwayOverlapProtocolParams(
					t,
					lcommon.ProtocolVersionVanRossem,
					tc.costModelIndex,
					tc.version,
				),
			)
			require.NoError(t, err)
		})
	}

	t.Run(
		"PV11 V3 execution rejects overlap despite unused V1 and V2 scripts",
		func(t *testing.T) {
			input := newTestInput(0x87, 0)
			v3Script, _ := newConwayOverlapMintScript(t, lang.LanguageVersionV3)
			tx := newConwayOverlapTx(
				input,
				v3Script,
				[]lcommon.PlutusV1Script{{0x11}},
			)
			ls := newMockLedgerState()
			ls.addUtxo(input, testAddressScriptOutput{
				testOutput: newTestOutput(2_000_000),
				addr:       newTestKeyAddress(t),
				scriptRef:  lcommon.PlutusV2Script{0x33},
			})
			err := ValidateTxConway(
				tx,
				0,
				ls,
				conwayOverlapProtocolParams(
					t,
					lcommon.ProtocolVersionVanRossem,
					2,
					lang.LanguageVersionV3,
				),
			)
			require.ErrorContains(t, err, "also a regular input")
			_, _, _, err = EvaluateTxConway(
				tx,
				ls,
				conwayOverlapProtocolParams(
					t,
					lcommon.ProtocolVersionVanRossem,
					2,
					lang.LanguageVersionV3,
				),
			)
			require.ErrorContains(t, err, "also a regular input")
		},
	)
}

func useConwayReferenceOverlapRules(t *testing.T) {
	t.Helper()
	originalRules := conwayUtxoValidationRules
	originalPhase1Rules := conwayPhase1UtxoValidationRules
	t.Cleanup(func() {
		conwayUtxoValidationRules = originalRules
		conwayPhase1UtxoValidationRules = originalPhase1Rules
	})
	descriptors := conway.UtxoValidationRuleDescriptors()
	disjointIndex := requireRuleIdResolvesToFunc(
		t,
		descriptors,
		conway.UtxoValidationRules,
		lcommon.UtxoValidationRuleDisjointRefInputs,
		"conway.UtxoValidateDisjointRefInputs",
	)
	featuresIndex := requireRuleIdResolvesToFunc(
		t,
		descriptors,
		conway.UtxoValidationRules,
		lcommon.UtxoValidationRuleConwayFeaturesWithPlutusV1V2,
		"conway.UtxoValidateConwayFeaturesWithPlutusV1V2",
	)
	rules := make([]indexedUtxoValidationRule, 0, 2)
	for _, rule := range originalRules {
		if rule.index == disjointIndex || rule.index == featuresIndex {
			rules = append(rules, rule)
		}
	}
	require.Len(t, rules, 2)
	conwayUtxoValidationRules = rules
	conwayPhase1UtxoValidationRules = rules
}

func newConwayOverlapTx(
	input lcommon.TransactionInput,
	usedScript lcommon.Script,
	unusedV1 []lcommon.PlutusV1Script,
) *mockConwayFeeTx {
	witnesses := &mockWitnessSet{
		plutusV1Scripts: append([]lcommon.PlutusV1Script(nil), unusedV1...),
	}
	if usedScript != nil {
		switch script := usedScript.(type) {
		case lcommon.PlutusV1Script:
			witnesses.plutusV1Scripts = []lcommon.PlutusV1Script{script}
		case lcommon.PlutusV2Script:
			witnesses.plutusV2Scripts = []lcommon.PlutusV2Script{script}
		case lcommon.PlutusV3Script:
			witnesses.plutusV3Scripts = []lcommon.PlutusV3Script{script}
		}
		hash := usedScript.Hash()
		assetMint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
			map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
				lcommon.Blake2b224(hash): {
					cbor.NewByteString([]byte("context-overlap")): big.NewInt(
						1,
					),
				},
			},
		)
		witnesses.redeemers = &mockRedeemers{entries: []struct {
			key lcommon.RedeemerKey
			val lcommon.RedeemerValue
		}{
			{
				key: lcommon.RedeemerKey{
					Tag:   lcommon.RedeemerTagMint,
					Index: 0,
				},
				val: lcommon.RedeemerValue{
					Data: lcommon.Datum{Data: data.NewConstr(0)},
					ExUnits: lcommon.ExUnits{
						Memory: 5_000_000,
						Steps:  50_000_000,
					},
				},
			},
		}}
		return &mockConwayFeeTx{
			mockFeeTx: mockFeeTx{fee: big.NewInt(0), witnesses: witnesses},
			inputs:    []lcommon.TransactionInput{input},
			referenceInputs: []lcommon.TransactionInput{
				input,
			},
			assetMint: &assetMint,
		}
	}
	return &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{fee: big.NewInt(0), witnesses: witnesses},
		inputs:    []lcommon.TransactionInput{input},
		referenceInputs: []lcommon.TransactionInput{
			input,
		},
	}
}

func conwayOverlapProtocolParams(
	t *testing.T,
	major uint,
	costModelIndex uint,
	version lang.LanguageVersion,
) *conway.ConwayProtocolParameters {
	models := map[uint][]int64(nil)
	if major >= lcommon.ProtocolVersionVanRossem {
		models = map[uint][]int64{
			costModelIndex: defaultMachineCostModel(t, version),
		}
	}
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: major,
		},
		CostModels: models,
		MaxTxExUnits: lcommon.ExUnits{
			Memory: 10_000_000,
			Steps:  100_000_000,
		},
	}
}

func newConwayOverlapMintScript(
	t *testing.T,
	version lang.LanguageVersion,
) (lcommon.Script, uint) {
	t.Helper()
	uplcVersion := lang.LanguageVersionV1
	parameterCount := 2
	costModelIndex := uint(0)
	switch version {
	case lang.LanguageVersionV2:
		costModelIndex = 1
	case lang.LanguageVersionV3:
		uplcVersion = lang.LanguageVersion{1, 1, 0}
		parameterCount = 1
		costModelIndex = 2
	default:
		require.Equal(t, lang.LanguageVersionV1, version)
	}
	var term syn.Term[syn.DeBruijn] = &syn.Constant{Con: &syn.Unit{}}
	for range parameterCount {
		term = &syn.Lambda[syn.DeBruijn]{Body: term}
	}
	flatProgram, err := syn.Encode(&syn.Program[syn.DeBruijn]{
		Version: uplcVersion,
		Term:    term,
	})
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flatProgram)
	require.NoError(t, err)
	switch version {
	case lang.LanguageVersionV1:
		return lcommon.PlutusV1Script(scriptBytes), costModelIndex
	case lang.LanguageVersionV2:
		return lcommon.PlutusV2Script(scriptBytes), costModelIndex
	default:
		return lcommon.PlutusV3Script(scriptBytes), costModelIndex
	}
}

func TestConwayScriptPurposeUsesActiveProtocolMajor(t *testing.T) {
	t.Parallel()
	certificate := &lcommon.RegistrationCertificate{
		StakeCredential: lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.Blake2b224{1},
		},
		Amount: 2_000_000,
	}
	for _, tc := range []struct {
		name  string
		major uint
		want  data.PlutusData
	}{
		{
			name:  "PV9",
			major: lcommon.ProtocolVersionConway,
			want:  data.NewConstr(1),
		},
		{
			name:  "PV10",
			major: lcommon.ProtocolVersionPlomin,
			want: data.NewConstr(0,
				data.NewInteger(big.NewInt(2_000_000))),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pp := &conway.ConwayProtocolParameters{
				ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
					Major: tc.major,
				},
			}
			purpose, ok := buildConwayScriptPurpose(
				lcommon.RedeemerKey{Tag: lcommon.RedeemerTagCert},
				nil,
				nil,
				lcommon.MultiAsset[lcommon.MultiAssetTypeMint]{},
				[]lcommon.Certificate{certificate},
				nil,
				nil,
				nil,
				nil,
				protocolMajorVersion(pp),
			)
			require.True(t, ok)
			fields := purpose.ToPlutusData().(*data.Constr).Fields
			certificateData := fields[1].(*data.Constr).Fields
			require.Equal(t, tc.want, certificateData[1])
		})
	}
}

func TestConwayFeaturesRuleRejectsNeededPlutusV1V2(t *testing.T) {
	for _, tc := range []struct {
		name          string
		script        lcommon.Script
		plutusVersion string
	}{
		{
			name:          "PlutusV1",
			script:        lcommon.PlutusV1Script{0x03},
			plutusVersion: "PlutusV1",
		},
		{
			name:          "PlutusV2",
			script:        lcommon.PlutusV2Script{0x04},
			plutusVersion: "PlutusV2",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := newTestInput(0x82, 0)
			tx := newConwayFeaturesTestTx(input)
			ls := newMockLedgerState()
			ls.addUtxo(input, testAddressScriptOutput{
				testOutput: newTestOutput(1_000_000),
				addr:       newTestScriptAddress(t, tc.script),
				scriptRef:  tc.script,
			})

			var featureErr conway.ConwayCertificateWithPlutusV1V2Error
			require.ErrorAs(t, conwayFeaturesRule(t)(
				tx,
				0,
				ls,
				&conway.ConwayProtocolParameters{},
			), &featureErr)
			assert.Equal(t, tc.plutusVersion, featureErr.PlutusVersion)
			assert.Equal(t, "VoteDelegation", featureErr.CertificateType)
		})
	}
}

func conwayFeaturesRule(t *testing.T) lcommon.UtxoValidationRuleFunc {
	t.Helper()
	for _, rule := range conwayUtxoValidationRules {
		if utxoValidationRuleName(rule.validationFunc) ==
			utxoValidationRuleName(validateConwayFeaturesWithNeededPlutusV1V2) {
			return rule.validationFunc
		}
	}
	t.Fatal("Conway PlutusV1/V2 feature rule was not installed")
	return nil
}

func newConwayFeaturesTestTx(
	input lcommon.TransactionInput,
) *mockConwayFeaturesTx {
	return &mockConwayFeaturesTx{
		mockConwayFeeTx: mockConwayFeeTx{
			mockFeeTx: mockFeeTx{},
			inputs:    []lcommon.TransactionInput{input},
			certificates: []lcommon.Certificate{
				&lcommon.VoteDelegationCertificate{
					StakeCredential: lcommon.Credential{
						CredType: lcommon.CredentialTypeAddrKeyHash,
					},
				},
			},
		},
	}
}
