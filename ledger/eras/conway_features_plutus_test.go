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

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
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
