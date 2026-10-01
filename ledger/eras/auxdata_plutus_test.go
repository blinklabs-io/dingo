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

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/require"
)

// dingoMetadataRule returns the validation function dingo runs for the
// upstream metadata rule, failing the test when dingo's era rule list does
// not carry it. It resolves the position by rule Id, like the list builder.
func dingoMetadataRule(
	t *testing.T,
	descriptors []lcommon.UtxoValidationRuleDescriptor,
	upstream []lcommon.UtxoValidationRuleFunc,
	dingoRules []indexedUtxoValidationRule,
) lcommon.UtxoValidationRuleFunc {
	t.Helper()
	index := resolveUtxoValidationSkipIndex(
		descriptors, upstream, lcommon.UtxoValidationRuleMetadata,
	)
	for _, rule := range dingoRules {
		if rule.index == index {
			return rule.validationFunc
		}
	}
	require.FailNow(t, "metadata rule is not in dingo's era rule list")
	return nil
}

// txWithAuxBytes returns transaction CBOR whose body auxiliary_data_hash
// matches the exact auxiliary-data bytes, so only the auxiliary-data content
// can fail the metadata rule. The layout is the same from Alonzo to Conway.
func txWithAuxBytes(aux []byte, isValid bool) []byte {
	hash := lcommon.Blake2b256Hash(aux)
	// {0: [], 1: [], 2: 0, 7: h'<hash>'}
	body := append(
		[]byte{0xa4, 0x00, 0x80, 0x01, 0x80, 0x02, 0x00, 0x07, 0x58, 0x20},
		hash.Bytes()...,
	)
	validByte := byte(0xf5)
	if !isValid {
		validByte = 0xf4
	}
	raw := append([]byte{0x84}, body...)
	raw = append(raw, 0xa0, validByte)
	return append(raw, aux...)
}

// taggedAuxWithPlutusV1 returns a tag-259 auxiliary-data map carrying one
// Plutus V1 script whose wire bytes are script.
func taggedAuxWithPlutusV1(t *testing.T, script []byte) []byte {
	t.Helper()
	element, err := cbor.Encode(script)
	require.NoError(t, err)
	aux := []byte{0xd9, 0x01, 0x03, 0xa1, 0x02, 0x81}
	return append(aux, element...)
}

// TestMetadataRuleRejectsMalformedAuxiliaryPlutusScripts covers the
// well-formedness of Plutus scripts carried in auxiliary data. They are never
// executed, so a phase-1 rule is the only thing that can refuse them, and it
// must refuse them for isValid=false transactions too. Each case runs both
// dingo's metadata rule alone and the era's full ValidateTx, which is what
// block application and mempool admission call.
func TestMetadataRuleRejectsMalformedAuxiliaryPlutusScripts(t *testing.T) {
	t.Parallel()

	program := &syn.Program[syn.DeBruijn]{
		Version: lang.LanguageVersionV1,
		Term:    &syn.Lambda[syn.DeBruijn]{Body: &syn.Error{}},
	}
	flat, err := syn.Encode(program)
	require.NoError(t, err)
	validScript, err := cbor.Encode(flat)
	require.NoError(t, err)
	badFlatScript, err := cbor.Encode([]byte{0x01, 0x02, 0x03})
	require.NoError(t, err)
	noWrapperScript := []byte{0x01, 0x02, 0x03}

	type decodeTx func(t *testing.T, raw []byte) lcommon.Transaction
	eras := []struct {
		name       string
		rule       lcommon.UtxoValidationRuleFunc
		validateTx lcommon.UtxoValidationRuleFunc
		pp         lcommon.ProtocolParameters
		decode     decodeTx
	}{
		{
			name: "alonzo",
			rule: dingoMetadataRule(t,
				alonzo.UtxoValidationRuleDescriptors(),
				alonzo.UtxoValidationRules,
				alonzoUtxoValidationRules,
			),
			validateTx: ValidateTxAlonzo,
			pp:         &alonzo.AlonzoProtocolParameters{ProtocolMajor: 6},
			decode: func(t *testing.T, raw []byte) lcommon.Transaction {
				var tx alonzo.AlonzoTransaction
				_, err := cbor.Decode(raw, &tx)
				require.NoError(t, err)
				return &tx
			},
		},
		{
			name: "babbage",
			rule: dingoMetadataRule(t,
				babbage.UtxoValidationRuleDescriptors(),
				babbage.UtxoValidationRules,
				babbageUtxoValidationRules,
			),
			validateTx: ValidateTxBabbage,
			pp:         &babbage.BabbageProtocolParameters{ProtocolMajor: 8},
			decode: func(t *testing.T, raw []byte) lcommon.Transaction {
				var tx babbage.BabbageTransaction
				_, err := cbor.Decode(raw, &tx)
				require.NoError(t, err)
				return &tx
			},
		},
		{
			name: "conway",
			rule: dingoMetadataRule(t,
				conway.UtxoValidationRuleDescriptors(),
				conway.UtxoValidationRules,
				conwayUtxoValidationRules,
			),
			validateTx: ValidateTxConway,
			pp: func() lcommon.ProtocolParameters {
				pp := &conway.ConwayProtocolParameters{}
				pp.ProtocolVersion.Major = 10
				return pp
			}(),
			decode: func(t *testing.T, raw []byte) lcommon.Transaction {
				var tx conway.ConwayTransaction
				_, err := cbor.Decode(raw, &tx)
				require.NoError(t, err)
				return &tx
			},
		},
	}

	tests := []struct {
		name    string
		script  []byte
		wantErr string
	}{
		{name: "well-formed script", script: validScript},
		{
			name:    "malformed UPLC",
			script:  badFlatScript,
			wantErr: "decode Plutus program",
		},
		{
			name:    "missing CBOR script wrapper",
			script:  noWrapperScript,
			wantErr: "decode CBOR script wrapper",
		},
	}
	for _, era := range eras {
		for _, tc := range tests {
			for _, isValid := range []bool{true, false} {
				name := era.name + "/" + tc.name + "/isValid=true"
				if !isValid {
					name = era.name + "/" + tc.name + "/isValid=false"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					tx := era.decode(t, txWithAuxBytes(
						taggedAuxWithPlutusV1(t, tc.script), isValid,
					))
					ruleErr := era.rule(tx, 0, nil, era.pp)
					// The transaction spends nothing, so the full validation
					// always fails on other rules; only the auxiliary-data
					// script error is asserted.
					txErr := era.validateTx(
						tx, 0, newMockLedgerState(), era.pp,
					)
					if tc.wantErr == "" {
						require.NoError(t, ruleErr)
						if txErr != nil {
							require.NotContains(t, txErr.Error(), "auxiliary-data")
						}
						return
					}
					require.ErrorContains(t, ruleErr, tc.wantErr)
					require.ErrorContains(t, txErr, tc.wantErr)
				})
			}
		}
	}
}
