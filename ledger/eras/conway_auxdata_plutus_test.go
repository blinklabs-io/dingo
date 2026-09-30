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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/require"
)

// conwayMetadataRule returns the validation function dingo runs for the
// upstream metadata rule, failing the test when dingo's Conway rule list does
// not carry it. It resolves the position by rule Id, like the list builder.
func conwayMetadataRule(t *testing.T) lcommon.UtxoValidationRuleFunc {
	t.Helper()
	index := resolveUtxoValidationSkipIndex(
		conway.UtxoValidationRuleDescriptors(),
		conway.UtxoValidationRules,
		lcommon.UtxoValidationRuleMetadata,
	)
	for _, rule := range conwayUtxoValidationRules {
		if rule.index == index {
			return rule.validationFunc
		}
	}
	require.FailNow(t, "metadata rule is not in dingo's Conway rule list")
	return nil
}

// conwayTxWithAux decodes a Conway transaction whose body auxiliary_data_hash
// matches the exact auxiliary-data bytes, so only the auxiliary-data content
// can fail the metadata rule.
func conwayTxWithAux(
	t *testing.T,
	aux []byte,
	isValid bool,
) *conway.ConwayTransaction {
	t.Helper()
	hash := lcommon.Blake2b256Hash(aux)
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
	raw = append(raw, aux...)
	var tx conway.ConwayTransaction
	_, err := cbor.Decode(raw, &tx)
	require.NoError(t, err)
	return &tx
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

// TestConwayMetadataRuleRejectsMalformedAuxiliaryPlutusScripts covers the
// well-formedness of Plutus scripts carried in auxiliary data. They are never
// executed, so a phase-1 rule is the only thing that can refuse them, and it
// must refuse them for isValid=false transactions too.
func TestConwayMetadataRuleRejectsMalformedAuxiliaryPlutusScripts(
	t *testing.T,
) {
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

	pp := &conway.ConwayProtocolParameters{}
	pp.ProtocolVersion.Major = 10
	rule := conwayMetadataRule(t)

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
	for _, tc := range tests {
		for _, isValid := range []bool{true, false} {
			name := tc.name + "/isValid=true"
			if !isValid {
				name = tc.name + "/isValid=false"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				tx := conwayTxWithAux(
					t, taggedAuxWithPlutusV1(t, tc.script), isValid,
				)
				err := rule(tx, 0, nil, pp)
				if tc.wantErr == "" {
					require.NoError(t, err)
					return
				}
				require.ErrorContains(t, err, tc.wantErr)
			})
		}
	}
}
