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

package mithril

import (
	"encoding/json"
	"testing"

	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// flattenKESSignatureJSON inverts EncodeKESSignature's nesting: innermost
// signature first, then each level's public-key pair from the inside out.
func flattenKESSignatureJSON(t *testing.T, level map[string]any) []byte {
	t.Helper()
	var out []byte
	switch inner := level["sigma"].(type) {
	case map[string]any:
		out = flattenKESSignatureJSON(t, inner)
	case []any:
		out = jsonNumbersToBytes(t, inner)
	default:
		require.Failf(t, "unexpected sigma", "%T", inner)
	}
	for _, key := range []string{"lhs_pk", "rhs_pk"} {
		out = append(out, jsonNumbersToBytes(t, level[key].([]any))...)
	}
	return out
}

func jsonNumbersToBytes(t *testing.T, numbers []any) []byte {
	t.Helper()
	out := make([]byte, len(numbers))
	for i, n := range numbers {
		out[i] = byte(n.(float64))
	}
	return out
}

func TestSignerWireFormsMatchReferenceRegistration(t *testing.T) {
	t.Parallel()
	fx := loadSignerFixture(t)
	var ref AggregatorSigner
	require.NoError(t, json.Unmarshal(fx.RegisteredSigner, &ref))

	// Operational certificate: decode the published value, rebuild it from
	// its fields and expect the identical string.
	rawOpCert, ok := decodePrimaryEncodedBytes(ref.OperationalCertificate)
	require.True(t, ok)
	var opCert []json.RawMessage
	require.NoError(t, json.Unmarshal(rawOpCert, &opCert))
	require.Len(t, opCert, 2)
	var body struct {
		KESVKey       []byte
		IssueNumber   uint64
		KESPeriod     uint64
		ColdSignature []byte
	}
	var fields []json.RawMessage
	require.NoError(t, json.Unmarshal(opCert[0], &fields))
	require.Len(t, fields, 4)
	require.NoError(t, json.Unmarshal(fields[0], &body.KESVKey))
	require.NoError(t, json.Unmarshal(fields[1], &body.IssueNumber))
	require.NoError(t, json.Unmarshal(fields[2], &body.KESPeriod))
	require.NoError(t, json.Unmarshal(fields[3], &body.ColdSignature))
	var coldVKey []byte
	require.NoError(t, json.Unmarshal(opCert[1], &coldVKey))
	encodedOpCert, err := EncodeOperationalCertificate(
		body.KESVKey,
		body.IssueNumber,
		body.KESPeriod,
		body.ColdSignature,
		coldVKey,
	)
	require.NoError(t, err)
	assert.Equal(t, ref.OperationalCertificate, encodedOpCert)

	// KES signature: flatten the published value to the raw layout kes.Sign
	// produces, check it is the reference's signature over the verification
	// key and its proof at the published period, then re-encode it.
	rawSig, ok := decodePrimaryEncodedBytes(ref.VerificationKeySignature)
	require.True(t, ok)
	var nested map[string]any
	require.NoError(t, json.Unmarshal(rawSig, &nested))
	flat := flattenKESSignatureJSON(t, nested)
	require.Len(t, flat, kes.CardanoKesSignatureSize)
	rawVK, ok := decodePrimaryEncodedBytes(ref.VerificationKey)
	require.True(t, ok)
	var vk stmSignerVerificationKey
	require.NoError(t, json.Unmarshal(rawVK, &vk))
	assert.True(t, kes.VerifySignedKES(
		body.KESVKey,
		ref.KESPeriod,
		append(append([]byte(nil), vk.VK...), vk.Pop...),
		flat,
	))
	encodedSig, err := EncodeKESSignature(flat)
	require.NoError(t, err)
	assert.Equal(t, ref.VerificationKeySignature, encodedSig)

	_, err = EncodeKESSignature(flat[:len(flat)-1])
	require.Error(t, err)
}
