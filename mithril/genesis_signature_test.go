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
	"crypto/ed25519"
	"encoding/hex"
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerifyGenesisCertificateSignature(t *testing.T) {
	t.Parallel()
	cert, keyText := signedGenesisCertificateForTest(t)
	require.NoError(t, VerifyGenesisCertificateSignature(cert, keyText))
}

func TestVerifyGenesisCertificateSignatureRejectsUnsignedFields(t *testing.T) {
	t.Parallel()
	cert, keyText := signedGenesisCertificateForTest(t)

	t.Run("aggregate verification key", func(t *testing.T) {
		t.Parallel()
		tampered := *cert
		tampered.AggregateVerificationKey = "different-key"
		var err error
		tampered.Hash, err = tampered.ComputeHash()
		require.NoError(t, err)
		require.ErrorContains(
			t, VerifyGenesisCertificateSignature(&tampered, keyText),
			"aggregate verification key",
		)
	})

	t.Run("protocol parameters", func(t *testing.T) {
		t.Parallel()
		tampered := *cert
		tampered.Metadata.Parameters.K++
		var err error
		tampered.Hash, err = tampered.ComputeHash()
		require.NoError(t, err)
		require.ErrorContains(
			t, VerifyGenesisCertificateSignature(&tampered, keyText),
			"protocol parameters",
		)
	})

	t.Run("protocol message", func(t *testing.T) {
		t.Parallel()
		tampered := *cert
		tampered.AggregateVerificationKey = "different-key"
		tampered.ProtocolMessage.MessageParts = maps.Clone(
			cert.ProtocolMessage.MessageParts,
		)
		tampered.ProtocolMessage.MessageParts["next_aggregate_verification_key"] =
			tampered.AggregateVerificationKey
		var err error
		tampered.Hash, err = tampered.ComputeHash()
		require.NoError(t, err)
		require.ErrorContains(
			t,
			VerifyGenesisCertificateSignature(&tampered, keyText),
			"signed message",
		)
	})
}

func signedGenesisCertificateForTest(t *testing.T) (*Certificate, string) {
	t.Helper()
	keyText, privateKey := testGenesisKeyPair(t)
	metadata := standardTestMetadata()
	avk := testBLSG2Hex(t)
	cert := &Certificate{
		Epoch:                    1,
		Metadata:                 metadata,
		AggregateVerificationKey: avk,
		ProtocolMessage: ProtocolMessage{MessageParts: map[string]string{
			"current_epoch":                   "1",
			"next_aggregate_verification_key": avk,
			"next_protocol_parameters":        metadata.Parameters.ComputeHash(),
		}},
	}
	signTestGenesisCertificate(t, cert, privateKey)
	return cert, keyText
}

func TestVerifyGenesisCertificateSignatureRejectsBadSignature(t *testing.T) {
	t.Parallel()

	cert, keyText := signedGenesisCertificateForTest(t)
	cert.GenesisSignature = hex.EncodeToString(
		make([]byte, ed25519.SignatureSize),
	)

	require.Error(t, VerifyGenesisCertificateSignature(cert, keyText))
}
