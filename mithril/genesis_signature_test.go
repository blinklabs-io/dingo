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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerifyGenesisCertificateSignature(t *testing.T) {
	pubKey, privKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	message := []byte("mithril-genesis-signed-message")
	signature := ed25519.Sign(privKey, message)
	keyText := fmt.Sprintf(
		`{"type":"GenesisVerificationKey_ed25519","description":"Mithril Genesis Verification Key","cborHex":"5820%s"}`,
		hex.EncodeToString(pubKey),
	)
	cert := &Certificate{
		Hash:             "genesis",
		PreviousHash:     "genesis",
		SignedMessage:    string(message),
		GenesisSignature: hex.EncodeToString(signature),
	}

	require.NoError(t, VerifyGenesisCertificateSignature(cert, keyText))
}

func TestVerifyGenesisCertificateSignatureRejectsBadSignature(t *testing.T) {
	pubKey, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	keyText := fmt.Sprintf(
		`{"type":"GenesisVerificationKey_ed25519","description":"Mithril Genesis Verification Key","cborHex":"5820%s"}`,
		hex.EncodeToString(pubKey),
	)
	cert := &Certificate{
		Hash:             "genesis",
		PreviousHash:     "genesis",
		SignedMessage:    "mithril-genesis-signed-message",
		GenesisSignature: hex.EncodeToString(make([]byte, ed25519.SignatureSize)),
	}

	require.Error(t, VerifyGenesisCertificateSignature(cert, keyText))
}
