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
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
)

// VerifyGenesisCertificateSignature verifies a genesis certificate signature
// with the configured Mithril genesis verification key.
func VerifyGenesisCertificateSignature(
	cert *Certificate,
	verificationKeyText string,
) error {
	if cert == nil {
		return errors.New("certificate is nil")
	}
	if !cert.IsGenesis() {
		return fmt.Errorf("certificate %s is not a genesis certificate", cert.Hash)
	}
	key, err := ParseVerificationKey(verificationKeyText)
	if err != nil {
		return fmt.Errorf("parsing genesis verification key: %w", err)
	}
	if len(key.RawKeyBytes) != ed25519.PublicKeySize {
		return fmt.Errorf(
			"unexpected genesis verification key size: got %d, expected %d",
			len(key.RawKeyBytes),
			ed25519.PublicKeySize,
		)
	}

	signatureCandidates := decodeByteCandidates(cert.GenesisSignature, false)
	messageCandidates := decodeByteCandidates(cert.SignedMessage, true)
	pubKey := ed25519.PublicKey(key.RawKeyBytes)
	for _, sig := range signatureCandidates {
		if len(sig) != ed25519.SignatureSize {
			continue
		}
		for _, msg := range messageCandidates {
			if ed25519.Verify(pubKey, msg, sig) {
				return nil
			}
		}
	}
	return errors.New("genesis signature verification failed")
}

func decodeByteCandidates(data string, includeRaw bool) [][]byte {
	var candidates [][]byte
	addCandidate := func(v []byte) {
		if len(v) == 0 {
			return
		}
		for _, existing := range candidates {
			if bytes.Equal(existing, v) {
				return
			}
		}
		candidates = append(candidates, v)
	}

	if includeRaw {
		addCandidate([]byte(data))
	}
	if decoded, err := decodeHexString(data); err == nil {
		addCandidate(decoded)
	}
	if decoded, err := base64.StdEncoding.DecodeString(data); err == nil {
		addCandidate(decoded)
	}
	if decoded, err := base64.RawStdEncoding.DecodeString(data); err == nil {
		addCandidate(decoded)
	}
	if decoded, err := base64.URLEncoding.DecodeString(data); err == nil {
		addCandidate(decoded)
	}
	if decoded, err := base64.RawURLEncoding.DecodeString(data); err == nil {
		addCandidate(decoded)
	}
	return candidates
}
