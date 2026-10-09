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

package offchainmetadata

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	tokenRegistryManifestVersion  = 1
	tokenRegistryManifestMaxBytes = 64 << 10
	// nolint:gosec // G101 matches the manifest domain separator; it is public.
	tokenRegistryManifestDomain    = "dingo-token-registry-manifest-v1\x00"
	tokenRegistryManifestSequence  = "token_registry_manifest_sequence"
	tokenRegistrySnapshotDigestKey = "token_registry_snapshot_digest"
)

type tokenRegistryManifestEnvelope struct {
	Payload   string `json:"payload"`
	Signature string `json:"signature"`
}

type tokenRegistryManifest struct {
	Version       int    `json:"version"`
	Sequence      uint64 `json:"sequence"`
	ArchiveURL    string `json:"archiveUrl"`
	ArchiveBytes  int64  `json:"archiveBytes"`
	ArchiveDigest string `json:"archiveDigest"`
}

func parseTokenRegistryManifestKey(raw string) (ed25519.PublicKey, error) {
	key, err := hex.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return nil, fmt.Errorf("decode token registry manifest key: %w", err)
	}
	if len(key) != ed25519.PublicKeySize {
		return nil, fmt.Errorf(
			"token registry manifest key is %d bytes, want %d",
			len(key),
			ed25519.PublicKeySize,
		)
	}
	return ed25519.PublicKey(key), nil
}

func decodeStrictJSON(raw []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func verifyTokenRegistryManifest(
	raw []byte,
	key ed25519.PublicKey,
	expectedArchiveURL string,
) (tokenRegistryManifest, error) {
	var envelope tokenRegistryManifestEnvelope
	if err := decodeStrictJSON(raw, &envelope); err != nil {
		return tokenRegistryManifest{}, fmt.Errorf(
			"decode token registry manifest envelope: %w",
			err,
		)
	}
	payload, err := base64.RawURLEncoding.Strict().DecodeString(envelope.Payload)
	if err != nil {
		return tokenRegistryManifest{}, fmt.Errorf(
			"decode token registry manifest payload: %w",
			err,
		)
	}
	signature, err := base64.RawURLEncoding.Strict().DecodeString(
		envelope.Signature,
	)
	if err != nil {
		return tokenRegistryManifest{}, fmt.Errorf(
			"decode token registry manifest signature: %w",
			err,
		)
	}
	message := make([]byte, 0, len(tokenRegistryManifestDomain)+len(payload))
	message = append(message, tokenRegistryManifestDomain...)
	message = append(message, payload...)
	if !ed25519.Verify(key, message, signature) {
		return tokenRegistryManifest{}, errors.New(
			"verify token registry manifest signature: invalid signature",
		)
	}
	var manifest tokenRegistryManifest
	if err := decodeStrictJSON(payload, &manifest); err != nil {
		return tokenRegistryManifest{}, fmt.Errorf(
			"decode signed token registry manifest: %w",
			err,
		)
	}
	if manifest.Version != tokenRegistryManifestVersion {
		return tokenRegistryManifest{}, fmt.Errorf(
			"token registry manifest version %d is not supported",
			manifest.Version,
		)
	}
	if manifest.Sequence == 0 {
		return tokenRegistryManifest{}, errors.New(
			"token registry manifest sequence must be positive",
		)
	}
	if manifest.ArchiveURL != expectedArchiveURL {
		return tokenRegistryManifest{}, errors.New(
			"token registry manifest archive URL does not match configured source",
		)
	}
	if manifest.ArchiveBytes < 0 {
		return tokenRegistryManifest{}, errors.New(
			"token registry manifest archive size must not be negative",
		)
	}
	digest, err := hex.DecodeString(manifest.ArchiveDigest)
	if err != nil || len(digest) != 32 {
		return tokenRegistryManifest{}, errors.New(
			"token registry manifest archive digest must be a 32-byte hex value",
		)
	}
	manifest.ArchiveDigest = strings.ToLower(manifest.ArchiveDigest)
	return manifest, nil
}
