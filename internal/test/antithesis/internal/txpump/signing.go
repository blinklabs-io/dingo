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

package txpump

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/common"
)

// UTxOKey holds the Ed25519 signing material for a UTxO's payment credential
// and the raw address bytes the key controls.
type UTxOKey struct {
	VKey    []byte // 32-byte raw Ed25519 public key (verification key)
	SKey    []byte // 32-byte raw Ed25519 private key seed (signing key)
	Address []byte // raw Cardano address bytes (for use as change address)
}

// cardanoKeyFile is the JSON format emitted by cardano-cli for key files.
type cardanoKeyFile struct {
	CborHex string `json:"cborHex"`
}

// cardanoAddrInfo is the JSON format emitted by cardano-cli address info.
type cardanoAddrInfo struct {
	Base16 string `json:"base16"`
}

// parseKeyFile reads a cardano-cli skey or vkey file and returns the raw
// 32-byte Ed25519 key bytes.  The cborHex field begins with the CBOR
// byte-string header 0x5820 (major type 2, one-byte length 0x20=32) followed
// by the 32 raw key bytes.
func parseKeyFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path) //nolint:gosec // trusted config path
	if err != nil {
		return nil, err
	}
	var kf cardanoKeyFile
	if err := json.Unmarshal(data, &kf); err != nil {
		return nil, fmt.Errorf("parse key file %s: %w", path, err)
	}
	raw, err := hex.DecodeString(kf.CborHex)
	if err != nil {
		return nil, fmt.Errorf("decode cborHex in %s: %w", path, err)
	}
	// Expect CBOR byte-string header: 0x58 (1-byte length follows), 0x20 (32 bytes).
	if len(raw) < 34 || raw[0] != 0x58 || raw[1] != 0x20 {
		return nil, fmt.Errorf(
			"unexpected key encoding in %s (want 5820 prefix, got %x)",
			path, raw[:2],
		)
	}
	return raw[2:], nil // skip 2-byte CBOR header, return 32-byte key
}

// parseAddrInfoFile reads a cardano-cli address info file and returns the
// address as raw bytes decoded from the base16 field.
func parseAddrInfoFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path) //nolint:gosec // trusted config path
	if err != nil {
		return nil, err
	}
	var ai cardanoAddrInfo
	if err := json.Unmarshal(data, &ai); err != nil {
		return nil, fmt.Errorf("parse addr info %s: %w", path, err)
	}
	if ai.Base16 == "" {
		return nil, fmt.Errorf("addr info %s has empty base16 field", path)
	}
	addrBytes, err := hex.DecodeString(ai.Base16)
	if err != nil {
		return nil, fmt.Errorf("decode base16 in %s: %w", path, err)
	}
	return addrBytes, nil
}

// LoadSigningKey reads the genesis UTxO key for pool index 1 from dir.
// It looks for genesis.1.skey, genesis.1.vkey, and genesis.1.addr.info.
// Returns nil, nil if the skey file is absent (signing is optional).
func LoadSigningKey(dir string) (*UTxOKey, error) {
	key, err := loadSigningKeyPrefix(filepath.Join(dir, "genesis.1"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // signing not configured — not an error
		}
		return nil, err
	}
	return key, nil
}

// LoadSigningKeys reads all genesis UTxO keys from dir. It looks for files
// named genesis.<n>.skey and their matching .vkey and .addr.info siblings.
func LoadSigningKeys(dir string) ([]*UTxOKey, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "genesis.*.skey"))
	if err != nil {
		return nil, fmt.Errorf("glob signing keys: %w", err)
	}
	sort.Strings(matches)
	keys := make([]*UTxOKey, 0, len(matches))
	for _, skeyPath := range matches {
		prefix := strings.TrimSuffix(skeyPath, ".skey")
		key, loadErr := loadSigningKeyPrefix(prefix)
		if loadErr != nil {
			return nil, loadErr
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func loadSigningKeyPrefix(prefix string) (*UTxOKey, error) {
	skey, err := parseKeyFile(prefix + ".skey")
	if err != nil {
		return nil, fmt.Errorf("load signing key: %w", err)
	}
	vkey, err := parseKeyFile(prefix + ".vkey")
	if err != nil {
		return nil, fmt.Errorf("load verification key: %w", err)
	}
	addr, err := parseAddrInfoFile(prefix + ".addr.info")
	if err != nil {
		return nil, fmt.Errorf("load signing key address: %w", err)
	}
	return &UTxOKey{VKey: vkey, SKey: skey, Address: addr}, nil
}

// vkeyWit is the two-element CBOR array structure for a VKey witness:
// [verification_key_bytes, signature_bytes].
type vkeyWit struct {
	cbor.StructAsArray
	VKey []byte // 32-byte Ed25519 verification key
	Sig  []byte // 64-byte Ed25519 signature
}

// BuildWitnessMap computes Ed25519 VKey witnesses for the CBOR-encoded tx body
// and returns a map[any]any ready for use as the witness set field of a Conway
// transaction.  Duplicate keys (by VKey hex) are silently skipped.
// Returns an empty map if no non-nil keys are provided.
func BuildWitnessMap(bodyBytes []byte, keys ...*UTxOKey) map[any]any {
	var nonNilKeys []*UTxOKey
	for _, k := range keys {
		if k != nil {
			nonNilKeys = append(nonNilKeys, k)
		}
	}
	if len(nonNilKeys) == 0 {
		return map[any]any{}
	}

	txBodyHash := common.Blake2b256Hash(bodyBytes)

	seen := make(map[string]struct{}, len(nonNilKeys))
	var witnesses []vkeyWit
	for _, k := range nonNilKeys {
		vkeyHex := hex.EncodeToString(k.VKey)
		if _, dup := seen[vkeyHex]; dup {
			continue
		}
		seen[vkeyHex] = struct{}{}
		privKey := ed25519.NewKeyFromSeed(k.SKey)
		sig := ed25519.Sign(privKey, txBodyHash[:])
		witnesses = append(witnesses, vkeyWit{VKey: k.VKey, Sig: sig})
	}
	if len(witnesses) == 0 {
		return map[any]any{}
	}
	return map[any]any{uint64(0): witnesses}
}
