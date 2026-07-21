//go:build devnet

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

package devnet

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	gledger "github.com/blinklabs-io/gouroboros/ledger"
	olsq "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"golang.org/x/crypto/blake2b"
)

// StakeKeyToCredential returns the key-hash stake credential (Tag 0) for a
// raw 32-byte ed25519 stake verification key: blake2b-224 of the key bytes.
func StakeKeyToCredential(vkeyRaw []byte) olsq.StakeCredential {
	h, err := blake2b.New(28, nil)
	if err != nil {
		panic(fmt.Sprintf("blake2b.New(28): %v", err))
	}
	h.Write(vkeyRaw)
	return olsq.StakeCredential{
		Tag:   0,
		Bytes: gledger.NewBlake2b224(h.Sum(nil)),
	}
}

// keyEnvelope is the cardano text-envelope key file format.
type keyEnvelope struct {
	Type        string `json:"type"`
	Description string `json:"description"`
	CborHex     string `json:"cborHex"`
}

// LoadGenesisStakeCredentials parses every *.vkey text-envelope file under
// dir whose type mentions "Stake", returning their key-hash credentials.
// The cborHex is a CBOR byte string (major type 2) wrapping the 32-byte
// ed25519 key, i.e. prefix 0x5820 followed by 32 key bytes.
func LoadGenesisStakeCredentials(dir string) ([]olsq.StakeCredential, error) {
	var out []olsq.StakeCredential
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".vkey") {
			continue
		}
		data, rerr := os.ReadFile(filepath.Join(dir, e.Name()))
		if rerr != nil {
			return nil, fmt.Errorf("read %s: %w", e.Name(), rerr)
		}
		var env keyEnvelope
		if jerr := json.Unmarshal(data, &env); jerr != nil {
			continue // not a text-envelope key file
		}
		if !strings.Contains(env.Type, "Stake") {
			continue
		}
		raw, herr := hex.DecodeString(env.CborHex)
		if herr != nil {
			return nil, fmt.Errorf("decode cborHex in %s: %w", e.Name(), herr)
		}
		// Strip the CBOR byte-string header (0x5820) if present.
		if len(raw) == 34 && raw[0] == 0x58 && raw[1] == 0x20 {
			raw = raw[2:]
		}
		if len(raw) != 32 {
			return nil, fmt.Errorf("unexpected stake key length %d in %s", len(raw), e.Name())
		}
		out = append(out, StakeKeyToCredential(raw))
	}
	return out, nil
}
